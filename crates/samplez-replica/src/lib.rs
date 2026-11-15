//! A samplez product's state replicated over openraft: the log and
//! snapshots on the replica's volume, the per-session tables of applied
//! command ids, and [`ReplicatedState`] over a [`Product`].

pub mod dedup;
pub mod entry;
pub mod log_store;
pub mod snapshot;
pub mod state_machine;

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::network::RaftNetworkFactory;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft, ServerState};
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};
use samplez_service::ws::{Delta, Snapshot};
use samplez_service::{Owner, Position, ReadIndex, ReplicaInfo, ReplicatedState};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::dedup::Limits;
use crate::entry::{
    Batch, Body, Command, NodeId, Origin, Request, Response, SessionKey, stamping_index,
};
use crate::log_store::Store;
use crate::state_machine::Product;

openraft::declare_raft_types!(
    /// The openraft types of a samplez replica set.
    pub TypeConfig:
        D = Request,
        R = Response,
);

/// openraft's timers as the chart pins them (`replica.raft`): its 0.9.21
/// defaults, so the election figures do not move with a dependency
/// update.
pub fn raft_config(cluster: &str) -> openraft::Config {
    openraft::Config {
        cluster_name: cluster.to_owned(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..openraft::Config::default()
    }
}

/// A replica's settings beside openraft's.
#[derive(Debug, Clone)]
pub struct Settings {
    pub id: NodeId,
    /// The id of the state's history, which revisions are named within.
    pub epoch: u64,
    pub limits: Limits,
    /// The engine block size B the stamping index rounds to.
    pub block: u64,
    /// The longest a submit waits, from the call, for its command to apply
    /// here, queueing behind earlier writes included, before it resolves
    /// [`NotApplied`] with [`Reason::Unavailable`]. A command still queued
    /// at its deadline is not ordered. An entry the replica set decides on
    /// waits this long from the moment it is proposed.
    pub write_timeout: Duration,
}

/// This replica's estimate E of the set's processed index, in samples.
pub type Estimate = Arc<dyn Fn() -> u64 + Send + Sync>;

type Reply = oneshot::Sender<Result<Ack, NotApplied>>;

enum Queued {
    Command(Command, Instant, Reply),
    Entry(Body),
}

/// One replica of a product's state.
pub struct Replica<P> {
    settings: Settings,
    raft: Raft<TypeConfig>,
    store: Store<P>,
    queue: mpsc::UnboundedSender<Queued>,
    /// Set once the replica applied the log up to the committed id it
    /// knows ([`ReplicaInfo::ready`]); nothing clears it yet.
    ready: AtomicBool,
}

impl<P: Product> Replica<P> {
    /// Opens the storage in `dir` and starts the replica: a restarted one
    /// applies its log up to the saved committed id before it hears from
    /// any peer.
    pub async fn start<N>(
        settings: Settings,
        raft: openraft::Config,
        dir: &Path,
        product: P,
        network: N,
        estimate: Estimate,
    ) -> Result<Replica<P>, String>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let store = Store::open(dir, product, settings.limits).map_err(|e| e.to_string())?;
        let raft_config = Arc::new(raft.validate().map_err(|e| e.to_string())?);
        let (log, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(settings.id, raft_config, network, log, state_machine)
            .await
            .map_err(|e| e.to_string())?;
        let (queue, queued) = mpsc::unbounded_channel();
        tokio::spawn(write(raft.clone(), queued, estimate, settings.clone()));
        Ok(Replica {
            settings,
            raft,
            store,
            queue,
            ready: AtomicBool::new(false),
        })
    }

    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub fn store(&self) -> &Store<P> {
        &self.store
    }

    /// Orders an entry the replica set decides on rather than a client,
    /// behind every command submitted before it.
    pub fn append(&self, body: Body) {
        let _ = self.queue.send(Queued::Entry(body));
    }

    /// Makes `members` the first voters; on one node of a new set only.
    pub async fn initialize(
        &self,
        members: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), String> {
        let members: std::collections::BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|id| (id, BasicNode::new(id.to_string())))
            .collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

/// The only writer: takes the queue in order, so commands commit in the
/// order they were submitted, and puts what queued while the previous
/// entry was written into the next one (group commit). A copy of a
/// command the batch holds already is folded into it and answered from
/// its outcome, an applied one as a duplicate.
///
/// A first cut: one entry is in flight at a time and the writer waits for
/// it to commit and apply here before it proposes the next, so the
/// batching window is one commit round-trip, not one log append and
/// fsync, and a command that arrives just after a write starts waits for
/// up to two commits.
async fn write(
    raft: Raft<TypeConfig>,
    mut queued: mpsc::UnboundedReceiver<Queued>,
    estimate: Estimate,
    settings: Settings,
) {
    let mut next = None;
    loop {
        let first = match next.take() {
            Some(first) => first,
            None => match queued.recv().await {
                Some(first) => first,
                None => return,
            },
        };
        let request = |body| Request {
            stamp: stamping_index(estimate(), settings.block),
            body,
        };
        match first {
            Queued::Entry(body) => {
                let _ =
                    tokio::time::timeout(settings.write_timeout, raft.client_write(request(body)))
                        .await;
            }
            Queued::Command(command, deadline, reply) => {
                let mut batch = Batch::default();
                let mut replies = Vec::new();
                let mut latest = Instant::now();
                let mut push = |command: Command, deadline: Instant, reply: Reply| {
                    // Its submit timed out while it queued: not ordered.
                    if deadline <= Instant::now() {
                        let _ = reply.send(Err(NotApplied::new(Reason::Unavailable, "timed out")));
                        return;
                    }
                    latest = latest.max(deadline);
                    let id = command.id.clone();
                    replies.push((batch.push(command), id, reply));
                };
                push(command, deadline, reply);
                while let Ok(item) = queued.try_recv() {
                    match item {
                        Queued::Command(command, deadline, reply) => push(command, deadline, reply),
                        entry => {
                            next = Some(entry);
                            break;
                        }
                    }
                }
                if batch.is_empty() {
                    continue;
                }
                // Each submit times out at its own deadline; the writer
                // waits for the latest of them.
                let written = tokio::time::timeout_at(
                    latest,
                    raft.client_write(request(Body::Commands(batch.into_commands()))),
                )
                .await;
                match written {
                    Ok(Ok(written)) => {
                        let outcomes = written.data.0;
                        let mut answered = vec![false; outcomes.len()];
                        for (at, id, reply) in replies {
                            let outcome = outcomes[at];
                            let resolution = match outcome.result {
                                entry::Resolution::Applied | entry::Resolution::Unchanged
                                    if answered[at] =>
                                {
                                    entry::Resolution::Duplicate
                                }
                                result => result,
                            };
                            answered[at] = true;
                            let _ = reply.send(Ok(Ack::new(
                                id,
                                outcome.revision,
                                resolution_of(resolution),
                            )));
                        }
                    }
                    // Not the leader, or no longer: the entry was not
                    // appended here, or a new leader truncated it.
                    Ok(Err(e)) if e.forward_to_leader::<BasicNode>().is_some() => {
                        reply_all(replies, Reason::NotCommitable, &e.to_string())
                    }
                    Ok(Err(e)) => reply_all(replies, Reason::Unavailable, &e.to_string()),
                    Err(_) => reply_all(replies, Reason::Unavailable, "timed out"),
                }
            }
        }
    }
}

fn resolution_of(resolution: entry::Resolution) -> Resolution {
    match resolution {
        entry::Resolution::Applied => Resolution::Applied,
        entry::Resolution::Unchanged => Resolution::Unchanged,
        entry::Resolution::Duplicate => Resolution::Duplicate,
        entry::Resolution::Stale => Resolution::Stale,
    }
}

fn reply_all(replies: Vec<(usize, String, Reply)>, reason: Reason, detail: &str) {
    for (_, _, reply) in replies {
        let _ = reply.send(Err(NotApplied::new(reason, detail)));
    }
}

impl<P: Product + Sync> ReplicatedState for Replica<P> {
    /// `ready` once the replica has joined a set, a committed id saved on
    /// its volume, and applied the log up to that id, which after a
    /// restart is the saved one; it stays set. An empty volume is not
    /// ready until it has joined, a saved vote alone included. The
    /// clock verification and the generations 2.11 adds to it are not
    /// known here. `commitable` while this replica is the leader: no
    /// write is forwarded, so a follower orders nothing and answers
    /// [`Reason::NotCommitable`].
    fn replica(&self) -> ReplicaInfo {
        let metrics = self.raft.metrics().borrow().clone();
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        if !self.ready.load(Ordering::Relaxed) && self.store.caught_up() {
            self.ready.store(true, Ordering::Relaxed);
        }
        ReplicaInfo {
            id: self.settings.id.to_string(),
            role: format!("{:?}", metrics.state).to_lowercase(),
            leader: metrics.current_leader.map(|id| id.to_string()),
            in_sync: metrics.last_log_index.unwrap_or(0) <= applied,
            lag: None,
            ready: self.ready.load(Ordering::Relaxed),
            commitable: metrics.state == ServerState::Leader,
        }
    }

    fn epoch(&self) -> u64 {
        self.settings.epoch
    }

    /// The product revision ([`state_machine::StateMachine::revision`]), not the log
    /// index: a client checks that each delta follows the one before.
    fn revision(&self) -> u64 {
        self.store.with_state(|sm| sm.revision)
    }

    /// The product revision once the leader's read index, the log index
    /// a round of heartbeats to a majority confirms, is applied here.
    /// openraft refuses the check on a follower: one that knows a leader is
    /// [`ReadIndex::NotLeader`], one that knows none, or a leader without a
    /// majority, [`ReadIndex::CutOff`]. Nothing is forwarded to the leader
    /// yet.
    fn read_index(&self) -> BoxFuture<'_, ReadIndex> {
        Box::pin(async move {
            let read = match self.raft.get_read_log_id().await {
                Ok((read, _)) => read,
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)))
                    if forward.leader_id.is_some() =>
                {
                    return ReadIndex::NotLeader;
                }
                Err(_) => return ReadIndex::CutOff,
            };
            if let Some(read) = read
                && self
                    .raft
                    .wait(None)
                    .applied_index_at_least(Some(read.index), "read index")
                    .await
                    .is_err()
            {
                return ReadIndex::CutOff;
            }
            ReadIndex::Confirmed(self.revision())
        })
    }

    fn applied(&self, revision: u64) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // Metrics change with every entry applied, after the state
            // machine took it.
            let applied = |_: &_| self.revision() >= revision;
            if self
                .raft
                .wait(None)
                .metrics(applied, "applied")
                .await
                .is_err()
            {
                // Stopped: it never applies the revision.
                std::future::pending::<()>().await;
            }
        })
    }

    fn snapshot(&self) -> BoxFuture<'_, Snapshot> {
        let snapshot = self.store.with_state(|sm| Snapshot {
            epoch: self.settings.epoch,
            revision: sm.revision,
            text: sm.product.text().into(),
        });
        Box::pin(async move { snapshot })
    }

    fn deltas_since(&self, since: Position) -> BoxFuture<'_, Option<Vec<Delta>>> {
        let epoch = self.settings.epoch;
        let deltas = self.store.with_state(|sm| {
            if since.epoch != epoch || since.revision < sm.deltas_after {
                return if since.epoch == epoch && since.revision >= sm.revision {
                    Some(Vec::new())
                } else {
                    None
                };
            }
            Some(
                sm.deltas
                    .iter()
                    .filter(|change| change.revision > since.revision)
                    .map(|change| Delta {
                        epoch,
                        revision: change.revision,
                        prev: change.prev,
                        commands: change.commands.clone(),
                        text: change.text.clone().into(),
                    })
                    .collect(),
            )
        });
        Box::pin(async move { deltas })
    }

    fn command_resolution(&self, owner: &Owner, id: &str) -> Option<(u64, Resolution)> {
        self.store
            .with_state(|sm| sm.dedup.answer(&SessionKey::from(owner), id))
            .map(|(revision, resolution)| (revision, resolution_of(resolution)))
    }

    fn submit(
        &self,
        owner: &Owner,
        id: &str,
        command: axum::body::Bytes,
    ) -> BoxFuture<'_, Result<Ack, NotApplied>> {
        let (reply, replied) = oneshot::channel();
        let deadline = Instant::now() + self.settings.write_timeout;
        let queued = self.queue.send(Queued::Command(
            Command {
                origin: Origin::from(owner),
                id: id.to_owned(),
                at_index: None,
                body: command.to_vec(),
            },
            deadline,
            reply,
        ));
        Box::pin(async move {
            let stopped = || NotApplied::new(Reason::Unavailable, "replica stopped");
            queued.map_err(|_| stopped())?;
            match tokio::time::timeout_at(deadline, replied).await {
                Ok(replied) => replied.unwrap_or_else(|_| Err(stopped())),
                Err(_) => Err(NotApplied::new(Reason::Unavailable, "timed out")),
            }
        })
    }

    fn renew_sessions(&self, live: &[Owner]) {
        self.append(Body::LeaseRenew(
            live.iter().map(SessionKey::from).collect(),
        ));
    }
}
