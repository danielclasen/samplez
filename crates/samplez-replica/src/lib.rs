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
use openraft::network::RaftNetworkFactory;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft, ServerState};
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};
use samplez_service::ws::{Delta, Snapshot};
use samplez_service::{Owner, Position, ReplicaInfo, ReplicatedState};
use tokio::sync::{mpsc, oneshot};

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
    /// The longest a submit waits for its entry to apply here before it
    /// resolves [`NotApplied`] with [`Reason::Unavailable`].
    pub write_timeout: Duration,
}

/// This replica's estimate E of the set's processed index, in samples.
pub type Estimate = Arc<dyn Fn() -> u64 + Send + Sync>;

enum Queued {
    Command(Command, oneshot::Sender<Result<Ack, NotApplied>>),
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
        tokio::spawn(write::<P>(raft.clone(), queued, estimate, settings.clone()));
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
/// order they were submitted, and folds what queued while the previous
/// entry was written into the next one (group commit).
async fn write<P: Product>(
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
            Queued::Command(command, reply) => {
                let mut batch = Batch::default();
                let mut replies = Vec::new();
                let mut push = |command: Command, reply| {
                    let path = command.body.as_deref().and_then(P::coalesce_path);
                    replies.push((command.id.clone(), reply));
                    batch.push(command, path);
                };
                push(command, reply);
                while let Ok(item) = queued.try_recv() {
                    match item {
                        Queued::Command(command, reply) => push(command, reply),
                        entry => {
                            next = Some(entry);
                            break;
                        }
                    }
                }
                let written = tokio::time::timeout(
                    settings.write_timeout,
                    raft.client_write(request(Body::Commands(batch.into_commands()))),
                )
                .await;
                match written {
                    Ok(Ok(written)) => {
                        for ((id, reply), outcome) in replies.into_iter().zip(written.data.0) {
                            let _ = reply.send(Ok(Ack {
                                command: id,
                                revision: outcome.revision,
                                resolution: resolution_of(outcome.result),
                            }));
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
        entry::Resolution::Duplicate => Resolution::Duplicate,
        entry::Resolution::Superseded => Resolution::Superseded,
        entry::Resolution::Stale => Resolution::Stale,
    }
}

fn reply_all(
    replies: Vec<(String, oneshot::Sender<Result<Ack, NotApplied>>)>,
    reason: Reason,
    detail: &str,
) {
    for (_, reply) in replies {
        let _ = reply.send(Err(NotApplied::new(reason, detail)));
    }
}

impl<P: Product + Sync> ReplicatedState for Replica<P> {
    /// `ready` once the replica applied the log up to the committed id it
    /// knows, which after a restart is the saved one; it stays set. The
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

    fn revision(&self) -> u64 {
        self.store.applied_index()
    }

    fn snapshot(&self) -> BoxFuture<'_, Snapshot> {
        let snapshot = self.store.view(|revision, sm| Snapshot {
            epoch: self.settings.epoch,
            revision,
            text: sm.product.text().into(),
        });
        Box::pin(async move { snapshot })
    }

    fn deltas_since(&self, since: Position) -> BoxFuture<'_, Option<Vec<Delta>>> {
        let epoch = self.settings.epoch;
        let deltas = self.store.view(|revision, sm| {
            if since.epoch != epoch || since.revision < sm.deltas_after {
                return if since.epoch == epoch && since.revision >= revision {
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
                        command: Some(change.command.clone()),
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
        let queued = self.queue.send(Queued::Command(
            Command {
                origin: Origin::from(owner),
                id: id.to_owned(),
                at_index: None,
                body: Some(command.to_vec()),
            },
            reply,
        ));
        Box::pin(async move {
            let stopped = || NotApplied::new(Reason::Unavailable, "replica stopped");
            queued.map_err(|_| stopped())?;
            replied.await.unwrap_or_else(|_| Err(stopped()))
        })
    }

    fn renew_sessions(&self, live: &[Owner]) {
        self.append(Body::LeaseRenew(
            live.iter().map(SessionKey::from).collect(),
        ));
    }
}
