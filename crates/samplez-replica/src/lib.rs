//! A samplez product's state replicated over openraft: the log and
//! snapshots on the replica's volume, the per-session tables of applied
//! command ids, and [`ReplicatedState`] over a [`Product`].
//!
//! Replicas reach each other on the peer listener ([`peer`]) with mutual
//! TLS. A command a client sends to any replica is forwarded to the leader
//! with its owner and the replica's E estimate at first receipt; the
//! replica answers it once it applied the entry carrying it.

pub mod dedup;
pub mod entry;
pub mod log_store;
pub mod net;
pub mod peer;
pub mod snapshot;
pub mod state_machine;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
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
    Batch, Body, Command, NodeId, Origin, Outcome, Request, Response, SessionKey, stamping_index,
};
use crate::log_store::Store;
use crate::peer::{Forwarded, PeerClient, PeerReply, PeerRequest, PeerResponse, Refused};
use crate::state_machine::Product;

openraft::declare_raft_types!(
    /// The openraft types of a samplez replica set.
    pub TypeConfig:
        D = Request,
        R = Response,
);

/// The largest command body a replica orders; a larger one is refused
/// with [`Reason::TooLarge`].
pub const MAX_COMMAND_BYTES: usize = 32 << 10;

/// The command bytes ([`Command::size`]) one entry carries at most: a
/// group commit closes before the command that would pass it, so only a
/// single command can exceed it, by its id and owner.
pub const MAX_ENTRY_BYTES: usize = 32 << 10;

/// The entries one AppendEntries carries at most. openraft gives an
/// AppendEntries the heartbeat interval to complete; at this count and
/// [`MAX_ENTRY_BYTES`] one carries about 128 KiB of commands, a third
/// more encoded.
pub const MAX_PAYLOAD_ENTRIES: u64 = 4;

/// openraft's timers as the chart pins them (`replica.raft`): its 0.9.21
/// defaults, so the election figures do not move with a dependency
/// update; and [`MAX_PAYLOAD_ENTRIES`].
pub fn raft_config(cluster: &str) -> openraft::Config {
    openraft::Config {
        cluster_name: cluster.to_owned(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        max_payload_entries: MAX_PAYLOAD_ENTRIES,
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
    /// here, queueing behind earlier writes and waiting for a leader
    /// included, before it resolves [`NotApplied`] with
    /// [`Reason::Unavailable`]. A command still queued at its deadline,
    /// here or on the leader it was forwarded to, is not ordered. An entry
    /// the replica set decides on waits this long from the moment it is
    /// proposed.
    pub write_timeout: Duration,
    /// This replica's peer address, `host:port`, as the others reach it.
    pub addr: String,
}

/// This replica's estimate E of the set's processed index, in samples.
pub type Estimate = Arc<dyn Fn() -> u64 + Send + Sync>;

type Reply = oneshot::Sender<Result<Outcome, Refused>>;

/// A command waiting for the writer.
struct Item {
    command: Command,
    /// The E estimate of the replica that first received it.
    received: u64,
    deadline: Instant,
    reply: Reply,
    /// Forwarded by another replica: answered, never forwarded on.
    forwarded: bool,
}

enum Queued {
    Command(Item),
    Entry {
        body: Body,
        deadline: Instant,
        reply: Option<oneshot::Sender<Result<(), Refused>>>,
        forwarded: bool,
    },
}

/// Whether a read sees every write committed before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Applied up to the leader's read index.
    Current,
    /// The locally applied state: no quorum, or no read index in time.
    Stale,
}

/// What the writer and the peer handlers share.
pub(crate) struct Shared {
    settings: Settings,
    raft: Raft<TypeConfig>,
    peers: Arc<dyn PeerClient>,
    /// The longest a leader goes without a quorum, and a follower without
    /// its leader, before it counts the quorum as lost: the election
    /// timeout's maximum.
    lease: Duration,
    /// The pause before a refused forward is resent.
    heartbeat: Duration,
    quiescing: AtomicBool,
    /// Proposals between the leadership check and openraft's answer.
    proposing: AtomicUsize,
}

/// Counts a proposal while it lives.
struct Proposing<'a>(&'a AtomicUsize);

impl Drop for Proposing<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One replica of a product's state.
pub struct Replica<P> {
    shared: Arc<Shared>,
    store: Store<P>,
    queue: mpsc::UnboundedSender<Queued>,
    estimate: Estimate,
    /// Set once the replica applied the log up to the committed id it
    /// knows ([`ReplicaInfo::ready`]); nothing clears it yet.
    ready: AtomicBool,
}

impl<P: Product> Replica<P> {
    /// Opens the storage in `dir` and starts the replica, as
    /// [`Replica::with_store`].
    pub async fn start<N>(
        settings: Settings,
        raft: openraft::Config,
        dir: &Path,
        product: P,
        network: N,
        peers: Arc<dyn PeerClient>,
        estimate: Estimate,
    ) -> Result<Replica<P>, String>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let store = Store::open(dir, product, settings.limits).map_err(|e| e.to_string())?;
        Replica::with_store(settings, raft, store, network, peers, estimate).await
    }

    /// Starts the replica on `store`: a restarted one applies its log up to
    /// the saved committed id before it hears from any peer.
    pub async fn with_store<N>(
        settings: Settings,
        raft: openraft::Config,
        store: Store<P>,
        network: N,
        peers: Arc<dyn PeerClient>,
        estimate: Estimate,
    ) -> Result<Replica<P>, String>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let raft_config = Arc::new(raft.validate().map_err(|e| e.to_string())?);
        let (log, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(
            settings.id,
            raft_config.clone(),
            network,
            log,
            state_machine,
        )
        .await
        .map_err(|e| e.to_string())?;
        let shared = Arc::new(Shared {
            settings,
            raft,
            peers,
            lease: Duration::from_millis(raft_config.election_timeout_max),
            heartbeat: Duration::from_millis(raft_config.heartbeat_interval),
            quiescing: AtomicBool::new(false),
            proposing: AtomicUsize::new(0),
        });
        let (queue, queued) = mpsc::unbounded_channel();
        tokio::spawn(write(shared.clone(), queued, estimate.clone()));
        Ok(Replica {
            shared,
            store,
            queue,
            estimate,
            ready: AtomicBool::new(false),
        })
    }

    pub fn id(&self) -> NodeId {
        self.shared.settings.id
    }

    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.shared.raft
    }

    pub fn store(&self) -> &Store<P> {
        &self.store
    }

    /// Orders an entry the replica set decides on rather than a client,
    /// behind every command submitted before it.
    pub fn append(&self, body: Body) {
        let _ = self.queue.send(Queued::Entry {
            body,
            deadline: Instant::now() + self.shared.settings.write_timeout,
            reply: None,
            forwarded: false,
        });
    }

    /// Makes `members` the first voters; on one node of a new set only.
    pub async fn initialize(
        &self,
        members: impl IntoIterator<Item = (NodeId, String)>,
    ) -> Result<(), String> {
        let members: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.shared
            .raft
            .initialize(members)
            .await
            .map_err(|e| e.to_string())
    }

    /// Whether this replica knows a quorum: as leader, one acknowledged it
    /// within the lease; else its leader's last append arrived within it.
    pub async fn quorum(&self) -> bool {
        self.shared.quorum().await
    }

    /// Waits up to `within` until this replica applied every write
    /// committed before the call, and says whether it did. Without a
    /// quorum it answers [`Freshness::Stale`] at once, so a reader never
    /// waits on a set that cannot answer.
    pub async fn read(&self, within: Duration) -> Freshness {
        let shared = &self.shared;
        if !shared.quorum().await {
            return Freshness::Stale;
        }
        let index = async {
            if shared.leads() {
                let (read, _) = shared.raft.get_read_log_id().await.ok()?;
                return Some(read.map_or(0, |id| id.index));
            }
            let (leader, addr) = shared.leader()?;
            match shared
                .peers
                .call(Some(leader), &addr, PeerRequest::ReadIndex, within)
                .await
            {
                Ok(Ok(PeerResponse::ReadIndex(index))) => Some(index),
                _ => None,
            }
        };
        let deadline = Instant::now() + within;
        match tokio::time::timeout_at(deadline, index).await {
            Ok(Some(index)) if shared.applied(index, deadline).await => Freshness::Current,
            _ => Freshness::Stale,
        }
    }

    /// For a voter's preStop. Stops ordering commands: forwarded ones are
    /// refused [`Refused::Quiescing`], which their sender resends to the
    /// next leader, and local ones wait for that leader. A leader then
    /// waits until no proposal is in progress, every entry it appended is
    /// committed and every voter holds it, and that still holds two
    /// heartbeats later, at most `bound`; so its exit leaves no entry in
    /// flight on a live replica, and a voter that is down holds it up to
    /// `bound`.
    pub async fn quiesce(&self, bound: Duration) {
        let shared = &self.shared;
        shared.quiescing.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + bound;
        let mut settled = false;
        while shared.raft.metrics().borrow().state == ServerState::Leader {
            let metrics = shared.raft.metrics().borrow().clone();
            let last = metrics.last_log_index.unwrap_or(0);
            let committed = metrics.last_applied.is_some_and(|id| id.index >= last);
            let held = metrics
                .membership_config
                .membership()
                .voter_ids()
                .all(|id| {
                    id == shared.settings.id
                        || metrics
                            .replication
                            .as_ref()
                            .and_then(|matched| matched.get(&id).copied().flatten())
                            .is_some_and(|matched| matched.index >= last)
                });
            let quiet = shared.proposing.load(Ordering::SeqCst) == 0 && committed && held;
            if quiet && settled {
                return;
            }
            settled = quiet;
            if Instant::now() >= deadline {
                return;
            }
            // Quiet: one more heartbeat carries the commit to the
            // followers, and a proposal racing the flag shows by then.
            let pause = if quiet {
                shared.heartbeat * 2
            } else {
                shared.heartbeat / 5
            };
            tokio::time::sleep(pause.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }

    /// Answers another replica's request; the peer listener calls it.
    pub async fn handle(&self, to: Option<NodeId>, request: PeerRequest) -> PeerReply {
        let shared = &self.shared;
        if to.is_some_and(|to| to != shared.settings.id) {
            return Err(Refused::Misaddressed);
        }
        match request {
            PeerRequest::Forward { commands } => {
                shared.refuse_unless_leading()?;
                let now = Instant::now();
                let mut replies = Vec::with_capacity(commands.len());
                for Forwarded {
                    command,
                    received,
                    within_ms,
                } in commands
                {
                    let (reply, replied) = oneshot::channel();
                    let item = Item {
                        command,
                        received,
                        deadline: now + Duration::from_millis(within_ms),
                        reply,
                        forwarded: true,
                    };
                    let _ = self.queue.send(Queued::Command(item));
                    replies.push(replied);
                }
                let mut results = Vec::with_capacity(replies.len());
                for replied in replies {
                    results.push(replied.await.unwrap_or_else(|_| Err(stopped())));
                }
                Ok(PeerResponse::Forwarded(results))
            }
            PeerRequest::Append { body, within_ms } => {
                // A peer orders its lease renewals; every other entry is
                // the leader's own decision.
                if !matches!(body, Body::LeaseRenew(_)) {
                    return Err(Refused::NotForwardable);
                }
                shared.refuse_unless_leading()?;
                let (reply, replied) = oneshot::channel();
                let _ = self.queue.send(Queued::Entry {
                    body,
                    deadline: Instant::now() + Duration::from_millis(within_ms),
                    reply: Some(reply),
                    forwarded: true,
                });
                replied.await.unwrap_or_else(|_| Err(stopped()))?;
                Ok(PeerResponse::Appended)
            }
            PeerRequest::ReadIndex => match shared.raft.get_read_log_id().await {
                Ok((read, _)) => Ok(PeerResponse::ReadIndex(read.map_or(0, |id| id.index))),
                Err(RaftError::APIError(_)) => Err(Refused::NotLeader),
                Err(e) => Err(unavailable(&e)),
            },
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.shared.raft.shutdown().await;
    }
}

fn stopped() -> Refused {
    Refused::Unavailable("replica stopped".into())
}

/// openraft's error, logged; its text names nodes and addresses, so it
/// stays out of what a client is told.
fn unavailable(e: &dyn std::fmt::Display) -> Refused {
    tracing::debug!(error = %e, "not committed");
    Refused::Unavailable("not committed".into())
}

impl Shared {
    fn quiescing(&self) -> bool {
        self.quiescing.load(Ordering::SeqCst)
    }

    /// Counts a proposal and says whether this replica leads; the count
    /// is taken first, so a [`Replica::quiesce`] that set its flag after
    /// this check sees the proposal.
    fn propose(&self) -> Option<Proposing<'_>> {
        self.proposing.fetch_add(1, Ordering::SeqCst);
        let proposing = Proposing(&self.proposing);
        self.leads().then_some(proposing)
    }

    /// Why a replica that does not lead refuses: the leader quiescing, or
    /// not the leader.
    fn refusal(&self) -> Refused {
        if self.quiescing() {
            Refused::Quiescing
        } else {
            Refused::NotLeader
        }
    }

    /// Leader and not quiescing: orders what it is given.
    fn leads(&self) -> bool {
        self.raft.metrics().borrow().state == ServerState::Leader && !self.quiescing()
    }

    fn refuse_unless_leading(&self) -> Result<(), Refused> {
        match self.raft.metrics().borrow().state {
            ServerState::Leader if self.quiescing() => Err(Refused::Quiescing),
            ServerState::Leader => Ok(()),
            _ => Err(Refused::NotLeader),
        }
    }

    /// Another replica this one knows as leader, with its address.
    fn leader(&self) -> Option<(NodeId, String)> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader = metrics
            .current_leader
            .filter(|&id| id != self.settings.id)?;
        let node = metrics
            .membership_config
            .membership()
            .get_node(&leader)?
            .clone();
        Some((leader, node.addr))
    }

    async fn quorum(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        match metrics.state {
            ServerState::Leader => metrics
                .millis_since_quorum_ack
                .is_some_and(|ms| u128::from(ms) <= self.lease.as_millis()),
            ServerState::Follower | ServerState::Learner if metrics.current_leader.is_some() => {
                let lease = self.lease;
                self.raft
                    .with_raft_state(move |state| {
                        state.vote_ref().committed
                            && state
                                .vote_last_modified()
                                .is_some_and(|at| at.elapsed() <= lease)
                    })
                    .await
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    /// Waits until this replica applied `index`; false at `deadline`.
    async fn applied(&self, index: u64, deadline: Instant) -> bool {
        let within = deadline.saturating_duration_since(Instant::now());
        self.raft
            .wait(Some(within))
            .applied_index_at_least(Some(index), "applied")
            .await
            .is_ok()
    }

    /// Orders an entry: appends it as leader, else forwards it to the
    /// leader, until `deadline`.
    async fn order_entry(
        &self,
        body: Body,
        deadline: Instant,
        forwarded: bool,
        estimate: &Estimate,
    ) -> Result<(), Refused> {
        loop {
            if let Some(_proposing) = self.propose() {
                let request = Request {
                    stamp: stamping_index(estimate(), self.settings.block),
                    body,
                };
                return match tokio::time::timeout_at(deadline, self.raft.client_write(request))
                    .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(unavailable(&e)),
                    Err(_) => Err(Refused::Unavailable("timed out".into())),
                };
            }
            if forwarded {
                return Err(self.refusal());
            }
            if let Some((leader, addr)) = self.leader() {
                let within = deadline.saturating_duration_since(Instant::now());
                let request = PeerRequest::Append {
                    body: body.clone(),
                    within_ms: within.as_millis() as u64,
                };
                if let Ok(Ok(PeerResponse::Appended)) =
                    self.peers.call(Some(leader), &addr, request, within).await
                {
                    return Ok(());
                }
            }
            if Instant::now() + self.heartbeat >= deadline {
                return Err(Refused::Unavailable("no leader".into()));
            }
            tokio::time::sleep(self.heartbeat).await;
        }
    }

    /// Orders a group of commands: as leader in one entry, else by
    /// forwarding them to the leader, resending what a leader change or a
    /// quiescing leader refused, each until its deadline. Answers each
    /// once this replica applied the entry carrying it.
    async fn order_commands(&self, mut items: Vec<Item>, estimate: &Estimate) {
        loop {
            let now = Instant::now();
            items.retain_mut(|item| {
                let expired = item.deadline <= now;
                if expired {
                    let (reply, _) = oneshot::channel();
                    let reply = std::mem::replace(&mut item.reply, reply);
                    let _ = reply.send(Err(Refused::Unavailable("timed out".into())));
                }
                !expired
            });
            if items.is_empty() {
                return;
            }
            if let Some(proposing) = self.propose() {
                let written = self.write_locally(items, estimate).await;
                drop(proposing);
                match written {
                    Ok(()) => return,
                    // Leadership moved before the entry committed: route
                    // them again; a copy that commits after all resolves
                    // as a duplicate.
                    Err(again) => items = again,
                }
                continue;
            }
            let (forwarded, local): (Vec<Item>, Vec<Item>) =
                items.into_iter().partition(|item| item.forwarded);
            let refusal = self.refusal();
            for item in forwarded {
                let _ = item.reply.send(Err(refusal.clone()));
            }
            items = local;
            if items.is_empty() {
                return;
            }
            if let Some((leader, addr)) = self.leader() {
                items = self.forward(leader, &addr, items).await;
                if items.is_empty() {
                    return;
                }
            }
            tokio::time::sleep(self.heartbeat).await;
        }
    }

    /// Appends `items` as one entry and answers them; gives them back when
    /// this replica turned out not to lead.
    async fn write_locally(&self, items: Vec<Item>, estimate: &Estimate) -> Result<(), Vec<Item>> {
        let mut batch = Batch::default();
        let latest = items
            .iter()
            .map(|item| item.deadline)
            .max()
            .expect("not empty");
        let positions: Vec<usize> = items
            .iter()
            .map(|item| batch.push(item.command.clone()))
            .collect();
        let request = Request {
            stamp: stamping_index(estimate(), self.settings.block),
            body: Body::Commands(batch.into_commands()),
        };
        let written = tokio::time::timeout_at(latest, self.raft.client_write(request)).await;
        let outcomes = match written {
            Ok(Ok(written)) => written.data.0,
            Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))) => {
                return Err(items);
            }
            Ok(Err(e)) => return answer_all(items, unavailable(&e)),
            Err(_) => return answer_all(items, Refused::Unavailable("timed out".into())),
        };
        let mut answered = vec![false; outcomes.len()];
        for (item, at) in items.into_iter().zip(positions) {
            let mut outcome = outcomes[at];
            if answered[at]
                && matches!(
                    outcome.result,
                    entry::Resolution::Applied | entry::Resolution::Unchanged
                )
            {
                outcome.result = entry::Resolution::Duplicate;
            }
            answered[at] = true;
            let _ = item.reply.send(Ok(outcome));
        }
        Ok(())
    }

    /// Forwards `items` to `leader` and answers what it ordered once
    /// applied here; gives back what to resend.
    async fn forward(&self, leader: NodeId, addr: &str, items: Vec<Item>) -> Vec<Item> {
        let latest = items
            .iter()
            .map(|item| item.deadline)
            .max()
            .expect("not empty");
        let now = Instant::now();
        let within = latest.saturating_duration_since(now);
        let request = PeerRequest::Forward {
            commands: items
                .iter()
                .map(|item| Forwarded {
                    command: item.command.clone(),
                    received: item.received,
                    within_ms: item.deadline.saturating_duration_since(now).as_millis() as u64,
                })
                .collect(),
        };
        let results = match self.peers.call(Some(leader), addr, request, within).await {
            Ok(Ok(PeerResponse::Forwarded(results))) if results.len() == items.len() => results,
            _ => return items,
        };
        let mut again = Vec::new();
        for (item, result) in items.into_iter().zip(results) {
            match result {
                Ok(outcome) => {
                    let applied = self.applied(outcome.revision, item.deadline).await;
                    let result = if applied {
                        Ok(outcome)
                    } else {
                        Err(Refused::Unavailable("timed out".into()))
                    };
                    let _ = item.reply.send(result);
                }
                Err(Refused::NotLeader | Refused::Quiescing | Refused::Misaddressed) => {
                    again.push(item)
                }
                Err(refused) => {
                    let _ = item.reply.send(Err(refused));
                }
            }
        }
        again
    }
}

fn answer_all(items: Vec<Item>, refused: Refused) -> Result<(), Vec<Item>> {
    for item in items {
        let _ = item.reply.send(Err(refused.clone()));
    }
    Ok(())
}

/// The only writer: takes the queue in order, so commands commit in the
/// order they were submitted, and puts what queued while the previous
/// entry was written into the next one (group commit). A copy of a
/// command the batch holds already is folded into it and answered from
/// its outcome, an applied one as a duplicate. An entry carries at most
/// [`MAX_ENTRY_BYTES`] of commands. On a follower the group is
/// forwarded to the leader instead, one group at a time, so the order
/// holds there too.
///
/// A first cut: one entry is in flight at a time and the writer waits for
/// it to commit and apply here before it proposes the next, so the
/// batching window is one commit round-trip, not one log append and
/// fsync, and a command that arrives just after a write starts waits for
/// up to two commits.
async fn write(
    shared: Arc<Shared>,
    mut queued: mpsc::UnboundedReceiver<Queued>,
    estimate: Estimate,
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
        match first {
            Queued::Entry {
                body,
                deadline,
                reply,
                forwarded,
            } => {
                let result = shared
                    .order_entry(body, deadline, forwarded, &estimate)
                    .await;
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            Queued::Command(item) => {
                let mut bytes = item.command.size();
                let mut items = vec![item];
                while let Ok(queued) = queued.try_recv() {
                    match queued {
                        Queued::Command(item) if bytes + item.command.size() <= MAX_ENTRY_BYTES => {
                            bytes += item.command.size();
                            items.push(item);
                        }
                        other => {
                            next = Some(other);
                            break;
                        }
                    }
                }
                shared.order_commands(items, &estimate).await;
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

impl<P: Product + Sync> ReplicatedState for Replica<P> {
    /// `ready` once the replica has joined a set, a committed id saved on
    /// its volume, and applied the log up to that id, which after a
    /// restart is the saved one; it stays set. An empty volume is not
    /// ready until it has joined, a saved vote alone included. The
    /// clock verification and the generations 2.11 adds to it are not
    /// known here. `commitable` once ready on a voter: it forwards what
    /// it does not order itself; a learner answers
    /// [`Reason::NotCommitable`].
    fn replica(&self) -> ReplicaInfo {
        let metrics = self.shared.raft.metrics().borrow().clone();
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        if !self.ready.load(Ordering::Relaxed) && self.store.caught_up() {
            self.ready.store(true, Ordering::Relaxed);
        }
        let ready = self.ready.load(Ordering::Relaxed);
        let voter = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == self.id());
        ReplicaInfo {
            id: self.id().to_string(),
            role: format!("{:?}", metrics.state).to_lowercase(),
            leader: metrics.current_leader.map(|id| id.to_string()),
            in_sync: metrics.last_log_index.unwrap_or(0) <= applied,
            lag: None,
            ready,
            commitable: ready && voter,
        }
    }

    fn epoch(&self) -> u64 {
        self.shared.settings.epoch
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
            let read = match self.shared.raft.get_read_log_id().await {
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
                    .shared
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
                .shared
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
        let epoch = self.epoch();
        let snapshot = self.store.with_state(|sm| Snapshot {
            epoch,
            revision: sm.revision,
            text: sm.product.text().into(),
        });
        Box::pin(async move { snapshot })
    }

    fn deltas_since(&self, since: Position) -> BoxFuture<'_, Option<Vec<Delta>>> {
        let epoch = self.epoch();
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

    /// The command is stamped with this replica's E estimate at receipt,
    /// which travels with it when it is forwarded.
    fn submit(
        &self,
        owner: &Owner,
        id: &str,
        command: axum::body::Bytes,
    ) -> BoxFuture<'_, Result<Ack, NotApplied>> {
        if command.len() > MAX_COMMAND_BYTES {
            let refused = NotApplied::new(
                Reason::TooLarge,
                format!("{} bytes, at most {MAX_COMMAND_BYTES}", command.len()),
            );
            return Box::pin(async move { Err(refused) });
        }
        let info = self.replica();
        if !info.commitable {
            let refused = NotApplied::new(Reason::NotCommitable, "not a voter yet");
            return Box::pin(async move { Err(refused) });
        }
        let (reply, replied) = oneshot::channel();
        let deadline = Instant::now() + self.shared.settings.write_timeout;
        let item = Item {
            command: Command {
                origin: Origin::from(owner),
                id: id.to_owned(),
                at_index: None,
                body: command.to_vec(),
            },
            received: (self.estimate)(),
            deadline,
            reply,
            forwarded: false,
        };
        let queued = self.queue.send(Queued::Command(item));
        let id = id.to_owned();
        Box::pin(async move {
            let stopped = || NotApplied::new(Reason::Unavailable, "replica stopped");
            queued.map_err(|_| stopped())?;
            let replied = match tokio::time::timeout_at(deadline, replied).await {
                Ok(replied) => replied.unwrap_or_else(|_| Err(self::stopped())),
                Err(_) => Err(Refused::Unavailable("timed out".into())),
            };
            match replied {
                Ok(outcome) => Ok(Ack::new(
                    id,
                    outcome.revision,
                    resolution_of(outcome.result),
                )),
                Err(Refused::Unavailable(detail)) => {
                    Err(NotApplied::new(Reason::Unavailable, detail))
                }
                Err(_) => Err(NotApplied::new(Reason::Unavailable, "no leader")),
            }
        })
    }

    fn renew_sessions(&self, live: &[Owner]) {
        self.append(Body::LeaseRenew(
            live.iter().map(SessionKey::from).collect(),
        ));
    }
}
