//! A samplez product's state replicated over openraft: the log and
//! snapshots on the replica's volume, the per-session tables of applied
//! command ids, and [`ReplicatedState`] over a [`Product`].
//!
//! Replicas reach each other on the peer listener ([`peer`]) with mutual
//! TLS. A command a client sends to any replica is forwarded to the leader
//! with its owner and the replica's E estimate at first receipt; the
//! replica answers it once it applied the entry carrying it. A set starts
//! from the marker ConfigMap ([`bootstrap`]): ordinal 0 initialises it,
//! every other volume joins as a learner, and the leader reconciles the
//! voters with the wanted ones ([`membership`]).

pub mod baseline;
pub mod bootstrap;
pub mod bridge;
pub mod clock;
pub mod contact;
pub mod decide;
pub mod dedup;
pub mod entry;
pub mod log_store;
pub mod membership;
pub mod net;
pub mod peer;
pub mod rank;
pub mod report;
pub mod schedule;
pub mod session;
pub mod snapshot;
pub mod state;
pub mod state_machine;
pub mod verdict;
pub mod withhold;

use std::collections::{BTreeMap, VecDeque};
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::network::RaftNetworkFactory;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft, ServerState};
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};
use samplez_service::ws::{Delta, Snapshot};
use samplez_service::{Owner, Position, ReadIndex, ReplicaInfo, ReplicatedState};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

use crate::bootstrap::Bootstrap;
use crate::dedup::Limits;
use crate::entry::{
    Batch, Body, Command, NodeId, Origin, Outcome, Request, Response, SessionKey, stamping_index,
};
use crate::log_store::Store;
use crate::membership::{Slot, Voters, slot_of};
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
        // Entries kept behind a snapshot when the log is purged, in
        // batches of one: a follower a little behind catches up from the
        // log, and a purge stays clear of entries replication is still
        // sending. openraft 0.9's defaults, pinned so a dependency bump
        // cannot move them; 0 only in tests.
        max_in_snapshot_log_to_keep: 1000,
        purge_batch_size: 1,
        ..openraft::Config::default()
    }
}

/// A replica's settings beside openraft's.
#[derive(Debug, Clone)]
pub struct Settings {
    pub id: NodeId,
    /// The id of the state's history, which revisions are named within:
    /// the cluster id ([`bootstrap`]); 0 on an empty volume that has not
    /// read the marker yet.
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
    /// The peer addresses of every voter the set wants, which a new volume
    /// asks to take it in.
    pub peers: Vec<String>,
    /// The voters the set wants, which the leader reconciles the
    /// membership with; `None` leaves the membership as it is.
    pub voters: Option<Voters>,
}

/// The longest a follower waits for its leader's read index before it
/// answers [`ReadIndex::NotLeader`].
pub const READ_INDEX_ASK: Duration = Duration::from_secs(1);

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

pub(crate) enum Queued {
    Command(Item),
    Entry {
        body: Body,
        deadline: Instant,
        reply: Option<oneshot::Sender<Result<(), Refused>>>,
        forwarded: bool,
    },
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
    /// The product revision this replica applied.
    revision: Box<dyn Fn() -> u64 + Send + Sync>,
    /// The cluster id: [`Settings::epoch`] until an empty volume takes on
    /// the marker's.
    epoch: AtomicU64,
    /// A leader counted this replica as a voter within the last
    /// [`membership::UNCOUNTED`].
    counted: AtomicBool,
    started: Instant,
    status: Mutex<Option<String>>,
    reconcile: Notify,
    /// Side reports and the contact fence, once started.
    contact: OnceLock<Arc<contact::Contact>>,
    /// The E estimate at first receipt of the commands clients sent here.
    receipts: Mutex<Receipts>,
}

/// First-receipt stamps of client commands, per session, oldest first:
/// at most `maxSessionCommands` a session and `maxDedupEntries` in all,
/// each kept until `2 x dedupRetentionS` old.
#[derive(Default)]
struct Receipts {
    sessions: BTreeMap<SessionKey, VecDeque<(String, u64)>>,
    total: usize,
}

impl Receipts {
    /// The stamp held for (`session`, `id`), else `e` recorded as its
    /// first receipt.
    fn stamp(&mut self, session: SessionKey, id: &str, e: u64, limits: &Limits) -> u64 {
        let keep = 2 * limits.retention;
        let mut total = self.total;
        self.sessions.retain(|_, held| {
            while held
                .front()
                .is_some_and(|&(_, at)| at.saturating_add(keep) < e)
            {
                held.pop_front();
                total -= 1;
            }
            !held.is_empty()
        });
        self.total = total;
        let held = self.sessions.entry(session).or_default();
        if let Some(&(_, at)) = held.iter().find(|(held, _)| held == id) {
            return at;
        }
        if held.len() >= limits.max_session_commands {
            held.pop_front();
            self.total -= 1;
        }
        held.push_back((id.to_owned(), e));
        self.total += 1;
        if self.total > limits.max_dedup_entries {
            // The oldest stamp of all goes.
            let oldest = self
                .sessions
                .iter()
                .filter_map(|(k, h)| h.front().map(|&(_, at)| (at, k.clone())))
                .min();
            if let Some((_, k)) = oldest
                && let Some(h) = self.sessions.get_mut(&k)
            {
                h.pop_front();
                self.total -= 1;
                if h.is_empty() {
                    self.sessions.remove(&k);
                }
            }
        }
        e
    }
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
    /// Generation-needing entries, ordered apart: one waits for the
    /// previous one's index to pass, which must hold up no other write.
    generations: mpsc::UnboundedSender<Queued>,
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
        let voters = settings.voters;
        let settings_epoch = settings.epoch;
        let shared = Arc::new(Shared {
            settings,
            raft,
            peers,
            lease: Duration::from_millis(raft_config.election_timeout_max),
            heartbeat: Duration::from_millis(raft_config.heartbeat_interval),
            quiescing: AtomicBool::new(false),
            proposing: AtomicUsize::new(0),
            revision: {
                let store = store.clone();
                Box::new(move || store.with_state(|sm| sm.revision))
            },
            epoch: AtomicU64::new(settings_epoch),
            counted: AtomicBool::new(false),
            started: Instant::now(),
            status: Mutex::default(),
            reconcile: Notify::new(),
            contact: OnceLock::new(),
            receipts: Mutex::default(),
        });
        let (queue, queued) = mpsc::unbounded_channel();
        tokio::spawn(write(shared.clone(), queued, estimate.clone()));
        let (generations, waiting) = mpsc::unbounded_channel();
        tokio::spawn(write(shared.clone(), waiting, estimate.clone()));
        if let Some(voters) = voters {
            tokio::spawn(membership::reconcile(shared.clone(), voters));
        }
        tokio::spawn(session::expire(
            shared.clone(),
            store.clone(),
            queue.clone(),
            estimate.clone(),
        ));
        Ok(Replica {
            shared,
            store,
            queue,
            generations,
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

    /// Why the replica is not part of a set yet, when it is not.
    pub fn status(&self) -> Option<String> {
        self.shared
            .status
            .lock()
            .expect("status lock poisoned")
            .clone()
    }

    /// Orders an entry the replica set decides on rather than a client,
    /// behind every command submitted before it; a generation-needing
    /// entry behind the earlier generation-needing ones only.
    pub fn append(&self, body: Body) {
        let queue = if body.needs_generation() {
            &self.generations
        } else {
            &self.queue
        };
        let _ = queue.send(Queued::Entry {
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

    /// Keeps the replica in its set ([`membership::watch`]): an empty
    /// volume starts or joins the set the marker names, a replica no
    /// leader counts asks to join again.
    pub fn begin(&self, bootstrap: Bootstrap) {
        tokio::spawn(membership::watch(self.shared.clone(), bootstrap));
    }

    /// Starts contact with the other voters ([`contact`]): side reports
    /// each report period, F published to `audio`, entries handed to it as
    /// they are appended and known committed, and unresolved ones once
    /// their apply deadline passes. The witness passes no `audio`.
    pub fn contact(
        &self,
        timing: contact::Timing,
        clocks: Arc<dyn contact::Clocks>,
        audio: Option<Arc<dyn contact::Audio>>,
    ) -> Result<(), String> {
        let first_seq = bootstrap::random_id().map_err(|e| e.to_string())? >> 1;
        let contact = Arc::new(contact::Contact::new(
            self.id(),
            timing,
            clocks,
            audio,
            Arc::new(self.store.clone()),
            first_seq,
        ));
        self.shared
            .contact
            .set(contact.clone())
            .map_err(|_| "contact runs already".to_owned())?;
        tokio::spawn(contact::run(
            self.shared.clone(),
            contact,
            self.queue.clone(),
        ));
        Ok(())
    }

    /// Contact as status shows it, once started.
    pub fn contact_metrics(&self) -> Option<contact::Metrics> {
        self.shared.contact.get().map(|c| c.metrics())
    }

    /// Whether this replica knows a quorum: as leader, one acknowledged it
    /// within the lease; else its leader's last append arrived within it.
    pub async fn quorum(&self) -> bool {
        self.shared.quorum().await
    }

    /// For a voter's preStop. Stops ordering commands: forwarded ones are
    /// refused [`Refused::Quiescing`], which their sender resends to the
    /// next leader, and local ones wait for that leader. A leader then
    /// waits until no proposal is in progress, every entry it appended is
    /// committed, every voter holds it and, with contact running, every
    /// voter heard within `w + D` reports it committed, and that still holds two
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
            // Every voter heard within w + D reports the commit too.
            let reported = shared.contact.get().is_none_or(|c| {
                let t = c.timing();
                c.heard_committed(last, t.contact_window + t.hold)
            });
            let quiet =
                shared.proposing.load(Ordering::SeqCst) == 0 && committed && held && reported;
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
            PeerRequest::Join { id, addr, cluster } => shared.admit(id, addr, cluster).await,
            PeerRequest::Report(report) => {
                if let Some(contact) = shared.contact.get() {
                    contact.receive(*report);
                }
                Ok(PeerResponse::Reported)
            }
            PeerRequest::Member => Ok(PeerResponse::Member(
                shared.raft.is_initialized().await.unwrap_or(false),
            )),
            PeerRequest::ReadIndex => match shared.raft.get_read_log_id().await {
                Ok((read, _)) => Ok(PeerResponse::ReadIndex(read.map_or(0, |id| id.index))),
                Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                    Err(Refused::CutOff)
                }
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
    /// E: contact's estimate once contact runs, none while it counts no
    /// audio replica; before, the estimate the replica was started with.
    fn e(&self, estimate: &Estimate) -> Option<u64> {
        match self.contact.get() {
            Some(c) => c.estimate(),
            None => Some(estimate()),
        }
    }

    /// The oldest a command's first-receipt stamp may lie below E and
    /// still be ordered: `dedupRetentionS - ageGuard`.
    fn max_age(&self) -> u64 {
        let guard = self.contact.get().map_or(0, |c| c.age_guard());
        self.settings.limits.retention.saturating_sub(guard)
    }

    /// Where the leader stamps an entry, as a refusal of what it carries
    /// when it stamps none.
    fn stamp(
        &self,
        generation: bool,
        estimate: &Estimate,
    ) -> Result<contact::Stamp, contact::StampRefusal> {
        match self.contact.get() {
            Some(c) => c.stamp(generation),
            None => {
                let stamp = stamping_index(estimate(), self.settings.block);
                Ok(contact::Stamp {
                    stamp,
                    earliest: stamp,
                })
            }
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

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

    /// Waits until this replica applied product revision `revision`;
    /// false at `deadline`. Metrics change with every entry applied, after
    /// the state machine took it.
    async fn applied(&self, revision: u64, deadline: Instant) -> bool {
        let within = deadline.saturating_duration_since(Instant::now());
        self.raft
            .wait(Some(within))
            .metrics(|_| (self.revision)() >= revision, "applied")
            .await
            .is_ok()
    }

    /// The log index a read must have applied to see every write
    /// committed before it: the leader's, confirmed by a round of
    /// heartbeats to a majority; a follower asks its leader for it. The
    /// error is what a reader is told instead.
    async fn read_log_index(&self) -> Result<u64, ReadIndex> {
        if !self.quorum().await {
            return Err(ReadIndex::CutOff);
        }
        if self.raft.metrics().borrow().state == ServerState::Leader {
            return match self.raft.get_read_log_id().await {
                Ok((read, _)) => Ok(read.map_or(0, |id| id.index)),
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)))
                    if forward.leader_id.is_some() =>
                {
                    Err(ReadIndex::NotLeader)
                }
                Err(_) => Err(ReadIndex::CutOff),
            };
        }
        let (leader, addr) = self.leader().ok_or(ReadIndex::CutOff)?;
        let asked = self
            .peers
            .call(Some(leader), &addr, PeerRequest::ReadIndex, READ_INDEX_ASK)
            .await;
        match asked {
            Ok(Ok(PeerResponse::ReadIndex(index))) => Ok(index),
            Ok(Err(Refused::CutOff)) => Err(ReadIndex::CutOff),
            _ => Err(ReadIndex::NotLeader),
        }
    }

    /// Makes this replica the first voter of a new set.
    async fn initialize_alone(&self) -> Result<(), String> {
        let own = BTreeMap::from([(self.settings.id, BasicNode::new(&self.settings.addr))]);
        self.raft.initialize(own).await.map_err(|e| e.to_string())
    }

    fn set_status(&self, status: Option<String>) {
        *self.status.lock().expect("status lock poisoned") = status;
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
                let stamped = self.stamp(body.needs_generation(), estimate);
                let stamp = match stamped {
                    Ok(stamp) => stamp,
                    // One pending generation at a time: wait for it.
                    Err(contact::StampRefusal::GenerationPending)
                        if Instant::now() + self.heartbeat < deadline =>
                    {
                        drop(_proposing);
                        tokio::time::sleep(self.heartbeat / 10).await;
                        continue;
                    }
                    Err(refusal) => return Err(stamp_refused(refusal)),
                };
                let mut body = body;
                // A later index asked for is kept.
                if let Some(at) = body.at_index() {
                    state_machine::restamp(&mut body, at.max(stamp.earliest));
                }
                if let Body::Baseline(b) = &mut body {
                    b.at_index = b.at_index.max(stamp.earliest);
                }
                let request = Request {
                    stamp: stamp.stamp,
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
        let stamp = match self.stamp(false, estimate) {
            Ok(stamp) => stamp,
            Err(refusal) => return answer_all(items, stamp_refused(refusal)),
        };
        // A command first received longer ago than the set deduplicates
        // is never ordered: E's frame, no TAI clock.
        let e = self.e(estimate).unwrap_or(0);
        let max_age = self.max_age();
        let (items, expired): (Vec<Item>, Vec<Item>) = items
            .into_iter()
            .partition(|item| item.received.saturating_add(max_age) >= e);
        for item in expired {
            let _ = item.reply.send(Err(Refused::Expired));
        }
        if items.is_empty() {
            return Ok(());
        }
        let mut batch = Batch::default();
        let latest = items
            .iter()
            .map(|item| item.deadline)
            .max()
            .expect("not empty");
        let positions: Vec<usize> = items
            .iter()
            .map(|item| {
                let mut command = item.command.clone();
                // A later index asked for is kept, an earlier or absent one
                // is the earliest.
                if self.contact.get().is_some() {
                    command.at_index = Some(command.at_index.unwrap_or(0).max(stamp.earliest));
                }
                batch.push(command)
            })
            .collect();
        let request = Request {
            stamp: stamp.stamp,
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

fn stamp_refused(refusal: contact::StampRefusal) -> Refused {
    Refused::Unavailable(
        match refusal {
            contact::StampRefusal::NobodyCounted => "clock_unverified",
            contact::StampRefusal::FutureListFull => "future_list_full",
            contact::StampRefusal::GenerationPending => "generation pending",
        }
        .into(),
    )
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
    /// known here. With contact running the witness is ready only while
    /// it is in contact, and an audio replica of a set of more than one
    /// voter only as [`contact::Contact::serving`] says; `commitable`
    /// does not take that term. `commitable` once ready on a voter other than the
    /// witness that a leader counts as a voter, now or within the last
    /// [`membership::UNCOUNTED`]: it forwards what it does not
    /// order itself. The only voter of its membership counts itself while
    /// it leads, as its own quorum, also without [`Replica::begin`]. A
    /// learner, the witness, which serves no clients, and a replica no
    /// leader counts, such as one removed from the voters while it was
    /// down, answer [`Reason::NotCommitable`]. So does, with contact
    /// running, an audio replica E does not count
    /// ([`contact::Contact::counts_self`]): clock-unverified, judged by a
    /// `ClockVerdict`, fenced on a detected clock step, or flagged by a
    /// self-report while E counts a replica free of flags; with E
    /// counting nobody every replica does.
    fn replica(&self) -> ReplicaInfo {
        let metrics = self.shared.raft.metrics().borrow().clone();
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        if !self.ready.load(Ordering::Relaxed) && self.store.caught_up() {
            self.ready.store(true, Ordering::Relaxed);
        }
        // The witness is ready only while in contact, and never before
        // its first contact.
        let witness = slot_of(self.id()) == Slot::Witness;
        let ready = self.ready.load(Ordering::Relaxed)
            && !(witness && self.shared.contact.get().is_some_and(|c| !c.in_contact()));
        let voters: Vec<NodeId> = metrics.membership_config.membership().voter_ids().collect();
        let voter = voters.contains(&self.id());
        let alone = voters == [self.id()] && metrics.state == ServerState::Leader;
        ReplicaInfo {
            id: self.id().to_string(),
            role: format!("{:?}", metrics.state).to_lowercase(),
            leader: metrics.current_leader.map(|id| id.to_string()),
            in_sync: metrics.last_log_index.unwrap_or(0) <= applied,
            lag: None,
            // An audio replica adds the term of plan 2.11, Deployment.
            ready: ready && self.shared.contact.get().is_none_or(|c| c.serving()),
            commitable: ready
                && voter
                && slot_of(self.id()) != Slot::Witness
                && (alone || self.shared.counted.load(Ordering::SeqCst))
                && self
                    .shared
                    .contact
                    .get()
                    .is_none_or(|c| !c.engine_failed() && c.counts_self()),
        }
    }

    fn epoch(&self) -> u64 {
        self.shared.epoch()
    }

    /// The product revision ([`state_machine::StateMachine::revision`]), not the log
    /// index: a client checks that each delta follows the one before.
    fn revision(&self) -> u64 {
        self.store.with_state(|sm| sm.revision)
    }

    /// The product revision once a leader-confirmed read index, the log
    /// index a round of heartbeats to a majority confirms, is applied
    /// here. A follower asks its leader for one over the peer listener.
    /// Without a quorum, or a leader without a majority, it is
    /// [`ReadIndex::CutOff`] at once. A follower whose leader does not
    /// answer, and a deposed leader that knows its successor, are
    /// [`ReadIndex::NotLeader`].
    fn read_index(&self) -> BoxFuture<'_, ReadIndex> {
        Box::pin(async move {
            let index = match self.shared.read_log_index().await {
                Ok(index) => index,
                Err(answer) => return answer,
            };
            if self
                .shared
                .raft
                .wait(None)
                .applied_index_at_least(Some(index), "read index")
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
            let detail = if slot_of(self.id()) == Slot::Witness {
                "the witness serves no clients"
            } else if self.shared.contact.get().is_some_and(|c| c.engine_failed()) {
                "the engine refused a log entry"
            } else if self.shared.contact.get().is_some_and(|c| !c.counts_self()) {
                "clock_unverified"
            } else {
                "no leader counts this replica as a voter"
            };
            let refused = NotApplied::new(Reason::NotCommitable, detail);
            return Box::pin(async move { Err(refused) });
        }
        let Some(e) = self.shared.e(&self.estimate) else {
            let refused = NotApplied::new(Reason::Unavailable, "clock_unverified");
            return Box::pin(async move { Err(refused) });
        };
        // A resend carries the stamp of its first receipt here.
        let received = self
            .shared
            .receipts
            .lock()
            .expect("receipts lock poisoned")
            .stamp(SessionKey::from(owner), id, e, &self.shared.settings.limits);
        if received.saturating_add(self.shared.max_age()) < e {
            let refused = NotApplied::new(Reason::Expired, "first received too long ago");
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
            received,
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
                Err(Refused::Expired) => Err(NotApplied::new(
                    Reason::Expired,
                    "first received too long ago",
                )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::tests::Paths;
    use openraft::error::{InstallSnapshotError, RPCError, Unreachable};
    use openraft::network::{RPCOption, RaftNetwork};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::storage::RaftStorage;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
    use std::path::PathBuf;

    const ID: NodeId = 7;

    /// A network that reaches no one; a set of one voter sends nothing.
    struct Nobody;

    fn unreachable<E: std::error::Error>() -> RPCError<NodeId, BasicNode, E> {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other("nobody")))
    }

    impl RaftNetworkFactory<TypeConfig> for Nobody {
        type Network = Nobody;
        async fn new_client(&mut self, _: NodeId, _: &BasicNode) -> Nobody {
            Nobody
        }
    }

    impl RaftNetwork<TypeConfig> for Nobody {
        async fn append_entries(
            &mut self,
            _: AppendEntriesRequest<TypeConfig>,
            _: RPCOption,
        ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
        {
            Err(unreachable())
        }
        async fn install_snapshot(
            &mut self,
            _: InstallSnapshotRequest<TypeConfig>,
            _: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
        > {
            Err(unreachable())
        }
        async fn vote(
            &mut self,
            _: VoteRequest<NodeId>,
            _: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
            Err(unreachable())
        }
    }

    impl PeerClient for Nobody {
        fn call<'a>(
            &'a self,
            _: Option<NodeId>,
            _: &'a str,
            _: PeerRequest,
            _: Duration,
        ) -> BoxFuture<'a, Result<PeerReply, String>> {
            Box::pin(async { Err("nobody".to_owned()) })
        }
    }

    fn dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("samplez-replica-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &Path) -> Store<Paths> {
        Store::open(dir, Paths::default(), Limits::defaults(48_000)).expect("opens")
    }

    async fn start(store: Store<Paths>) -> Replica<Paths> {
        let settings = Settings {
            id: ID,
            epoch: 1,
            limits: Limits::defaults(48_000),
            block: 48,
            write_timeout: Duration::from_secs(2),
            addr: "local".to_owned(),
            peers: vec![],
            voters: None,
        };
        Replica::with_store(
            settings,
            raft_config("lone"),
            store,
            Nobody,
            Arc::new(Nobody),
            Arc::new(|| 0),
        )
        .await
        .expect("starts")
    }

    /// A vote takes this long to save, longer than openraft's tick of
    /// 1.5 heartbeats (75 ms).
    const SLOW: Duration = Duration::from_millis(100);

    async fn leads(replica: &Replica<Paths>, what: &'static str) {
        let leads = replica
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, what)
            .await;
        assert!(leads.is_ok(), "{leads:?}");
    }

    /// openraft elects the only voter on a tick it does not lead, and
    /// counts its own vote only once the vote is saved. Up to 0.9.25 it
    /// also elected while that vote was still being saved, so the next
    /// term overtook every slow vote and the replica never led.
    #[tokio::test(start_paused = true)]
    async fn a_set_of_one_voter_whose_vote_takes_longer_than_the_tick_to_save_still_leads() {
        let dir = dir("lone-fresh");
        let replica = start(open(&dir).with_sync_delay(SLOW)).await;
        replica
            .initialize([(ID, "local".to_owned())])
            .await
            .expect("initializes");
        leads(&replica, "a fresh set of one voter leads").await;
        replica.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// As above, for a volume whose only voter is itself and whose last
    /// vote never counted: startup does not restore leadership, so the
    /// replica has to elect itself.
    #[tokio::test(start_paused = true)]
    async fn a_reopened_set_of_one_voter_whose_vote_takes_longer_than_the_tick_to_save_still_leads()
    {
        let dir = dir("lone-reopened");
        let mut store = open(&dir);
        let membership = Membership::from(BTreeMap::from([(ID, BasicNode::new("local"))]));
        store
            .append_to_log([Entry::<TypeConfig> {
                log_id: LogId::new(CommittedLeaderId::new(0, 0), 0),
                payload: EntryPayload::Membership(membership),
            }])
            .await
            .expect("appends");
        store.save_vote(&Vote::new(1, ID)).await.expect("votes");
        drop(store);
        let replica = start(open(&dir).with_sync_delay(SLOW)).await;
        leads(&replica, "a reopened set of one voter leads").await;
        replica.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
