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
pub mod horizon;
pub mod log_store;
pub mod membership;
pub mod net;
pub mod peer;
pub mod rank;
pub mod report;
pub mod resync;
pub mod rollout;
pub mod schedule;
pub mod session;
pub mod snapshot;
pub mod state;
pub mod state_machine;
pub mod verdict;
pub mod verify;
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
use samplez_service::{
    MetricKind, Owner, Position, ReadIndex, ReplicaInfo, ReplicaMetric, ReplicatedState,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

use crate::bootstrap::Bootstrap;
use crate::dedup::Limits;
use crate::entry::{
    Batch, Body, Command, NodeId, Origin, Outcome, PROTOCOL, Request, Response, SessionKey,
    stamping_index,
};
use crate::log_store::Store;
use crate::membership::{Slot, Voters, slot_of};
use crate::peer::{Forwarded, PeerClient, PeerReply, PeerRequest, PeerResponse, Refused};
use crate::state_machine::{Factory, Product};

openraft::declare_raft_types!(
    /// The openraft types of a samplez replica set.
    pub TypeConfig:
        D = Request,
        R = Response,
);

/// The committed vote of the last AppendEntries or InstallSnapshot a
/// replica accepted, and when.
#[derive(Clone, Default)]
struct Heard(Arc<Mutex<Option<Taken>>>);

type Taken = (openraft::Vote<NodeId>, Instant);

impl Heard {
    fn took(&self, vote: openraft::Vote<NodeId>) {
        if vote.committed {
            *self.0.lock().expect("heard lock poisoned") = Some((vote, Instant::now()));
        }
    }

    /// Whether the last one accepted carried `vote` and lies within
    /// `lease`: a record of another leader or term does not count.
    fn within(&self, vote: &openraft::Vote<NodeId>, lease: Duration) -> bool {
        self.0
            .lock()
            .expect("heard lock poisoned")
            .as_ref()
            .is_some_and(|(v, at)| v == vote && at.elapsed() <= lease)
    }
}

/// A replica's raft as other replicas' rafts reach it: the network hands
/// every AppendEntries, heartbeats included, InstallSnapshot and vote
/// request to these, so the replica records the leader it accepted.
#[derive(Clone)]
pub struct RaftRpcs {
    raft: Raft<TypeConfig>,
    heard: Heard,
}

impl RaftRpcs {
    /// An AppendEntries; one answered with anything but a higher vote
    /// took the sender's vote.
    pub async fn append_entries(
        &self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
    ) -> Result<openraft::raft::AppendEntriesResponse<NodeId>, RaftError<NodeId>> {
        let vote = rpc.vote;
        let reply = self.raft.append_entries(rpc).await;
        if reply
            .as_ref()
            .is_ok_and(|r| !matches!(r, openraft::raft::AppendEntriesResponse::HigherVote(_)))
        {
            self.heard.took(vote);
        }
        reply
    }

    /// An InstallSnapshot chunk; one answered with the sender's vote took
    /// it.
    pub async fn install_snapshot(
        &self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        RaftError<NodeId, openraft::error::InstallSnapshotError>,
    > {
        let vote = rpc.vote;
        let reply = self.raft.install_snapshot(rpc).await;
        if reply.as_ref().is_ok_and(|r| r.vote == vote) {
            self.heard.took(vote);
        }
        reply
    }

    /// A vote request; it records nothing, since a candidate's vote is no
    /// leader's.
    pub async fn vote(
        &self,
        rpc: openraft::raft::VoteRequest<NodeId>,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RaftError<NodeId>> {
        self.raft.vote(rpc).await
    }
}

/// The largest command body a replica orders; a larger one is refused
/// with [`Reason::TooLarge`].
pub const MAX_COMMAND_BYTES: usize = 32 << 10;

/// The command bytes ([`Command::size`]) one entry carries at most: a
/// group commit closes before the command that would pass it, so only a
/// single command can exceed it, by its id and owner.
pub const MAX_ENTRY_BYTES: usize = 32 << 10;

/// The commands one entry carries at most.
pub const MAX_ENTRY_COMMANDS: usize = 64;

/// The entries one AppendEntries carries at most.
pub const MAX_PAYLOAD_ENTRIES: u64 = 64;

/// The command bytes ([`Command::size`]) one AppendEntries carries at
/// most beside its first entry, about 128 KiB, a third more encoded:
/// openraft gives an AppendEntries the heartbeat interval to complete.
/// Counting bytes rather than entries lets one AppendEntries, and so one
/// fsync on a follower, take every small entry written since the last
/// one, so a follower whose fsync takes longer than the gap between
/// entries does not fall further behind with each of them.
pub const MAX_APPEND_BYTES: usize = 4 * MAX_ENTRY_BYTES;

/// openraft's timers as the chart pins them (`replica.raft`): its 0.9.21
/// defaults, so the election figures do not move with a dependency
/// update; and [`MAX_PAYLOAD_ENTRIES`], which [`MAX_APPEND_BYTES`]
/// bounds further.
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
    /// The product drives an engine ([`Product::DRIVES_ENGINE`]).
    drives_engine: bool,
    /// Whether a command would re-route an input applied to the state as
    /// it stands ([`Product::reroutes`]), for a product that drives an
    /// engine; read under the store lock.
    reroutes: Box<dyn Fn(&Command) -> bool + Send + Sync>,
    /// Whether the decidable entry at a log index re-routed an input, or,
    /// with `pending`, is an engine body that raises or re-routes one: a
    /// decision on it needs a generation. Read under the store lock.
    rerouted: Box<dyn Fn(u64, bool) -> bool + Send + Sync>,
    /// Set while a writer appends [`Body::Protocol`], so it is appended
    /// once.
    marking: AtomicBool,
    /// [`state_machine::StateMachine::protocol`] as applied here.
    protocol: Box<dyn Fn() -> u32 + Send + Sync>,
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
    /// The leader this replica last took an AppendEntries or an
    /// InstallSnapshot from ([`RaftRpcs`]).
    heard: Heard,
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
    /// Opens the storage in `dir` with the products `factory` builds
    /// ([`Store::open`]) and starts the replica, as [`Replica::with_store`].
    /// Every member, learners and the witness included, needs a factory of
    /// the same configuration ([`Product::config_hash`]): each applies the
    /// log into its own product state. A snapshot from a replica of another
    /// configuration ends the process ([`log_store::CONFIG_MISMATCH_EXIT`]);
    /// open the store with [`Store::on_config_mismatch`] and use
    /// [`Replica::with_store`] to do otherwise.
    pub async fn start<N>(
        settings: Settings,
        raft: openraft::Config,
        dir: &Path,
        factory: Factory<P>,
        network: N,
        peers: Arc<dyn PeerClient>,
        estimate: Estimate,
    ) -> Result<Replica<P>, String>
    where
        N: RaftNetworkFactory<TypeConfig>,
    {
        let store = Store::open(dir, factory, settings.limits).map_err(|e| e.to_string())?;
        Replica::with_store(settings, raft, store, network, peers, estimate).await
    }

    /// Starts the replica on `store`: a restarted one applies its log up to
    /// the saved committed id before it hears from any peer. A snapshot from
    /// a replica of another product configuration does what `store` was
    /// opened to do, by default end the process ([`Store::open`]).
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
            drives_engine: P::DRIVES_ENGINE,
            reroutes: {
                let store = store.clone();
                Box::new(move |command: &Command| {
                    P::DRIVES_ENGINE
                        && store.with_state(|sm| {
                            !sm.product
                                .reroutes(&command.origin, &command.body)
                                .is_empty()
                        })
                })
            },
            rerouted: {
                let store = store.clone();
                Box::new(move |index, pending| {
                    store.with_state(|sm| {
                        sm.derived
                            .get(&index)
                            .is_some_and(|slots| !slots.is_empty())
                            || (pending
                                && sm.pending.get(&index).is_some_and(Body::needs_generation))
                    })
                })
            },
            protocol: {
                let store = store.clone();
                Box::new(move || store.with_state(|sm| sm.protocol))
            },
            raft,
            peers,
            lease: Duration::from_millis(raft_config.election_timeout_max),
            heartbeat: Duration::from_millis(raft_config.heartbeat_interval),
            quiescing: AtomicBool::new(false),
            proposing: AtomicUsize::new(0),
            marking: AtomicBool::new(false),
            revision: {
                let store = store.clone();
                Box::new(move || store.with_state(|sm| sm.revision))
            },
            epoch: AtomicU64::new(settings_epoch),
            counted: AtomicBool::new(false),
            heard: Heard::default(),
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

    /// The replica's raft. A network delivers other replicas' RPCs through
    /// [`Replica::rpcs`], not through this: an AppendEntries delivered here
    /// leaves no record of the leader, so no leader counts the replica.
    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.shared.raft
    }

    /// The RPCs other replicas' rafts send this one, as the network
    /// delivers them.
    pub fn rpcs(&self) -> RaftRpcs {
        RaftRpcs {
            raft: self.shared.raft.clone(),
            heard: self.shared.heard.clone(),
        }
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
    /// entry behind the earlier generation-needing ones only. A product
    /// that drives an engine owns every input's flow: a `Route` or
    /// `MasterEnable` for it is refused.
    ///
    /// # Errors
    ///
    /// When the product drives an engine and `body` is a `Route` or a
    /// `MasterEnable`.
    pub fn append(&self, body: Body) -> Result<(), String> {
        if P::DRIVES_ENGINE && matches!(body, Body::Route { .. } | Body::MasterEnable { .. }) {
            return Err("the product owns the inputs' flows".to_owned());
        }
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
        Ok(())
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
    /// each report period, F published to `audio`, what each entry derived
    /// handed to it as the entry applies, and entries unresolved once
    /// their apply deadline passes unapplied. The witness passes no
    /// `audio`.
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

    /// Gives contact the compatibility key and the revisions of a rollout
    /// ([`rollout`]); without, every replica's key is equal.
    pub fn rollout(&self, rollout: Arc<dyn rollout::Rollout>) -> Result<(), String> {
        let contact = self
            .shared
            .contact
            .get()
            .ok_or_else(|| "contact does not run".to_owned())?;
        contact.set_rollout(rollout);
        Ok(())
    }

    /// Contact as status shows it, once started.
    pub fn contact_metrics(&self) -> Option<contact::Metrics> {
        self.shared.contact.get().map(|c| c.metrics())
    }

    /// Whether this replica knows a quorum: as leader, one acknowledged it
    /// within the lease; else it took its leader's last AppendEntries or
    /// InstallSnapshot within it.
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
            PeerRequest::Core => {
                let contact = shared
                    .contact
                    .get()
                    .cloned()
                    .ok_or_else(|| Refused::Unavailable("contact does not run".to_owned()))?;
                // The engine copies after its next block: off the async
                // workers.
                tokio::task::spawn_blocking(move || contact.copy_core())
                    .await
                    .unwrap_or_else(|e| Err(e.to_string()))
                    .map(PeerResponse::Core)
                    .map_err(Refused::Unavailable)
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

    /// The generation flag the leader writes for what it classified as
    /// `generation`: `Some` only while the protocol gate is open
    /// ([`contact::Contact::protocol_blocker`]) and the state holds
    /// [`PROTOCOL`], else `None`, which every binary applies alike. The
    /// first time the gate is open it appends [`Body::Protocol`] at
    /// `stamp` and flags only once that applied, so the marker precedes
    /// every flag in the log.
    async fn flag(&self, generation: bool, stamp: u64) -> Option<bool> {
        let contact = self.contact.get()?;
        // The membership as applied now, not as contact's last tick saw it.
        // A member added after this read and before the entry is written
        // still receives it. A binary of an older protocol among those
        // never applies the flag: the marker precedes every flag in the
        // log and it cannot decode the marker, nor install a snapshot of a
        // log past it (`snapshot::StoredSnapshot`). From its first report
        // on it shuts the gate.
        let members = contact::Members::of(&self.raft.metrics().borrow());
        if let Some(blocker) = contact.protocol_blocker(&members) {
            tracing::debug!(?blocker, "protocol gate shut: no generation flag");
            return None;
        }
        if (self.protocol)() < PROTOCOL {
            // Another writer appending it: unflagged until it applied.
            let _marking = Marking::take(&self.marking)?;
            let marker = Request {
                stamp,
                body: Body::Protocol(PROTOCOL),
            };
            let written =
                tokio::time::timeout(self.settings.write_timeout, self.raft.client_write(marker))
                    .await;
            if !matches!(written, Ok(Ok(_))) {
                return None;
            }
            tracing::info!(protocol = PROTOCOL, "protocol gate opened");
        }
        Some(generation)
    }

    /// Flags a decision on an entry that re-routed or raised an input for
    /// its generation, from this leader's own state, whatever the body
    /// carried: it re-asserts that entry on the generation path.
    async fn classify(&self, body: &mut Body, estimate: &Estimate) {
        // A restamp moves a pending engine body to its index; a void drops
        // it, re-asserting only a product entry's effect.
        let pending = matches!(body, Body::Restamp { .. });
        if let Body::Restamp {
            revision,
            generation,
            ..
        }
        | Body::Void {
            revision,
            generation,
            ..
        } = body
        {
            *generation = None;
            // No estimate, no stamp for the marker: unflagged.
            if (self.rerouted)(*revision, pending)
                && let Some(e) = self.e(estimate)
            {
                *generation = self
                    .flag(true, stamping_index(e, self.settings.block))
                    .await;
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
            ServerState::Follower | ServerState::Learner => self.leader_heard(&metrics),
            _ => false,
        }
    }

    /// Whether this replica took an AppendEntries or an InstallSnapshot
    /// from the leader its committed vote names, in that vote's term,
    /// within the lease. openraft's own vote time does not tell: it also
    /// moves when a vote is adopted from a peer's refusal, which proves no
    /// leader alive.
    fn leader_heard(&self, metrics: &openraft::RaftMetrics<NodeId, BasicNode>) -> bool {
        metrics.vote.committed
            && metrics.current_leader.is_some()
            && self.heard.within(&metrics.vote, self.lease)
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
                let mut body = body.clone();
                self.classify(&mut body, estimate).await;
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
                // A later index asked for is kept. A decision or a
                // `Baseline` takes the lead the entries lately needed, past
                // the cap: stamped with the lead at the cap, it is late
                // wherever the commit takes longer than the lead covers.
                let mut earliest = stamp.earliest;
                if (body.decides().is_some() || matches!(body, Body::Baseline(_)))
                    && let Some(contact) = self.contact.get()
                {
                    let lead = contact.decision_lead();
                    earliest =
                        earliest.max((stamp.stamp + lead).next_multiple_of(self.settings.block));
                }
                if let Some(at) = body.at_index() {
                    state_machine::restamp(&mut body, at.max(earliest));
                }
                if let Body::Baseline(b) = &mut body {
                    b.at_index = b.at_index.max(earliest);
                    if self.contact.get().is_some_and(|c| c.hands_over(b)) {
                        b.at_index = rollout::hand_over_at(b.at_index, self.settings.block);
                    }
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
    async fn order_commands(&self, mut items: Vec<Item>, estimate: &Estimate, generation: bool) {
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
                let stamp = match self.stamp(generation, estimate) {
                    Ok(stamp) => stamp,
                    // One pending generation at a time: wait for it
                    // proposing nothing, so a quiesce goes on meanwhile,
                    // and check the leadership again.
                    Err(contact::StampRefusal::GenerationPending)
                        if items
                            .iter()
                            .any(|item| Instant::now() + self.heartbeat < item.deadline) =>
                    {
                        drop(proposing);
                        tokio::time::sleep(self.heartbeat / 10).await;
                        continue;
                    }
                    Err(refusal) => {
                        let _ = answer_all(items, stamp_refused(refusal));
                        return;
                    }
                };
                let written = self.write_locally(items, stamp, estimate, generation).await;
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

    /// Appends `items` as one entry stamped `stamp` and answers them,
    /// each flagged as this leader classified them, `generation`, whatever
    /// a forwarding replica set; gives them back when this replica turned
    /// out not to lead.
    async fn write_locally(
        &self,
        items: Vec<Item>,
        stamp: contact::Stamp,
        estimate: &Estimate,
        generation: bool,
    ) -> Result<(), Vec<Item>> {
        // A member of another configuration would apply them into another
        // product state.
        if let Some(id) = self.contact.get().and_then(|c| c.config_differs()) {
            let refused = Refused::Unavailable(format!("config: replica {id} differs"));
            return answer_all(items, refused);
        }
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
        // A product that drives an engine derives its entries from the
        // state at the command's place in the log, which already holds a
        // scheduled command's effect: one asking for an index beyond the
        // earliest is not ordered.
        let earliest = stamp.earliest.next_multiple_of(self.settings.block);
        let (scheduled, items): (Vec<Item>, Vec<Item>) = items
            .into_iter()
            .partition(|item| scheduled(self.drives_engine, item.command.at_index, earliest));
        for item in scheduled {
            let at = item.command.at_index.unwrap_or(0);
            let refused = Refused::Invalid(format!(
                "scheduled at_index {at} beyond the earliest, {earliest}: this product applies \
                 commands as they are ordered"
            ));
            let _ = item.reply.send(Err(refused));
        }
        if items.is_empty() {
            return Ok(());
        }
        let flag = self.flag(generation, stamp.stamp).await;
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
                command.generation = flag;
                // A later index asked for is kept, an earlier or absent one
                // is the earliest.
                if self.contact.get().is_some() {
                    // A block start, as the engine takes it.
                    let at = command.at_index.unwrap_or(0).max(stamp.earliest);
                    command.at_index = Some(at.next_multiple_of(self.settings.block));
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

/// Whether a command asking for `at_index` is scheduled past `earliest`,
/// the earliest block start the writer stamps: refused for a product that
/// drives an engine.
fn scheduled(drives_engine: bool, at_index: Option<u64>, earliest: u64) -> bool {
    drives_engine && at_index.is_some_and(|at| at > earliest)
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

/// A writer: takes its queue in order, so the commands of the one queue
/// that carries them commit in the order they were submitted, and puts
/// what queued while the previous entry was written into the next one
/// (group commit). A copy of a command the batch holds already is folded
/// into it and answered from its outcome, an applied one as a duplicate.
/// An entry carries at most [`MAX_ENTRY_BYTES`] of commands. A command
/// that would re-route an input ([`Product::reroutes`]) ends the group
/// before it and is ordered alone, stamped for its generation. On a
/// follower the group is forwarded to the leader instead, one group at a
/// time, so the order holds there too. A re-routing command waiting for
/// the previous generation's index holds every command queued behind it,
/// as does a decision that re-asserts a re-route. The `InputUp` and
/// `Route` entries the replica set decides on have a writer of their own,
/// so one of those waiting holds no command.
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
                // One that would re-route an input is ordered alone,
                // stamped for its generation.
                if (shared.reroutes)(&item.command) {
                    shared.order_commands(vec![item], &estimate, true).await;
                    continue;
                }
                let mut bytes = item.command.size();
                let mut items = vec![item];
                while let Ok(queued) = queued.try_recv() {
                    match queued {
                        Queued::Command(item)
                            if joins(&items[0].command, items.len(), bytes, &item.command)
                                && !(shared.reroutes)(&item.command) =>
                        {
                            bytes += item.command.size();
                            items.push(item);
                        }
                        other => {
                            next = Some(other);
                            break;
                        }
                    }
                }
                shared.order_commands(items, &estimate, false).await;
            }
        }
    }
}

/// One writer's hold on appending the protocol marker
/// ([`Body::Protocol`]), released when dropped: a write cancelled while it
/// awaits the marker's commit leaves the next writer free to append it.
struct Marking<'a>(&'a AtomicBool);

impl<'a> Marking<'a> {
    /// The hold, none while another writer has it.
    fn take(flag: &'a AtomicBool) -> Option<Marking<'a>> {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| Marking(flag))
    }
}

impl Drop for Marking<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Whether `next` joins the entry `first` opened, of `commands` commands
/// and `bytes` command bytes: within [`MAX_ENTRY_COMMANDS`] and
/// [`MAX_ENTRY_BYTES`], and asking for the index `first` asks for, so
/// that every command of an entry applies at one index.
fn joins(first: &Command, commands: usize, bytes: usize, next: &Command) -> bool {
    commands < MAX_ENTRY_COMMANDS
        && bytes + next.size() <= MAX_ENTRY_BYTES
        && next.at_index == first.at_index
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

    /// Contact's view of this audio replica, once contact runs: none for
    /// a single replica.
    fn replica_metrics(&self) -> Vec<ReplicaMetric> {
        self.shared
            .contact
            .get()
            .map_or_else(Vec::new, |c| contact_samples(c.serving(), &c.metrics()))
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
                generation: None,
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
                Err(Refused::Invalid(detail)) => Err(NotApplied::new(Reason::Invalid, detail)),
                Err(_) => Err(NotApplied::new(Reason::Unavailable, "no leader")),
            }
        })
    }

    fn renew_sessions(&self, live: &[Owner]) {
        // Never a route: never refused.
        let _ = self.append(Body::LeaseRenew(
            live.iter().map(SessionKey::from).collect(),
        ));
    }
}

/// The `/metrics` samples of contact metrics `m`, `serving` the `/readyz`
/// term ([`contact::Contact::serving`]).
fn contact_samples(serving: bool, m: &contact::Metrics) -> Vec<ReplicaMetric> {
    use MetricKind::{Counter, Gauge};
    let one = |name, help, kind, value: f64| ReplicaMetric {
        name,
        help,
        kind,
        labels: Vec::new(),
        value,
    };
    let flag = |b: bool| if b { 1.0 } else { 0.0 };
    let r = &m.resync;
    let mut out = vec![
        one(
            "samplez_replica_serving",
            "The replicated state's /readyz term.",
            Gauge,
            flag(serving),
        ),
        one(
            "samplez_replica_in_contact",
            "Whether contact holds a majority within the contact window.",
            Gauge,
            flag(m.in_contact),
        ),
    ];
    use state::ReplicaState as S;
    let now = m.replica.and_then(|l| l.state);
    for (state, name) in [
        (S::Starting, "starting"),
        (S::Committing, "committing"),
        (S::Withdrawn, "withdrawn"),
        (S::Diverged, "diverged"),
        (S::VersionMismatch, "version_mismatch"),
        (S::Resync, "resync"),
    ] {
        out.push(ReplicaMetric {
            name: "samplez_replica_state",
            help: "1 for the audio replica's state.",
            kind: Gauge,
            labels: vec![("state", name.to_owned())],
            value: flag(now == Some(state)),
        });
    }
    let divergences = m.replica.map_or(0, |l| l.divergences);
    out.extend([
        one(
            "samplez_replica_divergences_total",
            "Times the replica diverged.",
            Counter,
            divergences as f64,
        ),
        one(
            "samplez_replica_resync_attempts_total",
            "Resyncs started: a donor asked for its core.",
            Counter,
            r.attempts as f64,
        ),
        one(
            "samplez_replica_resync_handovers_total",
            "Replayed states the RT thread took.",
            Counter,
            r.handovers as f64,
        ),
        one(
            "samplez_replica_resync_failures_total",
            "Resyncs that ended without a hand-over.",
            Counter,
            r.failures as f64,
        ),
        one(
            "samplez_replica_resync_no_donor_total",
            "Resyncs not started for want of a committing donor.",
            Counter,
            r.no_donor as f64,
        ),
        one(
            "samplez_replica_resync_rebuilt_for_flows_total",
            "Diverged replicas rebuilt from product state, not resynced: an input read another flow.",
            Counter,
            r.rebuilt_for_flows as f64,
        ),
        one(
            "samplez_replica_resync_backoff_seconds",
            "How long the next resync waits after the last one ended.",
            Gauge,
            r.backoff.as_secs_f64(),
        ),
        one(
            "samplez_replica_late_rebuilds_total",
            "Engine rebuilds past entries taken late while catching up.",
            Counter,
            m.late_rebuilds as f64,
        ),
        one(
            "samplez_replica_unresolved_total",
            "Entries handed to the engine unresolved.",
            Counter,
            m.unresolved as f64,
        ),
        one(
            "samplez_replica_apply_lead_samples",
            "The apply lead entries are stamped with.",
            Gauge,
            m.lead as f64,
        ),
        one(
            "samplez_replica_apply_lead_capped",
            "Whether the apply lead is at maxApplyLeadMs.",
            Gauge,
            flag(m.apply_lead_capped),
        ),
        one(
            "samplez_replica_decision_lead_samples",
            "The lead decisions and baselines are stamped with, and commands at the cap.",
            Gauge,
            m.decision_lead as f64,
        ),
    ]);
    if let Some(node) = m.apply_lead_raised_by {
        out.push(ReplicaMetric {
            name: "samplez_replica_apply_lead_raised_by",
            help: "The replica whose slack raised the apply lead last.",
            kind: Gauge,
            labels: vec![("node", node.to_string())],
            value: 1.0,
        });
    }
    out
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

    /// A writer cancelled while it awaits the marker's commit releases
    /// the hold, so a later writer appends the marker rather than write
    /// every flag unflagged.
    #[test]
    fn a_cancelled_marker_write_releases_the_hold() {
        let flag = AtomicBool::new(false);
        let mut write = Box::pin(async {
            let _held = Marking::take(&flag).expect("free");
            std::future::pending::<()>().await;
        });
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert!(Marking::take(&flag).is_none(), "held while it writes");
        drop(write);
        assert!(Marking::take(&flag).is_some(), "released when cancelled");
    }

    /// A product that drives an engine refuses a command asking for an
    /// index beyond the earliest; one that does not keeps it, and an
    /// index at or before the earliest, or none, is never refused.
    #[test]
    fn only_an_engine_product_refuses_a_scheduled_index() {
        assert!(scheduled(true, Some(4848), 4800));
        assert!(!scheduled(true, Some(4800), 4800));
        assert!(!scheduled(true, None, 4800));
        assert!(!scheduled(false, Some(48_000), 4800));
    }

    /// An entry carries commands asking for one index only: the engine
    /// applies a log entry at one index, so a command asking for a later
    /// one is not folded into an entry applied earlier.
    #[test]
    fn commands_asking_for_other_indexes_take_entries_of_their_own() {
        let command = |at_index, bytes: usize| Command {
            generation: None,
            origin: Origin {
                principal: "p".into(),
                session: "s".into(),
                role: samplez_service::Role::Operator,
                connection: "c".into(),
            },
            id: "1".into(),
            at_index,
            body: vec![0; bytes],
        };
        let first = command(None, 10);
        assert!(joins(&first, 1, 10, &command(None, 10)));
        assert!(!joins(&first, 1, 10, &command(Some(4800), 10)));
        assert!(!joins(&command(Some(4800), 10), 1, 10, &command(None, 10)));
        assert!(joins(
            &command(Some(4800), 10),
            1,
            10,
            &command(Some(4800), 10)
        ));
        let size = command(None, 0).size();
        assert!(!joins(&first, 1, MAX_ENTRY_BYTES - size, &command(None, 1)));
        // At most MAX_ENTRY_COMMANDS: the checks before each joins stay
        // bounded.
        assert!(joins(
            &first,
            MAX_ENTRY_COMMANDS - 1,
            10,
            &command(None, 10)
        ));
        assert!(!joins(&first, MAX_ENTRY_COMMANDS, 10, &command(None, 10)));
    }

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
        Store::open(dir, Arc::new(Paths::default), Limits::defaults(48_000)).expect("opens")
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

    /// An accepted request counts for the leader and term of its committed
    /// vote alone, and only within the lease; a candidate's uncommitted
    /// vote is no leader's.
    #[tokio::test(start_paused = true)]
    async fn a_heard_leader_counts_for_its_own_vote_within_the_lease() {
        use openraft::Vote;
        let lease = Duration::from_secs(2);
        let heard = Heard::default();
        let leader = Vote::new_committed(3, 1);
        assert!(!heard.within(&leader, lease), "nothing taken");
        heard.took(Vote::new(3, 1));
        assert!(!heard.within(&Vote::new(3, 1), lease), "a candidate's vote");
        heard.took(leader);
        assert!(heard.within(&leader, lease));
        assert!(
            !heard.within(&Vote::new_committed(4, 1), lease),
            "a later term"
        );
        assert!(
            !heard.within(&Vote::new_committed(3, 2), lease),
            "another leader"
        );
        tokio::time::advance(lease + Duration::from_millis(1)).await;
        assert!(!heard.within(&leader, lease), "past the lease");
    }

    /// The leader a replica took is recorded from what its raft answered:
    /// an AppendEntries answered Conflict or Success took the sender's vote,
    /// one answered HigherVote did not, nor an InstallSnapshot answered
    /// with another vote.
    #[tokio::test]
    async fn rpcs_record_only_a_vote_the_raft_accepted() {
        let dir = dir("rpcs");
        let replica = start(open(&dir)).await;
        let rpcs = replica.rpcs();
        let lease = Duration::from_secs(60);
        let heard = |vote: &Vote<NodeId>| replica.shared.heard.within(vote, lease);
        let append = |vote: Vote<NodeId>, prev: Option<LogId<NodeId>>| AppendEntriesRequest {
            vote,
            prev_log_id: prev,
            entries: Vec::new(),
            leader_commit: None,
        };
        let five = Vote::new_committed(5, 1);
        let behind = LogId::new(CommittedLeaderId::new(5, 1), 10);
        let reply = rpcs.append_entries(append(five, Some(behind))).await;
        assert!(
            matches!(reply, Ok(AppendEntriesResponse::Conflict)),
            "{reply:?}"
        );
        assert!(heard(&five), "a conflict took the vote");
        let six = Vote::new_committed(6, 2);
        let reply = rpcs.append_entries(append(six, None)).await;
        assert!(
            matches!(reply, Ok(AppendEntriesResponse::Success)),
            "{reply:?}"
        );
        assert!(heard(&six));
        let four = Vote::new_committed(4, 1);
        let reply = rpcs.append_entries(append(four, None)).await;
        assert!(
            matches!(reply, Ok(AppendEntriesResponse::HigherVote(_))),
            "{reply:?}"
        );
        assert!(
            !heard(&four) && heard(&six),
            "a refused append records nothing"
        );
        let snapshot = InstallSnapshotRequest {
            vote: four,
            meta: openraft::SnapshotMeta::default(),
            offset: 0,
            data: Vec::new(),
            done: true,
        };
        let reply = rpcs.install_snapshot(snapshot).await.expect("answers");
        assert_eq!(reply.vote, six);
        assert!(
            !heard(&four) && heard(&six),
            "a refused snapshot records nothing"
        );
        replica.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
