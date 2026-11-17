//! Contact between voters, leader-independent (plan 2.11, Fencing): side
//! reports each report period p, the contact fence F (B1), silence held
//! and relayed, commit knowledge from openraft and from reports, and the
//! apply deadline that leaves an entry unresolved (B2).
//!
//! Each replica keeps the rules on its own clocks: its TAI clock for
//! everything compared with block deadlines, `CLOCK_MONOTONIC` (tokio's
//! clock) for receipt ages and silence.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openraft::{BasicNode, RaftMetrics};
use samplez::blocks::{BlockStatus, Cause, Ranks};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::Shared;
use crate::baseline::{Leader, LeaderView, Voter};
use crate::entry::{Baseline, Body, NodeId, Request};
use crate::log_store::Store;
use crate::peer::PeerRequest;
use crate::rank::Baselines;
use crate::report::{Echo, Heard, Histogram, Listed, MAX_LISTED, Own, Report, Slack};
use crate::schedule::Lead;
use crate::snapshot::StoredLogId;
use crate::state::{Life, PageBook, ReplicaState, Seen, View};
use crate::state_machine::Product;
use crate::withhold::Withhold;

/// The engine's sample rate (plan 2.3).
pub const RATE: u64 = samplez_mxl::SAMPLE_RATE as u64;

/// The sample index at TAI time `tai_ns`.
pub fn index_at(tai_ns: u64) -> u64 {
    (u128::from(tai_ns) * u128::from(RATE) / 1_000_000_000) as u64
}

/// The TAI time of sample index `index`, ns.
pub fn time_of(index: u64) -> u64 {
    (u128::from(index) * 1_000_000_000).div_ceil(u128::from(RATE)) as u64
}

/// The timers of contact, as the chart sets them (plan 2.11, timer table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// The engine block size B.
    pub block: u64,
    /// `readerMarginSamples`: block k is due at `T((k + 1)B + margin)`.
    pub margin: u64,
    /// p, `reportPeriodMs`.
    pub report_period: Duration,
    /// w, `contactWindowMs`.
    pub contact_window: Duration,
    /// D, `holdMs`.
    pub hold: Duration,
    /// `clockGuardSamples`.
    pub clock_guard: u64,
    /// `maxApplyLeadMs`.
    pub max_apply_lead: Duration,
}

impl Timing {
    /// The defaults at block size `block`: p 2 ms, w 8 ms, D 10 ms, a
    /// margin of 576 samples, clockGuard half of it, maxApplyLead 50 ms.
    pub fn defaults(block: u64) -> Timing {
        Timing {
            block,
            margin: 576,
            report_period: Duration::from_millis(2),
            contact_window: Duration::from_millis(8),
            hold: Duration::from_millis(10),
            clock_guard: 288,
            max_apply_lead: Duration::from_millis(50),
        }
    }

    /// `applyGuard = w + 2 x clockGuard`, ns.
    pub fn apply_guard_ns(&self) -> u64 {
        self.contact_window.as_nanos() as u64 + 2 * time_of(self.clock_guard)
    }

    /// The TAI time from which an entry at `at_index` is unresolved unless
    /// its commit is known: `T(at_index) - applyGuard`.
    pub fn apply_deadline(&self, at_index: u64) -> u64 {
        time_of(at_index).saturating_sub(self.apply_guard_ns())
    }

    /// `T_k`, block k's deadline.
    pub fn block_deadline(&self, k: u64) -> u64 {
        time_of((k + 1) * self.block + self.margin)
    }
}

/// The TAI clock a replica judges deadlines by.
pub trait Clocks: Send + Sync + 'static {
    /// TAI time, ns.
    fn tai_ns(&self) -> u64;
}

/// `CLOCK_TAI`, the clock libmxl's `mxlGetTime` reads.
pub struct SystemClocks;

impl Clocks for SystemClocks {
    fn tai_ns(&self) -> u64 {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is writable; CLOCK_TAI exists on Linux.
        unsafe { libc::clock_gettime(libc::CLOCK_TAI, &mut ts) };
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
}

/// The engine of an audio replica as contact drives it; the witness has
/// none.
pub trait Audio: Send + Sync + 'static {
    /// What the replica says of itself in its reports. Read after every
    /// timestamp a report carries, so no exchange timestamp taken after a
    /// detected clock step goes out under the flags from before it.
    fn sample(&self) -> Sample;
    /// Publishes F: blocks ending above it are fenced.
    fn fence(&self, end: u64);
    /// Block k is final on every output, or was never computed and never
    /// will be: F may pass it whatever its contact.
    fn settled(&self, k: u64) -> bool;
    /// Entry `index` was appended to this replica's log.
    fn appended(&self, index: u64, request: &Request);
    /// The entries from `from` on were removed from the log.
    fn truncated(&self, from: u64);
    /// The entries up to `up_to` are known committed: they go to the RT
    /// thread.
    fn committed(&self, up_to: u64);
    /// Entry `index` at `at_index` is unresolved: every block from
    /// `at_index` is degraded until the log decides it. False when the
    /// engine had no room for it.
    fn unresolved(&self, index: u64, at_index: u64) -> bool;
    /// This log's entry `index` is never applied: its commit was not known
    /// by its apply deadline, so only the log's later decision on it
    /// counts.
    fn skip(&self, index: u64);
    /// An unresolved entry found no room: the replica can no longer tell
    /// its degraded blocks, so it stops committing them at once.
    fn overwhelmed(&self);
    /// Block `k`'s status on output `output`, none before its first.
    fn status(&self, output: usize, k: u64) -> Option<BlockStatus>;
    /// Moves block `k` of `output` to `withheld` for `cause`, from no
    /// status yet, `fenced` or `held`: the status it ends with, none when
    /// nothing was recorded.
    fn withhold(&self, output: usize, k: u64, cause: Cause) -> Option<BlockStatus>;
    /// Marks the held block `k` of `output` for commit through, `[kB, P)`
    /// only with `up_to` P, naming the voters whose silence the evidence
    /// used by bit; false when it is not held.
    fn commit_through(&self, output: usize, k: u64, on_silence_of: u8, up_to: Option<u64>) -> bool;
    /// The replica is withdrawn on every output that commits.
    fn withdraw(&self);
    /// Publishes the ranks the blocks take when F passes them; called
    /// before the F they hold for.
    fn ranks(&self, ranks: Ranks);
    /// Output `output` commits from the first page boundary in a block at
    /// or after `min_k` it computes, decided on the statuses up to block
    /// `seen`: ignored when a block after it was withheld for a cause.
    fn commit_from(&self, output: usize, min_k: u64, seen: u64);
    /// Drops output `output`'s commit start unless it took.
    fn cancel_commit_start(&self, output: usize);
    /// Entry `index`, given as unresolved, is decided: its blocks are no
    /// longer degraded for it.
    fn resolved(&self, _index: u64) {}
    /// Drops the running engine's core: its state no longer follows the
    /// log (snapshot install, purge-point verdict).
    fn drop_engine(&self) {}
    /// Builds the engine's core from the applied product state and
    /// installs it; false when that failed and is to be retried.
    fn rebuild_engine(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sample {
    pub own: Own,
    /// Status changes the engine lost, counted.
    pub status_changes_lost: u64,
    /// Appended entries the engine refused: it lacks a committed entry,
    /// so its output no longer follows the log.
    pub entries_refused: u64,
    /// Per output, realised.
    pub realised: Vec<bool>,
    /// The generation its pending entries need is built, or none is.
    pub generation_ready: bool,
}

/// The voters of the applied membership: one configuration, or two
/// during a joint change, each of which a majority must hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Members {
    pub configs: Vec<BTreeSet<NodeId>>,
    pub addrs: BTreeMap<NodeId, String>,
}

impl Members {
    pub fn of(metrics: &RaftMetrics<NodeId, BasicNode>) -> Members {
        let membership = metrics.membership_config.membership();
        Members {
            configs: membership.get_joint_config().clone(),
            addrs: membership
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
        }
    }

    pub fn voters(&self) -> BTreeSet<NodeId> {
        self.configs.iter().flatten().copied().collect()
    }

    /// Whether `holds` holds a majority of every configuration.
    pub(crate) fn majority(&self, holds: impl Fn(NodeId) -> bool) -> bool {
        !self.configs.is_empty()
            && self
                .configs
                .iter()
                .all(|c| c.iter().filter(|&&v| holds(v)).count() > c.len() / 2)
    }
}

/// The log as contact reads it.
pub trait LogView: Send + Sync + 'static {
    fn committed(&self) -> Option<StoredLogId>;
    fn applied(&self) -> u64;
    fn log_id_at(&self, index: u64) -> Option<StoredLogId>;
    fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request));
    fn pending(&self, f: &mut dyn FnMut(u64, &Body));
    /// The request of the entry at `index`, from the log, else, purged,
    /// the engine body the state machine holds pending under it.
    fn request(&self, index: u64) -> Option<Request> {
        let mut found = None;
        self.entries_after(index.saturating_sub(1), &mut |id, request| {
            if id.index == index && found.is_none() {
                found = Some(request.clone());
            }
        });
        if found.is_none() {
            self.pending(&mut |i, body| {
                if i == index {
                    found = Some(Request {
                        stamp: 0,
                        body: body.clone(),
                    });
                }
            });
        }
        found
    }
    /// The index of the last purged log id, 0 for none.
    fn purged(&self) -> u64 {
        0
    }
    /// Snapshots installed from a leader so far.
    fn installs(&self) -> u64 {
        0
    }
    /// The restamps behind the entry at `index`, as applied.
    fn restamps(&self, _index: u64) -> u32 {
        0
    }
    /// The replicas the latest applied `ClockVerdict` judges.
    fn judged(&self) -> BTreeSet<NodeId>;
    /// The replica the latest applied `ClockVerdict` judging one named.
    fn latest_judged(&self) -> Option<NodeId>;
    /// The latest applied `Baseline`.
    fn baseline(&self) -> Option<Baseline>;
    /// The voter holding the disruption slot.
    fn disrupting(&self) -> Option<NodeId>;
}

impl<P: Product> LogView for Store<P> {
    fn committed(&self) -> Option<StoredLogId> {
        Store::committed(self)
    }
    fn applied(&self) -> u64 {
        self.applied_index()
    }
    fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
        Store::log_id_at(self, index)
    }
    fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request)) {
        Store::entries_after(self, index, f)
    }
    fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
        Store::pending(self, f)
    }
    fn purged(&self) -> u64 {
        self.purged_index()
    }
    fn installs(&self) -> u64 {
        Store::installs(self)
    }
    fn restamps(&self, index: u64) -> u32 {
        Store::restamps(self, index)
    }
    fn judged(&self) -> BTreeSet<NodeId> {
        self.with_state(|sm| {
            sm.verdicts
                .iter()
                .filter(|&(_, &judged)| judged)
                .map(|(&id, _)| id)
                .collect()
        })
    }
    fn latest_judged(&self) -> Option<NodeId> {
        self.with_state(|sm| sm.latest_judged)
    }
    fn baseline(&self) -> Option<Baseline> {
        self.with_state(|sm| sm.baseline.clone())
    }
    fn disrupting(&self) -> Option<NodeId> {
        self.with_state(|sm| sm.slot.as_ref().map(|d| d.node))
    }
}

/// The lead before any slack was reported: 30 ms.
pub const START_LEAD: Duration = Duration::from_millis(30);

/// Entries with an at_index the engine holds at most (plan 2.7): the
/// leader stamps no entry beyond them.
pub const FUTURE_LIST: usize = 64;

/// Sequence numbers sent, with their TAI time, kept for echoes: 2 s at
/// the default period. An older echo counts for nothing.
const SENT_KEPT: usize = 1024;

/// Echoes kept per peer: 128 ms at the default period, past the hold
/// cap of every block not yet final.
const ECHOES_KEPT: usize = 64;

/// Entries this replica knows of and waits for the commit of, at most;
/// one more is unresolved at once.
const AWAITING_MAX: usize = 1024;

/// Entries with an at_index kept for listings, at most; the lowest index
/// is dropped first, counted.
const SCHEDULED_MAX: usize = 4096;

/// Entries given to the engine as unresolved that are remembered, so a
/// listing that names one again does not give it again.
const GIVEN_KEPT: usize = 1024;

/// The ids of the unresolvable entries a listing's overflow names: this
/// bit and the omitted at_index in blocks, so replicas that hold one for
/// the same index hold the same id, and none equals a log id or the
/// engine's own reserved `u64::MAX`.
pub const UNLISTED_BIT: u64 = 1 << 63;

/// The id of the unresolvable entry at `at_index` a listing's overflow
/// names.
pub fn unlisted_id(at_index: u64, block: u64) -> u64 {
    UNLISTED_BIT | (at_index / block)
}

/// A log id as a map key, in log order: index, then term and node.
type Key = (u64, u64, u64);

fn key(id: StoredLogId) -> Key {
    (id.index, id.term, id.node)
}

/// A report whose seq lies this far or less below the newest one taken
/// from its voter is older than that one; one further below comes from a
/// restarted process, which drew a new first seq.
const SEQ_WINDOW: u64 = 1 << 20;

/// What a voter received directly from another.
#[derive(Debug, Clone)]
struct Direct {
    seq: u64,
    /// This replica's TAI and monotonic time at receipt.
    received_tai: u64,
    received: Instant,
    own: Own,
    committed: Option<StoredLogId>,
}

#[derive(Debug, Default)]
struct Peer {
    last: Option<Direct>,
    /// The peer's latest echoes: the TAI time this replica sent the report
    /// echoed, and the `silentAt` the echo carried.
    echoes: VecDeque<(u64, Option<u64>)>,
    silent: bool,
    /// The latest TAI time this replica held the peer silent.
    silent_at: Option<u64>,
    /// The newest copy a third voter relayed, when newer than the direct
    /// one: when the peer generated it, by this replica's clock.
    relayed: Option<(Instant, Own)>,
    /// The latest NTP-style offset estimate, the peer's clock minus this
    /// one's, ns.
    offset_ns: Option<i64>,
    /// Its magnitude over the exchanges, ns.
    offsets: Histogram,
    /// The latest offsets, for their rolling MAD.
    thetas: crate::clock::Thetas,
    delay: Histogram,
    gaps: Histogram,
    gaps_over_window: u64,
    reports: u64,
}

#[derive(Debug, Clone, Copy)]
struct Waiting {
    at_index: u64,
    /// Past its apply deadline: given to the engine at once.
    expired: bool,
}

#[derive(Debug, Default)]
struct Counts {
    shared_gaps: u64,
    shared_gap_max: Duration,
    stagger_max: Duration,
    /// The start of the recovery from a gap on every inbound link, and
    /// the voters not heard since.
    recovering: Option<(Instant, BTreeSet<NodeId>)>,
    last_any: Option<Instant>,
    status_lag: Histogram,
    unresolved: u64,
    unresolved_refused: u64,
    awaiting_overflow: u64,
    scheduled_dropped: u64,
    installs: u64,
    purge_points: u64,
    contradicted: u64,
}

struct State {
    members: Members,
    seq: u64,
    sent: VecDeque<(u64, u64)>,
    peers: BTreeMap<NodeId, Peer>,
    in_contact: bool,
    regained: Instant,
    /// The last log index handed to the engine as appended.
    told: u64,
    /// Entries handed as appended whose commit is not known.
    uncommitted: BTreeMap<u64, StoredLogId>,
    /// Entries with an at_index, for listings, until every voter reported
    /// them committed: log id, at_index, stamp, and whether it is a
    /// `Baseline`.
    scheduled: BTreeMap<u64, (StoredLogId, u64, u64, bool)>,
    /// The at_index of the latest generation-needing entry appended.
    generation_at: Option<u64>,
    /// When `own` was sampled.
    own_at: Instant,
    judged: BTreeSet<NodeId>,
    /// Slack of the entries learned committed, for the next reports.
    slack: Vec<Slack>,
    lead: Lead,
    e_spread: Histogram,
    /// Every entry of this replica's log up to here is committed.
    known_committed: u64,
    handed: u64,
    awaiting: BTreeMap<Key, Waiting>,
    /// Entries given to the engine as unresolved, or refused by it, the
    /// lowest index forgotten first.
    given: BTreeMap<Key, u64>,
    /// The lowest at_index of a listing overflow given as unresolved: a
    /// later overflow at or above it adds nothing, one below it is given.
    unlisted_low: Option<u64>,
    /// Per listing overflow given as unresolved, the committed index of
    /// the report that carried it: once this log holds and knows committed
    /// every entry up to it, each one the overflow left out is one this
    /// replica knows by its own id, so the overflow is resolved.
    unlisted: BTreeMap<u64, u64>,
    /// Restamps and voids appended, by log index, with the index and term
    /// they name, until their commit is known.
    decisions: Vec<(u64, u64, u64)>,
    /// A purge-point verdict naming this replica, by log index, with the
    /// index its applied index must pass, until its commit is known.
    purge_points: Vec<(u64, u64)>,
    /// Back in `starting` until the applied index passes this and the
    /// engine is rebuilt from product state.
    restart: Option<u64>,
    /// The log's snapshot installs acted on.
    installs_seen: u64,
    fence: Option<u64>,
    /// The ranks last published, with the first block F had not passed
    /// then: blocks below it keep their rank.
    ranks: Option<(Ranks, u64)>,
    baselines: Baselines,
    withhold: Withhold,
    /// The audio replica's state; none for the witness.
    life: Option<Life>,
    /// Every voter's page hashes, this one's included.
    pages: PageBook,
    leader: Leader,
    clock: crate::clock::Judge,
    decider: crate::decide::Decider,
    /// The audio `/readyz` term, as of the last period in contact; none
    /// before the first.
    serving: Option<bool>,
    /// `Diverged` verdicts naming this replica, by log index and page,
    /// until their commit is known.
    verdicts: Vec<(u64, u64)>,
    realised: Vec<bool>,
    generation_ready: bool,
    own: Own,
    status_changes_lost: u64,
    entries_refused: u64,
    counts: Counts,
}

/// One voter's side of contact.
pub struct Contact {
    id: NodeId,
    timing: Timing,
    clocks: Arc<dyn Clocks>,
    audio: Option<Arc<dyn Audio>>,
    log: Arc<dyn LogView>,
    state: Mutex<State>,
}

/// Contact as status and metrics show it.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    /// F, none before it is first computed.
    pub fence: Option<u64>,
    pub in_contact: bool,
    pub known_committed: u64,
    pub peers: Vec<PeerMetrics>,
    /// Gaps longer than w in which no voter's report arrived.
    pub shared_gaps: u64,
    pub shared_gap_max: Duration,
    /// After such a gap, the longest from the first voter heard again to
    /// the last.
    pub stagger_max: Duration,
    /// statusLag: from `T_k` until the block's status was seen here,
    /// 99.99th percentile, as of the report period it was read in.
    pub status_lag_p9999: Option<Duration>,
    pub status_changes_lost: u64,
    /// Appended entries the engine refused.
    pub entries_refused: u64,
    /// Entries given to the engine as unresolved, and refusals of it.
    pub unresolved: u64,
    pub unresolved_refused: u64,
    /// Entries unresolved at once for want of room to wait for them.
    pub awaiting_overflow: u64,
    /// Entries dropped from the listings for want of room.
    pub scheduled_dropped: u64,
    /// Snapshot installs and purge-point verdicts that returned the
    /// replica to `starting`.
    pub snapshot_installs: u64,
    pub purge_points: u64,
    /// Decisions on entries this replica had applied, each diverging it.
    pub contradicted: u64,
    /// E, and the audio replicas it counts.
    pub estimate: Option<u64>,
    pub counted: Vec<NodeId>,
    /// The lead, in samples, and whether it wanted more than the cap.
    pub lead: u64,
    pub apply_lead_capped: bool,
    /// How far the E estimates other voters report lie from this one's at
    /// receipt, 99.99th percentile, in samples.
    pub e_spread_p9999: Option<u64>,
    /// The supervisor's moves on this replica's blocks.
    pub withholding: crate::withhold::Counts,
    /// `L = 2p + 2 delta + |theta|` (Withholding, floors), from delta and
    /// the exchange offset |theta| measured here, 99.99th percentiles;
    /// none before both are.
    pub floor: Option<Duration>,
    /// `w < L` or `D < statusLag + L`: a fenced replica's `withheld` or a
    /// live replica's status may miss the hold cap.
    pub hold_floor_unmet: bool,
    /// The audio replica's state and its moves; none for the witness.
    pub replica: Option<crate::state::Metrics>,
    /// The decisions taken while this voter led.
    pub leader: crate::baseline::Counts,
    /// `replica_disagreement`: the last page this voter judged as leader
    /// had no strict majority hash.
    pub replica_disagreement: bool,
    /// The clock check from this voter's offsets.
    pub clock: crate::clock::Status,
    /// `ClockVerdict` entries appended while this voter led.
    pub clock_verdicts: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerMetrics {
    pub node: NodeId,
    pub reports: u64,
    /// delta: the one-way report delay, half the exchange's round trip
    /// less the peer's turnaround, 99.99th percentile.
    pub delay_p9999: Option<Duration>,
    /// The longest gap between two of its reports, and those above w.
    pub gap_max: Duration,
    pub gaps_over_window: u64,
    /// This replica holds it silent.
    pub silent: bool,
    /// Its clock minus this one's, from the latest exchange, ns.
    pub offset_ns: Option<i64>,
}

fn since(now: Instant, then: Instant) -> Duration {
    now.saturating_duration_since(then)
}

impl Contact {
    /// Contact for voter `id`; `first_seq` should be drawn at random.
    pub fn new(
        id: NodeId,
        timing: Timing,
        clocks: Arc<dyn Clocks>,
        audio: Option<Arc<dyn Audio>>,
        log: Arc<dyn LogView>,
        first_seq: u64,
    ) -> Contact {
        let applied = log.applied();
        let installs_seen = log.installs();
        let mut scheduled = BTreeMap::new();
        let mut generation_at = None;
        // Entries the engine applies later, purged ones included, for
        // listings.
        // Collected first: the log's view is not re-entered from inside it.
        let mut pending = Vec::new();
        log.pending(&mut |index, body| {
            if body.needs_generation() {
                generation_at = generation_at.max(body.at_index());
            }
            if let Some(at) = body.at_index() {
                pending.push((index, at));
            }
        });
        for (index, at) in pending {
            let id = log.log_id_at(index).unwrap_or(StoredLogId {
                term: 0,
                node: 0,
                index,
            });
            scheduled.insert(index, (id, at, 0, false));
        }
        let mut baselines = Baselines::default();
        if let Some(b) = log.baseline() {
            baselines.applied(b);
        }
        let life = audio.is_some().then(Life::new);
        Contact {
            id,
            timing,
            clocks,
            audio,
            log,
            state: Mutex::new(State {
                members: Members::default(),
                seq: first_seq,
                sent: VecDeque::with_capacity(SENT_KEPT),
                peers: BTreeMap::new(),
                in_contact: false,
                regained: Instant::now(),
                // The engine is built from the state applied; entries after
                // it are handed as they come.
                told: applied,
                uncommitted: BTreeMap::new(),
                scheduled,
                generation_at,
                own_at: Instant::now(),
                judged: BTreeSet::new(),
                slack: Vec::new(),
                lead: Lead::new(
                    timing.block,
                    index_at(START_LEAD.as_nanos() as u64),
                    index_at(timing.max_apply_lead.as_nanos() as u64),
                ),
                e_spread: Histogram::default(),
                known_committed: applied,
                handed: applied,
                awaiting: BTreeMap::new(),
                given: BTreeMap::new(),
                unlisted_low: None,
                unlisted: BTreeMap::new(),
                decisions: Vec::new(),
                purge_points: Vec::new(),
                restart: None,
                installs_seen,
                fence: None,
                ranks: None,
                baselines,
                withhold: Withhold::default(),
                life,
                pages: PageBook::default(),
                leader: Leader::default(),
                clock: crate::clock::Judge::default(),
                decider: crate::decide::Decider::default(),
                serving: None,
                verdicts: Vec::new(),
                realised: Vec::new(),
                generation_ready: true,
                own: Own::default(),
                status_changes_lost: 0,
                entries_refused: 0,
                counts: Counts::default(),
            }),
        }
    }

    pub fn timing(&self) -> Timing {
        self.timing
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("contact lock poisoned")
    }

    /// Takes a report from another voter.
    pub fn receive(&self, report: Report) {
        let now = Instant::now();
        let tai = self.clocks.tai_ns();
        let w = self.timing.contact_window;
        let mut st = self.lock();
        let st = &mut *st;
        let voters = st.members.voters();
        if report.from == self.id || !voters.contains(&report.from) {
            return;
        }
        self.expire(st, tai);
        // Gaps on every inbound link, and how staggered they recover.
        let c = &mut st.counts;
        if let Some(last) = c.last_any
            && since(now, last) > w
        {
            c.shared_gaps += 1;
            c.shared_gap_max = c.shared_gap_max.max(since(now, last));
            let rest: BTreeSet<NodeId> = voters
                .iter()
                .copied()
                .filter(|&v| v != self.id && v != report.from)
                .collect();
            c.recovering = (!rest.is_empty()).then_some((now, rest));
        } else if let Some((start, rest)) = &mut c.recovering {
            rest.remove(&report.from);
            if rest.is_empty() {
                c.stagger_max = c.stagger_max.max(since(now, *start));
                c.recovering = None;
            }
        }
        c.last_any = Some(now);

        let sent_at = |seq| {
            st.sent
                .iter()
                .rev()
                .find(|&&(s, _)| s == seq)
                .map(|&(_, t)| t)
        };
        let t1 = report.echo.and_then(|e| sent_at(e.seq));
        let peer = st.peers.entry(report.from).or_default();
        // A copy, or a report delayed past a newer one from the same
        // process.
        if peer
            .last
            .as_ref()
            .is_some_and(|l| l.seq.wrapping_sub(report.seq) < SEQ_WINDOW)
        {
            return;
        }
        peer.reports += 1;
        if let Some(last) = &peer.last {
            let gap = since(now, last.received);
            peer.gaps.add(gap.as_nanos() as u64);
            if gap > w {
                peer.gaps_over_window += 1;
            }
        }
        if let (Some(echo), Some(t1)) = (report.echo, t1) {
            if peer.echoes.len() == ECHOES_KEPT {
                peer.echoes.pop_front();
            }
            peer.echoes.push_back((t1, echo.silent_at));
            let (t1, t2, t3, t4) = (
                i128::from(t1),
                i128::from(echo.received_at),
                i128::from(report.sent_at),
                i128::from(tai),
            );
            let delay = (t4 - t1) - (t3 - t2);
            if delay >= 0 {
                peer.delay.add((delay / 2) as u64);
            }
            let offset = (((t2 - t1) + (t3 - t4)) / 2) as i64;
            peer.offset_ns = Some(offset);
            peer.offsets.add(offset.unsigned_abs());
            peer.thetas.add(offset, (delay.max(0) / 2) as u64);
        }
        st.withhold.fold(self.id, report.from, &report.own);
        st.pages.fold(report.from, &report.own.pages);
        peer.last = Some(Direct {
            seq: report.seq,
            received_tai: tai,
            received: now,
            own: report.own,
            committed: report.committed,
        });
        // One relay hop: copies of voters the sender heard directly, and
        // whom it holds silent.
        for heard in report.heard {
            if heard.node == self.id || heard.node == report.from || !voters.contains(&heard.node) {
                continue;
            }
            if heard.silent {
                st.withhold
                    .held_silent(report.from, heard.node, report.sent_at);
            }
            let (Some(age), Some(own)) = (heard.age_us, heard.own) else {
                continue;
            };
            let other = st.peers.entry(heard.node).or_default();
            let generated = now.checked_sub(Duration::from_micros(age)).unwrap_or(now);
            if other.last.as_ref().is_none_or(|d| d.received < generated)
                && other.relayed.as_ref().is_none_or(|(g, _)| *g < generated)
            {
                st.withhold.fold(self.id, heard.node, &own);
                st.pages.fold(heard.node, &own.pages);
                other.relayed = Some((generated, own));
            }
        }
        if let (Some(theirs), Some(mine)) = (report.estimate, self.estimate_in(st, now)) {
            st.e_spread.add(theirs.abs_diff(mine));
        }
        if self.counted(st, now).contains(&report.from) {
            let current = self.current_stamp(st, now);
            for slack in &report.slack {
                st.lead.add(slack.samples, slack.stamp, current);
            }
        }
        // Commit knowledge: a committed id this log holds, or a listed
        // entry's exact id at or below the sender's committed index.
        let mut learned = st.known_committed;
        if let Some(c) = report.committed
            && c.index > learned
            && self.log.log_id_at(c.index) == Some(c)
        {
            learned = c.index;
        }
        let committed_index = report.committed.map_or(0, |c| c.index);
        for l in &report.listed {
            if l.id.index > learned
                && l.id.index <= committed_index
                && self.log.log_id_at(l.id.index) == Some(l.id)
            {
                learned = l.id.index;
            }
        }
        self.learn(st, learned);
        for l in &report.listed {
            if l.baseline {
                st.baselines.listed(l.id, l.at_index);
            } else {
                self.note(st, l.id, l.at_index, tai);
            }
        }
        if let Some(at) = report.unlisted
            && st.unlisted_low.is_none_or(|low| at < low)
        {
            let id = StoredLogId {
                term: 0,
                node: 0,
                index: unlisted_id(at, self.timing.block),
            };
            // Without a committed id from the sender nothing bounds what
            // the overflow left out: it stays until a report that has one.
            let covered = report.committed.map_or(u64::MAX, |c| c.index);
            let held = st.unlisted.entry(id.index).or_insert(covered);
            if *held == u64::MAX || covered != u64::MAX {
                *held = if *held == u64::MAX {
                    covered
                } else {
                    (*held).max(covered)
                };
            }
            self.note(st, id, at, tai);
        }
        self.fence(st, tai);
    }

    /// The log changed: appends and truncations go to the engine, and an
    /// entry openraft applied is known committed.
    pub fn log_changed(&self) {
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        self.expire(st, tai);
        self.sync_log(st, tai);
        let applied = self.log.applied();
        self.learn(st, applied);
    }

    /// One report period: the log, the deadlines, F, silence, and the
    /// reports to send, with each addressee's address.
    pub fn tick(&self, members: Members) -> Vec<(NodeId, String, Report)> {
        let now = Instant::now();
        // Taken before the engine's sample: every timestamp a report
        // carries precedes the flags beside it.
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        let voters = members.voters();
        st.peers.retain(|id, _| voters.contains(id));
        st.members = members;
        self.expire(st, tai);
        self.sync_log(st, tai);
        let applied = self.log.applied();
        self.learn(st, applied);

        if let Some(audio) = &self.audio {
            let sample = audio.sample();
            if let (Some(k), last) = (sample.own.processed, st.own.processed)
                && last.is_none_or(|l| k > l)
            {
                let lag = tai.saturating_sub(self.timing.block_deadline(k));
                st.counts.status_lag.add(lag);
            }
            st.own = sample.own;
            st.own_at = now;
            st.status_changes_lost = sample.status_changes_lost;
            st.entries_refused = sample.entries_refused;
            st.realised = sample.realised;
            st.generation_ready = sample.generation_ready;
            st.pages.fold(self.id, &st.own.pages);
            st.own.unresolved = Self::unresolved(st);
        }
        st.judged = self.log.judged();
        st.lead.roll();

        // Contact: reports from a majority, itself included, within w.
        let w = self.timing.contact_window;
        let heard_within = |st: &State, v: NodeId| {
            v == self.id
                || st
                    .peers
                    .get(&v)
                    .and_then(|p| p.last.as_ref())
                    .is_some_and(|d| since(now, d.received) <= w)
        };
        let in_contact = st.members.majority(|v| heard_within(st, v));
        if in_contact && !st.in_contact {
            st.regained = now;
        }
        st.in_contact = in_contact;
        for v in &voters {
            if *v == self.id {
                continue;
            }
            let regained = st.regained;
            let p = st.peers.entry(*v).or_default();
            let from = p
                .last
                .as_ref()
                .map_or(regained, |d| d.received.max(regained));
            p.silent = in_contact && since(now, from) > w;
            if p.silent {
                p.silent_at = Some(tai);
            }
        }
        st.withhold.retain(&st.members);
        st.pages.retain(|v| voters.contains(&v));
        self.fence(st, tai);
        self.decide(st, tai);
        self.live(st, now, tai);

        // Reports.
        if !voters.contains(&self.id) {
            return Vec::new();
        }
        st.seq += 1;
        if st.sent.len() == SENT_KEPT {
            st.sent.pop_front();
        }
        st.sent.push_back((st.seq, tai));
        self.prune_scheduled(st);
        // The commit point this replica knows, reports included, so peers
        // list nothing it knows committed.
        let committed = match (self.log.committed(), self.log.log_id_at(st.known_committed)) {
            (Some(c), Some(k)) if k.index > c.index => Some(k),
            (None, k) => k,
            (c, _) => c,
        };
        let limit = index_at(
            tai + (self.timing.max_apply_lead + self.timing.contact_window).as_nanos() as u64,
        );
        let estimate = self.estimate_in(st, now);
        let mut slack = std::mem::take(&mut st.slack);
        slack.truncate(MAX_LISTED);
        let mut out = Vec::new();
        for to in voters.iter().copied().filter(|&v| v != self.id) {
            let Some(addr) = st.members.addrs.get(&to).cloned() else {
                continue;
            };
            let peer = st.peers.get(&to);
            let echo = peer.and_then(|p| {
                p.last.as_ref().map(|d| Echo {
                    seq: d.seq,
                    received_at: d.received_tai,
                    silent_at: p.silent_at,
                })
            });
            let their_committed = peer
                .and_then(|p| p.last.as_ref())
                .and_then(|d| d.committed)
                .map_or(0, |c| c.index);
            let mut listed: Vec<Listed> = st
                .scheduled
                .range(their_committed.saturating_add(1)..)
                .filter(|(_, (_, at, _, _))| *at < limit)
                .map(|(_, &(id, at_index, _, baseline))| Listed {
                    id,
                    at_index,
                    baseline,
                })
                .collect();
            listed.sort_by_key(|l| (l.at_index, l.id.index));
            let unlisted = listed.get(MAX_LISTED).map(|l| l.at_index);
            listed.truncate(MAX_LISTED);
            let heard = voters
                .iter()
                .copied()
                .filter(|&v| v != self.id && v != to)
                .map(|v| {
                    let p = st.peers.get(&v);
                    let d = p.and_then(|p| p.last.as_ref());
                    Heard {
                        node: v,
                        age_us: d.map(|d| since(now, d.received).as_micros() as u64),
                        silent: p.is_some_and(|p| p.silent),
                        own: d.map(|d| d.own.clone()),
                    }
                })
                .collect();
            out.push((
                to,
                addr,
                Report {
                    from: self.id,
                    seq: st.seq,
                    sent_at: tai,
                    echo,
                    committed,
                    own: st.own.clone(),
                    heard,
                    listed,
                    unlisted,
                    estimate,
                    slack: slack.clone(),
                },
            ));
        }
        out
    }

    /// Entries whose apply deadline passed with the commit unknown are
    /// unresolved. Runs before any new knowledge is taken, so knowledge
    /// learned after a deadline never counts as before it.
    fn expire(&self, st: &mut State, tai: u64) {
        let timing = self.timing;
        for w in st.awaiting.values_mut() {
            if tai >= timing.apply_deadline(w.at_index) {
                w.expired = true;
            }
        }
        self.offer_unresolved(st);
    }

    /// Gives the expired entries to the engine.
    fn offer_unresolved(&self, st: &mut State) {
        let expired: Vec<(Key, u64)> = st
            .awaiting
            .iter()
            .filter(|(_, w)| w.expired)
            .map(|(&k, w)| (k, w.at_index))
            .collect();
        for (k, _) in &expired {
            st.awaiting.remove(k);
        }
        let Some(audio) = &self.audio else {
            return;
        };
        for (k, at) in expired {
            let (index, term, node) = k;
            // This log's own entry is never applied: the log decides it.
            if index & UNLISTED_BIT == 0
                && self.log.log_id_at(index) == Some(StoredLogId { term, node, index })
            {
                audio.skip(index);
            }
            if audio.unresolved(index, at) {
                st.counts.unresolved += 1;
                if index & UNLISTED_BIT != 0 {
                    st.unlisted_low = Some(st.unlisted_low.map_or(at, |l| l.min(at)));
                }
                // Listed again, an entry given once is not given again.
                st.given.insert(k, at);
                while st.given.len() > GIVEN_KEPT {
                    st.given.pop_first();
                }
            } else {
                st.counts.unresolved_refused += 1;
                audio.overwhelmed();
                // Kept with the given ones: listed again, it neither
                // waits nor withdraws again.
                st.given.insert(k, at);
                while st.given.len() > GIVEN_KEPT {
                    st.given.pop_first();
                }
            }
        }
    }

    /// Hands new appends and truncations to the engine.
    fn sync_log(&self, st: &mut State, tai: u64) {
        let cut = st
            .uncommitted
            .iter()
            .find(|&(&index, &id)| self.log.log_id_at(index) != Some(id))
            .map(|(&index, _)| index);
        if let Some(from) = cut {
            for (_, id) in st.uncommitted.split_off(&from) {
                st.awaiting.remove(&key(id));
            }
            st.scheduled.split_off(&from);
            st.baselines.truncated(from);
            st.verdicts.retain(|&(index, _)| index < from);
            st.decisions.retain(|&(index, _, _)| index < from);
            st.purge_points.retain(|&(index, _)| index < from);
            st.told = from - 1;
            if let Some(audio) = &self.audio {
                audio.truncated(from);
            }
        }
        // Collected first: the log's view is not re-entered from inside
        // it, and the engine takes appends in log order.
        let mut appended: Vec<(StoredLogId, Request)> = Vec::new();
        self.log.entries_after(st.told, &mut |id, request| {
            appended.push((id, request.clone()));
        });
        // A snapshot was installed: the state jumped past entries this
        // replica may never have held. A purge of its own log after its
        // own snapshot is none: it applied every entry purged.
        let installs = self.log.installs();
        if installs > st.installs_seen {
            st.installs_seen = installs;
            let applied = self.log.applied();
            self.installed(st, applied);
            appended.retain(|(id, _)| id.index > applied);
        }
        let mut new = Vec::new();
        for (id, request) in appended {
            if id.index <= st.told {
                continue;
            }
            if let Some(audio) = &self.audio {
                match self.engine_request(&request) {
                    Some(engine) => audio.appended(id.index, &engine),
                    None => audio.appended(id.index, &request),
                }
                // Given unresolved from a listing before it arrived here:
                // never applied.
                if st.given.contains_key(&key(id)) {
                    audio.skip(id.index);
                }
            }
            if request.body.needs_generation() {
                st.generation_at = request.at_index();
            }
            match &request.body {
                Body::Baseline(b) => st.baselines.appended(id, b),
                Body::Diverged { replica, page } if *replica == self.id => {
                    st.verdicts.push((id.index, *page))
                }
                Body::PurgePoint { replica, index } if *replica == self.id => {
                    st.purge_points.push((id.index, *index))
                }
                body => {
                    if let Some((revision, term)) = body.decides() {
                        st.decisions.push((id.index, revision, term));
                    }
                }
            }
            let baseline = match &request.body {
                Body::Baseline(b) => Some(b.at_index),
                _ => None,
            };
            new.push((id, request.at_index(), baseline, request.stamp));
        }
        for (id, at, baseline, stamp) in new {
            st.told = id.index;
            if id.index > st.known_committed {
                st.uncommitted.insert(id.index, id);
            }
            if let Some(at) = at {
                st.scheduled.insert(id.index, (id, at, stamp, false));
                self.note(st, id, at, tai);
            }
            // Listed, never unresolved: it changes no engine state.
            if let Some(at) = baseline {
                st.scheduled.insert(id.index, (id, at, stamp, true));
            }
        }
        while st.scheduled.len() > SCHEDULED_MAX {
            st.scheduled.pop_first();
            st.counts.scheduled_dropped += 1;
        }
        self.hand(st);
    }

    /// What the engine applies for a `Restamp`: the entry it names, under
    /// whatever restamps lie behind that, at the restamp's at_index; none
    /// for every other entry, which goes to the engine as it is.
    fn engine_request(&self, request: &Request) -> Option<Request> {
        let Body::Restamp { at_index, .. } = request.body else {
            return None;
        };
        let mut of = request.body.decides()?.0;
        // Each restamp names the one before it; at most three lie behind
        // an entry, and a missing one ends the walk.
        for _ in 0..=crate::decide::RESTAMPS_MAX {
            let named = self.log.request(of)?;
            match named.body {
                Body::Restamp { revision, .. } => of = revision,
                mut body => {
                    crate::state_machine::restamp(&mut body, at_index);
                    return Some(Request {
                        stamp: request.stamp,
                        body,
                    });
                }
            }
        }
        None
    }

    /// A snapshot replaced the state up to `applied`: the engine is built
    /// again from it (plan 2.11, Unresolved entries), and every entry the
    /// replica held unresolved is resolved by it.
    fn installed(&self, st: &mut State, applied: u64) {
        st.told = applied;
        st.known_committed = st.known_committed.max(applied);
        st.handed = st.handed.max(applied);
        st.uncommitted.clear();
        st.awaiting.clear();
        st.given.clear();
        st.unlisted.clear();
        st.unlisted_low = None;
        st.verdicts.retain(|&(index, _)| index > applied);
        st.decisions.retain(|&(index, _, _)| index > applied);
        st.purge_points.retain(|&(index, _)| index > applied);
        st.counts.installs += 1;
        self.begin_restart(st, applied);
    }

    /// Returns the replica to `starting` until its applied index passed
    /// `after` and its engine was rebuilt from product state.
    fn begin_restart(&self, st: &mut State, after: u64) {
        st.restart = Some(st.restart.map_or(after, |a| a.max(after)));
        if let (Some(audio), Some(life)) = (&self.audio, st.life.as_mut()) {
            life.restart(audio.as_ref());
            audio.drop_engine();
        }
    }

    /// An entry this replica knows of: unresolved at once when its apply
    /// deadline has passed, else awaited unless its commit is known.
    fn note(&self, st: &mut State, id: StoredLogId, at_index: u64, tai: u64) {
        // At or below the known commit point an entry is this log's own,
        // committed and handed when it was appended, or one that can no
        // longer commit.
        let k = key(id);
        if id.index <= st.known_committed
            || st.awaiting.contains_key(&k)
            || st.given.contains_key(&k)
        {
            return;
        }
        let full = st.awaiting.len() >= AWAITING_MAX;
        let expired = tai >= self.timing.apply_deadline(at_index);
        if full && !expired {
            st.counts.awaiting_overflow += 1;
        }
        st.awaiting.insert(
            k,
            Waiting {
                at_index,
                expired: expired || full,
            },
        );
        if expired || full {
            self.offer_unresolved(st);
        }
    }

    /// Every entry of this replica's log up to `index` is committed.
    /// Awaited entries at or below it are settled: this replica's own,
    /// or another at the same index that can no longer commit.
    fn learn(&self, st: &mut State, index: u64) {
        if index <= st.known_committed {
            return;
        }
        st.known_committed = index;
        let log = &self.log;
        st.baselines
            .learned(index, self.clocks.tai_ns(), |i| log.log_id_at(i));
        st.uncommitted = st.uncommitted.split_off(&(index + 1));
        // Awaited entries only wait unexpired: expired ones were given.
        st.awaiting = st.awaiting.split_off(&(index + 1, 0, 0));
        self.hand(st);
    }

    fn hand(&self, st: &mut State) {
        let up_to = st.known_committed.min(st.told);
        if up_to > st.handed && self.audio.is_some() {
            // Slack against the apply deadline: the first index this
            // replica could still apply an entry at, learned now.
            let tai = self.clocks.tai_ns();
            let first = index_at(tai + self.timing.apply_guard_ns()) as i64;
            let now = Instant::now();
            let counted = self.counted(st, now).contains(&self.id);
            let current = self.current_stamp(st, now);
            let handed: Vec<Slack> = st
                .scheduled
                .range(st.handed + 1..=up_to)
                .filter(|(_, e)| !e.3)
                .map(|(&index, &(_, at, stamp, _))| Slack {
                    index,
                    samples: at as i64 - first,
                    stamp,
                })
                .collect();
            for s in handed {
                if counted {
                    st.lead.add(s.samples, s.stamp, current);
                }
                if st.slack.len() < MAX_LISTED {
                    st.slack.push(s);
                }
            }
        }
        if up_to > st.handed {
            st.handed = up_to;
            if let Some(audio) = &self.audio {
                audio.committed(up_to);
            }
        }
    }

    /// F (B1): block k is valid when a majority of every configuration,
    /// this replica included, echoed a report it sent at or after
    /// `T((k + 1)B) - w`, an echo whose `silentAt` lies at or after
    /// `T_k - clockGuard` not counted. F is the end of the run of blocks
    /// from the last F that are valid or settled (final, or never
    /// computed), so it never skips a block that may still commit. The
    /// witness, which computes nothing, counts a block as settled once
    /// its hold cap passed.
    fn fence(&self, st: &mut State, tai: u64) {
        let t = self.timing;
        if st.members.configs.is_empty() {
            return;
        }
        let w = t.contact_window.as_nanos() as u64;
        let guard = time_of(t.clock_guard);
        let cap = (t.hold + t.contact_window).as_nanos() as u64;
        let mut k = match st.fence {
            Some(f) => f / t.block,
            // Below every block an output's hold ring can still hold.
            None => {
                let due = index_at(tai).saturating_sub(t.margin) / t.block;
                due.saturating_sub(2 * index_at(cap + w).div_ceil(t.block) + 4)
            }
        };
        let echoed = |st: &State, v: NodeId, k: u64| -> Option<u64> {
            if v == self.id {
                return Some(tai);
            }
            let void_from = t.block_deadline(k).saturating_sub(guard);
            st.peers
                .get(&v)?
                .echoes
                .iter()
                .filter(|(_, s)| s.is_none_or(|s| s < void_from))
                .map(|&(t1, _)| t1)
                .max()
        };
        // No echo is newer than now: a block ending more than w ahead is
        // never valid, so the scan ends there whatever `settled` says.
        let end = index_at(tai + w) / t.block;
        while k <= end {
            let start = time_of((k + 1) * t.block).saturating_sub(w);
            let valid = st
                .members
                .majority(|v| echoed(st, v, k).is_some_and(|t1| t1 >= start));
            let settled = match &self.audio {
                Some(audio) => !valid && audio.settled(k),
                None => t.block_deadline(k) + cap < tai,
            };
            if !valid && !settled {
                break;
            }
            k += 1;
        }
        let mut f = k * t.block;
        if let Some(audio) = &self.audio {
            f = self.publish_ranks(st, audio.as_ref(), f, tai);
        }
        if st.fence != Some(f) {
            st.fence = Some(f);
            if let Some(audio) = &self.audio {
                audio.fence(f);
            }
        }
    }

    /// Publishes the ranks of the blocks F at `f` passes before F, each
    /// fixed once F passed it, and returns `f`, lowered to a second rank
    /// change the RT thread cannot take yet.
    fn publish_ranks(&self, st: &mut State, audio: &dyn Audio, f: u64, tai: u64) -> u64 {
        let t = self.timing;
        let b = t.block;
        let ring = index_at((t.hold + t.contact_window).as_nanos() as u64).div_ceil(b) + 2;
        // The lowest block that can still take a rank: the next one
        // computed, or one computed fenced. One computed past its fence
        // deadline is withheld whatever its rank.
        let due = index_at(tai).saturating_sub(t.margin) / b;
        let next = st
            .own
            .processed
            .map_or(0, |p| p + 1)
            .max(due.saturating_sub(ring));
        let outputs = st.own.standing.len();
        let lo = (next.saturating_sub(ring)..next)
            .find(|&k| {
                (0..outputs).any(|o| {
                    audio
                        .status(o, k)
                        .is_some_and(|s| s.kind == samplez::blocks::Kind::Fenced)
                })
            })
            .unwrap_or(next);
        let hi = (f / b).max(lo + 1);
        st.baselines.prune(lo, &t);
        let published = st.ranks;
        let audio_voters = crate::rank::audio_voters(&st.members);
        let baselines = &st.baselines;
        let rank_of = |k: u64| match published {
            Some((r, passed)) if k < passed => r.of(k, false),
            _ => baselines.rank(k, self.id, &audio_voters, &t),
        };
        let class = match st.life.as_ref().map(Life::state) {
            Some(ReplicaState::Diverged) => crate::rank::Class::Diverged,
            _ => crate::rank::Class::Demoted,
        };
        let fill = crate::rank::demoted(self.id, &audio_voters, class);
        let (ranks, cap) = crate::rank::schedule(lo, hi, rank_of, fill);
        if published.map(|(r, _)| r) != Some(ranks) {
            audio.ranks(ranks);
        }
        let f = match cap {
            Some(c) => f.min(c * b).max(st.fence.unwrap_or(0)),
            None => f,
        };
        st.ranks = Some((ranks, f / b));
        f
    }

    /// The supervisor's moves on this replica's blocks (Withholding).
    fn decide(&self, st: &mut State, tai: u64) {
        let Some(audio) = &self.audio else {
            return;
        };
        if st.members.configs.is_empty() {
            return;
        }
        let peers = &st.peers;
        let mine = |x: NodeId| peers.get(&x).and_then(|p| p.silent_at);
        st.withhold.decide(
            self.id,
            &st.own,
            &mine,
            &st.members,
            &self.timing,
            audio.as_ref(),
            tai,
        );
    }

    /// The replica's own state (plan 2.11, Replica states), with the
    /// verdicts naming it once their commit is known, and the audio
    /// `/readyz` term while in contact.
    fn live(&self, st: &mut State, now: Instant, tai: u64) {
        let Some(audio) = &self.audio else { return };
        if st.life.is_none() {
            return;
        }
        self.resolve(st, audio.as_ref());
        let rebuilding = match st.restart {
            Some(after) if self.log.applied() >= after && audio.rebuild_engine() => {
                st.restart = None;
                // The rebuilt core holds no unresolved entry: the ones
                // still unresolved go to it again.
                for (&(index, _, _), &at) in &st.given {
                    let _ = audio.unresolved(index, at);
                }
                false
            }
            Some(_) => true,
            None => false,
        };
        let Some(life) = st.life.as_mut() else { return };
        let known = st.known_committed;
        let due: Vec<u64> = st
            .verdicts
            .iter()
            .filter(|&&(index, _)| index <= known)
            .map(|&(_, page)| page)
            .collect();
        st.verdicts.retain(|&(index, _)| index > known);
        for page in due {
            life.verdict(page, &st.own, &st.realised, audio.as_ref());
        }
        let ready = self.log.applied() >= st.known_committed && st.generation_ready && !rebuilding;
        let baseline = self.log.baseline();
        let t = self.timing;
        let fresh = t.contact_window + t.hold;
        let audio_voters = crate::rank::audio_voters(&st.members);
        let copies: Vec<(NodeId, Duration, Own)> = self
            .copies(st, now)
            .into_iter()
            .filter(|(id, _, _)| *id != self.id && audio_voters.contains(id))
            .map(|(id, age, own)| (id, age, own.clone()))
            .collect();
        let k = st.own.processed.map_or(0, |p| p + 1);
        let mine = st.baselines.rank(k, self.id, &audio_voters, &t);
        let above = audio_voters
            .iter()
            .copied()
            .filter(|&id| id != self.id && st.baselines.rank(k, id, &audio_voters, &t) < mine)
            .collect();
        let life = st.life.as_mut().expect("checked above");
        let view = View {
            me: self.id,
            own: &st.own,
            realised: &st.realised,
            ready,
            alone: st.members.voters().len() == 1,
            baseline: baseline.as_ref(),
            excluded: st.judged.contains(&self.id),
            peers: copies
                .iter()
                .map(|(id, age, own)| Seen {
                    id: *id,
                    silent: *age > fresh,
                    own,
                })
                .collect(),
            above,
            pages: &st.pages,
            timing: &t,
            tai,
        };
        life.step(&view, audio.as_ref());
        let state = life.state();
        st.own.state = Some(state);
        st.own.ready = ready;
        st.own.baseline = baseline.map(|b| (b.replica, b.at_index));
        if st.in_contact {
            let other = copies
                .iter()
                .any(|(_, age, own)| *age <= fresh && own.state == Some(ReplicaState::Committing));
            st.serving = Some(state == ReplicaState::Committing || !other);
        }
    }

    /// Acts on the decisions on unresolved entries known committed: the
    /// entry it holds unresolved is resolved; one it applied is
    /// contradicted, which diverges it at once. A purge-point verdict
    /// naming it returns it to `starting`; a listing overflow is resolved
    /// once every entry it may have left out is in the log known
    /// committed.
    fn resolve(&self, st: &mut State, audio: &dyn Audio) {
        let known = st.known_committed;
        let due: Vec<(u64, u64)> = st
            .decisions
            .iter()
            .filter(|&&(index, _, _)| index <= known)
            .map(|&(_, revision, term)| (revision, term))
            .collect();
        st.decisions.retain(|&(index, _, _)| index > known);
        for (revision, term) in due {
            let held: Vec<Key> = st
                .given
                .range((revision, term, 0)..=(revision, term, u64::MAX))
                .map(|(k, _)| *k)
                .collect();
            if !held.is_empty() {
                for k in held {
                    st.given.remove(&k);
                }
                audio.resolved(revision);
                continue;
            }
            // Applied here: its own committed entry, handed to the engine.
            let applied = revision <= st.handed
                && self
                    .log
                    .log_id_at(revision)
                    .is_none_or(|id| id.term == term);
            if applied && let Some(life) = st.life.as_mut() {
                st.counts.contradicted += 1;
                life.contradicted(audio);
            }
        }
        let verdicts: Vec<u64> = st
            .purge_points
            .iter()
            .filter(|&&(index, _)| index <= known)
            .map(|&(_, after)| after)
            .collect();
        st.purge_points.retain(|&(index, _)| index > known);
        for after in verdicts {
            st.counts.purge_points += 1;
            self.begin_restart(st, after);
        }
        let covered = known.min(st.told);
        let done: Vec<u64> = st
            .unlisted
            .iter()
            .filter(|&(_, &c)| c <= covered)
            .map(|(&id, _)| id)
            .collect();
        for id in done {
            st.unlisted.remove(&id);
            st.given.retain(|k, _| k.0 != id);
            audio.resolved(id);
        }
        if st.unlisted.is_empty() {
            st.unlisted_low = None;
        }
    }

    /// The entries this replica holds unresolved, for its reports.
    fn unresolved(st: &State) -> Vec<Listed> {
        let mut out: Vec<Listed> = st
            .given
            .iter()
            .map(|(&(index, term, node), &at_index)| Listed {
                id: StoredLogId { term, node, index },
                at_index,
                baseline: false,
            })
            .collect();
        out.sort_by_key(|l| (l.at_index, l.id.index));
        out.truncate(MAX_LISTED);
        out
    }

    /// The audio `/readyz` term (plan 2.11, Deployment): committing, or no
    /// other audio replica committing within `w + D` by its reports;
    /// judged while in contact, kept out of it, false before the first
    /// contact. Always true for the witness and for one voter.
    pub fn serving(&self) -> bool {
        let st = self.lock();
        if self.audio.is_none() || st.members.voters().len() == 1 {
            return true;
        }
        st.serving.unwrap_or(false)
    }

    /// This voter does not lead ([`crate::baseline::Leader::step_down`]).
    pub fn not_leading(&self) {
        let mut st = self.lock();
        st.leader.step_down();
        st.clock.step_down();
        st.decider.step_down();
    }

    /// The leader's decisions this period ([`crate::baseline`]): none
    /// while out of contact.
    pub fn decisions(&self, term: u64) -> Vec<Body> {
        let now = Instant::now();
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        if !st.in_contact || st.members.configs.is_empty() {
            return Vec::new();
        }
        let t = self.timing;
        let fresh = t.contact_window + t.hold;
        let regained = since(now, st.regained);
        let audio_ids = crate::rank::audio_voters(&st.members);
        let copies: BTreeMap<NodeId, (Duration, Own)> = self
            .copies(st, now)
            .into_iter()
            .map(|(id, age, own)| (id, (age, own.clone())))
            .collect();
        let counted = self.counted(st, now);
        let baseline = self.log.baseline();
        let ms = Duration::from_millis(1).as_nanos() as u64;
        let q = |h: &Histogram| h.quantile(0.9999);
        let delta = st
            .peers
            .values()
            .filter_map(|p| q(&p.delay))
            .max()
            .unwrap_or(ms);
        let theta = st
            .peers
            .values()
            .filter_map(|p| q(&p.offsets))
            .max()
            .unwrap_or(ms);
        let floor = index_at(2 * t.report_period.as_nanos() as u64 + 2 * delta + theta);
        let k = index_at(tai).saturating_sub(t.margin) / t.block;
        let baselines = &st.baselines;
        let rank = |id: NodeId| baselines.rank(k, id, &audio_ids, &t);
        let audio: Vec<Voter> = audio_ids
            .iter()
            .map(|&id| {
                let c = copies.get(&id);
                Voter {
                    id,
                    silent: c.map_or(regained, |(age, _)| (*age).min(regained)) > fresh,
                    own: c.map(|(_, own)| own),
                }
            })
            .collect();
        let log = &self.log;
        let committed = st.known_committed;
        let own_term_committed = log.log_id_at(committed).is_some_and(|id| id.term == term);
        let superseded = |index: u64| {
            let Some(earlier) = log.request(index) else {
                return false;
            };
            let mut later = false;
            log.entries_after(index, &mut |id, request| {
                later |= id.index <= committed && crate::decide::supersedes(&earlier, request);
            });
            later
        };
        let restamps = |index: u64| log.restamps(index);
        let log_id_at = |index: u64| log.log_id_at(index);
        let leader_log = crate::decide::LeaderLog {
            log_id_at: &log_id_at,
            committed,
            purged: log.purged(),
            own_term_committed,
            restamps: &restamps,
            superseded: &superseded,
        };
        let mut decided = st
            .decider
            .decide(&audio, baseline.as_ref(), None, &leader_log, &t, now);
        let view = LeaderView {
            contradicting: &decided.contradicting,
            audio: audio_ids
                .iter()
                .map(|&id| {
                    let c = copies.get(&id);
                    Voter {
                        id,
                        silent: c.map_or(regained, |(age, _)| (*age).min(regained)) > fresh,
                        own: c.map(|(_, own)| own),
                    }
                })
                .collect(),
            judged: &st.judged,
            disrupting: self.log.disrupting(),
            baseline: baseline.as_ref(),
            counted: &counted,
            pages: &st.pages,
            timing: &t,
            floor,
            rank: &rank,
        };
        let mut out = st.leader.decide(&view, now);
        // Re-decisions follow a rename at once: the entries the new
        // baseline holds unresolved are restamped.
        let renamed = out.iter().find_map(|b| match b {
            Body::Baseline(b) => Some(b.clone()),
            _ => None,
        });
        if let Some(renamed) = renamed {
            let again = st.decider.decide(
                &audio,
                baseline.as_ref(),
                Some(&renamed),
                &leader_log,
                &t,
                now,
            );
            decided.append.extend(again.append);
        }
        decided.append.append(&mut out);
        let (readings, witness) = self.clock_readings(st, now);
        let clock = crate::clock::View {
            audio: readings,
            witness,
            judged: &st.judged,
            latest_judged: self.log.latest_judged(),
            baseline: baseline.as_ref().map(|b| b.replica),
            guard_ns: time_of(t.clock_guard),
        };
        decided.append.extend(st.clock.decide(&clock, now));
        decided.append
    }

    /// Every voter's clock as this one sees it: the audio voters by
    /// ordinal, and the witness.
    fn clock_readings(
        &self,
        st: &State,
        now: Instant,
    ) -> (Vec<crate::clock::Reading>, Option<crate::clock::Reading>) {
        use crate::membership::{Slot, slot_of};
        let fresh = self.timing.contact_window + self.timing.hold;
        let copies = self.copies(st, now);
        let reading = |id: NodeId| {
            let copy = copies.iter().find(|(c, _, _)| *c == id);
            let flags = copy.map(|(_, _, own)| own.clock).unwrap_or_default();
            let (theta_ns, mad_ns, delay_ns) = if id == self.id {
                (Some(0), 0, 0)
            } else {
                st.peers.get(&id).map_or((None, 0, 0), |p| {
                    let best = p.thetas.best();
                    (best.map(|b| b.0), p.thetas.mad(), best.map_or(0, |b| b.1))
                })
            };
            crate::clock::Reading {
                id,
                theta_ns,
                mad_ns,
                delay_ns,
                self_report: flags.self_report || flags.step_fence,
                verified: !flags.unverified,
                silent: id != self.id && copy.is_none_or(|(_, age, _)| *age > fresh),
            }
        };
        let audio = crate::rank::audio_voters(&st.members)
            .into_iter()
            .map(reading)
            .collect();
        let witness = st
            .members
            .voters()
            .into_iter()
            .find(|&id| slot_of(id) == Slot::Witness)
            .map(reading);
        (audio, witness)
    }

    /// The clock check as this voter sees it: every audio replica's deviation, or an unverified one's
    /// offset to the witness, and `replica_clock_tie`.
    fn clock_status(&self, st: &State, now: Instant) -> crate::clock::Status {
        let (audio, witness) = self.clock_readings(st, now);
        crate::clock::compare(&crate::clock::View {
            audio,
            witness,
            judged: &st.judged,
            latest_judged: self.log.latest_judged(),
            baseline: self.log.baseline().map(|b| b.replica),
            guard_ns: time_of(self.timing.clock_guard),
        })
        .status
    }

    /// Drops scheduled entries every voter reported committed.
    fn prune_scheduled(&self, st: &mut State) {
        let lowest = st
            .members
            .voters()
            .iter()
            .map(|&v| {
                if v == self.id {
                    st.known_committed
                } else {
                    st.peers
                        .get(&v)
                        .and_then(|p| p.last.as_ref())
                        .and_then(|d| d.committed)
                        .map_or(0, |c| c.index)
                }
            })
            .min()
            .unwrap_or(0);
        st.scheduled = st.scheduled.split_off(&(lowest + 1));
    }

    /// The engine refused an appended entry: the replica's output no
    /// longer follows the log, so it takes no client writes.
    pub fn engine_failed(&self) -> bool {
        self.lock().entries_refused > 0
    }

    /// Reports from a majority of voters, itself included, within w, as
    /// of the last report period.
    pub fn in_contact(&self) -> bool {
        self.lock().in_contact
    }

    /// Whether every other voter heard directly within `within` reports a
    /// committed log index at or above `index`.
    pub fn heard_committed(&self, index: u64, within: Duration) -> bool {
        let now = Instant::now();
        let st = self.lock();
        st.peers.values().all(|p| {
            p.last.as_ref().is_none_or(|d| {
                since(now, d.received) > within || d.committed.is_some_and(|c| c.index >= index)
            })
        })
    }

    /// The audio replicas E counts (plan 2.11, Command scheduling), by
    /// their newest copy, direct or relayed: not silent (a copy generated
    /// within w + D), clock-verified, not judged by a `ClockVerdict`, and
    /// with neither a clock self-report nor a detected-step fence; when
    /// none is, those whose only flag is a self-report.
    fn counted(&self, st: &State, now: Instant) -> BTreeSet<NodeId> {
        let fresh = self.timing.contact_window + self.timing.hold;
        let copies = self.copies(st, now);
        let eligible =
            |own: &Own| own.processed.is_some() && !own.clock.unverified && !own.clock.step_fence;
        let pick = |self_report: bool| -> BTreeSet<NodeId> {
            copies
                .iter()
                .filter(|(id, age, own)| {
                    *age <= fresh
                        && eligible(own)
                        && own.clock.self_report == self_report
                        && !st.judged.contains(id)
                })
                .map(|(id, _, _)| *id)
                .collect()
        };
        let free = pick(false);
        if free.is_empty() { pick(true) } else { free }
    }

    /// Each voter's newest copy, direct or relayed, with its age; this
    /// replica's own as of its last sample.
    fn copies<'a>(&self, st: &'a State, now: Instant) -> Vec<(NodeId, Duration, &'a Own)> {
        let mut out = Vec::new();
        if self.audio.is_some() {
            out.push((self.id, since(now, st.own_at), &st.own));
        }
        for (&id, p) in &st.peers {
            let direct = p.last.as_ref().map(|d| (d.received, &d.own));
            let relayed = p.relayed.as_ref().map(|(g, own)| (*g, own));
            let newest = match (direct, relayed) {
                (Some(d), Some(r)) if r.0 > d.0 => Some(r),
                (d, r) => d.or(r),
            };
            if let Some((at, own)) = newest {
                out.push((id, since(now, at), own));
            }
        }
        out
    }

    /// `ceil(E / B) * B` now, 0 while E counts nobody.
    fn current_stamp(&self, st: &State, now: Instant) -> u64 {
        let b = self.timing.block;
        self.estimate_in(st, now).map_or(0, |e| e.div_ceil(b) * b)
    }

    /// Engine entries pending above `floor`: those the state machine holds
    /// and those this log holds unapplied.
    fn pending_above(&self, floor: u64) -> usize {
        let mut pending = 0;
        self.log.pending(&mut |_, body| {
            if body.at_index().is_some_and(|at| at > floor) {
                pending += 1;
            }
        });
        self.log
            .entries_after(self.log.applied(), &mut |_, request| {
                if request.at_index().is_some_and(|at| at > floor) {
                    pending += 1;
                }
            });
        pending
    }

    fn estimate_in(&self, st: &State, now: Instant) -> Option<u64> {
        let counted = self.counted(st, now);
        let b = self.timing.block;
        self.copies(st, now)
            .into_iter()
            .filter(|(id, _, _)| counted.contains(id))
            .filter_map(|(_, age, own)| {
                let end = (own.processed? + 1) * b;
                Some(end + index_at(age.as_nanos() as u64))
            })
            .max()
    }

    /// E: the highest processed index among the audio replicas it counts,
    /// each extrapolated by `CLOCK_MONOTONIC` since its copy was generated;
    /// none when it counts nobody.
    pub fn estimate(&self) -> Option<u64> {
        let st = self.lock();
        self.estimate_in(&st, Instant::now())
    }

    /// `ageGuard` (plan 2.11, Retention) with statusLag and delta as
    /// measured here, 1 ms each before any measurement.
    pub fn age_guard(&self) -> u64 {
        let st = self.lock();
        let ms = index_at(1_000_000);
        let status_lag = st.counts.status_lag.quantile(0.9999).map_or(ms, index_at);
        let delta = st
            .peers
            .values()
            .filter_map(|p| p.delay.quantile(0.9999))
            .max()
            .map_or(ms, index_at);
        crate::schedule::age_guard(
            self.timing.block,
            status_lag,
            delta,
            self.timing.clock_guard,
        )
    }

    /// Where the leader stamps an entry: none while E counts nobody, or
    /// with [`FUTURE_LIST`] entries pending above `E - applyGuard - S`.
    pub fn stamp(&self, generation: bool) -> Result<Stamp, StampRefusal> {
        let st = self.lock();
        let now = Instant::now();
        let e = self
            .estimate_in(&st, now)
            .ok_or(StampRefusal::NobodyCounted)?;
        let b = self.timing.block;
        let stamp = e.div_ceil(b) * b;
        let spread = st
            .e_spread
            .quantile(0.9999)
            .unwrap_or(2 * b + index_at(3_000_000));
        let floor = e.saturating_sub(index_at(self.timing.apply_guard_ns()) + spread);
        // Committed entries stay pending in every engine until their index:
        // counted from the state machine, not from what is still listed.
        if self.pending_above(floor) >= FUTURE_LIST {
            return Err(StampRefusal::FutureListFull);
        }
        let lead = st.lead.samples();
        let earliest = if generation {
            // The last one's at_index has passed on the baseline, or on E
            // while no baseline is named.
            if let Some(at) = st.generation_at {
                let baseline = self.log.baseline().map(|b| b.replica);
                let passed = self
                    .copies(&st, now)
                    .into_iter()
                    .find(|(id, _, _)| Some(*id) == baseline)
                    .and_then(|(_, _, own)| own.processed)
                    .map_or(e, |k| (k + 1) * b);
                if passed <= at {
                    return Err(StampRefusal::GenerationPending);
                }
            }
            let counted = self.counted(&st, now);
            let build_lead = self
                .copies(&st, now)
                .into_iter()
                .filter(|(id, _, _)| counted.contains(id))
                .filter_map(|(_, _, own)| own.build_lead)
                .max()
                .unwrap_or(0);
            stamp + lead.max(build_lead).next_multiple_of(b)
        } else {
            stamp + lead
        };
        Ok(Stamp { stamp, earliest })
    }

    pub fn metrics(&self) -> Metrics {
        let st = self.lock();
        let c = &st.counts;
        let q = |h: &Histogram| h.quantile(0.9999);
        let delta = st.peers.values().filter_map(|p| q(&p.delay)).max();
        let theta = st.peers.values().filter_map(|p| q(&p.offsets)).max();
        let floor = delta
            .zip(theta)
            .map(|(d, th)| 2 * self.timing.report_period + Duration::from_nanos(2 * d + th));
        let status_lag = q(&c.status_lag).map(Duration::from_nanos);
        Metrics {
            fence: st.fence,
            in_contact: st.in_contact,
            known_committed: st.known_committed,
            peers: st
                .peers
                .iter()
                .map(|(&node, p)| PeerMetrics {
                    node,
                    reports: p.reports,
                    delay_p9999: p.delay.quantile(0.9999).map(Duration::from_nanos),
                    gap_max: Duration::from_nanos(p.gaps.max()),
                    gaps_over_window: p.gaps_over_window,
                    silent: p.silent,
                    offset_ns: p.offset_ns,
                })
                .collect(),
            shared_gaps: c.shared_gaps,
            shared_gap_max: c.shared_gap_max,
            stagger_max: c.stagger_max,
            status_lag_p9999: c.status_lag.quantile(0.9999).map(Duration::from_nanos),
            status_changes_lost: st.status_changes_lost,
            entries_refused: st.entries_refused,
            unresolved: c.unresolved,
            unresolved_refused: c.unresolved_refused,
            awaiting_overflow: c.awaiting_overflow,
            scheduled_dropped: c.scheduled_dropped,
            snapshot_installs: c.installs,
            purge_points: c.purge_points,
            contradicted: c.contradicted,
            estimate: self.estimate_in(&st, Instant::now()),
            counted: self.counted(&st, Instant::now()).into_iter().collect(),
            lead: st.lead.samples(),
            apply_lead_capped: st.lead.capped,
            e_spread_p9999: st.e_spread.quantile(0.9999),
            withholding: st.withhold.counts,
            floor,
            hold_floor_unmet: match (floor, status_lag) {
                (Some(l), Some(lag)) => {
                    self.timing.contact_window < l || self.timing.hold < lag + l
                }
                _ => false,
            },
            replica: st.life.as_ref().map(|l| l.metrics),
            leader: st.leader.counts,
            replica_disagreement: st.leader.disagreement,
            clock: self.clock_status(&st, Instant::now()),
            clock_verdicts: st.clock.verdicts,
        }
    }
}

/// Where an entry is stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// `ceil(E / B) * B`: the log time dedup retention counts in.
    pub stamp: u64,
    /// The earliest at_index: the stamp plus the lead, or for a
    /// generation-needing entry plus the larger of the lead and buildLead.
    pub earliest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StampRefusal {
    /// E counts no audio replica: nothing is stamped.
    NobodyCounted,
    /// [`FUTURE_LIST`] entries are pending.
    FutureListFull,
    /// The previous generation-needing entry's at_index has not passed on
    /// the baseline.
    GenerationPending,
}

/// Runs contact for `contact` until the replica shuts down: a report to
/// every other voter each period, each sent as soon as the one before it
/// to the same voter was answered, a newer one replacing one not yet
/// sent.
pub(crate) async fn run(
    shared: Arc<Shared>,
    contact: Arc<Contact>,
    queue: tokio::sync::mpsc::UnboundedSender<crate::Queued>,
) {
    let mut metrics = shared.raft.metrics();
    let mut tick = tokio::time::interval(contact.timing.report_period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut senders: BTreeMap<NodeId, watch::Sender<Option<(String, Report)>>> = BTreeMap::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let members = Members::of(&metrics.borrow());
                let voters = members.voters();
                senders.retain(|id, _| voters.contains(id));
                for (to, addr, report) in contact.tick(members) {
                    senders
                        .entry(to)
                        .or_insert_with(|| send(shared.clone(), to, contact.timing.contact_window))
                        .send_replace(Some((addr, report)));
                }
                // Decisions are the leader's, and pause in an election.
                if shared.leads() {
                    let term = shared.raft.metrics().borrow().current_term;
                    for body in contact.decisions(term) {
                        let _ = queue.send(crate::Queued::Entry {
                            body,
                            deadline: Instant::now() + shared.settings.write_timeout,
                            reply: None,
                            forwarded: false,
                        });
                    }
                } else {
                    contact.not_leading();
                }
            }
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
                contact.log_changed();
            }
        }
    }
}

fn send(
    shared: Arc<Shared>,
    to: NodeId,
    within: Duration,
) -> watch::Sender<Option<(String, Report)>> {
    let (tx, mut rx) = watch::channel(None::<(String, Report)>);
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let next = rx.borrow_and_update().clone();
            if let Some((addr, report)) = next {
                let request = PeerRequest::Report(Box::new(report));
                let _ = shared.peers.call(Some(to), &addr, request, within).await;
            }
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::report::ClockFlags;

    const B: u64 = 48;
    const T0: u64 = 1_000_000_000_000_000;

    /// A TAI clock a test sets; each read moves it on by `step` ns.
    struct Clock {
        now: AtomicU64,
        step: AtomicU64,
    }

    impl Clocks for Clock {
        fn tai_ns(&self) -> u64 {
            self.now
                .fetch_add(self.step.load(Ordering::SeqCst), Ordering::SeqCst)
        }
    }

    impl Clock {
        fn set(&self, ns: u64) {
            self.now.store(ns, Ordering::SeqCst);
        }
        fn add(&self, d: Duration) {
            self.now.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
        }
        fn get(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    #[derive(Default)]
    struct LogInner {
        log: BTreeMap<u64, (StoredLogId, Request)>,
        committed: Option<StoredLogId>,
        applied: u64,
        pending: Vec<(u64, Body)>,
        installs: u64,
    }

    #[derive(Default)]
    struct Log(Mutex<LogInner>);

    impl Log {
        fn push(&self, term: u64, index: u64, at_index: Option<u64>) {
            let body = match at_index {
                Some(at_index) => Body::InputDown { slot: 0, at_index },
                None => Body::Commands(Vec::new()),
            };
            self.0
                .lock()
                .unwrap()
                .log
                .insert(index, (id(term, index), Request { stamp: 0, body }));
        }
    }

    impl LogView for Log {
        fn committed(&self) -> Option<StoredLogId> {
            self.0.lock().unwrap().committed
        }
        fn applied(&self) -> u64 {
            self.0.lock().unwrap().applied
        }
        fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
            self.0.lock().unwrap().log.get(&index).map(|(id, _)| *id)
        }
        fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request)) {
            for (id, r) in self
                .0
                .lock()
                .unwrap()
                .log
                .range(index + 1..)
                .map(|(_, e)| e)
            {
                f(*id, r);
            }
        }
        fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
            for (index, body) in &self.0.lock().unwrap().pending {
                f(*index, body);
            }
        }
        fn latest_judged(&self) -> Option<NodeId> {
            None
        }
        fn judged(&self) -> BTreeSet<NodeId> {
            BTreeSet::new()
        }
        fn baseline(&self) -> Option<Baseline> {
            None
        }
        fn disrupting(&self) -> Option<NodeId> {
            None
        }
        fn installs(&self) -> u64 {
            self.0.lock().unwrap().installs
        }
    }

    /// Records what contact hands the engine. Its step flag shows once
    /// the clock reads `step_at` or later.
    struct Engine {
        clock: Arc<Clock>,
        step_at: AtomicU64,
        fence: AtomicU64,
        calls: Mutex<Vec<String>>,
        /// Unresolved entries it takes before it refuses.
        room: AtomicU64,
        /// What each append carried.
        requests: Mutex<Vec<(u64, Request)>>,
        /// Whether a rebuild succeeds.
        rebuilds: std::sync::atomic::AtomicBool,
    }

    impl Audio for Engine {
        fn sample(&self) -> Sample {
            let step = self.clock.tai_ns() >= self.step_at.load(Ordering::SeqCst);
            Sample {
                own: Own {
                    processed: Some(1),
                    clock: ClockFlags {
                        step_fence: step,
                        ..ClockFlags::default()
                    },
                    ..Own::default()
                },
                status_changes_lost: 0,
                entries_refused: 0,
                realised: Vec::new(),
                generation_ready: true,
            }
        }
        fn fence(&self, end: u64) {
            self.fence.store(end, Ordering::SeqCst);
        }
        fn settled(&self, _: u64) -> bool {
            false
        }
        fn appended(&self, index: u64, request: &Request) {
            self.calls.lock().unwrap().push(format!("appended {index}"));
            self.requests.lock().unwrap().push((index, request.clone()));
        }
        fn resolved(&self, index: u64) {
            self.calls.lock().unwrap().push(format!("resolved {index}"));
        }
        fn drop_engine(&self) {
            self.calls.lock().unwrap().push("drop".into());
        }
        fn rebuild_engine(&self) -> bool {
            let ok = self.rebuilds.load(Ordering::SeqCst);
            if ok {
                self.calls.lock().unwrap().push("rebuild".into());
            }
            ok
        }
        fn truncated(&self, from: u64) {
            self.calls.lock().unwrap().push(format!("truncated {from}"));
        }
        fn committed(&self, up_to: u64) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("committed {up_to}"));
        }
        fn unresolved(&self, index: u64, at_index: u64) -> bool {
            if self.room.load(Ordering::SeqCst) == 0 {
                self.calls.lock().unwrap().push(format!("refused {index}"));
                return false;
            }
            self.room.fetch_sub(1, Ordering::SeqCst);
            let call = format!("unresolved {index} at {at_index}");
            self.calls.lock().unwrap().push(call);
            true
        }
        fn skip(&self, index: u64) {
            self.calls.lock().unwrap().push(format!("skip {index}"));
        }
        fn overwhelmed(&self) {
            self.calls.lock().unwrap().push("overwhelmed".into());
        }
        fn status(&self, _: usize, _: u64) -> Option<BlockStatus> {
            None
        }
        fn withhold(&self, _: usize, _: u64, _: Cause) -> Option<BlockStatus> {
            None
        }
        fn commit_through(&self, _: usize, _: u64, _: u8, _: Option<u64>) -> bool {
            false
        }
        fn withdraw(&self) {
            self.calls.lock().unwrap().push("withdraw".into());
        }
        fn ranks(&self, _: Ranks) {}
        fn commit_from(&self, _: usize, _: u64, _: u64) {}
        fn cancel_commit_start(&self, _: usize) {}
    }

    impl Engine {
        fn calls(&self) -> Vec<String> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }

    fn id(term: u64, index: u64) -> StoredLogId {
        StoredLogId {
            term,
            node: 1,
            index,
        }
    }

    struct Fixture {
        clock: Arc<Clock>,
        log: Arc<Log>,
        engine: Arc<Engine>,
        contact: Contact,
        members: Members,
    }

    fn fixture(configs: &[&[NodeId]]) -> Fixture {
        let clock = Arc::new(Clock {
            now: AtomicU64::new(T0),
            step: AtomicU64::new(0),
        });
        let log = Arc::new(Log::default());
        let engine = Arc::new(Engine {
            clock: clock.clone(),
            step_at: AtomicU64::new(u64::MAX),
            fence: AtomicU64::new(0),
            calls: Mutex::new(Vec::new()),
            room: AtomicU64::new(u64::MAX),
            requests: Mutex::new(Vec::new()),
            rebuilds: std::sync::atomic::AtomicBool::new(true),
        });
        let contact = Contact::new(
            1,
            Timing::defaults(B),
            clock.clone(),
            Some(engine.clone()),
            log.clone(),
            100,
        );
        let configs: Vec<BTreeSet<NodeId>> = configs
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect();
        let addrs = configs
            .iter()
            .flatten()
            .map(|&v| (v, v.to_string()))
            .collect();
        Fixture {
            clock,
            log,
            engine,
            contact,
            members: Members { configs, addrs },
        }
    }

    impl Fixture {
        fn tick(&self) -> Vec<(NodeId, String, Report)> {
            self.contact.tick(self.members.clone())
        }

        /// A report from `from` echoing seq `echo` received at `at`.
        fn report(&self, from: NodeId, echo: Option<(u64, u64)>) -> Report {
            Report {
                from,
                seq: self.clock.get(),
                sent_at: self.clock.get(),
                echo: echo.map(|(seq, received_at)| Echo {
                    seq,
                    received_at,
                    silent_at: None,
                }),
                committed: None,
                own: Own::default(),
                heard: Vec::new(),
                listed: Vec::new(),
                unlisted: None,
                estimate: None,
                slack: Vec::new(),
            }
        }

        fn fence(&self) -> u64 {
            self.engine.fence.load(Ordering::SeqCst)
        }

        fn silent(&self, node: NodeId) -> bool {
            let m = self.contact.metrics();
            m.peers.iter().any(|p| p.node == node && p.silent)
        }
    }

    const W: Duration = Duration::from_millis(8);

    fn w_ns() -> u64 {
        W.as_nanos() as u64
    }

    #[tokio::test(start_paused = true)]
    async fn f_ends_at_the_blocks_a_majority_echoed_within_w() {
        let f = fixture(&[&[1, 2, 3]]);
        f.members.voters();
        let sent = f.tick();
        let seq = sent[0].2.seq;
        assert!(
            f.fence() < index_at(T0) - 576,
            "no echo yet: every block not yet due is fenced"
        );
        f.clock.add(Duration::from_millis(3));
        f.contact.receive(f.report(2, Some((seq, T0 + 1_000_000))));
        // Me and 2 are a majority of three: blocks ending at or before
        // the echoed send time plus w pass.
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
        // Time alone moves nothing: without newer echoes F stands.
        f.clock.add(Duration::from_millis(50));
        tokio::time::advance(Duration::from_millis(50)).await;
        f.tick();
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn an_echo_stamped_silent_does_not_count_for_the_blocks_it_voids() {
        let f = fixture(&[&[1, 2, 3]]);
        let timing = Timing::defaults(B);
        let guard = time_of(timing.clock_guard);
        let first = f.tick()[0].2.seq;
        f.contact.receive(f.report(2, Some((first, T0))));
        let before = f.fence();
        assert_eq!(before, index_at(T0 + w_ns()) / B * B);
        // 40 ms later 2 echoes a newer report, stamped with the time it
        // last held this replica silent, 30 ms after the first report.
        f.clock.add(Duration::from_millis(40));
        tokio::time::advance(Duration::from_millis(40)).await;
        let second = f.tick()[0].2.seq;
        let silent_at = T0 + 30_000_000;
        let mut r = f.report(2, None);
        r.echo = Some(Echo {
            seq: second,
            received_at: f.clock.get(),
            silent_at: Some(silent_at),
        });
        f.contact.receive(r);
        // Blocks due up to silentAt + clockGuard stay fenced: the first
        // echo is too old for them, the second is voided for them.
        let f_after = f.fence();
        let k = f_after / B;
        assert!(
            timing.block_deadline(k) <= silent_at + guard,
            "F skipped a voided block"
        );
        assert!(f_after >= before);
        // 3 echoes the second report without a silentAt: a majority.
        f.contact
            .receive(f.report(3, Some((second, f.clock.get()))));
        assert_eq!(f.fence(), index_at(T0 + 40_000_000 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn a_joint_change_needs_a_majority_of_each_configuration() {
        let f = fixture(&[&[1, 2, 3], &[1, 4, 5]]);
        let seq = f.tick()[0].2.seq;
        f.contact.receive(f.report(2, Some((seq, T0))));
        assert!(
            f.fence() < index_at(T0) - 576,
            "the second configuration echoed nothing"
        );
        f.contact.receive(f.report(4, Some((seq, T0))));
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn silence_counts_from_regaining_contact_and_is_echoed() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        f.contact.receive(f.report(2, None));
        f.contact.receive(f.report(3, None));
        f.tick();
        // A gap every link shares: out of contact, nobody is held silent.
        tokio::time::advance(Duration::from_millis(20)).await;
        f.clock.add(Duration::from_millis(20));
        f.tick();
        assert!(!f.silent(2) && !f.silent(3));
        assert!(!f.contact.metrics().in_contact);
        // 2 comes back first, 3 six ms later: 3 was not heard for 26 ms,
        // but this replica regained contact only 6 ms before.
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.tick();
            assert!(!f.silent(3));
            tokio::time::advance(Duration::from_millis(2)).await;
            f.clock.add(Duration::from_millis(2));
        }
        f.contact.receive(f.report(3, None));
        f.tick();
        assert!(!f.silent(3), "a stagger below w holds nobody silent");
        // 3 then stays away for longer than w.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(2)).await;
            f.clock.add(Duration::from_millis(2));
            f.contact.receive(f.report(2, None));
            f.tick();
        }
        assert!(f.silent(3));
        let to_3 = f.tick().into_iter().find(|(to, _, _)| *to == 3).unwrap();
        let echo = to_3.2.echo.expect("3 was heard");
        assert_eq!(
            echo.silent_at,
            Some(to_3.2.sent_at),
            "silentAt names this report"
        );
        let to_2 = f.tick().into_iter().find(|(to, _, _)| *to == 2).unwrap();
        let heard_3 = to_2.2.heard.iter().find(|h| h.node == 3).unwrap();
        assert!(heard_3.silent, "the silence is relayed");
    }

    #[tokio::test(start_paused = true)]
    async fn a_committed_id_the_log_holds_commits_everything_up_to_it() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        for index in 1..=5 {
            f.log.push(1, index, None);
        }
        f.contact.log_changed();
        assert_eq!(f.engine.calls().len(), 5, "five appends");
        // Another term at index 5: not this log's entry.
        let mut r = f.report(2, None);
        r.committed = Some(id(2, 5));
        f.contact.receive(r);
        assert!(f.engine.calls().is_empty());
        let mut r = f.report(3, None);
        r.committed = Some(id(1, 4));
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 4"]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_exact_listed_id_below_the_committed_index_is_committed() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let at = index_at(T0) + 4800;
        for index in 1..=5 {
            f.log.push(1, index, Some(at));
        }
        f.contact.log_changed();
        f.engine.calls();
        // The sender's committed id lies past this log's end.
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 9));
        f.contact.receive(r.clone());
        assert!(f.engine.calls().is_empty(), "nothing this log holds");
        r.seq += 1;
        r.listed = vec![Listed {
            id: id(2, 5),
            at_index: at,
            baseline: false,
        }];
        f.contact.receive(r.clone());
        assert!(f.engine.calls().is_empty(), "another entry at index 5");
        r.seq += 1;
        r.listed = vec![Listed {
            id: id(1, 5),
            at_index: at,
            baseline: false,
        }];
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 5"]);
    }

    impl Fixture {
        fn insert(&self, index: u64, body: Body) {
            self.log
                .0
                .lock()
                .unwrap()
                .log
                .insert(index, (id(1, index), Request { stamp: 0, body }));
        }

        /// Entries up to `index` applied, which makes them known
        /// committed.
        fn apply(&self, index: u64) {
            let mut log = self.log.0.lock().unwrap();
            log.applied = index;
            log.committed = Some(id(1, index));
        }

        fn state(&self) -> Option<ReplicaState> {
            self.contact.metrics().replica.and_then(|m| m.state)
        }
    }

    /// An entry applied again by a restamp: the engine gets the entry the
    /// restamp names at the restamp's index, and a replica that applied
    /// it at its own index is diverged at once by the restamp's commit.
    #[tokio::test(start_paused = true)]
    async fn a_restamp_of_an_entry_applied_here_diverges_the_replica() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let at = index_at(T0) + 400 * 48;
        f.log.push(1, 1, Some(at));
        f.apply(1);
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        let restamp = Body::Restamp {
            revision: 1,
            term: 1,
            at_index: at + 96,
            paths: Vec::new(),
        };
        f.insert(2, restamp);
        f.contact.log_changed();
        let (index, engine) = f.engine.requests.lock().unwrap().last().cloned().unwrap();
        assert_eq!(index, 2);
        assert_eq!(
            engine.body,
            Body::InputDown {
                slot: 0,
                at_index: at + 96
            },
            "the entry the restamp names, at its index"
        );
        f.engine.calls();
        f.tick();
        assert_eq!(
            f.state(),
            Some(ReplicaState::Withdrawn),
            "not committed yet"
        );
        f.apply(2);
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Diverged));
        assert_eq!(f.contact.metrics().contradicted, 1);
    }

    /// A void of an entry held unresolved resolves it in the engine and
    /// takes it out of the reports, once the void is known committed.
    #[tokio::test(start_paused = true)]
    async fn a_void_resolves_the_entry_held_unresolved() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        f.log.push(1, 1, Some(at));
        f.contact.log_changed();
        f.clock.set(timing.apply_deadline(at));
        let unresolved = |f: &Fixture| -> Vec<u64> {
            let out = f.tick();
            out[0].2.own.unresolved.iter().map(|l| l.id.index).collect()
        };
        assert_eq!(unresolved(&f), [1]);
        f.insert(
            2,
            Body::Void {
                revision: 1,
                term: 1,
            },
        );
        f.apply(2);
        f.engine.calls();
        f.contact.log_changed();
        unresolved(&f);
        assert!(f.engine.calls().contains(&"resolved 1".to_owned()));
        assert_eq!(unresolved(&f), Vec::<u64>::new());
        assert_ne!(
            f.state(),
            Some(ReplicaState::Diverged),
            "it never applied it"
        );
    }

    /// A lagging follower whose next entry the leader purged installs a
    /// snapshot: it returns to `starting`, its engine dropped, counted,
    /// and leaves it only once the engine is rebuilt from product state.
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_install_returns_the_replica_to_starting_until_rebuilt() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        // Its own log purged past what contact was told: no install.
        f.apply(5);
        f.contact.log_changed();
        f.tick();
        assert_eq!(
            f.state(),
            Some(ReplicaState::Withdrawn),
            "a purge is no install"
        );
        f.engine.rebuilds.store(false, Ordering::SeqCst);
        f.apply(10);
        f.log.0.lock().unwrap().installs += 1;
        f.engine.calls();
        f.contact.log_changed();
        f.tick();
        let calls = f.engine.calls();
        assert!(calls.contains(&"drop".to_owned()), "{calls:?}");
        assert_eq!(f.state(), Some(ReplicaState::Starting));
        assert_eq!(f.contact.metrics().snapshot_installs, 1);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Starting), "not rebuilt");
        f.engine.rebuilds.store(true, Ordering::SeqCst);
        f.tick();
        assert!(f.engine.calls().contains(&"rebuild".to_owned()));
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    }

    /// A purge-point verdict naming this replica returns it to `starting`
    /// once known committed, and the rebuild waits for its applied index
    /// to pass the id's index; one naming another replica does nothing.
    #[tokio::test(start_paused = true)]
    async fn a_purge_point_verdict_rebuilds_once_the_applied_index_passed() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        f.insert(
            1,
            Body::PurgePoint {
                replica: 2,
                index: 5,
            },
        );
        f.insert(
            2,
            Body::PurgePoint {
                replica: 1,
                index: 5,
            },
        );
        f.apply(2);
        f.contact.log_changed();
        f.engine.calls();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Starting));
        assert_eq!(f.contact.metrics().purge_points, 1);
        f.tick();
        assert!(!f.engine.calls().contains(&"rebuild".to_owned()));
        for index in 3..=5 {
            f.log.push(1, index, None);
        }
        f.apply(5);
        f.contact.log_changed();
        f.tick();
        assert!(f.engine.calls().contains(&"rebuild".to_owned()));
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_whose_commit_is_unknown_at_its_deadline_is_unresolved() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let timing = Timing::defaults(B);
        // Three entries 40 ms ahead: one learned committed in time, one
        // only after its deadline, one never.
        let at = index_at(T0) + 40 * 48;
        for index in 1..=3 {
            f.log.push(1, index, Some(at + index * B));
        }
        f.contact.log_changed();
        f.engine.calls();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 1));
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 1"]);
        f.clock.set(timing.apply_deadline(at + 2 * B) - 1);
        f.tick();
        assert!(f.engine.calls().is_empty(), "before the deadline");
        f.clock.set(timing.apply_deadline(at + 2 * B));
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 2));
        f.contact.receive(r);
        assert_eq!(
            f.engine.calls(),
            [
                "skip 2".into(),
                format!("unresolved 2 at {}", at + 2 * B),
                "committed 2".into()
            ],
            "a commit learned at the deadline is late: entry 2 is never applied"
        );
        f.clock.set(timing.apply_deadline(at + 3 * B));
        f.tick();
        assert_eq!(
            f.engine.calls(),
            [
                "skip 3".to_owned(),
                format!("unresolved 3 at {}", at + 3 * B)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_listed_entry_this_log_lacks_is_unresolved_at_its_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 7),
            at_index: at,
            baseline: false,
        }];
        r.unlisted = Some(at + B);
        f.contact.receive(r);
        f.clock.set(timing.apply_deadline(at));
        f.tick();
        assert_eq!(f.engine.calls(), [format!("unresolved 7 at {at}")]);
        f.clock.set(timing.apply_deadline(at + B));
        f.tick();
        let marker = unlisted_id(at + B, B);
        assert_ne!(marker, u64::MAX, "the engine's own reserved id");
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved {marker} at {}", at + B)]
        );
        // A later overflow at a higher index adds nothing; one below the
        // lowest given is given again.
        for (n, unlisted) in [(1, at + 2 * B), (2, at)] {
            let mut r = f.report(2, None);
            r.seq += n;
            r.unlisted = Some(unlisted);
            f.contact.receive(r);
        }
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved {} at {at}", unlisted_id(at, B))]
        );
        f.clock.set(timing.apply_deadline(at + 2 * B));
        f.tick();
        assert!(
            f.engine.calls().is_empty(),
            "the overflow above the lowest given"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_listing_holds_the_64_earliest_entries_above_the_addressee_commit() {
        let f = fixture(&[&[1, 2]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let now = index_at(T0);
        for index in 1..=80 {
            // Descending at_index; index 80 lies past the listing horizon.
            let at = if index == 80 {
                now + 60 * 48
            } else {
                now + (90 - index) * 30
            };
            f.log.push(1, index, Some(at));
        }
        f.contact.log_changed();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 3));
        f.contact.receive(r);
        let (_, _, report) = f.tick().pop().unwrap();
        let indices: Vec<u64> = report.listed.iter().map(|l| l.id.index).collect();
        let expected: Vec<u64> = (16..=79).rev().collect();
        assert_eq!(indices, expected, "the earliest at_index first, 64 of them");
        assert_eq!(report.unlisted, Some(now + (90 - 15) * 30));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_timestamps_never_precede_the_flags_beside_them() {
        let f = fixture(&[&[1, 2]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        f.clock.step.store(1, Ordering::SeqCst);
        f.contact.receive(f.report(2, None));
        // The engine detects a step two reads on: after the period's own
        // timestamp and its sample of the engine, so the first report's
        // flags say nothing yet and none of its timestamps may postdate
        // the step.
        f.engine.step_at.store(f.clock.get() + 2, Ordering::SeqCst);
        for _ in 0..3 {
            let (_, _, r) = f.tick().pop().unwrap();
            let step_at = f.engine.step_at.load(Ordering::SeqCst);
            let echo = r.echo.unwrap();
            for t in [r.sent_at, echo.received_at] {
                assert!(
                    t < step_at || r.own.clock.step_fence,
                    "a timestamp taken at {t}, after the step at {step_at}, without its flag"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_truncated_entry_is_taken_back_and_its_successor_handed() {
        let f = fixture(&[&[1, 2, 3]]);
        for index in 1..=3 {
            f.log.push(1, index, None);
        }
        f.contact.log_changed();
        f.engine.calls();
        f.log.push(2, 3, None);
        f.contact.log_changed();
        assert_eq!(f.engine.calls(), ["truncated 3", "appended 3"]);
    }

    /// A stale listing at an index must not hide the deadline of the
    /// entry that index holds in the set's log.
    #[tokio::test(start_paused = true)]
    async fn review_two_listings_at_one_index_each_keep_their_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let early = index_at(T0) + 40 * 48;
        let late = early + 400 * 48;
        // A deposed leader's uncommitted entry at index 5 listed first,
        // then the entry the set committed there, with an earlier index.
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 5),
            at_index: late,
            baseline: false,
        }];
        f.contact.receive(r);
        let mut r = f.report(3, None);
        r.listed = vec![Listed {
            id: id(2, 5),
            at_index: early,
            baseline: false,
        }];
        f.contact.receive(r);
        f.engine.calls();
        f.clock.set(timing.apply_deadline(early));
        f.tick();
        let calls = f.engine.calls();
        assert!(
            calls.contains(&format!("unresolved 5 at {early}")),
            "entry (2, 5) passed its deadline unknown, not unresolved: {calls:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_truncated_entry_takes_its_deadline_with_it() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let early = index_at(T0) + 40 * 48;
        f.log.push(1, 1, Some(early));
        f.contact.log_changed();
        // Replaced by another leader's entry, far ahead.
        f.log.push(2, 1, Some(early + 4000 * B));
        f.contact.log_changed();
        f.engine.calls();
        f.clock.set(timing.apply_deadline(early));
        f.tick();
        assert!(f.engine.calls().is_empty(), "the cut entry's deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_without_room_withdraws_once_and_is_not_offered_again() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        f.engine.room.store(0, Ordering::SeqCst);
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 9),
            at_index: at,
            baseline: false,
        }];
        f.contact.receive(r);
        f.clock.set(timing.apply_deadline(at));
        f.tick();
        assert_eq!(f.engine.calls(), ["refused 9", "overwhelmed"]);
        f.engine.room.store(10, Ordering::SeqCst);
        for _ in 0..3 {
            f.tick();
        }
        assert!(f.engine.calls().is_empty(), "offered again");
        assert_eq!(f.contact.metrics().unresolved_refused, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_report_older_than_the_newest_taken_is_ignored() {
        let f = fixture(&[&[1, 2, 3]]);
        let first = f.tick()[0].2.seq;
        f.clock.add(Duration::from_millis(40));
        tokio::time::advance(Duration::from_millis(40)).await;
        let second = f.tick()[0].2.seq;
        let mut newer = f.report(2, Some((second, f.clock.get())));
        newer.seq = 1000;
        f.contact.receive(newer);
        let fence = f.fence();
        // The older report, delayed, echoes the older seq.
        let mut older = f.report(2, Some((first, T0)));
        older.seq = 999;
        f.contact.receive(older);
        let (_, _, to_2) = f.tick().into_iter().find(|(to, _, _)| *to == 2).unwrap();
        assert_eq!(
            to_2.echo.unwrap().seq,
            1000,
            "the newer report is the one echoed"
        );
        assert_eq!(f.fence(), fence);
    }

    #[tokio::test(start_paused = true)]
    async fn a_restarted_leader_waits_for_a_pending_generation_it_applied() {
        let f = fixture(&[&[1]]);
        // The state machine holds an InputUp whose index lies ahead.
        let at = index_at(T0) + 400 * B;
        f.log.0.lock().unwrap().pending.push((
            3,
            Body::InputUp {
                slot: 0,
                at_index: at,
            },
        ));
        let contact = Contact::new(
            1,
            Timing::defaults(B),
            f.clock.clone(),
            Some(f.engine.clone()),
            f.log.clone(),
            1,
        );
        contact.tick(f.members.clone());
        assert!(contact.stamp(false).is_ok());
        assert_eq!(contact.stamp(true), Err(StampRefusal::GenerationPending));
    }

    /// The log's view holds its lock while it walks the pending entries,
    /// as the store does: a replica started with one lists it without
    /// re-entering the view.
    #[tokio::test(start_paused = true)]
    async fn a_replica_started_with_pending_entries_lists_them() {
        let f = fixture(&[&[1, 2]]);
        let at = index_at(T0) + 20 * B;
        f.log.push(1, 3, Some(at));
        {
            let mut log = f.log.0.lock().unwrap();
            log.applied = 3;
            log.pending.push((
                3,
                Body::InputDown {
                    slot: 0,
                    at_index: at,
                },
            ));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let (clock, engine, log) = (f.clock.clone(), f.engine.clone(), f.log.clone());
        std::thread::spawn(move || {
            let contact = Contact::new(1, Timing::defaults(B), clock, Some(engine), log, 1);
            let _ = tx.send(contact);
        });
        let contact = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Contact::new deadlocked on the log's view");
        let (_, _, report) = contact.tick(f.members.clone()).pop().unwrap();
        assert_eq!(
            report.listed,
            [Listed {
                id: id(1, 3),
                at_index: at,
                baseline: false,
            }]
        );
    }

    /// Review probe: an entry the engine had no room for, listed again
    /// by the next reports, withdraws once.
    #[tokio::test(start_paused = true)]
    async fn review_a_relisted_entry_without_room_withdraws_once() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        f.engine.room.store(0, Ordering::SeqCst);
        f.clock.set(timing.apply_deadline(at));
        for n in 0..5u64 {
            let mut r = f.report(2, None);
            r.seq += n;
            r.listed = vec![Listed {
                id: id(1, 9),
                at_index: at,
                baseline: false,
            }];
            f.contact.receive(r);
            f.tick();
        }
        let calls = f.engine.calls();
        let n = calls.iter().filter(|c| *c == "overwhelmed").count();
        assert_eq!(n, 1, "withdrawn {n} times: {calls:?}");
    }
}
