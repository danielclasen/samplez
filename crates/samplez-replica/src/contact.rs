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
use samplez::blocks::{BlockStatus, Cause, Ranks, Standing};
use samplez::{Entry, ProductState};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::Shared;
use crate::baseline::{Leader, LeaderView, Voter};
use crate::entry::{Baseline, Body, NodeId, PROTOCOL, Request};
use crate::log_store::Store;
use crate::peer::{PeerRequest, PeerResponse};
use crate::rank::Baselines;
use crate::report::{Echo, Heard, Histogram, Listed, MAX_LISTED, Own, Report, Slack};
use crate::resync::{CoreReply, Missed, Resynced, Source};
use crate::schedule::Lead;
use crate::snapshot::StoredLogId;
use crate::state::{Life, PageBook, ReplicaState, Seen, View};
use crate::state_machine::{Product, Record, StateMachine};
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
    /// `resyncBudgetMs`: a resync handing over later than this from
    /// asking for the core warns.
    pub resync_budget: Duration,
}

impl Timing {
    /// The defaults at block size `block`: p 2 ms, w 8 ms, D 10 ms, a
    /// margin of 576 samples, clockGuard half of it, maxApplyLead 50 ms,
    /// resyncBudget 50 ms.
    pub fn defaults(block: u64) -> Timing {
        Timing {
            block,
            margin: 576,
            report_period: Duration::from_millis(2),
            contact_window: Duration::from_millis(8),
            hold: Duration::from_millis(10),
            clock_guard: 288,
            max_apply_lead: Duration::from_millis(50),
            resync_budget: Duration::from_millis(50),
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
    /// The kernel TAI offset, CLOCK_TAI - CLOCK_REALTIME read back to
    /// back and rounded to whole seconds. An audio replica reports the
    /// RT thread's sample instead; the witness reports this one.
    fn tai_offset_s(&self) -> Option<i64>;
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

    fn tai_offset_s(&self) -> Option<i64> {
        let read = |clock| {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: `ts` is writable; both clocks exist on Linux.
            unsafe { libc::clock_gettime(clock, &mut ts) };
            i128::from(ts.tv_sec) * 1_000_000_000 + i128::from(ts.tv_nsec)
        };
        let tai = read(libc::CLOCK_TAI);
        let real = read(libc::CLOCK_REALTIME);
        Some((tai - real + 500_000_000).div_euclid(1_000_000_000) as i64)
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
    /// Entry `index`, applied, derived `entries`, which take effect at
    /// `at_index`.
    fn appended(&self, index: u64, at_index: u64, entries: &[Entry]);
    /// The entries up to `up_to` are applied: what they derived goes to
    /// the RT thread.
    fn committed(&self, up_to: u64);
    /// Entry `index` at `at_index` is unresolved: every block from
    /// `at_index` is degraded until the log decides it. False when the
    /// engine had no room for it.
    fn unresolved(&self, index: u64, at_index: u64) -> bool;
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
    /// Output `output` stops committing at sample index `at`, a page
    /// boundary: the block holding it commits `[kB, at)` and the output
    /// is withdrawn from there (a key hand-over); `u64::MAX` lets a
    /// pending stop lapse.
    fn stop_at(&self, output: usize, at: u64);
    /// Entry `index`, given as unresolved, is decided: its blocks are no
    /// longer degraded for it.
    fn resolved(&self, _index: u64) {}
    /// Drops the running engine's core: its state no longer follows the
    /// log (snapshot install, purge-point verdict).
    fn drop_engine(&self) {}
    /// Builds the engine's core from `state`, the applied product state,
    /// and installs it at the first block at or after `at_least`, the
    /// floor of the applied log; false when that failed and is to be
    /// retried with a fresh read.
    fn rebuild_engine(&self, _state: &ProductState, _at_least: u64) -> bool {
        true
    }
    /// A donor's side of a resync: the engine's core after the block it
    /// computes next, encoded with the engine version; none when it
    /// copied none.
    fn copy_core(&self) -> Option<CoreReply> {
        None
    }
    /// A joiner's side: replays `core` from past input with the committed
    /// log entries after its last log id that `source` gives, and hands
    /// the replayed state to the RT thread at a block boundary while
    /// `source` says go. Blocks until handed over or failed. `source`
    /// takes contact's lock, which contact holds while it calls the other
    /// methods here: no lock those take may be held across a call to it.
    fn resync(&self, _core: &CoreReply, _source: &dyn Source) -> Result<Resynced, String> {
        Err("no engine to resync".to_owned())
    }
    /// Whether the engine's inputs read the flows a rebuild from `state`
    /// would have them read. A replay reads the inputs the engine reads,
    /// and a core copy carries no flows, so a resync cannot move an
    /// input that reads another flow.
    fn follows(&self, _state: &ProductState) -> bool {
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
    /// Derived engine entries left out before the engine saw them, as it
    /// refuses them: a product whose targets break its contract. The
    /// same on every replica, so its output still follows the log.
    pub entries_filtered: u64,
    /// Per output, realised.
    pub realised: Vec<bool>,
    /// Per output, enabled by the log; an output past the list is.
    pub enabled: Vec<bool>,
    /// The generation its pending entries need is built, or none is.
    pub generation_ready: bool,
    /// Clock steps the engine detected.
    pub clock_steps: u64,
    /// Times the engine dropped its core for committed entries its RT
    /// thread did not take: a rise returns the replica to `starting`,
    /// rebuilt from product state.
    pub log_overflows: u64,
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
    /// The log index of the first decision the state machine applied on
    /// (`index`, `term`).
    fn decided(&self, _index: u64, _term: u64) -> Option<u64> {
        None
    }
    /// The term of the entry at `index`, while it is known.
    fn term_at(&self, index: u64) -> Option<u64> {
        self.log_id_at(index).map(|id| id.term)
    }
    /// Calls `f` with the log index and record of every applied entry
    /// after `after` up to `up_to` that derived engine entries, shared,
    /// so taking one costs no copy of its entries under the store lock.
    /// False when a record after `after` is no longer held.
    fn records(&self, after: u64, up_to: u64, f: &mut dyn FnMut(u64, &Arc<Record>)) -> bool;
    /// The log index after which every applied entry's record is held
    /// ([`crate::state_machine::StateMachine::records_from`]).
    fn records_from(&self) -> u64 {
        0
    }
    /// The applied index, the floor there and the engine state of the
    /// applied log, read at once.
    fn product_state(&self) -> (u64, u64, ProductState);
    /// The floor after every entry below `index`, as the apply raises
    /// it ([`crate::state_machine::floored`]).
    fn floor_before(&self, index: u64) -> u64;
    /// The applied index and the floor after it, read at once.
    fn applied_floor(&self) -> (u64, u64);
    /// Engine entries pending above `floor`, read at once: the records
    /// of applied entries that take effect above it, which every engine
    /// holds until then, and the entries held unapplied whose index, as
    /// the apply will raise it, lies above it.
    fn pending_above(&self, floor: u64) -> usize;
    /// The product's configuration hash, none when it is not known.
    fn config_hash(&self) -> Option<u64> {
        None
    }
    /// The restamps behind the entry at `index`, as applied.
    fn restamps(&self, _index: u64) -> u32 {
        0
    }
    /// The generation applied decidable entries need
    /// ([`crate::state_machine::StateMachine::pending_generation`]).
    fn pending_generation(&self) -> Option<u64> {
        None
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
    fn decided(&self, index: u64, term: u64) -> Option<u64> {
        Store::decided(self, index, term)
    }
    fn term_at(&self, index: u64) -> Option<u64> {
        Store::term_at(self, index)
    }
    fn records(&self, after: u64, up_to: u64, f: &mut dyn FnMut(u64, &Arc<Record>)) -> bool {
        Store::records(self, after, up_to, f)
    }
    fn records_from(&self) -> u64 {
        Store::records_from(self)
    }
    fn product_state(&self) -> (u64, u64, ProductState) {
        Store::product_state(self)
    }
    fn floor_before(&self, index: u64) -> u64 {
        Store::floor_before(self, index)
    }
    fn applied_floor(&self) -> (u64, u64) {
        self.view(|n, sm| (n, sm.floor))
    }
    fn pending_above(&self, floor: u64) -> usize {
        Store::pending_above(self, floor)
    }
    fn config_hash(&self) -> Option<u64> {
        Some(Store::config_hash(self))
    }
    fn restamps(&self, index: u64) -> u32 {
        Store::restamps(self, index)
    }
    fn pending_generation(&self) -> Option<u64> {
        self.with_state(StateMachine::pending_generation)
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
    clock_holds: u64,
    late_rebuilds: u64,
}

/// What contact takes of a learner's newest report.
struct Learner {
    /// The product configuration hash it named.
    config: Option<u64>,
    /// The log protocol it speaks ([`crate::report::Own::protocol`]).
    protocol: u32,
    received: Instant,
}

/// A rebuild of the engine's core from product state. It waits for the
/// RT thread for up to the lead, so it runs on a thread of its own and a
/// later tick takes its result: contact goes on ticking meanwhile.
enum Rebuild {
    Idle,
    /// The log index its state folds in; stale once a restart began
    /// after it started.
    Running {
        n: u64,
        done: std::sync::mpsc::Receiver<bool>,
        stale: bool,
    },
    /// Installed, given the entries still unresolved; not yet taken. A
    /// retarget's is kept while the replica stays in `version_mismatch`:
    /// its next retarget takes it.
    Done,
}

struct State {
    members: Members,
    /// Each learner's newest report: its configuration hash and protocol
    /// are the only parts of it taken.
    learners: BTreeMap<NodeId, Learner>,
    seq: u64,
    sent: VecDeque<(u64, u64)>,
    peers: BTreeMap<NodeId, Peer>,
    in_contact: bool,
    regained: Instant,
    /// The last log index taken from the log as appended.
    told: u64,
    /// The floor after the entry at `told`, as the apply will raise it
    /// ([`crate::state_machine::floored`]).
    floor_told: u64,
    /// Entries taken as appended whose commit is not known, until then:
    /// a truncation is found among them.
    uncommitted: BTreeMap<u64, StoredLogId>,
    /// Entries with an at_index, for listings, until every voter reported
    /// them committed: log id, at_index, stamp, and whether it is a
    /// `Baseline`.
    scheduled: BTreeMap<u64, (StoredLogId, u64, u64, bool)>,
    /// The earliest index the last generation stamp reserved, whatever
    /// entry carries it: the applied log and the appended entries name
    /// the other pending generations.
    reserved: Option<u64>,
    /// The appended entries not applied yet that need a generation, by
    /// log index, with the index they take effect at.
    generations: BTreeMap<u64, u64>,
    /// Since the start or the last snapshot install this log has taken no
    /// entry past `catch_up_to` before the entry's apply deadline: the
    /// replica is catching up.
    catching_up: bool,
    /// The committed index the first voter report named while catching
    /// up: the entries up to it were written before the replica could take
    /// them, whatever lead each was stamped with.
    catch_up_to: Option<u64>,
    /// The entries not applied yet that this log took past their apply
    /// deadline while catching up, by log index.
    taken_late: BTreeSet<u64>,
    /// Entries taken late that went unresolved, while a restart was under
    /// way or the replica committed, for the rebuild past them once it
    /// ended or the replica commits nothing.
    late_given: BTreeSet<Key>,
    /// Entries taken late that the engine was rebuilt past instead of
    /// holding them unresolved: a decision on one neither resolves nor
    /// contradicts the rebuilt core. Past `GIVEN_KEPT` entries the lowest
    /// is forgotten: a decision on a forgotten one counts as a
    /// contradiction, which diverges the replica and costs it a resync,
    /// never a wrong commit.
    rebuilt_past: BTreeSet<Key>,
    /// When `own` was sampled.
    own_at: Instant,
    judged: BTreeSet<NodeId>,
    /// Slack of the entries learned committed, for the next reports.
    slack: Vec<Slack>,
    lead: Lead,
    /// The lead each entry with an at_index was stamped with, by log
    /// index, the lowest forgotten past [`SCHEDULED_MAX`]: kept after
    /// `scheduled` drops an entry every voter reported committed, since a
    /// replica that reported the commit may apply the entry, and report
    /// its slack, only later.
    leads: BTreeMap<u64, u64>,
    /// The replica whose slack last asked for a higher lead.
    lead_raised_by: Option<NodeId>,
    e_spread: Histogram,
    /// Every entry of this replica's log up to here is committed.
    known_committed: u64,
    /// Every entry up to here is applied: what it derived went to the
    /// engine, or it went unresolved.
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
    rebuild: Rebuild,
    /// The resync attempt in progress and the counts.
    resync: crate::resync::Tracker,
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
    verifier: crate::verify::Verifier,
    horizon: crate::horizon::Horizon,
    /// The clock hold has the replica in `starting`.
    held: bool,
    /// Clock steps the engine detected, as of the last sample.
    clock_steps: u64,
    /// What `replica_clock_unverified` names, and since when it has.
    unverified: Vec<(NodeId, Option<i64>)>,
    unverified_since: Option<Instant>,
    /// Voters reporting TAI offset 0.
    offset_zero: Vec<NodeId>,
    #[cfg(test)]
    verified_always: bool,
    decider: crate::decide::Decider,
    /// The audio `/readyz` term, as of the last period in contact; none
    /// before the first.
    serving: Option<bool>,
    /// Since when resync checks found an input on another flow than the
    /// applied state names ([`Audio::follows`]), and that state's
    /// [`State::flows_at`].
    off_flows_since: Option<(Instant, u64)>,
    /// The log index of the last entry applied here that re-routed or
    /// detached an input, or of the state a rebuild installed: the applied
    /// flows have not changed since.
    flows_at: u64,
    /// `Diverged` verdicts naming this replica, by log index and page,
    /// until their commit is known.
    verdicts: Vec<(u64, u64)>,
    realised: Vec<bool>,
    /// Per output, enabled by the log, as of the last sample; an output
    /// past the list is.
    enabled: Vec<bool>,
    generation_ready: bool,
    /// The key, revision and target revision of a rollout, once given.
    rollout: Option<Arc<dyn crate::rollout::Rollout>>,
    own: Own,
    status_changes_lost: u64,
    entries_refused: u64,
    entries_filtered: u64,
    log_overflows: u64,
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

/// Adds a counted replica's slack to the lead, naming the replica when
/// it asked for more, and warning once as the lead reaches its cap; an
/// entry whose stamp it knows counts the lead it needed too
/// ([`Lead::needs`]).
fn add_slack(st: &mut State, from: NodeId, slack: &Slack, current: u64) {
    if let Some(&lead) = st.leads.get(&slack.index) {
        st.lead.needs(lead, slack.samples);
    }
    let capped = st.lead.capped;
    if st.lead.add(slack.samples, slack.stamp, current) {
        st.lead_raised_by = Some(from);
        if st.lead.capped && !capped {
            tracing::warn!(
                replica = from,
                lead = st.lead.samples(),
                "the apply lead is at its cap: this replica applies entries past their \
                 deadline and holds them unresolved"
            );
        }
    }
}

/// Contact as status and metrics show it.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    /// `replica_protocol_gate_open`: every member speaks
    /// [`PROTOCOL`] ([`Contact::protocol_blocker`]), so the leader writes
    /// generation flags.
    pub protocol_gate_open: bool,
    /// The first member keeping the gate shut, its label.
    pub protocol_blocker: Option<NodeId>,
    /// This replica's product configuration hash
    /// ([`crate::state_machine::Product::config_hash`]), to compare with
    /// the other replicas'.
    pub config_hash: Option<u64>,
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
    /// Derived engine entries left out as the engine refuses them
    /// ([`Sample::entries_filtered`]).
    pub entries_filtered: u64,
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
    /// Rebuilds of the engine past entries taken late while catching up.
    pub late_rebuilds: u64,
    /// E, and the audio replicas it counts.
    pub estimate: Option<u64>,
    pub counted: Vec<NodeId>,
    /// The lead, in samples, and whether it wanted more than the cap.
    pub lead: u64,
    pub apply_lead_capped: bool,
    /// The lead decisions and `Baseline`s are stamped with, and commands
    /// at the cap ([`Contact::decision_lead`]), in samples.
    pub decision_lead: u64,
    /// The counted audio replica whose slack last asked for a higher
    /// lead: while the lead is capped, the one applying entries past
    /// their deadline, which holds them unresolved.
    pub apply_lead_raised_by: Option<NodeId>,
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
    /// The resyncs of this audio replica.
    pub resync: crate::resync::Metrics,
    /// The decisions taken while this voter led.
    pub leader: crate::baseline::Counts,
    /// `replica_disagreement`: the last page this voter judged as leader
    /// had no strict majority hash.
    pub replica_disagreement: bool,
    /// The clock check from this voter's offsets.
    pub clock: crate::clock::Status,
    /// `ClockVerdict` entries appended while this voter led.
    pub clock_verdicts: u64,
    /// This voter is clock-verified.
    pub verified: bool,
    /// The kernel TAI offset it reports, whole seconds.
    pub tai_offset_s: Option<i64>,
    /// Every unverified replica and every voter whose offset is not the
    /// set's (every voter while there is none), with its latest offset,
    /// as status shows them at once.
    pub clock_unverified: Vec<(NodeId, Option<i64>)>,
    /// `replica_clock_unverified`: that list has been non-empty for
    /// [`UNVERIFIED_DEBOUNCE`].
    pub replica_clock_unverified: bool,
    /// `replica_tai_offset_zero`: the voters reporting offset 0.
    pub replica_tai_offset_zero: Vec<NodeId>,
    /// The horizon holds this replica in `starting`.
    pub horizon_held: bool,
    /// Times the clock hold or the horizon took the replica to
    /// `starting`, its engine dropped.
    pub clock_holds: u64,
}

/// How long `replica_clock_unverified` waits before it raises, so the
/// window around a leap second, in which the set has no offset for the
/// nodes' clock spread plus one tick, raises nothing.
pub const UNVERIFIED_DEBOUNCE: Duration = Duration::from_secs(2);

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
        let (applied, floor_told) = log.applied_floor();
        let installs_seen = log.installs();
        let mut scheduled = BTreeMap::new();
        // Entries the engine applies later, purged ones included, for
        // listings.
        // Collected first: the log's view is not re-entered from inside it.
        let mut pending = Vec::new();
        log.pending(&mut |index, body| {
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
                floor_told,
                uncommitted: BTreeMap::new(),
                scheduled,
                reserved: None,
                generations: BTreeMap::new(),
                catching_up: true,
                catch_up_to: None,
                taken_late: BTreeSet::new(),
                late_given: BTreeSet::new(),
                rebuilt_past: BTreeSet::new(),
                own_at: Instant::now(),
                judged: BTreeSet::new(),
                learners: BTreeMap::new(),
                slack: Vec::new(),
                lead: Lead::new(
                    timing.block,
                    index_at(START_LEAD.as_nanos() as u64),
                    index_at(timing.max_apply_lead.as_nanos() as u64),
                ),
                leads: BTreeMap::new(),
                lead_raised_by: None,
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
                rebuild: Rebuild::Idle,
                resync: crate::resync::Tracker::default(),
                installs_seen,
                fence: None,
                ranks: None,
                baselines,
                withhold: Withhold::default(),
                life,
                pages: PageBook::default(),
                leader: Leader::default(),
                clock: crate::clock::Judge::default(),
                verifier: crate::verify::Verifier::default(),
                horizon: crate::horizon::Horizon::default(),
                held: false,
                clock_steps: 0,
                unverified: Vec::new(),
                unverified_since: None,
                offset_zero: Vec::new(),
                #[cfg(test)]
                verified_always: false,
                decider: crate::decide::Decider::default(),
                serving: None,
                off_flows_since: None,
                flows_at: 0,
                verdicts: Vec::new(),
                realised: Vec::new(),
                enabled: Vec::new(),
                generation_ready: true,
                rollout: None,
                own: Own::default(),
                status_changes_lost: 0,
                entries_refused: 0,
                entries_filtered: 0,
                log_overflows: 0,
                counts: Counts::default(),
            }),
        }
    }

    pub fn timing(&self) -> Timing {
        self.timing
    }

    /// Whether `b` hands the baseline over to another key than the
    /// latest `Baseline` applied carries.
    pub(crate) fn hands_over(&self, b: &Baseline) -> bool {
        self.log
            .baseline()
            .is_some_and(|cur| crate::state::differs(cur.key, b.key))
    }

    /// The compatibility key and revisions of a rollout
    /// ([`crate::rollout`]): without, every key is equal.
    pub fn set_rollout(&self, rollout: Arc<dyn crate::rollout::Rollout>) {
        self.lock().rollout = Some(rollout);
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
        if report.from == self.id {
            return;
        }
        if !voters.contains(&report.from) {
            // A learner applies the log too: its configuration and
            // protocol count.
            if st.members.addrs.contains_key(&report.from) {
                let learner = Learner {
                    config: report.own.config,
                    protocol: report.own.protocol,
                    received: now,
                };
                st.learners.insert(report.from, learner);
            }
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
                add_slack(st, report.from, slack, current);
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
        if st.catching_up && st.catch_up_to.is_none() {
            // The first voter to report sets it, whatever it knows: a
            // report without a committed id names 0, a lagging voter an
            // index below the set's.
            tracing::info!(
                voter = report.from,
                catch_up_to = committed_index,
                committed_known = report.committed.is_some(),
                told = st.told,
                "catching up to the committed index a voter reported"
            );
            st.catch_up_to = Some(committed_index);
        }
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

    /// The log changed: appends and truncations are taken, what the
    /// entries openraft applied derived goes to the engine, and they are
    /// known committed.
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
        st.learners
            .retain(|id, _| members.addrs.contains_key(id) && !voters.contains(id));
        st.members = members;
        self.expire(st, tai);
        self.sync_log(st, tai);
        // Alone, no voter names a commit to catch up to.
        if voters.len() == 1 && voters.contains(&self.id) {
            st.catching_up = false;
        }
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
            st.entries_filtered = sample.entries_filtered;
            st.realised = sample.realised;
            st.enabled = sample.enabled;
            st.generation_ready = sample.generation_ready;
            st.clock_steps = sample.clock_steps;
            st.pages.fold(self.id, &st.own.pages);
            if sample.log_overflows > st.log_overflows {
                st.log_overflows = sample.log_overflows;
                // The engine dropped its core and committed entries with
                // it: rebuilt from the state after them.
                self.begin_restart(st, applied);
            }
            if let Some(r) = &st.rollout {
                st.own.key = Some(r.key());
                st.own.revision = r.revision();
            }
        } else {
            st.own.clock.tai_offset_s = self.clocks.tai_offset_s();
        }
        st.own.config = self.log.config_hash();
        st.own.protocol = PROTOCOL;
        st.judged = self.log.judged();
        st.lead.roll();
        self.verify(st, now);

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
        // After the decisions known committed were acted on.
        if self.audio.is_some() {
            st.own.unresolved = Self::unresolved(st);
        }

        // Reports.
        st.seq += 1;
        if st.sent.len() == SENT_KEPT {
            st.sent.pop_front();
        }
        st.sent.push_back((st.seq, tai));
        if !voters.contains(&self.id) {
            // A learner's report carries what its promotion needs, its
            // configuration hash and protocol in `own`, and lists nothing:
            // a voter takes nothing else of it.
            st.slack.clear();
            let mut out = Vec::new();
            for to in voters {
                let Some(addr) = st.members.addrs.get(&to).cloned() else {
                    continue;
                };
                let report = Report {
                    from: self.id,
                    seq: st.seq,
                    sent_at: tai,
                    echo: None,
                    committed: self.log.committed(),
                    own: st.own.clone(),
                    heard: Vec::new(),
                    listed: Vec::new(),
                    unlisted: None,
                    estimate: None,
                    slack: Vec::new(),
                };
                out.push((to, addr, report));
            }
            return out;
        }
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
            let index = k.0;
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

    /// Takes new appends and truncations from the log: each entry with an
    /// index it takes effect at is listed and awaited there, the index the
    /// apply will derive at ([`crate::state_machine::floored`]); then
    /// hands what the applied entries derived to the engine.
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
            st.leads.split_off(&from);
            st.generations.split_off(&from);
            st.taken_late.split_off(&from);
            st.baselines.truncated(from);
            st.verdicts.retain(|&(index, _)| index < from);
            st.decisions.retain(|&(index, _, _)| index < from);
            st.purge_points.retain(|&(index, _)| index < from);
            st.told = from - 1;
            st.floor_told = self.log.floor_before(from);
        }
        // Collected first: the log's view is not re-entered from inside
        // it.
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
            let applied = self.installed(st);
            appended.retain(|(id, _)| id.index > applied);
        }
        // Purged past what was taken: the floor rises over the entries
        // purged, all applied, as it did on their apply; at the applied
        // state's when the first one held is applied too, never below
        // the floor its apply derived.
        if let Some((first, _)) = appended.iter().find(|(id, _)| id.index > st.told)
            && first.index > st.told + 1
        {
            st.floor_told = self.log.floor_before(first.index);
        }
        let mut new = Vec::new();
        for (id, request) in appended {
            if id.index <= st.told {
                continue;
            }
            let at = crate::state_machine::floored(st.floor_told, &request);
            if let Some(at) = at {
                st.floor_told = at;
                if request.body.needs_generation() {
                    st.generations.insert(id.index, at);
                }
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
            new.push((id, at, baseline, request.stamp));
        }
        for (id, at, baseline, stamp) in new {
            st.told = id.index;
            if id.index > st.known_committed {
                st.uncommitted.insert(id.index, id);
            }
            if let Some(at) = at {
                st.scheduled.insert(id.index, (id, at, stamp, false));
                st.leads.insert(id.index, at.saturating_sub(stamp));
                while st.leads.len() > SCHEDULED_MAX {
                    st.leads.pop_first();
                }
                let late = tai >= self.timing.apply_deadline(at);
                // Caught up once an entry past the commit named when
                // catching up began is taken in time; one before it may
                // be in time on a longer lead, a generation's, alone.
                if !late && st.catch_up_to.is_some_and(|to| id.index > to) {
                    st.catching_up = false;
                }
                if st.catching_up && late {
                    st.taken_late.insert(id.index);
                }
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

    /// The entry behind the restamps ending at `index`, with its own log
    /// index; at most [`crate::decide::RESTAMPS_MAX`] lie behind one, and
    /// a missing one ends the walk.
    fn original(&self, mut index: u64) -> Option<(u64, Request)> {
        for _ in 0..=crate::decide::RESTAMPS_MAX {
            let named = self.log.request(index)?;
            match named.body {
                Body::Restamp { revision, .. } => index = revision,
                _ => return Some((index, named)),
            }
        }
        None
    }

    /// The log index of the first decision on (`index`, `term`): the one
    /// the state machine applied, else the first the log holds.
    fn first_decision(&self, index: u64, term: u64) -> Option<u64> {
        if let Some(d) = self.log.decided(index, term) {
            return Some(d);
        }
        let mut first = None;
        self.log.entries_after(index, &mut |id, request| {
            if first.is_none() && request.body.decides() == Some((index, term)) {
                first = Some(id.index);
            }
        });
        first
    }

    /// Whether an entry committed after the one restamped up to `index`,
    /// up to `committed`, sets every engine target it sets: then a
    /// restamp would undo that entry. Only an engine body can be: a
    /// product entry's restamp re-asserts the state at its own place,
    /// which holds every later entry before it.
    fn superseded(&self, index: u64, committed: u64) -> bool {
        let Some((from, original)) = self.original(index) else {
            return true;
        };
        let mut superseded = false;
        self.log.entries_after(from, &mut |id, request| {
            superseded |= id.index <= committed && crate::decide::supersedes(&original, request);
        });
        superseded
    }

    /// A snapshot replaced the state up to the applied index, returned:
    /// the engine is built again from it (plan 2.11, Unresolved entries),
    /// and every entry the replica held unresolved is resolved by it.
    fn installed(&self, st: &mut State) -> u64 {
        let (applied, floor) = self.log.applied_floor();
        st.told = applied;
        st.floor_told = floor;
        st.known_committed = st.known_committed.max(applied);
        st.handed = st.handed.max(applied);
        st.uncommitted.clear();
        st.generations.clear();
        st.catching_up = true;
        st.catch_up_to = None;
        st.taken_late.clear();
        st.late_given.clear();
        st.rebuilt_past.clear();
        st.awaiting.clear();
        st.given.clear();
        st.unlisted.clear();
        st.unlisted_low = None;
        st.verdicts.retain(|&(index, _)| index > applied);
        st.decisions.retain(|&(index, _, _)| index > applied);
        st.purge_points.retain(|&(index, _)| index > applied);
        st.counts.installs += 1;
        self.begin_restart(st, applied);
        applied
    }

    /// Returns the replica to `starting` until its applied index passed
    /// `after` and its engine was rebuilt from product state.
    fn begin_restart(&self, st: &mut State, after: u64) {
        st.restart = Some(st.restart.map_or(after, |a| a.max(after)));
        // A rebuild under way installs a state from before the restart.
        if let Rebuild::Running { stale, .. } = &mut st.rebuild {
            *stale = true;
        } else {
            st.rebuild = Rebuild::Idle;
        }
        // The engine is rebuilt from product state: no resync hands over.
        st.resync.abandon(Instant::now());
        if let (Some(audio), Some(life)) = (&self.audio, st.life.as_mut()) {
            life.restart(audio.as_ref());
            audio.drop_engine();
        }
    }

    /// Whether the engine's core was rebuilt from the applied product
    /// state, at the floor of the applied log: true once, for a rebuild
    /// done. Else one is started unless one runs, read fresh after one
    /// that failed.
    fn rebuild(&self, st: &mut State, audio: &Arc<dyn Audio>) -> bool {
        match st.rebuild {
            Rebuild::Done => {
                st.rebuild = Rebuild::Idle;
                return true;
            }
            Rebuild::Running { .. } => return false,
            Rebuild::Idle => {}
        }
        let (n, floor, state) = self.log.product_state();
        let audio = Arc::clone(audio);
        // Dropped unsent, by a panic or a thread never started: failed.
        let (sent, done) = std::sync::mpsc::sync_channel(1);
        let run = move || {
            let _ = sent.send(audio.rebuild_engine(&state, floor));
        };
        // A blocking task in a runtime: a paused clock holds still while
        // it runs, as it did while the tick waited.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn_blocking(run);
        } else if let Err(e) = std::thread::Builder::new()
            .name("rebuild".into())
            .spawn(run)
        {
            tracing::warn!(error = %e, "no thread for an engine rebuild: retried");
        }
        st.rebuild = Rebuild::Running {
            n,
            done,
            stale: false,
        };
        false
    }

    /// Takes the result of a rebuild that ended. The rebuilt core holds no
    /// unresolved entry: it is given those still held. A stale one's core
    /// is dropped again; a failed one is retried.
    fn rebuild_ended(&self, st: &mut State, audio: &dyn Audio) {
        let Rebuild::Running { done, .. } = &st.rebuild else {
            return;
        };
        let ok = match done.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            ended => ended.unwrap_or(false),
        };
        let Rebuild::Running { n, stale, .. } = std::mem::replace(&mut st.rebuild, Rebuild::Idle)
        else {
            return;
        };
        if !ok {
            return;
        }
        if stale {
            audio.drop_engine();
            return;
        }
        // On a replica committing nothing, an entry held unresolved that
        // the state the core was built from applied is in the core:
        // rebuilt past, as one taken late while catching up. Held again it
        // could stay unresolved for good: the log decides an entry that
        // stands only when the baseline held it unresolved as it committed
        // its block, or every live replica withheld that block. A replica
        // about to commit, such as one retargeted
        // from `version_mismatch`, holds it again, as its peers do.
        let rebuilds = Self::commits_nothing(st);
        let past: Vec<Key> = (st.given.keys())
            .filter(|&&(index, term, node)| {
                rebuilds
                    && index <= n
                    && self.log.log_id_at(index) == Some(StoredLogId { term, node, index })
            })
            .copied()
            .collect();
        for k in past {
            st.given.remove(&k);
            st.late_given.remove(&k);
            Self::rebuild_past(st, k);
        }
        for (&(index, _, _), &at) in &st.given {
            let _ = audio.unresolved(index, at);
        }
        self.rebuilt(st, n);
        st.rebuild = Rebuild::Done;
    }

    /// Takes the result of a rebuild under way once it ended.
    #[cfg(test)]
    fn await_rebuild(&self) {
        let Some(audio) = self.audio.clone() else {
            return;
        };
        loop {
            let mut st = self.lock();
            self.rebuild_ended(&mut st, audio.as_ref());
            if !matches!(st.rebuild, Rebuild::Running { .. }) {
                return;
            }
            drop(st);
            std::thread::yield_now();
        }
    }

    /// A core was rebuilt from the state the entries up to `n` left: what
    /// they derived is in it, not handed again.
    fn rebuilt(&self, st: &mut State, n: u64) {
        st.flows_at = st.flows_at.max(n);
        if n > st.handed {
            st.handed = n;
            st.awaiting = st.awaiting.split_off(&(n + 1, 0, 0));
            st.generations = st.generations.split_off(&(n + 1));
            st.taken_late = st.taken_late.split_off(&(n + 1));
        }
    }

    /// An entry this replica knows of: unresolved at once when its apply
    /// deadline has passed, else awaited unless it is applied here.
    fn note(&self, st: &mut State, id: StoredLogId, at_index: u64, tai: u64) {
        // At or below the applied index an entry is this log's own,
        // applied and handed, or one that can no longer commit.
        let k = key(id);
        if id.index <= st.handed || st.awaiting.contains_key(&k) || st.given.contains_key(&k) {
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

    /// Every entry of this replica's log up to `index` is committed: for
    /// F, listings and baselines. Entries are handed and settled as they
    /// apply ([`Contact::hand`]).
    fn learn(&self, st: &mut State, index: u64) {
        if index <= st.known_committed {
            return;
        }
        st.known_committed = index;
        let log = &self.log;
        st.baselines
            .learned(index, self.clocks.tai_ns(), |i| log.log_id_at(i));
        st.uncommitted = st.uncommitted.split_off(&(index + 1));
    }

    /// Hands the records of the entries applied since the last call to
    /// the engine, each at the index it derived. One applied at or past
    /// its apply deadline, or already given unresolved, is not: it goes
    /// unresolved, the log deciding it, or, taken past its deadline while
    /// catching up on a replica committing nothing, the engine is rebuilt
    /// past it, one rebuild at a time. Awaited
    /// entries up to the applied index are settled: this replica's own,
    /// or another at the same index that can no longer commit.
    fn hand(&self, st: &mut State) {
        let up_to = self.log.applied().min(st.told);
        if up_to <= st.handed {
            return;
        }
        let from = st.handed;
        st.handed = up_to;
        st.awaiting = st.awaiting.split_off(&(up_to + 1, 0, 0));
        st.generations = st.generations.split_off(&(up_to + 1));
        let later = st.taken_late.split_off(&(up_to + 1));
        let taken_late = std::mem::replace(&mut st.taken_late, later);
        let Some(audio) = self.audio.clone() else {
            return;
        };
        let mut records = Vec::new();
        let whole = self.log.records(from, up_to, &mut |index, record| {
            records.push((index, Arc::clone(record)));
        });
        if !whole {
            // Records this replica derived went before they were handed:
            // its engine lacks them, so it is built again.
            tracing::warn!(from, up_to, "derived records gone before they were handed");
            // As below: the rebuild folds in what a replica committing
            // nothing took late, so none of it stays given.
            if Self::commits_nothing(st) {
                let folded: Vec<Key> = (st.given.keys())
                    .filter(|&&k @ (index, term, node)| {
                        st.late_given.contains(&k)
                            || (index > from
                                && index <= up_to
                                && taken_late.contains(&index)
                                && self.log.log_id_at(index)
                                    == Some(StoredLogId { term, node, index }))
                    })
                    .copied()
                    .collect();
                for k in folded {
                    st.given.remove(&k);
                    Self::rebuild_past(st, k);
                }
                st.late_given.clear();
            } else {
                // Committing, those taken late stay given until it commits
                // nothing: the restart must not drop them, as below.
                let late: Vec<Key> = (st.given.keys())
                    .filter(|&&(index, term, node)| {
                        index > from
                            && index <= up_to
                            && taken_late.contains(&index)
                            && self.log.log_id_at(index) == Some(StoredLogId { term, node, index })
                    })
                    .copied()
                    .collect();
                st.late_given.extend(late);
            }
            self.begin_restart(st, up_to);
            return;
        }
        // An entry held unresolved that applied and derived no record
        // changes no engine state: it is released, here as on a replica
        // that applied it in time, rather than held until a decision the
        // log appends for an entry that stands only when the baseline held
        // it unresolved as it committed its block, or every live replica
        // withheld that block. Should one come, the entry is rebuilt past,
        // so the decision neither resolves nor contradicts this replica.
        let rerouted = records.iter().rev().find(|(_, r)| {
            (r.entries.iter()).any(|e| matches!(e, Entry::Route { .. } | Entry::Detach { .. }))
        });
        if let Some(&(index, _)) = rerouted {
            st.flows_at = st.flows_at.max(index);
        }
        let derived: BTreeSet<u64> = records.iter().map(|(index, _)| *index).collect();
        let empty: Vec<Key> = (st.given.keys())
            .filter(|&&(index, term, node)| {
                index > from
                    && index <= up_to
                    && !derived.contains(&index)
                    && self.log.log_id_at(index) == Some(StoredLogId { term, node, index })
            })
            .copied()
            .collect();
        for k in empty {
            st.given.remove(&k);
            st.late_given.remove(&k);
            audio.resolved(k.0);
            Self::rebuild_past(st, k);
        }
        let tai = self.clocks.tai_ns();
        let first = index_at(tai + self.timing.apply_guard_ns());
        // An entry this log took past its deadline, as a replica catching
        // up takes the entries written while it was away, cannot have been
        // applied in time anywhere here. A replica committing nothing takes
        // it through a rebuild from the state after it rather than holding
        // it unresolved: no block it committed lacks it, and the log
        // appends no decision on an entry that stands, so one held
        // unresolved would degrade its blocks for good.
        // One rebuild past them at a time: while a restart is under way
        // they go unresolved, and are folded into a rebuild once it ended
        // ([`Contact::fold_late`]). So do those applied while the replica
        // commits, as one restarted does for a moment before its log has
        // caught up: they stand on the replicas that applied them in time,
        // which the log never decides, so they are left out of the rebuild
        // that follows once it commits nothing.
        let rebuilds = Self::commits_nothing(st);
        let at_once = rebuilds && st.restart.is_none();
        let mut late: Option<(u64, u64)> = None;
        for (index, record) in records {
            let at = record.at_index;
            // Purged from the log since it applied: this log's own, which
            // no decision names by an id this replica holds, so it is
            // handed whatever its deadline, the engine counting it late if
            // its index passed. A record this old needs a lag past the
            // entries the log keeps before records are retained again, so
            // it is rare; when it is late the engine marks itself diverged
            // and this replica is rebuilt.
            let Some(id) = self.log.log_id_at(index) else {
                audio.appended(index, at, &record.entries);
                continue;
            };
            let k = key(id);
            let taken = taken_late.contains(&index);
            let rebuilt = rebuilds && taken;
            if st.given.contains_key(&k) {
                if rebuilt && at_once {
                    st.given.remove(&k);
                    audio.resolved(index);
                    Self::rebuild_past(st, k);
                    late = Some(late.map_or((index, index), |(first, _)| (first, index)));
                } else if taken {
                    st.late_given.insert(k);
                }
                continue;
            }
            if tai >= self.timing.apply_deadline(at) {
                if rebuilt && at_once {
                    Self::rebuild_past(st, k);
                    late = Some(late.map_or((index, index), |(first, _)| (first, index)));
                    continue;
                }
                if taken {
                    st.late_given.insert(k);
                }
                st.awaiting.insert(
                    k,
                    Waiting {
                        at_index: at,
                        expired: true,
                    },
                );
                continue;
            }
            audio.appended(index, at, &record.entries);
        }
        self.offer_unresolved(st);
        audio.committed(up_to);
        // Slack against the apply deadline: the first index this replica
        // could still apply an entry at, as it applies.
        let now = Instant::now();
        let counted = self.counted(st, now).contains(&self.id);
        let current = self.current_stamp(st, now);
        let handed: Vec<Slack> = st
            .scheduled
            .range(from + 1..=up_to)
            .filter(|(_, e)| !e.3)
            .map(|(&index, &(_, at, stamp, _))| Slack {
                index,
                samples: at as i64 - first as i64,
                stamp,
            })
            .collect();
        for s in handed {
            if counted {
                add_slack(st, self.id, &s, current);
            }
            if st.slack.len() < MAX_LISTED {
                st.slack.push(s);
            }
        }
        if let Some((first, last)) = late {
            self.rebuild_past_late(st, first, last, up_to);
        }
        self.fold_late(st);
    }

    /// Entries from `first` to `last` taken late were left out of the
    /// engine: it is rebuilt from the state after `after`.
    fn rebuild_past_late(&self, st: &mut State, first: u64, last: u64, after: u64) {
        st.counts.late_rebuilds += 1;
        tracing::debug!(first, last, after, "engine rebuilt past entries taken late");
        self.begin_restart(st, after);
    }

    /// Entries taken late that went unresolved while a restart was under
    /// way or the replica committed: once it ended, on a replica
    /// committing nothing, the engine is rebuilt past them. Whether a
    /// restart began.
    fn fold_late(&self, st: &mut State) -> bool {
        if st.restart.is_some() || st.late_given.is_empty() {
            return false;
        }
        let given = &st.given;
        st.late_given.retain(|k| given.contains_key(k));
        let Some(audio) = self.audio.clone() else {
            return false;
        };
        let (Some(&(first, ..)), Some(&(last, ..))) = (st.late_given.first(), st.late_given.last())
        else {
            return false;
        };
        if !Self::commits_nothing(st) {
            return false;
        }
        for k in std::mem::take(&mut st.late_given) {
            st.given.remove(&k);
            audio.resolved(k.0);
            Self::rebuild_past(st, k);
        }
        self.rebuild_past_late(st, first, last, st.handed);
        true
    }

    fn rebuild_past(st: &mut State, k: Key) {
        st.rebuilt_past.insert(k);
        while st.rebuilt_past.len() > GIVEN_KEPT {
            st.rebuilt_past.pop_first();
        }
    }

    /// Whether this audio replica is starting, withdrawn or diverged with
    /// outputs none of which commits, as of its last sample.
    fn commits_nothing(st: &State) -> bool {
        let state = st.life.as_ref().map(Life::state);
        let standing = &st.own.standing;
        matches!(
            state,
            Some(ReplicaState::Starting | ReplicaState::Withdrawn | ReplicaState::Diverged)
        ) && !standing.is_empty()
            && !standing
                .iter()
                .any(|s| matches!(s, Standing::Committing { .. }))
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
        let class = match st.life.as_ref() {
            Some(l) if l.state().diverged() => crate::rank::Class::Diverged,
            Some(l) if l.filler() => crate::rank::Class::Filler,
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
        // The clock hold: an unverified replica, or one beyond the
        // horizon, returns to `starting` with its engine dropped, and is
        // rebuilt only once the hold ends.
        let held = !st.verifier.verified() || st.horizon.held();
        if held && !st.held {
            st.counts.clock_holds += 1;
            self.begin_restart(st, self.log.applied());
        }
        st.held = held;
        self.rebuild_ended(st, audio.as_ref());
        let rebuilding = match st.restart {
            Some(after) if !held && self.log.applied() >= after => {
                let done = self.rebuild(st, audio);
                if done {
                    st.restart = None;
                }
                !done
            }
            Some(_) => true,
            None => false,
        };
        let rebuilding = self.fold_late(st) || rebuilding;
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
        // Taken once done, else started after the step.
        let retargeted = matches!(st.rebuild, Rebuild::Done);
        let retarget_asked = std::cell::Cell::new(false);
        let retarget = || {
            retarget_asked.set(true);
            retargeted
        };
        let life = st.life.as_mut().expect("checked above");
        let view = View {
            me: self.id,
            own: &st.own,
            realised: &st.realised,
            ready,
            alone: st.members.voters().len() == 1,
            baseline: baseline.as_ref(),
            key: st.own.key,
            retarget: &retarget,
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
        if retarget_asked.get() {
            self.rebuild(st, audio);
        }
        // Done for a replica no longer in `version_mismatch`, which no
        // retarget takes: the replica left it another way, and its engine
        // goes on from there.
        if matches!(st.rebuild, Rebuild::Done) && state != ReplicaState::VersionMismatch {
            st.rebuild = Rebuild::Idle;
        }
        if state == ReplicaState::Committing {
            st.resync.committing();
        }
        st.own.state = Some(state);
        st.own.ready = ready;
        st.own.baseline = baseline.map(|b| (b.replica, b.at_index));
        // Diverged, resyncing or in `version_mismatch` with outputs, or
        // withdrawn once a `Baseline` applied with an output enabled, it
        // writes none of what it computes, in contact or not. A disabled
        // output writes nothing on any replica, so a withdrawn replica's
        // counts for none: its engine follows the log, so the outputs it
        // reports disabled are the log's. A resync drops the engine's
        // core, which then takes no entry, so in those states every
        // output counts. Before the first `Baseline` no replica of the
        // set committed, as the Named rule starts the first commit, and
        // every one is withdrawn once ready: it keeps the term by its
        // peers, so the first of a set becomes ready. A restarted replica
        // finds the baseline in its log.
        let outputs = st.own.standing.len();
        let writes_nothing = match state {
            ReplicaState::Diverged | ReplicaState::Resync | ReplicaState::VersionMismatch => {
                outputs > 0
            }
            ReplicaState::Withdrawn => {
                st.own.baseline.is_some()
                    && (0..outputs).any(|o| st.enabled.get(o).copied().unwrap_or(true))
            }
            _ => false,
        };
        if writes_nothing {
            st.serving = Some(false);
        } else if st.in_contact {
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
            // Rebuilt past: the core holds the state after it and resumes
            // only on matching pages.
            let past: Vec<Key> = st
                .rebuilt_past
                .range((revision, term, 0)..=(revision, term, u64::MAX))
                .copied()
                .collect();
            if !past.is_empty() {
                for k in past {
                    st.rebuilt_past.remove(&k);
                }
                continue;
            }
            // Applied here: its own committed entry, handed to the engine.
            // Applied here: an entry of this term it was handed. An index
            // whose term this replica no longer knows was never one it
            // can tell it applied.
            let applied = revision <= st.handed && self.log.term_at(revision) == Some(term);
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
            // The rebuild from product state past `after` supersedes every
            // unresolved entry at or below it: none is reported again.
            st.given
                .retain(|k, _| k.0 > after || k.0 & UNLISTED_BIT != 0);
            st.awaiting.retain(|k, _| k.0 > after);
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
    /// contact. False while diverged, resyncing or in `version_mismatch`,
    /// or withdrawn once a `Baseline` applied, with an output enabled, in
    /// contact or not: the replica writes none of what it computes. A set
    /// whose outputs are all disabled writes nothing anywhere, and such a
    /// replica serves as one without outputs does. Always true for
    /// the witness and for the only voter of its set; never for a learner,
    /// which commits nothing, a set's only voter writing the outputs
    /// alone.
    pub fn serving(&self) -> bool {
        let st = self.lock();
        if self.audio.is_none() {
            return true;
        }
        let voters = st.members.voters();
        if !voters.contains(&self.id) {
            return false;
        }
        voters.len() == 1 || st.serving.unwrap_or(false)
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
        let target = st.rollout.as_ref().and_then(|r| r.target());
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
        let superseded = |index: u64| self.superseded(index, committed);
        let restamps = |index: u64| log.restamps(index);
        let log_id_at = |index: u64| log.log_id_at(index);
        let decided = |index: u64, term: u64| self.first_decision(index, term).is_some();
        let leader_log = crate::decide::LeaderLog {
            log_id_at: &log_id_at,
            committed,
            purged: log.purged(),
            own_term_committed,
            restamps: &restamps,
            superseded: &superseded,
            decided: &decided,
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
            target: target.as_deref(),
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
    /// Clock verification and the horizon for this period: sets the
    /// unverified flag the reports carry and what
    /// `replica_clock_unverified` and `replica_tai_offset_zero` name.
    fn verify(&self, st: &mut State, now: Instant) {
        use crate::verify::Voter;
        st.own.clock.unverified = !st.verifier.verified();
        let mut heard = BTreeMap::new();
        heard.insert(
            self.id,
            Voter {
                offset_s: st.own.clock.tai_offset_s,
                theta_ns: Some(0),
                delay_ns: 0,
            },
        );
        let mut unverified = BTreeMap::new();
        for (id, _, own) in self.copies(st, now) {
            if own.clock.unverified {
                unverified.insert(id, own.clock.tai_offset_s);
            }
            if id == self.id {
                continue;
            }
            let best = st.peers.get(&id).and_then(|p| p.thetas.best());
            heard.insert(
                id,
                Voter {
                    offset_s: own.clock.tai_offset_s,
                    theta_ns: best.map(|b| b.0),
                    delay_ns: best.map_or(0, |b| b.1),
                },
            );
        }
        let counted = self.counted(st, now);
        let view = crate::verify::View {
            me: self.id,
            members: &st.members,
            heard: &heard,
            counted: &counted,
            guard_ns: time_of(self.timing.clock_guard),
        };
        let verifies = view.verifies();
        let conflict = view.conflict();
        let off = view.off_offset();
        #[cfg(test)]
        let verifies = verifies || st.verified_always;
        st.verifier.period(
            st.own.clock.tai_offset_s,
            st.clock_steps,
            conflict,
            now,
            || verifies,
        );
        let verified = st.verifier.verified();
        st.own.clock.unverified = !verified;
        if verified {
            unverified.remove(&self.id);
        } else {
            unverified.insert(self.id, st.own.clock.tai_offset_s);
        }
        unverified.extend(off);
        st.unverified = unverified.into_iter().collect();
        st.unverified_since = if st.unverified.is_empty() {
            None
        } else {
            st.unverified_since.or(Some(now))
        };
        let voters = st.members.voters();
        st.offset_zero = heard
            .iter()
            .filter(|(id, v)| voters.contains(id) && v.offset_s == Some(0))
            .map(|(&id, _)| id)
            .collect();
        // The horizon, for a verified audio replica E does not count.
        let b = self.timing.block;
        let end = st.own.processed.map(|k| (k + 1) * b);
        let estimate = self.estimate_in(st, now);
        let spread = st.e_spread.quantile(0.9999).unwrap_or(0);
        st.horizon.period(
            end.filter(|_| verified && self.audio.is_some()),
            estimate,
            counted.contains(&self.id),
            index_at(self.timing.apply_guard_ns()),
            spread,
        );
    }

    /// Unit tests that drive the log, not the clock: this replica counts
    /// as verified from its first period.
    #[cfg(test)]
    pub(crate) fn verified_always(&self) {
        self.lock().verified_always = true;
    }

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

    /// Drops scheduled entries every voter reported committed and this
    /// replica applied: one committed everywhere but not applied here
    /// still gives its slack at the apply-hand ([`Contact::hand`]), the
    /// sample of a replica that applies late.
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
            .unwrap_or(0)
            .min(st.handed);
        st.scheduled = st.scheduled.split_off(&(lowest + 1));
    }

    /// A member of the applied membership, learners included, whose
    /// newest report, direct or relayed, names another product
    /// configuration hash than this replica's: it would apply a command
    /// into another state, so the leader orders none while one does. A
    /// report without a hash names none.
    pub fn config_differs(&self) -> Option<NodeId> {
        let ours = self.log.config_hash()?;
        let st = self.lock();
        let now = Instant::now();
        let voter = self.copies(&st, now).into_iter().find_map(|(id, _, own)| {
            (st.members.addrs.contains_key(&id) && own.config.is_some_and(|c| c != ours))
                .then_some(id)
        });
        voter.or_else(|| {
            (st.learners.iter())
                .find(|(_, l)| l.config.is_some_and(|c| c != ours))
                .map(|(&id, _)| id)
        })
    }

    /// Whether learner `id` reported and its newest report names this
    /// replica's product configuration hash, or none, as a replica that
    /// records none does: only then is it promoted. One not heard from yet
    /// is not.
    pub fn learner_holds_set_config(&self, id: NodeId) -> bool {
        let ours = self.log.config_hash();
        self.lock()
            .learners
            .get(&id)
            .is_some_and(|l| l.config.is_none_or(|c| ours.is_none_or(|o| o == c)))
    }

    /// Whether learner `id` may be promoted: it holds the set's product
    /// configuration ([`Contact::learner_holds_set_config`]) and its newest
    /// report speaks [`PROTOCOL`] or later, since a voter of an older
    /// protocol would apply generation flags it does not know.
    pub fn learner_promotable(&self, id: NodeId) -> bool {
        self.learner_holds_set_config(id)
            && self
                .lock()
                .learners
                .get(&id)
                .is_some_and(|l| l.protocol >= PROTOCOL)
    }

    /// The protocol gate: the first member of `members`, learners
    /// included, whose newest report, direct or relayed, is older than
    /// `w + D`, missing, or speaks a protocol below [`PROTOCOL`]; none
    /// while every member speaks it, when the leader may write generation
    /// flags. This replica counts as its own binary's. A member contact's
    /// last tick did not know yet has no report here, so it blocks.
    pub fn protocol_blocker(&self, members: &Members) -> Option<NodeId> {
        let st = self.lock();
        let now = Instant::now();
        let fresh = self.timing.contact_window + self.timing.hold;
        let copies = self.copies(&st, now);
        let voters = members.voters();
        members.addrs.keys().copied().find(|&id| {
            let speaks = if id == self.id {
                true
            } else if voters.contains(&id) {
                copies
                    .iter()
                    .any(|(v, age, own)| *v == id && *age <= fresh && own.protocol >= PROTOCOL)
            } else {
                st.learners
                    .get(&id)
                    .is_some_and(|l| since(now, l.received) <= fresh && l.protocol >= PROTOCOL)
            };
            !speaks
        })
    }

    /// The engine refused an appended entry: the replica's output no
    /// longer follows the log, so it takes no client writes.
    pub fn engine_failed(&self) -> bool {
        self.lock().entries_refused > 0
    }

    /// Whether E counts this replica ([`Contact::counted`]); always for
    /// the witness, which E never counts. A replica E does not count takes
    /// no client writes: its stamps are outside E's frame.
    pub fn counts_self(&self) -> bool {
        self.audio.is_none()
            || self
                .counted(&self.lock(), Instant::now())
                .contains(&self.id)
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
    /// A generation waits for the pending one's index to pass: the
    /// latest of what the applied state built
    /// ([`LogView::pending_generation`]), what the appended entries not
    /// applied yet need, and the earliest index the last generation stamp
    /// reserved, whatever entry carries it, a command that re-routes an
    /// input included; one that is not written after all holds the next
    /// for at most its lead.
    pub fn stamp(&self, generation: bool) -> Result<Stamp, StampRefusal> {
        let mut st = self.lock();
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
        if self.log.pending_above(floor) >= FUTURE_LIST {
            return Err(StampRefusal::FutureListFull);
        }
        // At the cap a command takes the lead the entries needed too:
        // stamped with the capped lead it would be late on every replica
        // whose commit takes longer, as long as that lasts, and each one a
        // degraded block.
        let lead = st.lead.stamping();
        let earliest = if generation {
            // The last one's at_index has passed on the baseline, or on E
            // while no baseline is named.
            // Applied, appended, or reserved by the last generation stamp.
            let pending = [
                self.log.pending_generation(),
                st.generations.values().copied().max(),
                st.reserved,
            ];
            if let Some(at) = pending.into_iter().flatten().max() {
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
            // The build starts once the entry applies, which the lead
            // covers, and takes the build time on the slowest counted
            // replica. One that reports none counts its build lead, which
            // holds its engine's lead as well.
            let counted = self.counted(&st, now);
            let build = self
                .copies(&st, now)
                .into_iter()
                .filter(|(id, _, _)| counted.contains(id))
                .filter_map(|(_, _, own)| own.build_time.or(own.build_lead))
                .max()
                .unwrap_or(0);
            // Further than `AHEAD_MAX` past its stamp an entry would take
            // effect at the stamp, already passed: at most that far, where
            // a build longer than the lead leaves takes the generation
            // late rather than the entry.
            let most = stamp + crate::state_machine::AHEAD_MAX / b * b;
            let earliest = (stamp + lead + build).next_multiple_of(b).min(most);
            st.reserved = st.reserved.max(Some(earliest));
            earliest
        } else {
            stamp + lead
        };
        Ok(Stamp { stamp, earliest })
    }

    /// The lead a decision on an unresolved entry or a `Baseline` is
    /// stamped with: what the entries applied lately needed, up to
    /// [`crate::schedule::EXTRA_LEAD`] past the cap ([`Lead::wanted`]).
    /// Stamped with a lead the commit outlasts, a restamp or void is late
    /// again and decided in turn, and a `Baseline` lapses for the replica
    /// it names.
    pub fn decision_lead(&self) -> u64 {
        self.lock().lead.wanted()
    }

    /// A resync to start: this audio replica is diverged, not
    /// clock-unverified or excluded, no attempt runs and the last ended
    /// the backoff ago, and a committing donor reported within `w + D`,
    /// one that did within the last report period and tick first.
    /// Returns the attempt's epoch, the donor and its address. Without a
    /// donor it is checked for again on the next tick, and counted and
    /// logged with a backoff ([`crate::resync::Metrics::no_donor`]).
    pub fn resync_due(&self) -> Option<(u64, NodeId, String)> {
        self.audio.as_ref()?;
        let now = Instant::now();
        let mut st = self.lock();
        let st = &mut *st;
        let diverged = st.life.as_ref()?.state() == ReplicaState::Diverged;
        // A mismatch counts while the replica stays diverged or
        // resyncing, attempts and hand-overs included: once it is neither,
        // a later divergence starts its own.
        if !st.life.as_ref()?.state().diverged() {
            st.off_flows_since = None;
        }
        // A rebuild under way would install its core over the resync's.
        let rebuilding = matches!(st.rebuild, Rebuild::Running { .. });
        if !diverged || rebuilding || st.own.clock.unverified || st.judged.contains(&self.id) {
            return None;
        }
        // An input reading another flow than the applied state names, as
        // one does after holding a re-routing entry unresolved that
        // stands, makes every replay differ: a replay reads the engine's
        // inputs. The engine is rebuilt from product state instead, which
        // moves the input; a page that still differs then resyncs.
        // Checked as often as an attempt would start.
        // A route committed with its index still inside the lead reads as
        // another flow until its block is computed: rebuilt only once the
        // mismatch outlasts the longest lead an entry is stamped with. A
        // hand-over moves no input, so the mismatch counts on across one;
        // once an entry re-routed or detached an input since, it counts
        // anew, though the flows it left may equal those it began with.
        let lead = self.timing.max_apply_lead + crate::schedule::EXTRA_LEAD;
        if st.resync.due(now) {
            let state = self.log.product_state().2;
            let off = (self.audio.as_ref())
                .is_some_and(|audio| !state.flows.is_empty() && !audio.follows(&state));
            st.off_flows_since = match st.off_flows_since.take() {
                Some((since, at)) if off && at == st.flows_at => Some((since, at)),
                _ if off => Some((now, st.flows_at)),
                _ => None,
            };
            if st
                .off_flows_since
                .as_ref()
                .is_some_and(|(since, _)| *since + lead <= now)
            {
                st.off_flows_since = None;
                st.resync.rebuilt(now);
                tracing::warn!(
                    flows = ?state.flows,
                    "diverged with an input on another flow than the log names: rebuilt, not resynced"
                );
                self.begin_restart(st, self.log.applied());
                return None;
            }
        }
        let audio_voters = crate::rank::audio_voters(&st.members);
        let baseline = self.log.baseline().map(|b| b.replica);
        let candidates: Vec<(NodeId, Duration, &Own)> = self
            .copies(st, now)
            .into_iter()
            .filter(|(id, _, _)| {
                *id != self.id
                    && audio_voters.contains(id)
                    && !st.judged.contains(id)
                    && st.members.addrs.contains_key(id)
            })
            .collect();
        // A report up to `w + D` old may name a replica that withdrew
        // since, which refuses its core: one committing by a report of
        // the last period and tick comes first.
        let donor_within = |max_age: Duration| {
            let peers: Vec<(NodeId, bool, &Own)> = (candidates.iter())
                .map(|&(id, age, own)| (id, age > max_age, own))
                .collect();
            crate::resync::donor(baseline, &peers)
        };
        let donor = donor_within(2 * self.timing.report_period)
            .or_else(|| donor_within(self.timing.contact_window + self.timing.hold));
        let Some(donor) = donor else {
            if st.resync.no_donor(now) {
                let m = &st.resync.metrics;
                tracing::warn!(
                    no_donor = m.no_donor,
                    "diverged with no committing donor: retried every tick"
                );
            }
            return None;
        };
        let addr = st.members.addrs.get(&donor)?.clone();
        let epoch = st.resync.start(now)?;
        Some((epoch, donor, addr))
    }

    /// The donor's core for attempt `epoch` arrived: the replica turns
    /// `resync`. False when the attempt is no longer current or the
    /// replica no longer diverged, which gives the attempt up.
    pub fn resync_arrived(&self, epoch: u64) -> bool {
        let mut st = self.lock();
        let arrived = st.resync.current(epoch) && st.life.as_mut().is_some_and(Life::resyncing);
        if !arrived {
            st.resync.abandon(Instant::now());
        }
        arrived
    }

    /// Whether attempt `epoch` may still hand over: current, and the
    /// replica resyncing.
    pub fn resync_go(&self, epoch: u64) -> bool {
        let st = self.lock();
        st.resync.current(epoch)
            && st
                .life
                .as_ref()
                .is_some_and(|l| l.state() == ReplicaState::Resync)
    }

    /// Attempt `epoch` with `donor` ended. A hand-over whose replayed
    /// pages all equal the donor's reported ones leaves the replica to
    /// resume on its next matched page, holding the donor's unresolved
    /// entries as its own; anything else leaves it diverged, to try again.
    pub fn resync_ended(&self, epoch: u64, donor: NodeId, result: Result<Resynced, String>) {
        let Some(audio) = self.audio.clone() else {
            return;
        };
        let now = Instant::now();
        let mut st = self.lock();
        let st = &mut *st;
        let result = result.and_then(|r| {
            match crate::resync::mismatch(&r.pages, |o, q| st.pages.get(donor, o, q).copied()) {
                Some(q) => Err(format!("replayed page {q} differs from the donor's")),
                None => Ok(r),
            }
        });
        if !st
            .resync
            .end(epoch, result.is_ok(), self.timing.resync_budget, now)
        {
            return;
        }
        let Some(life) = st.life.as_mut() else { return };
        let r = match result {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(donor, error = %e, "a resync failed");
                life.resync_failed();
                return;
            }
        };
        life.handed_over(r.j);
        // The donor's unresolved entries are this replica's now: the
        // log's decision on them reaches both alike.
        let inherited: BTreeMap<u64, u64> = r.unresolved.iter().copied().collect();
        st.given.retain(|k, _| inherited.contains_key(&k.0));
        for (&index, &at) in &inherited {
            if st.given.keys().any(|k| k.0 == index) {
                continue;
            }
            if index & UNLISTED_BIT != 0 {
                st.given.insert((index, 0, 0), at);
                st.unlisted.entry(index).or_insert(st.known_committed);
                continue;
            }
            let decided = self.log.log_id_at(index).filter(|id| {
                self.log
                    .decided(index, id.term)
                    .is_none_or(|d| st.decisions.iter().any(|x| x.0 == d))
            });
            match decided {
                Some(id) => {
                    st.given.insert(key(id), at);
                }
                // Decided and acted on already, or purged with its
                // decision: resolved.
                None => audio.resolved(index),
            }
        }
    }

    /// The records of the entries applied after index `after`, for a
    /// resync's replay; [`Missed`] when one is no longer held.
    pub fn replay_entries(&self, after: u64) -> Result<Vec<(u64, Arc<Record>)>, Missed> {
        let applied = self.log.applied();
        let mut out = Vec::new();
        let whole = self.log.records(after, applied, &mut |index, record| {
            out.push((index, Arc::clone(record)));
        });
        if whole { Ok(out) } else { Err(Missed) }
    }

    /// The donor's side of a resync: the core, while this audio replica
    /// commits.
    pub fn copy_core(&self) -> Result<CoreReply, String> {
        let audio = self.audio.clone().ok_or("the witness holds no engine")?;
        let committing = self
            .lock()
            .life
            .as_ref()
            .is_some_and(|l| l.state() == ReplicaState::Committing);
        if !committing {
            return Err("not committing".to_owned());
        }
        audio.copy_core().ok_or_else(|| "no core copied".to_owned())
    }

    pub fn metrics(&self) -> Metrics {
        // Takes the lock itself.
        let members = self.lock().members.clone();
        let protocol_blocker = self.protocol_blocker(&members);
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
            protocol_gate_open: protocol_blocker.is_none(),
            protocol_blocker,
            config_hash: self.log.config_hash(),
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
            entries_filtered: st.entries_filtered,
            unresolved: c.unresolved,
            unresolved_refused: c.unresolved_refused,
            awaiting_overflow: c.awaiting_overflow,
            scheduled_dropped: c.scheduled_dropped,
            snapshot_installs: c.installs,
            purge_points: c.purge_points,
            contradicted: c.contradicted,
            late_rebuilds: c.late_rebuilds,
            estimate: self.estimate_in(&st, Instant::now()),
            counted: self.counted(&st, Instant::now()).into_iter().collect(),
            lead: st.lead.samples(),
            decision_lead: st.lead.wanted(),
            apply_lead_capped: st.lead.capped,
            apply_lead_raised_by: st.lead_raised_by,
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
            resync: st.resync.metrics,
            leader: st.leader.counts,
            replica_disagreement: st.leader.disagreement,
            clock: self.clock_status(&st, Instant::now()),
            clock_verdicts: st.clock.verdicts,
            verified: st.verifier.verified(),
            tai_offset_s: st.own.clock.tai_offset_s,
            clock_unverified: st.unverified.clone(),
            replica_clock_unverified: st
                .unverified_since
                .is_some_and(|t| since(Instant::now(), t) >= UNVERIFIED_DEBOUNCE),
            replica_tai_offset_zero: st.offset_zero.clone(),
            horizon_held: st.horizon.held(),
            clock_holds: c.clock_holds,
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
                if let Some((epoch, donor, addr)) = contact.resync_due() {
                    tokio::spawn(resync(shared.clone(), contact.clone(), epoch, donor, addr));
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

/// One resync attempt: the donor's core over the peer listener, then the
/// replay and hand-over off the async workers.
async fn resync(
    shared: Arc<Shared>,
    contact: Arc<Contact>,
    epoch: u64,
    donor: NodeId,
    addr: String,
) {
    let within = crate::resync::FETCH_WITHIN;
    let core = match shared
        .peers
        .call(Some(donor), &addr, PeerRequest::Core, within)
        .await
    {
        Ok(Ok(PeerResponse::Core(core))) => Ok(core),
        Ok(Ok(_)) => Err("an answer other than a core".to_owned()),
        Ok(Err(refused)) => Err(format!("refused: {refused:?}")),
        Err(e) => Err(e),
    };
    let core = match core {
        Ok(core) => core,
        Err(e) => {
            contact.resync_ended(epoch, donor, Err(format!("no core from {donor}: {e}")));
            return;
        }
    };
    let Some(audio) = contact.audio.clone() else {
        return;
    };
    if !contact.resync_arrived(epoch) {
        return;
    }
    let feed = Feed {
        contact: contact.clone(),
        epoch,
    };
    let result = tokio::task::spawn_blocking(move || audio.resync(&core, &feed))
        .await
        .unwrap_or_else(|e| Err(format!("the replay panicked: {e}")));
    contact.resync_ended(epoch, donor, result);
}

/// The log and the go-ahead a resync's replay reads.
struct Feed {
    contact: Arc<Contact>,
    epoch: u64,
}

impl Source for Feed {
    fn entries(&self, after: u64) -> Result<Vec<(u64, Arc<Record>)>, Missed> {
        self.contact.replay_entries(after)
    }

    fn go(&self) -> bool {
        self.contact.resync_go(self.epoch)
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
        fn tai_offset_s(&self) -> Option<i64> {
            Some(37)
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
        /// [`LogView::pending_generation`].
        generation: Option<u64>,
        /// Records by log index, read up to the applied index.
        records: BTreeMap<u64, Arc<Record>>,
        records_from: u64,
        /// The floor of the applied state.
        floor: u64,
        /// The applied index and floor an apply sets right after the next
        /// read of the applied index.
        apply_after_read: Option<(u64, u64)>,
        /// [`LogView::baseline`].
        baseline: Option<Baseline>,
        /// The flows of [`LogView::product_state`].
        flows: Vec<Option<String>>,
    }

    #[derive(Default)]
    struct Log(Mutex<LogInner>);

    impl Log {
        /// An entry, an `InputDown` at `at_index` that derives its
        /// record once applied.
        fn push(&self, term: u64, index: u64, at_index: Option<u64>) {
            let body = match at_index {
                Some(at_index) => Body::InputDown { slot: 0, at_index },
                None => Body::Commands(Vec::new()),
            };
            let mut log = self.0.lock().unwrap();
            let stamp = at_index.unwrap_or(0);
            log.log
                .insert(index, (id(term, index), Request { stamp, body }));
            if let Some(at_index) = at_index {
                let entries = vec![Entry::InputDown { slot: 0 }];
                let record = Record {
                    at_index,
                    entries,
                    generation: false,
                };
                log.records.insert(index, Arc::new(record));
            }
        }
    }

    impl LogView for Log {
        fn committed(&self) -> Option<StoredLogId> {
            self.0.lock().unwrap().committed
        }
        fn applied(&self) -> u64 {
            let mut log = self.0.lock().unwrap();
            let applied = log.applied;
            if let Some((index, floor)) = log.apply_after_read.take() {
                (log.applied, log.floor) = (index, floor);
            }
            applied
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
            self.0.lock().unwrap().baseline.clone()
        }
        fn disrupting(&self) -> Option<NodeId> {
            None
        }
        fn installs(&self) -> u64 {
            self.0.lock().unwrap().installs
        }
        fn pending_generation(&self) -> Option<u64> {
            self.0.lock().unwrap().generation
        }
        fn records(&self, after: u64, up_to: u64, f: &mut dyn FnMut(u64, &Arc<Record>)) -> bool {
            let log = self.0.lock().unwrap();
            if after < up_to {
                for (index, record) in log.records.range(after + 1..=up_to) {
                    f(*index, record);
                }
            }
            after >= log.records_from
        }
        fn records_from(&self) -> u64 {
            self.0.lock().unwrap().records_from
        }
        fn floor_before(&self, index: u64) -> u64 {
            let log = self.0.lock().unwrap();
            let mut floor = log.floor;
            let end = index.max(log.applied + 1);
            for (_, request) in log.log.range(log.applied + 1..end).map(|(_, e)| e) {
                if let Some(at) = crate::state_machine::floored(floor, request) {
                    floor = at;
                }
            }
            floor
        }
        fn applied_floor(&self) -> (u64, u64) {
            let log = self.0.lock().unwrap();
            (log.applied, log.floor)
        }
        fn pending_above(&self, floor: u64) -> usize {
            let log = self.0.lock().unwrap();
            let records = (log.records.values())
                .filter(|r| r.at_index > floor)
                .count();
            let mut raised = log.floor;
            let mut unapplied = 0;
            for (_, request) in log.log.range(log.applied + 1..).map(|(_, e)| e) {
                if let Some(at) = crate::state_machine::floored(raised, request) {
                    raised = at;
                    unapplied += usize::from(at > floor);
                }
            }
            records + unapplied
        }
        fn product_state(&self) -> (u64, u64, ProductState) {
            let log = self.0.lock().unwrap();
            let state = ProductState {
                limiters: Vec::new(),
                gains: Vec::new(),
                up: Vec::new(),
                flows: log.flows.clone(),
                outputs: Vec::new(),
                strips: Vec::new(),
                ducks: Vec::new(),
                masks: Vec::new(),
                pending: Vec::new(),
                last_id: log.applied,
            };
            (log.applied, log.floor, state)
        }
        fn config_hash(&self) -> Option<u64> {
            Some(1)
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
        requests: Mutex<Vec<(u64, u64, Vec<Entry>)>>,
        /// Where each append's entries lie in memory.
        handed_from: Mutex<Vec<usize>>,
        /// Whether a rebuild succeeds.
        rebuilds: std::sync::atomic::AtomicBool,
        /// A rebuild waits while set, as for the RT thread, 1 s at most.
        holding: std::sync::atomic::AtomicBool,
        /// Each rebuild's state's last log id and the index it installs
        /// at or after.
        rebuilt_with: Mutex<Vec<(u64, u64)>>,
        /// Whether it reports outputs, withdrawn: one, two with `second`.
        outputs: std::sync::atomic::AtomicBool,
        second: std::sync::atomic::AtomicBool,
        /// The outputs disabled, a bit per output.
        disabled: AtomicU64,
        /// The build time it reports, in samples; none at 0.
        build_time: AtomicU64,
        /// [`Audio::follows`].
        follows: std::sync::atomic::AtomicBool,
    }

    impl Engine {
        /// The outputs it reports.
        fn reported(&self) -> usize {
            match (
                self.outputs.load(Ordering::SeqCst),
                self.second.load(Ordering::SeqCst),
            ) {
                (false, _) => 0,
                (true, false) => 1,
                (true, true) => 2,
            }
        }
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
                    standing: vec![Standing::Withdrawn { since: 0 }; self.reported()],
                    build_time: Some(self.build_time.load(Ordering::SeqCst)).filter(|&t| t > 0),
                    ..Own::default()
                },
                status_changes_lost: 0,
                entries_refused: 0,
                entries_filtered: 0,
                realised: Vec::new(),
                enabled: (0..self.reported())
                    .map(|o| self.disabled.load(Ordering::SeqCst) & 1 << o == 0)
                    .collect(),
                generation_ready: true,
                clock_steps: 0,
                log_overflows: 0,
            }
        }
        fn fence(&self, end: u64) {
            self.fence.store(end, Ordering::SeqCst);
        }
        fn settled(&self, _: u64) -> bool {
            false
        }
        fn appended(&self, index: u64, at_index: u64, entries: &[Entry]) {
            self.calls.lock().unwrap().push(format!("appended {index}"));
            let call = (index, at_index, entries.to_vec());
            self.requests.lock().unwrap().push(call);
            let from = entries.as_ptr() as usize;
            self.handed_from.lock().unwrap().push(from);
        }
        fn resolved(&self, index: u64) {
            self.calls.lock().unwrap().push(format!("resolved {index}"));
        }
        fn drop_engine(&self) {
            self.calls.lock().unwrap().push("drop".into());
        }
        fn rebuild_engine(&self, state: &ProductState, at_least: u64) -> bool {
            let from = std::time::Instant::now();
            while self.holding.load(Ordering::SeqCst) && from.elapsed() < Duration::from_secs(1) {
                std::thread::sleep(Duration::from_millis(1));
            }
            let ok = self.rebuilds.load(Ordering::SeqCst);
            if ok {
                self.calls.lock().unwrap().push("rebuild".into());
                let call = (state.last_id, at_least);
                self.rebuilt_with.lock().unwrap().push(call);
            }
            ok
        }
        fn follows(&self, _state: &ProductState) -> bool {
            self.follows.load(Ordering::SeqCst)
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
        fn stop_at(&self, _: usize, _: u64) {}
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
            handed_from: Mutex::new(Vec::new()),
            rebuilds: std::sync::atomic::AtomicBool::new(true),
            holding: std::sync::atomic::AtomicBool::new(false),
            rebuilt_with: Mutex::new(Vec::new()),
            outputs: std::sync::atomic::AtomicBool::new(false),
            second: std::sync::atomic::AtomicBool::new(false),
            disabled: AtomicU64::new(0),
            build_time: AtomicU64::new(0),
            follows: std::sync::atomic::AtomicBool::new(true),
        });
        let contact = Contact::new(
            1,
            Timing::defaults(B),
            clock.clone(),
            Some(engine.clone()),
            log.clone(),
            100,
        );
        contact.verified_always();
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
        /// A tick, after which a rebuild it started has ended.
        fn tick(&self) -> Vec<(NodeId, String, Report)> {
            let reports = self.contact.tick(self.members.clone());
            self.contact.await_rebuild();
            reports
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

        /// A report from `from` naming product configuration `config`.
        fn config_report(&self, from: NodeId, config: Option<u64>) -> Report {
            // A later report than the one before.
            self.clock.add(Duration::from_millis(1));
            let mut r = self.report(from, None);
            r.own.config = config;
            r
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

    /// Writes stop while a voter or a learner reports another product
    /// configuration, and resume once it reports the set's or leaves the
    /// membership; a report without one names none. A learner of another
    /// configuration is not promoted.
    #[tokio::test(start_paused = true)]
    async fn a_member_of_another_configuration_stops_writes_until_it_matches_or_leaves() {
        let mut f = fixture(&[&[1, 2, 3]]);
        f.members.addrs.insert(9, "9".into());
        f.tick();
        assert_eq!(f.contact.config_differs(), None);
        assert!(!f.contact.learner_holds_set_config(9), "not heard from yet");
        f.contact.receive(f.config_report(2, None));
        assert_eq!(f.contact.config_differs(), None, "no hash names none");
        f.contact.receive(f.config_report(2, Some(5)));
        assert_eq!(f.contact.config_differs(), Some(2));
        f.contact.receive(f.config_report(2, Some(1)));
        assert_eq!(
            f.contact.config_differs(),
            None,
            "restarted under the set's"
        );
        f.contact.receive(f.config_report(9, Some(5)));
        assert_eq!(f.contact.config_differs(), Some(9), "a learner");
        assert!(!f.contact.learner_holds_set_config(9));
        assert!(!f.contact.learner_holds_set_config(2), "not a learner");
        f.contact.receive(f.config_report(9, Some(1)));
        assert!(f.contact.learner_holds_set_config(9));
        f.contact.receive(f.config_report(9, None));
        assert!(f.contact.learner_holds_set_config(9), "one recording none");
        f.contact.receive(f.config_report(9, Some(5)));
        f.contact.receive(f.config_report(3, Some(6)));
        f.members.addrs.remove(&9);
        f.members.configs = vec![[1, 2].into_iter().collect()];
        f.members.addrs.remove(&3);
        f.tick();
        assert_eq!(f.contact.config_differs(), None, "both left the membership");
        assert!(!f.contact.learner_holds_set_config(9));
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
        let known = |f: &Fixture| f.contact.metrics().known_committed;
        // Another term at index 5: not this log's entry.
        let mut r = f.report(2, None);
        r.committed = Some(id(2, 5));
        f.contact.receive(r);
        assert_eq!(known(&f), 0);
        let mut r = f.report(3, None);
        r.committed = Some(id(1, 4));
        f.contact.receive(r);
        assert_eq!(known(&f), 4);
        // Known committed, not applied: nothing reaches the engine.
        assert!(f.engine.calls().is_empty());
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
        assert_eq!(
            f.contact.metrics().known_committed,
            0,
            "another entry at index 5"
        );
        r.seq += 1;
        r.listed = vec![Listed {
            id: id(1, 5),
            at_index: at,
            baseline: false,
        }];
        f.contact.receive(r);
        assert_eq!(f.contact.metrics().known_committed, 5);
    }

    impl Fixture {
        /// Entry `index` of term 1, stamped at its own index.
        fn insert(&self, index: u64, body: Body) {
            let stamp = body.at_index().unwrap_or(0);
            self.log
                .0
                .lock()
                .unwrap()
                .log
                .insert(index, (id(1, index), Request { stamp, body }));
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
            generation: None,
        };
        f.insert(2, restamp);
        let record = Record {
            at_index: at + 96,
            entries: vec![Entry::InputDown { slot: 0 }],
            generation: false,
        };
        (f.log.0.lock().unwrap().records).insert(2, Arc::new(record.clone()));
        f.contact.log_changed();
        f.engine.calls();
        f.tick();
        assert_eq!(
            f.state(),
            Some(ReplicaState::Withdrawn),
            "not committed yet"
        );
        f.apply(2);
        f.contact.log_changed();
        let handed = f.engine.requests.lock().unwrap().last().cloned().unwrap();
        assert_eq!(
            handed,
            (2, record.at_index, record.entries),
            "what the restamp derived, at its index, once applied"
        );
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Diverged));
        assert_eq!(f.contact.metrics().contradicted, 1);
    }

    /// Diverges the fixture's replica as the restamp of an entry it
    /// applied at its own index does.
    fn diverge(f: &Fixture) {
        f.tick();
        let at = index_at(T0) + 400 * 48;
        f.log.push(1, 1, Some(at));
        f.apply(1);
        f.contact.log_changed();
        f.tick();
        f.insert(
            2,
            Body::Restamp {
                revision: 1,
                term: 1,
                at_index: at + 96,
                paths: Vec::new(),
                generation: None,
            },
        );
        let record = Record {
            at_index: at + 96,
            entries: vec![Entry::InputDown { slot: 0 }],
            generation: false,
        };
        (f.log.0.lock().unwrap().records).insert(2, Arc::new(record));
        f.apply(2);
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Diverged));
    }

    /// A report from `from` of a replica in `state`, `ago` before now.
    fn state_report(f: &Fixture, from: NodeId, state: ReplicaState, ago: Duration) -> Report {
        let mut r = f.report(from, None);
        r.sent_at = f.clock.get() - ago.as_nanos() as u64;
        r.own.state = Some(state);
        r
    }

    /// `/readyz`: a replica that writes nothing is not serving while no
    /// other replica commits, in contact or out of it, where it read
    /// ready by "nobody else commits"; one withdrawn before the set's
    /// first `Baseline` is, so the first replica of a set becomes ready,
    /// and one withdrawn after it or in `version_mismatch` is not. A sole
    /// voter always serves, a learner never, and one without outputs as
    /// before.
    #[tokio::test(start_paused = true)]
    async fn a_replica_writing_nothing_is_not_serving_though_nobody_commits() {
        let f = fixture(&[&[1]]);
        f.tick();
        f.tick();
        assert!(f.contact.serving(), "a sole voter");
        // A learner of a one-voter set, as one is while a set forms,
        // commits nothing.
        let f = fixture(&[&[2]]);
        f.tick();
        f.tick();
        assert!(!f.contact.serving(), "a learner beside a sole voter");
        // A learner of a set of three where no voter commits: in contact,
        // it would read ready by "nobody else commits".
        let f = fixture(&[&[2, 3, 4]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        for _ in 0..3 {
            for v in [2, 3, 4] {
                f.contact.receive(f.report(v, None));
            }
            f.tick();
        }
        assert!(f.contact.metrics().in_contact);
        assert!(!f.contact.serving(), "a learner, nobody commits");
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert!(f.contact.serving(), "withdrawn without outputs");
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert!(f.contact.metrics().in_contact);
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert!(
            f.contact.serving(),
            "withdrawn before its first commit, nobody else commits"
        );
        // Once a `Baseline` applied the set committed: a replica withdrawn
        // since, restarted or not, writes nothing.
        f.log.0.lock().unwrap().baseline = Some(Baseline {
            replica: 2,
            order: vec![2, 1, 3],
            at_index: index_at(T0),
            key: None,
        });
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert!(!f.contact.serving(), "withdrawn after a baseline");
        // With every output disabled it writes nothing anywhere, as a set
        // whose outputs a frontend disabled: it serves as one without
        // outputs does. Enabled again, it writes nothing it computes.
        f.engine.disabled.store(1, Ordering::SeqCst);
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert!(f.contact.serving(), "withdrawn, every output disabled");
        f.engine.disabled.store(0, Ordering::SeqCst);
        f.tick();
        assert!(!f.contact.serving(), "withdrawn, an output enabled again");
        // Of two outputs one enabled: it still writes nothing of it.
        f.engine.second.store(true, Ordering::SeqCst);
        f.engine.disabled.store(0b10, Ordering::SeqCst);
        f.tick();
        assert!(
            !f.contact.serving(),
            "withdrawn, one of two outputs enabled"
        );
        f.engine.disabled.store(0b11, Ordering::SeqCst);
        f.tick();
        assert!(f.contact.serving(), "withdrawn, both outputs disabled");
        // A replica whose key differs from the baseline's computes nothing.
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.contact.set_rollout(crate::rollout::Fixed::new(1, None));
        f.log.0.lock().unwrap().baseline = Some(Baseline {
            replica: 2,
            order: vec![2, 1, 3],
            at_index: index_at(T0),
            key: Some(2),
        });
        f.tick();
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert_eq!(f.state(), Some(ReplicaState::VersionMismatch));
        assert!(!f.contact.serving(), "version mismatch");
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        diverge(&f);
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.contact.receive(f.report(3, None));
            f.tick();
        }
        assert!(f.contact.metrics().in_contact);
        assert_eq!(f.state(), Some(ReplicaState::Diverged));
        assert!(!f.contact.serving(), "diverged, in contact");
        // A resync drops its engine's core, which then takes no entry, so
        // the outputs it reports disabled are not known to be the log's.
        f.engine.disabled.store(1, Ordering::SeqCst);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Diverged));
        assert!(!f.contact.serving(), "diverged, every output disabled");
        f.engine.disabled.store(0, Ordering::SeqCst);
        // Out of contact the term follows the state too.
        tokio::time::advance(Duration::from_millis(50)).await;
        f.clock.add(Duration::from_millis(50));
        f.tick();
        assert!(!f.contact.metrics().in_contact);
        assert!(!f.contact.serving(), "diverged, out of contact");
    }

    /// A diverged replica asks a donor committing by a report of the last
    /// period first, not one whose committing report is older though
    /// within `w + D`; with no committing donor at all it keeps checking,
    /// counted and logged with a backoff, rather than going silent.
    #[tokio::test(start_paused = true)]
    async fn a_resync_prefers_a_fresh_donor_and_retries_without_one() {
        let f = fixture(&[&[1, 2, 3]]);
        diverge(&f);
        let attempts = |f: &Fixture| f.contact.metrics().resync;
        assert_eq!(f.contact.resync_due(), None, "nobody commits");
        let m = attempts(&f);
        assert_eq!((m.no_donor, m.attempts), (1, 0), "{m:?}");
        assert_eq!(f.contact.resync_due(), None);
        assert_eq!(attempts(&f).no_donor, 1, "counted within the backoff once");
        tokio::time::advance(crate::resync::RETRY).await;
        assert_eq!(f.contact.resync_due(), None);
        assert_eq!(attempts(&f).no_donor, 2, "counted again after the backoff");
        // 2 reported committing 12 ms ago, 3 just now.
        f.contact.receive(state_report(
            &f,
            2,
            ReplicaState::Committing,
            Duration::ZERO,
        ));
        tokio::time::advance(Duration::from_millis(12)).await;
        f.clock.add(Duration::from_millis(12));
        f.contact.receive(state_report(
            &f,
            3,
            ReplicaState::Committing,
            Duration::ZERO,
        ));
        let (_, donor, _) = f.contact.resync_due().expect("an attempt");
        assert_eq!(donor, 3, "the fresh donor");
        assert_eq!(attempts(&f).attempts, 1);
    }

    /// A fresh donor without a peer address is no donor: the one
    /// committing by an older report within `w + D` is asked instead.
    #[tokio::test(start_paused = true)]
    async fn a_fresh_donor_without_an_address_leaves_the_older_one() {
        let mut f = fixture(&[&[1, 2, 3]]);
        f.members.addrs.remove(&3);
        diverge(&f);
        f.contact.receive(state_report(
            &f,
            2,
            ReplicaState::Committing,
            Duration::ZERO,
        ));
        tokio::time::advance(Duration::from_millis(12)).await;
        f.clock.add(Duration::from_millis(12));
        f.contact.receive(state_report(
            &f,
            3,
            ReplicaState::Committing,
            Duration::ZERO,
        ));
        let (_, donor, _) = f.contact.resync_due().expect("an attempt");
        assert_eq!(donor, 2);
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
                at_index: None,
                generation: None,
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
        f.log.0.lock().unwrap().floor = 4800;
        f.tick();
        assert!(f.engine.calls().contains(&"rebuild".to_owned()));
        assert_eq!(
            *f.engine.rebuilt_with.lock().unwrap(),
            [(10, 4800)],
            "the applied state, at its floor"
        );
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    }

    /// A rebuild waits for the RT thread for up to the lead: contact goes
    /// on ticking and reporting meanwhile, and takes the rebuilt core on a
    /// tick after it ended.
    #[tokio::test(start_paused = true)]
    async fn contact_ticks_on_while_a_rebuild_waits() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        f.engine.holding.store(true, Ordering::SeqCst);
        f.apply(10);
        f.log.0.lock().unwrap().installs += 1;
        f.contact.log_changed();
        for _ in 0..5 {
            let from = std::time::Instant::now();
            let reports = f.contact.tick(f.members.clone());
            assert!(
                from.elapsed() < Duration::from_millis(500),
                "a tick waited for the rebuild"
            );
            assert_eq!(reports.len(), 2);
            assert_eq!(f.state(), Some(ReplicaState::Starting));
        }
        f.engine.holding.store(false, Ordering::SeqCst);
        f.contact.await_rebuild();
        f.contact.tick(f.members.clone());
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        let calls = f.engine.calls();
        let rebuilds = calls.iter().filter(|c| *c == "rebuild").count();
        assert_eq!(rebuilds, 1, "{calls:?}");
    }

    /// Entry `index`, appended and applied `late` past its apply deadline
    /// or else in time by a block, on a tick. A late entry's index lies
    /// below the floor, which raises it to the last entry's, one block
    /// past that entry's deadline at most: 5 ms passed since.
    fn take(f: &Fixture, index: u64, late: bool) {
        f.clock.add(Duration::from_millis(5));
        let guard = Timing::defaults(B).apply_guard_ns();
        let at = if late {
            index_at(f.clock.get()) - 10 * B
        } else {
            index_at(f.clock.get() + guard) + B
        };
        f.log.push(1, index, Some(at));
        f.contact.log_changed();
        f.apply(index);
        f.contact.log_changed();
        f.contact.tick(f.members.clone());
    }

    /// A report from voter 2 naming `index` committed.
    fn committed_at(f: &Fixture, index: u64) {
        f.clock.add(Duration::from_millis(1));
        let mut r = f.report(2, None);
        r.committed = Some(id(1, index));
        f.contact.receive(r);
    }

    /// A replica that took an entry in time past the commit the voters
    /// named and then takes every entry past its deadline, as a log at the lead cap with
    /// a slow disk does, is not catching up: it holds them unresolved and
    /// keeps its core. A rebuild past each would leave it without a core
    /// while the lateness lasts. Once entries come in time again they are
    /// handed.
    #[tokio::test(start_paused = true)]
    async fn a_replica_late_after_catching_up_keeps_its_core() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        take(&f, 1, false);
        committed_at(&f, 1);
        take(&f, 2, false);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert!(!f.contact.lock().catching_up);
        f.engine.calls();
        for index in 3..=21 {
            take(&f, index, true);
            f.tick();
            assert_eq!(f.state(), Some(ReplicaState::Withdrawn), "entry {index}");
        }
        let calls = f.engine.calls();
        assert!(
            !calls.iter().any(|c| c == "drop" || c == "rebuild"),
            "{calls:?}"
        );
        assert_eq!(f.contact.metrics().late_rebuilds, 0);
        assert_eq!(f.contact.lock().given.len(), 19, "held unresolved");
        take(&f, 22, false);
        f.tick();
        assert!(f.engine.calls().contains(&"appended 22".to_owned()));
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    }

    /// A diverged replica commits nothing either: one that takes an entry
    /// past its deadline while catching up, as a restarted one diverged
    /// before it applied what was written while it was away, is rebuilt
    /// past it. Held unresolved, an entry that stands is never decided,
    /// and a `Route` held so leaves an input on a flow the log no longer
    /// names, which a resync's replay reads.
    #[tokio::test(start_paused = true)]
    async fn a_diverged_replica_catching_up_is_rebuilt_past_a_late_entry() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        diverge(&f);
        assert!(f.contact.lock().catching_up);
        // Past the floor the diverging entries left.
        f.clock.add(Duration::from_secs(1));
        take(&f, 3, true);
        f.tick();
        assert_eq!(f.contact.metrics().late_rebuilds, 1);
        assert_ne!(f.state(), Some(ReplicaState::Diverged), "restarted");
        assert!(f.contact.lock().given.is_empty(), "rebuilt past, not held");
    }

    /// A rebuild, on a replica committing nothing, from the state an
    /// entry held unresolved applied in holds it: the entry is rebuilt
    /// past rather than given to the new core again, where it would stay
    /// unresolved for good once it stands. One the state does not hold
    /// yet is given again.
    #[tokio::test(start_paused = true)]
    async fn a_rebuild_takes_the_held_entries_its_state_applied() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        take(&f, 1, false);
        committed_at(&f, 1);
        take(&f, 2, false);
        f.tick();
        assert!(!f.contact.lock().catching_up);
        for index in 3..=6 {
            take(&f, index, true);
            f.tick();
        }
        assert_eq!(f.contact.lock().given.len(), 4, "held unresolved");
        // Entry 7 is held unresolved before it applies.
        f.clock.add(Duration::from_millis(5));
        let at = index_at(f.clock.get()) - 10 * B;
        f.log.push(1, 7, Some(at));
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.contact.lock().given.len(), 5);
        // A rebuild from the state applied up to 6, as a diverged replica
        // on another flow than its state names starts.
        {
            let mut st = f.contact.lock();
            f.contact.begin_restart(&mut st, 6);
        }
        f.tick();
        f.contact.await_rebuild();
        f.engine.calls();
        f.tick();
        let st = f.contact.lock();
        let given: Vec<u64> = st.given.keys().map(|k| k.0).collect();
        assert_eq!(given, [7], "the applied ones rebuilt past");
        drop(st);
        let calls = f.engine.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("unresolved 3")),
            "{calls:?}"
        );
    }

    /// An entry held unresolved that applies without a record is released,
    /// and a decision the log appends on it after all, as for an entry
    /// every replica took late, neither resolves nor contradicts the
    /// replica: it is rebuilt past.
    #[tokio::test(start_paused = true)]
    async fn a_decision_on_a_released_entry_without_a_record_contradicts_nothing() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        take(&f, 1, false);
        committed_at(&f, 1);
        take(&f, 2, false);
        f.tick();
        assert!(!f.contact.lock().catching_up);
        // Entry 3 late, deriving no record.
        f.clock.add(Duration::from_millis(5));
        let at = index_at(f.clock.get()) - 10 * B;
        f.insert(
            3,
            Body::InputDown {
                slot: 0,
                at_index: at,
            },
        );
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.contact.lock().given.len(), 1, "held unresolved");
        f.apply(3);
        f.contact.log_changed();
        f.tick();
        assert!(f.contact.lock().given.is_empty(), "released");
        assert!(f.engine.calls().contains(&"resolved 3".to_owned()));
        // The log restamps it after all.
        f.insert(
            4,
            Body::Restamp {
                revision: 3,
                term: 1,
                at_index: at + 960,
                paths: Vec::new(),
                generation: None,
            },
        );
        f.apply(4);
        f.contact.log_changed();
        f.tick();
        f.tick();
        assert_eq!(f.contact.metrics().contradicted, 0);
        assert_ne!(f.state(), Some(ReplicaState::Diverged));
    }

    /// A replica about to commit, retargeted from `version_mismatch`, gives
    /// its rebuilt core the entries it holds unresolved again, those its
    /// state applied included, as its peers hold them: the log may still
    /// decide them. Only a replica committing nothing rebuilds past them.
    #[tokio::test(start_paused = true)]
    async fn a_retargeted_core_holds_the_entries_its_state_applied() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.contact.set_rollout(crate::rollout::Fixed::new(1, None));
        let baseline = |key| Baseline {
            replica: 2,
            order: vec![2, 1, 3],
            at_index: index_at(T0),
            key: Some(key),
        };
        f.log.0.lock().unwrap().baseline = Some(baseline(2));
        f.tick();
        take(&f, 1, false);
        committed_at(&f, 1);
        take(&f, 2, false);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::VersionMismatch));
        take(&f, 3, true);
        f.tick();
        assert_eq!(f.contact.lock().given.len(), 1, "held unresolved");
        f.engine.calls();
        // The baseline's key is its own again: retargeted from the state
        // applied up to 3.
        f.log.0.lock().unwrap().baseline = Some(baseline(1));
        f.tick();
        f.contact.await_rebuild();
        f.tick();
        f.tick();
        let calls = f.engine.calls();
        assert!(calls.contains(&"rebuild".to_owned()), "{calls:?}");
        assert!(
            calls.iter().any(|c| c.starts_with("unresolved 3 ")),
            "given to the new core again: {calls:?}"
        );
        assert_eq!(f.contact.lock().given.len(), 1);
    }

    /// A diverged replica whose input reads another flow than its applied
    /// state names, as one does while a committed re-route waits for its
    /// block inside the lead, resyncs as usual; it is rebuilt from product
    /// state instead only once the mismatch outlasts the longest lead.
    #[tokio::test(start_paused = true)]
    async fn a_flow_mismatch_rebuilds_only_once_it_outlasts_the_lead() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.log.0.lock().unwrap().flows = vec![Some("a".to_owned())];
        diverge(&f);
        f.engine.follows.store(false, Ordering::SeqCst);
        let committing = |f: &Fixture| {
            f.contact
                .receive(state_report(f, 2, ReplicaState::Committing, Duration::ZERO));
        };
        committing(&f);
        let rebuilt = |f: &Fixture| f.contact.metrics().resync.rebuilt_for_flows;
        assert!(f.contact.resync_due().is_some(), "resynced at first");
        assert_eq!(rebuilt(&f), 0);
        let lead = Timing::defaults(B).max_apply_lead + crate::schedule::EXTRA_LEAD;
        let wait = |f: &Fixture| f.contact.metrics().resync.backoff;
        let failed = |f: &Fixture, epoch| {
            f.contact
                .resync_ended(epoch, 2, Err("a replayed page differs".into()));
        };
        failed(&f, 1);
        let first = wait(&f);
        assert!(first < lead, "{first:?}");
        tokio::time::advance(first).await;
        f.clock.add(first);
        committing(&f);
        assert!(f.contact.resync_due().is_some(), "within the lead");
        assert_eq!(rebuilt(&f), 0);
        failed(&f, 2);
        let second = wait(&f);
        assert!(first + second >= lead);
        tokio::time::advance(second).await;
        f.clock.add(second);
        committing(&f);
        assert_eq!(f.contact.resync_due(), None, "rebuilt instead");
        assert_eq!(rebuilt(&f), 1);
    }

    /// A diverged replica on another flow resyncs, hands over and
    /// diverges again `lead` plus the backoff later, its input still on
    /// another flow; whether its next check rebuilds it from product state.
    /// In between it leaves `diverged` and `resync` for `withdrawn` once
    /// when `recovered`, and entries re-route its input to each of
    /// `reroutes` in turn.
    async fn rebuilt_after_a_hand_over(recovered: bool, reroutes: &[&str]) -> bool {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.log.0.lock().unwrap().flows = vec![Some("a".to_owned())];
        diverge(&f);
        f.engine.follows.store(false, Ordering::SeqCst);
        let committing = |f: &Fixture| {
            f.contact
                .receive(state_report(f, 2, ReplicaState::Committing, Duration::ZERO));
        };
        committing(&f);
        let (epoch, donor, _) = f.contact.resync_due().expect("resynced at first");
        assert!(f.contact.resync_arrived(epoch));
        f.contact.resync_ended(
            epoch,
            donor,
            Ok(Resynced {
                j: 1,
                pages: Vec::new(),
                unresolved: Vec::new(),
            }),
        );
        assert_eq!(f.contact.metrics().resync.handovers, 1);
        let audio: Arc<dyn Audio> = f.engine.clone();
        if recovered {
            f.contact
                .lock()
                .life
                .as_mut()
                .unwrap()
                .restart(audio.as_ref());
            f.tick();
            assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
            assert_eq!(f.contact.resync_due(), None);
        }
        let guard = Timing::defaults(B).apply_guard_ns();
        for (n, flow) in reroutes.iter().enumerate() {
            let index = 3 + n as u64;
            let at = index_at(f.clock.get() + guard) + B;
            f.log.push(1, index, Some(at));
            let record = Record {
                at_index: at,
                entries: vec![Entry::Route {
                    slot: 0,
                    flow_id: (*flow).to_owned(),
                }],
                generation: true,
            };
            (f.log.0.lock().unwrap().records).insert(index, Arc::new(record));
            f.log.0.lock().unwrap().flows = vec![Some((*flow).to_owned())];
            f.apply(index);
            f.contact.log_changed();
            f.tick();
        }
        let lead = Timing::defaults(B).max_apply_lead + crate::schedule::EXTRA_LEAD;
        let later = lead + f.contact.metrics().resync.backoff;
        tokio::time::advance(later).await;
        f.clock.add(later);
        // Diverged again on the first page after the hand-over.
        {
            let mut st = f.contact.lock();
            let life = st.life.as_mut().unwrap();
            life.contradicted(audio.as_ref());
            assert_eq!(life.state(), ReplicaState::Diverged);
        }
        committing(&f);
        let resynced = f.contact.resync_due().is_some();
        let rebuilt = f.contact.metrics().resync.rebuilt_for_flows;
        assert_eq!(resynced, rebuilt == 0);
        rebuilt == 1
    }

    /// A hand-over moves no input: a replay reads the engine's inputs. A
    /// replica on another flow before it is still on it after it, so the
    /// mismatch counts on and the replica is rebuilt once it outlasts the
    /// lead. Counted anew at each hand-over, every check would find a
    /// fresh mismatch and resync, which hands over and diverges again.
    #[tokio::test(start_paused = true)]
    async fn a_flow_mismatch_counts_on_across_a_hand_over() {
        assert!(rebuilt_after_a_hand_over(false, &[]).await);
    }

    /// Once an entry re-routed the input since, as one committed with its
    /// block still inside the lead, the mismatch counts anew: the replica
    /// resyncs at its first check. So it does after a re-route away and
    /// back, though the flows the log names are those it began with.
    #[tokio::test(start_paused = true)]
    async fn a_flow_mismatch_counts_anew_after_a_re_route() {
        assert!(!rebuilt_after_a_hand_over(false, &["b"]).await);
        assert!(!rebuilt_after_a_hand_over(false, &["b", "a"]).await);
    }

    /// A replica that left `diverged` and `resync` in between counts a
    /// later divergence's mismatch anew, on the same flows: it resyncs at
    /// its first check.
    #[tokio::test(start_paused = true)]
    async fn a_flow_mismatch_counts_anew_after_the_replica_recovered() {
        assert!(!rebuilt_after_a_hand_over(true, &[]).await);
    }

    /// A replica catching up past entries' deadlines is rebuilt past them
    /// one rebuild at a time: what it takes late while a rebuild runs is
    /// folded into one rebuild after it, not into a restart that makes
    /// the running one stale. Once entries come in time it holds nothing
    /// unresolved and is withdrawn with its core.
    #[tokio::test(start_paused = true)]
    async fn a_replica_catching_up_is_rebuilt_past_late_entries_once_per_rebuild() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        f.engine.holding.store(true, Ordering::SeqCst);
        for index in 1..=10 {
            take(&f, index, true);
            assert_eq!(f.state(), Some(ReplicaState::Starting), "entry {index}");
        }
        assert_eq!(f.contact.metrics().late_rebuilds, 1);
        assert_eq!(f.contact.lock().late_given.len(), 9, "held for the next");
        f.engine.holding.store(false, Ordering::SeqCst);
        f.contact.await_rebuild();
        f.tick();
        assert_eq!(f.contact.metrics().late_rebuilds, 2, "folded once it ended");
        assert!(f.contact.lock().given.is_empty());
        committed_at(&f, 10);
        take(&f, 11, false);
        f.tick();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert_eq!(f.contact.metrics().late_rebuilds, 2);
        let st = f.contact.lock();
        assert!(st.given.is_empty() && st.late_given.is_empty() && !st.catching_up);
    }

    /// An entry taken in time while catching up, as one stamped with a
    /// generation's build lead on top of the lead can be, does not end the
    /// catch-up: the entries after it up to the commit the voters named
    /// were written before the replica could take them, and those it
    /// takes past their deadlines are rebuilt past, not held unresolved.
    /// One in time past that commit ends it.
    #[tokio::test(start_paused = true)]
    async fn an_entry_in_time_ends_no_catch_up_before_the_named_commit() {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(true, Ordering::SeqCst);
        f.tick();
        committed_at(&f, 5);
        take(&f, 1, false);
        for index in 2..=5 {
            take(&f, index, true);
        }
        f.contact.await_rebuild();
        f.tick();
        f.tick();
        let rebuilds = f.contact.metrics().late_rebuilds;
        assert!(rebuilds >= 1, "rebuilt past 2..=5");
        let st = f.contact.lock();
        assert!(
            st.given.is_empty() && st.late_given.is_empty(),
            "{:?}",
            st.given
        );
        drop(st);
        for _ in 0..3 {
            f.tick();
        }
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        take(&f, 6, false);
        assert!(
            !f.contact.lock().catching_up,
            "in time past the named commit"
        );
        take(&f, 7, true);
        f.tick();
        assert_eq!(
            f.contact.metrics().late_rebuilds,
            rebuilds,
            "7 held unresolved"
        );
        assert_eq!(f.contact.lock().given.len(), 1);
    }

    /// What an entry derived reaches the engine as it applies, not as it
    /// is appended or learned committed, at the index it derived; one
    /// applied at or past its apply deadline does not, and goes
    /// unresolved, however late the deadline passed.
    #[tokio::test(start_paused = true)]
    async fn a_record_is_handed_as_it_applies_unless_past_its_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 400 * B;
        f.log.push(1, 1, Some(at));
        f.log.push(1, 2, Some(at + B));
        f.contact.log_changed();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 2));
        f.contact.receive(r);
        assert_eq!(f.contact.metrics().known_committed, 2);
        assert!(f.engine.requests.lock().unwrap().is_empty(), "not applied");
        f.apply(1);
        f.contact.log_changed();
        let handed = f.engine.requests.lock().unwrap().clone();
        assert_eq!(handed, [(1, at, vec![Entry::InputDown { slot: 0 }])]);
        assert_eq!(f.engine.calls(), ["appended 1", "committed 1"]);
        // Still awaited as contact looks, past its deadline as it hands.
        f.clock.set(timing.apply_deadline(at + B) - 1);
        f.clock.step.store(1, Ordering::SeqCst);
        f.apply(2);
        f.contact.log_changed();
        assert_eq!(f.engine.requests.lock().unwrap().len(), 1, "entry 2 handed");
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved 2 at {}", at + B), "committed 2".into()]
        );
    }

    /// An applied entry's record gone before contact handed it leaves the
    /// engine without it: the replica drops its core and is rebuilt.
    #[tokio::test(start_paused = true)]
    async fn a_record_gone_before_it_was_handed_rebuilds_the_engine() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let at = index_at(T0) + 400 * B;
        f.log.push(1, 1, Some(at));
        f.contact.log_changed();
        f.engine.calls();
        f.log.0.lock().unwrap().records_from = 1;
        f.apply(1);
        f.contact.log_changed();
        assert!(f.engine.calls().contains(&"drop".to_owned()));
        assert!(f.engine.requests.lock().unwrap().is_empty());
        f.tick();
        assert!(f.engine.calls().contains(&"rebuild".to_owned()));
        assert_eq!(*f.engine.rebuilt_with.lock().unwrap(), [(1, 0)]);
    }

    /// A resync's replay reads the records of the entries applied after
    /// the donor core's last log id, and fails over a gap: a record
    /// after it this replica no longer holds.
    #[tokio::test(start_paused = true)]
    async fn a_replay_from_before_the_records_held_is_missed() {
        let f = fixture(&[&[1, 2, 3]]);
        let at = index_at(T0) + 400 * B;
        for index in 1..=3 {
            f.log.push(1, index, Some(at + index * B));
        }
        f.apply(3);
        f.log.0.lock().unwrap().records_from = 1;
        assert_eq!(f.contact.replay_entries(0), Err(Missed));
        let replayed: Vec<(u64, u64)> = (f.contact.replay_entries(1).expect("held"))
            .iter()
            .map(|(i, r)| (*i, r.at_index))
            .collect();
        assert_eq!(replayed, [(2, at + 2 * B), (3, at + 3 * B)]);
    }

    /// A record reaches the engine, and a resync's replay, as the state
    /// machine holds it: its entries are shared, not copied under the
    /// store lock, so taking a recall of every coefficient at the largest
    /// shape costs no more there than taking one gain.
    #[tokio::test(start_paused = true)]
    async fn a_record_is_handed_and_replayed_without_copying_its_entries() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let at = index_at(T0) + 400 * B;
        f.log.push(1, 1, Some(at));
        let held = Arc::clone(&f.log.0.lock().unwrap().records[&1]);
        f.contact.log_changed();
        f.apply(1);
        f.contact.log_changed();
        let handed = f.engine.handed_from.lock().unwrap().clone();
        assert_eq!(handed, [held.entries.as_ptr() as usize]);
        let replayed = f.contact.replay_entries(0).expect("held");
        assert!(Arc::ptr_eq(&replayed[0].1, &held));
    }

    /// A lease expiry from an older leader, which stamps no index, takes
    /// effect [`crate::state_machine::FALLBACK_LEAD`] past its stamp: its
    /// apply deadline has not passed when it applies promptly, so its
    /// record reaches the engine rather than going unresolved, which no
    /// decision could settle for an entry without an index.
    #[tokio::test(start_paused = true)]
    async fn an_unstamped_lease_expiry_applied_promptly_is_handed() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let stamp = index_at(T0).next_multiple_of(B);
        let expiry = Request {
            stamp,
            body: Body::LeaseExpiry {
                session: crate::state_machine::tests::origin("s").key(),
                renewed: 0,
                at_index: None,
            },
        };
        let at = stamp + crate::state_machine::FALLBACK_LEAD;
        {
            let mut log = f.log.0.lock().unwrap();
            log.log.insert(1, (id(1, 1), expiry));
            let record = Record {
                at_index: at,
                entries: vec![Entry::InputDown { slot: 0 }],
                generation: false,
            };
            log.records.insert(1, Arc::new(record));
        }
        f.contact.log_changed();
        f.clock.add(Duration::from_millis(5));
        f.apply(1);
        f.contact.log_changed();
        let calls = f.engine.calls();
        assert_eq!(calls, ["appended 1", "committed 1"], "{calls:?}");
    }

    /// An apply landing between two reads of the applied state: contact
    /// takes the applied index and the floor after it in one, so the
    /// entries after that index are listed at the floor their apply
    /// derives at, not one raised by entries it then re-walks.
    #[tokio::test(start_paused = true)]
    async fn contact_reads_the_applied_index_and_its_floor_at_once() {
        let f = fixture(&[&[1, 2, 3]]);
        for (index, at) in [(1, 10 * B), (2, 20 * B), (3, 30 * B)] {
            f.log.push(1, index, Some(at));
        }
        {
            let mut log = f.log.0.lock().unwrap();
            (log.applied, log.floor) = (1, 10 * B);
            log.apply_after_read = Some((3, 30 * B));
        }
        let contact = Contact::new(
            1,
            Timing::defaults(B),
            f.clock.clone(),
            Some(f.engine.clone()),
            f.log.clone(),
            1,
        );
        contact.log_changed();
        assert_eq!(contact.lock().scheduled.get(&2).map(|s| s.1), Some(20 * B));
    }

    /// A log purged past what contact took: the entries purged all
    /// applied, so the floor rises over them, and the next entry is
    /// listed and awaited at the index its apply derives at, not at its
    /// own lower one.
    #[tokio::test(start_paused = true)]
    async fn the_floor_rises_over_entries_purged_before_contact_took_them() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let base = index_at(T0).next_multiple_of(B);
        f.log.push(1, 3, Some(base + 20 * B));
        {
            let mut log = f.log.0.lock().unwrap();
            (log.applied, log.floor) = (2, base + 40 * B);
            log.committed = Some(id(1, 2));
        }
        f.contact.log_changed();
        assert_eq!(
            f.contact.lock().scheduled.get(&3).map(|s| s.1),
            Some(base + 40 * B)
        );
    }

    /// A record of an entry purged from the log since it applied is
    /// handed whatever its deadline: no decision names it by an id this
    /// replica holds, so given unresolved it would degrade blocks until
    /// an install.
    #[tokio::test(start_paused = true)]
    async fn a_purged_applied_record_is_handed_past_its_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let at = index_at(T0) + 40 * B;
        f.log.push(1, 1, Some(at));
        f.log.push(1, 2, Some(at + 400 * B));
        f.log.0.lock().unwrap().log.remove(&1);
        f.clock.set(Timing::defaults(B).apply_deadline(at) + 1);
        f.engine.calls();
        f.apply(2);
        f.contact.log_changed();
        let calls = f.engine.calls();
        assert_eq!(
            calls,
            ["appended 1", "appended 2", "committed 2"],
            "{calls:?}"
        );
    }

    /// The leader stamps nothing while [`FUTURE_LIST`] records of applied
    /// entries take effect ahead, which every engine holds until then,
    /// whatever entry derived them.
    #[tokio::test(start_paused = true)]
    async fn applied_records_still_ahead_fill_the_future_list() {
        let f = fixture(&[&[1]]);
        f.tick();
        let ahead = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
        {
            let mut log = f.log.0.lock().unwrap();
            for index in 1..=FUTURE_LIST as u64 {
                let record = Record {
                    at_index: ahead,
                    entries: vec![Entry::Gain {
                        input: 0,
                        output: 0,
                        gain: 0.5,
                    }],
                    generation: false,
                };
                log.records.insert(index, Arc::new(record));
            }
        }
        assert_eq!(f.contact.stamp(false), Err(StampRefusal::FutureListFull));
        f.log.0.lock().unwrap().records.pop_first();
        assert!(f.contact.stamp(false).is_ok());
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

    fn restamp_of(revision: u64, at_index: u64, paths: &[&str]) -> Body {
        Body::Restamp {
            revision,
            term: 1,
            at_index,
            paths: paths.iter().map(|p| (*p).to_owned()).collect(),
            generation: None,
        }
    }

    /// A restamp never undoes a later committed value: a restamped slot
    /// change that a later committed entry on the same slot overtook is
    /// superseded and voided. A product entry never is, whatever a later
    /// one sets: its restamp re-asserts the state at its own place, which
    /// holds every entry before it.
    #[tokio::test(start_paused = true)]
    async fn only_an_engine_body_a_later_entry_overtook_is_superseded() {
        use crate::state_machine::tests::command;
        let f = fixture(&[&[1, 2, 3]]);
        let at = index_at(T0) + 400 * 48;
        f.log.push(1, 1, Some(at));
        f.insert(2, restamp_of(1, at + 96, &[]));
        assert!(!f.contact.superseded(2, 2), "nothing later");
        f.insert(
            3,
            Body::InputUp {
                slot: 0,
                at_index: at + 480,
            },
        );
        assert!(
            f.contact.superseded(2, 3),
            "the restamp's original entry, overtaken on its slot"
        );
        assert!(!f.contact.superseded(2, 2), "uncommitted");
        let commands = |cs: &[(&str, &str)]| {
            Body::Commands(
                cs.iter()
                    .map(|(id, body)| {
                        let mut c = command("s", id, body);
                        c.at_index = Some(at);
                        c
                    })
                    .collect(),
            )
        };
        f.insert(10, commands(&[("a", "gain=1@1"), ("b", "pan=1@1")]));
        f.insert(11, commands(&[("c", "gain=2@2"), ("d", "pan=2@2")]));
        assert!(!f.contact.superseded(10, 11), "a product entry");
    }

    /// A void naming an id at an index whose term this replica no longer
    /// knows, purged from its own log, is no decision on an entry it
    /// applied: it does not diverge.
    #[tokio::test(start_paused = true)]
    async fn a_void_on_an_index_this_log_purged_does_not_diverge() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        f.log.push(1, 4, None);
        f.insert(
            5,
            Body::Void {
                revision: 3,
                term: 7,
                at_index: None,
                generation: None,
            },
        );
        f.apply(5);
        f.contact.log_changed();
        f.tick();
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert_eq!(f.contact.metrics().contradicted, 0);
    }

    /// One purge-point verdict leads to exactly one rebuild: the entries
    /// it supersedes are no longer held unresolved, so they are neither
    /// reported again nor given to the rebuilt engine.
    #[tokio::test(start_paused = true)]
    async fn one_purge_point_verdict_rebuilds_once_and_clears_what_it_supersedes() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        f.log.push(1, 1, Some(at));
        f.contact.log_changed();
        f.clock.set(timing.apply_deadline(at));
        let reported = |f: &Fixture| -> Vec<u64> {
            f.tick()[0]
                .2
                .own
                .unresolved
                .iter()
                .map(|l| l.id.index)
                .collect()
        };
        assert_eq!(reported(&f), [1]);
        f.insert(
            2,
            Body::PurgePoint {
                replica: 1,
                index: 1,
            },
        );
        f.apply(2);
        f.engine.calls();
        f.contact.log_changed();
        for _ in 0..20 {
            assert_eq!(reported(&f), Vec::<u64>::new());
        }
        let calls = f.engine.calls();
        let count = |c: &str| calls.iter().filter(|x| *x == c).count();
        assert_eq!((count("drop"), count("rebuild")), (1, 1), "{calls:?}");
        assert!(
            !calls.iter().any(|c| c.starts_with("unresolved")),
            "{calls:?}"
        );
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    }

    /// An entry this replica has not applied by its apply deadline is
    /// unresolved, its commit known from reports or not; one applied in
    /// time is handed.
    #[tokio::test(start_paused = true)]
    async fn an_entry_not_applied_by_its_deadline_is_unresolved() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let timing = Timing::defaults(B);
        // Three entries 40 ms ahead: one applied in time, one learned
        // committed in time but not applied, one neither.
        let at = index_at(T0) + 40 * 48;
        for index in 1..=3 {
            f.log.push(1, index, Some(at + index * B));
        }
        f.contact.log_changed();
        f.engine.calls();
        f.apply(1);
        f.contact.log_changed();
        assert_eq!(f.engine.calls(), ["appended 1", "committed 1"]);
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 2));
        f.contact.receive(r);
        f.clock.set(timing.apply_deadline(at + 2 * B) - 1);
        f.tick();
        assert!(f.engine.calls().is_empty(), "before the deadline");
        f.clock.set(timing.apply_deadline(at + 2 * B));
        f.tick();
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved 2 at {}", at + 2 * B)],
            "known committed, not applied: entry 2 is never handed"
        );
        f.clock.set(timing.apply_deadline(at + 3 * B));
        f.tick();
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved 3 at {}", at + 3 * B)]
        );
    }

    /// An entry every voter reported committed before it applied here
    /// still gives its slack as it applies: the slack of a replica that
    /// applies late reaches the lead.
    #[tokio::test(start_paused = true)]
    async fn an_entry_committed_everywhere_before_it_applies_here_gives_its_slack() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 400 * B;
        f.log.push(1, 1, Some(at));
        f.contact.log_changed();
        for from in [2, 3] {
            let mut r = f.report(from, None);
            r.committed = Some(id(1, 1));
            f.contact.receive(r);
        }
        assert_eq!(f.contact.metrics().known_committed, 1);
        f.tick();
        f.clock.set(timing.apply_deadline(at) - 1);
        f.apply(1);
        f.contact.log_changed();
        let slack: Vec<(u64, i64)> = (f.tick().into_iter())
            .flat_map(|(_, _, r)| r.slack)
            .map(|s| (s.index, s.samples))
            .collect();
        assert!(!slack.is_empty(), "no slack reported");
        assert!(
            slack
                .iter()
                .all(|&(index, samples)| index == 1 && samples < B as i64),
            "{slack:?}"
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
            // Index 80 lies past the listing horizon.
            let at = if index == 80 {
                now + 60 * 48
            } else {
                now + index * 30
            };
            f.log.push(1, index, Some(at));
        }
        f.contact.log_changed();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 3));
        f.contact.receive(r);
        let (_, _, report) = f.tick().pop().unwrap();
        let indices: Vec<u64> = report.listed.iter().map(|l| l.id.index).collect();
        let expected: Vec<u64> = (4..=67).collect();
        assert_eq!(indices, expected, "the earliest at_index first, 64 of them");
        assert_eq!(report.unlisted, Some(now + 68 * 30));
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
        let at = index_at(T0) + 400 * B;
        for index in 1..=3 {
            f.log.push(1, index, Some(at + index * B));
        }
        f.contact.log_changed();
        f.engine.calls();
        // Its successor asks for an earlier index: raised to the floor
        // of the entries before it, not of the entry it replaced.
        f.log.push(2, 3, Some(at + B));
        f.contact.log_changed();
        assert!(
            f.engine.calls().is_empty(),
            "nothing appended reaches the engine"
        );
        {
            let st = f.contact.lock();
            assert_eq!(st.scheduled[&3].0, id(2, 3));
            assert_eq!(st.scheduled[&3].1, at + 2 * B);
            let awaited: Vec<Key> = st.awaiting.keys().copied().collect();
            assert_eq!(awaited, [(1, 1, 1), (2, 1, 1), (3, 2, 1)]);
        }
        f.apply(3);
        f.contact.log_changed();
        let handed = f.engine.requests.lock().unwrap().last().cloned().unwrap();
        assert_eq!(handed.0, 3);
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
            contact.verified_always();
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

    /// A report from `from` speaking `protocol`.
    fn protocol_report(f: &Fixture, from: NodeId, protocol: u32) -> Report {
        let mut r = f.config_report(from, None);
        r.own.protocol = protocol;
        r
    }

    /// The gate opens only while every member, learners included, sent a
    /// report within `w + D` speaking protocol 2: a member not heard from,
    /// one speaking protocol 1 and one gone silent each keep it shut, and
    /// the metric names it. A learner of protocol 1 is not promoted.
    #[tokio::test(start_paused = true)]
    async fn the_protocol_gate_opens_only_while_every_member_speaks_protocol_2() {
        let mut f = fixture(&[&[1, 2, 3]]);
        f.members.addrs.insert(9, "9".into());
        f.tick();
        assert_eq!(
            f.contact.protocol_blocker(&f.members),
            Some(2),
            "no report yet"
        );
        f.contact.receive(protocol_report(&f, 2, PROTOCOL));
        f.contact.receive(protocol_report(&f, 3, 1));
        assert_eq!(
            f.contact.protocol_blocker(&f.members),
            Some(3),
            "a voter at 1"
        );
        f.contact.receive(protocol_report(&f, 3, PROTOCOL));
        assert_eq!(
            f.contact.protocol_blocker(&f.members),
            Some(9),
            "a learner not heard"
        );
        f.contact.receive(protocol_report(&f, 9, 1));
        assert_eq!(
            f.contact.protocol_blocker(&f.members),
            Some(9),
            "a learner at 1"
        );
        assert!(f.contact.learner_holds_set_config(9));
        assert!(!f.contact.learner_promotable(9), "a learner at 1");
        let m = f.contact.metrics();
        assert!(!m.protocol_gate_open);
        assert_eq!(m.protocol_blocker, Some(9));
        f.contact.receive(protocol_report(&f, 9, PROTOCOL));
        assert!(f.contact.learner_promotable(9));
        assert_eq!(f.contact.protocol_blocker(&f.members), None);
        assert!(f.contact.metrics().protocol_gate_open);
        // A member added since the last tick, which contact holds no
        // report of yet, blocks as soon as it is in the membership passed.
        let mut added = f.members.clone();
        added.addrs.insert(10, "10".into());
        assert_eq!(
            f.contact.protocol_blocker(&added),
            Some(10),
            "a new learner"
        );
        let mut added = f.members.clone();
        added.configs[0].insert(11);
        added.addrs.insert(11, "11".into());
        assert_eq!(f.contact.protocol_blocker(&added), Some(11), "a new voter");
        // Voter 2 falls silent: its newest report ages past w + D.
        let fresh = W + Timing::defaults(B).hold;
        f.clock.add(fresh);
        tokio::time::advance(fresh + Duration::from_millis(1)).await;
        f.contact.receive(protocol_report(&f, 3, PROTOCOL));
        f.contact.receive(protocol_report(&f, 9, PROTOCOL));
        assert_eq!(
            f.contact.protocol_blocker(&f.members),
            Some(2),
            "a silent voter"
        );
        assert_eq!(f.contact.metrics().protocol_blocker, Some(2));
    }

    /// A generation stamp leaves room for the slowest build, but never
    /// further than `AHEAD_MAX` past its stamp, where the entry would take
    /// effect at the stamp itself.
    #[tokio::test(start_paused = true)]
    async fn a_generation_stamp_stays_within_ahead_max_of_its_stamp() {
        let f = fixture(&[&[1]]);
        f.engine.build_time.store(4800, Ordering::SeqCst);
        f.tick();
        let s = f.contact.stamp(true).expect("stamped");
        assert_eq!(
            s.earliest,
            s.stamp + index_at(START_LEAD.as_nanos() as u64) + 4800
        );
        let f = fixture(&[&[1]]);
        f.engine
            .build_time
            .store(2 * crate::state_machine::AHEAD_MAX, Ordering::SeqCst);
        f.tick();
        let s = f.contact.stamp(true).expect("stamped");
        assert_eq!(
            s.earliest,
            s.stamp + crate::state_machine::AHEAD_MAX / B * B
        );
    }

    /// An appended entry that needs a generation never lowers the
    /// generation a re-routing command's stamp reserved, and a `Commands`
    /// entry the leader flagged for its generation counts until it
    /// applies, so a leader that only appended it waits for it too.
    #[tokio::test(start_paused = true)]
    async fn appended_entries_never_lower_the_pending_generation() {
        let f = fixture(&[&[1]]);
        f.tick();
        let raised = f.contact.stamp(true).expect("stamped").earliest;
        let far = raised + 400 * B;
        f.insert(
            1,
            Body::InputUp {
                slot: 0,
                at_index: raised - B,
            },
        );
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.contact.lock().reserved, Some(raised));

        let restarted = Contact::new(
            1,
            Timing::defaults(B),
            f.clock.clone(),
            Some(f.engine.clone()),
            f.log.clone(),
            1,
        );
        restarted.verified_always();
        let mut command = crate::state_machine::tests::command("s", "1", "route");
        command.at_index = Some(far);
        command.generation = Some(true);
        f.insert(2, Body::Commands(vec![command]));
        restarted.tick(f.members.clone());
        assert_eq!(restarted.lock().generations.get(&2), Some(&far));
        assert_eq!(restarted.stamp(true), Err(StampRefusal::GenerationPending));
    }

    /// A command flagged for its generation that the product refused
    /// builds none: once it applied, the applied state, the source a
    /// restarted contact reads, names no generation for it, and the
    /// appended entries no longer do. On a live leader the stamp that
    /// ordered it still reserves its index, for at most its lead
    /// (`State::reserved`); this contact never stamped it.
    #[tokio::test(start_paused = true)]
    async fn an_applied_flagged_command_that_built_nothing_holds_no_generation() {
        let f = fixture(&[&[1]]);
        f.tick();
        let far = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
        let mut command = crate::state_machine::tests::command("s", "1", "route");
        command.at_index = Some(far);
        command.generation = Some(true);
        f.insert(1, Body::Commands(vec![command]));
        f.contact.log_changed();
        f.tick();
        assert_eq!(f.contact.stamp(true), Err(StampRefusal::GenerationPending));
        // Applied, refused by the product: no generation in the state.
        f.apply(1);
        f.contact.log_changed();
        assert!(f.contact.stamp(true).is_ok());
    }

    /// A replica restarted after it applied an entry that re-routed an
    /// input, whose index has not passed, waits for that generation before
    /// it stamps another: two would be pending.
    #[tokio::test(start_paused = true)]
    async fn a_restarted_contact_waits_for_an_applied_generation() {
        let f = fixture(&[&[1]]);
        f.tick();
        let far = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
        f.log.0.lock().unwrap().generation = Some(far);
        let restarted = Contact::new(
            1,
            Timing::defaults(B),
            f.clock.clone(),
            Some(f.engine.clone()),
            f.log.clone(),
            1,
        );
        restarted.verified_always();
        restarted.tick(f.members.clone());
        assert_eq!(restarted.stamp(true), Err(StampRefusal::GenerationPending));
    }

    /// A stamped void or lease expiry derives at its index like a
    /// command: listed and awaited there. A void from an older leader,
    /// which stamps none, takes effect nowhere and is neither.
    #[tokio::test(start_paused = true)]
    async fn a_stamped_void_or_expiry_is_awaited_where_it_derives() {
        let f = fixture(&[&[1, 2]]);
        f.tick();
        let at = index_at(T0) + 400 * B;
        let void = |at_index| Body::Void {
            revision: 1,
            term: 1,
            at_index,
            generation: None,
        };
        f.insert(4, void(None));
        f.insert(5, void(Some(at)));
        f.insert(
            6,
            Body::LeaseExpiry {
                session: crate::entry::SessionKey::from_parts("p", "s"),
                renewed: 0,
                at_index: Some(at + B),
            },
        );
        f.contact.log_changed();
        f.tick();
        let st = f.contact.lock();
        let scheduled: Vec<(u64, u64)> = st.scheduled.iter().map(|(&i, e)| (i, e.1)).collect();
        assert_eq!(scheduled, [(5, at), (6, at + B)]);
        let awaited: Vec<u64> = st.awaiting.keys().map(|k| k.0).collect();
        assert_eq!(awaited, [5, 6]);
    }
}
