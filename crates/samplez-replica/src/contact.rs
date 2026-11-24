//! Contact between voters, leader-independent (plan 2.11, Fencing): side
//! reports each report period p, the contact fence F (B1), silence held
//! and relayed, commit knowledge from openraft and from reports, and the
//! apply deadline that leaves an entry unresolved (B2).
//!
//! Each replica keeps the rules on its own clocks: its TAI clock for
//! everything compared with block deadlines, `CLOCK_MONOTONIC` (tokio's
//! clock) for receipt ages and silence.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use openraft::{BasicNode, RaftMetrics};
use samplez::blocks::{BlockStatus, Cause, Ranks, Standing};
use samplez::{Entry, ProductState};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::Shared;
use crate::baseline::{Leader, LeaderView, Voter};
use crate::bridge::PAGES_REPORTED;
use crate::entry::{Baseline, Body, NodeId, PROTOCOL, Request};
use crate::lock_watch::{WatchedGuard, WatchedMutex};
use crate::log_store::Store;
use crate::peer::{PeerRequest, PeerResponse};
use crate::rank::Baselines;
use crate::report::{Carried, Echo, Heard, Histogram, Listed, MAX_LISTED, Own, Report, Slack};
use crate::resync::{CoreReply, Failed, Missed, Resynced, Source};
use crate::schedule::Lead;
use crate::snapshot::StoredLogId;
use crate::state::{Life, PageBook, ReplicaState, Seen, View};
use crate::state_machine::{Product, Record};
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
    /// How long the minute of slack the apply lead falls on lasts
    /// ([`crate::schedule::MINUTE`]); shorter only in tests.
    pub lead_minute: Duration,
    /// `pruneAfterBlocks`: how long, in blocks, an input's liveness the
    /// audio voters decided holds before the leader hands it on
    /// ([`crate::liveness`]). Contact does not read the engine's
    /// configuration: the product sets it to its engine's
    /// `pruneAfterBlocks`.
    pub prune_after_blocks: u32,
}

impl Timing {
    /// The defaults at block size `block`: p 2 ms, w 8 ms, D 10 ms, a
    /// margin of 576 samples, clockGuard half of it, maxApplyLead 50 ms,
    /// resyncBudget 50 ms, pruneAfterBlocks 50.
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
            lead_minute: crate::schedule::MINUTE,
            prune_after_blocks: 50,
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
    /// The kernel TAI offset, `CLOCK_TAI` - `CLOCK_REALTIME` read back to
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
    /// The `rampSamples` and `blockSize` of the engine behind this audio,
    /// which the replica follows gain ramps at ([`crate::dedup::Limits`]);
    /// none for audio with no engine.
    fn ramp_config(&self) -> Option<(u32, u64)> {
        None
    }
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
    /// Whether the replica's output is apart from its peers': diverged,
    /// or resyncing before the hand-over. The engine's meters lose what
    /// it computes meanwhile. Set on every tick.
    fn diverged(&self, _apart: bool) {}
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
    fn resync(&self, _core: &CoreReply, _source: &dyn Source) -> Result<Resynced, Failed> {
        Err("no engine to resync".into())
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
    /// [`LogView::entries_after`] for the first `max` entries only, which
    /// it stops after: true when more follow.
    fn entries_after_max(
        &self,
        index: u64,
        max: usize,
        f: &mut dyn FnMut(StoredLogId, &Request),
    ) -> bool {
        let mut taken = 0;
        let mut more = false;
        self.entries_after(index, &mut |id, request| {
            if taken < max {
                taken += 1;
                f(id, request);
            } else {
                more = true;
            }
        });
        more
    }
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
    /// The newest entry at or below `records_from` that derived a record
    /// ([`crate::state_machine::StateMachine::record_floor`]).
    fn record_floor(&self) -> Option<u64> {
        None
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
        Store::entries_after(self, index, f);
    }
    fn entries_after_max(
        &self,
        index: u64,
        max: usize,
        f: &mut dyn FnMut(StoredLogId, &Request),
    ) -> bool {
        Store::entries_after_max(self, index, max, f)
    }
    fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
        Store::pending(self, f);
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
    fn record_floor(&self) -> Option<u64> {
        Store::record_floor(self)
    }
    fn product_state(&self) -> (u64, u64, ProductState) {
        Store::product_state(self)
    }
    fn floor_before(&self, index: u64) -> u64 {
        Store::floor_before(self, index)
    }
    fn applied_floor(&self) -> (u64, u64) {
        self.read_state(|n, sm| (n, sm.floor))
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
        self.read_state(|_, sm| sm.pending_generation())
    }
    fn judged(&self) -> BTreeSet<NodeId> {
        self.read_state(|_, sm| {
            sm.verdicts
                .iter()
                .filter(|&(_, &judged)| judged)
                .map(|(&id, _)| id)
                .collect()
        })
    }
    fn latest_judged(&self) -> Option<NodeId> {
        self.read_state(|_, sm| sm.latest_judged)
    }
    fn baseline(&self) -> Option<Baseline> {
        self.read_state(|_, sm| sm.baseline.clone())
    }
    fn disrupting(&self) -> Option<NodeId> {
        self.read_state(|_, sm| sm.slot.as_ref().map(|d| d.node))
    }
}

/// The lead before any slack was reported: 30 ms.
pub const START_LEAD: Duration = Duration::from_millis(30);

/// Entries with an `at_index` the engine holds at most (plan 2.7): the
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

/// Appended entries taken from the log under one hold of the contact
/// lock, at most: a replica back from a long absence takes its catch-up
/// in parts, each hold short of the lock watchdog's limit.
const TAKEN_AT_ONCE: usize = 1024;

/// Entries with an `at_index` kept for listings, at most; the lowest index
/// is dropped first, counted.
const SCHEDULED_MAX: usize = 4096;

/// Entries given to the engine as unresolved that are remembered, so a
/// listing that names one again does not give it again.
const GIVEN_KEPT: usize = 1024;

/// The ids of the unresolvable entries a listing's overflow names: this
/// bit and the omitted `at_index` in blocks, so replicas that hold one for
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
    /// The voters its latest report said it took no report from within
    /// w, or holds silent: their records are relayed to it untrimmed.
    stale: BTreeSet<NodeId>,
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
    /// Times it went from heard to held silent.
    silent_transitions: u64,
    /// The longest time from its last report heard to the next, over the
    /// gaps in which it was held silent.
    silent_gap_max: Duration,
    reports: u64,
}

#[derive(Debug, Clone, Copy)]
struct Waiting {
    at_index: u64,
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
    start_hold_warnings: u64,
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
    /// When contact was last lost, while out of contact; none before the
    /// first contact.
    lost: Option<Instant>,
    /// The last log index taken from the log as appended.
    told: u64,
    /// The floor after the entry at `told`, as the apply will raise it
    /// ([`crate::state_machine::floored`]).
    floor_told: u64,
    /// Entries taken as appended whose commit is not known, until then:
    /// a truncation is found among them.
    uncommitted: BTreeMap<u64, StoredLogId>,
    /// Entries with an `at_index`, for listings, until every voter reported
    /// them committed: log id, `at_index`, stamp, and whether it is a
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
    /// The Raft term this voter is in, as [`Contact::in_term`] was given
    /// it: slack of an entry stamped by an earlier term is not counted
    /// ([`add_slack`]). Zero before the first.
    office_term: u64,
    /// The lead each entry with an `at_index` was stamped with, by log
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
    /// Entries given to the engine as unresolved, or refused by it, past
    /// `GIVEN_KEPT` a refused one forgotten first, the lowest first: one
    /// the engine holds stays, so it is never handed as applied too.
    given: BTreeMap<Key, u64>,
    /// The entries of `given` the engine does not hold, refused or not
    /// asked yet, and maybe some no longer in it.
    refused: BTreeSet<Key>,
    /// The engine refused an unresolved entry, and is not told yet.
    overwhelmed: bool,
    /// The pending restart was begun by a rebuild past entries taken
    /// late: until its rebuild starts, the later parts of the same
    /// catch-up are rebuilt past by it too.
    late_restart: bool,
    /// The lowest `at_index` of a listing overflow given as unresolved: a
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
    /// Why the replica, in `starting`, is not ready, and how long it has
    /// been in `starting`.
    start_hold: Option<StartHold>,
    start_watch: StartWatch,
    /// Clock steps the engine detected, as of the last sample.
    clock_steps: u64,
    /// What `replica_clock_unverified` names, and since when it has.
    unverified: Vec<(NodeId, Option<i64>)>,
    unverified_since: Option<Instant>,
    /// Voters reporting TAI offset 0.
    offset_zero: Vec<NodeId>,
    /// The donor core's last log id a replay last missed a record after.
    /// One value only: a repeat of it is not logged again, and a value
    /// alternating with another is logged on every change.
    missed_after: Option<u64>,
    #[cfg(test)]
    verified_always: bool,
    /// The horizon holds this replica whatever it measures.
    #[cfg(test)]
    horizon_always: bool,
    /// Reports and copies as before [`Carried`]: every record in every
    /// report, copies keeping theirs and relaying them.
    #[cfg(test)]
    as_before: bool,
    /// Per addressee, the page records it confirmed.
    carried: BTreeMap<NodeId, Carried>,
    /// Per voter, the generation of its records in the page book: raised,
    /// from `page_generation`, whenever one of them is new or differs.
    page_generations: BTreeMap<NodeId, u64>,
    page_generation: u64,
    /// Offers of page records built, counted for the tests.
    #[cfg(test)]
    offers_built: u64,
    /// Every period builds its offers, none taken from the last one.
    #[cfg(test)]
    offer_cache_off: bool,
    decider: crate::decide::Decider,
    liveness: crate::liveness::Liveness,
    /// The audio `/readyz` term, as of the last period in contact or, out
    /// of contact for [`crate::rollout::DEPARTED`], of the last period;
    /// none before the first contact.
    serving: Option<bool>,
    /// This audio replica committed since its process started. Contact
    /// lives as long as the process: a rebuild of the engine from product
    /// state, a resync or a return to `starting` keeps it, a restart of
    /// the process starts it false.
    committed_since_start: bool,
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
    state: WatchedMutex<State>,
    costs: Costs,
}

/// The calling thread's CPU time.
pub(crate) fn thread_cpu() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is writable; CLOCK_THREAD_CPUTIME_ID exists on Linux.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// What the contact loop cost, counted outside the contact lock.
#[derive(Default)]
struct Costs {
    ticks: AtomicU64,
    tick_ns: AtomicU64,
    decisions_ns: AtomicU64,
    decode_ns: AtomicU64,
    receive_ns: AtomicU64,
}

/// Adds the calling thread's CPU time since `since` to `to`.
fn add_cpu(to: &AtomicU64, since: Duration) {
    let ns = thread_cpu().saturating_sub(since).as_nanos() as u64;
    to.fetch_add(ns, Ordering::Relaxed);
}

/// The contact loop's cost so far: the report periods it ran, and the
/// CPU time of the threads running each part of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CostCounts {
    pub ticks: u64,
    /// [`Contact::tick`]: the period's state and the reports to send.
    pub tick: Duration,
    /// [`Contact::decisions`], the leader's.
    pub decisions: Duration,
    /// Decoding the peers' requests that carry a report.
    pub decode: Duration,
    /// [`Contact::receive`]: taking a decoded report.
    pub receive: Duration,
}

impl State {
    /// [`State::as_before`] in tests; never otherwise.
    fn as_before(&self) -> bool {
        #[cfg(test)]
        return self.as_before;
        #[cfg(not(test))]
        false
    }
}

/// Adds a counted replica's slack to the lead, naming the replica when
/// it asked for more, and warning once as the lead reaches its cap; an
/// entry whose stamp it knows counts the lead it needed too
/// ([`Lead::needs`]).
///
/// Slack of an entry the log holds from a term before the one this voter
/// is in is not counted: such an entry was stamped by an earlier leader
/// and its commit waited for an election, so its lateness is the
/// election's, not the replica's. Entries of the current term count, one
/// stamped before the term began included, and so do entries the log no
/// longer holds.
fn add_slack(st: &mut State, log: &dyn LogView, from: NodeId, slack: &Slack, current: u64) {
    if log
        .term_at(slack.index)
        .is_some_and(|term| term < st.office_term)
    {
        return;
    }
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
    /// The versions this replica's compatibility key holds
    /// ([`crate::rollout::Rollout::versions`]); none for the witness and
    /// where the rollout reports none.
    pub versions: Option<crate::rollout::Versions>,
    /// buildLead of this replica's engine, in samples
    /// ([`crate::report::Own::build_lead`]); none without an engine.
    pub build_lead: Option<u64>,
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
    /// had no strict majority hash among replicas of one key, or was
    /// committed whole by replicas of two keys, an output carrying both.
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
    /// Why the audio replica, in `starting`, is not ready; none outside
    /// `starting` and once ready.
    pub start_hold: Option<StartHold>,
    /// Starts held past [`START_HOLD_WARN`], each logged once.
    pub start_hold_warnings: u64,
}

/// Why an audio replica in `starting` is not ready: the first term of
/// its readiness that fails, in the order [`Contact::live`] needs them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartHold {
    /// Its clock is not verified ([`crate::verify`]).
    ClockUnverified,
    /// The horizon holds it ([`crate::horizon`]).
    HorizonHeld,
    /// Its log has applied below the index its restart, or the commit it
    /// knows, needs.
    Applying { applied: u64, needed: u64 },
    /// Its engine core is being rebuilt from product state.
    Rebuilding,
    /// The engine's pending generation is not built yet.
    GenerationPending,
    /// The engine has processed no block since its core was installed.
    NotProcessing,
}

impl StartHold {
    /// Every [`StartHold::reason`].
    pub const REASONS: [&'static str; 6] = [
        "clock_unverified",
        "horizon_held",
        "applying",
        "rebuilding",
        "generation_pending",
        "not_processing",
    ];

    /// Its label.
    pub fn reason(self) -> &'static str {
        match self {
            StartHold::ClockUnverified => "clock_unverified",
            StartHold::HorizonHeld => "horizon_held",
            StartHold::Applying { .. } => "applying",
            StartHold::Rebuilding => "rebuilding",
            StartHold::GenerationPending => "generation_pending",
            StartHold::NotProcessing => "not_processing",
        }
    }
}

/// How long an audio replica stays in `starting` held for a reason
/// before it logs that reason, once per start.
pub const START_HOLD_WARN: Duration = Duration::from_secs(30);

/// One stay in `starting`: since when, and whether it was logged.
#[derive(Debug, Default)]
struct StartWatch {
    since: Option<Instant>,
    warned: bool,
}

impl StartWatch {
    /// One period at `now`, `starting` whether the replica is in
    /// `starting` and `held` whether a reason holds it there. True once
    /// per stay, the first period in which a reason holds it and it has
    /// been in `starting` for [`START_HOLD_WARN`].
    fn observe(&mut self, starting: bool, held: bool, now: Instant) -> bool {
        if !starting {
            *self = StartWatch::default();
            return false;
        }
        let since = *self.since.get_or_insert(now);
        if self.warned || !held || now.saturating_duration_since(since) < START_HOLD_WARN {
            return false;
        }
        self.warned = true;
        true
    }
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
    /// Times this replica went from hearing it to holding it silent.
    pub silent_transitions: u64,
    /// The longest time from its last report heard to the next, over the
    /// gaps in which this replica held it silent.
    pub silent_gap_max: Duration,
    /// This replica holds it silent. Silence counts from its last report
    /// or from contact regained, whichever is later, so for one contact
    /// window after contact is regained no peer is silent, however long
    /// ago its last report arrived.
    pub silent: bool,
    /// The time since its last report received directly, on the
    /// monotonic clock, at this snapshot; none before the first. Relayed
    /// copies and regaining contact leave it unchanged.
    pub last_report_age: Option<Duration>,
    /// Its clock minus this one's, from the latest exchange, ns.
    pub offset_ns: Option<i64>,
    /// What clock verification reads of it: its clock minus this one's
    /// from the exchange with the lowest round trip of the last
    /// [`crate::clock::WINDOW`], and that exchange's one-way delay, ns.
    pub clock_best: Option<(i64, u64)>,
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
            costs: Costs::default(),
            state: WatchedMutex::new(
                "contact",
                State {
                    members: Members::default(),
                    seq: first_seq,
                    sent: VecDeque::with_capacity(SENT_KEPT),
                    peers: BTreeMap::new(),
                    in_contact: false,
                    regained: Instant::now(),
                    lost: None,
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
                    lead: Lead::with_minute(
                        timing.block,
                        index_at(START_LEAD.as_nanos() as u64),
                        index_at(timing.max_apply_lead.as_nanos() as u64),
                        timing.lead_minute,
                    ),
                    office_term: 0,
                    leads: BTreeMap::new(),
                    lead_raised_by: None,
                    e_spread: Histogram::default(),
                    known_committed: applied,
                    handed: applied,
                    awaiting: BTreeMap::new(),
                    given: BTreeMap::new(),
                    refused: BTreeSet::new(),
                    overwhelmed: false,
                    late_restart: false,
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
                    start_hold: None,
                    start_watch: StartWatch::default(),
                    clock_steps: 0,
                    unverified: Vec::new(),
                    unverified_since: None,
                    offset_zero: Vec::new(),
                    missed_after: None,
                    #[cfg(test)]
                    verified_always: false,
                    #[cfg(test)]
                    horizon_always: false,
                    #[cfg(test)]
                    as_before: false,
                    carried: BTreeMap::new(),
                    page_generations: BTreeMap::new(),
                    page_generation: 0,
                    #[cfg(test)]
                    offers_built: 0,
                    #[cfg(test)]
                    offer_cache_off: false,
                    decider: crate::decide::Decider::default(),
                    liveness: crate::liveness::Liveness::default(),
                    serving: None,
                    committed_since_start: false,
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
                },
            ),
        }
    }

    pub fn timing(&self) -> Timing {
        self.timing
    }

    /// Whether `b` hands the baseline over to another key than the
    /// latest `Baseline` applied carries, resolved as the leader resolves
    /// it ([`crate::rollout::baseline_of`]).
    pub(crate) fn hands_over(&self, b: &Baseline) -> bool {
        let Some(cur) = self.log.baseline() else {
            return false;
        };
        let st = self.lock();
        let named = (self.copies(&st, Instant::now()).into_iter())
            .find(|&(id, _, _)| id == cur.replica)
            .map(|(_, _, own)| own);
        let (key, _) = crate::rollout::baseline_of(&cur, named);
        crate::state::differs(key, b.key)
    }

    /// The compatibility key and revisions of a rollout
    /// ([`crate::rollout`]): without, every key is equal.
    pub fn set_rollout(&self, rollout: Arc<dyn crate::rollout::Rollout>) {
        self.lock().rollout = Some(rollout);
    }

    #[track_caller]
    fn lock(&self) -> WatchedGuard<'_, State> {
        crate::lock_watch::assert_no_product_code("contact");
        self.state.lock()
    }

    /// Takes a report from another voter.
    pub fn receive(&self, report: Report) {
        let cpu = thread_cpu();
        self.receive_held(report);
        self.overwhelm();
        add_cpu(&self.costs.receive_ns, cpu);
    }

    fn receive_held(&self, report: Report) {
        let now = Instant::now();
        let tai = self.clocks.tai_ns();
        let w = self.timing.contact_window;
        let mut st = self.lock();
        let st = &mut *st;
        let as_before = st.as_before();
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
        let regained = st.regained;
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
        // A sequence number this far past the last one is another
        // process's: the sender restarted.
        let restarted =
            (peer.last.as_ref()).is_some_and(|l| report.seq.wrapping_sub(l.seq) > SEQ_WINDOW);
        // The page records it took: those of the report it echoes and
        // before. No echo, an echo of no report this process sent lately,
        // or a restart: it may hold no record, and is sent every one.
        let carried = st.carried.entry(report.from).or_default();
        match (report.echo, t1) {
            (Some(echo), Some(_)) if !restarted => carried.echoed(echo.seq),
            _ => *carried = Carried::default(),
        }
        peer.reports += 1;
        let w_us = w.as_micros() as u64;
        peer.stale = (report.heard.iter())
            .filter(|h| h.silent || h.age_us.is_none_or(|a| a > w_us))
            .map(|h| h.node)
            .collect();
        if let Some(last) = &peer.last {
            let gap = since(now, last.received);
            if peer.silent {
                // Silence is only held while in contact, so the gap
                // counts from the later of the last report and the
                // time contact was regained, as the tick does.
                let held = since(now, last.received.max(regained));
                peer.silent_gap_max = peer.silent_gap_max.max(held);
            }
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
        if st.pages.fold(report.from, &report.own.pages) {
            st.page_generation += 1;
            (st.page_generations).insert(report.from, st.page_generation);
        }
        // Page records live in the page book alone: a copy holds none, and
        // one relayed carries the book's.
        let mut own = report.own;
        if !as_before {
            own.pages = Vec::new();
        }
        peer.last = Some(Direct {
            seq: report.seq,
            received_tai: tai,
            received: now,
            own,
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
                if st.pages.fold(heard.node, &own.pages) {
                    st.page_generation += 1;
                    (st.page_generations).insert(heard.node, st.page_generation);
                }
                let mut own = own;
                if !as_before {
                    own.pages = Vec::new();
                }
                other.relayed = Some((generated, own));
            }
        }
        if let (Some(theirs), Some(mine)) = (report.estimate, self.estimate_in(st, now)) {
            st.e_spread.add(theirs.abs_diff(mine));
        }
        if self.counted(st, now).contains(&report.from) {
            let current = self.current_stamp(st, now);
            for slack in &report.slack {
                add_slack(st, self.log.as_ref(), report.from, slack, current);
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
        let mut unresolved = Vec::new();
        for l in &report.listed {
            if l.baseline {
                st.baselines.listed(l.id, l.at_index);
            } else {
                self.note(st, l.id, l.at_index, tai, &mut unresolved);
            }
        }
        self.offer_unresolved(st, unresolved);
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
            let mut unresolved = Vec::new();
            self.note(st, id, at, tai, &mut unresolved);
            self.offer_unresolved(st, unresolved);
        }
        self.fence(st, tai);
    }

    /// The log changed: appends and truncations are taken, what the
    /// entries openraft applied derived goes to the engine, and they are
    /// known committed. [`TAKEN_AT_ONCE`] entries at a time, the contact
    /// lock released between them.
    pub fn log_changed(&self) {
        loop {
            let tai = self.clocks.tai_ns();
            let mut st = self.lock();
            let st = &mut *st;
            self.expire(st, tai);
            let more = self.sync_log(st, tai);
            let applied = self.log.applied();
            self.learn(st, applied);
            if !more {
                break;
            }
        }
        self.overwhelm();
    }

    /// The engine refused an unresolved entry since the last call: it is
    /// told so, once, with the contact lock released.
    fn overwhelm(&self) {
        let overwhelmed = std::mem::take(&mut self.lock().overwhelmed);
        if let (true, Some(audio)) = (overwhelmed, &self.audio) {
            audio.overwhelmed();
        }
    }

    /// One report period: the log, the deadlines, F, silence, and the
    /// reports to send, with each addressee's address.
    pub fn tick(&self, members: Members) -> Vec<(NodeId, String, Report)> {
        let cpu = thread_cpu();
        let out = self.tick_held(members);
        self.overwhelm();
        self.costs.ticks.fetch_add(1, Ordering::Relaxed);
        add_cpu(&self.costs.tick_ns, cpu);
        out
    }

    /// What the contact loop cost so far.
    pub fn costs(&self) -> CostCounts {
        let ns = |a: &AtomicU64| Duration::from_nanos(a.load(Ordering::Relaxed));
        CostCounts {
            ticks: self.costs.ticks.load(Ordering::Relaxed),
            tick: ns(&self.costs.tick_ns),
            decisions: ns(&self.costs.decisions_ns),
            decode: ns(&self.costs.decode_ns),
            receive: ns(&self.costs.receive_ns),
        }
    }

    /// Counts the CPU time since `since` of decoding a peer's report.
    pub(crate) fn decoded(&self, since: Duration) {
        add_cpu(&self.costs.decode_ns, since);
    }

    fn tick_held(&self, members: Members) -> Vec<(NodeId, String, Report)> {
        let now = Instant::now();
        // Taken before the engine's sample: every timestamp a report
        // carries precedes the flags beside it.
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        let as_before = st.as_before();
        let voters = members.voters();
        st.peers.retain(|id, _| voters.contains(id));
        st.carried.retain(|id, _| voters.contains(id));
        st.lead_raised_by = st.lead_raised_by.filter(|id| voters.contains(id));
        st.learners
            .retain(|id, _| members.addrs.contains_key(id) && !voters.contains(id));
        st.members = members;
        self.expire(st, tai);
        // One part of a catch-up: the next tick or log change takes the
        // rest.
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
            // Its offer is its own latest records: a change to them, one
            // dropped included, is a new generation.
            let own_pages_changed = sample.own.pages != st.own.pages;
            st.own = sample.own;
            st.own_at = now;
            st.status_changes_lost = sample.status_changes_lost;
            st.entries_refused = sample.entries_refused;
            st.entries_filtered = sample.entries_filtered;
            st.realised = sample.realised;
            st.enabled = sample.enabled;
            st.generation_ready = sample.generation_ready;
            st.clock_steps = sample.clock_steps;
            if st.pages.fold(self.id, &st.own.pages) | own_pages_changed {
                st.page_generation += 1;
                (st.page_generations).insert(self.id, st.page_generation);
            }
            if sample.log_overflows > st.log_overflows {
                st.log_overflows = sample.log_overflows;
                // The engine dropped its core and committed entries with
                // it: rebuilt from the state after them.
                self.begin_restart(st, applied, "log overflow");
            }
            if let Some(r) = &st.rollout {
                let _product = crate::lock_watch::ProductCode::enter();
                st.own.key = Some(r.key());
                st.own.versions = r.versions();
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
        st.lost = if in_contact {
            None
        } else {
            st.lost.or(st.in_contact.then_some(now))
        };
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
            let was_silent = p.silent;
            p.silent = in_contact && since(now, from) > w;
            if p.silent && !was_silent {
                p.silent_transitions += 1;
            }
            if p.silent {
                p.silent_at = Some(tai);
            }
        }
        st.withhold.retain(&st.members);
        st.pages.retain(|v| voters.contains(&v));
        st.page_generations.retain(|v, _| voters.contains(v));
        self.fence(st, tai);
        self.decide(st, tai);
        self.live(st, now, tai);
        // After the decisions known committed were acted on.
        if let Some(audio) = &self.audio {
            st.own.unresolved = Self::unresolved(st);
            audio.diverged(st.life.as_ref().is_some_and(Life::apart));
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
            let mut heard: Vec<Heard> = voters
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
            // Page records: its own latest, and for each copy relayed the
            // newest this replica holds of that voter, each carried until
            // the addressee confirms it. An addressee not heard within w
            // may have restarted with no record: it is sent every one.
            // A period offering the same voters' records at the same
            // generations carries what the last did, less what was
            // confirmed since, and builds nothing.
            let generation = |v: NodeId| st.page_generations.get(&v).copied().unwrap_or(0);
            let offer: Vec<(NodeId, u64)> = std::iter::once(self.id)
                .chain(heard.iter().filter(|h| h.own.is_some()).map(|h| h.node))
                .map(|v| (v, generation(v)))
                .collect();
            let heard_within_w = peer
                .and_then(|p| p.last.as_ref())
                .is_some_and(|d| since(now, d.received) <= w);
            let carried = st.carried.entry(to).or_default();
            if !heard_within_w {
                *carried = Carried::default();
            }
            let cached = carried.unchanged(&offer);
            #[cfg(test)]
            let cached = cached.filter(|_| !st.offer_cache_off);
            let carry = match cached {
                Some(carry) => carry,
                None => {
                    #[cfg(test)]
                    {
                        st.offers_built += 1;
                    }
                    let mut offered = vec![(self.id, st.own.pages.clone())];
                    offered.extend(
                        (heard.iter())
                            .filter(|h| h.own.is_some())
                            .map(|h| (h.node, st.pages.latest(h.node, PAGES_REPORTED))),
                    );
                    carried.carry(st.seq, offer, &offered)
                }
            };
            let mut carry = carry.into_iter();
            let mut own = st.own.clone();
            if !as_before {
                own.pages = carry.next().unwrap_or_default();
                // A voter the addressee took no direct report from
                // within w gets its newest records relayed whole: one
                // confirmed meanwhile may have come in a copy it dropped
                // for a newer direct one.
                for h in &mut heard {
                    if let Some(relayed) = &mut h.own {
                        let trimmed = carry.next().unwrap_or_default();
                        relayed.pages = if peer.is_some_and(|p| p.stale.contains(&h.node)) {
                            st.pages.latest(h.node, PAGES_REPORTED)
                        } else {
                            trimmed
                        };
                    }
                }
            }
            out.push((
                to,
                addr,
                Report {
                    from: self.id,
                    seq: st.seq,
                    sent_at: tai,
                    echo,
                    committed,
                    own,
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
        let mut expired = Vec::new();
        st.awaiting.retain(|&k, w| {
            let due = tai >= timing.apply_deadline(w.at_index);
            if due {
                expired.push((k, w.at_index));
            }
            !due
        });
        self.offer_unresolved(st, expired);
    }

    /// Gives `entries` to the engine as unresolved. Once it refused one,
    /// the rest are taken as refused without asking it, and it is told it
    /// is overwhelmed once the contact lock is released
    /// ([`Contact::overwhelm`]): the engine holds
    /// [`samplez::Engine::unresolved`]'s few, so each call stays short
    /// however many come. A refusal can also follow a control ring full
    /// for a moment, which a later entry might have found free; the
    /// replica withdraws either way, and that is its own state, the same
    /// on no other replica.
    fn offer_unresolved(&self, st: &mut State, entries: Vec<(Key, u64)>) {
        let given = self.give(st, entries);
        self.ask(st, given);
        Self::forget_given(st);
    }

    /// Takes `entries` as given, the engine not asked yet, and returns
    /// them for [`Contact::ask`]: listed again, an entry given once is not
    /// given again, and one refused neither waits nor withdraws again.
    fn give(&self, st: &mut State, entries: Vec<(Key, u64)>) -> Vec<Key> {
        if self.audio.is_none() {
            return Vec::new();
        }
        let mut given = Vec::with_capacity(entries.len());
        for (k, at) in entries {
            st.given.insert(k, at);
            st.refused.insert(k);
            given.push(k);
        }
        given
    }

    /// Asks the engine to hold the entries [`Contact::give`] took that are
    /// still given: `hand` in between rebuilds past those a catch-up
    /// applied late, which the engine is then never asked to hold. Leaves
    /// the cap to the caller ([`Contact::forget_given`]), which applies it
    /// once the last entry of its part is asked.
    fn ask(&self, st: &mut State, given: Vec<Key>) {
        let Some(audio) = &self.audio else {
            return;
        };
        let mut room = true;
        for k in given {
            let Some(&at) = st.given.get(&k) else {
                continue;
            };
            if !st.refused.contains(&k) {
                continue;
            }
            room = room && audio.unresolved(k.0, at);
            if room {
                st.counts.unresolved += 1;
                if k.0 & UNLISTED_BIT != 0 {
                    st.unlisted_low = Some(st.unlisted_low.map_or(at, |l| l.min(at)));
                }
                st.refused.remove(&k);
            } else {
                st.counts.unresolved_refused += 1;
            }
        }
        st.overwhelmed |= !room;
    }

    /// Past [`GIVEN_KEPT`], the lowest refused entry is forgotten first:
    /// the engine holds none of them. One it holds is forgotten only
    /// once none is left, which takes an engine holding more than
    /// `GIVEN_KEPT`.
    fn forget_given(st: &mut State) {
        while st.given.len() > GIVEN_KEPT {
            match st.refused.pop_first() {
                Some(k) => {
                    st.given.remove(&k);
                }
                None => {
                    st.given.pop_first();
                }
            }
        }
        if st.refused.len() > GIVEN_KEPT {
            let given = &st.given;
            st.refused.retain(|k| given.contains_key(k));
        }
    }

    /// Takes new appends and truncations from the log: each entry with an
    /// index it takes effect at is listed and awaited there, the index the
    /// apply will derive at ([`crate::state_machine::floored`]); then
    /// hands what the applied entries derived to the engine. Takes at
    /// most [`TAKEN_AT_ONCE`] appended entries: true when more are left,
    /// as when openraft appended them later.
    fn sync_log(&self, st: &mut State, tai: u64) -> bool {
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
        let more = self
            .log
            .entries_after_max(st.told, TAKEN_AT_ONCE, &mut |id, request| {
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
                    st.verdicts.push((id.index, *page));
                }
                Body::PurgePoint { replica, index } if *replica == self.id => {
                    st.purge_points.push((id.index, *index));
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
        let mut unresolved = Vec::new();
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
                self.note(st, id, at, tai, &mut unresolved);
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
        let given = self.give(st, unresolved);
        self.hand(st);
        self.ask(st, given);
        // Trimmed only now, the engine asked in order: `give` marks every
        // entry refused before it is asked, so a trim in between, such as
        // the one `hand` ends with, would evict the lowest entries of
        // this very part, which are the ones the engine must be asked
        // about first.
        Self::forget_given(st);
        more
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
        st.refused.clear();
        st.unlisted.clear();
        st.unlisted_low = None;
        st.verdicts.retain(|&(index, _)| index > applied);
        st.decisions.retain(|&(index, _, _)| index > applied);
        st.purge_points.retain(|&(index, _)| index > applied);
        st.counts.installs += 1;
        self.begin_restart(st, applied, "snapshot install");
        applied
    }

    /// Returns the replica to `starting` until its applied index passed
    /// `after` and its engine was rebuilt from product state, `why`.
    fn begin_restart(&self, st: &mut State, after: u64, why: &'static str) {
        tracing::info!(after, why, "engine to be rebuilt from product state");
        st.late_restart = false;
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
        let given: Vec<(Key, u64)> = st.given.iter().map(|(&k, &at)| (k, at)).collect();
        for (k, at) in given {
            if audio.unresolved(k.0, at) {
                st.refused.remove(&k);
            } else {
                st.refused.insert(k);
            }
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

    /// An entry this replica knows of: added to `unresolved` when its
    /// apply deadline has passed or no more are awaited, for the caller
    /// to offer the engine, else awaited unless it is applied here.
    fn note(
        &self,
        st: &mut State,
        id: StoredLogId,
        at_index: u64,
        tai: u64,
        unresolved: &mut Vec<(Key, u64)>,
    ) {
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
        if expired || full {
            unresolved.push((k, at_index));
        } else {
            st.awaiting.insert(k, Waiting { at_index });
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
            self.begin_restart(st, up_to, "derived records gone");
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
            // Still marked refused: given, but the engine was never asked
            // to hold it.
            if !st.refused.remove(&k) {
                audio.resolved(k.0);
            }
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
        let at_once = rebuilds && (st.restart.is_none() || Self::late_restart_pending(st));
        let mut late: Option<(u64, u64)> = None;
        let mut unresolved = Vec::new();
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
                    // Still marked refused: `give` took it but the engine
                    // was never asked to hold it, so it has nothing to
                    // resolve.
                    if !st.refused.remove(&k) {
                        audio.resolved(index);
                    }
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
                unresolved.push((k, at));
                continue;
            }
            audio.appended(index, at, &record.entries);
        }
        // The cap is `sync_log`'s to apply, after its own part is asked.
        let given = self.give(st, unresolved);
        self.ask(st, given);
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
                add_slack(st, self.log.as_ref(), self.id, &s, current);
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
    /// While a restart begun so has not started its rebuild yet, as
    /// through the parts of one catch-up, it is moved past `after`
    /// instead: the core is dropped and rebuilt once.
    fn rebuild_past_late(&self, st: &mut State, first: u64, last: u64, after: u64) {
        if Self::late_restart_pending(st)
            && let Some(pending) = st.restart
        {
            st.restart = Some(pending.max(after));
            return;
        }
        st.counts.late_rebuilds += 1;
        tracing::debug!(first, last, after, "engine rebuilt past entries taken late");
        self.begin_restart(st, after, "entries taken late");
        st.late_restart = true;
    }

    /// Entries taken late that went unresolved while a restart was under
    /// way or the replica committed: once it ended, on a replica
    /// committing nothing, the engine is rebuilt past them. Whether a
    /// restart began. Not while a resync attempt runs: the restart would
    /// abandon the attempt and drop the engine's core in the middle of
    /// its hand-over, so the rebuild waits for the attempt to end. Every
    /// attempt ends, in a hand-over, a failure or its timeout, so the
    /// wait is bounded.
    fn fold_late(&self, st: &mut State) -> bool {
        if st.restart.is_some() || st.late_given.is_empty() || st.resync.running() {
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

    /// A restart begun by a rebuild past entries taken late is pending
    /// and its rebuild has not started.
    fn late_restart_pending(st: &State) -> bool {
        st.late_restart && st.restart.is_some() && matches!(st.rebuild, Rebuild::Idle)
    }

    fn rebuild_past(st: &mut State, k: Key) {
        st.rebuilt_past.insert(k);
        while st.rebuilt_past.len() > GIVEN_KEPT {
            st.rebuilt_past.pop_first();
        }
    }

    /// Whether this audio replica's engine has no output enabled, as of
    /// its last sample, nor the applied product state `logged` names one
    /// enabled (`ProductState::outputs`): every output either lists is
    /// disabled in both, or neither lists one. An output the engine does
    /// not list, as one `logged` names past its list, counts as enabled,
    /// and an output past `logged` as the engine holds it.
    fn writes_nothing(st: &State, logged: &[bool]) -> bool {
        let listed = st.own.standing.len().max(logged.len());
        !(0..listed)
            .any(|o| logged.get(o) == Some(&true) || st.enabled.get(o).copied().unwrap_or(true))
    }

    /// Whether this audio replica is starting, withdrawn or diverged with
    /// outputs none of which commits, as of its last sample. One taking
    /// the baseline over in a key hand-over, retargeted from
    /// `version_mismatch` and about to commit by the Named rule, holds
    /// what it took late, as the replica it takes over from did: rebuilt
    /// past them, it would return to `starting`, the leader would pass it
    /// over and name the old-key filler again, and the baseline would
    /// change hands twice.
    fn commits_nothing(st: &State) -> bool {
        let state = st.life.as_ref().map(Life::state);
        let standing = &st.own.standing;
        matches!(
            state,
            Some(ReplicaState::Starting | ReplicaState::Withdrawn | ReplicaState::Diverged)
        ) && !st.life.as_ref().is_some_and(Life::taking_over)
            && !standing.is_empty()
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

    /// Logs why the replica has been in `starting` for
    /// [`START_HOLD_WARN`], with the values that decide it.
    fn log_start_hold(&self, st: &State, hold: StartHold, now: Instant) {
        let held_s = st
            .start_watch
            .since
            .map_or(0.0, |s| since(now, s).as_secs_f64());
        match hold {
            StartHold::ClockUnverified => {
                // Each peer's clock minus this one's from its lowest
                // round trip, and that exchange's one-way delay.
                let peers: Vec<String> = (st.peers.iter())
                    .map(|(id, p)| match p.thetas.best() {
                        Some((theta, delay)) => format!(
                            "{id}: {:.3} ms +/- {:.3} ms",
                            theta as f64 / 1e6,
                            delay as f64 / 1e6
                        ),
                        None => format!("{id}: no exchange"),
                    })
                    .collect();
                tracing::warn!(
                    held_s,
                    tai_offset_s = ?st.own.clock.tai_offset_s,
                    clock_guard_ms = time_of(self.timing.clock_guard) as f64 / 1e6,
                    counted = ?self.counted(st, now),
                    peers = %peers.join(", "),
                    "a start is held: this replica's clock is not verified"
                );
            }
            StartHold::HorizonHeld => {
                let end = st.own.processed.map(|k| (k + 1) * self.timing.block);
                tracing::warn!(
                    held_s,
                    processed_end = ?end,
                    estimate = ?self.estimate_in(st, now),
                    apply_guard = index_at(self.timing.apply_guard_ns()),
                    "a start is held: the horizon holds this replica"
                );
            }
            StartHold::Applying { applied, needed } => tracing::warn!(
                held_s,
                applied,
                needed,
                "a start is held: the log has not applied the entries it needs"
            ),
            StartHold::Rebuilding => tracing::warn!(
                held_s,
                running = matches!(st.rebuild, Rebuild::Running { .. }),
                "a start is held: the engine core is being rebuilt from product state"
            ),
            StartHold::GenerationPending => tracing::warn!(
                held_s,
                "a start is held: the engine's pending generation is not built"
            ),
            StartHold::NotProcessing => {
                tracing::warn!(held_s, "a start is held: the engine has processed no block");
            }
        }
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
            self.begin_restart(st, self.log.applied(), "clock hold");
        }
        st.held = held;
        self.rebuild_ended(st, audio.as_ref());
        let rebuilding = match st.restart {
            Some(after) if !held && self.log.applied() >= after => {
                let done = self.rebuild(st, audio);
                if done {
                    st.restart = None;
                    st.late_restart = false;
                }
                !done
            }
            Some(_) => true,
            None => false,
        };
        let rebuilding = self.fold_late(st) || rebuilding;
        let applied = self.log.applied();
        let hold = if held && !st.verifier.verified() {
            Some(StartHold::ClockUnverified)
        } else if held {
            Some(StartHold::HorizonHeld)
        } else if let Some(needed) = st.restart.filter(|&after| applied < after) {
            Some(StartHold::Applying { applied, needed })
        } else if rebuilding {
            Some(StartHold::Rebuilding)
        } else if applied < st.known_committed {
            Some(StartHold::Applying {
                applied,
                needed: st.known_committed,
            })
        } else if !st.generation_ready {
            Some(StartHold::GenerationPending)
        } else if st.own.processed.is_none() {
            Some(StartHold::NotProcessing)
        } else {
            None
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
        let ready = applied >= st.known_committed && st.generation_ready && !rebuilding;
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
        let starting = state == ReplicaState::Starting;
        st.start_hold = hold.filter(|_| starting);
        if st
            .start_watch
            .observe(starting, st.start_hold.is_some(), now)
            && let Some(hold) = st.start_hold
        {
            st.counts.start_hold_warnings += 1;
            self.log_start_hold(st, hold, now);
        }
        // Without an engine core, or with one being rebuilt or replayed,
        // what the inputs read says nothing of their flows.
        if matches!(state, ReplicaState::Starting | ReplicaState::Resync) {
            st.own.readable = 0;
            st.own.unreadable = 0;
        }
        st.own.baseline = baseline.map(|b| (b.replica, b.at_index));
        // The `/readyz` term. A StatefulSet rolls on only past a Ready
        // pod, so readiness is the rollout's brake against a broken
        // release: a process that has not committed since it started is
        // Ready only once it commits. One that has is Ready while it or
        // another audio replica commits, whatever its state, so an old
        // pod that withdraws, hands the baseline over, resyncs or is
        // rebuilt never takes its readiness with it. A replica in
        // `version_mismatch` below the baseline's versions, as a pod
        // rolled back is, cannot commit while a newer one is live: it is
        // Ready while another commits, so a rollback goes on, and the
        // `below_baseline_versions` alert covers one that stops part way.
        // None is Ready while nobody commits. Naming reads no readiness,
        // so the set leaves those states on its own. A withdrawn replica
        // whose outputs are all disabled or absent writes nothing on any
        // replica, committed since its start or not: its engine follows
        // the log, so the outputs it reports disabled are the log's, and
        // it serves as one that commits, or a set without outputs would
        // never be Ready and its API never reached. Out of contact the
        // term keeps its last value for `DEPARTED`, so a blip in contact
        // never flips it; past that only the replica's own commit tells,
        // so a replica cut off from its set does not stay Ready.
        if state == ReplicaState::Committing {
            st.committed_since_start = true;
        }
        let below = st.life.as_ref().and_then(|l| l.metrics.mismatch)
            == Some(crate::state::Mismatch::BelowBaselineVersions);
        let nothing_to_write = state == ReplicaState::Withdrawn && Self::writes_nothing(st, &[]);
        let other = copies
            .iter()
            .any(|(_, age, own)| *age <= fresh && own.state == Some(ReplicaState::Committing));
        if st.in_contact {
            let follows = st.committed_since_start || below;
            st.serving =
                Some(state == ReplicaState::Committing || (other && follows) || nothing_to_write);
        } else if st.serving.is_some()
            && st
                .lost
                .is_some_and(|at| since(now, at) >= crate::rollout::DEPARTED)
        {
            st.serving = Some(state == ReplicaState::Committing);
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
            self.begin_restart(st, after, "purge-point verdict");
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

    /// The audio `/readyz` term (plan 2.11, Deployment): committing; or,
    /// once the replica committed since its process started, another
    /// audio replica committing within `w + D` by its reports, in any
    /// state (`starting`, `withdrawn`, `diverged`, `resync`,
    /// `version_mismatch`); or, before it committed, another committing
    /// while it is in `version_mismatch` below the baseline's versions.
    /// False while none commits. A withdrawn replica
    /// whose outputs are all disabled or absent writes nothing on any
    /// replica and serves as one that commits. Judged while in contact;
    /// out of contact kept for [`crate::rollout::DEPARTED`], then true
    /// only while committing; false before the first contact. Always true for
    /// the witness and for the only voter of its set; never for a
    /// learner, which commits nothing, a set's only voter writing the
    /// outputs alone.
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

    /// The Raft term this voter is in, every tick, leading or not: slack
    /// of an entry from an earlier term is not counted ([`add_slack`]),
    /// and a new term voids the slack the lead was kept on
    /// ([`Lead::new_term`]).
    pub fn in_term(&self, term: u64) {
        let mut st = self.lock();
        if term != st.office_term {
            st.office_term = term;
            st.lead.new_term();
            st.lead_raised_by = None;
        }
    }

    /// This voter does not lead ([`crate::baseline::Leader::step_down`]).
    pub fn not_leading(&self) {
        let mut st = self.lock();
        st.leader.step_down();
        st.clock.step_down();
        st.decider.step_down();
        st.liveness.step_down();
    }

    /// The leader's changes to the inputs' liveness that held for
    /// `pruneAfterBlocks` ([`crate::liveness`]), each once.
    pub fn input_liveness(&self) -> Vec<samplez::Report> {
        let blocks = u64::from(self.timing.prune_after_blocks) * self.timing.block;
        let debounce = Duration::from_nanos(time_of(blocks));
        self.lock().liveness.due(debounce, Instant::now())
    }

    /// The leader's decisions this period ([`crate::baseline`]): none
    /// while out of contact.
    pub fn decisions(&self, term: u64) -> Vec<Body> {
        let cpu = thread_cpu();
        let out = self.decisions_held(term);
        add_cpu(&self.costs.decisions_ns, cpu);
        out
    }

    fn decisions_held(&self, term: u64) -> Vec<Body> {
        let now = Instant::now();
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        if !st.in_contact || st.members.configs.is_empty() {
            st.liveness.step_down();
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
        let target = st.rollout.as_ref().and_then(|r| {
            let _product = crate::lock_watch::ProductCode::enter();
            r.target()
        });
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
        // Input liveness takes a copy by its own age alone, not from when
        // contact was regained: a copy from before a loss of contact says
        // nothing of what that voter reads now, and counts only once a
        // fresh one replaces it.
        let heard: Vec<Voter> = audio
            .iter()
            .map(|v| Voter {
                id: v.id,
                silent: copies.get(&v.id).is_none_or(|(age, _)| *age > fresh),
                own: v.own,
            })
            .collect();
        let (up, down) = crate::liveness::majority(&st.members.configs, &heard);
        let mut logged_up = u64::MAX;
        self.log.pending(&mut |_, body| match *body {
            Body::InputDown { slot, .. } if slot < 64 => logged_up &= !(1 << slot),
            Body::InputUp { slot, .. } if slot < 64 => logged_up |= 1 << slot,
            _ => {}
        });
        st.liveness.period(up, down, logged_up, now);
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
            // Its engine follows the log, so these are the log's outputs.
            nothing_to_write: !st.enabled.is_empty() && !st.enabled.contains(&true),
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
        #[cfg(test)]
        if st.horizon_always {
            st.horizon.hold();
        }
    }

    /// Unit tests that drive the log, not the clock: this replica counts
    /// as verified from its first period.
    #[cfg(test)]
    pub(crate) fn verified_always(&self) {
        self.lock().verified_always = true;
    }

    /// Reports and copies as before [`Carried`].
    #[cfg(test)]
    pub(crate) fn as_before(&self) {
        self.lock().as_before = true;
    }

    /// The page book, and the copies of every voter with their ages,
    /// without the page records nothing reads in a copy.
    #[cfg(test)]
    pub(crate) fn pages_and_copies(&self) -> (String, Vec<(NodeId, Duration, Own)>) {
        let st = self.lock();
        let copies = (self.copies(&st, Instant::now()).into_iter())
            .map(|(id, age, own)| {
                let mut own = own.clone();
                own.pages.clear();
                (id, age, own)
            })
            .collect();
        (format!("{:?}", st.pages), copies)
    }

    #[cfg(test)]
    pub(crate) fn latest_pages(&self, node: NodeId) -> Vec<crate::report::Page> {
        self.lock().pages.latest(node, PAGES_REPORTED)
    }

    #[cfg(test)]
    pub(crate) fn rebuild_every_offer(&self) {
        self.lock().offer_cache_off = true;
    }

    #[cfg(test)]
    pub(crate) fn offers_built(&self) -> u64 {
        self.lock().offers_built
    }

    #[cfg(test)]
    pub(crate) fn carried_len(&self, to: NodeId) -> usize {
        self.lock().carried.get(&to).map_or(0, Carried::len)
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
        let product = st.resync.due(now).then(|| self.log.product_state().2);
        if let Some(state) = &product {
            let off = (self.audio.as_ref())
                .is_some_and(|audio| !state.flows.is_empty() && !audio.follows(state));

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
                st.resync.rebuilt(now, crate::resync::Instead::Flows);
                tracing::warn!(
                    flows = ?state.flows,
                    "diverged with an input on another flow than the log names: rebuilt, not resynced"
                );
                self.begin_restart(st, self.log.applied(), "input on another flow");
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
        let mine = st.own.key;
        let peers_within = |max_age: Duration| {
            let peers: Vec<(NodeId, bool, &Own)> = (candidates.iter())
                .map(|&(id, age, own)| (id, age > max_age, own))
                .collect();
            peers
        };
        let fresh = self.timing.contact_window + self.timing.hold;
        let donor =
            crate::resync::donor(baseline, mine, &peers_within(2 * self.timing.report_period))
                .or_else(|| crate::resync::donor(baseline, mine, &peers_within(fresh)));
        // Every committing replica has another key: none ever donates a
        // core this engine takes. The engine is rebuilt from product
        // state instead, on the attempts' backoff.
        if donor.is_none()
            && crate::resync::other_keys_only(mine, &peers_within(fresh))
            && st.resync.due(now)
        {
            st.resync.rebuilt(now, crate::resync::Instead::Key);
            tracing::warn!(
                key = ?mine,
                "diverged with committing donors of other keys only: rebuilt, not resynced"
            );
            self.begin_restart(st, self.log.applied(), "no donor of its key");
            return None;
        }
        // Every output disabled, by the applied log and by the engine, or
        // none: nothing commits, so no donor comes, and nothing is written
        // for a rebuilt state to get wrong. The engine is rebuilt from
        // product state instead, on the attempts' backoff; what a rebuild
        // does not carry costs a page and a resync once an output is
        // enabled again and a peer commits.
        let logged = product.map(|state| state.outputs);
        if donor.is_none() && logged.is_some_and(|l| Self::writes_nothing(st, &l)) {
            st.resync
                .rebuilt(now, crate::resync::Instead::WritingNothing);
            tracing::warn!("diverged with every output disabled: rebuilt, not resynced");
            self.begin_restart(st, self.log.applied(), "every output disabled");
            return None;
        }
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
    /// entries as its own; a core refused for good rebuilds the engine
    /// from product state; anything else leaves it diverged, to try
    /// again.
    pub fn resync_ended(&self, epoch: u64, donor: NodeId, result: Result<Resynced, Failed>) {
        let Some(audio) = self.audio.clone() else {
            return;
        };
        let now = Instant::now();
        let mut st = self.lock();
        let st = &mut *st;
        let result = result.and_then(|r| {
            match crate::resync::mismatch(&r.pages, |o, q| st.pages.get(donor, o, q).copied()) {
                Some(q) => Err(Failed::Other(format!(
                    "replayed page {q} differs from the donor's"
                ))),
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
            // A missed record is logged where it was found, once per last
            // log id.
            Err(e @ Failed::Missed) => {
                tracing::debug!(donor, error = %e, "a resync failed");
                life.resync_failed();
                return;
            }
            // No core of this donor fits: rebuilt from product state, on
            // the attempts' backoff, rather than asked again.
            Err(e @ Failed::Refused(_)) => {
                st.resync.metrics.refused += 1;
                tracing::warn!(donor, error = %e, "a resync failed for good: rebuilt, not retried");
                self.begin_restart(st, self.log.applied(), "core refused for good");
                return;
            }
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
        // The handed-over core holds every one of them.
        st.refused.clear();
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
        if whole {
            return Ok(out);
        }
        // Each attempt fetches the newest core, which in a quiet set has
        // the same last id: logged only when it differs from the last one
        // missed ([`State::missed_after`]).
        let mut st = self.lock();
        if st.missed_after != Some(after) {
            st.missed_after = Some(after);
            tracing::warn!(
                last_id = after,
                records_from = self.log.records_from(),
                record_floor = ?self.log.record_floor(),
                "a record after the donor's core is gone"
            );
        }
        Err(Missed)
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
        let now = Instant::now();
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
            versions: st.own.versions,
            build_lead: st.own.build_lead,
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
                    silent_transitions: p.silent_transitions,
                    silent_gap_max: p.silent_gap_max,
                    silent: p.silent,
                    last_report_age: p.last.as_ref().map(|d| since(now, d.received)),
                    offset_ns: p.offset_ns,
                    clock_best: p.thetas.best(),
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
            start_hold: st.start_hold,
            start_hold_warnings: c.start_hold_warnings,
        }
    }
}

/// Where an entry is stamped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// `ceil(E / B) * B`: the log time dedup retention counts in.
    pub stamp: u64,
    /// The earliest `at_index`: the stamp plus the lead, or for a
    /// generation-needing entry plus the larger of the lead and buildLead.
    pub earliest: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StampRefusal {
    /// E counts no audio replica: nothing is stamped.
    NobodyCounted,
    /// [`FUTURE_LIST`] entries are pending.
    FutureListFull,
    /// The previous generation-needing entry's `at_index` has not passed on
    /// the baseline.
    GenerationPending,
}

/// The membership the metrics show, with the term they show entered into
/// `contact`. The metrics guard is released before `contact` is locked: a
/// read guard held while waiting on a busy lock would queue behind the
/// metrics writer every other reader then queues behind.
fn enter_term(
    contact: &Contact,
    metrics: &watch::Receiver<RaftMetrics<NodeId, BasicNode>>,
) -> Members {
    let (members, term) = {
        let seen = metrics.borrow();
        (Members::of(&seen), seen.current_term)
    };
    contact.in_term(term);
    members
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
                let members = enter_term(&contact, &metrics);
                let voters = members.voters();
                senders.retain(|id, _| voters.contains(id));
                // Like its silence metrics, a peer's link counts leave
                // with it, learners' kept while they are members.
                shared.peers.retain_links(&|id| members.addrs.contains_key(&id));
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
            let e = Failed::Other(format!("no core from {donor}: {e}"));
            contact.resync_ended(epoch, donor, Err(e));
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
        .unwrap_or_else(|e| Err(Failed::Other(format!("the replay panicked: {e}"))));
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
mod tests;
