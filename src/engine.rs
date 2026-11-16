//! The engine: start-up, the supervisor thread and the status it keeps.
//!
//! The supervisor is the one thread that makes instance-mutating libmxl
//! calls. It builds input generations and output writers, hands them to
//! the RT thread through the command ring, destroys what comes back
//! through the return ring, prunes and re-adds inputs, retries output
//! creation and turns telemetry into status and log lines.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release,
};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{Matrix, PlanarBuffer, Started, StripParam};
use samplez_mxl::input::{
    Counters as InputCounters, Generation, GroupWait, InputConfig, Poll, PolledFlow, Recovery,
    Timing,
};
use samplez_mxl::output::{self, Creation};
use samplez_mxl::{Instance, MxlApi, time};

use crate::blocks::{BlockStatus, Cause, Hold, PAGE, PageHash, Pages, StatusChange, Statuses};
use crate::config::Config;
use crate::meters::{OutputLoudness, OutputMeters};
use crate::rt::{self, ProcessStatus, RtStatus, Step, ThreadStatus};
use crate::rt_loop::{
    BlockVerdict, Change, Command, Control, Counters, Gen, LogId, LoudnessReset, MeterRecord, Mix,
    Out, OutMeter, PENDING_CHANGES, Replica, Returned, Rt, RtEntry, SLICE, Shared, Slot, SlotStats,
    Telemetry, UNRESOLVED,
};
use crate::transfer::{CopyError, Core, CoreCopy};

const COMMAND_RING: usize = 64;
const RETURN_RING: usize = 64;
const TELEMETRY_RING: usize = 8192;
/// Seconds of meter records the meter ring holds.
const METER_RING_SECONDS: usize = 2;
/// Loudness resets the reset ring holds per output.
const RESETS_PER_OUTPUT: usize = 4;
/// Status changes the change ring holds per output, and the fewest
/// status words per output.
const CHANGES_PER_OUTPUT: usize = 1024;
/// Page hashes the hash ring holds per output: 1.4 s of pages at 48 kHz.
const HASHES_PER_OUTPUT: usize = 64;
const MIN_STATUS_SLOTS: usize = 64;
const SUPERVISOR_TICK: Duration = Duration::from_millis(5);
const POLL_PERIOD: Duration = Duration::from_millis(100);
const BACKOFF_FIRST: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Seconds of slot statistics the margin warning looks at, plus one.
const MARGIN_WINDOW: usize = 11;
/// The build time assumed before a generation was built.
const UNMEASURED_BUILD: Duration = Duration::from_millis(100);
/// Quarter-octave buckets of the build-time histogram, up to 2^64 ns.
const BUILD_BUCKETS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputState {
    /// In the RT thread's generation.
    Attached,
    /// Not in the generation; polled until its head is on time.
    Pruned,
}

#[derive(Clone, Debug)]
pub struct InputStatus {
    pub name: String,
    pub state: InputState,
    /// The input's state in the log: up from an `InputUp`, down from an
    /// `InputDown` entry, applied by the RT thread.
    pub up: bool,
    /// The flow the input reads, changed by `Route` entries.
    pub flow_id: String,
    /// Why the last attach failed.
    pub error: Option<String>,
    /// The margin the producer's `maxCommitBatchSizeHint` needs, when the
    /// configured margin is below it.
    pub margin_short: Option<u64>,
    /// The margin the observed late blocks need: late blocks while the head
    /// advances at the nominal rate within 1 % over 10 s and some blocks
    /// are on time.
    pub margin_warning: Option<u64>,
    pub counters: InputCounters,
}

#[derive(Clone, Debug)]
pub struct OutputStatus {
    pub name: String,
    pub realised: bool,
    /// Why the output is not realised.
    pub reason: Option<String>,
    pub attempts: u64,
    /// Creations that opened a flow another writer created first.
    pub races: u64,
    /// `mxlCreateFlowWriter` failures after `mxlIsFlowActive` found no
    /// flow; libmxl reports any open() error of the check as not found.
    pub create_failures: u64,
    /// The flow's ring length and effective batch hints, once created.
    pub buffer_length: Option<u64>,
    pub hints: Option<(u32, u32)>,
    pub meters: OutputMeters,
}

/// The generation-needing entry (`InputUp`, `Route`) the engine builds or
/// holds, and the build times that set how far ahead such entries are
/// stamped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GenerationStatus {
    /// The log id of the entry; at most one generation is pending.
    pub pending: Option<LogId>,
    /// Its generation is built.
    pub ready: bool,
    /// Generations built.
    pub builds: u64,
    /// The 99.99th percentile of their build times (new instance,
    /// readers, ring pre-fault), ns.
    pub build_p9999_ns: Option<u64>,
    /// buildLead: samples a generation-needing entry is stamped ahead of
    /// the earliest index a gain change could take, covering that
    /// percentile and a supervisor tick.
    pub build_lead: u64,
}

/// The clocks as the engine sees them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClockStatus {
    /// The kernel TAI offset, CLOCK_TAI - CLOCK_REALTIME in whole seconds,
    /// as the RT thread sampled it last.
    pub tai_offset_s: Option<i64>,
    /// The kernel clock is unsynchronised (`adjtimex`: `TIME_ERROR` or
    /// `STA_UNSYNC`), sampled once a second; none when `adjtimex` failed.
    /// In replicated mode blocks then carry no digest.
    pub unsynchronised: Option<bool>,
    /// Clock steps detected, and whether one still fences the blocks
    /// after it (replicated mode).
    pub steps: u64,
    pub step_fenced: bool,
}

#[derive(Clone, Debug)]
pub struct Status {
    pub rt: RtStatus,
    pub clock: ClockStatus,
    pub generation: GenerationStatus,
    /// Why the output no longer follows from the inputs and the changes
    /// sent: the first log entry that reached the RT thread after its
    /// index, or whose generation it could not wait for.
    pub diverged: Option<String>,
    /// The message of a panic on the RT thread. The engine then stops
    /// processing: its state was released on the supervisor and every
    /// output flow deleted.
    pub rt_panic: Option<String>,
    pub inputs: Vec<InputStatus>,
    pub outputs: Vec<OutputStatus>,
}

#[derive(Debug)]
pub enum Error {
    Mxl(samplez_mxl::Error),
    Thread(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mxl(e) => write!(f, "{e}"),
            Error::Thread(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

/// Why [`Engine::set_gain`] or [`Engine::set_strip`] refused a change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GainError {
    /// No such coefficient, a gain that is not finite within
    /// `MAX_GAIN`, no such slot or a strip parameter it does not take, or
    /// an index that is not a multiple of the block size.
    Invalid(String),
    /// `at_index` is below `earliest`, the first index the engine accepts.
    TooNear { at_index: u64, earliest: u64 },
    /// [`PENDING_CHANGES`] entries are outstanding.
    Full,
}

impl std::fmt::Display for GainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GainError::Invalid(m) => write!(f, "{m}"),
            GainError::TooNear { at_index, earliest } => write!(
                f,
                "at_index {at_index} is below the earliest accepted index {earliest}"
            ),
            GainError::Full => write!(f, "{PENDING_CHANGES} entries are outstanding"),
        }
    }
}

impl std::error::Error for GainError {}

/// A log entry for the engine. Each sets an absolute target, so one
/// applied again once its ramp has ended changes nothing.
#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    /// Ramps the gain from input channel `input` to output channel
    /// `output` to `gain` over the ramp length.
    Gain {
        input: usize,
        output: usize,
        gain: f32,
    },
    /// Input slot `slot` is silent from the entry's index on, whatever is
    /// readable.
    InputDown { slot: usize },
    /// Input slot `slot` fades in from the entry's index, its reader
    /// recreated in a new generation.
    InputUp { slot: usize },
    /// Input slot `slot` crossfades to flow `flow_id` over the ramp
    /// length from the entry's index.
    Route { slot: usize, flow_id: String },
    /// Sets parameter `param` of input slot `slot`'s strip from the
    /// entry's index, ramping over the ramp length.
    Strip { slot: usize, param: StripParam },
}

/// Why the engine refused an entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryError {
    /// No such slot or coefficient, a gain that is not finite within
    /// `MAX_GAIN`, a flow an input may not read, a strip parameter the
    /// slot's strip does not take, an index that is not a multiple of the
    /// block size, or an entry the engine's log mode does not take.
    Invalid(String),
    /// [`PENDING_CHANGES`] entries are outstanding.
    Full,
    /// Log id `id` is not above the last one appended, `last`.
    Order { id: LogId, last: LogId },
}

impl std::fmt::Display for EntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryError::Invalid(m) => write!(f, "{m}"),
            EntryError::Full => write!(f, "{PENDING_CHANGES} entries are outstanding"),
            EntryError::Order { id, last } => {
                write!(f, "log id {id} is not above the last one appended, {last}")
            }
        }
    }
}

impl std::error::Error for EntryError {}

/// What the supervisor reports to the log that stamps `InputDown` and
/// `InputUp` entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// The input left the RT thread's generation: pruned, or not
    /// attachable when its generation was built.
    InputPruned { slot: usize },
    /// The input's head has been on time for a second while it is down.
    InputReadable { slot: usize },
}

/// Why [`Engine::reset_loudness`] refused a reset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResetError {
    /// No such output, or an index that is not a multiple of the block
    /// size.
    Invalid(String),
    /// `at_index` is below `earliest`, the first index the engine accepts.
    TooNear { at_index: u64, earliest: u64 },
    /// The loudness reset ring is full.
    Full,
}

impl std::fmt::Display for ResetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResetError::Invalid(m) => write!(f, "{m}"),
            ResetError::TooNear { at_index, earliest } => write!(
                f,
                "at_index {at_index} is below the earliest accepted index {earliest}"
            ),
            ResetError::Full => write!(f, "the loudness reset ring is full"),
        }
    }
}

impl std::error::Error for ResetError {}

/// A replicated engine's progress and clock as [`Engine::progress`] reads
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Progress {
    /// The highest block computed, its statuses set, none before the
    /// first.
    pub processed: Option<u64>,
    /// Clock steps detected, and those the replicated state cleared.
    pub steps: u64,
    pub steps_cleared: u64,
    /// The kernel TAI offset sampled last, in whole seconds; after a step,
    /// the sample after it.
    pub tai_offset_s: Option<i64>,
    /// The kernel clock is unsynchronised.
    pub self_report: bool,
    /// Status changes the RT thread's ring or the channel had no room for.
    pub status_changes_lost: u64,
    /// buildLead in samples ([`GenerationStatus::build_lead`]).
    pub build_lead: u64,
}

/// How an engine that follows a replicated log takes part in its set.
pub struct Replication {
    /// Voters of the set, the witness included. With one the engine
    /// commits each block at once and holds nothing; with more, every
    /// output starts committing nothing, holds blocks it cannot commit
    /// at once in a ring of `D + w + 2B` samples, commits them later in
    /// one write of at most that ring, and refuses a member flow whose
    /// ring is not twice that. A held block resolves on evidence
    /// (`commit_through`), when the replicated state withholds it, or by
    /// the ring's overflow: nothing in the engine sweeps it.
    pub voters: usize,
    /// Where [`Report`]s go, for the log to stamp; one the channel has no
    /// room for is counted in `reports_dropped`.
    pub reports: SyncSender<Report>,
    /// Where the block-status changes go, in the order the RT thread made
    /// them; one the channel has no room for is counted in
    /// `status_changes_unsent`, one the RT thread's ring had no room for in
    /// `status_changes_dropped`.
    pub statuses: Option<SyncSender<StatusChange>>,
    /// Where the page hashes go: with more than one voter, each page of
    /// each realised output the engine computed wholly, in every commit
    /// mode, once every block overlapping it is final. One the channel
    /// has no room for is counted in `page_hashes_unsent`, one the RT
    /// thread could not send in `page_hashes_dropped`. None, or one
    /// voter: no page is hashed.
    pub hashes: Option<SyncSender<PageHash>>,
}

/// Why the engine refused a replication control.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    /// No such output, or an engine that is not replicated.
    Invalid(String),
    /// The control ring is full, or [`UNRESOLVED`] entries are
    /// unresolved.
    Full,
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlError::Invalid(m) => write!(f, "{m}"),
            ControlError::Full => write!(f, "the control ring or the unresolved list is full"),
        }
    }
}

impl std::error::Error for ControlError {}

/// The engine side of the replicated state: the control ring to the RT
/// thread and the entries it holds unresolved.
struct ReplicaControl {
    ring: rtrb::Producer<Control>,
    unresolved: Vec<LogId>,
}

/// The off-RT side of core copies: at most one buffer is with the RT
/// thread at a time.
struct Transfer {
    cores: rtrb::Producer<Box<Core>>,
    copies: rtrb::Consumer<CoreCopy>,
    in_flight: bool,
    /// The buffer of a copy refused by the guard, for the next one.
    spare: Option<Box<Core>>,
    /// Input and output channels, input slots and the room for one
    /// page's entries: the shape of a core buffer.
    shape: (usize, usize, usize, usize),
    slack_blocks: u64,
}

/// What the supervisor learns of the log besides committed entries.
enum LogMsg {
    /// A generation-needing entry was appended: its generation is built
    /// on receipt. `flow` is a `Route`'s flow.
    Append {
        id: LogId,
        at_index: u64,
        change: Change,
        flow: Option<String>,
    },
    /// Uncommitted entries from `from` on were removed.
    Truncate { from: LogId },
    /// Uncommitted entry `id` was removed.
    Skip { id: LogId },
}

/// Appended entries not yet committed, and the producer of the RT
/// thread's entry ring. Committed entries go to the RT thread by log id.
struct Log {
    ring: rtrb::Producer<RtEntry>,
    sup: mpsc::Sender<LogMsg>,
    uncommitted: VecDeque<RtEntry>,
    last: LogId,
    committed: LogId,
    /// Entries pushed into the ring.
    submitted: u64,
    /// The engine's own log, absent when it follows a replicated one.
    local: Option<LocalLog>,
}

/// The engine's own log: per input slot whether its last `InputDown` or
/// `InputUp` leaves it up, and the latest index of an entry for it.
struct LocalLog {
    up: Vec<bool>,
    last_at: Vec<u64>,
    /// The index of the last generation-needing entry. The next one is
    /// stamped buildLead past it, as the replicated log's leader does, so
    /// its generation is built once the RT thread swapped this one in.
    gen_at: u64,
}

/// Makes the next `InputDown` or `InputUp` the engine's own log stamps
/// late: at an index the RT thread has produced.
#[cfg(test)]
static STAMP_LATE: AtomicBool = AtomicBool::new(false);

impl Log {
    /// Appends `e`; `consumed` is the RT thread's count of entries taken
    /// off its list, so committed entries never exceed what it holds.
    fn append(
        &mut self,
        e: RtEntry,
        flow: Option<String>,
        consumed: u64,
    ) -> Result<(), EntryError> {
        if e.id <= self.last {
            return Err(EntryError::Order {
                id: e.id,
                last: self.last,
            });
        }
        let outstanding = self.submitted - consumed + self.uncommitted.len() as u64;
        if outstanding >= PENDING_CHANGES as u64 {
            return Err(EntryError::Full);
        }
        if e.change.needs_generation() {
            // The supervisor outlives every caller.
            let _ = self.sup.send(LogMsg::Append {
                id: e.id,
                at_index: e.at_index,
                change: e.change,
                flow,
            });
        }
        self.uncommitted.push_back(e);
        self.last = e.id;
        Ok(())
    }

    fn commit(&mut self, up_to: LogId) {
        while let Some(e) = self.uncommitted.front().filter(|e| e.id <= up_to) {
            // append keeps the outstanding entries within the ring.
            if self.ring.push(*e).is_err() {
                unreachable!("the entry ring holds every outstanding entry");
            }
            self.submitted += 1;
            self.uncommitted.pop_front();
        }
        self.committed = self.committed.max(up_to.min(self.last));
    }

    /// Removes uncommitted entry `id`, and the generation built for it;
    /// false when no uncommitted entry has that id.
    fn skip(&mut self, id: LogId) -> bool {
        let before = self.uncommitted.len();
        self.uncommitted.retain(|e| e.id != id);
        let skipped = self.uncommitted.len() < before;
        if skipped {
            let _ = self.sup.send(LogMsg::Skip { id });
        }
        skipped
    }

    /// Removes the uncommitted entries from `from` on. Committed entries
    /// stay, and so do their generations: a `from` at or below the commit
    /// point removes those above it.
    fn truncate(&mut self, from: LogId) {
        let from = from.max(self.committed + 1);
        self.uncommitted.retain(|e| e.id < from);
        self.last = self.last.min(from - 1);
        let _ = self.sup.send(LogMsg::Truncate { from });
    }
}

/// Whether input slot `slot` exists and has a strip that takes `param`:
/// one of 1 to [`MAX_LEGS`] channels.
fn check_strip(config: &Config, slot: usize, param: &StripParam) -> Result<(), String> {
    let Some(input) = config.inputs.get(slot) else {
        return Err(format!("no input slot {slot}"));
    };
    let legs = input.channels as usize;
    if legs > MAX_LEGS {
        return Err(format!(
            "input slot {slot} has {legs} channels, a strip at most {MAX_LEGS}"
        ));
    }
    param.check(legs)
}

/// Where the next generation-needing or other entry for a slot may be
/// stamped: at `earliest`, after the latest one for the slot and its ramp.
fn stamp(earliest: u64, last_at: u64, ramp: u64, block: u64) -> u64 {
    earliest.max(last_at + ramp).next_multiple_of(block)
}

/// The first index an entry reaching the RT thread now is applied at, in
/// order: past what it may have produced, after the newest due block and
/// a backlog of `lead`.
fn earliest(api: &MxlApi, counters: &Counters, margin: u64, lead: u64) -> u64 {
    let produced = counters.produced.load(Acquire);
    let clock = time::timestamp_to_index(api, time::now(api));
    produced.max(clock.saturating_sub(margin)) + lead
}

/// buildLead: `lead` plus the 99.99th percentile of the build times, or
/// [`UNMEASURED_BUILD`] before any build, and a supervisor tick, in
/// samples, rounded up to a block.
fn build_lead_samples(p9999_ns: Option<u64>, lead: u64, block: u64) -> u64 {
    let ns =
        p9999_ns.unwrap_or(UNMEASURED_BUILD.as_nanos() as u64) + SUPERVISOR_TICK.as_nanos() as u64;
    let samples = (u128::from(ns) * samplez_mxl::SAMPLE_RATE as u128).div_ceil(1_000_000_000);
    (samples as u64 + lead).next_multiple_of(block)
}

/// Generation build times in quarter-octave buckets of nanoseconds.
struct BuildTimes {
    counts: Vec<u64>,
    total: u64,
}

impl BuildTimes {
    fn new() -> BuildTimes {
        BuildTimes {
            counts: vec![0; BUILD_BUCKETS],
            total: 0,
        }
    }

    fn add(&mut self, ns: u64) {
        let b = (4.0 * (ns.max(1) as f64).log2()) as usize;
        self.counts[b.min(BUILD_BUCKETS - 1)] += 1;
        self.total += 1;
    }

    /// The upper edge of the bucket holding the 99.99th percentile.
    fn p9999(&self) -> Option<u64> {
        let rank = (self.total as f64 * 0.9999).ceil() as u64;
        let mut seen = 0;
        for (b, &n) in self.counts.iter().enumerate() {
            seen += n;
            if n > 0 && seen >= rank {
                return Some(2f64.powf((b + 1) as f64 / 4.0).ceil() as u64);
            }
        }
        None
    }
}

/// Whether a copy of history rings with `slack` blocks of slack, read
/// after the RT thread handed out the core after block `k0`, is intact:
/// the replay of block `k0 + 1` needs the span ending at that block's
/// start, first overwritten by block `k0 + 1 + slack`.
fn history_kept(started: &Started, k0: u64, slack: u64) -> Result<(), u64> {
    started.confirms(k0 + 1, slack)
}

/// A running engine. Dropping it stops the engine and releases its
/// writers, which deletes their flows.
pub struct Engine {
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
    status: Arc<Mutex<Status>>,
    log: Arc<Mutex<Log>>,
    api: Arc<MxlApi>,
    block: u64,
    margin: u64,
    /// Samples between the newest index the RT thread may have produced
    /// and the earliest `at_index` accepted.
    lead: u64,
    /// buildLead in samples, kept by the supervisor.
    build_lead: Arc<AtomicU64>,
    shape: (usize, usize),
    resets: Mutex<rtrb::Producer<LoudnessReset>>,
    config: Config,
    /// Replicated mode with more than one voter.
    replica: Option<(Arc<Shared>, Mutex<ReplicaControl>)>,
    /// Core copies, in replicated mode with more than one voter.
    transfer: Option<Mutex<Transfer>>,
}

impl Engine {
    /// Starts the RT thread and the supervisor with the engine's own log:
    /// [`Engine::set_gain`], [`Engine::set_strip`] and [`Engine::route`]
    /// append and commit at once, and so does the supervisor for its own
    /// [`Report`]s. `process` is the outcome of [`rt::setup_process`], run
    /// before any other thread. Each block's verdict goes to `verdicts`
    /// when given; one the channel has no room for is counted in
    /// `verdicts_dropped`.
    pub fn start(
        config: Config,
        process: ProcessStatus,
        verdicts: Option<SyncSender<BlockVerdict>>,
    ) -> Result<Engine, Error> {
        Engine::start_with(config, process, verdicts, None)
    }

    /// [`Engine::start`] for an engine that follows a replicated log: its
    /// entries come through [`Engine::append`], [`Engine::commit`] and
    /// [`Engine::truncate`], and it takes part in the set as
    /// `replication` says. With more than one voter, each block of each
    /// output gets a status, and an output commits only from a commit
    /// start ([`Engine::commit_from`]) and while F
    /// ([`Engine::set_fence`]) has passed its blocks.
    pub fn start_replicated(
        config: Config,
        process: ProcessStatus,
        verdicts: Option<SyncSender<BlockVerdict>>,
        replication: Replication,
    ) -> Result<Engine, Error> {
        Engine::start_with(config, process, verdicts, Some(replication))
    }

    fn start_with(
        config: Config,
        process: ProcessStatus,
        verdicts: Option<SyncSender<BlockVerdict>>,
        replication: Option<Replication>,
    ) -> Result<Engine, Error> {
        let (reports, status_tx, voters, hash_tx) = match replication {
            Some(r) => (Some(r.reports), r.statuses, r.voters, r.hashes),
            None => (None, None, 0, None),
        };
        let api = samplez_mxl::load(&config.libmxl).map_err(Error::Mxl)?;
        let b = u64::from(config.block_size);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        counters.tai_offset_s.store(i64::MIN, Relaxed);
        let (cmd_tx, cmd_rx) = rtrb::RingBuffer::new(COMMAND_RING);
        let (ret_tx, ret_rx) = rtrb::RingBuffer::new(RETURN_RING);
        let (mut tel_tx, mut tel_rx) = rtrb::RingBuffer::new(TELEMETRY_RING);
        // The ring is allocated after mlockall: without MCL_FUTURE its
        // pages would fault in on the RT thread's first pushes. Every slot
        // is written once here instead.
        let blank = || Telemetry::ClockStep {
            delta_ns: 0,
            offset_before: 0,
            offset_after: 0,
        };
        while tel_tx.push(blank()).is_ok() {}
        while tel_rx.pop().is_ok() {}
        let (entry_tx, entry_rx) = rtrb::RingBuffer::new(PENDING_CHANGES);
        let (log_tx, log_rx) = mpsc::channel();
        let log = Arc::new(Mutex::new(Log {
            ring: entry_tx,
            sup: log_tx,
            uncommitted: VecDeque::new(),
            last: 0,
            committed: 0,
            submitted: 0,
            local: reports.is_none().then(|| LocalLog {
                up: vec![true; config.inputs.len()],
                last_at: vec![0; config.inputs.len()],
                gen_at: 0,
            }),
        }));
        let records = METER_RING_SECONDS
            * (samplez_mxl::SAMPLE_RATE as usize / SLICE as usize)
            * config.output_channels();
        let (mut meter_tx, mut meter_rx) = rtrb::RingBuffer::new(records.max(1));
        let blank = MeterRecord {
            seq: 0,
            output: 0,
            channel: 0,
            slice: 0,
            frames: 0,
            peak: 0.0,
            true_peak: 0.0,
            sum_sq: 0.0,
            plain_sum_sq: 0.0,
            reset: false,
        };
        while meter_tx.push(blank).is_ok() {}
        while meter_rx.pop().is_ok() {}
        let (reset_tx, reset_rx) =
            rtrb::RingBuffer::new((RESETS_PER_OUTPUT * config.outputs.len()).max(1));
        let hold_blocks = config.hold_blocks();
        let alone = voters == 1;
        let hash_tx = hash_tx.filter(|_| reports.is_some() && !alone);
        // Room for one page's entries, for its fingerprint; a core buffer
        // takes the same, so the RT thread's copy allocates nothing.
        let page_entries = if hash_tx.is_some() {
            PENDING_CHANGES * PAGE.div_ceil(b) as usize
        } else {
            0
        };
        let mut transfer = None;
        let (replica, rt_replica, change_rx, hash_rx) = if reports.is_some() {
            // A page's first settle reads the status of each of its
            // blocks, up to a page back, while the supervisor may set
            // statuses ahead: the ring holds two pages of blocks.
            let page_blocks = 2 * PAGE.div_ceil(b) as usize;
            let slots = (4 * hold_blocks)
                .max(page_blocks)
                .next_power_of_two()
                .max(MIN_STATUS_SLOTS);
            let shared = Arc::new(Shared {
                fence: AtomicU64::new(if alone { u64::MAX } else { 0 }),
                rank: AtomicU64::new(0),
                self_report: AtomicBool::new(false),
                steps_cleared: AtomicU64::new(0),
                statuses: (0..config.outputs.len())
                    .map(|_| Statuses::new(slots, b))
                    .collect(),
                started: Started::new(),
            });
            let changes = (CHANGES_PER_OUTPUT * config.outputs.len()).max(1);
            let (mut change_tx, mut change_rx) = rtrb::RingBuffer::new(changes);
            // Written once here: the RT thread's first pushes fault no page.
            while change_tx
                .push(StatusChange::Skipped { from: 0, to: 0 })
                .is_ok()
            {}
            while change_rx.pop().is_ok() {}
            let hashes = if hash_tx.is_some() {
                HASHES_PER_OUTPUT * config.outputs.len()
            } else {
                0
            };
            let (mut hashes_tx, mut hashes_rx) = rtrb::RingBuffer::new(hashes.max(1));
            let blank = PageHash {
                output: 0,
                page: 0,
                hash: 0,
                fingerprint: 0,
                committed: false,
            };
            while hashes_tx.push(blank).is_ok() {}
            while hashes_rx.pop().is_ok() {}
            let (control_tx, control_rx) = rtrb::RingBuffer::new(UNRESOLVED);
            let (cores_tx, cores_rx) = rtrb::RingBuffer::new(1);
            let (copies_tx, copies_rx) = rtrb::RingBuffer::new(1);
            transfer = (!alone).then(|| {
                Mutex::new(Transfer {
                    cores: cores_tx,
                    copies: copies_rx,
                    in_flight: false,
                    spare: None,
                    shape: (
                        config.input_channels(),
                        config.output_channels(),
                        config.inputs.len(),
                        page_entries,
                    ),
                    slack_blocks: config.history_slack_blocks(),
                })
            });
            let rt_replica = Replica {
                shared: Arc::clone(&shared),
                changes: change_tx,
                control: control_rx,
                unresolved: Vec::with_capacity(UNRESOLVED),
                ids: Vec::with_capacity(UNRESOLVED + PENDING_CHANGES),
                late_routes: Vec::with_capacity(PENDING_CHANGES),
                late_lost_until: 0,
                hashes: hashes_tx,
                cores: cores_rx,
                copies: copies_tx,
            };
            let control = ReplicaControl {
                ring: control_tx,
                unresolved: Vec::with_capacity(UNRESOLVED),
            };
            (
                Some((shared, Mutex::new(control))),
                Some(rt_replica),
                Some(change_rx),
                hash_tx.is_some().then_some(hashes_rx),
            )
        } else {
            (None, None, None, None)
        };

        let mut in_domains: Vec<usize> = Vec::new();
        let mut slots = Vec::new();
        let mut channel = 0;
        for i in &config.inputs {
            let d = config.domain(&i.domain).expect("validated");
            let domain = in_domains.iter().position(|&x| x == d).unwrap_or_else(|| {
                in_domains.push(d);
                in_domains.len() - 1
            });
            slots.push(Slot::new(
                domain,
                channel,
                i.channels as usize,
                i.offset_samples,
                config.ramp_samples,
            ));
            channel += i.channels as usize;
        }
        let mut outs = Vec::new();
        let mut channel = 0;
        for (n, o) in config.outputs.iter().enumerate() {
            outs.push(Out {
                first_channel: channel,
                channels: o.channels as usize,
                writer: None,
                first_k: 0,
                meter: OutMeter::new(o.channels as usize),
                hold: if replica.is_some() {
                    Hold::new(o.channels as usize, b, hold_blocks, alone)
                } else {
                    Hold::new(o.channels as usize, b, 0, true)
                },
                pages: hash_tx
                    .is_some()
                    .then(|| Pages::new(n as u32, o.channels as usize, b, hold_blocks)),
            });
            channel += o.channels as usize;
        }
        let (n, m) = (config.input_channels(), config.output_channels());
        let mut matrix = Matrix::new(n, m);
        for (i, row) in config.matrix.iter().enumerate() {
            for (j, &g) in row.iter().enumerate() {
                assert!(matrix.set(i, j, g, 0), "validated gain");
            }
        }
        let widest = config
            .inputs
            .iter()
            .map(|i| i.channels)
            .chain(config.outputs.iter().map(|o| o.channels))
            .max()
            .unwrap_or(0) as usize;
        let rt_state = Rt {
            api: Arc::clone(&api),
            block: b,
            margin: u64::from(config.reader_margin_samples),
            max_catchup: u64::from(config.max_catchup_blocks),
            ramp: config.ramp_samples,
            gens: (0..in_domains.len()).map(|_| None).collect(),
            statuses: (0..in_domains.len()).map(|_| GroupWait::Skipped).collect(),
            slots,
            outs,
            mix: Mix {
                page_entries: Vec::with_capacity(page_entries),
                ..Mix::new(matrix, b, config.ramp_samples)
            },
            inbuf: PlanarBuffer::new(n, b as usize),
            outbuf: PlanarBuffer::new(m, b as usize),
            scratch: vec![0.0; widest * b as usize],
            scratch_out: vec![0.0; widest * b as usize],
            pending_gen: None,
            awaiting_gen: Vec::with_capacity(PENDING_CHANGES),
            last_entry: 0,
            cmd: cmd_rx,
            entries: entry_rx,
            ret: ret_tx,
            tel: tel_tx,
            meters: meter_tx,
            resets: reset_rx,
            meter_seq: 0,
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
            replica: rt_replica,
            step_at: None,
        };

        let (thread_tx, thread_rx) = mpsc::sync_channel::<ThreadStatus>(1);
        let rt_cfg = config.rt.clone();
        let rt_thread = std::thread::Builder::new()
            .name("samplez-rt".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || {
                let _ = thread_tx.send(rt::setup_thread(&rt_cfg));
                drop(thread_tx);
                rt_state.run()
            })
            .map_err(|e| Error::Thread(format!("spawning the RT thread: {e}")))?;
        let thread = thread_rx
            .recv()
            .map_err(|_| Error::Thread("the RT thread ended during set-up".to_owned()))?;

        let status = Arc::new(Mutex::new(Status {
            rt: RtStatus {
                process: process.clone(),
                thread,
            },
            clock: ClockStatus::default(),
            generation: GenerationStatus::default(),
            diverged: None,
            rt_panic: None,
            inputs: config
                .inputs
                .iter()
                .map(|i| InputStatus {
                    name: i.name.clone(),
                    state: InputState::Pruned,
                    up: true,
                    flow_id: i.flow_id.clone(),
                    error: None,
                    margin_short: None,
                    margin_warning: None,
                    counters: InputCounters::default(),
                })
                .collect(),
            outputs: config
                .outputs
                .iter()
                .map(|o| OutputStatus {
                    name: o.name.clone(),
                    realised: false,
                    reason: Some("not created yet".to_owned()),
                    attempts: 0,
                    races: 0,
                    create_failures: 0,
                    buffer_length: None,
                    hints: None,
                    meters: OutputMeters::default(),
                })
                .collect(),
        }));
        let now = Instant::now();
        // Without MCL_FUTURE the pages of a new generation's rings are not
        // locked; they are read once on the supervisor before handover.
        let prefault = !matches!(process.mlock, Step::Active(_));
        let margin = u64::from(config.reader_margin_samples);
        let lead = (u64::from(config.max_catchup_blocks) + 2) * b;
        let build_lead = Arc::new(AtomicU64::new(build_lead_samples(None, lead, b)));
        let config_loudness = config
            .outputs
            .iter()
            .map(|o| {
                let w = o
                    .loudness_weights()
                    .expect("output checked by Config::parse");
                OutputLoudness::new(w)
            })
            .collect();
        let sup = Supervisor {
            api: Arc::clone(&api),
            timing: Timing {
                block: b,
                margin: u64::from(config.reader_margin_samples),
                max_catchup_blocks: u64::from(config.max_catchup_blocks),
                // A lone voter has no donor to resync from.
                resync_budget: if reports.is_some() && !alone {
                    config.resync_budget_samples()
                } else {
                    0
                },
            },
            prefault,
            in_domains: in_domains.clone(),
            slots: config
                .inputs
                .iter()
                .map(|i| SupSlot {
                    state: SupState::Wanted,
                    window: VecDeque::new(),
                    flow: i.flow_id.clone(),
                    up: true,
                    reported: false,
                    prune_unsent: false,
                    routing: None,
                })
                .collect(),
            jobs: VecDeque::new(),
            log_rx,
            log: Arc::clone(&log),
            reports,
            build_times: BuildTimes::new(),
            build_lead: Arc::clone(&build_lead),
            lead,
            ramp: u64::from(config.ramp_samples),
            dirty: vec![true; in_domains.len()],
            pollers: (0..in_domains.len()).map(|_| None).collect(),
            poll_errors: vec![None; in_domains.len()],
            rebuild_errors: vec![None; in_domains.len()],
            outs: (0..config.outputs.len())
                .map(|_| SupOut {
                    realised: false,
                    next_try: now,
                    backoff: BACKOFF_FIRST,
                })
                .collect(),
            out_instances: (0..config.domains.len()).map(|_| None).collect(),
            config: config.clone(),
            cmd: cmd_tx,
            ret: ret_rx,
            tel: tel_rx,
            meter_rx,
            next_meter_seq: 0,
            change_rx,
            status_tx,
            hash_rx,
            hash_tx,
            shared: replica.as_ref().map(|r| Arc::clone(&r.0)),
            alone,
            batch: if replica.is_some() && !alone {
                hold_blocks as u64 * b
            } else {
                b
            },
            loudness: config_loudness,
            verdicts,
            status: Arc::clone(&status),
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
        };
        // The RT thread's handle and the receiver of its set-up channel stay
        // here until the supervisor runs: if that spawn fails, the RT thread
        // is joined and its state released on this thread, never dropped
        // on the RT thread itself. The receiver is dropped only after the
        // join, so the RT thread's sender is never the last reference:
        // dropping that frees the channel on whichever side lets go last.
        let (handle_tx, handle_rx) = mpsc::sync_channel(1);
        let supervisor = std::thread::Builder::new()
            .name("samplez-supervisor".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || {
                let (rt_thread, thread_rx) = handle_rx.recv().expect("sent once spawned");
                sup.run(rt_thread);
                drop(thread_rx);
            });
        let supervisor = match supervisor {
            Ok(s) => {
                handle_tx
                    .send((rt_thread, thread_rx))
                    .expect("the supervisor receives it");
                s
            }
            Err(e) => {
                stop.store(true, Relaxed);
                let _ = rt_thread.join();
                drop(thread_rx);
                return Err(Error::Thread(format!("spawning the supervisor: {e}")));
            }
        };
        Ok(Engine {
            stop,
            supervisor: Some(supervisor),
            counters,
            status,
            log,
            api,
            block: b,
            margin,
            lead,
            build_lead,
            shape: (n, m),
            resets: Mutex::new(reset_tx),
            config,
            replica,
            transfer,
        })
    }

    pub fn status(&self) -> Status {
        self.status.lock().expect("status lock").clone()
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Ramps the gain from input channel `input` to output channel
    /// `output` to `gain` over the configured ramp length, starting at
    /// sample index `at_index`, a multiple of the block size. Changes at
    /// one index apply in the order sent.
    ///
    /// A change is refused, never dropped later, when `at_index` is below
    /// `max(produced, mxlTimestampToIndex(now) - margin) + (maxCatchupBlocks
    /// + 2)B`, `produced` being the index of the next sample the matrix
    /// produces: the RT thread takes changes once per wake and may then
    /// process a backlog of up to `maxCatchupBlocks` blocks and the next
    /// one. It is refused with [`GainError::Full`] while
    /// [`PENDING_CHANGES`] entries are outstanding, sent but not yet
    /// applied. A change that still reaches the RT thread after its index,
    /// after an overrun or a clock step, is counted in `command_late`,
    /// not applied, and marks the engine diverged in [`Status`].
    ///
    /// An engine that follows a replicated log refuses it as invalid.
    pub fn set_gain(
        &self,
        input: usize,
        output: usize,
        gain: f32,
        at_index: u64,
    ) -> Result<(), GainError> {
        if input >= self.shape.0 || output >= self.shape.1 {
            return Err(GainError::Invalid(format!(
                "no matrix coefficient {input} -> {output}"
            )));
        }
        if !samplez_dsp::valid_gain(gain) {
            return Err(GainError::Invalid(format!(
                "gain {gain} is not finite within MAX_GAIN"
            )));
        }
        let change = Change::Gain {
            input,
            output,
            gain,
        };
        self.set_local(change, at_index, "gain changes")
    }

    /// Sets parameter `param` of input slot `slot`'s strip from sample
    /// index `at_index`, a multiple of the block size, ramping over the
    /// configured ramp length. Refused as [`Engine::set_gain`] refuses a
    /// change, and as invalid for a slot that does not exist or has more
    /// than [`MAX_LEGS`] channels, and for a parameter its strip refuses
    /// ([`StripParam::check`]).
    pub fn set_strip(
        &self,
        slot: usize,
        param: StripParam,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_strip(&self.config, slot, &param).map_err(GainError::Invalid)?;
        self.set_local(Change::Strip { slot, param }, at_index, "strip changes")
    }

    /// Appends `change` at `at_index` to the engine's own log and commits
    /// it; `what` names the changes in the refusal of a replicated engine.
    fn set_local(&self, change: Change, at_index: u64, what: &str) -> Result<(), GainError> {
        if !at_index.is_multiple_of(self.block) {
            return Err(GainError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
        let mut log = self.log.lock().expect("log lock");
        if log.local.is_none() {
            return Err(GainError::Invalid(format!(
                "{what} come from the replicated log"
            )));
        }
        let earliest = earliest(&self.api, &self.counters, self.margin, self.lead);
        if at_index < earliest {
            return Err(GainError::TooNear { at_index, earliest });
        }
        let e = RtEntry {
            id: log.last + 1,
            at_index,
            change,
        };
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(e, None, consumed).map_err(|_| GainError::Full)?;
        log.commit(e.id);
        Ok(())
    }

    /// Re-routes input slot `slot` to flow `flow_id`: a new generation
    /// holding both flows' readers is built, and the slot crossfades from
    /// the old flow to the new one over the ramp length from the returned
    /// index, `at_index` or, when that is nearer, the earliest index a
    /// generation-needing entry is stamped at: buildLead past the earliest
    /// gain change and past the engine's last `InputUp` or `Route`, so one
    /// generation is built at a time and each before its index. A flow
    /// that cannot be read is silent and reported as pruned. Refused as
    /// invalid on an engine that follows a replicated log.
    pub fn route(&self, slot: usize, flow_id: &str, at_index: u64) -> Result<u64, EntryError> {
        self.config
            .check_input_flow(slot, flow_id)
            .map_err(|e| EntryError::Invalid(e.0))?;
        let mut log = self.log.lock().expect("log lock");
        let (last_at, gen_at) = match &log.local {
            Some(l) => (l.last_at[slot], l.gen_at),
            None => {
                return Err(EntryError::Invalid(
                    "routes come from the replicated log".to_owned(),
                ));
            }
        };
        let build_lead = self.build_lead.load(Relaxed);
        let earliest = (earliest(&self.api, &self.counters, self.margin, self.lead) - self.lead
            + build_lead)
            .max(gen_at + build_lead);
        let ramp = u64::from(self.config.ramp_samples);
        let at_index = at_index.max(stamp(earliest, last_at, ramp, self.block));
        let e = RtEntry {
            id: log.last + 1,
            at_index,
            change: Change::Route { slot },
        };
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(e, Some(flow_id.to_owned()), consumed)?;
        log.commit(e.id);
        if let Some(l) = &mut log.local {
            l.last_at[slot] = at_index;
            l.gen_at = at_index;
        }
        Ok(at_index)
    }

    /// Appends entry `entry` at log id `id`, above every id appended
    /// before, to apply at `at_index`, a multiple of the block size. An
    /// `InputUp` or `Route` has its generation built at once; at most one
    /// is built at a time, the next once the RT thread swapped in the one
    /// before. Committed entries go to the RT thread, which applies each
    /// at its index or, past it, counts it in `command_late` and marks the
    /// engine diverged. Refused as invalid on an engine with its own log.
    pub fn append(&self, id: LogId, at_index: u64, entry: Entry) -> Result<(), EntryError> {
        let invalid = |m: String| Err(EntryError::Invalid(m));
        if !at_index.is_multiple_of(self.block) {
            return invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            ));
        }
        let slots = self.config.inputs.len();
        let (change, flow) = match entry {
            Entry::Gain {
                input,
                output,
                gain,
            } => {
                if input >= self.shape.0 || output >= self.shape.1 {
                    return invalid(format!("no matrix coefficient {input} -> {output}"));
                }
                if !samplez_dsp::valid_gain(gain) {
                    return invalid(format!("gain {gain} is not finite within MAX_GAIN"));
                }
                (
                    Change::Gain {
                        input,
                        output,
                        gain,
                    },
                    None,
                )
            }
            Entry::InputDown { slot } | Entry::InputUp { slot } | Entry::Route { slot, .. }
                if slot >= slots =>
            {
                return invalid(format!("no input slot {slot}"));
            }
            Entry::InputDown { slot } => (Change::InputDown { slot }, None),
            Entry::InputUp { slot } => (Change::InputUp { slot }, None),
            Entry::Route { slot, flow_id } => {
                self.config
                    .check_input_flow(slot, &flow_id)
                    .map_err(|e| EntryError::Invalid(e.0))?;
                (Change::Route { slot }, Some(flow_id))
            }
            Entry::Strip { slot, param } => {
                check_strip(&self.config, slot, &param).map_err(EntryError::Invalid)?;
                (Change::Strip { slot, param }, None)
            }
        };
        let mut log = self.log.lock().expect("log lock");
        if log.local.is_some() {
            return invalid("the engine keeps its own log".to_owned());
        }
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(
            RtEntry {
                id,
                at_index,
                change,
            },
            flow,
            consumed,
        )
    }

    /// Commits the appended entries up to log id `up_to`: they go to the
    /// RT thread, in log order.
    pub fn commit(&self, up_to: LogId) {
        self.log.lock().expect("log lock").commit(up_to);
    }

    /// Replicated mode: the appended entry `id` is never applied, and its
    /// generation is dropped: its commit was not known by its apply
    /// deadline, so the log decides it. False when no uncommitted entry has
    /// that id: it was committed already, or never appended.
    pub fn skip(&self, id: LogId) -> bool {
        self.log.lock().expect("log lock").skip(id)
    }

    /// Removes the uncommitted entries from log id `from` on, and drops
    /// the generations built for them; committed entries are kept.
    pub fn truncate(&self, from: LogId) {
        self.log.lock().expect("log lock").truncate(from);
    }

    fn replica(&self) -> Result<&(Arc<Shared>, Mutex<ReplicaControl>), ControlError> {
        self.replica
            .as_ref()
            .ok_or_else(|| ControlError::Invalid("the engine is not replicated".to_owned()))
    }

    fn output_index(&self, output: usize) -> Result<(), ControlError> {
        if output < self.config.outputs.len() {
            Ok(())
        } else {
            Err(ControlError::Invalid(format!("no output {output}")))
        }
    }

    fn control(&self, c: Control) -> Result<(), ControlError> {
        let (_, control) = self.replica()?;
        let mut control = control.lock().expect("control lock");
        match c {
            Control::Unresolved { id, .. } => {
                if control.unresolved.contains(&id) {
                    return Ok(());
                }
                if control.unresolved.len() >= UNRESOLVED || control.ring.slots() == 0 {
                    return Err(ControlError::Full);
                }
                control.unresolved.push(id);
            }
            Control::Resolved { id } => {
                if !control.unresolved.contains(&id) {
                    return Ok(());
                }
                if control.ring.slots() == 0 {
                    return Err(ControlError::Full);
                }
                control.unresolved.retain(|&u| u != id);
            }
            _ => {}
        }
        control.ring.push(c).map_err(|_| ControlError::Full)
    }

    /// Replicated mode: publishes F, the end index of the highest block up
    /// to which every block not yet final is B1-valid (plan 2.11,
    /// Fencing). A block ending above F is `fenced`; once F passes it, it
    /// is committed, or held at the rank published then. F starts at 0
    /// with more than one voter, and past every block with one.
    pub fn set_fence(&self, end: u64) -> Result<(), ControlError> {
        self.replica()?.0.fence.store(end, Release);
        Ok(())
    }

    /// Replicated mode: this replica's rank, 0 the top, fixed into each
    /// block's status when F passes it; at most [`crate::blocks::MAX_RANK`].
    pub fn set_rank(&self, rank: u8) -> Result<(), ControlError> {
        self.replica()?.0.rank.store(u64::from(rank), Relaxed);
        Ok(())
    }

    /// Replicated mode: output `output` commits again from its commit
    /// start, the first page boundary P in a block at or after `min_k`
    /// it computes (with one voter, from that block's start). The block
    /// holding P reports `committed from P`, and `[kB, P)` withheld
    /// without a cause.
    pub fn commit_from(&self, output: usize, min_k: u64) -> Result<(), ControlError> {
        self.output_index(output)?;
        self.control(Control::CommitFrom { output, min_k })
    }

    /// Replicated mode: output `output` stops committing at sample index
    /// `at`: the block holding it commits `[kB, at)`, reported `committed
    /// up to at`, withholds `[at, (k + 1)B)` without a cause, and the
    /// output is withdrawn from there.
    pub fn stop_at(&self, output: usize, at: u64) -> Result<(), ControlError> {
        self.output_index(output)?;
        self.control(Control::StopAt { output, at })
    }

    /// Replicated mode: the replica is withdrawn on every output that
    /// commits: from now on it commits through only, from the next page
    /// boundary on.
    pub fn withdraw(&self) -> Result<(), ControlError> {
        self.control(Control::Withdraw)
    }

    /// Replicated mode: moves block `k` of output `output` to `withheld`
    /// for `cause`, from no status yet, `fenced` or `held`; the RT thread
    /// never writes it then. Returns the status the block ends with,
    /// `committed` when the RT thread committed it first, or none when
    /// nothing was recorded: the slot still holds another block's open
    /// status, which is settled first ([`crate::blocks::Statuses::withhold`]).
    pub fn withhold(
        &self,
        output: usize,
        k: u64,
        cause: Cause,
    ) -> Result<Option<BlockStatus>, ControlError> {
        self.output_index(output)?;
        Ok(self.replica()?.0.statuses[output].withhold(k, cause))
    }

    /// Replicated mode: block `k` of output `output`, `held`, commits
    /// through on evidence that used the silence of the voters in
    /// `on_silence_of`, by bit. False when it is not `held`.
    pub fn commit_through(
        &self,
        output: usize,
        k: u64,
        on_silence_of: u8,
    ) -> Result<bool, ControlError> {
        self.output_index(output)?;
        Ok(self.replica()?.0.statuses[output].commit_through(k, on_silence_of))
    }

    /// Replicated mode: what a side report says of this replica's clock
    /// and progress, read so that the processed index and the step count
    /// are never newer than the flags and offset beside them.
    pub fn progress(&self) -> Option<Progress> {
        let shared = &self.replica.as_ref()?.0;
        let c = &self.counters;
        // A block is published processed after its statuses and after the
        // steps detected before it; a step's count after the offset
        // sampled after it.
        let processed = c.processed.load(Acquire);
        let steps = c.clock_steps.load(Acquire);
        let offset = c.tai_offset_s.load(Relaxed);
        Some(Progress {
            processed: processed.checked_sub(1),
            steps,
            steps_cleared: shared.steps_cleared.load(Acquire),
            tai_offset_s: (offset != i64::MIN).then_some(offset),
            self_report: shared.self_report.load(Relaxed),
            status_changes_lost: c.status_changes_dropped.load(Relaxed)
                + c.status_changes_unsent.load(Relaxed),
            build_lead: self.build_lead.load(Relaxed),
        })
    }

    /// Replicated mode: the status of block `k` of output `output`, none
    /// before its first one or once its slot was reused.
    pub fn block_status(&self, output: usize, k: u64) -> Option<BlockStatus> {
        self.replica.as_ref()?.0.statuses.get(output)?.get(k)
    }

    /// Replicated mode: log entry `id` at `at_index` is unresolved; every
    /// block from its index carries it in its digest until
    /// [`Engine::resolved`]. At most [`UNRESOLVED`] at a time.
    pub fn unresolved(&self, id: LogId, at_index: u64) -> Result<(), ControlError> {
        self.control(Control::Unresolved { id, at_index })
    }

    /// Replicated mode: log entry `id` is decided.
    pub fn resolved(&self, id: LogId) -> Result<(), ControlError> {
        self.control(Control::Resolved { id })
    }

    /// Replicated mode with more than one voter: copies the engine's core
    /// after the next block the RT thread computes, `k0`, into a buffer
    /// allocated here, waiting up to `timeout` for it. The copy is kept
    /// only while the RT thread has not started the guard block of the
    /// history after `k0` (`historySlackMs`), which a copy of history
    /// rings needs; the engine keeps no delay line, so it holds the core
    /// alone. A copy left behind by a timeout is returned by the next
    /// call. Calls serialise: one buffer is with the RT thread at a time.
    pub fn copy_core(&self, timeout: Duration) -> Result<CoreCopy, CopyError> {
        let (Some((shared, _)), Some(transfer)) = (&self.replica, &self.transfer) else {
            return Err(CopyError::NotReplicated);
        };
        let mut t = transfer.lock().unwrap_or_else(|e| e.into_inner());
        if !t.in_flight {
            let (n, m, slots, page) = t.shape;
            let core = t
                .spare
                .take()
                .unwrap_or_else(|| Box::new(Core::new(n, m, slots, page)));
            if t.cores.push(core).is_err() {
                unreachable!("one buffer is with the RT thread at most");
            }
            t.in_flight = true;
        }
        let deadline = Instant::now() + timeout;
        let copy = loop {
            if let Ok(copy) = t.copies.pop() {
                t.in_flight = false;
                break copy;
            }
            if Instant::now() >= deadline {
                return Err(CopyError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        match history_kept(&shared.started, copy.k0, t.slack_blocks) {
            Ok(()) => Ok(copy),
            Err(started) => {
                t.spare = Some(copy.core);
                Err(CopyError::Passed {
                    k0: copy.k0,
                    started,
                })
            }
        }
    }

    /// Replicated mode: the clock check passed after the first `steps`
    /// clock steps the engine detected (`Counters::clock_steps`). A
    /// detected step lowers F below the next block until this covers it;
    /// a later step fences again. With one voter the engine's own clean
    /// self-report clears it as well.
    pub fn clear_step_fence(&self, steps: u64) -> Result<(), ControlError> {
        self.replica()?.0.steps_cleared.fetch_max(steps, Release);
        Ok(())
    }

    /// Restarts the loudness measurement of output `output` (the
    /// integrated loudness, the loudness range, the maxima and the
    /// validity) at the first 100 ms window starting at or after
    /// `at_index`, a multiple of the block size; the K-weighting starts
    /// from silence there, and the true-peak interpolator keeps the 11
    /// output samples before it. From that index on the integrated
    /// loudness, the loudness range and the maxima depend only on the
    /// output samples from those 11 on, and so do the momentary and
    /// short-term loudness once 0.4 s and 3 s past it: replicas that
    /// computed those samples and reset at the same index agree there.
    /// `at_index`
    /// is refused below the same earliest index as [`Engine::set_gain`].
    /// A reset replaces one still pending for the same output;
    /// [`ResetError::Full`] when the reset ring is full.
    pub fn reset_loudness(&self, output: usize, at_index: u64) -> Result<(), ResetError> {
        if output >= self.status.lock().expect("status lock").outputs.len() {
            return Err(ResetError::Invalid(format!("no output {output}")));
        }
        if !at_index.is_multiple_of(self.block) {
            return Err(ResetError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
        let mut tx = self.resets.lock().expect("reset lock");
        let earliest = earliest(&self.api, &self.counters, self.margin, self.lead);
        if at_index < earliest {
            return Err(ResetError::TooNear { at_index, earliest });
        }
        tx.push(LoudnessReset { output, at_index })
            .map_err(|_| ResetError::Full)
    }

    /// Stops the RT thread and the supervisor and releases every libmxl
    /// object.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Relaxed);
        if let Some(s) = self.supervisor.take() {
            let _ = s.join();
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum SupState {
    Attached,
    /// To be attached by the next generation of its domain.
    Wanted,
    Pruned {
        recovery: Recovery,
        /// Holds one reference to its flow's poll reader.
        held: bool,
    },
}

struct SupSlot {
    state: SupState,
    window: VecDeque<SlotStats>,
    /// The flow the slot reads.
    flow: String,
    /// The slot's state in the log, as the RT thread applied it.
    up: bool,
    /// `InputReadable` was reported since the polled head was last late.
    reported: bool,
    /// An `InputPruned` report did not go out.
    prune_unsent: bool,
    /// After a re-route: the index its crossfade ends at and the flow it
    /// left, kept in rebuilt generations until the RT thread passed it.
    routing: Option<(u64, String)>,
}

/// A generation-needing entry, from its append until the RT thread
/// swapped its generation in or handed it back.
struct Job {
    id: LogId,
    at_index: u64,
    change: Change,
    /// A `Route`'s flow.
    flow: Option<String>,
    domain: usize,
    stage: Stage,
}

enum Stage {
    /// Waits for the job before it.
    Waiting,
    Built(Box<Gen>, u64),
    /// Handed to the RT thread, with the slots its generation holds.
    Sent(u64),
}

struct SupOut {
    realised: bool,
    next_try: Instant,
    backoff: Duration,
}

/// The poll instance of one input domain and its poll readers by flow id.
struct Pollers {
    instance: Arc<Instance>,
    flows: HashMap<String, PolledFlow>,
}

struct Supervisor {
    api: Arc<MxlApi>,
    config: Config,
    timing: Timing,
    /// Read every page of a new generation's rings before handover.
    prefault: bool,
    /// Configuration domain index of each input domain.
    in_domains: Vec<usize>,
    slots: Vec<SupSlot>,
    /// Input domains whose generation is to be rebuilt.
    dirty: Vec<bool>,
    pollers: Vec<Option<Pollers>>,
    /// The last failure to create each input domain's poll instance and
    /// generation, logged once per change of its text.
    poll_errors: Vec<Option<String>>,
    rebuild_errors: Vec<Option<String>>,
    outs: Vec<SupOut>,
    /// Output instance per configuration domain.
    out_instances: Vec<Option<Arc<Instance>>>,
    /// Generation-needing entries, oldest first; only the first is built.
    jobs: VecDeque<Job>,
    log_rx: mpsc::Receiver<LogMsg>,
    log: Arc<Mutex<Log>>,
    /// Where reports go; the engine's own log when absent.
    reports: Option<SyncSender<Report>>,
    build_times: BuildTimes,
    build_lead: Arc<AtomicU64>,
    lead: u64,
    ramp: u64,
    cmd: rtrb::Producer<Command>,
    ret: rtrb::Consumer<Returned>,
    tel: rtrb::Consumer<Telemetry>,
    meter_rx: rtrb::Consumer<MeterRecord>,
    next_meter_seq: u64,
    /// The RT thread's status changes and where they go.
    change_rx: Option<rtrb::Consumer<StatusChange>>,
    status_tx: Option<SyncSender<StatusChange>>,
    /// The RT thread's page hashes and where they go.
    hash_rx: Option<rtrb::Consumer<PageHash>>,
    hash_tx: Option<SyncSender<PageHash>>,
    /// The longest range a member flow's writer commits at once: B, or
    /// the hold ring.
    batch: u64,
    /// What the supervisor shares with the RT thread in replicated mode.
    shared: Option<Arc<Shared>>,
    /// One voter: no peer checks its clock, so its own clean self-report
    /// lifts a detected step's fence.
    alone: bool,
    loudness: Vec<OutputLoudness>,
    verdicts: Option<SyncSender<BlockVerdict>>,
    status: Arc<Mutex<Status>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

fn log(message: std::fmt::Arguments) {
    eprintln!("samplez: {message}");
}

impl Supervisor {
    fn run(mut self, rt_thread: JoinHandle<(Rt, Option<String>)>) {
        let mut rt_thread = Some(rt_thread);
        let tid = self.status().rt.thread.tid;
        let mut last_poll = Instant::now();
        let mut last_faults = Instant::now();
        while !self.stop.load(Relaxed) {
            while let Ok(r) = self.ret.pop() {
                release(r);
            }
            self.telemetry();
            self.status_changes();
            self.page_hashes();
            self.meters();
            self.log_messages();
            self.jobs();
            if last_poll.elapsed() >= POLL_PERIOD {
                last_poll = Instant::now();
                self.poll();
            }
            let produced = self.counters.produced.load(Acquire);
            for slot in 0..self.slots.len() {
                if self.slots[slot]
                    .routing
                    .as_ref()
                    .is_some_and(|r| produced >= r.0)
                {
                    self.slots[slot].routing = None;
                    let d = self.input_domain(slot);
                    self.dirty[d] = true;
                }
            }
            for d in 0..self.dirty.len() {
                if self.dirty[d] && self.cmd.slots() > 0 {
                    self.rebuild(d);
                }
            }
            self.outputs();
            if last_faults.elapsed() >= Duration::from_secs(1) {
                last_faults = Instant::now();
                if let Some(f) = minor_faults(tid) {
                    self.counters.rt_minor_faults.store(f, Relaxed);
                }
                self.clock();
            }
            if rt_thread.as_ref().is_some_and(JoinHandle::is_finished) {
                self.rt_ended(rt_thread.take());
                break;
            }
            std::thread::sleep(SUPERVISOR_TICK);
        }
        // The RT thread sees the same flag.
        self.rt_ended(rt_thread);
    }

    /// Joins the RT thread and releases its state, with every generation
    /// and writer it holds, and what it handed back. A panic is recorded
    /// in the status.
    fn rt_ended(&mut self, rt_thread: Option<JoinHandle<(Rt, Option<String>)>>) {
        let Some(t) = rt_thread else { return };
        let message = match t.join() {
            Ok((state, message)) => {
                drop(state);
                message
            }
            Err(_) => Some("the RT thread ended outside its loop".to_owned()),
        };
        while let Ok(r) = self.ret.pop() {
            release(r);
        }
        if let Some(message) = message {
            log(format_args!(
                "the RT thread panicked: {message}; processing stopped"
            ));
            let mut st = self.status();
            for o in &mut st.outputs {
                o.realised = false;
                o.reason = Some("the RT thread panicked".to_owned());
            }
            st.rt_panic = Some(message);
        }
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().expect("status lock")
    }

    /// Turns the meter records into each output's meters. A gap in the
    /// sequence numbers means records were lost, of any output.
    fn meters(&mut self) {
        let mut seen = false;
        while let Ok(r) = self.meter_rx.pop() {
            seen = true;
            if r.seq != self.next_meter_seq {
                self.loudness.iter_mut().for_each(OutputLoudness::gap);
            }
            self.next_meter_seq = r.seq + 1;
            self.loudness[r.output as usize].record(&r);
        }
        if seen {
            let mut st = self.status();
            for (o, l) in st.outputs.iter_mut().zip(&self.loudness) {
                o.meters.clone_from(&l.meters);
            }
        }
    }

    /// Samples the kernel clock's synchronisation, for the clock
    /// self-report, and the clock status.
    fn clock(&mut self) {
        let (unsynchronised, clean_after) = self_report(&self.counters.clock_steps, unsynchronised);
        if let (Some(shared), Some(u)) = (&self.shared, unsynchronised) {
            shared.self_report.store(u, Relaxed);
            // A lone voter's clean self-report clears every step detected
            // before it.
            if self.alone
                && let Some(steps) = clean_after
            {
                shared.steps_cleared.fetch_max(steps, Release);
            }
        }
        let c = &self.counters;
        let offset = c.tai_offset_s.load(Relaxed);
        self.status().clock = ClockStatus {
            tai_offset_s: (offset != i64::MIN).then_some(offset),
            unsynchronised,
            steps: c.clock_steps.load(Relaxed),
            step_fenced: c.step_fenced.load(Relaxed),
        };
    }

    /// Passes the RT thread's status changes on.
    fn status_changes(&mut self) {
        let Some(rx) = &mut self.change_rx else {
            return;
        };
        while let Ok(c) = rx.pop() {
            if let Some(tx) = &self.status_tx
                && let Err(TrySendError::Full(_)) = tx.try_send(c)
            {
                self.counters.status_changes_unsent.fetch_add(1, Relaxed);
            }
        }
    }

    /// Passes the RT thread's page hashes on.
    fn page_hashes(&mut self) {
        let (Some(rx), Some(tx)) = (&mut self.hash_rx, &self.hash_tx) else {
            return;
        };
        while let Ok(h) = rx.pop() {
            if let Err(TrySendError::Full(_)) = tx.try_send(h) {
                self.counters.page_hashes_unsent.fetch_add(1, Relaxed);
            }
        }
    }

    fn telemetry(&mut self) {
        while let Ok(t) = self.tel.pop() {
            match t {
                Telemetry::Block(v) => {
                    if let Some(tx) = &self.verdicts
                        && let Err(TrySendError::Full(_)) = tx.try_send(v)
                    {
                        self.counters.verdicts_dropped.fetch_add(1, Relaxed);
                    }
                }
                Telemetry::Prune {
                    slot,
                    invalid,
                    head,
                    end,
                } => self.prune(slot, invalid, head, end),
                Telemetry::Stats { slot, stats } => self.stats(slot, stats),
                Telemetry::ClockStep {
                    delta_ns,
                    offset_before,
                    offset_after,
                } => {
                    log(format_args!(
                        "clock step of {delta_ns} ns against CLOCK_MONOTONIC; kernel TAI \
                         offset {offset_before} s before, {offset_after} s after"
                    ));
                }
                Telemetry::LateEntry { entry, pos } => {
                    let reason = format!(
                        "log entry {} ({:?}) at index {} arrived after index {pos} was \
                         produced; not applied",
                        entry.id, entry.change, entry.at_index
                    );
                    log(format_args!("diverged: {reason}"));
                    self.status().diverged.get_or_insert(reason);
                    // The engine's own log stamps a lost InputDown or
                    // InputUp again: from the state the RT thread has, on
                    // the next report.
                    if let Change::InputDown { slot } | Change::InputUp { slot } = entry.change {
                        let mut log = self.log.lock().expect("log lock");
                        let Some(l) = &mut log.local else { continue };
                        l.up[slot] = self.slots[slot].up;
                        drop(log);
                        self.slots[slot].reported = false;
                        if matches!(entry.change, Change::InputDown { .. }) {
                            self.report(Report::InputPruned { slot });
                        }
                    }
                }
                Telemetry::Input { slot, up } => {
                    self.slots[slot].up = up;
                    self.status().inputs[slot].up = up;
                    if !up && !matches!(self.slots[slot].state, SupState::Pruned { .. }) {
                        self.start_polling(slot, false);
                        let d = self.input_domain(slot);
                        self.dirty[d] = true;
                    }
                }
                Telemetry::GenerationDone { id, swapped } => self.generation_done(id, swapped),
                Telemetry::AwaitLost { entry } => {
                    let reason = format!(
                        "log entry {} ({:?}) at index {} was applied before its generation \
                         arrived, with {PENDING_CHANGES} others awaiting theirs; its \
                         generation is not swapped in",
                        entry.id, entry.change, entry.at_index
                    );
                    log(format_args!("diverged: {reason}"));
                    self.status().diverged.get_or_insert(reason);
                }
                Telemetry::StripRefused { entry } => {
                    let reason = format!(
                        "log entry {} ({:?}) at index {} was refused by its slot's strip; \
                         not applied",
                        entry.id, entry.change, entry.at_index
                    );
                    log(format_args!("diverged: {reason}"));
                    self.status().diverged.get_or_insert(reason);
                }
            }
        }
    }

    fn input_domain(&self, slot: usize) -> usize {
        let d = self
            .config
            .domain(&self.config.inputs[slot].domain)
            .expect("validated");
        self.in_domains
            .iter()
            .position(|&x| x == d)
            .expect("every input's domain is an input domain")
    }

    fn prune(&mut self, slot: usize, invalid: bool, head: u64, end: u64) {
        if !matches!(self.slots[slot].state, SupState::Attached) {
            return;
        }
        let name = &self.config.inputs[slot].name;
        let why = if invalid {
            "flow invalid"
        } else {
            "head frozen"
        };
        log(format_args!(
            "input {name} pruned: {why} at head {head}, block end {end}"
        ));
        self.start_polling(slot, true);
        let d = self.input_domain(slot);
        self.dirty[d] = true;
    }

    /// Moves `slot` to polling, reporting it pruned when `report`.
    fn start_polling(&mut self, slot: usize, report: bool) {
        let held = self.hold_poll(slot);
        self.slots[slot].state = SupState::Pruned {
            recovery: Recovery::default(),
            held,
        };
        self.slots[slot].reported = false;
        self.status().inputs[slot].state = InputState::Pruned;
        if report {
            self.report(Report::InputPruned { slot });
        }
    }

    /// Takes one hold on the poll reader of `slot`'s flow, creating the
    /// domain's poll instance and the poll reader when missing. False when
    /// either could not be created; `poll` tries again.
    fn hold_poll(&mut self, slot: usize) -> bool {
        let d = self.input_domain(slot);
        let id = self.slots[slot].flow.clone();
        if self.pollers[d].is_none() {
            let path = &self.config.domains[self.in_domains[d]].path;
            match Instance::new(&self.api, path) {
                Ok(instance) => {
                    self.poll_errors[d] = None;
                    self.pollers[d] = Some(Pollers {
                        instance,
                        flows: HashMap::new(),
                    })
                }
                Err(e) => {
                    let message = format!("poll instance on {}: {e}", path.display());
                    if changed(&mut self.poll_errors[d], message) {
                        log(format_args!(
                            "{}",
                            self.poll_errors[d].as_deref().unwrap_or("")
                        ));
                    }
                    return false;
                }
            }
        }
        let Some(p) = &mut self.pollers[d] else {
            return false;
        };
        if let Some(f) = p.flows.get_mut(&id) {
            f.hold();
        } else {
            match PolledFlow::new(&p.instance, &id) {
                Ok(f) => {
                    p.flows.insert(id, f);
                }
                Err(e) => {
                    log(format_args!("poll reader of {id}: {e}"));
                    return false;
                }
            }
        }
        true
    }

    /// Releases `slot`'s hold on its poll reader, if it has one.
    fn release_poll(&mut self, slot: usize) {
        let SupState::Pruned { held: true, .. } = self.slots[slot].state else {
            return;
        };
        let d = self.input_domain(slot);
        let id = &self.slots[slot].flow;
        if let Some(p) = &mut self.pollers[d]
            && let Some(f) = p.flows.get_mut(id)
            && f.release() == 0
        {
            p.flows.remove(id);
        }
    }

    fn poll(&mut self) {
        for slot in 0..self.slots.len() {
            if matches!(self.slots[slot].state, SupState::Pruned { held: false, .. }) {
                let held = self.hold_poll(slot);
                if let SupState::Pruned { held: h, .. } = &mut self.slots[slot].state {
                    *h = held;
                }
            }
            let s = &mut self.slots[slot];
            if s.prune_unsent {
                if s.up && matches!(s.state, SupState::Pruned { .. }) {
                    self.report(Report::InputPruned { slot });
                } else {
                    s.prune_unsent = false;
                }
            }
        }
        let now = time::now(&self.api);
        let mut heads: HashMap<(usize, String), Option<u64>> = HashMap::new();
        for (d, p) in self.pollers.iter_mut().enumerate() {
            let Some(p) = p else { continue };
            for (id, f) in &mut p.flows {
                let head = match f.poll() {
                    Poll::Head(h) => Some(h),
                    Poll::Invalid | Poll::Absent(_) => None,
                };
                heads.insert((d, id.clone()), head);
            }
        }
        for slot in 0..self.slots.len() {
            let d = self.input_domain(slot);
            let cfg = &self.config.inputs[slot];
            let s = &mut self.slots[slot];
            let head = heads.get(&(d, s.flow.clone())).copied().flatten();
            let SupState::Pruned { recovery: r, .. } = &mut s.state else {
                continue;
            };
            if !r.update(&self.api, head, now, &self.timing, cfg.offset_samples) {
                s.reported = false;
            } else if s.up {
                // Up in the log, so it is read again at once: the blocks
                // it was silent in were a degradation of this engine.
                log(format_args!("input {} on time again, re-adding", cfg.name));
                self.release_poll(slot);
                self.slots[slot].state = SupState::Wanted;
                self.dirty[d] = true;
            } else if !s.reported && self.report(Report::InputReadable { slot }) {
                let name = &self.config.inputs[slot].name;
                log(format_args!("input {name} on time again, reported"));
                self.slots[slot].reported = true;
            }
        }
    }

    /// Builds a new generation of input domain `d` with every attached and
    /// wanted slot and hands it to the RT thread. Waits while a
    /// generation-needing entry's generation for `d` is built or held by
    /// the RT thread, which would replace this one at its index: until the
    /// entry is committed, then until the RT thread reaches its index or
    /// hands it back. A slot of `d` pruned meanwhile stays in the live
    /// generation until then, silent.
    fn rebuild(&mut self, d: usize) {
        if self
            .jobs
            .front()
            .is_some_and(|j| j.domain == d && !matches!(j.stage, Stage::Waiting))
        {
            return;
        }
        let Some((generation, held)) = self.build(d, None) else {
            return;
        };
        let command = Command::Generation {
            domain: d,
            generation,
        };
        if self.cmd.push(command).is_ok() {
            self.dirty[d] = false;
            self.adopt(d, held, None);
        }
    }

    /// A generation of input domain `d`, with the slots it holds: every
    /// attached and wanted slot of `d`, the outgoing flow of a re-route
    /// until its crossfade ended, and the change of `job`. A slot that
    /// cannot be attached is left out and its error put in its status.
    fn build(&mut self, d: usize, job: Option<&Job>) -> Option<(Box<Gen>, u64)> {
        let started = Instant::now();
        let path: PathBuf = self.config.domains[self.in_domains[d]].path.clone();
        let n = self.slots.len();
        let mut generation = match Generation::new(&self.api, &path, n) {
            Ok(g) => g,
            Err(e) => {
                let message = format!("input generation on {}: {e}", path.display());
                if changed(&mut self.rebuild_errors[d], message) {
                    log(format_args!(
                        "{}",
                        self.rebuild_errors[d].as_deref().unwrap_or("")
                    ));
                }
                return None;
            }
        };
        self.rebuild_errors[d] = None;
        let produced = self.counters.produced.load(Acquire);
        let mut prune_after = vec![u64::MAX; n];
        let mut held = 0u64;
        let mut rerouted = false;
        for (slot, limit) in prune_after.iter_mut().enumerate() {
            if self.input_domain(slot) != d {
                continue;
            }
            let (target, flow) = match job.map(|j| (j.change, j.flow.as_deref())) {
                Some((Change::InputUp { slot: s }, _)) if s == slot => (true, None),
                Some((Change::Route { slot: s }, f)) if s == slot => (true, f),
                _ => (false, None),
            };
            let current = self.slots[slot].flow.clone();
            let wanted = !matches!(self.slots[slot].state, SupState::Pruned { .. });
            let cfg = &self.config.inputs[slot];
            fn input<'a>(cfg: &crate::config::Input, flow_id: &'a str) -> InputConfig<'a> {
                InputConfig {
                    flow_id,
                    channels: cfg.channels,
                    offset: cfg.offset_samples,
                }
            }
            // The flow a re-route leaves is read until its crossfade ends.
            let outgoing = match (flow, &self.slots[slot].routing) {
                (Some(f), _) if f != current && wanted => Some(current.clone()),
                (None, Some((end, old))) if produced < *end => Some(old.clone()),
                _ => None,
            };
            if let Some(old) = outgoing
                && let Err(e) = generation.attach_outgoing(slot, &input(cfg, &old), &self.timing)
            {
                log(format_args!("input {}: flow {old} left: {e}", cfg.name));
            }
            rerouted |= flow.is_some_and(|f| f != current);
            if !wanted && !target {
                continue;
            }
            let flow_id = flow.unwrap_or(&current);
            match generation.attach(slot, &input(cfg, flow_id), &self.timing) {
                Ok(short) => {
                    held |= 1 << slot;
                    let hint = generation
                        .input(slot)
                        .and_then(|i| i.reader().config().ok())
                        .map_or(0, |c| u64::from(c.common.maxCommitBatchSizeHint));
                    *limit = (u64::from(self.config.prune_after_blocks) * self.timing.block)
                        .max(2 * hint + self.timing.margin);
                    if let Some(needed) = short {
                        log(format_args!(
                            "input {}: readerMarginSamples {} is below the {needed} its \
                             producer's maxCommitBatchSizeHint needs",
                            cfg.name, self.timing.margin
                        ));
                    }
                    let mut st = self.status();
                    st.inputs[slot].error = None;
                    st.inputs[slot].margin_short = short;
                }
                Err(e) => {
                    let message = e.to_string();
                    log(format_args!("input {} not attached: {message}", cfg.name));
                    self.status().inputs[slot].error = Some(message);
                }
            }
        }
        if self.prefault
            && let Err(e) = generation.prefault()
        {
            log(format_args!(
                "input generation on {}: reading the rings before handover: {e}",
                path.display()
            ));
        }
        self.build_times.add(started.elapsed().as_nanos() as u64);
        let p = self.build_times.p9999();
        let lead = build_lead_samples(p, self.lead, self.timing.block);
        self.build_lead.store(lead, Relaxed);
        let mut st = self.status();
        st.generation.builds = self.build_times.total;
        st.generation.build_p9999_ns = p;
        st.generation.build_lead = lead;
        drop(st);
        let generation = Box::new(Gen {
            mxl: generation,
            prune_after,
            rerouted,
        });
        Some((generation, held))
    }

    /// Takes the slots of input domain `d` that the generation now live
    /// on the RT thread holds, `held`, as attached, and moves the others
    /// that were attached or wanted to polling, reported. `target` is the
    /// slot of an `InputUp` or `Route` entry: reported when not held.
    /// A held slot that was pruned meanwhile, or is down in the log, is
    /// left out of the next generation instead.
    fn adopt(&mut self, d: usize, held: u64, target: Option<usize>) {
        for slot in 0..self.slots.len() {
            if self.input_domain(slot) != d {
                continue;
            }
            let pruned = matches!(self.slots[slot].state, SupState::Pruned { .. });
            if held & (1 << slot) == 0 {
                if !pruned {
                    self.start_polling(slot, true);
                } else if target == Some(slot) {
                    self.slots[slot].reported = false;
                    self.report(Report::InputPruned { slot });
                }
                continue;
            }
            if (pruned && target != Some(slot)) || !self.slots[slot].up {
                if !pruned {
                    self.start_polling(slot, false);
                }
                self.dirty[d] = true;
                continue;
            }
            self.release_poll(slot);
            if !matches!(self.slots[slot].state, SupState::Attached) {
                log(format_args!(
                    "input {} attached",
                    self.config.inputs[slot].name
                ));
            }
            self.slots[slot].state = SupState::Attached;
            self.status().inputs[slot].state = InputState::Attached;
        }
    }

    /// Takes in what the log sent: generation-needing entries appended,
    /// and truncations, which drop the entries' generations unless the RT
    /// thread holds them (committed entries are never truncated).
    fn log_messages(&mut self) {
        while let Ok(m) = self.log_rx.try_recv() {
            match m {
                LogMsg::Append {
                    id,
                    at_index,
                    change,
                    flow,
                } => {
                    let (Change::InputUp { slot } | Change::Route { slot }) = change else {
                        continue;
                    };
                    self.jobs.push_back(Job {
                        id,
                        at_index,
                        change,
                        flow,
                        domain: self.input_domain(slot),
                        stage: Stage::Waiting,
                    });
                }
                LogMsg::Truncate { from } => self
                    .jobs
                    .retain(|j| j.id < from || matches!(j.stage, Stage::Sent(_))),
                LogMsg::Skip { id } => self
                    .jobs
                    .retain(|j| j.id != id || matches!(j.stage, Stage::Sent(_))),
            }
        }
    }

    /// Builds the first job's generation, on top of the one live, and
    /// hands it to the RT thread once its entry is committed. The next
    /// job waits until the RT thread is done with this one, so it holds
    /// at most one pending generation.
    fn jobs(&mut self) {
        if let Some(j) = self.jobs.pop_front() {
            let j = match j.stage {
                Stage::Waiting => match self.build(j.domain, Some(&j)) {
                    Some((g, held)) => Job {
                        stage: Stage::Built(g, held),
                        ..j
                    },
                    None => j,
                },
                _ => j,
            };
            let committed = self.log.lock().expect("log lock").committed;
            let j = match j.stage {
                Stage::Built(generation, held) if j.id <= committed && self.cmd.slots() > 0 => {
                    let command = Command::Pending {
                        id: j.id,
                        domain: j.domain,
                        generation,
                    };
                    if self.cmd.push(command).is_err() {
                        unreachable!("a free command slot was checked");
                    }
                    Job {
                        stage: Stage::Sent(held),
                        ..j
                    }
                }
                _ => j,
            };
            self.jobs.push_front(j);
        }
        let mut st = self.status();
        st.generation.pending = self.jobs.front().map(|j| j.id);
        st.generation.ready = self
            .jobs
            .front()
            .is_some_and(|j| !matches!(j.stage, Stage::Waiting));
    }

    /// The RT thread swapped in the generation of entry `id`, or handed
    /// it back unused.
    fn generation_done(&mut self, id: LogId, swapped: bool) {
        let Some(j) = self.jobs.front().filter(|j| j.id == id) else {
            return;
        };
        let Stage::Sent(held) = j.stage else {
            return;
        };
        let j = self.jobs.pop_front().expect("checked above");
        if !swapped {
            return;
        }
        let slot = match j.change {
            Change::InputUp { slot } => slot,
            Change::Route { slot } => {
                let flow = j.flow.expect("a route has a flow");
                if flow != self.slots[slot].flow {
                    // A polled slot polls the flow it reads: the hold on
                    // the old one is released, `poll` takes one on the new.
                    self.release_poll(slot);
                    if let SupState::Pruned { recovery, held } = &mut self.slots[slot].state {
                        *recovery = Recovery::default();
                        *held = false;
                    }
                    self.slots[slot].reported = false;
                    let old = std::mem::replace(&mut self.slots[slot].flow, flow.clone());
                    self.slots[slot].routing = Some((j.at_index + self.ramp, old));
                    self.status().inputs[slot].flow_id = flow;
                }
                slot
            }
            _ => return,
        };
        self.adopt(j.domain, held, Some(slot));
    }

    /// Sends `r` to the replicated log, or stamps it in the engine's own:
    /// a pruned input that is up goes down at the earliest index, a
    /// readable one that is down comes up at the earliest index a
    /// generation-needing entry takes, each after the slot's last entry
    /// and its ramp, and an `InputUp` buildLead after the log's last
    /// generation-needing entry. False when it did not go out: the channel
    /// was full or the log refused it. An `InputPruned` that did not go
    /// out is sent again by `poll` while it holds.
    fn report(&mut self, r: Report) -> bool {
        let (slot, up) = match r {
            Report::InputPruned { slot } => (slot, false),
            Report::InputReadable { slot } => (slot, true),
        };
        let sent = 'sent: {
            if let Some(tx) = &self.reports {
                if tx.try_send(r).is_err() {
                    self.counters.reports_dropped.fetch_add(1, Relaxed);
                    break 'sent false;
                }
                break 'sent true;
            }
            let mut earliest = earliest(&self.api, &self.counters, self.timing.margin, self.lead);
            let build_lead = self.build_lead.load(Relaxed);
            let consumed = self.counters.entries_consumed.load(Acquire);
            let mut log = self.log.lock().expect("log lock");
            let Some(local) = &log.local else {
                break 'sent true;
            };
            if local.up[slot] == up {
                break 'sent true;
            }
            let change = if up {
                earliest = (earliest - self.lead + build_lead).max(local.gen_at + build_lead);
                Change::InputUp { slot }
            } else {
                Change::InputDown { slot }
            };
            let at_index = stamp(earliest, local.last_at[slot], self.ramp, self.timing.block);
            #[cfg(test)]
            let at_index = if STAMP_LATE.swap(false, Relaxed) {
                let produced = self.counters.produced.load(Acquire) / self.timing.block;
                produced.saturating_sub(10) * self.timing.block
            } else {
                at_index
            };
            let e = RtEntry {
                id: log.last + 1,
                at_index,
                change,
            };
            if log.append(e, None, consumed).is_err() {
                break 'sent false;
            }
            log.commit(e.id);
            let local = log.local.as_mut().expect("checked above");
            local.up[slot] = up;
            local.last_at[slot] = at_index;
            if up {
                local.gen_at = at_index;
            }
            true
        };
        if !up {
            self.slots[slot].prune_unsent = !sent;
        }
        sent
    }

    fn stats(&mut self, slot: usize, stats: SlotStats) {
        let window = &mut self.slots[slot].window;
        window.push_back(stats);
        if window.len() > MARGIN_WINDOW {
            window.pop_front();
        }
        let warning = margin_warning(&self.api, window, &self.timing);
        let mut st = self.status.lock().expect("status lock");
        let s = &mut st.inputs[slot];
        s.counters = stats.counters;
        if warning != s.margin_warning {
            if let Some(required) = warning {
                log(format_args!(
                    "input {}: late blocks while the head advances; readerMarginSamples {} \
                     needs to be at least {required}",
                    s.name, self.timing.margin
                ));
            }
            s.margin_warning = warning;
        }
    }

    fn outputs(&mut self) {
        let now = Instant::now();
        for o in 0..self.outs.len() {
            let out = &mut self.outs[o];
            if out.realised || now < out.next_try || self.cmd.slots() == 0 {
                continue;
            }
            let cfg = &self.config.outputs[o];
            let d = self.config.domain(&cfg.domain).expect("validated");
            if self.out_instances[d].is_none() {
                match Instance::new(&self.api, &self.config.domains[d].path) {
                    Ok(i) => self.out_instances[d] = Some(i),
                    Err(e) => {
                        retry(out, now);
                        let mut st = self.status.lock().expect("status lock");
                        st.outputs[o].reason = Some(e.to_string());
                        st.outputs[o].attempts += 1;
                        continue;
                    }
                }
            }
            let instance = self.out_instances[d].as_ref().expect("created above");
            let oc = output::OutputConfig {
                batch: self.batch,
                ..self.config.output_config(cfg)
            };
            let result = output::create(instance, &oc);
            let mut st = self.status.lock().expect("status lock");
            let s = &mut st.outputs[o];
            s.attempts += 1;
            let reason = match result {
                Ok(Creation::Created { writer, collected }) => {
                    s.realised = true;
                    s.reason = None;
                    s.buffer_length = Some(writer.buffer_length());
                    s.hints = Some(writer.hints());
                    out.realised = true;
                    let command = Command::Writer {
                        output: o,
                        writer: Box::new(writer),
                    };
                    if self.cmd.push(command).is_err() {
                        unreachable!("a free command slot was checked");
                    }
                    log(format_args!(
                        "output {} realised{}",
                        cfg.name,
                        if collected {
                            " after collecting a stale flow"
                        } else {
                            ""
                        }
                    ));
                    continue;
                }
                Ok(Creation::Active) => "another writer is live on the flow".to_owned(),
                Ok(Creation::Raced) => {
                    s.races += 1;
                    "another writer created the flow first; libmxl v1.1.0 leaves a \
                     .mxl-tmp-* directory in the domain on this path (upstream #776)"
                        .to_owned()
                }
                Ok(Creation::Survived { gc }) => format!(
                    "a stale flow survived mxlGarbageCollectFlows (status {gc}); \
                     only the check is retried"
                ),
                Err(e) => {
                    if matches!(
                        e,
                        samplez_mxl::Error::Mxl {
                            call: "mxlCreateFlowWriter",
                            ..
                        }
                    ) {
                        s.create_failures += 1;
                    }
                    e.to_string()
                }
            };
            if s.reason.as_deref() != Some(&reason) {
                log(format_args!("output {} not realised: {reason}", cfg.name));
            }
            s.reason = Some(reason);
            retry(out, now);
        }
    }
}

/// The kernel clock's self-report from `read`, and on a clean one the
/// count of clock steps detected before the reading began: a step
/// detected while it ran is not cleared by it.
fn self_report(
    steps: &AtomicU64,
    read: impl FnOnce() -> Option<bool>,
) -> (Option<bool>, Option<u64>) {
    let before = steps.load(Acquire);
    let unsynchronised = read();
    (
        unsynchronised,
        (unsynchronised == Some(false)).then_some(before),
    )
}

/// Whether the kernel clock is unsynchronised: `adjtimex` reading
/// `TIME_ERROR` or `STA_UNSYNC`. None when the call failed.
fn unsynchronised() -> Option<bool> {
    // SAFETY: an all-zero timex is valid; modes 0 only reads.
    let mut t: libc::timex = unsafe { std::mem::zeroed() };
    // SAFETY: `t` is writable.
    let state = unsafe { libc::adjtimex(&mut t) };
    (state >= 0).then_some(state == libc::TIME_ERROR || t.status & libc::STA_UNSYNC != 0)
}

/// Releases what the RT thread handed back: a generation's readers,
/// group and instance, or a writer, which deletes its flow.
fn release(r: Returned) {
    match r {
        Returned::Generation(g) => drop(g),
        Returned::Writer(w) => drop(w),
    }
}

/// Stores `error` in `last`; true when it differs from the one stored, so
/// a failure repeated every tick is logged once per change of its text.
fn changed(last: &mut Option<String>, error: String) -> bool {
    if last.as_deref() == Some(error.as_str()) {
        return false;
    }
    *last = Some(error);
    true
}

fn retry(out: &mut SupOut, now: Instant) {
    out.next_try = now + out.backoff;
    out.backoff = (out.backoff * 2).min(BACKOFF_MAX);
}

/// The margin the slot's late blocks need, `margin - min_slack + B`, once
/// the window spans 10 s with late blocks, on-time blocks and a head that
/// advanced at the nominal rate within 1 %.
fn margin_warning(api: &MxlApi, window: &VecDeque<SlotStats>, timing: &Timing) -> Option<u64> {
    if window.len() < MARGIN_WINDOW {
        return None;
    }
    let (first, last) = (window.front()?, window.back()?);
    let late: u64 = window.iter().skip(1).map(|s| u64::from(s.late)).sum();
    let on_time: u64 = window.iter().skip(1).map(|s| u64::from(s.on_time)).sum();
    if late == 0 || on_time == 0 {
        return None;
    }
    let advanced = last.head?.checked_sub(first.head?)?;
    let nominal =
        time::timestamp_to_index(api, last.now) - time::timestamp_to_index(api, first.now);
    if advanced.abs_diff(nominal) * 100 > nominal {
        return None;
    }
    let min_slack = window
        .iter()
        .skip(1)
        .filter(|s| s.late > 0)
        .map(|s| s.min_slack)
        .min()?;
    Some((timing.margin as i64 - min_slack) as u64 + timing.block)
}

/// Minor page faults of thread `tid`, from `/proc/self/task/<tid>/stat`.
fn minor_faults(tid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")).ok()?;
    // Field 10, counted from the state field after the command name.
    stat.rsplit_once(") ")?.1.split(' ').nth(7)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The percentile follows the build times: every build below 10 000
    /// counts, one slow build among 20 000 does not, and buildLead covers
    /// it, a supervisor tick and the lead in whole blocks.
    /// The history after the core of block k0 ends at the start of block
    /// k0 + 1 and is first overwritten by block k0 + 1 + slack: with the
    /// block in progress and one guard block, a copy is kept while the RT
    /// thread started at most block k0 + slack - 1, refused from k0 + slack.
    #[test]
    fn the_history_copy_ends_at_the_block_after_k0() {
        let started = Started::new();
        let (k0, slack) = (1000, 50);
        started.begin(k0 + slack - 1);
        assert_eq!(history_kept(&started, k0, slack), Ok(()));
        started.begin(k0 + slack);
        assert_eq!(history_kept(&started, k0, slack), Err(k0 + slack));
    }

    #[test]
    fn build_lead_covers_the_p9999_build_time() {
        let mut t = BuildTimes::new();
        assert_eq!(t.p9999(), None);
        for _ in 0..19_999 {
            t.add(1_000_000);
        }
        let p = t.p9999().unwrap();
        assert!((1_000_000..1_190_000).contains(&p), "{p}");
        t.add(80_000_000);
        assert_eq!(t.p9999(), Some(p));
        t.add(80_000_000);
        t.add(80_000_000);
        assert!(t.p9999().unwrap() >= 80_000_000);
        // 1 ms + 5 ms is 288 samples, plus a lead of 192: 480.
        assert_eq!(build_lead_samples(Some(1_000_000), 192, 48), 480);
        assert_eq!(build_lead_samples(Some(1_000_001), 192, 48), 528);
        assert_eq!(build_lead_samples(None, 0, 48), 5040);
    }

    fn log() -> (Log, rtrb::Consumer<RtEntry>, mpsc::Receiver<LogMsg>) {
        let (ring, rx) = rtrb::RingBuffer::new(PENDING_CHANGES);
        let (sup, msgs) = mpsc::channel();
        let log = Log {
            ring,
            sup,
            uncommitted: VecDeque::new(),
            last: 0,
            committed: 0,
            submitted: 0,
            local: None,
        };
        (log, rx, msgs)
    }

    fn e(id: LogId, change: Change) -> RtEntry {
        RtEntry {
            id,
            at_index: 48 * id,
            change,
        }
    }

    /// Entries reach the RT thread only once committed, in log order;
    /// a truncation removes only uncommitted ones, and the outstanding
    /// count stays within what the RT thread holds.
    #[test]
    fn the_log_hands_over_committed_entries_only() {
        let (mut log, mut rx, msgs) = log();
        let down = Change::InputDown { slot: 0 };
        let up = Change::InputUp { slot: 0 };
        log.append(e(1, down), None, 0).unwrap();
        log.append(e(2, up), None, 0).unwrap();
        assert!(matches!(msgs.try_recv(), Ok(LogMsg::Append { id: 2, .. })));
        assert_eq!(
            log.append(e(2, down), None, 0),
            Err(EntryError::Order { id: 2, last: 2 })
        );
        assert!(rx.pop().is_err());
        log.commit(1);
        assert_eq!(rx.pop().unwrap().id, 1);
        // A truncation at or below the commit point keeps what is
        // committed, and the generations of its entries.
        log.truncate(1);
        assert!(matches!(msgs.try_recv(), Ok(LogMsg::Truncate { from: 2 })));
        assert_eq!((log.last, log.uncommitted.len()), (1, 0));
        log.append(e(2, down), None, 0).unwrap();
        log.commit(2);
        assert_eq!(rx.pop().unwrap().id, 2);
        for id in 3..=PENDING_CHANGES as u64 {
            log.append(e(id, down), None, 0).unwrap();
        }
        assert_eq!(
            log.append(e(PENDING_CHANGES as u64 + 1, down), None, 0),
            Err(EntryError::Full)
        );
        log.append(e(PENDING_CHANGES as u64 + 1, down), None, 1)
            .unwrap();
    }

    /// A full reset ring says so, not that gain changes are outstanding.
    #[test]
    fn a_full_reset_ring_names_itself() {
        assert_eq!(
            ResetError::Full.to_string(),
            "the loudness reset ring is full"
        );
    }

    /// Held by each test that starts engines: the test hooks are process
    /// wide, and any engine's RT thread or supervisor takes them.
    static ENGINES: Mutex<()> = Mutex::new(());

    /// A panic on the RT thread hands the state back: the supervisor
    /// releases the writer, which deletes the output flow, and records
    /// the panic.
    #[test]
    #[cfg_attr(
        samplez_rtsan,
        ignore = "the panic allocates in the RT loop, which RealtimeSanitizer aborts"
    )]
    fn a_panic_on_the_rt_thread_releases_its_state_on_the_supervisor() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-panic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let id = "7fbec3b1-1b0f-417d-9059-8b94a47197ed";
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [{ "name": "out", "domain": "d", "flowId": id, "label": "panic",
                          "group": "panic", "role": "Audio", "channels": 1 }],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let engine = Engine::start(config, process, None).unwrap();
        let wait = |what: &str, f: &dyn Fn() -> bool| {
            let t = Instant::now();
            while !f() {
                assert!(
                    t.elapsed() < Duration::from_secs(5),
                    "timed out waiting for {what}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let flow = domain.join(format!("{id}.mxl-flow"));
        wait("output realised", &|| engine.status().outputs[0].realised);
        assert!(flow.exists());

        crate::rt_loop::PANIC.store(true, Relaxed);
        wait("the panic recorded", &|| engine.status().rt_panic.is_some());
        let st = engine.status();
        assert_eq!(st.rt_panic.as_deref(), Some("the panic test hook"));
        assert!(!st.outputs[0].realised);
        assert!(!flow.exists(), "the released writer deletes its flow");
        engine.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// An overrun skip withholds the skipped blocks at the skip, in one
    /// ranged change for every output, and withdraws the replica: no block
    /// after it commits at once on any output.
    #[test]
    fn an_overrun_skip_withholds_the_skipped_blocks_and_withdraws() {
        use crate::blocks::{Kind, PAGE};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-skip-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let output = |id: &str| {
            serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": "skip",
                                "group": "skip", "role": "Audio", "channels": 1 })
        };
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [output("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d11"),
                        output("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d12")],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(1);
        let (status_tx, changes) = mpsc::sync_channel(1 << 20);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: Some(status_tx),
            hashes: None,
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().outputs.iter().all(|o| o.realised));
        e.set_fence(u64::MAX).unwrap();
        let from = processed() + 2;
        for o in 0..2 {
            e.commit_from(o, from).unwrap();
        }
        let kp = (from * 48).next_multiple_of(PAGE) / 48;
        let committed = |o: usize| {
            (kp..processed()).any(|k| {
                e.block_status(o, k)
                    .is_some_and(|s| s.kind == Kind::Committed)
            })
        };
        wait(&|| committed(0) && committed(1));

        crate::rt_loop::STALL_MS.store(40, Relaxed);
        // The stall's skip, past any overrun skip under load.
        let t = Instant::now();
        let (skip_from, skip_to) = loop {
            assert!(t.elapsed() < Duration::from_secs(5), "no skip");
            match changes.recv_timeout(Duration::from_millis(100)) {
                Ok(StatusChange::Skipped { from, to }) if to - from >= 20 => break (from, to),
                _ => {}
            }
        };
        assert!(skip_to - skip_from >= 20, "{skip_from}..{skip_to}");
        assert!(e.counters().blocks_withheld.load(Relaxed) >= skip_to - skip_from);
        wait(&|| processed() > skip_to + 40);
        for o in 0..2 {
            // A block without a status was skipped by an overrun under
            // load as well.
            for k in skip_to..skip_to + 40 {
                let Some(st) = e.block_status(o, k) else {
                    continue;
                };
                assert_ne!(st.kind, Kind::Committed, "output {o}: {st:?}");
                assert!(
                    st.kind == Kind::Withheld || st.digest.is_none(),
                    "output {o}: {st:?} is neither withheld nor a fill"
                );
            }
        }
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// A block withheld in the ring withdraws the replica before the next
    /// block takes its status: after a stall, with F past the ring and
    /// its newest block withheld, the blocks before it commit, and the
    /// first block computed after it does not commit at once, nor does
    /// any later one without evidence.
    #[test]
    fn a_block_after_a_withheld_one_does_not_commit_at_once() {
        use crate::blocks::{Cause, Kind, PAGE};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        // A run an overrun skip or an overflow disturbed is run again.
        let run = || -> Option<String> {
            let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
            let domain = std::env::temp_dir().join(format!("samplez-order-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&domain);
            std::fs::create_dir(&domain).unwrap();
            let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d31";
            // A catch-up long enough that the stall below skips nothing.
            let v = serde_json::json!({
                "libmxl": libmxl,
                "maxCatchupBlocks": 200,
                "holdMs": 100,
                "domains": [{ "name": "d", "path": domain }],
                "outputs": [{ "name": "out", "domain": "d", "flowId": id, "label": "order",
                              "group": "order", "role": "Audio", "channels": 1 }],
                "matrix": [],
                "rt": { "enabled": false, "rtCpu": "none" },
            });
            let config = Config::parse(&v.to_string()).unwrap();
            let process = rt::setup_process(&config.rt);
            let (report_tx, _reports) = mpsc::sync_channel(1);
            let replication = Replication {
                voters: 2,
                reports: report_tx,
                statuses: None,
                hashes: None,
            };
            let e = Engine::start_replicated(config, process, None, replication).unwrap();
            let processed = || e.counters().processed.load(Acquire);
            wait(&|| e.status().outputs[0].realised);
            e.set_fence(u64::MAX).unwrap();
            let from = processed() + 2;
            e.commit_from(0, from).unwrap();
            let kp = (from * 48).next_multiple_of(PAGE) / 48;
            wait(&|| processed() > kp + 5);
            // F frozen: blocks wait fenced in the ring.
            let f = processed() + 1;
            e.set_fence(f * 48).unwrap();
            wait(&|| processed() > f + 10);
            crate::rt_loop::STALL_MS.store(60, Relaxed);
            std::thread::sleep(Duration::from_millis(10));
            // During the stall: the newest block in the ring withheld, F past
            // all.
            let stalled = processed();
            e.withhold(0, stalled - 1, Cause::HoldCap).unwrap();
            e.set_fence(u64::MAX).unwrap();
            assert_eq!(processed(), stalled, "the RT thread did not stall");
            wait(&|| processed() > stalled + 40);
            let c = e.counters();
            let (skipped, overflows) = (
                c.overrun_skipped_samples.load(Relaxed),
                c.hold_overflows.load(Relaxed),
            );
            if skipped > 0 || overflows > 0 {
                // A skip or an overflow under load withdrew the replica
                // before the stall ended: the run proves nothing.
                e.stop();
                std::fs::remove_dir_all(&domain).unwrap();
                return Some(format!("{skipped} samples skipped, {overflows} overflows"));
            }
            for k in f..stalled - 1 {
                assert_eq!(e.block_status(0, k).unwrap().kind, Kind::Committed);
            }
            for k in stalled..stalled + 40 {
                let st = e.block_status(0, k).unwrap();
                assert_ne!(st.kind, Kind::Committed, "{st:?}");
            }
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            None
        };
        let mut seen = Vec::new();
        for _ in 0..4 {
            match run() {
                None => return,
                Some(why) => {
                    eprintln!("run disturbed ({why}): running it again");
                    seen.push(why);
                }
            }
        }
        panic!("every run was disturbed: {seen:?}");
    }

    /// A replicated engine of two voters with `outputs` outputs and no
    /// input, its status changes and verdicts, in a fresh domain.
    fn outputs_only(
        name: &str,
        outputs: usize,
    ) -> (
        Engine,
        mpsc::Receiver<StatusChange>,
        mpsc::Receiver<BlockVerdict>,
        std::path::PathBuf,
    ) {
        outputs_of(name, outputs, 2)
    }

    /// [`outputs_only`] with `voters` voters.
    fn outputs_of(
        name: &str,
        outputs: usize,
        voters: usize,
    ) -> (
        Engine,
        mpsc::Receiver<StatusChange>,
        mpsc::Receiver<BlockVerdict>,
        std::path::PathBuf,
    ) {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let outs: Vec<_> = (0..outputs)
            .map(|o| {
                let id = format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d2{o}");
                serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": name,
                                    "group": name, "role": "Audio", "channels": 1 })
            })
            .collect();
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": outs,
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(1);
        let (status_tx, changes) = mpsc::sync_channel(1 << 20);
        let (verdict_tx, verdicts) = mpsc::sync_channel(1 << 20);
        // Hashed, so the clock-step and skip paths run through the page
        // hashes too (also under RealtimeSanitizer); nobody reads them.
        let (hash_tx, _) = mpsc::sync_channel(1);
        let replication = Replication {
            voters,
            reports: report_tx,
            statuses: Some(status_tx),
            hashes: Some(hash_tx),
        };
        let e = Engine::start_replicated(config, process, Some(verdict_tx), replication).unwrap();
        wait(&|| e.status().outputs.iter().all(|o| o.realised));
        (e, changes, verdicts, domain)
    }

    /// Commits every output from the next page boundary on, F past every
    /// block; the block that boundary is in.
    fn committing(e: &Engine, outputs: usize) -> u64 {
        e.set_fence(u64::MAX).unwrap();
        let from = e.counters().processed.load(Acquire) + 2;
        for o in 0..outputs {
            e.commit_from(o, from).unwrap();
        }
        let kp = (from * 48).next_multiple_of(crate::blocks::PAGE) / 48;
        wait(&|| e.counters().processed.load(Acquire) > kp + 5);
        kp
    }

    /// Takes the clock shift off again, also when a test fails.
    struct Unshift;

    impl Drop for Unshift {
        fn drop(&mut self) {
            crate::rt_loop::CLOCK_STEP_NS.store(0, Relaxed);
            crate::rt_loop::CLOCK_SHIFT_NS.store(0, Relaxed);
        }
    }

    /// A forward step of libmxl's clock withholds the blocks it skips at
    /// the skip and fences the blocks after it: none commits until the
    /// step is cleared and a commit start names the output again. The
    /// TAI offset is sampled.
    #[test]
    fn a_forward_clock_step_withholds_the_skip_and_fences() {
        use crate::blocks::Kind;
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let _unshift = Unshift;
        let (e, changes, _verdicts, domain) = outputs_only("forward", 1);
        let processed = || e.counters().processed.load(Acquire);
        committing(&e, 1);
        wait(&|| e.status().clock.tai_offset_s.is_some());
        assert!(!e.status().clock.step_fenced);

        let steps = || e.counters().clock_steps.load(Relaxed);
        let before = steps();
        crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
        wait(&|| steps() > before);
        // The step's skip: the last one of 50 blocks or more by now; an
        // overrun under load can skip as well.
        std::thread::sleep(Duration::from_millis(20));
        let mut seen: Vec<_> = changes.try_iter().collect();
        let (from, to) = seen
            .iter()
            .rev()
            .find_map(|c| match *c {
                StatusChange::Skipped { from, to } if to - from >= 50 => Some((from, to)),
                _ => None,
            })
            .expect("no skip");
        wait(&|| processed() > to + 100);
        // From the skip on nothing commits while the step fences: blocks
        // are withheld or wait fenced.
        seen.extend(changes.try_iter());
        let after: Vec<_> = seen
            .into_iter()
            .filter_map(|c| match c {
                StatusChange::Block { status, .. } if status.k >= from => Some(status),
                _ => None,
            })
            .collect();
        assert!(after.iter().all(|s| s.kind != Kind::Committed), "{after:?}");
        assert!(after.iter().any(|s| s.kind == Kind::Fenced), "{after:?}");
        wait(&|| e.status().clock.step_fenced);

        e.clear_step_fence(1).unwrap();
        wait(&|| !e.status().clock.step_fenced);
        // A commit start names the output again; the fills the withdrawn
        // replica held meanwhile commit on evidence, then the blocks
        // after the start commit.
        let start = processed() + 2;
        e.commit_from(0, start).unwrap();
        let kp = (start * 48).next_multiple_of(crate::blocks::PAGE) / 48;
        let t = Instant::now();
        while e.block_status(0, kp + 3).map(|s| s.kind) != Some(Kind::Committed) {
            assert!(
                t.elapsed() < Duration::from_secs(5),
                "{:?}",
                e.block_status(0, kp + 3)
            );
            for k in to..processed() {
                let _ = e.commit_through(0, k, 0);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// With 16-sample blocks and a short hold a page is 64 blocks, as
    /// many as four times the hold ring: a block a page after the
    /// first block of a page, withheld ahead by the supervisor, must not
    /// take that block's status before the page settles, which reads it.
    #[test]
    fn a_status_set_ahead_keeps_a_page_committed_at_blocks_of_16() {
        use crate::blocks::PAGE;
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let run = || -> Option<String> {
            let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
            let domain = std::env::temp_dir().join(format!("samplez-b16-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&domain);
            std::fs::create_dir(&domain).unwrap();
            let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d61";
            let v = serde_json::json!({
                "libmxl": libmxl,
                "blockSize": 16,
                // A stall under load is caught up rather than skipped.
                "maxCatchupBlocks": 300,
                "holdMs": 2,
                "contactWindowMs": 2,
                "domains": [{ "name": "d", "path": domain }],
                "outputs": [{ "name": id, "domain": "d", "flowId": id, "label": "b16",
                              "group": "b16", "role": "Audio", "channels": 1 }],
                "matrix": [],
                "rt": { "enabled": false, "rtCpu": "none" },
            });
            let config = Config::parse(&v.to_string()).unwrap();
            assert!(4 * config.hold_blocks() <= 64, "{}", config.hold_blocks());
            let process = rt::setup_process(&config.rt);
            let (report_tx, _reports) = mpsc::sync_channel(64);
            let (hash_tx, hashes) = mpsc::sync_channel(1 << 16);
            let replication = Replication {
                voters: 2,
                reports: report_tx,
                statuses: None,
                hashes: Some(hash_tx),
            };
            let e = Engine::start_replicated(config, process, None, replication).unwrap();
            let processed = || e.counters().processed.load(Acquire);
            wait(&|| e.status().outputs[0].realised);
            e.set_fence(u64::MAX).unwrap();
            let c = e.counters();
            // A skip or an overflow from the commit start on withdraws
            // the replica.
            let base = (
                c.overrun_skipped_samples.load(Relaxed),
                c.hold_overflows.load(Relaxed),
            );
            e.commit_from(0, processed() + 2).unwrap();
            let per = PAGE / 16;
            let k0 = (processed() + 4) / per * per + per;
            wait(&|| processed() > k0);
            let disturbed = if processed() > k0 + per / 2 {
                Some(format!("block {k0} computed late"))
            } else {
                assert!(
                    e.withhold(0, k0 + per, Cause::FenceDeadline)
                        .unwrap()
                        .is_some()
                );
                let page = k0 * 16 / PAGE;
                let t = Instant::now();
                let record = loop {
                    assert!(
                        t.elapsed() < Duration::from_secs(5),
                        "page {page} not settled"
                    );
                    match hashes.recv_timeout(Duration::from_millis(100)) {
                        Ok(h) if h.page >= page => break h,
                        _ => {}
                    }
                };
                let skipped = c.overrun_skipped_samples.load(Relaxed) - base.0;
                let overflows = c.hold_overflows.load(Relaxed) - base.1;
                if skipped > 0 || overflows > 0 {
                    Some(format!("{skipped} samples skipped, {overflows} overflows"))
                } else {
                    assert_eq!(record.page, page, "{record:?}");
                    assert!(record.committed, "{record:?}");
                    None
                }
            };
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            disturbed
        };
        let mut seen = Vec::new();
        for _ in 0..4 {
            match run() {
                None => return,
                Some(why) => {
                    eprintln!("run disturbed ({why}): running it again");
                    seen.push(why);
                }
            }
        }
        panic!("every run was disturbed: {seen:?}");
    }

    /// An `InputDown`, `InputUp` or `Route` entry that reaches the RT
    /// thread after its index, while it still computes the page holding
    /// that index, is reported late, not applied, and left out of the
    /// page's fingerprint.
    #[test]
    fn a_late_input_entry_is_left_out_of_the_fingerprint() {
        use crate::blocks::{HEALTHY, PAGE};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        // A run an overrun skip or a slow pickup disturbed is run again.
        let run = || -> Option<String> {
            let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
            let domain =
                std::env::temp_dir().join(format!("samplez-latefp-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&domain);
            std::fs::create_dir(&domain).unwrap();
            let (src, next, out) = (
                "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d51",
                "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d52",
                "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d53",
            );
            let v = serde_json::json!({
                "libmxl": libmxl,
                "domains": [{ "name": "d", "path": domain }],
                "inputs": [{ "name": "in", "domain": "d", "flowId": src, "channels": 1 }],
                "outputs": [{ "name": out, "domain": "d", "flowId": out, "label": "latefp",
                              "group": "latefp", "role": "Audio", "channels": 1 }],
                "matrix": [[1.0]],
                "rt": { "enabled": false, "rtCpu": "none" },
            });
            let config = Config::parse(&v.to_string()).unwrap();
            let process = rt::setup_process(&config.rt);
            let (report_tx, _reports) = mpsc::sync_channel(64);
            let (hash_tx, hashes) = mpsc::sync_channel(1 << 16);
            let replication = Replication {
                voters: 2,
                reports: report_tx,
                statuses: None,
                hashes: Some(hash_tx),
            };
            let e = Engine::start_replicated(config, process, None, replication).unwrap();
            let processed = || e.counters().processed.load(Acquire);
            wait(&|| e.status().outputs[0].realised);
            // Hashed from the writer's first block on: the page it falls
            // in is not hashed whole.
            let begun = processed() * 48 / PAGE;
            // A few blocks into a later page, with room left in it.
            let t = Instant::now();
            let p0 = loop {
                let p = processed();
                let off = p * 48 % PAGE;
                if p * 48 / PAGE > begun && off >= 4 * 48 && off + 10 * 48 < PAGE {
                    break p;
                }
                assert!(t.elapsed() < Duration::from_secs(5), "no page entered");
                std::thread::sleep(Duration::from_micros(200));
            };
            let page = p0 * 48 / PAGE;
            let at = (page * PAGE).next_multiple_of(48);
            assert!(at < p0 * 48);
            e.append(1, at, Entry::InputDown { slot: 0 }).unwrap();
            e.append(2, at, Entry::InputUp { slot: 0 }).unwrap();
            let route = Entry::Route {
                slot: 0,
                flow_id: next.to_owned(),
            };
            e.append(3, at, route).unwrap();
            e.commit(3);
            wait(&|| e.counters().entries_consumed.load(Acquire) >= 3);
            let p1 = processed();
            let c = e.counters();
            let disturbed = if p1 * 48 / PAGE != page {
                Some(format!("entries taken in block {p1}, past page {page}"))
            } else {
                assert_eq!(c.command_late.load(Relaxed), 3);
                let t = Instant::now();
                let record = loop {
                    assert!(
                        t.elapsed() < Duration::from_secs(5),
                        "page {page} not settled"
                    );
                    match hashes.recv_timeout(Duration::from_millis(100)) {
                        Ok(h) if h.page >= page => break h,
                        _ => {}
                    }
                };
                if record.page != page {
                    let skipped = c.overrun_skipped_samples.load(Relaxed);
                    let dropped = c.page_hashes_dropped.load(Relaxed);
                    assert!(
                        skipped > 0,
                        "no record for page {page} ({dropped} dropped, taken by block \
                         {p1}, from {p0}): {record:?}"
                    );
                    Some(format!("{skipped} samples skipped"))
                } else {
                    assert_eq!(record.fingerprint, HEALTHY, "{record:?}");
                    None
                }
            };
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            disturbed
        };
        let mut seen = Vec::new();
        for _ in 0..4 {
            match run() {
                None => return,
                Some(why) => {
                    eprintln!("run disturbed ({why}): running it again");
                    seen.push(why);
                }
            }
        }
        panic!("every run was disturbed: {seen:?}");
    }

    /// Page hashes through an overrun skip and clock steps either way:
    /// no page missing a skipped block gets a record, every page gets at
    /// most one, and pages before and after the skips get theirs. The
    /// RealtimeSanitizer gate runs it as well.
    #[test]
    fn page_hashes_leave_out_the_pages_a_skip_cut() {
        use crate::blocks::PAGE;
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let _unshift = Unshift;
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-hashes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d41";
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [{ "name": id, "domain": "d", "flowId": id, "label": "hashes",
                          "group": "hashes", "role": "Audio", "channels": 1 }],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(1);
        let (status_tx, changes) = mpsc::sync_channel(1 << 20);
        let (hash_tx, hashes) = mpsc::sync_channel(1 << 20);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: Some(status_tx),
            hashes: Some(hash_tx),
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().outputs[0].realised);
        committing(&e, 1);
        let first = processed();
        // A page hashed whole from the commit start on, before the stall.
        let mut pages: Vec<u64> = Vec::new();
        let t = Instant::now();
        while pages.last().is_none_or(|&p| p * PAGE < first * 48) {
            assert!(t.elapsed() < Duration::from_secs(10), "no page hashed");
            pages.extend(
                hashes
                    .recv_timeout(Duration::from_millis(100))
                    .map(|h| h.page),
            );
        }
        let stalled = processed();

        crate::rt_loop::STALL_MS.store(40, Relaxed);
        let mut seen = Vec::new();
        let t = Instant::now();
        while !seen
            .iter()
            .any(|c| matches!(c, StatusChange::Skipped { from, to } if to - from >= 20 && *from >= stalled))
        {
            assert!(t.elapsed() < Duration::from_secs(5), "no skip");
            seen.extend(changes.recv_timeout(Duration::from_millis(100)));
        }
        let steps = || e.counters().clock_steps.load(Relaxed);
        let before = steps();
        crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
        wait(&|| steps() > before);
        crate::rt_loop::CLOCK_STEP_NS.store(-60_000_000, Relaxed);
        wait(&|| steps() > before + 1);
        // A page hashed whole after the steps; an overrun skip under load
        // only delays it.
        let last = processed() * 48 / PAGE;
        let t = Instant::now();
        while pages.last().is_none_or(|&p| p <= last) {
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "no page after the steps"
            );
            pages.extend(
                hashes
                    .recv_timeout(Duration::from_millis(100))
                    .map(|h| h.page),
            );
        }
        // Every skip before that page's record was sent first.
        seen.extend(changes.try_iter());

        let skips: Vec<(u64, u64)> = seen
            .iter()
            .filter_map(|c| match *c {
                StatusChange::Skipped { from, to } if to > from => Some((from * 48, to * 48)),
                _ => None,
            })
            .collect();
        assert!(skips.len() >= 2, "{skips:?}");
        for &p in &pages {
            let (start, end) = (p * PAGE, (p + 1) * PAGE);
            assert!(
                !skips.iter().any(|&(f, t)| f < end && start < t),
                "page {p} hashed over a skip: {skips:?}"
            );
        }
        assert!(pages.windows(2).all(|w| w[0] < w[1]), "{pages:?}");
        // A page hashed whole between the commit start and the stall.
        let stall = skips
            .iter()
            .find(|s| s.1 - s.0 >= 20 * 48 && s.0 >= stalled * 48)
            .unwrap();
        assert!(
            pages
                .iter()
                .any(|&p| p * PAGE >= first * 48 && (p + 1) * PAGE <= stall.0),
            "{pages:?}"
        );
        assert_eq!(e.counters().page_hashes_dropped.load(Relaxed), 0);
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// A clean self-report clears only the steps detected before its
    /// reading began.
    #[test]
    fn a_step_during_the_reading_is_not_cleared_by_it() {
        let steps = AtomicU64::new(3);
        let (u, clear) = self_report(&steps, || {
            steps.fetch_add(1, Relaxed);
            Some(false)
        });
        assert_eq!((u, clear), (Some(false), Some(3)));
        assert_eq!(self_report(&steps, || Some(true)), (Some(true), None));
        assert_eq!(self_report(&steps, || None), (None, None));
    }

    /// One voter has no peer to clear a detected clock step: its next
    /// clean self-report lifts the step's fence, and it commits again.
    #[test]
    fn a_lone_voter_lifts_its_step_fence_on_a_clean_self_report() {
        use crate::blocks::{Cause, Kind};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let _unshift = Unshift;
        let (e, _changes, _verdicts, domain) = outputs_of("lone", 1, 1);
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().clock.unsynchronised.is_some());
        assert_eq!(
            e.status().clock.unsynchronised,
            Some(false),
            "the host's kernel clock is unsynchronised"
        );
        wait(&|| processed() > 0);
        let before = processed();
        let fenced = || e.counters().step_fenced.load(Relaxed);
        crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
        wait(&|| fenced());
        let t = Instant::now();
        wait(&|| !fenced());
        // adjtimex is read once a second.
        assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
        let k = processed() + 2;
        wait(&|| processed() > k);
        assert_eq!(e.block_status(0, k).map(|s| s.kind), Some(Kind::Committed));
        // One voter withholds the blocks the step fences.
        assert!(
            (before..k).any(|x| {
                e.block_status(0, x)
                    .is_some_and(|s| (s.kind, s.cause) == (Kind::Withheld, Cause::ClockStep))
            }),
            "no block was fenced"
        );
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// A backward step of libmxl's clock withholds every block not yet
    /// final in the hold ring, cause skip, and computes no block twice:
    /// the loop wakes without computing until the clock is due at the
    /// block after the last one again, and no output is written below
    /// its last commit.
    #[test]
    fn a_backward_clock_step_withholds_the_ring_and_computes_nothing_twice() {
        use crate::blocks::{Cause, Kind};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let _unshift = Unshift;
        let (e, _changes, verdicts, domain) = outputs_only("backward", 1);
        let c = e.counters();
        let processed = || c.processed.load(Acquire);
        committing(&e, 1);
        // F frozen: blocks wait fenced in the ring.
        let f = processed() + 2;
        e.set_fence(f * 48).unwrap();
        wait(&|| processed() > f + 10);
        let open = processed() - 1;
        assert_eq!(e.block_status(0, open).unwrap().kind, Kind::Fenced);

        crate::rt_loop::CLOCK_STEP_NS.store(-60_000_000, Relaxed);
        wait(&|| c.clock_behind_wakes.load(Relaxed) > 20);
        let st = e.block_status(0, open).unwrap();
        assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::Skip));
        // The loop wakes but computes nothing while behind.
        let (p0, h0) = (processed(), c.heartbeat_blocks.load(Relaxed));
        std::thread::sleep(Duration::from_millis(10));
        if c.clock_behind_wakes.load(Relaxed) < 50 {
            assert_eq!(processed(), p0);
        }
        assert!(c.heartbeat_blocks.load(Relaxed) > h0);
        wait(&|| processed() > p0 + 50);
        let ks: Vec<u64> = verdicts.try_iter().map(|v| v.k).collect();
        assert!(
            ks.windows(2).all(|w| w[1] > w[0]),
            "a block computed twice or out of order"
        );
        assert_eq!(c.output_held_blocks.load(Relaxed), 0);
        wait(&|| e.status().clock.step_fenced);
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// The engine's own log stamps an `InputDown` or `InputUp` that
    /// reached the RT thread late again, from the state the RT thread has:
    /// the input goes down once its flow is gone, and comes up once it is
    /// there again. Engine `p` writes the flow, engine `e` reads it.
    #[test]
    fn a_late_input_entry_of_the_own_log_is_stamped_again() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let shm = std::path::Path::new("/dev/shm");
        let base = if shm.is_dir() {
            shm.to_owned()
        } else {
            std::env::temp_dir()
        };
        let domain = base.join(format!("samplez-restamp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let (src, out) = (
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d01",
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d02",
        );
        let output = |id: &str| {
            serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": "restamp",
                                "group": "restamp", "role": "Audio", "channels": 1 })
        };
        // The reader's margin covers the producer's, which its head lags
        // the clock by.
        let start =
            |inputs: serde_json::Value, out: &str, margin: u32, matrix: serde_json::Value| {
                let v = serde_json::json!({
                    "libmxl": libmxl,
                    "readerMarginSamples": margin,
                    "domains": [{ "name": "d", "path": domain }],
                    "inputs": inputs,
                    "outputs": [output(out)],
                    "matrix": matrix,
                    "rt": { "enabled": false, "rtCpu": "none" },
                });
                let config = Config::parse(&v.to_string()).unwrap();
                let process = rt::setup_process(&config.rt);
                Engine::start(config, process, None).unwrap()
            };
        let producer = || {
            let p = start(serde_json::json!([]), src, 576, serde_json::json!([]));
            wait(&|| p.status().outputs[0].realised);
            p
        };
        let input = serde_json::json!([{ "name": "in", "domain": "d", "flowId": src,
                                         "channels": 1 }]);
        let p = producer();
        let e = start(input, out, 3 * 576, serde_json::json!([[1.0]]));
        let attached = || {
            let s = e.status();
            s.inputs[0].up && s.inputs[0].state == InputState::Attached
        };
        wait(&attached);
        let late = || e.counters().command_late.load(Relaxed);

        STAMP_LATE.store(true, Relaxed);
        p.stop();
        wait(&|| !e.status().inputs[0].up);
        assert_eq!(late(), 1);
        assert!(e.status().diverged.is_some());

        STAMP_LATE.store(true, Relaxed);
        let p = producer();
        wait(&attached);
        assert_eq!(late(), 2);
        e.stop();
        p.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// The RT loop's failure paths run, so that CI's RealtimeSanitizer
    /// run over the lib tests checks them: a stall's catch-up and skip, a
    /// forward and a backward clock step with the wakes behind the last
    /// block after it, and a late input with its probe, with a strip
    /// advanced over the skip. Engine `p` writes the flow,
    /// engine `e` reads it.
    #[test]
    fn the_failure_paths_run_on_the_rt_thread() {
        use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_NS, STALL_MS};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        /// Takes the clock shift off again, also when the test fails.
        struct Unshift;
        impl Drop for Unshift {
            fn drop(&mut self) {
                CLOCK_STEP_NS.store(0, Relaxed);
                CLOCK_SHIFT_NS.store(0, Relaxed);
            }
        }
        let _unshift = Unshift;
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-failures-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let (src, out) = (
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d05",
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d06",
        );
        let start = |inputs: serde_json::Value, out: &str, margin: u32, matrix| {
            let v = serde_json::json!({
                "libmxl": libmxl,
                "readerMarginSamples": margin,
                "domains": [{ "name": "d", "path": domain }],
                "inputs": inputs,
                "outputs": [{ "name": out, "domain": "d", "flowId": out, "label": "failures",
                              "group": "failures", "role": "Audio", "channels": 1 }],
                "matrix": matrix,
                "rt": { "enabled": false, "rtCpu": "none" },
            });
            let config = Config::parse(&v.to_string()).unwrap();
            let process = rt::setup_process(&config.rt);
            Engine::start(config, process, None).unwrap()
        };
        let p = start(serde_json::json!([]), src, 576, serde_json::json!([]));
        wait(&|| p.status().outputs[0].realised);
        let input = serde_json::json!([{ "name": "in", "domain": "d", "flowId": src,
                                         "channels": 1 }]);
        let e = start(input, out, 3 * 576, serde_json::json!([[1.0]]));
        wait(&|| e.status().inputs[0].state == InputState::Attached);
        // A strip with filters on: Strip::advance runs over the skip.
        let produced = e.counters.produced.load(Acquire);
        let at = (produced / 48 + 40) * 48;
        let mut params = vec![
            StripParam::Trim { db: -6.0 },
            StripParam::Hpf {
                enabled: true,
                freq: 100.0,
            },
            StripParam::Eq { enabled: true },
            StripParam::DynamicsBeforeEq { enabled: true },
        ];
        params.extend(
            samplez_dsp::strip::DEFAULT_BANDS
                .iter()
                .enumerate()
                .map(|(band, b)| StripParam::EqBand {
                    band,
                    value: samplez_dsp::EqBand { gain_db: 6.0, ..*b },
                }),
        );
        for param in params {
            e.set_strip(0, param, at).unwrap();
        }
        let c = e.counters();
        let n = |a: &AtomicU64| a.load(Relaxed);
        let until = |what: &str, f: &dyn Fn() -> bool| {
            let t = Instant::now();
            while !f() {
                assert!(t.elapsed() < Duration::from_secs(10), "no {what}: {c:?}");
                std::thread::sleep(Duration::from_millis(10));
            }
        };

        // A stall: the overdue blocks are caught up, the rest skipped.
        // Both engines' loops take the stall hook, so it is set again
        // until this engine's loop took one.
        until("skip", &|| {
            STALL_MS.store(30, Relaxed);
            std::thread::sleep(Duration::from_millis(50));
            n(&c.overrun_skipped_samples) > 0
        });
        STALL_MS.store(0, Relaxed);
        // A forward and a backward step of libmxl's clock: after the
        // backward one the output holds the blocks below its last commit.
        let steps = n(&c.clock_steps);
        CLOCK_STEP_NS.store(50_000_000, Relaxed);
        until("forward step", &|| n(&c.clock_steps) > steps);
        CLOCK_STEP_NS.store(-50_000_000, Relaxed);
        until("backward step", &|| n(&c.clock_steps) > steps + 1);
        // Behind the last block computed, the loop wakes without computing.
        until("wake behind", &|| n(&c.clock_behind_wakes) > 0);
        // The producer stops: the input is late, frozen and probed.
        p.stop();
        until("late block", &|| n(&c.input_late_blocks) > 0);
        until("probe", &|| n(&c.input_probes) > 0);
        assert_eq!(e.status().rt_panic, None);
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// A strip entry the slot's strip refuses on the RT thread, as on a
    /// replica whose slot has another width than the one the entry was
    /// checked against, is not applied: it is counted and marks the
    /// engine diverged.
    #[test]
    fn a_strip_entry_refused_on_the_rt_thread_marks_the_engine_diverged() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-refused-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d07", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d08", "label": "refused",
                          "group": "refused", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let e = Engine::start(config, process, None).unwrap();
        let c = &e.counters;
        let until = |what: &str, f: &dyn Fn() -> bool| {
            let t = Instant::now();
            while !f() {
                assert!(t.elapsed() < Duration::from_secs(10), "no {what}: {c:?}");
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        until("block", &|| c.heartbeat_blocks.load(Relaxed) > 0);
        // Past the checks of set_strip: leg 1 of a mono strip.
        let refused = StripParam::LegTrim { leg: 1, db: 0.0 };
        let mut log = e.log.lock().unwrap();
        let entry = RtEntry {
            id: log.last + 1,
            at_index: earliest(&e.api, &e.counters, e.margin, e.lead),
            change: Change::Strip {
                slot: 0,
                param: refused,
            },
        };
        log.append(entry, None, e.counters.entries_consumed.load(Acquire))
            .unwrap();
        log.commit(entry.id);
        drop(log);
        e.set_strip(0, StripParam::Trim { db: -3.0 }, entry.at_index)
            .unwrap();
        until("divergence", &|| e.status().diverged.is_some());
        let diverged = e.status().diverged.unwrap();
        assert!(
            diverged.contains("refused by its slot's strip"),
            "{diverged}"
        );
        // The entry set_strip took is applied; the supervisor's own
        // InputDown for the missing input may come in between.
        until("both entries", &|| c.entries_consumed.load(Relaxed) >= 2);
        assert_eq!(c.strip_refused.load(Relaxed), 1);
        assert_eq!(c.command_late.load(Relaxed), 0);
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    fn wait(f: &dyn Fn() -> bool) {
        let t = Instant::now();
        while !f() {
            assert!(t.elapsed() < Duration::from_secs(10), "timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_repeated_failure_is_logged_once_per_text() {
        let mut last = None;
        assert!(changed(&mut last, "a".to_owned()));
        assert!(!changed(&mut last, "a".to_owned()));
        assert!(changed(&mut last, "b".to_owned()));
        last = None;
        assert!(changed(&mut last, "b".to_owned()));
    }
}
