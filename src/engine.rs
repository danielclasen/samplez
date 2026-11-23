//! The engine: start-up, the supervisor thread and the status it keeps.
//!
//! The supervisor is the one thread that makes instance-mutating libmxl
//! calls. It builds input generations and output writers, hands them to
//! the RT thread through the command ring, destroys what comes back
//! through the return ring, prunes and re-adds inputs, retries output
//! creation and turns telemetry into status and log lines.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicBool, AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release,
};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use samplez_dsp::duck::{DEPTH_DB, MAX_KEYS};
use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{
    Delay, DelayRings, DuckParam, Limiter, LimiterParams, MAX_DUCKS, Matrix, PlanarBuffer, Started,
    Strip, StripParam, Tone, ToneParams, ToneRamps,
};
use samplez_mxl::input::{
    Counters as InputCounters, Generation, GroupWait, Input, InputConfig, Poll, PollOpener,
    PolledFlow, RECOVERY_NS, Recovery, Releases, Timing,
};
use samplez_mxl::lookup::{self, Found, Gate};
use samplez_mxl::output::{self, Creation};
use samplez_mxl::{Instance, MxlApi, time};

use crate::batch::{self, Batch, Item};
use crate::blocks::{
    BlockStatus, Cause, Hold, PAGE, PageHash, Pages, Ranks, Standing, StatusChange, Statuses,
};
use crate::config::Config;
use crate::ducks::{DuckMeter, DuckRecord, DuckSetting};
use crate::meters::{InputMeters, OutputLoudness, OutputMeters};
use crate::rt::{self, ProcessStatus, RtStatus, Step, ThreadStatus};
use crate::rt_loop::{
    BlockVerdict, Change, Command, Control, Counters, Gen, LogId, MeterRecord, Metered, Mix, Out,
    OutMeter, PENDING_CHANGES, Replica, Returned, RingEntry, Rt, RtEntry, SLICE, SPENT, Shared,
    Slot, SlotStats, Telemetry, UNRESOLVED, bit, slots_of,
};
use crate::transfer::{
    Answer, CopyError, Core, CoreCopy, Handover, ProductState, Replay, ReplayError, items_of,
};

const COMMAND_RING: usize = 64;
const RETURN_RING: usize = 64;
const TELEMETRY_RING: usize = 8192;
/// Seconds of meter records the meter ring holds.
const METER_RING_SECONDS: usize = 2;
/// Status changes the change ring holds per output, and the fewest
/// status words per output.
const CHANGES_PER_OUTPUT: usize = 1024;
/// Page hashes the hash ring holds per output: 1.4 s of pages at 48 kHz.
const HASHES_PER_OUTPUT: usize = 64;
const MIN_STATUS_SLOTS: usize = 64;
const SUPERVISOR_TICK: Duration = Duration::from_millis(5);
const POLL_PERIOD: Duration = Duration::from_millis(100);
/// How long a build for a product state, or a replay, waits for the
/// lookups of the flows it attaches. One that answers at once does so in
/// microseconds; a flow not found by then is left out, its lookup kept
/// running, rather than waited for by libmxl's own open on a file system
/// that waits for a missing name to appear.
const GATE_WAIT: Duration = Duration::from_millis(20);
const BACKOFF_FIRST: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Seconds of slot statistics the margin warning looks at, plus one.
const MARGIN_WINDOW: usize = 11;
/// The build time assumed before a generation was built.
const UNMEASURED_BUILD: Duration = Duration::from_millis(100);
/// The least build time buildLead covers, whatever the build times: a
/// libmxl reader open was measured at up to 23 ms, unexplained, against
/// 30-150 us for most, and builds that open little or nothing would
/// otherwise set a lead too short for one.
const BUILD_FLOOR: Duration = Duration::from_millis(30);
/// Quarter-octave buckets of the build-time histogram, up to 2^64 ns.
const BUILD_BUCKETS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputState {
    /// In the RT thread's generation.
    Attached,
    /// Not in the generation; polled until its head advances again.
    Pruned,
    /// Detached by a `Detach` entry: reads no flow, is not polled and
    /// not reported until a `Route` attaches it again.
    Detached,
    /// A tone generator: reads no flow, is not polled, never late, pruned
    /// or down.
    Tone,
}

#[derive(Clone, Debug)]
pub struct InputStatus {
    pub name: String,
    pub state: InputState,
    /// The input's state in the log: up from an `InputUp`, down from an
    /// `InputDown` entry, applied by the RT thread.
    pub up: bool,
    /// Whether the engine reads the input's flow, for input liveness:
    /// `Some(true)` attached, or down in the log and polled with its head
    /// advancing for a second as `InputReadable` asks; `Some(false)`
    /// otherwise pruned; `None` while it waits for its generation, as from
    /// start-up to the first build, or is detached, which says nothing of
    /// its flow.
    pub readable: Option<bool>,
    /// The flow the input reads, changed by `Route` entries; for a
    /// detached input, the flow it read last, empty for one that has read
    /// none.
    pub flow_id: String,
    /// Why the last attach failed, or how far the flow's head stays off
    /// the MXL clock (README "Real-time requirements").
    pub error: Option<String>,
    /// The margin the producer's `maxCommitBatchSizeHint` needs, when the
    /// configured margin is below it.
    pub margin_short: Option<u64>,
    /// The margin the observed late blocks need: late blocks while the head
    /// advances at the nominal rate within 1 % over 10 s and some blocks
    /// are on time.
    pub margin_warning: Option<u64>,
    pub counters: InputCounters,
    /// The input after its strip, read or silenced.
    pub meters: InputMeters,
    /// The input after its delay and before its strip, where its config
    /// sets `preStripMeter` and the log has the meter on
    /// ([`Entry::PreStripMeter`]); none otherwise. Switched on, it reads
    /// what was metered since alone.
    pub pre_strip_meters: Option<InputMeters>,
}

#[derive(Clone, Debug)]
pub struct OutputStatus {
    pub name: String,
    /// Enabled by the log: a disabled output has no writer and no flow.
    pub enabled: bool,
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

/// A configured meter: matrix columns written to no flow.
#[derive(Clone, Debug)]
pub struct MeterStatus {
    pub name: String,
    pub meters: OutputMeters,
}

/// The generation-needing entry (`InputUp`, `Route`, `Detach`) the
/// engine builds or holds, and the build times that set how far ahead
/// such entries are stamped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GenerationStatus {
    /// The log id of the entry; at most one generation is pending.
    pub pending: Option<LogId>,
    /// Its generation is built.
    pub ready: bool,
    /// Generations built.
    pub builds: u64,
    /// The 99.99th percentile of the latest 256 build times, each the
    /// longest of its new readers' opens (instance, lookup, reader, ring
    /// pre-fault) and its assembly; a build that opens nothing is not
    /// counted, a late reader's open up to 100 ms, ns.
    pub build_p9999_ns: Option<u64>,
    /// buildLead: samples a generation-needing entry is stamped ahead of
    /// the earliest index a gain change could take, covering that
    /// percentile, a supervisor tick and the hand-over margin of its
    /// deadline.
    pub build_lead: u64,
    /// The latest readers left out of a generation for an open not done
    /// by its deadline, at most 16: slot, the entry's index, and how long
    /// the open had run, ns, none for one not started.
    pub left_out: Vec<(usize, u64, Option<u64>)>,
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
    pub meters: Vec<MeterStatus>,
    /// Replicated mode with more than one voter: replay speed s, the
    /// blocks a replay clone computes per block period, measured at
    /// start-up on silence.
    pub replay_speed: Option<f64>,
    /// Spent batches the RT thread leaked rather than free them: its
    /// return ring and spent list were full ([`Counters::batches_leaked`]).
    pub batches_leaked: u64,
    /// Per duck group, [`MAX_DUCKS`] of them, its readings over the last
    /// meter slice it was in use in.
    pub ducks: Vec<DuckMeter>,
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
    /// `at_index` is below `earliest`, the first index the engine
    /// accepts, a multiple of the block size.
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

/// Why [`Engine::set_duck`] or [`Engine::set_duck_mask`] refused a
/// setting: for what a gain change is refused for, and as invalid for a
/// setting [`Engine::admits`] refuses.
pub type DuckError = GainError;

/// A gain fade as a function of the sample index alone: the coefficient
/// holds `from` below `start`, ramps linearly to its target over `len`
/// samples from `start` (value `from + (n - start) * step`, `step =
/// (target - from) / len`, as [`samplez_dsp::Ramp`] computes it), and
/// holds the target after. `start` is an absolute sample index, ahead of
/// the entry's index (a delay), at it, or behind it (a fade resumed
/// mid-ramp); `from` is the value the coefficient had at `start`. An
/// engine applying the entry renders this function whatever the value it
/// held before. With no fade a gain change ramps over the ramp length
/// from the value held at the entry's index.
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Fade {
    pub from: f32,
    pub start: u64,
    pub len: u32,
}

impl Fade {
    /// A fade of `len` samples after `delay` samples from index `at`,
    /// starting from `from`.
    pub fn after(from: f32, at: u64, delay: u32, len: u32) -> Fade {
        Fade {
            from,
            start: at + u64::from(delay),
            len,
        }
    }

    /// Whether `from` is a valid gain and `len` is within
    /// [`samplez_dsp::MAX_EXACT_RAMP_LEN`].
    pub fn is_valid(&self) -> bool {
        samplez_dsp::valid_gain(self.from) && self.len <= samplez_dsp::MAX_EXACT_RAMP_LEN
    }

    /// Whether the fade is valid and starts at most
    /// [`samplez_dsp::MAX_EXACT_RAMP_LEN`] samples past the entry index
    /// `at`: what an entry may carry, so that every replica, whichever
    /// index it resumes from, starts the ramp at the same index.
    pub fn within(&self, at: u64) -> bool {
        self.is_valid()
            && self.start <= at.saturating_add(u64::from(samplez_dsp::MAX_EXACT_RAMP_LEN))
    }

    /// The ramp of this fade towards `to`, positioned at sample index
    /// `at`; none once it is over, or when an end is not a valid gain. A
    /// `start` more than [`samplez_dsp::MAX_EXACT_RAMP_LEN`] samples past
    /// `at` waits that long instead.
    pub fn ramp_at(&self, to: f32, at: u64) -> Option<samplez_dsp::Ramp> {
        let ahead = u32::try_from(self.start.saturating_sub(at))
            .unwrap_or(u32::MAX)
            .min(samplez_dsp::MAX_EXACT_RAMP_LEN);
        let done = u32::try_from(at.saturating_sub(self.start)).unwrap_or(u32::MAX);
        let r = samplez_dsp::Ramp::resume(self.from, to, ahead, self.len, done)?;
        r.is_active().then_some(r)
    }

    /// The value at sample index `at` of the fade towards `to`.
    pub fn value_at(&self, to: f32, at: u64) -> f32 {
        self.ramp_at(to, at).map_or(to, |r| r.current())
    }

    /// The fade an entry carrying this one towards `to`, applied at index
    /// `at` to a coefficient holding `held`, puts the coefficient on:
    /// this one where it starts at or after `at`, or where the
    /// coefficient is already on its value at `at`, so that an entry
    /// repeating it there, as [`Targets::assert`](crate::Targets::assert)
    /// gives, changes no bit; else, started, a straight line from `held`
    /// to `to` that ends where this one ends and takes at least `ramp`
    /// samples, so a late entry joins the fade without a step; and over,
    /// `ramp` samples from `held`, as a change with no fade. None where
    /// that leaves nothing to ramp: `held` is `to`, or the length is 0.
    pub fn joined(&self, to: f32, held: f32, at: u64, ramp: u32) -> Option<Fade> {
        if at <= self.start || held.to_bits() == self.value_at(to, at).to_bits() {
            return Some(*self);
        }
        let end = self.start.saturating_add(u64::from(self.len));
        let len = u32::try_from(end.saturating_sub(at))
            .unwrap_or(u32::MAX)
            .max(ramp);
        (held != to && len != 0).then_some(Fade {
            from: held,
            start: at,
            len,
        })
    }
}

/// A log entry for the engine. Each sets an absolute target, so one
/// applied again once its ramp has ended changes nothing.
#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    /// Ramps the gain from input channel `input` to matrix column
    /// `output`, an output channel or after them a meter channel, to
    /// `gain`: over the ramp length from the entry's index, or as the
    /// absolute `fade` says, which must be [`Fade::within`] the index.
    Gain {
        input: usize,
        output: usize,
        gain: f32,
        fade: Option<Fade>,
    },
    /// Input slot `slot` is silent from the entry's index on, whatever is
    /// readable.
    InputDown { slot: usize },
    /// Input slot `slot` fades in from the entry's index, its reader
    /// recreated in a new generation.
    InputUp { slot: usize },
    /// Input slot `slot` crossfades to flow `flow_id` over the ramp
    /// length from the entry's index; a detached slot fades in from
    /// silence.
    Route { slot: usize, flow_id: String },
    /// Input slot `slot` crossfades to silence over the ramp length from
    /// the entry's index and reads no flow until a `Route`: its reader
    /// is dropped, the flow is not polled and no `InputPruned` or
    /// `InputReadable` is reported for it. Its up or down state is kept.
    Detach { slot: usize },
    /// Output `slot` is written from the entry's index on, or disabled:
    /// nothing is written to it from the index on, its writer is
    /// released and its flow deleted, unless another writer holds the
    /// flow. Enabled again, its flow is created as at start-up, and an
    /// output another writer holds the flow of is not realised. Every
    /// output starts enabled.
    Output { slot: usize, enabled: bool },
    /// Sets parameter `param` of input slot `slot`'s strip from the
    /// entry's index, ramping over the ramp length.
    Strip { slot: usize, param: StripParam },
    /// Sets parameter `param` of the strip of output `output`, one
    /// configured with `strip`, from the entry's index, ramping over the
    /// ramp length.
    OutputStrip { output: usize, param: StripParam },
    /// Sets the limiter of output `output`, one configured with
    /// `limiter`, from the entry's index: switching it on or off ramps
    /// over the ramp length.
    Limiter {
        output: usize,
        params: LimiterParams,
    },
    /// Delays input slot `slot` by `samples` from the entry's index,
    /// crossfading from the delay before over the ramp length, or after
    /// a crossfade still running. At most the slot's `maxDelaySamples`;
    /// 0 on a slot without a delay line sets nothing.
    Delay { slot: usize, samples: u32 },
    /// Delays output `output` by `samples` from the entry's index, as
    /// `Delay` does an input slot: on the output's strip's output, ahead
    /// of its limiter. At most the output's `maxDelaySamples`; 0 on an
    /// output without a delay line sets nothing.
    OutputDelay { output: usize, samples: u32 },
    /// Sets the tone generator of input slot `slot`, one configured with
    /// `tone`, from the entry's index: `frequency_hz` within
    /// [`samplez_dsp::TONE_FREQUENCY_HZ`], crossfading from the sine
    /// before over the ramp length, or after a crossfade still running;
    /// `level_db`, the sample peak in dBFS within
    /// [`samplez_dsp::TONE_LEVEL_DB`], ramping over the ramp length; and
    /// `ident`, which cuts the legs in turn, switched on or off over the
    /// ramp length ([`samplez_dsp::tone`]).
    Tone {
        slot: usize,
        frequency_hz: u32,
        level_db: f32,
        ident: bool,
    },
    /// Sets `setting` of duck group `group`, below [`MAX_DUCKS`], from the
    /// entry's index.
    Duck { group: usize, setting: DuckSetting },
    /// Sets the duck mask of the coefficient from input channel `input`
    /// to matrix column `output`, a column the configuration names in
    /// `duckColumns` unless the mask is 0: bit `g` ducks it by group `g`.
    /// A coefficient that is not 0 crossfades from the old mask's gain to
    /// the new one's over the ramp length.
    DuckMask {
        input: usize,
        output: usize,
        mask: u8,
    },
    /// Runs the pre-strip meter of input slot `slot`, one configured with
    /// `preStripMeter`, from the entry's index on, or stops it: while it
    /// is off the slot's block is neither copied nor metered before the
    /// strip and the input's status carries no `pre_strip_meters`.
    /// Switched on, its measurement starts afresh at the first 10 ms
    /// slice at or after the index. Every pre-strip meter starts off;
    /// off is taken on any slot.
    PreStripMeter { slot: usize, enabled: bool },
    /// Runs the loudness measurement of `meter`, an output or after the
    /// outputs a configured meter, from the entry's index on, or pauses
    /// it: every 10 ms slice with a sample from a paused span is left out
    /// of the integrated loudness, the loudness range and the maxima,
    /// and counted paused, while the momentary and short-term loudness
    /// keep running. Every meter starts running.
    LoudnessRun { meter: usize, running: bool },
    /// Restarts the loudness measurement of `meter` at the first 100 ms
    /// window starting at or after the entry's index, as
    /// [`Engine::reset_loudness`] describes.
    ResetLoudness { meter: usize },
}

/// Why the engine refused an entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryError {
    /// No such slot or coefficient, a gain that is not finite within
    /// `MAX_GAIN`, a flow an input may not read, a strip parameter the
    /// slot's strip does not take, an index that is not a multiple of the
    /// block size, or an entry the engine's log mode does not take.
    Invalid(String),
    /// [`PENDING_CHANGES`] entries are outstanding on an engine keeping
    /// its own log; a replicated log refuses none for room.
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
    /// The input's head has advanced for a second while it is down.
    InputReadable { slot: usize },
}

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
    /// The 99.99th percentile of the generation build times, or the
    /// build time assumed before any build, a supervisor tick and the
    /// hand-over margin of its deadline, in samples: buildLead
    /// without the engine's lead, how long a generation takes once its
    /// entry reaches the engine.
    pub build_time: u64,
    /// Times the engine dropped its core for committed entries its RT
    /// thread did not take ([`Engine::commit`]).
    pub log_overflows: u64,
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
    /// The number of controls pushed to the ring and of hand-overs found
    /// installed: a copy buffer sent before either is filled again.
    sent: u64,
    /// The RT thread holds a core, or will once it takes the controls
    /// pushed: false from a `DropCore` pushed until a hand-over installs
    /// one.
    core: bool,
}

/// The off-RT side of core copies: at most one buffer is with the RT
/// thread at a time.
struct Transfer {
    cores: rtrb::Producer<Box<Core>>,
    copies: rtrb::Consumer<CoreCopy>,
    in_flight: bool,
    /// The number of controls pushed when the buffer in flight was sent,
    /// all of them taken by the RT thread before it fills that buffer.
    in_flight_after: u64,
    handovers: rtrb::Producer<Handover>,
    handed: rtrb::Consumer<Handover>,
    /// A hand-over is with the RT thread.
    handing: bool,
    /// The number of the last hand-over sent.
    seq: u64,
    /// A core buffer not in use, for the next copy or hand-over.
    spare: Option<Box<Core>>,
    /// Input and output channels, input slots and the room for one
    /// page's entries: the shape of a core buffer.
    shape: Shape,
    slack_blocks: u64,
}

/// What the supervisor learns of the log besides committed entries.
enum LogMsg {
    /// A generation-needing entry was appended: its generation is built
    /// on receipt.
    Append {
        id: LogId,
        at_index: u64,
        /// The slots the entry raises, by bit.
        up: u64,
        /// The slots it re-routes, with their new flows, none for one it
        /// detaches.
        routes: Vec<(usize, Option<String>)>,
    },
    /// Uncommitted entries from `from` on were removed.
    Truncate { from: LogId },
    /// Uncommitted entry `id` was removed.
    Skip { id: LogId },
    /// Committed entries up to `upto` were dropped before they reached
    /// the RT thread, and the core with them.
    Dropped { upto: LogId },
    /// A core built from product state is about to be installed: per
    /// slot whether it is up, the flow it reads, none when detached, and
    /// the flow a crossfade still running leaves with the index the
    /// crossfade ends at. The generations reading them are handed to the
    /// RT thread before `done` is sent.
    Product {
        up: Vec<bool>,
        flows: Vec<Option<String>>,
        leaving: Vec<Option<(u64, String)>>,
        done: SyncSender<()>,
    },
}

/// Appended entries not yet committed, and the producer of the RT
/// thread's entry ring. Committed entries go to the RT thread by log id.
struct Log {
    ring: rtrb::Producer<RingEntry>,
    sup: mpsc::Sender<LogMsg>,
    uncommitted: VecDeque<RingEntry>,
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
    /// Appends `e`. The engine's own log refuses it while
    /// [`PENDING_CHANGES`] entries are outstanding, `consumed` being the RT
    /// thread's count of entries taken off its list, so its committed
    /// entries never exceed what the RT thread holds. A replicated log
    /// refuses no entry for room: its leader refuses one before ordering
    /// it, and a committed entry the ring has no room for waits here until
    /// it has ([`Log::flush`]), applied late should its index pass.
    fn append(
        &mut self,
        e: RtEntry,
        batch: Option<Arc<Batch>>,
        routes: Vec<(usize, Option<String>)>,
        consumed: u64,
    ) -> Result<(), EntryError> {
        if e.id <= self.last {
            return Err(EntryError::Order {
                id: e.id,
                last: self.last,
            });
        }
        let outstanding = self.submitted - consumed + self.uncommitted.len() as u64;
        if self.local.is_some() && outstanding >= PENDING_CHANGES as u64 {
            return Err(EntryError::Full);
        }
        let (up, _) = e.change.generating();
        if e.change.needs_generation() {
            // The supervisor outlives every caller.
            let _ = self.sup.send(LogMsg::Append {
                id: e.id,
                at_index: e.at_index,
                up,
                routes,
            });
        }
        self.uncommitted.push_back(RingEntry { entry: e, batch });
        self.last = e.id;
        Ok(())
    }

    fn commit(&mut self, up_to: LogId) {
        self.committed = self.committed.max(up_to.min(self.last));
        self.flush();
    }

    /// Pushes the committed entries waiting to the ring while it has
    /// room, in log order.
    fn flush(&mut self) {
        while let Some(q) = self.uncommitted.pop_front() {
            if q.entry.id > self.committed {
                self.uncommitted.push_front(q);
                return;
            }
            if let Err(rtrb::PushError::Full(q)) = self.ring.push(q) {
                self.uncommitted.push_front(q);
                return;
            }
            self.submitted += 1;
        }
    }

    /// The committed entries waiting for room in the ring.
    fn waiting(&self) -> usize {
        self.uncommitted
            .iter()
            .take_while(|q| q.entry.id <= self.committed)
            .count()
    }

    /// Drops the committed entries waiting for room in the ring, and the
    /// generations built for them: the core no longer follows the log.
    fn drop_waiting(&mut self) {
        let n = self.waiting();
        let last = self.uncommitted.drain(..n).next_back().map(|q| q.entry.id);
        if let Some(upto) = last {
            let _ = self.sup.send(LogMsg::Dropped { upto });
        }
    }

    /// Removes uncommitted entry `id`, and the generation built for it;
    /// false when no uncommitted entry has that id.
    fn skip(&mut self, id: LogId) -> bool {
        let before = self.uncommitted.len();
        let committed = self.committed;
        self.uncommitted
            .retain(|q| q.entry.id != id || q.entry.id <= committed);
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
        self.uncommitted.retain(|q| q.entry.id < from);
        self.last = self.last.min(from - 1);
        let _ = self.sup.send(LogMsg::Truncate { from });
    }
}

/// A product state's strip parameters, per slot or output, folded to the
/// last value per parameter key, off the RT thread: it sets at most one
/// per slot or output and key at an install, however long the lists.
fn product_strips(strips: &[Vec<StripParam>]) -> Vec<(usize, StripParam)> {
    let mut folded = std::collections::BTreeMap::new();
    for (n, params) in strips.iter().enumerate() {
        for &param in params {
            folded.insert((n, batch::strip_key(&param)), param);
        }
    }
    folded.into_iter().map(|((n, _), p)| (n, p)).collect()
}

/// Whether output `output` exists, has a limiter and the limiter takes
/// `params`.
fn check_limiter(config: &Config, output: usize, params: &LimiterParams) -> Result<(), String> {
    let Some(o) = config.outputs.get(output) else {
        return Err(format!("no output {output}"));
    };
    if !o.limiter {
        return Err(format!("output {output} has no limiter"));
    }
    params.check().map_err(|e| e.to_string())
}

/// Whether output `output` exists, has a strip and the strip takes
/// `param`.
fn check_output_strip(config: &Config, output: usize, param: &StripParam) -> Result<(), String> {
    let Some(o) = config.outputs.get(output) else {
        return Err(format!("no output {output}"));
    };
    if !o.strip {
        return Err(format!("output {output} has no strip"));
    }
    param.check(o.channels as usize).map_err(|e| e.to_string())
}

/// Whether input slot `slot` exists and its delay line takes a delay of
/// `samples`; 0 any slot takes.
fn check_delay(config: &Config, slot: usize, samples: u32) -> Result<(), String> {
    let Some(input) = config.inputs.get(slot) else {
        return Err(format!("no input slot {slot}"));
    };
    if samples > input.max_delay_samples {
        return Err(format!(
            "a delay of {samples} samples on input slot {slot}, its maxDelaySamples {}",
            input.max_delay_samples
        ));
    }
    Ok(())
}

/// Whether output `output` exists and its delay line takes a delay of
/// `samples`; 0 any output takes.
fn check_output_delay(config: &Config, output: usize, samples: u32) -> Result<(), String> {
    let Some(o) = config.outputs.get(output) else {
        return Err(format!("no output {output}"));
    };
    if samples > o.max_delay_samples {
        return Err(format!(
            "a delay of {samples} samples on output {output}, its maxDelaySamples {}",
            o.max_delay_samples
        ));
    }
    Ok(())
}

/// Whether input slot `slot` exists and takes its pre-strip meter
/// `enabled`: on only where its config sets `preStripMeter`.
fn check_pre_strip_meter(config: &Config, slot: usize, enabled: bool) -> Result<(), String> {
    let Some(input) = config.inputs.get(slot) else {
        return Err(format!("no input slot {slot}"));
    };
    if enabled && !input.pre_strip_meter {
        return Err(format!(
            "a pre-strip meter on input slot {slot}, configured without preStripMeter"
        ));
    }
    Ok(())
}

/// Whether input slot `slot` is a tone generator that takes `params`.
fn check_tone(config: &Config, slot: usize, params: &ToneParams) -> Result<(), String> {
    let Some(input) = config.inputs.get(slot) else {
        return Err(format!("no input slot {slot}"));
    };
    if input.tone.is_none() {
        return Err(format!("input slot {slot} is no tone generator"));
    }
    params.check().map_err(|e| e.to_string())
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
    param.check(legs).map_err(|e| e.to_string())
}

/// Whether duck group `group` takes `setting` on `config`'s engine: a
/// group below [`MAX_DUCKS`], detector parameters and depths within their
/// ranges, at most [`MAX_KEYS`] keys of slots that exist, each once, each
/// at most [`MAX_LEGS`] wide, with gains within their range.
fn check_duck(config: &Config, group: usize, setting: &DuckSetting) -> Result<(), String> {
    if group >= MAX_DUCKS {
        return Err(format!("no duck group {group}, at most {MAX_DUCKS}"));
    }
    let slot_legs = |slot: usize| -> Result<usize, String> {
        let input = (config.inputs.get(slot)).ok_or_else(|| format!("no input slot {slot}"))?;
        Ok(input.channels as usize)
    };
    match *setting {
        DuckSetting::Detector(ref p) => DuckParam::Detector(**p).check().map_err(|e| e.to_string()),
        DuckSetting::Keys(ref keys) => {
            if keys.len() > MAX_KEYS {
                return Err(format!(
                    "{} keys for duck group {group}, at most {MAX_KEYS}",
                    keys.len()
                ));
            }
            for (key, &(slot, gain)) in keys.iter().enumerate() {
                if keys[..key].iter().any(|k| k.0 == slot) {
                    return Err(format!("input slot {slot} keyed twice"));
                }
                let legs = slot_legs(slot)?;
                DuckParam::Key { key, legs, gain }
                    .check()
                    .map_err(|e| format!("input slot {slot}: {e}"))?;
            }
            Ok(())
        }
        DuckSetting::Mode(_) | DuckSetting::Hold { .. } => Ok(()),
        DuckSetting::Depth { slot, db } => {
            slot_legs(slot)?;
            if DEPTH_DB.contains(&db) {
                Ok(())
            } else {
                Err(format!(
                    "duck depth {db} dB, within {} to {}",
                    DEPTH_DB.start(),
                    DEPTH_DB.end()
                ))
            }
        }
    }
}

/// Whether `config`'s engine takes duck mask `mask` on the coefficient
/// from `input` to `output`: it exists, and a mask other than 0 is of a
/// column in `duckColumns`.
fn check_mask(config: &Config, input: usize, output: usize, mask: u8) -> Result<(), String> {
    if input >= config.rows() || output >= config.columns() {
        return Err(format!("no matrix coefficient {input} -> {output}"));
    }
    // A pre-strip row is the input before its strip: it takes no duck.
    if mask != 0 && input >= config.input_channels() {
        return Err(format!(
            "duck mask on pre-strip row {input}, which takes none"
        ));
    }
    if mask != 0 && !config.duck_columns.contains(&output) {
        return Err(format!(
            "duck mask on column {output}, which duckColumns does not name"
        ));
    }
    Ok(())
}

/// Whether `config`'s engine takes the duck groups and masks of product
/// state `state`: what an entry setting each could set.
fn check_product_ducks(config: &Config, state: &ProductState) -> Result<(), String> {
    if state.ducks.len() > MAX_DUCKS {
        return Err(format!(
            "{} duck groups, at most {MAX_DUCKS}",
            state.ducks.len()
        ));
    }
    for (g, d) in state.ducks.iter().enumerate() {
        if d.depths.len() > config.inputs.len() {
            return Err(format!("duck group {g}: depths of more slots than inputs"));
        }
        let depths =
            (d.depths.iter().enumerate()).map(|(slot, &db)| DuckSetting::Depth { slot, db });
        let settings = [
            DuckSetting::Detector(Box::new(d.params)),
            DuckSetting::Keys(d.keys.clone()),
        ];
        for setting in settings.into_iter().chain(depths) {
            check_duck(config, g, &setting)?;
        }
    }
    let (n, m) = (config.rows(), config.columns());
    if !state.masks.is_empty() {
        if state.masks.len() != n || state.masks.iter().any(|row| row.len() != m) {
            return Err(format!("duck masks not of the shape {n} x {m}"));
        }
        for (input, row) in state.masks.iter().enumerate() {
            for (output, &mask) in row.iter().enumerate() {
                check_mask(config, input, output, mask)?;
            }
        }
    }
    Ok(())
}

/// Per input slot of `config`, its first matrix row and its width.
fn slot_rows(config: &Config) -> Vec<(usize, usize)> {
    let mut first = 0;
    (config.inputs.iter())
        .map(|i| {
            first += i.channels as usize;
            (first - i.channels as usize, i.channels as usize)
        })
        .collect()
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

/// buildLead: `lead` plus the build time ([`build_time_samples`]),
/// rounded up to a block.
fn build_lead_samples(p9999_ns: Option<u64>, lead: u64, block: u64) -> u64 {
    (build_time_samples(p9999_ns) + lead).next_multiple_of(block)
}

/// The 99.99th percentile of the build times, or [`UNMEASURED_BUILD`]
/// before any build, a supervisor tick and [`HAND_OVER_MARGIN`], in
/// samples: an entry stamped that far ahead is not due yet ([`due`]) when
/// the supervisor takes it in, a tick after its stamp at the latest, so
/// its readers have the build time to open.
fn build_time_samples(p9999_ns: Option<u64>) -> u64 {
    let ns = (p9999_ns.unwrap_or(UNMEASURED_BUILD.as_nanos() as u64))
        .max(BUILD_FLOOR.as_nanos() as u64)
        + SUPERVISOR_TICK.as_nanos() as u64
        + HAND_OVER_MARGIN.as_nanos() as u64;
    (u128::from(ns) * samplez_mxl::SAMPLE_RATE as u128).div_ceil(1_000_000_000) as u64
}

/// Why flow `flow_id` was not opened: its lookup found no directory, or
/// gave no answer within [`GATE_WAIT`].
fn unopened(flow_id: &str, found: Found) -> String {
    match found {
        Found::Pending => format!("flow {flow_id}: its lookup did not answer within {GATE_WAIT:?}"),
        _ => format!("flow {flow_id} not found"),
    }
}

/// Generation builds the build times are taken over: the latest ones,
/// so that a slow build ages out once that many faster ones followed.
const BUILD_WINDOW: usize = 256;
/// How long the time of a build that left a reader out, counted once the
/// reader opened, is kept at most: it ages out after this however few
/// builds followed it. Complete builds' times stay in the window: an
/// empty window falls back to [`UNMEASURED_BUILD`], so aging them out too
/// would raise the lead of a set that routes rarely to that maximum.
const BUILD_AGE: Duration = Duration::from_secs(300);

/// The latest [`BUILD_WINDOW`] generation build times in quarter-octave
/// buckets of nanoseconds.
struct BuildTimes {
    counts: Vec<u64>,
    /// The buckets of the builds in the window and when each was taken,
    /// oldest first.
    window: VecDeque<(usize, Option<Instant>)>,
}

impl BuildTimes {
    fn new() -> BuildTimes {
        BuildTimes {
            counts: vec![0; BUILD_BUCKETS],
            window: VecDeque::with_capacity(BUILD_WINDOW + 1),
        }
    }

    /// Adds a build time; `ages`, the time it was taken, for one that
    /// ages out ([`BUILD_AGE`]).
    fn add(&mut self, ns: u64, ages: Option<Instant>) {
        let b = ((4.0 * (ns.max(1) as f64).log2()) as usize).min(BUILD_BUCKETS - 1);
        self.counts[b] += 1;
        self.window.push_back((b, ages));
        if self.window.len() > BUILD_WINDOW
            && let Some((old, _)) = self.window.pop_front()
        {
            self.counts[old] -= 1;
        }
    }

    /// Drops the build times that age out and are older than
    /// [`BUILD_AGE`] at `now`.
    fn expire(&mut self, now: Instant) {
        let counts = &mut self.counts;
        self.window.retain(|&(b, at)| {
            let keep = at.is_none_or(|at| now.saturating_duration_since(at) <= BUILD_AGE);
            if !keep {
                counts[b] -= 1;
            }
            keep
        });
    }

    /// The upper edge of the bucket holding the 99.99th percentile of the
    /// builds in the window: the longest of them, as the window holds
    /// fewer than 10,000.
    fn p9999(&self) -> Option<u64> {
        let rank = (self.window.len() as f64 * 0.9999).ceil() as u64;
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

/// Threads opening the readers of the generations an engine builds.
const OPENERS: usize = 4;
/// libmxl instances the openers keep created ahead per input domain, so
/// that an open does not create one.
const SPARES: usize = 4;
/// How long a rebuild waits for its readers before it leaves out those
/// not open yet.
const REBUILD_WAIT: Duration = Duration::from_secs(1);
/// How long before the RT thread would apply a generation past its
/// entry's index the generation is due ([`due`]) at the latest: the whole
/// budget, beyond a tick's wait, for the supervisor loop's work between
/// one due check and the next and for assembling and handing over the
/// generation once due, not the assembly alone.
const HAND_OVER_MARGIN: Duration = Duration::from_millis(2);

/// `d` in samples, rounded up.
fn samples_of(d: Duration) -> u64 {
    (d.as_nanos() as u64 * samplez_mxl::SAMPLE_RATE as u64).div_ceil(1_000_000_000)
}

/// Whether a generation due at `at_index` must be built now, `earliest`
/// being the first index an entry reaching the RT thread now is applied
/// at: from the next supervisor tick on, it would reach the RT thread
/// with less than [`HAND_OVER_MARGIN`] before that index. The next check
/// comes a tick's wait after this one plus all the loop does in between,
/// and the hand-over follows the assembly, so a deadline first seen on
/// one check leaves the margin before it only while that work and the
/// assembly fit in it; a hand-over left less is counted
/// ([`past_margin`]). A due generation waits for no reader.
fn due(earliest: u64, at_index: u64) -> bool {
    earliest + samples_of(SUPERVISOR_TICK) + samples_of(HAND_OVER_MARGIN) >= at_index
}

/// Whether an entry at `at_index` falls due ([`due`]) within
/// [`POLL_PERIOD`] and a tick, `earliest` as for `due`: while one not
/// handed over does, the supervisor makes none of the calls that can
/// stall it ([`Supervisor::handing_over`]).
fn due_soon(earliest: u64, at_index: u64) -> bool {
    due(
        earliest + samples_of(POLL_PERIOD + SUPERVISOR_TICK),
        at_index,
    )
}

/// Whether a generation handed over now, `earliest` as for [`due`], has
/// less than [`HAND_OVER_MARGIN`] left before its entry's index
/// `at_index`.
fn past_margin(earliest: u64, at_index: u64) -> bool {
    earliest + samples_of(HAND_OVER_MARGIN) > at_index
}

/// One reader a generation wants: its slot, whether it reads the flow the
/// slot leaves in a re-route, and the flow.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Want {
    slot: usize,
    outgoing: bool,
    flow: String,
}

/// A reader to open, on an instance of its own.
struct OpenTask {
    draft: u64,
    api: Arc<MxlApi>,
    domain: PathBuf,
    dir: PathBuf,
    want: Want,
    channels: u32,
    flow_channels: Option<Vec<u32>>,
    offset: i64,
}

/// A reader an opener opened, or why it did not.
struct Opened {
    draft: u64,
    want: Want,
    /// How long the open ran, its instance's creation included.
    took: Duration,
    result: Result<(Input, Option<u64>), String>,
}

/// A domain's instances created ahead, how many are being created, and
/// after a failure when to try again.
type Spares = (Vec<Arc<Instance>>, usize, Option<Instant>);

#[derive(Default)]
struct OpenQueue {
    tasks: VecDeque<OpenTask>,
    /// The flows an opener opens now.
    busy: HashSet<String>,
    /// The opens running, by draft and reader, and when each started.
    running: Vec<(u64, Want, Instant)>,
    /// Per input domain, the instances created ahead, those being created,
    /// and after a failure when to try again.
    spares: HashMap<PathBuf, Spares>,
    stop: bool,
}

/// The engine's [`OPENERS`] opener threads, which look a flow up with the
/// configured lookup and open, check and pre-fault its reader off the
/// supervisor, oldest task first, one open per flow at a time: a task
/// whose flow an opener opens waits for that open.
struct Openers {
    queue: Arc<(Mutex<OpenQueue>, Condvar)>,
    results: mpsc::Receiver<Opened>,
    threads: Vec<JoinHandle<()>>,
    /// Releases what the supervisor no longer needs on a thread of its own:
    /// destroying a libmxl instance stops its watcher thread and closes
    /// its descriptors, which can wait in the kernel.
    reaper: Option<(mpsc::Sender<Reaped>, JoinHandle<()>)>,
}

/// What the reaper thread releases.
type Reaped = Box<dyn Send>;

impl Openers {
    fn start(
        api: &Arc<MxlApi>,
        domains: Vec<PathBuf>,
        look: lookup::Look,
        timing: Timing,
        prefault: bool,
    ) -> std::io::Result<Openers> {
        let (tx, results) = mpsc::channel();
        let (reap, reaped) = mpsc::channel::<Reaped>();
        let reaper = std::thread::Builder::new()
            .name("samplez-release".into())
            .spawn(move || reaped.into_iter().for_each(drop))?;
        let queue = OpenQueue {
            spares: (domains.into_iter())
                .map(|d| (d, (Vec::new(), 0, None)))
                .collect(),
            ..OpenQueue::default()
        };
        let mut openers = Openers {
            queue: Arc::new((Mutex::new(queue), Condvar::new())),
            results,
            threads: Vec::new(),
            reaper: Some((reap, reaper)),
        };
        for _ in 0..OPENERS {
            let (queue, tx, api) = (Arc::clone(&openers.queue), tx.clone(), Arc::clone(api));
            let thread = std::thread::Builder::new()
                .name("samplez-open".into())
                .spawn(move || open_loop(&queue, &tx, &api, look, &timing, prefault))?;
            openers.threads.push(thread);
        }
        Ok(openers)
    }

    /// Releases `x` on the reaper thread, or here once it is gone.
    fn reap(&self, x: impl Send + 'static) {
        let x: Reaped = Box::new(x);
        if let Some((tx, _)) = &self.reaper
            && let Err(mpsc::SendError(x)) = tx.send(x)
        {
            drop(x);
        }
    }

    fn submit(&self, tasks: Vec<OpenTask>) {
        let (lock, ready) = &*self.queue;
        lock.lock().expect("open queue").tasks.extend(tasks);
        ready.notify_all();
    }

    /// How long an open of `want`'s slot and flow, for any draft, has been
    /// running, its lookup included; none while none has started.
    fn running_for(&self, want: &Want) -> Option<Duration> {
        let q = self.queue.0.lock().expect("open queue");
        let same = |w: &Want| w.slot == want.slot && w.flow == want.flow;
        let opening = (q.running.iter()).filter(|r| same(&r.1)).map(|r| r.2);
        opening.min().map(|at| at.elapsed())
    }

    /// Takes draft `id`'s task for `want` off the queue, its open adopted
    /// from another draft's.
    fn cancel_want(&self, id: u64, want: &Want) {
        let mut q = self.queue.0.lock().expect("open queue");
        let gone: VecDeque<OpenTask> = {
            let (gone, kept) = std::mem::take(&mut q.tasks)
                .into_iter()
                .partition(|t| t.draft == id && t.want == *want);
            q.tasks = kept;
            gone
        };
        drop(q);
        drop(gone);
    }

    /// Takes the tasks of draft `id` no opener started off the queue: how
    /// many. One an opener runs answers all the same, and a draft wanting
    /// the same reader adopts it ([`Supervisor::opened`]).
    fn cancel(&self, id: u64) -> usize {
        let gone: VecDeque<OpenTask> = {
            let mut q = self.queue.0.lock().expect("open queue");
            let (gone, kept) = std::mem::take(&mut q.tasks)
                .into_iter()
                .partition(|t| t.draft == id);
            q.tasks = kept;
            gone
        };
        gone.len()
    }
}

impl Drop for Openers {
    fn drop(&mut self) {
        let (lock, ready) = &*self.queue;
        let tasks = {
            let mut q = lock.lock().unwrap_or_else(|e| e.into_inner());
            q.stop = true;
            std::mem::take(&mut q.tasks)
        };
        ready.notify_all();
        drop(tasks);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        if let Some((tx, t)) = self.reaper.take() {
            drop(tx);
            let _ = t.join();
        }
    }
}

/// An opener thread: takes the oldest task whose flow no other opener
/// opens, looks its flow up through a gate of its own, waiting
/// [`GATE_WAIT`] at most, so a lookup that waits frees the opener and
/// fails the open, its lookup kept running for the next one; opens the
/// reader of a flow found and sends what it opened. A task whose flow an
/// opener opens waits for it, and a draft wanting the same reader adopts
/// what it opens. Idle, creates the instances its domains are short of,
/// one at a time.
fn open_loop(
    queue: &(Mutex<OpenQueue>, Condvar),
    tx: &mpsc::Sender<Opened>,
    api: &Arc<MxlApi>,
    look: lookup::Look,
    timing: &Timing,
    prefault: bool,
) {
    let mut gate = Gate::new(look);
    let (lock, ready) = queue;
    let mut q = lock.lock().expect("open queue");
    loop {
        if q.stop {
            return;
        }
        let now = Instant::now();
        let next = (q.tasks.iter()).position(|t| !q.busy.contains(&t.want.flow));
        let Some(n) = next else {
            // One instance created at a time, so that the others stay free
            // for opens.
            let creating = q.spares.values().any(|s| s.1 > 0);
            let short = (q.spares.iter_mut()).find(|(_, s)| {
                !creating && s.0.len() + s.1 < SPARES && s.2.is_none_or(|at| now >= at)
            });
            let Some((domain, spare)) = short else {
                let backoff = q.spares.values().any(|s| s.2.is_some());
                q = if backoff {
                    let wait = Duration::from_millis(100);
                    ready.wait_timeout(q, wait).expect("open queue").0
                } else {
                    ready.wait(q).expect("open queue")
                };
                continue;
            };
            let domain = domain.clone();
            spare.1 += 1;
            drop(q);
            let instance = Instance::new(api, &domain);
            q = lock.lock().expect("open queue");
            if let Some(spare) = q.spares.get_mut(&domain) {
                spare.1 -= 1;
                match instance {
                    Ok(i) => {
                        spare.0.push(i);
                        spare.2 = None;
                    }
                    // Tried again after a second.
                    Err(_) => spare.2 = Some(Instant::now() + Duration::from_secs(1)),
                }
            }
            continue;
        };
        let task = q.tasks.remove(n).expect("found above");
        let spare = (q.spares.get_mut(&task.domain)).and_then(|s| s.0.pop());
        q.busy.insert(task.want.flow.clone());
        q.running.push((task.draft, task.want.clone(), now));
        drop(q);
        let found = gate.found(std::slice::from_ref(&task.dir), GATE_WAIT)[0];
        let result = match found {
            Found::Yes => open_one(&task, spare, timing, prefault),
            f => Err(unopened(&task.want.flow, f)),
        };
        let took = now.elapsed();
        let (flow, draft, want) = (task.want.flow.clone(), task.draft, task.want.clone());
        let _ = tx.send(Opened {
            draft: task.draft,
            want: task.want,
            took,
            result,
        });
        q = lock.lock().expect("open queue");
        q.busy.remove(&flow);
        q.running.retain(|r| r.0 != draft || r.1 != want);
        ready.notify_all();
    }
}

/// Opens, checks and, with `prefault`, pre-faults the reader of task
/// `t`, whose lookup found its flow.
fn open_one(
    t: &OpenTask,
    spare: Option<Arc<Instance>>,
    timing: &Timing,
    prefault: bool,
) -> Result<(Input, Option<u64>), String> {
    let cfg = InputConfig {
        flow_id: &t.want.flow,
        channels: t.channels,
        flow_channels: t.flow_channels.as_deref(),
        offset: t.offset,
    };
    // An instance of its own: a slow open holds no other reader's open on
    // libmxl's instance mutex. It is destroyed with the reader.
    let instance = match spare {
        Some(i) => i,
        None => Instance::new(&t.api, &t.domain).map_err(|e| e.to_string())?,
    };
    let (input, short) = Generation::open(&instance, &cfg, timing).map_err(|e| e.to_string())?;
    if prefault && let Err(e) = input.prefault() {
        log(format_args!(
            "flow {}: reading the ring before handover: {e}",
            t.want.flow
        ));
    }
    Ok((input, short))
}

/// A generation in the making: its instance, the readers it wants, and
/// those its openers sent back.
struct Draft {
    id: u64,
    generation: Generation,
    wants: Vec<Want>,
    opened: Vec<Opened>,
    /// How long each answered open ran.
    opens: Vec<Duration>,
    started: Instant,
}

/// What [`Supervisor::assemble`] made of a draft.
enum Assembled {
    /// The generation, the slots it holds and the slots it left out for
    /// a reader not open yet, by bit.
    Gen(Box<Gen>, u64, u64),
    /// Its readers are not all answered yet.
    Wait(Draft),
}

/// The time a hand-over waited for a generation: the longest of its new
/// readers' opens, instance creation included, and its assembly; none for
/// one that opened nothing, which says nothing of how long an open takes
/// and so does not shrink buildLead.
/// It is capped at the time assumed before any build, as a late reader's
/// is, so that one stalled open does not set buildLead beyond it.
fn build_sample(opens: &[Duration], assembly: Duration) -> Option<Duration> {
    (opens.iter().max()).map(|&m| (m + assembly).min(UNMEASURED_BUILD))
}

/// The slots of a generation, as `(slot, outgoing flow, flow attached)`.
type Plan = Vec<(usize, Option<String>, Option<String>)>;

/// The readers of `plan`, each slot's outgoing flow before its own.
fn wants(plan: &Plan) -> Vec<Want> {
    let mut wants = Vec::new();
    for (slot, outgoing, attach) in plan {
        for (flow, outgoing) in [(outgoing, true), (attach, false)] {
            if let Some(flow) = flow {
                wants.push(Want {
                    slot: *slot,
                    outgoing,
                    flow: flow.clone(),
                });
            }
        }
    }
    wants
}

/// Whether a copy of history rings with `slack` blocks of slack, read
/// after the RT thread handed out the core after block `k0`, is intact:
/// the replay of block `k0 + 1` needs the span ending at that block's
/// start, first overwritten by block `k0 + 1 + slack`.
fn history_kept(started: &Started, k0: u64, slack: u64) -> Result<(), u64> {
    started.confirms(k0 + 1, slack)
}

/// The committed entries a replicated engine keeps waiting for room in
/// its entry ring at most ([`Engine::commit`]).
pub const WAITING: usize = 4 * PENDING_CHANGES;

/// A core buffer's shape: input and output channels, input slots, the
/// room for one page's entries and outputs.
type Shape = (usize, usize, usize, usize, usize);

/// The input domain of a tone generator's slot: none.
pub(crate) const NO_DOMAIN: usize = usize::MAX;

/// The input slots of `config`'s shape, every one up, and the index in
/// `config.domains` of each input domain they use; a tone generator's
/// slot is in none.
fn new_slots(config: &Config) -> (Vec<Slot>, Vec<usize>) {
    let mut in_domains: Vec<usize> = Vec::new();
    let mut slots = Vec::new();
    let mut channel = 0;
    let mut pre_row = config.input_channels();
    for i in &config.inputs {
        let domain = match i.domain.as_deref().filter(|_| i.tone.is_none()) {
            None => NO_DOMAIN,
            Some(name) => {
                let d = config.domain(name).expect("validated");
                in_domains.iter().position(|&x| x == d).unwrap_or_else(|| {
                    in_domains.push(d);
                    in_domains.len() - 1
                })
            }
        };
        let mut slot = Slot::new(
            domain,
            channel,
            i.channels as usize,
            i.offset_samples,
            config.ramp_samples,
        );
        // Without a flow it starts as a `Detach` leaves it; a tone
        // generator reads none for good.
        slot.detached = i.flow_id.is_none();
        slot.pre_strip = i.pre_strip_meter;
        if i.pre_strip_rows {
            slot.pre_row = Some(pre_row);
            pre_row += i.channels as usize;
        }
        if i.tone.is_some() {
            slot.tone = Some(Tone::new(
                i.channels as usize,
                config.sample_rate,
                config.ramp_samples,
            ));
        }
        slot.delay = delay_line(config, i.channels, i.max_delay_samples);
        slots.push(slot);
        channel += i.channels as usize;
    }
    (slots, in_domains)
}

/// A delay line of `channels` legs taking delays up to `max` samples, its
/// rings allocated here; none for a `max` of 0.
fn delay_line(config: &Config, channels: u32, max: u32) -> Option<Delay> {
    (max > 0).then(|| {
        let rings = DelayRings::new(
            channels as usize,
            max,
            config.history_slack_blocks() as usize,
            config.block_size as usize,
        );
        Delay::new(rings, config.ramp_samples)
    })
}

/// The configured meters on their matrix columns, after the outputs',
/// each input slot's meter on its channels, and the pre-strip meter of
/// each input slot that has one on its channels of `prebuf`.
pub(crate) fn new_meters(config: &Config) -> (Vec<Metered>, Vec<Metered>, Vec<Metered>) {
    let mut channel = config.output_channels();
    let mut buses = Vec::new();
    for m in &config.meters {
        buses.push(Metered {
            first_channel: channel,
            meter: OutMeter::new(m.channels as usize).correlating(m.correlation),
        });
        channel += m.channels as usize;
    }
    let mut channel = 0;
    let (mut taps, mut pre_taps) = (Vec::new(), Vec::new());
    for i in &config.inputs {
        let meter = || Metered {
            first_channel: channel,
            meter: if i.true_peak {
                OutMeter::new(i.channels as usize)
            } else {
                OutMeter::without_true_peak(i.channels as usize)
            },
        };
        taps.push(meter());
        if i.pre_strip_meter {
            pre_taps.push(meter());
        }
        channel += i.channels as usize;
    }
    (buses, taps, pre_taps)
}

/// A replay clone of `config`'s shape: an engine computing off the RT
/// thread from `gens`, writing nothing and hashing every page it computes
/// whole into `hashes`, its entries and controls handed in directly.
pub(crate) fn replay_rt(
    config: &Config,
    api: &Arc<MxlApi>,
    gens: Vec<Option<Box<Gen>>>,
    hashes: rtrb::Producer<PageHash>,
    control: rtrb::Consumer<Control>,
) -> Rt {
    let b = u64::from(config.block_size);
    let (slots, in_domains) = new_slots(config);
    let (n, m) = (config.rows(), config.columns());
    let mut channel = 0;
    let outs = config
        .outputs
        .iter()
        .enumerate()
        .map(|(o, out)| {
            let ch = out.channels as usize;
            channel += ch;
            Out {
                first_channel: channel - ch,
                channels: ch,
                writer: None,
                enabled: true,
                first_k: 0,
                meter: OutMeter::new(ch).correlating(out.correlation),
                hold: Hold::new(ch, b, 0, true),
                pages: Some(Pages::new(o as u32, ch, b, 0)),
                strip: out.strip.then(|| Strip::new(ch, config.ramp_samples)),
                strip_pos: None,
                delay: delay_line(config, out.channels, out.max_delay_samples),
                limiter: out.limiter.then(|| Limiter::new(ch, config.ramp_samples)),
            }
        })
        .collect();
    let widest = config
        .inputs
        .iter()
        .map(|i| i.channels)
        .chain(config.outputs.iter().map(|o| o.channels))
        .max()
        .unwrap_or(0) as usize;
    fn none<T>() -> rtrb::Consumer<T> {
        rtrb::RingBuffer::new(1).1
    }
    fn sink<T>() -> rtrb::Producer<T> {
        rtrb::RingBuffer::new(1).0
    }
    let replica = Replica {
        shared: Arc::new(Shared {
            fence: AtomicU64::new(u64::MAX),
            ranks: AtomicU64::new(0),
            self_report: AtomicBool::new(false),
            diverged: AtomicBool::new(false),
            steps_cleared: AtomicU64::new(0),
            statuses: Vec::new(),
            started: Started::new(),
        }),
        changes: sink(),
        control,
        unresolved: Vec::with_capacity(UNRESOLVED),
        ids: Vec::with_capacity(UNRESOLVED + PENDING_CHANGES),
        late_routes: Vec::with_capacity(PENDING_CHANGES),
        late_lost_until: 0,
        hashes,
        cores: none(),
        copies: sink(),
        handovers: none(),
        handed: sink(),
    };
    Rt {
        api: Arc::clone(api),
        block: b,
        margin: u64::from(config.reader_margin_samples),
        max_catchup: u64::from(config.max_catchup_blocks),
        ramp: config.ramp_samples,
        gens,
        statuses: (0..in_domains.len()).map(|_| GroupWait::Skipped).collect(),
        slots,
        outs,
        // Its meter records go nowhere: only the outputs' are computed,
        // as they were before the configured and input meters.
        buses: Vec::new(),
        taps: Vec::new(),
        pre_taps: Vec::new(),
        mix: Mix {
            page_entries: Vec::with_capacity(PENDING_CHANGES * PAGE.div_ceil(b) as usize),
            ..Mix::new(Matrix::new(n, m), b, config.ramp_samples)
        },
        inbuf: PlanarBuffer::new(n, b as usize),
        prebuf: PlanarBuffer::new(n, b as usize),
        outbuf: PlanarBuffer::new(m, b as usize),
        scratch: vec![0.0; widest * b as usize],
        scratch_out: vec![0.0; widest * b as usize],
        pending_gen: None,
        awaiting_gen: Vec::with_capacity(PENDING_CHANGES),
        last_entry: 0,
        taken: 0,
        skip_upto: 0,
        install_at: 0,
        strips_upto: 0,
        replay: true,
        core: true,
        cmd: none(),
        entries: none(),
        spent: Vec::with_capacity(SPENT),
        ret: sink(),
        tel: sink(),
        meters: sink(),
        duck_meters: sink(),
        meter_seq: 0,
        counters: Arc::new(Counters::default()),
        stop: Arc::new(AtomicBool::new(false)),
        replica: Some(replica),
        step_at: None,
        #[cfg(test)]
        test_input: None,
    }
}

/// Replay speed s: blocks a silent replay clone of `config`'s shape
/// computes per block period, over a second of blocks.
fn replay_speed(config: &Config, api: &Arc<MxlApi>) -> f64 {
    let (_, control) = rtrb::RingBuffer::new(1);
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let domains = new_slots(config).1.len();
    let mut rt = replay_rt(
        config,
        api,
        (0..domains).map(|_| None).collect(),
        hashes,
        control,
    );
    let b = u64::from(config.block_size);
    let blocks = u64::from(config.sample_rate) / b;
    let t0 = Instant::now();
    for k in 0..blocks {
        rt.process(k, u64::MAX);
    }
    let elapsed = t0.elapsed().as_secs_f64().max(1e-9);
    blocks as f64 * b as f64 / f64::from(config.sample_rate) / elapsed
}

/// A running engine. Dropping it stops the engine and releases its
/// writers, which deletes their flows.
pub struct Engine {
    /// Per input slot the rings of its delay line, shared with the RT
    /// thread: copied for a core, filled for a hand-over.
    delays: Vec<Option<Arc<DelayRings>>>,
    /// Per output the rings of its delay line, as `delays`.
    out_delays: Vec<Option<Arc<DelayRings>>>,
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
    /// The build time in samples ([`Progress::build_time`]), kept by the
    /// supervisor.
    build_time: Arc<AtomicU64>,
    shape: (usize, usize),
    config: Config,
    /// Replicated mode with more than one voter.
    replica: Option<(Arc<Shared>, Mutex<ReplicaControl>)>,
    /// Core copies, in replicated mode with more than one voter.
    transfer: Option<Mutex<Transfer>>,
    /// The lookups of the flows a replay attaches.
    replay_gate: Mutex<Gate>,
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
        let disabled = (config.outputs.iter().enumerate())
            .filter(|(_, o)| !o.enabled)
            .fold(0, |d, (o, _)| d | bit(o));
        counters.outputs_disabled.store(disabled, Relaxed);
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
        let pre_strip: usize = (config.inputs.iter())
            .filter(|i| i.pre_strip_meter)
            .map(|i| i.channels as usize)
            .sum();
        // A record per metered channel and slice, and one more per
        // element that correlates its legs.
        let crosses = (config.outputs.iter().filter(|o| o.correlation).count())
            + config.meters.iter().filter(|m| m.correlation).count();
        let records = METER_RING_SECONDS
            * (samplez_mxl::SAMPLE_RATE as usize / SLICE as usize)
            * (config.columns() + config.input_channels() + crosses + pre_strip);
        let (mut meter_tx, mut meter_rx) = rtrb::RingBuffer::new(records.max(1));
        let blank = MeterRecord {
            seq: 0,
            meter: 0,
            channel: 0,
            slice: 0,
            frames: 0,
            peak: 0.0,
            true_peak: 0.0,
            sum_sq: 0.0,
            plain_sum_sq: 0.0,
            reset_at: crate::rt_loop::NO_RESET,
            flags: 0,
        };
        while meter_tx.push(blank).is_ok() {}
        while meter_rx.pop().is_ok() {}
        let (mut duck_tx, mut duck_rx) = rtrb::RingBuffer::new(
            METER_RING_SECONDS * (samplez_mxl::SAMPLE_RATE as usize / SLICE as usize) * MAX_DUCKS,
        );
        let blank = DuckRecord {
            group: 0,
            slice: 0,
            key: 0.0,
            envelope: 0.0,
            engaged: false,
        };
        while duck_tx.push(blank).is_ok() {}
        while duck_rx.pop().is_ok() {}
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
                ranks: AtomicU64::new(0),
                self_report: AtomicBool::new(false),
                diverged: AtomicBool::new(false),
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
            let (handovers_tx, handovers_rx) = rtrb::RingBuffer::new(1);
            let (handed_tx, handed_rx) = rtrb::RingBuffer::new(1);
            transfer = (!alone).then(|| {
                Mutex::new(Transfer {
                    cores: cores_tx,
                    copies: copies_rx,
                    in_flight: false,
                    in_flight_after: 0,
                    handovers: handovers_tx,
                    handed: handed_rx,
                    handing: false,
                    seq: 0,
                    spare: None,
                    shape: (
                        config.rows(),
                        config.columns(),
                        config.inputs.len(),
                        page_entries,
                        config.outputs.len(),
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
                handovers: handovers_rx,
                handed: handed_tx,
            };
            let control = ReplicaControl {
                ring: control_tx,
                unresolved: Vec::with_capacity(UNRESOLVED),
                sent: 0,
                core: true,
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

        let (slots, in_domains) = new_slots(&config);
        let delays: Vec<Option<Arc<DelayRings>>> = (slots.iter())
            .map(|s| s.delay.as_ref().map(|d| Arc::clone(d.rings())))
            .collect();
        let mut outs = Vec::new();
        let mut channel = 0;
        for (n, o) in config.outputs.iter().enumerate() {
            outs.push(Out {
                first_channel: channel,
                channels: o.channels as usize,
                writer: None,
                enabled: o.enabled,
                first_k: 0,
                meter: OutMeter::new(o.channels as usize).correlating(o.correlation),
                hold: if replica.is_some() {
                    Hold::new(o.channels as usize, b, hold_blocks, alone)
                } else {
                    Hold::new(o.channels as usize, b, 0, true)
                },
                pages: hash_tx
                    .is_some()
                    .then(|| Pages::new(n as u32, o.channels as usize, b, hold_blocks)),
                strip: o
                    .strip
                    .then(|| Strip::new(o.channels as usize, config.ramp_samples)),
                strip_pos: None,
                delay: delay_line(&config, o.channels, o.max_delay_samples),
                limiter: o
                    .limiter
                    .then(|| Limiter::new(o.channels as usize, config.ramp_samples)),
            });
            channel += o.channels as usize;
        }
        let out_delays: Vec<Option<Arc<DelayRings>>> = (outs.iter())
            .map(|o| o.delay.as_ref().map(|d| Arc::clone(d.rings())))
            .collect();
        let (n, m) = (config.rows(), config.columns());
        let mut matrix = Matrix::new(n, m);
        let (buses, taps, pre_taps) = new_meters(&config);
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
            buses,
            taps,
            pre_taps,
            mix: Mix {
                page_entries: Vec::with_capacity(page_entries),
                ..Mix::new(matrix, b, config.ramp_samples)
            },
            inbuf: PlanarBuffer::new(n, b as usize),
            prebuf: PlanarBuffer::new(n, b as usize),
            outbuf: PlanarBuffer::new(m, b as usize),
            scratch: vec![0.0; widest * b as usize],
            scratch_out: vec![0.0; widest * b as usize],
            pending_gen: None,
            awaiting_gen: Vec::with_capacity(PENDING_CHANGES),
            last_entry: 0,
            taken: 0,
            skip_upto: 0,
            install_at: 0,
            strips_upto: 0,
            replay: false,
            core: true,
            cmd: cmd_rx,
            entries: entry_rx,
            spent: Vec::with_capacity(SPENT),
            ret: ret_tx,
            tel: tel_tx,
            meters: meter_tx,
            duck_meters: duck_tx,
            meter_seq: 0,
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
            replica: rt_replica,
            step_at: None,
            #[cfg(test)]
            test_input: None,
        };

        let (thread_tx, thread_rx) = mpsc::sync_channel::<ThreadStatus>(1);
        let rt_cfg = config.rt.clone();
        let rt_process = process.clone();
        let rt_thread = std::thread::Builder::new()
            .name("samplez-rt".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || {
                let _ = thread_tx.send(rt::setup_thread(&rt_cfg, &rt_process));
                drop(thread_tx);
                rt_state.run()
            })
            .map_err(|e| Error::Thread(format!("spawning the RT thread: {e}")))?;
        let thread = thread_rx
            .recv()
            .map_err(|_| Error::Thread("the RT thread ended during set-up".to_owned()))?;

        let replay_speed = transfer.is_some().then(|| replay_speed(&config, &api));
        let status = Arc::new(Mutex::new(Status {
            rt: RtStatus {
                process: process.clone(),
                thread,
            },
            clock: ClockStatus::default(),
            generation: GenerationStatus::default(),
            diverged: None,
            rt_panic: None,
            replay_speed,
            batches_leaked: 0,
            ducks: vec![DuckMeter::default(); MAX_DUCKS],
            inputs: config
                .inputs
                .iter()
                .map(|i| InputStatus {
                    name: i.name.clone(),
                    state: if i.tone.is_some() {
                        InputState::Tone
                    } else if i.flow_id.is_some() {
                        InputState::Pruned
                    } else {
                        InputState::Detached
                    },
                    up: true,
                    readable: None,
                    flow_id: i.flow_id.clone().unwrap_or_default(),
                    error: None,
                    margin_short: None,
                    margin_warning: None,
                    counters: InputCounters::default(),
                    meters: InputMeters::default(),
                    pre_strip_meters: None,
                })
                .collect(),
            outputs: config
                .outputs
                .iter()
                .map(|o| OutputStatus {
                    name: o.name.clone(),
                    enabled: o.enabled,
                    realised: false,
                    reason: Some(
                        if o.enabled {
                            "not created yet"
                        } else {
                            "disabled"
                        }
                        .to_owned(),
                    ),
                    attempts: 0,
                    races: 0,
                    create_failures: 0,
                    buffer_length: None,
                    hints: None,
                    meters: OutputMeters::default(),
                })
                .collect(),
            meters: config
                .meters
                .iter()
                .map(|m| MeterStatus {
                    name: m.name.clone(),
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
        let build_time = Arc::new(AtomicU64::new(build_time_samples(None)));
        // Per meter, as MeterRecord numbers them: the outputs, the
        // configured meters, the inputs, the inputs' pre-strip meters.
        let checked = "checked by Config::parse";
        let config_loudness = (config.outputs.iter())
            .map(|o| (o.loudness_weights().expect(checked), o.correlation))
            .chain(
                (config.meters.iter())
                    .map(|m| (m.loudness_weights().expect(checked), m.correlation)),
            )
            .chain((config.inputs.iter()).map(|i| (i.loudness_weights().expect(checked), false)))
            .chain(
                (config.inputs.iter())
                    .filter(|i| i.pre_strip_meter)
                    .map(|i| (i.loudness_weights().expect(checked), false)),
            )
            .map(|(weights, on)| OutputLoudness::new(weights).correlating(on))
            .collect();
        let timing = Timing {
            block: b,
            margin: u64::from(config.reader_margin_samples),
            max_catchup_blocks: u64::from(config.max_catchup_blocks),
            // A lone voter has no donor to resync from.
            resync_budget: if reports.is_some() && !alone {
                config.resync_budget_samples()
            } else {
                0
            },
        };
        let input_domains = (in_domains.iter())
            .map(|&d| config.domains[d].path.clone())
            .collect();
        let openers = Openers::start(&api, input_domains, config.look, timing, prefault)
            .map_err(|e| Error::Thread(format!("opener thread: {e}")))?;
        let sup = Supervisor {
            api: Arc::clone(&api),
            timing,
            prefault,
            in_domains: in_domains.clone(),
            slots: config
                .inputs
                .iter()
                .map(|i| SupSlot {
                    state: if i.flow_id.is_some() {
                        SupState::Wanted
                    } else {
                        SupState::Detached
                    },
                    window: VecDeque::new(),
                    flow: i.flow_id.clone().unwrap_or_default(),
                    up: true,
                    reported: false,
                    prune_unsent: false,
                    routing: None,
                    gap: ClockGap::default(),
                })
                .collect(),
            jobs: VecDeque::new(),
            openers,
            next_draft: 0,
            rebuilding: (0..in_domains.len()).map(|_| None).collect(),
            awaiting: 0,
            arrived: Vec::new(),
            rejoins: Vec::new(),
            readers: HashMap::new(),
            log_rx,
            log: Arc::clone(&log),
            reports,
            build_times: BuildTimes::new(),
            builds: 0,
            build_lead: Arc::clone(&build_lead),
            build_time: Arc::clone(&build_time),
            lead,
            ramp: u64::from(config.ramp_samples),
            dirty: vec![true; in_domains.len()],
            reroute: vec![0; in_domains.len()],
            product: None,
            pollers: (0..in_domains.len()).map(|_| None).collect(),
            poll_setups: Vec::new(),
            gate: Gate::new(config.look),
            out_gate: Gate::fresh(config.look),
            poll_errors: vec![None; in_domains.len()],
            rebuild_errors: vec![None; in_domains.len()],
            outs: (config.outputs.iter())
                .map(|o| SupOut {
                    enabled: o.enabled,
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
            duck_rx,
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
            pre_runs: vec![None; config.inputs.iter().filter(|i| i.pre_strip_meter).count()],
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
        let replay_gate = Mutex::new(Gate::new(config.look));
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
            build_time,
            shape: (n, m),
            config,
            replica,
            transfer,
            replay_gate,
            delays,
            out_delays,
        })
    }

    /// The configured `rampSamples`: the length a gain change with no
    /// fade ramps over.
    pub fn ramp_samples(&self) -> u32 {
        self.config.ramp_samples
    }

    pub fn status(&self) -> Status {
        self.status.lock().expect("status lock").clone()
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Ramps the gain from input channel `input` to matrix column
    /// `output`, an output channel or after them a meter channel, to
    /// `gain` over the configured ramp length, starting at
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
        self.set_gain_fade(input, output, gain, None, at_index)
    }

    /// [`Engine::set_gain`] with a [`Fade`]: the coefficient follows the
    /// fade's function of the sample index from `at_index` on. A fade
    /// that is not [`Fade::within`] `at_index` is refused as invalid.
    pub fn set_gain_fade(
        &self,
        input: usize,
        output: usize,
        gain: f32,
        fade: Option<Fade>,
        at_index: u64,
    ) -> Result<(), GainError> {
        if fade.is_some_and(|f| !f.within(at_index)) {
            return Err(GainError::Invalid(format!(
                "a fade of a valid start gain, within {} samples, starting within the same \
                 distance of the entry",
                samplez_dsp::MAX_EXACT_RAMP_LEN
            )));
        }
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
            fade,
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

    /// Sets parameter `param` of output `output`'s strip from sample index
    /// `at_index`, a multiple of the block size, ramping over the
    /// configured ramp length. Refused as [`Engine::set_gain`] refuses a
    /// change, and as invalid for an output that does not exist or is
    /// configured without `strip`, and for a parameter its strip refuses
    /// ([`StripParam::check`]).
    pub fn set_output_strip(
        &self,
        output: usize,
        param: StripParam,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_output_strip(&self.config, output, &param).map_err(GainError::Invalid)?;
        let change = Change::OutputStrip { output, param };
        self.set_local(change, at_index, "strip changes")
    }

    /// Sets the limiter of output `output` to `params` from sample index
    /// `at_index`, a multiple of the block size. Refused as
    /// [`Engine::set_gain`] refuses a change, and as invalid for an output
    /// that does not exist or is configured without `limiter`, and for
    /// parameters a limiter refuses ([`LimiterParams::check`]).
    pub fn set_limiter(
        &self,
        output: usize,
        params: LimiterParams,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_limiter(&self.config, output, &params).map_err(GainError::Invalid)?;
        let change = Change::Limiter { output, params };
        self.set_local(change, at_index, "limiter changes")
    }

    /// Delays input slot `slot` by `samples` from sample index
    /// `at_index`, a multiple of the block size, as [`Entry::Delay`] does.
    /// Refused as [`Engine::set_gain`] refuses a change, and as invalid
    /// for a slot that does not exist or a delay above its
    /// `maxDelaySamples`.
    pub fn set_delay(&self, slot: usize, samples: u32, at_index: u64) -> Result<(), GainError> {
        check_delay(&self.config, slot, samples).map_err(GainError::Invalid)?;
        self.set_local(Change::Delay { slot, samples }, at_index, "delay changes")
    }

    /// Delays output `output` by `samples` from sample index `at_index`, a
    /// multiple of the block size, as [`Entry::OutputDelay`] does. Refused
    /// as [`Engine::set_gain`] refuses a change, and as invalid for an
    /// output that does not exist or a delay above its `maxDelaySamples`.
    pub fn set_output_delay(
        &self,
        output: usize,
        samples: u32,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_output_delay(&self.config, output, samples).map_err(GainError::Invalid)?;
        let change = Change::OutputDelay { output, samples };
        self.set_local(change, at_index, "delay changes")
    }

    /// Sets the tone generator of input slot `slot` to `params` from
    /// sample index `at_index`, a multiple of the block size, as
    /// [`Entry::Tone`] does. Refused as [`Engine::set_gain`] refuses a
    /// change, and as invalid for a slot that does not exist or is no
    /// tone generator, and for parameters out of range
    /// ([`ToneParams::check`]).
    pub fn set_tone(
        &self,
        slot: usize,
        params: ToneParams,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_tone(&self.config, slot, &params).map_err(GainError::Invalid)?;
        self.set_local(Change::Tone { slot, params }, at_index, "tone changes")
    }

    /// Sets `setting` of duck group `group` from sample index `at_index`,
    /// a multiple of the block size. Refused as [`Engine::set_gain`]
    /// refuses a change, and as invalid for a setting
    /// [`Engine::admits`] refuses as an [`Entry::Duck`].
    pub fn set_duck(
        &self,
        group: usize,
        setting: DuckSetting,
        at_index: u64,
    ) -> Result<(), DuckError> {
        self.set_local_entry(&Entry::Duck { group, setting }, at_index, "duck changes")
    }

    /// Sets the duck mask of the coefficient from input channel `input`
    /// to matrix column `output` from sample index `at_index`, as
    /// [`Entry::DuckMask`] does. Refused as [`Engine::set_duck`] refuses a
    /// setting.
    pub fn set_duck_mask(
        &self,
        input: usize,
        output: usize,
        mask: u8,
        at_index: u64,
    ) -> Result<(), DuckError> {
        let entry = Entry::DuckMask {
            input,
            output,
            mask,
        };
        self.set_local_entry(&entry, at_index, "duck changes")
    }

    /// Appends `entry`, checked and folded, at `at_index` to the engine's
    /// own log and commits it.
    fn set_local_entry(&self, entry: &Entry, at_index: u64, what: &str) -> Result<(), GainError> {
        let mut items = Vec::new();
        self.items(entry, &mut items).map_err(|e| match e {
            EntryError::Invalid(m) => GainError::Invalid(m),
            other => GainError::Invalid(other.to_string()),
        })?;
        let (n, m) = self.shape;
        let Some((change, batch)) = batch::fold(&items, m, n * m).map_err(GainError::Invalid)?
        else {
            return Ok(());
        };
        self.set_local_batch(change, batch, at_index, what)
    }

    /// Appends `change` at `at_index` to the engine's own log and commits
    /// it; `what` names the changes in the refusal of a replicated engine.
    fn set_local(&self, change: Change, at_index: u64, what: &str) -> Result<(), GainError> {
        self.set_local_batch(change, None, at_index, what)
    }

    /// [`Engine::set_local`] of `change` with its batch.
    fn set_local_batch(
        &self,
        change: Change,
        batch: Option<Arc<Batch>>,
        at_index: u64,
        what: &str,
    ) -> Result<(), GainError> {
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
        // A block start, as the index must be: equivalent for the check.
        let earliest = earliest(&self.api, &self.counters, self.margin, self.lead)
            .next_multiple_of(self.block);
        if at_index < earliest {
            return Err(GainError::TooNear { at_index, earliest });
        }
        let e = RtEntry {
            id: log.last + 1,
            at_index,
            change,
        };
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(e, batch, Vec::new(), consumed)
            .map_err(|_| GainError::Full)?;
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
    /// invalid on an engine that follows a replicated log, for a tone
    /// generator's slot, and for an `at_index` that is not a multiple of
    /// the block size.
    pub fn route(&self, slot: usize, flow_id: &str, at_index: u64) -> Result<u64, EntryError> {
        if !at_index.is_multiple_of(self.block) {
            return Err(EntryError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
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
        log.append(e, None, vec![(slot, Some(flow_id.to_owned()))], consumed)?;
        log.commit(e.id);
        if let Some(l) = &mut log.local {
            l.last_at[slot] = at_index;
            l.gen_at = at_index;
        }
        Ok(at_index)
    }

    /// Detaches input slot `slot`: a new generation holding no reader for
    /// it is built, and the slot crossfades from its flow to silence over
    /// the ramp length from the returned index, stamped as
    /// [`Engine::route`] stamps. A detached slot reads no flow, is not
    /// polled and is not reported, so its up or down state stays as it
    /// is, until a `Route` fades it in on a flow. Detaching a detached
    /// slot changes nothing audible. Refused as invalid on an engine that
    /// follows a replicated log, for no such slot or a tone generator's,
    /// and for an `at_index` that is not a multiple of the block size.
    pub fn detach(&self, slot: usize, at_index: u64) -> Result<u64, EntryError> {
        if !at_index.is_multiple_of(self.block) {
            return Err(EntryError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
        self.config
            .check_flow_slot(slot)
            .map_err(|e| EntryError::Invalid(e.0))?;
        let mut log = self.log.lock().expect("log lock");
        let (last_at, gen_at) = match &log.local {
            Some(l) => (l.last_at[slot], l.gen_at),
            None => {
                return Err(EntryError::Invalid(
                    "detaches come from the replicated log".to_owned(),
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
            change: Change::Detach { slot },
        };
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(e, None, vec![(slot, None)], consumed)?;
        log.commit(e.id);
        if let Some(l) = &mut log.local {
            l.last_at[slot] = at_index;
            l.gen_at = at_index;
        }
        Ok(at_index)
    }

    /// Enables output `slot` or disables it from the returned index,
    /// `at_index` or, when that is nearer, the earliest index a gain
    /// change is stamped at. Disabled, nothing is written to it from that
    /// index on and the supervisor releases its writer, which deletes its
    /// flow unless another writer holds it; enabled, the supervisor
    /// creates its flow as at start-up and it is written from the first
    /// block due once the writer is in. [`OutputStatus`] reports both, and
    /// why a flow is not created. Refused as invalid on an engine that
    /// follows a replicated log, for no such output, and for an
    /// `at_index` that is not a multiple of the block size; as full while
    /// [`PENDING_CHANGES`] entries are outstanding.
    pub fn set_output_enabled(
        &self,
        slot: usize,
        enabled: bool,
        at_index: u64,
    ) -> Result<u64, EntryError> {
        if !at_index.is_multiple_of(self.block) {
            return Err(EntryError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
        let change = self.change(&Entry::Output { slot, enabled })?;
        let mut log = self.log.lock().expect("log lock");
        if log.local.is_none() {
            return Err(EntryError::Invalid(
                "output changes come from the replicated log".to_owned(),
            ));
        }
        let earliest = earliest(&self.api, &self.counters, self.margin, self.lead)
            .next_multiple_of(self.block);
        let e = RtEntry {
            id: log.last + 1,
            at_index: at_index.max(earliest),
            change,
        };
        let consumed = self.counters.entries_consumed.load(Acquire);
        log.append(e, None, Vec::new(), consumed)?;
        log.commit(e.id);
        Ok(e.at_index)
    }

    /// Appends entry `entry` at log id `id`: [`Engine::append_batch`] of
    /// one entry.
    pub fn append(&self, id: LogId, at_index: u64, entry: Entry) -> Result<(), EntryError> {
        self.append_batch(id, at_index, std::slice::from_ref(&entry))
    }

    /// Appends `entries`, the engine entries of log entry `id`, above every
    /// id appended before, to apply at `at_index`, a multiple of the block
    /// size. They are folded here, off the RT thread: per matrix
    /// coefficient, strip parameter and input slot the last value counts
    /// (an `InputDown` or `InputUp` of a slot replaces the one before it,
    /// a `Route` or `Detach` the one before it), so the log entry takes
    /// one place on the RT thread's pending list and its batch is bounded
    /// by the engine's shape, however many entries it has. No entries
    /// append nothing. Refused whole, nothing appended, when any entry is
    /// refused alone or the slots its `InputUp`s, `Route`s and `Detach`es
    /// name are of more than one input domain: one generation, covering
    /// every slot it raises, re-routes or detaches, is built for a log
    /// entry, and one at a time. An `InputUp`, `Route` or `Detach` has
    /// its generation built at once,
    /// the next once the RT thread swapped in the one before. Committed
    /// log entries go to the RT thread, which applies each whole at its
    /// index, its gains first, or, past it, counts it once in
    /// `command_late` and marks the engine diverged. Refused as invalid on
    /// an engine with its own log.
    pub fn append_batch(
        &self,
        id: LogId,
        at_index: u64,
        entries: &[Entry],
    ) -> Result<(), EntryError> {
        if entries.is_empty() {
            return Ok(());
        }
        let invalid = |m: String| Err(EntryError::Invalid(m));
        if !at_index.is_multiple_of(self.block) {
            return invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            ));
        }
        if let Some(f) = fade_beyond(entries, at_index) {
            return invalid(format!(
                "{f:?} starts more than the fade bound past {at_index}"
            ));
        }
        let Some((change, batch, routes)) = self.fold(entries)? else {
            return Ok(());
        };
        let mut log = self.log.lock().expect("log lock");
        if log.local.is_some() {
            return invalid("the engine keeps its own log".to_owned());
        }
        let consumed = self.counters.entries_consumed.load(Acquire);
        let e = RtEntry {
            id,
            at_index,
            change,
        };
        log.append(e, batch, routes, consumed)
    }

    /// Whether [`Engine::append_batch`] takes `entries`, at an index it
    /// takes: what a log entry is checked against before it is ordered,
    /// so that no committed one is refused.
    pub fn admits(&self, entries: &[Entry]) -> Result<(), EntryError> {
        self.fold(entries).map(|_| ())
    }

    /// `entries` checked and folded, with the flows of the slots the
    /// fold re-routes, none for a slot it detaches: none for no change.
    /// Refused when an entry is, and when the slots the entries'
    /// `InputUp`s, `Route`s and `Detach`es name are of more than one input
    /// domain: one generation is built for a log entry.
    #[allow(clippy::type_complexity)]
    fn fold(
        &self,
        entries: &[Entry],
    ) -> Result<Option<(Change, Option<Arc<Batch>>, Vec<(usize, Option<String>)>)>, EntryError>
    {
        let mut items = Vec::with_capacity(entries.len());
        let mut flows = HashMap::new();
        for entry in entries {
            self.items(entry, &mut items)?;
            match entry {
                Entry::Route { slot, flow_id } => {
                    flows.insert(*slot, Some(flow_id));
                }
                Entry::Detach { slot } => {
                    flows.insert(*slot, None);
                }
                _ => {}
            }
        }
        // Every slot an `InputUp`, `Route` or `Detach` names, whether the fold keeps
        // it or not: then every subset of the entries passes as well, as a
        // restamp keeping some of a log entry's commands must.
        let named = items.iter().fold(0, |m, i| match i {
            Item::Change(c) => m | (c.generating().0 | c.generating().1),
            Item::Duck(_) => m,
        });
        let mut domains = slots_of(named).map(|s| &self.config.inputs[s].domain);
        if let Some(first) = domains.next()
            && domains.any(|d| d != first)
        {
            return Err(EntryError::Invalid(
                "a log entry raising or re-routing inputs of more than one domain: one \
                 generation is built for it"
                    .to_owned(),
            ));
        }
        let (n, m) = self.shape;
        let Some((change, batch)) = batch::fold(&items, m, n * m).map_err(EntryError::Invalid)?
        else {
            return Ok(None);
        };
        let (_, routed) = change.generating();
        let routes = slots_of(routed).map(|s| (s, flows[&s].cloned())).collect();
        Ok(Some((change, batch, routes)))
    }

    /// What `entry` sets, checked against the configuration, appended to
    /// `items`.
    fn items(&self, entry: &Entry, items: &mut Vec<Item>) -> Result<(), EntryError> {
        let invalid = |m: String| EntryError::Invalid(m);
        match *entry {
            Entry::Duck { group, ref setting } => {
                check_duck(&self.config, group, setting).map_err(invalid)?
            }
            Entry::DuckMask {
                input,
                output,
                mask,
            } => check_mask(&self.config, input, output, mask).map_err(invalid)?,
            _ => {
                items.push(Item::Change(self.change(entry)?));
                return Ok(());
            }
        }
        items_of(entry, &slot_rows(&self.config), items).map_err(invalid)
    }

    /// The change `entry`, other than a duck entry, makes, checked
    /// against the configuration.
    fn change(&self, entry: &Entry) -> Result<Change, EntryError> {
        let invalid = |m: String| Err(EntryError::Invalid(m));
        let slots = self.config.inputs.len();
        Ok(match *entry {
            Entry::Gain {
                input,
                output,
                gain,
                fade,
            } => {
                if fade.is_some_and(|f| !f.is_valid()) {
                    return invalid(format!(
                        "a fade of a valid start gain within {} samples",
                        samplez_dsp::MAX_EXACT_RAMP_LEN
                    ));
                }
                if input >= self.shape.0 || output >= self.shape.1 {
                    return invalid(format!("no matrix coefficient {input} -> {output}"));
                }
                if !samplez_dsp::valid_gain(gain) {
                    return invalid(format!("gain {gain} is not finite within MAX_GAIN"));
                }
                Change::Gain {
                    input,
                    output,
                    gain,
                    fade,
                }
            }
            Entry::InputDown { slot }
            | Entry::InputUp { slot }
            | Entry::Route { slot, .. }
            | Entry::Detach { slot }
                if slot >= slots =>
            {
                return invalid(format!("no input slot {slot}"));
            }
            Entry::InputDown { slot } | Entry::InputUp { slot } | Entry::Detach { slot }
                if self.config.inputs[slot].tone.is_some() =>
            {
                return Err(EntryError::Invalid(
                    self.config.check_flow_slot(slot).unwrap_err().0,
                ));
            }
            Entry::Detach { slot } => Change::Detach { slot },
            Entry::Output { slot, .. } if slot >= self.config.outputs.len() => {
                return invalid(format!("no output {slot}"));
            }
            Entry::Output { slot, enabled } => Change::Output {
                output: slot,
                enabled,
            },
            Entry::LoudnessRun { meter, .. } | Entry::ResetLoudness { meter }
                if meter >= self.config.outputs.len() + self.config.meters.len() =>
            {
                return invalid(format!("no output or meter {meter}"));
            }
            Entry::PreStripMeter { slot, enabled } => {
                check_pre_strip_meter(&self.config, slot, enabled).map_err(EntryError::Invalid)?;
                Change::PreStripMeter { slot, enabled }
            }
            Entry::LoudnessRun { meter, running } => Change::LoudnessRun { meter, running },
            Entry::ResetLoudness { meter } => Change::ResetLoudness { meter },
            Entry::InputDown { slot } => Change::InputDown { slot },
            Entry::InputUp { slot } => Change::InputUp { slot },
            Entry::Route { slot, ref flow_id } => {
                self.config
                    .check_input_flow(slot, flow_id)
                    .map_err(|e| EntryError::Invalid(e.0))?;
                Change::Route { slot }
            }
            Entry::Strip { slot, param } => {
                check_strip(&self.config, slot, &param).map_err(EntryError::Invalid)?;
                Change::Strip { slot, param }
            }
            Entry::OutputStrip { output, param } => {
                check_output_strip(&self.config, output, &param).map_err(EntryError::Invalid)?;
                Change::OutputStrip { output, param }
            }
            Entry::Limiter { output, params } => {
                check_limiter(&self.config, output, &params).map_err(EntryError::Invalid)?;
                Change::Limiter { output, params }
            }
            Entry::Delay { slot, samples } => {
                check_delay(&self.config, slot, samples).map_err(EntryError::Invalid)?;
                Change::Delay { slot, samples }
            }
            Entry::OutputDelay { output, samples } => {
                check_output_delay(&self.config, output, samples).map_err(EntryError::Invalid)?;
                Change::OutputDelay { output, samples }
            }
            Entry::Tone {
                slot,
                frequency_hz,
                level_db,
                ident,
            } => {
                let params = ToneParams {
                    frequency_hz,
                    level_db,
                    ident,
                };
                check_tone(&self.config, slot, &params).map_err(EntryError::Invalid)?;
                Change::Tone { slot, params }
            }
            Entry::Duck { .. } | Entry::DuckMask { .. } => {
                return invalid("a duck entry is no single change".to_owned());
            }
        })
    }

    /// Commits the appended entries up to log id `up_to`: they go to the
    /// RT thread, in log order. A replicated engine whose RT thread takes
    /// none of them for so long that more than [`WAITING`] wait for room
    /// drops them and its core: it no longer follows the log and is
    /// rebuilt or resynced, its progress counting the drop in
    /// `log_overflows`.
    pub fn commit(&self, up_to: LogId) {
        let mut log = self.log.lock().expect("log lock");
        log.commit(up_to);
        if log.local.is_none() && log.waiting() > WAITING {
            log.drop_waiting();
            drop(log);
            self.counters.log_overflows.fetch_add(1, Relaxed);
            // Refused with one voter, which has no core to drop, and while
            // the control ring is full: the replica reads the overflow in
            // its next report period's sample and drops the core then
            // (`Audio::drop_engine`), and `install_product` drops one
            // still held before its rebuild replaces it.
            let _ = self.drop_core();
        }
    }

    /// Replicated mode: the appended log entry `id` is never applied, and
    /// its generation is dropped: its commit was not known by its apply
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
        let dropped = matches!(c, Control::DropCore);
        control.ring.push(c).map_err(|_| ControlError::Full)?;
        control.sent += 1;
        if dropped {
            control.core = false;
        }
        Ok(())
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

    /// Replicated mode: whether the replicated state finds this replica's
    /// output apart from its peers' (diverged, or resyncing before the
    /// hand-over). Each meter slice with a block computed while it is set
    /// is lost to the loudness measurement and counted in
    /// [`OutputMeters::lost`].
    pub fn set_diverged(&self, diverged: bool) -> Result<(), ControlError> {
        self.replica()?.0.diverged.store(diverged, Relaxed);
        Ok(())
    }

    /// Replicated mode: this replica's ranks, 0 the top, each fixed into
    /// a block's status when F passes it; at most
    /// [`crate::blocks::MAX_RANK`]. Set before the F that passes the
    /// blocks they hold for ([`Engine::set_fence`]).
    pub fn set_ranks(&self, ranks: Ranks) -> Result<(), ControlError> {
        self.replica()?.0.ranks.store(ranks.pack(), Release);
        Ok(())
    }

    /// Replicated mode: output `output` commits again from its commit
    /// start, the first page boundary P in a block at or after `min_k`
    /// it computes (with one voter, from that block's start). The block
    /// holding P reports `committed from P`, and `[kB, P)` withheld
    /// without a cause. Ignored while the output commits. The latest
    /// request or cancel ([`Engine::cancel_commit_start`]) wins.
    pub fn commit_from(&self, output: usize, min_k: u64) -> Result<(), ControlError> {
        self.commit_from_seen(output, min_k, u64::MAX)
    }

    /// Replicated mode: [`Engine::commit_from`], for a commit start
    /// decided on this replica's block statuses up to block `seen`. The
    /// output ignores it when the replica was withdrawn for a cause while
    /// a block after `seen` was decided, and a withdrawal for a cause
    /// drops one not yet taken.
    pub fn commit_from_seen(
        &self,
        output: usize,
        min_k: u64,
        seen: u64,
    ) -> Result<(), ControlError> {
        self.output_index(output)?;
        let (shared, control) = self.replica()?;
        // One request at a time.
        let _one = control.lock().expect("control lock");
        shared.statuses[output].request_start(min_k, seen);
        Ok(())
    }

    /// Replicated mode: drops output `output`'s requested commit start
    /// ([`Engine::commit_from`]) unless the output took it already: a
    /// commit start named on a decision taken back. The latest request
    /// or cancel wins.
    pub fn cancel_commit_start(&self, output: usize) -> Result<(), ControlError> {
        self.output_index(output)?;
        let (shared, control) = self.replica()?;
        let _one = control.lock().expect("control lock");
        shared.statuses[output].request_cancel();
        Ok(())
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
    /// `on_silence_of`, by bit; with `up_to` P inside the block, only
    /// `[kB, P)` ([`crate::blocks::Statuses::commit_through`]). False when
    /// it is not `held`.
    pub fn commit_through(
        &self,
        output: usize,
        k: u64,
        on_silence_of: u8,
        up_to: Option<u64>,
    ) -> Result<bool, ControlError> {
        self.output_index(output)?;
        Ok(self.replica()?.0.statuses[output].commit_through(k, on_silence_of, up_to))
    }

    /// Replicated mode: whether output `output` commits, and since when.
    pub fn standing(&self, output: usize) -> Option<Standing> {
        Some(self.replica.as_ref()?.0.statuses.get(output)?.standing())
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
            log_overflows: c.log_overflows.load(Relaxed),
            status_changes_lost: c.status_changes_dropped.load(Relaxed)
                + c.status_changes_unsent.load(Relaxed),
            build_lead: self.build_lead.load(Relaxed),
            build_time: self.build_time.load(Relaxed),
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
    /// history after `k0` (`historySlackMs`): the history of each delay
    /// line its taps reach is copied from the rings after the core, while
    /// the RT thread runs on. A copy left behind by a timeout is returned by the next
    /// call unless a control was sent or a hand-over installed after its
    /// buffer was. Calls serialise:
    /// one buffer is with the RT thread at a time. The copy holds every
    /// control sent before the call, such as an entry given as
    /// unresolved.
    pub fn copy_core(&self, timeout: Duration) -> Result<CoreCopy, CopyError> {
        let (Some((shared, control)), Some(transfer)) = (&self.replica, &self.transfer) else {
            return Err(CopyError::NotReplicated);
        };
        let mut t = transfer.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = Instant::now() + timeout;
        let before = control.lock().expect("control lock").sent;
        let copy = loop {
            if !t.in_flight {
                // The RT thread takes controls at the start of a wake and
                // copies after a block: a buffer sent while a control waits
                // could be filled in the wake before the one that takes the
                // control. Sent once every control was taken, it is filled
                // after them.
                let sent = loop {
                    let c = control.lock().expect("control lock");
                    if c.ring.slots() == UNRESOLVED {
                        break c.sent;
                    }
                    drop(c);
                    if Instant::now() >= deadline {
                        return Err(CopyError::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(1));
                };
                let (n, m, slots, page, outs) = t.shape;
                let mut core = t
                    .spare
                    .take()
                    .unwrap_or_else(|| Box::new(Core::new(n, m, slots, page, outs)));
                core.prepare();
                if t.cores.push(core).is_err() {
                    unreachable!("one buffer is with the RT thread at most");
                }
                t.in_flight = true;
                t.in_flight_after = sent;
            }
            if let Ok(copy) = t.copies.pop() {
                t.in_flight = false;
                // A buffer an earlier call left behind can predate controls
                // sent since: it is filled again.
                if t.in_flight_after < before {
                    t.spare = Some(copy.core);
                    continue;
                }
                break copy;
            }
            if Instant::now() >= deadline {
                return Err(CopyError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let mut copy = copy;
        // The delays' history before the block after the core, kept only
        // as long as the RT thread has not reached it, as checked below.
        let end = (copy.k0 + 1) * self.block;
        let slots = copy.core.slots.iter_mut().map(|s| &mut s.delay);
        let lines = slots
            .zip(&self.delays)
            .chain((copy.core.outs.iter_mut()).filter_map(|(o, part)| {
                Some((&mut part.delay, self.out_delays.get(*o as usize)?))
            }));
        for (d, rings) in lines {
            if let (Some(d), Some(rings)) = (d, rings) {
                let len = d.span_len(rings.max()) as u32;
                d.span = rings.copy(usize::from(d.set), end, len);
            }
        }
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

    /// Replicated mode with more than one voter: a replay clone of this
    /// engine from `copy`, a donor's core, computing the blocks after
    /// `copy.k0` off the RT thread from past input. Its readers are on
    /// a new instance per input domain, on the flows this engine's
    /// attached inputs read. A core with a generation-needing entry in
    /// flight (a pending generation, an entry awaiting one, a crossfade
    /// running or an `InputUp`, `Route` or `Detach` pending) is refused:
    /// copy again once it applied. So is a core whose detached slots are
    /// not the ones detached here: the replay reads the flows of this
    /// engine's inputs.
    pub fn replay(&self, mut copy: CoreCopy) -> Result<Replay, ReplayError> {
        if self.transfer.is_none() {
            return Err(ReplayError::Invalid(
                "the engine is not replicated with more than one voter".to_owned(),
            ));
        }
        let c = &mut copy.core;
        let (n, m, _, page, _) = self
            .transfer
            .as_ref()
            .map(|t| t.lock().unwrap_or_else(|e| e.into_inner()).shape)
            .expect("checked above");
        let (slots, in_domains) = new_slots(&self.config);
        let legs: Vec<usize> = slots.iter().map(|s| s.channels).collect();
        let ducked = &self.config.duck_columns;
        let outputs = self.config.outputs.len();
        let limiters: Vec<Option<usize>> = (self.config.outputs.iter())
            .map(|o| o.limiter.then_some(o.channels as usize))
            .collect();
        let out_strips: Vec<Option<usize>> = (self.config.outputs.iter())
            .map(|o| o.strip.then_some(o.channels as usize))
            .collect();
        let delays: Vec<u32> = self
            .config
            .inputs
            .iter()
            .map(|i| i.max_delay_samples)
            .collect();
        let out_delays: Vec<(usize, u32)> = (self.config.outputs.iter())
            .map(|o| (o.channels as usize, o.max_delay_samples))
            .collect();
        let tones: Vec<bool> = self
            .config
            .inputs
            .iter()
            .map(|i| i.tone.is_some())
            .collect();
        let ramp = self.config.ramp_samples;
        let meters = outputs + self.config.meters.len();
        if !c.fits(
            n,
            m,
            outputs,
            meters,
            &legs,
            &limiters,
            &out_strips,
            &delays,
            &out_delays,
            &tones,
            ramp,
            page,
            ducked,
        ) {
            return Err(ReplayError::Invalid(
                "the core is of another engine shape".to_owned(),
            ));
        }
        let next = copy
            .k0
            .checked_add(1)
            .and_then(|k| k.checked_mul(self.block))
            .filter(|&n| n <= crate::transfer::INDEX_MAX)
            .ok_or_else(|| ReplayError::Invalid(format!("a core after block {}", copy.k0)))?;
        let ramp = u64::from(self.config.ramp_samples);
        let generating = c.pending_gen.is_some()
            || !c.awaiting_gen.is_empty()
            || c.slots
                .iter()
                .any(|s| s.xfade.is_some_and(|x| next < x.saturating_add(ramp)))
            || c.pending.iter().any(|e| e.change.needs_generation());
        if generating {
            return Err(ReplayError::Generation);
        }
        let status = self.status();
        let detached = |i: &InputStatus| matches!(i.state, InputState::Detached | InputState::Tone);
        if (c.slots.iter().zip(&status.inputs)).any(|(s, i)| s.detached != detached(i)) {
            return Err(ReplayError::Invalid(
                "the core's detached inputs are not this engine's".to_owned(),
            ));
        }
        let timing = Timing {
            block: self.block,
            margin: self.margin,
            max_catchup_blocks: u64::from(self.config.max_catchup_blocks),
            resync_budget: self.config.resync_budget_samples(),
        };
        let attach = |e: samplez_mxl::Error| ReplayError::Attach(e.to_string());
        // libmxl opens only flows a lookup found, so a missing one fails
        // the replay at once rather than once a waiting lookup gave up.
        let attached: Vec<(PathBuf, &str)> = (slots.iter().zip(&status.inputs))
            .filter(|(_, i)| i.state == InputState::Attached)
            .map(|(s, i)| {
                let domain = &self.config.domains[in_domains[s.domain]].path;
                (lookup::flow_dir(domain, &i.flow_id), i.flow_id.as_str())
            })
            .collect();
        let dirs: Vec<PathBuf> = attached.iter().map(|a| a.0.clone()).collect();
        let found = (self.replay_gate.lock())
            .unwrap_or_else(|e| e.into_inner())
            .found(&dirs, GATE_WAIT);
        if let Some(((_, id), f)) = attached.iter().zip(&found).find(|(_, f)| **f != Found::Yes) {
            return Err(ReplayError::Attach(unopened(id, *f)));
        }
        let mut gens = Vec::with_capacity(in_domains.len());
        for (d, &domain) in in_domains.iter().enumerate() {
            let path = &self.config.domains[domain].path;
            let mut g = Generation::new(&self.api, path, slots.len()).map_err(attach)?;
            for (i, slot) in slots.iter().enumerate() {
                let input = &status.inputs[i];
                if slot.domain != d || input.state != InputState::Attached {
                    continue;
                }
                let cfg = self
                    .config
                    .input_config(&self.config.inputs[i], &input.flow_id);
                g.attach(i, &cfg, &timing).map_err(attach)?;
            }
            gens.push(Some(Box::new(Gen {
                mxl: g,
                prune_after: vec![u64::MAX; slots.len()],
                prune_late: vec![u64::MAX; slots.len()],
                rerouted: 0,
            })));
        }
        let (hashes_tx, hashes) = rtrb::RingBuffer::new(4 * self.config.outputs.len().max(1));
        let (control, control_rx) = rtrb::RingBuffer::new(UNRESOLVED);
        let mut rt = replay_rt(&self.config, &self.api, gens, hashes_tx, control_rx);
        // The delays' history before the core, into the replay's rings.
        let end = next;
        let slots =
            (rt.slots.iter().map(|s| &s.delay)).zip(c.slots.iter_mut().map(|s| &mut s.delay));
        let outs = (c.outs.iter_mut())
            .filter_map(|(o, part)| Some((&rt.outs.get(*o as usize)?.delay, &mut part.delay)));
        for (l, d) in slots.chain(outs) {
            if let (Some(l), Some(d)) = (l, d) {
                let set = l.rings().active();
                l.rings().fill(set, end, &d.span);
                d.set = set as u8;
            }
        }
        rt.install_core(c, copy.last_id, true);
        Ok(Replay {
            rt: Box::new(rt),
            hashes,
            control,
            pages: Vec::new(),
            silenced: Vec::new(),
            next: copy.k0 + 1,
            last_id: copy.last_id,
            late: Vec::new(),
            duck_columns: self.config.duck_columns.clone(),
            meters: self.config.outputs.len() + self.config.meters.len(),
        })
    }

    /// Replicated mode with more than one voter: passes `replay`'s state
    /// to the RT thread, which installs it before its next block and
    /// computes on from it. The RT thread installs a core only before the
    /// block after the core's: once it computed a block, the replay runs
    /// up to that block and its state is passed on at once, before the
    /// RT thread's next wake. One that comes back refused for a block the
    /// RT thread passed, or for a pending generation it does not name,
    /// is tried again until `timeout`. Returns the block the installed
    /// state followed. The unresolved entries become the replay's.
    /// [`ReplayError::Missed`] when the RT thread took an entry off its
    /// pending list that the replay does not know: append it, then hand
    /// over again, or, once the replay computed past the entry's index,
    /// copy a new core and replay from it. An answer to a call that timed
    /// out is taken by the next call: if that hand-over was installed, the
    /// unresolved entries become its core's.
    pub fn hand_over(&self, replay: &mut Replay, timeout: Duration) -> Result<u64, ReplayError> {
        // Up to the last block the RT thread computed, before the first
        // attempt.
        replay.run(self.counters.processed.load(Acquire).saturating_sub(1));
        let rings = &self.delays;
        // The replay's delay history goes into the RT thread's other rings,
        // which the core names and only an install switches to. The first
        // attempt copies all of it; each later one only the blocks the
        // replay computed since, so that it fits between two blocks of the
        // RT thread however long the delay. A copy is keyed to the set it
        // went to: should an earlier call's core install meanwhile, the
        // other set is copied whole. Each attempt runs after the answers
        // to earlier calls are taken.
        let out_rings = &self.out_delays;
        let mut filled: Vec<Option<(usize, u64)>> = vec![None; rings.len() + out_rings.len()];
        self.offer(timeout, 0, true, &[], &[], &[], &[], &[], &[], |j, core| {
            replay.run(j);
            replay.rt.capture(core);
            let end = replay.next * replay.rt.block;
            let lines = (replay.rt.slots.iter().map(|s| &s.delay))
                .zip(rings)
                .chain((replay.rt.outs.iter().map(|o| &o.delay)).zip(out_rings));
            for ((l, rings), done) in lines.zip(filled.iter_mut()) {
                if let (Some(l), Some(rings)) = (l, rings) {
                    *done = Some(samplez_dsp::copy_since(
                        l.rings(),
                        rings,
                        end,
                        l.span(end),
                        *done,
                    ));
                }
            }
            for (s, rings) in core.slots.iter_mut().zip(rings) {
                if let (Some(d), Some(rings)) = (&mut s.delay, rings) {
                    d.set = rings.inactive() as u8;
                }
            }
            for (o, part) in core.outs.iter_mut() {
                if let (Some(d), Some(Some(rings))) = (&mut part.delay, out_rings.get(*o as usize))
                {
                    d.set = rings.inactive() as u8;
                }
            }
            (replay.next - 1, replay.last_id)
        })
    }

    /// Replicated mode with more than one voter: drops the RT thread's
    /// core. The RT thread keeps its writers, hold rings and statuses but
    /// computes no block: each is withheld, cause [`Cause::NoCore`], and
    /// the replica is withdrawn, until [`Engine::install_product`] or
    /// [`Engine::hand_over`] installs a core.
    pub fn drop_core(&self) -> Result<(), ControlError> {
        if self.transfer.is_none() {
            return Err(ControlError::Invalid(
                "the engine is not replicated with more than one voter".to_owned(),
            ));
        }
        self.control(Control::DropCore)
    }

    /// Drops the RT thread's core, unless a drop was sent since the last
    /// install known, and waits until `deadline` for the RT thread to take
    /// it. While a core is held, an entry could swap in a generation built
    /// on the old flows after the product state's one for its domain.
    fn drop_core_by(&self, deadline: Instant) -> Result<(), ReplayError> {
        let (Some((_, control)), Some(transfer)) = (&self.replica, &self.transfer) else {
            return Err(ReplayError::Invalid(
                "the engine is not replicated with more than one voter".to_owned(),
            ));
        };
        // A hand-over left unanswered by a timeout may install one: the
        // drop has to follow its answer.
        let mut unanswered = loop {
            let t = transfer.lock().unwrap_or_else(|e| e.into_inner());
            if !t.handing || t.handed.slots() > 0 {
                break t.handing;
            }
            drop(t);
            if Instant::now() >= deadline {
                return Err(ReplayError::Timeout { passed: 0 });
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        loop {
            let (core, taken) = {
                let c = control.lock().expect("control lock");
                (c.core, c.ring.slots() == UNRESOLVED)
            };
            if !core && !unanswered && taken {
                return Ok(());
            }
            // A full ring refuses it: the RT thread empties it at its
            // next wake.
            let refused = (core || unanswered) && self.drop_core().is_err();
            if !refused {
                unanswered = false;
            }
            if Instant::now() >= deadline {
                return Err(if refused {
                    ReplayError::Full
                } else {
                    ReplayError::Timeout { passed: 0 }
                });
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Replicated mode with more than one voter: builds a core from
    /// `state` off the RT thread, its fades cold, its pending entries, and
    /// installs it as a replay is handed over, at the first block
    /// boundary at or after sample index `at_least` the RT thread has not
    /// started, the RT thread keeping the generation it holds pending.
    /// Its gains ramp from silence to their targets over the ramp length
    /// from the install, as the strips take their parameters, so an
    /// output fades in processed. Each meter's loudness measurement runs
    /// or is paused from the install on as `loudness_running` and the
    /// entries the install passed say; the measurement itself is the
    /// replica's own and carries on. Returns the block the core followed.
    /// An `at_least` further ahead than `timeout` is refused as invalid
    /// before anything changes, as is a state holding a gain, flow,
    /// strip or tone parameter an entry could not set, a tone generator
    /// down or with a flow, or more meters than the engine has. A slot
    /// whose flow is none is detached: no reader is built for it. The
    /// core holds no unresolved entry: the caller adds the ones still
    /// unresolved with [`Engine::unresolved`] after the install. While no core is held the
    /// RT thread takes the entries needing no generation whose index
    /// passed off its pending list, applying only strip entries, to the
    /// strips the slots keep; [`ReplayError::Missed`] when it took one
    /// above `state.last_id`. A core still held is dropped first, the
    /// call failing with [`ReplayError::Full`] while the control ring
    /// stays full; on a later failure the core stays dropped. `timeout`
    /// bounds the drop, the supervisor's hand-over and the install
    /// together, except that the answer to a core already passed to the
    /// RT thread is waited for up to 1 s past it.
    pub fn install_product(
        &self,
        state: &ProductState,
        at_least: u64,
        timeout: Duration,
    ) -> Result<u64, ReplayError> {
        let deadline = Instant::now() + timeout;
        let left = || deadline.saturating_duration_since(Instant::now());
        let (n, m) = self.shape;
        let slots = self.config.inputs.len();
        if state.gains.len() != n
            || state.gains.iter().any(|row| row.len() != m)
            || !(state.fading.is_empty()
                || state.fading.len() == n && state.fading.iter().all(|row| row.len() == m))
            || state.up.len() != slots
            || state.flows.len() != slots
            || state.strips.len() != slots
            || state.delays.len() > slots
            || state.tones.len() > slots
            || state.tone_ramps.len() > slots
            || state.outputs.len() > self.config.outputs.len()
            || state.pre_strip_meters.len() > slots
            || state.limiters.len() > self.config.outputs.len()
            || state.output_strips.len() > self.config.outputs.len()
            || state.output_delays.len() > self.config.outputs.len()
            || state.loudness_running.len() > self.config.outputs.len() + self.config.meters.len()
        {
            return Err(ReplayError::Invalid(format!(
                "a product state of the shape {n} x {m} with {slots} inputs, at most {} \
                 outputs and at most {} meters",
                self.config.outputs.len(),
                self.config.outputs.len() + self.config.meters.len()
            )));
        }
        for (slot, (flow, params)) in state.flows.iter().zip(&state.strips).enumerate() {
            if let Some(flow) = flow {
                self.config
                    .check_input_flow(slot, flow)
                    .map_err(|e| ReplayError::Invalid(e.0))?;
            }
            for param in params {
                check_strip(&self.config, slot, param).map_err(ReplayError::Invalid)?;
            }
        }
        for (output, params) in state.limiters.iter().enumerate() {
            if let Some(params) = params {
                check_limiter(&self.config, output, params).map_err(ReplayError::Invalid)?;
            }
        }
        for (output, params) in state.output_strips.iter().enumerate() {
            for param in params {
                check_output_strip(&self.config, output, param).map_err(ReplayError::Invalid)?;
            }
        }
        for (slot, &samples) in state.delays.iter().enumerate() {
            check_delay(&self.config, slot, samples).map_err(ReplayError::Invalid)?;
        }
        for (output, &samples) in state.output_delays.iter().enumerate() {
            check_output_delay(&self.config, output, samples).map_err(ReplayError::Invalid)?;
        }
        for (slot, &on) in state.pre_strip_meters.iter().enumerate() {
            check_pre_strip_meter(&self.config, slot, on).map_err(ReplayError::Invalid)?;
        }
        for (slot, params) in state.tones.iter().enumerate() {
            if let Some(params) = params {
                check_tone(&self.config, slot, params).map_err(ReplayError::Invalid)?;
            }
        }
        for (slot, ramps) in state.tone_ramps.iter().enumerate() {
            if let Some(ramps) = ramps {
                check_tone(&self.config, slot, &ToneParams::DEFAULT)
                    .map_err(ReplayError::Invalid)?;
                if !ramps.is_valid() {
                    return Err(ReplayError::Invalid(format!(
                        "tone ramps of input slot {slot} hold a value no generator holds"
                    )));
                }
            }
        }
        // No entry takes a tone generator down.
        if let Some(slot) =
            (0..slots).find(|&s| !state.up[s] && self.config.inputs[s].tone.is_some())
        {
            return Err(ReplayError::Invalid(
                self.config.check_flow_slot(slot).unwrap_err().0,
            ));
        }
        check_product_ducks(&self.config, state).map_err(ReplayError::Invalid)?;
        if let Some(gain) = (state.gains.iter().flatten()).find(|g| !samplez_dsp::valid_gain(**g)) {
            return Err(ReplayError::Invalid(format!(
                "gain {gain} is not finite within MAX_GAIN"
            )));
        }
        if let Some(f) = (state.fading.iter().flatten().flatten())
            .find(|f| !samplez_dsp::valid_gain(f.from) || f.len > samplez_dsp::MAX_EXACT_RAMP_LEN)
        {
            return Err(ReplayError::Invalid(format!(
                "a fade from {} over {} samples is not within the fade bounds",
                f.from, f.len
            )));
        }
        product::check(&self.config, (n, m), state).map_err(ReplayError::Invalid)?;
        let strips = product_strips(&state.strips);
        let out_strips = product_strips(&state.output_strips);
        let limiters: Vec<(usize, LimiterParams)> = (state.limiters.iter().enumerate())
            .filter_map(|(o, p)| Some((o, (*p)?)))
            .collect();
        let delays: Vec<(usize, u32)> = state.delays.iter().copied().enumerate().collect();
        let out_delays: Vec<(usize, u32)> =
            (state.output_delays.iter().copied().enumerate()).collect();
        // Per tone generator the ramps the state carries, or its target
        // held where it carries none.
        let tones: Vec<(usize, ToneRamps)> = (0..slots)
            .filter_map(|slot| {
                let ramps = state.tone_ramps.get(slot).copied().flatten();
                let held = state
                    .tones
                    .get(slot)
                    .copied()
                    .flatten()
                    .map(ToneRamps::held);
                Some((slot, ramps.or(held)?))
            })
            .collect();
        // The entries of one log entry are adjacent and fold into one.
        let mut pending = Vec::with_capacity(state.pending.len());
        let mut batches = Vec::new();
        for group in state.pending.chunk_by(|a, b| a.0 == b.0) {
            let (id, at_index, _) = group[0];
            let entries: Vec<Entry> = group.iter().map(|g| g.2.clone()).collect();
            if let Some(f) = fade_beyond(&entries, at_index) {
                return Err(ReplayError::Invalid(format!(
                    "{f:?} starts more than the fade bound past {at_index}"
                )));
            }
            let mut items = Vec::with_capacity(group.len());
            for (_, _, e) in group {
                self.items(e, &mut items)
                    .map_err(|e| ReplayError::Invalid(e.to_string()))?;
            }
            let folded = batch::fold(&items, m, n * m).map_err(ReplayError::Invalid)?;
            let Some((change, b)) = folded else { continue };
            pending.push(RtEntry {
                id,
                at_index,
                change,
            });
            if let Some(b) = b {
                batches.push((id, b));
            }
        }
        pending.sort_by_key(|e| (e.at_index, e.id));
        if pending.len() > PENDING_CHANGES {
            return Err(ReplayError::Full);
        }
        // One that cannot be installed within the timeout is refused while
        // the core the RT thread holds is still there.
        let within = (timeout.as_nanos() * samplez_mxl::SAMPLE_RATE as u128 / 1_000_000_000) as u64;
        let now = self.counters.processed.load(Acquire) * self.block;
        if at_least > now.saturating_add(within) {
            return Err(ReplayError::Invalid(format!(
                "index {at_least} lies beyond the timeout, {within} samples after {now}"
            )));
        }
        self.drop_core_by(deadline)?;
        // The supervisor takes the slots' state and flows, and hands the
        // generations reading them over, before the core: a slot the core
        // has up reads its flow once installed.
        let (done, applied) = mpsc::sync_channel(1);
        let _ = self
            .log
            .lock()
            .expect("log lock")
            .sup
            .send(LogMsg::Product {
                up: state.up.clone(),
                flows: state.flows.clone(),
                leaving: product::leaving(state, self.config.ramp_samples),
                done,
            });
        if applied.recv_timeout(left()).is_err() {
            return Err(ReplayError::Timeout { passed: 0 });
        }
        let mut fill = product::ProductFill::new(&self.config, self.shape, state, pending, batches);
        self.offer(
            left(),
            at_least,
            false,
            &strips,
            &limiters,
            &delays,
            &out_strips,
            &out_delays,
            &tones,
            |j, core| fill.fill(j, core),
        )
    }

    /// Passes the cores `make` fills to the RT thread until one is
    /// installed or `timeout` passes. The answer to a core passed on is
    /// waited for up to 1 s, past `timeout` too: the RT thread gives it at
    /// its next wake, so a caller with a short timeout still learns
    /// whether that core was installed. An answer still missing then is
    /// taken by the next call. The RT thread installs a core only
    /// before the block after the core's: each attempt waits for it to
    /// compute a block, has `make` fill a core for that block at once and
    /// passes it on before the RT thread's next wake. `make` returns the
    /// block the core follows and the highest log id it knew. A
    /// `replayed` core must name the pending generation the RT thread
    /// holds, and its strips are installed.
    /// No core is installed before the first block starting at or after
    /// sample index `at_least`.
    #[allow(clippy::too_many_arguments)]
    fn offer(
        &self,
        timeout: Duration,
        at_least: u64,
        replayed: bool,
        strips: &[(usize, StripParam)],
        limiters: &[(usize, LimiterParams)],
        delays: &[(usize, u32)],
        out_strips: &[(usize, StripParam)],
        out_delays: &[(usize, u32)],
        tones: &[(usize, ToneRamps)],
        mut make: impl FnMut(u64, &mut Core) -> (u64, LogId),
    ) -> Result<u64, ReplayError> {
        let (Some((_, control)), Some(transfer)) = (&self.replica, &self.transfer) else {
            return Err(ReplayError::Invalid(
                "the engine is not replicated with more than one voter".to_owned(),
            ));
        };
        let mut t = transfer.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = Instant::now() + timeout;
        let (n, m, slots, page, outs) = t.shape;
        let new = || Box::new(Core::new(n, m, slots, page, outs));
        let mut core = t.spare.take().unwrap_or_else(new);
        // Hand-overs before this one were sent by earlier calls.
        let first = t.seq + 1;
        let period =
            Duration::from_nanos(self.block * 1_000_000_000 / samplez_mxl::SAMPLE_RATE as u64);
        let poll = period / 16;
        let processed = || self.counters.processed.load(Acquire);
        // This call's hand-overs refused for a block the RT thread passed.
        let mut passed = 0;
        loop {
            // The RT thread answers at its next wake. One unanswered for a
            // second can only be refused: the RT thread is then past its
            // block. The next call collects the answer.
            let asked = Instant::now();
            while t.handing {
                let Ok(h) = t.handed.pop() else {
                    if asked.elapsed() > Duration::from_secs(1) {
                        t.spare = Some(core);
                        return Err(ReplayError::Timeout { passed });
                    }
                    std::thread::sleep(poll);
                    continue;
                };
                t.handing = false;
                if h.answer == Answer::Installed {
                    let mut control = control.lock().expect("control lock");
                    control.core = true;
                    control.sent += 1;
                    control.unresolved.clear();
                    control
                        .unresolved
                        .extend(h.core.unresolved.iter().map(|u| u.0));
                }
                match h.answer {
                    _ if h.seq < first => {}
                    Answer::Installed => {
                        t.spare = Some(h.core);
                        return Ok(h.j);
                    }
                    Answer::Missed(id) => {
                        t.spare = Some(h.core);
                        return Err(ReplayError::Missed { id });
                    }
                    Answer::Passed => passed += 1,
                    Answer::Generation | Answer::Waiting => {}
                }
                core = h.core;
            }
            if Instant::now() >= deadline {
                t.spare = Some(core);
                return Err(ReplayError::Timeout { passed });
            }
            // The step to the next block: the RT thread sleeps after it
            // until its next wake, which installs a core for that block,
            // about one block period later. A sleep could take most of
            // that: the step is waited for by yielding, for one block
            // period, the longest a running RT thread takes to step. Past
            // that the RT thread is stalled, and a sixteenth of a period
            // is slept between looks, so the wait does not spend the CPU
            // a stalled RT thread may be waiting for.
            // Before the wait: the batches it carries are dropped here.
            core.prepare();
            let p = processed();
            let waiting = Instant::now();
            // The core is installed before block `processed()`.
            while processed() == p || processed() * self.block < at_least {
                if Instant::now() >= deadline {
                    t.spare = Some(core);
                    return Err(ReplayError::Timeout { passed });
                }
                if waiting.elapsed() < period {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(poll);
                }
            }
            let (j, last_id) = make(processed().saturating_sub(1), &mut core);
            t.seq += 1;
            let h = Handover {
                seq: t.seq,
                j,
                last_id,
                core,
                replayed,
                strips: strips.to_vec(),
                limiters: limiters.to_vec(),
                delays: delays.to_vec(),
                out_strips: out_strips.to_vec(),
                out_delays: out_delays.to_vec(),
                tones: tones.to_vec(),
                answer: Answer::Waiting,
            };
            if t.handovers.push(h).is_err() {
                unreachable!("one hand-over is with the RT thread at most");
            }
            t.handing = true;
            core = t.spare.take().unwrap_or_else(new);
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

    /// Restarts the loudness measurement of `meter`, an output or, after
    /// the outputs, a configured meter (the integrated loudness, the
    /// loudness range, the maxima and the counts of gaps, lost and paused
    /// slices) at the first 100 ms window starting at or after
    /// `at_index`, a multiple of the block size, as [`Entry::ResetLoudness`]
    /// does; the K-weighting starts
    /// from silence there, and the true-peak interpolator keeps the 11
    /// output samples before it. From that index on the integrated
    /// loudness, the loudness range and the maxima depend only on the
    /// output samples from those 11 on, and so do the momentary and
    /// short-term loudness once 0.4 s and 3 s past it: replicas that
    /// computed those samples and reset at the same index agree there.
    /// A reset replaces one whose window is not reached yet. Refused as
    /// [`Engine::set_gain`] refuses a change, and as invalid for no such
    /// meter: an engine that follows a replicated log takes the entry
    /// from the log.
    pub fn reset_loudness(&self, meter: usize, at_index: u64) -> Result<(), GainError> {
        let entry = Entry::ResetLoudness { meter };
        self.set_local_entry(&entry, at_index, "loudness resets")
    }

    /// Runs the pre-strip meter of input slot `slot` from sample index
    /// `at_index`, a multiple of the block size, or stops it, as
    /// [`Entry::PreStripMeter`] does. Refused as [`Engine::set_gain`]
    /// refuses a change, and as invalid for a slot that does not exist
    /// or, switched on, is configured without `preStripMeter`.
    pub fn set_pre_strip_meter(
        &self,
        slot: usize,
        enabled: bool,
        at_index: u64,
    ) -> Result<(), GainError> {
        check_pre_strip_meter(&self.config, slot, enabled).map_err(GainError::Invalid)?;
        let change = Change::PreStripMeter { slot, enabled };
        self.set_local(change, at_index, "pre-strip meter changes")
    }

    /// Runs the loudness measurement of `meter` from sample index
    /// `at_index`, a multiple of the block size, or pauses it, as
    /// [`Entry::LoudnessRun`] does. Refused as
    /// [`Engine::reset_loudness`] refuses a reset.
    pub fn set_loudness_running(
        &self,
        meter: usize,
        running: bool,
        at_index: u64,
    ) -> Result<(), GainError> {
        let entry = Entry::LoudnessRun { meter, running };
        self.set_local_entry(&entry, at_index, "loudness pauses")
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
    /// Detached by a `Detach`: in no generation, not polled, reported
    /// nothing.
    Detached,
}

/// The outgoing flow a detach of a slot in `state` keeps read until its
/// crossfade to silence ends at `end`. A pruned slot is read by nothing,
/// so no reader is attached for its flow.
fn detach_routing(state: &SupState, end: u64, flow: &str) -> Option<(u64, String)> {
    (!matches!(state, SupState::Pruned { .. })).then(|| (end, flow.to_owned()))
}

/// What a slot in `state` says of its flow in input liveness
/// ([`InputStatus::readable`]): read when attached, read when pruned with
/// its head `advancing` for the recovery period, else not; nothing while
/// it waits for its generation or is detached.
fn liveness_vote(state: &SupState, advancing: bool) -> Option<bool> {
    match state {
        SupState::Attached => Some(true),
        SupState::Pruned { .. } => Some(advancing),
        SupState::Wanted | SupState::Detached => None,
    }
}

struct SupSlot {
    state: SupState,
    window: VecDeque<SlotStats>,
    /// The flow the slot reads; detached, the one it read last, empty
    /// for one that has read none.
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
    /// Whether its flow's head stays off the MXL clock.
    gap: ClockGap,
}

/// A generation-needing entry, from its append until the RT thread
/// swapped its generation in or handed it back.
struct Job {
    id: LogId,
    at_index: u64,
    /// The slots the entry raises, by bit.
    up: u64,
    /// The slots it re-routes, with their new flows, none for one it
    /// detaches.
    routes: Vec<(usize, Option<String>)>,
    domain: usize,
    stage: Stage,
    /// Its generation's readers opening, from its append until it is
    /// built.
    draft: Option<Draft>,
    /// The slots its generation left out for a reader not open yet.
    opening: u64,
}

/// What a product state still needs handed to the RT thread before
/// `done` is sent: per input domain its generation, and the front job's
/// sent before it.
struct ProductHandOver {
    done: SyncSender<()>,
    domains: Vec<bool>,
    resend: bool,
}

enum Stage {
    /// Waits for the job before it.
    Waiting,
    Built(Box<Gen>, u64),
    /// Handed to the RT thread, with the slots its generation holds.
    Sent(u64),
}

struct SupOut {
    /// Enabled as the supervisor last read it.
    enabled: bool,
    realised: bool,
    next_try: Instant,
    backoff: Duration,
}

/// The poll instance of one input domain and its poll readers by flow id.
struct Pollers {
    instance: Arc<Instance>,
    /// The one thread every poll reader of the domain opens on.
    opener: PollOpener,
    flows: HashMap<String, PolledFlow>,
    /// Per flow id, the references its poll readers let go, kept while
    /// one is unreleased, so that a poll reader created anew does not
    /// open the flow before they are released.
    releases: HashMap<String, Releases>,
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
    /// Per input domain, the slots, by bit, a product state moved to
    /// another flow: the next generation built starts their readers
    /// anew.
    reroute: Vec<u64>,
    /// A product state's generations not all handed over yet.
    product: Option<ProductHandOver>,
    pollers: Vec<Option<Pollers>>,
    /// Slots whose poll hold waits until no entry falls due soon
    /// ([`Supervisor::take_poll_hold`]).
    poll_setups: Vec<usize>,
    /// The lookups of the flows a generation build attaches.
    gate: Gate,
    /// The lookups of the output flows about to be created.
    out_gate: Gate,
    /// The last failure to create each input domain's poll instance and
    /// generation, logged once per change of its text.
    poll_errors: Vec<Option<String>>,
    rebuild_errors: Vec<Option<String>>,
    outs: Vec<SupOut>,
    /// Output instance per configuration domain.
    out_instances: Vec<Option<Arc<Instance>>>,
    /// Generation-needing entries, oldest first; each one's readers open
    /// from its append, only the first is built.
    jobs: VecDeque<Job>,
    openers: Openers,
    next_draft: u64,
    /// Per input domain, a rebuild's draft.
    rebuilding: Vec<Option<Draft>>,
    /// Slots left out of a generation for a reader still opening, by bit:
    /// such a reader, opened, joins through the next build.
    awaiting: u64,
    /// Readers the openers sent back while the supervisor waited for one.
    arrived: Vec<Opened>,
    /// Slots and flows whose reader opened after their generation left
    /// it out: they join through the next build.
    rejoins: Vec<(usize, String, Arc<Input>)>,
    /// The readers of the generations built last, by slot and flow: a
    /// generation takes these instead of opening its slots' flows anew.
    /// A slot's are dropped once it is pruned, so it reads a fresh one.
    readers: HashMap<(usize, String), Arc<Input>>,
    log_rx: mpsc::Receiver<LogMsg>,
    log: Arc<Mutex<Log>>,
    /// Where reports go; the engine's own log when absent.
    reports: Option<SyncSender<Report>>,
    build_times: BuildTimes,
    /// Generations built with every slot attached.
    builds: u64,
    build_lead: Arc<AtomicU64>,
    build_time: Arc<AtomicU64>,
    lead: u64,
    ramp: u64,
    cmd: rtrb::Producer<Command>,
    ret: rtrb::Consumer<Returned>,
    tel: rtrb::Consumer<Telemetry>,
    meter_rx: rtrb::Consumer<MeterRecord>,
    next_meter_seq: u64,
    duck_rx: rtrb::Consumer<DuckRecord>,
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
    /// Per pre-strip meter, in slot order, the first index of the
    /// measurement its last restart record started, while it runs: none
    /// while the RT thread meters nothing for it, so its records are
    /// dropped until one restarts it.
    pre_runs: Vec<Option<u64>>,
    verdicts: Option<SyncSender<BlockVerdict>>,
    status: Arc<Mutex<Status>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

/// Samples an input's blocks may stay late with its head frozen before
/// the slot is pruned: `pruneAfterBlocks` blocks, or twice the producer's
/// `maxCommitBatchSizeHint` plus the margin when that is longer.
fn prune_limit(config: &Config, timing: &Timing, hint: u64) -> u64 {
    (u64::from(config.prune_after_blocks) * timing.block).max(2 * hint + timing.margin)
}

/// Samples an input's blocks may stay late with its head moving before
/// the slot is pruned: the frozen-head `limit`, and no less than the
/// [`RECOVERY_NS`] a pruned input takes to come back: a lag shorter than
/// that is silenced while it lasts rather than for a recovery on top.
fn late_prune_limit(config: &Config, limit: u64) -> u64 {
    limit.max(RECOVERY_NS * u64::from(config.sample_rate) / 1_000_000_000)
}

/// Whether `head`, polled at clock index `at`, holds the newest block of
/// `block` samples due by then with reader margin `margin`, the block's
/// range shifted by the input's `offset`: whether that block, read now,
/// would have been on time.
fn within_reach(head: u64, at: u64, block: u64, margin: u64, offset: i64) -> bool {
    let due = at.saturating_sub(margin) / block * block;
    i128::from(head) >= i128::from(due) + i128::from(offset)
}

/// Whether a flow's head stays off the MXL clock: farther from the
/// clock's index, behind or ahead, than the flow's readable history plus
/// the reader margin and the input's `|offsetSamples|`, so that no block
/// read at the clock is in the flow's ring, as with a producer stamping
/// its indices on another epoch. All of it is in samples of the flow.
#[derive(Clone, Copy, Debug, Default)]
struct ClockGap {
    /// The head last observed and the clock index it was first observed
    /// at.
    head: Option<(u64, u64)>,
    /// The clock index of the first observation of the current run off
    /// the clock.
    run: Option<u64>,
    /// The gap reported, clock index minus head, while reported.
    off: Option<i128>,
}

/// A change of [`ClockGap`]'s report, with the gap reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GapChange {
    Off(i128),
    Back(i128),
}

impl ClockGap {
    /// Observes `head` at clock index `at`, for a flow of `history`
    /// samples of readable history, `slack` the reader margin plus the
    /// input's `|offsetSamples|`, and `limit` its frozen-head prune limit.
    /// The head is reported off once it stayed off at every observation
    /// for `history` samples of the clock, and back at the first
    /// observation within reach again. A head unchanged for longer than
    /// `limit` is a stopped producer, which the frozen-head prune reports:
    /// it ends the run, and a head reported off is reported back, so a
    /// head is reported off only while it moves.
    fn observe(
        &mut self,
        head: u64,
        at: u64,
        history: u64,
        slack: u64,
        limit: u64,
    ) -> Option<GapChange> {
        let since = match self.head {
            Some((h, t)) if h == head => t,
            _ => at,
        };
        self.head = Some((head, since));
        let gap = i128::from(at) - i128::from(head);
        if gap.unsigned_abs() <= u128::from(history.saturating_add(slack)) {
            self.run = None;
            return self.off.take().map(GapChange::Back);
        }
        if at.saturating_sub(since) > limit {
            self.run = None;
            return self.off.take().map(GapChange::Back);
        }
        let start = *self.run.get_or_insert(at);
        if self.off.is_none() && at.saturating_sub(start) >= history {
            self.off = Some(gap);
            return Some(GapChange::Off(gap));
        }
        None
    }
}

/// The input error of a head `gap` samples, clock index minus head, off
/// the MXL clock at `rate`.
fn off_clock(gap: i128, rate: u32) -> String {
    let side = if gap > 0 { "behind" } else { "ahead of" };
    format!(
        "head {:.1} s {side} the MXL clock, beyond the flow's history",
        gap.unsigned_abs() as f64 / f64::from(rate)
    )
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
        // Releases go to the reaper thread; one made here is counted.
        samplez_mxl::guard_releases(true);
        while !self.stop.load(Relaxed) {
            while let Ok(r) = self.ret.pop() {
                self.take_back(r);
            }
            self.opened();
            self.telemetry();
            self.status_changes();
            self.page_hashes();
            self.meters();
            self.duck_meters();
            self.log_messages();
            self.hand_product();
            // Committed entries the ring had no room for.
            self.log.lock().expect("log lock").flush();
            self.jobs();
            self.deferred_polls();
            if last_poll.elapsed() >= POLL_PERIOD && !self.handing_over() {
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
                if (self.dirty[d] || self.rebuilding[d].is_some()) && self.cmd.slots() > 0 {
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
                // Build times age out without builds too.
                self.record_build(false, None, false);
                self.leaks();
            }
            if rt_thread.as_ref().is_some_and(JoinHandle::is_finished) {
                samplez_mxl::guard_releases(false);
                self.rt_ended(rt_thread.take());
                break;
            }
            // A tick, cut short by a reader opened, so that a generation
            // waiting for it is built at once.
            if let Ok(o) = self.openers.results.recv_timeout(SUPERVISOR_TICK) {
                self.arrived.push(o);
            }
        }
        // The RT thread sees the same flag.
        samplez_mxl::guard_releases(false);
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
            let m = r.meter as usize;
            // The pre-strip meters come last; one runs from the record
            // that restarts it, measured afresh, and reads nothing while
            // it is off.
            let pre = self.loudness.len() - self.pre_runs.len();
            if m >= pre {
                if r.channel == 0 && r.reset().is_some() {
                    self.pre_runs[m - pre] = r.reset();
                }
                if self.pre_runs[m - pre].is_some() {
                    self.loudness[m].record_restarting(&r);
                }
                continue;
            }
            self.loudness[m].record(&r);
        }
        if seen {
            let mut st = self.status();
            let st = &mut *st;
            let (outs, rest) = self.loudness.split_at(st.outputs.len());
            let (meters, rest) = rest.split_at(st.meters.len());
            let (inputs, pre) = rest.split_at(st.inputs.len());
            let mut pre = pre.iter().zip(&self.pre_runs);
            for (o, l) in st.outputs.iter_mut().zip(outs) {
                o.meters.clone_from(&l.meters);
            }
            for (m, l) in st.meters.iter_mut().zip(meters) {
                m.meters.clone_from(&l.meters);
            }
            for ((i, l), c) in st.inputs.iter_mut().zip(inputs).zip(&self.config.inputs) {
                i.meters.update(&l.meters, c.true_peak);
                if c.pre_strip_meter
                    && let Some((l, run)) = pre.next()
                {
                    i.pre_strip_meters = run.map(|_| {
                        let mut m = i.pre_strip_meters.take().unwrap_or_default();
                        m.update(&l.meters, c.true_peak);
                        m
                    });
                }
            }
        }
    }

    /// Turns the duck records into each group's readings.
    fn duck_meters(&mut self) {
        let mut st: Option<std::sync::MutexGuard<'_, Status>> = None;
        while let Ok(r) = self.duck_rx.pop() {
            let st = st.get_or_insert_with(|| self.status.lock().expect("status lock"));
            if let Some(d) = st.ducks.get_mut(usize::from(r.group)) {
                *d = DuckMeter {
                    slice: Some(r.slice),
                    key: r.key,
                    envelope: r.envelope,
                    engaged: r.engaged,
                };
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
                    frozen,
                    head,
                    end,
                } => self.prune(slot, invalid, frozen, head, end),
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
                Telemetry::PreStripMeterOff { slot, at_index } => {
                    let tap = (self.config.inputs[..slot].iter())
                        .filter(|i| i.pre_strip_meter)
                        .count();
                    // A restart the RT thread made after the stop, whose
                    // record came in first, stands.
                    if let Some(run) = self.pre_runs.get_mut(tap)
                        && run.is_some_and(|start| start <= at_index)
                    {
                        *run = None;
                        self.status().inputs[slot].pre_strip_meters = None;
                    }
                }
                Telemetry::Input { slot, up } => {
                    self.slots[slot].up = up;
                    self.status().inputs[slot].up = up;
                    let state = &self.slots[slot].state;
                    if !up && !matches!(state, SupState::Pruned { .. } | SupState::Detached) {
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
                        "log entry {} ({:?}) at index {} was refused by the strip, limiter, \
                         delay line or tone generator it sets; not applied",
                        entry.id, entry.change, entry.at_index
                    );
                    log(format_args!("diverged: {reason}"));
                    self.status().diverged.get_or_insert(reason);
                }
            }
        }
    }

    /// The input domain of slot `slot`, [`NO_DOMAIN`] for a tone
    /// generator's.
    fn input_domain(&self, slot: usize) -> usize {
        let input = &self.config.inputs[slot];
        let Some(name) = input.domain.as_deref().filter(|_| input.tone.is_none()) else {
            return NO_DOMAIN;
        };
        let d = self.config.domain(name).expect("validated");
        self.in_domains
            .iter()
            .position(|&x| x == d)
            .expect("every input's domain is an input domain")
    }

    fn prune(&mut self, slot: usize, invalid: bool, frozen: bool, head: u64, end: u64) {
        if !matches!(self.slots[slot].state, SupState::Attached) {
            return;
        }
        let name = &self.config.inputs[slot].name;
        let why = if invalid {
            "flow invalid"
        } else if frozen {
            "head frozen"
        } else {
            "blocks late with the head moving"
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
        self.forget(|k| k.0 == slot);
        if report {
            // Pruned for its flow, not left out for a reader still opening.
            self.awaiting &= !bit(slot);
        }
        self.slots[slot].state = SupState::Pruned {
            recovery: Recovery::default(),
            held: false,
        };
        self.take_poll_hold(slot);
        self.slots[slot].reported = false;
        let mut st = self.status();
        st.inputs[slot].state = InputState::Pruned;
        st.inputs[slot].readable = liveness_vote(&self.slots[slot].state, false);
        drop(st);
        if report {
            self.report(Report::InputPruned { slot });
        }
    }

    /// Whether an entry not handed over yet falls due soon ([`due_soon`]):
    /// the supervisor then neither polls nor takes a poll hold, which
    /// start threads, read flows and create an instance, nor creates an
    /// output's instance or writer. A stall there, a libmxl call taking
    /// about 23 ms, can make a hand-over late only if its deadline falls
    /// inside the stall, which the window covers with margin; an entry due
    /// later, as one stamped seconds ahead, holds back none of them.
    fn handing_over(&self) -> bool {
        let earliest = earliest(&self.api, &self.counters, self.timing.margin, self.lead);
        (self.jobs.iter())
            .any(|j| !matches!(j.stage, Stage::Sent(_)) && due_soon(earliest, j.at_index))
    }

    /// Takes the poll hold of pruned `slot` ([`Supervisor::hold_poll`]),
    /// or queues it while an entry falls due soon
    /// ([`Supervisor::handing_over`]).
    fn take_poll_hold(&mut self, slot: usize) {
        if self.handing_over() {
            if !self.poll_setups.contains(&slot) {
                self.poll_setups.push(slot);
            }
            return;
        }
        let held = self.hold_poll(slot);
        if let SupState::Pruned { held: h, .. } = &mut self.slots[slot].state {
            *h = held;
        }
    }

    /// Takes the poll holds queued while an entry fell due soon, once none
    /// does.
    fn deferred_polls(&mut self) {
        if self.poll_setups.is_empty() || self.handing_over() {
            return;
        }
        for slot in std::mem::take(&mut self.poll_setups) {
            if matches!(self.slots[slot].state, SupState::Pruned { held: false, .. }) {
                self.take_poll_hold(slot);
            }
        }
    }

    /// Takes one hold on the poll reader of `slot`'s flow, creating the
    /// domain's poll instance and the poll reader when missing. False when
    /// either could not be created; `poll` tries again. Called only while
    /// no entry falls due soon ([`Supervisor::handing_over`]): creating the
    /// poll instance, starting the domain's one poll open thread with it
    /// and starting the poll reader's lookup thread can stall the
    /// supervisor as an instance creation there did. The poll readers'
    /// opens run on that thread, so no open starts a thread.
    fn hold_poll(&mut self, slot: usize) -> bool {
        let d = self.input_domain(slot);
        let id = self.slots[slot].flow.clone();
        if self.pollers[d].is_none() {
            let path = &self.config.domains[self.in_domains[d]].path;
            // The opener first: an instance created for nothing would be
            // destroyed here.
            let started = PollOpener::start()
                .map_err(|e| format!("its open thread: {e}"))
                .and_then(|o| {
                    let instance = Instance::new(&self.api, path);
                    instance.map(|i| (i, o)).map_err(|e| e.to_string())
                });
            match started {
                Ok((instance, opener)) => {
                    self.poll_errors[d] = None;
                    self.pollers[d] = Some(Pollers {
                        instance,
                        opener,
                        flows: HashMap::new(),
                        releases: HashMap::new(),
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
            f.want();
            self.openers.reap(f.take_released());
        } else {
            match PolledFlow::looking_up_with(&p.instance, &id, self.config.look) {
                Ok(f) => {
                    let releases = p.releases.entry(id.clone()).or_default();
                    let f = f.with_releases(releases).with_opener(&p.opener);
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
        let Some(p) = &mut self.pollers[d] else {
            return;
        };
        let Some(f) = p.flows.get_mut(id) else {
            return;
        };
        let left = f.release();
        self.openers.reap(f.take_released());
        if left == 0
            && let Some(gone) = p.flows.remove(id)
        {
            self.openers.reap(gone);
        }
    }

    fn poll(&mut self) {
        for slot in 0..self.slots.len() {
            if matches!(self.slots[slot].state, SupState::Pruned { held: false, .. }) {
                self.take_poll_hold(slot);
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
        let at = time::timestamp_to_index(&self.api, now);
        // Each flow's head and prune limit, and its readable history.
        let mut heads: HashMap<(usize, String), Option<(u64, u64)>> = HashMap::new();
        let mut histories: HashMap<(usize, String), u64> = HashMap::new();
        for (d, p) in self.pollers.iter_mut().enumerate() {
            let Some(p) = p else { continue };
            for (id, f) in &mut p.flows {
                let head = match f.poll() {
                    Poll::Head(h) => Some(h),
                    Poll::Opening | Poll::Invalid | Poll::Absent(_) => None,
                };
                self.openers.reap(f.take_released());
                let limit = prune_limit(
                    &self.config,
                    &self.timing,
                    f.commit_batch_hint().unwrap_or(0),
                );
                if let Some(history) = f.history() {
                    histories.insert((d, id.clone()), history);
                }
                heads.insert((d, id.clone()), head.map(|h| (h, limit)));
            }
            let flows = &p.flows;
            (p.releases).retain(|id, r| r.pending() != 0 || flows.contains_key(id));
        }
        for slot in 0..self.slots.len() {
            let s = &self.slots[slot];
            if !matches!(s.state, SupState::Pruned { .. }) {
                continue;
            }
            let key = (self.input_domain(slot), s.flow.clone());
            if let (Some(&Some((head, limit))), Some(&history)) =
                (heads.get(&key), histories.get(&key))
            {
                self.clock_gap(slot, head, at, history, limit);
            }
        }
        let mut readable = vec![None; self.slots.len()];
        for (slot, vote) in readable.iter_mut().enumerate() {
            let d = self.input_domain(slot);
            let s = &mut self.slots[slot];
            *vote = liveness_vote(&s.state, false);
            let polled = heads.get(&(d, s.flow.clone())).copied().flatten();
            let SupState::Pruned { recovery: r, .. } = &mut s.state else {
                continue;
            };
            let (head, limit) = (polled.map(|p| p.0), polled.map_or(0, |p| p.1));
            let offset = self.config.inputs[slot].offset_samples;
            // The head must be in reach at every poll of the recovery
            // period, not only at its end: one that only touches it, as a
            // head hovering at the margin does, would be late again and
            // pruned again.
            let reach = |h| within_reach(h, at, self.timing.block, self.timing.margin, offset);
            if !head.is_some_and(reach) {
                *r = Recovery::default();
            }
            if !head.is_some_and(reach) || !r.update(&self.api, head, now, limit) {
                s.reported = false;
            } else if s.up {
                // Up in the log, so it is read again at once: the blocks
                // it was silent in were a degradation of this engine.
                let name = &self.config.inputs[slot].name;
                log(format_args!("input {name} advancing again, re-adding"));
                self.release_poll(slot);
                self.slots[slot].state = SupState::Wanted;
                *vote = liveness_vote(&self.slots[slot].state, true);
                self.dirty[d] = true;
            } else {
                *vote = liveness_vote(&s.state, true);
                if !s.reported && self.report(Report::InputReadable { slot }) {
                    let name = &self.config.inputs[slot].name;
                    log(format_args!("input {name} advancing again, reported"));
                    self.slots[slot].reported = true;
                }
            }
        }
        let mut st = self.status();
        for (input, readable) in st.inputs.iter_mut().zip(readable) {
            input.readable = readable;
        }
    }

    /// Builds a new generation of input domain `d` with every attached and
    /// wanted slot and hands it to the RT thread: starts its draft, then,
    /// on later ticks, builds it once its readers are open, leaving out
    /// those not open after [`REBUILD_WAIT`]. Waits while a
    /// generation-needing entry for `d` is queued, whose generation holds
    /// the domain's slots and would replace this one at its index: until
    /// the RT thread reaches its index or hands it back. A slot of `d`
    /// pruned meanwhile stays in the live generation until then, silent.
    fn rebuild(&mut self, d: usize) {
        if self.jobs.iter().any(|j| j.domain == d) {
            return;
        }
        let produced = self.counters.produced.load(Acquire);
        let Some(draft) = self.rebuilding[d].take() else {
            let plan = self.plan(d, None, produced, std::iter::empty()).0;
            self.rebuilding[d] = Some(self.start_draft(d, &plan));
            self.dirty[d] = false;
            return;
        };
        let due = draft.started.elapsed() >= REBUILD_WAIT;
        let (generation, held, opening) = match self.assemble(draft, d, None, produced, due) {
            Assembled::Gen(g, held, opening) => (g, held, opening),
            Assembled::Wait(draft) => {
                self.rebuilding[d] = Some(draft);
                return;
            }
        };
        let command = Command::Generation {
            domain: d,
            generation,
        };
        if self.cmd.push(command).is_ok() {
            self.adopt(d, held, 0, opening);
            self.rejoin();
        } else {
            self.dirty[d] = true;
        }
    }

    /// The readers a generation of input domain `d` takes at index `at`,
    /// for `job` or, without one, as a rebuild, once the jobs `ahead` of it
    /// were applied: every attached and wanted slot of `d`, the outgoing
    /// flow of a re-route or detach until its crossfade ends, and the
    /// change of `job`. A detached slot gets no reader, raised or not,
    /// until a re-route. Also returns the slots `job` moves to another
    /// flow, by bit.
    fn plan<'a>(
        &self,
        d: usize,
        job: Option<&Job>,
        at: u64,
        ahead: impl Iterator<Item = &'a Job>,
    ) -> (Plan, u64) {
        struct View {
            flow: String,
            detached: bool,
            pruned: bool,
            routing: Option<(u64, String)>,
        }
        let mut view: Vec<View> = (self.slots.iter())
            .map(|s| View {
                flow: s.flow.clone(),
                detached: matches!(s.state, SupState::Detached),
                pruned: matches!(s.state, SupState::Pruned { .. }),
                routing: s.routing.clone(),
            })
            .collect();
        // As `generation_done` and `adopt` leave the slots once each job
        // ahead is swapped in.
        for a in ahead {
            for slot in slots_of(a.up) {
                view[slot].pruned = false;
            }
            for (slot, flow) in &a.routes {
                let v = &mut view[*slot];
                let end = a.at_index + self.ramp;
                match flow {
                    None if !v.detached => {
                        v.routing = (!v.pruned).then(|| (end, v.flow.clone()));
                        v.detached = true;
                    }
                    None => {}
                    Some(f) if v.detached => {
                        (v.detached, v.pruned, v.routing) = (false, false, None);
                        v.flow = f.clone();
                    }
                    Some(f) => {
                        if *f != v.flow {
                            v.routing = Some((end, std::mem::replace(&mut v.flow, f.clone())));
                        }
                        v.pruned = false;
                    }
                }
            }
        }
        let mut rerouted = 0;
        let mut plan = Vec::new();
        for (slot, v) in view.iter().enumerate() {
            if self.input_domain(slot) != d {
                continue;
            }
            // The job's re-route of the slot: to a flow, or none to detach.
            let route =
                (job.and_then(|j| j.routes.iter().find(|r| r.0 == slot))).map(|r| r.1.as_deref());
            let target = route.is_some() || job.is_some_and(|j| j.up & bit(slot) != 0);
            // The flow the slot reads, none detached.
            let reads = (!v.detached).then_some(v.flow.as_str());
            let wanted = !v.pruned && !v.detached;
            // The flow a re-route leaves is read until its crossfade ends.
            let outgoing = match (route, &v.routing) {
                (Some(f), _) if f != reads && wanted => Some(v.flow.clone()),
                (None, Some((end, old))) if at < *end => Some(old.clone()),
                _ => None,
            };
            if route.is_some_and(|f| f != reads) {
                rerouted |= bit(slot);
            }
            let attach = (wanted || target)
                .then(|| route.unwrap_or(reads).map(str::to_owned))
                .flatten();
            plan.push((slot, outgoing, attach));
        }
        (plan, rerouted)
    }

    /// Starts a draft of a generation of input domain `d` taking the
    /// readers of `plan`: its instance, and the opens of its readers on the
    /// openers, the flows a slot reads now first, so that a new flow's
    /// open, which may wait, holds up no other.
    fn start_draft(&mut self, d: usize, plan: &Plan) -> Draft {
        // No instance here: each reader brings its own, created on an
        // opener.
        let generation = Generation::bare(self.slots.len());
        self.next_draft += 1;
        let now = Instant::now();
        let mut draft = Draft {
            id: self.next_draft,
            generation,
            wants: Vec::new(),
            opens: Vec::new(),
            opened: Vec::new(),
            started: now,
        };
        self.extend(&mut draft, d, wants(plan));
        draft
    }

    /// Hands the opens of `wants` for `draft`, of input domain `d`, to the
    /// openers, the flows a slot reads now first, so that a new flow's
    /// open, which may wait, holds up no other.
    fn extend(&mut self, draft: &mut Draft, d: usize, mut wants: Vec<Want>) {
        wants.retain(|w| self.shared(w).is_none());
        if wants.is_empty() {
            return;
        }
        let read: HashSet<&str> = (self.slots.iter())
            .filter(|s| matches!(s.state, SupState::Attached))
            .map(|s| s.flow.as_str())
            .collect();
        let mut tasks: Vec<OpenTask> = (wants.iter())
            .map(|w| self.open_task(draft.id, d, w))
            .collect();
        tasks.sort_by_key(|t| !read.contains(t.want.flow.as_str()));
        drop(read);
        self.openers.submit(tasks);
        draft.wants.extend(wants);
    }

    /// The task opening `w`'s reader for draft `id`, of input domain `d`.
    fn open_task(&self, id: u64, d: usize, w: &Want) -> OpenTask {
        let path = &self.config.domains[self.in_domains[d]].path;
        let cfg = &self.config.inputs[w.slot];
        OpenTask {
            draft: id,
            api: Arc::clone(&self.api),
            domain: path.clone(),
            dir: lookup::flow_dir(path, &w.flow),
            want: w.clone(),
            channels: cfg.channels,
            flow_channels: cfg.flow_channels.clone(),
            offset: cfg.offset_samples,
        }
    }

    /// Drops `draft`, its opens not started taken off the queue; those
    /// running release what they open when it comes back.
    fn drop_draft(&mut self, draft: Draft) {
        self.openers.cancel(draft.id);
        self.openers.reap(draft);
    }

    /// The generation of `draft` for `job`, none for a rebuild, swapped in
    /// at index `at`, from the readers its openers opened, with the slots it
    /// holds and those it left out for a reader not open yet, by bit. A
    /// reader the generation now wants and the draft lacks, as for a slot
    /// added back since it started, is opened for it. It waits while a
    /// reader is not answered; `due`, it waits for none and leaves out
    /// every reader not open, its slot pruned. Readers still opening are kept until the RT thread
    /// hands the generation back ([`Late`]). A build that left a slot out
    /// is not counted in the build times; the others count from the
    /// draft's start to its last answer.
    fn assemble(
        &mut self,
        mut draft: Draft,
        d: usize,
        job: Option<&Job>,
        at: u64,
        due: bool,
    ) -> Assembled {
        let assembling = Instant::now();
        let (plan, mut rerouted) = self.plan(d, job, at, std::iter::empty());
        let wanted = wants(&plan);
        let new: Vec<Want> = (wanted.iter())
            .filter(|w| !draft.wants.contains(w))
            .cloned()
            .collect();
        self.extend(&mut draft, d, new);
        let waiting = |w: &Want| {
            !due && !draft.opened.iter().any(|o| o.want == *w) && self.shared(w).is_none()
        };
        if wanted.iter().any(waiting) {
            return Assembled::Wait(draft);
        }
        if job.is_none() {
            rerouted |= std::mem::take(&mut self.reroute[d]);
        }
        let n = self.slots.len();
        let mut prune_after = vec![u64::MAX; n];
        let mut prune_late = vec![u64::MAX; n];
        let mut held = 0u64;
        let mut opening = 0u64;
        let mut complete = true;
        let mut opened = std::mem::take(&mut draft.opened);
        let mut generation = draft.generation;
        for w in &wanted {
            let found = match opened.iter().position(|o| o.want == *w) {
                Some(i) => Some(opened.swap_remove(i).result.map(|(i, s)| (Arc::new(i), s))),
                // The live generation's reader: its margin was logged when
                // it was opened.
                None => self.shared(w).map(|i| Ok((i, None))),
            };
            let result = match found {
                Some(r) => r.and_then(|(input, short)| {
                    (generation.insert(w.slot, input, w.outgoing))
                        .map(|()| short)
                        .map_err(|e| e.to_string())
                }),
                None => {
                    if !w.outgoing {
                        opening |= bit(w.slot);
                    }
                    let waited = self.openers.running_for(w);
                    if !w.outgoing {
                        let mut st = self.status();
                        let l = &mut st.generation.left_out;
                        if l.len() == 16 {
                            l.remove(0);
                        }
                        l.push((w.slot, at, waited.map(|d| d.as_nanos() as u64)));
                    }
                    Err(format!(
                        "flow {}: its reader was not open in time for index {at}, its open \
                         running for {waited:?}",
                        w.flow
                    ))
                }
            };
            if w.outgoing {
                if let Err(e) = result {
                    let name = &self.config.inputs[w.slot].name;
                    log(format_args!("input {name}: flow {} left: {e}", w.flow));
                }
                continue;
            }
            match result {
                Ok(short) => {
                    held |= 1 << w.slot;
                    (prune_after[w.slot], prune_late[w.slot]) =
                        self.attached(w.slot, short, &generation);
                }
                Err(message) => {
                    complete = false;
                    self.not_attached(w.slot, message);
                }
            }
        }
        // Readers the generation no longer wants, each on an instance of
        // its own that nothing else holds.
        self.openers.reap(opened);
        self.remember(d, &generation, &wanted);
        if complete {
            self.record_build(
                true,
                build_sample(&draft.opens, assembling.elapsed()),
                false,
            );
        }
        Assembled::Gen(
            Box::new(Gen {
                mxl: generation,
                prune_after,
                prune_late,
                rerouted,
            }),
            held,
            opening,
        )
    }

    /// Drops the kept readers whose slot and flow `gone` names, releasing
    /// them on the reaper thread.
    fn forget(&mut self, gone: impl Fn(&(usize, String)) -> bool) {
        let keys: Vec<(usize, String)> = self.readers.keys().filter(|k| gone(k)).cloned().collect();
        let dropped: Vec<Arc<Input>> = (keys.iter())
            .filter_map(|k| self.readers.remove(k))
            .collect();
        self.openers.reap(dropped);
    }

    /// The reader of the generations built last for `w`'s slot and flow.
    fn shared(&self, w: &Want) -> Option<Arc<Input>> {
        self.readers.get(&(w.slot, w.flow.clone())).cloned()
    }

    /// Keeps the readers `generation`, of input domain `d`, holds for
    /// `wanted`, for the next one, in place of those of the domain's slots
    /// kept before.
    fn remember(&mut self, d: usize, generation: &Generation, wanted: &[Want]) {
        let in_d: Vec<bool> = (0..self.slots.len())
            .map(|s| self.input_domain(s) == d)
            .collect();
        self.forget(|k| in_d[k.0]);
        for w in wanted {
            if let Some(input) = generation.shared(w.slot, w.outgoing) {
                self.readers.insert((w.slot, w.flow.clone()), input);
            }
        }
    }

    /// [`due`] now, for an entry at `at_index`.
    fn due(&self, at_index: u64) -> bool {
        due(
            earliest(&self.api, &self.counters, self.timing.margin, self.lead),
            at_index,
        )
    }

    /// Takes the readers the openers sent back to their drafts; one for a
    /// draft built meanwhile is kept until the RT thread hands its
    /// generation back, and one for a draft dropped is released.
    fn opened(&mut self) {
        let arrived = std::mem::take(&mut self.arrived);
        let mut late_builds = Vec::new();
        let mut unused = Vec::new();
        for o in arrived.into_iter().chain(self.openers.results.try_iter()) {
            let mut drafts = (self.jobs.iter_mut())
                .filter_map(|j| j.draft.as_mut())
                .chain(self.rebuilding.iter_mut().flatten());
            let same = |w: &Want| w.slot == o.want.slot && w.flow == o.want.flow;
            // Its own draft's, or adopted by a draft that wants the same
            // reader and has none yet, as after the draft that asked was
            // dropped: its own open is not queued behind this one.
            let taker = drafts.find(|d| {
                let answered = |w: &Want| d.opened.iter().any(|x| x.want == *w);
                if d.id == o.draft {
                    !answered(&o.want)
                } else {
                    o.result.is_ok() && d.wants.iter().any(|w| same(w) && !answered(w))
                }
            });
            if let Some(d) = taker {
                let want = if d.id == o.draft {
                    o.want.clone()
                } else {
                    let answered = |w: &Want| d.opened.iter().any(|x| x.want == *w);
                    let w = (d.wants.iter())
                        .find(|w| same(w) && !answered(w))
                        .expect("found above")
                        .clone();
                    self.openers.cancel_want(d.id, &w);
                    w
                };
                if o.result.is_ok() {
                    d.opens.push(o.took);
                }
                d.opened.push(Opened {
                    draft: d.id,
                    want,
                    ..o
                });
                continue;
            }
            let slot = o.want.slot;
            let awaited = !o.want.outgoing && self.awaiting & bit(slot) != 0 && {
                let to =
                    |r: &(usize, Option<String>)| r.0 == slot && r.1.as_ref() == Some(&o.want.flow);
                self.slots[slot].flow == o.want.flow
                    || self.jobs.iter().any(|j| j.routes.iter().any(to))
            };
            let input = match o.result {
                Ok((input, _)) => input,
                Err(_) => continue,
            };
            if awaited {
                // An open the hand-over did not wait for, bounded by the
                // time assumed before any build, so that a flow found only
                // after seconds does not set buildLead for long.
                late_builds.push(o.took.min(UNMEASURED_BUILD));
                let input = Arc::new(input);
                self.rejoins.push((o.want.slot, o.want.flow, input));
            } else {
                unused.push(input);
            }
        }
        self.openers.reap(unused);
        for took in late_builds {
            self.record_build(false, Some(took), true);
        }
        self.rejoin();
    }

    /// Adds back each slot whose reader opened after its generation left
    /// it out, once that generation is swapped in and the slot pruned for
    /// it, without the recovery period a stalled flow waits: it joins
    /// through the next build. One routed elsewhere meanwhile, or down in
    /// the log, is left to the poll.
    fn rejoin(&mut self) {
        let mut unused = Vec::new();
        for (slot, flow, input) in std::mem::take(&mut self.rejoins) {
            let routed = (self.jobs.iter()).any(|j| {
                (j.routes.iter()).any(|r| r.0 == slot && r.1.as_deref() == Some(flow.as_str()))
            });
            // The slot's job is still to be swapped in and adopted, which
            // marks it left out ([`Supervisor::adopt`]).
            let d = self.input_domain(slot);
            let queued = self.jobs.iter().any(|j| j.domain == d);
            let s = &self.slots[slot];
            // Its generation is not swapped in yet: kept while an entry
            // routes the slot to the flow.
            if queued || s.flow != flow || !matches!(s.state, SupState::Pruned { .. }) {
                if routed || queued {
                    self.rejoins.push((slot, flow, input));
                } else {
                    unused.push(input);
                }
                continue;
            }
            if !s.up || self.awaiting & bit(slot) == 0 {
                unused.push(input);
                continue;
            }
            self.awaiting &= !bit(slot);
            let name = &self.config.inputs[slot].name;
            let produced = self.counters.produced.load(Acquire);
            log(format_args!(
                "input {name}: its reader of {flow} opened, re-adding after index {produced}"
            ));
            self.release_poll(slot);
            self.slots[slot].state = SupState::Wanted;
            self.status().inputs[slot].readable = None;
            // The next build takes this reader rather than opening anew.
            self.readers.insert((slot, flow), input);
            let d = self.input_domain(slot);
            self.dirty[d] = true;
        }
        self.openers.reap(unused);
    }

    /// Releases what the RT thread handed back: a generation on the
    /// reaper thread, as releasing its readers' instances can wait in the
    /// kernel.
    fn take_back(&mut self, r: Returned) {
        match r {
            Returned::Generation(g) => self.openers.reap(g),
            r => release(r),
        }
    }

    /// Drafts every job not built yet anew, after the jobs ahead of it
    /// changed.
    fn redraft(&mut self) {
        for i in 0..self.jobs.len() {
            if !matches!(self.jobs[i].stage, Stage::Waiting) {
                continue;
            }
            if let Some(draft) = self.jobs[i].draft.take() {
                self.drop_draft(draft);
            }
            let j = &self.jobs[i];
            let (domain, at) = (j.domain, j.at_index);
            let plan = self.plan(domain, Some(j), at, self.jobs.iter().take(i)).0;
            self.jobs[i].draft = Some(self.start_draft(domain, &plan));
        }
    }

    /// Drops the jobs `gone` names and drafts those after them anew.
    fn drop_jobs(&mut self, gone: impl Fn(&Job) -> bool) {
        let (dropped, kept): (VecDeque<Job>, VecDeque<Job>) =
            std::mem::take(&mut self.jobs).into_iter().partition(&gone);
        self.jobs = kept;
        if dropped.is_empty() {
            return;
        }
        for j in &dropped {
            if let Some(draft) = &j.draft {
                self.openers.cancel(draft.id);
            }
        }
        // A job built and not sent holds its generation.
        self.openers.reap(dropped);
        self.redraft();
    }

    /// A generation of input domain `d`, with the slots it holds: every
    /// attached and wanted slot of `d`, the outgoing flow of a re-route
    /// or detach until its crossfade ended, and the change of `job`. A
    /// detached slot gets no reader, raised or not, until a re-route; a
    /// slot that cannot be attached is left out and its error put in its
    /// status.
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
        let mut prune_late = vec![u64::MAX; n];
        let mut held = 0u64;
        // The slots a product state moved to another flow, for the
        // domain's own generation only: a job's crossfades from its index.
        let mut rerouted = if job.is_none() {
            std::mem::take(&mut self.reroute[d])
        } else {
            0
        };
        // Each slot's outgoing flow and the flow it is to read, looked up
        // together before libmxl opens any of them.
        let mut plan: Vec<(usize, Option<String>, Option<String>)> = Vec::new();
        for slot in 0..n {
            if self.input_domain(slot) != d {
                continue;
            }
            // The job's re-route of the slot: to a flow, or none to detach.
            let route =
                (job.and_then(|j| j.routes.iter().find(|r| r.0 == slot))).map(|r| r.1.as_deref());
            let target = route.is_some() || job.is_some_and(|j| j.up & bit(slot) != 0);
            let current = self.slots[slot].flow.clone();
            let state = &self.slots[slot].state;
            // The flow the slot reads, none detached.
            let reads = (!matches!(state, SupState::Detached)).then_some(current.as_str());
            let wanted = !matches!(state, SupState::Pruned { .. } | SupState::Detached);
            // The flow a re-route leaves is read until its crossfade ends.
            let outgoing = match (route, &self.slots[slot].routing) {
                (Some(f), _) if f != reads && wanted => Some(current.clone()),
                (None, Some((end, old))) if produced < *end => Some(old.clone()),
                _ => None,
            };
            if route.is_some_and(|f| f != reads) {
                rerouted |= bit(slot);
            }
            let attach = (wanted || target)
                .then(|| route.unwrap_or(reads).map(str::to_owned))
                .flatten();
            plan.push((slot, outgoing, attach));
        }
        let dirs: Vec<PathBuf> = plan
            .iter()
            .flat_map(|(_, outgoing, attach)| outgoing.iter().chain(attach))
            .map(|id| lookup::flow_dir(&path, id))
            .collect();
        let mut found = self.gate.found(&dirs, GATE_WAIT).into_iter();
        let mut complete = true;
        for (slot, outgoing, attach) in plan {
            let cfg = &self.config.inputs[slot];
            if let Some(old) = outgoing {
                let result = match found.next() {
                    Some(Found::Yes) => generation
                        .attach_outgoing(slot, &self.config.input_config(cfg, &old), &self.timing)
                        .map_err(|e| e.to_string()),
                    f => Err(unopened(&old, f.unwrap_or(Found::No))),
                };
                if let Err(e) = result {
                    log(format_args!("input {}: flow {old} left: {e}", cfg.name));
                }
            }
            let Some(flow_id) = attach else {
                continue;
            };
            let result = match found.next() {
                Some(Found::Yes) => generation
                    .attach(slot, &self.config.input_config(cfg, &flow_id), &self.timing)
                    .map_err(|e| e.to_string()),
                f => Err(unopened(&flow_id, f.unwrap_or(Found::No))),
            };
            match result {
                Ok(short) => {
                    held |= 1 << slot;
                    (prune_after[slot], prune_late[slot]) = self.attached(slot, short, &generation);
                }
                Err(message) => {
                    complete = false;
                    self.not_attached(slot, message);
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
        // A build that left a slot out waited for a lookup, or failed an
        // attach: not the time a generation takes to build.
        self.record_build(
            complete,
            complete.then(|| started.elapsed().min(UNMEASURED_BUILD)),
            false,
        );
        let generation = Box::new(Gen {
            mxl: generation,
            prune_after,
            prune_late,
            rerouted,
        });
        Some((generation, held))
    }

    /// Slot `slot` was attached in `generation`, its producer needing the
    /// margin `short` when the configured one is below it: its error is
    /// cleared. Returns its frozen-head and late prune limits.
    fn attached(&mut self, slot: usize, short: Option<u64>, generation: &Generation) -> (u64, u64) {
        let hint = generation
            .input(slot)
            .and_then(|i| i.reader().config().ok())
            .map_or(0, |c| u64::from(c.common.maxCommitBatchSizeHint));
        let after = prune_limit(&self.config, &self.timing, hint);
        if let Some(needed) = short {
            log(format_args!(
                "input {}: readerMarginSamples {} is below the {needed} its \
                 producer's maxCommitBatchSizeHint needs",
                self.config.inputs[slot].name, self.timing.margin
            ));
        }
        let gap = self.slots[slot].gap.off;
        let mut st = self.status();
        st.inputs[slot].error = gap.map(|g| off_clock(g, self.config.sample_rate));
        st.inputs[slot].margin_short = short;
        (after, late_prune_limit(&self.config, after))
    }

    /// Slot `slot` was left out of a generation for `message`.
    fn not_attached(&mut self, slot: usize, message: String) {
        let name = &self.config.inputs[slot].name;
        log(format_args!("input {name} not attached: {message}"));
        self.status().inputs[slot].error = Some(message);
    }

    /// Observes `slot`'s head ([`ClockGap::observe`]) and, when its report
    /// changes, logs it once and sets the input's error, or clears the
    /// error it set.
    fn clock_gap(&mut self, slot: usize, head: u64, at: u64, history: u64, limit: u64) {
        let input = &self.config.inputs[slot];
        let slack = self.timing.margin + input.offset_samples.unsigned_abs();
        let Some(change) = (self.slots[slot].gap).observe(head, at, history, slack, limit) else {
            return;
        };
        let rate = self.config.sample_rate;
        let name = &self.config.inputs[slot].name;
        let mut st = self.status();
        let error = &mut st.inputs[slot].error;
        match change {
            GapChange::Off(gap) => {
                let message = off_clock(gap, rate);
                log(format_args!("input {name}: {message}"));
                *error = Some(message);
            }
            GapChange::Back(gap) => {
                log(format_args!(
                    "input {name}: head no longer off the MXL clock"
                ));
                if error.as_deref() == Some(off_clock(gap, rate).as_str()) {
                    *error = None;
                }
            }
        }
    }

    /// Starts `slot`'s [`ClockGap`] anew, as for another flow or none,
    /// clearing the input's error when it is the gap's.
    fn reset_gap(&mut self, slot: usize) {
        let Some(gap) = std::mem::take(&mut self.slots[slot].gap).off else {
            return;
        };
        let message = off_clock(gap, self.config.sample_rate);
        let mut st = self.status();
        let error = &mut st.inputs[slot].error;
        if error.as_deref() == Some(message.as_str()) {
            *error = None;
        }
    }

    /// Counts a generation `built` whole, adds a build time `took` to the
    /// window, aging out with `ages` ([`BuildTimes::add`]), and sets
    /// buildLead from the build times.
    fn record_build(&mut self, built: bool, took: Option<Duration>, ages: bool) {
        self.builds += u64::from(built);
        let now = Instant::now();
        if let Some(t) = took {
            self.build_times
                .add(t.as_nanos() as u64, ages.then_some(now));
        }
        self.build_times.expire(now);
        let p = self.build_times.p9999();
        let lead = build_lead_samples(p, self.lead, self.timing.block);
        self.build_lead.store(lead, Relaxed);
        self.build_time.store(build_time_samples(p), Relaxed);
        let mut st = self.status();
        st.generation.builds = self.builds;
        st.generation.build_p9999_ns = p;
        st.generation.build_lead = lead;
    }

    /// Takes the slots of input domain `d` that the generation now live
    /// on the RT thread holds, `held`, as attached, and moves the others
    /// that were attached or wanted to polling, reported. `targets` are
    /// the slots, by bit, a log entry raises or re-routes: each reported
    /// when not held, but for those in `opening`, left out for a reader
    /// still opening. A held slot that was pruned meanwhile, or is down in
    /// the log, is left out of the next generation instead. A detached
    /// slot stays as it is.
    fn adopt(&mut self, d: usize, held: u64, targets: u64, opening: u64) {
        for slot in 0..self.slots.len() {
            if self.input_domain(slot) != d || matches!(self.slots[slot].state, SupState::Detached)
            {
                continue;
            }
            let pruned = matches!(self.slots[slot].state, SupState::Pruned { .. });
            if held & (1 << slot) == 0 {
                // A slot whose reader is still opening is not reported: it
                // joins once the reader is open ([`Supervisor::rejoin`]).
                let quiet = opening & bit(slot) != 0;
                if quiet {
                    self.awaiting |= bit(slot);
                }
                if !pruned {
                    self.start_polling(slot, !quiet);
                } else if targets & bit(slot) != 0 && !quiet {
                    self.slots[slot].reported = false;
                    self.report(Report::InputPruned { slot });
                    // Raised or re-routed again: looked up without a gap.
                    let id = &self.slots[slot].flow;
                    if let Some(p) = &mut self.pollers[d]
                        && let Some(f) = p.flows.get_mut(id)
                    {
                        f.want();
                    }
                }
                continue;
            }
            if (pruned && targets & bit(slot) == 0) || !self.slots[slot].up {
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
            let mut st = self.status();
            st.inputs[slot].state = InputState::Attached;
            st.inputs[slot].readable = liveness_vote(&self.slots[slot].state, false);
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
                    up,
                    routes,
                } => {
                    let targets = up | routes.iter().fold(0, |m, r| m | bit(r.0));
                    let Some(slot) = slots_of(targets).next() else {
                        continue;
                    };
                    let mut job = Job {
                        id,
                        at_index,
                        up,
                        routes,
                        domain: self.input_domain(slot),
                        stage: Stage::Waiting,
                        draft: None,
                        opening: 0,
                    };
                    // The job's generation holds the domain's slots: a
                    // rebuild of it waits until the job is done.
                    if let Some(draft) = self.rebuilding[job.domain].take() {
                        self.drop_draft(draft);
                        self.dirty[job.domain] = true;
                    }
                    let plan = self
                        .plan(job.domain, Some(&job), at_index, self.jobs.iter())
                        .0;
                    job.draft = Some(self.start_draft(job.domain, &plan));
                    self.jobs.push_back(job);
                }
                LogMsg::Truncate { from } => {
                    self.drop_jobs(|j| j.id >= from && !matches!(j.stage, Stage::Sent(_)));
                }
                LogMsg::Skip { id } => {
                    self.drop_jobs(|j| j.id == id && !matches!(j.stage, Stage::Sent(_)));
                }
                // A generation sent for one is handed back unused by the
                // next one sent.
                LogMsg::Dropped { upto } => self.drop_jobs(|j| j.id <= upto),
                LogMsg::Product {
                    up,
                    flows,
                    leaving,
                    done,
                } => self.product(&up, flows, &leaving, done),
            }
        }
    }

    /// Builds the first job's generation, on top of the one live, from
    /// the readers its draft opened since its append, once they are all
    /// open or, at the latest, on the last tick from which it still reaches
    /// the RT thread before its index ([`Supervisor::due`]), leaving out a
    /// reader not open by then; and hands it to the RT thread once its
    /// entry is committed, counting a hand-over past the margin
    /// ([`past_margin`]). The next job waits until the RT thread is done
    /// with this one, so it holds at most one pending generation.
    fn jobs(&mut self) {
        if let Some(mut j) = self.jobs.pop_front() {
            if matches!(j.stage, Stage::Waiting) {
                if j.draft.is_none() {
                    let plan = self
                        .plan(j.domain, Some(&j), j.at_index, std::iter::empty())
                        .0;
                    j.draft = Some(self.start_draft(j.domain, &plan));
                }
                if let Some(draft) = j.draft.take() {
                    let due = self.due(j.at_index);
                    match self.assemble(draft, j.domain, Some(&j), j.at_index, due) {
                        Assembled::Gen(g, held, opening) => {
                            j.stage = Stage::Built(g, held);
                            j.opening = opening;
                        }
                        Assembled::Wait(draft) => j.draft = Some(draft),
                    }
                }
            }
            let committed = self.log.lock().expect("log lock").committed;
            let j = match j.stage {
                Stage::Built(generation, held) if j.id <= committed && self.cmd.slots() > 0 => {
                    let command = Command::Pending {
                        id: j.id,
                        domain: j.domain,
                        generation,
                    };
                    let earliest =
                        earliest(&self.api, &self.counters, self.timing.margin, self.lead);
                    if past_margin(earliest, j.at_index) {
                        self.counters.generation_past_margin.fetch_add(1, Relaxed);
                    }
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
            // The jobs after it were drafted on its re-routes.
            self.redraft();
            return;
        }
        let mut targets = j.up;
        for (slot, flow) in j.routes {
            targets |= bit(slot);
            let detached = matches!(self.slots[slot].state, SupState::Detached);
            let Some(flow) = flow else {
                if !detached {
                    let end = j.at_index + self.ramp;
                    self.release_poll(slot);
                    let s = &mut self.slots[slot];
                    s.routing = detach_routing(&s.state, end, &s.flow);
                    s.state = SupState::Detached;
                    s.reported = false;
                    s.prune_unsent = false;
                    self.status().inputs[slot].state = InputState::Detached;
                    self.reset_gap(slot);
                }
                continue;
            };
            if detached {
                // From silence: no flow is left.
                self.slots[slot].state = SupState::Wanted;
                self.slots[slot].routing = None;
            }
            if flow != self.slots[slot].flow {
                // A polled slot polls the flow it reads: the hold on the
                // old one is released, `poll` takes one on the new.
                self.release_poll(slot);
                if let SupState::Pruned { recovery, held } = &mut self.slots[slot].state {
                    *recovery = Recovery::default();
                    *held = false;
                }
                self.slots[slot].reported = false;
                let old = std::mem::replace(&mut self.slots[slot].flow, flow.clone());
                if !detached {
                    self.slots[slot].routing = Some((j.at_index + self.ramp, old));
                }
                self.status().inputs[slot].flow_id = flow;
                self.reset_gap(slot);
            }
        }
        self.adopt(j.domain, held, targets, j.opening);
        self.rejoin();
    }

    /// Takes the slots' state and flows of a product state: a slot on
    /// another flow, or attached again, moves to it at once, no
    /// crossfade, its reader state started anew in the generation's
    /// `rerouted`; a slot without a flow is detached at once. Every domain's
    /// generation is built anew on them and handed to the RT thread, and
    /// so is the generation a job built or sent before them, which read
    /// the old flows; `done` is sent once all are handed over
    /// ([`Supervisor::hand_product`]).
    fn product(
        &mut self,
        up: &[bool],
        flows: Vec<Option<String>>,
        leaving: &[Option<(u64, String)>],
        done: SyncSender<()>,
    ) {
        for (slot, flow) in flows.into_iter().enumerate() {
            self.slots[slot].up = up[slot];
            self.status().inputs[slot].up = up[slot];
            // The flow a crossfade still running leaves is read until it
            // ends, by the generations built for the install too.
            let routing = leaving.get(slot).cloned().flatten();
            let detached = matches!(self.slots[slot].state, SupState::Detached);
            if flow.as_ref().is_some_and(|f| *f == self.slots[slot].flow) && !detached
                || flow.is_none() && detached
            {
                self.slots[slot].routing = routing;
                continue;
            }
            self.release_poll(slot);
            if let SupState::Pruned { recovery, held } = &mut self.slots[slot].state {
                *recovery = Recovery::default();
                *held = false;
            }
            self.slots[slot].reported = false;
            self.slots[slot].routing = routing;
            self.reset_gap(slot);
            let Some(flow) = flow else {
                self.slots[slot].state = SupState::Detached;
                self.slots[slot].prune_unsent = false;
                self.status().inputs[slot].state = InputState::Detached;
                continue;
            };
            if detached {
                self.slots[slot].state = SupState::Wanted;
            }
            self.slots[slot].flow = flow.clone();
            self.status().inputs[slot].flow_id = flow;
            let d = self.input_domain(slot);
            self.reroute[d] |= bit(slot);
        }
        // Slots moved to other flows read them anew.
        self.forget(|_| true);
        // A generation built and not sent is built again when sent, its
        // readers opened anew on the new flows, and so is a rebuild's.
        for j in &mut self.jobs {
            if matches!(j.stage, Stage::Built(..)) {
                let built = std::mem::replace(&mut j.stage, Stage::Waiting);
                self.openers.reap(built);
            }
        }
        self.redraft();
        for d in 0..self.rebuilding.len() {
            if let Some(draft) = self.rebuilding[d].take() {
                self.drop_draft(draft);
                self.dirty[d] = true;
            }
        }
        let resend = self
            .jobs
            .front()
            .is_some_and(|j| matches!(j.stage, Stage::Sent(_)));
        self.product = Some(ProductHandOver {
            done,
            domains: vec![true; self.dirty.len()],
            resend,
        });
        self.hand_product();
    }

    /// Hands the generations a product state needs to the RT thread while
    /// the command ring has room, the rest at later ticks: each domain's,
    /// built on the slots' flows, and the front job's again when it was
    /// sent before, under its entry's id, which the RT thread takes in
    /// place of the one it holds. Then sends `done`.
    fn hand_product(&mut self) {
        let Some(mut p) = self.product.take() else {
            return;
        };
        for d in 0..p.domains.len() {
            if !p.domains[d] || self.cmd.slots() == 0 {
                continue;
            }
            let Some((generation, held)) = self.build(d, None) else {
                continue;
            };
            let command = Command::Generation {
                domain: d,
                generation,
            };
            if self.cmd.push(command).is_ok() {
                p.domains[d] = false;
                self.dirty[d] = false;
                self.adopt(d, held, 0, 0);
            }
        }
        if p.resend
            && self.cmd.slots() > 0
            && let Some(j) = self.jobs.pop_front()
        {
            if let Stage::Sent(_) = j.stage
                && let Some((generation, held)) = self.build(j.domain, Some(&j))
            {
                let command = Command::Pending {
                    id: j.id,
                    domain: j.domain,
                    generation,
                };
                if self.cmd.push(command).is_err() {
                    unreachable!("a free command slot was checked");
                }
                p.resend = false;
                self.jobs.push_front(Job {
                    stage: Stage::Sent(held),
                    ..j
                });
            } else {
                // Done with meanwhile, or not built: at the next tick.
                p.resend = matches!(j.stage, Stage::Sent(_));
                self.jobs.push_front(j);
            }
        }
        if p.domains.iter().any(|&d| d) || p.resend {
            self.product = Some(p);
        } else {
            let _ = p.done.try_send(());
        }
    }

    /// Puts the spent batches the RT thread leaked in the status, and
    /// logs a rise: the return ring and its spent list were full, the
    /// supervisor not taking returns for that long.
    fn leaks(&mut self) {
        let leaked = self.counters.batches_leaked.load(Relaxed);
        let mut st = self.status();
        if leaked > st.batches_leaked {
            st.batches_leaked = leaked;
            drop(st);
            log(format_args!(
                "{leaked} spent batches leaked: the return ring and the spent list were full"
            ));
        }
    }

    /// Sends `r` to the replicated log, or stamps it in the engine's own:
    /// a pruned input that is up goes down at the earliest index, a
    /// readable one that is down comes up at the earliest index a
    /// generation-needing entry takes, each after the slot's last entry
    /// and its ramp, and an `InputUp` buildLead after the log's last
    /// generation-needing entry. False when it did not go out: the channel
    /// was full or the log refused it. An `InputPruned` that did not go
    /// out is sent again by `poll` while it holds. Nothing goes out for a
    /// detached slot, counted as sent: the flow it read is not its input.
    fn report(&mut self, r: Report) -> bool {
        let (slot, up) = match r {
            Report::InputPruned { slot } => (slot, false),
            Report::InputReadable { slot } => (slot, true),
        };
        if matches!(self.slots[slot].state, SupState::Detached) {
            return true;
        }
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
            if log.append(e, None, Vec::new(), consumed).is_err() {
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
        // An attached slot's head, which a pruned one's poll reads.
        let s = &self.slots[slot];
        let reader = (self.readers.get(&(slot, s.flow.clone()))).map(|i| i.reader());
        if let (SupState::Attached, Some(head), Some(reader)) = (&s.state, stats.head, reader)
            && let (Ok(history), Ok(info)) = (reader.history(), reader.config())
        {
            let hint = u64::from(info.common.maxCommitBatchSizeHint);
            let limit = prune_limit(&self.config, &self.timing, hint);
            let at = time::timestamp_to_index(&self.api, stats.now);
            self.clock_gap(slot, head, at, history, limit);
        }
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

    /// Takes the outputs the RT thread released and the ones the log
    /// disabled or enabled into the status, and creates a writer for each
    /// enabled output without one, retried with a backoff while that
    /// fails, once no entry falls due soon ([`Supervisor::handing_over`]):
    /// creating the domain's output instance and the flow can stall the
    /// supervisor.
    fn outputs(&mut self) {
        let released = self.counters.outputs_released.swap(0, Acquire);
        if released != 0 {
            // Their writers are in the return ring: dropped, deleting
            // their flows, before a flow is created anew, so an output
            // enabled again does not find its own writer live on it.
            while let Ok(r) = self.ret.pop() {
                self.take_back(r);
            }
        }
        let disabled = self.counters.outputs_disabled.load(Acquire);
        let now = Instant::now();
        let mut handing_over = self.handing_over();
        let mut created = false;
        for o in 0..self.outs.len() {
            if std::mem::take(&mut created) {
                handing_over = self.handing_over();
            }
            let out = &mut self.outs[o];
            let enabled = disabled & bit(o) == 0;
            if released & bit(o) != 0 || enabled != out.enabled {
                let name = &self.config.outputs[o].name;
                let mut st = self.status.lock().expect("status lock");
                let s = &mut st.outputs[o];
                if released & bit(o) != 0 && out.realised {
                    out.realised = false;
                    s.realised = false;
                    log(format_args!("output {name} released"));
                }
                if enabled != out.enabled {
                    out.enabled = enabled;
                    out.next_try = now;
                    out.backoff = BACKOFF_FIRST;
                    s.enabled = enabled;
                    log(format_args!(
                        "output {name} {}",
                        if enabled { "enabled" } else { "disabled" }
                    ));
                }
                if !enabled {
                    s.reason = Some("disabled".to_owned());
                } else if !out.realised {
                    s.reason = Some("not created yet".to_owned());
                }
            }
            if !enabled
                || out.realised
                || now < out.next_try
                || self.cmd.slots() == 0
                || handing_over
            {
                continue;
            }
            let cfg = &self.config.outputs[o];
            let d = self.config.domain(&cfg.domain).expect("validated");
            // Checked again before each next output: a creation can take
            // its lookup's gate wait, and an entry may fall due meanwhile.
            created = true;
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
            // A flow whose directory is not found, or not yet, is created
            // without libmxl's check for a live writer, which looks the
            // missing name up and on a file system that waits for it would
            // hold the supervisor; a flow there meanwhile is opened, not
            // created, and reported raced.
            // After collecting a stale flow, the same lookup decides
            // whether it is still there.
            let dir = lookup::flow_dir(&self.config.domains[d].path, oc.flow_id);
            let gate = &mut self.out_gate;
            let mut found = || gate.found(std::slice::from_ref(&dir), GATE_WAIT)[0] == Found::Yes;
            let result = if found() {
                output::create_looking(instance, &oc, found)
            } else {
                output::create_new(instance, &oc)
            };
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
                    "another writer created the flow first, or a stale flow its lookup \
                     had not found yet was opened and removed; libmxl v1.1.0 leaves a \
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

/// The first fade of `entries`, applied at `at_index`, that an entry may
/// not carry ([`Fade::within`]).
fn fade_beyond(entries: &[Entry], at_index: u64) -> Option<Fade> {
    entries.iter().find_map(|e| match e {
        Entry::Gain { fade: Some(f), .. } if !f.within(at_index) => Some(*f),
        _ => None,
    })
}

/// Folds a gain entry the install passed into the targets and the fades
/// still running: the coefficient `c` goes to `to` by `fade`, joined
/// ([`Fade::joined`]), or over `ramp` samples, from its value at the
/// entry's index `at`, which the fade it was in counts into. Depths fold
/// in the same way, as values with no fade of their own.
fn fold_passed(
    gains: &mut [f32],
    fadings: &mut [Option<Fade>],
    c: usize,
    (to, fade): (f32, Option<Fade>),
    at: u64,
    ramp: u32,
) {
    let held = fadings[c].map_or(gains[c], |f| f.value_at(gains[c], at));
    fadings[c] = match fade {
        Some(f) => f.joined(to, held, at, ramp),
        None => (held != to && ramp != 0).then_some(Fade {
            from: held,
            start: at,
            len: ramp,
        }),
    };
    gains[c] = to;
}

/// The outputs disabled, a bit per output, `disabled` with output
/// `output` enabled or disabled.
fn output_disabled(disabled: u64, output: usize, enabled: bool) -> u64 {
    if enabled {
        disabled & !bit(output)
    } else {
        disabled | bit(output)
    }
}

/// Releases what the RT thread handed back: a generation's readers,
/// group and instance, or a writer, which deletes its flow.
fn release(r: Returned) {
    match r {
        Returned::Generation(g) => drop(g),
        Returned::Writer(w) => drop(w),
        Returned::Batch(b) => drop(b),
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
/// advanced at the nominal rate within 1 %. Only samples whose head is
/// known, above 0 and not behind the window's first head count, and a
/// margin beyond the samples the window spans is none: late blocks that
/// far behind a head advancing at the nominal rate are no reader margin
/// to widen.
fn margin_warning(api: &MxlApi, window: &VecDeque<SlotStats>, timing: &Timing) -> Option<u64> {
    let (first, last) = (first_head(window)?, window.back()?);
    let index = |now| time::timestamp_to_index(api, now);
    margin_needed(
        window,
        index(last.now).checked_sub(index(first.now))?,
        timing,
    )
}

/// The first sample with a head above 0: a slot not read yet has none.
fn first_head(window: &VecDeque<SlotStats>) -> Option<&SlotStats> {
    window.iter().find(|s| s.head.is_some_and(|h| h > 0))
}

/// [`margin_warning`] over a window whose clock advanced `nominal`
/// samples.
fn margin_needed(window: &VecDeque<SlotStats>, nominal: u64, timing: &Timing) -> Option<u64> {
    if window.len() < MARGIN_WINDOW {
        return None;
    }
    let (first, last) = (first_head(window)?, window.back()?);
    let late: u64 = window.iter().skip(1).map(|s| u64::from(s.late)).sum();
    let on_time: u64 = window.iter().skip(1).map(|s| u64::from(s.on_time)).sum();
    if late == 0 || on_time == 0 {
        return None;
    }
    let advanced = last.head?.checked_sub(first.head?)?;
    if advanced.abs_diff(nominal) * 100 > nominal {
        return None;
    }
    let start = first.head?;
    let min_slack = window
        .iter()
        .skip(1)
        .filter(|s| s.late > 0 && s.head.is_some_and(|h| h > 0 && h >= start))
        .filter_map(|s| s.min_slack)
        .min()?;
    let required = (timing.margin as i64).checked_sub(min_slack)?;
    let required = u64::try_from(required).ok()? + timing.block;
    (required <= nominal).then_some(required)
}

/// Minor page faults of thread `tid`, from `/proc/self/task/<tid>/stat`.
fn minor_faults(tid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")).ok()?;
    // Field 10, counted from the state field after the command name.
    stat.rsplit_once(") ")?.1.split(' ').nth(7)?.parse().ok()
}

#[cfg(test)]
mod golden;
mod product;

#[cfg(test)]
mod tests;
