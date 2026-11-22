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
    params.check()
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
    param.check(o.channels as usize)
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
    params.check()
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
        DuckSetting::Detector(ref p) => DuckParam::Detector(**p).check(),
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
        // Each flow's head and prune limit.
        let mut heads: HashMap<(usize, String), Option<(u64, u64)>> = HashMap::new();
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
                heads.insert((d, id.clone()), head.map(|h| (h, limit)));
            }
            let flows = &p.flows;
            (p.releases).retain(|id, r| r.pending() != 0 || flows.contains_key(id));
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
        let mut st = self.status();
        st.inputs[slot].error = None;
        st.inputs[slot].margin_short = short;
        (after, late_prune_limit(&self.config, after))
    }

    /// Slot `slot` was left out of a generation for `message`.
    fn not_attached(&mut self, slot: usize, message: String) {
        let name = &self.config.inputs[slot].name;
        log(format_args!("input {name} not attached: {message}"));
        self.status().inputs[slot].error = Some(message);
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
mod tests {
    use super::*;

    /// A config of two mono inputs, one output, column 0 ducked.
    fn ducked_config() -> Config {
        let v = serde_json::json!({
            "libmxl": "/nonexistent/libmxl.so",
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": (0..2).map(|n| serde_json::json!({
                "name": format!("in{n}"), "domain": "d",
                "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8d", "channels": 1 }))
                .collect::<Vec<_>>(),
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "7fbec3b1-1b0f-417d-9059-8b94a47197ed", "label": "o",
                          "group": "g", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0], [1.0]],
            "duckColumns": [0],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        Config::parse(&v.to_string()).unwrap()
    }

    /// Every duck setting's neutral form is one an engine takes, so a
    /// refused setting is replaced rather than left out; a refused
    /// finite one clamped is taken too, and one with a NaN is not.
    #[test]
    fn a_duck_settings_neutral_and_clamped_forms_are_taken() {
        let config = ducked_config();
        let mut refused = samplez_dsp::DetectorParams::DEFAULT;
        refused.attack_ms = 1e9;
        let settings = [
            DuckSetting::Detector(Box::new(refused)),
            DuckSetting::Keys(vec![(0, 1e9), (1, 1.0)]),
            DuckSetting::Depth { slot: 1, db: 99.0 },
        ];
        for s in &settings {
            assert!(check_duck(&config, 0, s).is_err(), "{s:?} refused");
            let clamped = s.clamped().expect("finite");
            assert_eq!(check_duck(&config, 0, &clamped), Ok(()), "{clamped:?}");
            assert_eq!(check_duck(&config, 0, &s.neutral()), Ok(()), "{s:?}");
        }
        assert_eq!(
            DuckSetting::Depth { slot: 1, db: 99.0 }.clamped(),
            Some(DuckSetting::Depth { slot: 1, db: 40.0 })
        );
        let nan = DuckSetting::Depth {
            slot: 1,
            db: f32::NAN,
        };
        assert_eq!(nan.clamped(), None);
        // A key of a slot the engine lacks stays refused clamped; its
        // neutral form, no key, is taken.
        let missing = DuckSetting::Keys(vec![(7, 1.0)]);
        assert!(check_duck(&config, 0, &missing.clamped().unwrap()).is_err());
        assert_eq!(check_duck(&config, 0, &missing.neutral()), Ok(()));
    }

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

    /// Detaching a pruned slot leaves no outgoing flow: its flow is not
    /// readable, so a reader attached for the crossfade would only fail.
    #[test]
    fn detaching_a_pruned_slot_keeps_no_outgoing_flow() {
        let pruned = SupState::Pruned {
            recovery: Recovery::default(),
            held: false,
        };
        assert_eq!(detach_routing(&pruned, 9, "f"), None);
        for state in [SupState::Attached, SupState::Wanted] {
            assert_eq!(detach_routing(&state, 9, "f"), Some((9, "f".to_owned())));
        }
    }

    /// A slot waiting for its generation, as after an `InputUp` or a
    /// `Route` while the build lags, has no say in input liveness: its
    /// flow is neither read nor known lost yet. Nor has a detached one.
    #[test]
    fn a_slot_waiting_for_its_generation_casts_no_liveness_vote() {
        let pruned = SupState::Pruned {
            recovery: Recovery::default(),
            held: false,
        };
        assert_eq!(liveness_vote(&SupState::Wanted, true), None);
        assert_eq!(liveness_vote(&SupState::Detached, true), None);
        assert_eq!(liveness_vote(&SupState::Attached, false), Some(true));
        assert_eq!(liveness_vote(&pruned, false), Some(false));
        assert_eq!(liveness_vote(&pruned, true), Some(true));
    }

    /// A 10 s window of stats, each sample 1 s of 48 kHz with a head
    /// advancing at the nominal rate, late blocks with `slack` in every
    /// sample after the first.
    fn margin_window(slack: impl Fn(usize) -> (Option<u64>, Option<i64>)) -> VecDeque<SlotStats> {
        (0..MARGIN_WINDOW)
            .map(|n| {
                let (head, min_slack) = slack(n);
                SlotStats {
                    late: u32::from(n > 0),
                    on_time: 100,
                    min_slack,
                    head,
                    now: n as u64,
                    ..crate::rt_loop::EMPTY_STATS
                }
            })
            .collect()
    }

    /// A head is in reach when it holds the newest block due, whose end
    /// lies the margin and up to a block short of it behind the clock: a
    /// positive offset reads later samples and needs a head that far
    /// further on, a negative one allows it that much less.
    #[test]
    fn a_head_is_in_reach_once_it_holds_the_newest_block_due() {
        let (block, margin) = (48, 960);
        // 20 samples past the deadline of the block ending at `due`.
        let due = 1_795_061_948 * block;
        let at = due + margin + 20;
        assert!(within_reach(due, at, block, margin, 0));
        assert!(!within_reach(due - 1, at, block, margin, 0));
        // At its deadline the block is due; one sample before, it is not.
        assert!(within_reach(due, due + margin, block, margin, 0));
        assert!(within_reach(
            due - block,
            due + margin - 1,
            block,
            margin,
            0
        ));
        assert!(!within_reach(
            due - block - 1,
            due + margin - 1,
            block,
            margin,
            0
        ));
        assert!(within_reach(due + 100, at, block, margin, 100));
        assert!(!within_reach(due + 99, at, block, margin, 100));
        assert!(within_reach(due - 100, at, block, margin, -100));
        assert!(!within_reach(due - 101, at, block, margin, -100));
        // A head 36.96 s behind, stamped on UTC rather than TAI.
        assert!(!within_reach(at - 1_774_242, at, block, margin, 0));
    }

    /// The margin warning names what the late blocks need, never an
    /// absolute index: a sample whose head was 0 or that fell behind the
    /// window's first head is left out, and a requirement beyond the
    /// samples the window spans is none.
    #[test]
    fn a_margin_warning_never_names_an_absolute_index() {
        let timing = Timing {
            block: 48,
            margin: 576,
            max_catchup_blocks: 4,
            resync_budget: 0,
        };
        let head0: u64 = 86_160_902_160_000;
        let head = |n: usize| head0 + n as u64 * 48_000;
        let nominal = 10 * 48_000;
        let w = margin_window(|n| (Some(head(n)), Some(-100)));
        assert_eq!(margin_needed(&w, nominal, &timing), Some(576 + 100 + 48));
        // One sample read its late blocks against a head of 0: its slack
        // is the negated absolute index.
        let w = margin_window(|n| match n {
            5 => (Some(head(n)), None),
            _ => (Some(head(n)), Some(-100)),
        });
        assert_eq!(margin_needed(&w, nominal, &timing), Some(724));
        let w = margin_window(|n| match n {
            5 => (Some(0), Some(-(head0 as i64))),
            _ => (Some(head(n)), Some(-100)),
        });
        assert_eq!(
            margin_needed(&w, nominal, &timing),
            Some(724),
            "a head of 0 left out"
        );
        let w = margin_window(|n| match n {
            5 => (Some(head0 - 1), Some(-(head0 as i64))),
            _ => (Some(head(n)), Some(-100)),
        });
        assert_eq!(
            margin_needed(&w, nominal, &timing),
            Some(724),
            "behind the start"
        );
        let w = margin_window(|n| (Some(head(n)), Some(-(head0 as i64))));
        assert_eq!(
            margin_needed(&w, nominal, &timing),
            None,
            "beyond the window"
        );
        // The first sample has no head yet: the window starts at the next.
        let w = margin_window(|n| match n {
            0 => (None, None),
            _ => (Some(head(n)), Some(-100)),
        });
        let nine = 9 * 48_000;
        assert_eq!(margin_needed(&w, nine, &timing), Some(724), "no first head");
    }

    #[test]
    fn a_deadline_leaves_a_tick_and_the_hand_over_margin() {
        // 5 ms and 2 ms at 48 kHz.
        assert_eq!(samples_of(SUPERVISOR_TICK), 240);
        assert_eq!(samples_of(HAND_OVER_MARGIN), 96);
        let at = 48_000;
        assert!(!due(at - 337, at));
        assert!(due(at - 336, at));
        // Checks are a tick apart at most: a deadline first seen on one,
        // not seen a tick before, leaves more than the margin.
        for first in at - 336..at - 96 {
            assert!(due(first, at) && !due(first - 240, at));
            assert!(first + samples_of(HAND_OVER_MARGIN) < at, "{first}");
        }
    }

    /// The window in which an entry holds back the calls that can stall
    /// the supervisor reaches a poll period and a tick past its deadline:
    /// an entry not due yet but due within them counts, one beyond them
    /// does not.
    #[test]
    fn an_entry_due_within_a_poll_period_and_a_tick_is_due_soon() {
        // 100 ms and 5 ms at 48 kHz, past the deadline's 336 samples.
        let at = 48_000;
        let edge = at - 336 - 5_040;
        assert!(!due(at - 337, at) && due_soon(at - 337, at));
        assert!(due_soon(edge, at));
        assert!(!due_soon(edge - 1, at));
    }

    /// A hand-over with less than the 2 ms margin left before its index
    /// is past the margin, one with the margin left is not.
    #[test]
    fn a_hand_over_with_less_than_the_margin_left_is_past_it() {
        let at = 48_000;
        assert!(!past_margin(at - 96, at));
        assert!(past_margin(at - 95, at));
        assert!(past_margin(at + 48, at));
    }

    #[test]
    fn builds_that_open_nothing_do_not_shrink_build_lead() {
        let us = Duration::from_micros(1);
        assert_eq!(build_sample(&[], us), None);
        assert_eq!(
            build_sample(&[Duration::from_secs(3)], us),
            Some(UNMEASURED_BUILD)
        );
        assert_eq!(
            build_sample(&[Duration::from_millis(2), us], us),
            Some(Duration::from_millis(2) + us)
        );
        let mut t = BuildTimes::new();
        let one = build_sample(&[Duration::from_millis(2)], us).unwrap();
        t.add(one.as_nanos() as u64, None);
        // A window of re-routes that share every reader.
        for _ in 0..2 * BUILD_WINDOW {
            if let Some(s) = build_sample(&[], us) {
                t.add(s.as_nanos() as u64, None);
            }
        }
        assert!(t.p9999().unwrap() >= 2_000_000);
    }

    #[test]
    fn a_slow_build_ages_out_after_five_minutes_without_builds() {
        let at = Instant::now();
        let mut t = BuildTimes::new();
        for _ in 0..10 {
            t.add(1_000_000, None);
        }
        let fast = t.p9999();
        // A late reader's open, counted once it opened, raises it ...
        t.add(80_000_000, Some(at));
        t.expire(at + Duration::from_secs(300));
        assert!(t.p9999().unwrap() >= 80_000_000);
        // ... until it is five minutes old, with no build since; the
        // complete builds' times stay.
        t.expire(at + Duration::from_secs(301));
        assert_eq!(t.p9999(), fast);
    }

    #[test]
    fn build_lead_covers_the_p9999_build_time() {
        let at = Instant::now();
        let mut t = BuildTimes::new();
        assert_eq!(t.p9999(), None);
        for _ in 0..BUILD_WINDOW {
            t.add(1_000_000, None);
        }
        let p = t.p9999().unwrap();
        assert!((1_000_000..1_190_000).contains(&p), "{p}");
        // One slow build raises it at once, as a late reader's open does.
        t.add(80_000_000, Some(at));
        assert!(t.p9999().unwrap() >= 80_000_000);
        let slow = build_lead_samples(t.p9999(), 192, 48);
        // It ages out after a window of fast builds, and only then.
        for _ in 0..BUILD_WINDOW - 1 {
            t.add(1_000_000, None);
        }
        assert!(t.p9999().unwrap() >= 80_000_000, "aged out too early");
        t.add(1_000_000, None);
        assert_eq!(t.p9999(), Some(p));
        assert!(build_lead_samples(t.p9999(), 192, 48) < slow);
        // 1 ms is below the 30 ms floor: 30 + 5 + 2 ms is 1776 samples,
        // plus a lead of 192: 1968.
        assert_eq!(build_lead_samples(Some(1_000_000), 192, 48), 1968);
        // Above the floor: 40 + 5 + 2 ms is 2256 samples, plus 192: 2448.
        assert_eq!(build_lead_samples(Some(40_000_000), 192, 48), 2448);
        assert_eq!(build_lead_samples(None, 0, 48), 5136);
        // Without the lead, not rounded: the floor + 5 ms + 2 ms, 100 ms +
        // 5 ms + 2 ms.
        assert_eq!(build_time_samples(Some(1_000_000)), 1776);
        assert_eq!(build_time_samples(None), 5136);
        // However short the builds, buildLead holds the lead, the floor,
        // the tick and the margin.
        let floor = 192
            + samples_of(BUILD_FLOOR)
            + samples_of(SUPERVISOR_TICK)
            + samples_of(HAND_OVER_MARGIN);
        for p in [0, 1, 1_000, 100_000] {
            assert!(build_lead_samples(Some(p), 192, 48) >= floor, "{p}");
        }
    }

    fn log() -> (Log, rtrb::Consumer<RingEntry>, mpsc::Receiver<LogMsg>) {
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
        log.append(e(1, down), None, Vec::new(), 0).unwrap();
        log.append(e(2, up), None, Vec::new(), 0).unwrap();
        assert!(matches!(msgs.try_recv(), Ok(LogMsg::Append { id: 2, .. })));
        assert_eq!(
            log.append(e(2, down), None, Vec::new(), 0),
            Err(EntryError::Order { id: 2, last: 2 })
        );
        assert!(rx.pop().is_err());
        log.commit(1);
        assert_eq!(rx.pop().unwrap().entry.id, 1);
        // A truncation at or below the commit point keeps what is
        // committed, and the generations of its entries.
        log.truncate(1);
        assert!(matches!(msgs.try_recv(), Ok(LogMsg::Truncate { from: 2 })));
        assert_eq!((log.last, log.uncommitted.len()), (1, 0));
        log.append(e(2, down), None, Vec::new(), 0).unwrap();
        log.commit(2);
        assert_eq!(rx.pop().unwrap().entry.id, 2);
        // The engine's own log refuses what the RT thread has no room for.
        log.local = Some(LocalLog {
            up: Vec::new(),
            last_at: Vec::new(),
            gen_at: 0,
        });
        for id in 3..=PENDING_CHANGES as u64 {
            log.append(e(id, down), None, Vec::new(), 0).unwrap();
        }
        assert_eq!(
            log.append(e(PENDING_CHANGES as u64 + 1, down), None, Vec::new(), 0),
            Err(EntryError::Full)
        );
        log.append(e(PENDING_CHANGES as u64 + 1, down), None, Vec::new(), 1)
            .unwrap();
    }

    /// A replicated log refuses no entry for room: a replica whose RT
    /// thread lags keeps the committed entries its ring has no room for
    /// and passes them on as the ring drains, rather than refuse one,
    /// which would stop it taking writes while the others apply it.
    #[test]
    fn a_replicated_log_keeps_what_its_ring_has_no_room_for() {
        let (mut log, mut rx, _msgs) = log();
        let down = Change::InputDown { slot: 0 };
        let n = PENDING_CHANGES as u64 + 36;
        for id in 1..=n {
            log.append(e(id, down), None, Vec::new(), 0).unwrap();
        }
        log.commit(n);
        assert_eq!(rx.slots(), PENDING_CHANGES);
        assert!(!log.skip(n), "a committed entry skipped");
        for id in 1..=10 {
            assert_eq!(rx.pop().unwrap().entry.id, id);
        }
        log.flush();
        assert_eq!(rx.slots(), PENDING_CHANGES);
        let ids: Vec<LogId> = std::iter::from_fn(|| rx.pop().ok().map(|q| q.entry.id)).collect();
        assert_eq!(ids, (11..=PENDING_CHANGES as u64 + 10).collect::<Vec<_>>());
        log.flush();
        assert_eq!(rx.slots(), 26);
    }

    /// A product state's strip parameters reach the RT thread folded to
    /// the last value per slot and key: a list of every value a parameter
    /// took would otherwise be set one by one in one wake.
    #[test]
    fn product_strip_parameters_fold_to_the_last_value_per_key() {
        let trim = |db| StripParam::Trim { db };
        let leg = StripParam::LegTrim { leg: 1, db: 2.0 };
        let mut history: Vec<StripParam> = (0..1000).map(|n| trim(-(n % 20) as f32)).collect();
        history.push(leg);
        history.push(trim(-9.0));
        assert_eq!(
            product_strips(&[history, vec![trim(-1.0), trim(-2.0)]]),
            [(0, trim(-9.0)), (0, leg), (1, trim(-2.0))]
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
            // A stall of a loaded host before the first commits is caught
            // up, not skipped: a skip then would withdraw the replica, and it
            // would commit only on evidence this test gives none of.
            "maxCatchupBlocks": 200,
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

        // Longer than the catch-up: the loop skips.
        crate::rt_loop::STALL_MS.store(400, Relaxed);
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
                let _ = e.commit_through(0, k, 0, None);
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

    /// Pauses the loop of `e` once in the step of the first block from
    /// `k` on: right after output 0 moved its ring with `in_ring`, else
    /// before the outputs decide the block
    /// ([`crate::rt_loop::HOLD_PASS_PAUSE`]). Armed before F is frozen,
    /// the loop pauses itself, so no ring fills while the test thread
    /// waits to be scheduled.
    fn pause_at(e: &Engine, k: u64, in_ring: bool) {
        use crate::rt_loop::{HOLD_PASS_PAUSE, HOLD_PASS_PAUSE_IN_RING, HOLD_PASS_PAUSE_K};
        HOLD_PASS_PAUSE_K.store(k, Relaxed);
        HOLD_PASS_PAUSE_IN_RING.store(in_ring, Relaxed);
        HOLD_PASS_PAUSE.store(std::ptr::from_ref(e.counters()) as usize, Release);
    }

    /// Waits for the loop [`pause_at`] armed to pause.
    fn paused() {
        let t = Instant::now();
        while !crate::rt_loop::HOLD_PASS_PAUSED.load(Acquire) {
            assert!(t.elapsed() < Duration::from_secs(10), "timed out");
            std::thread::yield_now();
        }
    }

    /// Resumes the loop [`pause_at`] paused.
    fn resume() {
        crate::rt_loop::HOLD_PASS_PAUSED.store(false, Release);
    }

    /// Disarms and resumes the loop [`pause_at`] armed, also when a test
    /// fails.
    struct Unpause;

    impl Drop for Unpause {
        fn drop(&mut self) {
            crate::rt_loop::HOLD_PASS_PAUSE.store(0, Relaxed);
            resume();
        }
    }

    /// A block withheld on one output withdraws the replica on every
    /// output before the next one advances: output 1 does not commit
    /// block k while output 0 withholds it, also when the supervisor
    /// withholds k and moves F past it in the middle of a pass over the
    /// hold rings, after output 0 moved its ring.
    #[test]
    fn a_block_withheld_on_one_output_withdraws_the_next_before_it_advances() {
        use crate::blocks::Kind;
        withhold_in_a_pass("withdraw-every", 0, true, |e, k| {
            let st = e.block_status(1, k).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
        });
    }

    /// Both outputs hold block k fenced, F frozen, with the loop paused in
    /// a step after it: right after output 0 moved its ring with
    /// `in_ring`, else after the pass over the rings. The supervisor
    /// withholds k on output `on` alone and moves F past it; `check` runs
    /// on the status once the loop went on. An overrun skip or a full ring
    /// under load withdraws the replica or leaves k without a fenced
    /// status: then the outputs are named again on a new engine, since a
    /// withdrawn output's ring of fills overflows before its commit
    /// start.
    fn withhold_in_a_pass(name: &str, on: usize, in_ring: bool, check: impl Fn(&Engine, u64)) {
        use crate::blocks::{Cause, Kind};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        for _ in 0..10 {
            let (e, _changes, _verdicts, domain) = outputs_only(name, 2);
            let _unpause = Unpause;
            let processed = || e.counters().processed.load(Acquire);
            let fenced = |k: u64| {
                (0..2).all(|o| e.block_status(o, k).is_some_and(|s| s.kind == Kind::Fenced))
            };
            let committing_all =
                || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
            committing(&e, 2);
            let f = processed() + 2;
            pause_at(&e, f + 5, in_ring);
            e.set_fence(f * 48).unwrap();
            paused();
            let first = (f..=processed()).find(|&j| fenced(j));
            if !committing_all() || first != Some(f) {
                resume();
                e.stop();
                std::fs::remove_dir_all(&domain).unwrap();
                continue;
            }
            let st = e.withhold(on, f, Cause::Silence).unwrap().unwrap();
            assert_eq!(st.kind, Kind::Withheld);
            e.set_fence(u64::MAX).unwrap();
            resume();
            wait(&|| processed() > f + 15);
            check(&e, f);
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            return;
        }
        panic!("no pass without an overrun in 10 attempts");
    }

    /// A block withheld on a later output alone withdraws the replica on
    /// every output before any ring moves at the F that passes it: output
    /// 0 does not commit block k while output 1 withholds it, though
    /// output 0's ring moves first in a pass.
    #[test]
    fn a_block_withheld_on_a_later_output_withdraws_the_first_before_it_advances() {
        use crate::blocks::Kind;
        withhold_in_a_pass("withdraw-later", 1, false, |e, k| {
            let st = e.block_status(0, k).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
        });
    }

    /// A held block committed through on one output only up to P inside
    /// it withholds the rest of it for a cause, which withdraws the
    /// replica on every output before any moves past it: output 0, its
    /// ring moved first in a pass, does not commit the block after it
    /// alone. Block k is degraded by an unresolved entry, the blocks after
    /// it healthy and held behind it; the supervisor marks k on output 0
    /// whole and on output 1 up to the middle of k, between two passes.
    #[test]
    fn a_commit_through_up_to_p_on_a_later_output_withdraws_the_first() {
        use crate::blocks::{HEALTHY, Kind};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        for _ in 0..10 {
            let (e, _changes, _verdicts, domain) = outputs_only("through-up-to", 2);
            let _unpause = Unpause;
            let processed = || e.counters().processed.load(Acquire);
            let held = |o: usize, k: u64, healthy: bool| {
                e.block_status(o, k)
                    .is_some_and(|s| s.kind == Kind::Held && (s.digest == Some(HEALTHY)) == healthy)
            };
            let committing_all =
                || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
            committing(&e, 2);
            // Block k alone carries the entry: it is resolved while the
            // loop is paused in block k's pass, after k's digest.
            let k = processed() + 10;
            e.unresolved(1, k * 48).unwrap();
            pause_at(&e, k, true);
            paused();
            e.resolved(1).unwrap();
            pause_at(&e, k + 3, false);
            resume();
            paused();
            let ready = committing_all()
                && (0..2)
                    .all(|o| held(o, k, false) && held(o, k + 1, true) && held(o, k + 2, true));
            if !ready {
                resume();
                e.stop();
                std::fs::remove_dir_all(&domain).unwrap();
                continue;
            }
            assert!(e.commit_through(0, k, 0, None).unwrap());
            assert!(e.commit_through(1, k, 0, Some(k * 48 + 24)).unwrap());
            resume();
            wait(&|| processed() > k + 10);
            let st = e.block_status(0, k).unwrap();
            assert_eq!(st.kind, Kind::Committed, "{st:?}");
            let st = e.block_status(0, k + 1).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            return;
        }
        panic!("no pass without an overrun in 10 attempts");
    }

    /// A full hold ring moves at the F its block's pass read: with output
    /// 0's ring shorter than output 1's, output 1 does not commit block k
    /// while output 0 withholds it, also when the supervisor withholds k
    /// and moves F past it after the pass over the hold rings, before the
    /// outputs decide the block at which output 1's ring is full.
    #[test]
    fn a_full_ring_moves_at_the_fence_of_its_pass() {
        use crate::blocks::{Cause, Kind, PAGE};
        // The hold ring at the default holdMs and contactWindowMs; the
        // overflow of output 1's block f below proves it.
        const RING: u64 = 20;
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        // An overrun skip under load leaves a block of the rings without a
        // fenced status: then the outputs are named again on a new engine.
        for _ in 0..10 {
            let (e, _changes, _verdicts, domain) = outputs_only("full-ring", 2);
            let _unpause = Unpause;
            let processed = || e.counters().processed.load(Acquire);
            let fenced =
                |o: usize, k: u64| e.block_status(o, k).is_some_and(|s| s.kind == Kind::Fenced);
            let committing_all =
                || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
            // Output 1 commits from a page boundary before block f, and
            // holds every block from f on, F frozen there. Output 0 commits
            // from the page boundary in block kp, five blocks later: its
            // ring is full five blocks after output 1's, which is full at
            // block f + RING.
            let now = processed();
            let kp = ((now + 40) * 48).next_multiple_of(PAGE) / 48;
            let (f, k) = (kp - 5, kp + 1);
            e.set_fence(u64::MAX).unwrap();
            e.commit_from(1, now + 2).unwrap();
            e.set_fence(f * 48).unwrap();
            e.commit_from(0, kp).unwrap();
            pause_at(&e, f + RING, false);
            paused();
            if !committing_all() || !(f..f + RING).all(|j| fenced(1, j)) || !fenced(0, k) {
                resume();
                e.stop();
                std::fs::remove_dir_all(&domain).unwrap();
                continue;
            }
            let st = e.withhold(0, k, Cause::HoldCap).unwrap().unwrap();
            assert_eq!(st.kind, Kind::Withheld);
            e.set_fence(u64::MAX).unwrap();
            resume();
            wait(&|| processed() > f + RING + 5);
            // Output 1's ring was full at the decision: its front block
            // overflowed, at the F the pass read.
            let st = e.block_status(1, f).unwrap();
            assert_eq!(
                (st.kind, st.cause),
                (Kind::Withheld, Cause::Overflow),
                "{st:?}"
            );
            let st = e.block_status(1, k).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            return;
        }
        panic!("no pass without an overrun in 10 attempts");
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
        // Readable while attached, past the polls after the attach too.
        let readable = || {
            std::thread::sleep(3 * POLL_PERIOD);
            e.status().inputs[0].readable
        };
        wait(&attached);
        assert_eq!(readable(), Some(true));
        let late = || e.counters().command_late.load(Relaxed);

        STAMP_LATE.store(true, Relaxed);
        p.stop();
        wait(&|| !e.status().inputs[0].up);
        assert_eq!(readable(), Some(false), "pruned");
        assert_eq!(late(), 1);
        assert!(e.status().diverged.is_some());

        STAMP_LATE.store(true, Relaxed);
        let p = producer();
        wait(&attached);
        assert_eq!(readable(), Some(true));
        assert_eq!(late(), 2);
        e.stop();
        p.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// The RT loop's failure paths run, so that CI's RealtimeSanitizer
    /// run over the lib tests checks them: a stall's catch-up and skip, a
    /// forward and a backward clock step with the wakes behind the last
    /// block after it, and a late input whose flow is deleted, waited for
    /// on its head and pruned without a probe, with a strip advanced over
    /// the skip. Engine `p` writes the flow,
    /// engine `e` reads it.
    #[test]
    fn the_failure_paths_run_on_the_rt_thread() {
        use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS, STALL_MS};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        /// Takes the clock shift off again, also when the test fails.
        struct Unshift;
        impl Drop for Unshift {
            fn drop(&mut self) {
                CLOCK_STEP_NS.store(0, Relaxed);
                CLOCK_SHIFT_NS.store(0, Relaxed);
                CLOCK_STEP_LOOP.store(0, Relaxed);
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
        // Engine `e`'s loop takes each step at its wake, between two of
        // its blocks; both engines read the stepped clock.
        CLOCK_STEP_LOOP.store(std::ptr::from_ref(c) as usize, Relaxed);
        let steps = n(&c.clock_steps);
        CLOCK_STEP_NS.store(50_000_000, Relaxed);
        until("forward step", &|| n(&c.clock_steps) > steps);
        CLOCK_STEP_NS.store(-50_000_000, Relaxed);
        until("backward step", &|| n(&c.clock_steps) > steps + 1);
        // Behind the last block computed, the loop wakes without computing.
        until("wake behind", &|| n(&c.clock_behind_wakes) > 0);
        // The producer stops and its flow is deleted: the input is late,
        // frozen and pruned, and the RT thread never checks the flow.
        p.stop();
        until("late block", &|| n(&c.input_late_blocks) > 0);
        until("prune", &|| {
            e.status().inputs[0].state == InputState::Pruned
        });
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
            // 40 blocks past the earliest index: set_strip below checks
            // the same index against a clock that has moved on, and a
            // scheduling delay of a block or more in between would refuse
            // it as too near.
            at_index: (earliest(&e.api, &e.counters, e.margin, e.lead) + 40 * e.block)
                .next_multiple_of(e.block),
            change: Change::Strip {
                slot: 0,
                param: refused,
            },
        };
        log.append(
            entry,
            None,
            Vec::new(),
            e.counters.entries_consumed.load(Acquire),
        )
        .unwrap();
        log.commit(entry.id);
        drop(log);
        e.set_strip(0, StripParam::Trim { db: -3.0 }, entry.at_index)
            .unwrap();
        until("divergence", &|| e.status().diverged.is_some());
        let diverged = e.status().diverged.unwrap();
        assert!(
            diverged.contains("refused by the strip, limiter"),
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

    /// A replayed core for a block the RT thread passed goes back
    /// refused, its state untouched: installed there, it would replace the
    /// state after a later block with an older one.
    #[test]
    fn a_handover_the_rt_thread_passed_is_refused() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("handover-passed", 1);
        let copy = loop {
            match e.copy_core(Duration::from_secs(2)) {
                Err(CopyError::Passed { .. }) => continue,
                c => break c.unwrap(),
            }
        };
        wait(&|| e.counters.processed.load(Acquire) > copy.k0 + 2);
        let mut t = e.transfer.as_ref().unwrap().lock().unwrap();
        let h = Handover {
            seq: 1,
            j: copy.k0,
            last_id: copy.last_id,
            core: copy.core,
            replayed: true,
            strips: Vec::new(),
            limiters: Vec::new(),
            delays: Vec::new(),
            out_strips: Vec::new(),
            out_delays: Vec::new(),
            tones: Vec::new(),
            answer: Answer::Waiting,
        };
        assert!(t.handovers.push(h).is_ok());
        let h = loop {
            if let Ok(h) = t.handed.pop() {
                break h;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(h.answer, Answer::Passed);
        assert_eq!(e.counters.handovers_passed.load(Relaxed), 1);
        assert_eq!(e.counters.handovers_installed.load(Relaxed), 0);
        drop(t);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A copy buffer left with the RT thread by a call that timed out, and
    /// filled there, `slack` widened so the test thread may stall.
    fn copy_left_behind(e: &Engine) {
        let transfer = e.transfer.as_ref().unwrap();
        transfer.lock().unwrap().slack_blocks = 1000;
        // No control waits: the buffer is sent, and no copy is back yet.
        assert_eq!(e.copy_core(Duration::ZERO).err(), Some(CopyError::Timeout));
        assert!(transfer.lock().unwrap().in_flight, "no buffer sent");
        wait(&|| transfer.lock().unwrap().copies.slots() == 1);
    }

    /// The copy a timed-out call left behind was filled before a control
    /// sent after that call: the next call has a buffer filled again, so
    /// its copy holds every control sent before it.
    #[test]
    fn a_copy_left_behind_holds_the_controls_sent_after_it() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("copy-behind", 1);
        copy_left_behind(&e);
        let far = (e.counters.processed.load(Acquire) + 1_000_000) * e.block;
        e.unresolved(1, far).unwrap();
        let c = e.copy_core(Duration::from_secs(2)).unwrap();
        assert_eq!(c.core.unresolved(), &[(1, far)]);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A core fit for a hand-over after block `j`, of an engine with no
    /// inputs and no log entries taken.
    fn bare_core(e: &Engine, j: u64, core: &mut Core) -> (u64, LogId) {
        core.pos = Some((j + 1) * e.block);
        (j, 0)
    }

    /// The copy a timed-out call left behind was filled before a core
    /// handed over since: the next call has a buffer filled again, after
    /// the install.
    #[test]
    fn a_copy_left_behind_is_filled_again_after_a_hand_over() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("copy-handover", 1);
        copy_left_behind(&e);
        let j = e
            .offer(
                Duration::from_secs(5),
                0,
                false,
                &[],
                &[],
                &[],
                &[],
                &[],
                &[],
                |j, core| bare_core(&e, j, core),
            )
            .unwrap();
        let c = e.copy_core(Duration::from_secs(2)).unwrap();
        assert!(
            c.k0 > j,
            "copy after block {} of a core installed after {j}",
            c.k0
        );
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A product install that finds the control ring full while a core is
    /// held retries the drop until its timeout and fails as full, sending
    /// nothing; once the ring has room it drops the core before the
    /// install.
    #[test]
    fn a_product_install_retries_the_drop_while_the_ring_is_full() {
        use crate::rt_loop::STALL_MS;
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("drop-full", 1);
        let c = &e.counters;
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: Vec::new(),
            up: Vec::new(),
            flows: Vec::new(),
            outputs: Vec::new(),
            pre_strip_meters: Vec::new(),
            loudness_running: Vec::new(),
            strips: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let processed = || c.processed.load(Acquire);
        // The RT thread stalls with the ring full. A stall another test's
        // RT thread takes is set again.
        let t = Instant::now();
        loop {
            assert!(t.elapsed() < Duration::from_secs(10), "no stall");
            STALL_MS.store(500, Relaxed);
            wait(&|| STALL_MS.load(Relaxed) == 0);
            let p = processed();
            std::thread::sleep(Duration::from_millis(20));
            if processed() == p {
                break;
            }
        }
        while e.withdraw().is_ok() {}
        assert_eq!(
            e.install_product(&state, 0, Duration::from_millis(50)),
            Err(ReplayError::Full)
        );
        assert_eq!(c.blocks_without_core.load(Relaxed), 0, "dropped while full");
        let j = e
            .install_product(&state, 0, Duration::from_secs(5))
            .unwrap();
        assert!(
            c.blocks_without_core.load(Relaxed) > 0,
            "installed over a held core"
        );
        assert!(processed() > j);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A product install's drop, hand-over and install share one timeout:
    /// a drop that waited out most of it leaves the install the rest. The
    /// RT thread stalls with the control ring full, then computes nothing
    /// for 3 s, its clock stepped back.
    #[test]
    fn a_product_install_keeps_to_one_timeout() {
        use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS, STALL_MS};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        struct Unshift;
        impl Drop for Unshift {
            fn drop(&mut self) {
                STALL_MS.store(0, Relaxed);
                CLOCK_STEP_NS.store(0, Relaxed);
                CLOCK_SHIFT_NS.store(0, Relaxed);
                CLOCK_STEP_LOOP.store(0, Relaxed);
            }
        }
        let _unshift = Unshift;
        let (e, _statuses, _verdicts, domain) = outputs_only("drop-deadline", 1);
        let c = &e.counters;
        CLOCK_STEP_LOOP.store(std::ptr::from_ref(&**c) as usize, Relaxed);
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: Vec::new(),
            up: Vec::new(),
            flows: Vec::new(),
            outputs: Vec::new(),
            pre_strip_meters: Vec::new(),
            loudness_running: Vec::new(),
            strips: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let processed = || c.processed.load(Acquire);
        let t = Instant::now();
        loop {
            assert!(t.elapsed() < Duration::from_secs(10), "no stall");
            STALL_MS.store(600, Relaxed);
            wait(&|| STALL_MS.load(Relaxed) == 0);
            let p = processed();
            std::thread::sleep(Duration::from_millis(20));
            if processed() == p {
                break;
            }
        }
        // Taken at the end of the stall, in the same wake.
        CLOCK_STEP_NS.store(-3_000_000_000, Relaxed);
        while e.withdraw().is_ok() {}
        let t = Instant::now();
        let r = e.install_product(&state, 0, Duration::from_secs(1));
        let took = t.elapsed();
        assert_eq!(r, Err(ReplayError::Timeout { passed: 0 }));
        assert!(took < Duration::from_millis(1300), "took {took:?}");
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A hand-over a call gave up on can still be installed. The drop
    /// before a product install waits for its answer and follows it, so
    /// no core is held after it returns: the clock steps back as the
    /// hand-over is sent, and the RT thread computes nothing, answering
    /// nothing, until the clock is due at its block again.
    #[test]
    fn the_drop_follows_a_hand_over_left_unanswered() {
        use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        struct Unshift;
        impl Drop for Unshift {
            fn drop(&mut self) {
                CLOCK_STEP_NS.store(0, Relaxed);
                CLOCK_SHIFT_NS.store(0, Relaxed);
                CLOCK_STEP_LOOP.store(0, Relaxed);
            }
        }
        let _unshift = Unshift;
        let (e, _statuses, _verdicts, domain) = outputs_only("drop-unanswered", 1);
        let c = &e.counters;
        CLOCK_STEP_LOOP.store(std::ptr::from_ref(&**c) as usize, Relaxed);
        let without = || c.blocks_without_core.load(Relaxed);
        let installed = || c.handovers_installed.load(Relaxed);
        // A hand-over taken a block late is refused, not installed: tried
        // again.
        for attempt in 0.. {
            assert!(attempt < 5, "no hand-over installed after its call");
            e.drop_core().unwrap();
            let w = without();
            wait(&|| without() > w + 5);
            let before = installed();
            let r = e.offer(
                Duration::from_secs(5),
                0,
                false,
                &[],
                &[],
                &[],
                &[],
                &[],
                &[],
                |j, core| {
                    CLOCK_STEP_NS.store(-1_500_000_000, Relaxed);
                    bare_core(&e, j, core)
                },
            );
            assert_eq!(r, Err(ReplayError::Timeout { passed: 0 }));
            assert!(e.transfer.as_ref().unwrap().lock().unwrap().handing);
            e.drop_core_by(Instant::now() + Duration::from_secs(5))
                .unwrap();
            let w = without();
            let p = c.processed.load(Acquire);
            wait(&|| c.processed.load(Acquire) > p + 20);
            if installed() == before {
                continue;
            }
            assert!(without() > w, "a core held after the drop");
            break;
        }
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Blocks a late wake skips while no core is held are not computed
    /// for want of a core, as every other block then: they are reported
    /// as such in one gapless run of ranges, not as a skip.
    #[test]
    fn a_skip_without_a_core_is_reported_as_no_core() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, statuses, _verdicts, domain) = outputs_only("no-core-skip", 1);
        let c = &e.counters;
        e.drop_core().unwrap();
        wait(&|| c.blocks_without_core.load(Relaxed) > 5);
        // Changes from before the drop, a skip under load among them.
        statuses.try_iter().for_each(drop);
        let skipped = c.overrun_skipped_samples.load(Relaxed);
        // A stall of 40 blocks, past maxCatchupBlocks. An RT thread of a
        // test holding no engine lock may take the stall: it is set again
        // until one is seen.
        let mut ranges = Vec::new();
        let t = Instant::now();
        while !ranges.iter().any(|&(from, to)| to - from >= 20) {
            assert!(t.elapsed() < Duration::from_secs(10), "no stall");
            if crate::rt_loop::STALL_MS.load(Relaxed) == 0 {
                crate::rt_loop::STALL_MS.store(40, Relaxed);
            }
            match statuses.recv_timeout(Duration::from_millis(200)) {
                Ok(StatusChange::NoCore { from, to }) => ranges.push((from, to)),
                Ok(StatusChange::Skipped { from, to }) => panic!("skipped {from}..{to}"),
                _ => {}
            }
        }
        crate::rt_loop::STALL_MS.store(0, Relaxed);
        assert!(ranges.windows(2).all(|w| w[0].1 == w[1].0), "{ranges:?}");
        assert_eq!(c.overrun_skipped_samples.load(Relaxed), skipped);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// The earliest index a refusal as too near names is a block start, so
    /// a caller can retry with it; while the RT thread is behind, the
    /// clock less the reader margin sets it, at any sample.
    #[test]
    fn too_near_names_a_block_start() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-too-near-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0b", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0c", "label": "near",
                          "group": "near", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let e = Engine::start(config, process, None).unwrap();
        wait(&|| e.counters.processed.load(Acquire) > 10);
        crate::rt_loop::STALL_MS.store(300, Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        let mut named = Vec::new();
        for _ in 0..8 {
            match e.set_gain(0, 0, 0.5, 0) {
                Err(GainError::TooNear { earliest, .. }) => named.push(earliest),
                other => panic!("{other:?}"),
            }
            match e.reset_loudness(0, 0) {
                Err(GainError::TooNear { earliest, .. }) => named.push(earliest),
                other => panic!("{other:?}"),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        crate::rt_loop::STALL_MS.store(0, Relaxed);
        assert!(named.iter().all(|x| x % e.block == 0), "{named:?}");
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    /// An entry applied before its generation arrived keeps waiting for it
    /// across an install, its slot held: a product core holds no
    /// generation and an empty awaiting list, so cleared here the
    /// generation would be returned unused when it came, and the slot
    /// would stay on the old flow that every other replica left.
    #[test]
    fn an_install_keeps_an_entry_waiting_for_its_generation() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("install-awaiting", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0d", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0e", "label": "await",
                          "group": "await", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        let route = RtEntry {
            id: 5,
            at_index: 96,
            change: Change::Route { slot: 0 },
        };
        rt.awaiting_gen.push(route);
        rt.slots[0].held = true;
        rt.install_core(&mut core, 5, true);
        assert_eq!(rt.awaiting_gen, [route]);
        assert!(rt.slots[0].held);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A config of a stereo tone generator on a stereo output.
    fn tone_config() -> Config {
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": [{ "name": "tone", "tone": {}, "channels": 2 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d20", "label": "tone",
                          "group": "tone", "role": "Audio", "channels": 2 }],
            "matrix": [[1.0, 0.0], [0.0, 1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        Config::parse(&v.to_string()).unwrap()
    }

    /// A tone generator's slot through the RT loop, from a block near
    /// index 2^40: settled, each leg is the formula, bit for bit, in no
    /// input domain, never silenced, failed or late. A core copied at
    /// block P and installed into a second loop continues it: both
    /// compute the same bits through a level, frequency and ident change
    /// pending at P, as a rebuilt replica computes a running one's. Fails
    /// with `d.tone = s.tone;` dropped from `install_core`, or with the
    /// generator bypassed in `process`.
    #[test]
    fn a_tone_slot_is_its_formula_and_a_copied_core_continues_it() {
        let config = tone_config();
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let (slots, in_domains) = new_slots(&config);
        assert!(in_domains.is_empty());
        assert_eq!(slots[0].domain, NO_DOMAIN);
        let rt = || {
            let (hashes, _) = rtrb::RingBuffer::new(1);
            let (_, control) = rtrb::RingBuffer::new(1);
            replay_rt(&config, &api, Vec::new(), hashes, control)
        };
        let b = u64::from(config.block_size);
        let first = (1u64 << 40) / b;
        let tone = |id, k: u64, frequency_hz, level_db, ident| RtEntry {
            id,
            at_index: k * b,
            change: Change::Tone {
                slot: 0,
                params: ToneParams {
                    frequency_hz,
                    level_db,
                    ident,
                },
            },
        };
        let mut a = rt();
        assert!(a.mix.matrix.set(0, 0, 1.0, 0) && a.mix.matrix.set(1, 1, 1.0, 0));
        for e in [
            tone(1, first + 2, 997, -6.0, false),
            tone(2, first + 60, 1000, -12.0, true),
            tone(3, first + 120, 440, 0.0, false),
        ] {
            assert!(a.mix.push(e));
        }
        let leg = |rt: &Rt, c: usize| rt.outbuf.channel(c)[..b as usize].to_vec();
        let p = first + 110;
        let settled = first + 2 + u64::from(config.ramp_samples).div_ceil(b);
        let level = samplez_dsp::tone::level_gain(-6.0);
        for k in first..=p {
            a.process(k, u64::MAX);
            if (settled..first + 60).contains(&k) {
                for c in 0..2 {
                    for (j, &y) in leg(&a, c).iter().enumerate() {
                        let n = k * b + j as u64;
                        let want = level * samplez_dsp::tone::sine(n, 997, 48_000);
                        assert_eq!(y.to_bits(), want.to_bits(), "leg {c} at {n}");
                    }
                }
            }
        }
        let counters = &a.counters;
        assert_eq!(counters.input_silenced_blocks.load(Relaxed), 0);
        assert_eq!(counters.input_late_blocks.load(Relaxed), 0);
        assert_eq!(counters.strip_refused.load(Relaxed), 0);
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        a.capture(&mut core);
        assert!(core.tone(0).is_some());
        let mut c = rt();
        c.install_core(&mut core, 3, true);
        for k in p + 1..p + 400 {
            a.process(k, u64::MAX);
            c.process(k, u64::MAX);
            for ch in 0..2 {
                let (x, y) = (leg(&a, ch), leg(&c, ch));
                let same = x.iter().zip(&y).all(|(x, y)| x.to_bits() == y.to_bits());
                assert!(same, "block {k} leg {ch}");
            }
        }
    }

    /// Two loops metering a 9.6 kHz tone input with `truePeak`, the
    /// second skipping blocks as after an overrun up to a window
    /// boundary, read the same true peak, bit for bit, in every 100 ms
    /// window both meter whole, the one after the skip included, and the
    /// same largest true peak: after the skip the interpolator restarts
    /// and leaves out the outputs that reach back over it. The tone's
    /// samples fall 18 degrees off its crest, so every window reads the
    /// true peak `TruePeak` computes from the formula, above the sample
    /// peak. Fails with `self.tp_skip = TP_HISTORY` dropped from
    /// `OutMeter::add`, the window after the skip then reading the
    /// interpolator's rise from silence, and with that reset dropped
    /// whole, the window then reading history from before the skip.
    #[test]
    fn an_input_true_peak_is_the_same_bits_across_an_overrun_skip() {
        let mut config = tone_config();
        config.inputs[0].true_peak = true;
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let b = u64::from(config.block_size);
        let (f, level) = (9600, samplez_dsp::tone::level_gain(-6.0));
        // Ending on a window boundary, so the window after it is metered
        // whole, and not a multiple of the tone's 5-sample period long, so
        // history kept across it is out of phase.
        let skip = 2003..2100;
        assert!((skip.end * b).is_multiple_of(4800));
        assert!(!((skip.end - skip.start) * b).is_multiple_of(5));
        let run = |skip: &std::ops::Range<u64>| {
            let (hashes, _) = rtrb::RingBuffer::new(1);
            let (_, control) = rtrb::RingBuffer::new(1);
            let mut rt = replay_rt(&config, &api, Vec::new(), hashes, control);
            // As the engine's loop, its input meters included.
            rt.replay = false;
            (rt.buses, rt.taps, rt.pre_taps) = new_meters(&config);
            let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
            rt.meters = meters;
            let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
            rt.duck_meters = ducks;
            let (ret, _back) = rtrb::RingBuffer::new(64);
            rt.ret = ret;
            let params = ToneParams {
                frequency_hz: f,
                level_db: -6.0,
                ident: false,
            };
            let change = Change::Tone { slot: 0, params };
            assert!(rt.mix.push(RtEntry {
                id: 1,
                at_index: 0,
                change,
            }));
            let tap = config.outputs.len() as u32;
            let mut l = OutputLoudness::new(config.inputs[0].loudness_weights().unwrap());
            let mut windows = std::collections::BTreeMap::new();
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            for k in (0..2400).filter(|k| !skip.contains(k)) {
                rt.process(k, u64::MAX);
                while let Ok(r) = rx.pop() {
                    if r.meter == tap {
                        l.record(&r);
                    }
                }
                if let Some(w) = l.meters.window {
                    windows.insert(w, (bits(&l.meters.true_peak), bits(&l.meters.peak)));
                }
            }
            (windows, bits(&l.meters.max_true_peak))
        };
        let (whole, max) = run(&(0..0));
        let (skipped, skipped_max) = run(&skip);
        let after = skip.end * b / 4800;
        assert!(!skipped.contains_key(&(after - 1)) && skipped.contains_key(&after));
        let x: Vec<f32> = (0..71)
            .map(|n| level * samplez_dsp::tone::sine(n, f, 48_000))
            .collect();
        let mut tp = samplez_dsp::TruePeak::new(1, 64);
        tp.process(0, &x[..samplez_dsp::TruePeak::HISTORY]);
        let want = tp.process(0, &x[samplez_dsp::TruePeak::HISTORY..]);
        let peak = x.iter().fold(0.0f32, |m, v| m.max(v.abs()));
        assert!(want > peak * 1.04, "{want} {peak}");
        for (w, (true_peak, peak)) in &whole {
            if *w > 0 {
                assert_eq!(*true_peak, [want.to_bits(); 2], "window {w}");
                assert!(peak.iter().all(|&p| f32::from_bits(p) < want));
            }
            if let Some(s) = skipped.get(w) {
                assert_eq!(s.0, *true_peak, "window {w}");
            }
        }
        assert_eq!(skipped_max, max);
    }

    /// A stereo input feeding a stereo output that correlates its legs
    /// with `correlating`, a second stereo output that does not, and a
    /// 6-channel configured meter that does with `correlating`, legs 0
    /// and 1 of each fed by the input's L and R.
    fn correlation_config(correlating: bool) -> Config {
        let out = |o: usize, correlation: bool| {
            serde_json::json!({ "name": format!("out{o}"), "domain": "d",
                "flowId": format!("6fbec3b1-1b0f-417d-9059-00000000000{o}"), "label": "o",
                "group": "g", "role": "Audio", "channels": 2, "correlation": correlation })
        };
        let mut matrix = vec![vec![0.0; 10]; 2];
        for c in [0, 2, 4] {
            (matrix[0][c], matrix[1][c + 1]) = (1.0, 1.0);
        }
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "5fbec3b1-1b0f-417d-9059-000000000000", "channels": 2 }],
            "outputs": [out(0, correlating), out(1, false)],
            "meters": [{ "name": "wide", "channels": 6, "correlation": correlating }],
            "matrix": matrix,
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        Config::parse(&v.to_string()).unwrap()
    }

    /// The meter records of [`correlation_config`] fed `source` over
    /// blocks `0..blocks` but `skip`, as an overrun skips them, and per
    /// 100 ms window read, the correlation bits of each output and meter
    /// read at its end.
    #[allow(clippy::type_complexity)]
    fn correlate(
        config: &Config,
        source: crate::rt_loop::TestInput,
        blocks: u64,
        skip: std::ops::Range<u64>,
    ) -> (
        Vec<MeterRecord>,
        std::collections::BTreeMap<(usize, u64), Option<u64>>,
        Vec<OutputMeters>,
    ) {
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(config, &api, vec![None], hashes, control);
        // As the engine's loop: configured and input meters, inputs read.
        rt.replay = false;
        (rt.buses, rt.taps, rt.pre_taps) = new_meters(config);
        rt.test_input = Some(source);
        for (row, gains) in config.matrix.iter().enumerate() {
            for (column, &g) in gains.iter().enumerate() {
                assert!(rt.mix.matrix.set(row, column, g, 0));
            }
        }
        let (meters, mut rx) = rtrb::RingBuffer::new(1 << 12);
        rt.meters = meters;
        let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
        rt.duck_meters = ducks;
        let (ret, _back) = rtrb::RingBuffer::new(64);
        rt.ret = ret;
        let elements = (config.outputs.iter().map(|o| (o.channels, o.correlation)))
            .chain(config.meters.iter().map(|m| (m.channels, m.correlation)));
        let mut l: Vec<OutputLoudness> = elements
            .map(|(n, on)| OutputLoudness::new(vec![1.0; n as usize]).correlating(on))
            .collect();
        let (mut records, mut windows) = (Vec::new(), std::collections::BTreeMap::new());
        for k in (0..blocks).filter(|k| !skip.contains(k)) {
            rt.process(k, u64::MAX);
            while let Ok(r) = rx.pop() {
                if let Some(l) = l.get_mut(r.meter as usize) {
                    l.record(&r);
                    records.push(r);
                }
            }
            for (m, l) in l.iter().enumerate() {
                if let Some(w) = l.meters.window {
                    windows.insert((m, w), l.meters.correlation.map(f64::to_bits));
                }
            }
        }
        assert_eq!(rt.counters.meter_dropped.load(Relaxed), 0);
        (records, windows, l.into_iter().map(|l| l.meters).collect())
    }

    /// A 1 kHz sine at half scale at sample `n`, phase-shifted by
    /// `quarters` quarter periods.
    fn sine_1k(n: u64, quarters: u64) -> f32 {
        let phase = ((n * 1000 + quarters * 12_000) % 48_000) as f64 / 48_000.0;
        (0.5 * (std::f64::consts::TAU * phase).sin()) as f32
    }

    /// An output and a meter that correlate their legs read +1 for L = R,
    /// -1 for L = -R, 0 within 1e-3 for sines in quadrature, 0 with one
    /// leg silent and none with both, over each 400 ms; the 6-channel
    /// meter reads its legs 0 and 1 as the stereo output does. The output
    /// that does not correlate reads none, and its records, and those of
    /// the others but their cross terms, are the same bits as without
    /// the configuration. Fails with the cross term accumulating L * L
    /// in `OutMeter::add`, which reads L = -R as +1.
    #[test]
    fn correlation_reads_the_relation_of_the_legs() {
        let config = correlation_config(true);
        let cases: [(crate::rt_loop::TestInput, Option<(f64, f64)>); 5] = [
            (|_, _, _, n| sine_1k(n, 0), Some((1.0, 0.0))),
            (
                |_, _, c, n| sine_1k(n, 0) * if c == 0 { 1.0 } else { -1.0 },
                Some((-1.0, 0.0)),
            ),
            (|_, _, c, n| sine_1k(n, c as u64), Some((0.0, 1e-3))),
            (
                |_, _, c, n| if c == 0 { sine_1k(n, 0) } else { 0.0 },
                Some((0.0, 0.0)),
            ),
            (|_, _, _, _| 0.0, None),
        ];
        for (i, (source, want)) in cases.into_iter().enumerate() {
            let (_, windows, meters) = correlate(&config, source, 500, 0..0);
            assert_eq!(meters[0].window, Some(4), "case {i}");
            for m in [0, 2] {
                // From the fourth window on, 400 ms are metered.
                for w in 3..5 {
                    let got = windows[&(m, w)].map(f64::from_bits);
                    match (got, want) {
                        (Some(got), Some((want, tolerance))) => {
                            assert!((got - want).abs() <= tolerance, "case {i} meter {m}: {got}");
                        }
                        (got, want) => assert_eq!(got, want.map(|w| w.0), "case {i} meter {m}"),
                    }
                }
            }
            assert_eq!(meters[1].correlation, None, "case {i}");
        }
        // The configuration adds the cross terms and changes nothing else.
        let source: crate::rt_loop::TestInput = |_, _, c, n| sine_1k(n, c as u64);
        let (on, ..) = correlate(&config, source, 500, 0..0);
        let (off, ..) = correlate(&correlation_config(false), source, 500, 0..0);
        let fields = |r: &MeterRecord| {
            let bits = [r.peak.to_bits(), r.true_peak.to_bits()];
            let sums = [r.sum_sq.to_bits(), r.plain_sum_sq.to_bits()];
            (
                r.meter, r.channel, r.slice, r.frames, bits, sums, r.reset_at, r.flags,
            )
        };
        let crosses = on.iter().filter(|r| r.cross()).count();
        assert_eq!(crosses, 2 * 50);
        let on: Vec<_> = on.iter().filter(|r| !r.cross()).map(fields).collect();
        assert_eq!(on, off.iter().map(fields).collect::<Vec<_>>());
    }

    /// Two loops metering the same input, the second skipping blocks as
    /// after an overrun up to a window boundary, read the same
    /// correlation, bit for bit, in every 100 ms window whose 400 ms both
    /// metered whole: the reading is a function of the slices' indices,
    /// not of which records arrived last. The legs beat at 1 Hz, so the
    /// reading moves from window to window, and every window's reading
    /// is the correlation computed from the samples of its 400 ms,
    /// `[(w - 3) * 4800, (w + 1) * 4800)`. Fails with `Correlation::add`
    /// placing a slice in the window by the count of slices it added
    /// rather than by the slice's index, which the skip shifts, and with
    /// `Correlation::read` summing three windows, five over a ring of
    /// five, or the window before.
    #[test]
    fn correlation_is_the_same_bits_across_an_overrun_skip() {
        let config = correlation_config(true);
        let source: crate::rt_loop::TestInput = |_, _, c, n| {
            let hz = if c == 0 { 440 } else { 441 };
            let phase = ((n * hz) % 48_000) as f64 / 48_000.0;
            (0.5 * (std::f64::consts::TAU * phase).sin()) as f32
        };
        // 48-sample blocks; the skip ends on a window boundary.
        let skip = 1003u64..1100;
        assert!((skip.end * 48).is_multiple_of(4800));
        let (_, whole, _) = correlate(&config, source, 2400, 0..0);
        // From the fifth window on, clear of the input's fade-in.
        for (&(m, w), bits) in whole.iter().filter(|((m, w), _)| *m != 1 && *w >= 4) {
            let mut sums = [0.0f64; 3];
            for n in (w - 3) * 4800..(w + 1) * 4800 {
                let (l, r) = (f64::from(source(0, 0, 0, n)), f64::from(source(0, 0, 1, n)));
                for (s, v) in sums.iter_mut().zip([l * l, r * r, l * r]) {
                    *s += v;
                }
            }
            let want = sums[2] / (sums[0] * sums[1]).sqrt();
            let got = f64::from_bits(bits.expect("a correlation"));
            assert!(
                (got - want).abs() < 1e-9,
                "meter {m} window {w}: {got} {want}"
            );
        }
        let (_, skipped, _) = correlate(&config, source, 2400, skip.clone());
        let (cut, after) = (skip.start * 48 / 4800, skip.end * 48 / 4800);
        let mut compared = std::collections::BTreeSet::new();
        for (&(m, w), bits) in &skipped {
            if w < cut || w >= after + 3 {
                assert_eq!(*bits, whole[&(m, w)], "meter {m} window {w}");
                compared.insert(bits.map(f64::from_bits).map(|c| (c * 10.0).round() as i64));
            }
        }
        assert!(skipped.contains_key(&(0, after + 3)) && skipped.contains_key(&(2, 23)));
        assert!(compared.len() > 5, "{compared:?}");
    }

    /// A tone input with `preStripRows` feeds its pre-strip rows, after
    /// the input's rows, untrimmed: an output whose leg 0 takes the
    /// input's row 0 reads a -20 dB trim, while its leg 1, on the
    /// pre-strip row of the input's second channel, stays the formula,
    /// bit for bit. A core copied at block P, with a gain on that
    /// pre-strip row pending at it, continues both legs in a second loop
    /// with the same bits, the gain applied on the same row. Fails with
    /// the copy into the pre-strip rows moved after `run_strip` in
    /// `Slot::finish`, leg 1 then reading the trim.
    #[test]
    fn pre_strip_rows_carry_the_input_before_its_trim_into_a_copied_core() {
        let mut config = tone_config();
        config.inputs[0].pre_strip_rows = true;
        config.matrix = vec![vec![0.0; 2]; 4];
        assert_eq!(config.rows(), 4);
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let rt = || {
            let (hashes, _) = rtrb::RingBuffer::new(1);
            let (_, control) = rtrb::RingBuffer::new(1);
            replay_rt(&config, &api, Vec::new(), hashes, control)
        };
        let b = u64::from(config.block_size);
        let first = (1u64 << 40) / b;
        let mut a = rt();
        assert!(a.mix.matrix.set(0, 0, 1.0, 0) && a.mix.matrix.set(3, 1, 1.0, 0));
        let params = ToneParams {
            frequency_hz: 997,
            level_db: -6.0,
            ident: false,
        };
        let trim = StripParam::Trim { db: -20.0 };
        let (input, output, gain, fade) = (3, 1, 0.5, None);
        let entries = [
            (first + 2, Change::Tone { slot: 0, params }),
            (
                first + 20,
                Change::Strip {
                    slot: 0,
                    param: trim,
                },
            ),
            (
                first + 150,
                Change::Gain {
                    input,
                    output,
                    gain,
                    fade,
                },
            ),
        ];
        for (id, (k, change)) in entries.into_iter().enumerate() {
            let at_index = k * b;
            assert!(a.mix.push(RtEntry {
                id: id as u64 + 1,
                at_index,
                change
            }));
        }
        let leg = |rt: &Rt, c: usize| rt.outbuf.channel(c)[..b as usize].to_vec();
        let ramp = u64::from(config.ramp_samples).div_ceil(b);
        let level = samplez_dsp::tone::level_gain(-6.0);
        let want =
            |k: u64, j: usize| level * samplez_dsp::tone::sine(k * b + j as u64, 997, 48_000);
        let p = first + 140;
        for k in first..=p {
            a.process(k, u64::MAX);
            if k > first + 20 + ramp {
                for (j, (&x, &y)) in leg(&a, 0).iter().zip(&leg(&a, 1)).enumerate() {
                    assert_eq!(y.to_bits(), want(k, j).to_bits(), "leg 1 at block {k}");
                    assert!((x - 0.1 * want(k, j)).abs() < 1e-4, "leg 0 at block {k}");
                }
            }
        }
        assert_eq!(a.counters.strip_refused.load(Relaxed), 0);
        let (rows, outputs) = (config.rows(), config.output_channels());
        let mut core = Box::new(Core::new(rows, outputs, 1, 64, 1));
        a.capture(&mut core);
        let mut c = rt();
        c.install_core(&mut core, 3, true);
        for k in p + 1..p + 400 {
            a.process(k, u64::MAX);
            c.process(k, u64::MAX);
            for ch in 0..2 {
                let (x, y) = (leg(&a, ch), leg(&c, ch));
                let same = x.iter().zip(&y).all(|(x, y)| x.to_bits() == y.to_bits());
                assert!(same, "block {k} leg {ch}");
            }
            if k > first + 150 + ramp {
                for (j, &y) in leg(&c, 1).iter().enumerate() {
                    assert!((y - 0.5 * want(k, j)).abs() < 1e-6, "leg 1 at block {k}");
                }
            }
        }
    }

    /// Two loops on one log metering a 1 kHz tone input with
    /// `preStripMeter`, trimmed by 20 dB in its strip, the second
    /// skipping blocks across the trim as after an overrun, read the same
    /// pre-strip peaks, bit for bit, in every 100 ms window both meter
    /// whole. The pre-strip meter reads the tone's peak before the trim
    /// and after it alike, while the input's meter after the strip reads
    /// it 20 dB down. Fails with the copy into `prebuf` moved after
    /// `run_strip` in `Slot::finish`, the pre-strip meter then reading the
    /// trim.
    #[test]
    fn a_pre_strip_meter_reads_the_input_before_its_trim_across_a_skip() {
        let mut config = tone_config();
        config.inputs[0].pre_strip_meter = true;
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let b = u64::from(config.block_size);
        assert!(4800 % b == 0);
        let per_window = 4800 / b;
        // The trim at the start of window 10; the skip from window 9 to
        // window 12, so the loop skipping it meters window 12 on whole.
        let trim_at = 10 * 4800;
        let skip = 9 * per_window..12 * per_window;
        let run = |skip: &std::ops::Range<u64>| {
            let (hashes, _) = rtrb::RingBuffer::new(1);
            let (_, control) = rtrb::RingBuffer::new(1);
            let mut rt = replay_rt(&config, &api, Vec::new(), hashes, control);
            rt.replay = false;
            (rt.buses, rt.taps, rt.pre_taps) = new_meters(&config);
            let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
            rt.meters = meters;
            let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
            rt.duck_meters = ducks;
            let (ret, _back) = rtrb::RingBuffer::new(64);
            rt.ret = ret;
            let params = ToneParams {
                frequency_hz: 1000,
                level_db: -6.0,
                ident: false,
            };
            let on = Change::PreStripMeter {
                slot: 0,
                enabled: true,
            };
            let entries = [
                (0, Change::Tone { slot: 0, params }),
                (0, on),
                (
                    trim_at,
                    Change::Strip {
                        slot: 0,
                        param: StripParam::Trim { db: -20.0 },
                    },
                ),
            ];
            for (id, (at_index, change)) in entries.into_iter().enumerate() {
                assert!(rt.mix.push(RtEntry {
                    id: id as u64 + 1,
                    at_index,
                    change,
                }));
            }
            let (tap, pre) = (1, 2);
            let weights = || config.inputs[0].loudness_weights().unwrap();
            let (mut post_l, mut pre_l) = (
                OutputLoudness::new(weights()),
                OutputLoudness::new(weights()),
            );
            let mut windows = std::collections::BTreeMap::new();
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            for k in (0..30 * per_window).filter(|k| !skip.contains(k)) {
                rt.process(k, u64::MAX);
                while let Ok(r) = rx.pop() {
                    match r.meter {
                        m if m == tap => post_l.record(&r),
                        m if m == pre => pre_l.record(&r),
                        m => assert_eq!(m, 0),
                    }
                }
                if let (Some(w), Some(v)) = (pre_l.meters.window, post_l.meters.window) {
                    assert_eq!(w, v);
                    windows.insert(w, (bits(&pre_l.meters.peak), bits(&post_l.meters.peak)));
                }
            }
            windows
        };
        let whole = run(&(0..0));
        let skipped = run(&skip);
        assert!(!skipped.contains_key(&11) && skipped.contains_key(&12));
        let level = whole[&5].0.clone();
        let db = |v: f32| 20.0 * f64::from(v).log10();
        for (w, (pre, post)) in &whole {
            if *w >= 1 {
                assert_eq!(*pre, level, "window {w}");
            }
            // Window 10 holds the trim's ramp.
            if *w >= 11 {
                for (p, q) in pre.iter().zip(post) {
                    let (p, q) = (f32::from_bits(*p), f32::from_bits(*q));
                    assert!((db(p) - db(q) - 20.0).abs() < 0.01, "window {w}");
                }
            } else if (1..10).contains(w) {
                assert_eq!(pre, post, "window {w}");
            }
            if let Some(s) = skipped.get(w) {
                assert_eq!(s.0, *pre, "window {w}");
            }
        }
    }

    /// [`tone_config`] at a ramp of 0, its input with a delay line of up
    /// to 9600 samples.
    fn delayed_tone_config() -> Config {
        let mut config = tone_config();
        config.ramp_samples = 0;
        config.inputs[0].max_delay_samples = 9600;
        config
    }

    /// The default tone's sample at index `n`, silence below 0.
    fn default_tone(n: i64) -> f32 {
        let p = ToneParams::DEFAULT;
        u64::try_from(n).map_or(0.0, |n| {
            samplez_dsp::tone::level_gain(p.level_db)
                * samplez_dsp::tone::sine(n, p.frequency_hz, 48_000)
        })
    }

    /// A loop on `config` metering as the engine's does, the matrix
    /// coefficients `ones` set to 1, run with `changes` over blocks
    /// `0..blocks`: the loop, each block's output legs, and the peaks of
    /// the input's pre-strip meter per 100 ms window, none without
    /// `preStripMeter`.
    #[allow(clippy::type_complexity)]
    fn delayed_tone(
        config: &Config,
        ones: &[(usize, usize)],
        changes: Vec<(u64, Change)>,
        blocks: u64,
    ) -> (
        Rt,
        Vec<[Vec<f32>; 2]>,
        std::collections::BTreeMap<u64, Vec<f32>>,
    ) {
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(config, &api, Vec::new(), hashes, control);
        rt.replay = false;
        (rt.buses, rt.taps, rt.pre_taps) = new_meters(config);
        let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
        rt.meters = meters;
        let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
        rt.duck_meters = ducks;
        let (ret, _back) = rtrb::RingBuffer::new(64);
        rt.ret = ret;
        for &(row, column) in ones {
            assert!(rt.mix.matrix.set(row, column, 1.0, 0));
        }
        for (id, (at_index, change)) in changes.into_iter().enumerate() {
            assert!(rt.mix.push(RtEntry {
                id: id as u64 + 1,
                at_index,
                change,
            }));
        }
        let b = config.block_size as usize;
        // The outputs' meters, the input's, then its pre-strip meter.
        let pre = config.outputs.len() as u32 + 1;
        let mut l = OutputLoudness::new(config.inputs[0].loudness_weights().unwrap());
        let (mut legs, mut windows) = (Vec::new(), std::collections::BTreeMap::new());
        for k in 0..blocks {
            rt.process(k, u64::MAX);
            legs.push([0, 1].map(|c| rt.outbuf.channel(c)[..b].to_vec()));
            while let Ok(r) = rx.pop() {
                if r.meter == pre {
                    l.record_restarting(&r);
                }
            }
            if let Some(w) = l.meters.window {
                windows.insert(w, l.meters.peak.clone());
            }
        }
        assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
        (rt, legs, windows)
    }

    /// An input with `preStripRows`, trimmed by 20 dB in its strip, delayed
    /// by 1000 samples from index D: an output leg fed only by the
    /// pre-strip row of its second channel is the tone untrimmed and
    /// delayed, bit for bit: the tone before D, silent for the 999 samples
    /// the delay reaches back over, then the tone 1000 samples late. Fails
    /// with the delay run after the copy into the pre-strip rows in
    /// `Slot::finish`, the row then undelayed.
    #[test]
    fn a_pre_strip_row_carries_the_input_as_late_as_its_delay() {
        let mut config = delayed_tone_config();
        config.inputs[0].pre_strip_rows = true;
        config.matrix = vec![vec![0.0; 2]; 4];
        let b = u64::from(config.block_size);
        let (n, d) = (1000, 100 * b);
        let trim = StripParam::Trim { db: -20.0 };
        let changes = vec![
            (
                0,
                Change::Strip {
                    slot: 0,
                    param: trim,
                },
            ),
            (
                d,
                Change::Delay {
                    slot: 0,
                    samples: n as u32,
                },
            ),
        ];
        let blocks = (d + n) / b + 100;
        let (_, legs, _) = delayed_tone(&config, &[(0, 0), (3, 1)], changes, blocks);
        for (k, [_, leg]) in legs.iter().enumerate() {
            for (j, &y) in leg.iter().enumerate() {
                let i = k as u64 * b + j as u64;
                let want = match i {
                    _ if i < d => default_tone(i as i64),
                    _ if i < d + n - 1 => 0.0,
                    _ => default_tone(i as i64 - n as i64),
                };
                assert_eq!(y.to_bits(), want.to_bits(), "at {i}");
            }
        }
    }

    /// An input metered before its strip with `preStripMeter`, delayed by
    /// 7200 samples from index 0, its tone stepping from -40 to -6 dB at
    /// the start of window 10: the pre-strip meter reads the step 7200
    /// samples later, in window 11, and window 10 at -40 dB. Fails with
    /// the delay run after the copy into `prebuf` in `Slot::finish`, the
    /// meter then reading the step in window 10.
    #[test]
    fn a_pre_strip_meter_reads_the_input_as_late_as_its_delay() {
        let mut config = delayed_tone_config();
        config.inputs[0].pre_strip_meter = true;
        let (n, step): (u32, u64) = (7200, 10 * 4800);
        let level = |level_db| ToneParams {
            level_db,
            ..ToneParams::DEFAULT
        };
        let tone = |params| Change::Tone { slot: 0, params };
        let on = Change::PreStripMeter {
            slot: 0,
            enabled: true,
        };
        let changes = vec![
            (0, on),
            (
                0,
                Change::Delay {
                    slot: 0,
                    samples: n,
                },
            ),
            (0, tone(level(-40.0))),
            (step, tone(level(-6.0))),
        ];
        let blocks = 15 * 4800 / u64::from(config.block_size);
        let (_, _, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
        // The largest magnitude of the tone delayed by `n` over window `w`.
        let peak = |w: u64| {
            let p = ToneParams::DEFAULT;
            (w * 4800 - u64::from(n)..(w + 1) * 4800 - u64::from(n))
                .map(|i| {
                    let db = if i < step { -40.0 } else { -6.0 };
                    let x = samplez_dsp::tone::sine(i, p.frequency_hz, 48_000);
                    (samplez_dsp::tone::level_gain(db) * x).abs()
                })
                .fold(0.0f32, f32::max)
        };
        assert!(peak(11) > 10.0 * peak(10));
        for w in 2..14 {
            let (peaks, want) = (&windows[&w], peak(w));
            assert!(
                peaks.iter().all(|p| (p - want).abs() < 1e-6),
                "window {w}: {peaks:?}, {want}"
            );
        }
    }

    /// An input delayed by 1000 samples from index 0 and trimmed by 20 dB
    /// in its strip from index X: the trim applies at X to the delayed
    /// tone there, the input's sample at X - 1000, as every tap of the
    /// input is as late as its delay. Before X each leg is the tone 1000
    /// samples late, bit for bit; from X on, 20 dB down. Fails with the
    /// delay run after the strip in `Slot::finish`, the legs then stepping
    /// at X + 1000.
    #[test]
    fn a_strip_change_applies_to_the_delayed_input_at_its_index() {
        let config = delayed_tone_config();
        let b = u64::from(config.block_size);
        let (n, x) = (1000, 200 * b);
        let trim = StripParam::Trim { db: -20.0 };
        let changes = vec![
            (
                0,
                Change::Delay {
                    slot: 0,
                    samples: n as u32,
                },
            ),
            (
                x,
                Change::Strip {
                    slot: 0,
                    param: trim,
                },
            ),
        ];
        let blocks = (x + n) / b + 100;
        let (_, legs, _) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
        for (k, block) in legs.iter().enumerate() {
            for (c, leg) in block.iter().enumerate() {
                for (j, &y) in leg.iter().enumerate() {
                    let i = k as u64 * b + j as u64;
                    let want = default_tone(i as i64 - n as i64);
                    if i < x {
                        assert_eq!(y.to_bits(), want.to_bits(), "leg {c} at {i}");
                    } else {
                        assert!((y - 0.1 * want).abs() < 1e-6, "leg {c} at {i}: {y}");
                    }
                }
            }
        }
    }

    /// An input with a delay line and neither `preStripMeter` nor
    /// `preStripRows` has no pre-strip meter, rows or copy: the matrix has
    /// the input's rows alone, and its slot copies nothing into `prebuf`,
    /// which stays silent through a delay and a trim. Fails with the copy
    /// into `prebuf` made whatever [`Slot::pre_strip_on`] says.
    #[test]
    fn without_the_opt_ins_an_input_has_no_pre_strip_copy() {
        let config = delayed_tone_config();
        assert_eq!(config.rows(), config.input_channels());
        let changes = vec![
            (
                0,
                Change::Delay {
                    slot: 0,
                    samples: 1000,
                },
            ),
            (
                4800,
                Change::Strip {
                    slot: 0,
                    param: StripParam::Trim { db: -20.0 },
                },
            ),
        ];
        let (rt, legs, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, 300);
        assert!(!rt.slots[0].pre_strip && rt.slots[0].pre_row.is_none());
        assert!(rt.pre_taps.is_empty() && windows.is_empty());
        assert!(legs.last().unwrap()[0].iter().any(|&y| y != 0.0));
        for c in 0..rt.prebuf.channels() {
            assert!(
                rt.prebuf.channel(c).iter().all(|&y| y == 0.0),
                "channel {c}"
            );
        }
    }

    /// An input configured with `preStripMeter` and no `PreStripMeter`
    /// entry has its pre-strip meter off: its slot copies nothing into
    /// `prebuf` and the meter sends no record. Fails with the slot's
    /// meter on from the start in `new_slots`, with the copy into
    /// `prebuf` made for every slot with the capability, and with the
    /// pre-strip meters run whether on or not.
    #[test]
    fn a_pre_strip_meter_is_off_until_switched_on() {
        let mut config = delayed_tone_config();
        config.inputs[0].pre_strip_meter = true;
        let (rt, legs, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], Vec::new(), 300);
        assert!(rt.slots[0].pre_strip && !rt.slots[0].pre_strip_on);
        assert_eq!(rt.pre_taps.len(), 1);
        assert!(windows.is_empty(), "{windows:?}");
        assert!(legs.last().unwrap()[0].iter().any(|&y| y != 0.0));
        for c in 0..rt.prebuf.channels() {
            assert!(rt.prebuf.channel(c).iter().all(|&y| y == 0.0), "channel {c}");
        }
    }

    /// A pre-strip meter switched on at window 2, off inside window 5 and
    /// on again at window 8 reads windows 2 to 4 and from 8 on, none of
    /// 5 to 7, each at the tone's level then, bit for bit as metered
    /// whole: the tone at -6 dB before the stop, at -30 dB from window 6
    /// on. Switched on again, the meter starts afresh, so window 8 reads
    /// -30 dB alone and not the part of window 5 metered before the stop.
    /// Fails with the `restart` dropped from `Rt::set_pre_strip_meter`,
    /// window 8 then reading -6 dB, and with the meter left running when
    /// switched off.
    #[test]
    fn a_pre_strip_meter_meters_exactly_where_it_is_on() {
        let mut config = delayed_tone_config();
        config.inputs[0].pre_strip_meter = true;
        let w = 4800u64;
        let set = |enabled| Change::PreStripMeter { slot: 0, enabled };
        let level = |level_db| Change::Tone {
            slot: 0,
            params: ToneParams {
                level_db,
                ..ToneParams::DEFAULT
            },
        };
        let changes = vec![
            (0, level(-6.0)),
            (2 * w, set(true)),
            (5 * w + 960, set(false)),
            (6 * w, level(-30.0)),
            (8 * w, set(true)),
        ];
        let blocks = 12 * w / u64::from(config.block_size);
        let (_, _, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
        let metered: Vec<u64> = windows.keys().copied().collect();
        assert_eq!(metered, [2, 3, 4, 8, 9, 10, 11]);
        let peak = |w: u64, db: f32| {
            let p = ToneParams::DEFAULT;
            (w * 4800..(w + 1) * 4800)
                .map(|i| {
                    let x = samplez_dsp::tone::sine(i, p.frequency_hz, 48_000);
                    (samplez_dsp::tone::level_gain(db) * x).abs()
                })
                .fold(0.0f32, f32::max)
        };
        for (&w, peaks) in &windows {
            let want = peak(w, if w < 6 { -6.0 } else { -30.0 });
            assert!(peaks.iter().all(|&p| p == want), "window {w}: {peaks:?} {want}");
        }
    }

    /// Tone parameters are checked against the configuration: a slot
    /// that reads flows, no slot, and a frequency or level out of range
    /// are refused, and a tone generator's slot reads no flow. Fails with
    /// `params.check()` replaced by `Ok(())` in `check_tone`.
    #[test]
    fn tone_parameters_are_checked_against_the_configuration() {
        let mut config = tone_config();
        let flow: crate::config::Input = serde_json::from_value(serde_json::json!({
            "name": "in", "domain": "d",
            "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d21", "channels": 1
        }))
        .unwrap();
        config.inputs.push(flow);
        let ok = ToneParams::DEFAULT;
        assert_eq!(check_tone(&config, 0, &ok), Ok(()));
        let refused = |slot, p: ToneParams| check_tone(&config, slot, &p).unwrap_err();
        assert!(refused(1, ok).contains("no tone generator"));
        assert!(refused(2, ok).contains("no input slot 2"));
        for p in [
            ToneParams {
                frequency_hz: 19,
                ..ok
            },
            ToneParams {
                frequency_hz: 20_001,
                ..ok
            },
            ToneParams {
                level_db: 0.5,
                ..ok
            },
            ToneParams {
                level_db: -60.5,
                ..ok
            },
            ToneParams {
                level_db: f32::NAN,
                ..ok
            },
        ] {
            assert!(refused(0, p).contains("outside"), "{p:?}");
        }
        let e = config.check_input_flow(0, "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d22");
        assert!(e.unwrap_err().0.contains("tone generator"));
        assert!(config.check_flow_slot(1).is_ok());
    }

    /// Without a core a strip entry whose index passed still reaches its
    /// slot's strip, which the slot keeps across the drop: a core built
    /// from product state holds no strip, so the entry would be lost.
    #[test]
    fn a_strip_entry_due_without_a_core_reaches_the_strip() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("no-core-strip", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d09", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0a", "label": "strip",
                          "group": "strip", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let param = StripParam::Trim { db: -6.0 };
        let entry = |id, at_index, change| RtEntry {
            id,
            at_index,
            change,
        };
        assert!(rt.mix.push(entry(1, 48, Change::Strip { slot: 0, param })));
        assert!(rt.mix.push(entry(2, 96, Change::InputDown { slot: 0 })));
        assert!(rt.mix.push(entry(3, 960, Change::InputDown { slot: 0 })));
        rt.pass_entries(2);
        let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
        assert_eq!(ids, [3]);
        assert_eq!(rt.mix.last_taken, 2);
        assert!(rt.slots[0].up, "an entry other than a strip entry applied");
        let mut reference = samplez_dsp::Strip::new(1, config.ramp_samples);
        assert!(reference.set(param));
        assert_eq!(rt.slots[0].strip.as_ref(), Some(&reference));
        assert_eq!(rt.slots[0].strip_pos, Some(48));
        assert_eq!(rt.counters.entries_without_core.load(Relaxed), 2);

        // A strip entry the core knew, pending here, whose index the core
        // passed: a product core holds no strip, so it applies at the
        // install.
        let gain = StripParam::Trim { db: -3.0 };
        assert!(rt.mix.push(entry(
            4,
            192,
            Change::Strip {
                slot: 0,
                param: gain
            }
        )));
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        rt.install_core(&mut core, 4, false);
        assert!(rt.mix.pending.is_empty());
        assert_eq!(rt.slots[0].strip_pos, Some(192));
        let mut set = samplez_dsp::Strip::new(1, config.ramp_samples);
        assert!(set.set(param));
        set.advance(192 - 48);
        assert!(set.set(gain));
        assert_eq!(rt.slots[0].strip.as_ref(), Some(&set));
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Without a core an output delay entry whose index passed still
    /// reaches the output's line, which the output keeps across the drop,
    /// as a slot's delay entry reaches the slot's; one the core knew whose
    /// index it passed applies at the install of a core built from product
    /// state, which holds no line, and waits for the crossfade the first
    /// started.
    #[test]
    fn an_output_delay_entry_due_without_a_core_reaches_the_line() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("no-core-output-delay", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d15", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d16", "label": "delay",
                          "group": "delay", "role": "Audio", "channels": 1,
                          "maxDelaySamples": 4800 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let entry = |id, at_index, change| RtEntry {
            id,
            at_index,
            change,
        };
        let out = |samples| Change::OutputDelay { output: 0, samples };
        assert!(rt.mix.push(entry(1, 48, out(960))));
        assert!(rt.mix.push(entry(2, 960, Change::InputDown { slot: 0 })));
        rt.pass_entries(2);
        let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
        assert_eq!(ids, [2]);
        let taps = |rt: &Rt| rt.outs[0].delay.as_ref().map(Delay::taps);
        let fading = samplez_dsp::Taps {
            from: 0,
            to: 960,
            start: 48,
            next: None,
        };
        assert_eq!(taps(&rt), Some(fading));

        assert!(rt.mix.push(entry(3, 192, out(480))));
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        rt.install_core(&mut core, 3, false);
        assert!(rt.mix.pending.is_empty());
        let waiting = samplez_dsp::Taps {
            next: Some(480),
            ..fading
        };
        assert_eq!(taps(&rt), Some(waiting));
        assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Without a core an output strip entry whose index passed still
    /// reaches the output's strip, which the output keeps across the drop,
    /// as a slot's strip entry reaches the slot's; one the core knew whose
    /// index it passed applies at the install of a core built from product
    /// state, which holds no strip.
    #[test]
    fn an_output_strip_entry_due_without_a_core_reaches_the_strip() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("no-core-output-strip", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d13", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d14", "label": "strip",
                          "group": "strip", "role": "Audio", "channels": 1, "strip": true }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let param = StripParam::Trim { db: -6.0 };
        let entry = |id, at_index, change| RtEntry {
            id,
            at_index,
            change,
        };
        let out = |param| Change::OutputStrip { output: 0, param };
        assert!(rt.mix.push(entry(1, 48, out(param))));
        assert!(rt.mix.push(entry(2, 960, Change::InputDown { slot: 0 })));
        rt.pass_entries(2);
        let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
        assert_eq!(ids, [2]);
        let mut reference = Strip::new(1, config.ramp_samples);
        assert!(reference.set(param));
        assert_eq!(rt.outs[0].strip.as_ref(), Some(&reference));
        assert_eq!(rt.outs[0].strip_pos, Some(48));

        let gain = StripParam::Trim { db: -3.0 };
        assert!(rt.mix.push(entry(3, 192, out(gain))));
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        rt.install_core(&mut core, 3, false);
        assert!(rt.mix.pending.is_empty());
        let mut set = Strip::new(1, config.ramp_samples);
        assert!(set.set(param));
        set.advance(192 - 48);
        assert!(set.set(gain));
        assert_eq!(rt.outs[0].strip.as_ref(), Some(&set));
        assert_eq!(rt.outs[0].strip_pos, Some(192));
        assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A product core that knew less than an earlier replayed one: a
    /// strip entry the replayed core's strips hold, delivered by the ring
    /// only after both, is not applied again, which would restart its
    /// ramp; a gain entry the product core did not know is taken as new,
    /// not skipped as known, which would lose it.
    #[test]
    fn a_product_core_knowing_less_than_a_replayed_one_loses_and_repeats_nothing() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("product-below-replay", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d11", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d12", "label": "below",
                          "group": "below", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let (mut ring, entries) = rtrb::RingBuffer::new(4);
        rt.entries = entries;
        let hpf = StripParam::Hpf {
            enabled: true,
            freq: 200.0,
        };
        let strip = RtEntry {
            id: 3,
            at_index: 48,
            change: Change::Strip {
                slot: 0,
                param: hpf,
            },
        };
        let gain = RtEntry {
            id: 2,
            at_index: 1440,
            change: Change::Gain {
                input: 0,
                output: 0,
                gain: 0.5,
                fade: None,
            },
        };
        // The replayed core knew entries up to 3 and applied the strip
        // entry at 48; its strip, 432 samples into the ramp, is installed.
        let mut replayed = samplez_dsp::Strip::new(1, config.ramp_samples);
        assert!(replayed.set(hpf));
        replayed.advance(480 - 48);
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        core.slots[0].strip = Some(replayed.clone());
        core.slots[0].strip_pos = Some(480);
        rt.install_core(&mut core, 3, true);
        assert_eq!(rt.slots[0].strip.as_ref(), Some(&replayed));
        // A product core that knew entries up to 1 only.
        rt.capture(&mut core);
        core.pos = Some(960);
        core.pending.clear();
        rt.install_core(&mut core, 1, false);
        // The ring delivers both now.
        let one = |entry| RingEntry { entry, batch: None };
        assert!(ring.push(one(strip)).is_ok() && ring.push(one(gain)).is_ok());
        rt.take_entries();
        assert_eq!(rt.slots[0].strip.as_ref(), Some(&replayed), "applied again");
        assert!(
            rt.mix.holds(2),
            "the gain entry the product core missed is lost"
        );
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// An entry consumed through a replayed core counts as taken, whether
    /// the RT thread held it pending at the install, which drops it as
    /// known, or the ring delivers it after, which skips it as known: a
    /// product core that knew less, installed next, would lose it from
    /// the matrix for good, so the hand-over check
    /// (`last_taken > last_id`) refuses that core as Missed.
    #[test]
    fn an_entry_taken_through_a_replayed_core_refuses_a_core_knowing_less() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("taken-through-core", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d13", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d14", "label": "taken",
                          "group": "taken", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let (mut ring, entries) = rtrb::RingBuffer::new(4);
        rt.entries = entries;
        let gain = |id, at_index| RtEntry {
            id,
            at_index,
            change: Change::Gain {
                input: 0,
                output: 0,
                gain: 0.5,
                fade: None,
            },
        };
        // Entry 2 pending here, entry 3 still in the ring: the replayed
        // core knew both and applied them.
        assert!(rt.mix.push(gain(2, 48)));
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        rt.install_core(&mut core, 3, true);
        assert!(rt.mix.pending.is_empty());
        assert_eq!(rt.mix.last_taken, 2, "dropped at the install");
        // Delivered between the replayed install and the next one.
        assert!(
            ring.push(RingEntry {
                entry: gain(3, 96),
                batch: None
            })
            .is_ok()
        );
        rt.take_entries();
        assert!(rt.mix.pending.is_empty());
        assert_eq!(rt.mix.last_taken, 3, "skipped as known");
        // A product core that knew entries up to 1 is refused.
        assert!(rt.mix.last_taken > 1);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A strip entry the replayed strips hold, delivered by the ring after
    /// the install, counts as taken: another replayed core that knew
    /// less would replace the strips with ones lacking it, so the
    /// hand-over check (`last_taken > last_id`) refuses that core.
    #[test]
    fn a_strip_entry_held_by_the_strips_refuses_a_replayed_core_knowing_less() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("strip-taken", 1);
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d15", "channels": 1 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d16", "label": "held",
                          "group": "held", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let (mut ring, entries) = rtrb::RingBuffer::new(4);
        rt.entries = entries;
        let (inputs, outputs) = (config.input_channels(), config.output_channels());
        let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
        rt.capture(&mut core);
        core.pos = Some(480);
        core.pending.clear();
        rt.install_core(&mut core, 3, true);
        // Strip entry 3, applied by the replay, delivered after the install.
        let strip = RtEntry {
            id: 3,
            at_index: 48,
            change: Change::Strip {
                slot: 0,
                param: StripParam::Trim { db: -6.0 },
            },
        };
        assert!(
            ring.push(RingEntry {
                entry: strip,
                batch: None
            })
            .is_ok()
        );
        rt.take_entries();
        assert!(rt.mix.pending.is_empty());
        assert_eq!(rt.mix.last_taken, 3, "skipped as held by the strips");
        // A replayed core that knew entries up to 2 is refused.
        assert!(rt.mix.last_taken > 2);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A replay RT thread of one input of two legs and one mono output,
    /// on flows named by `tag`, taking entries from the returned ring.
    fn rt_of(e: &Engine, domain: &std::path::Path, tag: &str) -> (Rt, rtrb::Producer<RingEntry>) {
        let v = serde_json::json!({
            "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
                .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c{tag}"),
                         "channels": 2 }],
            "outputs": [{ "name": "out", "domain": "d",
                          "flowId": format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6d{tag}"),
                          "label": tag, "group": tag, "role": "Audio", "channels": 1 }],
            "matrix": [[1.0], [1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
        let (ring, entries) = rtrb::RingBuffer::new(PENDING_CHANGES);
        rt.entries = entries;
        (rt, ring)
    }

    /// A loop for the skip oracle ([`slice_flags`]): its slots and their
    /// channels, those starting detached, its outputs' and configured
    /// meters' channels, the coefficients set at the start as (row,
    /// column, gain), and its entries as (block, items), one item an
    /// entry of its own and more a folded batch, with the block a late
    /// one is pushed at, if any.
    struct Scene {
        slots: Vec<u32>,
        detached: Vec<usize>,
        outputs: Vec<u32>,
        meters: Vec<u32>,
        gains: Vec<(usize, usize, f32)>,
        entries: Vec<(u64, Vec<Item>)>,
        late: Option<(u64, u64, Change)>,
        blocks: u64,
    }

    /// The flags of each slice of each meter, by (meter, slice), as `s`'s
    /// loop sends them at a block of 64 samples, which does not divide a
    /// slice, with the blocks of `skipped` not computed, and held
    /// diverged as it computes the block after them when `diverged`.
    fn slice_flags(
        s: &Scene,
        skipped: std::ops::Range<u64>,
        diverged: bool,
    ) -> std::collections::BTreeMap<(u32, u64), u8> {
        (slice_records(s, skipped, diverged).into_iter())
            .map(|(k, (flags, _))| (k, flags))
            .collect()
    }

    /// [`slice_flags`] with the reset each slice carries.
    #[allow(clippy::type_complexity)]
    fn slice_records(
        s: &Scene,
        skipped: std::ops::Range<u64>,
        diverged: bool,
    ) -> std::collections::BTreeMap<(u32, u64), (u8, Option<u64>)> {
        use crate::rt_loop::{Metered, SPAN};
        const B: u64 = 64;
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let rows: u32 = s.slots.iter().sum();
        let columns: u32 = s.outputs.iter().chain(&s.meters).sum();
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": B,
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": s.slots.iter().enumerate().map(|(i, &ch)| {
                let mut v = serde_json::json!({ "name": format!("in{i}"), "domain": "d",
                    "channels": ch });
                if !s.detached.contains(&i) {
                    v["flowId"] = format!("5fbec3b1-1b0f-417d-9059-00000000000{i}").into();
                }
                v
            }).collect::<Vec<_>>(),
            "outputs": s.outputs.iter().enumerate().map(|(o, &ch)| serde_json::json!({
                "name": format!("out{o}"), "domain": "d",
                "flowId": format!("6fbec3b1-1b0f-417d-9059-00000000000{o}"), "label": "o",
                "group": "g", "role": "Audio", "channels": ch }))
                .collect::<Vec<_>>(),
            "meters": s.meters.iter().enumerate().map(|(m, &ch)| serde_json::json!({
                "name": format!("m{m}"), "channels": ch }))
                .collect::<Vec<_>>(),
            "matrix": vec![vec![0.0; columns as usize]; rows as usize],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let api = samplez_mxl::load(std::path::Path::new(&libmxl)).unwrap();
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &api, vec![None], hashes, control);
        // A replay loop meters its outputs only: the configured meters
        // are added as an engine's loop holds them.
        let mut first: usize = s.outputs.iter().map(|&c| c as usize).sum();
        for &ch in &s.meters {
            rt.buses.push(Metered {
                first_channel: first,
                meter: crate::rt_loop::OutMeter::new(ch as usize),
            });
            first += ch as usize;
        }
        for &(row, column, gain) in &s.gains {
            assert!(rt.mix.matrix.set(row, column, gain, 0));
        }
        let (tx, mut rx) = rtrb::RingBuffer::new(1 << 14);
        rt.meters = tx;
        let coefficients = (rows * columns) as usize;
        for (n, (k, items)) in s.entries.iter().enumerate() {
            let entry = |change| RtEntry {
                id: n as u64 + 1,
                at_index: k * B,
                change,
            };
            match items[..] {
                [Item::Change(change)] => assert!(rt.mix.push(entry(change))),
                _ => {
                    let (change, batch) = batch::fold(items, columns as usize, coefficients)
                        .unwrap()
                        .unwrap();
                    assert!(rt.mix.push_with(entry(change), batch).is_ok());
                }
            }
        }
        let shared = Arc::clone(&rt.replica.as_ref().unwrap().shared);
        for k in 0..s.blocks {
            if let Some((at, block, change)) = s.late.filter(|l| l.0 == k) {
                let id = s.entries.len() as u64 + 1;
                assert!(rt.mix.push(RtEntry {
                    id,
                    at_index: block * B,
                    change,
                }));
                let _ = at;
            }
            if skipped.contains(&k) {
                continue;
            }
            let held = diverged && k == skipped.end;
            shared.diverged.store(held, Relaxed);
            rt.process(k, u64::MAX);
            shared.diverged.store(false, Relaxed);
        }
        let mut records = std::collections::BTreeMap::new();
        while let Ok(r) = rx.pop() {
            if r.flags & SPAN != 0 {
                for slice in r.slice..r.slice + u64::from(r.frames) {
                    // A span's reset is its first slice's.
                    let reset = r.reset().filter(|_| slice == r.slice);
                    records.insert((r.meter, slice), (r.flags & !SPAN, reset));
                }
            } else if r.channel == 0 {
                records.insert((r.meter, r.slice), (r.flags, r.reset()));
            }
        }
        records
    }

    /// The gap flags of `flags`.
    fn gaps_of(
        flags: &std::collections::BTreeMap<(u32, u64), u8>,
    ) -> std::collections::BTreeMap<(u32, u64), bool> {
        use crate::rt_loop::GAP;
        flags.iter().map(|(&k, &f)| (k, f & GAP != 0)).collect()
    }

    /// Three mono slots, slots 0 and 1 into output 0 and slot 2 into
    /// output 1, over 80 blocks. Entries: slot 0 down at block 12, its
    /// gain to output 0 to 0 at 25, a batch taking slot 2 down with a
    /// gain at 35, slot 2 detached at 50; and, as block 70 is due, an
    /// `InputDown` of slot 1 at block 4, an index already produced, which
    /// is late and applies nowhere.
    fn base_scene() -> Scene {
        let gain = |input, output, gain| {
            Item::Change(Change::Gain {
                input,
                output,
                gain,
                fade: None,
            })
        };
        let down = |slot| Item::Change(Change::InputDown { slot });
        Scene {
            slots: vec![1, 1, 1],
            detached: Vec::new(),
            outputs: vec![1, 1],
            meters: Vec::new(),
            gains: vec![(0, 0, 1.0), (1, 0, 1.0), (2, 1, 1.0)],
            entries: vec![
                (12, vec![down(0)]),
                (25, vec![gain(0, 0, 0.0)]),
                (35, vec![down(2), gain(2, 1, 0.5)]),
                (50, vec![Item::Change(Change::Detach { slot: 2 })]),
            ],
            late: Some((70, 4, Change::InputDown { slot: 1 })),
            blocks: 80,
        }
    }

    /// A stereo slot 0 and mono slots 1 to 3, slot 3 starting detached,
    /// into a stereo output 0 (columns 0 and 1), a mono output 1 (column
    /// 2) and a mono configured meter (column 3), over 90 blocks:
    /// - block 3: slots 0 and 1 down, two entries at one index; slot 0
    ///   feeds output 0, so a skip from here or later starts in a gap;
    /// - 5: a gain of slot 0 into output 0, at the start of a skip there;
    /// - 10: slot 1's gain to the meter from 0 to 0.5, while it is down;
    /// - 20: slot 0 up;
    /// - 30: a batch raising slot 1 with a gain;
    /// - 40: slot 2 down, feeding output 1;
    /// - 45: slot 2's gain to 0 by a fade delayed 300 samples;
    /// - 52: slot 3 down, detached; 55: slot 3 re-attached by a `Route`;
    /// - 58: slot 2's gain back to 0.7 by an absolute fade from 0;
    /// - 60: slot 3 up and slot 0 down again, at one index;
    /// - 70: slot 1 down, at the end of a skip there.
    ///
    /// Output 0 and output 1 are in a gap as a skip up to block 70 ends.
    fn moving_scene() -> Scene {
        let b = 64;
        let gain = |input, output, gain, fade| {
            Item::Change(Change::Gain {
                input,
                output,
                gain,
                fade,
            })
        };
        let down = |slot| Item::Change(Change::InputDown { slot });
        let up = |slot| Item::Change(Change::InputUp { slot });
        Scene {
            slots: vec![2, 1, 1, 1],
            detached: vec![3],
            outputs: vec![2, 1],
            meters: vec![1],
            gains: vec![(1, 1, 1.0), (3, 2, 1.0), (4, 3, 1.0)],
            entries: vec![
                (3, vec![down(0)]),
                (3, vec![down(1)]),
                (5, vec![gain(0, 0, 0.5, None)]),
                (10, vec![gain(2, 3, 0.5, None)]),
                (20, vec![up(0)]),
                (30, vec![up(1), gain(2, 3, 0.25, None)]),
                (40, vec![down(2)]),
                (
                    45,
                    vec![gain(3, 2, 0.0, Some(Fade::after(1.0, 45 * b, 300, 200)))],
                ),
                (52, vec![down(3)]),
                (55, vec![Item::Change(Change::Route { slot: 3 })]),
                (
                    58,
                    vec![gain(3, 2, 0.7, Some(Fade::after(0.0, 58 * b, 0, 400)))],
                ),
                (60, vec![up(3)]),
                (60, vec![down(0)]),
                (70, vec![down(1)]),
            ],
            late: None,
            blocks: 90,
        }
    }

    /// Slices an overrun skipped carry the gap flags the same slices carry
    /// on a loop that computed them: over an `InputDown`, a gain set to 0,
    /// a batch taking a slot down and a `Detach` inside the skip, a late
    /// entry that applies on neither, and a block size that does not
    /// divide a slice.
    #[test]
    fn skipped_slices_carry_the_gaps_computed_ones_do() {
        let s = base_scene();
        let computed = gaps_of(&slice_flags(&s, 0..0, false));
        let skipped = gaps_of(&slice_flags(&s, 5..70, false));
        // The run up to block 80 covers slices 0 to 10.
        assert!(computed.len() >= 20, "{computed:?}");
        assert_eq!(skipped, computed);
        let at = |m: u32, k: u64| computed[&(m, k * 64 / SLICE)];
        assert!(
            at(0, 15) && !at(0, 30),
            "output 0: slot 0 down, then its gain 0"
        );
        assert!(
            !at(1, 25) && at(1, 40) && !at(1, 60),
            "output 1: slot 2 down, detached"
        );
    }

    /// The same over gains from 0 to other than 0 on a slot down, an
    /// `InputUp` and a batch raising a slot, a `Route` re-attaching a
    /// detached slot, absolute fades, delayed and not, a configured meter
    /// and a stereo slot and output, skips starting and ending in a gap,
    /// entries at a skip's first and end index and two at one index.
    #[test]
    fn skipped_slices_carry_the_gaps_computed_ones_do_as_targets_move() {
        let s = moving_scene();
        let computed = gaps_of(&slice_flags(&s, 0..0, false));
        let at = |m: u32, k: u64| computed[&(m, k * 64 / SLICE)];
        assert!(at(0, 8) && !at(0, 25) && at(0, 65), "output 0");
        assert!(
            !at(1, 30) && at(1, 42) && !at(1, 47) && at(1, 65),
            "output 1"
        );
        assert!(
            !at(2, 5) && at(2, 15) && !at(2, 35) && at(2, 57) && !at(2, 65),
            "meter"
        );
        for skip in [5..70, 3..70, 11..62, 1..89] {
            assert_eq!(
                gaps_of(&slice_flags(&s, skip.clone(), false)),
                computed,
                "{skip:?}"
            );
        }
    }

    /// A loop held diverged as it computes the block after a skip sends
    /// the skipped slices diverged, spans included, with the gap flags of
    /// the slices computed: diverged wins over gap in the meters.
    #[test]
    fn skipped_slices_carry_the_divergence_of_the_block_after() {
        use crate::rt_loop::DIVERGED;
        let s = moving_scene();
        let computed = gaps_of(&slice_flags(&s, 0..0, false));
        let flags = slice_flags(&s, 5..70, true);
        assert_eq!(gaps_of(&flags), computed);
        for (&(m, slice), &f) in &flags {
            let within = (slice + 1) * SLICE > 5 * 64 && slice * SLICE < 71 * 64;
            assert_eq!(f & DIVERGED != 0, within, "meter {m} slice {slice}");
        }
    }

    /// A mono slot into a mono output (meter 0) and a mono slot into a
    /// mono configured meter (meter 1), over 240 blocks of 64 samples, a
    /// slice 7.5 blocks and a window 75:
    /// - block 4: meter 0 paused, inside slice 0;
    /// - 15: meter 0 run at the start of slice 2, and meter 1 reset there,
    ///   at window 1 (block 75);
    /// - 26: a batch pausing both meters and resetting meter 0 at window
    ///   1, inside slice 3;
    /// - 30: meter 0 run at the start of slice 4; 45: meter 1 run at the
    ///   start of slice 6;
    /// - 76: meter 0 paused inside slice 10, and 80: reset, at window 2
    ///   (block 150); 90: run at the start of slice 12;
    /// - 120: meter 1 reset at window 2; 155: a batch resetting meter 1 at
    ///   window 3 (block 225) and pausing it, inside slice 20.
    fn loudness_scene() -> Scene {
        let run = |meter, running| Item::Change(Change::LoudnessRun { meter, running });
        let reset = |meter| Item::Change(Change::ResetLoudness { meter });
        Scene {
            slots: vec![1, 1],
            detached: Vec::new(),
            outputs: vec![1],
            meters: vec![1],
            gains: vec![(0, 0, 1.0), (1, 1, 1.0)],
            entries: vec![
                (4, vec![run(0, false)]),
                (15, vec![run(0, true)]),
                (15, vec![reset(1)]),
                (26, vec![run(1, false), reset(0), run(0, false)]),
                (30, vec![run(0, true)]),
                (45, vec![run(1, true)]),
                (76, vec![run(0, false)]),
                (80, vec![reset(0)]),
                (90, vec![run(0, true)]),
                (120, vec![reset(1)]),
                (155, vec![reset(1), run(1, false)]),
            ],
            late: None,
            blocks: 240,
        }
    }

    /// A slice carries the paused flag when a sample of it was metered
    /// with its meter paused by the log, whether the `LoudnessRun` lies
    /// inside it or at its start, and the reset of a `ResetLoudness` at the
    /// first window at or after its index, alone or in a batch. Skipped
    /// slices carry the same flags and resets as computed ones, over
    /// skips holding such entries, starting or ending at them, and ending
    /// inside a slice or before a reset's window.
    #[test]
    fn skipped_slices_carry_the_pauses_and_resets_computed_ones_do() {
        use crate::rt_loop::PAUSED;
        let s = loudness_scene();
        let computed = slice_records(&s, 0..0, false);
        let paused = |m: u32, slices: std::ops::Range<u64>| -> Vec<bool> {
            slices.map(|i| computed[&(m, i)].0 & PAUSED != 0).collect()
        };
        assert_eq!(paused(0, 0..6), [true, true, false, true, false, false]);
        assert_eq!(paused(0, 9..13), [false, true, true, false]);
        assert_eq!(paused(1, 2..7), [false, true, true, true, false], "meter 1");
        // Paused from slice 20 to the end.
        let mut tail = vec![true; 13];
        tail[0] = false;
        assert_eq!(paused(1, 19..32), tail);
        let resets: Vec<(u32, u64, u64)> = (computed.iter())
            .filter_map(|(&(m, i), &(_, r))| Some((m, i, r?)))
            .collect();
        assert_eq!(
            resets,
            [
                (0, 10, 4800),
                (0, 20, 9600),
                (1, 10, 4800),
                (1, 20, 9600),
                (1, 30, 14_400)
            ]
        );
        for skip in [
            5..70,
            3..80,
            15..90,
            4..15,
            26..30,
            76..90,
            60..156,
            152..226,
            1..239,
        ] {
            assert_eq!(slice_records(&s, skip.clone(), false), computed, "{skip:?}");
        }
    }

    /// A batch of two gains, a trim and an `InputDown` of slot 0, and
    /// with `up` an `InputUp` of slot 0 after them.
    fn batch_of(up: bool) -> (Change, Option<Arc<Batch>>) {
        let mut changes = vec![
            Change::Gain {
                input: 0,
                output: 0,
                gain: 0.5,
                fade: None,
            },
            Change::Gain {
                input: 1,
                output: 0,
                gain: 0.25,
                fade: None,
            },
            Change::InputDown { slot: 0 },
            Change::Strip {
                slot: 0,
                param: StripParam::Trim { db: -6.0 },
            },
        ];
        if up {
            changes.push(Change::InputUp { slot: 0 });
        }
        let items: Vec<Item> = changes.into_iter().map(Item::Change).collect();
        batch::fold(&items, 1, 2).unwrap().unwrap()
    }

    /// An entry the pending list holds already is taken off the ring
    /// while the list is full: it needs no room, and the entries behind it
    /// would otherwise wait until the list drains, and turn late.
    #[test]
    fn a_held_entry_is_taken_off_a_full_ring() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("held-first", 1);
        let (mut rt, mut ring) = rt_of(&e, &domain, "1d41");
        let entry = |id| RtEntry {
            id,
            at_index: 1 << 30,
            change: Change::InputDown { slot: 0 },
        };
        for id in 1..=PENDING_CHANGES as u64 {
            assert!(rt.mix.push(entry(id)));
        }
        let (change, batch) = batch_of(false);
        let held = RingEntry {
            entry: RtEntry { change, ..entry(3) },
            batch,
        };
        let new = RingEntry {
            entry: entry(65),
            batch: None,
        };
        assert!(ring.push(held).is_ok() && ring.push(new).is_ok());
        rt.take_entries();
        assert_eq!((rt.taken, rt.last_entry), (1, 3), "the held entry waited");
        assert_eq!(ring.slots(), PENDING_CHANGES - 1, "the new one was taken");
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A copy shares the batches pending, and an install takes the core's
    /// batch and hands the batch of an entry it drops back in the core: no
    /// batch is allocated or freed on the RT thread.
    #[test]
    fn an_install_moves_batches_without_allocating() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("batch-install", 1);
        let (mut rt, _ring) = rt_of(&e, &domain, "1d43");
        let (change, mine) = batch_of(false);
        let entry = |id| RtEntry {
            id,
            at_index: 1440,
            change,
        };
        let mine = mine.unwrap();
        let theirs = Arc::new((*mine).clone());
        let (mine_at, theirs_at) = (Arc::as_ptr(&mine), Arc::as_ptr(&theirs));
        assert!(rt.mix.push_with(entry(3), Some(mine)).is_ok());
        let mut core = Box::new(Core::new(2, 1, 1, 64, 1));
        rt.capture(&mut core);
        assert!(
            std::ptr::eq(Arc::as_ptr(&core.batches[0].1), mine_at),
            "copied"
        );
        core.pos = Some(480);
        core.prepare();
        core.pending.clear();
        core.pending.push(entry(5));
        core.batches.push((5, theirs));
        rt.install_core(&mut core, 5, true);
        assert_eq!(rt.mix.pending, [entry(5)]);
        assert_eq!(rt.mix.batches.len(), 1);
        assert!(std::ptr::eq(Arc::as_ptr(&rt.mix.batches[0].1), theirs_at));
        assert!(core.batches.is_empty());
        assert!(
            core.handed
                .0
                .iter()
                .any(|b| std::ptr::eq(Arc::as_ptr(b), mine_at)),
            "the dropped batch did not go back with the core"
        );
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Without a core a batch whose index passed sets its strip
    /// parameters on the strips the slots keep and passes the rest; one
    /// holding an `InputUp` stays pending whole, as that entry does
    /// alone, so its strip parameters wait with it.
    #[test]
    fn a_batch_due_without_a_core_passes_whole_or_waits_whole() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("batch-no-core", 1);
        let (mut rt, _ring) = rt_of(&e, &domain, "1d45");
        let (plain, plain_batch) = batch_of(false);
        let (up, up_batch) = batch_of(true);
        let at = |id, change| RtEntry {
            id,
            at_index: 48,
            change,
        };
        assert!(rt.mix.push_with(at(1, up), up_batch).is_ok());
        rt.pass_entries(2);
        assert_eq!(rt.mix.pending, [at(1, up)]);
        assert_eq!(
            rt.slots[0].strip_pos, None,
            "a strip parameter of it applied"
        );
        assert!(rt.mix.push_with(at(2, plain), plain_batch).is_ok());
        rt.pass_entries(2);
        assert_eq!(rt.mix.pending, [at(1, up)]);
        assert_eq!(rt.mix.batches.len(), 1);
        assert_eq!(rt.slots[0].strip_pos, Some(48));
        assert!(rt.slots[0].up, "an InputDown applied without a core");
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Without a core, a batch whose strip parameters the replayed strips
    /// hold already, marked so when taken, passes without setting them
    /// again, which would restart their ramps.
    #[test]
    fn a_batch_passed_without_a_core_keeps_strips_the_core_brought() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("batch-held-strips", 1);
        let (mut rt, _ring) = rt_of(&e, &domain, "1d49");
        let (change, batch) = batch_of(false);
        let Change::Batch { up, routed, .. } = change else {
            panic!("{change:?}")
        };
        let held = RtEntry {
            id: 1,
            at_index: 48,
            change: Change::Batch {
                strips: false,
                up,
                routed,
                detached: 0,
                resets: false,
            },
        };
        assert!(rt.mix.push_with(held, batch).is_ok());
        rt.pass_entries(2);
        assert!(rt.mix.pending.is_empty());
        assert_eq!(rt.slots[0].strip_pos, None, "a strip parameter set again");
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// Spent batches leave the return ring's reserve free, so a
    /// generation swapped in hands the old one back, not leak it: past the
    /// reserve they wait on the spent list, beyond it they are leaked and
    /// counted, and they leave through the ring once it has room.
    #[test]
    fn spent_batches_leave_room_for_a_generation_handed_back() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("batch-spent", 1);
        let (mut rt, _ring) = rt_of(&e, &domain, "1d47");
        rt.replay = false;
        let (ret, mut back) = rtrb::RingBuffer::new(8);
        rt.ret = ret;
        rt.spent = Vec::with_capacity(2);
        let generation = || {
            Box::new(Gen {
                mxl: Generation::new(&e.api, &domain, 1).unwrap(),
                prune_after: vec![u64::MAX],
                prune_late: vec![u64::MAX],
                rerouted: Default::default(),
            })
        };
        rt.install(0, generation());
        for _ in 0..10 {
            rt.retire(batch_of(false).1);
        }
        // A reserve of one domain, one output and one more.
        assert_eq!((rt.ret.slots(), rt.spent.len()), (3, 2));
        assert_eq!(rt.counters.batches_leaked.load(Relaxed), 3);
        rt.install(0, generation());
        let returned: Vec<Returned> = std::iter::from_fn(|| back.pop().ok()).collect();
        let generations = returned
            .iter()
            .filter(|r| matches!(r, Returned::Generation(_)))
            .count();
        assert_eq!((returned.len(), generations), (6, 1));
        rt.take_entries();
        assert_eq!(rt.spent.len(), 0);
        assert!(matches!(back.pop(), Ok(Returned::Batch(_))));
        drop(returned);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// No engine entries append nothing, wherever the index: the log id
    /// stays free, and an empty translation is refused by no replica.
    #[test]
    fn a_log_entry_of_no_engine_entries_appends_nothing() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("batch-empty", 1);
        assert_eq!(e.append_batch(5, 7, &[]), Ok(()));
        assert_eq!(e.log.lock().unwrap().last, 0);
        assert_eq!(
            e.append_batch(5, 7, &[Entry::InputDown { slot: 0 }]),
            Err(EntryError::Invalid(
                "at_index 7 is not a multiple of the block size 48".to_owned()
            ))
        );
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A core whose block number leaves the index range at this block
    /// size is refused, not overflowed.
    #[test]
    fn a_core_after_a_block_beyond_any_index_is_not_replayed() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("replay-k0", 1);
        let copy = || loop {
            match e.copy_core(Duration::from_secs(2)) {
                Err(CopyError::Passed { .. }) => continue,
                c => break c.unwrap(),
            }
        };
        for k0 in [u64::MAX, crate::transfer::INDEX_MAX] {
            let mut c = copy();
            c.k0 = k0;
            assert!(
                matches!(e.replay(c), Err(ReplayError::Invalid(m)) if m.contains("after block")),
                "a core after block {k0}"
            );
        }
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// The answer to a hand-over whose caller gave up goes to the next
    /// call, which does not take it for its own: that call installs its
    /// own core.
    #[test]
    fn a_late_answer_is_not_taken_for_the_next_handover() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("handover-late", 1);
        let copy = || loop {
            match e.copy_core(Duration::from_secs(2)) {
                Err(CopyError::Passed { .. }) => continue,
                c => break c.unwrap(),
            }
        };
        let n = |c: &AtomicU64| c.load(Relaxed);
        let processed = || e.counters.processed.load(Acquire);
        // An earlier call's hand-over, installed once that call gave up.
        let mut stale = 0;
        while n(&e.counters.handovers_installed) == 0 {
            let mut replay = e.replay(copy()).unwrap();
            let mut t = e.transfer.as_ref().unwrap().lock().unwrap();
            let (inputs, outputs, slots, page, outs) = t.shape;
            let mut core = Box::new(Core::new(inputs, outputs, slots, page, outs));
            let p = processed();
            while processed() == p {
                std::thread::sleep(Duration::from_micros(50));
            }
            replay.run(processed() - 1);
            replay.rt.capture(&mut core);
            t.seq += 1;
            stale = replay.next - 1;
            let h = Handover {
                seq: t.seq,
                j: stale,
                last_id: replay.last_id,
                core,
                replayed: true,
                strips: Vec::new(),
                limiters: Vec::new(),
                delays: Vec::new(),
                out_strips: Vec::new(),
                out_delays: Vec::new(),
                tones: Vec::new(),
                answer: Answer::Waiting,
            };
            assert!(t.handovers.push(h).is_ok());
            t.handing = true;
            let answered = n(&e.counters.handovers_passed);
            wait(&|| {
                n(&e.counters.handovers_installed) > 0 || n(&e.counters.handovers_passed) > answered
            });
            if n(&e.counters.handovers_installed) == 0 {
                while t.handed.pop().is_err() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                t.handing = false;
            }
        }
        let mut replay = e.replay(copy()).unwrap();
        let j = e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        assert_ne!(j, stale, "the late answer's block returned");
        assert_eq!(n(&e.counters.handovers_installed), 2);
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A core is filled only for a block the RT thread has computed: the
    /// clock runs on while the RT thread stalls, so a core for the newest
    /// block due would follow a block the RT thread has not computed, and
    /// install a state ahead of the one it computes from. `processed` is
    /// read inside `make`, so the RT thread's steps after the stall can
    /// only make it larger.
    #[test]
    fn a_core_is_filled_only_for_a_block_the_rt_thread_computed() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("handover-stall", 1);
        let processed = || e.counters.processed.load(Acquire);
        wait(&|| processed() > 10);
        crate::rt_loop::STALL_MS.store(300, Relaxed);
        // The RT thread is in the stall once it stops computing.
        let still = |p: u64| {
            std::thread::sleep(Duration::from_millis(20));
            processed() == p
        };
        let t = Instant::now();
        while !still(processed()) {
            assert!(t.elapsed() < Duration::from_secs(5), "no stall");
        }
        let mut calls = Vec::new();
        let _ = e.offer(
            Duration::from_secs(1),
            0,
            true,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            |j, _| {
                calls.push((j, processed()));
                (j, 0)
            },
        );
        assert!(!calls.is_empty());
        for &(j, p) in &calls {
            assert!(j < p, "a core after block {j} with {p} computed");
        }
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A hand-over waiting for a stalled RT thread to step sleeps between
    /// looks after one block period of yielding: a spin for the whole
    /// timeout would spend the CPU quota the RT thread shares.
    #[test]
    fn a_handover_waiting_on_a_stalled_rt_thread_spends_little_cpu() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("handover-cpu", 1);
        let processed = || e.counters.processed.load(Acquire);
        wait(&|| processed() > 10);
        crate::rt_loop::STALL_MS.store(600, Relaxed);
        let t = Instant::now();
        loop {
            let p = processed();
            std::thread::sleep(Duration::from_millis(20));
            if processed() == p {
                break;
            }
            assert!(t.elapsed() < Duration::from_secs(5), "no stall");
        }
        let cpu = || {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: a valid clock id and a timespec to write.
            assert_eq!(
                unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
                0
            );
            Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
        };
        let before = cpu();
        let err = e.offer(
            Duration::from_millis(300),
            0,
            true,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            |j, _| (j, 0),
        );
        let spent = cpu() - before;
        assert!(matches!(err, Err(ReplayError::Timeout { .. })), "{err:?}");
        assert!(spent < Duration::from_millis(100), "{spent:?} of CPU");
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// A call that times out names how many of its hand-overs came back
    /// refused for a block the RT thread passed, so a resync budget can be
    /// judged against it.
    #[test]
    fn a_timed_out_handover_names_its_passed_refusals() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let (e, _statuses, _verdicts, domain) = outputs_only("handover-timeout", 1);
        let before = e.counters.handovers_passed.load(Relaxed);
        // Each core follows a block two behind the last one computed:
        // every attempt is refused.
        let err = e.offer(
            Duration::from_millis(300),
            0,
            true,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            |j, _| (j.saturating_sub(2), 0),
        );
        let refused = e.counters.handovers_passed.load(Relaxed) - before;
        let Err(ReplayError::Timeout { passed }) = err else {
            panic!("{err:?}");
        };
        assert!(passed > 0);
        // The last attempt's answer may come after the call gave up.
        assert!(
            refused == passed || refused == passed + 1,
            "{refused} {passed}"
        );
        e.stop();
        let _ = std::fs::remove_dir_all(domain);
    }

    /// What [`lost_run`] does to replica a during the measurement.
    #[derive(Clone, Copy, PartialEq)]
    enum Upset {
        /// A stall that skips one block, two seconds in.
        Skip,
        /// A stall that skips blocks around the index the log takes the
        /// input down at, a second in, for half a second.
        SkipOverDown,
        /// Marked diverged for about half a second, a second in.
        Diverged,
    }

    /// What one [`lost_run`] measured on replica a and b.
    struct Lost {
        a: OutputMeters,
        b: OutputMeters,
        /// The blocks a skipped, one block a slice.
        skipped: std::ops::Range<u64>,
        /// The blocks a computed while marked diverged, at the least and
        /// at the most.
        diverged: (u64, u64),
        /// The input down by the log over these blocks, on both.
        down: std::ops::Range<u64>,
        /// The measurement's reset and the end of its programme.
        reset: u64,
        end: u64,
    }

    /// Runs `lost_run` until a run is not disturbed, at most four times.
    fn lost(upset: Upset) -> Lost {
        use crate::rt_loop::{STALL_LOOP, STALL_MS};
        struct Unstall;
        impl Drop for Unstall {
            fn drop(&mut self) {
                STALL_MS.store(0, Relaxed);
                STALL_LOOP.store(0, Relaxed);
            }
        }
        let _unstall = Unstall;
        let mut seen = Vec::new();
        for run in 0..4 {
            match lost_run(run, upset) {
                Ok(l) => return l,
                Err(why) => {
                    eprintln!("run disturbed ({why}): running it again");
                    seen.push(why);
                }
            }
        }
        panic!("every run was disturbed: {seen:?}");
    }

    /// Two sines on flow `id`, 1 kHz on channel 0 and 1.5 kHz on channel
    /// 1 at half scale, written a block of `block` samples at a time as
    /// libmxl's clock passes it, until `stop` is set. Both hold whole
    /// periods in every 10 ms slice.
    fn sines(
        api: &Arc<samplez_mxl::MxlApi>,
        domain: &std::path::Path,
        id: &str,
        block: u64,
        stop: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use samplez_mxl::output::{self, Creation, OutputConfig};
        use samplez_mxl::time;
        let instance = samplez_mxl::Instance::new(api, domain).unwrap();
        let cfg = OutputConfig {
            flow_id: id,
            label: "sines",
            group: "sines",
            role: "Audio",
            channels: 2,
            block,
            batch: block,
        };
        let Creation::Created { mut writer, .. } = output::create(&instance, &cfg).unwrap() else {
            panic!("sine flow {id} not created");
        };
        let api = Arc::clone(api);
        std::thread::spawn(move || {
            let n = block as usize;
            let mut data = vec![0.0f32; 2 * n];
            let mut next: Option<u64> = None;
            while !stop.load(Relaxed) {
                let index = time::timestamp_to_index(&api, time::now(&api));
                if let Some(due) = (index / block).checked_sub(1) {
                    for k in next.unwrap_or(due)..=due {
                        for (ch, out) in data.chunks_exact_mut(n).enumerate() {
                            for (i, v) in out.iter_mut().enumerate() {
                                *v = sine(ch, k * block + i as u64);
                            }
                        }
                        writer.write_block(k, &data);
                    }
                    next = Some(due + 1);
                }
                let k = next.unwrap_or(0);
                time::sleep_until(&api, time::index_to_timestamp(&api, (k + 1) * block));
            }
            drop(writer);
            drop(instance);
        })
    }

    /// Sample `index` of channel `ch` of [`sines`].
    fn sine(ch: usize, index: u64) -> f32 {
        let f = 1000 + 500 * ch as u64;
        let phase = (f * (index % 48_000)) % 48_000;
        0.5 * (std::f64::consts::TAU * phase as f64 / 48_000.0).sin() as f32
    }

    /// Two replicas of one stereo programme measured from one reset to
    /// the end of the programme, its gains ramping to 0, and on past it,
    /// replica a upset by `upset` on the way. A run in which a skip or a
    /// silenced input read other than the one injected moved a counter
    /// is disturbed: an error.
    fn lost_run(run: u32, upset: Upset) -> Result<Lost, String> {
        use crate::rt_loop::{STALL_LOOP, STALL_MS};
        use samplez_dsp::WINDOW;
        use samplez_mxl::time;
        // One block a slice, so a skipped block is one slice.
        const B: u64 = 480;
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let shm = std::path::Path::new("/dev/shm");
        let base = if shm.is_dir() {
            shm.to_path_buf()
        } else {
            std::env::temp_dir()
        };
        let domain = base.join(format!("samplez-lost-{}-{run}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let api = samplez_mxl::load(std::path::Path::new(&libmxl)).unwrap();
        let src = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e01";
        let stop = Arc::new(AtomicBool::new(false));
        let producer = sines(&api, &domain, src, B, Arc::clone(&stop));
        let start = |out: &str| {
            let v = serde_json::json!({
                "libmxl": libmxl,
                "blockSize": B,
                "readerMarginSamples": 3 * B,
                // A stall of two and a half block periods, after a block,
                // leaves the loop one block past the next one: it skips
                // that one.
                "maxCatchupBlocks": 1,
                "domains": [{ "name": "d", "path": domain }],
                "inputs": [{ "name": "in", "domain": "d", "flowId": src, "channels": 2 }],
                "outputs": [{ "name": "out", "domain": "d", "flowId": out, "label": "lost",
                              "group": "lost", "role": "Audio", "channels": 2 }],
                "matrix": [[1.0, 0.0], [0.0, 1.0]],
                "rt": { "enabled": false, "rtCpu": "none" },
            });
            let config = Config::parse(&v.to_string()).unwrap();
            let process = rt::setup_process(&config.rt);
            let (report_tx, reports) = mpsc::sync_channel(64);
            let (status_tx, changes) = mpsc::sync_channel(1 << 20);
            let replication = Replication {
                voters: 2,
                reports: report_tx,
                statuses: Some(status_tx),
                hashes: None,
            };
            let e = Engine::start_replicated(config, process, None, replication).unwrap();
            wait(&|| {
                let s = e.status();
                s.outputs[0].realised && s.inputs[0].state == InputState::Attached
            });
            (e, changes, reports)
        };
        let (a, a_changes, _ra) = start("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e02");
        let (b, b_changes, _rb) = start("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e03");
        let now = || time::timestamp_to_index(&api, time::now(&api));
        let reset = (now() + 24_000).next_multiple_of(WINDOW);
        // The programme ends at `end`, its gains ramping to 0 over one
        // slice; after it no block nor short-term value is above the
        // gates.
        let end = reset + 480_000;
        let (down, up) = if upset == Upset::SkipOverDown {
            (reset + 48_000, reset + 72_000)
        } else {
            (end, end)
        };
        for e in [&a, &b] {
            e.append(1, reset, Entry::ResetLoudness { meter: 0 })
                .unwrap();
            let mut id = 1;
            if down < up {
                e.append(2, down, Entry::InputDown { slot: 0 }).unwrap();
                e.append(3, up, Entry::InputUp { slot: 0 }).unwrap();
                id = 3;
            }
            for c in 0..2 {
                let gain = Entry::Gain {
                    input: c,
                    output: c,
                    gain: 0.0,
                    fade: None,
                };
                id += 1;
                e.append(id, end, gain).unwrap();
            }
            e.commit(id);
        }
        let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Relaxed);
        let skipped = |e: &Engine| e.counters().overrun_skipped_samples.load(Relaxed);
        let (sa, sb) = (silenced(&a), silenced(&b));
        let processed = || a.counters().processed.load(Acquire);
        let at = |index: u64| {
            while now() < index {
                std::thread::sleep(Duration::from_millis(1));
            }
        };
        let stall = |ms: u64| {
            STALL_LOOP.store(std::ptr::from_ref(&*a.counters) as usize, Relaxed);
            STALL_MS.store(ms, Relaxed);
            wait(&|| STALL_MS.load(Relaxed) == 0);
            STALL_LOOP.store(0, Relaxed);
        };
        let mut diverged = (0, 0);
        match upset {
            Upset::Skip => {
                at(reset + 96_000);
                stall(B * 5 / 2 / 48);
            }
            Upset::SkipOverDown => {
                // About four blocks. The loop computes a block about four
                // block periods after its index (its read margin of three
                // and the block itself), so a stall from a block after the
                // input goes down skips blocks on both sides of it.
                at(down + B);
                stall(B * 11 / 2 / 48);
            }
            Upset::Diverged => {
                at(reset + 48_000);
                let first = processed();
                a.set_diverged(true).unwrap();
                let from = processed();
                at(reset + 72_000);
                let last = processed();
                a.set_diverged(false).unwrap();
                let to = processed();
                // The loop reads the flag while it meters a block and
                // counts the block processed after it, so the block in
                // flight at either store may have read the flag before
                // it. At the least the blocks processed between the two
                // stores but the first; at the most those up to the block
                // after.
                diverged = (last.saturating_sub(from + 1), to + 1 - first);
            }
        }
        let frozen = end + 3 * 48_000 + 4 * WINDOW;
        let t = Instant::now();
        while [&a, &b].iter().any(|e| {
            let w = e.status().outputs[0].meters.window;
            w.is_none_or(|w| w * WINDOW <= frozen)
        }) {
            assert!(
                t.elapsed() < Duration::from_secs(30),
                "measured past the end"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let skips = |changes: &mpsc::Receiver<StatusChange>| -> Vec<(u64, u64)> {
            changes
                .try_iter()
                .filter_map(|c| match c {
                    StatusChange::Skipped { from, to } if to * B > reset => Some((from, to)),
                    _ => None,
                })
                .collect()
        };
        let (ka, kb) = (skips(&a_changes), skips(&b_changes));
        let (ma, mb) = (
            a.status().outputs[0].meters.clone(),
            b.status().outputs[0].meters.clone(),
        );
        let downed = down / B..up / B;
        let expected_silenced = downed.end - downed.start;
        let injected = if upset == Upset::Diverged { 0 } else { 1 };
        // A silenced block outside the input's down span, a skip on b, or
        // other than the injected skip on a, is a measured disturbance.
        let silenced_a = silenced(&a) - sa;
        let silenced_b = silenced(&b) - sb;
        let disturbed = (silenced_b != expected_silenced || silenced_a > expected_silenced)
            .then(|| format!("input reads silenced: {silenced_a} and {silenced_b}"))
            .or_else(|| (!kb.is_empty()).then(|| format!("b skipped {kb:?}")))
            .or_else(|| (ka.len() != injected).then(|| format!("a skipped {ka:?}")))
            .or_else(|| {
                // The skip straddles the index the input goes down at, so
                // the entry applies inside it and splits its runs.
                let straddles = ka
                    .first()
                    .is_some_and(|k| k.0 < downed.start && downed.start < k.1);
                (upset == Upset::SkipOverDown && !straddles)
                    .then(|| format!("a skipped {ka:?}, not across {}", downed.start))
            })
            .or_else(|| {
                (upset == Upset::Skip
                    && skipped(&a) / B != ka.iter().map(|k| k.1 - k.0).sum::<u64>())
                .then(|| "a skipped more than the stall".to_owned())
            });
        stop.store(true, Relaxed);
        a.stop();
        b.stop();
        producer.join().unwrap();
        let _ = std::fs::remove_dir_all(&domain);
        if let Some(why) = disturbed {
            return Err(why);
        }
        let skipped = ka.first().map_or(0..0, |k| k.0..k.1);
        Ok(Lost {
            a: ma,
            b: mb,
            skipped,
            diverged,
            down: downed,
            reset,
            end,
        })
    }

    /// Per 10 ms slice, the K-weighted power of [`sine`] relative to that
    /// of slice `steady`, as a meter measures it from slice `from` on:
    /// with the slice at `skip` left out, its filters running on over the
    /// jump, and the gains ramping to 0 over one slice from slice `end`.
    fn transient(from: u64, steady: u64, skip: Option<u64>, end: u64, slices: u64) -> Vec<f64> {
        use samplez_dsp::{KWeighting, SLICE};
        let mut kw = KWeighting::new(2);
        let mut x = [vec![0.0f32; SLICE as usize], vec![0.0f32; SLICE as usize]];
        let mut powers = Vec::new();
        let mut jump = 0;
        for slice in from..from + slices {
            if Some(slice) == skip {
                jump = 1;
            }
            let mut p = 0.0;
            for (ch, x) in x.iter_mut().enumerate() {
                for (n, v) in x.iter_mut().enumerate() {
                    let s = slice + jump;
                    let gain = match s.cmp(&end) {
                        std::cmp::Ordering::Less => 1.0,
                        std::cmp::Ordering::Equal => 1.0 - n as f32 / SLICE as f32,
                        std::cmp::Ordering::Greater => 0.0,
                    };
                    *v = gain * sine(ch, s * SLICE + n as u64);
                }
                p += kw.process(ch, x) / SLICE as f64;
            }
            powers.push(p);
        }
        let steady = powers[(steady - from) as usize];
        powers.iter().map(|p| p / steady).collect()
    }

    /// Two replicas of one stereo programme, one of them skipping a block
    /// after an injected stall: the skipped slice is lost on that replica
    /// alone, both measure on, they count the same gaps (none), and their
    /// integrated loudness differs by no more than the bound below.
    #[test]
    fn a_skip_is_lost_on_its_replica_alone_and_both_measure_on() {
        use samplez_dsp::{SLICE, WINDOW};
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let l = lost(Upset::Skip);
        let (ma, mb) = (&l.a, &l.b);
        let lost = l.skipped.end - l.skipped.start;
        assert!(lost >= 1);
        assert_eq!((ma.lost, mb.lost), (lost, 0), "{ma:?} {mb:?}");
        assert_eq!((ma.gaps, mb.gaps), (0, 0));
        assert!(ma.range.is_some() && mb.range.is_some(), "{ma:?} {mb:?}");
        let (Some(ia), Some(ib)) = (ma.integrated, mb.integrated) else {
            panic!("the integrated loudness did not carry on: {ma:?} {mb:?}");
        };
        // The bound. The programme holds whole periods in every slice, so
        // every slice it fills has one power P. A gating block of 40 kept
        // slices on the 100 ms grid has power P unless it holds a slice
        // of another: on replica a, where the K-weighting runs on across
        // the skipped slice, and on both at the end of the programme. The
        // filters are run here once over each, as the meters run them:
        // after the skip, S slices differ from P, by at most a factor h;
        // at the end, E slices lie between P and 0. Each is asserted
        // below the margin assumed for it.
        let (skip, end) = (l.skipped.start, l.end / SLICE);
        // Steady from 50 slices on, as the filters settle from silence.
        let after_skip = transient(skip - 100, skip - 1, Some(skip), end, 150);
        let tol = 1e-6;
        let after_skip = &after_skip[100..];
        let s = (after_skip.iter())
            .filter(|p| (*p - 1.0).abs() > tol)
            .count() as u64;
        let h_skip = after_skip.iter().fold(1.0f64, |h, &p| h.max(p));
        let at_end = transient(end - 100, end - 1, None, end, 150);
        let e = at_end.iter().filter(|&&p| p > tol && p < 1.0 - tol).count() as u64;
        let h_end = at_end.iter().fold(1.0f64, |h, &p| h.max(p));
        eprintln!("transient: S {s} slices up to {h_skip} P after the skip, E {e} up to {h_end} P");
        // The margin: twice the measured length and twice the measured
        // peak, so a transient of either kind lasts at most S' = 2S and
        // E' = 2E slices, each at most h = 2 max(h_skip, h_end) P.
        let (s2, e2) = (2 * s, 2 * e);
        let h = 2.0 * h_skip.max(h_end);
        // A run of r slices on a 10-slice grid of 40-slice blocks lies in
        // at most (r + 39) / 10 + 1 blocks, and the end, past which the
        // slices are 0, in 4 more. So at most D of the N blocks of either
        // replica differ from P, each in [0, h P], and its integrated
        // loudness, the mean power of the blocks above the gates, lies in
        //   [10 log10((N - D) P / N), 10 log10((N - D + D h) P / N)]:
        // the two lie within the width of that interval. N is at least
        // the slices kept before the end over 10, less the 3 windows
        // before the first block.
        let runs = |r: u64| if r == 0 { 0 } else { (r + 39) / 10 + 1 };
        let d = (runs(s2) + runs(e2) + 4) as f64;
        let kept = (l.end - l.reset) / SLICE - lost;
        let n = (kept / (WINDOW / SLICE) - 3) as f64;
        let tolerance = 10.0 * ((n - d + d * h) / (n - d)).log10();
        assert!(
            (ia - ib).abs() <= tolerance,
            "{ia} {ib}, tolerance {tolerance} dB over {n} blocks"
        );
        eprintln!(
            "lost {lost}: integrated {ia} and {ib}, {} dB apart, tolerance {tolerance} dB",
            (ia - ib).abs()
        );
    }

    /// A replica that skips blocks over a stretch the log takes an input
    /// down for counts the same gaps there as its peer: the slices are
    /// decided from the log, not left out as lost. Only those it skipped
    /// outside that stretch are lost.
    #[test]
    fn a_skip_over_an_input_down_counts_the_peers_gaps() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let l = lost(Upset::SkipOverDown);
        let (ma, mb) = (&l.a, &l.b);
        let gaps = l.down.end - l.down.start;
        let outside = (l.skipped.clone()).filter(|k| !l.down.contains(k)).count() as u64;
        eprintln!("skipped {:?}, down {:?}", l.skipped, l.down);
        assert_eq!((ma.gaps, mb.gaps), (gaps, gaps), "{ma:?} {mb:?}");
        assert_eq!((ma.lost, mb.lost), (outside, 0), "{ma:?} {mb:?}");
        assert!(ma.integrated.is_some() && mb.integrated.is_some());
    }

    /// A replica marked diverged loses the slices it computes meanwhile,
    /// and its peer none; both count the same gaps (none) and measure on.
    #[test]
    fn a_diverged_replica_loses_its_slices_and_its_peer_none() {
        let _engines = ENGINES.lock().unwrap_or_else(|e| e.into_inner());
        let l = lost(Upset::Diverged);
        let (ma, mb) = (&l.a, &l.b);
        let (least, most) = l.diverged;
        assert!(least > 0, "{least}");
        assert!(
            (least..=most).contains(&ma.lost),
            "{} not in {least}..={most}",
            ma.lost
        );
        assert_eq!((mb.lost, ma.gaps, mb.gaps), (0, 0, 0), "{ma:?} {mb:?}");
        assert!(ma.integrated.is_some() && mb.integrated.is_some());
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

    /// An entry that arrives while a fade runs takes the value the fade
    /// has at its index as the start of its own ramp, or replaces the
    /// fade with its own, joined from that value where its own started
    /// before the index, or ends it when it changes nothing.
    #[test]
    fn a_passed_entry_folds_into_a_running_fade() {
        let running = Fade {
            from: 1.0,
            start: 1000,
            len: 400,
        };
        let at = 1200;
        let held = running.value_at(0.25, at);
        assert!(held < 1.0 && held > 0.25);
        let fold = |to: f32, fade: Option<Fade>| {
            let (mut gains, mut fadings) = ([0.25f32], [Some(running)]);
            fold_passed(&mut gains, &mut fadings, 0, (to, fade), at, 480);
            (gains[0], fadings[0])
        };
        // A plain entry ramps over the ramp length from the held value.
        assert_eq!(
            fold(0.5, None),
            (
                0.5,
                Some(Fade {
                    from: held,
                    start: at,
                    len: 480
                })
            )
        );
        // A fade replaces it as it is.
        let own = Fade {
            from: 0.9,
            start: at + 10,
            len: 100,
        };
        assert_eq!(fold(0.5, Some(own)), (0.5, Some(own)));
        // A fade started before the entry is joined from the held value.
        let late = Fade {
            from: 0.0,
            start: 900,
            len: 1000,
        };
        let joined = late.joined(0.5, held, at, 480);
        assert_eq!(
            joined,
            Some(Fade {
                from: held,
                start: at,
                len: 700
            })
        );
        assert_eq!(fold(0.5, Some(late)), (0.5, joined));
        // A target equal to the held value leaves no ramp.
        assert_eq!(fold(held, None), (held, None));
    }

    /// An entry's fade starts at most the fade bound past its index; a
    /// start near the end of the index range is refused, not wrapped.
    #[test]
    fn a_fade_must_start_within_the_bound_of_its_entry() {
        let max = u64::from(samplez_dsp::MAX_EXACT_RAMP_LEN);
        let at = 1000;
        let f = |start| Fade {
            from: 1.0,
            start,
            len: 10,
        };
        assert!(f(at + max).within(at) && f(0).within(at));
        assert!(!f(at + max + 1).within(at) && !f(u64::MAX).within(at));
        assert!(f(u64::MAX).within(u64::MAX));
        let entry = |fade| Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade,
        };
        assert_eq!(
            fade_beyond(&[entry(None), entry(Some(f(at + max)))], at),
            None
        );
        assert_eq!(
            fade_beyond(&[entry(None), entry(Some(f(at + max + 1)))], at),
            Some(f(at + max + 1))
        );
    }

    /// A plain gain change renders as the absolute fade a replica tracks
    /// for it, `from` the value held at its index and the ramp length as
    /// the length, bit for bit, mid-ramp and chained.
    #[test]
    fn a_plain_set_renders_as_its_absolute_fade() {
        use samplez_dsp::Matrix;
        let ramp = 480u32;
        let mut plain = Matrix::new(1, 1);
        let mut fades = Matrix::new(1, 1);
        assert!(plain.set(0, 0, 1.0, 0) && fades.set(0, 0, 1.0, 0));
        // (index, target): the second change lands mid-ramp, the third
        // after the second has ended.
        let changes = [(100u64, 0.25f32), (300, 0.75), (1500, 0.0)];
        let mut held = 1.0f32;
        let mut last: Option<Fade> = None;
        let mut next = changes.iter().peekable();
        for n in 0..2200u64 {
            if let Some(&&(at, to)) = next.peek().filter(|c| c.0 == n) {
                assert!(plain.set(0, 0, to, ramp));
                let from = last.map_or(held, |f| f.value_at(held, at));
                let f = Fade {
                    from,
                    start: at,
                    len: ramp,
                };
                assert!(fades.set_resumed(0, 0, to, f.ramp_at(to, at)));
                (held, last) = (to, Some(f));
                next.next();
            }
            assert_eq!(
                plain.gain(0, 0).to_bits(),
                fades.gain(0, 0).to_bits(),
                "sample {n}"
            );
            plain.advance(1);
            fades.advance(1);
        }
    }
}
