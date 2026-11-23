//! The RT thread: the block loop on libmxl's TAI clock, and the messages
//! it exchanges with the supervisor.
//!
//! Nothing here allocates, frees or drops a libmxl object once the loop
//! runs. Objects arrive through the command ring and leave through the
//! return ring; the whole state goes back to the supervisor when the loop
//! ends, also when it panics.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release,
};

use mxl_sys::MXL_ERR_OUT_OF_RANGE_TOO_EARLY;
use rtrb::{Consumer, Producer};
use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{
    Delay, KWeighting, Limiter, LimiterParams, MAX_DUCKS, Matrix, PlanarBuffer, Ramp, Started,
    Strip, StripParam, Tone, ToneParams, TruePeak, accumulate,
};
use samplez_mxl::input::{Counters as InputCounters, Generation, GroupWait, Outcome, SlotState};
use samplez_mxl::output::{FlowWriter, Write};
use samplez_mxl::{MxlApi, time};

use crate::batch::Batch;
use crate::blocks::{
    Cause, Changes, Ctx, Hold, PAGE, PageHash, Pages, Sink, StatusChange, Statuses,
};
use crate::ducks::{DuckRecord, Ducks};
use crate::transfer::{Answer, Core, CoreCopy, DelayCore, Handover, OutCore};

/// Engine counters, written by the RT thread and the supervisor.
#[derive(Debug, Default)]
pub struct Counters {
    /// Loop wakes: blocks processed, and wakes while libmxl's clock is
    /// behind the last block processed (`rt_heartbeat_blocks_total`).
    pub heartbeat_blocks: AtomicU64,
    /// Wakes while libmxl's clock was behind the last block processed,
    /// after a backward step: no block is computed twice.
    pub clock_behind_wakes: AtomicU64,
    /// Blocks committed later than `T_k` plus a quarter block period.
    pub deadline_miss: AtomicU64,
    /// Samples skipped when the loop was more than `maxCatchupBlocks`
    /// behind; libmxl zero-fills them on the next commit.
    pub overrun_skipped_samples: AtomicU64,
    /// Blocks processed before their deadline after every input head
    /// reached the block.
    pub wait_early: AtomicU64,
    /// Input waits that reached the block's deadline with a head behind.
    pub input_wait_timeout: AtomicU64,
    /// Sleeps of the input waits between two reads of the heads.
    pub input_wait_sleeps: AtomicU64,
    /// Input blocks not read because the head was below the range end.
    pub input_late_blocks: AtomicU64,
    /// Input blocks silenced for any reason, a pruned input included.
    pub input_silenced_blocks: AtomicU64,
    /// Validity probes of the failed-input identification. The RT thread
    /// makes none: a late input is pruned, frozen or not, and its flow
    /// checked on the supervisor side.
    pub input_probes: AtomicU64,
    /// Clock steps: libmxl's clock moved by more than a block period
    /// against `CLOCK_MONOTONIC` between two wakes.
    pub clock_steps: AtomicU64,
    /// The kernel TAI offset in seconds, `CLOCK_TAI` - `CLOCK_REALTIME` read
    /// back to back at each wake's step check; `i64::MIN` before the
    /// first.
    pub tai_offset_s: AtomicI64,
    /// Replicated mode: a detected clock step fences the blocks after it
    /// until the replicated state clears it.
    pub step_fenced: AtomicBool,
    /// The outputs disabled by the log, a bit per output.
    pub outputs_disabled: AtomicU64,
    /// The outputs whose writer was handed back since the supervisor last
    /// took them, a bit per output: set after the writer is in the return
    /// ring.
    pub outputs_released: AtomicU64,
    /// Output blocks starting below the last commit, not written.
    pub output_held_blocks: AtomicU64,
    /// Output open or commit calls libmxl refused.
    pub output_failed_writes: AtomicU64,
    /// Log entries whose `at_index` lay in samples already produced
    /// when they reached the RT thread; not applied, and each one marks
    /// the engine diverged.
    pub command_late: AtomicU64,
    /// Entries needing a generation that were due before their generation
    /// arrived: applied, with the slot silent until it arrives.
    pub generation_late: AtomicU64,
    /// Generations the supervisor handed to the RT thread with less than
    /// its 2 ms hand-over margin left before their entry's index, by the
    /// earliest index an entry reaching the RT thread then is applied at:
    /// the supervisor loop's work and the assembly outlasted the margin,
    /// or the entry came in or was committed that late. Counted whether
    /// the generation arrives in time or not.
    pub generation_past_margin: AtomicU64,
    /// Strip entries the slot's or output's strip refused, or for one
    /// without a strip, limiter entries for an output without a limiter,
    /// delays a slot's or output's line refused and tone entries a
    /// generator refused or a slot without one got: not applied, and each
    /// one marks the engine diverged.
    pub strip_refused: AtomicU64,
    /// Duck settings a detector refused: not applied. The engine checks
    /// them against its configuration when they are appended, so none is
    /// expected.
    pub duck_refused: AtomicU64,
    /// Duck group readings not sent because their ring was full.
    pub duck_records_dropped: AtomicU64,
    /// The index of the next sample the matrix produces, published after
    /// each block.
    pub produced: AtomicU64,
    /// Log entries the RT thread took off its pending list: applied,
    /// refused by the matrix or late.
    pub entries_consumed: AtomicU64,
    /// Telemetry records not sent because the ring was full.
    pub telemetry_dropped: AtomicU64,
    /// Meter records not sent because the meter ring was full.
    pub meter_dropped: AtomicU64,
    /// Block verdicts not sent because the verdict channel was full.
    pub verdicts_dropped: AtomicU64,
    /// Reports not sent because the report channel was full.
    pub reports_dropped: AtomicU64,
    /// Largest wake lateness after `mxlSleepUntil(T_k)`, ns.
    pub wake_jitter_max_ns: AtomicU64,
    /// Largest block processing time, ns.
    pub process_time_max_ns: AtomicU64,
    /// Minor page faults of the RT thread, read by the supervisor.
    pub rt_minor_faults: AtomicU64,
    /// The block after the last one whose statuses are set, published
    /// after them: the processed block index plus one.
    pub processed: AtomicU64,
    /// Blocks the RT thread withheld: skipped, overflowing the hold
    /// ring, unfillable, or below a commit start it computed.
    pub blocks_withheld: AtomicU64,
    /// Blocks withheld because the hold ring was full.
    pub hold_overflows: AtomicU64,
    /// Status changes the RT thread could not send: its change ring to
    /// the supervisor was full.
    pub status_changes_dropped: AtomicU64,
    /// Status changes the supervisor could not pass on: the replicated
    /// state's channel was full.
    pub status_changes_unsent: AtomicU64,
    /// Late re-routes the digest could not name: more than
    /// [`PENDING_CHANGES`] crossfades at once. Their blocks carry a
    /// degraded digest naming no entry.
    pub late_routes_lost: AtomicU64,
    /// Page hashes the RT thread could not send: its hash ring to the
    /// supervisor was full, more pages waited for their blocks than it
    /// keeps, or the page's entries did not fit its fingerprint list.
    pub page_hashes_dropped: AtomicU64,
    /// Replayed cores installed at a block boundary.
    pub handovers_installed: AtomicU64,
    /// Replayed cores refused: the RT thread had started a block after
    /// the one after theirs.
    pub handovers_passed: AtomicU64,
    /// Replayed cores refused: the RT thread held a pending generation
    /// they did not name.
    pub handovers_generation: AtomicU64,
    /// Replayed cores refused: the RT thread had taken an entry they did
    /// not know off its pending list.
    pub handovers_missed: AtomicU64,
    /// Pending entries a hand-over found no room for.
    pub handover_entries_lost: AtomicU64,
    /// Blocks not computed for want of a core, withheld.
    pub blocks_without_core: AtomicU64,
    /// Entries whose index passed without a core, taken off the pending
    /// list: strip entries applied to their strips, the others not.
    pub entries_without_core: AtomicU64,
    /// Spent batches neither the return ring nor the RT thread's spent
    /// list had room for: leaked rather than freed on the RT thread.
    pub batches_leaked: AtomicU64,
    /// Times a replicated engine dropped its core because more than
    /// `WAITING` committed entries waited for room in its entry ring.
    pub log_overflows: AtomicU64,
    /// Page hashes the supervisor could not pass on: the replicated
    /// state's channel was full.
    pub page_hashes_unsent: AtomicU64,
}

/// Makes the RT loop panic once, for the panic containment test.
#[cfg(test)]
pub(crate) static PANIC: AtomicBool = AtomicBool::new(false);

/// Stalls the RT loop once for this many ms, for the overrun tests.
#[cfg(test)]
pub(crate) static STALL_MS: AtomicU64 = AtomicU64::new(0);

/// The loop that takes [`STALL_MS`], by the address of its counters, or
/// 0 for any loop, as [`CLOCK_STEP_LOOP`] for a clock step.
#[cfg(test)]
pub(crate) static STALL_LOOP: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Shifts libmxl's clock as the RT loop reads it, for the clock-step
/// tests: its waits and sleeps take the shift off again.
#[cfg(test)]
pub(crate) static CLOCK_SHIFT_NS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// Added to [`CLOCK_SHIFT_NS`] at the RT loop's next wake, so that the
/// step lands between two blocks, not between a deadline and its sleep.
#[cfg(test)]
pub(crate) static CLOCK_STEP_NS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

/// The loop that takes [`CLOCK_STEP_NS`], by the address of its
/// counters, or 0 for any loop. With two engines in a test, the other
/// loop would take the step at its own wake, which can fall between a
/// deadline and the sleep of the loop under test.
#[cfg(test)]
pub(crate) static CLOCK_STEP_LOOP: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Pauses the loop of these counters, by address, once, in the step of
/// the first block from [`HOLD_PASS_PAUSE_K`] on: right after output 0
/// moved its ring with [`HOLD_PASS_PAUSE_IN_RING`], else after the pass
/// over the rings and before the outputs decide the block. It sets
/// [`HOLD_PASS_PAUSED`] and resumes once that is false again.
#[cfg(test)]
pub(crate) static HOLD_PASS_PAUSE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The block from which [`HOLD_PASS_PAUSE`] pauses.
#[cfg(test)]
pub(crate) static HOLD_PASS_PAUSE_K: AtomicU64 = AtomicU64::new(0);

/// Where [`HOLD_PASS_PAUSE`] pauses in the step.
#[cfg(test)]
pub(crate) static HOLD_PASS_PAUSE_IN_RING: AtomicBool = AtomicBool::new(false);

/// The loop [`HOLD_PASS_PAUSE`] names is paused.
#[cfg(test)]
pub(crate) static HOLD_PASS_PAUSED: AtomicBool = AtomicBool::new(false);

/// Pauses the loop of `counters` in the step of block `k`, `in_ring` or
/// not, as [`HOLD_PASS_PAUSE`] asks.
#[cfg(test)]
fn hold_pass_pause(counters: &Counters, k: Option<u64>, in_ring: bool) {
    let me = std::ptr::from_ref(counters) as usize;
    if HOLD_PASS_PAUSE.load(Acquire) != me
        || HOLD_PASS_PAUSE_IN_RING.load(Relaxed) != in_ring
        || k.is_none_or(|k| k < HOLD_PASS_PAUSE_K.load(Relaxed))
        || HOLD_PASS_PAUSE
            .compare_exchange(me, 0, Relaxed, Relaxed)
            .is_err()
    {
        return;
    }
    HOLD_PASS_PAUSED.store(true, Release);
    while HOLD_PASS_PAUSED.load(Acquire) {
        std::hint::spin_loop();
    }
}

/// The kernel TAI offset, `CLOCK_TAI` - `CLOCK_REALTIME` read back to back,
/// in whole seconds. Each read goes through the vDSO where the kernel has
/// one for the clock, else it is a `clock_gettime` syscall.
fn tai_offset() -> i64 {
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
    (tai - real + 500_000_000).div_euclid(1_000_000_000) as i64
}

fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

/// What happened to one block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockVerdict {
    /// Block `k`, the samples `[kB, (k + 1)B)`.
    pub k: u64,
    /// One bit per input slot whose block was silenced.
    pub silenced: u64,
    /// One bit per input slot whose fade-in was running.
    pub fading: u64,
    /// A matrix coefficient ramp was running.
    pub ramping: bool,
    /// One bit per output whose block was committed.
    pub written: u64,
    /// Log entries applied up to and including this block, in log order
    /// within an index; with `k` the point a joining replica's state is
    /// brought to.
    pub commands_applied: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every input copied, no ramp: each output is the matrix of its
    /// inputs after their strips.
    Clean,
    /// Every input copied, a fade or a coefficient ramp running.
    Ramp,
    /// At least one input silenced.
    Silenced,
}

impl BlockVerdict {
    pub fn verdict(&self) -> Verdict {
        if self.silenced != 0 {
            Verdict::Silenced
        } else if self.fading != 0 || self.ramping {
            Verdict::Ramp
        } else {
            Verdict::Clean
        }
    }
}

/// One second of an input slot's read results.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SlotStats {
    pub late: u32,
    pub on_time: u32,
    /// Smallest `head - range end` of the late blocks read with a head
    /// above 0; none without one. A head of 0 names no grain: its
    /// slack would be the negated absolute index.
    pub min_slack: Option<i64>,
    pub head: Option<u64>,
    /// libmxl's clock when the head was read.
    pub now: u64,
    pub counters: InputCounters,
}

pub(crate) enum Telemetry {
    Block(BlockVerdict),
    /// The slot is to be pruned: marked invalid, or its blocks have been
    /// late for the slot's prune limit, `frozen` with its head unchanged
    /// or for the longer one of a moving head.
    Prune {
        slot: usize,
        invalid: bool,
        frozen: bool,
        /// The last head seen and the end of the block range read.
        head: u64,
        end: u64,
    },
    Stats {
        slot: usize,
        stats: SlotStats,
    },
    ClockStep {
        delta_ns: i64,
        /// The TAI offset samples before and after the step.
        offset_before: i64,
        offset_after: i64,
    },
    /// A log entry reached the RT thread after the matrix produced its
    /// index; not applied.
    LateEntry {
        entry: RtEntry,
        /// The index of the next sample the matrix produced.
        pos: u64,
    },
    /// An `InputDown` or `InputUp` entry changed the slot's state.
    Input {
        slot: usize,
        up: bool,
    },
    /// A `PreStripMeter` entry, or a core installed, stopped the slot's
    /// pre-strip meter from index `at_index` on.
    PreStripMeterOff {
        slot: usize,
        at_index: u64,
    },
    /// The generation of entry `id` was swapped in, or handed back
    /// unused because the entry was late.
    GenerationDone {
        id: LogId,
        swapped: bool,
    },
    /// Entry `entry` was applied before its generation arrived while
    /// [`PENDING_CHANGES`] others awaited theirs: its generation is not
    /// swapped in, and its slot stays silent.
    AwaitLost {
        entry: RtEntry,
    },
    /// A strip, limiter, delay or tone entry its slot or output refused,
    /// or one for a slot or output without what it sets; not applied.
    StripRefused {
        entry: RtEntry,
    },
}

/// An input generation of one domain and, per slot, the number of
/// samples its blocks may stay late before the slot is pruned: with its
/// head frozen, and with its head moving.
pub(crate) struct Gen {
    pub mxl: Generation,
    pub prune_after: Vec<u64>,
    pub prune_late: Vec<u64>,
    /// The slots, by bit, a re-route moved to another flow: each one's
    /// reader is on the new flow, its outgoing reader on the old one.
    pub rerouted: u64,
}

pub(crate) enum Command {
    /// Replaces the generation of an input domain; the old one is
    /// returned.
    Generation { domain: usize, generation: Box<Gen> },
    /// The generation built for log entry `id`, swapped in at the entry's
    /// index. At most one is held.
    Pending {
        id: LogId,
        domain: usize,
        generation: Box<Gen>,
    },
    /// Hands an output's writer over; a replaced one is returned.
    Writer {
        output: usize,
        writer: Box<FlowWriter>,
    },
}

/// What the replicated state tells the RT thread about committing.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Control {
    /// Stop committing `output` at sample index `at`.
    StopAt { output: usize, at: u64 },
    /// The replica is withdrawn: every output commits through only.
    Withdraw,
    /// Entry `id`, at `at_index`, is unresolved: it degrades every block
    /// from its index.
    Unresolved { id: LogId, at_index: u64 },
    /// Entry `id` is decided.
    Resolved { id: LogId },
    /// The core is dropped: no block is computed until a hand-over
    /// installs one.
    DropCore,
}

/// Unresolved entries the RT thread holds, and the capacity of the
/// control ring.
pub(crate) const UNRESOLVED: usize = 64;

/// What the RT thread and the replicated state share.
pub(crate) struct Shared {
    /// F, the end of the highest block up to which every block is
    /// B1-valid: blocks ending above it are fenced.
    pub fence: AtomicU64,
    /// This replica's ranks ([`crate::blocks::Ranks`], packed), fixed into
    /// a block's status when F passes it; stored before the F they hold
    /// for.
    pub ranks: AtomicU64,
    /// The kernel clock is unsynchronised: blocks carry no digest.
    pub self_report: AtomicBool,
    /// The replicated state found this replica's output apart from its
    /// peers': diverged, or resyncing before the hand-over. The slices
    /// of each block computed while it is set are lost to the meters.
    pub diverged: AtomicBool,
    /// The clock steps the replicated state checked: a detected step
    /// fences until this reaches the count of steps detected.
    pub steps_cleared: AtomicU64,
    /// Per output.
    pub statuses: Vec<Statuses>,
    /// The block the RT thread computes, stored before any history sample
    /// of it, for history copies.
    pub started: Started,
}

/// The RT thread's part of a replicated engine.
pub(crate) struct Replica {
    pub shared: Arc<Shared>,
    pub changes: Producer<StatusChange>,
    pub control: Consumer<Control>,
    /// Unresolved entries, by log id. Capacity [`UNRESOLVED`].
    pub unresolved: Vec<(LogId, u64)>,
    /// The log ids of one block's digest.
    pub ids: Vec<LogId>,
    /// Re-routes swapped in after their index, with the end of the
    /// crossfade an on-time engine runs: their blocks differ from its
    /// until then. Capacity [`PENDING_CHANGES`].
    pub late_routes: Vec<(LogId, u64)>,
    /// The latest crossfade end of the late re-routes `late_routes` had
    /// no room for: blocks before it stay degraded.
    pub late_lost_until: u64,
    /// Where the page hashes go.
    pub hashes: Producer<PageHash>,
    /// Buffers for a copy of the core, allocated off the RT thread.
    pub cores: Consumer<Box<Core>>,
    /// The copies, back to the replicated state.
    pub copies: Producer<CoreCopy>,
    /// A replayed core to install at a block boundary.
    pub handovers: Consumer<Handover>,
    /// Handed-over cores back, installed or refused.
    pub handed: Producer<Handover>,
}

/// The index of a log entry in the log that orders it.
pub type LogId = u64;

/// What a committed log entry changes. Each sets an absolute target, so
/// one applied again once its ramp has ended changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum Change {
    /// Ramps one matrix coefficient to `gain`, over the ramp length from
    /// the index or as `fade` says.
    Gain {
        input: usize,
        output: usize,
        gain: f32,
        fade: Option<crate::engine::Fade>,
    },
    /// The input is silent from the index on, whatever is readable.
    InputDown { slot: usize },
    /// The input fades in from the index; needs a generation holding its
    /// reader.
    InputUp { slot: usize },
    /// The slot crossfades to another flow from the index; needs a
    /// generation holding both readers.
    Route { slot: usize },
    /// The slot crossfades to silence from the index and reads no flow
    /// until a `Route`; needs a generation holding the outgoing reader
    /// and none for the slot.
    Detach { slot: usize },
    /// Sets one parameter of the slot's strip from the index.
    Strip { slot: usize, param: StripParam },
    /// Sets one parameter of the output's strip from the index.
    OutputStrip { output: usize, param: StripParam },
    /// Sets the output's limiter from the index.
    Limiter {
        output: usize,
        params: LimiterParams,
    },
    /// Sets the slot's delay from the index, in samples.
    Delay { slot: usize, samples: u32 },
    /// Sets the output's delay from the index, in samples.
    OutputDelay { output: usize, samples: u32 },
    /// Sets the slot's tone generator from the index.
    Tone { slot: usize, params: ToneParams },
    /// The output is written from the index on, once it has a writer, or
    /// disabled: nothing is written from the index on and its writer is
    /// handed back.
    Output { output: usize, enabled: bool },
    /// The pre-strip meter of the slot, one configured with
    /// `preStripMeter`, runs from the index on, its measurement started
    /// afresh, or stops: its block is neither copied nor metered.
    PreStripMeter { slot: usize, enabled: bool },
    /// The loudness measurement of `meter`, an output or after the
    /// outputs a configured meter, runs from the index on, or is paused:
    /// the slices metered while it is are left out of it.
    LoudnessRun { meter: usize, running: bool },
    /// The loudness measurement of `meter` restarts at the first 100 ms
    /// window starting at or after the index.
    ResetLoudness { meter: usize },
    /// The engine entries of one log entry, folded: its gains, strip,
    /// output strip, limiter, delay, output delay and tone parameters
    /// (`strips` says whether it has any),
    /// `InputDown`s, `Output`s, `PreStripMeter`s, `LoudnessRun`s and
    /// loudness resets
    /// (`resets` says whether it has any) in
    /// a [`Batch`] the RT thread holds by log id, and by bit per slot the
    /// slots it raises after them and those it re-routes, all of one
    /// input domain, for which one generation is built; of the re-routed
    /// ones, `detached` are detached instead of moved to another flow.
    Batch {
        strips: bool,
        up: u64,
        routed: u64,
        detached: u64,
        resets: bool,
    },
}

/// The bit of input slot `slot` in a slot mask; at most
/// [`crate::config::MAX_INPUTS`], 64, slots.
pub(crate) fn bit(slot: usize) -> u64 {
    1 << slot
}

/// The slots of slot mask `mask`, ascending.
pub(crate) fn slots_of(mask: u64) -> impl Iterator<Item = usize> {
    (0..64).filter(move |&s| mask & bit(s) != 0)
}

impl Change {
    pub fn needs_generation(&self) -> bool {
        self.generating() != (0, 0)
    }

    /// The slots the change raises and those it re-routes, a detached
    /// slot among them, as masks: what its generation is built for.
    pub fn generating(&self) -> (u64, u64) {
        match *self {
            Change::InputUp { slot } => (bit(slot), 0),
            Change::Route { slot } | Change::Detach { slot } => (0, bit(slot)),
            Change::Batch { up, routed, .. } => (up, routed),
            _ => (0, 0),
        }
    }

    /// The slots of the re-routed ones the change detaches, as a mask.
    pub fn detaching(&self) -> u64 {
        match *self {
            Change::Detach { slot } => bit(slot),
            Change::Batch { detached, .. } => detached,
            _ => 0,
        }
    }

    /// Whether a core holds what this change did once applied: a gain
    /// change is in the matrix, an `InputDown` in the slot state, an
    /// `Output` in the outputs disabled, a `PreStripMeter` in the
    /// pre-strip meters on, a `LoudnessRun` in the meters paused and a
    /// strip entry in the strip of a core holding `strips`, but a
    /// swapped-in generation, the strips under a core built from
    /// product state and the meters a loudness reset restarts stay with
    /// the RT thread, so an entry making them must still reach it.
    pub fn in_core(&self, strips: bool) -> bool {
        match *self {
            Change::Gain { .. }
            | Change::InputDown { .. }
            | Change::Output { .. }
            | Change::PreStripMeter { .. }
            | Change::LoudnessRun { .. } => true,
            Change::Strip { .. }
            | Change::OutputStrip { .. }
            | Change::Limiter { .. }
            | Change::Delay { .. }
            | Change::OutputDelay { .. }
            | Change::Tone { .. } => strips,
            Change::InputUp { .. }
            | Change::Route { .. }
            | Change::Detach { .. }
            | Change::ResetLoudness { .. } => false,
            Change::Batch {
                strips: has,
                up,
                routed,
                resets,
                ..
            } => up | routed == 0 && (strips || !has) && !resets,
        }
    }

    /// `paused`, the meters paused by bit, after this change with its
    /// batch `b` applied: a `LoudnessRun`'s meter, or each of a batch's,
    /// paused or running as it says.
    pub fn paused_after(&self, paused: u128, b: Option<&Batch>) -> u128 {
        let set = |p: u128, meter: usize, running: bool| {
            let m = meter_bit(meter);
            if running { p & !m } else { p | m }
        };
        match *self {
            Change::LoudnessRun { meter, running } => set(paused, meter, running),
            Change::Batch { .. } => b.map_or(paused, |b| {
                (b.runs.iter()).fold(paused, |p, &(m, r)| set(p, m as usize, r))
            }),
            _ => paused,
        }
    }

    /// The meters, by bit, whose loudness measurement this change with its
    /// batch `b` restarts.
    pub fn resets(&self, b: Option<&Batch>) -> u128 {
        match *self {
            Change::ResetLoudness { meter } => meter_bit(meter),
            Change::Batch { resets: true, .. } => b.map_or(0, |b| {
                (b.resets.iter()).fold(0, |r, &m| r | meter_bit(m as usize))
            }),
            _ => 0,
        }
    }
}

/// The bit of meter `meter`, an output or a configured meter, in a mask of
/// [`METERS_IN_A_MASK`] meters; none past them.
pub(crate) fn meter_bit(meter: usize) -> u128 {
    u32::try_from(meter)
        .ok()
        .and_then(|m| 1u128.checked_shl(m))
        .unwrap_or(0)
}

/// A committed log entry, applied at sample index `at_index`, a multiple
/// of B. Entries have a ring of their own, so a full pending list never
/// holds back a generation or a writer.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RtEntry {
    pub id: LogId,
    pub at_index: u64,
    pub change: Change,
}

/// A committed log entry as the entry ring carries it: a
/// [`Change::Batch`] with its batch, allocated off the RT thread.
pub(crate) struct RingEntry {
    pub entry: RtEntry,
    pub batch: Option<Arc<Batch>>,
}

/// Log entries the RT thread holds before their index, and the capacity
/// of the entry ring: one each, however many engine entries it folds, as
/// the leader stamps at most `FUTURE_LIST` (the same 64) log entries
/// ahead.
pub(crate) const PENDING_CHANGES: usize = 64;

/// Spent batches the RT thread keeps while the return ring is full.
pub(crate) const SPENT: usize = 2 * PENDING_CHANGES;

pub(crate) enum Returned {
    Generation(Box<Gen>),
    Writer(Box<FlowWriter>),
    /// A batch applied, reported late or known already.
    Batch(Arc<Batch>),
}

pub(crate) struct Slot {
    /// Index of the slot's input domain.
    pub domain: usize,
    pub first_channel: usize,
    pub channels: usize,
    pub offset: i64,
    pub state: SlotState,
    /// The index the running or last fade-in started at.
    pub fade_start: Option<u64>,
    /// The slot's last processed block was silenced; the next copied
    /// block starts a fade-in.
    pub silenced_last: bool,
    /// The slot's state in the log: silent while down.
    pub up: bool,
    /// An entry for the slot was applied before its generation arrived:
    /// silent until it does.
    pub held: bool,
    /// The index a re-route's crossfade started at.
    pub xfade: Option<u64>,
    /// The read state of the flow a re-route leaves.
    pub prev: SlotState,
    /// Detached by a `Detach` from its index until a `Route`: the slot
    /// reads no flow, whatever its generation holds, and its silence is
    /// neither a failed read nor a silenced block.
    pub detached: bool,
    pub prune_requested: bool,
    pub stats: SlotStats,
    /// The slot's strip, run on each block computed after the copy, the
    /// fades and the delay; none on a slot wider than a strip.
    pub strip: Option<Strip>,
    /// The index of the next sample the strip processes.
    pub strip_pos: Option<u64>,
    /// The slot's delay line, run first on each block computed, after the
    /// copy and the fades, so its rings hold the input before the strip;
    /// none on a slot configured without one.
    pub delay: Option<Delay>,
    /// The slot's tone generator, which it computes in place of a read;
    /// none on a slot that reads flows.
    pub tone: Option<Tone>,
    /// The slot has a pre-strip meter, its config setting
    /// `preStripMeter`, which [`Slot::pre_strip_on`] runs.
    pub pre_strip: bool,
    /// The log has the slot's pre-strip meter on (`PreStripMeter`): each
    /// block is copied into the slot's channels of `prebuf` after the
    /// delay and before the strip, and metered there. Off at start.
    pub pre_strip_on: bool,
    /// The first of the slot's pre-strip matrix rows, after every
    /// input's rows: each block is copied into these channels of `inbuf`
    /// after the delay and before the strip. None on a slot whose config
    /// does not set `preStripRows`.
    pub pre_row: Option<usize>,
    /// The flow the slot reads from [`Rt::test_input`]: the re-routes
    /// swapped in so far. Its outgoing flow in a crossfade is the one
    /// before.
    #[cfg(test)]
    pub flow: u32,
}

impl Slot {
    /// A slot of `channels` channels from `first_channel` on, reading
    /// input domain `domain` `offset` samples off, up and waiting for its
    /// first block; it has a strip whose changes ramp over `ramp` samples
    /// when it is at most [`MAX_LEGS`] channels wide.
    pub fn new(
        domain: usize,
        first_channel: usize,
        channels: usize,
        offset: i64,
        ramp: u32,
    ) -> Slot {
        Slot {
            domain,
            first_channel,
            channels,
            offset,
            state: SlotState::default(),
            fade_start: None,
            silenced_last: true,
            up: true,
            held: false,
            xfade: None,
            prev: SlotState::default(),
            detached: false,
            prune_requested: false,
            stats: EMPTY_STATS,
            strip: (channels <= MAX_LEGS).then(|| Strip::new(channels, ramp)),
            strip_pos: None,
            delay: None,
            tone: None,
            pre_strip: false,
            pre_strip_on: false,
            pre_row: None,
            #[cfg(test)]
            flow: 0,
        }
    }

    /// Runs the delay and then the strip over block `dst` of `frames`
    /// samples per channel starting at sample index `start`, read,
    /// generated or silenced, and copies it into the slot's channels of
    /// `inbuf`; with [`Slot::pre_strip_on`], copies it after the delay
    /// and before the strip into those of `prebuf` too, and with
    /// [`Slot::pre_row`] into its pre-strip rows of `inbuf`, so every tap
    /// of the input is as late as its delay.
    fn finish(
        &mut self,
        start: u64,
        dst: &mut [f32],
        frames: usize,
        inbuf: &mut PlanarBuffer,
        prebuf: &mut PlanarBuffer,
    ) {
        // Silenced blocks too: the delay plays what came before.
        if let Some(d) = &mut self.delay {
            d.process(start, dst, frames);
        }
        if self.pre_strip_on {
            for (c, ch) in dst.chunks_exact(frames).enumerate() {
                prebuf.channel_mut(self.first_channel + c)[..frames].copy_from_slice(ch);
            }
        }
        if let Some(row) = self.pre_row {
            for (c, ch) in dst.chunks_exact(frames).enumerate() {
                inbuf.channel_mut(row + c)[..frames].copy_from_slice(ch);
            }
        }
        // Silenced blocks too: the strip's filters and dynamics ring out
        // on silence.
        self.run_strip(start, dst, frames);
        for (c, ch) in dst.chunks_exact(frames).enumerate() {
            inbuf.channel_mut(self.first_channel + c)[..frames].copy_from_slice(ch);
        }
    }

    /// Sets `param` on the strip from sample index `at_index`. False, and
    /// nothing set, when the slot has no strip or the strip refuses it.
    fn set_strip(&mut self, at_index: u64, param: StripParam) -> bool {
        set_strip(&mut self.strip, &mut self.strip_pos, at_index, param)
    }

    /// Runs the strip over block `dst` of `frames` samples per channel
    /// starting at sample index `start`.
    fn run_strip(&mut self, start: u64, dst: &mut [f32], frames: usize) {
        run_strip(&mut self.strip, &mut self.strip_pos, start, dst, frames);
    }
}

/// Sets `param` on `strip`, at `pos`, from sample index `at_index`. False,
/// and nothing set, when there is no strip or the strip refuses it.
fn set_strip(
    strip: &mut Option<Strip>,
    pos: &mut Option<u64>,
    at_index: u64,
    param: StripParam,
) -> bool {
    strip_to(strip, pos, at_index);
    strip.as_mut().is_some_and(|s| s.set(param))
}

/// Runs `strip`, at `pos`, over block `dst` of `frames` samples per
/// channel starting at sample index `start`.
fn run_strip(
    strip: &mut Option<Strip>,
    pos: &mut Option<u64>,
    start: u64,
    dst: &mut [f32],
    frames: usize,
) {
    strip_to(strip, pos, start);
    if let Some(strip) = strip {
        strip.process(dst, frames);
    }
    *pos = Some(start + frames as u64);
}

/// Brings `strip`, at `pos`, the index of the next sample it processes, to
/// sample index `index`: its ramps advance over samples not processed, so
/// a parameter's value depends on the sample index and the ordered entries
/// alone. A lower index follows a backward clock step: the position
/// re-anchors and the ramps hold where they are, never rewound. The
/// strip's dynamics compute their gains on the grid of the index wherever
/// the strip started.
fn strip_to(strip: &mut Option<Strip>, pos: &mut Option<u64>, index: u64) {
    if let Some(strip) = strip {
        if let Some(pos) = *pos
            && index > pos
        {
            strip.advance((index - pos) as usize);
        }
        strip.align(index);
    }
    *pos = Some(index);
}

/// A meter on channels of a buffer the RT thread computes: one
/// configured meter's matrix columns, or one input slot after or before
/// its strip.
pub(crate) struct Metered {
    pub first_channel: usize,
    pub meter: OutMeter,
}

pub(crate) struct Out {
    pub first_channel: usize,
    pub channels: usize,
    pub writer: Option<Box<FlowWriter>>,
    /// Enabled by the log: false from a disabling `Output`'s index until
    /// an enabling one, while the output holds no writer.
    pub enabled: bool,
    /// The first block written: the newest due block at handover.
    pub first_k: u64,
    pub meter: OutMeter,
    /// Statuses and the hold ring, in replicated mode.
    pub hold: Hold,
    /// The page hashes, in replicated mode with more than one voter.
    pub pages: Option<Pages>,
    /// The output's strip, when configured: run on every block computed,
    /// on the matrix's output before the limiter.
    pub strip: Option<Strip>,
    /// The index of the next sample the strip processes.
    pub strip_pos: Option<u64>,
    /// The output's delay line, when configured: run on every block
    /// computed, after the strip and before the limiter.
    pub delay: Option<Delay>,
    /// The output's limiter, when configured: run on every block
    /// computed, before the write, the page hash and the meters.
    pub limiter: Option<Limiter>,
}

impl Out {
    /// Runs the strip, then the delay line, then the limiter, over the
    /// output's channels of block `buf`, `frames` samples per channel
    /// starting at sample index `start`, through `scratch`, before anything
    /// reads them: the write, the page hash and the output's meter see what
    /// leaves, and the limiter's ceiling holds on it. An output with none
    /// of them is left as the matrix computed it.
    fn process(&mut self, start: u64, buf: &mut PlanarBuffer, scratch: &mut [f32], frames: usize) {
        if self.strip.is_none() && self.delay.is_none() && self.limiter.is_none() {
            return;
        }
        let legs = &mut scratch[..self.channels * frames];
        for (c, leg) in legs.chunks_exact_mut(frames).enumerate() {
            leg.copy_from_slice(&buf.channel(self.first_channel + c)[..frames]);
        }
        // An output without a strip keeps no position for one.
        if self.strip.is_some() {
            run_strip(&mut self.strip, &mut self.strip_pos, start, legs, frames);
        }
        if let Some(d) = &mut self.delay {
            d.process(start, legs, frames);
        }
        if let Some(limiter) = &mut self.limiter {
            limiter.process(start, legs, frames);
        }
        for (c, leg) in legs.chunks_exact(frames).enumerate() {
            buf.channel_mut(self.first_channel + c)[..frames].copy_from_slice(leg);
        }
    }

    /// Sets `param` on the strip from sample index `at_index`. False, and
    /// nothing set, when the output has no strip or the strip refuses it.
    fn set_strip(&mut self, at_index: u64, param: StripParam) -> bool {
        self.strip.is_some() && set_strip(&mut self.strip, &mut self.strip_pos, at_index, param)
    }
}

/// The shortest sleep between two reads of the input heads while the RT
/// thread waits for a block's input.
const HEAD_POLL_NS: u64 = 100_000;

/// Samples of a meter slice: 10 ms at 48 kHz.
pub(crate) const SLICE: u64 = samplez_dsp::SLICE;
const WINDOW: u64 = samplez_dsp::WINDOW;
/// Input samples before the current one in the true-peak interpolation
/// window.
const TP_HISTORY: usize = TruePeak::HISTORY;

/// One metered channel's values over one slice, the samples
/// `[slice * SLICE, (slice + 1) * SLICE)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MeterRecord {
    /// Counts every record the RT thread produced, sent or not, so a gap
    /// shows that one was lost.
    pub seq: u64,
    /// The meter: the outputs, then the configured meters, then the
    /// inputs, then the inputs' pre-strip meters, each in configuration
    /// order.
    pub meter: u32,
    pub channel: u32,
    pub slice: u64,
    /// Samples of the slice that were computed; fewer than [`SLICE`]
    /// after a skipped block or at the start. For a [`SPAN`] record, the
    /// slices it covers.
    pub frames: u32,
    pub peak: f32,
    /// 0 for an input whose config does not set `truePeak`, whose meter
    /// has no true peak.
    pub true_peak: f32,
    /// Sum of squares of the K-weighted samples.
    pub sum_sq: f64,
    /// Sum of squares of the samples, unweighted; for a [`CROSS`]
    /// record the sum of the products of legs 0 and 1.
    pub plain_sum_sq: f64,
    /// [`MeterRecord::reset`], or [`NO_RESET`].
    pub reset_at: u64,
    /// [`GAP`], [`DIVERGED`], [`PAUSED`], [`SPAN`] and [`CROSS`].
    pub flags: u8,
}

const _: () = assert!(std::mem::size_of::<MeterRecord>() <= 64);

/// [`MeterRecord::reset_at`] of a record that restarts nothing.
pub(crate) const NO_RESET: u64 = u64::MAX;
/// An input slot down by the log fed the meter's columns in a block of
/// the slice: the slice is a gap in its loudness measurement.
pub(crate) const GAP: u8 = 1;
/// A block of the slice was computed while the replica was diverged: the
/// slice is lost to its loudness measurement.
pub(crate) const DIVERGED: u8 = 2;
/// One record for the whole meter covering `frames` slices from `slice`
/// on, none of them computed: an overrun or a forward clock step skipped
/// them. They are lost with [`DIVERGED`], else paused with [`PAUSED`],
/// else gaps with [`GAP`], else lost.
pub(crate) const SPAN: u8 = 4;
/// The log held the meter's loudness measurement paused in a block of
/// the slice, computed or skipped: the slice is left out of it, neither a
/// gap nor lost.
pub(crate) const PAUSED: u8 = 8;
/// The slice's cross term of a meter that correlates legs 0 and 1, sent
/// before its channels' records: channel the meter's channel count,
/// the sum of their products in `plain_sum_sq`, the slice's flags.
pub(crate) const CROSS: u8 = 16;

impl MeterRecord {
    /// The loudness measurement restarts at this index, a window boundary
    /// at or before the slice: the K-weighting started from silence at
    /// the slice's first sample. Slices between the index and this one
    /// were not computed.
    pub fn reset(&self) -> Option<u64> {
        (self.reset_at != NO_RESET).then_some(self.reset_at)
    }

    pub fn gap(&self) -> bool {
        self.flags & GAP != 0
    }

    pub fn diverged(&self) -> bool {
        self.flags & DIVERGED != 0
    }

    pub fn span(&self) -> bool {
        self.flags & SPAN != 0
    }

    pub fn paused(&self) -> bool {
        self.flags & PAUSED != 0
    }

    pub fn cross(&self) -> bool {
        self.flags & CROSS != 0
    }
}

/// Per metered channel: sample peak, true peak (none on an input whose
/// config does not set `truePeak`) and K-weighted power of the slice in
/// progress, sent at its end.
pub(crate) struct OutMeter {
    pub tp: Option<TruePeak>,
    pub kw: KWeighting,
    /// The slice in progress.
    pub slice: Option<u64>,
    /// The index the pending reset applies at, a window boundary.
    pub reset_at: Option<u64>,
    /// The slice in progress starts the measurement the reset at this
    /// index restarts.
    pub reset: Option<u64>,
    /// The flags of the block being metered: [`GAP`] where an input slot
    /// down by the log feeds the meter's columns, [`DIVERGED`] where the
    /// replica is diverged, [`PAUSED`] where the log holds the meter's
    /// loudness measurement paused.
    pub block: u8,
    /// The flags of the slice in progress: those of its blocks, ORed.
    pub flags: u8,
    /// The first sample of a skip this meter has not sent yet.
    pub skip_from: u64,
    /// The index after the last sample metered.
    pub next: Option<u64>,
    /// Samples whose interpolated outputs the true peak leaves out: those
    /// whose interpolation window reaches back over a discontinuity.
    pub tp_skip: usize,
    pub frames: u32,
    pub peak: Vec<f32>,
    pub true_peak: Vec<f32>,
    pub sum_sq: Vec<f64>,
    pub plain_sum_sq: Vec<f64>,
    /// Where the meter correlates legs 0 and 1, the sum of their products
    /// over the slice in progress.
    pub cross: Option<f64>,
}

impl OutMeter {
    pub fn new(channels: usize) -> Self {
        Self {
            tp: Some(TruePeak::new(channels, SLICE as usize)),
            ..Self::without_true_peak(channels)
        }
    }

    /// An input's meter without `truePeak`: sample peak, power and
    /// K-weighted power only.
    pub fn without_true_peak(channels: usize) -> Self {
        Self {
            tp: None,
            kw: KWeighting::new(channels),
            slice: None,
            reset_at: None,
            reset: None,
            block: 0,
            flags: 0,
            skip_from: 0,
            next: None,
            tp_skip: 0,
            frames: 0,
            peak: vec![0.0; channels],
            true_peak: vec![0.0; channels],
            sum_sq: vec![0.0; channels],
            plain_sum_sq: vec![0.0; channels],
            cross: None,
        }
    }

    /// The meter, correlating legs 0 and 1 with `on`.
    pub fn correlating(self, on: bool) -> Self {
        Self {
            cross: on.then_some(0.0),
            ..self
        }
    }

    /// Restarts the loudness measurement at the first 100 ms window
    /// starting at or after `at_index`, replacing a reset still pending.
    pub fn set_reset(&mut self, at_index: u64) {
        self.reset_at = Some(at_index.div_ceil(WINDOW) * WINDOW);
    }

    /// Starts the meter afresh from `at_index`, after samples it did not
    /// meter: the slice in progress is dropped unsent, and the first
    /// slice starting at or after the index restarts the measurement, its
    /// record carrying the reset, so the readings fold from there on
    /// alone. Allocates nothing.
    pub fn restart(&mut self, at_index: u64) {
        self.slice = None;
        self.reset = None;
        self.reset_at = Some(at_index.div_ceil(SLICE) * SLICE);
        self.flags = 0;
        self.frames = 0;
        self.peak.fill(0.0);
        self.true_peak.fill(0.0);
        self.sum_sq.fill(0.0);
        self.plain_sum_sq.fill(0.0);
        if let Some(c) = &mut self.cross {
            *c = 0.0;
        }
    }

    /// Adds `frames` samples of output channels `first..` of `buf`,
    /// starting at sample index `start`, and sends each slice that ends or
    /// is left unfinished.
    fn add(
        &mut self,
        sink: &mut MeterSink<'_>,
        first: usize,
        buf: &PlanarBuffer,
        start: u64,
        frames: usize,
    ) {
        // After samples went unmetered (an overrun skip, a clock step
        // either way) the interpolator would mix samples from both sides
        // of the discontinuity. Its history restarts from silence, and the
        // outputs of the next 11 samples, which reach back into it, are
        // not measured.
        if self.next.is_some_and(|x| x != start)
            && let Some(tp) = &mut self.tp
        {
            tp.reset();
            self.tp_skip = TP_HISTORY;
        }
        self.next = Some(start + frames as u64);
        let mut n = 0;
        while n < frames {
            let index = start + n as u64;
            let slice = index / SLICE;
            if self.slice != Some(slice) {
                self.flush(sink);
                self.slice = Some(slice);
                // From a reset on, the measurement depends only on the
                // output from 11 samples before its index on: the
                // K-weighting restarts from silence, and the true-peak
                // interpolator keeps its history of the last output
                // samples, so it does not overshoot on partial sums.
                // Replicas that computed those samples measure the same,
                // whenever they started.
                if let Some(t) = self.reset_at.filter(|&t| slice * SLICE >= t) {
                    self.reset_at = None;
                    self.reset = Some(t);
                    self.kw.reset();
                }
            }
            let len = ((slice + 1) * SLICE - index).min((frames - n) as u64) as usize;
            self.flags |= self.block;
            for c in 0..self.peak.len() {
                let x = &buf.channel(first + c)[n..n + len];
                accumulate(x, &mut self.peak[c], &mut self.plain_sum_sq[c]);
                if let Some(tp) = &mut self.tp {
                    let skip = self.tp_skip.min(len);
                    if skip > 0 {
                        tp.process(c, &x[..skip]);
                    }
                    if skip < len {
                        let v = tp.process(c, &x[skip..]);
                        if v > self.true_peak[c] || v.is_nan() {
                            self.true_peak[c] = v;
                        }
                    }
                }
                self.sum_sq[c] += self.kw.process(c, x);
            }
            if let Some(cross) = &mut self.cross {
                let (l, r) = (buf.channel(first), buf.channel(first + 1));
                for (&a, &b) in l[n..n + len].iter().zip(&r[n..n + len]) {
                    *cross += f64::from(a) * f64::from(b);
                }
            }
            self.tp_skip -= self.tp_skip.min(len);
            self.frames += len as u32;
            n += len;
            if (index + len as u64).is_multiple_of(SLICE) {
                self.flush(sink);
            }
        }
    }

    /// Samples `[from, to)` were not computed: an overrun or a forward
    /// clock step skipped them, `flags` telling whether an input slot down
    /// by the log fed the meter there ([`GAP`]), the log held its
    /// measurement paused there ([`PAUSED`]) and the replica was diverged
    /// ([`DIVERGED`]). A slice in progress takes the flags
    /// and is sent once the samples reach its end; whole slices go out as
    /// [`SPAN`] records, one per run; a slice the samples end inside is
    /// started with the flags, for the block computed next to fill.
    fn skip(&mut self, sink: &mut MeterSink<'_>, from: u64, to: u64, flags: u8) {
        let mut i = from;
        while i < to {
            let slice = i / SLICE;
            let end = (slice + 1) * SLICE;
            if self.slice == Some(slice) {
                self.flags |= flags;
                if end <= to {
                    self.flush(sink);
                }
                i = end.min(to);
                continue;
            }
            self.flush(sink);
            // A reset whose window the skipped samples reach applies to
            // the first slice at or after it, as when it is computed.
            let mut reset = None;
            if let Some(t) = self.reset_at.filter(|&t| t < to) {
                if t > i {
                    // The run up to the reset's slice goes out first.
                    let first = t / SLICE;
                    self.span(sink, slice, first - slice, flags, None);
                    i = first * SLICE;
                    continue;
                }
                self.reset_at = None;
                self.kw.reset();
                reset = Some(t);
            }
            if end <= to && i == slice * SLICE {
                let n = (to - i) / SLICE;
                self.span(sink, slice, n, flags, reset);
                i += n * SLICE;
            } else {
                // Taken up again above, or by the block computed next.
                self.slice = Some(slice);
                self.reset = reset;
                self.flags = flags;
            }
        }
    }

    /// Sends `n` whole slices from `slice` on as [`SPAN`] records.
    fn span(
        &mut self,
        sink: &mut MeterSink<'_>,
        slice: u64,
        n: u64,
        flags: u8,
        reset: Option<u64>,
    ) {
        let (mut slice, mut n, mut reset) = (slice, n, reset);
        while n > 0 {
            let len = n.min(u64::from(u32::MAX));
            let r = MeterRecord {
                seq: *sink.seq,
                meter: sink.meter,
                channel: 0,
                slice,
                frames: len as u32,
                peak: 0.0,
                true_peak: 0.0,
                sum_sq: 0.0,
                plain_sum_sq: 0.0,
                reset_at: reset.take().unwrap_or(NO_RESET),
                flags: SPAN | flags,
            };
            *sink.seq += 1;
            if sink.ring.push(r).is_err() {
                add(&sink.counters.meter_dropped, 1);
            }
            slice += len;
            n -= len;
        }
    }

    /// Sends the slice in progress, if any, and clears it.
    fn flush(&mut self, sink: &mut MeterSink<'_>) {
        let Some(slice) = self.slice.take() else {
            return;
        };
        if let Some(cross) = self.cross.as_mut().map(std::mem::take) {
            let r = MeterRecord {
                seq: *sink.seq,
                meter: sink.meter,
                channel: self.peak.len() as u32,
                slice,
                frames: self.frames,
                peak: 0.0,
                true_peak: 0.0,
                sum_sq: 0.0,
                plain_sum_sq: cross,
                reset_at: NO_RESET,
                flags: self.flags | CROSS,
            };
            *sink.seq += 1;
            if sink.ring.push(r).is_err() {
                add(&sink.counters.meter_dropped, 1);
            }
        }
        for c in 0..self.peak.len() {
            let r = MeterRecord {
                seq: *sink.seq,
                meter: sink.meter,
                channel: c as u32,
                slice,
                frames: self.frames,
                peak: self.peak[c],
                true_peak: self.true_peak[c],
                sum_sq: self.sum_sq[c],
                plain_sum_sq: self.plain_sum_sq[c],
                reset_at: self.reset.unwrap_or(NO_RESET),
                flags: self.flags,
            };
            *sink.seq += 1;
            if sink.ring.push(r).is_err() {
                add(&sink.counters.meter_dropped, 1);
            }
        }
        self.frames = 0;
        self.reset = None;
        self.flags = 0;
        self.peak.fill(0.0);
        self.true_peak.fill(0.0);
        self.sum_sq.fill(0.0);
        self.plain_sum_sq.fill(0.0);
    }
}

/// The flags of meter `m` of a run or block: [`GAP`] by its bit in
/// `gaps`, [`PAUSED`] by its bit in `paused`, [`DIVERGED`] with
/// `diverged`.
fn flags(m: usize, gaps: u128, paused: u128, diverged: bool) -> u8 {
    let bit = meter_bit(m);
    (if gaps & bit != 0 { GAP } else { 0 })
        | (if paused & bit != 0 { PAUSED } else { 0 })
        | (if diverged { DIVERGED } else { 0 })
}

/// Whether an input channel of a slot in `down` feeds one of `columns`
/// of `matrix`: at most one look at each of their coefficients.
fn feeds(matrix: &Matrix, slots: &[Slot], down: u64, columns: Range<usize>) -> bool {
    // Slots hold ascending channels, so their ranges ascend in bit order,
    // and their pre-strip rows, after every input's rows, as well.
    let down_slots = || {
        let mut rest = down;
        std::iter::from_fn(move || {
            (rest != 0).then(|| {
                let s = &slots[rest.trailing_zeros() as usize];
                rest &= rest - 1;
                s
            })
        })
    };
    let rows = || {
        (down_slots().map(|s| s.first_channel..s.first_channel + s.channels))
            .chain(down_slots().filter_map(|s| s.pre_row.map(|r| r..r + s.channels)))
    };
    columns.into_iter().any(|c| matrix.feeds(rows(), c))
}

/// The meters a skip's masks hold by bit: every output and configured
/// meter of the largest configuration.
const METERS_IN_A_MASK: usize = u128::BITS as usize;
const _: () = assert!(crate::config::MAX_OUTPUTS + crate::config::MAX_METERS <= METERS_IN_A_MASK);

/// What the meters of a skip follow over its runs ([`Mix::bring_to`]):
/// the slots' state and, per slot down, the meters it feeds, each looked
/// up once and again only after an entry changes a target of its rows,
/// and the meters paused and reset by the entries applied; a meter's
/// skipped samples go out once its gap or pause changes or it is reset,
/// and at the end.
struct SkipState {
    /// The slots up and the slots detached, from the first run on.
    slots: Option<(u64, u64)>,
    /// Per slot, the meters, by bit, its rows feed; valid for `known`.
    feeds: [u128; 64],
    known: u64,
    /// The meters paused, by bit, from the next run on.
    paused: u128,
    /// The meters the entries at the next run's start reset, by bit.
    resets: u128,
    /// The meters' gaps and the meters paused over the run before, none
    /// before the first.
    gaps: Option<(u128, u128)>,
}

impl SkipState {
    /// From the meters paused before the skip.
    fn new(paused: u128) -> SkipState {
        SkipState {
            slots: None,
            feeds: [0; 64],
            known: 0,
            paused,
            resets: 0,
            gaps: None,
        }
    }

    /// The meters of `outs` then `buses` by bit that slot `s` feeds.
    fn feeds_of(matrix: &Matrix, s: &Slot, outs: &[Out], buses: &[Metered]) -> u128 {
        let rows = std::iter::once(s.first_channel..s.first_channel + s.channels)
            .chain(s.pre_row.map(|r| r..r + s.channels));
        let meters = (outs.iter().map(|o| (o.first_channel, o.meter.peak.len())))
            .chain(buses.iter().map(|m| (m.first_channel, m.meter.peak.len())));
        meters.enumerate().fold(0, |f, (i, (first, n))| {
            let fed = (first..first + n).any(|c| matrix.feeds(rows.clone(), c));
            f | u128::from(fed) << i
        })
    }

    /// Run `[from, to)`, the samples up to the next entry applied, or up
    /// to the block computed next when `last`: each meter whose gap or
    /// pause differs from the run before, or that the entries at `from`
    /// reset, sends the samples it held since that last changed, and a
    /// reset applies at `from`; with `last` every meter sends the rest.
    #[allow(clippy::too_many_arguments)]
    fn run(
        &mut self,
        sink: &mut MeterSink<'_>,
        slots: &[Slot],
        outs: &mut [Out],
        buses: &mut [Metered],
        matrix: &Matrix,
        (from, to, last): (u64, u64, bool),
        diverged: bool,
    ) {
        let (up, detached) = *self.slots.get_or_insert_with(|| {
            let mask = |f: fn(&Slot) -> bool| {
                (slots.iter().enumerate()).fold(0, |m, (i, s)| m | u64::from(f(s)) << i)
            };
            (mask(|s| s.up), mask(|s| s.detached))
        });
        let all = (0..slots.len()).fold(0u64, |a, s| a | bit(s));
        let mut rest = !up & !detached & all;
        let mut gaps = 0u128;
        while rest != 0 {
            let s = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            if self.known & bit(s) == 0 {
                self.feeds[s] = Self::feeds_of(matrix, &slots[s], outs, buses);
                self.known |= bit(s);
            }
            gaps |= self.feeds[s];
        }
        let n = outs.len() + buses.len();
        // At most 128 meters (`METERS_IN_A_MASK`), and none past `n` set.
        let every = (0..n).fold(0u128, |e, m| e | 1 << m);
        let paused = self.paused;
        let changed = self.gaps.map_or(every, |(g, p)| (g ^ gaps) | (p ^ paused));
        let mut rest = (changed | self.resets) & every;
        while rest != 0 {
            let m = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            let meter = meter_of(outs, buses, m);
            if let Some((g, p)) = self.gaps {
                sink.meter = m as u32;
                meter.skip(sink, meter.skip_from, from, flags(m, g, p, diverged));
            }
            meter.skip_from = from;
            if self.resets & 1 << m != 0 {
                meter.set_reset(from);
            }
        }
        self.resets = 0;
        self.gaps = Some((gaps, paused));
        if last {
            for m in 0..n {
                let meter = meter_of(outs, buses, m);
                sink.meter = m as u32;
                meter.skip(sink, meter.skip_from, to, flags(m, gaps, paused, diverged));
            }
        }
    }

    /// Entry `e` with its batch applies after the run before: the slots
    /// it takes down, raises, re-routes or detaches, and the slots whose
    /// rows' targets it sets, looked up again at the next run, and the
    /// meters it pauses, runs or resets, its index the next run's start.
    fn apply(&mut self, slots: &[Slot], matrix: &Matrix, e: &RtEntry, batch: Option<&Batch>) {
        self.paused = e.change.paused_after(self.paused, batch);
        self.resets |= e.change.resets(batch);
        let Some((up, detached)) = self.slots.as_mut() else {
            return;
        };
        // An input's row, or a pre-strip row after every input's.
        let slot_of = |row: usize| {
            let pre = |s: &Slot| {
                s.pre_row
                    .is_some_and(|r| (r..r + s.channels).contains(&row))
            };
            let inputs = slots.last().map_or(0, |s| s.first_channel + s.channels);
            if row < inputs {
                (slots.partition_point(|s| s.first_channel <= row)).saturating_sub(1)
            } else {
                // Gains are checked against the configured rows.
                (slots.iter().position(pre)).expect("a row past the inputs' is a pre-strip row")
            }
        };
        match e.change {
            Change::InputDown { slot } => *up &= !bit(slot),
            Change::Gain { input, .. } => self.known &= !bit(slot_of(input)),
            _ => {}
        }
        if let Some(b) = batch {
            for &slot in &b.downs {
                *up &= !bit(slot as usize);
            }
            let columns = matrix.outputs();
            b.gains
                .each(|k, _| self.known &= !bit(slot_of(k / columns)));
        }
        let (raised, routed) = e.change.generating();
        *detached = (*detached & !routed) | (e.change.detaching() & routed);
        *up |= raised;
    }
}

/// Meter `m` of `outs` then `buses`.
fn meter_of<'a>(outs: &'a mut [Out], buses: &'a mut [Metered], m: usize) -> &'a mut OutMeter {
    match m.checked_sub(outs.len()) {
        None => &mut outs[m].meter,
        Some(b) => &mut buses[b].meter,
    }
}

/// Where a meter's records go.
struct MeterSink<'a> {
    meter: u32,
    ring: &'a mut Producer<MeterRecord>,
    seq: &'a mut u64,
    counters: &'a Counters,
}

/// What the loop processes after a wake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Overdue blocks processed in order without a wait.
    pub catchup: Range<u64>,
    /// Samples skipped because the loop was more than `maxCatchupBlocks`
    /// behind; libmxl zero-fills them on the next commit.
    pub skipped: u64,
    /// The block waited for and processed next.
    pub k: u64,
    /// libmxl's clock is behind the last block processed: nothing is
    /// processed until it passes it again.
    pub idle: bool,
}

/// The blocks to process after a wake, from `next`, the block after the
/// last one processed, and `newest`, the newest block due. An overdue
/// backlog of up to `max_catchup` blocks is processed in order, beyond
/// that only the newest block. `k` is the block after the newest due one,
/// so after a forward clock step it re-anchors to the clock. No index is
/// computed twice: a wake up to a block early (libmxl rounds the index to
/// the nearest sample) waits for `next` as usual, and after a backward
/// step further back no block is processed until the clock is due at
/// `next` again.
pub(crate) fn plan(next: Option<u64>, newest: Option<u64>, max_catchup: u64, block: u64) -> Plan {
    let behind = |nx: u64| Plan {
        catchup: nx..nx,
        skipped: 0,
        k: nx,
        idle: true,
    };
    let Some(n) = newest else {
        return match next {
            Some(nx) => behind(nx),
            None => Plan {
                catchup: 0..0,
                skipped: 0,
                k: 0,
                idle: false,
            },
        };
    };
    if let Some(nx) = next
        && n + 1 < nx
    {
        if n + 2 < nx {
            return behind(nx);
        }
        return Plan {
            idle: false,
            ..behind(nx)
        };
    }
    let (first, skipped) = match next {
        Some(nx) if n >= nx && n - nx < max_catchup => (nx, 0),
        Some(nx) if n >= nx => (n, (n - nx) * block),
        _ => (n + 1, 0),
    };
    Plan {
        catchup: first..n + 1,
        skipped,
        k: n + 1,
        idle: false,
    }
}

/// Sets coefficient `(input, output, gain)` at index `at`: to follow
/// `fade` as it joins the value held ([`crate::engine::Fade::joined`]),
/// a function of the index alone once it is in, or over `ramp` samples
/// from the value held. Whether the matrix took it, and the index the
/// ramp ends at.
fn set_gain(
    matrix: &mut Matrix,
    ramp: u32,
    (input, output, gain): (usize, usize, f32),
    fade: Option<crate::engine::Fade>,
    at: u64,
) -> (bool, u64) {
    let Some(f) = fade else {
        return (matrix.set(input, output, gain, ramp), at + u64::from(ramp));
    };
    let held = if output < matrix.outputs() {
        matrix.gain(input, output)
    } else {
        0.0
    };
    let f = f.joined(gain, held, at, ramp);
    (
        matrix.set_resumed(input, output, gain, f.and_then(|f| f.ramp_at(gain, at))),
        f.map_or(at, |f| f.start.saturating_add(u64::from(f.len))),
    )
}

/// The matrix, its position on the sample timeline and the gain changes
/// waiting for their index.
pub(crate) struct Mix {
    pub matrix: Matrix,
    /// The duck detectors, which the matrix's ducked terms follow.
    pub ducks: Ducks,
    /// Per detector, the envelope of each sample of the block: scratch.
    pub envelopes: PlanarBuffer,
    pub block: u64,
    /// Length of every coefficient ramp in samples.
    pub ramp: u32,
    /// The index of the next sample the matrix produces.
    pub pos: Option<u64>,
    /// The exclusive end index of the latest coefficient ramp.
    pub ramp_end: u64,
    /// Pending entries, by `at_index`, in log order within one index.
    /// Capacity [`PENDING_CHANGES`], never exceeded.
    pub pending: Vec<RtEntry>,
    /// The batch of each [`Change::Batch`] pending or due, by log id.
    /// Capacity [`PENDING_CHANGES`], never exceeded: one per entry.
    pub batches: Vec<(LogId, Arc<Batch>)>,
    /// Entries other than gain changes taken off the pending list by the
    /// last [`Mix::to`], in order, each with whether it was late. They
    /// are applied by the loop.
    pub due: Vec<(RtEntry, bool)>,
    /// Entries applied: gain changes the matrix took, and the others.
    pub applied: u64,
    /// The highest log id taken off the pending list: applied, reported
    /// late, or passed to the loop.
    pub last_taken: LogId,
    /// The `(log id, at_index)` of the entries applied whose index lies
    /// in page `entries_page`, for its fingerprint. Empty capacity when
    /// no page is hashed; else room for every entry one page can take:
    /// [`PENDING_CHANGES`] per wake, one wake per block starting in it.
    pub page_entries: Vec<(LogId, u64)>,
    pub entries_page: Option<u64>,
    /// An entry of `entries_page` found the list full.
    pub entries_lost: bool,
    /// The meters whose loudness measurement the log holds paused, by
    /// bit: the outputs, then the configured meters
    /// ([`Change::LoudnessRun`]).
    pub paused: u128,
}

impl Mix {
    pub fn new(matrix: Matrix, block: u64, ramp: u32) -> Mix {
        Mix {
            matrix,
            ducks: Ducks::new(ramp),
            envelopes: PlanarBuffer::new(MAX_DUCKS, block as usize),
            block,
            ramp,
            pos: None,
            ramp_end: 0,
            pending: Vec::with_capacity(PENDING_CHANGES),
            batches: Vec::with_capacity(PENDING_CHANGES),
            due: Vec::with_capacity(PENDING_CHANGES),
            applied: 0,
            last_taken: 0,
            page_entries: Vec::new(),
            entries_page: None,
            entries_lost: false,
            paused: 0,
        }
    }

    /// Counts entry `e` applied and lists it for the fingerprint of the
    /// page holding its index. An entry reported late is not applied.
    pub fn applied_entry(&mut self, e: &RtEntry) {
        self.applied += 1;
        let list = &mut self.page_entries;
        if list.capacity() == 0 || Some(e.at_index / PAGE) != self.entries_page {
            return;
        }
        if list.len() < list.capacity() {
            list.push((e.id, e.at_index));
        } else {
            self.entries_lost = true;
        }
    }

    /// The entry fingerprint of page `entries_page`, once every entry in
    /// it was applied; none when one did not fit the list.
    pub fn fingerprint(&mut self) -> Option<u64> {
        (!self.entries_lost).then(|| crate::blocks::fingerprint(&mut self.page_entries))
    }

    /// Queues `entry` by its index and log id. False, and not queued,
    /// when [`PENDING_CHANGES`] are pending.
    pub fn push(&mut self, entry: RtEntry) -> bool {
        if self.pending.len() >= PENDING_CHANGES {
            return false;
        }
        let at = self
            .pending
            .partition_point(|c| (c.at_index, c.id) <= (entry.at_index, entry.id));
        self.pending.insert(at, entry);
        true
    }

    /// Queues `entry` with its batch, a [`Change::Batch`]'s. The batch
    /// back, and nothing queued, when [`PENDING_CHANGES`] are pending:
    /// the caller hands it off the RT thread.
    pub fn push_with(
        &mut self,
        entry: RtEntry,
        batch: Option<Arc<Batch>>,
    ) -> Result<(), Option<Arc<Batch>>> {
        if !self.push(entry) {
            return Err(batch);
        }
        if let Some(b) = batch {
            // As many as pending entries: room.
            self.batches.push((entry.id, b));
        }
        Ok(())
    }

    /// Takes the batch of entry `id` out of the list.
    pub fn take_batch(&mut self, id: LogId) -> Option<Arc<Batch>> {
        let n = self.batches.iter().position(|b| b.0 == id)?;
        Some(self.batches.swap_remove(n).1)
    }

    /// Whether entry `id` is pending.
    pub fn holds(&self, id: LogId) -> bool {
        self.pending.iter().any(|c| c.id == id)
    }

    /// Brings the matrix to the start of block `k`: every pending gain
    /// change due at or before `kB` is applied at its own index, and the
    /// ramps advance over samples not mixed, so a coefficient's value
    /// depends on the sample index and the ordered changes alone. A gain
    /// change whose index the matrix has already produced is passed to
    /// `late` with the matrix position, and not applied. Other due entries
    /// go to [`Mix::due`]. A block below the position follows a backward
    /// clock step: the position re-anchors to `kB` and the ramps hold
    /// where they are, never rewound.
    #[cfg(test)]
    pub fn to(&mut self, k: u64, late: impl FnMut(RtEntry, u64)) {
        self.bring_to(k, late, |_, _, _, _| {});
    }

    /// [`Mix::to`], handing `skipped` the samples between the position
    /// and `kB` that were not mixed, an overrun or a forward clock step
    /// having skipped them, in runs `[from, to)` over which neither the
    /// coefficients' targets nor the entries applied change: each run with
    /// the matrix holding the targets of its samples and the entry applied
    /// at its end with its batch, if any, the last run with none. Runs may
    /// be empty. Nothing is handed without a skip.
    pub fn bring_to(
        &mut self,
        k: u64,
        mut late: impl FnMut(RtEntry, u64),
        mut skipped: impl FnMut(u64, u64, &Matrix, Option<(&RtEntry, Option<&Batch>)>),
    ) {
        self.due.clear();
        let start = k * self.block;
        // A page's entries apply in blocks starting in it.
        if self.entries_page != Some(start / PAGE) {
            self.page_entries.clear();
            self.entries_page = Some(start / PAGE);
            self.entries_lost = false;
        }
        let mut pos = match self.pos {
            Some(p) if p > start => {
                // The remaining ramp length is kept.
                self.ramp_end = start + self.ramp_end.saturating_sub(p);
                start
            }
            Some(p) => p,
            None => start,
        };
        let mut run = (pos < start).then_some(pos);
        while let Some(&c) = self.pending.first()
            && c.at_index <= start
        {
            self.pending.remove(0);
            self.last_taken = self.last_taken.max(c.id);
            // A late entry is not applied, so no run ends at it.
            if let Some(from) = run.filter(|_| c.at_index >= pos) {
                let to = c.at_index.clamp(from, start);
                let batch = (self.batches.iter()).find(|b| b.0 == c.id).map(|b| &*b.1);
                skipped(from, to, &self.matrix, Some((&c, batch)));
                run = Some(to);
            }
            if matches!(c.change, Change::Batch { .. }) {
                // Its gains apply here, the rest with the other due
                // entries; a late one is reported there.
                if c.at_index >= pos {
                    self.matrix.advance((c.at_index - pos) as usize);
                    pos = c.at_index;
                    self.batch_gains(c);
                }
                self.due.push((c, c.at_index < pos));
                continue;
            }
            let Change::Gain {
                input,
                output,
                gain,
                fade,
            } = c.change
            else {
                self.due.push((c, c.at_index < pos));
                continue;
            };
            if c.at_index < pos {
                late(c, pos);
                continue;
            }
            self.matrix.advance((c.at_index - pos) as usize);
            pos = c.at_index;
            let (ok, end) = set_gain(
                &mut self.matrix,
                self.ramp,
                (input, output, gain),
                fade,
                c.at_index,
            );
            if ok {
                self.applied_entry(&c);
                if self.matrix.is_ramping(input, output) {
                    self.ramp_end = self.ramp_end.max(end);
                }
            }
        }
        if let Some(from) = run {
            skipped(from, start, &self.matrix, None);
        }
        self.matrix.advance((start - pos) as usize);
        self.pos = Some(start + self.block);
    }

    /// Sets the gains, duck masks and depths of batch entry `c` at its
    /// index, the matrix there, in that order.
    fn batch_gains(&mut self, c: RtEntry) {
        let Mix {
            matrix,
            batches,
            ramp,
            ramp_end,
            ..
        } = self;
        let ramp_len = *ramp;
        let Some((_, b)) = batches.iter().find(|b| b.0 == c.id) else {
            return;
        };
        let columns = matrix.outputs();
        b.gains.each(|k, gain| {
            let (input, output) = (k / columns, k % columns);
            let (ok, end) = set_gain(
                matrix,
                ramp_len,
                (input, output, gain),
                b.fades.get(k),
                c.at_index,
            );
            if ok && matrix.is_ramping(input, output) {
                *ramp_end = (*ramp_end).max(end);
            }
        });
        if !b.ducks_matrix() {
            return;
        }
        // A mask crossfades and a depth ramps over the ramp length.
        let mut moved = false;
        b.masks.each(|k, mask| {
            moved |= matrix.mask(k / columns, k % columns) != mask;
            let _ = matrix.set_mask(k / columns, k % columns, mask, *ramp);
        });
        for &(row, group, db) in &b.depths {
            let (row, group) = (row as usize, usize::from(group));
            moved |= matrix.depth(row, group) != db;
            let _ = matrix.set_depth(row, group, db, *ramp);
        }
        if moved {
            *ramp_end = (*ramp_end).max(c.at_index + u64::from(*ramp));
        }
    }

    /// Whether a coefficient ramp runs in block `k`.
    pub fn ramping(&self, k: u64) -> bool {
        k * self.block < self.ramp_end
    }

    /// Applies the duck settings of batch `b` at sample index `at_index`,
    /// the keys of slots of `legs(slot)` legs; the number a detector
    /// refused.
    pub fn apply_ducks(&mut self, at_index: u64, b: &Batch, legs: impl Fn(usize) -> usize) -> u64 {
        let mut refused = 0;
        for &(group, change) in &b.ducks {
            if !(self.ducks.core).set(at_index, usize::from(group), change, &legs) {
                refused += 1;
            }
        }
        refused
    }

    /// Mixes the block of `frames` samples from sample index `start` in
    /// `inbuf`, the inputs after their strips, into `outbuf`: the
    /// detectors key from `inbuf`, input slot `s` from its channel
    /// `first(s)` on, and the matrix ducks by their envelopes on the grid
    /// of the sample index. Each group's readings go to `sink`.
    pub fn block(
        &mut self,
        start: u64,
        frames: usize,
        first: impl Fn(usize) -> usize,
        inbuf: &PlanarBuffer,
        outbuf: &mut PlanarBuffer,
        sink: Option<&mut dyn FnMut(DuckRecord) -> bool>,
    ) {
        (self.ducks).process(start, frames, first, inbuf, &mut self.envelopes, sink);
        // The duck gains move on at the multiples of the interval of the
        // index, wherever the matrix started: a matrix copied without
        // ducks carries no position in the interval.
        self.matrix.align(start);
        (self.matrix).process_ducked(inbuf, &self.envelopes, outbuf, frames);
    }
}

pub(crate) struct Rt {
    pub api: Arc<MxlApi>,
    pub block: u64,
    pub margin: u64,
    pub max_catchup: u64,
    pub ramp: u32,
    pub gens: Vec<Option<Box<Gen>>>,
    /// The group wait of each input domain for the block processed.
    pub statuses: Vec<GroupWait>,
    pub slots: Vec<Slot>,
    pub outs: Vec<Out>,
    /// The configured meters, on the matrix columns after the outputs'.
    pub buses: Vec<Metered>,
    /// Each input slot's meter, after its strip.
    pub taps: Vec<Metered>,
    /// The meter before the strip of each input slot whose config sets
    /// `preStripMeter`, on `prebuf`.
    pub pre_taps: Vec<Metered>,
    pub mix: Mix,
    pub inbuf: PlanarBuffer,
    /// The blocks of the slots with [`Slot::pre_strip`] after their
    /// delays and before their strips, laid out as `inbuf`; other slots'
    /// channels are left as they are.
    pub prebuf: PlanarBuffer,
    pub outbuf: PlanarBuffer,
    /// One input's or one output's channels, planar.
    pub scratch: Vec<f32>,
    /// The outgoing flow's channels during a re-route's crossfade.
    pub scratch_out: Vec<f32>,
    /// The generation built for a pending entry, with the entry's id and
    /// the generation's input domain.
    pub pending_gen: Option<(LogId, usize, Box<Gen>)>,
    /// Entries applied before their generation arrived, in log order.
    /// Capacity [`PENDING_CHANGES`], never exceeded.
    pub awaiting_gen: Vec<RtEntry>,
    /// The highest log id taken from the entry ring.
    pub last_entry: LogId,
    /// Entries taken from the entry ring.
    pub taken: u64,
    /// The highest log id the last handed-over core knew: an entry up to
    /// it taken from the ring later is pending or applied already. A
    /// later core that knew less replaces the state of an earlier one, so
    /// an entry it did not know is taken as new.
    pub skip_upto: LogId,
    /// The first sample index the last handed-over core computed: an
    /// entry the core knew at or past it was on the core's pending list.
    pub install_at: u64,
    /// The highest log id the last replayed core knew: its strips stay
    /// until the next replayed core, a core built from product state
    /// leaving them, so a strip entry up to it is in them or was on that
    /// core's pending list.
    pub strips_upto: LogId,
    /// A replay clone: computes off the RT thread from past input, writes
    /// nothing and hashes every page it computes whole.
    pub replay: bool,
    /// The engine holds a core: false from a dropped core until a
    /// hand-over installs one.
    pub core: bool,
    pub cmd: Consumer<Command>,
    pub entries: Consumer<RingEntry>,
    /// Spent batches the return ring had no room for. Capacity
    /// [`SPENT`], never exceeded.
    pub spent: Vec<Arc<Batch>>,
    pub ret: Producer<Returned>,
    pub tel: Producer<Telemetry>,
    pub meters: Producer<MeterRecord>,
    /// Each duck group's readings per meter slice.
    pub duck_meters: Producer<DuckRecord>,
    /// The sequence number of the next meter record.
    pub meter_seq: u64,
    pub counters: Arc<Counters>,
    pub stop: Arc<AtomicBool>,
    /// Statuses, hold rings and fence, in replicated mode.
    pub replica: Option<Replica>,
    /// The index below which a detected clock step lowered F, until the
    /// step is cleared.
    pub step_at: Option<u64>,
    /// Feeds every readable slot no generation holds a reader for, and
    /// the outgoing flow of its crossfade, from this source instead of
    /// silence, its blocks counted copied: an offline loop then computes
    /// what the loop computes from flows carrying these samples.
    #[cfg(test)]
    pub test_input: Option<TestInput>,
}

/// A deterministic input for [`Rt::test_input`]: the sample of channel
/// `channel` of flow `flow` of slot `slot` at index `index`.
#[cfg(test)]
pub(crate) type TestInput = fn(slot: usize, flow: u32, channel: usize, index: u64) -> f32;

/// Fills `dst`, channels of `b` samples each, with `source`'s samples of
/// flow `flow` of slot `slot` from index `start` on: true, the block
/// copied. Without a source it is silence, and false.
#[cfg(test)]
fn synthesize(
    source: Option<TestInput>,
    slot: usize,
    flow: u32,
    start: u64,
    b: usize,
    dst: &mut [f32],
) -> bool {
    let Some(f) = source else {
        dst.fill(0.0);
        return false;
    };
    for (c, ch) in dst.chunks_exact_mut(b).enumerate() {
        for (n, v) in ch.iter_mut().enumerate() {
            *v = f(slot, flow, c, start + n as u64);
        }
    }
    true
}

/// A member flow's writer as the hold ring's sink.
struct WriterSink<'a> {
    writer: &'a mut FlowWriter,
    counters: &'a Counters,
    wrote: bool,
}

impl Sink for WriterSink<'_> {
    fn write(&mut self, start: u64, len: usize, data: &[f32], stride: usize) -> bool {
        match self.writer.write(start, len, data, stride) {
            Write::Committed => {
                self.wrote = true;
                true
            }
            Write::Held => {
                add(&self.counters.output_held_blocks, 1);
                false
            }
            Write::Failed(_) => {
                add(&self.counters.output_failed_writes, 1);
                false
            }
        }
    }
}

pub(crate) const EMPTY_STATS: SlotStats = SlotStats {
    late: 0,
    on_time: 0,
    min_slack: None,
    head: None,
    now: 0,
    counters: InputCounters {
        copied: 0,
        below_last_read: 0,
        late: 0,
        too_late: 0,
        too_early: 0,
        invalid: 0,
        other: 0,
        probes: 0,
    },
};

fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is writable; CLOCK_MONOTONIC is always available.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// libmxl's clock as the RT loop reads it, shifted in the clock-step
/// tests.
fn clock_now(api: &MxlApi) -> u64 {
    let now = time::now(api);
    #[cfg(test)]
    let now = now.saturating_add_signed(CLOCK_SHIFT_NS.load(Relaxed));
    now
}

/// What a block's statuses are set from now: F, lowered to `step_at`
/// by a detected clock step not yet cleared.
fn ctx(shared: &Shared, step_at: Option<u64>) -> Ctx {
    let fence = shared.fence.load(Acquire);
    Ctx {
        fence: step_at.map_or(fence, |s| fence.min(s)),
        ranks: crate::blocks::Ranks::unpack(shared.ranks.load(Acquire)),
    }
}

/// The degradation digest of the block starting at `start`, from the
/// inputs whose wanted read failed, by crossfade leg, the entries applied
/// without their generation, the re-routes swapped in late until the end
/// of the crossfade they skipped, and the unresolved entries at or below
/// `start`; none under a clock self-report.
/// Names late re-route `id` in the digest until `until`; false when
/// `late_routes` is full, and the blocks before `until` then carry a
/// degraded digest naming no entry.
fn note_late_route(r: &mut Replica, id: LogId, until: u64) -> bool {
    if r.late_routes.len() < PENDING_CHANGES {
        r.late_routes.push((id, until));
        return true;
    }
    r.late_lost_until = r.late_lost_until.max(until);
    false
}

fn digest(
    r: &mut Replica,
    awaiting: &[RtEntry],
    start: u64,
    incoming: u64,
    outgoing: u64,
) -> Option<u64> {
    r.late_routes.retain(|&(_, until)| start < until);
    if r.shared.self_report.load(Relaxed) {
        return None;
    }
    r.ids.clear();
    r.ids.extend(awaiting.iter().map(|e| e.id));
    r.ids.extend(r.late_routes.iter().map(|l| l.0));
    r.ids
        .extend(r.unresolved.iter().filter(|u| u.1 <= start).map(|u| u.0));
    // A late re-route the list had no room for: no log id names it.
    if start < r.late_lost_until {
        r.ids.push(LogId::MAX);
    }
    if r.ids.is_empty() && incoming == 0 && outgoing == 0 {
        return Some(crate::blocks::HEALTHY);
    }
    r.ids.sort_unstable();
    Some(crate::blocks::degraded_digest(&r.ids, incoming, outgoing))
}

/// Copies the core out of the RT thread's state into `c`, a buffer of
/// the same shape, without allocating.
#[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
fn capture_core(
    c: &mut Core,
    m: &Mix,
    slots: &[Slot],
    outs: &[Out],
    awaiting: &[RtEntry],
    pending_gen: Option<LogId>,
    r: &Replica,
) {
    c.matrix.copy_from(&m.matrix);
    c.ducks.copy_from(&m.ducks.core);
    c.pos = m.pos;
    c.ramp_end = m.ramp_end;
    c.pending.clear();
    c.pending.extend_from_slice(&m.pending);
    c.page_entries.clear();
    c.page_entries.extend_from_slice(&m.page_entries);
    c.entries_page = m.entries_page;
    c.entries_lost = m.entries_lost;
    c.outputs_disabled = (outs.iter().enumerate())
        .filter(|(_, o)| !o.enabled)
        .fold(0, |m, (o, _)| m | bit(o));
    c.pre_strip_meters = (slots.iter().enumerate())
        .filter(|(_, s)| s.pre_strip_on)
        .fold(0, |m, (s, _)| m | bit(s));
    c.loudness_paused = m.paused;
    for (d, o) in c.limiters.iter_mut().zip(outs) {
        // A limiter holds no heap memory: the copy allocates nothing.
        d.clone_from(&o.limiter);
    }
    // A prepared buffer's list holds no part, so clearing it frees
    // nothing.
    c.outs.clear();
    let processed = |o: &&Out| o.strip.is_some() || o.delay.is_some();
    for (o, out) in outs.iter().enumerate().filter(|(_, o)| processed(o)) {
        if c.outs.len() == c.outs.capacity() {
            debug_assert!(false, "a core buffer without room for its outputs");
            break;
        }
        // A strip holds no heap memory, a delay's span is copied off the
        // RT thread, and the list has room: the copy allocates nothing.
        let part = OutCore {
            strip: out.strip.clone(),
            strip_pos: out.strip_pos,
            delay: out.delay.as_ref().map(delay_core),
        };
        c.outs.push((o as u32, part));
    }
    for (d, s) in c.slots.iter_mut().zip(slots) {
        d.up = s.up;
        d.held = s.held;
        d.fade_start = s.fade_start;
        d.silenced_last = s.silenced_last;
        d.xfade = s.xfade;
        d.detached = s.detached;
        // A strip holds no heap memory: the copy allocates nothing.
        d.strip.clone_from(&s.strip);
        d.strip_pos = s.strip_pos;
        d.tone = s.tone;
        // The delay's state; its rings are copied off the RT thread, into
        // a span the buffer brought or that is allocated there.
        match (&mut d.delay, &s.delay) {
            (Some(c), Some(l)) => {
                c.taps = l.taps();
                c.pos = l.pos();
                c.floor = l.floor();
                c.set = l.rings().active() as u8;
            }
            (c, Some(l)) => *c = Some(delay_core(l)),
            (c, None) => {
                if c.is_some() {
                    debug_assert!(false, "a core buffer of another shape");
                }
            }
        }
    }
    c.put_batches(&m.batches);
    c.awaiting_gen.clear();
    c.awaiting_gen.extend_from_slice(awaiting);
    c.pending_gen = pending_gen;
    c.unresolved.clear();
    c.unresolved.extend_from_slice(&r.unresolved);
    c.late_routes.clear();
    c.late_routes.extend_from_slice(&r.late_routes);
    c.late_lost_until = r.late_lost_until;
}

/// Delay `l`'s state in a core, its span left to be copied off the RT
/// thread: allocates nothing.
fn delay_core(l: &Delay) -> DelayCore {
    DelayCore {
        taps: l.taps(),
        pos: l.pos(),
        floor: l.floor(),
        set: l.rings().active() as u8,
        span: Vec::new(),
    }
}

/// Moves output `o`'s ring below block `limit`
/// ([`crate::blocks::Hold::advance_below`]); what it did, and whether it
/// wrote. None without a writer.
fn advance_ring(
    out: &mut Out,
    o: usize,
    r: &mut Replica,
    counters: &Counters,
    ctx: Ctx,
    limit: u64,
) -> Option<(crate::blocks::Outcome, bool)> {
    let writer = out.writer.as_deref_mut()?;
    let mut changes = Changes {
        ring: &mut r.changes,
        dropped: &counters.status_changes_dropped,
        output: o as u32,
    };
    let mut sink = WriterSink {
        writer,
        counters,
        wrote: false,
    };
    let statuses = &r.shared.statuses[o];
    let moved = out
        .hold
        .advance_below(ctx, limit, statuses, &mut changes, &mut sink);
    Some((moved, sink.wrote))
}

/// The replica is withdrawn on every output, `cause` as
/// [`crate::blocks::Hold::withdraw`] takes it.
fn withdraw_all(outs: &mut [Out], r: &mut Replica, counters: &Counters, cause: bool) {
    for (o, out) in outs.iter_mut().enumerate() {
        if out.writer.is_none() {
            continue;
        }
        let mut changes = Changes {
            ring: &mut r.changes,
            dropped: &counters.status_changes_dropped,
            output: o as u32,
        };
        let w0 = out.hold.withheld;
        out.hold
            .withdraw(&r.shared.statuses[o], &mut changes, cause);
        add(&counters.blocks_withheld, out.hold.withheld - w0);
    }
}

impl Rt {
    /// libmxl's clock as the loop reads it.
    fn now(&self) -> u64 {
        clock_now(&self.api)
    }

    /// `mxlSleepUntil(timestamp)` on the clock [`Rt::now`] reads; it waits
    /// by design, so RealtimeSanitizer allows it. The allow is not
    /// exercised: libmxl sleeps with `clock_nanosleep`, which this
    /// RealtimeSanitizer runtime does not intercept.
    fn sleep_until(&self, timestamp: u64) {
        #[cfg(test)]
        let timestamp = timestamp.saturating_add_signed(-CLOCK_SHIFT_NS.load(Relaxed));
        samplez_mxl::rtsan::allow(|| time::sleep_until(&self.api, timestamp));
    }

    fn period_ns(&self) -> u64 {
        self.block * 1_000_000_000 / samplez_mxl::SAMPLE_RATE as u64
    }

    fn send(&mut self, t: Telemetry) -> bool {
        let ok = self.tel.push(t).is_ok();
        if !ok {
            add(&self.counters.telemetry_dropped, 1);
        }
        ok
    }

    fn give_back(&mut self, r: Returned) {
        if let Err(rtrb::PushError::Full(r)) = self.ret.push(r) {
            // Unreachable: a free slot was checked before the command was
            // popped, and only this thread pushes. Leaked rather than
            // released on the RT thread.
            std::mem::forget(r);
        }
    }

    /// The loop of plan 2.3. Per block k: wait for each input domain's
    /// heads to reach the group index until `T_k`, read, process and write,
    /// `mxlSleepUntil(T_k)`. After a wake, an overdue backlog of up to
    /// `maxCatchupBlocks` is processed in order without waiting, beyond
    /// that only the newest block. Returns its state when `stop` is set,
    /// or when the loop panics, with the panic's message, so that no
    /// libmxl object is released on the RT thread while it unwinds.
    pub fn run(mut self) -> (Self, Option<String>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.blocks()));
        let message = result.err().map(|p| {
            p.downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "a panic without a message".to_owned())
        });
        (self, message)
    }

    /// The loop body is real-time; the call that waits by design,
    /// `mxlSleepUntil`, is allowed by call site.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn blocks(&mut self) {
        let mut next: Option<u64> = None;
        // libmxl's clock, the CLOCK_MONOTONIC midpoint of the two reads
        // bracketing it and the bracket's width.
        let mut prev: Option<(u64, u64, u64)> = None;
        let period = self.period_ns();
        while !self.stop.load(Relaxed) {
            #[cfg(test)]
            if PANIC.swap(false, Relaxed) {
                panic!("the panic test hook");
            }
            #[cfg(test)]
            let stall = match STALL_LOOP.load(Relaxed) {
                only if only == 0 || only == Arc::as_ptr(&self.counters) as usize => {
                    STALL_MS.swap(0, Relaxed)
                }
                _ => 0,
            };
            #[cfg(test)]
            #[allow(clippy::disallowed_methods, reason = "a test hook")]
            match stall {
                0 => {}
                ms => samplez_mxl::rtsan::allow(|| {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }),
            }
            #[cfg(test)]
            {
                let only = CLOCK_STEP_LOOP.load(Relaxed);
                if only == 0 || only == Arc::as_ptr(&self.counters) as usize {
                    CLOCK_SHIFT_NS.fetch_add(CLOCK_STEP_NS.swap(0, Relaxed), Relaxed);
                }
            }
            let m0 = monotonic_ns();
            let now = self.now();
            let m1 = monotonic_ns();
            let offset = tai_offset();
            let (mono, width) = (m0 + (m1 - m0) / 2, m1 - m0);
            if let Some((t0, mono0, width0)) = prev {
                let step =
                    (i128::from(now) - i128::from(t0)) - (i128::from(mono) - i128::from(mono0));
                // Each midpoint lies within half its bracket of the read
                // of libmxl's clock.
                if step.unsigned_abs() > u128::from(period + width.max(width0)) {
                    let offset_before = self.counters.tai_offset_s.load(Relaxed);
                    // The offset sampled after the step is stored before the
                    // step is counted: a reader that sees the count sees it.
                    self.counters.tai_offset_s.store(offset, Relaxed);
                    self.counters.clock_steps.fetch_add(1, Release);
                    self.send(Telemetry::ClockStep {
                        delta_ns: step as i64,
                        offset_before,
                        offset_after: offset,
                    });
                    self.clock_step(next.unwrap_or(0) * self.block, step < 0);
                }
            }
            prev = Some((now, mono, width));
            self.counters.tai_offset_s.store(offset, Relaxed);
            let newest = time::newest_due_block(&self.api, now, self.block, self.margin);
            // Entries before commands: a generation handed over after its
            // entry's commit finds the entry pending.
            self.take_entries();
            self.drain_commands(newest.unwrap_or(0));
            self.control();
            // After a backward clock step writers hold until the clock
            // passes their last commit, and inputs never read below their
            // last range.
            self.clear_step();
            let Plan {
                catchup,
                skipped,
                k,
                idle,
            } = plan(next, newest, self.max_catchup, self.block);
            if idle {
                // Behind the last block processed, after a backward step:
                // held blocks still move, nothing is computed.
                add(&self.counters.heartbeat_blocks, 1);
                add(&self.counters.clock_behind_wakes, 1);
                self.advance_holds(None, self.pass_ctx());
                self.sleep_until(now + period);
                continue;
            }
            let first = if catchup.is_empty() { k } else { catchup.start };
            // The first block due this wake, overrun skips included.
            let due = first - skipped / self.block;
            if !self.core {
                // Without a core no block would have been computed: the
                // skipped ones are not computed for want of a core.
                if due < first {
                    self.no_core(due, first);
                }
            } else {
                add(&self.counters.overrun_skipped_samples, skipped);
                if skipped > 0 {
                    self.skipped(due, first);
                }
            }
            if !catchup.is_empty() {
                for s in &mut self.statuses {
                    *s = GroupWait::Skipped;
                }
            }
            self.take_handover(first);
            if !self.core {
                // No block is computed: each is withheld, held blocks
                // still move, and the entries whose index passed are
                // taken off the pending list.
                add(&self.counters.heartbeat_blocks, 1);
                self.advance_holds(None, self.pass_ctx());
                self.no_core(first, k + 1);
                self.pass_entries(k);
                let deadline = time::deadline(&self.api, k, self.block, self.margin);
                self.sleep_until(deadline);
                next = Some(k + 1);
                continue;
            }
            for k in catchup {
                let deadline = time::deadline(&self.api, k, self.block, self.margin);
                self.process(k, deadline);
            }
            let deadline = time::deadline(&self.api, k, self.block, self.margin);
            self.wait(k, deadline);
            self.process(k, deadline);
            self.sleep_until(deadline);
            let late = self.now().saturating_sub(deadline);
            self.counters.wake_jitter_max_ns.fetch_max(late, Relaxed);
            next = Some(k + 1);
        }
    }

    /// A clock step was detected before the block starting at `next`: F
    /// is lowered below it until the replicated state clears the step.
    /// After a backward step every block not yet final in the hold rings
    /// is withheld, cause skip, and the replica is withdrawn.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn clock_step(&mut self, next: u64, backward: bool) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        self.step_at = Some(self.step_at.map_or(next, |s| s.min(next)));
        self.counters.step_fenced.store(true, Relaxed);
        if !backward {
            return;
        }
        for (o, out) in self.outs.iter_mut().enumerate() {
            let Some(writer) = out.writer.as_deref_mut() else {
                continue;
            };
            let mut changes = Changes {
                ring: &mut r.changes,
                dropped: &self.counters.status_changes_dropped,
                output: o as u32,
            };
            let mut sink = WriterSink {
                writer,
                counters: &self.counters,
                wrote: false,
            };
            let w0 = out.hold.withheld;
            out.hold
                .withhold_open(&r.shared.statuses[o], &mut changes, &mut sink, Cause::Skip);
            add(&self.counters.blocks_withheld, out.hold.withheld - w0);
        }
        withdraw_all(&mut self.outs, r, &self.counters, true);
    }

    /// Lifts the step fence once the replicated state cleared every step
    /// detected.
    fn clear_step(&mut self) {
        if let Some(r) = &self.replica
            && self.step_at.is_some()
            && r.shared.steps_cleared.load(Acquire) >= self.counters.clock_steps.load(Relaxed)
        {
            self.step_at = None;
            self.counters.step_fenced.store(false, Relaxed);
        }
    }

    /// F and the ranks for one pass over the outputs, read once. Every
    /// withhold the supervisor stored before the F a pass reads is seen
    /// by every output in that pass: [`Rt::advance_holds`] withdraws the
    /// replica on every output for it before any block from it on moves
    /// at that F, and every block the pass decides is decided at that F.
    fn pass_ctx(&self) -> Option<Ctx> {
        self.replica.as_ref().map(|r| ctx(&r.shared, self.step_at))
    }

    /// Moves the blocks held in every output's ring and writes those
    /// committed; one bit per output written. A block withheld on any
    /// output, in its ring or block `next` before it is computed,
    /// withdraws the replica on every output once every ring moved below
    /// it, so no output commits at `ctx` a block from it on, whichever
    /// output withholds it. `ctx` is [`Rt::pass_ctx`], none without a
    /// replica.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn advance_holds(&mut self, next: Option<u64>, ctx: Option<Ctx>) -> u64 {
        let (Some(r), Some(ctx)) = (self.replica.as_mut(), ctx) else {
            return 0;
        };
        let mut written = 0;
        // The first block from which any output withdraws the replica for
        // a status it has not seen: every ring moves below it, then the
        // replica is withdrawn on every output before any block from it
        // on moves.
        let mut found: Option<(u64, bool)> = None;
        for (o, out) in self.outs.iter().enumerate() {
            if out.writer.is_some()
                && let Some((k, c)) = out.hold.withheld_unseen(&r.shared.statuses[o], next)
            {
                let (first, cause) = found.unwrap_or((k, false));
                found = Some((first.min(k), cause | c));
            }
        }
        if let Some((first, cause)) = found {
            for o in 0..self.outs.len() {
                if let Some((moved, wrote)) =
                    advance_ring(&mut self.outs[o], o, r, &self.counters, ctx, first)
                {
                    written |= u64::from(wrote) << o;
                    // As below: before the next output moves.
                    if moved.withdraw {
                        withdraw_all(&mut self.outs, r, &self.counters, moved.cause);
                    }
                }
            }
            withdraw_all(&mut self.outs, r, &self.counters, cause);
        }
        for o in 0..self.outs.len() {
            let out = &mut self.outs[o];
            let Some((moved, wrote)) = advance_ring(out, o, r, &self.counters, ctx, u64::MAX)
            else {
                continue;
            };
            if let Some(p) = out.pages.as_mut() {
                let statuses = &r.shared.statuses[o];
                p.settle(statuses, &mut r.hashes, &self.counters.page_hashes_dropped);
            }
            written |= u64::from(wrote) << o;
            // On every output before the next one advances: none commits a
            // block after this one withheld one.
            if moved.withdraw {
                withdraw_all(&mut self.outs, r, &self.counters, moved.cause);
            }
            #[cfg(test)]
            if o == 0 {
                hold_pass_pause(&self.counters, next, true);
            }
        }
        written
    }

    /// Waits for each input domain's readers to reach block `k`'s group
    /// index, until `deadline`: reads their heads, which are in the flows'
    /// shared memory, and sleeps until the slowest one can have reached it
    /// at the sample rate, [`HEAD_POLL_NS`] at least, half a block period
    /// and the deadline at most: a producer that lags is read when its
    /// samples are due, one that commits in batches or bursts to catch up
    /// within half a period of its commit, and a frozen head, falling
    /// further behind, costs two or three wakes per block. No
    /// libmxl call that looks a flow's files up, as the group wait's and the
    /// read's validity checks do once they time out, runs here: on a file
    /// system that waits for a missing name, a deleted flow would hold the
    /// RT thread for that wait. A frozen head is pruned instead, and its flow
    /// checked on the supervisor side.
    fn wait(&mut self, k: u64, deadline: u64) {
        let mut waited = false;
        let mut all_ok = true;
        for d in 0..self.gens.len() {
            let status = match &self.gens[d] {
                Some(g) if g.mxl.has_inputs() => {
                    waited = true;
                    let index = time::group_index(k, self.block, g.mxl.max_offset());
                    loop {
                        let behind = g.mxl.behind(index);
                        if behind == 0 {
                            break GroupWait::Ok;
                        }
                        let now = self.now();
                        if now >= deadline {
                            add(&self.counters.input_wait_timeout, 1);
                            break GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY);
                        }
                        let due =
                            behind.saturating_mul(1_000_000_000) / samplez_mxl::SAMPLE_RATE as u64;
                        add(&self.counters.input_wait_sleeps, 1);
                        let sleep = due.min(self.period_ns() / 2).max(HEAD_POLL_NS);
                        self.sleep_until(now.saturating_add(sleep).min(deadline));
                    }
                }
                _ => GroupWait::Skipped,
            };
            all_ok &= matches!(status, GroupWait::Ok);
            self.statuses[d] = status;
        }
        if waited && all_ok && self.now() < deadline {
            add(&self.counters.wait_early, 1);
        }
    }

    /// Moves committed entries from the entry ring to the pending list
    /// while it has room; an entry the list holds already needs none.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn take_entries(&mut self) {
        self.return_spent();
        while let Ok(head) = self.entries.peek() {
            // A hand-over brought the entry in already, pending or
            // applied by the replay.
            let held = self.mix.holds(head.entry.id);
            if !held && self.mix.pending.len() >= PENDING_CHANGES {
                return;
            }
            let Ok(RingEntry { entry, batch }) = self.entries.pop() else {
                return;
            };
            self.taken += 1;
            self.last_entry = self.last_entry.max(entry.id);
            if held {
                self.retire(batch);
                continue;
            }
            self.take(entry, batch);
        }
    }

    /// Takes entry `entry`, with its batch, off the ring: pending, or
    /// known to the last core installed.
    fn take(&mut self, mut entry: RtEntry, batch: Option<Arc<Batch>>) {
        if entry.id <= self.strips_upto {
            // Its strip parameters are in the strips the last replayed
            // core brought, or on its pending list, applied here at its
            // index; taken through that core.
            match &mut entry.change {
                Change::Strip { .. }
                | Change::OutputStrip { .. }
                | Change::Limiter { .. }
                | Change::Delay { .. }
                | Change::OutputDelay { .. }
                | Change::Tone { .. } => {
                    self.mix.last_taken = self.mix.last_taken.max(entry.id);
                    return;
                }
                // Its batch's strip parameters are left out.
                Change::Batch { strips, .. } => *strips = false,
                _ => {}
            }
        }
        if entry.id <= self.skip_upto {
            // The core knew it. It holds what gain changes and InputDowns
            // did; what else an entry below the install did did not reach
            // this thread, so that is applied as on time. One at or past
            // the install was on the core's pending list, applied here at
            // its index.
            if !entry.change.in_core(false) && entry.at_index < self.install_at {
                self.apply_known(entry, batch.as_deref(), true, true, true);
            }
            self.retire(batch);
            // Taken through the core, as one on the pending list.
            self.mix.last_taken = self.mix.last_taken.max(entry.id);
            return;
        }
        if let Err(b) = self.mix.push_with(entry, batch) {
            // Unreachable: room was checked.
            self.retire(b);
        }
    }

    /// The return ring's slots spent batches leave free: one per input
    /// domain and output and one more, for the generations and writers
    /// this thread hands back.
    fn reserve(&self) -> usize {
        self.gens.len() + self.outs.len() + 1
    }

    /// Hands this thread's handle on spent batch `batch` off it through
    /// the return ring, leaving the reserve free, or keeps it on the spent
    /// list until the ring has room: this thread never drops one, so it
    /// never frees one. One neither has room for is leaked and counted.
    /// A replay keeps it on the spent list, which it frees between blocks.
    pub(crate) fn retire(&mut self, batch: Option<Arc<Batch>>) {
        let Some(b) = batch else { return };
        let b = if !self.replay && self.ret.slots() > self.reserve() {
            match self.ret.push(Returned::Batch(b)) {
                Ok(()) => return,
                Err(rtrb::PushError::Full(Returned::Batch(b))) => b,
                Err(rtrb::PushError::Full(other)) => {
                    // Unreachable: a full ring hands back what was pushed,
                    // a batch. Leaked rather than released on the RT thread.
                    std::mem::forget(other);
                    return;
                }
            }
        } else {
            b
        };
        if self.spent.len() < self.spent.capacity() {
            self.spent.push(b);
        } else {
            // No room to keep it for the supervisor to free: leaked, and
            // counted, rather than released on the RT thread.
            add(&self.counters.batches_leaked, 1);
            std::mem::forget(b);
        }
    }

    /// Passes the spent batches kept to the return ring while it has
    /// more room than the reserve.
    fn return_spent(&mut self) {
        if self.replay {
            return;
        }
        while self.ret.slots() > self.reserve()
            && let Some(b) = self.spent.pop()
        {
            let _ = self.ret.push(Returned::Batch(b));
        }
    }

    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn drain_commands(&mut self, newest: u64) {
        loop {
            // Each command may return one object.
            if self.ret.slots() == 0 {
                return;
            }
            let Ok(cmd) = self.cmd.pop() else { return };
            match cmd {
                Command::Generation { domain, generation } => {
                    // Slots a product state moved to another flow read it
                    // anew from here, no crossfade.
                    let rerouted = generation.rerouted;
                    self.install(domain, generation);
                    for i in slots_of(rerouted) {
                        let slot = &mut self.slots[i];
                        let s = &mut slot.state;
                        s.last_end = None;
                        s.head = 0;
                        s.frozen_late_blocks = 0;
                        s.late_blocks = 0;
                        s.invalid = false;
                        slot.xfade = None;
                    }
                }
                Command::Pending {
                    id,
                    domain,
                    generation,
                } => {
                    if let Some(n) = self.awaiting_gen.iter().position(|e| e.id == id) {
                        // Its entry was applied without it: swapped in at
                        // once, the slot fading in from silence.
                        let e = self.awaiting_gen.remove(n);
                        self.swap_in(e, domain, generation, false);
                    } else if id > self.last_entry || self.mix.holds(id) {
                        // Its entry is pending, or still in the entry ring
                        // behind a full pending list.
                        match self.pending_gen.replace((id, domain, generation)) {
                            // Built again for the same entry, as after a
                            // product state: the entry still awaits it.
                            Some((old, _, g)) if old == id => {
                                self.give_back(Returned::Generation(g));
                            }
                            Some((old, _, g)) => self.gen_unused(old, g),
                            None => {}
                        }
                    } else {
                        self.gen_unused(id, generation);
                    }
                }
                Command::Writer { output, writer } => {
                    if !self.outs[output].enabled {
                        // Created before the output was disabled: back,
                        // for the supervisor to create anew once enabled.
                        self.hand_back(output, writer);
                        continue;
                    }
                    let out = &mut self.outs[output];
                    out.first_k = newest;
                    if let Some(mut old) = out.writer.replace(writer) {
                        // A new flow: nothing in the ring belongs to it.
                        // The committed blocks still in it go to the old
                        // flow they were committed for.
                        if let Some(r) = &mut self.replica {
                            let mut changes = Changes {
                                ring: &mut r.changes,
                                dropped: &self.counters.status_changes_dropped,
                                output: output as u32,
                            };
                            let mut sink = WriterSink {
                                writer: &mut old,
                                counters: &self.counters,
                                wrote: false,
                            };
                            out.hold.idle(
                                &r.shared.statuses[output],
                                &mut changes,
                                &mut sink,
                                Cause::Released,
                            );
                        }
                        // One replica commits from the newest due block on.
                        if out.hold.alone() {
                            out.hold.commit_from(newest);
                        }
                        self.give_back(Returned::Writer(old));
                    } else if out.hold.alone() {
                        out.hold.commit_from(newest);
                    }
                }
            }
        }
    }

    /// Takes the replicated state's control messages.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn control(&mut self) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        while let Ok(c) = r.control.pop() {
            match c {
                Control::StopAt { output, at } => self.outs[output].hold.stop_at(at),
                Control::Withdraw => withdraw_all(&mut self.outs, r, &self.counters, true),
                Control::Unresolved { id, at_index } => {
                    let u = &mut r.unresolved;
                    // The engine admits at most UNRESOLVED.
                    if let Err(n) = u.binary_search_by_key(&id, |e| e.0)
                        && u.len() < UNRESOLVED
                    {
                        u.insert(n, (id, at_index));
                    }
                }
                Control::Resolved { id } => r.unresolved.retain(|e| e.0 != id),
                Control::DropCore => {
                    self.core = false;
                    withdraw_all(&mut self.outs, r, &self.counters, true);
                }
            }
        }
        // After a withdrawal sent before them: a commit start decided
        // before it is stale.
        for (out, statuses) in self.outs.iter_mut().zip(&r.shared.statuses) {
            out.hold.take_request(statuses);
        }
    }

    /// Blocks `[from, to)` were skipped: withheld at the skip, which
    /// withdraws the replica.
    fn skipped(&mut self, from: u64, to: u64) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        add(&self.counters.blocks_withheld, to - from);
        if r.changes.push(StatusChange::Skipped { from, to }).is_err() {
            add(&self.counters.status_changes_dropped, 1);
        }
        withdraw_all(&mut self.outs, r, &self.counters, true);
    }

    /// Blocks `[from, to)` are not computed for want of a core: withheld,
    /// cause no core, on every output; the replica stays withdrawn.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn no_core(&mut self, from: u64, to: u64) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        add(&self.counters.blocks_withheld, to - from);
        add(&self.counters.blocks_without_core, to - from);
        if r.changes.push(StatusChange::NoCore { from, to }).is_err() {
            add(&self.counters.status_changes_dropped, 1);
        }
        self.counters.processed.store(to, Release);
    }

    /// Without a core: takes the entries due by block `k` that need no
    /// generation off the pending list, so that a long spell neither
    /// fills the list nor holds the log's entries back. A strip, limiter,
    /// delay, output delay or tone entry, and a batch's parameters, are
    /// applied to what the slots and outputs keep, and a loudness reset to
    /// the meters, which stay with the engine; the rest passes unapplied. A
    /// batch holding a generation-needing entry stays pending whole, as
    /// that entry does alone. A core installed later must know them, as it
    /// must know every entry taken.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn pass_entries(&mut self, k: u64) {
        let end = (k + 1) * self.block;
        let mut i = 0;
        while let Some(&e) = self.mix.pending.get(i) {
            if e.at_index >= end || e.change.needs_generation() {
                i += 1;
                continue;
            }
            self.mix.pending.remove(i);
            self.mix.last_taken = self.mix.last_taken.max(e.id);
            add(&self.counters.entries_without_core, 1);
            match e.change {
                Change::Strip { slot, param } => self.set_strip(e, slot, param),
                Change::OutputStrip { output, param } => self.set_output_strip(e, output, param),
                Change::Limiter { output, params } => self.set_limiter(e, output, params),
                Change::Delay { slot, samples } => self.set_delay(e, slot, samples),
                Change::OutputDelay { output, samples } => {
                    self.set_output_delay(e, output, samples);
                }
                Change::Tone { slot, params } => self.set_tone(e, slot, params),
                Change::ResetLoudness { .. } => self.reset_meters(&e, None),
                Change::Batch { strips, .. } => {
                    let b = self.mix.take_batch(e.id);
                    // Without strips, its strip and limiter parameters and
                    // delays are in those the last replayed core brought.
                    if let Some(b) = b.as_ref().filter(|_| strips) {
                        self.set_params(e, b);
                    }
                    self.reset_meters(&e, b.as_deref());
                    self.retire(b);
                }
                _ => {}
            }
        }
        self.publish_consumed();
    }

    /// Publishes the entries taken from the ring and done with: the
    /// taken ones still pending have ids up to the highest taken, those a
    /// hand-over brought in ahead of the ring above it.
    fn publish_consumed(&self) {
        let last = self.last_entry;
        let waiting = self.mix.pending.iter().filter(|e| e.id <= last).count() as u64;
        self.counters
            .entries_consumed
            .store(self.taken - waiting, Release);
    }

    /// Makes `generation` the one of input domain `domain` and hands the
    /// old one back. Slot state stays with the slots.
    pub(crate) fn install(&mut self, domain: usize, generation: Box<Gen>) {
        let old = self.gens[domain].take();
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if slot.domain != domain {
                continue;
            }
            let was = old.as_ref().is_some_and(|g| g.mxl.input(i).is_some());
            let is = generation.mxl.input(i).is_some();
            if is && !was {
                slot.state.invalid = false;
                slot.state.frozen_late_blocks = 0;
                slot.state.late_blocks = 0;
            }
            if is != was {
                slot.prune_requested = false;
            }
        }
        self.gens[domain] = Some(generation);
        if let Some(old) = old {
            self.give_back(Returned::Generation(old));
        }
    }

    /// Hands back the generation of entry `id`, which was late.
    fn gen_unused(&mut self, id: LogId, generation: Box<Gen>) {
        self.give_back(Returned::Generation(generation));
        self.send(Telemetry::GenerationDone { id, swapped: false });
    }

    /// Swaps in the generation of entry `e`, which raises or re-routes
    /// slots. A re-route on time starts its crossfade at the entry's
    /// index; one whose generation came late starts the new flow from
    /// silence.
    fn swap_in(&mut self, e: RtEntry, domain: usize, generation: Box<Gen>, on_time: bool) {
        let rerouted = generation.rerouted;
        self.install(domain, generation);
        let (up, routed) = e.change.generating();
        for i in slots_of(up | routed) {
            // Silent while another entry for the slot awaits its
            // generation.
            let held = self.awaiting_gen.iter().any(|a| {
                let (u, r) = a.change.generating();
                (u | r) & bit(i) != 0
            });
            let slot = &mut self.slots[i];
            slot.held = held;
            if rerouted & bit(i) != 0 {
                #[cfg(test)]
                {
                    slot.flow += 1;
                }
                slot.prev = slot.state.clone();
                let s = &mut slot.state;
                s.last_end = None;
                s.head = 0;
                s.frozen_late_blocks = 0;
                s.late_blocks = 0;
                s.invalid = false;
                // Anchored at the index whatever the slot's last block
                // was: an outgoing flow that is silent fades the new one
                // in.
                slot.xfade = on_time.then_some(e.at_index);
            }
        }
        // Late, a re-route skips its crossfade: the blocks differ from an
        // on-time engine's until the crossfade's end.
        if !on_time
            && routed & rerouted != 0
            && let Some(r) = self.replica.as_mut()
            && !note_late_route(r, e.id, e.at_index.saturating_add(u64::from(self.ramp)))
        {
            add(&self.counters.late_routes_lost, 1);
        }
        self.send(Telemetry::GenerationDone {
            id: e.id,
            swapped: true,
        });
    }

    /// Sets parameter `param` of slot `slot`'s strip from the index of
    /// entry `e`, which carries it; a refusal is counted and reported.
    fn set_strip(&mut self, e: RtEntry, slot: usize, param: StripParam) {
        // Checked against the configuration that appended it; a slot of
        // another width here refuses it.
        if !self.slots[slot].set_strip(e.at_index, param) {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::Strip { slot, param },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets parameter `param` of output `output`'s strip from the index of
    /// entry `e`, which carries it; a refusal is counted and reported as a
    /// slot's strip's.
    fn set_output_strip(&mut self, e: RtEntry, output: usize, param: StripParam) {
        if !self.outs[output].set_strip(e.at_index, param) {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::OutputStrip { output, param },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets the limiter of output `output` from the index of entry `e`,
    /// which carries it; a refusal is counted and reported as a strip's.
    fn set_limiter(&mut self, e: RtEntry, output: usize, params: LimiterParams) {
        let set =
            (self.outs[output].limiter.as_mut()).is_some_and(|l| l.set_at(e.at_index, params));
        if !set {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::Limiter { output, params },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets the delay of slot `slot` from the index of entry `e`, which
    /// carries it; a delay of 0 on a slot without a delay line sets
    /// nothing, any other refusal is counted and reported as a strip's.
    fn set_delay(&mut self, e: RtEntry, slot: usize, samples: u32) {
        let set = match &mut self.slots[slot].delay {
            Some(d) => d.set(e.at_index, samples),
            None => samples == 0,
        };
        if !set {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::Delay { slot, samples },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets the delay of output `output` from the index of entry `e`,
    /// which carries it, as a slot's.
    fn set_output_delay(&mut self, e: RtEntry, output: usize, samples: u32) {
        let set = match &mut self.outs[output].delay {
            Some(d) => d.set(e.at_index, samples),
            None => samples == 0,
        };
        if !set {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::OutputDelay { output, samples },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets the tone generator of slot `slot` from the index of entry
    /// `e`, which carries it; a refusal, or a slot without one, is
    /// counted and reported as a strip's.
    fn set_tone(&mut self, e: RtEntry, slot: usize, params: ToneParams) {
        let set = (self.slots[slot].tone.as_mut()).is_some_and(|t| t.set(e.at_index, params));
        if !set {
            add(&self.counters.strip_refused, 1);
            let entry = RtEntry {
                change: Change::Tone { slot, params },
                ..e
            };
            self.send(Telemetry::StripRefused { entry });
        }
    }

    /// Sets strip, output strip, limiter, delay, output delay and tone
    /// parameters of batch `b`, entry `e`.
    fn set_params(&mut self, e: RtEntry, b: &Batch) {
        for &(slot, param) in &b.strips {
            self.set_strip(e, slot as usize, param);
        }
        for &(output, param) in &b.output_strips {
            self.set_output_strip(e, output as usize, param);
        }
        for &(output, params) in &b.limiters {
            self.set_limiter(e, output as usize, params);
        }
        for &(slot, samples) in &b.delays {
            self.set_delay(e, slot as usize, samples);
        }
        for &(output, samples) in &b.output_delays {
            self.set_output_delay(e, output as usize, samples);
        }
        for &(slot, params) in &b.tones {
            self.set_tone(e, slot as usize, params);
        }
    }

    /// Applies entry `e`, other than a gain change, at the start of the
    /// block at its index: a batch's `InputDown`s, duck settings, strip
    /// parameters, meters paused or run and generation-needing entry, its
    /// gains, duck masks and depths set by [`Mix::to`], and with `resets`
    /// its loudness resets, which the meters of a skip took otherwise. An
    /// entry that sets what the slot already has starts nothing.
    fn apply(&mut self, e: RtEntry, resets: bool) {
        if let Change::Batch { .. } = e.change {
            let b = self.mix.take_batch(e.id);
            self.mix.applied_entry(&e);
            self.mix.paused = e.change.paused_after(self.mix.paused, b.as_deref());
            if let Some(b) = &b {
                for &slot in &b.downs {
                    self.apply_change(RtEntry {
                        change: Change::InputDown {
                            slot: slot as usize,
                        },
                        ..e
                    });
                }
                for &(output, enabled) in &b.outputs {
                    self.set_output(output as usize, enabled);
                }
                for &(slot, enabled) in &b.pre_strip_meters {
                    self.set_pre_strip_meter(slot as usize, enabled, e.at_index);
                }
                let slots = &self.slots;
                let legs = |s: usize| slots.get(s).map_or(0, |s| s.channels);
                let refused = self.mix.apply_ducks(e.at_index, b, legs);
                add(&self.counters.duck_refused, refused);
            }
            self.apply_known(e, b.as_deref(), false, true, resets);
            self.retire(b);
            return;
        }
        self.mix.applied_entry(&e);
        self.mix.paused = e.change.paused_after(self.mix.paused, None);
        if resets {
            self.reset_meters(&e, None);
        }
        self.apply_change(e);
    }

    /// Applies what of entry `e`, with batch `b`, a core does not hold:
    /// its strip parameters with `strips`, its loudness resets with
    /// `resets`, and its generation-needing entry; a single entry whole
    /// but for a `LoudnessRun`. Counted applied for the fingerprint with
    /// `count`.
    fn apply_known(
        &mut self,
        e: RtEntry,
        b: Option<&Batch>,
        count: bool,
        strips: bool,
        resets: bool,
    ) {
        if count {
            self.mix.applied_entry(&e);
        }
        if resets {
            self.reset_meters(&e, b);
        }
        let Change::Batch { strips: has, .. } = e.change else {
            self.apply_change(e);
            return;
        };
        if let Some(b) = b.filter(|_| strips && has) {
            self.set_params(e, b);
        }
        self.apply_generation(e);
    }

    /// Restarts the loudness measurement of each meter entry `e`, with
    /// batch `b`, resets, at the first window at or after its index: one
    /// past the index is lost to the measurement up to the slice metered
    /// next. A replay, which meters its outputs only, resets those.
    fn reset_meters(&mut self, e: &RtEntry, b: Option<&Batch>) {
        let mut rest = e.change.resets(b);
        while rest != 0 {
            let m = rest.trailing_zeros() as usize;
            rest &= rest - 1;
            if m < self.outs.len() + self.buses.len() {
                meter_of(&mut self.outs, &mut self.buses, m).set_reset(e.at_index);
            }
        }
    }

    /// Applies change `e`, a single one other than a gain change; a
    /// meter's pause and reset are applied by the caller.
    fn apply_change(&mut self, e: RtEntry) {
        match e.change {
            Change::Gain { .. }
            | Change::Batch { .. }
            | Change::LoudnessRun { .. }
            | Change::ResetLoudness { .. } => {}
            Change::InputDown { slot } => {
                let s = &mut self.slots[slot];
                if s.up {
                    s.up = false;
                    s.xfade = None;
                    self.send(Telemetry::Input { slot, up: false });
                }
            }
            Change::Strip { slot, param } => self.set_strip(e, slot, param),
            Change::OutputStrip { output, param } => self.set_output_strip(e, output, param),
            Change::Limiter { output, params } => self.set_limiter(e, output, params),
            Change::Delay { slot, samples } => self.set_delay(e, slot, samples),
            Change::OutputDelay { output, samples } => self.set_output_delay(e, output, samples),
            Change::Tone { slot, params } => self.set_tone(e, slot, params),
            Change::Output { output, enabled } => self.set_output(output, enabled),
            Change::PreStripMeter { slot, enabled } => {
                self.set_pre_strip_meter(slot, enabled, e.at_index);
            }
            Change::InputUp { .. } | Change::Route { .. } | Change::Detach { .. } => {
                self.apply_generation(e);
            }
        }
    }

    /// Enables output `output` or disables it from the block being
    /// computed. A disabled output's writer is handed back: in replicated
    /// mode the committed blocks still in its ring are written to it and
    /// the open ones withheld first, as when a writer is replaced. An
    /// enabled one is written once a writer arrives. A replay only keeps
    /// the flag.
    fn set_output(&mut self, output: usize, enabled: bool) {
        self.outs[output].enabled = enabled;
        if self.replay {
            return;
        }
        let disabled = &self.counters.outputs_disabled;
        if enabled {
            disabled.fetch_and(!bit(output), Release);
            return;
        }
        disabled.fetch_or(bit(output), Release);
        let out = &mut self.outs[output];
        let Some(mut writer) = out.writer.take() else {
            return;
        };
        if let Some(r) = &mut self.replica {
            let mut changes = Changes {
                ring: &mut r.changes,
                dropped: &self.counters.status_changes_dropped,
                output: output as u32,
            };
            let mut sink = WriterSink {
                writer: &mut writer,
                counters: &self.counters,
                wrote: false,
            };
            out.hold.idle(
                &r.shared.statuses[output],
                &mut changes,
                &mut sink,
                Cause::Released,
            );
        }
        self.hand_back(output, writer);
    }

    /// Runs slot `slot`'s pre-strip meter from sample index `at_index` on,
    /// its measurement restarted, or stops it, telling the supervisor. A
    /// slot without one, which the entry's check refuses, is left as it
    /// is, and one already so starts nothing.
    fn set_pre_strip_meter(&mut self, slot: usize, enabled: bool, at_index: u64) {
        let Some(s) = self.slots.get_mut(slot).filter(|s| s.pre_strip) else {
            return;
        };
        if s.pre_strip_on == enabled {
            return;
        }
        s.pre_strip_on = enabled;
        if !enabled {
            self.send(Telemetry::PreStripMeterOff { slot, at_index });
            return;
        }
        // The slot's meter among the pre-strip ones, in slot order.
        let tap = self.slots[..slot].iter().filter(|s| s.pre_strip).count();
        if let Some(m) = self.pre_taps.get_mut(tap) {
            m.meter.restart(at_index);
        }
    }

    /// Returns output `output`'s writer `writer` and marks the output
    /// released, so the supervisor drops the writer, which deletes its
    /// flow, and creates one anew once the output is enabled.
    fn hand_back(&mut self, output: usize, writer: Box<FlowWriter>) {
        self.give_back(Returned::Writer(writer));
        (self.counters.outputs_released).fetch_or(bit(output), Release);
    }

    /// Applies what of entry `e` its generation is built for: the slots
    /// it raises fade in from its index, the re-routed ones are detached
    /// or attached as it says, and its generation is swapped in, or, not
    /// here yet, awaited with those slots and the re-routed ones held
    /// silent.
    fn apply_generation(&mut self, e: RtEntry) {
        let (up, routed) = e.change.generating();
        if up | routed == 0 {
            return;
        }
        let detached = e.change.detaching();
        for slot in slots_of(routed) {
            self.slots[slot].detached = detached & bit(slot) != 0;
        }
        for slot in slots_of(up) {
            if !self.slots[slot].up {
                // The fade-in runs from the entry's index, also on an
                // engine that skipped the blocks around it.
                let s = &mut self.slots[slot];
                s.up = true;
                s.fade_start = Some(e.at_index);
                s.silenced_last = false;
                self.send(Telemetry::Input { slot, up: true });
            }
        }
        match self.pending_gen.take() {
            Some((id, domain, g)) if id == e.id => self.swap_in(e, domain, g, true),
            other => {
                self.pending_gen = other;
                add(&self.counters.generation_late, 1);
                for slot in slots_of(up | routed) {
                    self.slots[slot].held = true;
                }
                if self.awaiting_gen.len() < PENDING_CHANGES {
                    self.awaiting_gen.push(e);
                } else {
                    self.send(Telemetry::AwaitLost { entry: e });
                }
            }
        }
    }

    /// The entries [`Mix::to`] took off for block `k`: applied, or
    /// reported late. The loudness resets of those below its start, in a
    /// skip, went to the meters with the skipped samples.
    fn apply_due(&mut self, k: u64) {
        let start = k * self.block;
        for n in 0..self.mix.due.len() {
            let (e, late) = self.mix.due[n];
            if !late {
                self.apply(e, e.at_index >= start);
                continue;
            }
            add(&self.counters.command_late, 1);
            self.send(Telemetry::LateEntry {
                entry: e,
                pos: k * self.block,
            });
            if let Some((id, _, g)) = self.pending_gen.take_if(|p| p.0 == e.id) {
                self.gen_unused(id, g);
            }
            let b = self.mix.take_batch(e.id);
            self.retire(b);
        }
        self.mix.due.clear();
    }

    /// Brings the matrix to block `k` ([`Mix::bring_to`]), reporting the
    /// late entries, and meters the samples skipped before it.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn bring_to_block(&mut self, k: u64) {
        // Entries apply at the start of the block at their index, before
        // its inputs are read. Samples skipped before the block are
        // metered as skipped, gaps where a slot the log holds down fed a
        // meter and paused where the log held its measurement paused,
        // and a reset applies at its window among them: the slots' and
        // meters' state is followed over the entries applied in them, as
        // the blocks computed there would have held it.
        let (counters, tel) = (&self.counters, &mut self.tel);
        let (slots, outs, buses) = (&self.slots, &mut self.outs, &mut self.buses);
        let (ring, seq) = (&mut self.meters, &mut self.meter_seq);
        let diverged = (self.replica.as_ref()).is_some_and(|r| r.shared.diverged.load(Relaxed));
        let late = |entry, pos| {
            add(&counters.command_late, 1);
            if tel.push(Telemetry::LateEntry { entry, pos }).is_err() {
                add(&counters.telemetry_dropped, 1);
            }
        };
        let mut skip = SkipState::new(self.mix.paused);
        let skipped =
            |from: u64, to: u64, matrix: &Matrix, next: Option<(&RtEntry, Option<&Batch>)>| {
                let mut sink = MeterSink {
                    meter: 0,
                    ring,
                    seq,
                    counters,
                };
                let last = next.is_none();
                skip.run(
                    &mut sink,
                    slots,
                    outs,
                    buses,
                    matrix,
                    (from, to, last),
                    diverged,
                );
                if let Some((e, batch)) = next {
                    skip.apply(slots, matrix, e, batch);
                }
            };
        self.mix.bring_to(k, late, skipped);
    }

    /// Reads, mixes and writes block `k`, due at `deadline`.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn process(&mut self, k: u64, deadline: u64) {
        let t0 = self.now();
        let b = self.block as usize;
        if let Some(r) = &self.replica {
            r.shared.started.begin(k);
        }
        self.bring_to_block(k);
        self.apply_due(k);
        self.counters.produced.store((k + 1) * self.block, Release);
        self.publish_consumed();
        let start = k * self.block;
        let mut silenced = 0u64;
        let mut fading = 0u64;
        // Inputs whose wanted read failed: an up slot not copied (late,
        // invalid, pruned, or held for its generation), and apart from it
        // the outgoing flow of a crossfade not copied. Self-known
        // degradations.
        let mut failed = 0u64;
        let mut failed_out = 0u64;
        // Slots down by the log, which every replica holds alike: a read
        // failed or held on this replica alone is not one of them.
        let mut down = 0u64;
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if !slot.up && !slot.detached {
                down |= 1 << i;
            }
            let dst = &mut self.scratch[..slot.channels * b];
            // A tone generator reads nothing: its samples are a function
            // of the index, never late, silenced or failed.
            if let Some(tone) = &mut slot.tone {
                tone.process(start, dst, b);
                slot.finish(start, dst, b, &mut self.inbuf, &mut self.prebuf);
                continue;
            }
            // None for a slot in no input domain.
            let generation = self.gens.get(slot.domain).and_then(|g| g.as_deref());
            let readable = slot.up && !slot.held;
            // A crossfade runs over the ramp from its index; a block below
            // it follows a backward clock step.
            let xfade = slot
                .xfade
                .map(|s| start.saturating_sub(s))
                .filter(|&p| p < u64::from(self.ramp) && readable);
            if xfade.is_none() {
                slot.xfade = None;
            }
            // A detached slot reads no flow, an outgoing one aside.
            let input = generation
                .filter(|_| readable && !slot.detached)
                .and_then(|g| Some((g.mxl.input(i)?, g)));
            let copied = match input {
                Some((input, g)) => {
                    let group = &self.statuses[slot.domain];
                    let outcome = input.read(&mut slot.state, k, group, dst);
                    let s = &mut slot.stats;
                    match outcome {
                        Outcome::Copy => s.on_time += 1,
                        Outcome::Late => {
                            add(&self.counters.input_late_blocks, 1);
                            s.late += 1;
                            let end = ((k + 1) * self.block).saturating_add_signed(slot.offset);
                            let head = slot.state.head;
                            if head > 0 {
                                let slack = head as i64 - end as i64;
                                s.min_slack = Some(s.min_slack.map_or(slack, |m| m.min(slack)));
                            }
                        }
                        _ => {}
                    }
                    // A head that advances behind its blocks for good, as one
                    // stamped on another clock does, is as unreadable as a
                    // frozen one, and keeps every block degraded; it is
                    // given longer, as a moving head may catch up.
                    let frozen =
                        u64::from(slot.state.frozen_late_blocks) * self.block >= g.prune_after[i];
                    let late = u64::from(slot.state.late_blocks) * self.block >= g.prune_late[i];
                    if !slot.prune_requested && (slot.state.invalid || frozen || late) {
                        let t = Telemetry::Prune {
                            slot: i,
                            invalid: slot.state.invalid,
                            frozen,
                            head: slot.state.head,
                            end: ((k + 1) * self.block).saturating_add_signed(slot.offset),
                        };
                        if self.tel.push(t).is_ok() {
                            slot.prune_requested = true;
                        } else {
                            add(&self.counters.telemetry_dropped, 1);
                        }
                    }
                    outcome == Outcome::Copy
                }
                #[cfg(test)]
                None if self.test_input.is_some() && readable && !slot.detached => {
                    let at = start.saturating_add_signed(slot.offset);
                    synthesize(self.test_input, i, slot.flow, at, b, dst)
                }
                None => {
                    dst.fill(0.0);
                    false
                }
            };
            if slot.up && !slot.detached && !copied {
                failed |= 1 << i;
            }
            if let Some(pos) = xfade {
                // new * up + old * down, the old flow's reader absent or
                // failing counting as silence.
                fading |= 1 << i;
                let mut up = Ramp::new(0.0);
                let _ = up.set_target(1.0, self.ramp);
                up.advance(pos as usize);
                let mut down = Ramp::new(1.0);
                let _ = down.set_target(0.0, self.ramp);
                down.advance(pos as usize);
                let old = &mut self.scratch_out[..slot.channels * b];
                let old_copied = generation
                    .and_then(|g| g.mxl.outgoing(i))
                    .is_some_and(|input| {
                        let group = &self.statuses[slot.domain];
                        input.read(&mut slot.prev, k, group, old) == Outcome::Copy
                    });
                #[cfg(test)]
                let old_copied = old_copied
                    || (self.test_input.is_some()
                        && slot.flow > 0
                        && generation.is_none_or(|g| g.mxl.outgoing(i).is_none())
                        && synthesize(
                            self.test_input,
                            i,
                            slot.flow - 1,
                            start.saturating_add_signed(slot.offset),
                            b,
                            old,
                        ));
                if !old_copied {
                    failed_out |= 1 << i;
                }
                for (ch, o) in dst.chunks_exact_mut(b).zip(old.chunks_exact_mut(b)) {
                    let (mut up, mut down) = (up, down);
                    up.process(ch);
                    if old_copied {
                        down.process(o);
                        for (d, s) in ch.iter_mut().zip(o.iter()) {
                            *d += *s;
                        }
                    }
                }
                slot.silenced_last = !copied;
                if !copied && !old_copied {
                    silenced |= 1 << i;
                    add(&self.counters.input_silenced_blocks, 1);
                }
            } else if copied {
                // The fade-in after a silenced block starts at that
                // block's first index; its gain at index n is the ramp's
                // value at n - start.
                if slot.silenced_last {
                    slot.fade_start = Some(start);
                    slot.silenced_last = false;
                }
                // A block below the start follows a backward clock step.
                if let Some(pos) = slot.fade_start.map(|s| start.saturating_sub(s))
                    && pos < u64::from(self.ramp)
                {
                    fading |= 1 << i;
                    let mut fade = Ramp::new(0.0);
                    let _ = fade.set_target(1.0, self.ramp);
                    fade.advance(pos as usize);
                    for ch in dst.chunks_exact_mut(b) {
                        let mut ramp = fade;
                        ramp.process(ch);
                    }
                }
            } else {
                // A detached slot is silent by the log, not for want of
                // its input.
                if !slot.detached {
                    silenced |= 1 << i;
                    add(&self.counters.input_silenced_blocks, 1);
                }
                slot.silenced_last = true;
            }
            slot.finish(start, dst, b, &mut self.inbuf, &mut self.prebuf);
        }

        // Which meters a slot down feeds, on the coefficients' targets the
        // block starts with: their slices of this block are gaps. Those
        // the log holds paused are paused.
        let matrix = &self.mix.matrix;
        let diverged = (self.replica.as_ref()).is_some_and(|r| r.shared.diverged.load(Relaxed));
        let paused = self.mix.paused;
        for (m, (meter, first)) in (self
            .outs
            .iter_mut()
            .map(|o| (&mut o.meter, o.first_channel)))
        .chain(
            self.buses
                .iter_mut()
                .map(|m| (&mut m.meter, m.first_channel)),
        )
        .enumerate()
        {
            let columns = first..first + meter.peak.len();
            let gap = down != 0 && feeds(matrix, &self.slots, down, columns);
            meter.block = flags(m, u128::from(gap) << m, paused, diverged);
        }
        let ramping = self.mix.ramping(k);
        let slots = &self.slots;
        let (counters, duck_meters) = (&self.counters, &mut self.duck_meters);
        let mut send = |r: DuckRecord| {
            let sent = duck_meters.push(r).is_ok();
            if !sent {
                add(&counters.duck_records_dropped, 1);
            }
            sent
        };
        let sink: Option<&mut dyn FnMut(DuckRecord) -> bool> =
            if self.replay { None } else { Some(&mut send) };
        self.mix.block(
            start,
            b,
            |s| slots[s].first_channel,
            &self.inbuf,
            &mut self.outbuf,
            sink,
        );
        // Each output's strip and limiter on its channels, before anything
        // reads them.
        for out in &mut self.outs {
            out.process(start, &mut self.outbuf, &mut self.scratch, b);
        }

        let digest = self
            .replica
            .as_mut()
            .and_then(|r| digest(r, &self.awaiting_gen, start, failed, failed_out));
        // The fingerprint of the page that ends in block k: its entries
        // were all applied by now.
        let ends = (start / PAGE + 1) * PAGE <= start + self.block;
        let fingerprint = if ends && self.mix.page_entries.capacity() > 0 {
            self.mix.fingerprint()
        } else {
            None
        };
        // The held blocks move first: one found withheld, or block k
        // withheld before it was computed, withdraws the replica before
        // block k takes its status. Block k takes it at the same F: a ring
        // that is full moves at block k's decision.
        let pass = self.pass_ctx();
        let mut written = self.advance_holds(Some(k), pass);
        #[cfg(test)]
        hold_pass_pause(&self.counters, Some(k), false);
        for o in 0..self.outs.len() {
            let out = &mut self.outs[o];
            if self.replay {
                let src = &mut self.scratch[..out.channels * b];
                for (c, ch) in src.chunks_exact_mut(b).enumerate() {
                    ch.copy_from_slice(&self.outbuf.channel(out.first_channel + c)[..b]);
                }
                if let (Some(p), Some(r)) = (out.pages.as_mut(), self.replica.as_mut()) {
                    let dropped = &self.counters.page_hashes_dropped;
                    p.hash(k, src, fingerprint, dropped);
                    p.settle_unheld(&mut r.hashes, dropped);
                }
                continue;
            }
            let Some(writer) = out.writer.as_deref_mut() else {
                continue;
            };
            if k < out.first_k {
                continue;
            }
            let src = &mut self.scratch[..out.channels * b];
            for (c, ch) in src.chunks_exact_mut(b).enumerate() {
                ch.copy_from_slice(&self.outbuf.channel(out.first_channel + c)[..b]);
            }
            let (Some(r), Some(ctx)) = (self.replica.as_mut(), pass) else {
                match writer.write_block(k, src) {
                    Write::Committed => written |= 1 << o,
                    Write::Held => add(&self.counters.output_held_blocks, 1),
                    Write::Failed(_) => add(&self.counters.output_failed_writes, 1),
                }
                continue;
            };
            let statuses = &r.shared.statuses[o];
            let mut changes = Changes {
                ring: &mut r.changes,
                dropped: &self.counters.status_changes_dropped,
                output: o as u32,
            };
            let mut sink = WriterSink {
                writer,
                counters: &self.counters,
                wrote: false,
            };
            let hold = &mut out.hold;
            let (w0, o0) = (hold.withheld, hold.overflows);
            let decided = hold.compute(k, src, digest, ctx, statuses, &mut changes, &mut sink);
            add(&self.counters.blocks_withheld, hold.withheld - w0);
            add(&self.counters.hold_overflows, hold.overflows - o0);
            if sink.wrote {
                written |= 1 << o;
            }
            // Every block computed is hashed, in every mode.
            if let Some(p) = out.pages.as_mut() {
                p.hash(k, src, fingerprint, &self.counters.page_hashes_dropped);
            }
            // Before the next output decides block k.
            if decided.withdraw {
                withdraw_all(&mut self.outs, r, &self.counters, decided.cause);
            }
        }
        self.counters.processed.store(k + 1, Release);

        // Every computed block is metered, written or not: the meters
        // follow the engine's output, not a flow's state; and every input
        // after its strip, read or silenced. After the writes, since
        // nothing written depends on them.
        let outs = self
            .outs
            .iter_mut()
            .map(|o| (&mut o.meter, o.first_channel, &self.outbuf));
        let buses = self
            .buses
            .iter_mut()
            .map(|m| (&mut m.meter, m.first_channel, &self.outbuf));
        let taps = self
            .taps
            .iter_mut()
            .map(|m| (&mut m.meter, m.first_channel, &self.inbuf));
        // A slot's pre-strip meter runs while the log has it on. The
        // meters are those of the slots with one, in slot order, so the
        // zip pairs each with its slot; a replay computes no meters and
        // has none ([`crate::engine::replay_rt`]).
        debug_assert!(
            self.pre_taps.is_empty()
                || self.pre_taps.len() == self.slots.iter().filter(|s| s.pre_strip).count(),
            "a pre-strip meter per slot with one"
        );
        let on = self
            .slots
            .iter()
            .filter(|s| s.pre_strip)
            .map(|s| s.pre_strip_on);
        let pre_taps = (self.pre_taps.iter_mut().zip(on))
            .map(|(m, on)| on.then_some((&mut m.meter, m.first_channel, &self.prebuf)));
        let all = (outs.chain(buses).chain(taps).map(Some)).chain(pre_taps);
        for (i, metered) in all.enumerate() {
            let Some((meter, first, buf)) = metered else {
                continue;
            };
            let mut sink = MeterSink {
                meter: i as u32,
                ring: &mut self.meters,
                seq: &mut self.meter_seq,
                counters: &self.counters,
            };
            meter.add(&mut sink, first, buf, k * self.block, b);
        }

        let done = self.now();
        if done > deadline.saturating_add(self.period_ns() / 4) {
            add(&self.counters.deadline_miss, 1);
        }
        self.counters
            .process_time_max_ns
            .fetch_max(done.saturating_sub(t0), Relaxed);
        add(&self.counters.heartbeat_blocks, 1);
        self.send(Telemetry::Block(BlockVerdict {
            k,
            silenced,
            fading,
            ramping,
            written,
            commands_applied: self.mix.applied,
        }));
        if k.is_multiple_of(samplez_mxl::SAMPLE_RATE as u64 / self.block) {
            self.publish_stats(done);
        }
        self.copy_core(k);
    }

    /// Copies the core after block `k` into the buffer the replicated
    /// state handed over, if one waits and the copy has room to go back.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn copy_core(&mut self, k: u64) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        if r.copies.slots() == 0 {
            return;
        }
        let Ok(mut core) = r.cores.pop() else {
            return;
        };
        let pending_gen = self.pending_gen.as_ref().map(|p| p.0);
        capture_core(
            &mut core,
            &self.mix,
            &self.slots,
            &self.outs,
            &self.awaiting_gen,
            pending_gen,
            r,
        );
        // The ring had room: nothing is dropped here.
        let _ = r.copies.push(CoreCopy {
            k0: k,
            // A core installed from a product state or a replay knows
            // every entry up to the state's last id, though it took none
            // of them from the ring.
            last_id: self.last_entry.max(self.skip_upto),
            core,
        });
    }

    /// Copies the core into `c`, a buffer of the same shape, without
    /// allocating.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn capture(&self, c: &mut Core) {
        if let Some(r) = &self.replica {
            let pending_gen = self.pending_gen.as_ref().map(|p| p.0);
            capture_core(
                c,
                &self.mix,
                &self.slots,
                &self.outs,
                &self.awaiting_gen,
                pending_gen,
                r,
            );
        }
    }

    /// Makes `c`, a core that knew the entries up to `last_id`, the
    /// state, its strips too with `strips`: the next block computed is
    /// the one after it. Entries pending here that the core did not know
    /// stay pending; one that finds no room is counted in
    /// `handover_entries_lost`. A pending entry the core knew whose index
    /// it passed and whose effect it does not hold, an `InputUp` or
    /// `Route` held back for want of a core, or a strip entry under a
    /// core without strips, applies once the core is in, as on time: its
    /// generation is swapped in, a crossfade anchored at its index, or
    /// the strip set from its index.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn install_core(&mut self, c: &mut Core, last_id: LogId, strips: bool) {
        let m = &mut self.mix;
        let next = c.pos.unwrap_or(0);
        m.due.clear();
        for e in &m.pending {
            if e.id <= last_id && !e.change.in_core(strips) && e.at_index < next {
                // Room: due holds as many as pending.
                m.due.push((*e, false));
            }
        }
        m.matrix.copy_from(&c.matrix);
        // Both kinds of core carry the detectors.
        m.ducks.core.copy_from(&c.ducks);
        m.pos = c.pos;
        m.ramp_end = c.ramp_end;
        m.paused = c.loudness_paused;
        // An entry dropped here as known is taken through the core: a
        // later core that did not know it must be refused as Missed.
        for e in &m.pending {
            if e.id <= last_id {
                m.last_taken = m.last_taken.max(e.id);
            }
        }
        // The batches of the entries dropped go off the RT thread with the
        // core; those of the entries applied below once applied.
        let mut n = 0;
        while let Some(&(id, _)) = self.mix.batches.get(n) {
            if id > last_id || self.mix.due.iter().any(|d| d.0.id == id) {
                n += 1;
                continue;
            }
            let (_, b) = self.mix.batches.swap_remove(n);
            if let Err(b) = c.keep(b) {
                self.retire(Some(b));
            }
        }
        let m = &mut self.mix;
        m.pending.retain(|e| e.id > last_id);
        for i in 0..c.pending.len() {
            let e = c.pending[i];
            if self.mix.holds(e.id) {
                continue;
            }
            // The core's batch is taken, not copied: nothing allocates.
            let b = c.take_batch(e.id);
            if let Err(b) = self.mix.push_with(e, b) {
                add(&self.counters.handover_entries_lost, 1);
                if let Some(b) = b
                    && let Err(b) = c.keep(b)
                {
                    self.retire(Some(b));
                }
            }
        }
        let m = &mut self.mix;
        m.page_entries.clear();
        m.page_entries.extend_from_slice(&c.page_entries);
        m.entries_page = c.entries_page;
        m.entries_lost = c.entries_lost;
        for o in 0..self.outs.len() {
            let enabled = c.outputs_disabled & bit(o) == 0;
            if self.outs[o].enabled != enabled {
                self.set_output(o, enabled);
            }
        }
        for slot in 0..self.slots.len() {
            let on = c.pre_strip_meters & bit(slot) != 0;
            self.set_pre_strip_meter(slot, on, next);
        }
        for (d, s) in self.slots.iter_mut().zip(&c.slots) {
            d.up = s.up;
            d.held = s.held;
            d.fade_start = s.fade_start;
            d.silenced_last = s.silenced_last;
            d.xfade = s.xfade;
            d.detached = s.detached;
            if strips {
                // Of the same legs, `Core::fits` checked: the copy
                // allocates nothing.
                d.strip.clone_from(&s.strip);
                d.strip_pos = s.strip_pos;
                // A slot keeps its generator, or its lack of one, for
                // good: a tone generator's slot has no input domain to
                // read from.
                if let (Some(t), Some(c)) = (&mut d.tone, &s.tone) {
                    *t = *c;
                }
                // The rings of the set it names hold the history: filled
                // before the install.
                if let (Some(l), Some(c)) = (&mut d.delay, &s.delay) {
                    l.install(c.taps, c.pos, c.floor, usize::from(c.set));
                }
            }
        }
        if strips {
            for (d, l) in self.outs.iter_mut().zip(&c.limiters) {
                d.limiter.clone_from(l);
            }
            for (o, part) in &c.outs {
                if let Some(d) = self.outs.get_mut(*o as usize) {
                    // Of the same legs, `Core::fits` checked: the copy
                    // allocates nothing.
                    d.strip.clone_from(&part.strip);
                    d.strip_pos = part.strip_pos;
                    // The rings of the set it names hold the history, as a
                    // slot's.
                    if let (Some(l), Some(c)) = (&mut d.delay, &part.delay) {
                        l.install(c.taps, c.pos, c.floor, usize::from(c.set));
                    }
                }
            }
        }
        // An entry applied here before its generation arrived keeps
        // waiting for it: the core knew the entry, a refusal as Missed says
        // so otherwise, but holds no generation, and the one this thread
        // waits for arrives here. Its slot stays held until then.
        for e in &c.awaiting_gen {
            let known = self.awaiting_gen.iter().any(|a| a.id == e.id);
            if !known && self.awaiting_gen.len() < PENDING_CHANGES {
                self.awaiting_gen.push(*e);
            }
        }
        for a in &self.awaiting_gen {
            let (up, routed) = a.change.generating();
            for slot in slots_of(up | routed) {
                self.slots[slot].held = true;
            }
        }
        if let Some(r) = self.replica.as_mut() {
            r.unresolved.clear();
            r.unresolved.extend_from_slice(&c.unresolved);
            r.late_routes.clear();
            r.late_routes.extend_from_slice(&c.late_routes);
            r.late_lost_until = c.late_lost_until;
        }
        self.skip_upto = last_id;
        self.install_at = next;
        if strips {
            self.strips_upto = last_id;
        }
        for n in 0..self.mix.due.len() {
            let (e, _) = self.mix.due[n];
            let b = self.mix.take_batch(e.id);
            self.apply_known(e, b.as_deref(), true, !strips, true);
            self.retire(b);
        }
        self.mix.due.clear();
        // A generation held for an entry no pending list holds any more,
        // as one the core folded in, is dropped once the entries due at
        // the install applied: nothing would apply it, and while it
        // stayed it refused every later replayed core and held the
        // supervisor's job. An input it would have moved is on the
        // flow the core's state names: the supervisor took the slots'
        // flows and handed its generations over before the core.
        if let Some((id, _, _)) = self.pending_gen.as_ref()
            && *id <= last_id
            && !self.mix.holds(*id)
            && let Some((id, _, g)) = self.pending_gen.take()
        {
            self.gen_unused(id, g);
        }
    }

    /// Installs a replayed core whose last block is the one before
    /// `first`, the first block this wake computes. One for a block
    /// still to come waits; one this thread passed, that names a
    /// generation other than the one held here, or that misses an entry
    /// this thread took off its pending list goes back refused.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn take_handover(&mut self, first: u64) {
        let Some(r) = self.replica.as_mut() else {
            return;
        };
        if r.handed.slots() == 0 {
            return;
        }
        let Ok(h) = r.handovers.peek() else {
            return;
        };
        if h.j + 1 > first {
            return;
        }
        let Ok(mut h) = r.handovers.pop() else {
            return;
        };
        h.answer = if h.j + 1 != first {
            Answer::Passed
        } else if h.replayed && h.core.pending_gen != self.pending_gen.as_ref().map(|p| p.0) {
            Answer::Generation
        } else if self.mix.last_taken > h.last_id {
            Answer::Missed(self.mix.last_taken)
        } else {
            Answer::Installed
        };
        let counter = match h.answer {
            Answer::Installed => {
                self.install_core(&mut h.core, h.last_id, h.replayed);
                // A product state's strip, output strip, limiter, delay and
                // output delay parameters, from the install on.
                let e = RtEntry {
                    id: h.last_id,
                    at_index: first * self.block,
                    change: Change::Batch {
                        strips: true,
                        up: 0,
                        routed: 0,
                        detached: 0,
                        resets: false,
                    },
                };
                for n in 0..h.strips.len() {
                    let (slot, param) = h.strips[n];
                    self.set_strip(e, slot, param);
                }
                for n in 0..h.out_strips.len() {
                    let (output, param) = h.out_strips[n];
                    self.set_output_strip(e, output, param);
                }
                for n in 0..h.limiters.len() {
                    let (output, params) = h.limiters[n];
                    self.set_limiter(e, output, params);
                }
                for n in 0..h.delays.len() {
                    let (slot, samples) = h.delays[n];
                    self.set_delay(e, slot, samples);
                }
                for n in 0..h.out_delays.len() {
                    let (output, samples) = h.out_delays[n];
                    self.set_output_delay(e, output, samples);
                }
                // A product state's tone ramps replace the generators',
                // checked off the RT thread.
                for n in 0..h.tones.len() {
                    let (slot, ramps) = h.tones[n];
                    let resumed = (self.slots[slot].tone.as_mut()).is_some_and(|t| t.resume(ramps));
                    if !resumed {
                        add(&self.counters.strip_refused, 1);
                    }
                }
                // The page holding the boundary mixes both states: no
                // record.
                for p in self.outs.iter_mut().filter_map(|o| o.pages.as_mut()) {
                    p.restart();
                }
                self.core = true;
                &self.counters.handovers_installed
            }
            Answer::Generation => &self.counters.handovers_generation,
            Answer::Missed(_) => &self.counters.handovers_missed,
            Answer::Passed | Answer::Waiting => &self.counters.handovers_passed,
        };
        add(counter, 1);
        if let Some(r) = self.replica.as_mut() {
            // Room was checked: nothing is dropped here.
            let _ = r.handed.push(h);
        }
    }

    /// Sends each slot's statistics since the last call and starts new
    /// ones.
    fn publish_stats(&mut self, now: u64) {
        for i in 0..self.slots.len() {
            let slot = &self.slots[i];
            let head = (self.gens.get(slot.domain))
                .and_then(|g| g.as_deref())
                .and_then(|g| g.mxl.input(i))
                .and_then(|input| input.reader().head().ok());
            let stats = SlotStats {
                head,
                now,
                counters: slot.state.counters,
                ..slot.stats
            };
            if self.send(Telemetry::Stats { slot: i, stats }) {
                self.slots[i].stats = EMPTY_STATS;
            }
        }
    }
}

/// The lint probe (CI): built with `--cfg samplez_lint_probe="samplez"`,
/// clippy must refuse this lock and this sleep, the lock on a static
/// whose type it does not name, and libmxl's group wait, which stats a
/// flow's data file once it times out.
#[cfg(samplez_lint_probe = "samplez")]
pub fn lint_probe(m: &std::sync::Mutex<()>, g: &Generation) {
    #[allow(deprecated, reason = "the probe is of the disallowed-methods entry")]
    let _ = g.wait(0, 0);
    let _guard = m.lock();
    let _held = crate::PROBE_LOCK.lock();
    std::thread::sleep(std::time::Duration::from_millis(1));
}

#[cfg(test)]
mod tests;
