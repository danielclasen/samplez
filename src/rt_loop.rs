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
use samplez_dsp::{KWeighting, Matrix, PlanarBuffer, Ramp, Started, Strip, StripParam, TruePeak};
use samplez_mxl::input::{Counters as InputCounters, Generation, GroupWait, Outcome, SlotState};
use samplez_mxl::output::{FlowWriter, Write};
use samplez_mxl::{MxlApi, time};

use crate::blocks::{
    Cause, Changes, Ctx, Hold, Kind, PAGE, PageHash, Pages, Sink, StatusChange, Statuses,
};
use crate::transfer::{Answer, Core, CoreCopy, Handover, SlotCore};

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
    /// Blocks processed before their deadline after a group wait returned
    /// OK.
    pub wait_early: AtomicU64,
    /// Group waits that timed out.
    pub input_wait_timeout: AtomicU64,
    /// Input blocks not read because the head was below the range end.
    pub input_late_blocks: AtomicU64,
    /// Input blocks silenced for any reason, a pruned input included.
    pub input_silenced_blocks: AtomicU64,
    /// Validity probes of the failed-input identification.
    pub input_probes: AtomicU64,
    /// Clock steps: libmxl's clock moved by more than a block period
    /// against CLOCK_MONOTONIC between two wakes.
    pub clock_steps: AtomicU64,
    /// The kernel TAI offset in seconds, CLOCK_TAI - CLOCK_REALTIME read
    /// back to back at each wake's step check; `i64::MIN` before the
    /// first.
    pub tai_offset_s: AtomicI64,
    /// Replicated mode: a detected clock step fences the blocks after it
    /// until the replicated state clears it.
    pub step_fenced: AtomicBool,
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
    /// Strip entries the slot's strip refused, or for a slot without a
    /// strip: not applied, and each one marks the engine diverged.
    pub strip_refused: AtomicU64,
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

/// The kernel TAI offset, CLOCK_TAI - CLOCK_REALTIME read back to back,
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
    /// Smallest `head - range end` of the late blocks.
    pub min_slack: i64,
    pub head: Option<u64>,
    /// libmxl's clock when the head was read.
    pub now: u64,
    pub counters: InputCounters,
}

pub(crate) enum Telemetry {
    Block(BlockVerdict),
    /// The slot is to be pruned: marked invalid, or its head has not moved
    /// for the slot's prune limit.
    Prune {
        slot: usize,
        invalid: bool,
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
    /// A strip entry the slot's strip refused, or for a slot without a
    /// strip; not applied.
    StripRefused {
        entry: RtEntry,
    },
}

/// An input generation of one domain and, per slot, the number of
/// samples its head may stay frozen before the slot is pruned.
pub(crate) struct Gen {
    pub mxl: Generation,
    pub prune_after: Vec<u64>,
    /// Built for a `Route` entry that changes the slot's flow: the slot's
    /// reader is on the new flow, its outgoing reader on the old one.
    pub rerouted: bool,
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
    /// Commit `output` from the first page boundary in a block at or
    /// after `min_k`.
    CommitFrom { output: usize, min_k: u64 },
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
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Change {
    /// Ramps one matrix coefficient to `gain`.
    Gain {
        input: usize,
        output: usize,
        gain: f32,
    },
    /// The input is silent from the index on, whatever is readable.
    InputDown { slot: usize },
    /// The input fades in from the index; needs a generation holding its
    /// reader.
    InputUp { slot: usize },
    /// The slot crossfades to another flow from the index; needs a
    /// generation holding both readers.
    Route { slot: usize },
    /// Sets one parameter of the slot's strip from the index.
    Strip { slot: usize, param: StripParam },
}

impl Change {
    pub fn needs_generation(&self) -> bool {
        matches!(self, Change::InputUp { .. } | Change::Route { .. })
    }

    /// Whether a core holds what this change did once applied: a gain
    /// change is in the matrix and an `InputDown` in the slot state, but
    /// a strip's settings and a swapped-in generation stay with the RT
    /// thread, so an entry making them must still reach it.
    pub fn in_core(&self) -> bool {
        matches!(self, Change::Gain { .. } | Change::InputDown { .. })
    }
}

/// A committed log entry, applied at sample index `at_index`, a multiple
/// of B. Entries have a ring of their own, so a full pending list never
/// holds back a generation or a writer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RtEntry {
    pub id: LogId,
    pub at_index: u64,
    pub change: Change,
}

/// Entries the RT thread holds before their index, and the capacity of
/// the entry ring.
pub(crate) const PENDING_CHANGES: usize = 64;

pub(crate) enum Returned {
    Generation(Box<Gen>),
    Writer(Box<FlowWriter>),
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
    pub prune_requested: bool,
    pub stats: SlotStats,
    /// The slot's strip, run on each block computed after the copy and
    /// the fades; none on a slot wider than a strip.
    pub strip: Option<Strip>,
    /// The index of the next sample the strip processes.
    pub strip_pos: Option<u64>,
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
            prune_requested: false,
            stats: EMPTY_STATS,
            strip: (channels <= MAX_LEGS).then(|| Strip::new(channels, ramp)),
            strip_pos: None,
        }
    }

    /// Sets `param` on the strip from sample index `at_index`. False, and
    /// nothing set, when the slot has no strip or the strip refuses it.
    fn set_strip(&mut self, at_index: u64, param: StripParam) -> bool {
        self.strip_to(at_index);
        self.strip.as_mut().is_some_and(|s| s.set(param))
    }

    /// Runs the strip over block `dst` of `frames` samples per channel
    /// starting at sample index `start`.
    fn run_strip(&mut self, start: u64, dst: &mut [f32], frames: usize) {
        self.strip_to(start);
        if let Some(strip) = &mut self.strip {
            strip.process(dst, frames);
        }
        self.strip_pos = Some(start + frames as u64);
    }

    /// Brings the strip to sample index `index`: its ramps advance over
    /// samples not processed, so a parameter's value depends on the
    /// sample index and the ordered entries alone. A lower index follows
    /// a backward clock step: the position re-anchors and the ramps hold
    /// where they are, never rewound.
    fn strip_to(&mut self, index: u64) {
        if let (Some(strip), Some(pos)) = (&mut self.strip, self.strip_pos)
            && index > pos
        {
            strip.advance((index - pos) as usize);
        }
        self.strip_pos = Some(index);
    }
}

pub(crate) struct Out {
    pub first_channel: usize,
    pub channels: usize,
    pub writer: Option<Box<FlowWriter>>,
    /// The first block written: the newest due block at handover.
    pub first_k: u64,
    pub meter: OutMeter,
    /// Statuses and the hold ring, in replicated mode.
    pub hold: Hold,
    /// The page hashes, in replicated mode with more than one voter.
    pub pages: Option<Pages>,
}

/// Samples of a meter slice: 10 ms at 48 kHz.
pub(crate) const SLICE: u64 = samplez_dsp::SLICE;
const WINDOW: u64 = samplez_dsp::WINDOW;
/// Input samples before the current one in the true-peak interpolation
/// window.
const TP_HISTORY: usize = TruePeak::HISTORY;

/// One output channel's meter values over one slice, the samples
/// `[slice * SLICE, (slice + 1) * SLICE)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MeterRecord {
    /// Counts every record the RT thread produced, sent or not, so a gap
    /// shows that one was lost.
    pub seq: u64,
    pub output: u32,
    pub channel: u32,
    pub slice: u64,
    /// Samples of the slice that were computed; fewer than [`SLICE`]
    /// after a skipped block or at the start.
    pub frames: u32,
    pub peak: f32,
    pub true_peak: f32,
    /// Sum of squares of the K-weighted samples.
    pub sum_sq: f64,
    /// Sum of squares of the samples, unweighted.
    pub plain_sum_sq: f64,
    /// The loudness measurement restarts with this slice: the
    /// K-weighting started from silence at its first sample.
    pub reset: bool,
}

/// Restarts the loudness measurement of `output` at the first 100 ms
/// window starting at or after `at_index`. A reset replaces one still
/// pending for the same output.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LoudnessReset {
    pub output: usize,
    pub at_index: u64,
}

/// Per output channel: sample peak, true peak and K-weighted power of
/// the slice in progress, sent at its end.
pub(crate) struct OutMeter {
    pub tp: TruePeak,
    pub kw: KWeighting,
    /// The slice in progress.
    pub slice: Option<u64>,
    /// The index the pending reset applies at, a window boundary.
    pub reset_at: Option<u64>,
    /// The slice in progress started a new measurement.
    pub reset: bool,
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
}

impl OutMeter {
    pub fn new(channels: usize) -> Self {
        Self {
            tp: TruePeak::new(channels, SLICE as usize),
            kw: KWeighting::new(channels),
            slice: None,
            reset_at: None,
            reset: false,
            next: None,
            tp_skip: 0,
            frames: 0,
            peak: vec![0.0; channels],
            true_peak: vec![0.0; channels],
            sum_sq: vec![0.0; channels],
            plain_sum_sq: vec![0.0; channels],
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
        if self.next.is_some_and(|x| x != start) {
            self.tp.reset();
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
                if self.reset_at.is_some_and(|t| slice * SLICE >= t) {
                    self.reset_at = None;
                    self.reset = true;
                    self.kw.reset();
                }
            }
            let len = ((slice + 1) * SLICE - index).min((frames - n) as u64) as usize;
            for c in 0..self.peak.len() {
                let x = &buf.channel(first + c)[n..n + len];
                let peak = &mut self.peak[c];
                let plain = &mut self.plain_sum_sq[c];
                for &v in x {
                    let a = v.abs();
                    if a > *peak || a.is_nan() {
                        *peak = a;
                    }
                    *plain += f64::from(v) * f64::from(v);
                }
                let skip = self.tp_skip.min(len);
                if skip > 0 {
                    self.tp.process(c, &x[..skip]);
                }
                if skip < len {
                    let tp = self.tp.process(c, &x[skip..]);
                    if tp > self.true_peak[c] || tp.is_nan() {
                        self.true_peak[c] = tp;
                    }
                }
                self.sum_sq[c] += self.kw.process(c, x);
            }
            self.tp_skip -= self.tp_skip.min(len);
            self.frames += len as u32;
            n += len;
            if (index + len as u64).is_multiple_of(SLICE) {
                self.flush(sink);
            }
        }
    }

    /// Sends the slice in progress, if any, and clears it.
    fn flush(&mut self, sink: &mut MeterSink<'_>) {
        let Some(slice) = self.slice.take() else {
            return;
        };
        for c in 0..self.peak.len() {
            let r = MeterRecord {
                seq: *sink.seq,
                output: sink.output,
                channel: c as u32,
                slice,
                frames: self.frames,
                peak: self.peak[c],
                true_peak: self.true_peak[c],
                sum_sq: self.sum_sq[c],
                plain_sum_sq: self.plain_sum_sq[c],
                reset: self.reset,
            };
            *sink.seq += 1;
            if sink.ring.push(r).is_err() {
                add(&sink.counters.meter_dropped, 1);
            }
        }
        self.frames = 0;
        self.reset = false;
        self.peak.fill(0.0);
        self.true_peak.fill(0.0);
        self.sum_sq.fill(0.0);
        self.plain_sum_sq.fill(0.0);
    }
}

/// Where an output's meter records go.
struct MeterSink<'a> {
    output: u32,
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

/// The matrix, its position on the sample timeline and the gain changes
/// waiting for their index.
pub(crate) struct Mix {
    pub matrix: Matrix,
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
}

impl Mix {
    pub fn new(matrix: Matrix, block: u64, ramp: u32) -> Mix {
        Mix {
            matrix,
            block,
            ramp,
            pos: None,
            ramp_end: 0,
            pending: Vec::with_capacity(PENDING_CHANGES),
            due: Vec::with_capacity(PENDING_CHANGES),
            applied: 0,
            last_taken: 0,
            page_entries: Vec::new(),
            entries_page: None,
            entries_lost: false,
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
    pub fn to(&mut self, k: u64, mut late: impl FnMut(RtEntry, u64)) {
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
        while let Some(&c) = self.pending.first()
            && c.at_index <= start
        {
            self.pending.remove(0);
            self.last_taken = self.last_taken.max(c.id);
            let Change::Gain {
                input,
                output,
                gain,
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
            if self.matrix.set(input, output, gain, self.ramp) {
                self.applied_entry(&c);
                if self.matrix.is_ramping(input, output) {
                    self.ramp_end = self.ramp_end.max(c.at_index + u64::from(self.ramp));
                }
            }
        }
        self.matrix.advance((start - pos) as usize);
        self.pos = Some(start + self.block);
    }

    /// Whether a coefficient ramp runs in block `k`.
    pub fn ramping(&self, k: u64) -> bool {
        k * self.block < self.ramp_end
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
    pub mix: Mix,
    pub inbuf: PlanarBuffer,
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
    /// The highest log id a handed-over core knew: an entry up to it
    /// taken from the ring later is pending or applied already.
    pub skip_upto: LogId,
    /// The first sample index the last handed-over core computed: an
    /// entry the core knew at or past it was on the core's pending list.
    pub install_at: u64,
    /// A replay clone: computes off the RT thread from past input, writes
    /// nothing and hashes every page it computes whole.
    pub replay: bool,
    /// The engine holds a core: false from a dropped core until a
    /// hand-over installs one.
    pub core: bool,
    pub cmd: Consumer<Command>,
    pub entries: Consumer<RtEntry>,
    pub ret: Producer<Returned>,
    pub tel: Producer<Telemetry>,
    pub meters: Producer<MeterRecord>,
    pub resets: Consumer<LoudnessReset>,
    /// The sequence number of the next meter record.
    pub meter_seq: u64,
    pub counters: Arc<Counters>,
    pub stop: Arc<AtomicBool>,
    /// Statuses, hold rings and fence, in replicated mode.
    pub replica: Option<Replica>,
    /// The index below which a detected clock step lowered F, until the
    /// step is cleared.
    pub step_at: Option<u64>,
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
    min_slack: 0,
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
    awaiting: &[RtEntry],
    pending_gen: Option<LogId>,
    r: &Replica,
) {
    c.matrix.copy_from(&m.matrix);
    c.pos = m.pos;
    c.ramp_end = m.ramp_end;
    c.pending.clear();
    c.pending.extend_from_slice(&m.pending);
    c.page_entries.clear();
    c.page_entries.extend_from_slice(&m.page_entries);
    c.entries_page = m.entries_page;
    c.entries_lost = m.entries_lost;
    for (d, s) in c.slots.iter_mut().zip(slots) {
        *d = SlotCore {
            up: s.up,
            held: s.held,
            fade_start: s.fade_start,
            silenced_last: s.silenced_last,
            xfade: s.xfade,
        };
    }
    c.awaiting_gen.clear();
    c.awaiting_gen.extend_from_slice(awaiting);
    c.pending_gen = pending_gen;
    c.unresolved.clear();
    c.unresolved.extend_from_slice(&r.unresolved);
    c.late_routes.clear();
    c.late_routes.extend_from_slice(&r.late_routes);
    c.late_lost_until = r.late_lost_until;
}

/// The replica is withdrawn on every output.
fn withdraw_all(outs: &mut [Out], r: &mut Replica, counters: &Counters) {
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
        out.hold.withdraw(&r.shared.statuses[o], &mut changes);
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

    /// The loop of plan 2.3. Per block k: wait on each input domain's group
    /// for the group timestamp until `T_k`, read, process and write,
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

    /// The loop body is real-time; the two calls that wait by design, the
    /// sync-group wait and `mxlSleepUntil`, are allowed by call site.
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
            #[allow(clippy::disallowed_methods, reason = "a test hook")]
            match STALL_MS.swap(0, Relaxed) {
                0 => {}
                ms => samplez_mxl::rtsan::allow(|| {
                    std::thread::sleep(std::time::Duration::from_millis(ms))
                }),
            }
            #[cfg(test)]
            CLOCK_SHIFT_NS.fetch_add(CLOCK_STEP_NS.swap(0, Relaxed), Relaxed);
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
                self.advance_holds(None);
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
                self.advance_holds(None);
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
        withdraw_all(&mut self.outs, r, &self.counters);
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

    /// Moves the blocks held in every output's ring and writes those
    /// committed; one bit per output written. Before block `next` is
    /// computed, a status the supervisor gave it already withdraws the
    /// replica as well.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn advance_holds(&mut self, next: Option<u64>) -> u64 {
        let Some(r) = self.replica.as_mut() else {
            return 0;
        };
        let mut written = 0;
        for o in 0..self.outs.len() {
            let out = &mut self.outs[o];
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
            let ctx = ctx(&r.shared, self.step_at);
            let statuses = &r.shared.statuses[o];
            let mut withdraw = out
                .hold
                .advance(ctx, statuses, &mut changes, &mut sink)
                .withdraw;
            if let Some(p) = out.pages.as_mut() {
                p.settle(statuses, &mut r.hashes, &self.counters.page_hashes_dropped);
            }
            withdraw |= next.is_some_and(|k| {
                out.hold.mode() != crate::blocks::Mode::Idle
                    && statuses.get(k).is_some_and(|s| s.kind == Kind::Withheld)
            });
            if sink.wrote {
                written |= 1 << o;
            }
            // On every output before the next one advances: none commits a
            // block after this one withheld one.
            if withdraw {
                withdraw_all(&mut self.outs, r, &self.counters);
            }
        }
        written
    }

    /// Waits on each input domain's group for block `k`, until `deadline`.
    fn wait(&mut self, k: u64, deadline: u64) {
        let mut waited = false;
        let mut all_ok = true;
        for (g, status) in self.gens.iter().zip(self.statuses.iter_mut()) {
            *status = match g {
                Some(g) if g.mxl.has_inputs() => {
                    let ts = time::group_timestamp(&self.api, k, self.block, g.mxl.max_offset());
                    let timeout = deadline.saturating_sub(clock_now(&self.api));
                    let s = samplez_mxl::rtsan::allow(|| g.mxl.wait(ts, timeout));
                    if matches!(s, GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY)) {
                        add(&self.counters.input_wait_timeout, 1);
                    }
                    waited = true;
                    s
                }
                _ => GroupWait::Skipped,
            };
            all_ok &= matches!(status, GroupWait::Ok);
        }
        if waited && all_ok && self.now() < deadline {
            add(&self.counters.wait_early, 1);
        }
    }

    /// Moves committed entries from the entry ring to the pending list
    /// while it has room.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn take_entries(&mut self) {
        while self.mix.pending.len() < PENDING_CHANGES
            && let Ok(entry) = self.entries.pop()
        {
            self.taken += 1;
            self.last_entry = self.last_entry.max(entry.id);
            // A hand-over brought the entry in already, pending or
            // applied by the replay.
            if self.mix.holds(entry.id) {
                continue;
            }
            if entry.id <= self.skip_upto {
                // The core knew it. It holds what a gain change or an
                // InputDown did; any other entry below the install it
                // applied without what it did reaching this thread, so
                // that entry is applied as on time. One at or past the
                // install was on the core's pending list, applied here at
                // its index.
                if !entry.change.in_core() && entry.at_index < self.install_at {
                    self.apply(entry);
                }
                continue;
            }
            let _ = self.mix.push(entry);
        }
    }

    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    fn drain_commands(&mut self, newest: u64) {
        loop {
            // Each command may return one object.
            if self.ret.slots() == 0 {
                return;
            }
            let Ok(cmd) = self.cmd.pop() else { return };
            match cmd {
                Command::Generation { domain, generation } => self.install(domain, generation),
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
                        if let Some((old, _, g)) =
                            self.pending_gen.replace((id, domain, generation))
                        {
                            self.gen_unused(old, g);
                        }
                    } else {
                        self.gen_unused(id, generation);
                    }
                }
                Command::Writer { output, writer } => {
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
                Control::CommitFrom { output, min_k } => self.outs[output].hold.commit_from(min_k),
                Control::StopAt { output, at } => self.outs[output].hold.stop_at(at),
                Control::Withdraw => withdraw_all(&mut self.outs, r, &self.counters),
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
                    withdraw_all(&mut self.outs, r, &self.counters);
                }
            }
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
        withdraw_all(&mut self.outs, r, &self.counters);
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
    /// fills the list nor holds the log's entries back. A strip entry is
    /// applied to its slot's strip, which stays with the slot; the others
    /// pass unapplied. A core installed later must know them, as it must
    /// know every entry taken.
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
            if let Change::Strip { slot, param } = e.change
                && !self.slots[slot].set_strip(e.at_index, param)
            {
                add(&self.counters.strip_refused, 1);
                self.send(Telemetry::StripRefused { entry: e });
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
    fn install(&mut self, domain: usize, generation: Box<Gen>) {
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

    /// Swaps in the generation of entry `e`, an `InputUp` or a `Route`.
    /// A re-route on time starts its crossfade at the entry's index; one
    /// whose generation came late starts the new flow from silence.
    fn swap_in(&mut self, e: RtEntry, domain: usize, generation: Box<Gen>, on_time: bool) {
        let rerouted = generation.rerouted;
        self.install(domain, generation);
        let (Change::InputUp { slot: i } | Change::Route { slot: i }) = e.change else {
            unreachable!("only these entries need a generation")
        };
        // Silent while another entry for the slot awaits its generation.
        let held = self.awaiting_gen.iter().any(
            |a| matches!(a.change, Change::InputUp { slot } | Change::Route { slot } if slot == i),
        );
        let slot = &mut self.slots[i];
        slot.held = held;
        if rerouted {
            slot.prev = slot.state.clone();
            let s = &mut slot.state;
            s.last_end = None;
            s.head = 0;
            s.frozen_late_blocks = 0;
            s.invalid = false;
            // Anchored at the index whatever the slot's last block was:
            // an outgoing flow that is silent fades the new one in.
            slot.xfade = on_time.then_some(e.at_index);
            // Late, it skips that crossfade: its blocks differ from an
            // on-time engine's until the crossfade's end.
            if !on_time
                && matches!(e.change, Change::Route { .. })
                && let Some(r) = self.replica.as_mut()
                && !note_late_route(r, e.id, e.at_index.saturating_add(u64::from(self.ramp)))
            {
                add(&self.counters.late_routes_lost, 1);
            }
        }
        self.send(Telemetry::GenerationDone {
            id: e.id,
            swapped: true,
        });
    }

    /// Applies entry `e`, other than a gain change, at the start of the
    /// block at its index. An entry that sets what the slot already has
    /// starts nothing.
    fn apply(&mut self, e: RtEntry) {
        self.mix.applied_entry(&e);
        let slot = match e.change {
            Change::Gain { .. } => return,
            Change::InputDown { slot } => {
                let s = &mut self.slots[slot];
                if s.up {
                    s.up = false;
                    s.xfade = None;
                    self.send(Telemetry::Input { slot, up: false });
                }
                return;
            }
            Change::Strip { slot, param } => {
                // Checked against the configuration that appended it; a
                // slot of another width here refuses it.
                if !self.slots[slot].set_strip(e.at_index, param) {
                    add(&self.counters.strip_refused, 1);
                    self.send(Telemetry::StripRefused { entry: e });
                }
                return;
            }
            Change::InputUp { slot } | Change::Route { slot } => slot,
        };
        if matches!(e.change, Change::InputUp { .. }) && !self.slots[slot].up {
            // The fade-in runs from the entry's index, also on an engine
            // that skipped the blocks around it.
            let s = &mut self.slots[slot];
            s.up = true;
            s.fade_start = Some(e.at_index);
            s.silenced_last = false;
            self.send(Telemetry::Input { slot, up: true });
        }
        match self.pending_gen.take() {
            Some((id, domain, g)) if id == e.id => self.swap_in(e, domain, g, true),
            other => {
                self.pending_gen = other;
                add(&self.counters.generation_late, 1);
                self.slots[slot].held = true;
                if self.awaiting_gen.len() < PENDING_CHANGES {
                    self.awaiting_gen.push(e);
                } else {
                    self.send(Telemetry::AwaitLost { entry: e });
                }
            }
        }
    }

    /// The entries [`Mix::to`] took off for block `k`: applied, or
    /// reported late.
    fn apply_due(&mut self, k: u64) {
        for n in 0..self.mix.due.len() {
            let (e, late) = self.mix.due[n];
            if !late {
                self.apply(e);
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
        }
        self.mix.due.clear();
    }

    /// Reads, mixes and writes block `k`, due at `deadline`.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn process(&mut self, k: u64, deadline: u64) {
        let t0 = self.now();
        let b = self.block as usize;
        if let Some(r) = &self.replica {
            r.shared.started.begin(k);
        }
        // Entries apply at the start of the block at their index, before
        // its inputs are read.
        let (counters, tel) = (&self.counters, &mut self.tel);
        self.mix.to(k, |entry, pos| {
            add(&counters.command_late, 1);
            if tel.push(Telemetry::LateEntry { entry, pos }).is_err() {
                add(&counters.telemetry_dropped, 1);
            }
        });
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
        for (i, slot) in self.slots.iter_mut().enumerate() {
            let dst = &mut self.scratch[..slot.channels * b];
            let generation = self.gens[slot.domain].as_deref();
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
            let input = generation
                .filter(|_| readable)
                .and_then(|g| Some((g.mxl.input(i)?, g)));
            let copied = match input {
                Some((input, g)) => {
                    let group = &self.statuses[slot.domain];
                    let probes = slot.state.counters.probes;
                    let outcome = input.read(&mut slot.state, k, group, dst);
                    let s = &mut slot.stats;
                    match outcome {
                        Outcome::Copy => s.on_time += 1,
                        Outcome::Late => {
                            add(&self.counters.input_late_blocks, 1);
                            s.late += 1;
                            let end = ((k + 1) * self.block).saturating_add_signed(slot.offset);
                            let slack = slot.state.head as i64 - end as i64;
                            s.min_slack = if s.late == 1 {
                                slack
                            } else {
                                s.min_slack.min(slack)
                            };
                            // The failure path (README): the probe runs
                            // libmxl's flow validity check, a stat() and
                            // path allocations.
                            input.identify(&mut slot.state, group, outcome);
                        }
                        _ => {}
                    }
                    add(
                        &self.counters.input_probes,
                        slot.state.counters.probes - probes,
                    );
                    let frozen = u64::from(slot.state.frozen_late_blocks) * self.block;
                    if !slot.prune_requested && (slot.state.invalid || frozen >= g.prune_after[i]) {
                        let t = Telemetry::Prune {
                            slot: i,
                            invalid: slot.state.invalid,
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
                None => {
                    dst.fill(0.0);
                    false
                }
            };
            if slot.up && !copied {
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
                silenced |= 1 << i;
                add(&self.counters.input_silenced_blocks, 1);
                slot.silenced_last = true;
            }
            // Silenced blocks too: the strip's filters and dynamics ring
            // out on silence.
            slot.run_strip(start, dst, b);
            for (c, ch) in dst.chunks_exact(b).enumerate() {
                self.inbuf.channel_mut(slot.first_channel + c)[..b].copy_from_slice(ch);
            }
        }

        let ramping = self.mix.ramping(k);
        self.mix.matrix.process(&self.inbuf, &mut self.outbuf, b);

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
        // block k takes its status.
        let mut written = self.advance_holds(Some(k));
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
            let Some(r) = self.replica.as_mut() else {
                match writer.write_block(k, src) {
                    Write::Committed => written |= 1 << o,
                    Write::Held => add(&self.counters.output_held_blocks, 1),
                    Write::Failed(_) => add(&self.counters.output_failed_writes, 1),
                }
                continue;
            };
            let ctx = ctx(&r.shared, self.step_at);
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
            let withdraw = hold
                .compute(k, src, digest, ctx, statuses, &mut changes, &mut sink)
                .withdraw;
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
            if withdraw {
                withdraw_all(&mut self.outs, r, &self.counters);
            }
        }
        self.counters.processed.store(k + 1, Release);

        while let Ok(r) = self.resets.pop() {
            self.outs[r.output].meter.reset_at = Some(r.at_index.div_ceil(WINDOW) * WINDOW);
        }
        // Every computed block is metered, written or not: the meters
        // follow the engine's output, not a flow's state. After the
        // writes, since nothing written depends on them.
        for (o, out) in self.outs.iter_mut().enumerate() {
            let mut sink = MeterSink {
                output: o as u32,
                ring: &mut self.meters,
                seq: &mut self.meter_seq,
                counters: &self.counters,
            };
            out.meter.add(
                &mut sink,
                out.first_channel,
                &self.outbuf,
                k * self.block,
                b,
            );
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
            &self.awaiting_gen,
            pending_gen,
            r,
        );
        // The ring had room: nothing is dropped here.
        let _ = r.copies.push(CoreCopy {
            k0: k,
            last_id: self.last_entry,
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
                &self.awaiting_gen,
                pending_gen,
                r,
            );
        }
    }

    /// Makes `c`, a core that knew the entries up to `last_id`, the
    /// state: the next block computed is the one after it. Entries
    /// pending here that the core did not know stay pending; one that
    /// finds no room is counted in `handover_entries_lost`. A pending
    /// entry the core knew whose index it passed and whose effect it
    /// does not hold, an `InputUp` or `Route` held back for want of a
    /// core or a strip entry, applies once the core is in, as on time:
    /// its generation is swapped in, a crossfade anchored at its index,
    /// or the strip set from its index.
    #[cfg_attr(samplez_rtsan, sanitize(realtime = "nonblocking"))]
    pub(crate) fn install_core(&mut self, c: &Core, last_id: LogId) {
        let m = &mut self.mix;
        let next = c.pos.unwrap_or(0);
        m.due.clear();
        for e in &m.pending {
            if e.id <= last_id && !e.change.in_core() && e.at_index < next {
                // Room: due holds as many as pending.
                m.due.push((*e, false));
            }
        }
        m.matrix.copy_from(&c.matrix);
        m.pos = c.pos;
        m.ramp_end = c.ramp_end;
        m.pending.retain(|e| e.id > last_id);
        for e in &c.pending {
            if !m.holds(e.id) && !m.push(*e) {
                add(&self.counters.handover_entries_lost, 1);
            }
        }
        m.page_entries.clear();
        m.page_entries.extend_from_slice(&c.page_entries);
        m.entries_page = c.entries_page;
        m.entries_lost = c.entries_lost;
        for (d, s) in self.slots.iter_mut().zip(&c.slots) {
            d.up = s.up;
            d.held = s.held;
            d.fade_start = s.fade_start;
            d.silenced_last = s.silenced_last;
            d.xfade = s.xfade;
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
            if let Change::InputUp { slot } | Change::Route { slot } = a.change {
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
        self.skip_upto = self.skip_upto.max(last_id);
        self.install_at = next;
        for n in 0..self.mix.due.len() {
            let (e, _) = self.mix.due[n];
            self.apply(e);
        }
        self.mix.due.clear();
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
        } else if h.names_gen && h.core.pending_gen != self.pending_gen.as_ref().map(|p| p.0) {
            Answer::Generation
        } else if self.mix.last_taken > h.last_id {
            Answer::Missed(self.mix.last_taken)
        } else {
            Answer::Installed
        };
        let counter = match h.answer {
            Answer::Installed => {
                self.install_core(&h.core, h.last_id);
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
            let head = self.gens[slot.domain]
                .as_deref()
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
/// clippy must refuse this lock and this sleep, and the lock on a static
/// whose type it does not name.
#[cfg(samplez_lint_probe = "samplez")]
pub fn lint_probe(m: &std::sync::Mutex<()>) {
    let _guard = m.lock();
    let _held = crate::PROBE_LOCK.lock();
    std::thread::sleep(std::time::Duration::from_millis(1));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The meter records of one output channel pair from block `first`
    /// on, with a loudness reset at index `reset`, of a signal that is a
    /// function of the sample index.
    fn meter_records(first: u64, reset: u64, blocks: u64) -> Vec<MeterRecord> {
        let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 14);
        let counters = Counters::default();
        let mut seq = 0;
        let mut m = OutMeter::new(2);
        let b = B as usize;
        let mut buf = PlanarBuffer::new(2, b);
        let mut records = Vec::new();
        for k in first..blocks {
            if k * B == reset - 4 * B {
                // The command arrives a few blocks ahead, mid-window.
                m.reset_at = Some(reset.div_ceil(WINDOW) * WINDOW);
            }
            for c in 0..2 {
                for (n, s) in buf.channel_mut(c).iter_mut().enumerate() {
                    let i = k * B + n as u64;
                    *s = ((i * 7919 + c as u64 * 31) % 1013) as f32 / 1013.0 - 0.5;
                }
            }
            let mut sink = MeterSink {
                output: 0,
                ring: &mut tx,
                seq: &mut seq,
                counters: &counters,
            };
            m.add(&mut sink, 0, &buf, k * B, b);
            while let Ok(r) = rx.pop() {
                records.push(r);
            }
        }
        records
    }

    /// From a reset on, the records depend only on the output since 11
    /// samples before its index: two engines that started at different
    /// blocks before that, with different K-weighting histories, send the
    /// same bits. Records cover
    /// whole slices, and the reset lands on the window boundary after its
    /// index.
    #[test]
    fn meters_after_a_reset_do_not_depend_on_the_start() {
        let reset = 2 * WINDOW;
        let key = |r: &MeterRecord| {
            (
                r.channel,
                r.slice,
                r.frames,
                r.peak.to_bits(),
                r.true_peak.to_bits(),
                r.sum_sq.to_bits(),
                r.reset,
            )
        };
        let after = |v: Vec<MeterRecord>| -> Vec<_> {
            v.iter()
                .filter(|r| r.slice * SLICE >= reset)
                .map(key)
                .collect()
        };
        let a = meter_records(0, reset, 600);
        let b = meter_records(reset / B - 20, reset, 600);
        assert!(a.windows(2).all(|w| w[1].seq == w[0].seq + 1));
        let (a, b) = (after(a), after(b));
        assert_eq!(a.len(), 2 * (600 * B - reset).div_ceil(SLICE) as usize);
        assert!(
            a[0].6 && a[1].6 && !a[2].6,
            "the reset flags the first slice"
        );
        assert!(a.iter().all(|r| r.2 == SLICE as u32));
        assert_eq!(a, b);
    }

    /// A reset leaves the true peak of a steady signal alone: the
    /// interpolator keeps the samples before the reset index, so the
    /// first interpolated outputs after it are not partial sums of the
    /// filter, which overshoot by up to 1 dB.
    #[test]
    fn a_reset_does_not_move_the_true_peak() {
        let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 12);
        let counters = Counters::default();
        let mut seq = 0;
        let mut m = OutMeter::new(1);
        let mut buf = PlanarBuffer::new(1, B as usize);
        let mut peaks = Vec::new();
        for k in 0..30 * SLICE / B {
            if k == 10 * SLICE / B {
                m.reset_at = Some(2 * WINDOW);
            }
            // A 1 kHz cosine at -1.9 dBFS, one period per block.
            for (n, s) in buf.channel_mut(0).iter_mut().enumerate() {
                *s = 0.8 * (std::f32::consts::TAU * n as f32 / B as f32).cos();
            }
            let mut sink = MeterSink {
                output: 0,
                ring: &mut tx,
                seq: &mut seq,
                counters: &counters,
            };
            m.add(&mut sink, 0, &buf, k * B, B as usize);
            while let Ok(r) = rx.pop() {
                peaks.push((r.reset, r.true_peak.to_bits()));
            }
        }
        let reset = peaks.iter().position(|p| p.0).unwrap();
        assert_eq!(reset as u64, 2 * WINDOW / SLICE);
        assert!(
            peaks[1..].iter().all(|p| p.1 == peaks[1].1),
            "{:?}",
            peaks
                .iter()
                .map(|p| f32::from_bits(p.1))
                .collect::<Vec<_>>()
        );
    }

    const B: u64 = 48;

    /// After an overrun skip or a clock step either way, the true peak of
    /// the first slice is that of the samples after the discontinuity
    /// alone: a quiet cosine after a loud one reads the same bits as its
    /// later slices.
    #[test]
    fn a_discontinuity_restarts_the_true_peak() {
        for resume in [27, 15] {
            let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 12);
            let counters = Counters::default();
            let mut seq = 0;
            let mut m = OutMeter::new(1);
            let mut buf = PlanarBuffer::new(1, B as usize);
            let mut peaks = Vec::new();
            for (i, k) in (0..20).chain(resume..resume + 40).enumerate() {
                let a = if i < 20 { 0.8 } else { 0.1 };
                for (n, s) in buf.channel_mut(0).iter_mut().enumerate() {
                    *s = a * (std::f32::consts::TAU * n as f32 / B as f32).cos();
                }
                let mut sink = MeterSink {
                    output: 0,
                    ring: &mut tx,
                    seq: &mut seq,
                    counters: &counters,
                };
                m.add(&mut sink, 0, &buf, k * B, B as usize);
                while let Ok(r) = rx.pop() {
                    peaks.push(r.true_peak);
                }
            }
            let after = &peaks[2..];
            assert!(
                after.iter().all(|p| p.to_bits() == after[0].to_bits()) && after[0] < 0.11,
                "resumed at block {resume}: {peaks:?}"
            );
        }
    }

    fn entry(change: Change, at_index: u64) -> RtEntry {
        static ID: AtomicU64 = AtomicU64::new(1);
        RtEntry {
            id: ID.fetch_add(1, Relaxed),
            at_index,
            change,
        }
    }

    fn change(gain: f32, at_index: u64) -> RtEntry {
        let change = Change::Gain {
            input: 0,
            output: 0,
            gain,
        };
        entry(change, at_index)
    }

    fn mix() -> Mix {
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0));
        Mix::new(m, B, 480)
    }

    /// Runs `to(k)` and returns the late changes it reported.
    fn to(m: &mut Mix, k: u64) -> Vec<(u64, u64)> {
        let mut late = Vec::new();
        m.to(k, |c, pos| late.push((c.at_index, pos)));
        late
    }

    #[test]
    fn plan_follows_the_clock() {
        let p = |next, newest| plan(next, newest, 2, B);
        let none = |k| Plan {
            catchup: k..k,
            skipped: 0,
            k,
            idle: false,
        };
        assert_eq!(p(None, None), none(0));
        assert_eq!(p(None, Some(9)), none(10));
        // On time: the newest due block is the last one processed.
        assert_eq!(p(Some(10), Some(9)), none(10));
        // Two blocks behind: both caught up in order.
        assert_eq!(
            p(Some(10), Some(11)),
            Plan {
                catchup: 10..12,
                skipped: 0,
                k: 12,
                idle: false,
            }
        );
        // Forward clock step: only the newest block, the rest skipped.
        assert_eq!(
            p(Some(10), Some(100)),
            Plan {
                catchup: 100..101,
                skipped: 90 * B,
                k: 101,
                idle: false,
            }
        );
        // Backward clock step: nothing is processed until the clock is due
        // at the block after the last one processed again.
        let behind = Plan {
            catchup: 100..100,
            skipped: 0,
            k: 100,
            idle: true,
        };
        assert_eq!(p(Some(100), Some(49)), behind);
        assert_eq!(p(Some(100), Some(97)), behind);
        assert_eq!(p(Some(100), None), behind);
        // A wake a block early waits for the next block as usual.
        assert_eq!(p(Some(100), Some(98)), none(100));
    }

    #[test]
    fn a_forward_step_applies_changes_at_their_index() {
        let mut m = mix();
        assert!(to(&mut m, 10).is_empty());
        assert!(m.push(change(0.5, 50 * B)));
        assert!(to(&mut m, 100).is_empty());
        assert_eq!((m.applied, m.pending.len()), (1, 0));
        // The ramp started at 50B and ended at 50B + 480, before 100B.
        assert_eq!(m.matrix.gain(0, 0), 0.5);
        assert_eq!(m.ramp_end, 50 * B + 480);
        assert!(!m.ramping(100));
        assert_eq!(m.pos, Some(101 * B));
    }

    #[test]
    fn a_backward_step_re_anchors_and_applies_changes_at_its_start() {
        let mut m = mix();
        to(&mut m, 99);
        assert!(m.push(change(0.5, 50 * B)));
        // The clock stepped back to block 50: the change at the new start
        // applies, it is not late.
        assert!(to(&mut m, 50).is_empty());
        assert_eq!((m.applied, m.pending.len()), (1, 0));
        assert_eq!(m.pos, Some(51 * B));
        assert!(m.ramping(50) && m.matrix.is_ramping(0, 0));
        // The next block follows the new position.
        assert!(m.push(change(0.25, 51 * B)));
        assert!(to(&mut m, 51).is_empty());
        assert_eq!(m.applied, 2);
    }

    #[test]
    fn a_backward_step_keeps_the_remaining_ramp() {
        let mut m = mix();
        to(&mut m, 9);
        assert!(m.push(change(0.5, 10 * B)));
        to(&mut m, 10);
        assert_eq!(m.ramp_end, 10 * B + 480);
        // Back by 5 blocks at block 12: 480 - 2B of the ramp remain.
        to(&mut m, 11);
        to(&mut m, 7);
        assert_eq!(m.ramp_end, 7 * B + 480 - 2 * B);
    }

    #[test]
    fn a_change_below_the_position_is_late_and_not_applied() {
        let mut m = mix();
        to(&mut m, 10);
        assert!(m.push(change(0.5, 5 * B)));
        assert_eq!(to(&mut m, 11), vec![(5 * B, 11 * B)]);
        assert_eq!((m.applied, m.pending.len()), (0, 0));
        assert_eq!(m.matrix.gain(0, 0), 1.0);
    }

    #[test]
    fn a_no_op_change_does_not_extend_the_ramp() {
        let mut m = mix();
        to(&mut m, 9);
        assert!(m.push(change(1.0, 10 * B)));
        to(&mut m, 10);
        assert_eq!(m.applied, 1);
        assert_eq!(m.ramp_end, 0);
        assert!(!m.ramping(10));
    }

    #[test]
    fn pending_is_bounded_and_ordered() {
        let mut m = mix();
        for i in 0..PENDING_CHANGES as u64 {
            assert!(m.push(change(0.5, (100 - i) * B)));
        }
        assert!(!m.push(change(0.5, 200 * B)));
        assert!(m.pending.windows(2).all(|w| w[0].at_index <= w[1].at_index));
    }

    /// A page's fingerprint covers the entries applied at an index in
    /// it, whatever order they arrived in: not one reported late, nor one
    /// of the next page.
    #[test]
    fn a_page_fingerprints_the_entries_applied_in_it() {
        let mut m = mix();
        m.page_entries = Vec::with_capacity(PENDING_CHANGES * PAGE.div_ceil(B) as usize);
        // Page 3 is blocks 64 to 85; block 86 starts in page 4.
        let (a, b, c) = (
            change(0.5, 66 * B),
            change(0.25, 70 * B),
            change(0.75, 86 * B),
        );
        assert!(m.push(c) && m.push(b) && m.push(a));
        for k in 64..=70 {
            assert!(to(&mut m, k).is_empty());
        }
        let late = change(1.0, 65 * B);
        assert!(m.push(late));
        assert_eq!(to(&mut m, 71), [(65 * B, 71 * B)]);
        for k in 72..=85 {
            to(&mut m, k);
        }
        let want = crate::blocks::fingerprint(&mut [(a.id, a.at_index), (b.id, b.at_index)]);
        assert_eq!(m.fingerprint(), Some(want));
        for k in 86..=106 {
            to(&mut m, k);
        }
        let want = crate::blocks::fingerprint(&mut [(c.id, c.at_index)]);
        assert_eq!(m.fingerprint(), Some(want));
    }

    /// A page with more entries than its list holds has no fingerprint.
    #[test]
    fn a_page_with_entries_lost_has_no_fingerprint() {
        let mut m = mix();
        m.page_entries = Vec::with_capacity(1);
        assert!(m.push(change(0.5, 66 * B)) && m.push(change(0.25, 67 * B)));
        for k in 64..=85 {
            to(&mut m, k);
        }
        assert_eq!(m.fingerprint(), None);
        to(&mut m, 86);
        assert_eq!(m.fingerprint(), Some(crate::blocks::HEALTHY));
    }

    /// Entries at one index apply in log order, whatever order they
    /// arrived in.
    #[test]
    fn entries_at_one_index_are_in_log_order() {
        let mut m = mix();
        let (a, b) = (change(0.5, 10 * B), change(0.25, 10 * B));
        assert!(m.push(b) && m.push(a));
        let ids: Vec<_> = m.pending.iter().map(|e| e.id).collect();
        assert_eq!(ids, [a.id, b.id]);
        assert!(to(&mut m, 10).is_empty());
        // Past the ramp: the later entry's target holds.
        to(&mut m, 30);
        assert_eq!(m.matrix.gain(0, 0), 0.25);
    }

    fn replica() -> Replica {
        let (changes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, cores) = rtrb::RingBuffer::new(1);
        let (copies, _) = rtrb::RingBuffer::new(1);
        let (_, handovers) = rtrb::RingBuffer::new(1);
        let (handed, _) = rtrb::RingBuffer::new(1);
        Replica {
            shared: Arc::new(Shared {
                fence: AtomicU64::new(0),
                ranks: AtomicU64::new(0),
                self_report: AtomicBool::new(false),
                steps_cleared: AtomicU64::new(0),
                statuses: Vec::new(),
                started: Started::new(),
            }),
            changes,
            control,
            unresolved: Vec::with_capacity(UNRESOLVED),
            ids: Vec::with_capacity(UNRESOLVED + PENDING_CHANGES),
            late_routes: Vec::with_capacity(PENDING_CHANGES),
            late_lost_until: 0,
            hashes,
            cores,
            copies,
            handovers,
            handed,
        }
    }

    fn slot(up: bool, held: bool, fade: Option<u64>, silenced: bool, xfade: Option<u64>) -> Slot {
        Slot {
            domain: 0,
            first_channel: 0,
            channels: 1,
            offset: 0,
            state: SlotState::default(),
            fade_start: fade,
            silenced_last: silenced,
            up,
            held,
            xfade,
            prev: SlotState::default(),
            prune_requested: false,
            stats: EMPTY_STATS,
            strip: None,
            strip_pos: None,
        }
    }

    /// Every field of the core is copied: each set to a value other than
    /// its default, the copy equals a core built from those values.
    #[test]
    fn the_core_copy_takes_every_field() {
        let mut m = mix();
        m.page_entries = Vec::with_capacity(4);
        assert!(m.matrix.set(0, 0, 0.5, 480));
        m.matrix.advance(100);
        m.pos = Some(20 * B);
        m.ramp_end = 19 * B + 380;
        assert!(m.push(change(0.25, 40 * B)));
        assert!(m.push(entry(Change::InputDown { slot: 1 }, 41 * B)));
        m.page_entries.extend([(3, 2 * B), (4, 3 * B)]);
        m.entries_page = Some(1);
        m.entries_lost = true;
        let slots = [
            slot(true, true, Some(5 * B), false, Some(18 * B)),
            slot(false, false, None, true, None),
        ];
        let awaiting = [entry(Change::InputUp { slot: 0 }, 19 * B)];
        let mut r = replica();
        r.unresolved.extend([(8, 21 * B), (9, 30 * B)]);
        r.late_routes.push((10, 25 * B));
        r.late_lost_until = 26 * B;

        let mut c = Core::new(1, 1, 2, 4);
        capture_core(&mut c, &m, &slots, &awaiting, Some(11), &r);
        let want = Core {
            matrix: m.matrix.clone(),
            pos: Some(20 * B),
            ramp_end: 19 * B + 380,
            pending: m.pending.clone(),
            page_entries: vec![(3, 2 * B), (4, 3 * B)],
            entries_page: Some(1),
            entries_lost: true,
            slots: vec![
                SlotCore {
                    up: true,
                    held: true,
                    fade_start: Some(5 * B),
                    silenced_last: false,
                    xfade: Some(18 * B),
                },
                SlotCore {
                    up: false,
                    held: false,
                    fade_start: None,
                    silenced_last: true,
                    xfade: None,
                },
            ],
            awaiting_gen: awaiting.to_vec(),
            pending_gen: Some(11),
            unresolved: vec![(8, 21 * B), (9, 30 * B)],
            late_routes: vec![(10, 25 * B)],
            late_lost_until: 26 * B,
        };
        assert_eq!(c, want);
        assert_eq!(c.pending.len(), 2);
    }

    /// The digest covers the inputs whose read failed, by crossfade leg,
    /// the entries applied without their generation, a late re-route until
    /// its crossfade's end and the unresolved entries at or below the
    /// block's start, sorted; a healthy block has the empty digest, a
    /// clock self-report none.
    #[test]
    fn the_digest_names_every_self_known_degradation() {
        use crate::blocks::{HEALTHY, degraded_digest};
        let mut r = replica();
        assert_eq!(digest(&mut r, &[], 10 * B, 0, 0), Some(HEALTHY));
        assert_eq!(
            digest(&mut r, &[], 10 * B, 0b101, 0),
            Some(degraded_digest(&[], 0b101, 0))
        );
        assert_eq!(
            digest(&mut r, &[], 10 * B, 0, 0b1),
            Some(degraded_digest(&[], 0, 0b1))
        );
        r.late_routes.push((5, 12 * B));
        assert_eq!(
            digest(&mut r, &[], 11 * B, 0, 0),
            Some(degraded_digest(&[5], 0, 0))
        );
        assert_eq!(digest(&mut r, &[], 12 * B, 0, 0), Some(HEALTHY));
        assert!(r.late_routes.is_empty());
        r.unresolved = vec![(3, 10 * B), (9, 11 * B)];
        let awaiting = [entry(Change::InputUp { slot: 0 }, 5 * B)];
        let want = {
            let mut ids = vec![3, awaiting[0].id];
            ids.sort_unstable();
            degraded_digest(&ids, 0, 0)
        };
        // Entry 9 is not due in the block at 10B.
        assert_eq!(digest(&mut r, &awaiting, 10 * B, 0, 0), Some(want));
        assert_ne!(digest(&mut r, &awaiting, 11 * B, 0, 0), Some(want));
        r.shared.self_report.store(true, Relaxed);
        assert_eq!(digest(&mut r, &awaiting, 10 * B, 0, 0), None);
        r.shared.self_report.store(false, Relaxed);
        r.unresolved.clear();
        // A late re-route the full list has no room for still degrades
        // the blocks of its window.
        for id in 0..PENDING_CHANGES as u64 {
            assert!(note_late_route(&mut r, id, 13 * B));
        }
        assert!(!note_late_route(&mut r, 99, 20 * B));
        assert_ne!(digest(&mut r, &[], 15 * B, 0, 0), Some(HEALTHY));
        assert_eq!(digest(&mut r, &[], 20 * B, 0, 0), Some(HEALTHY));
    }

    /// Input entries leave the pending list at the block of their index,
    /// in order, flagged late when the matrix produced their index already.
    #[test]
    fn input_entries_are_due_at_their_block() {
        let mut m = mix();
        let down = entry(Change::InputDown { slot: 1 }, 10 * B);
        let route = entry(Change::Route { slot: 0 }, 12 * B);
        assert!(m.push(route) && m.push(down));
        to(&mut m, 9);
        assert!(m.due.is_empty());
        to(&mut m, 10);
        assert_eq!(m.due.len(), 1);
        assert_eq!((m.due[0].0.id, m.due[0].1), (down.id, false));
        // A forward step past its index applies it on the next block.
        to(&mut m, 14);
        assert_eq!((m.due[0].0.id, m.due[0].1), (route.id, false));
        // One arriving after the matrix produced its index is late.
        let up = entry(Change::InputUp { slot: 1 }, 14 * B);
        assert!(m.push(up));
        to(&mut m, 15);
        assert_eq!((m.due[0].0.id, m.due[0].1), (up.id, true));
        assert!(m.pending.is_empty());
    }

    /// A mono slot whose strip ramps over 480 samples.
    fn strip_slot() -> Slot {
        Slot::new(0, 0, 1, 0, 480)
    }

    /// A strip runs in the loop as alone, block by block, with entries set
    /// at their index: a slot that skipped blocks, an entry's index among
    /// them, computes the block after the skip as a strip advanced to the
    /// index, set, and advanced on. Ramps, the dynamics order's crossfade
    /// and the compressor's makeup are running across the skip, and over
    /// silence they land where processing the skipped samples puts them.
    /// Below its position a strip re-anchors and rewinds nothing.
    #[test]
    fn a_strip_advances_over_samples_not_processed() {
        let comp = |makeup_db| {
            StripParam::Comp(samplez_dsp::CompParams {
                enabled: true,
                threshold_db: -30.0,
                ratio: 4.0,
                attack_ms: 1.0,
                release_ms: 40.0,
                knee_db: 6.0,
                makeup_db,
            })
        };
        let first = [
            StripParam::Eq { enabled: true },
            StripParam::EqBand {
                band: 1,
                value: samplez_dsp::EqBand {
                    kind: samplez_dsp::BandType::Peak,
                    freq: 1000.0,
                    gain_db: 18.0,
                    q: 2.0,
                },
            },
            comp(6.0),
        ];
        // Inside the skip of blocks 2..8.
        let second = [
            StripParam::Trim { db: -6.0 },
            StripParam::DynamicsBeforeEq { enabled: true },
            comp(18.0),
        ];
        let block = |k: usize| -> Vec<f32> {
            (k * 48..(k + 1) * 48)
                .map(|n| 0.8 * (n as f32 * 0.21).sin())
                .collect()
        };
        let mut slot = strip_slot();
        let mut reference = Strip::new(1, 480);
        let mut got = Vec::new();
        for p in first {
            assert!(slot.set_strip(0, p) && reference.set(p));
        }
        for k in [0, 1] {
            let (mut a, mut b) = (block(k), block(k));
            slot.run_strip(k as u64 * 48, &mut a, 48);
            reference.process(&mut b, 48);
            assert_eq!(a, b, "block {k}");
            got.push(a);
        }
        for p in second {
            assert!(slot.set_strip(192, p));
        }
        reference.advance(192 - 96);
        for p in second {
            assert!(reference.set(p));
        }
        reference.advance(384 - 192);
        let (mut a, mut b) = (block(8), block(8));
        slot.run_strip(384, &mut a, 48);
        reference.process(&mut b, 48);
        assert_eq!(a, b);
        assert_eq!(slot.strip.as_ref(), Some(&reference));
        // The changes ramp still: the reference is not a settled strip.
        let mut settled = reference.clone();
        settled.advance(1 << 20);
        let mut c = block(9);
        let mut d = block(9);
        reference.process(&mut c, 48);
        settled.process(&mut d, 48);
        assert_ne!(c, d);

        let held = slot.strip.clone();
        slot.strip_to(96);
        assert_eq!((slot.strip, slot.strip_pos), (held, Some(96)));

        // Over silence, filter and envelope states stay at zero, so a
        // strip that processed the skipped samples is where one advanced
        // over them is: ramps, the order's crossfade and the makeup gain,
        // all ending inside the skip of blocks 2..18.
        let mut slot = strip_slot();
        let mut processed = Strip::new(1, 480);
        for p in &first {
            assert!(slot.set_strip(0, *p) && processed.set(*p));
        }
        let mut silence = vec![0.0f32; 192];
        for k in [0, 1] {
            slot.run_strip(k * 48, &mut silence[..48], 48);
        }
        processed.process(&mut silence, 192);
        for p in second {
            assert!(slot.set_strip(192, p) && processed.set(p));
        }
        let mut silence = vec![0.0f32; 864 - 192];
        processed.process(&mut silence, 864 - 192);
        let (mut a, mut b) = (block(18), block(18));
        slot.run_strip(864, &mut a, 48);
        processed.process(&mut b, 48);
        assert_eq!(a, b);
        assert_eq!(slot.strip.as_ref(), Some(&processed));
    }

    /// A strip refuses what it does not take, and a slot wider than a
    /// strip has none.
    #[test]
    fn a_slot_refuses_what_its_strip_does_not_take() {
        let mut slot = strip_slot();
        assert!(!slot.set_strip(0, StripParam::LegTrim { leg: 1, db: 0.0 }));
        assert_eq!(slot.strip, Some(Strip::new(1, 480)));
        let mut wide = Slot::new(0, 0, MAX_LEGS + 1, 0, 480);
        assert!(wide.strip.is_none());
        assert!(!wide.set_strip(0, StripParam::Trim { db: 0.0 }));
    }
}
