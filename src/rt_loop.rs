//! The RT thread: the block loop on libmxl's TAI clock, and the messages
//! it exchanges with the supervisor.
//!
//! Nothing here allocates, frees or drops a libmxl object once the loop
//! runs. Objects arrive through the command ring and leave through the
//! return ring; the whole state goes back to the supervisor when the loop
//! ends, also when it panics.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed, Ordering::Release};

use mxl_sys::MXL_ERR_OUT_OF_RANGE_TOO_EARLY;
use rtrb::{Consumer, Producer};
use samplez_dsp::{KWeighting, Matrix, PlanarBuffer, Ramp, TruePeak};
use samplez_mxl::input::{Counters as InputCounters, Generation, GroupWait, Outcome, SlotState};
use samplez_mxl::output::{FlowWriter, Write};
use samplez_mxl::{MxlApi, time};

/// Engine counters, written by the RT thread and the supervisor.
#[derive(Debug, Default)]
pub struct Counters {
    /// Blocks processed (`rt_heartbeat_blocks_total`).
    pub heartbeat_blocks: AtomicU64,
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
}

/// Makes the RT loop panic once, for the panic containment test.
#[cfg(test)]
pub(crate) static PANIC: AtomicBool = AtomicBool::new(false);

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
    /// inputs.
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

/// The index of a log entry in the log that orders it.
pub type LogId = u64;

/// What a committed log entry changes. Each sets an absolute target, so
/// one applied again changes nothing.
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
}

impl Change {
    pub fn needs_generation(&self) -> bool {
        matches!(self, Change::InputUp { .. } | Change::Route { .. })
    }
}

/// A committed log entry, applied at sample index `at_index`, a multiple
/// of B. Entries have a ring of their own, so a full pending list never
/// holds back a generation or a writer.
#[derive(Clone, Copy, Debug)]
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
}

pub(crate) struct Out {
    pub first_channel: usize,
    pub channels: usize,
    pub writer: Option<Box<FlowWriter>>,
    /// The first block written: the newest due block at handover.
    pub first_k: u64,
    pub meter: OutMeter,
}

/// Samples of a meter slice: 10 ms at 48 kHz.
pub(crate) const SLICE: u64 = samplez_dsp::SLICE;
const WINDOW: u64 = samplez_dsp::WINDOW;
/// Input samples before the current one in the true-peak interpolation
/// window.
const TP_HISTORY: usize = 11;

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
}

/// The blocks to process after a wake, from `next`, the block after the
/// last one processed, and `newest`, the newest block due. An overdue
/// backlog of up to `max_catchup` blocks is processed in order, beyond
/// that only the newest block. `k` is the block after the newest due one,
/// so after a clock step either way it re-anchors to the clock.
pub(crate) fn plan(next: Option<u64>, newest: Option<u64>, max_catchup: u64, block: u64) -> Plan {
    let Some(n) = newest else {
        return Plan {
            catchup: 0..0,
            skipped: 0,
            k: 0,
        };
    };
    let (first, skipped) = match next {
        Some(nx) if n >= nx && n - nx < max_catchup => (nx, 0),
        Some(nx) if n >= nx => (n, (n - nx) * block),
        _ => (n + 1, 0),
    };
    Plan {
        catchup: first..n + 1,
        skipped,
        k: n + 1,
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
    /// Entries taken off the pending list: applied, refused by the
    /// matrix or late.
    pub consumed: u64,
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
            consumed: 0,
        }
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
            self.consumed += 1;
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
                self.applied += 1;
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

impl Rt {
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
            let m0 = monotonic_ns();
            let now = time::now(&self.api);
            let m1 = monotonic_ns();
            let (mono, width) = (m0 + (m1 - m0) / 2, m1 - m0);
            if let Some((t0, mono0, width0)) = prev {
                let step =
                    (i128::from(now) - i128::from(t0)) - (i128::from(mono) - i128::from(mono0));
                // Each midpoint lies within half its bracket of the read
                // of libmxl's clock.
                if step.unsigned_abs() > u128::from(period + width.max(width0)) {
                    add(&self.counters.clock_steps, 1);
                    self.send(Telemetry::ClockStep {
                        delta_ns: step as i64,
                    });
                }
            }
            prev = Some((now, mono, width));
            let newest = time::newest_due_block(&self.api, now, self.block, self.margin);
            // Entries before commands: a generation handed over after its
            // entry's commit finds the entry pending.
            while self.mix.pending.len() < PENDING_CHANGES
                && let Ok(entry) = self.entries.pop()
            {
                self.last_entry = self.last_entry.max(entry.id);
                let _ = self.mix.push(entry);
            }
            self.drain_commands(newest.unwrap_or(0));
            // After a backward clock step writers hold until the clock
            // passes their last commit, and inputs never read below their
            // last range.
            let Plan {
                catchup,
                skipped,
                k,
            } = plan(next, newest, self.max_catchup, self.block);
            add(&self.counters.overrun_skipped_samples, skipped);
            if !catchup.is_empty() {
                for s in &mut self.statuses {
                    *s = GroupWait::Skipped;
                }
            }
            for k in catchup {
                let deadline = time::deadline(&self.api, k, self.block, self.margin);
                self.process(k, deadline);
            }
            let deadline = time::deadline(&self.api, k, self.block, self.margin);
            self.wait(k, deadline);
            self.process(k, deadline);
            time::sleep_until(&self.api, deadline);
            let late = time::now(&self.api).saturating_sub(deadline);
            self.counters.wake_jitter_max_ns.fetch_max(late, Relaxed);
            next = Some(k + 1);
        }
    }

    /// Waits on each input domain's group for block `k`, until `deadline`.
    fn wait(&mut self, k: u64, deadline: u64) {
        let mut waited = false;
        let mut all_ok = true;
        for (g, status) in self.gens.iter().zip(self.statuses.iter_mut()) {
            *status = match g {
                Some(g) if g.mxl.has_inputs() => {
                    let ts = time::group_timestamp(&self.api, k, self.block, g.mxl.max_offset());
                    let timeout = deadline.saturating_sub(time::now(&self.api));
                    let s = g.mxl.wait(ts, timeout);
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
        if waited && all_ok && time::now(&self.api) < deadline {
            add(&self.counters.wait_early, 1);
        }
    }

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
                    if let Some(old) = out.writer.replace(writer) {
                        self.give_back(Returned::Writer(old));
                    }
                }
            }
        }
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
            slot.xfade = (on_time && !slot.silenced_last).then_some(e.at_index);
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
        self.mix.applied += 1;
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
    fn process(&mut self, k: u64, deadline: u64) {
        let t0 = time::now(&self.api);
        let b = self.block as usize;
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
        self.counters
            .entries_consumed
            .store(self.mix.consumed, Release);
        let start = k * self.block;
        let mut silenced = 0u64;
        let mut fading = 0u64;
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
            for (c, ch) in dst.chunks_exact(b).enumerate() {
                self.inbuf.channel_mut(slot.first_channel + c)[..b].copy_from_slice(ch);
            }
        }

        let ramping = self.mix.ramping(k);
        self.mix.matrix.process(&self.inbuf, &mut self.outbuf, b);

        let mut written = 0u64;
        for (o, out) in self.outs.iter_mut().enumerate() {
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
            match writer.write_block(k, src) {
                Write::Committed => written |= 1 << o,
                Write::Held => add(&self.counters.output_held_blocks, 1),
                Write::Failed(_) => add(&self.counters.output_failed_writes, 1),
            }
        }

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

        let done = time::now(&self.api);
        if done > deadline + self.period_ns() / 4 {
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
                k: 12
            }
        );
        // Forward clock step: only the newest block, the rest skipped.
        assert_eq!(
            p(Some(10), Some(100)),
            Plan {
                catchup: 100..101,
                skipped: 90 * B,
                k: 101
            }
        );
        // Backward clock step: k re-anchors to the clock.
        assert_eq!(p(Some(100), Some(49)), none(50));
    }

    #[test]
    fn a_forward_step_applies_changes_at_their_index() {
        let mut m = mix();
        assert!(to(&mut m, 10).is_empty());
        assert!(m.push(change(0.5, 50 * B)));
        assert!(to(&mut m, 100).is_empty());
        assert_eq!((m.applied, m.consumed), (1, 1));
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
        assert_eq!((m.applied, m.consumed), (1, 1));
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
        assert_eq!((m.applied, m.consumed), (0, 1));
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
        assert_eq!(m.consumed, 3);
    }
}
