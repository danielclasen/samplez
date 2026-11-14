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
use samplez_dsp::{Matrix, PlanarBuffer, Ramp};
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
    /// Gain changes whose `at_index` lay in samples already produced
    /// when they reached the RT thread; not applied, and each one marks
    /// the engine diverged.
    pub command_late: AtomicU64,
    /// The index of the next sample the matrix produces, published after
    /// each block.
    pub produced: AtomicU64,
    /// Gain changes the RT thread took off its pending list: applied,
    /// refused by the matrix or late.
    pub gains_consumed: AtomicU64,
    /// Telemetry records not sent because the ring was full.
    pub telemetry_dropped: AtomicU64,
    /// Block verdicts not sent because the verdict channel was full.
    pub verdicts_dropped: AtomicU64,
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
    /// Gain changes applied up to and including this block, in stream
    /// order; with `k` the point a joining replica's parameter state is
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
    /// A gain change reached the RT thread after the matrix produced its
    /// index; not applied.
    LateGain {
        change: GainChange,
        /// The index of the next sample the matrix produced.
        pos: u64,
    },
}

/// An input generation of one domain and, per slot, the number of
/// samples its head may stay frozen before the slot is pruned.
pub(crate) struct Gen {
    pub mxl: Generation,
    pub prune_after: Vec<u64>,
}

pub(crate) enum Command {
    /// Replaces the generation of an input domain; the old one is
    /// returned.
    Generation { domain: usize, generation: Box<Gen> },
    /// Hands an output's writer over; a replaced one is returned.
    Writer {
        output: usize,
        writer: Box<FlowWriter>,
    },
}

/// Ramps one matrix coefficient to `gain`, starting at sample index
/// `at_index`, a multiple of B. Gain changes have a ring of their own, so
/// a full pending list never holds back a generation or a writer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GainChange {
    pub input: usize,
    pub output: usize,
    pub gain: f32,
    pub at_index: u64,
}

/// Gain changes the RT thread holds before their index, and the capacity
/// of the gain ring.
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
    pub prune_requested: bool,
    pub stats: SlotStats,
}

pub(crate) struct Out {
    pub first_channel: usize,
    pub channels: usize,
    pub writer: Option<Box<FlowWriter>>,
    /// The first block written: the newest due block at handover.
    pub first_k: u64,
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
    /// Pending gain changes, by `at_index`, in arrival order within one
    /// index. Capacity [`PENDING_CHANGES`], never exceeded.
    pub pending: Vec<GainChange>,
    /// Gain changes the matrix took.
    pub applied: u64,
    /// Gain changes taken off the pending list: applied, refused by the
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
            applied: 0,
            consumed: 0,
        }
    }

    /// Queues `change` by its index. False, and not queued, when
    /// [`PENDING_CHANGES`] are pending.
    pub fn push(&mut self, change: GainChange) -> bool {
        if self.pending.len() >= PENDING_CHANGES {
            return false;
        }
        let at = self
            .pending
            .partition_point(|c| c.at_index <= change.at_index);
        self.pending.insert(at, change);
        true
    }

    /// Brings the matrix to the start of block `k`: every pending change
    /// due at or before `kB` is applied at its own index, and the ramps
    /// advance over samples not mixed, so a coefficient's value depends on
    /// the sample index and the ordered changes alone. A change whose
    /// index the matrix has already produced is passed to `late` with the
    /// matrix position, and not applied. A block below the position
    /// follows a backward clock step: the position re-anchors to `kB` and
    /// the ramps hold where they are, never rewound.
    pub fn to(&mut self, k: u64, mut late: impl FnMut(GainChange, u64)) {
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
            if c.at_index < pos {
                late(c, pos);
                continue;
            }
            self.matrix.advance((c.at_index - pos) as usize);
            pos = c.at_index;
            if self.matrix.set(c.input, c.output, c.gain, self.ramp) {
                self.applied += 1;
                if self.matrix.is_ramping(c.input, c.output) {
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
    pub cmd: Consumer<Command>,
    pub gains: Consumer<GainChange>,
    pub ret: Producer<Returned>,
    pub tel: Producer<Telemetry>,
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
            self.drain_commands(newest.unwrap_or(0));
            while self.mix.pending.len() < PENDING_CHANGES
                && let Ok(change) = self.gains.pop()
            {
                let _ = self.mix.push(change);
            }
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
                Command::Generation { domain, generation } => {
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

    /// Reads, mixes and writes block `k`, due at `deadline`.
    fn process(&mut self, k: u64, deadline: u64) {
        let t0 = time::now(&self.api);
        let b = self.block as usize;
        let mut silenced = 0u64;
        let mut fading = 0u64;
        for (i, slot) in self.slots.iter_mut().enumerate() {
            let dst = &mut self.scratch[..slot.channels * b];
            let generation = self.gens[slot.domain].as_deref();
            let copied = match generation.and_then(|g| Some((g.mxl.input(i)?, g))) {
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
            if copied {
                // The fade-in after a silenced block starts at that
                // block's first index; its gain at index n is the ramp's
                // value at n - start.
                let start = k * self.block;
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

        let (counters, tel) = (&self.counters, &mut self.tel);
        self.mix.to(k, |change, pos| {
            add(&counters.command_late, 1);
            if tel.push(Telemetry::LateGain { change, pos }).is_err() {
                add(&counters.telemetry_dropped, 1);
            }
        });
        self.counters.produced.store((k + 1) * self.block, Release);
        self.counters
            .gains_consumed
            .store(self.mix.consumed, Release);
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

    const B: u64 = 48;

    fn change(gain: f32, at_index: u64) -> GainChange {
        GainChange {
            input: 0,
            output: 0,
            gain,
            at_index,
        }
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
}
