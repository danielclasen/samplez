//! The strip's gate and compressor: feed-forward, no look-ahead, one
//! sidechain over all legs (the largest magnitude of the legs at each
//! sample), the same gain applied to every leg.
//!
//! Both follow Giannoulis, Massberg and Reiss, "Digital Dynamic Range
//! Compressor Design - A Tutorial and Analysis" (JAES 2012): a static
//! gain computer in dB, then a branching one-pole smoother of the gain
//! in dB, attack while the gain moves the way the unit acts (more
//! reduction for the compressor, opening for the gate), release the
//! other way. A time constant is the time to 1 - 1/e of a step. The
//! smoothed gain snaps to its target within 1e-4 dB, so a unit at rest
//! applies exactly its settled gain. Switching a unit on or off, and a
//! change of the compressor's mix, ramps its effect over the ramp
//! length. A compressor switched on at a mix of 0 passes its input
//! unchanged while its gain follows the key, as at any other mix.
//!
//! With an attack time constant of at least [`PER_SAMPLE_BELOW`]
//! intervals, the gain computer and the smoother run once every
//! [`GAIN_INTERVAL`] samples, on the largest key of the interval, at the
//! sample indices that are multiples of it; between two runs the gain
//! moves linearly to the next. The key of a sample acts from the next
//! interval on: a unit attacks up to [`GAIN_INTERVAL`] samples and
//! releases up to twice that later than a gain computed every sample,
//! and an attack overshoots by what that gain would have reduced in the
//! time, at most a third of the time constant and 1/48 of the reduction.
//! With a shorter attack they run every sample. A change of the attack
//! across that bound takes effect at the next multiple of
//! [`GAIN_INTERVAL`]. Which samples share an interval depends on the
//! sample index alone, not on how the samples are split into blocks.

use crate::Ramp;
use crate::level::{DB_PER_OCTAVE, db_to_gain, db_to_gain_f32, log2};
use crate::strip::{ATTACK_MS, RELEASE_MS};
use crate::svf::SAMPLE_RATE;

/// Gate hold after the key last reached the threshold: 30 ms, longer
/// than half a period of 20 Hz, the lowest frequency the high-pass
/// passes, so the gate does not close between the peaks of a sustained
/// low note.
pub const GATE_HOLD: u32 = 1440;
/// An open gate's hold restarts on a key this much below the threshold,
/// dB, so a key hovering at the threshold does not chatter.
pub const GATE_HYSTERESIS_DB: f32 = 4.0;
/// The smoothed gain snaps to its target within this, dB.
const SNAP_DB: f32 = 1.0e-4;

/// Samples between two runs of the gain computer, a power of two.
pub const GAIN_INTERVAL: u32 = 8;

/// The gain computer runs every sample for an attack time constant
/// shorter than this many [`GAIN_INTERVAL`]s, in samples, so a fast
/// attack acts on the first sample of a step.
pub const PER_SAMPLE_BELOW: u32 = 6;

/// Samples per run of the gain computer for an attack of `ms`: 1 or
/// [`GAIN_INTERVAL`].
fn run_len(attack_ms: f32) -> u32 {
    let tau = f64::from(attack_ms) * SAMPLE_RATE / 1000.0;
    if tau < f64::from(PER_SAMPLE_BELOW * GAIN_INTERVAL) {
        1
    } else {
        GAIN_INTERVAL
    }
}

/// The one-pole coefficient of time constant `ms` over `len` samples.
fn pole(ms: f32, len: u32) -> f32 {
    libm::exp(-1000.0 * f64::from(len) / (f64::from(ms) * SAMPLE_RATE)) as f32
}

/// The one-pole coefficients of a time constant over one sample and
/// over one [`GAIN_INTERVAL`].
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Pole {
    sample: f32,
    interval: f32,
}

impl Pole {
    fn new(ms: f32) -> Self {
        Pole {
            sample: pole(ms, 1),
            interval: pole(ms, GAIN_INTERVAL),
        }
    }

    /// Both coefficients at least 0 and at most those of [`Pole::new`]
    /// at `max_ms`, the longest time constant of the pole's range: a
    /// pole's coefficients grow with its time constant.
    fn is_valid(&self, max_ms: f32) -> bool {
        let max = Pole::new(max_ms);
        (0.0..=max.sample).contains(&self.sample) && (0.0..=max.interval).contains(&self.interval)
    }

    /// The coefficient over a run of `len` samples.
    #[inline]
    fn over(&self, len: u32) -> f32 {
        if len == 1 { self.sample } else { self.interval }
    }
}

/// One smoothing step of `y` towards `target` with pole `a`, snapping
/// within [`SNAP_DB`].
#[inline]
fn smooth(y: f32, target: f32, a: f32) -> f32 {
    let next = target + a * (y - target);
    if (next - target).abs() < SNAP_DB {
        target
    } else {
        next
    }
}

/// The largest magnitude of the legs at sample `n`, at most `f32::MAX`:
/// a NaN fails the comparison and an infinity is taken as `f32::MAX`, so
/// the key an interval keeps is finite whatever the input.
#[inline]
fn key(buf: &[f32], legs: usize, frames: usize, n: usize) -> f32 {
    let mut k = 0.0f32;
    for leg in 0..legs {
        let v = buf[leg * frames + n].abs();
        if v > k {
            k = v;
        }
    }
    k.min(f32::MAX)
}

/// The interval of the gain computer running: the largest key so far,
/// the samples of it processed, and the gain moving linearly from `from`
/// by `step` a sample to `to`, the gain computed at its start. `len` is
/// the samples per run of the gain computer in this interval, 1 or
/// [`GAIN_INTERVAL`], and `next` the one set for the intervals after it.
/// Run every sample, the gain is held at the one computed for the
/// sample.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Interval {
    peak: f32,
    phase: u32,
    from: f32,
    step: f32,
    to: f32,
    len: u32,
    next: u32,
}

impl Interval {
    /// At 0 dB and the start of an interval.
    const REST: Interval = Interval {
        peak: 0.0,
        phase: 0,
        from: 1.0,
        step: 0.0,
        to: 1.0,
        len: GAIN_INTERVAL,
        next: GAIN_INTERVAL,
    };

    /// Whether the interval holds what a unit's does between two samples:
    /// a position within it, runs of 1 or [`GAIN_INTERVAL`] samples, a
    /// finite key, gains within `[0, 1]`, which both units' gains are,
    /// and a step of at most the distance between two over an interval.
    fn is_valid(&self) -> bool {
        let run = |len: u32| len == 1 || len == GAIN_INTERVAL;
        let gain = |g: f32| (0.0..=1.0).contains(&g);
        self.phase < GAIN_INTERVAL
            && run(self.len)
            && run(self.next)
            && self.peak.is_finite()
            && self.peak >= 0.0
            && gain(self.from)
            && gain(self.to)
            && self.step.abs() <= 1.0 / GAIN_INTERVAL as f32
    }

    /// Sets the samples per run of the gain computer from the next
    /// multiple of [`GAIN_INTERVAL`] on, or at once at one with the gain
    /// held, where the change moves no gain.
    fn set_len(&mut self, len: u32) {
        self.next = len;
        if self.phase == 0 && self.step == 0.0 {
            self.len = len;
        }
    }

    /// Holds the gain at `to` from the next sample on, computed for it.
    #[inline]
    fn hold(&mut self, to: f32) {
        self.from = to;
        self.step = 0.0;
        self.to = to;
    }

    /// Whether every sample of this interval and the next runs the gain
    /// computer.
    #[inline]
    fn every_sample(&self) -> bool {
        self.len == 1 && self.next == 1
    }

    /// Takes the next sample with the gain `to` computed for it, run
    /// every sample, and returns it.
    #[inline]
    fn tick(&mut self, to: f32) -> f32 {
        self.hold(to);
        self.phase += 1;
        to
    }

    /// After `frames` samples run every sample, the last at `to`.
    fn ran(&mut self, frames: usize, to: f32) {
        self.hold(to);
        self.advance(frames);
    }

    /// Takes the key of the next sample in and returns its gain.
    #[inline]
    fn gain(&mut self, key: f32) -> f32 {
        if key > self.peak {
            self.peak = key;
        }
        self.phase += 1;
        self.from + self.phase as f32 * self.step
    }

    /// Whether the interval is complete and the gain computer runs on it
    /// to [`start`](Self::start) the next. A complete interval run every
    /// sample, or followed by one that is, moves on to the next with the
    /// gain held at the last one computed.
    #[inline]
    fn due(&mut self) -> bool {
        if self.phase < GAIN_INTERVAL {
            return false;
        }
        if self.len != 1 && self.next != 1 {
            return true;
        }
        self.wrap();
        self.phase = 0;
        false
    }

    /// Into the next interval without a run of the gain computer: the
    /// gain held at the last one computed, no key taken yet.
    fn wrap(&mut self) {
        self.hold(self.to);
        self.peak = 0.0;
        self.len = self.next;
    }

    /// Starts the next interval, the gain moving from the last computed
    /// one to `to`.
    #[inline]
    fn start(&mut self, to: f32) {
        const INV: f32 = 1.0 / GAIN_INTERVAL as f32;
        self.from = self.to;
        self.step = (to - self.to) * INV;
        self.to = to;
        self.peak = 0.0;
        self.phase = 0;
    }

    /// At 0 dB, `frames` samples on.
    fn rest(&mut self, frames: usize) {
        *self = Interval {
            phase: self.phase,
            len: self.len,
            next: self.next,
            ..Interval::REST
        };
        self.advance(frames);
    }

    /// `frames` samples on without their keys: across the end of the
    /// interval, the gain is held at the last one computed.
    fn advance(&mut self, frames: usize) {
        let phase = self.phase as usize + frames;
        if phase >= GAIN_INTERVAL as usize {
            self.wrap();
        }
        self.phase = (phase % GAIN_INTERVAL as usize) as u32;
    }

    /// To the position of sample index `index`; a position other than
    /// the current one holds the gain as [`advance`](Self::advance) does.
    fn align(&mut self, index: u64) {
        let phase = (index % u64::from(GAIN_INTERVAL)) as u32;
        if phase != self.phase {
            self.wrap();
            self.phase = phase;
        }
    }
}

/// A unit's switch and mix: its effect `1 + w * (gain - 1)`, which is
/// `(1 - w) + w * gain`, the input unchanged mixed with the input at
/// `gain`, with `w` ramping between 0 and its level within `[0, 1]`: 1
/// for the gate, the mix for the compressor.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Wet(Ramp);

impl Wet {
    fn new() -> Self {
        Wet(Ramp::new(0.0))
    }

    /// Ramps to `level` when on, to 0 when off; `level` is within
    /// `[0, 1]`.
    fn set(&mut self, on: bool, level: f32, len: u32) {
        let _ = self.0.set_target(if on { level } else { 0.0 }, len);
    }

    /// Off and not ramping.
    fn is_off(&self) -> bool {
        !self.0.is_active() && self.0.target() == 0.0
    }

    /// Ramping within `[0, 1]`.
    fn is_valid(&self) -> bool {
        self.0.is_within(0.0..=1.0)
    }

    /// Ramping within `[0, 1]` to 0 or 1.
    fn is_switch(&self) -> bool {
        self.is_valid() && (self.0.target == 0.0 || self.0.target == 1.0)
    }
}

/// Gate parameters, the faderz `Gate`.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GateParams {
    pub enabled: bool,
    /// dBFS; the gate opens when the key reaches it.
    pub threshold_db: f32,
    /// Attenuation when closed, dB; below full attenuation the gate is
    /// an expander to that floor.
    pub range_db: f32,
    /// Opening time constant, ms.
    pub attack_ms: f32,
    /// Closing time constant, ms, after the hold.
    pub release_ms: f32,
}

/// Compressor parameters, the faderz `Comp` plus a knee.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompParams {
    pub enabled: bool,
    /// dBFS.
    pub threshold_db: f32,
    /// Input dB per output dB above the threshold.
    pub ratio: f32,
    /// ms.
    pub attack_ms: f32,
    /// ms.
    pub release_ms: f32,
    /// Width of the soft knee centred on the threshold, dB; 0 is a hard
    /// knee.
    pub knee_db: f32,
    /// Gain after the compressor, dB, on the compressed signal alone.
    pub makeup_db: f32,
    /// The share of the compressed signal in the output, within
    /// [`COMP_MIX`](crate::strip::COMP_MIX), the rest the input
    /// unchanged and sample-aligned with it: parallel compression. At 0
    /// an enabled compressor passes its input unchanged and its gain
    /// keeps following the key, so a mix raised from 0 starts from the
    /// gain a compressor at another mix has. 1, the compressed signal
    /// alone, when absent from an encoding, and left out of one at 1.
    #[serde(default = "full_mix", skip_serializing_if = "is_full_mix")]
    pub mix: f32,
}

fn full_mix() -> f32 {
    1.0
}

fn is_full_mix(mix: &f32) -> bool {
    *mix == 1.0
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Gate {
    wet: Wet,
    threshold: f32,
    /// [`GATE_HYSTERESIS_DB`] below `threshold`.
    reopen: f32,
    range_db: f32,
    floor: f32,
    attack: Pole,
    release: Pole,
    /// Smoothed gain, dB, the last one computed.
    y: f32,
    hold: u32,
    interval: Interval,
}

impl Gate {
    pub(crate) fn new() -> Self {
        Gate {
            wet: Wet::new(),
            threshold: 0.0,
            reopen: 0.0,
            range_db: 0.0,
            floor: 1.0,
            attack: Pole::new(0.0),
            release: Pole::new(0.0),
            y: 0.0,
            hold: 0,
            interval: Interval::REST,
        }
    }

    pub(crate) fn set(&mut self, p: &GateParams, ramp: u32) {
        self.wet.set(p.enabled, 1.0, ramp);
        self.threshold = db_to_gain(p.threshold_db);
        self.reopen = db_to_gain(p.threshold_db - GATE_HYSTERESIS_DB);
        self.range_db = -p.range_db;
        self.floor = db_to_gain(-p.range_db);
        self.attack = Pole::new(p.attack_ms);
        self.release = Pole::new(p.release_ms);
        self.interval.set_len(run_len(p.attack_ms));
    }

    /// One run of the gain computer over `len` samples whose largest key
    /// is `peak`: moves the hold and the smoothed gain `y` on and returns
    /// the gain.
    #[inline]
    fn compute(&self, y: &mut f32, hold: &mut u32, peak: f32, len: u32) -> f32 {
        let open_at = if *hold > 0 {
            self.reopen
        } else {
            self.threshold
        };
        if peak >= open_at {
            *hold = GATE_HOLD;
        } else {
            *hold = hold.saturating_sub(len);
        }
        let target = if *hold > 0 { 0.0 } else { self.range_db };
        let a = if target > *y {
            self.attack
        } else {
            self.release
        };
        *y = smooth(*y, target, a.over(len));
        if *y == 0.0 {
            1.0
        } else if *y == self.range_db {
            self.floor
        } else {
            db_to_gain_f32(*y)
        }
    }

    pub(crate) fn process(&mut self, buf: &mut [f32], legs: usize, frames: usize) {
        if self.wet.is_off() {
            self.y = 0.0;
            self.hold = 0;
            self.interval.rest(frames);
            return;
        }
        let u = *self;
        let (mut y, mut hold, mut iv) = (self.y, self.hold, self.interval);
        if iv.every_sample() {
            let mut gain = iv.to;
            for n in 0..frames {
                gain = u.compute(&mut y, &mut hold, key(buf, legs, frames, n), 1);
                let w = u.wet.0.value_at(u.wet.0.pos + n as u32);
                let g = 1.0 + w * (gain - 1.0);
                if g != 1.0 {
                    for leg in 0..legs {
                        buf[leg * frames + n] *= g;
                    }
                }
            }
            iv.ran(frames, gain);
        } else {
            for n in 0..frames {
                let k = key(buf, legs, frames, n);
                let gain = if iv.len == 1 {
                    iv.tick(u.compute(&mut y, &mut hold, k, 1))
                } else {
                    iv.gain(k)
                };
                let w = u.wet.0.value_at(u.wet.0.pos + n as u32);
                let g = 1.0 + w * (gain - 1.0);
                if g != 1.0 {
                    for leg in 0..legs {
                        buf[leg * frames + n] *= g;
                    }
                }
                if iv.due() {
                    iv.start(u.compute(&mut y, &mut hold, iv.peak, GAIN_INTERVAL));
                }
            }
        }
        self.y = y;
        self.hold = hold;
        self.interval = iv;
        self.wet.0.advance(frames);
    }

    /// Moves the switch ramp on. The smoothed gain and the hold stay as
    /// they are: they follow the signal, which samples not processed do
    /// not have.
    pub(crate) fn advance(&mut self, frames: usize) {
        self.wet.0.advance(frames);
        self.interval.advance(frames);
    }

    /// Sets the position in the interval of the gain computer to that of
    /// sample index `index`.
    pub(crate) fn align(&mut self, index: u64) {
        self.interval.align(index);
    }

    /// Whether the gate holds values a gate set within the parameter
    /// ranges holds: threshold, reopen level and floor gains within
    /// `[0, 1]`, range and smoothed gain within the range limit, a hold
    /// within [`GATE_HOLD`], valid poles and interval, and a switch
    /// ramping to 0 or 1.
    pub(crate) fn is_valid(&self) -> bool {
        let unit = |g: f32| (0.0..=1.0).contains(&g);
        let range = -*crate::strip::GATE_RANGE_DB.end()..=0.0;
        self.wet.is_switch()
            && unit(self.threshold)
            && unit(self.reopen)
            && range.contains(&self.range_db)
            && unit(self.floor)
            && self.attack.is_valid(*ATTACK_MS.end())
            && self.release.is_valid(*RELEASE_MS.end())
            && range.contains(&self.y)
            && self.hold <= GATE_HOLD
            && self.interval.is_valid()
    }

    /// Whether the gate passes every input unchanged until it is set
    /// again: off, or a range of 0 dB with the gain at 0 dB.
    pub(crate) fn is_identity(&self) -> bool {
        self.wet.is_off() || (self.range_db == 0.0 && self.y == 0.0 && self.interval.from == 1.0)
    }
}

/// The lowest smoothed gain a compressor reaches, dB: its target is
/// `slope (key - threshold)`, the key at most `f32::MAX`, 770.6 dBFS, the
/// threshold at least -60 dBFS and the slope at least `1 / 20 - 1`, so
/// at most 789.1 dB of reduction, and the smoothed gain moves between
/// targets.
pub(crate) const COMP_GAIN_MIN_DB: f32 = -800.0;

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Comp {
    wet: Wet,
    threshold_db: f32,
    slope: f32,
    knee_db: f32,
    /// Below this key the gain computer gives 0 dB.
    knee_start: f32,
    attack: Pole,
    release: Pole,
    makeup: Ramp,
    /// Smoothed gain, dB, the last one computed.
    y: f32,
    interval: Interval,
    /// Switched on at a mix of 0: the switch ramps to 0 as when off, and
    /// the gain computer and smoother run all the same. Left out of an
    /// encoding while false, so a compressor at another mix or off
    /// encodes as one before the mix.
    #[serde(default, skip_serializing_if = "is_false")]
    tracking: bool,
}

impl Comp {
    pub(crate) fn new() -> Self {
        Comp {
            wet: Wet::new(),
            threshold_db: 0.0,
            slope: 0.0,
            knee_db: 0.0,
            knee_start: 1.0,
            attack: Pole::new(0.0),
            release: Pole::new(0.0),
            makeup: Ramp::new(1.0),
            y: 0.0,
            interval: Interval::REST,
            tracking: false,
        }
    }

    pub(crate) fn set(&mut self, p: &CompParams, ramp: u32) {
        self.wet.set(p.enabled, p.mix, ramp);
        self.tracking = p.enabled && p.mix == 0.0;
        self.threshold_db = p.threshold_db;
        self.slope = 1.0 / p.ratio - 1.0;
        self.knee_db = p.knee_db;
        self.knee_start = db_to_gain(p.threshold_db - p.knee_db / 2.0);
        self.attack = Pole::new(p.attack_ms);
        self.release = Pole::new(p.release_ms);
        self.interval.set_len(run_len(p.attack_ms));
        // Within +24 dB, so within MAX_GAIN.
        let _ = self.makeup.set_target(db_to_gain(p.makeup_db), ramp);
    }

    /// The gain computer: the gain in dB for a key of `level` dB.
    fn static_gain(&self, level: f32) -> f32 {
        let over = level - self.threshold_db;
        if 2.0 * over > self.knee_db {
            self.slope * over
        } else if 2.0 * over > -self.knee_db {
            let d = over + self.knee_db / 2.0;
            self.slope * d * d / (2.0 * self.knee_db)
        } else {
            0.0
        }
    }

    /// One run of the gain computer over `len` samples whose largest key
    /// is `peak`: moves the smoothed gain `y` on and returns the gain.
    #[inline]
    fn compute(&self, y: &mut f32, peak: f32, len: u32) -> f32 {
        // Below the knee the gain is 0 dB; the comparison also keeps a
        // zero key from the logarithm.
        let target = if peak > self.knee_start {
            self.static_gain(log2(peak) * DB_PER_OCTAVE)
        } else {
            0.0
        };
        let a = if target < *y {
            self.attack
        } else {
            self.release
        };
        *y = smooth(*y, target, a.over(len));
        if *y == 0.0 { 1.0 } else { db_to_gain_f32(*y) }
    }

    /// Off, the switch at rest at 0: the input passes and the gain rests
    /// at 0 dB. Switched on at a mix of 0 the switch rests at 0 too, but
    /// the gain follows the key.
    fn is_bypassed(&self) -> bool {
        self.wet.is_off() && !self.tracking
    }

    /// Processes `frames` samples of `legs` legs in place. With the
    /// switch at exactly 0 the applied gain `1 + 0 (gain - 1)` is exactly
    /// 1 for any finite gain, and no sample is multiplied.
    pub(crate) fn process(&mut self, buf: &mut [f32], legs: usize, frames: usize) {
        if self.is_bypassed() {
            self.y = 0.0;
            self.interval.rest(frames);
            self.makeup.advance(frames);
            return;
        }
        let c = *self;
        let (mut y, mut iv) = (self.y, self.interval);
        if iv.every_sample() {
            let mut reduction = iv.to;
            for n in 0..frames {
                reduction = c.compute(&mut y, key(buf, legs, frames, n), 1);
                let gain = reduction * c.makeup.value_at(c.makeup.pos + n as u32);
                let w = c.wet.0.value_at(c.wet.0.pos + n as u32);
                let g = 1.0 + w * (gain - 1.0);
                if g != 1.0 {
                    for leg in 0..legs {
                        buf[leg * frames + n] *= g;
                    }
                }
            }
            iv.ran(frames, reduction);
        } else {
            for n in 0..frames {
                let k = key(buf, legs, frames, n);
                let reduction = if iv.len == 1 {
                    iv.tick(c.compute(&mut y, k, 1))
                } else {
                    iv.gain(k)
                };
                let gain = reduction * c.makeup.value_at(c.makeup.pos + n as u32);
                let w = c.wet.0.value_at(c.wet.0.pos + n as u32);
                let g = 1.0 + w * (gain - 1.0);
                if g != 1.0 {
                    for leg in 0..legs {
                        buf[leg * frames + n] *= g;
                    }
                }
                if iv.due() {
                    iv.start(c.compute(&mut y, iv.peak, GAIN_INTERVAL));
                }
            }
        }
        self.y = y;
        self.interval = iv;
        self.makeup.advance(frames);
        self.wet.0.advance(frames);
    }

    /// Moves the switch and makeup ramps on. The smoothed gain stays as
    /// it is, as the gate's does.
    pub(crate) fn advance(&mut self, frames: usize) {
        self.wet.0.advance(frames);
        self.makeup.advance(frames);
        self.interval.advance(frames);
    }

    /// Sets the position in the interval of the gain computer to that of
    /// sample index `index`.
    pub(crate) fn align(&mut self, index: u64) {
        self.interval.align(index);
    }

    /// Whether the compressor holds values a compressor set within the
    /// parameter ranges holds: threshold and knee within theirs, a slope
    /// of a ratio within it, the knee's start gain within `(0, 1]`, a
    /// makeup ramp within its range, a smoothed gain within
    /// [`COMP_GAIN_MIN_DB`] and 0 dB, valid poles and interval, a
    /// switch ramping within the mix's range, and one ramping to 0 when
    /// tracking at a mix of 0.
    pub(crate) fn is_valid(&self) -> bool {
        use crate::strip::{COMP_KNEE_DB, COMP_RATIO, COMP_THRESHOLD_DB, MAKEUP_DB};
        let slope = 1.0 / *COMP_RATIO.end() - 1.0..=0.0;
        let makeup = 1.0..=db_to_gain(*MAKEUP_DB.end());
        self.wet.is_valid()
            && COMP_THRESHOLD_DB.contains(&self.threshold_db)
            && slope.contains(&self.slope)
            && COMP_KNEE_DB.contains(&self.knee_db)
            && self.knee_start > 0.0
            && self.knee_start <= 1.0
            && self.attack.is_valid(*ATTACK_MS.end())
            && self.release.is_valid(*RELEASE_MS.end())
            && self.makeup.is_within(makeup)
            && (COMP_GAIN_MIN_DB..=0.0).contains(&self.y)
            && self.interval.is_valid()
            && (!self.tracking || self.wet.0.target == 0.0)
    }

    /// Whether the compressor passes every input unchanged until it is
    /// set again: off or at a mix of 0 with the switch at rest, or a
    /// ratio of 1 with the gain at 0 dB and the makeup at rest at 0 dB.
    pub(crate) fn is_identity(&self) -> bool {
        self.wet.is_off()
            || (self.slope == 0.0
                && self.y == 0.0
                && self.interval.from == 1.0
                && !self.makeup.is_active()
                && self.makeup.target() == 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comp(threshold_db: f32, ratio: f32, knee_db: f32, makeup_db: f32) -> CompParams {
        CompParams {
            enabled: true,
            threshold_db,
            ratio,
            attack_ms: 1.0,
            release_ms: 50.0,
            knee_db,
            makeup_db,
            mix: 1.0,
        }
    }

    fn gate(threshold_db: f32, range_db: f32) -> GateParams {
        GateParams {
            enabled: true,
            threshold_db,
            range_db,
            attack_ms: 1.0,
            release_ms: 20.0,
        }
    }

    /// The output level in dB of a settled unit fed a constant level.
    fn settled_db(mut process: impl FnMut(&mut [f32]), level_db: f32) -> f32 {
        let x = db_to_gain(level_db);
        let mut last = 0.0;
        for _ in 0..1000 {
            let mut b = [x; 48];
            process(&mut b);
            last = b[47];
        }
        20.0 * libm::log10f(last)
    }

    /// The static curve: hard knee, soft knee and makeup, against the
    /// curve computed by hand from threshold and ratio.
    #[test]
    fn the_compressor_follows_its_static_curve() {
        let cases = [
            // (threshold, ratio, knee, makeup, input, output)
            (-20.0, 4.0, 0.0, 0.0, -30.0, -30.0),
            (-20.0, 4.0, 0.0, 0.0, -8.0, -17.0),
            (-20.0, 4.0, 0.0, 6.0, -8.0, -11.0),
            (-30.0, 20.0, 0.0, 0.0, 0.0, -28.5),
            (-20.0, 1.0, 0.0, 0.0, -2.0, -2.0),
            // Inside the knee: T - (1 - 1/R) (W/2)^2 / (2W) at the
            // threshold, unchanged at its lower edge, on the line at its
            // upper edge.
            (-20.0, 4.0, 10.0, 0.0, -20.0, -20.9375),
            (-20.0, 4.0, 10.0, 0.0, -25.0, -25.0),
            (-20.0, 4.0, 10.0, 0.0, -15.0, -18.75),
        ];
        for (t, r, w, m, input, want) in cases {
            let mut c = Comp::new();
            c.set(&comp(t, r, w, m), 0);
            let got = settled_db(|b| c.process(b, 1, 48), input);
            assert!(
                (got - want).abs() < 0.01,
                "T {t} R {r} W {w} makeup {m}: {input} dB in, {got} out, want {want}"
            );
        }
    }

    /// The gain in dB after a step, sample by sample.
    fn gain_curve(mut process: impl FnMut(&mut [f32]), from: f32, to: f32, n: usize) -> Vec<f32> {
        for _ in 0..2000 {
            let mut b = [from; 48];
            process(&mut b);
        }
        let mut out = Vec::new();
        while out.len() < n {
            let mut b = [to; 48];
            process(&mut b);
            out.extend(b.iter().map(|v| 20.0 * libm::log10f(v / to)));
        }
        out
    }

    /// The gain covers 1 - 1/e of a step in its time constant: attack
    /// into reduction, release out of it, GAIN_INTERVAL samples late for
    /// a step at the start of an interval.
    #[test]
    fn compressor_time_constants() {
        for (attack, release) in [(1.0f32, 50.0f32), (10.0, 500.0)] {
            let mut c = Comp::new();
            let p = CompParams {
                attack_ms: attack,
                release_ms: release,
                ..comp(-40.0, 10.0, 0.0, 0.0)
            };
            c.set(&p, 0);
            let (quiet, loud) = (db_to_gain(-60.0), db_to_gain(0.0));
            // Settled reduction at 0 dB: 40 * (1/10 - 1) = -36 dB.
            for (from, to, ms, start, end) in [
                (quiet, loud, attack, 0.0, -36.0),
                (loud, quiet, release, -36.0, 0.0),
            ] {
                let tau = (ms * 48.0) as usize;
                let g = gain_curve(|b| c.process(b, 1, 48), from, to, 2 * tau + 2);
                let want = start + (end - start) * (1.0 - (-1.0f32).exp());
                // At tau the curve of a per-sample gain has covered
                // 1 - 1/e; sample tau - 1 and tau + 1 bracket it. The
                // key of the step acts from the next interval on.
                let late = GAIN_INTERVAL as usize;
                let (a, b) = (g[tau - 1 + late], g[tau + 1 + late]);
                assert!(
                    (a.min(b) - 0.05..=a.max(b) + 0.05).contains(&want),
                    "{ms} ms: {a} .. {b} around {want}"
                );
            }
        }
    }

    /// The gain computer runs on the grid of the sample index: a key
    /// stepping up inside an interval leaves the gain at exactly 0 dB up
    /// to the end of the interval, and from there the gain moves linearly
    /// within each interval.
    #[test]
    fn the_gain_moves_linearly_on_the_grid_of_the_sample_index() {
        let mut c = Comp::new();
        c.set(&comp(-40.0, 10.0, 0.0, 0.0), 0);
        let step = 4800usize + 3;
        let next = step.next_multiple_of(GAIN_INTERVAL as usize);
        let x: Vec<f32> = (0..9600)
            .map(|n| if n < step { 0.001 } else { 0.5 })
            .collect();
        let mut y = x.clone();
        for b in y.chunks_mut(48) {
            c.process(b, 1, 48);
        }
        assert_eq!(x[..next], y[..next]);
        let g: Vec<f64> = x.iter().zip(&y).map(|(x, y)| f64::from(y / x)).collect();
        assert!(g[next] < 1.0);
        for i in (next..next + 960).step_by(GAIN_INTERVAL as usize) {
            let d = g[i + 1] - g[i];
            for n in i + 1..i + GAIN_INTERVAL as usize - 1 {
                let e = g[n + 1] - g[n] - d;
                assert!(e.abs() < 1e-6, "sample {n}: {e:e} off the line of {d:e}");
            }
        }
    }

    /// The gain, dB, the compressor `p` gives `x` with the gain computer
    /// and the smoother run every sample: the reference the computer run
    /// once an interval is held to.
    fn per_sample_gain(p: &CompParams, x: &[f32]) -> Vec<f32> {
        let mut c = Comp::new();
        c.set(p, 0);
        let (attack, release) = (pole(p.attack_ms, 1), pole(p.release_ms, 1));
        let mut y = 0.0f32;
        x.iter()
            .map(|&k| {
                let k = k.abs();
                let target = if k > c.knee_start {
                    c.static_gain(log2(k) * DB_PER_OCTAVE)
                } else {
                    0.0
                };
                y = smooth(y, target, if target < y { attack } else { release });
                y
            })
            .collect()
    }

    /// Steps into reduction and out of it, at every position in an
    /// interval, from the fastest attack and release up: the gain is
    /// that of a per-sample computer at most two intervals before, never
    /// more reduction than it reached by then and never less than it had
    /// two intervals earlier.
    #[test]
    fn the_gain_trails_a_per_sample_computer_by_two_intervals_at_most() {
        let late = 2 * GAIN_INTERVAL as usize;
        for (attack_ms, release_ms) in [(0.1f32, 5.0f32), (1.0, 50.0), (10.0, 500.0)] {
            let p = CompParams {
                attack_ms,
                release_ms,
                ..comp(-40.0, 10.0, 6.0, 0.0)
            };
            for at in 4800..4800 + GAIN_INTERVAL as usize {
                let mut x = vec![0.001f32; at];
                x.extend([1.0f32; 9600]);
                x.extend([0.001f32; 48_000]);
                let want = per_sample_gain(&p, &x);
                let mut c = Comp::new();
                c.set(&p, 0);
                let mut y = x.clone();
                for b in y.chunks_mut(48) {
                    let len = b.len();
                    c.process(b, 1, len);
                }
                for n in late..x.len() {
                    let got = 20.0 * libm::log10f(y[n] / x[n]);
                    let span = &want[n - late..=n];
                    let lo = span.iter().fold(f32::INFINITY, |a, &b| a.min(b));
                    let hi = span.iter().fold(f32::NEG_INFINITY, |a, &b| a.max(b));
                    assert!(
                        (lo - 0.01..=hi + 0.01).contains(&got),
                        "{p:?} step at {at}, sample {n}: {got} dB, per sample {lo} .. {hi}"
                    );
                }
            }
        }
    }

    /// The output of a unit at full wet and 0 dB makeup applying `gain`
    /// to `x`, as the units compute it.
    fn applied(x: f32, gain: f32) -> f32 {
        let g = 1.0 + 1.0 * (gain - 1.0);
        if g != 1.0 { x * g } else { x }
    }

    /// The output of the gate `p` with the gain computer, the hold and
    /// the smoother run every sample: the reference for a fast attack.
    fn per_sample_gate(p: &GateParams, x: &[f32]) -> Vec<f32> {
        let mut g = Gate::new();
        g.set(p, 0);
        let (attack, release) = (pole(p.attack_ms, 1), pole(p.release_ms, 1));
        let (mut y, mut hold) = (0.0f32, 0u32);
        x.iter()
            .map(|&v| {
                let open_at = if hold > 0 { g.reopen } else { g.threshold };
                if v.abs() >= open_at {
                    hold = GATE_HOLD;
                } else {
                    hold = hold.saturating_sub(1);
                }
                let target = if hold > 0 { 0.0 } else { g.range_db };
                y = smooth(y, target, if target > y { attack } else { release });
                let gain = if y == 0.0 {
                    1.0
                } else if y == g.range_db {
                    g.floor
                } else {
                    db_to_gain_f32(y)
                };
                applied(v, gain)
            })
            .collect()
    }

    /// A burst of `len` samples at -10 dBFS starting at `at`, -60 dBFS
    /// around it, for 1.5 s.
    fn burst(at: usize, len: usize) -> Vec<f32> {
        let (quiet, loud) = (db_to_gain(-60.0), db_to_gain(-10.0));
        (0..72_000)
            .map(|n| {
                if (at..at + len).contains(&n) {
                    loud
                } else {
                    quiet
                }
            })
            .collect()
    }

    /// Runs `x` through `process` in blocks of `b`.
    fn run(mut process: impl FnMut(&mut [f32]), x: &[f32], b: usize) -> Vec<f32> {
        let mut y = x.to_vec();
        for c in y.chunks_mut(b) {
            process(c);
        }
        y
    }

    /// An attack time constant under PER_SAMPLE_BELOW intervals runs the
    /// gain computer every sample: a compressor at 0.1 ms computes the
    /// bits of one run every sample, a step reduced from its first
    /// sample on, at every position of the step in an interval and any
    /// block size. The bound is a ratio of samples: 0.99 ms runs every
    /// sample, 1 ms once an interval.
    #[test]
    fn a_fast_compressor_runs_its_gain_computer_every_sample() {
        let p = CompParams {
            attack_ms: 0.1,
            release_ms: 5.0,
            ..comp(-40.0, 20.0, 0.0, 0.0)
        };
        for at in 4800..4800 + GAIN_INTERVAL as usize {
            let x = burst(at, 4800);
            let want: Vec<f32> = per_sample_gain(&p, &x)
                .iter()
                .zip(&x)
                .map(|(&y, &v)| applied(v, if y == 0.0 { 1.0 } else { db_to_gain_f32(y) }))
                .collect();
            assert!(want[at] < x[at]);
            for b in [48, 5] {
                let mut c = Comp::new();
                c.set(&p, 0);
                let got = run(|y| c.process(y, 1, y.len()), &x, b);
                assert_eq!(got, want, "step at {at}, blocks of {b}");
            }
        }
        assert_eq!(run_len(0.99), 1);
        assert_eq!(run_len(1.0), GAIN_INTERVAL);
    }

    /// A gate at 0.1 ms attack opens on the first sample at the
    /// threshold and counts its hold down sample by sample: the bits of
    /// one run every sample through opening, hold and closing.
    #[test]
    fn a_fast_gate_opens_and_holds_on_every_sample() {
        let p = GateParams {
            attack_ms: 0.1,
            release_ms: 5.0,
            ..gate(-40.0, 40.0)
        };
        for at in 4800..4800 + GAIN_INTERVAL as usize {
            let x = burst(at, 100);
            let want = per_sample_gate(&p, &x);
            assert!(want[at] > db_to_gain(-10.0 - 40.0 * 0.9));
            // The last loud sample starts the hold; it ends GATE_HOLD
            // samples on.
            let close = at + 99 + GATE_HOLD as usize;
            assert!(want[close - 1] == x[close - 1] && want[close] < x[close]);
            let mut g = Gate::new();
            g.set(&p, 0);
            assert_eq!(
                run(|y| g.process(y, 1, y.len()), &x, 48),
                want,
                "burst at {at}"
            );
        }
    }

    /// A gate run once an interval opens and closes at most two
    /// intervals after one run every sample: its gain, dB, is always
    /// within the span the per-sample gain had over the two intervals
    /// before. Its hold counts down by intervals from the end of the one
    /// with the last key at the threshold, so it starts closing 1 to
    /// GAIN_INTERVAL samples after the per-sample gate.
    #[test]
    fn a_gate_trails_a_per_sample_one_by_two_intervals_at_most() {
        let late = 2 * GAIN_INTERVAL as usize;
        for attack_ms in [1.0f32, 10.0] {
            let p = GateParams {
                attack_ms,
                release_ms: 20.0,
                ..gate(-40.0, 40.0)
            };
            for at in 4800..4800 + GAIN_INTERVAL as usize {
                // Long enough to open fully at 10 ms.
                let len = 9600;
                let x = burst(at, len);
                let db = |y: &[f32]| -> Vec<f32> {
                    y.iter()
                        .zip(&x)
                        .map(|(y, x)| 20.0 * libm::log10f(y / x))
                        .collect()
                };
                let want = db(&per_sample_gate(&p, &x));
                let mut g = Gate::new();
                g.set(&p, 0);
                let got = db(&run(|y| g.process(y, 1, y.len()), &x, 48));
                let close = |g: &[f32]| (at + len..).find(|&n| g[n] < 0.0).unwrap();
                assert_eq!(close(&want), at + len - 1 + GATE_HOLD as usize);
                let lag = close(&got) - close(&want);
                assert!(
                    (1..=GAIN_INTERVAL as usize).contains(&lag),
                    "{attack_ms} ms, burst at {at}: closes {lag} samples late"
                );
                for n in late..x.len() {
                    let span = &want[n - late..=n];
                    let lo = span.iter().fold(f32::INFINITY, |a, &b| a.min(b));
                    let hi = span.iter().fold(f32::NEG_INFINITY, |a, &b| a.max(b));
                    assert!(
                        (lo - 0.01..=hi + 0.01).contains(&got[n]),
                        "{attack_ms} ms, burst at {at}, sample {n}: {} dB, per sample {lo} .. {hi}",
                        got[n]
                    );
                }
            }
        }
    }

    /// An attack set across the bound mid-interval takes effect at the
    /// next multiple of GAIN_INTERVAL: up to it the unit computes the
    /// bits of one left at the old attack, at any block size.
    #[test]
    fn a_new_run_length_starts_on_the_grid() {
        let slow = CompParams {
            attack_ms: 10.0,
            ..comp(-40.0, 20.0, 0.0, 0.0)
        };
        let fast = CompParams {
            attack_ms: 0.1,
            ..slow
        };
        let x = burst(4800, 4800);
        // Mid-attack, off the grid.
        let at = 4800usize + 200 + 3;
        let next = at.next_multiple_of(GAIN_INTERVAL as usize);
        let mut reference = Comp::new();
        reference.set(&slow, 0);
        let kept = run(|y| reference.process(y, 1, y.len()), &x, 48);
        let mut outs = Vec::new();
        for b in [48, 5] {
            let mut c = Comp::new();
            c.set(&slow, 0);
            let mut y = x.clone();
            let (head, tail) = y.split_at_mut(at);
            for chunk in head.chunks_mut(b) {
                c.process(chunk, 1, chunk.len());
            }
            c.set(&fast, 0);
            for chunk in tail.chunks_mut(b) {
                c.process(chunk, 1, chunk.len());
            }
            assert_eq!(y[..next], kept[..next], "blocks of {b}");
            assert_ne!(y[next..next + 1], kept[next..next + 1], "blocks of {b}");
            outs.push(y);
        }
        assert_eq!(outs[0], outs[1]);
    }

    /// Samples advanced over or skipped by an alignment across the end
    /// of an interval leave the gain held at the last one computed, and
    /// the keys before them out of the next run.
    #[test]
    fn skipping_samples_holds_the_last_gain() {
        let p = CompParams {
            attack_ms: 10.0,
            ..comp(-40.0, 20.0, 0.0, 0.0)
        };
        let (loud, quiet) = (db_to_gain(-10.0), db_to_gain(-60.0));
        let skips: [fn(&mut Comp); 2] = [|c| c.advance(13), |c| c.align(13)];
        for skip in skips {
            let mut c = Comp::new();
            c.set(&p, 0);
            // Mid-attack, the gain moving within the interval.
            let mut x = [loud; 48 * 4 + 3];
            c.process(&mut x, 1, 48 * 4 + 3);
            assert!(c.interval.step != 0.0);
            let held = c.interval.to;
            skip(&mut c);
            let mut y = [quiet];
            c.process(&mut y, 1, 1);
            assert_eq!(y[0], applied(quiet, held));
            assert_eq!(c.interval.peak, quiet);
        }
    }

    /// Below the threshold the gate settles at its range, above it the
    /// signal passes untouched; it holds GATE_HOLD samples after the key
    /// leaves the threshold, then closes with its release.
    #[test]
    fn the_gate_opens_holds_and_closes() {
        let mut g = Gate::new();
        g.set(&gate(-40.0, 30.0), 0);
        assert!((settled_db(|b| g.process(b, 1, 48), -50.0) + 80.0).abs() < 0.01);
        let x = db_to_gain(-20.0);
        let mut b = [x; 48];
        for _ in 0..200 {
            b.fill(x);
            g.process(&mut b, 1, 48);
        }
        assert_eq!(b, [x; 48], "an open gate passes every bit");
        // Below the threshold now: held open, then closing.
        let quiet = db_to_gain(-50.0);
        let mut out = Vec::new();
        for _ in 0..(GATE_HOLD as usize + 2 * 960) / 48 {
            let mut b = [quiet; 48];
            g.process(&mut b, 1, 48);
            out.extend_from_slice(&b);
        }
        let hold = GATE_HOLD as usize;
        assert!(
            out[..hold - 1].iter().all(|&v| v == quiet),
            "closed during the hold"
        );
        assert!(out[hold + 1] < quiet, "still open after the hold");
        let tau = hold + 960;
        let db = 20.0 * libm::log10f(out[tau] / quiet);
        assert!((db + 30.0 * (1.0 - (-1.0f32).exp())).abs() < 0.2, "{db}");
        // A range of 0 dB never attenuates.
        let mut g = Gate::new();
        g.set(&gate(-40.0, 0.0), 0);
        assert!((settled_db(|b| g.process(b, 1, 48), -60.0) + 60.0).abs() < 1e-4);
    }

    /// One sidechain: a loud leg reduces the quiet one by the same gain.
    #[test]
    fn legs_share_the_sidechain() {
        let mut c = Comp::new();
        c.set(&comp(-20.0, 4.0, 0.0, 0.0), 0);
        let (loud, quiet) = (db_to_gain(-8.0), db_to_gain(-40.0));
        let mut b = [0.0f32; 96];
        for _ in 0..1000 {
            b[..48].fill(quiet);
            b[48..].fill(loud);
            c.process(&mut b, 2, 48);
        }
        let reduction = |out: f32, x: f32| 20.0 * libm::log10f(out / x);
        assert!((reduction(b[95], loud) + 9.0).abs() < 0.01);
        assert_eq!(reduction(b[95], loud), reduction(b[47], quiet));
    }

    /// A unit switched on ramps its effect in, one switched off ramps it
    /// out and then passes every bit; a unit at rest after its release
    /// applies exactly 0 dB.
    #[test]
    fn switching_ramps_and_rest_is_exact() {
        let mut c = Comp::new();
        let loud = db_to_gain(-6.0);
        c.set(&comp(-30.0, 8.0, 0.0, 6.0), 480);
        let mut b = [loud; 480];
        c.process(&mut b, 1, 480);
        let steps = b
            .windows(2)
            .map(|p| (p[1] - p[0]).abs())
            .fold(0.0f32, f32::max);
        assert!(steps < 0.01, "switching on steps by {steps}");
        c.set(
            &CompParams {
                enabled: false,
                ..comp(-30.0, 8.0, 0.0, 6.0)
            },
            480,
        );
        c.process(&mut b, 1, 480);
        let x = [0.3f32, -0.0, f32::from_bits(1)];
        let mut y = x;
        c.process(&mut y, 1, 3);
        assert_eq!(x.map(f32::to_bits), y.map(f32::to_bits));
        // On again, below the threshold: the gain is exactly makeup
        // times 1 once the release ended.
        let mut c = Comp::new();
        c.set(&comp(-20.0, 4.0, 0.0, 0.0), 0);
        gain_curve(|b| c.process(b, 1, 48), loud, db_to_gain(-40.0), 48);
        for _ in 0..2000 {
            let mut b = [db_to_gain(-40.0); 48];
            c.process(&mut b, 1, 48);
        }
        assert_eq!(c.y, 0.0);
        let x = [0.03f32, -0.0, f32::from_bits(1)];
        let mut y = x;
        c.process(&mut y, 1, 3);
        assert_eq!(x.map(f32::to_bits), y.map(f32::to_bits));
    }

    /// An open gate keeps its hold while the key stays within the
    /// hysteresis below the threshold; a closed one opens only at the
    /// threshold.
    #[test]
    fn the_gate_has_hysteresis() {
        let mut g = Gate::new();
        g.set(&gate(-40.0, 30.0), 0);
        let between = db_to_gain(-42.0);
        // Closed: a key between the two thresholds leaves it closed.
        assert!((settled_db(|b| g.process(b, 1, 48), -42.0) + 72.0).abs() < 0.01);
        // Opened at the threshold, the same key keeps it open.
        let mut b = [db_to_gain(-39.0); 48];
        for _ in 0..100 {
            g.process(&mut b, 1, 48);
            b.fill(db_to_gain(-39.0));
        }
        for _ in 0..1000 {
            let mut b = [between; 48];
            g.process(&mut b, 1, 48);
            assert_eq!(b, [between; 48]);
        }
        // Below the hysteresis it closes after the hold.
        assert!((settled_db(|b| g.process(b, 1, 48), -45.0) + 75.0).abs() < 0.01);
    }

    /// A key reaching the threshold once per half period of 20 Hz, the
    /// peaks of a sustained low note, quiet between them: the hold spans
    /// the time between two peaks, so the gate passes every sample.
    #[test]
    fn the_gate_holds_through_a_20_hz_period() {
        let mut g = Gate::new();
        g.set(&gate(-20.0, 40.0), 0);
        let (peak, quiet) = (db_to_gain(-20.0), db_to_gain(-60.0));
        let half_period = 1200;
        let mut n = 0usize;
        let mut block = || {
            let b: [f32; 48] = core::array::from_fn(|i| {
                if (n + i).is_multiple_of(half_period) {
                    peak
                } else {
                    quiet
                }
            });
            n += 48;
            b
        };
        for _ in 0..100 {
            g.process(&mut block(), 1, 48);
        }
        for _ in 0..500 {
            let x = block();
            let mut y = x;
            g.process(&mut y, 1, 48);
            assert_eq!(x, y);
        }
    }

    /// A compressor switched off still moves its makeup ramp on as it
    /// processes, so processing and advancing the same samples land on
    /// the same state.
    #[test]
    fn an_off_compressor_moves_its_makeup_on() {
        let mut a = Comp::new();
        a.set(
            &CompParams {
                enabled: false,
                ..comp(-20.0, 4.0, 0.0, 12.0)
            },
            480,
        );
        let mut b = a;
        let mut x = [0.25f32; 300];
        a.process(&mut x, 1, 300);
        b.advance(300);
        assert_eq!(a, b);
    }

    /// A gate or compressor holding a value none reaches is not valid: a
    /// NaN gain or key, a gain beyond its range, a pole of no decay, a
    /// hold or phase beyond its length.
    #[test]
    fn a_unit_holding_a_value_none_reaches_is_not_valid() {
        let mut g = Gate::new();
        g.set(&gate(-40.0, 30.0), 48);
        let mut c = Comp::new();
        c.set(&comp(-30.0, 4.0, 6.0, 3.0), 48);
        let mut burst: Vec<f32> = (0..480).map(|n| (n as f32 * 0.1).sin()).collect();
        g.process(&mut burst, 1, 480);
        c.process(&mut burst, 1, 480);
        assert!(g.is_valid() && c.is_valid());
        let gate_invalid = |what: &str, f: &dyn Fn(&mut Gate)| {
            let mut u = g;
            f(&mut u);
            assert!(!u.is_valid(), "gate {what}");
        };
        let comp_invalid = |what: &str, f: &dyn Fn(&mut Comp)| {
            let mut u = c;
            f(&mut u);
            assert!(!u.is_valid(), "compressor {what}");
        };
        gate_invalid("NaN gain", &|u| u.y = f32::NAN);
        gate_invalid("gain below its range", &|u| u.y = -81.0);
        gate_invalid("hold beyond its length", &|u| u.hold = GATE_HOLD + 1);
        gate_invalid("floor above unity", &|u| u.floor = 2.0);
        gate_invalid("switch to neither", &|u| u.wet.0.target = 0.5);
        let mut half = c;
        half.set(
            &CompParams {
                mix: 0.5,
                ..comp(-30.0, 4.0, 6.0, 3.0)
            },
            48,
        );
        half.process(&mut burst, 1, 24);
        assert!(half.is_valid(), "a compressor ramping to a mix of 0.5");
        comp_invalid("mix above 1", &|u| u.wet.0.target = 1.5);
        comp_invalid("tracking at a mix above 0", &|u| u.tracking = true);
        comp_invalid("mix below 0", &|u| u.wet.0.c0 = -0.25);
        comp_invalid("NaN gain", &|u| u.y = f32::NAN);
        comp_invalid("gain above 0 dB", &|u| u.y = 1.0);
        comp_invalid("NaN key", &|u| u.interval.peak = f32::NAN);
        comp_invalid("phase beyond the interval", &|u| {
            u.interval.phase = GAIN_INTERVAL
        });
        comp_invalid("run of neither length", &|u| u.interval.next = 3);
        comp_invalid("interval gain above unity", &|u| u.interval.to = 1.5);
        comp_invalid("pole of no decay", &|u| u.release.sample = 1.0);
        let slowest = |ms: f32| Pole::new(ms * 1.01);
        gate_invalid("attack slower than the longest", &|u| {
            u.attack.interval = slowest(*ATTACK_MS.end()).interval
        });
        gate_invalid("release slower than the longest", &|u| {
            u.release.sample = slowest(*RELEASE_MS.end()).sample
        });
        comp_invalid("attack slower than the longest", &|u| {
            u.attack.sample = slowest(*ATTACK_MS.end()).sample
        });
        comp_invalid("release slower than the longest", &|u| {
            u.release.interval = 0.999_999_94
        });
        comp_invalid("gain beyond any reduction", &|u| u.y = -801.0);
        comp_invalid("step beyond one interval", &|u| u.interval.step = -0.13);
        comp_invalid("slope beyond the ratio", &|u| u.slope = -2.0);
        comp_invalid("makeup beyond its range", &|u| u.makeup.target = 100.0);
    }

    /// The lowest smoothed gain a compressor reaches is within
    /// [`COMP_GAIN_MIN_DB`]: at ratio 20 and threshold -60 dBFS, the
    /// widest of the ranges, a key of `f32::MAX`, 128 octaves or 770.6
    /// dBFS, is 830.6 dB over and takes `(1 / 20 - 1) 830.6`, -789.1 dB;
    /// a compressor fed `f32::MAX` and infinities settles there.
    #[test]
    fn the_largest_reduction_is_within_its_bound() {
        let ratio = *crate::strip::COMP_RATIO.end();
        let threshold = *crate::strip::COMP_THRESHOLD_DB.start();
        let key_db = log2(f32::MAX) * DB_PER_OCTAVE;
        let largest = (1.0 / ratio - 1.0) * (key_db - threshold);
        assert!((-790.0..-789.0).contains(&largest), "{largest}");
        assert!(largest > COMP_GAIN_MIN_DB);
        let mut c = Comp::new();
        c.set(
            &CompParams {
                enabled: true,
                threshold_db: threshold,
                ratio,
                attack_ms: *ATTACK_MS.start(),
                release_ms: 50.0,
                knee_db: 0.0,
                makeup_db: 0.0,
                mix: 1.0,
            },
            0,
        );
        let mut lowest = 0.0f32;
        for n in 0..2000 {
            let x = if n % 3 == 0 { f32::INFINITY } else { f32::MAX };
            let mut buf = [x; 48];
            c.process(&mut buf, 1, 48);
            lowest = lowest.min(c.y);
            assert!(c.is_valid(), "{c:?}");
        }
        assert!((COMP_GAIN_MIN_DB..-789.0).contains(&lowest), "{lowest}");
    }

    /// A compressor deep in reduction on the programmes below: 20 dB
    /// over the threshold on their loud parts, 12 dB of makeup.
    fn heavy(attack_ms: f32, knee_db: f32, mix: f32) -> CompParams {
        CompParams {
            enabled: true,
            threshold_db: -30.0,
            ratio: 10.0,
            attack_ms,
            release_ms: 80.0,
            knee_db,
            makeup_db: 12.0,
            mix,
        }
    }

    /// Programme of `legs` legs, planar, `secs` long as sampled at `rate`
    /// Hz: kind 0 sine bursts, 997 Hz at -6 dBFS for 60 ms every 200 ms
    /// and -40 dBFS between; kind 1 noise at -30 dBFS with a 180 Hz
    /// transient from -3 dBFS decaying over 20 ms every 250 ms. Leg `l` is `l + 1` times quieter,
    /// so a linked sidechain reduces the quiet legs by the loud one's
    /// gain. The engine computes at `SAMPLE_RATE`: another rate changes
    /// the programme's shape in samples, not the compressor.
    fn programme(kind: usize, rate: f64, legs: usize, secs: f64) -> Vec<f32> {
        let frames = (rate * secs) as usize;
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut x = vec![0.0f32; legs * frames];
        for leg in 0..legs {
            let scale = 1.0 / (leg + 1) as f64;
            for n in 0..frames {
                let t = n as f64 / rate;
                let v = if kind == 0 {
                    let level = if t % 0.2 < 0.06 { 0.5 } else { 0.01 };
                    level * libm::sin(2.0 * core::f64::consts::PI * 997.0 * t + leg as f64)
                } else {
                    seed = seed
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let noise = ((seed >> 40) as f64 / (1u64 << 24) as f64 - 0.5) * 0.063;
                    let since = t % 0.25;
                    noise
                        + 0.708
                            * libm::exp(-since / 0.02)
                            * libm::sin(2.0 * core::f64::consts::PI * 180.0 * since)
                };
                x[leg * frames + n] = (scale * v) as f32;
            }
        }
        x
    }

    /// Runs planar `x` of `legs` legs through a compressor set to `p` at
    /// index 0 and to each of `changes` at its index, all ramping over
    /// `ramp` samples, in blocks of `b` frames split at the changes.
    fn run_comp(
        p: &CompParams,
        changes: &[(usize, CompParams)],
        ramp: u32,
        x: &[f32],
        legs: usize,
        b: usize,
    ) -> Vec<f32> {
        let frames = x.len() / legs;
        let mut c = Comp::new();
        c.set(p, ramp);
        let mut y = x.to_vec();
        let mut buf = vec![0.0f32; legs * b];
        let mut start = 0;
        let mut next = changes.iter().peekable();
        while start < frames {
            if let Some((_, q)) = next.next_if(|(at, _)| *at == start) {
                c.set(q, ramp);
            }
            let until = next.peek().map_or(frames, |(at, _)| *at);
            let n = b.min(frames - start).min(until - start);
            for leg in 0..legs {
                buf[leg * n..(leg + 1) * n].copy_from_slice(&x[leg * frames + start..][..n]);
            }
            c.process(&mut buf[..legs * n], legs, n);
            for leg in 0..legs {
                y[leg * frames + start..][..n].copy_from_slice(&buf[leg * n..(leg + 1) * n]);
            }
            start += n;
        }
        y
    }

    /// Every programme at 44.1, 48 and 96 kHz, mono and stereo, through
    /// a compressor at a per-sample and an interval attack, with a hard
    /// and a soft knee, in blocks of 48 and of 5: (programme, legs,
    /// block, compressor at full mix).
    fn mix_cases() -> Vec<(Vec<f32>, usize, usize, CompParams)> {
        let mut cases = Vec::new();
        for kind in [0, 1] {
            for rate in [44_100.0, 48_000.0, 96_000.0] {
                for legs in [1, 2] {
                    let x = programme(kind, rate, legs, 0.6);
                    for (attack, knee) in [(0.1, 0.0), (1.0, 6.0)] {
                        for b in [48, 5] {
                            cases.push((x.clone(), legs, b, heavy(attack, knee, 1.0)));
                        }
                    }
                }
            }
        }
        cases
    }

    /// Parallel compression: at a mix `m` the output is `(1 - m) x + m
    /// c`, `x` the input and `c` the compressor's output alone, makeup
    /// included, computed here in f64 from the two. The engine applies it
    /// as one gain per sample, `1 + m (g - 1)`, whose rounding keeps every
    /// sample within 4 f32 epsilons of `|x| + |c|` of the reference. A
    /// makeup applied to both shares, or `m` and `1 - m` swapped, is off
    /// by far more on these programmes; at 0.5 the output is `(x + c) /
    /// 2`. Linked legs take one gain: the gain of each sample is the
    /// same on every leg.
    #[test]
    fn a_mix_is_the_input_and_the_compressed_signal_mixed() {
        let eps = f64::from(f32::EPSILON);
        for (x, legs, b, p) in mix_cases() {
            let frames = x.len() / legs;
            let c = run_comp(&p, &[], 0, &x, legs, b);
            let reduced = x
                .iter()
                .zip(&c)
                .filter(|(x, c)| c.abs() < 0.5 * x.abs())
                .count();
            assert!(
                reduced > frames / 50,
                "{p:?}: {reduced} samples reduced 6 dB"
            );
            for m in [0.25f32, 0.5, 0.75] {
                let got = run_comp(&CompParams { mix: m, ..p }, &[], 0, &x, legs, b);
                let m64 = f64::from(m);
                for i in 0..x.len() {
                    let (xi, ci) = (f64::from(x[i]), f64::from(c[i]));
                    let want = (1.0 - m64) * xi + m64 * ci;
                    let err = (f64::from(got[i]) - want).abs();
                    let bound = 4.0 * eps * (xi.abs() + ci.abs()) + f64::from(f32::MIN_POSITIVE);
                    assert!(
                        err <= bound,
                        "{p:?} mix {m}, {legs} legs, blocks of {b}, sample {i}: {} for {want} ({err:e} > {bound:e})",
                        got[i]
                    );
                }
                if legs == 2 {
                    for n in 0..frames {
                        let (x0, x1) = (x[n], x[frames + n]);
                        if x0.abs() < 1e-3 || x1.abs() < 1e-3 {
                            continue;
                        }
                        let gain = |y: f32, x: f32| f64::from(y) / f64::from(x);
                        let (g0, g1) = (gain(got[n], x0), gain(got[frames + n], x1));
                        assert!(
                            (g0 - g1).abs() <= 4.0 * eps * g0.abs().max(g1.abs()),
                            "{p:?} mix {m}, sample {n}: leg gains {g0} and {g1}"
                        );
                    }
                }
            }
        }
    }

    /// A mix of 0 passes every bit of the input, switched on at it or
    /// ramped to it, while the gain keeps following the key: on these
    /// programmes, more than 6 dB of reduction on their loud parts.
    #[test]
    fn a_mix_of_0_passes_the_input() {
        let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        for (x, legs, b, p) in mix_cases() {
            let dry = run_comp(&CompParams { mix: 0.0, ..p }, &[], 480, &x, legs, b);
            assert_eq!(bits(&dry), bits(&x), "{p:?} at mix 0, {legs} legs");
            if legs == 1 {
                let mut c = Comp::new();
                c.set(&CompParams { mix: 0.0, ..p }, 480);
                let mut y = x.clone();
                let mut deepest = 0.0f32;
                for chunk in y.chunks_mut(b) {
                    c.process(chunk, 1, chunk.len());
                    deepest = deepest.min(c.y);
                }
                assert_eq!(bits(&y), bits(&x));
                assert!(deepest < -6.0, "{p:?} at mix 0: deepest gain {deepest} dB");
            }
            // Ramped from 1 to 0 after 0.1 s: the input from the ramp's
            // end on.
            let frames = x.len() / legs;
            let at = 4801;
            let ramped = run_comp(&p, &[(at, CompParams { mix: 0.0, ..p })], 480, &x, legs, b);
            for leg in 0..legs {
                let from = leg * frames + at + 480;
                let to = (leg + 1) * frames;
                assert_eq!(
                    bits(&ramped[from..to]),
                    bits(&x[from..to]),
                    "{p:?} ramped to 0"
                );
            }
        }
    }

    /// At full mix the compressor computes the bits it computed before
    /// the mix existed: the FNV-1a hash of its output on every programme
    /// case is the one samplez v0.24.0 computes.
    #[test]
    fn a_full_mix_computes_the_bits_of_the_compressor_before_the_mix() {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for (x, legs, b, p) in mix_cases() {
            for v in run_comp(&p, &[], 480, &x, legs, b) {
                h = (h ^ u64::from(v.to_bits())).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        assert_eq!(h, 0x8375_fc3d_06de_0ffd);
    }

    /// The encoding of a compressor at full mix is the one before the
    /// mix existed, and that one decodes at full mix: logs, product
    /// states and core copies written before carry the same meaning and
    /// bytes. Another mix is encoded and decodes to its bits.
    #[test]
    fn a_full_mix_encodes_as_before_the_mix_existed() {
        let before = r#"{"enabled":true,"threshold_db":-30.0,"ratio":10.0,"attack_ms":1.0,"release_ms":80.0,"knee_db":6.0,"makeup_db":12.0}"#;
        let p: CompParams = serde_json::from_str(before).unwrap();
        assert_eq!(p, heavy(1.0, 6.0, 1.0));
        assert_eq!(p.mix.to_bits(), 1.0f32.to_bits());
        assert_eq!(serde_json::to_string(&p).unwrap(), before);
        for mix in [0.0, 0.3, 0.999_999_94] {
            let q = CompParams { mix, ..p };
            let encoded = serde_json::to_string(&q).unwrap();
            assert!(encoded.contains(r#""mix":"#), "{encoded}");
            let back: CompParams = serde_json::from_str(&encoded).unwrap();
            assert_eq!(back.mix.to_bits(), mix.to_bits());
        }
    }

    /// The gain of a compressor switched on at a mix of 0 follows the key
    /// throughout, so raising the mix starts from the gain it reached:
    /// under a level 20 dB over the threshold, a mix raised from 0 to 0.5
    /// computes from the change on the bits of a compressor that ran at
    /// full mix, was dropped to 0 at once and raised the same way. One
    /// switched on only at the change, its gain starting at 0 dB, computes
    /// other bits, and with a 10 ms attack plays louder while it attacks
    /// into reduction. Up to the change the output is the input's bits.
    #[test]
    fn a_mix_raised_from_0_starts_from_the_gain_followed_at_0() {
        let len = 480u32;
        let x = vec![db_to_gain(-10.0); 48_000];
        let at = 24_000 + 203;
        let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        for attack in [0.1, 1.0, 10.0] {
            let p = heavy(attack, 0.0, 0.0);
            let half = CompParams { mix: 0.5, ..p };
            let raised = run_comp(&p, &[(at, half)], len, &x, 1, 48);
            assert_eq!(
                bits(&raised[..at]),
                bits(&x[..at]),
                "attack {attack}: dry at 0"
            );
            let mut c = Comp::new();
            c.set(&CompParams { mix: 1.0, ..p }, len);
            let mut reference = x.clone();
            let (head, tail) = reference.split_at_mut(at);
            for b in head.chunks_mut(48) {
                c.process(b, 1, b.len());
            }
            c.set(&p, 0);
            c.set(&half, len);
            for b in tail.chunks_mut(48) {
                c.process(b, 1, b.len());
            }
            assert_eq!(
                bits(&raised[at..]),
                bits(&reference[at..]),
                "attack {attack}"
            );
            let off = CompParams {
                enabled: false,
                ..p
            };
            let cold = run_comp(&off, &[(at, half)], len, &x, 1, 48);
            assert_ne!(bits(&cold[at..]), bits(&raised[at..]), "attack {attack}");
            if attack == 10.0 {
                let louder = (at..at + len as usize).any(|n| cold[n] > raised[n] * 1.01);
                assert!(louder, "switching on at the change starts no attack");
            }
        }
    }

    /// A mix change ramps over the ramp length from the sample it is set
    /// at: on a steady level with the compressor settled, each sample
    /// moves the output by at most the ramp's share of the whole change,
    /// `|x| |m1 - m0| |g - 1| / len`, `g` the settled gain with makeup,
    /// and from the ramp's end on the output is that of a compressor set
    /// to the new mix from the start. Up to the change it is the old
    /// mix's; the bits do not depend on the blocks.
    #[test]
    fn a_mix_change_ramps_without_a_step() {
        let len = 480u32;
        let x = vec![db_to_gain(-6.0); 48_000 + 2400];
        let at = 48_000 + 203;
        for attack in [0.1, 1.0] {
            for (m0, m1) in [
                (1.0f32, 0.25f32),
                (0.25, 1.0),
                (0.5, 0.0),
                (1.0, 0.0),
                (0.4, 0.6),
            ] {
                let p = heavy(attack, 0.0, m0);
                let q = CompParams { mix: m1, ..p };
                let runs: Vec<Vec<f32>> = [48, 5, 1000]
                    .map(|b| run_comp(&p, &[(at, q)], len, &x, 1, b))
                    .into();
                assert!(
                    runs.iter().all(|r| r == &runs[0]),
                    "{m0} -> {m1}: blocks differ"
                );
                let y = &runs[0];
                let before = run_comp(&p, &[], len, &x, 1, 48);
                let after = run_comp(&q, &[], len, &x, 1, 48);
                assert_eq!(y[..at], before[..at], "{m0} -> {m1} before the change");
                let end = at + len as usize;
                assert_eq!(y[end..], after[end..], "{m0} -> {m1} after the ramp");
                // The settled gain with makeup, from the full mix run.
                let full = run_comp(&CompParams { mix: 1.0, ..p }, &[], 0, &x, 1, 48);
                let g = f64::from(full[at]) / f64::from(x[at]);
                let whole = f64::from(x[at]) * f64::from(m1 - m0).abs() * (g - 1.0).abs();
                assert!(whole > 0.05, "{m0} -> {m1}: the change moves {whole}");
                let bound = whole / f64::from(len) * 1.001 + 8.0 * f64::from(f32::EPSILON);
                for n in at - 1..end + 1 {
                    let step = (f64::from(y[n + 1]) - f64::from(y[n])).abs();
                    assert!(
                        step <= bound,
                        "{m0} -> {m1}, sample {n}: steps {step} > {bound}"
                    );
                }
            }
        }
    }

    /// The mix adds no latency: an impulse comes out at its own sample
    /// and nowhere else at every mix, and on programme the output lines
    /// up with the input, the cross-correlation peaking at lag 0.
    #[test]
    fn a_mix_adds_no_latency() {
        for attack in [0.1, 1.0] {
            for m in [0.0, 0.25, 0.5, 0.75, 1.0] {
                let p = heavy(attack, 0.0, m);
                for at in 4800..4800 + GAIN_INTERVAL as usize {
                    for v in [0.9f32, -0.9] {
                        let mut x = vec![0.0f32; 9600];
                        x[at] = v;
                        let y = run_comp(&p, &[], 0, &x, 1, 48);
                        let out: Vec<usize> = (0..y.len()).filter(|&n| y[n] != 0.0).collect();
                        assert_eq!(out, [at], "attack {attack} mix {m}: impulse at {at}");
                        assert_eq!(y[at].signum(), v.signum());
                    }
                }
                let x = programme(1, 48_000.0, 1, 0.6);
                let y = run_comp(&p, &[], 0, &x, 1, 48);
                let corr: Vec<(i64, f64)> = (-64..=64i64)
                    .map(|lag| {
                        let sum = (64..x.len() - 64)
                            .map(|n| f64::from(x[n]) * f64::from(y[(n as i64 + lag) as usize]))
                            .sum::<f64>();
                        (lag, sum.abs())
                    })
                    .collect();
                let peak = corr.iter().max_by(|a, b| a.1.total_cmp(&b.1)).unwrap();
                assert_eq!(peak.0, 0, "attack {attack} mix {m}");
            }
        }
    }
}
