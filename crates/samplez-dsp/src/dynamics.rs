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
//! applies exactly its settled gain. Switching a unit on or off ramps
//! its effect over the ramp length.
//!
//! The gain computer and the smoother run once every [`GAIN_INTERVAL`]
//! samples, on the largest key of the interval, at the sample indices
//! that are multiples of it; between two runs the gain moves linearly to
//! the next. The key of a sample acts from the next interval on: a unit
//! attacks up to [`GAIN_INTERVAL`] samples and releases up to twice that
//! later than a gain computed every sample, and an attack overshoots by
//! what that gain would have reduced in the time. Which samples share an
//! interval depends on the sample index alone, not on how the samples
//! are split into blocks.

use crate::Ramp;
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

/// The one-pole coefficient of time constant `ms` over one
/// [`GAIN_INTERVAL`].
fn pole(ms: f32) -> f32 {
    libm::exp(-1000.0 * f64::from(GAIN_INTERVAL) / (f64::from(ms) * SAMPLE_RATE)) as f32
}

fn db_to_gain(db: f32) -> f32 {
    libm::pow(10.0, f64::from(db) / 20.0) as f32
}

/// `20 log10(2)`: dB per octave of gain.
const DB_PER_OCTAVE: f32 = 6.020_6;
/// `1 / DB_PER_OCTAVE`.
const OCTAVES_PER_DB: f32 = 0.166_096_4;

/// `log2(x)` of a positive normal `x`, within 1e-7 plus rounding.
///
/// The gain computer runs on this and [`exp2`] rather than
/// libm's `logf` and `expf`: a few multiplies and adds with no call,
/// pure f32 arithmetic without FMA, so the same bits on every host and
/// build whatever the libm version. The mantissa is taken to
/// [1/sqrt(2), sqrt(2)] and its logarithm summed as `2 atanh(t)` with
/// `t = (m - 1) / (m + 1)`, `|t| <= 0.172`, to the `t^7` term.
#[inline]
fn log2(x: f32) -> f32 {
    const C1: f32 = 2.885_39;
    const C3: f32 = 0.961_796_7;
    const C5: f32 = 0.577_078;
    const C7: f32 = 0.412_198_6;
    let bits = x.to_bits();
    let mut e = ((bits >> 23) & 0xff) as i32 - 127;
    let mut m = f32::from_bits((bits & 0x007f_ffff) | 0x3f80_0000);
    if m > core::f32::consts::SQRT_2 {
        m *= 0.5;
        e += 1;
    }
    let t = (m - 1.0) / (m + 1.0);
    let t2 = t * t;
    e as f32 + t * (C1 + t2 * (C3 + t2 * (C5 + t2 * C7)))
}

/// `2^x` within 2e-7 relative, 0 below -126, `2^127` above 127: the
/// nearest integer `n` as the exponent, `2^(x - n)` with `|x - n| <= 0.5`
/// by its Taylor series to the 6th power. Pure f32 arithmetic, see
/// [`log2`].
#[inline]
fn exp2(x: f32) -> f32 {
    const E: [f32; 6] = [
        core::f32::consts::LN_2,
        0.240_226_5,
        0.055_504_11,
        0.009_618_129,
        0.001_333_356,
        0.000_154_035_3,
    ];
    if x < -126.0 {
        return 0.0;
    }
    let x = x.min(127.0);
    // x + 127.5 is positive, so the conversion's truncation is a floor.
    let n = (x + 127.5) as i32 - 127;
    let f = x - n as f32;
    let p = 1.0 + f * (E[0] + f * (E[1] + f * (E[2] + f * (E[3] + f * (E[4] + f * E[5])))));
    p * f32::from_bits(((n + 127) as u32) << 23)
}

/// [`db_to_gain`] in f32, for the computed gain.
#[inline]
fn db_to_gain_f32(db: f32) -> f32 {
    exp2(db * OCTAVES_PER_DB)
}

/// One smoothing step of `y` towards `target` with pole `a`, one
/// [`GAIN_INTERVAL`] long, snapping within [`SNAP_DB`].
#[inline]
fn smooth(y: f32, target: f32, a: f32) -> f32 {
    let next = target + a * (y - target);
    if (next - target).abs() < SNAP_DB {
        target
    } else {
        next
    }
}

#[inline]
fn key(buf: &[f32], legs: usize, frames: usize, n: usize) -> f32 {
    let mut k = 0.0f32;
    for leg in 0..legs {
        let v = buf[leg * frames + n].abs();
        if v > k {
            k = v;
        }
    }
    k
}

/// The interval of the gain computer running: the largest key so far,
/// the samples of it processed, and the gain moving linearly from `from`
/// by `step` a sample to `to`, the gain computed at its start.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Interval {
    peak: f32,
    phase: u32,
    from: f32,
    step: f32,
    to: f32,
}

impl Interval {
    /// At 0 dB and the start of an interval.
    const REST: Interval = Interval {
        peak: 0.0,
        phase: 0,
        from: 1.0,
        step: 0.0,
        to: 1.0,
    };

    /// Takes the key of the next sample in and returns its gain.
    #[inline]
    fn gain(&mut self, key: f32) -> f32 {
        if key > self.peak {
            self.peak = key;
        }
        self.phase += 1;
        self.from + self.phase as f32 * self.step
    }

    /// Whether the interval is complete, for the gain computer to run.
    #[inline]
    fn is_complete(&self) -> bool {
        self.phase == GAIN_INTERVAL
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
        let phase = self.phase;
        *self = Interval {
            phase,
            ..Interval::REST
        };
        self.advance(frames);
    }

    fn advance(&mut self, frames: usize) {
        self.phase = ((self.phase as usize + frames) % GAIN_INTERVAL as usize) as u32;
    }

    fn align(&mut self, index: u64) {
        self.phase = (index % u64::from(GAIN_INTERVAL)) as u32;
    }
}

/// A unit's switch: its effect `1 + w * (gain - 1)` with `w` ramping
/// between 0 and 1.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Wet(Ramp);

impl Wet {
    fn new() -> Self {
        Wet(Ramp::new(0.0))
    }

    fn set(&mut self, on: bool, len: u32) {
        let _ = self.0.set_target(if on { 1.0 } else { 0.0 }, len);
    }

    /// Off and not ramping.
    fn is_off(&self) -> bool {
        !self.0.is_active() && self.0.target() == 0.0
    }
}

/// Gate parameters, the faderz `Gate`.
#[derive(Clone, Copy, Debug, PartialEq)]
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
#[derive(Clone, Copy, Debug, PartialEq)]
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
    /// Gain after the compressor, dB.
    pub makeup_db: f32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Gate {
    wet: Wet,
    threshold: f32,
    /// [`GATE_HYSTERESIS_DB`] below `threshold`.
    reopen: f32,
    range_db: f32,
    floor: f32,
    attack: f32,
    release: f32,
    /// Smoothed gain, dB, computed at the start of the interval.
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
            attack: 0.0,
            release: 0.0,
            y: 0.0,
            hold: 0,
            interval: Interval::REST,
        }
    }

    pub(crate) fn set(&mut self, p: &GateParams, ramp: u32) {
        self.wet.set(p.enabled, ramp);
        self.threshold = db_to_gain(p.threshold_db);
        self.reopen = db_to_gain(p.threshold_db - GATE_HYSTERESIS_DB);
        self.range_db = -p.range_db;
        self.floor = db_to_gain(-p.range_db);
        self.attack = pole(p.attack_ms);
        self.release = pole(p.release_ms);
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
        for n in 0..frames {
            let gain = iv.gain(key(buf, legs, frames, n));
            let w = u.wet.0.value_at(u.wet.0.pos + n as u32);
            let g = 1.0 + w * (gain - 1.0);
            if g != 1.0 {
                for leg in 0..legs {
                    buf[leg * frames + n] *= g;
                }
            }
            if iv.is_complete() {
                let open_at = if hold > 0 { u.reopen } else { u.threshold };
                if iv.peak >= open_at {
                    hold = GATE_HOLD;
                } else {
                    hold = hold.saturating_sub(GAIN_INTERVAL);
                }
                let target = if hold > 0 { 0.0 } else { u.range_db };
                let a = if target > y { u.attack } else { u.release };
                y = smooth(y, target, a);
                iv.start(if y == 0.0 {
                    1.0
                } else if y == u.range_db {
                    u.floor
                } else {
                    db_to_gain_f32(y)
                });
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

    /// Whether the gate passes every input unchanged until it is set
    /// again: off, or a range of 0 dB with the gain at 0 dB.
    pub(crate) fn is_identity(&self) -> bool {
        self.wet.is_off() || (self.range_db == 0.0 && self.y == 0.0 && self.interval.from == 1.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Comp {
    wet: Wet,
    threshold_db: f32,
    slope: f32,
    knee_db: f32,
    /// Below this key the gain computer gives 0 dB.
    knee_start: f32,
    attack: f32,
    release: f32,
    makeup: Ramp,
    /// Smoothed gain, dB, computed at the start of the interval.
    y: f32,
    interval: Interval,
}

impl Comp {
    pub(crate) fn new() -> Self {
        Comp {
            wet: Wet::new(),
            threshold_db: 0.0,
            slope: 0.0,
            knee_db: 0.0,
            knee_start: 1.0,
            attack: 0.0,
            release: 0.0,
            makeup: Ramp::new(1.0),
            y: 0.0,
            interval: Interval::REST,
        }
    }

    pub(crate) fn set(&mut self, p: &CompParams, ramp: u32) {
        self.wet.set(p.enabled, ramp);
        self.threshold_db = p.threshold_db;
        self.slope = 1.0 / p.ratio - 1.0;
        self.knee_db = p.knee_db;
        self.knee_start = db_to_gain(p.threshold_db - p.knee_db / 2.0);
        self.attack = pole(p.attack_ms);
        self.release = pole(p.release_ms);
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

    pub(crate) fn process(&mut self, buf: &mut [f32], legs: usize, frames: usize) {
        if self.wet.is_off() {
            self.y = 0.0;
            self.interval.rest(frames);
            self.makeup.advance(frames);
            return;
        }
        let c = *self;
        let (mut y, mut iv) = (self.y, self.interval);
        for n in 0..frames {
            let reduction = iv.gain(key(buf, legs, frames, n));
            let gain = reduction * c.makeup.value_at(c.makeup.pos + n as u32);
            let w = c.wet.0.value_at(c.wet.0.pos + n as u32);
            let g = 1.0 + w * (gain - 1.0);
            if g != 1.0 {
                for leg in 0..legs {
                    buf[leg * frames + n] *= g;
                }
            }
            if iv.is_complete() {
                // Below the knee the gain is 0 dB; the comparison also
                // keeps a zero key from the logarithm.
                let target = if iv.peak > c.knee_start {
                    c.static_gain(log2(iv.peak) * DB_PER_OCTAVE)
                } else {
                    0.0
                };
                let a = if target < y { c.attack } else { c.release };
                y = smooth(y, target, a);
                iv.start(if y == 0.0 { 1.0 } else { db_to_gain_f32(y) });
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

    /// Whether the compressor passes every input unchanged until it is
    /// set again: off, or a ratio of 1 with the gain at 0 dB and the
    /// makeup at rest at 0 dB.
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
        let pole = |ms: f32| libm::exp(-1000.0 / (f64::from(ms) * SAMPLE_RATE)) as f32;
        let (attack, release) = (pole(p.attack_ms), pole(p.release_ms));
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

    /// The gain computer's log2 and exp2 stay within 0.001 dB of the
    /// exact ones over every key and gain the units meet, and compute
    /// the bits pinned here: a build or host giving others fails.
    #[test]
    fn log2_and_exp2_are_exact_to_a_thousandth_of_a_db() {
        let mut fold = 0u64;
        for i in 0..=20_000 {
            // Keys from -140 dBFS to +80 dBFS.
            let x = 10f64.powf(-7.0 + 11.0 * f64::from(i) / 20_000.0) as f32;
            let want = 20.0 * f64::from(x).log10();
            let got = f64::from(log2(x) * DB_PER_OCTAVE);
            assert!((got - want).abs() < 1e-3, "key {x}: {got} dB, want {want}");
            // Gains from -200 dB to +40 dB.
            let db = -200.0 + 240.0 * i as f32 / 20_000.0;
            let g = db_to_gain_f32(db);
            let got = 20.0 * f64::from(g).log10();
            assert!((got - f64::from(db)).abs() < 1e-3, "{db} dB: {got}");
            fold = fold.rotate_left(5) ^ u64::from(log2(x).to_bits());
            fold = fold.rotate_left(5) ^ u64::from(g.to_bits());
        }
        assert_eq!(exp2(-127.0), 0.0);
        assert_eq!(exp2(0.0), 1.0);
        assert_eq!(log2(1.0), 0.0);
        assert_eq!(fold, PINNED, "{fold:#x}");
    }

    const PINNED: u64 = 0x40ba_4aaf_02b7_7ca1;

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
}
