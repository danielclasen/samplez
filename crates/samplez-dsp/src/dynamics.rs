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

use crate::Ramp;
use crate::svf::SAMPLE_RATE;

/// Gate hold after the key last reached the threshold: 20 ms, half a
/// period of 25 Hz, so the gate does not close on the zero crossings of
/// a sustained low note.
pub const GATE_HOLD: u32 = 960;
/// The smoothed gain snaps to its target within this, dB.
const SNAP_DB: f32 = 1.0e-4;

/// The one-pole coefficient of time constant `ms`.
fn pole(ms: f32) -> f32 {
    libm::exp(-1000.0 / (f64::from(ms) * SAMPLE_RATE)) as f32
}

fn db_to_gain(db: f32) -> f32 {
    libm::pow(10.0, f64::from(db) / 20.0) as f32
}

/// `ln(10) / 20`: dB to the natural logarithm of the gain.
const DB_TO_LN: f32 = core::f32::consts::LN_10 / 20.0;

/// [`db_to_gain`] in f32, for the per-sample gain.
#[inline]
fn db_to_gain_f32(db: f32) -> f32 {
    libm::expf(db * DB_TO_LN)
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
    range_db: f32,
    floor: f32,
    attack: f32,
    release: f32,
    /// Smoothed gain, dB.
    y: f32,
    hold: u32,
}

impl Gate {
    pub(crate) fn new() -> Self {
        Gate {
            wet: Wet::new(),
            threshold: 0.0,
            range_db: 0.0,
            floor: 1.0,
            attack: 0.0,
            release: 0.0,
            y: 0.0,
            hold: 0,
        }
    }

    pub(crate) fn set(&mut self, p: &GateParams, ramp: u32) {
        self.wet.set(p.enabled, ramp);
        self.threshold = db_to_gain(p.threshold_db);
        self.range_db = -p.range_db;
        self.floor = db_to_gain(-p.range_db);
        self.attack = pole(p.attack_ms);
        self.release = pole(p.release_ms);
    }

    pub(crate) fn process(&mut self, buf: &mut [f32], legs: usize, frames: usize) {
        if self.wet.is_off() {
            self.y = 0.0;
            self.hold = 0;
            return;
        }
        for n in 0..frames {
            if key(buf, legs, frames, n) >= self.threshold {
                self.hold = GATE_HOLD;
            } else {
                self.hold = self.hold.saturating_sub(1);
            }
            let target = if self.hold > 0 { 0.0 } else { self.range_db };
            let a = if target > self.y {
                self.attack
            } else {
                self.release
            };
            self.y = smooth(self.y, target, a);
            let gain = if self.y == 0.0 {
                1.0
            } else if self.y == self.range_db {
                self.floor
            } else {
                db_to_gain_f32(self.y)
            };
            let w = self.wet.0.value_at(self.wet.0.pos + n as u32);
            let g = 1.0 + w * (gain - 1.0);
            if g != 1.0 {
                for leg in 0..legs {
                    buf[leg * frames + n] *= g;
                }
            }
        }
        self.wet.0.advance(frames);
    }

    pub(crate) fn advance(&mut self, frames: usize) {
        self.wet.0.advance(frames);
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
    y: f32,
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
            return;
        }
        for n in 0..frames {
            let k = key(buf, legs, frames, n);
            // Below the knee the gain is 0 dB; the comparison also keeps
            // a zero key from the logarithm.
            let target = if k > self.knee_start {
                self.static_gain(libm::logf(k) / DB_TO_LN)
            } else {
                0.0
            };
            let a = if target < self.y {
                self.attack
            } else {
                self.release
            };
            self.y = smooth(self.y, target, a);
            let reduction = if self.y == 0.0 {
                1.0
            } else {
                db_to_gain_f32(self.y)
            };
            let gain = reduction * self.makeup.value_at(self.makeup.pos + n as u32);
            let w = self.wet.0.value_at(self.wet.0.pos + n as u32);
            let g = 1.0 + w * (gain - 1.0);
            if g != 1.0 {
                for leg in 0..legs {
                    buf[leg * frames + n] *= g;
                }
            }
        }
        self.makeup.advance(frames);
        self.wet.0.advance(frames);
    }

    pub(crate) fn advance(&mut self, frames: usize) {
        self.wet.0.advance(frames);
        self.makeup.advance(frames);
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
    /// into reduction, release out of it.
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
                // At tau the curve has covered 1 - 1/e; sample tau and
                // tau + 1 bracket it.
                let (a, b) = (g[tau - 1], g[tau + 1]);
                assert!(
                    (a.min(b) - 0.05..=a.max(b) + 0.05).contains(&want),
                    "{ms} ms: {a} .. {b} around {want}"
                );
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
}
