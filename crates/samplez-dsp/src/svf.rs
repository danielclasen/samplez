//! The strip's filters: a state variable filter in the trapezoidal form
//! of Andrew Simper (Cytomic, "SvfLinearTrapOptimised2"), whose outputs
//! mix to the high-pass, bell and shelf responses of the RBJ Audio EQ
//! Cookbook (Robert Bristow-Johnson), with the same bilinear prewarp.
//!
//! The coefficients `(g, k, m0, m1, m2)` are computed in f64 with the
//! pure-Rust `libm`, so every replica gets the same bits, and stored as
//! f32. A change ramps all five linearly over the ramp length, each
//! sample's value computed from its position as in [`crate::Ramp`]; the
//! filter stays stable at every intermediate value, since `g > 0` and
//! `k > 0` throughout. The state runs in f32, one leg after the other,
//! in a fixed order of operations without FMA. After each block a state
//! below -400 dB is set to zero, so silence decays to exact zeros.

/// Sample rate the coefficients are computed for.
pub const SAMPLE_RATE: f64 = 48_000.0;

/// State magnitude set to zero after each block: -400 dB.
const FLUSH: f32 = 1.0e-20;

/// `(g, k, m0, m1, m2)`.
pub(crate) type Coefficients = [f32; 5];

/// The response of a filter section. Frequencies in Hz, gains in dB.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Shape {
    /// Second-order Butterworth high-pass, 12 dB per octave, -3.01 dB at
    /// `freq`: the cookbook HPF with Q = 1/sqrt(2).
    HighPass { freq: f32 },
    /// The cookbook peakingEQ.
    Bell { freq: f32, gain_db: f32, q: f32 },
    /// The cookbook lowShelf with its Q, `gain_db / 2` at `freq`.
    LowShelf { freq: f32, gain_db: f32, q: f32 },
    /// The cookbook highShelf with its Q, `gain_db / 2` at `freq`.
    HighShelf { freq: f32, gain_db: f32, q: f32 },
}

fn warp(freq: f32) -> f64 {
    libm::tan(core::f64::consts::PI * f64::from(freq) / SAMPLE_RATE)
}

fn amplitude(gain_db: f32) -> f64 {
    libm::pow(10.0, f64::from(gain_db) / 40.0)
}

/// The coefficients of `shape`.
pub(crate) fn coefficients(shape: Shape) -> Coefficients {
    let (g, k, m0, m1, m2) = match shape {
        Shape::HighPass { freq } => {
            let k = core::f64::consts::SQRT_2;
            (warp(freq), k, 1.0, -k, -1.0)
        }
        Shape::Bell { freq, gain_db, q } => {
            let a = amplitude(gain_db);
            let k = 1.0 / (f64::from(q) * a);
            (warp(freq), k, 1.0, k * (a * a - 1.0), 0.0)
        }
        Shape::LowShelf { freq, gain_db, q } => {
            let a = amplitude(gain_db);
            let k = 1.0 / f64::from(q);
            (
                warp(freq) / libm::sqrt(a),
                k,
                1.0,
                k * (a - 1.0),
                a * a - 1.0,
            )
        }
        Shape::HighShelf { freq, gain_db, q } => {
            let a = amplitude(gain_db);
            let k = 1.0 / f64::from(q);
            (
                warp(freq) * libm::sqrt(a),
                k,
                a * a,
                k * (1.0 - a) * a,
                1.0 - a * a,
            )
        }
    };
    [g as f32, k as f32, m0 as f32, m1 as f32, m2 as f32]
}

/// The coefficients of an identity before any shape: 1 kHz, k = 1.
fn neutral() -> Coefficients {
    [warp(1000.0) as f32, 1.0, 1.0, 0.0, 0.0]
}

/// One filter section over up to `L` legs: coefficients shared by the
/// legs, state per leg.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Svf<const L: usize> {
    c0: Coefficients,
    step: Coefficients,
    target: Coefficients,
    len: u32,
    /// Ramp position of the next sample.
    pos: u32,
    /// `(a1, a2, a3)` of `target`.
    a: [f32; 3],
    /// `(ic1eq, ic2eq)` per leg.
    pub(crate) state: [[f32; 2]; L],
}

#[inline]
fn a_of(c: &Coefficients) -> [f32; 3] {
    let a1 = 1.0 / (1.0 + c[0] * (c[0] + c[1]));
    let a2 = c[0] * a1;
    [a1, a2, c[0] * a2]
}

#[inline]
fn is_identity(c: &Coefficients) -> bool {
    c[2] == 1.0 && c[3] == 0.0 && c[4] == 0.0
}

/// One sample through the section with coefficients `c` and `a`.
#[inline]
fn tick(s: &mut [f32; 2], c: &Coefficients, a: &[f32; 3], x: f32) -> f32 {
    let v3 = x - s[1];
    let v1 = a[0] * s[0] + a[1] * v3;
    let v2 = s[1] + a[1] * s[0] + a[2] * v3;
    s[0] = 2.0 * v1 - s[0];
    s[1] = 2.0 * v2 - s[1];
    c[2] * x + c[3] * v1 + c[4] * v2
}

impl<const L: usize> Svf<L> {
    /// A section passing its input unchanged.
    pub(crate) fn new() -> Self {
        Self {
            c0: neutral(),
            step: [0.0; 5],
            target: neutral(),
            len: 0,
            pos: 0,
            a: a_of(&neutral()),
            state: [[0.0; 2]; L],
        }
    }

    /// The coefficients at ramp position `p`.
    #[inline]
    fn at(&self, p: u32) -> Coefficients {
        if p >= self.len {
            return self.target;
        }
        let mut c = [0.0; 5];
        for (i, v) in c.iter_mut().enumerate() {
            *v = self.c0[i] + p as f32 * self.step[i];
        }
        c
    }

    fn is_ramping(&self) -> bool {
        self.pos < self.len
    }

    /// Whether the section passes its input unchanged and is not
    /// ramping: its legs are not processed and its state is zero.
    pub(crate) fn is_bypassed(&self) -> bool {
        !self.is_ramping() && is_identity(&self.target)
    }

    /// The coefficients the section ramps to, or holds.
    #[cfg(test)]
    pub(crate) fn target(&self) -> Coefficients {
        self.target
    }

    /// Ramps from the current coefficients to those of `shape` over `len`
    /// samples, or to its `g` and `k` with the input passed unchanged
    /// when not `on`; 0 jumps. A section switched off keeps the `g` and
    /// `k` of its shape, so switching it on or off ramps only the mix.
    /// Equal targets on a section that is not ramping change nothing; a
    /// section off and staying off jumps, as nothing of it is heard.
    pub(crate) fn set(&mut self, shape: Shape, on: bool, len: u32) {
        let mut target = coefficients(shape);
        if !on {
            target[2..].copy_from_slice(&[1.0, 0.0, 0.0]);
        }
        if !self.is_ramping() && target == self.target {
            return;
        }
        let c0 = self.at(self.pos);
        let len = if is_identity(&target) && is_identity(&c0) {
            0
        } else {
            len.min(crate::MAX_RAMP_LEN)
        };
        if len == 0 {
            self.len = 0;
            self.c0 = target;
        } else {
            for i in 0..5 {
                self.step[i] = (target[i] - c0[i]) / len as f32;
            }
            self.len = len;
            self.c0 = c0;
        }
        self.pos = 0;
        self.target = target;
        self.a = a_of(&target);
    }

    /// Moves the coefficient ramp `frames` samples on without processing.
    pub(crate) fn advance(&mut self, frames: usize) {
        let frames = u32::try_from(frames).unwrap_or(u32::MAX);
        self.pos = self.pos.saturating_add(frames).min(self.len);
        if self.is_bypassed() {
            self.state = [[0.0; 2]; L];
        }
    }

    /// Filters `frames` samples of each of the first `legs` legs of the
    /// planar `buf` in place and moves the ramp on.
    pub(crate) fn process(&mut self, buf: &mut [f32], legs: usize, frames: usize) {
        if self.is_bypassed() {
            return;
        }
        let ramp = ((self.len - self.pos) as usize).min(frames);
        for (leg, x) in buf.chunks_exact_mut(frames).take(legs).enumerate() {
            let mut s = self.state[leg];
            let (ramped, held) = x.split_at_mut(ramp);
            for (n, v) in ramped.iter_mut().enumerate() {
                let c = self.at(self.pos + n as u32);
                *v = tick(&mut s, &c, &a_of(&c), *v);
            }
            for v in held {
                *v = tick(&mut s, &self.target, &self.a, *v);
            }
            // A state below FLUSH is set to zero: the trapezoidal
            // integrators can stop on a small value once their increment
            // underflows, under FTZ/DAZ or rounding, and silence must
            // reach exact zeros.
            for v in &mut s {
                if v.abs() < FLUSH {
                    *v = 0.0;
                }
            }
            self.state[leg] = s;
        }
        self.advance(frames);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use core::f64::consts::PI;

    /// `(b0, b1, b2, a0, a1, a2)` of the RBJ Audio EQ Cookbook, computed
    /// from its formulas in f64: the reference the sections are checked
    /// against.
    pub(crate) fn cookbook(shape: Shape) -> [f64; 6] {
        let (freq, gain_db, q) = match shape {
            Shape::HighPass { freq } => (freq, 0.0, core::f32::consts::FRAC_1_SQRT_2),
            Shape::Bell { freq, gain_db, q }
            | Shape::LowShelf { freq, gain_db, q }
            | Shape::HighShelf { freq, gain_db, q } => (freq, gain_db, q),
        };
        let w0 = 2.0 * PI * f64::from(freq) / SAMPLE_RATE;
        let (cw, sw) = (w0.cos(), w0.sin());
        let alpha = sw / (2.0 * f64::from(q));
        let a = 10f64.powf(f64::from(gain_db) / 40.0);
        let sa = 2.0 * a.sqrt() * alpha;
        match shape {
            Shape::HighPass { .. } => [
                (1.0 + cw) / 2.0,
                -(1.0 + cw),
                (1.0 + cw) / 2.0,
                1.0 + alpha,
                -2.0 * cw,
                1.0 - alpha,
            ],
            Shape::Bell { .. } => [
                1.0 + alpha * a,
                -2.0 * cw,
                1.0 - alpha * a,
                1.0 + alpha / a,
                -2.0 * cw,
                1.0 - alpha / a,
            ],
            Shape::LowShelf { .. } => [
                a * ((a + 1.0) - (a - 1.0) * cw + sa),
                2.0 * a * ((a - 1.0) - (a + 1.0) * cw),
                a * ((a + 1.0) - (a - 1.0) * cw - sa),
                (a + 1.0) + (a - 1.0) * cw + sa,
                -2.0 * ((a - 1.0) + (a + 1.0) * cw),
                (a + 1.0) + (a - 1.0) * cw - sa,
            ],
            Shape::HighShelf { .. } => [
                a * ((a + 1.0) + (a - 1.0) * cw + sa),
                -2.0 * a * ((a - 1.0) + (a + 1.0) * cw),
                a * ((a + 1.0) + (a - 1.0) * cw - sa),
                (a + 1.0) - (a - 1.0) * cw + sa,
                2.0 * ((a - 1.0) - (a + 1.0) * cw),
                (a + 1.0) - (a - 1.0) * cw - sa,
            ],
        }
    }

    /// `(magnitude in dB, phase in degrees)` of the cookbook biquad at
    /// `freq`.
    pub(crate) fn reference(shape: Shape, freq: f64) -> (f64, f64) {
        let [b0, b1, b2, a0, a1, a2] = cookbook(shape);
        let w = 2.0 * PI * freq / SAMPLE_RATE;
        // z^-1 = cos w - j sin w.
        let (c1, s1, c2, s2) = (w.cos(), -w.sin(), (2.0 * w).cos(), -(2.0 * w).sin());
        let (nr, ni) = (b0 + b1 * c1 + b2 * c2, b1 * s1 + b2 * s2);
        let (dr, di) = (a0 + a1 * c1 + a2 * c2, a1 * s1 + a2 * s2);
        let mag = ((nr * nr + ni * ni) / (dr * dr + di * di)).sqrt();
        let phase = ni.atan2(nr) - di.atan2(dr);
        let phase = (phase + PI).rem_euclid(2.0 * PI) - PI;
        (20.0 * mag.log10(), phase.to_degrees())
    }

    /// `(magnitude in dB, phase in degrees)` of `process` measured on a
    /// unit sine at `freq`, a whole number of Hz: one second to settle,
    /// then one second demodulated, a whole number of periods.
    pub(crate) fn measure(freq: f64, mut process: impl FnMut(&mut [f32])) -> (f64, f64) {
        let n = SAMPLE_RATE as usize;
        let w = 2.0 * PI * freq / SAMPLE_RATE;
        let mut x: Vec<f32> = (0..2 * n).map(|i| (w * i as f64).sin() as f32).collect();
        for block in x.chunks_mut(48) {
            process(block);
        }
        let (mut i_sum, mut q_sum) = (0.0f64, 0.0f64);
        for (i, &y) in x[n..].iter().enumerate() {
            let t = w * (n + i) as f64;
            i_sum += f64::from(y) * t.sin();
            q_sum += f64::from(y) * t.cos();
        }
        let (re, im) = (2.0 * i_sum / n as f64, 2.0 * q_sum / n as f64);
        (
            10.0 * (re * re + im * im).log10(),
            im.atan2(re).to_degrees(),
        )
    }

    fn svf_response(shape: Shape, freq: f64) -> (f64, f64) {
        let mut f = Svf::<1>::new();
        f.set(shape, true, 0);
        measure(freq, |b| f.process(b, 1, b.len()))
    }

    fn shapes() -> Vec<Shape> {
        let mut v = vec![
            Shape::HighPass { freq: 20.0 },
            Shape::HighPass { freq: 80.0 },
            Shape::HighPass { freq: 500.0 },
        ];
        for &(freq, gain_db, q) in &[
            (20.0, 18.0, 0.1),
            (100.0, -12.0, 0.707),
            (1000.0, 6.0, 1.0),
            (1000.0, -18.0, 16.0),
            (5000.0, 18.0, 4.0),
            (20000.0, -6.0, 0.5),
        ] {
            v.push(Shape::Bell { freq, gain_db, q });
            v.push(Shape::LowShelf { freq, gain_db, q });
            v.push(Shape::HighShelf { freq, gain_db, q });
        }
        v
    }

    /// Every response matches the cookbook biquad in magnitude and phase
    /// from 20 Hz to 20 kHz.
    #[test]
    fn responses_match_the_cookbook() {
        let freqs = [
            20.0, 50.0, 100.0, 250.0, 997.0, 1000.0, 4000.0, 12000.0, 20000.0,
        ];
        for shape in shapes() {
            for &f in &freqs {
                let (want_db, want_deg) = reference(shape, f);
                let (db, deg) = svf_response(shape, f);
                // Far down a high-pass skirt the level is small, and its
                // phase is measured on a small signal.
                let (tol_db, tol_deg) = if want_db < -40.0 {
                    (0.05, 0.5)
                } else {
                    (0.002, 0.02)
                };
                assert!(
                    (db - want_db).abs() < tol_db,
                    "{shape:?} at {f} Hz: {db} dB, cookbook {want_db} dB"
                );
                assert!(
                    (deg - want_deg).abs() < tol_deg,
                    "{shape:?} at {f} Hz: {deg} deg, cookbook {want_deg} deg"
                );
            }
        }
    }

    /// Fixed points of the shapes, independent of the cookbook: the
    /// Butterworth high-pass is 3.01 dB down at its frequency, a bell has
    /// its gain at its centre, a shelf half its gain at its frequency and
    /// all of it far into the shelf.
    #[test]
    fn responses_hit_their_defining_points() {
        let half_power = -10.0 * 2f64.log10();
        let (db, deg) = svf_response(Shape::HighPass { freq: 100.0 }, 100.0);
        assert!((db - half_power).abs() < 0.002, "{db}");
        assert!((deg - 90.0).abs() < 0.5, "{deg}");
        let (db, _) = svf_response(Shape::HighPass { freq: 100.0 }, 50.0);
        assert!((db + 12.3).abs() < 0.1, "one octave below: {db}");
        let (db, deg) = svf_response(
            Shape::Bell {
                freq: 1000.0,
                gain_db: 9.0,
                q: 2.0,
            },
            1000.0,
        );
        assert!((db - 9.0).abs() < 0.002 && deg.abs() < 0.02, "{db} {deg}");
        for (shape, inside) in [
            (
                Shape::LowShelf {
                    freq: 200.0,
                    gain_db: -12.0,
                    q: 0.707,
                },
                20.0,
            ),
            (
                Shape::HighShelf {
                    freq: 2000.0,
                    gain_db: -12.0,
                    q: 0.707,
                },
                20000.0,
            ),
        ] {
            let f = match shape {
                Shape::LowShelf { freq, .. } | Shape::HighShelf { freq, .. } => f64::from(freq),
                _ => unreachable!(),
            };
            let (db, _) = svf_response(shape, f);
            assert!((db + 6.0).abs() < 0.002, "{shape:?} at its frequency: {db}");
            let (db, _) = svf_response(shape, inside);
            assert!((db + 12.0).abs() < 0.1, "{shape:?} inside the shelf: {db}");
        }
    }

    /// A bypassed section passes every bit, -0.0 and NaN payloads
    /// included, and so does one ramped back to identity once settled.
    #[test]
    fn a_bypassed_section_is_bit_transparent() {
        let x = [
            1.5f32,
            -0.0,
            f32::from_bits(1),
            f32::from_bits(0x7fc0_1234),
            -3.0,
        ];
        let mut f = Svf::<1>::new();
        let mut y = x;
        f.process(&mut y, 1, x.len());
        assert_eq!(x.map(f32::to_bits), y.map(f32::to_bits));
        f.set(
            Shape::Bell {
                freq: 1000.0,
                gain_db: 6.0,
                q: 1.0,
            },
            true,
            0,
        );
        let mut noise = [0.25f32; 64];
        f.process(&mut noise, 1, 64);
        f.set(
            Shape::Bell {
                freq: 1000.0,
                gain_db: 6.0,
                q: 1.0,
            },
            false,
            32,
        );
        assert!(!f.is_bypassed());
        f.process(&mut noise, 1, 64);
        assert!(f.is_bypassed());
        let mut y = x;
        f.process(&mut y, 1, x.len());
        assert_eq!(x.map(f32::to_bits), y.map(f32::to_bits));
    }

    /// After a ramp the coefficients are the target's exactly, and each
    /// ramped sample's coefficients follow the direct formula, so a
    /// section split into blocks of any sizes gives the same bits.
    #[test]
    fn a_ramp_lands_on_the_target_and_ignores_block_sizes() {
        let a = Shape::Bell {
            freq: 300.0,
            gain_db: -18.0,
            q: 8.0,
        };
        let b = Shape::HighShelf {
            freq: 9000.0,
            gain_db: 12.0,
            q: 0.3,
        };
        let input: Vec<f32> = (0..2000)
            .map(|n| ((n * 7919) % 211) as f32 / 211.0 - 0.5)
            .collect();
        let run = |sizes: &[usize]| {
            let mut f = Svf::<2>::new();
            f.set(a, true, 0);
            let mut out = Vec::new();
            let mut at = 0;
            let mut sizes = sizes.iter().cycle();
            while at < input.len() {
                let len = (*sizes.next().unwrap()).min(input.len() - at);
                if at == 96 {
                    f.set(b, true, 480);
                }
                let mut buf: Vec<f32> = input[at..at + len]
                    .iter()
                    .chain(&input[at..at + len])
                    .copied()
                    .collect();
                f.process(&mut buf, 2, len);
                assert_eq!(buf[..len], buf[len..], "legs differ");
                out.extend_from_slice(&buf[..len]);
                at += len;
            }
            (out, f)
        };
        let (whole, f) = run(&[48]);
        assert_eq!(f.target(), coefficients(b));
        assert!(!f.is_ramping());
        for sizes in [&[16usize][..], &[48, 1, 47], &[96]] {
            let (other, _) = run(sizes);
            let same = whole
                .iter()
                .zip(&other)
                .all(|(x, y)| x.to_bits() == y.to_bits());
            assert!(same, "blocks of {sizes:?}");
        }
    }

    /// Silence after a signal decays to exact zeros under FTZ/DAZ, in
    /// every shape.
    #[test]
    fn decays_to_exact_zero() {
        let saved = crate::set_ftz_daz(true);
        for shape in shapes() {
            let mut f = Svf::<1>::new();
            f.set(shape, true, 0);
            let mut x = [1.0f32; 480];
            f.process(&mut x, 1, 480);

            for _ in 0..200_000 {
                let mut silence = [0.0f32; 48];
                f.process(&mut silence, 1, 48);
                if f.state == [[0.0; 2]] {
                    break;
                }
            }
            assert_eq!(f.state, [[0.0; 2]], "{shape:?}");
            let mut silence = [0.0f32; 48];
            f.process(&mut silence, 1, 48);
            assert!(silence.iter().all(|v| v.to_bits() == 0), "{shape:?}");
        }
        crate::restore_mxcsr(saved);
    }
}
