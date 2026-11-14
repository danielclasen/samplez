/// Longest ramp in samples. Ramp positions stay exactly representable as
/// f32 and as i32 in the SIMD kernels.
pub const MAX_RAMP_LEN: u32 = 1 << 24;

/// Largest gain magnitude a ramp accepts, +120 dB. With both ends of a
/// ramp within it, `target - c0` and every ramp value stay finite.
pub const MAX_GAIN: f32 = 1.0e6;

/// Whether `gain` is finite and within [`MAX_GAIN`].
pub fn valid_gain(gain: f32) -> bool {
    gain.abs() <= MAX_GAIN
}

/// A linear ramp in the gain domain.
///
/// Sample `n` of a ramp of length `len` from `c0` has the value
/// `c0 + n * step` with `step = (target - c0) / len`, computed from `n`
/// directly rather than by accumulation. From sample `len` on the value is
/// `target` exactly.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ramp {
    pub(crate) c0: f32,
    pub(crate) step: f32,
    pub(crate) target: f32,
    pub(crate) len: u32,
    /// Ramp position of the next sample produced.
    pub(crate) pos: u32,
}

impl Ramp {
    /// A ramp holding `value`.
    ///
    /// # Panics
    ///
    /// If `value` is not finite or exceeds [`MAX_GAIN`] in magnitude.
    pub fn new(value: f32) -> Self {
        assert!(
            valid_gain(value),
            "gain {value} is not finite within MAX_GAIN"
        );
        Self::hold(value)
    }

    fn hold(value: f32) -> Self {
        Self {
            c0: value,
            step: 0.0,
            target: value,
            len: 0,
            pos: 0,
        }
    }

    /// Value at ramp position `k`.
    #[inline]
    pub(crate) fn value_at(&self, k: u32) -> f32 {
        if k < self.len {
            self.c0 + k as f32 * self.step
        } else {
            self.target
        }
    }

    /// Value of the next sample produced.
    pub fn current(&self) -> f32 {
        self.value_at(self.pos)
    }

    pub fn target(&self) -> f32 {
        self.target
    }

    pub fn is_active(&self) -> bool {
        self.pos < self.len
    }

    /// Ramps from the current value to `target` over `len` samples, clamped
    /// to [`MAX_RAMP_LEN`]. A length of 0 jumps to `target`. A target equal
    /// to the held value of an inactive ramp leaves the ramp unchanged.
    ///
    /// Returns false and leaves the ramp unchanged if `target` is not
    /// finite or exceeds [`MAX_GAIN`] in magnitude.
    #[must_use]
    pub fn set_target(&mut self, target: f32, len: u32) -> bool {
        if !valid_gain(target) {
            return false;
        }
        let len = len.min(MAX_RAMP_LEN);
        if len == 0 || (!self.is_active() && target == self.target) {
            *self = Self::hold(target);
            return true;
        }
        let c0 = self.current();
        *self = Self {
            c0,
            step: (target - c0) / len as f32,
            target,
            len,
            pos: 0,
        };
        true
    }

    /// Moves the ramp `frames` samples on.
    pub fn advance(&mut self, frames: usize) {
        let frames = u32::try_from(frames).unwrap_or(u32::MAX);
        self.pos = self.pos.saturating_add(frames).min(self.len);
    }

    /// Multiplies `buf` by the ramp sample by sample and advances it.
    pub fn process(&mut self, buf: &mut [f32]) {
        let remaining = (self.len - self.pos) as usize;
        let (ramp, held) = buf.split_at_mut(remaining.min(buf.len()));
        for (n, s) in ramp.iter_mut().enumerate() {
            *s *= self.value_at(self.pos + n as u32);
        }
        for s in held {
            *s *= self.target;
        }
        self.advance(buf.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_gain_refuses_nan_and_infinities() {
        assert!(valid_gain(MAX_GAIN) && valid_gain(-MAX_GAIN) && valid_gain(0.0));
        assert!(!valid_gain(f32::NAN));
        assert!(!valid_gain(f32::INFINITY) && !valid_gain(f32::NEG_INFINITY));
        assert!(!valid_gain(MAX_GAIN * 1.01));
    }

    /// Runs `ramp` over a block of ones in blocks of the given sizes.
    fn run(ramp: &mut Ramp, blocks: &[usize]) -> Vec<f32> {
        let mut out = Vec::new();
        for &b in blocks {
            let mut buf = vec![1.0f32; b];
            ramp.process(&mut buf);
            out.extend_from_slice(&buf);
        }
        out
    }

    #[test]
    fn samples_follow_the_direct_formula() {
        let (c0, target, len) = (0.1f32, 0.7f32, 480u32);
        let mut r = Ramp::new(c0);
        assert!(r.set_target(target, len));
        let out = run(&mut r, &[7, 64, 1, 300, 128, 100]);
        let step = (target - c0) / len as f32;
        for (n, &v) in out.iter().enumerate() {
            let want = if (n as u32) < len {
                c0 + n as f32 * step
            } else {
                target
            };
            assert_eq!(v.to_bits(), want.to_bits(), "sample {n}");
        }
        // Accumulating step sample by sample drifts away from the direct
        // formula, so the comparison above separates the two.
        let mut acc = c0;
        let drifted = (0..len).any(|n| {
            let direct = c0 + n as f32 * step;
            let differs = acc.to_bits() != direct.to_bits();
            acc += step;
            differs
        });
        assert!(drifted);
    }

    #[test]
    fn end_value_is_the_target_exactly() {
        let (c0, target) = (0.1f32, 0.7f32);
        // A length where the formula itself does not land on the target.
        let len = (1..1000u32)
            .find(|&l| {
                let step = (target - c0) / l as f32;
                (c0 + l as f32 * step).to_bits() != target.to_bits()
            })
            .expect("a length whose formula misses the target");
        let mut r = Ramp::new(c0);
        assert!(r.set_target(target, len));
        let out = run(&mut r, &[len as usize + 3]);
        assert!(out[len as usize - 1] != target);
        for &v in &out[len as usize..] {
            assert_eq!(v.to_bits(), target.to_bits());
        }
        assert!(!r.is_active());
        assert_eq!(r.current().to_bits(), target.to_bits());
    }

    #[test]
    fn retarget_starts_from_the_current_value() {
        let mut r = Ramp::new(1.0);
        assert!(r.set_target(0.0, 1000));
        let first = run(&mut r, &[250]);
        let step1 = -1.0f32 / 1000.0;
        let c1 = 1.0 + 250.0 * step1;
        assert_eq!(r.current().to_bits(), c1.to_bits());

        assert!(r.set_target(0.5, 100));
        let second = run(&mut r, &[40, 80]);
        let step2 = (0.5 - c1) / 100.0;
        for (n, &v) in first.iter().enumerate() {
            assert_eq!(v.to_bits(), (1.0 + n as f32 * step1).to_bits());
        }
        for (n, &v) in second.iter().enumerate() {
            let want = if n < 100 { c1 + n as f32 * step2 } else { 0.5 };
            assert_eq!(v.to_bits(), want.to_bits(), "sample {n}");
        }
    }

    #[test]
    fn zero_length_jumps() {
        let mut r = Ramp::new(0.25);
        assert!(r.set_target(0.75, 0));
        assert!(!r.is_active());
        assert_eq!(run(&mut r, &[3]), vec![0.75; 3]);
    }

    #[test]
    fn same_target_on_a_held_ramp_does_not_start_a_ramp() {
        let mut r = Ramp::new(0.5);
        assert!(r.set_target(0.5, 480));
        assert!(!r.is_active());
    }

    #[test]
    fn length_is_clamped() {
        let mut r = Ramp::new(0.0);
        assert!(r.set_target(1.0, u32::MAX));
        assert_eq!(r.len, MAX_RAMP_LEN);
    }

    #[test]
    fn invalid_targets_are_rejected() {
        let mut r = Ramp::new(0.5);
        assert!(r.set_target(0.0, 1000));
        r.advance(10);
        let before = r;
        for bad in [
            f32::NAN,
            f32::INFINITY,
            -f32::INFINITY,
            f32::MAX,
            -MAX_GAIN * 1.5,
        ] {
            assert!(!r.set_target(bad, 100), "{bad}");
            assert_eq!(r, before);
        }
    }

    #[test]
    fn extreme_ramps_stay_finite() {
        for len in [1, 2, 3, 1000] {
            let mut r = Ramp::new(-MAX_GAIN);
            assert!(r.set_target(MAX_GAIN, len));
            let out = run(&mut r, &[len as usize + 1]);
            assert!(out.iter().all(|v| v.is_finite()), "len {len}");
            assert_eq!(out[0], -MAX_GAIN);
            assert_eq!(out[len as usize], MAX_GAIN);
        }
    }

    #[test]
    #[should_panic(expected = "not finite")]
    fn new_rejects_non_finite() {
        Ramp::new(f32::NAN);
    }
}
