/// Longest ramp in samples. Ramp positions stay exactly representable as
/// f32 and as i32 in the SIMD kernels.
pub const MAX_RAMP_LEN: u32 = 1 << 24;

/// Longest ramp whose every value lies between its ends. A longer one can
/// pass its target by one ulp at its last samples, as the rounded step
/// times a position above 2^23 does, so a check holding a ramp to the
/// range of its ends would refuse a state it reached.
pub const MAX_EXACT_RAMP_LEN: u32 = 1 << 23;

/// Largest gain magnitude a ramp accepts, +120 dB. With both ends of a
/// ramp within it, `target - c0` and every ramp value stay finite.
pub const MAX_GAIN: f32 = 1.0e6;

/// Whether `gain` is finite and within [`MAX_GAIN`].
pub fn valid_gain(gain: f32) -> bool {
    gain.abs() <= MAX_GAIN
}

/// A linear ramp in the gain domain.
///
/// Position `n` of a ramp of length `len` from `c0` that starts after
/// `delay` positions has the value `c0` below `delay`, and from `delay`
/// on `c0 + (n - delay) * step` with `step = (target - c0) / len`,
/// computed from `n` directly rather than by accumulation. From position
/// `delay + len` on the value is `target` exactly. The ramp is a pure
/// function of `(c0, target, delay, len, n)`.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Ramp {
    pub(crate) c0: f32,
    pub(crate) step: f32,
    pub(crate) target: f32,
    pub(crate) len: u32,
    /// Positions the ramp holds `c0` before it starts.
    pub(crate) delay: u32,
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
            delay: 0,
            pos: 0,
        }
    }

    /// Position the ramp ends at: `delay + len`.
    #[inline]
    pub fn end(&self) -> u32 {
        self.delay + self.len
    }

    /// Ramp position of the next sample produced.
    pub fn position(&self) -> u32 {
        self.pos
    }

    /// Value at ramp position `k`.
    #[inline]
    pub(crate) fn value_at(&self, k: u32) -> f32 {
        match k.checked_sub(self.delay) {
            None => self.c0,
            Some(j) if j < self.len => self.c0 + j as f32 * self.step,
            Some(_) => self.target,
        }
    }

    /// Value of the next sample produced.
    pub fn current(&self) -> f32 {
        self.value_at(self.pos)
    }

    pub fn target(&self) -> f32 {
        self.target
    }

    /// Whether the ramp holds what [`Ramp::new`] and [`Ramp::set_target`]
    /// give: both ends valid gains, a finite step, a length within
    /// [`MAX_RAMP_LEN`] and a position within the length. A ramp decoded
    /// from a peer is checked with it.
    pub fn is_valid(&self) -> bool {
        valid_gain(self.c0)
            && valid_gain(self.target)
            && self.step.is_finite()
            && self.len <= MAX_RAMP_LEN
            && self.delay <= MAX_EXACT_RAMP_LEN
            && self.pos <= self.end()
    }

    /// Whether the ramp is valid and both its ends lie within `range`.
    pub(crate) fn is_within(&self, range: core::ops::RangeInclusive<f32>) -> bool {
        self.is_valid() && range.contains(&self.c0) && range.contains(&self.target)
    }

    pub fn is_active(&self) -> bool {
        self.pos < self.end()
    }

    /// Ramps from the current value to `target` over `len` samples, clamped
    /// to [`MAX_RAMP_LEN`]. A length of 0 jumps to `target`. A target equal
    /// to the held value of an inactive ramp leaves the ramp unchanged.
    ///
    /// Returns false and leaves the ramp unchanged if `target` is not
    /// finite or exceeds [`MAX_GAIN`] in magnitude.
    #[must_use]
    pub fn set_target(&mut self, target: f32, len: u32) -> bool {
        self.set_fade(target, 0, len)
    }

    /// [`Ramp::set_target`] after the ramp held its current value for
    /// `delay` samples, clamped to [`MAX_EXACT_RAMP_LEN`]. A ramp that was
    /// itself waiting or running is replaced: the new one starts from the
    /// value it has now.
    #[must_use]
    pub fn set_fade(&mut self, target: f32, delay: u32, len: u32) -> bool {
        if !valid_gain(target) {
            return false;
        }
        let len = len.min(MAX_RAMP_LEN);
        let delay = delay.min(MAX_EXACT_RAMP_LEN);
        if (len == 0 && delay == 0) || (!self.is_active() && target == self.target) {
            *self = Self::hold(target);
            return true;
        }
        let c0 = self.current();
        *self = Self {
            c0,
            step: if len == 0 {
                0.0
            } else {
                (target - c0) / len as f32
            },
            target,
            len,
            delay,
            pos: 0,
        };
        true
    }

    /// The ramp from `c0` to `target` that waits `delay` samples, then
    /// takes `len`, at position `pos` of its `delay + len`: what
    /// [`Ramp::set_fade`] gives from `c0`, moved `pos` samples on. Its
    /// value at a position is the same whichever way the ramp got there.
    /// `delay`, `len` and `pos` are clamped as the ramp allows; `None`
    /// when `c0` or `target` is not a valid gain.
    pub fn resume(c0: f32, target: f32, delay: u32, len: u32, pos: u32) -> Option<Ramp> {
        if !valid_gain(c0) || !valid_gain(target) {
            return None;
        }
        let mut r = Ramp::new(c0);
        let _ = r.set_fade(target, delay, len);
        r.advance(pos as usize);
        Some(r)
    }

    /// Moves the ramp `frames` samples on.
    pub fn advance(&mut self, frames: usize) {
        let frames = u32::try_from(frames).unwrap_or(u32::MAX);
        self.pos = self.pos.saturating_add(frames).min(self.end());
    }

    /// Multiplies `buf` by the ramp sample by sample and advances it.
    pub fn process(&mut self, buf: &mut [f32]) {
        let remaining = (self.end() - self.pos) as usize;
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

    /// The last values of a ramp, whether they lie between its ends.
    fn ends_hold(c0: f32, target: f32, len: u32) -> bool {
        let mut r = Ramp::new(c0);
        assert!(r.set_target(target, len));
        let (lo, hi) = (c0.min(target), c0.max(target));
        (len.saturating_sub(3)..len).all(|k| (lo..=hi).contains(&r.value_at(k)))
    }

    /// A ramp of up to [`MAX_EXACT_RAMP_LEN`] samples stays between its
    /// ends, at that length and below, across ends within the leg gain
    /// range; a longer one can pass its target, -80.378 dB to 100 over
    /// 13407885 samples ending at 100.0000076.
    #[test]
    fn a_ramp_up_to_the_exact_length_stays_between_its_ends() {
        assert!(!ends_hold(-80.378_47, 100.0, 13_407_885));
        let mut rng = crate::test_util::Rng::new(0x2a3e_77c1);
        for i in 0..20_000 {
            let len = match i % 4 {
                0 => MAX_EXACT_RAMP_LEN,
                1 => MAX_EXACT_RAMP_LEN - rng.below(1000) as u32,
                _ => 2 + rng.below(MAX_EXACT_RAMP_LEN as usize - 1) as u32,
            };
            let c0 = rng.unit() * 100.0;
            let target = match rng.below(4) {
                0 => 100.0,
                1 => -100.0,
                2 => 1.0,
                _ => rng.unit() * 100.0,
            };
            assert!(ends_hold(c0, target, len), "{c0} -> {target} over {len}");
        }
    }

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

    /// A delayed ramp holds its start value for the delay, then follows
    /// the formula from the end of it, and is the same whichever block
    /// sizes it is processed in.
    #[test]
    fn a_delayed_ramp_holds_then_ramps_exactly() {
        let (delay, len) = (100u32, 300u32);
        let mut r = Ramp::new(1.0);
        assert!(r.set_fade(0.25, delay, len));
        let step = (0.25f32 - 1.0) / len as f32;
        for k in 0..(delay + len + 20) {
            let want = if k < delay {
                1.0
            } else if k < delay + len {
                1.0 + (k - delay) as f32 * step
            } else {
                0.25
            };
            assert_eq!(r.value_at(k).to_bits(), want.to_bits(), "position {k}");
        }
        let whole = run(&mut r.clone(), &[delay as usize + len as usize + 20]);
        for blocks in [vec![1; 420], vec![7; 60], vec![64, 1, 200, 155]] {
            let got = run(&mut r.clone(), &blocks);
            assert_eq!(
                got.iter().map(|g| g.to_bits()).collect::<Vec<_>>(),
                whole[..got.len()]
                    .iter()
                    .map(|g| g.to_bits())
                    .collect::<Vec<_>>(),
            );
        }
        assert!(r.is_active());
        r.advance(delay as usize + len as usize);
        assert!(!r.is_active());
    }

    /// A change while an earlier fade still waits starts from the value
    /// held, so the earlier one never takes effect.
    #[test]
    fn a_change_replaces_a_fade_still_waiting() {
        let mut r = Ramp::new(1.0);
        assert!(r.set_fade(0.0, 1000, 10));
        r.advance(400);
        assert!(r.set_target(0.5, 100));
        assert_eq!(r.current(), 1.0);
        r.advance(100);
        assert_eq!(r.current(), 0.5);
        assert!(!r.is_active());
    }

    /// A ramp resumed at a position gives, from there on, the bits the
    /// ramp that ran to that position gives.
    #[test]
    fn a_resumed_ramp_continues_bit_for_bit() {
        let mut full = Ramp::new(0.3);
        assert!(full.set_fade(-1.7, 90, 777));
        for pos in [0u32, 50, 89, 90, 91, 400, 866, 867] {
            let mut cont =
                Ramp::resume(0.3, -1.7, 90 - pos.min(90), 777, pos.saturating_sub(90)).unwrap();
            let mut ran = full;
            ran.advance(pos as usize);
            for k in 0..40 {
                assert_eq!(
                    cont.current().to_bits(),
                    ran.current().to_bits(),
                    "position {pos} + {k}"
                );
                cont.advance(1);
                ran.advance(1);
            }
        }
    }
}
