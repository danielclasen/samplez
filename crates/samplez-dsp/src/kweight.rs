//! K-weighting per ITU-R BS.1770-5 Annex 1 at 48 kHz: the stage 1
//! pre-filter of Table 1 followed by the RLB high-pass of Table 2, each a
//! biquad in transposed direct form II, in f64.
//!
//! The filter is recursive, so it runs one sample after the other on one
//! scalar path; every replica computes the same operations in the same
//! order, without FMA.

/// `(b0, b1, b2, a1, a2)` of the stage 1 pre-filter, BS.1770-5 Table 1,
/// for 48 kHz.
pub const STAGE1: [f64; 5] = [
    1.53512485958697,
    -2.69169618940638,
    1.19839281085285,
    -1.69065929318241,
    0.73248077421585,
];

/// `(b0, b1, b2, a1, a2)` of the RLB high-pass, BS.1770-5 Table 2, for
/// 48 kHz.
pub const STAGE2: [f64; 5] = [1.0, -2.0, 1.0, -1.99004745483398, 0.99007225036621];

#[derive(Clone, Copy, Debug, Default)]
struct Biquad {
    s1: f64,
    s2: f64,
}

impl Biquad {
    #[inline]
    fn step(&mut self, c: &[f64; 5], x: f64) -> f64 {
        let y = c[0] * x + self.s1;
        self.s1 = c[1] * x - c[3] * y + self.s2;
        self.s2 = c[2] * x - c[4] * y;
        y
    }
}

/// K-weighting filters for a fixed number of channels, with state carried
/// across calls.
#[derive(Clone, Debug)]
pub struct KWeighting {
    state: Vec<[Biquad; 2]>,
}

impl KWeighting {
    pub fn new(channels: usize) -> Self {
        Self {
            state: vec![[Biquad::default(); 2]; channels],
        }
    }

    pub fn channels(&self) -> usize {
        self.state.len()
    }

    /// Filters `x` on channel `ch` and returns the sum of squares of the
    /// filtered samples, accumulated in f64 in sample order.
    pub fn process(&mut self, ch: usize, x: &[f32]) -> f64 {
        let [s1, s2] = &mut self.state[ch];
        let mut sum = 0.0f64;
        for &v in x {
            let y = s2.step(&STAGE2, s1.step(&STAGE1, f64::from(v)));
            sum += y * y;
        }
        sum
    }

    /// Clears the filter state of every channel.
    pub fn reset(&mut self) {
        self.state.fill([Biquad::default(); 2]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mean square of a sine of amplitude 1 after the filter, over whole
    /// seconds after one second of settling.
    fn gain(freq: f64) -> f64 {
        let mut k = KWeighting::new(1);
        let x: Vec<f32> = (0..96_000)
            .map(|n| (2.0 * std::f64::consts::PI * freq * n as f64 / 48_000.0).sin() as f32)
            .collect();
        k.process(0, &x[..48_000]);
        k.process(0, &x[48_000..]) / 48_000.0 / 0.5
    }

    /// The shape of BS.1770-5 Figs 2 and 4: about +4 dB at high
    /// frequencies from the shelf, the high-pass far down at 20 Hz, and
    /// the +0.691 dB at 997 Hz that the constant in equation (2) cancels.
    #[test]
    fn response_matches_the_recommendation() {
        let db = |g: f64| 10.0 * g.log10();
        assert!(
            (db(gain(997.0)) - 0.691).abs() < 0.01,
            "{}",
            db(gain(997.0))
        );
        assert!(
            (db(gain(10_000.0)) - 4.0).abs() < 0.3,
            "{}",
            db(gain(10_000.0))
        );
        assert!(db(gain(20.0)) < -10.0, "{}", db(gain(20.0)));
    }

    /// The filter carries state across calls: one call and many give the
    /// same sum to a few ulps (the order of additions differs).
    #[test]
    fn state_carries_across_calls() {
        let x: Vec<f32> = (0..4800)
            .map(|n| ((n * 7919) % 101) as f32 / 101.0 - 0.5)
            .collect();
        let mut a = KWeighting::new(1);
        let whole = a.process(0, &x);
        let mut b = KWeighting::new(1);
        let parts: f64 = x.chunks(48).map(|c| b.process(0, c)).sum();
        assert!((whole - parts).abs() <= whole * 1e-12);
        b.reset();
        assert_eq!(b.process(0, &[0.0; 8]), 0.0);
    }

    /// Silence after a signal decays to exact zeros under FTZ/DAZ.
    #[test]
    fn decays_to_exact_zero() {
        let saved = crate::set_ftz_daz(true);
        let mut k = KWeighting::new(1);
        k.process(0, &[1.0; 480]);
        let silence = vec![0.0f32; 48_000];
        let mut last = 1.0;
        for _ in 0..60 {
            last = k.process(0, &silence);
        }
        crate::restore_mxcsr(saved);
        assert_eq!(last, 0.0);
    }
}
