//! True peak per ITU-R BS.1770-5 Annex 2: 4x oversampling with the
//! order-48, 4-phase interpolating FIR given there, then the absolute
//! value. The 12.04 dB attenuation of the Annex is left out: it provides
//! headroom for integer arithmetic, and the Annex says it is not needed in
//! floating point.
//!
//! Each phase output is `acc = +0.0`, then `acc = acc + h[j] * x[n - j]`
//! for `j` ascending, in f32 without FMA, on every path; the AVX2 path
//! computes 8 consecutive outputs of a phase per register with the same
//! operations, so it is bit-identical to the scalar one.

use crate::Path;

/// Taps per phase.
pub const TAPS: usize = 12;

/// The coefficients of ITU-R BS.1770-5 Annex 2, one row per phase, the
/// columns of the Annex's table. Each value is a multiple of 2^-13, exact
/// in f32; the literals keep the table's digits.
#[allow(
    clippy::excessive_precision,
    clippy::unreadable_literal,
    reason = "the digits as the Annex's table prints them"
)]
pub const COEFFICIENTS: [[f32; TAPS]; 4] = [
    [
        0.0017089843750,
        0.0109863281250,
        -0.0196533203125,
        0.0332031250000,
        -0.0594482421875,
        0.1373291015625,
        0.9721679687500,
        -0.1022949218750,
        0.0476074218750,
        -0.0266113281250,
        0.0148925781250,
        -0.0083007812500,
    ],
    [
        -0.0291748046875,
        0.0292968750000,
        -0.0517578125000,
        0.0891113281250,
        -0.1665039062500,
        0.4650878906250,
        0.7797851562500,
        -0.2003173828125,
        0.1015625000000,
        -0.0582275390625,
        0.0330810546875,
        -0.0189208984375,
    ],
    [
        -0.0189208984375,
        0.0330810546875,
        -0.0582275390625,
        0.1015625000000,
        -0.2003173828125,
        0.7797851562500,
        0.4650878906250,
        -0.1665039062500,
        0.0891113281250,
        -0.0517578125000,
        0.0292968750000,
        -0.0291748046875,
    ],
    [
        -0.0083007812500,
        0.0148925781250,
        -0.0266113281250,
        0.0476074218750,
        -0.1022949218750,
        0.9721679687500,
        0.1373291015625,
        -0.0594482421875,
        0.0332031250000,
        -0.0196533203125,
        0.0109863281250,
        0.0017089843750,
    ],
];

pub(crate) const HISTORY: usize = TAPS - 1;

/// True-peak detector for a fixed number of channels. Each channel keeps
/// the last 11 input samples across calls.
#[derive(Clone, Debug)]
pub struct TruePeak {
    /// Per channel the history followed by room for `capacity` samples.
    work: Vec<Vec<f32>>,
}

impl TruePeak {
    /// Input samples before the current one that each interpolated output
    /// depends on.
    pub const HISTORY: usize = HISTORY;

    /// A detector for `channels` channels taking up to `capacity` samples
    /// per call, with a silent history.
    pub fn new(channels: usize, capacity: usize) -> Self {
        Self {
            work: vec![vec![0.0; HISTORY + capacity]; channels],
        }
    }

    pub fn channels(&self) -> usize {
        self.work.len()
    }

    /// Clears the history of every channel.
    pub fn reset(&mut self) {
        for w in &mut self.work {
            w[..HISTORY].fill(0.0);
        }
    }

    /// The largest absolute value of the 4x oversampled signal over the
    /// 4 * `x.len()` interpolated samples that `x` completes on channel
    /// `ch`, on the best path the CPU supports. NaN when one of them is
    /// NaN.
    pub fn process(&mut self, ch: usize, x: &[f32]) -> f32 {
        self.process_on(Path::best(), ch, x)
    }

    /// [`TruePeak::process`] on `path`.
    ///
    /// # Panics
    ///
    /// When the CPU does not support `path`, or `x` is longer than the
    /// capacity.
    pub fn process_on(&mut self, path: Path, ch: usize, x: &[f32]) -> f32 {
        let w = &mut self.work[ch];
        let len = x.len();
        assert!(
            HISTORY + len <= w.len(),
            "true peak: more samples than capacity"
        );
        w[HISTORY..HISTORY + len].copy_from_slice(x);
        let (peak, nan) = match path {
            Path::Scalar => peak_scalar(&w[..HISTORY + len], 0),
            Path::Avx2 => {
                assert!(Path::Avx2.is_supported(), "AVX2 not supported");
                // SAFETY: AVX2 support was checked.
                unsafe { crate::simd::true_peak_avx2(&w[..HISTORY + len]) }
            }
        };
        w.copy_within(len..len + HISTORY, 0);
        if nan { f32::NAN } else { peak }
    }
}

/// The phase outputs at `w[HISTORY + n]` for every `n >= from`: the
/// largest absolute value and whether one was NaN.
pub(crate) fn peak_scalar(w: &[f32], from: usize) -> (f32, bool) {
    let mut peak = 0.0f32;
    let mut nan = false;
    for n in HISTORY + from..w.len() {
        for h in &COEFFICIENTS {
            let mut acc = 0.0f32;
            for (j, &c) in h.iter().enumerate() {
                acc += c * w[n - j];
            }
            let a = acc.abs();
            nan |= a.is_nan();
            if a > peak {
                peak = a;
            }
        }
    }
    (peak, nan)
}

/// Per input sample `w[HISTORY + n]`, `n` from `from` on, the largest
/// of `out[n]` and the absolute values of its four phase outputs, each
/// computed as [`peak_scalar`] computes it. The inputs must be finite.
pub(crate) fn phase_peaks_scalar(w: &[f32], from: usize, out: &mut [f32]) {
    for n in from..w.len() - HISTORY {
        let mut peak = out[n];
        for h in &COEFFICIENTS {
            let mut acc = 0.0f32;
            for (j, &c) in h.iter().enumerate() {
                acc += c * w[HISTORY + n - j];
            }
            let a = acc.abs();
            if a > peak {
                peak = a;
            }
        }
        out[n] = peak;
    }
}

/// [`phase_peaks_scalar`] on `path`, the same bits on every path.
///
/// # Panics
///
/// When the CPU does not support `path`, or `out` is shorter than the
/// samples after the history.
pub(crate) fn phase_peaks(path: Path, w: &[f32], out: &mut [f32]) {
    assert!(out.len() >= w.len() - HISTORY, "phase peaks: room");
    match path {
        Path::Scalar => phase_peaks_scalar(w, 0, out),
        Path::Avx2 => {
            assert!(Path::Avx2.is_supported(), "AVX2 not supported");
            // SAFETY: AVX2 support was checked.
            unsafe { crate::simd::phase_peaks_avx2(w, out) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::require::path_runs;
    use crate::test_util::Rng;
    use crate::{restore_mxcsr, set_ftz_daz};

    fn paths(test: &str) -> Vec<Path> {
        Path::ALL
            .into_iter()
            .filter(|&p| path_runs(test, p))
            .collect()
    }

    /// The coefficients are the Annex's table, which is symmetric: the
    /// 48 taps read in phase order equal themselves reversed. Each phase
    /// has a DC gain within 0.25 dB of unity (phases 1 and 2 sum to
    /// 0.973).
    #[test]
    fn coefficients_are_the_published_table() {
        let flat: Vec<f32> = (0..TAPS)
            .flat_map(|j| (0..4).map(move |p| COEFFICIENTS[p][j]))
            .collect();
        let mut rev = flat.clone();
        rev.reverse();
        assert_eq!(flat, rev);
        for h in &COEFFICIENTS {
            let dc: f64 = h.iter().map(|&c| f64::from(c)).sum();
            assert!((dc - 1.0).abs() < 0.03, "phase DC gain {dc}");
        }
        // The Annex 2 table column by column, each value in units of
        // 2^-13: every published digit, so a change to any coefficient,
        // or to a symmetric pair of them, fails.
        const TABLE: [[i32; TAPS]; 4] = [
            [
                14, 90, -161, 272, -487, 1125, 7964, -838, 390, -218, 122, -68,
            ],
            [
                -239, 240, -424, 730, -1364, 3810, 6388, -1641, 832, -477, 271, -155,
            ],
            [
                -155, 271, -477, 832, -1641, 6388, 3810, -1364, 730, -424, 240, -239,
            ],
            [
                -68, 122, -218, 390, -838, 7964, 1125, -487, 272, -161, 90, 14,
            ],
        ];
        for (h, t) in COEFFICIENTS.iter().zip(&TABLE) {
            let units: Vec<f32> = h.iter().map(|&c| c * 8192.0).collect();
            let table: Vec<f32> = t.iter().map(|&u| u as f32).collect();
            assert_eq!(units, table);
        }
    }

    /// The per-sample phase peaks are the same bits on every path, and
    /// their largest is the block's true peak; no phase output exceeds
    /// the limiter's bound on the largest input times its gain.
    #[test]
    fn phase_peaks_agree_on_every_path() {
        let mut rng = Rng::new(5);
        for _ in 0..200 {
            let len = HISTORY + rng.below(100);
            let w: Vec<f32> = (0..len).map(|_| rng.unit() * 3.0).collect();
            let mut want = vec![0.0f32; len - HISTORY];
            phase_peaks(Path::Scalar, &w, &mut want);
            for path in paths("true_peak_bit_exact_vs_scalar") {
                let mut got = vec![0.0f32; len - HISTORY];
                phase_peaks(path, &w, &mut got);
                let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
                assert_eq!(bits(&got), bits(&want), "{path:?}");
            }
            let (peak, _) = peak_scalar(&w, 0);
            assert_eq!(want.iter().fold(0.0f32, |m, &v| m.max(v)), peak);
        }
        let gain = COEFFICIENTS
            .iter()
            .map(|h| h.iter().map(|c| c.abs()).sum::<f32>())
            .fold(0.0f32, f32::max);
        assert!(gain < 2.03 && gain > 2.0, "{gain}");
    }

    /// Every path gives the same bits as the scalar one, for any split of
    /// the signal into calls, under production FTZ/DAZ and without it,
    /// with denormal and NaN inputs.
    #[test]
    fn bit_exact_vs_scalar() {
        for ftz in [true, false] {
            let saved = set_ftz_daz(ftz);
            for path in paths("true_peak_bit_exact_vs_scalar") {
                let mut rng = Rng::new(7);
                let mut reference = TruePeak::new(2, 1024);
                let mut tp = TruePeak::new(2, 1024);
                for round in 0..200 {
                    let len = rng.below(300);
                    let mut x: Vec<f32> = (0..len).map(|_| rng.sample() * 3.0).collect();
                    if round % 17 == 3 && len > 2 {
                        x[1] = 1.0e-40;
                    }
                    if round == 101 && len > 0 {
                        x[len - 1] = f32::NAN;
                    }
                    for ch in 0..2 {
                        let r = reference.process_on(Path::Scalar, ch, &x);
                        let t = tp.process_on(path, ch, &x);
                        assert_eq!(r.to_bits(), t.to_bits(), "{path:?} round {round}");
                    }
                }
            }
            restore_mxcsr(saved);
        }
    }

    /// The result does not depend on how the signal is split into calls:
    /// the history carries the filter state.
    #[test]
    fn splitting_does_not_change_the_peak() {
        let mut rng = Rng::new(3);
        let x: Vec<f32> = (0..2000).map(|_| rng.sample()).collect();
        let mut whole = TruePeak::new(1, 2000);
        let all = whole.process_on(Path::Scalar, 0, &x);
        let mut parts = TruePeak::new(1, 2000);
        let mut peak = 0.0f32;
        for chunk in x.chunks(37) {
            peak = peak.max(parts.process_on(Path::Scalar, 0, chunk));
        }
        assert_eq!(all.to_bits(), peak.to_bits());
    }

    /// A NaN shows for as long as it is in the filter, then the detector
    /// recovers.
    #[test]
    fn nan_clears_after_the_filter_length() {
        let mut tp = TruePeak::new(1, 64);
        assert!(tp.process(0, &[0.5, f32::NAN]).is_nan());
        assert!(tp.process(0, &[0.5; HISTORY]).is_nan());
        assert!(tp.process(0, &[0.5]).is_finite());
        tp.reset();
        assert_eq!(tp.process(0, &[0.0; 16]), 0.0);
    }
}
