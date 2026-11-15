//! Loudness per ITU-R BS.1770-5 and EBU R 128 from the K-weighted mean
//! square of consecutive 100 ms windows: momentary (400 ms) and short-term
//! (3 s) loudness and their maxima (EBU Tech 3341), integrated loudness
//! gated per BS.1770-5 equations (4) to (7), and the loudness range of
//! EBU Tech 3342.
//!
//! The input is the power of consecutive 10 ms slices, ten to a window.
//! A 400 ms gating block is four consecutive windows, so blocks overlap by
//! 75 % as BS.1770-5 requires; the short-term loudness LRA is computed
//! from is evaluated at every window, at 10 Hz as Tech 3342 requires. The
//! momentary and short-term loudness slide by one slice: Tech 3341 cases
//! 13 and 14 place 400 ms tones at multiples of 20 ms, which a 100 ms step
//! would read up to 0.6 dB low.
//!
//! Gating blocks and short-term values are kept in histograms of 0.01 LU
//! bins, with the exact sum of the powers in each bin, so memory is bounded
//! however long a measurement runs. Every decision is taken on powers, not
//! levels: the gates are power thresholds and the bin edges are products of
//! constants, so the outcome does not depend on the platform's `log10`.
//! Only a bin that straddles a relative gate is placed by its centre.

/// Window length in samples at 48 kHz: 100 ms, the step of the gating
/// blocks.
pub const WINDOW: u64 = 4800;
/// Slice length in samples at 48 kHz: 10 ms, the step of the momentary
/// and short-term loudness.
pub const SLICE: u64 = 480;

const PER_WINDOW: u64 = WINDOW / SLICE;
/// Slices of a gating block and of the momentary loudness (400 ms).
const BLOCK: usize = 40;
/// Slices of the short-term loudness (3 s).
const SHORT: usize = 300;

/// Lowest histogram edge: the power at -70 LKFS, 10^((-70 + 0.691) / 10).
const P_ABS: f64 = 1.1724653045822964e-07;
/// Ratio of consecutive edges: 10^(0.01 / 10), a 0.01 LU bin.
const STEP: f64 = 1.0023052380778996;
/// Square root of [`STEP`]: the centre of a bin over its lower edge.
const HALF_STEP: f64 = 1.001151955538169;
/// Bins from -70 to +20 LKFS. Powers at or above +20 LKFS go into the
/// last bin.
const BINS: usize = 9000;

/// Channel weights of ITU-R BS.1770-5 Table 3.
pub mod weight {
    /// Left, right and centre.
    pub const FRONT: f64 = 1.0;
    /// Left and right surround.
    pub const SURROUND: f64 = 1.41;
    /// The LFE channel is not included in the measurement.
    pub const LFE: f64 = 0.0;
}

/// Loudness in LKFS (LUFS) of a weighted mean-square power, equation (2).
pub fn lufs(power: f64) -> f64 {
    -0.691 + 10.0 * power.log10()
}

struct Histogram {
    count: Vec<u64>,
    sum: Vec<f64>,
    total: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            count: vec![0; BINS],
            sum: vec![0.0; BINS],
            total: 0,
        }
    }

    fn clear(&mut self) {
        self.count.fill(0);
        self.sum.fill(0.0);
        self.total = 0;
    }

    /// Adds a finite `power` to its bin; powers at or above the top edge
    /// go into the last bin.
    fn add(&mut self, edges: &[f64], power: f64) {
        let bin = edges[..BINS]
            .partition_point(|&e| e <= power)
            .saturating_sub(1);
        self.count[bin] += 1;
        self.sum[bin] += power;
        self.total += 1;
    }

    /// Bins whose centre lies above (`strict`) or at or above `threshold`.
    fn first_bin(edges: &[f64], threshold: f64, strict: bool) -> usize {
        edges[..BINS].partition_point(|&e| {
            let centre = e * HALF_STEP;
            if strict {
                centre <= threshold
            } else {
                centre < threshold
            }
        })
    }

    /// Mean power of every entry.
    fn mean(&self) -> f64 {
        self.sum.iter().sum::<f64>() / self.total as f64
    }
}

/// Bin edges, ascending: `P_ABS * STEP^i` by repeated multiplication.
fn edges() -> Vec<f64> {
    let mut e = Vec::with_capacity(BINS + 1);
    let mut p = P_ABS;
    for _ in 0..=BINS {
        e.push(p);
        p *= STEP;
    }
    e
}

/// Loudness measurement of one programme, fed one window at a time.
pub struct Loudness {
    edges: Vec<f64>,
    /// Weighted powers of the last [`SHORT`] slices, a ring indexed by the
    /// slice index.
    recent: Vec<f64>,
    /// Slices in `recent` without a gap before them.
    contiguous: usize,
    /// Slices since the start of the measurement.
    measured: usize,
    last: Option<u64>,
    valid: bool,
    blocks: Histogram,
    short_terms: Histogram,
    max_momentary: Option<f64>,
    max_short_term: Option<f64>,
}

impl Default for Loudness {
    fn default() -> Self {
        Self::new()
    }
}

impl Loudness {
    pub fn new() -> Self {
        Self {
            edges: edges(),
            recent: vec![0.0; SHORT],
            contiguous: 0,
            measured: 0,
            last: None,
            valid: true,
            blocks: Histogram::new(),
            short_terms: Histogram::new(),
            max_momentary: None,
            max_short_term: None,
        }
    }

    /// Adds slice `index`, the samples `[index * SLICE, (index + 1) *
    /// SLICE)`, with its weighted mean square `sum_i G_i z_i`; `None`
    /// when not all of its samples were measured. A missing slice, one
    /// out of order, an incomplete one or one whose power is not finite
    /// (an infinite or NaN sample) is a gap: the momentary and
    /// short-term loudness restart after it, and the integrated loudness
    /// and the loudness range are invalid until [`Loudness::reset`].
    pub fn push(&mut self, index: u64, power: Option<f64>) {
        if self.last.is_some_and(|l| index != l + 1) {
            self.gap();
        }
        self.last = Some(index);
        let Some(power) = power.filter(|p| p.is_finite()) else {
            self.gap();
            return;
        };
        self.recent[index as usize % SHORT] = power;
        self.contiguous = (self.contiguous + 1).min(SHORT);
        self.measured += 1;
        let window_end = (index + 1).is_multiple_of(PER_WINDOW);
        if let Some(m) = self.mean(index, BLOCK).filter(|_| self.measured >= BLOCK) {
            self.max_momentary = Some(self.max_momentary.map_or(m, |x| x.max(m)));
            if window_end && m > P_ABS {
                self.blocks.add(&self.edges, m);
            }
        }
        if let Some(s) = self.mean(index, SHORT).filter(|_| self.measured >= SHORT) {
            self.max_short_term = Some(self.max_short_term.map_or(s, |x| x.max(s)));
            if window_end && s >= P_ABS {
                self.short_terms.add(&self.edges, s);
            }
        }
    }

    /// A gap before the next window. Before the first complete window of
    /// a measurement, nothing has been measured, so the measurement
    /// starts after the gap instead.
    pub fn gap(&mut self) {
        self.contiguous = 0;
        if self.measured > 0 {
            self.valid = false;
        }
    }

    fn mean(&self, index: u64, windows: usize) -> Option<f64> {
        if self.contiguous < windows {
            return None;
        }
        // Oldest first, the same order on every replica.
        let mut sum = 0.0;
        for i in (0..windows as u64).rev() {
            sum += self.recent[((index + SHORT as u64 - i) % SHORT as u64) as usize];
        }
        Some(sum / windows as f64)
    }

    fn last_mean(&self, windows: usize) -> Option<f64> {
        self.last.and_then(|l| self.mean(l, windows))
    }

    /// Restarts the integrated loudness, the loudness range and the
    /// maxima at the next slice; a caller resetting on a window boundary
    /// keeps the gating blocks on the 100 ms grid. The momentary and short-term loudness
    /// keep running.
    pub fn reset(&mut self) {
        self.measured = 0;
        self.valid = true;
        self.blocks.clear();
        self.short_terms.clear();
        self.max_momentary = None;
        self.max_short_term = None;
    }

    /// Whether no window went missing since the measurement started.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Momentary loudness of the last 400 ms, LUFS, once 40 contiguous
    /// slices are in.
    pub fn momentary(&self) -> Option<f64> {
        self.last_mean(BLOCK).map(lufs)
    }

    /// Short-term loudness of the last 3 s, LUFS, once 300 contiguous
    /// slices are in.
    pub fn short_term(&self) -> Option<f64> {
        self.last_mean(SHORT).map(lufs)
    }

    pub fn max_momentary(&self) -> Option<f64> {
        self.max_momentary.map(lufs)
    }

    pub fn max_short_term(&self) -> Option<f64> {
        self.max_short_term.map(lufs)
    }

    /// Integrated loudness, LUFS: `None` while invalid or before a gating
    /// block above the absolute gate; `-inf` never.
    pub fn integrated(&self) -> Option<f64> {
        if !self.valid || self.blocks.total == 0 {
            return None;
        }
        // Equation (6): the relative gate 10 LU below the absolute-gated
        // loudness, a factor of 0.1 in power. Equation (7): blocks above
        // both gates.
        let from = Histogram::first_bin(&self.edges, self.blocks.mean() * 0.1, true);
        let n: u64 = self.blocks.count[from..].iter().sum();
        let sum: f64 = self.blocks.sum[from..].iter().sum();
        (n > 0).then(|| lufs(sum / n as f64))
    }

    /// Loudness range, LU, per EBU Tech 3342: short-term values at or
    /// above -70 LUFS and at or above 20 LU below their mean power; the
    /// 95th minus the 10th percentile, each the element at
    /// `round((n - 1) * p / 100 + 1)` of the sorted values as in the
    /// Tech 3342 reference code. `None` while invalid or before a
    /// short-term value above the absolute gate.
    pub fn range(&self) -> Option<f64> {
        if !self.valid || self.short_terms.total == 0 {
            return None;
        }
        let h = &self.short_terms;
        let from = Histogram::first_bin(&self.edges, h.mean() * 0.01, false);
        let n: u64 = h.count[from..].iter().sum();
        if n == 0 {
            return None;
        }
        let rank = |p: u64| ((n - 1) * p + 50) / 100 + 1;
        let bin_of = |rank: u64| {
            let mut seen = 0;
            for (i, &c) in h.count.iter().enumerate().skip(from) {
                seen += c;
                if seen >= rank {
                    return i;
                }
            }
            BINS - 1
        };
        let (low, high) = (bin_of(rank(10)), bin_of(rank(95)));
        Some((high - low) as f64 * 0.01)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn power(lufs: f64) -> f64 {
        10f64.powf((lufs + 0.691) / 10.0)
    }

    #[test]
    fn edges_span_the_bins() {
        let e = edges();
        assert_eq!(e.len(), BINS + 1);
        assert!((lufs(e[0]) + 70.0).abs() < 1e-9);
        assert!((lufs(e[BINS]) - 20.0).abs() < 1e-6);
        assert!((e[1] * HALF_STEP / e[1] - STEP.sqrt()).abs() < 1e-15);
    }

    #[test]
    fn constant_power_reads_its_level_everywhere() {
        let mut l = Loudness::new();
        for w in 0..1000 {
            l.push(w, Some(power(-23.0)));
        }
        for v in [
            l.momentary(),
            l.short_term(),
            l.integrated(),
            l.max_momentary(),
        ] {
            assert!((v.unwrap() + 23.0).abs() < 1e-9, "{v:?}");
        }
        assert_eq!(l.range(), Some(0.0));
    }

    /// Blocks below -70 LUFS and those 10 LU below the absolute-gated
    /// level do not count; the three blocks of each transition do.
    #[test]
    fn gates_exclude_quiet_blocks() {
        let mut l = Loudness::new();
        let mut w = 0;
        for (level, windows) in [(-80.0, 2000), (-23.0, 2000), (-40.0, 2000)] {
            for _ in 0..windows {
                l.push(w, Some(power(level)));
                w += 1;
            }
        }
        assert!(
            (l.integrated().unwrap() + 23.0).abs() < 0.1,
            "{:?}",
            l.integrated()
        );
    }

    /// Powers at or above the +20 LKFS top edge go into the last bin,
    /// whose exact sum still gives their level.
    #[test]
    fn loudness_above_the_top_bin_is_measured() {
        let mut l = Loudness::new();
        for w in 0..1000 {
            l.push(w, Some(power(21.0)));
        }
        assert!((l.integrated().unwrap() - 21.0).abs() < 1e-9);
        assert!((l.max_short_term().unwrap() - 21.0).abs() < 1e-9);
        assert_eq!(l.range(), Some(0.0));
    }

    /// A slice whose power is not finite, from an infinite or NaN sample,
    /// is a gap: it invalidates the measurement rather than poisoning the
    /// sums.
    #[test]
    fn a_non_finite_slice_is_a_gap() {
        for bad in [f64::INFINITY, f64::NAN] {
            let mut l = Loudness::new();
            for w in 0..500 {
                l.push(w, Some(power(-23.0)));
            }
            l.push(500, Some(bad));
            assert!(!l.valid(), "{bad}");
            assert_eq!(
                (l.integrated(), l.range(), l.momentary()),
                (None, None, None)
            );
            l.reset();
            for w in 501..900 {
                l.push(w, Some(power(-23.0)));
            }
            assert!(l.valid());
            assert!((l.integrated().unwrap() + 23.0).abs() < 1e-9);
            assert!((l.short_term().unwrap() + 23.0).abs() < 1e-9);
        }
    }

    #[test]
    fn a_gap_invalidates_until_reset() {
        let mut l = Loudness::new();
        // Incomplete slices before the first complete one are the start.
        l.push(3, None);
        for w in 4..500 {
            l.push(w, Some(power(-20.0)));
        }
        assert!(l.valid());
        l.push(501, Some(power(-20.0)));
        assert!(!l.valid());
        assert_eq!(
            (l.integrated(), l.range(), l.momentary()),
            (None, None, None)
        );
        for w in 502..560 {
            l.push(w, Some(power(-20.0)));
        }
        assert!(l.momentary().is_some());
        l.reset();
        assert!(l.valid() && l.integrated().is_none());
        for w in 560..600 {
            l.push(w, Some(power(-30.0)));
        }
        assert!((l.integrated().unwrap() + 30.0).abs() < 1e-9);
        assert!((l.max_momentary().unwrap() + 30.0).abs() < 1e-9);
        l.push(600, None);
        assert!(!l.valid());
    }
}
