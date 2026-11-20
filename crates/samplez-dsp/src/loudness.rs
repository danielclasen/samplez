//! Loudness per ITU-R BS.1770-5 and EBU R 128 from the K-weighted mean
//! square of consecutive 10 ms slices: momentary (400 ms) and short-term
//! (3 s) loudness and their maxima (EBU Tech 3341), integrated loudness
//! gated per BS.1770-5 equations (4) to (7), and the loudness range of
//! EBU Tech 3342.
//!
//! The input is the power of consecutive 10 ms slices, ten to a 100 ms
//! window. A 400 ms gating block, four consecutive windows, is taken at
//! the end of every window, so blocks overlap by 75 % as BS.1770-5
//! requires; the loudness range takes the short-term loudness at the end
//! of every window, at 10 Hz as Tech 3342 requires. The momentary and
//! short-term loudness slide by one slice: Tech 3341 cases 13 and 14
//! place 400 ms tones at multiples of 20 ms, which a 100 ms step would
//! read up to 0.6 dB low.
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
/// The logarithm is libm's, so the reported level does not depend on the
/// host C library's variant of `log10`.
pub fn lufs(power: f64) -> f64 {
    -0.691 + 10.0 * libm::log10(power)
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

/// One slice as [`Loudness::push`] takes it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Slice {
    /// Every sample of the slice measured: its weighted mean square
    /// `sum_i G_i z_i`. A power that is not finite, from an infinite or
    /// NaN sample, counts as a [`Slice::Gap`].
    Power(f64),
    /// Measured, but left out of the measurement by the caller.
    Gap,
    /// Not all of its samples were measured.
    Lost,
    /// Measured while the measurement is paused: its power, not finite
    /// when not all of its samples were measured, keeps the momentary
    /// and short-term loudness running, but the slice is left out of the
    /// integrated loudness, the loudness range and the maxima as a gap
    /// is, and counts in [`Loudness::paused`] alone.
    Paused(f64),
}

/// Loudness measurement of one programme, fed one slice at a time.
///
/// A slice that is a gap, lost, missing or out of order is left out of
/// the integrated loudness and the loudness range, which carry on over
/// the slices kept as if it had never been there: their gating blocks
/// and short-term values are taken over consecutive kept slices, on the
/// 100 ms grid of the kept slices counted from the start of the
/// measurement. Without such a slice that grid is the 100 ms grid of the
/// slice index. The momentary and short-term loudness restart after it,
/// but not after a [`Slice::Paused`] one with a finite power.
pub struct Loudness {
    edges: Vec<f64>,
    /// Weighted powers of the last [`SHORT`] slices, a ring indexed by the
    /// slice index.
    recent: Vec<f64>,
    /// Slices in `recent` without a gap before them.
    contiguous: usize,
    last: Option<u64>,
    /// Weighted powers of the last [`SHORT`] slices the measurement kept,
    /// a ring indexed by `kept`.
    kept_powers: Vec<f64>,
    /// Slices the measurement kept.
    kept: u64,
    /// The slice index the measurement started at, and that slice's
    /// position in its 100 ms window.
    start: Option<u64>,
    offset: u64,
    /// The slice index the measurement expects next; none past
    /// `u64::MAX`.
    next: Option<u64>,
    gaps: u64,
    lost: u64,
    paused: u64,
    /// The slice index after the last [`Slice::Paused`] one: the maxima
    /// take no momentary or short-term value reaching back before it.
    resumed: u64,
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
            last: None,
            kept_powers: vec![0.0; SHORT],
            kept: 0,
            start: None,
            offset: 0,
            next: None,
            gaps: 0,
            lost: 0,
            paused: 0,
            resumed: 0,
            blocks: Histogram::new(),
            short_terms: Histogram::new(),
            max_momentary: None,
            max_short_term: None,
        }
    }

    /// Adds slice `index`, the samples `[index * SLICE, (index + 1) *
    /// SLICE)`. Returns whether it ended a 100 ms window of the kept
    /// slices, the only point a kept slice changes the integrated
    /// loudness or the loudness range.
    ///
    /// A [`Slice::Gap`] or a power that is not finite counts in
    /// [`Loudness::gaps`], a [`Slice::Paused`] in [`Loudness::paused`];
    /// a [`Slice::Lost`], each slice index skipped before `index` and a
    /// slice out of order count in [`Loudness::lost`]. Without a
    /// [`Loudness::reset`] the measurement starts at the first slice
    /// that is not lost, so lost slices before it, such as the incomplete
    /// first slice of a stream, count nowhere.
    pub fn push(&mut self, index: u64, slice: Slice) -> bool {
        if self.last.is_some_and(|l| l.checked_add(1) != Some(index)) {
            self.contiguous = 0;
        }
        self.last = Some(index);
        // What the momentary and short-term loudness take: a paused
        // slice's power too.
        let live = match slice {
            Slice::Power(p) | Slice::Paused(p) if p.is_finite() => Some(p),
            _ => None,
        };
        let power = live.filter(|_| matches!(slice, Slice::Power(_)));
        match live {
            Some(p) => {
                self.recent[index as usize % SHORT] = p;
                self.contiguous = (self.contiguous + 1).min(SHORT);
            }
            None => self.contiguous = 0,
        }
        let start = match self.start {
            Some(s) => s,
            None if slice == Slice::Lost => return false,
            None => {
                self.begin(index);
                index
            }
        };
        match self.next {
            Some(n) if index >= n => self.lost += index - n,
            _ => {
                self.lost += 1;
                return false;
            }
        }
        self.next = index.checked_add(1);
        let Some(p) = power else {
            match slice {
                Slice::Lost => self.lost += 1,
                Slice::Paused(_) => {
                    self.paused += 1;
                    self.resumed = index.saturating_add(1);
                }
                _ => self.gaps += 1,
            }
            return false;
        };
        // The maxima follow the momentary and short-term loudness, over
        // slices of the measurement only, none of them paused.
        let since = index
            .checked_sub(start.max(self.resumed))
            .map_or(0, |d| d.saturating_add(1));
        if let Some(m) = self.mean(index, BLOCK).filter(|_| since >= BLOCK as u64) {
            self.max_momentary = Some(self.max_momentary.map_or(m, |x| x.max(m)));
        }
        if let Some(s) = self.mean(index, SHORT).filter(|_| since >= SHORT as u64) {
            self.max_short_term = Some(self.max_short_term.map_or(s, |x| x.max(s)));
        }
        let position = self.offset + self.kept;
        self.kept_powers[(self.kept % SHORT as u64) as usize] = p;
        self.kept += 1;
        if position % PER_WINDOW != PER_WINDOW - 1 {
            return false;
        }
        if self.kept >= BLOCK as u64 {
            let m = self.kept_mean(BLOCK);
            if m > P_ABS {
                self.blocks.add(&self.edges, m);
            }
        }
        if self.kept >= SHORT as u64 {
            let s = self.kept_mean(SHORT);
            if s >= P_ABS {
                self.short_terms.add(&self.edges, s);
            }
        }
        true
    }

    /// Adds `count` slices from `index` on that were not measured, all
    /// [`Slice::Gap`], all [`Slice::Paused`] (the power not taken) or all
    /// [`Slice::Lost`], as [`Loudness::push`] adds each with no power, in
    /// one step however many they are.
    pub fn skip(&mut self, index: u64, count: u64, slice: Slice) {
        let Some(last) = count.checked_sub(1).and_then(|c| index.checked_add(c)) else {
            return;
        };
        self.contiguous = 0;
        self.last = Some(last);
        let gap = slice != Slice::Lost;
        if self.start.is_none() {
            if !gap {
                return;
            }
            self.begin(index);
        }
        match self.next {
            Some(n) if index >= n => self.lost += index - n,
            _ => {
                self.lost += count;
                return;
            }
        }
        self.next = last.checked_add(1);
        match slice {
            Slice::Lost => self.lost += count,
            Slice::Paused(_) => {
                self.paused += count;
                self.resumed = last.saturating_add(1);
            }
            _ => self.gaps += count,
        }
    }

    fn begin(&mut self, index: u64) {
        self.start = Some(index);
        self.offset = index % PER_WINDOW;
        self.next = Some(index);
    }

    fn mean(&self, index: u64, windows: usize) -> Option<f64> {
        if self.contiguous < windows {
            return None;
        }
        // Oldest first, the same order on every replica. Reduced first,
        // so no index overflows.
        let (at, short) = (index % SHORT as u64, SHORT as u64);
        let mut sum = 0.0;
        for i in (0..windows as u64).rev() {
            sum += self.recent[((at + short - i) % short) as usize];
        }
        Some(sum / windows as f64)
    }

    /// Mean of the last `windows` kept slices, oldest first as
    /// [`Loudness::mean`] adds them, so without a gap they are the same
    /// bits.
    fn kept_mean(&self, windows: usize) -> f64 {
        let short = SHORT as u64;
        let at = (self.kept - 1) % short;
        let mut sum = 0.0;
        for i in (0..windows as u64).rev() {
            sum += self.kept_powers[((at + short - i) % short) as usize];
        }
        sum / windows as f64
    }

    fn last_mean(&self, windows: usize) -> Option<f64> {
        self.last.and_then(|l| self.mean(l, windows))
    }

    /// Restarts the integrated loudness, the loudness range, the maxima
    /// and the counts at slice `index`; a slice index before it that is
    /// pushed after the reset is out of order. A caller resetting on a
    /// window boundary keeps the gating blocks on the 100 ms grid. The
    /// momentary and short-term loudness keep running.
    pub fn reset(&mut self, index: u64) {
        self.begin(index);
        self.kept = 0;
        self.gaps = 0;
        self.lost = 0;
        self.paused = 0;
        self.blocks.clear();
        self.short_terms.clear();
        self.max_momentary = None;
        self.max_short_term = None;
    }

    /// The slice index the measurement started at.
    pub fn start(&self) -> Option<u64> {
        self.start
    }

    /// Slices of the measurement left out as gaps: [`Slice::Gap`] and
    /// powers that are not finite.
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// Slices of the measurement lost: [`Slice::Lost`], slice indices
    /// skipped and slices out of order.
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// Slices of the measurement left out as [`Slice::Paused`].
    pub fn paused(&self) -> u64 {
        self.paused
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

    /// Integrated loudness, LUFS: `None` before a gating block above the
    /// absolute gate; `-inf` never.
    pub fn integrated(&self) -> Option<f64> {
        if self.blocks.total == 0 {
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
    /// Tech 3342 reference code. `None` before a short-term value above
    /// the absolute gate.
    pub fn range(&self) -> Option<f64> {
        if self.short_terms.total == 0 {
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

    /// glibc's `log10` gives this power a last bit different from
    /// libm's; the level is libm's on every host.
    #[test]
    fn lufs_takes_the_libm_logarithm() {
        let p = f64::from_bits(0x3f18_18f7_bcb2_d2ef);
        assert_eq!(lufs(p), -41.056_673_893_056_626);
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
            l.push(w, Slice::Power(power(-23.0)));
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

    /// Every slice index is accepted: the last ones before `u64::MAX`
    /// measure like any others, and one after `u64::MAX` is out of order:
    /// lost, and left out.
    #[test]
    fn indices_up_to_the_largest_do_not_overflow() {
        let mut l = Loudness::new();
        let first = u64::MAX - 999;
        for w in first..=u64::MAX {
            l.push(w, Slice::Power(power(-23.0)));
        }
        for v in [l.momentary(), l.short_term(), l.max_short_term()] {
            assert!((v.unwrap() + 23.0).abs() < 1e-9, "{v:?}");
        }
        let integrated = l.integrated().unwrap();
        assert!((integrated + 23.0).abs() < 1e-9);
        l.push(0, Slice::Power(power(-10.0)));
        assert_eq!(
            (l.lost(), l.gaps(), l.integrated()),
            (1, 0, Some(integrated))
        );
    }

    /// Blocks below -70 LUFS and those 10 LU below the absolute-gated
    /// level do not count; the three blocks of each transition do.
    #[test]
    fn gates_exclude_quiet_blocks() {
        let mut l = Loudness::new();
        let mut w = 0;
        for (level, windows) in [(-80.0, 2000), (-23.0, 2000), (-40.0, 2000)] {
            for _ in 0..windows {
                l.push(w, Slice::Power(power(level)));
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
            l.push(w, Slice::Power(power(21.0)));
        }
        assert!((l.integrated().unwrap() - 21.0).abs() < 1e-9);
        assert!((l.max_short_term().unwrap() - 21.0).abs() < 1e-9);
        assert_eq!(l.range(), Some(0.0));
    }

    /// A slice whose power is not finite, from an infinite or NaN sample,
    /// is a gap: it is left out rather than poisoning the sums, and the
    /// integrated loudness and the range carry on over the rest.
    #[test]
    fn a_non_finite_slice_is_a_gap() {
        for bad in [f64::INFINITY, f64::NAN] {
            let mut l = Loudness::new();
            for w in 0..500 {
                l.push(w, Slice::Power(power(-23.0)));
            }
            l.push(500, Slice::Power(bad));
            assert_eq!((l.gaps(), l.lost()), (1, 0), "{bad}");
            assert!((l.integrated().unwrap() + 23.0).abs() < 1e-9);
            assert_eq!((l.range(), l.momentary()), (Some(0.0), None));
            for w in 501..900 {
                l.push(w, Slice::Power(power(-23.0)));
            }
            assert!((l.integrated().unwrap() + 23.0).abs() < 1e-9);
            assert!((l.short_term().unwrap() + 23.0).abs() < 1e-9);
            assert_eq!(l.range(), Some(0.0));
        }
    }

    fn constant(l: &mut Loudness, from: u64, slices: u64, level: f64) -> u64 {
        for i in from..from + slices {
            l.push(i, Slice::Power(power(level)));
        }
        from + slices
    }

    /// Gating blocks below the -70 LKFS absolute gate do not count, even
    /// where the relative gate would keep them: a programme entirely at
    /// -72 LUFS has no integrated loudness.
    #[test]
    fn the_absolute_gate_excludes_blocks_below_minus_70() {
        let mut l = Loudness::new();
        constant(&mut l, 0, 3000, -72.0);
        assert_eq!(l.integrated(), None);
        assert!((l.momentary().unwrap() + 72.0).abs() < 1e-9);
    }

    /// Short-term values below -70 LUFS do not count towards the range:
    /// a programme entirely at -75 LUFS has none, and after a -60 LUFS
    /// tone, 300 s more at -75 LUFS change nothing once its short-term
    /// loudness is below the gate.
    #[test]
    fn the_absolute_gate_excludes_short_terms_below_minus_70() {
        let mut l = Loudness::new();
        constant(&mut l, 0, 3000, -75.0);
        assert_eq!(l.range(), None);
        let mut l = Loudness::new();
        let i = constant(&mut l, 0, 3000, -60.0);
        let i = constant(&mut l, i, 350, -75.0);
        let before = l.range();
        assert_eq!(before, Some(0.0));
        constant(&mut l, i, 30_000, -75.0);
        assert_eq!(l.range(), before);
    }

    /// The 10th and 95th percentiles are the elements at ranks
    /// `round((n - 1) * p / 100 + 1)`: of 101 short-term values 0.1 LU
    /// apart from -30 LUFS, the 11th (-29.0) and the 96th (-20.5).
    #[test]
    fn the_range_is_the_95th_minus_the_10th_percentile() {
        let mut l = Loudness::new();
        for i in 0..=100 {
            // At a bin centre, clear of the edges.
            let level = -30.0 + f64::from(i) * 0.1 + 0.005;
            l.short_terms.add(&l.edges, power(level));
        }
        let r = l.range().unwrap();
        assert!((r - 8.5).abs() < 1e-9, "{r}");
    }

    /// Gating blocks and the short-term values of the range are taken at
    /// the end of each 100 ms window, ten slices, not at every slice:
    /// 1000 slices give a block at the ends of windows 3 to 99 and a
    /// short-term value at the ends of windows 29 to 99.
    #[test]
    fn blocks_and_short_terms_are_on_the_100_ms_grid() {
        let mut l = Loudness::new();
        constant(&mut l, 0, 1000, -23.0);
        assert_eq!((l.blocks.total, l.short_terms.total), (97, 71));
    }

    /// A gap, a lost slice and a missing slice index no longer blank the
    /// integrated loudness and the range: they carry on, each slice left
    /// out counted, gaps apart from lost slices. The momentary loudness
    /// restarts after each. Lost slices before the first slice measured
    /// are the start, and a reset restarts the counts.
    #[test]
    fn a_gap_is_left_out_and_the_measurement_carries_on() {
        let mut l = Loudness::new();
        l.push(3, Slice::Lost);
        for w in 4..500 {
            l.push(w, Slice::Power(power(-20.0)));
        }
        assert_eq!((l.start(), l.gaps(), l.lost()), (Some(4), 0, 0));
        let (i, r) = (l.integrated(), l.range());
        assert!((i.unwrap() + 20.0).abs() < 1e-9);
        l.push(500, Slice::Gap);
        assert_eq!((l.gaps(), l.lost(), l.momentary()), (1, 0, None));
        assert_eq!((l.integrated(), l.range()), (i, r));
        l.push(501, Slice::Lost);
        // 502 and 503 never arrive.
        l.push(504, Slice::Power(power(-20.0)));
        assert_eq!((l.gaps(), l.lost()), (1, 3));
        for w in 505..600 {
            l.push(w, Slice::Power(power(-20.0)));
        }
        assert!(l.momentary().is_some());
        assert!((l.integrated().unwrap() + 20.0).abs() < 1e-9);
        assert_eq!(l.range(), Some(0.0));
        l.reset(600);
        assert_eq!((l.gaps(), l.lost(), l.integrated()), (0, 0, None));
        for w in 600..640 {
            l.push(w, Slice::Power(power(-30.0)));
        }
        assert!((l.integrated().unwrap() + 30.0).abs() < 1e-9);
        assert!((l.max_momentary().unwrap() + 30.0).abs() < 1e-9);
        // A reset counts the slices after its index that never arrive.
        l.reset(700);
        l.push(702, Slice::Power(power(-30.0)));
        assert_eq!((l.start(), l.lost()), (Some(700), 2));
    }

    /// The integrated loudness and the range over slices with gaps, lost
    /// and missing ones among them are the same bits as a measurement of
    /// the kept slices alone, fed one after the other from the same start:
    /// a gap is taken out of the programme, not measured as silence.
    #[test]
    fn the_measurement_over_the_rest_is_the_one_without_the_gaps() {
        // A level that moves, so every block and short-term value differs.
        let level = |w: u64| -30.0 + ((w * 7919) % 211) as f64 / 10.0;
        let gapped = |w: u64| match w % 997 {
            13 => Some(Slice::Gap),
            400 => Some(Slice::Lost),
            555 => Some(Slice::Power(f64::NAN)),
            _ => None,
        };
        let (mut l, mut kept) = (Loudness::new(), Loudness::new());
        let (start, end) = (20, 20 + 40_000);
        let (mut k, mut gaps, mut lost) = (start, 0, 0);
        for w in start..end {
            if w % 997 == 700 {
                lost += 1; // missing
                continue;
            }
            match gapped(w) {
                Some(s) => {
                    if s == Slice::Lost {
                        lost += 1;
                    } else {
                        gaps += 1;
                    }
                    l.push(w, s);
                }
                None => {
                    l.push(w, Slice::Power(power(level(w))));
                    kept.push(k, Slice::Power(power(level(w))));
                    k += 1;
                }
            }
        }
        assert_eq!((l.gaps(), l.lost()), (gaps, lost));
        assert!(gaps > 50 && lost > 50);
        assert!(l.integrated().is_some() && l.range().is_some());
        assert_eq!(
            l.integrated().map(f64::to_bits),
            kept.integrated().map(f64::to_bits)
        );
        assert_eq!(l.range().map(f64::to_bits), kept.range().map(f64::to_bits));
        assert_eq!((kept.gaps(), kept.lost()), (0, 0));
    }

    /// Without a gap the kept slices' grid is the slice index's: a
    /// measurement starting mid-window takes its blocks at the ends of
    /// the windows of the slice index.
    #[test]
    fn the_grid_follows_the_slice_index_without_a_gap() {
        let mut l = Loudness::new();
        constant(&mut l, 7, 993, -23.0);
        // Windows 4 to 99 end at least 40 slices after slice 7.
        assert_eq!((l.blocks.total, l.short_terms.total), (96, 70));
    }

    /// Skipping slices counts them all at once, as pushing each would:
    /// gaps start a measurement, lost slices before one do not, and slices
    /// skipped past the next expected count as lost.
    #[test]
    fn skipped_slices_count_as_pushed_ones() {
        let (mut l, mut each) = (Loudness::new(), Loudness::new());
        l.skip(0, 5, Slice::Lost);
        l.skip(5, 3, Slice::Gap);
        for w in 0..5 {
            each.push(w, Slice::Lost);
        }
        for w in 5..8 {
            each.push(w, Slice::Gap);
        }
        for m in [&mut l, &mut each] {
            constant(m, 8, 500, -23.0);
        }
        l.skip(508, 4, Slice::Lost);
        l.skip(514, 2, Slice::Gap);
        for w in (508..512).chain(514..516) {
            each.push(w, if w < 512 { Slice::Lost } else { Slice::Gap });
        }
        for m in [&mut l, &mut each] {
            constant(m, 516, 300, -20.0);
        }
        assert_eq!((l.start(), l.gaps(), l.lost()), (Some(5), 5, 6));
        assert_eq!(
            (each.start(), each.gaps(), each.lost()),
            (l.start(), l.gaps(), l.lost())
        );
        assert_eq!(
            l.integrated().map(f64::to_bits),
            each.integrated().map(f64::to_bits)
        );
        assert_eq!(l.range().map(f64::to_bits), each.range().map(f64::to_bits));
        l.skip(0, 0, Slice::Gap);
        l.skip(10, 2, Slice::Gap);
        assert_eq!((l.gaps(), l.lost()), (5, 8), "out of order");
    }

    /// The integrated loudness and the range over a measurement paused
    /// for spans, pushed and skipped, are the same bits as a measurement
    /// of the running slices alone, fed one after the other from the same
    /// start: a pause takes its slices out of the programme, however loud
    /// they are. They count as paused, neither gaps nor lost.
    #[test]
    fn the_measurement_over_the_running_slices_is_the_one_without_the_paused() {
        // A level that moves, so every block and short-term value differs.
        let level = |w: u64| -30.0 + ((w * 7919) % 211) as f64 / 10.0;
        let (mut l, mut kept) = (Loudness::new(), Loudness::new());
        let (start, end) = (20, 20 + 40_000);
        let (mut k, mut paused, mut w) = (start, 0, start);
        while w < end {
            match w % 1999 {
                // Paused, and louder than anything kept.
                100..400 => {
                    l.push(w, Slice::Paused(power(level(w) + 15.0)));
                    paused += 1;
                }
                // Paused and skipped.
                1000 => {
                    l.skip(w, 250, Slice::Paused(f64::NAN));
                    paused += 250;
                    w += 250;
                    continue;
                }
                _ => {
                    l.push(w, Slice::Power(power(level(w))));
                    kept.push(k, Slice::Power(power(level(w))));
                    k += 1;
                }
            }
            w += 1;
        }
        assert_eq!((l.gaps(), l.lost(), l.paused()), (0, 0, paused));
        assert!(paused > 1000);
        assert!(l.integrated().is_some() && l.range().is_some());
        assert_eq!(
            l.integrated().map(f64::to_bits),
            kept.integrated().map(f64::to_bits)
        );
        assert_eq!(l.range().map(f64::to_bits), kept.range().map(f64::to_bits));
    }

    /// While paused the momentary and short-term loudness follow the
    /// programme, but the integrated loudness, the range and the maxima
    /// do not; once running again, the maxima take no momentary value
    /// whose 400 ms reach back into the pause.
    #[test]
    fn the_momentary_loudness_keeps_running_while_paused() {
        let mut l = Loudness::new();
        let w = constant(&mut l, 0, 400, -23.0);
        let (i, r) = (l.integrated(), l.range());
        for w in w..w + 400 {
            l.push(w, Slice::Paused(power(-10.0)));
        }
        for v in [l.momentary(), l.short_term()] {
            assert!((v.unwrap() + 10.0).abs() < 1e-9, "{v:?}");
        }
        assert_eq!((l.integrated(), l.range()), (i, r));
        for v in [l.max_momentary(), l.max_short_term()] {
            assert!((v.unwrap() + 23.0).abs() < 1e-9, "{v:?}");
        }
        assert_eq!((l.gaps(), l.lost(), l.paused()), (0, 0, 400));
        let w = constant(&mut l, w + 400, 39, -23.0);
        assert!(l.momentary().unwrap() > -22.0, "{:?}", l.momentary());
        assert!((l.max_momentary().unwrap() + 23.0).abs() < 1e-9);
        constant(&mut l, w, 1, -23.0);
        assert!((l.momentary().unwrap() + 23.0).abs() < 1e-9);
        assert!((l.max_momentary().unwrap() + 23.0).abs() < 1e-9);
    }

    /// A paused slice counts in `paused` alone, whatever its power, pushed
    /// or skipped; one without a finite power restarts the momentary
    /// loudness, as a skip does. Paused slices start a measurement, as
    /// gaps do, and a reset restarts their count.
    #[test]
    fn a_paused_slice_is_neither_a_gap_nor_lost() {
        let mut l = Loudness::new();
        l.push(5, Slice::Paused(power(-23.0)));
        assert_eq!(l.start(), Some(5));
        let w = constant(&mut l, 6, 100, -23.0);
        l.push(w, Slice::Paused(f64::NAN));
        assert_eq!(l.momentary(), None);
        l.skip(w + 1, 10, Slice::Paused(0.0));
        assert_eq!((l.gaps(), l.lost(), l.paused()), (0, 0, 12));
        l.reset(w + 11);
        assert_eq!(l.paused(), 0);
    }
}
