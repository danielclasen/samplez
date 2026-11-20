//! The supervisor's side of metering: meter records from the RT thread
//! become peaks, true peaks and loudness per output and configured meter,
//! and peaks, RMS and momentary loudness per input.
//!
//! Meters do not feed the output, so they are outside the output hashes
//! of plan 2.11. A loudness reset applies at a sample index. From it on,
//! the integrated loudness, the loudness range and the maxima are a pure
//! function of the output samples from 11 before that index on, the
//! history of the true-peak interpolator; the momentary and short-term
//! loudness are once 0.4 s and 3 s past it, and until then still include
//! slices from before the reset. Within those bounds, replicas that
//! compute the same output and lose no slice report the same values.
//! Before the first reset each measures from its own start. Levels in
//! LUFS come from libm's `log10`, not the host C library's.
//!
//! A slice in which an input down by the log fed the meter, or whose
//! power is not finite, is a gap; one this replica did not meter whole,
//! or metered while diverged, is lost; one metered while the log held
//! the meter paused is paused. Each is left out of the integrated
//! loudness and the loudness range, which carry on over the rest. Gaps
//! and pauses follow from the log and the output, so `gaps` and `paused`
//! are equal on all replicas whose `lost` is 0; a replica with `lost`
//! above 0 may count fewer or more of them over its lost and diverged
//! spans.

use samplez_dsp::{Loudness, Slice, WINDOW};

use crate::rt_loop::{MeterRecord, SLICE};

/// Meter readings of one output.
#[derive(Clone, Debug, PartialEq)]
pub struct OutputMeters {
    /// The last 100 ms window the readings cover, the samples
    /// `[window * 4800, (window + 1) * 4800)`.
    pub window: Option<u64>,
    /// Per channel, the largest absolute sample value of that window,
    /// full scale 1.0.
    pub peak: Vec<f32>,
    /// Per channel, the RMS level of that window referenced to a sine
    /// as in AES17: the root mean square times the square root of 2, so
    /// a full-scale sine reads 1.0 (0 dB FS).
    pub rms: Vec<f32>,
    /// Per channel, the true peak of that window per ITU-R BS.1770-5
    /// Annex 2, full scale 1.0, and never below the sample peak: no
    /// phase of the Annex's interpolator is the input sample itself, and
    /// two have a DC gain of 0.973, so the oversampled peak alone can
    /// read below the sample peak.
    pub true_peak: Vec<f32>,
    /// Per channel, the largest true peak since the measurement started.
    pub max_true_peak: Vec<f32>,
    /// Loudness in LUFS, `None` until enough audio has been measured.
    pub momentary: Option<f64>,
    pub short_term: Option<f64>,
    pub max_momentary: Option<f64>,
    pub max_short_term: Option<f64>,
    /// Gated per ITU-R BS.1770-5, over the slices of the measurement
    /// that are neither gaps, lost nor paused.
    pub integrated: Option<f64>,
    /// Loudness range per EBU Tech 3342, LU, over the same slices.
    pub range: Option<f64>,
    /// Slices of 10 ms since the measurement started that are gaps: an
    /// input down by the log (`InputDown` applied, not detached) fed one
    /// of the meter's columns through a coefficient whose target is other
    /// than 0 in a block of the slice, computed or skipped, or its
    /// loudness power is not finite. Equal on all replicas whose `lost` is
    /// 0; one with `lost` above 0 may count fewer or more over its lost
    /// and diverged spans.
    pub gaps: u64,
    /// Slices of 10 ms since the measurement started that this replica
    /// did not meter whole, or metered while held diverged: samples an
    /// overrun or a forward clock step skipped that are not gaps, blocks
    /// not computed without a core, and slices whose meter records were
    /// lost on the way from the RT thread. A slice metered while diverged
    /// is lost even where it would be a gap or paused. Other than 0, the
    /// integrated loudness and range may differ from those of a replica
    /// that lost none.
    pub lost: u64,
    /// Slices of 10 ms since the measurement started that were metered,
    /// computed or skipped, while the log held the meter paused (a
    /// `LoudnessRun` entry), in whole or in part, and are not lost: left
    /// out of the integrated loudness, the loudness range and the maxima,
    /// but not of the momentary and short-term loudness. Neither gaps nor
    /// lost, and equal on all replicas whose `lost` is 0.
    pub paused: u64,
    /// The measurement runs: the last slice metered was not paused, so a
    /// slice the log paused or ran again within reads paused. True before
    /// the first slice: every meter starts running.
    pub running: bool,
    /// The sample index the measurement started at: the first slice
    /// metered whole, or the window a reset applied at. The measurement
    /// restarts only with the process or at a reset; a resync, a rebuild
    /// and a hand-over keep it. A `since` later than the peers' marks a
    /// replica whose measurement restarted.
    pub since: Option<u64>,
}

impl Default for OutputMeters {
    fn default() -> Self {
        Self {
            window: None,
            peak: Vec::new(),
            rms: Vec::new(),
            true_peak: Vec::new(),
            max_true_peak: Vec::new(),
            momentary: None,
            short_term: None,
            max_momentary: None,
            max_short_term: None,
            integrated: None,
            range: None,
            gaps: 0,
            lost: 0,
            paused: 0,
            running: true,
            since: None,
        }
    }
}

/// Meter readings of one input after its strip, as an output's: no
/// true peak, and of the loudness the momentary only.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InputMeters {
    /// The last 100 ms window the readings cover.
    pub window: Option<u64>,
    /// Per channel, the largest absolute sample value of that window.
    pub peak: Vec<f32>,
    /// Per channel, the RMS level of that window, referenced to a sine.
    pub rms: Vec<f32>,
    /// Momentary loudness in LUFS, weighted by the input's
    /// `loudnessChannels`; `None` until 400 ms were measured.
    pub momentary: Option<f64>,
}

impl InputMeters {
    pub(crate) fn update(&mut self, m: &OutputMeters) {
        self.window = m.window;
        self.peak.clone_from(&m.peak);
        self.rms.clone_from(&m.rms);
        self.momentary = m.momentary;
    }
}

/// What a slice's first record makes it, besides its power.
#[derive(Clone, Copy, PartialEq)]
enum Mark {
    Measured,
    Gap,
    Paused,
}

impl Mark {
    /// A slice metered while diverged is measured, so lost, whatever else
    /// it is: what made it a gap or paused there may be what the replica
    /// got wrong.
    fn of(r: &MeterRecord) -> Mark {
        if r.diverged() {
            Mark::Measured
        } else if r.paused() {
            Mark::Paused
        } else if r.gap() {
            Mark::Gap
        } else {
            Mark::Measured
        }
    }

    /// The slice of this mark with weighted power `power`, none when not
    /// all of its samples were measured, or the slice was not computed.
    fn slice(self, power: Option<f64>) -> Slice {
        match self {
            Mark::Gap => Slice::Gap,
            Mark::Paused => Slice::Paused(power.unwrap_or(f64::NAN)),
            Mark::Measured => power.map_or(Slice::Lost, Slice::Power),
        }
    }
}

/// Turns the records of one meter into [`OutputMeters`].
pub(crate) struct OutputLoudness {
    weights: Vec<f64>,
    loudness: Loudness,
    /// The slice being assembled, the weighted power so far, whether all
    /// of its samples were measured, the next channel expected, and its
    /// mark.
    slice: Option<(u64, f64, bool, usize, Mark)>,
    window_peak: Vec<f32>,
    window_true_peak: Vec<f32>,
    /// Per channel, the unweighted sum of squares of the window so far
    /// and the samples it covers.
    window_power: Vec<(f64, u64)>,
    /// The integrated loudness and the loudness range may have changed:
    /// a window of the measured slices ended, or a reset cleared them.
    /// Evaluating them walks every histogram bin, so they are evaluated
    /// only then.
    dirty: bool,
    /// Evaluations of the integrated loudness and the loudness range.
    #[cfg(test)]
    evaluations: u64,
    pub meters: OutputMeters,
}

fn max(a: f32, b: f32) -> f32 {
    if b > a || b.is_nan() { b } else { a }
}

impl OutputLoudness {
    pub fn new(weights: Vec<f64>) -> Self {
        let n = weights.len();
        Self {
            weights,
            loudness: Loudness::new(),
            slice: None,
            window_peak: vec![0.0; n],
            window_true_peak: vec![0.0; n],
            window_power: vec![(0.0, 0); n],
            dirty: false,
            #[cfg(test)]
            evaluations: 0,
            meters: OutputMeters {
                peak: vec![0.0; n],
                rms: vec![0.0; n],
                true_peak: vec![0.0; n],
                max_true_peak: vec![0.0; n],
                ..OutputMeters::default()
            },
        }
    }

    /// Records were lost: the slice being assembled is lost, or a gap or
    /// paused when its first record said so, as it is on every replica.
    /// A slice whose records were all lost counts when the next one
    /// arrives.
    pub fn gap(&mut self) {
        if let Some((slice, .., mark)) = self.slice.take() {
            self.loudness.push(slice, mark.slice(None));
            self.publish();
        }
    }

    pub fn record(&mut self, r: &MeterRecord) {
        let c = r.channel as usize;
        if c == 0 {
            if self.slice.is_some() {
                self.gap();
            }
            if let Some(at) = r.reset() {
                self.loudness.reset(at / SLICE);
                self.dirty = true;
                self.meters.max_true_peak.fill(0.0);
            }
            self.meters.running = !r.paused();
            if r.span() {
                let kind = Mark::of(r).slice(None);
                self.loudness.skip(r.slice, u64::from(r.frames), kind);
                self.publish();
                return;
            }
            self.slice = Some((r.slice, 0.0, !r.diverged(), 0, Mark::of(r)));
        }
        let Some((slice, power, complete, next, _)) = self.slice.as_mut() else {
            return;
        };
        if *slice != r.slice || *next != c {
            self.gap();
            return;
        }
        *power += self.weights[c] * (r.sum_sq / SLICE as f64);
        *complete &= u64::from(r.frames) == SLICE;
        *next += 1;
        self.window_peak[c] = max(self.window_peak[c], r.peak);
        let true_peak = max(r.true_peak, r.peak);
        self.window_true_peak[c] = max(self.window_true_peak[c], true_peak);
        let (sum, frames) = &mut self.window_power[c];
        *sum += r.plain_sum_sq;
        *frames += u64::from(r.frames);
        let m = &mut self.meters.max_true_peak[c];
        *m = max(*m, true_peak);
        if *next < self.weights.len() {
            return;
        }
        let Some((slice, power, complete, _, mark)) = self.slice.take() else {
            return;
        };
        let s = mark.slice(complete.then_some(power));
        self.dirty |= self.loudness.push(slice, s);
        if ((slice + 1) * SLICE).is_multiple_of(WINDOW) {
            self.meters.window = Some(slice * SLICE / WINDOW);
            self.meters.peak.copy_from_slice(&self.window_peak);
            self.meters
                .true_peak
                .copy_from_slice(&self.window_true_peak);
            for (rms, &(sum, frames)) in self.meters.rms.iter_mut().zip(&self.window_power) {
                *rms = if frames == 0 {
                    0.0
                } else {
                    (2.0 * sum / frames as f64).sqrt() as f32
                };
            }
            self.window_peak.fill(0.0);
            self.window_true_peak.fill(0.0);
            self.window_power.fill((0.0, 0));
        }
        self.publish();
    }

    fn publish(&mut self) {
        let l = &self.loudness;
        let m = &mut self.meters;
        m.momentary = l.momentary();
        m.short_term = l.short_term();
        m.max_momentary = l.max_momentary();
        m.max_short_term = l.max_short_term();
        m.gaps = l.gaps();
        m.lost = l.lost();
        m.paused = l.paused();
        m.since = l.start().map(|s| s * SLICE);
        if self.dirty {
            m.integrated = l.integrated();
            m.range = l.range();
            self.dirty = false;
            #[cfg(test)]
            {
                self.evaluations += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt_loop::{GAP, NO_RESET};

    impl MeterRecord {
        fn clone_with_slice(&self, slice: u64) -> MeterRecord {
            MeterRecord { slice, ..*self }
        }
    }

    fn rec(seq: u64, channel: u32, slice: u64, sum_sq: f64) -> MeterRecord {
        MeterRecord {
            seq,
            meter: 0,
            channel,
            slice,
            frames: SLICE as u32,
            peak: 0.5,
            true_peak: 0.6,
            sum_sq,
            plain_sum_sq: 0.0,
            reset_at: NO_RESET,
            flags: 0,
        }
    }

    /// A stereo slice at -23 LUFS: each channel's mean square is half the
    /// power of 10^((-23 + 0.691) / 10).
    fn feed(l: &mut OutputLoudness, slices: std::ops::Range<u64>) {
        let ms = 10f64.powf((-23.0 + 0.691) / 10.0) / 2.0;
        for s in slices {
            for c in 0..2 {
                l.record(&rec(0, c, s, ms * SLICE as f64));
            }
        }
    }

    #[test]
    fn records_give_loudness_and_peaks() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..400);
        let m = &l.meters;
        assert!((m.integrated.unwrap() + 23.0).abs() < 1e-9);
        assert!((m.short_term.unwrap() + 23.0).abs() < 1e-9);
        assert_eq!(
            (m.window, m.since, m.gaps, m.lost),
            (Some(39), Some(0), 0, 0)
        );
        assert_eq!(
            (m.peak[1], m.true_peak[0], m.max_true_peak[1]),
            (0.5, 0.6, 0.6)
        );
    }

    /// A channel record missing in a slice loses the slice: the
    /// integrated loudness and range carry on without it and `lost`
    /// counts it. A reset record restarts the counts from its index.
    #[test]
    fn a_missing_record_is_lost_and_the_measurement_carries_on() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..50);
        l.record(&rec(0, 0, 50, 1.0));
        feed(&mut l, 51..100);
        assert_eq!((l.meters.gaps, l.meters.lost), (0, 1));
        assert!((l.meters.integrated.unwrap() + 23.0).abs() < 1e-9);
        assert_eq!(l.meters.range, None, "less than 3 s measured");
        let mut r = rec(0, 0, 100, 0.0);
        r.reset_at = 100 * SLICE;
        (r.peak, r.true_peak) = (0.1, 0.1);
        l.record(&r);
        l.record(&rec(0, 1, 100, 0.0));
        assert_eq!(
            (l.meters.since, l.meters.lost, l.meters.max_true_peak[0]),
            (Some(48_000), 0, 0.1)
        );
        assert_eq!(l.meters.integrated, None);
        feed(&mut l, 101..141);
        assert!(l.meters.integrated.is_some());
    }

    /// The true peak is at least the sample peak, which the oversampled
    /// peak alone may fall below.
    #[test]
    fn true_peak_is_never_below_the_sample_peak() {
        let mut l = OutputLoudness::new(vec![1.0]);
        for s in 0..10 {
            let mut r = rec(0, 0, s, 0.0);
            (r.peak, r.true_peak) = (0.7, 0.68);
            l.record(&r);
        }
        let m = &l.meters;
        assert_eq!(
            (m.peak[0], m.true_peak[0], m.max_true_peak[0]),
            (0.7, 0.7, 0.7)
        );
    }

    /// RMS is per window and referenced to a sine: a full-scale sine,
    /// mean square 0.5, reads 1.0, one at half scale 0.5. A slice with
    /// fewer samples counts by the samples it has.
    #[test]
    fn rms_reads_a_full_scale_sine_as_one() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        for s in 0..20 {
            for (c, ms) in [(0, 0.5), (1, 0.125)] {
                let mut r = rec(0, c, s, 0.0);
                if s == 15 {
                    r.frames = 240;
                }
                r.plain_sum_sq = ms * f64::from(r.frames);
                l.record(&r);
            }
        }
        assert_eq!(
            (l.meters.window, l.meters.rms.clone()),
            (Some(1), vec![1.0, 0.5])
        );
    }

    /// Integrated loudness and loudness range are evaluated once per
    /// 100 ms window, not per slice, and follow a reset within the slice
    /// that causes it; a lost slice changes neither.
    #[test]
    fn integrated_loudness_is_evaluated_per_window() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..400);
        assert_eq!(l.evaluations, 40);
        let integrated = l.meters.integrated;
        assert!(integrated.is_some());
        let mut short = rec(0, 0, 405, 0.0);
        short.frames = 1;
        feed(&mut l, 400..405);
        l.record(&short);
        l.record(&rec(0, 1, 405, 0.0));
        assert_eq!((l.meters.lost, l.meters.integrated), (1, integrated));
        // A reset mid-window clears them at once.
        let mut r = rec(0, 0, 406, 0.0);
        r.reset_at = 406 * SLICE;
        l.record(&r);
        l.record(&rec(0, 1, 406, 0.0));
        assert_eq!((l.meters.lost, l.meters.integrated), (0, None));
        assert_eq!(l.evaluations, 41);
    }

    /// A slice with fewer samples than its length is lost once the
    /// measurement has started, but not before: the first slice of a
    /// stream is the start.
    #[test]
    fn an_incomplete_slice_is_lost_after_the_start() {
        let mut l = OutputLoudness::new(vec![1.0]);
        let mut first = rec(0, 0, 7, 0.0);
        first.frames = 100;
        l.record(&first);
        l.record(&rec(0, 0, 8, 1.0));
        assert_eq!((l.meters.since, l.meters.lost), (Some(8 * SLICE), 0));
        let mut short = rec(0, 0, 9, 1.0);
        short.frames = 479;
        l.record(&short);
        assert_eq!((l.meters.gaps, l.meters.lost), (0, 1));
    }

    /// A slice the RT thread flagged a gap counts in `gaps`, complete or
    /// not, and is left out of the integrated loudness as a lost one is;
    /// each slice index that never arrives and each slice whose records
    /// a lost sequence cut counts in `lost`.
    #[test]
    fn gaps_and_lost_slices_count_per_slice() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..100);
        let integrated = l.meters.integrated;
        for s in 100..103 {
            for c in 0..2 {
                // Loud, which a slice kept would move the level by.
                let mut r = rec(0, c, s, 100.0 * SLICE as f64);
                r.flags = GAP;
                r.frames = if s == 102 { 10 } else { SLICE as u32 };
                l.record(&r);
            }
        }
        assert_eq!((l.meters.gaps, l.meters.lost), (3, 0));
        // Slices 103 to 105 never arrive; 106 is cut by lost records.
        l.record(&rec(0, 0, 106, 1.0));
        l.gap();
        l.gap();
        assert_eq!((l.meters.gaps, l.meters.lost), (3, 4));
        feed(&mut l, 107..110);
        assert_eq!(l.meters.integrated, integrated);
        assert_eq!((l.meters.gaps, l.meters.lost), (3, 4));
    }

    /// A slice whose first record flags it a gap stays a gap when the
    /// rest of its records are lost: its peers count it a gap whatever
    /// this replica lost of it.
    #[test]
    fn a_gap_slice_cut_by_lost_records_is_a_gap() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..100);
        let mut r = rec(0, 0, 100, 1.0);
        r.flags = GAP;
        l.record(&r);
        l.gap();
        assert_eq!((l.meters.gaps, l.meters.lost), (1, 0));
        // Cut by the next slice's first record arriving instead.
        l.record(&r.clone_with_slice(101));
        l.record(&rec(0, 0, 102, 1.0));
        assert_eq!((l.meters.gaps, l.meters.lost), (2, 0));
    }

    /// A span record counts every slice it covers at once, as gaps with
    /// the gap flag and else as lost, and the measurement carries on after
    /// it; one carrying a reset restarts the measurement at its index.
    #[test]
    fn a_span_counts_its_slices() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..100);
        let integrated = l.meters.integrated;
        let span = |slice, n, flags| MeterRecord {
            frames: n,
            flags: crate::rt_loop::SPAN | flags,
            ..rec(0, 0, slice, 0.0)
        };
        l.record(&span(100, 7, GAP));
        l.record(&span(107, 3, 0));
        assert_eq!((l.meters.gaps, l.meters.lost), (7, 3));
        feed(&mut l, 110..120);
        assert_eq!((l.meters.gaps, l.meters.lost), (7, 3));
        assert_eq!(l.meters.integrated, integrated);
        let mut reset = span(120, 5, GAP);
        reset.reset_at = 120 * SLICE;
        l.record(&reset);
        assert_eq!(
            (l.meters.gaps, l.meters.lost, l.meters.since),
            (5, 0, Some(120 * SLICE))
        );
    }

    /// A slice metered while diverged is lost even when it is flagged a
    /// gap, whole, cut by lost records or skipped: the entry that made it
    /// a gap may be one the log voided, so it counts on this replica
    /// alone and its peers may not count it.
    #[test]
    fn a_diverged_gap_slice_is_lost() {
        use crate::rt_loop::{DIVERGED, SPAN};
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..100);
        for c in 0..2 {
            let mut r = rec(0, c, 100, 1.0);
            r.flags = GAP | DIVERGED;
            l.record(&r);
        }
        let mut r = rec(0, 0, 101, 1.0);
        r.flags = GAP | DIVERGED;
        l.record(&r);
        l.gap();
        l.record(&MeterRecord {
            frames: 4,
            flags: SPAN | GAP | DIVERGED,
            ..rec(0, 0, 102, 0.0)
        });
        assert_eq!((l.meters.gaps, l.meters.lost), (0, 6));
    }

    /// Stereo records of slice `s` at `level` LUFS with `flags`.
    fn slice_at(l: &mut OutputLoudness, s: u64, level: f64, flags: u8) {
        let ms = 10f64.powf((level + 0.691) / 10.0) / 2.0;
        for c in 0..2 {
            let mut r = rec(0, c, s, ms * SLICE as f64);
            r.flags = flags;
            l.record(&r);
        }
    }

    /// A slice metered while paused is left out of the integrated
    /// loudness and the range however loud, and counts in `paused`, not
    /// in `gaps` or `lost`, gap flag or not; the momentary and short-term
    /// loudness follow it. The status reads the meter paused from the
    /// first paused slice and running from the first one after.
    #[test]
    fn a_paused_slice_is_counted_apart_and_left_out() {
        use crate::rt_loop::PAUSED;
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..400);
        let (integrated, range) = (l.meters.integrated, l.meters.range);
        assert!(l.meters.running);
        for s in 400..800 {
            let flags = if s % 7 == 0 { PAUSED | GAP } else { PAUSED };
            slice_at(&mut l, s, -10.0, flags);
        }
        let m = &l.meters;
        assert_eq!((m.gaps, m.lost, m.paused, m.running), (0, 0, 400, false));
        assert_eq!((m.integrated, m.range), (integrated, range));
        assert!((m.momentary.unwrap() + 10.0).abs() < 1e-9);
        assert!((m.short_term.unwrap() + 10.0).abs() < 1e-9);
        assert!((m.max_momentary.unwrap() + 23.0).abs() < 1e-9);
        feed(&mut l, 800..801);
        assert!(l.meters.running);
        assert_eq!(l.meters.integrated, integrated);
    }

    /// Diverged wins over paused, as it does over a gap: a paused slice
    /// metered while diverged is lost, whole, cut by lost records or
    /// skipped. Without it a paused slice stays paused when its records
    /// are cut or it was skipped, as it is on every replica.
    #[test]
    fn a_diverged_paused_slice_is_lost() {
        use crate::rt_loop::{DIVERGED, PAUSED, SPAN};
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..100);
        slice_at(&mut l, 100, -23.0, PAUSED | DIVERGED);
        let mut r = rec(0, 0, 101, 1.0);
        r.flags = PAUSED | GAP | DIVERGED;
        l.record(&r);
        l.gap();
        l.record(&MeterRecord {
            frames: 4,
            flags: SPAN | PAUSED | DIVERGED,
            ..rec(0, 0, 102, 0.0)
        });
        assert_eq!((l.meters.gaps, l.meters.lost, l.meters.paused), (0, 6, 0));
        r.flags = PAUSED;
        l.record(&r.clone_with_slice(106));
        l.gap();
        l.record(&MeterRecord {
            frames: 3,
            flags: SPAN | PAUSED | GAP,
            ..rec(0, 0, 107, 0.0)
        });
        let mut short = rec(0, 0, 110, 1.0);
        (short.frames, short.flags) = (100, PAUSED);
        l.record(&short);
        l.record(&MeterRecord {
            frames: 100,
            ..rec(0, 1, 110, 1.0)
        });
        assert_eq!((l.meters.gaps, l.meters.lost, l.meters.paused), (0, 6, 5));
    }
}
