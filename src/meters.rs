//! The supervisor's side of output metering: meter records from the RT
//! thread become per output peaks, true peaks and loudness.
//!
//! Meters do not feed the output, so they are outside the output hashes
//! of plan 2.11. They are a pure function of the output samples since
//! the last loudness reset, which applies at a sample index: replicas
//! that compute the same output and lose no record report the same
//! values from the reset on. Before the first reset each measures from
//! its own start.

use samplez_dsp::{Loudness, WINDOW};

use crate::rt_loop::{MeterRecord, SLICE};

/// Meter readings of one output.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutputMeters {
    /// The last 100 ms window the readings cover, the samples
    /// `[window * 4800, (window + 1) * 4800)`.
    pub window: Option<u64>,
    /// Per channel, the largest absolute sample value of that window,
    /// full scale 1.0.
    pub peak: Vec<f32>,
    /// Per channel, the true peak of that window per ITU-R BS.1770-5
    /// Annex 2, full scale 1.0.
    pub true_peak: Vec<f32>,
    /// Per channel, the largest true peak since the measurement started.
    pub max_true_peak: Vec<f32>,
    /// Loudness in LUFS, `None` until enough audio has been measured.
    pub momentary: Option<f64>,
    pub short_term: Option<f64>,
    pub max_momentary: Option<f64>,
    pub max_short_term: Option<f64>,
    /// Gated per ITU-R BS.1770-5; `None` while not `valid`.
    pub integrated: Option<f64>,
    /// Loudness range per EBU Tech 3342, LU; `None` while not `valid`.
    pub range: Option<f64>,
    /// No meter record lost, no samples left unmeasured and no infinite
    /// or NaN loudness power since the measurement started.
    pub valid: bool,
    /// The sample index the measurement started at: the first slice
    /// metered, or the window a reset applied at.
    pub since: Option<u64>,
}

/// Turns the records of one output into [`OutputMeters`].
pub(crate) struct OutputLoudness {
    weights: Vec<f64>,
    loudness: Loudness,
    /// The slice being assembled, the weighted power so far, whether all
    /// of its samples were measured, and the next channel expected.
    slice: Option<(u64, f64, bool, usize)>,
    window_peak: Vec<f32>,
    window_true_peak: Vec<f32>,
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
            meters: OutputMeters {
                peak: vec![0.0; n],
                true_peak: vec![0.0; n],
                max_true_peak: vec![0.0; n],
                valid: true,
                ..OutputMeters::default()
            },
        }
    }

    /// Records were lost: the slice being assembled is dropped and the
    /// loudness has a gap.
    pub fn gap(&mut self) {
        self.slice = None;
        self.loudness.gap();
        self.publish();
    }

    pub fn record(&mut self, r: &MeterRecord) {
        let c = r.channel as usize;
        if c == 0 {
            if self.slice.is_some() {
                self.gap();
            }
            if r.reset {
                self.loudness.reset();
                self.meters.max_true_peak.fill(0.0);
                self.meters.since = Some(r.slice * SLICE);
            }
            self.meters.since.get_or_insert(r.slice * SLICE);
            self.slice = Some((r.slice, 0.0, true, 0));
        }
        let Some((slice, power, complete, next)) = self.slice.as_mut() else {
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
        self.window_true_peak[c] = max(self.window_true_peak[c], r.true_peak);
        let m = &mut self.meters.max_true_peak[c];
        *m = max(*m, r.true_peak);
        if *next < self.weights.len() {
            return;
        }
        let (slice, power, complete) = (*slice, *power, *complete);
        self.slice = None;
        self.loudness.push(slice, complete.then_some(power));
        if ((slice + 1) * SLICE).is_multiple_of(WINDOW) {
            self.meters.window = Some(slice * SLICE / WINDOW);
            self.meters.peak.copy_from_slice(&self.window_peak);
            self.meters
                .true_peak
                .copy_from_slice(&self.window_true_peak);
            self.window_peak.fill(0.0);
            self.window_true_peak.fill(0.0);
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
        m.valid = l.valid();
        m.integrated = l.integrated();
        m.range = l.range();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64, channel: u32, slice: u64, sum_sq: f64) -> MeterRecord {
        MeterRecord {
            seq,
            output: 0,
            channel,
            slice,
            frames: SLICE as u32,
            peak: 0.5,
            true_peak: 0.6,
            sum_sq,
            reset: false,
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
        assert_eq!((m.window, m.since, m.valid), (Some(39), Some(0), true));
        assert_eq!(
            (m.peak[1], m.true_peak[0], m.max_true_peak[1]),
            (0.5, 0.6, 0.6)
        );
    }

    /// A channel record missing in a slice is a gap: integrated loudness
    /// and range turn invalid; a reset record makes them valid again.
    #[test]
    fn a_missing_record_invalidates_until_reset() {
        let mut l = OutputLoudness::new(vec![1.0, 1.0]);
        feed(&mut l, 0..50);
        l.record(&rec(0, 0, 50, 1.0));
        feed(&mut l, 51..100);
        assert!(!l.meters.valid);
        assert_eq!((l.meters.integrated, l.meters.range), (None, None));
        let mut r = rec(0, 0, 100, 0.0);
        r.reset = true;
        r.true_peak = 0.1;
        l.record(&r);
        l.record(&rec(0, 1, 100, 0.0));
        assert!(l.meters.valid);
        assert_eq!(
            (l.meters.since, l.meters.max_true_peak[0]),
            (Some(48_000), 0.1)
        );
        feed(&mut l, 101..141);
        assert!(l.meters.integrated.is_some());
    }

    /// A slice with fewer samples than its length is a gap once the
    /// measurement has started, but not before.
    #[test]
    fn an_incomplete_slice_is_a_gap_after_the_start() {
        let mut l = OutputLoudness::new(vec![1.0]);
        let mut first = rec(0, 0, 7, 0.0);
        first.frames = 100;
        l.record(&first);
        l.record(&rec(0, 0, 8, 1.0));
        assert!(l.meters.valid);
        let mut short = rec(0, 0, 9, 1.0);
        short.frames = 479;
        l.record(&short);
        assert!(!l.meters.valid);
    }
}
