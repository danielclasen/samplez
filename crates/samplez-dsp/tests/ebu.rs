//! The minimum-requirement cases of EBU Tech 3341 V4 (2023) Table 1 and
//! EBU Tech 3342 V4 (2023) Table 1, with the test signals generated from
//! their definitions there. The authentic programme cases (3341 cases 7
//! and 8, 3342 cases 5 and 6) are recordings and are not covered.

use samplez_dsp::{KWeighting, Loudness, Path, TruePeak, restore_mxcsr, set_ftz_daz, weight};
use std::f64::consts::PI;

const FS: f64 = 48_000.0;
const SLICE: usize = 480;

/// A meter as the engine runs one: K-weighting and true peak per channel,
/// the weighted mean square per 10 ms slice into [`Loudness`].
struct Meter {
    kw: KWeighting,
    tp: TruePeak,
    weights: Vec<f64>,
    loudness: Loudness,
    slice: u64,
    max_true_peak: f32,
}

impl Meter {
    fn new(weights: &[f64]) -> Self {
        Self {
            kw: KWeighting::new(weights.len()),
            tp: TruePeak::new(weights.len(), SLICE),
            weights: weights.to_vec(),
            loudness: Loudness::new(),
            slice: 0,
            max_true_peak: 0.0,
        }
    }

    fn stereo() -> Self {
        Self::new(&[weight::FRONT; 2])
    }

    /// Feeds `signal` (one vector per channel, a whole number of slices),
    /// calling `each` after every slice.
    fn feed(&mut self, signal: &[Vec<f32>], mut each: impl FnMut(&Loudness)) {
        let len = signal[0].len();
        assert_eq!(len % SLICE, 0);
        for s in 0..len / SLICE {
            let mut power = 0.0;
            for (ch, x) in signal.iter().enumerate() {
                let x = &x[s * SLICE..(s + 1) * SLICE];
                power += self.weights[ch] * (self.kw.process(ch, x) / SLICE as f64);
                self.max_true_peak = self.max_true_peak.max(self.tp.process(ch, x));
            }
            self.loudness.push(self.slice, Some(power));
            self.slice += 1;
            each(&self.loudness);
        }
    }
}

/// A 1 kHz sine at `dbfs` peak level for `secs`, or silence for `None`;
/// the phase runs on across segments.
fn tones(segments: &[(Option<f64>, f64)]) -> Vec<f32> {
    let mut out = Vec::new();
    for &(level, secs) in segments {
        let n = (secs * FS).round() as usize;
        for _ in 0..n {
            let t = out.len() as f64 / FS;
            let a = level.map_or(0.0, |db| 10f64.powf(db / 20.0));
            out.push((a * (2.0 * PI * 1000.0 * t).sin()) as f32);
        }
    }
    out
}

fn stereo(x: Vec<f32>) -> Vec<Vec<f32>> {
    vec![x.clone(), x]
}

fn near(v: Option<f64>, expect: f64, tol: f64) -> bool {
    v.is_some_and(|v| (v - expect).abs() <= tol)
}

/// Tech 3341 cases 1 and 2: M, S and I of a steady stereo tone.
#[test]
fn tech3341_cases_1_2_steady_tone() {
    for level in [-23.0, -33.0] {
        let mut m = Meter::stereo();
        m.feed(&stereo(tones(&[(Some(level), 20.0)])), |_| {});
        let l = &m.loudness;
        for (name, v) in [
            ("M", l.momentary()),
            ("S", l.short_term()),
            ("I", l.integrated()),
        ] {
            assert!(near(v, level, 0.1), "{level} dBFS: {name} = {v:?}");
        }
    }
}

/// Tech 3341 cases 3 to 6: I through the gates and with surround weights.
#[test]
fn tech3341_cases_3_to_6_integrated() {
    let cases: [&[(Option<f64>, f64)]; 3] = [
        &[
            (Some(-36.0), 10.0),
            (Some(-23.0), 60.0),
            (Some(-36.0), 10.0),
        ],
        &[
            (Some(-72.0), 10.0),
            (Some(-36.0), 10.0),
            (Some(-23.0), 60.0),
            (Some(-36.0), 10.0),
            (Some(-72.0), 10.0),
        ],
        &[
            (Some(-26.0), 20.0),
            (Some(-20.0), 20.1),
            (Some(-26.0), 20.0),
        ],
    ];
    for (i, segments) in cases.iter().enumerate() {
        let mut m = Meter::stereo();
        m.feed(&stereo(tones(segments)), |_| {});
        let v = m.loudness.integrated();
        assert!(near(v, -23.0, 0.1), "case {}: I = {v:?}", i + 3);
    }
    // Case 6: 5.0 channels L, R, C, Ls, Rs.
    let w = [
        weight::FRONT,
        weight::FRONT,
        weight::FRONT,
        weight::SURROUND,
        weight::SURROUND,
    ];
    let mut m = Meter::new(&w);
    let signal: Vec<Vec<f32>> = [-28.0, -28.0, -24.0, -30.0, -30.0]
        .iter()
        .map(|&db| tones(&[(Some(db), 20.0)]))
        .collect();
    m.feed(&signal, |_| {});
    let v = m.loudness.integrated();
    assert!(near(v, -23.0, 0.1), "case 6: I = {v:?}");
}

/// Tech 3341 cases 9 and 12: S and M constant on a signal whose period is
/// the window length.
#[test]
fn tech3341_cases_9_12_sliding_windows() {
    let mut s9 = Vec::new();
    for _ in 0..5 {
        s9.extend([(Some(-20.0), 1.34), (Some(-30.0), 1.66)]);
    }
    let mut m = Meter::stereo();
    let mut n = 0;
    m.feed(&stereo(tones(&s9)), |l| {
        n += 1;
        if n > 300 {
            assert!(
                near(l.short_term(), -23.0, 0.1),
                "case 9: S = {:?}",
                l.short_term()
            );
        }
    });
    let mut s12 = Vec::new();
    for _ in 0..25 {
        s12.extend([(Some(-20.0), 0.18), (Some(-30.0), 0.22)]);
    }
    let mut m = Meter::stereo();
    let mut n = 0;
    m.feed(&stereo(tones(&s12)), |l| {
        n += 1;
        if n > 100 {
            assert!(
                near(l.momentary(), -23.0, 0.1),
                "case 12: M = {:?}",
                l.momentary()
            );
        }
    });
}

/// Tech 3341 cases 10 and 13, file-based: each segment measured from a
/// reset reads its maximum S, or M, at -23.
#[test]
fn tech3341_cases_10_13_maxima_per_file() {
    for i in 0..20 {
        let mut m = Meter::stereo();
        let s = tones(&[(None, i as f64 * 0.15), (Some(-23.0), 3.0), (None, 1.0)]);
        m.feed(&stereo(s), |_| {});
        let v = m.loudness.max_short_term();
        assert!(near(v, -23.0, 0.1), "case 10 segment {i}: max S = {v:?}");
        let mut m = Meter::stereo();
        let s = tones(&[(None, i as f64 * 0.02), (Some(-23.0), 0.4), (None, 1.0)]);
        m.feed(&stereo(s), |_| {});
        let v = m.loudness.max_momentary();
        assert!(near(v, -23.0, 0.1), "case 13 segment {i}: max M = {v:?}");
    }
}

/// Tech 3341 cases 11 and 14, live: one signal of 20 segments; the
/// maximum of S, or M, over each segment is -38 + i.
#[test]
fn tech3341_cases_11_14_live_maxima() {
    for (case, tone, segment) in [(11, 3.0, 6.0), (14, 0.4, 0.8)] {
        let step = if case == 11 { 0.15 } else { 0.02 };
        let mut segments = Vec::new();
        for i in 0..20 {
            let lead = i as f64 * step;
            segments.extend([
                (None, lead),
                (Some(-38.0 + i as f64), tone),
                (None, segment - tone - lead),
            ]);
        }
        let per_segment = (segment * 100.0).round() as usize;
        let mut maxima = [f64::NEG_INFINITY; 20];
        let mut n = 0;
        let mut m = Meter::stereo();
        m.feed(&stereo(tones(&segments)), |l| {
            let v = if case == 11 {
                l.short_term()
            } else {
                l.momentary()
            };
            if let Some(v) = v {
                let seg = n / per_segment;
                maxima[seg] = maxima[seg].max(v);
            }
            n += 1;
        });
        for (i, &v) in maxima.iter().enumerate() {
            let expect = -38.0 + i as f64;
            assert!((v - expect).abs() <= 0.1, "case {case} segment {i}: {v}");
        }
    }
}

/// A stereo sine at `freq` Hz of `amplitude` and `phase_deg`, one second
/// long, with 10 ms raised-cosine fades.
fn tp_sine(freq: f64, amplitude: f64, phase_deg: f64) -> Vec<f32> {
    let n = 48_000;
    let fade = 480;
    (0..n)
        .map(|i| {
            let ramp = if i < fade {
                0.5 - 0.5 * (PI * i as f64 / fade as f64).cos()
            } else if i >= n - fade {
                0.5 - 0.5 * (PI * (n - i) as f64 / fade as f64).cos()
            } else {
                1.0
            };
            let x = amplitude * (2.0 * PI * freq * i as f64 / FS + phase_deg.to_radians()).sin();
            (ramp * x) as f32
        })
        .collect()
}

/// Tech 3341 cases 20 to 23: at 192 kHz a 8 kHz sine of amplitude 0.5
/// with a single period of 12 kHz at amplitude 1.0 inserted at a zero
/// crossing, low-pass filtered and taken at every fourth sample from
/// `offset`.
fn tp_burst(offset: usize) -> Vec<f32> {
    let fs4 = 4.0 * FS;
    let (lead, total) = (24 * 2000, 24 * 4000);
    let mut x = Vec::with_capacity(total);
    while x.len() < total {
        let i = x.len();
        if i >= lead && i < lead + 16 {
            x.push((2.0 * PI * 12_000.0 * (i - lead) as f64 / fs4).sin());
        } else {
            let j = if i < lead { i } else { i - 16 };
            x.push(0.5 * (2.0 * PI * 8000.0 * j as f64 / fs4).sin());
        }
    }
    // Fades of 1000 samples at 192 kHz.
    for i in 0..1000 {
        let g = 0.5 - 0.5 * (PI * i as f64 / 1000.0).cos();
        x[i] *= g;
        x[total - 1 - i] *= g;
    }
    // Anti-aliasing: Blackman-windowed sinc, cut-off 22 kHz, 1023 taps.
    let taps = 1023;
    let fc = 22_000.0 / fs4;
    let h: Vec<f64> = (0..taps)
        .map(|k| {
            let m = k as f64 - (taps / 2) as f64;
            let sinc = if m == 0.0 {
                2.0 * fc
            } else {
                (2.0 * PI * fc * m).sin() / (PI * m)
            };
            let w = 0.42 - 0.5 * (2.0 * PI * k as f64 / (taps - 1) as f64).cos()
                + 0.08 * (4.0 * PI * k as f64 / (taps - 1) as f64).cos();
            sinc * w
        })
        .collect();
    let dc: f64 = h.iter().sum();
    (offset..total)
        .step_by(4)
        .map(|i| {
            let mut acc = 0.0;
            for (k, &c) in h.iter().enumerate() {
                if let Some(j) = (i + taps / 2).checked_sub(k).filter(|&j| j < total) {
                    acc += c * x[j];
                }
            }
            (acc / dc) as f32
        })
        .collect()
}

fn dbtp(x: &[f32], path: Path) -> (f64, f64) {
    let saved = set_ftz_daz(true);
    let mut tp = TruePeak::new(1, x.len());
    let peak = tp.process_on(path, 0, x);
    restore_mxcsr(saved);
    let sample = x.iter().fold(0.0f32, |p, s| p.max(s.abs()));
    (
        20.0 * f64::from(peak).log10(),
        20.0 * f64::from(sample).log10(),
    )
}

/// Tech 3341 cases 15 to 23: maximum true-peak level within +0.2/-0.4
/// dB of the expected one, on every path. The intersample cases read low
/// on the sample peak, which the assertion on it shows.
#[test]
fn tech3341_cases_15_to_23_true_peak() {
    let mut cases: Vec<(usize, Vec<f32>, f64)> = vec![
        (15, tp_sine(12_000.0, 0.5, 0.0), -6.0),
        (16, tp_sine(12_000.0, 0.5, 45.0), -6.0),
        (17, tp_sine(8000.0, 0.5, 60.0), -6.0),
        (18, tp_sine(6000.0, 0.5, 67.5), -6.0),
        (19, tp_sine(12_000.0, 1.41, 45.0), 3.0),
    ];
    for offset in 0..4 {
        cases.push((20 + offset, tp_burst(offset), 0.0));
    }
    for path in Path::ALL.into_iter().filter(|p| p.is_supported()) {
        for (case, x, expect) in &cases {
            let (tp, sample) = dbtp(x, path);
            assert!(
                tp <= expect + 0.2 && tp >= expect - 0.4,
                "case {case} {path:?}: {tp:.3} dBTP"
            );
            if *case == 16 || *case == 19 {
                assert!(
                    sample < expect - 2.5,
                    "case {case}: sample peak {sample:.3}"
                );
            }
        }
    }
}

/// Tech 3342 cases 1 to 4: LRA of tone sequences, 20 s per tone.
#[test]
fn tech3342_cases_1_to_4_loudness_range() {
    let cases: [(&[f64], f64); 4] = [
        (&[-20.0, -30.0], 10.0),
        (&[-20.0, -15.0], 5.0),
        (&[-40.0, -20.0], 20.0),
        (&[-50.0, -35.0, -20.0, -35.0, -50.0], 15.0),
    ];
    for (i, (levels, expect)) in cases.iter().enumerate() {
        let segments: Vec<_> = levels.iter().map(|&db| (Some(db), 20.0)).collect();
        let mut m = Meter::stereo();
        m.feed(&stereo(tones(&segments)), |_| {});
        let v = m.loudness.range();
        assert!(
            near(v, *expect, 1.0),
            "Tech 3342 case {}: LRA = {v:?}",
            i + 1
        );
    }
}

/// The loudness of one signal fed twice gives the same bits: nothing in
/// the measurement depends on state outside its inputs.
#[test]
fn measurement_is_deterministic() {
    let s = stereo(tones(&[(Some(-20.0), 5.0), (Some(-31.0), 5.0)]));
    let run = || {
        let mut m = Meter::stereo();
        m.feed(&s, |_| {});
        let l = &m.loudness;
        (
            l.integrated().map(f64::to_bits),
            l.range().map(f64::to_bits),
            l.short_term().map(f64::to_bits),
            m.max_true_peak.to_bits(),
        )
    };
    assert_eq!(run(), run());
}
