//! The input channel strip: trim, per-leg trim and polarity, high-pass
//! filter and a four-band equaliser, in that order, over the legs of one
//! input of width 1 to [`MAX_LEGS`].
//!
//! Every parameter is set by a [`StripParam`], an absolute value, so one
//! applied again changes nothing. A change takes effect at the next
//! sample processed and ramps over the strip's ramp length: gains
//! linearly in the gain domain ([`crate::Ramp`]), a polarity inversion
//! through zero, filters by their coefficients. The ranges are those the
//! faderz model gives its strip controls.
//!
//! The strip holds no heap memory, so a clone allocates nothing. It runs
//! one leg after the other with scalar code, the same operations in the
//! same order on every host.

use core::ops::RangeInclusive;

use crate::Ramp;
use crate::svf::{Shape, Svf};

/// Most legs a strip carries: 7.1.
pub const MAX_LEGS: usize = 8;
/// Bands of the equaliser.
pub const EQ_BANDS: usize = 4;

/// Trim and per-leg trim, dB.
pub const TRIM_DB: RangeInclusive<f32> = -20.0..=20.0;
/// High-pass frequency, Hz.
pub const HPF_FREQ: RangeInclusive<f32> = 20.0..=500.0;
/// Equaliser band frequency, Hz.
pub const EQ_FREQ: RangeInclusive<f32> = 20.0..=20_000.0;
/// Equaliser band gain, dB.
pub const EQ_GAIN_DB: RangeInclusive<f32> = -18.0..=18.0;
/// Equaliser band Q.
pub const EQ_Q: RangeInclusive<f32> = 0.1..=16.0;

/// The response of an equaliser band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BandType {
    /// Shelf below `freq`, half its gain at `freq` (RBJ lowShelf).
    LowShelf,
    /// Bell around `freq` (RBJ peakingEQ).
    Peak,
    /// Shelf above `freq`, half its gain at `freq` (RBJ highShelf).
    HighShelf,
}

/// One equaliser band. A band at 0 dB passes its input unchanged.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EqBand {
    pub kind: BandType,
    /// Hz, within [`EQ_FREQ`].
    pub freq: f32,
    /// dB, within [`EQ_GAIN_DB`].
    pub gain_db: f32,
    /// Within [`EQ_Q`].
    pub q: f32,
}

impl EqBand {
    fn check(&self) -> Result<(), String> {
        within("eq band freq", self.freq, &EQ_FREQ)?;
        within("eq band gain", self.gain_db, &EQ_GAIN_DB)?;
        within("eq band q", self.q, &EQ_Q)
    }

    fn shape(&self) -> Shape {
        let (freq, gain_db, q) = (self.freq, self.gain_db, self.q);
        match self.kind {
            BandType::LowShelf => Shape::LowShelf { freq, gain_db, q },
            BandType::Peak => Shape::Bell { freq, gain_db, q },
            BandType::HighShelf => Shape::HighShelf { freq, gain_db, q },
        }
    }
}

/// One parameter of a strip, set to an absolute value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StripParam {
    /// Gain of every leg, dB within [`TRIM_DB`].
    Trim { db: f32 },
    /// Gain of leg `leg` on top of the trim, dB within [`TRIM_DB`].
    LegTrim { leg: usize, db: f32 },
    /// Inverts leg `leg`, or not.
    Polarity { leg: usize, inverted: bool },
    /// The 12 dB per octave Butterworth high-pass, -3 dB at `freq` Hz
    /// within [`HPF_FREQ`].
    Hpf { enabled: bool, freq: f32 },
    /// Switches the equaliser; a switched-off one passes its input.
    Eq { enabled: bool },
    /// Band `band` of the equaliser, bands ordered low to high.
    EqBand { band: usize, value: EqBand },
}

fn within(what: &str, v: f32, range: &RangeInclusive<f32>) -> Result<(), String> {
    if range.contains(&v) {
        Ok(())
    } else {
        Err(format!(
            "{what} {v} is outside {}..={}",
            range.start(),
            range.end()
        ))
    }
}

impl StripParam {
    /// Whether a strip of `legs` legs takes the parameter: legs and bands
    /// that exist, values within their ranges, no NaN.
    pub fn check(&self, legs: usize) -> Result<(), String> {
        let leg_exists = |leg: usize| {
            if leg < legs {
                Ok(())
            } else {
                Err(format!("no leg {leg} on a strip of {legs}"))
            }
        };
        match *self {
            StripParam::Trim { db } => within("trim", db, &TRIM_DB),
            StripParam::LegTrim { leg, db } => {
                leg_exists(leg)?;
                within("leg trim", db, &TRIM_DB)
            }
            StripParam::Polarity { leg, .. } => leg_exists(leg),
            StripParam::Hpf { freq, .. } => within("hpf freq", freq, &HPF_FREQ),
            StripParam::Eq { .. } => Ok(()),
            StripParam::EqBand { band, value } => {
                if band >= EQ_BANDS {
                    return Err(format!("no eq band {band}"));
                }
                value.check()
            }
        }
    }
}

/// The bands of a strip before any is set: shelves at 100 Hz and 8 kHz,
/// bells at 400 Hz and 2.5 kHz, all at 0 dB.
pub const DEFAULT_BANDS: [EqBand; EQ_BANDS] = [
    EqBand {
        kind: BandType::LowShelf,
        freq: 100.0,
        gain_db: 0.0,
        q: 0.707,
    },
    EqBand {
        kind: BandType::Peak,
        freq: 400.0,
        gain_db: 0.0,
        q: 0.707,
    },
    EqBand {
        kind: BandType::Peak,
        freq: 2500.0,
        gain_db: 0.0,
        q: 0.707,
    },
    EqBand {
        kind: BandType::HighShelf,
        freq: 8000.0,
        gain_db: 0.0,
        q: 0.707,
    },
];

/// One input's strip. Before any parameter is set it passes its input
/// unchanged: trims at 0 dB, no leg inverted, high-pass and equaliser
/// off, the bands at [`DEFAULT_BANDS`], the high-pass at 80 Hz.
#[derive(Clone, Debug, PartialEq)]
pub struct Strip {
    legs: usize,
    ramp: u32,
    trim_db: f32,
    leg_trim_db: [f32; MAX_LEGS],
    inverted: [bool; MAX_LEGS],
    gain: [Ramp; MAX_LEGS],
    hpf_enabled: bool,
    hpf_freq: f32,
    hpf: Svf<MAX_LEGS>,
    eq_enabled: bool,
    bands: [EqBand; EQ_BANDS],
    eq: [Svf<MAX_LEGS>; EQ_BANDS],
}

impl Strip {
    /// A strip of `legs` legs whose changes ramp over `ramp` samples.
    ///
    /// # Panics
    ///
    /// If `legs` is 0 or above [`MAX_LEGS`].
    pub fn new(legs: usize, ramp: u32) -> Strip {
        assert!(
            (1..=MAX_LEGS).contains(&legs),
            "a strip has 1 to {MAX_LEGS} legs, not {legs}"
        );
        Strip {
            legs,
            ramp,
            trim_db: 0.0,
            leg_trim_db: [0.0; MAX_LEGS],
            inverted: [false; MAX_LEGS],
            gain: [Ramp::new(1.0); MAX_LEGS],
            hpf_enabled: false,
            hpf_freq: 80.0,
            hpf: Svf::new(),
            eq_enabled: false,
            bands: DEFAULT_BANDS,
            eq: [Svf::new(); EQ_BANDS],
        }
    }

    pub fn legs(&self) -> usize {
        self.legs
    }

    /// Sets `param` from the next sample processed on, ramping over the
    /// strip's ramp length. Returns false and changes nothing when
    /// [`StripParam::check`] refuses it.
    #[must_use]
    pub fn set(&mut self, param: StripParam) -> bool {
        if param.check(self.legs).is_err() {
            return false;
        }
        match param {
            StripParam::Trim { db } => {
                self.trim_db = db;
                for leg in 0..self.legs {
                    self.retarget_gain(leg);
                }
            }
            StripParam::LegTrim { leg, db } => {
                self.leg_trim_db[leg] = db;
                self.retarget_gain(leg);
            }
            StripParam::Polarity { leg, inverted } => {
                self.inverted[leg] = inverted;
                self.retarget_gain(leg);
            }
            StripParam::Hpf { enabled, freq } => {
                self.hpf_enabled = enabled;
                self.hpf_freq = freq;
                let shape = if enabled {
                    Shape::HighPass { freq }
                } else {
                    Shape::Identity
                };
                self.hpf.set(shape, self.ramp);
            }
            StripParam::Eq { enabled } => {
                self.eq_enabled = enabled;
                for band in 0..EQ_BANDS {
                    self.retarget_band(band);
                }
            }
            StripParam::EqBand { band, value } => {
                self.bands[band] = value;
                self.retarget_band(band);
            }
        }
        true
    }

    fn retarget_gain(&mut self, leg: usize) {
        let db = f64::from(self.trim_db) + f64::from(self.leg_trim_db[leg]);
        let g = libm::pow(10.0, db / 20.0) as f32;
        let g = if self.inverted[leg] { -g } else { g };
        // Within +-40 dB, so within MAX_GAIN.
        let _ = self.gain[leg].set_target(g, self.ramp);
    }

    fn retarget_band(&mut self, band: usize) {
        let shape = if self.eq_enabled {
            self.bands[band].shape()
        } else {
            Shape::Identity
        };
        self.eq[band].set(shape, self.ramp);
    }

    /// Processes `frames` samples of each leg in place: `buf` holds the
    /// legs one after the other, `frames` samples each.
    ///
    /// # Panics
    ///
    /// If `buf` is shorter than `legs * frames`.
    pub fn process(&mut self, buf: &mut [f32], frames: usize) {
        let buf = &mut buf[..self.legs * frames];
        for (leg, x) in buf.chunks_exact_mut(frames).enumerate() {
            let g = &mut self.gain[leg];
            if g.is_active() || g.target() != 1.0 {
                g.process(x);
            }
        }
        self.hpf.process(buf, self.legs, frames);
        for eq in &mut self.eq {
            eq.process(buf, self.legs, frames);
        }
    }

    /// Moves every ramp `frames` samples on without processing, for
    /// samples the engine does not compute: a parameter's value depends
    /// on the sample index and the changes alone.
    pub fn advance(&mut self, frames: usize) {
        for g in &mut self.gain[..self.legs] {
            g.advance(frames);
        }
        self.hpf.advance(frames);
        for eq in &mut self.eq {
            eq.advance(frames);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svf::tests::{measure, reference};

    const RAMP: u32 = 480;

    fn band(kind: BandType, freq: f32, gain_db: f32, q: f32) -> EqBand {
        EqBand {
            kind,
            freq,
            gain_db,
            q,
        }
    }

    fn strip_with(legs: usize, params: &[StripParam]) -> Strip {
        let mut s = Strip::new(legs, 0);
        for &p in params {
            assert!(s.set(p), "{p:?}");
        }
        s.ramp = RAMP;
        s
    }

    fn mono_response(s: &mut Strip, freq: f64) -> (f64, f64) {
        measure(freq, |b| s.process(b, b.len()))
    }

    #[test]
    fn a_new_strip_passes_every_bit() {
        let mut s = Strip::new(2, RAMP);
        let x = [1.5f32, -0.0, f32::from_bits(1), f32::from_bits(0x7fc0_1234)];
        let mut y = [x, x].concat();
        s.process(&mut y, 4);
        let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        assert_eq!(bits(&y), bits(&[x, x].concat()));
    }

    #[test]
    fn checks_follow_the_faderz_ranges() {
        let ok = [
            StripParam::Trim { db: -20.0 },
            StripParam::Trim { db: 20.0 },
            StripParam::LegTrim { leg: 1, db: 3.0 },
            StripParam::Polarity {
                leg: 1,
                inverted: true,
            },
            StripParam::Hpf {
                enabled: true,
                freq: 20.0,
            },
            StripParam::Hpf {
                enabled: false,
                freq: 500.0,
            },
            StripParam::EqBand {
                band: 3,
                value: band(BandType::Peak, 20_000.0, -18.0, 16.0),
            },
            StripParam::EqBand {
                band: 0,
                value: band(BandType::LowShelf, 20.0, 18.0, 0.1),
            },
        ];
        for p in ok {
            assert_eq!(p.check(2), Ok(()), "{p:?}");
        }
        let refused = [
            StripParam::Trim { db: 20.5 },
            StripParam::Trim { db: f32::NAN },
            StripParam::LegTrim { leg: 2, db: 0.0 },
            StripParam::LegTrim { leg: 0, db: -21.0 },
            StripParam::Polarity {
                leg: 2,
                inverted: true,
            },
            StripParam::Hpf {
                enabled: true,
                freq: 19.0,
            },
            StripParam::Hpf {
                enabled: false,
                freq: 501.0,
            },
            StripParam::EqBand {
                band: 4,
                value: DEFAULT_BANDS[0],
            },
            StripParam::EqBand {
                band: 0,
                value: band(BandType::Peak, 20_001.0, 0.0, 1.0),
            },
            StripParam::EqBand {
                band: 0,
                value: band(BandType::Peak, 1000.0, 18.5, 1.0),
            },
            StripParam::EqBand {
                band: 0,
                value: band(BandType::Peak, 1000.0, 0.0, 0.05),
            },
            StripParam::EqBand {
                band: 0,
                value: band(BandType::Peak, 1000.0, 0.0, f32::INFINITY),
            },
        ];
        let mut s = Strip::new(2, RAMP);
        for p in refused {
            assert!(p.check(2).is_err(), "{p:?}");
            assert!(!s.set(p), "{p:?}");
        }
        assert_eq!(s, Strip::new(2, RAMP));
    }

    /// The gain of each leg is trim plus leg trim, negated when inverted.
    #[test]
    fn trims_add_and_polarity_inverts_per_leg() {
        let mut s = strip_with(
            3,
            &[
                StripParam::Trim { db: 6.0 },
                StripParam::LegTrim { leg: 1, db: -12.0 },
                StripParam::Polarity {
                    leg: 2,
                    inverted: true,
                },
            ],
        );
        let mut buf = vec![0.5f32; 3 * 4];
        s.process(&mut buf, 4);
        let db = |v: f32| 20.0 * f64::from(v / 0.5).abs().log10();
        assert!((db(buf[0]) - 6.0).abs() < 1e-5, "{}", buf[0]);
        assert!((db(buf[4]) + 6.0).abs() < 1e-5, "{}", buf[4]);
        assert!(
            buf[8] < 0.0 && (db(buf[8]) - 6.0).abs() < 1e-5,
            "{}",
            buf[8]
        );
    }

    /// The strip's filters are the cookbook's: the high-pass, each band
    /// type, and the whole chain the product of its sections.
    #[test]
    fn filters_match_the_cookbook() {
        let hpf = StripParam::Hpf {
            enabled: true,
            freq: 120.0,
        };
        let bands = [
            band(BandType::LowShelf, 200.0, -6.0, 0.707),
            band(BandType::Peak, 1000.0, 9.0, 2.0),
            band(BandType::Peak, 3150.0, -12.0, 8.0),
            band(BandType::HighShelf, 10_000.0, 4.5, 1.0),
        ];
        let mut params = vec![hpf, StripParam::Eq { enabled: true }];
        params.extend(
            bands
                .iter()
                .enumerate()
                .map(|(band, &value)| StripParam::EqBand { band, value }),
        );
        let mut s = strip_with(1, &params);
        for f in [30.0, 120.0, 200.0, 1000.0, 3150.0, 10_000.0, 18_000.0] {
            let (mut want_db, mut want_deg) = reference(Shape::HighPass { freq: 120.0 }, f);
            for b in &bands {
                let (db, deg) = reference(b.shape(), f);
                want_db += db;
                want_deg += deg;
            }
            let want_deg = (want_deg + 180.0).rem_euclid(360.0) - 180.0;
            let (db, deg) = mono_response(&mut s, f);
            assert!(
                (db - want_db).abs() < 0.005,
                "{f} Hz: {db} dB, want {want_db}"
            );
            assert!(
                (deg - want_deg).abs() < 0.05,
                "{f} Hz: {deg} deg, want {want_deg}"
            );
        }
    }

    /// Switched off, the high-pass and the equaliser leave their settings
    /// and pass the input bit for bit once the ramp is over.
    #[test]
    fn switching_off_returns_to_a_bit_exact_pass() {
        let mut s = strip_with(
            1,
            &[
                StripParam::Hpf {
                    enabled: true,
                    freq: 300.0,
                },
                StripParam::Eq { enabled: true },
                StripParam::EqBand {
                    band: 1,
                    value: band(BandType::Peak, 500.0, 12.0, 1.0),
                },
            ],
        );
        let mut buf = vec![0.3f32; 960];
        s.process(&mut buf, 960);
        assert!(s.set(StripParam::Hpf {
            enabled: false,
            freq: 300.0
        }));
        assert!(s.set(StripParam::Eq { enabled: false }));
        s.process(&mut buf, RAMP as usize);
        let x: Vec<f32> = (0..64).map(|n| (n as f32 * 0.37).sin()).collect();
        let mut y = x.clone();
        s.process(&mut y, 64);
        assert_eq!(x, y);
        assert_eq!(s.bands[1].gain_db, 12.0);
        assert!(s.set(StripParam::Eq { enabled: true }));
        assert!(!s.eq[1].is_bypassed());
    }

    /// The largest step between two output samples of a 110 Hz sine
    /// while a parameter changes: within the larger one of the settled
    /// states when the change ramps, beyond it when it jumps.
    fn largest_step(before: &[StripParam], after: StripParam, ramp: u32) -> (f32, f32) {
        let mut s = strip_with(1, before);
        s.ramp = ramp;
        let w = 2.0 * core::f64::consts::PI * 110.0 / 48_000.0;
        let mut n = 0usize;
        let mut block = |s: &mut Strip| {
            let mut b: Vec<f32> = (n..n + 48)
                .map(|i| (0.5 * (w * i as f64).sin()) as f32)
                .collect();
            n += 48;
            s.process(&mut b, 48);
            b
        };
        let steps = |v: &[f32]| {
            v.windows(2)
                .map(|p| (p[1] - p[0]).abs())
                .fold(0.0f32, f32::max)
        };
        let mut settled = 0.0f32;
        let mut prev = 0.0;
        for _ in 0..200 {
            let b = block(&mut s);
            prev = b[47];
        }
        // Settled before the change.
        for _ in 0..20 {
            let b = block(&mut s);
            settled = settled.max(steps(&b));
            prev = b[47];
        }
        assert!(s.set(after));
        let mut changing = 0.0f32;
        for _ in 0..30 {
            let b = block(&mut s);
            changing = changing.max(steps(&b)).max((b[0] - prev).abs());
            prev = b[47];
        }
        for _ in 0..200 {
            block(&mut s);
        }
        for _ in 0..20 {
            settled = settled.max(steps(&block(&mut s)));
        }
        (changing, settled)
    }

    #[test]
    fn changes_ramp_without_a_click() {
        let eq_on = StripParam::Eq { enabled: true };
        let low = StripParam::EqBand {
            band: 0,
            value: band(BandType::Peak, 100.0, -18.0, 2.0),
        };
        let high = StripParam::EqBand {
            band: 0,
            value: band(BandType::Peak, 100.0, 18.0, 2.0),
        };
        let cases = [
            (vec![eq_on, low], high),
            (
                vec![],
                StripParam::Polarity {
                    leg: 0,
                    inverted: true,
                },
            ),
            (vec![], StripParam::Trim { db: 20.0 }),
        ];
        for (before, after) in cases {
            let (ramped, settled) = largest_step(&before, after, RAMP);
            assert!(
                ramped <= settled * 1.5,
                "{after:?}: step {ramped} while ramping, {settled} settled"
            );
            let (jumped, settled) = largest_step(&before, after, 0);
            assert!(
                jumped > settled * 2.0,
                "{after:?}: a jump makes a step of only {jumped}, settled {settled}"
            );
        }
    }

    /// Two strips given the same parameters at the same samples compute
    /// the same bits, whatever the block sizes, and a clone taken while a
    /// ramp runs continues as the original does.
    #[test]
    fn strips_are_deterministic_and_clones_continue_alike() {
        let input: Vec<f32> = (0..4800 * 2)
            .map(|n| ((n * 7919) % 1013) as f32 / 1013.0 - 0.5)
            .collect();
        let changes = [
            (0, StripParam::Eq { enabled: true }),
            (
                0,
                StripParam::EqBand {
                    band: 2,
                    value: band(BandType::Peak, 2000.0, 8.0, 3.0),
                },
            ),
            (
                960,
                StripParam::Hpf {
                    enabled: true,
                    freq: 90.0,
                },
            ),
            (
                1440,
                StripParam::Polarity {
                    leg: 1,
                    inverted: true,
                },
            ),
            (1920, StripParam::LegTrim { leg: 0, db: -7.5 }),
        ];
        let run = |b: usize, clone_at: Option<usize>| {
            let mut s = Strip::new(2, RAMP);
            let mut out = vec![0.0f32; input.len()];
            let frames = input.len() / 2;
            let mut at = 0;
            while at < frames {
                if clone_at == Some(at) {
                    s = s.clone();
                }
                for &(i, p) in &changes {
                    if i == at {
                        assert!(s.set(p));
                    }
                }
                let mut buf = [&input[at..at + b], &input[frames + at..frames + at + b]].concat();
                s.process(&mut buf, b);
                out[at..at + b].copy_from_slice(&buf[..b]);
                out[frames + at..frames + at + b].copy_from_slice(&buf[b..]);
                at += b;
            }
            out
        };
        let reference = run(48, None);
        let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        assert_eq!(bits(&run(16, None)), bits(&reference));
        assert_eq!(bits(&run(480, None)), bits(&reference));
        assert_eq!(bits(&run(48, Some(1440 + 96))), bits(&reference));
    }

    /// Ramps advanced over samples not processed land where processing
    /// them would have: silence leaves the filter states at zero, so the
    /// two strips are equal.
    #[test]
    fn advance_moves_every_ramp() {
        let params = [
            StripParam::Trim { db: -10.0 },
            StripParam::Hpf {
                enabled: true,
                freq: 200.0,
            },
            StripParam::Eq { enabled: true },
            StripParam::EqBand {
                band: 3,
                value: band(BandType::HighShelf, 6000.0, 6.0, 0.7),
            },
        ];
        let mut a = Strip::new(1, RAMP);
        let mut b = Strip::new(1, RAMP);
        for p in params {
            assert!(a.set(p) && b.set(p));
        }
        a.advance(300);
        let mut skipped = vec![0.0f32; 300];
        b.process(&mut skipped, 300);
        assert!(a.gain[0].is_active() && !a.hpf.is_bypassed() && !a.eq[3].is_bypassed());
        assert_eq!(a, b);
        assert_ne!(a, strip_with(1, &params));
    }

    /// Silence after a signal decays to exact zeros through a strip with
    /// every filter on, under FTZ/DAZ.
    #[test]
    fn a_strip_decays_to_exact_zero() {
        let mut params = vec![
            StripParam::Hpf {
                enabled: true,
                freq: 20.0,
            },
            StripParam::Eq { enabled: true },
        ];
        for (b, value) in [
            band(BandType::LowShelf, 20.0, 18.0, 0.1),
            band(BandType::Peak, 50.0, 18.0, 16.0),
            band(BandType::Peak, 15_000.0, -18.0, 0.1),
            band(BandType::HighShelf, 20_000.0, 18.0, 16.0),
        ]
        .into_iter()
        .enumerate()
        {
            params.push(StripParam::EqBand { band: b, value });
        }
        let saved = crate::set_ftz_daz(true);
        let mut s = strip_with(2, &params);
        let mut x = vec![0.9f32; 2 * 480];
        s.process(&mut x, 480);
        let quiet = |s: &Strip| {
            s.hpf.state == [[0.0; 2]; MAX_LEGS]
                && s.eq.iter().all(|f| f.state == [[0.0; 2]; MAX_LEGS])
        };

        for _ in 0..1_000_000 {
            let mut silence = [0.0f32; 96];
            s.process(&mut silence, 48);
            if quiet(&s) {
                break;
            }
        }
        assert!(quiet(&s), "the filter state never reached zero");
        let mut silence = [0.0f32; 96];
        s.process(&mut silence, 48);
        crate::restore_mxcsr(saved);
        assert!(silence.iter().all(|v| v.to_bits() == 0));
    }
}
