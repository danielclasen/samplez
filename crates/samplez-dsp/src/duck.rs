//! The duck detector: from the keys of up to [`MAX_KEYS`] trigger
//! inputs, an envelope between 0, no duck, and 1, full depth, that a
//! target ducked by `depth` dB is attenuated by `depth * envelope` dB
//! ([`duck_gain`]).
//!
//! Each key is a copy of one input's legs after its strip, the audio left
//! untouched, through a key high-pass and low-pass (12 dB per octave
//! Butterworth sections, each switchable), then per sample the largest
//! magnitude over its legs times the key's gain. The detector's key is
//! the largest over its keys: two microphones picking up one voice read
//! as loud as the louder of them, not louder than both.
//!
//! The detector runs once every [`GAIN_INTERVAL`] samples, on the
//! largest key of the interval, at the sample indices that are multiples
//! of it, as the strip's gate does. The key triggers when it reaches the
//! threshold; while the detector is engaged it retriggers at the
//! threshold less the hysteresis, so a key hovering at the threshold does
//! not chatter. A trigger restarts the hold; the detector stays engaged
//! until the hold has run out after the last one. The manual controls
//! decide in place of the key: a momentary hold triggers, otherwise mode
//! [`DuckMode::On`] triggers and [`DuckMode::Off`] does not, and only
//! [`DuckMode::Auto`] follows the key. The hold applies after the key, a
//! momentary hold or mode On, so a manual duck released recovers after
//! the hold; mode Off is a bypass and releases at once.
//!
//! The envelope is linear in dB. While engaged it rises by one interval
//! in each run, otherwise it falls by one, counted in whole intervals:
//! attack and release are the times from no duck to full depth and back,
//! each rounded to a whole number of intervals. A change of direction or
//! of the time carries the envelope over to the new count, so the time
//! left is the new time times the distance left, and no run moves it by
//! more than one run of the new count.
//! Between two runs the envelope moves linearly to the value computed at
//! the start of the interval, so the key of a sample acts from the next
//! interval on.
//!
//! Every value is computed in f32 without FMA, levels and counts from the
//! parameters with libm in f64, so every host gets the same bits; which
//! samples share an interval depends on the sample index alone, not on
//! how samples are split into blocks. Samples not processed move the key
//! filters' coefficient ramps on and leave what follows the signal as it
//! is, as the strip does, except that a detector decided by its manual
//! controls moves on exactly, at once whatever the number of samples.
//! The key filters zero their small states at the multiples of 64 of the
//! sample index, not at the ends of blocks, so their states do not
//! depend on the split either. The detector holds no heap memory and
//! does not allocate.

use core::ops::RangeInclusive;

use crate::dynamics::GAIN_INTERVAL;
use crate::level::{OCTAVES_PER_DB, db_to_gain, exp2};
use crate::strip::{HPF_FREQ, MAX_LEGS, Refusal, within};
use crate::svf::{SAMPLE_RATE, Shape, Svf};
use crate::{MAX_GAIN, MAX_RAMP_LEN};

/// Most trigger inputs a detector keys from.
pub const MAX_KEYS: usize = 8;
/// Most detectors a [`Matrix`](crate::Matrix) ducks by: a duck mask has
/// one bit per detector.
pub const MAX_DUCKS: usize = 8;
/// Threshold, dBFS.
pub const THRESHOLD_DB: RangeInclusive<f32> = -60.0..=0.0;
/// Hysteresis below the threshold while engaged, dB.
pub const HYSTERESIS_DB: RangeInclusive<f32> = 0.0..=12.0;
/// Key high-pass frequency, Hz.
pub const KEY_HPF_FREQ: RangeInclusive<f32> = HPF_FREQ;
/// Key low-pass frequency, Hz.
pub const KEY_LPF_FREQ: RangeInclusive<f32> = 1000.0..=20_000.0;
/// Time from no duck to full depth, ms.
pub const ATTACK_MS: RangeInclusive<f32> = 1.0..=500.0;
/// Time the detector stays engaged after the last trigger, ms.
pub const HOLD_MS: RangeInclusive<f32> = 0.0..=5000.0;
/// Time from full depth to no duck, ms.
pub const RELEASE_MS: RangeInclusive<f32> = 10.0..=10_000.0;
/// Depth of a target, dB.
pub const DEPTH_DB: RangeInclusive<f32> = 0.0..=40.0;
/// Linear gain of a key.
pub const KEY_GAIN: RangeInclusive<f32> = 0.0..=MAX_GAIN;

/// Samples of a chunk the keys are filtered in, on the stack, at the
/// multiples of which of the sample index the key filters zero their
/// small states; a multiple of [`GAIN_INTERVAL`].
const CHUNK: usize = 64;
/// `1 / GAIN_INTERVAL`.
const INV: f32 = 1.0 / GAIN_INTERVAL as f32;

/// A key filter section.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyFilter {
    pub enabled: bool,
    /// -3 dB frequency, Hz.
    pub freq: f32,
}

/// The parameters of a detector.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DetectorParams {
    /// dBFS within [`THRESHOLD_DB`]; the key triggers when it reaches it.
    pub threshold_db: f32,
    /// dB within [`HYSTERESIS_DB`].
    pub hysteresis_db: f32,
    /// Frequency within [`KEY_HPF_FREQ`].
    pub hpf: KeyFilter,
    /// Frequency within [`KEY_LPF_FREQ`].
    pub lpf: KeyFilter,
    /// ms within [`ATTACK_MS`].
    pub attack_ms: f32,
    /// ms within [`HOLD_MS`].
    pub hold_ms: f32,
    /// ms within [`RELEASE_MS`].
    pub release_ms: f32,
}

impl DetectorParams {
    /// The parameters of a new detector.
    pub const DEFAULT: DetectorParams = DetectorParams {
        threshold_db: -30.0,
        hysteresis_db: 4.0,
        hpf: KeyFilter {
            enabled: true,
            freq: 120.0,
        },
        lpf: KeyFilter {
            enabled: false,
            freq: 4000.0,
        },
        attack_ms: 20.0,
        hold_ms: 500.0,
        release_ms: 1000.0,
    };

    /// The parameters with every value moved to the nearest one its
    /// range holds; none when a value is NaN or infinite.
    pub fn clamped(&self) -> Option<DetectorParams> {
        let c = crate::strip::clamp_in;
        Some(DetectorParams {
            threshold_db: c(self.threshold_db, &THRESHOLD_DB)?,
            hysteresis_db: c(self.hysteresis_db, &HYSTERESIS_DB)?,
            hpf: KeyFilter {
                enabled: self.hpf.enabled,
                freq: c(self.hpf.freq, &KEY_HPF_FREQ)?,
            },
            lpf: KeyFilter {
                enabled: self.lpf.enabled,
                freq: c(self.lpf.freq, &KEY_LPF_FREQ)?,
            },
            attack_ms: c(self.attack_ms, &ATTACK_MS)?,
            hold_ms: c(self.hold_ms, &HOLD_MS)?,
            release_ms: c(self.release_ms, &RELEASE_MS)?,
        })
    }

    fn refusal(&self) -> Result<(), Refusal> {
        within("duck threshold", self.threshold_db, &THRESHOLD_DB)?;
        within("duck hysteresis", self.hysteresis_db, &HYSTERESIS_DB)?;
        within("duck key hpf freq", self.hpf.freq, &KEY_HPF_FREQ)?;
        within("duck key lpf freq", self.lpf.freq, &KEY_LPF_FREQ)?;
        within("duck attack", self.attack_ms, &ATTACK_MS)?;
        within("duck hold", self.hold_ms, &HOLD_MS)?;
        within("duck release", self.release_ms, &RELEASE_MS)
    }

    fn hpf_shape(&self) -> Shape {
        Shape::HighPass {
            freq: self.hpf.freq,
        }
    }

    fn lpf_shape(&self) -> Shape {
        Shape::LowPass {
            freq: self.lpf.freq,
        }
    }
}

impl Default for DetectorParams {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What decides the detector when no momentary hold is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DuckMode {
    /// The key.
    Auto,
    /// Triggered whatever the key.
    On,
    /// Not triggered whatever the key, released at once: the hold does
    /// not apply.
    Off,
}

/// One setting of a detector, to an absolute value.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DuckParam {
    Detector(DetectorParams),
    /// Keys key `key`, below [`MAX_KEYS`], from an input of `legs` legs,
    /// 1 to [`MAX_LEGS`], at a linear `gain` within [`KEY_GAIN`]. A key
    /// newly keyed or of another width starts with its filters at rest.
    Key {
        key: usize,
        legs: usize,
        gain: f32,
    },
    /// Stops keying key `key`.
    Unkey {
        key: usize,
    },
    Mode(DuckMode),
    /// The momentary hold: while held, the detector is triggered whatever
    /// the mode and the key.
    Hold {
        held: bool,
    },
}

impl DuckParam {
    /// Whether a detector takes the parameter: keys that exist, widths
    /// and values within their ranges, no NaN.
    pub fn check(&self) -> Result<(), String> {
        self.refusal().map_err(|r| r.message())
    }

    fn refusal(&self) -> Result<(), Refusal> {
        let key_exists = |key: usize| {
            if key < MAX_KEYS {
                Ok(())
            } else {
                Err(Refusal::NoKey(key))
            }
        };
        match *self {
            DuckParam::Detector(p) => p.refusal(),
            DuckParam::Key { key, legs, gain } => {
                key_exists(key)?;
                if !(1..=MAX_LEGS).contains(&legs) {
                    return Err(Refusal::KeyLegs(legs));
                }
                within("duck key gain", gain, &KEY_GAIN)
            }
            DuckParam::Unkey { key } => key_exists(key),
            DuckParam::Mode(_) | DuckParam::Hold { .. } => Ok(()),
        }
    }
}

/// The gain of a target ducked by `depth_db` at `envelope`: `depth_db *
/// envelope` dB of attenuation through the strip's `exp2`, exactly 1 at
/// none.
#[inline]
pub fn duck_gain(depth_db: f32, envelope: f32) -> f32 {
    exp2(-(depth_db * envelope) * OCTAVES_PER_DB)
}

fn samples(ms: f32) -> f64 {
    f64::from(ms) * SAMPLE_RATE / 1000.0
}

/// Runs of a transit of `ms`, at least one.
fn runs(ms: f32) -> u32 {
    (libm::round(samples(ms) / f64::from(GAIN_INTERVAL)) as u32).max(1)
}

/// The values a detector computes from its parameters.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Levels {
    threshold: f32,
    /// The threshold less the hysteresis.
    reopen: f32,
    /// Runs from no duck to full depth.
    attack: u32,
    /// Runs from full depth to no duck.
    release: u32,
    /// Samples.
    hold: u32,
}

impl Levels {
    fn of(p: &DetectorParams) -> Levels {
        Levels {
            threshold: db_to_gain(p.threshold_db),
            reopen: db_to_gain(p.threshold_db - p.hysteresis_db),
            attack: runs(p.attack_ms),
            release: runs(p.release_ms),
            hold: libm::round(samples(p.hold_ms)) as u32,
        }
    }

    /// The runs of the envelope's transit while engaged or not.
    fn span(&self, engaged: bool) -> u32 {
        if engaged { self.attack } else { self.release }
    }
}

/// `pos` runs of a transit of `from` runs carried over to one of `to`
/// runs, down while rising and up while falling, so the next run moves
/// the envelope on by at most one run of the new transit.
fn carry(pos: u32, from: u32, to: u32, rising: bool) -> u32 {
    let (n, d) = (u64::from(pos) * u64::from(to), u64::from(from));
    (if rising { n / d } else { n.div_ceil(d) }) as u32
}

/// Runs of the longest transit.
fn max_span() -> u32 {
    runs(*ATTACK_MS.end()).max(runs(*RELEASE_MS.end()))
}

/// One key: its input's width and gain, and its filters.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Key {
    on: bool,
    legs: usize,
    gain: f32,
    hpf: Svf<MAX_LEGS>,
    lpf: Svf<MAX_LEGS>,
}

impl Key {
    fn new(p: &DetectorParams) -> Key {
        let mut k = Key {
            on: false,
            legs: 1,
            gain: 0.0,
            hpf: Svf::new(),
            lpf: Svf::new(),
        };
        k.retarget(p, 0);
        k
    }

    fn retarget(&mut self, p: &DetectorParams, ramp: u32) {
        self.hpf.set(p.hpf_shape(), p.hpf.enabled, ramp);
        self.lpf.set(p.lpf_shape(), p.lpf.enabled, ramp);
    }

    /// Filter states at rest.
    fn rest(&mut self) {
        self.hpf.state = REST;
        self.lpf.state = REST;
    }

    /// Takes the key of `out.len()` samples into `out` where it is
    /// larger, leg `l` of them from `leg(l)`.
    /// The filters zero their small states when `small`.
    fn key<'a>(&mut self, out: &mut [f32], leg: impl Fn(usize) -> &'a [f32], small: bool) {
        let (n, legs) = (out.len(), self.legs);
        let mut scratch = [0.0f32; MAX_LEGS * CHUNK];
        let x = &mut scratch[..legs * n];
        for (l, x) in x.chunks_exact_mut(n).enumerate() {
            x.copy_from_slice(leg(l));
        }
        self.hpf.process_flushing(x, legs, n, small);
        self.lpf.process_flushing(x, legs, n, small);
        for (i, o) in out.iter_mut().enumerate() {
            let mut m = 0.0f32;
            for l in 0..legs {
                let v = x[l * n + i].abs();
                if v > m {
                    m = v;
                }
            }
            // A NaN fails the comparisons, an infinity is taken as
            // f32::MAX, so the key is finite whatever the input.
            let v = (m.min(f32::MAX) * self.gain).min(f32::MAX);
            if v > *o {
                *o = v;
            }
        }
    }

    fn advance(&mut self, frames: usize) {
        self.hpf.advance(frames);
        self.lpf.advance(frames);
    }

    fn is_valid(&self, p: &DetectorParams) -> bool {
        (1..=MAX_LEGS).contains(&self.legs)
            && KEY_GAIN.contains(&self.gain)
            && self.hpf.is_valid(p.hpf_shape(), p.hpf.enabled)
            && self.lpf.is_valid(p.lpf_shape(), p.lpf.enabled)
            && (self.on || self.hpf.state == REST && self.lpf.state == REST)
    }
}

/// The states of a filter at rest.
const REST: [[f32; 2]; MAX_LEGS] = [[0.0; 2]; MAX_LEGS];

/// A duck detector. A new one is at [`DetectorParams::DEFAULT`], mode
/// [`DuckMode::Auto`], no key and no duck.
///
/// A detector deserializes only as one [`Detector::is_valid`] takes.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "Unchecked")]
pub struct Detector {
    ramp: u32,
    params: DetectorParams,
    levels: Levels,
    mode: DuckMode,
    held: bool,
    keys: [Key; MAX_KEYS],
    /// Triggered in the last run or holding: the envelope rises.
    engaged: bool,
    /// Samples of hold left.
    hold: u32,
    /// The envelope in runs of a transit of `span` runs.
    pos: u32,
    span: u32,
    /// The largest key of the interval so far.
    peak: f32,
    /// The sample index modulo [`CHUNK`]; modulo [`GAIN_INTERVAL`], the
    /// samples of the interval processed.
    at: u32,
    /// The envelope moving by `step` a sample from `from` to `to`.
    from: f32,
    step: f32,
    to: f32,
}

/// A detector as its serde form carries it, before it is checked.
#[derive(serde::Deserialize)]
struct Unchecked {
    ramp: u32,
    params: DetectorParams,
    levels: Levels,
    mode: DuckMode,
    held: bool,
    keys: [Key; MAX_KEYS],
    engaged: bool,
    hold: u32,
    pos: u32,
    span: u32,
    peak: f32,
    at: u32,
    from: f32,
    step: f32,
    to: f32,
}

impl TryFrom<Unchecked> for Detector {
    type Error = &'static str;

    fn try_from(u: Unchecked) -> Result<Detector, Self::Error> {
        let d = Detector {
            ramp: u.ramp,
            params: u.params,
            levels: u.levels,
            mode: u.mode,
            held: u.held,
            keys: u.keys,
            engaged: u.engaged,
            hold: u.hold,
            pos: u.pos,
            span: u.span,
            peak: u.peak,
            at: u.at,
            from: u.from,
            step: u.step,
            to: u.to,
        };
        if d.is_valid() {
            Ok(d)
        } else {
            Err("a duck detector state no detector holds")
        }
    }
}

impl Detector {
    /// A detector whose key filter changes ramp over `ramp` samples.
    pub fn new(ramp: u32) -> Detector {
        let params = DetectorParams::DEFAULT;
        let levels = Levels::of(&params);
        Detector {
            ramp: ramp.min(MAX_RAMP_LEN),
            params,
            levels,
            mode: DuckMode::Auto,
            held: false,
            keys: [Key::new(&params); MAX_KEYS],
            engaged: false,
            hold: 0,
            pos: 0,
            span: levels.span(false),
            peak: 0.0,
            at: 0,
            from: 0.0,
            step: 0.0,
            to: 0.0,
        }
    }

    pub fn params(&self) -> DetectorParams {
        self.params
    }

    /// The samples a key filter change ramps over.
    pub fn ramp(&self) -> u32 {
        self.ramp
    }

    pub fn mode(&self) -> DuckMode {
        self.mode
    }

    /// Whether the momentary hold is held.
    pub fn held(&self) -> bool {
        self.held
    }

    /// Whether the last run triggered or held: the envelope rises.
    pub fn is_engaged(&self) -> bool {
        self.engaged
    }

    /// The width and gain of key `key`, none when it is not keyed or
    /// beyond [`MAX_KEYS`].
    pub fn key(&self, key: usize) -> Option<(usize, f32)> {
        self.keys
            .get(key)
            .filter(|k| k.on)
            .map(|k| (k.legs, k.gain))
    }

    /// Whether processing leaves the detector at rest whatever its input:
    /// no key keyed, neither held nor in mode On, not engaged, the hold,
    /// the envelope and its step at 0. [`Detector::advance`] over the
    /// samples computes the same envelope, 0 throughout.
    pub fn is_idle(&self) -> bool {
        self.keys.iter().all(|k| !k.on)
            && !self.held
            && self.mode != DuckMode::On
            && !self.engaged
            && self.hold == 0
            && self.pos == 0
            && self.from == 0.0
            && self.step == 0.0
    }

    /// The envelope of the last sample processed, 0 to 1.
    pub fn envelope(&self) -> f32 {
        self.from + (self.at % GAIN_INTERVAL) as f32 * self.step
    }

    /// Sets `param` from the next run on: a new threshold, hysteresis,
    /// hold, mode or momentary hold decides the next run, a new attack or
    /// release moves the envelope from it on, and key filters ramp over
    /// the detector's ramp length. Returns false and changes nothing when
    /// [`DuckParam::check`] refuses it; a refusal does not allocate.
    #[must_use]
    pub fn set(&mut self, param: DuckParam) -> bool {
        if param.refusal().is_err() {
            return false;
        }
        match param {
            DuckParam::Detector(p) => {
                self.params = p;
                self.levels = Levels::of(&p);
                self.hold = self.hold.min(self.levels.hold);
                for k in &mut self.keys {
                    k.retarget(&p, self.ramp);
                }
            }
            DuckParam::Key { key, legs, gain } => {
                let k = &mut self.keys[key];
                if !k.on || k.legs != legs {
                    k.rest();
                }
                k.on = true;
                k.legs = legs;
                k.gain = gain;
            }
            DuckParam::Unkey { key } => {
                let k = &mut self.keys[key];
                k.rest();
                k.on = false;
                k.legs = 1;
                k.gain = 0.0;
            }
            DuckParam::Mode(m) => self.mode = m,
            DuckParam::Hold { held } => self.held = held,
        }
        true
    }

    /// Runs `frames` samples and writes the envelope of each to
    /// `envelope`. Leg `l` of key `k`, for each key keyed, is the slice
    /// `leg(k, l)`, holding the samples after the input's strip.
    ///
    /// # Panics
    ///
    /// If `envelope` or a leg is shorter than `frames`.
    pub fn process<'a>(
        &mut self,
        frames: usize,
        leg: impl Fn(usize, usize) -> &'a [f32],
        envelope: &mut [f32],
    ) {
        self.run_keys::<false>(frames, leg, envelope);
    }

    /// [`Detector::process`], returning the largest key of the samples:
    /// per sample the largest over the keys keyed of each key's largest
    /// leg magnitude after its filters times its gain, 0 with no key.
    ///
    /// # Panics
    ///
    /// If `envelope` or a leg is shorter than `frames`.
    pub fn process_metered<'a>(
        &mut self,
        frames: usize,
        leg: impl Fn(usize, usize) -> &'a [f32],
        envelope: &mut [f32],
    ) -> f32 {
        self.run_keys::<true>(frames, leg, envelope)
    }

    /// Runs `frames` samples, and with `METER` returns their largest key.
    fn run_keys<'a, const METER: bool>(
        &mut self,
        frames: usize,
        leg: impl Fn(usize, usize) -> &'a [f32],
        envelope: &mut [f32],
    ) -> f32 {
        let envelope = &mut envelope[..frames];
        let mut largest = 0.0f32;
        let mut done = 0;
        while done < frames {
            // Chunks end at the multiples of CHUNK of the sample index,
            // where the key filters zero their small states.
            let n = (frames - done).min(CHUNK - self.at as usize);
            let small = self.at as usize + n == CHUNK;
            let mut key = [0.0f32; CHUNK];
            for (k, chain) in self.keys.iter_mut().enumerate() {
                if chain.on {
                    chain.key(&mut key[..n], |l| &leg(k, l)[done..done + n], small);
                } else {
                    chain.advance(n);
                }
            }
            for (e, &k) in envelope[done..done + n].iter_mut().zip(&key[..n]) {
                *e = self.tick(k);
            }
            if METER {
                largest = key[..n].iter().fold(largest, |m, &k| m.max(k));
            }
            done += n;
        }
        largest
    }

    /// Takes the key of the next sample and returns its envelope.
    #[inline]
    fn tick(&mut self, key: f32) -> f32 {
        if key > self.peak {
            self.peak = key;
        }
        self.at = (self.at + 1) % CHUNK as u32;
        let phase = self.at % GAIN_INTERVAL;
        if phase != 0 {
            return self.from + phase as f32 * self.step;
        }
        let e = self.to;
        self.run();
        e
    }

    /// Whether the manual controls decide the detector.
    fn forced(&self) -> bool {
        self.held || self.mode != DuckMode::Auto
    }

    /// The run at the end of an interval: decides whether the interval
    /// triggered, moves the hold and the envelope on and starts the next
    /// interval.
    fn run(&mut self) {
        let l = self.levels;
        let (triggered, hold) = if self.held {
            (true, l.hold)
        } else {
            match self.mode {
                DuckMode::On => (true, l.hold),
                DuckMode::Off => (false, 0),
                DuckMode::Auto => {
                    let at = if self.engaged { l.reopen } else { l.threshold };
                    let triggered = self.peak >= at;
                    let hold = if triggered {
                        l.hold
                    } else {
                        self.hold.saturating_sub(GAIN_INTERVAL)
                    };
                    (triggered, hold)
                }
            }
        };
        self.hold = hold;
        self.toward(triggered || hold > 0, 1);
        let to = self.pos as f32 / self.span as f32;
        self.from = self.to;
        self.step = (to - self.to) * INV;
        self.to = to;
        self.peak = 0.0;
    }

    /// Moves the envelope `runs` runs up while `engaged`, down otherwise,
    /// in runs of the attack or the release, carried over from the
    /// transit it was counted in.
    fn toward(&mut self, engaged: bool, runs: u64) {
        let span = self.levels.span(engaged);
        if span != self.span {
            self.pos = carry(self.pos, self.span, span, engaged);
            self.span = span;
        }
        self.engaged = engaged;
        let pos = u64::from(self.pos);
        self.pos = if engaged {
            (pos + runs).min(u64::from(span))
        } else {
            pos.saturating_sub(runs)
        } as u32;
    }

    /// `runs` runs of a detector its manual controls decide, the same
    /// decision in each: all but the last at once, the last as
    /// [`run`](Self::run) makes it.
    fn run_forced(&mut self, runs: u64) {
        if runs > 1 {
            let on = self.held || self.mode == DuckMode::On;
            self.hold = if on { self.levels.hold } else { 0 };
            self.toward(on, runs - 1);
            self.to = self.pos as f32 / self.span as f32;
        }
        self.peak = 0.0;
        self.run();
    }

    /// Into the next interval without a run: the envelope held at the
    /// last value computed, no key taken yet.
    fn wrap(&mut self) {
        self.from = self.to;
        self.step = 0.0;
        self.peak = 0.0;
    }

    /// Moves `frames` samples on without their keys: the key filters'
    /// ramps move on, their states, the hold and the envelope stay as
    /// they are, held at the last value computed across the end of the
    /// interval. A detector decided by its manual controls makes every
    /// run it passes, as processing does, the keys not entering.
    pub fn advance(&mut self, frames: usize) {
        for k in &mut self.keys {
            k.advance(frames);
        }
        let len = GAIN_INTERVAL as usize;
        let runs = ((self.at as usize % len) + frames) / len;
        self.at = ((self.at as usize + frames % CHUNK) % CHUNK) as u32;
        if runs == 0 {
            return;
        }
        if self.forced() {
            self.run_forced(runs as u64);
        } else {
            self.wrap();
        }
    }

    /// Takes the next sample processed to be sample index `index`: the
    /// detector runs at the multiples of [`GAIN_INTERVAL`] samples of the
    /// index. A position in the interval other than the current one holds
    /// the envelope as [`advance`](Self::advance) does. A detector starts
    /// at index 0 and counts the samples it processes and advances over.
    pub fn align(&mut self, index: u64) {
        let at = (index % CHUNK as u64) as u32;
        if at % GAIN_INTERVAL != self.at % GAIN_INTERVAL {
            self.wrap();
        }
        self.at = at;
    }

    /// Whether the detector holds what one built by [`Detector::new`] and
    /// changed by its methods holds: parameters within their ranges and
    /// the levels computed from them, keys of 1 to [`MAX_LEGS`] legs and
    /// a gain within [`KEY_GAIN`], key filters ramping to the shapes of
    /// the parameters with states within the bound a block leaves and at
    /// rest when not keyed, a hold within the hold time and only while
    /// engaged, an envelope position within a transit no longer than the
    /// longest, a finite key, a position within the grid and an envelope
    /// within 0 and 1 moving by the step of its ends to its position. A
    /// detector decoded from a peer is checked with it before it runs.
    pub fn is_valid(&self) -> bool {
        let unit = |v: f32| (0.0..=1.0).contains(&v);
        self.ramp <= MAX_RAMP_LEN
            && self.params.refusal().is_ok()
            && self.levels == Levels::of(&self.params)
            && self.keys.iter().all(|k| k.is_valid(&self.params))
            && self.hold <= self.levels.hold
            && (self.hold == 0 || self.engaged)
            && (1..=max_span()).contains(&self.span)
            && self.pos <= self.span
            && self.peak.is_finite()
            && self.peak >= 0.0
            && self.at < CHUNK as u32
            && unit(self.from)
            && self.to == self.pos as f32 / self.span as f32
            && self.step == (self.to - self.from) * INV
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svf::tests::reference;
    use crate::test_util::Rng;

    /// Block sizes: the ends of the engine's range, the grid's multiples
    /// and sizes off it.
    const BLOCKS: [usize; 7] = [16, 17, 48, 55, 64, 480, 1024];
    /// Tolerance of a time, samples: two runs of the detector, one for
    /// the grid and one for the rounding of a time to it.
    const TOL: usize = 2 * GAIN_INTERVAL as usize;

    fn samples(ms: f32) -> usize {
        libm::round(f64::from(ms) * SAMPLE_RATE / 1000.0) as usize
    }

    /// `p` with both key filters off.
    fn plain(p: DetectorParams) -> DetectorParams {
        DetectorParams {
            hpf: KeyFilter {
                enabled: false,
                ..p.hpf
            },
            lpf: KeyFilter {
                enabled: false,
                ..p.lpf
            },
            ..p
        }
    }

    /// A detector at `p` keyed by one mono input at unity gain.
    fn detector(p: DetectorParams) -> Detector {
        let mut d = Detector::new(0);
        assert!(d.set(DuckParam::Detector(p)));
        assert!(d.set(DuckParam::Key {
            key: 0,
            legs: 1,
            gain: 1.0,
        }));
        d
    }

    /// The envelope of every sample of `keys[k][l]`, leg `l` of key `k`,
    /// run through `d` in blocks of `next()` samples, `events` set before
    /// the sample of their index.
    fn run_with(
        d: &mut Detector,
        keys: &[Vec<Vec<f32>>],
        mut next: impl FnMut() -> usize,
        events: &[(usize, DuckParam)],
    ) -> Vec<f32> {
        let len = keys[0][0].len();
        let mut out = vec![0.0; len];
        let mut start = 0;
        while start < len {
            for (_, p) in events.iter().filter(|(at, _)| *at == start) {
                assert!(d.set(*p));
            }
            let stop = events
                .iter()
                .map(|&(at, _)| at)
                .filter(|&at| at > start)
                .fold(len, usize::min);
            let n = next().clamp(1, stop - start);
            d.process(
                n,
                |k, l| &keys[k][l][start..start + n],
                &mut out[start..start + n],
            );
            start += n;
        }
        out
    }

    fn run(d: &mut Detector, keys: &[Vec<Vec<f32>>], b: usize) -> Vec<f32> {
        run_with(d, keys, || b, &[])
    }

    fn run_mono(d: &mut Detector, x: &[f32], b: usize) -> Vec<f32> {
        run(d, &[vec![x.to_vec()]], b)
    }

    fn bits(e: &[f32]) -> Vec<u32> {
        e.iter().map(|v| v.to_bits()).collect()
    }

    /// `len` samples of a sine of `freq` Hz and peak `amp`, faded in
    /// over 50 ms with a raised cosine, so the key filters' response to
    /// its onset stays small.
    fn tone(freq: f64, amp: f32, len: usize) -> Vec<f32> {
        let fade = samples(50.0);
        let w = 2.0 * core::f64::consts::PI * freq / SAMPLE_RATE;
        (0..len)
            .map(|n| {
                let f = if n < fade {
                    0.5 - 0.5 * (core::f64::consts::PI * n as f64 / fade as f64).cos()
                } else {
                    1.0
                };
                (f64::from(amp) * f * (w * n as f64).sin()) as f32
            })
            .collect()
    }

    fn peak(e: &[f32]) -> f32 {
        e.iter().fold(0.0f32, |a, &b| a.max(b))
    }

    /// A key reaching the threshold triggers and one 0.1 dB short of it
    /// does not, at the ends of the threshold's range and its default: a
    /// 1 kHz tone without key filters, and through the default high-pass
    /// and a low-pass at 4 kHz, its level set by their passband gain at
    /// 1 kHz.
    #[test]
    fn a_key_triggers_at_the_threshold() {
        let len = samples(200.0);
        let thresholds = [
            *THRESHOLD_DB.start(),
            DetectorParams::DEFAULT.threshold_db,
            *THRESHOLD_DB.end(),
        ];
        for threshold_db in thresholds {
            let filtered = DetectorParams {
                threshold_db,
                lpf: KeyFilter {
                    enabled: true,
                    freq: 4000.0,
                },
                ..DetectorParams::DEFAULT
            };
            for p in [plain(filtered), filtered] {
                let mut pass = 0.0;
                if p.hpf.enabled {
                    pass += reference(p.hpf_shape(), 1000.0).0;
                }
                if p.lpf.enabled {
                    pass += reference(p.lpf_shape(), 1000.0).0;
                }
                for (offset, triggers) in [(0.1, true), (-0.1, false)] {
                    let amp = db_to_gain(threshold_db + offset - pass as f32);
                    let e = run_mono(&mut detector(p), &tone(1000.0, amp, len), 48);
                    assert_eq!(
                        peak(&e) > 0.0,
                        triggers,
                        "{p:?}, {offset:+} dB: envelope up to {}",
                        peak(&e)
                    );
                }
            }
        }
    }

    /// The key filters keep out what they cut: a 40 Hz tone 20 dB above
    /// the threshold triggers without the high-pass and not through it at
    /// 150 Hz, through which a 1 kHz tone 1 dB above it does; an 8 kHz
    /// tone 20 dB above it triggers without the low-pass and not through
    /// it at 1 kHz.
    #[test]
    fn the_key_filters_keep_out_what_they_cut() {
        let len = samples(1000.0);
        let hpf = DetectorParams {
            hpf: KeyFilter {
                enabled: true,
                freq: 150.0,
            },
            ..DetectorParams::DEFAULT
        };
        let lpf = DetectorParams {
            lpf: KeyFilter {
                enabled: true,
                freq: 1000.0,
            },
            ..plain(DetectorParams::DEFAULT)
        };
        let cases = [
            (hpf, 40.0, 20.0, false),
            (plain(hpf), 40.0, 20.0, true),
            (hpf, 1000.0, 1.0, true),
            (lpf, 8000.0, 20.0, false),
            (plain(lpf), 8000.0, 20.0, true),
        ];
        for (p, freq, over_db, triggers) in cases {
            let amp = db_to_gain(p.threshold_db + over_db);
            let e = run_mono(&mut detector(p), &tone(freq, amp, len), 48);
            assert_eq!(
                peak(&e) > 0.0,
                triggers,
                "{freq} Hz {over_db} dB over, {p:?}"
            );
        }
    }

    /// While engaged the key retriggers at the threshold less the
    /// hysteresis: a key alternating between the threshold and 0.1 dB
    /// short of the hysteresis below it, each time for longer than the
    /// hold, keeps the duck at full depth, and 0.1 dB beyond the
    /// hysteresis releases it.
    #[test]
    fn the_detector_has_hysteresis() {
        for hysteresis_db in [
            1.0,
            DetectorParams::DEFAULT.hysteresis_db,
            *HYSTERESIS_DB.end(),
        ] {
            let p = plain(DetectorParams {
                hysteresis_db,
                hold_ms: 100.0,
                ..DetectorParams::DEFAULT
            });
            let (loud, quiet) = (samples(p.attack_ms) + 2 * TOL, 3 * samples(p.hold_ms));
            for (below_db, releases) in [(hysteresis_db - 0.1, false), (hysteresis_db + 0.1, true)]
            {
                let x: Vec<f32> = (0..3 * (loud + quiet))
                    .map(|n| {
                        let db = if n % (loud + quiet) < loud {
                            p.threshold_db
                        } else {
                            p.threshold_db - below_db
                        };
                        db_to_gain(db)
                    })
                    .collect();
                let e = run_mono(&mut detector(p), &x, 48);
                let full = e.iter().position(|&v| v == 1.0).unwrap();
                assert!(full < loud, "{hysteresis_db} dB");
                assert_eq!(
                    e[full..].iter().any(|&v| v < 1.0),
                    releases,
                    "hysteresis {hysteresis_db} dB, key {below_db} dB below"
                );
            }
        }
    }

    /// Attack, hold and release are the times from no duck to full
    /// depth, from the last trigger to the start of the release and from
    /// full depth back to none, within TOL, at the ends of their ranges
    /// and at their defaults, at every block size, and every block size
    /// computes the same bits.
    #[test]
    fn the_envelope_takes_its_attack_hold_and_release() {
        let d = DetectorParams::DEFAULT;
        let sets = [
            (*ATTACK_MS.start(), *HOLD_MS.start(), *RELEASE_MS.start()),
            (d.attack_ms, d.hold_ms, d.release_ms),
            (*ATTACK_MS.end(), *HOLD_MS.end(), *RELEASE_MS.end()),
        ];
        for (attack_ms, hold_ms, release_ms) in sets {
            let p = plain(DetectorParams {
                attack_ms,
                hold_ms,
                release_ms,
                ..d
            });
            let (attack, hold, release) =
                (samples(attack_ms), samples(hold_ms), samples(release_ms));
            // The key from `onset` to `last`, off the grid.
            let onset = samples(10.0) + 3;
            let last = onset + attack + samples(50.0);
            let len = last + hold + release + samples(50.0);
            let loud = db_to_gain(p.threshold_db + 6.0);
            let x: Vec<f32> = (0..len)
                .map(|n| {
                    if (onset..=last).contains(&n) {
                        loud
                    } else {
                        0.0
                    }
                })
                .collect();
            let mut first: Option<Vec<u32>> = None;
            for b in BLOCKS {
                let e = run_mono(&mut detector(p), &x, b);
                let at =
                    |from: usize, f: &dyn Fn(f32) -> bool| (from..len).find(|&n| f(e[n])).unwrap();
                let full = at(onset, &|v| v == 1.0);
                let falls = at(full, &|v| v < 1.0);
                let rest = at(falls, &|v| v == 0.0);
                let what = format!(
                    "attack {attack_ms}, hold {hold_ms}, release {release_ms} ms, blocks of {b}"
                );
                assert!(e[..=onset].iter().all(|&v| v == 0.0), "{what}");
                assert!(
                    (full - onset).abs_diff(attack) <= TOL,
                    "{what}: full depth {} samples after the onset, attack {attack}",
                    full - onset
                );
                assert!(
                    (hold..=hold + TOL).contains(&(falls - last)),
                    "{what}: release {} samples after the last trigger, hold {hold}",
                    falls - last
                );
                assert!(
                    (rest + 1 - falls).abs_diff(release) <= TOL,
                    "{what}: no duck {} samples after the release started, release {release}",
                    rest + 1 - falls
                );
                assert!(e[onset..full].windows(2).all(|w| w[1] >= w[0]), "{what}");
                assert!(e[falls..rest].windows(2).all(|w| w[1] <= w[0]), "{what}");
                assert!(e[rest..].iter().all(|&v| v == 0.0), "{what}");
                match &first {
                    None => first = Some(bits(&e)),
                    Some(f) => assert!(*f == bits(&e), "{what}: other bits than blocks of 16"),
                }
            }
        }
    }

    /// The detector's key is the loudest of its keys, each the loudest of
    /// its legs times its gain: two keys each 1 dB short of the threshold
    /// do not trigger together, where their sum would be 5 dB over it; the
    /// second leg of the second key alone triggers; the third key 6 dB
    /// short triggers at a gain of 7 dB and not at 5 dB; a key no longer
    /// keyed does not trigger.
    #[test]
    fn keys_combine_as_the_loudest() {
        let p = plain(DetectorParams::DEFAULT);
        let len = samples(200.0);
        let dc = |db: f32| vec![db_to_gain(db); len];
        let off = vec![0.0f32; len];
        let t = p.threshold_db;
        let triggers = |gain_db: f32, unkey: bool, keys: [Vec<Vec<f32>>; 3]| {
            let mut d = Detector::new(0);
            assert!(d.set(DuckParam::Detector(p)));
            for (key, legs, gain) in [(0, 1, 1.0), (1, 2, 1.0), (2, 1, db_to_gain(gain_db))] {
                assert!(d.set(DuckParam::Key { key, legs, gain }));
            }
            if unkey {
                assert!(d.set(DuckParam::Unkey { key: 1 }));
            }
            peak(&run(&mut d, &keys, 48)) > 0.0
        };
        let each_short = [
            vec![dc(t - 1.0)],
            vec![dc(t - 1.0), dc(t - 1.0)],
            vec![off.clone()],
        ];
        assert!(!triggers(0.0, false, each_short));
        let second_leg = [
            vec![off.clone()],
            vec![off.clone(), dc(t + 1.0)],
            vec![off.clone()],
        ];
        assert!(triggers(0.0, false, second_leg));
        let third = || {
            [
                vec![off.clone()],
                vec![off.clone(), off.clone()],
                vec![dc(t - 6.0)],
            ]
        };
        assert!(triggers(7.0, false, third()));
        assert!(!triggers(5.0, false, third()));
        let unkeyed = [
            vec![off.clone()],
            vec![dc(t + 6.0), dc(t + 6.0)],
            vec![off.clone()],
        ];
        assert!(triggers(0.0, false, unkeyed.clone()));
        assert!(!triggers(0.0, true, unkeyed));
    }

    /// The momentary hold triggers whatever the mode and the key, then
    /// mode On triggers and Off does not whatever the key, and Auto
    /// follows the key: after the attack the envelope is at full depth or
    /// was never above none, for every combination.
    #[test]
    fn the_hold_then_the_mode_then_the_key_decide() {
        let p = plain(DetectorParams::DEFAULT);
        let len = samples(p.attack_ms) + TOL;
        for held in [false, true] {
            for mode in [DuckMode::Auto, DuckMode::On, DuckMode::Off] {
                for loud in [false, true] {
                    let want = held || mode == DuckMode::On || (mode == DuckMode::Auto && loud);
                    let mut d = detector(p);
                    assert!(d.set(DuckParam::Mode(mode)) && d.set(DuckParam::Hold { held }));
                    let x = vec![
                        if loud {
                            db_to_gain(p.threshold_db + 6.0)
                        } else {
                            0.0
                        };
                        len
                    ];
                    let e = run_mono(&mut d, &x, 48);
                    let what = format!("held {held}, {mode:?}, key loud {loud}");
                    assert_eq!(e[len - 1] == 1.0, want, "{what}");
                    assert_eq!(peak(&e) == 0.0, !want, "{what}");
                }
            }
        }
    }

    /// A manual duck, held or latched, takes the attack to full depth
    /// and, released, the hold and the release back to none, as a keyed
    /// one does; mode Off
    /// set while a key holds the duck releases it at the next run,
    /// without the hold, and the momentary hold ducks again through it.
    #[test]
    fn a_manual_duck_takes_the_same_times() {
        let p = plain(DetectorParams::DEFAULT);
        let (attack, hold, release) = (
            samples(p.attack_ms),
            samples(p.hold_ms),
            samples(p.release_ms),
        );
        let span = attack + hold + release + 2 * TOL;
        let at = |e: &[f32], from: usize, f: &dyn Fn(f32) -> bool| {
            (from..e.len()).find(|&n| f(e[n])).unwrap()
        };
        for b in BLOCKS {
            // The momentary hold, and a latch set back to auto.
            let latches = [
                (
                    DuckParam::Hold { held: true },
                    DuckParam::Hold { held: false },
                ),
                (
                    DuckParam::Mode(DuckMode::On),
                    DuckParam::Mode(DuckMode::Auto),
                ),
            ];
            for (on, off) in latches {
                let press = 480;
                let lift = press + attack + 960;
                let silence = vec![vec![vec![0.0f32; lift + span]]];
                let events = [(press, on), (lift, off)];
                let e = run_with(&mut detector(p), &silence, || b, &events);
                let full = at(&e, press, &|v| v == 1.0);
                let falls = at(&e, full, &|v| v < 1.0);
                let rest = at(&e, falls, &|v| v == 0.0);
                assert!(
                    e[..=press].iter().all(|&v| v == 0.0),
                    "{on:?}, blocks of {b}"
                );
                assert!(
                    (full - press).abs_diff(attack) <= TOL,
                    "{on:?}, blocks of {b}: full after {}",
                    full - press
                );
                assert!(
                    (hold..=hold + TOL).contains(&(falls - lift)),
                    "{on:?}, blocks of {b}: release after {}",
                    falls - lift
                );
                assert!(
                    (rest + 1 - falls).abs_diff(release) <= TOL,
                    "{on:?}, blocks of {b}: none after {}",
                    rest + 1 - falls
                );
            }

            let bypass = attack + 960;
            let press = bypass + span;
            let loud = vec![vec![vec![db_to_gain(p.threshold_db + 6.0); press + span]]];
            let events = [
                (bypass, DuckParam::Mode(DuckMode::Off)),
                (press, DuckParam::Hold { held: true }),
            ];
            let e = run_with(&mut detector(p), &loud, || b, &events);
            assert_eq!(e[bypass - 1], 1.0, "blocks of {b}");
            let falls = at(&e, bypass, &|v| v < 1.0);
            let rest = at(&e, falls, &|v| v == 0.0);
            assert!(
                (1..=TOL).contains(&(falls - bypass)),
                "blocks of {b}: bypass after {}",
                falls - bypass
            );
            assert!(
                (rest + 1 - falls).abs_diff(release) <= TOL,
                "blocks of {b}: no duck {} samples after the bypass",
                rest + 1 - falls
            );
            assert!(e[rest..press].iter().all(|&v| v == 0.0), "blocks of {b}");
            let full = at(&e, press, &|v| v == 1.0);
            assert!(
                (full - press).abs_diff(attack) <= TOL,
                "blocks of {b}: full after {}",
                full - press
            );
        }
    }

    /// Samples advanced over leave what follows the signal as it is: mid
    /// attack and mid hold, the envelope stays at the last value computed
    /// and the hold where it was. A detector decided by its manual
    /// controls moves on as processing does: one advanced and one
    /// processing the same samples, from any position in an interval,
    /// then compute the same bits.
    #[test]
    fn advancing_holds_the_key_and_moves_a_forced_detector_on() {
        let p = plain(DetectorParams::DEFAULT);
        let loud = vec![db_to_gain(p.threshold_db + 6.0); samples(p.attack_ms) / 2 + 3];
        let mut d = detector(p);
        run_mono(&mut d, &loud, 48);
        let (to, hold) = (d.to, d.hold);
        assert!(0.0 < to && to < 1.0 && d.engaged);
        d.advance(samples(p.hold_ms + p.release_ms));
        assert_eq!(
            (d.envelope(), d.to, d.hold, d.engaged),
            (to, to, hold, true)
        );

        let tail = vec![0.0f32; samples(p.attack_ms + p.hold_ms + p.release_ms) + TOL];
        for (mode, held) in [
            (DuckMode::On, false),
            (DuckMode::Auto, true),
            (DuckMode::Off, false),
        ] {
            for skip in [
                3,
                8,
                13,
                samples(p.attack_ms) + 5,
                samples(p.hold_ms + p.release_ms) + 11,
            ] {
                let mut a = detector(p);
                run_mono(&mut a, &loud, 48);
                assert!(a.set(DuckParam::Mode(mode)) && a.set(DuckParam::Hold { held }));
                let mut b = a;
                run_mono(&mut a, &vec![1.0; skip], 48);
                b.advance(skip);
                assert_eq!(
                    bits(&run_mono(&mut a, &tail, 48)),
                    bits(&run_mono(&mut b, &tail, 48)),
                    "{mode:?}, held {held}, {skip} samples"
                );
            }
        }
    }

    /// Bursts of noise around the threshold on two keys, one of them
    /// stereo, through both key filters, with the momentary hold pressed
    /// and released and a new attack mid-way.
    fn speech(rng: &mut Rng, len: usize) -> Vec<Vec<Vec<f32>>> {
        let p = DetectorParams::DEFAULT;
        let segment = samples(30.0);
        [1, 2]
            .map(|legs| {
                (0..legs)
                    .map(|_| {
                        let mut level = 0.0;
                        (0..len)
                            .map(|n| {
                                if n % segment == 0 {
                                    let db =
                                        p.threshold_db - 12.0 + 18.0 * (rng.unit() + 1.0) / 2.0;
                                    level = db_to_gain(db);
                                }
                                level * rng.unit()
                            })
                            .collect()
                    })
                    .collect()
            })
            .into()
    }

    fn speech_detector() -> Detector {
        let mut d = Detector::new(480);
        assert!(d.set(DuckParam::Detector(DetectorParams {
            lpf: KeyFilter {
                enabled: true,
                freq: 4000.0,
            },
            hold_ms: 100.0,
            release_ms: 300.0,
            ..DetectorParams::DEFAULT
        })));
        assert!(d.set(DuckParam::Key {
            key: 0,
            legs: 1,
            gain: 1.0,
        }));
        assert!(d.set(DuckParam::Key {
            key: 1,
            legs: 2,
            gain: 0.7,
        }));
        d
    }

    /// The bits do not depend on the block split, at every block size
    /// and at random splits, and a clone taken mid-way continues alike.
    #[test]
    fn blocks_and_clones_do_not_change_a_bit() {
        let mut rng = Rng::new(0xd0c4_5eed);
        let len = samples(3000.0);
        let keys = speech(&mut rng, len);
        let events = [
            (samples(1000.0) + 5, DuckParam::Hold { held: true }),
            (samples(1500.0) + 11, DuckParam::Hold { held: false }),
            (
                samples(2000.0) + 1,
                DuckParam::Detector(DetectorParams {
                    attack_ms: 3.0,
                    hold_ms: 0.0,
                    ..speech_detector().params
                }),
            ),
        ];
        let want = run_with(&mut speech_detector(), &keys, || 48, &events);
        assert!(want.contains(&1.0) && want.iter().any(|&v| 0.0 < v && v < 1.0));
        for b in BLOCKS {
            let got = run_with(&mut speech_detector(), &keys, || b, &events);
            assert!(bits(&got) == bits(&want), "blocks of {b}");
        }
        for seed in 1..=4u64 {
            let mut sizes = Rng::new(seed);
            let got = run_with(
                &mut speech_detector(),
                &keys,
                || 1 + sizes.below(1024),
                &events,
            );
            assert!(bits(&got) == bits(&want), "random split {seed}");
        }
        let mut d = speech_detector();
        let half = |k: &Vec<Vec<f32>>, r: core::ops::Range<usize>| -> Vec<Vec<f32>> {
            k.iter().map(|l| l[r.clone()].to_vec()).collect()
        };
        let head: Vec<_> = keys.iter().map(|k| half(k, 0..len / 2)).collect();
        let tail: Vec<_> = keys.iter().map(|k| half(k, len / 2..len)).collect();
        run(&mut d, &head, 55);
        let mut clone = d;
        assert_eq!(
            bits(&run(&mut d, &tail, 55)),
            bits(&run(&mut clone, &tail, 17))
        );
    }

    /// A target ducks by depth times envelope in dB: exactly unity at
    /// envelope 0, its depth at 1 and half of it at 0.5, within 0.001
    /// dB, over the depth range.
    #[test]
    fn a_target_ducks_by_depth_times_envelope() {
        let (lo, hi) = (*DEPTH_DB.start(), *DEPTH_DB.end());
        for i in 0..=400 {
            let depth = lo + (hi - lo) * i as f32 / 400.0;
            assert_eq!(duck_gain(depth, 0.0), 1.0);
            for e in [0.5f32, 1.0] {
                let db = 20.0 * f64::from(duck_gain(depth, e)).log10();
                let want = -f64::from(depth * e);
                assert!((db - want).abs() < 1e-3, "depth {depth} at {e}: {db} dB");
            }
        }
    }

    /// Every parameter is refused outside its range and as NaN, by name,
    /// and taken at its ends; a refused one changes nothing.
    #[test]
    fn checks_follow_the_ranges() {
        let d = DetectorParams::DEFAULT;
        type Field = fn(&mut DetectorParams) -> &mut f32;
        let fields: [(&str, RangeInclusive<f32>, Field); 7] = [
            ("duck threshold", THRESHOLD_DB, |p| &mut p.threshold_db),
            ("duck hysteresis", HYSTERESIS_DB, |p| &mut p.hysteresis_db),
            ("duck key hpf freq", KEY_HPF_FREQ, |p| &mut p.hpf.freq),
            ("duck key lpf freq", KEY_LPF_FREQ, |p| &mut p.lpf.freq),
            ("duck attack", ATTACK_MS, |p| &mut p.attack_ms),
            ("duck hold", HOLD_MS, |p| &mut p.hold_ms),
            ("duck release", RELEASE_MS, |p| &mut p.release_ms),
        ];
        let mut det = Detector::new(48);
        for (name, range, field) in fields {
            let with = |v: f32| {
                let mut p = d;
                *field(&mut p) = v;
                DuckParam::Detector(p)
            };
            for v in [*range.start(), *range.end()] {
                assert_eq!(with(v).check(), Ok(()), "{name} {v}");
            }
            for v in [*range.start() - 0.5, *range.end() + 0.5, f32::NAN] {
                let err = with(v).check().unwrap_err();
                assert!(err.starts_with(name), "{name} {v}: {err}");
                let before = det;
                assert!(!det.set(with(v)));
                assert_eq!(det, before);
            }
        }
        let key = |key, legs, gain| DuckParam::Key { key, legs, gain };
        assert_eq!(key(MAX_KEYS - 1, MAX_LEGS, MAX_GAIN).check(), Ok(()));
        for p in [
            key(MAX_KEYS, 1, 1.0),
            DuckParam::Unkey { key: MAX_KEYS },
            key(0, 0, 1.0),
            key(0, MAX_LEGS + 1, 1.0),
            key(0, 1, -0.5),
            key(0, 1, f32::NAN),
            key(0, 1, f32::INFINITY),
        ] {
            assert!(p.check().is_err(), "{p:?}");
            assert!(!det.set(p), "{p:?}");
        }
        assert_eq!(det, Detector::new(48));
    }

    /// The speech detector after a second of its keys, a release and the
    /// start of a new duck: engaged, holding, the envelope moving.
    fn busy() -> Detector {
        let mut rng = Rng::new(0xb05e);
        let len = samples(1000.0);
        let mut d = speech_detector();
        let keys = speech(&mut rng, len);
        run(&mut d, &keys, 55);
        // Released, then a new duck starting.
        let quiet = vec![0.0f32; samples(600.0)];
        run(
            &mut d,
            &[vec![quiet.clone()], vec![quiet.clone(), quiet]],
            55,
        );
        let mut x = [db_to_gain(-10.0); 13];
        x[5] = -x[5];
        run(
            &mut d,
            &[vec![x.to_vec()], vec![x.to_vec(), x.to_vec()]],
            13,
        );
        assert!(
            d.engaged && d.hold > 0 && d.step != 0.0 && d.is_valid(),
            "{d:?}"
        );
        d
    }

    /// A detector's state comes back bit for bit through serde and
    /// continues alike; a state no detector holds is refused, by
    /// `is_valid` and on decoding: a value out of its range, levels not
    /// those of the parameters, a NaN, an envelope off its transit or its
    /// step, a hold beyond its time, a ninth key.
    #[test]
    fn a_detector_round_trips_and_refuses_what_none_holds() {
        let d = busy();
        let wire = serde_json::to_string(&d).unwrap();
        let mut back: Detector = serde_json::from_str(&wire).unwrap();
        assert_eq!(back, d);
        let mut rng = Rng::new(7);
        let keys = speech(&mut rng, 4800);
        let mut d2 = d;
        assert_eq!(
            bits(&run(&mut back, &keys, 48)),
            bits(&run(&mut d2, &keys, 48))
        );

        let refused = |what: &str, f: &dyn Fn(&mut Detector)| {
            let mut bad = d;
            f(&mut bad);
            assert!(!bad.is_valid(), "{what}");
            let wire = serde_json::to_string(&bad).unwrap();
            assert!(serde_json::from_str::<Detector>(&wire).is_err(), "{what}");
        };
        refused("ramp beyond the longest", &|d| d.ramp = MAX_RAMP_LEN + 1);
        refused("threshold out of range", &|d| d.params.threshold_db = 6.0);
        refused("levels not the parameters'", &|d| {
            d.levels.reopen = d.levels.threshold
        });
        refused("hold beyond its time", &|d| d.hold = d.levels.hold + 1);
        refused("hold while not engaged", &|d| d.engaged = false);
        refused("envelope beyond its transit", &|d| d.pos = d.span + 1);
        refused("transit beyond the longest", &|d| {
            d.pos = 0;
            d.span = max_span() + 1;
            d.to = 0.0;
            d.step = (d.to - d.from) * INV;
        });
        refused("envelope off its position", &|d| {
            d.to = (d.pos + 1) as f32 / d.span as f32;
            d.step = (d.to - d.from) * INV;
        });
        refused("NaN envelope", &|d| d.to = f32::NAN);
        refused("envelope beyond full depth", &|d| {
            d.to = 1.5;
            d.step = (d.to - d.from) * INV;
        });
        refused("step not between its ends", &|d| d.step = 0.5);
        refused("position beyond the grid", &|d| d.at = CHUNK as u32);
        refused("NaN key", &|d| d.peak = f32::NAN);
        refused("negative key", &|d| d.peak = -1.0);
        refused("key of no legs", &|d| d.keys[0].legs = 0);
        refused("key beyond a strip's legs", &|d| {
            d.keys[1].legs = MAX_LEGS + 1
        });
        refused("key gain beyond its range", &|d| {
            d.keys[1].gain = 2.0 * MAX_GAIN
        });
        refused("key filter state beyond a block's", &|d| {
            d.keys[0].hpf.state[0][1] = 1e31
        });
        refused("key filter off its shape", &|d| d.params.lpf.freq = 5000.0);
        refused("state on a key not keyed", &|d| {
            d.keys[5].lpf.state[0][0] = 0.5
        });
        let mut nine = serde_json::to_value(d).unwrap();
        let keys = nine["keys"].as_array_mut().unwrap();
        keys.push(keys[0].clone());
        assert!(
            serde_json::from_value::<Detector>(nine).is_err(),
            "nine keys"
        );
    }

    /// A key keyed anew or at another width starts with its filters at
    /// rest; one keyed again at its width keeps them.
    #[test]
    fn a_key_rekeyed_keeps_its_filters_only_at_its_width() {
        let d = busy();
        let state = d.keys[1].hpf.state;
        assert!(state[1] != [0.0; 2]);
        let key = |legs, gain| DuckParam::Key { key: 1, legs, gain };
        let mut same = d;
        assert!(same.set(key(2, 0.5)));
        assert_eq!(same.keys[1].hpf.state, state);
        let mut wider = d;
        assert!(wider.set(key(3, 0.7)));
        assert_eq!(wider.keys[1].hpf.state, REST);
        let mut anew = d;
        assert!(anew.set(DuckParam::Unkey { key: 1 }) && anew.set(key(2, 0.7)));
        assert_eq!(anew.keys[1].hpf.state, REST);
    }

    fn pick(rng: &mut Rng, r: &RangeInclusive<f32>) -> f32 {
        let (lo, hi) = (*r.start(), *r.end());
        match rng.below(8) {
            0 => lo,
            1 => hi,
            _ => lo + (hi - lo) * (rng.unit() + 1.0) / 2.0,
        }
        .clamp(lo, hi)
    }

    fn param(rng: &mut Rng) -> DuckParam {
        match rng.below(6) {
            0 => DuckParam::Detector(DetectorParams {
                threshold_db: pick(rng, &THRESHOLD_DB),
                hysteresis_db: pick(rng, &HYSTERESIS_DB),
                hpf: KeyFilter {
                    enabled: rng.below(2) == 0,
                    freq: pick(rng, &KEY_HPF_FREQ),
                },
                lpf: KeyFilter {
                    enabled: rng.below(2) == 0,
                    freq: pick(rng, &KEY_LPF_FREQ),
                },
                attack_ms: pick(rng, &ATTACK_MS),
                hold_ms: pick(rng, &(0.0..=200.0)),
                release_ms: pick(rng, &(10.0..=300.0)),
            }),
            1 => DuckParam::Key {
                key: rng.below(MAX_KEYS),
                legs: 1 + rng.below(MAX_LEGS),
                gain: pick(rng, &(0.0..=4.0)),
            },
            2 => DuckParam::Unkey {
                key: rng.below(MAX_KEYS),
            },
            3 => DuckParam::Mode([DuckMode::Auto, DuckMode::On, DuckMode::Off][rng.below(3)]),
            4 => DuckParam::Hold {
                held: rng.below(3) == 0,
            },
            _ => DuckParam::Key {
                key: rng.below(MAX_KEYS + 2),
                legs: rng.below(MAX_LEGS + 2),
                gain: [f32::NAN, -1.0, 1.0][rng.below(3)],
            },
        }
    }

    /// Every state a detector reaches through random parameters, keys,
    /// signals with infinities and NaNs among them, advances and
    /// alignments is valid, and a refused parameter changes nothing.
    #[test]
    fn every_state_a_detector_reaches_is_valid() {
        let mut rng = Rng::new(0xd0c4_7a1d);
        for ramp in [0, 48, 480] {
            let mut d = Detector::new(ramp);
            assert!(d.is_valid());
            let mut index = 0u64;
            for _ in 0..3000 {
                match rng.below(8) {
                    0..=2 => {
                        let p = param(&mut rng);
                        let before = d;
                        if !d.set(p) {
                            assert!(p.check().is_err() && d == before, "{p:?}");
                        }
                    }
                    3 => {
                        let frames = rng.below(300);
                        d.advance(frames);
                        index += frames as u64;
                        d.align(index);
                    }
                    4 => {
                        index += rng.below(20) as u64;
                        d.align(index);
                    }
                    _ => {
                        let frames = 1 + rng.below(300);
                        let scale = [1.0, 0.01, 1e-3][rng.below(3)];
                        let keys: Vec<Vec<Vec<f32>>> = (0..MAX_KEYS)
                            .map(|_| {
                                (0..MAX_LEGS)
                                    .map(|_| {
                                        (0..frames)
                                            .map(|_| match rng.below(2000) {
                                                0 => f32::INFINITY,
                                                1 => f32::NAN,
                                                2 => f32::MAX,
                                                _ => scale * rng.sample(),
                                            })
                                            .collect()
                                    })
                                    .collect()
                            })
                            .collect();
                        let e = run(&mut d, &keys, frames);
                        assert!(e.iter().all(|v| (0.0..=1.0).contains(v)));
                        index += frames as u64;
                    }
                }
                assert!(d.is_valid(), "ramp {ramp}: {d:?}");
            }
        }
    }

    /// An infinity, a NaN or f32::MAX in a key, through the key filters
    /// or past them at a key gain above unity, leaves a valid detector
    /// that recovers: the key filters drop the state it left, and once
    /// the duck it may have triggered has released the envelope is that
    /// of a detector that never read it.
    #[test]
    fn a_non_finite_key_leaves_a_valid_detector_that_recovers() {
        for p in [DetectorParams::DEFAULT, plain(DetectorParams::DEFAULT)] {
            let keyed = || {
                let mut d = detector(p);
                assert!(d.set(DuckParam::Key {
                    key: 0,
                    legs: 1,
                    gain: 2.0,
                }));
                d
            };
            let quiet = tone(1000.0, db_to_gain(p.threshold_db - 12.0), samples(3000.0));
            let want = run_mono(&mut keyed(), &quiet, 48);
            assert!(want.iter().all(|&v| v == 0.0));
            for bad in [f32::INFINITY, f32::NEG_INFINITY, f32::NAN, f32::MAX] {
                // Off the grid, a block ending right after it.
                let at = samples(100.0) + 3;
                let mut x = quiet.clone();
                x[at] = bad;
                let mut d = keyed();
                let mut got = Vec::new();
                let (head, tail) = x.split_at(at + 1);
                for block in head.chunks(48).chain(tail.chunks(48)) {
                    got.extend(run_mono(&mut d, block, 48));
                    assert!(d.is_valid(), "after {bad}: {d:?}");
                }
                let settled = samples(100.0 + p.attack_ms + p.hold_ms + p.release_ms) + 2 * TOL;
                assert!(
                    bits(&got[settled..]) == bits(&want[settled..]),
                    "after {bad}, {p:?}"
                );
            }
        }
    }

    /// A reversal mid-transit carries the envelope over: a key returning
    /// mid-release takes the attack times the distance left to full
    /// depth, a duck released mid-attack the release times the depth
    /// reached, within TOL, with attack and release either way round and
    /// at every block size; no sample moves the envelope by more than one
    /// run's share of the transit it moves in.
    #[test]
    fn a_reversal_takes_the_time_of_the_distance_left() {
        // Transits of no whole ratio, so a carry rounds.
        for (attack_ms, release_ms) in [(70.0, 330.0), (330.0, 70.0), (23.0, 1000.0)] {
            let p = plain(DetectorParams {
                attack_ms,
                hold_ms: 0.0,
                release_ms,
                ..DetectorParams::DEFAULT
            });
            let (attack, release) = (samples(attack_ms), samples(release_ms));
            let up = INV / runs(attack_ms) as f32 + 1e-6;
            let down = INV / runs(release_ms) as f32 + 1e-6;
            let loud = db_to_gain(p.threshold_db + 6.0);
            // Full depth, released for 0.4 of the release, back again.
            let first = attack + samples(20.0);
            let back = first + 2 * release / 5;
            let returns: Vec<f32> = (0..back + attack + samples(20.0))
                .map(|n| if n < first || n >= back { loud } else { 0.0 })
                .collect();
            // 0.4 of the attack, then released.
            let burst = 2 * attack / 5;
            let leaves: Vec<f32> = (0..burst + release + samples(20.0))
                .map(|n| if n < burst { loud } else { 0.0 })
                .collect();
            for b in BLOCKS {
                let what = format!("attack {attack_ms}, release {release_ms} ms, blocks of {b}");
                let e = run_mono(&mut detector(p), &returns, b);
                let rises = (first..e.len()).find(|&n| e[n] > e[n - 1]).unwrap();
                let e0 = e[rises - 1];
                assert!(0.3 < e0 && e0 < 0.9, "{what}: {e0}");
                let full = (rises..e.len()).find(|&n| e[n] == 1.0).unwrap();
                let want = f64::from(1.0 - e0) * attack as f64;
                let got = (full + 1 - rises) as f64;
                assert!(
                    (got - want).abs() <= TOL as f64,
                    "{what}: full {got} samples after the return from {e0}, want {want}"
                );
                let e1 = run_mono(&mut detector(p), &leaves, b);
                let falls = (1..e1.len()).find(|&n| e1[n] < e1[n - 1]).unwrap();
                let top = e1[falls - 1];
                assert!(0.2 < top && top < 0.6, "{what}: {top}");
                let rest = (falls..e1.len()).find(|&n| e1[n] == 0.0).unwrap();
                let want = f64::from(top) * release as f64;
                let got = (rest + 1 - falls) as f64;
                assert!(
                    (got - want).abs() <= TOL as f64,
                    "{what}: none {got} samples after the release from {top}, want {want}"
                );
                for e in [&e, &e1] {
                    for (n, w) in e.windows(2).enumerate() {
                        let d = w[1] - w[0];
                        assert!(
                            d <= up && -d <= down,
                            "{what}: sample {n} moves {d}, at most {up} up, {down} down"
                        );
                    }
                }
            }
        }
    }

    /// The key filters zero their small states on the grid of the sample
    /// index, not at the ends of blocks: a filtered tone dying into
    /// silence and returning leaves the same detector, key filter states
    /// included, at every block size, every 5 ms.
    #[test]
    fn key_filter_states_do_not_depend_on_the_split() {
        let p = DetectorParams {
            lpf: KeyFilter {
                enabled: true,
                freq: 4000.0,
            },
            ..DetectorParams::DEFAULT
        };
        let burst = tone(1000.0, db_to_gain(-20.0), samples(100.0));
        let mut x = burst.clone();
        x.extend(vec![0.0; samples(200.0)]);
        x.extend(&burst);
        let states = |b: usize| -> Vec<Detector> {
            let mut d = detector(p);
            x.chunks(samples(5.0))
                .map(|c| {
                    run_mono(&mut d, c, b);
                    d
                })
                .collect()
        };
        let want = states(48);
        for b in BLOCKS {
            assert!(states(b) == want, "blocks of {b}");
        }
    }

    /// The largest key `process_metered` returns is the largest per-sample
    /// key: the louder key's largest leg magnitude after its filters times
    /// its gain, and 0 without a key. The envelope it writes is the one
    /// `process` writes.
    #[test]
    fn metering_returns_the_largest_key_and_the_same_envelope() {
        let mut d = Detector::new(0);
        let p = plain(DetectorParams::DEFAULT);
        assert!(d.set(DuckParam::Detector(p)));
        let mut env = [0.0; 48];
        assert_eq!(d.process_metered(48, |_, _| &[0.0; 48], &mut env), 0.0);
        assert!(d.set(DuckParam::Key {
            key: 0,
            legs: 2,
            gain: 0.5,
        }));
        assert!(d.set(DuckParam::Key {
            key: 3,
            legs: 1,
            gain: 2.0,
        }));
        let a = [[0.1f32; 48], [-0.8; 48]];
        let mut b = [0.0f32; 48];
        b[17] = -0.3;
        let mut other = d;
        let leg = |k: usize, l: usize| if k == 0 { &a[l][..] } else { &b[..] };
        assert_eq!(d.process_metered(48, leg, &mut env), 0.6);
        let mut plain_env = [0.0; 48];
        other.process(48, leg, &mut plain_env);
        assert_eq!(bits(&env), bits(&plain_env));
        assert!(d == other);
    }

    /// An idle detector advanced computes what one processed computes:
    /// envelope 0 throughout and the same state but for the transit its
    /// envelope position counts in, at position 0 in either. A key, a
    /// manual trigger or an envelope not at rest makes it not idle.
    #[test]
    fn an_idle_detector_advanced_equals_one_processed() {
        let mut rng = Rng::new(7);
        let keys = speech(&mut rng, samples(200.0));
        let mut d = Detector::new(480);
        assert!(d.is_idle());
        let (mut processed, mut advanced) = (d, d);
        let out = run(&mut processed, &keys, 55);
        advanced.advance(keys[0][0].len());
        assert!(out.iter().all(|&e| e == 0.0));
        assert!(processed.is_idle() && advanced.is_idle());
        assert_eq!(processed.envelope(), advanced.envelope());
        assert!(processed == advanced, "{processed:?} {advanced:?}");
        assert!(d.set(DuckParam::Mode(DuckMode::Off)));
        assert!(d.is_idle());
        for p in [
            DuckParam::Mode(DuckMode::On),
            DuckParam::Hold { held: true },
            DuckParam::Key {
                key: 2,
                legs: 1,
                gain: 0.0,
            },
        ] {
            let mut busy = d;
            assert!(busy.set(p));
            assert!(!busy.is_idle(), "{p:?}");
        }
        let mut released = speech_detector();
        run(&mut released, &keys, 48);
        assert!(released.set(DuckParam::Unkey { key: 0 }));
        assert!(released.set(DuckParam::Unkey { key: 1 }));
        assert!(!released.is_idle(), "an envelope not at rest");
        run(&mut released, &[vec![vec![0.0; samples(2000.0)]]], 48);
        assert!(released.is_idle());
    }

    /// `key` names the width and gain of a key keyed, none otherwise.
    #[test]
    fn a_key_reads_back() {
        let d = speech_detector();
        assert_eq!(d.key(0), Some((1, 1.0)));
        assert_eq!(d.key(1), Some((2, 0.7)));
        assert_eq!(d.key(2), None);
        assert_eq!(d.key(MAX_KEYS), None);
    }
}
