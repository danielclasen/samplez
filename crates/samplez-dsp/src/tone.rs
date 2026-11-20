//! The line-up tone generator: a sine of whole hertz at a sample-peak
//! level, the same on every leg, with an optional ident that silences one
//! leg at a time.
//!
//! The sine at sample index `n` is `level * sin(2 pi ((n f) mod R) / R)`
//! for frequency `f` and sample rate `R`, the phase reduced in integers
//! and the sine taken by the pinned libm in f64, so the value is a
//! function of `n` alone: exactly periodic, with no phase accumulated
//! however far `n` lies, the same bits on every replica and whatever the
//! block size or the samples skipped.
//!
//! A level or ident change ramps linearly over the ramp length from its
//! index, from the value at that index, as a gain does. A frequency
//! change crossfades from the old sine to the new one over the ramp
//! length from its index; one during a crossfade waits until it ends, the
//! latest counting, as a delay change does. Every ramp is a function of
//! the indices of the changes, so it does not depend on the blocks either.
//!
//! With the ident on, leg `l` is silent over every window `[k Q, (k + 1)
//! Q)` with `k mod (legs + 1) = l`, `Q = R / 4` (250 ms) rounded down: the
//! legs are cut in turn, in leg order, and the window after the last
//! leg's cut sounds on every leg. Each cut fades out over the
//! [`ident_edge`] samples before its window and in over as many after
//! it, linearly, so its edges step by no more than the level over the
//! edge length at any frequency and rate.

use serde::{Deserialize, Serialize};

use crate::level::db_to_gain;

/// The lowest and highest frequency, Hz.
pub const TONE_FREQUENCY_HZ: core::ops::RangeInclusive<u32> = 20..=20_000;
/// The level range, dBFS sample peak.
pub const TONE_LEVEL_DB: core::ops::RangeInclusive<f32> = -60.0..=0.0;

/// A generator's parameters, set by one entry as absolute values.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToneParams {
    /// Within [`TONE_FREQUENCY_HZ`].
    pub frequency_hz: u32,
    /// The sample peak, dBFS, within [`TONE_LEVEL_DB`].
    pub level_db: f32,
    /// The legs are cut in turn.
    pub ident: bool,
}

impl ToneParams {
    /// A generator's parameters before any is set: 1 kHz at -18 dBFS, no
    /// ident.
    pub const DEFAULT: ToneParams = ToneParams {
        frequency_hz: 1000,
        level_db: -18.0,
        ident: false,
    };

    /// Whether a generator takes the parameters: values within their
    /// ranges, no NaN. Allocates nothing, as the RT thread asks it.
    pub fn is_valid(&self) -> bool {
        TONE_FREQUENCY_HZ.contains(&self.frequency_hz) && TONE_LEVEL_DB.contains(&self.level_db)
    }

    /// [`ToneParams::is_valid`], with the value refused named.
    pub fn check(&self) -> Result<(), String> {
        if !TONE_FREQUENCY_HZ.contains(&self.frequency_hz) {
            return Err(format!(
                "tone frequency {} Hz is outside {}..={}",
                self.frequency_hz,
                TONE_FREQUENCY_HZ.start(),
                TONE_FREQUENCY_HZ.end()
            ));
        }
        if !TONE_LEVEL_DB.contains(&self.level_db) {
            return Err(format!(
                "tone level {} dBFS is outside {}..={}",
                self.level_db,
                TONE_LEVEL_DB.start(),
                TONE_LEVEL_DB.end()
            ));
        }
        Ok(())
    }

    /// The parameters a generator takes in place of these, as a
    /// replica's bridge replaces a refused entry: these when valid, else
    /// [`ToneParams::clamped`], else [`ToneParams::DEFAULT`] for a level
    /// that is not finite.
    pub fn admitted(&self) -> ToneParams {
        if self.is_valid() {
            *self
        } else {
            self.clamped().unwrap_or(ToneParams::DEFAULT)
        }
    }

    /// The parameters with the frequency and level moved to the nearest
    /// values their ranges hold; none when the level is not finite.
    pub fn clamped(&self) -> Option<ToneParams> {
        let level = self.level_db;
        Some(ToneParams {
            frequency_hz: self
                .frequency_hz
                .clamp(*TONE_FREQUENCY_HZ.start(), *TONE_FREQUENCY_HZ.end()),
            level_db: level
                .is_finite()
                .then(|| level.clamp(*TONE_LEVEL_DB.start(), *TONE_LEVEL_DB.end()))?,
            ident: self.ident,
        })
    }

    /// Whether two parameter sets agree in every field's bits.
    pub fn same(&self, other: &ToneParams) -> bool {
        self.frequency_hz == other.frequency_hz
            && self.level_db.to_bits() == other.level_db.to_bits()
            && self.ident == other.ident
    }
}

/// The sine of frequency `f` at rate `rate` at sample index `n`:
/// `sin(2 pi ((n f) mod rate) / rate)`, the phase reduced in integers so
/// that it holds at any `n`.
#[inline]
pub fn sine(n: u64, f: u32, rate: u32) -> f32 {
    let rate = u64::from(rate);
    let phase = (n % rate) * u64::from(f) % rate;
    libm::sin(core::f64::consts::TAU * phase as f64 / rate as f64) as f32
}

/// The linear gain of level `db`, as the generator scales its sine by.
pub fn level_gain(db: f32) -> f32 {
    db_to_gain(db)
}

/// The ident's window, 250 ms at `rate`, rounded down.
pub fn ident_window(rate: u32) -> u64 {
    u64::from(rate / 4)
}

/// The samples each ident cut fades out over before its window and in
/// over after it: 10 ms at `rate`, rounded down.
pub fn ident_edge(rate: u32) -> u64 {
    u64::from(rate / 100)
}

/// A value moving linearly from `from` at index `start` to `to` over a
/// ramp length, holding `to` after: `from + (n - start) * step`, `step =
/// (to - from) / len`, as [`crate::Ramp`] computes a gain.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Glide {
    pub from: f32,
    pub to: f32,
    pub start: u64,
}

impl Glide {
    fn hold(v: f32) -> Glide {
        Glide {
            from: v,
            to: v,
            start: 0,
        }
    }

    /// The value at index `n` for a ramp length of `len`.
    #[inline]
    pub fn at(&self, n: u64, len: u32) -> f32 {
        let Some(j) = n.checked_sub(self.start) else {
            return self.from;
        };
        if j >= u64::from(len) {
            return self.to;
        }
        self.from + j as f32 * ((self.to - self.from) / len as f32)
    }

    /// Moves to `to` from index `at`, from the value there; one already
    /// heading there is left as it is.
    fn set(&mut self, to: f32, at: u64, len: u32) {
        if to.to_bits() != self.to.to_bits() {
            *self = Glide {
                from: self.at(at, len),
                to,
                start: at,
            };
        }
    }

    fn within(&self, lo: f32, hi: f32) -> bool {
        (lo..=hi).contains(&self.from) && (lo..=hi).contains(&self.to)
    }
}

/// Where the frequency stands: the sine it leaves, the one it crossfades
/// to from index `start` over the ramp length, and one waiting for that
/// crossfade to end.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Frequency {
    pub from: u32,
    pub to: u32,
    pub start: u64,
    pub next: Option<u32>,
}

impl Frequency {
    /// The waiting frequency starts its crossfade once the one before
    /// ended at or before index `n`.
    #[inline]
    fn promote(&mut self, n: u64, len: u32) {
        let end = self.start.saturating_add(u64::from(len));
        if let Some(next) = self.next
            && n >= end
        {
            *self = Frequency {
                from: self.to,
                to: next,
                start: end,
                next: None,
            };
        }
    }
}

/// The ramps of a generator as values: its level (a linear gain), its
/// frequency and its ident weight (0 off, 1 on), each with the index it
/// moves from and the value it moves from. A function of the parameters
/// set and their indices alone, so a replica follows them from the log
/// and a generator rebuilt from them computes a running one's bits.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToneRamps {
    pub level: Glide,
    pub frequency: Frequency,
    pub ident: Glide,
}

impl Default for ToneRamps {
    /// A generator's ramps before any parameter is set: held at
    /// [`ToneParams::DEFAULT`].
    fn default() -> ToneRamps {
        ToneRamps::held(ToneParams::DEFAULT)
    }
}

impl ToneRamps {
    /// Ramps holding `p` at every index.
    pub fn held(p: ToneParams) -> ToneRamps {
        ToneRamps {
            level: Glide::hold(level_gain(p.level_db)),
            frequency: Frequency {
                from: p.frequency_hz,
                to: p.frequency_hz,
                start: 0,
                next: None,
            },
            ident: Glide::hold(if p.ident { 1.0 } else { 0.0 }),
        }
    }

    /// Whether the ramps hold what [`ToneRamps::held`] and
    /// [`ToneRamps::set`] give: levels within the level range, ident
    /// weights within 0 and 1, frequencies within [`TONE_FREQUENCY_HZ`]
    /// and a waiting one only behind a crossfade.
    pub fn is_valid(&self) -> bool {
        let freq = |f: u32| TONE_FREQUENCY_HZ.contains(&f);
        let lowest = level_gain(*TONE_LEVEL_DB.start());
        let f = &self.frequency;
        self.level.within(lowest, 1.0)
            && self.ident.within(0.0, 1.0)
            && freq(f.from)
            && freq(f.to)
            && f.next.is_none_or(|n| freq(n) && f.from != f.to)
    }

    /// Sets `p` from sample index `at`, ramping over `len` samples: the
    /// level and ident from their values at `at`, the frequency
    /// crossfading from the sine before, or after a crossfade still
    /// running, the latest change waiting. False, and nothing set, when
    /// `p` is out of range ([`ToneParams::is_valid`]). Allocates nothing.
    pub fn set(&mut self, at: u64, p: ToneParams, len: u32) -> bool {
        if !p.is_valid() {
            return false;
        }
        self.level.set(level_gain(p.level_db), at, len);
        self.ident.set(if p.ident { 1.0 } else { 0.0 }, at, len);
        let f = &mut self.frequency;
        f.promote(at, len);
        if p.frequency_hz == f.next.unwrap_or(f.to) {
            return true;
        }
        let running = f.from != f.to && at < f.start.saturating_add(u64::from(len));
        if running {
            f.next = (p.frequency_hz != f.to).then_some(p.frequency_hz);
        } else {
            *f = Frequency {
                from: f.to,
                to: p.frequency_hz,
                start: at,
                next: None,
            };
        }
        true
    }
}

/// One generator: its legs, rate and ramp length, and its ramps.
///
/// A generator deserializes only as one [`Tone::is_valid`] takes.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Unchecked")]
pub struct Tone {
    legs: u32,
    rate: u32,
    ramp: u32,
    ramps: ToneRamps,
}

/// A generator as its serde form carries it, before it is checked.
#[derive(Deserialize)]
struct Unchecked {
    legs: u32,
    rate: u32,
    ramp: u32,
    ramps: ToneRamps,
}

impl TryFrom<Unchecked> for Tone {
    type Error = &'static str;

    fn try_from(u: Unchecked) -> Result<Tone, Self::Error> {
        let t = Tone {
            legs: u.legs,
            rate: u.rate,
            ramp: u.ramp,
            ramps: u.ramps,
        };
        if t.is_valid() {
            Ok(t)
        } else {
            Err("a tone generator holding a value no generator holds")
        }
    }
}

impl Tone {
    /// A generator of `legs` legs at `rate` whose changes ramp over
    /// `ramp` samples, at [`ToneParams::DEFAULT`].
    ///
    /// # Panics
    ///
    /// If `legs` is 0 or `rate` is below 100, where the ident's edge
    /// would be shorter than a sample.
    pub fn new(legs: usize, rate: u32, ramp: u32) -> Tone {
        assert!(legs > 0, "a generator of no legs");
        assert!(rate >= 100, "a generator at {rate} Hz");
        Tone {
            legs: u32::try_from(legs).expect("legs within u32"),
            rate,
            ramp,
            ramps: ToneRamps::default(),
        }
    }

    pub fn legs(&self) -> usize {
        self.legs as usize
    }

    pub fn rate(&self) -> u32 {
        self.rate
    }

    pub fn ramp(&self) -> u32 {
        self.ramp
    }

    /// The generator's ramps.
    pub fn ramps(&self) -> ToneRamps {
        self.ramps
    }

    /// Whether the generator holds legs, a rate as [`Tone::new`] takes
    /// it, a ramp length within [`crate::MAX_EXACT_RAMP_LEN`] and valid
    /// ramps ([`ToneRamps::is_valid`]). A generator decoded from a peer
    /// is checked with it.
    pub fn is_valid(&self) -> bool {
        self.legs > 0
            && self.rate >= 100
            && self.ramp <= crate::MAX_EXACT_RAMP_LEN
            && self.ramps.is_valid()
    }

    /// Sets `p` from sample index `at` ([`ToneRamps::set`] over the
    /// generator's ramp length). False, and nothing set, when `p` is out
    /// of range. Allocates nothing.
    pub fn set(&mut self, at: u64, p: ToneParams) -> bool {
        self.ramps.set(at, p, self.ramp)
    }

    /// Replaces the generator's ramps with `ramps`, as a replica that
    /// followed the same parameters holds them. False, and nothing
    /// replaced, when they are not valid. Allocates nothing.
    pub fn resume(&mut self, ramps: ToneRamps) -> bool {
        if !ramps.is_valid() {
            return false;
        }
        self.ramps = ramps;
        true
    }

    /// The sine at index `n`, crossfading while the frequency moves: the
    /// old one before the change's index, as a level holds its old value
    /// there.
    #[inline]
    fn wave(&mut self, n: u64) -> f32 {
        let len = self.ramp;
        self.ramps.frequency.promote(n, len);
        let f = self.ramps.frequency;
        match n.checked_sub(f.start) {
            None => sine(n, f.from, self.rate),
            Some(j) if f.from != f.to && j < u64::from(len) => {
                let up = j as f32 * (1.0 / len as f32);
                sine(n, f.to, self.rate) * up + sine(n, f.from, self.rate) * (1.0 - up)
            }
            Some(_) => sine(n, f.to, self.rate),
        }
    }

    /// The gain of leg `leg` at index `n` while the ident is fully on: 0
    /// in its cut's window, rising linearly over the edge after it and
    /// falling over the edge before it, 1 elsewhere.
    #[inline]
    fn cut(&self, leg: u64, n: u64) -> f32 {
        let q = ident_window(self.rate);
        let edge = ident_edge(self.rate);
        let cycle = u64::from(self.legs) + 1;
        let w = n / q;
        if w % cycle == leg {
            return 0.0;
        }
        let mut d = u64::MAX;
        // The leg's cut ended at the start of this window.
        if w > 0 && (w - 1) % cycle == leg {
            d = d.min(n - w * q + 1);
        }
        // Its cut starts at the end of this window.
        if (w + 1) % cycle == leg {
            d = d.min((w + 1) * q - n);
        }
        if d >= edge {
            1.0
        } else {
            d as f32 / edge as f32
        }
    }

    /// Writes the generator's samples for indices `start..start + frames`
    /// into `dst`, `frames` per leg, leg after leg.
    pub fn process(&mut self, start: u64, dst: &mut [f32], frames: usize) {
        let legs = self.legs as usize;
        debug_assert!(dst.len() >= legs * frames);
        let len = self.ramp;
        for j in 0..frames {
            let n = start + j as u64;
            let v = self.ramps.level.at(n, len) * self.wave(n);
            let ident = self.ramps.ident.at(n, len);
            if ident == 0.0 {
                for leg in 0..legs {
                    dst[leg * frames + j] = v;
                }
                continue;
            }
            for leg in 0..legs {
                let g = 1.0 - ident * (1.0 - self.cut(leg as u64, n));
                dst[leg * frames + j] = v * g;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rates the generator is tested at: the engine runs at 48 kHz
    /// only, the generator at any.
    const RATES: [u32; 6] = [32_000, 44_100, 48_000, 88_200, 96_000, 192_000];
    const RAMP: u32 = 480;

    fn params(frequency_hz: u32, level_db: f32, ident: bool) -> ToneParams {
        ToneParams {
            frequency_hz,
            level_db,
            ident,
        }
    }

    /// The largest step between two consecutive samples of a unit sine of
    /// `f` at `rate`: `2 sin(pi f / rate)`.
    fn step(f: u32, rate: u32) -> f32 {
        (2.0 * libm::sin(core::f64::consts::PI * f64::from(f) / f64::from(rate))) as f32
    }

    /// The samples of `t` over `range`, leg 0..legs, as one block.
    fn run(t: &mut Tone, start: u64, frames: usize) -> Vec<f32> {
        let mut buf = vec![0.0; t.legs() * frames];
        t.process(start, &mut buf, frames);
        buf
    }

    /// The samples of `t` over `from..to`, per leg, each change in
    /// `changes` set at its index before the sample there is computed.
    fn run_through(
        t: &mut Tone,
        from: u64,
        to: u64,
        changes: &[(u64, ToneParams)],
    ) -> Vec<Vec<f32>> {
        let mut legs = vec![Vec::new(); t.legs()];
        let mut n = from;
        while n < to {
            for &(at, p) in changes.iter().filter(|c| c.0 == n) {
                assert!(t.set(at, p));
            }
            let next = (changes.iter().map(|c| c.0))
                .filter(|&at| at > n && at < to)
                .min()
                .unwrap_or(to);
            let frames = (next - n) as usize;
            let got = run(t, n, frames);
            for (l, leg) in legs.iter_mut().enumerate() {
                leg.extend_from_slice(&got[l * frames..(l + 1) * frames]);
            }
            n = next;
        }
        legs
    }

    /// The formula, written out apart from the generator.
    fn formula(level_db: f32, f: u32, rate: u32, n: u64) -> f32 {
        let level = libm::pow(10.0, f64::from(level_db) / 20.0) as f32;
        let p = (u128::from(n) * u128::from(f) % u128::from(rate)) as f64;
        level * libm::sin(2.0 * core::f64::consts::PI * p / f64::from(rate)) as f32
    }

    /// Settled, the value at index `n` is the formula's, bit for bit, at
    /// every rate, at indices near 0, near 2^40 and near the largest index
    /// an engine holds: the phase holds with no drift, and the generator
    /// at rate R is not one at another rate. Fails with `rate` replaced by
    /// 48000 in `sine`, or the phase taken in floating point.
    #[test]
    fn a_settled_tone_is_the_formula_bit_for_bit() {
        for rate in RATES {
            for (f, db) in [(1000, -18.0), (997, 0.0), (20, -60.0), (20_000, -6.0)] {
                let mut t = Tone::new(2, rate, RAMP);
                assert!(t.set(0, params(f, db, false)));
                for base in [0u64, 1 << 40, (1 << 62) - 4096] {
                    // Past the crossfade from the default frequency.
                    let start = base + u64::from(RAMP);
                    let frames = 1024;
                    let got = run(&mut t, start, frames);
                    for j in 0..frames {
                        let n = start + j as u64;
                        let want = formula(db, f, rate, n);
                        for leg in 0..2 {
                            assert_eq!(
                                got[leg * frames + j].to_bits(),
                                want.to_bits(),
                                "rate {rate} f {f} n {n} leg {leg}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The samples are a function of the index and the changes: a
    /// generator that computes every block, ones that skip every other
    /// block, one at another block size and one rebuilt from a copy at
    /// index P compute the same bits, through level, frequency and ident
    /// changes. P lies inside a frequency crossfade with a change waiting
    /// behind it and inside a level and an ident ramp, and the rebuilt one
    /// resumes only after that crossfade's end, as a skip of 400 samples
    /// does too: the waiting crossfade starts at the end of the one
    /// before, wherever the generator first sees it. Fails with `start:
    /// end,` replaced by `start: n,` in `Frequency::promote`, or if a ramp
    /// counted the samples it processed.
    #[test]
    fn the_samples_are_a_function_of_the_index() {
        for rate in RATES {
            let changes = [
                (0, params(1000, -18.0, false)),
                (4800, params(440, -6.0, false)),
                // Inside the crossfade to 440 Hz, so it waits until 5280.
                (5200, params(997, -12.0, true)),
                (30_000, params(1000, -20.0, false)),
            ];
            let total = 40_000u64;
            let reference = |block: usize, skip: bool| {
                let mut t = Tone::new(3, rate, RAMP);
                let mut out = vec![None; total as usize * 3];
                let mut k = 0;
                while k * block < total as usize {
                    let start = (k * block) as u64;
                    for &(at, p) in changes.iter().filter(|c| c.0 == start) {
                        assert!(t.set(at, p));
                    }
                    if !(skip && k % 2 == 1) {
                        let got = run(&mut t, start, block);
                        for leg in 0..3 {
                            for j in 0..block {
                                out[leg * total as usize + start as usize + j] =
                                    Some(got[leg * block + j]);
                            }
                        }
                    }
                    k += 1;
                }
                out
            };
            // The changes lie on block starts of every size used. A skip of
            // the blocks of 400 passes over 5280.
            let whole = reference(80, false);
            for (block, skip) in [(80, true), (16, false), (400, false), (400, true)] {
                let other = reference(block, skip);
                for (n, (a, b)) in whole.iter().zip(&other).enumerate() {
                    if let (Some(a), Some(b)) = (a, b) {
                        assert_eq!(
                            a.to_bits(),
                            b.to_bits(),
                            "rate {rate} block {block} skip {skip} at {n}"
                        );
                    }
                }
            }
            // Rebuilt at P from a serde copy of a running generator, and
            // resumed after the crossfade's end.
            let (p, resume) = (5240u64, 5600u64);
            let mut t = Tone::new(3, rate, RAMP);
            run_through(&mut t, 0, p, &changes);
            let mut copy: Tone = serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
            let unbroken = run_through(&mut t, p, total, &changes);
            let rebuilt = run_through(&mut copy, resume, total, &changes);
            for leg in 0..3 {
                let a = &unbroken[leg][(resume - p) as usize..];
                let b = &rebuilt[leg];
                assert_eq!(a.len(), b.len());
                for (i, (x, y)) in a.iter().zip(b).enumerate() {
                    let n = resume + i as u64;
                    assert_eq!(x.to_bits(), y.to_bits(), "rate {rate} leg {leg} at {n}");
                }
            }
        }
    }

    /// A level change ramps: from the index on the samples move by no more
    /// than the old and new levels' slopes plus the ramp's step, and over
    /// the ramp the envelope reaches the new level linearly. Fails if the
    /// level jumped at the index (`Glide::set` starting from `to`). The
    /// index is off the sine's zero crossings at every rate.
    #[test]
    fn a_level_change_ramps_without_a_step() {
        for rate in RATES {
            let f = 1000;
            let mut t = Tone::new(1, rate, RAMP);
            assert!(t.set(0, params(f, -40.0, false)));
            let at = 10 * u64::from(RAMP) + 7;
            assert!(t.set(at, params(f, 0.0, false)));
            let got = run(&mut t, at - 100, RAMP as usize + 200);
            // The sine's own largest step at full level, and the ramp's.
            // |L(n+1) s(n+1) - L(n) s(n)| <= L |s(n+1) - s(n)| + |L(n+1) -
            // L(n)| |s(n)|, at most the sine's step plus the ramp's.
            let bound = step(f, rate) + (1.0 - level_gain(-40.0)) / RAMP as f32 + 1e-6;
            for w in got.windows(2) {
                assert!(
                    (w[1] - w[0]).abs() <= bound,
                    "rate {rate}: {w:?} beyond {bound}"
                );
            }
            // Halfway the envelope is halfway.
            let half = level_gain(-40.0) + (1.0 - level_gain(-40.0)) * 0.5;
            let n = at + u64::from(RAMP) / 2;
            let want = half * sine(n, f, rate);
            let got = got[(n - (at - 100)) as usize];
            assert!((got - want).abs() < 1e-5, "rate {rate}: {got} {want}");
        }
    }

    /// A frequency change crossfades over the ramp from its index: the
    /// samples step by no more than the faster sine's slope plus the
    /// fade's step times the two sines' span, and from the ramp's end the
    /// new sine is the formula's. One during a crossfade waits for it to
    /// end. The changes are set before the samples ahead of their index
    /// are computed, which carry the old sine. Fails if the frequency
    /// jumped at the index (no crossfade), or with `wave`'s `None` arm
    /// taking `f.to`.
    #[test]
    fn a_frequency_change_crossfades_over_the_ramp() {
        for rate in RATES {
            let (a, b) = (1000, 5000);
            let mut t = Tone::new(1, rate, RAMP);
            assert!(t.set(0, params(a, 0.0, false)));
            let at = 10 * u64::from(RAMP) + 7;
            assert!(t.set(at, params(b, 0.0, false)));
            // A change inside the crossfade waits for its end.
            assert!(t.set(at + 100, params(a, 0.0, false)));
            assert!(t.set(at + 200, params(3000, 0.0, false)));
            let start = at - 50;
            let got = run(&mut t, start, 3 * RAMP as usize);
            // u s_new + (1 - u) s_old steps by at most the larger sine's
            // step, plus the weight's step 1 / RAMP times the sines' span 2.
            let bound = step(b, rate) + 2.0 / RAMP as f32 + 1e-5;
            for (i, w) in got.windows(2).enumerate() {
                assert!(
                    (w[1] - w[0]).abs() <= bound,
                    "rate {rate} at {}: {w:?} beyond {bound}",
                    start + i as u64
                );
            }
            // The first crossfade ends on 5000 Hz, the waiting 3000 Hz
            // crossfades from there over the next ramp.
            let end = at + u64::from(RAMP);
            let n = end - 1;
            let up = (RAMP - 1) as f32 * (1.0 / RAMP as f32);
            let want = sine(n, b, rate) * up + sine(n, a, rate) * (1.0 - up);
            assert_eq!(got[(n - start) as usize].to_bits(), want.to_bits());
            assert_eq!(
                got[(end - start) as usize].to_bits(),
                sine(end, b, rate).to_bits(),
                "the waiting change starts from the first one's end"
            );
            let settled = end + u64::from(RAMP);
            for n in settled..start + 3 * u64::from(RAMP) {
                assert_eq!(
                    got[(n - start) as usize].to_bits(),
                    formula(0.0, 3000, rate, n).to_bits()
                );
            }
        }
    }

    /// With the ident on, leg `l` is exactly silent over `[k Q, (k + 1)
    /// Q)` for each `k` with `k mod (legs + 1) = l`, `Q = R / 4`, and at
    /// full level at least an edge away from every one of its cuts: the
    /// legs in turn, in leg order, then a window on every leg. Fails with
    /// 48000 in place of the rate in `ident_window`, at the other rates.
    #[test]
    fn the_ident_cuts_each_leg_in_turn() {
        for rate in RATES {
            for legs in [1usize, 2, 3] {
                let q = u64::from(rate) / 4;
                let edge = u64::from(rate) / 100;
                let mut t = Tone::new(legs, rate, 0);
                assert!(t.set(0, params(997, 0.0, true)));
                let cycle = legs as u64 + 1;
                let first = 7 * cycle * q;
                let frames = (2 * cycle * q) as usize;
                let got = run(&mut t, first, frames);
                for leg in 0..legs {
                    for j in 0..frames {
                        let n = first + j as u64;
                        let w = n / q;
                        let v = got[leg * frames + j];
                        let clean = formula(0.0, 997, rate, n);
                        if w % cycle == leg as u64 {
                            assert_eq!(v, 0.0, "rate {rate} leg {leg} {n}");
                            continue;
                        }
                        let near = |k: u64| k % cycle == leg as u64;
                        let after = n - w * q < edge && w > 0 && near(w - 1);
                        let before = (w + 1) * q - n <= edge && near(w + 1);
                        if !after && !before {
                            assert_eq!(v.to_bits(), clean.to_bits(), "rate {rate} leg {leg} {n}");
                        } else {
                            assert!(v.abs() <= clean.abs(), "rate {rate} leg {leg} {n}");
                        }
                    }
                }
            }
        }
    }

    /// The ident's edges step by no more than the edge allows: at 1 kHz
    /// and at 997 Hz, whose cuts fall off its zero crossings, at every
    /// rate, the largest step of a leg is within the sine's slope plus the
    /// level over the edge length, and well below the full-scale step a
    /// hard cut makes. Fails with the edge ramp dropped (`cut` returning 1
    /// outside the window).
    #[test]
    fn the_ident_edges_are_bounded() {
        for rate in RATES {
            for f in [1000, 997] {
                let legs = 2;
                let mut t = Tone::new(legs, rate, 0);
                assert!(t.set(0, params(f, 0.0, true)));
                let q = u64::from(rate) / 4;
                let start = 3 * 3 * q - 2 * u64::from(rate) / 100;
                let frames = (3 * q) as usize + 4 * rate as usize / 100;
                let got = run(&mut t, start, frames);
                let bound = step(f, rate) + 1.0 / ident_edge(rate) as f32 + 1e-6;
                for leg in 0..legs {
                    let leg = &got[leg * frames..(leg + 1) * frames];
                    let worst = leg
                        .windows(2)
                        .map(|w| (w[1] - w[0]).abs())
                        .fold(0.0f32, f32::max);
                    assert!(worst <= bound, "rate {rate} f {f}: {worst} beyond {bound}");
                }
            }
        }
    }

    /// Switching the ident on or off ramps over the ramp length, inside
    /// the cut windows of leg 0, where the switch changes the output: on,
    /// halfway through the ramp the leg is at half its level, then
    /// silent; off, back to the sine; on and off again a quarter into the
    /// ramp, the off ramp starts from the quarter it reached. No step
    /// beyond the sine's and the ramp's. Fails with `self.ramps.ident.at(n,
    /// len)` replaced by `self.ramps.ident.at(n, 0)` in `process`, or with
    /// `Glide::set` starting from `self.at(at, 0)`.
    #[test]
    fn switching_the_ident_ramps() {
        for rate in RATES {
            let q = u64::from(rate) / 4;
            let half = u64::from(RAMP) / 2;
            // Leg 0 of a mono generator is cut in the even windows.
            let on = 2 * q + q / 3;
            let off = 4 * q + q / 3;
            let on2 = 6 * q + q / 3;
            let off2 = on2 + u64::from(RAMP) / 4;
            let ident = |ident| params(997, 0.0, ident);
            let changes = [
                (on, ident(true)),
                (off, ident(false)),
                (on2, ident(true)),
                (off2, ident(false)),
            ];
            let mut t = Tone::new(1, rate, RAMP);
            assert!(t.set(0, ident(false)));
            let from = on - 10;
            let got = &run_through(&mut t, from, off2 + 2 * u64::from(RAMP), &changes)[0];
            let at = |n: u64| got[(n - from) as usize];
            let v = |n: u64| formula(0.0, 997, rate, n);
            // The switch's ramp, or a cut's edge while the ident is on.
            let edge = u64::from(RAMP).min(ident_edge(rate));
            let bound = step(997, rate) + 1.0 / edge as f32 + 1e-6;
            for (i, w) in got.windows(2).enumerate() {
                let n = from + i as u64;
                assert!(
                    (w[1] - w[0]).abs() <= bound,
                    "rate {rate} at {n}: {w:?} beyond {bound}"
                );
            }
            // The ident's weight `j` samples into a switch from `from` to
            // `to`, and the leg in its cut at that weight, as the
            // generator computes them.
            let glide = |from: f32, to: f32, j: u64| from + j as f32 * ((to - from) / RAMP as f32);
            let cut = |n: u64, w: f32| v(n) * (1.0 - w * (1.0 - 0.0));
            assert_eq!(at(on - 1).to_bits(), v(on - 1).to_bits());
            let n = on + half;
            assert_eq!(at(n).to_bits(), cut(n, glide(0.0, 1.0, half)).to_bits());
            assert_eq!(at(on + u64::from(RAMP)), 0.0, "cut once the ramp ended");
            let n = off + half;
            assert_eq!(at(n).to_bits(), cut(n, glide(1.0, 0.0, half)).to_bits());
            let n = off + u64::from(RAMP);
            assert_eq!(at(n).to_bits(), v(n).to_bits(), "sounding once it ended");
            // Off a quarter into the on ramp: from the weight reached there.
            let reached = glide(0.0, 1.0, off2 - on2);
            let n = off2 + 1;
            assert_eq!(at(n).to_bits(), cut(n, glide(reached, 0.0, 1)).to_bits());
        }
    }

    /// Out-of-range parameters are refused and set nothing; clamped, a
    /// finite value moves into its range.
    #[test]
    fn parameters_out_of_range_are_refused() {
        let mut t = Tone::new(1, 48_000, RAMP);
        let held = t;
        for p in [
            params(19, -18.0, false),
            params(20_001, -18.0, false),
            params(1000, 0.1, false),
            params(1000, -60.1, false),
            params(1000, f32::NAN, false),
            params(1000, f32::INFINITY, false),
        ] {
            assert!(p.check().is_err(), "{p:?}");
            assert!(!t.set(960, p), "{p:?}");
            assert_eq!(t, held);
        }
        for p in [params(20, -60.0, false), params(20_000, 0.0, true)] {
            assert!(p.check().is_ok(), "{p:?}");
        }
        assert_eq!(params(5, 3.0, true).clamped(), Some(params(20, 0.0, true)));
        assert_eq!(params(1000, f32::NAN, false).clamped(), None);
    }

    /// A decoded generator is checked: one holding a level above full
    /// scale or a frequency out of range is refused.
    #[test]
    fn a_decoded_generator_is_checked() {
        let t = Tone::new(2, 48_000, RAMP);
        let mut v = serde_json::to_value(t).unwrap();
        let back: Tone = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(back, t);
        v["ramps"]["level"]["to"] = serde_json::json!(2.0);
        assert!(serde_json::from_value::<Tone>(v).is_err());
        let mut v = serde_json::to_value(t).unwrap();
        v["ramps"]["frequency"]["to"] = serde_json::json!(19);
        assert!(serde_json::from_value::<Tone>(v).is_err());
    }
}
