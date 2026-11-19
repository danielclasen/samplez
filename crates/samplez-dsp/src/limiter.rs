//! The output limiter: a feed-forward look-ahead limiter on the true
//! peak, linked over the legs of one output of width 1 to
//! [`MAX_LEGS`].
//!
//! The sidechain at sample index `m` is the largest of `|x[m]|`,
//! `|x[m + 1]|` and the four phases of the ITU-R BS.1770-5 Annex 2
//! interpolator ([`crate::TruePeak`]'s filter) between them, over every
//! leg: the peak of the 4x oversampled signal over `[m, m + 1]`. Its
//! required gain is `q[m] = min(1, c / s[m])` for the ceiling `c`. The
//! gain is the smallest `q` of the last [`LOOKAHEAD`] + 1 indices, then
//! released (taken at once when lower, else moving towards it by a
//! one-pole of the release time in f64, which an f32 pole would stall
//! short of unity, and snapping onto it within 1e-6), then
//! averaged over the last [`LOOKAHEAD`] indices, and it is applied to the
//! input [`LIMITER_DELAY`] samples late. Every value averaged for sample
//! `m` is at most `q[m - 1]` and `q[m]`, so the gain at sample `m` is too
//! (the average is also capped by both, against rounding): every output
//! sample is at most the ceiling, within float rounding. The interpolated
//! output between two samples stays within a few hundredths of a dB of
//! it (the tests measure the bound), since the gain moves by at most an
//! average's step from one sample to the next.
//!
//! The latency is [`LIMITER_DELAY`] samples whether the limiter is on or
//! off. Switching it on or off ramps its effect over the ramp length;
//! ceiling and release take effect at the next sample computed.
//!
//! Every value is a function of the sample index and the input samples
//! from [`LIMITER_DELAY`] + 11 before it, except the released gain, which
//! a lower required gain takes at once: two limiters fed the same samples
//! hold the same bits from the first sample whose required gain lies
//! below both released gains, whatever the block split. The ring buffers
//! are indexed by the sample index, the operations scalar f32 in a fixed
//! order without FMA, the ceiling and release pole from libm in f64. A
//! jump of the sample index (an overrun skip, a clock step) restarts the
//! limiter from silence. A sample that is not finite is taken as 0.

use core::ops::RangeInclusive;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::level::db_to_gain;
use crate::strip::{MAX_LEGS, within};
use crate::svf::SAMPLE_RATE;
use crate::true_peak::{HISTORY, phase_peaks};
use crate::{Path, Ramp};

/// Samples the gain is held and averaged over before the peak that
/// needs it: 1 ms.
pub const LOOKAHEAD: usize = 48;

/// The interpolator's outputs at index `n` lie between `x[n - LAG]` and
/// `x[n - LAG + 1]`.
const LAG: usize = 6;

/// The samples an output carrying a limiter is late: the look-ahead
/// and the interpolator's lag, less one, about 1.1 ms.
pub const LIMITER_DELAY: usize = LOOKAHEAD + LAG - 1;

/// Ring length, a power of two holding a chunk and the delay before it.
const RING: usize = 128;
const MASK: u64 = RING as u64 - 1;

/// Ceiling in dBTP.
pub const LIMITER_CEILING_DB: RangeInclusive<f32> = -20.0..=0.0;
/// Release time constant in ms.
pub const LIMITER_RELEASE_MS: RangeInclusive<f32> = 1.0..=1000.0;

/// The released gain snaps onto the held one within this.
const SNAP: f64 = 1.0e-6;

/// Limiter parameters, set by one entry as absolute values.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LimiterParams {
    pub enabled: bool,
    /// The largest true peak let through, dBTP, within
    /// [`LIMITER_CEILING_DB`].
    pub ceiling_db: f32,
    /// Time constant of the gain's recovery, ms, within
    /// [`LIMITER_RELEASE_MS`].
    pub release_ms: f32,
}

impl LimiterParams {
    /// A limiter's parameters before any is set: off, 0 dBTP, 50 ms.
    pub const OFF: LimiterParams = LimiterParams {
        enabled: false,
        ceiling_db: 0.0,
        release_ms: 50.0,
    };

    /// Whether a limiter takes the parameters: values within their
    /// ranges, no NaN.
    pub fn check(&self) -> Result<(), String> {
        within("limiter ceiling", self.ceiling_db, &LIMITER_CEILING_DB)
            .and_then(|()| within("limiter release", self.release_ms, &LIMITER_RELEASE_MS))
            .map_err(|r| r.message())
    }
}

/// The one-pole coefficient of the release, per sample.
fn release_pole(ms: f32) -> f64 {
    libm::exp(-1000.0 / (f64::from(ms) * SAMPLE_RATE))
}

/// The largest sum of the absolute coefficients of a phase of the
/// interpolator, rounded up: a phase output is at most this times the
/// largest of its inputs.
const PHASE_GAIN: f32 = 2.03;

/// Samples of a block taken through the sidechain at once.
const CHUNK: usize = 64;
const _: () = assert!(RING >= CHUNK + LIMITER_DELAY && RING > LOOKAHEAD + 1);

/// The released gain in the box average as a fixed-point fraction of
/// this: truncated, so the average stays at or below the exact one.
const ONE: u32 = 1 << 24;
/// The box average's sum of [`LOOKAHEAD`] gains of 1.
const FULL: u32 = LOOKAHEAD as u32 * ONE;

/// One output's limiter. It holds no heap memory, so a clone allocates
/// nothing. A limiter deserializes only as one [`Limiter::is_valid`]
/// takes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Unchecked")]
pub struct Limiter {
    legs: usize,
    ramp: u32,
    params: LimiterParams,
    /// The ceiling as a gain and the release pole, of `params`.
    ceiling: f32,
    release: f64,
    /// The released gain of the last index taken.
    released: f64,
    /// The limiter's effect, ramping between 0 and 1.
    wet: Ramp,
    /// The input samples by index modulo [`RING`], legs side by side.
    #[serde(with = "rows")]
    x: [[f32; MAX_LEGS]; RING],
    /// The required gains by index modulo [`RING`].
    #[serde(with = "row")]
    q: [f32; RING],
    /// The released gains by index modulo [`RING`], in units of 1 /
    /// [`ONE`], and the sum of the last [`LOOKAHEAD`] of them.
    #[serde(with = "fixed")]
    r: [u32; RING],
    sum: u32,
    /// Every required and released gain is 1: the sidechain is at rest.
    rest: bool,
    /// The index of the next sample processed.
    pos: Option<u64>,
    /// The indices of the required gains in the window that no later one
    /// is at most: the front holds the window's smallest. A function of
    /// the gains and the position, rebuilt when decoded.
    #[serde(skip)]
    mins: Mins,
}

/// A monotonic queue of indices, ascending in index and in required
/// gain, in a ring of [`RING`].
#[derive(Clone, Copy, Debug)]
struct Mins {
    at: [u64; RING],
    head: usize,
    len: usize,
}

impl Default for Mins {
    fn default() -> Mins {
        Mins {
            at: [0; RING],
            head: 0,
            len: 0,
        }
    }
}

/// Equal when they hold the same indices, wherever in the ring.
impl PartialEq for Mins {
    fn eq(&self, other: &Mins) -> bool {
        self.len == other.len
            && (0..self.len)
                .all(|i| self.at[(self.head + i) % RING] == other.at[(other.head + i) % RING])
    }
}

impl Mins {
    /// Takes index `m`, whose required gain `q[m]` was just stored, and
    /// returns the smallest required gain of `m - LOOKAHEAD..=m`.
    #[inline]
    fn push(&mut self, m: u64, q: &[f32; RING]) -> f32 {
        let qm = q[(m & MASK) as usize];
        while self.len > 0 {
            let back = self.at[(self.head + self.len - 1) % RING];
            if q[(back & MASK) as usize] < qm {
                break;
            }
            self.len -= 1;
        }
        self.at[(self.head + self.len) % RING] = m;
        self.len += 1;
        while m.wrapping_sub(self.at[self.head]) > LOOKAHEAD as u64 {
            self.head = (self.head + 1) % RING;
            self.len -= 1;
        }
        q[(self.at[self.head] & MASK) as usize]
    }
}

/// A limiter as its serde form carries it, before it is checked.
#[derive(Deserialize)]
struct Unchecked {
    legs: usize,
    ramp: u32,
    params: LimiterParams,
    ceiling: f32,
    release: f64,
    released: f64,
    wet: Ramp,
    #[serde(with = "rows")]
    x: [[f32; MAX_LEGS]; RING],
    #[serde(with = "row")]
    q: [f32; RING],
    #[serde(with = "fixed")]
    r: [u32; RING],
    sum: u32,
    rest: bool,
    pos: Option<u64>,
}

impl TryFrom<Unchecked> for Limiter {
    type Error = &'static str;

    fn try_from(u: Unchecked) -> Result<Limiter, Self::Error> {
        let mut l = Limiter {
            legs: u.legs,
            ramp: u.ramp,
            params: u.params,
            ceiling: u.ceiling,
            release: u.release,
            released: u.released,
            wet: u.wet,
            x: u.x,
            q: u.q,
            r: u.r,
            sum: u.sum,
            rest: u.rest,
            pos: u.pos,
            mins: Mins::default(),
        };
        if !l.is_valid() {
            return Err("a limiter state no limiter holds");
        }
        l.rebuild_mins();
        Ok(l)
    }
}

impl Limiter {
    /// A limiter of `legs` legs at [`LimiterParams::OFF`] whose switch
    /// ramps over `ramp` samples, silent and at rest.
    ///
    /// # Panics
    ///
    /// If `legs` is 0 or above [`MAX_LEGS`].
    pub fn new(legs: usize, ramp: u32) -> Limiter {
        assert!(
            (1..=MAX_LEGS).contains(&legs),
            "a limiter has 1 to {MAX_LEGS} legs, not {legs}"
        );
        let p = LimiterParams::OFF;
        Limiter {
            legs,
            ramp,
            params: p,
            ceiling: db_to_gain(p.ceiling_db),
            release: release_pole(p.release_ms),
            released: 1.0,
            wet: Ramp::new(0.0),
            x: [[0.0; MAX_LEGS]; RING],
            q: [1.0; RING],
            r: [ONE; RING],
            sum: FULL,
            rest: true,
            pos: None,
            mins: Mins::default(),
        }
    }

    pub fn legs(&self) -> usize {
        self.legs
    }

    /// The samples switching on or off ramps over.
    pub fn ramp(&self) -> u32 {
        self.ramp
    }

    pub fn params(&self) -> LimiterParams {
        self.params
    }

    /// Sets `params` from the next sample processed on. Returns false and
    /// changes nothing when [`LimiterParams::check`] refuses them; a
    /// refusal does not allocate.
    #[must_use]
    pub fn set(&mut self, params: LimiterParams) -> bool {
        let ok = |r: &RangeInclusive<f32>, v: f32| r.contains(&v);
        if !ok(&LIMITER_CEILING_DB, params.ceiling_db)
            || !ok(&LIMITER_RELEASE_MS, params.release_ms)
        {
            return false;
        }
        self.params = params;
        self.ceiling = db_to_gain(params.ceiling_db);
        self.release = release_pole(params.release_ms);
        let _ = (self.wet).set_target(if params.enabled { 1.0 } else { 0.0 }, self.ramp);
        true
    }

    /// Sets `params` from sample index `index` on, the switch moved on
    /// over the samples before it not processed and the limiter
    /// restarted from silence there, as [`Limiter::process`] does over a
    /// jump; at an index already processed, from the next sample. Returns
    /// false and changes nothing when [`LimiterParams::check`] refuses
    /// them.
    #[must_use]
    pub fn set_at(&mut self, index: u64, params: LimiterParams) -> bool {
        if params.check().is_err() {
            return false;
        }
        if let Some(pos) = self.pos
            && index > pos
        {
            self.wet
                .advance((index - pos).min(u64::from(u32::MAX)) as usize);
            self.restart();
            self.pos = Some(index);
        }
        self.set(params)
    }

    /// Whether the limiter holds what one built by [`Limiter::new`] and
    /// changed by its methods holds: 1 to [`MAX_LEGS`] legs, parameters
    /// within their ranges and the ceiling and pole of them, the switch
    /// ramping within `[0, 1]` to the state `params` names, finite
    /// samples, unused legs silent, gains within `[0, 1]`, the sum that
    /// of the window, and all 1 at rest. A limiter decoded from a peer is
    /// checked with it.
    pub fn is_valid(&self) -> bool {
        let gain = |g: &f32| (0.0..=1.0).contains(g);
        let on = if self.params.enabled { 1.0 } else { 0.0 };
        let last = self.last();
        let window: u64 = (0..LOOKAHEAD as u64)
            .map(|k| u64::from(self.r[(last.wrapping_sub(k) & MASK) as usize]))
            .sum();
        (1..=MAX_LEGS).contains(&self.legs)
            && self.params.check().is_ok()
            && self.ceiling.to_bits() == db_to_gain(self.params.ceiling_db).to_bits()
            && self.release.to_bits() == release_pole(self.params.release_ms).to_bits()
            && self.wet.is_within(0.0..=1.0)
            && self.wet.target() == on
            && (self.x.iter()).all(|row| {
                row[..self.legs].iter().all(|v| v.is_finite())
                    && row[self.legs..].iter().all(|&v| v == 0.0)
            })
            && self.q.iter().all(gain)
            && self.r.iter().all(|&r| r <= ONE)
            && window == u64::from(self.sum)
            && (0.0..=1.0).contains(&self.released)
            && (!self.rest
                || self.q.iter().all(|&g| g == 1.0)
                    && self.r.iter().all(|&r| r == ONE)
                    && self.released == 1.0)
    }

    /// The index of the last required gain taken: [`LAG`] before the
    /// last sample processed.
    fn last(&self) -> u64 {
        self.pos.unwrap_or(0).wrapping_sub(1 + LAG as u64)
    }

    /// The queue of the window's smallest required gains, from the gains
    /// held: empty at rest, where every one is 1.
    fn rebuild_mins(&mut self) {
        self.mins = Mins::default();
        if self.rest {
            return;
        }
        let last = self.last();
        for k in (0..=LOOKAHEAD as u64).rev() {
            self.mins.push(last.wrapping_sub(k), &self.q);
        }
    }

    /// Whether the limiter passes its input on unchanged, only late: off
    /// and not ramping.
    fn is_off(&self) -> bool {
        !self.wet.is_active() && self.wet.target() == 0.0
    }

    /// Silent and at rest, the switch and parameters kept.
    fn restart(&mut self) {
        self.x = [[0.0; MAX_LEGS]; RING];
        self.rest();
    }

    fn rest(&mut self) {
        self.q = [1.0; RING];
        self.r = [ONE; RING];
        self.sum = FULL;
        self.released = 1.0;
        self.mins = Mins::default();
        self.rest = true;
    }

    /// Processes `frames` samples of each leg from sample index `start`
    /// in place: `buf` holds the legs one after the other, `frames`
    /// samples each, and holds the limited output [`LIMITER_DELAY`]
    /// samples late afterwards. An index other than the next one
    /// restarts the limiter from silence, its switch moved on over the
    /// samples skipped.
    ///
    /// # Panics
    ///
    /// If `buf` is shorter than `legs * frames`.
    pub fn process(&mut self, start: u64, buf: &mut [f32], frames: usize) {
        let buf = &mut buf[..self.legs * frames];
        if self.pos != Some(start) {
            if let Some(pos) = self.pos
                && start > pos
            {
                self.wet
                    .advance((start - pos).min(u64::from(u32::MAX)) as usize);
            }
            self.restart();
        }
        let off = self.is_off();
        if off && !self.rest {
            self.rest();
        }
        let path = Path::best();
        let mut n = 0;
        while n < frames {
            let len = CHUNK.min(frames - n);
            self.chunk(path, start, buf, frames, n, len, off);
            n += len;
        }
        self.pos = Some(start + frames as u64);
        self.wet.advance(frames);
    }

    /// Samples `n..n + len` of the block of `frames` from `start` in
    /// `buf`, `off` or not.
    #[allow(clippy::too_many_arguments)]
    fn chunk(
        &mut self,
        path: Path,
        start: u64,
        buf: &mut [f32],
        frames: usize,
        n: usize,
        len: usize,
        off: bool,
    ) {
        let first = start + n as u64;
        let at = |index: u64| (index & MASK) as usize;
        for leg in 0..self.legs {
            for i in 0..len {
                let v = buf[leg * frames + n + i];
                self.x[at(first + i as u64)][leg] = if v.is_finite() { v } else { 0.0 };
            }
        }
        let mut gains = [1.0f32; CHUNK];
        if !off {
            self.rest = false;
            // Per sample the peak over [m, m + 1], m = index - LAG.
            let mut s = [0.0f32; CHUNK];
            for leg in 0..self.legs {
                let mut w = [0.0f32; HISTORY + CHUNK];
                let w = &mut w[..HISTORY + len];
                let mut largest = 0.0f32;
                for (i, v) in w.iter_mut().enumerate() {
                    *v = self.x[at((first + i as u64).wrapping_sub(HISTORY as u64))][leg];
                    largest = largest.max(v.abs());
                }
                // Below the ceiling whatever the phases: none is needed.
                if largest * PHASE_GAIN <= self.ceiling {
                    continue;
                }
                phase_peaks(path, w, &mut s[..len]);
                for (i, p) in s[..len].iter_mut().enumerate() {
                    let a = w[HISTORY + i - LAG]
                        .abs()
                        .max(w[HISTORY + i - LAG + 1].abs());
                    if a > *p {
                        *p = a;
                    }
                }
            }
            for i in 0..len {
                gains[i] = self.gain(first + i as u64, s[i]);
            }
        }
        for i in 0..len {
            let w = self.wet.value_at(self.wet.pos + (n + i) as u32);
            let k = 1.0 + w * (gains[i] - 1.0);
            let late = &self.x[at((first + i as u64).wrapping_sub(LIMITER_DELAY as u64))];
            for leg in 0..self.legs {
                let y = &mut buf[leg * frames + n + i];
                *y = if k == 1.0 { late[leg] } else { late[leg] * k };
            }
        }
    }

    /// Takes the sidechain peak `s` of the sample at `index` and returns
    /// the gain of the sample [`LIMITER_DELAY`] before it.
    #[inline]
    fn gain(&mut self, index: u64, s: f32) -> f32 {
        let m = index.wrapping_sub(LAG as u64);
        let back = |k: usize| (m.wrapping_sub(k as u64) & MASK) as usize;
        self.q[back(0)] = if s > self.ceiling {
            self.ceiling / s
        } else {
            1.0
        };
        let held = f64::from(self.mins.push(m, &self.q));
        let last = self.released;
        self.released = if held < last {
            held
        } else {
            let next = held + self.release * (last - held);
            if held - next < SNAP { held } else { next }
        };
        let r = (self.released * f64::from(ONE)) as u32;
        self.sum = self.sum - self.r[back(LOOKAHEAD)] + r;
        self.r[back(0)] = r;
        let edge = LOOKAHEAD - 1;
        (self.sum as f32 / FULL as f32)
            .min(self.q[back(edge)])
            .min(self.q[back(edge + 1)])
    }
}

/// Serde of the fixed-point gains as a list.
mod fixed {
    use super::*;

    pub fn serialize<S: Serializer>(x: &[u32; RING], s: S) -> Result<S::Ok, S::Error> {
        x.as_slice().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u32; RING], D::Error> {
        Vec::<u32>::deserialize(d)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("a limiter ring of another length"))
    }
}

/// Serde of a ring of rows as one flat list.
mod rows {
    use super::*;

    pub fn serialize<S: Serializer>(x: &[[f32; MAX_LEGS]; RING], s: S) -> Result<S::Ok, S::Error> {
        x.as_flattened().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<[[f32; MAX_LEGS]; RING], D::Error> {
        let flat = Vec::<f32>::deserialize(d)?;
        if flat.len() != MAX_LEGS * RING {
            return Err(serde::de::Error::custom("a limiter ring of another length"));
        }
        let mut x = [[0.0; MAX_LEGS]; RING];
        x.as_flattened_mut().copy_from_slice(&flat);
        Ok(x)
    }
}

/// Serde of a ring as a list.
mod row {
    use super::*;

    pub fn serialize<S: Serializer>(x: &[f32; RING], s: S) -> Result<S::Ok, S::Error> {
        x.as_slice().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[f32; RING], D::Error> {
        Vec::<f32>::deserialize(d)?
            .try_into()
            .map_err(|_| serde::de::Error::custom("a limiter ring of another length"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TruePeak;
    use crate::test_util::Rng;

    const RAMP: u32 = 480;

    fn on(ceiling_db: f32, release_ms: f32) -> LimiterParams {
        LimiterParams {
            enabled: true,
            ceiling_db,
            release_ms,
        }
    }

    /// `x`, legs one after the other, through `l` in blocks of `b` from
    /// sample index `start`.
    fn run(l: &mut Limiter, start: u64, x: &[f32], b: usize) -> Vec<f32> {
        let legs = l.legs();
        let len = x.len() / legs;
        let mut out = vec![0.0; x.len()];
        let mut n = 0;
        while n < len {
            let f = b.min(len - n);
            let mut block: Vec<f32> = (0..legs)
                .flat_map(|leg| x[leg * len + n..leg * len + n + f].iter().copied())
                .collect();
            l.process(start + n as u64, &mut block, f);
            for leg in 0..legs {
                out[leg * len + n..leg * len + n + f]
                    .copy_from_slice(&block[leg * f..(leg + 1) * f]);
            }
            n += f;
        }
        out
    }

    /// A stereo programme with peaks up to +12 dBFS: noise bursts, a
    /// quarter-rate sine at 45 degrees (+3 dB between its samples), an
    /// alternating full-scale square and decaying clicks.
    fn programme(len: usize, seed: u64) -> Vec<f32> {
        let mut rng = Rng::new(seed);
        let mut x = vec![0.0f32; 2 * len];
        for n in 0..len {
            let part = n / 4800 % 4;
            for leg in 0..2 {
                let v = match part {
                    0 => rng.unit() * if n % 9600 < 600 { 4.0 } else { 0.3 },
                    1 => {
                        let phase = core::f32::consts::FRAC_PI_4
                            + (n as f32) * core::f32::consts::FRAC_PI_2;
                        2.0 * phase.sin()
                    }
                    2 => {
                        if (n / 2) % 2 == 0 {
                            1.0
                        } else {
                            -1.0
                        }
                    }
                    _ => {
                        let t = n % 1200;
                        if t < 40 {
                            3.0 * (1.0 - t as f32 / 40.0)
                        } else {
                            0.0
                        }
                    }
                };
                x[leg * len + n] = if leg == 1 { -v * 0.8 } else { v };
            }
        }
        x
    }

    fn db(v: f32) -> f64 {
        20.0 * f64::from(v).log10()
    }

    /// Every output sample of a limiter on is at most its ceiling, over a
    /// programme up to 32 dB over it and every block size. Limiting on
    /// the true peak, the sample peak can stay below it.
    #[test]
    fn no_sample_exceeds_the_ceiling() {
        let x = programme(4 * 4800, 3);
        for (ceiling, b) in [(-1.0, 48), (0.0, 16), (-9.0, 1024), (-20.0, 7)] {
            let mut l = Limiter::new(2, 0);
            assert!(l.set(on(ceiling, 50.0)));
            let y = run(&mut l, 0, &x, b);
            let c = db_to_gain(ceiling);
            let peak = y.iter().fold(0.0f32, |m, v| m.max(v.abs()));
            assert!(peak <= c * (1.0 + 1e-6), "peak {peak} over {c}");
            assert!(peak > c * 0.8, "peak {peak} far below {c}");
        }
    }

    /// The true peak of the output, measured as the meters measure it,
    /// stays within 0.05 dB of the ceiling, over a programme of
    /// inter-sample peaks.
    #[test]
    fn the_true_peak_stays_at_the_ceiling() {
        let len = 4 * 4800;
        let x = programme(len, 5);
        let mut l = Limiter::new(2, 0);
        assert!(l.set(on(-1.0, 20.0)));
        let y = run(&mut l, 0, &x, 48);
        let mut tp = TruePeak::new(1, len);
        let peak = (0..2)
            .map(|leg| {
                tp.reset();
                tp.process(0, &y[leg * len..(leg + 1) * len])
            })
            .fold(0.0f32, f32::max);
        let mut raw = TruePeak::new(1, len);
        assert!(db(raw.process(0, &x[..len])) > 11.0);
        assert!(db(peak) <= -1.0 + 0.05, "true peak {} dBTP", db(peak));
    }

    /// Below the ceiling a limiter on passes its input unchanged, only
    /// [`LIMITER_DELAY`] samples late, bit for bit.
    #[test]
    fn below_the_ceiling_the_input_passes_late() {
        let mut rng = Rng::new(9);
        let len: usize = 3000;
        let x: Vec<f32> = (0..3 * len).map(|_| rng.unit() * 0.25).collect();
        let mut l = Limiter::new(3, RAMP);
        assert!(l.set(on(0.0, 50.0)));
        // Switched on before the first sample: no ramp.
        l.wet = Ramp::new(1.0);
        let y = run(&mut l, 100, &x, 48);
        for leg in 0..3 {
            for n in 0..len {
                let want = n
                    .checked_sub(LIMITER_DELAY)
                    .map_or(0.0, |m| x[leg * len + m]);
                assert_eq!(
                    y[leg * len + n].to_bits(),
                    want.to_bits(),
                    "leg {leg} sample {n}"
                );
            }
        }
    }

    /// The output depends on neither the block size nor the split: the
    /// rings follow the sample index.
    #[test]
    fn the_output_does_not_depend_on_the_blocks() {
        let x = programme(3 * 4800, 11);
        let reference = {
            let mut l = Limiter::new(2, RAMP);
            assert!(l.set(on(-3.0, 80.0)));
            run(&mut l, 4800, &x, 48)
        };
        for b in [1, 16, 47, 1024] {
            let mut l = Limiter::new(2, RAMP);
            assert!(l.set(on(-3.0, 80.0)));
            let y = run(&mut l, 4800, &x, b);
            assert!(
                y.iter()
                    .zip(&reference)
                    .all(|(a, b)| a.to_bits() == b.to_bits()),
                "block size {b}"
            );
        }
    }

    /// A limiter started cold in the middle of a programme holds the
    /// same bits as one that ran from the start once a peak deeper than
    /// both released gains came: what a rebuilt replica relies on.
    #[test]
    fn a_cold_limiter_converges_on_a_running_one() {
        let len = 4 * 4800;
        let mut x = programme(len, 13);
        // A deep peak late in the programme.
        let deep = 3 * 4800 + 2000;
        x[deep] = 30.0;
        x[len + deep] = -30.0;
        let mut live = Limiter::new(2, 0);
        assert!(live.set(on(-1.0, 300.0)));
        let y = run(&mut live, 0, &x, 48);
        let from = 4800 * 2 + 17;
        let tail: Vec<f32> = (0..2)
            .flat_map(|leg| x[leg * len + from..(leg + 1) * len].iter().copied())
            .collect();
        let mut cold = Limiter::new(2, 0);
        assert!(cold.set(on(-1.0, 300.0)));
        let z = run(&mut cold, from as u64, &tail, 48);
        let rest = len - from;
        let differ = |n: usize| (0..2).any(|leg| y[leg * len + from + n] != z[leg * rest + n]);
        assert!(differ(0) || (0..rest).any(differ), "the start differs");
        let equal_from = deep - from + LOOKAHEAD;
        assert!(
            (equal_from..rest).all(|n| !differ(n)),
            "not equal after the deep peak"
        );
    }

    /// After a single peak the gain holds through the look-ahead, then
    /// recovers with the release time constant: at one time constant
    /// past the hold 1 - 1/e of the reduction is gone.
    #[test]
    fn the_gain_recovers_with_the_release() {
        let len = 100_000;
        let mut x = vec![0.4f32; len];
        let peak = 1000;
        x[peak] = 4.0;
        let mut l = Limiter::new(1, 0);
        assert!(l.set(on(-6.0, 100.0)));
        let y = run(&mut l, 0, &x, 64);
        let gain = |n: usize| y[n + LIMITER_DELAY] / x[n];
        let c = db_to_gain(-6.0);
        assert!(gain(peak) <= c / 4.0 * (1.0 + 1e-6));
        // Held at least through the look-ahead after the peak.
        assert!(gain(peak + 1) < 0.2);
        let tau = 4800;
        let reduction = 1.0 - c / 4.0;
        let left = 1.0 - gain(peak + LOOKAHEAD + tau);
        // The average lags the release by up to the look-ahead.
        assert!(
            (left / reduction - (-1.0f32).exp()).abs() < 0.02,
            "{left} of {reduction}"
        );
        // Recovered and snapped onto unity.
        assert_eq!(gain(len - LIMITER_DELAY - 1), 1.0);
    }

    /// Switching on ramps the effect in over the ramp length, and off
    /// back out; off at rest the input passes unchanged and late.
    #[test]
    fn switching_ramps() {
        let len = 4000;
        let x = vec![2.0f32; len];
        let mut l = Limiter::new(1, RAMP);
        let y = run(&mut l, 0, &x[..1000], 48);
        assert!(y[LIMITER_DELAY..].iter().all(|&v| v == 2.0));
        assert!(l.set(on(0.0, 50.0)));
        let y = run(&mut l, 1000, &x[1000..2000], 48);
        // The effect grows over the ramp, then holds at the ceiling, less
        // the 0.016 dB by which phase 0's DC gain lifts the sidechain.
        assert!(y[RAMP as usize / 2] < 2.0 && y[RAMP as usize / 2] > 1.0);
        assert!(y[RAMP as usize..].iter().all(|&v| v <= 1.0 && v > 0.998));
        assert!(l.set(LimiterParams {
            enabled: false,
            ..on(0.0, 50.0)
        }));
        let y = run(&mut l, 2000, &x[2000..], 48);
        assert!(y[RAMP as usize..].iter().all(|&v| v == 2.0));
        assert!(l.rest);
    }

    /// A jump of the index restarts the limiter from silence: the
    /// samples before it are not played late.
    #[test]
    fn a_jump_restarts_from_silence() {
        let mut l = Limiter::new(1, 0);
        let mut a = vec![0.5f32; 480];
        l.process(0, &mut a, 480);
        let mut b = vec![0.25f32; 480];
        l.process(4800, &mut b, 480);
        assert!(b[..LIMITER_DELAY].iter().all(|&v| v == 0.0));
        assert!(b[LIMITER_DELAY..].iter().all(|&v| v == 0.25));
    }

    /// A sample that is not finite is taken as silence, so the ceiling
    /// holds and the state stays valid.
    #[test]
    fn a_non_finite_sample_is_silence() {
        let mut l = Limiter::new(1, 0);
        assert!(l.set(on(-1.0, 50.0)));
        let mut x = vec![0.5f32; 200];
        x[10] = f32::NAN;
        x[11] = f32::INFINITY;
        l.process(0, &mut x, 200);
        assert_eq!(x[10 + LIMITER_DELAY], 0.0);
        assert_eq!(x[11 + LIMITER_DELAY], 0.0);
        assert!(l.is_valid());
    }

    /// Set at an index past the samples processed, the switch ramps from
    /// that index, as on a limiter that processed up to it.
    #[test]
    fn set_at_an_index_ahead_ramps_from_it() {
        // Silence up to the index: what the restart leaves.
        let x: Vec<f32> = (0..2000)
            .map(|n| if n < 1000 { 0.0 } else { 2.0 })
            .collect();
        let mut a = Limiter::new(1, RAMP);
        let _ = run(&mut a, 0, &x[..500], 50);
        assert!(a.set_at(1000, on(-6.0, 50.0)));
        let ya = run(&mut a, 1000, &x[1000..], 50);
        let mut b = Limiter::new(1, RAMP);
        let _ = run(&mut b, 0, &x[..1000], 50);
        assert!(b.set_at(1000, on(-6.0, 50.0)));
        let yb = run(&mut b, 1000, &x[1000..], 50);
        assert_eq!(ya, yb);
        assert!(ya[LIMITER_DELAY + 1] > 1.5 && ya[RAMP as usize + LIMITER_DELAY] < 0.51);
    }

    /// Parameters out of range, or NaN, are refused and change nothing.
    #[test]
    fn parameters_out_of_range_are_refused() {
        let mut l = Limiter::new(2, RAMP);
        for p in [
            on(0.5, 50.0),
            on(-21.0, 50.0),
            on(-1.0, 0.5),
            on(-1.0, 1001.0),
            on(f32::NAN, 50.0),
        ] {
            assert!(p.check().is_err(), "{p:?}");
            assert!(!l.set(p), "{p:?}");
        }
        assert_eq!(l.params(), LimiterParams::OFF);
        assert!(on(-20.0, 1.0).check().is_ok() && on(0.0, 1000.0).check().is_ok());
    }

    /// A running limiter round-trips through serde and decodes only as a
    /// valid one: a parameter, gain, sample or switch no limiter holds is
    /// refused.
    #[test]
    fn a_limiter_decodes_only_valid() {
        let mut l = Limiter::new(2, RAMP);
        assert!(l.set(on(-2.0, 70.0)));
        let x = programme(4800, 1);
        let _ = run(&mut l, 0, &x, 48);
        let json = serde_json::to_value(&l).unwrap();
        let back: Limiter = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back, l);
        let broken = |edit: &dyn Fn(&mut serde_json::Value)| {
            let mut j = json.clone();
            edit(&mut j);
            serde_json::from_value::<Limiter>(j).is_err()
        };
        assert!(broken(&|j| j["legs"] = 9.into()));
        assert!(broken(&|j| j["params"]["ceiling_db"] = 1.0.into()));
        assert!(broken(&|j| j["ceiling"] = 0.5.into()));
        assert!(broken(&|j| j["release"] = 0.5.into()));
        assert!(broken(&|j| j["q"][3] = 1.5.into()));
        assert!(broken(&|j| j["r"][3] = (-0.1).into()));
        assert!(broken(&|j| j["r"][3] = (ONE + 1).into()));
        assert!(broken(&|j| j["sum"] = (FULL - 1).into()));
        assert!(broken(&|j| j["released"] = 1.5.into()));
        assert!(broken(&|j| j["x"][7] = 1.0.into()));
        assert!(broken(&|j| j["wet"]["target"] = 0.0.into()));
        assert!(broken(&|j| j["q"] = serde_json::json!([1.0])));
        assert!(broken(&|j| {
            j["rest"] = true.into();
            j["q"][3] = 0.5.into();
        }));
    }
}
