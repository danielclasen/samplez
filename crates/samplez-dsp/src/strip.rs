//! The input channel strip: trim, per-leg trim and polarity, high-pass
//! filter, a four-band equaliser, gate and compressor, in that order or
//! with the dynamics before the equaliser, over the legs of one input of
//! width 1 to [`MAX_LEGS`].
//!
//! Every parameter is set by a [`StripParam`], an absolute value, so one
//! applied again changes nothing. A change takes effect at the next
//! sample processed and ramps over the strip's ramp length: gains
//! linearly in the gain domain ([`crate::Ramp`]), a polarity inversion
//! through zero, filters by their coefficients. Gate and compressor take
//! a new curve or time constant at their next gain computation, at most
//! [`GAIN_INTERVAL`] samples and one on, and ramp only their switch and
//! makeup.
//! The ranges are those the faderz model gives its strip controls.
//!
//! The strip holds no heap memory, so a clone allocates nothing. It runs
//! one leg after the other with scalar code, the same operations in the
//! same order on every host.

use core::ops::RangeInclusive;

use crate::Ramp;
use crate::dynamics::{Comp, Gate};
pub use crate::dynamics::{CompParams, GAIN_INTERVAL, GATE_HOLD, GateParams};
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
/// Gate threshold, dBFS.
pub const GATE_THRESHOLD_DB: RangeInclusive<f32> = -80.0..=0.0;
/// Gate range, dB.
pub const GATE_RANGE_DB: RangeInclusive<f32> = 0.0..=80.0;
/// Compressor threshold, dBFS.
pub const COMP_THRESHOLD_DB: RangeInclusive<f32> = -60.0..=0.0;
/// Compressor ratio.
pub const COMP_RATIO: RangeInclusive<f32> = 1.0..=20.0;
/// Compressor knee width, dB. faderz has no knee control.
pub const COMP_KNEE_DB: RangeInclusive<f32> = 0.0..=24.0;
/// Compressor makeup gain, dB.
pub const MAKEUP_DB: RangeInclusive<f32> = 0.0..=24.0;
/// Attack time constant of the gate and the compressor, ms.
pub const ATTACK_MS: RangeInclusive<f32> = 0.1..=200.0;
/// Release time constant of the gate and the compressor, ms.
pub const RELEASE_MS: RangeInclusive<f32> = 5.0..=5000.0;

/// The response of an equaliser band.
///
/// A bell takes the band's Q as it is. A shelf takes it as the RBJ shelf
/// slope `S = 2 Q^2`, at most 1: the slope whose Q at 0 dB is the band's,
/// and from Q 0.707 up the steepest shelf whose response stays between 0
/// dB and its gain. A higher Q would add a resonant peak and dip beside
/// the shelf, +23 dB at Q 2 on an 18 dB shelf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BandType {
    /// Shelf below `freq`, half its gain at `freq` (RBJ lowShelf).
    LowShelf,
    /// Bell around `freq` (RBJ peakingEQ).
    Peak,
    /// Shelf above `freq`, half its gain at `freq` (RBJ highShelf).
    HighShelf,
}

/// One equaliser band. A band at 0 dB passes its input unchanged.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
    fn refusal(&self) -> Result<(), Refusal> {
        within("eq band freq", self.freq, &EQ_FREQ)?;
        within("eq band gain", self.gain_db, &EQ_GAIN_DB)?;
        within("eq band q", self.q, &EQ_Q)
    }

    fn shape(&self) -> Shape {
        let (freq, gain_db, q) = (self.freq, self.gain_db, self.q);
        match self.kind {
            BandType::LowShelf => Shape::LowShelf {
                freq,
                gain_db,
                q: shelf_q(gain_db, q),
            },
            BandType::Peak => Shape::Bell { freq, gain_db, q },
            BandType::HighShelf => Shape::HighShelf {
                freq,
                gain_db,
                q: shelf_q(gain_db, q),
            },
        }
    }
}

/// The RBJ Q of a shelf of `gain_db` whose slope is `S = 2 q^2`, at most
/// 1 (see [`BandType`]).
fn shelf_q(gain_db: f32, q: f32) -> f32 {
    let s = (2.0 * f64::from(q) * f64::from(q)).min(1.0);
    let a = libm::pow(10.0, f64::from(gain_db) / 40.0);
    (1.0 / libm::sqrt((a + 1.0 / a) * (1.0 / s - 1.0) + 2.0)) as f32
}

/// One parameter of a strip, set to an absolute value.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// The gate, linked over the legs.
    Gate(GateParams),
    /// The compressor, linked over the legs.
    Comp(CompParams),
    /// Runs gate and compressor before the equaliser, or after it.
    DynamicsBeforeEq { enabled: bool },
}

/// Why a strip refuses a parameter. Building one does not allocate, so
/// [`Strip::set`] can refuse on the real-time thread.
enum Refusal {
    Outside(&'static str, f32, f32, f32),
    NoLeg(usize, usize),
    NoBand(usize),
}

impl Refusal {
    fn message(&self) -> String {
        match *self {
            Refusal::Outside(what, v, lo, hi) => format!("{what} {v} is outside {lo}..={hi}"),
            Refusal::NoLeg(leg, legs) => format!("no leg {leg} on a strip of {legs}"),
            Refusal::NoBand(band) => format!("no eq band {band}"),
        }
    }
}

fn within(what: &'static str, v: f32, range: &RangeInclusive<f32>) -> Result<(), Refusal> {
    if range.contains(&v) {
        Ok(())
    } else {
        Err(Refusal::Outside(what, v, *range.start(), *range.end()))
    }
}

impl StripParam {
    /// Whether a strip of `legs` legs takes the parameter: legs and bands
    /// that exist, values within their ranges, no NaN.
    pub fn check(&self, legs: usize) -> Result<(), String> {
        self.refusal(legs).map_err(|r| r.message())
    }

    fn refusal(&self, legs: usize) -> Result<(), Refusal> {
        let leg_exists = |leg: usize| {
            if leg < legs {
                Ok(())
            } else {
                Err(Refusal::NoLeg(leg, legs))
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
                    return Err(Refusal::NoBand(band));
                }
                value.refusal()
            }
            StripParam::Gate(g) => {
                within("gate threshold", g.threshold_db, &GATE_THRESHOLD_DB)?;
                within("gate range", g.range_db, &GATE_RANGE_DB)?;
                within("gate attack", g.attack_ms, &ATTACK_MS)?;
                within("gate release", g.release_ms, &RELEASE_MS)
            }
            StripParam::Comp(c) => {
                within("comp threshold", c.threshold_db, &COMP_THRESHOLD_DB)?;
                within("comp ratio", c.ratio, &COMP_RATIO)?;
                within("comp attack", c.attack_ms, &ATTACK_MS)?;
                within("comp release", c.release_ms, &RELEASE_MS)?;
                within("comp knee", c.knee_db, &COMP_KNEE_DB)?;
                within("comp makeup", c.makeup_db, &MAKEUP_DB)
            }
            StripParam::DynamicsBeforeEq { .. } => Ok(()),
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
/// unchanged: trims at 0 dB, no leg inverted, high-pass, equaliser, gate
/// and compressor off, the bands at [`DEFAULT_BANDS`], the high-pass at
/// 80 Hz, the dynamics after the equaliser.
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
    gate: Gate,
    comp: Comp,
    /// The order of the chain running: `eq` states, `gate` and `comp`.
    dynamics_before_eq: bool,
    /// While the order flips, the weight of the other order's chain:
    /// `alt_eq` states, `alt_gate` and `alt_comp`.
    fade: Ramp,
    alt_eq: [[[f32; 2]; MAX_LEGS]; EQ_BANDS],
    alt_gate: Gate,
    alt_comp: Comp,
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
        let mut s = Strip {
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
            gate: Gate::new(),
            comp: Comp::new(),
            dynamics_before_eq: false,
            fade: Ramp::new(0.0),
            alt_eq: [[[0.0; 2]; MAX_LEGS]; EQ_BANDS],
            alt_gate: Gate::new(),
            alt_comp: Comp::new(),
        };
        // The sections off, holding the g and k of their shapes.
        s.hpf.set(Shape::HighPass { freq: 80.0 }, false, 0);
        for band in 0..EQ_BANDS {
            s.retarget_band(band);
        }
        s
    }

    pub fn legs(&self) -> usize {
        self.legs
    }

    /// Sets `param` from the next sample processed on, ramping over the
    /// strip's ramp length. Returns false and changes nothing when
    /// [`StripParam::check`] refuses it; a refusal does not allocate.
    #[must_use]
    pub fn set(&mut self, param: StripParam) -> bool {
        if param.refusal(self.legs).is_err() {
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
                self.hpf.set(Shape::HighPass { freq }, enabled, self.ramp);
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
            StripParam::Gate(g) => {
                self.gate.set(&g, self.ramp);
                self.alt_gate.set(&g, self.ramp);
            }
            StripParam::Comp(c) => {
                self.comp.set(&c, self.ramp);
                self.alt_comp.set(&c, self.ramp);
            }
            StripParam::DynamicsBeforeEq { enabled } => self.set_order(enabled),
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
        let shape = self.bands[band].shape();
        self.eq[band].set(shape, self.eq_enabled, self.ramp);
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
        if self.is_fading() {
            self.process_fading(buf, frames);
        } else {
            self.process_dynamics_and_eq(buf, frames);
        }
    }

    fn process_dynamics_and_eq(&mut self, buf: &mut [f32], frames: usize) {
        if self.dynamics_before_eq {
            self.gate.process(buf, self.legs, frames);
            self.comp.process(buf, self.legs, frames);
        }
        for eq in &mut self.eq {
            eq.process(buf, self.legs, frames);
        }
        if !self.dynamics_before_eq {
            self.gate.process(buf, self.legs, frames);
            self.comp.process(buf, self.legs, frames);
        }
    }

    /// Runs the dynamics before the equaliser or after it. A flip runs
    /// both orders over the ramp length and crossfades from the one to
    /// the other: switching outright would play the equaliser state built
    /// on the other order's signal, a full-scale burst behind a closed
    /// gate or a compressor deep in reduction. The new order starts with
    /// the equaliser state at zero and the dynamics as they are. Flipping
    /// back during the crossfade fades back. With gate and compressor
    /// passing their input unchanged both orders feed the equaliser the
    /// same signal, so the order flips at once and the equaliser keeps
    /// its state.
    fn set_order(&mut self, before: bool) {
        let other = before != self.dynamics_before_eq;
        let unity = self.gate.is_identity() && self.comp.is_identity();
        if self.ramp == 0 || (unity && !self.is_fading()) {
            self.dynamics_before_eq = before;
            self.fade = Ramp::new(0.0);
            return;
        }
        if other && !self.is_fading() {
            self.alt_eq = [[[0.0; 2]; MAX_LEGS]; EQ_BANDS];
            self.alt_gate = self.gate;
            self.alt_comp = self.comp;
        }
        let _ = self
            .fade
            .set_target(if other { 1.0 } else { 0.0 }, self.ramp);
    }

    fn is_fading(&self) -> bool {
        self.fade.is_active() || self.fade.target() != 0.0
    }

    /// Once a crossfade is over, the order faded to becomes the one
    /// running.
    fn end_fade(&mut self) {
        if self.fade.is_active() {
            return;
        }
        if self.fade.target() == 1.0 {
            for (eq, state) in self.eq.iter_mut().zip(&self.alt_eq) {
                eq.state = *state;
            }
            self.gate = self.alt_gate;
            self.comp = self.alt_comp;
            self.dynamics_before_eq = !self.dynamics_before_eq;
        }
        self.fade = Ramp::new(0.0);
    }

    /// The dynamics and the equaliser in both orders, mixed by the fade,
    /// in chunks through scratch on the stack. A chunk ends where the
    /// crossfade does, so the samples after it run one order alone
    /// whatever the block size.
    fn process_fading(&mut self, buf: &mut [f32], frames: usize) {
        const CHUNK: usize = 64;
        let legs = self.legs;
        let mut scratch_a = [0.0f32; MAX_LEGS * CHUNK];
        let mut scratch_b = [0.0f32; MAX_LEGS * CHUNK];
        let mut at = 0;
        while at < frames {
            let fading = self.is_fading();
            let mut n = (frames - at).min(CHUNK);
            if self.fade.is_active() {
                n = n.min((self.fade.len - self.fade.pos) as usize);
            }
            let a = &mut scratch_a[..legs * n];
            for (leg, x) in a.chunks_exact_mut(n).enumerate() {
                x.copy_from_slice(&buf[leg * frames + at..][..n]);
            }
            if fading {
                let b = &mut scratch_b[..legs * n];
                b.copy_from_slice(a);
                let mut states = core::array::from_fn(|i| self.eq[i].state);
                let order = self.dynamics_before_eq;
                let units = (&mut self.gate, &mut self.comp);
                run_chain(&self.eq, &mut states, units, order, a, legs, n);
                let units = (&mut self.alt_gate, &mut self.alt_comp);
                run_chain(&self.eq, &mut self.alt_eq, units, !order, b, legs, n);
                for (eq, state) in self.eq.iter_mut().zip(states) {
                    eq.state = state;
                    eq.advance(n);
                }
                for (i, (x, y)) in a.iter_mut().zip(b.iter()).enumerate() {
                    let w = self.fade.value_at(self.fade.pos + (i % n) as u32);
                    *x += w * (*y - *x);
                }
                self.fade.advance(n);
                self.end_fade();
            } else {
                self.process_dynamics_and_eq(a, n);
            }
            for (leg, x) in a.chunks_exact(n).enumerate() {
                buf[leg * frames + at..][..n].copy_from_slice(x);
            }
            at += n;
        }
    }

    /// Moves every ramp `frames` samples on without processing, for
    /// samples the engine does not compute: a parameter's value depends
    /// on the sample index and the changes alone. What follows the signal
    /// stays as it is: filter states, the smoothed gains of gate and
    /// compressor and the gate's hold. A crossfade between the dynamics
    /// orders that ends within `frames` ends here as it would processing.
    pub fn advance(&mut self, frames: usize) {
        for g in &mut self.gain[..self.legs] {
            g.advance(frames);
        }
        self.hpf.advance(frames);
        for eq in &mut self.eq {
            eq.advance(frames);
        }
        self.gate.advance(frames);
        self.comp.advance(frames);
        if self.is_fading() {
            self.alt_gate.advance(frames);
            self.alt_comp.advance(frames);
            self.fade.advance(frames);
            self.end_fade();
        }
    }

    /// Takes the next sample processed to be sample index `index`: gate
    /// and compressor compute their gains at the multiples of
    /// [`GAIN_INTERVAL`] samples of the index. A strip starts at index 0
    /// and counts the samples it processes and advances over.
    pub fn align(&mut self, index: u64) {
        self.gate.align(index);
        self.comp.align(index);
        self.alt_gate.align(index);
        self.alt_comp.align(index);
    }
}

/// Gate and compressor, then the equaliser on `states`, or the other way
/// round; the equaliser's ramps do not move.
fn run_chain(
    eq: &[Svf<MAX_LEGS>; EQ_BANDS],
    states: &mut [[[f32; 2]; MAX_LEGS]; EQ_BANDS],
    (gate, comp): (&mut Gate, &mut Comp),
    dynamics_first: bool,
    buf: &mut [f32],
    legs: usize,
    frames: usize,
) {
    if dynamics_first {
        gate.process(buf, legs, frames);
        comp.process(buf, legs, frames);
    }
    for (f, state) in eq.iter().zip(states) {
        f.filter(state, buf, legs, frames);
    }
    if !dynamics_first {
        gate.process(buf, legs, frames);
        comp.process(buf, legs, frames);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svf::tests::{measure, reference};
    use crate::svf::{Coefficients, coefficients};

    const RAMP: u32 = 480;
    const GATE: GateParams = GateParams {
        enabled: true,
        threshold_db: -12.0,
        range_db: 20.0,
        attack_ms: 0.5,
        release_ms: 30.0,
    };
    const COMP: CompParams = CompParams {
        enabled: true,
        threshold_db: -18.0,
        ratio: 4.0,
        attack_ms: 2.0,
        release_ms: 80.0,
        knee_db: 6.0,
        makeup_db: 4.0,
    };

    /// The peak of `frames` samples of a 100 Hz sine at `level` from
    /// sample `from` on, through `s`.
    fn sine_peak(s: &mut Strip, level: f32, from: usize, frames: usize) -> f32 {
        let w = 2.0 * core::f64::consts::PI * 100.0 / 48_000.0;
        let mut peak = 0.0f32;
        for start in (from..from + frames).step_by(48) {
            let mut b: Vec<f32> = (start..start + 48)
                .map(|n| level * (w * n as f64).sin() as f32)
                .collect();
            s.process(&mut b, 48);
            peak = b.iter().fold(peak, |p, v| p.max(v.abs()));
        }
        peak
    }

    /// Flipping the order crossfades: through the flip the output peaks
    /// within 0.5 dB of the louder of the two orders settled, with the
    /// compressor deep in reduction or the gate closed, where switching
    /// outright played the equaliser state built on the other order's
    /// signal, up to 39 dB louder.
    #[test]
    fn flipping_the_order_crossfades() {
        let bell = StripParam::EqBand {
            band: 1,
            value: band(BandType::Peak, 100.0, 12.0, 2.0),
        };
        let comp = StripParam::Comp(CompParams {
            enabled: true,
            threshold_db: -40.0,
            ratio: 20.0,
            attack_ms: 1.0,
            release_ms: 100.0,
            knee_db: 0.0,
            makeup_db: 0.0,
        });
        let gate = StripParam::Gate(GateParams {
            enabled: true,
            threshold_db: -6.0,
            range_db: 80.0,
            attack_ms: 1.0,
            release_ms: 50.0,
        });
        let second = 48_000;
        for (unit, level) in [(comp, 0.5), (gate, 0.1)] {
            for before in [false, true] {
                let order = |enabled| {
                    let mut s = strip_with(
                        1,
                        &[
                            StripParam::Eq { enabled: true },
                            bell,
                            unit,
                            StripParam::DynamicsBeforeEq { enabled },
                        ],
                    );
                    sine_peak(&mut s, level, 0, second);
                    s
                };
                let settled = |enabled| sine_peak(&mut order(enabled), level, second, second);
                let louder = settled(before).max(settled(!before));
                let mut s = order(before);
                assert!(s.set(StripParam::DynamicsBeforeEq { enabled: !before }));
                let peak = sine_peak(&mut s, level, second, second);
                let over = 20.0 * (peak / louder).log10();
                assert!(
                    over <= 0.5,
                    "{unit:?} before eq {before} -> {}: {peak} through the flip, {louder} settled ({over:+.1} dB)",
                    !before
                );
            }
        }
    }

    /// With gate and compressor off or at unity the order flips at once
    /// and the equaliser keeps its state: the output is the bits of the
    /// strip that did not flip, where a crossfade to the other order
    /// played its equaliser from a state of zero.
    #[test]
    fn a_flip_at_unity_dynamics_changes_no_bit() {
        let unity = [
            StripParam::Gate(GateParams {
                range_db: 0.0,
                ..GATE
            }),
            StripParam::Comp(CompParams {
                ratio: 1.0,
                makeup_db: 0.0,
                ..COMP
            }),
        ];
        let off = [
            StripParam::Gate(GateParams {
                enabled: false,
                ..GATE
            }),
            StripParam::Comp(CompParams {
                enabled: false,
                ..COMP
            }),
        ];
        for units in [unity, off] {
            for before in [false, true] {
                let mut kept = strip_with(
                    1,
                    &[
                        StripParam::Eq { enabled: true },
                        StripParam::EqBand {
                            band: 1,
                            value: band(BandType::Peak, 100.0, 12.0, 8.0),
                        },
                        units[0],
                        units[1],
                        StripParam::DynamicsBeforeEq { enabled: before },
                    ],
                );
                sine_peak(&mut kept, 0.5, 0, 4800);
                let mut flipped = kept.clone();
                assert!(flipped.set(StripParam::DynamicsBeforeEq { enabled: !before }));
                for start in (4800..4800 + 4 * RAMP as usize).step_by(48) {
                    let x: Vec<f32> = (start..start + 48)
                        .map(|n| 0.5 * (n as f32 * 0.013).sin())
                        .collect();
                    let (mut a, mut b) = (x.clone(), x);
                    kept.process(&mut a, 48);
                    flipped.process(&mut b, 48);
                    let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
                    assert_eq!(bits(&a), bits(&b), "{units:?} at {start}");
                }
            }
        }
    }

    /// A crossfade between the orders gives the same bits whatever the
    /// block sizes, also where a block spans its end.
    #[test]
    fn a_crossfade_ignores_block_sizes() {
        let frames = 1920;
        let input: Vec<f32> = (0..2 * frames)
            .map(|n| ((n * 7919) % 1013) as f32 / 1013.0 - 0.5)
            .collect();
        let run = |sizes: &[usize]| {
            let mut s = strip_with(
                2,
                &[
                    StripParam::Eq { enabled: true },
                    StripParam::EqBand {
                        band: 1,
                        value: band(BandType::Peak, 300.0, 12.0, 2.0),
                    },
                    StripParam::Comp(COMP),
                    StripParam::Gate(GATE),
                ],
            );
            assert!(s.set(StripParam::DynamicsBeforeEq { enabled: true }));
            let mut out = vec![0.0f32; 2 * frames];
            let mut at = 0;
            let mut sizes = sizes.iter().cycle();
            while at < frames {
                let b = (*sizes.next().unwrap()).min(frames - at);
                let mut buf = [&input[at..at + b], &input[frames + at..frames + at + b]].concat();
                s.process(&mut buf, b);
                out[at..at + b].copy_from_slice(&buf[..b]);
                out[frames + at..frames + at + b].copy_from_slice(&buf[b..]);
                at += b;
            }
            out
        };
        let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        let whole = bits(&run(&[48]));
        for sizes in [&[100usize][..], &[7, 41], &[1920]] {
            assert_eq!(bits(&run(sizes)), whole, "blocks of {sizes:?}");
        }
    }

    /// The dynamics run before or after the equaliser as switched: the
    /// strip's output is the bits of its units run in that order.
    #[test]
    fn dynamics_run_where_switched() {
        let bell = band(BandType::Peak, 1000.0, 18.0, 1.0);
        let input: Vec<f32> = (0..4800).map(|n| 0.4 * (n as f32 * 0.13).sin()).collect();
        for before in [false, true] {
            let mut s = strip_with(
                1,
                &[
                    StripParam::Eq { enabled: true },
                    StripParam::EqBand {
                        band: 1,
                        value: bell,
                    },
                    StripParam::Comp(COMP),
                    StripParam::Gate(GATE),
                    StripParam::DynamicsBeforeEq { enabled: before },
                ],
            );
            let mut eq = Svf::<1>::new();
            eq.set(bell.shape(), true, 0);
            let (mut gate, mut comp) = (Gate::new(), Comp::new());
            gate.set(&GATE, 0);
            comp.set(&COMP, 0);
            for block in input.chunks(48) {
                let mut a = block.to_vec();
                s.process(&mut a, 48);
                let mut b = block.to_vec();
                if !before {
                    eq.process(&mut b, 1, 48);
                }
                gate.process(&mut b, 1, 48);
                comp.process(&mut b, 1, 48);
                if before {
                    eq.process(&mut b, 1, 48);
                }
                assert_eq!(a, b, "dynamics before eq: {before}");
            }
        }
    }

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
        let refused = refused.into_iter().chain([
            StripParam::Gate(GateParams {
                threshold_db: -81.0,
                ..GATE
            }),
            StripParam::Gate(GateParams {
                range_db: 81.0,
                ..GATE
            }),
            StripParam::Gate(GateParams {
                attack_ms: 0.05,
                ..GATE
            }),
            StripParam::Gate(GateParams {
                release_ms: 5001.0,
                ..GATE
            }),
            StripParam::Comp(CompParams {
                threshold_db: -61.0,
                ..COMP
            }),
            StripParam::Comp(CompParams { ratio: 0.5, ..COMP }),
            StripParam::Comp(CompParams {
                ratio: f32::NAN,
                ..COMP
            }),
            StripParam::Comp(CompParams {
                attack_ms: 201.0,
                ..COMP
            }),
            StripParam::Comp(CompParams {
                release_ms: 4.0,
                ..COMP
            }),
            StripParam::Comp(CompParams {
                knee_db: 25.0,
                ..COMP
            }),
            StripParam::Comp(CompParams {
                makeup_db: -1.0,
                ..COMP
            }),
        ]);
        for p in [StripParam::Gate(GATE), StripParam::Comp(COMP)] {
            assert_eq!(p.check(1), Ok(()));
        }
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

    /// The largest difference over the first 8 samples after a change
    /// between the strip given it and the same strip without it, on a
    /// 110 Hz sine: a ramp starts from the old coefficients and stays
    /// close, a coefficient that jumps does not.
    fn departure(before: &[StripParam], after: StripParam, ramp: u32) -> f32 {
        let mut s = strip_with(1, before);
        s.ramp = ramp;
        let w = 2.0 * core::f64::consts::PI * 110.0 / 48_000.0;
        let sine = |n: usize| (0.5 * (w * n as f64).sin()) as f32;
        let warm = 9637;
        let mut x: Vec<f32> = (0..warm).map(sine).collect();
        s.process(&mut x, warm);
        let mut kept = s.clone();
        assert!(s.set(after));
        let mut a: Vec<f32> = (warm..warm + 8).map(sine).collect();
        let mut b = a.clone();
        s.process(&mut a, 8);
        kept.process(&mut b, 8);
        a.iter()
            .zip(&b)
            .map(|(x, y)| (x - y).abs())
            .fold(0.0f32, f32::max)
    }

    #[test]
    fn changes_ramp_without_a_click() {
        let eq_on = StripParam::Eq { enabled: true };
        let bell = |freq, gain_db| StripParam::EqBand {
            band: 0,
            value: band(BandType::Peak, freq, gain_db, 2.0),
        };
        let hpf = |freq| StripParam::Hpf {
            enabled: true,
            freq,
        };
        // (settings, change, whether the change sweeps over octaves)
        let cases = [
            (vec![eq_on, bell(100.0, -18.0)], bell(100.0, 18.0), false),
            (vec![hpf(20.0)], hpf(500.0), false),
            (
                vec![],
                StripParam::Polarity {
                    leg: 0,
                    inverted: true,
                },
                false,
            ),
            (vec![], StripParam::Trim { db: 20.0 }, false),
            (vec![eq_on, bell(100.0, 12.0)], bell(5000.0, 12.0), true),
        ];
        for (before, after, sweep) in cases {
            let (ramped, settled) = largest_step(&before, after, RAMP);
            let (jumped, _) = largest_step(&before, after, 0);
            if sweep {
                // g ramps linearly, so the bell leaves the sine's octave
                // in the first 2 ms of the ramp: a larger step than a
                // settled one, still a fraction of a jump's.
                assert!(
                    ramped * 4.0 < jumped,
                    "{after:?}: step {ramped} while ramping, {jumped} on a jump"
                );
            } else {
                assert!(
                    ramped <= settled * 1.5,
                    "{after:?}: step {ramped} while ramping, {settled} settled"
                );
                assert!(
                    jumped > settled * 2.0,
                    "{after:?}: a jump makes a step of only {jumped}, settled {settled}"
                );
            }
            let (ramped, jumped) = (
                departure(&before, after, RAMP),
                departure(&before, after, 0),
            );
            assert!(
                ramped * 10.0 < jumped,
                "{after:?}: departs by {ramped} when ramped, {jumped} when jumped"
            );
        }
    }

    /// A shelf at full boost or cut stays between 0 dB and its gain at
    /// every Q of the range, where the RBJ Q of 16 would peak at +41 dB.
    #[test]
    fn shelves_do_not_overshoot_at_any_q() {
        let qs = [0.1, 0.25, 0.5, 0.707, 1.0, 2.0, 4.0, 8.0, 16.0];
        for kind in [BandType::LowShelf, BandType::HighShelf] {
            for gain_db in [-18.0f32, 18.0] {
                let (lo, hi) = (f64::from(gain_db.min(0.0)), f64::from(gain_db.max(0.0)));
                for q in qs {
                    for freq in [20.0, 100.0, 1000.0, 8000.0, 20_000.0] {
                        let shape = band(kind, freq, gain_db, q).shape();
                        for i in 0..=600 {
                            let f = 10.0 * 2399.0f64.powf(f64::from(i) / 600.0);
                            let (db, _) = reference(shape, f);
                            assert!(
                                db <= hi + 0.1 && db >= lo - 0.1,
                                "{kind:?} {freq} Hz {gain_db} dB Q {q}: {db} dB at {f} Hz"
                            );
                        }
                    }
                }
            }
        }
        // The strip itself, where the RBJ Q 16 low shelf peaks.
        let mut s = strip_with(
            1,
            &[
                StripParam::Eq { enabled: true },
                StripParam::EqBand {
                    band: 0,
                    value: band(BandType::LowShelf, 100.0, 18.0, 16.0),
                },
            ],
        );
        for f in [40.0, 59.0, 80.0] {
            let (db, _) = mono_response(&mut s, f);
            assert!(db <= 18.1, "{f} Hz: {db} dB");
        }
    }

    /// A switched-off section holds the g and k of its shape, so switching
    /// it on ramps its mix alone, not from a frequency it was never set
    /// to; a change while off does not ramp.
    #[test]
    fn switched_off_sections_hold_their_shape() {
        let gk = |c: Coefficients| [c[0], c[1]];
        let mut s = Strip::new(1, RAMP);
        assert_eq!(
            gk(s.hpf.target()),
            gk(coefficients(Shape::HighPass { freq: 80.0 }))
        );
        for (b, value) in DEFAULT_BANDS.iter().enumerate() {
            assert_eq!(gk(s.eq[b].target()), gk(coefficients(value.shape())));
        }
        assert!(s.set(StripParam::Hpf {
            enabled: false,
            freq: 20.0
        }));
        assert!(s.hpf.is_bypassed());
        let off = s.hpf.target();
        assert_eq!(gk(off), gk(coefficients(Shape::HighPass { freq: 20.0 })));
        assert!(s.set(StripParam::Hpf {
            enabled: true,
            freq: 20.0
        }));
        assert_eq!(gk(s.hpf.target()), gk(off));
    }

    /// Two strips given the same parameters at the same samples compute
    /// the same bits, whatever the block sizes, and a clone taken while a
    /// ramp runs continues as the original does, under FTZ/DAZ as in
    /// production.
    #[test]
    fn strips_are_deterministic_and_clones_continue_alike() {
        let saved = crate::set_ftz_daz(true);
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
            (2400, StripParam::Gate(GATE)),
            (2880, StripParam::Comp(COMP)),
            (3360, StripParam::DynamicsBeforeEq { enabled: true }),
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
        crate::restore_mxcsr(saved);
    }

    /// Ramps advanced over samples not processed land where processing
    /// them would have, the crossfade between the dynamics orders and
    /// the units it runs included: silence leaves the filter states at
    /// zero and the compressor at 0 dB, so the two strips are equal.
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
            // Switched on, with a makeup ramp running.
            StripParam::Comp(CompParams {
                makeup_db: 12.0,
                ..COMP
            }),
            // Crossfading, the compressor being on.
            StripParam::DynamicsBeforeEq { enabled: true },
        ];
        let mut a = Strip::new(1, RAMP);
        let mut b = Strip::new(1, RAMP);
        for p in params {
            assert!(a.set(p) && b.set(p));
        }
        assert!(a.fade.is_active());
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
