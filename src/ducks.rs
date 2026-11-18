//! Duck groups: up to [`MAX_DUCKS`] detectors keyed by input slots after
//! their strips, whose envelopes duck the matrix coefficients whose masks
//! name them by each input row's depth ([`samplez_dsp::Matrix`]).
//!
//! The detectors are core state: the RT thread runs them on every block
//! it computes, on the grid of the sample index, and a core copy carries
//! them with their filter, hold and envelope state. A detector with no
//! key, not triggered by its manual controls and at rest is advanced
//! rather than run, which computes the same envelope, 0.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use samplez_dsp::duck::{DEPTH_DB, KEY_GAIN, MAX_KEYS};
use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{Detector, DetectorParams, DuckMode, DuckParam, MAX_DUCKS, Matrix, PlanarBuffer};
use serde::{Deserialize, Serialize};

use crate::rt_loop::SLICE;
use crate::transfer::INDEX_MAX;

/// One setting of a duck group, to an absolute value.
#[derive(Clone, Debug, PartialEq)]
pub enum DuckSetting {
    /// The detector's parameters. Boxed, so a duck entry is no larger
    /// than a strip entry and lists of entries do not grow with it.
    Detector(Box<DetectorParams>),
    /// The group's keys, in key order: per key the input slot it keys
    /// from, after the slot's strip, and a linear gain within
    /// [`KEY_GAIN`]. At most [`MAX_KEYS`], each slot once, each slot of at
    /// most [`MAX_LEGS`] channels. A key whose slot changes, or that is
    /// newly keyed, starts with its filters at rest; a key left out is
    /// unkeyed.
    Keys(Vec<(usize, f32)>),
    /// What decides the detector when no momentary hold is held.
    Mode(DuckMode),
    /// The momentary hold: while held, the group ducks whatever the mode
    /// and the keys.
    Hold { held: bool },
    /// The depth of every row of input slot `slot` under the group, dB
    /// within [`DEPTH_DB`], ramping over the ramp length.
    Depth { slot: usize, db: f32 },
}

/// A duck group's targets as values.
#[derive(Clone, Debug, PartialEq)]
pub struct DuckTargets {
    pub params: DetectorParams,
    /// As [`DuckSetting::Keys`] sets them.
    pub keys: Vec<(usize, f32)>,
    pub mode: DuckMode,
    pub held: bool,
    /// Per input slot its depth in dB; a slot past the list at 0 dB.
    pub depths: Vec<f32>,
}

impl Default for DuckTargets {
    /// What an engine holds for a group no entry set: the default
    /// parameters, mode Auto, no hold, no key and every depth 0 dB.
    fn default() -> Self {
        DuckTargets {
            params: DetectorParams::DEFAULT,
            keys: Vec::new(),
            mode: DuckMode::Auto,
            held: false,
            depths: Vec::new(),
        }
    }
}

/// A duck group's readings over the last meter slice of 10 ms the RT
/// thread computed it in. A group that is not in use reports nothing
/// after the slice it went out of use in.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DuckMeter {
    /// The slice, the samples `[slice * 480, (slice + 1) * 480)`.
    pub slice: Option<u64>,
    /// The largest key, linear: the largest leg magnitude of any key
    /// after its filters, times the key's gain.
    pub key: f32,
    /// The largest envelope, 0 to 1: a target ducks by its depth times
    /// the envelope.
    pub envelope: f32,
    /// The detector was engaged, triggered or holding, at the end of a
    /// block computed in the slice.
    pub engaged: bool,
}

/// A group's keys as a log entry's batch and the RT thread hold them:
/// the first `len` of `slots` and `gains`, the others 0.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct KeySet {
    pub len: u8,
    pub slots: [u8; MAX_KEYS],
    pub gains: [f32; MAX_KEYS],
}

impl KeySet {
    /// `keys` as a set; none for more than [`MAX_KEYS`] or a slot beyond
    /// a slot mask's.
    pub fn of(keys: &[(usize, f32)]) -> Option<KeySet> {
        if keys.len() > MAX_KEYS || keys.iter().any(|&(s, _)| s >= 64) {
            return None;
        }
        let mut set = KeySet {
            len: keys.len() as u8,
            slots: [0; MAX_KEYS],
            gains: [0.0; MAX_KEYS],
        };
        for (k, &(slot, gain)) in keys.iter().enumerate() {
            set.slots[k] = slot as u8;
            set.gains[k] = gain;
        }
        Some(set)
    }

    /// The keys, in key order.
    pub fn keys(&self) -> impl Iterator<Item = (usize, f32)> + '_ {
        let n = usize::from(self.len).min(MAX_KEYS);
        (0..n).map(|k| (usize::from(self.slots[k]), self.gains[k]))
    }

    /// Whether a set holds it: at most [`MAX_KEYS`] keys, each slot once,
    /// gains within [`KEY_GAIN`], the places past them 0.
    pub fn sane(&self) -> bool {
        let n = usize::from(self.len);
        n <= MAX_KEYS
            && (0..n).all(|k| !self.slots[..k].contains(&self.slots[k]))
            && self.gains[..n].iter().all(|g| KEY_GAIN.contains(g))
            && self.slots[n..].iter().all(|&s| s == 0)
            && self.gains[n..].iter().all(|g| g.to_bits() == 0)
    }
}

/// A setting of a detector as the RT thread applies it.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum DuckChange {
    Detector(DetectorParams),
    Keys(KeySet),
    Mode(DuckMode),
    Hold(bool),
}

impl DuckChange {
    /// Distinguishes the settings a log entry folds: one value per kind
    /// and group, the last.
    pub fn kind(&self) -> u8 {
        match self {
            DuckChange::Detector(_) => 0,
            DuckChange::Keys(_) => 1,
            DuckChange::Mode(_) => 2,
            DuckChange::Hold(_) => 3,
        }
    }

    /// Whether a detector takes it, the widths of the slots keyed aside.
    pub fn sane(&self) -> bool {
        match *self {
            DuckChange::Detector(p) => DuckParam::Detector(p).check().is_ok(),
            DuckChange::Keys(k) => k.sane(),
            DuckChange::Mode(_) | DuckChange::Hold(_) => true,
        }
    }

    /// Whether it fits input slots of `legs` legs: each slot keyed exists
    /// and is at most [`MAX_LEGS`] wide.
    pub fn fits(&self, legs: &[usize]) -> bool {
        match self {
            DuckChange::Keys(k) => k
                .keys()
                .all(|(s, _)| legs.get(s).is_some_and(|&l| (1..=MAX_LEGS).contains(&l))),
            _ => true,
        }
    }
}

/// The settings of a group's targets other than its depths, in the
/// order an engine applies them: parameters, keys, mode, hold.
pub(crate) fn changes_of(d: &DuckTargets) -> impl Iterator<Item = DuckChange> {
    let keys = KeySet::of(&d.keys).map(DuckChange::Keys);
    [Some(DuckChange::Detector(d.params)), keys]
        .into_iter()
        .flatten()
        .chain([DuckChange::Mode(d.mode), DuckChange::Hold(d.held)])
}

/// The depth `db` is one a matrix takes.
pub(crate) fn valid_depth(db: f32) -> bool {
    DEPTH_DB.contains(&db)
}

/// The detectors as a core holds them: each with the input slot of each
/// key, and the index of the next sample they process.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct DuckCore {
    /// [`MAX_DUCKS`] of them.
    pub detectors: Vec<Detector>,
    /// Per detector and key, the input slot keyed; none where the key is
    /// not.
    pub keys: [[Option<u8>; MAX_KEYS]; MAX_DUCKS],
    pub pos: Option<u64>,
}

impl DuckCore {
    /// New detectors whose key filter changes ramp over `ramp` samples.
    pub fn new(ramp: u32) -> DuckCore {
        DuckCore {
            detectors: vec![Detector::new(ramp); MAX_DUCKS],
            keys: [[None; MAX_KEYS]; MAX_DUCKS],
            pos: None,
        }
    }

    /// Makes this a copy of `src` without allocating: both hold
    /// [`MAX_DUCKS`] detectors, which hold no heap memory.
    pub fn copy_from(&mut self, src: &DuckCore) {
        self.detectors.copy_from_slice(&src.detectors);
        self.keys = src.keys;
        self.pos = src.pos;
    }

    /// Every detector new, of key filter ramp `ramp`, without allocating.
    pub fn reset(&mut self, ramp: u32) {
        self.detectors.fill(Detector::new(ramp));
        self.keys = [[None; MAX_KEYS]; MAX_DUCKS];
        self.pos = None;
    }

    /// Whether every value is one an engine holds: [`MAX_DUCKS`]
    /// detectors, each valid as it deserializes, a slot named exactly for
    /// each key keyed, the position at most [`INDEX_MAX`].
    pub fn sane(&self) -> bool {
        self.detectors.len() == MAX_DUCKS
            && self.pos.is_none_or(|p| p <= INDEX_MAX)
            && (self.detectors.iter().zip(&self.keys))
                .all(|(d, keys)| (0..MAX_KEYS).all(|k| d.key(k).is_some() == keys[k].is_some()))
    }

    /// Whether this sane core fits input slots of `legs` legs and key
    /// filter ramps of `ramp` samples: each key's slot exists and has
    /// the key's width.
    pub fn fits(&self, legs: &[usize], ramp: u32) -> bool {
        let ramp = Detector::new(ramp).ramp();
        (self.detectors.iter().zip(&self.keys)).all(|(d, keys)| {
            d.ramp() == ramp
                && (0..MAX_KEYS).all(|k| match (d.key(k), keys[k]) {
                    (Some((width, _)), Some(s)) => legs.get(usize::from(s)) == Some(&width),
                    _ => true,
                })
        })
    }

    /// Brings the detectors to sample index `index`: they move on over
    /// samples not processed, as [`Detector::advance`] does, and take
    /// `index` as the next sample. A lower index follows a backward clock
    /// step: the position re-anchors, nothing rewound.
    pub fn to(&mut self, index: u64) {
        if let Some(pos) = self.pos
            && index > pos
        {
            let frames = usize::try_from(index - pos).unwrap_or(usize::MAX);
            for d in &mut self.detectors {
                d.advance(frames);
            }
        }
        for d in &mut self.detectors {
            d.align(index);
        }
        self.pos = Some(index);
    }

    /// Applies `change` to detector `group` from sample index `at_index`,
    /// the keys of slots of `legs(slot)` legs. False, and nothing set,
    /// when the detector refuses it.
    #[must_use]
    pub fn set(
        &mut self,
        at_index: u64,
        group: usize,
        change: DuckChange,
        legs: impl Fn(usize) -> usize,
    ) -> bool {
        self.to(at_index);
        self.apply(group, change, legs)
    }

    /// Applies `change` to detector `group` where it stands.
    #[must_use]
    pub fn apply(
        &mut self,
        group: usize,
        change: DuckChange,
        legs: impl Fn(usize) -> usize,
    ) -> bool {
        let Some(d) = self.detectors.get_mut(group) else {
            return false;
        };
        let param = match change {
            DuckChange::Detector(p) => DuckParam::Detector(p),
            DuckChange::Mode(m) => DuckParam::Mode(m),
            DuckChange::Hold(held) => DuckParam::Hold { held },
            DuckChange::Keys(set) => {
                let key = |k: usize, (slot, gain): (usize, f32)| DuckParam::Key {
                    key: k,
                    legs: legs(slot),
                    gain,
                };
                if !set.sane()
                    || set
                        .keys()
                        .enumerate()
                        .any(|(k, s)| key(k, s).check().is_err())
                {
                    return false;
                }
                let keys = &mut self.keys[group];
                for (k, held) in keys.iter_mut().enumerate() {
                    let new = set.keys().nth(k);
                    if held.is_some() && *held != new.map(|(s, _)| s as u8) {
                        let _ = d.set(DuckParam::Unkey { key: k });
                    }
                    if let Some(s) = new {
                        // Checked above: taken.
                        let _ = d.set(key(k, s));
                    }
                    *held = new.map(|(s, _)| s as u8);
                }
                return true;
            }
        };
        d.set(param)
    }

    /// Every detector new and set to `targets`, a group past them at
    /// what no entry set, the keys of slots of `legs(slot)` legs: what a
    /// core built from product state starts from, whatever this one held.
    /// The settings a detector refuses, which a checked state holds none
    /// of, are left out.
    pub fn cold(&mut self, ramp: u32, targets: &[DuckTargets], legs: impl Fn(usize) -> usize) {
        self.reset(ramp);
        for (g, d) in targets.iter().enumerate() {
            for change in changes_of(d) {
                let _ = self.apply(g, change, &legs);
            }
        }
    }

    /// Settles detector state built from values at sample index `at`:
    /// key filter ramps ended, a detector its manual controls trigger at
    /// full depth, the others at rest.
    pub fn settle(&mut self, ramp: u32, at: u64) {
        // The longest attack and a ramp: every forced detector has run
        // its transit, every filter ramp ended.
        let longest = samplez_dsp::duck::ATTACK_MS.end() * samplez_mxl::SAMPLE_RATE as f32 / 1000.0;
        let frames =
            ramp as usize + longest as usize + 2 * samplez_dsp::strip::GAIN_INTERVAL as usize;
        for d in &mut self.detectors {
            d.advance(frames);
            d.align(at);
        }
        self.pos = Some(at);
    }
}

/// Sets `matrix` to a product state's duck masks, by coefficient, and
/// depths, by input channel and group, and fades its gains in from
/// silence over `ramp` samples as [`Matrix::fade_in`] does, whatever the
/// matrix held: the masks and depths at their targets, so a coefficient
/// with a mask fades in ducked, from the sample index `at` its position
/// in the interval follows. Whether any gain ramps.
pub(crate) fn fill_matrix(
    matrix: &mut Matrix,
    gains: &[f32],
    masks: &[u8],
    depths: &[f32],
    ramp: u32,
    at: u64,
) -> bool {
    let m = matrix.outputs();
    // No mask while the gains fade in: no term crossfades from no duck.
    for c in 0..masks.len() {
        let _ = matrix.set_mask(c / m, c % m, 0, 0);
    }
    for (c, &db) in depths.iter().enumerate() {
        let _ = matrix.set_depth(c / MAX_DUCKS, c % MAX_DUCKS, db, 0);
    }
    let ramping = matrix.fade_in(gains, ramp);
    for (c, &k) in masks.iter().enumerate().filter(|(_, k)| **k != 0) {
        let _ = matrix.set_mask(c / m, c % m, k, 0);
    }
    matrix.align(at);
    ramping
}

/// One duck group's readings over one slice, from the RT thread.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DuckRecord {
    pub group: u8,
    pub slice: u64,
    pub key: f32,
    pub envelope: f32,
    pub engaged: bool,
}

/// What a group's readings over the slice in progress hold so far.
#[derive(Clone, Copy, Debug, Default)]
struct Slice {
    slice: Option<u64>,
    key: f32,
    envelope: f32,
    engaged: bool,
    /// The group was run in the slice, not advanced.
    active: bool,
}

/// The detectors and their readings on the RT thread.
pub(crate) struct Ducks {
    pub core: DuckCore,
    slices: [Slice; MAX_DUCKS],
    /// By bit, the groups whose last record sent was of a slice they
    /// were run in: each sends one more once it is no longer run.
    reported: u8,
}

impl Ducks {
    pub fn new(ramp: u32) -> Ducks {
        Ducks {
            core: DuckCore::new(ramp),
            slices: [Slice::default(); MAX_DUCKS],
            reported: 0,
        }
    }

    /// Runs every detector over the `frames` samples from sample index
    /// `start`, keyed from `inbuf`, input slot `s`'s legs from its
    /// channel `first(s)` on, and writes each one's envelope of each
    /// sample to its channel of `envelopes`. An idle detector is advanced
    /// and its envelope written 0. Each group's readings of each slice
    /// end go to `sink`, as long as it returns true; none without one.
    pub fn process(
        &mut self,
        start: u64,
        frames: usize,
        first: impl Fn(usize) -> usize,
        inbuf: &PlanarBuffer,
        envelopes: &mut PlanarBuffer,
        mut sink: Option<&mut dyn FnMut(DuckRecord) -> bool>,
    ) {
        self.core.to(start);
        let mut done = 0;
        while done < frames {
            let index = start + done as u64;
            // Parts end at slice ends, where a slice's readings are sent.
            let n = (frames - done).min((SLICE - index % SLICE) as usize);
            let slice = index / SLICE;
            for g in 0..MAX_DUCKS {
                let s = &mut self.slices[g];
                if s.slice != Some(slice) {
                    if let Some(sink) = sink.as_mut() {
                        send(s, g, &mut self.reported, sink);
                    }
                    *s = Slice {
                        slice: Some(slice),
                        ..Slice::default()
                    };
                }
                let d = &mut self.core.detectors[g];
                let env = &mut envelopes.channel_mut(g)[done..done + n];
                if d.is_idle() {
                    d.advance(n);
                    env.fill(0.0);
                    continue;
                }
                let keys = &self.core.keys[g];
                let leg = |k: usize, l: usize| {
                    let slot = keys[k].map_or(0, usize::from);
                    &inbuf.channel(first(slot) + l)[done..done + n]
                };
                let key = d.process_metered(n, leg, env);
                s.active = true;
                s.key = s.key.max(key);
                s.envelope = env.iter().fold(s.envelope, |m, &e| m.max(e));
                s.engaged |= d.is_engaged();
            }
            done += n;
            if (start + done as u64).is_multiple_of(SLICE)
                && let Some(sink) = sink.as_mut()
            {
                for g in 0..MAX_DUCKS {
                    send(&mut self.slices[g], g, &mut self.reported, sink);
                }
            }
        }
        self.core.pos = Some(start + frames as u64);
    }
}

/// Sends group `g`'s readings of the slice `s` holds, if it was run in it
/// or its last record sent was of a slice it was run in, and clears them.
fn send(s: &mut Slice, g: usize, reported: &mut u8, sink: &mut dyn FnMut(DuckRecord) -> bool) {
    let bit = 1u8 << g;
    if let Some(slice) = s.slice
        && (s.active || *reported & bit != 0)
    {
        let sent = sink(DuckRecord {
            group: g as u8,
            slice,
            key: s.key,
            envelope: s.envelope,
            engaged: s.engaged,
        });
        if sent {
            *reported = if s.active {
                *reported | bit
            } else {
                *reported & !bit
            };
        }
    }
    s.slice = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A duck setting takes no more room than a strip parameter: every
    /// list of engine entries a product derives holds entries of the
    /// size it held before ducks.
    #[test]
    fn a_duck_setting_is_no_larger_than_a_strip_parameter() {
        use std::mem::size_of;
        assert!(size_of::<DuckSetting>() <= size_of::<samplez_dsp::StripParam>());
    }

    #[test]
    fn a_key_set_holds_at_most_max_keys_each_slot_once() {
        let keys: Vec<(usize, f32)> = (0..MAX_KEYS).map(|s| (s, 1.0)).collect();
        let set = KeySet::of(&keys).unwrap();
        assert!(set.sane());
        assert_eq!(set.keys().collect::<Vec<_>>(), keys);
        let mut over = keys.clone();
        over.push((MAX_KEYS, 1.0));
        assert_eq!(KeySet::of(&over), None);
        assert!(!KeySet::of(&[(1, 1.0), (1, 0.5)]).unwrap().sane());
        assert!(!KeySet::of(&[(1, f32::NAN)]).unwrap().sane());
        assert!(!KeySet::of(&[(1, -1.0)]).unwrap().sane());
        let mut stray = KeySet::of(&[(1, 1.0)]).unwrap();
        stray.gains[3] = 1.0;
        assert!(!stray.sane());
    }

    /// Keys follow the set: a key whose slot changes starts at rest even
    /// of the same width, one left out is unkeyed, one kept keeps its
    /// filters; each key's slot is held beside it.
    #[test]
    fn keys_follow_their_slots() {
        let mut c = DuckCore::new(0);
        let legs = |s: usize| [1, 1, 2][s];
        let set = |k: &[(usize, f32)]| DuckChange::Keys(KeySet::of(k).unwrap());
        assert!(c.apply(2, set(&[(0, 1.0), (2, 0.5)]), legs));
        assert_eq!(c.keys[2][..3], [Some(0), Some(2), None]);
        assert_eq!(c.detectors[2].key(1), Some((2, 0.5)));
        assert!(c.sane() && c.fits(&[1, 1, 2], 0));
        assert!(!c.fits(&[1, 1, 1], 0), "a key of another width");
        assert!(!c.fits(&[1, 1], 0), "a key of a slot beyond the shape");
        assert!(c.apply(2, set(&[(1, 1.0)]), legs));
        assert_eq!(c.keys[2][..3], [Some(1), None, None]);
        assert_eq!(c.detectors[2].key(1), None);
        assert!(
            !c.apply(2, set(&[(1, 1.0), (1, 1.0)]), legs),
            "a slot twice"
        );
        assert!(!c.apply(MAX_DUCKS, set(&[]), legs));
        assert_eq!(c.keys[2][..3], [Some(1), None, None]);
    }
}
