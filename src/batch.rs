//! The engine entries of one log entry, folded into one change the RT
//! thread applies at the log entry's index: per matrix coefficient, strip
//! parameter and input slot the last value the log entry sets. A folded
//! log entry holds at most one value per coefficient, per strip parameter
//! and per slot, so the engine's shape bounds it, whatever the number of
//! entries a command translated to.

use std::collections::BTreeMap;
use std::sync::Arc;

use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{StripParam, valid_gain};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::rt_loop::Change;

/// A log entry's gain targets, by coefficient `input * columns + output`.
#[derive(Clone, Debug)]
pub(crate) enum Gains {
    /// Ascending by coefficient.
    Sparse(Vec<(u32, f32)>),
    /// The target of every coefficient, read only where its bit in `set`
    /// is set: smaller than the list once about half the coefficients
    /// change.
    Dense { gains: Vec<f32>, set: Vec<u64> },
}

impl Gains {
    /// Calls `f` with each coefficient set and its target, ascending.
    pub fn each(&self, mut f: impl FnMut(usize, f32)) {
        match self {
            Gains::Sparse(list) => {
                for &(c, g) in list {
                    f(c as usize, g);
                }
            }
            Gains::Dense { gains, set } => {
                for (w, &word) in set.iter().enumerate() {
                    let mut bits = word;
                    while bits != 0 {
                        let c = w * 64 + bits.trailing_zeros() as usize;
                        f(c, gains[c]);
                        bits &= bits - 1;
                    }
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Gains::Sparse(list) => list.len(),
            Gains::Dense { set, .. } => set.iter().map(|w| w.count_ones() as usize).sum(),
        }
    }

    fn list(&self) -> Vec<(u32, f32)> {
        let mut list = Vec::with_capacity(self.len());
        self.each(|c, g| list.push((c as u32, g)));
        list
    }

    /// `list`, ascending and unique, in the smaller of the two forms for
    /// `coefficients` coefficients.
    fn of(list: Vec<(u32, f32)>, coefficients: usize) -> Gains {
        let words = coefficients.div_ceil(64);
        if list.len() * 8 <= coefficients * 4 + words * 8 {
            return Gains::Sparse(list);
        }
        let mut gains = vec![0.0; coefficients];
        let mut set = vec![0u64; words];
        for (c, g) in list {
            gains[c as usize] = g;
            set[c as usize / 64] |= 1 << (c % 64);
        }
        Gains::Dense { gains, set }
    }
}

impl PartialEq for Gains {
    fn eq(&self, other: &Gains) -> bool {
        self.list() == other.list()
    }
}

/// A core carries the list form, whatever the form held.
impl Serialize for Gains {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.list().serialize(s)
    }
}

impl<'de> Deserialize<'de> for Gains {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Gains, D::Error> {
        Ok(Gains::Sparse(Vec::deserialize(d)?))
    }
}

/// What a folded log entry sets besides its generation-needing entry,
/// which its [`Change::Batch`] names. Never changed once folded: the RT
/// thread, a core copy and the log share one by [`Arc`], and only off the
/// RT thread is the last one dropped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Batch {
    pub gains: Gains,
    /// Strip parameters by slot, one per parameter key ([`strip_key`]),
    /// ascending by slot and key.
    pub strips: Vec<(u32, StripParam)>,
    /// The slots going down, ascending.
    pub downs: Vec<u32>,
}

/// Distinguishes the strip parameters a log entry folds: one value per
/// key, the last.
pub(crate) fn strip_key(p: &StripParam) -> (u8, usize) {
    match *p {
        StripParam::Trim { .. } => (0, 0),
        StripParam::LegTrim { leg, .. } => (1, leg),
        StripParam::Polarity { leg, .. } => (2, leg),
        StripParam::Hpf { .. } => (3, 0),
        StripParam::Eq { .. } => (4, 0),
        StripParam::EqBand { band, .. } => (5, band),
        StripParam::Gate(_) => (6, 0),
        StripParam::Comp(_) => (7, 0),
        StripParam::DynamicsBeforeEq { .. } => (8, 0),
    }
}

impl Batch {
    /// Every value is one an engine holds: gains within `MAX_GAIN`, strip
    /// parameters within their ranges, keys strictly ascending.
    pub fn sane(&self) -> bool {
        let gains = self.gains.list();
        gains.windows(2).all(|w| w[0].0 < w[1].0)
            && gains.iter().all(|&(_, g)| valid_gain(g))
            && self.strips.iter().all(|(_, p)| p.check(usize::MAX).is_ok())
            && self
                .strips
                .windows(2)
                .all(|w| (w[0].0, strip_key(&w[0].1)) < (w[1].0, strip_key(&w[1].1)))
            && self.downs.windows(2).all(|w| w[0] < w[1])
    }

    /// Whether every value fits an engine of `coefficients` coefficients
    /// and input slots of `legs` legs.
    pub fn fits(&self, coefficients: usize, legs: &[usize]) -> bool {
        let mut gains = true;
        self.gains.each(|c, _| gains &= c < coefficients);
        let strip = |&(s, p): &(u32, StripParam)| {
            legs.get(s as usize)
                .is_some_and(|&l| l <= MAX_LEGS && p.check(l).is_ok())
        };
        gains
            && self.strips.iter().all(strip)
            && self.downs.iter().all(|&s| (s as usize) < legs.len())
    }
}

/// What one log entry does to one input slot, its entries taken in
/// order: re-routed to the last flow a `Route` names, down, and up, which
/// fades in from silence at the index; a later `InputDown` cancels an
/// `InputUp` before it, a later `InputUp` follows an `InputDown` before
/// it. Applied in that order.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct SlotFold {
    routed: bool,
    down: bool,
    up: bool,
}

/// What `changes`, the checked engine entries of one log entry in order,
/// fold into: none for no change, a single change as itself, more as a
/// [`Change::Batch`] with its batch. Per coefficient and strip parameter
/// key the last value counts. Per input slot a `Route` and its up or down
/// state are kept apart ([`SlotFold`]): an `InputUp` and a `Route` raise
/// the slot on the new flow, a `Route` and an `InputDown` leave it down on
/// the new flow. `columns` is the matrix's column count, `coefficients`
/// its size.
#[allow(clippy::type_complexity)]
pub(crate) fn fold(
    changes: &[Change],
    columns: usize,
    coefficients: usize,
) -> Result<Option<(Change, Option<Arc<Batch>>)>, String> {
    let mut gains = BTreeMap::new();
    let mut strips = BTreeMap::new();
    let mut slots: BTreeMap<usize, SlotFold> = BTreeMap::new();
    for &c in changes {
        match c {
            Change::Gain {
                input,
                output,
                gain,
            } => {
                gains.insert((input * columns + output) as u32, gain);
            }
            Change::Strip { slot, param } => {
                strips.insert((slot as u32, strip_key(&param)), param);
            }
            Change::Route { slot } => slots.entry(slot).or_default().routed = true,
            Change::InputDown { slot } => {
                let s = slots.entry(slot).or_default();
                s.down = true;
                s.up = false;
            }
            Change::InputUp { slot } => slots.entry(slot).or_default().up = true,
            Change::Batch { .. } => return Err("a batch inside a batch".to_owned()),
        }
    }
    let downs: Vec<u32> = slots
        .iter()
        .filter(|(_, s)| s.down)
        .map(|(&slot, _)| slot as u32)
        .collect();
    let mask = |f: fn(&SlotFold) -> bool| {
        slots
            .iter()
            .filter(|(_, s)| f(s))
            .fold(0u64, |m, (&slot, _)| m | 1 << slot)
    };
    let (up, routed) = (mask(|s| s.up), mask(|s| s.routed));
    let facts = gains.len()
        + strips.len()
        + downs.len()
        + up.count_ones() as usize
        + routed.count_ones() as usize;
    if facts == 0 {
        return Ok(None);
    }
    if facts == 1 {
        let slot = (up | routed).trailing_zeros() as usize;
        let single = if let Some((&c, &gain)) = gains.first_key_value() {
            let c = c as usize;
            Change::Gain {
                input: c / columns,
                output: c % columns,
                gain,
            }
        } else if let Some((&(slot, _), &param)) = strips.first_key_value() {
            Change::Strip {
                slot: slot as usize,
                param,
            }
        } else if let Some(&slot) = downs.first() {
            Change::InputDown {
                slot: slot as usize,
            }
        } else if up != 0 {
            Change::InputUp { slot }
        } else {
            Change::Route { slot }
        };
        return Ok(Some((single, None)));
    }
    let batch = Batch {
        gains: Gains::of(gains.into_iter().collect(), coefficients),
        strips: strips.into_iter().map(|((s, _), p)| (s, p)).collect(),
        downs,
    };
    let change = Change::Batch {
        strips: !batch.strips.is_empty(),
        up,
        routed,
    };
    Ok(Some((change, Some(Arc::new(batch)))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gain(input: usize, output: usize, gain: f32) -> Change {
        Change::Gain {
            input,
            output,
            gain,
        }
    }

    fn trim(slot: usize, db: f32) -> Change {
        Change::Strip {
            slot,
            param: StripParam::Trim { db },
        }
    }

    /// Per coefficient and strip parameter the last value counts, whatever
    /// the number of entries: a log entry's batch is bounded by the
    /// engine's shape.
    #[test]
    fn a_log_entry_folds_to_the_last_value_per_key() {
        let mut changes = Vec::new();
        for n in 0..1000 {
            changes.push(gain(n % 2, 1, n as f32 / 1000.0));
            changes.push(trim(0, -(n as f32) / 100.0));
        }
        changes.push(Change::InputDown { slot: 1 });
        let (change, batch) = fold(&changes, 3, 6).unwrap().unwrap();
        let batch = batch.unwrap();
        assert_eq!(
            change,
            Change::Batch {
                strips: true,
                up: 0,
                routed: 0
            }
        );
        assert_eq!(batch.gains.list(), [(1, 0.998), (4, 0.999)]);
        assert_eq!(batch.strips, [(0, StripParam::Trim { db: -9.99 })]);
        assert_eq!(batch.downs, [1]);
        assert!(batch.sane() && batch.fits(6, &[1, 1]));
        assert!(!batch.fits(4, &[1, 1]), "coefficient 4 of 4");
    }

    /// Per slot a re-route and the up or down state are kept apart, in
    /// the order the command gave them: a route is never lost to an
    /// `InputDown` after it, an `InputUp` and a `Route` raise the slot on
    /// the new flow, and an `InputUp` after an `InputDown` fades in from
    /// silence, its down kept.
    #[test]
    fn a_slots_route_and_state_fold_apart() {
        let (up, down, route) = (
            Change::InputUp { slot: 1 },
            Change::InputDown { slot: 1 },
            Change::Route { slot: 1 },
        );
        // (downs, up mask, routed mask) of each sequence on slot 1.
        let facts = |changes: &[Change]| -> (Vec<u32>, u64, u64) {
            match fold(changes, 2, 4).unwrap().unwrap() {
                (Change::Batch { up, routed, .. }, Some(b)) => (b.downs.clone(), up, routed),
                (Change::InputUp { slot }, None) => (vec![], 1 << slot, 0),
                (Change::InputDown { slot }, None) => (vec![slot as u32], 0, 0),
                (Change::Route { slot }, None) => (vec![], 0, 1 << slot),
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(facts(&[up, route]), (vec![], 2, 2), "routed and raised");
        assert_eq!(facts(&[route, up]), (vec![], 2, 2), "routed and raised");
        assert_eq!(facts(&[route, down]), (vec![1], 0, 2), "routed and down");
        assert_eq!(facts(&[down, route]), (vec![1], 0, 2), "routed and down");
        assert_eq!(facts(&[down, up]), (vec![1], 2, 0), "down, then a fade in");
        assert_eq!(facts(&[up, down]), (vec![1], 0, 0), "down");
        assert_eq!(facts(&[down, up, down]), (vec![1], 0, 0), "down");
        assert_eq!(facts(&[up, up]), (vec![], 2, 0));
        assert_eq!(facts(&[route, down, up]), (vec![1], 2, 2));
    }

    /// No change folds to none, one change to itself; generation-needing
    /// entries of several slots fold into one batch.
    #[test]
    fn a_log_entry_of_one_change_or_none_folds_to_itself() {
        assert_eq!(fold(&[], 2, 4).unwrap(), None);
        for c in [
            gain(1, 1, 0.5),
            trim(1, -3.0),
            Change::InputDown { slot: 1 },
            Change::InputUp { slot: 1 },
            Change::Route { slot: 1 },
        ] {
            assert_eq!(fold(&[c, c], 2, 4).unwrap(), Some((c, None)));
        }
        let up = |slot| Change::InputUp { slot };
        assert_eq!(
            fold(&[up(0), Change::Route { slot: 1 }], 2, 4).unwrap(),
            Some((
                Change::Batch {
                    strips: false,
                    up: 1,
                    routed: 2
                },
                Some(Arc::new(Batch {
                    gains: Gains::Sparse(Vec::new()),
                    strips: Vec::new(),
                    downs: Vec::new(),
                }))
            ))
        );
    }

    /// A batch setting most coefficients is held dense, a few as a list;
    /// both carry the list form in a core.
    #[test]
    fn gains_take_the_smaller_form() {
        let all: Vec<Change> = (0..64).map(|c| gain(c / 8, c % 8, 0.25)).collect();
        let (_, dense) = fold(&all, 8, 64).unwrap().unwrap();
        let dense = dense.unwrap();
        assert!(matches!(dense.gains, Gains::Dense { .. }));
        let (_, sparse) = fold(&all[..3], 8, 64).unwrap().unwrap();
        let sparse = sparse.unwrap();
        assert!(matches!(sparse.gains, Gains::Sparse(_)));
        let wire = serde_json::to_string(&dense).unwrap();
        let back: Batch = serde_json::from_str(&wire).unwrap();
        assert!(matches!(back.gains, Gains::Sparse(_)));
        assert_eq!(back, *dense);
    }
}
