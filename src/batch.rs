//! The engine entries of one log entry, folded into one change the RT
//! thread applies at the log entry's index: per matrix coefficient, strip
//! parameter, input slot, duck setting, depth and duck mask the last
//! value the log entry sets. A folded log entry holds at most one value
//! of each, so the engine's shape bounds it, whatever the number of
//! entries a command translated to.

use std::collections::BTreeMap;
use std::sync::Arc;

use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{LimiterParams, MAX_DUCKS, StripParam, valid_gain};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ducks::{DuckChange, valid_depth};
use crate::rt_loop::Change;

/// A log entry's values per matrix coefficient `input * columns +
/// output`: its gain targets ([`Gains`]) or its duck masks.
#[derive(Clone, Debug)]
pub(crate) enum Coefs<T> {
    /// Ascending by coefficient.
    Sparse(Vec<(u32, T)>),
    /// The value of every coefficient, read only where its bit in `set`
    /// is set: smaller than the list once about half the coefficients
    /// change.
    Dense { values: Vec<T>, set: Vec<u64> },
}

/// A log entry's gain targets.
pub(crate) type Gains = Coefs<f32>;

impl<T: Copy + Default> Coefs<T> {
    /// Calls `f` with each coefficient set and its value, ascending.
    pub fn each(&self, mut f: impl FnMut(usize, T)) {
        match self {
            Coefs::Sparse(list) => {
                for &(c, g) in list {
                    f(c as usize, g);
                }
            }
            Coefs::Dense { values, set } => {
                for (w, &word) in set.iter().enumerate() {
                    let mut bits = word;
                    while bits != 0 {
                        let c = w * 64 + bits.trailing_zeros() as usize;
                        f(c, values[c]);
                        bits &= bits - 1;
                    }
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Coefs::Sparse(list) => list.len(),
            Coefs::Dense { set, .. } => set.iter().map(|w| w.count_ones() as usize).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn list(&self) -> Vec<(u32, T)> {
        let mut list = Vec::with_capacity(self.len());
        self.each(|c, g| list.push((c as u32, g)));
        list
    }

    /// `list`, ascending and unique, in the smaller of the two forms for
    /// `coefficients` coefficients.
    fn of(list: Vec<(u32, T)>, coefficients: usize) -> Coefs<T> {
        let words = coefficients.div_ceil(64);
        let item = std::mem::size_of::<(u32, T)>();
        if list.len() * item <= coefficients * std::mem::size_of::<T>() + words * 8 {
            return Coefs::Sparse(list);
        }
        let mut values = vec![T::default(); coefficients];
        let mut set = vec![0u64; words];
        for (c, g) in list {
            values[c as usize] = g;
            set[c as usize / 64] |= 1 << (c % 64);
        }
        Coefs::Dense { values, set }
    }

    /// Whether the coefficients ascend strictly, each below `end`, each
    /// value taken by `ok`.
    fn sane(&self, end: usize, ok: impl Fn(T) -> bool) -> bool {
        let list = self.list();
        list.windows(2).all(|w| w[0].0 < w[1].0)
            && list.iter().all(|&(c, v)| (c as usize) < end && ok(v))
    }
}

impl<T: Copy + Default + PartialEq> PartialEq for Coefs<T> {
    fn eq(&self, other: &Coefs<T>) -> bool {
        self.list() == other.list()
    }
}

impl<T: Copy + Default> Default for Coefs<T> {
    fn default() -> Self {
        Coefs::Sparse(Vec::new())
    }
}

/// A core carries the list form, whatever the form held.
impl<T: Copy + Default + Serialize> Serialize for Coefs<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.list().serialize(s)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Coefs<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Coefs<T>, D::Error> {
        Ok(Coefs::Sparse(Vec::deserialize(d)?))
    }
}

/// What a log entry sets of its duck groups, besides its [`Change`]s.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum DuckItem {
    /// A setting of detector `group`.
    Set { group: u8, change: DuckChange },
    /// The depth of input channel `row` under detector `group`.
    Depth { row: u32, group: u8, db: f32 },
    /// The duck mask of a coefficient.
    Mask {
        input: usize,
        output: usize,
        mask: u8,
    },
}

/// One thing a log entry's engine entry sets.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Item {
    Change(Change),
    Duck(DuckItem),
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
    /// Limiter parameters by output, ascending.
    pub limiters: Vec<(u32, LimiterParams)>,
    /// The slots going down, ascending.
    pub downs: Vec<u32>,
    /// The outputs enabled (true) or disabled, ascending.
    pub outputs: Vec<(u32, bool)>,
    /// Duck settings by group, one per kind ([`DuckChange::kind`]),
    /// ascending by group and kind.
    pub ducks: Vec<(u8, DuckChange)>,
    /// Duck depths by input channel and group, ascending.
    pub depths: Vec<(u32, u8, f32)>,
    /// Duck masks by coefficient.
    pub masks: Coefs<u8>,
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
    /// parameters, duck settings and depths within their ranges, groups
    /// below [`MAX_DUCKS`], keys strictly ascending.
    pub fn sane(&self) -> bool {
        let gains = self.gains.list();
        gains.windows(2).all(|w| w[0].0 < w[1].0)
            && gains.iter().all(|&(_, g)| valid_gain(g))
            && self.strips.iter().all(|(_, p)| p.check(usize::MAX).is_ok())
            && self
                .strips
                .windows(2)
                .all(|w| (w[0].0, strip_key(&w[0].1)) < (w[1].0, strip_key(&w[1].1)))
            && self.limiters.iter().all(|(_, p)| p.check().is_ok())
            && self.limiters.windows(2).all(|w| w[0].0 < w[1].0)
            && self.downs.windows(2).all(|w| w[0] < w[1])
            && self.outputs.windows(2).all(|w| w[0].0 < w[1].0)
            && self
                .ducks
                .iter()
                .all(|(g, c)| usize::from(*g) < MAX_DUCKS && c.sane())
            && self
                .ducks
                .windows(2)
                .all(|w| (w[0].0, w[0].1.kind()) < (w[1].0, w[1].1.kind()))
            && (self.depths.iter()).all(|&(_, g, db)| usize::from(g) < MAX_DUCKS && valid_depth(db))
            && self
                .depths
                .windows(2)
                .all(|w| (w[0].0, w[0].1) < (w[1].0, w[1].1))
            && self.masks.sane(usize::MAX, |_| true)
    }

    /// Whether every value fits an engine of `inputs` input and `columns`
    /// matrix channels, `outputs` outputs, input slots of `legs` legs and
    /// the duck columns `ducked`: a mask other than 0 only on those.
    /// Limiter parameters fit any output: one without a limiter refuses
    /// them on the RT thread, as a slot without a strip does.
    pub fn fits(
        &self,
        inputs: usize,
        columns: usize,
        outputs: usize,
        legs: &[usize],
        ducked: &[usize],
    ) -> bool {
        let coefficients = inputs * columns;
        let mut gains = true;
        self.gains.each(|c, _| gains &= c < coefficients);
        let strip = |&(s, p): &(u32, StripParam)| {
            legs.get(s as usize)
                .is_some_and(|&l| l <= MAX_LEGS && p.check(l).is_ok())
        };
        gains
            && self.strips.iter().all(strip)
            && self.downs.iter().all(|&s| (s as usize) < legs.len())
            && self.outputs.iter().all(|&(o, _)| (o as usize) < outputs)
            && self.limiters.iter().all(|&(o, _)| (o as usize) < outputs)
            && self.ducks.iter().all(|(_, c)| c.fits(legs))
            && self
                .depths
                .iter()
                .all(|&(row, _, _)| (row as usize) < inputs)
            && self.masks.sane(coefficients, |_| true)
            && {
                let mut on = true;
                self.masks
                    .each(|c, k| on &= k == 0 || ducked.contains(&(c % columns)));
                on
            }
    }

    /// Whether it sets anything of the matrix's ducks.
    pub fn ducks_matrix(&self) -> bool {
        !self.depths.is_empty() || !self.masks.is_empty()
    }
}

/// What one log entry does to one input slot, its entries taken in
/// order: re-routed to the last flow a `Route` names, or detached when a
/// `Detach` came after the last `Route`, down, and up, which fades in
/// from silence at the index; a later `InputDown` cancels an `InputUp`
/// before it, a later `InputUp` follows an `InputDown` before it.
/// Applied in that order.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct SlotFold {
    routed: bool,
    detached: bool,
    down: bool,
    up: bool,
}

/// What `items`, the checked engine entries of one log entry in order,
/// fold into: none for no change, a single change as itself, more, or
/// any duck setting, depth or mask, as a [`Change::Batch`] with its
/// batch. Per coefficient, strip parameter key, duck setting kind, depth
/// and mask the last value counts, and per output its last `Output`
/// and its last limiter parameters. Per
/// input slot a `Route` and its up or
/// down state are kept apart ([`SlotFold`]): an `InputUp` and a `Route`
/// raise the slot on the new flow, a `Route` and an `InputDown` leave it
/// down on the new flow. Of a slot's `Route`s and `Detach`es the last
/// counts. `columns` is the matrix's column count,
/// `coefficients` its size.
#[allow(clippy::type_complexity)]
pub(crate) fn fold(
    items: &[Item],
    columns: usize,
    coefficients: usize,
) -> Result<Option<(Change, Option<Arc<Batch>>)>, String> {
    let mut gains = BTreeMap::new();
    let mut strips = BTreeMap::new();
    let mut slots: BTreeMap<usize, SlotFold> = BTreeMap::new();
    let mut ducks = BTreeMap::new();
    let mut depths = BTreeMap::new();
    let mut masks = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    let mut limiters = BTreeMap::new();
    for &item in items {
        let c = match item {
            Item::Change(c) => c,
            Item::Duck(DuckItem::Set { group, change }) => {
                ducks.insert((group, change.kind()), change);
                continue;
            }
            Item::Duck(DuckItem::Depth { row, group, db }) => {
                depths.insert((row, group), db);
                continue;
            }
            Item::Duck(DuckItem::Mask {
                input,
                output,
                mask,
            }) => {
                masks.insert((input * columns + output) as u32, mask);
                continue;
            }
        };
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
            Change::Limiter { output, params } => {
                limiters.insert(output as u32, params);
            }
            Change::Route { slot } | Change::Detach { slot } => {
                let s = slots.entry(slot).or_default();
                s.routed = true;
                s.detached = matches!(c, Change::Detach { .. });
            }
            Change::InputDown { slot } => {
                let s = slots.entry(slot).or_default();
                s.down = true;
                s.up = false;
            }
            Change::InputUp { slot } => slots.entry(slot).or_default().up = true,
            Change::Output { output, enabled } => {
                outputs.insert(output as u32, enabled);
            }
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
    let detached = mask(|s| s.detached);
    let facts = gains.len()
        + strips.len()
        + limiters.len()
        + downs.len()
        + outputs.len()
        + up.count_ones() as usize
        + routed.count_ones() as usize;
    let duck_facts = ducks.len() + depths.len() + masks.len();
    if facts + duck_facts == 0 {
        return Ok(None);
    }
    if facts == 1 && duck_facts == 0 {
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
        } else if let Some((&output, &params)) = limiters.first_key_value() {
            Change::Limiter {
                output: output as usize,
                params,
            }
        } else if let Some(&slot) = downs.first() {
            Change::InputDown {
                slot: slot as usize,
            }
        } else if let Some((&output, &enabled)) = outputs.first_key_value() {
            Change::Output {
                output: output as usize,
                enabled,
            }
        } else if up != 0 {
            Change::InputUp { slot }
        } else if detached != 0 {
            Change::Detach { slot }
        } else {
            Change::Route { slot }
        };
        return Ok(Some((single, None)));
    }
    let batch = Batch {
        gains: Coefs::of(gains.into_iter().collect(), coefficients),
        strips: strips.into_iter().map(|((s, _), p)| (s, p)).collect(),
        limiters: limiters.into_iter().collect(),
        downs,
        outputs: outputs.into_iter().collect(),
        ducks: ducks.into_iter().map(|((g, _), c)| (g, c)).collect(),
        depths: (depths.into_iter())
            .map(|((row, g), db)| (row, g, db))
            .collect(),
        masks: Coefs::of(masks.into_iter().collect(), coefficients),
    };
    let change = Change::Batch {
        strips: !batch.strips.is_empty() || !batch.limiters.is_empty(),
        up,
        routed,
        detached,
    };
    Ok(Some((change, Some(Arc::new(batch)))))
}

#[cfg(test)]
mod tests {
    use samplez_dsp::{DetectorParams, DuckMode};

    use super::*;
    use crate::ducks::KeySet;

    fn items(changes: &[Change]) -> Vec<Item> {
        changes.iter().map(|&c| Item::Change(c)).collect()
    }

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
        let (change, batch) = fold(&items(&changes), 3, 6).unwrap().unwrap();
        let batch = batch.unwrap();
        assert_eq!(
            change,
            Change::Batch {
                strips: true,
                up: 0,
                routed: 0,
                detached: 0
            }
        );
        assert_eq!(batch.gains.list(), [(1, 0.998), (4, 0.999)]);
        assert_eq!(batch.strips, [(0, StripParam::Trim { db: -9.99 })]);
        assert_eq!(batch.downs, [1]);
        assert!(batch.sane() && batch.fits(2, 3, 1, &[1, 1], &[]));
        assert!(!batch.fits(1, 4, 1, &[1, 1], &[]), "coefficient 4 of 4");
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
            match fold(&items(changes), 2, 4).unwrap().unwrap() {
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

    /// An output's enable and disable in one log entry: the last counts.
    #[test]
    fn an_outputs_last_enable_or_disable_wins() {
        let set = |output, enabled| Change::Output { output, enabled };
        let one = |changes: &[Change]| fold(&items(changes), 2, 4).unwrap().unwrap();
        assert_eq!(one(&[set(1, false)]), (set(1, false), None));
        assert_eq!(one(&[set(1, false), set(1, true)]), (set(1, true), None));
        assert_eq!(one(&[set(1, true), set(1, false)]), (set(1, false), None));
        // Two outputs: a batch holding each one's last, ascending.
        let (change, batch) = one(&[set(3, false), set(0, false), set(3, true)]);
        assert!(matches!(
            change,
            Change::Batch {
                up: 0,
                routed: 0,
                ..
            }
        ));
        let batch = batch.unwrap();
        assert_eq!(batch.outputs, [(0, false), (3, true)]);
        assert!(batch.sane() && batch.fits(2, 2, 4, &[1, 1], &[]));
        assert!(!batch.fits(2, 2, 3, &[1, 1], &[]), "output 3 of 3");
        let unordered = Batch {
            outputs: vec![(3, true), (0, false)],
            ..(*batch).clone()
        };
        assert!(!unordered.sane());
    }

    /// An output's limiter parameters in one log entry: the last counts,
    /// a lone one is itself, and a batch holding them says it sets
    /// parameters, as one holding strip parameters does.
    #[test]
    fn an_outputs_last_limiter_parameters_win() {
        let set = |output, ceiling_db| Change::Limiter {
            output,
            params: samplez_dsp::LimiterParams {
                enabled: true,
                ceiling_db,
                release_ms: 50.0,
            },
        };
        let one = |changes: &[Change]| fold(&items(changes), 2, 4).unwrap().unwrap();
        assert_eq!(one(&[set(1, -1.0), set(1, -2.0)]), (set(1, -2.0), None));
        let (change, batch) = one(&[set(3, -1.0), set(0, -1.0), set(3, -3.0)]);
        assert!(
            matches!(change, Change::Batch { strips: true, .. }),
            "{change:?}"
        );
        let batch = batch.unwrap();
        let params = |c: Change| match c {
            Change::Limiter { params, .. } => params,
            _ => unreachable!(),
        };
        assert_eq!(
            batch.limiters,
            [(0, params(set(0, -1.0))), (3, params(set(3, -3.0)))]
        );
        assert!(batch.sane() && batch.fits(2, 2, 4, &[1, 1], &[]));
        assert!(!batch.fits(2, 2, 3, &[1, 1], &[]), "output 3 of 3");
        let mut bad = (*batch).clone();
        bad.limiters[0].1.ceiling_db = 3.0;
        assert!(!bad.sane());
        bad.limiters = vec![batch.limiters[1], batch.limiters[0]];
        assert!(!bad.sane());
    }

    /// Of a slot's `Route`s and `Detach`es in one log entry the last
    /// counts: a `Detach` after a `Route` leaves the slot detached, a
    /// `Route` after a `Detach` re-routes it, and either is one change.
    #[test]
    fn a_slots_route_and_detach_fold_last_wins() {
        let (route, detach) = (Change::Route { slot: 1 }, Change::Detach { slot: 1 });
        let one = |changes: &[Change]| fold(&items(changes), 2, 4).unwrap().unwrap().0;
        assert_eq!(one(&[route, detach]), detach);
        assert_eq!(one(&[detach, route]), route);
        assert_eq!(one(&[route, detach, route, detach]), detach);
        // With another slot raised, a batch: slot 1 re-routed and
        // detached, slot 0 only raised.
        let up = Change::InputUp { slot: 0 };
        for (changes, detached) in [([up, route, detach], 2), ([up, detach, route], 0)] {
            let Change::Batch {
                up,
                routed,
                detached: d,
                ..
            } = one(&changes)
            else {
                panic!("{changes:?}");
            };
            assert_eq!((up, routed, d), (1, 2, detached), "{changes:?}");
        }
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
            Change::Detach { slot: 1 },
        ] {
            assert_eq!(fold(&items(&[c, c]), 2, 4).unwrap(), Some((c, None)));
        }
        let up = |slot| Change::InputUp { slot };
        assert_eq!(
            fold(&items(&[up(0), Change::Route { slot: 1 }]), 2, 4).unwrap(),
            Some((
                Change::Batch {
                    strips: false,
                    up: 1,
                    routed: 2,
                    detached: 0
                },
                Some(Arc::new(Batch {
                    gains: Gains::Sparse(Vec::new()),
                    strips: Vec::new(),
                    limiters: Vec::new(),
                    downs: Vec::new(),
                    outputs: Vec::new(),
                    ducks: Vec::new(),
                    depths: Vec::new(),
                    masks: Coefs::default(),
                }))
            ))
        );
    }

    /// A batch setting most coefficients is held dense, a few as a list;
    /// both carry the list form in a core.
    #[test]
    fn gains_take_the_smaller_form() {
        let all: Vec<Change> = (0..64).map(|c| gain(c / 8, c % 8, 0.25)).collect();
        let (_, dense) = fold(&items(&all), 8, 64).unwrap().unwrap();
        let dense = dense.unwrap();
        assert!(matches!(dense.gains, Gains::Dense { .. }));
        let (_, sparse) = fold(&items(&all[..3]), 8, 64).unwrap().unwrap();
        let sparse = sparse.unwrap();
        assert!(matches!(sparse.gains, Gains::Sparse(_)));
        let wire = serde_json::to_string(&dense).unwrap();
        let back: Batch = serde_json::from_str(&wire).unwrap();
        assert!(matches!(back.gains, Gains::Sparse(_)));
        assert_eq!(back, *dense);
    }

    fn duck(group: u8, change: DuckChange) -> Item {
        Item::Duck(DuckItem::Set { group, change })
    }

    /// Per group and kind of setting, per depth's channel and group and
    /// per mask's coefficient the last value counts; a duck item alone
    /// folds into a batch, whose settings ascend by group and kind.
    #[test]
    fn duck_items_fold_to_the_last_value_per_key() {
        let keys = |k: &[(usize, f32)]| DuckChange::Keys(KeySet::of(k).unwrap());
        let depth = |row, group, db| Item::Duck(DuckItem::Depth { row, group, db });
        let mask = |input, output, mask| {
            Item::Duck(DuckItem::Mask {
                input,
                output,
                mask,
            })
        };
        let mut list = Vec::new();
        for n in 0..100u8 {
            list.push(duck(3, DuckChange::Mode(DuckMode::On)));
            list.push(duck(1, keys(&[(usize::from(n % 4), 1.0)])));
            list.push(duck(3, DuckChange::Hold(n % 2 == 0)));
            list.push(depth(u32::from(n % 2), 3, f32::from(n) / 10.0));
            list.push(mask(1, usize::from(n % 3), n));
        }
        list.push(duck(3, DuckChange::Mode(DuckMode::Off)));
        list.push(duck(
            1,
            DuckChange::Detector(DetectorParams {
                attack_ms: 5.0,
                ..DetectorParams::DEFAULT
            }),
        ));
        let (change, b) = fold(&list, 3, 6).unwrap().unwrap();
        let b = b.unwrap();
        assert_eq!(
            change,
            Change::Batch {
                strips: false,
                up: 0,
                routed: 0,
                detached: 0
            }
        );
        assert_eq!(
            b.ducks,
            [
                (
                    1,
                    DuckChange::Detector(DetectorParams {
                        attack_ms: 5.0,
                        ..DetectorParams::DEFAULT
                    })
                ),
                (1, keys(&[(3, 1.0)])),
                (3, DuckChange::Mode(DuckMode::Off)),
                (3, DuckChange::Hold(false)),
            ]
        );
        assert_eq!(b.depths, [(0, 3, 9.8), (1, 3, 9.9)]);
        assert_eq!(b.masks.list(), [(3, 99), (4, 97), (5, 98)]);
        let all = [0, 1, 2];
        assert!(b.sane() && b.fits(2, 3, 1, &[1, 1, 1, 1], &all));
        assert!(!b.fits(2, 3, 1, &[1, 1, 1], &all), "a key of slot 3 of 3");
        assert!(
            !b.fits(1, 3, 1, &[1, 1, 1, 1], &all),
            "a depth of channel 1 of 1"
        );
        // Masks 99, 97 and 98 on columns 0, 1 and 2.
        assert!(
            !b.fits(2, 3, 1, &[1, 1, 1, 1], &[0, 2]),
            "a mask off the duck columns"
        );
        for item in [
            duck(0, DuckChange::Hold(true)),
            depth(0, 0, 1.0),
            mask(0, 0, 1),
        ] {
            let (change, b) = fold(&[item], 3, 6).unwrap().unwrap();
            assert!(
                matches!(change, Change::Batch { .. }) && b.is_some(),
                "{item:?}"
            );
        }
    }

    /// A batch holding a duck value no engine holds is not sane.
    #[test]
    fn a_batch_with_a_duck_value_no_engine_holds_is_not_sane() {
        let sane = |item: Item| fold(&[item], 3, 6).unwrap().unwrap().1.unwrap().sane();
        assert!(sane(duck(7, DuckChange::Hold(true))));
        assert!(!sane(duck(8, DuckChange::Hold(true))), "group");
        let p = DetectorParams {
            threshold_db: 1.0,
            ..DetectorParams::DEFAULT
        };
        assert!(!sane(duck(0, DuckChange::Detector(p))), "threshold");
        let twice = KeySet::of(&[(0, 1.0), (0, 1.0)]).unwrap();
        assert!(
            !sane(duck(0, DuckChange::Keys(twice))),
            "a slot keyed twice"
        );
        assert!(!sane(Item::Duck(DuckItem::Depth {
            row: 0,
            group: 0,
            db: 41.0
        })));
        assert!(!sane(Item::Duck(DuckItem::Depth {
            row: 0,
            group: 8,
            db: 1.0
        })));
    }
}
