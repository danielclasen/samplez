//! A replica's rank per block (plan 2.11, Baseline): fixed when F passes
//! the block, from the `Baseline` entries the replica knew of by the
//! block's apply deadline `T(kB) - applyGuard`.
//!
//! The replicas rank by the `Baseline` latest in log order among those at
//! or below kB whose commit they knew in time: the baseline first, then
//! its order, then the audio voters the order omits, by ordinal. A
//! replica that knows of a `Baseline` at or below kB whose commit it did
//! not know by that deadline ranks as demoted for k, as do its fills: below
//! every other, by ordinal.

use std::collections::BTreeMap;

use samplez::blocks::{MAX_RANK, Ranks};

use crate::contact::{Members, Timing};
use crate::entry::{Baseline, NodeId};
use crate::membership::{Slot, slot_of};
use crate::snapshot::StoredLogId;

/// A log id as a map key, in log order.
type Key = (u64, u64, u64);

fn key(id: StoredLogId) -> Key {
    (id.index, id.term, id.node)
}

#[derive(Debug, Clone)]
struct Known {
    at_index: u64,
    /// The entry, once this replica's log holds it.
    body: Option<Baseline>,
    /// The TAI time this replica learned it committed.
    committed_at: Option<u64>,
}

/// The `Baseline` entries a replica knows of: from its log, applied
/// before it started, or listed by another voter.
#[derive(Debug, Default)]
pub(crate) struct Baselines {
    known: BTreeMap<Key, Known>,
}

/// The audio voters of `members` by ordinal, then node id.
pub fn audio_voters(members: &Members) -> Vec<NodeId> {
    let mut v: Vec<NodeId> = members
        .voters()
        .into_iter()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect();
    v.sort_by_key(|&id| (slot_of(id), id));
    v
}

impl Baselines {
    /// The `Baseline` the state applied before contact started: known
    /// committed long ago, below every entry of the log after it.
    pub fn applied(&mut self, baseline: Baseline) {
        self.known.insert(
            (0, 0, 0),
            Known {
                at_index: baseline.at_index,
                body: Some(baseline),
                committed_at: Some(0),
            },
        );
    }

    /// Entry `id` of this replica's log is `baseline`.
    pub fn appended(&mut self, id: StoredLogId, baseline: &Baseline) {
        let k = self.known.entry(key(id)).or_insert(Known {
            at_index: baseline.at_index,
            body: None,
            committed_at: None,
        });
        k.body = Some(baseline.clone());
    }

    /// Another voter listed `Baseline` `id` at `at_index`.
    pub fn listed(&mut self, id: StoredLogId, at_index: u64) {
        self.known.entry(key(id)).or_insert(Known {
            at_index,
            body: None,
            committed_at: None,
        });
    }

    /// The log lost its entries from `from` on.
    pub fn truncated(&mut self, from: u64) {
        self.known
            .retain(|&(index, _, _), k| index < from || k.body.is_none());
    }

    /// Every entry of this replica's log up to `index` is known committed
    /// at TAI time `tai`. A `Baseline` known only from a listing whose
    /// index this log holds with another id there can never commit: it
    /// stops counting.
    pub fn learned(
        &mut self,
        index: u64,
        tai: u64,
        log_id_at: impl Fn(u64) -> Option<StoredLogId>,
    ) {
        self.known.retain(|&(i, term, node), k| {
            if i == 0 || i > index {
                return true;
            }
            if k.body.is_some() {
                k.committed_at.get_or_insert(tai);
                return true;
            }
            log_id_at(i).is_none_or(|id| key(id) == (i, term, node))
        });
    }

    /// Forgets what no block from `k` on is ranked by: entries known
    /// committed by `k`'s apply deadline, below the latest such at or
    /// below kB.
    pub fn prune(&mut self, k: u64, timing: &Timing) {
        let deadline = timing.apply_deadline(k * timing.block);
        let in_time =
            |kn: &Known| kn.body.is_some() && kn.committed_at.is_some_and(|t| t <= deadline);
        let latest = self
            .known
            .iter()
            .filter(|(_, kn)| kn.at_index <= k * timing.block && in_time(kn))
            .map(|(&key, _)| key)
            .next_back();
        if let Some(latest) = latest {
            self.known
                .retain(|&key, kn| key >= latest || !in_time(kn) || kn.at_index > k * timing.block);
        }
    }

    /// Whether `replica` ranks as demoted for block `k` by what it knows
    /// of `Baseline` entries, and else the one it ranks by.
    fn base(&self, k: u64, timing: &Timing) -> Result<Option<&Baseline>, ()> {
        let deadline = timing.apply_deadline(k * timing.block);
        let mut base = None;
        for kn in self.known.values() {
            if kn.at_index > k * timing.block {
                continue;
            }
            match (&kn.body, kn.committed_at) {
                (Some(b), Some(t)) if t <= deadline => base = Some(b),
                _ => return Err(()),
            }
        }
        Ok(base)
    }

    /// `me`'s rank for block `k` among the audio voters `audio` (by
    /// [`audio_voters`]), 0 the top.
    pub fn rank(&self, k: u64, me: NodeId, audio: &[NodeId], timing: &Timing) -> u8 {
        let Ok(base) = self.base(k, timing) else {
            return demoted(me, audio, Class::Demoted);
        };
        let mut order: Vec<NodeId> = Vec::with_capacity(audio.len());
        if let Some(b) = base {
            for &id in std::iter::once(&b.replica).chain(&b.order) {
                if audio.contains(&id) && !order.contains(&id) {
                    order.push(id);
                }
            }
        }
        for &id in audio {
            if !order.contains(&id) {
                order.push(id);
            }
        }
        match order.iter().position(|&id| id == me) {
            Some(p) => (p as u8).min(MAX_RANK),
            None => demoted(me, audio, Class::Demoted),
        }
    }
}

/// The ranks the RT thread fixes into blocks `[lo, hi)` with block k at
/// `rank_of(k)`: one up to a change and the next from it on. A second
/// change below `hi` is returned: F must not pass it before the RT thread
/// has given every block before the first change its rank.
pub fn schedule(lo: u64, hi: u64, rank_of: impl Fn(u64) -> u8, fill: u8) -> (Ranks, Option<u64>) {
    let rank = rank_of(lo);
    let mut ranks = Ranks {
        rank,
        next: rank,
        from: hi,
        fill,
    };
    let mut last = rank;
    for k in lo + 1..hi {
        let r = rank_of(k);
        if r == last {
            continue;
        }
        if last != rank {
            return (ranks, Some(k));
        }
        ranks.next = r;
        ranks.from = k;
        last = r;
    }
    (ranks, None)
}

/// The classes demoted replicas rank in, below every replica that is
/// not demoted, each by ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    /// Not diverged, with the baseline's key.
    Demoted,
    /// An old-key filler in a key hand-over.
    Filler,
    Diverged,
}

/// `me`'s rank as demoted in `class`: below every audio voter that is not
/// demoted, then by class, then by ordinal. Seven audio voters, the most a
/// set has, rank up to 27 and fit [`MAX_RANK`].
pub fn demoted(me: NodeId, audio: &[NodeId], class: Class) -> u8 {
    let n = audio.len();
    let p = audio.iter().position(|&id| id == me).unwrap_or(n);
    let class = match class {
        Class::Demoted => 1,
        Class::Filler => 2,
        Class::Diverged => 3,
    };
    ((class * n + p) as u8).min(MAX_RANK)
}

/// The bit of `id` in an `onSilenceOf` mask: its ordinal; none for the
/// witness or an ordinal past the mask.
pub fn silence_bit(id: NodeId) -> Option<u8> {
    match slot_of(id) {
        Slot::Audio(o) if o < 8 => Some(1 << o),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::membership::node_id;

    const B: u64 = 48;

    fn a(o: u16) -> NodeId {
        node_id(Slot::Audio(o), 100 + u64::from(o))
    }

    fn members() -> Members {
        let mut c: BTreeSet<NodeId> = (0..3).map(a).collect();
        c.insert(node_id(Slot::Witness, 9));
        Members {
            configs: vec![c],
            addrs: BTreeMap::new(),
        }
    }

    fn id(index: u64) -> StoredLogId {
        StoredLogId {
            term: 1,
            node: 1,
            index,
        }
    }

    fn baseline(replica: NodeId, order: &[NodeId], at_index: u64) -> Baseline {
        Baseline {
            replica,
            order: order.to_vec(),
            at_index,
            key: None,
            versions: None,
        }
    }

    /// Without a `Baseline` the audio voters rank by ordinal; a
    /// `Baseline` ranks its replica first, then its order, then those it
    /// omits; the witness takes no rank.
    #[test]
    fn a_baseline_orders_the_audio_voters_from_its_index() {
        let t = Timing::defaults(B);
        let m = audio_voters(&members());
        let mut b = Baselines::default();
        assert_eq!([0, 1, 2].map(|o| b.rank(1000, a(o), &m[..], &t)), [0, 1, 2]);
        b.appended(id(5), &baseline(a(2), &[a(1)], 1000 * B));
        b.learned(5, 0, |_| Some(id(5)));
        assert_eq!([0, 1, 2].map(|o| b.rank(999, a(o), &m[..], &t)), [0, 1, 2]);
        assert_eq!([0, 1, 2].map(|o| b.rank(1000, a(o), &m[..], &t)), [2, 1, 0]);
    }

    /// A `Baseline` whose commit became known after a block's apply
    /// deadline ranks the replica as demoted for that block, and not for
    /// the blocks whose deadline came after; one known only from a
    /// listing ranks it demoted until its log decides it.
    #[test]
    fn a_baseline_known_late_demotes_until_its_commit_was_known() {
        let t = Timing::defaults(B);
        let m = audio_voters(&members());
        let mut b = Baselines::default();
        let at = 1000 * B;
        b.listed(id(5), at);
        assert_eq!(
            b.rank(1000, a(0), &m[..], &t),
            3,
            "demoted, below every voter"
        );
        assert_eq!(b.rank(999, a(0), &m[..], &t), 0);
        b.appended(id(5), &baseline(a(1), &[], at));
        let known = t.apply_deadline(1010 * B);
        b.learned(5, known, |_| Some(id(5)));
        assert_eq!(b.rank(1009, a(1), &m[..], &t), 4);
        assert_eq!(b.rank(1010, a(1), &m[..], &t), 0);
        // Truncated under another id: no longer counted.
        let mut b = Baselines::default();
        b.listed(id(5), at);
        b.learned(5, 0, |_| Some(StoredLogId { term: 2, ..id(5) }));
        assert_eq!(b.rank(1000, a(0), &m[..], &t), 0);
    }

    /// Demoted replicas rank below every other, the diverged ones below
    /// the rest and old-key fillers between them; with seven audio voters
    /// the lowest fits the status's 5 bits of rank.
    #[test]
    fn demoted_classes_rank_below_the_rest_and_fit_five_bits() {
        let seven: Vec<NodeId> = (0..7).map(a).collect();
        let r = |o, c| demoted(a(o), &seven, c);
        assert_eq!(r(0, Class::Demoted), 7);
        assert!(r(6, Class::Demoted) < r(0, Class::Filler));
        assert!(r(6, Class::Filler) < r(0, Class::Diverged));
        assert_eq!(r(6, Class::Diverged), 27);
        assert!(r(6, Class::Diverged) <= MAX_RANK);
        let three: Vec<NodeId> = (0..3).map(a).collect();
        assert!(demoted(a(2), &three, Class::Demoted) < demoted(a(0), &three, Class::Diverged));
    }

    /// One change in `[lo, hi)` goes into the ranks; a second one is
    /// where F has to wait.
    #[test]
    fn ranks_carry_one_change_and_name_the_second() {
        let t = Timing::defaults(B);
        let m = audio_voters(&members());
        let mut b = Baselines::default();
        b.appended(id(5), &baseline(a(2), &[], 1010 * B));
        b.appended(id(6), &baseline(a(1), &[], 1020 * B));
        b.learned(6, 0, |i| Some(id(i)));
        let rank_of = |k| b.rank(k, a(1), &m[..], &t);
        let fill = demoted(a(1), &m, Class::Demoted);
        let (r, cap) = schedule(1000, 1015, rank_of, fill);
        assert_eq!(
            r,
            Ranks {
                rank: 1,
                next: 2,
                from: 1010,
                fill: 4
            }
        );
        assert_eq!(cap, None);
        let (r, cap) = schedule(1000, 1030, rank_of, fill);
        assert_eq!((r.from, r.next, cap), (1010, 2, Some(1020)));
        let (r, _) = schedule(1020, 1030, rank_of, fill);
        assert_eq!((r.rank, r.from), (0, 1030));
        b.prune(1020, &t);
        assert_eq!(b.known.len(), 1);
        assert_eq!(b.rank(1020, a(1), &m[..], &t), 0);
    }
}
