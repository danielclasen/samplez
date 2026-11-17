//! Clock verification: a replica is clock-unverified from its process
//! start until one exchange shows its kernel TAI offset equal to the
//! set's offset and its clock within the guard of a majority of the
//! reference voters. Its own self-report does not enter.
//!
//! While E counts a replica, the set's offset is the offset of the
//! replicas E counts, none while those differ, and the reference voters
//! are those replicas. While E counts nobody, it is the offset a majority
//! of all voters reports, provided no voter heard since this process
//! started reports another, and the reference voters are all voters, the
//! witness and this one included. A voter not heard counts against both
//! majorities, a voter heard keeps its latest report however long it is
//! silent, and this replica counts itself as heard.
//!
//! Verification holds until the offset changes across a detected clock
//! step; an offset that moves without one, as a leap second the kernel
//! applies, ends nothing.

use std::collections::{BTreeMap, BTreeSet};

use crate::contact::Members;
use crate::entry::NodeId;

/// One voter as verification reads it, by its latest report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Voter {
    /// Its kernel TAI offset, whole seconds; none before it reported one.
    pub offset_s: Option<i64>,
    /// Its clock minus this replica's from the lowest-round-trip exchange,
    /// ns, and that exchange's one-way delay; 0 and 0 for this replica.
    pub theta_ns: Option<i64>,
    pub delay_ns: u64,
}

/// What one replica knows when it verifies.
#[derive(Debug, Clone)]
pub struct View<'a> {
    pub me: NodeId,
    pub members: &'a Members,
    /// Every voter heard since this process started, this one included.
    pub heard: &'a BTreeMap<NodeId, Voter>,
    /// The audio replicas E counts.
    pub counted: &'a BTreeSet<NodeId>,
    /// clockGuard, ns.
    pub guard_ns: u64,
}

impl View<'_> {
    fn offset_of(&self, id: NodeId) -> Option<i64> {
        self.heard.get(&id).and_then(|v| v.offset_s)
    }

    /// The set's offset; none while the replicas E counts differ or, with
    /// E counting nobody, no offset has a majority of all voters or a
    /// voter heard reports another.
    pub fn set_offset(&self) -> Option<i64> {
        if !self.counted.is_empty() {
            let mut offsets = self.counted.iter().map(|&id| self.offset_of(id));
            let first = offsets.next().flatten()?;
            return offsets.all(|o| o == Some(first)).then_some(first);
        }
        let reported: BTreeSet<i64> = self
            .members
            .voters()
            .into_iter()
            .filter_map(|id| self.offset_of(id))
            .collect();
        let [only] = reported.iter().copied().collect::<Vec<_>>()[..] else {
            return None;
        };
        self.members
            .majority(|id| self.offset_of(id) == Some(only))
            .then_some(only)
    }

    /// Whether this replica's clock lies within the guard of `id`'s: the
    /// exchange's offset within the guard plus its one-way delay, as the
    /// clock check reads it.
    fn agrees(&self, id: NodeId) -> bool {
        if id == self.me {
            return true;
        }
        self.heard
            .get(&id)
            .and_then(|v| {
                v.theta_ns
                    .map(|t| t.unsigned_abs() <= self.guard_ns + v.delay_ns)
            })
            .unwrap_or(false)
    }

    /// Whether one exchange verifies this replica now.
    pub fn verifies(&self) -> bool {
        let Some(set) = self.set_offset() else {
            return false;
        };
        if self.offset_of(self.me) != Some(set) {
            return false;
        }
        if self.counted.is_empty() {
            self.members.majority(|id| self.agrees(id))
        } else {
            let agreeing = self.counted.iter().filter(|&&id| self.agrees(id)).count();
            agreeing > self.counted.len() / 2
        }
    }

    /// The voters `replica_clock_unverified` names beside the unverified
    /// replicas: those whose offset is not the set's (every voter while
    /// there is none, a voter not heard then named without an offset).
    pub fn off_offset(&self) -> Vec<(NodeId, Option<i64>)> {
        let set = self.set_offset();
        self.members
            .voters()
            .into_iter()
            .map(|id| (id, self.offset_of(id)))
            .filter(|&(_, o)| set.is_none() || o.is_some_and(|o| Some(o) != set))
            .collect()
    }
}

/// One replica's verification across report periods.
#[derive(Debug, Default)]
pub struct Verifier {
    verified: bool,
    /// The offset and the detected-step count of the last period.
    last: Option<(Option<i64>, u64)>,
}

impl Verifier {
    pub fn verified(&self) -> bool {
        self.verified
    }

    /// One period: `offset` and `steps` as sampled now, `verifies` the
    /// exchange's verdict. A verified replica whose offset changed across
    /// a step detected since the last period is unverified, and verifies
    /// again no earlier than the next period, so whatever follows it sees
    /// it unverified once.
    pub fn period(&mut self, offset: Option<i64>, steps: u64, verifies: impl FnOnce() -> bool) {
        let lost = self.verified
            && self
                .last
                .is_some_and(|(before, s)| steps > s && offset != before);
        if lost {
            self.verified = false;
        } else if !self.verified {
            self.verified = verifies();
        }
        self.last = Some((offset, steps));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{Slot, node_id};

    fn a(o: u16) -> NodeId {
        node_id(Slot::Audio(o), 1 + u64::from(o))
    }
    const W: NodeId = 1 << 40;

    const GUARD: u64 = 6_000_000;

    fn members(ids: &[NodeId]) -> Members {
        Members {
            configs: vec![ids.iter().copied().collect()],
            addrs: BTreeMap::new(),
        }
    }

    fn at(offset: i64) -> Voter {
        Voter {
            offset_s: Some(offset),
            theta_ns: Some(0),
            delay_ns: 0,
        }
    }

    fn heard(v: &[(NodeId, Voter)]) -> BTreeMap<NodeId, Voter> {
        v.iter().copied().collect()
    }

    /// Whether each of `ids` verifies, each seeing the others as `voters`
    /// says and E counting nobody.
    fn cold(ids: &[NodeId], voters: &[(NodeId, Voter)]) -> Vec<bool> {
        let m = members(ids);
        let h = heard(voters);
        let none = BTreeSet::new();
        ids.iter()
            .map(|&me| {
                View {
                    me,
                    members: &m,
                    heard: &h,
                    counted: &none,
                    guard_ns: GUARD,
                }
                .verifies()
            })
            .collect()
    }

    fn set_of(ids: &[NodeId], voters: &[(NodeId, Voter)]) -> Option<i64> {
        let m = members(ids);
        let h = heard(voters);
        let none = BTreeSet::new();
        View {
            me: ids[0],
            members: &m,
            heard: &h,
            counted: &none,
            guard_ns: GUARD,
        }
        .set_offset()
    }

    #[test]
    fn a_uniform_cold_start_verifies_every_voter() {
        for offset in [37, 0] {
            for ids in [vec![a(0), a(1), W], vec![a(0), a(1), a(2)]] {
                let voters: Vec<_> = ids.iter().map(|&id| (id, at(offset))).collect();
                assert!(cold(&ids, &voters).iter().all(|&v| v), "{offset} {ids:?}");
            }
        }
    }

    #[test]
    fn a_voter_heard_on_another_offset_blocks_a_cold_start() {
        // The witness and one audio node at 0, one audio node at 0, the
        // witness alone at 0, and one node at 36: a voter heard on another
        // offset leaves the set without one.
        let two_w = [a(0), a(1), W];
        let three = [a(0), a(1), a(2)];
        let cases: Vec<(&[NodeId], Vec<i64>)> = vec![
            (&two_w, vec![37, 0, 0]),
            (&two_w, vec![0, 37, 37]),
            (&three, vec![0, 37, 37]),
            (&two_w, vec![37, 37, 0]),
            (&two_w, vec![37, 36, 37]),
            (&three, vec![37, 37, 36]),
            // A stale leap-second offset is a mismatch like 0.
            (&three, vec![36, 37, 37]),
        ];
        for (ids, offsets) in cases {
            let voters: Vec<_> = ids
                .iter()
                .zip(&offsets)
                .map(|(&id, &o)| (id, at(o)))
                .collect();
            assert_eq!(set_of(ids, &voters), None, "{offsets:?}");
            assert!(cold(ids, &voters).iter().all(|&v| !v), "{offsets:?}");
            // Every voter is named, with its offset.
            let m = members(ids);
            let h = heard(&voters);
            let none = BTreeSet::new();
            let named = View {
                me: ids[0],
                members: &m,
                heard: &h,
                counted: &none,
                guard_ns: GUARD,
            }
            .off_offset();
            assert_eq!(named.len(), ids.len());
        }
    }

    #[test]
    fn a_majority_verifies_while_a_voter_on_another_offset_is_not_heard() {
        // Both audio nodes at 0, the witness at 37 not yet heard: they are
        // a majority with nobody heard disagreeing.
        let ids = [a(0), a(1), W];
        let voters = [(a(0), at(0)), (a(1), at(0))];
        assert_eq!(set_of(&ids, &voters), Some(0));
        assert_eq!(cold(&ids, &voters)[..2], [true, true]);
        // Once heard, it blocks the next cold start.
        let voters = [(a(0), at(0)), (a(1), at(0)), (W, at(37))];
        assert_eq!(set_of(&ids, &voters), None);
    }

    #[test]
    fn a_voter_not_heard_counts_against_both_majorities() {
        // One of three heard: no majority reports an offset.
        let ids = [a(0), a(1), W];
        assert_eq!(set_of(&ids, &[(a(0), at(37))]), None);
        // A voter heard without an exchange does not agree with the clock.
        let mut silent = at(37);
        silent.theta_ns = None;
        let voters = [(a(0), at(37)), (a(1), silent)];
        assert!(!cold(&ids, &voters)[0]);
    }

    #[test]
    fn one_voter_is_its_own_majority() {
        assert_eq!(cold(&[a(0)], &[(a(0), at(37))]), [true]);
        assert_eq!(cold(&[a(0)], &[(a(0), at(0))]), [true]);
        // Before its first offset sample it does not verify.
        let none = Voter {
            offset_s: None,
            theta_ns: Some(0),
            delay_ns: 0,
        };
        assert_eq!(cold(&[a(0)], &[(a(0), none)]), [false]);
    }

    /// `me` against counted replicas at 37 and clock 0.
    fn joining(me: Voter, counted: &[NodeId]) -> bool {
        let ids = [a(0), a(1), a(2)];
        let m = members(&ids);
        let mut voters: Vec<_> = counted.iter().map(|&id| (id, at(37))).collect();
        voters.push((a(2), me));
        let h = heard(&voters);
        let c: BTreeSet<_> = counted.iter().copied().collect();
        View {
            me: a(2),
            members: &m,
            heard: &h,
            counted: &c,
            guard_ns: GUARD,
        }
        .verifies()
    }

    #[test]
    fn while_e_counts_replicas_only_their_offset_verifies() {
        // A replica restarted onto 0 among replicas at 37 never verifies,
        // however many voters share its offset.
        assert!(!joining(at(0), &[a(0), a(1)]));
        assert!(joining(at(37), &[a(0), a(1)]));
        assert!(joining(at(37), &[a(0)]));
    }

    #[test]
    fn a_clock_off_beyond_the_guard_at_the_right_offset_does_not_verify() {
        let ids = [a(0), a(1), a(2)];
        // As this replica reads the others: 10 ms ahead of it.
        for (theta, delay, verifies) in [
            (10_000_000, 0, false),
            (GUARD as i64, 0, true),
            // A one-way delay explains up to its own length.
            (10_000_000, 5_000_000, true),
        ] {
            let other = Voter {
                offset_s: Some(37),
                theta_ns: Some(theta),
                delay_ns: delay,
            };
            let m = members(&ids);
            let h = heard(&[(a(0), other), (a(1), other), (a(2), at(37))]);
            let c: BTreeSet<_> = [a(0), a(1)].into();
            let v = View {
                me: a(2),
                members: &m,
                heard: &h,
                counted: &c,
                guard_ns: GUARD,
            };
            assert_eq!(v.verifies(), verifies, "theta {theta} delay {delay}");
        }
    }

    #[test]
    fn two_clocks_apart_with_the_witness_agreeing_with_neither_verify_neither() {
        let ids = [a(0), a(1), W];
        let off = |t: i64| Voter {
            offset_s: Some(37),
            theta_ns: Some(t),
            delay_ns: 0,
        };
        let m = members(&ids);
        let none = BTreeSet::new();
        // From a(0): a(1) 20 s ahead, the witness 10 s ahead.
        for w in [Some(off(10_000_000_000)), None] {
            let mut voters = vec![(a(0), at(37)), (a(1), off(20_000_000_000))];
            voters.extend(w.map(|w| (W, w)));
            let h = heard(&voters);
            let v = View {
                me: a(0),
                members: &m,
                heard: &h,
                counted: &none,
                guard_ns: GUARD,
            };
            assert!(!v.verifies(), "witness {w:?}");
        }
    }

    #[test]
    fn counted_replicas_on_different_offsets_leave_no_set_offset() {
        let ids = [a(0), a(1), a(2)];
        let m = members(&ids);
        let h = heard(&[(a(0), at(37)), (a(1), at(38)), (a(2), at(37))]);
        let c: BTreeSet<_> = [a(0), a(1)].into();
        let v = View {
            me: a(2),
            members: &m,
            heard: &h,
            counted: &c,
            guard_ns: GUARD,
        };
        assert_eq!(v.set_offset(), None);
        assert!(!v.verifies());
        assert_eq!(v.off_offset().len(), 3);
    }

    #[test]
    fn verification_ends_only_on_an_offset_change_across_a_detected_step() {
        let mut v = Verifier::default();
        v.period(Some(37), 0, || true);
        assert!(v.verified());
        // A leap second: the offset moves, no step.
        v.period(Some(38), 0, || false);
        assert!(v.verified());
        // A step without an offset change.
        v.period(Some(38), 1, || false);
        assert!(v.verified());
        // A step with one: unverified for at least this period.
        v.period(Some(0), 2, || true);
        assert!(!v.verified());
        v.period(Some(0), 2, || true);
        assert!(v.verified());
    }

    #[test]
    fn an_unverified_replica_waits_for_an_exchange() {
        let mut v = Verifier::default();
        v.period(Some(37), 0, || false);
        assert!(!v.verified());
        v.period(Some(37), 0, || true);
        assert!(v.verified());
    }
}
