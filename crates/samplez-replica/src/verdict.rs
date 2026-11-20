//! The leader's page verdicts (plan 2.11, Withholding and divergence):
//! per page of each output, in order, (1) a replica whose state
//! contradicts the log's decision on an entry is diverged; (2) one that
//! self-reported a degradation for the page and did not commit it is left
//! out, one that committed it through is diverged only when another
//! committed the page with another digest; (3) among the rest a strict
//! majority hash wins and the minority is diverged, and without one the
//! baseline's hash wins and `replica_disagreement` alarms; (4) a diverged
//! baseline is renamed. (3) compares only replicas whose entries agree
//! with the baseline's: equal entry fingerprints. Each rule compares only
//! replicas that report equal compatibility keys ([`judge_by_key`]);
//! a page committed whole by replicas of two keys is a disagreement, and
//! those whose key is not the baseline's are diverged.

use std::collections::{BTreeMap, BTreeSet};

use crate::entry::NodeId;
use crate::report::Page;

/// What the leader decided on one page.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Judged {
    pub diverged: BTreeSet<NodeId>,
    /// No strict majority among two or more hashes of one key: the
    /// baseline's won.
    pub disagreement: bool,
    /// The page was committed whole by replicas of two keys.
    pub cross_key: bool,
}

/// Judges one page from the records of the replicas that reported it,
/// each with the compatibility key it reports: replicas of each key apart
/// ([`judge`]). Pages of another key differ without either replica's state
/// diverging, so a replica of a key the baseline does not have is judged
/// among those of its own, never against the baseline's majority. A page
/// committed whole under two keys is an output carrying both, which no
/// hand-over leaves: a disagreement, and the replicas that committed it
/// under another key than `base_key`, the baseline's, are diverged.
pub fn judge_by_key(
    records: &[(NodeId, Option<u64>, Page)],
    baseline: Option<NodeId>,
    base_key: Option<u64>,
    contradicting: &BTreeSet<NodeId>,
) -> Judged {
    let keys: BTreeSet<Option<u64>> = records.iter().map(|r| r.1).collect();
    let mut out = Judged::default();
    let committed: BTreeSet<Option<u64>> = (records.iter())
        .filter(|r| r.2.committed)
        .map(|r| r.1)
        .collect();
    if committed.len() > 1 {
        out.cross_key = true;
        out.diverged.extend(
            (records.iter())
                .filter(|r| r.2.committed && base_key.is_some() && r.1 != base_key)
                .map(|r| r.0),
        );
    }
    for key in keys {
        let group: Vec<(NodeId, Page)> = records
            .iter()
            .filter(|r| r.1 == key)
            .map(|r| (r.0, r.2))
            .collect();
        let j = judge(&group, baseline, contradicting);
        out.diverged.extend(j.diverged);
        out.disagreement |= j.disagreement;
    }
    out
}

/// Judges one page from the records of the replicas that reported it.
/// `contradicting` are the replicas rule (1) diverges, which the log's
/// decisions on entries name; they are left out of the rest.
pub fn judge(
    records: &[(NodeId, Page)],
    baseline: Option<NodeId>,
    contradicting: &BTreeSet<NodeId>,
) -> Judged {
    let mut out = Judged {
        diverged: contradicting
            .iter()
            .copied()
            .filter(|id| records.iter().any(|(r, _)| r == id))
            .collect(),
        disagreement: false,
        cross_key: false,
    };
    let records: Vec<&(NodeId, Page)> = records
        .iter()
        .filter(|(id, _)| !contradicting.contains(id))
        .collect();
    // (2)
    for (id, p) in &records {
        if p.degraded
            && p.committed
            && records
                .iter()
                .any(|(x, q)| x != id && q.committed && q.digest != p.digest)
        {
            out.diverged.insert(*id);
        }
    }
    let fingerprint = records
        .iter()
        .find(|(id, p)| Some(*id) == baseline && !p.degraded)
        .map(|(_, p)| p.fingerprint);
    let rest: Vec<&(NodeId, Page)> = records
        .into_iter()
        .filter(|(_, p)| !p.degraded && fingerprint.is_none_or(|f| p.fingerprint == f))
        .collect();
    // (3)
    let mut votes: BTreeMap<u64, usize> = BTreeMap::new();
    for (_, p) in &rest {
        *votes.entry(p.hash).or_default() += 1;
    }
    if votes.len() < 2 {
        return out;
    }
    let majority = votes
        .iter()
        .find(|&(_, &n)| 2 * n > rest.len())
        .map(|(&h, _)| h);
    let winner = match majority {
        Some(h) => Some(h),
        None => {
            out.disagreement = true;
            rest.iter()
                .find(|(id, _)| Some(*id) == baseline)
                .map(|(_, p)| p.hash)
        }
    };
    if let Some(w) = winner {
        out.diverged
            .extend(rest.iter().filter(|(_, p)| p.hash != w).map(|(id, _)| *id));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(hash: u64) -> Page {
        Page {
            output: 0,
            page: 7,
            hash,
            fingerprint: 1,
            committed: true,
            degraded: false,
            digest: Some(0),
        }
    }

    fn none() -> BTreeSet<NodeId> {
        BTreeSet::new()
    }

    /// A strict majority hash wins over the baseline's; without one the
    /// baseline's wins and the disagreement alarms.
    #[test]
    fn a_strict_majority_wins_else_the_baseline() {
        let r = [(1, page(5)), (2, page(5)), (3, page(6))];
        let j = judge(&r, Some(3), &none());
        assert_eq!(j.diverged, [3].into());
        assert!(!j.disagreement);
        let r = [(1, page(4)), (2, page(5)), (3, page(6))];
        let j = judge(&r, Some(2), &none());
        assert_eq!(j.diverged, [1, 3].into());
        assert!(j.disagreement);
        let r = [(1, page(5)), (2, page(6))];
        let j = judge(&r, Some(1), &none());
        assert_eq!(j.diverged, [2].into());
        assert!(j.disagreement, "two audio replicas disagree");
        assert_eq!(
            judge(&[(1, page(5)), (2, page(5))], Some(1), &none()),
            Judged::default()
        );
    }

    /// A self-reported degradation not committed is left out; committed
    /// through, it diverges only beside another committed digest.
    #[test]
    fn a_degraded_page_is_left_out_unless_committed_beside_another_digest() {
        let degraded = |committed, digest| Page {
            hash: 9,
            degraded: true,
            committed,
            digest: Some(digest),
            ..page(9)
        };
        let r = [(1, page(5)), (2, page(5)), (3, degraded(false, 3))];
        assert_eq!(judge(&r, Some(1), &none()), Judged::default());
        let r = [(1, degraded(true, 3)), (2, degraded(true, 3))];
        assert_eq!(judge(&r, Some(1), &none()), Judged::default());
        let r = [(1, page(5)), (2, degraded(true, 3))];
        assert_eq!(judge(&r, Some(1), &none()).diverged, [2].into());
    }

    /// Replicas of another key than the baseline's are judged among their
    /// own key only: a lone new-key replica is never diverged by the
    /// old-key majority, two of one key are compared with each other, and
    /// the old-key majority still diverges its own minority.
    #[test]
    fn hashes_compare_only_between_equal_keys() {
        let (old, new) = (Some(1), Some(2));
        let held = |hash| Page {
            committed: false,
            ..page(hash)
        };
        let r = [
            (1, old, page(5)),
            (2, old, page(5)),
            (3, old, page(5)),
            (4, new, held(9)),
        ];
        assert_eq!(judge_by_key(&r, Some(1), old, &none()), Judged::default());
        let r = [
            (1, old, page(5)),
            (2, old, page(5)),
            (3, old, held(6)),
            (4, new, held(9)),
            (5, new, held(9)),
            (6, new, held(8)),
        ];
        let j = judge_by_key(&r, Some(1), old, &none());
        assert_eq!(j.diverged, [3, 6].into());
        assert!(!j.disagreement);
    }

    /// A page committed whole by replicas of two keys is an output that
    /// carries both: a disagreement, and the replica of the key that is
    /// not the baseline's is diverged, however equal the hashes.
    #[test]
    fn a_page_committed_under_two_keys_diverges_the_other_key() {
        let (old, new) = (Some(1), Some(2));
        let r = [(1, old, page(5)), (2, old, page(5)), (3, new, page(5))];
        let j = judge_by_key(&r, Some(1), old, &none());
        assert_eq!(j.diverged, [3].into());
        assert!(j.cross_key && !j.disagreement, "{j:?}");
        let j = judge_by_key(&r, Some(3), new, &none());
        assert_eq!(j.diverged, [1, 2].into());
    }

    /// Only replicas whose entry fingerprint equals the baseline's are
    /// compared; the replicas (1) names are diverged and left out.
    #[test]
    fn hashes_compare_only_under_the_baselines_fingerprint() {
        let other = Page {
            fingerprint: 2,
            ..page(6)
        };
        let r = [(1, page(5)), (2, other), (3, other)];
        assert_eq!(judge(&r, Some(1), &none()), Judged::default());
        let r = [(1, page(5)), (2, page(6)), (3, page(5))];
        let j = judge(&r, Some(1), &[3].into());
        assert_eq!(j.diverged, [2, 3].into());
    }
}
