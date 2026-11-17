//! The leader's decisions on the baseline (plan 2.11, Baseline): the
//! no-committer rule with its candidates and start bound, the renames of
//! a baseline that stops committing or is about to, and the page
//! verdicts ([`crate::verdict`]). Every one goes into the log; none is
//! taken while the leader is out of contact, and a new leader decides the
//! same from the applied log and the reports.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use samplez::blocks::{Kind, PAGE, Standing};
use tokio::time::Instant;

use crate::contact::{Timing, index_at};
use crate::entry::{Baseline, Body, NodeId};
use crate::rank::{Class, demoted};
use crate::report::Own;
use crate::state::{PAGES_KEPT, PageBook, ReplicaState, filling, withheld_from};
use crate::verdict::judge;

/// How long the leader waits for a decision it appended to show in the
/// log before it decides again.
const INFLIGHT: Duration = Duration::from_secs(1);

/// An audio voter as the leader sees it.
pub(crate) struct Voter<'a> {
    pub id: NodeId,
    /// Nothing from it, direct or relayed, within `w + D`.
    pub silent: bool,
    pub own: Option<&'a Own>,
}

/// What the leader reads in one report period.
pub(crate) struct LeaderView<'a> {
    /// The replicas whose state contradicts the log's decision on an
    /// entry: page verdict rule (1).
    pub contradicting: &'a BTreeSet<NodeId>,
    /// The audio voters, by ordinal.
    pub audio: Vec<Voter<'a>>,
    pub judged: &'a BTreeSet<NodeId>,
    /// The voter holding the disruption slot.
    pub disrupting: Option<NodeId>,
    /// The latest `Baseline` applied.
    pub baseline: Option<&'a Baseline>,
    /// The audio replicas E counts.
    pub counted: &'a BTreeSet<NodeId>,
    pub pages: &'a PageBook,
    pub timing: &'a Timing,
    /// L, in samples (Withholding, floors).
    pub floor: u64,
    /// Rank now under the latest `Baseline`, 0 the top.
    pub rank: &'a dyn Fn(NodeId) -> u8,
    /// The audio StatefulSet's target revision ([`crate::rollout`]).
    pub target: Option<&'a str>,
}

/// The leader's decisions, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// `Baseline` entries appended under the no-committer rule, or
    /// naming a committing replica while none is named.
    pub named: u64,
    /// Renames of a baseline that stopped committing, was diverged or
    /// left the audio voters.
    pub renamed: u64,
    /// `Diverged` verdicts appended.
    pub verdicts: u64,
    /// Pages without a strict majority hash among two or more.
    pub disagreements: u64,
    /// `Baseline` entries naming a target-revision replica with another
    /// key than the baseline's: key hand-overs.
    pub handed_over: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Leader {
    /// A `Baseline` appended, with the one applied then, until the log
    /// shows another or [`INFLIGHT`] passes.
    inflight: Option<(Instant, Option<Baseline>)>,
    /// `Diverged` verdicts appended, per replica, until it reports itself
    /// diverged or [`INFLIGHT`] passes.
    verdicts: BTreeMap<NodeId, Instant>,
    /// Pages judged, per output.
    judged: BTreeMap<u32, BTreeSet<u64>>,
    /// The baseline a page verdict diverged, with the `Baseline` applied
    /// then: void once another is applied.
    against: Option<(NodeId, Option<Baseline>)>,
    pub counts: Counts,
    /// `replica_disagreement`: the last page judged had no strict majority.
    pub disagreement: bool,
}

fn state(v: &Voter) -> Option<ReplicaState> {
    v.own.and_then(|o| o.state)
}

fn committing(v: &Voter) -> bool {
    !v.silent && state(v) == Some(ReplicaState::Committing)
}

/// Whether `own` commits a block at or after P; a fill (no digest),
/// committed through while withdrawn, is no commit start.
fn commits_from(own: &Own, p: u64, block: u64) -> bool {
    own.state == Some(ReplicaState::Committing)
        || own.statuses.iter().flatten().any(|run| {
            run.status.kind == Kind::Committed
                && run.status.digest.is_some()
                && (run.status.k + u64::from(run.count)) * block > p
        })
}

impl Leader {
    /// The entries the leader appends this period.
    pub fn decide(&mut self, v: &LeaderView, now: Instant) -> Vec<Body> {
        let mut out = self.verdicts(v, now);
        if let Some((at, sent)) = &self.inflight
            && sent.as_ref() == v.baseline
            && now.duration_since(*at) < INFLIGHT
        {
            return out;
        }
        self.inflight = None;
        if let Some(b) = self.baseline(v) {
            self.inflight = Some((now, v.baseline.cloned()));
            out.push(Body::Baseline(b));
        }
        out
    }

    /// Leadership is lost: a verdict taken while leading does not rename
    /// a baseline in a later term.
    pub fn step_down(&mut self) {
        self.against = None;
    }

    /// The `Baseline` to append, if any.
    fn baseline(&mut self, v: &LeaderView) -> Option<Baseline> {
        if self
            .against
            .as_ref()
            .is_some_and(|(_, under)| under.as_ref() != v.baseline)
        {
            self.against = None;
        }
        let anyone = v.audio.iter().any(committing);
        if anyone && let Some(b) = self.hand_over(v) {
            self.counts.handed_over += 1;
            return Some(b);
        }
        let Some(current) = v.baseline else {
            if !anyone {
                return self.no_committer(v);
            }
            // One voter commits without waiting for its entry: a set grown
            // from it names the top committing replica.
            let b = self.rename(v, None)?;
            self.counts.named += 1;
            return Some(b);
        };
        let named = v.audio.iter().find(|a| a.id == current.replica);
        if !anyone {
            if named.is_some_and(|n| self.about_to_commit(v, n, current)) {
                return None;
            }
            return self.no_committer(v);
        }
        // Left the audio voters: it commits no more.
        let Some(n) = named else {
            let b = self.rename(v, None)?;
            self.counts.renamed += 1;
            return Some(b);
        };
        let renamed = if self.against.as_ref().is_some_and(|(id, _)| *id == n.id) {
            true
        } else if committing(n) {
            // About to stop: another committed through on its silence.
            v.audio.iter().any(|a| {
                a.id != n.id
                    && a.own
                        .is_some_and(|o| names_silent(o, n.id, current, v.timing))
            })
        } else {
            n.silent || !self.about_to_commit(v, n, current)
        };
        if !renamed {
            return None;
        }
        let b = self.rename(v, Some(n.id))?;
        self.counts.renamed += 1;
        Some(b)
    }

    /// The key hand-over: in a rollout that changes the key, a `Baseline`
    /// naming the top target-revision replica with the target's key once
    /// it is a candidate, has applied the latest `Baseline` and reports
    /// no block withheld for a cause. A replica passed over, or one that
    /// never learned of its naming, is named again only then.
    fn hand_over(&mut self, v: &LeaderView) -> Option<Baseline> {
        let target = v.target?;
        let current = v.baseline?;
        let at_target = |a: &&Voter| {
            a.own
                .is_some_and(|o| o.key.is_some() && o.revision.as_deref() == Some(target))
        };
        let key = v.audio.iter().filter(at_target).find_map(|a| a.own?.key)?;
        // A `Baseline` named before any key was reported carries none:
        // the named replica's report tells it.
        let base_key = current.key.or_else(|| {
            v.audio
                .iter()
                .find(|a| a.id == current.replica)
                .and_then(|a| a.own?.key)
        });
        if base_key.is_none_or(|k| k == key) {
            return None;
        }
        let n = v
            .audio
            .iter()
            .filter(at_target)
            .filter(|a| {
                let own = a.own.expect("reported");
                own.key == Some(key)
                    && candidate(v, a)
                    && own.baseline == Some((current.replica, current.at_index))
                    // One that computes nothing reports every block
                    // withheld.
                    && (own.state == Some(ReplicaState::VersionMismatch) || !withheld_from(own, 0))
            })
            .min_by_key(|a| ((v.rank)(a.id), a.id))?;
        self.against = None;
        Some(Baseline {
            replica: n.id,
            order: order(v, n.id),
            at_index: 0,
            key: Some(key),
        })
    }

    /// A `Baseline` naming the highest-ranked replica committing the
    /// latest blocks other than `from`.
    fn rename(&mut self, v: &LeaderView, from: Option<NodeId>) -> Option<Baseline> {
        let next = v
            .audio
            .iter()
            .filter(|a| Some(a.id) != from && committing(a))
            .min_by_key(|a| ((v.rank)(a.id), a.id))?;
        self.against = None;
        Some(Baseline {
            replica: next.id,
            order: order(v, next.id),
            at_index: 0,
            key: next.own.and_then(|o| o.key),
        })
    }

    /// A named replica that is about to commit: still a candidate, no
    /// block withheld for a cause at or after its P, within its start
    /// bound.
    fn about_to_commit(&self, v: &LeaderView, n: &Voter, b: &Baseline) -> bool {
        let p = b.at_index.next_multiple_of(PAGE);
        // Readiness does not move P: a named replica not ready at P holds
        // its degraded blocks as usual.
        candidate_unready(v, n)
            && !n.own.is_some_and(|o| withheld_from(o, p / v.timing.block))
            && !lapsed(v, n, p)
    }

    /// The no-committer rule: the top candidate.
    fn no_committer(&mut self, v: &LeaderView) -> Option<Baseline> {
        let top = v.audio.iter().filter(|a| candidate(v, a)).min_by_key(|a| {
            let own = a.own.expect("a candidate reported");
            let class = if own.state == Some(ReplicaState::Diverged) {
                Class::Diverged
            } else {
                Class::Demoted
            };
            let ids: Vec<NodeId> = v.audio.iter().map(|a| a.id).collect();
            (!filling(own), !own.ready, demoted(a.id, &ids, class))
        })?;
        self.counts.named += 1;
        Some(Baseline {
            replica: top.id,
            order: order(v, top.id),
            at_index: 0,
            key: top.own.and_then(|o| o.key),
        })
    }

    /// Page verdicts on the pages every replica that may report one has
    /// reported; at most one `Diverged` in flight per replica.
    fn verdicts(&mut self, v: &LeaderView, now: Instant) -> Vec<Body> {
        let mut out = Vec::new();
        self.verdicts
            .retain(|_, at| now.duration_since(*at) < INFLIGHT);
        let reporting: Vec<&Voter> = v
            .audio
            .iter()
            .filter(|a| {
                !a.silent
                    && matches!(
                        state(a),
                        Some(ReplicaState::Committing | ReplicaState::Withdrawn)
                    )
            })
            .collect();
        let base = v.baseline.map(|b| b.replica);
        for o in v.pages.outputs() {
            // Every page some voter still reports: a new leader judges
            // the pages of the election too.
            let pages = v.pages.pages(o);
            let newest = reporting
                .iter()
                .filter_map(|a| v.pages.newest(a.id, o))
                .max();
            let judged = self.judged.entry(o).or_default();
            judged.retain(|q| pages.contains(q));
            for q in &pages {
                if judged.contains(q) {
                    continue;
                }
                // Every replica that may report it did, or reported a
                // later page, or no longer hashes the output.
                let complete = reporting.iter().all(|a| {
                    v.pages.get(a.id, o, *q).is_some()
                        || v.pages.newest(a.id, o).is_some_and(|n| n > *q)
                        || v.pages.of(a.id, o).is_none()
                        || stopped(v, a, o, newest)
                });
                if !complete {
                    break;
                }
                let records: Vec<_> = reporting
                    .iter()
                    .filter_map(|a| v.pages.get(a.id, o, *q).map(|p| (a.id, *p)))
                    .collect();
                let j = judge(&records, base, v.contradicting);
                let reported = |id: NodeId| {
                    v.audio
                        .iter()
                        .any(|a| a.id == id && state(a) == Some(ReplicaState::Diverged))
                };
                // Judged again once a verdict in flight on the replica it
                // diverges clears: that one may be ignored as stale.
                if j.diverged
                    .iter()
                    .any(|id| self.verdicts.contains_key(id) && !reported(*id))
                {
                    continue;
                }
                judged.insert(*q);
                if j.disagreement {
                    self.counts.disagreements += 1;
                }
                self.disagreement = j.disagreement;
                for id in j.diverged {
                    // (4)
                    if Some(id) == base {
                        self.against = Some((id, v.baseline.cloned()));
                    }
                    if reported(id) {
                        continue;
                    }
                    self.verdicts.insert(id, now);
                    self.counts.verdicts += 1;
                    out.push(Body::Diverged {
                        replica: id,
                        page: *q,
                    });
                }
            }
        }
        out
    }
}

/// Whether `a` no longer hashes output `o`: it reports the output idle,
/// or its newest page of it lags the newest any replica reported by more
/// than the pages the book keeps.
fn stopped(v: &LeaderView, a: &Voter, o: u32, newest: Option<u64>) -> bool {
    let idle = a.own.is_some_and(|own| {
        matches!(
            own.standing.get(o as usize),
            None | Some(Standing::Idle { .. })
        )
    });
    let lags = v
        .pages
        .newest(a.id, o)
        .zip(newest)
        .is_some_and(|(n, m)| n + (PAGES_KEPT as u64) < m);
    idle || lags
}

/// Whether a committed status in `own` at or after the baseline's P names
/// `n` in `onSilenceOf`.
fn names_silent(own: &Own, n: NodeId, b: &Baseline, t: &Timing) -> bool {
    let Some(bit) = crate::rank::silence_bit(n) else {
        return false;
    };
    let kp = b.at_index.next_multiple_of(PAGE) / t.block;
    own.statuses.iter().flatten().any(|run| {
        run.status.kind == Kind::Committed
            && run.status.on_silence_of & bit != 0
            && run.status.k + u64::from(run.count) > kp
    })
}

/// Candidates: not silent, not excluded, clock-verified, holding no
/// disruption slot, ready or filling a stop; one passed over under the
/// latest `Baseline` naming it (its start bound lapsed, or it withheld a
/// block for a cause at or after its P) only once its reports show that
/// `Baseline` applied and a clean page since.
fn candidate(v: &LeaderView, a: &Voter) -> bool {
    a.own.is_some_and(|own| own.ready || filling(own)) && candidate_unready(v, a)
}

/// [`candidate`] whether ready or not.
fn candidate_unready(v: &LeaderView, a: &Voter) -> bool {
    let Some(own) = a.own else { return false };
    if a.silent
        || v.judged.contains(&a.id)
        || own.clock.unverified
        || v.disrupting == Some(a.id)
        || own.state == Some(ReplicaState::Starting)
    {
        return false;
    }
    let Some(b) = v.baseline.filter(|b| b.replica == a.id) else {
        return true;
    };
    let p = b.at_index.next_multiple_of(PAGE);
    let b_k = v.timing.block;
    let passed_over = lapsed(v, a, p) || withheld_from(own, p / b_k);
    if !passed_over {
        return true;
    }
    // The statuses a report carries span more than a page.
    let applied = own.baseline == Some((b.replica, b.at_index));
    let clean = !withheld_from(own, 0);
    applied && clean && own.processed.is_some_and(|k| (k + 1) * b_k >= p + PAGE)
}

/// The start bound of the replica `n` named from P: lapsed once an audio
/// replica E counts reported a processed index past P plus one page plus
/// `D + w` plus L while no report shows `n` committing at or after P.
fn lapsed(v: &LeaderView, n: &Voter, p: u64) -> bool {
    let t = v.timing;
    let bound = p + PAGE + index_at((t.hold + t.contact_window).as_nanos() as u64) + v.floor;
    let past = v.audio.iter().any(|a| {
        v.counted.contains(&a.id)
            && a.own
                .and_then(|o| o.processed)
                .is_some_and(|k| (k + 1) * t.block > bound)
    });
    past && !n.own.is_some_and(|o| commits_from(o, p, t.block))
}

/// The order of a `Baseline` naming `named`: the other committing
/// replicas that are not demoted, by the longest clean run (the earliest
/// commit start), then the lowest ordinal.
fn order(v: &LeaderView, named: NodeId) -> Vec<NodeId> {
    let mut rest: Vec<(u64, usize, NodeId)> = v
        .audio
        .iter()
        .enumerate()
        .filter(|(_, a)| a.id != named && committing(a))
        .map(|(ordinal, a)| {
            let from = a
                .own
                .into_iter()
                .flat_map(|o| &o.standing)
                .filter_map(|s| match s {
                    Standing::Committing { from } => Some(*from),
                    _ => None,
                })
                .min()
                .unwrap_or(u64::MAX);
            (from, ordinal, a.id)
        })
        .collect();
    rest.sort();
    rest.into_iter().map(|(_, _, id)| id).collect()
}

#[cfg(test)]
mod tests {
    use samplez::blocks::{BlockStatus, Cause, HEALTHY, Range};

    use super::*;
    use crate::membership::{Slot, node_id};
    use crate::report::runs;

    const B: u64 = 48;

    fn a(o: u16) -> NodeId {
        node_id(Slot::Audio(o), 10 + u64::from(o))
    }

    fn own(state: ReplicaState, processed: u64) -> Own {
        Own {
            processed: Some(processed),
            state: Some(state),
            ready: true,
            standing: vec![Standing::Withdrawn { since: 0 }],
            statuses: vec![Vec::new()],
            ..Own::default()
        }
    }

    fn status(k: u64, kind: Kind, cause: Cause, digest: Option<u64>) -> BlockStatus {
        BlockStatus {
            k,
            kind,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause,
            on_silence_of: 0,
            digest,
        }
    }

    struct Fixture {
        owns: Vec<(NodeId, bool, Own)>,
        baseline: Option<Baseline>,
        counted: BTreeSet<NodeId>,
        judged: BTreeSet<NodeId>,
        pages: PageBook,
        timing: Timing,
    }

    impl Fixture {
        fn new(owns: Vec<(NodeId, bool, Own)>) -> Fixture {
            Fixture {
                counted: owns.iter().map(|(id, _, _)| *id).collect(),
                owns,
                baseline: None,
                judged: BTreeSet::new(),
                pages: PageBook::default(),
                timing: Timing::defaults(B),
            }
        }

        fn decide(&self, leader: &mut Leader) -> Vec<Body> {
            let rank = |id: NodeId| self.owns.iter().position(|(x, _, _)| *x == id).unwrap() as u8;
            let none = BTreeSet::new();
            let v = LeaderView {
                contradicting: &none,
                audio: self
                    .owns
                    .iter()
                    .map(|(id, silent, own)| Voter {
                        id: *id,
                        silent: *silent,
                        own: Some(own),
                    })
                    .collect(),
                judged: &self.judged,
                disrupting: None,
                baseline: self.baseline.as_ref(),
                counted: &self.counted,
                pages: &self.pages,
                timing: &self.timing,
                floor: index_at(7_000_000),
                rank: &rank,
                target: None,
            };
            leader.decide(&v, Instant::now())
        }
    }

    fn named(bodies: &[Body]) -> Option<NodeId> {
        bodies.iter().find_map(|b| match b {
            Body::Baseline(b) => Some(b.replica),
            _ => None,
        })
    }

    /// With nobody committing the top candidate is named: one filling a
    /// stop first, then a ready one, then by the demoted rank, a diverged
    /// replica below the others; silent and excluded ones never.
    #[tokio::test(start_paused = true)]
    async fn the_no_committer_rule_names_the_top_candidate() {
        let k = 1_000_000;
        let mut fill = own(ReplicaState::Withdrawn, k);
        fill.statuses[0] = runs([Some(status(k, Kind::Committed, Cause::None, None))]);
        let unready = Own {
            ready: false,
            ..own(ReplicaState::Withdrawn, k)
        };
        let f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Diverged, k)),
            (a(1), false, unready.clone()),
            (a(2), false, own(ReplicaState::Withdrawn, k)),
        ]);
        assert_eq!(named(&f.decide(&mut Leader::default())), Some(a(2)));
        let f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Diverged, k)),
            (a(1), true, own(ReplicaState::Withdrawn, k)),
        ]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(0)),
            "a diverged one when no other is a candidate"
        );
        let mut f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, k)),
            (a(1), false, fill),
        ]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "filling first"
        );
        f.judged.insert(a(1));
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(0)),
            "excluded"
        );
    }

    /// A new leader names no other while the named replica is a candidate
    /// that withheld nothing at or after its P and is within its start
    /// bound; once the bound lapses with it not committing it names
    /// another, and the lapsed one is no candidate until it reports that
    /// `Baseline` applied and a clean page since.
    #[tokio::test(start_paused = true)]
    async fn the_start_bound_keeps_and_then_passes_over_the_named_replica() {
        let k = 1_000_000;
        let p = (k * B + 10 * B).next_multiple_of(PAGE);
        let b = Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: p - 1,
            key: None,
        };
        let mut f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, k)),
            (a(1), false, own(ReplicaState::Withdrawn, k)),
        ]);
        f.baseline = Some(b.clone());
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            None,
            "about to commit"
        );
        // Past P plus a page plus D + w plus L on a replica E counts.
        let bound = p + PAGE + index_at(18_000_000) + index_at(7_000_000);
        let past = bound / B + 1;
        f.owns[1].2.processed = Some(past);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "lapsed"
        );
        // Not yet applied by a0: no candidate even if it were the top.
        f.owns.swap(0, 1);
        f.owns[0].2.state = Some(ReplicaState::Diverged);
        assert_eq!(named(&f.decide(&mut Leader::default())), Some(a(1)));
        f.owns[0].2.state = Some(ReplicaState::Starting);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            None,
            "no candidate"
        );
        // Applied, a clean page since: a candidate again.
        let a0 = &mut f.owns[1].2;
        a0.baseline = Some((b.replica, b.at_index));
        a0.processed = Some(past);
        assert_eq!(named(&f.decide(&mut Leader::default())), Some(a(0)));
        // A block withheld for a cause at or after P passes it over too.
        let mut f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, p / B + 2)),
            (a(1), false, own(ReplicaState::Withdrawn, k)),
        ]);
        f.owns[0].2.statuses[0] =
            runs([Some(status(p / B + 1, Kind::Withheld, Cause::Skip, None))]);
        f.baseline = Some(b);
        assert_eq!(named(&f.decide(&mut Leader::default())), Some(a(1)));
    }

    /// The baseline is renamed when it stops committing while another
    /// commits (silent, withdrawn, or named in another's `onSilenceOf`),
    /// to the highest-ranked committing replica; one committing keeps it.
    #[tokio::test(start_paused = true)]
    async fn a_baseline_that_stops_committing_is_renamed() {
        let k = 1_000_000;
        let b = Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: 0,
            key: None,
        };
        let committing = || Own {
            standing: vec![Standing::Committing { from: 0 }],
            ..own(ReplicaState::Committing, k)
        };
        let mut f = Fixture::new(vec![
            (a(0), false, committing()),
            (a(1), false, committing()),
            (a(2), false, committing()),
        ]);
        f.baseline = Some(b);
        assert_eq!(named(&f.decide(&mut Leader::default())), None);
        f.owns[0].1 = true;
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "silent"
        );
        f.owns[0].1 = false;
        f.owns[0].2.state = Some(ReplicaState::Withdrawn);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "withdrawn"
        );
        // The order: the other committing replicas, the longest clean run
        // (the earliest commit start) first, then the lowest ordinal.
        let from = |from| Own {
            standing: vec![Standing::Committing { from }],
            ..committing()
        };
        let mut g = Fixture::new(vec![
            (a(0), true, from(50)),
            (a(1), false, from(200)),
            (a(2), false, from(300)),
            (a(3), false, from(100)),
            (a(4), false, from(300)),
        ]);
        g.baseline = f.baseline.clone();
        match &g.decide(&mut Leader::default())[..] {
            [Body::Baseline(b)] => {
                assert_eq!((b.replica, &b.order[..]), (a(1), &[a(3), a(2), a(4)][..]))
            }
            other => panic!("{other:?}"),
        }
        f.owns[0].2 = committing();
        f.owns[2].2.statuses[0] = runs([Some(BlockStatus {
            on_silence_of: crate::rank::silence_bit(a(0)).unwrap(),
            ..status(k, Kind::Committed, Cause::None, Some(HEALTHY))
        })]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "onSilenceOf"
        );
    }

    /// One `Baseline` in flight: the leader does not name again until the
    /// log shows it or a second passed.
    #[tokio::test(start_paused = true)]
    async fn one_baseline_is_in_flight_at_a_time() {
        let f = Fixture::new(vec![(a(0), false, own(ReplicaState::Withdrawn, 1_000_000))]);
        let mut leader = Leader::default();
        assert!(named(&f.decide(&mut leader)).is_some());
        assert!(named(&f.decide(&mut leader)).is_none());
        tokio::time::advance(INFLIGHT).await;
        assert!(named(&f.decide(&mut leader)).is_some());
    }

    fn committing_own(k: u64) -> Own {
        Own {
            standing: vec![Standing::Committing { from: 0 }],
            ..own(ReplicaState::Committing, k)
        }
    }

    fn record(q: u64, hash: u64) -> crate::report::Page {
        crate::report::Page {
            output: 0,
            page: q,
            hash,
            fingerprint: 1,
            committed: true,
            degraded: false,
            digest: Some(HEALTHY),
        }
    }

    /// A replica committing while no `Baseline` is applied (one voter
    /// commits without its entry) is named once the set grows.
    #[tokio::test(start_paused = true)]
    async fn a_committing_replica_is_named_while_none_is() {
        let k = 1_000_000;
        let f = Fixture::new(vec![
            (a(0), false, committing_own(k)),
            (a(1), false, own(ReplicaState::Withdrawn, k)),
            (a(2), false, own(ReplicaState::Withdrawn, k)),
        ]);
        let mut leader = Leader::default();
        assert_eq!(named(&f.decide(&mut leader)), Some(a(0)));
        assert_eq!(leader.counts.named, 1);
    }

    /// A baseline no longer among the audio voters is renamed to the top
    /// committing replica.
    #[tokio::test(start_paused = true)]
    async fn a_baseline_that_left_the_voters_is_renamed() {
        let k = 1_000_000;
        let mut f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, k)),
            (a(1), false, committing_own(k)),
        ]);
        f.baseline = Some(Baseline {
            replica: a(5),
            order: Vec::new(),
            at_index: 0,
            key: None,
        });
        let mut leader = Leader::default();
        assert_eq!(named(&f.decide(&mut leader)), Some(a(1)));
        assert_eq!(leader.counts.renamed, 1);
    }

    /// A replica that still reports but no longer hashes an output it
    /// reported pages of once does not hold the verdicts on that output
    /// back.
    #[tokio::test(start_paused = true)]
    async fn a_replica_idle_on_an_output_does_not_halt_its_verdicts() {
        let k = 1_000_000;
        let q = 900;
        let mut f = Fixture::new(vec![
            (a(0), false, committing_own(k)),
            (a(1), false, committing_own(k)),
            (a(2), false, own(ReplicaState::Withdrawn, k)),
        ]);
        f.owns[2].2.standing = vec![Standing::Idle { since: 0 }];
        f.pages.fold(a(2), &[record(q - 1, 1)]);
        for x in [a(0), a(1)] {
            f.pages.fold(x, &[record(q, 1), record(q + 1, 1)]);
        }
        let mut leader = Leader::default();
        f.decide(&mut leader);
        let judged = &leader.judged[&0];
        assert!(
            judged.contains(&q) && judged.contains(&(q + 1)),
            "{judged:?}"
        );
    }

    /// A page that diverges a replica with a verdict in flight is judged
    /// again once that verdict clears, which the replica may have ignored
    /// as stale.
    #[tokio::test(start_paused = true)]
    async fn a_page_behind_a_verdict_in_flight_is_judged_once_it_clears() {
        let k = 1_000_000;
        let q = 900;
        let mut f = Fixture::new(vec![
            (a(0), false, committing_own(k)),
            (a(1), false, committing_own(k)),
            (a(2), false, committing_own(k)),
        ]);
        for (x, h) in [(a(0), 1), (a(1), 2), (a(2), 1)] {
            f.pages.fold(x, &[record(q, h), record(q + 1, h)]);
        }
        let verdicts = |b: &[Body]| -> Vec<(NodeId, u64)> {
            b.iter()
                .filter_map(|b| match b {
                    Body::Diverged { replica, page } => Some((*replica, *page)),
                    _ => None,
                })
                .collect()
        };
        let mut leader = Leader::default();
        assert_eq!(verdicts(&f.decide(&mut leader)), [(a(1), q)]);
        assert!(verdicts(&f.decide(&mut leader)).is_empty());
        tokio::time::advance(INFLIGHT).await;
        assert_eq!(verdicts(&f.decide(&mut leader)), [(a(1), q + 1)]);
    }

    /// A page verdict against the baseline renames it only under the
    /// `Baseline` applied when it was taken, and only in that term.
    #[tokio::test(start_paused = true)]
    async fn a_verdict_against_the_baseline_lapses_with_its_term_and_baseline() {
        let k = 1_000_000;
        let q = 900;
        let b = Baseline {
            replica: a(1),
            order: Vec::new(),
            at_index: 0,
            key: None,
        };
        let mut f = Fixture::new(vec![
            (a(0), false, committing_own(k)),
            (a(1), false, committing_own(k)),
            (a(2), false, committing_own(k)),
        ]);
        f.baseline = Some(b.clone());
        for (x, h) in [(a(0), 1), (a(1), 2), (a(2), 1)] {
            f.pages.fold(x, &[record(q, h)]);
        }
        let mut leader = Leader::default();
        // The verdict is taken; the rename waits for nothing in flight.
        let out = f.decide(&mut leader);
        assert_eq!(named(&out), Some(a(0)), "{out:?}");
        // Taken again, then leadership lost: no rename in a later term.
        let mut leader = Leader::default();
        leader.judged.entry(0).or_default().insert(q);
        leader.against = Some((a(1), Some(b.clone())));
        leader.step_down();
        assert_eq!(named(&f.decide(&mut leader)), None);
        // Taken under another `Baseline`: void under this one.
        let mut leader = Leader::default();
        leader.judged.entry(0).or_default().insert(q);
        leader.against = Some((a(1), Some(Baseline { at_index: 1, ..b })));
        assert_eq!(named(&f.decide(&mut leader)), None);
    }

    /// A named replica that only fills a stop after P does not hold its
    /// start bound open: a fill is no commit start, so the bound lapses
    /// and the leader names again.
    #[tokio::test(start_paused = true)]
    async fn a_named_replica_filling_after_p_lets_its_start_bound_lapse() {
        let k = 1_000_000;
        let p = (k * B + 10 * B).next_multiple_of(PAGE);
        let b = Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: p - 1,
            key: None,
        };
        let bound = p + PAGE + index_at(18_000_000) + index_at(7_000_000);
        let past = bound / B + 1;
        let mut filler = own(ReplicaState::Withdrawn, past);
        filler.statuses[0] = runs([Some(BlockStatus {
            through: true,
            ..status(past - 5, Kind::Committed, Cause::None, None)
        })]);
        filler.baseline = Some((b.replica, b.at_index));
        let mut f = Fixture::new(vec![
            (a(0), false, filler),
            (a(1), false, own(ReplicaState::Withdrawn, past)),
        ]);
        f.baseline = Some(b);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(0)),
            "named again, filling first"
        );
    }
}
