//! The leader's decisions on unresolved entries (plan 2.11, Unresolved
//! entries). Audio replicas report the entries they hold unresolved; the
//! leader decides each one once the baseline committed the block holding
//! its `at_index`, or once every live audio replica withheld that block:
//! it stands when the baseline applied it there, and nothing is appended;
//! else a `Restamp` gives it a fresh `at_index`, or a `Void` resolves it
//! when it is an engine body a later committed entry supersedes, or once
//! three restamps were late. A product entry's restamp or void re-asserts
//! the state at its own place, so no later entry is undone. An id the
//! leader's log does not hold is voided once the leader committed an
//! entry of its own term, or draws a purge-point verdict at or below the
//! leader's purge point. Replicas that hold an entry unresolved where it
//! stands contradict the log's decision: page verdict rule (1) diverges
//! them.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use samplez::blocks::{Kind, Standing};
use tokio::time::Instant;

use crate::baseline::Voter;
use crate::contact::{Timing, UNLISTED_BIT};
use crate::entry::{Baseline, Body, NodeId, Request};
use crate::report::{Own, status_of};
use crate::snapshot::StoredLogId;

/// Restamps of one entry at most; a fourth late one voids it and the
/// writer's session gets `command_late`.
pub const RESTAMPS_MAX: u32 = 3;

/// How long a decision appended is not appended again while the log does
/// not show it.
const INFLIGHT: Duration = Duration::from_secs(1);

/// Decisions kept in flight at most; the oldest goes first.
const INFLIGHT_MAX: usize = 1024;

/// The leader's log as the decisions read it.
pub(crate) struct LeaderLog<'a> {
    pub log_id_at: &'a dyn Fn(u64) -> Option<StoredLogId>,
    /// The highest index the leader knows committed.
    pub committed: u64,
    /// The index of the leader's last purged log id, 0 for none.
    pub purged: u64,
    /// The leader committed an entry of its own term.
    pub own_term_committed: bool,
    /// The restamps behind the entry at an index.
    pub restamps: &'a dyn Fn(u64) -> u32,
    /// Whether a later committed entry sets every engine target the entry
    /// at an index, through the restamps behind it, sets: a restamp would
    /// undo it.
    pub superseded: &'a dyn Fn(u64) -> bool,
    /// A decision on (index, term) is in the log, committed or not.
    pub decided: &'a dyn Fn(u64, u64) -> bool,
}

/// What the leader decided this period.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Decided {
    pub append: Vec<Body>,
    /// Replicas that hold unresolved an entry that stands.
    pub contradicting: BTreeSet<NodeId>,
}

/// Counted while this voter led.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub restamps: u64,
    pub voids: u64,
    /// Voids after three late restamps.
    pub command_late: u64,
    pub purge_points: u64,
    /// Unresolved entries found standing.
    pub stood: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Decider {
    inflight: BTreeMap<(u64, u64, u64), Instant>,
    /// What the reports showed of an entry's block while its commit was
    /// not yet known here: the statuses a report carries span only the
    /// latest blocks, so a commit learned later would find none.
    seen: BTreeMap<(u64, u64, u64), Seen>,
    pub counts: Counts,
}

type Key = (u64, u64, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// The baseline committed the block with the entry applied.
    Stands,
    /// The baseline committed it without the entry, or nobody did.
    Decide,
}

fn key(id: StoredLogId) -> Key {
    (id.index, id.term, id.node)
}

fn lists(own: &Own, id: StoredLogId) -> bool {
    own.unresolved.iter().any(|l| l.id == id)
}

/// Whether `own` committed block `k`: by its status, or committing since
/// before it on every output that commits.
fn committed(own: &Own, k: u64, block: u64) -> bool {
    let by_status = own
        .statuses
        .iter()
        .any(|runs| status_of(runs, k).is_some_and(|s| s.kind == Kind::Committed));
    let by_standing = own.processed.is_some_and(|p| p >= k)
        && own
            .standing
            .iter()
            .any(|s| matches!(s, Standing::Committing { from } if *from <= k * block));
    by_status || by_standing
}

/// Whether `own` reported block `k` withheld on every output it reports
/// it on, and on at least one.
fn withheld(own: &Own, k: u64) -> bool {
    let mut seen = false;
    for runs in &own.statuses {
        if let Some(s) = status_of(runs, k) {
            if s.kind != Kind::Withheld {
                return false;
            }
            seen = true;
        }
    }
    seen
}

impl Decider {
    /// Leadership is lost: a new term decides again from the reports.
    pub fn step_down(&mut self) {
        self.inflight.clear();
        self.seen.clear();
    }

    /// The decisions on the entries the live audio replicas report
    /// unresolved, and on those a newly named baseline holds unresolved:
    /// `renamed` is the `Baseline` the leader appends this period.
    pub fn decide(
        &mut self,
        audio: &[Voter],
        baseline: Option<&Baseline>,
        renamed: Option<&Baseline>,
        log: &LeaderLog,
        timing: &Timing,
        now: Instant,
    ) -> Decided {
        self.inflight
            .retain(|_, at| now.duration_since(*at) < INFLIGHT);
        let live: Vec<&Voter> = audio.iter().filter(|a| !a.silent).collect();
        let mut reported: BTreeMap<Key, (StoredLogId, u64, BTreeSet<NodeId>)> = BTreeMap::new();
        for a in &live {
            for l in a.own.map(|o| &o.unresolved[..]).unwrap_or(&[]) {
                reported
                    .entry(key(l.id))
                    .or_insert((l.id, l.at_index, BTreeSet::new()))
                    .2
                    .insert(a.id);
            }
        }
        let mut out = Decided::default();
        let base = baseline.and_then(|b| live.iter().find(|a| a.id == b.replica));
        let next = renamed.and_then(|b| live.iter().find(|a| a.id == b.replica));
        for (k, (id, at, by)) in reported {
            if id.index & UNLISTED_BIT != 0
                || self.inflight.contains_key(&k)
                || (log.decided)(id.index, id.term)
            {
                continue;
            }
            if (log.log_id_at)(id.index) != Some(id) {
                // An id the leader's log lacks.
                if id.index <= log.purged {
                    for replica in by {
                        self.counts.purge_points += 1;
                        out.append.push(Body::PurgePoint {
                            replica,
                            index: id.index,
                        });
                    }
                } else if log.own_term_committed {
                    self.counts.voids += 1;
                    out.append.push(Body::Void {
                        revision: id.index,
                        term: id.term,
                        // The leader stamps it on append.
                        at_index: Some(0),
                        generation: None,
                    });
                } else {
                    continue;
                }
                self.hold(k, now);
                continue;
            }
            let kb = at / timing.block;
            // A newly named baseline holding it unresolved: re-decided
            // with the rename.
            let on_next = next.is_some_and(|n| n.own.is_some_and(|o| lists(o, id)));
            let now_seen = match base.and_then(|b| b.own) {
                _ if on_next => Some(Seen::Decide),
                Some(own) if committed(own, kb, timing.block) => Some(if lists(own, id) {
                    Seen::Decide
                } else {
                    Seen::Stands
                }),
                // Nobody committed the block: every live audio replica
                // withheld it or is silent about it.
                _ if !live.is_empty()
                    && live.iter().all(|a| a.own.is_some_and(|o| withheld(o, kb))) =>
                {
                    Some(Seen::Decide)
                }
                _ => None,
            };
            if let Some(seen) = now_seen {
                self.seen.insert(k, seen);
                while self.seen.len() > INFLIGHT_MAX {
                    self.seen.pop_first();
                }
            }
            if id.index > log.committed {
                continue;
            }
            match self.seen.get(&k) {
                None => continue,
                Some(Seen::Stands) => {
                    // It stands: applied where the baseline committed.
                    self.counts.stood += 1;
                    out.contradicting.extend(by);
                    continue;
                }
                Some(Seen::Decide) => {}
            }
            self.seen.remove(&k);
            let late = (log.restamps)(id.index);
            if late >= RESTAMPS_MAX || (log.superseded)(id.index) {
                if late >= RESTAMPS_MAX {
                    self.counts.command_late += 1;
                }
                self.counts.voids += 1;
                out.append.push(Body::Void {
                    revision: id.index,
                    term: id.term,
                    // The leader stamps it on append.
                    at_index: Some(0),
                    generation: None,
                });
            } else {
                self.counts.restamps += 1;
                // The leader stamps the fresh at_index on append.
                out.append.push(Body::Restamp {
                    revision: id.index,
                    term: id.term,
                    at_index: 0,
                    paths: Vec::new(),
                    generation: None,
                });
            }
            self.hold(k, now);
        }
        out
    }

    fn hold(&mut self, k: Key, now: Instant) {
        self.inflight.insert(k, now);
        while self.inflight.len() > INFLIGHT_MAX {
            self.inflight.pop_first();
        }
    }
}

/// Whether `later`, an entry after `earlier`, sets every engine target
/// `earlier` sets: then restamping `earlier` would undo it.
pub fn supersedes(earlier: &Request, later: &Request) -> bool {
    fn target(body: &Body) -> Option<(u8, u64)> {
        match body {
            Body::InputDown { slot, .. } | Body::InputUp { slot, .. } => {
                Some((0, u64::from(*slot)))
            }
            Body::MasterEnable { .. } => Some((1, 0)),
            _ => None,
        }
    }
    target(&earlier.body).is_some_and(|t| target(&later.body) == Some(t))
}

#[cfg(test)]
mod tests {
    use samplez::blocks::{BlockStatus, Cause, HEALTHY, Range};

    use super::*;
    use crate::report::{Listed, runs};

    const B: u64 = 48;
    const AT: u64 = 1_000 * B;
    const KA: u64 = AT / B;

    fn id(index: u64, term: u64) -> StoredLogId {
        StoredLogId {
            term,
            node: 1,
            index,
        }
    }

    fn status(kind: Kind) -> BlockStatus {
        BlockStatus {
            k: KA,
            kind,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::None,
            on_silence_of: 0,
            digest: Some(HEALTHY),
        }
    }

    /// A replica that reported block `KA` as `kind`, holding `unresolved`.
    fn own(kind: Kind, unresolved: &[StoredLogId]) -> Own {
        Own {
            processed: Some(KA + 2),
            statuses: vec![runs([Some(status(kind))])],
            standing: vec![Standing::Withdrawn { since: 0 }],
            unresolved: unresolved
                .iter()
                .map(|&id| Listed {
                    id,
                    at_index: AT,
                    baseline: false,
                })
                .collect(),
            ..Own::default()
        }
    }

    fn baseline(replica: NodeId) -> Baseline {
        Baseline {
            replica,
            order: Vec::new(),
            at_index: 0,
            key: None,
            versions: None,
        }
    }

    struct Log {
        held: Vec<StoredLogId>,
        committed: u64,
        purged: u64,
        own_term: bool,
        restamps: u32,
        superseded: bool,
        decided: bool,
    }

    impl Default for Log {
        fn default() -> Log {
            Log {
                held: vec![id(7, 2)],
                committed: 9,
                purged: 0,
                own_term: true,
                restamps: 0,
                superseded: false,
                decided: false,
            }
        }
    }

    fn decide(
        d: &mut Decider,
        owns: &[(NodeId, bool, Own)],
        base: Option<NodeId>,
        log: &Log,
    ) -> Decided {
        let audio: Vec<Voter> = owns
            .iter()
            .map(|(id, silent, own)| Voter {
                id: *id,
                silent: *silent,
                own: Some(own),
            })
            .collect();
        let held = |i: u64| log.held.iter().copied().find(|h| h.index == i);
        let restamps = |_| log.restamps;
        let superseded = |_| log.superseded;
        let decided = |_, _| log.decided;
        let view = LeaderLog {
            log_id_at: &held,
            committed: log.committed,
            purged: log.purged,
            own_term_committed: log.own_term,
            restamps: &restamps,
            superseded: &superseded,
            decided: &decided,
        };
        let b = base.map(baseline);
        d.decide(
            &audio,
            b.as_ref(),
            None,
            &view,
            &Timing::defaults(B),
            Instant::now(),
        )
    }

    fn restamp(index: u64, term: u64) -> Body {
        Body::Restamp {
            revision: index,
            term,
            at_index: 0,
            paths: Vec::new(),
            generation: None,
        }
    }

    /// An entry appended after its apply deadline is unresolved wherever
    /// it arrives, so on the baseline too: once the baseline commits the
    /// block holding its `at_index` the leader restamps it, once.
    #[tokio::test(start_paused = true)]
    async fn an_entry_late_on_the_baseline_is_restamped_once_the_baseline_commits_its_block() {
        let e = id(7, 2);
        let mut owns = vec![
            (1, false, own(Kind::Held, &[e])),
            (2, false, own(Kind::Committed, &[e])),
        ];
        let mut d = Decider::default();
        assert_eq!(
            decide(&mut d, &owns, Some(1), &Log::default()).append,
            [],
            "the baseline has not committed the block"
        );
        owns[0].2 = own(Kind::Committed, &[e]);
        let out = decide(&mut d, &owns, Some(1), &Log::default());
        assert_eq!(out.append, [restamp(7, 2)]);
        assert_eq!(decide(&mut d, &owns, Some(1), &Log::default()).append, []);
        tokio::time::advance(INFLIGHT).await;
        assert_eq!(
            decide(&mut d, &owns, Some(1), &Log::default()).append,
            [restamp(7, 2)],
            "decided again once the first is not seen in the log"
        );
    }

    /// A decision the log holds, committed or not, is never appended again:
    /// not after the in-flight window lapses (a stalled leader), nor by a
    /// new leader. Only the log says whether the id was decided.
    #[tokio::test(start_paused = true)]
    async fn an_id_the_log_holds_a_decision_on_is_not_decided_again() {
        let e = id(7, 2);
        let owns = vec![
            (1, false, own(Kind::Committed, &[e])),
            (2, false, own(Kind::Committed, &[e])),
        ];
        let mut d = Decider::default();
        assert_eq!(
            decide(&mut d, &owns, Some(1), &Log::default()).append,
            [restamp(7, 2)]
        );
        let held = Log {
            decided: true,
            ..Log::default()
        };
        tokio::time::advance(INFLIGHT).await;
        assert_eq!(decide(&mut d, &owns, Some(1), &held).append, []);
        d.step_down();
        assert_eq!(
            decide(&mut Decider::default(), &owns, Some(1), &held).append,
            []
        );
    }

    /// An entry whose `at_index` block every live audio replica withheld
    /// was committed by nobody: restamped without waiting for the
    /// baseline. A silent replica does not hold it up; one that committed
    /// the block does.
    #[tokio::test(start_paused = true)]
    async fn an_entry_whose_block_every_live_replica_withheld_is_restamped() {
        let e = id(7, 2);
        let mut owns = vec![
            (1, false, own(Kind::Withheld, &[e])),
            (2, false, own(Kind::Withheld, &[e])),
            (3, true, own(Kind::Held, &[])),
        ];
        let out = decide(&mut Decider::default(), &owns, Some(1), &Log::default());
        assert_eq!(out.append, [restamp(7, 2)]);
        owns[2].1 = false;
        let out = decide(&mut Decider::default(), &owns, Some(1), &Log::default());
        assert_eq!(out.append, [], "a live replica still holds the block");
    }

    /// Applied where the baseline committed: the entry stands, nothing is
    /// appended, and the replicas holding it unresolved contradict the
    /// log, which page verdict rule (1) diverges.
    #[tokio::test(start_paused = true)]
    async fn an_entry_the_baseline_applied_stands_and_contradicts_who_lacks_it() {
        let e = id(7, 2);
        let owns = vec![
            (1, false, own(Kind::Committed, &[])),
            (2, false, own(Kind::Committed, &[e])),
        ];
        let out = decide(&mut Decider::default(), &owns, Some(1), &Log::default());
        assert_eq!(out.append, []);
        assert_eq!(out.contradicting, [2].into());
        let page = |hash| crate::report::Page {
            output: 0,
            page: 3,
            hash,
            fingerprint: 1,
            committed: true,
            degraded: false,
            digest: Some(HEALTHY),
        };
        let judged =
            crate::verdict::judge(&[(1, page(5)), (2, page(5))], Some(1), &out.contradicting);
        assert_eq!(judged.diverged, [2].into(), "equal hashes, still diverged");
    }

    /// Three restamps late again void the entry and count `command_late`;
    /// an entry a later committed one fully supersedes is voided at once.
    #[tokio::test(start_paused = true)]
    async fn a_fourth_late_decision_voids_the_entry() {
        let e = id(7, 2);
        let owns = vec![(1, false, own(Kind::Committed, &[e]))];
        let void = Body::Void {
            revision: 7,
            term: 2,
            at_index: Some(0),
            generation: None,
        };
        for (restamps, superseded, want, late) in [
            (2, false, restamp(7, 2), 0),
            (3, false, void.clone(), 1),
            (0, true, void.clone(), 0),
        ] {
            let log = Log {
                restamps,
                superseded,
                ..Log::default()
            };
            let mut d = Decider::default();
            assert_eq!(decide(&mut d, &owns, Some(1), &log).append, [want]);
            assert_eq!(d.counts.command_late, late);
        }
    }

    /// An unresolved id from an earlier term the leader's log lacks is
    /// voided once the leader committed an entry of its own term, never
    /// before; at or below the leader's purge point the reporter gets a
    /// purge-point verdict instead.
    #[tokio::test(start_paused = true)]
    async fn an_id_the_leaders_log_lacks_is_voided_or_draws_a_purge_point_verdict() {
        let lost = id(8, 1);
        let owns = vec![
            (1, false, own(Kind::Committed, &[])),
            (2, false, own(Kind::Withheld, &[lost])),
        ];
        let waiting = Log {
            own_term: false,
            ..Log::default()
        };
        assert_eq!(
            decide(&mut Decider::default(), &owns, Some(1), &waiting).append,
            []
        );
        assert_eq!(
            decide(&mut Decider::default(), &owns, Some(1), &Log::default()).append,
            [Body::Void {
                revision: 8,
                term: 1,
                at_index: Some(0),
                generation: None,
            }]
        );
        let purged = Log {
            purged: 8,
            own_term: false,
            ..Log::default()
        };
        assert_eq!(
            decide(&mut Decider::default(), &owns, Some(1), &purged).append,
            [Body::PurgePoint {
                replica: 2,
                index: 8
            }]
        );
    }

    /// A rename re-decides what the new baseline holds unresolved, before
    /// the old baseline's block decides anything.
    #[tokio::test(start_paused = true)]
    async fn a_rename_restamps_what_the_new_baseline_holds_unresolved() {
        let e = id(7, 2);
        let owns = [
            (1, false, own(Kind::Held, &[])),
            (2, false, own(Kind::Committed, &[e])),
        ];
        let audio: Vec<Voter> = owns
            .iter()
            .map(|(id, silent, own)| Voter {
                id: *id,
                silent: *silent,
                own: Some(own),
            })
            .collect();
        let log = Log::default();
        let held = |i: u64| log.held.iter().copied().find(|h| h.index == i);
        let zero = |_| 0;
        let whole = |_| false;
        let no = |_, _| false;
        let view = LeaderLog {
            log_id_at: &held,
            committed: 9,
            purged: 0,
            own_term_committed: true,
            restamps: &zero,
            superseded: &whole,
            decided: &no,
        };
        let (old, new) = (baseline(1), baseline(2));
        let mut d = Decider::default();
        let t = Timing::defaults(B);
        let none = d.decide(&audio, Some(&old), None, &view, &t, Instant::now());
        assert_eq!(none.append, []);
        let again = d.decide(&audio, Some(&old), Some(&new), &view, &t, Instant::now());
        assert_eq!(again.append, [restamp(7, 2)]);
    }

    #[test]
    fn a_later_entry_on_the_same_target_supersedes() {
        let down = |slot, at_index| Request {
            stamp: 0,
            body: Body::InputDown { slot, at_index },
        };
        let up = Request {
            stamp: 0,
            body: Body::InputUp {
                slot: 1,
                at_index: 9,
            },
        };
        assert!(supersedes(&down(1, 5), &up));
        assert!(!supersedes(&down(2, 5), &up));
    }
}
