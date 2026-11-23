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
use crate::rollout::{DEPARTED, Versions, base_gone, baseline_of, may_take_over};
use crate::state::{PAGES_KEPT, PageBook, ReplicaState, filling, withheld_from};
use crate::verdict::judge_by_key;

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
    /// The leader's engine has outputs and the log disabled every one:
    /// no replica writes, so no named replica's start bound lapses. False
    /// on a leader without an engine.
    pub nothing_to_write: bool,
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
    /// Pages without a strict majority hash among two or more of one key.
    pub disagreements: u64,
    /// Pages committed whole by replicas of two keys.
    pub cross_key_disagreements: u64,
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
    /// Replicas a `Baseline` named that were passed over before they
    /// committed: how often in a row, and when the hand-over may name one
    /// again. A named replica's withheld blocks need not show (an output
    /// that never committed stands idle and says nothing), so this, not
    /// its statuses, keeps the hand-over from naming it every period.
    failed: BTreeMap<NodeId, (u32, Instant)>,
    /// `replica_disagreement`: the last page judged had no strict majority
    /// among two or more hashes of one key, or was committed whole by
    /// replicas of two keys.
    pub disagreement: bool,
    /// The latest `Baseline`'s versions and when a live audio voter last
    /// carried them, or this leader first saw them.
    base_seen: Option<(Option<Versions>, Instant)>,
    /// No live audio voter has carried the latest `Baseline`'s versions
    /// for [`DEPARTED`]: lower versions may take it over.
    gone: bool,
    /// Why the key hand-over passed over each replica of another key
    /// than the baseline's, as last logged: logged again only once the
    /// reason changes.
    refused: BTreeMap<NodeId, &'static str>,
}

/// Which rule a `Baseline` the leader appends follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// The key hand-over.
    HandOver,
    /// The no-committer rule, or a committing replica while none is
    /// named.
    Named,
    /// A rename of a baseline that stopped committing, was diverged or
    /// left the audio voters.
    Renamed,
}

/// The versions of every live audio voter that reported.
fn live(v: &LeaderView) -> Vec<Option<Versions>> {
    v.audio
        .iter()
        .filter(|a| !a.silent)
        .filter_map(|a| a.own)
        .map(|o| o.versions)
        .collect()
}

/// The latest `Baseline`'s key and versions ([`baseline_of`]).
fn base_of(v: &LeaderView, b: &Baseline) -> (Option<u64>, Option<Versions>) {
    let named = v
        .audio
        .iter()
        .find(|a| a.id == b.replica)
        .and_then(|a| a.own);
    baseline_of(b, named)
}

/// The key `id` reports.
fn a_key(v: &LeaderView, id: NodeId) -> Option<u64> {
    (v.audio.iter())
        .find(|a| a.id == id)
        .and_then(|a| a.own)
        .and_then(|o| o.key)
}

/// Whether `a` may be named: it reports its compatibility key, or no
/// audio voter reports one, as in a set without a rollout, where every
/// key is equal. A `Baseline` naming a replica of unknown key carries
/// none, and that replica may since run under another key than the one
/// the set commits.
fn keyed(v: &LeaderView, a: &Voter) -> bool {
    let reports = |a: &Voter| a.own.is_some_and(|o| o.key.is_some());
    reports(a) || !v.audio.iter().any(reports)
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
        self.gone = self.base_departed(v, now);
        let mut out = self.verdicts(v, now);
        if let Some((at, sent)) = &self.inflight
            && sent.as_ref() == v.baseline
            && now.duration_since(*at) < INFLIGHT
        {
            return out;
        }
        self.inflight = None;
        self.failed
            .retain(|id, _| !v.audio.iter().any(|a| a.id == *id && committing(a)));
        if let Some((b, rule)) = self.baseline(v, now) {
            tracing::info!(
                rule = ?rule,
                replica = b.replica,
                key = ?b.key,
                versions = ?b.versions,
                order = ?b.order,
                "Baseline decided"
            );
            if let Some(cur) = v.baseline
                && cur.replica != b.replica
                && !v.audio.iter().any(|a| a.id == cur.replica && committing(a))
            {
                let f = self.failed.entry(cur.replica).or_insert((0, now));
                f.0 += 1;
                f.1 = now + INFLIGHT * 2u32.pow((f.0 - 1).min(5));
            }
            self.inflight = Some((now, v.baseline.cloned()));
            out.push(Body::Baseline(b));
        }
        out
    }

    /// Whether no live audio voter has carried the latest `Baseline`'s
    /// versions for [`DEPARTED`]. A leader counts from when it first saw
    /// them, so a new one waits that long too.
    fn base_departed(&mut self, v: &LeaderView, now: Instant) -> bool {
        let Some(versions) = v.baseline.map(|b| base_of(v, b).1) else {
            self.base_seen = None;
            return false;
        };
        let gone_now = base_gone(versions, &live(v));
        match &mut self.base_seen {
            Some((seen, at)) if *seen == versions => {
                if !gone_now {
                    *at = now;
                }
            }
            _ => self.base_seen = Some((versions, now)),
        }
        let since = self.base_seen.map_or(now, |(_, at)| at);
        gone_now && now.duration_since(since) >= DEPARTED
    }

    /// Leadership is lost: a verdict taken while leading does not rename
    /// a baseline in a later term.
    pub fn step_down(&mut self) {
        self.against = None;
        self.base_seen = None;
    }

    /// The `Baseline` to append, if any, and the rule it follows.
    fn baseline(&mut self, v: &LeaderView, now: Instant) -> Option<(Baseline, Rule)> {
        if self
            .against
            .as_ref()
            .is_some_and(|(_, under)| under.as_ref() != v.baseline)
        {
            self.against = None;
        }
        let anyone = v.audio.iter().any(committing);
        if anyone && let Some(b) = self.hand_over(v, now) {
            self.counts.handed_over += 1;
            return Some((b, Rule::HandOver));
        }
        let Some(current) = v.baseline else {
            if !anyone {
                return self.no_committer(v).map(|b| (b, Rule::Named));
            }
            // One voter commits without waiting for its entry: a set grown
            // from it names the top committing replica.
            let b = self.rename(v, None)?;
            self.counts.named += 1;
            return Some((b, Rule::Named));
        };
        let named = v.audio.iter().find(|a| a.id == current.replica);
        if !anyone {
            if named.is_some_and(|n| self.about_to_commit(v, n, current)) {
                return None;
            }
            return self.no_committer(v).map(|b| (b, Rule::Named));
        }
        // Left the audio voters: it commits no more.
        let Some(n) = named else {
            let b = self.rename(v, None)?;
            self.counts.renamed += 1;
            return Some((b, Rule::Renamed));
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
        Some((b, Rule::Renamed))
    }

    /// The key hand-over: in a rollout that changes the key, a `Baseline`
    /// naming the top target-revision replica with the target's key once
    /// it is a candidate, has applied the latest `Baseline`, reports no
    /// block withheld for a cause and may take the baseline over by its
    /// versions ([`may_take_over`]): higher ones, or equal ones under
    /// another output config. A replica passed over, or one that never
    /// learned of its naming, is named again only then. Why each replica
    /// of another key than the baseline's is passed over is logged once
    /// per change of the reason.
    fn hand_over(&mut self, v: &LeaderView, now: Instant) -> Option<Baseline> {
        let n = self.hand_over_to(v, now);
        if n.is_none() {
            self.refused.clear();
        }
        let (replica, key, versions) = n?;
        self.against = None;
        Some(Baseline {
            replica,
            order: order(v, replica),
            at_index: 0,
            key: Some(key),
            versions,
        })
    }

    /// The replica the key hand-over names, the target's key and that
    /// replica's versions; logs why each other one of another key than
    /// the baseline's is passed over, when the reason changed.
    fn hand_over_to(
        &mut self,
        v: &LeaderView,
        now: Instant,
    ) -> Option<(NodeId, u64, Option<Versions>)> {
        let target = v.target?;
        let current = v.baseline?;
        let at_target = |own: &Own| own.key.is_some() && own.revision.as_deref() == Some(target);
        let key = (v.audio.iter())
            .filter_map(|a| a.own)
            .filter(|o| at_target(o))
            .find_map(|o| o.key)?;
        let (base_key, base_versions) = base_of(v, current);
        if base_key.is_none_or(|k| k == key) {
            return None;
        }
        let live = live(v);
        let refusal = |a: &Voter, own: &Own| {
            if !at_target(own) {
                Some("revision")
            } else if own.key != Some(key) {
                Some("key")
            } else if !may_take_over(own.versions, base_versions, self.gone, &live) {
                Some("versions")
            } else if self
                .failed
                .get(&a.id)
                .is_some_and(|&(_, until)| now < until)
            {
                Some("failed")
            } else if !candidate(v, a) {
                Some("candidate")
            } else if own.baseline != Some((current.replica, current.at_index)) {
                Some("baseline applied")
            // One that computes nothing reports every block withheld.
            } else if own.state != Some(ReplicaState::VersionMismatch) && withheld_from(own, 0) {
                Some("withheld")
            } else {
                None
            }
        };
        let mut refused = BTreeMap::new();
        let mut named: Option<(NodeId, Option<Versions>)> = None;
        for a in &v.audio {
            let Some(own) = a.own.filter(|o| o.key.is_some() && o.key != base_key) else {
                continue;
            };
            match refusal(a, own) {
                Some(why) => {
                    refused.insert(a.id, why);
                }
                None if named.is_none_or(|(n, _)| ((v.rank)(a.id), a.id) < ((v.rank)(n), n)) => {
                    named = Some((a.id, own.versions));
                }
                None => {}
            }
        }
        for (&id, &why) in &refused {
            if self.refused.get(&id) != Some(&why) {
                tracing::info!(
                    replica = id,
                    reason = why,
                    key = ?a_key(v, id),
                    "the key hand-over passes a replica over"
                );
            }
        }
        self.refused = refused;
        named.map(|(id, versions)| (id, key, versions))
    }

    /// A `Baseline` naming the highest-ranked replica committing the
    /// latest blocks other than `from`.
    fn rename(&mut self, v: &LeaderView, from: Option<NodeId>) -> Option<Baseline> {
        let next = v
            .audio
            .iter()
            .filter(|a| Some(a.id) != from && committing(a) && keyed(v, a))
            .min_by_key(|a| ((v.rank)(a.id), a.id))?;
        self.against = None;
        Some(Baseline {
            replica: next.id,
            order: order(v, next.id),
            at_index: 0,
            key: next.own.and_then(|o| o.key),
            versions: next.own.and_then(|o| o.versions),
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

    /// The no-committer rule: the top candidate that may hold the
    /// baseline. Under a `Baseline` whose versions a live voter still
    /// carries, that is one with its key, one that may take it over, or
    /// one filling a stop: a previous baseline's replica whose hand-over
    /// did not take, which committed up to it. Once none has carried them
    /// for [`DEPARTED`], one with the highest versions live.
    fn no_committer(&mut self, v: &LeaderView) -> Option<Baseline> {
        let live = live(v);
        let base = v.baseline.map(|b| base_of(v, b));
        let gone = base.is_some() && self.gone;
        let highest = live.iter().copied().max().flatten();
        let may = |a: &&Voter| {
            let own = a.own.expect("a candidate reported");
            let Some((key, versions)) = base else {
                return true;
            };
            if gone {
                return own.versions == highest;
            }
            filling(own)
                || key.is_none()
                || own.key.is_none_or(|k| Some(k) == key)
                || may_take_over(own.versions, versions, gone, &live)
        };
        let top = v
            .audio
            .iter()
            .filter(|a| candidate(v, a) && keyed(v, a))
            .filter(may)
            .min_by_key(|a| {
                let own = a.own.expect("a candidate reported");
                let class = if own.state.is_some_and(ReplicaState::diverged) {
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
            versions: top.own.and_then(|o| o.versions),
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
                    .filter_map(|a| {
                        let key = a.own.and_then(|own| own.key);
                        v.pages.get(a.id, o, *q).map(|p| (a.id, key, *p))
                    })
                    .collect();
                let base_key = v.baseline.and_then(|b| base_of(v, b).0);
                let j = judge_by_key(&records, base, base_key, v.contradicting);
                let reported = |id: NodeId| {
                    v.audio
                        .iter()
                        .any(|a| a.id == id && state(a).is_some_and(ReplicaState::diverged))
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
                if j.cross_key {
                    self.counts.cross_key_disagreements += 1;
                }
                self.disagreement = j.disagreement || j.cross_key;
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
        // Fenced by a clock step it detected: its withheld blocks may
        // not show, since an output that never committed stands idle
        // and an idle output's statuses say nothing.
        || own.clock.step_fence
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
/// It does not lapse while nothing is to be written: a named replica
/// commits nothing then, and neither would any other named instead.
fn lapsed(v: &LeaderView, n: &Voter, p: u64) -> bool {
    if v.nothing_to_write {
        return false;
    }
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
        target: Option<&'static str>,
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
                target: None,
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
                target: self.target,
                nothing_to_write: false,
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
            versions: None,
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
            versions: None,
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
                assert_eq!((b.replica, &b.order[..]), (a(1), &[a(3), a(2), a(4)][..]));
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
            versions: None,
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

    /// A replica reporting another key than the others is not judged
    /// against their majority: its pages differ by its key alone.
    #[tokio::test(start_paused = true)]
    async fn a_page_verdict_leaves_out_replicas_of_another_key() {
        let k = 1_000_000;
        let q = 900;
        let keyed = |own: Own, key| Own {
            key: Some(key),
            ..own
        };
        let mut f = Fixture::new(vec![
            (a(0), false, keyed(committing_own(k), 1)),
            (a(1), false, keyed(committing_own(k), 1)),
            (a(2), false, keyed(committing_own(k), 1)),
            (a(3), false, keyed(own(ReplicaState::Withdrawn, k), 2)),
        ]);
        for (x, h) in [(a(0), 1), (a(1), 1), (a(2), 1)] {
            f.pages.fold(x, &[record(q, h)]);
        }
        let held = crate::report::Page {
            committed: false,
            ..record(q, 2)
        };
        f.pages.fold(a(3), &[held]);
        let mut leader = Leader::default();
        let out = f.decide(&mut leader);
        assert!(
            !out.iter().any(|b| matches!(b, Body::Diverged { .. })),
            "{out:?}"
        );
        assert!(leader.judged[&0].contains(&q), "judged");
    }

    /// The leader names no replica whose report carries no key while
    /// another reports one, by a rename or by the no-committer rule: its
    /// `Baseline` would carry no key. In a set where none reports a key
    /// it names as before.
    #[tokio::test(start_paused = true)]
    async fn the_leader_names_no_replica_without_a_key() {
        let k = 1_000_000;
        let keyed = |own: Own| Own {
            key: Some(1),
            ..own
        };
        let f = Fixture::new(vec![
            (a(0), false, committing_own(k)),
            (a(1), false, keyed(committing_own(k))),
        ]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "renamed"
        );
        let f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, k)),
            (a(1), false, keyed(own(ReplicaState::Withdrawn, k))),
        ]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(1)),
            "no committer"
        );
        let f = Fixture::new(vec![
            (a(0), false, own(ReplicaState::Withdrawn, k)),
            (a(1), false, own(ReplicaState::Withdrawn, k)),
        ]);
        assert_eq!(
            named(&f.decide(&mut Leader::default())),
            Some(a(0)),
            "no key anywhere"
        );
    }

    /// Two committing replicas of two keys that both committed a page
    /// whole: the output carried both keys. The leader raises
    /// `replica_disagreement` and diverges the one whose key is not the
    /// baseline's.
    #[tokio::test(start_paused = true)]
    async fn a_page_committed_under_two_keys_diverges_the_other_key() {
        let k = 1_000_000;
        let q = 900;
        let keyed = |own: Own, key| Own {
            key: Some(key),
            ..own
        };
        let mut f = Fixture::new(vec![
            (a(0), false, keyed(committing_own(k), 1)),
            (a(1), false, keyed(committing_own(k), 1)),
            (a(2), false, keyed(committing_own(k), 2)),
        ]);
        f.baseline = Some(Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: 0,
            key: Some(1),
            versions: None,
        });
        for x in [a(0), a(1), a(2)] {
            f.pages.fold(x, &[record(q, 1)]);
        }
        let mut leader = Leader::default();
        let out = f.decide(&mut leader);
        let diverged: Vec<NodeId> = out
            .iter()
            .filter_map(|b| match b {
                Body::Diverged { replica, .. } => Some(*replica),
                _ => None,
            })
            .collect();
        assert_eq!(diverged, [a(2)], "{out:?}");
        assert!(leader.disagreement);
        assert_eq!(
            (
                leader.counts.cross_key_disagreements,
                leader.counts.disagreements
            ),
            (1, 0)
        );
        // The reverse: two replicas of one key without a majority.
        f.owns[2].2.key = Some(1);
        f.pages = PageBook::default();
        for (x, h) in [(a(0), 1), (a(1), 2)] {
            f.pages.fold(x, &[record(q, h)]);
        }
        f.owns.pop();
        let mut leader = Leader::default();
        f.decide(&mut leader);
        assert!(leader.disagreement);
        assert_eq!(
            (
                leader.counts.cross_key_disagreements,
                leader.counts.disagreements
            ),
            (0, 1)
        );
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
            versions: None,
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
            versions: None,
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

    fn versions(compute: u32) -> Option<Versions> {
        Some(Versions {
            compute,
            protocol: 1,
            derivation: 1,
        })
    }

    /// A replica at `revision` with key `key` of `versions`, which applied
    /// `applied`.
    fn rolled(
        state: ReplicaState,
        key: u64,
        versions: Option<Versions>,
        revision: &str,
        applied: &Baseline,
    ) -> Own {
        Own {
            key: Some(key),
            versions,
            revision: Some(revision.to_owned()),
            baseline: Some((applied.replica, applied.at_index)),
            ..own(state, 1_000_000)
        }
    }

    /// The hand-over names a target-revision replica with another key
    /// only when its versions are not below the baseline's: higher ones,
    /// or equal ones under another output config; a lower one, a
    /// downgrade, is never named while the baseline's versions are live.
    /// None ranks below any versions and hands over as before when no
    /// versions are known.
    #[tokio::test(start_paused = true)]
    async fn the_hand_over_names_only_versions_not_below_the_baselines() {
        let b = Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: 0,
            key: Some(1),
            versions: versions(2),
        };
        let cases = [
            (versions(3), versions(2), Some(a(1)), "higher"),
            (
                versions(2),
                versions(2),
                Some(a(1)),
                "equal, another config",
            ),
            (versions(1), versions(2), None, "a downgrade"),
            (None, versions(2), None, "none below some"),
            (versions(1), None, Some(a(1)), "some above none"),
            (None, None, Some(a(1)), "none known"),
        ];
        for (mine, base, want, what) in cases {
            let b = Baseline {
                versions: base,
                ..b.clone()
            };
            let mut f = Fixture::new(vec![
                (
                    a(0),
                    false,
                    rolled(ReplicaState::Committing, 1, base, "r1", &b),
                ),
                (
                    a(1),
                    false,
                    rolled(ReplicaState::VersionMismatch, 2, mine, "r2", &b),
                ),
            ]);
            f.baseline = Some(b);
            f.target = Some("r2");
            let mut leader = Leader::default();
            assert_eq!(named(&f.decide(&mut leader)), want, "{what}");
            assert_eq!(leader.counts.handed_over, u64::from(want.is_some()));
        }
    }

    /// With nobody committing, a candidate below the baseline's versions
    /// is not named while a live voter carries them, though that voter is
    /// no candidate, nor while it is unheard for less than the departure
    /// window; once none has carried them for that long, the candidate
    /// with the highest live versions is, whatever the order says.
    #[tokio::test(start_paused = true)]
    async fn the_highest_live_versions_take_over_once_the_baselines_are_gone() {
        let b = Baseline {
            replica: a(0),
            order: Vec::new(),
            at_index: 0,
            key: Some(3),
            versions: versions(3),
        };
        let mut unverified = rolled(ReplicaState::Withdrawn, 3, versions(3), "r3", &b);
        unverified.clock.unverified = true;
        let mut f = Fixture::new(vec![
            (a(0), false, unverified),
            (
                a(1),
                false,
                rolled(ReplicaState::VersionMismatch, 1, versions(1), "r1", &b),
            ),
            (
                a(2),
                false,
                rolled(ReplicaState::VersionMismatch, 2, versions(2), "r1", &b),
            ),
        ]);
        f.baseline = Some(b);
        f.target = Some("r1");
        let mut leader = Leader::default();
        assert_eq!(
            named(&f.decide(&mut leader)),
            None,
            "a downgrade while the baseline's versions are live"
        );
        // Unheard for less than the departure window: not gone.
        f.owns[0].1 = true;
        tokio::time::advance(DEPARTED - Duration::from_millis(10)).await;
        assert_eq!(named(&f.decide(&mut leader)), None, "unheard, not gone");
        // Heard again, then unheard: the window starts over.
        f.owns[0].1 = false;
        assert_eq!(named(&f.decide(&mut leader)), None);
        f.owns[0].1 = true;
        tokio::time::advance(DEPARTED - Duration::from_millis(10)).await;
        assert_eq!(named(&f.decide(&mut leader)), None, "the window restarted");
        tokio::time::advance(Duration::from_millis(10)).await;
        let bodies = f.decide(&mut leader);
        let named_b = bodies.iter().find_map(|b| match b {
            Body::Baseline(b) => Some(b.clone()),
            _ => None,
        });
        let named_b = named_b.expect("a Baseline once the baseline's versions are gone");
        assert_eq!(named_b.replica, a(2), "the highest live versions");
        assert_eq!(named_b.key, Some(2));
        assert_eq!(named_b.versions, versions(2));
    }
}
