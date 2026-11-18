//! Resync by state transfer: a diverged replica takes a
//! committing donor's engine core over the peer listener, replays it from
//! its own input and log up to the block in progress, and hands the
//! replayed state to its RT thread at a block boundary. It commits again
//! once a page after the hand-over matches (Resume) or by the Named rule;
//! a failed attempt leaves it diverged, and it tries again with a newer
//! core.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::entry::NodeId;
use crate::report::{Own, Page};
use crate::state::ReplicaState;
use crate::state_machine::Record;

/// A donor's core after block `k0`, as the joiner takes it: `core` is the
/// engine's own encoding, which carries the engine version, and
/// `last_id` the highest log index the donor's engine had taken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreReply {
    pub k0: u64,
    pub last_id: u64,
    pub core: String,
}

/// A hand-over: the RT thread runs on from the replayed state after block
/// `j`.
#[derive(Debug, Clone, PartialEq)]
pub struct Resynced {
    pub j: u64,
    /// The pages the replay computed whole, `committed` false.
    pub pages: Vec<Page>,
    /// The unresolved entries the state holds, the donor's: `(log index,
    /// at_index)`.
    pub unresolved: Vec<(u64, u64)>,
}

/// The replica no longer holds the record of an entry the replay needs:
/// one applied after the donor core's last log id. The attempt fails and
/// another core is fetched, never a gap replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Missed;

/// What a joiner's replay reads from the replica while it runs.
pub trait Source: Send + Sync {
    /// The records of the entries applied after log index `after`, in
    /// log order, with their log index; [`Missed`] when one is no longer
    /// held.
    fn entries(&self, after: u64) -> Result<Vec<(u64, Record)>, Missed>;
    /// Whether the hand-over may still happen: false once the replica
    /// left `resync` or the attempt was given up.
    fn go(&self) -> bool;
}

/// How long the next attempt waits after one ended: [`RETRY`], doubled
/// after each attempt up to [`RETRY_MAX`], back to [`RETRY`] once the
/// replica commits again. A replica whose replayed state never matches
/// asks its donor for a core at most every [`RETRY_MAX`].
pub const RETRY: Duration = Duration::from_millis(100);
pub const RETRY_MAX: Duration = Duration::from_secs(10);

/// How long a joiner waits for the donor's core.
pub const FETCH_WITHIN: Duration = Duration::from_secs(1);

/// The resyncs of one replica, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Metrics {
    /// Attempts started: a donor asked for its core.
    pub attempts: u64,
    /// Replayed states the RT thread took.
    pub handovers: u64,
    /// Attempts that ended without a hand-over: no core, a refused one, a
    /// replayed page unequal to the donor's, or none taken in time.
    pub failures: u64,
    /// Hand-overs reached later than `resyncBudgetMs` after the core was
    /// asked for, and whether the latest one was.
    pub over_budget: u64,
    pub budget_exceeded: bool,
    /// From asking for the core to the hand-over, the latest.
    pub last: Option<Duration>,
    /// How long the next attempt waits after the last one ended.
    pub backoff: Duration,
}

/// The attempt in progress and the counts.
#[derive(Debug, Default)]
pub(crate) struct Tracker {
    /// Numbers the attempts: a late answer to an abandoned one is told
    /// apart.
    epoch: u64,
    /// The attempt in progress, from when its core was asked for.
    running: Option<(u64, Instant)>,
    ended: Option<Instant>,
    /// The backoff after the next attempt ends, none for [`RETRY`].
    wait: Option<Duration>,
    pub metrics: Metrics,
}

impl Tracker {
    /// A new attempt, unless one runs or the last ended less than the
    /// backoff ago.
    pub fn start(&mut self, now: Instant) -> Option<u64> {
        let wait = self.metrics.backoff;
        if self.running.is_some() || self.ended.is_some_and(|e| now < e + wait) {
            return None;
        }
        self.epoch += 1;
        self.running = Some((self.epoch, now));
        self.metrics.attempts += 1;
        Some(self.epoch)
    }

    /// Whether `epoch` is the attempt in progress.
    pub fn current(&self, epoch: u64) -> bool {
        self.running.is_some_and(|(e, _)| e == epoch)
    }

    /// Gives up the attempt in progress: its answer, when it comes, is
    /// not acted on, and the next attempt waits the backoff.
    pub fn abandon(&mut self, now: Instant) {
        if self.running.take().is_some() {
            self.ended(now);
        }
    }

    /// The replica commits again: the next resync waits [`RETRY`] only.
    pub fn committing(&mut self) {
        self.wait = None;
        self.metrics.backoff = RETRY;
    }

    /// An attempt ended at `now`: the next waits the backoff, which
    /// doubles for the one after.
    fn ended(&mut self, now: Instant) {
        self.ended = Some(now);
        let wait = self.wait.unwrap_or(RETRY);
        self.metrics.backoff = wait;
        self.wait = Some((wait * 2).min(RETRY_MAX));
    }

    /// Ends attempt `epoch`; `handed` when the RT thread took its state.
    /// False for an attempt no longer current.
    pub fn end(&mut self, epoch: u64, handed: bool, budget: Duration, now: Instant) -> bool {
        let Some((e, began)) = self.running else {
            return false;
        };
        if e != epoch {
            return false;
        }
        self.running = None;
        self.ended(now);
        if handed {
            let took = now - began;
            self.metrics.handovers += 1;
            self.metrics.last = Some(took);
            self.metrics.budget_exceeded = took > budget;
            if took > budget {
                self.metrics.over_budget += 1;
                tracing::warn!(
                    took_ms = took.as_millis() as u64,
                    budget_ms = budget.as_millis() as u64,
                    "a resync handed over later than resyncBudgetMs"
                );
            }
        } else {
            self.metrics.failures += 1;
        }
        true
    }
}

/// The donor: a fresh, clock-verified committing audio replica, the
/// baseline first, else the lowest node id. The caller leaves out the
/// replicas a `ClockVerdict` excludes.
pub(crate) fn donor(baseline: Option<NodeId>, peers: &[(NodeId, bool, &Own)]) -> Option<NodeId> {
    let committing: Vec<NodeId> = peers
        .iter()
        .filter(|(_, silent, own)| {
            !silent && own.state == Some(ReplicaState::Committing) && !own.clock.unverified
        })
        .map(|&(id, _, _)| id)
        .collect();
    baseline
        .filter(|b| committing.contains(b))
        .or_else(|| committing.into_iter().min())
}

/// The first replayed page that differs from what the donor reported for
/// it, hash or entry fingerprint; pages the donor did not report are not
/// compared.
pub(crate) fn mismatch(replayed: &[Page], donor: impl Fn(u32, u64) -> Option<Page>) -> Option<u64> {
    replayed
        .iter()
        .find(|r| {
            donor(r.output, r.page)
                .is_some_and(|d| (d.hash, d.fingerprint) != (r.hash, r.fingerprint))
        })
        .map(|r| r.page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own(state: ReplicaState) -> Own {
        Own {
            state: Some(state),
            ..Own::default()
        }
    }

    fn page(page: u64, hash: u64) -> Page {
        Page {
            output: 0,
            page,
            hash,
            fingerprint: 0,
            committed: true,
            degraded: false,
            digest: None,
        }
    }

    #[test]
    fn the_baseline_donates_first_then_the_lowest_committing_id() {
        let c = own(ReplicaState::Committing);
        let w = own(ReplicaState::Withdrawn);
        let peers = [
            (5, false, &c),
            (3, false, &c),
            (2, false, &w),
            (1, true, &c),
        ];
        assert_eq!(donor(Some(5), &peers), Some(5));
        assert_eq!(donor(Some(2), &peers), Some(3), "a withdrawn baseline");
        assert_eq!(donor(Some(1), &peers), Some(3), "a silent baseline");
        assert_eq!(donor(None, &[(2, false, &w)]), None);
        let mut unverified = own(ReplicaState::Committing);
        unverified.clock.unverified = true;
        assert_eq!(
            donor(Some(4), &[(4, false, &unverified), (6, false, &c)]),
            Some(6)
        );
    }

    #[test]
    fn a_replayed_page_unequal_to_the_donors_is_found() {
        let donor = |_: u32, q: u64| match q {
            4 => Some(page(4, 40)),
            5 => Some(page(5, 50)),
            _ => None,
        };
        assert_eq!(mismatch(&[page(4, 40), page(6, 1)], donor), None);
        assert_eq!(mismatch(&[page(4, 40), page(5, 51)], donor), Some(5));
        assert_eq!(mismatch(&[page(5, 51)], |_, _| None), None);
    }

    #[test]
    fn an_attempt_retries_only_after_the_last_ended() {
        let mut t = Tracker::default();
        let t0 = Instant::now();
        let budget = Duration::from_millis(50);
        let e = t.start(t0).unwrap();
        assert_eq!(t.start(t0), None, "one runs");
        assert!(t.end(e, false, budget, t0));
        assert_eq!(t.start(t0 + RETRY / 2), None, "too soon");
        let e2 = t.start(t0 + RETRY).unwrap();
        assert!(!t.end(e, true, budget, t0 + RETRY), "a stale answer");
        assert!(t.end(e2, true, budget, t0 + RETRY + budget * 2));
        assert_eq!(
            (t.metrics.attempts, t.metrics.handovers, t.metrics.failures),
            (2, 1, 1)
        );
        assert!(t.metrics.budget_exceeded);
        assert_eq!(t.metrics.over_budget, 1);
    }

    /// Attempts that never bring the replica back wait twice as long each
    /// time, up to the cap, hand-over or not; committing again resets it.
    #[test]
    fn the_wait_doubles_to_its_cap_until_the_replica_commits() {
        let mut t = Tracker::default();
        let budget = Duration::from_millis(50);
        let mut now = Instant::now();
        let mut waits = Vec::new();
        for n in 0..10 {
            let e = t.start(now).expect("due");
            assert!(t.end(e, n % 2 == 0, budget, now));
            let wait = t.metrics.backoff;
            assert_eq!(t.start(now + wait - Duration::from_millis(1)), None);
            waits.push(wait);
            now += wait;
        }
        assert_eq!(waits[0], RETRY);
        assert_eq!(waits[1], RETRY * 2);
        assert_eq!(*waits.last().unwrap(), RETRY_MAX);
        assert_eq!(t.metrics.attempts, 10);
        t.committing();
        let e = t.start(now).unwrap();
        assert!(t.end(e, false, budget, now));
        assert_eq!(t.metrics.backoff, RETRY);
    }
}
