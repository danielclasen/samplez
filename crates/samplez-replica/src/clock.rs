//! The clock check: the leader compares the
//! clocks of the verified audio replicas through the offsets theta of its
//! report exchanges and judges one that lies off the others with a
//! `ClockVerdict`. Every theta is a replica's clock minus the judging
//! voter's, so a deviation from their median does not depend on which
//! voter judges, the witness included.
//!
//! A replica is judged once its deviation exceeds
//! `max(clockGuard, 4 x MAD)`, MAD the rolling median absolute deviation
//! of its theta, and cleared once the deviation is back within clockGuard.
//! Replicas that self-report (an unsynchronised kernel clock or a step
//! they detected) are compared against the others, never the reverse.
//! With two replicas to compare the median tells nothing: in a set of two
//! audio replicas the witness's clock breaks the tie when it is clean and
//! exactly one replica lies off it; otherwise the replica the latest
//! verdict judged stays judged, the baseline is taken as right when
//! neither was, and `replica_clock_tie` names both.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

use crate::entry::{Body, NodeId};

/// How long the leader waits for a verdict it appended to show in the
/// log before it appends it again.
const INFLIGHT: Duration = Duration::from_secs(1);

/// The exchanges a replica's rolling MAD of theta is taken over.
pub const WINDOW: usize = 64;

/// The latest exchanges with one peer: theta and the one-way delay (half
/// the round trip less the peer's turnaround), ns.
#[derive(Debug, Default, Clone)]
pub(crate) struct Thetas(VecDeque<(i64, u64)>);

impl Thetas {
    pub fn add(&mut self, theta_ns: i64, delay_ns: u64) {
        if self.0.len() == WINDOW {
            self.0.pop_front();
        }
        self.0.push_back((theta_ns, delay_ns));
    }

    /// The window's lowest round trip, the latest of equals: its theta
    /// and its one-way delay, ns. A theta is off the true offset by at
    /// most its exchange's one-way delay, so this exchange gives the
    /// tightest bound, and delay or jitter on later exchanges moves
    /// neither.
    pub fn best(&self) -> Option<(i64, u64)> {
        self.0.iter().rev().copied().min_by_key(|&(_, d)| d)
    }

    /// The median absolute deviation of the window's thetas, ns; 0 when
    /// empty.
    pub fn mad(&self) -> u64 {
        let v: Vec<i64> = self.0.iter().map(|&(t, _)| t).collect();
        let Some(m) = median(&v) else {
            return 0;
        };
        let dev: Vec<i64> = v.iter().map(|&x| (x - m).abs()).collect();
        median(&dev).map_or(0, |d| d as u64)
    }
}

fn median(v: &[i64]) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    let mut v = v.to_vec();
    v.sort_unstable();
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        // Midpoint without overflow.
        v[n / 2 - 1] + (v[n / 2] - v[n / 2 - 1]) / 2
    })
}

/// One voter as the clock check reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reading {
    pub id: NodeId,
    /// Its clock minus the judging voter's from the lowest-round-trip
    /// exchange of the window ([`Thetas::best`]), ns; 0 for the judging
    /// voter, none before a first exchange.
    pub theta_ns: Option<i64>,
    /// Rolling MAD of its theta, ns.
    pub mad_ns: u64,
    /// The one-way delay of that exchange, ns: theta is off the true offset by up to this when the delay is
    /// all one way.
    pub delay_ns: u64,
    /// A clock self-report or a detected-step fence, by its latest copy.
    pub self_report: bool,
    pub verified: bool,
    /// Nothing from it within `w + D`.
    pub silent: bool,
}

impl Reading {
    fn live(&self) -> Option<i64> {
        (self.verified && !self.silent)
            .then_some(self.theta_ns)
            .flatten()
    }
}

/// What the clock check reads in one report period.
#[derive(Debug, Clone)]
pub struct View<'a> {
    /// The audio voters.
    pub audio: Vec<Reading>,
    pub witness: Option<Reading>,
    /// The replicas the latest applied `ClockVerdict` judges.
    pub judged: &'a BTreeSet<NodeId>,
    /// The replica the latest applied `ClockVerdict` judging one named.
    pub latest_judged: Option<NodeId>,
    /// The replica the latest applied `Baseline` names.
    pub baseline: Option<NodeId>,
    /// clockGuard, ns.
    pub guard_ns: u64,
}

/// One audio replica's clock in status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Offset {
    pub replica: NodeId,
    pub verified: bool,
    /// A verified replica's deviation from the median of the verified
    /// ones it is compared against, ns.
    pub deviation_ns: Option<i64>,
    /// An unverified replica's clock minus the witness's, ns; consulted
    /// by nothing.
    pub against_witness_ns: Option<i64>,
    /// Rolling MAD of its theta, ns.
    pub mad_ns: u64,
    /// Its one-way delay bound, ns, added to the threshold it is judged
    /// at.
    pub delay_ns: u64,
}

/// The clock check as status shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub offsets: Vec<Offset>,
    /// `replica_clock_tie`: two replicas apart beyond the guard that the
    /// witness could not tell apart, each with its deviation from their
    /// midpoint, ns.
    pub tie: Option<[(NodeId, i64); 2]>,
    /// `replica_clock_masked`: verified audio replicas whose delay bound
    /// exceeds clockGuard, so an offset up to that bound beyond the guard
    /// goes unjudged.
    pub masked: Vec<NodeId>,
}

/// The outcome of one comparison.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    /// Per replica: judged or cleared.
    pub verdicts: BTreeMap<NodeId, bool>,
    pub status: Status,
}

/// Compares the clocks in `v`; pure, so every voter computes the same
/// status from the same offsets.
pub(crate) fn compare(v: &View) -> Outcome {
    let guard = v.guard_ns;
    let live: Vec<(&Reading, i64)> = v
        .audio
        .iter()
        .filter_map(|r| r.live().map(|t| (r, t)))
        .collect();
    let clean: Vec<(&Reading, i64)> = live
        .iter()
        .copied()
        .filter(|(r, _)| !r.self_report)
        .collect();
    let reference = if clean.is_empty() { &live } else { &clean };
    // A replica is judged only when it lies beyond the threshold even if
    // its exchange's delay was all one way, so a link delayed in one
    // direction does not judge a correct clock.
    let thresh = |r: &Reading| guard.max(4 * r.mad_ns) + r.delay_ns;
    let mid = median(&reference.iter().map(|&(_, t)| t).collect::<Vec<_>>());
    // The witness breaks a tie only in a set of two audio replicas; with
    // more, two left to compare are a tie the witness is not asked about.
    let witness = v
        .witness
        .filter(|w| v.audio.len() == 2 && !w.silent && !w.self_report)
        .and_then(|w| w.theta_ns.map(|t| (t, w.delay_ns)));
    let mut out = Outcome::default();
    for r in &v.audio {
        let deviation_ns = r.live().zip(mid).map(|(t, m)| t - m);
        out.status.offsets.push(Offset {
            replica: r.id,
            verified: r.verified,
            deviation_ns: deviation_ns.filter(|_| live.len() >= 2),
            against_witness_ns: if r.verified {
                None
            } else {
                r.theta_ns
                    .zip(v.witness.and_then(|w| w.theta_ns))
                    .map(|(t, w)| t - w)
            },
            mad_ns: r.mad_ns,
            delay_ns: r.delay_ns,
        });
        if r.verified && !r.silent && r.delay_ns > guard {
            out.status.masked.push(r.id);
        }
    }
    // One live verified replica: nothing is compared, a verdict stands.
    if live.len() < 2 {
        return out;
    }
    let mid = mid.expect("a reference");
    let mut set = |id: NodeId, judged: bool| {
        out.verdicts.insert(id, judged);
    };
    if let [(a, ta), (b, tb)] = reference[..] {
        let apart = ta.abs_diff(tb);
        if apart <= guard {
            set(a.id, false);
            set(b.id, false);
        } else if apart > guard.max(4 * a.mad_ns.max(b.mad_ns)) + a.delay_ns + b.delay_ns {
            let off = |r: &Reading, t: i64| {
                witness.is_some_and(|(w, wd)| t.abs_diff(w) > guard + r.delay_ns + wd)
            };
            match (off(a, ta), off(b, tb)) {
                (true, false) => {
                    set(a.id, true);
                    set(b.id, false);
                }
                (false, true) => {
                    set(a.id, false);
                    set(b.id, true);
                }
                _ => {
                    let wrong = match (v.judged.contains(&a.id), v.judged.contains(&b.id)) {
                        (true, false) => a.id,
                        (false, true) => b.id,
                        // Both judged, as when a third replica left: the
                        // latest verdict decides.
                        (true, true) if v.latest_judged == Some(a.id) => a.id,
                        (true, true) if v.latest_judged == Some(b.id) => b.id,
                        // Neither, or no latest known: the baseline is
                        // taken as right.
                        _ if v.baseline == Some(a.id) => b.id,
                        _ => a.id,
                    };
                    let right = if wrong == a.id { b.id } else { a.id };
                    set(wrong, true);
                    set(right, false);
                    out.status.tie = Some([(a.id, ta - mid), (b.id, tb - mid)]);
                }
            }
        }
    } else if let [(only, t)] = reference[..] {
        // Judged against no other: cleared once every self-reporting
        // replica agrees with it.
        if live.iter().all(|&(_, s)| s.abs_diff(t) <= guard) {
            set(only.id, false);
        }
    } else {
        for &(r, t) in reference.iter() {
            let d = t.abs_diff(mid);
            if d > thresh(r) {
                set(r.id, true);
            } else if d <= guard {
                set(r.id, false);
            }
        }
    }
    // Self-reporting replicas are judged against the reference, which
    // they never move.
    if !clean.is_empty() {
        for &(r, t) in live.iter().filter(|(r, _)| r.self_report) {
            let d = t.abs_diff(mid);
            if d > thresh(r) {
                set(r.id, true);
            } else if d <= guard {
                set(r.id, false);
            }
        }
    }
    out
}

/// The leader's side of the clock check.
#[derive(Debug, Default)]
pub(crate) struct Judge {
    /// Verdicts appended, until the log shows them or [`INFLIGHT`] passes.
    inflight: BTreeMap<NodeId, (bool, Instant)>,
    /// `ClockVerdict` entries appended while this voter led.
    pub verdicts: u64,
}

impl Judge {
    /// The `ClockVerdict` entries to append this period.
    pub fn decide(&mut self, v: &View, now: Instant) -> Vec<Body> {
        self.inflight.retain(|id, (judged, at)| {
            now.duration_since(*at) < INFLIGHT && v.judged.contains(id) != *judged
        });
        let mut out = Vec::new();
        for (id, judged) in compare(v).verdicts {
            if v.judged.contains(&id) == judged || self.inflight.contains_key(&id) {
                continue;
            }
            self.inflight.insert(id, (judged, now));
            self.verdicts += 1;
            out.push(Body::ClockVerdict {
                replica: id,
                judged,
            });
        }
        out
    }

    pub fn step_down(&mut self) {
        self.inflight.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contact::{Timing, time_of};

    const A: NodeId = 1;
    const B: NodeId = 2;
    const C: NodeId = 3;
    const D: NodeId = 4;
    const W: NodeId = 9;

    fn r(id: NodeId, theta: i64) -> Reading {
        Reading {
            id,
            theta_ns: Some(theta),
            mad_ns: 0,
            delay_ns: 0,
            self_report: false,
            verified: true,
            silent: false,
        }
    }

    /// clockGuard at its default for each block size the engine runs,
    /// so no threshold is tied to one block size.
    fn guards() -> Vec<u64> {
        [16, 32, 64, 128]
            .into_iter()
            .map(|b| time_of(Timing::defaults(b).clock_guard))
            .collect()
    }

    fn view<'a>(audio: Vec<Reading>, judged: &'a BTreeSet<NodeId>, guard: u64) -> View<'a> {
        View {
            audio,
            witness: None,
            judged,
            latest_judged: None,
            baseline: None,
            guard_ns: guard,
        }
    }

    fn verdicts(v: &View) -> Vec<(NodeId, bool)> {
        compare(v).verdicts.into_iter().collect()
    }

    #[test]
    fn one_of_three_off_is_judged_and_cleared_within_the_guard() {
        let none = BTreeSet::new();
        for g in guards() {
            let g = g as i64;
            let v = view(vec![r(A, 0), r(B, g / 4), r(C, 3 * g)], &none, g as u64);
            assert_eq!(
                verdicts(&v),
                [(A, false), (B, false), (C, true)],
                "guard {g}"
            );
            // Just beyond the guard is judged; at the guard it clears.
            let v = view(vec![r(A, 0), r(B, 0), r(C, g + 1)], &none, g as u64);
            assert!(compare(&v).verdicts[&C]);
            let v = view(vec![r(A, 0), r(B, 0), r(C, g)], &none, g as u64);
            assert!(!compare(&v).verdicts[&C]);
        }
    }

    #[test]
    fn a_noisy_offset_widens_the_threshold_to_four_mad() {
        let none = BTreeSet::new();
        for g in guards() {
            let gi = g as i64;
            // MAD of g/2: 4 MAD = 2 guard, so 1.5 guard off is neither
            // judged nor cleared.
            let mut c = r(C, 3 * gi / 2);
            c.mad_ns = g / 2;
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert!(!compare(&v).verdicts.contains_key(&C), "guard {g}");
            c.theta_ns = Some(2 * gi + 1);
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert!(compare(&v).verdicts[&C]);
        }
    }

    #[test]
    fn an_offset_a_one_way_delay_explains_is_not_judged() {
        let none = BTreeSet::new();
        for g in guards() {
            let gi = g as i64;
            // Reports delayed 4 guards one way read as 2 guards off.
            let mut c = r(C, 2 * gi);
            c.delay_ns = 2 * g;
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert!(!compare(&v).verdicts.contains_key(&C), "guard {g}");
            // Beyond what the delay explains it is.
            c.theta_ns = Some(3 * gi + 1);
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert!(compare(&v).verdicts[&C]);
            // Of two, the same.
            let mut b = r(B, 2 * gi);
            b.delay_ns = 2 * g;
            let v = view(vec![r(A, 0), b], &none, g);
            assert!(compare(&v).verdicts.is_empty(), "guard {g}");
        }
    }

    #[test]
    fn the_rolling_mad_follows_the_window() {
        let mut t = Thetas::default();
        assert_eq!(t.mad(), 0);
        for i in 0..WINDOW as i64 {
            t.add(if i % 2 == 0 { 100 } else { -100 }, 0);
        }
        assert_eq!(t.mad(), 100);
        // A steady offset is no noise: a full window of it sets MAD 0.
        for _ in 0..WINDOW {
            t.add(5_000_000, 0);
        }
        assert_eq!(t.mad(), 0);
    }

    #[test]
    fn the_offset_and_delay_come_from_the_lowest_round_trip_in_the_window() {
        let mut t = Thetas::default();
        assert_eq!(t.best(), None);
        t.add(7, 3_000);
        // Later exchanges slower, their theta skewed by the one-way part.
        for i in 0..WINDOW as i64 - 1 {
            t.add(10_000 + i, 10_000 + i as u64);
        }
        assert_eq!(t.best(), Some((7, 3_000)));
        // Once the fast exchange leaves the window, the next lowest.
        t.add(50_000, 50_000);
        assert_eq!(t.best(), Some((10_000, 10_000)));
        // Of equal round trips the latest.
        t.add(-1, 10_000);
        assert_eq!(t.best(), Some((-1, 10_000)));
    }

    #[test]
    fn a_delay_bound_beyond_the_guard_raises_the_masked_alarm() {
        let none = BTreeSet::new();
        for g in guards() {
            let mut c = r(C, 0);
            c.delay_ns = g;
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            let o = compare(&v);
            assert!(o.status.masked.is_empty(), "guard {g}");
            assert_eq!(o.status.offsets[2].delay_ns, g);
            c.delay_ns = g + 1;
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert_eq!(compare(&v).status.masked, [C], "guard {g}");
            // An unverified or silent replica is compared with nothing.
            c.verified = false;
            let v = view(vec![r(A, 0), r(B, 0), c], &none, g);
            assert!(compare(&v).status.masked.is_empty());
        }
    }

    #[test]
    fn a_self_reporting_replica_of_two_is_judged_never_the_other() {
        let none = BTreeSet::new();
        for g in guards() {
            let mut b = r(B, 3 * g as i64);
            b.self_report = true;
            let v = view(vec![r(A, 0), b], &none, g);
            assert_eq!(verdicts(&v), [(B, true)]);
            // The same with A judging from B's frame.
            let mut b = r(B, 0);
            b.self_report = true;
            let v = view(vec![r(A, -3 * g as i64), b], &none, g);
            assert_eq!(verdicts(&v), [(B, true)]);
        }
    }

    #[test]
    fn both_self_reporting_within_the_guard_neither_is_judged() {
        let judged = BTreeSet::from([B]);
        for g in guards() {
            let (mut a, mut b) = (r(A, 0), r(B, g as i64));
            a.self_report = true;
            b.self_report = true;
            let v = view(vec![a, b], &judged, g);
            assert_eq!(verdicts(&v), [(A, false), (B, false)]);
            assert_eq!(compare(&v).status.tie, None);
        }
    }

    #[test]
    fn two_apart_the_clean_witness_picks_the_one_off_it() {
        let none = BTreeSet::new();
        for g in guards() {
            let gi = g as i64;
            for self_report in [false, true] {
                let (mut a, mut b) = (r(A, 0), r(B, 3 * gi));
                a.self_report = self_report;
                b.self_report = self_report;
                let mut v = view(vec![a, b], &none, g);
                v.witness = Some(r(W, gi / 2));
                assert_eq!(verdicts(&v), [(A, false), (B, true)]);
                v.witness = Some(r(W, 3 * gi));
                assert_eq!(verdicts(&v), [(A, true), (B, false)]);
                assert_eq!(compare(&v).status.tie, None);
            }
        }
    }

    #[test]
    fn two_apart_without_the_witness_keep_the_judged_one_and_alarm() {
        for g in guards() {
            let gi = g as i64;
            let none = BTreeSet::new();
            // A self-reporting witness, and one equally far from both,
            // tell nothing.
            let mut ws = r(W, 0);
            ws.self_report = true;
            for witness in [None, Some(ws), Some(r(W, 3 * gi / 2)), Some(r(W, 9 * gi))] {
                let mut v = view(vec![r(A, 0), r(B, 3 * gi)], &none, g);
                v.witness = witness;
                v.baseline = Some(B);
                // Neither judged: the baseline is taken as right.
                let o = compare(&v);
                assert_eq!(
                    o.verdicts.into_iter().collect::<Vec<_>>(),
                    [(A, true), (B, false)]
                );
                let half = 3 * gi / 2;
                assert_eq!(o.status.tie, Some([(A, -half), (B, 3 * gi - half)]));
                // The one judged before stays judged, whatever the baseline.
                let judged = BTreeSet::from([B]);
                v.judged = &judged;
                v.baseline = Some(A);
                let o = compare(&v);
                assert_eq!(
                    o.verdicts.into_iter().collect::<Vec<_>>(),
                    [(A, false), (B, true)]
                );
                assert!(o.status.tie.is_some());
            }
        }
    }

    #[test]
    fn of_two_both_judged_only_the_latest_verdict_stays() {
        // A set of three shrinks to two with both judged: output must not
        // stop on both.
        let both = BTreeSet::from([A, B]);
        for g in guards() {
            let gi = g as i64;
            for latest in [A, B] {
                let mut v = view(vec![r(A, 0), r(B, 3 * gi)], &both, g);
                v.latest_judged = Some(latest);
                v.baseline = Some(latest);
                let o = compare(&v);
                let right = if latest == A { B } else { A };
                assert!(o.verdicts[&latest], "latest {latest}");
                assert!(!o.verdicts[&right], "latest {latest}");
            }
            // No latest known: the baseline is taken as right.
            let mut v = view(vec![r(A, 0), r(B, 3 * gi)], &both, g);
            v.baseline = Some(B);
            let o = compare(&v);
            assert!(o.verdicts[&A] && !o.verdicts[&B]);
        }
    }

    #[test]
    fn the_witness_is_not_asked_with_more_than_two_audio_replicas() {
        let none = BTreeSet::new();
        for g in guards() {
            let gi = g as i64;
            // Four audio replicas, two self-reporting: the two clean ones
            // apart, the witness's clock on A's. It picks nothing.
            let (mut c, mut d) = (r(C, 0), r(D, 0));
            c.self_report = true;
            d.self_report = true;
            let mut v = view(vec![r(A, 0), r(B, 3 * gi), c, d], &none, g);
            v.witness = Some(r(W, 0));
            v.baseline = Some(B);
            let o = compare(&v);
            assert!(o.verdicts[&A] && !o.verdicts[&B], "{:?}", o.verdicts);
            assert!(o.status.tie.is_some());
            // The same with two audio replicas: the witness picks B.
            let mut v = view(vec![r(A, 0), r(B, 3 * gi)], &none, g);
            v.witness = Some(r(W, 0));
            v.baseline = Some(B);
            assert!(compare(&v).verdicts[&B]);
        }
    }

    #[test]
    fn unverified_and_silent_replicas_enter_no_comparison() {
        let judged = BTreeSet::from([C]);
        for g in guards() {
            let gi = g as i64;
            let mut c = r(C, 3 * gi);
            c.verified = false;
            let mut v = view(vec![r(A, 0), r(B, 3 * gi), c], &judged, g);
            v.witness = Some(r(W, 0));
            let o = compare(&v);
            // Two left of three: a tie the witness is not asked about; C
            // is never compared.
            assert_eq!(
                o.verdicts.into_iter().collect::<Vec<_>>(),
                [(A, true), (B, false)]
            );
            let c = o.status.offsets[2];
            assert_eq!((c.deviation_ns, c.against_witness_ns), (None, Some(3 * gi)));
            // One live replica: a verdict stands, nothing is cleared.
            let mut b = r(B, 0);
            b.silent = true;
            let v = view(vec![r(A, 0), b, r(C, 0)], &judged, g);
            assert!(compare(&v).verdicts.contains_key(&C));
            let mut c = r(C, 0);
            c.silent = true;
            let v = view(vec![r(A, 0), r(B, 0), c], &judged, g);
            assert!(!compare(&v).verdicts.contains_key(&C));
            let v = view(vec![r(A, 0), c], &judged, g);
            assert!(compare(&v).verdicts.is_empty());
        }
    }

    #[test]
    fn the_judge_appends_a_verdict_once_until_the_log_shows_it() {
        let none = BTreeSet::new();
        let g = time_of(Timing::defaults(64).clock_guard);
        let v = view(vec![r(A, 0), r(B, 0), r(C, 3 * g as i64)], &none, g);
        let mut j = Judge::default();
        let t = Instant::now();
        let want = vec![Body::ClockVerdict {
            replica: C,
            judged: true,
        }];
        assert_eq!(j.decide(&v, t), want);
        assert!(j.decide(&v, t + Duration::from_millis(10)).is_empty());
        assert_eq!(j.decide(&v, t + INFLIGHT), want);
        let judged = BTreeSet::from([C]);
        let v = view(vec![r(A, 0), r(B, 0), r(C, 3 * g as i64)], &judged, g);
        assert!(j.decide(&v, t + 3 * INFLIGHT).is_empty());
        assert_eq!(j.verdicts, 2);
    }
}
