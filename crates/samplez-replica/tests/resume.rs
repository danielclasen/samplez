//! Baselines, the Named rule, Resume on page hashes and the leader's page
//! verdicts in an in-process set (plan 2.11, Replica states, Withholding
//! and divergence, Baseline): every audio replica's engine is a
//! [`harness::FakeAudio`] whose outputs are real hold rings, each sample
//! committed carrying its own index, and whose pages are hashed as the
//! engine hashes them. Nothing names a commit start by hand: the set
//! starts committing through the leader's first `Baseline`.

mod harness;

use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez::blocks::{PAGE, Standing};
use samplez_replica::contact::Timing;
use samplez_replica::entry::Request;
use samplez_replica::entry::{Baseline, Body, NodeId};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::state::ReplicaState;
use samplez_service::ReplicatedState;
use tokio::time::{Instant, sleep};

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

const DEGRADED: u64 = 0xd1;

async fn start(ids: Vec<NodeId>, outputs: usize) -> Set {
    Set::start(Options {
        ids: Some(ids),
        contact: Some(timing()),
        outputs,
        ..Options::default()
    })
    .await
}

fn audio(set: &Set) -> Vec<NodeId> {
    set.running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

/// Waits, in virtual time, until `f` holds; fails naming `what` after
/// `secs` seconds.
async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {secs} s");
        sleep(Duration::from_millis(1)).await;
    }
}

fn state(set: &Set, id: NodeId) -> Option<ReplicaState> {
    set.replica(id).contact_metrics()?.replica?.state
}

fn standing(set: &Set, id: NodeId, o: usize) -> Standing {
    set.audio(id).step();
    set.audio(id).lock().outputs[o].statuses.standing()
}

fn from(set: &Set, id: NodeId, o: usize) -> Option<u64> {
    match standing(set, id, o) {
        Standing::Committing { from } => Some(from),
        _ => None,
    }
}

fn committing(set: &Set, id: NodeId) -> bool {
    state(set, id) == Some(ReplicaState::Committing)
}

/// The latest `Baseline` `id` applied.
fn baseline(set: &Set, id: NodeId) -> Option<Baseline> {
    set.replica(id).store().with_state(|sm| sm.baseline.clone())
}

/// The `Baseline` entries in `id`'s log, in order.
fn baselines(set: &Set, id: NodeId) -> Vec<Baseline> {
    let mut out = Vec::new();
    set.replica(id)
        .store()
        .entries_after(0, &mut |_, request: &Request| {
            if let Body::Baseline(b) = &request.body {
                out.push(b.clone());
            }
        });
    out
}

fn diverged_entries(set: &Set, id: NodeId) -> Vec<(NodeId, u64)> {
    let mut out = Vec::new();
    set.replica(id)
        .store()
        .entries_after(0, &mut |_, request: &Request| {
            if let Body::Diverged { replica, page } = request.body {
                out.push((replica, page));
            }
        });
    out
}

/// The newest block due on `id`'s clock.
fn due(set: &Set, id: NodeId) -> u64 {
    set.audio(id).due().unwrap()
}

/// Every page `[from, to)` of output `o` is whole on one of `ids`.
fn pages_whole(set: &Set, ids: &[NodeId], o: usize, from: u64, to: u64) {
    let writes: Vec<_> = ids.iter().map(|&id| set.audio(id).writes(o)).collect();
    for page in from..to {
        let whole = writes
            .iter()
            .any(|w| (page * PAGE..(page + 1) * PAGE).all(|i| w.has(i)));
        assert!(whole, "page {page} of output {o} whole on no member");
    }
}

fn metrics(set: &Set, id: NodeId) -> samplez_replica::state::Metrics {
    set.replica(id)
        .contact_metrics()
        .and_then(|m| m.replica)
        .expect("an audio replica")
}

/// The leader's decisions, summed over every voter that led.
fn led(set: &Set) -> samplez_replica::baseline::Counts {
    let mut c = samplez_replica::baseline::Counts::default();
    for id in set.running() {
        let l = set.replica(id).contact_metrics().unwrap().leader;
        c.named += l.named;
        c.renamed += l.renamed;
        c.verdicts += l.verdicts;
        c.disagreements += l.disagreements;
    }
    c
}

/// A cold start of two audio replicas plus the witness and of three: the
/// leader's first `Baseline` names one ready replica, which commits from
/// P, the first page boundary at or after the entry's at_index, a partial
/// first commit when P lies inside a block; the others resume on a page
/// hash match from a later page boundary, and every page from P on is
/// whole on a member.
#[tokio::test(start_paused = true)]
async fn a_cold_start_names_one_replica_and_the_others_resume_on_a_hash_match() {
    for ids in [vec![a(0), a(1), witness()], vec![a(0), a(1), a(2)]] {
        let set = start(ids, 1).await;
        let ids = audio(&set);
        until("every audio replica committing", 5, || {
            ids.iter().all(|&id| committing(&set, id))
        })
        .await;
        let b = baseline(&set, ids[0]).expect("a Baseline applied");
        assert_eq!(baselines(&set, ids[0]).len(), 1, "one naming");
        // Only the leader decides: followers count no decision.
        assert_eq!(led(&set).named, 1, "decisions counted on followers");
        let p = b.at_index.next_multiple_of(PAGE);
        // Its first write starts at P, inside the block holding P unless
        // P is that block's start: `committed from P`.
        let first = set.audio(b.replica).writes(0).0[0].0;
        assert_eq!(first, p, "the named one's first commit");
        assert_eq!(from(&set, b.replica, 0), Some(p), "the named one from P");
        for &id in &ids {
            if id != b.replica {
                let f = from(&set, id, 0).unwrap();
                assert!(f > p && f.is_multiple_of(PAGE), "{id} from {f}, P {p}");
                assert_eq!(metrics(&set, id).resumes, 1);
            }
            assert_eq!(set.audio(id).lock().commit_starts.len(), 1, "{id}");
        }
        sleep(Duration::from_millis(100)).await;
        let now = due(&set, ids[0]) * BLOCK / PAGE - 2;
        pages_whole(&set, &ids, 0, p / PAGE, now);
        for &id in &ids {
            assert!(
                set.replica(id).replica().ready,
                "{id} not Ready while committing"
            );
        }
    }
}

/// A restart of the whole set: every audio replica commits again, and
/// the member flows hold every page whole again from a page boundary on.
#[tokio::test(start_paused = true)]
async fn a_whole_set_restart_commits_again() {
    let mut set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let all: Vec<NodeId> = set.ids().collect();
    for &id in &all {
        set.crash(id).await;
    }
    for &id in &all {
        set.restart(id).await;
    }
    set.leader().await;
    until("committing after the restart", 10, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let first = ids
        .iter()
        .map(|&id| from(&set, id, 0).unwrap())
        .min()
        .unwrap();
    sleep(Duration::from_millis(100)).await;
    let now = due(&set, ids[0]) * BLOCK / PAGE - 2;
    pages_whole(&set, &ids, 0, first / PAGE, now);
}

/// `replicas: 1` commits once ready, also after a restart, and is Ready.
#[tokio::test(start_paused = true)]
async fn a_single_replica_commits_once_ready_also_after_a_restart() {
    let mut set = start(vec![a(0)], 1).await;
    until("committing", 5, || committing(&set, a(0))).await;
    assert!(set.replica(a(0)).replica().ready);
    // Without waiting for its entry: from before the Baseline's index.
    until("its Baseline applied", 5, || baseline(&set, a(0)).is_some()).await;
    let b = baseline(&set, a(0)).unwrap();
    let f = from(&set, a(0), 0).unwrap();
    assert!(f < b.at_index, "from {f}, the Baseline at {}", b.at_index);
    set.crash(a(0)).await;
    set.restart(a(0)).await;
    set.leader().await;
    until("committing after the restart", 5, || committing(&set, a(0))).await;
    assert!(set.replica(a(0)).replica().ready);
}

/// An input glitch on one of two audio replicas, a stateless shape: it
/// withholds on the other's committed report, withdraws, resumes on its
/// own page-hash match within three pages, and the samples its member
/// skipped are counted in `resume_gap_samples`. It is not Ready while
/// withdrawn and the other commits.
#[tokio::test(start_paused = true)]
async fn a_glitch_on_one_replica_withdraws_it_until_it_resumes_on_its_hashes() {
    let set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let other = ids.iter().copied().find(|&id| id != named).unwrap();
    let k = due(&set, other) + 30;
    set.audio(other).degrade(k, k + 4, DEGRADED);
    until("withdrawn", 2, || {
        state(&set, other) == Some(ReplicaState::Withdrawn)
    })
    .await;
    assert!(
        !set.replica(other).replica().ready,
        "a demoted replica is not Ready while another commits"
    );
    until("resumed", 2, || committing(&set, other)).await;
    let f = from(&set, other, 0).unwrap();
    assert!(
        f <= (k + 4) * BLOCK + 3 * PAGE + PAGE,
        "resumed from {f}, the glitch ended at {}",
        (k + 4) * BLOCK
    );
    let m = metrics(&set, other);
    assert_eq!((m.resumes, m.divergences), (2, 0), "{m:?}");
    let w = set.audio(other).writes(0);
    let end =
        w.0.iter()
            .map(|&(s, n)| s + n as u64)
            .filter(|&e| e <= f)
            .max()
            .unwrap();
    assert_eq!(m.resume_gap_samples, f - end);
    assert!(m.withdrawn_seconds_total > 0.0);
    assert!(set.replica(other).replica().ready, "Ready again");
}

/// A replica whose content differs from the others' without knowing it
/// (equal digests, unequal page hashes): among three, the leader's
/// verdict diverges the minority, which withdraws and never resumes on a
/// hash match; a non-diverged demoted replica then ranks above it.
#[tokio::test(start_paused = true)]
async fn the_leader_diverges_a_minority_hash() {
    let set = start(vec![a(0), a(1), a(2)], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let odd = ids.iter().copied().find(|&id| id != named).unwrap();
    let k = due(&set, odd) + 30;
    set.audio(odd).glitch(k, k + 2, 0.5);
    until("diverged", 2, || {
        state(&set, odd) == Some(ReplicaState::Diverged)
    })
    .await;
    let leader = set.leader().await;
    assert!(
        diverged_entries(&set, leader)
            .iter()
            .any(|&(r, _)| r == odd),
        "a Diverged entry names {odd}"
    );
    assert_eq!(baseline(&set, leader).unwrap().replica, named, "no rename");
    sleep(Duration::from_millis(200)).await;
    assert_eq!(state(&set, odd), Some(ReplicaState::Diverged), "no resume");
    assert_eq!(metrics(&set, odd).divergences, 1, "applied once");
    // A demoted replica that is not diverged fills above it.
    let third = ids
        .iter()
        .copied()
        .find(|&id| id != named && id != odd)
        .unwrap();
    let k = due(&set, third) + 30;
    set.audio(third).degrade(k, k + 4, DEGRADED);
    until("withdrawn", 2, || {
        state(&set, third) == Some(ReplicaState::Withdrawn)
    })
    .await;
    sleep(Duration::from_millis(20)).await;
    let fill = |id| set.audio(id).lock().ranks.last().unwrap().1.fill;
    assert!(fill(third) < fill(odd), "{} vs {}", fill(third), fill(odd));
}

/// Three-way disagreement among three audio replicas: the baseline's hash
/// wins, the other two are diverged, and `replica_disagreement` alarms.
#[tokio::test(start_paused = true)]
async fn a_three_way_disagreement_follows_the_baseline() {
    let set = start(vec![a(0), a(1), a(2)], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let others: Vec<NodeId> = ids.iter().copied().filter(|&id| id != named).collect();
    let k = due(&set, named) + 30;
    set.audio(others[0]).glitch(k, k + 2, 0.5);
    set.audio(others[1]).glitch(k, k + 2, 0.25);
    until("both diverged", 2, || {
        others
            .iter()
            .all(|&id| state(&set, id) == Some(ReplicaState::Diverged))
    })
    .await;
    assert!(committing(&set, named));
    assert!(led(&set).disagreements >= 1);
    let leader = set.leader().await;
    assert_eq!(baseline(&set, leader).unwrap().replica, named);
}

/// A strict majority against the baseline diverges it and renames the
/// baseline to the highest-ranked committing replica.
#[tokio::test(start_paused = true)]
async fn a_majority_against_the_baseline_renames_it() {
    let set = start(vec![a(0), a(1), a(2)], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let k = due(&set, named) + 30;
    set.audio(named).glitch(k, k + 2, 0.5);
    until("diverged", 2, || {
        state(&set, named) == Some(ReplicaState::Diverged)
    })
    .await;
    let leader = set.leader().await;
    until("renamed", 2, || {
        baseline(&set, leader).is_some_and(|b| b.replica != named)
    })
    .await;
    let next = baseline(&set, leader).unwrap().replica;
    assert!(committing(&set, next));
    assert!(led(&set).renamed >= 1);
}

/// The leader killed while it is also the baseline among three audio
/// replicas: a survivor keeps committing, and the new leader renames the
/// silent baseline to a committing survivor.
#[tokio::test(start_paused = true)]
async fn a_killed_leader_and_baseline_is_renamed_to_a_survivor() {
    let mut set = start(vec![a(0), a(1), a(2)], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let leader = set.leader().await;
    if baseline(&set, leader).unwrap().replica != leader {
        set.replica(leader)
            .append(Body::Baseline(Baseline {
                replica: leader,
                order: Vec::new(),
                at_index: 0,
                key: None,
            }))
            .unwrap();
        until("the leader named", 2, || {
            ids.iter()
                .all(|&id| baseline(&set, id).is_some_and(|b| b.replica == leader))
        })
        .await;
    }
    set.crash(leader).await;
    let survivors: Vec<NodeId> = ids.iter().copied().filter(|&id| id != leader).collect();
    let next = set.leader().await;
    until("renamed", 3, || {
        baseline(&set, next).is_some_and(|b| b.replica != leader)
    })
    .await;
    let b = baseline(&set, next).unwrap();
    assert!(survivors.contains(&b.replica));
    assert!(survivors.iter().all(|&id| committing(&set, id)));
}

/// The only committing replica stops while the other just restarted: the
/// restarted one, idle since its start, fills nothing (it would add a
/// member, plan 2.11 Replica states, `withdrawn` row) until the leader
/// names it under the no-committer rule, and then commits from P.
#[tokio::test(start_paused = true)]
async fn a_replica_withdrawn_since_its_start_fills_nothing_until_named() {
    let mut set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let other = ids.iter().copied().find(|&id| id != named).unwrap();
    set.crash(other).await;
    set.restart(other).await;
    until("withdrawn", 3, || {
        state(&set, other) == Some(ReplicaState::Withdrawn)
    })
    .await;
    // Stopped by a kill before it could resume.
    set.audio(named).lock().frozen = true;
    set.crash(named).await;
    until("named", 5, || {
        baseline(&set, other).is_some_and(|b| b.replica == other)
    })
    .await;
    until("committing", 3, || committing(&set, other)).await;
    let b = baseline(&set, other).unwrap();
    let p = b.at_index.next_multiple_of(PAGE);
    assert_eq!(from(&set, other, 0), Some(p));
    let w = set.audio(other).writes(0);
    assert!(
        w.0.iter().all(|&(s, _)| s >= p),
        "wrote below its commit start {p}: {:?}",
        &w.0[..w.0.len().min(4)]
    );
}

/// An output unrealised on one replica: its standing there is idle while
/// the other replica commits it, its other output keeps committing; once
/// realised again it commits that output from a page boundary after a
/// hash match.
#[tokio::test(start_paused = true)]
async fn an_output_unrealised_on_one_replica_resumes_once_realised() {
    let set = start(vec![a(0), a(1), witness()], 2).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter()
            .all(|&id| from(&set, id, 0).is_some() && from(&set, id, 1).is_some())
    })
    .await;
    set.audio(a(1)).unrealise(1);
    sleep(Duration::from_millis(100)).await;
    assert!(matches!(standing(&set, a(1), 1), Standing::Idle { .. }));
    assert!(committing(&set, a(1)), "its other output commits");
    assert!(from(&set, a(0), 1).is_some(), "a0 commits output 1");
    let k = due(&set, a(1));
    set.audio(a(1)).realise(1);
    until("output 1 resumed", 2, || from(&set, a(1), 1).is_some()).await;
    let f = from(&set, a(1), 1).unwrap();
    assert!(f > k * BLOCK && f.is_multiple_of(PAGE));
}

/// An output unrealised on a0 while a1 is down: nobody commits it; on its
/// return a1 resumes its other output on a hash match and commits that
/// one by the per-output rule, no leader involved.
#[tokio::test(start_paused = true)]
async fn the_per_output_rule_commits_an_output_nobody_commits() {
    let mut set = start(vec![a(0), a(1), witness()], 2).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter()
            .all(|&id| from(&set, id, 0).is_some() && from(&set, id, 1).is_some())
    })
    .await;
    set.crash(a(1)).await;
    set.audio(a(0)).unrealise(1);
    sleep(Duration::from_millis(50)).await;
    let decided = led(&set);
    set.restart(a(1)).await;
    until("a1 commits both outputs", 5, || {
        from(&set, a(1), 0).is_some() && from(&set, a(1), 1).is_some()
    })
    .await;
    assert!(matches!(standing(&set, a(0), 1), Standing::Idle { .. }));
    assert_eq!(metrics(&set, a(1)).resumes, 1, "output 0 resumed");
    let after = led(&set);
    assert_eq!(after.named, decided.named, "no Baseline for it");
}

/// The audio `/readyz` term out of contact: a replica Ready when it lost
/// contact stays Ready; one never in contact since its start is not.
#[tokio::test(start_paused = true)]
async fn readiness_is_kept_out_of_contact_and_false_before_any() {
    let mut set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let all: Vec<NodeId> = set.ids().collect();
    set.net.isolate(a(1), all.iter().copied());
    sleep(Duration::from_millis(300)).await;
    assert!(
        set.replica(a(1)).replica().ready,
        "Ready when it lost contact"
    );
    set.crash(a(1)).await;
    set.restart(a(1)).await;
    sleep(Duration::from_millis(300)).await;
    assert!(
        !set.replica(a(1)).replica().ready,
        "Ready without contact since its start"
    );
    set.net.heal();
    until("Ready once in contact and committing", 5, || {
        set.replica(a(1)).replica().ready
    })
    .await;
}

/// A `Diverged` verdict is the leader's: while no leader can be elected a
/// replica whose hash lost commits on and no verdict enters the log;
/// once a leader is elected it is decided. A verdict on a page before the
/// replica's commit start is ignored, and one applied twice counts once.
#[tokio::test(start_paused = true)]
async fn a_verdict_waits_for_a_leader_and_a_stale_one_is_ignored() {
    let ids = vec![a(0), a(1), a(2), a(3), witness()];
    let mut set = start(ids, 1).await;
    let audio_ids = audio(&set);
    until("committing", 5, || {
        audio_ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let leader = set.leader().await;
    set.crash(leader).await;
    let rest: Vec<NodeId> = set.running().collect();
    for &x in &rest {
        for &y in &rest {
            if x < y {
                set.net.cut_raft(x, y);
            }
        }
    }
    let odd = rest
        .iter()
        .copied()
        .find(|&id| slot_of(id) != Slot::Witness && id != named)
        .unwrap();
    let k = due(&set, odd) + 30;
    set.audio(odd).glitch(k, k + 2, 0.5);
    sleep(Duration::from_millis(150)).await;
    assert!(committing(&set, odd), "no verdict without a leader");
    assert!(diverged_entries(&set, odd).is_empty());
    set.net.heal();
    until("diverged", 5, || {
        state(&set, odd) == Some(ReplicaState::Diverged)
    })
    .await;

    // Stale: a page before a committing replica's commit start.
    let healthy = rest
        .iter()
        .copied()
        .find(|&id| committing(&set, id))
        .unwrap();
    let f = from(&set, healthy, 0).unwrap();
    let leader = set.leader().await;
    set.replica(leader)
        .append(Body::Diverged {
            replica: healthy,
            page: f / PAGE - 1,
        })
        .unwrap();
    set.replica(leader)
        .append(Body::Diverged {
            replica: odd,
            page: k * BLOCK / PAGE,
        })
        .unwrap();
    until("both verdicts in every log", 2, || {
        [healthy, odd]
            .iter()
            .all(|&id| diverged_entries(&set, id).len() >= 3)
    })
    .await;
    sleep(Duration::from_millis(20)).await;
    assert!(committing(&set, healthy), "a stale verdict diverged it");
    assert_eq!(metrics(&set, odd).divergences, 1, "applied twice");
}

/// A withdrawn replica whose page hashes equal the committing replica's
/// while its entry fingerprints differ (it applied other entries, the
/// difference inaudible on the page) does not resume: it is diverged.
#[tokio::test(start_paused = true)]
async fn unequal_entry_fingerprints_block_a_resume() {
    let set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let other = ids.iter().copied().find(|&id| id != named).unwrap();
    let k = due(&set, other) + 30;
    let page = k * BLOCK / PAGE;
    set.audio(other)
        .lock()
        .fingerprints
        .push((page, page + 1000, 9));
    set.audio(other).degrade(k, k + 4, DEGRADED);
    until("diverged", 2, || {
        state(&set, other) == Some(ReplicaState::Diverged)
    })
    .await;
    assert_eq!(metrics(&set, other).resumes, 1, "only its first resume");
    assert!(
        diverged_entries(&set, named).is_empty(),
        "by its own Resume, not a leader verdict"
    );
}

/// Quorum lost and regained: every member stops (nobody passes B1), and
/// once the links heal the leader names a baseline under the
/// no-committer rule and both audio replicas commit again.
#[tokio::test(start_paused = true)]
async fn quorum_regained_restarts_output_through_a_new_baseline() {
    let set = start(vec![a(0), a(1), witness()], 1).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let named = led(&set).named;
    let all: Vec<NodeId> = set.ids().collect();
    set.net.cut_all(all.iter().copied());
    until("every member stopped", 3, || {
        ids.iter().all(|&id| !committing(&set, id))
    })
    .await;
    sleep(Duration::from_millis(500)).await;
    let stopped = due(&set, ids[0]);
    set.net.heal();
    until("committing again", 10, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    assert!(led(&set).named > named, "named under the no-committer rule");
    for &id in &ids {
        assert!(from(&set, id, 0).unwrap() > stopped * BLOCK);
    }
}
