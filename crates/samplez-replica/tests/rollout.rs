//! Key hand-over in a rollout in an in-process set (plan 2.11, Baseline,
//! Replica states): a rollout that changes the compatibility key moves
//! the baseline to a target-revision replica at P, the old-key replicas
//! commit up to P and fill only from it, and replicas with another key
//! commit nothing. Every audio replica's engine is a
//! [`harness::FakeAudio`] whose outputs are real hold rings.

mod harness;

use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez::blocks::{Cause, PAGE, Standing};
use samplez_replica::contact::{Timing, index_at};
use samplez_replica::entry::{Baseline, Body, NodeId};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::state::ReplicaState;
use samplez_service::ReplicatedState;
use tokio::time::{Instant, sleep};

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

async fn start(ids: Vec<NodeId>) -> Set {
    Set::start(Options {
        ids: Some(ids),
        contact: Some(Timing::defaults(BLOCK)),
        outputs: 1,
        ..Options::default()
    })
    .await
}

fn audio(set: &Set) -> Vec<NodeId> {
    set.running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

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

fn committing(set: &Set, id: NodeId) -> bool {
    state(set, id) == Some(ReplicaState::Committing)
}

fn from(set: &Set, id: NodeId) -> Option<u64> {
    set.audio(id).step();
    match set.audio(id).lock().outputs[0].statuses.standing() {
        Standing::Committing { from } => Some(from),
        _ => None,
    }
}

fn baseline(set: &Set, id: NodeId) -> Option<Baseline> {
    set.replica(id).store().with_state(|sm| sm.baseline.clone())
}

fn metrics(set: &Set, id: NodeId) -> samplez_replica::state::Metrics {
    set.replica(id)
        .contact_metrics()
        .and_then(|m| m.replica)
        .expect("an audio replica")
}

fn handed_over(set: &Set) -> u64 {
    set.running()
        .map(|id| {
            set.replica(id)
                .contact_metrics()
                .unwrap()
                .leader
                .handed_over
        })
        .sum()
}

/// The page holding the newest block due on `id`'s clock.
fn page_now(set: &Set, id: NodeId) -> u64 {
    set.audio(id).due().unwrap() * BLOCK / PAGE
}

/// Every page `[from, to)` is whole on one of `ids`.
fn pages_whole(set: &Set, ids: &[NodeId], from: u64, to: u64) {
    let writes: Vec<_> = ids.iter().map(|&id| set.audio(id).writes(0)).collect();
    for page in from..to {
        let whole = writes
            .iter()
            .any(|w| (page * PAGE..(page + 1) * PAGE).all(|i| w.has(i)));
        assert!(whole, "page {page} whole on no member");
    }
}

/// No sample of `[from, to)` written by `id`.
fn writes_none(set: &Set, id: NodeId, from: u64, to: u64) {
    let w = set.audio(id).writes(0);
    assert!(
        !w.0.iter().any(|&(s, n)| s < to && s + n as u64 > from),
        "{id} wrote into [{from}, {to})"
    );
}

/// The `/readyz` term.
fn ready(set: &Set, id: NodeId) -> bool {
    set.replica(id).replica().ready
}

/// Starts `ids`, waits for every audio replica committing, and rolls the
/// highest ordinal to key 2 at revision `r2`, the target.
async fn roll_last(ids: Vec<NodeId>) -> (Set, Vec<NodeId>, NodeId) {
    roll(ids, true).await
}

/// As [`roll_last`]; without `target` the StatefulSet reports no target
/// revision yet, so nothing is handed over.
async fn roll(ids: Vec<NodeId>, target: bool) -> (Set, Vec<NodeId>, NodeId) {
    let mut set = start(ids).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let n = *ids.last().unwrap();
    if target {
        set.set_target(Some("r2"));
    }
    set.set_version(n, 2, "r2");
    set.restart(n).await;
    (set, ids, n)
}

async fn named(set: &Set, at: NodeId, n: NodeId) -> u64 {
    until("the target-revision replica named", 10, || {
        baseline(set, at).is_some_and(|b| b.replica == n && b.key == Some(2))
    })
    .await;
    baseline(set, at).unwrap().at_index.next_multiple_of(PAGE)
}

/// A rollout that changes the key, of two audio replicas plus the witness
/// and of three: the restarted replica N, its key differing from the
/// baseline's, is `version_mismatch` and commits nothing until the leader
/// names it; the hand-over is at P, a block boundary; N commits from P,
/// counting one discontinuity in `version_switch_total`; every old-key
/// replica commits up to P and from P fills only, so every page stays
/// whole on a member. Old-key fillers are not Ready while N commits, and
/// turn `version_mismatch` once a second replica with N's key commits.
#[tokio::test(start_paused = true)]
async fn a_key_changing_rollout_hands_over_at_p() {
    for ids in [vec![a(0), a(1), witness()], vec![a(0), a(1), a(2)]] {
        let (mut set, ids, n) = roll_last(ids).await;
        let old: Vec<NodeId> = ids.iter().copied().filter(|&id| id != n).collect();
        until("N version_mismatch", 10, || {
            state(&set, n) == Some(ReplicaState::VersionMismatch)
        })
        .await;
        let p = named(&set, old[0], n).await;
        assert_eq!(p % BLOCK, 0, "P is a block boundary");
        assert_eq!(handed_over(&set), 1);
        until("N committing", 10, || committing(&set, n)).await;
        assert_eq!(from(&set, n), Some(p), "N commits from P");
        assert_eq!(set.audio(n).writes(0).0[0].0, p, "N's first commit at P");
        sleep(Duration::from_millis(200)).await;
        let now = page_now(&set, n);
        for &o in &old {
            assert_eq!(set.audio(o).lock().stops, vec![(0, p)], "{o} stops at P");
            assert!(set.audio(o).writes(0).has(p - 1), "{o} commits up to P");
            writes_none(&set, o, p, now * PAGE);
            assert_eq!(state(&set, o), Some(ReplicaState::Withdrawn), "{o}");
            assert!(!ready(&set, o), "an old-key filler is not Ready");
        }
        pages_whole(&set, &ids, p / PAGE - 2, now - 1);
        assert_eq!(metrics(&set, n).version_switch_total, 1);
        assert!(ready(&set, n));

        if ids.len() == 3 {
            // A second replica with N's key resumes on N's pages; the
            // remaining old-key filler then commits nothing at all.
            let second = old[1];
            set.set_version(second, 2, "r2");
            set.restart(second).await;
            until("the second new-key replica committing", 10, || {
                committing(&set, second)
            })
            .await;
            until("the old-key filler version_mismatch", 5, || {
                state(&set, old[0]) == Some(ReplicaState::VersionMismatch)
            })
            .await;
            assert_eq!(set.audio(old[0]).lock().drops, 1, "its core dropped");
            assert!(!ready(&set, old[0]));
            assert_eq!(handed_over(&set), 1, "no further hand-over");
        }
    }
}

/// N made unready at P: the old-key replica reports `committed up to P`,
/// N commits from P anyway, its degraded blocks held and committed through
/// on the filler's statuses; no page whole on no member.
#[tokio::test(start_paused = true)]
async fn n_unready_at_p_commits_from_p_through_its_degraded_blocks() {
    let (set, ids, n) = roll_last(vec![a(0), a(1), witness()]).await;
    let p = named(&set, a(0), n).await;
    let kp = p / BLOCK;
    until("N retargeted", 10, || set.audio(n).lock().rebuilds == 1).await;
    {
        let mut fake = set.audio(n).lock();
        fake.generation_pending = true;
        fake.degraded.push((kp, kp + 30, 0xd1));
    }
    sleep(Duration::from_millis(300)).await;
    set.audio(n).lock().generation_pending = false;
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(from(&set, n), Some(p));
    assert!(set.audio(n).writes(0).has(p), "N commits P through");
    assert!(set.audio(a(0)).writes(0).has(p - 1));
    sleep(Duration::from_millis(100)).await;
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, n) - 1);
}

/// N learning of its naming only after P: the old-key filler commits
/// through on N's standing `withdrawn` from P on, contiguous with its
/// commit up to P; N commits from its commit start once it knows and
/// the filler withholds from there. No page whole on no member.
#[tokio::test(start_paused = true)]
async fn n_learning_late_is_covered_by_the_filler() {
    let (set, ids, n) = roll_last(vec![a(0), a(1), witness()]).await;
    until("N version_mismatch", 10, || {
        state(&set, n) == Some(ReplicaState::VersionMismatch)
    })
    .await;
    // N's log learns nothing for a while; its reports flow.
    for &x in &[a(0), witness()] {
        set.net.cut_one_way(x, n);
    }
    let p = named(&set, a(0), n).await;
    until("past P", 10, || page_now(&set, a(0)) * PAGE > p + 4 * PAGE).await;
    assert!(!committing(&set, n));
    set.net.heal();
    until("N committing", 10, || committing(&set, n)).await;
    let f = from(&set, n).unwrap();
    assert!(f > p, "N commits from its commit start {f}, after P {p}");
    sleep(Duration::from_millis(100)).await;
    let now = page_now(&set, n);
    // From P the filler's fill and N's commit hold every page.
    pages_whole(&set, &ids, p / PAGE - 2, now - 1);
    assert!(set.audio(a(0)).writes(0).has(p), "the filler fills from P");
    writes_none(&set, a(0), f + PAGE, now * PAGE);
    assert!(
        metrics(&set, a(0)).version_switch_total > 0,
        "fills counted"
    );
}

/// N with an inbound-only cut that never ends while the start bound runs:
/// N is passed over, the leader names the filling old-key replica, which
/// commits again as baseline; N stays `version_mismatch`, not Ready, and
/// is not named again until it applied that `Baseline`; once the cut
/// heals the hand-over completes. The output never stops.
#[tokio::test(start_paused = true)]
async fn an_inbound_cut_n_is_passed_over_and_named_again_once_it_catches_up() {
    let (set, ids, n) = roll_last(vec![a(0), a(1), witness()]).await;
    until("N version_mismatch", 10, || {
        state(&set, n) == Some(ReplicaState::VersionMismatch)
    })
    .await;
    for &x in &[a(0), witness()] {
        set.net.cut_one_way(x, n);
    }
    let p = named(&set, a(0), n).await;
    until("the filler named again", 10, || {
        baseline(&set, a(0)).is_some_and(|b| b.replica == a(0) && b.key == Some(1))
    })
    .await;
    until("the filler committing", 10, || committing(&set, a(0))).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(handed_over(&set), 1, "N not named again while cut");
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    assert!(!ready(&set, n));
    pages_whole(&set, &[a(0)], p / PAGE - 2, page_now(&set, a(0)) - 1);
    set.net.heal();
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(handed_over(&set), 2);
    sleep(Duration::from_millis(100)).await;
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, n) - 1);
}

/// A rollout superseded by a third revision before it completes, and a
/// rollback to the earlier revision: the baseline moves to the target's
/// key each time; the previous baseline's key fills only, and any other
/// key commits nothing.
#[tokio::test(start_paused = true)]
async fn superseded_and_rolled_back_rollouts_follow_the_target() {
    let (mut set, ids, n) = roll_last(vec![a(0), a(1), a(2)]).await;
    named(&set, a(0), n).await;
    until("N committing", 10, || committing(&set, n)).await;
    // Superseded: a third revision rolls a(1).
    set.set_target(Some("r3"));
    set.set_version(a(1), 3, "r3");
    set.restart(a(1)).await;
    until("a(1) named", 10, || {
        baseline(&set, a(0)).is_some_and(|b| b.replica == a(1) && b.key == Some(3))
    })
    .await;
    until("a(1) committing", 10, || committing(&set, a(1))).await;
    sleep(Duration::from_millis(200)).await;
    // The previous baseline's key fills; the first one's commits nothing.
    assert_eq!(state(&set, n), Some(ReplicaState::Withdrawn));
    assert_eq!(state(&set, a(0)), Some(ReplicaState::VersionMismatch));
    // Rolled back to r1: the baseline moves to a(0)'s key.
    set.set_target(Some("r1"));
    until("a(0) named", 10, || {
        baseline(&set, a(1)).is_some_and(|b| b.replica == a(0) && b.key == Some(1))
    })
    .await;
    until("a(0) committing", 10, || committing(&set, a(0))).await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(state(&set, a(1)), Some(ReplicaState::Withdrawn));
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    let p = baseline(&set, a(1)).unwrap().at_index;
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, a(0)) - 1);
}

async fn mismatched(set: &Set, n: NodeId) {
    until("N version_mismatch", 10, || {
        state(set, n) == Some(ReplicaState::VersionMismatch)
    })
    .await;
}

/// A `version_mismatch` N named and learning late an entry stamped in the
/// same window: the entry is unresolved on N when its core is retargeted
/// from product state, and the retargeted core is given it again, so N
/// holds no unresolved entry the log has not decided below its first
/// computed block. N commits and the baseline does not move back.
#[tokio::test(start_paused = true)]
async fn a_retargeted_core_is_given_the_entries_still_unresolved() {
    let (mut set, _, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    let leader = set.leader().await;
    assert_ne!(leader, n);
    set.net.stall(n, Duration::from_millis(60));
    let at = (index_at(set.clock(leader).tai_ns()) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader).append(Body::InputDown {
        slot: 0,
        at_index: at,
    });
    set.set_target(Some("r2"));
    sleep(Duration::from_millis(100)).await;
    set.net.unstall(n);
    until("N committing", 10, || committing(&set, n)).await;
    sleep(Duration::from_millis(200)).await;
    let fake = set.audio(n).lock();
    let index = fake
        .unresolved
        .iter()
        .find(|&&(_, x)| x == at)
        .map(|&(i, _)| i)
        .expect("the entry unresolved on N");
    let rebuilt = fake.rebuilt_at[0];
    let given =
        |range: std::ops::Range<usize>| fake.unresolved[range].iter().any(|&(i, _)| i == index);
    let resolved_before = fake
        .resolved
        .iter()
        .any(|&(i, k)| i == index && k <= rebuilt);
    assert!(given(0..rebuilt), "unresolved before the retarget");
    assert!(!resolved_before, "decided before the retarget");
    assert!(
        given(rebuilt..fake.unresolved.len()),
        "the retargeted core was not given the unresolved entry"
    );
    drop(fake);
    assert_eq!(handed_over(&set), 1, "the baseline moved back");
    assert_eq!(baseline(&set, a(0)).unwrap().replica, n);
}

/// A `version_mismatch` N whose input was re-routed while it computed
/// nothing: named, it is not ready until the routed generation is built,
/// and its core is not retargeted before; P does not move and N is not
/// renamed meanwhile. Once the generation is built it commits.
#[tokio::test(start_paused = true)]
async fn a_re_routed_n_retargets_only_once_its_generation_is_built() {
    let (mut set, _, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    set.audio(n).lock().generation_pending = true;
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    until("a retarget refused", 1, || {
        set.audio(n).lock().refused_rebuilds > 0
    })
    .await;
    assert_eq!(
        set.audio(n).lock().rebuilds,
        0,
        "retargeted without its generation"
    );
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    set.audio(n).lock().generation_pending = false;
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(set.audio(n).lock().rebuilds, 1);
    assert_eq!(
        baseline(&set, a(0))
            .unwrap()
            .at_index
            .next_multiple_of(PAGE),
        p
    );
    assert_eq!(handed_over(&set), 1, "N renamed");
    assert_eq!(from(&set, n), Some(p), "N commits from P");
}

/// An aborted rollout: the target goes back to r1 while N is named but
/// cannot retarget, its generation not built. An old-key replica is named
/// again with key 1 and commits, N never commits, and every page stays
/// whole on a member.
#[tokio::test(start_paused = true)]
async fn an_aborted_rollout_names_an_old_key_replica_again() {
    let (mut set, ids, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    set.audio(n).lock().generation_pending = true;
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    until("a retarget refused", 1, || {
        set.audio(n).lock().refused_rebuilds > 0
    })
    .await;
    set.set_target(Some("r1"));
    until("an old-key replica named again", 10, || {
        baseline(&set, a(0)).is_some_and(|b| b.replica != n && b.key == Some(1))
    })
    .await;
    let old = baseline(&set, a(0)).unwrap().replica;
    until("the old-key replica committing", 10, || {
        committing(&set, old)
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    assert!(!committing(&set, n), "N committing");
    assert_eq!(set.audio(n).lock().rebuilds, 0, "N retargeted");
    writes_none(&set, n, p - PAGE, page_now(&set, old) * PAGE);
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, old) - 1);
}

/// A fenced N: blocks withheld at and after P while its output, never
/// started, stands idle, for a detected clock step (with the step fence
/// reported) or for skips. N is not named again while it withholds: the
/// old-key replica is named meanwhile and goes on committing through P,
/// also when the lapse of its stop at P arrives after the stop took; once
/// N withholds no more the hand-over completes.
#[tokio::test(start_paused = true)]
async fn a_fenced_n_is_not_named_again_while_fenced() {
    for (cause, late) in [
        (Cause::ClockStep, false),
        (Cause::ClockStep, true),
        (Cause::Skip, false),
    ] {
        fenced(cause, late).await;
    }
}

async fn fenced(cause: Cause, late: bool) {
    let (mut set, ids, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    set.audio(a(0)).lock().lapse_late = late;
    let now = set.audio(n).due().unwrap();
    // 0.6 s of blocks.
    let span = 600 * 48 / BLOCK;
    set.audio(n).lock().stepped.push((now, now + span, cause));
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    until("the old-key replica named again", 10, || {
        baseline(&set, a(0)).is_some_and(|b| b.replica == a(0))
    })
    .await;
    until("the old-key replica committing", 10, || {
        committing(&set, a(0))
    })
    .await;
    until("past N's step", 10, || {
        set.audio(n).due().unwrap() > now + span
    })
    .await;
    assert_eq!(
        handed_over(&set),
        1,
        "{cause:?}: N named again while fenced"
    );
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(handed_over(&set), 2);
    sleep(Duration::from_millis(100)).await;
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, n) - 1);
}
