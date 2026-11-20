//! Key hand-over in a rollout in an in-process set (plan 2.11, Baseline,
//! Replica states): a rollout that changes the compatibility key moves
//! the baseline to a target-revision replica at P, the old-key replicas
//! commit up to P and fill only from it, and replicas with another key
//! commit nothing. The baseline moves only to versions not below its
//! own while a live voter carries them. Every audio replica's engine is a
//! [`harness::FakeAudio`] whose outputs are real hold rings.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Paths, Set, base_key, base_versions, owner};
use samplez::blocks::{Cause, PAGE, Standing};
use samplez_replica::contact::{Timing, index_at};
use samplez_replica::entry::{Baseline, Body, NodeId};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::rollout::{DEPARTED, Versions};
use samplez_replica::state::{Mismatch, ReplicaState};
use samplez_service::replica::{Reason, Resolution};
use samplez_service::{ReadIndex, ReplicatedState};
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

/// Why `id` is in `version_mismatch`.
fn mismatch(set: &Set, id: NodeId) -> Option<Mismatch> {
    set.replica(id).contact_metrics()?.replica?.mismatch
}

/// This build's versions with the compute version raised by `by`: a
/// release that computes other samples.
fn up(by: u32) -> Option<Versions> {
    let v = base_versions();
    Some(Versions {
        compute: v.compute + by,
        ..v
    })
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

/// The hand-overs `id` counted while it led, since it last started.
fn handed_by(set: &Set, id: NodeId) -> u64 {
    set.replica(id)
        .contact_metrics()
        .unwrap()
        .leader
        .handed_over
}

fn handed_over(set: &Set) -> u64 {
    set.running().map(|id| handed_by(set, id)).sum()
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
/// highest ordinal to key 2 of higher versions at revision `r2`, the
/// target.
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
    set.set_version(n, 2, up(1), "r2");
    // A restart starts the leader's counts anew: none is lost here.
    assert_eq!(handed_by(&set, n), 0, "a hand-over before the roll");
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

/// The key a build runs with is its own versions' key over the output
/// config: this build's compute and protocol versions and the product's
/// derivation version, each in its own field.
#[test]
fn the_key_is_this_builds_versions_over_the_output_config() {
    use samplez_replica::rollout::{self, Versions};
    use samplez_replica::state_machine::Product;
    let v = Versions::of::<Paths>();
    assert_eq!(
        v,
        Versions {
            compute: samplez::COMPUTE_VERSION,
            protocol: samplez_replica::entry::PROTOCOL,
            derivation: <Paths as Product>::DERIVATION_VERSION,
        }
    );
    assert_eq!(rollout::key::<Paths>(b"harness"), v.key(b"harness"));
    assert_eq!(base_key(), v.key(b"harness"));
}

/// A rollout that keeps the key, a release that bumps no version, restarts
/// every audio replica pod by pod at the new revision, the target: each
/// rejoins as a peer and commits again, nothing is handed over, none is
/// ever `version_mismatch`, and every page stays whole on a member
/// throughout.
#[tokio::test(start_paused = true)]
async fn a_rollout_that_keeps_the_key_rolls_pod_by_pod_without_a_gap() {
    let mut set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    set.set_target(Some("r2"));
    // A restart starts the replica's write log anew, so each window is
    // checked before the next restart, while the replicas that wrote it
    // still hold their logs.
    let mut mark = page_now(&set, ids[0]);
    // A restart also starts the leader's counts anew.
    let mut handed = 0;
    for &id in ids.iter().rev() {
        let now = page_now(&set, ids[0]);
        pages_whole(&set, &ids, mark, now - 1);
        mark = now - 1;
        set.set_version(id, base_key(), Some(base_versions()), "r2");
        handed += handed_by(&set, id);
        set.restart(id).await;
        until("the restarted replica committing again", 10, || {
            committing(&set, id)
        })
        .await;
        assert!(
            ids.iter()
                .all(|&o| state(&set, o) != Some(ReplicaState::VersionMismatch)),
            "a replica with the set's key is never version_mismatch"
        );
    }
    sleep(Duration::from_millis(200)).await;
    let last = page_now(&set, ids[0]);
    pages_whole(&set, &ids, mark, last - 1);
    assert_eq!(handed + handed_over(&set), 0, "nothing handed over");
    assert!(ids.iter().all(|&id| ready(&set, id)), "every replica Ready");
}

/// A rollout that changes the key, of two audio replicas plus the witness
/// and of three: the restarted replica N, its key differing from the
/// baseline's, is `version_mismatch` and commits nothing until the leader
/// names it; the hand-over is at P, a block boundary; N commits from P,
/// counting one discontinuity in `version_switch_total`; every old-key
/// replica commits up to P and from P fills only, so every page stays
/// whole on a member. Old-key fillers are Ready while N commits, as every
/// replica is while another commits, and turn `version_mismatch` once a
/// second replica with N's key commits.
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
        // Counted before each restart too: a restart starts the
        // leader's counts anew.
        let mut handed = 0;
        assert_eq!(handed + handed_over(&set), 1);
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
            assert!(ready(&set, o), "an old-key filler is Ready while N commits");
        }
        pages_whole(&set, &ids, p / PAGE - 2, now - 1);
        assert_eq!(metrics(&set, n).version_switch_total, 1);
        assert!(ready(&set, n));

        if ids.len() == 3 {
            // A second replica with N's key resumes on N's pages; the
            // remaining old-key filler then commits nothing at all.
            let second = old[1];
            let drops = set.audio(old[0]).lock().drops.len();
            set.set_version(second, 2, up(1), "r2");
            handed += handed_by(&set, second);
            set.restart(second).await;
            until("the second new-key replica committing", 10, || {
                committing(&set, second)
            })
            .await;
            until("the old-key filler version_mismatch", 5, || {
                state(&set, old[0]) == Some(ReplicaState::VersionMismatch)
            })
            .await;
            assert_eq!(
                set.audio(old[0]).lock().drops.len(),
                drops + 1,
                "its core dropped"
            );
            assert!(ready(&set, old[0]), "a version_mismatch replica is Ready");
            assert_eq!(handed + handed_over(&set), 1, "no further hand-over");
        }
    }
}

/// N made unready at P: the old-key replica reports `committed up to P`,
/// N commits from P anyway, its degraded blocks held and committed through
/// on the filler's statuses; no page whole on no member.
#[tokio::test(start_paused = true)]
async fn n_unready_at_p_commits_from_p_through_its_degraded_blocks() {
    let (mut set, ids, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    let rebuilds = set.audio(n).lock().rebuilds.len();
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    let kp = p / BLOCK;
    until("N retargeted", 10, || {
        set.audio(n).lock().rebuilds.len() == rebuilds + 1
    })
    .await;
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
/// commits again as baseline; N stays `version_mismatch`, not Ready, as
/// it never committed since it started, and is not named again until it
/// applied that `Baseline`; once the cut
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
        baseline(&set, a(0)).is_some_and(|b| b.replica == a(0) && b.key == Some(base_key()))
    })
    .await;
    until("the filler committing", 10, || committing(&set, a(0))).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(handed_over(&set), 1, "N not named again while cut");
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    assert!(!ready(&set, n), "N Ready before it committed");
    pages_whole(&set, &[a(0)], p / PAGE - 2, page_now(&set, a(0)) - 1);
    set.net.heal();
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(handed_over(&set), 2);
    sleep(Duration::from_millis(100)).await;
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, n) - 1);
}

/// A rollout superseded by a third revision of higher versions before it
/// completes, and a rollback to the first revision: the baseline moves to
/// the third revision's key; the previous baseline's key fills only, and
/// the first one's commits nothing. Rolled back, the target names the
/// first revision, whose versions are below the baseline's: while a
/// replica with those is live the baseline stays, and the first
/// revision's replica is `version_mismatch`, below the baseline's
/// versions.
#[tokio::test(start_paused = true)]
async fn superseded_and_rolled_back_rollouts_follow_the_target() {
    let (mut set, ids, n) = roll_last(vec![a(0), a(1), a(2)]).await;
    named(&set, a(0), n).await;
    until("N committing", 10, || committing(&set, n)).await;
    // Superseded: a third revision rolls a(1).
    set.set_target(Some("r3"));
    set.set_version(a(1), 3, up(2), "r3");
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
    let p = baseline(&set, a(1)).unwrap().at_index;
    // Rolled back to r1: a(0) at the target never takes over.
    set.set_target(Some("r1"));
    sleep(Duration::from_millis(1000)).await;
    let b = baseline(&set, a(1)).unwrap();
    assert_eq!((b.replica, b.key), (a(1), Some(3)), "the baseline moved");
    assert_eq!(b.versions, up(2));
    assert!(committing(&set, a(1)));
    assert_eq!(state(&set, a(0)), Some(ReplicaState::VersionMismatch));
    assert_eq!(mismatch(&set, a(0)), Some(Mismatch::BelowBaselineVersions));
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, a(1)) - 1);
}

async fn mismatched(set: &Set, n: NodeId) {
    until("N version_mismatch", 10, || {
        state(set, n) == Some(ReplicaState::VersionMismatch)
    })
    .await;
}

/// A `version_mismatch` N named and learning late an entry stamped in the
/// same window, before N's P: the entry is unresolved on N when its core
/// is retargeted from product state, and the retargeted core is given it
/// again, so N holds no unresolved entry the log has not decided below
/// its first computed block. N commits and the baseline does not move
/// back. P is on the hand-over grid
/// ([`samplez_replica::rollout::hand_over_at`]) past the leader's E
/// estimate and decision lead, so where it falls depends on when the
/// test starts. The window starts where P lies at least 70 ms ahead, past
/// the entry's index: with the entry at or past P, N and the filler both
/// withhold its block while N is stalled and the leader restamps it
/// before N learns of either, a decision this test is not about. N
/// learns of its naming after P, still catching up, and holds the entry
/// it took late until it commits rather than being rebuilt past it.
#[tokio::test(start_paused = true)]
async fn a_retargeted_core_is_given_the_entries_still_unresolved() {
    let (mut set, _, n) = roll(vec![a(0), a(1), witness()], false).await;
    mismatched(&set, n).await;
    let before = set.audio(n).lock().rebuilt_at.len();
    let leader = set.leader().await;
    assert_ne!(leader, n);
    // P at least as late as the grid point past E and the decision lead
    // now: both only grow until the hand-over is stamped.
    let ahead = index_at(Duration::from_millis(70).as_nanos() as u64);
    until("P 70 ms ahead", 1, || {
        let m = set.replica(leader).contact_metrics().unwrap();
        let earliest = m.estimate.unwrap_or(0) + m.decision_lead;
        let p = samplez_replica::rollout::hand_over_at(earliest, BLOCK).next_multiple_of(PAGE);
        p >= index_at(set.clock(leader).tai_ns()) + ahead
    })
    .await;
    set.net.stall(n, Duration::from_millis(60));
    let at = (index_at(set.clock(leader).tai_ns()) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader)
        .append(Body::InputDown {
            slot: 0,
            at_index: at,
        })
        .unwrap();
    set.set_target(Some("r2"));
    sleep(Duration::from_millis(100)).await;
    set.net.unstall(n);
    let p = named(&set, leader, n).await;
    assert!(at < p, "the entry at {at} lies at or past P {p}");
    until("N committing", 10, || committing(&set, n)).await;
    sleep(Duration::from_millis(200)).await;
    let fake = set.audio(n).lock();
    let index = fake
        .unresolved
        .iter()
        .find(|&&(_, x)| x == at)
        .map(|&(i, _)| i)
        .expect("the entry unresolved on N");
    let rebuilt = fake.rebuilt_at[before];
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
    let rebuilds = set.audio(n).lock().rebuilds.len();
    set.audio(n).lock().generation_pending = true;
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    until("a retarget refused", 1, || {
        set.audio(n).lock().refused_rebuilds > 0
    })
    .await;
    assert_eq!(
        set.audio(n).lock().rebuilds.len(),
        rebuilds,
        "retargeted without its generation"
    );
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    set.audio(n).lock().generation_pending = false;
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(set.audio(n).lock().rebuilds.len(), rebuilds + 1);
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
    let rebuilds = set.audio(n).lock().rebuilds.len();
    set.audio(n).lock().generation_pending = true;
    set.set_target(Some("r2"));
    let p = named(&set, a(0), n).await;
    until("a retarget refused", 1, || {
        set.audio(n).lock().refused_rebuilds > 0
    })
    .await;
    set.set_target(Some("r1"));
    until("an old-key replica named again", 10, || {
        baseline(&set, a(0)).is_some_and(|b| b.replica != n && b.key == Some(base_key()))
    })
    .await;
    let old = baseline(&set, a(0)).unwrap().replica;
    until("the old-key replica committing", 10, || {
        committing(&set, old)
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    assert!(!committing(&set, n), "N committing");
    assert_eq!(set.audio(n).lock().rebuilds.len(), rebuilds, "N retargeted");
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

/// Rolls every audio replica of a running set, the highest ordinal first,
/// to key 2 of higher versions at revision `r2`, the target, as a
/// StatefulSet does. Each window is checked before the next restart,
/// which starts that replica's write log anew. Returns P and the
/// hand-overs counted over the roll: a restart also starts the leader's
/// counts anew, so each replica's are read before it restarts.
async fn roll_forward(set: &mut Set, ids: &[NodeId]) -> (u64, u64) {
    set.set_target(Some("r2"));
    let mut mark = page_now(set, ids[0]);
    let mut p = None;
    let mut handed = 0;
    for (i, &id) in ids.iter().enumerate().rev() {
        let now = page_now(set, ids[0]);
        pages_whole(set, ids, mark, now - 1);
        // No page mixed: from P the old-key replicas write nothing.
        if let Some(p) = p {
            for &o in &ids[..=i] {
                writes_none(set, o, p, now * PAGE);
            }
        }
        mark = now - 1;
        set.set_version(id, 2, up(1), "r2");
        handed += handed_by(set, id);
        set.restart(id).await;
        if p.is_none() {
            p = Some(named(set, ids[0], id).await);
        }
        let p = p.expect("named");
        until("the restarted replica committing", 10, || {
            committing(set, id)
        })
        .await;
        // Nor does a new-key replica write before P.
        assert!(set.audio(id).writes(0).0[0].0 >= p, "{id} wrote before P");
    }
    sleep(Duration::from_millis(200)).await;
    pages_whole(set, ids, mark, page_now(set, ids[0]) - 1);
    (p.expect("handed over"), handed + handed_over(set))
}

/// A rollout to strictly higher versions under another key, pod by pod:
/// the baseline is handed over once, at P, to the first new pod, which
/// commits from P; the others rejoin as its peers. Every page is whole on
/// a member throughout and none is committed by both an old-key and a
/// new-key replica.
#[tokio::test(start_paused = true)]
async fn an_incompatible_roll_forward_hands_over_once_without_a_gap() {
    let mut set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let (p, handed) = roll_forward(&mut set, &ids).await;
    assert_eq!(p % BLOCK, 0, "P is a block boundary");
    assert_eq!(
        from(&set, a(2)),
        Some(p),
        "the first new pod commits from P"
    );
    assert_eq!(handed, 1, "handed over once over the roll");
    let b = baseline(&set, a(0)).unwrap();
    assert_eq!((b.key, b.versions), (Some(2), up(1)));
    assert!(ids.iter().all(|&id| ready(&set, id)), "every replica Ready");
}

/// A rollout forward, then back to the lower versions pod by pod: each
/// pod rolled back is `version_mismatch`, below the baseline's versions,
/// and
/// Ready, while a replica with the higher versions is live, and the
/// baseline stays. Once the last of those is gone the replicas with the
/// highest live versions take over and the outputs resume; the only
/// pages whole on no member are those between that replica's stop and
/// the new baseline's commit start.
#[tokio::test(start_paused = true)]
async fn a_full_rollback_hands_over_to_the_highest_live_versions() {
    let mut set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    roll_forward(&mut set, &ids).await;
    set.set_target(Some("r1"));
    let mut mark = page_now(&set, ids[0]);
    for &id in ids[1..].iter().rev() {
        let now = page_now(&set, ids[0]);
        pages_whole(&set, &ids, mark, now - 1);
        mark = now - 1;
        set.set_version(id, base_key(), Some(base_versions()), "r1");
        set.restart(id).await;
        until("the rolled-back replica version_mismatch", 10, || {
            state(&set, id) == Some(ReplicaState::VersionMismatch)
        })
        .await;
    }
    sleep(Duration::from_millis(500)).await;
    for &id in &ids[1..] {
        assert_eq!(state(&set, id), Some(ReplicaState::VersionMismatch));
        assert_eq!(mismatch(&set, id), Some(Mismatch::BelowBaselineVersions));
        assert!(ready(&set, id), "{id} Ready");
    }
    assert_eq!(baseline(&set, a(0)).unwrap().key, Some(2));
    assert!(committing(&set, a(0)));
    let gone = page_now(&set, a(0));
    pages_whole(&set, &ids, mark, gone - 1);
    // The last replica with the higher versions rolls back.
    set.set_version(a(0), base_key(), Some(base_versions()), "r1");
    set.restart(a(0)).await;
    until("a replica with the lower versions named", 10, || {
        baseline(&set, a(1)).is_some_and(|b| b.key == Some(base_key()))
    })
    .await;
    let b = baseline(&set, a(1)).unwrap();
    assert_eq!(b.versions, Some(base_versions()));
    until("the outputs resume", 10, || committing(&set, b.replica)).await;
    let start = from(&set, b.replica).unwrap();
    sleep(Duration::from_millis(200)).await;
    let now = page_now(&set, b.replica);
    pages_whole(&set, &ids, start.div_ceil(PAGE), now - 1);
    // The gap: the departure window from the last report of the higher
    // versions, then a decision, the decision lead and a retarget.
    let bound = index_at((DEPARTED + Duration::from_secs(2)).as_nanos() as u64);
    assert!(
        start - gone * PAGE <= bound,
        "the outputs resumed {} samples after the stop",
        start - gone * PAGE
    );
}

/// A pod rolled back to lower versions while a replica with the
/// baseline's higher versions is live is `version_mismatch` with its
/// below the baseline's versions: never named, though it is at the target
/// revision
/// and a candidate, and Ready.
#[tokio::test(start_paused = true)]
async fn a_downgraded_pod_never_takes_over_while_a_newer_one_is_live() {
    let (mut set, _, n) = roll_last(vec![a(0), a(1), a(2)]).await;
    let p = named(&set, a(0), n).await;
    until("N committing", 10, || committing(&set, n)).await;
    set.set_version(a(1), 2, up(1), "r2");
    set.restart(a(1)).await;
    until("a(1) committing", 10, || committing(&set, a(1))).await;
    // a(1) rolls back to r1.
    set.set_target(Some("r1"));
    set.set_version(a(1), base_key(), Some(base_versions()), "r1");
    set.restart(a(1)).await;
    until("a(1) version_mismatch", 10, || {
        state(&set, a(1)) == Some(ReplicaState::VersionMismatch)
    })
    .await;
    sleep(Duration::from_millis(1000)).await;
    let b = baseline(&set, a(0)).unwrap();
    assert_eq!((b.replica, b.key), (n, Some(2)), "the baseline moved");
    assert_eq!(state(&set, a(1)), Some(ReplicaState::VersionMismatch));
    assert_eq!(mismatch(&set, a(1)), Some(Mismatch::BelowBaselineVersions));
    assert!(
        ready(&set, a(1)),
        "one below the baseline's versions is Ready"
    );
    assert!(committing(&set, n));
    writes_none(&set, a(1), 0, page_now(&set, n) * PAGE);
    pages_whole(&set, &[n], p / PAGE, page_now(&set, n) - 1);
}

/// Makes `id` the Raft leader: an election is called until it wins one,
/// as votes are refused while the current leader's lease runs.
async fn lead(set: &Set, id: NodeId) {
    let raft = set.replica(id).raft().clone();
    let deadline = Instant::now() + Duration::from_secs(30);
    while raft.metrics().borrow().state != openraft::ServerState::Leader {
        let m = raft.metrics().borrow().clone();
        assert!(
            Instant::now() < deadline,
            "{id} leads within 30 s: the leader is {:?} in term {}",
            m.current_leader,
            m.current_term
        );
        let _ = raft.trigger().elect().await;
        sleep(Duration::from_millis(500)).await;
    }
}

/// The `Baseline`s `at` applies over `span`, as (replica, key), each once
/// in the order they were applied.
async fn baselines_over(set: &Set, at: NodeId, span: Duration) -> Vec<(NodeId, Option<u64>)> {
    let end = Instant::now() + span;
    let mut seen = Vec::new();
    while Instant::now() < end {
        if let Some(b) = baseline(set, at) {
            let now = (b.replica, b.key);
            if seen.last() != Some(&now) {
                seen.push(now);
            }
        }
        sleep(Duration::from_millis(1)).await;
    }
    seen
}

/// The only replica with the baseline's higher versions, unheard by the
/// others for less than the departure window, as a late report makes
/// it: the rolled-back replicas below its versions do not take over and
/// the baseline stays. Unheard for longer, it counts as gone: one of them
/// takes over, once, and nothing hands the baseline back once it is
/// heard again.
#[tokio::test(start_paused = true)]
async fn a_downgrade_waits_for_the_baselines_versions_to_depart() {
    // N, a(2), has the higher versions; a(0) and a(1) are rolled back.
    let (mut set, _, n) = roll_last(vec![a(0), a(1), a(2)]).await;
    named(&set, a(0), n).await;
    until("N committing", 10, || committing(&set, n)).await;
    set.set_version(a(1), 2, up(1), "r2");
    set.restart(a(1)).await;
    until("a(1) committing", 10, || committing(&set, a(1))).await;
    // a(0), the old-key filler, turns `version_mismatch` once it sees a
    // second replica of N's key commit; a(1) restarted at once could go
    // before a(0) took a report of it committing.
    until("a(0) version_mismatch", 10, || {
        state(&set, a(0)) == Some(ReplicaState::VersionMismatch)
    })
    .await;
    set.set_target(Some("r1"));
    set.set_version(a(1), base_key(), Some(base_versions()), "r1");
    set.restart(a(1)).await;
    for id in [a(0), a(1)] {
        until("rolled back, version_mismatch", 10, || {
            state(&set, id) == Some(ReplicaState::VersionMismatch)
        })
        .await;
    }
    // A leader that does not hear N decides.
    lead(&set, a(0)).await;
    let base = (n, Some(2));
    let unheard = |set: &Set, cut: bool| {
        for o in [a(0), a(1)] {
            if cut {
                set.net.cut_peer(n, o);
            } else {
                set.net.join_peer(n, o);
            }
        }
    };

    // Unheard for less than the departure window.
    unheard(&set, true);
    let short = DEPARTED - Duration::from_millis(500);
    assert_eq!(baselines_over(&set, a(0), short).await, [base]);
    unheard(&set, false);
    let heard = Duration::from_millis(500);
    assert_eq!(baselines_over(&set, a(0), heard).await, [base]);

    // Unheard for longer: one replica below N's versions takes over.
    unheard(&set, true);
    let long = DEPARTED + Duration::from_secs(2);
    let seen = baselines_over(&set, a(0), long).await;
    assert_eq!(seen.len(), 2, "one change of baseline: {seen:?}");
    assert_eq!(seen[0], base);
    assert_ne!(seen[1].0, n);
    assert_eq!(seen[1].1, Some(base_key()), "the lower versions' key");
    unheard(&set, false);
    assert_eq!(
        baselines_over(&set, a(0), Duration::from_secs(1)).await,
        [seen[1]],
        "handed back once N is heard again"
    );
}

/// A new pod in `version_mismatch` awaiting its hand-over, with higher
/// versions and no target revision yet, says so and reports its
/// versions; it never committed since it started, so it is not Ready
/// while the others commit: readiness holds a rollout until a new pod
/// commits. Naming reads no readiness, so the hand-over is not held.
#[tokio::test(start_paused = true)]
async fn a_version_mismatch_replica_awaiting_its_hand_over_is_not_ready() {
    let (set, ids, n) = roll(vec![a(0), a(1), a(2)], false).await;
    mismatched(&set, n).await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    assert_eq!(mismatch(&set, n), Some(Mismatch::AwaitingHandOver));
    assert!(ids.iter().any(|&o| committing(&set, o)));
    assert!(!ready(&set, n), "Ready before it committed");
    let m = set.replica(n).contact_metrics().unwrap();
    assert_eq!(m.versions, up(1));
    assert_eq!(handed_over(&set), 0);
}

/// Starts `ids` with no replica given its rollout until every audio
/// replica commits, so the set's `Baseline` carries no key, as one named
/// before any key reached contact does; then every replica reports its
/// key. Each page hash has its replica's key mixed in and each engine is
/// of its key's shape, so replicas of two keys compute other pages and
/// refuse each other's cores, and every fake copies and takes cores.
async fn start_keyless(ids: Vec<NodeId>) -> (Set, Vec<NodeId>) {
    let mut set = Set::start(Options {
        ids: Some(ids),
        contact: Some(Timing::defaults(BLOCK)),
        outputs: 1,
        late_rollout: true,
        key_pages: true,
        ..Options::default()
    })
    .await;
    let ids = audio(&set);
    for &id in &ids {
        set.audio(id).lock().resyncs = true;
    }
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let all: Vec<NodeId> = set.ids().collect();
    for id in all {
        set.release_rollout(id);
    }
    until("every key reported", 5, || {
        ids.iter()
            .all(|&id| (set.replica(id).contact_metrics()).is_some_and(|m| m.versions.is_some()))
    })
    .await;
    assert_eq!(
        baseline(&set, ids[0]).unwrap().key,
        None,
        "a keyless Baseline"
    );
    (set, ids)
}

/// A rollout that changes the key under a `Baseline` carrying none: the
/// restarted replica N takes the key the named replica reports, as the
/// leader does, so it is `version_mismatch`, not withdrawn against pages
/// of another key; the leader names it with its key within 10 s, it
/// commits from P and is Ready, it never asks an old-key replica for a
/// core, and the old-key replicas stop at P. With `diverged`, N's key
/// reaches contact only after a verdict diverged it: with no donor of its
/// key it is rebuilt from product state, not resynced, and the hand-over
/// completes alike.
#[tokio::test(start_paused = true)]
async fn a_key_change_under_a_keyless_baseline_hands_over() {
    for diverged in [false, true] {
        key_change_under_a_keyless_baseline(diverged).await;
    }
}

async fn key_change_under_a_keyless_baseline(diverged: bool) {
    let (mut set, ids) = start_keyless(vec![a(0), a(1), a(2)]).await;
    let named0 = baseline(&set, ids[0]).unwrap().replica;
    let n = *ids.iter().rev().find(|&&id| id != named0).unwrap();
    let old: Vec<NodeId> = ids.iter().copied().filter(|&id| id != n).collect();
    set.on_start(n, |fake| fake.lock().resyncs = true);
    set.set_version(n, 2, up(1), "r2");
    if diverged {
        set.hold_rollout(n);
        set.restart(n).await;
        until("N out of starting", 10, || {
            state(&set, n).is_some_and(|s| s != ReplicaState::Starting)
        })
        .await;
        let leader = set.leader().await;
        let page = page_now(&set, n);
        set.replica(leader)
            .append(Body::Diverged { replica: n, page })
            .unwrap();
        until("N diverged", 10, || metrics(&set, n).divergences > 0).await;
        set.set_target(Some("r2"));
        set.release_rollout(n);
    } else {
        set.set_target(Some("r2"));
        set.restart(n).await;
    }
    let p = named(&set, old[0], n).await;
    until("N committing", 10, || committing(&set, n)).await;
    assert_eq!(from(&set, n), Some(p), "N commits from P");
    assert!(ready(&set, n), "N Ready");
    sleep(Duration::from_millis(300)).await;
    assert!(committing(&set, n), "N stays committing");
    let m = set.replica(n).contact_metrics().unwrap().resync;
    assert_eq!(
        (m.attempts, m.failures),
        (0, 0),
        "a resync from an old-key donor: {m:?}"
    );
    for &o in &old {
        assert_eq!(set.audio(o).lock().stops, vec![(0, p)], "{o} stops at P");
        assert!(set.audio(o).writes(0).has(p - 1), "{o} commits up to P");
        writes_none(&set, o, p, page_now(&set, n) * PAGE);
        assert!(ready(&set, o), "{o} Ready while N commits");
    }
    pages_whole(&set, &ids, p / PAGE - 2, page_now(&set, n) - 1);
}

/// Rolls a running set of three to key 2 by its two highest ordinals, as
/// a StatefulSet does: the old-key replica a(0), which committed since
/// it started, is `version_mismatch` once the second new-key replica
/// commits, and Ready while they commit.
async fn roll_two() -> Set {
    let (mut set, _, n) = roll_last(vec![a(0), a(1), a(2)]).await;
    named(&set, a(0), n).await;
    until("N committing", 10, || committing(&set, n)).await;
    set.set_version(a(1), 2, up(1), "r2");
    set.restart(a(1)).await;
    until("a(1) committing", 10, || committing(&set, a(1))).await;
    until("a(0) version_mismatch", 10, || {
        state(&set, a(0)) == Some(ReplicaState::VersionMismatch)
    })
    .await;
    until("a(0) Ready", 5, || ready(&set, a(0))).await;
    set
}

/// A Ready replica that commits nothing, in `version_mismatch`, serves
/// the product's API as any other: a write sent to it reaches the log
/// through the leader and is answered once applied on it, every replica
/// holds it under the client's session, and its read index is
/// confirmed at or past that write. Readiness routes clients; what they
/// are answered does not depend on the audio state.
#[tokio::test(start_paused = true)]
async fn a_ready_replica_committing_nothing_serves_the_api() {
    let set = roll_two().await;
    let n = a(0);
    assert!(!committing(&set, n));
    let replica = set.replica(n);
    let ack = replica
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("forwarded and committed");
    assert_eq!(ack.resolution, Resolution::Applied);
    assert!(
        replica.revision() >= ack.revision,
        "answered once applied on N"
    );
    set.applied(ack.revision).await;
    for id in set.ids() {
        assert_eq!(
            set.replica(id).command_resolution(&owner("s"), "1"),
            Some((ack.revision, Resolution::Duplicate)),
            "{id}"
        );
    }
    let read = replica.read_index().await;
    assert!(
        matches!(read, ReadIndex::Confirmed(r) if r >= ack.revision),
        "{read:?}"
    );
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    assert!(ready(&set, n));
}

/// Every page `[from, to)` is whole on one of `ids` and written by
/// replicas of one key only, `key` giving each one's: no output carries
/// pages of two keys.
fn pages_of_one_key(set: &Set, ids: &[NodeId], key: impl Fn(NodeId) -> u64, from: u64, to: u64) {
    let writes: Vec<_> = ids
        .iter()
        .map(|&id| (id, set.audio(id).writes(0)))
        .collect();
    for page in from..to {
        let span = page * PAGE..(page + 1) * PAGE;
        let keys: std::collections::BTreeSet<u64> = (writes.iter())
            .filter(|(_, w)| {
                w.0.iter()
                    .any(|&(s, n)| s < span.end && s + n as u64 > span.start)
            })
            .map(|&(id, _)| key(id))
            .collect();
        assert!(keys.len() <= 1, "page {page} written under keys {keys:?}");
        let whole = (writes.iter()).any(|(_, w)| span.clone().all(|i| w.has(i)));
        assert!(whole, "page {page} whole on no member");
    }
}

/// A `Baseline` without a key, as an older log holds, naming a replica
/// that restarts under another key while the others commit and no leader
/// can rename it: the restarted replica takes no commit under that
/// naming, which would put pages of its key beside the others', and is
/// `version_mismatch`; every page stays whole and of one key.
#[tokio::test(start_paused = true)]
async fn a_keyless_naming_of_a_pod_restarted_under_another_key_mixes_no_pages() {
    let mut set = Set::start(Options {
        ids: Some(vec![a(0), a(1), a(2)]),
        contact: Some(Timing::defaults(BLOCK)),
        outputs: 1,
        key_pages: true,
        ..Options::default()
    })
    .await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let leader = set.leader().await;
    let n = *ids.iter().rev().find(|&&id| id != leader).unwrap();
    set.replica(leader)
        .append(Body::Baseline(Baseline {
            replica: n,
            order: ids.iter().copied().filter(|&id| id != n).collect(),
            at_index: 0,
            key: None,
            versions: None,
        }))
        .unwrap();
    until("the keyless Baseline applied everywhere", 5, || {
        ids.iter()
            .all(|&id| baseline(&set, id).is_some_and(|b| b.replica == n && b.key.is_none()))
    })
    .await;
    assert!(committing(&set, n));
    // Every commit known everywhere, then no leader can rename.
    sleep(Duration::from_millis(500)).await;
    for &x in &ids {
        for &y in &ids {
            if x < y {
                set.net.cut_raft(x, y);
            }
        }
    }
    let old: Vec<NodeId> = ids.iter().copied().filter(|&id| id != n).collect();
    let mark = page_now(&set, old[0]);
    set.set_version(n, 2, up(1), "r2");
    set.restart(n).await;
    until("N out of starting", 10, || {
        state(&set, n).is_some_and(|s| s != ReplicaState::Starting)
    })
    .await;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(state(&set, n), Some(ReplicaState::VersionMismatch));
    let now = page_now(&set, old[0]);
    writes_none(&set, n, 0, now * PAGE);
    assert!(
        old.iter().all(|&o| committing(&set, o)),
        "the others commit on"
    );
    let key = |id: NodeId| if id == n { 2 } else { base_key() };
    pages_of_one_key(&set, &ids, key, mark, now - 1);
}

/// A Ready replica that commits nothing and reaches no leader, its raft
/// links cut while its reports flow, takes a write and answers it within
/// the write timeout as not applied, for want of a leader; it never
/// hangs. Cut off for longer than a leader counts a replica, it is still
/// Ready, in contact with its set, and still takes writes.
#[tokio::test(start_paused = true)]
async fn a_ready_replica_without_a_leader_answers_within_the_write_timeout() {
    let mut set = roll_two().await;
    let n = a(0);
    // Not the leader: a cut-off leader still names itself one. A leading
    // N is stopped until the others elect one of them, then started
    // again: it follows that leader's heartbeats rather than campaign.
    // Restarted, N is below the baseline's versions, never committed
    // since its start, and Ready while the others commit.
    if set.leader().await == n {
        set.crash(n).await;
        until("another leader", 10, || {
            [a(1), a(2)].iter().any(|&o| {
                set.replica(o).raft().metrics().borrow().state == openraft::ServerState::Leader
            })
        })
        .await;
        set.restart(n).await;
        until("N version_mismatch", 10, || {
            state(&set, n) == Some(ReplicaState::VersionMismatch)
        })
        .await;
        assert_eq!(mismatch(&set, n), Some(Mismatch::BelowBaselineVersions));
        until("N Ready", 5, || ready(&set, n)).await;
    }
    assert_ne!(set.leader().await, n, "N leads");
    for o in [a(1), a(2)] {
        set.net.cut_raft(n, o);
    }
    // Past the window a leader counts a replica for, no leader heard.
    sleep(DEPARTED + Duration::from_secs(1)).await;
    let replica = set.replica(n);
    assert!(!replica.quorum().await, "N follows a leader");
    assert!(ready(&set, n), "N Ready");
    assert!(!committing(&set, n));
    let bound = Options::default().write_timeout + Duration::from_millis(500);
    let asked = Instant::now();
    let refused = tokio::time::timeout(
        bound,
        replica.submit(&owner("s"), "1", Bytes::from("gain=1@1")),
    )
    .await
    .expect("answered within the write timeout")
    .expect_err("no leader orders it");
    assert_eq!(refused.reason, Reason::Unavailable, "{refused}");
    assert_eq!(refused.detail, "no leader");
    assert!(asked.elapsed() <= bound);
}

/// A broken release, one that keeps the key but whose engine computes
/// other samples, rolled into a running set: the new pod diverges and
/// never commits, so it is not Ready and the StatefulSet stops at it,
/// while the two old pods commit on and stay Ready.
#[tokio::test(start_paused = true)]
async fn a_broken_release_stops_the_roll_at_its_first_pod() {
    let mut set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let n = a(2);
    set.set_target(Some("r2"));
    set.set_version(n, base_key(), Some(base_versions()), "r2");
    set.on_start(n, |fake| fake.lock().rebuild_glitch = Some(0.5));
    set.restart(n).await;
    until("N out of starting", 10, || {
        state(&set, n).is_some_and(|s| s != ReplicaState::Starting)
    })
    .await;
    let end = Instant::now() + Duration::from_secs(2);
    while Instant::now() < end {
        assert!(!committing(&set, n), "the broken pod committed");
        assert!(!ready(&set, n), "the broken pod is Ready");
        sleep(Duration::from_millis(10)).await;
    }
    for o in [a(0), a(1)] {
        assert!(committing(&set, o), "{o} commits");
        assert!(ready(&set, o), "{o} Ready");
    }
}

/// The leader exposes `samplez_replica_disagreements_total` with both
/// kinds, at 0 in a set whose replicas agree.
#[tokio::test(start_paused = true)]
async fn the_leader_exposes_disagreements_by_kind() {
    let set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let leader = set.leader().await;
    let samples = set.replica(leader).replica_metrics();
    for kind in ["no_majority", "cross_key"] {
        let s = samples
            .iter()
            .find(|m| {
                m.name == "samplez_replica_disagreements_total"
                    && m.labels.iter().any(|(k, v)| *k == "kind" && v == kind)
            })
            .unwrap_or_else(|| panic!("{kind} on the leader"));
        assert_eq!(s.value, 0.0, "{kind}");
    }
}
