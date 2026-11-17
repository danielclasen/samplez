//! The clock check in an in-process set (plan 2.11, Replicated state,
//! clock check): one replica's TAI clock moved off the others' is judged
//! by a `ClockVerdict`, excluded, and cleared once the clocks agree within
//! clockGuard again; with two audio replicas the witness or the tie rule
//! decides. Every offset is a multiple of clockGuard, so no case is tied
//! to one block size.

mod harness;

use std::collections::BTreeMap;
use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez_replica::contact::{Timing, time_of};
use samplez_replica::entry::{Body, NodeId, Request};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::state::ReplicaState;
use tokio::time::{Instant, sleep};

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

/// clockGuard, ns.
fn guard() -> i64 {
    time_of(timing().clock_guard) as i64
}

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

async fn start(ids: Vec<NodeId>) -> Set {
    Set::start(Options {
        ids: Some(ids),
        contact: Some(timing()),
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

/// The latest `ClockVerdict` per replica `id` applied.
fn verdicts(set: &Set, id: NodeId) -> BTreeMap<NodeId, bool> {
    set.replica(id).store().with_state(|sm| sm.verdicts.clone())
}

fn judged(set: &Set, id: NodeId) -> Vec<NodeId> {
    verdicts(set, id)
        .into_iter()
        .filter(|&(_, j)| j)
        .map(|(id, _)| id)
        .collect()
}

/// Whether every running voter applied exactly `want` as judged.
fn judged_everywhere(set: &Set, want: &[NodeId]) -> bool {
    set.running().all(|id| judged(set, id) == want)
}

fn baselines_naming(set: &Set, id: NodeId, named: NodeId) -> usize {
    let mut n = 0;
    set.replica(id)
        .store()
        .entries_after(0, &mut |_, r: &Request| {
            if let Body::Baseline(b) = &r.body
                && b.replica == named
            {
                n += 1;
            }
        });
    n
}

fn tie(set: &Set) -> Option<[(NodeId, i64); 2]> {
    set.running()
        .find_map(|id| set.replica(id).contact_metrics().and_then(|m| m.clock.tie))
}

fn counted(set: &Set, id: NodeId) -> Vec<NodeId> {
    set.replica(id).contact_metrics().unwrap().counted
}

async fn all_committing(set: &Set) {
    let ids = audio(set);
    until("every audio replica committing", 5, || {
        ids.iter().all(|&id| committing(set, id))
    })
    .await;
}

/// A clock off by three guards on one of two audio replicas plus the
/// witness and on one of three: the leader judges that one alone, it is
/// excluded (diverged) while the others keep committing, and the verdict
/// clears once its clock is back.
#[tokio::test(start_paused = true)]
async fn an_offset_on_one_of_two_and_one_of_three_judges_that_one() {
    for ids in [vec![a(0), a(1), witness()], vec![a(0), a(1), a(2)]] {
        let set = start(ids).await;
        all_committing(&set).await;
        let off = *audio(&set).last().unwrap();
        set.clock(off).set_tai_offset(3 * guard());
        until("the offset replica judged", 5, || {
            judged_everywhere(&set, &[off])
        })
        .await;
        until("the judged one excluded", 5, || {
            state(&set, off) == Some(ReplicaState::Diverged)
        })
        .await;
        for id in audio(&set) {
            if id != off {
                assert!(committing(&set, id), "{id} stopped committing");
            }
        }
        let leader = set.leader().await;
        let status = set.replica(leader).contact_metrics().unwrap().clock;
        let dev = status
            .offsets
            .iter()
            .find(|o| o.replica == off)
            .and_then(|o| o.deviation_ns)
            .expect("a deviation in status");
        assert!(dev > guard(), "deviation {dev} in status");
        assert_eq!(status.tie, None);
        set.clock(off).set_tai_offset(0);
        until("the verdict cleared", 5, || {
            set.running()
                .all(|id| verdicts(&set, id).get(&off) == Some(&false))
        })
        .await;
    }
}

/// Two audio replicas apart beyond the guard with the witness's clock
/// half way between them: the witness tells nothing, so the baseline is
/// taken as right, the other is judged and `replica_clock_tie` names both;
/// the verdict holds across a restart of the baseline and clears once the
/// clocks agree.
#[tokio::test(start_paused = true)]
async fn two_replicas_the_witness_cannot_tell_apart_follow_the_tie_rule() {
    let mut set = start(vec![a(0), a(1), witness()]).await;
    all_committing(&set).await;
    let leader = set.leader().await;
    let base = set
        .replica(leader)
        .store()
        .with_state(|sm| sm.baseline.clone())
        .unwrap()
        .replica;
    let other = if base == a(0) { a(1) } else { a(0) };
    set.clock(witness()).set_tai_offset(3 * guard() / 2);
    set.clock(other).set_tai_offset(3 * guard());
    until("the non-baseline judged", 5, || {
        judged_everywhere(&set, &[other])
    })
    .await;
    until("replica_clock_tie", 5, || tie(&set).is_some()).await;
    let named: Vec<NodeId> = tie(&set).unwrap().iter().map(|&(id, _)| id).collect();
    assert!(named.contains(&base) && named.contains(&other), "{named:?}");
    for id in [base] {
        set.restart(id).await;
        until("the restarted replica in contact", 10, || {
            set.replica(id)
                .contact_metrics()
                .is_some_and(|m| m.in_contact)
        })
        .await;
        sleep(Duration::from_secs(1)).await;
        assert!(judged_everywhere(&set, &[other]), "after restarting {id}");
    }
    set.clock(witness()).set_tai_offset(0);
    set.clock(other).set_tai_offset(0);
    until("the verdict cleared", 5, || judged_everywhere(&set, &[])).await;
    until("the alarm cleared", 5, || tie(&set).is_none()).await;
}

/// R excluded by a `ClockVerdict` while X stops: R fills nothing and is
/// not named, so output stops; on X's return R stays excluded until its
/// clock agrees again.
#[tokio::test(start_paused = true)]
async fn a_replica_excluded_by_a_clock_verdict_fills_nothing_and_is_not_named() {
    let (x, r) = (a(0), a(1));
    let mut set = start(vec![x, r, witness()]).await;
    all_committing(&set).await;
    set.clock(r).set_tai_offset(3 * guard());
    until("R judged", 5, || judged_everywhere(&set, &[r])).await;
    until("R excluded", 5, || {
        state(&set, r) == Some(ReplicaState::Diverged)
    })
    .await;
    set.crash(x).await;
    sleep(Duration::from_millis(200)).await;
    let written = set.audio(r).writes(0).0.len();
    let named = baselines_naming(&set, r, r);
    sleep(Duration::from_secs(2)).await;
    assert_eq!(set.audio(r).writes(0).0.len(), written, "R filled");
    assert_eq!(baselines_naming(&set, r, r), named, "R named");
    assert!(
        judged_everywhere(&set, &[r]),
        "a lone replica's verdict cleared"
    );
    set.restart(x).await;
    until("X committing", 10, || committing(&set, x)).await;
    sleep(Duration::from_secs(1)).await;
    assert!(judged_everywhere(&set, &[r]), "R cleared on X's return");
    assert_eq!(state(&set, r), Some(ReplicaState::Diverged));
    set.clock(r).set_tai_offset(0);
    until("R cleared", 5, || judged_everywhere(&set, &[])).await;
}

/// Both audio replicas self-report: within the guard of each other
/// neither is judged, E counts both and both take writes; beyond it the
/// witness picks, E follows the other and output continues on it.
#[tokio::test(start_paused = true)]
async fn both_self_reporting_within_and_beyond_the_guard() {
    let (x, r) = (a(0), a(1));
    let set = start(vec![x, r, witness()]).await;
    all_committing(&set).await;
    for id in [x, r] {
        set.audio(id).lock().flags.self_report = true;
    }
    set.clock(r).set_tai_offset(guard() / 2);
    sleep(Duration::from_secs(1)).await;
    assert!(judged_everywhere(&set, &[]));
    for id in [x, r] {
        let mut c = counted(&set, id);
        c.sort();
        assert_eq!(c, [x, r], "E on {id}");
        set.commitable(id).await;
    }
    set.clock(r).set_tai_offset(3 * guard());
    until("R judged", 5, || judged_everywhere(&set, &[r])).await;
    until("E follows X", 5, || {
        [x, r].iter().all(|&id| counted(&set, id) == [x])
    })
    .await;
    let before = set.audio(x).writes(0).0.len();
    sleep(Duration::from_millis(200)).await;
    assert!(
        set.audio(x).writes(0).0.len() > before,
        "output stopped on X"
    );
}
