//! Contact between the voters of an in-process set: side reports, the
//! contact fence F, silence, and commit knowledge from reports. Every
//! replica's engine is a [`harness::FakeAudio`] computing each block as it
//! falls due on the replica's TAI clock.

mod harness;

use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez_replica::contact::{Timing, index_at};
use samplez_replica::entry::{Body, NodeId};
use tokio::time::sleep;

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

async fn start() -> Set {
    let set = Set::start(Options {
        contact: Some(timing()),
        ..Options::default()
    })
    .await;
    // Every voter heard every other.
    sleep(Duration::from_millis(50)).await;
    set
}

/// The blocks due (`T_k`) within `[from, to]` TAI ns.
fn due_between(from: u64, to: u64) -> std::ops::RangeInclusive<u64> {
    let t = timing();
    let mut first = (index_at(from).saturating_sub(t.margin) / BLOCK).saturating_sub(2);
    while t.block_deadline(first) < from {
        first += 1;
    }
    let mut last = first;
    while t.block_deadline(last + 1) <= to {
        last += 1;
    }
    first..=last
}

/// Every block due within `[from, to]` passed F on `id` by its fence
/// deadline `T_k + D`.
fn fenced_in_time(set: &Set, id: NodeId, from: u64, to: u64) {
    let t = timing();
    let audio = set.audio(id);
    let hold = t.hold.as_nanos() as u64;
    for k in due_between(from, to) {
        let deadline = t.block_deadline(k) + hold;
        match audio.passed(k) {
            Some(at) => assert!(
                at <= deadline,
                "{id}: F passed block {k} {} us after its fence deadline",
                (at - deadline) / 1000
            ),
            None => panic!("{id}: F never passed block {k}"),
        }
    }
}

/// Runs for `d`, checking every millisecond that no replica holds
/// another silent.
async fn nobody_silent_for(set: &Set, d: Duration) {
    let steps = d.as_millis() as u64;
    for _ in 0..steps {
        for id in set.running() {
            let m = set.replica(id).contact_metrics().expect("contact runs");
            let silent: Vec<NodeId> = m
                .peers
                .iter()
                .filter(|p| p.silent)
                .map(|p| p.node)
                .collect();
            assert!(silent.is_empty(), "{id} holds {silent:?} silent");
        }
        sleep(Duration::from_millis(1)).await;
    }
}

fn tai(set: &Set) -> u64 {
    set.clock(0).tai_ns()
}

#[tokio::test(start_paused = true)]
async fn a_follower_cut_off_is_fenced_and_the_rest_keep_committing() {
    let set = start().await;
    let leader = set.leader().await;
    let cut = set.ids().find(|&id| id != leader).unwrap();
    let w = timing().contact_window.as_nanos() as u64;

    let t_cut = tai(&set);
    set.net.isolate(cut, set.ids());
    sleep(Duration::from_millis(100)).await;
    let t_heal = tai(&set);
    // Nothing it sent after the cut was echoed: F passes no block ending
    // more than w after the cut that its engine still holds.
    let t = timing();
    let ring = index_at((t.hold + t.contact_window).as_nanos() as u64).div_ceil(BLOCK) + 2;
    let fences = set.audio(cut).lock().fences.clone();
    for &(at, f) in fences
        .iter()
        .filter(|&&(at, _)| (t_cut..t_heal).contains(&at))
    {
        let last = f / BLOCK - 1;
        let due = (index_at(at) - t.margin) / BLOCK - 1;
        assert!(
            (last + 1) * BLOCK <= index_at(t_cut + w) || last + ring <= due,
            "at {at} F passed block {last}, ending after the cut plus w and still held"
        );
    }
    assert!(
        fences.iter().any(|&(at, _)| (t_cut..t_heal).contains(&at)),
        "F moved during the cut only past held blocks; no fence seen"
    );
    for id in set.ids().filter(|&id| id != cut) {
        fenced_in_time(&set, id, t_cut, t_heal);
        let m = set.replica(id).contact_metrics().unwrap();
        let p = m.peers.iter().find(|p| p.node == cut).unwrap();
        assert!(p.silent, "{id} holds the cut-off replica silent");
    }
    // Each of the two relays that the other holds it silent.
    let reports = {
        set.net.tap_reports();
        sleep(Duration::from_millis(10)).await;
        set.net.reports()
    };
    assert!(
        reports
            .iter()
            .filter(|(to, r)| *to != cut && r.from != cut)
            .all(|(_, r)| r.heard.iter().any(|h| h.node == cut && h.silent)),
        "every report between the two names the cut-off replica silent"
    );

    set.net.heal();
    sleep(Duration::from_millis(150)).await;
    // Back in contact, F passes the blocks it fenced once its hold ring
    // let go of them, and then every block in time.
    fenced_in_time(&set, cut, t_heal + 60_000_000, tai(&set) - 20_000_000);
}

#[tokio::test(start_paused = true)]
async fn a_20_ms_report_gap_on_every_link_withdraws_nobody() {
    let set = start().await;
    let t_gap = tai(&set);
    set.net.cut_all(set.ids());
    nobody_silent_for(&set, Duration::from_millis(20)).await;
    set.net.heal();
    nobody_silent_for(&set, Duration::from_millis(60)).await;
    for id in set.ids() {
        // Every block, those due during the gap included, passes F before
        // its fence deadline: nothing is withheld.
        fenced_in_time(&set, id, t_gap - 10_000_000, tai(&set) - 20_000_000);
        let m = set.replica(id).contact_metrics().unwrap();
        assert_eq!(m.shared_gaps, 1, "{id} counts the gap on every link once");
        assert!(m.shared_gap_max >= Duration::from_millis(20));
    }
}

#[tokio::test(start_paused = true)]
async fn links_recovering_6_ms_apart_hold_nobody_silent() {
    let set = start().await;
    let t_gap = tai(&set);
    set.net.cut_all(set.ids());
    nobody_silent_for(&set, Duration::from_millis(20)).await;
    set.net.join(0, 1);
    nobody_silent_for(&set, Duration::from_millis(3)).await;
    set.net.join(0, 2);
    nobody_silent_for(&set, Duration::from_millis(3)).await;
    set.net.join(1, 2);
    nobody_silent_for(&set, Duration::from_millis(60)).await;
    for id in set.ids() {
        fenced_in_time(&set, id, t_gap - 10_000_000, tai(&set) - 20_000_000);
    }
    // 1 heard 0 at once and 2 six ms later.
    let m = set.replica(1).contact_metrics().unwrap();
    assert!(
        m.stagger_max >= Duration::from_millis(5),
        "{:?}",
        m.stagger_max
    );
}

/// Appends an `InputDown` on the leader 40 ms ahead and waits until `on`
/// appended it; its log index and at_index.
async fn append_ahead(set: &Set, leader: NodeId, on: NodeId) -> (u64, u64) {
    let at = (index_at(tai(set)) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader).append(Body::InputDown {
        slot: 0,
        at_index: at,
    });
    for _ in 0..20 {
        let appended = set.audio(on).lock().appended.clone();
        if let Some(&(index, _)) = appended.iter().find(|(_, a)| *a == Some(at)) {
            return (index, at);
        }
        sleep(Duration::from_millis(1)).await;
    }
    panic!("{on} did not append the entry");
}

/// `on` handed entry `index` at `at` to its engine as committed before
/// its apply deadline, though openraft has not applied it there.
async fn committed_in_time(set: &Set, on: NodeId, index: u64, at: u64) {
    let t = timing();
    sleep(Duration::from_millis(30)).await;
    let audio = set.audio(on);
    let handed = audio
        .committed_at(index)
        .unwrap_or_else(|| panic!("{on} never learned entry {index} committed"));
    assert!(handed < t.apply_deadline(at), "learned after the deadline");
    let unresolved = audio.lock().unresolved.clone();
    assert!(
        unresolved.iter().all(|&(i, _)| i != index),
        "entry {index} at {at} is unresolved: {unresolved:?}, appended {:?}",
        audio.lock().appended
    );
    assert!(
        set.replica(on).store().applied_index() < index,
        "openraft applied it: the test proves nothing"
    );
}

#[tokio::test(start_paused = true)]
async fn a_commit_learned_only_from_the_other_follower_reaches_the_engine() {
    let set = start().await;
    let leader = set.leader().await;
    let mut others = set.ids().filter(|&id| id != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    // a appends the entry, and the leader never hears that it did: it
    // sends a nothing more, a commit advance included.
    set.net.hold_replies(leader, a);
    let (index, at) = append_ahead(&set, leader, a).await;
    // Nor do the leader's reports reach a: only b's do.
    set.net.cut_one_way(leader, a);
    assert!(set.replica(b).raft().metrics().borrow().state != openraft::ServerState::Leader);
    committed_in_time(&set, a, index, at).await;
}

#[tokio::test(start_paused = true)]
async fn a_stalled_append_response_leaves_the_commit_to_the_listing() {
    let set = start().await;
    let leader = set.leader().await;
    let mut others = set.ids().filter(|&id| id != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    set.net.hold_replies(leader, a);
    let (index, at) = append_ahead(&set, leader, a).await;
    // The leader commits a second entry with b; a never receives it, so
    // the leader's committed id lies past a's log end.
    set.net.cut_one_way(b, a);
    let second = (at + BLOCK, index + 1);
    set.replica(leader).append(Body::InputUp {
        slot: 0,
        at_index: second.0,
    });
    committed_in_time(&set, a, index, at).await;
    assert!(
        set.replica(a).store().log_id_at(second.1).is_none(),
        "a holds only the first entry"
    );
}

#[tokio::test(start_paused = true)]
async fn a_witness_is_ready_only_while_in_contact() {
    use samplez_replica::membership::{Slot, node_id};
    use samplez_service::ReplicatedState;
    let witness = node_id(Slot::Witness, 7);
    let set = Set::start(Options {
        ids: Some(vec![0, 1, witness]),
        contact: Some(timing()),
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    assert!(set.replica(witness).replica().ready, "in contact");
    set.net.isolate(witness, set.ids());
    sleep(Duration::from_millis(20)).await;
    assert!(!set.replica(witness).replica().ready, "cut off from both");
    for id in [0, 1] {
        assert!(set.replica(id).replica().ready, "{id} keeps its readiness");
    }
    set.net.heal();
    sleep(Duration::from_millis(20)).await;
    assert!(set.replica(witness).replica().ready, "in contact again");
}

#[tokio::test(start_paused = true)]
async fn a_quiescing_leader_waits_for_every_voter_heard_to_report_the_commit() {
    let set = start().await;
    let leader = set.leader().await;
    let mut others = set.ids().filter(|&id| id != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    // a takes entries and acknowledges them, and learns no commit: not
    // from openraft, whose commit advances are empty appends, and not
    // from the others' reports. Its own reports reach the leader.
    set.net.drop_heartbeats(leader, a);
    set.net.cut_peer(leader, a);
    set.net.cut_peer(b, a);
    let (index, _) = append_ahead(&set, leader, a).await;
    while set.replica(leader).store().applied_index() < index {
        sleep(Duration::from_millis(1)).await;
    }
    let bound = Duration::from_millis(200);
    let started = tokio::time::Instant::now();
    set.replica(leader).quiesce(bound).await;
    assert!(
        started.elapsed() >= bound,
        "the leader quiesced after {:?} while a reported the entry uncommitted",
        started.elapsed()
    );
    assert!(
        set.audio(a).committed_at(index).is_none(),
        "a learned the commit: the test proves nothing"
    );
    // Once a learns it from b's reports, the leader quiesces at once.
    set.net.heal();
    let started = tokio::time::Instant::now();
    set.replica(leader).quiesce(bound).await;
    assert!(started.elapsed() < bound);
}
