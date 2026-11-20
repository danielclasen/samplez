//! Contact between the voters of an in-process set: side reports, the
//! contact fence F, silence, and commit knowledge from reports. Every
//! replica's engine is a [`harness::FakeAudio`] computing each block as it
//! falls due on the replica's TAI clock.

mod harness;

use std::collections::BTreeSet;
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
/// holds it in its log; its log index and at_index.
async fn append_ahead(set: &Set, leader: NodeId, on: NodeId) -> (u64, u64) {
    let at = (index_at(tai(set)) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader)
        .append(Body::InputDown {
            slot: 0,
            at_index: at,
        })
        .unwrap();
    for _ in 0..20 {
        let mut found = None;
        set.replica(on).store().entries_after(0, |id, request| {
            if request.body.at_index() == Some(at) {
                found = Some(id.index);
            }
        });
        if let Some(index) = found {
            return (index, at);
        }
        sleep(Duration::from_millis(1)).await;
    }
    panic!("{on} did not append the entry");
}

/// `on` learned entry `index` committed from reports before its apply
/// deadline, but openraft did not apply it there by then: it is
/// unresolved, and what it derived never reaches the engine, however
/// soon the commit was known.
async fn unresolved_though_known_committed(set: &Set, on: NodeId, index: u64, at: u64) {
    let t = timing();
    let mut known = false;
    while set.clock(on).tai_ns() < t.apply_deadline(at) {
        known |= set.replica(on).contact_metrics().unwrap().known_committed >= index;
        sleep(Duration::from_millis(1)).await;
    }
    assert!(known, "{on} never learned entry {index} committed in time");
    sleep(Duration::from_millis(30)).await;
    let audio = set.audio(on);
    let unresolved = audio.lock().unresolved.clone();
    assert!(
        unresolved.iter().any(|&(i, a)| (i, a) == (index, at)),
        "entry {index} at {at} is not unresolved: {unresolved:?}"
    );
    assert!(
        audio.lock().appended.iter().all(|e| e.0 != index),
        "entry {index} reached the engine"
    );
    assert!(
        set.replica(on).store().applied_index() < index,
        "openraft applied it: the test proves nothing"
    );
}

/// A commit a follower learns only from the other follower's reports
/// does not hand the entry to its engine: only its apply does, so an
/// entry not applied by its deadline is unresolved there.
#[tokio::test(start_paused = true)]
async fn a_commit_learned_only_from_the_other_follower_leaves_the_entry_unresolved() {
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
    unresolved_though_known_committed(&set, a, index, at).await;
}

#[tokio::test(start_paused = true)]
async fn a_stalled_append_response_leaves_the_entry_unresolved_though_listed() {
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
    set.replica(leader)
        .append(Body::InputUp {
            slot: 0,
            at_index: second.0,
        })
        .unwrap();
    unresolved_though_known_committed(&set, a, index, at).await;
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

/// A product reading its state through `with_state` and asking for the
/// replica's readiness from inside the closure, while the contact loop
/// ticks: each tick holds the contact lock and reads the store. With the
/// closure under the lock the tick waits for, the two wait for each
/// other for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_with_state_closure_asks_for_readiness_while_contact_ticks() {
    use samplez_service::ReplicatedState;
    let set = start().await;
    let replica = set.replica(0).clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !replica.replica().ready {
        assert!(std::time::Instant::now() < deadline, "never ready");
        sleep(Duration::from_millis(10)).await;
    }
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for _ in 0..20 {
            let ready = replica.store().with_state(|_| {
                // Long enough for a tick to wait for the store while it
                // holds the contact lock.
                std::thread::sleep(Duration::from_millis(20));
                replica.replica().ready
            });
            assert!(ready);
        }
        let _ = done.send(());
    });
    if finished.recv_timeout(Duration::from_secs(5)).is_err() {
        // Deadlocked threads never return, and dropping the runtime
        // waits for the contact task among them: end the process.
        eprintln!("the with_state closure and the contact loop deadlocked");
        std::process::exit(1);
    }
}

/// What `observer` counts of `peer`'s silences: transitions and the
/// longest gap.
fn silences(set: &Set, observer: NodeId, peer: NodeId) -> (u64, Duration) {
    let m = set.replica(observer).contact_metrics().unwrap();
    let p = m.peers.iter().find(|p| p.node == peer).unwrap();
    (p.silent_transitions, p.silent_gap_max)
}

/// Cuts `peer` off for `pause`, then heals and lets reports flow again.
async fn pause_reports(set: &Set, peer: NodeId, pause: Duration) {
    set.net.isolate(peer, set.ids());
    sleep(pause).await;
    set.net.heal();
    sleep(Duration::from_millis(50)).await;
}

#[tokio::test(start_paused = true)]
async fn a_pause_past_the_window_counts_one_transition_and_the_gap() {
    let set = start().await;
    let leader = set.leader().await;
    let cut = set.ids().find(|&id| id != leader).unwrap();
    assert_eq!(silences(&set, leader, cut), (0, Duration::ZERO));

    let pause = Duration::from_millis(30);
    pause_reports(&set, cut, pause).await;

    let (transitions, gap) = silences(&set, leader, cut);
    assert_eq!(transitions, 1);
    assert!(gap >= pause, "gap {gap:?} is shorter than the pause");
    assert!(gap < pause + Duration::from_millis(10), "gap {gap:?}");
}

#[tokio::test(start_paused = true)]
async fn two_pauses_count_two_transitions_and_the_longer_gap() {
    let set = start().await;
    let leader = set.leader().await;
    let cut = set.ids().find(|&id| id != leader).unwrap();

    let long = Duration::from_millis(50);
    pause_reports(&set, cut, long).await;
    pause_reports(&set, cut, Duration::from_millis(30)).await;

    let (transitions, gap) = silences(&set, leader, cut);
    assert_eq!(transitions, 2);
    assert!(gap >= long, "gap {gap:?} is shorter than the longer pause");
    assert!(gap < long + Duration::from_millis(10), "gap {gap:?}");
}

/// A peer stays unheard while the observer is out of contact and after
/// it regains contact through the third voter: its silence, and so its
/// gap, counts from the contact regained, not from its last report
/// heard before the observer lost contact.
#[tokio::test(start_paused = true)]
async fn a_silent_gap_counts_from_the_contact_regained() {
    let set = start().await;
    let observer = set.leader().await;
    let mut others = set.ids().filter(|&id| id != observer);
    let (unheard, third) = (others.next().unwrap(), others.next().unwrap());

    // Out of contact for 100 ms: nobody is held silent meanwhile.
    set.net.isolate(observer, set.ids());
    sleep(Duration::from_millis(100)).await;
    assert_eq!(silences(&set, observer, unheard), (0, Duration::ZERO));

    // Contact regained through `third` alone; `unheard` stays cut off
    // for 30 ms more, then reports again.
    set.net.heal();
    set.net.cut(observer, unheard);
    let held = Duration::from_millis(30);
    sleep(held).await;
    set.net.heal();
    sleep(Duration::from_millis(50)).await;

    let (transitions, gap) = silences(&set, observer, unheard);
    assert_eq!(transitions, 1);
    assert!(gap >= held - Duration::from_millis(4), "gap {gap:?}");
    assert!(
        gap < held + Duration::from_millis(10),
        "gap {gap:?} counts time out of contact"
    );
    assert_eq!(silences(&set, observer, third).0, 0);
}

#[tokio::test(start_paused = true)]
async fn the_exposition_carries_both_silence_metrics_per_peer() {
    use samplez_service::ReplicatedState;
    let set = start().await;
    let leader = set.leader().await;
    let cut = set.ids().find(|&id| id != leader).unwrap();
    pause_reports(&set, cut, Duration::from_millis(30)).await;

    let samples = set.replica(leader).replica_metrics();
    let of = |name: &str, peer: NodeId| {
        samples
            .iter()
            .find(|m| m.name == name && m.labels == [("node", peer.to_string())])
            .unwrap_or_else(|| panic!("no {name} sample for node {peer}"))
    };
    let transitions = of("samplez_replica_peer_silent_transitions_total", cut);
    assert_eq!(transitions.value, 1.0);
    let gap = of("samplez_replica_peer_longest_silent_gap_seconds", cut);
    assert!(gap.value >= 0.030, "{}", gap.value);
    for peer in set.ids().filter(|&id| id != leader) {
        of("samplez_replica_peer_silent_transitions_total", peer);
        of("samplez_replica_peer_longest_silent_gap_seconds", peer);
    }
}

/// A peer that leaves the membership leaves the exposition: its silence
/// metrics and its link counts go with it, so a set whose members change
/// keeps no series of former members.
#[tokio::test(start_paused = true)]
async fn a_removed_peer_leaves_no_series_in_the_exposition() {
    use samplez_service::ReplicatedState;
    let mut set = start().await;
    let leader = set.leader().await;
    let gone = set.ids().find(|&id| id != leader).unwrap();
    let rest: BTreeSet<NodeId> = set.ids().filter(|&id| id != gone).collect();
    // The per-peer series labelled `gone` on each remaining replica.
    let series = |set: &Set| {
        let label = ("node", gone.to_string());
        (rest.iter())
            .flat_map(|&id| set.replica(id).replica_metrics())
            .filter(|m| m.name.starts_with("samplez_replica_peer_") && m.labels.contains(&label))
            .map(|m| m.name)
            .collect::<BTreeSet<_>>()
    };

    // Crashed first, so that it does not ask to join again; its reports
    // stop and the leader's to it get no answer.
    set.crash(gone).await;
    sleep(Duration::from_millis(50)).await;
    let before = series(&set);
    for name in [
        "samplez_replica_peer_silent_transitions_total",
        "samplez_replica_peer_longest_silent_gap_seconds",
        "samplez_replica_peer_report_timeouts_total",
        "samplez_replica_peer_dials_total",
    ] {
        assert!(before.contains(name), "no {name} before the removal");
    }

    let raft = set.replica(leader).raft().clone();
    raft.change_membership(
        openraft::ChangeMembers::ReplaceAllVoters(rest.clone()),
        false,
    )
    .await
    .expect("removed");
    sleep(Duration::from_millis(50)).await;
    assert_eq!(series(&set), BTreeSet::new(), "series of a former member");
}
