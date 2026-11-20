//! Input liveness in an in-process set: whether an input is up or down
//! is the majority of the audio voters' reports, not the leader's engine
//! alone. Every audio replica's engine is a [`harness::FakeAudio`] told
//! which input slots it reads; the test plays the product, appending what
//! the leader hands on ([`harness::Set::follow_liveness`]).

mod harness;

use std::time::Duration;

use harness::{BLOCK, FOR_GOOD, Options, Set};
use openraft::ServerState;
use samplez_replica::contact::{Timing, time_of};
use samplez_replica::entry::NodeId;
use samplez_replica::membership::{Slot, node_id};
use samplez_replica::state::ReplicaState;
use samplez_service::ReplicatedState;
use tokio::time::{Instant, sleep};

/// Input slot 0, by bit.
const IN: u64 = 1;
const DOWN: (u32, bool) = (0, false);
const UP: (u32, bool) = (0, true);

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

/// `pruneAfterBlocks` of the default timing.
fn debounce() -> Duration {
    let t = Timing::defaults(BLOCK);
    Duration::from_nanos(time_of(u64::from(t.prune_after_blocks) * BLOCK))
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

/// A set of `n` audio voters with an output each, every one reading the
/// input and committing.
async fn start(n: u16) -> Set {
    start_timed(n, Timing::defaults(BLOCK)).await
}

/// [`start`] with contact's timers `timing`.
async fn start_timed(n: u16, timing: Timing) -> Set {
    let set = Set::start(Options {
        ids: Some((0..n).map(a).collect()),
        contact: Some(timing),
        outputs: 1,
        ..Options::default()
    })
    .await;
    for id in set.ids() {
        set.audio(id).set_inputs(IN, 0);
    }
    until("committing", 5, || {
        set.ids()
            .all(|id| state(&set, id) == Some(ReplicaState::Committing))
    })
    .await;
    set
}

fn reads(set: &Set, id: NodeId, readable: bool) {
    let (r, u) = if readable { (IN, 0) } else { (0, IN) };
    set.audio(id).set_inputs(r, u);
}

/// The input entries in `leader`'s log.
fn logged(set: &Set, leader: NodeId) -> Vec<(u32, bool)> {
    set.input_entries(leader)
}

/// The leader alone loses the input, its own mirror gone: the input stays
/// up for the set, and the leader, whose output then differs from its
/// peers', is caught by its page hashes and withholds while the other two
/// commit; it stays Ready, as they commit.
#[tokio::test(start_paused = true)]
async fn the_leader_alone_losing_an_input_takes_it_down_for_nobody() {
    let set = start(3).await;
    let leader = set.leader().await;
    reads(&set, leader, false);
    // Its engine silences the input from here: other samples than its
    // peers'.
    let k = set.audio(leader).due().unwrap() + 30;
    set.audio(leader).glitch(k, FOR_GOOD, 0.5);
    set.follow_liveness(Duration::from_secs(1)).await;
    assert_eq!(set.leader().await, leader);
    assert_eq!(logged(&set, leader), [], "no input entry");
    until("the leader diverged", 3, || {
        state(&set, leader) == Some(ReplicaState::Diverged)
    })
    .await;
    // Ready all the same: the other two commit.
    assert!(set.replica(leader).replica().ready, "the leader serves");
    for id in set.ids().filter(|&id| id != leader) {
        assert!(set.replica(id).replica().ready, "{id} serves");
        assert_eq!(state(&set, id), Some(ReplicaState::Committing), "{id}");
    }
}

/// One of three losing the input changes nothing; a second makes a
/// majority, which takes it down once; the two reading it again bring it
/// up once.
#[tokio::test(start_paused = true)]
async fn a_majority_takes_an_input_down_once_and_up_once() {
    let set = start(3).await;
    let leader = set.leader().await;
    let others: Vec<NodeId> = set.ids().filter(|&id| id != leader).collect();
    reads(&set, others[0], false);
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, leader), [], "one of three");
    reads(&set, others[1], false);
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, leader), [DOWN]);
    reads(&set, others[0], true);
    reads(&set, others[1], true);
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, leader), [DOWN, UP]);
}

/// With one voter of three silent, the two others decide: one losing the
/// input and one reading it change nothing, both losing it take it down.
/// The silent voter's last report, which lost the input, counts for
/// neither.
#[tokio::test(start_paused = true)]
async fn with_one_voter_silent_the_other_two_must_agree() {
    let set = start(3).await;
    let leader = set.leader().await;
    let others: Vec<NodeId> = set.ids().filter(|&id| id != leader).collect();
    let (silent, other) = (others[0], others[1]);
    reads(&set, silent, false);
    sleep(Duration::from_millis(20)).await;
    set.net.isolate(silent, set.ids());
    sleep(Duration::from_millis(50)).await;
    reads(&set, other, false);
    set.follow_liveness(Duration::from_millis(500)).await;
    assert_eq!(logged(&set, leader), [], "one down, one up");
    reads(&set, leader, false);
    set.follow_liveness(Duration::from_millis(500)).await;
    assert_eq!(logged(&set, leader), [DOWN], "both down");
}

/// A leader restarted in a rolling update reads no input at first and
/// leads again: the two others still read the input, so it is not
/// silenced.
#[tokio::test(start_paused = true)]
async fn a_restarted_leader_reading_nothing_silences_no_input() {
    let mut set = start(3).await;
    let rolled = set.leader().await;
    set.on_start(rolled, |audio| audio.set_inputs(0, IN));
    set.restart(rolled).await;
    // Its peers refuse a vote while they hear a leader: elections until
    // one finds the lease lapsed.
    let raft = set.replica(rolled).raft().clone();
    let deadline = Instant::now() + Duration::from_secs(60);
    while set.leader().await != rolled {
        assert!(Instant::now() < deadline, "the restarted replica never led");
        let _ = raft.trigger().elect().await;
        let tried = Instant::now() + Duration::from_secs(1);
        while Instant::now() < tried && raft.metrics().borrow().state != ServerState::Leader {
            sleep(Duration::from_millis(10)).await;
        }
    }
    until("committing again", 5, || {
        state(&set, rolled) == Some(ReplicaState::Committing)
    })
    .await;
    set.follow_liveness(Duration::from_secs(1)).await;
    assert_eq!(set.leader().await, rolled);
    assert_eq!(logged(&set, rolled), [], "no input silenced");
}

/// The only audio voter decides alone, as its engine always did: the
/// input goes down once it cannot read it and comes up once it can.
#[tokio::test(start_paused = true)]
async fn a_single_voter_decides_on_its_own_engine() {
    let set = start(1).await;
    let only = a(0);
    reads(&set, only, false);
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, only), [DOWN]);
    reads(&set, only, true);
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, only), [DOWN, UP]);
}

/// A majority flapping faster than `pruneAfterBlocks` appends nothing;
/// one changing slower appends one entry per settled change, no burst.
#[tokio::test(start_paused = true)]
async fn a_flapping_input_appends_once_per_settled_change() {
    let set = start(3).await;
    let leader = set.leader().await;
    let ids: Vec<NodeId> = set.ids().collect();
    let all = |r: bool| {
        for &id in &ids {
            reads(&set, id, r);
        }
    };
    let fast = debounce() * 2 / 5;
    for i in 0..20 {
        all(i % 2 == 1);
        set.follow_liveness(fast).await;
    }
    assert_eq!(logged(&set, leader), [], "faster than the debounce");
    let slow = debounce() * 4;
    for i in 0..4 {
        all(i % 2 == 1);
        set.follow_liveness(slow).await;
    }
    assert_eq!(logged(&set, leader), [DOWN, UP, DOWN, UP]);
}

/// A majority losing the input for two seconds appends one `InputDown`:
/// once the log shows it, nothing is offered again, past the second a
/// change handed on waits for the log too.
#[tokio::test(start_paused = true)]
async fn a_down_held_for_seconds_is_appended_once() {
    let set = start(3).await;
    let leader = set.leader().await;
    for id in set.ids().filter(|&id| id != leader) {
        reads(&set, id, false);
    }
    set.follow_liveness(Duration::from_secs(2)).await;
    assert_eq!(set.leader().await, leader);
    assert_eq!(logged(&set, leader), [DOWN]);
}

/// Reads its engine in `starting` as unreadable from `id`'s next start
/// on, its rebuild held, so it stays in `starting`.
fn restart_unreadable(set: &mut Set, id: NodeId) {
    set.on_start(id, |audio| {
        audio.set_inputs(0, IN);
        audio.lock().rebuild_held = true;
    });
}

/// A replica in `starting` has no say, whatever its engine reports: with
/// one voter of three losing the input and a second restarted and held
/// in `starting` reporting it unreadable, nothing is appended.
#[tokio::test(start_paused = true)]
async fn a_replica_in_starting_has_no_say() {
    let mut set = start(3).await;
    let leader = set.leader().await;
    let others: Vec<NodeId> = set.ids().filter(|&id| id != leader).collect();
    let (lost, restarted) = (others[0], others[1]);
    reads(&set, lost, false);
    restart_unreadable(&mut set, restarted);
    set.restart(restarted).await;
    until("the restarted replica reports", 5, || {
        state(&set, restarted) == Some(ReplicaState::Starting)
    })
    .await;
    set.follow_liveness(Duration::from_millis(500)).await;
    assert_eq!(state(&set, restarted), Some(ReplicaState::Starting));
    assert_eq!(set.leader().await, leader);
    assert_eq!(logged(&set, leader), []);
}

/// The only audio voter, restarted, decides nothing while it is in
/// `starting`, though its engine reports the input unreadable. A lone
/// voter verifies its clock on its own at once, so it takes no clock hold
/// and no rebuild: it leaves `starting` once ready, which needs the
/// generation its engine builds first. That build is held here.
#[tokio::test(start_paused = true)]
async fn a_single_voter_in_starting_decides_nothing() {
    let mut set = start(1).await;
    let only = a(0);
    set.on_start(only, |audio| {
        audio.set_inputs(0, IN);
        audio.lock().generation_pending = true;
    });
    set.restart(only).await;
    set.leader().await;
    until("the restarted voter reports", 5, || {
        state(&set, only) == Some(ReplicaState::Starting)
    })
    .await;
    set.follow_liveness(Duration::from_millis(500)).await;
    assert_eq!(state(&set, only), Some(ReplicaState::Starting));
    assert_eq!(logged(&set, only), []);
}

/// After an `InputUp`, every engine waits for the generation it needs
/// longer than `pruneAfterBlocks` and says nothing of the input
/// meanwhile: no `InputDown` follows.
#[tokio::test(start_paused = true)]
async fn an_input_waiting_for_its_generation_is_not_taken_down() {
    let set = start(3).await;
    let leader = set.leader().await;
    let ids: Vec<NodeId> = set.ids().collect();
    for &id in &ids {
        reads(&set, id, false);
    }
    set.follow_liveness(Duration::from_millis(300)).await;
    for &id in &ids {
        reads(&set, id, true);
    }
    set.follow_liveness(Duration::from_millis(300)).await;
    assert_eq!(logged(&set, leader), [DOWN, UP]);
    for &id in &ids {
        set.audio(id).set_inputs(0, 0);
    }
    set.follow_liveness(debounce() * 10).await;
    assert_eq!(logged(&set, leader), [DOWN, UP]);
}

/// A voter's copy from before the leader lost contact does not count once
/// contact is regained, until a fresh one replaces it: the voter whose
/// last report lost the input, cut off since, and one more voter losing
/// it make no majority. A debounce of one block lets the moments right
/// after the contact is regained decide.
#[tokio::test(start_paused = true)]
async fn a_copy_from_before_a_loss_of_contact_does_not_count() {
    let set = start_timed(
        3,
        Timing {
            prune_after_blocks: 1,
            ..Timing::defaults(BLOCK)
        },
    )
    .await;
    let leader = set.leader().await;
    let others: Vec<NodeId> = set.ids().filter(|&id| id != leader).collect();
    let (stale, other) = (others[0], others[1]);
    reads(&set, stale, false);
    sleep(Duration::from_millis(20)).await;
    // Its reports reach nobody from here on; the raft RPCs still flow.
    set.net.cut_peer(stale, leader);
    set.net.cut_peer(stale, other);
    sleep(Duration::from_millis(40)).await;
    reads(&set, other, false);
    // The leader hears nobody for longer than the contact window.
    set.net.cut_peer(other, leader);
    sleep(Duration::from_millis(30)).await;
    set.net.join_peer(other, leader);
    set.follow_liveness(Duration::from_millis(200)).await;
    assert_eq!(set.leader().await, leader);
    assert_eq!(logged(&set, leader), []);
}
