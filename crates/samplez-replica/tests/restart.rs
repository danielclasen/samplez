//! A replica restarted inside a running set: the set agrees on a leader
//! and every audio replica commits again within a bound derived from the
//! election timeout.

mod harness;

use std::collections::BTreeSet;
use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez_replica::contact::Timing;
use samplez_replica::entry::NodeId;
use samplez_replica::membership::{Slot, Voters, node_id, slot_of};
use samplez_replica::raft_config;
use samplez_replica::state::ReplicaState;
use tokio::time::{Instant, sleep};

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

/// A few election rounds: a vote, a lost race and a heartbeat each.
fn bound() -> Duration {
    Duration::from_millis(10 * raft_config("test").election_timeout_max)
}

async fn until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {within:?}");
        sleep(Duration::from_millis(1)).await;
    }
}

fn committing(set: &Set, id: NodeId) -> bool {
    set.replica(id)
        .contact_metrics()
        .and_then(|m| m.replica)
        .and_then(|r| r.state)
        == Some(ReplicaState::Committing)
}

fn agreed_leader(set: &Set) -> Option<NodeId> {
    let mut leaders = set
        .running()
        .map(|id| set.replica(id).raft().metrics().borrow().current_leader);
    let first = leaders.next()??;
    leaders.all(|l| l == Some(first)).then_some(first)
}

async fn all_committing(set: &Set, what: &str) {
    let ids: Vec<NodeId> = set
        .running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect();
    until(&format!("{what}: a leader agreed"), bound(), || {
        agreed_leader(set).is_some()
    })
    .await;
    until(
        &format!("{what}: every audio replica committing"),
        bound(),
        || ids.iter().all(|&id| committing(set, id)),
    )
    .await;
}

/// The non-baseline audio follower, else any audio follower.
fn follower(set: &Set, leader: NodeId) -> NodeId {
    let base = set
        .replica(leader)
        .store()
        .with_state(|sm| sm.baseline.clone())
        .map(|b| b.replica);
    let audio: Vec<NodeId> = set
        .running()
        .filter(|&id| id != leader && slot_of(id) != Slot::Witness)
        .collect();
    audio
        .iter()
        .copied()
        .find(|&id| Some(id) != base)
        .unwrap_or(audio[0])
}

/// Restarts a follower, then the leader, each with `set.restart` alone
/// as a process restart in place: every replica commits again within the
/// bound, and the set keeps its leader for a while after.
async fn follower_then_leader(ids: Vec<NodeId>) {
    let mut set = Set::start(Options {
        ids: Some(ids),
        contact: Some(Timing::defaults(BLOCK)),
        outputs: 1,
        ..Options::default()
    })
    .await;
    all_committing(&set, "started").await;
    let leader = set.leader().await;
    let follower = follower(&set, leader);
    set.restart(follower).await;
    all_committing(&set, "follower restarted").await;
    stays(&set, "follower restarted").await;
    let leader = set.leader().await;
    set.restart(leader).await;
    all_committing(&set, "leader restarted").await;
    stays(&set, "leader restarted").await;
}

/// For a few election timeouts: one leader agreed and every audio
/// replica committing throughout.
async fn stays(set: &Set, what: &str) {
    let leader = agreed_leader(set);
    for _ in 0..bound().as_millis() {
        assert_eq!(agreed_leader(set), leader, "{what}: the leader moved");
        for id in set.running().filter(|&id| slot_of(id) != Slot::Witness) {
            assert!(committing(set, id), "{what}: {id} stopped committing");
        }
        sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn two_audio_and_a_witness_commit_again_after_a_follower_and_a_leader_restart() {
    follower_then_leader(vec![a(0), a(1), witness()]).await;
}

#[tokio::test(start_paused = true)]
async fn three_audio_commit_again_after_a_follower_and_a_leader_restart() {
    follower_then_leader(vec![a(0), a(1), a(2)]).await;
}

/// A replica that lost its volume rejoins under a new node id as a
/// learner, which reports its configuration and protocol to the voters:
/// the leader, running contact, promotes it in the old node's place and it
/// commits again.
#[tokio::test(start_paused = true)]
async fn a_replica_rejoining_on_an_empty_volume_is_promoted_while_contact_runs() {
    let mut set = Set::start(Options {
        ids: Some(vec![a(0), a(1), a(2)]),
        contact: Some(Timing::defaults(BLOCK)),
        outputs: 1,
        wanted: Some(Voters::new(3, false).unwrap()),
        ..Options::default()
    })
    .await;
    all_committing(&set, "started").await;
    let leader = set.leader().await;
    let old = follower(&set, leader);
    set.crash(old).await;
    set.wipe(old);
    let Slot::Audio(ordinal) = slot_of(old) else {
        unreachable!("an audio replica")
    };
    let new = node_id(Slot::Audio(ordinal), 10);
    set.restart(new).await;
    let voters = |set: &Set| -> BTreeSet<NodeId> {
        let leader = set.replica(leader);
        let metrics = leader.raft().metrics().borrow().clone();
        metrics.membership_config.membership().voter_ids().collect()
    };
    until("the new node promoted", Duration::from_secs(30), || {
        voters(&set).contains(&new) && !voters(&set).contains(&old)
    })
    .await;
    all_committing(&set, "rejoined").await;
}
