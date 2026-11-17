//! Unresolved entries decided in the log, across a replica set: one
//! decision per entry whatever the leader does meanwhile, and no entry
//! applied twice.

mod harness;

use std::time::Duration;

use harness::{BLOCK, Options, Set};
use samplez_replica::contact::{Timing, index_at};
use samplez_replica::entry::{Body, NodeId, Request};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::state::ReplicaState;
use tokio::time::{Instant, sleep};

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
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

async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {secs} s");
        sleep(Duration::from_millis(1)).await;
    }
}

fn committing(set: &Set, id: NodeId) -> bool {
    set.replica(id)
        .contact_metrics()
        .and_then(|m| m.replica?.state)
        == Some(ReplicaState::Committing)
}

fn audio(set: &Set) -> Vec<NodeId> {
    set.running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

/// The log indexes of the decisions `id`'s log holds on entry `index`.
fn decisions(set: &Set, id: NodeId, index: u64) -> Vec<u64> {
    let mut out = Vec::new();
    set.replica(id).store().entries_after(
        0,
        &mut |log_id: samplez_replica::snapshot::StoredLogId, request: &Request| {
            if request.body.decides().map(|(i, _)| i) == Some(index) {
                out.push(log_id.index);
            }
        },
    );
    out
}

/// The engine entries `id` was handed at or after log index `from`.
fn engine_entries(set: &Set, id: NodeId, from: u64) -> usize {
    set.audio(id)
        .lock()
        .appended
        .iter()
        .filter(|(i, at)| *i >= from && at.is_some())
        .count()
}

/// An entry appended 40 ms ahead while the followers take every Raft
/// message 60 ms late: its commit is known nowhere by its apply deadline,
/// so it is unresolved on every replica. `kill` crashes the leader once
/// it appended the entry. Returns the entry's log index on the survivors.
async fn late_everywhere(kill: bool) -> (Set, u64) {
    let mut set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter().all(|&id| committing(&set, id))
    })
    .await;
    let leader = set.leader().await;
    for &id in &ids {
        if id != leader {
            set.net.stall(id, Duration::from_millis(60));
        }
    }
    let at = (index_at(set.clock(leader).tai_ns()) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader).append(Body::InputDown {
        slot: 0,
        at_index: at,
    });
    let mut index = None;
    until("appended on the leader", 1, || {
        index = set
            .audio(leader)
            .lock()
            .appended
            .iter()
            .find(|(_, x)| *x == Some(at))
            .map(|(i, _)| *i);
        index.is_some()
    })
    .await;
    let index = index.unwrap();
    if kill {
        set.crash(leader).await;
    }
    sleep(Duration::from_millis(100)).await;
    for &id in &ids {
        set.net.unstall(id);
    }
    (set, index)
}

/// The leader's commits stall past the lead: the entry is late on every
/// replica, the leader decides it once, and no replica is handed it more
/// than once at its fresh index, also long after the decision's
/// in-flight window.
#[tokio::test(start_paused = true)]
async fn an_entry_late_through_a_stalled_leader_is_decided_once() {
    let (set, index) = late_everywhere(false).await;
    let ids = audio(&set);
    let leader = set.leader().await;
    until("decided", 5, || !decisions(&set, leader, index).is_empty()).await;
    sleep(Duration::from_secs(3)).await;
    for &id in &ids {
        assert_eq!(decisions(&set, id, index).len(), 1, "on {id}");
        assert!(
            engine_entries(&set, id, index) <= 2,
            "{id} handed the entry more than once again: {:?}",
            set.audio(id).lock().appended
        );
    }
}

/// As above with the leader killed once it appended the entry: the new
/// leader decides each id the survivors report at most once.
#[tokio::test(start_paused = true)]
async fn an_entry_in_flight_when_the_leader_dies_is_decided_once() {
    let (set, index) = late_everywhere(true).await;
    let ids = audio(&set);
    let leader = set.leader().await;
    sleep(Duration::from_secs(3)).await;
    for &id in &ids {
        let held = decisions(&set, id, index).len();
        assert!(held <= 1, "{id} holds {held} decisions on {index}");
        assert!(engine_entries(&set, id, index) <= 2, "on {id}");
    }
    assert!(
        ids.iter().any(|&id| committing(&set, id)),
        "the survivors commit again under leader {leader}"
    );
}
