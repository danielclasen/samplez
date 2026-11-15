//! Replica set rows over the in-process harness.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{Options, Set, owner};
use samplez_replica::dedup::Limits;
use samplez_service::ReplicatedState;
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};

async fn set(replica: &impl ReplicatedState, session: &str, id: &str, command: &str) -> u64 {
    ack(replica, session, id, command).await.revision
}

async fn ack(replica: &impl ReplicatedState, session: &str, id: &str, command: &str) -> Ack {
    replica
        .submit(&owner(session), id, Bytes::from(command.to_owned()))
        .await
        .expect("committed")
}

#[tokio::test(start_paused = true)]
async fn a_restarted_replica_is_back_at_its_last_applied_revision_before_any_election() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let mut last = 0;
    for n in 1..=5 {
        last = set(
            &**set_.replica(leader),
            "s",
            &n.to_string(),
            &format!("gain={n}@{n}"),
        )
        .await;
    }
    set_.applied(last).await;
    let text = set_.replica(leader).snapshot().await.text;
    for id in set_.ids() {
        set_.crash(id).await;
    }
    // Alone, the replica cannot win an election: what it applies comes
    // from its own log up to the saved committed id.
    let follower = (leader + 1) % 3;
    set_.restart(follower).await;
    let replica = set_.replica(follower);
    assert_eq!(replica.revision(), last);
    assert_eq!(replica.snapshot().await.text, text);
    assert!(
        replica.replica().ready,
        "applied up to the saved committed id"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(replica.raft().metrics().borrow().current_leader, None);
    assert_eq!(replica.revision(), last);
    let info = replica.replica();
    assert!(info.ready, "readiness does not wait for a leader");
    assert!(!info.commitable, "no leader, so it orders nothing");
}

async fn refused(replica: &impl ReplicatedState, id: &str) -> NotApplied {
    replica
        .submit(&owner("s"), id, Bytes::from("gain=1@1"))
        .await
        .expect_err("not applied")
}

#[tokio::test(start_paused = true)]
async fn only_the_leader_is_commitable_and_every_refusal_names_its_reason() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    for id in set_.ids() {
        let info = set_.replica(id).replica();
        assert!(info.ready);
        assert_eq!(info.commitable, id == leader, "replica {id}");
    }
    // A follower forwards nothing, so it orders nothing.
    let follower = (leader + 1) % 3;
    let not_ordered = refused(&**set_.replica(follower), "f").await;
    assert_eq!(not_ordered.reason, Reason::NotCommitable);
    assert_eq!(
        set_.replica(follower).command_revision(&owner("s"), "f"),
        None
    );

    // A leader cut off from its quorum appends but cannot commit.
    set_.net.isolate(leader, set_.ids());
    let timed_out = refused(&**set_.replica(leader), "q").await;
    assert_eq!(timed_out.reason, Reason::Unavailable);
    assert_eq!(timed_out.detail, "timed out");

    // A stopped replica answers every command it still gets.
    let stopped = set_.replica(leader).clone();
    set_.crash(leader).await;
    assert_eq!(refused(&*stopped, "x").await.reason, Reason::Unavailable);
}

#[tokio::test(start_paused = true)]
async fn a_resend_after_failover_and_election_resolves_to_the_stored_revision() {
    let mut set_ = Set::start(Options::default()).await;
    let first = set_.leader().await;
    let revision = set(&**set_.replica(first), "s", "cmd-1", "gain=1@1").await;
    set_.applied(revision).await;
    set_.crash(first).await;
    let second = set_.leader().await;
    assert_ne!(first, second);
    let replica = set_.replica(second);
    assert_eq!(
        replica.command_revision(&owner("s"), "cmd-1"),
        Some(revision)
    );
    assert_eq!(set(&**replica, "s", "cmd-1", "gain=1@1").await, revision);
    assert_eq!(replica.store().with_state(|sm| sm.product.applied), 1);
    // The same id from another session is another command.
    assert_ne!(set(&**replica, "t", "cmd-1", "pan=1@1").await, revision);
}

#[tokio::test(start_paused = true)]
async fn a_coalesced_entry_acknowledges_every_merged_id() {
    let set_ = Set::start(Options::default()).await;
    let replica = set_.replica(set_.leader().await);
    // Submitted before the writer runs, so they form one entry.
    let pending = [
        ("s", "1", "gain=1@1"),
        ("t", "1", "gain=2@2"),
        ("s", "2", "gain=3@3"),
        ("s", "3", "pan=0@4"),
    ]
    .map(|(session, id, command)| replica.submit(&owner(session), id, Bytes::from(command)));
    let mut revisions = Vec::new();
    let mut resolutions = Vec::new();
    for ack in pending {
        let ack = ack.await.expect("committed");
        revisions.push(ack.revision);
        resolutions.push(ack.resolution);
    }
    assert_eq!(
        resolutions,
        [
            Resolution::Superseded,
            Resolution::Superseded,
            Resolution::Applied,
            Resolution::Applied
        ]
    );
    assert!(
        revisions.iter().all(|r| *r == revisions[0]),
        "{revisions:?}"
    );
    replica.store().with_state(|sm| {
        assert_eq!(sm.product.applied, 2);
        assert_eq!(sm.product.values["gain"].0, "3");
    });
    // A superseded id is recorded: its resend changes nothing.
    let resent = ack(&**replica, "s", "1", "gain=1@1").await;
    assert_eq!(resent.revision, revisions[0]);
    assert_eq!(resent.resolution, Resolution::Duplicate);
    assert_eq!(
        replica
            .store()
            .with_state(|sm| sm.product.values["gain"].0.clone()),
        "3"
    );
}

#[tokio::test(start_paused = true)]
async fn a_delayed_copy_is_a_duplicate_within_retention_and_stale_after_it() {
    let set_ = Set::start(Options::default()).await;
    let replica = set_.replica(set_.leader().await);
    let seven = set(&**replica, "s", "7", "gain=7@7").await;
    set(&**replica, "s", "8", "gain=8@8").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let within = ack(&**replica, "s", "7", "gain=7@7").await;
    assert_eq!(
        (within.revision, within.resolution),
        (seven, Resolution::Duplicate)
    );
    tokio::time::sleep(Duration::from_secs(11)).await;
    let after = ack(&**replica, "s", "7", "gain=7@7").await;
    assert_ne!(after.revision, seven);
    assert_eq!(after.resolution, Resolution::Stale, "the client is told");
    replica.store().with_state(|sm| {
        assert_eq!(sm.product.values["gain"].0, "8");
        assert_eq!(sm.product.applied, 2);
    });
}

#[tokio::test(start_paused = true)]
async fn tables_stay_identical_under_the_caps_and_after_a_snapshot_install() {
    let mut options = Options {
        limits: Limits {
            max_session_commands: 16,
            max_dedup_entries: 40,
            ..Limits::defaults(harness::RATE)
        },
        ..Options::default()
    };
    // One snapshot, taken by hand once the writes are in: a snapshot
    // built while the leader retries an unreachable peer is sent in place
    // of the one it planned, which openraft 0.9.21 debug-asserts against
    // (progress/inflight `ack`).
    options.raft.snapshot_policy = openraft::SnapshotPolicy::Never;
    options.raft.max_in_snapshot_log_to_keep = 0;
    options.raft.purge_batch_size = 1;
    let mut set_ = Set::start(options).await;
    let leader = set_.leader().await;
    let behind = (leader + 1) % 3;
    set_.crash(behind).await;
    // 50 writes/s on two controls in each of three sessions.
    let mut last = 0;
    for n in 1..=150u64 {
        let session = ["a", "b", "c"][(n % 3) as usize];
        let path = ["gain", "pan"][(n % 2) as usize];
        let command = format!("{session}.{path}={n}@{n}");
        last = set(&**set_.replica(leader), session, &n.to_string(), &command).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let raft = set_.replica(leader).raft().clone();
    raft.trigger().snapshot().await.expect("snapshot");
    while raft.metrics().borrow().snapshot.map(|id| id.index) != Some(last) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raft.trigger().purge_log(last).await.expect("purge");
    while raft.metrics().borrow().purged.map(|id| id.index) != Some(last) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    set_.restart(behind).await;
    set_.applied(last).await;
    assert!(
        set_.replica(behind)
            .raft()
            .metrics()
            .borrow()
            .snapshot
            .is_some(),
        "the restarted replica installed a snapshot"
    );
    let tables: Vec<_> = set_
        .running()
        .map(|id| set_.replica(id).store().with_state(|sm| sm.dedup.save()))
        .collect();
    assert!(tables.iter().all(|t| *t == tables[0]));
    assert_eq!(tables[0].records.len(), 40);
    assert!(
        set_.replica(leader)
            .store()
            .with_state(|sm| sm.dedup.early_retired())
            > 0
    );
    // A repeat of an id retired early applies again: the product refuses
    // it as stale and the state does not change.
    let leader = set_.leader().await;
    let replica = set_.replica(leader);
    assert_eq!(replica.command_revision(&owner("b"), "1"), None);
    let before = replica.snapshot().await.text;
    let applied = replica.store().with_state(|sm| sm.product.applied);
    set(&**replica, "b", "1", "b.pan=1@1").await;
    assert_eq!(replica.snapshot().await.text, before);
    assert_eq!(replica.store().with_state(|sm| sm.product.applied), applied);
}
