//! Replica set rows over the in-process harness.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{Options, Set, owner};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::{Body, SessionKey};
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};
use samplez_service::{Position, ReadIndex, ReplicatedState};

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

/// A replica whose volume was lost starts at revision 0 in the set's
/// epoch: it is not ready, so clients do not see the state go back,
/// until it has joined and applied what was committed.
#[tokio::test(start_paused = true)]
async fn an_empty_volume_is_ready_only_once_it_has_joined() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let revision = set(&**set_.replica(leader), "s", "1", "gain=1@1").await;
    set_.applied(revision).await;
    let follower = (leader + 1) % 3;
    set_.crash(follower).await;
    set_.wipe(follower);
    set_.net.isolate(follower, set_.ids());
    set_.restart(follower).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let replica = set_.replica(follower).clone();
    assert_eq!(replica.revision(), 0);
    assert!(
        !replica.replica().ready,
        "an empty volume has joined nothing"
    );
    set_.net.heal();
    set_.applied(revision).await;
    assert!(replica.replica().ready);
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
        set_.replica(follower).command_resolution(&owner("s"), "f"),
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

/// The leader's read index is what it committed, confirmed by a majority;
/// a follower that knows the leader confirms none, and one cut off from
/// the others says so. Asked again when an election moved the leadership
/// meanwhile.
#[tokio::test(start_paused = true)]
async fn the_leader_confirms_its_read_index_and_a_follower_answers_none() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let revision = set(&**set_.replica(leader), "s", "1", "gain=1@1").await;
    set_.applied(revision).await;
    loop {
        let leader = set_.leader().await;
        let follower = (leader + 1) % 3;
        let start = tokio::time::Instant::now();
        let confirmed = set_.replica(leader).read_index().await;
        let unconfirmed = set_.replica(follower).read_index().await;
        let took = start.elapsed();
        if set_.leader().await != leader {
            continue;
        }
        assert_eq!(
            confirmed,
            ReadIndex::Confirmed(revision),
            "a heartbeat round confirms it"
        );
        assert_eq!(unconfirmed, ReadIndex::NotLeader);
        assert!(took < Duration::from_millis(100), "at once: {took:?}");
        break;
    }

    let follower = (set_.leader().await + 1) % 3;
    set_.net.isolate(follower, set_.ids());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        set_.replica(follower).read_index().await,
        ReadIndex::CutOff,
        "no leader known"
    );
}

/// Revisions count the product's changes, not log entries: lease
/// renewals between two commands, and the leader's blank and membership
/// entries before them, take no revision, so the second command's delta
/// follows the first's, as a client checks.
#[tokio::test(start_paused = true)]
async fn lease_renewals_between_two_product_commands_give_consecutive_revisions() {
    let set_ = Set::start(Options::default()).await;
    let replica = set_.replica(set_.leader().await);
    let first = set(&**replica, "s", "1", "gain=1@1").await;
    assert_eq!(first, 1, "the first change");
    let index = replica.store().applied_index();
    for _ in 0..3 {
        replica.renew_sessions(&[owner("s")]);
    }
    let second = set(&**replica, "s", "2", "pan=1@2").await;
    assert_eq!(
        replica.store().applied_index(),
        index + 4,
        "the renewals were written between the two"
    );
    assert_eq!(second, first + 1);
    let since = Position {
        epoch: replica.epoch(),
        revision: first,
    };
    let deltas = replica.deltas_since(since).await.expect("held");
    let steps: Vec<_> = deltas.iter().map(|d| (d.revision, d.prev)).collect();
    assert_eq!(steps, [(second, first)]);
    assert_eq!(replica.revision(), second);
    assert_eq!(replica.snapshot().await.revision, second);
    assert_eq!(replica.read_index().await, ReadIndex::Confirmed(second));
}

/// Every replica counts the same revisions from the same log, one that
/// installed a snapshot of it included: the snapshot carries the count,
/// and the next delta follows it on both.
#[tokio::test(start_paused = true)]
async fn replicas_report_the_same_revision_after_the_same_log_and_a_snapshot_install() {
    let mut options = Options::default();
    // One snapshot taken by hand, as in the caps row.
    options.raft.snapshot_policy = openraft::SnapshotPolicy::Never;
    options.raft.max_in_snapshot_log_to_keep = 0;
    options.raft.purge_batch_size = 1;
    let mut set_ = Set::start(options).await;
    let leader = set_.leader().await;
    let behind = (leader + 1) % 3;
    set_.crash(behind).await;
    let replica = set_.replica(leader).clone();
    for n in 1..=4u64 {
        set(&*replica, "s", &n.to_string(), &format!("gain={n}@{n}")).await;
        replica.renew_sessions(&[owner("s")]);
    }
    // Refused as stale: no change, so no revision.
    set(&*replica, "s", "5", "gain=0@1").await;
    let index = replica.store().applied_index();
    assert!(index > 5, "log index {index}");
    let raft = replica.raft().clone();
    raft.trigger().snapshot().await.expect("snapshot");
    while raft.metrics().borrow().snapshot.map(|id| id.index) != Some(index) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raft.trigger().purge_log(index).await.expect("purge");
    while raft.metrics().borrow().purged.map(|id| id.index) != Some(index) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    set_.restart(behind).await;
    set_.applied(4).await;
    let installed = set_.replica(behind).clone();
    assert!(
        installed.raft().metrics().borrow().snapshot.is_some(),
        "the restarted replica installed a snapshot"
    );
    for r in [&replica, &installed] {
        assert_eq!(r.revision(), 4, "four changes");
        assert_eq!(r.snapshot().await.revision, 4);
    }
    // It holds no delta at or below its snapshot, so a client below it is
    // answered with a snapshot, never an empty resume.
    let below = Position {
        epoch: replica.epoch(),
        revision: 2,
    };
    assert!(installed.deltas_since(below).await.is_none());
    let fifth = set(&*replica, "s", "6", "pan=1@6").await;
    assert_eq!(fifth, 5);
    set_.applied(fifth).await;
    let since = Position {
        epoch: replica.epoch(),
        revision: 4,
    };
    for r in [&replica, &installed] {
        let deltas = r.deltas_since(since).await.expect("held");
        let steps: Vec<_> = deltas.iter().map(|d| (d.revision, d.prev)).collect();
        assert_eq!(steps, [(5, 4)]);
    }
}

/// The write timeout counts from the submit: a command queued behind
/// writes that cannot commit, a command and an entry of the set's own,
/// waits no longer than one that is written at once.
#[tokio::test(start_paused = true)]
async fn a_queued_submit_times_out_at_its_own_deadline() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    set_.net.isolate(leader, set_.ids());
    let replica = set_.replica(leader).clone();
    let first = tokio::spawn({
        let replica = replica.clone();
        async move { refused(&*replica, "a").await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    replica.append(Body::LeaseRenew(vec![SessionKey::from_parts(
        "anonymous",
        "s",
    )]));
    let queued_at = tokio::time::Instant::now();
    let second = refused(&*replica, "b").await;
    assert_eq!(second.reason, Reason::Unavailable);
    assert_eq!(queued_at.elapsed(), Duration::from_secs(2));
    assert_eq!(first.await.expect("joins").reason, Reason::Unavailable);
}

/// A command whose deadline passes while it queues behind a write that
/// cannot commit is never ordered: it does not apply once that write goes
/// through.
#[tokio::test(start_paused = true)]
async fn a_command_past_its_deadline_while_queued_never_applies() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let followers: Vec<_> = set_.ids().filter(|&id| id != leader).collect();
    for &id in &followers {
        set_.crash(id).await;
    }
    let replica = set_.replica(leader).clone();
    // Proposed, not committed: no quorum while the followers are down.
    let first = tokio::spawn({
        let replica = replica.clone();
        async move {
            replica
                .submit(&owner("s"), "a", Bytes::from("gain=1@1"))
                .await
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    // The writer takes this entry once the first write times out and
    // waits for it past the deadline of the command queued behind it.
    replica.append(Body::LeaseRenew(vec![SessionKey::from_parts(
        "anonymous",
        "s",
    )]));
    let queued = replica
        .submit(&owner("s"), "b", Bytes::from("pan=1@1"))
        .await
        .expect_err("not committed");
    assert_eq!(queued.reason, Reason::Unavailable);
    first.await.expect("joins").expect_err("no quorum");
    tokio::time::sleep(Duration::from_secs(2)).await;
    // One follower back: the leader's log is the longest, so what it
    // proposed commits.
    set_.restart(followers[0]).await;
    let leader = set_.leader().await;
    set(&**set_.replica(leader), "s", "c", "mute=1@1").await;
    set_.replica(leader).store().with_state(|sm| {
        assert!(sm.product.values.contains_key("gain"), "the stalled write");
        assert!(
            !sm.product.values.contains_key("pan"),
            "the expired command"
        );
    });
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
        replica.command_resolution(&owner("s"), "cmd-1"),
        Some((revision, Resolution::Duplicate))
    );
    assert_eq!(set(&**replica, "s", "cmd-1", "gain=1@1").await, revision);
    assert_eq!(replica.store().with_state(|sm| sm.product.applied), 1);
    // The same id from another session is another command.
    assert_ne!(set(&**replica, "t", "cmd-1", "pan=1@1").await, revision);
}

/// Submitted before the writer runs, so they form one entry: every
/// command is applied in order with its own body, the product refusing
/// the older seq that came later, and a copy of a command in the same
/// entry is answered as its first copy resolved: an applied or unchanged
/// one as a duplicate, a stale one as stale. The entry is one delta at one
/// revision, every command resolving at it, with the changes of both
/// commands that made one.
#[tokio::test(start_paused = true)]
async fn one_entry_applies_every_command_and_answers_a_copy_as_a_duplicate() {
    let set_ = Set::start(Options::default()).await;
    let replica = set_.replica(set_.leader().await);
    let pending = [
        ("s", "1", "gain=5@5"),
        ("t", "1", "gain=3@3"),
        ("s", "1", "gain=5@5"),
        ("s", "2", "pan=0@4"),
        ("t", "1", "gain=3@3"),
        ("u", "1", "pan=0@5"),
        ("u", "1", "pan=0@5"),
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
            Resolution::Applied,
            Resolution::Stale,
            Resolution::Duplicate,
            Resolution::Applied,
            Resolution::Stale,
            Resolution::Unchanged,
            Resolution::Duplicate
        ]
    );
    assert_eq!(revisions, [1; 7]);
    let since = Position {
        epoch: replica.epoch(),
        revision: 0,
    };
    let deltas = replica.deltas_since(since).await.expect("held");
    let deltas: Vec<_> = deltas
        .iter()
        .map(|d| (d.revision, d.prev, d.commands.clone(), d.text.as_str()))
        .collect();
    let ids = vec!["1".to_owned(), "2".to_owned()];
    assert_eq!(deltas, [(1, 0, ids, "gain=5,pan=0 from s/1,s/2")]);
    replica.store().with_state(|sm| {
        assert_eq!(sm.product.applied, 3);
        assert_eq!(sm.product.values["gain"].0, "5");
    });
    // The stale command's id is recorded: its resend is stale again.
    let resent = ack(&**replica, "t", "1", "gain=3@3").await;
    assert_eq!(
        (resent.revision, resent.resolution),
        (revisions[0], Resolution::Stale)
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
    let index = set_.replica(leader).store().applied_index();
    raft.trigger().snapshot().await.expect("snapshot");
    while raft.metrics().borrow().snapshot.map(|id| id.index) != Some(index) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raft.trigger().purge_log(index).await.expect("purge");
    while raft.metrics().borrow().purged.map(|id| id.index) != Some(index) {
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
    assert_eq!(replica.command_resolution(&owner("b"), "1"), None);
    let before = replica.snapshot().await.text;
    let applied = replica.store().with_state(|sm| sm.product.applied);
    set(&**replica, "b", "1", "b.pan=1@1").await;
    assert_eq!(replica.snapshot().await.text, before);
    assert_eq!(replica.store().with_state(|sm| sm.product.applied), applied);
}

/// A client resending a command it got no answer for learns how its first
/// copy resolved, on every replica and after a snapshot install: a stale
/// one stays stale and an applied one is a duplicate.
#[tokio::test(start_paused = true)]
async fn a_resend_answers_as_its_first_copy_resolved_also_after_a_snapshot_install() {
    let mut options = Options::default();
    // One snapshot taken by hand, as in the caps row.
    options.raft.snapshot_policy = openraft::SnapshotPolicy::Never;
    options.raft.max_in_snapshot_log_to_keep = 0;
    options.raft.purge_batch_size = 1;
    let mut set_ = Set::start(options).await;
    let leader = set_.leader().await;
    let behind = (leader + 1) % 3;
    set_.crash(behind).await;
    let replica = set_.replica(leader).clone();
    let eight = ack(&*replica, "s", "8", "gain=8@8").await;
    assert_eq!(eight.resolution, Resolution::Applied);
    let seven = ack(&*replica, "s", "7", "gain=7@7").await;
    assert_eq!(seven.resolution, Resolution::Stale);
    let resent = ack(&*replica, "s", "7", "gain=7@7").await;
    assert_eq!(resent, seven, "stale again, at the same revision");
    let expected = |id: &str| match id {
        "7" => Some((seven.revision, Resolution::Stale)),
        _ => Some((eight.revision, Resolution::Duplicate)),
    };
    for id in ["7", "8"] {
        assert_eq!(replica.command_resolution(&owner("s"), id), expected(id));
    }

    let last = replica.store().applied_index();
    let raft = replica.raft().clone();
    raft.trigger().snapshot().await.expect("snapshot");
    while raft.metrics().borrow().snapshot.map(|id| id.index) != Some(last) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raft.trigger().purge_log(last).await.expect("purge");
    while raft.metrics().borrow().purged.map(|id| id.index) != Some(last) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    set_.restart(behind).await;
    set_.applied(replica.revision()).await;
    let installed = set_.replica(behind);
    assert!(
        installed.raft().metrics().borrow().snapshot.is_some(),
        "the restarted replica installed a snapshot"
    );
    for id in ["7", "8"] {
        assert_eq!(installed.command_resolution(&owner("s"), id), expected(id));
    }
    // A resend through the next leader resolves as its first copy did, and
    // the installed replica applies it from its table alike.
    set_.crash(leader).await;
    let next = set_.replica(set_.leader().await).clone();
    assert_eq!(ack(&*next, "s", "7", "gain=7@7").await, seven);
    let again = ack(&*next, "s", "8", "gain=8@8").await;
    assert_eq!(
        (again.revision, again.resolution),
        (eight.revision, Resolution::Duplicate)
    );
    let tables: Vec<_> = set_
        .running()
        .map(|id| set_.replica(id).store().with_state(|sm| sm.dedup.save()))
        .collect();
    assert_eq!(tables[0], tables[1]);
}

/// A leader cut off from the others confirms no read index, even
/// while its own metrics still name it leader, so a client holding a
/// revision the new leader committed is never answered from it as ahead
/// or as confirmed.
#[tokio::test(start_paused = true)]
async fn a_deposed_leader_confirms_no_read_index() {
    let set_ = Set::start(Options::default()).await;
    let old = set_.leader().await;
    let first = set(&**set_.replica(old), "s", "1", "gain=1@1").await;
    set_.applied(first).await;
    set_.net.isolate(old, set_.ids());
    let others: Vec<_> = set_.ids().filter(|&id| id != old).collect();
    let new = loop {
        if let Some(l) = others.iter().copied().find(|&id| {
            set_.replica(id).raft().metrics().borrow().state == openraft::ServerState::Leader
        }) {
            break l;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let second = set(&**set_.replica(new), "s", "2", "gain=2@2").await;
    assert_eq!(second, first + 1);
    let still = set_.replica(old).raft().metrics().borrow().state == openraft::ServerState::Leader;
    let read = set_.replica(old).read_index().await;
    assert!(
        !matches!(read, ReadIndex::Confirmed(r) if r < second),
        "old leader (metrics say leader: {still}) answered {read:?}, new leader committed {second}"
    );
}

/// `applied` resolves only once the revision is applied here.
#[tokio::test(start_paused = true)]
async fn applied_waits_until_the_revision_is_applied() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let first = set(&**set_.replica(leader), "s", "1", "gain=1@1").await;
    set_.applied(first).await;
    let follower = (leader + 1) % 3;
    let mut waiting = set_.replica(follower).applied(first + 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut waiting)
            .await
            .is_err(),
        "resolved before revision {} was applied",
        first + 1
    );
    let second = set(&**set_.replica(leader), "s", "2", "gain=2@2").await;
    assert_eq!(second, first + 1);
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("applied on the follower");
}
