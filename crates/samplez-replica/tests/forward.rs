//! Forwarding, quorum loss and a quiescing leader over the in-process
//! harness.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{Options, Set, owner};
use samplez_replica::Freshness;
use samplez_replica::entry::{Command, Origin, SessionKey};
use samplez_replica::peer::{Forwarded, PeerRequest, Refused};
use samplez_service::ReplicatedState;
use samplez_service::replica::{Reason, Resolution};
use tokio::time::Instant;

fn others(set_: &Set, of: u64) -> Vec<u64> {
    set_.ids().filter(|&id| id != of).collect()
}

/// A write sent to a follower reaches the log under its owner and is
/// answered once the follower itself applied it; its resend through a
/// third replica finds the stored result.
#[tokio::test(start_paused = true)]
async fn a_command_sent_to_a_follower_is_forwarded_with_its_owner() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let [follower, third] = others(&set_, leader)[..] else {
        unreachable!()
    };
    let replica = set_.replica(follower);
    let ack = replica
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("forwarded and committed");
    assert_eq!(ack.resolution, Resolution::Applied);
    assert!(
        replica.revision() >= ack.revision,
        "answered once applied on the follower"
    );
    set_.applied(ack.revision).await;
    for id in set_.ids() {
        let state = set_.replica(id);
        assert_eq!(
            state.command_resolution(&owner("s"), "1"),
            Some((ack.revision, Resolution::Duplicate)),
            "replica {id}: recorded under the client's session"
        );
        let other = samplez_service::Owner {
            principal: "someone".into(),
            ..owner("s")
        };
        assert_eq!(state.command_resolution(&other, "1"), None);
        assert_eq!(state.store().with_state(|sm| sm.product.applied), 1);
    }
    let resent = set_
        .replica(third)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("answered");
    assert_eq!(
        (resent.revision, resent.resolution),
        (ack.revision, Resolution::Duplicate)
    );
    assert_eq!(
        set_.replica(leader)
            .store()
            .with_state(|sm| sm.product.applied),
        1
    );

    // A follower's own entries reach the log too.
    set_.replica(follower).renew_sessions(&[owner("t")]);
    let key = SessionKey::from_parts("anonymous", "t");
    let deadline = Instant::now() + Duration::from_secs(2);
    while !set_
        .replica(leader)
        .store()
        .with_state(|sm| sm.leases.contains_key(&key))
    {
        assert!(Instant::now() < deadline, "the renewal was not forwarded");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Without a quorum writes are refused 503 within the write timeout and
/// reads get the local state marked stale at once; once the quorum is
/// back both work again.
#[tokio::test(start_paused = true)]
async fn quorum_loss_refuses_writes_and_serves_stale_reads_until_it_returns() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let follower = others(&set_, leader)[0];
    let before = set_
        .replica(follower)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("committed");
    set_.applied(before.revision).await;
    assert_eq!(
        set_.replica(follower).read(Duration::from_secs(2)).await,
        Freshness::Current
    );

    for id in set_.ids() {
        set_.net.isolate(id, set_.ids());
    }
    // Past the lease, every replica knows it has no quorum.
    tokio::time::sleep(Duration::from_millis(400)).await;
    for id in set_.ids() {
        let replica = set_.replica(id);
        assert!(!replica.quorum().await, "replica {id}");
        let asked = Instant::now();
        assert_eq!(replica.read(Duration::from_secs(2)).await, Freshness::Stale);
        assert_eq!(asked.elapsed(), Duration::ZERO, "replica {id}: no wait");
        assert_eq!(replica.revision(), before.revision, "the local state");
        let refused = replica
            .submit(&owner("s"), &format!("q{id}"), Bytes::from("pan=1@1"))
            .await
            .expect_err("no quorum");
        assert_eq!(refused.reason, Reason::Unavailable, "replica {id}");
        assert!(asked.elapsed() <= Duration::from_secs(2));
    }

    set_.net.heal();
    set_.leader().await;
    let after = set_
        .replica(follower)
        .submit(&owner("s"), "2", Bytes::from("gain=2@2"))
        .await
        .expect("committed once the quorum is back");
    for id in set_.ids() {
        let replica = set_.replica(id);
        assert!(replica.quorum().await, "replica {id}");
        assert_eq!(
            replica.read(Duration::from_secs(2)).await,
            Freshness::Current
        );
        assert!(replica.revision() >= after.revision, "replica {id}");
    }
}

/// A quiescing leader orders nothing more: a forwarded command is refused
/// as retryable, resent by the follower to the next leader once the old
/// one exits, and applied once.
#[tokio::test(start_paused = true)]
async fn a_quiescing_leader_hands_forwarded_commands_to_the_next_leader() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let follower = others(&set_, leader)[0];

    // An entry in flight when the preStop starts is committed and held by
    // every voter before quiesce returns. The stall stays below the
    // heartbeat interval, so the leader keeps its followers.
    for id in others(&set_, leader) {
        set_.net.stall(id, Duration::from_millis(40));
    }
    let in_flight = tokio::spawn({
        let replica = set_.replica(leader).clone();
        async move {
            replica
                .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1)).await;
    let started = Instant::now();
    set_.replica(leader).quiesce(Duration::from_secs(1)).await;
    let quiesced = started.elapsed();
    assert!(quiesced >= Duration::from_millis(40), "{quiesced:?}");
    assert!(quiesced < Duration::from_secs(1), "{quiesced:?}");
    let first = in_flight.await.expect("joins").expect("committed");
    assert!(set_.replica(leader).store().applied_index() >= first.revision);
    for id in others(&set_, leader) {
        set_.net.unstall(id);
    }

    let forward = PeerRequest::Forward {
        commands: vec![Forwarded {
            command: Command {
                origin: Origin::from(&owner("s")),
                id: "x".into(),
                at_index: None,
                body: b"pan=1@1".to_vec(),
            },
            received: 0,
        }],
        within_ms: 1000,
    };
    let refused = set_.replica(leader).handle(Some(leader), forward).await;
    assert_eq!(refused, Err(Refused::Quiescing));

    let resent = tokio::spawn({
        let replica = set_.replica(follower).clone();
        async move {
            replica
                .submit(&owner("s"), "2", Bytes::from("gain=2@2"))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!resent.is_finished(), "held for the next leader");
    set_.crash(leader).await;
    let ack = resent
        .await
        .expect("joins")
        .expect("ordered by the next leader");
    assert_eq!(ack.resolution, Resolution::Applied);
    let next = set_.leader().await;
    assert_ne!(next, leader);
    set_.applied(ack.revision).await;
    for id in set_.running().collect::<Vec<_>>() {
        let applied = set_.replica(id).store().with_state(|sm| sm.product.applied);
        assert_eq!(applied, 2, "replica {id}: each command applied once");
    }
}
