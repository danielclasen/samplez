//! Forwarding, quorum loss and a quiescing leader over the in-process
//! harness.

mod harness;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::Message;
use harness::{Options, Set, owner};
use samplez_replica::entry::{Body, Command, Origin, SessionKey};
use samplez_replica::peer::{Forwarded, PeerRequest, PeerResponse, Refused};
use samplez_replica::{MAX_COMMAND_BYTES, MAX_ENTRY_BYTES};
use samplez_service::replica::{Reason, Resolution};
use samplez_service::ws::{Answer, CatchUp, ClientQueue, Encode, Reply, Sessions, serve_client};
use samplez_service::{Connect, Identity, Position, ReadIndex, ReplicatedState, Role};
use tokio::time::Instant;

async fn set(replica: &impl ReplicatedState, session: &str, id: &str, command: &str) -> u64 {
    replica
        .submit(&owner(session), id, Bytes::from(command.to_owned()))
        .await
        .expect("committed")
        .revision
}

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
/// the read index is cut off at once, so a reader gets the local state
/// marked stale without waiting; once the quorum is back both work again.
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
        set_.replica(follower).read_index().await,
        ReadIndex::Confirmed(before.revision)
    );

    // Links that drop what they carry: a read that asked for a read
    // index would wait for its whole timeout.
    for id in set_.ids() {
        set_.net.hang(id, set_.ids());
    }
    // Past the lease, every replica knows it has no quorum.
    tokio::time::sleep(Duration::from_millis(400)).await;
    for id in set_.ids() {
        let replica = set_.replica(id);
        assert!(!replica.quorum().await, "replica {id}");
        let asked = Instant::now();
        assert_eq!(replica.read_index().await, ReadIndex::CutOff);
        assert_eq!(asked.elapsed(), Duration::ZERO, "replica {id}: no wait");
        assert_eq!(replica.revision(), before.revision, "the local state");
        let refused = replica
            .submit(&owner("s"), &format!("q{id}"), Bytes::from("pan=1@1"))
            .await
            .expect_err("no quorum");
        // 503 either way: unavailable while a leader counted the replica
        // within the last rounds, not commitable once none has for longer.
        assert!(
            matches!(refused.reason, Reason::Unavailable | Reason::NotCommitable),
            "replica {id}: {refused}"
        );
        assert!(asked.elapsed() <= Duration::from_secs(2));
    }

    set_.net.heal();
    set_.leader().await;
    // Commitable again once a leader counts it, within a check.
    let deadline = Instant::now() + Duration::from_secs(1);
    while !set_.replica(follower).replica().commitable {
        assert!(Instant::now() < deadline, "not commitable again");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let after = set_
        .replica(follower)
        .submit(&owner("s"), "2", Bytes::from("gain=2@2"))
        .await
        .expect("committed once the quorum is back");
    for id in set_.ids() {
        let replica = set_.replica(id);
        assert!(replica.quorum().await, "replica {id}");
        let read = replica.read_index().await;
        assert!(
            matches!(read, ReadIndex::Confirmed(r) if r >= after.revision),
            "replica {id}: {read:?}"
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
            within_ms: 1000,
        }],
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

fn forwarded(session: &str, id: &str, body: &str, within_ms: u64) -> Forwarded {
    Forwarded {
        command: Command {
            origin: Origin::from(&owner(session)),
            id: id.into(),
            at_index: None,
            body: body.as_bytes().to_vec(),
        },
        received: 0,
        within_ms,
    }
}

/// Holds the leader's writer on an entry whose commit waits for the
/// stalled followers, so what is queued behind it waits too.
fn hold_the_writer(set_: &Set, leader: u64) -> tokio::task::JoinHandle<()> {
    for id in others(set_, leader) {
        set_.net.stall(id, Duration::from_millis(40));
    }
    let replica = set_.replica(leader).clone();
    tokio::spawn(async move {
        replica
            .submit(&owner("w"), "hold", Bytes::from("hold=1@1"))
            .await
            .expect("committed");
    })
}

/// A leader that starts quiescing after it accepted a forward refuses its
/// commands one by one; the follower resends them to the next leader,
/// which applies each once.
#[tokio::test(start_paused = true)]
async fn commands_refused_one_by_one_by_a_quiescing_leader_reach_the_next_one() {
    let mut set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let follower = others(&set_, leader)[0];
    let held = hold_the_writer(&set_, leader);
    tokio::time::sleep(Duration::from_millis(1)).await;
    let sent = tokio::spawn({
        let replica = set_.replica(follower).clone();
        async move {
            replica
                .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
                .await
        }
    });
    // Accepted by the leader, queued behind the held entry.
    tokio::time::sleep(Duration::from_millis(1)).await;
    let quiesced = tokio::spawn({
        let replica = set_.replica(leader).clone();
        async move { replica.quiesce(Duration::from_secs(1)).await }
    });
    held.await.expect("joins");
    quiesced.await.expect("joins");
    for id in others(&set_, leader) {
        set_.net.unstall(id);
    }
    assert!(!sent.is_finished(), "held for the next leader");
    set_.crash(leader).await;
    let ack = sent
        .await
        .expect("joins")
        .expect("ordered by the next leader");
    assert_eq!(ack.resolution, Resolution::Applied);
    set_.applied(ack.revision).await;
    for id in set_.running().collect::<Vec<_>>() {
        let values = set_
            .replica(id)
            .store()
            .with_state(|sm| sm.product.values.clone());
        assert_eq!(values["gain"].1, 1, "replica {id}");
    }
}

/// A command answered through a follower is visible on that follower,
/// also when the leader committed it with another one first.
#[tokio::test(start_paused = true)]
async fn a_follower_answers_a_forwarded_command_once_it_applied_it() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let [slow, _] = others(&set_, leader)[..] else {
        unreachable!()
    };
    set_.net.stall(slow, Duration::from_millis(40));
    let ack = set_
        .replica(slow)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("committed");
    let replica = set_.replica(slow);
    assert!(replica.revision() >= ack.revision, "read your writes");
    let values = replica.store().with_state(|sm| sm.product.values.clone());
    assert_eq!(values["gain"].0, "1");
}

/// The leader orders a forwarded command only within what was left of its
/// own write timeout, not the group's longest.
#[tokio::test(start_paused = true)]
async fn a_forwarded_command_is_not_ordered_after_its_own_deadline() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let held = hold_the_writer(&set_, leader);
    tokio::time::sleep(Duration::from_millis(1)).await;
    let forward = PeerRequest::Forward {
        commands: vec![
            forwarded("s", "short", "pan=1@1", 10),
            forwarded("s", "long", "gain=1@1", 2000),
        ],
    };
    let answered = set_.replica(leader).handle(Some(leader), forward).await;
    held.await.expect("joins");
    let Ok(PeerResponse::Forwarded(results)) = answered else {
        panic!("{answered:?}");
    };
    assert_eq!(
        results[0],
        Err(Refused::Unavailable("timed out".into())),
        "its deadline passed while it queued"
    );
    assert!(results[1].is_ok());
    let replica = set_.replica(leader);
    assert_eq!(replica.command_resolution(&owner("s"), "short"), None);
    let values = replica.store().with_state(|sm| sm.product.values.clone());
    assert!(!values.contains_key("pan"));
}

/// A peer has the leader append its lease renewals and nothing else.
#[tokio::test(start_paused = true)]
async fn a_peer_appends_only_lease_renewals() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let append = |body| PeerRequest::Append {
        body,
        within_ms: 1000,
    };
    let replica = set_.replica(leader);
    let verdict = Body::ClockVerdict {
        replica: 1,
        judged: true,
    };
    assert_eq!(
        replica.handle(Some(leader), append(verdict)).await,
        Err(Refused::NotForwardable)
    );
    assert!(replica.store().with_state(|sm| sm.verdicts.is_empty()));
    let renewal = Body::LeaseRenew(vec![SessionKey::from_parts("anonymous", "s")]);
    assert_eq!(
        replica.handle(Some(leader), append(renewal)).await,
        Ok(PeerResponse::Appended)
    );
}

/// A command too large for the log is refused before it is ordered, and
/// commands queued together are split into entries of bounded size.
#[tokio::test(start_paused = true)]
async fn entries_are_bounded_in_size() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let replica = set_.replica(leader);
    let big = format!("big={}@1", "x".repeat(MAX_COMMAND_BYTES));
    let refused = replica
        .submit(&owner("s"), "big", Bytes::from(big))
        .await
        .expect_err("too large");
    assert_eq!(refused.reason, Reason::TooLarge);
    assert_eq!(replica.command_resolution(&owner("s"), "big"), None);

    // Queued before the writer runs: without the cap one entry.
    let half = |n: u64| format!("p{n}={}@1", "x".repeat(MAX_ENTRY_BYTES / 2));
    let submitted: Vec<_> = (0..3)
        .map(|n| replica.submit(&owner("s"), &n.to_string(), Bytes::from(half(n))))
        .collect();
    let mut revisions = Vec::new();
    for submit in submitted {
        revisions.push(submit.await.expect("committed").revision);
    }
    revisions.dedup();
    assert_eq!(revisions.len(), 3, "one entry each: {revisions:?}");
}

/// openraft's error text names nodes and addresses; a client is told only
/// that its command was not committed.
#[tokio::test(start_paused = true)]
async fn a_client_is_not_shown_openraft_errors() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let replica = set_.replica(leader).clone();
    // In flight when openraft stops: its proposal fails with openraft's
    // own error.
    for id in others(&set_, leader) {
        set_.net.cut_one_way(leader, id);
    }
    let submitted = tokio::spawn({
        let replica = replica.clone();
        async move {
            replica
                .submit(&owner("s"), "x", Bytes::from("gain=1@1"))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    replica.shutdown().await;
    let refused = submitted.await.expect("joins").expect_err("stopped");
    assert_eq!(refused.reason, Reason::Unavailable);
    assert_eq!(refused.detail, "not committed");
}

/// A proposal the leader checked its leadership for just before quiesce
/// set its flag is committed and held by every voter before quiesce
/// returns.
#[tokio::test(start_paused = true)]
async fn quiesce_waits_for_a_proposal_that_raced_its_flag() {
    let mut options = Options::default();
    // Followers that do not hear the leader for a while do not call an
    // election meanwhile.
    options.raft.election_timeout_min = 4000;
    options.raft.election_timeout_max = 5000;
    let set_ = Set::start(options).await;
    let leader = set_.leader().await;
    let replica = set_.replica(leader).clone();
    // The writer stamps the entry after its leadership check; quiesce
    // starts right there, before the proposal reaches openraft.
    let submitted = replica.submit(&owner("s"), "1", Bytes::from("gain=1@1"));
    let stamped = set_.clock(leader).next_read();
    let quiesce = tokio::spawn({
        let replica = replica.clone();
        async move {
            let _ = stamped.await;
            replica.quiesce(Duration::from_secs(5)).await
        }
    });
    // The entry reaches the followers only after quiesce has looked.
    for id in others(&set_, leader) {
        set_.net.cut_one_way(leader, id);
    }
    let committed = async {
        let ack = submitted.await.expect("committed");
        (ack, Instant::now())
    };
    let heal = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        set_.net.heal();
    };
    let quiesced = async {
        quiesce.await.expect("joins");
        Instant::now()
    };
    let ((ack, committed_at), (), quiesced_at) = tokio::join!(committed, heal, quiesced);
    assert!(
        committed_at <= quiesced_at,
        "quiesce returned before the entry committed"
    );
    let metrics = replica.raft().metrics().borrow().clone();
    for id in others(&set_, leader) {
        let matched = metrics.replication.as_ref().unwrap()[&id];
        assert!(matched.is_some_and(|m| m.index >= ack.revision), "{id}");
    }
}

/// A WebSocket made of two channels.
struct ChannelSocket {
    to_client: tokio::sync::mpsc::UnboundedSender<Message>,
    from_client: tokio::sync::mpsc::UnboundedReceiver<Result<Message, ()>>,
}

impl futures_util::Stream for ChannelSocket {
    type Item = Result<Message, ()>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_client.poll_recv(cx)
    }
}

impl futures_util::Sink<Message> for ChannelSocket {
    type Error = ();
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
        self.to_client.send(item).map_err(|_| ())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

/// Answers as `resume <revision> <stale>`, `snapshot <revision> <stale>`
/// or `ahead`.
struct Wire;

impl Encode for Wire {
    fn ping(&mut self, t: u64) -> Message {
        Message::Text(format!("ping {t}").into())
    }

    fn answer(&mut self, answer: Answer) -> Vec<Message> {
        let text = match answer {
            Answer::Resume {
                revision, stale, ..
            } => format!("resume {revision} {stale}"),
            Answer::Snapshot { snapshot, stale } => {
                format!("snapshot {} {stale}", snapshot.revision)
            }
            Answer::Ahead { .. } => "ahead".into(),
        };
        vec![Message::Text(text.into())]
    }
}

/// A client reconnecting to a follower that has not applied the leader's
/// latest write yet is answered, unmarked, with that write: the follower
/// takes the read index from its leader and applies up to it first.
#[tokio::test(start_paused = true)]
async fn a_follower_catches_a_client_up_to_what_its_leader_committed() {
    let set_ = Set::start(Options::default()).await;
    let leader = set_.leader().await;
    let [slow, _] = others(&set_, leader)[..] else {
        unreachable!()
    };
    let replica = set_.replica(slow).clone();
    let seen = set(&*replica, "s", "1", "gain=1@1").await;
    set_.applied(seen).await;
    set_.net.stall(slow, Duration::from_millis(40));
    let committed = set(&**set_.replica(leader), "s", "2", "gain=2@2").await;
    assert!(replica.revision() < committed, "the follower lags");

    let (to_client, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (send, from_client) = tokio::sync::mpsc::unbounded_channel();
    let socket = ChannelSocket {
        to_client,
        from_client,
    };
    let sessions = Arc::new(Sessions::new("r1").expect("a replica id"));
    let connect = Connect {
        identity: Identity {
            user: None,
            role: Role::Operator,
        },
        owner: owner("c"),
        sessions,
    };
    let queue = ClientQueue::new(16);
    let since = Position {
        epoch: replica.epoch(),
        revision: seen,
    };
    send.send(Ok(Message::Text("sync".into()))).unwrap();
    let served = serve_client(
        socket,
        &connect,
        &queue,
        &*replica,
        Wire,
        |message, _| match message {
            Message::Text(t) if t.as_str() == "sync" => Reply::CatchUp(CatchUp {
                since: Some(since),
                full: false,
            }),
            _ => Reply::<std::future::Ready<Option<Message>>>::Now(None),
        },
    );
    let answered = async {
        loop {
            match received.recv().await {
                Some(Message::Text(t)) if !t.as_str().starts_with("ping") => {
                    break t.as_str().to_owned();
                }
                Some(_) => {}
                None => panic!("the connection ended"),
            }
        }
    };
    let answer = tokio::select! {
        answer = answered => answer,
        ended = served => panic!("ended: {ended:?}"),
    };
    assert_eq!(answer, format!("resume {committed} false"));
}
