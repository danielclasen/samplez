mod common;

use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::Message;
use common::*;
use futures_util::{FutureExt, Sink, Stream, StreamExt};
use samplez_service::replica::{Ack, Begin, Commands};
use samplez_service::ws::{
    ClientQueue, Delta, Ended, Frame, Hub, MAX_PENDING_REPLIES, PONG_TIMEOUT, Popped, Pushed,
    Sessions, serve_client,
};
use samplez_service::{Connect, Forbidden, Identity, Ops, Owner, Position, ReplicatedState, Role};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

fn delta(revision: u64) -> Frame {
    Frame::Delta(Delta {
        epoch: 7,
        revision,
        prev: revision - 1,
        command: None,
        text: format!("delta@{revision}").into(),
    })
}

fn meters(b: &'static [u8]) -> Frame {
    Frame::Meters(Bytes::from_static(b))
}

async fn popped(queue: &ClientQueue) -> Popped {
    tokio::time::timeout(Duration::from_secs(1), queue.pop())
        .await
        .expect("a frame is queued")
}

#[tokio::test]
async fn a_full_delta_queue_resyncs_and_never_drops_a_single_delta() {
    let q = ClientQueue::new(2);
    assert_eq!(q.push(delta(1)), Pushed::Queued);
    assert_eq!(q.push(delta(2)), Pushed::Queued);
    assert_eq!(q.push(Frame::Counters("c1".into())), Pushed::Queued);
    assert_eq!(q.push(delta(3)), Pushed::Resync, "full");
    assert!(q.is_empty());
    assert_eq!(q.push(delta(4)), Pushed::Resync, "covered by the catch-up");
    assert_eq!(popped(&q).await, Popped::Resync);
    assert_eq!(
        popped(&q).await,
        Popped::Frame(Frame::Counters("c1".into()))
    );
    assert_eq!(q.push(delta(5)), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(delta(5)));
}

#[tokio::test]
async fn meters_and_counters_keep_their_latest_frame_and_meters_need_credit() {
    let q = ClientQueue::new(2);
    assert_eq!(q.push(meters(b"m1")), Pushed::Skipped, "no credit");
    q.grant_meter_credits(2);
    assert_eq!(q.push(meters(b"m2")), Pushed::Queued);
    assert_eq!(q.push(meters(b"m3")), Pushed::Replaced);
    assert_eq!(q.push(Frame::Counters("c1".into())), Pushed::Queued);
    assert_eq!(q.push(Frame::Counters("c2".into())), Pushed::Replaced);
    assert_eq!(q.push(delta(1)), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(delta(1)), "deltas first");
    assert_eq!(
        popped(&q).await,
        Popped::Frame(Frame::Counters("c2".into()))
    );
    assert_eq!(popped(&q).await, Popped::Frame(meters(b"m3")));
    assert_eq!(q.meter_credits(), 1, "one credit per meter frame sent");
    assert_eq!(q.push(meters(b"m4")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(meters(b"m4")));
    assert_eq!(q.push(meters(b"m5")), Pushed::Skipped, "credits used up");
}

/// A WebSocket made of two channels.
struct ChannelSocket {
    to_client: mpsc::UnboundedSender<Message>,
    from_client: mpsc::UnboundedReceiver<Result<Message, ()>>,
}

impl Stream for ChannelSocket {
    type Item = Result<Message, ()>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_client.poll_recv(cx)
    }
}

impl Sink<Message> for ChannelSocket {
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

fn connect(sessions: &Arc<Sessions>, role: Role, since: Option<(u64, u64)>, n: u32) -> Connect {
    Connect {
        identity: Identity { user: None, role },
        since: since.map(|(epoch, revision)| Position { epoch, revision }),
        owner: Owner {
            principal: "anonymous".into(),
            session: format!("s{n}"),
            connection: format!("r1:{n}"),
        },
        sessions: sessions.clone(),
    }
}

struct Client {
    rx: mpsc::UnboundedReceiver<Message>,
    tx: mpsc::UnboundedSender<Result<Message, ()>>,
    task: tokio::task::JoinHandle<Ended>,
    queue: Arc<ClientQueue>,
    pong: bool,
}

impl Client {
    /// Runs `serve_client` over channels; the product callback echoes text,
    /// answers `slow` after 1.5 s and grants a meter credit for the binary
    /// message `credit`.
    fn start(hub: &Hub, state: &Arc<FakeState>, connect: Connect) -> Client {
        let queue = hub.subscribe();
        let state = state.clone();
        let (to_client, rx) = mpsc::unbounded_channel();
        let (tx, from_client) = mpsc::unbounded_channel();
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        let q = queue.clone();
        let task = tokio::spawn(async move {
            serve_client(socket, &connect, &q, &*state, |m| {
                let q = q.clone();
                async move {
                    match m {
                        Message::Text(t) if t == "slow" => {
                            tokio::time::sleep(Duration::from_millis(1500)).await;
                            Some(Message::Text("slow done".into()))
                        }
                        Message::Text(t) => Some(Message::Text(format!("echo {t}").into())),
                        Message::Binary(b) if &b[..] == b"credit" => {
                            q.grant_meter_credits(1);
                            None
                        }
                        _ => None,
                    }
                }
            })
            .await
        });
        Client {
            rx,
            tx,
            task,
            queue,
            pong: true,
        }
    }

    fn connect(hub: &Hub, state: &Arc<FakeState>, since: Option<(u64, u64)>) -> Client {
        let sessions = Arc::new(Sessions::new("r1"));
        Client::start(hub, state, connect(&sessions, Role::Operator, since, 1))
    }

    /// The next message other than a ping, answering pings when `pong`.
    /// Within 2 s in all, so pings arriving meanwhile cannot extend it.
    async fn next_message(&mut self) -> Message {
        let wait = async {
            loop {
                match self.rx.recv().await.expect("serving") {
                    Message::Ping(_) if self.pong => {
                        let _ = self.tx.send(Ok(Message::Pong(Bytes::new())));
                    }
                    Message::Ping(_) => {}
                    other => return other,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("a message within 2 s")
    }

    async fn ended(&mut self) -> Ended {
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .expect("ended within 5 s")
            .unwrap()
    }

    async fn next(&mut self) -> String {
        match self.next_message().await {
            Message::Text(t) => t.to_string(),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn silent(&mut self) {
        let got = tokio::time::timeout(Duration::from_millis(300), self.next_message()).await;
        assert!(got.is_err(), "unexpected {got:?}");
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Applies a delta to the state and publishes it, as the product does.
fn apply(state: &FakeState, hub: &Hub) -> Delta {
    let d = state.apply(None);
    hub.publish(Frame::Delta(d.clone()));
    d
}

#[tokio::test(start_paused = true)]
async fn connect_catches_up_from_the_last_seen_position() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    for _ in 0..3 {
        apply(&state, &hub);
    }

    let mut fresh = Client::connect(&hub, &state, None);
    assert_eq!(fresh.next().await, "snapshot@7:3");

    let mut resumed = Client::connect(&hub, &state, Some((7, 1)));
    assert_eq!(resumed.next().await, "delta@2");
    assert_eq!(resumed.next().await, "delta@3");

    let mut current = Client::connect(&hub, &state, Some((7, 3)));
    current.silent().await;

    let mut other_epoch = Client::connect(&hub, &state, Some((6, 1)));
    assert_eq!(other_epoch.next().await, "snapshot@7:3", "another history");

    state.forget_through(2);
    let mut stale = Client::connect(&hub, &state, Some((7, 1)));
    assert_eq!(stale.next().await, "snapshot@7:3", "deltas no longer held");

    apply(&state, &hub);
    hub.publish(Frame::Counters("c".into()));
    for client in [
        &mut fresh,
        &mut resumed,
        &mut current,
        &mut other_epoch,
        &mut stale,
    ] {
        assert_eq!(client.next().await, "delta@4");
        assert_eq!(client.next().await, "c");
    }
}

#[tokio::test(start_paused = true)]
async fn a_new_epoch_sends_a_snapshot() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    for _ in 0..5 {
        apply(&state, &hub);
    }
    let mut client = Client::connect(&hub, &state, Some((7, 5)));
    client.silent().await;

    state.reset(8);
    apply(&state, &hub);
    assert_eq!(client.next().await, "snapshot@8:1", "revision 1 is below 5");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@2");
}

#[tokio::test(start_paused = true)]
async fn a_gap_in_prev_catches_up_from_the_state() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, Some((7, 0)));
    client.silent().await;

    state.apply(None);
    state.apply(None);
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@1", "3 names prev 2, not 0");
    assert_eq!(client.next().await, "delta@2");
    assert_eq!(client.next().await, "delta@3");
    client.silent().await;
}

#[tokio::test(start_paused = true)]
async fn a_lagging_replica_skips_what_the_client_saw_on_another() {
    let follower = FakeState::new("r2", "follower");
    let hub = Hub::new(8);
    apply(&follower, &hub);

    let mut client = Client::connect(&hub, &follower, Some((7, 3)));
    client.silent().await;
    for _ in 0..3 {
        apply(&follower, &hub);
    }
    assert_eq!(client.next().await, "delta@4", "2 and 3 skipped");
}

/// The bursts below run without an await, so the client cannot drain the
/// two-delta queue in between.
#[tokio::test(start_paused = true)]
async fn overflow_catches_up_from_the_last_sent_revision() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(2);
    for _ in 0..3 {
        apply(&state, &hub);
    }
    let mut client = Client::connect(&hub, &state, Some((7, 3)));
    client.silent().await;

    for _ in 0..6 {
        apply(&state, &hub);
    }
    let mut got = Vec::new();
    for _ in 0..6 {
        got.push(client.next().await);
    }
    let expected: Vec<String> = (4..=9).map(|r| format!("delta@{r}")).collect();
    assert_eq!(got, expected, "in order, none lost or repeated");
    client.silent().await;

    for _ in 0..3 {
        apply(&state, &hub);
    }
    state.forget_through(12);
    assert_eq!(client.next().await, "snapshot@7:12");
}

#[tokio::test(start_paused = true)]
async fn meters_go_out_as_binary_against_credits_and_messages_reach_the_product() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");

    hub.publish(meters(b"m1"));
    client.silent().await;
    client
        .tx
        .send(Ok(Message::Binary(Bytes::from_static(b"credit"))))
        .unwrap();
    client.silent().await;
    hub.publish(meters(b"m2"));
    assert_eq!(
        client.next_message().await,
        Message::Binary(Bytes::from_static(b"m2"))
    );
    hub.publish(meters(b"m3"));
    client.silent().await;
    assert_eq!(client.queue.meter_credits(), 0);

    client.tx.send(Ok(Message::Text("hello".into()))).unwrap();
    assert_eq!(client.next().await, "echo hello");
}

#[tokio::test(start_paused = true)]
async fn a_client_that_stops_answering_pings_is_closed_after_one_second() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    client.pong = false;
    let start = Instant::now();
    assert_eq!(client.next().await, "snapshot@7:0");
    assert_eq!(client.next_message().await, Message::Close(None));
    let ended = client.ended().await;
    assert_eq!(ended, Ended::PongTimeout);
    assert_eq!(start.elapsed(), Duration::from_secs(1));
}

/// Longer than [`PONG_TIMEOUT`], as a reply waiting on a commit during an
/// election can take.
#[tokio::test(start_paused = true)]
async fn a_slow_reply_keeps_the_heartbeat_and_the_order_of_replies() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    client.tx.send(Ok(Message::Text("slow".into()))).unwrap();
    client.tx.send(Ok(Message::Text("hello".into()))).unwrap();
    assert_eq!(client.next().await, "slow done");
    assert_eq!(start.elapsed(), Duration::from_millis(1500));
    assert_eq!(client.next().await, "echo hello", "in the order received");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@1", "still served");
}

#[tokio::test(start_paused = true)]
async fn a_slow_snapshot_keeps_the_heartbeat() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(1500);
    state.reset(8);
    apply(&state, &hub);
    assert_eq!(client.next().await, "snapshot@8:1");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@2", "still served");
}

#[tokio::test(start_paused = true)]
async fn a_connection_is_not_read_while_its_replies_are_at_the_limit() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1"));
    let connect = connect(&sessions, Role::Operator, None, 1);
    let queue = Hub::new(8).subscribe();
    let (to_client, _rx) = mpsc::unbounded_channel();
    let (tx, from_client) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn({
        let calls = calls.clone();
        async move {
            let socket = ChannelSocket {
                to_client,
                from_client,
            };
            serve_client(socket, &connect, &queue, &*state, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                std::future::pending::<Option<Message>>()
            })
            .await
        }
    });
    for _ in 0..=MAX_PENDING_REPLIES {
        tx.send(Ok(Message::Text("never answered".into()))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::Relaxed), MAX_PENDING_REPLIES);
    task.abort();
}

#[tokio::test(start_paused = true)]
async fn live_sessions_are_renewed_once_per_second_in_one_batch() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1"));
    let renew = tokio::spawn({
        let (sessions, state) = (sessions.clone(), state.clone());
        async move { samplez_service::server::renew_sessions(&sessions, &*state).await }
    });
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(state.renewals.lock().unwrap().is_empty(), "nobody to renew");

    let mut a = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    let mut b = Client::start(&hub, &state, connect(&sessions, Role::Viewer, None, 2));
    let mut silent = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 3));
    silent.pong = false;
    for c in [&mut a, &mut b, &mut silent] {
        c.next().await;
    }
    // Over 2 s of answered pings; the silent client is closed after 1 s.
    let (_, closed) = tokio::join!(
        async {
            for _ in 0..7 {
                wait_both(&mut a, &mut b).await;
            }
        },
        async { silent.ended().await }
    );
    assert_eq!(closed, Ended::PongTimeout);

    let renewals = state.renewals.lock().unwrap().clone();
    let first = &renewals[0];
    assert_eq!(first.len(), 3, "all three were live at the first renewal");
    let owners: Vec<&str> = renewals
        .last()
        .unwrap()
        .iter()
        .map(|o| o.session.as_str())
        .collect();
    assert_eq!(owners, ["s1", "s2"], "only connections that answered");
    renew.abort();
}

async fn wait_both(a: &mut Client, b: &mut Client) {
    tokio::join!(a.silent(), b.silent());
}

#[test]
fn require_operator_refuses_a_viewer() {
    let sessions = Arc::new(Sessions::new("r1"));
    assert_eq!(
        connect(&sessions, Role::Operator, None, 1).require_operator(),
        Ok(())
    );
    assert_eq!(
        connect(&sessions, Role::Viewer, None, 1).require_operator(),
        Err(Forbidden)
    );
}

fn command() -> Bytes {
    Bytes::from_static(b"gain -6")
}

/// The owner of a command sent by the client with this session.
fn owner(session: &str) -> Owner {
    Owner {
        principal: "anonymous".into(),
        session: session.into(),
        connection: "r1:1".into(),
    }
}

#[tokio::test]
async fn commands_apply_once_per_id_from_the_replicated_table() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 2);
    let s1 = owner("s1");

    let Begin::New(pending) = commands.begin(&s1, "c1") else {
        panic!("c1 is new");
    };
    let mut submitted = pin!(pending.submit(command()));
    assert!(submitted.as_mut().now_or_never().is_none(), "committing");
    // The client lost the connection and resent through the same replica.
    let Begin::New(again) = commands.begin(&s1, "c1") else {
        panic!("not applied yet, so new again");
    };
    let mut resent = pin!(again.submit(command()));
    assert!(resent.as_mut().now_or_never().is_none(), "committing");
    state.commit();
    let ack = submitted.await.unwrap();
    assert_eq!(
        ack,
        Ack {
            command: "c1".into(),
            revision: 1
        }
    );
    assert_eq!(resent.await.unwrap(), ack, "the resend gets the answer too");
    assert_eq!(state.revision(), 1, "applied once");
    match commands.begin(&s1, "c1") {
        Begin::Duplicate(again) => assert_eq!(again, ack),
        _ => panic!("c1 is applied"),
    }

    let Begin::New(rejected) = commands.begin(&s1, "c2") else {
        panic!("c2 is new");
    };
    drop(rejected);
    assert!(
        matches!(commands.begin(&s1, "c2"), Begin::New(_)),
        "a rejected command may be resent"
    );

    let _a = commands.begin(&s1, "a");
    let _b = commands.begin(&s1, "b");
    assert!(
        matches!(commands.begin(&s1, "c"), Begin::Busy),
        "two in flight at most"
    );
}

/// Two replicas over one replicated state: the client lost the first
/// before its commit finished and resent through the second.
#[tokio::test]
async fn a_command_resent_through_another_replica_while_pending_applies_once() {
    let state = FakeState::new("r1", "leader");
    let (on_a, on_b) = (
        Commands::new(state.clone(), 8),
        Commands::new(state.clone(), 8),
    );
    let s1 = owner("s1");
    let Begin::New(a) = on_a.begin(&s1, "cmd-1") else {
        panic!("cmd-1 is new on a");
    };
    let mut first = pin!(a.submit(command()));
    assert!(first.as_mut().now_or_never().is_none(), "committing");
    let Begin::New(b) = on_b.begin(&s1, "cmd-1") else {
        panic!("not applied yet, so new on b as well");
    };
    let mut second = pin!(b.submit(command()));
    assert!(second.as_mut().now_or_never().is_none(), "committing");
    state.commit();
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!(
        first,
        Ack {
            command: "cmd-1".into(),
            revision: 1
        }
    );
    assert_eq!(second, first, "the same result");
    assert_eq!(state.revision(), 1, "applied once");
}

#[test]
fn a_command_resent_to_another_replica_is_not_applied_twice() {
    let (a, b) = (
        FakeState::new("a", "leader"),
        FakeState::new("b", "follower"),
    );
    let applied = a.apply(Some(("s1", "cmd-1")));
    b.receive(applied.clone(), Some("s1"));
    let on_b = Commands::new(b.clone(), 8);
    match on_b.begin(&owner("s1"), "cmd-1") {
        Begin::Duplicate(ack) => assert_eq!(ack.revision, applied.revision),
        _ => panic!("replica b holds cmd-1 in the replicated table"),
    }
}

/// The futures of a connection's replies are polled in no particular
/// order, so the order of the submit calls has to be the commit order.
#[tokio::test]
async fn commands_commit_in_the_order_they_were_submitted() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 8);
    let s1 = owner("s1");
    let (Begin::New(first), Begin::New(second)) =
        (commands.begin(&s1, "1"), commands.begin(&s1, "2"))
    else {
        panic!("both new");
    };
    let mut first = pin!(first.submit(command()));
    let mut second = pin!(second.submit(command()));
    assert!(second.as_mut().now_or_never().is_none(), "committing");
    assert!(first.as_mut().now_or_never().is_none(), "committing");
    state.commit();
    assert_eq!(first.await.unwrap().revision, 1);
    assert_eq!(second.await.unwrap().revision, 2);
}

/// Ids are unique within a session only, such as per-client sequence
/// numbers.
#[tokio::test]
async fn the_same_id_from_two_sessions_is_two_commands() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 8);
    let (s1, s2) = (owner("s1"), owner("s2"));
    let Begin::New(first) = commands.begin(&s1, "1") else {
        panic!("new for s1");
    };
    let Begin::New(second) = commands.begin(&s2, "1") else {
        panic!("new for s2");
    };
    let mut first = pin!(first.submit(command()));
    let mut second = pin!(second.submit(command()));
    assert!(first.as_mut().now_or_never().is_none(), "committing");
    assert!(second.as_mut().now_or_never().is_none(), "committing");
    state.commit();
    assert_eq!(first.await.unwrap().revision, 1);
    assert_eq!(second.await.unwrap().revision, 2, "applied as well");
    match commands.begin(&s2, "1") {
        Begin::Duplicate(ack) => assert_eq!(ack.revision, 2),
        _ => panic!("applied for s2"),
    }
    state.apply(Some(("s1", "2")));
    assert!(
        matches!(commands.begin(&s2, "2"), Begin::New(_)),
        "applied for s1 only"
    );
}

#[tokio::test]
async fn loopback_upgrade_checks_origin_and_resumes_from_since() {
    let state = FakeState::new("r1", "leader");
    let hub = Arc::new(Hub::new(8));
    for _ in 0..3 {
        apply(&state, &hub);
    }
    let mut config = config("none", "loopback");
    let loopback = serde_json::json!({ "bind": "127.0.0.1", "port": 0 });
    config["listeners"] = serde_json::json!({ "api": loopback, "ops": loopback, "nmos": loopback });
    let service = service_from(config, Ops::default(), state.clone()).ws({
        let (hub, state) = (hub.clone(), state.clone());
        move |socket, connect| {
            let (hub, state) = (hub.clone(), state.clone());
            async move {
                assert_eq!(connect.owner.session, "tab-1");
                assert_eq!(connect.owner.principal, "anonymous");
                assert!(connect.owner.connection.starts_with("r1:"));
                let queue = hub.subscribe();
                serve_client(socket, &connect, &queue, &*state, |_| async { None }).await;
            }
        }
    });
    let bound = service.bind().await.unwrap();
    let addr = bound.api_addr().unwrap();
    let server = tokio::spawn(bound.serve());

    let url = format!("ws://{addr}/api/v1/ws?since=7:2&session=tab-1");
    let mut foreign = url.as_str().into_client_request().unwrap();
    foreign
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    match tokio_tungstenite::connect_async(foreign).await {
        Err(tungstenite::Error::Http(res)) => assert_eq!(res.status(), 403),
        other => panic!("foreign Origin accepted: {other:?}"),
    }
    for bad in ["since=2", "since=7:", "since=%2B7:2", "session="] {
        let url = format!("ws://{addr}/api/v1/ws?{bad}");
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Err(tungstenite::Error::Http(res)) => assert_eq!(res.status(), 400, "{bad}"),
            other => panic!("{bad} accepted: {other:?}"),
        }
    }

    let (mut ws, res) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    assert_eq!(res.status(), 101);
    let mut next = async || loop {
        match ws.next().await {
            Some(Ok(tungstenite::Message::Text(t))) => return t.to_string(),
            Some(Ok(tungstenite::Message::Ping(_))) => {}
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(next().await, "delta@3");
    apply(&state, &hub);
    assert_eq!(next().await, "delta@4");
    server.abort();
}

/// Takes the first message, then never accepts another and never yields
/// one: the connection stays open while the server is stuck sending.
struct StuckSocket {
    sent: usize,
}

impl Stream for StuckSocket {
    type Item = Result<Message, ()>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Sink<Message> for StuckSocket {
    type Error = ();
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if self.sent == 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(mut self: Pin<&mut Self>, _: Message) -> Result<(), ()> {
        self.sent += 1;
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn a_client_that_stops_reading_ends_one_second_after_its_last_pong() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1"));
    let connect = connect(&sessions, Role::Operator, None, 1);
    let queue = Hub::new(8).subscribe();
    let start = Instant::now();
    let task = tokio::spawn({
        let state = state.clone();
        async move {
            let socket = StuckSocket { sent: 0 };
            serve_client(socket, &connect, &queue, &*state, |_| async { None }).await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(sessions.live().len(), 1, "answered at connect");
    let ended = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("not stuck sending")
        .unwrap();
    assert_eq!(ended, Ended::Failed);
    assert_eq!(start.elapsed(), PONG_TIMEOUT);
    assert!(sessions.live().is_empty());
}
