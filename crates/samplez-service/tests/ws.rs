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
use samplez_service::server::MAX_SESSION_ID;
use samplez_service::ws::{
    ClientQueue, Delta, Ended, Frame, Hub, MAX_PAUSE, MAX_PAUSE_TOTAL, MAX_PENDING_REPLIES,
    PING_PERIOD, PONG_TIMEOUT, Popped, Pushed, Sessions, serve_client,
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
            role,
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
    /// answers text starting with `slow` after 1.5 s and grants a meter credit for the binary
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
                        Message::Text(t) if t.starts_with("slow") => {
                            tokio::time::sleep(Duration::from_millis(1500)).await;
                            Some(Message::Text(format!("{t} done").into()))
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
        let sessions = Arc::new(Sessions::new("r1").unwrap());
        Client::start(hub, state, connect(&sessions, Role::Operator, since, 1))
    }

    /// The next message other than a ping, answering pings when `pong`.
    /// Within 2 s in all, so pings arriving meanwhile cannot extend it.
    async fn next_message(&mut self) -> Message {
        let wait = async {
            loop {
                match self.rx.recv().await.expect("serving") {
                    Message::Ping(p) if self.pong => {
                        let _ = self.tx.send(Ok(Message::Pong(p)));
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

/// At the limit the connection is not read, its pongs included, which is
/// not the client's fault.
#[tokio::test(start_paused = true)]
async fn replies_at_the_limit_slower_than_the_pong_timeout_keep_the_connection() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    for n in 0..MAX_PENDING_REPLIES {
        let slow = format!("slow {n}");
        client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
    }
    client.tx.send(Ok(Message::Text("hello".into()))).unwrap();
    for n in 0..MAX_PENDING_REPLIES {
        assert_eq!(client.next().await, format!("slow {n} done"));
    }
    assert_eq!(start.elapsed(), Duration::from_millis(1500));
    assert_eq!(client.next().await, "echo hello", "read again");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@1", "still served");
}

/// A client resending its unacknowledged commands at once after
/// connecting reads the answers against the state they refer to.
#[tokio::test(start_paused = true)]
async fn no_reply_goes_out_before_the_first_catch_up() {
    let state = FakeState::new("r1", "leader");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(500);
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    client.tx.send(Ok(Message::Text("hello".into()))).unwrap();
    assert_eq!(client.next().await, "snapshot@7:0");
    assert_eq!(client.next().await, "echo hello");
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
    let sessions = Arc::new(Sessions::new("r1").unwrap());
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

/// Replies that never resolve, to a client that never answers a ping: a
/// peer that is gone is not held for as long as the replies hang.
#[tokio::test(start_paused = true)]
async fn a_connection_not_read_at_the_limit_ends_after_the_longest_pause() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let connect = connect(&sessions, Role::Operator, None, 1);
    let queue = Hub::new(8).subscribe();
    let (to_client, _rx) = mpsc::unbounded_channel();
    let (tx, from_client) = mpsc::unbounded_channel();
    let start = Instant::now();
    let task = tokio::spawn(async move {
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        serve_client(socket, &connect, &queue, &*state, |_| {
            std::future::pending::<Option<Message>>()
        })
        .await
    });
    for _ in 0..MAX_PENDING_REPLIES {
        tx.send(Ok(Message::Text("never answered".into()))).unwrap();
    }
    let ended = tokio::time::timeout(MAX_PAUSE + Duration::from_secs(1), task)
        .await
        .expect("ended")
        .unwrap();
    assert_eq!(ended, Ended::Failed);
    assert_eq!(start.elapsed(), MAX_PAUSE);
    assert!(sessions.live().is_empty());
}

/// Replies that each resolve within the longest pause, to a client that
/// never answers a ping, with messages queued for more pauses than the
/// total allows: a peer that is gone is not held for as long as its queued
/// messages last.
#[tokio::test(start_paused = true)]
async fn a_connection_not_read_at_the_limit_ends_after_the_longest_total_pause() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let connect = connect(&sessions, Role::Operator, None, 1);
    let queue = Hub::new(8).subscribe();
    let (to_client, _rx) = mpsc::unbounded_channel();
    let (tx, from_client) = mpsc::unbounded_channel();
    let reply = MAX_PAUSE - Duration::from_millis(100);
    let start = Instant::now();
    let task = tokio::spawn(async move {
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        serve_client(socket, &connect, &queue, &*state, |_| async move {
            tokio::time::sleep(reply).await;
            Some(Message::Text("done".into()))
        })
        .await
    });
    for _ in 0..5 * MAX_PENDING_REPLIES {
        tx.send(Ok(Message::Text("queued".into()))).unwrap();
    }
    tokio::time::sleep(MAX_PAUSE_TOTAL - Duration::from_millis(1)).await;
    assert_eq!(
        sessions.live().len(),
        1,
        "every pause shorter than the longest"
    );
    let ended = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("ended")
        .unwrap();
    assert_eq!(ended, Ended::Failed);
    assert_eq!(start.elapsed(), MAX_PAUSE_TOTAL);
    assert!(sessions.live().is_empty());
}

/// Its pongs wait unread behind the messages, so a healthy client at the
/// limit keeps its lease until the connection ends.
#[tokio::test(start_paused = true)]
async fn a_connection_that_answered_before_the_limit_stays_live_while_not_read() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    for n in 0..MAX_PENDING_REPLIES {
        let slow = format!("slow {n}");
        client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert_eq!(
        sessions.live().len(),
        1,
        "not read for over {PONG_TIMEOUT:?}, answered before"
    );
    assert_eq!(client.next().await, "slow 0 done");
}

/// The messages fill the limit three times over, so the pongs wait unread
/// through several pauses with almost no reading between them; twice, so
/// the second time adds up to more than the total pause and the pongs read
/// in between restart the count.
#[tokio::test(start_paused = true)]
async fn a_client_answering_every_ping_stays_live_through_repeated_pauses() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    let round = 3 * MAX_PENDING_REPLIES;
    let (mut sent, mut replies, mut not_live) = (0, 0, Vec::new());
    let mut ended = false;
    let start = Instant::now();
    while !ended && start.elapsed() < Duration::from_secs(10) {
        if replies == sent && sent < 2 * round {
            for n in sent..sent + round {
                let slow = format!("slow {n}");
                client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
            }
            sent += round;
        }
        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                match client.rx.recv().await {
                    Some(Message::Ping(p)) => {
                        let _ = client.tx.send(Ok(Message::Pong(p)));
                    }
                    Some(Message::Text(_)) => replies += 1,
                    Some(_) => {}
                    None => break ended = true,
                }
            }
        })
        .await;
        if !ended && sessions.live().is_empty() {
            not_live.push(start.elapsed());
        }
    }
    assert!(!ended, "connection ended after {:?}", start.elapsed());
    assert_eq!(replies, 2 * round);
    assert!(not_live.is_empty(), "not live at {not_live:?}");
}

/// After a pause the client has its whole pong timeout again, counted from
/// sending the last ping answered as if the pause had not happened.
#[tokio::test(start_paused = true)]
async fn a_pause_does_not_count_against_the_pong_timeout_after_it_ends() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    client.pong = false;
    let start = Instant::now();
    for n in 0..MAX_PENDING_REPLIES {
        let slow = format!("slow {n}");
        client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
    }
    for n in 0..MAX_PENDING_REPLIES {
        assert_eq!(client.next().await, format!("slow {n} done"));
    }
    let resumed = start.elapsed();
    assert_eq!(resumed, Duration::from_millis(1500));
    tokio::time::sleep(PONG_TIMEOUT - Duration::from_millis(100)).await;
    assert_eq!(sessions.live().len(), 1, "within the pong timeout after it");
    assert_eq!(client.next_message().await, Message::Close(None));
    assert_eq!(client.ended().await, Ended::PongTimeout);
    assert_eq!(start.elapsed(), resumed + PONG_TIMEOUT);
    assert!(sessions.live().is_empty());
}

/// A client answering every ping, each pong queued behind another limit's
/// worth of messages, that then stops: the pongs it left wait unread
/// through the backlog, and each one read counts from when its ping was
/// sent, so the backlog does not keep the gone peer live.
#[tokio::test(start_paused = true)]
async fn pongs_left_behind_a_backlog_do_not_keep_a_gone_peer_live() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    let mut last_ping = start;
    while start.elapsed() < Duration::from_secs(2) {
        if let Message::Ping(p) = client.rx.recv().await.expect("serving") {
            for n in 0..MAX_PENDING_REPLIES {
                let slow = format!("slow {n}");
                client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
            }
            client.tx.send(Ok(Message::Pong(p))).unwrap();
            last_ping = Instant::now();
        }
    }
    let bound = last_ping + PONG_TIMEOUT + MAX_PAUSE_TOTAL;
    let mut last_live = None;
    while !client.task.is_finished() && start.elapsed() < Duration::from_secs(30) {
        if !sessions.live().is_empty() {
            last_live = Some(Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(client.task.is_finished(), "still served after 30 s");
    assert!(
        last_live.is_none_or(|t| t <= bound),
        "live {:?} after the last ping answered, at {:?}",
        last_live.map(|t| t - last_ping),
        last_ping - start,
    );
}

/// A pong counts only when it echoes a ping's payload: an empty or made-up
/// one does not keep the connection.
#[tokio::test(start_paused = true)]
async fn a_pong_that_echoes_no_ping_does_not_count() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let mut client = Client::connect(&hub, &state, None);
    client.pong = false;
    let start = Instant::now();
    let answer = async {
        loop {
            match client.rx.recv().await {
                Some(Message::Ping(_)) => {
                    let _ = client.tx.send(Ok(Message::Pong(Bytes::new())));
                    let forged = Bytes::copy_from_slice(&u64::MAX.to_be_bytes());
                    let _ = client.tx.send(Ok(Message::Pong(forged)));
                }
                Some(Message::Close(_)) | None => break,
                Some(_) => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), answer)
        .await
        .expect("closed");
    assert_eq!(client.ended().await, Ended::PongTimeout);
    assert_eq!(start.elapsed(), PONG_TIMEOUT);
}

/// An unsolicited pong (RFC 6455 5.5.3) carrying 8 bytes that read as a
/// number above every ping outstanding does not drop those pings: the
/// pong that echoes one still counts.
#[tokio::test(start_paused = true)]
async fn an_unsolicited_pong_does_not_drop_the_pings_outstanding() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    let start = Instant::now();
    while start.elapsed() < 4 * PONG_TIMEOUT {
        if let Message::Ping(p) = client.rx.recv().await.expect("serving") {
            let unsolicited = Bytes::copy_from_slice(&1_700_000_000_000u64.to_be_bytes());
            client.tx.send(Ok(Message::Pong(unsolicited))).unwrap();
            client.tx.send(Ok(Message::Pong(p))).unwrap();
        }
    }
    assert!(!client.task.is_finished());
    assert_eq!(sessions.live().len(), 1);
}

/// A client may answer only the most recent of several pings (RFC 6455
/// 5.5.3): answering every other one keeps the connection.
#[tokio::test(start_paused = true)]
async fn a_client_answering_every_other_ping_stays_connected() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    let start = Instant::now();
    let mut skip = true;
    while start.elapsed() < 4 * PONG_TIMEOUT {
        if let Message::Ping(p) = client.rx.recv().await.expect("serving") {
            if !skip {
                client.tx.send(Ok(Message::Pong(p))).unwrap();
            }
            skip = !skip;
        }
    }
    assert!(!client.task.is_finished());
    assert_eq!(sessions.live().len(), 1);
}

/// Answers every ping `delay` after it was sent, for 4 s or until the
/// connection is closed: how it ended and when, if it did, and whether it
/// was live throughout.
async fn pongs_delayed_by(delay: Duration) -> (Option<(Ended, Duration)>, bool) {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, None, 1));
    let start = Instant::now();
    let mut always_live = true;
    while start.elapsed() < 4 * PONG_TIMEOUT {
        match tokio::time::timeout(Duration::from_millis(50), client.rx.recv()).await {
            Ok(Some(Message::Ping(p))) => {
                let tx = client.tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = tx.send(Ok(Message::Pong(p)));
                });
            }
            Ok(Some(Message::Close(_)) | None) => {
                let closed = start.elapsed();
                return (Some((client.ended().await, closed)), always_live);
            }
            _ => {}
        }
        always_live &= !sessions.live().is_empty();
    }
    (None, always_live)
}

/// Pings go every PING_PERIOD, so by each one the ping sent
/// PONG_TIMEOUT - PING_PERIOD before has to be answered: a steady round
/// trip just below that keeps the connection live, one just above ends it.
#[tokio::test(start_paused = true)]
async fn the_round_trip_has_to_stay_below_the_pong_timeout_less_a_ping_period() {
    let ceiling = PONG_TIMEOUT - PING_PERIOD;
    let margin = Duration::from_millis(50);
    assert_eq!(pongs_delayed_by(ceiling - margin).await, (None, true));
    let (ended, _) = pongs_delayed_by(ceiling + margin).await;
    assert_eq!(ended, Some((Ended::PongTimeout, PONG_TIMEOUT)));
}

/// Yields what the client sends and takes messages at once, except
/// between `from` and `until`, when a send waits.
struct StallingSocket {
    from_client: mpsc::UnboundedReceiver<Result<Message, ()>>,
    from: Instant,
    until: Instant,
    stall: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for StallingSocket {
    type Item = Result<Message, ()>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_client.poll_recv(cx)
    }
}

impl Sink<Message> for StallingSocket {
    type Error = ();
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        let now = Instant::now();
        if now < self.from || now >= self.until {
            return Poll::Ready(Ok(()));
        }
        let until = self.until;
        let stall = self
            .stall
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(until)));
        stall.poll_unpin(cx).map(Ok)
    }
    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), ()> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

/// The replies resolve just before the longest pause ends, but the first
/// one's send outlasts the end: the connection ends at the end, rather
/// than counting the whole pause and coming back live in [`Sessions`]
/// after it was dropped.
#[tokio::test(start_paused = true)]
async fn a_send_while_not_read_does_not_outlast_the_pause() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let connect = connect(&sessions, Role::Operator, None, 1);
    let queue = Hub::new(8).subscribe();
    let (tx, from_client) = mpsc::unbounded_channel();
    let start = Instant::now();
    let ready = start + MAX_PAUSE - Duration::from_millis(100);
    let task = tokio::spawn(async move {
        let socket = StallingSocket {
            from_client,
            from: ready,
            until: ready + Duration::from_millis(500),
            stall: None,
        };
        serve_client(socket, &connect, &queue, &*state, |_| async move {
            tokio::time::sleep_until(ready).await;
            Some(Message::Text("done".into()))
        })
        .await
    });
    for _ in 0..MAX_PENDING_REPLIES {
        tx.send(Ok(Message::Text("queued".into()))).unwrap();
    }
    let ended = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("ended")
        .unwrap();
    assert_eq!(ended, Ended::Failed);
    assert_eq!(start.elapsed(), MAX_PAUSE);
    assert!(sessions.live().is_empty());
}

/// Replies resolving at the instant the longest pause ends, to clients
/// that never answer a ping: whichever the select takes first, the reply or
/// the end, each connection ends at the end. One connection takes the
/// reply first with a probability of about 2/5, so 32 do with near
/// certainty.
#[tokio::test(start_paused = true)]
async fn a_reply_ready_as_the_pause_ends_does_not_resume_the_connection() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let start = Instant::now();
    let end = start + MAX_PAUSE;
    let mut clients = Vec::new();
    for n in 0..32 {
        let connect = connect(&sessions, Role::Operator, None, n);
        let queue = Hub::new(8).subscribe();
        let state = state.clone();
        let (to_client, rx) = mpsc::unbounded_channel();
        let (tx, from_client) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let socket = ChannelSocket {
                to_client,
                from_client,
            };
            serve_client(socket, &connect, &queue, &*state, |_| async move {
                tokio::time::sleep_until(end).await;
                Some(Message::Text("done".into()))
            })
            .await
        });
        for _ in 0..MAX_PENDING_REPLIES {
            tx.send(Ok(Message::Text("queued".into()))).unwrap();
        }
        clients.push((task, tx, rx));
    }
    for (task, _tx, _rx) in clients {
        let ended = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("ended")
            .unwrap();
        assert_eq!(ended, Ended::Failed);
    }
    assert_eq!(start.elapsed(), MAX_PAUSE);
    assert!(sessions.live().is_empty());
}

#[tokio::test(start_paused = true)]
async fn live_sessions_are_renewed_once_per_second_in_one_batch() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8);
    let sessions = Arc::new(Sessions::new("r1").unwrap());
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
    let sessions = Arc::new(Sessions::new("r1").unwrap());
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

/// The owner of a command sent by an `anonymous` client with this session.
fn owner(session: &str) -> Owner {
    Owner {
        principal: "anonymous".into(),
        session: session.into(),
        role: Role::Operator,
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
    let s1 = owner("s1");
    let applied = a.apply(Some((&s1, "cmd-1")));
    b.receive(applied.clone(), Some(&s1));
    let on_b = Commands::new(b.clone(), 8);
    match on_b.begin(&s1, "cmd-1") {
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
    state.apply(Some((&s1, "2")));
    assert!(
        matches!(commands.begin(&s2, "2"), Begin::New(_)),
        "applied for s1 only"
    );
}

/// The session id is not authenticated: another principal presenting it
/// must not apply a command the session's own client sends later.
#[tokio::test]
async fn another_principal_with_the_same_session_cannot_pre_empt_its_commands() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 8);
    let session = "kM2pV9xQ4tR7wY1zB6nC3d";
    let alice = Owner {
        principal: "alice".into(),
        ..owner(session)
    };
    let mallory = Owner {
        principal: "mallory".into(),
        ..owner(session)
    };
    let Begin::New(early) = commands.begin(&mallory, "1") else {
        panic!("new for mallory");
    };
    let early = early.submit(command());
    state.commit();
    assert_eq!(early.await.unwrap().revision, 1);
    let Begin::New(own) = commands.begin(&alice, "1") else {
        panic!("not applied for alice");
    };
    let own = own.submit(command());
    state.commit();
    assert_eq!(own.await.unwrap().revision, 2, "applied for alice");
}

/// A client session id: 16 random bytes in base64url without padding.
const SESSION: &str = "q3Jx-Lh0_9TfWzKcVb2s1A";

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
    let longest = SESSION.repeat(6)[..MAX_SESSION_ID].to_owned();
    let service = service_from(config, Ops::default(), state.clone()).ws({
        let (hub, state, longest) = (hub.clone(), state.clone(), longest.clone());
        move |socket, connect| {
            let (hub, state, longest) = (hub.clone(), state.clone(), longest.clone());
            async move {
                assert!(
                    [SESSION, &longest].contains(&connect.owner.session.as_str()),
                    "{}",
                    connect.owner.session
                );
                assert_eq!(connect.owner.principal, "anonymous");
                assert_eq!(connect.owner.role, Role::Operator);
                assert!(connect.owner.connection.starts_with("r1:"));
                let queue = hub.subscribe();
                serve_client(socket, &connect, &queue, &*state, |_| async { None }).await;
            }
        }
    });
    let bound = service.bind().await.unwrap();
    let addr = bound.api_addr().unwrap();
    let server = tokio::spawn(bound.serve());

    let url = format!("ws://{addr}/api/v1/ws?since=7:2&session={SESSION}");
    let mut foreign = url.as_str().into_client_request().unwrap();
    foreign
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    match tokio_tungstenite::connect_async(foreign).await {
        Err(tungstenite::Error::Http(res)) => assert_eq!(res.status(), 403),
        other => panic!("foreign Origin accepted: {other:?}"),
    }
    let short = &SESSION[1..];
    let bad = [
        "since=2".to_owned(),
        "since=7:".to_owned(),
        "since=%2B7:2".to_owned(),
        "session=".to_owned(),
        "session=tab-1".to_owned(),
        format!("session={short}"),
        format!("session={}", &SESSION.repeat(6)[..MAX_SESSION_ID + 1]),
        format!("session={short}%3D"),
        format!("session={short}."),
    ];
    for bad in bad {
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

    let url = format!("ws://{addr}/api/v1/ws?since=7:3&session={longest}");
    let (mut ws, res) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    assert_eq!(res.status(), 101, "the longest session id");
    loop {
        match ws.next().await {
            Some(Ok(tungstenite::Message::Text(t))) => break assert_eq!(t.as_str(), "delta@4"),
            Some(Ok(tungstenite::Message::Ping(_))) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    server.abort();
}

/// A forwarded command names who sent it, with the role it was allowed.
#[tokio::test]
async fn the_owner_carries_the_principal_and_role_from_the_proxy() {
    let state = FakeState::new("r1", "leader");
    let mut config = config("proxy", "owner-role");
    let loopback = serde_json::json!({ "bind": "127.0.0.1", "port": 0 });
    config["listeners"] = serde_json::json!({
        "api": loopback, "ops": loopback, "nmos": loopback, "machine": loopback,
    });
    let (owners, mut owner) = mpsc::unbounded_channel();
    let service = service_from(config, Ops::default(), state).ws(move |_, connect| {
        let _ = owners.send(connect.owner);
        async {}
    });
    let bound = service.bind().await.unwrap();
    let addr = bound.api_addr().unwrap();
    let server = tokio::spawn(bound.serve());
    for (groups, role) in [("watchers", Role::Viewer), ("ops", Role::Operator)] {
        let url = format!("ws://{addr}/api/v1/ws?session={SESSION}");
        let mut req = url.as_str().into_client_request().unwrap();
        req.headers_mut()
            .insert("host", "samplez.example".parse().unwrap());
        req.headers_mut()
            .insert("origin", "https://samplez.example".parse().unwrap());
        req.headers_mut()
            .insert("x-forwarded-user", "alice".parse().unwrap());
        req.headers_mut()
            .insert("x-forwarded-groups", groups.parse().unwrap());
        let _ = tokio_tungstenite::connect_async(req).await.unwrap();
        let got = owner.recv().await.unwrap();
        assert_eq!(
            (got.principal.as_str(), got.session.as_str(), got.role),
            ("alice", SESSION, role)
        );
    }
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
async fn a_client_that_stops_reading_ends_one_second_after_it_connected() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
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
