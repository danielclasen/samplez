mod common;

use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::Message;
use common::*;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use samplez_service::replica::{Ack, Begin, Commands, NotApplied, Reason, Resolution};
use samplez_service::server::MAX_SESSION_ID;
use samplez_service::ws::{
    Admission, Answer, CatchUp, ClientQueue, Delta, Encode, Ended, Frame, Hub, MAX_PAUSE,
    MAX_PAUSE_TOTAL, MAX_PENDING_REPLIES, PING_PERIOD, PONG_TIMEOUT, Popped, Pushed,
    READ_INDEX_HOLD, Reply, SESSION_ENDED_CLOSE, Sent, Sessions, Slot, To, serve_client,
};
use samplez_service::{
    Connect, Forbidden, Identity, Metrics, Ops, Owner, OwnerHandle, Position, ReplicatedState,
    Role, SessionEnd,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

fn delta(revision: u64) -> Frame {
    Frame::Delta(Delta {
        epoch: 7,
        revision,
        prev: revision - 1,
        commands: Vec::new(),
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

/// Nothing is waiting in the queue.
async fn nothing_queued(queue: &ClientQueue) {
    let got = tokio::time::timeout(Duration::from_millis(10), queue.pop()).await;
    assert!(got.is_err(), "unexpected {got:?}");
}

fn text_frame(t: &str) -> Frame {
    Frame::Text(t.to_owned().into())
}

/// Deltas and text frames pop in the order pushed, so a text frame never
/// goes out after a delta pushed after it, nor ahead of one pushed before
/// it. A delta overflow discards the text frames with the deltas: the
/// catch-up carries the state they went with, and none pushed before it
/// goes out after it.
#[tokio::test]
async fn text_frames_pop_in_push_order_with_deltas_and_go_with_a_resync() {
    let q = ClientQueue::new(2);
    assert_eq!(q.push(text_frame("t0")), Pushed::Queued);
    assert_eq!(q.push(delta(1)), Pushed::Queued);
    assert_eq!(q.push(text_frame("t1")), Pushed::Queued);
    assert_eq!(q.push(delta(2)), Pushed::Queued);
    assert_eq!(q.push(Frame::Counters("c".into())), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(text_frame("t0")));
    assert_eq!(popped(&q).await, Popped::Frame(delta(1)));
    assert_eq!(popped(&q).await, Popped::Frame(text_frame("t1")));
    assert_eq!(popped(&q).await, Popped::Frame(delta(2)));
    assert_eq!(popped(&q).await, Popped::Frame(Frame::Counters("c".into())));

    q.push(delta(3));
    q.push(text_frame("before"));
    q.push(delta(4));
    assert_eq!(q.push(delta(5)), Pushed::Resync, "full");
    assert_eq!(q.push(text_frame("after")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Resync);
    assert_eq!(popped(&q).await, Popped::Frame(text_frame("after")));
    nothing_queued(&q).await;
}

/// A text frame that would overflow the queue is dropped, empties it and
/// ends the connection, so the loss is never silent: the client catches up
/// on a new one. Text frames are bounded apart from the deltas.
#[tokio::test]
async fn a_text_overflow_empties_the_queue_and_ends_the_connection() {
    let q = ClientQueue::new(2);
    q.push(delta(1));
    q.push(delta(2));
    assert_eq!(
        q.push(text_frame("t1")),
        Pushed::Queued,
        "not counted as deltas"
    );
    assert_eq!(q.push(text_frame("t2")), Pushed::Queued);
    assert_eq!(q.push(text_frame("t3")), Pushed::Dropped, "full");
    assert!(q.is_empty());
    assert_eq!(q.push(delta(3)), Pushed::Dropped, "ending");
    assert_eq!(popped(&q).await, Popped::Overflow);
    assert_eq!(popped(&q).await, Popped::Overflow, "until it ends");
}

const PACED: Slot = Slot {
    name: "paced",
    every: Duration::from_millis(100),
};
const UNPACED: Slot = Slot {
    name: "unpaced",
    every: Duration::ZERO,
};

fn status(slot: Slot, t: &str) -> Frame {
    Frame::Status(slot, t.to_owned().into())
}

/// A status slot keeps its latest frame only, and one paced at 100 ms
/// sends a client at most one frame per 100 ms however often the product
/// pushes: the latest waits for the time, the slots apart from it do not.
#[tokio::test(start_paused = true)]
async fn status_frames_keep_their_latest_per_slot_and_are_paced() {
    let q = ClientQueue::new(2);
    assert_eq!(q.push(status(PACED, "p1")), Pushed::Queued);
    assert_eq!(q.push(status(PACED, "p2")), Pushed::Replaced);
    assert_eq!(q.push(status(UNPACED, "u1")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(status(PACED, "p2")));
    assert_eq!(popped(&q).await, Popped::Frame(status(UNPACED, "u1")));
    nothing_queued(&q).await;

    let sent = Instant::now() - Duration::from_millis(10);
    assert_eq!(q.push(status(PACED, "p3")), Pushed::Queued);
    assert_eq!(q.push(status(UNPACED, "u2")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(status(UNPACED, "u2")));
    assert_eq!(q.push(status(PACED, "p4")), Pushed::Replaced);
    assert_eq!(popped(&q).await, Popped::Frame(status(PACED, "p4")));
    assert_eq!(sent.elapsed(), Duration::from_millis(100), "paced");
    assert_eq!(q.push(status(UNPACED, "u3")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(status(UNPACED, "u3")));
    assert_eq!(sent.elapsed(), Duration::from_millis(100), "not paced");
}

fn owner_of(principal: &str, session: &str, connection: &str) -> Owner {
    Owner {
        principal: principal.into(),
        session: session.into(),
        role: Role::Operator,
        connection: connection.into(),
    }
}

/// A frame for a session reaches its connections only, under its
/// principal only; one for its primary connections skips the ones not
/// marked primary; one for a connection reaches that one, under its
/// principal only. A dropped queue is reached by nothing, and a text frame
/// that overflows a queue is counted as dropped, not reached.
#[tokio::test]
async fn frames_reach_the_session_its_primary_connections_or_one_connection() {
    let metrics = Metrics::default();
    let hub = Hub::new(1, &metrics);
    let primary = hub.subscribe(&owner_of("alice", "s1", "r1:1"));
    primary.set_primary(true);
    let standby = hub.subscribe(&owner_of("alice", "s1", "r1:2"));
    let other_principal = hub.subscribe(&owner_of("bob", "s1", "r1:3"));
    let other_session = hub.subscribe(&owner_of("alice", "s2", "r1:4"));
    let all = [&primary, &standby, &other_principal, &other_session];
    let reached = |reached| Sent {
        reached,
        dropped: 0,
    };

    let s1 = To::Session {
        principal: "alice",
        session: "s1",
    };
    assert_eq!(hub.send(s1, text_frame("session")), reached(2));
    for q in [&primary, &standby] {
        assert_eq!(popped(q).await, Popped::Frame(text_frame("session")));
    }
    let to = To::Primary {
        principal: "alice",
        session: "s1",
    };
    assert_eq!(hub.send(to, text_frame("primary")), reached(1));
    assert_eq!(popped(&primary).await, Popped::Frame(text_frame("primary")));
    let to = |principal| To::Connection {
        principal,
        connection: "r1:4",
    };
    assert_eq!(hub.send(to("bob"), text_frame("not bob's")), reached(0));
    assert_eq!(hub.send(to("alice"), text_frame("one")), reached(1));
    assert_eq!(
        popped(&other_session).await,
        Popped::Frame(text_frame("one"))
    );
    for q in all {
        nothing_queued(q).await;
    }

    assert_eq!(hub.publish(text_frame("every")), reached(4));
    for q in all {
        assert_eq!(popped(q).await, Popped::Frame(text_frame("every")));
    }

    assert_eq!(hub.send(s1, text_frame("waits")), reached(2));
    primary.push(delta(1));
    assert_eq!(primary.push(delta(2)), Pushed::Resync);
    let sent = hub.send(s1, text_frame("over"));
    assert_eq!(
        sent,
        Sent {
            reached: 1,
            dropped: 1
        },
        "the standby's is full"
    );
    assert_eq!(popped(&standby).await, Popped::Overflow);
    assert!(
        metrics
            .render()
            .contains("samplez_ws_text_overflows_total 1")
    );
    drop(other_session);
    assert_eq!(hub.send(to("alice"), text_frame("gone")), reached(0));
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

fn connect(sessions: &Arc<Sessions>, role: Role, n: u32) -> Connect {
    Connect {
        identity: Identity { user: None, role },
        owner: Owner {
            principal: "anonymous".into(),
            session: format!("s{n}"),
            role,
            connection: format!("r1:{n}"),
        },
        sessions: sessions.clone(),
        ends: SessionEnd::never(),
    }
}

/// The test protocol: `sync [full] [<epoch>:<revision>]` catches up,
/// `promote [<epoch>:<revision>]` promotes and `pong <t>` answers the
/// server's `ping <t>`. A stale resume is marked by a message of its own
/// ahead of its deltas.
struct Wire;

impl Encode for Wire {
    fn ping(&mut self, t: u64) -> Message {
        Message::Text(format!("ping {t}").into())
    }

    fn answer(&mut self, answer: Answer) -> Vec<Message> {
        let text = |t: String| Message::Text(t.into());
        match answer {
            Answer::Snapshot {
                snapshot,
                stale: false,
            } => vec![Message::Text(snapshot.text)],
            Answer::Snapshot {
                snapshot,
                stale: true,
            } => vec![text(format!("{} stale", snapshot.text))],
            Answer::Resume {
                epoch,
                revision,
                deltas,
                stale,
            } => stale
                .then(|| text(format!("resume@{epoch}:{revision} stale")))
                .into_iter()
                .chain(deltas.into_iter().map(|d| Message::Text(d.text)))
                .collect(),
            Answer::Ahead {
                epoch,
                committed,
                last_revision,
            } => vec![text(format!(
                "ahead@{epoch}:{committed} below {last_revision}"
            ))],
        }
    }
}

fn position(s: &str) -> Option<Position> {
    let (epoch, revision) = s.split_once(':')?;
    Some(Position {
        epoch: epoch.parse().ok()?,
        revision: revision.parse().ok()?,
    })
}

/// The [`Wire`] protocol messages as the product answers them; `None` for
/// the others.
fn protocol<Fut>(message: &Message) -> Option<Reply<Fut>> {
    let Message::Text(t) = message else {
        return None;
    };
    let mut words = t.split(' ');
    match words.next()? {
        "sync" => {
            let rest: Vec<&str> = words.collect();
            Some(Reply::CatchUp(CatchUp {
                since: rest.last().and_then(|p| position(p)),
                full: rest.first() == Some(&"full"),
            }))
        }
        "promote" => Some(Reply::Promote(words.next().and_then(position))),
        "pong" => Some(Reply::Pong(words.next()?.parse().ok()?)),
        _ => None,
    }
}

/// The `t` of a ping.
fn ping_of(message: &Message) -> Option<u64> {
    match message {
        Message::Text(t) => t.strip_prefix("ping ")?.parse().ok(),
        _ => None,
    }
}

fn pong(t: u64) -> Result<Message, ()> {
    Ok(Message::Text(format!("pong {t}").into()))
}

fn text(t: &str) -> Result<Message, ()> {
    Ok(Message::Text(t.to_owned().into()))
}

/// The first message of a connection: catch up from `since`.
fn sync(since: Option<(u64, u64)>) -> Result<Message, ()> {
    match since {
        Some((epoch, revision)) => text(&format!("sync {epoch}:{revision}")),
        None => text("sync"),
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
    /// Runs `serve_client` over channels with the [`Wire`] protocol; the
    /// product callback answers other text `not applied` while it may not
    /// submit commands ([`Admission::NotApplied`]), else echoes it, answers text starting with
    /// `slow` after 1.5 s, `beat` at once, `quiet` with nothing at once and
    /// grants a meter credit for the binary message `credit`. Sends nothing
    /// for the client.
    fn open(hub: &Hub, state: &Arc<FakeState>, connect: Connect) -> Client {
        let queue = hub.subscribe(&connect.owner);
        let state = state.clone();
        let (to_client, rx) = mpsc::unbounded_channel();
        let (tx, from_client) = mpsc::unbounded_channel();
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        let q = queue.clone();
        let task = tokio::spawn(async move {
            serve_client(socket, &connect, &q, &*state, Wire, |m, admission| {
                if let Some(reply) = protocol(&m) {
                    return reply;
                }
                if let (Message::Text(t), Admission::NotApplied) = (&m, admission) {
                    let refused = Some(Message::Text(format!("not applied {t}").into()));
                    return Reply::Command(Box::pin(async move { refused }) as BoxFuture<_>);
                }
                let q = q.clone();
                match &m {
                    Message::Text(t) if t.as_str() == "beat" => {
                        return Reply::Now(Some(Message::Text("beat back".into())));
                    }
                    Message::Text(t) if t.as_str() == "quiet" => return Reply::Now(None),
                    Message::Text(t) if t.as_str() == "hang" => {
                        return Reply::Command(Box::pin(std::future::pending()) as BoxFuture<_>);
                    }
                    _ => {}
                }
                Reply::Command(Box::pin(async move {
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
                }) as BoxFuture<'static, Option<Message>>)
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

    /// [`Client::open`], catching up without a position.
    fn start(hub: &Hub, state: &Arc<FakeState>, connect: Connect) -> Client {
        let client = Client::open(hub, state, connect);
        client.tx.send(sync(None)).unwrap();
        client
    }

    /// [`Client::open`], catching up from `since`.
    fn connect(hub: &Hub, state: &Arc<FakeState>, since: Option<(u64, u64)>) -> Client {
        let sessions = Arc::new(Sessions::new("r1").unwrap());
        let client = Client::open(hub, state, connect(&sessions, Role::Operator, 1));
        client.tx.send(sync(since)).unwrap();
        client
    }

    /// The next message other than a ping, answering pings when `pong`.
    /// Within 3 s in all, so pings arriving meanwhile cannot extend it: an
    /// answer held for the read index takes 2 s.
    async fn next_message(&mut self) -> Message {
        let wait = async {
            loop {
                let message = self.rx.recv().await.expect("serving");
                match ping_of(&message) {
                    Some(t) if self.pong => {
                        let _ = self.tx.send(pong(t));
                    }
                    Some(_) => {}
                    None => return message,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(3), wait)
            .await
            .expect("a message within 3 s")
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
    let hub = Hub::new(8, &Metrics::default());
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

/// Product frames pushed before the first catch-up wait for its answer,
/// as deltas do, and go out as text messages. A text frame goes out in
/// push order with the deltas: never after a delta pushed after it, never
/// ahead of one pushed before it.
#[tokio::test(start_paused = true)]
async fn product_frames_go_out_as_text_in_push_order_with_deltas() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    let to = To::Session {
        principal: "anonymous",
        session: "s1",
    };
    assert_eq!(hub.send(to, text_frame("early")).reached, 1);
    hub.publish(status(UNPACED, "status"));
    client.silent().await;
    client.tx.send(sync(None)).unwrap();
    assert_eq!(client.next().await, "snapshot@7:0");
    assert_eq!(client.next().await, "early");
    assert_eq!(client.next().await, "status");

    let one = To::Connection {
        principal: "anonymous",
        connection: "r1:1",
    };
    hub.send(one, text_frame("before"));
    apply(&state, &hub);
    hub.send(one, text_frame("after"));
    apply(&state, &hub);
    for expected in ["before", "delta@1", "after", "delta@2"] {
        assert_eq!(client.next().await, expected);
    }
    client.silent().await;
}

/// A text frame pushed before a delta overflow goes with the deltas the
/// catch-up replaces: the client gets the state, never the text after it.
#[tokio::test(start_paused = true)]
async fn no_text_pushed_before_a_resync_goes_out_after_its_catch_up() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(2, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    // No await in between, so the client cannot drain the queue.
    hub.publish(text_frame("stale"));
    for _ in 0..3 {
        apply(&state, &hub);
    }
    for expected in ["delta@1", "delta@2", "delta@3"] {
        assert_eq!(client.next().await, expected);
    }
    client.silent().await;
}

/// A text frame that overflows the queue closes the connection, so the
/// client catches up on a new one rather than missing it unseen.
#[tokio::test(start_paused = true)]
async fn a_text_overflow_closes_the_connection() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(1, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    hub.publish(text_frame("t1"));
    let sent = hub.publish(text_frame("t2"));
    assert_eq!(
        sent,
        Sent {
            reached: 0,
            dropped: 1
        }
    );
    assert_eq!(client.ended().await, Ended::Overflow);
    let mut last = None;
    while let Ok(m) = client.rx.try_recv() {
        assert!(ping_of(&m).is_some() || m == Message::Close(None), "{m:?}");
        last = Some(m);
    }
    assert_eq!(last, Some(Message::Close(None)));
}

/// A client above the read index is sent no text frame, as it is sent no
/// delta, and its status frames wait until it is answered with state.
#[tokio::test(start_paused = true)]
async fn a_client_ahead_gets_no_text_and_its_statuses_wait() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    let mut client = Client::connect(&hub, &state, Some((7, 5)));
    assert_eq!(client.next().await, "ahead@7:3 below 5");
    hub.publish(text_frame("text"));
    hub.publish(status(UNPACED, "status"));
    hub.publish(Frame::Counters("c".into()));
    assert_eq!(client.next().await, "c");
    client.silent().await;
    client.tx.send(sync(None)).unwrap();
    assert_eq!(client.next().await, "snapshot@7:3");
    assert_eq!(client.next().await, "status");
    client.silent().await;
}

/// A state initialised anew or restored has another epoch: every client
/// answered in the old one is closed, whether a delta of the new epoch
/// reaches it or not, and a client reconnecting with its old position gets
/// a snapshot of the new epoch at once, never held for a read index.
#[tokio::test(start_paused = true)]
async fn an_epoch_change_closes_every_connection() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..5 {
        apply(&state, &hub);
    }
    let mut resumed = Client::connect(&hub, &state, Some((7, 5)));
    let mut fresh = Client::connect(&hub, &state, None);
    resumed.silent().await;
    assert_eq!(fresh.next().await, "snapshot@7:5");

    let start = Instant::now();
    state.reset(8);
    for client in [&mut resumed, &mut fresh] {
        assert_eq!(client.next_message().await, Message::Close(None));
        assert_eq!(client.ended().await, Ended::EpochChanged);
    }
    assert!(start.elapsed() <= PING_PERIOD, "{:?}", start.elapsed());

    let mut delta = Client::connect(&hub, &state, None);
    assert_eq!(delta.next().await, "snapshot@8:0");
    state.reset(9);
    apply(&state, &hub);
    assert_eq!(delta.next_message().await, Message::Close(None));
    assert_eq!(delta.ended().await, Ended::EpochChanged);

    *state.read_index.lock().unwrap() = Reads::Hang;
    let start = Instant::now();
    let mut reconnected = Client::connect(&hub, &state, Some((7, 5)));
    assert_eq!(
        reconnected.next().await,
        "snapshot@9:1",
        "revision 1 is below 5"
    );
    assert_eq!(start.elapsed(), Duration::ZERO);
    apply(&state, &hub);
    assert_eq!(reconnected.next().await, "delta@2");
}

/// A connection whose credentials end is closed then, with a close code
/// the client can tell from a lost connection; one without an end goes on.
#[tokio::test(start_paused = true)]
async fn a_connection_closes_when_its_session_ends() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut ending = connect(&sessions, Role::Operator, 1);
    ending.ends = SessionEnd::at(samplez_service::auth::now() + 2);
    let mut client = Client::start(&hub, &state, ending);
    let mut lasting = Client::start(&hub, &state, connect(&sessions, Role::Operator, 2));
    assert_eq!(client.next().await, "snapshot@7:0");
    assert_eq!(lasting.next().await, "snapshot@7:0");
    // The other connection answers its pings meanwhile.
    let (close, ()) = tokio::join!(
        async {
            loop {
                match client.next_message().await {
                    Message::Close(c) => break c,
                    Message::Text(t) => panic!("unexpected {t}"),
                    _ => {}
                }
            }
        },
        async {
            let quiet =
                tokio::time::timeout(Duration::from_millis(2500), lasting.next_message()).await;
            assert!(quiet.is_err(), "unexpected {quiet:?}");
        }
    );
    let close = close.expect("a close frame");
    assert_eq!(close.code, SESSION_ENDED_CLOSE);
    assert_eq!(client.ended().await, Ended::SessionEnded);
    apply(&state, &hub);
    assert_eq!(lasting.next().await, "delta@1");
}

/// A catch-up held when the epoch changes answers with the new epoch's
/// state, which the client's revisions do not name: the connection is
/// closed instead, before the next ping would find the change.
#[tokio::test(start_paused = true)]
async fn an_answer_of_another_epoch_closes_the_connection() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    *state.read_index.lock().unwrap() = Reads::Committed(1);
    client.tx.send(sync(Some((7, 0)))).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let start = Instant::now();
    state.reset(8);
    state.apply(None);
    assert_eq!(client.next_message().await, Message::Close(None));
    assert_eq!(client.ended().await, Ended::EpochChanged);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_gap_in_prev_catches_up_from_the_state() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
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

/// A client that saw revision 3 on another replica reaches one that has
/// applied 1: the answer waits until this replica has applied what was
/// committed, so it never goes back, and the deltas the client saw are
/// skipped.
#[tokio::test(start_paused = true)]
async fn a_lagging_replica_answers_once_it_applied_the_read_index() {
    let follower = FakeState::new("r2", "follower");
    let hub = Hub::new(8, &Metrics::default());
    apply(&follower, &hub);
    *follower.read_index.lock().unwrap() = Reads::Committed(4);

    let mut client = Client::connect(&hub, &follower, Some((7, 3)));
    client.silent().await;
    apply(&follower, &hub);
    apply(&follower, &hub);
    client.silent().await;
    apply(&follower, &hub);
    assert_eq!(client.next().await, "delta@4", "2 and 3 skipped");
    client.silent().await;
    apply(&follower, &hub);
    assert_eq!(client.next().await, "delta@5");
    assert_eq!(
        follower.catch_ups.load(Ordering::Relaxed),
        1,
        "the deltas the answer covered start no catch-up of their own"
    );
}

/// A follower confirms no read index: it holds the answer, within the
/// same bound, until it has applied the client's position, and answers
/// it unmarked then; one that does not get there answers stale.
#[tokio::test(start_paused = true)]
async fn a_follower_answers_once_it_applied_the_clients_position() {
    let follower = FakeState::new("r2", "follower");
    let hub = Hub::new(8, &Metrics::default());
    apply(&follower, &hub);
    *follower.read_index.lock().unwrap() = Reads::NotLeader;
    let mut client = Client::connect(&hub, &follower, Some((7, 3)));
    client.silent().await;
    apply(&follower, &hub);
    client.silent().await;
    apply(&follower, &hub);
    // Answered at 3 with no delta to send, and not marked stale.
    client.silent().await;
    apply(&follower, &hub);
    assert_eq!(client.next().await, "delta@4");

    let start = Instant::now();
    let mut behind = Client::connect(&hub, &follower, Some((7, 9)));
    assert_eq!(behind.next().await, "snapshot@7:4 stale");
    assert_eq!(start.elapsed(), READ_INDEX_HOLD);
}

/// Held up to [`READ_INDEX_HOLD`]: a replica that does not get the read
/// index, or does not apply it, by then answers with what it has applied,
/// marked stale, even when that is older than the client's position.
#[tokio::test(start_paused = true)]
async fn a_catch_up_held_for_two_seconds_answers_stale() {
    let follower = FakeState::new("r2", "follower");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&follower, &hub);
    }
    for (read_index, since, answer) in [
        (Reads::Committed(9), (7, 1), "resume@7:3 stale"),
        (Reads::Hang, (7, 3), "resume@7:3 stale"),
        (Reads::Hang, (7, 5), "snapshot@7:3 stale"),
    ] {
        *follower.read_index.lock().unwrap() = read_index;
        let start = Instant::now();
        let mut client = Client::connect(&hub, &follower, Some(since));
        assert_eq!(client.next().await, answer, "{read_index:?} {since:?}");
        assert_eq!(start.elapsed(), READ_INDEX_HOLD);
        if since.1 == 1 {
            assert_eq!(client.next().await, "delta@2");
            assert_eq!(client.next().await, "delta@3");
        }
        client.silent().await;
    }
}

/// A replica cut off from the quorum cannot confirm its state and says so
/// at once, rather than holding the client for two seconds.
#[tokio::test(start_paused = true)]
async fn a_replica_cut_off_answers_stale_at_once() {
    let state = FakeState::new("r2", "follower");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    *state.read_index.lock().unwrap() = Reads::CutOff;
    let start = Instant::now();
    let mut resumed = Client::connect(&hub, &state, Some((7, 1)));
    assert_eq!(resumed.next().await, "resume@7:3 stale");
    assert_eq!(resumed.next().await, "delta@2");
    assert_eq!(resumed.next().await, "delta@3");
    let mut behind = Client::connect(&hub, &state, Some((7, 5)));
    assert_eq!(behind.next().await, "snapshot@7:3 stale");
    let mut fresh = Client::connect(&hub, &state, None);
    assert_eq!(fresh.next().await, "snapshot@7:3", "nothing to confirm");
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// A position above what was committed in the state's epoch is state the
/// replication never had: the client is told so and sent no deltas until
/// it takes the state again.
#[tokio::test(start_paused = true)]
async fn a_position_above_the_read_index_is_answered_ahead() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    let mut client = Client::connect(&hub, &state, Some((7, 5)));
    assert_eq!(client.next().await, "ahead@7:3 below 5");
    client.tx.send(text("sync full 7:5")).unwrap();
    assert_eq!(client.next().await, "ahead@7:3 below 5", "full or not");
    // Revision 6 follows the client's 5, but names other changes.
    for _ in 0..3 {
        apply(&state, &hub);
    }
    hub.publish(Frame::Counters("c".into()));
    assert_eq!(client.next().await, "c", "no delta while ahead");
    client.tx.send(sync(None)).unwrap();
    assert_eq!(client.next().await, "snapshot@7:6");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@7");
}

/// `full` takes a snapshot where a resume would do, after the same hold.
#[tokio::test(start_paused = true)]
async fn a_full_catch_up_is_a_snapshot() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    let mut client = Client::connect(&hub, &state, Some((7, 3)));
    client.silent().await;
    client.tx.send(text("sync full 7:3")).unwrap();
    assert_eq!(client.next().await, "snapshot@7:3");
}

/// The messages after a catch-up request are read once it is answered, so
/// a command sent after it is applied and answered after the state it
/// answers with. The pause does not count against the client's pong
/// timeout, and the server's pings go on meanwhile.
#[tokio::test(start_paused = true)]
async fn nothing_after_a_catch_up_request_is_read_until_it_is_answered() {
    let state = FakeState::new("r2", "follower");
    let hub = Hub::new(8, &Metrics::default());
    apply(&state, &hub);
    *state.read_index.lock().unwrap() = Reads::Committed(2);
    let sessions = Arc::new(Sessions::new("r2").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    client.pong = false;
    for message in [sync(Some((7, 1))), text("beat"), text("hello")] {
        client.tx.send(message).unwrap();
    }
    let mut pings = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(1500) {
        match tokio::time::timeout(Duration::from_millis(100), client.rx.recv()).await {
            Ok(Some(m)) if ping_of(&m).is_some() => pings += 1,
            Ok(other) => panic!("unexpected {other:?}"),
            Err(_) => {}
        }
    }
    assert!(pings >= 5, "{pings} pings");
    assert_eq!(sessions.live().len(), 1, "not read, so not timed out");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@2");
    assert_eq!(client.next().await, "beat back");
    assert_eq!(client.next().await, "echo hello");
}

/// After a promote of the state's epoch, nothing is read until this
/// replica has applied the client's position, so a command the client
/// sends next never applies to older state than it saw. Another epoch's
/// position, or none, holds nothing.
#[tokio::test(start_paused = true)]
async fn commands_after_a_promote_wait_until_its_position_is_applied() {
    let state = FakeState::new("r2", "follower");
    *state.read_index.lock().unwrap() = Reads::NotLeader;
    let hub = Hub::new(8, &Metrics::default());
    apply(&state, &hub);
    let mut client = Client::connect(&hub, &state, Some((7, 1)));
    client.silent().await;
    for message in [text("promote 7:3"), text("hello")] {
        client.tx.send(message).unwrap();
    }
    client.silent().await;
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@2");
    client.silent().await;
    apply(&state, &hub);
    let mut got = [client.next().await, client.next().await];
    got.sort();
    assert_eq!(got, ["delta@3", "echo hello"]);

    for promote in ["promote 6:9", "promote", "promote 7:2"] {
        client.tx.send(text(promote)).unwrap();
        client.tx.send(text("hello")).unwrap();
        assert_eq!(client.next().await, "echo hello", "{promote}");
    }
}

/// A promote above everything committed in the state's epoch, then the
/// resync the client sends after it: the read index releases the hold, so
/// the resync is answered `ahead` rather than never read.
#[tokio::test(start_paused = true)]
async fn a_promote_above_the_read_index_then_resync_is_answered_ahead() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    *state.read_index.lock().unwrap() = Reads::Committed(3);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:3");
    client.tx.send(text("promote 7:5")).unwrap();
    client.tx.send(sync(Some((7, 5)))).unwrap();
    let start = Instant::now();
    assert_eq!(client.next().await, "ahead@7:3 below 5");
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// The contract applies no command that follows a promote of the state's
/// epoch before its position is applied: after one above the read index,
/// a command read before the client takes the cluster's state is answered
/// not applied and never reaches the product; once the client took a
/// snapshot, commands apply again.
#[tokio::test(start_paused = true)]
async fn a_command_after_a_promote_above_the_read_index_is_not_applied() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    for _ in 0..3 {
        apply(&state, &hub);
    }
    *state.read_index.lock().unwrap() = Reads::Committed(3);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:3");
    for m in [text("promote 7:5"), text("hello"), sync(Some((7, 5)))] {
        client.tx.send(m).unwrap();
    }
    // The refusal is ready before the resync is read, so either may go
    // first.
    let mut got = [client.next().await, client.next().await];
    got.sort();
    assert_eq!(got, ["ahead@7:3 below 5", "not applied hello"]);
    client.silent().await;
    client.tx.send(text("again")).unwrap();
    assert_eq!(client.next().await, "not applied again", "still ahead");
    client.tx.send(sync(None)).unwrap();
    client.tx.send(text("hello")).unwrap();
    assert_eq!(client.next().await, "snapshot@7:3");
    assert_eq!(client.next().await, "echo hello");
}

/// A position this replica never applies holds the client's next commands
/// for no longer than the read index hold plus [`MAX_PAUSE`].
#[tokio::test(start_paused = true)]
async fn a_promote_never_applied_ends_the_connection() {
    let state = FakeState::new("r2", "follower");
    *state.read_index.lock().unwrap() = Reads::NotLeader;
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    client.tx.send(text("promote 7:3")).unwrap();
    client.tx.send(text("hello")).unwrap();
    let ended = loop {
        match tokio::time::timeout(Duration::from_secs(10), client.rx.recv()).await {
            Ok(Some(m)) if ping_of(&m).is_some() => {}
            Ok(None) => break client.ended().await,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(ended, Ended::Failed);
    assert_eq!(start.elapsed(), READ_INDEX_HOLD + MAX_PAUSE);
}

/// The product makes the pings, each carrying the milliseconds since the
/// connection was accepted; WebSocket protocol pings and pongs from the
/// client are not heartbeats.
#[tokio::test(start_paused = true)]
async fn pings_are_the_products_with_the_time_since_the_connection_opened() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    let (mut ts, mut others) = (Vec::new(), Vec::new());
    while ts.len() < 4 {
        let m = client.rx.recv().await.expect("serving");
        match ping_of(&m) {
            Some(t) => {
                client.tx.send(Ok(Message::Ping(Bytes::new()))).unwrap();
                client.tx.send(pong(t)).unwrap();
                ts.push(t);
            }
            None => others.push(m),
        }
    }
    assert_eq!(ts, [0, 250, 500, 750]);
    assert_eq!(others, [Message::Text("snapshot@7:0".into())]);
    client.silent().await;
}

/// No ping goes out before the client's first catch-up request, its hello:
/// the product's protocol has the client identify itself first. The first
/// ping goes out with the request, and they follow every ping period.
#[tokio::test(start_paused = true)]
async fn pings_start_with_the_first_catch_up() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(client.rx.try_recv().is_err(), "a ping before the hello");
    client.tx.send(sync(None)).unwrap();
    let (mut ts, mut others) = (Vec::new(), Vec::new());
    while ts.len() < 3 {
        let m = client.rx.recv().await.expect("serving");
        match ping_of(&m) {
            Some(t) => {
                client.tx.send(pong(t)).unwrap();
                ts.push(t);
            }
            None => others.push(m),
        }
    }
    assert_eq!(ts, [600, 850, 1100]);
    assert_eq!(others, [Message::Text("snapshot@7:0".into())]);
    assert_eq!(sessions.live().len(), 1);
}

/// A client that never asks to catch up is sent nothing and closed once
/// [`PONG_TIMEOUT`] passed since it was accepted, as one that stops
/// answering pings is.
#[tokio::test(start_paused = true)]
async fn a_client_that_sends_no_catch_up_is_closed_after_the_pong_timeout() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    let start = Instant::now();
    assert_eq!(client.ended().await, Ended::PongTimeout);
    assert_eq!(start.elapsed(), PONG_TIMEOUT);
    assert_eq!(client.rx.recv().await, Some(Message::Close(None)));
    assert!(sessions.live().is_empty());
}

/// The bursts below run without an await, so the client cannot drain the
/// two-delta queue in between.
#[tokio::test(start_paused = true)]
async fn overflow_catches_up_from_the_last_sent_revision() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(2, &Metrics::default());
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
    let hub = Hub::new(8, &Metrics::default());
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
    let hub = Hub::new(8, &Metrics::default());
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
    let hub = Hub::new(8, &Metrics::default());
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
    let hub = Hub::new(8, &Metrics::default());
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

/// `Reply::Now(None)` sends nothing and takes no place among the replies
/// waiting: one place short of the limit, the connection is still read.
#[tokio::test(start_paused = true)]
async fn a_message_answered_now_with_nothing_sends_nothing_and_counts_nothing() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    for n in 1..MAX_PENDING_REPLIES {
        let slow = format!("slow {n}");
        client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
    }
    client.tx.send(Ok(Message::Text("quiet".into()))).unwrap();
    client.tx.send(Ok(Message::Text("beat".into()))).unwrap();
    assert_eq!(client.next().await, "beat back");
    assert_eq!(start.elapsed(), Duration::ZERO, "read below the limit");
    for n in 1..MAX_PENDING_REPLIES {
        assert_eq!(client.next().await, format!("slow {n} done"));
    }
    client.tx.send(Ok(Message::Text("hello".into()))).unwrap();
    assert_eq!(client.next().await, "echo hello", "nothing for quiet");
}

/// A command a client sends ahead of its first catch-up request is
/// answered after the state the answer brings it to.
#[tokio::test(start_paused = true)]
async fn no_reply_goes_out_before_the_first_catch_up() {
    let state = FakeState::new("r1", "leader");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(500);
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    client.tx.send(text("hello")).unwrap();
    client.silent().await;
    client.tx.send(sync(None)).unwrap();
    assert_eq!(client.next().await, "snapshot@7:0");
    assert_eq!(client.next().await, "echo hello");
}

/// A catch-up after a gap the client did not ask for does not hold its
/// messages, so its pongs are read during a slow snapshot.
#[tokio::test(start_paused = true)]
async fn a_slow_snapshot_keeps_the_heartbeat() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(1500);
    state.apply(None);
    state.forget_through(1);
    apply(&state, &hub);
    assert_eq!(client.next().await, "snapshot@7:2");
    apply(&state, &hub);
    assert_eq!(client.next().await, "delta@3", "still served");
}

/// A reply ready while a catch-up after a gap is under way waits for it,
/// so it never goes out ahead of the state it refers to.
#[tokio::test(start_paused = true)]
async fn a_reply_waits_for_a_catch_up_under_way() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(1000);
    state.apply(None);
    state.forget_through(1);
    apply(&state, &hub);
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.tx.send(text("hello")).unwrap();
    assert_eq!(client.next().await, "snapshot@7:2");
    assert_eq!(client.next().await, "echo hello");
}

/// The epoch is checked before each reply: one ready between two pings
/// after the epoch changed closes the connection instead of going out.
#[tokio::test(start_paused = true)]
async fn a_reply_after_an_epoch_change_closes_the_connection() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    client.silent().await;
    // Answered 1.5 s later, at 1800 ms; pings go at multiples of 250 ms.
    client.tx.send(text("slow")).unwrap();
    for _ in 0..4 {
        client.silent().await;
    }
    // Past the ping at 1750 ms, before the reply.
    tokio::time::sleep(Duration::from_millis(260)).await;
    let changed = Instant::now();
    state.reset(8);
    assert_eq!(client.next_message().await, Message::Close(None));
    assert_eq!(client.ended().await, Ended::EpochChanged);
    assert_eq!(changed.elapsed(), Duration::from_millis(40));
}

#[tokio::test(start_paused = true)]
async fn a_connection_is_not_read_while_its_replies_are_at_the_limit() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let connect = connect(&sessions, Role::Operator, 1);
    let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
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
            serve_client(socket, &connect, &queue, &*state, Wire, |_, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                Reply::Command(std::future::pending::<Option<Message>>())
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

/// Heartbeats count against nothing: with replies one short of the limit
/// waiting, they are still read and answered ahead of those, and the next
/// command is read as well; it reaches the limit, and nothing is read until
/// one of them is ready.
#[tokio::test(start_paused = true)]
async fn replies_sent_at_once_do_not_count_against_the_limit() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    for _ in 1..MAX_PENDING_REPLIES {
        client.tx.send(Ok(Message::Text("slow".into()))).unwrap();
    }
    for _ in 0..2 * MAX_PENDING_REPLIES {
        client.tx.send(Ok(Message::Text("beat".into()))).unwrap();
    }
    for _ in 0..2 * MAX_PENDING_REPLIES {
        assert_eq!(client.next().await, "beat back");
    }
    assert_eq!(start.elapsed(), Duration::ZERO, "ahead of the slow replies");
    client.tx.send(Ok(Message::Text("slow".into()))).unwrap();
    client.tx.send(Ok(Message::Text("beat".into()))).unwrap();
    assert_eq!(client.next().await, "slow done", "the limit reached");
    assert_eq!(start.elapsed(), Duration::from_millis(1500));
    let mut rest = Vec::new();
    for _ in 0..MAX_PENDING_REPLIES {
        rest.push(client.next().await);
    }
    rest.sort();
    assert_eq!(rest.pop().as_deref(), Some("slow done"));
    assert_eq!(rest.first().map(String::as_str), Some("beat back"));
    assert!(rest[1..].iter().all(|m| m == "slow done"), "{rest:?}");
}

/// A heartbeat answer to a message read before the first catch-up does
/// not wait for it.
#[tokio::test(start_paused = true)]
async fn a_reply_sent_at_once_goes_out_before_the_first_catch_up() {
    let state = FakeState::new("r1", "leader");
    *state.snapshot_delay.lock().unwrap() = Duration::from_millis(500);
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    client.tx.send(text("beat")).unwrap();
    client.tx.send(sync(None)).unwrap();
    assert_eq!(client.next().await, "beat back");
    assert_eq!(client.next().await, "snapshot@7:0");
}

/// Replies that never resolve, to a client that never answers a ping: a
/// peer that is gone is not held for as long as the replies hang.
#[tokio::test(start_paused = true)]
async fn a_connection_not_read_at_the_limit_ends_after_the_longest_pause() {
    let state = FakeState::new("r1", "leader");
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let connect = connect(&sessions, Role::Operator, 1);
    let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
    let (to_client, _rx) = mpsc::unbounded_channel();
    let (tx, from_client) = mpsc::unbounded_channel();
    let start = Instant::now();
    let task = tokio::spawn(async move {
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        serve_client(socket, &connect, &queue, &*state, Wire, |_, _| {
            Reply::Command(std::future::pending::<Option<Message>>())
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
    let connect = connect(&sessions, Role::Operator, 1);
    let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
    let (to_client, _rx) = mpsc::unbounded_channel();
    let (tx, from_client) = mpsc::unbounded_channel();
    let reply = MAX_PAUSE - Duration::from_millis(100);
    let start = Instant::now();
    let task = tokio::spawn(async move {
        let socket = ChannelSocket {
            to_client,
            from_client,
        };
        serve_client(socket, &connect, &queue, &*state, Wire, |m, _| {
            protocol(&m).unwrap_or(Reply::Command(Box::pin(async move {
                tokio::time::sleep(reply).await;
                Some(Message::Text("done".into()))
            }) as BoxFuture<_>))
        })
        .await
    });
    tx.send(sync(None)).unwrap();
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
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
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
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
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
                    Some(m) if ping_of(&m).is_some() => {
                        let _ = client.tx.send(pong(ping_of(&m).unwrap()));
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
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
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

/// A pong read after a pause counts from its ping's send moved on by the
/// pause, as the other pongs and the pause itself: the client answered in
/// time, and only the server did not read it.
#[tokio::test(start_paused = true)]
async fn a_pong_read_after_a_pause_counts_the_pause() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    let mut paused_at = None;
    while start.elapsed() < Duration::from_secs(4) && !client.task.is_finished() {
        let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(50), client.rx.recv()).await
        else {
            continue;
        };
        let Some(t) = ping_of(&m) else { continue };
        match paused_at {
            None if start.elapsed() >= Duration::from_millis(500) => {
                // The pong waits unread behind a limit's worth of replies.
                for n in 0..MAX_PENDING_REPLIES {
                    client.tx.send(text(&format!("slow {n}"))).unwrap();
                }
                client.tx.send(pong(t)).unwrap();
                paused_at = Some(Instant::now());
            }
            // Silent until 300 ms after the pause, so only that pong
            // carries the client over it.
            Some(at) if at.elapsed() < Duration::from_millis(1800) => {}
            _ => client.tx.send(pong(t)).unwrap(),
        }
    }
    assert!(
        !client.task.is_finished(),
        "ended {:?}",
        client.ended().await
    );
    assert_eq!(sessions.live().len(), 1);
}

/// A client answering every ping, each pong queued behind another limit's
/// worth of messages, that then stops: the pongs it left wait unread
/// through the backlog, and each one read counts from when its ping was
/// sent, so the backlog does not keep the gone peer live.
#[tokio::test(start_paused = true)]
async fn pongs_left_behind_a_backlog_do_not_keep_a_gone_peer_live() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    let start = Instant::now();
    let mut last_ping = start;
    while start.elapsed() < Duration::from_secs(2) {
        if let Some(t) = ping_of(&client.rx.recv().await.expect("serving")) {
            for n in 0..MAX_PENDING_REPLIES {
                let slow = format!("slow {n}");
                client.tx.send(Ok(Message::Text(slow.into()))).unwrap();
            }
            client.tx.send(pong(t)).unwrap();
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

/// A pong counts only when it echoes the `t` of a ping: a made-up one, or
/// a WebSocket protocol pong echoing the ping's encoding, does not keep the
/// connection.
#[tokio::test(start_paused = true)]
async fn a_pong_that_echoes_no_ping_does_not_count() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let mut client = Client::connect(&hub, &state, None);
    client.pong = false;
    let start = Instant::now();
    let answer = async {
        loop {
            match client.rx.recv().await {
                Some(m) if ping_of(&m).is_some() => {
                    let t = ping_of(&m).unwrap();
                    let _ = client.tx.send(pong(t + 1));
                    let _ = client.tx.send(pong(u64::MAX));
                    let _ = client
                        .tx
                        .send(Ok(Message::Pong(Bytes::from(format!("pong {t}")))));
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

/// An unsolicited pong carrying a `t` above every ping outstanding,
/// including the `t` of the next ping not yet sent, does not drop those
/// pings: the pong that echoes one still counts.
#[tokio::test(start_paused = true)]
async fn an_unsolicited_pong_does_not_drop_the_pings_outstanding() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    let start = Instant::now();
    while start.elapsed() < 4 * PONG_TIMEOUT {
        if let Some(t) = ping_of(&client.rx.recv().await.expect("serving")) {
            let next = t + PING_PERIOD.as_millis() as u64;
            for unsolicited in [1_700_000_000_000u64, next] {
                client.tx.send(pong(unsolicited)).unwrap();
            }
            client.tx.send(pong(t)).unwrap();
        }
    }
    assert!(!client.task.is_finished());
    assert_eq!(sessions.live().len(), 1);
}

/// A client may answer only the most recent of several pings: answering
/// every third one keeps the connection, because
/// liveness counts from the ping answered, not from the oldest one the
/// answer drops.
#[tokio::test(start_paused = true)]
async fn a_client_answering_every_third_ping_stays_connected() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    let start = Instant::now();
    let mut n = 0u32;
    while start.elapsed() < 4 * PONG_TIMEOUT {
        if let Some(t) = ping_of(&client.rx.recv().await.expect("serving")) {
            if n % 3 == 2 {
                client.tx.send(pong(t)).unwrap();
            }
            n += 1;
        }
    }
    assert!(!client.task.is_finished());
    assert_eq!(sessions.live().len(), 1);
}

/// Answers every ping `delay` after it was sent, for 4 s or until the
/// connection is closed: how it ended and when, if it did, and whether it
/// was live throughout.
async fn pongs_delayed_by(hello: Duration, delay: Duration) -> (Option<(Ended, Duration)>, bool) {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut client = Client::open(&hub, &state, connect(&sessions, Role::Operator, 1));
    let start = Instant::now();
    tokio::time::sleep(hello).await;
    client.tx.send(sync(None)).unwrap();
    let mut always_live = true;
    while start.elapsed() < 4 * PONG_TIMEOUT {
        match tokio::time::timeout(Duration::from_millis(50), client.rx.recv()).await {
            Ok(Some(m)) if ping_of(&m).is_some() => {
                let (tx, t) = (client.tx.clone(), ping_of(&m).unwrap());
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = tx.send(pong(t));
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
    let hello = Duration::ZERO;
    assert_eq!(
        pongs_delayed_by(hello, ceiling - margin).await,
        (None, true)
    );
    let (ended, _) = pongs_delayed_by(hello, ceiling + margin).await;
    assert_eq!(ended, Some((Ended::PongTimeout, PONG_TIMEOUT)));
}

/// The pong timeout counts from the hello, as no ping went out before it:
/// a client whose hello is read just inside it keeps the connection, and
/// stays live, with the slowest round trip allowed.
#[tokio::test(start_paused = true)]
async fn a_late_hello_has_the_whole_pong_timeout_for_its_first_pong() {
    let margin = Duration::from_millis(50);
    let hello = PONG_TIMEOUT - 2 * margin;
    let slowest = PONG_TIMEOUT - PING_PERIOD - margin;
    assert_eq!(pongs_delayed_by(hello, slowest).await, (None, true));
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
    let connect = connect(&sessions, Role::Operator, 1);
    let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
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
        serve_client(socket, &connect, &queue, &*state, Wire, |m, _| {
            protocol(&m).unwrap_or(Reply::Command(Box::pin(async move {
                tokio::time::sleep_until(ready).await;
                Some(Message::Text("done".into()))
            }) as BoxFuture<_>))
        })
        .await
    });
    tx.send(sync(None)).unwrap();
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
        let connect = connect(&sessions, Role::Operator, n);
        let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
        let state = state.clone();
        let (to_client, rx) = mpsc::unbounded_channel();
        let (tx, from_client) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let socket = ChannelSocket {
                to_client,
                from_client,
            };
            serve_client(socket, &connect, &queue, &*state, Wire, |m, _| {
                protocol(&m).unwrap_or(Reply::Command(Box::pin(async move {
                    tokio::time::sleep_until(end).await;
                    Some(Message::Text("done".into()))
                }) as BoxFuture<_>))
            })
            .await
        });
        tx.send(sync(None)).unwrap();
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
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let renew = tokio::spawn({
        let (sessions, state) = (sessions.clone(), state.clone());
        async move { samplez_service::server::renew_sessions(&sessions, &*state).await }
    });
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(state.renewals.lock().unwrap().is_empty(), "nobody to renew");

    let mut a = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    let mut b = Client::start(&hub, &state, connect(&sessions, Role::Viewer, 2));
    let mut silent = Client::start(&hub, &state, connect(&sessions, Role::Operator, 3));
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

/// A session is renewed from the first renewal after its connection
/// opens, through its pongs while it answers, and no more once the
/// connection closed: a touch as its first command, then a disconnect,
/// leaves the lease to run out from there.
#[tokio::test(start_paused = true)]
async fn a_session_is_renewed_while_its_client_answers_and_not_after_it_disconnects() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, &Metrics::default());
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let renew = tokio::spawn({
        let (sessions, state) = (sessions.clone(), state.clone());
        async move { samplez_service::server::renew_sessions(&sessions, &*state).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut client = Client::start(&hub, &state, connect(&sessions, Role::Operator, 1));
    assert_eq!(client.next().await, "snapshot@7:0");
    client.tx.send(text("touch")).unwrap();
    assert_eq!(client.next().await, "echo touch");
    for _ in 0..7 {
        client.silent().await;
    }
    let renewed = |renewals: &[Vec<Owner>]| {
        renewals
            .iter()
            .filter(|r| r.iter().any(|o| o.session == "s1"))
            .count()
    };
    let renewals = state.renewals.lock().unwrap().clone();
    assert_eq!(renewed(&renewals), 2, "at 1 s and 2 s: {renewals:?}");

    client.tx.send(Ok(Message::Close(None))).unwrap();
    assert_eq!(client.ended().await, Ended::Closed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = state.renewals.lock().unwrap().clone();
    assert_eq!(renewed(&after), 2, "none after the disconnect: {after:?}");
    renew.abort();
}

async fn wait_both(a: &mut Client, b: &mut Client) {
    tokio::join!(a.silent(), b.silent());
}

#[test]
fn require_operator_refuses_a_viewer() {
    let sessions = Arc::new(Sessions::new("r1").unwrap());
    assert_eq!(
        connect(&sessions, Role::Operator, 1).require_operator(),
        Ok(())
    );
    assert_eq!(
        connect(&sessions, Role::Viewer, 1).require_operator(),
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

/// Records published to clients and logs name a session by its handle:
/// base64url of the first 16 bytes of the SHA-256 of the session id, as
/// faderz's client computes it to recognise its own.
#[test]
fn an_owner_shows_a_one_way_handle_of_its_session() {
    let session = "Zm9yLXRoZS1oYW5kbGUtdGVzdA";
    let owner = owner(session);
    assert_eq!(owner.handle().as_str(), "x5EMvQbzpnhsqp4w__cJbQ");
    assert_eq!(OwnerHandle::of("").to_string(), "47DEQpj8HBSa-_TImW-5JA");
    assert_eq!(
        serde_json::to_value(owner.handle()).unwrap(),
        "x5EMvQbzpnhsqp4w__cJbQ"
    );

    let sessions = Arc::new(Sessions::new("r1").unwrap());
    let mut connect = connect(&sessions, Role::Operator, 1);
    connect.owner = owner.clone();
    for logged in [format!("{owner:?}"), format!("{connect:?}")] {
        assert!(!logged.contains(session), "{logged}");
        assert!(logged.contains("x5EMvQbzpnhsqp4w__cJbQ"), "{logged}");
    }
}

/// A resend of a command the product refused as stale is answered stale
/// again from the table, not as a duplicate of an applied one.
#[test]
fn a_resend_of_a_stale_command_is_answered_stale() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 2);
    let s1 = owner("s1");
    state.refused_stale(&s1, "c7", 4);
    match commands.begin(&s1, "c7") {
        Begin::Duplicate(ack) => assert_eq!(ack, Ack::new("c7", 4, Resolution::Stale)),
        _ => panic!("c7 is committed"),
    }
}

/// faderz's command ids are UUIDs in their text form.
#[tokio::test]
async fn uuid_command_ids_apply_once_as_faderz_sends_them() {
    let state = FakeState::new("r1", "leader");
    let commands = Commands::new(state.clone(), 2);
    let s1 = owner("s1");
    let id = "0b9c6f1e-3d5a-4c2b-9e8f-7a6b5c4d3e2f";
    let Begin::New(pending) = commands.begin(&s1, id) else {
        panic!("new");
    };
    let submitted = pending.submit(command());
    state.commit();
    let ack = submitted.await.unwrap();
    assert_eq!(ack.command, id);
    match commands.begin(&s1, id) {
        Begin::Duplicate(again) => assert_eq!(again.revision, ack.revision),
        _ => panic!("applied"),
    }
    assert!(matches!(
        commands.begin(&s1, &id.to_uppercase()),
        Begin::New(_)
    ));
}

/// Every reason maps to the product's "not applied" answer.
#[test]
fn not_applied_is_503_with_its_reason() {
    let expired = NotApplied::new(Reason::Expired, "first received 12 s ago");
    assert_eq!(NotApplied::STATUS.as_u16(), 503);
    assert_eq!(
        expired.to_string(),
        "not applied (expired): first received 12 s ago"
    );
    assert_eq!(
        NotApplied::new(Reason::NotCommitable, "").to_string(),
        "not applied (not_commitable)"
    );
    assert_eq!(Reason::Unavailable.as_str(), "unavailable");
    assert_eq!(Reason::TooLarge.as_str(), "too_large");
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
    assert_eq!(ack, Ack::new("c1", 1, Resolution::Applied));
    let duplicate = Ack::new(ack.command.clone(), ack.revision, Resolution::Duplicate);
    assert_eq!(
        resent.await.unwrap(),
        duplicate,
        "the resend gets the revision too"
    );
    assert_eq!(state.revision(), 1, "applied once");
    match commands.begin(&s1, "c1") {
        Begin::Duplicate(again) => assert_eq!(again, duplicate, "a duplicate, not applied again"),
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
    assert_eq!(first, Ack::new("cmd-1", 1, Resolution::Applied));
    assert_eq!(second.revision, first.revision, "the same revision");
    assert_eq!(second.resolution, Resolution::Duplicate, "applied once");
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
async fn loopback_upgrade_checks_origin_and_resumes_from_the_first_message() {
    let state = FakeState::new("r1", "leader");
    let hub = Arc::new(Hub::new(8, &Metrics::default()));
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
                let queue = hub.subscribe(&connect.owner);
                serve_client(socket, &connect, &queue, &*state, Wire, |m, _| {
                    protocol(&m).unwrap_or(Reply::Command(async { None }))
                })
                .await;
            }
        }
    });
    let bound = service.bind().await.unwrap();
    let addr = bound.api_addr().unwrap();
    let server = tokio::spawn(bound.serve());

    let url = format!("ws://{addr}/api/v1/ws?session={SESSION}");
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
    ws.send("sync 7:2".into()).await.unwrap();
    assert_eq!(next_text(&mut ws).await, "delta@3");
    apply(&state, &hub);
    assert_eq!(next_text(&mut ws).await, "delta@4");

    let url = format!("ws://{addr}/api/v1/ws?since=7:3&session={longest}");
    let (mut ws, res) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        101,
        "the longest session id; `since` is not read"
    );
    ws.send("sync".into()).await.unwrap();
    assert_eq!(next_text(&mut ws).await, "snapshot@7:4");
    server.abort();
}

/// The next text message other than a ping, answering pings.
async fn next_text<S>(ws: &mut S) -> String
where
    S: Stream<Item = Result<tungstenite::Message, tungstenite::Error>>
        + Sink<tungstenite::Message>
        + Unpin,
    <S as Sink<tungstenite::Message>>::Error: std::fmt::Debug,
{
    loop {
        match ws.next().await {
            Some(Ok(tungstenite::Message::Text(t))) => match t.strip_prefix("ping ") {
                Some(t) => ws.send(format!("pong {t}").into()).await.unwrap(),
                None => return t.to_string(),
            },
            Some(Ok(tungstenite::Message::Ping(_))) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
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

/// Yields a catch-up request, takes the first message, then never accepts
/// another and never yields one: the connection stays open while the
/// server is stuck sending.
struct StuckSocket {
    sent: usize,
    synced: bool,
}

impl Stream for StuckSocket {
    type Item = Result<Message, ()>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.synced {
            Poll::Pending
        } else {
            self.synced = true;
            Poll::Ready(Some(sync(None)))
        }
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
    let connect = connect(&sessions, Role::Operator, 1);
    let queue = Hub::new(8, &Metrics::default()).subscribe(&connect.owner);
    let start = Instant::now();
    let task = tokio::spawn({
        let state = state.clone();
        async move {
            let socket = StuckSocket {
                sent: 0,
                synced: false,
            };
            serve_client(socket, &connect, &queue, &*state, Wire, |m, _| {
                protocol(&m).unwrap_or(Reply::Command(async { None }))
            })
            .await
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
