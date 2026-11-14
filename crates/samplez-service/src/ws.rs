//! Per-client WebSocket send queues, fan-out, the session registry and the
//! loop that serves one client.
//!
//! A client connecting with the last position it saw gets the deltas after
//! it, from whichever replica it reaches, when that replica holds them and
//! the epoch is the same, and a snapshot otherwise; a client without one
//! gets a snapshot. Live deltas follow in order. A delta whose `prev` is not
//! the revision last sent to the client, or whose epoch differs, makes the
//! client catch up the same way as on connect; so does a full delta queue.
//! Meters and counters keep only their latest frame; a meter frame is sent
//! only against a credit the product grants for the client.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, Utf8Bytes};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

use crate::replica::{Owner, Position, ReplicatedState};
use crate::server::Connect;

/// How often [`serve_client`] pings.
pub const PING_PERIOD: Duration = Duration::from_millis(250);
/// [`serve_client`] closes a connection that has not answered a ping for
/// this long, and only connections that answered within it are renewed.
pub const PONG_TIMEOUT: Duration = Duration::from_secs(1);
/// The replies [`serve_client`] waits for on one connection before it stops
/// reading it.
pub const MAX_PENDING_REPLIES: usize = 64;

/// A state change at a global revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub epoch: u64,
    pub revision: u64,
    /// The revision of the delta before this one in the same epoch, 0 for
    /// the first.
    pub prev: u64,
    /// The id of the command that caused it; the text echoes it.
    pub command: Option<String>,
    pub text: Utf8Bytes,
}

/// The whole state as of `revision`.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub epoch: u64,
    pub revision: u64,
    pub text: Utf8Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Delta(Delta),
    /// Sent as a binary message; replaces a meter frame not sent yet.
    Meters(Bytes),
    /// Sent as a text message; replaces a counters frame not sent yet.
    Counters(Utf8Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    /// Replaced the frame of its kind that was not sent yet.
    Replaced,
    /// A meter frame while the client has no credit.
    Skipped,
    /// The delta queue was emptied, or the delta discarded, because the
    /// client has to catch up.
    Resync,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Popped {
    Frame(Frame),
    /// The client has to catch up from the last position it was sent.
    Resync,
}

/// The frames waiting for one client, with one consumer.
pub struct ClientQueue {
    capacity: usize,
    inner: Mutex<Inner>,
    notify: Notify,
}

#[derive(Default)]
struct Inner {
    deltas: VecDeque<Delta>,
    resync: bool,
    meters: Option<Bytes>,
    counters: Option<Utf8Bytes>,
    credits: u64,
}

impl ClientQueue {
    /// `capacity` bounds the deltas waiting.
    pub fn new(capacity: usize) -> ClientQueue {
        assert!(capacity > 0, "a client queue needs room for one delta");
        ClientQueue {
            capacity,
            inner: Mutex::default(),
            notify: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("queue lock poisoned")
    }

    pub fn push(&self, frame: Frame) -> Pushed {
        let mut q = self.lock();
        let pushed = match frame {
            // The pending catch-up reads the state after this delta.
            Frame::Delta(_) if q.resync => return Pushed::Resync,
            Frame::Delta(d) if q.deltas.len() < self.capacity => {
                q.deltas.push_back(d);
                Pushed::Queued
            }
            Frame::Delta(_) => {
                q.deltas.clear();
                q.resync = true;
                Pushed::Resync
            }
            Frame::Meters(_) if q.credits == 0 => return Pushed::Skipped,
            Frame::Meters(m) => replace(&mut q.meters, m),
            Frame::Counters(c) => replace(&mut q.counters, c),
        };
        drop(q);
        self.notify.notify_one();
        pushed
    }

    /// Allows `n` more meter frames to be sent to this client.
    pub fn grant_meter_credits(&self, n: u64) {
        let mut q = self.lock();
        q.credits = q.credits.saturating_add(n);
    }

    /// The meter frames that may still be sent.
    pub fn meter_credits(&self) -> u64 {
        self.lock().credits
    }

    /// A resync first, then deltas in order, then counters, then meters;
    /// each meter frame popped uses one credit.
    pub async fn pop(&self) -> Popped {
        loop {
            {
                let mut q = self.lock();
                if q.resync {
                    q.resync = false;
                    return Popped::Resync;
                }
                if let Some(d) = q.deltas.pop_front() {
                    return Popped::Frame(Frame::Delta(d));
                }
                if let Some(c) = q.counters.take() {
                    return Popped::Frame(Frame::Counters(c));
                }
                if q.credits > 0
                    && let Some(m) = q.meters.take()
                {
                    q.credits -= 1;
                    return Popped::Frame(Frame::Meters(m));
                }
            }
            self.notify.notified().await;
        }
    }

    /// The deltas waiting.
    pub fn len(&self) -> usize {
        self.lock().deltas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn replace<T>(slot: &mut Option<T>, value: T) -> Pushed {
    match slot.replace(value) {
        Some(_) => Pushed::Replaced,
        None => Pushed::Queued,
    }
}

/// Fans frames out to every subscribed client queue.
pub struct Hub {
    capacity: usize,
    clients: Mutex<Vec<Weak<ClientQueue>>>,
}

impl Hub {
    /// `capacity` bounds the deltas waiting for each client.
    pub fn new(capacity: usize) -> Hub {
        Hub {
            capacity,
            clients: Mutex::default(),
        }
    }

    /// The queue stays subscribed until it is dropped. Subscribe before
    /// [`serve_client`] reads the state, so no delta falls between the two.
    pub fn subscribe(&self) -> Arc<ClientQueue> {
        let queue = Arc::new(ClientQueue::new(self.capacity));
        self.clients
            .lock()
            .expect("hub lock poisoned")
            .push(Arc::downgrade(&queue));
        queue
    }

    /// Publishes a frame to every client. The product publishes a delta
    /// after the state has applied it, the ones replicated from other
    /// replicas included, so a client catching up from the state in between
    /// gets it from there and skips the published copy by revision.
    pub fn publish(&self, frame: Frame) {
        self.clients
            .lock()
            .expect("hub lock poisoned")
            .retain(|client| match client.upgrade() {
                Some(queue) => {
                    queue.push(frame.clone());
                    true
                }
                None => false,
            });
    }
}

/// The connections served on this replica and when each last answered a
/// ping.
#[derive(Debug)]
pub struct Sessions {
    replica: String,
    next: AtomicU64,
    live: Mutex<HashMap<String, (Owner, Instant)>>,
}

impl Sessions {
    /// `replica` prefixes the connection ids.
    pub fn new(replica: &str) -> Sessions {
        Sessions {
            replica: replica.to_owned(),
            next: AtomicU64::new(1),
            live: Mutex::default(),
        }
    }

    pub(crate) fn connection_id(&self) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{}:{n}", self.replica)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (Owner, Instant)>> {
        self.live.lock().expect("sessions lock poisoned")
    }

    fn answered(&self, owner: &Owner) {
        let entry = (owner.clone(), Instant::now());
        self.lock().insert(owner.connection.clone(), entry);
    }

    fn remove(&self, owner: &Owner) {
        self.lock().remove(&owner.connection);
    }

    /// The owners whose connection answered within [`PONG_TIMEOUT`].
    pub fn live(&self) -> Vec<Owner> {
        let now = Instant::now();
        let mut live: Vec<Owner> = self
            .lock()
            .values()
            .filter(|(_, at)| now.duration_since(*at) <= PONG_TIMEOUT)
            .map(|(owner, _)| owner.clone())
            .collect();
        live.sort_by(|a, b| a.connection.cmp(&b.connection));
        live
    }
}

/// How [`serve_client`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The client closed the connection or the stream ended.
    Closed,
    /// No pong within [`PONG_TIMEOUT`]; a close frame was sent.
    PongTimeout,
    /// Sending or receiving failed.
    Failed,
}

/// The messages that bring a client from `since` to the state's position,
/// and that position.
async fn catch_up(
    state: &dyn ReplicatedState,
    since: Option<Position>,
) -> (Position, Vec<Utf8Bytes>) {
    if let Some(since) = since
        && since.epoch == state.epoch()
        && let Some(deltas) = state.deltas_since(since).await
    {
        let mut position = since;
        let mut texts = Vec::new();
        for d in deltas.into_iter().filter(|d| d.revision > since.revision) {
            position.revision = d.revision;
            texts.push(d.text);
        }
        return (position, texts);
    }
    let snapshot = state.snapshot().await;
    let position = Position {
        epoch: snapshot.epoch,
        revision: snapshot.revision,
    };
    (position, vec![snapshot.text])
}

/// Serves one client until it closes, fails or stops answering pings:
/// catches it up from `connect.since` (see the module docs), sends what
/// `queue` holds, pings every [`PING_PERIOD`], keeps the connection in
/// `connect.sessions` while it answers, and passes each text and binary
/// message to `on_message`, sending the reply it returns. Subscribe `queue`
/// before calling.
///
/// `on_message` is called for each message in the order received, and the
/// replies go out in that order. The futures it returns run beside the
/// heartbeat and each other, so a reply that waits on a commit or an
/// election does not stall the pings; work that must follow the order of
/// the messages belongs in the call itself, before the future. With
/// [`MAX_PENDING_REPLIES`] replies outstanding the connection is not read,
/// pongs included, until one is ready. A catch-up runs beside the heartbeat
/// as well, with `queue` waiting for it.
pub async fn serve_client<S, E, F, Fut>(
    socket: S,
    connect: &Connect,
    queue: &ClientQueue,
    state: &dyn ReplicatedState,
    mut on_message: F,
) -> Ended
where
    S: Sink<Message> + Stream<Item = Result<Message, E>>,
    F: FnMut(Message) -> Fut,
    Fut: Future<Output = Option<Message>>,
{
    let (mut tx, mut rx) = socket.split();
    let sessions = &connect.sessions;
    let owner = &connect.owner;
    sessions.answered(owner);
    let ended = async {
        let mut answered = Instant::now();
        let mut ping = tokio::time::interval(PING_PERIOD);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut catching_up: Option<BoxFuture<'_, _>> =
            Some(Box::pin(catch_up(state, connect.since)));
        // Set by the first catch-up, before `queue` is read.
        let mut position = Position {
            epoch: 0,
            revision: 0,
        };
        let mut replies = FuturesOrdered::new();
        loop {
            let out: Vec<Message> = tokio::select! {
                _ = ping.tick() => {
                    if answered.elapsed() >= PONG_TIMEOUT {
                        let _ = tx.send(Message::Close(None)).await;
                        return Ended::PongTimeout;
                    }
                    vec![Message::Ping(Bytes::new())]
                }
                received = rx.next(), if replies.len() < MAX_PENDING_REPLIES => match received {
                    None | Some(Ok(Message::Close(_))) => return Ended::Closed,
                    Some(Err(_)) => return Ended::Failed,
                    Some(Ok(Message::Pong(_))) => {
                        answered = Instant::now();
                        sessions.answered(owner);
                        Vec::new()
                    }
                    Some(Ok(Message::Ping(_))) => Vec::new(),
                    Some(Ok(m)) => {
                        replies.push_back(on_message(m));
                        Vec::new()
                    }
                },
                Some(reply) = replies.next(), if !replies.is_empty() => reply.into_iter().collect(),
                (p, texts) = async { catching_up.as_mut().expect("guarded").await },
                    if catching_up.is_some() =>
                {
                    catching_up = None;
                    position = p;
                    texts.into_iter().map(Message::Text).collect()
                }
                popped = queue.pop(), if catching_up.is_none() => match popped {
                    Popped::Frame(Frame::Delta(d))
                        if d.epoch == position.epoch && d.revision <= position.revision =>
                    {
                        Vec::new()
                    }
                    Popped::Frame(Frame::Delta(d))
                        if d.epoch == position.epoch && d.prev == position.revision =>
                    {
                        position.revision = d.revision;
                        vec![Message::Text(d.text)]
                    }
                    Popped::Frame(Frame::Delta(_)) | Popped::Resync => {
                        catching_up = Some(Box::pin(catch_up(state, Some(position))));
                        Vec::new()
                    }
                    Popped::Frame(Frame::Meters(m)) => vec![Message::Binary(m)],
                    Popped::Frame(Frame::Counters(c)) => vec![Message::Text(c)],
                },
            };
            for message in out {
                if tx.send(message).await.is_err() {
                    return Ended::Failed;
                }
            }
        }
    }
    .await;
    sessions.remove(owner);
    ended
}
