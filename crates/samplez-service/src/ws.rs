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

use crate::Error;
use crate::replica::{Owner, Position, ReplicatedState};
use crate::server::Connect;

/// How often [`serve_client`] pings.
pub const PING_PERIOD: Duration = Duration::from_millis(250);
/// [`serve_client`] closes a connection that has answered no ping sent in
/// this long, and only connections that answered one are renewed
/// ([`Sessions::live`]); neither counts the time the connection was not
/// read at [`MAX_PENDING_REPLIES`]. It is counted from when the ping was
/// sent, so the round trip counts against it and a pong that waited unread
/// behind other messages is not taken as fresh.
///
/// With a ping every [`PING_PERIOD`], the pong to the ping sent
/// `PONG_TIMEOUT - PING_PERIOD`, 750 ms, before each ping has to have been
/// read by then: a connection whose round trip, send queues on both ends
/// included, stays at 750 ms or more ends. A longer timeout would keep such
/// connections, and keep a peer that is gone live for longer.
pub const PONG_TIMEOUT: Duration = Duration::from_secs(1);
/// The replies [`serve_client`] waits for on one connection before it stops
/// reading it. Its pongs then go unread as well, so the time until a reply
/// is ready does not count against [`PONG_TIMEOUT`], and the connection
/// stays live in [`Sessions`] meanwhile when it answered within
/// [`PONG_TIMEOUT`] before. A connection not read for [`MAX_PAUSE`] at
/// once, or [`MAX_PAUSE_TOTAL`] since the last ping it answered was sent,
/// ends.
pub const MAX_PENDING_REPLIES: usize = 64;
/// How long [`serve_client`] goes without reading a connection that has
/// [`MAX_PENDING_REPLIES`] replies outstanding before it ends it as
/// [`Ended::Failed`]. Without a bound, replies that never resolve would hold
/// a peer that is gone, and its place in [`crate::replica::Commands`], for
/// as long as they hang.
///
/// 2 s: the replication is sized to commit again within about 1 s of losing
/// its leader (two election rounds of 300 ms, the 50 ms apply lead cap and
/// three pages), so a reply waiting through one failover resolves within
/// half of it. How long a peer that is gone stays live is bounded by
/// [`MAX_PAUSE_TOTAL`].
pub const MAX_PAUSE: Duration = Duration::from_secs(2);
/// How long [`serve_client`] goes without reading a connection in all, over
/// the pauses at [`MAX_PENDING_REPLIES`] since the last ping it answered
/// was sent, before it ends it as [`Ended::Failed`]. A pong waits unread
/// behind the messages sent before it, and each pause is taken off the
/// client's [`PONG_TIMEOUT`], so without this bound a peer that is gone,
/// with messages queued, would stay live for as long as they last.
///
/// Three times [`MAX_PAUSE`]: a client whose pongs wait behind three times
/// [`MAX_PENDING_REPLIES`] messages, each answered within [`MAX_PAUSE`],
/// keeps its connection. A peer that is gone is counted live by
/// [`Sessions::live`] for at most [`PONG_TIMEOUT`] + [`MAX_PAUSE_TOTAL`],
/// 7 s, after the last ping it answered was sent, however long the pong
/// waited unread. Its touch ownership then lasts as long as the
/// [`crate::ReplicatedState`] implementation keeps a session lease without
/// renewal, 2 s in the replication this crate is sized for: up to 9 s after
/// that ping.
pub const MAX_PAUSE_TOTAL: Duration = Duration::from_secs(6);

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

/// The connections served on this replica, when each last answered a ping
/// and whether it is read, as [`serve_client`] counts them.
#[derive(Debug)]
pub struct Sessions {
    replica: String,
    /// Random, so connection ids do not recur after a restart.
    nonce: String,
    next: AtomicU64,
    live: Mutex<HashMap<String, Connection>>,
}

#[derive(Debug)]
struct Connection {
    owner: Owner,
    /// When the last ping it answered was sent, moved on by the pauses
    /// since.
    answered: Instant,
    /// Since when it is not read at [`MAX_PENDING_REPLIES`], and when it
    /// ends unless read again.
    paused: Option<(Instant, Instant)>,
}

impl Sessions {
    /// `replica` prefixes the connection ids, followed by 128 random bits
    /// drawn here.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the operating system's random source fails.
    pub fn new(replica: &str) -> Result<Sessions, Error> {
        let mut nonce = [0u8; 16];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut nonce)
            .map_err(|_| Error::Config("the random source failed".into()))?;
        Ok(Sessions {
            replica: replica.to_owned(),
            nonce: nonce.iter().map(|b| format!("{b:02x}")).collect(),
            next: AtomicU64::new(1),
            live: Mutex::default(),
        })
    }

    /// `<replica id>:<nonce>:<n>`, see [`Owner::connection`].
    pub(crate) fn connection_id(&self) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{}:{}:{n}", self.replica, self.nonce)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Connection>> {
        self.live.lock().expect("sessions lock poisoned")
    }

    /// The connection answered a ping sent at `answered`, moved on by the
    /// pauses since.
    fn answered(&self, owner: &Owner, answered: Instant) {
        let entry = Connection {
            owner: owner.clone(),
            answered,
            paused: None,
        };
        self.lock().insert(owner.connection.clone(), entry);
    }

    /// The connection is not read since `since`, and ends at `end` unless
    /// it is read again.
    fn paused(&self, owner: &Owner, since: Instant, end: Instant) {
        if let Some(connection) = self.lock().get_mut(&owner.connection) {
            connection.paused = Some((since, end));
        }
    }

    /// The connection is read again; `answered` is when the last ping it
    /// answered was sent, moved on by the pauses since.
    fn resumed(&self, owner: &Owner, answered: Instant) {
        if let Some(connection) = self.lock().get_mut(&owner.connection) {
            connection.answered = answered;
            connection.paused = None;
        }
    }

    fn remove(&self, owner: &Owner) {
        self.lock().remove(&owner.connection);
    }

    /// The owners whose connection answered a ping sent within
    /// [`PONG_TIMEOUT`], not counting the time it was not read at
    /// [`MAX_PENDING_REPLIES`], and, while it is not read, until
    /// [`serve_client`] ends it after [`MAX_PAUSE`] or [`MAX_PAUSE_TOTAL`]:
    /// its pongs wait unread, and a client answering them keeps its lease.
    /// A round trip of `PONG_TIMEOUT - PING_PERIOD` or more is not live.
    pub fn live(&self) -> Vec<Owner> {
        let now = Instant::now();
        let mut live: Vec<Owner> = self
            .lock()
            .values()
            .filter(|c| match c.paused {
                None => now.duration_since(c.answered) <= PONG_TIMEOUT,
                Some((since, end)) => since.duration_since(c.answered) <= PONG_TIMEOUT && now < end,
            })
            .map(|c| c.owner.clone())
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
    /// No ping sent within [`PONG_TIMEOUT`] answered, not counting the time
    /// the connection was not read at [`MAX_PENDING_REPLIES`]; a close frame
    /// was sent. A round trip of `PONG_TIMEOUT - PING_PERIOD` or more ends a
    /// connection this way.
    PongTimeout,
    /// Sending or receiving failed, a send did not finish within
    /// [`PONG_TIMEOUT`] of sending the last ping answered, counted the same
    /// way, or the connection was not read at [`MAX_PENDING_REPLIES`] for
    /// [`MAX_PAUSE`] at once or [`MAX_PAUSE_TOTAL`] since that ping was sent.
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

/// When the client has to have answered a ping by: [`PONG_TIMEOUT`] after
/// the last ping it answered was sent, `answered`, not counting the time
/// since `paused`, the server not reading the connection meanwhile.
fn pong_deadline(answered: Instant, paused: Option<Instant>) -> Instant {
    answered + PONG_TIMEOUT + paused.map_or(Duration::ZERO, |since| since.elapsed())
}

/// Sends `message`, giving up at `deadline`, the client's
/// [`pong_deadline`] or, while the connection is not read, the end of the
/// pause if sooner: a client that stopped reading is not waited for.
async fn send<T: Sink<Message> + Unpin>(
    tx: &mut T,
    message: Message,
    deadline: Instant,
) -> Result<(), ()> {
    match tokio::time::timeout_at(deadline, tx.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) | Err(_) => Err(()),
    }
}

/// Serves one client until it closes, fails or stops answering pings:
/// catches it up from `connect.since` (see the module docs), sends what
/// `queue` holds, pings every [`PING_PERIOD`], keeps the connection in
/// `connect.sessions` while it answers, and passes each text and binary
/// message to `on_message`, sending the reply it returns. Subscribe `queue`
/// before calling.
///
/// `on_message` is called for each message in the order received, and the
/// replies go out in that order, the first after the first catch-up. The
/// futures it returns run beside the heartbeat and each other, so a reply
/// that waits on a commit or an election does not stall the pings; work
/// that must follow the order of the messages belongs in the call itself,
/// before the future. With [`MAX_PENDING_REPLIES`] replies outstanding the
/// connection is not read, pongs included, until one is ready, and that
/// time does not count against the client's [`PONG_TIMEOUT`]; when none is
/// ready within [`MAX_PAUSE`], or the pauses since the last ping answered
/// was sent reach [`MAX_PAUSE_TOTAL`], the connection ends. A catch-up
/// runs beside the heartbeat as well, with `queue` waiting for it. Each
/// ping carries an opaque payload; a pong answers it when it echoes the
/// payload unchanged, as RFC 6455 requires, and other pongs are ignored.
///
/// A command is begun and submitted in the call, outside the `async`
/// block, so commands commit in the order the client sent them; submitted
/// inside it, they are ordered as the futures happen to be polled:
///
/// ```
/// # use axum::body::Bytes;
/// # use axum::extract::ws::{Message, WebSocket};
/// # use samplez_service::replica::{Begin, Commands};
/// # use samplez_service::ws::{ClientQueue, Ended, serve_client};
/// # use samplez_service::{Connect, ReplicatedState};
/// # async fn serve(
/// #     socket: WebSocket,
/// #     connect: &Connect,
/// #     queue: &ClientQueue,
/// #     state: &dyn ReplicatedState,
/// #     commands: &Commands,
/// # ) -> Ended {
/// serve_client(socket, connect, queue, state, |message| {
///     // Here, in the order received: the message is the command's id.
///     let submitted = match message {
///         Message::Text(id) if connect.require_operator().is_ok() => {
///             match commands.begin(&connect.owner, id.as_str()) {
///                 Begin::New(pending) => Some(pending.submit(Bytes::from(id.to_string()))),
///                 Begin::Duplicate(_) | Begin::Busy => None,
///             }
///         }
///         _ => None,
///     };
///     // Only the wait for the reply is in the future.
///     async move {
///         let ack = submitted?.await.ok()?;
///         Some(Message::Text(format!("{} {}", ack.command, ack.revision).into()))
///     }
/// })
/// .await
/// # }
/// ```
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
    sessions.answered(owner, Instant::now());
    let ended = async {
        // When the last ping answered was sent, or the connection
        // accepted, moved on by the pauses since.
        let mut answered = Instant::now();
        // The time not read in the pauses since then, the current one not
        // included.
        let mut paused_before = Duration::ZERO;
        // The time not read in all the pauses that ended.
        let mut paused_total = Duration::ZERO;
        // The pings not answered yet: the number each carries, when it was
        // sent, and the time not read until then.
        let mut pings: VecDeque<(u64, Instant, Duration)> = VecDeque::new();
        let mut next_ping: u64 = 0;
        // Since when the connection is not read for the replies at the
        // limit, and when it ends unless read again.
        let mut paused: Option<(Instant, Instant)> = None;
        let mut ping = tokio::time::interval(PING_PERIOD);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut catching_up: Option<BoxFuture<'_, _>> =
            Some(Box::pin(catch_up(state, connect.since)));
        // Set by the first catch-up, before `queue` and replies are read.
        let mut caught_up = false;
        let mut position = Position {
            epoch: 0,
            revision: 0,
        };
        let mut replies = FuturesOrdered::new();
        loop {
            let at_limit = replies.len() >= MAX_PENDING_REPLIES;
            match paused {
                None if at_limit => {
                    let since = Instant::now();
                    let end = since + MAX_PAUSE.min(MAX_PAUSE_TOTAL.saturating_sub(paused_before));
                    paused = Some((since, end));
                    sessions.paused(owner, since, end);
                }
                // Ended even with a reply ready: one ready at the instant
                // the pause ends can win the select over the end, and a
                // send can finish at the end.
                Some((_, end)) if !at_limit && Instant::now() >= end => return Ended::Failed,
                Some((since, _)) if !at_limit => {
                    let pause = since.elapsed();
                    answered += pause;
                    paused_before += pause;
                    paused_total += pause;
                    paused = None;
                    sessions.resumed(owner, answered);
                }
                _ => {}
            }
            let paused_since = paused.map(|(since, _)| since);
            let out: Vec<Message> = tokio::select! {
                _ = tokio::time::sleep_until(paused.map_or_else(Instant::now, |(_, end)| end)),
                    if paused.is_some() =>
                {
                    return Ended::Failed;
                }
                _ = ping.tick() => {
                    let deadline = pong_deadline(answered, paused_since);
                    if Instant::now() >= deadline {
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::PongTimeout;
                    }
                    let unread = paused_since.map_or(Duration::ZERO, |since| since.elapsed());
                    pings.push_back((next_ping, Instant::now(), paused_total + unread));
                    let payload = Bytes::copy_from_slice(&next_ping.to_be_bytes());
                    next_ping += 1;
                    vec![Message::Ping(payload)]
                }
                received = rx.next(), if !at_limit => match received {
                    None | Some(Ok(Message::Close(_))) => return Ended::Closed,
                    Some(Err(_)) => return Ended::Failed,
                    Some(Ok(Message::Pong(payload))) => {
                        // Counted from when its ping was sent, so pongs that
                        // waited unread do not count as fresh; one that
                        // echoes no ping sent and not answered yet does not
                        // count.
                        let echoed = <[u8; 8]>::try_from(&payload[..]).ok().map(u64::from_be_bytes);
                        while pings.front().is_some_and(|p| Some(p.0) < echoed) {
                            pings.pop_front();
                        }
                        if let Some((_, sent, unread)) =
                            pings.pop_front_if(|p| Some(p.0) == echoed)
                        {
                            paused_before = paused_total - unread;
                            answered = sent + paused_before;
                            sessions.answered(owner, answered);
                        }
                        Vec::new()
                    }
                    Some(Ok(Message::Ping(_))) => Vec::new(),
                    Some(Ok(m)) => {
                        replies.push_back(on_message(m));
                        Vec::new()
                    }
                },
                Some(reply) = replies.next(), if caught_up && !replies.is_empty() => {
                    reply.into_iter().collect()
                }
                (p, texts) = async { catching_up.as_mut().expect("guarded").await },
                    if catching_up.is_some() =>
                {
                    catching_up = None;
                    caught_up = true;
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
            // While paused, a send does not outlast the pause.
            let end = paused.map(|(_, end)| end);
            for message in out {
                let deadline = pong_deadline(answered, paused_since);
                let deadline = end.map_or(deadline, |end| deadline.min(end));
                if send(&mut tx, message, deadline).await.is_err() {
                    return Ended::Failed;
                }
            }
        }
    }
    .await;
    sessions.remove(owner);
    ended
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connection stays registered until its serving loop ends, which
    /// can be a while after its last pong when the task is not polled.
    #[tokio::test(start_paused = true)]
    async fn a_connection_is_live_only_within_the_pong_timeout() {
        let sessions = Sessions::new("r1").unwrap();
        let owner = Owner {
            principal: "anonymous".into(),
            session: "s1".into(),
            role: crate::Role::Operator,
            connection: sessions.connection_id(),
        };
        sessions.answered(&owner, Instant::now());
        tokio::time::advance(PONG_TIMEOUT).await;
        assert_eq!(sessions.live(), std::slice::from_ref(&owner));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(sessions.live().is_empty(), "silent for over 1 s");
        sessions.answered(&owner, Instant::now());
        assert_eq!(sessions.live(), [owner]);
    }

    /// Paused within the pong timeout of its last pong, a connection stays
    /// live until the pause ends it; paused later, it is not live.
    #[tokio::test(start_paused = true)]
    async fn a_paused_connection_is_live_if_it_answered_before_the_pause() {
        let sessions = Sessions::new("r1").unwrap();
        let owner = Owner {
            principal: "anonymous".into(),
            session: "s1".into(),
            role: crate::Role::Operator,
            connection: sessions.connection_id(),
        };
        sessions.answered(&owner, Instant::now());
        tokio::time::advance(PONG_TIMEOUT).await;
        let now = Instant::now();
        sessions.paused(&owner, now, now + MAX_PAUSE);
        tokio::time::advance(MAX_PAUSE - Duration::from_millis(1)).await;
        assert_eq!(sessions.live(), std::slice::from_ref(&owner));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(sessions.live().is_empty(), "paused until its end");

        sessions.answered(&owner, Instant::now());
        tokio::time::advance(PONG_TIMEOUT + Duration::from_millis(1)).await;
        let now = Instant::now();
        sessions.paused(&owner, now, now + MAX_PAUSE);
        assert!(sessions.live().is_empty(), "silent for over 1 s before");
    }

    /// Resumed, a connection is live within the pong timeout of the moved
    /// last pong, and paused again within it, until the new pause ends.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_connection_counts_from_its_moved_last_pong() {
        let sessions = Sessions::new("r1").unwrap();
        let owner = Owner {
            principal: "anonymous".into(),
            session: "s1".into(),
            role: crate::Role::Operator,
            connection: sessions.connection_id(),
        };
        sessions.answered(&owner, Instant::now());
        let answered = Instant::now();
        sessions.paused(&owner, answered, answered + MAX_PAUSE);
        tokio::time::advance(Duration::from_millis(1500)).await;
        sessions.resumed(&owner, answered + Duration::from_millis(1500));
        tokio::time::advance(PONG_TIMEOUT).await;
        assert_eq!(sessions.live(), std::slice::from_ref(&owner));
        let now = Instant::now();
        sessions.paused(&owner, now, now + Duration::from_millis(500));
        tokio::time::advance(Duration::from_millis(499)).await;
        assert_eq!(sessions.live(), std::slice::from_ref(&owner));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(sessions.live().is_empty(), "the pause ended");

        sessions.resumed(&owner, answered + Duration::from_millis(2000));
        assert_eq!(
            sessions.live(),
            std::slice::from_ref(&owner),
            "both taken off"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(sessions.live().is_empty(), "over 1 s since the moved pong");
    }

    /// The connection id is the session of a client that names none, and
    /// the per-session tables of applied command ids outlive a restart.
    #[test]
    fn connection_ids_do_not_recur_after_a_restart() {
        let before = Sessions::new("r1").unwrap().connection_id();
        let after = Sessions::new("r1").unwrap().connection_id();
        assert_ne!(before, after);
        let (replica, rest) = before.split_once(':').unwrap();
        let (nonce, n) = rest.split_once(':').unwrap();
        assert_eq!((replica, n), ("r1", "1"));
        assert_eq!(nonce.len(), 32);
        assert!(nonce.bytes().all(|b| b.is_ascii_hexdigit()));
    }
}
