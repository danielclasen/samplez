//! Per-client WebSocket send queues, fan-out, the session registry and the
//! loop that serves one client.
//!
//! The product's protocol carries the client's position: a client that
//! asks to catch up with the last position it saw ([`CatchUp`]) gets the
//! deltas after it, from whichever replica it reaches, once that replica
//! has applied what the replication committed (the read index), when it
//! holds them and the epoch is the same, and a snapshot otherwise; a client
//! without one gets a snapshot. Live deltas follow in order. A delta whose
//! `prev` is not the revision last sent to the client makes the client
//! catch up from there; so does a full delta queue. A replica whose epoch
//! changes closes the connection. Meters and counters keep only their
//! latest frame; a meter frame is sent only against a credit the product
//! grants for the client. The product's own messages go to every client, to
//! the connections of one session or to one connection ([`Hub::send`]):
//! status messages keep only their latest frame per [`Slot`], and other text
//! frames go out in push order with the deltas, discarded with the deltas a
//! catch-up replaces. A text frame that would overflow the queue closes the
//! connection. While the client is ahead, text frames are discarded and
//! status frames held.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

use crate::Error;
use crate::metrics::{Counter, Metrics};
use crate::replica::{Owner, Position, ReadIndex, ReplicatedState};
use crate::server::Connect;

/// How often [`serve_client`] pings, from the client's first [`CatchUp`]
/// on.
pub const PING_PERIOD: Duration = Duration::from_millis(250);
/// The close code [`serve_client`] sends when the connection's credentials
/// end ([`Ended::SessionEnded`]): 1008, policy violation (RFC 6455 section
/// 7.4.1).
pub const SESSION_ENDED_CLOSE: u16 = 1008;
/// [`serve_client`] closes a connection that has answered no ping sent in
/// this long, and only connections that answered one are renewed
/// ([`Sessions::live`]); neither counts the time the connection was not
/// read ([`serve_client`]). It is counted from when the ping was
/// sent, so the round trip counts against it and a pong that waited unread
/// behind other messages is not taken as fresh.
///
/// With a ping every [`PING_PERIOD`], the pong to the ping sent
/// `PONG_TIMEOUT - PING_PERIOD`, 750 ms, before each ping has to have been
/// read by then: a connection whose round trip, send queues on both ends
/// included, stays at 750 ms or more ends. A longer timeout would keep such
/// connections, and keep a peer that is gone live for longer.
pub const PONG_TIMEOUT: Duration = Duration::from_secs(1);
/// The [`Reply::Command`] replies [`serve_client`] waits for on one
/// connection before it stops reading it; a [`Reply::Now`] counts against
/// nothing. Its pongs then go unread as well, so the time until a reply
/// is ready does not count against [`PONG_TIMEOUT`], and the connection
/// stays live in [`Sessions`] meanwhile when it answered a ping sent
/// within [`PONG_TIMEOUT`] before. A connection not read for [`MAX_PAUSE`] at
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
/// the pauses since the last ping it answered was sent, at
/// [`MAX_PENDING_REPLIES`] or holding the answer to a [`CatchUp`] or the
/// commands after a promote, before it ends it as [`Ended::Failed`]. A pong waits unread
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
/// The longest [`serve_client`] holds the answer to a [`CatchUp`] naming a
/// position of the state's epoch for the read index and this replica's
/// applying it ([`ReplicatedState::read_index`]) before it answers with
/// the state it has applied, marked stale. The connection is not read
/// meanwhile, for at most this plus [`MAX_PAUSE`].
pub const READ_INDEX_HOLD: Duration = Duration::from_secs(2);

/// The longest replica id [`Sessions::new`] takes, a DNS label's length:
/// with the nonce and the counter the connection id stays within
/// [`MAX_CONNECTION_ID`]. It takes a DNS label's characters as well, so
/// the id never holds the `:` that separates it from the nonce.
pub const MAX_REPLICA_ID: usize = 63;
/// The longest connection id ([`crate::Owner::connection`]): a
/// [`MAX_REPLICA_ID`] replica id, 32 hexadecimal digits and a decimal
/// `u64`, with their two separators, take 117.
pub const MAX_CONNECTION_ID: usize = 128;

/// What [`serve_client`]'s `on_message` returns for a message.
pub enum Reply<Fut> {
    /// The reply to a command, or to another message whose answer has to
    /// follow the commands received before it: counted against
    /// [`MAX_PENDING_REPLIES`] until it resolves, and sent in the order
    /// received among the others.
    Command(Fut),
    /// Sent at once, ahead of replies still waiting and before the first
    /// catch-up, and counted against nothing; `None` sends nothing. For
    /// messages that carry no state, such as the answer to a client's
    /// heartbeat, which must not wait on a commit or a snapshot; never for
    /// the answer to a command, such as its refusal, which would overtake
    /// the answers to the commands before it. While the connection is not
    /// read a message answered with `Now` waits as well.
    Now(Option<Message>),
    /// The client asks to catch up, as with the first message of a
    /// connection or after a gap: answered through [`Encode::answer`], and
    /// nothing after it is read until the answer is sent.
    CatchUp(CatchUp),
    /// The client makes this connection the one it sends commands on, with
    /// the last position it saw: when that is of the state's epoch, nothing
    /// after it is read until this replica has applied it, so no command
    /// the client sends next applies to a state older than the one it saw,
    /// or until the read index is confirmed below it, so the catch-up the
    /// client sends next is answered [`Answer::Ahead`]. Sends nothing.
    Promote(Option<Position>),
    /// The client's answer to the ping [`Encode::ping`] made with `t`.
    /// Sends nothing.
    Pong(u64),
}

/// Whether the product may submit a command [`serve_client`] passes it now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Submit,
    /// The client promoted this connection naming a position the
    /// replication never committed, and has not taken state since: a
    /// command now would apply to state older than the one the client saw.
    /// Answer it not applied, at once and without submitting it, so the
    /// client sends it again once it has taken the cluster's state.
    NotApplied,
}

/// A client's request to catch up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatchUp {
    /// The last position the client saw, `None` for a snapshot.
    pub since: Option<Position>,
    /// A snapshot even when this replica holds the deltas after `since`.
    pub full: bool,
}

/// What brings a client up to date, for the product to encode
/// ([`Encode::answer`]).
#[derive(Debug, Clone)]
pub enum Answer {
    /// The deltas after the client's position, in order, up to `revision`
    /// in `epoch`: the revision of the last of them, or the client's own
    /// when there are none.
    Resume {
        epoch: u64,
        revision: u64,
        deltas: Vec<Delta>,
        /// Not confirmed to be the latest committed state: the replica
        /// could not get the read index, or did not apply it within
        /// [`READ_INDEX_HOLD`]. Its revision may then be older than the
        /// client's.
        stale: bool,
    },
    Snapshot {
        snapshot: Snapshot,
        /// As for [`Answer::Resume`].
        stale: bool,
    },
    /// The client's position is above the read index in the state's epoch:
    /// it holds state the replication never committed. The connection is
    /// sent no deltas and no [`Frame::Text`] until a later [`CatchUp`] is
    /// answered with state, and its [`Frame::Status`] frames are held until
    /// then.
    Ahead {
        epoch: u64,
        /// The read index.
        committed: u64,
        /// The revision of the client's position.
        last_revision: u64,
    },
}

impl Answer {
    pub fn epoch(&self) -> u64 {
        match self {
            Answer::Resume { epoch, .. } | Answer::Ahead { epoch, .. } => *epoch,
            Answer::Snapshot { snapshot, .. } => snapshot.epoch,
        }
    }

    /// The position the client is at once it took the answer.
    fn position(&self) -> Option<Position> {
        match self {
            Answer::Resume {
                epoch, revision, ..
            } => Some(Position {
                epoch: *epoch,
                revision: *revision,
            }),
            Answer::Snapshot { snapshot, .. } => Some(Position {
                epoch: snapshot.epoch,
                revision: snapshot.revision,
            }),
            Answer::Ahead { .. } => None,
        }
    }
}

/// The messages of the product's protocol that [`serve_client`] sends on
/// its own.
pub trait Encode {
    /// A heartbeat carrying `t`, the milliseconds since the connection was
    /// accepted, unique among the pings of the connection; the client's
    /// answer to it is a [`Reply::Pong`] with `t`.
    fn ping(&mut self, t: u64) -> Message;

    /// The answer to a [`CatchUp`], or the catch-up after a gap or a full
    /// delta queue.
    fn answer(&mut self, answer: Answer) -> Vec<Message>;
}

/// A state change at a global revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub epoch: u64,
    pub revision: u64,
    /// The revision of the delta before this one in the same epoch, 0 for
    /// the first.
    pub prev: u64,
    /// The ids of the commands whose changes it carries, in the order
    /// applied; empty for a change no command made, such as a lease
    /// expiry. A command that changed nothing is in no delta
    /// ([`crate::replica::Resolution::Unchanged`]).
    pub commands: Vec<String>,
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
    /// A product status message, sent as a text message: replaces the
    /// frame of the same slot not sent yet, and goes out at most once per
    /// [`Slot::every`]. Held while the client is above the read index
    /// ([`Answer::Ahead`]).
    Status(Slot, Utf8Bytes),
    /// A product message sent as a text message, in push order with the
    /// deltas. One that would overflow the queue ends the connection
    /// ([`Ended::Overflow`]). Discarded with the deltas a catch-up replaces,
    /// and while the client is above the read index: the client is then
    /// brought up to date from the state, never from these messages, so
    /// whatever has to survive belongs in the replicated state.
    Text(Utf8Bytes),
}

/// A kind of [`Frame::Status`]: each queue keeps one frame per name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub name: &'static str,
    /// The least time between two frames of the slot sent to one client,
    /// counted from when the queue hands the first out; `ZERO` for none.
    pub every: Duration,
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
    /// A text frame while the queue's capacity of them wait: the queue is
    /// emptied and the connection ends ([`Popped::Overflow`]).
    Dropped,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Popped {
    Frame(Frame),
    /// The client has to catch up from the last position it was sent.
    Resync,
    /// A text frame was dropped: the connection ends, and the client
    /// catches up on a new one.
    Overflow,
}

/// The frames waiting for one client, with one consumer.
pub struct ClientQueue {
    capacity: usize,
    inner: Mutex<Inner>,
    notify: Notify,
    primary: AtomicBool,
}

#[derive(Default)]
struct Inner {
    /// Deltas and text frames, in the order pushed.
    sequence: VecDeque<Frame>,
    deltas: usize,
    texts: usize,
    resync: bool,
    overflow: bool,
    meters: Option<Bytes>,
    counters: Option<Utf8Bytes>,
    /// One entry per slot pushed, kept after its frame is sent for when
    /// that was.
    statuses: Vec<StatusSlot>,
    credits: u64,
}

struct StatusSlot {
    slot: Slot,
    text: Option<Utf8Bytes>,
    sent: Option<Instant>,
}

impl ClientQueue {
    /// `capacity` bounds the deltas waiting, and apart from them the text
    /// frames.
    pub fn new(capacity: usize) -> ClientQueue {
        assert!(capacity > 0, "a client queue needs room for one delta");
        ClientQueue {
            capacity,
            inner: Mutex::default(),
            notify: Notify::new(),
            primary: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("queue lock poisoned")
    }

    pub fn push(&self, frame: Frame) -> Pushed {
        let mut q = self.lock();
        let pushed = match frame {
            _ if q.overflow => return Pushed::Dropped,
            // The pending catch-up reads the state after this delta.
            Frame::Delta(_) if q.resync => return Pushed::Resync,
            Frame::Delta(d) if q.deltas < self.capacity => {
                q.sequence.push_back(Frame::Delta(d));
                q.deltas += 1;
                Pushed::Queued
            }
            // The catch-up carries the state the texts went with.
            Frame::Delta(_) => {
                q.sequence.clear();
                (q.deltas, q.texts) = (0, 0);
                q.resync = true;
                Pushed::Resync
            }
            Frame::Meters(_) if q.credits == 0 => return Pushed::Skipped,
            Frame::Meters(m) => replace(&mut q.meters, m),
            Frame::Counters(c) => replace(&mut q.counters, c),
            Frame::Text(_) if q.texts >= self.capacity => {
                q.sequence.clear();
                (q.deltas, q.texts) = (0, 0);
                q.overflow = true;
                Pushed::Dropped
            }
            Frame::Text(t) => {
                q.sequence.push_back(Frame::Text(t));
                q.texts += 1;
                Pushed::Queued
            }
            Frame::Status(slot, text) => {
                match q.statuses.iter_mut().find(|s| s.slot.name == slot.name) {
                    Some(s) => {
                        s.slot = slot;
                        replace(&mut s.text, text)
                    }
                    None => {
                        q.statuses.push(StatusSlot {
                            slot,
                            text: Some(text),
                            sent: None,
                        });
                        Pushed::Queued
                    }
                }
            }
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

    /// Marks whether the client sends its commands on this connection, as
    /// the product's protocol tells it, such as on its hello or a promote;
    /// [`To::Primary`] reads it. The service never sets it, and it is per
    /// connection, not exclusive: two connections of a session can both be
    /// marked, as the old one of a failover until it ends. `false` until
    /// set.
    pub fn set_primary(&self, primary: bool) {
        self.primary.store(primary, Ordering::Relaxed);
    }

    /// What [`ClientQueue::set_primary`] last set, `false` before.
    pub fn is_primary(&self) -> bool {
        self.primary.load(Ordering::Relaxed)
    }

    /// An overflow first, then a resync, then deltas and text frames in the
    /// order pushed, then counters, then status frames whose slot allows
    /// one now, then meters; each meter frame popped uses one credit. A
    /// text frame is therefore never popped after a delta pushed after it,
    /// nor ahead of one pushed before it, and none pushed before a resync
    /// is popped after it.
    pub async fn pop(&self) -> Popped {
        self.pop_held(false).await
    }

    /// [`ClientQueue::pop`], holding status frames when `hold_statuses`.
    async fn pop_held(&self, hold_statuses: bool) -> Popped {
        loop {
            let wake = {
                let mut q = self.lock();
                if q.overflow {
                    return Popped::Overflow;
                }
                if q.resync {
                    q.resync = false;
                    return Popped::Resync;
                }
                if let Some(frame) = q.sequence.pop_front() {
                    match frame {
                        Frame::Delta(_) => q.deltas -= 1,
                        _ => q.texts -= 1,
                    }
                    return Popped::Frame(frame);
                }
                if let Some(c) = q.counters.take() {
                    return Popped::Frame(Frame::Counters(c));
                }
                let now = Instant::now();
                let mut wake: Option<Instant> = None;
                let due = q
                    .statuses
                    .iter_mut()
                    .filter(|s| s.text.is_some() && !hold_statuses);
                for s in due {
                    match s.sent.map(|sent| sent + s.slot.every) {
                        Some(next) if now < next => {
                            wake = Some(wake.map_or(next, |w| w.min(next)));
                        }
                        _ => {
                            s.sent = Some(now);
                            let text = s.text.take().expect("filtered");
                            return Popped::Frame(Frame::Status(s.slot, text));
                        }
                    }
                }
                if q.credits > 0
                    && let Some(m) = q.meters.take()
                {
                    q.credits -= 1;
                    return Popped::Frame(Frame::Meters(m));
                }
                wake
            };
            match wake {
                Some(at) => {
                    tokio::select! {
                        () = self.notify.notified() => {}
                        () = tokio::time::sleep_until(at) => {}
                    }
                }
                None => self.notify.notified().await,
            }
        }
    }

    /// The deltas waiting.
    pub fn len(&self) -> usize {
        self.lock().deltas
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

/// Fans frames out to the subscribed client queues.
pub struct Hub {
    capacity: usize,
    clients: Mutex<Vec<(Owner, Weak<ClientQueue>)>>,
    overflows: Counter,
}

/// The connections [`Hub::send`] pushes a frame to. Each names its
/// principal, so a frame never reaches another principal's connection
/// whatever session or connection id a client presents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum To<'a> {
    /// Every connection of a session: the same session id under another
    /// principal is another session ([`Owner::session`]).
    Session {
        principal: &'a str,
        session: &'a str,
    },
    /// The connections of a session marked primary
    /// ([`ClientQueue::set_primary`]).
    Primary {
        principal: &'a str,
        session: &'a str,
    },
    /// One connection, by [`Owner::connection`].
    Connection {
        principal: &'a str,
        connection: &'a str,
    },
}

impl To<'_> {
    fn matches(&self, owner: &Owner, queue: &ClientQueue) -> bool {
        match *self {
            To::Session { principal, session } => {
                owner.principal == principal && owner.session == session
            }
            To::Primary { principal, session } => {
                owner.principal == principal && owner.session == session && queue.is_primary()
            }
            To::Connection {
                principal,
                connection,
            } => owner.principal == principal && owner.connection == connection,
        }
    }
}

/// What [`Hub::send`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sent {
    /// The connections that queued the frame or replaced one of its kind
    /// with it.
    pub reached: usize,
    /// The connections a text frame overflowed, which end
    /// ([`Pushed::Dropped`]).
    pub dropped: usize,
}

impl Hub {
    /// `capacity` bounds the deltas waiting for each client, and apart from
    /// them its text frames. Counts the text frames that overflowed a queue
    /// in `samplez_ws_text_overflows_total` of `metrics`.
    pub fn new(capacity: usize, metrics: &Metrics) -> Hub {
        Hub {
            capacity,
            clients: Mutex::default(),
            overflows: metrics.counter(
                "samplez_ws_text_overflows_total",
                "Product text frames that overflowed a WebSocket client's queue, \
                 ending the connection.",
                &[],
            ),
        }
    }

    /// The queue of `owner`'s connection, subscribed until it is dropped.
    /// Subscribe before [`serve_client`] reads the state, so no delta falls
    /// between the two.
    pub fn subscribe(&self, owner: &Owner) -> Arc<ClientQueue> {
        let queue = Arc::new(ClientQueue::new(self.capacity));
        self.clients
            .lock()
            .expect("hub lock poisoned")
            .push((owner.clone(), Arc::downgrade(&queue)));
        queue
    }

    /// Publishes a frame to every client. The product publishes a delta
    /// after the state has applied it, the ones replicated from other
    /// replicas included, so a client catching up from the state in between
    /// gets it from there and skips the published copy by revision.
    pub fn publish(&self, frame: Frame) -> Sent {
        self.push(|_, _| true, &frame)
    }

    /// Pushes a frame to the connections `to` names, as [`Hub::publish`]
    /// does to every one.
    ///
    /// Each queue holds deltas and text frames in the order pushed, and the
    /// hub pushes to every queue under one lock: a text frame sent after
    /// [`Hub::publish`] of a delta returned reaches each client after that
    /// delta, and one sent before it ahead of it. A product that derives a
    /// text frame from a state change sends it after publishing the delta
    /// of that change. A text frame is not delivered when the client
    /// catches up instead, is above the read index, or its queue overflows,
    /// which ends the connection; what has to reach the client belongs in
    /// the replicated state.
    pub fn send(&self, to: To<'_>, frame: Frame) -> Sent {
        self.push(|owner, queue| to.matches(owner, queue), &frame)
    }

    fn push(&self, to: impl Fn(&Owner, &ClientQueue) -> bool, frame: &Frame) -> Sent {
        let mut sent = Sent::default();
        self.clients
            .lock()
            .expect("hub lock poisoned")
            .retain(|(owner, client)| match client.upgrade() {
                Some(queue) => {
                    if to(owner, &queue) {
                        match queue.push(frame.clone()) {
                            Pushed::Queued | Pushed::Replaced => sent.reached += 1,
                            Pushed::Dropped => sent.dropped += 1,
                            Pushed::Skipped | Pushed::Resync => {}
                        }
                    }
                    true
                }
                None => false,
            });
        if matches!(frame, Frame::Text(_)) {
            self.overflows.add(sent.dropped as u64);
        }
        sent
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
    /// Since when it is not read ([`serve_client`]), and when it ends
    /// unless read again.
    paused: Option<(Instant, Instant)>,
}

impl Sessions {
    /// `replica` prefixes the connection ids, followed by 128 random bits
    /// drawn here.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when `replica` is empty, longer than
    /// [`MAX_REPLICA_ID`], holds anything but lowercase ASCII letters,
    /// digits and `-` or starts or ends with `-`, or when the operating
    /// system's random source fails.
    pub fn new(replica: &str) -> Result<Sessions, Error> {
        if replica.is_empty() || replica.len() > MAX_REPLICA_ID {
            return Err(Error::Config(format!(
                "the replica id has to be 1 to {MAX_REPLICA_ID} bytes long, not {}",
                replica.len()
            )));
        }
        if !replica
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || replica.starts_with('-')
            || replica.ends_with('-')
        {
            return Err(Error::Config(format!(
                "the replica id {replica:?} has to be lowercase letters, digits and '-', \
                 starting and ending with a letter or digit"
            )));
        }
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
    /// [`PONG_TIMEOUT`], not counting the time it was not read, and, while
    /// it is not read, until [`serve_client`] ends the pause:
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
    /// the connection was not read, or no [`CatchUp`] read within it of the
    /// connection being accepted; a close frame was sent. A round trip of
    /// `PONG_TIMEOUT - PING_PERIOD` or more ends a connection this way.
    PongTimeout,
    /// Sending or receiving failed, a send did not finish within
    /// [`PONG_TIMEOUT`] of sending the last ping answered, counted the same
    /// way, or the connection was not read for longer than a pause allows
    /// ([`serve_client`]).
    Failed,
    /// The state's epoch is no longer the one the client was answered in:
    /// the state was initialised anew or restored, and the client's
    /// revisions name nothing in it. A close frame was sent; the client
    /// catches up on a new connection, with a snapshot of the new epoch.
    EpochChanged,
    /// A text frame overflowed the client's queue ([`Pushed::Dropped`]); a
    /// close frame was sent. The client catches up on a new connection.
    Overflow,
    /// The credentials the connection was opened with ended
    /// ([`crate::SessionEnd`]): a close frame with
    /// [`SESSION_ENDED_CLOSE`] was sent. A new connection needs new ones.
    SessionEnded,
}

/// Answers a client's [`CatchUp`]. A position of the state's epoch waits up
/// to [`READ_INDEX_HOLD`] for the read index and for this replica to apply
/// it, so the answer is never older than what the client saw or what was
/// committed before; one above the read index is [`Answer::Ahead`]. A
/// replica that confirms no read index ([`ReadIndex::NotLeader`]) waits as
/// long for applying the client's position. Without either in that time,
/// or at once when the replica is [`ReadIndex::CutOff`], the answer is the
/// state applied here, stale. Another epoch's position, or none, is
/// answered at once.
async fn answer(state: &dyn ReplicatedState, request: CatchUp) -> Answer {
    let mut stale = false;
    if let Some(since) = request.since
        && since.epoch == state.epoch()
    {
        let confirmed = tokio::time::timeout(READ_INDEX_HOLD, async {
            let wait_for = match state.read_index().await {
                ReadIndex::Confirmed(committed) if since.revision > committed => {
                    return Some(Err(committed));
                }
                ReadIndex::Confirmed(committed) => committed,
                ReadIndex::NotLeader => since.revision,
                ReadIndex::CutOff => return None,
            };
            if state.revision() < wait_for {
                state.applied(wait_for).await;
            }
            Some(Ok(()))
        })
        .await;
        match confirmed {
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(committed))) => {
                return Answer::Ahead {
                    epoch: since.epoch,
                    committed,
                    last_revision: since.revision,
                };
            }
            Ok(None) | Err(_) => stale = true,
        }
    }
    catch_up(state, request, stale).await
}

/// Resolves `true` once this replica has applied `revision`, the position
/// of a promote, or `false` once the read index is confirmed below it: the
/// client holds state the replication never committed, and the catch-up it
/// sends next is answered [`Answer::Ahead`] rather than held.
async fn promote_floor(state: &dyn ReplicatedState, revision: u64) -> bool {
    let below = async {
        match state.read_index().await {
            ReadIndex::Confirmed(committed) if committed < revision => {}
            _ => std::future::pending().await,
        }
    };
    tokio::select! {
        () = state.applied(revision) => true,
        () = below => false,
    }
}

/// The deltas after `request.since` when this replica holds them, has
/// applied that position and the epoch is the same, and a snapshot
/// otherwise.
async fn catch_up(state: &dyn ReplicatedState, request: CatchUp, stale: bool) -> Answer {
    if let Some(since) = request.since
        && !request.full
        && since.epoch == state.epoch()
        && since.revision <= state.revision()
        && let Some(deltas) = state.deltas_since(since).await
    {
        let deltas: Vec<Delta> = deltas
            .into_iter()
            .filter(|d| d.revision > since.revision)
            .collect();
        return Answer::Resume {
            epoch: since.epoch,
            revision: deltas.last().map_or(since.revision, |d| d.revision),
            deltas,
            stale,
        };
    }
    Answer::Snapshot {
        snapshot: state.snapshot().await,
        stale,
    }
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

/// Serves one client until it closes, fails, stops answering pings or the
/// state's epoch changes: sends a ping made by `encode` every
/// [`PING_PERIOD`] from the client's first [`CatchUp`] on, its hello in the
/// product's protocol, keeps the connection in `connect.sessions` while it
/// answers, passes each text and binary message to `on_message` and acts on
/// what it returns, answers each [`CatchUp`] (see the module docs) and then
/// sends what `queue` holds. Subscribe `queue` before calling.
///
/// The first [`CatchUp`] has to be read within [`PONG_TIMEOUT`] of the
/// connection being accepted, and counts as the answer to a ping sent when
/// it is read; the first ping goes out with it.
///
/// `on_message` is called for each message in the order received, with
/// whether the product may submit a command now ([`Admission`]). The
/// [`Reply::Command`] replies go out in that order, the first after the
/// first answer to a [`CatchUp`], and none while a catch-up after a gap or
/// a full delta queue is under way; a [`Reply::Now`] goes out at once. The
/// futures run beside the heartbeat and each other, so a reply that waits
/// on a commit or an election does not stall the pings; work that must
/// follow the order of the messages belongs in the call itself, before the
/// future. A [`Reply::Pong`] answers the ping made with its `t`, counted
/// from when that ping was sent; a `t` of no ping outstanding does not
/// count. WebSocket protocol pings and pongs are not heartbeats here.
///
/// The connection is not read, pongs included, while
/// [`MAX_PENDING_REPLIES`] [`Reply::Command`] replies wait, until one is
/// ready; while the answer to a [`CatchUp`] is held, until it is sent; and
/// after a [`Reply::Promote`] of the state's epoch, until this replica has
/// applied its position or the read index is confirmed below it. That time does not count against the client's
/// [`PONG_TIMEOUT`]. A pause ends the connection as [`Ended::Failed`] when
/// it lasts longer than [`MAX_PAUSE`] at the limit, or than
/// [`READ_INDEX_HOLD`] plus [`MAX_PAUSE`] otherwise, or when the pauses
/// since the last ping answered was sent reach [`MAX_PAUSE_TOTAL`].
///
/// Once the client was answered in an epoch, the connection is closed as
/// [`Ended::EpochChanged`] when the state's epoch, a delta's or a later
/// answer's is another, checked before each reply and at least every
/// [`PING_PERIOD`]. A client
/// answered [`Answer::Ahead`] is sent no deltas and no text frames, and its
/// status frames are held, until it is answered with state again. A text
/// frame that overflows `queue` closes the connection as [`Ended::Overflow`].
///
/// A command is begun and submitted in the call, outside the `async`
/// block, so commands commit in the order the client sent them; submitted
/// inside it, they are ordered as the futures happen to be polled:
///
/// ```
/// # use axum::body::Bytes;
/// # use axum::extract::ws::{Message, WebSocket};
/// # use samplez_service::replica::{Begin, Commands};
/// # use futures_util::future::BoxFuture;
/// # use samplez_service::ws::{
/// #     Admission, Answer, CatchUp, ClientQueue, Encode, Ended, Reply, serve_client,
/// # };
/// # use samplez_service::{Connect, ReplicatedState};
/// struct Text;
///
/// impl Encode for Text {
///     fn ping(&mut self, t: u64) -> Message {
///         Message::Text(format!("ping {t}").into())
///     }
///
///     fn answer(&mut self, answer: Answer) -> Vec<Message> {
///         match answer {
///             Answer::Snapshot { snapshot, .. } => vec![Message::Text(snapshot.text)],
///             Answer::Resume { deltas, .. } => {
///                 deltas.into_iter().map(|d| Message::Text(d.text)).collect()
///             }
///             Answer::Ahead { .. } => vec![Message::Text("ahead".into())],
///         }
///     }
/// }
///
/// # async fn serve(
/// #     socket: WebSocket,
/// #     connect: &Connect,
/// #     queue: &ClientQueue,
/// #     state: &dyn ReplicatedState,
/// #     commands: &Commands,
/// # ) -> Ended {
/// serve_client(socket, connect, queue, state, Text, |message, admission| {
///     // Here, in the order received: `hello` asks for a snapshot,
///     // `pong <t>` answers a ping, and other text is a command's id.
///     let submitted = match message {
///         Message::Text(t) if t.as_str() == "hello" => {
///             let request = CatchUp { since: None, full: false };
///             return Reply::CatchUp(request);
///         }
///         Message::Text(t) if t.starts_with("pong ") => {
///             return t.as_str()[5..].parse().map_or(Reply::Now(None), Reply::Pong);
///         }
///         Message::Text(id) if admission == Admission::NotApplied => {
///             let refused = format!("{id} not applied");
///             let reply = async move { Some(Message::Text(refused.into())) };
///             return Reply::Command(Box::pin(reply) as BoxFuture<'_, _>);
///         }
///         Message::Text(id) if connect.require_operator().is_ok() => {
///             match commands.begin(&connect.owner, id.as_str()) {
///                 Begin::New(pending) => Some(pending.submit(Bytes::from(id.to_string()))),
///                 Begin::Duplicate(_) | Begin::Busy => None,
///             }
///         }
///         _ => None,
///     };
///     // Only the wait for the reply is in the future.
///     Reply::Command(Box::pin(async move {
///         let ack = submitted?.await.ok()?;
///         Some(Message::Text(format!("{} {}", ack.command, ack.revision).into()))
///     }) as BoxFuture<'_, _>)
/// })
/// .await
/// # }
/// ```
pub async fn serve_client<S, E, P, F, Fut>(
    socket: S,
    connect: &Connect,
    queue: &ClientQueue,
    state: &dyn ReplicatedState,
    mut encode: P,
    mut on_message: F,
) -> Ended
where
    S: Sink<Message> + Stream<Item = Result<Message, E>>,
    P: Encode,
    F: FnMut(Message, Admission) -> Reply<Fut>,
    Fut: Future<Output = Option<Message>>,
{
    let (mut tx, mut rx) = socket.split();
    let sessions = &connect.sessions;
    let owner = &connect.owner;
    sessions.answered(owner, Instant::now());
    let session_ended = connect.ends.ended();
    tokio::pin!(session_ended);
    let ended = async {
        let accepted = Instant::now();
        // When the last ping answered was sent, or the connection
        // accepted, moved on by the pauses since.
        let mut answered = accepted;
        // The time not read in the pauses since then, the current one not
        // included.
        let mut paused_before = Duration::ZERO;
        // The time not read in all the pauses that ended.
        let mut paused_total = Duration::ZERO;
        // The pings not answered yet: the `t` each carries, when it was
        // sent, and the time not read until then.
        let mut pings: VecDeque<(u64, Instant, Duration)> = VecDeque::new();
        // Set by the first catch-up request, before which nothing pings.
        let mut pinging = false;
        // Since when the connection is not read, and when it ends unless
        // read again.
        let mut paused: Option<(Instant, Instant)> = None;
        let mut ping = tokio::time::interval(PING_PERIOD);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // A catch-up being answered, and whether the client asked for it,
        // which holds what it sends next.
        let mut catching_up: Option<(BoxFuture<'_, Answer>, bool)> = None;
        // Set by the first answer, before `queue` and replies are read.
        let mut caught_up = false;
        // The epoch of the answers, once the client was answered.
        let mut epoch: Option<u64> = None;
        // The position last sent; `None` while the client holds a position
        // the replication never committed.
        let mut position: Option<Position> = None;
        // This replica applying the position of a promote.
        let mut floor: Option<BoxFuture<'_, bool>> = None;
        // Set when a promote's position turned out above the read index,
        // until a catch-up is answered with state.
        let mut admission = Admission::Submit;
        let mut replies = FuturesOrdered::new();
        loop {
            let at_limit = replies.len() >= MAX_PENDING_REPLIES;
            let held = matches!(catching_up, Some((_, true))) || floor.is_some();
            match paused {
                None if at_limit || held => {
                    let since = Instant::now();
                    let longest = if at_limit {
                        MAX_PAUSE
                    } else {
                        READ_INDEX_HOLD + MAX_PAUSE
                    };
                    let end = since + longest.min(MAX_PAUSE_TOTAL.saturating_sub(paused_before));
                    paused = Some((since, end));
                    sessions.paused(owner, since, end);
                }
                // Ended even with a reply ready: one ready at the instant
                // the pause ends can win the select over the end, and a
                // send can finish at the end.
                Some((_, end)) if !at_limit && !held && Instant::now() >= end => {
                    return Ended::Failed;
                }
                Some((since, _)) if !at_limit && !held => {
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
                () = &mut session_ended => {
                    let deadline = pong_deadline(answered, paused_since);
                    let close = CloseFrame {
                        code: SESSION_ENDED_CLOSE,
                        reason: "session ended".into(),
                    };
                    let _ = send(&mut tx, Message::Close(Some(close)), deadline).await;
                    return Ended::SessionEnded;
                }
                _ = ping.tick() => {
                    let deadline = pong_deadline(answered, paused_since);
                    if Instant::now() >= deadline {
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::PongTimeout;
                    }
                    if epoch.is_some_and(|e| e != state.epoch()) {
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::EpochChanged;
                    }
                    if !pinging {
                        continue;
                    }
                    let unread = paused_since.map_or(Duration::ZERO, |since| since.elapsed());
                    let ms = u64::try_from(accepted.elapsed().as_millis()).unwrap_or(u64::MAX);
                    let t = pings.back().map_or(ms, |&(last, _, _)| ms.max(last + 1));
                    pings.push_back((t, Instant::now(), paused_total + unread));
                    vec![encode.ping(t)]
                }
                received = rx.next(), if !at_limit && !held => match received {
                    None | Some(Ok(Message::Close(_))) => return Ended::Closed,
                    Some(Err(_)) => return Ended::Failed,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => Vec::new(),
                    Some(Ok(m)) => match on_message(m, admission) {
                        Reply::Command(reply) => {
                            replies.push_back(reply);
                            Vec::new()
                        }
                        Reply::Now(reply) => reply.into_iter().collect(),
                        Reply::Pong(t) => {
                            // Counted from when its ping was sent, so pongs
                            // that waited unread do not count as fresh; one
                            // that echoes no ping outstanding does not
                            // count, and leaves the pings outstanding.
                            if let Some(at) = pings.iter().position(|&(sent_t, _, _)| sent_t == t)
                                && let Some((_, sent, unread)) = pings.drain(..=at).next_back()
                            {
                                paused_before = paused_total - unread;
                                answered = sent + paused_before;
                                sessions.answered(owner, answered);
                            }
                            Vec::new()
                        }
                        Reply::CatchUp(request) => {
                            if !pinging {
                                // Read now, so not paused.
                                pinging = true;
                                answered = Instant::now();
                                paused_before = Duration::ZERO;
                                sessions.answered(owner, answered);
                                ping.reset_immediately();
                            }
                            catching_up = Some((Box::pin(answer(state, request)), true));
                            Vec::new()
                        }
                        Reply::Promote(Some(p))
                            if p.epoch == state.epoch() && state.revision() < p.revision =>
                        {
                            floor = Some(Box::pin(promote_floor(state, p.revision)));
                            Vec::new()
                        }
                        Reply::Promote(_) => Vec::new(),
                    },
                },
                applied = async { floor.as_mut().expect("guarded").await }, if floor.is_some() => {
                    floor = None;
                    if !applied {
                        admission = Admission::NotApplied;
                    }
                    Vec::new()
                }
                // Not while catching up, so no reply overtakes the state it
                // refers to.
                Some(reply) = replies.next(),
                    if caught_up && catching_up.is_none() && !replies.is_empty() =>
                {
                    if epoch.is_some_and(|e| e != state.epoch()) {
                        let deadline = pong_deadline(answered, paused_since);
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::EpochChanged;
                    }
                    reply.into_iter().collect()
                }
                a = async { catching_up.as_mut().expect("guarded").0.as_mut().await },
                    if catching_up.is_some() =>
                {
                    catching_up = None;
                    if epoch.is_some_and(|e| e != a.epoch()) {
                        let deadline = pong_deadline(answered, paused_since);
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::EpochChanged;
                    }
                    caught_up = true;
                    epoch = Some(a.epoch());
                    position = a.position();
                    if position.is_some() {
                        admission = Admission::Submit;
                    }
                    encode.answer(a)
                }
                // Statuses held while the client holds a position the
                // replication never committed.
                popped = queue.pop_held(position.is_none()), if caught_up && catching_up.is_none() => match (popped, position) {
                    (Popped::Overflow, _) => {
                        let deadline = pong_deadline(answered, paused_since);
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::Overflow;
                    }
                    (Popped::Frame(Frame::Delta(d)), _) if epoch != Some(d.epoch) => {
                        let deadline = pong_deadline(answered, paused_since);
                        let _ = send(&mut tx, Message::Close(None), deadline).await;
                        return Ended::EpochChanged;
                    }
                    (Popped::Frame(Frame::Delta(_) | Frame::Text(_)) | Popped::Resync, None) => {
                        Vec::new()
                    }
                    (Popped::Frame(Frame::Delta(d)), Some(p)) if d.revision <= p.revision => {
                        Vec::new()
                    }
                    (Popped::Frame(Frame::Delta(d)), Some(p)) if d.prev == p.revision => {
                        position = Some(Position {
                            revision: d.revision,
                            ..p
                        });
                        vec![Message::Text(d.text)]
                    }
                    (Popped::Frame(Frame::Delta(_)) | Popped::Resync, Some(p)) => {
                        let request = CatchUp {
                            since: Some(p),
                            full: false,
                        };
                        catching_up = Some((Box::pin(catch_up(state, request, false)), false));
                        Vec::new()
                    }
                    (Popped::Frame(Frame::Meters(m)), _) => vec![Message::Binary(m)],
                    (
                        Popped::Frame(
                            Frame::Counters(t) | Frame::Text(t) | Frame::Status(_, t),
                        ),
                        _,
                    ) => vec![Message::Text(t)],
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
    /// can be a while after the last ping it answered was sent when the
    /// task is not polled.
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

    /// Paused within the pong timeout of sending the last ping answered, a
    /// connection stays live until the pause ends it; paused later, it is
    /// not live.
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

    /// Resumed, a connection is live within the pong timeout of sending
    /// the last ping answered, moved on by the pause, and paused again
    /// within it, until the new pause ends.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_connection_counts_from_its_moved_last_ping_answered() {
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

    /// The connection id fits [`MAX_CONNECTION_ID`] with the longest
    /// replica id and the largest counter; a longer replica id is refused.
    #[test]
    fn connection_ids_stay_within_128_characters() {
        let longest = "r".repeat(MAX_REPLICA_ID);
        let sessions = Sessions::new(&longest).unwrap();
        sessions.next.store(u64::MAX, Ordering::Relaxed);
        let id = sessions.connection_id();
        assert!(id.ends_with(":18446744073709551615"), "{id}");
        assert!(id.len() <= MAX_CONNECTION_ID, "{} characters", id.len());

        for replica in [format!("{longest}r"), String::new()] {
            assert!(matches!(Sessions::new(&replica), Err(Error::Config(_))));
        }
    }

    /// A connection id splits at its first `:`, so a replica id holding
    /// one would name another replica; ids are DNS labels, as pod names.
    #[test]
    fn replica_ids_are_dns_labels() {
        assert!(Sessions::new("samplez-0").is_ok());
        assert!(Sessions::new("0").is_ok());
        for replica in ["a:b", "R1", "r.1", "r_1", "r 1", "r\u{e9}", "-", "-a", "a-"] {
            assert!(
                matches!(Sessions::new(replica), Err(Error::Config(_))),
                "{replica:?}"
            );
        }
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
