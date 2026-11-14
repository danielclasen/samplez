//! Per-client WebSocket send queues, fan-out, and the loop that drains a
//! queue into a socket.
//!
//! A client connecting with the last global revision it saw gets the deltas
//! after it, from whichever replica it reaches, when that replica still
//! holds them, and a snapshot otherwise; a client without one gets a
//! snapshot. Live deltas follow in order. Meter frames are dropped when the
//! queue is full; deltas are never dropped one by one: when the queue holds
//! nothing but deltas and another arrives, the queue is emptied and the
//! client catches up the same way as on connect.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::extract::ws::{Message, Utf8Bytes};
use futures_util::{Sink, SinkExt};
use tokio::sync::Notify;

use crate::replica::{Commands, ReplicatedState};

/// A state change at a global revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    pub revision: u64,
    /// The id of the command that caused it; the text echoes it.
    pub command: Option<String>,
    pub text: Utf8Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Superseded by the next one of its kind: meters, counters.
    Meter(Utf8Bytes),
    Delta(Delta),
}

/// The whole state as of `revision`.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub revision: u64,
    pub text: Utf8Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    /// A meter frame was dropped: the queued oldest one, or the pushed one
    /// when no meter was queued.
    MeterDropped,
    /// The queue was emptied, or the delta discarded, because the client
    /// has to catch up.
    Resync,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Popped {
    Frame(Frame),
    /// The client has to catch up from the last revision it was sent.
    Resync,
}

/// A bounded queue with one consumer.
pub struct ClientQueue {
    capacity: usize,
    inner: Mutex<Inner>,
    notify: Notify,
}

struct Inner {
    frames: VecDeque<Frame>,
    resync: bool,
}

impl ClientQueue {
    pub fn new(capacity: usize) -> ClientQueue {
        assert!(capacity > 0, "a client queue needs room for one frame");
        ClientQueue {
            capacity,
            inner: Mutex::new(Inner {
                frames: VecDeque::with_capacity(capacity),
                resync: false,
            }),
            notify: Notify::new(),
        }
    }

    pub fn push(&self, frame: Frame) -> Pushed {
        let mut q = self.inner.lock().expect("queue lock poisoned");
        let is_delta = matches!(frame, Frame::Delta(_));
        let pushed = if q.resync && is_delta {
            // The pending catch-up reads the state after this delta.
            return Pushed::Resync;
        } else if q.frames.len() < self.capacity {
            q.frames.push_back(frame);
            Pushed::Queued
        } else if let Some(i) = q.frames.iter().position(|f| matches!(f, Frame::Meter(_))) {
            q.frames.remove(i);
            q.frames.push_back(frame);
            Pushed::MeterDropped
        } else if is_delta {
            q.frames.clear();
            q.resync = true;
            Pushed::Resync
        } else {
            return Pushed::MeterDropped;
        };
        drop(q);
        self.notify.notify_one();
        pushed
    }

    pub async fn pop(&self) -> Popped {
        loop {
            {
                let mut q = self.inner.lock().expect("queue lock poisoned");
                if q.resync {
                    q.resync = false;
                    return Popped::Resync;
                }
                if let Some(frame) = q.frames.pop_front() {
                    return Popped::Frame(frame);
                }
            }
            self.notify.notified().await;
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("queue lock poisoned").frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Fans frames out to every subscribed client queue and records the
/// command ids the deltas carry.
pub struct Hub {
    capacity: usize,
    clients: Mutex<Vec<Weak<ClientQueue>>>,
    commands: Commands,
}

impl Hub {
    /// `capacity` is the length of each client's queue; command ids are
    /// remembered for `retention`.
    pub fn new(capacity: usize, retention: Duration) -> Hub {
        Hub {
            capacity,
            clients: Mutex::default(),
            commands: Commands::new(retention),
        }
    }

    pub fn commands(&self) -> &Commands {
        &self.commands
    }

    /// The queue stays subscribed until it is dropped. Subscribe before
    /// [`pump`] reads the state, so no delta falls between the two.
    pub fn subscribe(&self) -> Arc<ClientQueue> {
        let queue = Arc::new(ClientQueue::new(self.capacity));
        self.clients
            .lock()
            .expect("hub lock poisoned")
            .push(Arc::downgrade(&queue));
        queue
    }

    /// Publishes every delta the state applies, the ones replicated from
    /// other replicas included.
    pub fn publish(&self, frame: Frame) {
        if let Frame::Delta(Delta {
            revision,
            command: Some(id),
            ..
        }) = &frame
        {
            self.commands.record(id, *revision);
        }
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

/// The messages that bring a client from `since` to the state's revision,
/// and that revision.
fn catch_up(state: &dyn ReplicatedState, since: Option<u64>) -> (u64, Vec<Utf8Bytes>) {
    if let Some(since) = since
        && let Some(deltas) = state.deltas_since(since)
    {
        let revision = deltas.last().map_or(since, |d| d.revision.max(since));
        let texts = deltas.into_iter().filter(|d| d.revision > since);
        return (revision, texts.map(|d| d.text).collect());
    }
    let snapshot = state.snapshot();
    (snapshot.revision, vec![snapshot.text])
}

/// Catches the client up from `since` (see the module docs), then drains
/// `queue` into `sink`, skipping deltas the client has. Returns the send
/// error that ends it.
pub async fn pump<S>(
    sink: &mut S,
    queue: &ClientQueue,
    state: &dyn ReplicatedState,
    since: Option<u64>,
) -> S::Error
where
    S: Sink<Message> + Unpin,
{
    let (mut revision, mut batch) = catch_up(state, since);
    loop {
        for text in batch.drain(..) {
            if let Err(e) = sink.send(Message::Text(text)).await {
                return e;
            }
        }
        match queue.pop().await {
            Popped::Frame(Frame::Meter(text)) => batch.push(text),
            Popped::Frame(Frame::Delta(d)) if d.revision > revision => {
                revision = d.revision;
                batch.push(d.text);
            }
            Popped::Frame(Frame::Delta(_)) => {}
            Popped::Resync => (revision, batch) = catch_up(state, Some(revision)),
        }
    }
}
