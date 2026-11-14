//! The interface to the product's replicated state, and command
//! deduplication by client-generated command id.
//!
//! Every replica of a product applies the same deltas at the same global
//! revisions. The service layer never counts revisions itself: they come
//! from [`ReplicatedState`] and from the deltas the product publishes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;

use crate::ws::{Delta, Snapshot};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaInfo {
    pub id: String,
    /// As the replication mechanism names it.
    pub role: String,
}

/// Implemented by the product or by the replication mechanism.
pub trait ReplicatedState: Send + Sync + 'static {
    fn replica(&self) -> ReplicaInfo;

    /// The global revision this replica has applied.
    fn revision(&self) -> u64;

    /// The whole state as of the revision in it.
    fn snapshot(&self) -> Snapshot;

    /// The deltas with a revision above `revision` up to this replica's
    /// own, in order: empty when `revision` is not below it, `None` when
    /// this replica no longer holds all of them.
    fn deltas_since(&self, revision: u64) -> Option<Vec<Delta>>;
}

/// The answer to a command, echoing its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ack {
    pub command: String,
    /// The revision whose delta carries the command's change.
    pub revision: u64,
}

pub enum Begin<'a> {
    /// Not seen within the retention window: apply it, then call
    /// [`Pending::applied`]. Dropping the [`Pending`] forgets the id, so a
    /// resend is applied.
    New(Pending<'a>),
    /// Applied already, here or on another replica whose delta reached
    /// this one; acknowledge it again without applying it.
    Duplicate(Ack),
    /// Being applied on this replica right now.
    InFlight,
}

#[derive(Debug, Clone, Copy)]
enum Entry {
    InFlight,
    Done { revision: u64, at: Instant },
}

/// Command ids seen within the retention window: those applied here and
/// those carried by the deltas [`crate::ws::Hub::publish`] passes on, which
/// include the deltas replicated from other replicas.
pub struct Commands {
    retention: Duration,
    entries: Mutex<HashMap<String, Entry>>,
}

impl Commands {
    pub fn new(retention: Duration) -> Commands {
        Commands {
            retention,
            entries: Mutex::default(),
        }
    }

    pub fn begin(&self, id: &str) -> Begin<'_> {
        let mut entries = self.lock();
        match entries.get(id) {
            Some(Entry::Done { revision, .. }) => Begin::Duplicate(Ack {
                command: id.to_owned(),
                revision: *revision,
            }),
            Some(Entry::InFlight) => Begin::InFlight,
            None => {
                entries.insert(id.to_owned(), Entry::InFlight);
                Begin::New(Pending {
                    commands: self,
                    id: Some(id.to_owned()),
                })
            }
        }
    }

    /// Marks `id` applied at `revision`.
    pub fn record(&self, id: &str, revision: u64) {
        let at = Instant::now();
        self.lock()
            .insert(id.to_owned(), Entry::Done { revision, at });
    }

    /// Locks the map with the entries older than the window removed.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        let mut entries = self.entries.lock().expect("commands lock poisoned");
        let now = Instant::now();
        entries.retain(|_, e| match e {
            Entry::InFlight => true,
            Entry::Done { at, .. } => now.duration_since(*at) < self.retention,
        });
        entries
    }
}

pub struct Pending<'a> {
    commands: &'a Commands,
    id: Option<String>,
}

impl Pending<'_> {
    pub fn applied(mut self, revision: u64) -> Ack {
        let command = self.id.take().expect("taken only here and in drop");
        self.commands.record(&command, revision);
        Ack { command, revision }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let mut entries = self.commands.lock();
            if matches!(entries.get(&id), Some(Entry::InFlight)) {
                entries.remove(&id);
            }
        }
    }
}
