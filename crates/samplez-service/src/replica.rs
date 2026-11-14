//! The interface to the product's replicated state, and the local guard
//! against applying one command id twice at once.
//!
//! Every replica of a product applies the same deltas at the same global
//! revisions. The service layer never counts revisions itself: they come
//! from [`ReplicatedState`] and from the deltas the product publishes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use serde::Serialize;

use crate::ws::{Delta, Snapshot};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaInfo {
    pub id: String,
    /// As the replication mechanism names it.
    pub role: String,
    /// The id of the replica that orders commands, when known.
    pub leader: Option<String>,
    /// Whether this replica has applied every committed entry it knows of.
    pub in_sync: bool,
    /// Committed entries not yet applied here, when known.
    pub lag: Option<u64>,
}

/// A point in the history of one state: a revision within an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Position {
    pub epoch: u64,
    pub revision: u64,
}

/// A WebSocket client's claim on state that expires without renewal, such
/// as touch ownership of a control.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Owner {
    /// The user the proxy names, or `anonymous`.
    pub principal: String,
    /// The client's own session id, stable across its reconnects.
    pub session: String,
    /// `<replica id>:<n>`, unique per connection on a running replica.
    pub connection: String,
}

/// Implemented by the product or by the replication mechanism.
pub trait ReplicatedState: Send + Sync + 'static {
    fn replica(&self) -> ReplicaInfo;

    /// The id of this state's history: the replication group's cluster id,
    /// or a random id persisted with the state. A state that starts empty
    /// gets a new epoch, so revisions from before it are not resumed from.
    fn epoch(&self) -> u64;

    /// The global revision this replica has applied.
    fn revision(&self) -> u64;

    /// The whole state as of the position in it.
    fn snapshot(&self) -> BoxFuture<'_, Snapshot>;

    /// The deltas after `since` up to this replica's revision, in order:
    /// empty when `since.revision` is not below it, `None` when this
    /// replica no longer holds all of them or `since.epoch` is not this
    /// state's epoch.
    fn deltas_since(&self, since: Position) -> BoxFuture<'_, Option<Vec<Delta>>>;

    /// The revision at which the command with this client-generated id was
    /// applied, from the replicated table of applied command ids that
    /// snapshots carry; `None` when it was not applied.
    fn command_revision(&self, id: &str) -> Option<u64>;

    /// Called about once per second by [`crate::Bound::serve`] with the
    /// owners whose connection to this replica answered a ping within the
    /// last second; not called when there are none. Must not block: the
    /// renewal is passed on to the replication mechanism.
    fn renew_sessions(&self, live: &[Owner]);
}

/// The answer to a command, echoing its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ack {
    pub command: String,
    /// The revision whose delta carries the command's change.
    pub revision: u64,
}

pub enum Begin<'a> {
    /// Not applied: apply it, then call [`Pending::applied`]. Dropping the
    /// [`Pending`] releases the id, so a resend is applied.
    New(Pending<'a>),
    /// Applied already, on any replica; acknowledge it again without
    /// applying it.
    Duplicate(Ack),
    /// Being applied on this replica right now.
    InFlight,
    /// As many commands as the limit are being applied on this replica.
    Busy,
}

/// The command ids being applied on this replica. Whether an id was applied
/// is the replicated state's answer, [`ReplicatedState::command_revision`].
pub struct Commands {
    state: Arc<dyn ReplicatedState>,
    limit: usize,
    in_flight: Mutex<HashSet<String>>,
}

impl Commands {
    /// `limit` caps the commands in flight at once.
    pub fn new(state: Arc<dyn ReplicatedState>, limit: usize) -> Commands {
        Commands {
            state,
            limit,
            in_flight: Mutex::default(),
        }
    }

    pub fn begin(&self, id: &str) -> Begin<'_> {
        let mut in_flight = self.lock();
        if in_flight.contains(id) {
            return Begin::InFlight;
        }
        if let Some(revision) = self.state.command_revision(id) {
            return Begin::Duplicate(Ack {
                command: id.to_owned(),
                revision,
            });
        }
        if in_flight.len() >= self.limit {
            return Begin::Busy;
        }
        in_flight.insert(id.to_owned());
        Begin::New(Pending {
            commands: self,
            id: id.to_owned(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.in_flight.lock().expect("commands lock poisoned")
    }
}

pub struct Pending<'a> {
    commands: &'a Commands,
    id: String,
}

impl Pending<'_> {
    /// Call once the state has applied the command at `revision`, so
    /// [`ReplicatedState::command_revision`] answers for it.
    pub fn applied(self, revision: u64) -> Ack {
        Ack {
            command: self.id.clone(),
            revision,
        }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.commands.lock().remove(&self.id);
    }
}
