//! The interface to the product's replicated state, and the local guard
//! against submitting one command id twice at once from this replica.
//!
//! Every replica of a product applies the same deltas at the same global
//! revisions. The service layer never counts revisions itself: they come
//! from [`ReplicatedState`] and from the deltas the product publishes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
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
    /// The client's own session id, stable across its reconnects. The
    /// client chooses it, so it is not an authenticated identity: any
    /// client can present another's, and in `none` and `token` mode every
    /// client has the principal `anonymous`. Grant nothing on it beyond
    /// what the principal and role already allow.
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
    /// snapshots carry; `None` when this replica has not applied it.
    fn command_revision(&self, id: &str) -> Option<u64>;

    /// Orders `command`, the product's encoding of it, under the
    /// client-generated `id`, and resolves once this replica has applied
    /// the entry carrying it.
    ///
    /// The state machine dedups by id when it applies an entry: an entry
    /// whose id the replicated table of applied command ids holds changes
    /// nothing and resolves with the revision the table holds. A command
    /// resent through another replica while the first copy is still being
    /// committed is so applied once, and both submits resolve to the same
    /// [`Ack`]. A check before ordering, such as [`Commands::begin`], cannot
    /// give that: both copies pass it before either is applied.
    fn submit(&self, id: &str, command: Bytes) -> BoxFuture<'_, Result<Ack, NotApplied>>;

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

/// [`ReplicatedState::submit`] could not order the command, for example
/// without a quorum. Whether it was applied is unknown; a resend with the
/// same id is applied at most once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotApplied(pub String);

pub enum Begin<'a> {
    /// Not applied as far as this replica has applied: submit it with
    /// [`Pending::submit`]. Dropping the [`Pending`] unsubmitted releases
    /// the id, so a resend is submitted.
    New(Pending<'a>),
    /// Applied already, through any replica; acknowledge it again without
    /// submitting it.
    Duplicate(Ack),
    /// Being submitted from this replica right now.
    InFlight,
    /// As many commands as the limit are being submitted from this replica.
    Busy,
}

/// The command ids being submitted from this replica, so a resend to the
/// same replica waits for the first copy instead of ordering a second.
/// Whether an id was applied is the replicated state's answer; that its
/// state machine dedups by id ([`ReplicatedState::submit`]) is what keeps a
/// copy resent through another replica from being applied twice.
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
    /// Submits the command to the replicated state and releases the id once
    /// it resolved.
    pub async fn submit(self, command: Bytes) -> Result<Ack, NotApplied> {
        self.commands.state.submit(&self.id, command).await
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.commands.lock().remove(&self.id);
    }
}
