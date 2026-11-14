//! The interface to the product's replicated state, and the local check
//! before a command is submitted to it.
//!
//! Every replica of a product applies the same deltas at the same global
//! revisions. The service layer never counts revisions itself: they come
//! from [`ReplicatedState`] and from the deltas the product publishes.

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
    /// The client's own session id, stable across its reconnects: 128
    /// random bits or more, drawn once per client instance, so two clients
    /// or two tabs never share one (see [`crate::Service::ws`]); or, for a
    /// client that sends none, its connection id. The client chooses it,
    /// so it is not an authenticated identity: any client can present
    /// another's, and in `none` and `token` mode every client has the
    /// principal `anonymous`. Grant nothing on it beyond what the principal
    /// and role already allow; the tables of applied command ids are kept
    /// per principal and session, so presenting another principal's
    /// session reaches none of its commands.
    pub session: String,
    /// `<replica id>:<nonce>:<n>`: the nonce is 128 random bits drawn when
    /// the replica's [`crate::Service`] is created, `n` counts its
    /// connections from 1, so the id does not recur after a restart.
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
    /// applied for `owner.principal` and `owner.session`, from the
    /// replicated per-session tables of applied command ids that snapshots
    /// carry; `None` when this replica has not applied it.
    ///
    /// A session's table is kept as long as a client may resend a command
    /// of it that it got no [`Ack`] for, which is longer than the session's
    /// lease: a client that reconnects after its lease expired resends
    /// what it did not get answered, and with the table dropped at the
    /// expiry the resend is applied twice. The service sets no bound on
    /// that time; the implementation sets the retention after which a
    /// session's table is dropped, and a resend after it is applied again.
    fn command_revision(&self, owner: &Owner, id: &str) -> Option<u64>;

    /// Orders `command`, the product's encoding of it, under the
    /// client-generated `id` for `owner`, and resolves once this replica has
    /// applied the entry carrying it. The entry carries `owner`, so a
    /// replica forwarding it to the leader passes on who sent it.
    ///
    /// The state machine dedups by `owner.principal`, `owner.session` and
    /// `id` when it applies an entry: an entry whose id the table of the
    /// principal's session holds changes nothing and resolves with the
    /// revision the table holds. An id is unique within its session only,
    /// so the same id from two sessions, or from two principals presenting
    /// the same session, is two commands. A command resent through another
    /// replica while the first copy is still being committed is so applied
    /// once, and both submits resolve to the same [`Ack`]. A check before
    /// ordering, such as [`Commands::begin`], cannot give that: both copies
    /// pass it before either is applied.
    ///
    /// The calls on one replica fix the order in which their commands are
    /// committed, before any returned future is polled: a command submitted
    /// after another is committed after it, also when the replica forwards
    /// them to the leader. One client's commands, submitted in the order it
    /// sent them, so commit in that order.
    ///
    /// Must resolve in bounded time, with [`NotApplied`] when the command
    /// cannot be ordered: a WebSocket connection with
    /// [`crate::ws::MAX_PENDING_REPLIES`] replies waiting is not read until
    /// one of them is ready.
    fn submit(
        &self,
        owner: &Owner,
        id: &str,
        command: Bytes,
    ) -> BoxFuture<'_, Result<Ack, NotApplied>>;

    /// Called about once per second by [`crate::Bound::serve`] with the
    /// owners whose connection to this replica is live
    /// ([`crate::ws::Sessions::live`]): it answered a ping within the last
    /// second, or within the second before it stopped being read at
    /// [`crate::ws::MAX_PENDING_REPLIES`]; not called when there are none.
    /// Must not block: the renewal is passed on to the replication
    /// mechanism.
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
    /// [`Pending::submit`]. A resend while an earlier copy is still being
    /// submitted, through this replica or another, is new as well; the
    /// state machine applies it once and both copies get the same [`Ack`]
    /// ([`ReplicatedState::submit`]).
    New(Pending<'a>),
    /// Applied already, through any replica; acknowledge it again without
    /// submitting it.
    Duplicate(Ack),
    /// As many commands as the limit are being submitted from this replica.
    Busy,
}

/// Answers a command that was applied already from its session's table of
/// applied command ids, and limits the commands being submitted from this
/// replica at once.
pub struct Commands {
    state: Arc<dyn ReplicatedState>,
    limit: usize,
    in_flight: Mutex<usize>,
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

    pub fn begin(&self, owner: &Owner, id: &str) -> Begin<'_> {
        if let Some(revision) = self.state.command_revision(owner, id) {
            return Begin::Duplicate(Ack {
                command: id.to_owned(),
                revision,
            });
        }
        let mut in_flight = self.lock();
        if *in_flight >= self.limit {
            return Begin::Busy;
        }
        *in_flight += 1;
        Begin::New(Pending {
            commands: self,
            owner: owner.clone(),
            id: id.to_owned(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, usize> {
        self.in_flight.lock().expect("commands lock poisoned")
    }
}

pub struct Pending<'a> {
    commands: &'a Commands,
    owner: Owner,
    id: String,
}

impl<'a> Pending<'a> {
    /// Submits the command to the replicated state now, not when the
    /// returned future is first polled, so commands submitted one after
    /// another commit in that order; it counts against the limit until the
    /// future resolved or was dropped.
    pub fn submit(self, command: Bytes) -> BoxFuture<'a, Result<Ack, NotApplied>> {
        let state: &'a dyn ReplicatedState = &*self.commands.state;
        let submitted = state.submit(&self.owner, &self.id, command);
        Box::pin(async move {
            let _counted = self;
            submitted.await
        })
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        *self.commands.lock() -= 1;
    }
}
