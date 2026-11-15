//! The interface to the product's replicated state, and the local check
//! before a command is submitted to it.
//!
//! Every replica of a product applies the same deltas at the same global
//! revisions. The service layer never counts revisions itself: they come
//! from [`ReplicatedState`] and from the deltas the product publishes.

use std::fmt;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::http::StatusCode;
use futures_util::future::BoxFuture;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::auth::Role;
use crate::ws::{Delta, Snapshot};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaInfo {
    /// A DNS label, as a pod name is: [`crate::Service::new`] refuses
    /// others ([`crate::ws::Sessions::new`]).
    pub id: String,
    /// As the replication mechanism names it.
    pub role: String,
    /// The id of the replica that orders commands, when known.
    pub leader: Option<String>,
    /// Whether this replica has applied every committed entry it knows of.
    pub in_sync: bool,
    /// Committed entries not yet applied here, when known.
    pub lag: Option<u64>,
    /// The replicated state's term of `/readyz`, beside the API serving and
    /// the engine started; the role does not enter it. Always true for a
    /// single replica.
    pub ready: bool,
    /// Whether this replica takes client writes: false while the
    /// replication refuses them here, which it answers with
    /// [`Reason::NotCommitable`]. Products publish it per replica, so a
    /// client served by one that is not commitable fails over. Always true
    /// for a single replica.
    pub commitable: bool,
}

/// A point in the history of one state: a revision within an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Position {
    pub epoch: u64,
    pub revision: u64,
}

/// A WebSocket client's claim on state that expires without renewal, such
/// as touch ownership of a control.
///
/// Not serializable: the session is a credential of its client, so a
/// record published to clients carries [`Owner::handle`] in its place. The
/// `Debug` output shows the handle as well, so a log of an owner does not
/// carry the session.
#[derive(Clone, PartialEq, Eq, Hash)]
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
    /// The role the connection was authorized with at the upgrade, so a
    /// command forwarded to another replica carries it with the principal
    /// and session.
    pub role: Role,
    /// `<replica id>:<nonce>:<n>`, at most
    /// [`crate::ws::MAX_CONNECTION_ID`] characters: the nonce is 128 random
    /// bits in 32 hexadecimal digits drawn when the replica's
    /// [`crate::Service`] is created, `n` counts its connections from 1, so
    /// the id does not recur after a restart.
    pub connection: String,
}

impl Owner {
    /// The public handle of [`Owner::session`].
    pub fn handle(&self) -> OwnerHandle {
        OwnerHandle::of(&self.session)
    }
}

impl fmt::Debug for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Owner")
            .field("principal", &self.principal)
            .field("handle", &self.handle())
            .field("role", &self.role)
            .field("connection", &self.connection)
            .finish()
    }
}

/// The public handle of a session: the first 16 bytes of the SHA-256 digest
/// of the session id in base64url without padding, 22 characters. It names
/// the session in records published to clients; the session id cannot be
/// derived from it, and a client computes the handle of its own session to
/// recognise its own records.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OwnerHandle(String);

impl OwnerHandle {
    pub fn of(session: &str) -> OwnerHandle {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let digest = Sha256::digest(session.as_bytes());
        let mut bits: u32 = 0;
        let mut held = 0;
        let mut handle = String::with_capacity(22);
        for &byte in &digest[..16] {
            bits = bits << 8 | u32::from(byte);
            held += 8;
            while held >= 6 {
                held -= 6;
                handle.push(char::from(ALPHABET[(bits >> held) as usize & 63]));
            }
        }
        handle.push(char::from(ALPHABET[(bits << (6 - held)) as usize & 63]));
        OwnerHandle(handle)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OwnerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Implemented by the product or by the replication mechanism.
pub trait ReplicatedState: Send + Sync + 'static {
    fn replica(&self) -> ReplicaInfo;

    /// The id of this state's history, so a revision names one state only
    /// together with it. Drawn anew whenever the state is initialised empty
    /// or restored from a backup, and kept across a restart, a snapshot
    /// install and a resync: a restored state continues from the backup's
    /// revisions, which clients that saw the state after the backup also
    /// hold for other changes, so they are not resumed from.
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
    /// committed for `owner.principal` and `owner.session`, and the
    /// [`Resolution`] a copy of it gets, from the replicated per-session
    /// tables of committed command ids that snapshots carry; `None` when
    /// this replica has not applied it. A copy of an applied command is a
    /// [`Resolution::Duplicate`]; one of a superseded or stale command
    /// resolves as its first copy did, so a client resending a command it
    /// got no answer for learns what happened to it.
    ///
    /// A session's table is kept as long as a client may resend a command
    /// of it that it got no [`Ack`] for, which is longer than the session's
    /// lease: a client that reconnects after its lease expired resends
    /// what it did not get answered, and with the table dropped at the
    /// expiry the resend is applied twice. The service sets no bound on
    /// that time; the implementation sets the retention after which a
    /// session's table is dropped, and a resend after it is applied again.
    fn command_resolution(&self, owner: &Owner, id: &str) -> Option<(u64, Resolution)>;

    /// Orders `command`, the product's encoding of it, under the
    /// client-generated `id` for `owner`, and resolves once this replica has
    /// applied the entry carrying it. The entry carries `owner`, so a
    /// replica forwarding it to the leader passes on who sent it.
    ///
    /// The state machine dedups by `owner.principal`, `owner.session` and
    /// `id` when it applies an entry: an entry whose id the table of the
    /// principal's session holds changes nothing and resolves with the
    /// revision and resolution the table holds
    /// ([`ReplicatedState::command_resolution`]). An id is unique within its session only,
    /// so the same id from two sessions, or from two principals presenting
    /// the same session, is two commands. A command resent through another
    /// replica while the first copy is still being committed is so applied
    /// once, and both submits resolve to the same revision, the copy
    /// applied second with [`Resolution::Duplicate`]. A check before
    /// ordering, such as [`Commands::begin`], cannot give that: both copies
    /// pass it before either is applied.
    ///
    /// The calls on one replica fix the order in which their commands are
    /// committed, before any returned future is polled: a command submitted
    /// after another is committed after it, also when the replica forwards
    /// them to the leader. One client's commands, submitted in the order it
    /// sent them, so commit in that order. That holds among the commands
    /// that commit: after one resolved with [`NotApplied`], a later one may
    /// still commit, and a resend of the first then commits after it.
    ///
    /// So the command is queued before `submit` returns, not in the
    /// returned future: an ordering call that is an `async fn`, such as
    /// openraft's `Raft::client_write`, does nothing until it is polled.
    /// One task that takes the queue in order and is its only writer keeps
    /// the order, on the leader, and on a follower as one ordered stream to
    /// the leader that resends what it did not get acknowledged in the
    /// same order.
    ///
    /// A command folded into a later one before it is ordered, such as a
    /// continuous control coalesced to its latest value, still has its id
    /// recorded in its session's table, with the revision of the entry
    /// that carries the later one; otherwise a resend of it is applied
    /// after the commands that superseded it.
    ///
    /// Must resolve in bounded time, with [`NotApplied`] when the command
    /// cannot be ordered, [`Reason::NotCommitable`] while
    /// [`ReplicaInfo::commitable`] is false: a WebSocket connection with
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
    /// ([`crate::ws::Sessions::live`]): it answered a ping sent within the
    /// last second, not counting up to [`crate::ws::MAX_PAUSE_TOTAL`] in
    /// which it was not read at [`crate::ws::MAX_PENDING_REPLIES`], which
    /// takes a round trip below 750 ms; not called when there are none.
    /// Must not block: the renewal is passed on to the replication
    /// mechanism.
    fn renew_sessions(&self, live: &[Owner]);
}

/// The answer to a command, echoing its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ack {
    pub command: String,
    /// The revision the command resolved at; its delta carries the
    /// command's change when there is one.
    pub revision: u64,
    pub resolution: Resolution,
}

/// How a committed command resolved. A product answers each in its own
/// protocol: a client told only that its command committed cannot tell
/// a refused one from an applied one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Resolution {
    /// The product applied it.
    Applied,
    /// A copy of a command applied already, through any replica: nothing
    /// changed, and the revision is the one its session's table holds.
    Duplicate,
    /// A later command folded into the same entry replaced its value; so
    /// does a copy of it.
    Superseded,
    /// The product refused it as older than what it applied already;
    /// nothing changed, and a copy of it resolves so as well.
    Stale,
}

/// [`ReplicatedState::submit`] has not applied the command when it
/// resolves; with [`Reason::Unavailable`] it may still commit later, and a
/// resend with the same id applies once. Every reason is answered as not
/// applied, [`NotApplied::STATUS`] in HTTP terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotApplied {
    pub reason: Reason,
    /// For logs and status; not meant to be matched on.
    pub detail: String,
}

/// Why a command was not applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// It could not be committed in bounded time, for example without a
    /// leader or quorum. Whether it commits later is unknown; a resend with
    /// the same id is applied at most once.
    Unavailable,
    /// This replica takes no client writes ([`ReplicaInfo::commitable`]);
    /// the command was not ordered.
    NotCommitable,
    /// The command was first received longer ago than the replication
    /// still deduplicates, so it is refused and never applied; whether to
    /// send it afresh under a new id is the client's decision.
    Expired,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Unavailable => "unavailable",
            Reason::NotCommitable => "not_commitable",
            Reason::Expired => "expired",
        }
    }
}

impl NotApplied {
    /// 503 for every reason.
    pub const STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

    pub fn new(reason: Reason, detail: impl Into<String>) -> NotApplied {
        NotApplied {
            reason,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for NotApplied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not applied ({})", self.reason.as_str())?;
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
    }
}

impl std::error::Error for NotApplied {}

pub enum Begin<'a> {
    /// Not applied as far as this replica has applied: submit it with
    /// [`Pending::submit`]. A resend while an earlier copy is still being
    /// submitted, through this replica or another, is new as well; the
    /// state machine applies it once and both copies get the same revision
    /// ([`ReplicatedState::submit`]).
    New(Pending<'a>),
    /// Committed already, through any replica; answer it again without
    /// submitting it, with the resolution a copy gets
    /// ([`ReplicatedState::command_resolution`]).
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
    /// `limit` caps the commands from this replica whose replies wait on
    /// [`Pending::submit`] at once. A future dropped before it resolved
    /// frees its place, but its command was ordered already and still
    /// commits, so the limit bounds the replies waiting, not the commands
    /// in the log; those the replication mechanism bounds.
    pub fn new(state: Arc<dyn ReplicatedState>, limit: usize) -> Commands {
        Commands {
            state,
            limit,
            in_flight: Mutex::default(),
        }
    }

    pub fn begin(&self, owner: &Owner, id: &str) -> Begin<'_> {
        if let Some((revision, resolution)) = self.state.command_resolution(owner, id) {
            return Begin::Duplicate(Ack {
                command: id.to_owned(),
                revision,
                resolution,
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
