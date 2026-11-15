//! What the replicated log carries: one [`Request`] per entry, and the
//! [`Outcome`] of each command in it once applied.

use std::collections::BTreeMap;

use samplez_service::{Owner, Role};
use serde::{Deserialize, Serialize};

/// The node id openraft orders replicas by.
pub type NodeId = u64;

/// A session's dedup and lease key: the same session id under two
/// principals is two sessions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SessionKey {
    pub principal: String,
    pub session: String,
}

impl SessionKey {
    pub fn from_parts(principal: &str, session: &str) -> SessionKey {
        SessionKey {
            principal: principal.into(),
            session: session.into(),
        }
    }
}

impl From<&Owner> for SessionKey {
    fn from(owner: &Owner) -> SessionKey {
        SessionKey {
            principal: owner.principal.clone(),
            session: owner.session.clone(),
        }
    }
}

/// [`Owner`] as an entry carries it, so a forwarded command keeps who sent
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    pub principal: String,
    pub session: String,
    pub role: Role,
    pub connection: String,
}

impl Origin {
    pub fn key(&self) -> SessionKey {
        SessionKey::from_parts(&self.principal, &self.session)
    }
}

impl From<&Owner> for Origin {
    fn from(owner: &Owner) -> Origin {
        Origin {
            principal: owner.principal.clone(),
            session: owner.session.clone(),
            role: owner.role,
            connection: owner.connection.clone(),
        }
    }
}

/// One client command under its client-generated id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub origin: Origin,
    pub id: String,
    /// The index the client asked for, kept when later than the stamping
    /// index (scheduled); `None` applies at the earliest.
    pub at_index: Option<u64>,
    /// The product's encoding; `None` once a later command in the same
    /// entry replaced its value (coalesced), so only its id is recorded.
    pub body: Option<Vec<u8>>,
}

/// The commands of one entry, coalesced per path before commit.
#[derive(Debug, Default)]
pub struct Batch {
    commands: Vec<Command>,
    latest: BTreeMap<String, usize>,
}

impl Batch {
    /// Appends `command`. With a `path`, an earlier command of this batch
    /// on the same path is superseded: its body is dropped and its id kept.
    pub fn push(&mut self, command: Command, path: Option<String>) {
        let at = self.commands.len();
        if let Some(path) = path
            && let Some(earlier) = self.latest.insert(path, at)
        {
            self.commands[earlier].body = None;
        }
        self.commands.push(command);
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub fn into_commands(self) -> Vec<Command> {
        self.commands
    }
}

/// A replica's claim on the set's one disruption slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disruption {
    pub node: NodeId,
    /// The witness's own update rather than an audio replica's.
    pub witness: bool,
}

/// The replica the outputs commit from, and the order the others follow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    pub replica: NodeId,
    pub order: Vec<NodeId>,
    pub at_index: u64,
}

/// What an entry does once applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Body {
    /// Client commands, deduplicated by (principal, session, id).
    Commands(Vec<Command>),
    Baseline(Baseline),
    /// Moves the pending entry at `revision` to `at_index`.
    Restamp {
        revision: u64,
        at_index: u64,
        paths: Vec<String>,
    },
    /// Drops the pending entry at `revision` unapplied.
    Void {
        revision: u64,
    },
    ClockVerdict {
        replica: NodeId,
        judged: bool,
    },
    Disrupt(Option<Disruption>),
    /// Renews the leases of these sessions at the entry's stamping index.
    LeaseRenew(Vec<SessionKey>),
    /// Ends `session`'s lease if it was not renewed after `renewed`, the
    /// renewal the expiry was decided on.
    LeaseExpiry {
        session: SessionKey,
        renewed: u64,
    },
    InputDown {
        slot: u32,
        at_index: u64,
    },
    InputUp {
        slot: u32,
        at_index: u64,
    },
    /// The product's encoding of a re-route.
    Route {
        at_index: u64,
        body: Vec<u8>,
    },
    MasterEnable {
        enabled: bool,
        at_index: u64,
    },
}

impl Body {
    /// The engine index the entry takes effect at, for the entries the
    /// engine applies rather than the state machine alone.
    pub fn at_index(&self) -> Option<u64> {
        match self {
            Body::InputDown { at_index, .. }
            | Body::InputUp { at_index, .. }
            | Body::Route { at_index, .. }
            | Body::MasterEnable { at_index, .. } => Some(*at_index),
            _ => None,
        }
    }
}

/// The application data of one log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// `ceil(E / B) * B` at stamping: the log time dedup retention counts
    /// in, whatever the entry's at_index.
    pub stamp: u64,
    pub body: Body,
}

/// How one command of an entry resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resolution {
    /// The product applied it.
    Applied,
    /// Its id was in its session's table: nothing changed.
    Duplicate,
    /// A later command in the entry replaced its value.
    Superseded,
    /// The product refused it as older than what it already applied.
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// The revision whose delta carries the command's change: the stored
    /// one for a duplicate, the entry's otherwise.
    pub revision: u64,
    pub result: Resolution,
}

/// One [`Outcome`] per command of a [`Body::Commands`] entry, in order;
/// empty for every other entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response(pub Vec<Outcome>);

/// `ceil(e / block) * block`: an estimate rounded up to a block boundary.
pub fn stamping_index(e: u64, block: u64) -> u64 {
    e.div_ceil(block) * block
}
