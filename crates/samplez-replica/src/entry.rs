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
    /// The product's encoding.
    #[serde(with = "bytes")]
    pub body: Vec<u8>,
}

impl Command {
    /// What the command adds to an entry, for the bytes-per-entry cap.
    pub fn size(&self) -> usize {
        let origin = &self.origin;
        self.body.len()
            + self.id.len()
            + origin.principal.len()
            + origin.session.len()
            + origin.connection.len()
    }
}

/// The commands of one entry, in the order they were submitted. Every
/// command keeps its body: the product's own ordering decides what an
/// older one does once applied after a newer one.
#[derive(Debug, Default)]
pub struct Batch {
    commands: Vec<Command>,
    held: BTreeMap<(SessionKey, String), usize>,
}

impl Batch {
    /// Appends `command` and returns its position in the entry, or the
    /// position of the copy of it (same session and id) the batch holds
    /// already, which it is folded into: one apply answers both.
    pub fn push(&mut self, command: Command) -> usize {
        let at = self.commands.len();
        let at = *self
            .held
            .entry((command.origin.key(), command.id.clone()))
            .or_insert(at);
        if at == self.commands.len() {
            self.commands.push(command);
        }
        at
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
        #[serde(with = "bytes")]
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
    /// The product applied it, and it changed the product.
    Applied,
    /// The product applied it, and it changed nothing: the state held it
    /// already.
    Unchanged,
    /// A copy of an applied command, its id in its session's table:
    /// nothing changed.
    Duplicate,
    /// The product refused it as older than what it already applied; a
    /// copy of it while its id is held resolves so as well.
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    /// The product revision the command resolved at: the stored one for a
    /// duplicate, otherwise the revision after its entry, which is the
    /// entry's own when the entry changed the product.
    pub revision: u64,
    pub result: Resolution,
}

/// One [`Outcome`] per command of a [`Body::Commands`] entry, in order;
/// empty for every other entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response(pub Vec<Outcome>);

/// Bytes as base64 text, a third above their length, where serde_json
/// would write an array of numbers, up to four times it.
pub mod bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// `ceil(e / block) * block`: an estimate rounded up to a block boundary.
pub fn stamping_index(e: u64, block: u64) -> u64 {
    e.div_ceil(block) * block
}
