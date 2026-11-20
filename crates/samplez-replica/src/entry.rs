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
    /// Whether the writer ordered it on the generation path, as one that
    /// re-routes inputs ([`crate::state_machine::Product::reroutes`]);
    /// `None` from a writer that did not classify it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<bool>,
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
    /// The named replica's compatibility key ([`crate::rollout`]): the
    /// key every replica compares its own with; none when it reported
    /// none.
    #[serde(default)]
    pub key: Option<u64>,
    /// The versions that key holds, as the named replica reported them;
    /// none when it reported none.
    #[serde(default)]
    pub versions: Option<crate::rollout::Versions>,
}

/// What an entry does once applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Body {
    /// Client commands, deduplicated by (principal, session, id).
    Commands(Vec<Command>),
    Baseline(Baseline),
    /// Applies the unresolved entry at log index `revision` of term
    /// `term` at `at_index` instead, a fresh index the leader stamps; the
    /// entry is then known under the restamp's own log id, which a later
    /// restamp of it names. `paths` names the product paths it still sets
    /// when only some are left; empty for the whole entry.
    Restamp {
        revision: u64,
        term: u64,
        at_index: u64,
        paths: Vec<String>,
        /// `Some(true)` when the entry it names re-routed or raised an
        /// input, so it is ordered on the generation path; `None` from a
        /// leader that did not classify it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        generation: Option<bool>,
    },
    /// Resolves the unresolved entry at log index `revision` of term
    /// `term`: an engine body is dropped unapplied, a product entry, whose
    /// effect the state holds, is re-asserted at `at_index`. One naming an
    /// id the log does not hold at that index changes no state; it
    /// resolves the id where a replica holds it.
    Void {
        revision: u64,
        term: u64,
        /// Stamped by the leader like a restamp's; `None` from an older
        /// leader, which stamps none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at_index: Option<u64>,
        /// As [`Body::Restamp`]'s.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        generation: Option<bool>,
    },
    ClockVerdict {
        replica: NodeId,
        judged: bool,
    },
    /// The leader's page verdict: `replica`'s hash for `page` lost (plan
    /// 2.11, Withholding and divergence). It turns diverged once it knows
    /// the entry committed, unless its commit start lies after the page.
    Diverged {
        replica: NodeId,
        page: u64,
    },
    /// The leader cannot tell the unresolved id `replica` reported from
    /// the committed entry it purged at that index: `replica` returns to
    /// `starting` and rebuilds its engine from its product state once its
    /// applied index passed `index`.
    PurgePoint {
        replica: NodeId,
        index: u64,
    },
    Disrupt(Option<Disruption>),
    /// Renews the leases of these sessions at the entry's stamping index.
    LeaseRenew(Vec<SessionKey>),
    /// Ends `session`'s lease if it was not renewed after `renewed`, the
    /// renewal the expiry was decided on.
    LeaseExpiry {
        session: SessionKey,
        renewed: u64,
        /// Stamped by the leader like a command's; `None` from an older
        /// leader, which stamps none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at_index: Option<u64>,
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
    /// The protocol the leader writes from here on, [`PROTOCOL`]: appended
    /// once, before the first generation flag. A binary of an older
    /// protocol cannot decode it, so it stops rather than apply such a
    /// flag as one it does not know.
    Protocol(u32),
}

/// The log protocol this binary writes and applies: 2 adds the
/// generation flags ([`Command::generation`]), which a binary of
/// protocol 1 ignores; 3 adds the tone generator's engine entry
/// (`samplez::Entry::Tone`) and targets (`samplez::Targets::tones`),
/// which a product's records carry.
pub const PROTOCOL: u32 = 3;

impl Body {
    /// An entry whose generation every replica builds on receipt: an
    /// input raised or re-routed, a decision flagged so, or commands one
    /// of which the leader ordered on the generation path. A new leader
    /// learns the pending generation from these; one written while the
    /// protocol gate was shut carries no flag and is not counted.
    pub fn needs_generation(&self) -> bool {
        match self {
            Body::InputUp { .. } | Body::Route { .. } => true,
            Body::Restamp { generation, .. } | Body::Void { generation, .. } => {
                *generation == Some(true)
            }
            Body::Commands(commands) => commands.iter().any(|c| c.generation == Some(true)),
            _ => false,
        }
    }

    /// The engine index the entry takes effect at, for the entries the
    /// engine applies rather than the state machine alone.
    pub fn at_index(&self) -> Option<u64> {
        match self {
            Body::InputDown { at_index, .. }
            | Body::InputUp { at_index, .. }
            | Body::Route { at_index, .. }
            | Body::MasterEnable { at_index, .. }
            | Body::Restamp { at_index, .. } => Some(*at_index),
            Body::Void { at_index, .. } | Body::LeaseExpiry { at_index, .. } => *at_index,
            _ => None,
        }
    }

    /// The decision on an unresolved entry: the log index and term it
    /// names.
    pub fn decides(&self) -> Option<(u64, u64)> {
        match self {
            Body::Restamp { revision, term, .. } | Body::Void { revision, term, .. } => {
                Some((*revision, *term))
            }
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

impl Request {
    /// The index the entry takes effect at in the engine: an engine
    /// body's, or the earliest of its commands' once stamped; none for an
    /// entry the state machine alone applies.
    pub fn at_index(&self) -> Option<u64> {
        match &self.body {
            Body::Commands(commands) => commands.iter().filter_map(|c| c.at_index).min(),
            body => body.at_index(),
        }
    }
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
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(bytes))
    }

    /// Decodes the text where the deserializer holds it, without a copy.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct Base64;

        impl Visitor<'_> for Base64 {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("base64 text")
            }

            fn visit_str<E: Error>(self, text: &str) -> Result<Vec<u8>, E> {
                STANDARD.decode(text).map_err(E::custom)
            }
        }

        d.deserialize_str(Base64)
    }
}

/// `ceil(e / block) * block`: an estimate rounded up to a block boundary.
pub fn stamping_index(e: u64, block: u64) -> u64 {
    e.div_ceil(block) * block
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`Body`] as a binary of protocol 1 decodes it: no `Protocol`, and
    /// none of the flags or stamps protocol 2 adds.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    enum Protocol1 {
        Commands(Vec<serde_json::Value>),
        Baseline(Baseline),
        Restamp {
            revision: u64,
            term: u64,
            at_index: u64,
            paths: Vec<String>,
        },
        Void {
            revision: u64,
            term: u64,
        },
        ClockVerdict {
            replica: NodeId,
            judged: bool,
        },
        Diverged {
            replica: NodeId,
            page: u64,
        },
        PurgePoint {
            replica: NodeId,
            index: u64,
        },
        Disrupt(Option<Disruption>),
        LeaseRenew(Vec<SessionKey>),
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
        Route {
            at_index: u64,
            body: String,
        },
        MasterEnable {
            enabled: bool,
            at_index: u64,
        },
    }

    /// A binary of protocol 1 reads the stamps and flags protocol 2 adds
    /// as unknown fields, and fails on the marker: it stops at the first
    /// entry it cannot apply alike instead of applying flags it ignores.
    #[test]
    fn a_protocol_1_binary_fails_on_the_marker() {
        let decode =
            |body: Body| serde_json::from_slice::<Protocol1>(&serde_json::to_vec(&body).unwrap());
        let void = Body::Void {
            revision: 3,
            term: 1,
            at_index: Some(960),
            generation: Some(true),
        };
        assert!(matches!(
            decode(void),
            Ok(Protocol1::Void { revision: 3, .. })
        ));
        let expiry = Body::LeaseExpiry {
            session: SessionKey::from_parts("p", "s"),
            renewed: 0,
            at_index: Some(960),
        };
        assert!(decode(expiry).is_ok());
        assert!(decode(Body::Protocol(PROTOCOL)).is_err());
    }

    /// The protocol is pinned, as `samplez::COMPUTE_VERSION`'s rows pin
    /// what that version stands for: a change to the log's wire format,
    /// the replicated semantics or the page hash and fingerprint
    /// computation bumps it, with the reason added here, and only such a
    /// change does. 2: the generation flags; 3: the tone generator's
    /// entry and targets.
    #[test]
    fn the_protocol_is_pinned() {
        assert_eq!(PROTOCOL, 3);
    }

    /// The entries the leader flagged for a generation need one, so a
    /// leader that only appended them learns the pending generation.
    #[test]
    fn flagged_entries_need_a_generation() {
        let command = |generation| Command {
            origin: Origin {
                principal: "p".into(),
                session: "s".into(),
                role: samplez_service::Role::Operator,
                connection: "c".into(),
            },
            id: "1".into(),
            at_index: Some(960),
            body: Vec::new(),
            generation,
        };
        for (generation, needs) in [(None, false), (Some(false), false), (Some(true), true)] {
            let commands = Body::Commands(vec![command(Some(false)), command(generation)]);
            assert_eq!(commands.needs_generation(), needs);
            let restamp = Body::Restamp {
                revision: 1,
                term: 1,
                at_index: 960,
                paths: Vec::new(),
                generation,
            };
            assert_eq!(restamp.needs_generation(), needs);
            let void = Body::Void {
                revision: 1,
                term: 1,
                at_index: Some(960),
                generation,
            };
            assert_eq!(void.needs_generation(), needs);
        }
    }
}
