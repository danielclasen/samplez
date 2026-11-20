//! What snapshots carry, and openraft's log types as the volume stores
//! them: openraft's own types implement serde only with its `serde`
//! feature, which the pinned default feature set leaves out.

use std::collections::{BTreeMap, BTreeSet};

use openraft::{
    BasicNode, CommittedLeaderId, EntryPayload, LogId, Membership, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::TypeConfig;
use crate::dedup::Saved;
use crate::entry::{Baseline, Body, Disruption, NodeId, Request, SessionKey};

/// The replicated state beside the log: everything a replica that
/// installs a snapshot needs to apply the entries after it alike.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replicated {
    #[serde(with = "crate::entry::bytes")]
    pub product: Vec<u8>,
    /// Entries the engine applies at a future at_index, and the decisions
    /// on them (restamped, voided), by revision.
    pub pending: BTreeMap<u64, Body>,
    /// [`crate::state_machine::StateMachine::terms`].
    #[serde(default)]
    pub terms: BTreeMap<u64, (u64, u64)>,
    /// [`crate::state_machine::StateMachine::restamps`].
    #[serde(default)]
    pub restamps: BTreeMap<u64, (u32, u64)>,
    /// [`crate::state_machine::StateMachine::decided`], as pairs: JSON
    /// keys are strings only.
    #[serde(default)]
    pub decided: Vec<((u64, u64), (u64, u64))>,
    pub baseline: Option<Baseline>,
    /// The latest `ClockVerdict` per replica.
    pub verdicts: BTreeMap<NodeId, bool>,
    /// The replica the latest `ClockVerdict` judging one named; absent
    /// from snapshots written before it was kept.
    #[serde(default)]
    pub latest_judged: Option<NodeId>,
    pub slot: Option<Disruption>,
    pub dedup: Saved,
    /// Each session's last renewal, in session order: JSON keys are
    /// strings only.
    pub leases: Vec<(SessionKey, u64)>,
    /// The sessions whose lifetime ended, with the stamping index it ended
    /// at, until the product forgets them; in session order.
    pub ended: Vec<(SessionKey, u64)>,
    /// The highest stamping index applied.
    pub highest_stamp: u64,
    /// The product revision ([`crate::state_machine::StateMachine::revision`]).
    pub revision: u64,
    /// [`crate::state_machine::StateMachine::floor`]; absent from
    /// snapshots written before it was kept.
    #[serde(default)]
    pub floor: Option<u64>,
    /// [`crate::state_machine::StateMachine::derived`].
    #[serde(default)]
    pub derived: BTreeMap<u64, Vec<u32>>,
    /// [`crate::state_machine::StateMachine::resets`].
    #[serde(default)]
    pub resets: BTreeMap<u64, Vec<u32>>,
    /// [`crate::state_machine::StateMachine::protocol`].
    #[serde(default)]
    pub protocol: u32,
    /// [`crate::state_machine::StateMachine::ramps`] as `(from bits, start,
    /// len)`, so the snapshot stays `Eq`; absent from snapshots written
    /// before it was kept, which then carry no running ramps.
    #[serde(default)]
    pub ramps: Vec<Vec<Option<(u32, u64, u32)>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredLogId {
    pub term: u64,
    pub node: NodeId,
    pub index: u64,
}

impl From<&LogId<NodeId>> for StoredLogId {
    fn from(id: &LogId<NodeId>) -> StoredLogId {
        StoredLogId {
            term: id.leader_id.term,
            node: id.leader_id.node_id,
            index: id.index,
        }
    }
}

impl From<StoredLogId> for LogId<NodeId> {
    fn from(id: StoredLogId) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(id.term, id.node), id.index)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredVote {
    pub term: u64,
    pub node: NodeId,
    pub committed: bool,
}

impl From<&Vote<NodeId>> for StoredVote {
    fn from(vote: &Vote<NodeId>) -> StoredVote {
        StoredVote {
            term: vote.leader_id.term,
            node: vote.leader_id.node_id,
            committed: vote.committed,
        }
    }
}

impl From<StoredVote> for Vote<NodeId> {
    fn from(vote: StoredVote) -> Vote<NodeId> {
        if vote.committed {
            Vote::new_committed(vote.term, vote.node)
        } else {
            Vote::new(vote.term, vote.node)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMembershipConfig {
    pub log_id: Option<StoredLogId>,
    pub configs: Vec<BTreeSet<NodeId>>,
    pub nodes: BTreeMap<NodeId, String>,
}

impl From<&Membership<NodeId, BasicNode>> for StoredMembershipConfig {
    fn from(membership: &Membership<NodeId, BasicNode>) -> StoredMembershipConfig {
        StoredMembershipConfig {
            log_id: None,
            configs: membership.get_joint_config().clone(),
            nodes: membership
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
        }
    }
}

impl StoredMembershipConfig {
    pub fn of(stored: &StoredMembership<NodeId, BasicNode>) -> StoredMembershipConfig {
        StoredMembershipConfig {
            log_id: stored.log_id().as_ref().map(StoredLogId::from),
            ..StoredMembershipConfig::from(stored.membership())
        }
    }

    pub fn membership(&self) -> Membership<NodeId, BasicNode> {
        let nodes: BTreeMap<NodeId, BasicNode> = self
            .nodes
            .iter()
            .map(|(id, addr)| (*id, BasicNode::new(addr)))
            .collect();
        Membership::new(self.configs.clone(), nodes)
    }

    pub fn stored(&self) -> StoredMembership<NodeId, BasicNode> {
        StoredMembership::new(self.log_id.map(LogId::from), self.membership())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredPayload {
    Blank,
    Normal(Request),
    Membership(StoredMembershipConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEntry {
    pub log_id: StoredLogId,
    pub payload: StoredPayload,
}

impl From<&openraft::Entry<TypeConfig>> for StoredEntry {
    fn from(entry: &openraft::Entry<TypeConfig>) -> StoredEntry {
        StoredEntry {
            log_id: StoredLogId::from(&entry.log_id),
            payload: match &entry.payload {
                EntryPayload::Blank => StoredPayload::Blank,
                EntryPayload::Normal(request) => StoredPayload::Normal(request.clone()),
                EntryPayload::Membership(membership) => {
                    StoredPayload::Membership(StoredMembershipConfig::from(membership))
                }
            },
        }
    }
}

impl From<&StoredEntry> for openraft::Entry<TypeConfig> {
    fn from(entry: &StoredEntry) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: entry.log_id.into(),
            payload: match &entry.payload {
                StoredPayload::Blank => EntryPayload::Blank,
                StoredPayload::Normal(request) => EntryPayload::Normal(request.clone()),
                StoredPayload::Membership(membership) => {
                    EntryPayload::Membership(membership.membership())
                }
            },
        }
    }
}

/// A snapshot as the volume stores it and a peer receives it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "Wire<Replicated>")]
pub struct StoredSnapshot {
    pub id: String,
    pub last_applied: Option<StoredLogId>,
    pub membership: StoredMembershipConfig,
    pub state: Replicated,
    /// The product configuration hash of the replica that sent it
    /// ([`crate::state_machine::Product::config_hash`]); none from a
    /// replica that did not record one.
    pub config: Option<u64>,
}

/// The first protocol whose log may hold generation flags.
const FLAGGED: u32 = 2;

/// [`StoredSnapshot`] as encoded: the state of a log that reached
/// [`FLAGGED`] goes under `flagged` instead of `state`, which a binary of
/// protocol 1 requires, so that binary refuses the snapshot rather than
/// apply the flags after it as unknown fields. The log marker that stops it
/// is gone once compacted into the snapshot.
#[derive(Serialize, Deserialize)]
struct Wire<S> {
    id: String,
    last_applied: Option<StoredLogId>,
    membership: StoredMembershipConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<S>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flagged: Option<S>,
    /// None from a replica that did not record one, the field absent.
    config: Option<u64>,
}

impl Serialize for StoredSnapshot {
    fn serialize<Z: serde::Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        let flagged = self.state.protocol >= FLAGGED;
        Wire {
            id: self.id.clone(),
            last_applied: self.last_applied,
            membership: self.membership.clone(),
            state: (!flagged).then_some(&self.state),
            flagged: flagged.then_some(&self.state),
            config: self.config,
        }
        .serialize(serializer)
    }
}

impl TryFrom<Wire<Replicated>> for StoredSnapshot {
    type Error = String;

    fn try_from(wire: Wire<Replicated>) -> Result<StoredSnapshot, String> {
        let state = match (wire.state, wire.flagged) {
            (Some(state), None) | (None, Some(state)) => state,
            _ => return Err("a snapshot holds one state, `state` or `flagged`".to_owned()),
        };
        Ok(StoredSnapshot {
            id: wire.id,
            last_applied: wire.last_applied,
            membership: wire.membership,
            state,
            config: wire.config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`StoredSnapshot`] as a binary of protocol 1 decodes it. Its
    /// `Replicated` lacks `protocol`, a field it ignores as unknown, so
    /// this one stands in for it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct Protocol1 {
        id: String,
        last_applied: Option<StoredLogId>,
        membership: StoredMembershipConfig,
        state: Replicated,
        #[serde(default)]
        config: Option<u64>,
    }

    fn snapshot(protocol: u32) -> StoredSnapshot {
        StoredSnapshot {
            id: "s".into(),
            last_applied: None,
            membership: StoredMembershipConfig::default(),
            state: Replicated {
                protocol,
                ..Replicated::default()
            },
            config: Some(1),
        }
    }

    /// A binary of protocol 1 reads a snapshot of a log the marker has
    /// not reached and refuses one of a log past it, where generation
    /// flags may follow; this binary reads both as written.
    #[test]
    fn a_protocol_1_binary_refuses_a_snapshot_of_a_flagged_log() {
        for protocol in [0, 1, 2, 3] {
            let bytes = serde_json::to_vec(&snapshot(protocol)).expect("encodes");
            let old = serde_json::from_slice::<Protocol1>(&bytes);
            assert_eq!(old.is_ok(), protocol < 2, "protocol {protocol}: {old:?}");
            let decoded: StoredSnapshot = serde_json::from_slice(&bytes).expect("decodes");
            assert_eq!(decoded, snapshot(protocol));
        }
        let mut both = serde_json::to_value(snapshot(2)).expect("encodes");
        let state = both["flagged"].clone();
        both["state"] = state;
        assert!(serde_json::from_value::<StoredSnapshot>(both).is_err());
    }
}
