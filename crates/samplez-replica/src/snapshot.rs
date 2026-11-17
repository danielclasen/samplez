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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub id: String,
    pub last_applied: Option<StoredLogId>,
    pub membership: StoredMembershipConfig,
    pub state: Replicated,
}
