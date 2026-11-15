//! Who votes: a node id from the voter's slot and its volume, the voters
//! a set wants, a new volume joining as a learner, and the leader
//! reconciling the voters with the wanted ones.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers, ServerState};

use crate::Shared;
use crate::entry::NodeId;
use crate::peer::{PeerReply, PeerRequest, PeerResponse, Refused};

/// How often the leader compares the voters with the wanted ones, beside
/// each accepted join; and how often a new volume asks to join.
pub const PERIOD: Duration = Duration::from_secs(1);

/// The low bits of a node id that come from its volume.
const VOLUME_BITS: u32 = 48;
const VOLUME_MASK: u64 = (1 << VOLUME_BITS) - 1;
const WITNESS: u64 = 0xffff;

/// The place a voter fills in the set: an audio replica's StatefulSet
/// ordinal, or the witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Slot {
    Audio(u16),
    Witness,
}

/// The node id of the voter in `slot` on the volume with id `volume`: the
/// slot in the top 16 bits, the volume's low 48 bits below, so a replica
/// that lost its volume comes back as a new node.
pub fn node_id(slot: Slot, volume: u64) -> NodeId {
    let slot = match slot {
        Slot::Audio(ordinal) => {
            assert!(
                u64::from(ordinal) < WITNESS,
                "ordinal {ordinal} out of range"
            );
            u64::from(ordinal)
        }
        Slot::Witness => WITNESS,
    };
    slot << VOLUME_BITS | volume & VOLUME_MASK
}

pub fn slot_of(id: NodeId) -> Slot {
    match id >> VOLUME_BITS {
        WITNESS => Slot::Witness,
        ordinal => Slot::Audio(ordinal as u16),
    }
}

/// The voters a set wants: one per audio replica and the witness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Voters {
    pub replicas: u16,
    pub witness: bool,
}

impl Voters {
    /// Refuses an even number of voters: two halves could not tell which
    /// one holds the quorum.
    pub fn new(replicas: u16, witness: bool) -> Result<Voters, String> {
        let count = u32::from(replicas) + u32::from(witness);
        if replicas == 0 || count % 2 == 0 || u64::from(replicas) >= WITNESS {
            return Err(format!(
                "{replicas} replicas {} the witness are {count} voters; the count must be odd",
                if witness { "with" } else { "without" }
            ));
        }
        Ok(Voters { replicas, witness })
    }

    pub fn wants(&self, slot: Slot) -> bool {
        match slot {
            Slot::Audio(ordinal) => ordinal < self.replicas,
            Slot::Witness => self.witness,
        }
    }
}

/// A membership change the leader makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub voters: BTreeSet<NodeId>,
    /// Nodes no longer needed: replaced voters' slot-mates among the
    /// learners, and learners of slots the set no longer wants.
    pub remove: BTreeSet<NodeId>,
}

/// The next membership for `voters`, `None` when it is the wanted one or
/// nothing can be done yet.
///
/// A learner of a wanted slot that has caught up takes the slot's place
/// among the voters, and the slot's other nodes go: a learner only exists
/// because its replica came back on a new volume, so the slot's voter is
/// the lost volume. Several caught-up learners of one slot (a volume lost
/// again before it was promoted) give the highest id the place; one of
/// them that is gone is replaced in a later round. Voters of slots the set
/// no longer wants go. The count stays odd: when it would be even, one
/// learner filling an empty slot waits for the next round, or else one
/// removal does, so a set starting from its first voter grows by two.
pub fn plan(
    voters: &BTreeSet<NodeId>,
    learners: &BTreeSet<NodeId>,
    caught_up: &BTreeSet<NodeId>,
    want: Voters,
) -> Option<Change> {
    let by_slot = |ids: &BTreeSet<NodeId>| {
        let mut slots: BTreeMap<Slot, Vec<NodeId>> = BTreeMap::new();
        for &id in ids {
            slots.entry(slot_of(id)).or_default().push(id);
        }
        slots
    };
    let (held, joined) = (by_slot(voters), by_slot(caught_up));
    let mut next = BTreeSet::new();
    let mut remove: BTreeSet<NodeId> = learners
        .iter()
        .copied()
        .filter(|&id| !want.wants(slot_of(id)))
        .collect();
    let (mut added, mut removed) = (Vec::new(), Vec::new());
    for (&slot, ids) in &held {
        match joined.get(&slot) {
            Some(new) if want.wants(slot) => {
                let new = *new.iter().max().expect("not empty");
                next.insert(new);
                remove.extend(
                    learners
                        .iter()
                        .filter(|&&id| slot_of(id) == slot && id != new),
                );
            }
            _ if want.wants(slot) => next.extend(ids),
            _ => removed.extend(ids),
        }
    }
    for (slot, ids) in &joined {
        if want.wants(*slot) && !held.contains_key(slot) {
            added.push(*ids.iter().max().expect("not empty"));
        }
    }
    while (next.len() + added.len()) % 2 == 0 && !(added.is_empty() && removed.is_empty()) {
        if added.pop().is_none() {
            next.insert(removed.pop().expect("one of the two is not empty"));
        }
    }
    next.extend(added);
    let removable: BTreeSet<NodeId> = remove.difference(&next).copied().collect();
    if next == *voters && removable.is_empty() {
        return None;
    }
    Some(Change {
        voters: next,
        remove: removable,
    })
}

impl Shared {
    /// Takes a new volume in as a learner; the leader only.
    pub(crate) async fn admit(&self, id: NodeId, addr: String) -> PeerReply {
        if !self.leads() {
            return Err(Refused::NotLeader);
        }
        match self.raft.add_learner(id, BasicNode::new(addr), false).await {
            Ok(_) => {
                self.reconcile.notify_one();
                Ok(PeerResponse::Joined)
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(Refused::NotLeader)
            }
            Err(e) => Err(Refused::Unavailable(e.to_string())),
        }
    }
}

/// Asks the set's replicas to take this one in, until it holds the
/// membership. The status names `marker`: with no voter left holding a
/// log, nothing takes it in, and only deleting the marker lets ordinal 0
/// start a new set.
pub(crate) async fn join(shared: Arc<Shared>, marker: String) {
    let settings = &shared.settings;
    loop {
        if shared.raft.is_initialized().await.unwrap_or(true) {
            shared.set_status(None);
            return;
        }
        shared.set_status(Some(format!(
            "joining as a learner through {}; with no voter holding a log, nothing takes it in: \
             delete ConfigMap {marker} to start a new set",
            settings.peers.join(", ")
        )));
        for addr in settings.peers.iter().filter(|addr| **addr != settings.addr) {
            let join = PeerRequest::Join {
                id: settings.id,
                addr: settings.addr.clone(),
            };
            if let Ok(Ok(PeerResponse::Joined)) = shared.peers.call(None, addr, join, PERIOD).await
            {
                break;
            }
        }
        tokio::time::sleep(PERIOD).await;
    }
}

/// On the leader, makes the voters the wanted ones, a round per
/// [`PERIOD`] or accepted join.
pub(crate) async fn reconcile(shared: Arc<Shared>, want: Voters) {
    loop {
        tokio::select! {
            _ = shared.reconcile.notified() => {}
            _ = tokio::time::sleep(PERIOD) => {}
        }
        let metrics = shared.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        if metrics.state != ServerState::Leader
            || shared.quiescing()
            || membership.get_joint_config().len() > 1
        {
            continue;
        }
        let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
        let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
        let applied = metrics.last_applied.map_or(0, |id| id.index);
        let matched = metrics.replication.unwrap_or_default();
        let caught_up = learners
            .iter()
            .copied()
            .filter(|id| {
                matched
                    .get(id)
                    .copied()
                    .flatten()
                    .is_some_and(|m| m.index >= applied)
            })
            .collect();
        let Some(change) = plan(&voters, &learners, &caught_up, want) else {
            continue;
        };
        tracing::info!(?voters, next = ?change.voters, remove = ?change.remove, "reconciling the voters");
        if change.voters != voters
            && let Err(e) = shared
                .raft
                .change_membership(ChangeMembers::ReplaceAllVoters(change.voters), false)
                .await
        {
            tracing::warn!(error = %e, "changing the voters");
            continue;
        }
        if !change.remove.is_empty()
            && let Err(e) = shared
                .raft
                .change_membership(ChangeMembers::RemoveNodes(change.remove), false)
                .await
        {
            tracing::warn!(error = %e, "removing nodes");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audio(ordinal: u16, volume: u64) -> NodeId {
        node_id(Slot::Audio(ordinal), volume)
    }

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    const THREE: Voters = Voters {
        replicas: 3,
        witness: false,
    };

    #[test]
    fn a_node_id_names_its_slot_and_a_new_volume_is_a_new_node() {
        let (witness, last) = (node_id(Slot::Witness, 7), audio(0xfffe, 7));
        assert_eq!(slot_of(witness), Slot::Witness);
        assert_eq!(slot_of(last), Slot::Audio(0xfffe));
        assert_eq!(slot_of(audio(2, u64::MAX)), Slot::Audio(2));
        assert_ne!(audio(0, 1), audio(0, 2));
        assert_ne!(audio(0, 1), audio(1, 1));
    }

    #[test]
    fn an_even_voter_count_is_refused() {
        for (replicas, witness) in [(2, false), (1, true), (4, false), (3, true), (0, false)] {
            assert!(
                Voters::new(replicas, witness).is_err(),
                "{replicas} {witness}"
            );
        }
        for (replicas, witness) in [(1, false), (2, true), (3, false), (4, true)] {
            assert!(
                Voters::new(replicas, witness).is_ok(),
                "{replicas} {witness}"
            );
        }
    }

    #[test]
    fn a_set_grows_from_its_first_voter_by_two() {
        let first = audio(0, 1);
        let (one, two) = (audio(1, 2), audio(2, 3));
        let learners = set(&[one, two]);
        assert_eq!(plan(&set(&[first]), &learners, &set(&[one]), THREE), None);
        let change = plan(&set(&[first]), &learners, &learners, THREE).unwrap();
        assert_eq!(change.voters, set(&[first, one, two]));
        assert!(change.remove.is_empty());
        assert_eq!(plan(&change.voters, &set(&[]), &set(&[]), THREE), None);
    }

    #[test]
    fn a_volume_lost_on_ordinal_0_rejoins_in_its_place() {
        let (old, one, two, new) = (audio(0, 1), audio(1, 2), audio(2, 3), audio(0, 4));
        let voters = set(&[old, one, two]);
        assert_eq!(
            plan(&voters, &set(&[new]), &set(&[]), THREE),
            None,
            "still catching up"
        );
        let change = plan(&voters, &set(&[new]), &set(&[new]), THREE).unwrap();
        assert_eq!(change.voters, set(&[new, one, two]));
    }

    #[test]
    fn of_two_learners_of_one_slot_the_promoted_one_keeps_it() {
        let (old, one, two) = (audio(0, 1), audio(1, 2), audio(2, 3));
        let (lost, new) = (audio(0, 5), audio(0, 9));
        let voters = set(&[old, one, two]);
        let change = plan(&voters, &set(&[lost, new]), &set(&[lost, new]), THREE).unwrap();
        assert_eq!(change.voters, set(&[new, one, two]));
        assert_eq!(change.remove, set(&[lost]));
    }

    #[test]
    fn scaling_down_removes_two_voters_at_once_and_unwanted_learners() {
        let five: Vec<NodeId> = (0..5).map(|o| audio(o, u64::from(o) + 1)).collect();
        let stray = audio(4, 99);
        let change = plan(&set(&five), &set(&[stray]), &set(&[]), THREE).unwrap();
        assert_eq!(change.voters, set(&five[..3]));
        assert_eq!(change.remove, set(&[stray]));
    }

    #[test]
    fn the_count_stays_odd_when_a_slot_is_dropped_and_another_filled() {
        // Wanted: ordinals 0 and 1 and the witness. Ordinal 2 goes, the
        // witness joins: three voters again, in one change.
        let want = Voters::new(2, true).unwrap();
        let voters = set(&[audio(0, 1), audio(1, 2), audio(2, 3)]);
        let witness = node_id(Slot::Witness, 4);
        let change = plan(&voters, &set(&[witness]), &set(&[witness]), want).unwrap();
        assert_eq!(change.voters, set(&[audio(0, 1), audio(1, 2), witness]));
        // Without the witness yet, ordinal 2 stays: two voters would be even.
        assert_eq!(plan(&voters, &set(&[]), &set(&[]), want), None);
    }
}
