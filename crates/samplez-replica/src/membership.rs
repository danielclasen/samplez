//! Who votes: a node id from the voter's slot and its volume, the voters
//! a set wants, a new volume joining as a learner, and the leader
//! reconciling the voters with the wanted ones.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers, RaftMetrics, ServerState};

use crate::Shared;
use crate::bootstrap::{self, Bootstrap, Marker, Step, Volume};
use crate::entry::NodeId;
use crate::peer::{PeerReply, PeerRequest, PeerResponse, Refused};

/// How often the leader compares the voters with the wanted ones, beside
/// each accepted join; and how often a replica checks that a leader
/// counts it, or an empty volume reads the marker and asks to join.
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
/// among the voters, and the slot's other nodes go: a learner of a slot
/// that has a voter exists only because its replica came back on a new
/// volume, so the slot's voter is the lost volume. Several caught-up
/// learners of one slot (a volume lost again before it was promoted) give
/// the highest id the place, the newest volume ([`bootstrap::volume_id`]);
/// one of them that is gone is replaced in a later round. Voters of slots the set
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
    /// Takes a replica of the set `cluster` in as a learner; the leader
    /// only. A replica the membership holds already is answered as joined.
    pub(crate) async fn admit(&self, id: NodeId, addr: String, cluster: u64) -> PeerReply {
        if !self.raft.is_initialized().await.unwrap_or(false) {
            return Err(Refused::NotMember);
        }
        if cluster != self.epoch() {
            return Err(Refused::OtherCluster);
        }
        if self
            .settings
            .voters
            .is_some_and(|want| !want.wants(slot_of(id)))
        {
            return Err(Refused::NotWanted);
        }
        let Some(_proposing) = self.propose() else {
            return Err(self.refusal());
        };
        let membership = self.raft.metrics().borrow().membership_config.clone();
        if membership.membership().get_node(&id).is_some() {
            return Ok(PeerResponse::Joined);
        }
        match self.raft.add_learner(id, BasicNode::new(addr), false).await {
            Ok(_) => {
                self.reconcile.notify_one();
                Ok(PeerResponse::Joined)
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(Refused::NotLeader)
            }
            Err(e) => Err(crate::unavailable(&e)),
        }
    }

    /// Asks every other wanted voter to take this replica in under
    /// `cluster`; true when one of them answered as a member of a set.
    async fn ask_to_join(&self, cluster: u64) -> bool {
        let mut member = false;
        for addr in self
            .settings
            .peers
            .iter()
            .filter(|a| **a != self.settings.addr)
        {
            let join = PeerRequest::Join {
                id: self.settings.id,
                addr: self.settings.addr.clone(),
                cluster,
            };
            match self.peers.call(None, addr, join, PERIOD).await {
                Ok(Ok(PeerResponse::Joined)) => return true,
                Ok(Err(Refused::NotMember)) | Err(_) => {}
                Ok(_) => member = true,
            }
        }
        member
    }

    /// Whether this replica may start a new set: no other wanted voter
    /// answers as a member of one, and a majority of the wanted voters,
    /// this one included, answered that it is not.
    async fn alone(&self) -> bool {
        let others: Vec<&String> = self
            .settings
            .peers
            .iter()
            .filter(|a| **a != self.settings.addr)
            .collect();
        let mut answered = 1;
        for addr in &others {
            match self
                .peers
                .call(None, addr, PeerRequest::Member, PERIOD)
                .await
            {
                Ok(Ok(PeerResponse::Member(true))) => return false,
                Ok(Ok(PeerResponse::Member(false))) => answered += 1,
                _ => {}
            }
        }
        answered * 2 > others.len() + 1
    }

    /// Takes on `cluster` while the volume is empty: nothing was applied
    /// under the previous one.
    fn adopt(&self, cluster: u64, volume: &Volume) -> std::io::Result<()> {
        if cluster != self.epoch() {
            volume.set_cluster(cluster)?;
            self.epoch.store(cluster, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Whether a leader counts this replica as a voter: it leads with a
    /// quorum's acknowledgement within the lease, or it heard from its
    /// leader within the lease, since it started, and its membership has
    /// it as a voter. A vote saved before a restart names a leader the
    /// replica has not heard since, itself included, and openraft dates it
    /// to the start.
    async fn counted_now(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state == ServerState::Leader {
            return self.quorum().await;
        }
        let voter = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == self.settings.id);
        let (lease, started) = (self.lease, self.started);
        voter
            && metrics.current_leader.is_some()
            && self
                .raft
                .with_raft_state(move |state| {
                    state.vote_ref().committed
                        && state
                            .vote_last_modified()
                            .is_some_and(|at| at > started && at.elapsed() <= lease)
                })
                .await
                .unwrap_or(false)
    }
}

/// How long no leader counts a replica as a voter before it is not
/// commitable and asks to join again: past an election, which takes
/// about a second.
pub const UNCOUNTED: Duration = Duration::from_secs(3);

/// How often a replica checks that a leader counts it.
const TICK: Duration = Duration::from_millis(100);

/// Rounds in which an empty volume found no member of a set before its
/// status names the marker to delete.
pub const NO_MEMBER_ROUNDS: u32 = 5;

/// Whether a leader counts this replica as a voter, from one check per
/// tick. A replica is counted from a check that finds it counted. Once
/// counted it stays counted, while it leads or its membership has it as
/// a voter, through ticks in which its applied index advanced: an entry
/// committed, so a quorum took it whatever the age of the last
/// acknowledgement or append openraft reports. Progress alone never
/// counts a replica: one restarted from its volume applies its own log
/// without any leader. It stops being counted after [`UNCOUNTED`] of
/// consecutive checks without either, counted in checks rather than wall
/// time: one check that ran late, after a pause of the runtime that held
/// up heartbeats and acknowledgements with it, is one check of thirty.
#[derive(Debug, Default)]
pub(crate) struct Counting {
    counted: bool,
    uncounted: u32,
    applied: Option<u64>,
}

impl Counting {
    const TICKS: u32 = (UNCOUNTED.as_millis() / TICK.as_millis()) as u32;

    /// Takes one tick's check: `found` what [`Shared::counted_now`] found,
    /// `metrics` replica `id`'s own. Returns whether it is counted.
    pub(crate) fn check(
        &mut self,
        found: bool,
        metrics: &RaftMetrics<NodeId, BasicNode>,
        id: NodeId,
    ) -> bool {
        let voter = metrics.state == ServerState::Leader
            || metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|voter| voter == id);
        let applied = voter.then(|| metrics.last_applied.map_or(0, |log| log.index));
        let progressed = matches!((self.applied, applied), (Some(was), Some(now)) if now > was);
        self.applied = applied;
        if found || progressed && self.counted {
            self.counted = true;
            self.uncounted = 0;
        } else {
            self.uncounted = self.uncounted.saturating_add(1);
            if self.lapsed() {
                self.counted = false;
            }
        }
        self.counted
    }

    /// No check found it counted for [`UNCOUNTED`] of ticks.
    pub(crate) fn lapsed(&self) -> bool {
        self.uncounted >= Self::TICKS
    }
}

/// Keeps a replica in a set. An empty volume starts or joins the set the
/// marker names (`bootstrap`) a round per [`PERIOD`], reading the marker
/// every round, so a deleted or replaced marker takes effect without a
/// restart. A replica with a log is commitable while a leader counts it
/// as a voter ([`Counting`]), checked each 100 ms; once it was not
/// counted for [`UNCOUNTED`] it asks to join again under its own id each
/// [`PERIOD`], as one removed from the voters while it was down must.
pub(crate) async fn watch(shared: Arc<Shared>, bootstrap: Bootstrap) {
    let mut empty_rounds = 0;
    let mut counting = Counting::default();
    let mut asked_at: Option<Instant> = None;
    loop {
        if shared.raft.is_initialized().await.unwrap_or(true) {
            let metrics = shared.raft.metrics().borrow().clone();
            let found = shared.counted_now().await;
            let counted = counting.check(found, &metrics, shared.settings.id);
            shared.counted.store(counted, Ordering::SeqCst);
            if counted {
                shared.set_status(None);
            } else {
                let learner = metrics
                    .membership_config
                    .membership()
                    .learner_ids()
                    .any(|id| id == shared.settings.id);
                let status = if learner {
                    "a learner, catching up before a leader makes it a voter"
                } else if counting.lapsed() {
                    "no leader counted this replica as a voter for 3 s; asking to join as a learner"
                } else {
                    "not counted as a voter yet"
                };
                shared.set_status(Some(status.into()));
                let now = Instant::now();
                if counting.lapsed() && asked_at.is_none_or(|at| now - at >= PERIOD) {
                    asked_at = Some(now);
                    shared.ask_to_join(shared.epoch()).await;
                }
            }
            tokio::time::sleep(TICK).await;
        } else {
            let started = start(&shared, &bootstrap, &mut empty_rounds).await;
            shared.set_status(started.err());
            tokio::time::sleep(PERIOD).await;
        }
    }
}

/// One round of an empty volume: [`bootstrap::step`] on the marker.
async fn start(
    shared: &Shared,
    bootstrap: &Bootstrap,
    empty_rounds: &mut u32,
) -> Result<(), String> {
    let markers = &bootstrap.markers;
    let marker = markers
        .get()
        .await
        .map_err(|e| format!("reading ConfigMap {}: {e}", markers.name()))?;
    let io = |e: std::io::Error| e.to_string();
    let refuse = || {
        format!(
            "another replica answers as a member of a set, though ConfigMap {} names none \
             to join; not starting a second one",
            markers.name()
        )
    };
    match bootstrap::step(bootstrap.slot, bootstrap.volume.id, marker) {
        Step::Wait => Err(format!(
            "waiting for ordinal 0 to create ConfigMap {}",
            markers.name()
        )),
        Step::Create => {
            if !shared.alone().await {
                return Err(refuse());
            }
            let cluster = bootstrap::random_id().map_err(io)?;
            let marker = Marker {
                cluster,
                volume: bootstrap.volume.id,
            };
            // Lost to another creator: read theirs next round.
            if markers.create(marker).await? {
                shared.adopt(cluster, &bootstrap.volume).map_err(io)?;
                shared.initialize_alone().await?;
            }
            Ok(())
        }
        Step::Initialize { cluster } => {
            if !shared.alone().await {
                return Err(refuse());
            }
            shared.adopt(cluster, &bootstrap.volume).map_err(io)?;
            shared.initialize_alone().await
        }
        Step::Join { cluster } => {
            shared.adopt(cluster, &bootstrap.volume).map_err(io)?;
            if shared.ask_to_join(cluster).await {
                *empty_rounds = 0;
            } else {
                *empty_rounds += 1;
            }
            if *empty_rounds >= NO_MEMBER_ROUNDS {
                return Err(format!(
                    "no replica answered as a member of the set ConfigMap {} names in {} \
                     rounds; with no voter left holding a log, nothing takes this replica in: \
                     delete the ConfigMap to start a new set",
                    markers.name(),
                    empty_rounds
                ));
            }
            Err(format!(
                "joining as a learner through {}",
                shared.settings.peers.join(", ")
            ))
        }
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
        let Some(_proposing) = shared.propose() else {
            continue;
        };
        // A leader elected between the two steps of a change finds the
        // joint configuration and has to make the second step itself.
        if let [.., last] = &membership.get_joint_config()[..]
            && membership.get_joint_config().len() > 1
        {
            let finish = ChangeMembers::ReplaceAllVoters(last.clone());
            if let Err(e) = shared.raft.change_membership(finish, false).await {
                tracing::warn!(error = %e, "leaving a joint configuration");
            }
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

    const ME: NodeId = 7;

    /// Replica [`ME`]'s metrics: a follower, a voter or a learner, that
    /// applied up to `applied`.
    fn metrics(voter: bool, applied: u64) -> RaftMetrics<NodeId, BasicNode> {
        let (voters, learners) = if voter {
            (BTreeSet::from([ME, 8, 9]), BTreeSet::new())
        } else {
            (BTreeSet::from([8, 9]), BTreeSet::from([ME]))
        };
        let nodes: BTreeMap<NodeId, BasicNode> = voters
            .iter()
            .chain(&learners)
            .map(|&id| (id, BasicNode::new(id.to_string())))
            .collect();
        let mut metrics = RaftMetrics::new_initial(ME);
        metrics.state = ServerState::Follower;
        metrics.membership_config = Arc::new(openraft::StoredMembership::new(
            None,
            openraft::Membership::new(vec![voters], nodes),
        ));
        metrics.last_applied = Some(openraft::LogId::new(
            openraft::CommittedLeaderId::new(1, 8),
            applied,
        ));
        metrics
    }

    /// `n` checks of a voter with `found` and no progress; the result of
    /// the last.
    fn ticks(counting: &mut Counting, n: u32, found: bool) -> bool {
        (0..n).fold(false, |_, _| counting.check(found, &metrics(true, 0), ME))
    }

    #[test]
    fn a_replica_is_uncounted_only_after_uncounted_of_ticks() {
        let mut counting = Counting::default();
        assert!(
            !ticks(&mut counting, 1, false),
            "not counted before a check finds it"
        );
        assert!(ticks(&mut counting, 1, true));
        assert!(
            ticks(&mut counting, Counting::TICKS - 1, false),
            "still counted"
        );
        assert!(!counting.lapsed());
        assert!(!ticks(&mut counting, 1, false));
        assert!(counting.lapsed());
        assert!(ticks(&mut counting, 1, true), "counted again at once");
        assert!(!counting.lapsed());
    }

    #[test]
    fn a_voter_whose_applied_index_advances_stays_counted() {
        let mut counting = Counting::default();
        let check = |counting: &mut Counting, found, applied| {
            counting.check(found, &metrics(true, applied), ME)
        };
        assert!(!check(&mut counting, false, 0));
        assert!(
            !check(&mut counting, false, 5),
            "progress alone: a restart replaying its own log"
        );
        assert!(check(&mut counting, true, 5));
        for tick in 1..10 * Counting::TICKS {
            let applied = 5 + u64::from(tick / 10);
            assert!(
                check(&mut counting, false, applied),
                "an entry committed every 10 ticks, at tick {tick}"
            );
        }
        let applied = 5 + u64::from(Counting::TICKS);
        assert!(check(&mut counting, true, applied));
        for _ in 0..Counting::TICKS - 1 {
            assert!(check(&mut counting, false, applied));
        }
        assert!(
            !check(&mut counting, false, applied),
            "no commit and no acknowledgement"
        );
    }

    #[test]
    fn a_learner_whose_applied_index_advances_is_not_kept_counted() {
        let mut counting = Counting::default();
        assert!(counting.check(true, &metrics(true, 1), ME));
        for tick in 1..=Counting::TICKS {
            let counted = counting.check(false, &metrics(false, 1 + u64::from(tick)), ME);
            assert_eq!(
                counted,
                tick < Counting::TICKS,
                "removed from the voters, at tick {tick}"
            );
        }
    }

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
