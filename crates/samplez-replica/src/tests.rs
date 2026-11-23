use super::*;

/// The value of `samplez_replica_version_mismatch` with reason `reason`
/// among `samples`.
fn reason(samples: &[ReplicaMetric], reason: &str) -> f64 {
    let s = samples
        .iter()
        .find(|s| s.labels == [("reason", reason.to_owned())])
        .expect("a sample per reason");
    assert_eq!(s.name, "samplez_replica_version_mismatch");
    s.value
}

/// Each `version_mismatch` reason is exposed under its own label: the
/// one the replica is in reads 1, the other 0, and outside
/// `version_mismatch` both read 0.
#[test]
fn the_version_mismatch_reason_is_exposed_by_its_label() {
    use state::Mismatch::{AwaitingHandOver, BelowBaselineVersions};
    let below = mismatch_samples(Some(BelowBaselineVersions));
    assert_eq!(reason(&below, "below_baseline_versions"), 1.0);
    assert_eq!(reason(&below, "awaiting_hand_over"), 0.0);
    let awaiting = mismatch_samples(Some(AwaitingHandOver));
    assert_eq!(reason(&awaiting, "below_baseline_versions"), 0.0);
    assert_eq!(reason(&awaiting, "awaiting_hand_over"), 1.0);
    let none = mismatch_samples(None);
    assert_eq!(reason(&none, "below_baseline_versions"), 0.0);
    assert_eq!(reason(&none, "awaiting_hand_over"), 0.0);
}

/// The value of `samplez_replica_disagreements_total` of `kind`.
fn disagreements(samples: &[ReplicaMetric], kind: &str) -> f64 {
    let s = samples
        .iter()
        .find(|s| s.labels == [("kind", kind.to_owned())])
        .expect("a sample per kind");
    assert_eq!(s.name, "samplez_replica_disagreements_total");
    s.value
}

/// Each kind of page disagreement the leader counts is exposed under
/// its own label: a cross-key one raises `cross_key` alone, one
/// without a majority among a key's hashes `no_majority` alone.
#[test]
fn disagreements_are_exposed_by_their_kind() {
    let cross = baseline::Counts {
        cross_key_disagreements: 1,
        ..baseline::Counts::default()
    };
    let samples = disagreement_samples(&cross);
    assert_eq!(disagreements(&samples, "cross_key"), 1.0);
    assert_eq!(disagreements(&samples, "no_majority"), 0.0);
    let same = baseline::Counts {
        disagreements: 1,
        ..baseline::Counts::default()
    };
    let samples = disagreement_samples(&same);
    assert_eq!(disagreements(&samples, "cross_key"), 0.0);
    assert_eq!(disagreements(&samples, "no_majority"), 1.0);
}

use crate::state_machine::tests::Paths;
use openraft::error::{InstallSnapshotError, RPCError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::RaftStorage;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
use std::path::PathBuf;

const ID: NodeId = 7;

/// A writer cancelled while it awaits the marker's commit releases
/// the hold, so a later writer appends the marker rather than write
/// every flag unflagged.
#[test]
fn a_cancelled_marker_write_releases_the_hold() {
    let flag = AtomicBool::new(false);
    let mut write = Box::pin(async {
        let _held = Marking::take(&flag).expect("free");
        std::future::pending::<()>().await;
    });
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(write.as_mut().poll(&mut cx).is_pending());
    assert!(Marking::take(&flag).is_none(), "held while it writes");
    drop(write);
    assert!(Marking::take(&flag).is_some(), "released when cancelled");
}

/// A product that drives an engine refuses a command asking for an
/// index beyond the earliest; one that does not keeps it, and an
/// index at or before the earliest, or none, is never refused.
#[test]
fn only_an_engine_product_refuses_a_scheduled_index() {
    assert!(scheduled(true, Some(4848), 4800));
    assert!(!scheduled(true, Some(4800), 4800));
    assert!(!scheduled(true, None, 4800));
    assert!(!scheduled(false, Some(48_000), 4800));
}

/// An entry carries commands asking for one index only: the engine
/// applies a log entry at one index, so a command asking for a later
/// one is not folded into an entry applied earlier.
#[test]
fn commands_asking_for_other_indexes_take_entries_of_their_own() {
    let command = |at_index, bytes: usize| Command {
        generation: None,
        origin: Origin {
            principal: "p".into(),
            session: "s".into(),
            role: samplez_service::Role::Operator,
            connection: "c".into(),
        },
        id: "1".into(),
        at_index,
        body: vec![0; bytes],
    };
    let first = command(None, 10);
    assert!(joins(&first, 1, 10, &command(None, 10)));
    assert!(!joins(&first, 1, 10, &command(Some(4800), 10)));
    assert!(!joins(&command(Some(4800), 10), 1, 10, &command(None, 10)));
    assert!(joins(
        &command(Some(4800), 10),
        1,
        10,
        &command(Some(4800), 10)
    ));
    let size = command(None, 0).size();
    assert!(!joins(&first, 1, MAX_ENTRY_BYTES - size, &command(None, 1)));
    // At most MAX_ENTRY_COMMANDS: the checks before each joins stay
    // bounded.
    assert!(joins(
        &first,
        MAX_ENTRY_COMMANDS - 1,
        10,
        &command(None, 10)
    ));
    assert!(!joins(&first, MAX_ENTRY_COMMANDS, 10, &command(None, 10)));
}

/// A network that reaches no one; a set of one voter sends nothing.
struct Nobody;

fn unreachable<E: std::error::Error>() -> RPCError<NodeId, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("nobody")))
}

impl RaftNetworkFactory<TypeConfig> for Nobody {
    type Network = Nobody;
    async fn new_client(&mut self, _: NodeId, _: &BasicNode) -> Nobody {
        Nobody
    }
}

impl RaftNetwork<TypeConfig> for Nobody {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable())
    }
    async fn install_snapshot(
        &mut self,
        _: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(unreachable())
    }
    async fn vote(
        &mut self,
        _: VoteRequest<NodeId>,
        _: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable())
    }
}

impl PeerClient for Nobody {
    fn call<'a>(
        &'a self,
        _: Option<NodeId>,
        _: &'a str,
        _: PeerRequest,
        _: Duration,
    ) -> BoxFuture<'a, Result<PeerReply, String>> {
        Box::pin(async { Err("nobody".to_owned()) })
    }
}

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("samplez-replica-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open(dir: &Path) -> Store<Paths> {
    Store::open(dir, Arc::new(Paths::default), Limits::defaults(48_000)).expect("opens")
}

async fn start(store: Store<Paths>) -> Replica<Paths> {
    let settings = Settings {
        id: ID,
        epoch: 1,
        limits: Limits::defaults(48_000),
        block: 48,
        write_timeout: Duration::from_secs(2),
        addr: "local".to_owned(),
        peers: vec![],
        voters: None,
    };
    Replica::with_store(
        settings,
        raft_config("lone"),
        store,
        Nobody,
        Arc::new(Nobody),
        Arc::new(|| 0),
    )
    .await
    .expect("starts")
}

/// The replica follows the ramps of plain gain entries at its limits'
/// ramp length and block size, so audio whose engine has others is
/// refused with an error naming both.
#[test]
fn audio_with_another_ramp_length_or_block_is_refused() {
    let limits = Limits::defaults(48_000);
    assert_eq!(
        check_ramp_config(&limits, Some((limits.ramp_samples, limits.block))),
        Ok(())
    );
    assert_eq!(check_ramp_config(&limits, None), Ok(()));
    for engine in [
        (limits.ramp_samples + 1, limits.block),
        (limits.ramp_samples, 96),
    ] {
        let e = check_ramp_config(&limits, Some(engine)).unwrap_err();
        assert!(
            e.contains(&engine.0.to_string()) && e.contains(&engine.1.to_string()),
            "{e}"
        );
        assert!(
            e.contains("ramp_samples") && e.contains("rampSamples"),
            "{e}"
        );
    }
}

/// A vote takes this long to save, longer than openraft's tick of
/// 1.5 heartbeats (75 ms).
const SLOW: Duration = Duration::from_millis(100);

async fn leads(replica: &Replica<Paths>, what: &'static str) {
    let leads = replica
        .raft()
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, what)
        .await;
    assert!(leads.is_ok(), "{leads:?}");
}

/// openraft elects the only voter on a tick it does not lead, and
/// counts its own vote only once the vote is saved. Up to 0.9.25 it
/// also elected while that vote was still being saved, so the next
/// term overtook every slow vote and the replica never led.
#[tokio::test(start_paused = true)]
async fn a_set_of_one_voter_whose_vote_takes_longer_than_the_tick_to_save_still_leads() {
    let dir = dir("lone-fresh");
    let replica = start(open(&dir).with_sync_delay(SLOW)).await;
    replica
        .initialize([(ID, "local".to_owned())])
        .await
        .expect("initializes");
    leads(&replica, "a fresh set of one voter leads").await;
    replica.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// As above, for a volume whose only voter is itself and whose last
/// vote never counted: startup does not restore leadership, so the
/// replica has to elect itself.
#[tokio::test(start_paused = true)]
async fn a_reopened_set_of_one_voter_whose_vote_takes_longer_than_the_tick_to_save_still_leads() {
    let dir = dir("lone-reopened");
    let mut store = open(&dir);
    let membership = Membership::from(BTreeMap::from([(ID, BasicNode::new("local"))]));
    store
        .append_to_log([Entry::<TypeConfig> {
            log_id: LogId::new(CommittedLeaderId::new(0, 0), 0),
            payload: EntryPayload::Membership(membership),
        }])
        .await
        .expect("appends");
    store.save_vote(&Vote::new(1, ID)).await.expect("votes");
    drop(store);
    let replica = start(open(&dir).with_sync_delay(SLOW)).await;
    leads(&replica, "a reopened set of one voter leads").await;
    replica.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_default_timing_is_valid_and_reaches_openraft() {
    assert_eq!(RaftTiming::default(), RaftTiming::DEFAULT);
    assert_eq!(RaftTiming::DEFAULT.validate(), Ok(()));
    let config = raft_config("t");
    let ms = |d: Duration| d.as_millis() as u64;
    assert_eq!(
        (
            config.election_timeout_min,
            config.election_timeout_max,
            config.heartbeat_interval
        ),
        (
            ms(RaftTiming::DEFAULT.election_min),
            ms(RaftTiming::DEFAULT.election_max),
            ms(RaftTiming::DEFAULT.heartbeat)
        ),
        "the heartbeat, which openraft also gives an AppendEntries, is the configured one"
    );
    assert!(config.validate().is_ok(), "openraft accepts it");
}

#[test]
fn a_timing_out_of_order_is_refused() {
    let ms = Duration::from_millis;
    let with = |election_min, election_max, heartbeat| {
        raft_config_with(
            "t",
            &RaftTiming {
                election_min: ms(election_min),
                election_max: ms(election_max),
                heartbeat: ms(heartbeat),
            },
        )
    };
    assert!(with(500, 1000, 100).is_ok());
    assert!(with(500, 1000, 0).is_err(), "no heartbeat");
    assert!(with(500, 1000, 500).is_err(), "heartbeat at the minimum");
    assert!(with(500, 1000, 600).is_err(), "heartbeat past the minimum");
    assert!(with(500, 500, 100).is_err(), "no range to draw from");
    assert!(with(500, 400, 100).is_err(), "maximum below minimum");
}

/// An accepted request counts for the leader and term of its committed
/// vote alone, and only within the lease; a candidate's uncommitted
/// vote is no leader's.
#[tokio::test(start_paused = true)]
async fn a_heard_leader_counts_for_its_own_vote_within_the_lease() {
    use openraft::Vote;
    let lease = Duration::from_secs(2);
    let heard = Heard::default();
    let leader = Vote::new_committed(3, 1);
    assert!(!heard.within(&leader, lease), "nothing taken");
    heard.took(Vote::new(3, 1));
    assert!(!heard.within(&Vote::new(3, 1), lease), "a candidate's vote");
    heard.took(leader);
    assert!(heard.within(&leader, lease));
    assert!(
        !heard.within(&Vote::new_committed(4, 1), lease),
        "a later term"
    );
    assert!(
        !heard.within(&Vote::new_committed(3, 2), lease),
        "another leader"
    );
    tokio::time::advance(lease + Duration::from_millis(1)).await;
    assert!(!heard.within(&leader, lease), "past the lease");
}

/// The leader a replica took is recorded from what its raft answered:
/// an AppendEntries answered Conflict or Success took the sender's vote,
/// one answered HigherVote did not, nor an InstallSnapshot answered
/// with another vote.
#[tokio::test]
async fn rpcs_record_only_a_vote_the_raft_accepted() {
    let dir = dir("rpcs");
    let replica = start(open(&dir)).await;
    let rpcs = replica.rpcs();
    let lease = Duration::from_secs(60);
    let heard = |vote: &Vote<NodeId>| replica.shared.heard.within(vote, lease);
    let append = |vote: Vote<NodeId>, prev: Option<LogId<NodeId>>| AppendEntriesRequest {
        vote,
        prev_log_id: prev,
        entries: Vec::new(),
        leader_commit: None,
    };
    let five = Vote::new_committed(5, 1);
    let behind = LogId::new(CommittedLeaderId::new(5, 1), 10);
    let reply = rpcs.append_entries(append(five, Some(behind))).await;
    assert!(
        matches!(reply, Ok(AppendEntriesResponse::Conflict)),
        "{reply:?}"
    );
    assert!(heard(&five), "a conflict took the vote");
    let six = Vote::new_committed(6, 2);
    let reply = rpcs.append_entries(append(six, None)).await;
    assert!(
        matches!(reply, Ok(AppendEntriesResponse::Success)),
        "{reply:?}"
    );
    assert!(heard(&six));
    let four = Vote::new_committed(4, 1);
    let reply = rpcs.append_entries(append(four, None)).await;
    assert!(
        matches!(reply, Ok(AppendEntriesResponse::HigherVote(_))),
        "{reply:?}"
    );
    assert!(
        !heard(&four) && heard(&six),
        "a refused append records nothing"
    );
    let snapshot = InstallSnapshotRequest {
        vote: four,
        meta: openraft::SnapshotMeta::default(),
        offset: 0,
        data: Vec::new(),
        done: true,
    };
    let reply = rpcs.install_snapshot(snapshot).await.expect("answers");
    assert_eq!(reply.vote, six);
    assert!(
        !heard(&four) && heard(&six),
        "a refused snapshot records nothing"
    );
    replica.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
