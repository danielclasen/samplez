//! Resync by state transfer, and the snapshot install that rebuilds an
//! engine from product state, in an in-process set: every audio replica's
//! engine is a [`harness::FakeAudio`] whose state divergence is an open
//! glitch, gone once a donor's state is handed over or the engine is
//! rebuilt.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Clock, FOR_GOOD, FakeAudio, Options, Set, owner};
use samplez::blocks::PAGE;
use samplez_replica::contact::{Audio, Timing, index_at};
use samplez_replica::entry::{Baseline, Body, NodeId, Request};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::resync::{CoreReply, Missed, Source};
use samplez_replica::state::ReplicaState;
use samplez_replica::state_machine::Record;
use samplez_service::ReplicatedState;
use tokio::time::{Instant, sleep};

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

/// A set whose fakes copy and take cores; `install` lets a test force a
/// snapshot install.
async fn start(ids: Vec<NodeId>, install: bool) -> Set {
    start_timed(ids, install, Timing::defaults(BLOCK)).await
}

/// [`start`] with the contact timers `timing`.
async fn start_timed(ids: Vec<NodeId>, install: bool, timing: Timing) -> Set {
    let mut options = Options {
        ids: Some(ids),
        contact: Some(timing),
        outputs: 1,
        ..Options::default()
    };
    if install {
        options.raft.snapshot_policy = openraft::SnapshotPolicy::Never;
        options.raft.max_in_snapshot_log_to_keep = 0;
        options.raft.purge_batch_size = 1;
    }
    let set = Set::start(options).await;
    for id in audio(&set) {
        set.audio(id).lock().resyncs = true;
    }
    let ids = audio(&set);
    until("committing", 5, || {
        ids.iter()
            .all(|&id| state(&set, id) == Some(ReplicaState::Committing))
    })
    .await;
    set
}

fn audio(set: &Set) -> Vec<NodeId> {
    set.running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {secs} s");
        sleep(Duration::from_millis(1)).await;
    }
}

fn state(set: &Set, id: NodeId) -> Option<ReplicaState> {
    set.replica(id).contact_metrics()?.replica?.state
}

fn resyncs(set: &Set, id: NodeId) -> samplez_replica::resync::Metrics {
    set.replica(id).contact_metrics().unwrap().resync
}

fn baseline(set: &Set, id: NodeId) -> Option<Baseline> {
    set.replica(id).store().with_state(|sm| sm.baseline.clone())
}

fn due(set: &Set, id: NodeId) -> u64 {
    set.audio(id).due().unwrap()
}

/// Whether `id` committed every sample of the page after block `k`.
fn committed_after(set: &Set, id: NodeId, k: u64) -> bool {
    let p = ((k + 1) * BLOCK).div_ceil(PAGE);
    let w = set.audio(id).writes(0);
    (p * PAGE..(p + 1) * PAGE).all(|i| w.has(i))
}

/// Waits until `id` passes through `want`, polling every millisecond.
async fn passes(set: &Set, id: NodeId, want: ReplicaState, secs: u64) {
    until(&format!("{id} {want:?}"), secs, || {
        state(set, id) == Some(want)
    })
    .await;
}

/// State transfer end to end: a replica whose state diverged for good
/// is diverged by the leader's verdict, takes the baseline's core, hands
/// the replayed state over and commits again once a page after the
/// hand-over matches; it never resumes on its own state.
#[tokio::test(start_paused = true)]
async fn a_diverged_replica_resyncs_from_the_baseline_and_commits_again() {
    let set = start(vec![a(0), a(1), a(2)], false).await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let odd = audio(&set).into_iter().find(|&id| id != named).unwrap();
    let k = due(&set, odd) + 30;
    set.audio(odd).glitch(k, FOR_GOOD, 0.5);
    passes(&set, odd, ReplicaState::Diverged, 2).await;
    passes(&set, odd, ReplicaState::Committing, 5).await;
    let m = resyncs(&set, odd);
    assert!(m.handovers >= 1, "{m:?}");
    assert!(!m.budget_exceeded, "{m:?}");
    assert!(
        set.audio(named).lock().cores_copied >= 1,
        "the baseline donated"
    );
    let (_, j) = *set.audio(odd).lock().handovers.last().unwrap();
    assert!(j > k, "handed over after the divergence");
    // Committing on the donor's state.
    sleep(Duration::from_millis(50)).await;
    assert!(committed_after(
        &set,
        odd,
        due(&set, odd) - 3 * PAGE / BLOCK
    ));
    assert_eq!(
        state(&set, odd),
        Some(ReplicaState::Committing),
        "stays committing"
    );
}

/// The value of `name`'s sample with `label` in `id`'s `/metrics`
/// samples.
fn sample(set: &Set, id: NodeId, name: &str, label: Option<(&str, &str)>) -> f64 {
    let samples = set.replica(id).replica_metrics();
    (samples.iter())
        .find(|m| {
            m.name == name
                && label.is_none_or(|(k, v)| m.labels.iter().any(|(mk, mv)| *mk == k && mv == v))
        })
        .unwrap_or_else(|| panic!("{name} {label:?} in {samples:?}"))
        .value
}

/// The sc pod-kill test: 3 replicas after the apply lead reached its
/// cap, a follower diverged, then the leader killed. The diverged
/// follower resyncs from the replica still committing and commits again
/// within 5 s; `/metrics` shows the hand-over and the replica serving
/// again, and not serving while it wrote nothing.
#[tokio::test(start_paused = true)]
async fn a_follower_diverged_under_a_capped_lead_resyncs_after_the_leader_is_killed() {
    let mut set = start(vec![a(0), a(1), a(2)], false).await;
    let leader = set.leader().await;
    // Commits 50 ms after their stamp: the lead rises to its cap.
    let followers: Vec<NodeId> = audio(&set).into_iter().filter(|&id| id != leader).collect();
    for &id in &followers {
        set.net.stall(id, Duration::from_millis(25));
    }
    for n in 0..3 {
        let command = Bytes::from(format!("g=1@{n}"));
        let _ = (set.replica(leader))
            .submit(&owner("s"), &n.to_string(), command)
            .await;
    }
    sleep(Duration::from_millis(100)).await;
    assert!(
        set.replica(leader)
            .contact_metrics()
            .unwrap()
            .apply_lead_capped
    );
    assert_eq!(
        sample(&set, leader, "samplez_replica_apply_lead_capped", None),
        1.0
    );
    for &id in &followers {
        set.net.unstall(id);
    }
    let ids = audio(&set);
    until("committing again", 5, || {
        ids.iter()
            .all(|&id| state(&set, id) == Some(ReplicaState::Committing))
    })
    .await;
    let named = baseline(&set, leader).unwrap().replica;
    let odd = (followers.iter().copied()).find(|&id| id != named).unwrap();
    // Its first two replays fail: it resyncs after the kill.
    set.audio(odd).lock().failing = 2;
    let k = due(&set, odd) + 30;
    set.audio(odd).glitch(k, FOR_GOOD, 0.5);
    passes(&set, odd, ReplicaState::Diverged, 3).await;
    assert_eq!(
        sample(&set, odd, "samplez_replica_serving", None),
        0.0,
        "diverged, it writes nothing"
    );
    let diverged = Some(("state", "diverged"));
    assert_eq!(sample(&set, odd, "samplez_replica_state", diverged), 1.0);
    set.crash(leader).await;
    let killed = Instant::now();
    passes(&set, odd, ReplicaState::Committing, 5).await;
    assert!(killed.elapsed() < Duration::from_secs(5));
    assert!(resyncs(&set, odd).handovers >= 1);
    let handovers = sample(&set, odd, "samplez_replica_resync_handovers_total", None);
    assert!(handovers >= 1.0);
    sleep(Duration::from_millis(50)).await;
    assert_eq!(sample(&set, odd, "samplez_replica_serving", None), 1.0);
}

/// A failed replay leaves the replica diverged, and it tries again with a
/// newer core.
#[tokio::test(start_paused = true)]
async fn a_failed_resync_is_retried_with_a_newer_core() {
    let set = start(vec![a(0), a(1), a(2)], false).await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let odd = audio(&set).into_iter().find(|&id| id != named).unwrap();
    set.audio(odd).lock().failing = 2;
    let k = due(&set, odd) + 30;
    set.audio(odd).glitch(k, FOR_GOOD, 0.5);
    passes(&set, odd, ReplicaState::Diverged, 2).await;
    passes(&set, odd, ReplicaState::Committing, 5).await;
    let m = resyncs(&set, odd);
    assert_eq!((m.failures, m.handovers), (2, 1), "{m:?}");
    assert!(m.attempts >= 3, "{m:?}");
}

/// 2 audio replicas plus witness, the baseline killed while the other
/// resyncs from it: the survivor, resyncing on a core it already holds
/// and not handed over yet, is named baseline by the new leader without
/// the donor and commits by the Named rule from its next page boundary.
#[tokio::test(start_paused = true)]
async fn the_baseline_killed_during_the_resync_leaves_the_survivor_named() {
    let mut set = start(vec![a(0), a(1), witness()], false).await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let other = audio(&set).into_iter().find(|&id| id != named).unwrap();
    // The replay hands over 100 s after the core arrived: within the
    // test only the Named rule can take the survivor out of `resync`.
    set.audio(other).lock().catch_up = 100_000;
    let k = due(&set, other) + 30;
    set.audio(other).glitch(k, FOR_GOOD, 0.5);
    passes(&set, other, ReplicaState::Resync, 3).await;
    set.crash(named).await;
    let killed = due(&set, other);
    until("renamed", 5, || {
        baseline(&set, other).is_some_and(|b| b.replica == other)
    })
    .await;
    passes(&set, other, ReplicaState::Committing, 5).await;
    let (_, j) = *set.audio(other).lock().handovers.last().unwrap();
    assert!(due(&set, other) < j, "committing before any hand-over");
    assert_eq!(resyncs(&set, other).handovers, 1);
    sleep(Duration::from_millis(50)).await;
    assert!(
        committed_after(&set, other, due(&set, other) - 3 * PAGE / BLOCK)
            && due(&set, other) > killed + 3 * PAGE / BLOCK,
        "the survivor commits after the kill"
    );
}

/// A report gap of 20 ms on one replica of a stateful shape: its state
/// diverged meanwhile, so it does not resume on its own page hashes but
/// resyncs, then commits again.
#[tokio::test(start_paused = true)]
async fn a_20_ms_report_gap_on_a_stateful_shape_resyncs() {
    let set = start(vec![a(0), a(1), witness()], false).await;
    let named = baseline(&set, a(0)).unwrap().replica;
    let x = audio(&set).into_iter().find(|&id| id != named).unwrap();
    // Its strip state takes what it saw during the gap: unequal for good.
    set.audio(x).glitch(due(&set, x) + 1, FOR_GOOD, 0.5);
    for other in [named, witness()] {
        set.net.cut_peer(x, other);
    }
    sleep(Duration::from_millis(20)).await;
    set.net.heal();
    until("diverged", 3, || {
        state(&set, x).is_some_and(ReplicaState::diverged)
    })
    .await;
    passes(&set, x, ReplicaState::Committing, 5).await;
    let m = set.replica(x).contact_metrics().unwrap();
    assert!(m.resync.handovers >= 1, "resynced: {:?}", m.resync);
    assert!(m.replica.unwrap().divergences >= 1, "not resumed on a hash");
}

/// A lagging follower whose next entry the leader purged installs a
/// snapshot: it returns to `starting`, its engine dropped, and commits
/// again on an engine rebuilt from product state.
#[tokio::test(start_paused = true)]
async fn a_lagging_follower_installs_a_snapshot_and_rebuilds_its_engine() {
    let set = start(vec![a(0), a(1), a(2)], true).await;
    let leader = set.leader().await;
    let r = audio(&set).into_iter().find(|&id| id != leader).unwrap();
    // Rebuilds before the install are the start-up clock hold's.
    let before = set.audio(r).lock().rebuilds.len();
    set.audio(r).lock().rebuild_held = true;
    set.force_install(r).await;
    passes(&set, r, ReplicaState::Starting, 2).await;
    let dropped = set.audio(r).lock().drops.last().copied().flatten().unwrap();
    sleep(Duration::from_millis(30)).await;
    let held = due(&set, r);
    set.audio(r).lock().rebuild_held = false;
    passes(&set, r, ReplicaState::Committing, 5).await;
    let m = set.replica(r).contact_metrics().unwrap();
    assert_eq!(m.snapshot_installs, 1);
    let rebuilt = set.audio(r).lock().rebuilds[before].unwrap();
    assert!(rebuilt >= held, "rebuilt at {rebuilt}, released at {held}");
    // Nothing its dropped engine computed is committed.
    let w = set.audio(r).writes(0);
    for i in (dropped + 1) * BLOCK..=rebuilt * BLOCK {
        assert!(!w.has(i), "sample {i} committed while starting");
    }
}

/// A replica rebuilt from product state whose pages differ from the
/// set's for good, as a duck group keyed throughout keeps its envelope
/// off 0 and full depth where the rebuild started it cold: it does not
/// stay withdrawn waiting for a matching page, but takes a donor's core,
/// which carries that state, and commits again within a bound.
#[tokio::test(start_paused = true)]
async fn a_rebuilt_replica_whose_pages_never_match_resyncs() {
    let set = start(vec![a(0), a(1), a(2)], true).await;
    let leader = set.leader().await;
    let r = audio(&set).into_iter().find(|&id| id != leader).unwrap();
    let before = set.audio(r).lock().rebuilds.len();
    set.audio(r).lock().rebuild_glitch = Some(0.5);
    set.force_install(r).await;
    until("rebuilt", 2, || set.audio(r).lock().rebuilds.len() > before).await;
    passes(&set, r, ReplicaState::Committing, 5).await;
    let m = resyncs(&set, r);
    assert!(m.handovers >= 1, "resynced: {m:?}");
}

/// 2 audio replicas plus witness, X stopped while R is in `starting`
/// after a snapshot install: R's engine is rebuilt from product state, R
/// is named and commits from its first page boundary once ready; nothing
/// its earlier engine computed is committed, and output stops until then.
#[tokio::test(start_paused = true)]
async fn x_stopped_while_r_starts_after_a_snapshot_install() {
    let mut set = start(vec![a(0), a(1), witness()], true).await;
    let leader = set.leader().await;
    let r = audio(&set)
        .into_iter()
        .find(|&id| id != leader)
        .unwrap_or_else(|| unreachable!("two audio replicas, one leader"));
    let x = audio(&set).into_iter().find(|&id| id != r).unwrap();
    // Rebuilds before the install are the start-up clock hold's.
    let before = set.audio(r).lock().rebuilds.len();
    set.audio(r).lock().rebuild_held = true;
    set.force_install(r).await;
    passes(&set, r, ReplicaState::Starting, 2).await;
    let dropped = set.audio(r).lock().drops.last().copied().flatten().unwrap();
    set.crash(x).await;
    sleep(Duration::from_millis(50)).await;
    assert_eq!(state(&set, r), Some(ReplicaState::Starting), "held");
    let released = due(&set, r);
    set.audio(r).lock().rebuild_held = false;
    until("R named", 5, || {
        baseline(&set, r).is_some_and(|b| b.replica == r)
    })
    .await;
    passes(&set, r, ReplicaState::Committing, 5).await;
    sleep(Duration::from_millis(50)).await;
    let rebuilt = set.audio(r).lock().rebuilds[before].unwrap();
    assert!(rebuilt >= released);
    let w = set.audio(r).writes(0);
    for i in (dropped + 1) * BLOCK..=rebuilt * BLOCK {
        assert!(!w.has(i), "sample {i} committed while starting");
    }
    assert!(committed_after(&set, r, due(&set, r) - 3 * PAGE / BLOCK));
}

/// The fake's replay reads its source with its own lock free. Contact
/// holds its lock while it calls into the fake, and the source takes that
/// lock: read under the fake's lock, the replay's blocking thread and the
/// runtime's thread each wait for the other's lock, and a paused-clock
/// test stops for good, its deadlines never reached.
#[test]
fn the_fake_replay_reads_its_source_without_its_lock() {
    struct Probe(Arc<FakeAudio>);
    impl Source for Probe {
        fn entries(&self, _after: u64) -> Result<Vec<(u64, Arc<Record>)>, Missed> {
            assert!(
                self.0.state.try_lock().is_ok(),
                "entries read under the lock"
            );
            Ok(Vec::new())
        }
        fn go(&self) -> bool {
            assert!(self.0.state.try_lock().is_ok(), "go read under the lock");
            true
        }
    }
    let clock = Arc::new(Clock::new(Instant::now()));
    let audio = Arc::new(FakeAudio::new(clock, Timing::defaults(BLOCK), 1, false));
    audio.lock().resyncs = true;
    let core = CoreReply {
        k0: 0,
        last_id: 0,
        core: "[]".to_owned(),
    };
    audio
        .resync(&core, &Probe(audio.clone()))
        .expect("handed over");
}

/// The log index of the `Restamp`s and `Void`s `id`'s log holds on entry
/// `index`.
fn decisions(set: &Set, id: NodeId, index: u64) -> Vec<u64> {
    let mut out = Vec::new();
    set.replica(id).store().entries_after(
        0,
        &mut |log_id: samplez_replica::snapshot::StoredLogId, request: &Request| {
            if request.body.decides().map(|(i, _)| i) == Some(index) {
                out.push(log_id.index);
            }
        },
    );
    out
}

/// An entry unresolved on every replica while the leader renames the
/// baseline, whose content a majority contradicts: the decision taken
/// around the rename converges, one decision on the entry in every log,
/// every replica that held it unresolved resolving it, all under the new
/// baseline.
#[tokio::test(start_paused = true)]
async fn a_decision_across_a_rename_converges() {
    let set = start(vec![a(0), a(1), a(2)], false).await;
    let ids = audio(&set);
    let leader = set.leader().await;
    let named = baseline(&set, leader).unwrap().replica;
    for &id in &ids {
        if id != leader {
            set.net.stall(id, Duration::from_millis(60));
        }
    }
    let at = (index_at(set.clock(leader).tai_ns()) + 40 * 48).next_multiple_of(BLOCK);
    set.replica(leader)
        .append(Body::InputDown {
            slot: 0,
            at_index: at,
        })
        .unwrap();
    let mut index = None;
    until("appended", 1, || {
        index = set.logged_at(leader, at);
        index.is_some()
    })
    .await;
    let index = index.unwrap();
    let k = due(&set, named) + 2;
    set.audio(named).glitch(k, k + 2, 0.5);
    sleep(Duration::from_millis(100)).await;
    for &id in &ids {
        set.net.unstall(id);
    }
    until("renamed", 5, || {
        baseline(&set, leader).is_some_and(|b| b.replica != named)
    })
    .await;
    until("decided everywhere", 5, || {
        ids.iter().all(|&id| decisions(&set, id, index).len() == 1)
    })
    .await;
    sleep(Duration::from_secs(2)).await;
    let renamed = baseline(&set, leader).unwrap().replica;
    for &id in &ids {
        assert_eq!(decisions(&set, id, index).len(), 1, "on {id}");
        assert_eq!(baseline(&set, id).unwrap().replica, renamed, "on {id}");
        let held = set
            .audio(id)
            .lock()
            .unresolved
            .iter()
            .any(|&(i, _)| i == index);
        assert!(
            !held
                || set
                    .audio(id)
                    .lock()
                    .resolved
                    .iter()
                    .any(|&(i, _)| i == index),
            "{id} still holds {index} unresolved"
        );
    }
    assert_eq!(state(&set, renamed), Some(ReplicaState::Committing));
}

/// The leader's log entries of each kind: the name of the body's variant.
fn kinds(set: &Set, id: NodeId) -> std::collections::BTreeMap<String, u64> {
    let mut kinds = std::collections::BTreeMap::new();
    set.replica(id).store().entries_after(0, |_, r| {
        let body = format!("{:?}", r.body);
        let kind = body
            .split(|c: char| !c.is_alphanumeric())
            .next()
            .unwrap_or("");
        *kinds.entry(kind.to_owned()).or_default() += 1;
    });
    kinds
}

/// Commits that take longer than the capped lead covers: followers
/// stalled 6 ms per RPC, within the contact window, under a 36 ms cap,
/// whose 4 ms past the margin and applyGuard no commit makes, the
/// leader's included. A command is late on every replica, which holds
/// it unresolved, or resyncs where the baseline applied it; a decision
/// is stamped with
/// the lead the entries needed, past the cap, so it applies in time,
/// where one stamped at the cap was late again and decided in turn,
/// without end, and a `Baseline` lapsed for its named replica. Every
/// replica commits again within 5 s and the unresolved entries stay
/// bounded.
#[tokio::test(start_paused = true)]
async fn a_set_whose_commits_outlast_the_capped_lead_commits_with_bounded_unresolved() {
    let mut timing = Timing::defaults(BLOCK);
    timing.max_apply_lead = Duration::from_millis(36);
    let set = start_timed(vec![a(0), a(1), a(2)], false, timing).await;
    let leader = set.leader().await;
    let followers: Vec<NodeId> = audio(&set).into_iter().filter(|&id| id != leader).collect();
    for &id in &followers {
        set.net.stall(id, Duration::from_millis(6));
    }
    let commands = 10;
    for n in 0..commands {
        let command = Bytes::from(format!("g=1@{n}"));
        let _ = (set.replica(leader))
            .submit(&owner("s"), &n.to_string(), command)
            .await;
        sleep(Duration::from_millis(100)).await;
    }
    let m = set.replica(leader).contact_metrics().unwrap();
    assert!(m.apply_lead_capped, "the lead at its cap");
    let ids = audio(&set);
    // Past the session's lease expiry, an entry of its own.
    sleep(Duration::from_secs(2)).await;
    until("committing under the stall", 5, || {
        ids.iter()
            .all(|&id| state(&set, id) == Some(ReplicaState::Committing))
    })
    .await;
    for &id in &ids {
        let m = set.replica(id).contact_metrics().unwrap();
        assert!(
            m.unresolved <= 2 * commands,
            "{id}: {} unresolved",
            m.unresolved
        );
    }
    let kinds = kinds(&set, leader);
    let restamps = kinds.get("Restamp").copied().unwrap_or(0);
    assert!(restamps <= commands + 2, "one restamp a command: {kinds:?}");
    assert!(
        kinds.get("Baseline").copied().unwrap_or(0) <= 2,
        "{kinds:?}"
    );
}
