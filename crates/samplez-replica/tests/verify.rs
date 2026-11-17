//! Clock verification and the `starting` hold in an in-process set: a
//! replica whose kernel TAI offset or clock disagrees with the set's never
//! verifies, is held in `starting` with its engine's core dropped, is not
//! counted in E, takes no client writes and is named by
//! `replica_clock_unverified`; an offset moved across a detected step
//! unverifies a replica in any state, an offset moved without one ends
//! nothing. Clock offsets are multiples of clockGuard, so no case is tied
//! to one block size.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Set, owner};
use samplez_replica::contact::{Metrics, Timing, UNVERIFIED_DEBOUNCE, time_of};
use samplez_replica::entry::{Body, NodeId, Request};
use samplez_replica::membership::{Slot, node_id, slot_of};
use samplez_replica::state::ReplicaState;
use samplez_service::ReplicatedState;
use samplez_service::replica::Reason;
use tokio::time::{Instant, sleep};

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

/// clockGuard, ns.
fn guard() -> i64 {
    time_of(timing().clock_guard) as i64
}

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

/// Kernel TAI offsets by node; every other node is at 37.
type Offsets = Vec<(NodeId, i64)>;

async fn start(ids: Vec<NodeId>, kernel_offsets: Offsets) -> Set {
    start_with(ids, kernel_offsets, false).await
}

/// `unverified`: the offsets verify nobody.
async fn start_with(ids: Vec<NodeId>, kernel_offsets: Offsets, unverified: bool) -> Set {
    Set::start(Options {
        ids: Some(ids),
        contact: Some(timing()),
        outputs: 1,
        kernel_offsets,
        unverified,
        ..Options::default()
    })
    .await
}

/// Its engine dropped and not rebuilt since.
fn dropped(set: &Set, id: NodeId) -> bool {
    let f = set.audio(id).lock();
    f.drops > f.rebuilds
}

fn audio(set: &Set) -> Vec<NodeId> {
    set.running()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

fn metrics(set: &Set, id: NodeId) -> Metrics {
    set.replica(id).contact_metrics().expect("contact runs")
}

fn state(set: &Set, id: NodeId) -> Option<ReplicaState> {
    metrics(set, id).replica?.state
}

fn baselines(set: &Set, id: NodeId) -> usize {
    let mut n = 0;
    set.replica(id)
        .store()
        .entries_after(0, &mut |_, r: &Request| {
            if matches!(r.body, Body::Baseline(_)) {
                n += 1;
            }
        });
    n
}

async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {secs} s");
        sleep(Duration::from_millis(1)).await;
    }
}

async fn committing(set: &Set, ids: &[NodeId]) {
    until("committing", 10, || {
        ids.iter()
            .all(|&id| state(set, id) == Some(ReplicaState::Committing))
    })
    .await;
}

/// `id` is unverified, held in `starting` with its engine dropped, and
/// not counted anywhere.
fn held(set: &Set, id: NodeId) {
    let m = metrics(set, id);
    assert!(!m.verified, "{id} verified");
    assert_eq!(state(set, id), Some(ReplicaState::Starting), "{id}");
    assert!(dropped(set, id), "{id} keeps its engine");
    for other in set.running() {
        assert!(
            !metrics(set, other).counted.contains(&id),
            "{other} counts {id}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_uniform_cold_start_verifies_every_voter_and_output_starts() {
    for offset in [37, 0] {
        for ids in [vec![a(0), a(1), witness()], vec![a(0), a(1), a(2)]] {
            let offsets = ids.iter().map(|&id| (id, offset)).collect();
            let set = start(ids.clone(), offsets).await;
            committing(&set, &audio(&set)[..1]).await;
            until("every voter verified", 5, || {
                ids.iter().all(|&id| metrics(&set, id).verified)
            })
            .await;
            sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(100)).await;
            for &id in &ids {
                let m = metrics(&set, id);
                assert!(m.clock_unverified.is_empty(), "{offset}: {m:?}");
                assert!(!m.replica_clock_unverified);
                // Offset 0 is the kernel's value until a time daemon sets
                // it: every node is named, also in a uniform set.
                let zero = if offset == 0 { ids.clone() } else { Vec::new() };
                let mut got = m.replica_tai_offset_zero.clone();
                got.sort();
                let mut want = zero;
                want.sort();
                assert_eq!(got, want, "{offset}");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_voter_heard_on_another_offset_blocks_a_cold_start() {
    let w = witness();
    let cases: Vec<(Vec<NodeId>, Offsets)> = vec![
        // The witness and one audio node at 0.
        (vec![a(0), a(1), w], vec![(a(1), 0), (w, 0)]),
        // One audio node at 0, in 2 plus witness and in 3.
        (vec![a(0), a(1), w], vec![(a(0), 0)]),
        (vec![a(0), a(1), a(2)], vec![(a(2), 0)]),
        // The witness alone at 0.
        (vec![a(0), a(1), w], vec![(w, 0)]),
        // One node at 36, as a stale leap-second offset.
        (vec![a(0), a(1), a(2)], vec![(a(1), 36)]),
    ];
    for (ids, offsets) in cases {
        let set = start_with(ids.clone(), offsets.clone(), true).await;
        sleep(Duration::from_millis(500)).await;
        for &id in &ids {
            let m = metrics(&set, id);
            assert!(!m.verified, "{offsets:?}: {id} verified");
            // Status names every voter at once; the alarm waits.
            assert_eq!(m.clock_unverified.len(), ids.len(), "{offsets:?}");
            assert!(!m.replica_clock_unverified, "{offsets:?}: raised at once");
        }
        for &id in &audio(&set) {
            held(&set, id);
            assert_eq!(baselines(&set, id), 0, "{offsets:?}");
        }
        sleep(UNVERIFIED_DEBOUNCE).await;
        for &id in &ids {
            let m = metrics(&set, id);
            assert!(m.replica_clock_unverified, "{offsets:?}");
            for &(node, offset) in &m.clock_unverified {
                let want = offsets.iter().find(|o| o.0 == node).map_or(37, |o| o.1);
                assert_eq!(offset, Some(want), "{offsets:?}: {node}");
            }
        }
        let leader = set.leader().await;
        let refused = set
            .replica(leader)
            .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
            .await
            .expect_err("nothing is stamped while nobody verifies");
        assert_eq!(refused.reason, Reason::NotCommitable, "{offsets:?}");
        assert_eq!(refused.detail, "clock_unverified");
    }
}

/// A voter first heard after output started on another offset never
/// verifies: an audio replica is held in `starting`, not counted, no
/// candidate, and refuses writes 503 while its clients fail over to a
/// replica that commits them; output keeps running and the alarm names it.
#[tokio::test(start_paused = true)]
async fn a_late_voter_on_another_offset_never_verifies_and_output_continues() {
    // A witness of 2 plus witness at 0 among nodes at 37, an audio pod of
    // 3 at 0 (37 s behind TAI), and one on a wrong time source at the
    // right offset, two guards behind.
    let w = witness();
    let cases: Vec<(Vec<NodeId>, NodeId, i64, i64)> = vec![
        (vec![a(0), a(1), w], w, 0, 0),
        (vec![a(0), a(1), a(2)], a(2), 0, -37_000_000_000),
        (vec![a(0), a(1), a(2)], a(2), 37, -2 * guard()),
    ];
    for (ids, late, offset, behind) in cases {
        let mut set = start(ids.clone(), Vec::new()).await;
        let others: Vec<NodeId> = audio(&set).into_iter().filter(|&id| id != late).collect();
        committing(&set, &others[..1]).await;
        set.crash(late).await;
        set.clock(late).set_kernel_offset(offset);
        set.clock(late).set_tai_offset(behind);
        set.restart(late).await;
        sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(500)).await;
        assert!(!metrics(&set, late).verified, "{late}");
        for &id in &others {
            let m = metrics(&set, id);
            assert!(m.verified, "{id}");
            assert!(m.replica_clock_unverified, "{id}: {m:?}");
            assert!(
                m.clock_unverified.contains(&(late, Some(offset))),
                "{id}: {:?}",
                m.clock_unverified
            );
        }
        committing(&set, &others[..1]).await;
        if slot_of(late) == Slot::Witness {
            continue;
        }
        held(&set, late);
        assert!(!set.replica(late).replica().commitable);
        let refused = set
            .replica(late)
            .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
            .await
            .expect_err("an unverified replica refuses writes");
        assert_eq!(refused.reason, Reason::NotCommitable);
        assert_eq!(refused.detail, "clock_unverified");
        // Its client fails over: the write commits on a verified replica.
        set.replica(others[0])
            .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
            .await
            .expect("committed on a verified replica");
        for id in set.running() {
            let mut n = 0;
            set.replica(id)
                .store()
                .entries_after(0, &mut |_, r: &Request| {
                    if let Body::Baseline(b) = &r.body
                        && b.replica == late
                    {
                        n += 1;
                    }
                });
            assert_eq!(n, 0, "{late} named by a Baseline");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn one_voter_verifies_on_its_own_offset_and_again_after_an_offset_step() {
    for (offset, to) in [(37, 0), (0, 37)] {
        let set = start(vec![a(0)], vec![(a(0), offset)]).await;
        until("verified", 5, || metrics(&set, a(0)).verified).await;
        let holds = metrics(&set, a(0)).clock_holds;
        let rebuilds = set.audio(a(0)).lock().rebuilds;
        set.clock(a(0)).step_kernel_offset(to);
        until("held and rebuilt", 5, || {
            metrics(&set, a(0)).clock_holds > holds && set.audio(a(0)).lock().rebuilds > rebuilds
        })
        .await;
        until("verified on the new offset", 5, || {
            let m = metrics(&set, a(0));
            m.verified && m.tai_offset_s == Some(to)
        })
        .await;
        assert_eq!(metrics(&set, a(0)).clock_holds, holds + 1, "one hold");
    }
}

/// An offset step on the only live audio replica of 2 plus witness leaves
/// it unverified against the witness's offset and held in `starting`:
/// output stops, and the alarm names every voter, the down audio replica
/// with its last offset.
#[tokio::test(start_paused = true)]
async fn an_offset_step_on_the_only_live_audio_replica_holds_it() {
    let w = witness();
    let mut set = start(vec![a(0), a(1), w], Vec::new()).await;
    committing(&set, &[a(0)]).await;
    set.crash(a(1)).await;
    sleep(Duration::from_millis(200)).await;
    set.clock(a(0)).step_kernel_offset(0);
    until("held", 5, || {
        state(&set, a(0)) == Some(ReplicaState::Starting)
    })
    .await;
    sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(200)).await;
    held(&set, a(0));
    let m = metrics(&set, a(0));
    assert!(m.replica_clock_unverified);
    let mut got = m.clock_unverified.clone();
    got.sort();
    let mut want = vec![(a(0), Some(0)), (a(1), Some(37)), (w, Some(37))];
    want.sort();
    assert_eq!(got, want);
}

/// A leap second the kernel applies moves the offset without stepping
/// CLOCK_TAI: it ends no verification, and the window in which the
/// voters' offsets differ raises no `replica_clock_unverified`.
#[tokio::test(start_paused = true)]
async fn an_offset_moved_without_a_step_ends_nothing_and_raises_nothing() {
    let ids = vec![a(0), a(1), a(2)];
    let set = start(ids.clone(), Vec::new()).await;
    committing(&set, &ids).await;
    let mut shown = false;
    for &id in &ids {
        set.clock(id).set_kernel_offset(38);
        let until = Instant::now() + Duration::from_millis(100);
        while Instant::now() < until {
            for &id in &ids {
                let m = metrics(&set, id);
                assert!(m.verified, "{id}");
                assert!(!m.replica_clock_unverified, "{id} raised the alarm");
                shown |= !m.clock_unverified.is_empty();
            }
            sleep(Duration::from_millis(5)).await;
        }
    }
    assert!(
        shown,
        "status names the voters off the set's offset at once"
    );
    sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(100)).await;
    for &id in &ids {
        let m = metrics(&set, id);
        assert!(m.verified && m.clock_unverified.is_empty(), "{m:?}");
        assert_eq!(state(&set, id), Some(ReplicaState::Committing));
    }
}

/// While E counts a replica the set's offset does not move: in an all-0
/// set of 2 plus witness an audio node corrected to 37, with its pod
/// restarted or by the step, stays unverified and held, takes nothing
/// over, and E keeps counting the replica at 0; once that one crashes
/// too, E counts nobody, the offsets heard differ and nothing fills.
#[tokio::test(start_paused = true)]
async fn a_node_corrected_in_a_running_set_stays_held() {
    let w = witness();
    for restart in [true, false] {
        let offsets = vec![(a(0), 0), (a(1), 0), (w, 0)];
        let mut set = start(vec![a(0), a(1), w], offsets).await;
        committing(&set, &[a(0)]).await;
        until("both verified", 5, || {
            metrics(&set, a(0)).verified && metrics(&set, a(1)).verified
        })
        .await;
        if restart {
            set.crash(a(1)).await;
            set.clock(a(1)).set_kernel_offset(37);
            set.restart(a(1)).await;
        } else {
            set.clock(a(1)).step_kernel_offset(37);
        }
        sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(500)).await;
        held(&set, a(1));
        assert_eq!(metrics(&set, a(0)).counted, [a(0)]);
        assert!(metrics(&set, a(0)).verified);
        assert!(
            metrics(&set, a(0))
                .clock_unverified
                .contains(&(a(1), Some(37)))
        );
        committing(&set, &[a(0)]).await;
        let fills = set.audio(a(1)).lock().commit_starts.len();
        set.crash(a(0)).await;
        sleep(UNVERIFIED_DEBOUNCE + Duration::from_millis(500)).await;
        held(&set, a(1));
        assert_eq!(
            set.audio(a(1)).lock().commit_starts.len(),
            fills,
            "it fills"
        );
        let m = metrics(&set, a(1));
        assert!(m.replica_clock_unverified);
        let mut got = m.clock_unverified.clone();
        got.sort();
        let mut want = vec![(a(0), Some(0)), (a(1), Some(37)), (w, Some(0))];
        want.sort();
        assert_eq!(got, want, "restart {restart}");
    }
}

/// The horizon: a verified replica under a `ClockVerdict` 20 s behind is
/// not counted and lags E beyond applyGuard, so it is held in `starting`
/// with its core dropped; with its clock back it rebuilds once and is
/// counted again.
#[tokio::test(start_paused = true)]
async fn a_verified_replica_beyond_the_horizon_is_held_and_rebuilt_once() {
    let ids = vec![a(0), a(1), a(2)];
    let set = start(ids.clone(), Vec::new()).await;
    committing(&set, &ids).await;
    let holds = metrics(&set, a(2)).clock_holds;
    let rebuilds = set.audio(a(2)).lock().rebuilds;
    set.clock(a(2)).set_tai_offset(-20_000_000_000);
    until("held by the horizon", 10, || {
        let m = metrics(&set, a(2));
        m.horizon_held
    })
    .await;
    assert!(metrics(&set, a(2)).verified, "a lag ends no verification");
    until("held in starting", 5, || {
        state(&set, a(2)) == Some(ReplicaState::Starting) && dropped(&set, a(2))
    })
    .await;
    set.clock(a(2)).set_tai_offset(0);
    until("rebuilt and counted again", 20, || {
        let m = metrics(&set, a(2));
        !m.horizon_held && !dropped(&set, a(2)) && m.counted.contains(&a(2))
    })
    .await;
    // No second rebuild while its lag settles.
    sleep(Duration::from_secs(1)).await;
    assert_eq!(metrics(&set, a(2)).clock_holds, holds + 1, "one hold");
    assert_eq!(set.audio(a(2)).lock().rebuilds, rebuilds + 1, "one rebuild");
}
