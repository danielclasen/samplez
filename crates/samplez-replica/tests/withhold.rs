//! Withholding and commit through on evidence in an in-process set (plan
//! 2.11, Withholding and divergence, Fencing): every audio replica's
//! engine is a [`harness::FakeAudio`] whose outputs are real hold rings,
//! each sample committed carrying its own index. Without a `Baseline` the
//! audio replicas rank by ordinal, a0 first.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{BLOCK, Clock, FakeAudio, Options, RATE, Set};
use samplez::blocks::{Cause, Kind, PAGE, Range, Standing};
use samplez_replica::contact::{Audio, Timing, index_at};
use samplez_replica::entry::NodeId;
use samplez_replica::membership::{Slot, node_id, slot_of};
use tokio::time::{Instant, sleep};

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

fn a(o: u16) -> NodeId {
    node_id(Slot::Audio(o), 1 + u64::from(o))
}

fn witness() -> NodeId {
    node_id(Slot::Witness, 7)
}

const DEGRADED: u64 = 0xd1;
const OTHER: u64 = 0xd2;

fn audio(set: &Set) -> Vec<NodeId> {
    set.ids()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect()
}

fn tai(set: &Set) -> u64 {
    set.clock(set.ids().next().unwrap()).tai_ns()
}

/// Starts `ids` with one output on each audio replica, every one of them
/// committing.
async fn start(ids: Vec<NodeId>) -> Set {
    start_with(ids, 1, timing()).await
}

/// [`start`] with `outputs` outputs on each audio replica and `t`.
async fn start_with(ids: Vec<NodeId>, outputs: usize, t: Timing) -> Set {
    let set = Set::start(Options {
        ids: Some(ids),
        contact: Some(t),
        outputs,
        manual: true,
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    let k = audio(&set)
        .iter()
        .map(|&id| set.audio(id).due().unwrap())
        .max()
        .unwrap()
        + 1;
    for id in audio(&set) {
        for o in 0..outputs {
            set.audio(id).commit_from(o, k);
        }
    }
    // Past the first page boundary.
    sleep(Duration::from_millis(40)).await;
    for id in audio(&set) {
        for o in 0..outputs {
            let standing = set.audio(id).lock().outputs[o].statuses.standing();
            assert!(
                matches!(standing, Standing::Committing { .. }),
                "{id} output {o} not committing: {standing:?}"
            );
        }
    }
    set
}

/// The block `ms` milliseconds after the one due now.
fn ahead(set: &Set, ms: u64) -> u64 {
    let id = audio(set)[0];
    set.audio(id).due().unwrap() + ms * 48_000 / 1000 / BLOCK
}

/// Waits until block `k` is past its hold cap, and a report period more.
async fn past_cap(set: &Set, k: u64) {
    let t = timing();
    let target =
        t.block_deadline(k) + (t.hold + t.contact_window + 3 * t.report_period).as_nanos() as u64;
    while tai(set) < target {
        sleep(Duration::from_millis(1)).await;
    }
}

/// The replicas among `ids` that committed sample `i`.
fn committers(set: &Set, ids: &[NodeId], i: u64) -> Vec<NodeId> {
    ids.iter()
        .copied()
        .filter(|&id| set.audio(id).writes(0).has(i))
        .collect()
}

/// Every sample of blocks `[from, to)` was committed by exactly one of
/// `ids`.
fn once(set: &Set, ids: &[NodeId], from: u64, to: u64) {
    for i in from * BLOCK..to * BLOCK {
        let c = committers(set, ids, i);
        assert_eq!(
            c.len(),
            1,
            "sample {i} (block {}) committed by {c:?}",
            i / BLOCK
        );
    }
}

/// Every page overlapping blocks `[from, to)` is whole on one of `ids`.
fn pages_whole(set: &Set, ids: &[NodeId], from: u64, to: u64) {
    for page in from * BLOCK / PAGE..=(to * BLOCK - 1) / PAGE {
        let whole = ids.iter().any(|&id| {
            let w = set.audio(id).writes(0);
            (page * PAGE..(page + 1) * PAGE).all(|i| w.has(i))
        });
        assert!(whole, "page {page} whole on no member");
    }
}

fn status(set: &Set, id: NodeId, k: u64) -> (Kind, Cause) {
    let s = set.audio(id).status_of(0, k).expect("a status");
    (s.kind, s.cause)
}

fn withdrawals(set: &Set, id: NodeId) -> u64 {
    set.audio(id).lock().withdrawals
}

/// Two degraded replicas with equal digests both commit through, and
/// none withdraws; with different digests only the top-ranked commits,
/// and the other withholds on its report.
/// The fake decides a block as the engine does: a block the supervisor
/// withheld on a later output alone before it was computed withdraws the
/// replica on every output before output 0 decides it, so output 0 does
/// not commit it.
#[tokio::test(start_paused = true)]
async fn the_fake_withdraws_every_output_before_it_decides_a_block_withheld_on_a_later_one() {
    let clock = Arc::new(Clock::new(Instant::now()));
    // Virtual time is paused: the clock moves only as the test moves it.
    let to = |blocks: u64| clock.set_tai_offset((blocks * BLOCK * 1_000_000_000 / RATE) as i64);
    to(0);
    let audio = FakeAudio::new(clock.clone(), timing(), 2, false);
    audio.fence(u64::MAX);
    audio.commit_all();
    to(100);
    audio.step();
    let k = audio.due().unwrap() + 50;
    assert_eq!(
        audio.status_of(0, k - 51).map(|s| s.kind),
        Some(Kind::Committed)
    );
    let st = audio.withhold(1, k, Cause::FenceDeadline).unwrap();
    assert_eq!(st.kind, Kind::Withheld);
    to(200);
    audio.step();
    let st = audio.status_of(0, k).unwrap();
    assert_ne!(st.kind, Kind::Committed, "{st:?}");
}

#[tokio::test(start_paused = true)]
async fn equal_digests_commit_together_and_different_ones_once() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let k = ahead(&set, 30);
    for &id in &ids {
        set.audio(id).degrade(k, k + 5, DEGRADED);
    }
    past_cap(&set, k + 5).await;
    for &id in &ids {
        for j in k..k + 5 {
            assert_eq!(status(&set, id, j).0, Kind::Committed, "{id} block {j}");
        }
        assert_eq!(withdrawals(&set, id), 0, "{id} withdrew");
    }

    let k = ahead(&set, 30);
    set.audio(a(0)).degrade(k, k + 5, DEGRADED);
    set.audio(a(1)).degrade(k, k + 5, OTHER);
    past_cap(&set, k + 5).await;
    for j in k..k + 5 {
        assert_eq!(status(&set, a(0), j).0, Kind::Committed, "a0 block {j}");
        assert_eq!(status(&set, a(1), j).0, Kind::Withheld, "a1 block {j}");
    }
    // Withdrawn by it: the next blocks hold no page boundary.
    assert_eq!(status(&set, a(1), k), (Kind::Withheld, Cause::OtherDigest));
    once(&set, &ids, k, k + 5);
    let m = set.replica(a(0)).contact_metrics().unwrap();
    assert!(m.withholding.through >= 5, "{:?}", m.withholding);
}

/// A producer stall every member sees: equal digests on the blocks every
/// member finds late, nobody withdraws, and output keeps flowing; at its
/// end a block degraded differently on two members is committed by the
/// top-ranked one alone.
#[tokio::test(start_paused = true)]
async fn a_producer_stall_seen_by_every_member_withdraws_none() {
    let set = start(vec![a(0), a(1), a(2)]).await;
    let ids = audio(&set);
    let k = ahead(&set, 30);
    for &id in &ids {
        set.audio(id).degrade(k, k + 40, DEGRADED);
    }
    set.audio(a(1)).degrade(k + 40, k + 41, OTHER);
    set.audio(a(2)).degrade(k + 40, k + 41, OTHER + 1);
    set.audio(a(0)).degrade(k + 40, k + 41, OTHER + 2);
    past_cap(&set, k + 41).await;
    for &id in &ids {
        for j in k..k + 40 {
            assert_eq!(status(&set, id, j).0, Kind::Committed, "{id} block {j}");
        }
    }
    assert_eq!(status(&set, a(0), k + 40).0, Kind::Committed);
    for id in [a(1), a(2)] {
        assert_eq!(
            status(&set, id, k + 40),
            (Kind::Withheld, Cause::OtherDigest)
        );
    }
    once(&set, &ids, k + 40, k + 41);
}

/// The link between two degraded audio replicas down while both reach the
/// witness: the witness relays their statuses and the top-ranked commits
/// through by `T_k + D + w`; the other withholds.
#[tokio::test(start_paused = true)]
async fn statuses_relayed_by_the_witness_let_the_top_ranked_commit_through() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    set.net.cut(a(0), a(1));
    sleep(Duration::from_millis(20)).await;
    let k = ahead(&set, 30);
    set.audio(a(0)).degrade(k, k + 5, DEGRADED);
    set.audio(a(1)).degrade(k, k + 5, OTHER);
    let t = timing();
    let cap = t.block_deadline(k + 4) + (t.hold + t.contact_window).as_nanos() as u64;
    while tai(&set) < cap {
        sleep(Duration::from_millis(1)).await;
    }
    for j in k..k + 5 {
        assert_eq!(
            status(&set, a(0), j).0,
            Kind::Committed,
            "block {j} by its cap"
        );
    }
    past_cap(&set, k + 5).await;
    once(&set, &ids, k, k + 5);
    let m = set.replica(a(0)).contact_metrics().unwrap();
    assert_eq!(m.withholding.through_on_silence, 0, "a1 is not silent");
}

/// A successor cut off from a live baseline that keeps majority contact
/// through the witness: it learns the baseline's commits relayed and
/// never commits through.
#[tokio::test(start_paused = true)]
async fn a_successor_cut_off_from_a_live_baseline_does_not_commit_through() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    set.net.cut(a(0), a(1));
    sleep(Duration::from_millis(20)).await;
    let k = ahead(&set, 30);
    set.audio(a(1)).degrade(k, k + 10, DEGRADED);
    past_cap(&set, k + 10).await;
    for j in k..k + 10 {
        assert_eq!(status(&set, a(1), j).0, Kind::Withheld, "block {j}");
    }
    assert_eq!(status(&set, a(1), k), (Kind::Withheld, Cause::OtherDigest));
    once(&set, &ids, k, k + 10);
    let m = set.replica(a(1)).contact_metrics().unwrap();
    assert_eq!(m.withholding.through, 0);
}

/// A forced overrun on one member: its skipped blocks are reported
/// `withheld` at the skip, so a degraded peer commits them through
/// without waiting for the hold cap; the skipping one withdraws.
#[tokio::test(start_paused = true)]
async fn an_overrun_skip_is_evidence_at_once() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let k = ahead(&set, 2);
    set.audio(a(1)).degrade(k, k + 40, DEGRADED);
    set.audio(a(0)).overrun(40);
    let t = timing();
    let fence = t.block_deadline(k + 20) + t.hold.as_nanos() as u64;
    while tai(&set) < fence {
        sleep(Duration::from_millis(1)).await;
    }
    for j in k + 5..k + 20 {
        assert_eq!(
            status(&set, a(1), j).0,
            Kind::Committed,
            "block {j} by T_k + D"
        );
    }
    past_cap(&set, k + 40).await;
    assert!(withdrawals(&set, a(0)) >= 1);
    once(&set, &ids, k + 5, k + 39);
}

/// An RT thread stalled past `T_k + D` on a fenced block and on blocks it
/// has not computed, while its supervisor keeps reporting: all are
/// withheld at `T_k + D` and never written; the other replica commits
/// them.
#[tokio::test(start_paused = true)]
async fn a_stalled_rt_thread_has_its_blocks_withheld_at_the_fence_deadline() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let x = a(0);
    // F held below the next blocks: they are computed fenced.
    set.net.cut_peer(a(1), x);
    set.net.cut_peer(witness(), x);
    sleep(Duration::from_millis(20)).await;
    let fenced = set.audio(x).lock().computed.unwrap();
    assert_eq!(status(&set, x, fenced).0, Kind::Fenced);
    set.audio(x).lock().frozen = true;
    set.net.heal();
    let t = timing();
    let deadline = t.block_deadline(fenced + 10) + (t.hold + 2 * t.report_period).as_nanos() as u64;
    while tai(&set) < deadline {
        sleep(Duration::from_millis(1)).await;
    }
    for j in [fenced, fenced + 1, fenced + 10] {
        let s = set.audio(x).lock().outputs[0].statuses.get(j).unwrap();
        assert_eq!(
            (s.kind, s.cause),
            (Kind::Withheld, Cause::FenceDeadline),
            "block {j}"
        );
    }
    set.audio(x).lock().frozen = false;
    past_cap(&set, fenced + 12).await;
    for j in fenced..=fenced + 10 {
        assert!(!set.audio(x).writes(0).has(j * BLOCK), "block {j} written");
    }
    once(&set, &ids, fenced, fenced + 10);
}

/// A report gap on one replica of 10 ms withholds nothing; one of 20 ms,
/// beyond `w + margin - clockGuard`, withholds its gap blocks and
/// withdraws it, while the other replica commits every block.
#[tokio::test(start_paused = true)]
async fn a_report_gap_on_one_replica_withdraws_it_only_past_14_ms() {
    for (gap, withdraws) in [(10, false), (20, true)] {
        let set = start(vec![a(0), a(1), witness()]).await;
        let ids = audio(&set);
        let x = a(0);
        let from = set.audio(x).due().unwrap();
        for other in [a(1), witness()] {
            set.net.cut_peer(x, other);
        }
        sleep(Duration::from_millis(gap)).await;
        set.net.heal();
        sleep(Duration::from_millis(60)).await;
        let to = set.audio(x).due().unwrap() - 30;
        assert_eq!(withdrawals(&set, x) > 0, withdraws, "a {gap} ms gap");
        assert_eq!(withdrawals(&set, a(1)), 0, "the other replica");
        if !withdraws {
            for j in from..to {
                assert_eq!(
                    status(&set, x, j).0,
                    Kind::Committed,
                    "{gap} ms gap, block {j}"
                );
            }
        }
        for j in from..to {
            assert_eq!(
                status(&set, a(1), j).0,
                Kind::Committed,
                "{gap} ms gap, block {j}"
            );
        }
        let _ = ids;
    }
}

/// A report gap every voter shares: of 20 ms no member withholds; of
/// 40 ms every member withholds, nobody is held silent, and the
/// top-ranked withdrawn member fills from its next page boundary.
#[tokio::test(start_paused = true)]
async fn a_shared_report_gap_withholds_on_every_member_only_past_its_floor() {
    for (gap, withholds) in [(20, false), (40, true)] {
        let set = start(vec![a(0), a(1), witness()]).await;
        let ids = audio(&set);
        let from = set.audio(a(0)).due().unwrap();
        for x in set.ids() {
            for y in set.ids() {
                if x != y {
                    set.net.cut_peer(x, y);
                }
            }
        }
        sleep(Duration::from_millis(gap)).await;
        set.net.heal();
        for id in set.ids() {
            let m = set.replica(id).contact_metrics().unwrap();
            assert!(
                m.peers.iter().all(|p| !p.silent),
                "{id} holds a voter silent"
            );
        }
        sleep(Duration::from_millis(80)).await;
        let to = set.audio(a(0)).due().unwrap() - 30;
        for &id in &ids {
            assert_eq!(withdrawals(&set, id) > 0, withholds, "{id}, a {gap} ms gap");
        }
        if withholds {
            // a0 fills from the first page boundary after the gap.
            let p = ((from + gap + 30) * BLOCK).next_multiple_of(PAGE);
            assert!(set.audio(a(0)).writes(0).has(p), "a0 fills from {p}");
            assert!(!set.audio(a(1)).writes(0).has(p), "a1 fills too");
        } else {
            once(&set, &ids[..1], from, to);
        }
    }
}

/// Starts `ids` with one output on each audio replica, only those in
/// `committing` committing.
async fn start_some(ids: Vec<NodeId>, committing: &[NodeId]) -> Set {
    let set = Set::start(Options {
        ids: Some(ids),
        contact: Some(timing()),
        outputs: 1,
        manual: true,
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    for &id in committing {
        set.audio(id).commit_all();
    }
    sleep(Duration::from_millis(40)).await;
    set
}

/// The first page boundary at least `ms` ahead that lies inside a block.
fn page_inside(set: &Set, ms: u64) -> u64 {
    let mut p = (ahead(set, ms) * BLOCK).next_multiple_of(PAGE);
    while p.is_multiple_of(BLOCK) {
        p += PAGE;
    }
    p
}

/// A degraded baseline killed during its hold: the successor, degraded
/// too, commits the blocks through on its silence by `T_k + D + w`.
#[tokio::test(start_paused = true)]
async fn a_degraded_baseline_killed_in_its_hold_leaves_the_blocks_to_the_successor() {
    let mut set = start(vec![a(0), a(1), witness()]).await;
    let k = ahead(&set, 30);
    set.audio(a(0)).degrade(k, k + 5, DEGRADED);
    set.audio(a(1)).degrade(k, k + 5, OTHER);
    // Killed as it computes block k, before any report of it.
    while set.audio(a(0)).due().unwrap() < k {
        sleep(Duration::from_micros(200)).await;
    }
    set.crash(a(0)).await;
    let t = timing();
    let cap = t.block_deadline(k + 4) + (t.hold + t.contact_window).as_nanos() as u64;
    while tai(&set) < cap {
        sleep(Duration::from_millis(1)).await;
    }
    for j in k..k + 5 {
        let s = set.audio(a(1)).status_of(0, j).unwrap();
        assert_eq!(s.kind, Kind::Committed, "block {j} by its cap");
        assert_eq!(s.on_silence_of, 1, "block {j} names a0's silence");
    }
}

/// A lower-ranked replica that applied e, its status for k reaching the
/// degraded baseline after `T_k + D`: the baseline withholds on that
/// committed report when it arrives before `T_k + D + w`, else at the
/// hold cap, and never commits k through.
#[tokio::test(start_paused = true)]
async fn a_late_committed_report_withholds_the_degraded_top_ranked_replica() {
    for (delay, cause) in [(12, Cause::OtherDigest), (22, Cause::HoldCap)] {
        let set = start(vec![a(0), a(1), witness()]).await;
        let ids = audio(&set);
        // Raised in steps below w: no gap between two arrivals is held
        // silent.
        for d in (6..delay).step_by(6).chain([delay]) {
            set.net.delay_reports(a(1), Duration::from_millis(d));
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(withdrawals(&set, a(1)), 0);
        let k = ahead(&set, 30);
        set.audio(a(0)).degrade(k, k + 1, DEGRADED);
        past_cap(&set, k + 1).await;
        assert_eq!(
            status(&set, a(0), k),
            (Kind::Withheld, cause),
            "a {delay} ms delay"
        );
        assert_eq!(status(&set, a(1), k).0, Kind::Committed);
        once(&set, &ids, k, k + 1);
        let m = set.replica(a(0)).contact_metrics().unwrap();
        assert!(
            m.peers.iter().all(|p| !p.silent),
            "a {delay} ms delay holds a1 silent"
        );
    }
}

/// The blocks both `x` and `r` committed among `[from, to)`.
fn overlap(set: &Set, x: NodeId, r: NodeId, from: u64, to: u64) -> Vec<u64> {
    (from..to)
        .filter(|&k| committers(set, &[x, r], k * BLOCK).len() == 2)
        .collect()
}

/// A one-way cut of the top-ranked replica X in each direction while R
/// is degraded. Inbound (X's reports flow): X is fenced; lifted within D
/// it commits on the lift and R withholds on its report, lifted later X
/// withholds at `T_k + D` and R commits through. Outbound beyond w: R
/// commits through on X's silence, the echoes stamped `silentAt` keep X
/// fenced, X withholds at `T_k + D` and withdraws; X and R both commit
/// only blocks ending within `w + clockGuard` of X's last report.
#[tokio::test(start_paused = true)]
async fn a_one_way_cut_of_the_top_ranked_replica_leaves_each_block_to_one() {
    let t = timing();
    let window = index_at((t.contact_window).as_nanos() as u64) + t.clock_guard;
    for (inbound, cut_ms) in [(true, 22), (true, 40), (false, 40)] {
        let set = start(vec![a(0), a(1), witness()]).await;
        let ids = audio(&set);
        let (x, r) = (a(0), a(1));
        let from = ahead(&set, 0);
        // Inbound, R is degraded on the blocks X computes fenced; outbound,
        // from the first block whose status X never sent.
        let fence = set.audio(x).lock().fences.last().unwrap().1;
        let degraded = if inbound { fence / BLOCK } else { from + 1 };
        set.audio(r)
            .degrade(degraded, degraded + cut_ms + 40, DEGRADED);
        let t_cut = tai(&set);
        for other in [r, witness()] {
            if inbound {
                set.net.cut_peer(other, x);
            } else {
                set.net.cut_peer(x, other);
            }
        }
        sleep(Duration::from_millis(cut_ms)).await;
        set.net.heal();
        let to = ahead(&set, 0);
        past_cap(&set, to).await;
        // Healthy blocks both commit, with equal output.
        let both = overlap(&set, x, r, degraded, to.min(degraded + cut_ms + 40));
        if inbound {
            assert!(
                both.is_empty(),
                "inbound {cut_ms} ms: both committed {both:?}"
            );
            if cut_ms > 30 {
                assert!(withdrawals(&set, x) > 0, "X fenced past D withdraws");
            } else {
                // Lifted within D: X commits every block it held fenced on
                // the lift, and R withholds each on X's report.
                assert_eq!(withdrawals(&set, x), 0, "X withdrew");
                let held = (degraded..degraded + cut_ms + 40)
                    .filter(|&k| {
                        set.audio(x)
                            .passed(k)
                            .is_some_and(|at| at > t.block_deadline(k))
                    })
                    .count();
                assert!(held > 0, "X held no block fenced past its deadline");
                for k in degraded..to.min(degraded + cut_ms + 40) {
                    assert_eq!(committers(&set, &ids, k * BLOCK), [x], "block {k}");
                }
            }
        } else {
            let last = index_at(t_cut);
            for k in &both {
                assert!(
                    (k + 1) * BLOCK <= last + window,
                    "block {k} ends past the silence exception window"
                );
            }
            assert!(withdrawals(&set, x) > 0, "X withdraws");
            let m = set.replica(r).contact_metrics().unwrap();
            assert!(m.withholding.through_on_silence > 0, "R used X's silence");
        }
        // Every sample of the cut is committed by one of them at least.
        for k in from..to {
            assert!(
                !committers(&set, &ids, k * BLOCK).is_empty(),
                "block {k} on no member"
            );
        }
    }
}

/// A withdrawn W resuming at P while R is degraded on the block holding
/// P: R commits `[kB, P)` through on W's `committed from P` and withholds
/// from P, so each sample is committed once and no page is whole on no
/// member.
#[tokio::test(start_paused = true)]
async fn a_replica_resuming_inside_a_degraded_block_splits_it() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let (r, w) = (a(0), a(1));
    set.audio(w).lock().withdraw_all();
    sleep(Duration::from_millis(30)).await;
    let p = page_inside(&set, 30);
    let kp = p / BLOCK;
    set.audio(r).degrade(kp - 2, kp + 3, DEGRADED);
    set.audio(w).commit_from(0, kp);
    past_cap(&set, kp + 3).await;
    let s = set.audio(r).status_of(0, kp).unwrap();
    assert_eq!((s.kind, s.range), (Kind::Committed, Range::UpTo(p)));
    let s = set.audio(w).status_of(0, kp).unwrap();
    assert_eq!((s.kind, s.range), (Kind::Committed, Range::From(p)));
    once(&set, &ids, kp - 2, kp + 3);
    pages_whole(&set, &ids, kp - 2, kp + 3);
}

/// The same with R healthy after the block holding P: R commits `[kB, P)`
/// through, withdraws on it and commits none of the blocks after it, which
/// W, committing from P, holds.
#[tokio::test(start_paused = true)]
async fn a_replica_split_at_p_commits_no_healthy_block_after_it() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let (r, w) = (a(0), a(1));
    set.audio(w).lock().withdraw_all();
    sleep(Duration::from_millis(30)).await;
    let p = page_inside(&set, 30);
    let kp = p / BLOCK;
    set.audio(r).degrade(kp - 2, kp + 1, DEGRADED);
    set.audio(w).commit_from(0, kp);
    past_cap(&set, kp + 3).await;
    let s = set.audio(r).status_of(0, kp).unwrap();
    assert_eq!((s.kind, s.range), (Kind::Committed, Range::UpTo(p)));
    for j in [kp + 1, kp + 2] {
        assert_ne!(status(&set, r, j).0, Kind::Committed, "R block {j}");
    }
    once(&set, &ids, kp - 2, kp + 3);
    pages_whole(&set, &ids, kp - 2, kp + 3);
    let m = set.replica(r).contact_metrics().unwrap();
    assert_eq!(m.withholding.withdrawals, 1, "{:?}", m.withholding);
}

/// A named N committing from P while a withdrawn F fills: F commits
/// `[kB, P)` and withholds from P on N's `committed from P` report.
#[tokio::test(start_paused = true)]
async fn a_filler_stops_at_the_commit_start_of_a_named_replica() {
    let (f, n) = (a(0), a(1));
    let set = start_some(vec![f, n, witness()], &[f]).await;
    let ids = audio(&set);
    set.audio(f).lock().withdraw_all();
    // F fills from its next page boundary on.
    sleep(Duration::from_millis(40)).await;
    let filled = set.audio(f).due().unwrap() - 25;
    assert!(set.audio(f).writes(0).has(filled * BLOCK), "F fills");
    let p = page_inside(&set, 30);
    let kp = p / BLOCK;
    set.audio(n).commit_from(0, kp);
    past_cap(&set, kp + 5).await;
    let s = set.audio(f).status_of(0, kp).unwrap();
    assert_eq!((s.kind, s.range), (Kind::Committed, Range::UpTo(p)));
    once(&set, &ids, filled, kp + 5);
    pages_whole(&set, &ids, filled, kp + 5);
}

/// A replica X whose commit start P lies inside block k still holding
/// `[P, (k + 1)B)` while an outranking degraded R has another digest: X
/// reports `[kB, P)` withheld when it computes k, and R commits k through
/// without waiting for the hold cap; k is whole on R.
#[tokio::test(start_paused = true)]
async fn a_commit_start_inside_a_block_is_evidence_below_it_at_once() {
    let (r, x) = (a(0), a(1));
    let set = start_some(vec![r, x, witness()], &[r]).await;
    let ids = audio(&set);
    let p = page_inside(&set, 30);
    let k = p / BLOCK;
    set.audio(x).commit_from(0, k);
    set.audio(r).degrade(k, k + 1, DEGRADED);
    set.audio(x).degrade(k, k + 1, OTHER);
    let t = timing();
    let fence = t.block_deadline(k) + t.hold.as_nanos() as u64;
    while tai(&set) < fence {
        sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(status(&set, r, k).0, Kind::Committed, "before the hold cap");
    past_cap(&set, k + 1).await;
    let s = set.audio(x).status_of(0, k).unwrap();
    assert_eq!(
        (s.kind, s.range, s.cause),
        (Kind::Withheld, Range::From(p), Cause::OtherDigest)
    );
    once(&set, &ids, k, k + 1);
}

/// A `Baseline` applied in time on one of two degraded replicas and late
/// on the other: the late one ranks demoted for the blocks whose apply
/// deadline passed before it knew the commit, so only the one that knew
/// in time commits through.
#[tokio::test(start_paused = true)]
async fn a_baseline_known_late_ranks_its_replica_demoted() {
    use samplez_replica::entry::{Baseline, Body, Request};
    let set = start(vec![a(0), a(1), witness()]).await;
    let ids = audio(&set);
    let leader = set.leader().await;
    // The late one is a follower; the Baseline names it first.
    let late = ids.iter().copied().find(|&id| id != leader).unwrap();
    let in_time = ids.iter().copied().find(|&id| id != late).unwrap();
    for other in set.ids().filter(|&id| id != late) {
        set.net.cut_raft(other, late);
    }
    // Far enough ahead that the others know the commit before k's apply
    // deadline `T(kB) - applyGuard`.
    let k = ahead(&set, 60);
    set.audio(late).degrade(k, k + 5, DEGRADED);
    set.audio(in_time).degrade(k, k + 5, OTHER);
    let request = Request {
        stamp: 0,
        body: Body::Baseline(Baseline {
            replica: late,
            order: vec![in_time],
            at_index: k * BLOCK,
            key: None,
        }),
    };
    set.replica(leader)
        .raft()
        .client_write(request)
        .await
        .expect("the Baseline commits");
    past_cap(&set, k + 5).await;
    for j in k..k + 5 {
        assert_eq!(status(&set, in_time, j).0, Kind::Committed, "block {j}");
        assert_eq!(status(&set, late, j).0, Kind::Withheld, "block {j}");
    }
    // Ranked second by the Baseline, and demoted, below both voters.
    let in_time_rank = set.audio(in_time).status_of(0, k).unwrap().rank;
    let late_rank = set.audio(late).status_of(0, k).unwrap().rank;
    assert_eq!(in_time_rank, 1);
    assert!(late_rank >= 2, "late ranks {late_rank}");
    once(&set, &ids, k, k + 5);
    set.net.heal();
}

/// `hold_floor_unmet` alarms once the measured report delay puts the
/// floor L above w: here a 10 ms one-way delay on one voter's reports.
#[tokio::test(start_paused = true)]
async fn the_hold_floor_alarm_follows_the_measured_delay() {
    let set = start(vec![a(0), a(1), witness()]).await;
    let m = set.replica(a(0)).contact_metrics().unwrap();
    assert!(
        m.floor.is_some_and(|l| l < timing().contact_window),
        "{:?}",
        m.floor
    );
    assert!(!m.hold_floor_unmet);
    for d in [5, 10] {
        set.net.delay_reports(a(1), Duration::from_millis(d));
        sleep(Duration::from_millis(10)).await;
    }
    sleep(Duration::from_millis(50)).await;
    let m = set.replica(a(0)).contact_metrics().unwrap();
    assert!(m.hold_floor_unmet, "floor {:?}", m.floor);
}

/// Each block carries the rank of the `Baseline` in force at its index:
/// two `Baseline` entries three blocks apart put two rank changes ahead
/// of the RT thread at once, and F waits at the second until the first
/// is taken.
#[tokio::test(start_paused = true)]
async fn blocks_carry_the_rank_of_the_baseline_at_their_index() {
    use samplez_replica::entry::{Baseline, Body, Request};
    let set = start(vec![a(0), a(1), witness()]).await;
    let leader = set.leader().await;
    let k = ahead(&set, 40);
    for (at, first) in [(k, a(1)), (k + 3, a(0))] {
        let other = if first == a(0) { a(1) } else { a(0) };
        let request = Request {
            stamp: 0,
            body: Body::Baseline(Baseline {
                replica: first,
                order: vec![other],
                at_index: at * BLOCK,
                key: None,
            }),
        };
        set.replica(leader)
            .raft()
            .client_write(request)
            .await
            .expect("the Baseline commits");
    }
    past_cap(&set, k + 6).await;
    let ranks: Vec<u8> = (k - 2..k + 6)
        .map(|j| set.audio(a(0)).status_of(0, j).unwrap().rank)
        .collect();
    assert_eq!(ranks, [0, 0, 1, 1, 1, 0, 0, 0]);
    let ranks: Vec<u8> = (k - 2..k + 6)
        .map(|j| set.audio(a(1)).status_of(0, j).unwrap().rank)
        .collect();
    assert_eq!(ranks, [1, 1, 0, 0, 0, 1, 1, 1]);
}

/// Blocks the RT thread computes late, in one catch-up, are held at the
/// rank of their index although two rank changes lie among them: F waits
/// at the second change, so the blocks after it are computed fenced and
/// take their rank when F passes them.
#[tokio::test(start_paused = true)]
async fn blocks_computed_in_one_catch_up_take_the_rank_of_their_index() {
    use samplez_replica::entry::{Baseline, Body, Request};
    let set = start(vec![a(0), a(1), witness()]).await;
    let leader = set.leader().await;
    let k = ahead(&set, 40);
    // Held, so the rank F passes them with is the one they carry; equal
    // digests commit them through on both.
    for id in [a(0), a(1)] {
        set.audio(id).degrade(k - 2, k + 6, DEGRADED);
    }
    for (at, first) in [(k, a(1)), (k + 3, a(0))] {
        let other = if first == a(0) { a(1) } else { a(0) };
        let request = Request {
            stamp: 0,
            body: Body::Baseline(Baseline {
                replica: first,
                order: vec![other],
                at_index: at * BLOCK,
                key: None,
            }),
        };
        set.replica(leader)
            .raft()
            .client_write(request)
            .await
            .expect("the Baseline commits");
    }
    // a0's RT thread computes blocks k - 1 to k + 4 at once, each within
    // its fence deadline.
    while set.audio(a(0)).due().unwrap() < k - 2 {
        sleep(Duration::from_micros(200)).await;
    }
    set.audio(a(0)).lock().frozen = true;
    while set.audio(a(0)).due().unwrap() < k + 4 {
        sleep(Duration::from_micros(200)).await;
    }
    set.audio(a(0)).lock().frozen = false;
    past_cap(&set, k + 6).await;
    let ranks: Vec<u8> = (k - 2..k + 6)
        .map(|j| set.audio(a(0)).status_of(0, j).unwrap().rank)
        .collect();
    assert_eq!(ranks, [0, 0, 1, 1, 1, 0, 0, 0]);
}

/// A `Baseline` this replica learns of after F passed a block keeps the
/// rank the block was published with: its index lies below F, so the
/// block computed after it does not rank demoted.
#[tokio::test(start_paused = true)]
async fn a_baseline_learned_after_f_passed_a_block_leaves_its_rank() {
    use samplez_replica::entry::{Baseline, Body, Request};
    let set = start(vec![a(0), a(1), witness()]).await;
    let leader = set.leader().await;
    let follower = audio(&set).into_iter().find(|&id| id != leader).unwrap();
    let due = set.audio(follower).due().unwrap();
    let f = set.audio(follower).lock().fences.last().unwrap().1 / BLOCK;
    assert!(f > due + 8, "F passed blocks not computed yet");
    let request = Request {
        stamp: 0,
        body: Body::Baseline(Baseline {
            replica: a(0),
            order: vec![a(1)],
            at_index: (due + 3) * BLOCK,
            key: None,
        }),
    };
    set.replica(leader)
        .raft()
        .client_write(request)
        .await
        .expect("the Baseline commits");
    past_cap(&set, due + 8).await;
    let want = if follower == a(0) { 0 } else { 1 };
    for j in due + 3..due + 8 {
        let rank = set.audio(follower).status_of(0, j).unwrap().rank;
        assert_eq!(rank, want, "block {j}");
    }
}

/// A block withheld on one output withdraws the replica on every output
/// before the next output's ring advances: in the pass in which F passes
/// block k, output 1 does not commit k beside output 0 withholding it.
#[tokio::test(start_paused = true)]
async fn a_block_withheld_on_one_output_withdraws_every_output_in_the_pass() {
    let set = start_with(vec![a(0), a(1), witness()], 2, timing()).await;
    let x = a(0);
    // F stalls on X: none of its reports is echoed.
    set.net.cut_peer(a(1), x);
    set.net.cut_peer(witness(), x);
    sleep(Duration::from_millis(22)).await;
    let k = {
        let mut st = set.audio(x).lock();
        let fenced = |st: &harness::Fake, o: usize, j: u64| {
            st.outputs[o]
                .statuses
                .get(j)
                .is_some_and(|s| s.kind == Kind::Fenced)
        };
        for o in 0..2 {
            let standing = st.outputs[o].statuses.standing();
            assert!(
                matches!(standing, Standing::Committing { .. }),
                "{standing:?}"
            );
        }
        let last = st.computed.unwrap();
        let k = (last - 30..=last)
            .find(|&j| fenced(&st, 0, j) && fenced(&st, 1, j))
            .expect("a block fenced on both outputs");
        // The supervisor withholds k on output 0, then F passes it, with
        // no pass of the RT thread between the two.
        let s = st.outputs[0].statuses.withhold(k, Cause::HoldCap).unwrap();
        assert_eq!(s.kind, Kind::Withheld);
        st.fences.push((0, (k + 1) * BLOCK));
        k
    };
    set.audio(x).step();
    let s = set.audio(x).lock().outputs[1].statuses.get(k).unwrap();
    assert_ne!(s.kind, Kind::Committed, "{s:?}");
    assert!(!set.audio(x).writes(1).has(k * BLOCK));
    set.net.heal();
}
