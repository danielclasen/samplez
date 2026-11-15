//! Statuses and the hold ring of a replicated engine with more than one
//! voter, against a real MXL domain: the replicated state's side (F, the
//! commit start, withholding, commit through) is played by the test.

mod common;

use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use common::{B, Domain, MARGIN, Pilot, new_id, pilot, read_blocks, wait_for};
use samplez::blocks::{
    BlockStatus, Cause, HEALTHY, Kind, PAGE, Range, StatusChange, degraded_digest,
};
use samplez::{Config, Engine, Entry, InputState, Replication};
use samplez_mxl::input::{Generation, InputConfig, Timing};

/// A replicated engine of two voters, one input through gain 1 to one
/// output, with its status changes.
fn start(domain: &Domain, src: &str, out: &str, hold_ms: u32) -> (Engine, Receiver<StatusChange>) {
    // `hold_ms` 0: the default, and the input is not waited for.
    let attached = hold_ms > 0;
    let hold_ms = if attached { hold_ms } else { 10 };
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": MARGIN,
        "holdMs": hold_ms,
        "libmxl": common::libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "in0", "domain": "d", "flowId": src, "channels": 1 }],
        "outputs": [{ "name": "out0", "domain": "d", "flowId": out, "label": "hold",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (report_tx, _reports) = std::sync::mpsc::sync_channel(64);
    let (status_tx, statuses) = std::sync::mpsc::sync_channel(1 << 20);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: Some(status_tx),
        hashes: None,
    };
    let e = Engine::start_replicated(config, process, None, replication).unwrap();
    if attached {
        wait_for(Duration::from_secs(5), "input attached", || {
            e.status().inputs[0].state == InputState::Attached
        });
    }
    wait_for(Duration::from_secs(5), "output realised", || {
        e.status().outputs[0].realised
    });
    (e, statuses)
}

/// What disturbed one run of a scenario, read from its engine when the
/// run ends, by a failure too.
#[derive(Default)]
struct Watch(RefCell<Vec<String>>);

impl Watch {
    /// Records, when it drops, the overrun skips of `e` and, unless the
    /// scenario causes them, its ring overflows. Created after the
    /// engine, so it drops before it.
    fn guard<'a>(&'a self, e: &'a Engine, overflows: bool) -> Guard<'a> {
        Guard {
            watch: self,
            e,
            overflows,
        }
    }
}

struct Guard<'a> {
    watch: &'a Watch,
    e: &'a Engine,
    overflows: bool,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let c = self.e.counters();
        let mut why = self.watch.0.borrow_mut();
        let skipped = c.overrun_skipped_samples.load(Relaxed);
        if skipped > 0 {
            why.push(format!("{skipped} samples skipped by overruns"));
        }
        let overflows = c.hold_overflows.load(Relaxed);
        if overflows > 0 && !self.overflows {
            why.push(format!("{overflows} ring overflows"));
        }
    }
}

/// Runs `scenario` until one run is not disturbed by an overrun skip or
/// a ring overflow it did not cause, at most four times: a skip or an
/// overflow withdraws the replica and changes every status after it. An
/// undisturbed run must pass. A disturbed one is run again whatever its
/// result, named on stderr, and the test fails naming the disturbances
/// when every run was disturbed.
fn undisturbed(scenario: impl Fn(&Watch)) {
    let mut seen = Vec::new();
    for _ in 0..4 {
        let w = Watch::default();
        let result = catch_unwind(AssertUnwindSafe(|| scenario(&w)));
        let why = w.0.take();
        if why.is_empty() {
            if let Err(p) = result {
                resume_unwind(p);
            }
            return;
        }
        eprintln!("run disturbed ({}): running it again", why.join(", "));
        seen.push(why.join(", "));
    }
    panic!("every run was disturbed: {seen:?}");
}

/// The head of a flow, as libmxl reports it.
fn head(domain: &Domain, flow_id: &str) -> u64 {
    let mut g = Generation::new(&common::api(), &domain.0, 1).unwrap();
    let cfg = InputConfig {
        flow_id,
        channels: 1,
        offset: 0,
    };
    let timing = Timing {
        block: B,
        margin: 0,
        max_catchup_blocks: 0,
        resync_budget: 0,
    };
    g.attach(0, &cfg, &timing).unwrap();
    g.input(0).unwrap().reader().head().unwrap()
}

fn processed(e: &Engine) -> u64 {
    e.counters()
        .processed
        .load(std::sync::atomic::Ordering::Acquire)
}

fn wait_processed(e: &Engine, k: u64) {
    wait_for(
        Duration::from_secs(10),
        &format!("block {k} processed"),
        || processed(e) > k,
    );
}

/// Marks every held block in `ks` but those in `skip` for commit through,
/// as evidence would.
fn commit_through(e: &Engine, ks: std::ops::Range<u64>, skip: &[u64]) {
    for k in ks {
        if !skip.contains(&k) {
            let _ = e.commit_through(0, k, 0);
        }
    }
}

/// Block `k` as committed and read back: the pilot from its commit start
/// on for a healthy block, zeros below it.
fn check_written(st: &BlockStatus, got: &[f32]) {
    let start = match st.range {
        Range::From(p) => p,
        _ => st.k * B,
    };
    for (i, &v) in got.iter().enumerate() {
        let idx = st.k * B + i as u64;
        if idx < start {
            assert_eq!(v, 0.0, "block {} sample {idx} below its commit start", st.k);
        } else if st.digest == Some(HEALTHY) {
            assert_eq!(v, pilot(0, idx), "block {} sample {idx}", st.k);
        }
    }
}

/// An output commits nothing before its commit start, then from the
/// first page boundary at or after it: the block holding it reports
/// `committed from P` (or a whole block when P is its start) and the
/// member flow holds zeros below P and the output from P on.
#[test]
fn an_output_commits_from_a_page_boundary() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, out) = (new_id(), new_id());
        let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
        let (e, _changes) = start(&domain, &src, &out, 10);
        let _guard = w.guard(&e, false);
        let k0 = processed(&e) + 10;
        wait_processed(&e, k0 + 30);
        assert!(
            (k0..k0 + 30).all(|k| e.block_status(0, k).is_none()),
            "an idle output gives no status"
        );

        e.set_fence(u64::MAX).unwrap();
        let from = processed(&e) + 2;
        e.commit_from(0, from).unwrap();
        let p = (from * B).next_multiple_of(PAGE);
        let kp = p / B;
        let deadline = Instant::now() + Duration::from_secs(10);
        while processed(&e) <= kp + 40 {
            commit_through(&e, kp..processed(&e), &[]);
            assert!(Instant::now() < deadline, "block {} not processed", kp + 40);
            std::thread::sleep(Duration::from_millis(2));
        }
        let st = e.block_status(0, kp).unwrap();
        assert_eq!(st.kind, Kind::Committed, "{st:?}");
        let want = if p > kp * B {
            Range::From(p)
        } else {
            Range::Whole
        };
        assert_eq!(st.range, want);
        assert!((from..kp).all(|k| e.block_status(0, k).is_none()));
        let got = read_blocks(&domain.0, &out, 1, kp - 2..kp + 20);
        assert!(got[..2 * B as usize].iter().all(|&v| v == 0.0));
        for k in kp..kp + 20 {
            let st = e.block_status(0, k).unwrap();
            assert_eq!(st.kind, Kind::Committed, "{st:?}");
            let i = ((k - (kp - 2)) * B) as usize;
            check_written(&st, &got[i..i + B as usize]);
        }
    });
}

/// Blocks past F are fenced and held in the ring; a block withheld there
/// is never written, nor is one withheld before it was computed, while
/// the blocks before them commit once F passes them. The replica is then
/// withdrawn and fills again from the next page boundary on evidence:
/// the withheld blocks read as libmxl's zero-fill between output on both
/// sides.
#[test]
fn a_withheld_block_is_never_written() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, out) = (new_id(), new_id());
        let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
        let (e, changes) = start(&domain, &src, &out, 400);
        let _guard = w.guard(&e, false);
        e.set_fence(u64::MAX).unwrap();
        let from = processed(&e) + 2;
        e.commit_from(0, from).unwrap();
        let kp = (from * B).next_multiple_of(PAGE) / B;
        wait_processed(&e, kp + 5);

        // F frozen: every later block is fenced and stays in the ring.
        let f = processed(&e) + 5;
        e.set_fence(f * B).unwrap();
        let k = f + 40;
        wait_processed(&e, k + 1);
        let st = e.block_status(0, k).unwrap();
        assert_eq!(st.kind, Kind::Fenced, "{st:?}");
        assert_eq!(
            e.withhold(0, k, Cause::FenceDeadline)
                .unwrap()
                .unwrap()
                .kind,
            Kind::Withheld
        );
        // Not computed yet: withheld before its first status.
        let later = processed(&e) + 100;
        assert_eq!(
            e.withhold(0, later, Cause::FenceDeadline)
                .unwrap()
                .unwrap()
                .kind,
            Kind::Withheld
        );

        e.set_fence(u64::MAX).unwrap();
        // Evidence for every held block but the two, until a fill past
        // `later` committed.
        let deadline = Instant::now() + Duration::from_secs(10);
        let filled = |e: &Engine| {
            (later + 1..processed(e)).find(|&x| {
                e.block_status(0, x)
                    .is_some_and(|s| s.kind == Kind::Committed)
            })
        };
        let after = loop {
            commit_through(&e, kp..processed(&e), &[k, later]);
            if let Some(x) = filled(&e)
                && processed(&e) > x + 3
            {
                break x;
            }
            assert!(Instant::now() < deadline, "nothing committed after {later}");
            std::thread::sleep(Duration::from_millis(2));
        };
        commit_through(&e, kp..processed(&e), &[k, later]);
        std::thread::sleep(Duration::from_millis(50));

        for x in [k, later] {
            let st = e.block_status(0, x).unwrap();
            assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::FenceDeadline));
        }
        // Every block committed reads back as the engine's output, every
        // other one as libmxl's zero-fill.
        let lo = k - 3;
        let got = read_blocks(&domain.0, &out, 1, lo..after + 1);
        let at = |x: u64| &got[((x - lo) * B) as usize..((x - lo + 1) * B) as usize];
        let mut committed = (0, 0);
        for x in lo..after + 1 {
            match e.block_status(0, x) {
                Some(st) if st.kind == Kind::Committed => {
                    if x < k {
                        committed.0 += 1;
                    } else if x > later {
                        committed.1 += 1;
                    }
                    // A fill has no digest; its samples are the output too.
                    let st = BlockStatus {
                        digest: st.digest.or(Some(HEALTHY)),
                        ..st
                    };
                    if !at(x).iter().all(|&v| v == 0.0) {
                        check_written(&st, at(x));
                    }
                }
                st => assert!(
                    at(x).iter().all(|&v| v == 0.0),
                    "block {x} was written: {st:?}"
                ),
            }
        }
        assert!(committed.0 > 0 && committed.1 > 0, "{committed:?}");
        // The fenced blocks before k committed once F passed them, then the
        // replica was withdrawn and filled again from the page boundaries
        // after each withheld block, on evidence.
        for x in f..k {
            assert_eq!(
                e.block_status(0, x).unwrap().kind,
                Kind::Committed,
                "block {x}"
            );
        }
        let fill = |after: u64| ((after + 1) * B).next_multiple_of(PAGE) / B;
        let (f1, f2) = (fill(k), fill(later));
        assert_eq!(after, f2);
        for x in [f1, f2] {
            let st = e.block_status(0, x).unwrap();
            assert_eq!(st.kind, Kind::Committed, "fill block {x}: {st:?}");
            assert_eq!(st.digest, None, "a fill carries no digest");
        }
        for x in (k + 1..f1).chain(later + 1..f2) {
            let st = e.block_status(0, x).unwrap();
            assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::Unfillable));
        }
        // The RT thread sent its moves of the fill block, ending committed.
        let last = changes
            .try_iter()
            .filter_map(|c| match c {
                StatusChange::Block { status, .. } if status.k == f1 => Some(status.kind),
                _ => None,
            })
            .last();
        assert_eq!(last, Some(Kind::Committed));
    });
}

/// With F frozen the ring fills: its oldest block is withheld for
/// overflow, counted, and never written; the replica is withdrawn.
#[test]
fn a_full_hold_ring_withholds_its_oldest_block() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, out) = (new_id(), new_id());
        let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
        let (e, _changes) = start(&domain, &src, &out, 10);
        let _guard = w.guard(&e, true);
        e.set_fence(u64::MAX).unwrap();
        let from = processed(&e) + 2;
        e.commit_from(0, from).unwrap();
        let kp = (from * B).next_multiple_of(PAGE) / B;
        wait_processed(&e, kp + 5);
        let f = processed(&e) + 2;
        e.set_fence(f * B).unwrap();
        // The ring holds D + w + 2B = 20 blocks.
        wait_processed(&e, f + 40);
        let c = e.counters();
        assert!(c.hold_overflows.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        let st = e.block_status(0, f).unwrap();
        assert_eq!(
            (st.kind, st.cause),
            (Kind::Withheld, Cause::Overflow),
            "{st:?}"
        );
        e.set_fence(u64::MAX).unwrap();
        wait_processed(&e, processed(&e) + 50);
        // Nothing from the first overflowing block on was written: the
        // withdrawn replica fills only on evidence, which nobody gave.
        assert_eq!(head(&domain, &out), f * B);
        let got = read_blocks(&domain.0, &out, 1, f - 2..f);
        assert!(
            got.iter().any(|&v| v != 0.0),
            "blocks before F were written"
        );
    });
}

/// An input that is up but not read degrades every block: its bit is in
/// the digest, and the blocks are held, never committed at once.
#[test]
fn an_input_not_read_degrades_its_blocks() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, out) = (new_id(), new_id());
        let (e, _changes) = start(&domain, &src, &out, 0);
        let _guard = w.guard(&e, false);
        assert_ne!(e.status().inputs[0].state, InputState::Attached);
        e.set_fence(u64::MAX).unwrap();
        let from = processed(&e) + 2;
        e.commit_from(0, from).unwrap();
        let kp = (from * B).next_multiple_of(PAGE) / B;
        wait_processed(&e, kp + 10);
        for k in kp..kp + 10 {
            let st = e.block_status(0, k).unwrap();
            assert_eq!(st.kind, Kind::Held, "{st:?}");
            assert_eq!(st.digest, Some(degraded_digest(&[], 1, 0)));
        }
        assert_eq!(head(&domain, &out), 0);
    });
}

/// A crossfade whose outgoing flow fails degrades its blocks: the slot's
/// outgoing bit is in the digest while the incoming flow is read.
#[test]
fn a_crossfade_with_a_failed_outgoing_flow_degrades_its_blocks() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, next, out) = (new_id(), new_id(), new_id());
        let old = Pilot::start(&domain.0, &src, 1, B, 0);
        let _new = Pilot::start(&domain.0, &next, 1, B, 0);
        let (e, _changes) = start(&domain, &src, &out, 10);
        let _guard = w.guard(&e, false);
        e.set_fence(u64::MAX).unwrap();
        e.commit_from(0, processed(&e) + 2).unwrap();
        let r = (processed(&e) + 150) * B;
        let flow_id = next.clone();
        e.append(1, r, samplez::Entry::Route { slot: 0, flow_id })
            .unwrap();
        e.commit(1);
        let ramp = 480 / B;
        // Evidence for every held block, so none overflows the ring.
        let through = |e: &Engine| commit_through(e, r / B - 200..processed(e), &[]);
        let deadline = Instant::now() + Duration::from_secs(10);
        // The engine reads MARGIN behind the clock, and the producer has
        // written up to the clock: deleted 14 blocks before the crossfade,
        // the outgoing flow runs out in its middle.
        while processed(&e) < r / B - 14 {
            through(&e);
            assert!(Instant::now() < deadline, "crossfade not reached");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(MARGIN / B, 20);
        drop(old);
        while processed(&e) <= r / B + ramp + 5 {
            through(&e);
            assert!(Instant::now() < deadline, "crossfade not passed");
            std::thread::sleep(Duration::from_millis(1));
        }
        let degraded = Some(degraded_digest(&[], 0, 1));
        let digests: Vec<_> = (r / B..r / B + ramp)
            .map(|k| e.block_status(0, k).and_then(|s| s.digest))
            .collect();
        // The outgoing flow has run out by the crossfade's last block, while
        // the incoming one is read: that block carries the slot's bit.
        assert_eq!(digests[ramp as usize - 1], degraded, "{digests:?}");
    });
}

/// Blocks withheld while held, with nothing behind them, still withdraw
/// the replica before the next block takes its status: no block after
/// them commits at once, and without evidence none commits at all.
#[test]
fn no_block_commits_at_once_after_a_withheld_one() {
    undisturbed(|w| {
        let domain = Domain::new();
        let (src, out) = (new_id(), new_id());
        let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
        let (e, _changes) = start(&domain, &src, &out, 10);
        let _guard = w.guard(&e, false);
        e.set_fence(u64::MAX).unwrap();
        let from = processed(&e) + 2;
        e.commit_from(0, from).unwrap();
        let kp = (from * B).next_multiple_of(PAGE) / B;
        wait_processed(&e, kp + 5);
        let f = processed(&e);
        e.set_fence(f * B).unwrap();
        wait_processed(&e, f + 5);
        // Every block from F on, computed or not yet, withheld.
        let last = processed(&e) + 3;
        for k in f..=last {
            e.withhold(0, k, Cause::HoldCap).unwrap();
        }
        e.set_fence(u64::MAX).unwrap();
        wait_processed(&e, last + 25);
        for k in last + 1..last + 25 {
            let st = e.block_status(0, k).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
        }
    });
}

/// A re-route whose generation comes late skips the crossfade an on-time
/// engine runs from its index, so its blocks differ from an on-time
/// engine's until the crossfade's end: none of them carries the healthy
/// digest. Three routes at one index: their generations are built one at
/// a time, so the later ones are late.
#[test]
fn a_late_route_degrades_its_whole_crossfade_window() {
    undisturbed(|w| {
        let domain = Domain::new();
        let src: Vec<String> = (0..3).map(|_| new_id()).collect();
        let next: Vec<String> = (0..3).map(|_| new_id()).collect();
        let out = new_id();
        let _pilots: Vec<Pilot> = src
            .iter()
            .chain(&next)
            .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
            .collect();
        let ramp: u64 = 4_800;
        let v = serde_json::json!({
            "blockSize": B, "readerMarginSamples": MARGIN, "rampSamples": ramp,
            "libmxl": common::libmxl(),
            "domains": [{ "name": "d", "path": domain.0 }],
            "inputs": src.iter().enumerate().map(|(i, id)| serde_json::json!({
                "name": format!("in{i}"), "domain": "d", "flowId": id, "channels": 1 }))
                .collect::<Vec<_>>(),
            "outputs": [{ "name": "out0", "domain": "d", "flowId": out, "label": "late",
                          "group": "samplez-test", "role": "Audio 0", "channels": 3 }],
            "matrix": common::diagonal(3, 1.0),
            "rt": { "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let (report_tx, _reports) = std::sync::mpsc::sync_channel(64);
        let (status_tx, changes) = std::sync::mpsc::sync_channel(1 << 22);
        let process = samplez::rt::setup_process(&config.rt);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: Some(status_tx),
            hashes: None,
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let _guard = w.guard(&e, false);
        wait_for(Duration::from_secs(5), "attached", || {
            let s = e.status();
            s.outputs[0].realised && s.inputs.iter().all(|i| i.state == InputState::Attached)
        });
        e.set_fence(u64::MAX).unwrap();
        e.commit_from(0, processed(&e) + 2).unwrap();
        let at = (processed(&e) + 100) * B;
        for (i, flow_id) in next.iter().enumerate() {
            let entry = Entry::Route {
                slot: i,
                flow_id: flow_id.clone(),
            };
            e.append(i as u64 + 1, at, entry).unwrap();
        }
        e.commit(3);
        let end = (at + ramp) / B;
        // Evidence for every held block, so the replica stays committing.
        let t = Instant::now();
        while processed(&e) <= end + 5 {
            assert!(t.elapsed() < Duration::from_secs(30), "window not passed");
            let p = processed(&e);
            commit_through(&e, p.saturating_sub(100)..p, &[]);
            std::thread::sleep(Duration::from_millis(1));
        }
        let late = e.counters().generation_late.load(Relaxed);
        assert!(late >= 1, "no late generation");
        let mut first = std::collections::BTreeMap::new();
        for c in changes.try_iter() {
            if let StatusChange::Block { status, .. } = c {
                first.entry(status.k).or_insert(status);
            }
        }
        // Every block of the window was computed committing, with a digest.
        let window = at / B..end;
        let missing: Vec<u64> = window
            .clone()
            .filter(|k| first.get(k).is_none_or(|s| s.digest.is_none()))
            .collect();
        assert!(missing.is_empty(), "blocks without a digest: {missing:?}");
        let healthy: Vec<u64> = window
            .filter(|k| first.get(k).is_some_and(|s| s.digest == Some(HEALTHY)))
            .collect();
        assert!(
            healthy.is_empty(),
            "{} blocks of the crossfade window of a late route carry the healthy \
             digest, the first {:?}",
            healthy.len(),
            healthy.first()
        );
    });
}
