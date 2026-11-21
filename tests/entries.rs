//! Log entries on real libmxl flows: `InputDown` and `InputUp` at their
//! index on two engines, also across a forward skip, re-route crossfades,
//! entries applied again, the one pending generation an engine builds at a
//! time, re-routes on an engine with its own log and reports sent again.

mod common;

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::{Duration, Instant};

use common::{
    B, Domain, MARGIN, Pilot, RAMP, config, diagonal, new_id, start_replicated, wait_for,
};
use samplez::{BlockVerdict, Engine, Entry, InputState, Report};
use samplez_dsp::Ramp;

const SECOND: Duration = Duration::from_secs(1);

fn running(e: &Engine) {
    wait_for(5 * SECOND, "engine running", || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised)
            && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
}

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(5 * SECOND).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

/// [`now`], the verdicts read kept in `seen`.
fn now_kept(rx: &Receiver<BlockVerdict>, seen: &mut Vec<BlockVerdict>) -> u64 {
    seen.push(rx.recv_timeout(5 * SECOND).unwrap());
    seen.extend(rx.try_iter());
    seen.last().unwrap().k
}

/// Waits until block `k` has been written and is past the margin.
fn wait_past(rx: &Receiver<BlockVerdict>, k: u64, seen: &mut Vec<BlockVerdict>) {
    wait_for(10 * SECOND, "block written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > k)
    });
    std::thread::sleep(Duration::from_millis(50));
    seen.extend(rx.try_iter());
}

/// Block `k` of pilot channel `ch` through a ramp from `from` to `to`
/// starting at index `start`, as the engine applies it per block.
fn ramped(ch: usize, k: u64, from: f32, to: f32, start: u64) -> Vec<f32> {
    let mut x: Vec<f32> = (0..B).map(|n| common::pilot(ch, k * B + n)).collect();
    let pos = k * B - start;
    if pos < u64::from(RAMP) {
        let mut r = Ramp::new(from);
        let _ = r.set_target(to, RAMP);
        r.advance(pos as usize);
        r.process(&mut x);
    } else if to == 0.0 {
        x.fill(0.0);
    }
    x
}

/// Channel `c` of block `i` of planar blocks of `channels` channels.
fn block(v: &[f32], channels: usize, i: usize, c: usize) -> &[f32] {
    let b = B as usize;
    &v[(i * channels + c) * b..(i * channels + c + 1) * b]
}

/// Whether an engine with verdicts `v` computed block `k`. A block it
/// skipped after an overrun is zero-filled by libmxl, not computed.
fn computed(v: &[BlockVerdict], k: u64) -> bool {
    v.iter().any(|v| v.k == k)
}

/// Whether both engines computed block `k`.
fn both(va: &[BlockVerdict], vb: &[BlockVerdict], k: u64) -> bool {
    computed(va, k) && computed(vb, k)
}

/// Whether two blocks are bit-identical.
fn same(x: &[f32], y: &[f32]) -> bool {
    x.iter().zip(y).all(|(p, q)| p.to_bits() == q.to_bits())
}

fn report(rx: &Receiver<Report>, want: Report) {
    let deadline = std::time::Instant::now() + 10 * SECOND;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match rx.recv_timeout(left) {
            Ok(r) if r == want => return,
            Ok(_) => {}
            Err(e) => panic!("no {want:?}: {e}"),
        }
    }
}

/// `InputDown` silences the input from its index on whatever is readable,
/// `InputUp` fades it in from its index, on every engine the same, and
/// bit-identical between them. The supervisor polls an input that went
/// down and reports it readable.
#[test]
fn input_down_and_up_apply_at_their_index_on_every_engine() {
    let domain = Domain::new();
    let (src, out_a, out_b) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let cfg = |out: &str| {
        config(
            &domain.0,
            &[(&src, 2)],
            &[(out, 2)],
            diagonal(2, 1.0),
            MARGIN,
        )
    };
    let (a, rx_a, rep_a) = start_replicated(cfg(&out_a));
    std::thread::sleep(SECOND / 5);
    let (b, rx_b, rep_b) = start_replicated(cfg(&out_b));
    running(&a);
    running(&b);
    let k0 = now(&rx_a).max(now(&rx_b));
    let down = (k0 + 150) * B;
    for e in [&a, &b] {
        e.append(1, down, Entry::InputDown { slot: 0 }).unwrap();
        e.commit(1);
    }
    let (mut va, mut vb) = (Vec::new(), Vec::new());
    wait_past(&rx_a, down / B + 50, &mut va);
    wait_past(&rx_b, down / B + 50, &mut vb);
    let compare = |range: std::ops::Range<u64>,
                   va: &[BlockVerdict],
                   vb: &[BlockVerdict],
                   want: &dyn Fn(usize, u64) -> Vec<f32>| {
        let x = common::read_blocks(&domain.0, &out_a, 2, range.clone());
        let y = common::read_blocks(&domain.0, &out_b, 2, range.clone());
        // Each engine against the expected values on the blocks it
        // computed, and the two bit-identical where both did.
        let mut compared = 0;
        for (i, k) in range.clone().enumerate() {
            let (in_a, in_b) = (computed(va, k), computed(vb, k));
            for c in 0..2 {
                let (p, q) = (block(&x, 2, i, c), block(&y, 2, i, c));
                assert!(!in_a || p == want(c, k), "engine a block {k} channel {c}");
                assert!(!in_b || q == want(c, k), "engine b block {k} channel {c}");
                assert!(!(in_a && in_b) || same(p, q), "block {k}");
            }
            compared += u64::from(in_a && in_b);
        }
        assert!(
            compared * 4 > range.count() as u64 * 3,
            "{compared} blocks compared"
        );
    };
    compare(down / B - 20..down / B + 50, &va, &vb, &|c, k| {
        if k * B < down {
            ramped(c, k, 1.0, 1.0, 0)
        } else {
            vec![0.0; B as usize]
        }
    });
    // Down for a second while its head is on time: reported readable.
    report(&rep_a, Report::InputReadable { slot: 0 });
    report(&rep_b, Report::InputReadable { slot: 0 });
    let up = (now(&rx_a).max(now(&rx_b)) + 200) * B;
    for e in [&a, &b] {
        e.append(2, up, Entry::InputUp { slot: 0 }).unwrap();
        e.commit(2);
    }
    let end = up / B + 2 * u64::from(RAMP) / B;
    wait_past(&rx_a, end, &mut va);
    wait_past(&rx_b, end, &mut vb);
    compare(up / B - 20..end, &va, &vb, &|c, k| {
        if k * B < up {
            vec![0.0; B as usize]
        } else {
            ramped(c, k, 0.0, 1.0, up)
        }
    });
    for v in va.iter().chain(&vb).filter(|v| v.k > k0 + 50 && v.k < end) {
        let silent = (down / B..up / B).contains(&v.k);
        let fading = (up / B..up / B + u64::from(RAMP) / B).contains(&v.k);
        assert_eq!(v.silenced == 1, silent, "{v:?}");
        assert_eq!(v.fading == 1, fading, "{v:?}");
    }
    for e in [&a, &b] {
        let s = e.status();
        assert!(s.inputs[0].up && s.inputs[0].state == InputState::Attached);
        assert_eq!(s.diverged, None);
    }
    common::stop(b);
    common::stop(a);
}

/// A gain change to the gain a coefficient has, an `InputUp` for an input
/// that is up and a re-route to the flow a slot reads each go through
/// their whole path, generations included, and change nothing: the
/// output is bit-identical to an engine that got none of them.
#[test]
fn an_entry_applied_again_starts_no_fade_crossfade_or_ramp() {
    let domain = Domain::new();
    let (src, out_a, out_b) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let cfg = |out: &str| {
        config(
            &domain.0,
            &[(&src, 2)],
            &[(out, 2)],
            diagonal(2, 0.5),
            MARGIN,
        )
    };
    let (a, rx_a, _rep_a) = start_replicated(cfg(&out_a));
    let (b, rx_b, _rep_b) = start_replicated(cfg(&out_b));
    running(&a);
    running(&b);
    let k0 = now(&rx_a);
    let builds = a.status().generation.builds;
    let (at, at2) = ((k0 + 150) * B, (k0 + 250) * B);
    a.append(
        1,
        at,
        Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: None,
        },
    )
    .unwrap();
    a.append(2, at, Entry::InputUp { slot: 0 }).unwrap();
    let flow_id = src.clone();
    a.append(3, at2, Entry::Route { slot: 0, flow_id }).unwrap();
    a.commit(3);
    let end = at2 / B + 2 * u64::from(RAMP) / B;
    let (mut va, mut vb) = (Vec::new(), Vec::new());
    wait_past(&rx_a, end, &mut va);
    wait_past(&rx_b, end, &mut vb);
    for v in va.iter().filter(|v| v.k > k0 + 50 && v.k < end) {
        assert_eq!((v.silenced, v.fading, v.ramping), (0, 0, false), "{v:?}");
        let applied = u64::from(v.k >= at / B) * 2 + u64::from(v.k >= at2 / B);
        assert_eq!(v.commands_applied, applied, "{v:?}");
    }
    let range = k0 + 100..end;
    let x = common::read_blocks(&domain.0, &out_a, 2, range.clone());
    let y = common::read_blocks(&domain.0, &out_b, 2, range.clone());
    let mut compared = 0;
    for (i, k) in range.clone().enumerate() {
        if both(&va, &vb, k) {
            compared += 1;
            let n = 2 * B as usize;
            assert!(
                same(&x[i * n..(i + 1) * n], &y[i * n..(i + 1) * n]),
                "block {k}"
            );
        }
    }
    assert!(
        compared * 4 > range.count() * 3,
        "{compared} blocks compared"
    );
    let s = a.status();
    assert_eq!(s.generation.builds, builds + 2, "{:?}", s.generation);
    assert_eq!(s.generation.pending, None);
    assert_eq!(s.inputs[0].counters.below_last_read, 0);
    assert_eq!(a.counters().generation_late.load(Relaxed), 0);
    common::stop(b);
    common::stop(a);
}

/// An engine builds the generation of one entry at a time, on receipt and
/// before the commit, the next only once the RT thread swapped the first
/// in; a truncated entry's generation is dropped.
#[test]
fn generation_needing_entries_are_built_one_at_a_time() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (e, rx, _reports) = start_replicated(config(
        &domain.0,
        &[(&src, 2)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    ));
    running(&e);
    let generation = || e.status().generation;
    let k0 = now(&rx);
    let route = |id, at| {
        let flow_id = src.clone();
        e.append(id, at, Entry::Route { slot: 0, flow_id }).unwrap();
    };
    e.append(1, (k0 + 2000) * B, Entry::InputUp { slot: 0 })
        .unwrap();
    wait_for(5 * SECOND, "first generation built", || generation().ready);
    route(2, (k0 + 2100) * B);
    let builds = generation().builds;
    std::thread::sleep(SECOND / 5);
    let g = generation();
    assert_eq!((g.pending, g.ready, g.builds), (Some(1), true, builds));
    e.truncate(1);
    wait_for(5 * SECOND, "both entries dropped", || {
        generation().pending.is_none()
    });
    assert_eq!(generation().builds, builds);

    let k0 = now(&rx);
    let (at1, at2) = ((k0 + 200) * B, (k0 + 300) * B);
    e.append(1, at1, Entry::InputUp { slot: 0 }).unwrap();
    route(2, at2);
    e.commit(2);
    wait_for(SECOND, "first entry seen", || {
        generation().pending == Some(1)
    });
    let mut seen = Vec::new();
    let mut second = false;
    while seen
        .last()
        .is_none_or(|v: &BlockVerdict| v.k < at2 / B + 20)
    {
        let g = generation();
        let k = seen.last().map_or(k0, |v: &BlockVerdict| v.k);
        if k + 5 < at1 / B {
            assert_eq!(g.pending, Some(1), "block {k}");
        }
        second |= g.pending == Some(2) && g.ready;
        seen.extend(rx.recv_timeout(5 * SECOND));
        seen.extend(rx.try_iter());
    }
    std::thread::sleep(SECOND / 10);
    assert!(second, "the second generation was never built alone");
    assert_eq!(generation().pending, None);
    assert_eq!(generation().builds, builds + 2);
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    common::stop(e);
}

/// A generation build that leaves a slot out, its flow not found, is not
/// timed: buildLead comes from the 99.99th percentile of the build times,
/// which is the longest one until 10000 builds were timed, so a build that
/// waited for a missing flow would stamp every later entry that much
/// further ahead. A build that attaches every slot is timed.
#[test]
fn a_build_that_leaves_a_missing_flow_out_is_not_timed() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (e, rx, _reports) = start_replicated(config(
        &domain.0,
        &[(&src, 2)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    ));
    running(&e);
    let generation = || e.status().generation;
    let builds = generation().builds;
    let built = |flow_id: String| {
        let at = (now(&rx) + 2000) * B;
        e.append(1, at, Entry::Route { slot: 0, flow_id }).unwrap();
        wait_for(5 * SECOND, "the generation built", || generation().ready);
        let g = generation();
        e.truncate(1);
        wait_for(5 * SECOND, "the entry dropped", || {
            generation().pending.is_none()
        });
        g.builds
    };
    assert_eq!(built(new_id()), builds, "a missing flow");
    assert_eq!(built(src.clone()), builds + 1, "a flow that is there");
    common::stop(e);
}

/// Flow ids whose lookup [`slow_look`] makes wait.
static SLOW: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// The engine's lookup of a flow's directory, waiting 500 ms first for a
/// flow [`SLOW`] names, as a file system does that waits for a name.
fn slow_look(dir: &std::path::Path) -> bool {
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    if SLOW
        .lock()
        .unwrap()
        .iter()
        .any(|id| name.starts_with(id.as_str()))
    {
        std::thread::sleep(Duration::from_millis(500));
    }
    samplez_mxl::lookup::exists(dir)
}

/// A generation build opens no flow its lookup has not found: a re-route
/// to a flow that is there, but whose lookup does not answer within the
/// opener's 20 ms gate, leaves the slot out at once, saying so, instead of
/// libmxl's open waiting on a file system that waits for a missing name.
/// The generation is not late and blocks go on meanwhile.
#[test]
fn a_build_leaves_out_a_flow_its_lookup_has_not_found() {
    let domain = Domain::new();
    let (src, slow, out) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _slow = Pilot::start(&domain.0, &slow, 2, B, 0);
    SLOW.lock().unwrap().push(slow.clone());
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    cfg.look = slow_look;
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    let generation = || e.status().generation;
    let started = Instant::now();
    let at = (now(&rx) + 2000) * B;
    e.append(
        1,
        at,
        Entry::Route {
            slot: 0,
            flow_id: slow,
        },
    )
    .unwrap();
    wait_for(5 * SECOND, "the generation built", || generation().ready);
    let built = started.elapsed();
    let error = e.status().inputs[0].error.clone().unwrap_or_default();
    assert!(error.contains("did not answer"), "{error:?}");
    assert!(built < Duration::from_millis(250), "built in {built:?}");
    e.truncate(1);
    wait_for(5 * SECOND, "the entry dropped", || {
        generation().pending.is_none()
    });
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    common::stop(e);
}

/// An output flow is created without libmxl's check for a live writer,
/// which looks the flow's name up, while a lookup has not found its
/// directory: a live writer's flow is then opened and reported raced,
/// not waited for.
#[test]
fn an_output_is_created_without_waiting_for_its_lookup() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _live = Pilot::start(&domain.0, &out, 2, B, 0);
    SLOW.lock().unwrap().push(out.clone());
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    cfg.look = slow_look;
    let (e, _rx) = common::start(cfg);
    wait_for(5 * SECOND, "a creation tried", || {
        e.status().outputs[0].attempts > 0
    });
    let o = &e.status().outputs[0];
    assert!(!o.realised);
    assert_eq!(o.races, 1, "{:?}", o.reason);
    e.stop();
}

/// A re-route to a flow that is not there crossfades the old flow to
/// silence from the entry's index; the input is reported pruned, and once
/// the flow appears it is reported readable and an `InputUp` fades it in.
/// The other input, read through every new generation, stays exact.
#[test]
fn re_route_crossfades_at_its_index_and_leaves_other_inputs_exact() {
    // An overrun skips blocks, and the skipped ones can be the crossfade
    // itself, which this test exists to check: a run that left one
    // uncomputed is run again.
    let mut missed = Vec::new();
    for _ in 0..4 {
        let m = re_route_scenario();
        if m.is_empty() {
            return;
        }
        eprintln!("crossfade blocks {m:?} not computed: running it again");
        missed.push(m);
    }
    panic!("no run computed every crossfade block: {missed:?}");
}

/// One run of the re-route scenario: the crossfade blocks the engine did
/// not compute, which the run could not check.
fn re_route_scenario() -> Vec<u64> {
    let domain = Domain::new();
    let (src0, src1, next, out) = (new_id(), new_id(), new_id(), new_id());
    let _p0 = Pilot::start(&domain.0, &src0, 1, B, 0);
    let _p1 = Pilot::start(&domain.0, &src1, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src0, 1), (&src1, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    let (e, rx, reports) = start_replicated(cfg);
    running(&e);
    let k0 = now(&rx);
    let r = (k0 + 150) * B;
    let flow_id = next.clone();
    e.append(1, r, Entry::Route { slot: 1, flow_id }).unwrap();
    e.commit(1);
    let mut seen = Vec::new();
    let d = r + u64::from(RAMP);
    wait_past(&rx, d / B + 20, &mut seen);
    // Against the expected values on the blocks the engine computed: one
    // it skipped after an overrun is zero-filled by libmxl.
    let check = |range: std::ops::Range<u64>,
                 must: std::ops::Range<u64>,
                 seen: &[BlockVerdict],
                 want: &dyn Fn(u64) -> Vec<f32>|
     -> Vec<u64> {
        let x = common::read_blocks(&domain.0, &out, 2, range.clone());
        let mut compared = 0;
        for (i, k) in range.clone().enumerate() {
            if !computed(seen, k) {
                continue;
            }
            assert!(block(&x, 2, i, 0) == ramped(0, k, 1.0, 1.0, 0), "block {k}");
            assert!(
                block(&x, 2, i, 1) == want(k),
                "block {k} of the re-routed input"
            );
            compared += 1;
        }
        assert!(
            compared * 4 > range.count() as u64 * 3,
            "{compared} blocks compared"
        );
        must.filter(|&k| !computed(seen, k)).collect()
    };
    let ramp = u64::from(RAMP).div_ceil(B);
    let mut missed = check(r / B - 20..d / B + 20, r / B..d / B + 1, &seen, &|k| {
        if k * B < r {
            ramped(0, k, 1.0, 1.0, 0)
        } else {
            ramped(0, k, 1.0, 0.0, r)
        }
    });
    report(&reports, Report::InputPruned { slot: 1 });
    e.append(
        2,
        (now_kept(&rx, &mut seen) + 20) * B,
        Entry::InputDown { slot: 1 },
    )
    .unwrap();
    e.commit(2);
    let _p2 = Pilot::start(&domain.0, &next, 1, B, 0);
    report(&reports, Report::InputReadable { slot: 1 });
    let lead = e.status().generation.build_lead;
    let u = (now_kept(&rx, &mut seen) * B + lead).next_multiple_of(B);
    e.append(3, u, Entry::InputUp { slot: 1 }).unwrap();
    e.commit(3);
    let end = u / B + 2 * u64::from(RAMP) / B;
    wait_past(&rx, end, &mut seen);

    missed.extend(check(
        u / B - 20..end,
        u / B..u / B + ramp + 1,
        &seen,
        &|k| {
            if k * B < u {
                vec![0.0; B as usize]
            } else {
                ramped(0, k, 0.0, 1.0, u)
            }
        },
    ));
    for v in seen.iter().filter(|v| v.k > k0 + 50) {
        assert_eq!(v.silenced & 1, 0, "{v:?}");
    }
    let s = e.status();
    assert_eq!(s.inputs[1].flow_id, next);
    assert_eq!(s.inputs[0].counters.below_last_read, 0);
    assert_eq!(s.diverged, None);
    common::stop(e);
    missed
}

/// A generation built on append and handed over after its entry's commit
/// is swapped in at the entry's index, however the RT thread's take of the
/// entry and of the generation interleave.
#[test]
fn a_generation_handed_over_after_the_commit_is_swapped_in_on_time() {
    let domain = Domain::new();
    let (a, b, out) = (new_id(), new_id(), new_id());
    let _pa = Pilot::start(&domain.0, &a, 1, B, 0);
    let _pb = Pilot::start(&domain.0, &b, 1, B, 0);
    let (e, rx, _reports) = start_replicated(config(
        &domain.0,
        &[(&a, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    running(&e);
    let mut late = Vec::new();
    let mut seen = Vec::new();
    for id in 1..=20u64 {
        let at = (now(&rx) + 200) * B;
        let flow_id = if id % 2 == 1 { b.clone() } else { a.clone() };
        let before = e.counters().generation_late.load(Relaxed);
        e.append(id, at, Entry::Route { slot: 0, flow_id }).unwrap();
        wait_for(5 * SECOND, "generation built", || {
            e.status().generation.ready
        });
        e.commit(id);
        wait_past(&rx, at / B, &mut seen);
        wait_for(5 * SECOND, "generation swapped in", || {
            e.status().generation.pending.is_none()
        });
        if e.counters().generation_late.load(Relaxed) != before {
            late.push(id);
        }
    }
    assert!(late.is_empty(), "generations late for entries {late:?}");
    let s = e.status();
    assert_eq!(s.inputs[0].flow_id, a);
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// Re-routes of several inputs sent together on an engine with its own
/// log are stamped buildLead apart, so each generation is built before its
/// index: every input crossfades to its new flow, none is left silent.
#[test]
fn routes_sent_together_on_an_engine_with_its_own_log_each_swap_in() {
    let domain = Domain::new();
    let src: Vec<String> = (0..3).map(|_| new_id()).collect();
    let next: Vec<String> = (0..3).map(|_| new_id()).collect();
    let out = new_id();
    let _pilots: Vec<Pilot> = src
        .iter()
        .chain(&next)
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let ins: Vec<(&str, u32)> = src.iter().map(|s| (s.as_str(), 1)).collect();
    let (e, rx) = common::start(config(
        &domain.0,
        &ins,
        &[(&out, 3)],
        diagonal(3, 1.0),
        MARGIN,
    ));
    running(&e);
    let ats: Vec<u64> = (0..3).map(|i| e.route(i, &next[i], 0).unwrap()).collect();
    assert!(ats.windows(2).all(|w| w[1] > w[0]), "{ats:?}");
    let mut seen = Vec::new();
    let end = (ats[2] + u64::from(RAMP)) / B;
    // Past the 2 s a left-out slot may stay silent, so one that never
    // comes back is seen.
    wait_past(&rx, end + 2500, &mut seen);
    let s = e.status();
    // A slot is silent only where its reader was left out at its own
    // index, its open not done by the deadline, an open slower than
    // buildLead, and only until the poll's recovery brings it back:
    // anything else silencing it is a bug.
    for (slot, &at) in ats.iter().enumerate() {
        let silent: Vec<u64> = (seen.iter())
            .filter(|v| v.k >= at / B && v.silenced & (1 << slot) != 0)
            .map(|v| v.k)
            .collect();
        let Some(&last) = silent.last() else { continue };
        let record = (s.generation.left_out.iter()).find(|l| l.0 == slot && l.1 == at);
        assert!(
            record.is_some(),
            "slot {slot} silenced without a left-out reader: {silent:?}"
        );
        let ran = record.unwrap().2;
        assert!(
            ran.is_some_and(|ns| ns >= 2_000_000),
            "slot {slot} left out after its open ran {ran:?} ns"
        );
        assert!(
            last <= at / B + blocks_of(Duration::from_secs(2)),
            "slot {slot} silent until {last}, its open ran {ran:?} ns"
        );
    }
    let flows: Vec<&str> = s.inputs.iter().map(|i| i.flow_id.as_str()).collect();
    assert_eq!(flows, next.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// The flows whose lookup [`slow_open`] makes wait: each after that many
/// lookups of it, once, for that long.
static SLOW_OPEN: Mutex<Vec<(String, usize, Duration)>> = Mutex::new(Vec::new());

/// A lookup that waits for a flow [`SLOW_OPEN`] names: a reader whose
/// open takes that long.
fn slow_open(dir: &std::path::Path) -> bool {
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let wait = {
        let mut slow = SLOW_OPEN.lock().unwrap();
        match slow.iter().position(|e| name.starts_with(e.0.as_str())) {
            Some(n) if slow[n].1 > 0 => {
                slow[n].1 -= 1;
                None
            }
            Some(n) => Some(slow.remove(n).2),
            None => None,
        }
    };
    if let Some(wait) = wait {
        std::thread::sleep(wait);
    }
    samplez_mxl::lookup::exists(dir)
}

/// The flows whose reader opens [`slow_reader`] makes wait, once each.
static SLOW_READER: Mutex<Vec<(String, Duration)>> = Mutex::new(Vec::new());

/// The open hook of `samplez_mxl` (`set_open_delay`): an opener's open
/// of a reader of a flow [`SLOW_READER`] names waits, inside libmxl's
/// open as a slow one does; other threads' opens of it do not.
fn slow_reader(flow_id: &str) {
    if std::thread::current().name() != Some("samplez-open") {
        return;
    }
    let wait = {
        let mut slow = SLOW_READER.lock().unwrap();
        let n = slow.iter().position(|e| e.0 == flow_id);
        n.map(|n| slow.remove(n).1)
    };
    if let Some(wait) = wait {
        std::thread::sleep(wait);
    }
}

/// Samples of `d` in whole blocks, rounded up.
fn blocks_of(d: Duration) -> u64 {
    (d.as_nanos() as u64 * 48_000)
        .div_ceil(1_000_000_000)
        .div_ceil(B)
}

/// Re-routes sent together whose readers open slowly hold up nothing
/// but their own slots: a reader open after its entry's index leaves
/// its slot out of a generation swapped in on time, silent until the
/// next build once the reader is open, and every other slot crossfades
/// at its index; a reader slower than the gap between two re-routes but
/// open before its own index, its open having started with its entry,
/// is in time.
#[test]
fn a_slow_reader_open_silences_only_its_own_slot() {
    let domain = Domain::new();
    let src: Vec<String> = (0..3).map(|_| new_id()).collect();
    let next: Vec<String> = (0..3).map(|_| new_id()).collect();
    let out = new_id();
    let _pilots: Vec<Pilot> = (src.iter().chain(&next))
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let ins: Vec<(&str, u32)> = src.iter().map(|s| (s.as_str(), 1)).collect();
    let cfg = config(&domain.0, &ins, &[(&out, 3)], diagonal(3, 1.0), MARGIN);
    samplez_mxl::input::set_open_delay(Some(slow_reader));
    let (e, rx) = common::start(cfg);
    running(&e);
    // The re-routes are stamped buildLead apart: next[1]'s reader opens
    // past its index, next[2]'s within its own lead but past the gap
    // after next[1]'s index.
    let lead = e.status().generation.build_lead * 1_000_000 / 48_000;
    let lead = Duration::from_micros(lead);
    let slow = (4 * lead).max(Duration::from_millis(50));
    SLOW_READER
        .lock()
        .unwrap()
        .extend([(next[1].clone(), slow), (next[2].clone(), 5 * lead / 4)]);
    let ats: Vec<u64> = (0..3).map(|i| e.route(i, &next[i], 0).unwrap()).collect();
    let mut seen = Vec::new();
    let end = ats[1] / B + 1500;
    wait_past(&rx, end, &mut seen);
    let silent = |slot: usize| -> Vec<u64> {
        (seen.iter())
            .filter(|v| v.k >= ats[0] / B && v.silenced & (1 << slot) != 0)
            .map(|v| v.k)
            .collect()
    };
    assert_eq!(silent(0), Vec::<u64>::new(), "slot 0 silenced");
    assert_eq!(silent(2), Vec::<u64>::new(), "slot 2 silenced");
    let one = silent(1);
    let first = *one.first().expect("slot 1 silent without its reader");
    let last = *one.last().unwrap();
    // Silent once the crossfade from its old flow ended, on time.
    assert!(
        first <= (ats[1] + u64::from(RAMP)) / B + 1,
        "slot 1 silent from {first}, its index {}",
        ats[1] / B
    );
    // Its open started with its entry, before its index: it plays again
    // within the open's time, a build and a supervisor tick of the index.
    // A tick, and 20 ms for a loaded runner.
    let bound = ats[1] / B + blocks_of(slow + lead + Duration::from_millis(25));
    assert!(
        last <= bound,
        "slot 1 silent until {last}, its index {}, at most until {bound}",
        ats[1] / B
    );
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    let s = e.status();
    let flows: Vec<&str> = s.inputs.iter().map(|i| i.flow_id.as_str()).collect();
    assert_eq!(flows, next.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(s.inputs.iter().all(|i| i.state == InputState::Attached));
    assert_eq!(s.diverged, None);
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    common::stop(e);
}

/// When [`slow_open`]'s last wait for each flow began and ended.
static SLOW_TIMES: Mutex<Vec<(String, Instant, Instant)>> = Mutex::new(Vec::new());

/// [`slow_open`], recording its waits in [`SLOW_TIMES`].
fn timed_open(dir: &std::path::Path) -> bool {
    let started = Instant::now();
    let found = slow_open(dir);
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    let id = name.split('.').next().unwrap_or_default().to_owned();
    SLOW_TIMES
        .lock()
        .unwrap()
        .push((id, started, Instant::now()));
    found
}

/// A re-route appended already due, as a replica behind the leader gets
/// one, whose reader open is running when the supervisor takes the entry
/// in: the generation waits for no open once due, so it leaves the reader
/// out and is not late, and the slot joins once its reader is open.
#[test]
fn an_open_running_at_the_deadline_is_left_out() {
    let domain = Domain::new();
    let (src, next, out) = (new_id(), new_id(), new_id());
    let _p0 = Pilot::start(&domain.0, &src, 1, B, 0);
    let _p1 = Pilot::start(&domain.0, &next, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    samplez_mxl::input::set_open_delay(Some(slow_reader));
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    SLOW_READER
        .lock()
        .unwrap()
        .push((next.clone(), Duration::from_millis(50)));
    let api = common::api();
    let clock = || samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    // The engine's `earliest`: past the newest block due and what it
    // produced, and a lead of (maxCatchupBlocks + 2) blocks.
    let lead = 4 * B;
    let earliest = || (clock() - MARGIN).max(e.counters().produced.load(Relaxed)) + lead;
    // A supervisor tick and the hand-over margin, 5 ms and 2 ms: due when
    // the supervisor takes it in, where its open starts.
    let at = (earliest() + 240 + 96) / B * B;
    e.append(
        1,
        at,
        Entry::Route {
            slot: 0,
            flow_id: next.clone(),
        },
    )
    .unwrap();
    e.commit(1);
    let mut seen = Vec::new();
    wait_past(&rx, at / B + 50, &mut seen);
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    let s = e.status();
    assert!(
        (s.generation.left_out.iter()).any(|l| l.0 == 0 && l.1 == at),
        "not left out: {:?}",
        s.generation.left_out
    );
    wait_for(5 * SECOND, "the slot attached on its new flow", || {
        let s = e.status();
        s.inputs[0].state == InputState::Attached && s.inputs[0].flow_id == next
    });
    common::stop(e);
}

/// While an entry not handed over falls due within a poll period and a
/// tick, the supervisor neither polls nor takes a poll hold, whose opens
/// and reads could stall it past that entry's deadline: a pruned slot's
/// flow is not looked up, by its poll or by a slot pruned meanwhile, until
/// the entry is gone. An entry due seconds ahead holds back no poll, and
/// its generation is not late.
#[test]
fn polls_wait_only_for_an_entry_due_soon() {
    let domain = Domain::new();
    let ids: Vec<String> = (0..5).map(|_| new_id()).collect();
    let (next, out) = (&ids[3], &ids[4]);
    let mut pilots: Vec<Option<Pilot>> = (ids[..4].iter())
        .map(|id| Some(Pilot::start(&domain.0, id, 1, B, 0)))
        .collect();
    let mut cfg = config(
        &domain.0,
        &[(&ids[0], 1), (&ids[1], 1), (&ids[2], 1)],
        &[(out, 3)],
        diagonal(3, 1.0),
        MARGIN,
    );
    cfg.look = timed_open;
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    let looked = |id: &str, since: Instant| {
        (SLOW_TIMES.lock().unwrap().iter()).any(|t| t.0 == id && t.1 >= since)
    };
    let pruned = |slot: usize| {
        wait_for(5 * SECOND, "the slot pruned", || {
            e.status().inputs[slot].state == InputState::Pruned
        });
    };
    let route = || Entry::Route {
        slot: 0,
        flow_id: next.clone(),
    };
    // Slot 1 pruned, its flow gone: its poll looks it up every 100 ms.
    let stopped = Instant::now();
    pilots[1] = None;
    pruned(1);
    wait_for(5 * SECOND, "slot 1's flow looked up", || {
        looked(&ids[1], stopped)
    });

    // Due within the window, and never handed over while uncommitted.
    e.append(1, (now(&rx) + 10) * B, route()).unwrap();
    wait_for(5 * SECOND, "the entry taken in", || {
        e.status().generation.pending.is_some()
    });
    // Past a lookup the last poll before it started.
    std::thread::sleep(Duration::from_millis(100));
    let held = Instant::now();
    pilots[2] = None;
    pruned(2);
    std::thread::sleep(Duration::from_millis(300));
    assert!(!looked(&ids[1], held), "slot 1's flow polled");
    assert!(!looked(&ids[2], held), "slot 2's poll hold taken");
    e.truncate(1);
    wait_for(5 * SECOND, "slot 2's flow looked up", || {
        looked(&ids[2], held)
    });
    assert!(looked(&ids[1], held));

    // Due seconds ahead: polls go on.
    let late = e.counters().generation_late.load(Relaxed);
    let at = (now(&rx) + 4000) * B;
    e.append(1, at, route()).unwrap();
    wait_for(5 * SECOND, "the generation built", || {
        e.status().generation.ready
    });
    let since = Instant::now();
    assert!(e.status().generation.pending.is_some());
    wait_for(SECOND, "slot 1's flow polled", || looked(&ids[1], since));
    e.commit(1);
    let mut seen = Vec::new();
    wait_past(&rx, at / B + 50, &mut seen);
    assert_eq!(e.counters().generation_late.load(Relaxed), late);
    let s = e.status();
    assert_eq!(s.inputs[0].flow_id, *next);
    assert_eq!(s.inputs[0].state, InputState::Attached);
    common::stop(e);
}

/// While an entry not handed over falls due within a poll period and a
/// tick, the supervisor creates no output writer, whose instance and flow
/// creation could stall it past that entry's deadline: an output enabled
/// meanwhile is realised once the entry is gone.
#[test]
fn outputs_wait_for_an_entry_due_soon() {
    let domain = Domain::new();
    let (src, next, out) = (new_id(), new_id(), new_id());
    let _pilots = [&src, &next].map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    let mut c = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    c.outputs[0].enabled = false;
    let (e, rx, _reports) = start_replicated(c);
    let output = || e.status().outputs[0].clone();
    // The output enabled 100 blocks on; the re-route, never committed,
    // falls due soon before that, about 40 blocks on.
    let k = now(&rx);
    let enable = Entry::Output {
        slot: 0,
        enabled: true,
    };
    e.append(1, (k + 100) * B, enable).unwrap();
    let route = Entry::Route {
        slot: 0,
        flow_id: next.clone(),
    };
    e.append(2, (k + 150) * B, route).unwrap();
    e.commit(1);
    wait_for(5 * SECOND, "the output enabled", || output().enabled);
    std::thread::sleep(Duration::from_millis(300));
    let o = output();
    assert!(!o.realised && !domain.has_flow(&out), "{o:?}");
    e.truncate(2);
    wait_for(10 * SECOND, "output realised", || {
        let o = output();
        o.realised && domain.has_flow(&out)
    });
    common::stop(e);
}

/// Outputs enabled together are created one after another, each taking
/// its lookup's gate wait on the supervisor: an entry falling due soon in
/// the middle of that pass stops it before the next output, and no output
/// is created while the entry waits for its hand-over.
#[test]
fn an_entry_falling_due_stops_a_pass_creating_outputs() {
    let domain = Domain::new();
    let (src, next) = (new_id(), new_id());
    let outs: Vec<String> = (0..16).map(|_| new_id()).collect();
    let _pilots = [&src, &next].map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    let outputs: Vec<(&str, u32)> = outs.iter().map(|o| (o.as_str(), 1)).collect();
    let mut c = config(
        &domain.0,
        &[(&src, 1)],
        &outputs,
        vec![vec![1.0; 16]],
        MARGIN,
    );
    for o in &mut c.outputs {
        o.enabled = false;
    }
    // Each output's lookup waits past the 20 ms gate, so each creation
    // takes the gate's whole wait.
    c.look = slow_open;
    (SLOW_OPEN.lock().unwrap())
        .extend((outs.iter()).map(|o| (o.clone(), 0, Duration::from_millis(100))));
    let (e, rx, _reports) = start_replicated(c);
    let enabled = || e.status().outputs.iter().all(|o| o.enabled);
    let realised = || e.status().outputs.iter().filter(|o| o.realised).count();
    // All enabled at one index 100 blocks on. The re-route, never
    // committed, at 300 falls due soon at 188 (a poll period and a tick,
    // and a tick and the margin, before it), 88 ms into a pass that takes
    // 320 ms at least.
    let k = now(&rx);
    for o in 0..16 {
        let enable = Entry::Output {
            slot: o,
            enabled: true,
        };
        e.append(1 + o as u64, (k + 100) * B, enable).unwrap();
    }
    let route = Entry::Route {
        slot: 0,
        flow_id: next.clone(),
    };
    e.append(17, (k + 300) * B, route).unwrap();
    e.commit(16);
    wait_for(5 * SECOND, "the outputs enabled", enabled);
    // Past the end of a pass that went on.
    std::thread::sleep(Duration::from_millis(500));
    let n = realised();
    assert!(n < 16, "{n} outputs created while the entry was due soon");
    e.truncate(17);
    wait_for(10 * SECOND, "every output realised", || realised() == 16);
    common::stop(e);
}

/// A re-route opens only the flow it brings in: the other slot keeps the
/// live generation's reader, so a lookup of its flow that would wait is
/// never made, and it is never silenced.
#[test]
fn a_re_route_reopens_no_other_slots_flow() {
    let domain = Domain::new();
    let (src0, src1, next, out) = (new_id(), new_id(), new_id(), new_id());
    let _pilots: Vec<Pilot> = [&src0, &src1, &next]
        .iter()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let mut cfg = config(
        &domain.0,
        &[(&src0, 1), (&src1, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    cfg.look = slow_open;
    let (e, rx) = common::start(cfg);
    running(&e);
    SLOW_OPEN
        .lock()
        .unwrap()
        .push((src1.clone(), 0, Duration::from_millis(50)));
    let at = e.route(0, &next, 0).unwrap();
    let mut seen = Vec::new();
    wait_past(&rx, at / B + 50, &mut seen);
    let silenced: Vec<u64> = (seen.iter())
        .filter(|v| v.k >= at / B && v.silenced != 0)
        .map(|v| v.k)
        .collect();
    assert_eq!(silenced, Vec::<u64>::new());
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    assert!(
        SLOW_OPEN.lock().unwrap().iter().any(|e| e.0 == src1),
        "src1 looked up again"
    );
    common::stop(e);
}

/// Lookups that wait, as on a file system that waits for a missing name,
/// hold an opener for the gate's 20 ms at most: with re-routes of four
/// slots to flows whose lookups wait, as many as there are openers, a
/// fifth re-route to a flow that is there swaps in on time, or is left out
/// and joins once its reader is open, within a stated bound; no
/// generation is late and a slot no entry touches is never silenced.
#[test]
fn lookups_that_wait_hold_no_opener() {
    let domain = Domain::new();
    let src: Vec<String> = (0..6).map(|_| new_id()).collect();
    let gone: Vec<String> = (0..4).map(|_| new_id()).collect();
    let (next, out) = (new_id(), new_id());
    let _pilots: Vec<Pilot> = (src.iter().chain([&next]))
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let ins: Vec<(&str, u32)> = src.iter().map(|s| (s.as_str(), 1)).collect();
    let mut cfg = config(&domain.0, &ins, &[(&out, 6)], diagonal(6, 1.0), MARGIN);
    cfg.look = slow_open;
    let (e, rx) = common::start(cfg);
    running(&e);
    SLOW_OPEN.lock().unwrap().extend(
        gone.iter()
            .map(|id| (id.clone(), 0, Duration::from_secs(2))),
    );
    for (slot, id) in gone.iter().enumerate() {
        e.route(slot, id, 0).unwrap();
    }
    let at = e.route(4, &next, 0).unwrap();
    let mut seen = Vec::new();
    wait_past(&rx, at / B + 300, &mut seen);
    let silent = |slot: usize| -> Vec<u64> {
        (seen.iter())
            .filter(|v| v.k >= at / B && v.silenced & (1 << slot) != 0)
            .map(|v| v.k)
            .collect()
    };
    assert_eq!(silent(5), Vec::<u64>::new(), "the untouched slot silenced");
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    // The gate's 20 ms, a build and a tick, and 20 ms for a loaded runner.
    let lead = e.status().generation.build_lead * 1_000_000 / 48_000;
    let bound = at / B + blocks_of(Duration::from_millis(45) + Duration::from_micros(lead));
    assert!(
        silent(4).iter().all(|&k| k <= bound),
        "slot 4 silent past {bound}: {:?}",
        silent(4)
    );
    let s = e.status();
    assert_eq!(s.inputs[4].flow_id, next);
    assert_eq!(s.inputs[4].state, InputState::Attached);
    common::stop(e);
}

/// Every opener held in a slow reader open: a re-route whose open cannot
/// start before its deadline is left out, its generation swapped in on
/// time, and the slot joins once an opener is free and its reader open.
#[test]
fn an_open_that_cannot_start_by_its_deadline_is_left_out() {
    let domain = Domain::new();
    let src: Vec<String> = (0..5).map(|_| new_id()).collect();
    let busy: Vec<String> = (0..4).map(|_| new_id()).collect();
    let (next, out) = (new_id(), new_id());
    let _pilots: Vec<Pilot> = (src.iter().chain(&busy).chain([&next]))
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let ins: Vec<(&str, u32)> = src.iter().map(|s| (s.as_str(), 1)).collect();
    let cfg = config(&domain.0, &ins, &[(&out, 5)], diagonal(5, 1.0), MARGIN);
    samplez_mxl::input::set_open_delay(Some(slow_reader));
    let (e, rx) = common::start(cfg);
    running(&e);
    let slow = Duration::from_millis(500);
    SLOW_READER
        .lock()
        .unwrap()
        .extend(busy.iter().map(|id| (id.clone(), slow)));
    for (slot, id) in busy.iter().enumerate() {
        e.route(slot, id, 0).unwrap();
    }
    let at = e.route(4, &next, 0).unwrap();
    let mut seen = Vec::new();
    wait_past(&rx, at / B + 1000, &mut seen);
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    let s = e.status();
    assert_eq!(s.inputs[4].flow_id, next);
    assert_eq!(s.inputs[4].state, InputState::Attached);
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    common::stop(e);
}

/// A re-route whose flow a dropped entry's open still looks up adopts that
/// open rather than wait for it: no generation is late, and the slot,
/// left out while the open runs, plays again within the poll's recovery
/// period at the latest.
#[test]
fn an_adopted_slow_open_rejoins_within_the_poll_recovery_limit() {
    let domain = Domain::new();
    let (src, next, out) = (new_id(), new_id(), new_id());
    let _p0 = Pilot::start(&domain.0, &src, 1, B, 0);
    let _p1 = Pilot::start(&domain.0, &next, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    cfg.look = slow_open;
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    let slow = Duration::from_millis(300);
    // The dropped entry's lookup and the next one both wait, so the
    // re-route's own open does not answer within the gate either.
    SLOW_OPEN
        .lock()
        .unwrap()
        .extend([(next.clone(), 0, slow), (next.clone(), 0, slow)]);
    let route = || Entry::Route {
        slot: 0,
        flow_id: next.clone(),
    };
    e.append(1, (now(&rx) + 2000) * B, route()).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    e.truncate(1);
    let at = (now(&rx) + 100) * B;
    e.append(1, at, route()).unwrap();
    e.commit(1);
    let mut seen = Vec::new();
    // Past the poll's recovery second and the build re-adding the slot.
    wait_past(&rx, at / B + 2500, &mut seen);
    wait_for(3 * SECOND, "the slot attached on its new flow", || {
        let s = e.status();
        s.inputs[0].state == InputState::Attached && s.inputs[0].flow_id == next
    });
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    let last = (seen.iter())
        .filter(|v| v.k >= at / B && v.silenced != 0)
        .map(|v| v.k)
        .next_back();
    // The poll's recovery second, then the re-attach and the rebuild that
    // adds the slot back, with room for a loaded runner.
    let bound = at / B + blocks_of(Duration::from_secs(2));
    assert!(
        last.is_none_or(|k| k <= bound),
        "silent until {last:?}, bound {bound}"
    );
    let s = e.status();
    assert_eq!(s.inputs[0].flow_id, next);
    assert_eq!(s.inputs[0].state, InputState::Attached);
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    common::stop(e);
}

/// A rebuild waiting for a reader whose open is slow holds back no
/// entry: a re-route appended meanwhile swaps in at its index, its slot
/// crossfading and never silenced, while the generation leaves out the
/// slot whose open is still running, which joins once it is open.
#[test]
fn a_rebuild_waiting_for_a_slow_open_holds_back_no_re_route() {
    let domain = Domain::new();
    let (src0, src1, x, y, out) = (new_id(), new_id(), new_id(), new_id(), new_id());
    let _pilots: Vec<Pilot> = [&src0, &src1, &x, &y]
        .iter()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let mut cfg = config(
        &domain.0,
        &[(&src0, 1), (&src1, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    cfg.look = slow_open;
    let (e, rx) = common::start(cfg);
    running(&e);
    // src1's next open after the first re-route's, the rebuild's once
    // that re-route's crossfade ended, waits.
    let slow = Duration::from_millis(300);
    SLOW_OPEN.lock().unwrap().push((src1.clone(), 1, slow));
    let first = e.route(0, &x, 0).unwrap();
    let mut seen = Vec::new();
    wait_past(&rx, (first + u64::from(RAMP)) / B + 20, &mut seen);
    // The rebuild's open of src1 runs for another 200 ms or more.
    let at = e.route(0, &y, 0).unwrap();
    let end = at / B + 1000;
    wait_past(&rx, end, &mut seen);
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    let silent = |slot: usize| -> Vec<u64> {
        (seen.iter())
            .filter(|v| v.k >= first / B && v.silenced & (1 << slot) != 0)
            .map(|v| v.k)
            .collect()
    };
    assert_eq!(silent(0), Vec::<u64>::new(), "the re-routed slot silenced");
    let lead = e.status().generation.build_lead * 1_000_000 / 48_000;
    let bound = at / B + blocks_of(slow + Duration::from_micros(lead) + Duration::from_millis(25));
    assert!(
        silent(1).iter().all(|&k| k <= bound),
        "slot 1 silent past {bound}: {:?}",
        silent(1)
    );
    let s = e.status();
    assert_eq!(s.inputs[0].flow_id, y);
    assert!(s.inputs.iter().all(|i| i.state == InputState::Attached));
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    common::stop(e);
}

/// Generation-needing entries at one index, as a replica that lagged the
/// leader by more than buildLead gets them: the first swaps in on time,
/// the others are applied before their generations and each is swapped
/// in once it arrives, handed over past the hand-over margin. No input
/// stays silent.
#[test]
fn entries_at_one_index_each_get_their_generation() {
    let domain = Domain::new();
    let src: Vec<String> = (0..3).map(|_| new_id()).collect();
    let next: Vec<String> = (0..3).map(|_| new_id()).collect();
    let out = new_id();
    let _pilots: Vec<Pilot> = src
        .iter()
        .chain(&next)
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let ins: Vec<(&str, u32)> = src.iter().map(|s| (s.as_str(), 1)).collect();
    let (e, rx, _reports) = start_replicated(config(
        &domain.0,
        &ins,
        &[(&out, 3)],
        diagonal(3, 1.0),
        MARGIN,
    ));
    running(&e);
    let at = (now(&rx) + 200) * B;
    for (i, flow_id) in next.iter().enumerate() {
        let entry = Entry::Route {
            slot: i,
            flow_id: flow_id.clone(),
        };
        e.append(i as u64 + 1, at, entry).unwrap();
    }
    e.commit(3);
    wait_for(10 * SECOND, "every input on its new flow", || {
        let s = e.status();
        s.generation.pending.is_none() && s.inputs.iter().zip(&next).all(|(i, n)| &i.flow_id == n)
    });
    let mut seen = Vec::new();
    let k = now(&rx) + u64::from(RAMP) / B;
    wait_past(&rx, k + 100, &mut seen);
    for v in seen.iter().filter(|v| v.k > k) {
        assert_eq!(v.silenced, 0, "{v:?}");
    }
    let s = e.status();
    assert!(e.counters().generation_late.load(Relaxed) >= 2);
    // Each handed over once the RT thread had passed its index.
    assert!(e.counters().generation_past_margin.load(Relaxed) >= 2);
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// An input that is down because its flow is gone, re-routed to a live
/// flow, comes up on it: the supervisor polls the flow the input reads.
#[test]
fn a_down_input_re_routed_to_a_live_flow_comes_up() {
    let domain = Domain::new();
    let (dead, live, out) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &live, 1, B, 0);
    let (e, _rx) = common::start(config(
        &domain.0,
        &[(&dead, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    wait_for(10 * SECOND, "input down", || !e.status().inputs[0].up);
    e.route(0, &live, 0).unwrap();
    wait_for(10 * SECOND, "input up on the live flow", || {
        let s = e.status();
        s.inputs[0].up && s.inputs[0].state == InputState::Attached
    });
    assert_eq!(e.status().inputs[0].flow_id, live);
    common::stop(e);
}

/// A detached input crossfades to silence at its index and stays silent
/// and detached, neither polled nor reported and no silenced block, while
/// the writer of the flow it read stops and starts again; a `Route` fades
/// it in on that flow from silence, as from a flow that is not there.
#[test]
fn a_detached_input_stays_silent_and_unreported_until_routed() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    let (e, rx, reports) = start_replicated(cfg);
    running(&e);
    assert!(
        matches!(e.detach(0, 0), Err(samplez::EntryError::Invalid(_))),
        "a detach outside the replicated log taken"
    );
    let startup: Vec<Report> = reports.try_iter().collect();
    let mut seen = Vec::new();
    let k0 = now_kept(&rx, &mut seen);
    let d = (k0 + 150) * B;
    e.append(1, d, Entry::Detach { slot: 0 }).unwrap();
    e.commit(1);
    let end = (d + u64::from(RAMP)) / B + 20;
    wait_past(&rx, end, &mut seen);
    let check =
        |range: std::ops::Range<u64>, seen: &[BlockVerdict], want: &dyn Fn(u64) -> Vec<f32>| {
            let x = common::read_blocks(&domain.0, &out, 1, range.clone());
            let mut compared = 0;
            for (i, k) in range.clone().enumerate() {
                if computed(seen, k) {
                    assert!(block(&x, 1, i, 0) == want(k), "block {k}");
                    compared += 1;
                }
            }
            assert!(
                compared * 4 > range.count() as u64 * 3,
                "{compared} blocks compared"
            );
        };
    check(d / B - 20..end, &seen, &|k| {
        if k * B < d {
            ramped(0, k, 1.0, 1.0, 0)
        } else {
            ramped(0, k, 1.0, 0.0, d)
        }
    });
    assert_eq!(e.status().inputs[0].state, InputState::Detached);
    // The writer stops, and starts again after more than a poll's
    // recovery.
    drop(pilot);
    std::thread::sleep(2 * SECOND);
    let pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    std::thread::sleep(3 * SECOND);
    let s = e.status();
    assert!(
        s.inputs[0].up && s.inputs[0].state == InputState::Detached,
        "{:?}",
        s.inputs[0]
    );
    assert_eq!(s.inputs[0].flow_id, src);
    let lead = s.generation.build_lead;
    let u = ((now_kept(&rx, &mut seen) + 20) * B + lead).next_multiple_of(B);
    let flow_id = src.clone();
    e.append(2, u, Entry::Route { slot: 0, flow_id }).unwrap();
    e.commit(2);
    let end = u / B + 2 * u64::from(RAMP) / B;
    wait_past(&rx, end, &mut seen);
    check(u / B - 20..end, &seen, &|k| {
        if k * B < u {
            vec![0.0; B as usize]
        } else {
            ramped(0, k, 0.0, 1.0, u)
        }
    });
    // Silent by the log from the crossfade's end to the route: no block
    // counts it silenced.
    for v in seen.iter().filter(|v| v.k > k0 + 50) {
        assert_eq!(v.silenced & 1, 0, "{v:?}");
    }
    let s = e.status();
    assert_eq!(s.inputs[0].state, InputState::Attached);
    assert_eq!(s.diverged, None);
    let after: Vec<Report> = reports.try_iter().collect();
    assert_eq!(after, [], "reported after {startup:?}");
    common::stop(e);
    drop(pilot);
}

/// On an engine with its own log a detached input keeps its log state:
/// the writer of the flow it read stopping stamps no `InputDown`, and it
/// stays detached until `route` reads the flow again. A detach of no
/// slot or inside a block is refused.
#[test]
fn a_detached_input_on_an_engine_with_its_own_log_keeps_its_state() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (e, _rx) = common::start(config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    running(&e);
    for (slot, at) in [(1, 0), (0, B + 8)] {
        assert!(
            matches!(e.detach(slot, at), Err(samplez::EntryError::Invalid(_))),
            "a detach of slot {slot} at {at} taken"
        );
    }
    e.detach(0, 0).unwrap();
    wait_for(10 * SECOND, "input detached", || {
        e.status().inputs[0].state == InputState::Detached
    });
    drop(pilot);
    std::thread::sleep(3 * SECOND);
    let s = e.status();
    assert!(
        s.inputs[0].up && s.inputs[0].state == InputState::Detached,
        "{:?}",
        s.inputs[0]
    );
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    e.route(0, &src, 0).unwrap();
    wait_for(10 * SECOND, "input attached on its flow again", || {
        let s = e.status();
        s.inputs[0].up && s.inputs[0].state == InputState::Attached
    });
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// An engine starts with an output configured disabled and an input
/// configured without a flow as a disabled `Output` and a `Detach` leave
/// them: the output creates no flow and the input opens no reader, is
/// not polled and is not reported, while the engine computes. Enabled,
/// the output's flow is created; routed, the input reads its flow.
#[test]
fn an_engine_starts_with_an_output_disabled_and_an_input_detached() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut c = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    c.inputs[0].flow_id = None;
    c.outputs[0].enabled = false;
    let (e, rx) = common::start(c);
    now(&rx);
    std::thread::sleep(SECOND);
    let s = e.status();
    let (input, output) = (&s.inputs[0], &s.outputs[0]);
    assert_eq!(input.state, InputState::Detached, "{input:?}");
    assert!(
        input.error.is_none() && input.flow_id.is_empty(),
        "{input:?}"
    );
    assert!(
        !output.enabled && !output.realised && output.reason.as_deref() == Some("disabled"),
        "{output:?}"
    );
    assert!(!domain.has_flow(&out), "a disabled output's flow created");
    e.set_output_enabled(0, true, 0).unwrap();
    wait_for(10 * SECOND, "output realised", || {
        let o = e.status().outputs[0].clone();
        o.enabled && o.realised && domain.has_flow(&out)
    });
    e.route(0, &src, 0).unwrap();
    wait_for(10 * SECOND, "input attached on its flow", || {
        let s = e.status();
        s.inputs[0].state == InputState::Attached && s.inputs[0].flow_id == src
    });
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// A disabled output's flow is gone from the domain while the engine runs
/// on; enabled again, a writer another process holds on the flow is
/// reported and the output realised once that writer is gone, written
/// from then on.
#[test]
fn a_disabled_output_leaves_no_flow_and_is_created_again_when_enabled() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (e, rx) = common::start(config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    running(&e);
    assert!(domain.has_flow(&out));
    for (slot, at) in [(1, 0), (0, B + 8)] {
        assert!(
            matches!(
                e.set_output_enabled(slot, false, at),
                Err(samplez::EntryError::Invalid(_))
            ),
            "output {slot} disabled at {at}"
        );
    }
    let output = || e.status().outputs[0].clone();
    e.set_output_enabled(0, false, 0).unwrap();
    wait_for(10 * SECOND, "output released, its flow deleted", || {
        let o = output();
        !o.enabled && !o.realised && !domain.has_flow(&out)
    });
    std::thread::sleep(SECOND);
    assert!(
        !domain.has_flow(&out),
        "the flow created again while disabled"
    );
    // Another writer on the flow when it is enabled.
    let foreign = Pilot::start(&domain.0, &out, 1, B, 0);
    wait_for(10 * SECOND, "foreign flow", || domain.has_flow(&out));
    e.set_output_enabled(0, true, 0).unwrap();
    wait_for(10 * SECOND, "the other writer reported", || {
        let o = output();
        o.enabled
            && !o.realised
            && o.reason.as_deref() == Some("another writer is live on the flow")
    });
    drop(foreign);
    wait_for(40 * SECOND, "output realised again", || {
        output().realised && domain.has_flow(&out)
    });
    // Written from then on: the pilot through a gain of 1.
    let mut seen = Vec::new();
    let k = now_kept(&rx, &mut seen) + 2;
    wait_past(&rx, k + 10, &mut seen);
    let x = common::read_blocks(&domain.0, &out, 1, k..k + 10);
    for (i, k) in (k..k + 10).enumerate() {
        if computed(&seen, k) {
            assert!(block(&x, 1, i, 0) == ramped(0, k, 1.0, 1.0, 0), "block {k}");
        }
    }
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// Of the `Output`s of one log entry the last counts, on an engine that
/// follows a replicated log.
#[test]
fn an_outputs_last_enable_or_disable_in_a_log_entry_counts() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (e, rx, _reports) = start_replicated(config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    running(&e);
    assert!(
        matches!(
            e.set_output_enabled(0, false, 0),
            Err(samplez::EntryError::Invalid(_))
        ),
        "an output disabled outside the replicated log"
    );
    let set = |enabled| Entry::Output { slot: 0, enabled };
    let output = || e.status().outputs[0].clone();
    let mut seen = Vec::new();
    let at = (now_kept(&rx, &mut seen) + 100) * B;
    e.append_batch(1, at, &[set(false), set(true)]).unwrap();
    e.commit(1);
    wait_past(&rx, at / B + 10, &mut seen);
    let o = output();
    assert!(o.enabled && o.realised && domain.has_flow(&out), "{o:?}");
    let at = (now_kept(&rx, &mut seen) + 100) * B;
    e.append_batch(2, at, &[set(true), set(false)]).unwrap();
    e.commit(2);
    wait_for(10 * SECOND, "output released, its flow deleted", || {
        let o = output();
        !o.enabled && !o.realised && !domain.has_flow(&out)
    });
    let at = (now_kept(&rx, &mut seen) + 100) * B;
    e.append(3, at, set(true)).unwrap();
    e.commit(3);
    wait_for(10 * SECOND, "output realised again", || {
        let o = output();
        o.enabled && o.realised && domain.has_flow(&out)
    });
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// A re-route at an index inside a block is refused, as a gain or strip
/// change there is: taken at the next block start it would be late, its
/// generation dropped although the call succeeded.
#[test]
fn a_re_route_inside_a_block_is_refused() {
    let domain = Domain::new();
    let (src, next, out) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let _next = Pilot::start(&domain.0, &next, 1, B, 0);
    let (e, _rx) = common::start(config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    running(&e);
    // Past the earliest index a re-route is stamped at, so the index the
    // caller gave stands.
    let ahead = (common::processed(&e) + 2000) * B + 8;
    for at in [1000, ahead] {
        assert_ne!(at % B, 0);
        assert!(
            matches!(e.route(0, &next, at), Err(samplez::EntryError::Invalid(_))),
            "a re-route at {at} taken"
        );
    }
    assert_eq!(e.status().generation.pending, None);
    common::stop(e);
}

/// A report the log's channel had no room for is sent again: an input
/// pruned while the channel was full is reported pruned once there is
/// room, and so is one that became readable while it was full.
#[test]
fn a_report_the_channel_had_no_room_for_is_sent_again() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let filler = Report::InputReadable { slot: 1 };
    let (report_tx, reports) = sync_channel(1);
    report_tx.send(filler).unwrap();
    let (verdict_tx, rx) = sync_channel(1 << 16);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    let process = samplez::rt::setup_process(&cfg.rt);
    let replication = samplez::Replication {
        voters: 1,
        reports: report_tx.clone(),
        statuses: None,
        hashes: None,
    };
    let e = Engine::start_replicated(cfg, process, Some(verdict_tx), replication).unwrap();
    let dropped = || e.counters().reports_dropped.load(Relaxed);
    wait_for(10 * SECOND, "a report dropped", || dropped() >= 1);
    assert_eq!(reports.recv().unwrap(), filler);
    report(&reports, Report::InputPruned { slot: 0 });

    e.append(1, (now(&rx) + 20) * B, Entry::InputDown { slot: 0 })
        .unwrap();
    e.commit(1);
    wait_for(5 * SECOND, "input down", || !e.status().inputs[0].up);
    report_tx.send(filler).unwrap();
    let before = dropped();
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    wait_for(10 * SECOND, "a report dropped", || dropped() > before);
    assert_eq!(reports.recv().unwrap(), filler);
    report(&reports, Report::InputReadable { slot: 0 });
    common::stop(e);
}

/// A child process of this test binary, continued and killed when the
/// test ends before it was waited for: a panic between a SIGSTOP and its
/// SIGCONT leaves no stopped process behind.
struct Child(std::process::Child);

impl Child {
    /// Runs test `name` of this binary with `env` set to `value`.
    fn spawn(name: &str, env: &str, value: String) -> Child {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(env, value)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        Child(child)
    }

    /// Sends `signal` to the child.
    fn signal(&self, signal: libc::c_int) {
        // SAFETY: kill has no memory preconditions; the pid is our child's,
        // not yet waited for.
        assert_eq!(unsafe { libc::kill(self.0.id() as libc::pid_t, signal) }, 0);
    }

    /// Stops the child for `d`.
    fn stall(&self, d: Duration) {
        self.signal(libc::SIGSTOP);
        std::thread::sleep(d);
        self.signal(libc::SIGCONT);
    }

    /// Closes the child's stdin and waits for it to end successfully.
    fn finish(&mut self) {
        drop(self.0.stdin.take());
        assert!(self.0.wait().unwrap().success());
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Ok(None) = self.0.try_wait() {
            self.signal(libc::SIGCONT);
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// The lines a child prints, read up to the next one starting with
/// `marker`; returns the rest of that line, split at whitespace.
fn until(lines: &mut impl Iterator<Item = std::io::Result<String>>, marker: &str) -> Vec<String> {
    for line in lines {
        let line = line.unwrap();
        if let Some(rest) = line.strip_prefix(marker) {
            return rest.split_whitespace().map(str::to_owned).collect();
        }
    }
    panic!("the child ended before {marker:?}");
}

/// The ramp of the forward-skip test: long enough that the skip ends
/// inside it.
const LONG_RAMP: u32 = 48_000;

fn skip_config(domain: &Path, src: &str, out: &str) -> samplez::Config {
    let mut c = config(domain, &[(src, 1)], &[(out, 1)], diagonal(1, 1.0), MARGIN);
    c.ramp_samples = LONG_RAMP;
    c
}

/// Engine a of `input_up_fades_in_from_its_index_across_a_forward_skip`,
/// run in a process of its own so that process can be stopped. Started by
/// that test with `SAMPLEZ_SKIP_CHILD` set to the domain and the input
/// and output flow ids; without it there is nothing to run. It takes
/// lines on stdin: `entries <down> <up>` appends and commits an
/// `InputDown` and an `InputUp` at those indices, `report <end>` prints
/// its counters and the blocks it processed from `up` to `end`. It stops
/// once stdin closes.
#[test]
fn forward_skip_child() {
    let Ok(args) = std::env::var("SAMPLEZ_SKIP_CHILD") else {
        return;
    };
    let a: Vec<&str> = args.split(' ').collect();
    let (e, rx, _reports) = start_replicated(skip_config(Path::new(a[0]), a[1], a[2]));
    running(&e);
    println!("\nskip-child ready");
    let mut up = 0;
    for line in std::io::stdin().lines() {
        let line = line.unwrap();
        let w: Vec<&str> = line.split_whitespace().collect();
        match w.as_slice() {
            ["entries", down, u] => {
                up = u.parse().unwrap();
                e.append(1, down.parse().unwrap(), Entry::InputDown { slot: 0 })
                    .unwrap();
                e.append(2, up, Entry::InputUp { slot: 0 }).unwrap();
                e.commit(2);
                println!("\nskip-child appended");
            }
            ["report", end] => {
                let end: u64 = end.parse().unwrap();
                let blocks: Vec<String> = rx
                    .try_iter()
                    .filter(|v| v.k * B >= up && v.k < end)
                    .map(|v| v.k.to_string())
                    .collect();
                let c = e.counters();
                println!(
                    "\nskip-child skipped {} late {} diverged {} blocks {}",
                    c.overrun_skipped_samples.load(Relaxed),
                    c.generation_late.load(Relaxed),
                    e.status().diverged.is_some(),
                    blocks.join(" "),
                );
            }
            _ => panic!("unknown line {line}"),
        }
    }
    common::stop(e);
}

/// An engine that skips the blocks around an `InputUp`'s index fades the
/// input in from that index like one that processed them: from its first
/// block after the skip both engines' outputs are bit-identical. Engine a
/// runs in a child process, stopped for 200 ms before the index, which
/// makes it skip forward once it runs again.
#[test]
fn input_up_fades_in_from_its_index_across_a_forward_skip() {
    let domain = Domain::new();
    let (src, out_a, out_b) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut child = Child::spawn(
        "forward_skip_child",
        "SAMPLEZ_SKIP_CHILD",
        format!("{} {src} {out_a}", domain.0.display()),
    );
    let mut stdin = child.0.stdin.take().unwrap();
    let mut lines = BufReader::new(child.0.stdout.take().unwrap()).lines();
    until(&mut lines, "skip-child ready");
    let (b, rx_b, _rep_b) = start_replicated(skip_config(&domain.0, &src, &out_b));
    running(&b);
    let k0 = now(&rx_b);
    let (down, up) = ((k0 + 150) * B, (k0 + 750) * B);
    writeln!(stdin, "entries {down} {up}").unwrap();
    until(&mut lines, "skip-child appended");
    b.append(1, down, Entry::InputDown { slot: 0 }).unwrap();
    b.append(2, up, Entry::InputUp { slot: 0 }).unwrap();
    b.commit(2);
    let mut vb = Vec::new();
    wait_for(10 * SECOND, "engine b near the index", || {
        vb.extend(rx_b.try_iter());
        vb.last().is_some_and(|v| v.k + 60 >= up / B)
    });
    child.stall(Duration::from_millis(200));
    let end = up / B + 400;
    wait_past(&rx_b, end, &mut vb);
    std::thread::sleep(Duration::from_millis(100));
    writeln!(stdin, "report {end}").unwrap();
    let r = until(&mut lines, "skip-child skipped");
    assert_eq!((r[2].as_str(), r[4].as_str()), ("0", "false"), "{r:?}");
    let va: HashSet<u64> = r[6..].iter().map(|k| k.parse().unwrap()).collect();
    let first = va.iter().copied().min().unwrap();
    assert!(
        first > up / B + 20 && r[0] != "0",
        "engine a did not skip over index {up}: skipped {}, first block {first}",
        r[0]
    );
    let vb: HashSet<u64> = vb.iter().map(|v| v.k).collect();
    let x = common::read_blocks(&domain.0, &out_a, 1, first..end);
    let y = common::read_blocks(&domain.0, &out_b, 1, first..end);
    drop(stdin);
    child.finish();
    // Each engine against the fade from `up` on the blocks it computed,
    // and the two bit-identical where both did.
    let n = B as usize;
    let mut compared = 0;
    for (i, k) in (first..end).enumerate() {
        let (p, q) = (&x[i * n..(i + 1) * n], &y[i * n..(i + 1) * n]);
        let mut fade = Ramp::new(0.0);
        let _ = fade.set_target(1.0, LONG_RAMP);
        fade.advance((k * B - up) as usize);
        let mut want: Vec<f32> = (0..B).map(|j| common::pilot(0, k * B + j)).collect();
        fade.process(&mut want);
        let (in_a, in_b) = (va.contains(&k), vb.contains(&k));
        assert!(!in_a || p == want, "engine a fades in from {up}: block {k}");
        assert!(!in_b || q == want, "engine b fades in from {up}: block {k}");
        assert!(!(in_a && in_b) || same(p, q), "block {k}");
        compared += u64::from(in_a && in_b);
    }
    assert!(
        compared * 4 > (end - first) * 3,
        "{compared} blocks of {} compared",
        end - first
    );
    assert_eq!(b.status().diverged, None);
    assert_eq!(b.counters().generation_late.load(Relaxed), 0);
    common::stop(b);
}

/// A pilot producer in a process of its own, so that process can be
/// stopped: started by `a_re_route_crossfades_from_its_index_after_a_silent_block`
/// with `SAMPLEZ_PILOT_CHILD` set to the domain and the flow id; without
/// it there is nothing to run. It stops once stdin closes.
#[test]
fn pilot_child() {
    let Ok(args) = std::env::var("SAMPLEZ_PILOT_CHILD") else {
        return;
    };
    let a: Vec<&str> = args.split(' ').collect();
    let _pilot = Pilot::start(Path::new(a[0]), a[1], 1, B, 0);
    println!("\npilot-child ready");
    for line in std::io::stdin().lines() {
        line.unwrap();
    }
}

/// A re-route on time crossfades from its index also on an engine whose
/// input was silent in the block before it: engine a reads the old flow
/// with a margin its producer's stall exceeds around the index, engine b
/// with one it does not. From the old flow's return on, both engines
/// compute the same crossfade, bit for bit.
#[test]
fn a_re_route_crossfades_from_its_index_after_a_silent_block() {
    let domain = Domain::new();
    let (old, new, out_a, out_b) = (new_id(), new_id(), new_id(), new_id());
    let mut child = Child::spawn(
        "pilot_child",
        "SAMPLEZ_PILOT_CHILD",
        format!("{} {old}", domain.0.display()),
    );
    let mut lines = BufReader::new(child.0.stdout.take().unwrap()).lines();
    until(&mut lines, "pilot-child ready");
    let _new = Pilot::start(&domain.0, &new, 1, B, 0);
    // A one-second crossfade, and no prune while the old flow stalls.
    let cfg = |out: &str, margin: u64| {
        let mut c = config(
            &domain.0,
            &[(&old, 1)],
            &[(out, 1)],
            diagonal(1, 1.0),
            margin,
        );
        c.ramp_samples = LONG_RAMP;
        c.prune_after_blocks = 1000;
        c
    };
    let (a, rx_a, _rep_a) = start_replicated(cfg(&out_a, MARGIN));
    let (b, rx_b, _rep_b) = start_replicated(cfg(&out_b, 10 * MARGIN));
    running(&a);
    running(&b);
    let api = common::api();
    let clock = || samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    let at = (clock() / B + 500) * B;
    for e in [&a, &b] {
        let flow_id = new.clone();
        e.append(1, at, Entry::Route { slot: 0, flow_id }).unwrap();
        e.commit(1);
    }
    // The old flow stalls 80 ms from 30 ms before the index: engine a,
    // 20 ms behind the clock, finds blocks from 30 ms before the index to
    // 30 ms after it late; engine b, 200 ms behind, none.
    wait_for(5 * SECOND, "30 ms before the index", || {
        clock() + 1440 >= at
    });
    child.stall(Duration::from_millis(80));
    // Within the crossfade and the flows' history.
    let end = at / B + 400;
    let (mut va, mut vb) = (Vec::new(), Vec::new());
    wait_past(&rx_a, end, &mut va);
    wait_past(&rx_b, end, &mut vb);
    let verdict = |v: &[BlockVerdict], k| v.iter().find(|v| v.k == k).copied();
    let k = at / B - 1;
    assert_eq!(
        (
            verdict(&va, k).map(|v| v.silenced),
            verdict(&vb, k).map(|v| v.silenced)
        ),
        (Some(1), Some(0)),
        "engine a silent and engine b not in the block before the index"
    );
    let range = at / B + 100..end;
    let x = common::read_blocks(&domain.0, &out_a, 1, range.clone());
    let y = common::read_blocks(&domain.0, &out_b, 1, range.clone());
    let n = B as usize;
    let mut compared = 0;
    for (i, k) in range.clone().enumerate() {
        if both(&va, &vb, k) {
            compared += 1;
            let (p, q) = (&x[i * n..(i + 1) * n], &y[i * n..(i + 1) * n]);
            assert!(same(p, q), "block {k}, {} after the index", k - at / B);
        }
    }
    assert!(
        compared * 4 > range.count() * 3,
        "{compared} blocks compared"
    );
    child.finish();
    for e in [&a, &b] {
        assert_eq!(e.counters().generation_late.load(Relaxed), 0);
        assert_eq!(e.status().inputs[0].flow_id, new);
    }
    common::stop(b);
    common::stop(a);
}

/// A log entry re-routing two inputs of one domain gets one generation
/// covering both, swapped in at its index: both cross-fade from their
/// flows to the new ones, which nobody writes, from that index on, sample
/// for sample, and neither waits for a generation still to come.
#[test]
fn a_log_entry_re_routing_two_inputs_builds_one_generation() {
    let domain = Domain::new();
    let (src0, src1, next0, next1, out) = (new_id(), new_id(), new_id(), new_id(), new_id());
    let _p0 = Pilot::start(&domain.0, &src0, 1, B, 0);
    let _p1 = Pilot::start(&domain.0, &src1, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src0, 1), (&src1, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    let r = (now(&rx) + 150) * B;
    let routes = [
        Entry::Route {
            slot: 0,
            flow_id: next0.clone(),
        },
        Entry::Route {
            slot: 1,
            flow_id: next1.clone(),
        },
    ];
    e.append_batch(1, r, &routes).unwrap();
    e.commit(1);
    let mut seen = Vec::new();
    let d = r + u64::from(RAMP);
    wait_past(&rx, d / B + 20, &mut seen);
    let range = r / B - 20..d / B + 20;
    let x = common::read_blocks(&domain.0, &out, 2, range.clone());
    for (i, k) in range.enumerate() {
        let want = if k * B < r {
            ramped(0, k, 1.0, 1.0, 0)
        } else {
            ramped(0, k, 1.0, 0.0, r)
        };
        for c in 0..2 {
            assert!(block(&x, 2, i, c) == want, "block {k} of input {c}");
        }
    }
    let s = e.status();
    assert_eq!(
        (&*s.inputs[0].flow_id, &*s.inputs[1].flow_id),
        (&*next0, &*next1)
    );
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// One log entry's `InputUp` and `Route` of a slot raise it on the new
/// flow, and its `Route` and `InputDown` leave it down on the new flow:
/// the route is kept, whatever the order.
#[test]
fn a_slot_routed_in_one_entry_keeps_its_route_and_its_state() {
    let domain = Domain::new();
    let (src0, src1, next, out) = (new_id(), new_id(), new_id(), new_id());
    let _pilots: Vec<Pilot> = [&src0, &src1, &next]
        .iter()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0))
        .collect();
    let cfg = config(
        &domain.0,
        &[(&src0, 1), (&src1, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    let (e, rx, _reports) = start_replicated(cfg);
    running(&e);
    let mut seen = Vec::new();
    let mut at = |e: &Engine, id, entries: &[Entry]| {
        let at = (now(&rx) + 100) * B;
        e.append_batch(id, at, entries).unwrap();
        e.commit(id);
        wait_past(&rx, (at + u64::from(RAMP)) / B + 20, &mut seen);
    };
    let route = |flow_id: &str| Entry::Route {
        slot: 1,
        flow_id: flow_id.to_owned(),
    };
    at(&e, 1, &[Entry::InputDown { slot: 1 }]);
    assert!(!e.status().inputs[1].up);
    at(&e, 2, &[Entry::InputUp { slot: 1 }, route(&next)]);
    let s = e.status();
    assert!(s.inputs[1].up, "raised");
    assert_eq!(s.inputs[1].flow_id, next);
    at(&e, 3, &[route(&src1), Entry::InputDown { slot: 1 }]);
    let s = e.status();
    assert!(!s.inputs[1].up, "down");
    assert_eq!(s.inputs[1].flow_id, src1, "the route was lost");
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// A log entry raising or re-routing inputs of two domains is refused
/// before anything is appended: one generation is built for a log entry,
/// and a generation is of one domain. Every slot an `InputUp` or `Route`
/// names counts, also one a later entry cancels, so every subset of an
/// admitted entry is admitted.
#[test]
fn a_log_entry_across_input_domains_is_refused() {
    let (a, b) = (Domain::new(), Domain::new());
    let (src0, src1, out) = (new_id(), new_id(), new_id());
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": MARGIN,
        "libmxl": common::libmxl(),
        "domains": [{ "name": "a", "path": a.0 }, { "name": "b", "path": b.0 }],
        "inputs": [
            { "name": "in0", "domain": "a", "flowId": src0, "channels": 1 },
            { "name": "in1", "domain": "b", "flowId": src1, "channels": 1 },
        ],
        "outputs": [{ "name": "out0", "domain": "a", "flowId": out, "label": "two domains",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0], [1.0]],
        "rt": { "rtCpu": "none" },
    });
    let (e, _rx, _reports) = start_replicated(samplez::Config::parse(&v.to_string()).unwrap());
    let up = |slot| Entry::InputUp { slot };
    let refused = e.admits(&[up(0), up(1)]);
    assert!(
        matches!(&refused, Err(samplez::EntryError::Invalid(m)) if m.contains("domain")),
        "{refused:?}"
    );
    assert_eq!(e.append_batch(1, 48_000 * B, &[up(0), up(1)]), refused);
    assert_eq!(e.admits(&[up(0), Entry::InputDown { slot: 1 }]), Ok(()));
    // Raising input 0 and dropping it again leaves one domain re-routed,
    // but a restamp keeping only some of the commands may keep the raise:
    // the entries are refused whole, so no subset of an admitted log
    // entry is refused after its commit.
    let route = Entry::Route {
        slot: 1,
        flow_id: new_id(),
    };
    let down = Entry::InputDown { slot: 0 };
    assert!(e.admits(&[up(0), route.clone(), down.clone()]).is_err());
    let admitted = [up(0), down, Entry::InputDown { slot: 1 }, up(0)];
    for subset in 0..1u32 << admitted.len() {
        let entries: Vec<Entry> = (0..admitted.len())
            .filter(|n| subset & 1 << n != 0)
            .map(|n| admitted[n].clone())
            .collect();
        assert_eq!(e.admits(&entries), Ok(()), "{entries:?}");
    }
    e.stop();
}

/// A producer that commits each block after the RT thread began waiting
/// for it, one block before the block's deadline, but before the
/// deadline, is read within half a block period of its commit rather
/// than at the deadline: the head wait sleeps half a period at most.
#[test]
fn a_block_committed_while_the_rt_thread_waits_is_read_within_half_a_period() {
    const BLOCK: u64 = 1024;
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    // Committed 1.1 blocks after its end; the wait begins one block after
    // the end, the deadline is two blocks after it (the margin).
    let delay = BLOCK + BLOCK / 10;
    let _pilot = Pilot::start(&domain.0, &src, 1, BLOCK, delay);
    let mut c = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        2 * BLOCK,
    );
    c.block_size = BLOCK as u32;
    let (e, _rx) = common::start(c);
    running(&e);
    let api = common::api();
    let now = || samplez_mxl::time::now(&api);
    let period = BLOCK * 1_000_000_000 / 48_000;
    // Lag of each block's processing behind its commit, taken where one
    // block was processed since the last look, so the time seen is its.
    let mut lags = Vec::new();
    let mut last = common::processed(&e);
    let deadline = Instant::now() + 10 * SECOND;
    while lags.len() < 50 {
        assert!(Instant::now() < deadline, "{} blocks seen", lags.len());
        let p = common::processed(&e);
        if p == last + 1 {
            // Block p - 1, committed once the clock passed its end plus
            // the delay.
            let committed = samplez_mxl::time::index_to_timestamp(&api, p * BLOCK + delay);
            lags.push(now().saturating_sub(committed));
        }
        last = p;
        std::thread::sleep(Duration::from_micros(100));
    }
    lags.sort_unstable();
    let median = lags[lags.len() / 2];
    // Half a period, and slack for the producer's and the RT thread's
    // wake-ups; read at the deadline instead it is 0.9 of a period.
    assert!(
        median <= period / 2 + 3_000_000,
        "median lag {median} ns over a period of {period} ns: {lags:?}"
    );
    assert_eq!(e.counters().input_wait_timeout.load(Relaxed), 0);
    common::stop(e);
}
