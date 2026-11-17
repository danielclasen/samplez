//! Log entries on real libmxl flows: `InputDown` and `InputUp` at their
//! index on two engines, also across a forward skip, re-route crossfades,
//! entries applied again, the one pending generation an engine builds at a
//! time, re-routes on an engine with its own log and reports sent again.

mod common;

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;

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

/// A re-route to a flow that is not there crossfades the old flow to
/// silence from the entry's index; the input is reported pruned, and once
/// the flow appears it is reported readable and an `InputUp` fades it in.
/// The other input, read through every new generation, stays exact.
#[test]
fn re_route_crossfades_at_its_index_and_leaves_other_inputs_exact() {
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
    let range = r / B - 20..d / B + 20;
    let x = common::read_blocks(&domain.0, &out, 2, range.clone());
    for (i, k) in range.enumerate() {
        assert!(block(&x, 2, i, 0) == ramped(0, k, 1.0, 1.0, 0), "block {k}");
        let want = if k * B < r {
            ramped(0, k, 1.0, 1.0, 0)
        } else {
            ramped(0, k, 1.0, 0.0, r)
        };
        assert!(
            block(&x, 2, i, 1) == want,
            "block {k} of the re-routed input"
        );
    }
    report(&reports, Report::InputPruned { slot: 1 });
    e.append(2, (now(&rx) + 20) * B, Entry::InputDown { slot: 1 })
        .unwrap();
    e.commit(2);
    let _p2 = Pilot::start(&domain.0, &next, 1, B, 0);
    report(&reports, Report::InputReadable { slot: 1 });
    let lead = e.status().generation.build_lead;
    let u = (now(&rx) * B + lead).next_multiple_of(B);
    e.append(3, u, Entry::InputUp { slot: 1 }).unwrap();
    e.commit(3);
    let end = u / B + 2 * u64::from(RAMP) / B;
    wait_past(&rx, end, &mut seen);

    let range = u / B - 20..end;
    let x = common::read_blocks(&domain.0, &out, 2, range.clone());
    for (i, k) in range.enumerate() {
        assert!(block(&x, 2, i, 0) == ramped(0, k, 1.0, 1.0, 0), "block {k}");
        let want = if k * B < u {
            vec![0.0; B as usize]
        } else {
            ramped(0, k, 0.0, 1.0, u)
        };
        assert!(block(&x, 2, i, 1) == want, "block {k} after InputUp");
    }
    for v in seen.iter().filter(|v| v.k > k0 + 50) {
        assert_eq!(v.silenced & 1, 0, "{v:?}");
    }
    let s = e.status();
    assert_eq!(s.inputs[1].flow_id, next);
    assert_eq!(s.inputs[0].counters.below_last_read, 0);
    assert_eq!(s.diverged, None);
    common::stop(e);
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
    wait_past(&rx, end + 100, &mut seen);
    for v in seen.iter().filter(|v| v.k > end) {
        assert_eq!(v.silenced, 0, "{v:?}");
    }
    let s = e.status();
    let flows: Vec<&str> = s.inputs.iter().map(|i| i.flow_id.as_str()).collect();
    assert_eq!(flows, next.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// Generation-needing entries at one index, as a replica that lagged the
/// leader by more than buildLead gets them: the first swaps in on time,
/// the others are applied before their generations and each is swapped
/// in once it arrives. No input stays silent.
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
/// and a generation is of one domain.
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
    e.stop();
}
