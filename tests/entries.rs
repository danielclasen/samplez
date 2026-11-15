//! Log entries on real libmxl flows: `InputDown` and `InputUp` at their
//! index on two engines, re-route crossfades, entries applied again, and
//! the one pending generation an engine builds at a time.

mod common;

use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Receiver;
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
    let compare = |range: std::ops::Range<u64>, want: &dyn Fn(usize, u64) -> Vec<f32>| {
        let x = common::read_blocks(&domain.0, &out_a, 2, range.clone());
        let y = common::read_blocks(&domain.0, &out_b, 2, range.clone());
        assert!(x.iter().zip(&y).all(|(p, q)| p.to_bits() == q.to_bits()));
        for (i, k) in range.enumerate() {
            for c in 0..2 {
                assert!(block(&x, 2, i, c) == want(c, k), "block {k} channel {c}");
            }
        }
    };
    compare(down / B - 20..down / B + 50, &|c, k| {
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
    compare(up / B - 20..end, &|c, k| {
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
    let y = common::read_blocks(&domain.0, &out_b, 2, range);
    assert!(x.iter().zip(&y).all(|(p, q)| p.to_bits() == q.to_bits()));
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
