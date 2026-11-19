//! The RT thread's loop under RealtimeSanitizer: built with
//! `-Zsanitizer=realtime --cfg samplez_rtsan` (CI's RTSan step), any call
//! RTSan counts as blocking (allocation, pthread lock, sleep, blocking
//! syscall) inside it aborts the process. The run covers input generation
//! swaps, a re-route crossfade, detach and route cycles, output disable
//! and enable cycles, a gain ramp,
//! output commits, output, meter and input metering and loudness resets,
//! strips with every unit on, a writer's refusals, a read of selected
//! channels of a wide flow, and, for a replicated engine, every path of
//! the hold ring
//! (`common::hold_scenario`): holding, withholding and overflow, with its
//! pages hashed and strip entries applied. The lib test
//! `the_failure_paths_run_on_the_rt_thread`, run under the same flags,
//! covers catch-up with strips advanced over the skip, clock steps and a
//! late input. `mxlSleepUntil` and the commit's `FUTEX_WAKE` are allowed
//! by call site. Other builds compile this file
//! to nothing.

#![cfg(samplez_rtsan)]
#![feature(sanitize)]

mod common;

use std::process::Command;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, start, wait_for};
use samplez::InputState;
use samplez_mxl::Instance;
use samplez_mxl::input::{Generation, GroupWait, Input, InputConfig, Outcome, SlotState, Timing};
use samplez_mxl::output::{self, Creation, FlowWriter, OutputConfig, Write};

const PROBE: &str = "SAMPLEZ_RTSAN_PROBE";

/// Allocates in a real-time function; RTSan aborts the process.
#[sanitize(realtime = "nonblocking")]
fn allocate_in_rt() -> usize {
    std::hint::black_box(vec![1u8; 64]).len()
}

/// RTSan is live in this binary: an allocation in a real-time function
/// aborts a child running it.
#[test]
fn rtsan_aborts_an_allocation_in_a_real_time_function() {
    if std::env::var_os(PROBE).is_some() {
        allocate_in_rt();
        return;
    }
    let out = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "rtsan_aborts_an_allocation_in_a_real_time_function",
        ])
        .env(PROBE, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "the probe was not stopped:\n{stderr}"
    );
    assert!(
        stderr.contains("RealtimeSanitizer") && stderr.contains("malloc"),
        "{stderr}"
    );
}

#[test]
fn the_rt_thread_passes_rtsan() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let matrix = vec![vec![0.5, 0.0], vec![0.0, 0.5]];
    let mut cfg = config(&domain.0, &[(&src, 2)], &[(&dst, 2)], matrix, MARGIN);
    common::add_meter(&mut cfg, "bus", 2, 0.5);
    cfg.outputs[0].limiter = true;
    cfg.inputs[0].max_delay_samples = 4800;
    let (engine, rx) = start(cfg);
    let attached = || engine.status().inputs[0].state == InputState::Attached;
    wait_for(Duration::from_secs(5), "input attached", attached);
    wait_for(Duration::from_secs(5), "output realised", || {
        engine.status().outputs[0].realised
    });
    let api = common::api();
    let index = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    // A delayed fade: held for three blocks, then ramped over 4800 samples,
    // and a later change that replaces a fade still waiting.
    let fade = samplez::Fade {
        delay: 3 * B as u32,
        len: 4800,
    };
    engine
        .set_gain_fade(0, 0, 0.25, Some(fade), (index / B + 20) * B)
        .unwrap();
    engine
        .set_gain_fade(0, 0, 0.125, Some(fade), (index / B + 21) * B)
        .unwrap();
    engine
        .set_gain_fade(0, 0, 0.25, Some(fade), (index / B + 40) * B)
        .unwrap();
    // The limiter on, below the output's level: it limits.
    let limiter = samplez_dsp::LimiterParams {
        enabled: true,
        ceiling_db: -12.0,
        release_ms: 50.0,
    };
    engine
        .set_limiter(0, limiter, (index / B + 20) * B)
        .unwrap();
    engine.set_delay(0, 2400, (index / B + 20) * B).unwrap();
    engine.set_delay(0, 1200, (index / B + 21) * B).unwrap();
    let reset = (index / B + 40) * B;
    engine.reset_loudness(0, reset).unwrap();
    // The meter after the output.
    engine.reset_loudness(1, reset).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    // A re-route: a generation holding both flows, then one without the
    // old flow once the crossfade ended.
    let next = new_id();
    let _next = Pilot::start(&domain.0, &next, 2, B, 0);
    let at = engine.route(0, &next, 0).unwrap();
    wait_for(Duration::from_secs(5), "re-routed", || {
        engine.status().inputs[0].flow_id == next
    });
    std::thread::sleep(Duration::from_millis(1500));

    let verdicts: Vec<_> = rx.try_iter().collect();
    assert!(
        verdicts.iter().any(|v| v.fading != 0 && v.k * B >= at),
        "no crossfade seen"
    );
    assert!(verdicts.iter().any(|v| v.ramping), "no gain ramp seen");
    let s = engine.status();
    assert!(
        s.meters[0].meters.window.is_some() && s.inputs[0].meters.window.is_some(),
        "the meter and the input were not metered"
    );
    assert!(verdicts.iter().filter(|v| v.written != 0).count() > 1_000);
    assert_eq!(engine.status().diverged, None);
    assert_eq!(engine.status().rt_panic, None);
    common::stop(engine);
}

/// Cycles of an output's state under RealtimeSanitizer, each a disable,
/// its writer handed back, then an enable and the new writer taken in.
#[test]
fn the_rt_thread_passes_rtsan_through_output_disable_and_enable() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (engine, _rx) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        common::diagonal(2, 1.0),
        MARGIN,
    ));
    let output = || engine.status().outputs[0].clone();
    wait_for(Duration::from_secs(5), "output realised", || {
        output().realised
    });
    for _ in 0..3 {
        engine.set_output_enabled(0, false, 0).unwrap();
        wait_for(Duration::from_secs(5), "output released", || {
            let o = output();
            !o.enabled && !o.realised && !domain.has_flow(&dst)
        });
        std::thread::sleep(Duration::from_millis(300));
        engine.set_output_enabled(0, true, 0).unwrap();
        wait_for(Duration::from_secs(5), "output realised again", || {
            let o = output();
            o.enabled && o.realised
        });
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(engine.status().diverged, None);
    assert_eq!(engine.status().rt_panic, None);
    common::stop(engine);
}

/// Cycles of an input's state under RealtimeSanitizer, each a detach,
/// its crossfade to silence and the generation without the input, then a
/// route back, its generation and the fade-in from silence.
#[test]
fn the_rt_thread_passes_rtsan_through_detach_and_route() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (engine, rx) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        common::diagonal(2, 1.0),
        MARGIN,
    ));
    let state = || engine.status().inputs[0].state;
    wait_for(Duration::from_secs(5), "input attached", || {
        state() == InputState::Attached
    });
    let mut cycles = Vec::new();
    for _ in 0..3 {
        let detached = engine.detach(0, 0).unwrap();
        wait_for(Duration::from_secs(5), "input detached", || {
            state() == InputState::Detached
        });
        std::thread::sleep(Duration::from_millis(300));
        let routed = engine.route(0, &src, 0).unwrap();
        wait_for(Duration::from_secs(5), "input attached again", || {
            state() == InputState::Attached
        });
        std::thread::sleep(Duration::from_millis(300));
        cycles.push((detached, routed));
    }

    let verdicts: Vec<_> = rx.try_iter().collect();
    let fading = |at: u64| verdicts.iter().any(|v| v.fading != 0 && v.k * B >= at);
    for &(detached, routed) in &cycles {
        assert!(fading(detached) && fading(routed), "no crossfade seen");
    }
    assert_eq!(engine.status().diverged, None);
    assert_eq!(engine.status().rt_panic, None);
    common::stop(engine);
}

/// The strips under RealtimeSanitizer: entries switching every unit of
/// a stereo strip on, applied and ramping, then the strip running
/// settled with every unit on.
#[test]
fn the_rt_thread_passes_rtsan_with_strips() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (engine, rx) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        common::diagonal(2, 1.0),
        MARGIN,
    ));
    wait_for(Duration::from_secs(5), "input attached", || {
        engine.status().inputs[0].state == InputState::Attached
    });
    let api = common::api();
    let index = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    let at = (index / B + 20) * B;
    for param in common::strip_params(2) {
        engine.set_strip(0, param, at).unwrap();
    }
    std::thread::sleep(Duration::from_millis(1500));
    let verdicts: Vec<_> = rx.try_iter().collect();
    let applied = verdicts.last().map_or(0, |v| v.commands_applied);
    assert_eq!(applied, common::strip_params(2).len() as u64);
    assert!(verdicts.iter().filter(|v| v.k * B > at).count() > 1_000);
    assert_eq!(engine.status().diverged, None);
    assert_eq!(engine.status().rt_panic, None);
    common::stop(engine);
}

/// The duck groups under RealtimeSanitizer: a group keyed by a stereo
/// input, one forced on, masks and depths set and changed while ducking,
/// a release, and the readings of every slice sent.
#[test]
fn the_rt_thread_passes_rtsan_with_ducks() {
    use samplez::DuckSetting;
    use samplez_dsp::DuckMode;
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        common::diagonal(2, 1.0),
        MARGIN,
    );
    cfg.duck_columns = vec![0, 1];
    let (engine, rx) = start(cfg);
    wait_for(Duration::from_secs(5), "input attached", || {
        engine.status().inputs[0].state == InputState::Attached
    });
    let api = common::api();
    let index = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    let at = (index / B + 20) * B;
    let set = |group, setting, at| engine.set_duck(group, setting, at).unwrap();
    set(0, DuckSetting::Keys(vec![(0, 1.0)]), at);
    set(0, DuckSetting::Depth { slot: 0, db: 12.0 }, at);
    set(4, DuckSetting::Mode(DuckMode::On), at);
    set(4, DuckSetting::Depth { slot: 0, db: 20.0 }, at);
    engine.set_duck_mask(0, 0, 1, at).unwrap();
    engine.set_duck_mask(1, 1, 1 << 4, at).unwrap();
    let later = at + 400 * B;
    engine.set_duck_mask(0, 0, 1 | 1 << 4, later).unwrap();
    set(4, DuckSetting::Depth { slot: 0, db: 6.0 }, later);
    set(4, DuckSetting::Mode(DuckMode::Off), later + 200 * B);
    std::thread::sleep(Duration::from_millis(1500));
    let verdicts: Vec<_> = rx.try_iter().collect();
    assert!(verdicts.iter().filter(|v| v.k * B > later).count() > 500);
    let s = engine.status();
    assert!(
        s.ducks[0].engaged && s.ducks[4].slice.is_some(),
        "{:?}",
        s.ducks
    );
    assert_eq!(s.diverged, None);
    assert_eq!(s.rt_panic, None);
    common::stop(engine);
}

/// Ducks through a resync under RealtimeSanitizer: a key unkeyed and
/// keyed again while its group ducks, the core with its detectors
/// captured and a replayed one installed.
#[test]
fn the_rt_thread_passes_rtsan_with_ducks_through_a_resync() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let extra = serde_json::json!({ "duckColumns": [0] });
    let (engine, _hashes) = common::start_held_with(&domain.0, &src, &dst, 2, extra);
    wait_for(Duration::from_secs(5), "input attached", || {
        engine.status().inputs[0].state == InputState::Attached
    });
    common::duck_resync_scenario(&engine);
    assert_eq!(engine.status().rt_panic, None);
    engine.stop();
}

/// Writes block `k`, then block `k - 1`, below the last commit, then block
/// `k + 1` from too little data, in a real-time function.
#[sanitize(realtime = "nonblocking")]
fn refusals(w: &mut FlowWriter, k: u64, data: &[f32]) -> [Write; 3] {
    [
        w.write_block(k, data),
        w.write_block(k - 1, data),
        w.write_block(k + 1, &data[1..]),
    ]
}

/// A writer's refusals, a block below its last commit (held) and too
/// little data (failed), run in the real-time context.
#[test]
fn a_writer_refuses_in_the_real_time_context() {
    let domain = Domain::new();
    let id = new_id();
    let api = common::api();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        flow_id: &id,
        label: "rtsan refusals",
        group: "samplez-test",
        role: "Audio",
        channels: 2,
        block: B,
        batch: B,
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow not created");
    };
    let data = vec![0.25f32; 2 * B as usize];
    let [committed, held, failed] = refusals(&mut w, 1_000_000, &data);
    assert_eq!(committed, Write::Committed);
    assert_eq!(held, Write::Held);
    assert!(matches!(failed, Write::Failed(_)), "{failed:?}");
}

#[test]
fn the_rt_thread_passes_rtsan_holding_blocks() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (engine, hashes) = common::start_held(&domain.0, &src, &dst, 2);
    common::hold_scenario(&engine);
    let c = engine.counters();
    assert!(c.hold_overflows.load(std::sync::atomic::Ordering::Relaxed) > 0);
    // Pages were hashed, entries fingerprinted.
    let hashes: Vec<_> = hashes.try_iter().collect();
    assert!(
        hashes
            .iter()
            .any(|h| h.fingerprint != samplez::blocks::HEALTHY),
        "no page with an entry hashed of {}",
        hashes.len()
    );
    assert_eq!(engine.status().rt_panic, None);
    engine.stop();
}

/// Reads block `k` of `input` in a real-time function.
#[sanitize(realtime = "nonblocking")]
fn read_in_rt(input: &Input, state: &mut SlotState, k: u64, dst: &mut [f32]) -> Outcome {
    input.read(state, k, &GroupWait::Ok, dst)
}

/// Two legs read out of a 16-channel flow, run in the real-time context.
#[test]
fn a_selected_read_runs_in_the_real_time_context() {
    const K: u64 = 1_000_000;
    let domain = Domain::new();
    let id = new_id();
    let api = common::api();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        flow_id: &id,
        label: "rtsan selection",
        group: "samplez-test",
        role: "Audio",
        channels: 16,
        block: B,
        batch: B,
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow not created");
    };
    let data: Vec<f32> = (0..16 * B as usize)
        .map(|i| (i / B as usize) as f32)
        .collect();
    assert_eq!(w.write_block(K, &data), Write::Committed);
    let mut g = Generation::new(&api, &domain.0, 1).unwrap();
    let input = InputConfig {
        flow_id: &id,
        channels: 2,
        flow_channels: Some(&[9, 4]),
        offset: 0,
    };
    let timing = Timing {
        block: B,
        margin: 0,
        max_catchup_blocks: 0,
        resync_budget: 0,
    };
    g.attach(0, &input, &timing).unwrap();
    let mut state = SlotState::default();
    let mut dst = vec![0.0f32; 2 * B as usize];
    assert_eq!(
        read_in_rt(g.input(0).unwrap(), &mut state, K, &mut dst),
        Outcome::Copy
    );
    assert!(dst[..B as usize].iter().all(|&v| v == 9.0));
    assert!(dst[B as usize..].iter().all(|&v| v == 4.0));
}
