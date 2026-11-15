//! The RT thread's loop under RealtimeSanitizer: built with
//! `-Zsanitizer=realtime --cfg samplez_rtsan` (CI's RTSan step), any call
//! RTSan counts as blocking (allocation, pthread lock, sleep, blocking
//! syscall) inside it aborts the process. The run covers input generation
//! swaps, a re-route crossfade, a gain ramp, output commits, metering and
//! a loudness reset, and a writer's refusals; the lib test
//! `the_failure_paths_run_on_the_rt_thread`, run under the same flags,
//! covers catch-up, clock steps and a late input. libmxl's sync-group
//! wait, `mxlSleepUntil`, the
//! commit's `FUTEX_WAKE` and the failure path's probe are allowed by call
//! site. Other builds compile this file to nothing.

#![cfg(samplez_rtsan)]
#![feature(sanitize)]

mod common;

use std::process::Command;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, start, wait_for};
use samplez::InputState;
use samplez_mxl::Instance;
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
    let (engine, rx) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        matrix,
        MARGIN,
    ));
    let attached = || engine.status().inputs[0].state == InputState::Attached;
    wait_for(Duration::from_secs(5), "input attached", attached);
    wait_for(Duration::from_secs(5), "output realised", || {
        engine.status().outputs[0].realised
    });
    let api = common::api();
    let index = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    engine.set_gain(0, 0, 0.25, (index / B + 20) * B).unwrap();
    let reset = (index / B + 40) * B;
    engine.reset_loudness(0, reset).unwrap();
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
    assert!(verdicts.iter().filter(|v| v.written != 0).count() > 1_000);
    assert_eq!(engine.status().diverged, None);
    assert_eq!(engine.status().rt_panic, None);
    common::stop(engine);
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
