//! Without privileges every RT set-up step that needs one reports a
//! named degradation, and the engine runs anyway. The builder container
//! runs the tests as a non-root uid without capabilities, RT priority or
//! an unlimited memlock limit; the test asserts that first.

mod common;

use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, wait_for};
use samplez::rt::{self, Step};
use samplez::{Engine, InputState};

fn rlimit(resource: libc::__rlimit_resource_t) -> u64 {
    let mut r = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `r` is writable.
    assert_eq!(unsafe { libc::getrlimit(resource, &mut r) }, 0);
    r.rlim_cur
}

#[test]
fn rt_setup_degrades_without_privileges_and_the_engine_runs() {
    // SAFETY: getuid has no preconditions.
    assert_ne!(unsafe { libc::getuid() }, 0, "needs a non-root uid");
    assert_eq!(
        rt::permitted().unwrap(),
        0,
        "needs no permitted capabilities"
    );
    assert_eq!(rlimit(libc::RLIMIT_RTPRIO), 0, "needs RLIMIT_RTPRIO 0");
    assert_ne!(
        rlimit(libc::RLIMIT_MEMLOCK),
        libc::RLIM_INFINITY,
        "needs a finite memlock limit"
    );

    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&dst, 1)],
        vec![vec![1.0]],
        MARGIN,
    );
    cfg.rt.rt_cpu = samplez::config::RtCpu::Auto;
    let process = rt::setup_process(&cfg.rt);
    let engine = Engine::start(cfg, process, None).unwrap();
    let s = engine.status().rt;

    assert_eq!(
        s.process.capabilities,
        Step::Degraded("no capabilities permitted".into())
    );
    assert!(s.process.mlock.is_degraded(), "{}", s.process.mlock);
    // With a finite limit MCL_FUTURE is never requested.
    let mlock = s.process.mlock.to_string();
    assert!(
        mlock.contains("MCL_CURRENT") && !mlock.contains("MCL_FUTURE"),
        "{mlock}"
    );
    assert!(
        matches!(s.process.stack, Step::Active(_)),
        "{}",
        s.process.stack
    );
    assert!(s.thread.sched.is_degraded(), "{}", s.thread.sched);
    assert!(
        s.thread.sched.to_string().contains("SCHED_FIFO"),
        "{}",
        s.thread.sched
    );
    // auto without the downward-API values does not pin.
    assert!(matches!(&s.thread.affinity, Step::Off(m) if m.contains(rt::CPU_REQUEST_ENV)));
    assert!(
        matches!(s.thread.ftz_daz, Step::Active(_)),
        "{}",
        s.thread.ftz_daz
    );
    assert!(
        matches!(s.thread.signals, Step::Active(_)),
        "{}",
        s.thread.signals
    );
    assert!(s.degraded());

    wait_for(Duration::from_secs(5), "input attached", || {
        engine.status().inputs[0].state == InputState::Attached
    });
    wait_for(Duration::from_secs(5), "output realised", || {
        engine.status().outputs[0].realised
    });
    let beat = engine.counters().heartbeat_blocks.load(Relaxed);
    std::thread::sleep(Duration::from_millis(500));
    assert!(engine.counters().heartbeat_blocks.load(Relaxed) > beat + 300);
    engine.stop();
}
