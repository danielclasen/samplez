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

const CHANNELS: u32 = 4;
/// Minor faults the RT thread may take in the second after a re-add.
const MAX_FAULTS: u64 = 2;

/// Minor page faults of thread `tid`, field 10 of its stat file.
fn minor_faults(tid: i32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")).unwrap();
    let fields = stat.rsplit_once(") ").unwrap().1;
    fields.split(' ').nth(7).unwrap().parse().unwrap()
}

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
    let pilot = Pilot::start(&domain.0, &src, CHANNELS, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, CHANNELS)],
        &[(&dst, 1)],
        vec![vec![1.0]; CHANNELS as usize],
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

    // Degraded memory locking: a re-added input's rings are mapped anew
    // in a new generation, after mlockall(MCL_CURRENT). The supervisor
    // reads their pages before handover, so the RT thread's reads over the
    // following second take no page faults; without that it takes one
    // per page read, about 47 per channel and second.
    std::thread::sleep(Duration::from_secs(3));
    let attached = || engine.status().inputs[0].state == InputState::Attached;
    drop(pilot);
    wait_for(Duration::from_secs(5), "input pruned", || !attached());
    let _pilot = Pilot::start(&domain.0, &src, CHANNELS, B, 0);
    wait_for(Duration::from_secs(10), "input re-added", attached);
    let tid = s.thread.tid;
    let before = minor_faults(tid);
    std::thread::sleep(Duration::from_secs(1));
    let faults = minor_faults(tid) - before;
    eprintln!("RT thread minor faults in the second after the re-add: {faults}");
    assert!(
        faults <= MAX_FAULTS,
        "{faults} minor faults on the RT thread"
    );
    engine.stop();
}
