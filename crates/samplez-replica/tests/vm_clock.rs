//! Kernel clock changes against a real replicated engine, as the clock
//! verification reads them: an `adjtimex` step of the TAI offset, and a
//! leap second the kernel inserts at UTC midnight. Both step the clock of
//! the machine they run on, so they run only inside a disposable VM, with
//! `SAMPLEZ_VM_CLOCK=1` (the `vm-clock` CI job); otherwise they return at
//! once.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use samplez::{Config, Engine, Replication};
use samplez_replica::bootstrap::random_id;
use samplez_replica::bridge::{EngineAudio, input_entry};
use samplez_replica::contact::{Audio, Timing};
use samplez_replica::verify::Verifier;

const B: u64 = 48;

fn in_vm() -> bool {
    if std::env::var("SAMPLEZ_VM_CLOCK").as_deref() == Ok("1") {
        return true;
    }
    eprintln!("skipped: steps the kernel clock, set SAMPLEZ_VM_CLOCK=1 inside a VM");
    false
}

/// An MXL domain on `/dev/shm`, removed on drop.
struct Domain(PathBuf);

impl Drop for Domain {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn wait_for(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within {secs} s");
        std::thread::sleep(Duration::from_millis(5));
    }
}

const SOURCE: &str = "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a";
const FEED: &str = "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f01";

/// A replicated engine of three voters reading the flow a second engine
/// writes, so each block waits in libmxl for its input up to the
/// deadline, and writing one output.
fn engine() -> (Domain, Arc<Engine>, EngineAudio) {
    let dir = PathBuf::from("/dev/shm").join(format!(
        "samplez-vm-{:016x}",
        random_id().expect("a random id")
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let (feeder, _) = start(&dir, "0b1c9a52-4f0e-4c55-9d0d-6b1f2b8e3a10", FEED);
    wait_for("the feed realised", 10, || {
        feeder.status().outputs[0].realised
    });
    let (engine, (status_rx, hash_rx)) = start(&dir, FEED, SOURCE);
    // The feeder lives as long as the process.
    std::mem::forget(feeder);
    let timing = Timing {
        margin: 960,
        ..Timing::defaults(B)
    };
    let audio = EngineAudio::new(
        engine.clone(),
        status_rx,
        hash_rx,
        &timing,
        Box::new(input_entry),
    );
    wait_for("a block processed", 10, || {
        audio.sample().own.processed.is_some()
    });
    (Domain(dir), engine, audio)
}

type Channels = (
    std::sync::mpsc::Receiver<samplez::blocks::StatusChange>,
    std::sync::mpsc::Receiver<samplez::blocks::PageHash>,
);

/// A replicated engine of three voters on the domain at `dir`, reading
/// flow `input` and writing flow `output`.
fn start(dir: &std::path::Path, input: &str, output: &str) -> (Arc<Engine>, Channels) {
    let libmxl =
        std::env::var("SAMPLEZ_TEST_LIBMXL").unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".into());
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": dir }],
        "inputs": [{ "name": "in0", "domain": "d", "flowId": input, "channels": 1 }],
        "outputs": [{ "name": "out0", "domain": "d", "flowId": output, "label": "vm",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (reports, _) = std::sync::mpsc::sync_channel(64);
    let (status_tx, status_rx) = std::sync::mpsc::sync_channel(1 << 12);
    let (hash_tx, hash_rx) = std::sync::mpsc::sync_channel(1 << 10);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 3,
        reports,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let engine = Arc::new(Engine::start_replicated(config, process, None, replication).unwrap());
    (engine, (status_rx, hash_rx))
}

/// Sets the kernel TAI offset (`ADJ_TAI`): CLOCK_TAI moves by the
/// difference, CLOCK_REALTIME does not.
fn set_tai_offset(s: i64) {
    // SAFETY: a zeroed timex is valid input; only `constant` is read.
    let mut tx: libc::timex = unsafe { std::mem::zeroed() };
    tx.modes = libc::ADJ_TAI;
    tx.constant = s as _;
    // SAFETY: `tx` is a valid, writable timex.
    let r = unsafe { libc::adjtimex(&mut tx) };
    assert!(r >= 0, "adjtimex: {}", std::io::Error::last_os_error());
}

fn clock_ns(clock: libc::clockid_t) -> i128 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is writable.
    unsafe { libc::clock_gettime(clock, &mut ts) };
    i128::from(ts.tv_sec) * 1_000_000_000 + i128::from(ts.tv_nsec)
}

/// The offset and the steps the engine's RT thread sampled.
fn clock(audio: &EngineAudio) -> (Option<i64>, u64) {
    let s = audio.sample();
    (s.own.clock.tai_offset_s, s.clock_steps)
}

/// An `adjtimex` step of the TAI offset steps CLOCK_TAI: the RT thread
/// detects the step, its offset sample moves with it, and the replica
/// loses its verification.
#[test]
fn an_adjtimex_offset_step_unverifies_the_replica() {
    if !in_vm() {
        return;
    }
    let (_domain, _engine, audio) = engine();
    set_tai_offset(36);
    wait_for("offset 36 sampled", 10, || clock(&audio).0 == Some(36));
    std::thread::sleep(Duration::from_millis(100));
    let mut v = Verifier::default();
    let (offset, steps) = clock(&audio);
    v.period(offset, steps, || true);
    assert!(v.verified());
    // Forward: the RT thread detects a backward step only at its next
    // wake, up to the step's length later.
    set_tai_offset(37);
    wait_for("the step detected", 10, || clock(&audio).1 > steps);
    let (after, steps_after) = clock(&audio);
    assert_eq!(after, Some(37), "the sample after the step");
    assert_eq!(steps_after, steps + 1, "one step");
    v.period(after, steps_after, || false);
    assert!(!v.verified(), "an offset change across a step unverifies");
}

/// A leap second the kernel inserts (STA_INS, armed by the kernel
/// selftest `leap-a-day` shortly before UTC midnight) moves the offset
/// 37 -> 38 with CLOCK_TAI continuous: no step is detected across it and
/// verification holds. The deadline misses of libmxl's CLOCK_REALTIME
/// deadline waits across the 1 s step are counted and printed.
#[test]
fn a_leap_second_moves_the_offset_without_a_step() {
    if !in_vm() {
        return;
    }
    let leap = std::env::var("SAMPLEZ_LEAP_A_DAY").expect("SAMPLEZ_LEAP_A_DAY names leap-a-day");
    let (_domain, engine, audio) = engine();
    set_tai_offset(37);
    wait_for("offset 37 sampled", 10, || clock(&audio).0 == Some(37));
    // Each iteration sets the time to 10 s before the next UTC midnight,
    // a step with the offset unchanged, and arms STA_INS; `-i 1` runs one.
    let mut child = std::process::Command::new(&leap)
        .args(["-i", "1"])
        .spawn()
        .expect("leap-a-day starts");
    let mut v = Verifier::default();
    let mut samples = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut misses_before = None;
    loop {
        let (offset, steps) = clock(&audio);
        let gap = clock_ns(libc::CLOCK_TAI) - clock_ns(libc::CLOCK_MONOTONIC);
        let misses = engine.counters().deadline_miss.load(Ordering::Relaxed);
        if offset == Some(37) {
            misses_before = Some(misses);
        }
        v.period(offset, steps, || true);
        assert!(v.verified(), "verification ended at offset {offset:?}");
        samples.push((Instant::now(), offset, steps, gap, misses));
        if offset == Some(38)
            && samples
                .iter()
                .find(|s| s.1 == Some(38))
                .is_some_and(|s| s.0.elapsed() > Duration::from_secs(2))
        {
            break;
        }
        assert!(Instant::now() < deadline, "no leap within 60 s");
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    let at = samples
        .iter()
        .position(|s| s.1 == Some(38))
        .expect("the offset moved");
    let before = &samples[at - 1];
    assert_eq!(before.1, Some(37));
    // CLOCK_TAI continuous across the leap: no step detected and no jump
    // against CLOCK_MONOTONIC from the last sample before to the last one.
    let last = samples.last().unwrap();
    assert_eq!(before.2, last.2, "a step detected across the leap");
    let jump = (last.3 - before.3).abs();
    assert!(jump < 50_000_000, "CLOCK_TAI jumped {jump} ns at the leap");
    let misses = last.4 - misses_before.expect("a sample at 37");
    println!("deadline misses across the leap second: {misses}");
    set_tai_offset(37);
}
