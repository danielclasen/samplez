//! The engine bridge against a real replicated engine on an MXL domain:
//! what reports carry of it, and what contact hands it.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use samplez::{Config, Engine, Replication};
use samplez_replica::bootstrap::random_id;
use samplez_replica::bridge::{EngineAudio, input_entry};
use samplez_replica::contact::{Audio, Timing};
use samplez_replica::entry::{Body, Request};
use samplez_replica::report::status_of;

const B: u64 = 48;

fn libmxl() -> PathBuf {
    let path = PathBuf::from(
        std::env::var("SAMPLEZ_TEST_LIBMXL").unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".into()),
    );
    assert!(path.is_file(), "libmxl not found at {}", path.display());
    path
}

/// An MXL domain on `/dev/shm`, removed on drop.
struct Domain(PathBuf);

impl Domain {
    fn new() -> Domain {
        // The process id alone repeats across containers sharing the host's
        // /dev/shm.
        let dir = PathBuf::from("/dev/shm").join(format!(
            "samplez-bridge-{}-{:016x}",
            std::process::id(),
            random_id().expect("a random id")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir(&dir).unwrap();
        Domain(dir)
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "{what} within 10 s");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A replicated engine of three voters, one input whose flow nobody
/// writes, one output; its status channel holds `changes`.
fn bridge(domain: &Domain, changes: usize) -> (Arc<Engine>, EngineAudio) {
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "in0", "domain": "d",
                     "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a", "channels": 1 }],
        "outputs": [{ "name": "out0", "domain": "d",
                      "flowId": "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f01", "label": "bridge",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (reports, _) = std::sync::mpsc::sync_channel(64);
    let (status_tx, status_rx) = std::sync::mpsc::sync_channel(changes);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 3,
        reports,
        statuses: Some(status_tx),
        hashes: None,
    };
    let engine = Arc::new(Engine::start_replicated(config, process, None, replication).unwrap());
    wait_for("output realised", || engine.status().outputs[0].realised);
    let timing = Timing {
        margin: 960,
        ..Timing::defaults(B)
    };
    let audio = EngineAudio::new(engine.clone(), status_rx, &timing, Box::new(input_entry));
    (engine, audio)
}

#[test]
fn reports_carry_every_recent_status_though_the_change_channel_overflows() {
    let domain = Domain::new();
    // Room for one change: nearly every change is lost on the way.
    let (engine, audio) = bridge(&domain, 1);
    // An output commits from its commit start: before it, its blocks
    // take no status.
    engine.commit_from(0, 0).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let sample = audio.sample();
    let processed = sample.own.processed.expect("blocks computed");
    let progress = engine.progress().unwrap();
    assert!(progress.processed >= Some(processed));
    assert!(
        sample.status_changes_lost > 0,
        "the channel had room for every change: the test proves nothing"
    );
    let runs = &sample.own.statuses[0];
    // The window: D + w + 2p at 48 kHz, one more block.
    let window = (22 * 48u64).div_ceil(B) + 1;
    for k in processed + 1 - window..=processed {
        assert!(
            status_of(runs, k).is_some(),
            "block {k} of the window {}..={processed} has no status: {runs:?}",
            processed + 1 - window
        );
    }
    assert!(!sample.own.clock.step_fence);
}

#[test]
fn entries_reach_the_engine_by_log_id_once_committed() {
    let domain = Domain::new();
    let (engine, audio) = bridge(&domain, 1 << 16);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    let processed = engine.progress().unwrap().processed.unwrap();
    let at_index = (processed + 200) * B;
    let request = Request {
        stamp: 0,
        body: Body::InputDown { slot: 0, at_index },
    };
    let consumed = || engine.counters().entries_consumed.load(Ordering::Relaxed);
    let before = consumed();
    audio.appended(7, &request);
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(consumed(), before, "appended, not yet committed");
    audio.committed(7);
    wait_for("the entry taken by the RT thread", || {
        consumed() == before + 1
    });
    assert_eq!(engine.counters().command_late.load(Ordering::Relaxed), 0);
}

#[test]
fn an_unresolved_entry_the_engine_has_no_room_for_is_refused() {
    let domain = Domain::new();
    let (_engine, audio) = bridge(&domain, 1 << 16);
    let at = 1 << 40;
    let given = (1..=200).take_while(|&i| audio.unresolved(i, at)).count();
    assert!(
        (1..200).contains(&given),
        "{given} unresolved entries taken: the engine has room for a bounded number"
    );
}

#[test]
fn a_block_without_a_status_word_is_reported_withheld() {
    use samplez::blocks::{Cause, Kind};
    let domain = Domain::new();
    // No commit start: the output's blocks take no status word.
    let (engine, audio) = bridge(&domain, 1 << 16);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    std::thread::sleep(Duration::from_millis(50));
    let sample = audio.sample();
    let processed = sample.own.processed.unwrap();
    assert_eq!(engine.block_status(0, processed), None);
    let reported = status_of(&sample.own.statuses[0], processed).expect("reported");
    assert_eq!(
        (reported.kind, reported.cause),
        (Kind::Withheld, Cause::Skip)
    );
}

#[test]
fn a_skipped_entry_never_reaches_the_rt_thread() {
    let domain = Domain::new();
    let (engine, audio) = bridge(&domain, 1 << 16);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    let processed = engine.progress().unwrap().processed.unwrap();
    let request = |at| Request {
        stamp: 0,
        body: Body::InputDown {
            slot: 0,
            at_index: at,
        },
    };
    let consumed = || engine.counters().entries_consumed.load(Ordering::Relaxed);
    let before = consumed();
    audio.appended(7, &request((processed + 200) * B));
    audio.appended(8, &request((processed + 201) * B));
    // Its commit came after its apply deadline: the log decides it.
    audio.skip(7);
    audio.committed(8);
    wait_for("entry 8 taken by the RT thread", || {
        consumed() == before + 1
    });
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(consumed(), before + 1, "entry 7 was applied");
}

#[test]
fn an_entry_the_engine_refuses_is_counted_in_every_sample() {
    let domain = Domain::new();
    let (_engine, audio) = bridge(&domain, 1 << 16);
    assert_eq!(audio.sample().entries_refused, 0);
    // No input slot 5: the engine refuses the entry.
    let request = Request {
        stamp: 0,
        body: Body::InputDown {
            slot: 5,
            at_index: 48_000,
        },
    };
    audio.appended(3, &request);
    assert_eq!(audio.sample().entries_refused, 1);
}
