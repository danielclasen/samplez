//! The engine bridge against a real replicated engine on an MXL domain:
//! what reports carry of it, and what contact hands it.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use samplez::blocks::{HEALTHY, PAGE};
use samplez::{Config, Engine, Replication};
use samplez_replica::bootstrap::random_id;
use samplez_replica::bridge::{EngineAudio, PAGES_REPORTED, input_entry};
use samplez_replica::contact::{Audio, Timing};
use samplez_replica::entry::{Body, Request};
use samplez_replica::report::{page_digest, status_of};
use samplez_replica::resync::Source;

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
    let (hash_tx, hash_rx) = std::sync::mpsc::sync_channel(1 << 10);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 3,
        reports,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let engine = Arc::new(Engine::start_replicated(config, process, None, replication).unwrap());
    wait_for("output realised", || engine.status().outputs[0].realised);
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

#[test]
fn the_supervisor_moves_the_engines_blocks_through_the_bridge() {
    use samplez::blocks::{Cause, Kind, Ranks, Standing};
    let domain = Domain::new();
    let (engine, audio) = bridge(&domain, 1 << 16);
    engine.commit_from(0, 0).unwrap();
    audio.ranks(Ranks::flat(3));
    audio.fence(u64::MAX);
    // Nobody writes the input: every block is degraded and held at the
    // rank.
    let held = |audio: &EngineAudio| {
        let p = engine.progress().unwrap().processed?;
        (p.saturating_sub(4)..=p)
            .find(|&k| audio.status(0, k).is_some_and(|s| s.kind == Kind::Held))
    };
    wait_for("a held block", || held(&audio).is_some());
    let k = held(&audio).unwrap();
    assert_eq!(audio.status(0, k).unwrap().rank, 3);
    assert!(matches!(
        audio.sample().own.standing[..],
        [Standing::Committing { .. }]
    ));
    assert!(audio.commit_through(0, k, 0b10, None));
    wait_for("committed through", || {
        audio
            .status(0, k)
            .is_some_and(|s| s.kind == Kind::Committed && s.on_silence_of == 0b10)
    });
    wait_for("a later held block", || held(&audio).is_some_and(|j| j > k));
    let j = held(&audio).unwrap();
    let s = audio.withhold(0, j, Cause::HoldCap).unwrap();
    assert_eq!((s.kind, s.cause), (Kind::Withheld, Cause::HoldCap));
    wait_for("withdrawn", || {
        matches!(
            audio.sample().own.standing[..],
            [Standing::Withdrawn { since }] if since > k
        )
    });
}

/// Reports carry each output's latest page records, an idle output's
/// too: uncommitted and without digest while it gives no status, and once
/// it commits with the degradation its blocks' statuses show.
#[test]
fn reports_carry_the_latest_page_hashes() {
    let domain = Domain::new();
    let (engine, audio) = bridge(&domain, 1 << 16);
    wait_for("an idle output's page hashed", || {
        !audio.sample().own.pages.is_empty()
    });
    let idle = audio.sample().own.pages;
    assert!(idle.len() <= PAGES_REPORTED, "{idle:?}");
    assert!(
        idle.iter()
            .all(|p| !p.committed && !p.degraded && p.digest.is_none()),
        "{idle:?}"
    );
    engine.set_fence(u64::MAX).unwrap();
    engine.commit_from(0, 0).unwrap();
    // Read as the records arrive: a block's status is gone once its slot
    // is reused.
    let blocks = |q: u64| q * PAGE / B..=((q + 1) * PAGE - 1) / B;
    let mut checked = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while checked < 3 {
        assert!(
            Instant::now() < deadline,
            "{checked} pages with statuses in 10 s"
        );
        for page in audio.sample().own.pages {
            let statuses: Vec<_> = blocks(page.page)
                .map(|k| engine.block_status(0, k))
                .collect();
            if statuses.iter().any(Option::is_none) {
                continue;
            }
            let digests: Vec<Option<u64>> = statuses.iter().map(|s| s.unwrap().digest).collect();
            let degraded = digests.iter().any(|d| d.is_some_and(|d| d != HEALTHY));
            assert_eq!(page.degraded, degraded, "{page:?} {statuses:?}");
            assert_eq!(page.digest, page_digest(digests), "{page:?} {statuses:?}");
            checked += 1;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The horizon reads the processed block while the engine is dropped: the
/// RT thread keeps moving it without a core, so a dropped replica keeps
/// reporting one, and a rebuild installs a core again.
#[test]
fn a_dropped_engine_keeps_reporting_its_processed_block() {
    let domain = Domain::new();
    let (_engine, audio) = bridge(&domain, 1 << 12);
    let audio = audio.with_rebuild(Box::new(|| {
        Some(samplez::ProductState {
            gains: vec![vec![1.0]],
            up: vec![true],
            pending: Vec::new(),
            last_id: 0,
        })
    }));
    wait_for("a block processed", || {
        audio.sample().own.processed.is_some()
    });
    audio.drop_engine();
    let dropped_at = audio.sample().own.processed.expect("processed");
    wait_for("blocks processed without a core", || {
        audio
            .sample()
            .own
            .processed
            .is_some_and(|k| k > dropped_at + 4)
    });
    wait_for("the core rebuilt", || audio.rebuild_engine());
}

struct Feed(bool);

impl Source for Feed {
    fn entries(&self, _after: u64) -> Vec<(u64, Request)> {
        Vec::new()
    }
    fn go(&self) -> bool {
        self.0
    }
}

/// A resync between two engines through the bridge: the donor's core,
/// encoded, is replayed on the joiner, which hands it over at a block
/// boundary and holds the donor's unresolved entry; told to stop, a
/// joiner hands nothing over.
#[test]
fn a_donors_core_is_replayed_and_handed_over_on_the_joiner() {
    let (da, db) = (Domain::new(), Domain::new());
    let (donor, donor_audio) = bridge(&da, 64);
    let (joiner, joiner_audio) = bridge(&db, 64);
    let processed = |e: &Engine| e.progress().and_then(|p| p.processed);
    wait_for("both compute", || {
        processed(&donor).is_some() && processed(&joiner).is_some()
    });
    let at = (processed(&donor).unwrap() + 100_000) * B;
    donor.unresolved(9, at).unwrap();
    let installed = || {
        joiner
            .counters()
            .handovers_installed
            .load(Ordering::Relaxed)
    };
    let core = donor_audio.copy_core().expect("a core");
    let r = joiner_audio.resync(&core, &Feed(true)).unwrap();
    assert!(r.j >= core.k0, "handed over after {} from {}", r.j, core.k0);
    assert_eq!(r.unresolved, vec![(9, at)]);
    assert_eq!(installed(), 1);
    let core = donor_audio.copy_core().expect("a core");
    assert!(joiner_audio.resync(&core, &Feed(false)).is_err());
    assert_eq!(installed(), 1, "no hand-over once told to stop");
}
