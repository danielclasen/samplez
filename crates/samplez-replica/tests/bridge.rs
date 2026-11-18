//! The engine bridge against a real replicated engine on an MXL domain:
//! what reports carry of it, and what contact hands it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use samplez::blocks::{HEALTHY, PAGE};
use samplez::{Config, Engine, Entry, InputState, Replication};
use samplez_dsp::StripParam;
use samplez_mxl::output::{self, Creation, OutputConfig};
use samplez_mxl::{Instance, time};
use samplez_replica::bootstrap::random_id;
use samplez_replica::bridge::{EngineAudio, PAGES_REPORTED};
use samplez_replica::contact::{Audio, Timing};
use samplez_replica::report::{Page, page_digest, status_of};
use samplez_replica::resync::{Missed, Source};
use samplez_replica::state_machine::Record;

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
    bridge_on(
        domain,
        changes,
        [
            "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a",
            "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f01",
        ],
    )
}

/// [`bridge`] with input and output flows `flows`.
fn bridge_on(domain: &Domain, changes: usize, flows: [&str; 2]) -> (Arc<Engine>, EngineAudio) {
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "in0", "domain": "d", "flowId": flows[0], "channels": 1 }],
        "outputs": [{ "name": "out0", "domain": "d", "flowId": flows[1], "label": "bridge",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    bridge_of(&v, changes)
}

/// [`bridge`] of configuration `v`.
fn bridge_of(v: &serde_json::Value, changes: usize) -> (Arc<Engine>, EngineAudio) {
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
    wait_for("outputs realised", || {
        engine.status().outputs.iter().all(|o| o.realised)
    });
    let timing = Timing {
        margin: 960,
        ..Timing::defaults(B)
    };
    let audio = EngineAudio::new(engine.clone(), status_rx, hash_rx, &timing);
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
    // The window: D + w + 2p at 48 kHz, one more block.
    let window = (22 * 48u64).div_ceil(B) + 1;
    // Until the engine has a status word for every block of a window and
    // the channel lost changes: however long start-up and the commit
    // start take on a loaded machine.
    wait_for("a window of status words and changes lost", || {
        let progress = engine.progress().unwrap();
        progress.status_changes_lost > 0
            && progress.processed.is_some_and(|p| {
                p + 1 >= window && (p + 1 - window..=p).all(|k| engine.block_status(0, k).is_some())
            })
    });
    let sample = audio.sample();
    let processed = sample.own.processed.expect("blocks computed");
    let progress = engine.progress().unwrap();
    assert!(progress.processed >= Some(processed));
    assert!(
        sample.status_changes_lost > 0,
        "the channel had room for every change: the test proves nothing"
    );
    let runs = &sample.own.statuses[0];
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
    let consumed = || engine.counters().entries_consumed.load(Ordering::Relaxed);
    let before = consumed();
    audio.appended(7, at_index, &[Entry::InputDown { slot: 0 }]);
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(consumed(), before, "appended, not yet handed");
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
fn an_entry_the_engine_refuses_is_counted_in_every_sample() {
    let domain = Domain::new();
    let (_engine, audio) = bridge(&domain, 1 << 16);
    assert_eq!(audio.sample().entries_refused, 0);
    // Off a block start, as a lease expiry from an older leader derives
    // under a block size that does not divide the fallback lead: taken
    // at the next one, which keeps the log's order.
    audio.appended(3, 48_001, &[Entry::InputDown { slot: 0 }]);
    assert_eq!(audio.sample().entries_refused, 0);
    // Not above the last log id: the engine refuses the batch.
    audio.appended(3, 48_048, &[Entry::InputDown { slot: 0 }]);
    assert_eq!(audio.sample().entries_refused, 1);
}

/// An engine entry the engine refuses is left out of its record before
/// the engine sees it, and counted; the rest of the record is appended.
/// Unlike a refused batch it stops no writes: every replica of the key
/// leaves it out alike.
#[test]
fn a_derived_entry_the_engine_refuses_is_left_out_and_counted() {
    let domain = Domain::new();
    let (engine, audio) = bridge(&domain, 1 << 16);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    let at = (engine.progress().unwrap().processed.unwrap() + 100) * B;
    let consumed = || engine.counters().entries_consumed.load(Ordering::Relaxed);
    let before = consumed();
    // No input slot 5, and a gain beyond the engine's bound.
    let entries = [
        Entry::InputDown { slot: 5 },
        Entry::Gain {
            input: 0,
            output: 0,
            gain: f32::INFINITY,
        },
        Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
        },
    ];
    audio.appended(3, at, &entries);
    audio.committed(3);
    let sample = audio.sample();
    assert_eq!(
        (sample.entries_filtered, sample.entries_refused),
        (2, 0),
        "two left out, the batch taken"
    );
    wait_for("the rest taken by the RT thread", || {
        consumed() == before + 1
    });
    wait_for("its index passed", || {
        engine.progress().unwrap().processed.unwrap() > at / B + 480 / B
    });
    let copy = engine.copy_core(Duration::from_secs(2)).unwrap();
    assert_eq!(copy.core.gain(0, 0), 0.5);
}

/// Of the `InputUp`s and `Route`s of one record whose slots lie in two
/// input domains, the engine builds one generation: those of the domain
/// of the lowest slot stay, the others are left out.
#[test]
fn a_record_raising_inputs_of_two_domains_keeps_the_lowest_slots_domain() {
    let (d0, d1) = (Domain::new(), Domain::new());
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "a", "path": d0.0 }, { "name": "b", "path": d1.0 }],
        "inputs": [
            { "name": "in0", "domain": "b", "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd4a00", "channels": 1 },
            { "name": "in1", "domain": "a", "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd4a01", "channels": 1 },
            { "name": "in2", "domain": "a", "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd4a02", "channels": 1 },
        ],
        "outputs": [{ "name": "out0", "domain": "a", "flowId": "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f20",
                      "label": "domains", "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0], [1.0], [1.0]],
        "rt": { "rtCpu": "none" },
    });
    let (engine, audio) = bridge_of(&v, 1 << 12);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    let at = (engine.progress().unwrap().processed.unwrap() + 100) * B;
    let up = |slot| Entry::InputUp { slot };
    assert!(engine.admits(&[up(2), up(1), up(0)]).is_err());
    audio.appended(4, at, &[up(2), up(1), up(0)]);
    audio.committed(4);
    // Slot 0's domain, b, stays, though slots 1 and 2 of domain a come
    // first in the record.
    let sample = audio.sample();
    assert_eq!((sample.entries_filtered, sample.entries_refused), (2, 0));
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
    let state = samplez::ProductState {
        gains: vec![vec![1.0]],
        up: vec![true],
        flows: vec!["5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a".to_owned()],
        strips: vec![Vec::new()],
        pending: Vec::new(),
        last_id: 0,
    };
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
    wait_for("the core rebuilt", || audio.rebuild_engine(&state, 0));
}

/// A joiner's replica: go while it says so, its records gone when
/// `missed`.
struct Feed {
    go: bool,
    missed: bool,
}

impl Source for Feed {
    fn entries(&self, _after: u64) -> Result<Vec<(u64, Record)>, Missed> {
        if self.missed {
            Err(Missed)
        } else {
            Ok(Vec::new())
        }
    }
    fn go(&self) -> bool {
        self.go
    }
}

/// A resync between two engines through the bridge: the donor's core,
/// encoded, is replayed on the joiner, which hands it over at a block
/// boundary and holds the donor's unresolved entry; told to stop, or
/// without the records after the donor's core, a joiner hands nothing
/// over.
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
    let feed = |go, missed| Feed { go, missed };
    let r = joiner_audio.resync(&core, &feed(true, false)).unwrap();
    assert!(r.j >= core.k0, "handed over after {} from {}", r.j, core.k0);
    assert_eq!(r.unresolved, vec![(9, at)]);
    assert_eq!(installed(), 1);
    let core = donor_audio.copy_core().expect("a core");
    assert!(joiner_audio.resync(&core, &feed(false, false)).is_err());
    assert_eq!(installed(), 1, "no hand-over once told to stop");
    let core = donor_audio.copy_core().expect("a core");
    assert!(joiner_audio.resync(&core, &feed(true, true)).is_err());
    assert_eq!(installed(), 1, "no hand-over over a gap");
}

/// A mixer command's record: it ramps the coefficient to `gain` and
/// trims the input's strip by `trim` dB, two engine entries for one log
/// entry.
fn mixer(at_index: u64, gain: f32, trim: f32) -> Record {
    Record {
        at_index,
        entries: vec![
            Entry::Gain {
                input: 0,
                output: 0,
                gain,
            },
            Entry::Strip {
                slot: 0,
                param: StripParam::Trim { db: trim },
            },
        ],
        generation: false,
    }
}

/// A sine on flow `id`, written block by block as libmxl's clock passes
/// each block, until dropped.
struct Pilot {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Pilot {
    fn start(domain: &Domain, id: &str) -> Pilot {
        let api = samplez_mxl::load(&libmxl()).unwrap();
        let instance = Instance::new(&api, &domain.0).unwrap();
        let cfg = OutputConfig {
            flow_id: id,
            label: "pilot",
            group: "pilot",
            role: "Audio",
            channels: 1,
            block: B,
            batch: B,
        };
        let Creation::Created { mut writer, .. } = output::create(&instance, &cfg).unwrap() else {
            panic!("pilot flow {id} not created");
        };
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let mut next = None;
            let mut data = vec![0.0f32; B as usize];
            while !flag.load(Ordering::Relaxed) {
                let idx = time::timestamp_to_index(&api, time::now(&api));
                if let Some(due) = (idx / B).checked_sub(1) {
                    for k in next.unwrap_or(due)..=due {
                        for (i, v) in data.iter_mut().enumerate() {
                            let n = (k * B + i as u64) % 48;
                            *v = (2.0 * std::f32::consts::PI * n as f32 / 48.0).sin();
                        }
                        writer.write_block(k, &data);
                    }
                    next = Some(due + 1);
                }
                let k = next.unwrap_or(0);
                time::sleep_until(&api, time::index_to_timestamp(&api, (k + 1) * B));
            }
            drop(writer);
            drop(instance);
        });
        Pilot {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Pilot {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}

/// A log entry carrying several engine entries keeps a resynced replica
/// equal to its donor: the joiner learns the log entry, a mixer command
/// of a gain and a strip entry, only from the log after the donor's core,
/// replays it whole and from the hand-over computes the donor's pages
/// through both ramps. A joiner missing either entry computes other
/// pages from the log entry's index on.
#[test]
fn a_command_of_several_entries_keeps_a_resynced_replica_equal() {
    undisturbed(|| resync_after(1));
}

/// A resync while a console is busy: more log entries commit between
/// the donor's core and the replay than the replay holds pending at once.
/// It replays them as it appends them and the joiner computes the
/// donor's pages from the hand-over on.
#[test]
fn a_resync_takes_more_entries_than_it_holds_pending() {
    undisturbed(|| resync_after(100));
}

/// Runs `scenario` until a run is not disturbed, four times at most.
fn undisturbed(scenario: impl Fn() -> Result<(), String>) {
    let mut seen = Vec::new();
    for _ in 0..3 {
        match scenario() {
            Ok(()) => return,
            Err(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

struct Log(Vec<(u64, Record)>);

impl Source for Log {
    fn entries(&self, after: u64) -> Result<Vec<(u64, Record)>, Missed> {
        Ok(self.0.iter().filter(|(i, _)| *i > after).cloned().collect())
    }
    fn go(&self) -> bool {
        true
    }
}

/// A donor's core copied, then the records of `n` mixer commands handed
/// to the donor, each 40 blocks ahead as it applies, which a joiner
/// learns only from its replica's records in its resync.
fn resync_after(n: u64) -> Result<(), String> {
    let domain = Domain::new();
    // A second of history: the replay reads from the donor's core on.
    std::fs::write(
        domain.0.join("options.json"),
        r#"{"urn:x-mxl:option:history_duration/v1.0": 1000000000}"#,
    )
    .unwrap();
    let input = "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8d";
    let _pilot = Pilot::start(&domain, input);
    let (donor, donor_audio) = bridge_on(
        &domain,
        1 << 12,
        [input, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f04"],
    );
    let (joiner, joiner_audio) = bridge_on(
        &domain,
        1 << 12,
        [input, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f05"],
    );
    wait_for("both read the input", || {
        [&donor, &joiner].iter().all(|e| {
            e.status().inputs[0].state == InputState::Attached
                && e.progress().and_then(|p| p.processed).is_some()
        })
    });
    // Past the fade-in from the attach.
    std::thread::sleep(Duration::from_millis(200));
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Ordering::Relaxed);
    let silenced_before = silenced(&donor) + silenced(&joiner);
    let core = donor_audio.copy_core().ok_or("no core copied")?;
    let mut log = Vec::new();
    let mut at = 0;
    for id in 1..=n {
        let processed = donor.progress().and_then(|p| p.processed).unwrap();
        at = (processed + 40) * B;
        let record = mixer(at, 0.5 + (id % 4) as f32 / 10.0, -((id % 7) as f32));
        donor_audio.appended(id, at, &record.entries);
        donor_audio.committed(id);
        log.push((id, record));
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(donor_audio.sample().entries_refused, 0);
    let r = joiner_audio.resync(&core, &Log(log))?;
    // Past both ramps, by four pages.
    let ramp = 480;
    let end = (at + ramp) / B + 4 * PAGE / B;
    let (mut pd, mut pj) = (BTreeMap::new(), BTreeMap::new());
    let keep = |pages: &mut BTreeMap<u64, Page>, audio: &EngineAudio| {
        for p in audio.sample().own.pages {
            pages.insert(p.page, p);
        }
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while [&donor, &joiner]
        .iter()
        .any(|e| e.progress().and_then(|p| p.processed).unwrap_or(0) <= end + PAGE / B)
    {
        assert!(Instant::now() < deadline, "pages within 10 s");
        keep(&mut pd, &donor_audio);
        keep(&mut pj, &joiner_audio);
        std::thread::sleep(Duration::from_millis(5));
    }
    keep(&mut pd, &donor_audio);
    keep(&mut pj, &joiner_audio);
    if silenced(&donor) + silenced(&joiner) > silenced_before {
        return Err("an input block silenced".into());
    }
    let from = ((r.j + 1) * B).div_ceil(PAGE);
    let mut compared = 0;
    for p in from..=end * B / PAGE {
        let (Some(d), Some(j)) = (pd.get(&p), pj.get(&p)) else {
            continue;
        };
        if (d.hash, d.fingerprint) != (j.hash, j.fingerprint) {
            return Err(format!("page {p} differs"));
        }
        if p >= at / PAGE {
            compared += 1;
        }
    }
    if compared < 4 {
        return Err(format!(
            "{compared} pages from the log entry's index compared"
        ));
    }
    assert_eq!(joiner.counters().command_late.load(Ordering::Relaxed), 0);
    Ok(())
}

/// Rows and columns of the large shape: 16 stereo channels, or four
/// inputs of eight legs, to programme, four auxes, four mix-minus and
/// two monitor buses, all stereo.
const ROWS: usize = 32;
const COLUMNS: usize = 22;

/// A scene recall's engine entries: every coefficient of the matrix and
/// every strip parameter of the four strips, each set twice, 1616 for one
/// log entry; the gains scaled by `scale`.
fn scene(scale: f32) -> Vec<Entry> {
    let mut entries = Vec::new();
    for pass in [0.5, 1.0] {
        for input in 0..ROWS {
            for output in 0..COLUMNS {
                entries.push(Entry::Gain {
                    input,
                    output,
                    gain: pass * scene_gain(scale, input, output),
                });
            }
        }
        for slot in 0..ROWS / 8 {
            for param in strip_params(pass) {
                entries.push(Entry::Strip { slot, param });
            }
        }
    }
    entries
}

fn scene_gain(scale: f32, input: usize, output: usize) -> f32 {
    scale * ((input * COLUMNS + output) % 7) as f32 / 7.0
}

/// Every parameter of a strip of eight legs.
fn strip_params(pass: f32) -> Vec<StripParam> {
    use samplez_dsp::{BandType, CompParams, EqBand, GateParams};
    let mut params = vec![
        StripParam::Trim { db: -3.0 * pass },
        StripParam::Hpf {
            enabled: true,
            freq: 80.0,
        },
        StripParam::Eq { enabled: true },
        StripParam::Gate(GateParams {
            enabled: true,
            threshold_db: -50.0,
            range_db: 30.0,
            attack_ms: 1.0,
            release_ms: 200.0,
        }),
        StripParam::Comp(CompParams {
            enabled: true,
            threshold_db: -20.0,
            ratio: 4.0,
            attack_ms: 5.0,
            release_ms: 300.0,
            knee_db: 6.0,
            makeup_db: 2.0,
        }),
        StripParam::DynamicsBeforeEq { enabled: true },
    ];
    for leg in 0..8 {
        params.push(StripParam::LegTrim { leg, db: pass });
        params.push(StripParam::Polarity {
            leg,
            inverted: leg % 2 == 0,
        });
    }
    for band in 0..4 {
        params.push(StripParam::EqBand {
            band,
            value: EqBand {
                kind: BandType::Peak,
                freq: 250.0 * (band + 1) as f32,
                gain_db: 3.0 * pass,
                q: 1.0,
            },
        });
    }
    params
}

/// Every replica takes a run of 64 scene recalls of 1616 engine entries
/// each, as many log entries as the leader stamps ahead, refusing none,
/// and applies the last one whole at its index: a log entry's size is
/// bounded by the engine's shape, not by a count of engine entries, and
/// the engine holds as many log entries as the leader stamps ahead.
#[test]
fn scene_recalls_on_a_large_shape_apply_on_every_replica() {
    let domains: Vec<Domain> = (0..3).map(|_| Domain::new()).collect();
    let replicas: Vec<(Arc<Engine>, EngineAudio)> = domains
        .iter()
        .enumerate()
        .map(|(r, domain)| {
            let flow = |n: usize| format!("5fbec3b1-1b0b-417e-a9f4-a7b1f1fd{r:02x}{n:02x}");
            let v = serde_json::json!({
                "blockSize": B,
                "readerMarginSamples": 960,
                "libmxl": libmxl(),
                "domains": [{ "name": "d", "path": domain.0 }],
                "inputs": (0..ROWS / 8).map(|n| serde_json::json!({
                    "name": format!("in{n}"), "domain": "d", "flowId": flow(n),
                    "channels": 8 })).collect::<Vec<_>>(),
                "outputs": (0..2).map(|n| serde_json::json!({
                    "name": format!("out{n}"), "domain": "d", "flowId": flow(0x10 + n),
                    "label": "scene", "group": "samplez-test", "role": format!("Audio {n}"),
                    "channels": COLUMNS / 2 })).collect::<Vec<_>>(),
                "matrix": vec![vec![0.0; COLUMNS]; ROWS],
                "rt": { "rtCpu": "none" },
            });
            bridge_of(&v, 1 << 12)
        })
        .collect();
    let processed = |e: &Engine| e.progress().and_then(|p| p.processed);
    wait_for("every replica computes", || {
        replicas.iter().all(|(e, _)| processed(e).is_some())
    });
    let first = replicas
        .iter()
        .map(|(e, _)| processed(e).unwrap())
        .max()
        .unwrap();
    let recalls = 64u64;
    let at = |id: u64| (first + 500 + id) * B;
    for id in 1..=recalls {
        let entries = scene(id as f32 / recalls as f32);
        assert_eq!(entries.len(), 1616);
        for (_, audio) in &replicas {
            audio.appended(id, at(id), &entries);
        }
    }
    for (_, audio) in &replicas {
        audio.committed(recalls);
    }
    let end = at(recalls) / B + 480 / B + 2;
    wait_for("the last recall ramped", || {
        replicas.iter().all(|(e, _)| processed(e) > Some(end))
    });
    for (e, audio) in &replicas {
        assert_eq!(audio.sample().entries_refused, 0);
        let c = e.counters();
        assert_eq!(c.command_late.load(Ordering::Relaxed), 0);
        assert_eq!(c.strip_refused.load(Ordering::Relaxed), 0);
        let copy = loop {
            match e.copy_core(Duration::from_secs(2)) {
                Err(samplez::CopyError::Passed { .. }) => continue,
                c => break c.unwrap(),
            }
        };
        for input in 0..ROWS {
            for output in 0..COLUMNS {
                assert_eq!(
                    copy.core.gain(input, output),
                    scene_gain(1.0, input, output),
                    "{input} -> {output}"
                );
            }
        }
    }
}

const SOURCES: [&str; 4] = [
    "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd3a00",
    "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd3a01",
    "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd3a02",
    "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd3a03",
];

/// A recall re-routing both inputs to the other two sources.
fn recall() -> Vec<Entry> {
    (0..2)
        .map(|slot| Entry::Route {
            slot,
            flow_id: SOURCES[2 + slot].to_owned(),
        })
        .collect()
}

/// A recall changing the sources of two inputs of one domain applies on
/// every replica, one generation re-routing both, and no replica counts
/// a refused entry.
#[test]
fn a_two_source_recall_applies_on_every_replica() {
    let domains: Vec<Domain> = (0..3).map(|_| Domain::new()).collect();
    let replicas: Vec<(Arc<Engine>, EngineAudio)> = domains
        .iter()
        .map(|domain| {
            let v = serde_json::json!({
                "blockSize": B,
                "readerMarginSamples": 960,
                "libmxl": libmxl(),
                "domains": [{ "name": "d", "path": domain.0 }],
                "inputs": (0..2).map(|n| serde_json::json!({
                    "name": format!("in{n}"), "domain": "d", "flowId": SOURCES[n],
                    "channels": 1 })).collect::<Vec<_>>(),
                "outputs": [{ "name": "out0", "domain": "d",
                              "flowId": "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f10",
                              "label": "recall", "group": "samplez-test", "role": "Audio 0",
                              "channels": 1 }],
                "matrix": [[1.0], [1.0]],
                "rt": { "rtCpu": "none" },
            });
            bridge_of(&v, 1 << 12)
        })
        .collect();
    let processed = |e: &Engine| e.progress().and_then(|p| p.processed);
    wait_for("every replica computes", || {
        replicas.iter().all(|(e, _)| processed(e).is_some())
    });
    let first = replicas
        .iter()
        .map(|(e, _)| processed(e).unwrap())
        .max()
        .unwrap();
    let at = (first + 300) * B;
    for (_, audio) in &replicas {
        audio.appended(1, at, &recall());
        audio.committed(1);
    }
    wait_for("both inputs re-routed", || {
        replicas.iter().all(|(e, _)| {
            let s = e.status();
            s.inputs[0].flow_id == SOURCES[2] && s.inputs[1].flow_id == SOURCES[3]
        })
    });
    for (e, audio) in &replicas {
        assert_eq!(audio.sample().entries_refused, 0);
        assert_eq!(e.counters().command_late.load(Ordering::Relaxed), 0);
    }
}

const ROUTED: &str = "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b9e";

/// A command re-routing input 0 to [`ROUTED`] and trimming its strip.
fn route_and_trim() -> Vec<Entry> {
    vec![
        Entry::Route {
            slot: 0,
            flow_id: ROUTED.to_owned(),
        },
        Entry::Strip {
            slot: 0,
            param: StripParam::Trim { db: -6.0 },
        },
    ]
}

/// A replica rebuilt from product state, as after a snapshot install,
/// gets the flow a `Route` moved a slot to and the strip parameters the
/// log set: from the rebuild on its slot reads the new flow, its old one
/// gone, and its pages equal those of a replica that applied the entry.
#[test]
fn a_rebuilt_replica_reads_the_routed_flow_with_its_strip_parameters() {
    let mut seen = Vec::new();
    for _ in 0..3 {
        match rebuild_after_a_route() {
            Ok(()) => return,
            Err(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

fn rebuild_after_a_route() -> Result<(), String> {
    let domain = Domain::new();
    std::fs::write(
        domain.0.join("options.json"),
        r#"{"urn:x-mxl:option:history_duration/v1.0": 1000000000}"#,
    )
    .unwrap();
    let input = "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b9d";
    let old = Pilot::start(&domain, input);
    let _routed = Pilot::start(&domain, ROUTED);
    let (donor, donor_audio) = bridge_on(
        &domain,
        1 << 12,
        [input, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f06"],
    );
    let (joiner, joiner_audio) = bridge_on(
        &domain,
        1 << 12,
        [input, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f07"],
    );
    let state = samplez::ProductState {
        gains: vec![vec![1.0]],
        up: vec![true],
        flows: vec![ROUTED.to_owned()],
        strips: vec![vec![StripParam::Trim { db: -6.0 }]],
        pending: Vec::new(),
        last_id: 1,
    };
    let processed = |e: &Engine| e.progress().and_then(|p| p.processed).unwrap_or(0);
    wait_for("both read the input", || {
        [&donor, &joiner]
            .iter()
            .all(|e| e.status().inputs[0].state == InputState::Attached && processed(e) > 0)
    });
    let at = (processed(&donor) + 40) * B;
    donor_audio.appended(1, at, &route_and_trim());
    donor_audio.committed(1);
    // The joiner's state jumps past entry 1, as a snapshot install does.
    joiner_audio.drop_engine();
    wait_for("the index passed", || processed(&donor) > at / B + 10);
    wait_for("rebuilt", || joiner_audio.rebuild_engine(&state, 0));
    let rebuilt = processed(&joiner);
    drop(old);
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Ordering::Relaxed);
    let from = ((rebuilt + 1) * B + 2 * 480).div_ceil(PAGE) + 2;
    let end = (from + 6) * PAGE / B;
    let (mut pd, mut pj) = (BTreeMap::new(), BTreeMap::new());
    let keep = |pages: &mut BTreeMap<u64, Page>, audio: &EngineAudio| {
        for p in audio.sample().own.pages {
            pages.insert(p.page, p);
        }
    };
    let silenced_from = std::cell::Cell::new(None);
    let deadline = Instant::now() + Duration::from_secs(10);
    while [&donor, &joiner]
        .iter()
        .any(|e| processed(e) <= end + PAGE / B)
    {
        assert!(Instant::now() < deadline, "pages within 10 s");
        if silenced_from.get().is_none() && processed(&joiner) * B >= from * PAGE {
            silenced_from.set(Some(silenced(&joiner)));
        }
        keep(&mut pd, &donor_audio);
        keep(&mut pj, &joiner_audio);
        std::thread::sleep(Duration::from_millis(5));
    }
    keep(&mut pd, &donor_audio);
    keep(&mut pj, &joiner_audio);
    assert_eq!(
        silenced(&joiner),
        silenced_from.get().unwrap(),
        "the joiner read its old flow"
    );
    let mut compared = 0;
    for p in from..end * B / PAGE {
        let (Some(d), Some(j)) = (pd.get(&p), pj.get(&p)) else {
            continue;
        };
        assert_eq!(j.hash, d.hash, "page {p}");
        compared += 1;
    }
    if compared < 3 {
        return Err(format!("{compared} pages compared"));
    }
    assert_eq!(joiner.status().inputs[0].flow_id, ROUTED);
    Ok(())
}
