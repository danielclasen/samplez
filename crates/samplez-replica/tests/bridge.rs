//! The engine bridge against a real replicated engine on an MXL domain:
//! what reports carry of it, and what contact hands it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use samplez::blocks::{HEALTHY, PAGE};
use samplez::{Config, Engine, Entry, InputState, Replication, Targets};
use samplez_dsp::{GateParams, StripParam};
use samplez_mxl::output::{self, Creation, OutputConfig};
use samplez_mxl::{Instance, time};
use samplez_replica::bootstrap::random_id;
use samplez_replica::bridge::{EngineAudio, PAGES_REPORTED};
use samplez_replica::contact::{Audio, Timing};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::{Body, Command, Origin, Request, SessionKey};
use samplez_replica::report::{Page, page_digest, status_of};
use samplez_replica::resync::{Missed, Source};
use samplez_replica::state_machine::{Apply, Changed, Effect, Product, Record, StateMachine};

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
    let timing = Timing {
        margin: 960,
        ..Timing::defaults(B)
    };
    bridge_timed(v, changes, &timing)
}

/// [`bridge_of`] with contact's timers `timing`.
fn bridge_timed(
    v: &serde_json::Value,
    changes: usize,
    timing: &Timing,
) -> (Arc<Engine>, EngineAudio) {
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
    let audio = EngineAudio::new(engine.clone(), status_rx, hash_rx, timing);
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

/// A sample drains the engine's page-hash and status-change channels into
/// what every sample reads: a second caller, such as a product reading
/// metrics, takes no page record from contact's next sample.
#[test]
fn a_second_caller_takes_no_page_record_from_contacts_sample() {
    let domain = Domain::new();
    let (_engine, audio) = bridge(&domain, 1 << 16);
    let mut checked = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while checked < 3 {
        assert!(Instant::now() < deadline, "{checked} pages in 10 s");
        let other = audio.sample().own.pages;
        let contact = audio.sample().own.pages;
        for p in &other {
            // Gone only behind as many newer records of its output.
            let newer = (contact.iter())
                .filter(|c| c.output == p.output && c.page > p.page)
                .count();
            assert!(
                contact.contains(p) || newer == PAGES_REPORTED,
                "{p:?} taken: {contact:?}"
            );
        }
        checked += usize::from(!other.is_empty());
        std::thread::sleep(Duration::from_millis(20));
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
        flows: vec![Some("5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a".to_owned())],
        outputs: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
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
    fn entries(&self, _after: u64) -> Result<Vec<(u64, Arc<Record>)>, Missed> {
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

/// A resync onto a donor whose apply lead is at its cap: half the
/// entries after the donor's core reach the donor's engine after their
/// index, which it reports late and does not apply. The replay takes
/// them as the donor did, so the joiner hands over and computes the
/// donor's pages instead of failing every attempt on the first late
/// entry.
#[test]
fn a_resync_takes_entries_the_donor_applied_late() {
    undisturbed(|| resync_with(20, |id| id % 2 == 1));
}

/// More entries taken late after the core than the engine's control
/// ring holds before the replay computes a block: the replay holds them
/// unresolved without the ring, which only a computed block drains, so
/// a burst of them does not fail the attempt.
#[test]
fn a_resync_takes_more_late_entries_than_the_control_ring_holds() {
    undisturbed(|| resync_with(72, |_| true));
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
    fn entries(&self, after: u64) -> Result<Vec<(u64, Arc<Record>)>, Missed> {
        Ok((self.0.iter())
            .filter(|(i, _)| *i > after)
            .map(|(i, r)| (*i, Arc::new(r.clone())))
            .collect())
    }
    fn go(&self) -> bool {
        true
    }
}

/// A donor's core copied, then the records of `n` mixer commands handed
/// to the donor, each 40 blocks ahead as it applies, which a joiner
/// learns only from its replica's records in its resync.
fn resync_after(n: u64) -> Result<(), String> {
    resync_with(n, |_| false)
}

/// [`resync_after`], the commands `late` names handed to the donor two
/// blocks after their index: its engine reports them late and applies
/// none of them. The last command is handed in time.
fn resync_with(n: u64, late: fn(u64) -> bool) -> Result<(), String> {
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
    let late_before = donor.counters().command_late.load(Ordering::Relaxed);
    let mut log = Vec::new();
    let mut at = 0;
    let mut behind = 0;
    for id in 1..=n {
        let processed = donor.progress().and_then(|p| p.processed).unwrap();
        let index = if late(id) && id < n {
            // Below the core's block: the donor's engine takes it after
            // the core, late.
            behind += 1;
            (core.k0 - 2) * B
        } else {
            at = (processed + 40) * B;
            at
        };
        let record = mixer(index, 0.5 + (id % 4) as f32 / 10.0, -((id % 7) as f32));
        donor_audio.appended(id, index, &record.entries);
        donor_audio.committed(id);
        log.push((id, record));
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(donor_audio.sample().entries_refused, 0);
    if behind > 0 {
        wait_for("the donor took the late entries", || {
            donor.counters().command_late.load(Ordering::Relaxed) - late_before >= behind
        });
    }
    let r = joiner_audio.resync(&core, &Log(log.clone()))?;
    // Held unresolved, as the donor holds an entry it took late.
    let taken_late: Vec<u64> = (log.iter())
        .filter(|(_, r)| r.at_index <= core.k0 * B)
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(taken_late.len() as u64, behind);
    for id in taken_late {
        assert!(r.unresolved.iter().any(|u| u.0 == id), "{id} unresolved");
    }
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
        flows: vec![Some(ROUTED.to_owned())],
        outputs: Vec::new(),
        strips: vec![vec![StripParam::Trim { db: -6.0 }]],
        ducks: Vec::new(),
        masks: Vec::new(),
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

/// Two input slots in two input domains, `a` and `b`, into one output of
/// domain `a`; no flow is written.
fn two_domains(d0: &Domain, d1: &Domain, output: &str) -> serde_json::Value {
    serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "a", "path": d0.0 }, { "name": "b", "path": d1.0 }],
        "inputs": [
            { "name": "in0", "domain": "a", "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd5a00", "channels": 1 },
            { "name": "in1", "domain": "b", "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd5b00", "channels": 1 },
        ],
        "outputs": [{ "name": "out0", "domain": "a", "flowId": output,
                      "label": "domains", "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0], [1.0]],
        "rt": { "rtCpu": "none" },
    })
}

/// A re-route of both slots, of two domains, as a command flagged `None`
/// derives during a rollout: a live engine keeps the lower slot's route
/// and leaves the other out. A replica rebuilt from the product state,
/// which holds both, reads what the live one reads, and a gain the
/// engine refuses is silenced in the rebuild rather than refusing it.
#[test]
fn a_rebuilt_replica_reads_what_a_cross_domain_reroute_left_live() {
    let (d0, d1) = (Domain::new(), Domain::new());
    let (live, live_audio) = bridge_of(
        &two_domains(&d0, &d1, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f30"),
        1 << 12,
    );
    let (rebuilt, rebuilt_audio) = bridge_of(
        &two_domains(&d0, &d1, "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f31"),
        1 << 12,
    );
    let processed = |e: &Engine| e.progress().and_then(|p| p.processed);
    wait_for("both compute", || {
        processed(&live).is_some() && processed(&rebuilt).is_some()
    });
    let routed = [
        "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd5a01",
        "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd5b01",
    ];
    let reroute: Vec<Entry> = (0..2)
        .map(|slot| Entry::Route {
            slot,
            flow_id: routed[slot].to_owned(),
        })
        .collect();
    let at = (processed(&live).unwrap() + 100) * B;
    live_audio.appended(1, at, &reroute);
    live_audio.committed(1);
    let flows =
        |e: &Engine| -> Vec<String> { e.status().inputs.into_iter().map(|i| i.flow_id).collect() };
    wait_for("slot 0 re-routed", || flows(&live)[0] == routed[0]);
    assert_eq!(live_audio.sample().entries_filtered, 1);
    let mut state = samplez::ProductState {
        gains: vec![vec![1.0], vec![1.0]],
        up: vec![true, true],
        flows: routed.map(|f| Some(f.to_owned())).to_vec(),
        outputs: Vec::new(),
        strips: vec![Vec::new(), Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        pending: Vec::new(),
        last_id: 1,
    };
    rebuilt_audio.drop_engine();
    assert!(rebuilt_audio.rebuild_engine(&state, 0), "rebuilt");
    assert_eq!(
        flows(&rebuilt),
        flows(&live),
        "the flows the live one reads"
    );
    // A live replica keeps the previous gain of a cell it refuses; the
    // product state holds no previous one, so the rebuild silences it.
    state.gains[1][0] = f32::INFINITY;
    state.last_id = 2;
    assert!(rebuilt_audio.rebuild_engine(&state, 0), "rebuilt again");
    // Past the gains' fade-in.
    std::thread::sleep(Duration::from_millis(100));
    let core = loop {
        match rebuilt.copy_core(Duration::from_secs(2)) {
            Err(samplez::CopyError::Passed { .. }) => continue,
            c => break c.unwrap(),
        }
    };
    assert_eq!((core.core.gain(0, 0), core.core.gain(1, 0)), (1.0, 0.0));
    // A duck setting or mask the engine refuses is left at what no entry
    // sets, the others taken: nine keys, and a mask on a column the
    // configuration names no duck column.
    state.ducks = vec![samplez::DuckTargets {
        keys: vec![(0, 1.0); 9],
        mode: samplez_dsp::DuckMode::On,
        depths: vec![12.0],
        ..samplez::DuckTargets::default()
    }];
    state.masks = vec![vec![1], vec![0]];
    state.last_id = 3;
    assert!(
        rebuilt_audio.rebuild_engine(&state, 0),
        "rebuilt with ducks"
    );
    let core = loop {
        match rebuilt.copy_core(Duration::from_secs(2)) {
            Err(samplez::CopyError::Passed { .. }) => continue,
            c => break c.unwrap(),
        }
    };
    let d = core.core.detector(0).unwrap();
    assert_eq!((d.key(0), d.mode()), (None, samplez_dsp::DuckMode::On));
    assert_eq!(core.core.duck_depth(0, 0), 12.0);
    assert_eq!(core.core.duck_mask(0, 0), 0);
}

/// A rebuild offered for a floor further ahead than the rebuild's own
/// wait, as continuous writes keep it with a `maxApplyLead` near 100 ms
/// and a generation's build ahead of that, waits for the floor and
/// installs: refused, the replica would never rebuild under writes.
#[test]
fn a_rebuild_waits_for_a_floor_the_lead_and_a_build_ahead() {
    let domain = Domain::new();
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "in0", "domain": "d",
                     "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a", "channels": 1 }],
        "outputs": [{ "name": "out0", "domain": "d", "flowId": "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f32",
                      "label": "bridge", "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "rtCpu": "none" },
    });
    let timing = Timing {
        margin: 960,
        max_apply_lead: Duration::from_millis(100),
        ..Timing::defaults(B)
    };
    let (engine, audio) = bridge_timed(&v, 1 << 12, &timing);
    wait_for("a block computed", || {
        engine.progress().unwrap().processed.is_some()
    });
    let state = samplez::ProductState {
        gains: vec![vec![0.5]],
        up: vec![true],
        flows: vec![Some("5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a".to_owned())],
        outputs: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        pending: Vec::new(),
        last_id: 1,
    };
    audio.drop_engine();
    // 180 ms ahead: past the 100 ms a rebuild waits beyond the lead.
    let now = (engine.progress().unwrap().processed.unwrap() + 1) * B;
    let floor = (now + 180 * 48).next_multiple_of(B);
    assert!(audio.rebuild_engine(&state, floor), "rebuilt at the floor");
    let installed = engine.progress().unwrap().processed.unwrap();
    assert!(installed * B + B >= floor, "installed at {installed}");
}

/// Two mono input slots reading one flow into one output, as a product
/// drives them: `fader <slot> <gain>`, `vca <gain>`, `gate <slot> <dB>`
/// for the gate threshold, `recall <gain>` setting both faders.
#[derive(serde::Serialize, serde::Deserialize)]
struct Console {
    faders: [f32; 2],
    vca: f32,
    gates: [f32; 2],
    flow: String,
}

impl Console {
    fn new(flow: &str) -> Console {
        Console {
            faders: [1.0, 1.0],
            vca: 1.0,
            gates: [-60.0, -60.0],
            flow: flow.to_owned(),
        }
    }
}

impl Product for Console {
    type Change = ();
    const DRIVES_ENGINE: bool = true;

    fn apply(&mut self, _: &Origin, command: &[u8], _: &Apply) -> Effect<()> {
        let text = std::str::from_utf8(command).expect("utf-8");
        let number = |s: &str| s.parse::<f32>().unwrap();
        match text.split(' ').collect::<Vec<_>>()[..] {
            ["fader", slot, gain] => self.faders[slot.parse::<usize>().unwrap()] = number(gain),
            ["vca", gain] => self.vca = number(gain),
            ["gate", slot, db] => self.gates[slot.parse::<usize>().unwrap()] = number(db),
            ["recall", gain] => self.faders = [number(gain), number(gain) / 2.0],
            _ => return Effect::Stale,
        }
        Effect::Changed(())
    }
    fn expire(&mut self, _: &SessionKey) -> Effect<()> {
        Effect::Unchanged
    }
    fn forget(&mut self, _: &SessionKey) {}
    fn delta(&self, _: u64, _: &[Changed<'_, ()>]) -> String {
        String::new()
    }
    fn text(&self) -> String {
        String::new()
    }
    fn save(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("encodes")
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        *self = serde_json::from_slice(saved).map_err(|e| e.to_string())?;
        Ok(())
    }
    fn config_hash(&self) -> u64 {
        0
    }
    fn targets(&self) -> Option<Targets> {
        let gate = |slot: usize| {
            vec![StripParam::Gate(GateParams {
                enabled: true,
                threshold_db: self.gates[slot],
                range_db: 30.0,
                attack_ms: 1.0,
                release_ms: 200.0,
            })]
        };
        Some(Targets {
            gains: (0..2).map(|s| vec![self.faders[s] * self.vca]).collect(),
            flows: vec![Some(self.flow.clone()); 2],
            strips: (0..2).map(gate).collect(),
            ..Targets::default()
        })
    }
}

/// An engine of [`Console`]'s shape reading `input`, writing `output`.
fn console(domain: &Domain, input: &str, output: &str) -> (Arc<Engine>, EngineAudio) {
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": 960,
        "libmxl": libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": (0..2).map(|n| serde_json::json!({
            "name": format!("in{n}"), "domain": "d", "flowId": input, "channels": 1 }))
            .collect::<Vec<_>>(),
        "outputs": [{ "name": "out0", "domain": "d", "flowId": output, "label": "console",
                      "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
        "matrix": [[1.0], [1.0]],
        "rt": { "rtCpu": "none" },
    });
    bridge_of(&v, 1 << 12)
}

/// A machine applying a log of [`Console`] commands, one per entry.
struct Machine(StateMachine<Console>);

impl Machine {
    fn new(flow: &str) -> Machine {
        Machine(StateMachine::new(
            Console::new(flow),
            Limits::defaults(48_000),
        ))
    }

    /// Applies entry `index`, command `body` stamped `stamp` asking for
    /// `at`: the record it derived.
    fn apply(&mut self, index: u64, stamp: u64, at: u64, body: &str) -> Arc<Record> {
        let command = Command {
            origin: Origin {
                principal: "anonymous".into(),
                session: "s".into(),
                role: samplez_service::Role::Operator,
                connection: "r0:n:1".into(),
            },
            id: index.to_string(),
            at_index: Some(at),
            body: body.as_bytes().to_vec(),
            generation: None,
        };
        let request = Request {
            stamp,
            body: Body::Commands(vec![command]),
        };
        self.0.apply_at(index, 1, &request);
        Arc::clone(&self.0.records[&index])
    }
}

/// Collects the page records of every engine of `audios` until each has
/// processed block `end`: per engine, by page.
fn pages_until(
    engines: &[(&Engine, &EngineAudio)],
    end: u64,
) -> Result<Vec<BTreeMap<u64, Page>>, String> {
    let mut pages = vec![BTreeMap::new(); engines.len()];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for (n, (_, audio)) in engines.iter().enumerate() {
            for p in audio.sample().own.pages {
                pages[n].insert(p.page, p);
            }
        }
        let done = (engines.iter())
            .all(|(e, _)| e.progress().and_then(|p| p.processed).unwrap_or(0) > end);
        if done {
            return Ok(pages);
        }
        if Instant::now() > deadline {
            return Err("pages within 10 s".into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Pages `from..=to` of `a` and `b` equal by hash and fingerprint: the
/// number compared.
fn equal_pages(
    a: &BTreeMap<u64, Page>,
    b: &BTreeMap<u64, Page>,
    from: u64,
    to: u64,
) -> Result<usize, String> {
    let mut compared = 0;
    for p in from..=to {
        let (Some(x), Some(y)) = (a.get(&p), b.get(&p)) else {
            continue;
        };
        if (x.hash, x.fingerprint) != (y.hash, y.fingerprint) {
            return Err(format!("page {p} differs"));
        }
        compared += 1;
    }
    Ok(compared)
}

const CONSOLE_INPUT: &str = "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd6a00";

/// A domain with a second of history and a pilot on [`CONSOLE_INPUT`],
/// and two console engines reading it, past their fade-in from the
/// attach.
fn consoles(domain: &Domain) -> [(Arc<Engine>, EngineAudio); 2] {
    std::fs::write(
        domain.0.join("options.json"),
        r#"{"urn:x-mxl:option:history_duration/v1.0": 1000000000}"#,
    )
    .unwrap();
    let engines = [
        console(
            domain,
            CONSOLE_INPUT,
            "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f40",
        ),
        console(
            domain,
            CONSOLE_INPUT,
            "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f41",
        ),
    ];
    wait_for("both read the input", || {
        engines.iter().all(|(e, _)| {
            e.status()
                .inputs
                .iter()
                .all(|i| i.state == InputState::Attached)
                && e.progress().and_then(|p| p.processed).is_some()
        })
    });
    std::thread::sleep(Duration::from_millis(200));
    engines
}

fn processed_of(e: &Engine) -> u64 {
    e.progress().and_then(|p| p.processed).unwrap_or(0)
}

/// Two engines each handed the records its own machine derived from one
/// log (design test 22): a VCA move, a gate change and a recall derive
/// equal records on both machines, and the engines compute equal pages
/// from the first entry's index on.
#[test]
fn two_engines_fed_two_machines_records_compute_equal_pages() {
    undisturbed(machines_records_on_two_engines);
}

fn machines_records_on_two_engines() -> Result<(), String> {
    let domain = Domain::new();
    let _pilot = Pilot::start(&domain, CONSOLE_INPUT);
    let [(ea, aa), (eb, ab)] = consoles(&domain);
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Ordering::Relaxed);
    let silenced_before = silenced(&ea) + silenced(&eb);
    let (mut ma, mut mb) = (Machine::new(CONSOLE_INPUT), Machine::new(CONSOLE_INPUT));
    let log = ["vca 0.5", "gate 0 -20", "recall 0.8", "fader 1 0.25"];
    let (mut first, mut last) = (None, 0);
    for (n, body) in log.iter().enumerate() {
        let index = n as u64 + 1;
        let stamp = processed_of(&ea).max(processed_of(&eb)) * B;
        let at = stamp + 40 * B;
        let (ra, rb) = (
            ma.apply(index, stamp, at, body),
            mb.apply(index, stamp, at, body),
        );
        assert_eq!(ra, rb, "entry {index}: equal records");
        aa.appended(index, ra.at_index, &ra.entries);
        aa.committed(index);
        ab.appended(index, rb.at_index, &rb.entries);
        ab.committed(index);
        first.get_or_insert(ra.at_index);
        last = ra.at_index;
        std::thread::sleep(Duration::from_millis(10));
    }
    let end = (last + 480) / B + 4 * PAGE / B;
    let pages = pages_until(&[(&ea, &aa), (&eb, &ab)], end + PAGE / B)?;
    if silenced(&ea) + silenced(&eb) > silenced_before {
        return Err("an input block silenced".into());
    }
    let from = first.unwrap() / PAGE;
    let compared = equal_pages(&pages[0], &pages[1], from, end * B / PAGE)?;
    if compared < 4 {
        return Err(format!("{compared} pages compared"));
    }
    for e in [&ea, &eb] {
        assert_eq!(e.counters().command_late.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

/// How a rebuild meets the floor (design test 25).
#[derive(Clone, Copy, Debug)]
enum Floor {
    /// An entry after the state read takes effect at exactly its floor.
    At,
    /// An entry after the state read passes before the install: the
    /// install is missed, and a fresh read installs.
    Passed,
    /// Nothing written for a while: the floor lies in the past.
    Idle,
}

/// A core rebuilt from a machine's product state at its floor, with the
/// records after it handed on top while fader writes go on, computes the
/// live engine's pages once the ramps of the install and of the last
/// write ended.
#[test]
fn a_core_rebuilt_at_the_floor_computes_the_live_pages() {
    for case in [Floor::At, Floor::Passed, Floor::Idle] {
        undisturbed(|| rebuild_at_the_floor(case));
    }
}

fn rebuild_at_the_floor(case: Floor) -> Result<(), String> {
    let domain = Domain::new();
    let _pilot = Pilot::start(&domain, CONSOLE_INPUT);
    let [(live, la), (rebuilt, ra)] = consoles(&domain);
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Ordering::Relaxed);
    let silenced_before = silenced(&live) + silenced(&rebuilt);
    let mut m = Machine::new(CONSOLE_INPUT);
    let index = std::cell::Cell::new(0u64);
    // Entry `body` asking for `ahead` blocks past the latest block either
    // engine computed, handed to both as contact hands it.
    let write = |m: &mut Machine, body: &str, ahead: u64| {
        index.set(index.get() + 1);
        let index = index.get();
        let stamp = processed_of(&live).max(processed_of(&rebuilt)) * B;
        let r = m.apply(index, stamp, stamp + ahead * B, body);
        for audio in [&la, &ra] {
            audio.appended(index, r.at_index, &r.entries);
            audio.committed(index);
        }
        (index, r)
    };
    for n in 0..3 {
        write(&mut m, &format!("fader 0 0.{}", n + 3), 40);
    }
    ra.drop_engine();
    match case {
        Floor::At => {
            write(&mut m, "fader 1 0.7", 40);
            let (n, floor) = (index.get(), m.0.floor);
            let state = m.0.product_state(n);
            let (_, r) = write(&mut m, "fader 0 0.2", 5);
            assert_eq!(r.at_index, floor, "at exactly the floor");
            if !ra.rebuild_engine(&state, floor) {
                return Err("not rebuilt".into());
            }
        }
        Floor::Passed => {
            write(&mut m, "fader 1 0.7", 40);
            let (n, floor) = (index.get(), m.0.floor);
            let stale = m.0.product_state(n);
            write(&mut m, "fader 0 0.2", 5);
            wait_for("its index passed", || {
                processed_of(&rebuilt) > m.0.floor / B + 2
            });
            assert!(
                !ra.rebuild_engine(&stale, floor),
                "an entry it lacks passed"
            );
            let fresh = m.0.product_state(index.get());
            if !ra.rebuild_engine(&fresh, m.0.floor) {
                return Err("not rebuilt from a fresh read".into());
            }
        }
        Floor::Idle => {
            wait_for("the floor passed", || {
                processed_of(&rebuilt) > m.0.floor / B + 20
            });
            let state = m.0.product_state(index.get());
            if !ra.rebuild_engine(&state, m.0.floor) {
                return Err("not rebuilt".into());
            }
        }
    }
    let installed = processed_of(&rebuilt);
    let mut last = 0;
    for n in 0..10 {
        let (_, r) = write(&mut m, &format!("fader {} 0.{}", n % 2, n + 1), 40);
        last = r.at_index;
        std::thread::sleep(Duration::from_millis(5));
    }
    let from = ((installed + 1) * B + 2 * 480).div_ceil(PAGE) + 2;
    let end = (last + 480) / B + 4 * PAGE / B;
    let pages = pages_until(&[(&live, &la), (&rebuilt, &ra)], end + PAGE / B)?;
    if silenced(&live) + silenced(&rebuilt) > silenced_before {
        return Err("an input block silenced".into());
    }
    let compared = equal_pages(&pages[0], &pages[1], from, end * B / PAGE)?;
    if compared < 4 {
        return Err(format!("{case:?}: {compared} pages compared"));
    }
    assert_eq!(rebuilt.counters().command_late.load(Ordering::Relaxed), 0);
    Ok(())
}

/// A command giving a gain the engine refuses leaves a live engine handed
/// the records and one rebuilt from the product state past it computing
/// the same pages: both silence the cell, where the live one would hold
/// its previous gain and the rebuilt one, from a state holding no previous
/// gain, silence it.
#[test]
fn a_refused_gain_leaves_a_live_and_a_rebuilt_engine_alike() {
    undisturbed(refused_gain_live_and_rebuilt);
}

fn refused_gain_live_and_rebuilt() -> Result<(), String> {
    let domain = Domain::new();
    let _pilot = Pilot::start(&domain, CONSOLE_INPUT);
    let [(live, la), (rebuilt, ra)] = consoles(&domain);
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Ordering::Relaxed);
    let silenced_before = silenced(&live) + silenced(&rebuilt);
    let mut m = Machine::new(CONSOLE_INPUT);
    let index = std::cell::Cell::new(0u64);
    let write = |m: &mut Machine, body: &str| {
        index.set(index.get() + 1);
        let index = index.get();
        let stamp = processed_of(&live).max(processed_of(&rebuilt)) * B;
        let r = m.apply(index, stamp, stamp + 40 * B, body);
        for audio in [&la, &ra] {
            audio.appended(index, r.at_index, &r.entries);
            audio.committed(index);
        }
        r
    };
    write(&mut m, "fader 0 0.5");
    ra.drop_engine();
    write(&mut m, "fader 0 inf");
    wait_for("the floor passed", || {
        processed_of(&rebuilt) > m.0.floor / B + 20
    });
    let state = m.0.product_state(index.get());
    if !ra.rebuild_engine(&state, m.0.floor) {
        return Err("not rebuilt".into());
    }
    let installed = processed_of(&rebuilt);
    let mut last = 0;
    for n in 0..4 {
        last = write(&mut m, &format!("fader 1 0.{}", n + 2)).at_index;
        std::thread::sleep(Duration::from_millis(5));
    }
    let from = ((installed + 1) * B + 2 * 480).div_ceil(PAGE) + 2;
    let end = (last + 480) / B + 4 * PAGE / B;
    let pages = pages_until(&[(&live, &la), (&rebuilt, &ra)], end + PAGE / B)?;
    if silenced(&live) + silenced(&rebuilt) > silenced_before {
        return Err("an input block silenced".into());
    }
    let compared = equal_pages(&pages[0], &pages[1], from, end * B / PAGE)
        .map_err(|e| format!("{e}: the live engine holds another gain"))?;
    if compared < 4 {
        return Err(format!("{compared} pages compared"));
    }
    Ok(())
}
