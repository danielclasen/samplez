//! The core a replicated engine copies after a block, for a resync.

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::{Duration, Instant};

use common::{
    B, Domain, Pilot, copy_core as copy, new_id, processed, start_held, start_held_with, wait_for,
};
use common::{MARGIN, config, diagonal};
use samplez::blocks::{PAGE, PageHash, StatusChange};
use samplez::{
    BlockVerdict, Config, CopyError, DuckSetting, DuckTargets, Engine, Entry, Fade, InputState,
    ProductState, ReplayError, Replication,
};
use samplez_dsp::{
    BandType, CompParams, DetectorParams, DuckMode, EqBand, GateParams, LimiterParams, StripParam,
};

/// Flow ids whose lookup [`slow_look`] makes wait.
static SLOW: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// The engine's lookup of a flow's directory, waiting 500 ms first for a
/// flow [`SLOW`] names, as a file system does that waits for a name.
fn slow_look(dir: &std::path::Path) -> bool {
    let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    if SLOW
        .lock()
        .unwrap()
        .iter()
        .any(|id| name.starts_with(id.as_str()))
    {
        std::thread::sleep(Duration::from_millis(500));
    }
    samplez_mxl::lookup::exists(dir)
}

/// A replay opens no input flow its lookup has not found: one whose
/// lookup does not answer within the replay's wait fails the replay at
/// once, saying so, instead of libmxl's open waiting on a file system that
/// waits for a missing name.
#[test]
fn a_replay_fails_at_once_on_a_flow_its_lookup_has_not_found() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let a = hashed(&domain, &src);
    let b = hashed_of(&domain, &[(&src, 2)], |c| c.look = slow_look);
    wait_for(Duration::from_secs(5), "both read the input", || {
        [&a.e, &b.e]
            .iter()
            .all(|e| e.status().inputs[0].state == InputState::Attached && processed(e) > 0)
    });
    let copy = copy(&a.e);
    SLOW.lock().unwrap().push(src.clone());
    let started = Instant::now();
    match b.e.replay(copy) {
        Err(ReplayError::Attach(m)) => assert!(m.contains("did not answer"), "{m}"),
        Err(e) => panic!("{e:?}"),
        Ok(_) => panic!("replayed over a flow its lookup has not found"),
    }
    let failed = started.elapsed();
    assert!(
        failed < Duration::from_millis(250),
        "failed after {failed:?}"
    );
}

/// The copy holds the state after block k0: entries due by then applied
/// with their ramps run out, later ones pending, the input's log state,
/// the unresolved entries, and the last log id taken.
#[test]
fn the_core_is_copied_after_a_block() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (e, _hashes) = start_held(&domain.0, &src, &dst, 2);
    wait_for(Duration::from_secs(5), "input attached", || {
        e.status().inputs[0].state == InputState::Attached
    });
    let p = processed(&e);
    let (near, far) = ((p + 100) * B, (p + 100_000) * B);
    let gain = |gain| Entry::Gain {
        input: 0,
        output: 0,
        gain,
        fade: None,
    };
    e.append(1, near, gain(0.25)).unwrap();
    e.append(2, near, Entry::InputDown { slot: 0 }).unwrap();
    e.append(3, far, gain(0.5)).unwrap();
    e.commit(3);
    e.unresolved(9, far).unwrap();
    // The ramp of 480 samples ends 10 blocks after its index.
    let ramped = near / B + 10;
    wait_for(Duration::from_secs(5), "ramp run out", || {
        processed(&e) > ramped
    });

    let c = copy(&e);
    assert!(c.k0 >= ramped, "k0 {} before block {ramped}", c.k0);
    assert_eq!(c.core.next_index(), Some((c.k0 + 1) * B), "after block k0");
    assert_eq!(c.last_id, 3);
    assert_eq!(c.core.gain(0, 0), 0.25);
    assert_eq!(c.core.pending().collect::<Vec<_>>(), vec![(3, far)]);
    assert!(!c.core.input_up(0));
    assert_eq!(c.core.unresolved(), &[(9, far)]);

    // A copy after a later block.
    let later = copy(&e);
    assert!(later.k0 > c.k0);
    assert_eq!(later.core.next_index(), Some((later.k0 + 1) * B));
    e.stop();
}

/// A single voter has no peer to resync: it copies nothing.
#[test]
fn a_single_voter_copies_no_core() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (e, _hashes) = start_held(&domain.0, &src, &dst, 1);
    assert!(matches!(
        e.copy_core(Duration::from_millis(100)),
        Err(CopyError::NotReplicated)
    ));
    e.stop();
}

/// A resync replay reads `resyncBudgetMs` past the read window: with more
/// than one voter an input whose flow keeps less history is refused,
/// naming the history_duration it needs. A lone voter has no donor and
/// attaches it.
#[test]
fn only_a_set_of_voters_needs_the_replay_history() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    // A second of budget: with the read window, more than the 1 s of
    // history the test domain keeps.
    let budget = serde_json::json!({ "resyncBudgetMs": 1000 });
    let (two, _) = start_held_with(&domain.0, &src, &new_id(), 2, budget.clone());
    wait_for(Duration::from_secs(5), "attach refused", || {
        two.status().inputs[0]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("history_duration of at least"))
    });
    assert_eq!(two.status().inputs[0].state, InputState::Pruned);
    two.stop();
    let (one, _) = start_held_with(&domain.0, &src, &new_id(), 1, budget);
    wait_for(Duration::from_secs(5), "input attached", || {
        one.status().inputs[0].state == InputState::Attached
    });
    one.stop();
}

/// A replicated engine of two voters, a two-channel input through gain 1
/// to a two-channel output, with its page hashes and block verdicts.
struct Hashed {
    e: Engine,
    hashes: Receiver<PageHash>,
    verdicts: Receiver<BlockVerdict>,
    statuses: Receiver<StatusChange>,
    /// The flow id of output 0.
    out: String,
}

/// Samples of a gain ramp: about two pages, so the pages a ramp runs
/// over differ from one another.
const RAMP: u32 = 2048;

/// Waits until the gains of a product core installed after block `j`
/// have ramped to their targets.
fn past_fade_in(e: &Engine, j: u64) {
    wait_for(Duration::from_secs(5), "past the fade-in", || {
        processed(e) > j + 2 + u64::from(RAMP).div_ceil(B)
    });
}

fn hashed(domain: &Domain, src: &str) -> Hashed {
    hashed_with(domain, &[(src, 2)])
}

/// [`hashed`] over the inputs `inputs`, as (flow, channels), one output
/// channel per input channel.
fn hashed_with(domain: &Domain, inputs: &[(&str, u32)]) -> Hashed {
    hashed_of(domain, inputs, |_| {})
}

/// [`hashed_with`], its configuration changed by `f`.
fn hashed_of(domain: &Domain, inputs: &[(&str, u32)], f: impl FnOnce(&mut Config)) -> Hashed {
    let channels = inputs.iter().map(|i| i.1).sum::<u32>();
    let mut config = config(
        &domain.0,
        inputs,
        &[(&new_id(), channels)],
        diagonal(channels as usize, 1.0),
        MARGIN,
    );
    config.ramp_samples = RAMP;
    f(&mut config);
    let out = config.outputs[0].flow_id.clone();
    let (report_tx, _reports) = sync_channel(64);
    let (hash_tx, hashes) = sync_channel(1 << 16);
    let (verdict_tx, verdicts) = sync_channel(1 << 16);
    let (status_tx, statuses) = sync_channel(1 << 16);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let e = Engine::start_replicated(config, process, Some(verdict_tx), replication).unwrap();
    wait_for(Duration::from_secs(5), "attached and realised", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    Hashed {
        e,
        hashes,
        verdicts,
        statuses,
        out,
    }
}

impl Hashed {
    fn pages(&self) -> BTreeMap<u64, PageHash> {
        self.hashes.try_iter().map(|h| (h.page, h)).collect()
    }

    /// Samples the engine skipped after an overrun. A stateful unit such
    /// as a limiter restarts after a skip, so its output differs from an
    /// engine's that computed those samples: a run that skipped is
    /// disturbed.
    fn skipped(&self) -> u64 {
        self.e.counters().overrun_skipped_samples.load(Relaxed)
    }

    /// Takes what the engine reported since the last call into `seen`:
    /// the blocks whose input was silenced, the newest block a verdict
    /// named and the blocks an overrun skip cut.
    fn observe(&self, seen: &mut Seen) {
        for v in self.verdicts.try_iter() {
            seen.newest = seen.newest.max(v.k);
            if v.silenced != 0 {
                seen.silenced.push(v.k);
            }
        }
        for s in self.statuses.try_iter() {
            if let StatusChange::Skipped { from, to } = s {
                seen.skipped.push(from..to);
                seen.skipped_samples += (to - from) * B;
            }
        }
    }

    /// The pages a silenced input block and the fade-in after it touch:
    /// their output depends on more than the input and the log.
    fn disturbed(&self) -> Vec<u64> {
        let silenced: Vec<u64> = self
            .verdicts
            .try_iter()
            .filter(|v| v.silenced != 0)
            .map(|v| v.k)
            .collect();
        touched(&silenced)
    }
}

/// Samples a limiter with a 1 ms release takes after a restart to hold
/// the bits of one that did not: 64 for its delay and window, and 14 time
/// constants of 48 samples to snap its released gain onto the held one.
/// Rounded up.
const LIMITER_MEMORY: u64 = 1024;

/// What an engine reported about its blocks, from [`Hashed::observe`].
#[derive(Default)]
struct Seen {
    silenced: Vec<u64>,
    /// Blocks the newest verdict named, 0 before the first.
    newest: u64,
    skipped: Vec<std::ops::Range<u64>>,
    skipped_samples: u64,
}

impl Seen {
    /// Whether a silenced or skipped block, or what follows it, touches
    /// page `p`. A silenced input fades in over `RAMP` samples after it,
    /// and a limiter restarted by a skip or fed the fade-in holds the
    /// bits of one that was not once its released gain snaps onto the
    /// held gain: `LIMITER_MEMORY` samples on.
    fn touches(&self, p: u64) -> bool {
        let reach =
            |lo: u64, hi: u64, tail: u64| lo * B / PAGE <= p && p < (hi * B + tail).div_ceil(PAGE);
        self.silenced
            .iter()
            .any(|&k| reach(k, k + 1, u64::from(RAMP) + LIMITER_MEMORY))
            || (self.skipped.iter()).any(|r| reach(r.start, r.end, LIMITER_MEMORY))
    }
}

/// The pages blocks `ks` and the fade-in after each touch.
fn touched(ks: &[u64]) -> Vec<u64> {
    ks.iter()
        .flat_map(|k| k * B / PAGE..((k + 1) * B + u64::from(RAMP)).div_ceil(PAGE))
        .collect()
}

/// Hand-overs refused because the RT thread had passed their block
/// before one landed: each attempt runs the replay up to the block the
/// RT thread just computed and passes it on before the next wake, about
/// one block period later, so only a supervisor thread held off the CPU
/// for that long misses. A run with more runs again.
const MAX_PASSED: u64 = 10;

fn gain(e: &Engine, id: u64, at: u64, channel: usize, gain: f32) {
    let entry = Entry::Gain {
        input: channel,
        output: channel,
        gain,
        fade: None,
    };
    e.append(id, at, entry).unwrap();
    e.commit(id);
}

fn now_block() -> u64 {
    let api = common::api();
    samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api)) / B
}

/// The resync row on the engine: a joiner whose state diverged from a
/// donor's replays the donor's core from past input, and every page the
/// replay computes whole equals the donor's, an entry ramping in it; the
/// joiner's RT thread takes the replayed state at a block boundary and
/// from there computes the donor's pages, an entry the replay never saw
/// included, where before it computed others.
#[test]
fn a_replayed_core_computes_the_donors_pages() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        // Entry 1 diverges the joiner: gain 0.5 there, 1 on the donor.
        let at1 = (now_block() + 20) * B;
        gain(&a.e, 1, at1, 0, 1.0);
        gain(&b.e, 1, at1, 0, 0.5);
        let ramped = (at1 + u64::from(RAMP)) / B + 3 * PAGE / B;
        wait_for(Duration::from_secs(5), "diverged", || {
            processed(&a.e) > ramped && processed(&b.e) > ramped
        });
        // Entry 2 ramps within the replay, entry 3 after the hand-over.
        let at2 = (now_block() + 40) * B;
        gain(&a.e, 2, at2, 1, 0.25);
        gain(&b.e, 2, at2, 1, 0.25);
        // An entry the donor holds unresolved and the joiner does not.
        a.e.unresolved(9, at2 + 100_000 * B).unwrap();
        let copy = copy(&a.e);
        if (copy.k0 + 1) * B > at2 {
            return Err(format!("copied after block {}, past entry 2", copy.k0));
        }
        let k0 = copy.k0;
        // As a joiner takes it from the donor: encoded.
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        replay
            .append(
                2,
                at2,
                &Entry::Gain {
                    input: 1,
                    output: 1,
                    gain: 0.25,
                    fade: None,
                },
            )
            .unwrap();
        let at3 = (now_block() + 1000) * B;
        gain(&a.e, 3, at3, 0, 0.75);
        gain(&b.e, 3, at3, 0, 0.75);
        // Past entry 2's ramp before the hand-over: the newest block due
        // is the reader margin behind the clock.
        let past2 = (at2 + u64::from(RAMP)) / B + PAGE / B;
        while now_block() <= past2 + MARGIN / B + 2 {
            std::thread::sleep(Duration::from_millis(5));
            replay.run_to(now_block().saturating_sub(MARGIN / B + 2));
        }
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        assert!(j > past2, "handed over at block {j}");
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        let end = (at3 + u64::from(RAMP)) / B + 4 * PAGE / B;
        wait_for(Duration::from_secs(10), "entry 3 ramped", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        assert_eq!(b.e.counters().handovers_installed.load(Relaxed), 1);
        // The joiner inherited it, and its decision reaches the RT thread:
        // a copy after the next wake's control drain no longer holds it.
        b.e.resolved(9).unwrap();
        wait_for(Duration::from_secs(5), "entry 9 resolved", || {
            common::copy_core(&b.e).core.unresolved().is_empty()
        });

        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a
            .disturbed()
            .into_iter()
            .chain(b.disturbed())
            .chain(touched(&replay.silenced()))
            .collect();
        let clean = |p: &u64| !disturbed.contains(p);
        // The replay against the donor, on every page both computed.
        let replayed = replay.pages();
        let mut compared = 0;
        for h in replayed.iter().filter(|h| clean(&h.page)) {
            let Some(d) = pa.get(&h.page) else { continue };
            assert_eq!(
                (h.hash, h.fingerprint),
                (d.hash, d.fingerprint),
                "replayed page {}",
                h.page
            );
            compared += 1;
        }
        if compared < 2
            || !replayed
                .iter()
                .any(|h| h.page == at2 / PAGE && clean(&h.page))
        {
            return Err(format!(
                "{compared} replayed pages compared, entry 2's page among them?"
            ));
        }
        assert!(replayed.iter().all(|h| h.page * PAGE >= (k0 + 1) * B));
        // Neither the replay nor the joiner hashes a page it computed only
        // in part: the one holding block k0 + 1 and the one holding j + 1.
        let (first, boundary) = ((k0 + 1) * B, (j + 1) * B);
        assert!(first % PAGE == 0 || replayed.iter().all(|h| h.page != first / PAGE));
        assert!(boundary % PAGE == 0 || !pb.contains_key(&(boundary / PAGE)));
        // The joiner against the donor, before and after the hand-over.
        let after = |p: u64| p * PAGE >= (j + 1) * B;
        let (mut equal, mut differ) = (0, 0);
        for (p, hb) in pb
            .iter()
            .filter(|(p, _)| clean(p) && **p > ramped * B / PAGE)
        {
            let Some(ha) = pa.get(p) else { continue };
            if after(*p) {
                assert_eq!(
                    (hb.hash, hb.fingerprint),
                    (ha.hash, ha.fingerprint),
                    "page {p}"
                );
                equal += 1;
            } else if hb.hash != ha.hash {
                differ += 1;
            }
        }
        if equal < 20 || !pb.contains_key(&(at3 / PAGE)) || !clean(&(at3 / PAGE)) {
            return Err(format!("{equal} pages after the hand-over compared"));
        }
        assert!(differ > 0, "the joiner never diverged");
        Ok(())
    });
}

/// Sets strip parameter `param` of input slot 0 from `at`.
fn strip(e: &Engine, id: u64, at: u64, param: StripParam) {
    e.append(id, at, Entry::Strip { slot: 0, param }).unwrap();
    e.commit(id);
}

/// A high-pass, a bell, a gate and a compressor whose release is
/// `release_ms`, its threshold `threshold_db`.
fn strips(release_ms: f32, threshold_db: f32) -> [StripParam; 5] {
    let bell = EqBand {
        kind: BandType::Peak,
        freq: 1100.0,
        gain_db: 9.0,
        q: 2.0,
    };
    [
        StripParam::Hpf {
            enabled: true,
            freq: 120.0,
        },
        StripParam::Eq { enabled: true },
        StripParam::EqBand {
            band: 1,
            value: bell,
        },
        StripParam::Gate(GateParams {
            enabled: true,
            threshold_db: -40.0,
            range_db: 30.0,
            attack_ms: 0.5,
            release_ms: 300.0,
        }),
        StripParam::Comp(CompParams {
            enabled: true,
            threshold_db,
            ratio: 6.0,
            attack_ms: 2.0,
            release_ms,
            knee_db: 6.0,
            makeup_db: 4.0,
        }),
    ]
}

/// The resync row with strips active: a joiner whose strips diverged
/// replays a donor's core copied while the donor's compressor releases
/// over the longest time constant after a 20 dB drop, and from the
/// hand-over on computes the donor's pages bit for bit, as the replay
/// does before it. The core carries each strip's filter and dynamics
/// state: a joiner keeping its own would differ from the donor until
/// both envelopes settled, and with strips set apart, for good.
#[test]
fn a_replayed_core_carries_the_strips_mid_envelope() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        // Entries 1 to 5 set the strips, apart on the joiner.
        let at1 = (now_block() + 20) * B;
        let (donor, joiner) = (strips(5000.0, -30.0), strips(50.0, -20.0));
        for (id, (pa, pb)) in (1..).zip(donor.into_iter().zip(joiner)) {
            strip(&a.e, id, at1, pa);
            strip(&b.e, id, at1, pb);
        }
        // Entry 6 drops the level once the compressor settled: the
        // donor's releases over 5 s from there.
        let at6 = at1 + 300 * B;
        let drop = StripParam::Trim { db: -20.0 };
        strip(&a.e, 6, at6, drop);
        strip(&b.e, 6, at6, drop);
        let ramped = (at6 + u64::from(RAMP)) / B + PAGE / B;
        wait_for(Duration::from_secs(5), "level dropped", || {
            processed(&a.e) > ramped && processed(&b.e) > ramped
        });
        // Skips from before the copy on: the donor's state the copy
        // holds, and the replay, must not be cut by one.
        let skips = (a.skipped(), b.skipped());
        let copy = copy(&a.e);
        let k0 = copy.k0;
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        // Within a second of the drop: the release has seconds to run.
        if (j + 1) * B > at6 + 48_000 {
            return Err(format!("handed over at block {j}, past the release"));
        }
        // Before the hand-over the strips set apart differ; from it on
        // the joiner computes the donor's pages.
        let mut pair = Pair::new(&a, &b);
        let before = pair.comparable((k0 * B / PAGE).saturating_sub(4)..k0 * B / PAGE);
        if !before.iter().any(|&(_, same)| !same) {
            return Err("no page before the copy shows the joiner apart".to_owned());
        }
        let from = ((j + 1) * B).div_ceil(PAGE);
        let end = (at6 + 48_000) / PAGE + 8;
        // A skip restarts one engine's strip state (the engines' own
        // counters say so): pages read while neither has skipped since
        // the copy are the ones that must agree.
        let mut pages = std::collections::BTreeMap::new();
        wait_for(Duration::from_secs(10), "comparable pages", || {
            let seen = pair.comparable(from..end);
            if (a.skipped(), b.skipped()) == skips {
                pages.extend(seen);
            }
            !pages.is_empty() || processed(&a.e).min(processed(&b.e)) * B / PAGE >= end + 2
        });
        if pages.is_empty() {
            return Err(format!("{} comparable pages before a skip", pages.len()));
        }
        let apart: Vec<_> = pages.iter().filter(|(_, same)| !**same).collect();
        assert!(
            apart.is_empty(),
            "pages {apart:?} differ after the hand-over"
        );
        Ok(())
    });
}

/// The resync row with a limiter releasing: a joiner whose limiter was
/// set apart replays a donor's core copied while the donor's limiter
/// recovers over a second after its input dropped below the ceiling,
/// and from the hand-over on computes the donor's pages bit for bit. The
/// core carries the limiter's parameters, rings and released gain: a
/// joiner keeping its own, released at once, would differ for the rest
/// of the donor's release, and with its parameters set apart, for good.
#[test]
fn a_replayed_core_carries_the_limiter_mid_release() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let limited = |c: &mut Config| {
            c.outputs[0].limiter = true;
            c.matrix = diagonal(2, 4.0);
        };
        let (a, b) = (
            hashed_of(&domain, &[(&src, 2)], limited),
            hashed_of(&domain, &[(&src, 2)], limited),
        );
        let limiter = |release_ms| LimiterParams {
            enabled: true,
            ceiling_db: -6.0,
            release_ms,
        };
        let skipped = (a.skipped(), b.skipped());
        // Entry 1 sets the limiters, apart on the joiner.
        let at1 = (now_block() + 20) * B;
        let set = |e: &Engine, params| {
            e.append(1, at1, Entry::Limiter { output: 0, params })
                .unwrap();
            e.commit(1);
        };
        set(&a.e, limiter(1000.0));
        set(&b.e, limiter(5.0));
        // Entry 2 drops the level below the ceiling once the limiters
        // settled: the donor's gain recovers over 1 s from there.
        let at2 = at1 + 100 * B;
        for e in [&a.e, &b.e] {
            for channel in 0..2 {
                let entry = Entry::Gain {
                    input: channel,
                    output: channel,
                    gain: 0.25,
                    fade: None,
                };
                e.append(2 + channel as u64, at2, entry).unwrap();
            }
            e.commit(3);
        }
        let ramped = (at2 + u64::from(RAMP)) / B + PAGE / B;
        wait_for(Duration::from_secs(5), "level dropped", || {
            processed(&a.e) > ramped && processed(&b.e) > ramped
        });
        let copy = copy(&a.e);
        let k0 = copy.k0;
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        // Within half a second of the drop: the release has most of a
        // second to run.
        if (j + 1) * B > at2 + 24_000 {
            return Err(format!("handed over at block {j}, past the release"));
        }
        let before = (k0 * B / PAGE).saturating_sub(4)..k0 * B / PAGE;
        let from = ((j + 1) * B).div_ceil(PAGE);
        let end = (from + 8) * PAGE / B + PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        if (a.skipped(), b.skipped()) != skipped {
            return Err("an overrun skip".to_owned());
        }
        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
        let differ = before
            .filter(|p| matches!((pa.get(p), pb.get(p)), (Some(x), Some(y)) if x.hash != y.hash))
            .count();
        assert!(differ > 0, "the joiner never diverged");
        for p in from..from + 8 {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
        }
        Ok(())
    });
}

/// The resync row with a delay line: a joiner whose input was taken
/// down apart until shortly before the copy holds another history in its
/// delay line, which runs ahead of the strip: silence and a fade-in,
/// which its output plays for the delay after the input came up. The
/// core copy carries the donor's history and the hand-over writes the
/// replay's into the joiner's other rings, so from the hand-over on the
/// joiner computes the donor's pages bit for bit; keeping its own
/// history, it would differ for the rest of the delay.
#[test]
fn a_replayed_core_carries_the_delay_history() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let delayed = |c: &mut Config| c.inputs[0].max_delay_samples = 96_000;
        let (a, b) = (
            hashed_of(&domain, &[(&src, 2)], delayed),
            hashed_of(&domain, &[(&src, 2)], delayed),
        );
        // Entry 1 delays both by 2 s; entry 2 takes the joiner's input
        // down, entry 3 brings it up on both.
        let at1 = (now_block() + 20) * B;
        const D: u64 = 96_000;
        let delay = Entry::Delay {
            slot: 0,
            samples: D as u32,
        };
        for e in [&a.e, &b.e] {
            e.append(1, at1, delay.clone()).unwrap();
            e.commit(1);
        }
        strip(&a.e, 2, at1, StripParam::Trim { db: 0.0 });
        b.e.append(2, at1, Entry::InputDown { slot: 0 }).unwrap();
        b.e.commit(2);
        // Up a tenth of a second before the history played from at1 is
        // heard: the joiner's output differs from at1 + D to at3 + D and
        // the fade-in after it.
        let at3 = at1 + D - 4_800;
        for e in [&a.e, &b.e] {
            e.append(3, at3, Entry::InputUp { slot: 0 }).unwrap();
            e.commit(3);
        }
        let heard = at1 + D + 4_800;
        wait_for(Duration::from_secs(10), "the trimmed history heard", || {
            processed(&a.e) > heard / B && processed(&b.e) > heard / B
        });
        let copy = copy(&a.e);
        let k0 = copy.k0;
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        // The pages compared lie before the trimmed history has played.
        let from = ((j + 1) * B).div_ceil(PAGE);
        if (from + 8) * PAGE > at3 + D {
            return Err(format!(
                "handed over at block {j}, past the trimmed history"
            ));
        }
        let before = (k0 * B / PAGE).saturating_sub(4)..k0 * B / PAGE;
        let end = (from + 8) * PAGE / B + PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
        let differ = before
            .filter(|p| matches!((pa.get(p), pb.get(p)), (Some(x), Some(y)) if x.hash != y.hash))
            .count();
        assert!(differ > 0, "the joiner never diverged");
        for p in from..from + 8 {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
        }
        Ok(())
    });
}

/// The resync row with an output's delay line, as with an input's: a
/// joiner whose input was trimmed apart until shortly before the copy
/// holds another history in its output's line, which the output plays for
/// the delay after the trims met. The core copy carries the donor's
/// history and the hand-over writes the replay's into the joiner's other
/// rings, so from the hand-over on the joiner computes the donor's pages
/// bit for bit; keeping its own history, it would differ for the rest of
/// the delay.
#[test]
fn a_replayed_core_carries_the_output_delay_history() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let delayed = |c: &mut Config| c.outputs[0].max_delay_samples = 96_000;
        let (a, b) = (
            hashed_of(&domain, &[(&src, 2)], delayed),
            hashed_of(&domain, &[(&src, 2)], delayed),
        );
        // Entry 1 delays both by 2 s; entry 2 trims the joiner apart,
        // entry 3 brings it back.
        let at1 = (now_block() + 20) * B;
        const D: u64 = 96_000;
        let delay = Entry::OutputDelay {
            output: 0,
            samples: D as u32,
        };
        for e in [&a.e, &b.e] {
            e.append(1, at1, delay.clone()).unwrap();
            e.commit(1);
        }
        strip(&a.e, 2, at1, StripParam::Trim { db: 0.0 });
        strip(&b.e, 2, at1, StripParam::Trim { db: -6.0 });
        // The trims meet a second after the trimmed history begins to play:
        // the joiner's output differs from at1 + D to at3 + D.
        let at3 = at1 + D + 48_000;
        for e in [&a.e, &b.e] {
            strip(e, 3, at3, StripParam::Trim { db: 0.0 });
        }
        let heard = at1 + D + 4_800;
        wait_for(Duration::from_secs(10), "the trimmed history heard", || {
            processed(&a.e) > heard / B && processed(&b.e) > heard / B
        });
        let copy = copy(&a.e);
        let k0 = copy.k0;
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        // The pages compared lie before the trimmed history has played.
        let from = ((j + 1) * B).div_ceil(PAGE);
        if (from + 8) * PAGE > at3 + D {
            return Err(format!(
                "handed over at block {j}, past the trimmed history"
            ));
        }
        let before = (k0 * B / PAGE).saturating_sub(4)..k0 * B / PAGE;
        let end = (from + 8) * PAGE / B + PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
        let differ = before
            .filter(|p| matches!((pa.get(p), pb.get(p)), (Some(x), Some(y)) if x.hash != y.hash))
            .count();
        assert!(differ > 0, "the joiner never diverged");
        for p in from..from + 8 {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
        }
        Ok(())
    });
}

/// A product state's delays are set at the install, the delay lines
/// keeping their history: an input delayed in the state plays late
/// after the install. A delay beyond a slot's line is refused before
/// anything changes. A copy of the installed core names the state's
/// last log id, though the engine took no entry.
#[test]
fn a_product_state_sets_the_delay() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed_of(&domain, &[(&src, 2)], |c| {
        c.inputs[0].max_delay_samples = 4800;
    });
    let state = |delays| ProductState {
        limiters: Vec::new(),
        delays,
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: common::diagonal(2, 1.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    assert!(matches!(
        h.e.install_product(&state(vec![4801]), 0, Duration::from_secs(2)),
        Err(ReplayError::Invalid(_))
    ));
    h.e.drop_core().unwrap();
    let j = (h.e)
        .install_product(&state(vec![4800]), 0, Duration::from_secs(2))
        .unwrap();
    let k0 = loop {
        let c = copy(&h.e);
        if c.k0 > j {
            break c;
        }
    };
    // The core knows the state's entries, which it took from no ring:
    // its copy names the state's last id.
    assert_eq!(k0.last_id, 1, "the installed state's last id");
    let taps = k0.core.delay_taps(0).unwrap();
    assert_eq!((taps.from, taps.to), (0, 4800), "{taps:?}");
    assert_eq!(taps.start, (j + 1) * B);
}

/// A product state's output delays are set at the install, as an input's,
/// the line keeping its history. A delay beyond an output's line, or one
/// of an output the engine lacks, is refused before anything changes.
#[test]
fn a_product_state_sets_the_output_delay() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed_of(&domain, &[(&src, 2)], |c| {
        c.outputs[0].max_delay_samples = 4800;
    });
    let state = |output_delays| ProductState {
        gains: common::diagonal(2, 1.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        strips: vec![Vec::new()],
        output_delays,
        last_id: 1,
        ..Default::default()
    };
    let refused = |s: &ProductState| {
        matches!(
            h.e.install_product(s, 0, Duration::from_secs(2)),
            Err(ReplayError::Invalid(_))
        )
    };
    assert!(refused(&state(vec![4801])), "a delay beyond the line");
    assert!(refused(&state(vec![4800, 0])), "a second output");
    h.e.drop_core().unwrap();
    let j = (h.e)
        .install_product(&state(vec![4800]), 0, Duration::from_secs(2))
        .unwrap();
    let k0 = loop {
        let c = copy(&h.e);
        if c.k0 > j {
            break c;
        }
    };
    let taps = k0.core.output_delay_taps(0).expect("output 0's line");
    assert_eq!((taps.from, taps.to), (0, 4800), "{taps:?}");
    assert_eq!(taps.start, (j + 1) * B);
}

/// A product state's tone ramps are installed as they are: a generator
/// rebuilt mid level ramp, mid frequency crossfade with a change waiting
/// behind it and mid ident switch computes, from the install on, the bits
/// of a generator that took the same changes unbroken, per leg. Its ramp
/// is two seconds, so the install lands inside it. Fails with the held
/// target taking the ramps' place in `install_product`
/// (`ramps.or(held)?` replaced by `held.or(ramps)?`).
#[test]
fn a_product_state_resumes_the_tone_ramps() {
    const TONE_RAMP: u32 = 96_000;
    let domain = Domain::new();
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": MARGIN,
        "rampSamples": TONE_RAMP,
        "libmxl": common::libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [{ "name": "tone", "tone": {}, "channels": 2 }],
        "outputs": [{ "name": "out", "domain": "d", "flowId": new_id(), "label": "tone",
                      "group": "samplez-test", "role": "Audio", "channels": 2 }],
        "matrix": [[1.0, 0.0], [0.0, 1.0]],
        "rt": { "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (report_tx, _reports) = sync_channel(64);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: None,
        hashes: None,
    };
    let e = Engine::start_replicated(config, process, None, replication).unwrap();
    wait_for(Duration::from_secs(5), "realised", || {
        e.status().outputs[0].realised
    });
    // The changes, at absolute indices before the install: the level and
    // ident ramping from `base`, the level again from the second, the
    // frequency crossfading to 440 Hz with 1 kHz waiting behind it.
    let base = (now_block() + 10) * B;
    let p = |frequency_hz, level_db, ident| samplez_dsp::ToneParams {
        frequency_hz,
        level_db,
        ident,
    };
    let changes = [
        (base, p(440, -6.0, true)),
        (base + 10 * B, p(1000, -3.0, true)),
    ];
    let mut ramps = samplez_dsp::ToneRamps::default();
    for &(at, params) in &changes {
        assert!(ramps.set(at, params, TONE_RAMP));
    }
    assert_eq!(ramps.frequency.next, Some(1000), "a change waiting");
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: vec![Some(changes[1].1)],
        tone_ramps: vec![Some(ramps)],
        gains: diagonal(2, 1.0),
        up: vec![true],
        flows: vec![None],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let j = e
        .install_product(&state, base + 100 * B, Duration::from_secs(2))
        .unwrap();
    let from = (j + 1) * B;
    let to = from + 9600;
    assert!(
        to < base + u64::from(TONE_RAMP),
        "installed at {from}, past the ramps from {base}"
    );
    let c = loop {
        let c = copy(&e);
        if c.k0 > j {
            break c;
        }
    };
    // A generator that took the changes unbroken, from `base` on, per leg.
    let mut unbroken = samplez_dsp::Tone::new(2, 48_000, TONE_RAMP);
    let mut want = [Vec::new(), Vec::new()];
    let ends = [changes[1].0, to];
    for (&(at, params), &end) in changes.iter().zip(&ends) {
        assert!(unbroken.set(at, params));
        let frames = (end - at) as usize;
        let mut buf = vec![0.0; 2 * frames];
        unbroken.process(at, &mut buf, frames);
        for (leg, w) in want.iter_mut().enumerate() {
            w.extend_from_slice(&buf[leg * frames..(leg + 1) * frames]);
        }
    }
    // The installed generator, its ramps functions of the index, from the
    // install's first sample on.
    let mut installed = *c.core.tone(0).expect("a tone generator in the core");
    let frames = (to - from) as usize;
    let mut got = vec![0.0; 2 * frames];
    installed.process(from, &mut got, frames);
    let skip = (from - base) as usize;
    for (leg, w) in want.iter().enumerate() {
        for i in 0..frames {
            let (x, y) = (got[leg * frames + i], w[skip + i]);
            let at = from + i as u64;
            assert_eq!(x.to_bits(), y.to_bits(), "leg {leg} at {at}");
        }
    }
    e.stop();
}

/// An entry the replay applied before the hand-over, which reaches the
/// joiner's RT thread only after it, is not taken again: its index
/// passed, it would count late and degrade blocks the donor computed
/// clean. The replay never computes a block whose input may still be
/// unwritten.
#[test]
fn an_entry_the_replay_applied_is_not_taken_again() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        let mut replay = b.e.replay(copy(&a.e)).unwrap();
        // The RT thread computes a block once its input is in, before it
        // is due: the copy can follow a block not due yet.
        let start = replay.next_block();
        replay.run_to(now_block() + 100);
        let due = now_block() - MARGIN / B;
        assert!(
            replay.next_block() <= start.max(due),
            "the replay ran to block {}, {due} due",
            replay.next_block()
        );
        let at = (now_block() + 20) * B;
        gain(&a.e, 1, at, 0, 0.5);
        let entry = Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: None,
        };
        replay.append(1, at, &entry).unwrap();
        let past = (at + u64::from(RAMP)) / B + PAGE / B;
        while now_block() <= past + MARGIN / B + 2 {
            std::thread::sleep(Duration::from_millis(5));
            replay.run_to(now_block());
        }
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        assert!(j * B > at, "handed over at block {j}");
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        let late = b.e.counters().command_late.load(Relaxed);
        gain(&b.e, 1, at, 0, 0.5);
        let end = j + 4 * PAGE / B;
        wait_for(Duration::from_secs(5), "pages after the hand-over", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        assert_eq!(b.e.counters().command_late.load(Relaxed), late);

        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
        let mut equal = 0;
        for (p, hb) in pb
            .iter()
            .filter(|(p, _)| *p * PAGE >= (j + 1) * B && !disturbed.contains(p))
        {
            let Some(ha) = pa.get(p) else { continue };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
            equal += 1;
        }
        if equal < 2 {
            return Err(format!("{equal} pages after the hand-over compared"));
        }
        Ok(())
    });
}

/// A core missing an entry the joiner's RT thread took off its pending
/// list is refused: installed, it would drop the entry, and the joiner
/// would compute without it what the donor computed with it.
#[test]
fn a_core_missing_an_entry_the_rt_thread_took_is_refused() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let b = hashed(&domain, &src);
    let mut replay = b.e.replay(copy(&b.e)).unwrap();
    let at = (now_block() + 5) * B;
    gain(&b.e, 1, at, 0, 0.5);
    wait_for(Duration::from_secs(5), "entry 1 applied", || {
        processed(&b.e) > at / B
    });
    assert_eq!(
        b.e.hand_over(&mut replay, Duration::from_secs(2)),
        Err(ReplayError::Missed { id: 1 })
    );
    let c = b.e.counters();
    assert_eq!(c.handovers_missed.load(Relaxed), 1);
    assert_eq!(c.handovers_installed.load(Relaxed), 0);
    b.e.stop();
}

/// A replay names the blocks it computed with an input silenced: those
/// of a core older than the history the flow keeps, whose input is gone.
#[test]
fn a_replay_names_its_silenced_blocks() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let b = hashed(&domain, &src);
    let copy = copy(&b.e);
    let k0 = copy.k0;
    // The test domain's flows keep a ring of two seconds.
    std::thread::sleep(Duration::from_millis(2500));
    let mut replay = b.e.replay(copy).unwrap();
    replay.run_to(k0 + 10);
    assert_eq!(replay.silenced(), (k0 + 1..k0 + 11).collect::<Vec<_>>());
    assert_eq!(replay.silenced(), Vec::<u64>::new());
    b.e.stop();
}

/// With its core dropped the RT thread computes no block: each is
/// withheld, cause no core, the ranges sent without a gap, and none is
/// written or metered. A core built from product state is installed at a
/// block boundary: computing resumes at the block after it, never a block
/// twice, with the product's targets in place and no ramp to them.
#[test]
fn a_dropped_core_withholds_until_a_product_core_is_installed() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    h.e.drop_core().unwrap();
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 50
    });
    // The block counter moves on: a hand-over waits for its steps.
    let p = processed(&h.e);
    wait_for(Duration::from_secs(5), "blocks passed", || {
        processed(&h.e) > p + 20
    });
    // Entries whose index passed are taken off the pending list: a full
    // log takes entries again.
    let entry = Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.5,
        fade: None,
    };
    let at = (processed(&h.e) + 5) * B;
    for id in 1..=64 {
        h.e.append(id, at, entry.clone()).unwrap();
    }
    h.e.commit(64);
    wait_for(Duration::from_secs(5), "entries taken", || {
        c.entries_consumed.load(Relaxed) == 64
    });
    h.e.append(65, at, entry).unwrap();
    h.e.commit(65);
    wait_for(Duration::from_secs(5), "entry 65 taken", || {
        c.entries_without_core.load(Relaxed) == 65
    });
    // A core must know them.
    let mut state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![0.25, 0.0], vec![0.0, 0.5]],
        up: vec![true],

        flows: vec![Some(src.clone())],

        strips: vec![Vec::new()],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 0,
        ..Default::default()
    };
    // A gain no entry could set is refused before the core is dropped.
    let mut beyond = state.clone();
    beyond.gains[1][1] = f32::INFINITY;
    assert!(matches!(
        h.e.install_product(&beyond, 0, Duration::from_secs(2)),
        Err(ReplayError::Invalid(_))
    ));
    assert_eq!(
        h.e.install_product(&state, 0, Duration::from_secs(2)),
        Err(ReplayError::Missed { id: 65 })
    );
    state.last_id = 65;
    let j =
        h.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    // At the targets once their fade-in ended.
    past_fade_in(&h.e, j);
    let core = common::copy_core(&h.e).core;
    assert_eq!(
        (core.gain(0, 0), core.gain(1, 1), core.gain(0, 1)),
        (0.25, 0.5, 0.0)
    );
    assert!(core.input_up(0));
    wait_for(Duration::from_secs(5), "computing again", || {
        processed(&h.e) > j + 20
    });

    let ranges: Vec<(u64, u64)> = h
        .statuses
        .try_iter()
        .filter_map(|s| match s {
            StatusChange::NoCore { from, to } => Some((from, to)),
            _ => None,
        })
        .collect();
    assert!(!ranges.is_empty());
    assert!(ranges.windows(2).all(|w| w[0].1 == w[1].0), "{ranges:?}");
    let (from, to) = (ranges[0].0, ranges.last().unwrap().1);
    assert_eq!(
        to,
        j + 1,
        "the last block without a core is the one installed after"
    );
    let computed: Vec<u64> = h.verdicts.try_iter().map(|v| v.k).collect();
    assert!(
        computed.iter().all(|k| !(from..to).contains(k)),
        "a block without a core computed"
    );
    assert!(computed.contains(&(j + 1)) && computed.contains(&(from - 1)));
    h.e.stop();
}

/// A product core is installed only once the core the RT thread held is
/// dropped, as when the drop at a log overflow found the control ring
/// full: the product state's generations are handed over across ticks,
/// and a core held meanwhile could swap in an entry's generation built on
/// the old flows over the new one.
#[test]
fn a_product_core_replaces_a_held_core_only_after_dropping_it() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![0.25, 0.0], vec![0.0, 0.5]],
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 0,
        ..Default::default()
    };
    let j =
        h.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    assert!(
        c.blocks_without_core.load(Relaxed) > 0,
        "the core was held through the product hand-over"
    );
    past_fade_in(&h.e, j);
    let core = common::copy_core(&h.e).core;
    assert_eq!((core.gain(0, 0), core.gain(1, 1)), (0.25, 0.5));
    assert!(processed(&h.e) > j);
    h.e.stop();
}

/// A product core is installed at the first block at or after the index
/// its caller names, not before, however soon the RT thread could take
/// it; its gains hold their targets from the first sample, with no ramp
/// from silence. An index further ahead than the timeout is refused
/// before the core the RT thread holds is dropped.
#[test]
fn a_product_core_installs_at_the_index_named_with_its_gains_at_target() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let h = hashed(&domain, &src);
        let c = h.e.counters();
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![0.25, 0.0], vec![0.0, 0.5]],
            up: vec![true],
            flows: vec![Some(src.clone())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new()],
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let far = processed(&h.e) * B + 10 * 48_000;
        assert!(matches!(
            h.e.install_product(&state, far, Duration::from_secs(2)),
            Err(ReplayError::Invalid(_))
        ));
        assert_eq!(
            c.blocks_without_core.load(Relaxed),
            0,
            "the core was dropped"
        );
        // 40 blocks ahead, a block start plus a sample: the block after.
        let at_least = (processed(&h.e) + 40) * B + 1;
        let j =
            h.e.install_product(&state, at_least, Duration::from_secs(2))
                .unwrap();
        let installed = (j + 1) * B;
        assert!(
            installed >= at_least,
            "installed at {installed}, asked for {at_least}"
        );
        // Never more than a second late, whatever the load.
        assert!(installed < at_least + 48_000, "installed at {installed}");
        // At the targets from the install on, not on a ramp from silence.
        let core = common::copy_core(&h.e).core;
        assert_eq!((core.gain(0, 0), core.gain(1, 1)), (0.25, 0.5));
        assert_eq!(core.gain(0, 1), 0.0);
        h.e.stop();
        Ok(())
    });
}

/// A product state naming a re-route's crossfade still running has the
/// supervisor read the flow the crossfade leaves until it ends: the
/// generation handed over for the install holds the outgoing reader, so
/// the slot mixes the outgoing flow fading out with the new one fading
/// in. The crossfade takes 2^22 samples, about 87 s, so it runs for the
/// whole case whatever the load. Both flows carry the same pilot, so the
/// input reads the pilot at full level from the install on; without the
/// outgoing reader it reads the new flow alone, at the crossfade's first
/// percent.
#[test]
fn a_product_state_keeps_reading_the_flow_a_crossfade_leaves() {
    let domain = Domain::new();
    let (src, old) = (new_id(), new_id());
    let _pilots = [&src, &old].map(|id| Pilot::start(&domain.0, id, 2, B, 0));
    let long = |c: &mut Config| c.ramp_samples = 1 << 22;
    let h = hashed_of(&domain, &[(&src, 2)], long);
    let start = processed(&h.e) * B;
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: diagonal(2, 1.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: samplez::InFlight {
            crossfades: vec![Some((start, Some(old.clone())))],
            ..Default::default()
        },
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 0,
        ..Default::default()
    };
    h.e.drop_core().unwrap();
    let j =
        h.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    let installed = (j + 1) * B;
    let after = |w: Option<u64>| w.is_some_and(|w| w * 4800 >= installed);
    wait_for(
        Duration::from_secs(10),
        "a meter window after the install",
        || after(h.e.status().inputs[0].meters.window),
    );
    let s = h.e.status();
    let meters = &s.inputs[0].meters;
    assert!(
        meters.peak.iter().all(|&p| p > 0.9),
        "the outgoing flow is not read: {meters:?}"
    );
    h.e.stop();
}

/// A re-route whose index passes while no core is held is applied once a
/// product core that folds it in is installed: its generation is swapped
/// in, as on every replica that computed it, where it was held pending
/// for good before.
#[test]
fn a_route_passed_without_a_core_applies_at_the_install() {
    let domain = Domain::new();
    let (src, other) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _other = Pilot::start(&domain.0, &other, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    h.e.drop_core().unwrap();
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 5
    });
    let at = (processed(&h.e) + 200) * B;
    let route = Entry::Route {
        slot: 0,
        flow_id: other.clone(),
    };
    h.e.append(1, at, route).unwrap();
    wait_for(Duration::from_secs(5), "generation built", || {
        h.e.status().generation.ready
    });
    h.e.commit(1);
    wait_for(Duration::from_secs(5), "index passed", || {
        processed(&h.e) > at / B + 10
    });
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        up: vec![true],

        flows: vec![Some(src.clone())],

        strips: vec![Vec::new()],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let late = c.command_late.load(Relaxed);
    h.e.install_product(&state, 0, Duration::from_secs(2))
        .unwrap();
    wait_for(Duration::from_secs(5), "routed", || {
        let s = h.e.status();
        s.inputs[0].flow_id == other && s.generation.pending.is_none()
    });
    assert_eq!(c.command_late.load(Relaxed), late);
    h.e.stop();
}

/// A product state's pending entry whose index the install block passed
/// is folded into the targets: left pending it would be reported late
/// and never apply.
#[test]
fn a_product_entry_the_install_passed_is_folded_in() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    h.e.drop_core().unwrap();
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 5
    });
    // Due in a block computed already when the state is built.
    let at = (processed(&h.e) - 1) * B;
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        up: vec![true],

        flows: vec![Some(src.clone())],

        strips: vec![Vec::new()],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: vec![(
            1,
            at,
            Entry::Gain {
                input: 0,
                output: 0,
                gain: 0.5,
                fade: None,
            },
        )],
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let late = c.command_late.load(Relaxed);
    let j =
        h.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    assert!(j * B >= at, "installed after block {j}");
    past_fade_in(&h.e, j);
    let core = common::copy_core(&h.e).core;
    assert_eq!((core.gain(0, 0), core.gain(1, 1)), (0.5, 1.0));
    assert_eq!(c.command_late.load(Relaxed), late);
    h.e.stop();
}

/// A strip entry the replay applied, which reaches the joiner's RT thread
/// only after the hand-over, is in the strip the core hands over: the
/// joiner computes the donor's pages with it, and taking it again would
/// count it late.
#[test]
fn a_strip_entry_the_replay_applied_still_reaches_the_strip() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        let mut replay = b.e.replay(copy(&a.e)).unwrap();
        let at = (now_block() + 20) * B;
        let strip = Entry::Strip {
            slot: 0,
            param: samplez_dsp::StripParam::Trim { db: -6.0 },
        };
        a.e.append(1, at, strip.clone()).unwrap();
        a.e.commit(1);
        replay.append(1, at, &strip).unwrap();
        let past = (at + u64::from(RAMP)) / B + PAGE / B;
        while now_block() <= past + MARGIN / B + 2 {
            std::thread::sleep(Duration::from_millis(5));
            replay.run_to(now_block());
        }
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        assert!(j * B > at, "handed over at block {j}");
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        let late = b.e.counters().command_late.load(Relaxed);
        b.e.append(1, at, strip).unwrap();
        b.e.commit(1);
        let from = processed(&b.e) + 2;
        assert_same_within(&a, &b, from * B, from * B + 4 * PAGE)?;
        assert_eq!(b.e.counters().command_late.load(Relaxed), late);
        Ok(())
    });
}

/// Two replicas given one log with a `Detach` compute the same pages, the
/// detached input's silence counted as no silenced block, and so does a
/// replica whose core is built from a product state holding no flow for
/// the input: it reads none, as the live replicas do.
#[test]
fn replicas_detach_alike_and_a_product_state_without_a_flow_matches() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let (src, other) = (new_id(), new_id());
        let _p = Pilot::start(&domain.0, &src, 2, B, 0);
        let _o = Pilot::start(&domain.0, &other, 2, B, 0);
        let inputs = [(src.as_str(), 2), (other.as_str(), 2)];
        let hs = [0; 3].map(|_| hashed_with(&domain, &inputs));
        for h in &hs {
            wait_for(Duration::from_secs(5), "inputs attached", || {
                (h.e.status().inputs.iter()).all(|i| i.state == InputState::Attached)
            });
        }
        let [a, b, c] = &hs;
        let at = (now_block() + 20) * B;
        for h in [a, b] {
            h.e.append(1, at, Entry::Detach { slot: 0 }).unwrap();
            h.e.commit(1);
        }
        assert_same_within(a, b, at, at + u64::from(RAMP) + 4 * PAGE)?;
        c.e.drop_core().unwrap();
        let identity = (0..4)
            .map(|i| (0..4).map(|o| if i == o { 1.0 } else { 0.0 }).collect())
            .collect();
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: identity,
            up: vec![true, true],
            flows: vec![None, Some(other.clone())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new(), Vec::new()],
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 1,
            ..Default::default()
        };
        let j =
            c.e.install_product(&state, 0, Duration::from_secs(2))
                .unwrap();
        assert_eq!(c.e.status().inputs[0].state, InputState::Detached);
        // From the first page whole after the install: no ramp from
        // silence in between.
        let from = (j + 1) * B;
        assert_same_within(a, c, from, from + 4 * PAGE)?;
        for h in &hs {
            assert_eq!(h.e.status().diverged, None);
        }
        Ok(())
    });
}

/// A product state holding an output disabled installs a core that
/// disables it: its writer is released. One holding it enabled, or not
/// at all, enables it again, and a pending `Output` the install passed
/// is folded in.
#[test]
fn a_product_state_disables_and_enables_an_output() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let output = || h.e.status().outputs[0].clone();
    wait_for(Duration::from_secs(5), "output realised", || {
        output().realised
    });
    let state = |outputs: Vec<bool>, pending: Vec<(u64, u64, Entry)>| ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: common::diagonal(2, 1.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs,
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending,
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let install = |s: &ProductState| {
        h.e.drop_core().unwrap();
        h.e.install_product(s, 0, Duration::from_secs(2)).unwrap()
    };
    install(&state(vec![false], Vec::new()));
    wait_for(Duration::from_secs(5), "output released", || {
        let o = output();
        !o.enabled && !o.realised
    });
    install(&state(Vec::new(), Vec::new()));
    wait_for(Duration::from_secs(5), "output realised again", || {
        let o = output();
        o.enabled && o.realised
    });
    // Enabled in the state, disabled by an entry whose index it passed.
    let off = Entry::Output {
        slot: 0,
        enabled: false,
    };
    install(&state(vec![true], vec![(1, 0, off)]));
    wait_for(
        Duration::from_secs(5),
        "output released by the entry",
        || {
            let o = output();
            !o.enabled && !o.realised
        },
    );
    let too_many = state(vec![true, true], Vec::new());
    assert!(matches!(
        h.e.install_product(&too_many, 0, Duration::from_secs(2)),
        Err(samplez::ReplayError::Invalid(_))
    ));
}

/// A product state's limiter parameters are set at the install, the
/// limiter's state kept: an output at +12 dB is held at the ceiling the
/// state names. A state naming a limiter the output does not have, or
/// parameters out of range, is refused before anything changes.
#[test]
fn a_product_state_sets_the_limiter() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed_of(&domain, &[(&src, 2)], |c| {
        c.outputs[0].limiter = true;
        c.matrix = diagonal(2, 4.0);
    });
    let state = |limiters| ProductState {
        limiters,
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: common::diagonal(2, 4.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let on = LimiterParams {
        enabled: true,
        ceiling_db: -6.0,
        release_ms: 50.0,
    };
    let refused = |s: &ProductState| {
        matches!(
            h.e.install_product(s, 0, Duration::from_secs(2)),
            Err(ReplayError::Invalid(_))
        )
    };
    assert!(refused(&state(vec![Some(on), Some(on)])), "a second output");
    let loud = LimiterParams {
        ceiling_db: 3.0,
        ..on
    };
    assert!(refused(&state(vec![Some(loud)])), "a ceiling out of range");
    h.e.drop_core().unwrap();
    let j = (h.e)
        .install_product(&state(vec![Some(on)]), 0, Duration::from_secs(2))
        .unwrap();
    let window = |w: Option<u64>| w.is_some_and(|w| w * 4800 > (j + 1) * B + 2 * 4800);
    wait_for(Duration::from_secs(5), "metered past the install", || {
        window(h.e.status().outputs[0].meters.window)
    });
    let ceiling = 10f32.powf(-6.0 / 20.0);
    let m = h.e.status().outputs[0].meters.clone();
    for c in 0..2 {
        assert!(m.peak[c] <= ceiling * (1.0 + 1e-6), "{m:?}");
        assert!(m.peak[c] > 0.8 * ceiling, "{m:?}");
    }
}

/// A product state's output strip parameters are set at the install, the
/// strip's state kept: an output the state trims by 12 dB is metered at
/// that level. A state naming parameters of an output the engine does
/// not have, or out of range, is refused before anything changes.
#[test]
fn a_product_state_sets_the_output_strip() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed_of(&domain, &[(&src, 2)], |c| {
        c.outputs[0].strip = true;
        c.matrix = diagonal(2, 1.0);
    });
    let state = |output_strips| ProductState {
        gains: common::diagonal(2, 1.0),
        up: vec![true],
        flows: vec![Some(src.clone())],
        strips: vec![Vec::new()],
        output_strips,
        last_id: 1,
        ..Default::default()
    };
    let trim = |db| StripParam::Trim { db };
    let refused = |s: &ProductState| {
        matches!(
            h.e.install_product(s, 0, Duration::from_secs(2)),
            Err(ReplayError::Invalid(_))
        )
    };
    let two = state(vec![vec![trim(-12.0)], vec![trim(-12.0)]]);
    assert!(refused(&two), "a second output");
    assert!(
        refused(&state(vec![vec![trim(20.5)]])),
        "a trim out of range"
    );
    h.e.drop_core().unwrap();
    let j = (h.e)
        .install_product(&state(vec![vec![trim(-12.0)]]), 0, Duration::from_secs(2))
        .unwrap();
    let window = |w: Option<u64>| w.is_some_and(|w| w * 4800 > (j + 1) * B + 2 * 4800);
    wait_for(Duration::from_secs(5), "metered past the install", || {
        window(h.e.status().outputs[0].meters.window)
    });
    let level = 10f32.powf(-12.0 / 20.0);
    let m = h.e.status().outputs[0].meters.clone();
    for c in 0..2 {
        assert!(m.peak[c] <= level * (1.0 + 1e-5), "{m:?}");
        assert!(m.peak[c] > 0.8 * level, "{m:?}");
    }
}

/// A core copied with a detached input decodes with it and is replayed
/// and handed over on a replica whose input is detached too. A replica
/// whose input reads a flow refuses it, and a detached one refuses a core
/// reading the input: the replay reads the replica's own inputs.
#[test]
fn a_core_with_a_detached_input_replays_only_where_it_is_detached() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
    let reading = copy(&b.e).encode();
    let detach = |h: &Hashed| {
        let at = (now_block() + 20) * B;
        h.e.append(1, at, Entry::Detach { slot: 0 }).unwrap();
        h.e.commit(1);
        wait_for(Duration::from_secs(5), "past the crossfade", || {
            h.e.status().inputs[0].state == InputState::Detached
                && processed(&h.e) > (at + u64::from(RAMP)) / B + 2
        });
    };
    detach(&a);
    let encoded = copy(&a.e).encode();
    let wire: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(wire["copy"]["core"]["slots"][0]["detached"], true);
    let decoded = || samplez::CoreCopy::decode(&encoded).unwrap();
    assert!(matches!(
        b.e.replay(decoded()),
        Err(ReplayError::Invalid(_))
    ));
    detach(&b);
    let reading = samplez::CoreCopy::decode(&reading).unwrap();
    assert!(matches!(b.e.replay(reading), Err(ReplayError::Invalid(_))));
    let mut replay = b.e.replay(decoded()).unwrap();
    b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
    assert_eq!(b.e.counters().handovers_installed.load(Relaxed), 1);
    assert_eq!(b.e.status().inputs[0].state, InputState::Detached);
    a.e.stop();
    b.e.stop();
}

/// Every page both engines computed whole within `[from, to)` hashes
/// alike, each of them compared; a run where one is disturbed or
/// missing runs again.
fn assert_same_within(a: &Hashed, b: &Hashed, from: u64, to: u64) -> Result<(), String> {
    let end = to.div_ceil(B) + PAGE / B;
    wait_for(Duration::from_secs(10), "pages compared", || {
        processed(&a.e) > end && processed(&b.e) > end
    });
    let (pa, pb) = (a.pages(), b.pages());
    let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
    for p in from.div_ceil(PAGE)..to / PAGE {
        if disturbed.contains(&p) {
            return Err(format!("page {p} disturbed"));
        }
        let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
            return Err(format!("page {p} not hashed by both"));
        };
        assert_eq!(
            (hb.hash, hb.fingerprint),
            (ha.hash, ha.fingerprint),
            "page {p}"
        );
    }
    Ok(())
}

/// [`wait_for`] that returns an error on a stall instead of panicking, so
/// a run starved of CPU is run again.
fn try_wait(timeout: Duration, what: &str, mut f: impl FnMut() -> bool) -> Result<(), String> {
    let t = std::time::Instant::now();
    while !f() {
        if t.elapsed() >= timeout {
            return Err(format!("timed out waiting for {what}"));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

/// Collects `n` pages in `[from, end)` both engines hashed whole and no
/// skipped or silenced block touched, as the engines compute them, and
/// requires each to hash alike on both. The engines keep only recent
/// pages and a loaded host skips blocks, so the pages are gathered poll
/// by poll rather than read from a fixed window. Too few, because the
/// engines skipped the stretch, is an error and not a mismatch.
///
/// The supervisor forwards verdicts, status changes and page hashes from
/// three rings, in that order within one pass, so a page can arrive before
/// a report the RT thread pushed ahead of it. A page is therefore judged
/// only once the reports that can touch it are known to have arrived:
/// - the skips by their count: the samples the status changes name must
///   reach the engine's `overrun_skipped_samples`, read after the pages;
/// - the silenced blocks by order: verdicts of one ring arrive in order,
///   so a verdict of the block after the page's last sample means every
///   verdict before it has arrived.
fn assert_same_untouched(
    a: &Hashed,
    b: &Hashed,
    from: u64,
    end: u64,
    n: usize,
) -> Result<(), String> {
    let (mut pa, mut pb) = (BTreeMap::new(), BTreeMap::new());
    let mut seen = [Seen::default(), Seen::default()];
    let mut same = BTreeMap::new();
    try_wait(Duration::from_secs(30), "comparable pages", || {
        pa.extend(a.pages());
        pb.extend(b.pages());
        let counted = [a.skipped(), b.skipped()];
        a.observe(&mut seen[0]);
        b.observe(&mut seen[1]);
        let all_skips = seen
            .iter()
            .zip(counted)
            .all(|(s, c)| s.skipped_samples >= c);
        let newest = seen[0].newest.min(seen[1].newest);
        same = (from..end)
            .filter(|_| all_skips)
            .filter(|p| ((p + 1) * PAGE).div_ceil(B) <= newest)
            .filter(|p| !seen.iter().any(|s| s.touches(*p)))
            .filter_map(|p| {
                let (x, y) = (pa.get(&p)?, pb.get(&p)?);
                Some((p, (x.hash, x.fingerprint) == (y.hash, y.fingerprint)))
            })
            .collect();
        same.len() >= n || processed(&a.e).min(processed(&b.e)) * B / PAGE >= end
    })?;
    if same.len() < n {
        return Err(format!(
            "{} comparable pages in {from}..{end}, {n} wanted",
            same.len()
        ));
    }
    let apart: Vec<u64> = same.iter().filter(|(_, s)| !**s).map(|(p, _)| *p).collect();
    assert!(apart.is_empty(), "pages {apart:?} differ");
    Ok(())
}

/// A strip entry a product state folds in, which reaches the RT thread
/// only after the install, still sets the strip, as on the engines that
/// applied it at its index.
#[test]
fn a_strip_entry_folded_into_a_product_core_still_reaches_the_strip() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        b.e.drop_core().unwrap();
        let at = (now_block() + 20) * B;
        let strip = Entry::Strip {
            slot: 0,
            param: samplez_dsp::StripParam::Trim { db: -6.0 },
        };
        a.e.append(1, at, strip.clone()).unwrap();
        a.e.commit(1);
        // Appended, committed only after the install.
        b.e.append(1, at, strip.clone()).unwrap();
        wait_for(Duration::from_secs(5), "index passed", || {
            processed(&b.e) > (at + u64::from(RAMP)) / B + 2
        });
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            up: vec![true],

            flows: vec![Some(src.clone())],

            strips: vec![Vec::new()],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: vec![(1, at, strip)],
            fading: Vec::new(),
            last_id: 1,
            ..Default::default()
        };
        let j =
            b.e.install_product(&state, 0, Duration::from_secs(2))
                .unwrap();
        b.e.commit(1);
        // After the fade-in from silence the install starts with.
        let from = ((processed(&b.e) + 2) * B).max((j + 1) * B + u64::from(RAMP));
        assert_same_within(&a, &b, from, from + 4 * PAGE)?;
        assert_eq!(b.e.counters().command_late.load(Relaxed), 0);
        Ok(())
    });
}

/// A limiter entry a product state folds in, which reaches the RT
/// thread only after the install, still sets the limiter, as on the
/// engines that applied it at its index.
#[test]
fn a_limiter_entry_folded_into_a_product_core_still_reaches_the_limiter() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let limited = |c: &mut Config| {
            c.outputs[0].limiter = true;
            c.matrix = diagonal(2, 4.0);
        };
        let (a, b) = (
            hashed_of(&domain, &[(&src, 2)], limited),
            hashed_of(&domain, &[(&src, 2)], limited),
        );
        b.e.drop_core().unwrap();
        let at = (now_block() + 20) * B;
        let limiter = Entry::Limiter {
            output: 0,
            params: LimiterParams {
                enabled: true,
                ceiling_db: -6.0,
                release_ms: 1.0,
            },
        };
        a.e.append(1, at, limiter.clone()).unwrap();
        a.e.commit(1);
        // Appended, committed only after the install.
        b.e.append(1, at, limiter.clone()).unwrap();
        try_wait(Duration::from_secs(10), "index passed", || {
            processed(&b.e) > (at + u64::from(RAMP)) / B + 2
        })?;
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: diagonal(2, 4.0),
            up: vec![true],
            flows: vec![Some(src.clone())],
            strips: vec![Vec::new()],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: vec![(1, at, limiter)],
            fading: Vec::new(),
            last_id: 1,
            ..Default::default()
        };
        let j =
            b.e.install_product(&state, 0, Duration::from_secs(2))
                .unwrap();
        b.e.commit(1);
        // Past the fade-in from silence, and the limiter's release from
        // the silence it restarted with.
        let from = ((processed(&b.e) + 2) * B).max((j + 1) * B + u64::from(RAMP)) + 48_000;
        let want = (from / B) + 2;
        try_wait(Duration::from_secs(10), "past the release", || {
            processed(&a.e) > want && processed(&b.e) > want
        })?;
        // A skip across the entry's index drops it as late on the engine
        // that applied it at its index: the two then differ for good.
        if a.e.counters().command_late.load(Relaxed) != 0 {
            return Err("the limiter entry was late".to_owned());
        }
        assert_same_untouched(&a, &b, from.div_ceil(PAGE), from.div_ceil(PAGE) + 600, 3)?;
        assert_eq!(b.e.counters().command_late.load(Relaxed), 0);
        Ok(())
    });
}

/// A re-route a product state folds in, appended but committed only after
/// the install, still swaps its generation in. Its generation is built
/// but held until the commit, so the install must not need the RT thread
/// to hold it.
#[test]
fn a_route_folded_into_a_product_core_still_swaps_in() {
    let domain = Domain::new();
    let (src, other) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _other = Pilot::start(&domain.0, &other, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    h.e.drop_core().unwrap();
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 5
    });
    let at = (processed(&h.e) + 200) * B;
    let route = Entry::Route {
        slot: 0,
        flow_id: other.clone(),
    };
    h.e.append(1, at, route).unwrap();
    wait_for(Duration::from_secs(5), "generation built", || {
        h.e.status().generation.ready
    });
    wait_for(Duration::from_secs(5), "index passed", || {
        processed(&h.e) > at / B + 10
    });
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        up: vec![true],

        flows: vec![Some(src.clone())],

        strips: vec![Vec::new()],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    h.e.install_product(&state, 0, Duration::from_secs(2))
        .unwrap();
    h.e.commit(1);
    wait_for(Duration::from_secs(5), "routed", || {
        let s = h.e.status();
        s.inputs[0].flow_id == other && s.generation.pending.is_none()
    });
    assert_eq!(c.command_late.load(Relaxed), 0);
    h.e.stop();
}

/// A strip entry the replay knew ahead of its index travels in the core's
/// pending list and applies there at its index; the joiner's log
/// delivering it later, within its ramp, applies nothing again: a second
/// apply would restart the ramp and move the strip back to the entry's
/// index, so the pages of the ramp would differ from the donor's.
#[test]
fn a_strip_entry_the_core_carried_is_not_applied_again() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        let mut replay = b.e.replay(copy(&a.e)).unwrap();
        let at = (now_block() + 400) * B;
        let strip = Entry::Strip {
            slot: 0,
            param: samplez_dsp::StripParam::Trim { db: -6.0 },
        };
        a.e.append(1, at, strip.clone()).unwrap();
        a.e.commit(1);
        replay.append(1, at, &strip).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        if (j + 1) * B >= at {
            return Err(format!("handed over at block {j}, past the entry"));
        }
        wait_for(Duration::from_secs(5), "index passed", || {
            processed(&b.e) > at / B + 2
        });
        b.e.append(1, at, strip).unwrap();
        b.e.commit(1);
        let delivered = processed(&b.e);
        let end = at + u64::from(RAMP);
        if delivered * B >= end {
            return Err(format!("delivered at block {delivered}, past the ramp"));
        }
        // From the entry's page through the end of its ramp.
        assert_same_within(&a, &b, at / PAGE * PAGE, end + PAGE)?;
        Ok(())
    });
}

/// A re-route a product state lists pending applies at its index from the
/// core's pending list; the log delivering it after its index applies
/// nothing again: a second apply finds the generation swapped in already,
/// and would hold the slot silent for good, waiting for another.
#[test]
fn a_route_the_core_carried_is_not_applied_again() {
    let domain = Domain::new();
    let (src, other) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _other = Pilot::start(&domain.0, &other, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    h.e.drop_core().unwrap();
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 5
    });
    let at = (processed(&h.e) + 100) * B;
    let route = Entry::Route {
        slot: 0,
        flow_id: other.clone(),
    };
    // Appended, committed only once its index passed.
    h.e.append(1, at, route.clone()).unwrap();
    wait_for(Duration::from_secs(5), "generation built", || {
        h.e.status().generation.ready
    });
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        up: vec![true],

        flows: vec![Some(src.clone())],

        strips: vec![Vec::new()],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: vec![(1, at, route)],
        fading: Vec::new(),
        last_id: 1,
        ..Default::default()
    };
    let j =
        h.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    assert!(
        (j + 1) * B <= at,
        "installed after block {j}, past the route"
    );
    wait_for(Duration::from_secs(5), "index passed", || {
        processed(&h.e) > at / B + 5
    });
    h.e.commit(1);
    wait_for(Duration::from_secs(5), "routed", || {
        let s = h.e.status();
        s.inputs[0].flow_id == other && s.generation.pending.is_none()
    });
    // Past the fade-in after the held blocks, no block is silenced.
    let k = processed(&h.e) + u64::from(RAMP) / B + 2;
    let _ = h.verdicts.try_iter().count();
    wait_for(Duration::from_secs(5), "blocks after the route", || {
        processed(&h.e) > k + 100
    });
    let silenced: Vec<u64> = h
        .verdicts
        .try_iter()
        .filter(|v| v.k > k && v.silenced != 0)
        .map(|v| v.k)
        .collect();
    assert!(silenced.is_empty(), "silenced blocks {silenced:?}");
    h.e.stop();
}

/// One log entry's engine entries: two gain changes and a strip entry.
fn scene() -> Vec<Entry> {
    vec![
        Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.75,
            fade: None,
        },
        Entry::Gain {
            input: 1,
            output: 1,
            gain: 0.25,
            fade: None,
        },
        Entry::Strip {
            slot: 0,
            param: StripParam::Trim { db: -6.0 },
        },
    ]
}

/// Every engine entry of a log entry applies at its index: an engine
/// taking them under one log id computes the pages of one taking each
/// under a log id of its own at that index. The entry fingerprints name
/// other log ids, so only the samples' hashes compare.
#[test]
fn a_log_entrys_entries_all_apply_at_its_index() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        // Past the fade-in from the attach.
        let at = (now_block() + 200) * B;
        a.e.append_batch(1, at, &scene()).unwrap();
        a.e.commit(1);
        for (id, entry) in (1..).zip(scene()) {
            b.e.append(id, at, entry).unwrap();
        }
        b.e.commit(3);
        let (from, to) = (at / PAGE, (at + u64::from(RAMP)) / PAGE + 4);
        let end = (to + 1) * PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a.disturbed().into_iter().chain(b.disturbed()).collect();
        for p in from..to {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(ha.hash, hb.hash, "page {p}");
        }
        for e in [&a.e, &b.e] {
            assert_eq!(e.counters().command_late.load(Relaxed), 0);
        }
        Ok(())
    });
}

/// A donor's core copied while a log entry of several engine entries is
/// pending carries its batch, and a joiner replaying it computes the
/// donor's pages bit for bit through their ramps and after the
/// hand-over, the batch handed to its RT thread with the replayed core:
/// a core without it would leave the joiner's pages apart from the
/// donor's. A log entry of no engine entries leaves the replay as it is.
#[test]
fn a_core_copied_with_a_log_entry_pending_replays_all_of_it() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        // Entry 1 diverges the joiner.
        let at1 = (now_block() + 20) * B;
        gain(&a.e, 1, at1, 0, 1.0);
        gain(&b.e, 1, at1, 0, 0.5);
        let ramped = (at1 + u64::from(RAMP)) / B + 3 * PAGE / B;
        wait_for(Duration::from_secs(5), "diverged", || {
            processed(&a.e) > ramped && processed(&b.e) > ramped
        });
        let at2 = (now_block() + 60) * B;
        for e in [&a.e, &b.e] {
            e.append_batch(2, at2, &scene()).unwrap();
            e.commit(2);
        }
        let mut copied = None;
        for _ in 0..20 {
            let c = copy(&a.e);
            let held = c.core.pending().filter(|p| p.0 == 2).count();
            assert!(held <= 1, "log entry 2 pending {held} times");
            if held == 1 {
                assert_eq!(c.last_id, 2);
                copied = Some(c);
                break;
            }
        }
        let copy = copied.ok_or("log entry 2 never pending in a copy")?;
        if (copy.k0 + 1) * B > at2 {
            return Err(format!("copied after block {}, past entry 2", copy.k0));
        }
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        // Known to the core: nothing changes.
        replay.append_batch(2, at2, &scene()).unwrap();
        assert_eq!(replay.append_batch(3, 7, &[]), Ok(()));
        let past2 = (at2 + u64::from(RAMP)) / B + PAGE / B;
        while now_block() <= past2 + MARGIN / B + 2 {
            std::thread::sleep(Duration::from_millis(5));
            replay.run_to(now_block().saturating_sub(MARGIN / B + 2));
        }
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        let end = j + 6 * PAGE / B;
        wait_for(Duration::from_secs(10), "pages after the hand-over", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        let (pa, pb) = (a.pages(), b.pages());
        let disturbed: Vec<u64> = a
            .disturbed()
            .into_iter()
            .chain(b.disturbed())
            .chain(touched(&replay.silenced()))
            .collect();
        let clean = |p: &u64| !disturbed.contains(p);
        let replayed = replay.pages();
        let mut ramp_pages = 0;
        for h in replayed.iter().filter(|h| clean(&h.page)) {
            let Some(d) = pa.get(&h.page) else { continue };
            assert_eq!(
                (h.hash, h.fingerprint),
                (d.hash, d.fingerprint),
                "replayed page {}",
                h.page
            );
            if (at2 / PAGE..=(at2 + u64::from(RAMP)) / PAGE).contains(&h.page) {
                ramp_pages += 1;
            }
        }
        if ramp_pages < 2 {
            return Err(format!("{ramp_pages} replayed pages of entry 2's ramps"));
        }
        let mut equal = 0;
        for (p, hb) in pb.iter().filter(|(p, _)| **p * PAGE >= (j + 1) * B) {
            let Some(ha) = pa.get(p).filter(|_| clean(p)) else {
                continue;
            };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
            equal += 1;
        }
        if equal < 4 {
            return Err(format!("{equal} pages after the hand-over compared"));
        }
        Ok(())
    });
}

/// A replay folds a log entry and checks its entries as the engine does:
/// an `InputUp` followed by an `InputDown` needs no generation, a
/// `Route` with an `InputDown` keeps its route and needs one, and a
/// coefficient outside the matrix is refused rather than set elsewhere.
#[test]
fn a_replay_folds_and_checks_a_log_entry_as_the_engine_does() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let mut replay = h.e.replay(copy(&h.e)).unwrap();
    let at = (now_block() + 100) * B;
    let (up, down) = (Entry::InputUp { slot: 0 }, Entry::InputDown { slot: 0 });
    assert_eq!(replay.append_batch(1, at, &[up, down.clone()]), Ok(()));
    let route = Entry::Route {
        slot: 0,
        flow_id: new_id(),
    };
    assert_eq!(
        replay.append_batch(2, at, &[route, down]),
        Err(ReplayError::Generation)
    );
    let outside = Entry::Gain {
        input: 0,
        output: 2,
        gain: 0.5,
        fade: None,
    };
    assert!(matches!(
        replay.append_batch(3, at, &[outside]),
        Err(ReplayError::Invalid(_))
    ));
    h.e.stop();
}

/// A core copied while a log entry's batch awaits its generation decodes,
/// and a replay of it is refused as one with a generation in flight, which
/// a joiner retries with a later copy, not as another engine's core.
#[test]
fn a_core_with_a_batch_awaiting_its_generation_decodes() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let mut wire: serde_json::Value = serde_json::from_str(&copy(&h.e).encode()).unwrap();
    let at = wire["copy"]["core"]["pos"].as_u64().unwrap();
    wire["copy"]["core"]["awaiting_gen"] = serde_json::json!([{
        "id": wire["copy"]["last_id"],
        "at_index": at,
        "change": { "Batch": {
            "strips": true, "up": 1, "routed": 0, "detached": 0, "resets": false,
        } },
    }]);
    let copy = samplez::CoreCopy::decode(&wire.to_string()).unwrap();
    assert!(matches!(h.e.replay(copy), Err(ReplayError::Generation)));
    h.e.stop();
}

/// A replicated engine whose RT thread takes no committed entry keeps at
/// most `WAITING` of them waiting for room; past that it drops them and
/// its core, counted, to be rebuilt, rather than hold them without bound.
#[test]
fn committed_entries_past_the_wait_bound_drop_the_core() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let c = h.e.counters();
    // Far ahead: the RT thread holds 64 pending, the ring 64 more.
    let at = (now_block() + 1_000_000) * B;
    let held = 2 * 64 + samplez::engine::WAITING as u64;
    for id in 1..=held + 1 {
        let gain = Entry::Gain {
            input: 0,
            output: 0,
            gain: (id % 7) as f32 / 7.0,
            fade: None,
        };
        h.e.append(id, at, gain).unwrap();
        h.e.commit(id);
        if id == 2 * 64 {
            // The RT thread takes 64 off the ring at its next wakes.
            std::thread::sleep(Duration::from_millis(100));
        }
        let overflows = c.log_overflows.load(Relaxed);
        assert_eq!(overflows, u64::from(id > held), "after entry {id}");
    }
    assert_eq!(h.e.progress().unwrap().log_overflows, 1);
    wait_for(Duration::from_secs(5), "blocks without a core", || {
        c.blocks_without_core.load(Relaxed) > 0
    });
    h.e.stop();
}

/// An engine that dropped committed entries past the wait bound, a
/// `Route` and a `Trim` among them, is rebuilt from a product state that
/// holds what they set: from the rebuild on the slot reads the new flow,
/// its old one gone, and its strip holds the trim, the pages equal to
/// those of an engine that applied both at their index.
#[test]
fn a_rebuild_restores_a_route_and_a_strip_parameter_the_engine_dropped() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let (src, next) = (new_id(), new_id());
        let old = Pilot::start(&domain.0, &src, 2, B, 0);
        let _next = Pilot::start(&domain.0, &next, 2, B, 0);
        let (a, b) = (hashed(&domain, &src), hashed(&domain, &src));
        let trim = StripParam::Trim { db: -6.0 };
        let changes = [
            Entry::Route {
                slot: 0,
                flow_id: next.clone(),
            },
            Entry::Strip {
                slot: 0,
                param: trim,
            },
        ];
        // b's RT thread holds 64 entries far ahead pending and 64 in its
        // ring; what commits next waits, up to the bound.
        let far = (now_block() + 1_000_000) * B;
        let filler = |id: u64| Entry::Gain {
            input: 1,
            output: 1,
            gain: (id % 7) as f32 / 7.0,
            fade: None,
        };
        for id in 1..=128 {
            b.e.append(id, far, filler(id)).unwrap();
            b.e.commit(id);
        }
        std::thread::sleep(Duration::from_millis(100));
        let at = (now_block() + 40) * B;
        a.e.append_batch(1, at, &changes).unwrap();
        a.e.commit(1);
        b.e.append_batch(129, at, &changes).unwrap();
        b.e.commit(129);
        let last = 129 + samplez::engine::WAITING as u64;
        for id in 130..=last {
            b.e.append(id, far, filler(id)).unwrap();
            b.e.commit(id);
        }
        if b.e.counters().log_overflows.load(Relaxed) != 1 {
            return Err("no overflow at the bound".into());
        }
        wait_for(Duration::from_secs(5), "the entries' index passed", || {
            processed(&b.e) > at / B + 10
        });
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            up: vec![true],
            flows: vec![Some(next.clone())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![vec![trim]],
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: last,
            ..Default::default()
        };
        let j =
            b.e.install_product(&state, 0, Duration::from_secs(2))
                .unwrap();
        // A slot still on the old flow is silenced from here.
        drop(old);
        let from = ((j + 1) * B + 2 * u64::from(RAMP)).div_ceil(PAGE);
        let end = (from + 8) * PAGE / B + PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            processed(&a.e) > end && processed(&b.e) > end
        });
        let (pa, pb) = (a.pages(), b.pages());
        let silenced: Vec<u64> = b
            .verdicts
            .try_iter()
            .filter(|v| v.silenced != 0 && v.k * B >= from * PAGE)
            .map(|v| v.k)
            .collect();
        assert!(
            silenced.is_empty(),
            "the slot read its old flow: {silenced:?}"
        );
        let disturbed = a.disturbed();
        for p in from..from + 8 {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(hb.hash, ha.hash, "page {p}");
        }
        let s = b.e.status();
        assert_eq!(s.inputs[0].flow_id, next);
        Ok(())
    });
}

/// A copy holds every entry given as unresolved before it was asked for:
/// the RT thread takes controls at the start of a wake and copies after a
/// block, so a copy asked for right after the control could be taken in
/// the wake before the one that applies it, and a joiner would lack the
/// entry.
#[test]
fn a_copy_holds_every_unresolved_entry_given_before_it() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let h = hashed(&domain, &src);
    let far = (now_block() + 1_000_000) * B;
    for id in 1..=200 {
        h.e.unresolved(id, far).unwrap();
        let c = copy(&h.e);
        assert!(
            c.core.unresolved().iter().any(|u| u.0 == id),
            "entry {id} given before the copy and not in it"
        );
        h.e.resolved(id).unwrap();
    }
    h.e.stop();
}

/// A product state that moves an input to another flow while a log
/// entry's generation for another input of its domain is built, or sent,
/// on the old flows: that generation is built again on the new ones, so
/// when the entry swaps it in, the moved input stays on its new flow, its
/// old flow gone, and the pages equal those of an engine that moved it
/// through the log.
#[test]
fn a_product_state_rebuilds_a_generation_built_on_the_old_flows() {
    for sent in [false, true] {
        common::undisturbed(|| product_over_a_pending_generation(sent));
    }
}

fn product_over_a_pending_generation(sent: bool) -> Result<(), String> {
    let domain = Domain::new();
    let (src0, src1, next0, next1) = (new_id(), new_id(), new_id(), new_id());
    let _p0 = Pilot::start(&domain.0, &src0, 1, B, 0);
    let old1 = Pilot::start(&domain.0, &src1, 1, B, 0);
    let _n0 = Pilot::start(&domain.0, &next0, 1, B, 0);
    let _n1 = Pilot::start(&domain.0, &next1, 1, B, 0);
    let inputs = [(src0.as_str(), 1), (src1.as_str(), 1)];
    let (a, b) = (hashed_with(&domain, &inputs), hashed_with(&domain, &inputs));
    let route = |slot, flow: &str| Entry::Route {
        slot,
        flow_id: flow.to_owned(),
    };
    // Input 1 moves to next1 now on a, through the product state on b.
    a.e.append(1, (now_block() + 20) * B, route(1, &next1))
        .unwrap();
    a.e.commit(1);
    // Input 0's re-route, far enough ahead to be pending on b throughout.
    let at = (now_block() + 400) * B;
    a.e.append(2, at, route(0, &next0)).unwrap();
    a.e.commit(2);
    b.e.drop_core().unwrap();
    b.e.append(2, at, route(0, &next0)).unwrap();
    if sent {
        b.e.commit(2);
    }
    let pending = |e: &Engine| e.status().generation;
    wait_for(Duration::from_secs(5), "entry 2's generation built", || {
        let g = pending(&b.e);
        g.pending == Some(2) && g.ready
    });
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        up: vec![true, true],
        flows: vec![Some(src0.clone()), Some(next1.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new(), Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: if sent {
            vec![(2, at, route(0, &next0))]
        } else {
            Vec::new()
        },
        fading: Vec::new(),
        last_id: if sent { 2 } else { 1 },
        ..Default::default()
    };
    let j =
        b.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    if !sent {
        b.e.commit(2);
    }
    // Input 1's old flow is gone: read, it is silenced.
    drop(old1);
    let from = ((j + 1) * B + 2 * u64::from(RAMP)).div_ceil(PAGE);
    let end = (at + 2 * u64::from(RAMP)) / B + 8 * PAGE / B;
    wait_for(Duration::from_secs(10), "past entry 2", || {
        processed(&a.e) > end && processed(&b.e) > end
    });
    let late: Vec<u64> = b
        .verdicts
        .try_iter()
        .filter(|v| v.silenced & 0b10 != 0 && v.k * B >= from * PAGE)
        .map(|v| v.k)
        .collect();
    assert!(
        late.is_empty(),
        "input 1 read its old flow in {} blocks from {:?}",
        late.len(),
        late.first()
    );
    assert_eq!(b.e.status().inputs[1].flow_id, next1);
    assert_eq!(b.e.status().inputs[0].flow_id, next0);
    let (pa, pb) = (a.pages(), b.pages());
    let disturbed = a.disturbed();
    let after = (at + 2 * u64::from(RAMP)).div_ceil(PAGE);
    for p in after..after + 6 {
        if disturbed.contains(&p) {
            return Err(format!("page {p} disturbed"));
        }
        let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
            return Err(format!("page {p} not hashed by both"));
        };
        assert_eq!(hb.hash, ha.hash, "page {p}");
    }
    Ok(())
}

/// A product state that folds in an entry whose generation the RT thread
/// holds pending, as a replica rebuilt past a re-route it took late
/// does: the generation goes with the entry, so the supervisor's job
/// ends and a core replayed from a donor, which holds none, is not
/// refused for it.
#[test]
fn a_product_state_drops_the_generation_of_an_entry_it_folded_in() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let (src0, src1, next0) = (new_id(), new_id(), new_id());
        let _p0 = Pilot::start(&domain.0, &src0, 1, B, 0);
        let _p1 = Pilot::start(&domain.0, &src1, 1, B, 0);
        let _n0 = Pilot::start(&domain.0, &next0, 1, B, 0);
        let inputs = [(src0.as_str(), 1), (src1.as_str(), 1)];
        let b = hashed_with(&domain, &inputs);
        let at = (now_block() + 400) * B;
        b.e.drop_core().unwrap();
        b.e.append(
            2,
            at,
            Entry::Route {
                slot: 0,
                flow_id: next0.clone(),
            },
        )
        .unwrap();
        b.e.commit(2);
        wait_for(Duration::from_secs(5), "entry 2's generation sent", || {
            b.e.status().generation.pending == Some(2)
        });
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            up: vec![true, true],
            flows: vec![Some(next0.clone()), Some(src1.clone())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new(), Vec::new()],
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 2,
            ..Default::default()
        };
        b.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
        wait_for(
            Duration::from_secs(5),
            "entry 2's generation dropped",
            || b.e.status().generation.pending.is_none(),
        );
        // A core replayed from a donor, which holds no pending
        // generation, is taken: the dropped one would refuse it.
        // The donor knows entry 2 as one needing none.
        let a = hashed_with(&domain, &inputs);
        gain(&a.e, 2, (now_block() + 20) * B, 0, 0.5);
        let mut replay = b.e.replay(copy(&a.e)).unwrap();
        replay.run_to(now_block() + 20);
        b.e.hand_over(&mut replay, Duration::from_secs(5))
            .map_err(|e| format!("no hand-over: {e}"))?;
        Ok(())
    });
}

/// The determinism rows of the ducks: replicas given the same entries
/// compute the same pages while a group ducks by its key; a joiner whose
/// ducks were set apart replays a donor's core copied 100 ms into a
/// 500 ms attack and from the hand-over on computes the donor's pages.
/// The core carries the detectors' key filter, hold and envelope state.
/// A detector follows the samples it processed: a skipped or silenced
/// block on one replica leaves its envelope apart for as long as the
/// attack, hold and release take, beyond the pages such a block touches,
/// so a run with one after the entries is disturbed.
#[test]
fn replicas_duck_alike_and_a_core_copied_mid_duck_carries_the_detectors() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let ducked = || hashed_of(&domain, &[(&src, 2)], |c| c.duck_columns = vec![0, 1]);
        let (a, b, c) = (ducked(), ducked(), ducked());
        // Past every replica's fade-in from its attach: a key that fades
        // in on one replica only parts its detector from the others'.
        let at1 = (now_block() + u64::from(RAMP).div_ceil(B) + 20) * B;
        let entries = |attack_ms: f32| {
            let duck = |setting| Entry::Duck { group: 0, setting };
            let p = DetectorParams {
                attack_ms,
                ..DetectorParams::DEFAULT
            };
            vec![
                duck(DuckSetting::Detector(Box::new(p))),
                duck(DuckSetting::Keys(vec![(0, 1.0)])),
                duck(DuckSetting::Depth { slot: 0, db: 20.0 }),
                Entry::DuckMask {
                    input: 0,
                    output: 0,
                    mask: 1,
                },
                Entry::DuckMask {
                    input: 1,
                    output: 1,
                    mask: 1,
                },
            ]
        };
        let skipped = |e: &Engine| e.counters().overrun_skipped_samples.load(Relaxed);
        let live_skipped = (skipped(&a.e), skipped(&c.e));
        // The joiner's attack sets its detector apart, and nothing else:
        // a joiner keeping its own detector would compute other pages.
        for (e, attack) in [(&a.e, 500.0), (&c.e, 500.0), (&b.e, 50.0)] {
            e.append_batch(1, at1, &entries(attack)).unwrap();
            e.commit(1);
        }
        let mid = at1 / B + 100;
        wait_for(Duration::from_secs(5), "into the attack", || {
            processed(&a.e) > mid && processed(&b.e) > mid
        });
        let copy = copy(&a.e);
        let e0 = copy.core.detector(0).unwrap().envelope();
        if !(0.0 < e0 && e0 < 1.0) {
            return Err(format!("copied at envelope {e0}"));
        }
        let k0 = copy.k0;
        let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
        let mut replay = b.e.replay(copy).unwrap();
        let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
        let joiner_skipped = skipped(&b.e);
        let passed = b.e.counters().handovers_passed.load(Relaxed);
        if passed >= MAX_PASSED {
            return Err(format!("{passed} hand-overs passed"));
        }
        if (j + 1) * B > at1 + 20_000 {
            return Err(format!("handed over at block {j}, past the attack"));
        }
        let from = ((j + 1) * B).div_ceil(PAGE);
        let end = (from + 8) * PAGE / B + PAGE / B;
        wait_for(Duration::from_secs(10), "pages compared", || {
            [&a, &b, &c].iter().all(|h| processed(&h.e) > end)
        });
        let (pa, pb, pc) = (a.pages(), b.pages(), c.pages());
        // The live replicas from the entries on, the joiner from its
        // hand-over on, which its core and detectors start from.
        let (da, db, dc) = (a.disturbed(), b.disturbed(), c.disturbed());
        let skips = (
            skipped(&a.e) - live_skipped.0,
            skipped(&c.e) - live_skipped.1,
            skipped(&b.e) - joiner_skipped,
        );
        if skips != (0, 0, 0) {
            return Err(format!("samples skipped after the entries: {skips:?}"));
        }
        // The joiner's pages are compared over eight pages after its
        // hand-over, which the loop below checks.
        for (name, d) in [("a", &da), ("c", &dc)] {
            if let Some(p) = d.iter().find(|&&p| p >= at1 / PAGE) {
                let past = p - at1 / PAGE;
                return Err(format!("{name}: page {past} after the entries disturbed"));
            }
        }
        let disturbed: Vec<u64> = [da, db, dc].concat();
        let before = (at1 / PAGE + 1)..k0 * B / PAGE;
        let differ = before
            .filter(|p| matches!((pa.get(p), pb.get(p)), (Some(x), Some(y)) if x.hash != y.hash))
            .count();
        assert!(differ > 0, "the joiner never diverged");
        for p in from..from + 8 {
            if disturbed.contains(&p) {
                return Err(format!("page {p} disturbed"));
            }
            let (Some(ha), Some(hb)) = (pa.get(&p), pb.get(&p)) else {
                return Err(format!("page {p} not hashed by both"));
            };
            assert_eq!(
                (hb.hash, hb.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
        }
        let mut alike = 0;
        for (p, ha) in pa
            .iter()
            .filter(|(p, _)| **p > at1 / PAGE && !disturbed.contains(p))
        {
            let Some(hc) = pc.get(p) else { continue };
            assert_eq!(
                (hc.hash, hc.fingerprint),
                (ha.hash, ha.fingerprint),
                "page {p}"
            );
            alike += 1;
        }
        if alike < 10 {
            return Err(format!("{alike} pages of two replicas compared"));
        }
        Ok(())
    });
}

/// A product core starts its ducks cold at the state's settings: a group
/// its manual controls trigger at full depth from the install, its keys
/// keyed, the masks and depths at their targets, and every other group
/// at rest. A state holding a duck value
/// no entry could set is refused before anything changes.
#[test]
fn a_product_core_starts_its_ducks_cold_and_a_forced_group_at_full_depth() {
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
        let h = hashed_of(&domain, &[(&src, 2)], |c| c.duck_columns = vec![0]);
        let state = ProductState {
            fading: Vec::new(),
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            up: vec![true],
            flows: vec![Some(src.clone())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new()],
            ducks: vec![DuckTargets {
                keys: vec![(0, 0.5)],
                mode: DuckMode::On,
                depths: vec![20.0],
                ..DuckTargets::default()
            }],
            masks: vec![vec![1, 0], vec![0, 0]],
            in_flight: Default::default(),
            pending: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let refused = |what: &str, f: &dyn Fn(&mut ProductState)| {
            let mut s = state.clone();
            f(&mut s);
            let r = h.e.install_product(&s, 0, Duration::from_secs(2));
            assert!(matches!(r, Err(ReplayError::Invalid(_))), "{what}: {r:?}");
        };
        refused("a mask off the duck columns", &|s| s.masks[0][1] = 1);
        refused("masks of another shape", &|s| {
            s.masks.pop();
        });
        refused("nine keys", &|s| s.ducks[0].keys = vec![(0, 1.0); 9]);
        refused("nine groups", &|s| {
            s.ducks.resize(9, DuckTargets::default())
        });
        refused("a depth beyond its range", &|s| s.ducks[0].depths[0] = 41.0);
        assert_eq!(h.e.counters().blocks_without_core.load(Relaxed), 0);
        let at_least = (processed(&h.e) + 40) * B;
        let j =
            h.e.install_product(&state, at_least, Duration::from_secs(2))
                .unwrap();
        let copy = common::copy_core(&h.e);
        if copy.k0 > j + 20 {
            return Err(format!("copied {} blocks after the install", copy.k0 - j));
        }
        let d = copy.core.detector(0).unwrap();
        assert_eq!(d.envelope(), 1.0);
        assert!(d.is_engaged() && d.mode() == DuckMode::On);
        assert_eq!(d.key(0), Some((2, 0.5)));
        assert!((1..8).all(|g| copy.core.detector(g).unwrap().is_idle()));
        assert_eq!(copy.core.duck_mask(0, 0), 1);
        assert_eq!(copy.core.duck_mask(1, 1), 0);
        assert_eq!(
            (copy.core.duck_depth(0, 0), copy.core.duck_depth(1, 0)),
            (20.0, 20.0)
        );
        h.e.stop();
        Ok(())
    });
}

/// An install of a product state whose outputs list leaves an output out,
/// empty or shorter, starts it as configured: one configured disabled
/// stays disabled and gets no flow, as on a replica that never dropped
/// its core. Listed enabled, it is enabled.
#[test]
fn an_install_leaving_an_output_out_starts_it_as_configured() {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let (out0, out1) = (new_id(), new_id());
    let mut config = config(
        &domain.0,
        &[(&src, 2)],
        &[(&out0, 2), (&out1, 1)],
        vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 1.0]],
        MARGIN,
    );
    config.outputs[1].enabled = false;
    let (report_tx, _reports) = sync_channel(64);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: None,
        hashes: None,
    };
    let e = Engine::start_replicated(config, process, None, replication).unwrap();
    wait_for(Duration::from_secs(5), "attached and realised", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    let install = |outputs: Vec<bool>| {
        e.drop_core().unwrap();
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 1.0]],
            up: vec![true],
            flows: vec![Some(src.clone())],
            strips: vec![Vec::new()],
            outputs,
            loudness_running: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let j = e
            .install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
        wait_for(Duration::from_secs(5), "computing again", || {
            processed(&e) > j + 50
        });
    };
    // None listed, or a shorter list: output 1 as configured.
    for outputs in [Vec::new(), vec![true]] {
        install(outputs.clone());
        std::thread::sleep(Duration::from_millis(500));
        let o = e.status().outputs[1].clone();
        assert!(!o.enabled && !o.realised, "{outputs:?}: {o:?}");
        assert!(
            !domain.has_flow(&out1),
            "{outputs:?}: the disabled output's flow created"
        );
    }
    // Listed enabled, it is enabled whatever its configured start.
    install(vec![true, true]);
    wait_for(Duration::from_secs(10), "output 1 realised", || {
        let o = e.status().outputs[1].clone();
        o.enabled && o.realised && domain.has_flow(&out1)
    });
    assert!(e.status().outputs[0].realised);
    e.stop();
}

/// Both gains of a 2-channel diagonal to 0.25 over a fade, as entries
/// `1` and `2` committed on `e` at `at`.
fn fade_entries(e: &Engine, at: u64, fade: Fade) {
    for channel in 0..2 {
        let entry = Entry::Gain {
            input: channel,
            output: channel,
            gain: 0.25,
            fade: Some(fade),
        };
        e.append(1 + channel as u64, at, entry).unwrap();
    }
    e.commit(2);
}

/// A fade long enough to copy and rebuild inside, in samples, and where
/// the cases look at it: waiting for its start, ramping.
const FADE_DELAY: u64 = 30_000;
const FADE_LEN: u32 = 120_000;

/// The fade of the log's entries at `at`.
fn fade_at(at: u64) -> Fade {
    Fade::after(1.0, at, FADE_DELAY as u32, FADE_LEN)
}

/// Waits until sample index `n` is processed on every engine.
fn wait_index(engines: &[&Engine], n: u64) {
    wait_for(Duration::from_secs(10), "index reached", || {
        engines.iter().all(|e| processed(e) > n.div_ceil(B))
    });
}

/// Resync across a gain fade: a joiner whose fade was set apart replays a
/// donor's core copied while the fade still waits for its start, or
/// while it ramps, and computes the donor's pages bit for bit. The
/// coefficient carries its start value, delay, length and position, so
/// nothing of the fade lives apart from the matrix.
fn replayed_core_carries_a_fade(wait: u64) -> Result<(), String> {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let plain = |c: &mut Config| c.matrix = diagonal(2, 1.0);
    let (a, b) = (
        hashed_of(&domain, &[(&src, 2)], plain),
        hashed_of(&domain, &[(&src, 2)], plain),
    );
    let at = (now_block() + 20) * B;
    fade_entries(&a.e, at, fade_at(at));
    // Set apart: the joiner's fade is a short one, which has ended.
    fade_entries(&b.e, at, Fade::after(1.0, at, 0, 100));
    wait_index(&[&a.e, &b.e], at + wait);
    let copy = copy(&a.e);
    let k0 = copy.k0;
    let copy = samplez::CoreCopy::decode(&copy.encode()).unwrap();
    let mut replay = b.e.replay(copy).unwrap();
    let j = b.e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
    let passed = b.e.counters().handovers_passed.load(Relaxed);
    if passed >= MAX_PASSED {
        return Err(format!("{passed} hand-overs passed"));
    }
    // The fade is still running at the hand-over, or the case tests nothing.
    if (j + 1) * B > at + (FADE_DELAY + u64::from(FADE_LEN)) - 3 * PAGE {
        return Err(format!("handed over at block {j}, near the fade's end"));
    }
    // Before the copy the joiner's short fade and the donor's long one
    // differ on every page the ramp touches.
    let mut pair = Pair::new(&a, &b);
    if wait > FADE_DELAY {
        let before = pair.comparable((at / PAGE + 1)..k0 * B / PAGE);
        if !before.iter().any(|&(_, same)| !same) {
            return Err("no page before the copy shows the joiner apart".to_owned());
        }
    }
    let from = ((j + 1) * B).div_ceil(PAGE);
    let end = (at + FADE_DELAY + u64::from(FADE_LEN)) / PAGE;
    let pages = wait_comparable(&mut pair, from, end, 3, (at + FADE_DELAY) / PAGE)?;
    let apart: Vec<_> = pages.iter().filter(|(_, same)| !**same).collect();
    assert!(
        apart.is_empty(),
        "pages {apart:?} differ after the hand-over"
    );
    Ok(())
}

/// What two engines' page hashes and block verdicts have delivered so
/// far, collected as they come: the channels are drained on every read.
/// A page hash can reach its channel before the verdict of the page's
/// last block, which the RT thread sends at the end of that block, so a
/// page counts as comparable only once a verdict beyond its last block
/// came from both engines: the verdicts are in block order, and every
/// one before it came with it.
struct Pair<'a> {
    a: &'a Hashed,
    b: &'a Hashed,
    pages: [BTreeMap<u64, PageHash>; 2],
    silenced: [Vec<u64>; 2],
    newest: [u64; 2],
    /// The engines' overrun-skip counters from before the state under
    /// test was built: pages count only while neither has moved. A skip
    /// restarts the detectors of one engine, so pages after it may differ.
    calm: Option<(u64, u64)>,
    /// The domain to read the outputs from, when a mismatch should be
    /// read back at once: the flows keep little history.
    probe: Option<std::path::PathBuf>,
    /// Per differing page, where its outputs first differ, read when the
    /// page was first seen to differ.
    diffs: BTreeMap<u64, String>,
}

impl<'a> Pair<'a> {
    fn new(a: &'a Hashed, b: &'a Hashed) -> Pair<'a> {
        Pair {
            a,
            b,
            pages: [BTreeMap::new(), BTreeMap::new()],
            silenced: [Vec::new(), Vec::new()],
            newest: [0, 0],
            calm: None,
            probe: None,
            diffs: BTreeMap::new(),
        }
    }

    fn poll(&mut self) {
        for (i, h) in [self.a, self.b].into_iter().enumerate() {
            self.pages[i].extend(h.hashes.try_iter().map(|p| (p.page, p)));
            for v in h.verdicts.try_iter() {
                if v.silenced != 0 {
                    self.silenced[i].push(v.k);
                }
                self.newest[i] = self.newest[i].max(v.k);
            }
        }
    }

    /// The pages in `range` both engines hashed whole, neither one's
    /// silenced block or the fade-in after it touched, and a verdict past
    /// them came for: each with whether the two hash alike. A loaded host
    /// narrows the set, it never moves what the set holds.
    fn comparable(&mut self, range: std::ops::Range<u64>) -> Vec<(u64, bool)> {
        self.poll();
        let confirmed = self.newest[0].min(self.newest[1]);
        let disturbed: Vec<u64> = self.silenced.iter().flat_map(|s| touched(s)).collect();
        let pages: Vec<(u64, bool)> = range
            .filter(|p| (p + 1) * PAGE / B < confirmed && !disturbed.contains(p))
            .filter_map(|p| {
                let (x, y) = (self.pages[0].get(&p)?, self.pages[1].get(&p)?);
                Some((p, (x.hash, x.fingerprint) == (y.hash, y.fingerprint)))
            })
            .collect();
        if let Some(domain) = &self.probe {
            for &(p, _) in pages.iter().filter(|(_, same)| !same) {
                if !self.diffs.contains_key(&p) {
                    let d = first_difference(domain, &self.a.out, &self.b.out, p);
                    self.diffs.insert(p, d);
                }
            }
        }
        pages
    }
}

/// Where the outputs `a` and `b` first differ within page `p`, read back
/// at once.
fn first_difference(domain: &std::path::Path, a: &str, b: &str, p: u64) -> String {
    let (first, last) = (p * PAGE / B, (p + 1) * PAGE / B - 1);
    let read = |flow: &str| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            common::read_blocks(domain, flow, 2, first..last + 1)
        }))
    };
    let (Ok(x), Ok(y)) = (read(a), read(b)) else {
        return "the output could not be read back".to_owned();
    };
    let n = B as usize;
    // Planar per block: channel c of block k at (k * 2 + c) * B.
    match x
        .iter()
        .zip(&y)
        .position(|(a, b)| a.to_bits() != b.to_bits())
    {
        Some(i) => {
            let (k, c, s) = (i / (2 * n), i / n % 2, i % n);
            format!(
                "first differing sample {} (block {}, channel {c}): {} vs {}",
                (first + k as u64) * B + s as u64,
                first + k as u64,
                x[i],
                y[i]
            )
        }
        None => "the samples read back are equal".to_owned(),
    }
}

/// Everything a mismatch between `pair`'s engines on page `p` needs to
/// explain itself: the overrun-skip counters at the baseline and now,
/// the install index, the page's blocks, how far the verdicts reached,
/// and the first sample of the page where the two outputs differ.
fn explain(pair: &Pair, p: u64, installed: u64) -> String {
    let (first, last) = (p * PAGE / B, (p + 1) * PAGE / B - 1);
    let now = (pair.a.skipped(), pair.b.skipped());
    let mut out = format!(
        "page {p} (blocks {first}..={last}), installed at index {installed}, skips at the \
         baseline {:?} now {now:?}, newest verdicts {:?}, last block of the page confirmed {}",
        pair.calm,
        pair.newest,
        pair.newest[0].min(pair.newest[1]) > last,
    );
    out += &format!(
        ", {}",
        pair.diffs
            .get(&p)
            .map_or("no read-back was taken", String::as_str)
    );
    out
}

/// Collects `n` comparable pages at or after page `from` and before page
/// `end`, as the engines compute them, with pages on both sides of page
/// `split` when it lies past `from`, and returns each with whether the
/// engines agree. Too few, because an engine skipped the stretch or the
/// fade ended first, is an error and not a mismatch.
fn wait_comparable(
    pair: &mut Pair,
    from: u64,
    end: u64,
    n: usize,
    split: u64,
) -> Result<BTreeMap<u64, bool>, String> {
    let both = |f: &BTreeMap<u64, bool>| {
        split <= from || (f.keys().any(|&p| p < split) && f.keys().any(|&p| p > split))
    };
    let mut found = BTreeMap::new();
    let (ea, eb) = (&pair.a.e, &pair.b.e);
    let over = || processed(ea).min(processed(eb)) * B / PAGE >= end + 2;
    let mut done = false;
    wait_for(Duration::from_secs(10), "comparable pages", || {
        let seen = pair.comparable(from..end);
        if pair
            .calm
            .is_none_or(|base| (pair.a.skipped(), pair.b.skipped()) == base)
        {
            found.extend(seen);
        }
        done = found.len() >= n && both(&found);
        done || over()
    });
    if !done {
        return Err(format!(
            "{} comparable pages in {from}..{end}, {n} wanted, on both sides of {split}",
            found.len()
        ));
    }
    Ok(found)
}

#[test]
fn a_replayed_core_carries_a_fade_still_waiting() {
    common::undisturbed(|| replayed_core_carries_a_fade(5_000));
}

#[test]
fn a_replayed_core_carries_a_fade_mid_ramp() {
    common::undisturbed(|| replayed_core_carries_a_fade(FADE_DELAY + 20_000));
}

/// A replica rebuilt from a product state mid-fade, the state naming the
/// fade's start value and absolute start, computes its peers' pages from
/// the first page after the install, bit for bit: the coefficient
/// continues the fade where the peers are, it does not jump to the
/// target. The same with the fade not started yet.
fn rebuilt_replica_continues_a_fade(wait: u64, assert_mid: bool) -> Result<(), String> {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let plain = |c: &mut Config| c.matrix = diagonal(2, 1.0);
    let hs = [0; 3].map(|_| hashed_of(&domain, &[(&src, 2)], plain));
    let [a, b, c] = &hs;
    let at = (now_block() + 20) * B;
    for h in [a, b] {
        fade_entries(&h.e, at, fade_at(at));
        if assert_mid {
            // The same fades asserted again inside the ramp, as a
            // restamp re-asserts every target.
            let again = (at + FADE_DELAY + 5_000).next_multiple_of(B);
            for channel in 0..2 {
                let entry = Entry::Gain {
                    input: channel,
                    output: channel,
                    gain: 0.25,
                    fade: Some(fade_at(at)),
                };
                h.e.append(3 + channel as u64, again, entry).unwrap();
            }
            h.e.commit(4);
        }
    }
    wait_index(&[&a.e, &b.e, &c.e], at + wait);
    c.e.drop_core().unwrap();
    let fading = Some(fade_at(at));
    let state = ProductState {
        fading: vec![vec![fading, None], vec![None, fading]],
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![0.25, 0.0], vec![0.0, 0.25]],
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        last_id: if assert_mid { 4 } else { 2 },
        ..Default::default()
    };
    let j =
        c.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    if (j + 1) * B > at + (FADE_DELAY + u64::from(FADE_LEN)) - 5 * PAGE {
        return Err(format!("installed at block {j}, near the fade's end"));
    }
    let from = ((j + 1) * B).div_ceil(PAGE);
    let mut pair = Pair::new(a, c);
    let end = (at + FADE_DELAY + u64::from(FADE_LEN)) / PAGE;
    let pages = wait_comparable(&mut pair, from, end, 3, (at + FADE_DELAY) / PAGE)?;
    let apart: Vec<_> = pages.iter().filter(|(_, same)| !**same).collect();
    assert!(apart.is_empty(), "pages {apart:?} differ after the install");
    Ok(())
}

#[test]
fn a_rebuilt_replica_continues_a_fade_still_waiting() {
    common::undisturbed(|| rebuilt_replica_continues_a_fade(2_000, false));
}

#[test]
fn a_rebuilt_replica_continues_a_fade_mid_ramp() {
    common::undisturbed(|| rebuilt_replica_continues_a_fade(FADE_DELAY + 20_000, false));
}

#[test]
fn a_rebuilt_replica_matches_its_peers_after_a_mid_fade_assert() {
    common::undisturbed(|| rebuilt_replica_continues_a_fade(FADE_DELAY + 20_000, true));
}

/// A replica rebuilt mid-fade, in a matrix with a duck, from a product
/// state that holds the fade, computes its peers' pages: the fade brings
/// a masked coefficient in from silence, and the peers' duck on it is
/// the one the install builds, with no crossfade of its own. The group is
/// forced on from entries long before the fade, so the detectors agree.
fn rebuilt_replica_continues_a_masked_fade(delay: u32) -> Result<(), String> {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let ducked = |c: &mut Config| {
        c.duck_columns = vec![0];
        c.matrix = vec![vec![0.0, 0.0], vec![0.0, 1.0]];
    };
    let hs = [0; 3].map(|_| hashed_of(&domain, &[(&src, 2)], ducked));
    let [a, b, c] = &hs;
    // The overrun counters are the engines' own, kept across drop_core and
    // install_product. Taken as soon as the engines exist, before the duck
    // and mask entries: a skip anywhere from here on restarts a detector
    // and disqualifies the run (no comparable page is left).
    let calm = (a.skipped(), c.skipped());
    let at1 = (now_block() + 20) * B;
    let at = at1 + 400 * B;
    let fade = Fade::after(0.0, at, delay, FADE_LEN);
    for h in [a, b] {
        let duck = |setting| Entry::Duck { group: 0, setting };
        let setup = [
            duck(DuckSetting::Mode(samplez_dsp::DuckMode::On)),
            duck(DuckSetting::Depth { slot: 0, db: 20.0 }),
            Entry::DuckMask {
                input: 0,
                output: 0,
                mask: 1,
            },
        ];
        h.e.append_batch(1, at1, &setup).unwrap();
        h.e.commit(1);
        let entry = Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.25,
            fade: Some(fade),
        };
        h.e.append(2, at, entry).unwrap();
        h.e.commit(2);
    }
    wait_index(&[&a.e, &b.e, &c.e], at + u64::from(delay) + 20_000);
    c.e.drop_core().unwrap();
    let state = ProductState {
        fading: vec![vec![Some(fade), None], vec![None, None]],
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![0.25, 0.0], vec![0.0, 1.0]],
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: vec![DuckTargets {
            mode: samplez_dsp::DuckMode::On,
            depths: vec![20.0],
            ..DuckTargets::default()
        }],
        masks: vec![vec![1, 0], vec![0, 0]],
        in_flight: Default::default(),
        pending: Vec::new(),
        last_id: 2,
        ..Default::default()
    };
    let j =
        c.e.install_product(&state, 0, Duration::from_secs(2))
            .unwrap();
    if (j + 1) * B > at + FADE_DELAY + u64::from(FADE_LEN) - 5 * PAGE {
        return Err(format!("installed at block {j}, near the fade's end"));
    }
    // From the install's own page: the duck of a masked term the install
    // fills starts at the gain a live engine holds, so even an install at
    // the first block of a page computes that page alike.
    let from = ((j + 1) * B).div_ceil(PAGE);
    let mut pair = Pair::new(a, c);
    pair.calm = Some(calm);
    pair.probe = Some(domain.0.clone());
    let end = (at + u64::from(delay) + u64::from(FADE_LEN)) / PAGE;
    let pages = wait_comparable(&mut pair, from, end, 3, 0)?;
    if let Some((&p, _)) = pages.iter().find(|(_, same)| !**same) {
        let why = explain(&pair, p, (j + 1) * B);
        panic!("pages {pages:?} differ after the install: {why}");
    }
    Ok(())
}

#[test]
fn a_rebuilt_replica_continues_a_masked_fade_that_starts_at_its_entry() {
    common::undisturbed(|| rebuilt_replica_continues_a_masked_fade(0));
}

#[test]
fn a_rebuilt_replica_continues_a_masked_fade_that_waits() {
    common::undisturbed(|| rebuilt_replica_continues_a_masked_fade(FADE_DELAY as u32));
}

/// A replica rebuilt inside the ramp of plain gain entries, from a state
/// that holds the ramps the entries left running, as a replica's product
/// state does, computes its peers' pages from the first page after the
/// install: the install index lies inside the last entry's ramp, and with
/// `chained` the entry before it ramps on inside that one's.
fn rebuilt_replica_continues_plain_ramps(chained: bool) -> Result<(), String> {
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let plain = |c: &mut Config| c.matrix = diagonal(2, 1.0);
    let hs = [0; 3].map(|_| hashed_of(&domain, &[(&src, 2)], plain));
    let [a, b, c] = &hs;
    let at = (now_block() + 100) * B;
    let second = at + 10 * B;
    let gain = |to| Entry::Gain {
        input: 0,
        output: 0,
        gain: to,
        fade: None,
    };
    let ramp = RAMP;
    let first = Fade {
        from: 1.0,
        start: at,
        len: ramp,
    };
    let (last, target, install) = if chained {
        let from = first.value_at(0.25, second);
        let f = Fade {
            from,
            start: second,
            len: ramp,
        };
        (f, 0.75, second + 96)
    } else {
        (first, 0.25, at + 96)
    };
    for h in [a, b] {
        h.e.append(1, at, gain(0.25)).unwrap();
        h.e.commit(1);
        if chained {
            h.e.append(2, second, gain(0.75)).unwrap();
            h.e.commit(2);
        }
    }
    c.e.drop_core().unwrap();
    let state = ProductState {
        fading: vec![vec![Some(last), None], vec![None, None]],
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: vec![vec![target, 0.0], vec![0.0, 1.0]],
        up: vec![true],
        flows: vec![Some(src.clone())],
        outputs: Vec::new(),
        loudness_running: Vec::new(),
        strips: vec![Vec::new()],
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        last_id: if chained { 2 } else { 1 },
        ..Default::default()
    };
    let j =
        c.e.install_product(&state, install, Duration::from_secs(2))
            .unwrap();
    let from = ((j + 1) * B).div_ceil(PAGE);
    let end = (last.start + u64::from(ramp)) / PAGE;
    let mut pair = Pair::new(a, c);
    let pages = wait_comparable(&mut pair, from, end, 1, 0)?;
    let apart: Vec<_> = pages.iter().filter(|(_, same)| !**same).collect();
    assert!(apart.is_empty(), "pages {apart:?} differ after the install");
    Ok(())
}

#[test]
fn a_rebuilt_replica_continues_a_plain_ramp() {
    common::undisturbed(|| rebuilt_replica_continues_plain_ramps(false));
}

#[test]
fn a_rebuilt_replica_continues_chained_plain_ramps() {
    common::undisturbed(|| rebuilt_replica_continues_plain_ramps(true));
}

/// Replicas agree on the integrated loudness, the loudness range, the
/// gaps, the paused slices and the start of a measurement the log resets,
/// over an `InputDown` and the `InputUp` after it and a pause and run
/// both mid-slice: the slices the log took the input down for are gaps on
/// both, those it held the measurement paused in are paused on both, each
/// left out of both measurements, which carry on after them. The
/// programme ends with its gains at 0, after which nothing changes the
/// measurement; it is compared once both engines delivered a verdict past
/// that point and metered it. A run that lost a slice, or silenced an
/// input outside the `InputDown`, is disturbed and runs again.
#[test]
fn replicas_agree_on_the_loudness_and_its_gaps() {
    use samplez_dsp::{SLICE, WINDOW};
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _p = Pilot::start(&domain.0, &src, 2, B, 0);
        // A loaded host catches up rather than skipping: a skip loses
        // slices, and the run would be disturbed.
        let hs = [0; 2].map(|_| hashed_of(&domain, &[(&src, 2)], |c| c.max_catchup_blocks = 50));
        // What loses slices to a measurement, counted from before it.
        let losses = |h: &Hashed| {
            let c = h.e.counters();
            (
                h.skipped(),
                c.meter_dropped.load(Relaxed),
                c.blocks_without_core.load(Relaxed),
            )
        };
        let before = hs.each_ref().map(losses);
        let reset = ((now_block() + 100) * B).next_multiple_of(WINDOW);
        // 5.5 s of programme around half a second down and half a second
        // paused, enough for a loudness range. The pause starts and ends
        // inside a slice.
        let down = reset + 72_000;
        let up = down + 24_000;
        let pause = up + 48_000 + 5 * B;
        let run = pause + 24_000 + 2 * B;
        let end = run + 96_000;
        let running = |running| Entry::LoudnessRun { meter: 0, running };
        for h in &hs {
            h.e.append(1, reset, Entry::ResetLoudness { meter: 0 })
                .unwrap();
            h.e.append(2, down, Entry::InputDown { slot: 0 }).unwrap();
            h.e.append(3, up, Entry::InputUp { slot: 0 }).unwrap();
            h.e.append(4, pause, running(false)).unwrap();
            h.e.append(5, run, running(true)).unwrap();
            h.e.commit(5);
            gain(&h.e, 6, end, 0, 0.0);
            gain(&h.e, 7, end, 1, 0.0);
        }
        // Past the gains' ramp and the 3 s of short-term loudness after
        // it, no gating block or short-term value is above the gates. The
        // supervisor drains the meter records after the verdicts within a
        // pass, so a verdict past that point does not show the records
        // metered: the status's window has to be past it as well.
        let frozen = end + u64::from(RAMP) + 3 * 48_000 + WINDOW;
        let mut verdicts = [Vec::new(), Vec::new()];
        try_wait(
            Duration::from_secs(30),
            "both measured past the end",
            || {
                for (v, h) in verdicts.iter_mut().zip(&hs) {
                    v.extend(h.verdicts.try_iter());
                }
                let past = |v: &Vec<BlockVerdict>| v.iter().any(|v| v.k * B > frozen);
                let metered = |h: &Hashed| {
                    let w = h.e.status().outputs[0].meters.window;
                    w.is_some_and(|w| w * WINDOW > frozen)
                };
                verdicts.iter().all(past) && hs.iter().all(metered)
            },
        )?;
        let meters = hs
            .each_ref()
            .map(|h| h.e.status().outputs[0].meters.clone());
        let [ma, mb] = &meters;
        // A run is disturbed only by a measured cause: a skip, a dropped
        // meter record or a block without a core since before the reset,
        // or an input read silenced outside the down span. Slices lost
        // without one fail the test.
        let after = hs.each_ref().map(losses);
        if after != before {
            return Err(format!(
                "skipped samples, meter records dropped, blocks without a core: \
                 {before:?} to {after:?}; slices lost: {} and {}",
                ma.lost, mb.lost
            ));
        }
        let downed = down / B..up / B;
        for v in verdicts.iter().flatten() {
            if v.k * B >= reset && v.silenced != 0 && !downed.contains(&v.k) {
                return Err(format!("block {} silenced", v.k));
            }
        }
        assert_eq!((ma.lost, mb.lost), (0, 0), "slices lost, no cause measured");
        let gaps = (up - down) / SLICE;
        assert_eq!((ma.gaps, mb.gaps), (gaps, gaps), "{ma:?} {mb:?}");
        // Every slice with a sample from the paused span.
        let paused = run.div_ceil(SLICE) - pause / SLICE;
        assert_eq!((ma.paused, mb.paused), (paused, paused), "{ma:?} {mb:?}");
        assert!(ma.running && mb.running, "{ma:?} {mb:?}");
        assert_eq!(ma.since, Some(reset), "{ma:?}");
        assert!(ma.integrated.is_some() && ma.range.is_some(), "{ma:?}");
        let bits = |m: &samplez::OutputMeters| {
            (
                m.integrated.map(f64::to_bits),
                m.range.map(f64::to_bits),
                m.since,
            )
        };
        assert_eq!(bits(ma), bits(mb), "{ma:?} {mb:?}");
        Ok(())
    });
}

/// A replica rebuilt from a product state while the log holds its
/// output's loudness paused keeps it paused, and computes its peer's
/// pages from the first page after the install. Once a reset after the
/// install restarts both measurements, it counts the slices paused up to
/// the `LoudnessRun` that runs the meter again as its peer does, and both
/// agree on the integrated loudness, the range and the start. A run that
/// skipped, dropped a meter record, went without a core or silenced an
/// input after the install is disturbed and runs again.
#[test]
fn a_rebuilt_replica_keeps_a_meter_paused() {
    use samplez_dsp::{SLICE, WINDOW};
    common::undisturbed(|| {
        let domain = Domain::new();
        let src = new_id();
        let _p = Pilot::start(&domain.0, &src, 2, B, 0);
        let plain = |c: &mut Config| {
            c.matrix = diagonal(2, 1.0);
            c.max_catchup_blocks = 50;
        };
        let hs = [0; 2].map(|_| hashed_of(&domain, &[(&src, 2)], plain));
        let [a, c] = &hs;
        let running = |running| Entry::LoudnessRun { meter: 0, running };
        let pause = (now_block() + 100) * B;
        for h in &hs {
            h.e.append(1, pause, running(false)).unwrap();
            h.e.commit(1);
        }
        c.e.drop_core().unwrap();
        let state = ProductState {
            gains: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            fading: Vec::new(),
            up: vec![true],
            flows: vec![Some(src.clone())],
            outputs: Vec::new(),
            loudness_running: vec![false],
            strips: vec![Vec::new()],
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            last_id: 1,
            ..Default::default()
        };
        let j =
            c.e.install_product(&state, pause + B, Duration::from_secs(2))
                .map_err(|e| format!("install: {e:?}"))?;
        let losses = |h: &Hashed| {
            let c = h.e.counters();
            (
                h.skipped(),
                c.meter_dropped.load(Relaxed),
                c.blocks_without_core.load(Relaxed),
            )
        };
        let before = hs.each_ref().map(losses);
        // A reset after the install, the meter still paused; run again
        // inside a slice.
        let reset = ((now_block() + 100) * B).next_multiple_of(WINDOW);
        let run = reset + 24_000 + 2 * B;
        let end = run + 96_000;
        for h in &hs {
            h.e.append(2, reset, Entry::ResetLoudness { meter: 0 })
                .unwrap();
            h.e.append(3, run, running(true)).unwrap();
            h.e.commit(3);
            gain(&h.e, 4, end, 0, 0.0);
            gain(&h.e, 5, end, 1, 0.0);
        }
        let mut pair = Pair::new(a, c);
        let from = ((j + 1) * B).div_ceil(PAGE);
        let pages = wait_comparable(&mut pair, from, end / PAGE, 1, 0)?;
        if let Some((&p, _)) = pages.iter().find(|(_, same)| !**same) {
            let why = explain(&pair, p, (j + 1) * B);
            panic!("pages {pages:?} differ after the install: {why}");
        }
        let frozen = end + u64::from(RAMP) + 3 * 48_000 + WINDOW;
        let mut verdicts = [Vec::new(), Vec::new()];
        try_wait(
            Duration::from_secs(30),
            "both measured past the end",
            || {
                for (v, h) in verdicts.iter_mut().zip(&hs) {
                    v.extend(h.verdicts.try_iter());
                }
                let past = |v: &Vec<BlockVerdict>| v.iter().any(|v| v.k * B > frozen);
                let metered = |h: &Hashed| {
                    let w = h.e.status().outputs[0].meters.window;
                    w.is_some_and(|w| w * WINDOW > frozen)
                };
                verdicts.iter().all(past) && hs.iter().all(metered)
            },
        )?;
        let meters = hs
            .each_ref()
            .map(|h| h.e.status().outputs[0].meters.clone());
        let [ma, mc] = &meters;
        let after = hs.each_ref().map(losses);
        if after != before {
            return Err(format!(
                "skipped samples, meter records dropped, blocks without a core: \
                 {before:?} to {after:?}; slices lost: {} and {}",
                ma.lost, mc.lost
            ));
        }
        for v in verdicts.iter().flatten() {
            if v.k * B >= reset && v.silenced != 0 {
                return Err(format!("block {} silenced", v.k));
            }
        }
        assert_eq!((ma.lost, mc.lost), (0, 0), "slices lost, no cause measured");
        let paused = run.div_ceil(SLICE) - reset / SLICE;
        assert_eq!((ma.paused, mc.paused), (paused, paused), "{ma:?} {mc:?}");
        assert!(ma.running && mc.running, "{ma:?} {mc:?}");
        assert!(ma.integrated.is_some() && ma.range.is_some(), "{ma:?}");
        let bits = |m: &samplez::OutputMeters| {
            (
                m.integrated.map(f64::to_bits),
                m.range.map(f64::to_bits),
                m.gaps,
                m.since,
            )
        };
        assert_eq!(bits(ma), bits(mc), "{ma:?} {mc:?}");
        assert_eq!(ma.since, Some(reset), "{ma:?}");
        Ok(())
    });
}
