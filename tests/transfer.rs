//! The core a replicated engine copies after a block, for a resync.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;

use common::{
    B, Domain, Pilot, copy_core as copy, new_id, processed, start_held, start_held_with, wait_for,
};
use common::{MARGIN, config, diagonal};
use samplez::blocks::{PAGE, PageHash};
use samplez::{BlockVerdict, CopyError, Engine, Entry, InputState, ReplayError, Replication};

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
}

/// Samples of a gain ramp: about two pages, so the pages a ramp runs
/// over differ from one another.
const RAMP: u32 = 2048;

fn hashed(domain: &Domain, src: &str) -> Hashed {
    let mut config = config(
        &domain.0,
        &[(src, 2)],
        &[(&new_id(), 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    config.ramp_samples = RAMP;
    let (report_tx, _reports) = sync_channel(64);
    let (hash_tx, hashes) = sync_channel(1 << 16);
    let (verdict_tx, verdicts) = sync_channel(1 << 16);
    let process = samplez::rt::setup_process(&config.rt);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: None,
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
    }
}

impl Hashed {
    fn pages(&self) -> BTreeMap<u64, PageHash> {
        self.hashes.try_iter().map(|h| (h.page, h)).collect()
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

/// The pages blocks `ks` and the fade-in after each touch.
fn touched(ks: &[u64]) -> Vec<u64> {
    ks.iter()
        .flat_map(|k| k * B / PAGE..((k + 1) * B + u64::from(RAMP)).div_ceil(PAGE))
        .collect()
}

/// Hand-overs refused because the RT thread had passed their block
/// before one landed: each attempt runs the replay up to the block the
/// RT thread just computed and passes it on before the next wake, so
/// only a supervisor thread held off the CPU for a block period misses.
const MAX_PASSED: u64 = 10;

fn gain(e: &Engine, id: u64, at: u64, channel: usize, gain: f32) {
    let entry = Entry::Gain {
        input: channel,
        output: channel,
        gain,
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
        let mut replay = b.e.replay(copy).unwrap();
        replay
            .append(
                2,
                at2,
                &Entry::Gain {
                    input: 1,
                    output: 1,
                    gain: 0.25,
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
        assert!(passed < MAX_PASSED, "{passed} hand-overs passed");
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
        assert!(passed < MAX_PASSED, "{passed} hand-overs passed");
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
