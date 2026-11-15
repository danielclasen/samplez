//! The core a replicated engine copies after a block, for a resync.

mod common;

use std::time::Duration;

use common::{
    B, Domain, Pilot, copy_core as copy, new_id, processed, start_held, start_held_with, wait_for,
};
use samplez::{CopyError, Entry, InputState};

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
        Err(CopyError::Invalid(_))
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
    // A second of budget: more than half the flow's ring of 19456.
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
