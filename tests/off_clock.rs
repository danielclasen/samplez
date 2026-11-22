//! An input whose producer stamps its flow off the MXL clock, as one on
//! another epoch does: a head that stays farther from the clock than the
//! flow's readable history is the input's error, naming the gap, and the
//! error clears once the head is back. A late input within its flow's
//! history and a stopped producer are not reported so.

mod common;

use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, start, wait_for};
use samplez::{Engine, InputState};

const SECOND: Duration = Duration::from_secs(1);
const RATE: u64 = 48_000;
const OFF: &str = " the MXL clock, beyond the flow's history";

/// A domain whose flows keep `secs` seconds of readable history.
fn domain(secs: u64) -> Domain {
    let d = Domain::new();
    std::fs::write(
        d.0.join("options.json"),
        format!(
            r#"{{"urn:x-mxl:option:history_duration/v1.0": {}}}"#,
            secs * 1_000_000_000
        ),
    )
    .unwrap();
    d
}

/// An engine reading `src` as its one input.
fn engine(domain: &Domain, src: &str) -> Engine {
    let out = new_id();
    let cfg = config(
        &domain.0,
        &[(src, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    );
    start(cfg).0
}

fn error(e: &Engine) -> Option<String> {
    e.status().inputs[0].error.clone()
}

/// The gap in seconds and its side an off-clock error names.
fn gap(error: &str) -> (f64, &str) {
    let rest = error.strip_prefix("head ").expect(error);
    let (secs, rest) = rest.split_once(" s ").expect(error);
    let side = rest.strip_suffix(OFF).expect(error);
    (secs.parse().expect(error), side)
}

/// A producer 37 s behind the clock, with a flow of 1 s of history: the
/// input attaches and never copies a block, and its error names the gap.
/// Back on the clock, the error clears and the input is read again.
#[test]
fn an_input_37_s_behind_the_clock_reports_the_gap_until_back() {
    let domain = domain(1);
    let src = new_id();
    let pilot = Pilot::start(&domain.0, &src, 1, B, 37 * RATE);
    let e = engine(&domain, &src);
    wait_for(10 * SECOND, "the gap reported", || {
        error(&e).is_some_and(|m| m.ends_with(OFF))
    });
    let message = error(&e).unwrap();
    let (secs, side) = gap(&message);
    assert_eq!(side, "behind", "{message}");
    assert!((36.9..37.3).contains(&secs), "{message}");
    assert_eq!(e.status().inputs[0].counters.copied, 0);

    pilot.set_delay(0);
    wait_for(10 * SECOND, "the error cleared", || error(&e).is_none());
    wait_for(10 * SECOND, "the input read again", || {
        let s = e.status();
        s.inputs[0].state == InputState::Attached && s.inputs[0].counters.copied > 0
    });
    assert_eq!(error(&e), None);
    common::stop(e);
}

/// A producer 5 s ahead of the clock, whose blocks the input reads as
/// too late: its error names the gap ahead.
#[test]
fn an_input_ahead_of_the_clock_reports_the_gap() {
    let domain = domain(1);
    let src = new_id();
    let pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let e = engine(&domain, &src);
    wait_for(10 * SECOND, "the input read", || {
        e.status().inputs[0].counters.copied > 0
    });
    assert_eq!(error(&e), None);
    pilot.set_delay(-5 * RATE as i64);
    wait_for(10 * SECOND, "the gap reported", || {
        error(&e).is_some_and(|m| m.ends_with(OFF))
    });
    let message = error(&e).unwrap();
    let (secs, side) = gap(&message);
    assert_eq!(side, "ahead of", "{message}");
    assert!((4.8..5.1).contains(&secs), "{message}");
    assert!(e.status().inputs[0].counters.too_late > 0);
    common::stop(e);
}

/// The rule is the flow's history: a producer 2 s behind is off the clock
/// for a flow of 1 s of history, and a late input, pruned and not
/// reported, for a flow of 3 s, for longer than the first took to be
/// reported plus that history.
#[test]
fn a_late_input_within_its_flows_history_is_not_reported() {
    let short = domain(1);
    let src = new_id();
    let _pilot = Pilot::start(&short.0, &src, 1, B, 2 * RATE);
    let e = engine(&short, &src);
    let started = std::time::Instant::now();
    wait_for(10 * SECOND, "the gap reported", || {
        error(&e).is_some_and(|m| m.ends_with(OFF))
    });
    let took = started.elapsed();
    let message = error(&e).unwrap();
    let (secs, side) = gap(&message);
    assert!(side == "behind" && (1.9..2.3).contains(&secs), "{message}");
    common::stop(e);

    let long = domain(3);
    let src = new_id();
    let _pilot = Pilot::start(&long.0, &src, 1, B, 2 * RATE);
    let e = engine(&long, &src);
    wait_for(10 * SECOND, "the late input pruned", || {
        e.status().inputs[0].state == InputState::Pruned
    });
    std::thread::sleep(took + 3 * SECOND);
    assert_eq!(e.status().inputs[0].state, InputState::Pruned);
    assert_eq!(error(&e), None);
    common::stop(e);
}

/// A producer that stops: its head falls behind the clock past the flow's
/// history, and the input is pruned with its head frozen, not reported
/// off the clock. Started again, it is read again.
#[test]
fn a_stopped_producer_is_not_reported_off_the_clock() {
    let domain = domain(1);
    let src = new_id();
    let (_pilot, gate) = Pilot::gated(&domain.0, &src, 1, B, 0);
    gate.store(true, std::sync::atomic::Ordering::Relaxed);
    let e = engine(&domain, &src);
    let copied = || e.status().inputs[0].counters.copied;
    wait_for(10 * SECOND, "the input read", || copied() > 0);
    gate.store(false, std::sync::atomic::Ordering::Relaxed);
    wait_for(10 * SECOND, "the stopped input pruned", || {
        e.status().inputs[0].state == InputState::Pruned
    });
    // Past the history, the margin and the history again.
    std::thread::sleep(4 * SECOND);
    assert_eq!(error(&e), None);
    gate.store(true, std::sync::atomic::Ordering::Relaxed);
    let before = copied();
    wait_for(10 * SECOND, "the input read again", || copied() > before);
    assert_eq!(error(&e), None);
    common::stop(e);
}
