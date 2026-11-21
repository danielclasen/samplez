//! A poll reader's opens, through samplez-mxl's test hooks: failed ones
//! let the references it held go for the caller to release, and the flow
//! is opened again only once they are released; slow ones run on a thread
//! of their own, so neither a poll nor the engine's supervisor waits for
//! them.

mod common;

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use common::{B, Domain, MARGIN, Pilot, api, config, diagonal, new_id, wait_for};
use samplez::InputState;
use samplez_mxl::Instance;
use samplez_mxl::input::{
    Poll, PollOpener, PolledFlow, Releases, set_open_delay, set_open_failure,
};

/// Per flow id, how many more opens succeed before every later one fails.
static FAILING: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

fn fails(flow_id: &str) -> bool {
    let mut failing = FAILING.lock().unwrap();
    match failing.get_or_insert_default().get_mut(flow_id) {
        Some(0) => true,
        Some(left) => {
            *left -= 1;
            false
        }
        None => false,
    }
}

fn fail_after(flow_id: &str, opens: usize) {
    set_open_failure(Some(fails));
    (FAILING.lock().unwrap().get_or_insert_default()).insert(flow_id.to_owned(), opens);
}

fn stop_failing(flow_id: &str) {
    (FAILING.lock().unwrap().get_or_insert_default()).remove(flow_id);
}

/// Polls until `done` or five seconds passed, and returns the last poll.
fn poll_until(p: &mut PolledFlow, done: impl Fn(&Poll) -> bool) -> Poll {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let polled = p.poll();
        if done(&polled) || Instant::now() >= until {
            return polled;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn head(p: &mut PolledFlow) -> Poll {
    poll_until(p, |r| matches!(r, Poll::Head(_)))
}

/// A hold whose open fails lets every reference go, the open one
/// included, and the flow is not opened again while it is held.
#[test]
fn a_hold_whose_open_fails_lets_every_reference_go() {
    let domain = Domain::new();
    let id = new_id();
    let _pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    let poll = Instance::new(&api(), &domain.0).unwrap();
    let mut p = PolledFlow::new(&poll, &id).unwrap();
    assert!(matches!(head(&mut p), Poll::Head(_)));

    fail_after(&id, 0);
    p.hold();
    // The hold's open runs on a thread of its own; the poll that takes
    // its failure lets the references go.
    let until = Instant::now() + Duration::from_secs(5);
    let held = loop {
        assert!(Instant::now() < until, "no reference let go");
        let polled = p.poll();
        let held = p.take_released();
        if !held.is_empty() {
            assert!(matches!(polled, Poll::Absent(_)), "{polled:?}");
            break held;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(held.len(), 1);
    stop_failing(&id);
    assert!(matches!(p.poll(), Poll::Absent(_)));
    drop(held);
    assert!(matches!(head(&mut p), Poll::Head(_)));
}

/// An open of two holders' references that fails at the second lets the
/// first go, and the flow is not opened again while it is held.
#[test]
fn an_open_that_fails_part_way_lets_the_opened_references_go() {
    let domain = Domain::new();
    let id = new_id();
    let _pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    let poll = Instance::new(&api(), &domain.0).unwrap();
    fail_after(&id, 1);
    let mut p = PolledFlow::new(&poll, &id).unwrap();
    p.hold();
    // The lookup finds the flow, then the second open fails.
    let until = Instant::now() + Duration::from_secs(5);
    let held = loop {
        assert!(Instant::now() < until, "no reference let go");
        assert!(!matches!(p.poll(), Poll::Head(_)));
        let held = p.take_released();
        if !held.is_empty() {
            break held;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(held.len(), 1);
    stop_failing(&id);
    assert!(matches!(p.poll(), Poll::Absent(_)));
    drop(held);
    assert!(matches!(head(&mut p), Poll::Head(_)));
}

/// A flow id, whether its opens wait, and the threads they ran on, by
/// name and id.
type Held = (String, bool, Vec<(String, ThreadId)>);

/// Per flow id, its [`Held`] record.
static HELD: Mutex<Vec<Held>> = Mutex::new(Vec::new());

/// The open hook of `samplez_mxl` (`set_open_delay`): an open of a flow
/// [`HELD`] names records its thread and waits, inside libmxl's open as a
/// slow one does, until the flow is let open, 20 s at most.
fn held_open(flow_id: &str) {
    let current = std::thread::current();
    let thread = (current.name().unwrap_or_default().to_owned(), current.id());
    {
        let mut held = HELD.lock().unwrap();
        let Some(h) = held.iter_mut().find(|h| h.0 == flow_id) else {
            return;
        };
        h.2.push(thread);
    }
    let until = Instant::now() + Duration::from_secs(20);
    let waits = || (HELD.lock().unwrap().iter()).any(|h| h.0 == flow_id && h.1);
    while waits() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Makes the opens of `flow_id` wait until [`let_open`].
fn hold_opens(flow_id: &str) {
    set_open_delay(Some(held_open));
    (HELD.lock().unwrap()).push((flow_id.to_owned(), true, Vec::new()));
}

/// Records the threads the opens of `flow_id` run on.
fn watch_opens(flow_id: &str) {
    set_open_delay(Some(held_open));
    (HELD.lock().unwrap()).push((flow_id.to_owned(), false, Vec::new()));
}

fn let_open(flow_id: &str) {
    for h in HELD.lock().unwrap().iter_mut().filter(|h| h.0 == flow_id) {
        h.1 = false;
    }
}

/// The names of the threads the opens of `flow_id` ran on.
fn opened_on(flow_id: &str) -> Vec<String> {
    (HELD.lock().unwrap().iter())
        .filter(|h| h.0 == flow_id)
        .flat_map(|h| h.2.iter().map(|t| t.0.clone()))
        .collect()
}

/// The threads the poll opens of `flows` ran on, one per open.
fn poll_opens_on(flows: &[&str]) -> Vec<ThreadId> {
    (HELD.lock().unwrap().iter())
        .filter(|h| flows.contains(&h.0.as_str()))
        .flat_map(|h| h.2.iter().filter(|t| t.0 == "samplez-poll-open"))
        .map(|t| t.1)
        .collect()
}

/// Poll readers given one opener open on its one thread, one after
/// another: a poll starts no thread.
#[test]
fn poll_readers_sharing_an_opener_open_on_its_one_thread() {
    let domain = Domain::new();
    let ids = [new_id(), new_id()];
    let _pilots = ids
        .each_ref()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    let poll = Instance::new(&api(), &domain.0).unwrap();
    let opener = PollOpener::start().unwrap();
    for id in &ids {
        watch_opens(id);
        let mut p = PolledFlow::new(&poll, id).unwrap().with_opener(&opener);
        assert!(matches!(head(&mut p), Poll::Head(_)));
    }
    let threads = poll_opens_on(&[&ids[0], &ids[1]]);
    assert_eq!(threads.len(), 2, "{threads:?}");
    assert_eq!(threads[0], threads[1]);
}

/// A poll reader opens its flow on a thread of its own: a poll while the
/// open runs, however long it takes, reads the flow opening and returns,
/// and the first poll after the open is done reads the head.
#[test]
fn a_poll_opens_its_flow_on_a_thread_of_its_own() {
    let domain = Domain::new();
    let id = new_id();
    let _pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    let poll = Instance::new(&api(), &domain.0).unwrap();
    hold_opens(&id);
    let mut p = PolledFlow::new(&poll, &id).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while opened_on(&id).is_empty() {
        assert!(Instant::now() < until, "no open started");
        let polled = p.poll();
        assert!(!matches!(polled, Poll::Head(_)), "{polled:?}");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(p.poll(), Poll::Opening));
    let me = std::thread::current().name().unwrap_or_default().to_owned();
    let threads = opened_on(&id);
    assert!(
        !threads.contains(&me),
        "opened on the polling thread: {threads:?}"
    );
    let_open(&id);
    assert!(matches!(head(&mut p), Poll::Head(_)));
}

/// A poll reader whose opener's thread is gone drops that opener and
/// opens on one started at its next poll, rather than asking the gone
/// one for every open.
#[test]
fn a_poll_reader_replaces_an_opener_whose_thread_is_gone() {
    let domain = Domain::new();
    let id = new_id();
    let _pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    let poll = Instance::new(&api(), &domain.0).unwrap();
    let mut p = (PolledFlow::new(&poll, &id).unwrap()).with_opener(&PollOpener::gone());
    assert!(matches!(head(&mut p), Poll::Head(_)));
}

/// An open running counts in the flow's releases, and one still running
/// when the flow turns invalid lets what it opens go once it ends: libmxl
/// hands it the replaced flow's reader object, which no poll reads.
#[test]
fn an_open_running_when_the_flow_turns_invalid_lets_what_it_opened_go() {
    let domain = Domain::new();
    let id = new_id();
    let pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    let poll = Instance::new(&api(), &domain.0).unwrap();
    let releases = Releases::default();
    let mut p = PolledFlow::new(&poll, &id)
        .unwrap()
        .with_releases(&releases);
    assert!(matches!(head(&mut p), Poll::Head(_)));
    hold_opens(&id);
    p.hold();
    wait_for(Duration::from_secs(5), "the hold's open started", || {
        !opened_on(&id).is_empty()
    });
    assert_eq!(releases.pending(), 1, "the open running");
    drop(pilot);
    assert!(matches!(
        poll_until(&mut p, |r| matches!(r, Poll::Invalid)),
        Poll::Invalid
    ));
    let first = p.take_released();
    assert_eq!(first.len(), 1);
    // Recreated before the open goes on, so that it opens either way.
    let _pilot = Pilot::start(&domain.0, &id, 1, B, 0);
    assert!(matches!(p.poll(), Poll::Absent(_)));
    let_open(&id);
    let until = Instant::now() + Duration::from_secs(5);
    let second = loop {
        assert!(Instant::now() < until, "the open's reference not let go");
        let polled = p.poll();
        assert!(!matches!(polled, Poll::Head(_)), "{polled:?}");
        let second = p.take_released();
        if !second.is_empty() {
            break second;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(second.len(), 1);
    assert_eq!(releases.pending(), 2);
    assert!(matches!(p.poll(), Poll::Absent(_)));
    drop((first, second));
    assert!(matches!(head(&mut p), Poll::Head(_)));
}

/// A pruned input's flow that comes back is opened off the supervisor:
/// while that open hangs, the supervisor takes a re-route of another slot
/// in and swaps its generation in on time, and the pruned input is read
/// again once the open is done.
#[test]
fn a_hanging_poll_open_holds_up_no_hand_over() {
    let domain = Domain::new();
    let (src, other, next, out) = (new_id(), new_id(), new_id(), new_id());
    let pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let _pilots = [&other, &next].map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    let cfg = config(
        &domain.0,
        &[(&src, 1), (&other, 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    let (e, _rx) = common::start(cfg);
    let state = |slot: usize| e.status().inputs[slot].state;
    wait_for(Duration::from_secs(5), "engine running", || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised)
            && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    drop(pilot);
    wait_for(Duration::from_secs(5), "input pruned", || {
        state(0) == InputState::Pruned
    });
    hold_opens(&src);
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    wait_for(Duration::from_secs(10), "the poll's open started", || {
        !opened_on(&src).is_empty()
    });
    e.route(1, &next, 0).unwrap();
    wait_for(Duration::from_secs(10), "the re-route swapped in", || {
        let s = e.status();
        s.inputs[1].flow_id == next && s.generation.pending.is_none()
    });
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    assert_eq!(state(0), InputState::Pruned);
    let threads = opened_on(&src);
    assert!(
        !threads.iter().any(|t| t == "samplez-supervisor"),
        "opened on the supervisor: {threads:?}"
    );
    let_open(&src);
    wait_for(Duration::from_secs(10), "input re-added", || {
        state(0) == InputState::Attached
    });
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    common::stop(e);
}

/// The poll readers of an input domain open on one thread the engine
/// starts with the domain's poll instance: two pruned inputs whose flows
/// come back are opened on the same thread.
#[test]
fn a_domains_poll_opens_run_on_one_thread() {
    let domain = Domain::new();
    let (src, out) = ([new_id(), new_id()], new_id());
    let pilots = src
        .each_ref()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    let cfg = config(
        &domain.0,
        &[(&src[0], 1), (&src[1], 1)],
        &[(&out, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    let (e, _rx) = common::start(cfg);
    let attached = |s: InputState| s == InputState::Attached;
    let states = || {
        e.status()
            .inputs
            .iter()
            .map(|i| i.state)
            .collect::<Vec<_>>()
    };
    wait_for(Duration::from_secs(5), "engine running", || {
        e.status().outputs.iter().all(|o| o.realised) && states().into_iter().all(attached)
    });
    drop(pilots);
    wait_for(Duration::from_secs(5), "both inputs pruned", || {
        states().iter().all(|&s| s == InputState::Pruned)
    });
    for id in &src {
        watch_opens(id);
    }
    let _pilots = src
        .each_ref()
        .map(|id| Pilot::start(&domain.0, id, 1, B, 0));
    wait_for(Duration::from_secs(10), "both inputs re-added", || {
        states().into_iter().all(attached)
    });
    let threads = poll_opens_on(&[&src[0], &src[1]]);
    assert!(threads.len() >= 2, "{threads:?}");
    assert!(threads.iter().all(|&t| t == threads[0]), "{threads:?}");
    common::stop(e);
}
