//! A poll reader's failed opens, forced through samplez-mxl's test hook:
//! the references it held are let go for the caller to release, and the
//! flow is opened again only once they are released.

mod common;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::{B, Domain, Pilot, api, new_id};
use samplez_mxl::Instance;
use samplez_mxl::input::{Poll, PolledFlow, set_open_failure};

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
    let held = p.take_released();
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
