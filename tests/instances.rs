//! Alone in its process, so that no other test's instances count: libmxl
//! instances, each with a domain watcher thread and three descriptors, do
//! not pile up across re-routes.

mod common;

use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, wait_for};
use samplez_mxl::Instance;

/// 200 re-routes between two flows, each swapped in before the next is
/// sent, leave as many live instances as before them once the last
/// crossfade ended and its rebuild is in: every replaced generation and
/// every reader opened for one is released with its instance.
#[test]
fn re_routes_leave_the_instance_count_flat() {
    let domain = Domain::new();
    let (a, b, out) = (new_id(), new_id(), new_id());
    let _pa = Pilot::start(&domain.0, &a, 1, B, 0);
    let _pb = Pilot::start(&domain.0, &b, 1, B, 0);
    let (e, _rx) = common::start(config(
        &domain.0,
        &[(&a, 1)],
        &[(&out, 1)],
        diagonal(1, 1.0),
        MARGIN,
    ));
    let settled = || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised)
            && s.inputs[0].state == samplez::InputState::Attached
            && s.generation.pending.is_none()
    };
    wait_for(Duration::from_secs(5), "engine running", settled);
    // Past the start-up rebuilds.
    std::thread::sleep(Duration::from_millis(500));
    let before = Instance::live();
    for n in 0..200 {
        let flow = if n % 2 == 0 { &b } else { &a };
        e.route(0, flow, 0).unwrap();
        // Swapped in, its flow in the status, before the next is sent: the
        // log takes one generation-needing entry at a time.
        wait_for(Duration::from_secs(5), "the re-route swapped in", || {
            settled() && e.status().inputs[0].flow_id == *flow
        });
    }
    // The last crossfade ends and its rebuild drops the outgoing reader.
    std::thread::sleep(Duration::from_millis(500));
    wait_for(Duration::from_secs(5), "instances released", || {
        Instance::live() == before
    });
    assert_eq!(e.counters().generation_late.load(Relaxed), 0);
    // The supervisor released none itself: all went to its reaper thread.
    assert_eq!(samplez_mxl::guarded_releases(), 0);
    assert_eq!(e.status().inputs[0].flow_id, a);
    common::stop(e);
}
