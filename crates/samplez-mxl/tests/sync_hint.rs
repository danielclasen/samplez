//! A reader waiting in a synchronization group is woken by every commit
//! of a writer whose batch is longer than B: by each block committed on
//! its own and by a range committed at once. libmxl signals waiters once
//! per `maxSyncBatchSizeHint` window, so the hints stay at B.

#![allow(
    deprecated,
    reason = "the group wait libmxl gives; the engine waits on heads"
)]

mod common;

use std::time::{Duration, Instant};

use common::{B, Domain, TIMING, api, input, new_id};
use samplez_mxl::input::{Generation, GroupWait};
use samplez_mxl::output::{self, Creation, FlowWriter, OutputConfig, Write};
use samplez_mxl::{Instance, time};

/// `K0 * B` is a multiple of 20B, so blocks K0..K0 + 20 share one window
/// of a batch of 20B.
const K0: u64 = 1_000_000;
const TIMEOUT_NS: u64 = 3_000_000_000;

/// Commits blocks K0 and K0 + 1, starts a group wait for block `k`,
/// runs `commit` 100 ms later, and returns the wait's result and how long
/// it took.
fn wait_for(
    batch: u64,
    k: u64,
    commit: impl FnOnce(&mut FlowWriter, &[f32]),
) -> (GroupWait, Duration) {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        flow_id: &id,
        label: "sync hint",
        group: "samplez-test",
        role: "Audio",
        channels: 2,
        block: B,
        batch,
    };
    let Creation::Created { writer: mut w, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("flow not created");
    };
    assert_eq!(w.hints(), (B as u32, B as u32));
    let data = vec![0.25f32; 2 * batch as usize];
    for k in K0..K0 + 2 {
        assert_eq!(w.write_block(k, &data[..2 * B as usize]), Write::Committed);
    }
    let mut g = Generation::new(&api, &domain.0, 1).unwrap();
    g.attach(0, &input(&id, 2, 0), &TIMING).unwrap();
    let ts = time::group_timestamp(g.api(), k, B, g.max_offset());
    let t0 = Instant::now();
    let waiter = std::thread::spawn(move || {
        let s = g.wait(ts, TIMEOUT_NS);
        (s, t0.elapsed(), g)
    });
    std::thread::sleep(Duration::from_millis(100));
    commit(&mut w, &data);
    let (s, took, _g) = waiter.join().unwrap();
    (s, took)
}

fn woken((s, took): (GroupWait, Duration)) {
    assert_eq!(s, GroupWait::Ok, "woken after {took:?}");
    assert!(took < Duration::from_secs(2), "{took:?}");
}

#[test]
fn a_block_commit_wakes_a_waiting_reader_with_batch_b() {
    woken(wait_for(B, K0 + 2, |w, data| {
        assert_eq!(
            w.write_block(K0 + 2, &data[..2 * B as usize]),
            Write::Committed
        );
    }));
}

#[test]
fn a_block_commit_wakes_a_waiting_reader_with_a_larger_batch() {
    woken(wait_for(20 * B, K0 + 2, |w, data| {
        assert_eq!(
            w.write_block(K0 + 2, &data[..2 * B as usize]),
            Write::Committed
        );
    }));
}

#[test]
fn a_range_commit_wakes_a_waiting_reader() {
    let stride = 20 * B as usize;
    woken(wait_for(20 * B, K0 + 6, |w, data| {
        assert_eq!(
            w.write((K0 + 2) * B, 5 * B as usize, data, stride),
            Write::Committed
        );
    }));
}
