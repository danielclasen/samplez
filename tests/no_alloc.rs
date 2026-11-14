//! The RT thread allocates and frees nothing through Rust's global
//! allocator once it runs, over a run that includes input generation
//! swaps, a prune, a re-add with fade-in and a gain ramp. libmxl's own C++
//! allocations are not seen by this allocator.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, start, wait_for};
use samplez::InputState;

/// The RT thread's kernel tid; 0 before it is known.
static RT_TID: AtomicI32 = AtomicI32::new(0);
static RT_CALLS: AtomicU64 = AtomicU64::new(0);
/// Where the RT thread's first allocator call came from, so a failure
/// names the call site.
static FIRST_RT_CALL: OnceLock<Backtrace> = OnceLock::new();

thread_local! {
    /// Set while the first call's backtrace is taken, which allocates.
    static CAPTURING: Cell<bool> = const { Cell::new(false) };
}

struct Counting;

fn count() {
    let tid = RT_TID.load(Relaxed);
    // SAFETY: gettid has no preconditions.
    if tid == 0 || unsafe { libc::gettid() } != tid || CAPTURING.get() {
        return;
    }
    // A panicking thread holds the backtrace lock already: capturing here
    // would deadlock instead of failing.
    if RT_CALLS.fetch_add(1, Relaxed) == 0 && !std::thread::panicking() {
        CAPTURING.set(true);
        let _ = FIRST_RT_CALL.set(Backtrace::force_capture());
        CAPTURING.set(false);
    }
}

// SAFETY: forwards to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: the caller's contract is passed through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count();
        // SAFETY: the caller's contract is passed through.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: the caller's contract is passed through.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn rt_thread_allocates_nothing() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let matrix = vec![vec![0.5, 0.0], vec![0.0, 0.5]];
    let (engine, rx) = start(config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        matrix,
        MARGIN,
    ));
    RT_TID.store(engine.status().rt.thread.tid, Relaxed);
    let attached = || engine.status().inputs[0].state == InputState::Attached;
    wait_for(Duration::from_secs(5), "input attached", attached);
    wait_for(Duration::from_secs(5), "output realised", || {
        engine.status().outputs[0].realised
    });

    // Releasing the producer deletes the flow: the input turns invalid and
    // is pruned, a new generation without it is swapped in.
    drop(pilot);
    wait_for(Duration::from_secs(5), "input pruned", || !attached());
    let api = common::api();
    let index = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api));
    engine.set_gain(0, 0, 0.25, (index / B + 20) * B).unwrap();
    // The same id again: polled, re-added in a new generation, faded in.
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    wait_for(Duration::from_secs(10), "input re-added", attached);
    std::thread::sleep(Duration::from_millis(500));

    let c = engine.counters();
    assert!(c.input_silenced_blocks.load(Relaxed) > 0);
    assert!(c.heartbeat_blocks.load(Relaxed) > 1_000);
    let verdicts: Vec<_> = rx.try_iter().collect();
    assert!(verdicts.iter().any(|v| v.fading != 0), "no fade-in seen");
    assert!(verdicts.iter().any(|v| v.ramping), "no gain ramp seen");
    let calls = RT_CALLS.load(Relaxed);
    assert!(
        calls == 0,
        "{calls} allocator calls on the RT thread, the first from:\n{}",
        FIRST_RT_CALL
            .get()
            .map_or_else(|| "not captured".into(), ToString::to_string)
    );
    common::stop(engine);
}
