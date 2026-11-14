//! The RT functions make no Rust allocation. The counting allocator sees
//! only allocations through Rust's global allocator; libmxl's C++ code
//! allocates through its own operator new and malloc, which this test does
//! not see.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use common::{B, Domain, RawWriter, TIMING, api, input, new_id};
use mxl_sys::{MXL_ERR_OUT_OF_RANGE_TOO_EARLY, MXL_STATUS_OK};
use samplez_mxl::input::{Generation, Outcome, SlotState};
use samplez_mxl::output::{self, Creation, OutputConfig, Write};
use samplez_mxl::{Instance, time};

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: forwards to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|a| a.set(a.get() + 1));
        // SAFETY: the caller's contract is passed through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed through.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.with(|a| a.set(a.get() + 1));
        // SAFETY: the caller's contract is passed through.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

#[test]
fn rt_functions_do_not_allocate() {
    const K0: u64 = 4_000_000;
    let api = api();
    let domain = Domain::new();
    let (src, dst_id) = (new_id(), new_id());
    let mut w = RawWriter::create(&api, &domain.0, &src, 2);
    w.blocks(K0..K0 + 300);
    let mut g = Generation::new(&api, &domain.0, 1).unwrap();
    g.attach(0, &input(&src, 2, 0), &TIMING).unwrap();
    let out_instance = Instance::new(&api, &domain.0).unwrap();
    let cfg = OutputConfig {
        flow_id: &dst_id,
        label: "no-alloc",
        group: "samplez-test",
        role: "Audio",
        channels: 2,
        block: B,
    };
    let Creation::Created {
        writer: mut out, ..
    } = output::create(&out_instance, &cfg).unwrap()
    else {
        panic!("output not created");
    };
    let n = 2 * B as usize;
    let mut buf = vec![0.0f32; n];
    let timeout = MXL_ERR_OUT_OF_RANGE_TOO_EARLY;
    let mut counts = [0u64; 7];

    let before = allocations();
    let r = g.input(0).unwrap();
    for k in K0 + 200..K0 + 299 {
        let mut s = SlotState::default();
        let now = time::now(&api);
        let due = time::newest_due_block(&api, now, B, TIMING.margin).unwrap();
        let ts = time::group_timestamp(&api, k, B, g.max_offset());
        let t = time::deadline(&api, due, B, TIMING.margin);
        counts[0] +=
            u64::from(time::timestamp_to_index(&api, t) <= time::timestamp_to_index(&api, now));
        let status = g.wait(ts, 0);
        assert_eq!(status, MXL_STATUS_OK);
        counts[1] += u64::from(r.read(&mut s, k, status, &mut buf) == Outcome::Copy);
        counts[2] += u64::from(r.read(&mut s, k, status, &mut buf) == Outcome::BelowLastRead);
        let o = r.read(&mut s, k + 200, timeout, &mut buf);
        counts[3] += u64::from(o == Outcome::Late);
        counts[4] += u64::from(!r.identify(&mut s, timeout, o));
        let mut old = SlotState::default();
        counts[6] += u64::from(r.read(&mut old, k - 250, timeout, &mut buf) == Outcome::TooLate);
        assert_eq!(out.write_block(k, &buf), Write::Committed);
        counts[5] += u64::from(out.write_block(k - 1, &buf) == Write::Held);
        time::sleep_until(&api, 0);
    }
    let after = allocations();

    assert_eq!(after - before, 0);
    assert_eq!(counts, [99; 7]);
}
