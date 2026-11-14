//! The process functions do not allocate. A counting global allocator
//! counts allocations made on the test thread while they run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use samplez_dsp::{Matrix, Meters, Path, PlanarBuffer, Ramp, restore_mxcsr, set_ftz_daz};

#[path = "../src/require.rs"]
mod require;

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: forwards to the system allocator unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|c| c.set(c.get() + 1));
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.with(|c| c.set(c.get() + 1));
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

#[test]
fn process_calls_do_not_allocate() {
    let (n, m, cap) = (64, 64, 1024);
    let mut input = PlanarBuffer::new(n, cap);
    for ch in 0..n {
        for (k, s) in input.channel_mut(ch).iter_mut().enumerate() {
            *s = ((ch * 31 + k * 7) % 97) as f32 / 97.0 - 0.5;
        }
    }
    let mut output = PlanarBuffer::new(m, cap);
    let mut meters = Meters::new(m);
    let mut ramp = Ramp::new(1.0);
    let paths: Vec<Path> = Path::ALL
        .into_iter()
        .filter(|&p| require::path_runs("process_calls_do_not_allocate", p))
        .collect();
    // Every other matrix is a clone of a sparse one, so set() on a clone
    // has to fill columns beyond the clone's length.
    let mut sparse = Matrix::new(n, m);
    assert!(sparse.set(1, 0, 0.5, 0));
    let mut runs: Vec<(Path, Matrix)> = paths
        .iter()
        .flat_map(|&p| [(p, Matrix::new(n, m)), (p, sparse.clone())])
        .collect();
    let mut scratch = vec![0.0f32; cap];

    let before = allocations();
    let saved = set_ftz_daz(true);
    for round in 0..8usize {
        let frames = 16 + round * 131;
        for (path, matrix) in &mut runs {
            // Fill every column, ramp some, route some, then empty them, so
            // every insert and removal of a coefficient is covered.
            for j in 0..m {
                for i in 0..n {
                    let c = match (round + i + j) % 4 {
                        0 => 0.0,
                        1 => 1.0,
                        _ => 0.5,
                    };
                    let len = if (i + j) % 3 == 0 { 0 } else { 300 };
                    assert!(matrix.set(i, j, if round == 7 { 0.0 } else { c }, len));
                }
            }
            assert!(matrix.set(0, 1, 1.0, 0));
            matrix.process_on(*path, &input, &mut output, frames);
            meters.process(&output, frames);
        }
        assert!(ramp.set_target(0.25 * round as f32, 100));
        ramp.process(&mut scratch[..frames]);
    }
    restore_mxcsr(saved);
    let during = allocations() - before;

    assert_eq!(during, 0, "allocations during process calls on {paths:?}");
    assert!(meters.frames() > 0);
}
