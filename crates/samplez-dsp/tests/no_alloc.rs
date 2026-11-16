//! The process functions do not allocate. A counting global allocator
//! counts allocations made on the test thread while they run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use samplez_dsp::strip::DEFAULT_BANDS;
use samplez_dsp::{
    BandType, EqBand, KWeighting, Loudness, Matrix, Meters, Path, PlanarBuffer, Ramp, Strip,
    StripParam, TruePeak, restore_mxcsr, set_ftz_daz,
};

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
    let mut true_peak = TruePeak::new(m, cap);
    let mut kweighting = KWeighting::new(m);
    let mut loudness = Loudness::new();
    let mut slice = 0;
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
    let mut strips: Vec<Strip> = [1, 2, 6, 8].map(|legs| Strip::new(legs, 480)).into();
    let mut strip_buf = vec![0.0f32; 8 * cap];

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
            let mut power = 0.0;
            for ch in 0..m {
                let x = &output.channel(ch)[..frames];
                assert!(true_peak.process_on(*path, ch, x).is_finite());
                power += kweighting.process(ch, x);
            }
            loudness.push(slice, Some(power / frames as f64));
            slice += 1;
        }
        assert!(ramp.set_target(0.25 * round as f32, 100));
        ramp.process(&mut scratch[..frames]);
        // Every parameter of every strip, set and processed while the
        // ramps run, and a clone into an existing strip.
        let on = round % 2 == 0;
        for s in &mut strips {
            let last = s.legs() - 1;
            let gain_db = if on { 12.0 } else { -6.0 };
            for p in [
                StripParam::Trim { db: gain_db },
                StripParam::LegTrim {
                    leg: last,
                    db: -gain_db,
                },
                StripParam::Polarity {
                    leg: last,
                    inverted: on,
                },
                StripParam::Hpf {
                    enabled: on,
                    freq: 120.0,
                },
                StripParam::Eq { enabled: true },
            ] {
                assert!(s.set(p));
            }
            // Refused: a leg and a band the strip lacks, a value out of
            // range.
            for p in [
                StripParam::LegTrim {
                    leg: s.legs(),
                    db: 0.0,
                },
                StripParam::EqBand {
                    band: DEFAULT_BANDS.len(),
                    value: DEFAULT_BANDS[0],
                },
                StripParam::Trim { db: f32::NAN },
            ] {
                assert!(!s.set(p));
            }
            for (band, b) in DEFAULT_BANDS.iter().enumerate() {
                let value = EqBand {
                    kind: if on { b.kind } else { BandType::Peak },
                    gain_db,
                    ..*b
                };
                assert!(s.set(StripParam::EqBand { band, value }));
            }
            let x = &mut strip_buf[..s.legs() * frames];
            for (n, v) in x.iter_mut().enumerate() {
                *v = ((n * 7) % 97) as f32 / 97.0 - 0.5;
            }
            s.process(x, frames);
            s.advance(frames);
        }
        let first = strips[0].clone();
        strips[0].clone_from(&first);
    }
    restore_mxcsr(saved);
    let during = allocations() - before;

    assert_eq!(during, 0, "allocations during process calls on {paths:?}");
    assert!(meters.frames() > 0);
}
