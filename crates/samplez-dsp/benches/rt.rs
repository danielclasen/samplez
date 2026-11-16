//! Instruction-count gates on the DSP of one block, run under Callgrind
//! by Gungraun (`cargo bench --bench rt`). Each benchmark processes one
//! block of B = 48 samples at the largest shape measured here, 64 input
//! channels to 96 output channels, on both dispatch paths, with every
//! coefficient ramping; the strips, 64 mono ones, have one path. The
//! hard limits are the instruction counts measured in the builder image
//! plus 10 %, rounded up; a run above a limit fails.

use std::hint::black_box;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez_dsp::strip::DEFAULT_BANDS;
use samplez_dsp::{EqBand, KWeighting, Matrix, Path, PlanarBuffer, Strip, StripParam, TruePeak};

const B: usize = 48;
const INPUTS: usize = 64;
const OUTPUTS: usize = 96;

fn limit(ir: u64) -> LibraryBenchmarkConfig {
    let mut c = LibraryBenchmarkConfig::default();
    c.tool(Callgrind::default().hard_limits([(EventKind::Ir, ir)]));
    c
}

fn signal(channels: usize) -> PlanarBuffer {
    let mut buf = PlanarBuffer::new(channels, B);
    for c in 0..channels {
        for (n, s) in buf.channel_mut(c).iter_mut().enumerate() {
            *s = ((n * 7919 + c * 31) % 1013) as f32 / 1013.0 - 0.5;
        }
    }
    buf
}

/// A full matrix whose every coefficient ramps from 1 to 0.5 over 480
/// samples, with its input and output buffers.
fn ramping_matrix() -> (Matrix, PlanarBuffer, PlanarBuffer) {
    let mut m = Matrix::new(INPUTS, OUTPUTS);
    for i in 0..INPUTS {
        for j in 0..OUTPUTS {
            assert!(m.set(i, j, 1.0, 0));
            assert!(m.set(i, j, 0.5, 480));
        }
    }
    (m, signal(INPUTS), PlanarBuffer::new(OUTPUTS, B))
}

#[library_benchmark]
#[bench::scalar(args = (Path::Scalar,), setup = setup_matrix, config = limit(MATRIX_SCALAR))]
#[bench::avx2(args = (Path::Avx2,), setup = setup_matrix, config = limit(MATRIX_AVX2))]
fn matrix(
    (path, (mut m, input, mut output)): (Path, (Matrix, PlanarBuffer, PlanarBuffer)),
) -> (Matrix, PlanarBuffer, PlanarBuffer) {
    m.process_on(path, black_box(&input), &mut output, B);
    (m, input, output)
}

fn setup_matrix(path: Path) -> (Path, (Matrix, PlanarBuffer, PlanarBuffer)) {
    (path, ramping_matrix())
}

// The output meters of one block: true peak and K-weighting over every
// output channel.
#[library_benchmark]
#[bench::scalar(args = (Path::Scalar,), setup = setup_meters, config = limit(METERS_SCALAR))]
#[bench::avx2(args = (Path::Avx2,), setup = setup_meters, config = limit(METERS_AVX2))]
fn meters(
    (path, mut tp, mut kw, buf): (Path, TruePeak, KWeighting, PlanarBuffer),
) -> (f32, f64, TruePeak, KWeighting, PlanarBuffer) {
    let (mut peak, mut power) = (0.0f32, 0.0f64);
    for c in 0..OUTPUTS {
        let x = black_box(buf.channel(c));
        peak = peak.max(tp.process_on(path, c, x));
        power += kw.process(c, x);
    }
    (peak, power, tp, kw, buf)
}

fn setup_meters(path: Path) -> (Path, TruePeak, KWeighting, PlanarBuffer) {
    (
        path,
        TruePeak::new(OUTPUTS, B),
        KWeighting::new(OUTPUTS),
        signal(OUTPUTS),
    )
}

/// 64 mono strips with trim, high-pass and every band on, all of them
/// ramping (the costliest block) or all settled.
fn strips(ramping: bool) -> (Vec<Strip>, PlanarBuffer) {
    let mut v = Vec::new();
    for _ in 0..INPUTS {
        let mut s = Strip::new(1, if ramping { 480 } else { 0 });
        let mut params = vec![
            StripParam::Trim { db: 6.0 },
            StripParam::Hpf {
                enabled: true,
                freq: 80.0,
            },
            StripParam::Eq { enabled: true },
        ];
        for (band, b) in DEFAULT_BANDS.iter().enumerate() {
            let value = EqBand { gain_db: 6.0, ..*b };
            params.push(StripParam::EqBand { band, value });
        }
        for p in params {
            assert!(s.set(p));
        }
        v.push(s);
    }
    (v, signal(INPUTS))
}

#[library_benchmark]
#[bench::ramping(args = (true,), setup = strips, config = limit(STRIPS_RAMPING))]
#[bench::settled(args = (false,), setup = strips, config = limit(STRIPS_SETTLED))]
fn strip((mut strips, mut buf): (Vec<Strip>, PlanarBuffer)) -> (Vec<Strip>, PlanarBuffer) {
    for (c, s) in strips.iter_mut().enumerate() {
        s.process(black_box(buf.channel_mut(c)), B);
    }
    (strips, buf)
}

const MATRIX_SCALAR: u64 = 8_306_000;
const MATRIX_AVX2: u64 = 1_051_000;
const METERS_SCALAR: u64 = 1_118_000;
const METERS_AVX2: u64 = 303_000;
const STRIPS_RAMPING: u64 = 998_000;
const STRIPS_SETTLED: u64 = 510_000;

library_benchmark_group!(name = block, benchmarks = [matrix, meters, strip]);

main!(library_benchmark_groups = block);
