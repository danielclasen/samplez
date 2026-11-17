//! Instruction-count gates on the DSP of one block, run under Callgrind
//! by Gungraun (`cargo bench --bench rt`). Each benchmark processes one
//! block of B = 48 samples at the largest shape measured here, 64 input
//! channels to 96 output channels, on both dispatch paths, with every
//! coefficient ramping; the strips, 64 mono ones, and the meters of the
//! 64 input channels after them have one path. It also
//! gates the RT parts of a resync at that shape: the matrix copy of the
//! core and one block's history stores. The hard limits are the
//! instruction counts measured in the builder image plus 10 %, rounded
//! up; a run above a limit fails.

use std::hint::black_box;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez_dsp::strip::DEFAULT_BANDS;
use samplez_dsp::{
    CompParams, EqBand, GateParams, History, KWeighting, Matrix, Path, PlanarBuffer, Started,
    Strip, StripParam, TruePeak, accumulate,
};

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

// The input meters of one block, each input channel after its strip:
// sample peak, sum of squares and K-weighting, as the RT loop meters
// every input leg.
#[library_benchmark]
#[bench::block(setup = setup_input_meters, config = limit(INPUT_METERS))]
fn input_meters(
    (mut kw, mut peak, mut power, buf): (KWeighting, Vec<f32>, Vec<f64>, PlanarBuffer),
) -> (KWeighting, Vec<f32>, Vec<f64>, PlanarBuffer) {
    for c in 0..INPUTS {
        let x = black_box(buf.channel(c));
        accumulate(x, &mut peak[c], &mut power[c]);
        power[c] += kw.process(c, x);
    }
    (kw, peak, power, buf)
}

fn setup_input_meters() -> (KWeighting, Vec<f32>, Vec<f64>, PlanarBuffer) {
    (
        KWeighting::new(INPUTS),
        vec![0.0; INPUTS],
        vec![0.0; INPUTS],
        signal(INPUTS),
    )
}

/// 64 mono strips with trim, high-pass, every band, gate and compressor
/// on at an attack of `attack_ms`, the signal above both thresholds, all
/// of them ramping (the costliest block) or all settled. An attack under
/// 1 ms runs the gain computers every sample.
fn strips(ramping: bool, attack_ms: f32) -> (Vec<Strip>, PlanarBuffer) {
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
            StripParam::Gate(GateParams {
                enabled: true,
                threshold_db: -60.0,
                range_db: 40.0,
                attack_ms,
                release_ms: 50.0,
            }),
            StripParam::Comp(CompParams {
                enabled: true,
                threshold_db: -20.0,
                ratio: 4.0,
                attack_ms,
                release_ms: 50.0,
                knee_db: 6.0,
                makeup_db: 3.0,
            }),
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
#[bench::ramping(args = (true, 1.0), setup = strips, config = limit(STRIPS_RAMPING))]
#[bench::settled(args = (false, 1.0), setup = strips, config = limit(STRIPS_SETTLED))]
#[bench::ramping_fast_attack(args = (true, 0.1), setup = strips, config = limit(STRIPS_RAMPING_FAST))]
#[bench::settled_fast_attack(args = (false, 0.1), setup = strips, config = limit(STRIPS_SETTLED_FAST))]
fn strip((mut strips, mut buf): (Vec<Strip>, PlanarBuffer)) -> (Vec<Strip>, PlanarBuffer) {
    for (c, s) in strips.iter_mut().enumerate() {
        s.process(black_box(buf.channel_mut(c)), B);
    }
    (strips, buf)
}

// The matrix part of the RT thread's core copy for a resync: every
// coefficient and its ramp into a buffer of the same shape.
#[library_benchmark]
#[bench::full(setup = setup_copy, config = limit(CORE_COPY))]
fn core_copy((mut dst, src): (Matrix, Matrix)) -> (Matrix, Matrix) {
    dst.copy_from(black_box(&src));
    (dst, src)
}

fn setup_copy() -> (Matrix, Matrix) {
    (Matrix::new(INPUTS, OUTPUTS), ramping_matrix().0)
}

// One block's history stores: the started block, then one ring per input
// channel of 100 ms of delay and 50 ms of slack, with scalar relaxed
// stores.
#[library_benchmark]
#[bench::block(setup = setup_history, config = limit(HISTORY_BLOCK))]
fn history(
    (started, rings, buf): (Started, Vec<History>, PlanarBuffer),
) -> (Started, Vec<History>, PlanarBuffer) {
    started.begin(black_box(4000));
    for (c, ring) in rings.iter().enumerate() {
        ring.write(4000 * B as u64, black_box(buf.channel(c)));
    }
    (started, rings, buf)
}

fn setup_history() -> (Started, Vec<History>, PlanarBuffer) {
    let rings = (0..INPUTS).map(|_| History::new(4800, 50, B)).collect();
    (Started::new(), rings, signal(INPUTS))
}

const CORE_COPY: u64 = 26_000;
const HISTORY_BLOCK: u64 = 45_000;
const MATRIX_SCALAR: u64 = 8_306_000;
const MATRIX_AVX2: u64 = 1_051_000;
const METERS_SCALAR: u64 = 1_118_000;
const METERS_AVX2: u64 = 303_000;
const INPUT_METERS: u64 = 152_623;
const STRIPS_RAMPING: u64 = 1_502_708;
const STRIPS_SETTLED: u64 = 979_919;
const STRIPS_RAMPING_FAST: u64 = 1_771_069;
const STRIPS_SETTLED_FAST: u64 = 1_273_161;

library_benchmark_group!(
    name = block,
    benchmarks = [matrix, meters, input_meters, strip]
);
library_benchmark_group!(name = transfer, benchmarks = [core_copy, history]);

main!(library_benchmark_groups = block, transfer);
