//! Instruction-count gates on the DSP of one block, run under Callgrind
//! by Gungraun (`cargo bench --bench rt`). Each benchmark processes one
//! block of B = 48 samples at the largest shape measured here, 64 input
//! channels to 96 output channels, on both dispatch paths, with every
//! coefficient ramping, undocked and ducked; the strips, 64 mono ones at full, half and no compressor mix and with the compressor off, the meters of the 64
//! input channels after them and eight duck detectors keyed by them have
//! one path, metered as the RT loop runs them, and idle as an engine with
//! no duck group in use runs them; so does the gap decision of the
//! meters of 48 stereo outputs with half the inputs down, and so do limiters on 96 output
//! channels as 48 stereo outputs, limiting and off, 64 mono input
//! delays, settled and crossfading, and a tone generator of eight legs,
//! settled, crossfading and in an ident cut's edge. It also gates the RT parts of a resync
//! at that shape: the matrix and detector copies of the core and one
//! block's history stores. The hard limits are the
//! instruction counts measured in the builder image plus 10 %, rounded
//! up; a run above a limit fails.

use std::hint::black_box;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez_dsp::duck::MAX_KEYS;
use samplez_dsp::strip::DEFAULT_BANDS;
use samplez_dsp::{
    CompParams, Delay, DelayRings, Detector, DetectorParams, DuckMode, DuckParam, EqBand,
    GateParams, History, KWeighting, KeyFilter, Limiter, LimiterParams, MAX_DUCKS, Matrix, Path,
    PlanarBuffer, Started, Strip, StripParam, Tone, ToneParams, TruePeak, accumulate,
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

/// A full matrix whose every coefficient waits 480 samples, then ramps
/// from 1 to 0.5 over 480: its block is the delayed-fade path, through
/// the hold and across the start of the ramp.
fn delayed_matrix() -> (Matrix, PlanarBuffer, PlanarBuffer) {
    let mut m = Matrix::new(INPUTS, OUTPUTS);
    for i in 0..INPUTS {
        for j in 0..OUTPUTS {
            assert!(m.set(i, j, 1.0, 0));
            assert!(m.set_fade(i, j, 0.5, B as u32 / 2, 480));
        }
    }
    (m, signal(INPUTS), PlanarBuffer::new(OUTPUTS, B))
}

#[library_benchmark]
#[bench::scalar(args = (Path::Scalar,), setup = setup_delayed, config = limit(DELAYED_SCALAR))]
#[bench::avx2(args = (Path::Avx2,), setup = setup_delayed, config = limit(DELAYED_AVX2))]
fn matrix_delayed(
    (path, (mut m, input, mut output)): (Path, (Matrix, PlanarBuffer, PlanarBuffer)),
) -> (Matrix, PlanarBuffer, PlanarBuffer) {
    m.process_on(path, black_box(&input), &mut output, B);
    (m, input, output)
}

fn setup_delayed(path: Path) -> (Path, (Matrix, PlanarBuffer, PlanarBuffer)) {
    (path, delayed_matrix())
}

/// The ramping matrix with every coefficient ducked, their depths
/// ramping and every mask crossfading, with the detectors' envelopes
/// moving: by all eight detectors, or, when `varied`, by a mask of its
/// own per output, so no two outputs of an input share one.
fn ducked_matrix(varied: bool) -> (Matrix, PlanarBuffer, PlanarBuffer, PlanarBuffer) {
    let (mut m, input, output) = ramping_matrix();
    for i in 0..INPUTS {
        for g in 0..MAX_DUCKS {
            assert!(m.set_depth(i, g, 10.0 + g as f32, 0));
            assert!(m.set_depth(i, g, 20.0, 480));
        }
        for j in 0..OUTPUTS {
            let mask = if varied { 1 + j as u8 } else { 0xff };
            assert!(m.set_mask(i, j, 0x0f, 0));
            assert!(m.set_mask(i, j, mask, 480));
        }
    }
    let mut env = PlanarBuffer::new(MAX_DUCKS, B);
    for g in 0..MAX_DUCKS {
        for (n, e) in env.channel_mut(g).iter_mut().enumerate() {
            *e = (n + g) as f32 / (B + MAX_DUCKS) as f32;
        }
    }
    (m, input, env, output)
}

#[library_benchmark]
#[bench::scalar(args = (Path::Scalar, false), setup = setup_ducked, config = limit(DUCKED_SCALAR))]
#[bench::avx2(args = (Path::Avx2, false), setup = setup_ducked, config = limit(DUCKED_AVX2))]
#[bench::scalar_varied(args = (Path::Scalar, true), setup = setup_ducked, config = limit(VARIED_SCALAR))]
#[bench::avx2_varied(args = (Path::Avx2, true), setup = setup_ducked, config = limit(VARIED_AVX2))]
fn matrix_ducked(
    (path, (mut m, input, env, mut output)): (
        Path,
        (Matrix, PlanarBuffer, PlanarBuffer, PlanarBuffer),
    ),
) -> (Matrix, PlanarBuffer, PlanarBuffer, PlanarBuffer) {
    m.process_ducked_on(path, black_box(&input), black_box(&env), &mut output, B);
    (m, input, env, output)
}

fn setup_ducked(
    path: Path,
    varied: bool,
) -> (Path, (Matrix, PlanarBuffer, PlanarBuffer, PlanarBuffer)) {
    (path, ducked_matrix(varied))
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

// The true peak of the input meters of one block, every input channel
// opted in (an input's `truePeak`): the cost an opt-in adds to
// `input_meters` per channel.
#[library_benchmark]
#[bench::avx2(args = (Path::Avx2,), setup = setup_input_true_peak, config = limit(INPUT_TRUE_PEAK_AVX2))]
fn input_true_peak(
    (path, mut tp, buf): (Path, TruePeak, PlanarBuffer),
) -> (f32, TruePeak, PlanarBuffer) {
    let mut peak = 0.0f32;
    for c in 0..INPUTS {
        peak = peak.max(tp.process_on(path, c, black_box(buf.channel(c))));
    }
    (peak, tp, buf)
}

fn setup_input_true_peak(path: Path) -> (Path, TruePeak, PlanarBuffer) {
    (path, TruePeak::new(INPUTS, B), signal(INPUTS))
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

// The gap decision of one block, as the RT loop takes it for each output's
// meter: whether an input slot down by the log feeds one of its columns.
// The even ones of 64 mono slots are down and feed nothing, the odd ones
// feed every one of 48 stereo outputs: no slice is a gap, and every slot
// down is looked up in every column.
#[library_benchmark]
#[bench::half_down(setup = setup_meter_gaps, config = limit(METER_GAPS))]
fn meter_gaps((m, down): (Matrix, u64)) -> (Matrix, u64) {
    let mut gaps = 0;
    for meter in 0..OUTPUTS / 2 {
        let rows = || {
            let mut rest = black_box(down);
            std::iter::from_fn(move || {
                (rest != 0).then(|| {
                    let slot = rest.trailing_zeros() as usize;
                    rest &= rest - 1;
                    slot..slot + 1
                })
            })
        };
        let feeds = (2 * meter..2 * meter + 2).any(|c| m.feeds(rows(), c));
        gaps += u64::from(black_box(feeds));
    }
    (m, gaps)
}

fn setup_meter_gaps() -> (Matrix, u64) {
    let mut m = Matrix::new(INPUTS, OUTPUTS);
    for i in (1..INPUTS).step_by(2) {
        for j in 0..OUTPUTS {
            assert!(m.set(i, j, 0.5, 0));
        }
    }
    (m, 0x5555_5555_5555_5555)
}

/// 64 mono strips with trim, high-pass, every band, gate and compressor
/// on at an attack of `attack_ms` and a mix of `mix`, or the compressor
/// off at none, the signal above both thresholds, all of them ramping
/// (the costliest block) or all settled. An attack under 1 ms runs the
/// gain computers every sample.
fn strips(ramping: bool, attack_ms: f32, mix: Option<f32>) -> (Vec<Strip>, PlanarBuffer) {
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
                enabled: mix.is_some(),
                threshold_db: -20.0,
                ratio: 4.0,
                attack_ms,
                release_ms: 50.0,
                knee_db: 6.0,
                makeup_db: 3.0,
                mix: mix.unwrap_or(1.0),
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
#[bench::ramping(args = (true, 1.0, Some(1.0)), setup = strips, config = limit(STRIPS_RAMPING))]
#[bench::settled(args = (false, 1.0, Some(1.0)), setup = strips, config = limit(STRIPS_SETTLED))]
#[bench::ramping_fast_attack(args = (true, 0.1, Some(1.0)), setup = strips, config = limit(STRIPS_RAMPING_FAST))]
#[bench::settled_fast_attack(args = (false, 0.1, Some(1.0)), setup = strips, config = limit(STRIPS_SETTLED_FAST))]
// A compressor mix below full costs what the compressor alone does at
// most; at 0 the gain still follows the key.
#[bench::ramping_half_mix(args = (true, 1.0, Some(0.5)), setup = strips, config = limit(STRIPS_RAMPING))]
#[bench::settled_half_mix(args = (false, 1.0, Some(0.5)), setup = strips, config = limit(STRIPS_SETTLED))]
#[bench::ramping_no_mix(args = (true, 1.0, Some(0.0)), setup = strips, config = limit(STRIPS_RAMPING))]
#[bench::settled_no_mix(args = (false, 1.0, Some(0.0)), setup = strips, config = limit(STRIPS_SETTLED))]
#[bench::settled_comp_off(args = (false, 1.0, None), setup = strips, config = limit(STRIPS_SETTLED))]
fn strip((mut strips, mut buf): (Vec<Strip>, PlanarBuffer)) -> (Vec<Strip>, PlanarBuffer) {
    for (c, s) in strips.iter_mut().enumerate() {
        s.process(black_box(buf.channel_mut(c)), B);
    }
    (strips, buf)
}

/// 48 stereo limiters over the 96 output channels, on with the signal
/// above their ceiling, or off, which only delays; each run on a block
/// copied into a scratch buffer and back, as the RT loop runs them.
fn limiters(on: bool) -> (Vec<Limiter>, PlanarBuffer, Vec<f32>) {
    let mut v = Vec::new();
    for _ in 0..OUTPUTS / 2 {
        let mut l = Limiter::new(2, 0);
        let params = LimiterParams {
            enabled: on,
            ceiling_db: -12.0,
            release_ms: 50.0,
        };
        assert!(l.set(params));
        v.push(l);
    }
    let mut buf = signal(OUTPUTS);
    let mut scratch = vec![0.0; 2 * B];
    // A block before, so the one measured is the next one.
    for l in &mut v {
        l.process(0, &mut scratch, B);
    }
    for c in 0..OUTPUTS {
        for s in buf.channel_mut(c) {
            *s *= 2.0;
        }
    }
    (v, buf, scratch)
}

#[library_benchmark]
#[bench::on(args = (true,), setup = limiters, config = limit(LIMITERS_ON))]
#[bench::off(args = (false,), setup = limiters, config = limit(LIMITERS_OFF))]
fn limiter(
    (mut limiters, mut buf, mut scratch): (Vec<Limiter>, PlanarBuffer, Vec<f32>),
) -> (Vec<Limiter>, PlanarBuffer, Vec<f32>) {
    for (o, l) in limiters.iter_mut().enumerate() {
        for (c, leg) in scratch.chunks_exact_mut(B).enumerate() {
            leg.copy_from_slice(&buf.channel(2 * o + c)[..B]);
        }
        l.process(B as u64, black_box(&mut scratch), B);
        for (c, leg) in scratch.chunks_exact(B).enumerate() {
            buf.channel_mut(2 * o + c)[..B].copy_from_slice(leg);
        }
    }
    (limiters, buf, scratch)
}

/// 64 mono delays of 100 ms, settled or crossfading to 50 ms.
fn delays(fading: bool) -> (Vec<Delay>, PlanarBuffer) {
    let mut v = Vec::new();
    for _ in 0..INPUTS {
        let mut d = Delay::new(DelayRings::new(1, 9600, 2, B), 480);
        assert!(d.set(0, 4800));
        let mut block = vec![0.0; B];
        // Up to the block measured.
        for k in 0..40 {
            d.process(k * B as u64, &mut block, B);
        }
        if fading {
            assert!(d.set(1920, 2400));
        }
        v.push(d);
    }
    (v, signal(INPUTS))
}

#[library_benchmark]
#[bench::settled(args = (false,), setup = delays, config = limit(DELAYS_SETTLED))]
#[bench::fading(args = (true,), setup = delays, config = limit(DELAYS_FADING))]
fn delay((mut delays, mut buf): (Vec<Delay>, PlanarBuffer)) -> (Vec<Delay>, PlanarBuffer) {
    for (c, d) in delays.iter_mut().enumerate() {
        d.process(1920, black_box(buf.channel_mut(c)), B);
    }
    (delays, buf)
}

/// The tone generator's block: the last 48 samples of ident window 9001
/// of 12000 samples, in which leg 1 is cut while leg 2 fades out before
/// its own cut.
const TONE_AT: u64 = 9001 * 12_000 + 11_952;

/// A generator of eight legs at 48 kHz, 997 Hz at -18 dBFS: settled
/// (case 0); crossfading to 440 Hz with its level ramping to -6 dBFS
/// (case 1); with the ident on, in a cut's fading edge (case 2).
fn tones(case: u8) -> (Tone, Vec<f32>) {
    let mut t = Tone::new(8, 48_000, 480);
    let p = ToneParams {
        frequency_hz: 997,
        level_db: -18.0,
        ident: case == 2,
    };
    assert!(t.set(0, p));
    if case == 1 {
        let moved = ToneParams {
            frequency_hz: 440,
            level_db: -6.0,
            ..p
        };
        assert!(t.set(TONE_AT - 96, moved));
    }
    (t, vec![0.0; 8 * B])
}

#[library_benchmark]
#[bench::settled(args = (0,), setup = tones, config = limit(TONE_SETTLED))]
#[bench::crossfading(args = (1,), setup = tones, config = limit(TONE_CROSSFADING))]
#[bench::ident(args = (2,), setup = tones, config = limit(TONE_IDENT))]
fn tone((mut t, mut buf): (Tone, Vec<f32>)) -> (Tone, Vec<f32>) {
    t.process(black_box(TONE_AT), &mut buf, B);
    (t, buf)
}

/// Eight detectors, each keyed by eight of the 64 mono inputs through
/// both key filters, the low-pass ramping on, the signal around the
/// threshold.
fn detectors() -> (Vec<Detector>, PlanarBuffer, Vec<f32>) {
    let params = DetectorParams {
        threshold_db: -12.0,
        lpf: KeyFilter {
            enabled: true,
            freq: 4000.0,
        },
        ..DetectorParams::DEFAULT
    };
    let mut v = Vec::new();
    for _ in 0..INPUTS / MAX_KEYS {
        let mut d = Detector::new(480);
        assert!(d.set(DuckParam::Detector(params)));
        for key in 0..MAX_KEYS {
            assert!(d.set(DuckParam::Key {
                key,
                legs: 1,
                gain: 1.0,
            }));
        }
        v.push(d);
    }
    (v, signal(INPUTS), vec![0.0; B])
}

#[library_benchmark]
#[bench::block(setup = detectors, config = limit(DUCK))]
fn duck(
    (mut detectors, buf, mut envelope): (Vec<Detector>, PlanarBuffer, Vec<f32>),
) -> (Vec<Detector>, PlanarBuffer, Vec<f32>) {
    for (g, d) in detectors.iter_mut().enumerate() {
        d.process(
            B,
            |k, _| black_box(buf.channel(g * MAX_KEYS + k)),
            &mut envelope,
        );
    }
    (detectors, buf, envelope)
}

/// The eight detectors at full depth, held there by mode On at the
/// longest hold and release, then switched to mode Off.
fn bypassed() -> Vec<Detector> {
    let params = DetectorParams {
        hold_ms: 5000.0,
        release_ms: 10_000.0,
        ..DetectorParams::DEFAULT
    };
    let mut v = detectors().0;
    for d in &mut v {
        assert!(d.set(DuckParam::Detector(params)));
        assert!(d.set(DuckParam::Mode(DuckMode::On)));
        d.advance(48_000);
        assert!(d.envelope() == 1.0);
        assert!(d.set(DuckParam::Mode(DuckMode::Off)));
    }
    v
}

// A skip of 15 s, the longest release and more, over detectors their
// manual controls decide: it costs the same whatever its length.
#[library_benchmark]
#[bench::skip(setup = bypassed, config = limit(DUCK_ADVANCE))]
fn duck_advance(mut detectors: Vec<Detector>) -> Vec<Detector> {
    for d in &mut detectors {
        d.advance(black_box(15 * 48_000));
    }
    detectors
}

// The duck stage as the RT loop runs it: the eight keyed detectors
// metered, each returning its largest key.
#[library_benchmark]
#[bench::block(setup = detectors, config = limit(DUCK_METERED))]
fn duck_metered(
    (mut detectors, buf, mut envelope): (Vec<Detector>, PlanarBuffer, Vec<f32>),
) -> (Vec<Detector>, PlanarBuffer, Vec<f32>) {
    for (g, d) in detectors.iter_mut().enumerate() {
        black_box(d.process_metered(
            B,
            |k, _| black_box(buf.channel(g * MAX_KEYS + k)),
            &mut envelope,
        ));
    }
    (detectors, buf, envelope)
}

// The duck stage of an engine with no group in use: eight idle
// detectors advanced over a block and their envelopes written 0.
#[library_benchmark]
#[bench::block(setup = idle, config = limit(DUCK_IDLE))]
fn duck_idle(
    (mut detectors, mut envelopes): (Vec<Detector>, PlanarBuffer),
) -> (Vec<Detector>, PlanarBuffer) {
    for (g, d) in detectors.iter_mut().enumerate() {
        if black_box(d.is_idle()) {
            d.advance(B);
            envelopes.channel_mut(g)[..B].fill(0.0);
        }
    }
    (detectors, envelopes)
}

fn idle() -> (Vec<Detector>, PlanarBuffer) {
    let v = vec![Detector::new(480); MAX_DUCKS];
    assert!(v.iter().all(Detector::is_idle));
    (v, PlanarBuffer::new(MAX_DUCKS, B))
}

// The detector part of the RT thread's core copy: the eight keyed
// detectors mid-envelope into a buffer of the same length.
#[library_benchmark]
#[bench::full(setup = setup_duck_copy, config = limit(DUCK_COPY))]
fn duck_copy((mut dst, src): (Vec<Detector>, Vec<Detector>)) -> (Vec<Detector>, Vec<Detector>) {
    dst.copy_from_slice(black_box(&src));
    (dst, src)
}

fn setup_duck_copy() -> (Vec<Detector>, Vec<Detector>) {
    let (mut src, buf, mut envelope) = detectors();
    for (g, d) in src.iter_mut().enumerate() {
        d.process(B, |k, _| buf.channel(g * MAX_KEYS + k), &mut envelope);
    }
    (vec![Detector::new(480); MAX_DUCKS], src)
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

const CORE_COPY: u64 = 31_700;
const DUCK_METERED: u64 = 409_172;
const DUCK_IDLE: u64 = 4_002;
const DUCK_COPY: u64 = 22_310;
const HISTORY_BLOCK: u64 = 45_000;
const MATRIX_SCALAR: u64 = 8_306_000;
const MATRIX_AVX2: u64 = 1_051_000;
const DELAYED_SCALAR: u64 = 8_010_000;
const DELAYED_AVX2: u64 = 1_228_000;
const DUCKED_SCALAR: u64 = 17_217_000;
const DUCKED_AVX2: u64 = 5_868_000;
const VARIED_SCALAR: u64 = 17_204_000;
const VARIED_AVX2: u64 = 5_858_000;
const METERS_SCALAR: u64 = 1_118_000;
const METERS_AVX2: u64 = 303_000;
const INPUT_TRUE_PEAK_AVX2: u64 = 94_500;
const INPUT_METERS: u64 = 152_623;
const METER_GAPS: u64 = 58_937;
const STRIPS_RAMPING: u64 = 1_502_708;
const STRIPS_SETTLED: u64 = 979_919;
const STRIPS_RAMPING_FAST: u64 = 1_771_069;
const STRIPS_SETTLED_FAST: u64 = 1_273_161;
const DUCK: u64 = 407_817;
const LIMITERS_ON: u64 = 655_232;
const LIMITERS_OFF: u64 = 171_994;
const DELAYS_SETTLED: u64 = 372_606;
const DELAYS_FADING: u64 = 473_348;
const DUCK_ADVANCE: u64 = 4_065;
const TONE_SETTLED: u64 = 11_072;
const TONE_CROSSFADING: u64 = 16_847;
const TONE_IDENT: u64 = 29_499;

library_benchmark_group!(
    name = block,
    benchmarks = [
        matrix,
        matrix_delayed,
        matrix_ducked,
        meters,
        input_meters,
        input_true_peak,
        meter_gaps,
        strip,
        limiter,
        delay,
        tone,
        duck,
        duck_metered,
        duck_idle,
        duck_advance
    ]
);
library_benchmark_group!(
    name = transfer,
    benchmarks = [core_copy, duck_copy, history]
);

main!(library_benchmark_groups = block, transfer);
