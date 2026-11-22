//! The bits the engine's kernels compute, pinned per
//! [`samplez::COMPUTE_VERSION`]. A fixed signal from index 0 runs through
//! every stage that touches output samples or meters: an input
//! crossfade, each input's strip, two duck detectors (one keyed and
//! automatic, one forced on then off) with mask and depth ramps, the
//! matrix with gain ramps and a delayed fade into two outputs, an aux and
//! a mix-minus column, an output delay, and the output limiter, with a
//! burst only its true-peak estimate catches. The
//! samples and the loudness of the outputs are hashed apart. A change to
//! what any stage computes fails here until the version is bumped and its
//! hashes added, so replicas of a release that keeps the version compute
//! what a running set computes.

use std::sync::Arc;

use samplez_dsp::strip::{CompParams, DEFAULT_BANDS, EqBand, GateParams, Strip, StripParam};
use samplez_dsp::{
    Delay, DelayRings, Detector, DetectorParams, DuckMode, DuckParam, KWeighting, Limiter,
    LimiterParams, Loudness, MAX_DUCKS, Matrix, PlanarBuffer, Ramp, SLICE, Slice,
};

/// The golden run's hashes for each compute version: (version, samples,
/// loudness).
const GOLDEN: &[(u32, u64, u64)] = &[
    (1, 0xe51d_eb18_8958_9e71, 0xcdde_c278_9d44_294c),
    (2, 0xe51d_eb18_8958_9e71, 0xcdde_c278_9d44_294c),
    (3, 0xe51d_eb18_8958_9e71, 0xcdde_c278_9d44_294c),
];

const BLOCK: usize = 48;
const BLOCKS: usize = 3000;
/// Programme, programme, the voice keying the automatic detector, and
/// the flow input 1 crosses over to.
const INPUTS: usize = 4;
/// Two outputs, an aux and a mix-minus.
const COLUMNS: usize = 4;
const OUTPUTS: usize = 2;
const RAMP: u32 = 480;
/// The first block of the inter-sample-peak burst.
const TRUE_PEAK_AT: usize = 2600;

/// A signal of plain arithmetic, so no libm function decides its bits:
/// a triangle per input at its own period, a little noise from a linear
/// congruential generator, and for the voice bursts of 0.3 s every 0.8 s.
fn signal(input: usize, n: u64, seed: &mut u64) -> f32 {
    let period = [96u64, 211, 389, 151][input];
    let phase = (n % period) as f32 / period as f32;
    let triangle = 4.0 * (phase - 0.5).abs() - 1.0;
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    let noise = ((*seed >> 40) as f32 / (1u64 << 24) as f32) - 0.5;
    let level = match input {
        2 if n % 38_400 >= 14_400 => 0.0,
        2 => 0.8,
        _ => 0.6,
    };
    level * triangle + 0.05 * noise
}

fn strip() -> Strip {
    let mut s = Strip::new(1, RAMP);
    let mut params = vec![
        StripParam::Trim { db: 3.0 },
        StripParam::Hpf {
            enabled: true,
            freq: 80.0,
        },
        StripParam::Eq { enabled: true },
        StripParam::Gate(GateParams {
            enabled: true,
            threshold_db: -50.0,
            range_db: 30.0,
            attack_ms: 1.0,
            release_ms: 50.0,
        }),
        StripParam::Comp(CompParams {
            enabled: true,
            threshold_db: -18.0,
            ratio: 4.0,
            attack_ms: 2.0,
            release_ms: 80.0,
            knee_db: 6.0,
            makeup_db: 2.0,
            mix: 1.0,
        }),
    ];
    for (band, b) in DEFAULT_BANDS.iter().enumerate() {
        let value = EqBand { gain_db: 4.0, ..*b };
        params.push(StripParam::EqBand { band, value });
    }
    for p in params {
        assert!(s.set(p));
    }
    s
}

fn detector(mode: DuckMode, key: Option<usize>) -> Detector {
    let mut d = Detector::new(RAMP);
    assert!(d.set(DuckParam::Detector(DetectorParams {
        threshold_db: -24.0,
        attack_ms: 5.0,
        hold_ms: 100.0,
        release_ms: 300.0,
        ..DetectorParams::DEFAULT
    })));
    if let Some(key) = key {
        assert!(d.set(DuckParam::Key {
            key,
            legs: 1,
            gain: 1.0
        }));
    }
    assert!(d.set(DuckParam::Mode(mode)));
    d
}

/// The golden run's hashes: XXH3-64 over every column's samples' bits,
/// and over the loudness readings' bits each slice.
fn run() -> (u64, u64) {
    let mut strips: Vec<Strip> = (0..INPUTS).map(|_| strip()).collect();
    let mut ducks = [
        detector(DuckMode::Auto, Some(2)),
        detector(DuckMode::On, None),
    ];
    let mut matrix = Matrix::new(INPUTS, COLUMNS);
    // Outputs 0 and 1, the aux taking every input, the mix-minus every
    // input but the voice.
    for (i, o, g) in [
        (0, 0, 0.8),
        (1, 0, 0.5),
        (2, 0, 0.7),
        (1, 1, 0.5),
        (2, 1, 0.9),
        (0, 2, 0.4),
        (1, 2, 0.4),
        (2, 2, 0.4),
        (0, 3, 0.6),
        (1, 3, 0.6),
    ] {
        assert!(matrix.set(i, o, g, 0));
    }
    // Programme on output 0 ducked under the voice, input 1 on output 1
    // by the forced detector.
    assert!(matrix.set_mask(0, 0, 0b01, 0));
    assert!(matrix.set_mask(1, 1, 0b10, 0));
    assert!(matrix.set_depth(0, 0, 12.0, 0));
    assert!(matrix.set_depth(1, 1, 6.0, 0));
    let rings = DelayRings::new(1, 4800, 2, BLOCK);
    let mut delay = Delay::new(Arc::clone(&rings), RAMP);
    let mut limiter = Limiter::new(OUTPUTS, RAMP);
    assert!(limiter.set(LimiterParams {
        enabled: true,
        ceiling_db: -18.0,
        release_ms: 50.0,
    }));
    let mut kw = KWeighting::new(OUTPUTS);
    let mut loudness = Loudness::new();
    let mut slice_power = 0.0f64;

    let mut input = PlanarBuffer::new(INPUTS, BLOCK);
    let mut envelopes = PlanarBuffer::new(MAX_DUCKS, BLOCK);
    let mut output = PlanarBuffer::new(COLUMNS, BLOCK);
    let mut out = vec![0.0f32; OUTPUTS * BLOCK];
    let mut seeds = [1u64, 2, 3, 4];
    let (mut samples, mut meters) = (
        xxhash_rust::xxh3::Xxh3Default::new(),
        xxhash_rust::xxh3::Xxh3Default::new(),
    );
    // Input 1 crosses over to the flow of input 3 from this block.
    let xfade_at = 300 * BLOCK as u64;
    for k in 0..BLOCKS {
        let start = (k * BLOCK) as u64;
        match k {
            600 => {
                assert!(matrix.set(0, 0, 0.3, RAMP));
                assert!(matrix.set_fade(2, 1, 0.2, 960, 4800));
            }
            900 => {
                assert!(matrix.set_depth(0, 0, 20.0, RAMP));
                assert!(matrix.set_mask(1, 1, 0b11, RAMP));
            }
            1200 => assert!(ducks[1].set(DuckParam::Mode(DuckMode::Off))),
            1500 => assert!(delay.set(start, 1000)),
            2100 => assert!(delay.set(start, 250)),
            _ => {}
        }
        for (i, seed) in seeds.iter_mut().enumerate() {
            let x = input.channel_mut(i);
            for (j, v) in x[..BLOCK].iter_mut().enumerate() {
                *v = signal(i, start + j as u64, seed);
            }
        }
        // The engine's crossfade: new * up + old * down over the ramp.
        let pos = start.saturating_sub(xfade_at);
        if start >= xfade_at && pos < u64::from(RAMP) {
            let mut up = Ramp::new(0.0);
            let _ = up.set_target(1.0, RAMP);
            up.advance(pos as usize);
            let mut down = Ramp::new(1.0);
            let _ = down.set_target(0.0, RAMP);
            down.advance(pos as usize);
            let new: Vec<f32> = input.channel(3)[..BLOCK].to_vec();
            let mut old: Vec<f32> = input.channel(1)[..BLOCK].to_vec();
            let x = &mut input.channel_mut(1)[..BLOCK];
            x.copy_from_slice(&new);
            up.process(x);
            down.process(&mut old);
            for (d, s) in x.iter_mut().zip(&old) {
                *d += *s;
            }
        } else if start >= xfade_at {
            let new: Vec<f32> = input.channel(3)[..BLOCK].to_vec();
            input.channel_mut(1)[..BLOCK].copy_from_slice(&new);
        }
        for (i, s) in strips.iter_mut().enumerate() {
            s.process(&mut input.channel_mut(i)[..BLOCK], BLOCK);
        }
        for (d, duck) in ducks.iter_mut().enumerate() {
            let env = &mut envelopes.channel_mut(d)[..BLOCK];
            duck.process(BLOCK, |key, _| &input.channel(key)[..BLOCK], env);
        }
        matrix.process_ducked(&input, &envelopes, &mut output, BLOCK);
        for o in 0..OUTPUTS {
            out[o * BLOCK..(o + 1) * BLOCK].copy_from_slice(&output.channel(o)[..BLOCK]);
        }
        delay.process(start, &mut out[..BLOCK], BLOCK);
        // An inter-sample peak: a sine at a quarter of the sample rate,
        // 45 degrees off its zero crossings, is the samples a, a, -a, -a,
        // below the ceiling, with a reconstructed peak of a * sqrt(2)
        // above it, so only the limiter's true-peak estimate engages it.
        if (TRUE_PEAK_AT..TRUE_PEAK_AT + 100).contains(&k) {
            for (n, v) in out.iter_mut().enumerate() {
                *v = if (n % BLOCK) % 4 < 2 { 0.11 } else { -0.11 };
            }
        }
        limiter.process(start, &mut out, BLOCK);
        for v in &out {
            samples.update(&v.to_bits().to_le_bytes());
        }
        for c in OUTPUTS..COLUMNS {
            for v in &output.channel(c)[..BLOCK] {
                samples.update(&v.to_bits().to_le_bytes());
            }
        }
        for o in 0..OUTPUTS {
            slice_power += kw.process(o, &out[o * BLOCK..(o + 1) * BLOCK]);
        }
        let end = start + BLOCK as u64;
        if end.is_multiple_of(SLICE) {
            let index = end / SLICE - 1;
            loudness.push(index, Slice::Power(slice_power / SLICE as f64));
            slice_power = 0.0;
            for reading in [
                loudness.momentary(),
                loudness.integrated(),
                loudness.range(),
            ] {
                meters.update(&reading.map_or(u64::MAX, f64::to_bits).to_le_bytes());
            }
        }
    }
    (samples.digest(), meters.digest())
}

#[test]
fn the_kernels_compute_the_bits_pinned_for_this_compute_version() {
    let (s, m) = run();
    let pinned = GOLDEN
        .iter()
        .find(|g| g.0 == samplez::COMPUTE_VERSION)
        .map(|g| (g.1, g.2));
    assert_eq!(
        Some((s, m)),
        pinned,
        "the kernels compute samples {s:#018x} and loudness {m:#018x}: a \
         change to the bits they compute needs a COMPUTE_VERSION bump and \
         its hashes here"
    );
}

/// The run is a function of its inputs alone: two runs agree.
#[test]
fn the_golden_run_is_deterministic() {
    assert_eq!(run(), run());
}
