/// A fade-in replaces a populated, ramping matrix whole: coefficients
/// it leaves at silence are gone, the others ramp from silence.
#[test]
fn a_fade_in_replaces_every_coefficient_from_silence() {
    let mut m = Matrix::new(3, 2);
    for i in 0..3 {
        for o in 0..2 {
            assert!(m.set(i, o, 0.5, 480));
        }
    }
    let gains = [0.25, 0.0, 0.0, 1.0, 0.5, f32::NAN];
    assert!(m.fade_in(&gains, 100));
    let mut expect = Ramp::new(0.0);
    assert!(expect.set_target(0.25, 100));
    assert_eq!(m.gain(0, 0).to_bits(), expect.current().to_bits());
    for (i, o) in [(0, 1), (1, 0), (2, 1)] {
        assert_eq!(m.gain(i, o), 0.0);
        assert!(!m.is_ramping(i, o));
    }
    m.advance(40);
    assert!(m.is_ramping(1, 1) && m.is_ramping(2, 0));
    m.advance(60);
    assert_eq!((m.gain(0, 0), m.gain(1, 1), m.gain(2, 0)), (0.25, 1.0, 0.5));
    assert!(!m.fade_in(&[0.0; 6], 100));
    assert_eq!(m.gain(1, 1), 0.0);
}

/// A ducked fill replaces whatever a matrix held, masks, depths and
/// crossfades included, with what a fresh matrix holds after a
/// fade-in and every nonzero mask and depth set with no crossfade or
/// ramp; all-zero masks and depths leave it without ducks.
#[test]
fn a_ducked_fill_replaces_every_mask_and_depth() {
    let gains = [0.25, 0.0, 0.5, 1.0, 0.5, 0.75];
    let masks = [1, 4, 0, 0, 0b11, 0];
    let mut depths = [0.0; 3 * MAX_DUCKS];
    depths[MAX_DUCKS + 2] = 12.0;
    depths[2 * MAX_DUCKS] = 41.0;
    let mut used = Matrix::new(3, 2);
    for i in 0..3 {
        for o in 0..2 {
            assert!(used.set(i, o, 0.5, 480));
            assert!(used.set_mask(i, o, 0b1000_0000, 480));
        }
        assert!(used.set_depth(i, 7, 30.0, 480));
    }
    used.advance(117);
    used.align(240);
    assert!(used.fade_in_ducked(&gains, &masks, &depths, 100));
    let mut fresh = Matrix::new(3, 2);
    fresh.align(240);
    assert!(fresh.fade_in(&gains, 100));
    for (c, &k) in masks.iter().enumerate().filter(|(_, k)| **k != 0) {
        assert!(fresh.set_mask(c / 2, c % 2, k, 0));
    }
    assert!(fresh.set_depth(1, 2, 12.0, 0));
    assert!(used == fresh, "{used:?}\n{fresh:?}");
    assert!(used.fade_in_ducked(&gains, &[0; 6], &[0.0; 3 * MAX_DUCKS], 100));
    assert!(!used.has_ducks());
    assert!(used.fade_in_ducked(&gains, &[], &[], 100));
    assert!(!used.has_ducks());
}

use super::*;
use crate::require::path_runs;
use crate::test_util::Rng;
use crate::{restore_mxcsr, set_ftz_daz};

const CAP: usize = 1024;

fn fill(rng: &mut Rng, buf: &mut PlanarBuffer, frames: usize) {
    for ch in 0..buf.channels() {
        for s in &mut buf.channel_mut(ch)[..frames] {
            *s = rng.sample();
        }
    }
}

/// Sets a random subset of coefficients, with ramps of random length
/// including none and lengths that end inside a block.
fn change(rng: &mut Rng, m: &mut Matrix, count: usize) {
    for _ in 0..count {
        let i = rng.below(m.inputs());
        let j = rng.below(m.outputs());
        let len = match rng.below(4) {
            0 => 0,
            _ => rng.below(2000) as u32,
        };
        let c = rng.coefficient();
        assert!(m.set(i, j, c, len));
    }
}

fn assert_bits_eq(a: &PlanarBuffer, b: &PlanarBuffer, frames: usize, ctx: &str) {
    for ch in 0..a.channels() {
        let (x, y) = (&a.channel(ch)[..frames], &b.channel(ch)[..frames]);
        for n in 0..frames {
            assert_eq!(
                x[n].to_bits(),
                y[n].to_bits(),
                "{ctx}: output {ch} sample {n}: {} vs {}",
                x[n],
                y[n]
            );
        }
    }
}

/// Runs random matrices, coefficient changes and blocks on `path` and
/// on the scalar reference, with FTZ/DAZ off and on, and compares every
/// output sample bit for bit.
fn bit_exact_vs_scalar(path: Path, seed: u64) {
    if !path_runs("bit_exact_vs_scalar", path) {
        return;
    }
    for ftz in [false, true] {
        let saved = set_ftz_daz(ftz);
        let mut rng = Rng::new(seed);
        for trial in 0..24 {
            let (n, m) = if trial == 0 {
                (64, 64)
            } else {
                (1 + rng.below(64), 1 + rng.below(64))
            };
            let mut reference = Matrix::new(n, m);
            change(&mut rng, &mut reference, n * m);
            let mut under_test = reference.clone();
            let mut input = PlanarBuffer::new(n, CAP);
            let mut want = PlanarBuffer::new(m, CAP);
            let mut got = PlanarBuffer::new(m, CAP);
            for block in 0..4 {
                let frames = if trial == 0 && block == 0 {
                    CAP
                } else {
                    16 + rng.below(CAP - 15)
                };
                fill(&mut rng, &mut input, frames);
                reference.process_on(Path::Scalar, &input, &mut want, frames);
                under_test.process_on(path, &input, &mut got, frames);
                let ctx = format!("ftz {ftz} trial {trial} {n}x{m} block {block}/{frames}");
                assert_bits_eq(&want, &got, frames, &ctx);
                assert_eq!(reference, under_test, "{ctx}: state");
                let k = rng.below(n * m / 4 + 2);
                change(&mut rng, &mut reference, k);
                under_test.columns.clone_from(&reference.columns);
            }
        }
        restore_mxcsr(saved);
    }
}

#[test]
fn advance_moves_ramps_as_process_does() {
    let mut rng = Rng::new(7);
    let mut a = Matrix::new(3, 2);
    assert!(a.set(0, 0, 0.8, 1000));
    assert!(a.set(2, 1, -0.5, 300));
    assert!(a.set(1, 1, 0.25, 0));
    let mut b = a.clone();
    let mut input = PlanarBuffer::new(3, CAP);
    let (mut out_a, mut out_b) = (PlanarBuffer::new(2, CAP), PlanarBuffer::new(2, CAP));
    fill(&mut rng, &mut input, CAP);
    a.process(&input, &mut out_a, 400);
    b.advance(400);
    assert_eq!(a, b);
    a.process(&input, &mut out_a, CAP);
    b.process(&input, &mut out_b, CAP);
    assert_bits_eq(&out_a, &out_b, CAP, "after advance");
}

/// A copy taken mid-ramp continues bit-identically, a coefficient the
/// target had and the source has not included.
#[test]
fn a_copy_continues_the_ramps_bit_identically() {
    let mut rng = Rng::new(11);
    let mut src = Matrix::new(3, 2);
    assert!(src.set(0, 0, 0.8, 1000));
    assert!(src.set(2, 1, -0.5, 300));
    src.advance(123);
    let mut dst = Matrix::new(3, 2);
    assert!(dst.set(1, 0, 0.3, 0));
    dst.copy_from(&src);
    assert_eq!(dst, src);
    let mut input = PlanarBuffer::new(3, CAP);
    let (mut out_a, mut out_b) = (PlanarBuffer::new(2, CAP), PlanarBuffer::new(2, CAP));
    fill(&mut rng, &mut input, CAP);
    src.process(&input, &mut out_a, CAP);
    dst.process(&input, &mut out_b, CAP);
    assert_bits_eq(&out_a, &out_b, CAP, "after the copy");
}

#[test]
fn is_ramping_only_while_a_ramp_runs() {
    let mut m = Matrix::new(2, 2);
    assert!(!m.is_ramping(0, 0), "absent coefficient");
    assert!(m.set(0, 0, 0.5, 10));
    assert!(m.is_ramping(0, 0));
    assert!(!m.is_ramping(1, 0) && !m.is_ramping(0, 1));
    m.advance(10);
    assert!(!m.is_ramping(0, 0), "ramp ended");
    // A target equal to the held value starts no ramp.
    assert!(m.set(0, 0, 0.5, 10));
    assert!(!m.is_ramping(0, 0));
    assert!(m.set(1, 1, 0.0, 10), "zero on an absent coefficient");
    assert!(!m.is_ramping(1, 1));
    assert!(m.set(0, 0, 0.0, 0), "jump");
    assert!(!m.is_ramping(0, 0));
    assert!(!m.is_ramping(5, 5), "out of range");
}

/// An input range feeds an output through any coefficient of it whose
/// target is other than 0: from the change that sets it, through a
/// ramp up, and no longer from the change that sets it to 0, ramping
/// down or not.
#[test]
fn feeds_while_a_target_is_other_than_zero() {
    use std::iter::once;
    let mut m = Matrix::new(6, 2);
    assert!(!m.feeds(once(0..6), 0), "no coefficient");
    assert!(m.set(3, 1, 0.5, 10));
    assert!(m.set(5, 1, 0.5, 0));
    assert!(
        m.feeds(once(2..4), 1) && m.feeds(once(3..4), 1),
        "ramping up"
    );
    assert!(m.feeds([0..1, 5..6], 1));
    assert!(!m.feeds(once(0..3), 1) && !m.feeds(once(4..5), 1));
    assert!(!m.feeds([0..1, 4..5], 1));
    assert!(!m.feeds(once(2..4), 0) && !m.feeds([], 1));
    m.advance(10);
    assert!(m.set(3, 1, 0.0, 10));
    assert!(!m.feeds(once(3..4), 1), "ramping down to 0");
    m.advance(10);
    assert!(!m.feeds(once(3..4), 1), "at 0");
    assert!(!m.feeds(once(0..6), 7), "out of range");
}

#[test]
fn bit_exact_avx2() {
    bit_exact_vs_scalar(Path::Avx2, 0xa2);
}

/// Random coefficient changes, ramps and blocks on `matrix`, the
/// output bits of every block folded into one value.
fn fingerprint(path: Path, matrix: &mut Matrix, rng: &mut Rng) -> u64 {
    let (n, m) = (matrix.inputs(), matrix.outputs());
    let mut input = PlanarBuffer::new(n, CAP);
    let mut out = PlanarBuffer::new(m, CAP);
    let mut fold = 0u64;
    for _ in 0..12 {
        change(rng, matrix, n * m / 3);
        let frames = 1 + rng.below(CAP);
        fill(rng, &mut input, frames);
        matrix.process_on(path, &input, &mut out, frames);
        for ch in 0..m {
            for s in &out.channel(ch)[..frames] {
                fold = fold.rotate_left(5) ^ u64::from(s.to_bits());
            }
        }
    }
    fold
}

/// The output of a matrix with no duck set carries the bits pinned
/// here, computed by the matrix before ducks existed, on every path.
#[test]
fn a_matrix_without_ducks_keeps_its_bits() {
    for path in Path::ALL {
        if !path_runs("a_matrix_without_ducks_keeps_its_bits", path) {
            continue;
        }
        let mut rng = Rng::new(0x5eed);
        let mut m = Matrix::new(13, 9);
        let fold = fingerprint(path, &mut m, &mut rng);
        assert_eq!(fold, UNDUCKED, "{path:?}: {fold:#x}");
    }
}

const UNDUCKED: u64 = 0xb2ae_7ead_edfa_9a5b;

/// The scalar reference equals the dense definition
/// `sum_i M[i][j] * in[i][n]` over every coefficient, zeros included,
/// for finite input without -0.0.
#[test]
fn scalar_equals_dense_sum() {
    let mut rng = Rng::new(7);
    for _ in 0..16 {
        let (n, m) = (1 + rng.below(64), 1 + rng.below(64));
        let frames = 16 + rng.below(CAP - 15);
        let mut coef = vec![0.0f32; n * m];
        let mut mat = Matrix::new(n, m);
        for i in 0..n {
            for j in 0..m {
                coef[i * m + j] = rng.coefficient();
                assert!(mat.set(i, j, coef[i * m + j], 0));
            }
        }
        let mut input = PlanarBuffer::new(n, CAP);
        let mut out = PlanarBuffer::new(m, CAP);
        fill(&mut rng, &mut input, frames);
        mat.process_on(Path::Scalar, &input, &mut out, frames);
        for j in 0..m {
            for s in 0..frames {
                let mut acc = 0.0f32;
                for i in 0..n {
                    acc += coef[i * m + j] * input.channel(i)[s];
                }
                assert_eq!(out.channel(j)[s].to_bits(), acc.to_bits());
            }
        }
    }
}

/// A coefficient change ramps each coefficient per `c0 + n * step`,
/// retargets from the current value, and sums in input order.
#[test]
fn coefficient_ramp_follows_formula() {
    let ramp = |c0: f32, target: f32, len: u32, k: u32| {
        let step = (target - c0) / len as f32;
        if k < len {
            c0 + k as f32 * step
        } else {
            target
        }
    };
    for path in Path::ALL {
        if !path_runs("coefficient_ramp_follows_formula", path) {
            continue;
        }
        let mut rng = Rng::new(11);
        let mut mat = Matrix::new(3, 2);
        assert!(mat.set(0, 0, 0.5, 0));
        assert!(mat.set(1, 0, -0.25, 0));
        assert!(mat.set(2, 1, 1.0, 0));
        let mut input = PlanarBuffer::new(3, CAP);
        let mut out = PlanarBuffer::new(2, CAP);

        // Block 1: output 0 ramps input 0 from 0.5 to 0.9 over 300 and
        // input 2 in from 0 to 0.3 over 77; output 1 ramps its route
        // down to 0.125 over 200.
        assert!(mat.set(0, 0, 0.9, 300));
        assert!(mat.set(2, 0, 0.3, 77));
        assert!(mat.set(2, 1, 0.125, 200));
        let f1 = 131;
        fill(&mut rng, &mut input, f1);
        mat.process_on(path, &input, &mut out, f1);
        for n in 0..f1 {
            let k = n as u32;
            let x = |i: usize| input.channel(i)[n];
            let mut acc = 0.0f32;
            acc += ramp(0.5, 0.9, 300, k) * x(0);
            acc += -0.25 * x(1);
            acc += ramp(0.0, 0.3, 77, k) * x(2);
            assert_eq!(out.channel(0)[n].to_bits(), acc.to_bits(), "{path:?} n {n}");
            let want = 0.0 + ramp(1.0, 0.125, 200, k) * x(2);
            assert_eq!(
                out.channel(1)[n].to_bits(),
                want.to_bits(),
                "{path:?} n {n}"
            );
        }

        // Block 2: retarget input 0 on output 0 mid-ramp.
        let c_retarget = ramp(0.5, 0.9, 300, f1 as u32);
        assert_eq!(mat.gain(0, 0).to_bits(), c_retarget.to_bits());
        assert!(mat.set(0, 0, 0.0, 100));
        let f2 = 203;
        fill(&mut rng, &mut input, f2);
        mat.process_on(path, &input, &mut out, f2);
        for n in 0..f2 {
            let k = (f1 + n) as u32;
            let x = |i: usize| input.channel(i)[n];
            let mut acc = 0.0f32;
            acc += ramp(c_retarget, 0.0, 100, n as u32) * x(0);
            acc += -0.25 * x(1);
            acc += ramp(0.0, 0.3, 77, k) * x(2);
            assert_eq!(out.channel(0)[n].to_bits(), acc.to_bits(), "{path:?} n {n}");
            let want = 0.0 + ramp(1.0, 0.125, 200, k) * x(2);
            assert_eq!(
                out.channel(1)[n].to_bits(),
                want.to_bits(),
                "{path:?} n {n}"
            );
        }
        // The ramp to zero has ended, so the coefficient is gone.
        assert_eq!(mat.columns[0].len(), 2);
        assert_eq!(mat.gain(0, 0).to_bits(), 0);
        assert_eq!(mat.gain(2, 1).to_bits(), 0.125f32.to_bits());
    }
}

/// Bit patterns a multiply by 1.0 does not preserve under FTZ/DAZ or
/// that an add to +0.0 changes.
fn special(rng: &mut Rng) -> f32 {
    match rng.below(6) {
        0 => -0.0,
        1 => f32::from_bits(0x7fa0_1234),
        2 => f32::from_bits(0x8000_0001 | (rng.next_u64() as u32 & 0x007f_ffff)),
        3 => f32::NEG_INFINITY,
        _ => rng.sample(),
    }
}

#[test]
fn route_is_bit_transparent() {
    for path in Path::ALL {
        if !path_runs("route_is_bit_transparent", path) {
            continue;
        }
        let saved = set_ftz_daz(true);
        let mut rng = Rng::new(13);
        let (n, m, frames) = (8, 10, 509);
        let mut input = PlanarBuffer::new(n, CAP);
        for ch in 0..n {
            for s in &mut input.channel_mut(ch)[..frames] {
                *s = special(&mut rng);
            }
        }
        // A shuffle of all inputs, one input selected twice, and one
        // output that becomes a route once its crossfade from input 0
        // to input 5 has ended.
        let mut mat = Matrix::new(n, m);
        let src = [3, 0, 7, 1, 6, 2, 5, 4];
        for (j, &i) in src.iter().enumerate() {
            assert!(mat.set(i, j, 1.0, 0));
        }
        assert!(mat.set(3, 8, 1.0, 0));
        assert!(mat.set(0, 9, 1.0, 0));
        let mut out = PlanarBuffer::new(m, CAP);
        mat.process_on(path, &input, &mut out, frames);
        assert!(mat.set(0, 9, 0.0, 64));
        assert!(mat.set(5, 9, 1.0, 64));
        let mut fade = PlanarBuffer::new(m, CAP);
        mat.process_on(path, &input, &mut fade, 64);
        mat.process_on(path, &input, &mut out, frames);
        restore_mxcsr(saved);

        let routed = src.iter().enumerate().map(|(j, &i)| (j, i));
        for (j, i) in routed.chain([(8, 3), (9, 5)]) {
            for s in 0..frames {
                assert_eq!(
                    out.channel(j)[s].to_bits(),
                    input.channel(i)[s].to_bits(),
                    "{path:?} output {j} sample {s}"
                );
            }
        }
    }
}

#[test]
fn silence_gives_exact_zeros() {
    for path in Path::ALL {
        if !path_runs("silence_gives_exact_zeros", path) {
            continue;
        }
        for ftz in [false, true] {
            let saved = set_ftz_daz(ftz);
            let mut rng = Rng::new(17);
            let (n, m) = (24, 20);
            let mut mat = Matrix::new(n, m);
            change(&mut rng, &mut mat, n * m);
            let input = PlanarBuffer::new(n, CAP);
            let mut out = PlanarBuffer::new(m, CAP);
            for _ in 0..6 {
                for ch in 0..m {
                    out.channel_mut(ch).fill(f32::NAN);
                }
                let frames = 16 + rng.below(CAP - 15);
                mat.process_on(path, &input, &mut out, frames);
                for ch in 0..m {
                    for &s in &out.channel(ch)[..frames] {
                        assert_eq!(s.to_bits(), 0, "{path:?} ftz {ftz} output {ch}");
                    }
                }
                change(&mut rng, &mut mat, 40);
            }
            restore_mxcsr(saved);
        }
    }
}

#[test]
fn best_path_is_supported() {
    assert!(Path::best().is_supported());
    assert_eq!(Matrix::new(1, 1).path(), Path::best());
}

/// A settled 1.0 is a copy, but a ramp towards 1.0 is not: the ramp is
/// computed until it ends, and the block after it copies.
#[test]
fn ramp_to_unity_is_not_copied_until_it_ends() {
    for path in Path::ALL {
        if !path_runs("ramp_to_unity_is_not_copied_until_it_ends", path) {
            continue;
        }
        let saved = set_ftz_daz(true);
        let mut rng = Rng::new(23);
        let mut mat = Matrix::new(1, 1);
        let mut input = PlanarBuffer::new(1, CAP);
        let mut out = PlanarBuffer::new(1, CAP);
        assert!(mat.set(0, 0, 0.5, 0));
        fill(&mut rng, &mut input, 64);
        mat.process_on(path, &input, &mut out, 64);

        assert!(mat.set(0, 0, 1.0, 100));
        let frames = 150;
        fill(&mut rng, &mut input, frames);
        mat.process_on(path, &input, &mut out, frames);
        let step = (1.0f32 - 0.5) / 100.0;
        for n in 0..frames {
            let g = if n < 100 { 0.5 + n as f32 * step } else { 1.0 };
            let want = 0.0 + g * input.channel(0)[n];
            assert_eq!(
                out.channel(0)[n].to_bits(),
                want.to_bits(),
                "{path:?} n {n}"
            );
        }

        let frames = 97;
        for s in &mut input.channel_mut(0)[..frames] {
            *s = special(&mut rng);
        }
        mat.process_on(path, &input, &mut out, frames);
        restore_mxcsr(saved);
        for n in 0..frames {
            assert_eq!(
                out.channel(0)[n].to_bits(),
                input.channel(0)[n].to_bits(),
                "{path:?} n {n}"
            );
        }
    }
}

#[test]
fn invalid_coefficients_are_rejected() {
    let mut mat = Matrix::new(2, 2);
    assert!(mat.set(0, 1, 0.5, 0));
    let before = mat.clone();
    for bad in [f32::NAN, f32::INFINITY, f32::MAX] {
        assert!(!mat.set(0, 1, bad, 10), "{bad}");
        assert!(!mat.set(1, 0, bad, 10), "{bad}");
    }
    assert!(!mat.set(2, 0, 0.5, 0));
    assert!(!mat.set(0, 2, 0.5, 0));
    assert_eq!(mat, before);
}

#[test]
fn clone_reserves_every_input() {
    let mut mat = Matrix::new(16, 3);
    assert!(mat.set(4, 1, 0.5, 0));
    let copy = mat.clone();
    assert_eq!(copy, mat);
    assert!(copy.columns.iter().all(|c| c.capacity() >= 16));
}

// Ducks.

use crate::duck_gain;

/// Random envelopes, per detector: none, full, a step, a ramp or
/// uniform noise within 0 and 1.
fn envelopes(rng: &mut Rng, env: &mut PlanarBuffer, frames: usize) {
    for g in 0..MAX_DUCKS {
        let kind = rng.below(5);
        let (a, b) = ((rng.unit() + 1.0) * 0.5, (rng.unit() + 1.0) * 0.5);
        let at = rng.below(frames + 1);
        for (n, e) in env.channel_mut(g)[..frames].iter_mut().enumerate() {
            *e = match kind {
                0 => 0.0,
                1 => 1.0,
                2 => {
                    if n < at {
                        a
                    } else {
                        b
                    }
                }
                3 => a + (b - a) * n as f32 / frames as f32,
                _ => (rng.unit() + 1.0) * 0.5,
            };
        }
    }
}

/// Sets random masks, some 0, on random coefficients and random
/// depths, with random lengths including none.
fn duck_changes(rng: &mut Rng, m: &mut Matrix, count: usize) {
    for _ in 0..count {
        let (i, j) = (rng.below(m.inputs()), rng.below(m.outputs()));
        let len = match rng.below(4) {
            0 => 0,
            _ => rng.below(2000) as u32,
        };
        let mask = if rng.below(4) == 0 {
            0
        } else {
            rng.next_u64() as u8
        };
        assert!(m.set_mask(i, j, mask, len));
        let db = (rng.unit() + 1.0) * 20.0;
        assert!(m.set_depth(i, rng.below(MAX_DUCKS), db, len));
    }
}

/// Ducked terms in random matrices, under random masks, depths,
/// envelopes and their changes, coefficient changes, blocks shorter
/// and longer than an interval, skips and positions in the interval,
/// on `path` and on the scalar reference, with FTZ/DAZ off and on:
/// every output sample and the state bit for bit.
fn ducked_bit_exact_vs_scalar(path: Path, seed: u64) {
    if !path_runs("ducked_bit_exact_vs_scalar", path) {
        return;
    }
    let mut ducked = 0;
    for ftz in [false, true] {
        let saved = set_ftz_daz(ftz);
        let mut rng = Rng::new(seed);
        for trial in 0..24 {
            let (n, m) = (1 + rng.below(24), 1 + rng.below(24));
            let mut reference = Matrix::new(n, m);
            change(&mut rng, &mut reference, n * m);
            duck_changes(&mut rng, &mut reference, n * m);
            reference.align(rng.next_u64());
            let mut under_test = reference.clone();
            let mut input = PlanarBuffer::new(n, CAP);
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            let mut want = PlanarBuffer::new(m, CAP);
            let mut got = PlanarBuffer::new(m, CAP);
            for block in 0..6 {
                let frames = match rng.below(3) {
                    0 => 1 + rng.below(9),
                    _ => 1 + rng.below(CAP),
                };
                fill(&mut rng, &mut input, frames);
                envelopes(&mut rng, &mut env, frames);
                reference.process_ducked_on(Path::Scalar, &input, &env, &mut want, frames);
                under_test.process_ducked_on(path, &input, &env, &mut got, frames);
                let ctx = format!("ftz {ftz} trial {trial} {n}x{m} block {block}/{frames}");
                assert_bits_eq(&want, &got, frames, &ctx);
                assert_eq!(reference, under_test, "{ctx}: state");
                ducked += reference
                    .ducks
                    .iter()
                    .flatten()
                    .filter(|d| d.to < 1.0)
                    .count();
                let k = rng.below(n * m / 4 + 2);
                change(&mut rng, &mut reference, k);
                duck_changes(&mut rng, &mut reference, k);
                match rng.below(4) {
                    0 => reference.advance(rng.below(40)),
                    1 => reference.align(rng.next_u64()),
                    _ => {}
                }
                under_test.copy_from(&reference);
            }
        }
        restore_mxcsr(saved);
    }
    assert!(ducked > 1000, "only {ducked} ducked terms");
}

#[test]
fn ducked_bit_exact_avx2() {
    ducked_bit_exact_vs_scalar(Path::Avx2, 0xd2);
}

/// A matrix whose masks are 0 gives the bits of one that never had a
/// mask, depths and envelopes whatever they are, and a coefficient
/// whose mask goes back to 0 returns to them once its crossfade has
/// ended and its gain is back at no duck, with no duck left.
#[test]
fn mask_0_gives_the_bits_of_no_duck() {
    for path in Path::ALL {
        if !path_runs("mask_0_gives_the_bits_of_no_duck", path) {
            continue;
        }
        let mut rng = Rng::new(0x3a5c);
        let (n, m) = (12, 10);
        let mut plain = Matrix::new(n, m);
        change(&mut rng, &mut plain, n * m);
        let mut ducked = plain.clone();
        for i in 0..n {
            for g in 0..MAX_DUCKS {
                assert!(ducked.set_depth(i, g, 40.0, 100));
            }
        }
        let mut b = Buffers::new(n, m);
        for _ in 0..8 {
            b.run(path, &mut plain, &mut ducked, &mut rng, 300, None);
        }
        assert!(ducked.ducks.iter().all(Vec::is_empty));
        for i in 0..n {
            for j in 0..m {
                assert!(ducked.set_mask(i, j, 0xff, 50));
            }
        }
        for _ in 0..4 {
            b.run(path, &mut plain, &mut ducked, &mut rng, 300, Some(1.0));
        }
        for i in 0..n {
            for j in 0..m {
                assert!(ducked.set_mask(i, j, 0, 70));
            }
        }
        // The crossfade and two intervals after it.
        let mut frames = 0;
        while frames < 70 + 16 {
            frames += b.run(path, &mut plain, &mut ducked, &mut rng, 30, Some(0.5));
        }
        assert!(ducked.ducks.iter().all(Vec::is_empty), "{path:?}");
        for _ in 0..8 {
            b.run(path, &mut plain, &mut ducked, &mut rng, 300, None);
        }
    }
}

/// The buffers of a matrix and its twin with ducks.
struct Buffers {
    input: PlanarBuffer,
    env: PlanarBuffer,
    want: PlanarBuffer,
    got: PlanarBuffer,
}

impl Buffers {
    fn new(n: usize, m: usize) -> Buffers {
        Buffers {
            input: PlanarBuffer::new(n, CAP),
            env: PlanarBuffer::new(MAX_DUCKS, CAP),
            want: PlanarBuffer::new(m, CAP),
            got: PlanarBuffer::new(m, CAP),
        }
    }

    /// A random block of up to `max` samples through `plain` and
    /// `ducked`, the envelopes all at `level` or random; with random
    /// envelopes, both outputs bit-identical. Returns its length.
    fn run(
        &mut self,
        path: Path,
        plain: &mut Matrix,
        ducked: &mut Matrix,
        rng: &mut Rng,
        max: usize,
        level: Option<f32>,
    ) -> usize {
        let frames = 1 + rng.below(max);
        fill(rng, &mut self.input, frames);
        match level {
            Some(e) => (0..MAX_DUCKS).for_each(|g| self.env.channel_mut(g).fill(e)),
            None => envelopes(rng, &mut self.env, frames),
        }
        plain.process_on(path, &self.input, &mut self.want, frames);
        ducked.process_ducked_on(path, &self.input, &self.env, &mut self.got, frames);
        if level.is_none() {
            assert_bits_eq(&self.want, &self.got, frames, &format!("{path:?}"));
        }
        frames
    }
}

/// The gain of a ducked term moves linearly over each interval of the
/// sample index to the duck gain of the envelope at the last sample
/// of the interval before, whatever the split into blocks.
#[test]
fn a_duck_follows_the_envelope_an_interval_on() {
    for path in Path::ALL {
        if !path_runs("a_duck_follows_the_envelope_an_interval_on", path) {
            continue;
        }
        let mut rng = Rng::new(0x1e7);
        let (c, depth, start) = (0.75f32, 18.0f32, 5u64);
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, c, 0));
        assert!(m.set_depth(0, 2, depth, 0));
        assert!(m.set_mask(0, 0, 0b100, 0));
        m.align(start);
        let mut input = PlanarBuffer::new(1, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        let mut out = PlanarBuffer::new(1, CAP);
        let (mut from, mut to) = (1.0f32, 1.0f32);
        let mut index = start;
        while index < start + 2000 {
            let frames = 1 + rng.below(70);
            fill(&mut rng, &mut input, frames);
            envelopes(&mut rng, &mut env, frames);
            m.process_ducked_on(path, &input, &env, &mut out, frames);
            for k in 0..frames {
                let p = (index % 8) as u32;
                let d = from + (p + 1) as f32 * ((to - from) * 0.125);
                let want = 0.0 + (c * d) * input.channel(0)[k];
                let got = out.channel(0)[k];
                assert_eq!(got.to_bits(), want.to_bits(), "{path:?} index {index}");
                if p == 7 {
                    from = to;
                    to = duck_gain(depth, env.channel(2)[k]);
                }
                index += 1;
            }
        }
    }
}

/// Skipped samples move the depths and crossfades on and hold the
/// duck gain at the last one computed across the end of an interval;
/// the next interval moves from it.
#[test]
fn a_skip_holds_the_duck_gain() {
    let mut m = Matrix::new(1, 1);
    assert!(m.set(0, 0, 1.0, 0));
    assert!(m.set_depth(0, 0, 20.0, 0));
    assert!(m.set_mask(0, 0, 1, 0));
    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
    env.channel_mut(0).fill(1.0);
    let input = {
        let mut b = PlanarBuffer::new(1, CAP);
        b.channel_mut(0).fill(1.0);
        b
    };
    let mut out = PlanarBuffer::new(1, CAP);
    m.process_ducked(&input, &env, &mut out, 11);
    let (from, to) = (m.ducks[0][0].from, m.ducks[0][0].to);
    assert!(from == 1.0 && to < 1.0);
    m.advance(30);
    assert_eq!((m.ducks[0][0].from, m.ducks[0][0].to), (to, to));
    assert_eq!(m.phase, (11 + 30) % 8);
    // The rest of the interval holds, the next moves to no duck.
    env.channel_mut(0).fill(0.0);
    m.process_ducked(&input, &env, &mut out, 16);
    let rest = 8 - (11 + 30) % 8;
    assert!(out.channel(0)[..rest].iter().all(|&s| s == to));
    assert_eq!(out.channel(0)[rest + 7], to + 8.0 * ((1.0 - to) * 0.125));
    // A new position holds as well.
    let held = m.ducks[0][0].to;
    m.align(3);
    assert_eq!((m.ducks[0][0].from, m.phase), (held, 3));
}

/// The largest step between two samples of a DC input through a
/// ducked coefficient over `blocks` blocks of 37 samples.
fn largest_step(
    m: &mut Matrix,
    path: Path,
    env: &PlanarBuffer,
    prev: &mut Option<f32>,
    blocks: usize,
) -> f32 {
    let mut input = PlanarBuffer::new(1, CAP);
    input.channel_mut(0).fill(0.5);
    let mut out = PlanarBuffer::new(1, CAP);
    let mut step = 0.0f32;
    for _ in 0..blocks {
        m.process_ducked_on(path, &input, env, &mut out, 37);
        for &s in &out.channel(0)[..37] {
            if let Some(p) = *prev {
                step = step.max((s - p).abs());
            }
            *prev = Some(s);
        }
    }
    step
}

/// A mask change while ducked crossfades: adding a target, removing
/// it, moving it to another detector and changing it again in the
/// middle of a crossfade step by no more than a linear crossfade
/// over the length does, where the same change with no length steps
/// by the whole difference within one interval.
#[test]
fn a_mask_change_while_ducked_does_not_step() {
    const LEN: u32 = 480;
    // (mask before, masks after, one after another half the length
    // apart)
    let cases: [(u8, &[u8]); 4] = [
        (0b01, &[0]),
        (0, &[0b01]),
        (0b01, &[0b10]),
        (0b01, &[0, 0b11]),
    ];
    for path in Path::ALL {
        if !path_runs("a_mask_change_while_ducked_does_not_step", path) {
            continue;
        }
        for (before, after) in cases {
            let run = |len: u32| {
                let mut m = Matrix::new(1, 1);
                assert!(m.set(0, 0, 1.0, 0));
                assert!(m.set_depth(0, 0, 20.0, 0));
                assert!(m.set_depth(0, 1, 30.0, 0));
                assert!(m.set_mask(0, 0, before, 0));
                m.align(3);
                let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                env.channel_mut(0).fill(1.0);
                env.channel_mut(1).fill(0.5);
                let prev = &mut None;
                largest_step(&mut m, path, &env, prev, 2);
                let settled = largest_step(&mut m, path, &env, prev, 4);
                assert_eq!(settled, 0.0);
                let mut step = 0.0f32;
                for &mask in after {
                    assert!(m.set_mask(0, 0, mask, len));
                    step = step.max(largest_step(&mut m, path, &env, prev, 7));
                }
                step.max(largest_step(&mut m, path, &env, prev, 20))
            };
            let (ramped, jumped) = (run(LEN), run(0));
            // The output's range over the case: detector 0 at 20 dB,
            // detector 1 at 15.
            let gain = |mask: u8| match mask {
                0 => 1.0,
                0b10 => duck_gain(30.0, 0.5),
                _ => duck_gain(20.0, 1.0),
            };
            let gains = after.iter().map(|&k| gain(k)).chain([gain(before)]);
            let (lo, hi) = gains.fold((1.0f32, 0.0f32), |(l, h), g| (l.min(g), h.max(g)));
            let whole = 0.5 * (hi - lo);
            assert!(
                ramped <= 1.1 * whole / LEN as f32,
                "{path:?} {before:#b} -> {after:?}: step {ramped} when crossfaded"
            );
            assert!(
                jumped >= 0.5 * whole / 8.0,
                "{path:?} {before:#b} -> {after:?}: step {jumped} when jumped"
            );
        }
    }
}

/// Two mask changes on one coefficient before the end of an interval,
/// with samples processed or skipped between them, step by no more
/// than a linear crossfade over the length does: one back to the
/// mask it started from does not move it at all.
#[test]
fn two_mask_changes_in_one_interval_do_not_step() {
    const LEN: u32 = 4800;
    let cases: [[u8; 3]; 4] = [
        [0b01, 0, 0b01],
        [0b01, 0b10, 0b01],
        [0, 0b01, 0],
        [0b01, 0b10, 0b10],
    ];
    let gain = |mask: u8| match mask {
        0 => 1.0,
        0b10 => duck_gain(20.0, 1.0),
        _ => duck_gain(40.0, 1.0),
    };
    for path in Path::ALL {
        if !path_runs("two_mask_changes_in_one_interval_do_not_step", path) {
            continue;
        }
        for [a, b, c] in cases {
            for skip in [false, true] {
                let mut m = Matrix::new(1, 1);
                assert!(m.set(0, 0, 1.0, 0));
                assert!(m.set_depth(0, 0, 40.0, 0) && m.set_depth(0, 1, 20.0, 0));
                assert!(m.set_mask(0, 0, a, 0));
                let mut input = PlanarBuffer::new(1, CAP);
                input.channel_mut(0).fill(0.5);
                let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
                env.channel_mut(0).fill(1.0);
                env.channel_mut(1).fill(1.0);
                let mut out = PlanarBuffer::new(1, CAP);
                let mut prev: Option<f32> = None;
                let mut step = 0.0f32;
                let mut run = |m: &mut Matrix, frames: usize, step: &mut f32| {
                    m.process_ducked_on(path, &input, &env, &mut out, frames);
                    for &s in &out.channel(0)[..frames] {
                        if let Some(p) = prev {
                            *step = step.max((s - p).abs());
                        }
                        prev = Some(s);
                    }
                };
                // Settled, then to sample 2 of an interval.
                run(&mut m, 64 + 2, &mut 0.0);
                assert!(m.set_mask(0, 0, b, LEN));
                if skip {
                    m.advance(3);
                } else {
                    run(&mut m, 3, &mut step);
                }
                assert!(m.set_mask(0, 0, c, LEN));
                for _ in 0..140 {
                    run(&mut m, 37, &mut step);
                }
                let whole = 0.5 * (gain(a) - gain(c)).abs().max((gain(b) - gain(c)).abs());
                let ctx = format!("{path:?} {a:#b} -> {b:#b} -> {c:#b}, skip {skip}");
                assert!(step <= 1.1 * whole / LEN as f32, "{ctx}: step {step}");
                if a == c {
                    assert_eq!(step, 0.0, "{ctx}");
                }
                assert_eq!(prev, Some(0.5 * gain(c)), "{ctx}");
            }
        }
    }
}

/// A coefficient in several groups is ducked by the deepest of them,
/// not by their sum or by the first, and by none outside its mask.
#[test]
fn overlapping_groups_take_the_deepest() {
    for path in Path::ALL {
        if !path_runs("overlapping_groups_take_the_deepest", path) {
            continue;
        }
        let mut rng = Rng::new(0x0d);
        let mut m = Matrix::new(2, 1);
        assert!(m.set(1, 0, 0.5, 0));
        for (g, db) in [(0, 6.0), (1, 12.0), (2, 30.0)] {
            assert!(m.set_depth(1, g, db, 0));
        }
        assert!(m.set_mask(1, 0, 0b011, 0));
        let mut input = PlanarBuffer::new(2, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        let mut out = PlanarBuffer::new(1, CAP);
        for (e1, gain) in [(0.75, duck_gain(12.0, 0.75)), (0.25, duck_gain(6.0, 1.0))] {
            env.channel_mut(0).fill(1.0);
            env.channel_mut(1).fill(e1);
            env.channel_mut(2).fill(1.0);
            // Two intervals to settle, then a block at the gain.
            fill(&mut rng, &mut input, 16);
            m.process_ducked_on(path, &input, &env, &mut out, 16);
            fill(&mut rng, &mut input, 100);
            m.process_ducked_on(path, &input, &env, &mut out, 100);
            for n in 0..100 {
                let want = 0.0 + (0.5 * gain) * input.channel(1)[n];
                assert_eq!(out.channel(0)[n].to_bits(), want.to_bits(), "{path:?} {e1}");
            }
        }
    }
}

/// The outputs of one input each take the gain of their own mask
/// and depths, in every interval of every block.
#[test]
fn each_output_of_an_input_takes_its_own_mask() {
    for path in Path::ALL {
        if !path_runs("each_output_of_an_input_takes_its_own_mask", path) {
            continue;
        }
        let mut rng = Rng::new(0x77);
        let mut m = Matrix::new(1, 3);
        for (j, mask) in [0b01u8, 0b10, 0b11].into_iter().enumerate() {
            assert!(m.set(0, j, 0.5, 0));
            assert!(m.set_mask(0, j, mask, 0));
        }
        assert!(m.set_depth(0, 0, 10.0, 0) && m.set_depth(0, 1, 20.0, 0));
        let mut input = PlanarBuffer::new(1, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        let mut out = PlanarBuffer::new(3, CAP);
        for (e0, e1) in [(1.0, 0.25), (0.5, 1.0), (0.0, 0.0), (1.0, 1.0)] {
            env.channel_mut(0).fill(e0);
            env.channel_mut(1).fill(e1);
            fill(&mut rng, &mut input, 16);
            m.process_ducked_on(path, &input, &env, &mut out, 16);
            fill(&mut rng, &mut input, 40);
            m.process_ducked_on(path, &input, &env, &mut out, 40);
            let (a, b) = (duck_gain(10.0, e0), duck_gain(20.0, e1));
            let deepest = if 10.0 * e0 > 20.0 * e1 { a } else { b };
            for (j, gain) in [a, b, deepest].into_iter().enumerate() {
                for n in 0..40 {
                    let want = 0.0 + (0.5 * gain) * input.channel(0)[n];
                    let got = out.channel(j)[n];
                    assert_eq!(got.to_bits(), want.to_bits(), "{path:?} output {j}");
                }
            }
        }
    }
}

/// A matrix with ducks round-trips through its serde form mid-duck
/// and continues bit-identically; a matrix without has the form it
/// had before ducks existed.
#[test]
fn a_ducked_matrix_round_trips() {
    let mut rng = Rng::new(0x5e);
    let plain = {
        let mut m = Matrix::new(3, 2);
        change(&mut rng, &mut m, 6);
        m
    };
    let json = serde_json::to_string(&plain).unwrap();
    for key in ["mask", "duck", "depths", "phase"] {
        assert!(!json.contains(key), "{json}");
    }
    let mut m = Matrix::new(5, 4);
    change(&mut rng, &mut m, 20);
    assert!(m.set(1, 2, 0.5, 0) && m.set(3, 1, 0.25, 100));
    duck_changes(&mut rng, &mut m, 12);
    assert!(m.set_mask(1, 2, 0b101, 0) && m.set_depth(1, 0, 10.0, 0));
    let mut input = PlanarBuffer::new(5, CAP);
    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
    let (mut a, mut b) = (PlanarBuffer::new(4, CAP), PlanarBuffer::new(4, CAP));
    fill(&mut rng, &mut input, 203);
    envelopes(&mut rng, &mut env, 203);
    m.process_ducked(&input, &env, &mut a, 203);
    assert!(m.set_mask(1, 2, 0b010, 300) && m.set_mask(3, 1, 0, 300));
    m.process_ducked(&input, &env, &mut a, 30);
    assert!(m.phase != 0 && m.ducks.iter().flatten().any(|d| d.pos < d.len));
    let json = serde_json::to_string(&m).unwrap();
    let mut copy: Matrix = serde_json::from_str(&json).unwrap();
    assert_eq!(copy, m);
    for _ in 0..4 {
        fill(&mut rng, &mut input, 150);
        envelopes(&mut rng, &mut env, 150);
        m.process_ducked(&input, &env, &mut a, 150);
        copy.process_ducked(&input, &env, &mut b, 150);
        assert_bits_eq(&a, &b, 150, "after the round trip");
    }
}

/// What no matrix holds of ducks is refused when decoded.
#[test]
fn a_duck_no_matrix_holds_is_refused() {
    use serde_json::{Value, json};
    let mut m = Matrix::new(2, 2);
    assert!(m.set(1, 0, 0.5, 0) && m.set(0, 1, 0.5, 0));
    assert!(m.set_mask(1, 0, 0b11, 100) && m.set_mask(0, 0, 0b100, 0));
    assert!(m.set_depth(1, 1, 10.0, 50));
    m.advance(3);
    let wire: Value = serde_json::to_value(&m).unwrap();
    assert!(serde_json::from_value::<Matrix>(wire.clone()).is_ok());
    fn term(w: &mut Value) -> &mut Value {
        &mut w["columns"][0][0]
    }
    let refused = |what: &str, f: &dyn Fn(&mut Value)| {
        let mut w = wire.clone();
        f(&mut w);
        assert!(serde_json::from_value::<Matrix>(w).is_err(), "{what}");
    };
    refused("a term mask other than the table's", &|w| {
        term(w)["mask"] = json!(1);
    });
    refused("a mask with no duck", &|w| {
        term(w).as_object_mut().unwrap().remove("duck");
    });
    refused("a duck gain above 1", &|w| {
        term(w)["duck"]["to"] = json!(1.5);
    });
    refused("a negative duck gain", &|w| {
        term(w)["duck"]["live"] = json!(-0.25);
    });
    refused("a live gain of mask 0 other than 1", &|w| {
        term(w)["duck"]["live_mask"] = json!(0);
        term(w)["duck"]["live"] = json!(0.5);
    });
    refused("a bias beyond the distance of two gains", &|w| {
        term(w)["duck"]["bias"] = json!(1.5);
    });
    refused("a crossfade past its length", &|w| {
        term(w)["duck"]["pos"] = json!(101);
    });
    refused("a crossfade too long", &|w| {
        term(w)["duck"]["len"] = json!(MAX_RAMP_LEN + 1);
        term(w)["duck"]["pos"] = json!(0);
    });
    refused("a mask of 0 listed", &|w| w["masks"][0][1] = json!(0));
    refused("a mask past the coefficients", &|w| {
        w["masks"][1][0] = json!(4);
    });
    refused("masks out of order", &|w| {
        w["masks"] = json!([[2, 3], [0, 4]]);
    });
    refused("a depth beyond 40 dB", &|w| {
        w["depths"][0][1]["target"] = json!(41.0);
    });
    refused("a depth past the inputs", &|w| {
        w["depths"][0][0] = json!(2 * MAX_DUCKS);
    });
    refused("a position past the interval", &|w| {
        w["phase"] = json!(GAIN_INTERVAL);
    });
}

/// A matrix the size of which a peer names beyond the limit in its
/// inputs alone is refused before anything is allocated for it: with
/// no output it has no coefficient, but a duck table per input.
#[test]
fn a_matrix_of_too_many_inputs_is_refused() {
    for inputs in [MAX_COEFFICIENTS + 1, 1 << 34, usize::MAX / 2] {
        let wire = format!("{{\"inputs\":{inputs},\"columns\":[]}}");
        assert!(serde_json::from_str::<Matrix>(&wire).is_err(), "{inputs}");
    }
    let wire = format!("{{\"inputs\":{MAX_COEFFICIENTS},\"columns\":[]}}");
    assert!(serde_json::from_str::<Matrix>(&wire).is_ok());
}

/// The top-level and term keys of `m`'s serde form.
fn keys(m: &Matrix) -> (Vec<String>, Vec<String>) {
    let v = serde_json::to_value(m).unwrap();
    let names = |o: &serde_json::Value| o.as_object().unwrap().keys().cloned().collect();
    let term = v["columns"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|c| c.as_array().unwrap())
        .map(names)
        .fold(Vec::new(), |mut all: Vec<String>, k: Vec<String>| {
            all.extend(k);
            all.sort();
            all.dedup();
            all
        });
    (names(&v), term)
}

/// A matrix with no duck, processed or skipped over any number of
/// samples, has the serde form a matrix had before ducks existed,
/// and one that set masks and depths and cleared them again equals
/// and encodes as one that never set any.
#[test]
fn a_matrix_without_ducks_keeps_its_form() {
    let mut rng = Rng::new(0xf0);
    let mut m = Matrix::new(3, 2);
    change(&mut rng, &mut m, 6);
    assert!(m.set(1, 1, 0.5, 0));
    let mut input = PlanarBuffer::new(3, CAP);
    let mut out = PlanarBuffer::new(2, CAP);
    fill(&mut rng, &mut input, 45);
    m.process(&input, &mut out, 45);
    m.advance(2);
    assert_eq!(m.phase, 47 % 8);
    let old = (
        vec!["columns".to_owned(), "inputs".to_owned()],
        vec!["input".to_owned(), "ramp".to_owned()],
    );
    assert_eq!(keys(&m), old);
    let copy: Matrix = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
    assert_eq!(copy, m);

    let fresh = m.clone();
    assert!(m.set_mask(1, 1, 0b11, 0) && m.set_depth(2, 3, 12.0, 0));
    assert_ne!(m, fresh);
    assert!(m.set_mask(1, 1, 0, 0) && m.set_depth(2, 3, 0.0, 0));
    m.process_ducked(&input, &PlanarBuffer::new(MAX_DUCKS, CAP), &mut out, 13);
    let mut fresh = fresh;
    fresh.process(&input, &mut out, 13);
    assert!(!m.has_ducks() && !m.masks.is_empty() && fresh.masks.is_empty());
    assert_eq!(m, fresh);
    assert_eq!(
        serde_json::to_string(&m).unwrap(),
        serde_json::to_string(&fresh).unwrap()
    );
    assert_eq!(keys(&m), old);
}

/// A matrix with ducks processed without envelopes would release
/// every duck within one interval, so it is refused in debug builds.
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "without envelopes")]
fn process_refuses_a_matrix_with_ducks() {
    let mut m = Matrix::new(1, 1);
    assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
    let input = PlanarBuffer::new(1, 8);
    m.process(&input, &mut PlanarBuffer::new(1, 8), 8);
}

/// A fade-in brings a coefficient with a mask back ducked, from no
/// duck to its mask's gain over the fade's length.
#[test]
fn a_fade_in_keeps_the_masks() {
    for path in Path::ALL {
        if !path_runs("a_fade_in_keeps_the_masks", path) {
            continue;
        }
        let mut rng = Rng::new(0xfade);
        let mut m = Matrix::new(3, 2);
        assert!(m.set_mask(0, 1, 0b1, 0) && m.set_mask(2, 1, 0b10, 0));
        assert!(m.set_depth(0, 0, 20.0, 0) && m.set_depth(2, 1, 10.0, 0));
        assert!(m.fade_in(&[0.0, 0.5, 0.25, 0.0, 0.0, 0.75], 100));
        assert_eq!(m.ducks[1].len(), 2);
        let mut input = PlanarBuffer::new(3, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        env.channel_mut(1).fill(1.0);
        let mut out = PlanarBuffer::new(2, CAP);
        fill(&mut rng, &mut input, 120);
        m.process_ducked_on(path, &input, &env, &mut out, 120);
        fill(&mut rng, &mut input, 50);
        m.process_ducked_on(path, &input, &env, &mut out, 50);
        let (a, b) = (duck_gain(20.0, 1.0), duck_gain(10.0, 1.0));
        for n in 0..50 {
            let x = |i: usize| input.channel(i)[n];
            let want = 0.0 + (0.5 * a) * x(0) + (0.75 * b) * x(2);
            assert_eq!(out.channel(1)[n].to_bits(), want.to_bits(), "{path:?}");
            assert_eq!(out.channel(0)[n].to_bits(), (0.0 + 0.25 * x(1)).to_bits());
        }
    }
}

/// The duck gain of a ducked term with every input sample 1: its
/// output divided by its coefficient.
fn duck_of(out: &PlanarBuffer, n: usize, coefficient: f32) -> f32 {
    out.channel(0)[n] / coefficient
}

/// A coefficient that comes back from 0 with its mask set is ducked
/// from the start, with no crossfade of the duck: its own ramp from
/// silence hides the duck gain stepping in, and an install builds the
/// same state.
#[test]
fn a_coefficient_back_from_0_is_ducked_from_the_start() {
    let mut m = Matrix::new(1, 1);
    assert!(m.set_depth(0, 0, 20.0, 0) && m.set_mask(0, 0, 1, 0));
    assert!(m.set(0, 0, 1.0, 0));
    let mut input = PlanarBuffer::new(1, CAP);
    input.channel_mut(0).fill(1.0);
    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
    env.channel_mut(0).fill(1.0);
    let mut out = PlanarBuffer::new(1, CAP);
    m.process_ducked(&input, &env, &mut out, 64);
    let full = duck_gain(20.0, 1.0);
    assert_eq!(duck_of(&out, 63, 1.0), full);
    assert!(m.set(0, 0, 0.0, 0) && m.ducks[0].is_empty());
    m.process_ducked(&input, &env, &mut out, 64);
    assert!(m.set(0, 0, 1.0, 480));
    m.process_ducked(&input, &env, &mut out, 480);
    let mid = duck_of(&out, 240, 240.0 / 480.0);
    assert!((mid - full).abs() < 0.02, "{mid}, want about {full}");
}

/// A depth change while ducked ramps: the output does not step.
#[test]
fn a_depth_change_while_ducked_ramps() {
    for path in Path::ALL {
        if !path_runs("a_depth_change_while_ducked_ramps", path) {
            continue;
        }
        let run = |len: u32| {
            let mut m = Matrix::new(1, 1);
            assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
            let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
            env.channel_mut(0).fill(1.0);
            let prev = &mut None;
            largest_step(&mut m, path, &env, prev, 3);
            assert!(m.set_depth(0, 0, 40.0, len));
            let step = largest_step(&mut m, path, &env, prev, 20);
            assert_eq!(m.depth(0, 0), 40.0);
            step
        };
        let (ramped, jumped) = (run(480), run(0));
        assert!(
            ramped * 8.0 < jumped,
            "{path:?}: {ramped} ramped, {jumped} jumped"
        );
    }
}

/// A new position in the interval holds the duck gain where it is,
/// also in the middle of a move.
#[test]
fn align_holds_the_gain_mid_interval() {
    let mut m = Matrix::new(1, 1);
    assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 1, 0));
    assert!(m.set_depth(0, 0, 20.0, 0));
    let mut input = PlanarBuffer::new(1, CAP);
    input.channel_mut(0).fill(1.0);
    let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
    env.channel_mut(0).fill(1.0);
    let mut out = PlanarBuffer::new(1, CAP);
    m.process_ducked(&input, &env, &mut out, 11);
    let d = m.ducks[0][0];
    assert!(d.from == 1.0 && d.to < 1.0);
    m.align(6);
    m.process_ducked(&input, &env, &mut out, 2);
    assert_eq!(out.channel(0)[..2], [d.to; 2]);
}

/// A mask changed while its old group releases follows the old group
/// out: from the third interval after the change on, the term is never
/// ducked deeper than the shallower of the old and the new mask's live
/// gains, as it would be crossfading from the gain frozen at the
/// change.
#[test]
fn a_mask_crossfade_follows_the_live_gains() {
    for path in Path::ALL {
        if !path_runs("a_mask_crossfade_follows_the_live_gains", path) {
            continue;
        }
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0) && m.set_mask(0, 0, 0b01, 0));
        assert!(m.set_depth(0, 0, 20.0, 0) && m.set_depth(0, 1, 10.0, 0));
        let mut input = PlanarBuffer::new(1, CAP);
        input.channel_mut(0).fill(1.0);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        env.channel_mut(1).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        m.process_ducked_on(path, &input, &env, &mut out, 64);
        assert!(m.set_mask(0, 0, 0b10, 4800));
        // The old group releases, by one step of its envelope per
        // block, while the crossfade runs.
        let new = duck_gain(10.0, 1.0);
        let mut index = 0;
        for b in 0..100 {
            let e = (1.0 - b as f32 / 20.0).max(0.0);
            env.channel_mut(0).fill(e);
            m.process_ducked_on(path, &input, &env, &mut out, 48);
            if b > 0 {
                let old = duck_gain(20.0, (e + 1.0 / 20.0).min(1.0));
                let floor = old.min(new);
                for n in 0..48 {
                    let g = out.channel(0)[n];
                    assert!(g >= floor - 1e-6, "{path:?} sample {index}: {g} < {floor}");
                    index += 1;
                }
            }
        }
        m.process_ducked_on(path, &input, &env, &mut out, 48);
        assert_eq!(out.channel(0)[47], new);
    }
}

/// Masks and depths set out of range or to values no matrix takes are
/// refused and change nothing; a mask survives its coefficient going
/// to 0 and coming back.
#[test]
fn masks_and_depths_are_checked_and_kept() {
    let mut m = Matrix::new(2, 3);
    assert!(!m.set_mask(2, 0, 1, 0) && !m.set_mask(0, 3, 1, 0));
    for (i, g, db) in [(2, 0, 1.0), (0, MAX_DUCKS, 1.0), (0, 0, -1.0), (0, 0, 40.5)] {
        assert!(!m.set_depth(i, g, db, 0), "{i} {g} {db}");
    }
    assert!(!m.set_depth(0, 0, f32::NAN, 0));
    assert_eq!(m, Matrix::new(2, 3));
    assert!(m.set_depth(1, 7, 40.0, 10));
    assert_eq!(m.depth(1, 7), 0.0);
    m.advance(10);
    assert_eq!(m.depth(1, 7), 40.0);
    assert!(m.set_mask(1, 2, 0b1000_0001, 0));
    assert_eq!(m.mask(1, 2), 0b1000_0001);
    assert!(m.set(1, 2, 0.5, 0) && m.set(1, 2, 0.0, 0));
    assert!(m.columns[2].is_empty() && m.ducks[2].is_empty());
    assert!(m.set(1, 2, 0.5, 0));
    assert_eq!(m.columns[2][0].mask, 0b1000_0001);
    assert_eq!(m.ducks[2].len(), 1);
    assert_eq!((m.mask(2, 0), m.depth(0, MAX_DUCKS)), (0, 0.0));
}

/// A delayed fade mixes to the same bits on the best path and the
/// scalar one, and in any block sizes.
#[test]
fn a_delayed_fade_is_bit_exact_across_paths_and_block_sizes() {
    let frames = 1100;
    let mut rng = Rng::new(0x51de);
    let mut input = PlanarBuffer::new(3, frames);
    fill(&mut rng, &mut input, frames);
    let setup = || {
        let mut m = Matrix::new(3, 2);
        for i in 0..3 {
            for o in 0..2 {
                assert!(m.set(i, o, 0.5 + 0.25 * i as f32, 0));
            }
        }
        assert!(m.set_fade(0, 0, 0.0, 150, 400));
        assert!(m.set_fade(1, 0, 2.0, 301, 17));
        assert!(m.set_fade(2, 1, 0.125, 37, 0));
        m
    };
    let run = |path: Path, sizes: &[usize]| {
        let mut m = setup();
        let mut out = PlanarBuffer::new(2, frames);
        let mut at = 0;
        let mut per = vec![Vec::new(), Vec::new()];
        for &n in sizes.iter().cycle() {
            if at + n > frames {
                break;
            }
            let mut block = PlanarBuffer::new(3, n);
            for c in 0..3 {
                block.channel_mut(c)[..n].copy_from_slice(&input.channel(c)[at..at + n]);
            }
            m.process_on(path, &block, &mut out, n);
            for (c, p) in per.iter_mut().enumerate() {
                p.extend(out.channel(c)[..n].iter().map(|v| v.to_bits()));
            }
            at += n;
        }
        per
    };
    let want = run(Path::Scalar, &[1100]);
    assert_eq!(want[0].len(), frames);
    for path in [Path::Scalar, Path::Avx2] {
        if !path.is_supported() {
            continue;
        }
        for sizes in [&[1100][..], &[100, 100, 55], &[8, 13, 64], &[1]] {
            let got = run(path, sizes);
            for c in 0..2 {
                assert_eq!(got[c], want[c][..got[c].len()], "{path:?} {sizes:?}");
            }
        }
    }
}

/// A masked coefficient a fade brings in is ducked as an install of
/// the same fade builds it, with no crossfade of its own: the fade
/// ramps it up from silence, so a replica rebuilt mid-fade holds the
/// bits of one that applied the entry.
#[test]
fn a_fade_inserted_masked_term_matches_an_install_of_it() {
    let ramp = Ramp::resume(0.0, 0.25, 40, 400, 0).unwrap();
    let mut live = Matrix::new(1, 1);
    assert!(live.set_mask(0, 0, 0b1, 0));
    assert!(live.set_resumed(0, 0, 0.25, Some(ramp)));
    let mut built = Matrix::new(1, 1);
    assert!(built.fade_in_ducked_resuming(&[0.25], &[0b1], &[], 0, &[(0, ramp)]));
    assert!(live == built);
}

/// A matrix filled at index 272 and given the mask changes a running
/// one took at 200 and, inside that crossfade, at 248, under a steady
/// envelope, mixes the running one's bits from its first sample, both
/// crossfades included.
#[test]
fn a_resumed_mask_crossfade_mixes_the_bits_of_a_running_matrix() {
    for path in Path::ALL {
        if !path_runs(
            "a_resumed_mask_crossfade_mixes_the_bits_of_a_running_matrix",
            path,
        ) {
            continue;
        }
        let mut rng = Rng::new(0x3a5c);
        let mut input = PlanarBuffer::new(1, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        let mut live = Matrix::new(1, 1);
        assert!(live.set_depth(0, 0, 20.0, 0) && live.set(0, 0, 0.5, 0));
        for (frames, mask) in [(200, 1), (48, 0), (24, 0)] {
            fill(&mut rng, &mut input, frames);
            live.process_ducked_on(path, &input, &env, &mut out, frames);
            assert!(live.set_mask(0, 0, mask, 96));
        }
        let mut depths = vec![0.0; MAX_DUCKS];
        depths[0] = 20.0;
        let mut built = Matrix::new(1, 1);
        built.fade_in_ducked(&[0.5], &[0], &depths, 0);
        built.align(272);
        built.settle_ducks(&[1.0]);
        let depth = |g: usize, _| if g == 0 { 20.0 } else { 0.0 };
        let changes: [(u64, u8); 2] = [(200, 1), (248, 0)];
        assert!(built.resume_mask((0, 0), (0, &changes), 96, &[1.0], depth, 272));
        fill(&mut rng, &mut input, 64);
        let mut want = PlanarBuffer::new(1, CAP);
        live.process_ducked_on(path, &input, &env, &mut want, 64);
        built.process_ducked_on(path, &input, &env, &mut out, 64);
        for n in 0..64 {
            assert_eq!(
                out.channel(0)[n].to_bits(),
                want.channel(0)[n].to_bits(),
                "{path:?} sample {n}"
            );
        }
        assert_ne!(
            want.channel(0)[0],
            0.5 * input.channel(0)[0],
            "no crossfade"
        );
    }
}

/// A matrix filled with its masked terms settled at steady envelopes
/// mixes from its first sample the bits of one that has run at those
/// envelopes for a while: the duck does not start at 1 and move to its
/// gain over two intervals.
#[test]
fn a_settled_fill_mixes_the_bits_of_a_running_matrix() {
    for path in Path::ALL {
        if !path_runs("a_settled_fill_mixes_the_bits_of_a_running_matrix", path) {
            continue;
        }
        let mut rng = Rng::new(0x005e_771e);
        let mut input = PlanarBuffer::new(1, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        let mut live = Matrix::new(1, 1);
        assert!(live.set_depth(0, 0, 20.0, 0) && live.set_mask(0, 0, 1, 0));
        assert!(live.set(0, 0, 0.5, 0));
        fill(&mut rng, &mut input, 200);
        live.process_ducked_on(path, &input, &env, &mut out, 200);
        let mut depths = vec![0.0; MAX_DUCKS];
        depths[0] = 20.0;
        let mut built = Matrix::new(1, 1);
        built.fade_in_ducked(&[0.5], &[1], &depths, 0);
        built.settle_ducks(&[1.0]);
        fill(&mut rng, &mut input, 64);
        let mut want = PlanarBuffer::new(1, CAP);
        live.process_ducked_on(path, &input, &env, &mut want, 64);
        built.process_ducked_on(path, &input, &env, &mut out, 64);
        for n in 0..64 {
            assert_eq!(
                out.channel(0)[n].to_bits(),
                want.channel(0)[n].to_bits(),
                "{path:?} sample {n}"
            );
        }
    }
}

/// A matrix filled at index 272 with a depth ramp that started at
/// `start` while an earlier one still ran there, settled at a steady
/// envelope, mixes the bits of a running matrix over the first
/// interval only where the later ramp started at or before 263, the
/// second last end of an interval before the fill: a depth ramp is
/// taken to have held its start value before its start, where the
/// running matrix had the earlier ramp's.
#[test]
fn a_depth_changed_again_settles_alike_only_two_interval_ends_after_it() {
    let at: usize = 272;
    for (start, alike) in [(at - 9, true), (at - 8, false), (at, false)] {
        let mut rng = Rng::new(0xde97);
        let mut input = PlanarBuffer::new(1, CAP);
        let mut env = PlanarBuffer::new(MAX_DUCKS, CAP);
        env.channel_mut(0).fill(1.0);
        let mut out = PlanarBuffer::new(1, CAP);
        let mut live = Matrix::new(1, 1);
        assert!(live.set_depth(0, 0, 10.0, 0) && live.set_mask(0, 0, 1, 0));
        assert!(live.set(0, 0, 0.5, 0));
        fill(&mut rng, &mut input, 200);
        live.process_ducked_on(Path::Scalar, &input, &env, &mut out, 200);
        // The earlier ramp, still running at the fill.
        assert!(live.set_depth(0, 0, 30.0, 480));
        fill(&mut rng, &mut input, start - 200);
        live.process_ducked_on(Path::Scalar, &input, &env, &mut out, start - 200);
        let from = live.depth(0, 0);
        assert!(live.set_depth(0, 0, 20.0, 480));
        if at > start {
            fill(&mut rng, &mut input, at - start);
            live.process_ducked_on(Path::Scalar, &input, &env, &mut out, at - start);
        }
        let mut depths = vec![0.0; MAX_DUCKS];
        depths[0] = 20.0;
        let mut built = Matrix::new(1, 1);
        built.fade_in_ducked(&[0.5], &[1], &depths, 0);
        let ramp = Ramp::resume(from, 20.0, 0, 480, (at - start) as u32).unwrap();
        assert!(built.resume_depth(0, 0, ramp));
        built.align(at as u64);
        built.settle_ducks(&[1.0]);
        fill(&mut rng, &mut input, 8);
        let mut want = PlanarBuffer::new(1, CAP);
        live.process_ducked_on(Path::Scalar, &input, &env, &mut want, 8);
        built.process_ducked_on(Path::Scalar, &input, &env, &mut out, 8);
        let same = (0..8).all(|n| out.channel(0)[n].to_bits() == want.channel(0)[n].to_bits());
        assert_eq!(same, alike, "the later ramp from {start}");
    }
}
