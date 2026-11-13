//! AVX2 kernel of the matrix. It computes, per sample, the operations
//! of [`mix_scalar`] in the same order: acc = +0.0, then
//! acc = acc + g * x per term in ascending input order, with the gain
//! g = c0 + k * step inside a ramp and g = target outside it.

use std::arch::x86_64::*;

use crate::PlanarBuffer;
use crate::matrix::{Term, mix_scalar};

/// Mixes `terms` from `input` into `out` with AVX2.
#[target_feature(enable = "avx2")]
pub(crate) fn mix_avx2(terms: &[Term], input: &PlanarBuffer, out: &mut [f32]) {
    let frames = out.len();
    let mut n = 0;
    while n + 32 <= frames {
        block_avx2::<4>(terms, input, out, n);
        n += 32;
    }
    while n + 8 <= frames {
        block_avx2::<1>(terms, input, out, n);
        n += 8;
    }
    mix_scalar(terms, input, out, n);
}

/// Samples `n..n + 8 * U` of one output, accumulated in registers.
#[target_feature(enable = "avx2")]
#[inline]
fn block_avx2<const U: usize>(terms: &[Term], input: &PlanarBuffer, out: &mut [f32], n: usize) {
    let lane = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
    let mut acc = [_mm256_setzero_ps(); U];
    for t in terms {
        let x = &input.channel(t.input)[n..n + 8 * U];
        let r = &t.ramp;
        if r.is_active() {
            let c0 = _mm256_set1_ps(r.c0);
            let step = _mm256_set1_ps(r.step);
            let target = _mm256_set1_ps(r.target);
            let len = _mm256_set1_epi32(r.len as i32);
            let k0 = _mm256_add_epi32(_mm256_set1_epi32((r.pos as usize + n) as i32), lane);
            for (u, a) in acc.iter_mut().enumerate() {
                let k = _mm256_add_epi32(k0, _mm256_set1_epi32(8 * u as i32));
                let ramp = _mm256_add_ps(c0, _mm256_mul_ps(_mm256_cvtepi32_ps(k), step));
                let inside = _mm256_castsi256_ps(_mm256_cmpgt_epi32(len, k));
                let g = _mm256_blendv_ps(target, ramp, inside);
                // SAFETY: `x` holds 8 * U samples.
                let v = unsafe { _mm256_loadu_ps(x.as_ptr().add(8 * u)) };
                *a = _mm256_add_ps(*a, _mm256_mul_ps(g, v));
            }
        } else {
            let g = _mm256_set1_ps(r.target);
            for (u, a) in acc.iter_mut().enumerate() {
                // SAFETY: `x` holds 8 * U samples.
                let v = unsafe { _mm256_loadu_ps(x.as_ptr().add(8 * u)) };
                *a = _mm256_add_ps(*a, _mm256_mul_ps(g, v));
            }
        }
    }
    let o = &mut out[n..n + 8 * U];
    for (u, a) in acc.iter().enumerate() {
        // SAFETY: `o` holds 8 * U samples.
        unsafe { _mm256_storeu_ps(o.as_mut_ptr().add(8 * u), *a) };
    }
}
