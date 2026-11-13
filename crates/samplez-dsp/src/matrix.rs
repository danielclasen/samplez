use crate::{PlanarBuffer, Ramp, simd};

/// Instruction set a [`Matrix`] runs on. Every path gives bit-identical
/// output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Scalar,
    Avx2,
}

impl Path {
    pub const ALL: [Path; 2] = [Path::Scalar, Path::Avx2];

    /// Whether the running CPU supports this path.
    pub fn is_supported(self) -> bool {
        match self {
            Path::Scalar => true,
            Path::Avx2 => is_x86_feature_detected!("avx2"),
        }
    }

    /// The widest path the running CPU supports.
    pub fn best() -> Path {
        if Path::Avx2.is_supported() {
            Path::Avx2
        } else {
            Path::Scalar
        }
    }
}

/// One nonzero or ramping coefficient of an output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Term {
    pub(crate) input: usize,
    pub(crate) ramp: Ramp,
}

/// Mixing matrix of N inputs and M outputs:
/// `out[j][n] = sum_i M[i][j] * in[i][n]`.
///
/// Each output sums its nonzero coefficients in ascending input order,
/// starting from +0.0, with a multiply and an add per term and no FMA. A
/// coefficient change ramps per [`Ramp`]. An output with no coefficient is
/// filled with +0.0; an output whose only coefficient is a settled 1.0 is a
/// copy of its input.
#[derive(Clone, Debug, PartialEq)]
pub struct Matrix {
    inputs: usize,
    /// Per output, the nonzero or ramping coefficients sorted by input.
    columns: Vec<Vec<Term>>,
    path: Path,
}

impl Matrix {
    /// An all-zero matrix running on [`Path::best`].
    pub fn new(inputs: usize, outputs: usize) -> Self {
        Self {
            inputs,
            columns: (0..outputs).map(|_| Vec::with_capacity(inputs)).collect(),
            path: Path::best(),
        }
    }

    pub fn inputs(&self) -> usize {
        self.inputs
    }

    pub fn outputs(&self) -> usize {
        self.columns.len()
    }

    /// The path [`Matrix::process`] runs on.
    pub fn path(&self) -> Path {
        self.path
    }

    /// Value the coefficient from `input` to `output` has on the next
    /// sample.
    pub fn gain(&self, input: usize, output: usize) -> f32 {
        let col = &self.columns[output];
        col.binary_search_by_key(&input, |t| t.input)
            .map_or(0.0, |k| col[k].ramp.current())
    }

    /// Ramps the coefficient from `input` to `output` to `target` over
    /// `len` samples, per [`Ramp::set_target`].
    pub fn set(&mut self, input: usize, output: usize, target: f32, len: u32) {
        assert!(input < self.inputs, "input {input} out of range");
        let col = &mut self.columns[output];
        match col.binary_search_by_key(&input, |t| t.input) {
            Ok(k) => {
                col[k].ramp.set_target(target, len);
                if !col[k].ramp.is_active() && target == 0.0 {
                    col.remove(k);
                }
            }
            Err(k) if target != 0.0 => {
                let mut ramp = Ramp::new(0.0);
                ramp.set_target(target, len);
                col.insert(k, Term { input, ramp });
            }
            Err(_) => {}
        }
    }

    /// Mixes the first `frames` frames of `input` into `output` on
    /// [`Matrix::path`].
    pub fn process(&mut self, input: &PlanarBuffer, output: &mut PlanarBuffer, frames: usize) {
        self.process_on(self.path, input, output, frames);
    }

    /// Mixes the first `frames` frames of `input` into `output` on `path`.
    ///
    /// # Panics
    ///
    /// If the CPU does not support `path`, or the buffer shapes do not
    /// match the matrix.
    pub fn process_on(
        &mut self,
        path: Path,
        input: &PlanarBuffer,
        output: &mut PlanarBuffer,
        frames: usize,
    ) {
        assert!(path.is_supported(), "{path:?} is not supported by this CPU");
        assert_eq!(input.channels(), self.inputs);
        assert_eq!(output.channels(), self.columns.len());
        assert!(frames <= input.capacity() && frames <= output.capacity());
        for (j, col) in self.columns.iter_mut().enumerate() {
            let out = &mut output.channel_mut(j)[..frames];
            match col.as_slice() {
                [] => out.fill(0.0),
                [t] if !t.ramp.is_active() && t.ramp.target == 1.0 => {
                    out.copy_from_slice(&input.channel(t.input)[..frames]);
                }
                terms => match path {
                    Path::Scalar => mix_scalar(terms, input, out, 0),
                    // SAFETY: the CPU supports AVX2, asserted above.
                    Path::Avx2 => unsafe { simd::mix_avx2(terms, input, out) },
                },
            }
            for t in col.iter_mut() {
                t.ramp.advance(frames);
            }
            col.retain(|t| t.ramp.is_active() || t.ramp.target != 0.0);
        }
    }
}

/// Scalar reference for one output, from sample `start` to the end of
/// `out`. The SIMD paths compute the same operations in the same order.
pub(crate) fn mix_scalar(terms: &[Term], input: &PlanarBuffer, out: &mut [f32], start: usize) {
    for (n, o) in out.iter_mut().enumerate().skip(start) {
        let mut acc = 0.0f32;
        for t in terms {
            let g = t.ramp.value_at(t.ramp.pos.saturating_add(n as u32));
            acc += g * input.channel(t.input)[n];
        }
        *o = acc;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{Rng, path_runs};
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
            m.set(i, j, c, len);
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
    fn bit_exact_avx2() {
        bit_exact_vs_scalar(Path::Avx2, 0xa2);
    }

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
                    mat.set(i, j, coef[i * m + j], 0);
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
            mat.set(0, 0, 0.5, 0);
            mat.set(1, 0, -0.25, 0);
            mat.set(2, 1, 1.0, 0);
            let mut input = PlanarBuffer::new(3, CAP);
            let mut out = PlanarBuffer::new(2, CAP);

            // Block 1: output 0 ramps input 0 from 0.5 to 0.9 over 300 and
            // input 2 in from 0 to 0.3 over 77; output 1 ramps its route
            // down to 0.125 over 200.
            mat.set(0, 0, 0.9, 300);
            mat.set(2, 0, 0.3, 77);
            mat.set(2, 1, 0.125, 200);
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
            mat.set(0, 0, 0.0, 100);
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
                mat.set(i, j, 1.0, 0);
            }
            mat.set(3, 8, 1.0, 0);
            mat.set(0, 9, 1.0, 0);
            let mut out = PlanarBuffer::new(m, CAP);
            mat.process_on(path, &input, &mut out, frames);
            mat.set(0, 9, 0.0, 64);
            mat.set(5, 9, 1.0, 64);
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
}
