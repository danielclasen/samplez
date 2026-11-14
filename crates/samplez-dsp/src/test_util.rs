use crate::Path;
use crate::require::parse_paths;

/// xorshift64* generator, seeded per test so failures reproduce.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `0..n`.
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Uniform in [-1, 1).
    pub(crate) fn unit(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }

    /// A finite sample, never -0.0: mostly uniform audio, some denormals,
    /// some normals small enough that products with small coefficients
    /// underflow, some exact zeros.
    pub(crate) fn sample(&mut self) -> f32 {
        match self.below(32) {
            0 | 1 => {
                let mantissa = (self.next_u64() as u32 & 0x007f_ffff).max(1);
                let sign = (self.next_u64() as u32 & 1) << 31;
                f32::from_bits(sign | mantissa)
            }
            2 | 3 => self.unit() * 1e-37,
            4 => 0.0,
            _ => self.unit() * 4.0,
        }
    }

    /// A matrix coefficient: zeros, exact unity, tiny values and uniform
    /// gains.
    pub(crate) fn coefficient(&mut self) -> f32 {
        match self.below(16) {
            0..=3 => 0.0,
            4 | 5 => 1.0,
            6 => self.unit() * 1e-3,
            _ => self.unit() * 2.0,
        }
    }
}

#[test]
fn required_paths_parse() {
    assert_eq!(parse_paths(""), Ok(vec![]));
    assert_eq!(
        parse_paths(" Scalar, avx2 ,"),
        Ok(vec![Path::Scalar, Path::Avx2])
    );
    assert!(parse_paths("avx2,avx512").is_err());
}
