//! Level conversions between dB and gain shared by the gain computers:
//! libm in f64 for values set from parameters, and pure f32 `log2` and
//! `exp2` for values computed while processing.

/// `10^(db / 20)` through libm in f64: the same bits on every host.
pub(crate) fn db_to_gain(db: f32) -> f32 {
    libm::pow(10.0, f64::from(db) / 20.0) as f32
}

/// `20 log10(2)`: dB per octave of gain.
pub(crate) const DB_PER_OCTAVE: f32 = 6.020_6;
/// `1 / DB_PER_OCTAVE`.
pub(crate) const OCTAVES_PER_DB: f32 = 0.166_096_4;

/// `log2(x)` of a positive normal `x`, within 1e-7 plus rounding.
///
/// The gain computers run on this and [`exp2`] rather than
/// libm's `logf` and `expf`: a few multiplies and adds with no call,
/// pure f32 arithmetic without FMA, so the same bits on every host and
/// build whatever the libm version. The mantissa is taken to
/// [1/sqrt(2), sqrt(2)] and its logarithm summed as `2 atanh(t)` with
/// `t = (m - 1) / (m + 1)`, `|t| <= 0.172`, to the `t^7` term.
#[inline]
pub(crate) fn log2(x: f32) -> f32 {
    const C1: f32 = 2.885_39;
    const C3: f32 = 0.961_796_7;
    const C5: f32 = 0.577_078;
    const C7: f32 = 0.412_198_6;
    let bits = x.to_bits();
    let mut e = ((bits >> 23) & 0xff) as i32 - 127;
    let mut m = f32::from_bits((bits & 0x007f_ffff) | 0x3f80_0000);
    if m > core::f32::consts::SQRT_2 {
        m *= 0.5;
        e += 1;
    }
    let t = (m - 1.0) / (m + 1.0);
    let t2 = t * t;
    e as f32 + t * (C1 + t2 * (C3 + t2 * (C5 + t2 * C7)))
}

/// `2^x` within 2e-7 relative, 0 below -126, `2^127` above 127: the
/// nearest integer `n` as the exponent, `2^(x - n)` with `|x - n| <= 0.5`
/// by its Taylor series to the 6th power. Pure f32 arithmetic, see
/// [`log2`].
#[inline]
pub(crate) fn exp2(x: f32) -> f32 {
    const E: [f32; 6] = [
        core::f32::consts::LN_2,
        0.240_226_5,
        0.055_504_11,
        0.009_618_129,
        0.001_333_356,
        0.000_154_035_3,
    ];
    if x < -126.0 {
        return 0.0;
    }
    let x = x.min(127.0);
    // x + 127.5 is positive, so the conversion's truncation is a floor.
    let n = (x + 127.5) as i32 - 127;
    let f = x - n as f32;
    let p = 1.0 + f * (E[0] + f * (E[1] + f * (E[2] + f * (E[3] + f * (E[4] + f * E[5])))));
    p * f32::from_bits(((n + 127) as u32) << 23)
}

/// [`db_to_gain`] in f32, for the computed gain.
#[inline]
pub(crate) fn db_to_gain_f32(db: f32) -> f32 {
    exp2(db * OCTAVES_PER_DB)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// log2 and exp2 stay within 0.001 dB of the exact ones over every
    /// key and gain the units meet, and compute the bits pinned here: a
    /// build or host giving others fails.
    #[test]
    fn log2_and_exp2_are_exact_to_a_thousandth_of_a_db() {
        let mut fold = 0u64;
        for i in 0..=20_000 {
            // Keys from -140 dBFS to +80 dBFS.
            let x = 10f64.powf(-7.0 + 11.0 * f64::from(i) / 20_000.0) as f32;
            let want = 20.0 * f64::from(x).log10();
            let got = f64::from(log2(x) * DB_PER_OCTAVE);
            assert!((got - want).abs() < 1e-3, "key {x}: {got} dB, want {want}");
            // Gains from -200 dB to +40 dB.
            let db = -200.0 + 240.0 * i as f32 / 20_000.0;
            let g = db_to_gain_f32(db);
            let got = 20.0 * f64::from(g).log10();
            assert!((got - f64::from(db)).abs() < 1e-3, "{db} dB: {got}");
            fold = fold.rotate_left(5) ^ u64::from(log2(x).to_bits());
            fold = fold.rotate_left(5) ^ u64::from(g.to_bits());
        }
        assert_eq!(exp2(-127.0), 0.0);
        assert_eq!(exp2(0.0), 1.0);
        assert_eq!(log2(1.0), 0.0);
        assert_eq!(fold, PINNED, "{fold:#x}");
    }

    const PINNED: u64 = 0x40ba_4aaf_02b7_7ca1;
}
