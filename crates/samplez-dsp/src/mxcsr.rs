use std::arch::asm;

/// MXCSR flush-to-zero: denormal results are written as zero.
pub const FTZ: u32 = 1 << 15;
/// MXCSR denormals-are-zero: denormal operands are read as zero.
pub const DAZ: u32 = 1 << 6;

/// The calling thread's MXCSR, read with `stmxcsr`.
pub fn mxcsr() -> u32 {
    let mut value: u32 = 0;
    // SAFETY: stmxcsr writes 4 bytes to a valid, aligned u32.
    unsafe {
        asm!("stmxcsr [{}]", in(reg) &mut value, options(nostack, preserves_flags));
    }
    value
}

/// Loads `saved` into the calling thread's MXCSR with `ldmxcsr`.
pub fn restore_mxcsr(saved: u32) {
    // SAFETY: ldmxcsr reads 4 bytes from a valid, aligned u32. Reserved
    // MXCSR bits are 0 in any value read by `mxcsr`.
    unsafe {
        asm!("ldmxcsr [{}]", in(reg) &saved, options(nostack, readonly, preserves_flags));
    }
}

/// Sets or clears FTZ and DAZ on the calling thread and returns the
/// previous MXCSR for [`restore_mxcsr`].
pub fn set_ftz_daz(enabled: bool) -> u32 {
    let saved = mxcsr();
    let value = if enabled {
        saved | FTZ | DAZ
    } else {
        saved & !(FTZ | DAZ)
    };
    restore_mxcsr(value);
    saved
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;

    #[test]
    fn ftz_daz_flush_denormals_and_restore() {
        let tiny = black_box(f32::from_bits(1));
        let saved = set_ftz_daz(true);
        assert_eq!(mxcsr() & (FTZ | DAZ), FTZ | DAZ);
        let on = black_box(tiny) * black_box(1.0f32);
        set_ftz_daz(false);
        assert_eq!(mxcsr() & (FTZ | DAZ), 0);
        let off = black_box(tiny) * black_box(1.0f32);
        restore_mxcsr(saved);
        assert_eq!(mxcsr(), saved);
        assert_eq!(on.to_bits(), 0);
        assert_eq!(off.to_bits(), 1);
    }
}
