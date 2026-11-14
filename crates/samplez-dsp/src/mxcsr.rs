use std::arch::asm;
use std::marker::PhantomData;

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

/// MXCSR as it was before [`set_ftz_daz`], for [`restore_mxcsr`]. MXCSR
/// is per thread, so the value is not `Send`: restoring it on another
/// thread would load one thread's MXCSR into another.
#[derive(Debug)]
#[must_use]
pub struct SavedMxcsr(u32, PhantomData<*const ()>);

fn load_mxcsr(value: u32) {
    // SAFETY: ldmxcsr reads 4 bytes from a valid, aligned u32. Every value
    // loaded is one read by `mxcsr` with at most FTZ and DAZ changed, so
    // no reserved bit is set.
    unsafe {
        asm!("ldmxcsr [{}]", in(reg) &value, options(nostack, readonly, preserves_flags));
    }
}

/// Loads the MXCSR saved by [`set_ftz_daz`] back into the calling
/// thread's MXCSR with `ldmxcsr`.
pub fn restore_mxcsr(saved: SavedMxcsr) {
    load_mxcsr(saved.0);
}

/// Sets or clears FTZ and DAZ on the calling thread and returns the
/// previous MXCSR for [`restore_mxcsr`].
///
/// LLVM compiles and constant-folds float code assuming IEEE denormal
/// handling, which FTZ/DAZ departs from, so results on denormals can
/// differ between compile time and run time. FTZ/DAZ is for the RT thread
/// only.
pub fn set_ftz_daz(enabled: bool) -> SavedMxcsr {
    let saved = mxcsr();
    let value = if enabled {
        saved | FTZ | DAZ
    } else {
        saved & !(FTZ | DAZ)
    };
    load_mxcsr(value);
    SavedMxcsr(saved, PhantomData)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;

    /// Compiles only while `SavedMxcsr` is not `Send`: for a `Send` type
    /// both impls apply and the call is ambiguous.
    #[test]
    fn saved_mxcsr_is_not_send() {
        trait AmbiguousIfSend<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfSend<()> for T {}
        impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}
        <SavedMxcsr as AmbiguousIfSend<_>>::check();
    }

    #[test]
    fn ftz_daz_flush_denormals_and_restore() {
        let tiny = black_box(f32::from_bits(1));
        let before = mxcsr();
        let saved = set_ftz_daz(true);
        assert_eq!(mxcsr() & (FTZ | DAZ), FTZ | DAZ);
        let on = black_box(tiny) * black_box(1.0f32);
        restore_mxcsr(set_ftz_daz(false));
        assert_eq!(mxcsr() & (FTZ | DAZ), FTZ | DAZ);
        let inner = set_ftz_daz(false);
        assert_eq!(mxcsr() & (FTZ | DAZ), 0);
        let off = black_box(tiny) * black_box(1.0f32);
        restore_mxcsr(inner);
        restore_mxcsr(saved);
        assert_eq!(mxcsr(), before);
        assert_eq!(on.to_bits(), 0);
        assert_eq!(off.to_bits(), 1);
    }
}
