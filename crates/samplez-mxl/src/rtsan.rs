//! RealtimeSanitizer scopes. A build with `--cfg samplez_rtsan` and
//! `-Zsanitizer=realtime` checks the functions samplez marks as real-time;
//! [`allow`] takes the calls whose blocking is documented and bounded out
//! of that check by call site. Any other build runs them unchanged.

#[cfg(samplez_rtsan)]
unsafe extern "C" {
    fn __rtsan_disable();
    fn __rtsan_enable();
}

/// Runs `f` with RealtimeSanitizer's checks off on this thread. Every
/// use names the blocking call it covers.
#[inline(always)]
pub fn allow<R>(f: impl FnOnce() -> R) -> R {
    #[cfg(samplez_rtsan)]
    {
        /// Enables the checks again, also when `f` unwinds.
        struct Enable;
        impl Drop for Enable {
            fn drop(&mut self) {
                // SAFETY: pairs the disable below.
                unsafe { __rtsan_enable() };
            }
        }
        // SAFETY: the RTSan runtime is linked into every samplez_rtsan
        // build; the enable runs when `_enable` drops.
        unsafe { __rtsan_disable() };
        let _enable = Enable;
        f()
    }
    #[cfg(not(samplez_rtsan))]
    f()
}
