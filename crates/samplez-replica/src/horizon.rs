//! The horizon: a verified audio replica E does not count whose processed
//! index lies more than `applyGuard` below its own E estimate is held in
//! `starting` as an unverified one is, and rebuilds only once it is back
//! within `applyGuard - S` of it, S the E spread, so a lag near the bound
//! does not rebuild the engine again and again. A replica E counts is
//! never held.

/// One replica's horizon hold across report periods.
#[derive(Debug, Default)]
pub struct Horizon {
    held: bool,
}

impl Horizon {
    pub fn held(&self) -> bool {
        self.held
    }

    /// Holds the replica, for tests that drive contact, not the horizon.
    #[cfg(test)]
    pub(crate) fn hold(&mut self) {
        self.held = true;
    }

    /// One period: the end of the replica's processed block and its E
    /// estimate, samples, none before either is known; `counted` whether
    /// E counts it; the bounds in samples.
    pub fn period(
        &mut self,
        processed_end: Option<u64>,
        estimate: Option<u64>,
        counted: bool,
        apply_guard: u64,
        spread: u64,
    ) -> bool {
        if counted {
            self.held = false;
            return false;
        }
        // Without both, nothing is known: the hold stands as it is, so a
        // dropped engine that reports no processed block is not released.
        let (Some(p), Some(e)) = (processed_end, estimate) else {
            return self.held;
        };
        let lag = e.saturating_sub(p);
        if self.held {
            self.held = lag > apply_guard.saturating_sub(spread);
        } else {
            self.held = lag > apply_guard;
        }
        self.held
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GUARD: u64 = 1000;
    const S: u64 = 100;

    #[test]
    fn a_lag_beyond_apply_guard_holds_until_back_within_guard_less_spread() {
        let mut h = Horizon::default();
        assert!(!h.period(Some(10_000), Some(10_000 + GUARD), false, GUARD, S));
        assert!(h.period(Some(10_000), Some(10_001 + GUARD), false, GUARD, S));
        // Hovering at the bound keeps the hold: one rebuild, not one per
        // crossing.
        for lag in [GUARD, GUARD - 1, GUARD - S + 1, GUARD + 1, GUARD - S + 1] {
            assert!(
                h.period(Some(10_000), Some(10_000 + lag), false, GUARD, S),
                "lag {lag}"
            );
        }
        assert!(!h.period(Some(10_000), Some(10_000 + GUARD - S), false, GUARD, S));
    }

    #[test]
    fn a_counted_replica_is_never_held() {
        let mut h = Horizon::default();
        assert!(!h.period(Some(0), Some(20 * GUARD), true, GUARD, S));
        assert!(h.period(Some(0), Some(20 * GUARD), false, GUARD, S));
        assert!(!h.period(Some(0), Some(20 * GUARD), true, GUARD, S));
        // Without an estimate nothing is compared and nothing is held.
        assert!(!h.period(Some(0), None, false, GUARD, S));
    }

    #[test]
    fn a_held_replica_reporting_no_processed_block_stays_held() {
        let mut h = Horizon::default();
        assert!(h.period(Some(0), Some(20 * GUARD), false, GUARD, S));
        // A dropped engine reports none: no release, so no rebuild.
        for _ in 0..3 {
            assert!(h.period(None, Some(20 * GUARD), false, GUARD, S));
        }
        assert!(h.period(Some(0), None, false, GUARD, S));
    }
}
