//! Command scheduling (plan 2.11, Replicated state): the lead entries are
//! stamped ahead of E with, and the bound on a command's age.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

/// The lead an entry is stamped ahead of `ceil(E / B) * B` with, kept on
/// the slack the counted audio replicas report: it rises at once to keep
/// the 0.01 % slack quantile at or above 2B, and falls by B a minute while
/// that quantile held at 3B or more for ten minutes; at most the cap.
#[derive(Debug, Clone)]
pub struct Lead {
    block: u64,
    max: u64,
    lead: u64,
    /// Slack histograms per minute, the current one last.
    minutes: VecDeque<Slacks>,
    minute_start: Instant,
    /// The lead wanted more than the cap since the last fall.
    pub capped: bool,
    /// The stamping index at the last raise: entries stamped below it
    /// carry the lead from before it.
    raised_at: u64,
}

/// Minutes of slack the lead falls on.
const MINUTES: usize = 10;

/// Slack in blocks, from [`Slacks::LOW`] blocks below 0, one bucket a
/// block; the outer buckets take everything beyond them.
#[derive(Debug, Clone)]
struct Slacks {
    counts: Vec<u64>,
}

impl Slacks {
    const LOW: i64 = 64;
    const BUCKETS: usize = 512;

    fn new() -> Slacks {
        Slacks {
            counts: vec![0; Slacks::BUCKETS],
        }
    }

    fn bucket(blocks: i64) -> usize {
        (blocks + Slacks::LOW).clamp(0, Slacks::BUCKETS as i64 - 1) as usize
    }
}

impl Lead {
    /// Starts at `start` samples, at most `max`; both multiples of
    /// `block` are kept.
    pub fn new(block: u64, start: u64, max: u64) -> Lead {
        Lead {
            block,
            max: max / block * block,
            lead: start.next_multiple_of(block).min(max / block * block),
            minutes: VecDeque::from([Slacks::new()]),
            minute_start: Instant::now(),
            capped: false,
            raised_at: 0,
        }
    }

    pub fn samples(&self) -> u64 {
        self.lead
    }

    /// The 0.01 % quantile of the slack over the minutes kept, in whole
    /// blocks rounded down; none without slack.
    fn quantile(&self) -> Option<i64> {
        let total: u64 = self.minutes.iter().flat_map(|m| &m.counts).sum();
        if total == 0 {
            return None;
        }
        let rank = (total as f64 * 0.0001).ceil().max(1.0) as u64;
        let mut seen = 0;
        for b in 0..Slacks::BUCKETS {
            seen += self.minutes.iter().map(|m| m.counts[b]).sum::<u64>();
            if seen >= rank {
                return Some(b as i64 - Slacks::LOW);
            }
        }
        None
    }

    /// One entry's slack, in samples, the entry stamped at `stamp`, with
    /// `now` the stamping index at this moment. Slack of an entry stamped
    /// before the last raise is measured at the old lead: ignored, so one
    /// late entry, whichever replicas report it, raises the lead once.
    /// True when this slack asked for a higher lead, granted or capped.
    pub fn add(&mut self, slack: i64, stamp: u64, now: u64) -> bool {
        self.roll();
        if stamp < self.raised_at {
            return false;
        }
        let b = self.block as i64;
        let blocks = slack.div_euclid(b);
        self.minutes.back_mut().expect("a minute").counts[Slacks::bucket(blocks)] += 1;
        if let Some(q) = self.quantile()
            && q < 2
        {
            let want = self.lead + (2 - q) as u64 * self.block;
            self.capped |= want > self.max;
            self.lead = want.min(self.max);
            self.raised_at = now.max(stamp + 1);
            // The slack measured was at the old lead: entries stamped from
            // now on carry the raise, so the window starts again.
            self.minutes = VecDeque::from([Slacks::new()]);
            self.minute_start = Instant::now();
            return true;
        }
        false
    }

    /// Moves to the minute now runs in, falling by B per minute passed
    /// while ten minutes of slack held 3B or more.
    pub fn roll(&mut self) {
        while self.minute_start.elapsed() >= Duration::from_secs(60) {
            self.minute_start += Duration::from_secs(60);
            if self.minutes.len() == MINUTES
                && self.quantile().is_some_and(|q| q >= 3)
                && self.lead > self.block
            {
                self.lead -= self.block;
                self.capped = false;
            }
            self.minutes.push_back(Slacks::new());
            if self.minutes.len() > MINUTES {
                self.minutes.pop_front();
            }
        }
    }
}

/// `ageGuard = 2 x (3B + statusLag + delta + clockGuard)`, in samples:
/// what two voters' E estimates and their rounding to B can differ by.
pub fn age_guard(block: u64, status_lag: u64, delta: u64, clock_guard: u64) -> u64 {
    2 * (3 * block + status_lag + delta + clock_guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: u64 = 48;

    #[tokio::test(start_paused = true)]
    async fn the_lead_rises_at_once_and_falls_by_a_block_a_minute() {
        let mut lead = Lead::new(B, 1440, 2400);
        for _ in 0..10_000 {
            lead.add(10 * B as i64, 0, 0);
        }
        assert_eq!(lead.samples(), 1440, "slack of 10 blocks moves nothing");
        // One entry of 10 001 below 2B is within the 0.01 % quantile, a
        // second is not.
        lead.add(B as i64, 0, 0);
        assert_eq!(lead.samples(), 1440);
        lead.add(B as i64, 0, 0);
        assert_eq!(lead.samples(), 1440 + B, "risen by the block short");
        for minute in 0..9 {
            for _ in 0..100 {
                lead.add(5 * B as i64, 1, 1);
            }
            tokio::time::advance(Duration::from_secs(60)).await;
            lead.roll();
            assert_eq!(lead.samples(), 1440 + B, "minute {minute}: not ten yet");
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 1440, "ten minutes at 3B or more");
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 1440 - B, "and a block a minute after");
    }

    #[tokio::test(start_paused = true)]
    async fn the_lead_stops_at_its_cap_and_says_so() {
        let mut lead = Lead::new(B, 1440, 2400);
        lead.add(-(40 * B as i64), 0, 0);
        assert_eq!(lead.samples(), 2400);
        assert!(lead.capped);
    }

    #[tokio::test(start_paused = true)]
    async fn review_one_late_entry_raises_the_lead_once() {
        let b = B;
        let mut lead = Lead::new(b, 30 * b, 50 * b);
        lead.add(b as i64, 100 * b, 200 * b);
        assert_eq!(lead.samples(), 31 * b, "risen by the block short");
        // The same entry as a second counted replica reports it, and
        // another stamped before the raise: measured at the old lead.
        lead.add(b as i64, 100 * b, 210 * b);
        lead.add(b as i64, 150 * b, 210 * b);
        assert_eq!(
            lead.samples(),
            31 * b,
            "the old lead's slack raised it again"
        );
        // One stamped after the raise still raises it.
        lead.add(b as i64, 220 * b, 230 * b);
        assert_eq!(lead.samples(), 32 * b);
    }

    #[test]
    fn the_age_guard_is_22_ms_at_the_defaults() {
        // B 48, statusLag and delta 1 ms, clockGuard 6 ms.
        assert_eq!(age_guard(48, 48, 48, 288), 1056);
    }
}
