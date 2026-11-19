//! Command scheduling (plan 2.11, Replicated state): the lead entries are
//! stamped ahead of E with, and the bound on a command's age.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use crate::contact::index_at;
use crate::state_machine::AHEAD_MAX;

/// The lead an entry is stamped ahead of `ceil(E / B) * B` with, kept on
/// the slack the counted audio replicas report: it rises at once to keep
/// the 0.01 % slack quantile at or above 2B, and falls by B a minute while
/// that quantile held at 3B or more for ten minutes; at most the cap. A
/// new Raft term voids the slack kept and falls faster ([`Lead::new_term`]).
#[derive(Debug, Clone)]
pub struct Lead {
    block: u64,
    max: u64,
    lead: u64,
    /// A term began since the lead last rose: it may fall on
    /// [`FAST_MINUTES`] ([`Lead::new_term`]).
    fast: bool,
    /// Slack histograms per minute, the current one last.
    minutes: VecDeque<Slacks>,
    minute_start: Instant,
    /// The lead wanted more than the cap since the last fall.
    pub capped: bool,
    /// The most lead an entry needed, by the slack of this minute and of
    /// the last ([`Lead::needs`]).
    needed: [u64; 2],
    /// The stamping index at the last raise: entries stamped below it
    /// carry the lead from before it.
    raised_at: u64,
}

/// Minutes of slack the lead falls on.
const MINUTES: usize = 10;

/// Minutes of slack the lead falls on after a new term, until it next
/// rises ([`Lead::new_term`]).
pub const FAST_MINUTES: usize = 1;

/// How far past the cap [`Lead::wanted`] goes at most.
pub const EXTRA_LEAD: Duration = Duration::from_millis(64);

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
            fast: false,
            minutes: VecDeque::from([Slacks::new()]),
            minute_start: Instant::now(),
            capped: false,
            needed: [0; 2],
            raised_at: 0,
        }
    }

    /// A new Raft term began: what the last term's slack and need said is
    /// void, the lead is kept. The slack windows, the kept need, the cap
    /// flag and the raise mark start again, and until the lead next rises
    /// it may start falling after [`FAST_MINUTES`] clean minutes instead
    /// of ten: an election raises the lead on no voter
    /// ([`Lead::add`] is not fed its slack), so what is left is need the
    /// replicas showed, which a minute of clean slack at 3B or more
    /// already answers.
    pub fn new_term(&mut self) {
        self.capped = false;
        self.needed = [0; 2];
        self.raised_at = 0;
        self.fast = true;
        self.minutes = VecDeque::from([Slacks::new()]);
        self.minute_start = Instant::now();
    }

    pub fn samples(&self) -> u64 {
        self.lead
    }

    /// The lead a command is stamped with: [`Lead::wanted`] at the cap,
    /// the lead below it.
    pub fn stamping(&self) -> u64 {
        if self.capped {
            self.wanted()
        } else {
            self.lead
        }
    }

    /// One entry stamped `lead` ahead applied with `slack`: it needed
    /// `lead - slack + 2B` to keep the slack the lead keeps.
    pub fn needs(&mut self, lead: u64, slack: i64) {
        self.roll();
        let needed = (lead as i64 - slack + 2 * self.block as i64).max(0) as u64;
        self.needed[1] = self.needed[1].max(needed);
    }

    /// The lead the entries applied this minute and the last needed, at
    /// least the lead and at most [`EXTRA_LEAD`] past the cap, in whole
    /// blocks below [`AHEAD_MAX`]: past the cap, what an entry needs to
    /// apply in time. Further past its stamp than `AHEAD_MAX` an entry
    /// would take effect at its stamp instead, already passed.
    pub fn wanted(&self) -> u64 {
        let b = self.block;
        let extra = index_at(EXTRA_LEAD.as_nanos() as u64);
        let most = ((self.max + extra) / b * b).min(AHEAD_MAX / b * b - b);
        let needed = self.needed[0].max(self.needed[1]);
        needed.next_multiple_of(b).min(most).max(self.lead)
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
            self.fast = false;
            // The slack measured was at the old lead: entries stamped from
            // now on carry the raise, so the window starts again.
            self.minutes = VecDeque::from([Slacks::new()]);
            self.minute_start = Instant::now();
            return true;
        }
        false
    }

    /// Moves to the minute now runs in, falling by B per minute passed
    /// while ten minutes of slack held 3B or more, unless at the cap the
    /// entries of the minute just ended needed more than the lead: they
    /// are stamped with what they needed, so their slack says nothing of
    /// the lead, and a fall would clear `capped` and stamp the next
    /// commands with less than they need. Below the cap the slack alone
    /// decides.
    pub fn roll(&mut self) {
        while self.minute_start.elapsed() >= Duration::from_secs(60) {
            self.minute_start += Duration::from_secs(60);
            self.needed = [self.needed[1], 0];
            let held = self.capped && self.needed[0].max(self.needed[1]) > self.lead;
            let kept = if self.fast { FAST_MINUTES } else { MINUTES };
            if self.minutes.len() >= kept
                && self.quantile().is_some_and(|q| q >= 3)
                && self.lead > self.block
                && !held
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

    /// A new term keeps the lead, voids the cap flag and the slack kept,
    /// and lets the lead fall after one clean minute instead of ten.
    #[tokio::test(start_paused = true)]
    async fn a_new_term_keeps_the_lead_and_lets_it_fall_after_a_minute() {
        let mut lead = Lead::new(B, 1440, 2400);
        lead.add(-(40 * B as i64), 0, 0);
        assert!(lead.capped);
        lead.needs(2400, -(20 * B as i64));
        lead.new_term();
        assert_eq!(lead.samples(), 2400, "the lead is kept");
        assert!(!lead.capped);
        assert_eq!(lead.wanted(), 2400, "what was needed is forgotten");
        for _ in 0..100 {
            lead.add(5 * B as i64, 1, 1);
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 2400 - B, "one clean minute is enough");
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 2400 - 2 * B, "and a block a minute after");
        // A raise ends the fast fall.
        lead.add(0, 2, 2);
        let raised = lead.samples();
        for _ in 0..100 {
            lead.add(5 * B as i64, 3, 3);
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), raised, "ten minutes again");
    }

    /// Without a new term one clean minute moves nothing.
    #[tokio::test(start_paused = true)]
    async fn without_a_new_term_one_clean_minute_does_not_lower_the_lead() {
        let mut lead = Lead::new(B, 2400, 2400);
        for _ in 0..100 {
            lead.add(5 * B as i64, 1, 1);
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 2400);
    }

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

    /// Past the cap the lead wanted is what the entries applied needed,
    /// at most 64 ms past it, forgotten after two minutes.
    #[tokio::test(start_paused = true)]
    async fn the_lead_wanted_covers_what_entries_needed_past_the_cap() {
        let mut lead = Lead::new(B, 30 * B, 50 * B);
        assert_eq!(lead.wanted(), 30 * B);
        lead.needs(30 * B, -(20 * B as i64));
        assert_eq!(lead.stamping(), 30 * B, "below the cap: the lead");
        lead.add(-(40 * B as i64), 0, 0);
        assert!(lead.capped);
        lead.needs(50 * B, -(20 * B as i64));
        assert_eq!(lead.wanted(), 72 * B, "20 blocks late and 2B of slack");
        assert_eq!(lead.stamping(), 72 * B, "at the cap: what was needed");
        lead.needs(50 * B, -(200 * B as i64));
        assert_eq!(lead.wanted(), 50 * B + 3072, "64 ms past the cap");
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.wanted(), 50 * B + 3072, "the last minute counts");
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.wanted(), 50 * B, "the lead itself");
    }

    /// At the cap, entries stamped with what they needed keep slack of 3B
    /// for ten minutes while still needing more than the lead: the lead
    /// does not fall and commands keep the lead they need. Once they need
    /// no more than the lead it falls again.
    #[tokio::test(start_paused = true)]
    async fn the_lead_holds_its_fall_while_entries_need_more_than_it() {
        let mut lead = Lead::new(B, 30 * B, 50 * B);
        lead.add(-(40 * B as i64), 0, 0);
        assert!(lead.capped);
        for minute in 0..12 {
            for _ in 0..100 {
                lead.add(5 * B as i64, 1, 1);
                lead.needs(72 * B, 5 * B as i64);
            }
            tokio::time::advance(Duration::from_secs(60)).await;
            lead.roll();
            assert_eq!(lead.samples(), 50 * B, "minute {minute}");
            assert!(lead.capped, "minute {minute}");
            assert_eq!(lead.stamping(), 69 * B, "minute {minute}");
        }
        for _ in 0..100 {
            lead.add(5 * B as i64, 1, 1);
            lead.needs(40 * B, 5 * B as i64);
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        lead.roll();
        assert_eq!(lead.samples(), 49 * B, "a minute needing less, it falls");
        assert!(!lead.capped);
        // Below the cap the slack's quantile alone decides: one entry
        // with slack B, among over 10 000 slacks so the 0.01 % quantile
        // passes over it, needed more than the lead and holds no fall.
        let mut lead = Lead::new(B, 30 * B, 50 * B);
        for minute in 0..10 {
            for _ in 0..1001 {
                lead.add(5 * B as i64, 1, 1);
            }
            if minute == 9 {
                lead.add(B as i64, 1, 1);
                lead.needs(30 * B, B as i64);
            }
            tokio::time::advance(Duration::from_secs(60)).await;
            lead.roll();
        }
        assert_eq!(lead.samples(), 29 * B, "below the cap it falls");
    }

    /// At the largest block size the lead wanted stays within 64 ms of
    /// the cap and below `AHEAD_MAX`, however late an entry was.
    #[tokio::test(start_paused = true)]
    async fn the_lead_wanted_stays_within_its_time_bound_at_block_1024() {
        let b = 1024;
        let mut lead = Lead::new(b, 1440, 3840);
        lead.add(-(400 * b as i64), 0, 0);
        assert_eq!(lead.samples(), 3 * b);
        lead.needs(3 * b, -(1000 * b as i64));
        assert_eq!(
            lead.wanted(),
            6 * b,
            "3072 samples past the cap, whole blocks"
        );
        assert!(lead.wanted() < AHEAD_MAX);
        let mut lead = Lead::new(b, 1440, 46_000);
        lead.add(-(400 * b as i64), 0, 0);
        lead.needs(lead.samples(), -(1000 * b as i64));
        assert_eq!(lead.wanted(), AHEAD_MAX / b * b - b, "below AHEAD_MAX");
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
