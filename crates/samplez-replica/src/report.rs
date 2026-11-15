//! The side report every voter sends every other voter each report
//! period, beside Raft (plan 2.11, Fencing and Withholding): the echo of
//! the addressee's latest report, what the sender says of itself, what it
//! last heard from every other voter (one relay hop) and the entries it
//! lists for the addressee.

use samplez::blocks::{BlockStatus, Range};
use serde::{Deserialize, Serialize};

use crate::entry::NodeId;
use crate::snapshot::StoredLogId;

/// Entries one report lists at most.
pub const MAX_LISTED: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub from: NodeId,
    /// Drawn at random at process start and counted up per period, so a
    /// sequence number from before a restart never matches one after it.
    pub seq: u64,
    /// The sender's TAI time when it built the report, ns.
    pub sent_at: u64,
    /// The addressee's latest report the sender received.
    pub echo: Option<Echo>,
    /// The sender's committed log id.
    pub committed: Option<StoredLogId>,
    pub own: Own,
    /// What the sender last received directly from each other voter.
    pub heard: Vec<Heard>,
    /// The entries above the addressee's reported commit index whose
    /// at_index lies below the sender's `now + maxApplyLead + w`, lowest
    /// at_index first, at most [`MAX_LISTED`].
    pub listed: Vec<Listed>,
    /// The lowest at_index of the entries the listing had no room for:
    /// an entry the addressee knows of and cannot resolve.
    pub unlisted: Option<u64>,
    /// The sender's E estimate when it built the report, in samples.
    pub estimate: Option<u64>,
    /// Per entry the sender learned committed since its last report, the
    /// slack: its at_index less the first index the sender could still
    /// apply it at by the apply deadline, in samples; at most
    /// [`MAX_LISTED`].
    pub slack: Vec<Slack>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slack {
    pub index: u64,
    pub samples: i64,
}

/// An echo of the addressee's report `seq`: the four timestamps of an
/// NTP-style exchange are its send time (the addressee keeps it), its
/// receipt time here, this report's `sent_at` and the addressee's receipt
/// time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Echo {
    pub seq: u64,
    /// The sender's TAI time when it received report `seq`, ns.
    pub received_at: u64,
    /// The latest TAI time at which the sender held the addressee silent,
    /// this report included.
    pub silent_at: Option<u64>,
}

/// A voter's clock as its reports state it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockFlags {
    /// The kernel clock is unsynchronised.
    pub self_report: bool,
    /// A clock step the replica detected still fences it.
    pub step_fence: bool,
    /// Not clock-verified.
    pub unverified: bool,
    /// The kernel TAI offset sampled last, in whole seconds.
    pub tai_offset_s: Option<i64>,
}

/// One status of a run of consecutive blocks of one output: block `k`'s,
/// and the same for the `count - 1` blocks after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub status: BlockStatus,
    pub count: u32,
}

/// Samples of committed blocks the member flow refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unwritten {
    pub output: u32,
    pub from: u64,
    pub to: u64,
}

/// What a voter says of itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Own {
    /// The highest block it computed; none for the witness and before
    /// the first block.
    pub processed: Option<u64>,
    pub clock: ClockFlags,
    /// Per output, the statuses of its latest blocks.
    pub statuses: Vec<Vec<Run>>,
    pub unwritten: Vec<Unwritten>,
    /// buildLead: samples a generation-needing entry is stamped ahead of
    /// the earliest index, covering this replica's generation builds.
    pub build_lead: Option<u64>,
}

/// What the sender holds of another voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heard {
    pub node: NodeId,
    /// How long ago the sender received that voter's latest report, us;
    /// none when it received none.
    pub age_us: Option<u64>,
    /// The sender holds it silent.
    pub silent: bool,
    /// What that report said of its sender.
    pub own: Option<Own>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listed {
    pub id: StoredLogId,
    pub at_index: u64,
}

/// `statuses` of consecutive blocks, a none for a block without one, as
/// runs: one per status repeated over consecutive blocks.
pub fn runs(statuses: impl IntoIterator<Item = Option<BlockStatus>>) -> Vec<Run> {
    let mut out: Vec<Run> = Vec::new();
    for s in statuses.into_iter().flatten() {
        if let Some(run) = out.last_mut()
            && run.status.k + u64::from(run.count) == s.k
            && run.status.range == Range::Whole
            && s.range == Range::Whole
            && (BlockStatus {
                k: s.k,
                ..run.status
            }) == s
        {
            run.count += 1;
            continue;
        }
        out.push(Run {
            status: s,
            count: 1,
        });
    }
    out
}

/// The status of block `k` in `runs`.
pub fn status_of(runs: &[Run], k: u64) -> Option<BlockStatus> {
    runs.iter()
        .find(|r| r.status.k <= k && k < r.status.k + u64::from(r.count))
        .map(|r| BlockStatus { k, ..r.status })
}

/// Durations in quarter-octave buckets of nanoseconds: bounded, and
/// within a fifth of the value at any quantile.
#[derive(Debug, Clone)]
pub struct Histogram {
    counts: [u64; BUCKETS],
    total: u64,
    max: u64,
}

const BUCKETS: usize = 160;

impl Default for Histogram {
    fn default() -> Histogram {
        Histogram {
            counts: [0; BUCKETS],
            total: 0,
            max: 0,
        }
    }
}

impl Histogram {
    pub fn add(&mut self, ns: u64) {
        let b = (4.0 * (ns.max(1) as f64).log2()) as usize;
        self.counts[b.min(BUCKETS - 1)] += 1;
        self.total += 1;
        self.max = self.max.max(ns);
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// The upper edge of the bucket holding quantile `q`, at most the
    /// largest value seen; none when empty.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        let rank = ((self.total as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (b, &n) in self.counts.iter().enumerate() {
            seen += n;
            if n > 0 && seen >= rank {
                let edge = 2f64.powf((b + 1) as f64 / 4.0).ceil() as u64;
                return Some(edge.min(self.max));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use samplez::blocks::{Cause, HEALTHY, Kind};

    use super::*;

    fn st(k: u64, kind: Kind) -> BlockStatus {
        BlockStatus {
            k,
            kind,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::None,
            on_silence_of: 0,
            digest: Some(HEALTHY),
        }
    }

    #[test]
    fn runs_merge_equal_statuses_and_keep_every_block() {
        let split = BlockStatus {
            range: Range::From(10 * 48 + 16),
            ..st(10, Kind::Committed)
        };
        let blocks = vec![
            Some(st(7, Kind::Committed)),
            Some(st(8, Kind::Committed)),
            Some(st(9, Kind::Committed)),
            Some(split),
            Some(st(11, Kind::Committed)),
            None,
            Some(st(13, Kind::Held)),
            Some(st(14, Kind::Held)),
        ];
        let r = runs(blocks.clone());
        assert_eq!(r.len(), 4, "{r:?}");
        for b in blocks.into_iter().flatten() {
            assert_eq!(status_of(&r, b.k), Some(b));
        }
        assert_eq!(status_of(&r, 12), None, "a block without a status");
        assert_eq!(status_of(&r, 15), None);
    }

    #[test]
    fn histogram_quantiles_stay_within_a_bucket() {
        let mut h = Histogram::default();
        for ns in 1..=10_000u64 {
            h.add(ns * 1_000);
        }
        let p50 = h.quantile(0.5).unwrap();
        assert!((5_000_000..=6_000_000).contains(&p50), "{p50}");
        assert_eq!(h.quantile(1.0), Some(10_000_000));
        assert_eq!(Histogram::default().quantile(0.5), None);
    }
}
