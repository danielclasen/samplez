//! Block statuses and the output hold ring of a replica (plan 2.11,
//! Replica states and Withholding).
//!
//! Each output keeps a ring of status words, one per block, every move a
//! compare-and-swap. The RT thread sets a block's first status, makes
//! every move to `committed` and writes a block only after its own CAS
//! succeeded; the supervisor moves blocks to `withheld` and marks held
//! blocks for commit through on evidence. A block that cannot be
//! committed at once stays in the output's hold ring, with every block
//! computed after it, until it is final; committed blocks leave the ring
//! in one write. Each output's pages are hashed as they are computed, and
//! a page's hash is sent once every block overlapping it is final.
//!
//! Nothing here allocates once built.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release};

use rtrb::Producer;
use serde::{Deserialize, Serialize};

/// Samples per merge page: an output's member resumes at a multiple of it.
pub const PAGE: u64 = 1024;

/// What a status says about the samples it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// Computed while the fence F had not passed it.
    Fenced,
    /// Degraded, a fill, or behind a block not yet committed, with F
    /// past it: committed on evidence or once every block before it is.
    Held,
    /// Final; written to the member flow.
    Committed,
    /// Final; never written.
    Withheld,
}

/// Why a block was withheld.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cause {
    /// None given: the range below a commit start, or past a stop.
    None,
    /// Skipped by an overrun or a clock step.
    Skip,
    /// The oldest block of a full hold ring.
    Overflow,
    /// A block a withdrawn replica cannot fill.
    Unfillable,
    /// Fenced, or not computed, at its fence deadline.
    FenceDeadline,
    /// Held at its hold cap.
    HoldCap,
    /// A `committed` report for it with another digest.
    OtherDigest,
    /// A status naming this replica in `onSilenceOf`.
    Silence,
    /// The output's writer was released or replaced.
    Released,
    /// Its status slot still held another block's open status, or a later
    /// block's.
    NoSlot,
    /// Fenced by a detected clock step with no ring to hold it: one voter.
    ClockStep,
    /// Not computed: the engine holds no core while the replica starts.
    NoCore,
}

const CAUSES: [Cause; 12] = [
    Cause::None,
    Cause::Skip,
    Cause::Overflow,
    Cause::Unfillable,
    Cause::FenceDeadline,
    Cause::HoldCap,
    Cause::OtherDigest,
    Cause::Silence,
    Cause::Released,
    Cause::NoSlot,
    Cause::ClockStep,
    Cause::NoCore,
];

/// The samples of block k a status covers. A block split at P carries
/// the status for one range; the other range is `withheld` without a
/// cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Range {
    Whole,
    /// `[P, (k + 1)B)`: a commit start at P; `[kB, P)` is withheld.
    From(u64),
    /// `[kB, P)`: a stop at P; `[P, (k + 1)B)` is withheld.
    UpTo(u64),
}

/// One block's status for one output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStatus {
    pub k: u64,
    pub kind: Kind,
    pub range: Range,
    /// The supervisor found evidence for committing a held block through.
    pub through: bool,
    /// The replica's rank, fixed when F passed the block; 0 is the top.
    pub rank: u8,
    pub cause: Cause,
    /// The voters, by bit, whose silence a commit through used.
    pub on_silence_of: u8,
    /// The degradation digest: the empty digest ([`healthy_digest`]) for
    /// a healthy block, none for a fill or a clock self-report.
    pub digest: Option<u64>,
}

impl BlockStatus {
    /// Whether it is `withheld` for a cause (a degradation, a fence, a
    /// skip, `onSilenceOf`): not the range below a commit start, not a
    /// block a withdrawn replica could not fill, and not a fill (no
    /// digest) withheld because another replica committed the block or
    /// nobody gave evidence for it by its hold cap.
    pub fn for_a_cause(&self) -> bool {
        let fill_not_needed =
            self.digest.is_none() && matches!(self.cause, Cause::OtherDigest | Cause::HoldCap);
        self.kind == Kind::Withheld
            && !matches!(self.cause, Cause::None | Cause::Unfillable)
            && !fill_not_needed
    }
}

/// The digest of a healthy block: XXH3-64 over no bytes.
pub const HEALTHY: u64 = 0x2d06_8005_38d3_94c2;

/// The digest of a degraded block: XXH3-64 over the sorted log ids, then
/// the bitmask of inputs whose wanted read failed (an input's flow, the
/// incoming one during a crossfade), then the bitmask of crossfades whose
/// outgoing flow failed, each as 8 bytes little endian. The two legs of
/// a crossfade differ in the output, so they differ in the digest. `ids`
/// must be sorted.
pub fn degraded_digest(ids: &[u64], incoming: u64, outgoing: u64) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3Default::new();
    for id in ids {
        h.update(&id.to_le_bytes());
    }
    h.update(&incoming.to_le_bytes());
    h.update(&outgoing.to_le_bytes());
    h.digest()
}

// Status word layout, from bit 0: kind (3, 0 = no status), range (2),
// split offset P - kB (10, below B <= 1024), through (1), digest present
// (1), rank (5), cause (4), onSilenceOf (8), the block index's low 30 bits.
const KIND_BITS: u32 = 3;
const RANGE_SHIFT: u32 = 3;
const SPLIT_SHIFT: u32 = 5;
const THROUGH_BIT: u32 = 15;
const DIGEST_BIT: u32 = 16;
const RANK_SHIFT: u32 = 17;
const CAUSE_SHIFT: u32 = 22;
const SILENCE_SHIFT: u32 = 26;
const TAG_SHIFT: u32 = 34;
const TAG_MASK: u64 = (1 << 30) - 1;

/// Ranks from 0 to this; a status carries 5 bits of rank.
pub const MAX_RANK: u8 = 31;

fn encode(s: &BlockStatus, block: u64) -> u64 {
    let kind = match s.kind {
        Kind::Fenced => 1,
        Kind::Held => 2,
        Kind::Committed => 3,
        Kind::Withheld => 4,
    };
    let (range, split) = match s.range {
        Range::Whole => (0, 0),
        Range::From(p) => (1, p - s.k * block),
        Range::UpTo(p) => (2, p - s.k * block),
    };
    let cause = CAUSES.iter().position(|&c| c == s.cause).unwrap_or(0) as u64;
    kind | range << RANGE_SHIFT
        | split << SPLIT_SHIFT
        | u64::from(s.through) << THROUGH_BIT
        | u64::from(s.digest.is_some()) << DIGEST_BIT
        | u64::from(s.rank.min(MAX_RANK)) << RANK_SHIFT
        | cause << CAUSE_SHIFT
        | u64::from(s.on_silence_of) << SILENCE_SHIFT
        | (s.k & TAG_MASK) << TAG_SHIFT
}

/// The status of block `k` in `word`, if the word holds one for `k`.
fn decode(word: u64, digest: u64, k: u64, block: u64) -> Option<BlockStatus> {
    let kind = match word & ((1 << KIND_BITS) - 1) {
        1 => Kind::Fenced,
        2 => Kind::Held,
        3 => Kind::Committed,
        4 => Kind::Withheld,
        _ => return None,
    };
    if word >> TAG_SHIFT != k & TAG_MASK {
        return None;
    }
    let split = k * block + (word >> SPLIT_SHIFT & 0x3ff);
    let range = match word >> RANGE_SHIFT & 3 {
        1 => Range::From(split),
        2 => Range::UpTo(split),
        _ => Range::Whole,
    };
    Some(BlockStatus {
        k,
        kind,
        range,
        through: word >> THROUGH_BIT & 1 == 1,
        rank: (word >> RANK_SHIFT & 0x1f) as u8,
        cause: CAUSES[(word >> CAUSE_SHIFT & 0xf) as usize % CAUSES.len()],
        on_silence_of: (word >> SILENCE_SHIFT & 0xff) as u8,
        digest: (word >> DIGEST_BIT & 1 == 1).then_some(digest),
    })
}

struct Slot {
    word: AtomicU64,
    digest: AtomicU64,
}

/// What a slot's word holds when it holds no status of block `k`.
#[derive(PartialEq, Eq)]
enum Occupant {
    /// Nothing, or a final status of an earlier block: `k` may take it.
    Free,
    /// An open (`fenced`, `held`) status of an earlier block.
    Open,
    /// A later block's status: set ahead by the supervisor.
    Later,
}

fn occupant(word: u64, k: u64) -> Occupant {
    let kind = word & ((1 << KIND_BITS) - 1);
    if kind == 0 {
        return Occupant::Free;
    }
    let d = (word >> TAG_SHIFT).wrapping_sub(k & TAG_MASK) & TAG_MASK;
    if d != 0 && d < 1 << 29 {
        Occupant::Later
    } else if kind == 1 || kind == 2 {
        Occupant::Open
    } else {
        Occupant::Free
    }
}

/// Block `k` withheld for want of a slot.
fn no_slot(k: u64) -> BlockStatus {
    BlockStatus {
        k,
        kind: Kind::Withheld,
        range: Range::Whole,
        through: false,
        rank: 0,
        cause: Cause::NoSlot,
        on_silence_of: 0,
        digest: None,
    }
}

/// The status words of one output, one slot per block modulo the ring's
/// length. Shared by the RT thread and the supervisor.
pub struct Statuses {
    block: u64,
    slots: Box<[Slot]>,
    /// The output's [`Standing`], set by the RT thread.
    standing: AtomicU64,
    /// The latest commit start requested, set by the supervisor.
    start: StartSlot,
}

/// A requested commit start that is a cancel.
const CANCEL: u64 = u64::MAX;

/// One output's latest commit start request, latest wins: a sequence
/// lock with one writer at a time, read by the RT thread without
/// waiting. An even sequence is a complete request.
struct StartSlot {
    seq: AtomicU64,
    min_k: AtomicU64,
    seen: AtomicU64,
}

impl StartSlot {
    fn new() -> StartSlot {
        StartSlot {
            seq: AtomicU64::new(0),
            min_k: AtomicU64::new(0),
            seen: AtomicU64::new(0),
        }
    }

    /// The caller allows one writer at a time.
    fn write(&self, min_k: u64, seen: u64) {
        let s = self.seq.load(Relaxed);
        self.seq.store(s + 1, Relaxed);
        std::sync::atomic::fence(Release);
        self.min_k.store(min_k, Relaxed);
        self.seen.store(seen, Relaxed);
        self.seq.store(s + 2, Release);
    }

    /// The request after sequence `last`, if one is complete.
    fn read(&self, last: u64) -> Option<(u64, u64, u64)> {
        let s = self.seq.load(Acquire);
        if s == last || s & 1 == 1 {
            return None;
        }
        let (min_k, seen) = (self.min_k.load(Relaxed), self.seen.load(Relaxed));
        std::sync::atomic::fence(Acquire);
        (self.seq.load(Relaxed) == s).then_some((s, min_k, seen))
    }
}

impl Statuses {
    /// A ring of `len` slots, a power of two, for blocks of `block`
    /// samples.
    pub fn new(len: usize, block: u64) -> Statuses {
        assert!(len.is_power_of_two() && block <= 1024);
        Statuses {
            block,
            slots: (0..len)
                .map(|_| Slot {
                    word: AtomicU64::new(0),
                    digest: AtomicU64::new(0),
                })
                .collect(),
            standing: AtomicU64::new(Standing::Idle { since: 0 }.pack()),
            start: StartSlot::new(),
        }
    }

    /// Requests a commit start ([`Hold::commit_from_seen`]), replacing
    /// any request the RT thread has not taken yet; the caller allows one
    /// request at a time.
    pub fn request_start(&self, min_k: u64, seen: u64) {
        debug_assert!(min_k != CANCEL);
        self.start.write(min_k, seen);
    }

    /// Requests a requested commit start dropped
    /// ([`Hold::cancel_commit_start`]), replacing any request the RT
    /// thread has not taken yet; the caller allows one request at a time.
    pub fn request_cancel(&self) {
        self.start.write(CANCEL, 0);
    }

    /// Whether the output commits.
    pub fn standing(&self) -> Standing {
        Standing::unpack(self.standing.load(Acquire))
    }

    fn set_standing(&self, s: Standing) {
        self.standing.store(s.pack(), Release);
    }

    fn slot(&self, k: u64) -> &Slot {
        &self.slots[(k % self.slots.len() as u64) as usize]
    }

    /// The status of block `k`; none before its first status, or once
    /// the ring reused its slot. The word read last decides: a slot being
    /// reused clears its word before its digest changes ([`Statuses::first`]),
    /// so a word read again unchanged after the digest pairs with it.
    pub fn get(&self, k: u64) -> Option<BlockStatus> {
        let s = self.slot(k);
        let mut word = s.word.load(Acquire);
        loop {
            let digest = s.digest.load(Acquire);
            let again = s.word.load(Acquire);
            if again == word {
                return decode(word, digest, k, self.block);
            }
            word = again;
        }
    }

    /// Supervisor. Moves block `k` to `withheld` for `cause`: from no
    /// status yet (not computed), `fenced` or `held`. Returns the status
    /// the block ends with: `withheld`, or `committed` when the RT thread
    /// committed it first. Returns none, recording nothing, while the slot
    /// still holds another block's open status or a later block's: the
    /// caller settles that block first and withholds `k` again.
    pub fn withhold(&self, k: u64, cause: Cause) -> Option<BlockStatus> {
        let s = self.slot(k);
        let mut cur = s.word.load(Acquire);
        loop {
            let old = decode(cur, s.digest.load(Acquire), k, self.block);
            let new = match old {
                Some(st) if matches!(st.kind, Kind::Committed | Kind::Withheld) => {
                    return Some(st);
                }
                Some(st) => BlockStatus {
                    kind: Kind::Withheld,
                    through: false,
                    cause,
                    ..st
                },
                None if occupant(cur, k) != Occupant::Free => return None,
                None => BlockStatus {
                    k,
                    kind: Kind::Withheld,
                    range: Range::Whole,
                    through: false,
                    rank: 0,
                    cause,
                    on_silence_of: 0,
                    digest: None,
                },
            };
            match s
                .word
                .compare_exchange(cur, encode(&new, self.block), Release, Acquire)
            {
                Ok(_) => return Some(new),
                Err(w) => cur = w,
            }
        }
    }

    /// Supervisor. Marks the held block `k` for commit through, naming
    /// the voters whose silence the evidence used; with `up_to` P inside
    /// a whole block, through `[kB, P)` only, reported `committed up to
    /// P`, `[P, (k + 1)B)` withheld. False when `k` is not `held`
    /// (committed, withheld, fenced or without a status), or `up_to`
    /// leaves none of its range.
    pub fn commit_through(&self, k: u64, on_silence_of: u8, up_to: Option<u64>) -> bool {
        let s = self.slot(k);
        let mut cur = s.word.load(Acquire);
        loop {
            let Some(st) = decode(cur, s.digest.load(Relaxed), k, self.block) else {
                return false;
            };
            if st.kind != Kind::Held {
                return false;
            }
            let range = match (up_to, st.range) {
                (None, r) => r,
                (Some(p), _) if p >= (k + 1) * self.block => st.range,
                (Some(p), Range::Whole) if p > k * self.block => Range::UpTo(p),
                (Some(p), Range::UpTo(q)) if p > k * self.block => Range::UpTo(p.min(q)),
                _ => return false,
            };
            let new = BlockStatus {
                through: true,
                range,
                on_silence_of: st.on_silence_of | on_silence_of,
                ..st
            };
            match s
                .word
                .compare_exchange(cur, encode(&new, self.block), Release, Acquire)
            {
                Ok(_) => return true,
                Err(w) => cur = w,
            }
        }
    }

    /// RT. Sets the first status of block `k`, unless the supervisor gave
    /// `k` a status first, which is returned, or the slot is not free
    /// ([`Cause::NoSlot`]). An earlier block's final word is cleared
    /// first, then the digest stored, then the word set, so no reader
    /// pairs the earlier block's word with this digest.
    fn first(&self, new: &BlockStatus) -> Result<(), BlockStatus> {
        let s = self.slot(new.k);
        let mut cur = s.word.load(Acquire);
        loop {
            if let Some(st) = decode(cur, s.digest.load(Acquire), new.k, self.block) {
                return Err(st);
            }
            if occupant(cur, new.k) != Occupant::Free {
                return Err(no_slot(new.k));
            }
            if cur != 0 {
                if let Err(w) = s.word.compare_exchange(cur, 0, Release, Acquire) {
                    cur = w;
                    continue;
                }
                cur = 0;
            }
            s.digest.store(new.digest.unwrap_or(0), Release);
            match s
                .word
                .compare_exchange(cur, encode(new, self.block), Release, Acquire)
            {
                Ok(_) => return Ok(()),
                Err(w) => cur = w,
            }
        }
    }

    /// RT. Replaces `old` by `new` for the same block; on a lost race the
    /// status the block has now.
    fn swap(&self, old: &BlockStatus, new: &BlockStatus) -> Result<(), BlockStatus> {
        let s = self.slot(old.k);
        match s.word.compare_exchange(
            encode(old, self.block),
            encode(new, self.block),
            Release,
            Acquire,
        ) {
            Ok(_) => Ok(()),
            // A block's open status keeps its slot, so a reused slot held a
            // final one, which no move starts from.
            Err(w) => Err(decode(w, s.digest.load(Relaxed), old.k, self.block)
                .unwrap_or_else(|| no_slot(old.k))),
        }
    }
}

/// A block-status change the RT thread made, in the order it made them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusChange {
    Block {
        output: u32,
        status: BlockStatus,
    },
    /// Blocks `[from, to)` were skipped, on every output: `withheld` at
    /// the skip, cause [`Cause::Skip`].
    Skipped {
        from: u64,
        to: u64,
    },
    /// Blocks `[from, to)` were not computed, on every output: the engine
    /// held no core. `withheld` there, cause [`Cause::NoCore`].
    NoCore {
        from: u64,
        to: u64,
    },
    /// Samples `[from, to)` of committed blocks the member flow refused
    /// (`Write::Held` or `Write::Failed`): their statuses say committed,
    /// the flow does not hold them.
    Unwritten {
        output: u32,
        from: u64,
        to: u64,
    },
}

/// Where the RT thread sends status changes, and the counters it keeps.
pub struct Changes<'a> {
    pub ring: &'a mut Producer<StatusChange>,
    /// Changes the ring had no room for.
    pub dropped: &'a AtomicU64,
    pub output: u32,
}

impl Changes<'_> {
    fn send(&mut self, status: BlockStatus) {
        self.push(StatusChange::Block {
            output: self.output,
            status,
        });
    }

    fn push(&mut self, c: StatusChange) {
        if self.ring.push(c).is_err() {
            self.dropped.fetch_add(1, Relaxed);
        }
    }

    /// A committed range the sink did not write.
    fn unwritten(&mut self, from: u64, len: usize) {
        self.push(StatusChange::Unwritten {
            output: self.output,
            from,
            to: from + len as u64,
        });
    }
}

/// Writes samples `[start, start + len)` to the member flow, planar in
/// `data` with `stride` samples per channel; false when it was not
/// written.
pub trait Sink {
    fn write(&mut self, start: u64, len: usize, data: &[f32], stride: usize) -> bool;
}

/// How an output commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Commits nothing and gives no status: unrealised, starting, or
    /// waiting for a commit start.
    Idle,
    /// Through only: from the next page boundary, and every block after
    /// it once it committed one.
    Withdrawn,
    /// Commits every block, healthy ones at once.
    Committing,
}

/// What the RT thread knows when it computes or moves a block.
#[derive(Clone, Copy, Debug)]
pub struct Ctx {
    /// F: blocks ending above it are fenced.
    pub fence: u64,
    pub ranks: Ranks,
}

/// The ranks a replica's blocks take when F passes them: one up to a
/// block and another from it on, as the `Baseline` entries the replica
/// knew of by each block's apply deadline place it; and a withdrawn
/// replica's fills, which rank as demoted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ranks {
    /// Blocks below `from`.
    pub rank: u8,
    /// Blocks from `from` on.
    pub next: u8,
    pub from: u64,
    /// Fills.
    pub fill: u8,
}

impl Ranks {
    /// `rank` for every block and fill.
    pub const fn flat(rank: u8) -> Ranks {
        Ranks {
            rank,
            next: rank,
            from: 0,
            fill: rank,
        }
    }

    /// The rank of block `k`, a fill or not.
    pub fn of(&self, k: u64, fill: bool) -> u8 {
        if fill {
            self.fill
        } else if k < self.from {
            self.rank
        } else {
            self.next
        }
    }

    /// One word: 5 bits of each rank, the block index above them.
    pub fn pack(&self) -> u64 {
        let r = |r: u8| u64::from(r.min(MAX_RANK));
        self.from << 15 | r(self.fill) << 10 | r(self.next) << 5 | r(self.rank)
    }

    pub fn unpack(word: u64) -> Ranks {
        Ranks {
            rank: (word & 0x1f) as u8,
            next: (word >> 5 & 0x1f) as u8,
            fill: (word >> 10 & 0x1f) as u8,
            from: word >> 15,
        }
    }
}

/// Whether an output commits, as its replica's reports state it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Standing {
    /// Commits nothing and gives no status (unrealised, starting, or
    /// waiting for a commit start), from block `since` on.
    Idle { since: u64 },
    /// Through only from block `since` on: standing `withdrawn`.
    Withdrawn { since: u64 },
    /// Committing from its commit start, sample index `from`.
    Committing { from: u64 },
}

impl Standing {
    fn pack(self) -> u64 {
        match self {
            Standing::Idle { since } => since << 2,
            Standing::Withdrawn { since } => since << 2 | 1,
            Standing::Committing { from } => from << 2 | 2,
        }
    }

    fn unpack(word: u64) -> Standing {
        match word & 3 {
            1 => Standing::Withdrawn { since: word >> 2 },
            2 => Standing::Committing { from: word >> 2 },
            _ => Standing::Idle { since: word >> 2 },
        }
    }
}

/// A block in the hold ring.
#[derive(Clone, Copy, Debug)]
struct Pending {
    status: BlockStatus,
    /// The samples it commits, its status's range.
    start: u64,
    end: u64,
    /// Its data slot in the ring.
    slot: usize,
    /// A withdrawn replica's fill: committed through only.
    fill: bool,
}

/// One output's commit state and hold ring, owned by the RT thread.
pub struct Hold {
    channels: usize,
    block: u64,
    /// One replica: no ring, every block committed once F passed it.
    alone: bool,
    /// Ring length in blocks.
    cap: usize,
    data: Vec<f32>,
    /// One flush's run of blocks, planar with `cap * block` per channel.
    run: Vec<f32>,
    pending: std::collections::VecDeque<Pending>,
    next_slot: usize,
    mode: Mode,
    /// The index the next commit continues from.
    frontier: Option<u64>,
    /// The end of the last block this output committed, while no block
    /// after it is final without a commit: a fill may continue from it.
    committed_end: Option<u64>,
    /// A requested commit start: the first page boundary in a block at
    /// or after it.
    start_from: Option<u64>,
    /// The block decided when the replica was last withdrawn for a cause:
    /// a commit start decided on statuses before it is stale.
    cause_at: Option<u64>,
    /// The last commit start request taken from the statuses.
    start_seq: u64,
    /// A requested stop at a sample index.
    stop_at: Option<u64>,
    /// Withdrawn by a stop at a block boundary right after a block it
    /// committed, and filling on from there.
    stopped: bool,
    /// The block after the last one computed.
    next: u64,
    /// Blocks withheld by the RT thread, and ring overflows.
    pub withheld: u64,
    pub overflows: u64,
    /// Writes the sink refused.
    pub failed_writes: u64,
}

/// What [`Hold`] did that concerns the other outputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// A block of this output was withheld: the replica is withdrawn on
    /// every output.
    pub withdraw: bool,
    /// One of them for a cause ([`BlockStatus::for_a_cause`]).
    pub cause: bool,
}

impl Outcome {
    fn add(&mut self, other: Outcome) {
        self.withdraw |= other.withdraw;
        self.cause |= other.cause;
    }

    fn withheld(&mut self, st: &BlockStatus) {
        self.withdraw = true;
        self.cause |= st.for_a_cause();
    }
}

impl Hold {
    /// The commit state of an output of `channels` channels, with a ring
    /// of `cap` blocks of `block` samples; `alone` for one replica.
    pub fn new(channels: usize, block: u64, cap: usize, alone: bool) -> Hold {
        let cap = if alone { 0 } else { cap.max(1) };
        let samples = cap * block as usize;
        Hold {
            channels,
            block,
            alone,
            cap,
            data: vec![0.0; channels * samples],
            run: vec![0.0; channels * samples],
            pending: std::collections::VecDeque::with_capacity(cap),
            next_slot: 0,
            mode: Mode::Idle,
            frontier: None,
            committed_end: None,
            start_from: None,
            cause_at: None,
            start_seq: 0,
            stop_at: None,
            stopped: false,
            next: 0,
            withheld: 0,
            overflows: 0,
            failed_writes: 0,
        }
    }

    /// One replica: no ring.
    pub fn alone(&self) -> bool {
        self.alone
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The first block in the ring, or block `next` before it is
    /// computed, from which this output withdraws the replica for a
    /// status it has not seen, with whether for a cause
    /// ([`BlockStatus::for_a_cause`]); none while idle. A block
    /// `withheld` withdraws from itself; a held block marked for commit
    /// through only up to P inside its range withholds `[P, (k + 1)B)`
    /// for a cause and withdraws from the block after it
    /// ([`Hold::advance`]). The replica is withdrawn on every output from
    /// there on ([`Hold::advance_below`]).
    pub fn withheld_unseen(&self, statuses: &Statuses, next: Option<u64>) -> Option<(u64, bool)> {
        if self.mode == Mode::Idle {
            return None;
        }
        for p in self
            .pending
            .iter()
            .filter(|p| p.status.kind != Kind::Withheld)
        {
            match statuses.get(p.status.k) {
                Some(st) if st.kind == Kind::Withheld => return Some((st.k, st.for_a_cause())),
                Some(BlockStatus {
                    k,
                    kind: Kind::Held,
                    through: true,
                    range: Range::UpTo(e),
                    ..
                }) if e < p.end => return Some((k + 1, true)),
                _ => {}
            }
        }
        let st = statuses.get(next?).filter(|s| s.kind == Kind::Withheld)?;
        Some((st.k, st.for_a_cause()))
    }

    /// Blocks in the ring.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Commits from the first page boundary in a block at or after
    /// `min_k` this output computes (from that block's start when alone).
    /// Ignored while the output commits.
    pub fn commit_from(&mut self, min_k: u64) {
        self.commit_from_seen(min_k, u64::MAX);
    }

    /// As [`Hold::commit_from`], for a commit start decided on the
    /// statuses up to block `seen`: ignored when the replica was withdrawn
    /// for a cause while a block after it was decided, which that
    /// decision did not see.
    pub fn commit_from_seen(&mut self, min_k: u64, seen: u64) {
        if self.mode == Mode::Committing || self.cause_at.is_some_and(|c| c > seen) {
            return;
        }
        self.start_from = Some(min_k);
    }

    /// Takes the latest commit start request in `statuses`
    /// ([`Statuses::request_start`], [`Statuses::request_cancel`]).
    pub fn take_request(&mut self, statuses: &Statuses) {
        let Some((seq, min_k, seen)) = statuses.start.read(self.start_seq) else {
            return;
        };
        self.start_seq = seq;
        if min_k == CANCEL {
            self.cancel_commit_start();
        } else {
            self.commit_from_seen(min_k, seen);
        }
    }

    /// Drops a requested commit start that has not taken: the output goes
    /// on as it is. One that took is no longer pending.
    pub fn cancel_commit_start(&mut self) {
        self.start_from = None;
    }

    /// Stops committing at sample index `p`: the block holding it commits
    /// `[kB, p)`, and the output is withdrawn from `p` on.
    pub fn stop_at(&mut self, p: u64) {
        self.stop_at = Some(p);
    }

    /// Commits nothing from now on and empties the ring: its open blocks
    /// are withheld, cause `cause`, and its committed ones written to
    /// `sink`, the writer they were committed for.
    pub fn idle(
        &mut self,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
        cause: Cause,
    ) {
        self.mode = Mode::Idle;
        statuses.set_standing(Standing::Idle { since: self.next });
        self.frontier = None;
        self.start_from = None;
        self.stop_at = None;
        self.stopped = false;
        for n in 0..self.pending.len() {
            let p = self.pending[n].status;
            let cur = statuses.get(p.k).unwrap_or(p);
            self.pending[n].status = match cur.kind {
                Kind::Fenced | Kind::Held => self.withhold_rt(statuses, changes, cur, cause),
                _ => cur,
            };
        }
        self.flush(changes, sink);
        self.pending.clear();
        self.committed_end = None;
    }

    /// The replica is withdrawn, `cause` for a block withheld for a
    /// cause or the supervisor's withdrawal: from now on this output
    /// commits through only, from the next page boundary on. Blocks in
    /// the ring that a withdrawn replica cannot fill are withheld at once.
    /// One replica is never withdrawn.
    /// A pending stop lapses: it applies only to a committing output. A
    /// pending commit start lapses on a cause, or unless the replica is
    /// withdrawn already: there every block is a fill, and a fill
    /// withheld while another replica commits leaves the commit start in
    /// place.
    pub fn withdraw(&mut self, statuses: &Statuses, changes: &mut Changes, cause: bool) {
        // One replica has no member to leave a block to: it goes on
        // committing after a block it withheld, from any index.
        if self.alone {
            return;
        }
        if cause {
            self.cause_at = Some(self.next);
        }
        if cause || self.mode != Mode::Withdrawn {
            self.start_from = None;
        }
        self.stop_at = None;
        self.stopped = false;
        if self.mode == Mode::Idle {
            return;
        }
        if self.mode == Mode::Committing {
            statuses.set_standing(Standing::Withdrawn { since: self.next });
        }
        self.mode = Mode::Withdrawn;
        // The page stays whole on this member after a block it committed.
        self.frontier = self.committed_end;
        for n in 0..self.pending.len() {
            let mut p = self.pending[n];
            p.status = statuses.get(p.status.k).unwrap_or(p.status);
            self.pending[n].status = p.status;
            match p.status.kind {
                Kind::Committed => {
                    self.frontier = Some(p.end);
                    continue;
                }
                Kind::Withheld => {
                    self.frontier = None;
                    continue;
                }
                // Commits on its evidence, its range as marked.
                Kind::Held if p.status.through => {
                    self.frontier = Some(match p.status.range {
                        Range::UpTo(e) => e.min(p.end),
                        _ => p.end,
                    });
                    continue;
                }
                _ => {}
            }
            let contiguous = self.frontier == Some(p.start);
            let at = if contiguous {
                p.start
            } else {
                p.start.next_multiple_of(PAGE)
            };
            if at >= p.end {
                let st = self.withhold_rt(statuses, changes, p.status, Cause::Unfillable);
                self.pending[n].status = st;
                self.frontier = None;
                continue;
            }
            self.pending[n].fill = true;
            self.frontier = Some(p.end);
            // A fenced block was not sent held yet: it becomes a fill,
            // without digest, from the page boundary. A held one keeps the
            // status it was sent with.
            if p.status.kind == Kind::Fenced && (at > p.start || p.status.digest.is_some()) {
                let new = BlockStatus {
                    range: if at > p.start {
                        Range::From(at)
                    } else {
                        p.status.range
                    },
                    digest: None,
                    ..p.status
                };
                match statuses.swap(&p.status, &new) {
                    Ok(()) => {
                        self.pending[n].status = new;
                        self.pending[n].start = at;
                        changes.send(new);
                    }
                    // Moved meanwhile: withheld, or held at its F pass.
                    Err(now) => self.pending[n].status = now,
                }
            } else if at > p.start {
                // A held block keeps its digest and rank; the commit start
                // still moves to the page boundary.
                let new = BlockStatus {
                    range: Range::From(at),
                    ..p.status
                };
                match statuses.swap(&p.status, &new) {
                    Ok(()) => {
                        self.pending[n].status = new;
                        self.pending[n].start = at;
                        changes.send(new);
                    }
                    // Marked for commit through, which then commits the
                    // whole block on its evidence, or withheld.
                    Err(now) => self.pending[n].status = now,
                }
            }
        }
    }

    /// RT. Withholds every block in the ring not yet final for `cause`,
    /// and writes the committed ones before them.
    pub fn withhold_open(
        &mut self,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
        cause: Cause,
    ) {
        for n in 0..self.pending.len() {
            let p = self.pending[n].status;
            let cur = statuses.get(p.k).unwrap_or(p);
            self.pending[n].status = match cur.kind {
                Kind::Fenced | Kind::Held => self.withhold_rt(statuses, changes, cur, cause),
                _ => cur,
            };
        }
        self.flush(changes, sink);
    }

    /// Moves `st` to `withheld` from the RT thread; the status the block
    /// ends with.
    fn withhold_rt(
        &mut self,
        statuses: &Statuses,
        changes: &mut Changes,
        st: BlockStatus,
        cause: Cause,
    ) -> BlockStatus {
        let new = BlockStatus {
            kind: Kind::Withheld,
            through: false,
            cause,
            ..st
        };
        match statuses.swap(&st, &new) {
            Ok(()) => {
                self.withheld += 1;
                changes.send(new);
                new
            }
            Err(now) => now,
        }
    }

    /// RT. Takes block `k`, planar in `data` with B samples per channel,
    /// with its digest, `None` for no digest: sets its first status and
    /// commits it at once, holds it in the ring, or withholds it.
    #[allow(clippy::too_many_arguments)]
    pub fn compute(
        &mut self,
        k: u64,
        data: &[f32],
        digest: Option<u64>,
        ctx: Ctx,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
    ) -> Outcome {
        // A withdrawal while k is decided takes effect from k.
        self.next = k;
        let out = self.decide(k, data, digest, ctx, statuses, changes, sink);
        self.next = k + 1;
        out
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn decide(
        &mut self,
        k: u64,
        data: &[f32],
        digest: Option<u64>,
        ctx: Ctx,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
    ) -> Outcome {
        let b = self.block;
        let (start, end) = (k * b, (k + 1) * b);
        let healthy = digest == Some(HEALTHY);
        let mut range = Range::Whole;
        let mut live = (start, end);
        // The overflowing block is past its hold cap by now: a stalled
        // supervisor freezes F.
        let mut out = Outcome::default();
        // Each pass ends with the front block final, unless the supervisor
        // moved it meanwhile; it moves a block a bounded number of times.
        while !self.alone && self.mode != Mode::Idle && self.pending.len() == self.cap {
            out.add(self.advance(ctx, statuses, changes, sink));
            if self.pending.len() < self.cap {
                break;
            }
            let front = self.pending[0].status;
            let front = statuses.get(front.k).unwrap_or(front);
            let st = self.withhold_rt(statuses, changes, front, Cause::Overflow);
            self.pending[0].status = st;
            if st.kind == Kind::Withheld {
                self.overflows += u64::from(st.cause == Cause::Overflow);
                out.withheld(&st);
            }
            self.flush(changes, sink);
        }
        // A block withheld for a cause withdraws the replica before block
        // k is decided: k follows the withdrawn rules.
        if out.withdraw {
            self.withdraw(statuses, changes, out.cause);
        }
        if let Some(min_k) = self.start_from
            && k >= min_k
            && self.mode != Mode::Committing
        {
            let at = if self.alone {
                start
            } else {
                start.next_multiple_of(PAGE)
            };
            // An output filling on from a stop takes it only in a block
            // that starts on a page boundary: inside a block it would end
            // the fill below the commit start, and with the stop no other
            // member need commit that part of the page.
            let filling_on = self.stopped && self.frontier == Some(start);
            if at < end && !(filling_on && at > start) {
                self.start_from = None;
                self.stopped = false;
                self.mode = Mode::Committing;
                statuses.set_standing(Standing::Committing { from: at });
                self.frontier = Some(at);
                // A fill in progress ends below the commit start.
                if at > start {
                    range = Range::From(at);
                    live.0 = at;
                }
            }
        }
        let mut fill = false;
        let mut stopped = false;
        match self.mode {
            Mode::Idle => return out,
            Mode::Committing => {
                if let Some(p) = self.stop_at
                    && p < end
                {
                    self.stop_at = None;
                    if p <= start {
                        self.mode = Mode::Withdrawn;
                        statuses.set_standing(Standing::Withdrawn { since: k });
                        // A stop at the start of this block, right after
                        // the block before it, leaves the page whole on
                        // this member: it fills on from there.
                        if p != start || self.frontier != Some(start) {
                            self.frontier = None;
                        } else {
                            self.stopped = true;
                        }
                    } else {
                        range = Range::UpTo(p);
                        live.1 = p;
                        stopped = true;
                    }
                }
            }
            Mode::Withdrawn => {}
        }
        if self.mode == Mode::Withdrawn {
            fill = true;
            if self.frontier != Some(start) {
                let at = start.next_multiple_of(PAGE);
                if at >= end {
                    // No page boundary, and the block before it final
                    // without a commit.
                    self.frontier = None;
                    let st = BlockStatus {
                        k,
                        kind: Kind::Withheld,
                        range: Range::Whole,
                        through: false,
                        rank: ctx.ranks.of(k, true),
                        cause: Cause::Unfillable,
                        on_silence_of: 0,
                        // Its own digest: a block computed degraded is
                        // reported so, its page left out of the verdicts
                        // rather than judged by its hash.
                        digest,
                    };
                    if statuses.first(&st).is_ok() {
                        self.withheld += 1;
                        changes.send(st);
                    }
                    self.committed_end = None;
                    return out;
                }
                if at > start {
                    range = Range::From(at);
                    live.0 = at;
                }
            }
        }
        let passed = ctx.fence >= end;
        let ahead = self
            .pending
            .iter()
            .any(|p| p.status.kind != Kind::Committed);
        let kind = if !passed {
            Kind::Fenced
        } else if self.alone || (!fill && healthy && !ahead) {
            Kind::Committed
        } else {
            Kind::Held
        };
        let st = BlockStatus {
            k,
            kind,
            range,
            through: false,
            rank: ctx.ranks.of(k, fill),
            cause: Cause::None,
            on_silence_of: 0,
            digest: if fill { None } else { digest },
        };
        let st = match (self.alone, kind) {
            // One replica cannot hold a fenced block.
            (true, Kind::Fenced) => BlockStatus {
                kind: Kind::Withheld,
                cause: Cause::ClockStep,
                ..st
            },
            _ => st,
        };
        if let Err(now) = statuses.first(&st) {
            // The supervisor withheld it before it was computed, or its
            // slot is not free.
            if now.cause == Cause::NoSlot {
                changes.send(now);
            }
            if now.kind == Kind::Withheld {
                out.withheld(&now);
            }
            self.frontier = None;
            self.committed_end = None;
            return out;
        }
        changes.send(st);
        match st.kind {
            Kind::Withheld => {
                self.withheld += 1;
                self.frontier = None;
                self.committed_end = None;
                out.withheld(&st);
                return out;
            }
            Kind::Committed if self.pending.is_empty() => {
                let off = (live.0 - start) as usize;
                let len = (live.1 - live.0) as usize;
                if !sink.write(live.0, len, &data[off..], b as usize) {
                    self.failed_writes += 1;
                    changes.unwritten(live.0, len);
                }
                self.frontier = Some(live.1);
                self.committed_end = Some(live.1);
            }
            _ => {
                let slot = self.next_slot;
                self.next_slot = (slot + 1) % self.cap;
                let n = b as usize;
                let samples = self.cap * n;
                for c in 0..self.channels {
                    self.data[c * samples + slot * n..][..n].copy_from_slice(&data[c * n..][..n]);
                }
                self.pending.push_back(Pending {
                    status: st,
                    start: live.0,
                    end: live.1,
                    slot,
                    fill,
                });
                self.frontier = Some(live.1);
            }
        }
        if stopped {
            self.mode = Mode::Withdrawn;
            statuses.set_standing(Standing::Withdrawn { since: k + 1 });
            self.frontier = None;
        }
        out
    }

    /// RT. Moves the blocks in the ring as F, the evidence and the blocks
    /// before them allow, and writes those committed in order.
    pub fn advance(
        &mut self,
        ctx: Ctx,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
    ) -> Outcome {
        self.advance_below(ctx, u64::MAX, statuses, changes, sink)
    }

    /// RT. [`Hold::advance`] for the blocks below block `limit` only: a
    /// block another output withholds withdraws the replica on this one
    /// before any block from it on moves.
    pub fn advance_below(
        &mut self,
        ctx: Ctx,
        limit: u64,
        statuses: &Statuses,
        changes: &mut Changes,
        sink: &mut impl Sink,
    ) -> Outcome {
        let mut out = Outcome::default();
        let mut prefix = true;
        for n in 0..self.pending.len() {
            let p = self.pending[n];
            if p.status.k >= limit {
                break;
            }
            // The supervisor may have moved it.
            let mut cur = statuses.get(p.status.k).unwrap_or(p.status);
            loop {
                let target = match cur.kind {
                    Kind::Committed | Kind::Withheld => break,
                    Kind::Fenced if ctx.fence < (cur.k + 1) * self.block => break,
                    Kind::Fenced => {
                        let healthy = cur.digest == Some(HEALTHY);
                        if !p.fill && healthy && prefix {
                            BlockStatus {
                                kind: Kind::Committed,
                                ..cur
                            }
                        } else {
                            BlockStatus {
                                kind: Kind::Held,
                                rank: ctx.ranks.of(cur.k, p.fill),
                                ..cur
                            }
                        }
                    }
                    Kind::Held => {
                        let healthy = cur.digest == Some(HEALTHY);
                        let auto = !p.fill && healthy && prefix;
                        if !cur.through && !auto {
                            break;
                        }
                        BlockStatus {
                            kind: Kind::Committed,
                            ..cur
                        }
                    }
                };
                match statuses.swap(&cur, &target) {
                    Ok(()) => {
                        changes.send(target);
                        cur = target;
                        // Committed through up to P: `[P, (k + 1)B)` is
                        // withheld for a cause, which withdraws the replica
                        // before any block after it is decided.
                        if let (Kind::Committed, Range::UpTo(e)) = (cur.kind, cur.range)
                            && e < self.pending[n].end
                        {
                            self.pending[n].end = e;
                            out.withdraw = true;
                            out.cause = true;
                        }
                    }
                    Err(now) => cur = now,
                }
            }
            if cur.kind == Kind::Withheld && p.status.kind != Kind::Withheld {
                out.withheld(&cur);
            }
            self.pending[n].status = cur;
            prefix &= cur.kind == Kind::Committed;
            // The replica is withdrawn: the blocks after this one follow
            // the withdrawn rules before F moves them.
            if out.withdraw && !self.alone {
                break;
            }
        }
        self.flush(changes, sink);
        out
    }

    /// Writes the committed blocks at the front of the ring, each run of
    /// contiguous ones in one write, and drops the withheld ones.
    fn flush(&mut self, changes: &mut Changes, sink: &mut impl Sink) {
        let n = self.block as usize;
        let samples = self.cap * n;
        loop {
            match self.pending.front().map(|p| p.status.kind) {
                Some(Kind::Withheld) => {
                    self.pending.pop_front();
                    self.committed_end = None;
                }
                Some(Kind::Committed) => {
                    let first = self.pending[0];
                    let mut len = 0;
                    let mut end = first.start;
                    while let Some(p) = self.pending.front()
                        && p.status.kind == Kind::Committed
                        && p.start == end
                    {
                        let p = self.pending.pop_front().expect("checked");
                        let off = (p.start - p.status.k * self.block) as usize;
                        let m = (p.end - p.start) as usize;
                        for c in 0..self.channels {
                            let src = &self.data[c * samples + p.slot * n + off..][..m];
                            self.run[c * samples + len..][..m].copy_from_slice(src);
                        }
                        len += m;
                        end = p.end;
                    }
                    if !sink.write(first.start, len, &self.run, samples) {
                        self.failed_writes += 1;
                        changes.unwritten(first.start, len);
                    }
                    self.committed_end = Some(end);
                }
                _ => return,
            }
        }
    }
}

/// One page of one output as this replica computed it: what replicas
/// compare to find out whether their member flows are bit-identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageHash {
    pub output: u32,
    /// The samples `[page * PAGE, (page + 1) * PAGE)`.
    pub page: u64,
    /// XXH3-64 over the page's samples in index order, the channels of
    /// one index interleaved, each sample's f32 bit pattern as 4 bytes
    /// little endian.
    pub hash: u64,
    /// [`fingerprint`] of the log entries applied in the page.
    pub fingerprint: u64,
    /// Every sample of the page was committed.
    pub committed: bool,
}

/// The entry fingerprint of a page: XXH3-64 over the `(log id,
/// at_index)` pairs of the entries applied in it, sorted, each number as
/// 8 bytes little endian. An entry belongs to the page holding its
/// `at_index`. Sorts `entries`.
pub fn fingerprint(entries: &mut [(u64, u64)]) -> u64 {
    entries.sort_unstable();
    let mut h = xxhash_rust::xxh3::Xxh3Default::new();
    for (id, at) in entries.iter() {
        h.update(&id.to_le_bytes());
        h.update(&at.to_le_bytes());
    }
    h.digest()
}

/// A hashed page, until every block overlapping it is final.
#[derive(Clone, Copy, Debug)]
struct Waiting {
    record: PageHash,
    /// The page's first block not yet seen final, and its last block.
    next: u64,
    last: u64,
}

/// One output's page hashes, owned by the RT thread: the page being
/// hashed, and the pages hashed whose blocks are not all final yet.
pub struct Pages {
    output: u32,
    channels: usize,
    block: u64,
    hasher: xxhash_rust::xxh3::Xxh3Default,
    /// The page being hashed and the index of the next sample it takes.
    current: Option<(u64, u64)>,
    /// One block's samples as hashed.
    bytes: Vec<u8>,
    waiting: std::collections::VecDeque<Waiting>,
    /// Pages waiting at most.
    cap: usize,
}

impl Pages {
    /// The page hashes of output `output`, of `channels` channels, in
    /// blocks of `block` samples, with a hold ring of `hold` blocks.
    pub fn new(output: u32, channels: usize, block: u64, hold: usize) -> Pages {
        // Only a block in the hold ring is not final, and it overlaps at
        // most two pages.
        let cap = 2 * hold + 2;
        Pages {
            output,
            channels,
            block,
            hasher: xxhash_rust::xxh3::Xxh3Default::new(),
            current: None,
            bytes: vec![0; channels * block as usize * 4],
            waiting: std::collections::VecDeque::with_capacity(cap),
            cap,
        }
    }

    /// RT. Hashes block `k`, planar in `data` with B samples per channel.
    /// A page whose every sample was hashed, in order, waits for its
    /// blocks to be final; a page missing one (a skip, a block not
    /// computed for this output) gets no record. `fingerprint` is the
    /// entry fingerprint of the page that ends in block `k`, none when
    /// it is unknown: that page then gets no record, counted in
    /// `dropped`, as is the oldest page waiting when too many wait.
    pub fn hash(&mut self, k: u64, data: &[f32], fingerprint: Option<u64>, dropped: &AtomicU64) {
        let b = self.block;
        let (start, end) = (k * b, (k + 1) * b);
        let mut from = start;
        while from < end {
            let page = from / PAGE;
            let to = end.min((page + 1) * PAGE);
            match self.current {
                Some((p, next)) if p == page && next == from => {}
                _ if from == page * PAGE => self.hasher.reset(),
                _ => {
                    self.current = None;
                    from = to;
                    continue;
                }
            }
            let mut len = 0;
            for i in (from - start) as usize..(to - start) as usize {
                for c in 0..self.channels {
                    let x = data[c * b as usize + i].to_bits().to_le_bytes();
                    self.bytes[len..len + 4].copy_from_slice(&x);
                    len += 4;
                }
            }
            self.hasher.update(&self.bytes[..len]);
            self.current = Some((page, to));
            if to == (page + 1) * PAGE {
                self.current = None;
                match fingerprint {
                    None => {
                        dropped.fetch_add(1, Relaxed);
                    }
                    Some(fingerprint) => {
                        if self.waiting.len() == self.cap {
                            self.waiting.pop_front();
                            dropped.fetch_add(1, Relaxed);
                        }
                        self.waiting.push_back(Waiting {
                            record: PageHash {
                                output: self.output,
                                page,
                                hash: self.hasher.digest(),
                                fingerprint,
                                committed: true,
                            },
                            next: page * PAGE / b,
                            last: (to - 1) / b,
                        });
                    }
                }
            }
            from = to;
        }
    }

    /// Drops the page being hashed: its samples before now came from
    /// another state, so it gets no record.
    pub fn restart(&mut self) {
        self.current = None;
    }

    /// A replay's pages: sends the record of every waiting page, with
    /// `committed` false, since a replay holds and writes nothing. A
    /// record `ring` has no room for is counted in `dropped`.
    pub fn settle_unheld(&mut self, ring: &mut Producer<PageHash>, dropped: &AtomicU64) {
        while let Some(mut w) = self.waiting.pop_front() {
            w.record.committed = false;
            if ring.push(w.record).is_err() {
                dropped.fetch_add(1, Relaxed);
            }
        }
    }

    /// RT. Sends the record of each waiting page whose every block is
    /// final: committed, withheld, or without a status, computed while
    /// the output committed nothing. A record `ring` has no room for is
    /// counted in `dropped`.
    pub fn settle(
        &mut self,
        statuses: &Statuses,
        ring: &mut Producer<PageHash>,
        dropped: &AtomicU64,
    ) {
        let b = self.block;
        self.waiting.retain_mut(|w| {
            let (start, end) = (w.record.page * PAGE, (w.record.page + 1) * PAGE);
            while w.next <= w.last {
                let k = w.next;
                match statuses.get(k) {
                    Some(s) if matches!(s.kind, Kind::Fenced | Kind::Held) => return true,
                    Some(s) if s.kind == Kind::Committed => {
                        let (from, to) = match s.range {
                            Range::Whole => (k * b, (k + 1) * b),
                            Range::From(p) => (p, (k + 1) * b),
                            Range::UpTo(p) => (k * b, p),
                        };
                        // The block's samples in the page are committed.
                        w.record.committed &=
                            from <= start.max(k * b) && to >= end.min((k + 1) * b);
                    }
                    _ => w.record.committed = false,
                }
                w.next += 1;
            }
            if ring.push(w.record).is_err() {
                dropped.fetch_add(1, Relaxed);
            }
            false
        });
    }
}

#[cfg(test)]
mod tests;
