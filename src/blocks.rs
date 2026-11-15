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
//! in one write.
//!
//! Nothing here allocates once built.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::atomic::{AtomicU64, Ordering::Acquire, Ordering::Relaxed, Ordering::Release};

use rtrb::Producer;

/// Samples per merge page: an output's member resumes at a multiple of it.
pub const PAGE: u64 = 1024;

/// What a status says about the samples it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

const CAUSES: [Cause; 10] = [
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
];

/// The samples of block k a status covers. A block split at P carries
/// the status for one range; the other range is `withheld` without a
/// cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Range {
    Whole,
    /// `[P, (k + 1)B)`: a commit start at P; `[kB, P)` is withheld.
    From(u64),
    /// `[kB, P)`: a stop at P; `[P, (k + 1)B)` is withheld.
    UpTo(u64),
}

/// One block's status for one output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
// split offset P - kB (11), through (1), digest present (1), rank (4),
// cause (4), onSilenceOf (8), the block index's low 30 bits.
const KIND_BITS: u32 = 3;
const RANGE_SHIFT: u32 = 3;
const SPLIT_SHIFT: u32 = 5;
const THROUGH_BIT: u32 = 16;
const DIGEST_BIT: u32 = 17;
const RANK_SHIFT: u32 = 18;
const CAUSE_SHIFT: u32 = 22;
const SILENCE_SHIFT: u32 = 26;
const TAG_SHIFT: u32 = 34;
const TAG_MASK: u64 = (1 << 30) - 1;

/// Ranks from 0 to this; a status carries 4 bits of rank.
pub const MAX_RANK: u8 = 15;

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
    let split = k * block + (word >> SPLIT_SHIFT & 0x7ff);
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
        rank: (word >> RANK_SHIFT & 0xf) as u8,
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
        }
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
    /// the voters whose silence the evidence used. False when `k` is not
    /// `held`: committed, withheld, fenced or without a status.
    pub fn commit_through(&self, k: u64, on_silence_of: u8) -> bool {
        let s = self.slot(k);
        let mut cur = s.word.load(Acquire);
        loop {
            let Some(st) = decode(cur, s.digest.load(Relaxed), k, self.block) else {
                return false;
            };
            if st.kind != Kind::Held {
                return false;
            }
            let new = BlockStatus {
                through: true,
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
    pub rank: u8,
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
    /// A requested stop at a sample index.
    stop_at: Option<u64>,
    /// Blocks withheld by the RT thread, and ring overflows.
    pub withheld: u64,
    pub overflows: u64,
    /// Writes the sink refused.
    pub failed_writes: u64,
}

/// What [`Hold`] did that concerns the other outputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// A block of this output was withheld for a cause: the replica is
    /// withdrawn on every output.
    pub withdraw: bool,
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
            stop_at: None,
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

    /// Blocks in the ring.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Commits from the first page boundary in a block at or after
    /// `min_k` this output computes (from that block's start when alone).
    pub fn commit_from(&mut self, min_k: u64) {
        self.start_from = Some(min_k);
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
        self.frontier = None;
        self.start_from = None;
        self.stop_at = None;
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

    /// The replica is withdrawn: from now on this output commits through
    /// only, from the next page boundary on. Blocks in the ring that a
    /// withdrawn replica cannot fill are withheld at once. One replica
    /// is never withdrawn.
    /// A pending commit start or stop lapses in every mode: the
    /// supervisor names a commit start again once it still wants one.
    pub fn withdraw(&mut self, statuses: &Statuses, changes: &mut Changes) {
        // One replica has no member to leave a block to: it goes on
        // committing after a block it withheld, from any index.
        if self.alone {
            return;
        }
        self.start_from = None;
        self.stop_at = None;
        if self.mode == Mode::Idle {
            return;
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
            out.withdraw |= self.advance(ctx, statuses, changes, sink).withdraw;
            if self.pending.len() < self.cap {
                break;
            }
            let front = self.pending[0].status;
            let front = statuses.get(front.k).unwrap_or(front);
            let st = self.withhold_rt(statuses, changes, front, Cause::Overflow);
            self.pending[0].status = st;
            if st.kind == Kind::Withheld {
                self.overflows += u64::from(st.cause == Cause::Overflow);
                out.withdraw = true;
            }
            self.flush(changes, sink);
        }
        // A block withheld for a cause withdraws the replica before block
        // k is decided: k follows the withdrawn rules.
        if out.withdraw {
            self.withdraw(statuses, changes);
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
            if at < end {
                self.start_from = None;
                self.mode = Mode::Committing;
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
                        self.frontier = None;
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
                        rank: ctx.rank,
                        cause: Cause::Unfillable,
                        on_silence_of: 0,
                        digest: None,
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
            rank: ctx.rank,
            cause: Cause::None,
            on_silence_of: 0,
            digest: if fill { None } else { digest },
        };
        let st = match (self.alone, kind) {
            // One replica cannot hold a fenced block.
            (true, Kind::Fenced) => BlockStatus {
                kind: Kind::Withheld,
                cause: Cause::Skip,
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
            out.withdraw |= now.kind == Kind::Withheld;
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
                out.withdraw = true;
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
        let mut out = Outcome::default();
        let mut prefix = true;
        for n in 0..self.pending.len() {
            let p = self.pending[n];
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
                                rank: ctx.rank,
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
                    }
                    Err(now) => cur = now,
                }
            }
            if cur.kind == Kind::Withheld && p.status.kind != Kind::Withheld {
                out.withdraw = true;
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

#[cfg(test)]
mod tests {
    use super::*;

    const B: u64 = 48;
    const CAP: usize = 20;

    /// The ranges written and their samples, channel 0 and 1.
    #[derive(Default)]
    struct Recorder {
        writes: Vec<(u64, usize)>,
        samples: Vec<(u64, f32, f32)>,
        /// Refuses every write, as a member flow holding or failing.
        refuse: bool,
    }

    impl Sink for Recorder {
        fn write(&mut self, start: u64, len: usize, data: &[f32], stride: usize) -> bool {
            if self.refuse {
                return false;
            }
            self.writes.push((start, len));
            for i in 0..len {
                self.samples
                    .push((start + i as u64, data[i], data[stride + i]));
            }
            true
        }
    }

    impl Recorder {
        /// Every sample written carries its own index on channel 0 and
        /// its negation on channel 1, and none was written twice.
        fn check(&self) {
            for &(i, a, b) in &self.samples {
                assert_eq!((a, b), (i as f32, -(i as f32)), "sample {i}");
            }
            let mut idx: Vec<u64> = self.samples.iter().map(|s| s.0).collect();
            let n = idx.len();
            idx.sort_unstable();
            idx.dedup();
            assert_eq!(idx.len(), n, "a sample written twice");
        }

        fn wrote(&self, k: u64) -> bool {
            self.samples.iter().any(|s| s.0 / B == k)
        }
    }

    struct T {
        hold: Hold,
        statuses: Statuses,
        tx: Producer<StatusChange>,
        rx: rtrb::Consumer<StatusChange>,
        dropped: AtomicU64,
        sink: Recorder,
        ctx: Ctx,
    }

    impl T {
        fn new(alone: bool) -> T {
            let (tx, rx) = rtrb::RingBuffer::new(4096);
            T {
                hold: Hold::new(2, B, CAP, alone),
                statuses: Statuses::new(64, B),
                tx,
                rx,
                dropped: AtomicU64::new(0),
                sink: Recorder::default(),
                ctx: Ctx {
                    fence: u64::MAX,
                    rank: 0,
                },
            }
        }

        fn committing(alone: bool, from: u64) -> T {
            let mut t = T::new(alone);
            t.hold.commit_from(from);
            t
        }

        fn changes(&mut self) -> Changes<'_> {
            Changes {
                ring: &mut self.tx,
                dropped: &self.dropped,
                output: 0,
            }
        }

        fn block(k: u64) -> Vec<f32> {
            let mut d = vec![0.0; 2 * B as usize];
            for i in 0..B as usize {
                let x = (k * B + i as u64) as f32;
                d[i] = x;
                d[B as usize + i] = -x;
            }
            d
        }

        fn compute(&mut self, k: u64, digest: Option<u64>) -> Outcome {
            let data = T::block(k);
            let (ctx, statuses) = (self.ctx, &self.statuses);
            let mut changes = Changes {
                ring: &mut self.tx,
                dropped: &self.dropped,
                output: 0,
            };
            self.hold.compute(
                k,
                &data,
                digest,
                ctx,
                statuses,
                &mut changes,
                &mut self.sink,
            )
        }

        fn advance(&mut self) -> Outcome {
            let (ctx, statuses) = (self.ctx, &self.statuses);
            let mut changes = Changes {
                ring: &mut self.tx,
                dropped: &self.dropped,
                output: 0,
            };
            self.hold
                .advance(ctx, statuses, &mut changes, &mut self.sink)
        }

        fn withdraw(&mut self) {
            let statuses = &self.statuses;
            let mut changes = Changes {
                ring: &mut self.tx,
                dropped: &self.dropped,
                output: 0,
            };
            self.hold.withdraw(statuses, &mut changes);
        }

        fn kind(&self, k: u64) -> Option<Kind> {
            self.statuses.get(k).map(|s| s.kind)
        }

        /// The changes sent since the last call.
        fn sent(&mut self) -> Vec<StatusChange> {
            let mut v = Vec::new();
            while let Ok(c) = self.rx.pop() {
                v.push(c);
            }
            v
        }
    }

    /// The block with a page boundary at its start, and one after it.
    const K0: u64 = 1024 * 3 / 16 * 1000;
    const DEGRADED: Option<u64> = Some(7);

    #[test]
    fn a_status_word_round_trips() {
        let st = BlockStatus {
            k: (1 << 40) + 77,
            kind: Kind::Held,
            range: Range::From(((1 << 40) + 77) * B + 47),
            through: true,
            rank: 5,
            cause: Cause::Silence,
            on_silence_of: 0b1010_0001,
            digest: Some(42),
        };
        assert_eq!(decode(encode(&st, B), 42, st.k, B), Some(st));
        // Another block in the same slot reads as no status.
        assert_eq!(decode(encode(&st, B), 42, st.k + 64, B), None);
        let up_to = BlockStatus {
            range: Range::UpTo(st.k * B + 1),
            digest: None,
            ..st
        };
        assert_eq!(decode(encode(&up_to, B), 0, st.k, B), Some(up_to));
        assert_eq!(decode(0, 0, 0, B), None);
    }

    #[test]
    fn the_healthy_digest_is_xxh3_over_nothing() {
        assert_eq!(HEALTHY, xxhash_rust::xxh3::xxh3_64(&[]));
        assert_ne!(degraded_digest(&[], 0, 0), HEALTHY);
        assert_ne!(degraded_digest(&[1], 0, 0), degraded_digest(&[], 1, 0));
        // The crossfade legs of one slot differ.
        assert_ne!(degraded_digest(&[], 1, 0), degraded_digest(&[], 0, 1));
    }

    /// A healthy block with F past it is committed and written at once,
    /// alone in its write; nothing stays in the ring.
    #[test]
    fn a_healthy_block_commits_at_once() {
        let mut t = T::committing(false, K0);
        for k in K0..K0 + 3 {
            assert!(!t.compute(k, Some(HEALTHY)).withdraw);
            assert_eq!(t.kind(k), Some(Kind::Committed));
        }
        assert!(t.hold.is_empty());
        assert_eq!(
            t.sink.writes,
            [(K0 * B, 48), ((K0 + 1) * B, 48), ((K0 + 2) * B, 48)]
        );
        t.sink.check();
    }

    /// A block computed while F has not passed it is fenced and held in
    /// the ring with the blocks after it; once F passes them they commit
    /// in one write.
    #[test]
    fn fenced_blocks_commit_in_one_write_once_f_passes() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = K0 * B;
        for k in K0..K0 + 4 {
            t.compute(k, Some(HEALTHY));
            assert_eq!(t.kind(k), Some(Kind::Fenced));
        }
        assert!(t.sink.writes.is_empty());
        t.ctx.fence = (K0 + 3) * B;
        t.advance();
        assert_eq!(t.sink.writes, [(K0 * B, 3 * 48)]);
        assert_eq!(t.kind(K0 + 3), Some(Kind::Fenced));
        t.ctx.fence = u64::MAX;
        t.advance();
        assert_eq!(t.sink.writes[1], ((K0 + 3) * B, 48));
        t.sink.check();
    }

    /// A degraded block is held at the rank F passed it with, with its
    /// digest; a healthy block computed after it is held too, never
    /// committed at once, and both commit in one write once the degraded
    /// one is committed through.
    #[test]
    fn a_block_behind_a_held_one_is_held() {
        let mut t = T::committing(false, K0);
        t.ctx.rank = 3;
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, Some(HEALTHY));
        let held = t.statuses.get(K0).unwrap();
        assert_eq!(
            (held.kind, held.rank, held.digest),
            (Kind::Held, 3, DEGRADED)
        );
        let next = t.statuses.get(K0 + 1).unwrap();
        assert_eq!((next.kind, next.digest), (Kind::Held, Some(HEALTHY)));
        assert!(t.sink.writes.is_empty());
        assert!(t.statuses.commit_through(K0, 0b10));
        t.advance();
        assert_eq!(t.statuses.get(K0).unwrap().on_silence_of, 0b10);
        assert_eq!(t.kind(K0 + 1), Some(Kind::Committed));
        assert_eq!(t.sink.writes, [(K0 * B, 96)]);
        t.sink.check();
    }

    /// A healthy block held behind a degraded one commits once every
    /// block before it is committed, without evidence of its own; one
    /// committed on evidence before them waits in the ring for them, so
    /// writes stay in order.
    #[test]
    fn a_held_block_commits_once_every_earlier_one_did() {
        let mut t = T::committing(false, K0);
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, DEGRADED);
        t.compute(K0 + 2, Some(HEALTHY));
        assert!(t.statuses.commit_through(K0 + 1, 0));
        t.advance();
        assert_eq!(t.kind(K0 + 1), Some(Kind::Committed));
        assert!(t.sink.writes.is_empty(), "written before K0");
        assert!(t.statuses.commit_through(K0, 0));
        t.advance();
        assert_eq!(t.kind(K0 + 2), Some(Kind::Committed));
        assert_eq!(t.sink.writes, [(K0 * B, 144)]);
    }

    /// A block the supervisor withholds is never written; the replica is
    /// withdrawn, and a block after it without a page boundary is
    /// withheld at once, unfillable.
    #[test]
    fn a_withheld_block_is_never_written_and_withdraws() {
        let mut t = T::committing(false, K0);
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, Some(HEALTHY));
        let st = t.statuses.withhold(K0, Cause::HoldCap).unwrap();
        assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::HoldCap));
        assert!(t.advance().withdraw);
        t.withdraw();
        assert_eq!(t.hold.mode(), Mode::Withdrawn);
        let next = t.statuses.get(K0 + 1).unwrap();
        assert_eq!((next.kind, next.cause), (Kind::Withheld, Cause::Unfillable));
        t.advance();
        assert!(t.sink.writes.is_empty());
        assert!(t.hold.is_empty());
        // Later blocks without a page boundary are withheld at once.
        t.compute(K0 + 2, Some(HEALTHY));
        assert_eq!(t.kind(K0 + 2), Some(Kind::Withheld));
        assert!(t.sink.writes.is_empty());
    }

    /// After a block final without a commit, a withdrawn replica fills
    /// from the next page boundary on, through only: the block holding it
    /// reports `From(P)`, is held without a digest, and commits
    /// `[P, (k + 1)B)` on evidence; the blocks after it are filled as
    /// well.
    #[test]
    fn a_withdrawn_replica_fills_from_the_next_page_boundary() {
        let mut t = T::committing(false, K0);
        t.compute(K0, DEGRADED);
        t.statuses.withhold(K0, Cause::HoldCap).unwrap();
        assert!(t.advance().withdraw);
        t.withdraw();
        // The next page boundary lies inside block k1.
        let p = (K0 * B + 1).next_multiple_of(PAGE);
        let k1 = p / B;
        assert!(p > k1 * B);
        for k in K0 + 1..k1 {
            t.compute(k, Some(HEALTHY));
            assert_eq!(t.kind(k), Some(Kind::Withheld));
        }
        t.compute(k1, Some(HEALTHY));
        t.compute(k1 + 1, Some(HEALTHY));
        let st = t.statuses.get(k1).unwrap();
        assert_eq!(
            (st.kind, st.range, st.digest),
            (Kind::Held, Range::From(p), None)
        );
        t.advance();
        assert!(t.sink.writes.is_empty(), "a fill commits on evidence only");
        assert!(t.statuses.commit_through(k1, 0));
        assert!(t.statuses.commit_through(k1 + 1, 0));
        t.advance();
        assert_eq!(t.sink.writes[0], (p, ((k1 + 2) * B - p) as usize));
        t.sink.check();
    }

    /// A fenced block a withdrawal turns into a fill was not sent held
    /// yet: it loses its digest, and is held without one once F passes.
    #[test]
    fn a_fenced_block_turned_fill_carries_no_digest() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, DEGRADED);
        assert_eq!(t.statuses.get(K0).unwrap().digest, DEGRADED);
        t.withdraw();
        for k in [K0, K0 + 1] {
            let st = t.statuses.get(k).unwrap();
            assert_eq!((st.kind, st.digest), (Kind::Fenced, None), "block {k}");
        }
        t.ctx.fence = u64::MAX;
        t.advance();
        let st = t.statuses.get(K0).unwrap();
        assert_eq!((st.kind, st.digest), (Kind::Held, None));
        assert!(t.sink.writes.is_empty(), "a fill commits on evidence only");
    }

    /// F passing a withheld block and the fenced blocks after it at once:
    /// those follow the withdrawn rules first, so the one turned into a
    /// fill is held without a digest.
    #[test]
    fn blocks_after_a_withheld_one_become_fills_before_f_moves_them() {
        let p = (K0 * B + 1).next_multiple_of(PAGE);
        let k1 = p / B;
        let mut t = T::committing(false, K0);
        t.compute(K0, Some(HEALTHY));
        t.compute(K0 + 1, Some(HEALTHY));
        // The ring holds K0 + 2 ..= k1, 20 blocks, without overflowing.
        t.ctx.fence = 0;
        for k in K0 + 2..=k1 {
            t.compute(k, Some(HEALTHY));
        }
        assert_eq!(t.hold.len(), CAP);
        t.statuses.withhold(K0 + 2, Cause::HoldCap).unwrap();
        t.ctx.fence = u64::MAX;
        assert!(t.advance().withdraw);
        t.withdraw();
        t.advance();
        assert_eq!(t.kind(K0 + 3), Some(Kind::Withheld));
        let st = t.statuses.get(k1).unwrap();
        assert_eq!(
            (st.kind, st.range, st.digest),
            (Kind::Held, Range::From(p), None)
        );
        assert_eq!(t.sink.writes.len(), 2, "only K0 and K0 + 1 written");
    }

    /// A full ring withholds its oldest block, overflow, and the replica
    /// is withdrawn; the block is never written.
    #[test]
    fn a_full_ring_withholds_its_oldest_block() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        for k in K0..K0 + CAP as u64 {
            assert!(!t.compute(k, Some(HEALTHY)).withdraw);
        }
        assert_eq!(t.hold.len(), CAP);
        assert!(t.compute(K0 + CAP as u64, Some(HEALTHY)).withdraw);
        let st = t.statuses.get(K0).unwrap();
        assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::Overflow));
        assert_eq!(t.hold.overflows, 1);
        t.ctx.fence = u64::MAX;
        t.advance();
        assert!(!t.sink.wrote(K0));
    }

    /// A stalled RT thread never writes a block the supervisor gave
    /// `withheld`: before it computed the block, or while it was fenced.
    #[test]
    fn a_block_withheld_while_the_rt_thread_stalls_is_never_written() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = (K0 + 1) * B;
        t.compute(K0, Some(HEALTHY));
        // The RT thread stalls past both deadlines: K0 + 1 fenced, K0 + 2
        // not computed yet.
        t.compute(K0 + 1, Some(HEALTHY));
        t.statuses.withhold(K0 + 1, Cause::FenceDeadline).unwrap();
        t.statuses.withhold(K0 + 2, Cause::FenceDeadline).unwrap();
        t.ctx.fence = u64::MAX;
        assert!(t.advance().withdraw);
        assert!(t.compute(K0 + 2, Some(HEALTHY)).withdraw);
        assert_eq!(t.kind(K0 + 2), Some(Kind::Withheld));
        assert!(t.sink.wrote(K0));
        assert!(!t.sink.wrote(K0 + 1) && !t.sink.wrote(K0 + 2));
        t.sink.check();
    }

    /// A block withheld ahead whose slot still holds an open block keeps
    /// that block's status; computed, it is withheld for want of a slot
    /// and never written. A later block withheld ahead keeps its slot from
    /// the earlier block that shares it.
    #[test]
    fn a_slot_is_never_taken_from_an_open_block() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, Some(HEALTHY));
        let ahead = K0 + 64;
        // Nothing recorded: the supervisor learns it withheld nothing.
        assert_eq!(t.statuses.withhold(ahead, Cause::FenceDeadline), None);
        assert_eq!(t.kind(K0), Some(Kind::Fenced));
        t.ctx.fence = u64::MAX;
        for k in K0 + 1..ahead {
            t.compute(k, Some(HEALTHY));
        }
        // K0 left the ring when F passed it, so `ahead` takes its slot.
        t.compute(ahead, Some(HEALTHY));
        assert_eq!(t.kind(ahead), Some(Kind::Committed));
        // Withheld ahead of an open block, then computed: not written.
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, Some(HEALTHY));
        assert_eq!(t.statuses.withhold(K0 + 64, Cause::FenceDeadline), None);
        let st = t.statuses.get(K0 + 64);
        assert_eq!(st, None, "the open block's slot was taken");
        let data = T::block(K0 + 64);
        let mut changes = Changes {
            ring: &mut t.tx,
            dropped: &t.dropped,
            output: 0,
        };
        let ctx = Ctx {
            fence: u64::MAX,
            rank: 0,
        };
        let out = t.hold.compute(
            K0 + 64,
            &data,
            Some(HEALTHY),
            ctx,
            &t.statuses,
            &mut changes,
            &mut t.sink,
        );
        assert!(out.withdraw);
        assert!(!t.sink.wrote(K0 + 64));
        assert!(t.sent().iter().any(|c| matches!(c,
            StatusChange::Block { status, .. } if status.k == K0 + 64 && status.cause == Cause::NoSlot)));
        // A later block's status set ahead keeps its slot.
        let s = Statuses::new(64, B);
        assert!(s.withhold(K0 + 64, Cause::FenceDeadline).is_some());
        assert_eq!(s.withhold(K0, Cause::HoldCap), None);
        assert_eq!(s.get(K0 + 64).unwrap().cause, Cause::FenceDeadline);
    }

    /// The supervisor withholds a block only from no status, `fenced` or
    /// `held`: once committed it stays committed.
    #[test]
    fn a_committed_block_cannot_be_withheld() {
        let mut t = T::committing(false, K0);
        t.compute(K0, Some(HEALTHY));
        let st = t.statuses.withhold(K0, Cause::OtherDigest).unwrap();
        assert_eq!(st.kind, Kind::Committed);
        assert!(!t.statuses.commit_through(K0, 0));
    }

    /// A commit start inside a block: the block reports `committed from
    /// P` and writes `[P, (k + 1)B)` only; blocks before it give no
    /// status.
    #[test]
    fn a_commit_start_splits_its_block() {
        let p = (K0 * B + 1).next_multiple_of(PAGE);
        let k1 = p / B;
        let mut t = T::committing(false, K0 + 1);
        for k in K0..k1 {
            t.compute(k, Some(HEALTHY));
            assert_eq!(t.kind(k), None);
        }
        t.compute(k1, Some(HEALTHY));
        let st = t.statuses.get(k1).unwrap();
        assert_eq!((st.kind, st.range), (Kind::Committed, Range::From(p)));
        t.compute(k1 + 1, Some(HEALTHY));
        assert_eq!(t.sink.writes[0], (p, ((k1 + 1) * B - p) as usize));
        t.sink.check();
    }

    /// A stop at P inside a block: it reports `committed up to P` and
    /// writes `[kB, P)`; the output is withdrawn from P.
    #[test]
    fn a_stop_splits_its_block_and_withdraws() {
        let mut t = T::committing(false, K0);
        let p = (K0 + 2) * B + 10;
        t.compute(K0, Some(HEALTHY));
        t.hold.stop_at(p);
        t.compute(K0 + 1, Some(HEALTHY));
        t.compute(K0 + 2, Some(HEALTHY));
        let st = t.statuses.get(K0 + 2).unwrap();
        assert_eq!((st.kind, st.range), (Kind::Committed, Range::UpTo(p)));
        assert_eq!(t.hold.mode(), Mode::Withdrawn);
        assert_eq!(t.sink.writes.last(), Some(&((K0 + 2) * B, 10)));
        t.compute(K0 + 3, Some(HEALTHY));
        assert_eq!(t.kind(K0 + 3), Some(Kind::Withheld));
        t.sink.check();
    }

    /// One replica commits every block at once, degraded ones too, and
    /// holds nothing; a block it withholds withdraws nothing, so the next
    /// block commits at once again.
    #[test]
    fn one_replica_commits_at_once() {
        let mut t = T::committing(true, K0);
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, Some(HEALTHY));
        assert_eq!(t.kind(K0), Some(Kind::Committed));
        assert_eq!(t.statuses.get(K0).unwrap().digest, DEGRADED);
        assert!(t.hold.is_empty());
        assert_eq!(t.sink.writes.len(), 2);
        t.ctx.fence = 0;
        assert!(t.compute(K0 + 2, Some(HEALTHY)).withdraw);
        assert_eq!(t.kind(K0 + 2), Some(Kind::Withheld));
        t.withdraw();
        assert_eq!(t.hold.mode(), Mode::Committing);
        t.ctx.fence = u64::MAX;
        t.compute(K0 + 3, Some(HEALTHY));
        assert_eq!(t.kind(K0 + 3), Some(Kind::Committed));
        assert!(!t.sink.wrote(K0 + 2) && t.sink.wrote(K0 + 3));
    }

    /// An idle output gives no status and writes nothing.
    #[test]
    fn an_idle_output_gives_no_status() {
        let mut t = T::new(false);
        t.compute(K0, Some(HEALTHY));
        assert_eq!(t.kind(K0), None);
        assert!(t.sink.writes.is_empty());
        assert!(t.sent().is_empty());
    }

    /// Every move is sent as a change, in order.
    #[test]
    fn every_move_is_sent() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, DEGRADED);
        t.ctx.fence = u64::MAX;
        t.advance();
        t.statuses.commit_through(K0, 0);
        t.advance();
        let kinds: Vec<_> = t
            .sent()
            .iter()
            .map(|c| match c {
                StatusChange::Block { status, .. } => status.kind,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(kinds, [Kind::Fenced, Kind::Held, Kind::Committed]);
        let _ = t.changes();
    }

    /// A committed range the member flow refuses is reported unwritten,
    /// at once and from the ring alike.
    #[test]
    fn a_refused_write_is_reported() {
        let mut t = T::committing(false, K0);
        t.sink.refuse = true;
        t.compute(K0, Some(HEALTHY));
        t.ctx.fence = 0;
        t.compute(K0 + 1, Some(HEALTHY));
        t.ctx.fence = u64::MAX;
        t.advance();
        assert_eq!(t.kind(K0 + 1), Some(Kind::Committed));
        let unwritten: Vec<_> = t
            .sent()
            .into_iter()
            .filter_map(|c| match c {
                StatusChange::Unwritten { from, to, .. } => Some((from, to)),
                _ => None,
            })
            .collect();
        assert_eq!(
            unwritten,
            [(K0 * B, (K0 + 1) * B), ((K0 + 1) * B, (K0 + 2) * B)]
        );
        assert_eq!(t.hold.failed_writes, 2);
    }

    /// A full ring's overflow withholds its oldest block, which
    /// withdraws the replica; the block computed in the same call must
    /// follow the withdrawn rules, never commit at once.
    #[test]
    fn a_block_after_an_overflow_does_not_commit_at_once() {
        let mut t = T::committing(false, K0);
        t.compute(K0, DEGRADED);
        for k in K0 + 1..K0 + CAP as u64 {
            t.compute(k, Some(HEALTHY));
            assert!(t.statuses.commit_through(k, 0), "block {k} not held");
        }
        t.advance();
        for k in K0 + 1..K0 + CAP as u64 {
            assert_eq!(t.kind(k), Some(Kind::Committed), "block {k}");
        }
        assert_eq!(t.hold.len(), CAP);
        let k = K0 + CAP as u64;
        assert!(t.compute(k, Some(HEALTHY)).withdraw);
        assert_eq!(t.statuses.get(K0).unwrap().cause, Cause::Overflow);
        assert_ne!(
            t.kind(k),
            Some(Kind::Committed),
            "the block after the overflowed one committed at once"
        );
    }

    /// A stop at P (key hand-over) ends committing at P: the healthy
    /// blocks before P that wait fenced in the ring still commit once F
    /// passes them, as does `[kB, P)`.
    #[test]
    fn blocks_before_a_stop_commit_once_f_passes() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, Some(HEALTHY));
        t.compute(K0 + 1, Some(HEALTHY));
        let p = (K0 + 2) * B + 10;
        t.hold.stop_at(p);
        t.compute(K0 + 2, Some(HEALTHY));
        t.ctx.fence = u64::MAX;
        t.advance();
        for k in [K0, K0 + 1, K0 + 2] {
            assert_eq!(
                t.kind(k),
                Some(Kind::Committed),
                "block {k} before the stop"
            );
        }
    }

    /// A commit start whose block is skipped (an overrun or a clock step
    /// withholds it, which withdraws the replica) does not move on to the
    /// next page boundary by itself: the Named rule waits for a later
    /// Baseline, Resume for a new reference.
    #[test]
    fn a_skipped_commit_start_is_not_moved_to_the_next_page() {
        // P = 9001 * 1024 lies in block K0 + 21, the next page in K0 + 42.
        let mut t = T::committing(false, K0 + 1);
        for k in K0 + 1..K0 + 21 {
            t.compute(k, Some(HEALTHY));
        }
        // K0 + 21 skipped: withheld at the skip, the replica withdrawn.
        t.withdraw();
        for k in K0 + 22..K0 + 44 {
            t.compute(k, Some(HEALTHY));
            assert_ne!(
                t.kind(k),
                Some(Kind::Committed),
                "block {k} committed at once after the skipped commit start"
            );
        }
    }

    /// A commit start named while the output already commits is not
    /// latent: a later withdrawal is not undone by it at the next page
    /// boundary. K0 + 1, withheld before it is computed, leaves no block
    /// to fill from, so only a commit start could commit K0 + 21.
    #[test]
    fn a_withdrawal_is_not_undone_by_an_earlier_commit_start() {
        let mut t = T::committing(false, K0);
        t.compute(K0, Some(HEALTHY));
        t.hold.commit_from(K0 + 1);
        t.statuses.withhold(K0 + 1, Cause::HoldCap).unwrap();
        assert!(t.compute(K0 + 1, Some(HEALTHY)).withdraw);
        t.withdraw();
        assert_eq!(t.hold.mode(), Mode::Withdrawn);
        for k in K0 + 2..K0 + 23 {
            t.compute(k, Some(HEALTHY));
            assert_ne!(
                t.kind(k),
                Some(Kind::Committed),
                "block {k} committed at once while withdrawn"
            );
        }
    }

    /// What `withhold` returns is what the block ends with: it reports
    /// nothing for a block whose slot another block's open status holds,
    /// and that block may then be written.
    #[test]
    fn a_block_withhold_reported_withheld_is_never_written() {
        let mut t = T::committing(false, K0);
        t.ctx.fence = 0;
        t.compute(K0, Some(HEALTHY));
        let st = t.statuses.withhold(K0 + 64, Cause::FenceDeadline);
        t.ctx.fence = u64::MAX;
        for k in K0 + 1..=K0 + 64 {
            t.compute(k, Some(HEALTHY));
        }
        // The slot still held K0's open status: nothing was recorded, and
        // the supervisor was told so.
        assert_eq!(st, None, "withhold() reported a status it did not record");
        assert!(t.sink.wrote(K0 + 64));
    }

    /// A block whose status is `committed` is written: idling an output
    /// (writer replaced) does not drop committed blocks unwritten.
    #[test]
    fn idle_does_not_drop_committed_blocks_unwritten() {
        let mut t = T::committing(false, K0);
        t.compute(K0, DEGRADED);
        t.compute(K0 + 1, Some(HEALTHY));
        assert!(t.statuses.commit_through(K0 + 1, 0));
        t.advance();
        assert_eq!(t.kind(K0 + 1), Some(Kind::Committed));
        let statuses = &t.statuses;
        let mut changes = Changes {
            ring: &mut t.tx,
            dropped: &t.dropped,
            output: 0,
        };
        t.hold
            .idle(statuses, &mut changes, &mut t.sink, Cause::Released);
        assert!(
            t.sink.wrote(K0 + 1),
            "block {} reported committed, never written",
            K0 + 1
        );
    }

    /// A withdrawn replica withholds at once only a block with no page
    /// boundary whose previous block is final without a commit: the block
    /// right after one this replica committed can be filled, the page
    /// staying whole on this member.
    #[test]
    fn a_block_after_a_committed_one_can_be_filled() {
        let mut t = T::committing(false, K0);
        t.compute(K0, Some(HEALTHY));
        assert_eq!(t.kind(K0), Some(Kind::Committed));
        t.withdraw();
        t.compute(K0 + 1, Some(HEALTHY));
        let st = t.statuses.get(K0 + 1).unwrap();
        assert_ne!(
            (st.kind, st.cause),
            (Kind::Withheld, Cause::Unfillable),
            "the block after a committed one withheld as unfillable"
        );
    }
}
