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
                ranks: Ranks::flat(0),
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

    /// Computes block `k` and commits it through on evidence, so no
    /// fill overflows the ring.
    fn evidenced(&mut self, k: u64, digest: Option<u64>) {
        self.compute(k, digest);
        self.statuses.commit_through(k, 0, None);
        self.advance();
    }

    /// Withdrawn for a cause.
    fn withdraw(&mut self) {
        self.withdraw_for(true);
    }

    fn withdraw_for(&mut self, cause: bool) {
        let statuses = &self.statuses;
        let mut changes = Changes {
            ring: &mut self.tx,
            dropped: &self.dropped,
            output: 0,
        };
        self.hold.withdraw(statuses, &mut changes, cause);
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
    t.ctx.ranks = Ranks::flat(3);
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
    assert!(t.statuses.commit_through(K0, 0b10, None));
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
    assert!(t.statuses.commit_through(K0 + 1, 0, None));
    t.advance();
    assert_eq!(t.kind(K0 + 1), Some(Kind::Committed));
    assert!(t.sink.writes.is_empty(), "written before K0");
    assert!(t.statuses.commit_through(K0, 0, None));
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
    // Computed withdrawn, no page boundary in it: withheld at once,
    // keeping the degradation it was computed with.
    t.compute(K0 + 2, DEGRADED);
    let st = t.statuses.get(K0 + 2).unwrap();
    assert_eq!(
        (st.kind, st.cause, st.digest),
        (Kind::Withheld, Cause::Unfillable, DEGRADED)
    );
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
    assert!(t.statuses.commit_through(k1, 0, None));
    assert!(t.statuses.commit_through(k1 + 1, 0, None));
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
        ranks: Ranks::flat(0),
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
    assert!(!t.statuses.commit_through(K0, 0, None));
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

/// A stop at the start of a block, right after a block it committed:
/// the output is withdrawn from there and fills on contiguously, so a
/// stop that falls on a block boundary leaves no block it cannot fill.
/// A stop at a block boundary it reaches only later, after a block
/// it never computed, still leaves the blocks to the next page
/// boundary unfillable.
#[test]
fn a_stop_at_a_block_boundary_fills_on_from_there() {
    let mut t = T::committing(false, K0);
    let p = (K0 + 1) * B;
    t.compute(K0, Some(HEALTHY));
    t.hold.stop_at(p);
    t.compute(K0 + 1, Some(HEALTHY));
    assert_eq!(t.hold.mode(), Mode::Withdrawn);
    assert_eq!(t.statuses.standing(), Standing::Withdrawn { since: K0 + 1 });
    let st = t.statuses.get(K0 + 1).unwrap();
    assert_eq!(st.range, Range::Whole);
    assert_ne!(st.kind, Kind::Withheld, "a fill, not unfillable");
    assert_eq!(st.digest, None, "a fill carries no digest");
    assert_eq!(t.sink.writes.last(), Some(&(K0 * B, B as usize)));

    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.hold.stop_at((K0 + 2) * B);
    t.compute(K0 + 2, Some(HEALTHY));
    if !((K0 + 2) * B).is_multiple_of(PAGE) {
        assert_eq!(t.kind(K0 + 2), Some(Kind::Withheld));
    }
}

/// A commit start requested while the output fills on contiguously
/// after a stop at a block boundary takes in the first block that
/// starts on a page boundary: taken inside a block, it would end the
/// fill below the commit start and leave that part of the page to no
/// member. Every sample from the stop on is written once.
#[test]
fn a_contiguous_fill_takes_a_commit_start_at_a_page_aligned_block() {
    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.hold.stop_at((K0 + 1) * B);
    t.compute(K0 + 1, Some(HEALTHY));
    assert!(t.statuses.commit_through(K0 + 1, 0, None));
    t.hold.commit_from(K0 + 2);
    let mut k = K0 + 2;
    loop {
        t.compute(k, Some(HEALTHY));
        if t.hold.mode() == Mode::Committing {
            break;
        }
        let st = t.statuses.get(k).unwrap();
        assert_eq!(st.range, Range::Whole, "block {k} filled in part");
        assert!(t.statuses.commit_through(k, 0, None));
        k += 1;
        assert!(k < K0 + 2 * PAGE, "no commit start");
    }
    assert!((k * B).is_multiple_of(PAGE));
    assert_eq!(t.statuses.standing(), Standing::Committing { from: k * B });
    for j in k + 1..k + CAP as u64 + 2 {
        t.compute(j, Some(HEALTHY));
    }
    let mut end = K0 * B;
    for &(s, n) in &t.sink.writes {
        assert_eq!(s, end, "a gap or an overlap at {s}");
        end = s + n as u64;
    }
    assert!(end > k * B, "nothing written past the commit start");
}

/// After a backward clock step every open block of the ring is
/// withheld at once and never written, one marked for commit through
/// included.
#[test]
fn open_blocks_are_withheld_at_a_backward_step() {
    let mut t = T::committing(false, K0);
    t.compute(K0, DEGRADED);
    t.ctx.fence = 0;
    t.compute(K0 + 1, Some(HEALTHY));
    t.compute(K0 + 2, Some(HEALTHY));
    assert!(t.statuses.commit_through(K0, 0, None));
    let statuses = &t.statuses;
    let mut changes = Changes {
        ring: &mut t.tx,
        dropped: &t.dropped,
        output: 0,
    };
    t.hold
        .withhold_open(statuses, &mut changes, &mut t.sink, Cause::Skip);
    for k in [K0, K0 + 1, K0 + 2] {
        let st = t.statuses.get(k).unwrap();
        assert_eq!(
            (st.kind, st.cause),
            (Kind::Withheld, Cause::Skip),
            "block {k}"
        );
    }
    assert!(t.hold.is_empty());
    assert!(!t.sink.wrote(K0 + 1) && !t.sink.wrote(K0 + 2));
    // K0, marked for commit through but not committed when the step
    // came, is withheld too.
    assert_eq!(t.kind(K0), Some(Kind::Withheld));
    assert!(t.sink.writes.is_empty());
}

/// One replica commits every block at once, degraded ones too, and
/// holds nothing; a block it withholds (fenced by a clock step)
/// withdraws nothing, so the next block commits at once again.
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
    let st = t.statuses.get(K0 + 2).unwrap();
    assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::ClockStep));
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
    t.statuses.commit_through(K0, 0, None);
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
        assert!(t.statuses.commit_through(k, 0, None), "block {k} not held");
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
    assert!(t.statuses.commit_through(K0 + 1, 0, None));
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

/// One output's page hashes over [`T`]'s two channels, and their
/// ring.
struct H {
    pages: Pages,
    tx: Producer<PageHash>,
    rx: rtrb::Consumer<PageHash>,
    dropped: AtomicU64,
}

impl H {
    fn new() -> H {
        let (tx, rx) = rtrb::RingBuffer::new(64);
        H {
            pages: Pages::new(3, 2, B, CAP),
            tx,
            rx,
            dropped: AtomicU64::new(0),
        }
    }

    fn hash(&mut self, k: u64) {
        self.pages.hash(k, &T::block(k), Some(FP), &self.dropped);
    }

    /// The records sent by a settle.
    fn settle(&mut self, statuses: &Statuses) -> Vec<PageHash> {
        self.pages.settle(statuses, &mut self.tx, &self.dropped);
        let mut v = Vec::new();
        while let Ok(h) = self.rx.pop() {
            v.push(h);
        }
        v
    }
}

const FP: u64 = 0x1234;
/// The page starting at block K0, the next one, and their last
/// blocks: K0 + 21 holds the boundary between them at P1.
const P0: u64 = K0 * B / PAGE;
const P1: u64 = (P0 + 1) * PAGE;
const LAST0: u64 = K0 + 21;
const LAST1: u64 = K0 + 42;

/// The hash covers the page's samples in index order with the
/// channels of one index interleaved, each f32's bit pattern little
/// endian; a page whose blocks have no status (computed while the
/// output committed nothing) is sent, not committed.
#[test]
fn a_page_hash_is_xxh3_over_its_samples_interleaved() {
    let mut h = H::new();
    let statuses = Statuses::new(64, B);
    for k in K0..LAST0 {
        h.hash(k);
    }
    assert_eq!(
        h.settle(&statuses),
        [],
        "a page sent before it was hashed whole"
    );
    h.hash(LAST0);
    let mut bytes = Vec::new();
    for i in P0 * PAGE..P1 {
        bytes.extend((i as f32).to_bits().to_le_bytes());
        bytes.extend((-(i as f32)).to_bits().to_le_bytes());
    }
    let want = PageHash {
        output: 3,
        page: P0,
        hash: xxhash_rust::xxh3::xxh3_64(&bytes),
        fingerprint: FP,
        committed: false,
    };
    assert_eq!(h.settle(&statuses), [want]);
}

/// A page is sent once every block overlapping it is final: not while
/// one is fenced, and committed once all are committed.
#[test]
fn a_page_hash_waits_for_every_block_to_be_final() {
    let mut t = T::committing(false, K0);
    let mut h = H::new();
    for k in K0..=LAST0 {
        if k == K0 + 16 {
            t.ctx.fence = (K0 + 16) * B;
        }
        t.compute(k, Some(HEALTHY));
        h.hash(k);
    }
    assert_eq!(t.kind(LAST0), Some(Kind::Fenced));
    assert_eq!(h.settle(&t.statuses), [], "sent with fenced blocks");
    t.ctx.fence = u64::MAX;
    t.advance();
    let got = h.settle(&t.statuses);
    assert_eq!(got.len(), 1);
    assert!(got[0].committed && got[0].page == P0, "{got:?}");
    assert_eq!(h.settle(&t.statuses), [], "sent twice");
}

/// A withheld block leaves its page not committed.
#[test]
fn a_page_with_a_withheld_block_is_not_committed() {
    let mut t = T::committing(false, K0);
    let mut h = H::new();
    for k in K0..=LAST0 {
        if k == K0 + 16 {
            t.ctx.fence = (K0 + 16) * B;
        }
        t.compute(k, Some(HEALTHY));
        h.hash(k);
    }
    t.statuses.withhold(LAST0, Cause::HoldCap).unwrap();
    t.ctx.fence = u64::MAX;
    t.advance();
    assert_eq!(t.kind(LAST0 - 1), Some(Kind::Committed));
    let got = h.settle(&t.statuses);
    assert_eq!(got.len(), 1);
    assert!(!got[0].committed && got[0].page == P0, "{got:?}");
}

/// A commit start inside a block commits the page from it whole, and
/// the page before it in part: not committed.
#[test]
fn a_page_is_committed_from_a_commit_start_on() {
    let mut t = T::committing(false, K0 + 1);
    let mut h = H::new();
    for k in K0..=LAST1 {
        t.compute(k, Some(HEALTHY));
        h.hash(k);
    }
    assert_eq!(t.statuses.get(LAST0).unwrap().range, Range::From(P1));
    let got = h.settle(&t.statuses);
    let pages: Vec<_> = got.iter().map(|g| (g.page, g.committed)).collect();
    assert_eq!(pages, [(P0, false), (P0 + 1, true)]);
}

/// A stop inside a page leaves it not committed.
#[test]
fn a_page_with_a_stop_is_not_committed() {
    let mut t = T::committing(false, K0);
    t.hold.stop_at(P1 + PAGE - 18);
    let mut h = H::new();
    for k in K0..=LAST1 {
        t.compute(k, Some(HEALTHY));
        h.hash(k);
    }
    let st = t.statuses.get(LAST1).unwrap();
    assert_eq!(
        (st.kind, st.range),
        (Kind::Committed, Range::UpTo(P1 + PAGE - 18))
    );
    let got = h.settle(&t.statuses);
    let pages: Vec<_> = got.iter().map(|g| (g.page, g.committed)).collect();
    assert_eq!(pages, [(P0, true), (P0 + 1, false)]);
}

/// A page missing a block gets no record; the next page whole does.
#[test]
fn a_page_not_computed_whole_gets_no_record() {
    let mut h = H::new();
    let statuses = Statuses::new(64, B);
    for k in (K0..=LAST1).filter(|&k| k != K0 + 5) {
        h.hash(k);
    }
    let pages: Vec<_> = h.settle(&statuses).iter().map(|g| g.page).collect();
    assert_eq!(pages, [P0 + 1]);
}

/// A page whose fingerprint is unknown gets no record and is counted.
#[test]
fn a_page_without_a_fingerprint_is_counted() {
    let mut h = H::new();
    let statuses = Statuses::new(64, B);
    for k in K0..=LAST0 {
        let fp = (k != LAST0).then_some(FP);
        h.pages.hash(k, &T::block(k), fp, &h.dropped);
    }
    assert_eq!(h.settle(&statuses), []);
    assert_eq!(h.dropped.load(Relaxed), 1);
}

/// The fingerprint is XXH3-64 over the (log id, at_index) pairs in log
/// id order, whatever order they come in.
#[test]
fn the_fingerprint_is_xxh3_over_the_sorted_entries() {
    let mut bytes = Vec::new();
    for n in [2u64, 300, 5, 100] {
        bytes.extend(n.to_le_bytes());
    }
    let want = xxhash_rust::xxh3::xxh3_64(&bytes);
    assert_eq!(fingerprint(&mut [(5, 100), (2, 300)]), want);
    assert_eq!(fingerprint(&mut [(2, 300), (5, 100)]), want);
    assert_eq!(fingerprint(&mut []), HEALTHY);
}

/// Ranks hold per block: one up to `from`, the next from it on, and a
/// withdrawn replica's fills take the fill rank, so a rank change the
/// replicated state knows ahead lands on the block it belongs to.
#[test]
fn blocks_take_the_rank_of_their_index_and_fills_the_fill_rank() {
    let mut t = T::committing(false, K0);
    t.ctx.ranks = Ranks {
        rank: 1,
        next: 2,
        from: K0 + 1,
        fill: 9,
    };
    t.compute(K0, DEGRADED);
    t.compute(K0 + 1, DEGRADED);
    assert_eq!(t.statuses.get(K0).unwrap().rank, 1);
    assert_eq!(t.statuses.get(K0 + 1).unwrap().rank, 2);
    // Fenced, then passed by F: the rank of its index.
    t.ctx.fence = 0;
    t.compute(K0 + 2, DEGRADED);
    t.ctx.fence = u64::MAX;
    t.advance();
    assert_eq!(t.statuses.get(K0 + 2).unwrap().rank, 2);
    t.withdraw();
    // The next page boundary: a fill.
    let p = (K0 * B).next_multiple_of(PAGE) + PAGE;
    let k = p / B;
    t.compute(k, DEGRADED);
    let fill = t.statuses.get(k).unwrap();
    assert_eq!((fill.kind, fill.digest, fill.rank), (Kind::Held, None, 9));
    assert_eq!(Ranks::unpack(t.ctx.ranks.pack()), t.ctx.ranks);
}

/// A held block committed through up to P writes `[kB, P)` alone and
/// reports `committed up to P`; a withdrawal after the mark keeps it,
/// and the next block is no fill continuing from it.
#[test]
fn a_block_committed_through_up_to_p_writes_only_below_p() {
    let p = K0 * B + PAGE;
    let k = p / B;
    assert_ne!(p, k * B, "P inside the block");
    let mut t = T::committing(false, K0);
    for j in K0..k {
        t.compute(j, Some(HEALTHY));
    }
    t.sink.writes.clear();
    t.compute(k, DEGRADED);
    assert!(
        !t.statuses.commit_through(k, 0, Some(k * B)),
        "nothing left"
    );
    assert!(t.statuses.commit_through(k, 0, Some(p)));
    t.withdraw();
    t.advance();
    let st = t.statuses.get(k).unwrap();
    assert_eq!((st.kind, st.range), (Kind::Committed, Range::UpTo(p)));
    assert_eq!(t.sink.writes, [(k * B, (p - k * B) as usize)]);
    t.compute(k + 1, DEGRADED);
    assert_eq!(t.kind(k + 1), Some(Kind::Withheld), "no page boundary");
    t.sink.check();
}

/// A commit through up to P withholds `[P, (k + 1)B)` for a cause:
/// the same pass withdraws the replica before the healthy blocks held
/// behind the block commit, whenever the supervisor's own withdrawal
/// arrives.
#[test]
fn a_commit_through_up_to_p_withdraws_before_the_blocks_behind_it() {
    let p = K0 * B + PAGE;
    let k = p / B;
    let mut t = T::committing(false, K0);
    for j in K0..k {
        t.compute(j, Some(HEALTHY));
    }
    t.sink.writes.clear();
    t.compute(k, DEGRADED);
    t.compute(k + 1, Some(HEALTHY));
    t.compute(k + 2, Some(HEALTHY));
    assert!(t.statuses.commit_through(k, 0, Some(p)));
    assert!(t.advance().withdraw);
    assert_eq!(t.sink.writes, [(k * B, (p - k * B) as usize)]);
    for j in [k + 1, k + 2] {
        assert_eq!(t.kind(j), Some(Kind::Held), "block {j}");
    }
    // A block that stops at P on its own, held and committed through
    // over its whole range, withdraws nothing.
    let mut t = T::committing(false, K0);
    for j in K0..k {
        t.compute(j, Some(HEALTHY));
    }
    t.hold.stop_at(p);
    t.compute(k, DEGRADED);
    assert!(t.statuses.commit_through(k, 0, None));
    assert!(!t.advance().withdraw);
    t.sink.check();
}

/// The output's standing follows its commit state: idle until its
/// commit start, committing from P, withdrawn from the block after
/// the last one computed.
#[test]
fn the_standing_follows_the_commit_state() {
    let mut t = T::new(false);
    assert_eq!(t.statuses.standing(), Standing::Idle { since: 0 });
    t.hold.commit_from(K0);
    t.compute(K0, Some(HEALTHY));
    assert_eq!(t.statuses.standing(), Standing::Committing { from: K0 * B });
    t.compute(K0 + 1, Some(HEALTHY));
    t.withdraw();
    assert_eq!(t.statuses.standing(), Standing::Withdrawn { since: K0 + 2 });
    // Withdrawn while the block is decided: from that block on.
    let mut t = T::committing(false, K0);
    t.ctx.fence = 0;
    for k in K0..K0 + CAP as u64 {
        t.compute(k, Some(HEALTHY));
    }
    t.compute(K0 + CAP as u64, Some(HEALTHY));
    assert_eq!(
        t.statuses.standing(),
        Standing::Withdrawn {
            since: K0 + CAP as u64
        }
    );
}

/// A stop named while the replica is withdrawn lapses on its next
/// withdrawal: the commit start named after it takes, and the output
/// commits from it rather than stopping in the same block.
#[test]
fn a_stop_named_while_withdrawn_lapses_before_the_next_commit_start() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.withdraw();
    t.hold.stop_at(K0 * B + 1);
    let fill = (K0 * B).next_multiple_of(PAGE) / B + 1;
    for j in K0 + 1..=fill {
        t.compute(j, DEGRADED);
    }
    assert_eq!(
        t.statuses
            .withhold(fill, Cause::OtherDigest)
            .map(|s| s.kind),
        Some(Kind::Withheld)
    );
    t.advance();
    t.withdraw();
    t.hold.commit_from(kp);
    for j in fill + 1..=kp {
        t.compute(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Committing { from: p });
    let st = t.statuses.get(kp).unwrap();
    assert_eq!((st.kind, st.range), (Kind::Committed, Range::From(p)));
}

/// A status holds a rank up to [`MAX_RANK`] beside a split offset up
/// to `B - 1` at the largest block size: the demoted classes of seven
/// replicas fit, and neither field spills into the other.
#[test]
fn a_status_holds_a_rank_above_15_beside_the_largest_split() {
    let block = 1024;
    let statuses = Statuses::new(64, block);
    let k = 5_000;
    for (rank, range) in [
        (20, Range::From(k * block + 1023)),
        (MAX_RANK, Range::UpTo(k * block + 1)),
        (16, Range::Whole),
    ] {
        let s = BlockStatus {
            k,
            kind: Kind::Held,
            range,
            through: true,
            rank,
            cause: Cause::HoldCap,
            on_silence_of: 0b101,
            digest: Some(9),
        };
        statuses.slot(k).word.store(0, Relaxed);
        assert!(statuses.first(&s).is_ok());
        assert_eq!(statuses.get(k), Some(s));
    }
    let ranks = Ranks {
        rank: 20,
        next: MAX_RANK,
        from: 1 << 40,
        fill: 17,
    };
    assert_eq!(Ranks::unpack(ranks.pack()), ranks);
}

/// A cancelled commit start never takes, whether the output is idle
/// or withdrawn; one requested after it does, and a cancel after one
/// took changes nothing.
#[test]
fn a_cancelled_commit_start_never_takes() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::new(false);
    t.hold.commit_from(K0);
    t.hold.cancel_commit_start();
    for j in K0..=kp {
        t.compute(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Idle { since: 0 });
    assert!(t.sink.writes.is_empty());

    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.withdraw();
    t.hold.commit_from(kp);
    t.hold.cancel_commit_start();
    for j in K0 + 1..=kp + 1 {
        t.evidenced(j, DEGRADED);
    }
    assert!(matches!(t.statuses.standing(), Standing::Withdrawn { .. }));
    t.hold.commit_from(kp + 2);
    let next = ((kp + 2) * B).next_multiple_of(PAGE);
    for j in kp + 2..=next / B {
        t.evidenced(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Committing { from: next });
    t.hold.cancel_commit_start();
    t.compute(next / B + 1, Some(HEALTHY));
    assert_eq!(t.statuses.standing(), Standing::Committing { from: next });
    t.sink.check();
}

/// A withdrawn replica keeps a requested commit start when one of its
/// fills is withheld: it commits from that page boundary, not from a
/// later one named again after it.
#[test]
fn a_withheld_fill_keeps_a_requested_commit_start() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.withdraw();
    t.hold.commit_from(kp);
    // A fill withheld on another replica's report withdraws again.
    let fill = (K0 * B).next_multiple_of(PAGE) / B + 1;
    for j in K0 + 1..=fill {
        t.compute(j, DEGRADED);
    }
    assert_eq!(
        t.statuses
            .withhold(fill, Cause::OtherDigest)
            .map(|s| s.kind),
        Some(Kind::Withheld)
    );
    let out = t.advance();
    assert_eq!((out.withdraw, out.cause), (true, false));
    t.withdraw_for(out.cause);
    for j in fill + 1..=kp {
        t.compute(j, Some(HEALTHY));
    }
    let st = t.statuses.get(kp).unwrap();
    assert_eq!(st.range, Range::From(p), "{st:?}");
    assert_eq!(t.statuses.standing(), Standing::Committing { from: p });
}

/// A withdrawn replica drops a requested commit start when a block is
/// withheld for a cause before it takes: the start was decided on
/// statuses that did not have that block.
#[test]
fn a_block_withheld_for_a_cause_drops_a_requested_commit_start() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.withdraw();
    t.hold.commit_from(kp);
    // Skipped: withheld at the skip, before the commit start takes.
    assert_eq!(
        t.statuses.withhold(K0 + 1, Cause::Skip).map(|s| s.kind),
        Some(Kind::Withheld)
    );
    t.withdraw();
    for j in K0 + 2..=kp + 1 {
        t.compute(j, Some(HEALTHY));
    }
    assert!(
        matches!(t.statuses.standing(), Standing::Withdrawn { .. }),
        "{:?}",
        t.statuses.standing()
    );
}

/// A commit start decided on statuses before a withdrawal for a cause
/// is ignored, one decided after it takes.
#[test]
fn a_commit_start_decided_before_a_withdrawal_for_a_cause_is_ignored() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::committing(false, K0);
    for j in K0..kp - 2 {
        t.compute(j, Some(HEALTHY));
    }
    // Withdrawn for a cause while block kp - 2 is decided.
    t.withdraw();
    t.hold.commit_from_seen(kp, kp - 3);
    for j in kp - 2..=kp {
        t.evidenced(j, Some(HEALTHY));
    }
    assert!(matches!(t.statuses.standing(), Standing::Withdrawn { .. }));
    t.hold.commit_from_seen(kp + 1, kp);
    let next = ((kp + 1) * B).next_multiple_of(PAGE);
    for j in kp + 1..=next / B {
        t.evidenced(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Committing { from: next });
    t.sink.check();
}

/// A commit start requested again after the output took one is
/// ignored: a stop then leaves the output withdrawn.
#[test]
fn a_commit_start_requested_while_committing_is_ignored() {
    let mut t = T::committing(false, K0);
    t.compute(K0, Some(HEALTHY));
    t.hold.commit_from(K0);
    t.hold.stop_at((K0 + 1) * B);
    let next = ((K0 + 1) * B).next_multiple_of(PAGE) / B;
    for j in K0 + 1..=next {
        t.compute(j, Some(HEALTHY));
    }
    assert!(
        matches!(t.statuses.standing(), Standing::Withdrawn { .. }),
        "{:?}",
        t.statuses.standing()
    );
}

/// The RT thread takes the latest commit start request once: a cancel
/// replaces a start not yet taken, and a start taken is not taken
/// again.
#[test]
fn the_latest_commit_start_request_wins() {
    let p = K0 * B + PAGE;
    let kp = p / B;
    let mut t = T::new(false);
    t.statuses.request_start(K0, K0);
    t.statuses.request_cancel();
    t.hold.take_request(&t.statuses);
    for j in K0..=kp {
        t.compute(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Idle { since: 0 });
    t.statuses.request_cancel();
    t.statuses.request_start(kp + 1, kp);
    t.hold.take_request(&t.statuses);
    t.hold.cancel_commit_start();
    // Taken already: the same request does not come back.
    t.hold.take_request(&t.statuses);
    let next = ((kp + 1) * B).next_multiple_of(PAGE) / B;
    for j in kp + 1..=next {
        t.compute(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Idle { since: 0 });
    t.statuses.request_start(next + 1, next);
    t.hold.take_request(&t.statuses);
    let at = ((next + 1) * B).next_multiple_of(PAGE);
    for j in next + 1..=at / B {
        t.compute(j, Some(HEALTHY));
    }
    assert_eq!(t.statuses.standing(), Standing::Committing { from: at });
}
