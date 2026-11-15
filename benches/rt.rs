//! Instruction-count gates on the hold ring of a replicated output, run
//! under Callgrind by Gungraun (`cargo bench --bench rt`): 8 channels,
//! B = 48 and the default ring of `D + w + 2B` = 20 blocks, with a sink
//! that writes nothing, so the counts are the ring's own. The hard
//! limits are the instruction counts measured in the builder image plus
//! 10 %, rounded up; a run above a limit fails.

use std::hint::black_box;
use std::sync::atomic::AtomicU64;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez::blocks::{Changes, Ctx, HEALTHY, Hold, Sink, StatusChange, Statuses};

const B: u64 = 48;
const CHANNELS: usize = 8;
const CAP: usize = 20;
/// A block whose start is a page boundary.
const K0: u64 = 192_000;

fn limit(ir: u64) -> LibraryBenchmarkConfig {
    let mut c = LibraryBenchmarkConfig::default();
    c.tool(Callgrind::default().hard_limits([(EventKind::Ir, ir)]));
    c
}

struct Null;

impl Sink for Null {
    fn write(&mut self, _: u64, _: usize, data: &[f32], _: usize) -> bool {
        black_box(data);
        true
    }
}

/// A committing output, its statuses and change ring, and one block of
/// data.
struct Out {
    hold: Hold,
    statuses: Statuses,
    tx: rtrb::Producer<StatusChange>,
    _rx: rtrb::Consumer<StatusChange>,
    dropped: AtomicU64,
    data: Vec<f32>,
}

impl Out {
    fn new() -> Out {
        let (tx, rx) = rtrb::RingBuffer::new(4096);
        let mut hold = Hold::new(CHANNELS, B, CAP, false);
        hold.commit_from(K0);
        Out {
            hold,
            statuses: Statuses::new(128, B),
            tx,
            _rx: rx,
            dropped: AtomicU64::new(0),
            data: vec![0.25; CHANNELS * B as usize],
        }
    }

    fn compute(&mut self, k: u64, fence: u64) {
        let mut changes = Changes {
            ring: &mut self.tx,
            dropped: &self.dropped,
            output: 0,
        };
        let ctx = Ctx { fence, rank: 0 };
        self.hold.compute(
            k,
            &self.data,
            Some(HEALTHY),
            ctx,
            &self.statuses,
            &mut changes,
            &mut Null,
        );
    }

    fn advance(&mut self, fence: u64) {
        let mut changes = Changes {
            ring: &mut self.tx,
            dropped: &self.dropped,
            output: 0,
        };
        let ctx = Ctx { fence, rank: 0 };
        self.hold
            .advance(ctx, &self.statuses, &mut changes, &mut Null);
    }

    /// Statuses set and the ring full of fenced blocks.
    fn full() -> Out {
        let mut o = Out::new();
        for k in K0..K0 + CAP as u64 {
            o.compute(k, 0);
        }
        assert_eq!(o.hold.len(), CAP);
        o
    }
}

// A healthy block, F past it, nothing held: committed and written at
// once.
#[library_benchmark]
#[bench::at_once(setup = Out::new, config = limit(AT_ONCE))]
fn commit_at_once(mut o: Out) -> Out {
    o.compute(black_box(K0), u64::MAX);
    o
}

// A full ring of fenced blocks once F passes them: each moved to
// committed, all written in one range.
#[library_benchmark]
#[bench::flush(setup = Out::full, config = limit(FLUSH))]
fn flush_full_ring(mut o: Out) -> Out {
    o.advance(black_box(u64::MAX));
    o
}

// A block computed into a full ring under a frozen F: the oldest block
// withheld for overflow, the block held.
#[library_benchmark]
#[bench::overflow(setup = Out::full, config = limit(OVERFLOW))]
fn overflow(mut o: Out) -> Out {
    o.compute(black_box(K0 + CAP as u64), 0);
    o
}

// The replica withdrawn with a full ring: every block a withdrawn replica
// cannot fill withheld, the one holding a page boundary turned into a
// fill.
#[library_benchmark]
#[bench::withdraw(setup = Out::full, config = limit(WITHDRAW))]
fn withdraw(mut o: Out) -> Out {
    let mut changes = Changes {
        ring: &mut o.tx,
        dropped: &o.dropped,
        output: 0,
    };
    o.hold.withdraw(black_box(&o.statuses), &mut changes);
    o
}

const AT_ONCE: u64 = 437;
const FLUSH: u64 = 18_154;
const OVERFLOW: u64 = 12_012;
const WITHDRAW: u64 = 8_096;

library_benchmark_group!(
    name = hold,
    benchmarks = [commit_at_once, flush_full_ring, overflow, withdraw]
);

main!(library_benchmark_groups = hold);
