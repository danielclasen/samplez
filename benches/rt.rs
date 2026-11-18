//! Instruction-count gates on the hold ring and the page hashes of a
//! replicated output, run under Callgrind by Gungraun (`cargo bench
//! --bench rt`): 8 channels, B = 48 and the default ring of `D + w + 2B`
//! = 20 blocks, with a sink that writes nothing, so the counts are the
//! ring's and the hashes' own; and on the strip calls the RT loop makes
//! for 64 stereo input slots: a block through strips at rest, as every
//! slot runs one, through strips with every unit on and ramping, the
//! dynamics order crossfading, and through the same strips without gate
//! and compressor, the order flipped at once; the most strip entries one
//! wake applies, and a skip.
//! And on the sets a log entry's batch makes at its index when it holds
//! every coefficient of a populated, ramping matrix: of the largest
//! matrix a configuration of B = 48 takes and of the largest any takes,
//! at B = 1024 (`COEFFICIENTS_PER_BLOCK_SAMPLE`), set within a quarter of
//! the block. And on the fill of a product core between two blocks of the
//! RT thread: every coefficient of the same two matrices, populated and
//! ramping, replaced by a fade-in from silence (`Matrix::fade_in`), within
//! the block it has. And on the entries a product's state change derives
//! (`Targets::diff`), a recall changing every coefficient of the same two
//! shapes, which a replica computes as each log entry applies, and a
//! fader move on 64 stereo slots into 56 columns with every strip
//! parameter unchanged.
//! The hard limits are the instruction counts measured in the builder
//! image plus 10 %, rounded up; a run above a limit fails.

use std::hint::black_box;
use std::sync::atomic::AtomicU64;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez::blocks::{
    Changes, Ctx, HEALTHY, Hold, PageHash, Pages, Ranks, Sink, StatusChange, Statuses, fingerprint,
};
use samplez_dsp::strip::{CompParams, DEFAULT_BANDS, EqBand, GateParams, Strip, StripParam};

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
        let ctx = Ctx {
            fence,
            ranks: Ranks::flat(0),
        };
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
        let ctx = Ctx {
            fence,
            ranks: Ranks::flat(0),
        };
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
    o.hold.withdraw(black_box(&o.statuses), &mut changes, true);
    o
}

// A backward clock step with a full ring: every block withheld at once,
// none written.
#[library_benchmark]
#[bench::backward(setup = Out::full, config = limit(BACKWARD))]
fn backward_step(mut o: Out) -> Out {
    let mut changes = Changes {
        ring: &mut o.tx,
        dropped: &o.dropped,
        output: 0,
    };
    o.hold.withhold_open(
        black_box(&o.statuses),
        &mut changes,
        &mut Null,
        samplez::blocks::Cause::Skip,
    );
    o
}

/// An output's page hashes, its statuses and its hash ring.
struct Hashed {
    out: Out,
    pages: Pages,
    tx: rtrb::Producer<PageHash>,
    _rx: rtrb::Consumer<PageHash>,
}

impl Hashed {
    /// The page starting at K0 hashed up to block `k`, each block
    /// committed at once.
    fn upto(k: u64) -> Hashed {
        let (tx, rx) = rtrb::RingBuffer::new(64);
        let mut h = Hashed {
            out: Out::new(),
            pages: Pages::new(0, CHANNELS, B, CAP),
            tx,
            _rx: rx,
        };
        for k in K0..k {
            h.out.compute(k, u64::MAX);
            h.hash(k);
        }
        h
    }

    fn hash(&mut self, k: u64) {
        self.pages
            .hash(k, &self.out.data, Some(HEALTHY), &self.out.dropped);
    }

    fn mid_page() -> Hashed {
        Hashed::upto(K0 + 1)
    }

    /// Every block of the page but the last, which straddles the next.
    fn page_end() -> Hashed {
        Hashed::upto(K0 + 21)
    }

    /// The page hashed whole and every block of it committed.
    fn settled() -> Hashed {
        Hashed::upto(K0 + 22)
    }
}

// One block hashed inside a page.
#[library_benchmark]
#[bench::block(setup = Hashed::mid_page, config = limit(HASH_BLOCK))]
fn hash_block(mut h: Hashed) -> Hashed {
    h.hash(black_box(K0 + 1));
    h
}

// The block ending a page and starting the next: the page's hash taken
// and its record put to wait.
#[library_benchmark]
#[bench::end(setup = Hashed::page_end, config = limit(HASH_PAGE_END))]
fn hash_page_end(mut h: Hashed) -> Hashed {
    h.hash(black_box(K0 + 21));
    h
}

// A waiting page whose 22 blocks are all committed: each block's status
// read, the record sent.
#[library_benchmark]
#[bench::settle(setup = Hashed::settled, config = limit(SETTLE_PAGE))]
fn settle_page(mut h: Hashed) -> Hashed {
    h.pages
        .settle(black_box(&h.out.statuses), &mut h.tx, &h.out.dropped);
    h
}

/// The most entries one page's fingerprint covers at B = 48: 64 pending
/// entries applied at each of the 22 blocks starting in a page.
const PAGE_ENTRIES: u64 = 64 * 22;

/// A full page's entries out of order, as `(log id, at_index)`.
fn page_entries() -> Vec<(u64, u64)> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..PAGE_ENTRIES)
        .map(|n| {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (x >> 40, K0 * B + n / 64 * B)
        })
        .collect()
}

// The fingerprint of a page holding the most entries it can: sorted and
// hashed.
#[library_benchmark]
#[bench::full(setup = page_entries, config = limit(FINGERPRINT_FULL))]
fn fingerprint_full(mut entries: Vec<(u64, u64)>) -> u64 {
    fingerprint(black_box(&mut entries))
}

const BACKWARD: u64 = 8_300;
const AT_ONCE: u64 = 437;
const FLUSH: u64 = 18_154;
const OVERFLOW: u64 = 12_012;
const WITHDRAW: u64 = 8_096;
const HASH_BLOCK: u64 = 8_274;
const HASH_PAGE_END: u64 = 8_511;
const SETTLE_PAGE: u64 = 2_090;
const FINGERPRINT_FULL: u64 = 501_769;

/// Input slots of the strip benchmarks, each of two legs.
const SLOTS: usize = 64;
/// Entries one wake takes off the entry ring at most: `PENDING_CHANGES`.
const ENTRIES: usize = 64;

/// The strips of 64 stereo slots, at rest, or with trim, high-pass and
/// every band on and ramping and the dynamics order flipped, with gate
/// and compressor on (`dynamics`) or off; and one block of each slot's
/// samples, above both dynamics thresholds.
fn slot_strips(on: bool, dynamics: bool) -> (Vec<Strip>, Vec<f32>) {
    let mut strips = vec![Strip::new(2, 480); SLOTS];
    if on {
        for s in &mut strips {
            let mut params = vec![
                StripParam::Trim { db: 6.0 },
                StripParam::Hpf {
                    enabled: true,
                    freq: 80.0,
                },
                StripParam::Eq { enabled: true },
            ];
            if dynamics {
                params.push(StripParam::Gate(GateParams {
                    enabled: true,
                    threshold_db: -60.0,
                    range_db: 40.0,
                    attack_ms: 1.0,
                    release_ms: 50.0,
                }));
                params.push(StripParam::Comp(CompParams {
                    enabled: true,
                    threshold_db: -20.0,
                    ratio: 4.0,
                    attack_ms: 1.0,
                    release_ms: 50.0,
                    knee_db: 6.0,
                    makeup_db: 3.0,
                }));
            }
            params.push(StripParam::DynamicsBeforeEq { enabled: true });
            for (band, b) in DEFAULT_BANDS.iter().enumerate() {
                let value = EqBand { gain_db: 6.0, ..*b };
                params.push(StripParam::EqBand { band, value });
            }
            for p in params {
                assert!(s.set(p));
            }
        }
    }
    let x = (0..2 * B as usize)
        .map(|n| 0.5 * (n as f32 * 0.37).sin())
        .collect();
    (strips, x)
}

fn strips_at_rest() -> (Vec<Strip>, Vec<f32>) {
    slot_strips(false, false)
}

fn strips_on() -> (Vec<Strip>, Vec<f32>) {
    slot_strips(true, true)
}

fn strips_without_dynamics() -> (Vec<Strip>, Vec<f32>) {
    slot_strips(true, false)
}

// One block through the strips of 64 slots, each copied into the slot's
// buffer as the loop does: at rest, passed through; with every unit on
// and ramping, the costliest block; and with gate and compressor off,
// whose order flips without a crossfade.
#[library_benchmark]
#[bench::at_rest(setup = strips_at_rest, config = limit(STRIPS_AT_REST))]
#[bench::ramping(setup = strips_on, config = limit(STRIPS_RAMPING))]
#[bench::no_dynamics(setup = strips_without_dynamics, config = limit(STRIPS_NO_DYNAMICS))]
fn strip_block((mut strips, x): (Vec<Strip>, Vec<f32>)) -> Vec<Strip> {
    let mut buf = [0.0f32; 2 * B as usize];
    for s in &mut strips {
        buf.copy_from_slice(&x);
        s.process(black_box(&mut buf), B as usize);
    }
    strips
}

// The most entries one wake applies, each an equaliser band, whose
// coefficients are the costliest to compute.
#[library_benchmark]
#[bench::bands(setup = strips_on, config = limit(STRIP_ENTRIES))]
fn strip_entries((mut strips, _): (Vec<Strip>, Vec<f32>)) -> Vec<Strip> {
    for n in 0..ENTRIES {
        let value = EqBand {
            gain_db: -3.0,
            ..DEFAULT_BANDS[n % 4]
        };
        let param = StripParam::EqBand { band: n % 4, value };
        assert!(strips[n % SLOTS].set(black_box(param)));
    }
    strips
}

// The strips of 64 slots, every unit on and ramping, advanced over a
// skip of two blocks.
#[library_benchmark]
#[bench::ramping(setup = strips_on, config = limit(STRIP_SKIP))]
fn strip_skip((mut strips, _): (Vec<Strip>, Vec<f32>)) -> Vec<Strip> {
    for s in &mut strips {
        s.advance(black_box(2 * B as usize));
    }
    strips
}

const STRIPS_AT_REST: u64 = 51_995;
const STRIPS_RAMPING: u64 = 4_907_976;
const STRIPS_NO_DYNAMICS: u64 = 1_986_024;
const STRIP_ENTRIES: u64 = 48_961;
const STRIP_SKIP: u64 = 13_005;

library_benchmark_group!(
    name = strips,
    benchmarks = [strip_block, strip_entries, strip_skip]
);

library_benchmark_group!(
    name = pages,
    benchmarks = [hash_block, hash_page_end, settle_page, fingerprint_full]
);

library_benchmark_group!(
    name = hold,
    benchmarks = [
        commit_at_once,
        flush_full_ring,
        overflow,
        withdraw,
        backward_step
    ]
);

/// A matrix of `inputs` by `outputs` with every coefficient set and
/// ramping, and the batch of a recall setting every coefficient anew, in
/// the order a batch sets them.
fn recall(inputs: usize, outputs: usize) -> (samplez_dsp::Matrix, Vec<(usize, usize, f32)>) {
    let mut matrix = samplez_dsp::Matrix::new(inputs, outputs);
    for i in 0..inputs {
        for o in 0..outputs {
            assert!(matrix.set(i, o, 0.5, 480));
        }
    }
    let sets = (0..inputs)
        .flat_map(|i| (0..outputs).map(move |o| (i, o, ((i + o) % 7) as f32 / 7.0 + 0.1)))
        .collect();
    (matrix, sets)
}

/// The largest matrix a configuration of B = 48 takes: 80 by 90, 48 x
/// 150 coefficients.
fn recall_at_block_48() -> (samplez_dsp::Matrix, Vec<(usize, usize, f32)>) {
    recall(80, 90)
}

/// The largest matrix any configuration takes: 384 by 400, 1024 x 150
/// coefficients.
fn recall_at_block_1024() -> (samplez_dsp::Matrix, Vec<(usize, usize, f32)>) {
    recall(384, 400)
}

#[library_benchmark]
#[bench::block_48(setup = recall_at_block_48, config = limit(RECALL_BLOCK_48))]
#[bench::block_1024(setup = recall_at_block_1024, config = limit(RECALL_BLOCK_1024))]
fn batch_recall(
    (mut matrix, sets): (samplez_dsp::Matrix, Vec<(usize, usize, f32)>),
) -> samplez_dsp::Matrix {
    for &(i, o, g) in &sets {
        black_box(matrix.set(i, o, g, 480));
    }
    matrix
}

/// A populated, ramping matrix of `inputs` by `outputs` and a product
/// state's gain for every coefficient.
fn fill(inputs: usize, outputs: usize) -> (samplez_dsp::Matrix, Vec<f32>) {
    let (matrix, sets) = recall(inputs, outputs);
    (matrix, sets.into_iter().map(|(_, _, g)| g).collect())
}

fn fill_at_block_48() -> (samplez_dsp::Matrix, Vec<f32>) {
    fill(80, 90)
}

fn fill_at_block_1024() -> (samplez_dsp::Matrix, Vec<f32>) {
    fill(384, 400)
}

#[library_benchmark]
#[bench::block_48(setup = fill_at_block_48, config = limit(FILL_BLOCK_48))]
#[bench::block_1024(setup = fill_at_block_1024, config = limit(FILL_BLOCK_1024))]
fn product_fill((mut matrix, gains): (samplez_dsp::Matrix, Vec<f32>)) -> samplez_dsp::Matrix {
    black_box(matrix.fade_in(&gains, 480));
    matrix
}

/// The targets before and after a recall that changes every coefficient
/// of `inputs` by `outputs`.
fn targets(inputs: usize, outputs: usize) -> (samplez::Targets, samplez::Targets) {
    let of = |g: fn(usize, usize) -> f32| samplez::Targets {
        gains: (0..inputs)
            .map(|i| (0..outputs).map(|o| g(i, o)).collect())
            .collect(),
        flows: Vec::new(),
        strips: Vec::new(),
    };
    (of(|_, _| 0.5), of(|i, o| ((i + o) % 7) as f32 / 7.0 + 0.1))
}

fn targets_at_block_48() -> (samplez::Targets, samplez::Targets) {
    targets(80, 90)
}

fn targets_at_block_1024() -> (samplez::Targets, samplez::Targets) {
    targets(384, 400)
}

/// One stereo slot's strip parameters, every one a strip takes, in key
/// order: 14 of them.
fn stereo_strip(slot: usize) -> Vec<StripParam> {
    let db = slot as f32 / 8.0 - 4.0;
    let mut p = vec![StripParam::Trim { db }];
    p.extend((0..2).map(|leg| StripParam::LegTrim { leg, db: -db }));
    p.extend((0..2).map(|leg| StripParam::Polarity {
        leg,
        inverted: leg == 1,
    }));
    p.push(StripParam::Hpf {
        enabled: true,
        freq: 80.0,
    });
    p.push(StripParam::Eq { enabled: true });
    p.extend(DEFAULT_BANDS.iter().enumerate().map(|(band, b)| {
        let value = EqBand { gain_db: db, ..*b };
        StripParam::EqBand { band, value }
    }));
    p.push(StripParam::Gate(GateParams {
        enabled: true,
        threshold_db: -60.0,
        range_db: 40.0,
        attack_ms: 1.0,
        release_ms: 50.0,
    }));
    p.push(StripParam::Comp(CompParams {
        enabled: true,
        threshold_db: -20.0,
        ratio: 4.0,
        attack_ms: 1.0,
        release_ms: 50.0,
        knee_db: 6.0,
        makeup_db: 3.0,
    }));
    p.push(StripParam::DynamicsBeforeEq { enabled: false });
    p
}

/// A fader move on a desk of 64 stereo slots into 56 columns: 128 by 56
/// coefficients and 64 strips of 14 parameters, the strips unchanged and
/// one slot's two rows of coefficients moved.
fn targets_at_stereo_64x56() -> (samplez::Targets, samplez::Targets) {
    let from = samplez::Targets {
        gains: (0..128)
            .map(|i| (0..56).map(|o| ((i + o) % 7) as f32 / 7.0).collect())
            .collect(),
        flows: (0..64).map(|s| format!("flow-{s}")).collect(),
        strips: (0..64).map(stereo_strip).collect(),
    };
    let mut to = from.clone();
    for row in &mut to.gains[10..12] {
        for g in row {
            *g *= 0.5;
        }
    }
    (from, to)
}

#[library_benchmark]
#[bench::block_48(setup = targets_at_block_48, config = limit(DIFF_BLOCK_48))]
#[bench::block_1024(setup = targets_at_block_1024, config = limit(DIFF_BLOCK_1024))]
#[bench::stereo_64x56(setup = targets_at_stereo_64x56, config = limit(DIFF_STEREO_64X56))]
fn targets_diff((from, to): (samplez::Targets, samplez::Targets)) -> Vec<samplez::Entry> {
    black_box(from.diff(&to))
}

const DIFF_BLOCK_48: u64 = 397_843;
const DIFF_BLOCK_1024: u64 = 4_030_452;
const DIFF_STEREO_64X56: u64 = 374_284;

const FILL_BLOCK_48: u64 = 451_896;
const FILL_BLOCK_1024: u64 = 9_631_507;

const RECALL_BLOCK_48: u64 = 1_291_097;
const RECALL_BLOCK_1024: u64 = 31_257_737;

library_benchmark_group!(
    name = batches,
    benchmarks = [batch_recall, product_fill, targets_diff]
);

main!(library_benchmark_groups = hold, pages, strips, batches);
