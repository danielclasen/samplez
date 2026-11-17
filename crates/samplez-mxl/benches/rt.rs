//! Instruction-count gates on the RT output commit and input read, run
//! under Callgrind by Gungraun (`cargo bench --bench rt`) against the real
//! libmxl (`SAMPLEZ_TEST_LIBMXL`, default `/opt/mxl/lib/libmxl.so`). The
//! counts include libmxl's own code. One block of B = 48 samples on 8
//! channels is committed right after the previous one, and after a gap of
//! `bufferLength - B` samples, which libmxl zero-fills in the commit. One
//! block is read from a 16-channel flow into 2 selected legs, and into all
//! 16 legs, which bounds what the selection saves. The
//! hard limits are the instruction counts measured in the builder image
//! plus 10 %, rounded up; a run above a limit fails.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez_mxl::input::{Generation, GroupWait, InputConfig, Outcome, SlotState, Timing};
use samplez_mxl::output::{self, Creation, FlowWriter, OutputConfig, Write};
use samplez_mxl::{Instance, MxlApi};

const B: u64 = 48;
const CHANNELS: usize = 8;
const FLOW: &str = "7fbec3b1-1b0f-417d-9059-8b94a47197ed";
/// Far from 0, so that a gap below it does not reach index 0.
const K0: u64 = 4_000_000;

fn limit(ir: u64) -> LibraryBenchmarkConfig {
    let mut c = LibraryBenchmarkConfig::default();
    c.tool(Callgrind::default().hard_limits([(EventKind::Ir, ir)]));
    c
}

/// A writer on a fresh flow in a fresh domain, removed on drop.
struct Output {
    writer: FlowWriter,
    _instance: Arc<Instance>,
    api: Arc<MxlApi>,
    dir: PathBuf,
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A writer whose last commit ended at `(K0 + 1)B`, and one block of
/// data.
fn setup_writer() -> (Output, Vec<f32>) {
    writer(CHANNELS, "samplez-mxl-bench")
}

fn writer(channels: usize, dir: &str) -> (Output, Vec<f32>) {
    let lib = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let api = samplez_mxl::load(Path::new(&lib)).unwrap();
    let dir = std::env::temp_dir().join(format!("{dir}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let instance = Instance::new(&api, &dir).unwrap();
    let cfg = OutputConfig {
        flow_id: FLOW,
        label: "bench",
        group: "samplez-bench",
        role: "Audio",
        channels: channels as u32,
        block: B,
        batch: B,
    };
    let Ok(Creation::Created { mut writer, .. }) = output::create(&instance, &cfg) else {
        panic!("output not created");
    };
    let data = vec![0.25f32; channels * B as usize];
    assert_eq!(writer.write_block(K0, &data), Write::Committed);
    let out = Output {
        writer,
        _instance: instance,
        api,
        dir,
    };
    (out, data)
}

// The block after the last commit.
#[library_benchmark]
#[bench::next(setup = setup_writer, config = limit(COMMIT_NEXT))]
fn commit_next((mut out, data): (Output, Vec<f32>)) -> (Output, Vec<f32>) {
    assert_eq!(
        out.writer.write_block(black_box(K0 + 1), &data),
        Write::Committed
    );
    (out, data)
}

// A block `bufferLength - B` samples after the last commit: libmxl
// zero-fills the gap.
#[library_benchmark]
#[bench::gap(setup = setup_writer, config = limit(COMMIT_GAP))]
fn commit_gap((mut out, data): (Output, Vec<f32>)) -> (Output, Vec<f32>) {
    let k = K0 + out.writer.buffer_length() / B;
    assert_eq!(
        out.writer.write_block(black_box(k), &data),
        Write::Committed
    );
    (out, data)
}

const COMMIT_NEXT: u64 = 1_509;
const COMMIT_GAP: u64 = 685_000;

library_benchmark_group!(name = commit, benchmarks = [commit_next, commit_gap]);

const WIDE: usize = 16;

/// A 16-channel flow written up to `(K0 + 1)B`, an input on it reading
/// `legs`, and a buffer for its block.
struct Read {
    generation: Generation,
    state: SlotState,
    dst: Vec<f32>,
    _flow: Output,
}

fn setup_read(legs: &[u32]) -> Read {
    let (flow, _) = writer(WIDE, &format!("samplez-mxl-bench-read{}", legs.len()));
    let mut generation = Generation::new(&flow.api, &flow.dir, 1).unwrap();
    let cfg = InputConfig {
        flow_id: FLOW,
        channels: legs.len() as u32,
        flow_channels: Some(legs),
        offset: 0,
    };
    let timing = Timing {
        block: B,
        margin: 0,
        max_catchup_blocks: 0,
        resync_budget: 0,
    };
    generation.attach(0, &cfg, &timing).unwrap();
    Read {
        generation,
        state: SlotState::default(),
        dst: vec![0.0; legs.len() * B as usize],
        _flow: flow,
    }
}

fn two_of_sixteen() -> Read {
    setup_read(&[2, 3])
}

fn sixteen_of_sixteen() -> Read {
    setup_read(&(0..WIDE as u32).collect::<Vec<_>>())
}

// Block K0 of the 16-channel flow, the head already past it.
#[library_benchmark]
#[bench::two_legs(setup = two_of_sixteen, config = limit(READ_TWO_OF_16))]
#[bench::all_legs(setup = sixteen_of_sixteen, config = limit(READ_16_OF_16))]
fn read(mut r: Read) -> Read {
    let input = r.generation.input(0).unwrap();
    assert_eq!(
        input.read(&mut r.state, black_box(K0), &GroupWait::Ok, &mut r.dst),
        Outcome::Copy
    );
    r
}

const READ_TWO_OF_16: u64 = 779;
const READ_16_OF_16: u64 = 1_726;

library_benchmark_group!(name = input, benchmarks = [read]);

main!(library_benchmark_groups = commit, input);
