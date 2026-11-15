//! Instruction-count gates on the RT output commit, run under Callgrind
//! by Gungraun (`cargo bench --bench rt`) against the real libmxl
//! (`SAMPLEZ_TEST_LIBMXL`, default `/opt/mxl/lib/libmxl.so`). The counts
//! include libmxl's own code. One block of B = 48 samples on 8 channels
//! is committed right after the previous one, and after a gap of
//! `bufferLength - B` samples, which libmxl zero-fills in the commit. The
//! hard limits are the instruction counts measured in the builder image
//! plus 10 %, rounded up; a run above a limit fails.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use samplez_mxl::Instance;
use samplez_mxl::output::{self, Creation, FlowWriter, OutputConfig, Write};

const B: u64 = 48;
const CHANNELS: usize = 8;
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
    let lib = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let api = samplez_mxl::load(Path::new(&lib)).unwrap();
    let dir = std::env::temp_dir().join(format!("samplez-mxl-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let instance = Instance::new(&api, &dir).unwrap();
    let cfg = OutputConfig {
        flow_id: "7fbec3b1-1b0f-417d-9059-8b94a47197ed",
        label: "bench",
        group: "samplez-bench",
        role: "Audio",
        channels: CHANNELS as u32,
        block: B,
        batch: B,
    };
    let Ok(Creation::Created { mut writer, .. }) = output::create(&instance, &cfg) else {
        panic!("output not created");
    };
    let data = vec![0.25f32; CHANNELS * B as usize];
    assert_eq!(writer.write_block(K0, &data), Write::Committed);
    let out = Output {
        writer,
        _instance: instance,
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

main!(library_benchmark_groups = commit);
