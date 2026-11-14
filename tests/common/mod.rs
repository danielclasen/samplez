//! Test producer and checker for the engine tests, against the real
//! libmxl loaded from `SAMPLEZ_TEST_LIBMXL` (default
//! `/opt/mxl/lib/libmxl.so`). A missing library fails the tests.
//!
//! [`Pilot`] writes an index-coded pilot, `x[idx] = sin(2 pi f idx / fs)`
//! with a frequency per channel. [`Checker`] reads an output flow and
//! checks every block the engine's verdict stream names: a clean block is
//! exactly `g * x[idx]`, a ramp block follows the fade and gain ramps
//! sample by sample, a silenced block is exact +0.0.

#![allow(dead_code)]

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use samplez::{BlockVerdict, Config, Engine};
use samplez_mxl::input::{Generation, GroupWait, InputConfig, Outcome, SlotState, Timing};
use samplez_mxl::output::{self, Creation, OutputConfig};
use samplez_mxl::{Instance, MxlApi, time};

pub const B: u64 = 48;
/// The test margin, 20 ms, generous for shared CI runners.
pub const MARGIN: u64 = 960;
pub const RAMP: u32 = 480;

pub fn libmxl() -> PathBuf {
    let path = PathBuf::from(
        std::env::var("SAMPLEZ_TEST_LIBMXL").unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".into()),
    );
    assert!(path.is_file(), "libmxl not found at {}", path.display());
    path
}

pub fn api() -> Arc<MxlApi> {
    samplez_mxl::load(&libmxl()).unwrap()
}

/// A fresh MXL domain directory, removed on drop, on `/dev/shm` when it
/// exists. On a disk-backed file system a write into a flow's shared
/// mapping can wait for the page's writeback; the RT loop then stalls for
/// tens of milliseconds and skips blocks.
pub struct Domain(pub PathBuf);

impl Domain {
    pub fn new() -> Domain {
        static N: AtomicU64 = AtomicU64::new(0);
        let shm = Path::new("/dev/shm");
        let base = if shm.is_dir() {
            shm.to_owned()
        } else {
            std::env::temp_dir()
        };
        let dir = base.join(format!(
            "samplez-engine-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let d = Domain(dir);
        d.create();
        d
    }

    /// Creates the directory again after it was removed.
    pub fn create(&self) {
        std::fs::create_dir(&self.0).unwrap();
        // A 1 s history, ring of 2 s: room for the checker and the
        // margins to lag on a loaded runner. libmxl reads it per domain.
        std::fs::write(
            self.0.join("options.json"),
            r#"{"urn:x-mxl:option:history_duration/v1.0": 1000000000}"#,
        )
        .unwrap();
    }

    pub fn has_flow(&self, id: &str) -> bool {
        self.0.join(format!("{id}.mxl-flow")).exists()
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn new_id() -> String {
    let r = RandomState::new();
    let x = (u128::from(r.hash_one(1u8)) << 64) | u128::from(r.hash_one(2u8));
    let h = format!("{x:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// The pilot: channel `ch` is a sine of `1000 + 250 ch` Hz, exact per index.
pub fn pilot(ch: usize, idx: u64) -> f32 {
    let f = 1000 + 250 * ch as u64;
    let phase = (f * (idx % 48_000)) % 48_000;
    (2.0 * std::f64::consts::PI * phase as f64 / 48_000.0).sin() as f32
}

/// A pilot producer on its own instance: block `k` of `batch` samples is
/// committed once libmxl's clock passes index `(k + 1) batch + delay`.
/// Dropping it stops the thread and releases the writer, which deletes
/// the flow.
///
/// A new flow is zero-filled and its history reaches below the first
/// commit, so a reader started right after the producer reads zeros
/// there. [`Pilot::start`] returns 100 ms after the first commit, past
/// every margin the tests use.
pub struct Pilot {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Pilot {
    pub fn start(domain: &Path, id: &str, channels: u32, batch: u64, delay: u64) -> Pilot {
        let api = api();
        let instance = Instance::new(&api, domain).unwrap();
        let cfg = OutputConfig {
            flow_id: id,
            label: "pilot",
            group: "pilot",
            role: "Audio",
            channels,
            block: batch,
        };
        let Creation::Created { mut writer, .. } = output::create(&instance, &cfg).unwrap() else {
            panic!("pilot flow {id} not created");
        };
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let n = batch as usize;
            let mut data = vec![0.0f32; channels as usize * n];
            let mut next: Option<u64> = None;
            while !flag.load(Ordering::Relaxed) {
                let idx = time::timestamp_to_index(&api, time::now(&api));
                if let Some(due) = (idx.saturating_sub(delay) / batch).checked_sub(1) {
                    for k in next.unwrap_or(due)..=due {
                        for (ch, out) in data.chunks_exact_mut(n).enumerate() {
                            for (i, v) in out.iter_mut().enumerate() {
                                *v = pilot(ch, k * batch + i as u64);
                            }
                        }
                        writer.write_block(k, &data);
                    }
                    next = Some(due + 1);
                    let _ = first_tx.send(());
                }
                let k = next.unwrap_or(0);
                time::sleep_until(
                    &api,
                    time::index_to_timestamp(&api, (k + 1) * batch + delay),
                );
            }
            drop(writer);
            drop(instance);
        });
        first_rx
            .recv()
            .expect("the pilot thread ended before its first commit");
        std::thread::sleep(Duration::from_millis(100));
        Pilot {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Pilot {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}

/// A one-domain engine configuration: inputs and outputs as (flow id,
/// channels), `matrix` rows per input channel.
pub fn config(
    domain: &Path,
    inputs: &[(&str, u32)],
    outputs: &[(&str, u32)],
    matrix: Vec<Vec<f32>>,
    margin: u64,
) -> Config {
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": margin,
        "rampSamples": RAMP,
        "libmxl": libmxl(),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": inputs.iter().enumerate().map(|(i, (id, ch))| serde_json::json!({
            "name": format!("in{i}"), "domain": "d", "flowId": id, "channels": ch,
        })).collect::<Vec<_>>(),
        "outputs": outputs.iter().enumerate().map(|(i, (id, ch))| serde_json::json!({
            "name": format!("out{i}"), "domain": "d", "flowId": id, "label": format!("test out{i}"),
            "group": "samplez-test", "role": format!("Audio {i}"), "channels": ch,
        })).collect::<Vec<_>>(),
        "matrix": matrix,
        "rt": { "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

pub fn diagonal(channels: usize, g: f32) -> Vec<Vec<f32>> {
    (0..channels)
        .map(|i| {
            (0..channels)
                .map(|j| if i == j { g } else { 0.0 })
                .collect()
        })
        .collect()
}

/// Waits up to `timeout` for `f` to hold.
pub fn wait_for(timeout: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn ramp_value(c0: f32, target: f32, len: u32, pos: u64) -> f32 {
    if pos < u64::from(len) {
        c0 + pos as f32 * ((target - c0) / len as f32)
    } else {
        target
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Counts {
    pub clean: u64,
    pub ramp: u64,
    pub silenced: u64,
    /// Blocks the last hop skipped, checked zero-filled.
    pub skipped: u64,
    /// Verdicts whose block the engine did not write: before the output
    /// was realised.
    pub unread: u64,
}

/// The fade and gain state of one hop, from its verdicts. Fades and
/// coefficient ramps are functions of the sample index: a fade-in starts
/// at the first index of the first copied block after a silenced one, a
/// gain change at its `at_index`.
struct Stage {
    gain: f32,
    /// A pending coefficient ramp: from, to, `at_index`.
    gain_ramp: Option<(f32, f32, u64)>,
    fade_start: Option<u64>,
    prev_silenced: bool,
}

/// What one hop did to one block.
#[derive(Clone, Copy, Debug)]
struct Step {
    /// Silenced, or not processed by the hop.
    zero: bool,
    fade: Option<u64>,
    gain: f32,
    gain_ramp: Option<(f32, f32, u64)>,
}

impl Stage {
    /// `None`: the hop skipped the block; libmxl zero-filled its output.
    fn step(&mut self, k: u64, v: Option<&BlockVerdict>) -> Result<Step, String> {
        let start = k * B;
        let gain_ramp = match self.gain_ramp {
            Some((_, to, at)) if start >= at + u64::from(RAMP) => {
                self.gain = to;
                self.gain_ramp = None;
                None
            }
            Some(r) if start >= r.2 => Some(r),
            _ => None,
        };
        let Some(v) = v else {
            return Ok(Step {
                zero: true,
                fade: None,
                gain: self.gain,
                gain_ramp,
            });
        };
        if v.ramping != gain_ramp.is_some() {
            return Err(format!(
                "block {k}: ramping {}, expected {gain_ramp:?}",
                v.ramping
            ));
        }
        let silenced = v.silenced & 1 != 0;
        let fading = v.fading & 1 != 0;
        if !silenced && self.prev_silenced {
            self.fade_start = Some(start);
        }
        self.prev_silenced = silenced;
        let in_fade = self
            .fade_start
            .is_some_and(|s| !silenced && start - s < u64::from(RAMP));
        if fading != in_fade {
            return Err(format!("block {k}: fading {fading}, expected {in_fade}"));
        }
        Ok(Step {
            zero: silenced,
            fade: if fading { self.fade_start } else { None },
            gain: self.gain,
            gain_ramp,
        })
    }
}

impl Step {
    /// The hop's output sample at `idx` from its input sample `v`, in the
    /// engine's order: fade on the input, then `0 + g * x`.
    fn apply(&self, v: f32, idx: u64) -> f32 {
        if self.zero {
            return 0.0;
        }
        let f = self
            .fade
            .map_or(1.0, |s| ramp_value(0.0, 1.0, RAMP, idx - s));
        let g = match self.gain_ramp {
            Some((from, to, at)) => ramp_value(from, to, RAMP, idx - at),
            None => self.gain,
        };
        0.0 + g * (v * f)
    }
}

/// Checks an output flow block by block against the verdicts of the
/// hops that produced it, the pilot entering the first hop: each hop
/// applies its fade and gain in the engine's order, a silenced block is
/// exact +0.0.
pub struct Checker {
    generation: Option<Generation>,
    domain: PathBuf,
    flow_id: String,
    channels: usize,
    state: SlotState,
    buf: Vec<f32>,
    stages: Vec<Stage>,
    last_k: Option<u64>,
    read_any: bool,
    pub counts: Counts,
}

impl Checker {
    /// A checker for hops with these gains, upstream first.
    pub fn new(domain: &Path, flow_id: &str, channels: usize, gains: &[f32]) -> Checker {
        Checker {
            generation: None,
            domain: domain.to_owned(),
            flow_id: flow_id.to_owned(),
            channels,
            state: SlotState::default(),
            buf: vec![0.0; channels * B as usize],
            stages: gains
                .iter()
                .map(|&gain| Stage {
                    gain,
                    gain_ramp: None,
                    fade_start: None,
                    prev_silenced: true,
                })
                .collect(),
            last_k: None,
            read_any: false,
            counts: Counts::default(),
        }
    }

    /// The last hop's gain.
    pub fn gain(&self) -> f32 {
        self.stages.last().unwrap().gain
    }

    /// The last hop ramps its gain to `to` from `at_index` on.
    pub fn expect_gain_change(&mut self, to: f32, at_index: u64) {
        let s = self.stages.last_mut().unwrap();
        s.gain_ramp = Some((s.gain, to, at_index));
    }

    /// The last block checked.
    pub fn last_k(&self) -> Option<u64> {
        self.last_k
    }

    fn read(&mut self, k: u64) -> Option<&[f32]> {
        if self.generation.is_none() {
            let mut g = Generation::new(&api(), &self.domain, 1).unwrap();
            let cfg = InputConfig {
                flow_id: &self.flow_id,
                channels: self.channels as u32,
                offset: 0,
            };
            let timing = Timing {
                block: B,
                margin: 0,
                max_catchup_blocks: 0,
            };
            g.attach(0, &cfg, &timing).ok()?;
            self.generation = Some(g);
        }
        let input = self.generation.as_ref()?.input(0)?;
        match input.read(&mut self.state, k, &GroupWait::Skipped, &mut self.buf) {
            Outcome::Copy => Some(&self.buf),
            _ => None,
        }
    }

    /// Checks block `k` given each hop's verdict for it, upstream first;
    /// `None` for an upstream hop that skipped the block. Blocks the last
    /// hop skipped since the previous call are checked to be zero-filled.
    pub fn check(&mut self, vs: &[Option<BlockVerdict>]) -> Result<(), String> {
        let last = vs.last().unwrap().expect("the last hop's verdict");
        let k = last.k;
        if self.last_k.is_some_and(|l| k <= l) {
            return Err(format!("block {k} after block {:?}", self.last_k));
        }
        if self.read_any {
            for skipped in self.last_k.unwrap() + 1..k {
                self.counts.skipped += 1;
                let channels = self.channels;
                let Some(out) = self.read(skipped) else {
                    return Err(format!("skipped block {skipped} not readable"));
                };
                if out[..channels * B as usize]
                    .iter()
                    .any(|v| v.to_bits() != 0)
                {
                    return Err(format!("skipped block {skipped} is not zero-filled"));
                }
            }
        }
        self.last_k = Some(k);
        let mut steps = Vec::new();
        for (stage, v) in self.stages.iter_mut().zip(vs) {
            let mut step = stage.step(k, v.as_ref())?;
            // An upstream block not written (before its output was
            // realised) reads as libmxl's zero-fill downstream.
            step.zero |= v.is_some_and(|v| v.written & 1 == 0);
            steps.push(step);
        }
        if last.written & 1 == 0 {
            if self.read_any {
                return Err(format!("block {k}: not written after earlier blocks were"));
            }
            self.counts.unread += 1;
            return Ok(());
        }
        let channels = self.channels;
        let Some(out) = self.read(k) else {
            return Err(format!("block {k}: written but not readable"));
        };
        let b = B as usize;
        for ch in 0..channels {
            for n in 0..b {
                let idx = k * B + n as u64;
                let got = out[ch * b + n];
                let want = steps.iter().fold(pilot(ch, idx), |v, s| s.apply(v, idx));
                let ok = if want == 0.0 {
                    got.to_bits() == 0
                } else {
                    got == want
                };
                if !ok {
                    return Err(format!(
                        "block {k} ({vs:?}, {steps:?}) channel {ch} index {idx}: got {got:e}, \
                         want {want:e}"
                    ));
                }
            }
        }
        self.read_any = true;
        let all = vs.iter().map(|v| v.map(|v| v.verdict()));
        if all
            .clone()
            .any(|v| v.is_none_or(|v| v == samplez::Verdict::Silenced))
        {
            self.counts.silenced += 1;
        } else if all.clone().any(|v| v == Some(samplez::Verdict::Ramp)) {
            self.counts.ramp += 1;
        } else {
            self.counts.clean += 1;
        }
        Ok(())
    }

    /// Checks every block the last hop reports within `d`, with the
    /// verdicts of the hops before it, calling `tick` between blocks with
    /// the time elapsed. `rxs` are the hops' verdict streams, upstream
    /// first.
    pub fn run(
        &mut self,
        rxs: &[&Receiver<BlockVerdict>],
        d: Duration,
        mut tick: impl FnMut(Duration, &mut Checker),
    ) {
        let (upstream, last) = rxs.split_at(rxs.len() - 1);
        let mut pending: Vec<HashMap<u64, BlockVerdict>> = vec![HashMap::new(); upstream.len()];
        let t = Instant::now();
        while t.elapsed() < d {
            if let Ok(v) = last[0].recv_timeout(Duration::from_millis(50)) {
                let mut vs = Vec::new();
                for (rx, p) in upstream.iter().zip(&mut pending) {
                    let found = Instant::now();
                    // Upstream hops process block k first; a later block
                    // there without k means the hop skipped k.
                    while !p.contains_key(&v.k)
                        && !p.keys().any(|&u| u > v.k)
                        && found.elapsed() < Duration::from_millis(100)
                    {
                        if let Ok(u) = rx.recv_timeout(Duration::from_millis(10)) {
                            p.insert(u.k, u);
                        }
                    }
                    vs.push(p.remove(&v.k));
                    p.retain(|&k, _| k > v.k);
                }
                vs.push(Some(v));
                if let Err(e) = self.check(&vs) {
                    panic!("{e}");
                }
            }
            tick(t.elapsed(), self);
        }
    }
}

/// Blocks `ks` of a flow, planar per block, as libmxl serves them.
pub fn read_blocks(domain: &Path, flow_id: &str, channels: usize, ks: Range<u64>) -> Vec<f32> {
    let mut g = Generation::new(&api(), domain, 1).unwrap();
    let cfg = InputConfig {
        flow_id,
        channels: channels as u32,
        offset: 0,
    };
    let timing = Timing {
        block: B,
        margin: 0,
        max_catchup_blocks: 0,
    };
    g.attach(0, &cfg, &timing).unwrap();
    let input = g.input(0).unwrap();
    let mut state = SlotState::default();
    let mut buf = vec![0.0; channels * B as usize];
    let mut out = Vec::new();
    for k in ks {
        let o = input.read(&mut state, k, &GroupWait::Skipped, &mut buf);
        assert_eq!(o, Outcome::Copy, "block {k} of {flow_id}");
        out.extend_from_slice(&buf);
    }
    out
}

/// Starts an engine with a verdict stream of 65 s of blocks.
pub fn start(config: Config) -> (Engine, Receiver<BlockVerdict>) {
    let (tx, rx) = std::sync::mpsc::sync_channel(1 << 16);
    let process = samplez::rt::setup_process(&config.rt);
    (Engine::start(config, process, Some(tx)).unwrap(), rx)
}

/// Stops an engine whose verdicts were checked: every verdict was sent.
pub fn stop(engine: Engine) {
    assert_eq!(
        engine.counters().verdicts_dropped.load(Ordering::Relaxed),
        0
    );
    engine.stop();
}
