//! The engine: start-up, the supervisor thread and the status it keeps.
//!
//! The supervisor is the one thread that makes instance-mutating libmxl
//! calls. It builds input generations and output writers, hands them to
//! the RT thread through the command ring, destroys what comes back
//! through the return ring, prunes and re-adds inputs, retries output
//! creation and turns telemetry into status and log lines.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use samplez_dsp::{Matrix, PlanarBuffer};
use samplez_mxl::input::{
    Counters as InputCounters, Generation, GroupWait, InputConfig, Poll, PolledFlow, Recovery,
    SlotState, Timing,
};
use samplez_mxl::output::{self, Creation};
use samplez_mxl::{Instance, MxlApi, time};

use crate::config::Config;
use crate::rt::{self, ProcessStatus, RtStatus, ThreadStatus};
use crate::rt_loop::{
    BlockVerdict, Command, Counters, EMPTY_STATS, GainChange, Gen, Mix, Out, Returned, Rt, Slot,
    SlotStats, Telemetry,
};

const COMMAND_RING: usize = 64;
const RETURN_RING: usize = 64;
const TELEMETRY_RING: usize = 8192;
const SUPERVISOR_TICK: Duration = Duration::from_millis(5);
const POLL_PERIOD: Duration = Duration::from_millis(100);
const BACKOFF_FIRST: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Seconds of slot statistics the margin warning looks at, plus one.
const MARGIN_WINDOW: usize = 11;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputState {
    /// In the RT thread's generation.
    Attached,
    /// Not in the generation; polled until its head is on time.
    Pruned,
}

#[derive(Clone, Debug)]
pub struct InputStatus {
    pub name: String,
    pub state: InputState,
    /// Why the last attach failed.
    pub error: Option<String>,
    /// The margin the producer's `maxCommitBatchSizeHint` needs, when the
    /// configured margin is below it.
    pub margin_short: Option<u64>,
    /// The margin the observed late blocks need: late blocks while the head
    /// advances at the nominal rate within 1 % over 10 s and some blocks
    /// are on time.
    pub margin_warning: Option<u64>,
    pub counters: InputCounters,
}

#[derive(Clone, Debug)]
pub struct OutputStatus {
    pub name: String,
    pub realised: bool,
    /// Why the output is not realised.
    pub reason: Option<String>,
    pub attempts: u64,
    /// Creations that opened a flow another writer created first.
    pub races: u64,
    /// `mxlCreateFlowWriter` failures after `mxlIsFlowActive` found no
    /// flow; libmxl reports any open() error of the check as not found.
    pub create_failures: u64,
    /// The flow's ring length and effective batch hints, once created.
    pub buffer_length: Option<u64>,
    pub hints: Option<(u32, u32)>,
}

#[derive(Clone, Debug)]
pub struct Status {
    pub rt: RtStatus,
    pub inputs: Vec<InputStatus>,
    pub outputs: Vec<OutputStatus>,
}

#[derive(Debug)]
pub enum Error {
    Mxl(samplez_mxl::Error),
    Thread(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mxl(e) => write!(f, "{e}"),
            Error::Thread(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {}

enum Request {
    Gain(GainChange),
}

/// A running engine. Dropping it stops the engine and releases its
/// writers, which deletes their flows.
pub struct Engine {
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
    status: Arc<Mutex<Status>>,
    requests: Sender<Request>,
    block: u64,
    shape: (usize, usize),
}

impl Engine {
    /// Starts the RT thread and the supervisor. `process` is the outcome
    /// of [`rt::setup_process`], run before any other thread. Each block's
    /// verdict goes to `verdicts` when given.
    pub fn start(
        config: Config,
        process: ProcessStatus,
        verdicts: Option<Sender<BlockVerdict>>,
    ) -> Result<Engine, Error> {
        let api = samplez_mxl::load(&config.libmxl).map_err(Error::Mxl)?;
        let b = u64::from(config.block_size);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let (cmd_tx, cmd_rx) = rtrb::RingBuffer::new(COMMAND_RING);
        let (ret_tx, ret_rx) = rtrb::RingBuffer::new(RETURN_RING);
        let (tel_tx, tel_rx) = rtrb::RingBuffer::new(TELEMETRY_RING);

        let mut in_domains: Vec<usize> = Vec::new();
        let mut slots = Vec::new();
        let mut channel = 0;
        for i in &config.inputs {
            let d = config.domain(&i.domain).expect("validated");
            let domain = in_domains.iter().position(|&x| x == d).unwrap_or_else(|| {
                in_domains.push(d);
                in_domains.len() - 1
            });
            slots.push(Slot {
                domain,
                first_channel: channel,
                channels: i.channels as usize,
                offset: i.offset_samples,
                state: SlotState::default(),
                fade_start: None,
                silenced_last: true,
                prune_requested: false,
                stats: EMPTY_STATS,
            });
            channel += i.channels as usize;
        }
        let mut outs = Vec::new();
        let mut channel = 0;
        for o in &config.outputs {
            outs.push(Out {
                first_channel: channel,
                channels: o.channels as usize,
                writer: None,
                first_k: 0,
            });
            channel += o.channels as usize;
        }
        let (n, m) = (config.input_channels(), config.output_channels());
        let mut matrix = Matrix::new(n, m);
        for (i, row) in config.matrix.iter().enumerate() {
            for (j, &g) in row.iter().enumerate() {
                assert!(matrix.set(i, j, g, 0), "validated gain");
            }
        }
        let widest = config
            .inputs
            .iter()
            .map(|i| i.channels)
            .chain(config.outputs.iter().map(|o| o.channels))
            .max()
            .unwrap_or(0) as usize;
        let rt_state = Rt {
            api: Arc::clone(&api),
            block: b,
            margin: u64::from(config.reader_margin_samples),
            max_catchup: u64::from(config.max_catchup_blocks),
            ramp: config.ramp_samples,
            gens: (0..in_domains.len()).map(|_| None).collect(),
            statuses: (0..in_domains.len()).map(|_| GroupWait::Skipped).collect(),
            slots,
            outs,
            mix: Mix::new(matrix, b, config.ramp_samples),
            inbuf: PlanarBuffer::new(n, b as usize),
            outbuf: PlanarBuffer::new(m, b as usize),
            scratch: vec![0.0; widest * b as usize],
            cmd: cmd_rx,
            ret: ret_tx,
            tel: tel_tx,
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
        };

        let (thread_tx, thread_rx) = mpsc::sync_channel::<ThreadStatus>(1);
        let rt_cfg = config.rt.clone();
        let rt_thread = std::thread::Builder::new()
            .name("samplez-rt".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || {
                let _ = thread_tx.send(rt::setup_thread(&rt_cfg));
                drop(thread_tx);
                rt_state.run()
            })
            .map_err(|e| Error::Thread(format!("spawning the RT thread: {e}")))?;
        let thread = thread_rx
            .recv()
            .map_err(|_| Error::Thread("the RT thread ended during set-up".to_owned()))?;

        let status = Arc::new(Mutex::new(Status {
            rt: RtStatus { process, thread },
            inputs: config
                .inputs
                .iter()
                .map(|i| InputStatus {
                    name: i.name.clone(),
                    state: InputState::Pruned,
                    error: None,
                    margin_short: None,
                    margin_warning: None,
                    counters: InputCounters::default(),
                })
                .collect(),
            outputs: config
                .outputs
                .iter()
                .map(|o| OutputStatus {
                    name: o.name.clone(),
                    realised: false,
                    reason: Some("not created yet".to_owned()),
                    attempts: 0,
                    races: 0,
                    create_failures: 0,
                    buffer_length: None,
                    hints: None,
                })
                .collect(),
        }));
        let (req_tx, req_rx) = mpsc::channel();
        let now = Instant::now();
        let sup = Supervisor {
            api,
            timing: Timing {
                block: b,
                margin: u64::from(config.reader_margin_samples),
                max_catchup_blocks: u64::from(config.max_catchup_blocks),
            },
            in_domains: in_domains.clone(),
            slots: (0..config.inputs.len())
                .map(|_| SupSlot {
                    state: SupState::Wanted,
                    window: VecDeque::new(),
                })
                .collect(),
            dirty: vec![true; in_domains.len()],
            pollers: (0..in_domains.len()).map(|_| None).collect(),
            outs: (0..config.outputs.len())
                .map(|_| SupOut {
                    realised: false,
                    next_try: now,
                    backoff: BACKOFF_FIRST,
                })
                .collect(),
            out_instances: (0..config.domains.len()).map(|_| None).collect(),
            config,
            cmd: cmd_tx,
            ret: ret_rx,
            tel: tel_rx,
            requests: req_rx,
            verdicts,
            status: Arc::clone(&status),
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
        };
        let supervisor = std::thread::Builder::new()
            .name("samplez-supervisor".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || sup.run(rt_thread))
            .map_err(|e| {
                stop.store(true, Relaxed);
                Error::Thread(format!("spawning the supervisor: {e}"))
            })?;
        Ok(Engine {
            stop,
            supervisor: Some(supervisor),
            counters,
            status,
            requests: req_tx,
            block: b,
            shape: (n, m),
        })
    }

    pub fn status(&self) -> Status {
        self.status.lock().expect("status lock").clone()
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Ramps the gain from input channel `input` to output channel
    /// `output` to `gain` over the configured ramp length, starting at
    /// sample index `at_index`, a multiple of the block size. Changes at
    /// one index apply in the order sent. A change whose index the engine
    /// has already produced is counted in `command_late` and not applied;
    /// one the command ring has no room for is counted in
    /// `command_rejected`.
    pub fn set_gain(
        &self,
        input: usize,
        output: usize,
        gain: f32,
        at_index: u64,
    ) -> Result<(), String> {
        if input >= self.shape.0 || output >= self.shape.1 {
            return Err(format!("no matrix coefficient {input} -> {output}"));
        }
        if gain.abs() > samplez_dsp::MAX_GAIN {
            return Err(format!("gain {gain} is not finite within MAX_GAIN"));
        }
        if !at_index.is_multiple_of(self.block) {
            return Err(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            ));
        }
        self.requests
            .send(Request::Gain(GainChange {
                input,
                output,
                gain,
                at_index,
            }))
            .map_err(|_| "the engine stopped".to_owned())
    }

    /// Stops the RT thread and the supervisor and releases every libmxl
    /// object.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Relaxed);
        if let Some(s) = self.supervisor.take() {
            let _ = s.join();
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum SupState {
    Attached,
    /// To be attached by the next generation of its domain.
    Wanted,
    Pruned(Recovery),
}

struct SupSlot {
    state: SupState,
    window: VecDeque<SlotStats>,
}

struct SupOut {
    realised: bool,
    next_try: Instant,
    backoff: Duration,
}

/// The poll instance of one input domain and its poll readers by flow id.
struct Pollers {
    instance: Arc<Instance>,
    flows: HashMap<String, PolledFlow>,
}

struct Supervisor {
    api: Arc<MxlApi>,
    config: Config,
    timing: Timing,
    /// Configuration domain index of each input domain.
    in_domains: Vec<usize>,
    slots: Vec<SupSlot>,
    /// Input domains whose generation is to be rebuilt.
    dirty: Vec<bool>,
    pollers: Vec<Option<Pollers>>,
    outs: Vec<SupOut>,
    /// Output instance per configuration domain.
    out_instances: Vec<Option<Arc<Instance>>>,
    cmd: rtrb::Producer<Command>,
    ret: rtrb::Consumer<Returned>,
    tel: rtrb::Consumer<Telemetry>,
    requests: Receiver<Request>,
    verdicts: Option<Sender<BlockVerdict>>,
    status: Arc<Mutex<Status>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

fn log(message: std::fmt::Arguments) {
    eprintln!("samplez: {message}");
}

impl Supervisor {
    fn run(mut self, rt_thread: JoinHandle<Rt>) {
        let tid = self.status().rt.thread.tid;
        let mut last_poll = Instant::now();
        let mut last_faults = Instant::now();
        while !self.stop.load(Relaxed) {
            while let Ok(r) = self.ret.pop() {
                release(r);
            }
            while let Ok(Request::Gain(change)) = self.requests.try_recv() {
                if self.cmd.push(Command::Gain(change)).is_err() {
                    self.counters.command_rejected.fetch_add(1, Relaxed);
                    log(format_args!(
                        "gain change {change:?} rejected: command ring full"
                    ));
                }
            }
            self.telemetry();
            if last_poll.elapsed() >= POLL_PERIOD {
                last_poll = Instant::now();
                self.poll();
            }
            for d in 0..self.dirty.len() {
                if self.dirty[d] && self.cmd.slots() > 0 {
                    self.rebuild(d);
                }
            }
            self.outputs();
            if last_faults.elapsed() >= Duration::from_secs(1) {
                last_faults = Instant::now();
                if let Some(f) = minor_faults(tid) {
                    self.counters.rt_minor_faults.store(f, Relaxed);
                }
            }
            std::thread::sleep(SUPERVISOR_TICK);
        }
        // The RT thread sees the same flag. Its state, with every
        // generation and writer it holds, is released here.
        drop(rt_thread.join());
        while let Ok(r) = self.ret.pop() {
            release(r);
        }
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().expect("status lock")
    }

    fn telemetry(&mut self) {
        while let Ok(t) = self.tel.pop() {
            match t {
                Telemetry::Block(v) => {
                    if let Some(tx) = &self.verdicts {
                        let _ = tx.send(v);
                    }
                }
                Telemetry::Prune {
                    slot,
                    invalid,
                    head,
                    end,
                } => self.prune(slot, invalid, head, end),
                Telemetry::Stats { slot, stats } => self.stats(slot, stats),
                Telemetry::ClockStep { delta_ns } => {
                    log(format_args!(
                        "clock step of {delta_ns} ns against CLOCK_MONOTONIC"
                    ));
                }
            }
        }
    }

    fn input_domain(&self, slot: usize) -> usize {
        let d = self
            .config
            .domain(&self.config.inputs[slot].domain)
            .expect("validated");
        self.in_domains
            .iter()
            .position(|&x| x == d)
            .expect("every input's domain is an input domain")
    }

    fn prune(&mut self, slot: usize, invalid: bool, head: u64, end: u64) {
        if !matches!(self.slots[slot].state, SupState::Attached) {
            return;
        }
        let name = &self.config.inputs[slot].name;
        let why = if invalid {
            "flow invalid"
        } else {
            "head frozen"
        };
        log(format_args!(
            "input {name} pruned: {why} at head {head}, block end {end}"
        ));
        self.start_polling(slot);
        let d = self.input_domain(slot);
        self.dirty[d] = true;
    }

    /// Moves `slot` to polling, holding the poll reader of its flow.
    fn start_polling(&mut self, slot: usize) {
        let d = self.input_domain(slot);
        let id = self.config.inputs[slot].flow_id.clone();
        if self.pollers[d].is_none() {
            let path = &self.config.domains[self.in_domains[d]].path;
            match Instance::new(&self.api, path) {
                Ok(instance) => {
                    self.pollers[d] = Some(Pollers {
                        instance,
                        flows: HashMap::new(),
                    })
                }
                Err(e) => log(format_args!("poll instance on {}: {e}", path.display())),
            }
        }
        if let Some(p) = &mut self.pollers[d] {
            if let Some(f) = p.flows.get_mut(&id) {
                f.hold();
            } else if let Ok(f) = PolledFlow::new(&p.instance, &id) {
                p.flows.insert(id, f);
            }
        }
        self.slots[slot].state = SupState::Pruned(Recovery::default());
        self.status().inputs[slot].state = InputState::Pruned;
    }

    /// Releases `slot`'s hold on its poll reader.
    fn release_poll(&mut self, slot: usize) {
        let d = self.input_domain(slot);
        let id = &self.config.inputs[slot].flow_id;
        if let Some(p) = &mut self.pollers[d]
            && let Some(f) = p.flows.get_mut(id)
            && f.release() == 0
        {
            p.flows.remove(id);
        }
    }

    fn poll(&mut self) {
        let now = time::now(&self.api);
        let mut heads: HashMap<(usize, String), Option<u64>> = HashMap::new();
        for (d, p) in self.pollers.iter_mut().enumerate() {
            let Some(p) = p else { continue };
            for (id, f) in &mut p.flows {
                let head = match f.poll() {
                    Poll::Head(h) => Some(h),
                    Poll::Invalid | Poll::Absent(_) => None,
                };
                heads.insert((d, id.clone()), head);
            }
        }
        for slot in 0..self.slots.len() {
            let d = self.input_domain(slot);
            let cfg = &self.config.inputs[slot];
            let head = heads.get(&(d, cfg.flow_id.clone())).copied().flatten();
            let SupState::Pruned(r) = &mut self.slots[slot].state else {
                continue;
            };
            if r.update(&self.api, head, now, &self.timing, cfg.offset_samples) {
                log(format_args!("input {} on time again, re-adding", cfg.name));
                self.release_poll(slot);
                self.slots[slot].state = SupState::Wanted;
                self.dirty[d] = true;
            }
        }
    }

    /// Builds a new generation of input domain `d` with every attached and
    /// wanted slot and hands it to the RT thread.
    fn rebuild(&mut self, d: usize) {
        let path: PathBuf = self.config.domains[self.in_domains[d]].path.clone();
        let n = self.slots.len();
        let mut generation = match Generation::new(&self.api, &path, n) {
            Ok(g) => g,
            Err(e) => {
                log(format_args!("input generation on {}: {e}", path.display()));
                return;
            }
        };
        let mut prune_after = vec![u64::MAX; n];
        for (slot, limit) in prune_after.iter_mut().enumerate() {
            if self.input_domain(slot) != d || matches!(self.slots[slot].state, SupState::Pruned(_))
            {
                continue;
            }
            let cfg = &self.config.inputs[slot];
            let input = InputConfig {
                flow_id: &cfg.flow_id,
                channels: cfg.channels,
                offset: cfg.offset_samples,
            };
            match generation.attach(slot, &input, &self.timing) {
                Ok(short) => {
                    let hint = generation
                        .input(slot)
                        .and_then(|i| i.reader().config().ok())
                        .map_or(0, |c| u64::from(c.common.maxCommitBatchSizeHint));
                    *limit = (u64::from(self.config.prune_after_blocks) * self.timing.block)
                        .max(2 * hint + self.timing.margin);
                    if let Some(needed) = short {
                        log(format_args!(
                            "input {}: readerMarginSamples {} is below the {needed} its \
                             producer's maxCommitBatchSizeHint needs",
                            cfg.name, self.timing.margin
                        ));
                    }
                    if !matches!(self.slots[slot].state, SupState::Attached) {
                        log(format_args!("input {} attached", cfg.name));
                    }
                    self.slots[slot].state = SupState::Attached;
                    let mut st = self.status();
                    let s = &mut st.inputs[slot];
                    s.state = InputState::Attached;
                    s.error = None;
                    s.margin_short = short;
                }
                Err(e) => {
                    let message = e.to_string();
                    let name = cfg.name.clone();
                    log(format_args!("input {name} not attached: {message}"));
                    self.status().inputs[slot].error = Some(message);
                    self.start_polling(slot);
                }
            }
        }
        let command = Command::Generation {
            domain: d,
            generation: Box::new(Gen {
                mxl: generation,
                prune_after,
            }),
        };
        if self.cmd.push(command).is_ok() {
            self.dirty[d] = false;
        }
    }

    fn stats(&mut self, slot: usize, stats: SlotStats) {
        let window = &mut self.slots[slot].window;
        window.push_back(stats);
        if window.len() > MARGIN_WINDOW {
            window.pop_front();
        }
        let warning = margin_warning(&self.api, window, &self.timing);
        let mut st = self.status.lock().expect("status lock");
        let s = &mut st.inputs[slot];
        s.counters = stats.counters;
        if warning != s.margin_warning {
            if let Some(required) = warning {
                log(format_args!(
                    "input {}: late blocks while the head advances; readerMarginSamples {} \
                     needs to be at least {required}",
                    s.name, self.timing.margin
                ));
            }
            s.margin_warning = warning;
        }
    }

    fn outputs(&mut self) {
        let now = Instant::now();
        for o in 0..self.outs.len() {
            let out = &mut self.outs[o];
            if out.realised || now < out.next_try || self.cmd.slots() == 0 {
                continue;
            }
            let cfg = &self.config.outputs[o];
            let d = self.config.domain(&cfg.domain).expect("validated");
            if self.out_instances[d].is_none() {
                match Instance::new(&self.api, &self.config.domains[d].path) {
                    Ok(i) => self.out_instances[d] = Some(i),
                    Err(e) => {
                        retry(out, now);
                        let mut st = self.status.lock().expect("status lock");
                        st.outputs[o].reason = Some(e.to_string());
                        st.outputs[o].attempts += 1;
                        continue;
                    }
                }
            }
            let instance = self.out_instances[d].as_ref().expect("created above");
            let result = output::create(instance, &self.config.output_config(cfg));
            let mut st = self.status.lock().expect("status lock");
            let s = &mut st.outputs[o];
            s.attempts += 1;
            let reason = match result {
                Ok(Creation::Created { writer, collected }) => {
                    s.realised = true;
                    s.reason = None;
                    s.buffer_length = Some(writer.buffer_length());
                    s.hints = Some(writer.hints());
                    out.realised = true;
                    let command = Command::Writer {
                        output: o,
                        writer: Box::new(writer),
                    };
                    if self.cmd.push(command).is_err() {
                        unreachable!("a free command slot was checked");
                    }
                    log(format_args!(
                        "output {} realised{}",
                        cfg.name,
                        if collected {
                            " after collecting a stale flow"
                        } else {
                            ""
                        }
                    ));
                    continue;
                }
                Ok(Creation::Active) => "another writer is live on the flow".to_owned(),
                Ok(Creation::Raced) => {
                    s.races += 1;
                    "another writer created the flow first; libmxl v1.1.0 leaves a \
                     .mxl-tmp-* directory in the domain on this path (upstream #776)"
                        .to_owned()
                }
                Ok(Creation::Survived { gc }) => format!(
                    "a stale flow survived mxlGarbageCollectFlows (status {gc}); \
                     only the check is retried"
                ),
                Err(e) => {
                    if matches!(
                        e,
                        samplez_mxl::Error::Mxl {
                            call: "mxlCreateFlowWriter",
                            ..
                        }
                    ) {
                        s.create_failures += 1;
                    }
                    e.to_string()
                }
            };
            if s.reason.as_deref() != Some(&reason) {
                log(format_args!("output {} not realised: {reason}", cfg.name));
            }
            s.reason = Some(reason);
            retry(out, now);
        }
    }
}

/// Releases what the RT thread handed back: a generation's readers,
/// group and instance, or a writer, which deletes its flow.
fn release(r: Returned) {
    match r {
        Returned::Generation(g) => drop(g),
        Returned::Writer(w) => drop(w),
    }
}

fn retry(out: &mut SupOut, now: Instant) {
    out.next_try = now + out.backoff;
    out.backoff = (out.backoff * 2).min(BACKOFF_MAX);
}

/// The margin the slot's late blocks need, `margin - min_slack + B`, once
/// the window spans 10 s with late blocks, on-time blocks and a head that
/// advanced at the nominal rate within 1 %.
fn margin_warning(api: &MxlApi, window: &VecDeque<SlotStats>, timing: &Timing) -> Option<u64> {
    if window.len() < MARGIN_WINDOW {
        return None;
    }
    let (first, last) = (window.front()?, window.back()?);
    let late: u64 = window.iter().skip(1).map(|s| u64::from(s.late)).sum();
    let on_time: u64 = window.iter().skip(1).map(|s| u64::from(s.on_time)).sum();
    if late == 0 || on_time == 0 {
        return None;
    }
    let advanced = last.head?.checked_sub(first.head?)?;
    let nominal =
        time::timestamp_to_index(api, last.now) - time::timestamp_to_index(api, first.now);
    if advanced.abs_diff(nominal) * 100 > nominal {
        return None;
    }
    let min_slack = window
        .iter()
        .skip(1)
        .filter(|s| s.late > 0)
        .map(|s| s.min_slack)
        .min()?;
    Some((timing.margin as i64 - min_slack) as u64 + timing.block)
}

/// Minor page faults of thread `tid`, from `/proc/self/task/<tid>/stat`.
fn minor_faults(tid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")).ok()?;
    // Field 10, counted from the state field after the command name.
    stat.rsplit_once(") ")?.1.split(' ').nth(7)?.parse().ok()
}
