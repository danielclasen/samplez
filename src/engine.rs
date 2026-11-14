//! The engine: start-up, the supervisor thread and the status it keeps.
//!
//! The supervisor is the one thread that makes instance-mutating libmxl
//! calls. It builds input generations and output writers, hands them to
//! the RT thread through the command ring, destroys what comes back
//! through the return ring, prunes and re-adds inputs, retries output
//! creation and turns telemetry into status and log lines.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering::Acquire, Ordering::Relaxed};
use std::sync::mpsc::{self, SyncSender, TrySendError};
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
use crate::rt::{self, ProcessStatus, RtStatus, Step, ThreadStatus};
use crate::rt_loop::{
    BlockVerdict, Command, Counters, EMPTY_STATS, GainChange, Gen, Mix, Out, PENDING_CHANGES,
    Returned, Rt, Slot, SlotStats, Telemetry,
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
    /// Why the output no longer follows from the inputs and the changes
    /// sent: the first gain change that reached the RT thread after its
    /// index.
    pub diverged: Option<String>,
    /// The message of a panic on the RT thread. The engine then stops
    /// processing: its state was released on the supervisor and every
    /// output flow deleted.
    pub rt_panic: Option<String>,
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

/// Why [`Engine::set_gain`] refused a change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GainError {
    /// No such coefficient, a gain that is not finite within
    /// `MAX_GAIN`, or an index that is not a multiple of the block size.
    Invalid(String),
    /// `at_index` is below `earliest`, the first index the engine accepts.
    TooNear { at_index: u64, earliest: u64 },
    /// [`PENDING_CHANGES`] changes are outstanding.
    Full,
}

impl std::fmt::Display for GainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GainError::Invalid(m) => write!(f, "{m}"),
            GainError::TooNear { at_index, earliest } => write!(
                f,
                "at_index {at_index} is below the earliest accepted index {earliest}"
            ),
            GainError::Full => write!(f, "{PENDING_CHANGES} gain changes are outstanding"),
        }
    }
}

impl std::error::Error for GainError {}

/// The producer side of the gain ring and the changes pushed into it.
struct GainTx {
    ring: rtrb::Producer<GainChange>,
    submitted: u64,
}

/// A running engine. Dropping it stops the engine and releases its
/// writers, which deletes their flows.
pub struct Engine {
    stop: Arc<AtomicBool>,
    supervisor: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
    status: Arc<Mutex<Status>>,
    gains: Mutex<GainTx>,
    api: Arc<MxlApi>,
    block: u64,
    margin: u64,
    /// Samples between the newest index the RT thread may have produced
    /// and the earliest `at_index` accepted.
    lead: u64,
    shape: (usize, usize),
}

impl Engine {
    /// Starts the RT thread and the supervisor. `process` is the outcome
    /// of [`rt::setup_process`], run before any other thread. Each block's
    /// verdict goes to `verdicts` when given; one the channel has no room
    /// for is counted in `verdicts_dropped`.
    pub fn start(
        config: Config,
        process: ProcessStatus,
        verdicts: Option<SyncSender<BlockVerdict>>,
    ) -> Result<Engine, Error> {
        let api = samplez_mxl::load(&config.libmxl).map_err(Error::Mxl)?;
        let b = u64::from(config.block_size);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(Counters::default());
        let (cmd_tx, cmd_rx) = rtrb::RingBuffer::new(COMMAND_RING);
        let (ret_tx, ret_rx) = rtrb::RingBuffer::new(RETURN_RING);
        let (mut tel_tx, mut tel_rx) = rtrb::RingBuffer::new(TELEMETRY_RING);
        // The ring is allocated after mlockall: without MCL_FUTURE its
        // pages would fault in on the RT thread's first pushes. Every slot
        // is written once here instead.
        while tel_tx.push(Telemetry::ClockStep { delta_ns: 0 }).is_ok() {}
        while tel_rx.pop().is_ok() {}
        let (gain_tx, gain_rx) = rtrb::RingBuffer::new(PENDING_CHANGES);

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
            gains: gain_rx,
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
            rt: RtStatus {
                process: process.clone(),
                thread,
            },
            diverged: None,
            rt_panic: None,
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
        let now = Instant::now();
        // Without MCL_FUTURE the pages of a new generation's rings are not
        // locked; they are read once on the supervisor before handover.
        let prefault = !matches!(process.mlock, Step::Active(_));
        let margin = u64::from(config.reader_margin_samples);
        let lead = (u64::from(config.max_catchup_blocks) + 2) * b;
        let sup = Supervisor {
            api: Arc::clone(&api),
            timing: Timing {
                block: b,
                margin: u64::from(config.reader_margin_samples),
                max_catchup_blocks: u64::from(config.max_catchup_blocks),
            },
            prefault,
            in_domains: in_domains.clone(),
            slots: (0..config.inputs.len())
                .map(|_| SupSlot {
                    state: SupState::Wanted,
                    window: VecDeque::new(),
                })
                .collect(),
            dirty: vec![true; in_domains.len()],
            pollers: (0..in_domains.len()).map(|_| None).collect(),
            poll_errors: vec![None; in_domains.len()],
            rebuild_errors: vec![None; in_domains.len()],
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
            verdicts,
            status: Arc::clone(&status),
            counters: Arc::clone(&counters),
            stop: Arc::clone(&stop),
        };
        let supervisor = std::thread::Builder::new()
            .name("samplez-supervisor".to_owned())
            .stack_size(rt::STACK_SIZE)
            .spawn(move || {
                sup.run(rt_thread);
                // Held until the RT thread is joined, so its sender is
                // never the last reference: dropping that frees the
                // channel on whichever side lets go last.
                drop(thread_rx);
            })
            .map_err(|e| {
                stop.store(true, Relaxed);
                Error::Thread(format!("spawning the supervisor: {e}"))
            })?;
        Ok(Engine {
            stop,
            supervisor: Some(supervisor),
            counters,
            status,
            gains: Mutex::new(GainTx {
                ring: gain_tx,
                submitted: 0,
            }),
            api,
            block: b,
            margin,
            lead,
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
    /// one index apply in the order sent.
    ///
    /// A change is refused, never dropped later, when `at_index` is below
    /// `max(produced, mxlTimestampToIndex(now) - margin) + (maxCatchupBlocks
    /// + 2)B`, `produced` being the index of the next sample the matrix
    /// produces: the RT thread takes changes once per wake and may then
    /// process a backlog of up to `maxCatchupBlocks` blocks and the next
    /// one. It is refused with [`GainError::Full`] while
    /// [`PENDING_CHANGES`] changes are outstanding, sent but not yet
    /// applied. A change that still reaches the RT thread after its index,
    /// after an overrun or a clock step, is counted in `command_late`,
    /// not applied, and marks the engine diverged in [`Status`].
    pub fn set_gain(
        &self,
        input: usize,
        output: usize,
        gain: f32,
        at_index: u64,
    ) -> Result<(), GainError> {
        if input >= self.shape.0 || output >= self.shape.1 {
            return Err(GainError::Invalid(format!(
                "no matrix coefficient {input} -> {output}"
            )));
        }
        if !samplez_dsp::valid_gain(gain) {
            return Err(GainError::Invalid(format!(
                "gain {gain} is not finite within MAX_GAIN"
            )));
        }
        if !at_index.is_multiple_of(self.block) {
            return Err(GainError::Invalid(format!(
                "at_index {at_index} is not a multiple of the block size {}",
                self.block
            )));
        }
        let mut tx = self.gains.lock().expect("gain lock");
        let consumed = self.counters.gains_consumed.load(Acquire);
        if tx.submitted - consumed >= PENDING_CHANGES as u64 {
            return Err(GainError::Full);
        }
        let produced = self.counters.produced.load(Acquire);
        let clock = time::timestamp_to_index(&self.api, time::now(&self.api));
        let earliest = produced.max(clock.saturating_sub(self.margin)) + self.lead;
        if at_index < earliest {
            return Err(GainError::TooNear { at_index, earliest });
        }
        let change = GainChange {
            input,
            output,
            gain,
            at_index,
        };
        // The ring holds PENDING_CHANGES, at least the outstanding count.
        tx.ring.push(change).map_err(|_| GainError::Full)?;
        tx.submitted += 1;
        Ok(())
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
    Pruned {
        recovery: Recovery,
        /// Holds one reference to its flow's poll reader.
        held: bool,
    },
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
    /// Read every page of a new generation's rings before handover.
    prefault: bool,
    /// Configuration domain index of each input domain.
    in_domains: Vec<usize>,
    slots: Vec<SupSlot>,
    /// Input domains whose generation is to be rebuilt.
    dirty: Vec<bool>,
    pollers: Vec<Option<Pollers>>,
    /// The last failure to create each input domain's poll instance and
    /// generation, logged once per change of its text.
    poll_errors: Vec<Option<String>>,
    rebuild_errors: Vec<Option<String>>,
    outs: Vec<SupOut>,
    /// Output instance per configuration domain.
    out_instances: Vec<Option<Arc<Instance>>>,
    cmd: rtrb::Producer<Command>,
    ret: rtrb::Consumer<Returned>,
    tel: rtrb::Consumer<Telemetry>,
    verdicts: Option<SyncSender<BlockVerdict>>,
    status: Arc<Mutex<Status>>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

fn log(message: std::fmt::Arguments) {
    eprintln!("samplez: {message}");
}

impl Supervisor {
    fn run(mut self, rt_thread: JoinHandle<(Rt, Option<String>)>) {
        let mut rt_thread = Some(rt_thread);
        let tid = self.status().rt.thread.tid;
        let mut last_poll = Instant::now();
        let mut last_faults = Instant::now();
        while !self.stop.load(Relaxed) {
            while let Ok(r) = self.ret.pop() {
                release(r);
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
            if rt_thread.as_ref().is_some_and(JoinHandle::is_finished) {
                self.rt_ended(rt_thread.take());
                break;
            }
            std::thread::sleep(SUPERVISOR_TICK);
        }
        // The RT thread sees the same flag.
        self.rt_ended(rt_thread);
    }

    /// Joins the RT thread and releases its state, with every generation
    /// and writer it holds, and what it handed back. A panic is recorded
    /// in the status.
    fn rt_ended(&mut self, rt_thread: Option<JoinHandle<(Rt, Option<String>)>>) {
        let Some(t) = rt_thread else { return };
        let message = match t.join() {
            Ok((state, message)) => {
                drop(state);
                message
            }
            Err(_) => Some("the RT thread ended outside its loop".to_owned()),
        };
        while let Ok(r) = self.ret.pop() {
            release(r);
        }
        if let Some(message) = message {
            log(format_args!(
                "the RT thread panicked: {message}; processing stopped"
            ));
            let mut st = self.status();
            for o in &mut st.outputs {
                o.realised = false;
                o.reason = Some("the RT thread panicked".to_owned());
            }
            st.rt_panic = Some(message);
        }
    }

    fn status(&self) -> std::sync::MutexGuard<'_, Status> {
        self.status.lock().expect("status lock")
    }

    fn telemetry(&mut self) {
        while let Ok(t) = self.tel.pop() {
            match t {
                Telemetry::Block(v) => {
                    if let Some(tx) = &self.verdicts
                        && let Err(TrySendError::Full(_)) = tx.try_send(v)
                    {
                        self.counters.verdicts_dropped.fetch_add(1, Relaxed);
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
                Telemetry::LateGain { change, pos } => {
                    let reason = format!(
                        "gain change {} -> {} to {} at index {} arrived after index {} \
                         was produced; not applied",
                        change.input, change.output, change.gain, change.at_index, pos
                    );
                    log(format_args!("diverged: {reason}"));
                    self.status().diverged.get_or_insert(reason);
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

    /// Moves `slot` to polling.
    fn start_polling(&mut self, slot: usize) {
        let held = self.hold_poll(slot);
        self.slots[slot].state = SupState::Pruned {
            recovery: Recovery::default(),
            held,
        };
        self.status().inputs[slot].state = InputState::Pruned;
    }

    /// Takes one hold on the poll reader of `slot`'s flow, creating the
    /// domain's poll instance and the poll reader when missing. False when
    /// either could not be created; `poll` tries again.
    fn hold_poll(&mut self, slot: usize) -> bool {
        let d = self.input_domain(slot);
        let id = self.config.inputs[slot].flow_id.clone();
        if self.pollers[d].is_none() {
            let path = &self.config.domains[self.in_domains[d]].path;
            match Instance::new(&self.api, path) {
                Ok(instance) => {
                    self.poll_errors[d] = None;
                    self.pollers[d] = Some(Pollers {
                        instance,
                        flows: HashMap::new(),
                    })
                }
                Err(e) => {
                    let message = format!("poll instance on {}: {e}", path.display());
                    if changed(&mut self.poll_errors[d], message) {
                        log(format_args!(
                            "{}",
                            self.poll_errors[d].as_deref().unwrap_or("")
                        ));
                    }
                    return false;
                }
            }
        }
        let Some(p) = &mut self.pollers[d] else {
            return false;
        };
        if let Some(f) = p.flows.get_mut(&id) {
            f.hold();
        } else {
            match PolledFlow::new(&p.instance, &id) {
                Ok(f) => {
                    p.flows.insert(id, f);
                }
                Err(e) => {
                    log(format_args!("poll reader of {id}: {e}"));
                    return false;
                }
            }
        }
        true
    }

    /// Releases `slot`'s hold on its poll reader, if it has one.
    fn release_poll(&mut self, slot: usize) {
        let SupState::Pruned { held: true, .. } = self.slots[slot].state else {
            return;
        };
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
        for slot in 0..self.slots.len() {
            if matches!(self.slots[slot].state, SupState::Pruned { held: false, .. }) {
                let held = self.hold_poll(slot);
                if let SupState::Pruned { held: h, .. } = &mut self.slots[slot].state {
                    *h = held;
                }
            }
        }
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
            let SupState::Pruned { recovery: r, .. } = &mut self.slots[slot].state else {
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
                let message = format!("input generation on {}: {e}", path.display());
                if changed(&mut self.rebuild_errors[d], message) {
                    log(format_args!(
                        "{}",
                        self.rebuild_errors[d].as_deref().unwrap_or("")
                    ));
                }
                return;
            }
        };
        self.rebuild_errors[d] = None;
        let mut prune_after = vec![u64::MAX; n];
        for (slot, limit) in prune_after.iter_mut().enumerate() {
            if self.input_domain(slot) != d
                || matches!(self.slots[slot].state, SupState::Pruned { .. })
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
        if self.prefault
            && let Err(e) = generation.prefault()
        {
            log(format_args!(
                "input generation on {}: reading the rings before handover: {e}",
                path.display()
            ));
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

/// Stores `error` in `last`; true when it differs from the one stored, so
/// a failure repeated every tick is logged once per change of its text.
fn changed(last: &mut Option<String>, error: String) -> bool {
    if last.as_deref() == Some(error.as_str()) {
        return false;
    }
    *last = Some(error);
    true
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic on the RT thread hands the state back: the supervisor
    /// releases the writer, which deletes the output flow, and records
    /// the panic.
    #[test]
    fn a_panic_on_the_rt_thread_releases_its_state_on_the_supervisor() {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-panic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let id = "7fbec3b1-1b0f-417d-9059-8b94a47197ed";
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [{ "name": "out", "domain": "d", "flowId": id, "label": "panic",
                          "group": "panic", "role": "Audio", "channels": 1 }],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let engine = Engine::start(config, process, None).unwrap();
        let wait = |what: &str, f: &dyn Fn() -> bool| {
            let t = Instant::now();
            while !f() {
                assert!(
                    t.elapsed() < Duration::from_secs(5),
                    "timed out waiting for {what}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let flow = domain.join(format!("{id}.mxl-flow"));
        wait("output realised", &|| engine.status().outputs[0].realised);
        assert!(flow.exists());

        crate::rt_loop::PANIC.store(true, Relaxed);
        wait("the panic recorded", &|| engine.status().rt_panic.is_some());
        let st = engine.status();
        assert_eq!(st.rt_panic.as_deref(), Some("the panic test hook"));
        assert!(!st.outputs[0].realised);
        assert!(!flow.exists(), "the released writer deletes its flow");
        engine.stop();
        std::fs::remove_dir_all(&domain).unwrap();
    }

    #[test]
    fn a_repeated_failure_is_logged_once_per_text() {
        let mut last = None;
        assert!(changed(&mut last, "a".to_owned()));
        assert!(!changed(&mut last, "a".to_owned()));
        assert!(changed(&mut last, "b".to_owned()));
        last = None;
        assert!(changed(&mut last, "b".to_owned()));
    }
}
