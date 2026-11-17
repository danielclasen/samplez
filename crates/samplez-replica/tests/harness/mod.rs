//! An in-process replica set: every replica's storage in its own
//! directory, openraft's RPCs routed through one [`Net`] that cuts links
//! and stalls replicas, and a fake clock per replica. Run under a paused
//! tokio clock (`#[tokio::test(start_paused = true)]`) so openraft's
//! timers and the replicas' clocks move only with the runtime's virtual
//! time.
//!
//! openraft draws its election timeouts from an unseeded random source,
//! so which replica leads differs between runs; tests assert what holds
//! whichever one does. A "crash" ([`Set::crash`]) is a shutdown inside
//! this process: what was written but not fsynced stays in the page
//! cache, so nothing here models a power loss or a lost fsync.

#![allow(dead_code)]

use samplez_replica::rollout::Fixed;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, ServerState};
use samplez::blocks::{
    BlockStatus, Cause, Changes, Ctx, HEALTHY, Hold, Kind, PAGE, PageHash, Pages, Ranks, Sink,
    StatusChange, Statuses,
};
use samplez_replica::bootstrap::{Bootstrap, Marker, Markers, Volume, random_id};
use samplez_replica::bridge::{PAGES_REPORTED, page_of, word_or_skip};
use samplez_replica::contact::{self, Audio, Clocks, Sample, Timing};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::{Body, Request};
use samplez_replica::entry::{NodeId, Origin, SessionKey};
use samplez_replica::membership::{Slot, slot_of};
use samplez_replica::peer::{PeerClient, PeerReply, PeerRequest, PeerResponse};
use samplez_replica::report::{ClockFlags, Own, Page, Report, Run, runs};
use samplez_replica::resync::{CoreReply, Resynced, Source};
use samplez_replica::state_machine::{Changed, Effect, Product};
use samplez_replica::{Replica, Settings, TypeConfig, raft_config};
use samplez_service::{Owner, ReplicatedState, Role};
use tokio::time::Instant;

pub const RATE: u64 = 48_000;
pub const BLOCK: u64 = 48;

/// One value per path, set by `path=value@seq`: a seq not above the last
/// applied on its path is refused stale, as faderz orders its commands, and
/// the value a path holds already is unchanged.
///
/// `touch path@seq` and `release path@seq` take and give back a session's
/// touch ownership of a path, ordered by one seq per (session, path): one
/// not above the last applied for its (session, path) is refused stale. A
/// touch of a path another session owns changes nothing and is counted in
/// `conflicts`. A lease expiry releases what its session owns.
#[derive(Default)]
pub struct Paths {
    pub values: BTreeMap<String, (String, u64)>,
    pub applied: u64,
    /// Path -> the session owning it.
    pub owners: BTreeMap<String, String>,
    /// (session, path) -> the last touch or release seq applied.
    pub touches: BTreeMap<(String, String), u64>,
    pub conflicts: u64,
}

impl Paths {
    /// The paths `session` owns.
    pub fn owned_by(&self, session: &str) -> Vec<String> {
        self.owners
            .iter()
            .filter(|(_, s)| *s == session)
            .map(|(p, _)| p.clone())
            .collect()
    }

    fn touch(&mut self, origin: &Origin, text: &str) -> Effect<(String, String)> {
        let (verb, rest) = text.split_once(' ').expect("verb path@seq");
        let (path, seq) = rest.split_once('@').expect("path@seq");
        let seq: u64 = seq.parse().expect("seq");
        let key = (origin.session.clone(), path.to_owned());
        if self.touches.get(&key).is_some_and(|last| *last >= seq) {
            return Effect::Stale;
        }
        self.touches.insert(key, seq);
        self.applied += 1;
        let owner = self.owners.get(path).cloned();
        match verb {
            "touch" if owner.as_deref() == Some(&origin.session) => Effect::Unchanged,
            "touch" if owner.is_some() => {
                self.conflicts += 1;
                Effect::Unchanged
            }
            "touch" => {
                self.owners.insert(path.into(), origin.session.clone());
                Effect::Changed((format!("touch:{path}"), origin.session.clone()))
            }
            "release" if owner.as_deref() == Some(&origin.session) => {
                self.owners.remove(path);
                Effect::Changed((format!("touch:{path}"), String::new()))
            }
            "release" => Effect::Unchanged,
            verb => panic!("unknown verb {verb}"),
        }
    }
}

fn parse(command: &[u8]) -> (String, String, u64) {
    let text = std::str::from_utf8(command).expect("utf-8");
    let (path, rest) = text.split_once('=').expect("path=value@seq");
    let (value, seq) = rest.split_once('@').expect("value@seq");
    (path.into(), value.into(), seq.parse().expect("seq"))
}

impl Product for Paths {
    type Change = (String, String);

    fn apply(&mut self, origin: &Origin, command: &[u8]) -> Effect<(String, String)> {
        let text = std::str::from_utf8(command).expect("utf-8");
        if text.starts_with("touch ") || text.starts_with("release ") {
            return self.touch(origin, text);
        }
        let (path, value, seq) = parse(command);
        if self.values.get(&path).is_some_and(|(_, last)| *last >= seq) {
            return Effect::Stale;
        }
        self.applied += 1;
        let was = self.values.insert(path.clone(), (value.clone(), seq));
        if was.is_some_and(|(was, _)| was == value) {
            return Effect::Unchanged;
        }
        Effect::Changed((path, value))
    }
    fn expire(&mut self, session: &SessionKey) -> Effect<(String, String)> {
        let owned = self.owned_by(&session.session);
        if owned.is_empty() {
            return Effect::Unchanged;
        }
        self.owners.retain(|_, s| *s != session.session);
        Effect::Changed(("expired".into(), owned.join(",")))
    }
    fn forget(&mut self, _session: &SessionKey) {}
    fn delta(&self, _stamp: u64, changes: &[Changed<'_, (String, String)>]) -> String {
        // Later wins per path; every command that changed something is an
        // origin.
        let mut values = BTreeMap::new();
        let mut origins = Vec::new();
        for changed in changes {
            let (path, value) = changed.change;
            values.insert(path, value);
            if let Some(command) = changed.command {
                origins.push(format!("{}/{}", command.origin.session, command.id));
            }
        }
        let values: Vec<String> = values.iter().map(|(p, v)| format!("{p}={v}")).collect();
        format!("{} from {}", values.join(","), origins.join(","))
    }
    fn paths(&self, command: &[u8]) -> Vec<String> {
        let text = std::str::from_utf8(command).expect("utf-8");
        if let Some(rest) = text
            .strip_prefix("touch ")
            .or_else(|| text.strip_prefix("release "))
        {
            let (path, _) = rest.split_once('@').expect("path@seq");
            return vec![path.to_owned()];
        }
        vec![parse(command).0]
    }

    fn text(&self) -> String {
        serde_json::to_string(&self.values).expect("encodes")
    }
    fn save(&self) -> Vec<u8> {
        let touches: Vec<_> = self.touches.iter().collect();
        let saved = (
            &self.values,
            self.applied,
            &self.owners,
            touches,
            self.conflicts,
        );
        serde_json::to_vec(&saved).expect("encodes")
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        let touches: Vec<((String, String), u64)>;
        (
            self.values,
            self.applied,
            self.owners,
            touches,
            self.conflicts,
        ) = serde_json::from_slice(saved).map_err(|e| e.to_string())?;
        self.touches = touches.into_iter().collect();
        Ok(())
    }
}

pub fn owner(session: &str) -> Owner {
    Owner {
        principal: "anonymous".into(),
        session: session.into(),
        role: Role::Operator,
        connection: format!("{session}:nonce:1"),
    }
}

/// A replica's CLOCK_MONOTONIC in samples: the runtime's virtual time
/// since the set started, plus an offset a test sets.
pub struct Clock {
    base: Instant,
    offset: AtomicI64,
    /// Added to the TAI clock, ns.
    tai_offset: AtomicI64,
    /// The kernel TAI offset, whole seconds, and the clock steps the
    /// replica's engine detected.
    kernel_offset: AtomicI64,
    steps: AtomicU64,
    /// Told on the next read: a test's hook into the moment a replica
    /// stamps an entry, between its leadership check and the proposal.
    on_read: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    stall: Mutex<Option<(u32, Duration, tokio::sync::oneshot::Sender<()>)>>,
}

impl Clock {
    /// A clock at TAI offset 37 s.
    pub fn new(base: Instant) -> Clock {
        Clock {
            base,
            offset: AtomicI64::new(0),
            tai_offset: AtomicI64::new(0),
            kernel_offset: AtomicI64::new(37),
            steps: AtomicU64::new(0),
            on_read: Mutex::new(None),
            stall: Mutex::new(None),
        }
    }

    pub fn kernel_offset(&self) -> i64 {
        self.kernel_offset.load(Ordering::Relaxed)
    }

    pub fn steps(&self) -> u64 {
        self.steps.load(Ordering::Relaxed)
    }

    /// Sets the kernel TAI offset without a clock step, as the kernel's
    /// leap second (STA_INS) or a node configured before its pod starts.
    pub fn set_kernel_offset(&self, s: i64) {
        self.kernel_offset.store(s, Ordering::Relaxed);
    }

    /// Steps the kernel TAI offset as `adjtimex` does: CLOCK_TAI moves by
    /// the difference and the engine detects the step.
    pub fn step_kernel_offset(&self, s: i64) {
        let before = self.kernel_offset.swap(s, Ordering::Relaxed);
        self.tai_offset
            .fetch_add((s - before) * 1_000_000_000, Ordering::Relaxed);
        self.steps.fetch_add(1, Ordering::Relaxed);
    }

    pub fn samples(&self) -> u64 {
        if let Some(read) = self.on_read.lock().expect("clock lock poisoned").take() {
            let _ = read.send(());
        }
        let stalled = {
            let mut stall = self.stall.lock().expect("clock lock poisoned");
            match stall.take() {
                Some((0, by, read)) => Some((by, read)),
                Some((skip, by, read)) => {
                    *stall = Some((skip - 1, by, read));
                    None
                }
                None => None,
            }
        };
        if let Some((by, read)) = stalled {
            let _ = read.send(());
            std::thread::sleep(by);
        }
        let elapsed = self.base.elapsed().as_nanos() as u64 * RATE / 1_000_000_000;
        elapsed.saturating_add_signed(self.offset.load(Ordering::Relaxed))
    }

    /// Blocks the thread of the read after the next `skip` ones for `by`,
    /// as of a writer the operating system does not run between its
    /// leadership check and its proposal; resolves as that read starts.
    /// Only for a multi-threaded runtime: on the test's own thread it would
    /// stop everything.
    pub fn stall_read(&self, skip: u32, by: Duration) -> tokio::sync::oneshot::Receiver<()> {
        let (read, stalled) = tokio::sync::oneshot::channel();
        *self.stall.lock().expect("clock lock poisoned") = Some((skip, by, read));
        stalled
    }

    /// Resolves at the next read.
    pub fn next_read(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (read, next) = tokio::sync::oneshot::channel();
        *self.on_read.lock().expect("clock lock poisoned") = Some(read);
        next
    }

    pub fn set_offset(&self, samples: i64) {
        self.offset.store(samples, Ordering::Relaxed);
    }

    /// The replica's TAI clock, ns: [`TAI_BASE`] plus the runtime's
    /// virtual time since the set started, plus an offset a test sets.
    pub fn tai_ns(&self) -> u64 {
        let elapsed = self.base.elapsed().as_nanos() as u64;
        (TAI_BASE + elapsed).saturating_add_signed(self.tai_offset.load(Ordering::Relaxed))
    }

    pub fn set_tai_offset(&self, ns: i64) {
        self.tai_offset.store(ns, Ordering::Relaxed);
    }
}

/// Where every harness TAI clock starts, ns: a block boundary well past 0.
pub const TAI_BASE: u64 = 1_000_000_000_000_000;

/// A replica's TAI clock as contact reads it.
pub struct Tai(pub Arc<Clock>);

impl Clocks for Tai {
    fn tai_ns(&self) -> u64 {
        self.0.tai_ns()
    }

    fn tai_offset_s(&self) -> Option<i64> {
        Some(self.0.kernel_offset())
    }
}

/// The engine of one replica, in place of a real one: it computes every
/// block as it falls due on the replica's TAI clock, unless stalled, and
/// records what contact hands it. With outputs, each one is a real hold
/// ring and status ring ([`Hold`], [`Statuses`]) over a sink that records
/// the samples it commits; every sample carries its own index.
pub struct FakeAudio {
    clock: Arc<Clock>,
    timing: Timing,
    pub state: Mutex<Fake>,
}

/// One output of a [`FakeAudio`]: its pages hashed as the engine hashes
/// them, from the samples it computes.
pub struct Output {
    pub hold: Hold,
    pub statuses: Statuses,
    tx: rtrb::Producer<StatusChange>,
    rx: rtrb::Consumer<StatusChange>,
    dropped: AtomicU64,
    pub sink: Writes,
    pages: Pages,
    page_tx: rtrb::Producer<PageHash>,
    page_rx: rtrb::Consumer<PageHash>,
    /// The latest page records, as reports carry them.
    pub kept: VecDeque<Page>,
    /// Without a writer the output computes, hashes and commits nothing.
    pub realised: bool,
}

/// The ranges an output committed, `(start, len)`, in order.
#[derive(Debug, Default, Clone)]
pub struct Writes(pub Vec<(u64, usize)>);

impl Sink for Writes {
    fn write(&mut self, start: u64, len: usize, data: &[f32], _stride: usize) -> bool {
        for (i, &x) in data[..len].iter().enumerate() {
            assert_eq!(
                x,
                (start + i as u64) as f32,
                "sample {} out of place",
                start + i as u64
            );
        }
        self.0.push((start, len));
        true
    }
}

impl Writes {
    /// Whether sample `i` was committed.
    pub fn has(&self, i: u64) -> bool {
        self.0.iter().any(|&(s, n)| (s..s + n as u64).contains(&i))
    }
}

#[derive(Default)]
pub struct Fake {
    /// Entries appended, with their at_index.
    pub appended: Vec<(u64, Option<u64>)>,
    pub truncated: Vec<u64>,
    /// Each commit point handed, with the TAI time it was handed at.
    pub committed: Vec<(u64, u64)>,
    /// F as published, with the TAI time, when it changed.
    pub fences: Vec<(u64, u64)>,
    pub unresolved: Vec<(u64, u64)>,
    pub skipped: Vec<u64>,
    pub overwhelmed: u64,
    /// The processed block stands still from here.
    pub stalled_at: Option<u64>,
    pub flags: ClockFlags,
    pub build_lead: Option<u64>,
    pub entries_refused: u64,
    pub outputs: Vec<Output>,
    /// The last block computed.
    pub computed: Option<u64>,
    /// The ranks as published, with the TAI time.
    pub ranks: Vec<(u64, Ranks)>,
    /// Digests of degraded blocks: `(from, to, digest)` for the blocks
    /// `[from, to)` of every output; the rest are healthy.
    pub degraded: Vec<(u64, u64, u64)>,
    pub withdrawals: u64,
    /// The RT thread neither computes nor moves a block.
    pub frozen: bool,
    /// Commit starts and cancels the supervisor named are recorded and not
    /// applied: a test names them by hand.
    pub manual: bool,
    /// Commit starts the supervisor named, `(output, min_k)`, and cancels.
    pub commit_starts: Vec<(usize, u64)>,
    pub cancels: Vec<usize>,
    /// Blocks `[from, to)` whose samples differ from every other replica's
    /// by a factor `1 + by`, in what is hashed, not in what is written: content
    /// diverged.
    pub glitched: Vec<(u64, u64, f32)>,
    /// Entry fingerprints `(from, to, fingerprint)` of pages `[from, to)`;
    /// the rest have 0.
    pub fingerprints: Vec<(u64, u64, u64)>,
    /// A generation its pending entries need is not built.
    pub generation_pending: bool,
    /// The engine's core is dropped: every block is skipped, withheld
    /// without a status word, until it is rebuilt.
    pub no_core: bool,
    /// Engine drops, and rebuilds from product state, at the block last
    /// computed.
    pub drops: Vec<Option<u64>>,
    pub rebuilds: Vec<Option<u64>>,
    /// Stops the supervisor named, `(output, at)`.
    pub stops: Vec<(usize, u64)>,
    /// Blocks `[from, to)` skipped and withheld for a cause; for
    /// `ClockStep`, a detected clock step, with the step fence
    /// self-reported.
    pub stepped: Vec<(u64, u64, Cause)>,
    /// A stop at `u64::MAX`, letting a pending stop lapse, is recorded and
    /// not applied: it arrives after the stop took.
    pub lapse_late: bool,
    /// Rebuilds refused: a generation the core names was not built.
    pub refused_rebuilds: u64,
    /// Entries the core holds unresolved: a rebuilt core holds none.
    pub core_unresolved: BTreeSet<u64>,
    /// The length of `unresolved` at each rebuild.
    pub rebuilt_at: Vec<usize>,
    /// Entries resolved, with the length of `unresolved` then.
    pub resolved: Vec<(u64, usize)>,
    /// Copies a core for a resync and takes one: off, a donor copies
    /// none and a diverged replica stays diverged.
    pub resyncs: bool,
    /// Cores copied as a donor.
    pub cores_copied: u64,
    /// Hand-overs taken, `(k0, j)`: the donor's core after `k0`, the RT
    /// thread on from it after `j`.
    pub handovers: Vec<(u64, u64)>,
    /// A hand-over comes this many blocks after the block computed when
    /// the core arrived: the replay's catch-up.
    pub catch_up: u64,
    /// The next this many resyncs fail.
    pub failing: u32,
    /// The state a hand-over installs after its block: the donor's open
    /// glitches.
    pending_handover: Option<(u64, Vec<f32>)>,
    /// Rebuilds fail while set.
    pub rebuild_held: bool,
    /// Times the engine dropped its core for committed entries its RT
    /// thread did not take.
    pub log_overflows: u64,
}

/// An open glitch: a state divergence that lasts until a resync or a
/// rebuild replaces the state.
pub const FOR_GOOD: u64 = u64::MAX;

impl Fake {
    fn digest(&self, k: u64) -> u64 {
        self.degraded
            .iter()
            .rev()
            .find(|&&(a, b, _)| (a..b).contains(&k))
            .map_or(HEALTHY, |&(_, _, d)| d)
    }

    fn fingerprint(&self, page: u64) -> u64 {
        self.fingerprints
            .iter()
            .rev()
            .find(|&&(a, b, _)| (a..b).contains(&page))
            .map_or(0, |&(_, _, f)| f)
    }

    fn ctx(&self) -> Ctx {
        Ctx {
            fence: self.fences.last().map_or(0, |&(_, f)| f),
            ranks: self.ranks.last().map_or(Ranks::default(), |&(_, r)| r),
        }
    }

    /// Withdrawn for a cause.
    pub fn withdraw_all(&mut self) {
        self.withdraw(true);
    }

    /// Withdrawn on every output, `cause` as the engine's hold takes it.
    fn withdraw(&mut self, cause: bool) {
        self.withdrawals += 1;
        for out in &mut self.outputs {
            let mut changes = Changes {
                ring: &mut out.tx,
                dropped: &out.dropped,
                output: 0,
            };
            out.hold.withdraw(&out.statuses, &mut changes, cause);
        }
    }
}

impl FakeAudio {
    /// `alone`: the only voter, which commits every block at once.
    pub fn new(clock: Arc<Clock>, timing: Timing, outputs: usize, alone: bool) -> FakeAudio {
        let cap = contact::index_at((timing.hold + timing.contact_window).as_nanos() as u64)
            .div_ceil(timing.block) as usize
            + 2;
        let outputs = (0..outputs)
            .map(|o| {
                let (tx, rx) = rtrb::RingBuffer::new(4096);
                let (page_tx, page_rx) = rtrb::RingBuffer::new(256);
                Output {
                    hold: Hold::new(1, timing.block, cap, alone),
                    statuses: Statuses::new(256, timing.block),
                    tx,
                    rx,
                    dropped: AtomicU64::new(0),
                    sink: Writes::default(),
                    pages: Pages::new(o as u32, 1, timing.block, cap),
                    page_tx,
                    page_rx,
                    kept: VecDeque::new(),
                    realised: true,
                }
            })
            .collect();
        FakeAudio {
            clock,
            timing,
            state: Mutex::new(Fake {
                outputs,
                ..Fake::default()
            }),
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, Fake> {
        self.state.lock().expect("fake lock poisoned")
    }

    /// The newest block due on the replica's clock.
    pub fn due(&self) -> Option<u64> {
        let index = contact::index_at(self.clock.tai_ns());
        (index.saturating_sub(self.timing.margin) / self.timing.block).checked_sub(1)
    }

    /// The TAI time F first passed the end of block `k`.
    pub fn passed(&self, k: u64) -> Option<u64> {
        let end = (k + 1) * self.timing.block;
        self.lock()
            .fences
            .iter()
            .find(|&&(_, f)| f >= end)
            .map(|&(t, _)| t)
    }

    /// The TAI time entry `index` was first handed as committed.
    pub fn committed_at(&self, index: u64) -> Option<u64> {
        self.lock()
            .committed
            .iter()
            .find(|&&(up_to, _)| up_to >= index)
            .map(|&(_, t)| t)
    }

    /// Output `output` commits from the first page boundary in a block at
    /// or after `k`.
    pub fn commit_from(&self, output: usize, k: u64) {
        self.lock().outputs[output].hold.commit_from(k);
    }

    /// Every output commits from the first page boundary after the block
    /// due now; returns that block.
    pub fn commit_all(&self) -> u64 {
        let k = self.due().expect("a block is due") + 1;
        let mut st = self.lock();
        for out in &mut st.outputs {
            out.hold.commit_from(k);
        }
        k
    }

    /// The RT thread overruns: the next `n` blocks are skipped, withheld at
    /// the skip without a status word, and the replica withdraws.
    pub fn overrun(&self, n: u64) {
        self.step();
        let mut st = self.lock();
        if let Some(c) = st.computed {
            st.computed = Some(c + n);
        }
        st.withdraw_all();
    }

    /// Blocks `[from, to)` are degraded with `digest`.
    pub fn degrade(&self, from: u64, to: u64, digest: u64) {
        self.lock().degraded.push((from, to, digest));
    }

    /// Blocks `[from, to)` compute other samples than every other
    /// replica's, scaled by `1 + by`, unknown to the replica: its page hashes
    /// differ.
    pub fn glitch(&self, from: u64, to: u64, by: f32) {
        self.lock().glitched.push((from, to, by));
    }

    /// Output `output` loses its writer, as the engine releases one: its
    /// open blocks are withheld and it commits, hashes and computes
    /// nothing until [`FakeAudio::realise`].
    pub fn unrealise(&self, output: usize) {
        self.step();
        let mut st = self.lock();
        let out = &mut st.outputs[output];
        out.realised = false;
        let mut changes = Changes {
            ring: &mut out.tx,
            dropped: &out.dropped,
            output: output as u32,
        };
        out.hold
            .idle(&out.statuses, &mut changes, &mut out.sink, Cause::Released);
        out.pages.restart();
        out.kept.clear();
    }

    /// Output `output` has a writer again: idle until a commit start.
    pub fn realise(&self, output: usize) {
        self.step();
        self.lock().outputs[output].realised = true;
    }

    /// Commit starts and cancels the supervisor names are recorded only.
    pub fn manual(&self) {
        self.lock().manual = true;
    }

    /// Block `k`'s status on `output`.
    pub fn status_of(&self, output: usize, k: u64) -> Option<BlockStatus> {
        self.step();
        self.lock().outputs[output].statuses.get(k)
    }

    /// What `output` committed.
    pub fn writes(&self, output: usize) -> Writes {
        self.step();
        self.lock().outputs[output].sink.clone()
    }

    /// The RT thread: computes every block due since the last one, unless
    /// stalled, then moves the blocks in each ring.
    pub fn step(&self) {
        let due = self.due();
        let mut st = self.lock();
        let st = &mut *st;
        if st.outputs.is_empty() || st.frozen {
            return;
        }
        let target = match (due, st.stalled_at) {
            (Some(d), Some(s)) => Some(d.min(s)),
            (due, _) => due,
        };
        let Some(target) = target else {
            return;
        };
        if st.no_core {
            st.computed = Some(target);
            return;
        }
        let first = st.computed.map_or(target, |c| c + 1);
        if !st.stepped.is_empty() {
            st.flags.step_fence = st
                .stepped
                .iter()
                .any(|&(a, z, c)| c == Cause::ClockStep && (a..z).contains(&target));
        }
        let b = self.timing.block;
        for k in first..=target {
            if let Some((j, _)) = st.pending_handover
                && k == j + 1
            {
                let (_, donor) = st.pending_handover.take().expect("checked");
                for g in &mut st.glitched {
                    g.1 = g.1.min(k);
                }
                st.glitched
                    .extend(donor.into_iter().map(|by| (k, FOR_GOOD, by)));
                // The page holding the boundary mixes both states: no
                // record.
                for out in &mut st.outputs {
                    out.pages.restart();
                }
            }
            if let Some(&(_, _, cause)) = st.stepped.iter().find(|&&(a, z, _)| (a..z).contains(&k))
            {
                for out in &mut st.outputs {
                    out.statuses.withhold(k, cause);
                }
                st.withdraw(true);
                st.computed = Some(k);
                continue;
            }
            let ctx = st.ctx();
            let digest = st.digest(k);
            let data: Vec<f32> = (k * b..(k + 1) * b).map(|i| i as f32).collect();
            let shift = st
                .glitched
                .iter()
                .rev()
                .find(|&&(a, z, _)| (a..z).contains(&k))
                .map_or(0.0, |&(_, _, by)| by);
            // Scaled: an offset is lost at these indices' magnitude.
            let hashed: Vec<f32> = data.iter().map(|&x| x * (1.0 + shift)).collect();
            let fingerprint = st.fingerprint(((k + 1) * b - 1) / PAGE);
            for o in 0..st.outputs.len() {
                let out = &mut st.outputs[o];
                if !out.realised {
                    continue;
                }
                out.pages.hash(k, &hashed, Some(fingerprint), &out.dropped);
                let mut changes = Changes {
                    ring: &mut out.tx,
                    dropped: &out.dropped,
                    output: o as u32,
                };
                let decided = out.hold.compute(
                    k,
                    &data,
                    Some(digest),
                    ctx,
                    &out.statuses,
                    &mut changes,
                    &mut out.sink,
                );
                if decided.withdraw {
                    st.withdraw(decided.cause);
                }
            }
            st.computed = Some(k);
        }
        let ctx = st.ctx();
        for o in 0..st.outputs.len() {
            let out = &mut st.outputs[o];
            let mut changes = Changes {
                ring: &mut out.tx,
                dropped: &out.dropped,
                output: o as u32,
            };
            let moved = out
                .hold
                .advance(ctx, &out.statuses, &mut changes, &mut out.sink);
            while out.rx.pop().is_ok() {}
            out.pages
                .settle(&out.statuses, &mut out.page_tx, &out.dropped);
            while let Ok(h) = out.page_rx.pop() {
                if out.kept.len() == PAGES_REPORTED {
                    out.kept.pop_front();
                }
                let statuses = &out.statuses;
                out.kept.push_back(page_of(&h, b, |k| statuses.get(k)));
            }
            // As the engine does: before the next output advances.
            if moved.withdraw {
                st.withdraw(moved.cause);
            }
        }
    }

    /// Statuses a report carries: those of the blocks a peer can still
    /// hold a decision on, as [`samplez_replica::bridge::EngineAudio`]
    /// reports them.
    fn statuses(&self, st: &Fake) -> Vec<Vec<Run>> {
        let t = &self.timing;
        let span = t.hold + t.contact_window + 2 * t.report_period;
        let window = contact::index_at(span.as_nanos() as u64).div_ceil(t.block) + 1;
        let Some(newest) = st.computed else {
            return st.outputs.iter().map(|_| Vec::new()).collect();
        };
        let first = (newest + 1).saturating_sub(window);
        st.outputs
            .iter()
            .map(|out| runs((first..=newest).map(|k| Some(word_or_skip(out.statuses.get(k), k)))))
            .collect()
    }
}

impl Audio for FakeAudio {
    fn sample(&self) -> Sample {
        self.step();
        let due = self.due();
        let st = self.lock();
        let processed = if st.outputs.is_empty() {
            match (due, st.stalled_at) {
                (Some(d), Some(s)) => Some(d.min(s)),
                (due, _) => due,
            }
        } else {
            st.computed
        };
        Sample {
            own: Own {
                processed,
                clock: ClockFlags {
                    tai_offset_s: Some(self.clock.kernel_offset()),
                    ..st.flags
                },
                statuses: self.statuses(&st),
                standing: st.outputs.iter().map(|o| o.statuses.standing()).collect(),
                unwritten: Vec::new(),
                build_lead: st.build_lead,
                pages: st
                    .outputs
                    .iter()
                    .flat_map(|o| o.kept.iter().copied())
                    .collect(),
                ..Own::default()
            },
            status_changes_lost: 0,
            entries_refused: st.entries_refused,
            realised: st.outputs.iter().map(|o| o.realised).collect(),
            generation_ready: !st.generation_pending,
            clock_steps: self.clock.steps(),
            log_overflows: st.log_overflows,
        }
    }
    fn fence(&self, end: u64) {
        let tai = self.clock.tai_ns();
        {
            let mut st = self.lock();
            if st.fences.last().is_none_or(|&(_, f)| f != end) {
                st.fences.push((tai, end));
            }
        }
        self.step();
    }
    /// Without outputs, a block the engine computed leaves its hold ring,
    /// withheld when it is still fenced, once `D + w + 2B` of later blocks
    /// were computed; with outputs, as the bridge reads the status words.
    fn settled(&self, k: u64) -> bool {
        self.step();
        let st = self.lock();
        if st.outputs.is_empty() {
            drop(st);
            let ring = (self.timing.hold + self.timing.contact_window).as_nanos() as u64;
            let ring = contact::index_at(ring).div_ceil(self.timing.block) + 2;
            return self.sample().own.processed.is_some_and(|p| p >= k + ring);
        }
        st.outputs.iter().all(|o| match o.statuses.get(k) {
            Some(s) => matches!(s.kind, Kind::Committed | Kind::Withheld),
            None => st.computed.is_some_and(|p| k <= p),
        })
    }
    fn appended(&self, index: u64, request: &Request) {
        self.lock().appended.push((index, request.at_index()));
    }
    /// The engine refuses a command setting path `bad`, and an entry
    /// holding more than one command setting path `pair`.
    fn admits(&self, request: &Request) -> Result<(), String> {
        let Body::Commands(commands) = &request.body else {
            return Ok(());
        };
        let path = |p: &str| {
            commands
                .iter()
                .filter(|c| c.body.starts_with(p.as_bytes()))
                .count()
        };
        if path("bad=") > 0 {
            return Err("path bad".into());
        }
        if path("pair=") > 1 {
            return Err("two pairs".into());
        }
        Ok(())
    }
    fn truncated(&self, from: u64) {
        self.lock().truncated.push(from);
    }
    fn committed(&self, up_to: u64) {
        let tai = self.clock.tai_ns();
        self.lock().committed.push((up_to, tai));
    }
    fn unresolved(&self, index: u64, at_index: u64) -> bool {
        let mut st = self.lock();
        st.unresolved.push((index, at_index));
        st.core_unresolved.insert(index);
        true
    }
    fn resolved(&self, index: u64) {
        let mut st = self.lock();
        let n = st.unresolved.len();
        st.resolved.push((index, n));
        st.core_unresolved.remove(&index);
    }
    fn skip(&self, index: u64) {
        self.lock().skipped.push(index);
    }
    fn overwhelmed(&self) {
        self.lock().overwhelmed += 1;
    }
    fn status(&self, output: usize, k: u64) -> Option<BlockStatus> {
        self.step();
        self.lock().outputs.get(output)?.statuses.get(k)
    }
    fn withhold(&self, output: usize, k: u64, cause: Cause) -> Option<BlockStatus> {
        self.step();
        self.lock().outputs.get(output)?.statuses.withhold(k, cause)
    }
    fn commit_through(&self, output: usize, k: u64, on_silence_of: u8, up_to: Option<u64>) -> bool {
        self.step();
        self.lock()
            .outputs
            .get(output)
            .is_some_and(|o| o.statuses.commit_through(k, on_silence_of, up_to))
    }
    fn withdraw(&self) {
        self.step();
        self.lock().withdraw_all();
    }
    fn ranks(&self, ranks: Ranks) {
        let tai = self.clock.tai_ns();
        self.lock().ranks.push((tai, ranks));
    }
    fn commit_from(&self, output: usize, min_k: u64, seen: u64) {
        let mut st = self.lock();
        if st.commit_starts.last() != Some(&(output, min_k)) {
            st.commit_starts.push((output, min_k));
        }
        if !st.manual
            && let Some(out) = st.outputs.get_mut(output)
        {
            out.hold.commit_from_seen(min_k, seen);
        }
    }
    fn cancel_commit_start(&self, output: usize) {
        let mut st = self.lock();
        st.cancels.push(output);
        if !st.manual
            && let Some(out) = st.outputs.get_mut(output)
        {
            out.hold.cancel_commit_start();
        }
    }
    fn stop_at(&self, output: usize, at: u64) {
        self.step();
        let mut st = self.lock();
        st.stops.push((output, at));
        if at == u64::MAX && st.lapse_late {
            return;
        }
        if let Some(out) = st.outputs.get_mut(output) {
            out.hold.stop_at(at);
        }
    }
    fn drop_engine(&self) {
        self.step();
        let mut st = self.lock();
        let at = st.computed;
        st.drops.push(at);
        st.no_core = true;
    }
    /// Rebuilt from product state: whatever state diverged is gone.
    /// Refused while a generation the core names is not built, and
    /// failed while held.
    fn rebuild_engine(&self) -> bool {
        self.step();
        let mut st = self.lock();
        if st.generation_pending {
            st.refused_rebuilds += 1;
            return false;
        }
        if st.rebuild_held {
            return false;
        }
        let at = st.computed;
        let k = at.map_or(0, |c| c + 1);
        for g in &mut st.glitched {
            g.1 = g.1.min(k);
        }
        st.rebuilds.push(at);
        st.no_core = false;
        st.core_unresolved.clear();
        let n = st.unresolved.len();
        st.rebuilt_at.push(n);
        true
    }
    /// The open glitches as of the next block, in place of an engine core.
    fn copy_core(&self) -> Option<CoreReply> {
        self.step();
        let mut st = self.lock();
        if !st.resyncs {
            return None;
        }
        st.cores_copied += 1;
        let k0 = st.computed?;
        let open: Vec<f32> = st
            .glitched
            .iter()
            .filter(|g| g.0 <= k0 + 1 && g.1 > k0 + 1)
            .map(|g| g.2)
            .collect();
        Some(CoreReply {
            k0,
            last_id: 0,
            core: serde_json::to_string(&open).expect("encodes"),
        })
    }
    /// The replay catches up within `catch_up` blocks and hands over.
    /// `source` is read before the fake's lock is taken: it takes
    /// contact's lock, which contact holds while it calls into the fake.
    fn resync(&self, core: &CoreReply, source: &dyn Source) -> Result<Resynced, String> {
        self.step();
        let _ = source.entries(core.last_id);
        let go = source.go();
        let mut st = self.lock();
        if !st.resyncs {
            return Err("resyncs are off".to_owned());
        }
        if st.failing > 0 {
            st.failing -= 1;
            return Err("a replay failed".to_owned());
        }
        if !go {
            return Err("given up".to_owned());
        }
        let donor: Vec<f32> = serde_json::from_str(&core.core).map_err(|e| e.to_string())?;
        let j = st.computed.unwrap_or(0) + st.catch_up;
        st.pending_handover = Some((j, donor));
        st.handovers.push((core.k0, j));
        Ok(Resynced {
            j,
            pages: Vec::new(),
            unresolved: Vec::new(),
        })
    }
}

#[derive(Default)]
struct Links {
    rafts: BTreeMap<NodeId, Raft<TypeConfig>>,
    replicas: BTreeMap<NodeId, Arc<Replica<Paths>>>,
    /// (from, to): RPCs from `from` to `to` do not arrive.
    cut: BTreeSet<(NodeId, NodeId)>,
    /// (from, to): RPCs from `from` to `to` get no answer, as through a
    /// network that drops packets, until the caller's timeout.
    hung: BTreeSet<(NodeId, NodeId)>,
    /// RPCs to a stalled replica wait this long before it handles them.
    stalled: BTreeMap<NodeId, Duration>,
    delivered: u64,
    /// Appends from this node that carry the second step of a membership
    /// change, a configuration out of joint, do not arrive.
    hold_uniform_from: Option<NodeId>,
    /// (from, to): an append that carries an application entry arrives,
    /// and its answer never does.
    hold_replies: BTreeSet<(NodeId, NodeId)>,
    /// Side reports delivered, when a test taps them.
    reports: Option<Vec<(NodeId, Report)>>,
    /// (from, to): appends that carry no entry, heartbeats and commit
    /// advances, do not arrive.
    drop_heartbeats: BTreeSet<(NodeId, NodeId)>,
    /// (from, to): peer requests, side reports among them, do not
    /// arrive; openraft's RPCs do.
    cut_peer: BTreeSet<(NodeId, NodeId)>,
    /// (from, to): openraft's RPCs do not arrive; peer requests do.
    cut_raft: BTreeSet<(NodeId, NodeId)>,
    /// Side reports from this node arrive this much later, each on its
    /// own, as through a slow link that still carries every report.
    delayed: BTreeMap<NodeId, Duration>,
}

/// The links between the replicas of one set.
#[derive(Clone, Default)]
pub struct Net(Arc<Mutex<Links>>);

impl Net {
    fn links(&self) -> std::sync::MutexGuard<'_, Links> {
        self.0.lock().expect("net lock poisoned")
    }

    /// Cuts both directions between `a` and `b`.
    pub fn cut(&self, a: NodeId, b: NodeId) {
        let mut links = self.links();
        links.cut.insert((a, b));
        links.cut.insert((b, a));
    }

    pub fn cut_one_way(&self, from: NodeId, to: NodeId) {
        self.links().cut.insert((from, to));
    }

    /// Cuts `node` off from every other replica.
    pub fn isolate(&self, node: NodeId, of: impl IntoIterator<Item = NodeId>) {
        for other in of {
            if other != node {
                self.cut(node, other);
            }
        }
    }

    /// Makes every RPC between `node` and the others wait for an answer
    /// that never comes.
    pub fn hang(&self, node: NodeId, of: impl IntoIterator<Item = NodeId>) {
        let mut links = self.links();
        for other in of {
            if other != node {
                links.hung.insert((node, other));
                links.hung.insert((other, node));
            }
        }
    }

    pub fn heal(&self) {
        let mut links = self.links();
        links.cut.clear();
        links.hung.clear();
        links.hold_replies.clear();
        links.drop_heartbeats.clear();
        links.cut_peer.clear();
        links.cut_raft.clear();
        links.delayed.clear();
    }

    /// Joins `a` and `b` again in both directions.
    pub fn join(&self, a: NodeId, b: NodeId) {
        let mut links = self.links();
        links.cut.remove(&(a, b));
        links.cut.remove(&(b, a));
    }

    /// Cuts every link between the replicas in `ids`.
    pub fn cut_all(&self, ids: impl IntoIterator<Item = NodeId>) {
        let ids: Vec<NodeId> = ids.into_iter().collect();
        for &a in &ids {
            for &b in &ids {
                if a != b {
                    self.cut_one_way(a, b);
                }
            }
        }
    }

    /// From now on, an append from `from` to `to` that carries an
    /// application entry arrives and its answer never does, as openraft
    /// waits for an AppendEntries response stalled on the way.
    pub fn hold_replies(&self, from: NodeId, to: NodeId) {
        self.links().hold_replies.insert((from, to));
    }

    /// From now on, appends from `from` to `to` that carry no entry do
    /// not arrive: `to` takes entries and never learns their commit from
    /// openraft.
    pub fn drop_heartbeats(&self, from: NodeId, to: NodeId) {
        self.links().drop_heartbeats.insert((from, to));
    }

    /// Peer requests from `from` to `to` do not arrive; openraft's RPCs do.
    pub fn cut_peer(&self, from: NodeId, to: NodeId) {
        self.links().cut_peer.insert((from, to));
    }

    /// Peer requests from `from` to `to` arrive again.
    pub fn join_peer(&self, from: NodeId, to: NodeId) {
        self.links().cut_peer.remove(&(from, to));
    }

    /// openraft's RPCs between `a` and `b` do not arrive, either way; peer
    /// requests do.
    pub fn cut_raft(&self, a: NodeId, b: NodeId) {
        let mut links = self.links();
        links.cut_raft.insert((a, b));
        links.cut_raft.insert((b, a));
    }

    /// Side reports from `from` arrive `by` late, every one of them.
    pub fn delay_reports(&self, from: NodeId, by: Duration) {
        self.links().delayed.insert(from, by);
    }

    /// Records every side report delivered from now on.
    pub fn tap_reports(&self) {
        self.links().reports = Some(Vec::new());
    }

    /// The side reports delivered since the tap, with their addressee.
    pub fn reports(&self) -> Vec<(NodeId, Report)> {
        self.links().reports.clone().unwrap_or_default()
    }

    pub fn stall(&self, node: NodeId, by: Duration) {
        self.links().stalled.insert(node, by);
    }

    pub fn unstall(&self, node: NodeId) {
        self.links().stalled.remove(&node);
    }

    /// Drops `from`'s appends that carry the second step of a membership
    /// change, as a leader that goes down between the two steps leaves it.
    pub fn hold_uniform_from(&self, from: NodeId) {
        self.links().hold_uniform_from = Some(from);
    }

    /// RPCs delivered so far.
    pub fn delivered(&self) -> u64 {
        self.links().delivered
    }

    async fn route<E: std::error::Error>(
        &self,
        from: NodeId,
        to: NodeId,
        raft_rpc: bool,
    ) -> Result<Raft<TypeConfig>, RPCError<NodeId, BasicNode, E>> {
        let hung = self.links().hung.contains(&(from, to));
        if hung {
            std::future::pending::<()>().await;
        }
        let (raft, stall) = {
            let links = self.links();
            let raft = links
                .rafts
                .get(&to)
                .filter(|_| !links.cut.contains(&(from, to)))
                .cloned();
            (raft, links.stalled.get(&to).copied())
        };
        let unreachable = || {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                "{from} -> {to} down"
            ))))
        };
        let raft = raft.ok_or_else(unreachable)?;
        if let Some(stall) = stall {
            tokio::time::sleep(stall).await;
        }
        let mut links = self.links();
        if links.cut.contains(&(from, to)) || !links.rafts.contains_key(&to) {
            return Err(unreachable());
        }
        if raft_rpc && links.cut_raft.contains(&(from, to)) {
            return Err(unreachable());
        }
        links.delivered += 1;
        Ok(raft)
    }
}

/// The peer requests of one replica, over the same links as its RPCs; a
/// harness address is the node id.
pub struct Peers {
    from: NodeId,
    net: Net,
}

impl PeerClient for Peers {
    fn call<'a>(
        &'a self,
        to: Option<NodeId>,
        addr: &'a str,
        request: PeerRequest,
        within: Duration,
    ) -> BoxFuture<'a, Result<PeerReply, String>> {
        Box::pin(async move {
            let target = addr.parse().expect("a harness address is a node id");
            let delay = self.net.links().delayed.get(&self.from).copied();
            if let (Some(by), PeerRequest::Report(_)) = (delay, &request) {
                let (net, from) = (self.net.clone(), self.from);
                tokio::spawn(async move {
                    tokio::time::sleep(by).await;
                    let peers = Peers { from, net };
                    let _ = peers.deliver(to, target, request).await;
                });
                return Ok(Ok(PeerResponse::Reported));
            }
            let call = self.deliver(to, target, request);
            tokio::time::timeout(within, call)
                .await
                .unwrap_or_else(|_| Err("timed out".into()))
        })
    }
}

impl Peers {
    async fn deliver(
        &self,
        to: Option<NodeId>,
        target: NodeId,
        request: PeerRequest,
    ) -> Result<PeerReply, String> {
        if self.net.links().cut_peer.contains(&(self.from, target)) {
            return Err(format!("{} -> {target} down", self.from));
        }
        self.net
            .route::<io::Error>(self.from, target, false)
            .await
            .map_err(|e| e.to_string())?;
        let replica = {
            let mut links = self.net.links();
            if let (Some(tap), PeerRequest::Report(report)) = (links.reports.as_mut(), &request) {
                tap.push((target, (**report).clone()));
            }
            links.replicas.get(&target).cloned()
        };
        let replica = replica.ok_or_else(|| format!("{target} down"))?;
        Ok(replica.handle(to, request).await)
    }
}

pub struct Factory {
    from: NodeId,
    net: Net,
}

pub struct Link {
    from: NodeId,
    to: NodeId,
    net: Net,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Link;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Link {
        Link {
            from: self.from,
            to: target,
            net: self.net.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Link {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let held = self.net.links().hold_uniform_from == Some(self.from)
            && rpc.entries.iter().any(|e| {
                matches!(&e.payload,
                openraft::EntryPayload::Membership(m) if m.get_joint_config().len() == 1)
            });
        let dropped = rpc.entries.is_empty()
            && self
                .net
                .links()
                .drop_heartbeats
                .contains(&(self.from, self.to));
        if held || dropped {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "held",
            ))));
        }
        let carries = rpc
            .entries
            .iter()
            .any(|e| matches!(e.payload, openraft::EntryPayload::Normal(_)));
        let raft = self.net.route(self.from, self.to, true).await?;
        let reply = raft
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)));
        if carries
            && self
                .net
                .links()
                .hold_replies
                .contains(&(self.from, self.to))
        {
            std::future::pending::<()>().await;
        }
        reply
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.net.route(self.from, self.to, true).await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.net.route(self.from, self.to, true).await?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }
}

/// A fresh directory name under the target's tmp dir, for one test's
/// volumes. The process id alone repeats across containers that share a
/// target dir, where one test's cleanup would delete another's volumes.
pub fn root(kind: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "{kind}-{}-{:016x}",
        std::process::id(),
        random_id().expect("a random id")
    ))
}

/// How a set is built; the defaults are the chart's.
#[derive(Clone)]
pub struct Options {
    pub voters: u64,
    pub limits: Limits,
    pub raft: openraft::Config,
    /// The voters the leader reconciles the membership with; `None` leaves
    /// it as it is.
    pub wanted: Option<samplez_replica::membership::Voters>,
    pub write_timeout: Duration,
    /// Whether each replica starts `Replica::begin`, the marker bootstrap
    /// and the counting watch.
    pub begin: bool,
    /// Starts contact on every replica, with a [`FakeAudio`] each but the
    /// witness.
    pub contact: Option<Timing>,
    /// The node ids, in place of `0..voters`; a witness among them is
    /// waited for as a voter, not as commitable.
    pub ids: Option<Vec<NodeId>>,
    /// Outputs of each [`FakeAudio`]; none leaves it without blocks.
    pub outputs: usize,
    /// Each [`FakeAudio`] records the commit starts its supervisor names
    /// and applies none ([`FakeAudio::manual`]): the test names them.
    pub manual: bool,
    /// Kernel TAI offsets, whole seconds, set before the replicas start;
    /// every other node is at 37.
    pub kernel_offsets: Vec<(NodeId, i64)>,
    /// The offsets verify nobody: start waits for readiness only.
    pub unverified: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            voters: 3,
            limits: Limits::defaults(RATE),
            raft: raft_config("test"),
            wanted: None,
            write_timeout: Duration::from_secs(2),
            begin: true,
            contact: None,
            ids: None,
            outputs: 0,
            manual: false,
            kernel_offsets: Vec::new(),
            unverified: false,
        }
    }
}

pub struct Set {
    options: Options,
    root: PathBuf,
    pub net: Net,
    clocks: BTreeMap<NodeId, Arc<Clock>>,
    base: Instant,
    markers: Arc<FakeMarkers>,
    _panics: PanicGuard,
    replicas: BTreeMap<NodeId, Arc<Replica<Paths>>>,
    audios: BTreeMap<NodeId, Arc<FakeAudio>>,
    /// Per replica, the compatibility key and revision it starts with
    /// next; key 1 at revision `r1` when not set.
    versions: BTreeMap<NodeId, (u64, String)>,
    rollouts: BTreeMap<NodeId, Arc<Fixed>>,
    target: Option<String>,
}

impl Set {
    /// Starts `options.voters` replicas and initializes them as one set.
    pub async fn start(options: Options) -> Set {
        let root = root("replica-set");
        let _ = std::fs::remove_dir_all(&root);
        let base = Instant::now();
        let ids: Vec<NodeId> = options
            .ids
            .clone()
            .unwrap_or_else(|| (0..options.voters).collect());
        let clocks = ids
            .iter()
            .map(|&id| {
                let clock = Clock::new(base);
                (id, Arc::new(clock))
            })
            .collect();
        let mut set = Set {
            options,
            root,
            net: Net::default(),
            clocks,
            base,
            markers: Arc::new(FakeMarkers),
            _panics: PanicGuard::new(),
            replicas: BTreeMap::new(),
            audios: BTreeMap::new(),
            versions: BTreeMap::new(),
            rollouts: BTreeMap::new(),
            target: None,
        };
        for &(id, s) in &set.options.kernel_offsets {
            set.clocks[&id].set_kernel_offset(s);
        }
        for id in set.ids() {
            set.restart(id).await;
        }
        set.replica(ids[0])
            .initialize(set.ids().map(|id| (id, id.to_string())))
            .await
            .expect("initializes");
        set.leader().await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !set.replicas.iter().all(|(&id, r)| {
            let info = r.replica();
            info.ready
                && (slot_of(id) == Slot::Witness || info.commitable || set.options.unverified)
        }) {
            assert!(
                Instant::now() < deadline,
                "not every replica commitable in 30 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        set
    }

    pub fn ids(&self) -> impl Iterator<Item = NodeId> + use<> {
        self.options
            .ids
            .clone()
            .unwrap_or_else(|| (0..self.options.voters).collect())
            .into_iter()
    }

    pub fn clock(&self, id: NodeId) -> &Clock {
        &self.clocks[&id]
    }

    pub fn audio(&self, id: NodeId) -> &Arc<FakeAudio> {
        self.audios.get(&id).expect("contact running")
    }

    pub fn replica(&self, id: NodeId) -> &Arc<Replica<Paths>> {
        self.replicas.get(&id).expect("replica running")
    }

    pub fn running(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas.keys().copied()
    }

    /// Starts `id` from what its volume holds, stopping it first if it
    /// runs: a second instance would take the same node id and volume
    /// while the first keeps its timers, calling elections and appending
    /// to the log beside it.
    pub async fn restart(&mut self, id: NodeId) {
        self.crash(id).await;
        let base = self.base;
        let clock = self
            .clocks
            .entry(id)
            .or_insert_with(|| Arc::new(Clock::new(base)))
            .clone();
        let settings = Settings {
            id,
            epoch: CLUSTER,
            limits: self.options.limits,
            block: BLOCK,
            write_timeout: self.options.write_timeout,
            addr: id.to_string(),
            peers: self.ids().map(|id| id.to_string()).collect(),
            voters: self.options.wanted,
        };
        let replica = Replica::start(
            settings,
            self.options.raft.clone(),
            &self.root.join(id.to_string()),
            Paths::default(),
            Factory {
                from: id,
                net: self.net.clone(),
            },
            Arc::new(Peers {
                from: id,
                net: self.net.clone(),
            }),
            Arc::new(move || clock.samples()),
        )
        .await
        .expect("replica starts");
        let replica = Arc::new(replica);
        if let Some(timing) = self.options.contact {
            let clock = self.clocks[&id].clone();
            let audio = (slot_of(id) != Slot::Witness).then(|| {
                let alone = self.ids().count() == 1;
                let audio = FakeAudio::new(clock.clone(), timing, self.options.outputs, alone);
                if self.options.manual {
                    audio.manual();
                }
                Arc::new(audio)
            });
            let engine = audio.clone().map(|a| a as Arc<dyn Audio>);
            replica
                .contact(timing, Arc::new(Tai(clock)), engine)
                .expect("contact starts");
            let (key, revision) = self
                .versions
                .get(&id)
                .cloned()
                .unwrap_or((1, "r1".to_owned()));
            let rollout = Fixed::new(key, Some(revision));
            rollout.set_target(self.target.clone());
            replica.rollout(rollout.clone()).expect("contact runs");
            self.rollouts.insert(id, rollout);
            if let Some(audio) = audio {
                self.audios.insert(id, audio);
            }
        }
        if self.options.begin {
            let dir = self.root.join(id.to_string());
            replica.begin(Bootstrap {
                markers: self.markers.clone(),
                volume: Volume::open(&dir).expect("volume opens"),
                slot: slot_of(id),
            });
        }
        let mut links = self.net.links();
        links.rafts.insert(id, replica.raft().clone());
        links.replicas.insert(id, replica.clone());
        drop(links);
        self.replicas.insert(id, replica);
    }

    /// `id` starts with compatibility key `key` at `revision` from its
    /// next restart on: its pod rolled to that revision.
    pub fn set_version(&mut self, id: NodeId, key: u64, revision: &str) {
        self.versions.insert(id, (key, revision.to_owned()));
    }

    /// The audio StatefulSet's target revision, as every replica reads it.
    pub fn set_target(&mut self, target: Option<&str>) {
        self.target = target.map(str::to_owned);
        for r in self.rollouts.values() {
            r.set_target(self.target.clone());
        }
    }

    /// Stops `id` as a process crash would: its peers stop reaching it at
    /// once and it keeps only what its volume holds. The shutdown is
    /// graceful and in-process, so the volume keeps unsynced writes too.
    pub async fn crash(&mut self, id: NodeId) {
        self.net.links().rafts.remove(&id);
        self.net.links().replicas.remove(&id);
        if let Some(replica) = self.replicas.remove(&id) {
            replica.shutdown().await;
        }
    }

    /// The leader every running replica agrees on, waiting for one.
    pub async fn leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leaders: BTreeSet<Option<NodeId>> = self
                .replicas
                .values()
                .map(|r| r.raft().metrics().borrow().current_leader)
                .collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..]
                && self
                    .replicas
                    .get(&leader)
                    .is_some_and(|r| r.raft().metrics().borrow().state == ServerState::Leader)
            {
                return leader;
            }
            assert!(Instant::now() < deadline, "no leader agreed within 30 s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until `id` takes client writes again: within a check of a
    /// leader counting it.
    pub async fn commitable(&self, id: NodeId) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.replica(id).replica().commitable {
            assert!(Instant::now() < deadline, "{id} not commitable within 5 s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Removes what `id`'s volume holds; for a stopped replica.
    pub fn wipe(&self, id: NodeId) {
        std::fs::remove_dir_all(self.root.join(id.to_string())).expect("wipes");
    }

    /// Waits until every running replica applied product revision
    /// `revision`.
    pub async fn applied(&self, revision: u64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self
            .replicas
            .values()
            .any(|r| ReplicatedState::revision(&**r) < revision)
        {
            assert!(
                Instant::now() < deadline,
                "revision {revision} not applied within 30 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Forces a snapshot install on `follower`, which must not lead: cut
    /// off from openraft's RPCs (peer requests still flow), it falls
    /// behind an entry the leader appends, snapshots and purges; once its
    /// RPCs flow again the leader can only send it the snapshot. Returns
    /// once it installed one. The set needs `max_in_snapshot_log_to_keep`
    /// 0 and no snapshot policy of its own.
    pub async fn force_install(&self, follower: NodeId) {
        let leader = self.leader().await;
        assert_ne!(leader, follower, "the follower leads");
        let installs = self.replica(follower).store().installs();
        let others: Vec<NodeId> = self.running().filter(|&id| id != follower).collect();
        for &id in &others {
            self.net.cut_raft(follower, id);
        }
        let store = self.replica(leader).store();
        let before = store.applied_index();
        let at = (contact::index_at(self.clock(leader).tai_ns()) + RATE).next_multiple_of(BLOCK);
        self.replica(leader).append(Body::InputDown {
            slot: 0,
            at_index: at,
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.applied_index() <= before {
            assert!(Instant::now() < deadline, "the entry applied on the leader");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let index = store.applied_index();
        let raft = self.replica(leader).raft().clone();
        raft.trigger().snapshot().await.expect("snapshot");
        while raft
            .metrics()
            .borrow()
            .snapshot
            .is_none_or(|id| id.index < index)
        {
            assert!(Instant::now() < deadline, "the leader's snapshot");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        raft.trigger().purge_log(index).await.expect("purge");
        while raft
            .metrics()
            .borrow()
            .purged
            .is_none_or(|id| id.index < index)
        {
            assert!(Instant::now() < deadline, "the leader's purge");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        {
            let mut links = self.net.links();
            links
                .cut_raft
                .retain(|&(a, b)| a != follower && b != follower);
        }
        while self.replica(follower).store().installs() == installs {
            assert!(Instant::now() < deadline, "{follower} installed a snapshot");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

impl Drop for Set {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The cluster id of every harness set, as its marker names it.
pub const CLUSTER: u64 = 1;

/// A marker naming a set that already runs: no harness replica is the
/// volume it names, so an empty volume joins as a learner.
pub struct FakeMarkers;

impl Markers for FakeMarkers {
    fn name(&self) -> &str {
        "harness-replica-cluster"
    }

    fn get(&self) -> BoxFuture<'_, Result<Option<Marker>, String>> {
        Box::pin(async {
            Ok(Some(Marker {
                cluster: CLUSTER,
                volume: u64::MAX,
            }))
        })
    }

    fn create(&self, _: Marker) -> BoxFuture<'_, Result<bool, String>> {
        Box::pin(async { Ok(false) })
    }
}

thread_local! {
    static PANICKED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Fails the test, when it is dropped, if a task on the test's thread
/// panicked: the current-thread runtime of `#[tokio::test]` runs every
/// spawned task there, openraft's core among them, and a panicking task
/// does not fail the test by itself.
pub struct PanicGuard(());

impl PanicGuard {
    pub fn new() -> PanicGuard {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                PANICKED.with(|panicked| panicked.set(true));
                previous(info);
            }));
        });
        PANICKED.with(|panicked| panicked.set(false));
        PanicGuard(())
    }
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() && PANICKED.with(|panicked| panicked.get()) {
            panic!("a task panicked during the test");
        }
    }
}
