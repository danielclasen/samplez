//! Contact between voters, leader-independent (plan 2.11, Fencing): side
//! reports each report period p, the contact fence F (B1), silence held
//! and relayed, commit knowledge from openraft and from reports, and the
//! apply deadline that leaves an entry unresolved (B2).
//!
//! Each replica keeps the rules on its own clocks: its TAI clock for
//! everything compared with block deadlines, `CLOCK_MONOTONIC` (tokio's
//! clock) for receipt ages and silence.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openraft::{BasicNode, RaftMetrics};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::Shared;
use crate::entry::{Body, NodeId, Request};
use crate::log_store::Store;
use crate::peer::PeerRequest;
use crate::report::{Echo, Heard, Histogram, Listed, MAX_LISTED, Own, Report};
use crate::snapshot::StoredLogId;
use crate::state_machine::Product;

/// The engine's sample rate (plan 2.3).
pub const RATE: u64 = samplez_mxl::SAMPLE_RATE as u64;

/// The sample index at TAI time `tai_ns`.
pub fn index_at(tai_ns: u64) -> u64 {
    (u128::from(tai_ns) * u128::from(RATE) / 1_000_000_000) as u64
}

/// The TAI time of sample index `index`, ns.
pub fn time_of(index: u64) -> u64 {
    (u128::from(index) * 1_000_000_000).div_ceil(u128::from(RATE)) as u64
}

/// The timers of contact, as the chart sets them (plan 2.11, timer table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// The engine block size B.
    pub block: u64,
    /// `readerMarginSamples`: block k is due at `T((k + 1)B + margin)`.
    pub margin: u64,
    /// p, `reportPeriodMs`.
    pub report_period: Duration,
    /// w, `contactWindowMs`.
    pub contact_window: Duration,
    /// D, `holdMs`.
    pub hold: Duration,
    /// `clockGuardSamples`.
    pub clock_guard: u64,
    /// `maxApplyLeadMs`.
    pub max_apply_lead: Duration,
}

impl Timing {
    /// The defaults at block size `block`: p 2 ms, w 8 ms, D 10 ms, a
    /// margin of 576 samples, clockGuard half of it, maxApplyLead 50 ms.
    pub fn defaults(block: u64) -> Timing {
        Timing {
            block,
            margin: 576,
            report_period: Duration::from_millis(2),
            contact_window: Duration::from_millis(8),
            hold: Duration::from_millis(10),
            clock_guard: 288,
            max_apply_lead: Duration::from_millis(50),
        }
    }

    /// `applyGuard = w + 2 x clockGuard`, ns.
    pub fn apply_guard_ns(&self) -> u64 {
        self.contact_window.as_nanos() as u64 + 2 * time_of(self.clock_guard)
    }

    /// The TAI time from which an entry at `at_index` is unresolved unless
    /// its commit is known: `T(at_index) - applyGuard`.
    pub fn apply_deadline(&self, at_index: u64) -> u64 {
        time_of(at_index).saturating_sub(self.apply_guard_ns())
    }

    /// `T_k`, block k's deadline.
    pub fn block_deadline(&self, k: u64) -> u64 {
        time_of((k + 1) * self.block + self.margin)
    }
}

/// The TAI clock a replica judges deadlines by.
pub trait Clocks: Send + Sync + 'static {
    /// TAI time, ns.
    fn tai_ns(&self) -> u64;
}

/// `CLOCK_TAI`, the clock libmxl's `mxlGetTime` reads.
pub struct SystemClocks;

impl Clocks for SystemClocks {
    fn tai_ns(&self) -> u64 {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is writable; CLOCK_TAI exists on Linux.
        unsafe { libc::clock_gettime(libc::CLOCK_TAI, &mut ts) };
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    }
}

/// The engine of an audio replica as contact drives it; the witness has
/// none.
pub trait Audio: Send + Sync + 'static {
    /// What the replica says of itself in its reports. Read after every
    /// timestamp a report carries, so no exchange timestamp taken after a
    /// detected clock step goes out under the flags from before it.
    fn sample(&self) -> Sample;
    /// Publishes F: blocks ending above it are fenced.
    fn fence(&self, end: u64);
    /// Block k is final on every output, or was never computed and never
    /// will be: F may pass it whatever its contact.
    fn settled(&self, k: u64) -> bool;
    /// Entry `index` was appended to this replica's log.
    fn appended(&self, index: u64, request: &Request);
    /// The entries from `from` on were removed from the log.
    fn truncated(&self, from: u64);
    /// The entries up to `up_to` are known committed: they go to the RT
    /// thread.
    fn committed(&self, up_to: u64);
    /// Entry `index` at `at_index` is unresolved: every block from
    /// `at_index` is degraded until the log decides it. False when the
    /// engine had no room for it.
    fn unresolved(&self, index: u64, at_index: u64) -> bool;
    /// This log's entry `index` is never applied: its commit was not known
    /// by its apply deadline, so only the log's later decision on it
    /// counts.
    fn skip(&self, index: u64);
    /// An unresolved entry found no room: the replica can no longer tell
    /// its degraded blocks, so it stops committing them at once.
    fn overwhelmed(&self);
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sample {
    pub own: Own,
    /// Status changes the engine lost, counted.
    pub status_changes_lost: u64,
}

/// The voters of the applied membership: one configuration, or two
/// during a joint change, each of which a majority must hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Members {
    pub configs: Vec<BTreeSet<NodeId>>,
    pub addrs: BTreeMap<NodeId, String>,
}

impl Members {
    pub fn of(metrics: &RaftMetrics<NodeId, BasicNode>) -> Members {
        let membership = metrics.membership_config.membership();
        Members {
            configs: membership.get_joint_config().clone(),
            addrs: membership
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect(),
        }
    }

    pub fn voters(&self) -> BTreeSet<NodeId> {
        self.configs.iter().flatten().copied().collect()
    }

    /// Whether `holds` holds a majority of every configuration.
    fn majority(&self, holds: impl Fn(NodeId) -> bool) -> bool {
        !self.configs.is_empty()
            && self
                .configs
                .iter()
                .all(|c| c.iter().filter(|&&v| holds(v)).count() > c.len() / 2)
    }
}

/// The log as contact reads it.
pub trait LogView: Send + Sync + 'static {
    fn committed(&self) -> Option<StoredLogId>;
    fn applied(&self) -> u64;
    fn log_id_at(&self, index: u64) -> Option<StoredLogId>;
    fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request));
    fn pending(&self, f: &mut dyn FnMut(u64, &Body));
}

impl<P: Product> LogView for Store<P> {
    fn committed(&self) -> Option<StoredLogId> {
        Store::committed(self)
    }
    fn applied(&self) -> u64 {
        self.applied_index()
    }
    fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
        Store::log_id_at(self, index)
    }
    fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request)) {
        Store::entries_after(self, index, f)
    }
    fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
        Store::pending(self, f)
    }
}

/// Sequence numbers sent, with their TAI time, kept for echoes: 2 s at
/// the default period. An older echo counts for nothing.
const SENT_KEPT: usize = 1024;

/// Echoes kept per peer: 128 ms at the default period, past the hold
/// cap of every block not yet final.
const ECHOES_KEPT: usize = 64;

/// Entries this replica knows of and waits for the commit of, at most;
/// one more is unresolved at once.
const AWAITING_MAX: usize = 1024;

/// Entries with an at_index kept for listings, at most; the lowest index
/// is dropped first, counted.
const SCHEDULED_MAX: usize = 4096;

/// Entries given to the engine as unresolved that are remembered, so a
/// listing that names one again does not give it again.
const GIVEN_KEPT: usize = 1024;

/// The ids of the unresolvable entries a listing's overflow names: this
/// bit and the omitted at_index in blocks, so replicas that hold one for
/// the same index hold the same id, and none equals a log id or the
/// engine's own reserved `u64::MAX`.
pub const UNLISTED_BIT: u64 = 1 << 63;

/// The id of the unresolvable entry at `at_index` a listing's overflow
/// names.
pub fn unlisted_id(at_index: u64, block: u64) -> u64 {
    UNLISTED_BIT | (at_index / block)
}

/// A log id as a map key, in log order: index, then term and node.
type Key = (u64, u64, u64);

fn key(id: StoredLogId) -> Key {
    (id.index, id.term, id.node)
}

/// A report whose seq lies this far or less below the newest one taken
/// from its voter is older than that one; one further below comes from a
/// restarted process, which drew a new first seq.
const SEQ_WINDOW: u64 = 1 << 20;

/// What a voter received directly from another.
#[derive(Debug, Clone)]
struct Direct {
    seq: u64,
    /// This replica's TAI and monotonic time at receipt.
    received_tai: u64,
    received: Instant,
    own: Own,
    committed: Option<StoredLogId>,
}

#[derive(Debug, Default)]
struct Peer {
    last: Option<Direct>,
    /// The peer's latest echoes: the TAI time this replica sent the report
    /// echoed, and the `silentAt` the echo carried.
    echoes: VecDeque<(u64, Option<u64>)>,
    silent: bool,
    /// The latest TAI time this replica held the peer silent.
    silent_at: Option<u64>,
    /// The latest NTP-style offset estimate, the peer's clock minus this
    /// one's, ns.
    offset_ns: Option<i64>,
    delay: Histogram,
    gaps: Histogram,
    gaps_over_window: u64,
    reports: u64,
}

#[derive(Debug, Clone, Copy)]
struct Waiting {
    at_index: u64,
    /// Past its apply deadline: given to the engine at once.
    expired: bool,
}

#[derive(Debug, Default)]
struct Counts {
    shared_gaps: u64,
    shared_gap_max: Duration,
    stagger_max: Duration,
    /// The start of the recovery from a gap on every inbound link, and
    /// the voters not heard since.
    recovering: Option<(Instant, BTreeSet<NodeId>)>,
    last_any: Option<Instant>,
    status_lag: Histogram,
    unresolved: u64,
    unresolved_refused: u64,
    awaiting_overflow: u64,
    scheduled_dropped: u64,
}

struct State {
    members: Members,
    seq: u64,
    sent: VecDeque<(u64, u64)>,
    peers: BTreeMap<NodeId, Peer>,
    in_contact: bool,
    regained: Instant,
    /// The last log index handed to the engine as appended.
    told: u64,
    /// Entries handed as appended whose commit is not known.
    uncommitted: BTreeMap<u64, StoredLogId>,
    /// Entries with an at_index, for listings, until every voter reported
    /// them committed.
    scheduled: BTreeMap<u64, (StoredLogId, u64)>,
    /// Every entry of this replica's log up to here is committed.
    known_committed: u64,
    handed: u64,
    awaiting: BTreeMap<Key, Waiting>,
    /// Entries given to the engine as unresolved, the lowest index
    /// forgotten first.
    given: BTreeMap<Key, u64>,
    /// The lowest at_index of a listing overflow given as unresolved: a
    /// later overflow at or above it adds nothing, one below it is given.
    unlisted_low: Option<u64>,
    fence: Option<u64>,
    own: Own,
    status_changes_lost: u64,
    counts: Counts,
}

/// One voter's side of contact.
pub struct Contact {
    id: NodeId,
    timing: Timing,
    clocks: Arc<dyn Clocks>,
    audio: Option<Arc<dyn Audio>>,
    log: Arc<dyn LogView>,
    state: Mutex<State>,
}

/// Contact as status and metrics show it.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    /// F, none before it is first computed.
    pub fence: Option<u64>,
    pub in_contact: bool,
    pub known_committed: u64,
    pub peers: Vec<PeerMetrics>,
    /// Gaps longer than w in which no voter's report arrived.
    pub shared_gaps: u64,
    pub shared_gap_max: Duration,
    /// After such a gap, the longest from the first voter heard again to
    /// the last.
    pub stagger_max: Duration,
    /// statusLag: from `T_k` until the block's status was seen here,
    /// 99.99th percentile, as of the report period it was read in.
    pub status_lag_p9999: Option<Duration>,
    pub status_changes_lost: u64,
    /// Entries given to the engine as unresolved, and refusals of it.
    pub unresolved: u64,
    pub unresolved_refused: u64,
    /// Entries unresolved at once for want of room to wait for them.
    pub awaiting_overflow: u64,
    /// Entries dropped from the listings for want of room.
    pub scheduled_dropped: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerMetrics {
    pub node: NodeId,
    pub reports: u64,
    /// delta: the one-way report delay, half the exchange's round trip
    /// less the peer's turnaround, 99.99th percentile.
    pub delay_p9999: Option<Duration>,
    /// The longest gap between two of its reports, and those above w.
    pub gap_max: Duration,
    pub gaps_over_window: u64,
    /// This replica holds it silent.
    pub silent: bool,
    /// Its clock minus this one's, from the latest exchange, ns.
    pub offset_ns: Option<i64>,
}

fn since(now: Instant, then: Instant) -> Duration {
    now.saturating_duration_since(then)
}

impl Contact {
    /// Contact for voter `id`; `first_seq` should be drawn at random.
    pub fn new(
        id: NodeId,
        timing: Timing,
        clocks: Arc<dyn Clocks>,
        audio: Option<Arc<dyn Audio>>,
        log: Arc<dyn LogView>,
        first_seq: u64,
    ) -> Contact {
        let applied = log.applied();
        let mut scheduled = BTreeMap::new();
        // Entries the engine applies later, purged ones included, for
        // listings.
        // Collected first: the log's view is not re-entered from inside it.
        let mut pending = Vec::new();
        log.pending(&mut |index, body| {
            if let Some(at) = body.at_index() {
                pending.push((index, at));
            }
        });
        for (index, at) in pending {
            let id = log.log_id_at(index).unwrap_or(StoredLogId {
                term: 0,
                node: 0,
                index,
            });
            scheduled.insert(index, (id, at));
        }
        Contact {
            id,
            timing,
            clocks,
            audio,
            log,
            state: Mutex::new(State {
                members: Members::default(),
                seq: first_seq,
                sent: VecDeque::with_capacity(SENT_KEPT),
                peers: BTreeMap::new(),
                in_contact: false,
                regained: Instant::now(),
                // The engine is built from the state applied; entries after
                // it are handed as they come.
                told: applied,
                uncommitted: BTreeMap::new(),
                scheduled,
                known_committed: applied,
                handed: applied,
                awaiting: BTreeMap::new(),
                given: BTreeMap::new(),
                unlisted_low: None,
                fence: None,
                own: Own::default(),
                status_changes_lost: 0,
                counts: Counts::default(),
            }),
        }
    }

    pub fn timing(&self) -> Timing {
        self.timing
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("contact lock poisoned")
    }

    /// Takes a report from another voter.
    pub fn receive(&self, report: Report) {
        let now = Instant::now();
        let tai = self.clocks.tai_ns();
        let w = self.timing.contact_window;
        let mut st = self.lock();
        let st = &mut *st;
        let voters = st.members.voters();
        if report.from == self.id || !voters.contains(&report.from) {
            return;
        }
        self.expire(st, tai);
        // Gaps on every inbound link, and how staggered they recover.
        let c = &mut st.counts;
        if let Some(last) = c.last_any
            && since(now, last) > w
        {
            c.shared_gaps += 1;
            c.shared_gap_max = c.shared_gap_max.max(since(now, last));
            let rest: BTreeSet<NodeId> = voters
                .iter()
                .copied()
                .filter(|&v| v != self.id && v != report.from)
                .collect();
            c.recovering = (!rest.is_empty()).then_some((now, rest));
        } else if let Some((start, rest)) = &mut c.recovering {
            rest.remove(&report.from);
            if rest.is_empty() {
                c.stagger_max = c.stagger_max.max(since(now, *start));
                c.recovering = None;
            }
        }
        c.last_any = Some(now);

        let sent_at = |seq| {
            st.sent
                .iter()
                .rev()
                .find(|&&(s, _)| s == seq)
                .map(|&(_, t)| t)
        };
        let t1 = report.echo.and_then(|e| sent_at(e.seq));
        let peer = st.peers.entry(report.from).or_default();
        // A copy, or a report delayed past a newer one from the same
        // process.
        if peer
            .last
            .as_ref()
            .is_some_and(|l| l.seq.wrapping_sub(report.seq) < SEQ_WINDOW)
        {
            return;
        }
        peer.reports += 1;
        if let Some(last) = &peer.last {
            let gap = since(now, last.received);
            peer.gaps.add(gap.as_nanos() as u64);
            if gap > w {
                peer.gaps_over_window += 1;
            }
        }
        if let (Some(echo), Some(t1)) = (report.echo, t1) {
            if peer.echoes.len() == ECHOES_KEPT {
                peer.echoes.pop_front();
            }
            peer.echoes.push_back((t1, echo.silent_at));
            let (t1, t2, t3, t4) = (
                i128::from(t1),
                i128::from(echo.received_at),
                i128::from(report.sent_at),
                i128::from(tai),
            );
            let delay = (t4 - t1) - (t3 - t2);
            if delay >= 0 {
                peer.delay.add((delay / 2) as u64);
            }
            peer.offset_ns = Some((((t2 - t1) + (t3 - t4)) / 2) as i64);
        }
        peer.last = Some(Direct {
            seq: report.seq,
            received_tai: tai,
            received: now,
            own: report.own,
            committed: report.committed,
        });
        // Commit knowledge: a committed id this log holds, or a listed
        // entry's exact id at or below the sender's committed index.
        let mut learned = st.known_committed;
        if let Some(c) = report.committed
            && c.index > learned
            && self.log.log_id_at(c.index) == Some(c)
        {
            learned = c.index;
        }
        let committed_index = report.committed.map_or(0, |c| c.index);
        for l in &report.listed {
            if l.id.index > learned
                && l.id.index <= committed_index
                && self.log.log_id_at(l.id.index) == Some(l.id)
            {
                learned = l.id.index;
            }
        }
        self.learn(st, learned);
        for l in &report.listed {
            self.note(st, l.id, l.at_index, tai);
        }
        if let Some(at) = report.unlisted
            && st.unlisted_low.is_none_or(|low| at < low)
        {
            let id = StoredLogId {
                term: 0,
                node: 0,
                index: unlisted_id(at, self.timing.block),
            };
            self.note(st, id, at, tai);
        }
        self.fence(st, tai);
    }

    /// The log changed: appends and truncations go to the engine, and an
    /// entry openraft applied is known committed.
    pub fn log_changed(&self) {
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        self.expire(st, tai);
        self.sync_log(st, tai);
        let applied = self.log.applied();
        self.learn(st, applied);
    }

    /// One report period: the log, the deadlines, F, silence, and the
    /// reports to send, with each addressee's address.
    pub fn tick(&self, members: Members) -> Vec<(NodeId, String, Report)> {
        let now = Instant::now();
        // Taken before the engine's sample: every timestamp a report
        // carries precedes the flags beside it.
        let tai = self.clocks.tai_ns();
        let mut st = self.lock();
        let st = &mut *st;
        let voters = members.voters();
        st.peers.retain(|id, _| voters.contains(id));
        st.members = members;
        self.expire(st, tai);
        self.sync_log(st, tai);
        let applied = self.log.applied();
        self.learn(st, applied);

        if let Some(audio) = &self.audio {
            let sample = audio.sample();
            if let (Some(k), last) = (sample.own.processed, st.own.processed)
                && last.is_none_or(|l| k > l)
            {
                let lag = tai.saturating_sub(self.timing.block_deadline(k));
                st.counts.status_lag.add(lag);
            }
            st.own = sample.own;
            st.status_changes_lost = sample.status_changes_lost;
        }

        // Contact: reports from a majority, itself included, within w.
        let w = self.timing.contact_window;
        let heard_within = |st: &State, v: NodeId| {
            v == self.id
                || st
                    .peers
                    .get(&v)
                    .and_then(|p| p.last.as_ref())
                    .is_some_and(|d| since(now, d.received) <= w)
        };
        let in_contact = st.members.majority(|v| heard_within(st, v));
        if in_contact && !st.in_contact {
            st.regained = now;
        }
        st.in_contact = in_contact;
        for v in &voters {
            if *v == self.id {
                continue;
            }
            let regained = st.regained;
            let p = st.peers.entry(*v).or_default();
            let from = p
                .last
                .as_ref()
                .map_or(regained, |d| d.received.max(regained));
            p.silent = in_contact && since(now, from) > w;
            if p.silent {
                p.silent_at = Some(tai);
            }
        }
        self.fence(st, tai);

        // Reports.
        if !voters.contains(&self.id) {
            return Vec::new();
        }
        st.seq += 1;
        if st.sent.len() == SENT_KEPT {
            st.sent.pop_front();
        }
        st.sent.push_back((st.seq, tai));
        self.prune_scheduled(st);
        // The commit point this replica knows, reports included, so peers
        // list nothing it knows committed.
        let committed = match (self.log.committed(), self.log.log_id_at(st.known_committed)) {
            (Some(c), Some(k)) if k.index > c.index => Some(k),
            (None, k) => k,
            (c, _) => c,
        };
        let limit = index_at(
            tai + (self.timing.max_apply_lead + self.timing.contact_window).as_nanos() as u64,
        );
        let mut out = Vec::new();
        for to in voters.iter().copied().filter(|&v| v != self.id) {
            let Some(addr) = st.members.addrs.get(&to).cloned() else {
                continue;
            };
            let peer = st.peers.get(&to);
            let echo = peer.and_then(|p| {
                p.last.as_ref().map(|d| Echo {
                    seq: d.seq,
                    received_at: d.received_tai,
                    silent_at: p.silent_at,
                })
            });
            let their_committed = peer
                .and_then(|p| p.last.as_ref())
                .and_then(|d| d.committed)
                .map_or(0, |c| c.index);
            let mut listed: Vec<Listed> = st
                .scheduled
                .range(their_committed.saturating_add(1)..)
                .filter(|(_, (_, at))| *at < limit)
                .map(|(_, &(id, at_index))| Listed { id, at_index })
                .collect();
            listed.sort_by_key(|l| (l.at_index, l.id.index));
            let unlisted = listed.get(MAX_LISTED).map(|l| l.at_index);
            listed.truncate(MAX_LISTED);
            let heard = voters
                .iter()
                .copied()
                .filter(|&v| v != self.id && v != to)
                .map(|v| {
                    let p = st.peers.get(&v);
                    let d = p.and_then(|p| p.last.as_ref());
                    Heard {
                        node: v,
                        age_us: d.map(|d| since(now, d.received).as_micros() as u64),
                        silent: p.is_some_and(|p| p.silent),
                        own: d.map(|d| d.own.clone()),
                    }
                })
                .collect();
            out.push((
                to,
                addr,
                Report {
                    from: self.id,
                    seq: st.seq,
                    sent_at: tai,
                    echo,
                    committed,
                    own: st.own.clone(),
                    heard,
                    listed,
                    unlisted,
                },
            ));
        }
        out
    }

    /// Entries whose apply deadline passed with the commit unknown are
    /// unresolved. Runs before any new knowledge is taken, so knowledge
    /// learned after a deadline never counts as before it.
    fn expire(&self, st: &mut State, tai: u64) {
        let timing = self.timing;
        for w in st.awaiting.values_mut() {
            if tai >= timing.apply_deadline(w.at_index) {
                w.expired = true;
            }
        }
        self.offer_unresolved(st);
    }

    /// Gives the expired entries to the engine.
    fn offer_unresolved(&self, st: &mut State) {
        let expired: Vec<(Key, u64)> = st
            .awaiting
            .iter()
            .filter(|(_, w)| w.expired)
            .map(|(&k, w)| (k, w.at_index))
            .collect();
        for (k, _) in &expired {
            st.awaiting.remove(k);
        }
        let Some(audio) = &self.audio else {
            return;
        };
        for (k, at) in expired {
            let (index, term, node) = k;
            // This log's own entry is never applied: the log decides it.
            if index & UNLISTED_BIT == 0
                && self.log.log_id_at(index) == Some(StoredLogId { term, node, index })
            {
                audio.skip(index);
            }
            if audio.unresolved(index, at) {
                st.counts.unresolved += 1;
                if index & UNLISTED_BIT != 0 {
                    st.unlisted_low = Some(st.unlisted_low.map_or(at, |l| l.min(at)));
                }
                // Listed again, an entry given once is not given again.
                st.given.insert(k, at);
                while st.given.len() > GIVEN_KEPT {
                    st.given.pop_first();
                }
            } else {
                st.counts.unresolved_refused += 1;
                audio.overwhelmed();
            }
        }
    }

    /// Hands new appends and truncations to the engine.
    fn sync_log(&self, st: &mut State, tai: u64) {
        let cut = st
            .uncommitted
            .iter()
            .find(|&(&index, &id)| self.log.log_id_at(index) != Some(id))
            .map(|(&index, _)| index);
        if let Some(from) = cut {
            for (_, id) in st.uncommitted.split_off(&from) {
                st.awaiting.remove(&key(id));
            }
            st.scheduled.split_off(&from);
            st.told = from - 1;
            if let Some(audio) = &self.audio {
                audio.truncated(from);
            }
        }
        let mut new = Vec::new();
        self.log.entries_after(st.told, &mut |id, request| {
            if let Some(audio) = &self.audio {
                audio.appended(id.index, request);
                // Given unresolved from a listing before it arrived here:
                // never applied.
                if st.given.contains_key(&key(id)) {
                    audio.skip(id.index);
                }
            }
            new.push((id, request.at_index()));
        });
        for (id, at) in new {
            st.told = id.index;
            if id.index > st.known_committed {
                st.uncommitted.insert(id.index, id);
            }
            if let Some(at) = at {
                st.scheduled.insert(id.index, (id, at));
                self.note(st, id, at, tai);
            }
        }
        while st.scheduled.len() > SCHEDULED_MAX {
            st.scheduled.pop_first();
            st.counts.scheduled_dropped += 1;
        }
        self.hand(st);
    }

    /// An entry this replica knows of: unresolved at once when its apply
    /// deadline has passed, else awaited unless its commit is known.
    fn note(&self, st: &mut State, id: StoredLogId, at_index: u64, tai: u64) {
        // At or below the known commit point an entry is this log's own,
        // committed and handed when it was appended, or one that can no
        // longer commit.
        let k = key(id);
        if id.index <= st.known_committed
            || st.awaiting.contains_key(&k)
            || st.given.contains_key(&k)
        {
            return;
        }
        let full = st.awaiting.len() >= AWAITING_MAX;
        let expired = tai >= self.timing.apply_deadline(at_index);
        if full && !expired {
            st.counts.awaiting_overflow += 1;
        }
        st.awaiting.insert(
            k,
            Waiting {
                at_index,
                expired: expired || full,
            },
        );
        if expired || full {
            self.offer_unresolved(st);
        }
    }

    /// Every entry of this replica's log up to `index` is committed.
    /// Awaited entries at or below it are settled: this replica's own,
    /// or another at the same index that can no longer commit.
    fn learn(&self, st: &mut State, index: u64) {
        if index <= st.known_committed {
            return;
        }
        st.known_committed = index;
        st.uncommitted = st.uncommitted.split_off(&(index + 1));
        // Awaited entries only wait unexpired: expired ones were given.
        st.awaiting = st.awaiting.split_off(&(index + 1, 0, 0));
        self.hand(st);
    }

    fn hand(&self, st: &mut State) {
        let up_to = st.known_committed.min(st.told);
        if up_to > st.handed {
            st.handed = up_to;
            if let Some(audio) = &self.audio {
                audio.committed(up_to);
            }
        }
    }

    /// F (B1): block k is valid when a majority of every configuration,
    /// this replica included, echoed a report it sent at or after
    /// `T((k + 1)B) - w`, an echo whose `silentAt` lies at or after
    /// `T_k - clockGuard` not counted. F is the end of the run of blocks
    /// from the last F that are valid or settled (final, or never
    /// computed), so it never skips a block that may still commit. The
    /// witness, which computes nothing, counts a block as settled once
    /// its hold cap passed.
    fn fence(&self, st: &mut State, tai: u64) {
        let t = self.timing;
        if st.members.configs.is_empty() {
            return;
        }
        let w = t.contact_window.as_nanos() as u64;
        let guard = time_of(t.clock_guard);
        let cap = (t.hold + t.contact_window).as_nanos() as u64;
        let mut k = match st.fence {
            Some(f) => f / t.block,
            // Below every block an output's hold ring can still hold.
            None => {
                let due = index_at(tai).saturating_sub(t.margin) / t.block;
                due.saturating_sub(2 * index_at(cap + w).div_ceil(t.block) + 4)
            }
        };
        let echoed = |st: &State, v: NodeId, k: u64| -> Option<u64> {
            if v == self.id {
                return Some(tai);
            }
            let void_from = t.block_deadline(k).saturating_sub(guard);
            st.peers
                .get(&v)?
                .echoes
                .iter()
                .filter(|(_, s)| s.is_none_or(|s| s < void_from))
                .map(|&(t1, _)| t1)
                .max()
        };
        // No echo is newer than now: a block ending more than w ahead is
        // never valid, so the scan ends there whatever `settled` says.
        let end = index_at(tai + w) / t.block;
        while k <= end {
            let start = time_of((k + 1) * t.block).saturating_sub(w);
            let valid = st
                .members
                .majority(|v| echoed(st, v, k).is_some_and(|t1| t1 >= start));
            let settled = match &self.audio {
                Some(audio) => !valid && audio.settled(k),
                None => t.block_deadline(k) + cap < tai,
            };
            if !valid && !settled {
                break;
            }
            k += 1;
        }
        let f = k * t.block;
        if st.fence != Some(f) {
            st.fence = Some(f);
            if let Some(audio) = &self.audio {
                audio.fence(f);
            }
        }
    }

    /// Drops scheduled entries every voter reported committed.
    fn prune_scheduled(&self, st: &mut State) {
        let lowest = st
            .members
            .voters()
            .iter()
            .map(|&v| {
                if v == self.id {
                    st.known_committed
                } else {
                    st.peers
                        .get(&v)
                        .and_then(|p| p.last.as_ref())
                        .and_then(|d| d.committed)
                        .map_or(0, |c| c.index)
                }
            })
            .min()
            .unwrap_or(0);
        st.scheduled = st.scheduled.split_off(&(lowest + 1));
    }

    /// Reports from a majority of voters, itself included, within w, as
    /// of the last report period.
    pub fn in_contact(&self) -> bool {
        self.lock().in_contact
    }

    /// Whether every other voter heard directly within `within` reports a
    /// committed log index at or above `index`.
    pub fn heard_committed(&self, index: u64, within: Duration) -> bool {
        let now = Instant::now();
        let st = self.lock();
        st.peers.values().all(|p| {
            p.last.as_ref().is_none_or(|d| {
                since(now, d.received) > within || d.committed.is_some_and(|c| c.index >= index)
            })
        })
    }

    pub fn metrics(&self) -> Metrics {
        let st = self.lock();
        let c = &st.counts;
        Metrics {
            fence: st.fence,
            in_contact: st.in_contact,
            known_committed: st.known_committed,
            peers: st
                .peers
                .iter()
                .map(|(&node, p)| PeerMetrics {
                    node,
                    reports: p.reports,
                    delay_p9999: p.delay.quantile(0.9999).map(Duration::from_nanos),
                    gap_max: Duration::from_nanos(p.gaps.max()),
                    gaps_over_window: p.gaps_over_window,
                    silent: p.silent,
                    offset_ns: p.offset_ns,
                })
                .collect(),
            shared_gaps: c.shared_gaps,
            shared_gap_max: c.shared_gap_max,
            stagger_max: c.stagger_max,
            status_lag_p9999: c.status_lag.quantile(0.9999).map(Duration::from_nanos),
            status_changes_lost: st.status_changes_lost,
            unresolved: c.unresolved,
            unresolved_refused: c.unresolved_refused,
            awaiting_overflow: c.awaiting_overflow,
            scheduled_dropped: c.scheduled_dropped,
        }
    }
}

/// Runs contact for `contact` until the replica shuts down: a report to
/// every other voter each period, each sent as soon as the one before it
/// to the same voter was answered, a newer one replacing one not yet
/// sent.
pub(crate) async fn run(shared: Arc<Shared>, contact: Arc<Contact>) {
    let mut metrics = shared.raft.metrics();
    let mut tick = tokio::time::interval(contact.timing.report_period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut senders: BTreeMap<NodeId, watch::Sender<Option<(String, Report)>>> = BTreeMap::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let members = Members::of(&metrics.borrow());
                let voters = members.voters();
                senders.retain(|id, _| voters.contains(id));
                for (to, addr, report) in contact.tick(members) {
                    senders
                        .entry(to)
                        .or_insert_with(|| send(shared.clone(), to, contact.timing.contact_window))
                        .send_replace(Some((addr, report)));
                }
            }
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
                contact.log_changed();
            }
        }
    }
}

fn send(
    shared: Arc<Shared>,
    to: NodeId,
    within: Duration,
) -> watch::Sender<Option<(String, Report)>> {
    let (tx, mut rx) = watch::channel(None::<(String, Report)>);
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let next = rx.borrow_and_update().clone();
            if let Some((addr, report)) = next {
                let request = PeerRequest::Report(Box::new(report));
                let _ = shared.peers.call(Some(to), &addr, request, within).await;
            }
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::report::ClockFlags;

    const B: u64 = 48;
    const T0: u64 = 1_000_000_000_000_000;

    /// A TAI clock a test sets; each read moves it on by `step` ns.
    struct Clock {
        now: AtomicU64,
        step: AtomicU64,
    }

    impl Clocks for Clock {
        fn tai_ns(&self) -> u64 {
            self.now
                .fetch_add(self.step.load(Ordering::SeqCst), Ordering::SeqCst)
        }
    }

    impl Clock {
        fn set(&self, ns: u64) {
            self.now.store(ns, Ordering::SeqCst);
        }
        fn add(&self, d: Duration) {
            self.now.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
        }
        fn get(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    #[derive(Default)]
    struct LogInner {
        log: BTreeMap<u64, (StoredLogId, Request)>,
        committed: Option<StoredLogId>,
        applied: u64,
        pending: Vec<(u64, Body)>,
    }

    #[derive(Default)]
    struct Log(Mutex<LogInner>);

    impl Log {
        fn push(&self, term: u64, index: u64, at_index: Option<u64>) {
            let body = match at_index {
                Some(at_index) => Body::InputDown { slot: 0, at_index },
                None => Body::Commands(Vec::new()),
            };
            self.0
                .lock()
                .unwrap()
                .log
                .insert(index, (id(term, index), Request { stamp: 0, body }));
        }
    }

    impl LogView for Log {
        fn committed(&self) -> Option<StoredLogId> {
            self.0.lock().unwrap().committed
        }
        fn applied(&self) -> u64 {
            self.0.lock().unwrap().applied
        }
        fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
            self.0.lock().unwrap().log.get(&index).map(|(id, _)| *id)
        }
        fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request)) {
            for (id, r) in self
                .0
                .lock()
                .unwrap()
                .log
                .range(index + 1..)
                .map(|(_, e)| e)
            {
                f(*id, r);
            }
        }
        fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
            for (index, body) in &self.0.lock().unwrap().pending {
                f(*index, body);
            }
        }
    }

    /// Records what contact hands the engine. Its step flag shows once
    /// the clock reads `step_at` or later.
    struct Engine {
        clock: Arc<Clock>,
        step_at: AtomicU64,
        fence: AtomicU64,
        calls: Mutex<Vec<String>>,
        /// Unresolved entries it takes before it refuses.
        room: AtomicU64,
    }

    impl Audio for Engine {
        fn sample(&self) -> Sample {
            let step = self.clock.tai_ns() >= self.step_at.load(Ordering::SeqCst);
            Sample {
                own: Own {
                    processed: Some(1),
                    clock: ClockFlags {
                        step_fence: step,
                        ..ClockFlags::default()
                    },
                    ..Own::default()
                },
                status_changes_lost: 0,
            }
        }
        fn fence(&self, end: u64) {
            self.fence.store(end, Ordering::SeqCst);
        }
        fn settled(&self, _: u64) -> bool {
            false
        }
        fn appended(&self, index: u64, _: &Request) {
            self.calls.lock().unwrap().push(format!("appended {index}"));
        }
        fn truncated(&self, from: u64) {
            self.calls.lock().unwrap().push(format!("truncated {from}"));
        }
        fn committed(&self, up_to: u64) {
            self.calls
                .lock()
                .unwrap()
                .push(format!("committed {up_to}"));
        }
        fn unresolved(&self, index: u64, at_index: u64) -> bool {
            if self.room.load(Ordering::SeqCst) == 0 {
                self.calls.lock().unwrap().push(format!("refused {index}"));
                return false;
            }
            self.room.fetch_sub(1, Ordering::SeqCst);
            let call = format!("unresolved {index} at {at_index}");
            self.calls.lock().unwrap().push(call);
            true
        }
        fn skip(&self, index: u64) {
            self.calls.lock().unwrap().push(format!("skip {index}"));
        }
        fn overwhelmed(&self) {
            self.calls.lock().unwrap().push("overwhelmed".into());
        }
    }

    impl Engine {
        fn calls(&self) -> Vec<String> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }

    fn id(term: u64, index: u64) -> StoredLogId {
        StoredLogId {
            term,
            node: 1,
            index,
        }
    }

    struct Fixture {
        clock: Arc<Clock>,
        log: Arc<Log>,
        engine: Arc<Engine>,
        contact: Contact,
        members: Members,
    }

    fn fixture(configs: &[&[NodeId]]) -> Fixture {
        let clock = Arc::new(Clock {
            now: AtomicU64::new(T0),
            step: AtomicU64::new(0),
        });
        let log = Arc::new(Log::default());
        let engine = Arc::new(Engine {
            clock: clock.clone(),
            step_at: AtomicU64::new(u64::MAX),
            fence: AtomicU64::new(0),
            calls: Mutex::new(Vec::new()),
            room: AtomicU64::new(u64::MAX),
        });
        let contact = Contact::new(
            1,
            Timing::defaults(B),
            clock.clone(),
            Some(engine.clone()),
            log.clone(),
            100,
        );
        let configs: Vec<BTreeSet<NodeId>> = configs
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect();
        let addrs = configs
            .iter()
            .flatten()
            .map(|&v| (v, v.to_string()))
            .collect();
        Fixture {
            clock,
            log,
            engine,
            contact,
            members: Members { configs, addrs },
        }
    }

    impl Fixture {
        fn tick(&self) -> Vec<(NodeId, String, Report)> {
            self.contact.tick(self.members.clone())
        }

        /// A report from `from` echoing seq `echo` received at `at`.
        fn report(&self, from: NodeId, echo: Option<(u64, u64)>) -> Report {
            Report {
                from,
                seq: self.clock.get(),
                sent_at: self.clock.get(),
                echo: echo.map(|(seq, received_at)| Echo {
                    seq,
                    received_at,
                    silent_at: None,
                }),
                committed: None,
                own: Own::default(),
                heard: Vec::new(),
                listed: Vec::new(),
                unlisted: None,
            }
        }

        fn fence(&self) -> u64 {
            self.engine.fence.load(Ordering::SeqCst)
        }

        fn silent(&self, node: NodeId) -> bool {
            let m = self.contact.metrics();
            m.peers.iter().any(|p| p.node == node && p.silent)
        }
    }

    const W: Duration = Duration::from_millis(8);

    fn w_ns() -> u64 {
        W.as_nanos() as u64
    }

    #[tokio::test(start_paused = true)]
    async fn f_ends_at_the_blocks_a_majority_echoed_within_w() {
        let f = fixture(&[&[1, 2, 3]]);
        f.members.voters();
        let sent = f.tick();
        let seq = sent[0].2.seq;
        assert!(
            f.fence() < index_at(T0) - 576,
            "no echo yet: every block not yet due is fenced"
        );
        f.clock.add(Duration::from_millis(3));
        f.contact.receive(f.report(2, Some((seq, T0 + 1_000_000))));
        // Me and 2 are a majority of three: blocks ending at or before
        // the echoed send time plus w pass.
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
        // Time alone moves nothing: without newer echoes F stands.
        f.clock.add(Duration::from_millis(50));
        tokio::time::advance(Duration::from_millis(50)).await;
        f.tick();
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn an_echo_stamped_silent_does_not_count_for_the_blocks_it_voids() {
        let f = fixture(&[&[1, 2, 3]]);
        let timing = Timing::defaults(B);
        let guard = time_of(timing.clock_guard);
        let first = f.tick()[0].2.seq;
        f.contact.receive(f.report(2, Some((first, T0))));
        let before = f.fence();
        assert_eq!(before, index_at(T0 + w_ns()) / B * B);
        // 40 ms later 2 echoes a newer report, stamped with the time it
        // last held this replica silent, 30 ms after the first report.
        f.clock.add(Duration::from_millis(40));
        tokio::time::advance(Duration::from_millis(40)).await;
        let second = f.tick()[0].2.seq;
        let silent_at = T0 + 30_000_000;
        let mut r = f.report(2, None);
        r.echo = Some(Echo {
            seq: second,
            received_at: f.clock.get(),
            silent_at: Some(silent_at),
        });
        f.contact.receive(r);
        // Blocks due up to silentAt + clockGuard stay fenced: the first
        // echo is too old for them, the second is voided for them.
        let f_after = f.fence();
        let k = f_after / B;
        assert!(
            timing.block_deadline(k) <= silent_at + guard,
            "F skipped a voided block"
        );
        assert!(f_after >= before);
        // 3 echoes the second report without a silentAt: a majority.
        f.contact
            .receive(f.report(3, Some((second, f.clock.get()))));
        assert_eq!(f.fence(), index_at(T0 + 40_000_000 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn a_joint_change_needs_a_majority_of_each_configuration() {
        let f = fixture(&[&[1, 2, 3], &[1, 4, 5]]);
        let seq = f.tick()[0].2.seq;
        f.contact.receive(f.report(2, Some((seq, T0))));
        assert!(
            f.fence() < index_at(T0) - 576,
            "the second configuration echoed nothing"
        );
        f.contact.receive(f.report(4, Some((seq, T0))));
        assert_eq!(f.fence(), index_at(T0 + w_ns()) / B * B);
    }

    #[tokio::test(start_paused = true)]
    async fn silence_counts_from_regaining_contact_and_is_echoed() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        f.contact.receive(f.report(2, None));
        f.contact.receive(f.report(3, None));
        f.tick();
        // A gap every link shares: out of contact, nobody is held silent.
        tokio::time::advance(Duration::from_millis(20)).await;
        f.clock.add(Duration::from_millis(20));
        f.tick();
        assert!(!f.silent(2) && !f.silent(3));
        assert!(!f.contact.metrics().in_contact);
        // 2 comes back first, 3 six ms later: 3 was not heard for 26 ms,
        // but this replica regained contact only 6 ms before.
        for _ in 0..3 {
            f.contact.receive(f.report(2, None));
            f.tick();
            assert!(!f.silent(3));
            tokio::time::advance(Duration::from_millis(2)).await;
            f.clock.add(Duration::from_millis(2));
        }
        f.contact.receive(f.report(3, None));
        f.tick();
        assert!(!f.silent(3), "a stagger below w holds nobody silent");
        // 3 then stays away for longer than w.
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(2)).await;
            f.clock.add(Duration::from_millis(2));
            f.contact.receive(f.report(2, None));
            f.tick();
        }
        assert!(f.silent(3));
        let to_3 = f.tick().into_iter().find(|(to, _, _)| *to == 3).unwrap();
        let echo = to_3.2.echo.expect("3 was heard");
        assert_eq!(
            echo.silent_at,
            Some(to_3.2.sent_at),
            "silentAt names this report"
        );
        let to_2 = f.tick().into_iter().find(|(to, _, _)| *to == 2).unwrap();
        let heard_3 = to_2.2.heard.iter().find(|h| h.node == 3).unwrap();
        assert!(heard_3.silent, "the silence is relayed");
    }

    #[tokio::test(start_paused = true)]
    async fn a_committed_id_the_log_holds_commits_everything_up_to_it() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        for index in 1..=5 {
            f.log.push(1, index, None);
        }
        f.contact.log_changed();
        assert_eq!(f.engine.calls().len(), 5, "five appends");
        // Another term at index 5: not this log's entry.
        let mut r = f.report(2, None);
        r.committed = Some(id(2, 5));
        f.contact.receive(r);
        assert!(f.engine.calls().is_empty());
        let mut r = f.report(3, None);
        r.committed = Some(id(1, 4));
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 4"]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_exact_listed_id_below_the_committed_index_is_committed() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let at = index_at(T0) + 4800;
        for index in 1..=5 {
            f.log.push(1, index, Some(at));
        }
        f.contact.log_changed();
        f.engine.calls();
        // The sender's committed id lies past this log's end.
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 9));
        f.contact.receive(r.clone());
        assert!(f.engine.calls().is_empty(), "nothing this log holds");
        r.seq += 1;
        r.listed = vec![Listed {
            id: id(2, 5),
            at_index: at,
        }];
        f.contact.receive(r.clone());
        assert!(f.engine.calls().is_empty(), "another entry at index 5");
        r.seq += 1;
        r.listed = vec![Listed {
            id: id(1, 5),
            at_index: at,
        }];
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 5"]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_whose_commit_is_unknown_at_its_deadline_is_unresolved() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let timing = Timing::defaults(B);
        // Three entries 40 ms ahead: one learned committed in time, one
        // only after its deadline, one never.
        let at = index_at(T0) + 40 * 48;
        for index in 1..=3 {
            f.log.push(1, index, Some(at + index * B));
        }
        f.contact.log_changed();
        f.engine.calls();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 1));
        f.contact.receive(r);
        assert_eq!(f.engine.calls(), ["committed 1"]);
        f.clock.set(timing.apply_deadline(at + 2 * B) - 1);
        f.tick();
        assert!(f.engine.calls().is_empty(), "before the deadline");
        f.clock.set(timing.apply_deadline(at + 2 * B));
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 2));
        f.contact.receive(r);
        assert_eq!(
            f.engine.calls(),
            [
                "skip 2".into(),
                format!("unresolved 2 at {}", at + 2 * B),
                "committed 2".into()
            ],
            "a commit learned at the deadline is late: entry 2 is never applied"
        );
        f.clock.set(timing.apply_deadline(at + 3 * B));
        f.tick();
        assert_eq!(
            f.engine.calls(),
            [
                "skip 3".to_owned(),
                format!("unresolved 3 at {}", at + 3 * B)
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_listed_entry_this_log_lacks_is_unresolved_at_its_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 7),
            at_index: at,
        }];
        r.unlisted = Some(at + B);
        f.contact.receive(r);
        f.clock.set(timing.apply_deadline(at));
        f.tick();
        assert_eq!(f.engine.calls(), [format!("unresolved 7 at {at}")]);
        f.clock.set(timing.apply_deadline(at + B));
        f.tick();
        let marker = unlisted_id(at + B, B);
        assert_ne!(marker, u64::MAX, "the engine's own reserved id");
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved {marker} at {}", at + B)]
        );
        // A later overflow at a higher index adds nothing; one below the
        // lowest given is given again.
        for (n, unlisted) in [(1, at + 2 * B), (2, at)] {
            let mut r = f.report(2, None);
            r.seq += n;
            r.unlisted = Some(unlisted);
            f.contact.receive(r);
        }
        assert_eq!(
            f.engine.calls(),
            [format!("unresolved {} at {at}", unlisted_id(at, B))]
        );
        f.clock.set(timing.apply_deadline(at + 2 * B));
        f.tick();
        assert!(
            f.engine.calls().is_empty(),
            "the overflow above the lowest given"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_listing_holds_the_64_earliest_entries_above_the_addressee_commit() {
        let f = fixture(&[&[1, 2]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        let now = index_at(T0);
        for index in 1..=80 {
            // Descending at_index; index 80 lies past the listing horizon.
            let at = if index == 80 {
                now + 60 * 48
            } else {
                now + (90 - index) * 30
            };
            f.log.push(1, index, Some(at));
        }
        f.contact.log_changed();
        let mut r = f.report(2, None);
        r.committed = Some(id(1, 3));
        f.contact.receive(r);
        let (_, _, report) = f.tick().pop().unwrap();
        let indices: Vec<u64> = report.listed.iter().map(|l| l.id.index).collect();
        let expected: Vec<u64> = (16..=79).rev().collect();
        assert_eq!(indices, expected, "the earliest at_index first, 64 of them");
        assert_eq!(report.unlisted, Some(now + (90 - 15) * 30));
    }

    #[tokio::test(start_paused = true)]
    async fn exchange_timestamps_never_precede_the_flags_beside_them() {
        let f = fixture(&[&[1, 2]]);
        // Reports from voters count once a period named the voters.
        f.tick();
        f.clock.step.store(1, Ordering::SeqCst);
        f.contact.receive(f.report(2, None));
        // The engine detects a step two reads on: after the period's own
        // timestamp and its sample of the engine, so the first report's
        // flags say nothing yet and none of its timestamps may postdate
        // the step.
        f.engine.step_at.store(f.clock.get() + 2, Ordering::SeqCst);
        for _ in 0..3 {
            let (_, _, r) = f.tick().pop().unwrap();
            let step_at = f.engine.step_at.load(Ordering::SeqCst);
            let echo = r.echo.unwrap();
            for t in [r.sent_at, echo.received_at] {
                assert!(
                    t < step_at || r.own.clock.step_fence,
                    "a timestamp taken at {t}, after the step at {step_at}, without its flag"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_truncated_entry_is_taken_back_and_its_successor_handed() {
        let f = fixture(&[&[1, 2, 3]]);
        for index in 1..=3 {
            f.log.push(1, index, None);
        }
        f.contact.log_changed();
        f.engine.calls();
        f.log.push(2, 3, None);
        f.contact.log_changed();
        assert_eq!(f.engine.calls(), ["truncated 3", "appended 3"]);
    }

    /// A stale listing at an index must not hide the deadline of the
    /// entry that index holds in the set's log.
    #[tokio::test(start_paused = true)]
    async fn review_two_listings_at_one_index_each_keep_their_deadline() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let early = index_at(T0) + 40 * 48;
        let late = early + 400 * 48;
        // A deposed leader's uncommitted entry at index 5 listed first,
        // then the entry the set committed there, with an earlier index.
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 5),
            at_index: late,
        }];
        f.contact.receive(r);
        let mut r = f.report(3, None);
        r.listed = vec![Listed {
            id: id(2, 5),
            at_index: early,
        }];
        f.contact.receive(r);
        f.engine.calls();
        f.clock.set(timing.apply_deadline(early));
        f.tick();
        let calls = f.engine.calls();
        assert!(
            calls.contains(&format!("unresolved 5 at {early}")),
            "entry (2, 5) passed its deadline unknown, not unresolved: {calls:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_truncated_entry_takes_its_deadline_with_it() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let early = index_at(T0) + 40 * 48;
        f.log.push(1, 1, Some(early));
        f.contact.log_changed();
        // Replaced by another leader's entry, far ahead.
        f.log.push(2, 1, Some(early + 4000 * B));
        f.contact.log_changed();
        f.engine.calls();
        f.clock.set(timing.apply_deadline(early));
        f.tick();
        assert!(f.engine.calls().is_empty(), "the cut entry's deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn an_entry_without_room_withdraws_once_and_is_not_offered_again() {
        let f = fixture(&[&[1, 2, 3]]);
        f.tick();
        let timing = Timing::defaults(B);
        let at = index_at(T0) + 40 * 48;
        f.engine.room.store(0, Ordering::SeqCst);
        let mut r = f.report(2, None);
        r.listed = vec![Listed {
            id: id(1, 9),
            at_index: at,
        }];
        f.contact.receive(r);
        f.clock.set(timing.apply_deadline(at));
        f.tick();
        assert_eq!(f.engine.calls(), ["refused 9", "overwhelmed"]);
        f.engine.room.store(10, Ordering::SeqCst);
        for _ in 0..3 {
            f.tick();
        }
        assert!(f.engine.calls().is_empty(), "offered again");
        assert_eq!(f.contact.metrics().unresolved_refused, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_report_older_than_the_newest_taken_is_ignored() {
        let f = fixture(&[&[1, 2, 3]]);
        let first = f.tick()[0].2.seq;
        f.clock.add(Duration::from_millis(40));
        tokio::time::advance(Duration::from_millis(40)).await;
        let second = f.tick()[0].2.seq;
        let mut newer = f.report(2, Some((second, f.clock.get())));
        newer.seq = 1000;
        f.contact.receive(newer);
        let fence = f.fence();
        // The older report, delayed, echoes the older seq.
        let mut older = f.report(2, Some((first, T0)));
        older.seq = 999;
        f.contact.receive(older);
        let (_, _, to_2) = f.tick().into_iter().find(|(to, _, _)| *to == 2).unwrap();
        assert_eq!(
            to_2.echo.unwrap().seq,
            1000,
            "the newer report is the one echoed"
        );
        assert_eq!(f.fence(), fence);
    }

    /// The log's view holds its lock while it walks the pending entries,
    /// as the store does: a replica started with one lists it without
    /// re-entering the view.
    #[tokio::test(start_paused = true)]
    async fn a_replica_started_with_pending_entries_lists_them() {
        let f = fixture(&[&[1, 2]]);
        let at = index_at(T0) + 20 * B;
        f.log.push(1, 3, Some(at));
        {
            let mut log = f.log.0.lock().unwrap();
            log.applied = 3;
            log.pending.push((
                3,
                Body::InputDown {
                    slot: 0,
                    at_index: at,
                },
            ));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let (clock, engine, log) = (f.clock.clone(), f.engine.clone(), f.log.clone());
        std::thread::spawn(move || {
            let contact = Contact::new(1, Timing::defaults(B), clock, Some(engine), log, 1);
            let _ = tx.send(contact);
        });
        let contact = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Contact::new deadlocked on the log's view");
        let (_, _, report) = contact.tick(f.members.clone()).pop().unwrap();
        assert_eq!(
            report.listed,
            [Listed {
                id: id(1, 3),
                at_index: at
            }]
        );
    }
}
