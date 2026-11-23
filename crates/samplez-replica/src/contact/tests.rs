use std::sync::atomic::{AtomicU64, Ordering};

use std::sync::Mutex;

use super::*;
use crate::report::{ClockFlags, Page};

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
    fn tai_offset_s(&self) -> Option<i64> {
        Some(37)
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
    installs: u64,
    /// [`LogView::pending_generation`].
    generation: Option<u64>,
    /// Records by log index, read up to the applied index.
    records: BTreeMap<u64, Arc<Record>>,
    records_from: u64,
    /// The floor of the applied state.
    floor: u64,
    /// The applied index and floor an apply sets right after the next
    /// read of the applied index.
    apply_after_read: Option<(u64, u64)>,
    /// [`LogView::baseline`].
    baseline: Option<Baseline>,
    /// The most entries one read of the log went through.
    visited_most: usize,
    /// The flows of [`LogView::product_state`].
    flows: Vec<Option<String>>,
    /// The outputs of [`LogView::product_state`].
    outputs: Vec<bool>,
}

#[derive(Default)]
struct Log(Mutex<LogInner>);

impl Log {
    /// An entry, an `InputDown` at `at_index` that derives its
    /// record once applied.
    fn push(&self, term: u64, index: u64, at_index: Option<u64>) {
        let body = match at_index {
            Some(at_index) => Body::InputDown { slot: 0, at_index },
            None => Body::Commands(Vec::new()),
        };
        let mut log = self.0.lock().unwrap();
        let stamp = at_index.unwrap_or(0);
        log.log
            .insert(index, (id(term, index), Request { stamp, body }));
        if let Some(at_index) = at_index {
            let entries = vec![Entry::InputDown { slot: 0 }];
            let record = Record {
                at_index,
                entries,
                generation: false,
            };
            log.records.insert(index, Arc::new(record));
        }
    }
}

impl LogView for Log {
    fn committed(&self) -> Option<StoredLogId> {
        self.0.lock().unwrap().committed
    }
    fn applied(&self) -> u64 {
        let mut log = self.0.lock().unwrap();
        let applied = log.applied;
        if let Some((index, floor)) = log.apply_after_read.take() {
            (log.applied, log.floor) = (index, floor);
        }
        applied
    }
    fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
        self.0.lock().unwrap().log.get(&index).map(|(id, _)| *id)
    }
    fn entries_after(&self, index: u64, f: &mut dyn FnMut(StoredLogId, &Request)) {
        self.entries_after_max(index, usize::MAX, f);
    }
    fn entries_after_max(
        &self,
        index: u64,
        max: usize,
        f: &mut dyn FnMut(StoredLogId, &Request),
    ) -> bool {
        let mut log = self.0.lock().unwrap();
        let mut visited = 0;
        let mut entries = log.log.range(index + 1..).map(|(_, e)| e);
        for (id, r) in entries.by_ref().take(max) {
            visited += 1;
            f(*id, r);
        }
        let more = entries.next().is_some();
        log.visited_most = log.visited_most.max(visited);
        more
    }
    fn pending(&self, f: &mut dyn FnMut(u64, &Body)) {
        for (index, body) in &self.0.lock().unwrap().pending {
            f(*index, body);
        }
    }
    fn latest_judged(&self) -> Option<NodeId> {
        None
    }
    fn judged(&self) -> BTreeSet<NodeId> {
        BTreeSet::new()
    }
    fn baseline(&self) -> Option<Baseline> {
        self.0.lock().unwrap().baseline.clone()
    }
    fn disrupting(&self) -> Option<NodeId> {
        None
    }
    fn installs(&self) -> u64 {
        self.0.lock().unwrap().installs
    }
    fn pending_generation(&self) -> Option<u64> {
        self.0.lock().unwrap().generation
    }
    fn records(&self, after: u64, up_to: u64, f: &mut dyn FnMut(u64, &Arc<Record>)) -> bool {
        let log = self.0.lock().unwrap();
        if after < up_to {
            for (index, record) in log.records.range(after + 1..=up_to) {
                f(*index, record);
            }
        }
        after >= log.records_from
    }
    fn records_from(&self) -> u64 {
        self.0.lock().unwrap().records_from
    }
    fn floor_before(&self, index: u64) -> u64 {
        let log = self.0.lock().unwrap();
        let mut floor = log.floor;
        let end = index.max(log.applied + 1);
        for (_, request) in log.log.range(log.applied + 1..end).map(|(_, e)| e) {
            if let Some(at) = crate::state_machine::floored(floor, request) {
                floor = at;
            }
        }
        floor
    }
    fn applied_floor(&self) -> (u64, u64) {
        let log = self.0.lock().unwrap();
        (log.applied, log.floor)
    }
    fn pending_above(&self, floor: u64) -> usize {
        let log = self.0.lock().unwrap();
        let records = (log.records.values())
            .filter(|r| r.at_index > floor)
            .count();
        let mut raised = log.floor;
        let mut unapplied = 0;
        for (_, request) in log.log.range(log.applied + 1..).map(|(_, e)| e) {
            if let Some(at) = crate::state_machine::floored(raised, request) {
                raised = at;
                unapplied += usize::from(at > floor);
            }
        }
        records + unapplied
    }
    fn product_state(&self) -> (u64, u64, ProductState) {
        let log = self.0.lock().unwrap();
        let state = ProductState {
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            gains: Vec::new(),
            up: Vec::new(),
            flows: log.flows.clone(),
            outputs: log.outputs.clone(),
            pre_strip_meters: Vec::new(),
            loudness_running: Vec::new(),
            strips: Vec::new(),
            ducks: Vec::new(),
            masks: Vec::new(),
            in_flight: Default::default(),
            pending: Vec::new(),
            fading: Vec::new(),
            last_id: log.applied,
            ..Default::default()
        };
        (log.applied, log.floor, state)
    }
    fn config_hash(&self) -> Option<u64> {
        Some(1)
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
    /// What each append carried.
    requests: Mutex<Vec<(u64, u64, Vec<Entry>)>>,
    /// Where each append's entries lie in memory.
    handed_from: Mutex<Vec<usize>>,
    /// Whether a rebuild succeeds.
    rebuilds: std::sync::atomic::AtomicBool,
    /// A rebuild waits while set, as for the RT thread, 1 s at most.
    holding: std::sync::atomic::AtomicBool,
    /// Each rebuild's state's last log id and the index it installs
    /// at or after.
    rebuilt_with: Mutex<Vec<(u64, u64)>>,
    /// Whether it reports outputs, withdrawn: one, two with `second`.
    outputs: std::sync::atomic::AtomicBool,
    second: std::sync::atomic::AtomicBool,
    /// The outputs disabled, a bit per output.
    disabled: AtomicU64,
    /// The build time it reports, in samples; none at 0.
    build_time: AtomicU64,
    /// [`Audio::follows`].
    follows: std::sync::atomic::AtomicBool,
    /// Its outputs stand committing, not withdrawn.
    committing: std::sync::atomic::AtomicBool,
    /// The last [`Audio::diverged`].
    apart: std::sync::atomic::AtomicBool,
    /// The page records it reports.
    pages: Mutex<Vec<Page>>,
}

impl Engine {
    /// Page `page` of output 0 is no longer reported, and no other
    /// comes in its place.
    fn drop_page(&self, page: u64) {
        let mut pages = self.pages.lock().unwrap();
        pages.retain(|p| (p.output, p.page) != (0, page));
    }

    /// Page `page`'s records get another hash.
    fn rehash(&self, page: u64) {
        for p in self.pages.lock().unwrap().iter_mut() {
            if p.page == page {
                p.hash += 1;
            }
        }
    }

    /// Page `page` of outputs 0 and 1 is final: it reports it, and the
    /// latest [`PAGES_REPORTED`] of each output.
    fn page(&self, page: u64) {
        let mut pages = self.pages.lock().unwrap();
        for output in 0..2 {
            pages.push(Page {
                output,
                page,
                hash: page * 10 + u64::from(output),
                fingerprint: 1,
                committed: true,
                degraded: false,
                digest: None,
            });
            let of = pages.iter().filter(|p| p.output == output).count();
            if of > PAGES_REPORTED {
                let oldest = pages.iter().position(|p| p.output == output).unwrap();
                pages.remove(oldest);
            }
        }
    }

    /// The outputs it reports.
    fn reported(&self) -> usize {
        match (
            self.outputs.load(Ordering::SeqCst),
            self.second.load(Ordering::SeqCst),
        ) {
            (false, _) => 0,
            (true, false) => 1,
            (true, true) => 2,
        }
    }
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
                standing: vec![
                    if self.committing.load(Ordering::SeqCst) {
                        Standing::Committing { from: 0 }
                    } else {
                        Standing::Withdrawn { since: 0 }
                    };
                    self.reported()
                ],
                build_time: Some(self.build_time.load(Ordering::SeqCst)).filter(|&t| t > 0),
                pages: self.pages.lock().unwrap().clone(),
                ..Own::default()
            },
            status_changes_lost: 0,
            entries_refused: 0,
            entries_filtered: 0,
            realised: Vec::new(),
            enabled: (0..self.reported())
                .map(|o| self.disabled.load(Ordering::SeqCst) & 1 << o == 0)
                .collect(),
            generation_ready: true,
            clock_steps: 0,
            log_overflows: 0,
        }
    }
    fn fence(&self, end: u64) {
        self.fence.store(end, Ordering::SeqCst);
    }
    fn settled(&self, _: u64) -> bool {
        false
    }
    fn appended(&self, index: u64, at_index: u64, entries: &[Entry]) {
        self.calls.lock().unwrap().push(format!("appended {index}"));
        let call = (index, at_index, entries.to_vec());
        self.requests.lock().unwrap().push(call);
        let from = entries.as_ptr() as usize;
        self.handed_from.lock().unwrap().push(from);
    }
    fn resolved(&self, index: u64) {
        self.calls.lock().unwrap().push(format!("resolved {index}"));
    }
    fn drop_engine(&self) {
        self.calls.lock().unwrap().push("drop".into());
    }
    fn rebuild_engine(&self, state: &ProductState, at_least: u64) -> bool {
        let from = std::time::Instant::now();
        while self.holding.load(Ordering::SeqCst) && from.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(1));
        }
        let ok = self.rebuilds.load(Ordering::SeqCst);
        if ok {
            self.calls.lock().unwrap().push("rebuild".into());
            let call = (state.last_id, at_least);
            self.rebuilt_with.lock().unwrap().push(call);
        }
        ok
    }
    fn follows(&self, _state: &ProductState) -> bool {
        self.follows.load(Ordering::SeqCst)
    }
    fn diverged(&self, apart: bool) {
        self.apart.store(apart, Ordering::SeqCst);
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
    fn overwhelmed(&self) {
        self.calls.lock().unwrap().push("overwhelmed".into());
    }
    fn status(&self, _: usize, _: u64) -> Option<BlockStatus> {
        None
    }
    fn withhold(&self, _: usize, _: u64, _: Cause) -> Option<BlockStatus> {
        None
    }
    fn commit_through(&self, _: usize, _: u64, _: u8, _: Option<u64>) -> bool {
        false
    }
    fn withdraw(&self) {
        self.calls.lock().unwrap().push("withdraw".into());
    }
    fn ranks(&self, _: Ranks) {}
    fn commit_from(&self, _: usize, _: u64, _: u64) {}
    fn cancel_commit_start(&self, _: usize) {}
    fn stop_at(&self, _: usize, _: u64) {}
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

/// A busy contact lock stalls `enter_term` only: the metrics sender
/// and the readers of the metrics, the ops path among them, are not
/// queued behind a read guard held across the wait.
#[test]
fn a_busy_contact_lock_does_not_block_the_metrics_watch() {
    use std::sync::mpsc;
    const BOUND: Duration = Duration::from_secs(1);
    let f = fixture(&[&[1, 2, 3]]);
    let contact = Arc::new(f.contact);
    let (tx, rx) = watch::channel(RaftMetrics::new_initial(1));
    let (hold, release) = mpsc::channel::<()>();
    let (held_tx, held) = mpsc::channel::<()>();
    let holder = {
        let contact = contact.clone();
        std::thread::spawn(move || {
            let _st = contact.lock();
            held_tx.send(()).unwrap();
            let _ = release.recv();
        })
    };
    held.recv().unwrap();
    // The ticking thread names itself, then runs `enter_term`, whose
    // only blocking point is the contact lock. Its kernel state turns
    // to sleeping once it waits there, holding the metrics read guard
    // when the guard is not released first.
    let (named_tx, named) = mpsc::channel::<String>();
    let ticking = {
        let (contact, rx) = (contact.clone(), rx.clone());
        std::thread::spawn(move || {
            let me = std::fs::read_link("/proc/thread-self").expect("linux procfs");
            named_tx.send(me.to_string_lossy().into_owned()).unwrap();
            enter_term(&contact, &rx)
        })
    };
    let task = named.recv().unwrap();
    let tid = task.rsplit('/').next().unwrap().to_owned();
    let stat = format!("/proc/self/task/{tid}/stat");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let line = std::fs::read_to_string(&stat).unwrap_or_default();
        // `pid (comm) S ...`: the state follows the last parenthesis.
        let state = line
            .rsplit(')')
            .next()
            .and_then(|r| r.trim().chars().next());
        if state == Some('S') {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the ticking thread never waited on the contact lock"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let (sent_tx, sent) = mpsc::channel();
    let sender = std::thread::spawn(move || {
        let mut m = RaftMetrics::new_initial(1);
        m.current_term = 9;
        tx.send(m).unwrap();
        sent_tx.send(()).unwrap();
    });
    // The sender has to be a queued writer before the reader arrives;
    // it has no observable state, so the wait is a lower bound only: a
    // reader first in line passes against either code.
    std::thread::sleep(Duration::from_millis(300));
    let (read_tx, read) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let term = rx.borrow().clone().current_term;
        read_tx.send(term).unwrap();
    });
    let sent_ok = sent.recv_timeout(BOUND);
    let read_ok = read.recv_timeout(BOUND);
    hold.send(()).unwrap();
    holder.join().unwrap();
    ticking.join().unwrap();
    sender.join().unwrap();
    reader.join().unwrap();
    assert!(sent_ok.is_ok(), "a metrics send waited on the contact lock");
    assert!(read_ok.is_ok(), "a metrics read waited on the contact lock");
}

fn fixture(configs: &[&[NodeId]]) -> Fixture {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let log = Arc::new(Log::default());
    let engine = engine(&clock);
    let contact = Contact::new(
        1,
        Timing::defaults(B),
        clock.clone(),
        Some(engine.clone()),
        log.clone(),
        100,
    );
    contact.verified_always();
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

fn engine(clock: &Arc<Clock>) -> Arc<Engine> {
    Arc::new(Engine {
        clock: clock.clone(),
        step_at: AtomicU64::new(u64::MAX),
        fence: AtomicU64::new(0),
        calls: Mutex::new(Vec::new()),
        room: AtomicU64::new(u64::MAX),
        requests: Mutex::new(Vec::new()),
        handed_from: Mutex::new(Vec::new()),
        rebuilds: std::sync::atomic::AtomicBool::new(true),
        holding: std::sync::atomic::AtomicBool::new(false),
        rebuilt_with: Mutex::new(Vec::new()),
        outputs: std::sync::atomic::AtomicBool::new(false),
        second: std::sync::atomic::AtomicBool::new(false),
        disabled: AtomicU64::new(0),
        build_time: AtomicU64::new(0),
        follows: std::sync::atomic::AtomicBool::new(true),
        committing: std::sync::atomic::AtomicBool::new(false),
        apart: std::sync::atomic::AtomicBool::new(false),
        pages: Mutex::new(Vec::new()),
    })
}

impl Fixture {
    /// A tick, after which a rebuild it started has ended.
    fn tick(&self) -> Vec<(NodeId, String, Report)> {
        let reports = self.contact.tick(self.members.clone());
        self.contact.await_rebuild();
        reports
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
            estimate: None,
            slack: Vec::new(),
        }
    }

    fn fence(&self) -> u64 {
        self.engine.fence.load(Ordering::SeqCst)
    }

    /// A report from `from` naming product configuration `config`.
    fn config_report(&self, from: NodeId, config: Option<u64>) -> Report {
        // A later report than the one before.
        self.clock.add(Duration::from_millis(1));
        let mut r = self.report(from, None);
        r.own.config = config;
        r
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

/// Writes stop while a voter or a learner reports another product
/// configuration, and resume once it reports the set's or leaves the
/// membership; a report without one names none. A learner of another
/// configuration is not promoted.
#[tokio::test(start_paused = true)]
async fn a_member_of_another_configuration_stops_writes_until_it_matches_or_leaves() {
    let mut f = fixture(&[&[1, 2, 3]]);
    f.members.addrs.insert(9, "9".into());
    f.tick();
    assert_eq!(f.contact.config_differs(), None);
    assert!(!f.contact.learner_holds_set_config(9), "not heard from yet");
    f.contact.receive(f.config_report(2, None));
    assert_eq!(f.contact.config_differs(), None, "no hash names none");
    f.contact.receive(f.config_report(2, Some(5)));
    assert_eq!(f.contact.config_differs(), Some(2));
    f.contact.receive(f.config_report(2, Some(1)));
    assert_eq!(
        f.contact.config_differs(),
        None,
        "restarted under the set's"
    );
    f.contact.receive(f.config_report(9, Some(5)));
    assert_eq!(f.contact.config_differs(), Some(9), "a learner");
    assert!(!f.contact.learner_holds_set_config(9));
    assert!(!f.contact.learner_holds_set_config(2), "not a learner");
    f.contact.receive(f.config_report(9, Some(1)));
    assert!(f.contact.learner_holds_set_config(9));
    f.contact.receive(f.config_report(9, None));
    assert!(f.contact.learner_holds_set_config(9), "one recording none");
    f.contact.receive(f.config_report(9, Some(5)));
    f.contact.receive(f.config_report(3, Some(6)));
    f.members.addrs.remove(&9);
    f.members.configs = vec![[1, 2].into_iter().collect()];
    f.members.addrs.remove(&3);
    f.tick();
    assert_eq!(f.contact.config_differs(), None, "both left the membership");
    assert!(!f.contact.learner_holds_set_config(9));
}

/// Each tick counts one period, and each part of the contact loop
/// adds its thread's CPU time to its own part alone.
#[tokio::test(start_paused = true)]
async fn each_part_of_the_contact_loop_counts_its_own_cpu_time() {
    let f = fixture(&[&[1, 2, 3]]);
    assert_eq!(f.contact.costs(), CostCounts::default());
    f.tick();
    f.tick();
    let ticked = f.contact.costs();
    assert_eq!(ticked.ticks, 2);
    assert!(ticked.tick > Duration::ZERO);
    assert_eq!(
        (ticked.decisions, ticked.receive),
        (Duration::ZERO, Duration::ZERO)
    );
    f.contact.receive(f.report(2, None));
    let received = f.contact.costs();
    assert!(received.receive > Duration::ZERO);
    assert_eq!((received.ticks, received.tick), (2, ticked.tick));
    f.contact.decisions(1);
    let decided = f.contact.costs();
    assert!(decided.decisions > Duration::ZERO);
    assert_eq!(decided.receive, received.receive);
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
async fn the_report_age_counts_from_the_last_direct_report_not_from_regaining_contact() {
    let f = fixture(&[&[1, 2, 3]]);
    let age = |node: NodeId| {
        let m = f.contact.metrics();
        let p = m.peers.iter().find(|p| p.node == node).expect("a voter");
        p.last_report_age
    };
    f.tick();
    assert_eq!(age(2), None, "no report from 2 yet");
    assert_eq!(age(3), None, "no report from 3 yet");
    f.contact.receive(f.report(2, None));
    f.contact.receive(f.report(3, None));
    f.tick();
    // 3 stops; 2 reports every 2 ms until 3 is held silent, 10 ms on.
    for _ in 0..5 {
        tokio::time::advance(Duration::from_millis(2)).await;
        f.clock.add(Duration::from_millis(2));
        f.contact.receive(f.report(2, None));
        f.tick();
    }
    assert!(f.silent(3));
    assert!(age(3).is_some_and(|a| a >= Duration::from_millis(10)));
    // Then nothing for 20 ms: contact lost.
    tokio::time::advance(Duration::from_millis(20)).await;
    f.clock.add(Duration::from_millis(20));
    f.tick();
    assert!(!f.contact.metrics().in_contact);
    // 2 alone brings contact back, relaying a fresh copy of 3.
    let mut r = f.report(2, None);
    r.heard = vec![Heard {
        node: 3,
        age_us: Some(0),
        silent: false,
        own: Some(Own::default()),
    }];
    f.contact.receive(r);
    f.tick();
    assert!(f.contact.metrics().in_contact);
    assert!(!f.silent(3), "silence counts from regaining contact");
    // 3 was last heard directly 30 ms ago, longer than w; 2 just now.
    assert!(
        age(3).is_some_and(|a| a >= Duration::from_millis(30) && a > W),
        "{:?}",
        age(3)
    );
    assert!(age(2).is_some_and(|a| a <= W), "{:?}", age(2));
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
    let known = |f: &Fixture| f.contact.metrics().known_committed;
    // Another term at index 5: not this log's entry.
    let mut r = f.report(2, None);
    r.committed = Some(id(2, 5));
    f.contact.receive(r);
    assert_eq!(known(&f), 0);
    let mut r = f.report(3, None);
    r.committed = Some(id(1, 4));
    f.contact.receive(r);
    assert_eq!(known(&f), 4);
    // Known committed, not applied: nothing reaches the engine.
    assert!(f.engine.calls().is_empty());
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
        baseline: false,
    }];
    f.contact.receive(r.clone());
    assert_eq!(
        f.contact.metrics().known_committed,
        0,
        "another entry at index 5"
    );
    r.seq += 1;
    r.listed = vec![Listed {
        id: id(1, 5),
        at_index: at,
        baseline: false,
    }];
    f.contact.receive(r);
    assert_eq!(f.contact.metrics().known_committed, 5);
}

impl Fixture {
    /// Entry `index` of term 1, stamped at its own index.
    fn insert(&self, index: u64, body: Body) {
        let stamp = body.at_index().unwrap_or(0);
        self.log
            .0
            .lock()
            .unwrap()
            .log
            .insert(index, (id(1, index), Request { stamp, body }));
    }

    /// Entries up to `index` applied, which makes them known
    /// committed.
    fn apply(&self, index: u64) {
        let mut log = self.log.0.lock().unwrap();
        log.applied = index;
        log.committed = Some(id(1, index));
    }

    fn state(&self) -> Option<ReplicaState> {
        self.contact.metrics().replica.and_then(|m| m.state)
    }
}

/// An entry applied again by a restamp: the engine gets the entry the
/// restamp names at the restamp's index, and a replica that applied
/// it at its own index is diverged at once by the restamp's commit.
#[tokio::test(start_paused = true)]
async fn a_restamp_of_an_entry_applied_here_diverges_the_replica() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let at = index_at(T0) + 400 * 48;
    f.log.push(1, 1, Some(at));
    f.apply(1);
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    let restamp = Body::Restamp {
        revision: 1,
        term: 1,
        at_index: at + 96,
        paths: Vec::new(),
        generation: None,
    };
    f.insert(2, restamp);
    let record = Record {
        at_index: at + 96,
        entries: vec![Entry::InputDown { slot: 0 }],
        generation: false,
    };
    (f.log.0.lock().unwrap().records).insert(2, Arc::new(record.clone()));
    f.contact.log_changed();
    f.engine.calls();
    f.tick();
    assert_eq!(
        f.state(),
        Some(ReplicaState::Withdrawn),
        "not committed yet"
    );
    f.apply(2);
    f.contact.log_changed();
    let handed = f.engine.requests.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        handed,
        (2, record.at_index, record.entries),
        "what the restamp derived, at its index, once applied"
    );
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Diverged));
    assert_eq!(f.contact.metrics().contradicted, 1);
}

/// Diverges the fixture's replica as the restamp of an entry it
/// applied at its own index does.
fn diverge(f: &Fixture) {
    f.tick();
    let at = index_at(T0) + 400 * 48;
    f.log.push(1, 1, Some(at));
    f.apply(1);
    f.contact.log_changed();
    f.tick();
    f.insert(
        2,
        Body::Restamp {
            revision: 1,
            term: 1,
            at_index: at + 96,
            paths: Vec::new(),
            generation: None,
        },
    );
    let record = Record {
        at_index: at + 96,
        entries: vec![Entry::InputDown { slot: 0 }],
        generation: false,
    };
    (f.log.0.lock().unwrap().records).insert(2, Arc::new(record));
    f.apply(2);
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Diverged));
}

/// A report from `from` of a replica in `state`, `ago` before now.
fn state_report(f: &Fixture, from: NodeId, state: ReplicaState, ago: Duration) -> Report {
    let mut r = f.report(from, None);
    r.sent_at = f.clock.get() - ago.as_nanos() as u64;
    r.own.state = Some(state);
    r
}

/// Reports from 2 and 3 for three periods, 2 committing when
/// `committing`; then whether the fixture's replica serves.
async fn serving_while(f: &Fixture, committing: bool) -> bool {
    for _ in 0..3 {
        tokio::time::advance(Duration::from_millis(4)).await;
        f.clock.add(Duration::from_millis(4));
        let mut r = f.report(2, None);
        r.own.state = committing.then_some(ReplicaState::Committing);
        f.contact.receive(r);
        f.contact.receive(f.report(3, None));
        f.tick();
    }
    assert!(f.contact.metrics().in_contact);
    f.contact.serving()
}

/// The fixture's replica of a set of three with an output, withdrawn
/// once `baseline` applied.
fn withdrawn_under(baseline: Baseline) -> Fixture {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.log.0.lock().unwrap().baseline = Some(baseline);
    f.tick();
    f
}

fn named_2(key: Option<u64>) -> Baseline {
    Baseline {
        replica: 2,
        order: vec![2, 1, 3],
        at_index: index_at(T0),
        key,
        versions: None,
    }
}

/// A state the per-state readiness test puts the fixture's replica
/// in, and how.
type StateCase = (ReplicaState, fn(&Fixture));

/// The fixture's replica diverged, then resyncing from 2.
fn resyncing(f: &Fixture) -> (u64, NodeId) {
    diverge(f);
    // A later report than any before: one is taken only with a
    // higher seq, which the fixture's clock gives.
    f.clock.add(Duration::from_millis(1));
    f.contact
        .receive(state_report(f, 2, ReplicaState::Committing, Duration::ZERO));
    let (epoch, donor, _) = f.contact.resync_due().expect("a resync");
    assert!(f.contact.resync_arrived(epoch));
    (epoch, donor)
}

/// The fixture's replica commits, then withdraws: its process has
/// committed since it started.
async fn commit_once(f: &Fixture) {
    f.engine.committing.store(true, Ordering::SeqCst);
    // The only replica committing serves.
    assert!(serving_while(f, false).await, "committing alone");
    assert_eq!(f.state(), Some(ReplicaState::Committing));
    f.engine.committing.store(false, Ordering::SeqCst);
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
}

/// `/readyz`: a process that committed since it started serves while
/// it commits or another audio replica commits, whatever its own
/// state, and not while none does, in `withdrawn`, `diverged`,
/// `resync`, `version_mismatch` and `starting` alike; one that never
/// committed does not, though another commits, in `version_mismatch`
/// awaiting the hand-over too. Only below the baseline's versions, as
/// a pod rolled back, does it serve while another commits. A sole
/// voter always serves and a learner never; a withdrawn replica with
/// no output enabled writes nothing on any replica and serves though
/// nobody commits, committed or not. Out of contact the term keeps
/// its last value.
#[tokio::test(start_paused = true)]
async fn a_replica_serves_while_any_audio_replica_commits() {
    let f = fixture(&[&[1]]);
    f.tick();
    f.tick();
    assert!(f.contact.serving(), "a sole voter");
    // A learner of a one-voter set, as one is while a set forms,
    // commits nothing.
    let f = fixture(&[&[2]]);
    f.tick();
    f.tick();
    assert!(!f.contact.serving(), "a learner beside a sole voter");
    let f = fixture(&[&[2, 3, 4]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    for _ in 0..3 {
        for v in [2, 3, 4] {
            f.contact.receive(f.report(v, None));
        }
        f.tick();
    }
    assert!(f.contact.metrics().in_contact);
    assert!(!f.contact.serving(), "a learner, nobody commits");

    let states: [StateCase; 5] = [
        (ReplicaState::Withdrawn, |f| {
            f.log.0.lock().unwrap().baseline = Some(named_2(None));
            f.tick();
        }),
        (ReplicaState::Diverged, |f| diverge(f)),
        (ReplicaState::Resync, |f| {
            resyncing(f);
            f.tick();
        }),
        // Awaiting the hand-over: no versions known.
        (ReplicaState::VersionMismatch, |f| {
            f.contact
                .set_rollout(crate::rollout::Fixed::new(1, None, None));
            f.log.0.lock().unwrap().baseline = Some(named_2(Some(2)));
            f.tick();
        }),
        // Rebuilt from product state after a refused core, the
        // rebuild failing: back in `starting` for good.
        (ReplicaState::Starting, |f| {
            let (epoch, donor) = resyncing(f);
            f.engine.rebuilds.store(false, Ordering::SeqCst);
            let refused = Failed::Refused("another shape".into());
            f.contact.resync_ended(epoch, donor, Err(refused));
            f.tick();
        }),
    ];
    for committed in [false, true] {
        for (state, setup) in states {
            let f = fixture(&[&[1, 2, 3]]);
            f.engine.outputs.store(true, Ordering::SeqCst);
            if committed {
                commit_once(&f).await;
            }
            setup(&f);
            let what = format!("{state:?}, committed since start {committed}");
            assert!(!serving_while(&f, false).await, "{what}, nobody commits");
            assert_eq!(f.state(), Some(state), "{what}");
            if state == ReplicaState::VersionMismatch {
                assert_eq!(
                    f.contact.metrics().replica.and_then(|m| m.mismatch),
                    Some(crate::state::Mismatch::AwaitingHandOver)
                );
            }
            let other = serving_while(&f, true).await;
            assert_eq!(other, committed, "{what}, another commits");
            assert_eq!(f.state(), Some(state), "{what}");
            assert!(!serving_while(&f, false).await, "{what}, nobody again");
            assert_eq!(serving_while(&f, true).await, committed, "{what}, again");
            // Out of contact the term keeps its last value.
            tokio::time::advance(Duration::from_millis(50)).await;
            f.clock.add(Duration::from_millis(50));
            f.tick();
            assert!(!f.contact.metrics().in_contact);
            assert_eq!(f.contact.serving(), committed, "{what}, out of contact");
        }
    }

    // Below the baseline's versions, as a pod rolled back is, a
    // replica that never committed serves while another commits.
    let low = crate::rollout::Versions {
        compute: 1,
        protocol: 1,
        derivation: 1,
    };
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.contact
        .set_rollout(crate::rollout::Fixed::new(1, Some(low), None));
    f.log.0.lock().unwrap().baseline = Some(Baseline {
        versions: Some(crate::rollout::Versions { compute: 2, ..low }),
        ..named_2(Some(2))
    });
    f.tick();
    assert!(!serving_while(&f, false).await, "below, nobody commits");
    assert_eq!(f.state(), Some(ReplicaState::VersionMismatch));
    assert_eq!(
        f.contact.metrics().replica.and_then(|m| m.mismatch),
        Some(crate::state::Mismatch::BelowBaselineVersions)
    );
    assert!(serving_while(&f, true).await, "below, another commits");

    // Without outputs, or with every output disabled, a withdrawn
    // replica writes nothing on any replica: it serves as one that
    // commits. An output enabled again, it serves only while another
    // commits.
    let f = fixture(&[&[1, 2, 3]]);
    f.log.0.lock().unwrap().baseline = Some(named_2(None));
    assert!(serving_while(&f, false).await, "withdrawn without outputs");
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    let f = withdrawn_under(named_2(None));
    f.engine.disabled.store(1, Ordering::SeqCst);
    assert!(serving_while(&f, false).await, "every output disabled");
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    f.engine.disabled.store(0, Ordering::SeqCst);
    assert!(!serving_while(&f, false).await, "an output enabled again");
    f.engine.second.store(true, Ordering::SeqCst);
    f.engine.disabled.store(0b10, Ordering::SeqCst);
    assert!(!serving_while(&f, false).await, "one of two enabled");
    f.engine.disabled.store(0b11, Ordering::SeqCst);
    assert!(serving_while(&f, false).await, "both disabled");
    // Diverged, its engine takes no entry: the outputs it reports
    // disabled are not known to be the log's.
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    diverge(&f);
    f.engine.disabled.store(1, Ordering::SeqCst);
    assert!(!serving_while(&f, false).await, "diverged, disabled");
}

/// Out of contact `/readyz` keeps its last value for `DEPARTED`, so a
/// blip in contact never flips it; past that a replica that commits
/// nothing reads not ready, and one that commits stays ready.
#[tokio::test(start_paused = true)]
async fn readiness_out_of_contact_lasts_a_departure() {
    let departed = crate::rollout::DEPARTED;
    for committing in [false, true] {
        let f = withdrawn_under(named_2(None));
        commit_once(&f).await;
        f.engine.committing.store(committing, Ordering::SeqCst);
        assert!(serving_while(&f, true).await, "in contact");
        let state = if committing {
            ReplicaState::Committing
        } else {
            ReplicaState::Withdrawn
        };
        assert_eq!(f.state(), Some(state));
        let away = |d: Duration| {
            f.clock.add(d);
            tokio::time::advance(d)
        };
        // Contact is lost on the first tick past the contact window.
        away(Duration::from_millis(50)).await;
        f.tick();
        assert!(!f.contact.metrics().in_contact);
        away(departed - Duration::from_millis(100)).await;
        f.tick();
        assert!(f.contact.serving(), "{state:?}, out of contact a while");
        away(Duration::from_millis(200)).await;
        f.tick();
        assert_eq!(f.state(), Some(state));
        assert_eq!(
            f.contact.serving(),
            committing,
            "{state:?}, out of contact past DEPARTED"
        );
    }
}

/// A core the engine refuses for good, of another engine shape, is
/// not asked of the same donor again: the replica is rebuilt from
/// product state and leaves `diverged`.
#[tokio::test(start_paused = true)]
async fn a_core_refused_for_good_rebuilds_rather_than_retries() {
    let f = fixture(&[&[1, 2, 3]]);
    let (epoch, donor) = resyncing(&f);
    let rebuilds = f.engine.rebuilt_with.lock().unwrap().len();
    let refused = Failed::Refused("the core is of another engine shape".into());
    f.contact.resync_ended(epoch, donor, Err(refused));
    for _ in 0..3 {
        f.tick();
    }
    let m = f.contact.metrics().resync;
    assert_eq!((m.refused, m.failures), (1, 1), "{m:?}");
    assert_eq!(
        f.engine.rebuilt_with.lock().unwrap().len(),
        rebuilds + 1,
        "rebuilt from product state"
    );
    assert_ne!(f.state(), Some(ReplicaState::Diverged));
    assert_eq!(f.contact.resync_due(), None, "asked again");
    assert_eq!(f.contact.metrics().resync.attempts, 1);
}

/// A diverged replica whose committing peers all report another
/// compatibility key asks none of them for a core, which its engine
/// would refuse: it is rebuilt from product state instead. One that
/// reports its key donates.
#[tokio::test(start_paused = true)]
async fn a_diverged_replica_without_a_donor_of_its_key_is_rebuilt() {
    for (theirs, donates) in [(2, false), (1, true)] {
        let f = fixture(&[&[1, 2, 3]]);
        f.contact
            .set_rollout(crate::rollout::Fixed::new(1, None, None));
        diverge(&f);
        let mut r = state_report(&f, 2, ReplicaState::Committing, Duration::ZERO);
        r.own.key = Some(theirs);
        f.contact.receive(r);
        let rebuilds = f.engine.rebuilt_with.lock().unwrap().len();
        let due = f.contact.resync_due();
        let m = f.contact.metrics().resync;
        if donates {
            assert_eq!(due.map(|d| d.1), Some(2), "a donor of its key");
            assert_eq!((m.attempts, m.rebuilt_for_key), (1, 0), "{m:?}");
            continue;
        }
        assert_eq!(due, None, "a donor of key {theirs}");
        assert_eq!((m.attempts, m.rebuilt_for_key), (0, 1), "{m:?}");
        for _ in 0..3 {
            f.tick();
        }
        assert_eq!(f.engine.rebuilt_with.lock().unwrap().len(), rebuilds + 1);
        assert_ne!(f.state(), Some(ReplicaState::Diverged));
    }
}

/// A diverged replica asks a donor committing by a report of the last
/// period first, not one whose committing report is older though
/// within `w + D`; with no committing donor at all it keeps checking,
/// counted and logged with a backoff, rather than going silent, while
/// it has an output enabled.
#[tokio::test(start_paused = true)]
async fn a_resync_prefers_a_fresh_donor_and_retries_without_one() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    diverge(&f);
    let attempts = |f: &Fixture| f.contact.metrics().resync;
    assert_eq!(f.contact.resync_due(), None, "nobody commits");
    let m = attempts(&f);
    assert_eq!((m.no_donor, m.attempts), (1, 0), "{m:?}");
    assert_eq!(f.contact.resync_due(), None);
    assert_eq!(attempts(&f).no_donor, 1, "counted within the backoff once");
    tokio::time::advance(crate::resync::RETRY).await;
    assert_eq!(f.contact.resync_due(), None);
    assert_eq!(attempts(&f).no_donor, 2, "counted again after the backoff");
    // 2 reported committing 12 ms ago, 3 just now.
    f.contact.receive(state_report(
        &f,
        2,
        ReplicaState::Committing,
        Duration::ZERO,
    ));
    tokio::time::advance(Duration::from_millis(12)).await;
    f.clock.add(Duration::from_millis(12));
    f.contact.receive(state_report(
        &f,
        3,
        ReplicaState::Committing,
        Duration::ZERO,
    ));
    let (_, donor, _) = f.contact.resync_due().expect("an attempt");
    assert_eq!(donor, 3, "the fresh donor");
    assert_eq!(attempts(&f).attempts, 1);
}

/// A diverged replica whose every output the log disabled has no donor
/// to wait for: nothing commits while the log keeps them disabled. It
/// is rebuilt from product state instead and leaves `diverged`, again
/// once diverged again, after the attempts' backoff. With an output
/// enabled, by its engine or by the applied log ahead of the engine,
/// it waits for a donor.
#[tokio::test(start_paused = true)]
async fn a_diverged_replica_with_every_output_disabled_is_rebuilt() {
    // Whether the engine has an output and has it disabled, and
    // whether the log has it enabled: an engine listing none while the
    // log enables one is not taken as writing nothing.
    let cases = [
        (true, true, false),
        (true, false, false),
        (true, true, true),
        (false, true, true),
    ];
    for (output, disabled, logged) in cases {
        let f = fixture(&[&[1, 2, 3]]);
        f.engine.outputs.store(output, Ordering::SeqCst);
        f.engine
            .disabled
            .store(u64::from(disabled), Ordering::SeqCst);
        f.log.0.lock().unwrap().outputs = vec![logged];
        diverge(&f);
        let rebuilds = f.engine.rebuilt_with.lock().unwrap().len();
        assert_eq!(f.contact.resync_due(), None, "nobody commits");
        let m = f.contact.metrics().resync;
        for _ in 0..3 {
            f.tick();
        }
        let rebuilt = f.engine.rebuilt_with.lock().unwrap().len() - rebuilds;
        if output && disabled && !logged {
            assert_eq!((m.rebuilt_writing_nothing, m.no_donor), (1, 0), "{m:?}");
            assert_eq!(rebuilt, 1, "rebuilt from product state");
            assert_ne!(f.state(), Some(ReplicaState::Diverged));
            let audio: Arc<dyn Audio> = f.engine.clone();
            {
                let mut st = f.contact.lock();
                let life = st.life.as_mut().unwrap();
                life.contradicted(audio.as_ref());
                assert_eq!(life.state(), ReplicaState::Diverged);
            }
            assert_eq!(f.contact.resync_due(), None);
            let m = f.contact.metrics().resync;
            assert_eq!(m.rebuilt_writing_nothing, 1, "within the backoff");
            tokio::time::advance(m.backoff).await;
            assert_eq!(f.contact.resync_due(), None);
            let m = f.contact.metrics().resync;
            assert_eq!(m.rebuilt_writing_nothing, 2, "after the backoff");
        } else {
            let case = (output, disabled, logged);
            assert_eq!(
                (m.rebuilt_writing_nothing, m.no_donor),
                (0, 1),
                "{case:?}: {m:?}"
            );
            assert_eq!(rebuilt, 0, "{case:?}");
            assert_eq!(f.state(), Some(ReplicaState::Diverged), "{case:?}");
        }
    }
}

/// A fresh donor without a peer address is no donor: the one
/// committing by an older report within `w + D` is asked instead.
#[tokio::test(start_paused = true)]
async fn a_fresh_donor_without_an_address_leaves_the_older_one() {
    let mut f = fixture(&[&[1, 2, 3]]);
    f.members.addrs.remove(&3);
    diverge(&f);
    f.contact.receive(state_report(
        &f,
        2,
        ReplicaState::Committing,
        Duration::ZERO,
    ));
    tokio::time::advance(Duration::from_millis(12)).await;
    f.clock.add(Duration::from_millis(12));
    f.contact.receive(state_report(
        &f,
        3,
        ReplicaState::Committing,
        Duration::ZERO,
    ));
    let (_, donor, _) = f.contact.resync_due().expect("an attempt");
    assert_eq!(donor, 2);
}

/// A void of an entry held unresolved resolves it in the engine and
/// takes it out of the reports, once the void is known committed.
#[tokio::test(start_paused = true)]
async fn a_void_resolves_the_entry_held_unresolved() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let timing = Timing::defaults(B);
    let at = index_at(T0) + 40 * 48;
    f.log.push(1, 1, Some(at));
    f.contact.log_changed();
    f.clock.set(timing.apply_deadline(at));
    let unresolved = |f: &Fixture| -> Vec<u64> {
        let out = f.tick();
        out[0].2.own.unresolved.iter().map(|l| l.id.index).collect()
    };
    assert_eq!(unresolved(&f), [1]);
    f.insert(
        2,
        Body::Void {
            revision: 1,
            term: 1,
            at_index: None,
            generation: None,
        },
    );
    f.apply(2);
    f.engine.calls();
    f.contact.log_changed();
    unresolved(&f);
    assert!(f.engine.calls().contains(&"resolved 1".to_owned()));
    assert_eq!(unresolved(&f), Vec::<u64>::new());
    assert_ne!(
        f.state(),
        Some(ReplicaState::Diverged),
        "it never applied it"
    );
}

/// A lagging follower whose next entry the leader purged installs a
/// snapshot: it returns to `starting`, its engine dropped, counted,
/// and leaves it only once the engine is rebuilt from product state.
#[tokio::test(start_paused = true)]
async fn a_snapshot_install_returns_the_replica_to_starting_until_rebuilt() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    // Its own log purged past what contact was told: no install.
    f.apply(5);
    f.contact.log_changed();
    f.tick();
    assert_eq!(
        f.state(),
        Some(ReplicaState::Withdrawn),
        "a purge is no install"
    );
    f.engine.rebuilds.store(false, Ordering::SeqCst);
    f.apply(10);
    f.log.0.lock().unwrap().installs += 1;
    f.engine.calls();
    f.contact.log_changed();
    f.tick();
    let calls = f.engine.calls();
    assert!(calls.contains(&"drop".to_owned()), "{calls:?}");
    assert_eq!(f.state(), Some(ReplicaState::Starting));
    assert_eq!(f.contact.metrics().snapshot_installs, 1);
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Starting), "not rebuilt");
    f.engine.rebuilds.store(true, Ordering::SeqCst);
    f.log.0.lock().unwrap().floor = 4800;
    f.tick();
    assert!(f.engine.calls().contains(&"rebuild".to_owned()));
    assert_eq!(
        *f.engine.rebuilt_with.lock().unwrap(),
        [(10, 4800)],
        "the applied state, at its floor"
    );
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
}

/// A rebuild waits for the RT thread for up to the lead: contact goes
/// on ticking and reporting meanwhile, and takes the rebuilt core on a
/// tick after it ended.
#[tokio::test(start_paused = true)]
async fn contact_ticks_on_while_a_rebuild_waits() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    f.engine.holding.store(true, Ordering::SeqCst);
    f.apply(10);
    f.log.0.lock().unwrap().installs += 1;
    f.contact.log_changed();
    for _ in 0..5 {
        let from = std::time::Instant::now();
        let reports = f.contact.tick(f.members.clone());
        assert!(
            from.elapsed() < Duration::from_millis(500),
            "a tick waited for the rebuild"
        );
        assert_eq!(reports.len(), 2);
        assert_eq!(f.state(), Some(ReplicaState::Starting));
    }
    f.engine.holding.store(false, Ordering::SeqCst);
    f.contact.await_rebuild();
    f.contact.tick(f.members.clone());
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    let calls = f.engine.calls();
    let rebuilds = calls.iter().filter(|c| *c == "rebuild").count();
    assert_eq!(rebuilds, 1, "{calls:?}");
}

/// Entry `index`, appended and applied `late` past its apply deadline
/// or else in time by a block, on a tick. A late entry's index lies
/// below the floor, which raises it to the last entry's, one block
/// past that entry's deadline at most: 5 ms passed since.
fn take(f: &Fixture, index: u64, late: bool) {
    f.clock.add(Duration::from_millis(5));
    let guard = Timing::defaults(B).apply_guard_ns();
    let at = if late {
        index_at(f.clock.get()) - 10 * B
    } else {
        index_at(f.clock.get() + guard) + B
    };
    f.log.push(1, index, Some(at));
    f.contact.log_changed();
    f.apply(index);
    f.contact.log_changed();
    f.contact.tick(f.members.clone());
}

/// A report from voter 2 naming `index` committed.
fn committed_at(f: &Fixture, index: u64) {
    f.clock.add(Duration::from_millis(1));
    let mut r = f.report(2, None);
    r.committed = Some(id(1, index));
    f.contact.receive(r);
}

/// A replica that took an entry in time past the commit the voters
/// named and then takes every entry past its deadline, as a log at the lead cap with
/// a slow disk does, is not catching up: it holds them unresolved and
/// keeps its core. A rebuild past each would leave it without a core
/// while the lateness lasts. Once entries come in time again they are
/// handed.
#[tokio::test(start_paused = true)]
async fn a_replica_late_after_catching_up_keeps_its_core() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    assert!(!f.contact.lock().catching_up);
    f.engine.calls();
    for index in 3..=21 {
        take(&f, index, true);
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn), "entry {index}");
    }
    let calls = f.engine.calls();
    assert!(
        !calls.iter().any(|c| c == "drop" || c == "rebuild"),
        "{calls:?}"
    );
    assert_eq!(f.contact.metrics().late_rebuilds, 0);
    assert_eq!(f.contact.lock().given.len(), 19, "held unresolved");
    take(&f, 22, false);
    f.tick();
    assert!(f.engine.calls().contains(&"appended 22".to_owned()));
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
}

/// A replica taking the baseline over in a key hand-over, retargeted
/// from `version_mismatch` and named, holds an entry it took late while
/// catching up, as the replica it takes over from does, until it
/// commits: rebuilt past it, it would return to `starting`, be passed
/// over and the old-key filler named again.
#[tokio::test(start_paused = true)]
async fn a_replica_taking_over_holds_what_it_took_late() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.contact
        .set_rollout(crate::rollout::Fixed::new(2, None, None));
    f.log.0.lock().unwrap().baseline = Some(named_2(Some(1)));
    f.tick();
    serving_while(&f, false).await;
    assert_eq!(f.state(), Some(ReplicaState::VersionMismatch));
    assert!(f.contact.lock().catching_up);
    take(&f, 1, true);
    f.tick();
    assert_eq!(
        f.contact.lock().given.len(),
        1,
        "held while it computes nothing"
    );
    // Named with its key, P well ahead.
    f.log.0.lock().unwrap().baseline = Some(Baseline {
        replica: 1,
        order: vec![1, 2, 3],
        at_index: index_at(f.clock.get()) + 400 * B,
        key: Some(2),
        versions: None,
    });
    for _ in 0..5 {
        f.tick();
    }
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn), "retargeted");
    assert_eq!(f.contact.metrics().late_rebuilds, 0, "rebuilt past it");
    assert_eq!(f.contact.lock().given.len(), 1, "held");
}

/// A diverged replica commits nothing either: one that takes an entry
/// past its deadline while catching up, as a restarted one diverged
/// before it applied what was written while it was away, is rebuilt
/// past it. Held unresolved, an entry that stands is never decided,
/// and a `Route` held so leaves an input on a flow the log no longer
/// names, which a resync's replay reads.
#[tokio::test(start_paused = true)]
async fn a_diverged_replica_catching_up_is_rebuilt_past_a_late_entry() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    diverge(&f);
    assert!(f.contact.lock().catching_up);
    // Past the floor the diverging entries left.
    f.clock.add(Duration::from_secs(1));
    take(&f, 3, true);
    f.tick();
    assert_eq!(f.contact.metrics().late_rebuilds, 1);
    assert_ne!(f.state(), Some(ReplicaState::Diverged), "restarted");
    assert!(f.contact.lock().given.is_empty(), "rebuilt past, not held");
}

/// An entry a catch-up taken late is rebuilt past was given but never
/// asked of the engine, so the engine holds nothing to resolve: no
/// `resolved` call is sent into a core that was dropped.
#[tokio::test(start_paused = true)]
async fn a_catch_up_rebuilt_past_an_entry_resolves_none_the_engine_never_held() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    diverge(&f);
    assert!(f.contact.lock().catching_up);
    f.clock.add(Duration::from_secs(1));
    f.engine.calls();
    // Appended and applied before the contact takes either: given and
    // rebuilt past in one pass, the engine asked in between by no one.
    f.clock.add(Duration::from_millis(5));
    let at = index_at(f.clock.get()) - 10 * B;
    f.log.push(1, 3, Some(at));
    f.apply(3);
    f.contact.log_changed();
    f.tick();
    let calls = f.engine.calls();
    assert!(f.contact.lock().given.is_empty(), "rebuilt past");
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("resolved") || c.starts_with("unresolved")),
        "{calls:?}"
    );
}

/// A rebuild, on a replica committing nothing, from the state an
/// entry held unresolved applied in holds it: the entry is rebuilt
/// past rather than given to the new core again, where it would stay
/// unresolved for good once it stands. One the state does not hold
/// yet is given again.
#[tokio::test(start_paused = true)]
async fn a_rebuild_takes_the_held_entries_its_state_applied() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    assert!(!f.contact.lock().catching_up);
    for index in 3..=6 {
        take(&f, index, true);
        f.tick();
    }
    assert_eq!(f.contact.lock().given.len(), 4, "held unresolved");
    // Entry 7 is held unresolved before it applies.
    f.clock.add(Duration::from_millis(5));
    let at = index_at(f.clock.get()) - 10 * B;
    f.log.push(1, 7, Some(at));
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.contact.lock().given.len(), 5);
    // A rebuild from the state applied up to 6, as a diverged replica
    // on another flow than its state names starts.
    {
        let mut st = f.contact.lock();
        f.contact.begin_restart(&mut st, 6, "test");
    }
    f.tick();
    f.contact.await_rebuild();
    f.engine.calls();
    f.tick();
    let st = f.contact.lock();
    let given: Vec<u64> = st.given.keys().map(|k| k.0).collect();
    assert_eq!(given, [7], "the applied ones rebuilt past");
    drop(st);
    let calls = f.engine.calls();
    assert!(
        !calls.iter().any(|c| c.starts_with("unresolved 3")),
        "{calls:?}"
    );
}

/// A full part taken by `sync_log` while `hand` gives an expired entry
/// of its own is asked about from the part's lowest entry: `hand`
/// trims no entry the part's own `ask` has not reached.
#[tokio::test(start_paused = true)]
async fn a_full_part_with_a_hand_over_asks_the_engine_about_its_lowest_entries() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    // Entry 3 is awaited, in time when taken.
    f.clock.add(Duration::from_millis(5));
    let guard = Timing::defaults(B).apply_guard_ns();
    f.log.push(1, 3, Some(index_at(f.clock.get() + guard) + B));
    f.contact.log_changed();
    f.tick();
    // It applies past its deadline, with a part of late entries
    // beyond it taken in the same pass.
    f.clock.add(Duration::from_secs(1));
    let late = index_at(f.clock.get()) - 10 * B;
    f.apply(3);
    for index in 4..4 + GIVEN_KEPT as u64 {
        f.log.push(1, index, Some(late));
    }
    f.engine.calls();
    f.contact.log_changed();
    f.tick();
    let asked = f.engine.calls();
    assert!(
        asked.iter().any(|c| c.starts_with("unresolved 3 ")),
        "hand gave entry 3: {asked:?}"
    );
    assert!(
        asked.iter().any(|c| c.starts_with("unresolved 4 ")),
        "the part's lowest asked: {} calls, first {:?}",
        asked.len(),
        asked.iter().take(4).collect::<Vec<_>>()
    );
}

/// An entry given and applied without a record before the engine was
/// asked about it is released without a `resolved` call: the engine
/// never held it.
#[tokio::test(start_paused = true)]
async fn an_entry_applied_without_a_record_before_it_was_asked_resolves_none() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    f.clock.add(Duration::from_millis(5));
    let at = index_at(f.clock.get()) - 10 * B;
    f.insert(
        3,
        Body::InputDown {
            slot: 0,
            at_index: at,
        },
    );
    f.apply(3);
    f.engine.calls();
    f.contact.log_changed();
    f.tick();
    assert!(f.contact.lock().given.is_empty(), "released");
    let calls = f.engine.calls();
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("resolved") || c.starts_with("unresolved")),
        "{calls:?}"
    );
}

/// An entry held unresolved that applies without a record is released,
/// and a decision the log appends on it after all, as for an entry
/// every replica took late, neither resolves nor contradicts the
/// replica: it is rebuilt past.
#[tokio::test(start_paused = true)]
async fn a_decision_on_a_released_entry_without_a_record_contradicts_nothing() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    assert!(!f.contact.lock().catching_up);
    // Entry 3 late, deriving no record.
    f.clock.add(Duration::from_millis(5));
    let at = index_at(f.clock.get()) - 10 * B;
    f.insert(
        3,
        Body::InputDown {
            slot: 0,
            at_index: at,
        },
    );
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.contact.lock().given.len(), 1, "held unresolved");
    f.apply(3);
    f.contact.log_changed();
    f.tick();
    assert!(f.contact.lock().given.is_empty(), "released");
    assert!(f.engine.calls().contains(&"resolved 3".to_owned()));
    // The log restamps it after all.
    f.insert(
        4,
        Body::Restamp {
            revision: 3,
            term: 1,
            at_index: at + 960,
            paths: Vec::new(),
            generation: None,
        },
    );
    f.apply(4);
    f.contact.log_changed();
    f.tick();
    f.tick();
    assert_eq!(f.contact.metrics().contradicted, 0);
    assert_ne!(f.state(), Some(ReplicaState::Diverged));
}

/// A replica about to commit, retargeted from `version_mismatch`, gives
/// its rebuilt core the entries it holds unresolved again, those its
/// state applied included, as its peers hold them: the log may still
/// decide them. Only a replica committing nothing rebuilds past them.
#[tokio::test(start_paused = true)]
async fn a_retargeted_core_holds_the_entries_its_state_applied() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.contact
        .set_rollout(crate::rollout::Fixed::new(1, None, None));
    let baseline = |key| Baseline {
        replica: 2,
        order: vec![2, 1, 3],
        at_index: index_at(T0),
        key: Some(key),
        versions: None,
    };
    f.log.0.lock().unwrap().baseline = Some(baseline(2));
    f.tick();
    take(&f, 1, false);
    committed_at(&f, 1);
    take(&f, 2, false);
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::VersionMismatch));
    take(&f, 3, true);
    f.tick();
    assert_eq!(f.contact.lock().given.len(), 1, "held unresolved");
    f.engine.calls();
    // The baseline's key is its own again: retargeted from the state
    // applied up to 3.
    f.log.0.lock().unwrap().baseline = Some(baseline(1));
    f.tick();
    f.contact.await_rebuild();
    f.tick();
    f.tick();
    let calls = f.engine.calls();
    assert!(calls.contains(&"rebuild".to_owned()), "{calls:?}");
    assert!(
        calls.iter().any(|c| c.starts_with("unresolved 3 ")),
        "given to the new core again: {calls:?}"
    );
    assert_eq!(f.contact.lock().given.len(), 1);
}

/// A diverged replica whose input reads another flow than its applied
/// state names, as one does while a committed re-route waits for its
/// block inside the lead, resyncs as usual; it is rebuilt from product
/// state instead only once the mismatch outlasts the longest lead.
#[tokio::test(start_paused = true)]
async fn a_flow_mismatch_rebuilds_only_once_it_outlasts_the_lead() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.log.0.lock().unwrap().flows = vec![Some("a".to_owned())];
    diverge(&f);
    f.engine.follows.store(false, Ordering::SeqCst);
    let committing = |f: &Fixture| {
        f.contact
            .receive(state_report(f, 2, ReplicaState::Committing, Duration::ZERO));
    };
    committing(&f);
    let rebuilt = |f: &Fixture| f.contact.metrics().resync.rebuilt_for_flows;
    assert!(f.contact.resync_due().is_some(), "resynced at first");
    assert_eq!(rebuilt(&f), 0);
    let lead = Timing::defaults(B).max_apply_lead + crate::schedule::EXTRA_LEAD;
    let wait = |f: &Fixture| f.contact.metrics().resync.backoff;
    let failed = |f: &Fixture, epoch| {
        f.contact
            .resync_ended(epoch, 2, Err("a replayed page differs".into()));
    };
    failed(&f, 1);
    let first = wait(&f);
    assert!(first < lead, "{first:?}");
    tokio::time::advance(first).await;
    f.clock.add(first);
    committing(&f);
    assert!(f.contact.resync_due().is_some(), "within the lead");
    assert_eq!(rebuilt(&f), 0);
    failed(&f, 2);
    let second = wait(&f);
    assert!(first + second >= lead);
    tokio::time::advance(second).await;
    f.clock.add(second);
    committing(&f);
    assert_eq!(f.contact.resync_due(), None, "rebuilt instead");
    assert_eq!(rebuilt(&f), 1);
}

/// Every tick tells the engine whether the replica's output is apart
/// from its peers': from a divergence until the hand-over of a
/// resync, so the engine's meters lose what it computes meanwhile.
#[tokio::test(start_paused = true)]
async fn the_engine_is_told_of_a_divergence_until_the_hand_over() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    assert!(!f.engine.apart.load(Ordering::SeqCst));
    diverge(&f);
    assert_eq!(f.state(), Some(ReplicaState::Diverged));
    assert!(f.engine.apart.load(Ordering::SeqCst), "diverged");
    f.contact.receive(state_report(
        &f,
        2,
        ReplicaState::Committing,
        Duration::ZERO,
    ));
    let (epoch, donor, _) = f.contact.resync_due().expect("a resync");
    assert!(f.contact.resync_arrived(epoch));
    f.tick();
    assert!(f.engine.apart.load(Ordering::SeqCst), "replaying");
    f.contact.resync_ended(
        epoch,
        donor,
        Ok(Resynced {
            j: 1,
            pages: Vec::new(),
            unresolved: Vec::new(),
        }),
    );
    f.tick();
    assert!(!f.engine.apart.load(Ordering::SeqCst), "handed over");
}

/// A diverged replica on another flow resyncs, hands over and
/// diverges again `lead` plus the backoff later, its input still on
/// another flow; whether its next check rebuilds it from product state.
/// In between it leaves `diverged` and `resync` for `withdrawn` once
/// when `recovered`, and entries re-route its input to each of
/// `reroutes` in turn.
async fn rebuilt_after_a_hand_over(recovered: bool, reroutes: &[&str]) -> bool {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.log.0.lock().unwrap().flows = vec![Some("a".to_owned())];
    diverge(&f);
    f.engine.follows.store(false, Ordering::SeqCst);
    let committing = |f: &Fixture| {
        f.contact
            .receive(state_report(f, 2, ReplicaState::Committing, Duration::ZERO));
    };
    committing(&f);
    let (epoch, donor, _) = f.contact.resync_due().expect("resynced at first");
    assert!(f.contact.resync_arrived(epoch));
    f.contact.resync_ended(
        epoch,
        donor,
        Ok(Resynced {
            j: 1,
            pages: Vec::new(),
            unresolved: Vec::new(),
        }),
    );
    assert_eq!(f.contact.metrics().resync.handovers, 1);
    let audio: Arc<dyn Audio> = f.engine.clone();
    if recovered {
        f.contact
            .lock()
            .life
            .as_mut()
            .unwrap()
            .restart(audio.as_ref());
        f.tick();
        assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
        assert_eq!(f.contact.resync_due(), None);
    }
    let guard = Timing::defaults(B).apply_guard_ns();
    for (n, flow) in reroutes.iter().enumerate() {
        let index = 3 + n as u64;
        let at = index_at(f.clock.get() + guard) + B;
        f.log.push(1, index, Some(at));
        let record = Record {
            at_index: at,
            entries: vec![Entry::Route {
                slot: 0,
                flow_id: (*flow).to_owned(),
            }],
            generation: true,
        };
        (f.log.0.lock().unwrap().records).insert(index, Arc::new(record));
        f.log.0.lock().unwrap().flows = vec![Some((*flow).to_owned())];
        f.apply(index);
        f.contact.log_changed();
        f.tick();
    }
    let lead = Timing::defaults(B).max_apply_lead + crate::schedule::EXTRA_LEAD;
    let later = lead + f.contact.metrics().resync.backoff;
    tokio::time::advance(later).await;
    f.clock.add(later);
    // Diverged again on the first page after the hand-over.
    {
        let mut st = f.contact.lock();
        let life = st.life.as_mut().unwrap();
        life.contradicted(audio.as_ref());
        assert_eq!(life.state(), ReplicaState::Diverged);
    }
    committing(&f);
    let resynced = f.contact.resync_due().is_some();
    let rebuilt = f.contact.metrics().resync.rebuilt_for_flows;
    assert_eq!(resynced, rebuilt == 0);
    rebuilt == 1
}

/// A hand-over moves no input: a replay reads the engine's inputs. A
/// replica on another flow before it is still on it after it, so the
/// mismatch counts on and the replica is rebuilt once it outlasts the
/// lead. Counted anew at each hand-over, every check would find a
/// fresh mismatch and resync, which hands over and diverges again.
#[tokio::test(start_paused = true)]
async fn a_flow_mismatch_counts_on_across_a_hand_over() {
    assert!(rebuilt_after_a_hand_over(false, &[]).await);
}

/// Once an entry re-routed the input since, as one committed with its
/// block still inside the lead, the mismatch counts anew: the replica
/// resyncs at its first check. So it does after a re-route away and
/// back, though the flows the log names are those it began with.
#[tokio::test(start_paused = true)]
async fn a_flow_mismatch_counts_anew_after_a_re_route() {
    assert!(!rebuilt_after_a_hand_over(false, &["b"]).await);
    assert!(!rebuilt_after_a_hand_over(false, &["b", "a"]).await);
}

/// A replica that left `diverged` and `resync` in between counts a
/// later divergence's mismatch anew, on the same flows: it resyncs at
/// its first check.
#[tokio::test(start_paused = true)]
async fn a_flow_mismatch_counts_anew_after_the_replica_recovered() {
    assert!(!rebuilt_after_a_hand_over(true, &[]).await);
}

/// A replica catching up past entries' deadlines is rebuilt past them
/// one rebuild at a time: what it takes late while a rebuild runs is
/// folded into one rebuild after it, not into a restart that makes
/// the running one stale. Once entries come in time it holds nothing
/// unresolved and is withdrawn with its core.
#[tokio::test(start_paused = true)]
async fn a_replica_catching_up_is_rebuilt_past_late_entries_once_per_rebuild() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    f.engine.holding.store(true, Ordering::SeqCst);
    for index in 1..=10 {
        take(&f, index, true);
        assert_eq!(f.state(), Some(ReplicaState::Starting), "entry {index}");
    }
    assert_eq!(f.contact.metrics().late_rebuilds, 1);
    assert_eq!(f.contact.lock().late_given.len(), 9, "held for the next");
    f.engine.holding.store(false, Ordering::SeqCst);
    f.contact.await_rebuild();
    f.tick();
    assert_eq!(f.contact.metrics().late_rebuilds, 2, "folded once it ended");
    assert!(f.contact.lock().given.is_empty());
    committed_at(&f, 10);
    take(&f, 11, false);
    f.tick();
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    assert_eq!(f.contact.metrics().late_rebuilds, 2);
    let st = f.contact.lock();
    assert!(st.given.is_empty() && st.late_given.is_empty() && !st.catching_up);
}

/// Entries taken late that stay unresolved while a resync attempt
/// runs are not rebuilt past: the restart would abandon the attempt
/// and drop the engine's core mid hand-over. Once the attempt ended
/// the rebuild follows.
#[tokio::test(start_paused = true)]
async fn a_late_rebuild_waits_for_a_running_resync() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    f.engine.holding.store(true, Ordering::SeqCst);
    for index in 1..=10 {
        take(&f, index, true);
    }
    assert_eq!(f.contact.metrics().late_rebuilds, 1);
    assert_eq!(f.contact.lock().late_given.len(), 9);
    assert!(f.contact.lock().resync.start(Instant::now()).is_some());
    f.engine.holding.store(false, Ordering::SeqCst);
    f.contact.await_rebuild();
    for _ in 0..3 {
        f.tick();
    }
    assert_eq!(
        f.contact.metrics().late_rebuilds,
        1,
        "waits for the attempt"
    );
    assert_eq!(f.contact.lock().late_given.len(), 9);
    f.contact.lock().resync.abandon(Instant::now());
    f.tick();
    assert_eq!(f.contact.metrics().late_rebuilds, 2, "folded once it ended");
}

/// An entry taken in time while catching up, as one stamped with a
/// generation's build lead on top of the lead can be, does not end the
/// catch-up: the entries after it up to the commit the voters named
/// were written before the replica could take them, and those it
/// takes past their deadlines are rebuilt past, not held unresolved.
/// One in time past that commit ends it.
#[tokio::test(start_paused = true)]
async fn an_entry_in_time_ends_no_catch_up_before_the_named_commit() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.tick();
    committed_at(&f, 5);
    take(&f, 1, false);
    for index in 2..=5 {
        take(&f, index, true);
    }
    f.contact.await_rebuild();
    f.tick();
    f.tick();
    let rebuilds = f.contact.metrics().late_rebuilds;
    assert!(rebuilds >= 1, "rebuilt past 2..=5");
    let st = f.contact.lock();
    assert!(
        st.given.is_empty() && st.late_given.is_empty(),
        "{:?}",
        st.given
    );
    drop(st);
    for _ in 0..3 {
        f.tick();
    }
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    take(&f, 6, false);
    assert!(
        !f.contact.lock().catching_up,
        "in time past the named commit"
    );
    take(&f, 7, true);
    f.tick();
    assert_eq!(
        f.contact.metrics().late_rebuilds,
        rebuilds,
        "7 held unresolved"
    );
    assert_eq!(f.contact.lock().given.len(), 1);
}

/// What an entry derived reaches the engine as it applies, not as it
/// is appended or learned committed, at the index it derived; one
/// applied at or past its apply deadline does not, and goes
/// unresolved, however late the deadline passed.
#[tokio::test(start_paused = true)]
async fn a_record_is_handed_as_it_applies_unless_past_its_deadline() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let timing = Timing::defaults(B);
    let at = index_at(T0) + 400 * B;
    f.log.push(1, 1, Some(at));
    f.log.push(1, 2, Some(at + B));
    f.contact.log_changed();
    let mut r = f.report(2, None);
    r.committed = Some(id(1, 2));
    f.contact.receive(r);
    assert_eq!(f.contact.metrics().known_committed, 2);
    assert!(f.engine.requests.lock().unwrap().is_empty(), "not applied");
    f.apply(1);
    f.contact.log_changed();
    let handed = f.engine.requests.lock().unwrap().clone();
    assert_eq!(handed, [(1, at, vec![Entry::InputDown { slot: 0 }])]);
    assert_eq!(f.engine.calls(), ["appended 1", "committed 1"]);
    // Still awaited as contact looks, past its deadline as it hands.
    f.clock.set(timing.apply_deadline(at + B) - 1);
    f.clock.step.store(1, Ordering::SeqCst);
    f.apply(2);
    f.contact.log_changed();
    assert_eq!(f.engine.requests.lock().unwrap().len(), 1, "entry 2 handed");
    assert_eq!(
        f.engine.calls(),
        [format!("unresolved 2 at {}", at + B), "committed 2".into()]
    );
}

/// An applied entry's record gone before contact handed it leaves the
/// engine without it: the replica drops its core and is rebuilt.
#[tokio::test(start_paused = true)]
async fn a_record_gone_before_it_was_handed_rebuilds_the_engine() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let at = index_at(T0) + 400 * B;
    f.log.push(1, 1, Some(at));
    f.contact.log_changed();
    f.engine.calls();
    f.log.0.lock().unwrap().records_from = 1;
    f.apply(1);
    f.contact.log_changed();
    assert!(f.engine.calls().contains(&"drop".to_owned()));
    assert!(f.engine.requests.lock().unwrap().is_empty());
    f.tick();
    assert!(f.engine.calls().contains(&"rebuild".to_owned()));
    assert_eq!(*f.engine.rebuilt_with.lock().unwrap(), [(1, 0)]);
}

/// The most one `hand` call takes under the contact lock: every
/// record the state machine still holds, [`RECORDS_KEPT`] samples of
/// entries stamped one sample apart, the densest the entry rate
/// goes. Older records are gone, and an entry range reaching into
/// them takes the rebuild path instead of the loop
/// (`a_record_gone_before_it_was_handed_rebuilds_the_engine`). The
/// hold stays far under the lock watchdog's limit. The engine here is
/// the test fake, which records each call; the real one's calls are
/// non-blocking and in memory (see the pull request).
#[tokio::test(start_paused = true)]
async fn handing_every_record_still_held_stays_far_under_the_watchdog_limit() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let at = index_at(T0) + 400 * B;
    let entries = crate::state_machine::RECORDS_KEPT;
    for index in 1..=entries {
        f.log.push(1, index, Some(at + index));
    }
    // Contact was told of the entries as they came, at most
    // AWAITING_MAX unapplied at a time: past that `note` offers an
    // entry unresolved, and one given so is not handed again. Told
    // already, they all reach the engine as entries in this one call.
    f.contact.lock().told = entries;
    f.apply(entries);
    let started = std::time::Instant::now();
    f.contact.log_changed();
    let held = started.elapsed();
    let calls = f.engine.calls();
    println!(
        "handing {entries} records took {held:?} ({} engine calls)",
        calls.len()
    );
    let expected: Vec<String> = (1..=entries)
        .map(|i| format!("appended {i}"))
        .chain(["committed 48000".to_owned()])
        .collect();
    assert_eq!(calls, expected, "every record, then the commit");
    assert!(
        held < Duration::from_secs(2),
        "{held:?} under the contact lock, limit {:?}",
        crate::lock_watch::HELD_LIMIT
    );
}

/// A resync's replay reads the records of the entries applied after
/// the donor core's last log id, and fails over a gap: a record
/// after it this replica no longer holds.
#[tokio::test(start_paused = true)]
async fn a_replay_from_before_the_records_held_is_missed() {
    let f = fixture(&[&[1, 2, 3]]);
    let at = index_at(T0) + 400 * B;
    for index in 1..=3 {
        f.log.push(1, index, Some(at + index * B));
    }
    f.apply(3);
    f.log.0.lock().unwrap().records_from = 1;
    assert_eq!(f.contact.replay_entries(0), Err(Missed));
    let replayed: Vec<(u64, u64)> = (f.contact.replay_entries(1).expect("held"))
        .iter()
        .map(|(i, r)| (*i, r.at_index))
        .collect();
    assert_eq!(replayed, [(2, at + 2 * B), (3, at + 3 * B)]);
}

/// A record reaches the engine, and a resync's replay, as the state
/// machine holds it: its entries are shared, not copied under the
/// store lock, so taking a recall of every coefficient at the largest
/// shape costs no more there than taking one gain.
#[tokio::test(start_paused = true)]
async fn a_record_is_handed_and_replayed_without_copying_its_entries() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let at = index_at(T0) + 400 * B;
    f.log.push(1, 1, Some(at));
    let held = Arc::clone(&f.log.0.lock().unwrap().records[&1]);
    f.contact.log_changed();
    f.apply(1);
    f.contact.log_changed();
    let handed = f.engine.handed_from.lock().unwrap().clone();
    assert_eq!(handed, [held.entries.as_ptr() as usize]);
    let replayed = f.contact.replay_entries(0).expect("held");
    assert!(Arc::ptr_eq(&replayed[0].1, &held));
}

/// A lease expiry from an older leader, which stamps no index, takes
/// effect [`crate::state_machine::FALLBACK_LEAD`] past its stamp: its
/// apply deadline has not passed when it applies promptly, so its
/// record reaches the engine rather than going unresolved, which no
/// decision could settle for an entry without an index.
#[tokio::test(start_paused = true)]
async fn an_unstamped_lease_expiry_applied_promptly_is_handed() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let stamp = index_at(T0).next_multiple_of(B);
    let expiry = Request {
        stamp,
        body: Body::LeaseExpiry {
            session: crate::state_machine::tests::origin("s").key(),
            renewed: 0,
            at_index: None,
        },
    };
    let at = stamp + crate::state_machine::FALLBACK_LEAD;
    {
        let mut log = f.log.0.lock().unwrap();
        log.log.insert(1, (id(1, 1), expiry));
        let record = Record {
            at_index: at,
            entries: vec![Entry::InputDown { slot: 0 }],
            generation: false,
        };
        log.records.insert(1, Arc::new(record));
    }
    f.contact.log_changed();
    f.clock.add(Duration::from_millis(5));
    f.apply(1);
    f.contact.log_changed();
    let calls = f.engine.calls();
    assert_eq!(calls, ["appended 1", "committed 1"], "{calls:?}");
}

/// An apply landing between two reads of the applied state: contact
/// takes the applied index and the floor after it in one, so the
/// entries after that index are listed at the floor their apply
/// derives at, not one raised by entries it then re-walks.
#[tokio::test(start_paused = true)]
async fn contact_reads_the_applied_index_and_its_floor_at_once() {
    let f = fixture(&[&[1, 2, 3]]);
    for (index, at) in [(1, 10 * B), (2, 20 * B), (3, 30 * B)] {
        f.log.push(1, index, Some(at));
    }
    {
        let mut log = f.log.0.lock().unwrap();
        (log.applied, log.floor) = (1, 10 * B);
        log.apply_after_read = Some((3, 30 * B));
    }
    let contact = Contact::new(
        1,
        Timing::defaults(B),
        f.clock.clone(),
        Some(f.engine.clone()),
        f.log.clone(),
        1,
    );
    contact.log_changed();
    assert_eq!(contact.lock().scheduled.get(&2).map(|s| s.1), Some(20 * B));
}

/// A log purged past what contact took: the entries purged all
/// applied, so the floor rises over them, and the next entry is
/// listed and awaited at the index its apply derives at, not at its
/// own lower one.
#[tokio::test(start_paused = true)]
async fn the_floor_rises_over_entries_purged_before_contact_took_them() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let base = index_at(T0).next_multiple_of(B);
    f.log.push(1, 3, Some(base + 20 * B));
    {
        let mut log = f.log.0.lock().unwrap();
        (log.applied, log.floor) = (2, base + 40 * B);
        log.committed = Some(id(1, 2));
    }
    f.contact.log_changed();
    assert_eq!(
        f.contact.lock().scheduled.get(&3).map(|s| s.1),
        Some(base + 40 * B)
    );
}

/// A record of an entry purged from the log since it applied is
/// handed whatever its deadline: no decision names it by an id this
/// replica holds, so given unresolved it would degrade blocks until
/// an install.
#[tokio::test(start_paused = true)]
async fn a_purged_applied_record_is_handed_past_its_deadline() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let at = index_at(T0) + 40 * B;
    f.log.push(1, 1, Some(at));
    f.log.push(1, 2, Some(at + 400 * B));
    f.log.0.lock().unwrap().log.remove(&1);
    f.clock.set(Timing::defaults(B).apply_deadline(at) + 1);
    f.engine.calls();
    f.apply(2);
    f.contact.log_changed();
    let calls = f.engine.calls();
    assert_eq!(
        calls,
        ["appended 1", "appended 2", "committed 2"],
        "{calls:?}"
    );
}

/// The leader stamps nothing while [`FUTURE_LIST`] records of applied
/// entries take effect ahead, which every engine holds until then,
/// whatever entry derived them.
#[tokio::test(start_paused = true)]
async fn applied_records_still_ahead_fill_the_future_list() {
    let f = fixture(&[&[1]]);
    f.tick();
    let ahead = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
    {
        let mut log = f.log.0.lock().unwrap();
        for index in 1..=FUTURE_LIST as u64 {
            let record = Record {
                at_index: ahead,
                entries: vec![Entry::Gain {
                    input: 0,
                    output: 0,
                    gain: 0.5,
                    fade: None,
                }],
                generation: false,
            };
            log.records.insert(index, Arc::new(record));
        }
    }
    assert_eq!(f.contact.stamp(false), Err(StampRefusal::FutureListFull));
    f.log.0.lock().unwrap().records.pop_first();
    assert!(f.contact.stamp(false).is_ok());
}

/// A purge-point verdict naming this replica returns it to `starting`
/// once known committed, and the rebuild waits for its applied index
/// to pass the id's index; one naming another replica does nothing.
#[tokio::test(start_paused = true)]
async fn a_purge_point_verdict_rebuilds_once_the_applied_index_passed() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    f.insert(
        1,
        Body::PurgePoint {
            replica: 2,
            index: 5,
        },
    );
    f.insert(
        2,
        Body::PurgePoint {
            replica: 1,
            index: 5,
        },
    );
    f.apply(2);
    f.contact.log_changed();
    f.engine.calls();
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Starting));
    assert_eq!(f.contact.metrics().purge_points, 1);
    f.tick();
    assert!(!f.engine.calls().contains(&"rebuild".to_owned()));
    for index in 3..=5 {
        f.log.push(1, index, None);
    }
    f.apply(5);
    f.contact.log_changed();
    f.tick();
    assert!(f.engine.calls().contains(&"rebuild".to_owned()));
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
}

fn restamp_of(revision: u64, at_index: u64, paths: &[&str]) -> Body {
    Body::Restamp {
        revision,
        term: 1,
        at_index,
        paths: paths.iter().map(|p| (*p).to_owned()).collect(),
        generation: None,
    }
}

/// A restamp never undoes a later committed value: a restamped slot
/// change that a later committed entry on the same slot overtook is
/// superseded and voided. A product entry never is, whatever a later
/// one sets: its restamp re-asserts the state at its own place, which
/// holds every entry before it.
#[tokio::test(start_paused = true)]
async fn only_an_engine_body_a_later_entry_overtook_is_superseded() {
    use crate::state_machine::tests::command;
    let f = fixture(&[&[1, 2, 3]]);
    let at = index_at(T0) + 400 * 48;
    f.log.push(1, 1, Some(at));
    f.insert(2, restamp_of(1, at + 96, &[]));
    assert!(!f.contact.superseded(2, 2), "nothing later");
    f.insert(
        3,
        Body::InputUp {
            slot: 0,
            at_index: at + 480,
        },
    );
    assert!(
        f.contact.superseded(2, 3),
        "the restamp's original entry, overtaken on its slot"
    );
    assert!(!f.contact.superseded(2, 2), "uncommitted");
    let commands = |cs: &[(&str, &str)]| {
        Body::Commands(
            cs.iter()
                .map(|(id, body)| {
                    let mut c = command("s", id, body);
                    c.at_index = Some(at);
                    c
                })
                .collect(),
        )
    };
    f.insert(10, commands(&[("a", "gain=1@1"), ("b", "pan=1@1")]));
    f.insert(11, commands(&[("c", "gain=2@2"), ("d", "pan=2@2")]));
    assert!(!f.contact.superseded(10, 11), "a product entry");
}

/// A void naming an id at an index whose term this replica no longer
/// knows, purged from its own log, is no decision on an entry it
/// applied: it does not diverge.
#[tokio::test(start_paused = true)]
async fn a_void_on_an_index_this_log_purged_does_not_diverge() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    f.log.push(1, 4, None);
    f.insert(
        5,
        Body::Void {
            revision: 3,
            term: 7,
            at_index: None,
            generation: None,
        },
    );
    f.apply(5);
    f.contact.log_changed();
    f.tick();
    f.tick();
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
    assert_eq!(f.contact.metrics().contradicted, 0);
}

/// One purge-point verdict leads to exactly one rebuild: the entries
/// it supersedes are no longer held unresolved, so they are neither
/// reported again nor given to the rebuilt engine.
#[tokio::test(start_paused = true)]
async fn one_purge_point_verdict_rebuilds_once_and_clears_what_it_supersedes() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let timing = Timing::defaults(B);
    let at = index_at(T0) + 40 * 48;
    f.log.push(1, 1, Some(at));
    f.contact.log_changed();
    f.clock.set(timing.apply_deadline(at));
    let reported = |f: &Fixture| -> Vec<u64> {
        f.tick()[0]
            .2
            .own
            .unresolved
            .iter()
            .map(|l| l.id.index)
            .collect()
    };
    assert_eq!(reported(&f), [1]);
    f.insert(
        2,
        Body::PurgePoint {
            replica: 1,
            index: 1,
        },
    );
    f.apply(2);
    f.engine.calls();
    f.contact.log_changed();
    for _ in 0..20 {
        assert_eq!(reported(&f), Vec::<u64>::new());
    }
    let calls = f.engine.calls();
    let count = |c: &str| calls.iter().filter(|x| *x == c).count();
    assert_eq!((count("drop"), count("rebuild")), (1, 1), "{calls:?}");
    assert!(
        !calls.iter().any(|c| c.starts_with("unresolved")),
        "{calls:?}"
    );
    assert_eq!(f.state(), Some(ReplicaState::Withdrawn));
}

/// An entry this replica has not applied by its apply deadline is
/// unresolved, its commit known from reports or not; one applied in
/// time is handed.
#[tokio::test(start_paused = true)]
async fn an_entry_not_applied_by_its_deadline_is_unresolved() {
    let f = fixture(&[&[1, 2, 3]]);
    // Reports from voters count once a period named the voters.
    f.tick();
    let timing = Timing::defaults(B);
    // Three entries 40 ms ahead: one applied in time, one learned
    // committed in time but not applied, one neither.
    let at = index_at(T0) + 40 * 48;
    for index in 1..=3 {
        f.log.push(1, index, Some(at + index * B));
    }
    f.contact.log_changed();
    f.engine.calls();
    f.apply(1);
    f.contact.log_changed();
    assert_eq!(f.engine.calls(), ["appended 1", "committed 1"]);
    let mut r = f.report(2, None);
    r.committed = Some(id(1, 2));
    f.contact.receive(r);
    f.clock.set(timing.apply_deadline(at + 2 * B) - 1);
    f.tick();
    assert!(f.engine.calls().is_empty(), "before the deadline");
    f.clock.set(timing.apply_deadline(at + 2 * B));
    f.tick();
    assert_eq!(
        f.engine.calls(),
        [format!("unresolved 2 at {}", at + 2 * B)],
        "known committed, not applied: entry 2 is never handed"
    );
    f.clock.set(timing.apply_deadline(at + 3 * B));
    f.tick();
    assert_eq!(
        f.engine.calls(),
        [format!("unresolved 3 at {}", at + 3 * B)]
    );
}

/// An entry every voter reported committed before it applied here
/// still gives its slack as it applies: the slack of a replica that
/// applies late reaches the lead.
#[tokio::test(start_paused = true)]
async fn an_entry_committed_everywhere_before_it_applies_here_gives_its_slack() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let timing = Timing::defaults(B);
    let at = index_at(T0) + 400 * B;
    f.log.push(1, 1, Some(at));
    f.contact.log_changed();
    for from in [2, 3] {
        let mut r = f.report(from, None);
        r.committed = Some(id(1, 1));
        f.contact.receive(r);
    }
    assert_eq!(f.contact.metrics().known_committed, 1);
    f.tick();
    f.clock.set(timing.apply_deadline(at) - 1);
    f.apply(1);
    f.contact.log_changed();
    let slack: Vec<(u64, i64)> = (f.tick().into_iter())
        .flat_map(|(_, _, r)| r.slack)
        .map(|s| (s.index, s.samples))
        .collect();
    assert!(!slack.is_empty(), "no slack reported");
    assert!(
        slack
            .iter()
            .all(|&(index, samples)| index == 1 && samples < B as i64),
        "{slack:?}"
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
        baseline: false,
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
        // Index 80 lies past the listing horizon.
        let at = if index == 80 {
            now + 60 * 48
        } else {
            now + index * 30
        };
        f.log.push(1, index, Some(at));
    }
    f.contact.log_changed();
    let mut r = f.report(2, None);
    r.committed = Some(id(1, 3));
    f.contact.receive(r);
    let (_, _, report) = f.tick().pop().unwrap();
    let indices: Vec<u64> = report.listed.iter().map(|l| l.id.index).collect();
    let expected: Vec<u64> = (4..=67).collect();
    assert_eq!(indices, expected, "the earliest at_index first, 64 of them");
    assert_eq!(report.unlisted, Some(now + 68 * 30));
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
    let at = index_at(T0) + 400 * B;
    for index in 1..=3 {
        f.log.push(1, index, Some(at + index * B));
    }
    f.contact.log_changed();
    f.engine.calls();
    // Its successor asks for an earlier index: raised to the floor
    // of the entries before it, not of the entry it replaced.
    f.log.push(2, 3, Some(at + B));
    f.contact.log_changed();
    assert!(
        f.engine.calls().is_empty(),
        "nothing appended reaches the engine"
    );
    {
        let st = f.contact.lock();
        assert_eq!(st.scheduled[&3].0, id(2, 3));
        assert_eq!(st.scheduled[&3].1, at + 2 * B);
        let awaited: Vec<Key> = st.awaiting.keys().copied().collect();
        assert_eq!(awaited, [(1, 1, 1), (2, 1, 1), (3, 2, 1)]);
    }
    f.apply(3);
    f.contact.log_changed();
    let handed = f.engine.requests.lock().unwrap().last().cloned().unwrap();
    assert_eq!(handed.0, 3);
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
        baseline: false,
    }];
    f.contact.receive(r);
    let mut r = f.report(3, None);
    r.listed = vec![Listed {
        id: id(2, 5),
        at_index: early,
        baseline: false,
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
        baseline: false,
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

/// A part filling the given entries to the cap, with entries given
/// before, is asked about from its lowest entry: the cap trims after
/// the engine was asked, the oldest given first, rather than evicting
/// the part's own lowest entries as each is taken.
#[tokio::test(start_paused = true)]
async fn a_full_part_over_the_cap_asks_the_engine_about_its_lowest_entries() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    f.engine.calls();
    let at = index_at(T0) + 40 * 48;
    let first = 100u64;
    let part = (first..first + GIVEN_KEPT as u64).map(|i| ((i, 1, 1), at));
    {
        let mut st = f.contact.lock();
        let before = (1..=10u64).map(|i| ((i, 1, 1), at)).collect();
        f.contact.offer_unresolved(&mut st, before);
        f.engine.calls();
        f.contact.offer_unresolved(&mut st, part.collect());
    }
    let asked: Vec<String> = f.engine.calls();
    assert_eq!(asked.len(), GIVEN_KEPT, "the whole part asked");
    assert!(
        asked[0].starts_with(&format!("unresolved {first} ")),
        "lowest first: {:?}",
        asked[0]
    );
    let st = f.contact.lock();
    assert_eq!(st.given.len(), GIVEN_KEPT);
    assert!(st.given.keys().all(|k| k.0 >= first), "oldest forgotten");
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
        contact.verified_always();
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
            at_index: at,
            baseline: false,
        }]
    );
}

/// Review probe: an entry the engine had no room for, listed again
/// by the next reports, withdraws once.
#[tokio::test(start_paused = true)]
async fn review_a_relisted_entry_without_room_withdraws_once() {
    let f = fixture(&[&[1, 2, 3]]);
    f.tick();
    let timing = Timing::defaults(B);
    let at = index_at(T0) + 40 * 48;
    f.engine.room.store(0, Ordering::SeqCst);
    f.clock.set(timing.apply_deadline(at));
    for n in 0..5u64 {
        let mut r = f.report(2, None);
        r.seq += n;
        r.listed = vec![Listed {
            id: id(1, 9),
            at_index: at,
            baseline: false,
        }];
        f.contact.receive(r);
        f.tick();
    }
    let calls = f.engine.calls();
    let n = calls.iter().filter(|c| *c == "overwhelmed").count();
    assert_eq!(n, 1, "withdrawn {n} times: {calls:?}");
}

/// A report from `from` speaking `protocol`.
fn protocol_report(f: &Fixture, from: NodeId, protocol: u32) -> Report {
    let mut r = f.config_report(from, None);
    r.own.protocol = protocol;
    r
}

/// The gate opens only while every member, learners included, sent a
/// report within `w + D` speaking protocol 2: a member not heard from,
/// one speaking protocol 1 and one gone silent each keep it shut, and
/// the metric names it. A learner of protocol 1 is not promoted.
#[tokio::test(start_paused = true)]
async fn the_protocol_gate_opens_only_while_every_member_speaks_protocol_2() {
    let mut f = fixture(&[&[1, 2, 3]]);
    f.members.addrs.insert(9, "9".into());
    f.tick();
    assert_eq!(
        f.contact.protocol_blocker(&f.members),
        Some(2),
        "no report yet"
    );
    f.contact.receive(protocol_report(&f, 2, PROTOCOL));
    f.contact.receive(protocol_report(&f, 3, 1));
    assert_eq!(
        f.contact.protocol_blocker(&f.members),
        Some(3),
        "a voter at 1"
    );
    f.contact.receive(protocol_report(&f, 3, PROTOCOL));
    assert_eq!(
        f.contact.protocol_blocker(&f.members),
        Some(9),
        "a learner not heard"
    );
    f.contact.receive(protocol_report(&f, 9, 1));
    assert_eq!(
        f.contact.protocol_blocker(&f.members),
        Some(9),
        "a learner at 1"
    );
    assert!(f.contact.learner_holds_set_config(9));
    assert!(!f.contact.learner_promotable(9), "a learner at 1");
    let m = f.contact.metrics();
    assert!(!m.protocol_gate_open);
    assert_eq!(m.protocol_blocker, Some(9));
    f.contact.receive(protocol_report(&f, 9, PROTOCOL));
    assert!(f.contact.learner_promotable(9));
    assert_eq!(f.contact.protocol_blocker(&f.members), None);
    assert!(f.contact.metrics().protocol_gate_open);
    // A member added since the last tick, which contact holds no
    // report of yet, blocks as soon as it is in the membership passed.
    let mut added = f.members.clone();
    added.addrs.insert(10, "10".into());
    assert_eq!(
        f.contact.protocol_blocker(&added),
        Some(10),
        "a new learner"
    );
    let mut added = f.members.clone();
    added.configs[0].insert(11);
    added.addrs.insert(11, "11".into());
    assert_eq!(f.contact.protocol_blocker(&added), Some(11), "a new voter");
    // Voter 2 falls silent: its newest report ages past w + D.
    let fresh = W + Timing::defaults(B).hold;
    f.clock.add(fresh);
    tokio::time::advance(fresh + Duration::from_millis(1)).await;
    f.contact.receive(protocol_report(&f, 3, PROTOCOL));
    f.contact.receive(protocol_report(&f, 9, PROTOCOL));
    assert_eq!(
        f.contact.protocol_blocker(&f.members),
        Some(2),
        "a silent voter"
    );
    assert_eq!(f.contact.metrics().protocol_blocker, Some(2));
}

/// A generation stamp leaves room for the slowest build, but never
/// further than `AHEAD_MAX` past its stamp, where the entry would take
/// effect at the stamp itself.
#[tokio::test(start_paused = true)]
async fn a_generation_stamp_stays_within_ahead_max_of_its_stamp() {
    let f = fixture(&[&[1]]);
    f.engine.build_time.store(4800, Ordering::SeqCst);
    f.tick();
    let s = f.contact.stamp(true).expect("stamped");
    assert_eq!(
        s.earliest,
        s.stamp + index_at(START_LEAD.as_nanos() as u64) + 4800
    );
    let f = fixture(&[&[1]]);
    f.engine
        .build_time
        .store(2 * crate::state_machine::AHEAD_MAX, Ordering::SeqCst);
    f.tick();
    let s = f.contact.stamp(true).expect("stamped");
    assert_eq!(
        s.earliest,
        s.stamp + crate::state_machine::AHEAD_MAX / B * B
    );
}

/// An appended entry that needs a generation never lowers the
/// generation a re-routing command's stamp reserved, and a `Commands`
/// entry the leader flagged for its generation counts until it
/// applies, so a leader that only appended it waits for it too.
#[tokio::test(start_paused = true)]
async fn appended_entries_never_lower_the_pending_generation() {
    let f = fixture(&[&[1]]);
    f.tick();
    let raised = f.contact.stamp(true).expect("stamped").earliest;
    let far = raised + 400 * B;
    f.insert(
        1,
        Body::InputUp {
            slot: 0,
            at_index: raised - B,
        },
    );
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.contact.lock().reserved, Some(raised));

    let restarted = Contact::new(
        1,
        Timing::defaults(B),
        f.clock.clone(),
        Some(f.engine.clone()),
        f.log.clone(),
        1,
    );
    restarted.verified_always();
    let mut command = crate::state_machine::tests::command("s", "1", "route");
    command.at_index = Some(far);
    command.generation = Some(true);
    f.insert(2, Body::Commands(vec![command]));
    restarted.tick(f.members.clone());
    assert_eq!(restarted.lock().generations.get(&2), Some(&far));
    assert_eq!(restarted.stamp(true), Err(StampRefusal::GenerationPending));
}

/// A command flagged for its generation that the product refused
/// builds none: once it applied, the applied state, the source a
/// restarted contact reads, names no generation for it, and the
/// appended entries no longer do. On a live leader the stamp that
/// ordered it still reserves its index, for at most its lead
/// (`State::reserved`); this contact never stamped it.
#[tokio::test(start_paused = true)]
async fn an_applied_flagged_command_that_built_nothing_holds_no_generation() {
    let f = fixture(&[&[1]]);
    f.tick();
    let far = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
    let mut command = crate::state_machine::tests::command("s", "1", "route");
    command.at_index = Some(far);
    command.generation = Some(true);
    f.insert(1, Body::Commands(vec![command]));
    f.contact.log_changed();
    f.tick();
    assert_eq!(f.contact.stamp(true), Err(StampRefusal::GenerationPending));
    // Applied, refused by the product: no generation in the state.
    f.apply(1);
    f.contact.log_changed();
    assert!(f.contact.stamp(true).is_ok());
}

/// A replica restarted after it applied an entry that re-routed an
/// input, whose index has not passed, waits for that generation before
/// it stamps another: two would be pending.
#[tokio::test(start_paused = true)]
async fn a_restarted_contact_waits_for_an_applied_generation() {
    let f = fixture(&[&[1]]);
    f.tick();
    let far = f.contact.stamp(false).expect("stamped").stamp + 400 * B;
    f.log.0.lock().unwrap().generation = Some(far);
    let restarted = Contact::new(
        1,
        Timing::defaults(B),
        f.clock.clone(),
        Some(f.engine.clone()),
        f.log.clone(),
        1,
    );
    restarted.verified_always();
    restarted.tick(f.members.clone());
    assert_eq!(restarted.stamp(true), Err(StampRefusal::GenerationPending));
}

/// A stamped void or lease expiry derives at its index like a
/// command: listed and awaited there. A void from an older leader,
/// which stamps none, takes effect nowhere and is neither.
#[tokio::test(start_paused = true)]
async fn a_stamped_void_or_expiry_is_awaited_where_it_derives() {
    let f = fixture(&[&[1, 2]]);
    f.tick();
    let at = index_at(T0) + 400 * B;
    let void = |at_index| Body::Void {
        revision: 1,
        term: 1,
        at_index,
        generation: None,
    };
    f.insert(4, void(None));
    f.insert(5, void(Some(at)));
    f.insert(
        6,
        Body::LeaseExpiry {
            session: crate::entry::SessionKey::from_parts("p", "s"),
            renewed: 0,
            at_index: Some(at + B),
        },
    );
    f.contact.log_changed();
    f.tick();
    let st = f.contact.lock();
    let scheduled: Vec<(u64, u64)> = st.scheduled.iter().map(|(&i, e)| (i, e.1)).collect();
    assert_eq!(scheduled, [(5, at), (6, at + B)]);
    let awaited: Vec<u64> = st.awaiting.keys().map(|k| k.0).collect();
    assert_eq!(awaited, [5, 6]);
}

/// Slack of an entry an earlier term stamped, committed once this
/// voter leads, is the election's lateness: it does not raise the
/// lead. The same lateness on an entry of the leader's own term does.
#[test]
fn slack_of_an_earlier_terms_entry_does_not_raise_the_lead() {
    let f = fixture(&[&[1, 2]]);
    f.log.push(1, 5, Some(index_at(T0) + 400 * B));
    f.log.push(2, 6, Some(index_at(T0) + 401 * B));
    f.contact.in_term(2);
    let late = |index| Slack {
        index,
        samples: -(40 * B as i64),
        stamp: 0,
    };
    let mut st = f.contact.lock();
    let start = st.lead.samples();
    add_slack(&mut st, f.log.as_ref(), 2, &late(5), 0);
    assert_eq!(st.lead.samples(), start, "term 1 entry under term 2");
    assert!(st.lead_raised_by.is_none());
    add_slack(&mut st, f.log.as_ref(), 2, &late(6), 0);
    assert!(st.lead.samples() > start, "term 2 entry raises it");
    assert_eq!(st.lead_raised_by, Some(2));
}

/// A new term keeps the lead a voter holds and voids its cap flag and
/// slack; the same term again changes nothing.
#[test]
fn a_new_term_keeps_the_lead_and_voids_the_cap() {
    let f = fixture(&[&[1, 2]]);
    f.log.push(2, 6, Some(index_at(T0) + 401 * B));
    f.contact.in_term(2);
    {
        let mut st = f.contact.lock();
        let slack = Slack {
            index: 6,
            samples: -(4000 * B as i64),
            stamp: 0,
        };
        add_slack(&mut st, f.log.as_ref(), 2, &slack, 0);
        assert!(st.lead.capped);
    }
    let raised = f.contact.lock().lead.samples();
    f.contact.in_term(2);
    assert!(f.contact.lock().lead.capped, "the same term keeps it");
    f.contact.in_term(3);
    let st = f.contact.lock();
    assert_eq!(st.lead.samples(), raised, "the lead is kept");
    assert!(!st.lead.capped);
    assert!(st.lead_raised_by.is_none());
}

/// A follower, the next leader, skips the slack of an earlier term's
/// entry too, and counts one it holds no term for.
#[test]
fn a_follower_skips_slack_of_an_earlier_terms_entry() {
    let f = fixture(&[&[1, 2]]);
    f.log.push(1, 5, Some(index_at(T0) + 400 * B));
    f.contact.in_term(2);
    let late = |index| Slack {
        index,
        samples: -(40 * B as i64),
        stamp: 0,
    };
    let mut st = f.contact.lock();
    let start = st.lead.samples();
    add_slack(&mut st, f.log.as_ref(), 2, &late(5), 0);
    assert_eq!(st.lead.samples(), start);
    add_slack(&mut st, f.log.as_ref(), 2, &late(99), 0);
    assert!(st.lead.samples() > start, "no term known: counted");
}

/// The real replicated engine of three voters on an MXL domain under
/// `/dev/shm`, one input nobody writes and one output, behind the
/// bridge, with every call contact makes on it about entries
/// recorded.
struct Real {
    audio: crate::bridge::EngineAudio,
    calls: Mutex<Vec<(&'static str, u64)>>,
    dir: std::path::PathBuf,
}

impl Drop for Real {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Real {
    fn start(timing: &Timing) -> Arc<Real> {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".into());
        let dir = std::path::PathBuf::from("/dev/shm").join(format!(
            "samplez-contact-{}-{:016x}",
            std::process::id(),
            crate::bootstrap::random_id().expect("a random id")
        ));
        std::fs::create_dir(&dir).unwrap();
        let v = serde_json::json!({
            "blockSize": B,
            "readerMarginSamples": 960,
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": dir }],
            "inputs": [{ "name": "in0", "domain": "d",
                         "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8a", "channels": 1 }],
            "outputs": [{ "name": "out0", "domain": "d",
                          "flowId": "8f2b8f6e-0d8a-4b8e-9d55-2f4c1f6f7f01", "label": "contact",
                          "group": "samplez-test", "role": "Audio 0", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "rtCpu": "none" },
        });
        let config = samplez::Config::parse(&v.to_string()).unwrap();
        let (reports, _) = std::sync::mpsc::sync_channel(64);
        let (status_tx, status_rx) = std::sync::mpsc::sync_channel(1 << 10);
        let (hash_tx, hash_rx) = std::sync::mpsc::sync_channel(1 << 10);
        let process = samplez::rt::setup_process(&config.rt);
        let replication = samplez::Replication {
            voters: 3,
            reports,
            statuses: Some(status_tx),
            hashes: Some(hash_tx),
        };
        let engine = Arc::new(
            samplez::Engine::start_replicated(config, process, None, replication).unwrap(),
        );
        let started = std::time::Instant::now();
        while !engine.status().outputs.iter().all(|o| o.realised) {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "outputs realised"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let audio = crate::bridge::EngineAudio::new(engine, status_rx, hash_rx, timing);
        Arc::new(Real {
            audio,
            calls: Mutex::new(Vec::new()),
            dir,
        })
    }

    fn record(&self, call: &'static str, index: u64) {
        self.calls.lock().unwrap().push((call, index));
    }

    fn calls(&self) -> Vec<(&'static str, u64)> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}

impl Audio for Real {
    fn sample(&self) -> Sample {
        self.audio.sample()
    }
    fn fence(&self, end: u64) {
        self.audio.fence(end)
    }
    fn settled(&self, k: u64) -> bool {
        self.audio.settled(k)
    }
    fn appended(&self, index: u64, at_index: u64, entries: &[Entry]) {
        self.record("appended", index);
        self.audio.appended(index, at_index, entries)
    }
    fn committed(&self, up_to: u64) {
        self.audio.committed(up_to)
    }
    fn unresolved(&self, index: u64, at_index: u64) -> bool {
        let taken = self.audio.unresolved(index, at_index);
        self.record(if taken { "unresolved" } else { "refused" }, index);
        taken
    }
    fn overwhelmed(&self) {
        self.record("overwhelmed", 0);
        self.audio.overwhelmed()
    }
    fn status(&self, output: usize, k: u64) -> Option<BlockStatus> {
        self.audio.status(output, k)
    }
    fn withhold(&self, output: usize, k: u64, cause: Cause) -> Option<BlockStatus> {
        self.audio.withhold(output, k, cause)
    }
    fn commit_through(&self, output: usize, k: u64, on: u8, up_to: Option<u64>) -> bool {
        self.audio.commit_through(output, k, on, up_to)
    }
    fn withdraw(&self) {
        self.audio.withdraw()
    }
    fn ranks(&self, ranks: Ranks) {
        self.audio.ranks(ranks)
    }
    fn commit_from(&self, output: usize, min_k: u64, seen: u64) {
        self.audio.commit_from(output, min_k, seen)
    }
    fn cancel_commit_start(&self, output: usize) {
        self.audio.cancel_commit_start(output)
    }
    fn stop_at(&self, output: usize, at: u64) {
        self.audio.stop_at(output, at)
    }
    fn resolved(&self, index: u64) {
        self.record("resolved", index);
        self.audio.resolved(index)
    }
    fn drop_engine(&self) {
        self.record("drop", 0);
        self.audio.drop_engine()
    }
    fn rebuild_engine(&self, state: &ProductState, at_least: u64) -> bool {
        self.audio.rebuild_engine(state, at_least)
    }
    fn follows(&self, state: &ProductState) -> bool {
        self.audio.follows(state)
    }
}

/// Contact of voter 1 of three on the real engine, its clock at the
/// engine's TAI time.
fn real() -> (Arc<Clock>, Arc<Log>, Arc<Real>, Contact) {
    let tai = SystemClocks.tai_ns();
    let clock = Arc::new(Clock {
        now: AtomicU64::new(tai),
        step: AtomicU64::new(0),
    });
    let log = Arc::new(Log::default());
    let timing = Timing::defaults(B);
    let engine = Real::start(&timing);
    let contact = Contact::new(
        1,
        timing,
        clock.clone(),
        Some(engine.clone()),
        log.clone(),
        100,
    );
    contact.verified_always();
    (clock, log, engine, contact)
}

/// The longest any one hold of `contact`'s lock lasted while `f`
/// ran, sampled every 200 us.
fn longest_hold(contact: &Contact, f: impl FnOnce()) -> Duration {
    let record = contact.state.record();
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        let sampler = s.spawn(|| {
            let mut longest = Duration::ZERO;
            while !done.load(Ordering::SeqCst) {
                let now = std::time::Instant::now();
                longest = longest.max(record.held_for(now).unwrap_or_default());
                std::thread::sleep(Duration::from_micros(200));
            }
            longest
        });
        f();
        done.store(true, Ordering::SeqCst);
        sampler.join().unwrap()
    })
}

/// Pushes entries `indices` to `log`, each stamped `stamp(index)` and
/// taking effect at `at(index)`.
fn push_all(log: &Log, indices: std::ops::RangeInclusive<u64>, at: impl Fn(u64) -> u64) {
    for index in indices {
        log.push(1, index, Some(at(index)));
    }
}

/// A replica back after `away` blocks at the densest entry rate, one
/// entry a block, the newest stamped at the longest lead, takes the
/// catch-up in one log change on the real engine. The longest hold of
/// the contact lock and the most entries read of the log in one hold.
fn catch_up(away: u64) -> (Duration, usize) {
    let (clock, log, engine, contact) = real();
    let now = index_at(clock.get()) / B * B;
    let lead = crate::state_machine::AHEAD_MAX;
    let fresh = lead / B;
    let at = |index: u64| {
        let stamp = now - (away - index) * B;
        if away - index < fresh {
            stamp + lead
        } else {
            stamp + B
        }
    };
    push_all(&log, 1..=away, at);
    let timing = contact.timing();
    let in_time = (1..=away)
        .filter(|&i| clock.get() < timing.apply_deadline(at(i)))
        .count();
    f_apply(&log, away);
    let started = std::time::Instant::now();
    let longest = longest_hold(&contact, || contact.log_changed());
    let calls = engine.calls();
    let count = |c: &str| calls.iter().filter(|x| x.0 == c).count();
    let visited = log.0.lock().unwrap().visited_most;
    println!(
        "{away} entries taken in {:?}, the contact lock held {longest:?} at most, \
             {visited} entries read of the log at once; engine: {} unresolved, \
             {} refused, {} overwhelmed, {} appended",
        started.elapsed(),
        count("unresolved"),
        count("refused"),
        count("overwhelmed"),
        count("appended"),
    );
    assert!(in_time > 0, "some applied in time");
    assert_eq!(count("appended"), in_time, "those applied in time handed");
    assert!(count("overwhelmed") <= 1, "overwhelmed once at most");
    assert!(
        count("refused") as u64 <= away / TAKEN_AT_ONCE as u64 + 1,
        "the engine not asked again after a refusal"
    );
    (longest, visited)
}

/// A replica back after a minute away, and one whose log still holds
/// five minutes of entries, take the catch-up with the contact lock
/// held briefly at a time, reading only the part of the log each hold
/// takes: the entries taken late are offered unresolved, which the
/// engine refuses past its few, and the newest are applied in time.
/// Taken in one hold, the minute held the lock about 0.8 s in a debug
/// build, growing with the time away towards the lock watchdog's
/// limit.
#[test]
fn a_catch_up_after_a_minute_away_holds_the_contact_lock_briefly() {
    let limit = Duration::from_millis(250);
    for away in [60_000, 300_000] {
        let (longest, visited) = catch_up(away);
        assert!(
            longest < limit,
            "the contact lock held {longest:?}, over {limit:?}"
        );
        assert!(
            visited <= TAKEN_AT_ONCE,
            "{visited} entries read of the log under the lock at once"
        );
    }
}

/// Entries up to `index` applied and committed.
fn f_apply(log: &Log, index: u64) {
    let mut l = log.0.lock().unwrap();
    l.applied = index;
    l.committed = Some(id(1, index));
}

/// Past [`AWAITING_MAX`] awaited entries, more appended before any is
/// applied are offered unresolved: the engine takes its few and
/// refuses the rest, so contact gives more than [`GIVEN_KEPT`]. The
/// ones the engine holds stay given and are not handed when they
/// apply in time: handed, the engine would apply an entry it also
/// holds unresolved, degrading its blocks from there for good, and
/// the replica would no longer list it for the log to decide.
#[test]
fn an_entry_the_engine_holds_unresolved_is_not_handed_as_applied() {
    let (clock, log, engine, contact) = real();
    let now = index_at(clock.get()) / B * B;
    let lead = crate::state_machine::AHEAD_MAX;
    let entries = (AWAITING_MAX + GIVEN_KEPT + 100) as u64;
    push_all(&log, 1..=entries, |index| now + lead + index);
    contact.log_changed();
    let calls = engine.calls();
    let count = |c: &str| calls.iter().filter(|x| x.0 == c).count();
    assert_eq!(count("overwhelmed"), 1, "overwhelmed once");
    assert!(
        count("refused") <= entries as usize / TAKEN_AT_ONCE + 1,
        "the engine not asked again after a refusal: {} refusals",
        count("refused")
    );
    let held: BTreeSet<u64> = (calls.iter())
        .filter(|c| c.0 == "unresolved")
        .map(|c| c.1)
        .collect();
    let refused = contact.lock().counts.unresolved_refused;
    assert!(!held.is_empty(), "the engine held some");
    assert!(
        held.len() + refused as usize > GIVEN_KEPT,
        "more given than kept: {} held, {refused} refused",
        held.len()
    );
    f_apply(&log, entries);
    contact.log_changed();
    let calls = engine.calls();
    let handed: BTreeSet<u64> = (calls.iter())
        .filter(|c| c.0 == "appended")
        .map(|c| c.1)
        .collect();
    let resolved = calls.iter().any(|c| c.0 == "resolved");
    assert!(!resolved, "nothing decided");
    let both: Vec<u64> = held.intersection(&handed).copied().collect();
    println!(
        "{} held unresolved, {refused} refused, {} handed as applied, {} of them held",
        held.len(),
        handed.len(),
        both.len()
    );
    assert!(both.is_empty(), "held unresolved and handed: {both:?}");
    assert!(
        (1..=AWAITING_MAX as u64).all(|i| handed.contains(&i)),
        "the awaited ones handed"
    );
    let st = contact.lock();
    let given: BTreeSet<u64> = st.given.keys().map(|k| k.0).collect();
    assert!(
        held.is_subset(&given),
        "every entry the engine holds still given, for the log to decide"
    );
}

/// A replica committing nothing back after a minute away rebuilds its
/// engine past the entries it applied late once, whatever parts it
/// takes them in, and never asks the engine to hold one unresolved:
/// asked, the engine, which holds a few, would be overwhelmed.
#[tokio::test(start_paused = true)]
async fn a_replica_committing_nothing_rebuilds_past_a_minute_away_once() {
    let f = fixture(&[&[1, 2, 3]]);
    f.engine.outputs.store(true, Ordering::SeqCst);
    f.engine.room.store(64, Ordering::SeqCst);
    f.tick();
    assert!(f.contact.lock().catching_up);
    f.engine.calls();
    let now = index_at(f.clock.get()) / B * B;
    let away = 60_000;
    for index in 1..=away {
        f.log.push(1, index, Some(now - (away - index + 100) * B));
    }
    f.apply(away);
    f.contact.log_changed();
    let calls = f.engine.calls();
    let count = |p: &str| calls.iter().filter(|c| c.starts_with(p)).count();
    println!(
        "{} drops, {} unresolved, {} refused, {} overwhelmed, {} late rebuilds",
        count("drop"),
        count("unresolved"),
        count("refused"),
        count("overwhelmed"),
        f.contact.metrics().late_rebuilds
    );
    assert_eq!(count("drop"), 1, "one restart");
    assert_eq!(f.contact.metrics().late_rebuilds, 1);
    assert_eq!(count("overwhelmed"), 0);
    assert_eq!(count("unresolved") + count("refused"), 0, "nothing offered");
    assert!(f.contact.lock().given.is_empty(), "rebuilt past, not held");
    f.tick();
    assert_eq!(f.contact.metrics().late_rebuilds, 1, "nothing left to fold");
}

/// Three voters, each a contact over a fake engine and log of its
/// own on one clock, whose reports a test delivers or drops.
struct Net {
    clock: Arc<Clock>,
    members: Members,
    nodes: BTreeMap<NodeId, (Contact, Arc<Engine>)>,
}

impl Net {
    /// `as_before`: every node reports and relays as before
    /// [`Carried`].
    fn new(clock: &Arc<Clock>, as_before: bool) -> Net {
        let ids = [1, 2, 3];
        let nodes = ids
            .into_iter()
            .map(|id| (id, Net::node(clock, id, id * 1000, as_before)))
            .collect();
        Net {
            clock: clock.clone(),
            members: Members {
                configs: vec![ids.into_iter().collect()],
                addrs: ids.into_iter().map(|v| (v, v.to_string())).collect(),
            },
            nodes,
        }
    }

    fn node(
        clock: &Arc<Clock>,
        id: NodeId,
        first_seq: u64,
        as_before: bool,
    ) -> (Contact, Arc<Engine>) {
        let engine = engine(clock);
        let log = Arc::new(Log::default());
        let contact = Contact::new(
            id,
            Timing::defaults(B),
            clock.clone(),
            Some(engine.clone()),
            log,
            first_seq,
        );
        contact.verified_always();
        if as_before {
            contact.as_before();
        }
        (contact, engine)
    }

    fn contact(&self, id: NodeId) -> &Contact {
        &self.nodes[&id].0
    }

    /// Replaces `id` by a process that holds nothing, its sequence
    /// numbers from `first_seq` on.
    fn restart(&mut self, id: NodeId, first_seq: u64) {
        let node = Net::node(&self.clock, id, first_seq, false);
        self.nodes.insert(id, node);
    }

    /// Every node ticks, and each report reaches its addressee unless
    /// `lost(from, to)`. Returns the reports, delivered or not.
    fn period(&self, lost: impl Fn(NodeId, NodeId) -> bool) -> Vec<(NodeId, NodeId, Report)> {
        self.period_with(lost, |_, _, _| {})
    }

    /// [`Net::period`], each report changed by `edit` on its way.
    fn period_with(
        &self,
        lost: impl Fn(NodeId, NodeId) -> bool,
        edit: impl Fn(NodeId, NodeId, &mut Report),
    ) -> Vec<(NodeId, NodeId, Report)> {
        let mut sent: Vec<(NodeId, NodeId, Report)> = (self.nodes.iter())
            .flat_map(|(&from, (c, _))| {
                let reports = c.tick(self.members.clone());
                reports.into_iter().map(move |(to, _, r)| (from, to, r))
            })
            .collect();
        for (from, to, report) in &mut sent {
            edit(*from, *to, report);
            if !lost(*from, *to) {
                self.contact(*to).receive(report.clone());
            }
        }
        sent
    }

    /// One report period on, on the TAI clock and the monotonic one.
    async fn advance(clock: &Clock) {
        clock.add(Duration::from_millis(2));
        tokio::time::advance(Duration::from_millis(2)).await;
    }
}

fn records(r: &Report) -> usize {
    r.own.pages.len()
        + (r.heard.iter())
            .filter_map(|h| h.own.as_ref())
            .map(|o| o.pages.len())
            .sum::<usize>()
}

/// A receiver fed by senders that carry each page record until it
/// confirms it holds the same page book and the same copies, and
/// decides the same, as one in a set reporting as before: every
/// record in every report, copies keeping theirs and relaying them.
/// Through lost reports, a lost echo, and a voter it hears only
/// through a relay. The trimmed reports carry no record once every
/// one is confirmed.
#[tokio::test(start_paused = true)]
async fn a_receiver_holds_the_same_whether_records_are_trimmed_or_all_sent() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let all = Net::new(&clock, true);
    let trimmed = Net::new(&clock, false);
    let lost = |round: u64, from: NodeId, to: NodeId| {
        (from == 1 && to == 2 && (5..8).contains(&round))
            || (from == 2 && to == 1 && round == 12)
            || (from == 3 && to == 2 && (20..40).contains(&round))
    };
    let mut steady = 0;
    for round in 0..60u64 {
        for net in [&all, &trimmed] {
            if round % 5 == 0 {
                net.nodes[&1].1.page(round / 5);
                net.nodes[&3].1.page(100 + round / 5);
            }
        }
        all.period(|f, t| lost(round, f, t));
        let sent = trimmed.period(|f, t| lost(round, f, t));
        Net::advance(&clock).await;
        assert_eq!(
            all.contact(2).pages_and_copies(),
            trimmed.contact(2).pages_and_copies(),
            "round {round}"
        );
        if round == 39 {
            // While 3 did not reach 2, its records came through 1.
            let newest = trimmed.contact(2).latest_pages(3);
            assert!(newest.iter().any(|p| p.page == 107), "{newest:?}");
        }
        // Confirmed two periods after a record is first carried, three
        // when relayed; the record of round 5 waits out its lost
        // reports. While 2 does not hear 3, 1 relays 3's newest whole.
        if round % 5 == 4 && round != 9 {
            let to_2 = &sent.iter().find(|(f, t, _)| (*f, *t) == (1, 2)).unwrap().2;
            let carried = if (21..=41).contains(&round) {
                to_2.own.pages.len()
            } else {
                records(to_2)
            };
            assert_eq!(carried, 0, "round {round}: all confirmed");
            steady += 1;
        }
    }
    assert!(steady > 5);
    assert_eq!(all.contact(2).decisions(1), trimmed.contact(2).decisions(1));
}

/// A record is carried in every report from the first that carries
/// it, the lost ones too, until the addressee echoes one of them.
#[tokio::test(start_paused = true)]
async fn a_record_is_carried_until_a_report_with_it_is_echoed() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let net = Net::new(&clock, false);
    for _ in 0..3 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    net.nodes[&1].1.page(7);
    let to_2 = |sent: &[(NodeId, NodeId, Report)]| {
        records(&sent.iter().find(|(f, t, _)| (*f, *t) == (1, 2)).unwrap().2)
    };
    // Its first two reports to 2 are lost, and 2 echoes older ones.
    for _ in 0..2 {
        let sent = net.period(|f, t| (f, t) == (1, 2));
        assert_eq!(to_2(&sent), 2, "both outputs' records, until confirmed");
        Net::advance(&clock).await;
    }
    let sent = net.period(|_, _| false);
    assert_eq!(to_2(&sent), 2, "carried again: no echo of a report with it");
    Net::advance(&clock).await;
    // 2's report echoing it arrives after 1 built this one.
    let sent = net.period(|_, _| false);
    assert_eq!(to_2(&sent), 2, "its echo is on the way");
    Net::advance(&clock).await;
    let sent = net.period(|_, _| false);
    assert_eq!(to_2(&sent), 0, "confirmed");
    assert!(net.contact(2).latest_pages(1).iter().any(|p| p.page == 7));
}

/// An addressee restarted with no record is sent every one again,
/// each rule alone: a sequence number of another process, a report
/// with no echo, an echo of no report the sender sent, and a silence
/// longer than w.
#[tokio::test(start_paused = true)]
async fn a_restarted_addressee_is_sent_every_record_again() {
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Case {
        OtherProcess,
        NoEcho,
        UnknownEcho,
        Silent,
    }
    use Case::*;
    for case in [OtherProcess, NoEcho, UnknownEcho, Silent] {
        let clock = Arc::new(Clock {
            now: AtomicU64::new(T0),
            step: AtomicU64::new(0),
        });
        let mut net = Net::new(&clock, false);
        net.nodes[&1].1.page(1);
        net.nodes[&1].1.page(2);
        let mut last_seq = 0;
        for _ in 0..10 {
            let sent = net.period(|_, _| false);
            last_seq = sent.iter().find(|(f, _, _)| *f == 2).unwrap().2.seq;
            Net::advance(&clock).await;
        }
        assert_eq!(net.contact(1).carried_len(2), 4, "{case:?}: confirmed");
        // Each case but one goes on with 2's sequence numbers, and
        // all but `Silent` report to 1 within w of the last report.
        net.restart(
            2,
            if case == OtherProcess {
                1 << 40
            } else {
                last_seq
            },
        );
        let from_2 = |f: NodeId, t: NodeId| (f, t) == (2, 1);
        match case {
            // Its first report is lost: its next one echoes a report
            // of 1's it took, under a sequence number of its own.
            OtherProcess => {
                net.period(from_2);
            }
            // Its first report reaches 1 and echoes nothing.
            NoEcho => {}
            // Its first report is lost, its next one echoes a report
            // 1 never sent.
            UnknownEcho => {
                net.period(from_2);
                Net::advance(&clock).await;
                net.period_with(
                    |_, _| false,
                    |f, t, r| {
                        if (f, t) == (2, 1) {
                            r.echo.as_mut().unwrap().seq = 1;
                        }
                    },
                );
            }
            // Nothing of it reaches 1 for longer than w.
            Silent => {
                for _ in 0..5 {
                    net.period(|f, _| f == 2);
                    Net::advance(&clock).await;
                }
            }
        }
        Net::advance(&clock).await;
        for _ in 0..3 {
            net.period(|_, _| false);
            Net::advance(&clock).await;
        }
        let sorted = |mut pages: Vec<Page>| {
            pages.sort_by_key(|p| (p.output, p.page));
            pages
        };
        assert_eq!(
            sorted(net.contact(2).latest_pages(1)),
            sorted(net.nodes[&1].1.pages.lock().unwrap().clone()),
            "{case:?}"
        );
    }
}

/// A confirmed record that changes is a new record: carried again
/// until confirmed, and the addressee holds the new one.
#[tokio::test(start_paused = true)]
async fn a_confirmed_record_that_changes_is_sent_again() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let net = Net::new(&clock, false);
    net.nodes[&1].1.page(1);
    for _ in 0..5 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    let to_2 = |sent: &[(NodeId, NodeId, Report)]| {
        records(&sent.iter().find(|(f, t, _)| (*f, *t) == (1, 2)).unwrap().2)
    };
    assert_eq!(to_2(&net.period(|_, _| false)), 0, "confirmed");
    Net::advance(&clock).await;
    net.nodes[&1].1.rehash(1);
    assert_eq!(
        to_2(&net.period(|_, _| false)),
        2,
        "both outputs' changed records"
    );
    Net::advance(&clock).await;
    let held = net.contact(2).latest_pages(1);
    assert_eq!(
        held,
        *net.nodes[&1].1.pages.lock().unwrap(),
        "the changed ones"
    );
}

/// A record relayed in copies the addressee dropped for a newer
/// direct report, and confirmed with them, still reaches the
/// addressee when the direct link has failed: that newer direct report
/// was built before the record, and the relayer sends a voter's newest
/// records whole while the addressee hears it no more.
#[tokio::test(start_paused = true)]
async fn a_record_in_a_dropped_relay_reaches_the_addressee_once_its_link_fails() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let net = Net::new(&clock, false);
    for _ in 0..5 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    // From here on 3 does not reach 2; one report of 3's, built
    // before page 9, is held back.
    let cut = |f: NodeId, t: NodeId| (f, t) == (3, 2);
    let sent = net.period(cut);
    let late = sent
        .into_iter()
        .find(|(f, t, _)| (*f, *t) == (3, 2))
        .unwrap()
        .2;
    Net::advance(&clock).await;
    net.nodes[&3].1.page(9);
    // 1 takes page 9 from 3.
    net.period(cut);
    Net::advance(&clock).await;
    // 1's first report relaying it is lost.
    let sent = net.period(|f, t| cut(f, t) || (f, t) == (1, 2));
    let relayed = |sent: &[(NodeId, NodeId, Report)]| {
        let r = &sent.iter().find(|(f, t, _)| (*f, *t) == (1, 2)).unwrap().2;
        let h = r.heard.iter().find(|h| h.node == 3).unwrap();
        h.own.as_ref().unwrap().pages.iter().any(|p| p.page == 9)
    };
    assert!(relayed(&sent));
    Net::advance(&clock).await;
    // The held-back report reaches 2 now: newer than the copies 1
    // relays next, which 2 drops.
    net.contact(2).receive(late);
    for _ in 0..2 {
        let sent = net.period(cut);
        assert!(relayed(&sent), "carried until 2 echoes one");
        Net::advance(&clock).await;
    }
    let sent = net.period(cut);
    assert!(!relayed(&sent), "confirmed");
    Net::advance(&clock).await;
    assert!(
        !net.contact(2).latest_pages(3).iter().any(|p| p.page == 9),
        "2 dropped every copy with page 9"
    );
    for _ in 0..8 {
        net.period(cut);
        Net::advance(&clock).await;
    }
    let held = net.contact(2).latest_pages(3);
    assert!(held.iter().any(|p| p.page == 9), "{held:?}");
}

/// A period offering the records of the last one builds no offer,
/// confirmations meanwhile included: no record is cloned, read from
/// the page book or looked up. A new record builds one again, once
/// per addressee.
#[tokio::test(start_paused = true)]
async fn an_unchanged_period_builds_no_offer() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let net = Net::new(&clock, false);
    net.nodes[&1].1.page(1);
    net.nodes[&3].1.page(1);
    for _ in 0..6 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    let built = net.contact(1).offers_built();
    for _ in 0..20 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    assert_eq!(net.contact(1).offers_built(), built, "20 unchanged periods");
    net.nodes[&1].1.page(2);
    for _ in 0..6 {
        net.period(|_, _| false);
        Net::advance(&clock).await;
    }
    assert_eq!(
        net.contact(1).offers_built(),
        built + 2,
        "one per addressee, its confirmation building none"
    );
}

/// A set whose offers come from the last period's, where nothing
/// changed, sends exactly the reports of one building every offer
/// anew: over new records on two voters, a sender's record dropped
/// with nothing newer while an addressee has not confirmed it, lost
/// reports and echoes confirming records to one addressee and not
/// yet to the other.
#[tokio::test(start_paused = true)]
async fn offers_from_the_last_period_carry_what_a_rebuild_does() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let cached = Net::new(&clock, false);
    let rebuilt = Net::new(&clock, false);
    for id in 1..=3 {
        rebuilt.contact(id).rebuild_every_offer();
    }
    let lost = |round: u64, from: NodeId, to: NodeId| {
        (from == 2 && to == 1 && (8..11).contains(&round))
            || (from == 1 && to == 3 && (round % 7 == 3 || (20..35).contains(&round)))
            || (from == 3 && to == 1 && (40..43).contains(&round))
    };
    let pages = |r: &Report| {
        let relayed = r
            .heard
            .iter()
            .map(|h| h.own.as_ref().map(|o| o.pages.clone()));
        (r.own.pages.clone(), relayed.collect::<Vec<_>>())
    };
    for round in 0..60u64 {
        for net in [&cached, &rebuilt] {
            match round {
                r if r % 9 == 0 => net.nodes[&1].1.page(r / 9),
                r if r % 11 == 5 => net.nodes[&3].1.page(100 + r / 11),
                // Page 3, new at round 27, which 3 cannot confirm yet.
                28 => net.nodes[&1].1.drop_page(3),
                _ => {}
            }
        }
        let a = cached.period(|f, t| lost(round, f, t));
        let b = rebuilt.period(|f, t| lost(round, f, t));
        Net::advance(&clock).await;
        for ((fa, ta, ra), (fb, tb, rb)) in a.iter().zip(&b) {
            assert_eq!((fa, ta), (fb, tb));
            assert_eq!(pages(ra), pages(rb), "round {round}: {fa} to {ta}");
        }
    }
    assert!(cached.contact(1).offers_built() < rebuilt.contact(1).offers_built());
}

/// What a sender holds per addressee is bounded by one report's
/// records, however many pages pass, and whether or not it hears it.
#[tokio::test(start_paused = true)]
async fn the_records_held_per_addressee_are_bounded() {
    let clock = Arc::new(Clock {
        now: AtomicU64::new(T0),
        step: AtomicU64::new(0),
    });
    let net = Net::new(&clock, false);
    // Its own records and those it relays of 3, two outputs each.
    let bound = 2 * 2 * PAGES_REPORTED;
    for round in 0..300u64 {
        net.nodes[&1].1.page(round);
        net.nodes[&3].1.page(round);
        net.period(|f, _| f == 2 && (100..200).contains(&round));
        Net::advance(&clock).await;
        let held = net.contact(1).carried_len(2);
        assert!(held <= bound, "round {round}: {held} records held");
    }
}
