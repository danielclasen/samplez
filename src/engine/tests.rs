use super::*;

/// A config of two mono inputs, one output, column 0 ducked.
fn ducked_config() -> Config {
    let v = serde_json::json!({
        "libmxl": "/nonexistent/libmxl.so",
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": (0..2).map(|n| serde_json::json!({
            "name": format!("in{n}"), "domain": "d",
            "flowId": "5fbec3b1-1b0b-417e-a9f4-a7b1f1fd2b8d", "channels": 1 }))
            .collect::<Vec<_>>(),
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "7fbec3b1-1b0f-417d-9059-8b94a47197ed", "label": "o",
                      "group": "g", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0], [1.0]],
        "duckColumns": [0],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

/// Every duck setting's neutral form is one an engine takes, so a
/// refused setting is replaced rather than left out; a refused
/// finite one clamped is taken too, and one with a NaN is not.
#[test]
fn a_duck_settings_neutral_and_clamped_forms_are_taken() {
    let config = ducked_config();
    let mut refused = samplez_dsp::DetectorParams::DEFAULT;
    refused.attack_ms = 1e9;
    let settings = [
        DuckSetting::Detector(Box::new(refused)),
        DuckSetting::Keys(vec![(0, 1e9), (1, 1.0)]),
        DuckSetting::Depth { slot: 1, db: 99.0 },
    ];
    for s in &settings {
        assert!(check_duck(&config, 0, s).is_err(), "{s:?} refused");
        let clamped = s.clamped().expect("finite");
        assert_eq!(check_duck(&config, 0, &clamped), Ok(()), "{clamped:?}");
        assert_eq!(check_duck(&config, 0, &s.neutral()), Ok(()), "{s:?}");
    }
    assert_eq!(
        DuckSetting::Depth { slot: 1, db: 99.0 }.clamped(),
        Some(DuckSetting::Depth { slot: 1, db: 40.0 })
    );
    let nan = DuckSetting::Depth {
        slot: 1,
        db: f32::NAN,
    };
    assert_eq!(nan.clamped(), None);
    // A key of a slot the engine lacks stays refused clamped; its
    // neutral form, no key, is taken.
    let missing = DuckSetting::Keys(vec![(7, 1.0)]);
    assert!(check_duck(&config, 0, &missing.clamped().unwrap()).is_err());
    assert_eq!(check_duck(&config, 0, &missing.neutral()), Ok(()));
}

/// The percentile follows the build times: every build below 10 000
/// counts, one slow build among 20 000 does not, and buildLead covers
/// it, a supervisor tick and the lead in whole blocks.
/// The history after the core of block k0 ends at the start of block
/// k0 + 1 and is first overwritten by block k0 + 1 + slack: with the
/// block in progress and one guard block, a copy is kept while the RT
/// thread started at most block k0 + slack - 1, refused from k0 + slack.
#[test]
fn the_history_copy_ends_at_the_block_after_k0() {
    let started = Started::new();
    let (k0, slack) = (1000, 50);
    started.begin(k0 + slack - 1);
    assert_eq!(history_kept(&started, k0, slack), Ok(()));
    started.begin(k0 + slack);
    assert_eq!(history_kept(&started, k0, slack), Err(k0 + slack));
}

/// The changes of a [`ClockGap`] observing, every 100 ms of a 48 kHz
/// clock for `secs` seconds from `at`, a head `lag(at)` samples below
/// the clock index, for a flow of `history` samples, a slack of 960
/// and a frozen-head limit of 2400. Returns them with their clock
/// index, and the index after the last observation.
fn observe(
    gap: &mut ClockGap,
    at: u64,
    secs: u64,
    history: u64,
    lag: impl Fn(u64) -> i64,
) -> (Vec<(u64, GapChange)>, u64) {
    let step = 4_800;
    let mut changes = Vec::new();
    let mut at = at;
    for _ in 0..secs * 10 {
        let head = at.checked_add_signed(-lag(at)).unwrap();
        if let Some(c) = gap.observe(head, at, history, 960, 2_400) {
            changes.push((at, c));
        }
        at += step;
    }
    (changes, at)
}

/// A head 37 s behind the clock, or 5 s ahead, with a moving head, is
/// reported off once it stayed off for the flow's history, which is
/// 1 s for one flow and 3 s for another, and once only; the gap is
/// the clock index minus the head.
#[test]
fn a_head_far_off_the_clock_is_reported_after_the_flows_history() {
    const T0: u64 = 1 << 40;
    for history in [48_000, 144_000] {
        for lag in [37 * 48_000, -5 * 48_000] {
            let mut gap = ClockGap::default();
            let (changes, _) = observe(&mut gap, T0, 10, history, |_| lag);
            assert_eq!(
                changes,
                [(T0 + history, GapChange::Off(i128::from(lag)))],
                "history {history}, lag {lag}"
            );
        }
    }
    assert_eq!(
        off_clock(37 * 48_000 + 4_800, 48_000),
        "head 37.1 s behind the MXL clock, beyond the flow's history"
    );
    assert_eq!(
        off_clock(-5 * 48_000, 48_000),
        "head 5.0 s ahead of the MXL clock, beyond the flow's history"
    );
}

/// The rule follows the flow's history: a head 2 s behind is off for
/// a flow with 1 s of history and a late one for a flow with 3 s.
/// Neither a head a margin or a few hundred milliseconds behind nor a
/// head off for one observation is reported.
#[test]
fn a_head_within_the_flows_history_is_not_reported() {
    const T0: u64 = 1 << 40;
    let off = |history, lag| {
        let mut gap = ClockGap::default();
        !observe(&mut gap, T0, 10, history, |_| lag).0.is_empty()
    };
    assert!(off(48_000, 2 * 48_000));
    assert!(!off(144_000, 2 * 48_000));
    for lag in [960, 48 * 4, 14_400, 48_000 + 960, -48_000] {
        assert!(!off(48_000, lag), "lag {lag}");
    }
    // Off for one observation, a run shorter than the history.
    let mut gap = ClockGap::default();
    let spike = |at| if at == T0 + 9_600 { 10 * 48_000 } else { 960 };
    assert!(observe(&mut gap, T0, 10, 48_000, spike).0.is_empty());
}

/// A stopped head, which drifts behind the clock past any history, is
/// the frozen-head prune's: it is not reported off, also when it
/// stopped after a move, and starting to move again far off starts a
/// run of its own.
#[test]
fn a_frozen_head_is_not_reported_off_the_clock() {
    const T0: u64 = 1 << 40;
    let mut gap = ClockGap::default();
    // On the clock, then stopped at T0 + 1 s.
    let stop = T0 + 48_000;
    let stopped = |at: u64| at.saturating_sub(stop) as i64;
    assert!(observe(&mut gap, T0, 20, 48_000, stopped).0.is_empty());
    // Far behind and frozen from the first observation.
    let mut gap = ClockGap::default();
    let frozen = |at: u64| (at - 37 * 48_000 + 1) as i64;
    assert!(observe(&mut gap, T0, 20, 48_000, frozen).0.is_empty());
    // Moving again, 37 s behind.
    let (changes, _) = observe(&mut gap, T0 + 20 * 48_000, 5, 48_000, |_| 37 * 48_000);
    assert_eq!(changes, [(T0 + 21 * 48_000, GapChange::Off(37 * 48_000))]);
}

/// A head reported off whose producer then stops is reported back
/// once the head has stood still past the frozen-head limit: the
/// input reads as frozen, not as off the clock.
#[test]
fn a_head_off_the_clock_that_stops_is_reported_back() {
    const T0: u64 = 1 << 40;
    let mut gap = ClockGap::default();
    let (changes, at) = observe(&mut gap, T0, 3, 48_000, |_| 37 * 48_000);
    assert_eq!(changes.len(), 1, "{changes:?}");
    // Stopped at `at`, still 37 s behind and more.
    let head = at - 37 * 48_000;
    let (changes, _) = observe(&mut gap, at, 2, 48_000, |t| (t - head) as i64);
    let back: Vec<GapChange> = changes.iter().map(|c| c.1).collect();
    assert_eq!(back.len(), 1, "{changes:?}");
    assert!(matches!(back[0], GapChange::Back(_)), "{changes:?}");
    assert_eq!(gap.off, None);
}

/// A head reported off is reported back at the first observation
/// within reach, once.
#[test]
fn a_head_back_on_the_clock_is_reported_back_once() {
    const T0: u64 = 1 << 40;
    let mut gap = ClockGap::default();
    let (changes, at) = observe(&mut gap, T0, 3, 48_000, |_| 37 * 48_000);
    assert_eq!(changes.len(), 1);
    let (changes, _) = observe(&mut gap, at, 5, 48_000, |_| 960);
    assert_eq!(changes, [(at, GapChange::Back(37 * 48_000))]);
}

/// Detaching a pruned slot leaves no outgoing flow: its flow is not
/// readable, so a reader attached for the crossfade would only fail.
#[test]
fn detaching_a_pruned_slot_keeps_no_outgoing_flow() {
    let pruned = SupState::Pruned {
        recovery: Recovery::default(),
        held: false,
    };
    assert_eq!(detach_routing(&pruned, 9, "f"), None);
    for state in [SupState::Attached, SupState::Wanted] {
        assert_eq!(detach_routing(&state, 9, "f"), Some((9, "f".to_owned())));
    }
}

/// A slot waiting for its generation, as after an `InputUp` or a
/// `Route` while the build lags, has no say in input liveness: its
/// flow is neither read nor known lost yet. Nor has a detached one.
#[test]
fn a_slot_waiting_for_its_generation_casts_no_liveness_vote() {
    let pruned = SupState::Pruned {
        recovery: Recovery::default(),
        held: false,
    };
    assert_eq!(liveness_vote(&SupState::Wanted, true), None);
    assert_eq!(liveness_vote(&SupState::Detached, true), None);
    assert_eq!(liveness_vote(&SupState::Attached, false), Some(true));
    assert_eq!(liveness_vote(&pruned, false), Some(false));
    assert_eq!(liveness_vote(&pruned, true), Some(true));
}

/// A 10 s window of stats, each sample 1 s of 48 kHz with a head
/// advancing at the nominal rate, late blocks with `slack` in every
/// sample after the first.
fn margin_window(slack: impl Fn(usize) -> (Option<u64>, Option<i64>)) -> VecDeque<SlotStats> {
    (0..MARGIN_WINDOW)
        .map(|n| {
            let (head, min_slack) = slack(n);
            SlotStats {
                late: u32::from(n > 0),
                on_time: 100,
                min_slack,
                head,
                now: n as u64,
                ..crate::rt_loop::EMPTY_STATS
            }
        })
        .collect()
}

/// A head is in reach when it holds the newest block due, whose end
/// lies the margin and up to a block short of it behind the clock: a
/// positive offset reads later samples and needs a head that far
/// further on, a negative one allows it that much less.
#[test]
fn a_head_is_in_reach_once_it_holds_the_newest_block_due() {
    let (block, margin) = (48, 960);
    // 20 samples past the deadline of the block ending at `due`.
    let due = 1_795_061_948 * block;
    let at = due + margin + 20;
    assert!(within_reach(due, at, block, margin, 0));
    assert!(!within_reach(due - 1, at, block, margin, 0));
    // At its deadline the block is due; one sample before, it is not.
    assert!(within_reach(due, due + margin, block, margin, 0));
    assert!(within_reach(
        due - block,
        due + margin - 1,
        block,
        margin,
        0
    ));
    assert!(!within_reach(
        due - block - 1,
        due + margin - 1,
        block,
        margin,
        0
    ));
    assert!(within_reach(due + 100, at, block, margin, 100));
    assert!(!within_reach(due + 99, at, block, margin, 100));
    assert!(within_reach(due - 100, at, block, margin, -100));
    assert!(!within_reach(due - 101, at, block, margin, -100));
    // A head 36.96 s behind, stamped on UTC rather than TAI.
    assert!(!within_reach(at - 1_774_242, at, block, margin, 0));
}

/// The margin warning names what the late blocks need, never an
/// absolute index: a sample whose head was 0 or that fell behind the
/// window's first head is left out, and a requirement beyond the
/// samples the window spans is none.
#[test]
fn a_margin_warning_never_names_an_absolute_index() {
    let timing = Timing {
        block: 48,
        margin: 576,
        max_catchup_blocks: 4,
        resync_budget: 0,
    };
    let head0: u64 = 86_160_902_160_000;
    let head = |n: usize| head0 + n as u64 * 48_000;
    let nominal = 10 * 48_000;
    let w = margin_window(|n| (Some(head(n)), Some(-100)));
    assert_eq!(margin_needed(&w, nominal, &timing), Some(576 + 100 + 48));
    // One sample read its late blocks against a head of 0: its slack
    // is the negated absolute index.
    let w = margin_window(|n| match n {
        5 => (Some(head(n)), None),
        _ => (Some(head(n)), Some(-100)),
    });
    assert_eq!(margin_needed(&w, nominal, &timing), Some(724));
    let w = margin_window(|n| match n {
        5 => (Some(0), Some(-(head0 as i64))),
        _ => (Some(head(n)), Some(-100)),
    });
    assert_eq!(
        margin_needed(&w, nominal, &timing),
        Some(724),
        "a head of 0 left out"
    );
    let w = margin_window(|n| match n {
        5 => (Some(head0 - 1), Some(-(head0 as i64))),
        _ => (Some(head(n)), Some(-100)),
    });
    assert_eq!(
        margin_needed(&w, nominal, &timing),
        Some(724),
        "behind the start"
    );
    let w = margin_window(|n| (Some(head(n)), Some(-(head0 as i64))));
    assert_eq!(
        margin_needed(&w, nominal, &timing),
        None,
        "beyond the window"
    );
    // The first sample has no head yet: the window starts at the next.
    let w = margin_window(|n| match n {
        0 => (None, None),
        _ => (Some(head(n)), Some(-100)),
    });
    let nine = 9 * 48_000;
    assert_eq!(margin_needed(&w, nine, &timing), Some(724), "no first head");
}

#[test]
fn a_deadline_leaves_a_tick_and_the_hand_over_margin() {
    // 5 ms and 2 ms at 48 kHz.
    assert_eq!(samples_of(SUPERVISOR_TICK), 240);
    assert_eq!(samples_of(HAND_OVER_MARGIN), 96);
    let at = 48_000;
    assert!(!due(at - 337, at));
    assert!(due(at - 336, at));
    // Checks are a tick apart at most: a deadline first seen on one,
    // not seen a tick before, leaves more than the margin.
    for first in at - 336..at - 96 {
        assert!(due(first, at) && !due(first - 240, at));
        assert!(first + samples_of(HAND_OVER_MARGIN) < at, "{first}");
    }
}

/// The window in which an entry holds back the calls that can stall
/// the supervisor reaches a poll period and a tick past its deadline:
/// an entry not due yet but due within them counts, one beyond them
/// does not.
#[test]
fn an_entry_due_within_a_poll_period_and_a_tick_is_due_soon() {
    // 100 ms and 5 ms at 48 kHz, past the deadline's 336 samples.
    let at = 48_000;
    let edge = at - 336 - 5_040;
    assert!(!due(at - 337, at) && due_soon(at - 337, at));
    assert!(due_soon(edge, at));
    assert!(!due_soon(edge - 1, at));
}

/// A hand-over with less than the 2 ms margin left before its index
/// is past the margin, one with the margin left is not.
#[test]
fn a_hand_over_with_less_than_the_margin_left_is_past_it() {
    let at = 48_000;
    assert!(!past_margin(at - 96, at));
    assert!(past_margin(at - 95, at));
    assert!(past_margin(at + 48, at));
}

#[test]
fn builds_that_open_nothing_do_not_shrink_build_lead() {
    let us = Duration::from_micros(1);
    assert_eq!(build_sample(&[], us), None);
    assert_eq!(
        build_sample(&[Duration::from_secs(3)], us),
        Some(UNMEASURED_BUILD)
    );
    assert_eq!(
        build_sample(&[Duration::from_millis(2), us], us),
        Some(Duration::from_millis(2) + us)
    );
    let mut t = BuildTimes::new();
    let one = build_sample(&[Duration::from_millis(2)], us).unwrap();
    t.add(one.as_nanos() as u64, None);
    // A window of re-routes that share every reader.
    for _ in 0..2 * BUILD_WINDOW {
        if let Some(s) = build_sample(&[], us) {
            t.add(s.as_nanos() as u64, None);
        }
    }
    assert!(t.p9999().unwrap() >= 2_000_000);
}

#[test]
fn a_slow_build_ages_out_after_five_minutes_without_builds() {
    let at = Instant::now();
    let mut t = BuildTimes::new();
    for _ in 0..10 {
        t.add(1_000_000, None);
    }
    let fast = t.p9999();
    // A late reader's open, counted once it opened, raises it ...
    t.add(80_000_000, Some(at));
    t.expire(at + Duration::from_mins(5));
    assert!(t.p9999().unwrap() >= 80_000_000);
    // ... until it is five minutes old, with no build since; the
    // complete builds' times stay.
    t.expire(at + Duration::from_secs(301));
    assert_eq!(t.p9999(), fast);
}

#[test]
fn build_lead_covers_the_p9999_build_time() {
    let at = Instant::now();
    let mut t = BuildTimes::new();
    assert_eq!(t.p9999(), None);
    for _ in 0..BUILD_WINDOW {
        t.add(1_000_000, None);
    }
    let p = t.p9999().unwrap();
    assert!((1_000_000..1_190_000).contains(&p), "{p}");
    // One slow build raises it at once, as a late reader's open does.
    t.add(80_000_000, Some(at));
    assert!(t.p9999().unwrap() >= 80_000_000);
    let slow = build_lead_samples(t.p9999(), 192, 48);
    // It ages out after a window of fast builds, and only then.
    for _ in 0..BUILD_WINDOW - 1 {
        t.add(1_000_000, None);
    }
    assert!(t.p9999().unwrap() >= 80_000_000, "aged out too early");
    t.add(1_000_000, None);
    assert_eq!(t.p9999(), Some(p));
    assert!(build_lead_samples(t.p9999(), 192, 48) < slow);
    // 1 ms is below the 30 ms floor: 30 + 5 + 2 ms is 1776 samples,
    // plus a lead of 192: 1968.
    assert_eq!(build_lead_samples(Some(1_000_000), 192, 48), 1968);
    // Above the floor: 40 + 5 + 2 ms is 2256 samples, plus 192: 2448.
    assert_eq!(build_lead_samples(Some(40_000_000), 192, 48), 2448);
    assert_eq!(build_lead_samples(None, 0, 48), 5136);
    // Without the lead, not rounded: the floor + 5 ms + 2 ms, 100 ms +
    // 5 ms + 2 ms.
    assert_eq!(build_time_samples(Some(1_000_000)), 1776);
    assert_eq!(build_time_samples(None), 5136);
    // However short the builds, buildLead holds the lead, the floor,
    // the tick and the margin.
    let floor =
        192 + samples_of(BUILD_FLOOR) + samples_of(SUPERVISOR_TICK) + samples_of(HAND_OVER_MARGIN);
    for p in [0, 1, 1_000, 100_000] {
        assert!(build_lead_samples(Some(p), 192, 48) >= floor, "{p}");
    }
}

fn log() -> (Log, rtrb::Consumer<RingEntry>, mpsc::Receiver<LogMsg>) {
    let (ring, rx) = rtrb::RingBuffer::new(PENDING_CHANGES);
    let (sup, msgs) = mpsc::channel();
    let log = Log {
        ring,
        sup,
        uncommitted: VecDeque::new(),
        last: 0,
        committed: 0,
        submitted: 0,
        local: None,
    };
    (log, rx, msgs)
}

fn e(id: LogId, change: Change) -> RtEntry {
    RtEntry {
        id,
        at_index: 48 * id,
        change,
    }
}

/// Entries reach the RT thread only once committed, in log order;
/// a truncation removes only uncommitted ones, and the outstanding
/// count stays within what the RT thread holds.
#[test]
fn the_log_hands_over_committed_entries_only() {
    let (mut log, mut rx, msgs) = log();
    let down = Change::InputDown { slot: 0 };
    let up = Change::InputUp { slot: 0 };
    log.append(e(1, down), None, Vec::new(), 0).unwrap();
    log.append(e(2, up), None, Vec::new(), 0).unwrap();
    assert!(matches!(msgs.try_recv(), Ok(LogMsg::Append { id: 2, .. })));
    assert_eq!(
        log.append(e(2, down), None, Vec::new(), 0),
        Err(EntryError::Order { id: 2, last: 2 })
    );
    assert!(rx.pop().is_err());
    log.commit(1);
    assert_eq!(rx.pop().unwrap().entry.id, 1);
    // A truncation at or below the commit point keeps what is
    // committed, and the generations of its entries.
    log.truncate(1);
    assert!(matches!(msgs.try_recv(), Ok(LogMsg::Truncate { from: 2 })));
    assert_eq!((log.last, log.uncommitted.len()), (1, 0));
    log.append(e(2, down), None, Vec::new(), 0).unwrap();
    log.commit(2);
    assert_eq!(rx.pop().unwrap().entry.id, 2);
    // The engine's own log refuses what the RT thread has no room for.
    log.local = Some(LocalLog {
        up: Vec::new(),
        last_at: Vec::new(),
        gen_at: 0,
    });
    for id in 3..=PENDING_CHANGES as u64 {
        log.append(e(id, down), None, Vec::new(), 0).unwrap();
    }
    assert_eq!(
        log.append(e(PENDING_CHANGES as u64 + 1, down), None, Vec::new(), 0),
        Err(EntryError::Full)
    );
    log.append(e(PENDING_CHANGES as u64 + 1, down), None, Vec::new(), 1)
        .unwrap();
}

/// A replicated log refuses no entry for room: a replica whose RT
/// thread lags keeps the committed entries its ring has no room for
/// and passes them on as the ring drains, rather than refuse one,
/// which would stop it taking writes while the others apply it.
#[test]
fn a_replicated_log_keeps_what_its_ring_has_no_room_for() {
    let (mut log, mut rx, _msgs) = log();
    let down = Change::InputDown { slot: 0 };
    let n = PENDING_CHANGES as u64 + 36;
    for id in 1..=n {
        log.append(e(id, down), None, Vec::new(), 0).unwrap();
    }
    log.commit(n);
    assert_eq!(rx.slots(), PENDING_CHANGES);
    assert!(!log.skip(n), "a committed entry skipped");
    for id in 1..=10 {
        assert_eq!(rx.pop().unwrap().entry.id, id);
    }
    log.flush();
    assert_eq!(rx.slots(), PENDING_CHANGES);
    let ids: Vec<LogId> = std::iter::from_fn(|| rx.pop().ok().map(|q| q.entry.id)).collect();
    assert_eq!(ids, (11..=PENDING_CHANGES as u64 + 10).collect::<Vec<_>>());
    log.flush();
    assert_eq!(rx.slots(), 26);
}

/// A product state's strip parameters reach the RT thread folded to
/// the last value per slot and key: a list of every value a parameter
/// took would otherwise be set one by one in one wake.
#[test]
fn product_strip_parameters_fold_to_the_last_value_per_key() {
    let trim = |db| StripParam::Trim { db };
    let leg = StripParam::LegTrim { leg: 1, db: 2.0 };
    let mut history: Vec<StripParam> = (0..1000).map(|n| trim(-(n % 20) as f32)).collect();
    history.push(leg);
    history.push(trim(-9.0));
    assert_eq!(
        product_strips(&[history, vec![trim(-1.0), trim(-2.0)]]),
        [(0, trim(-9.0)), (0, leg), (1, trim(-2.0))]
    );
}

/// Held by each test that starts engines: the test hooks are process
/// wide, and any engine's RT thread or supervisor takes them.
static ENGINES: Mutex<()> = Mutex::new(());

/// A panic on the RT thread hands the state back: the supervisor
/// releases the writer, which deletes the output flow, and records
/// the panic.
#[test]
#[cfg_attr(
    samplez_rtsan,
    ignore = "the panic allocates in the RT loop, which RealtimeSanitizer aborts"
)]
fn a_panic_on_the_rt_thread_releases_its_state_on_the_supervisor() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// An overrun skip withholds the skipped blocks at the skip, in one
/// ranged change for every output, and withdraws the replica: no block
/// after it commits at once on any output.
#[test]
fn an_overrun_skip_withholds_the_skipped_blocks_and_withdraws() {
    use crate::blocks::{Kind, PAGE};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-skip-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let output = |id: &str| {
        serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": "skip",
                                "group": "skip", "role": "Audio", "channels": 1 })
    };
    let v = serde_json::json!({
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": domain }],
        "outputs": [output("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d11"),
                    output("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d12")],
        "matrix": [],
        // A stall of a loaded host before the first commits is caught
        // up, not skipped: a skip then would withdraw the replica, and it
        // would commit only on evidence this test gives none of.
        "maxCatchupBlocks": 200,
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let process = rt::setup_process(&config.rt);
    let (report_tx, _reports) = mpsc::sync_channel(1);
    let (status_tx, changes) = mpsc::sync_channel(1 << 20);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: Some(status_tx),
        hashes: None,
    };
    let e = Engine::start_replicated(config, process, None, replication).unwrap();
    let processed = || e.counters().processed.load(Acquire);
    wait(&|| e.status().outputs.iter().all(|o| o.realised));
    e.set_fence(u64::MAX).unwrap();
    let from = processed() + 2;
    for o in 0..2 {
        e.commit_from(o, from).unwrap();
    }
    let kp = (from * 48).next_multiple_of(PAGE) / 48;
    let committed = |o: usize| {
        (kp..processed()).any(|k| {
            e.block_status(o, k)
                .is_some_and(|s| s.kind == Kind::Committed)
        })
    };
    wait(&|| committed(0) && committed(1));

    // Longer than the catch-up: the loop skips.
    crate::rt_loop::STALL_MS.store(400, Relaxed);
    // The stall's skip, past any overrun skip under load.
    let t = Instant::now();
    let (skip_from, skip_to) = loop {
        assert!(t.elapsed() < Duration::from_secs(5), "no skip");
        match changes.recv_timeout(Duration::from_millis(100)) {
            Ok(StatusChange::Skipped { from, to }) if to - from >= 20 => break (from, to),
            _ => {}
        }
    };
    assert!(skip_to - skip_from >= 20, "{skip_from}..{skip_to}");
    assert!(e.counters().blocks_withheld.load(Relaxed) >= skip_to - skip_from);
    wait(&|| processed() > skip_to + 40);
    for o in 0..2 {
        // A block without a status was skipped by an overrun under
        // load as well.
        for k in skip_to..skip_to + 40 {
            let Some(st) = e.block_status(o, k) else {
                continue;
            };
            assert_ne!(st.kind, Kind::Committed, "output {o}: {st:?}");
            assert!(
                st.kind == Kind::Withheld || st.digest.is_none(),
                "output {o}: {st:?} is neither withheld nor a fill"
            );
        }
    }
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// A block withheld in the ring withdraws the replica before the next
/// block takes its status: after a stall, with F past the ring and
/// its newest block withheld, the blocks before it commit, and the
/// first block computed after it does not commit at once, nor does
/// any later one without evidence.
#[test]
fn a_block_after_a_withheld_one_does_not_commit_at_once() {
    use crate::blocks::{Cause, Kind, PAGE};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A run an overrun skip or an overflow disturbed is run again.
    let run = || -> Option<String> {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-order-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d31";
        // A catch-up long enough that the stall below skips nothing.
        let v = serde_json::json!({
            "libmxl": libmxl,
            "maxCatchupBlocks": 200,
            "holdMs": 100,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [{ "name": "out", "domain": "d", "flowId": id, "label": "order",
                          "group": "order", "role": "Audio", "channels": 1 }],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(1);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: None,
            hashes: None,
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().outputs[0].realised);
        e.set_fence(u64::MAX).unwrap();
        let from = processed() + 2;
        e.commit_from(0, from).unwrap();
        let kp = (from * 48).next_multiple_of(PAGE) / 48;
        wait(&|| processed() > kp + 5);
        // F frozen: blocks wait fenced in the ring.
        let f = processed() + 1;
        e.set_fence(f * 48).unwrap();
        wait(&|| processed() > f + 10);
        crate::rt_loop::STALL_MS.store(60, Relaxed);
        std::thread::sleep(Duration::from_millis(10));
        // During the stall: the newest block in the ring withheld, F past
        // all.
        let stalled = processed();
        e.withhold(0, stalled - 1, Cause::HoldCap).unwrap();
        e.set_fence(u64::MAX).unwrap();
        assert_eq!(processed(), stalled, "the RT thread did not stall");
        wait(&|| processed() > stalled + 40);
        let c = e.counters();
        let (skipped, overflows) = (
            c.overrun_skipped_samples.load(Relaxed),
            c.hold_overflows.load(Relaxed),
        );
        if skipped > 0 || overflows > 0 {
            // A skip or an overflow under load withdrew the replica
            // before the stall ended: the run proves nothing.
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            return Some(format!("{skipped} samples skipped, {overflows} overflows"));
        }
        for k in f..stalled - 1 {
            assert_eq!(e.block_status(0, k).unwrap().kind, Kind::Committed);
        }
        for k in stalled..stalled + 40 {
            let st = e.block_status(0, k).unwrap();
            assert_ne!(st.kind, Kind::Committed, "{st:?}");
        }
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        None
    };
    let mut seen = Vec::new();
    for _ in 0..4 {
        match run() {
            None => return,
            Some(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

/// A replicated engine of two voters with `outputs` outputs and no
/// input, its status changes and verdicts, in a fresh domain.
fn outputs_only(
    name: &str,
    outputs: usize,
) -> (
    Engine,
    mpsc::Receiver<StatusChange>,
    mpsc::Receiver<BlockVerdict>,
    std::path::PathBuf,
) {
    outputs_of(name, outputs, 2)
}

/// [`outputs_only`] with `voters` voters.
fn outputs_of(
    name: &str,
    outputs: usize,
    voters: usize,
) -> (
    Engine,
    mpsc::Receiver<StatusChange>,
    mpsc::Receiver<BlockVerdict>,
    std::path::PathBuf,
) {
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let outs: Vec<_> = (0..outputs)
        .map(|o| {
            let id = format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d2{o}");
            serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": name,
                                    "group": name, "role": "Audio", "channels": 1 })
        })
        .collect();
    let v = serde_json::json!({
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": domain }],
        "outputs": outs,
        "matrix": [],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let process = rt::setup_process(&config.rt);
    let (report_tx, _reports) = mpsc::sync_channel(1);
    let (status_tx, changes) = mpsc::sync_channel(1 << 20);
    let (verdict_tx, verdicts) = mpsc::sync_channel(1 << 20);
    // Hashed, so the clock-step and skip paths run through the page
    // hashes too (also under RealtimeSanitizer); nobody reads them.
    let (hash_tx, _) = mpsc::sync_channel(1);
    let replication = Replication {
        voters,
        reports: report_tx,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let e = Engine::start_replicated(config, process, Some(verdict_tx), replication).unwrap();
    wait(&|| e.status().outputs.iter().all(|o| o.realised));
    (e, changes, verdicts, domain)
}

/// Commits every output from the next page boundary on, F past every
/// block; the block that boundary is in.
fn committing(e: &Engine, outputs: usize) -> u64 {
    e.set_fence(u64::MAX).unwrap();
    let from = e.counters().processed.load(Acquire) + 2;
    for o in 0..outputs {
        e.commit_from(o, from).unwrap();
    }
    let kp = (from * 48).next_multiple_of(crate::blocks::PAGE) / 48;
    wait(&|| e.counters().processed.load(Acquire) > kp + 5);
    kp
}

/// Takes the clock shift off again, also when a test fails.
struct Unshift;

impl Drop for Unshift {
    fn drop(&mut self) {
        crate::rt_loop::CLOCK_STEP_NS.store(0, Relaxed);
        crate::rt_loop::CLOCK_SHIFT_NS.store(0, Relaxed);
    }
}

/// A forward step of libmxl's clock withholds the blocks it skips at
/// the skip and fences the blocks after it: none commits until the
/// step is cleared and a commit start names the output again. The
/// TAI offset is sampled.
#[test]
fn a_forward_clock_step_withholds_the_skip_and_fences() {
    use crate::blocks::Kind;
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _unshift = Unshift;
    let (e, changes, _verdicts, domain) = outputs_only("forward", 1);
    let processed = || e.counters().processed.load(Acquire);
    committing(&e, 1);
    wait(&|| e.status().clock.tai_offset_s.is_some());
    assert!(!e.status().clock.step_fenced);

    let steps = || e.counters().clock_steps.load(Relaxed);
    let before = steps();
    crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
    wait(&|| steps() > before);
    // The step's skip: the last one of 50 blocks or more by now; an
    // overrun under load can skip as well.
    std::thread::sleep(Duration::from_millis(20));
    let mut seen: Vec<_> = changes.try_iter().collect();
    let (from, to) = seen
        .iter()
        .rev()
        .find_map(|c| match *c {
            StatusChange::Skipped { from, to } if to - from >= 50 => Some((from, to)),
            _ => None,
        })
        .expect("no skip");
    wait(&|| processed() > to + 100);
    // From the skip on nothing commits while the step fences: blocks
    // are withheld or wait fenced.
    seen.extend(changes.try_iter());
    let after: Vec<_> = seen
        .into_iter()
        .filter_map(|c| match c {
            StatusChange::Block { status, .. } if status.k >= from => Some(status),
            _ => None,
        })
        .collect();
    assert!(after.iter().all(|s| s.kind != Kind::Committed), "{after:?}");
    assert!(after.iter().any(|s| s.kind == Kind::Fenced), "{after:?}");
    wait(&|| e.status().clock.step_fenced);

    e.clear_step_fence(1).unwrap();
    wait(&|| !e.status().clock.step_fenced);
    // A commit start names the output again; the fills the withdrawn
    // replica held meanwhile commit on evidence, then the blocks
    // after the start commit.
    let start = processed() + 2;
    e.commit_from(0, start).unwrap();
    let kp = (start * 48).next_multiple_of(crate::blocks::PAGE) / 48;
    let t = Instant::now();
    while e.block_status(0, kp + 3).map(|s| s.kind) != Some(Kind::Committed) {
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "{:?}",
            e.block_status(0, kp + 3)
        );
        for k in to..processed() {
            let _ = e.commit_through(0, k, 0, None);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// With 16-sample blocks and a short hold a page is 64 blocks, as
/// many as four times the hold ring: a block a page after the
/// first block of a page, withheld ahead by the supervisor, must not
/// take that block's status before the page settles, which reads it.
#[test]
fn a_status_set_ahead_keeps_a_page_committed_at_blocks_of_16() {
    use crate::blocks::PAGE;
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let run = || -> Option<String> {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-b16-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d61";
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": 16,
            // A stall under load is caught up rather than skipped.
            "maxCatchupBlocks": 300,
            "holdMs": 2,
            "contactWindowMs": 2,
            "domains": [{ "name": "d", "path": domain }],
            "outputs": [{ "name": id, "domain": "d", "flowId": id, "label": "b16",
                          "group": "b16", "role": "Audio", "channels": 1 }],
            "matrix": [],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        assert!(4 * config.hold_blocks() <= 64, "{}", config.hold_blocks());
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(64);
        let (hash_tx, hashes) = mpsc::sync_channel(1 << 16);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: None,
            hashes: Some(hash_tx),
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().outputs[0].realised);
        e.set_fence(u64::MAX).unwrap();
        let c = e.counters();
        // A skip or an overflow from the commit start on withdraws
        // the replica.
        let base = (
            c.overrun_skipped_samples.load(Relaxed),
            c.hold_overflows.load(Relaxed),
        );
        e.commit_from(0, processed() + 2).unwrap();
        let per = PAGE / 16;
        let k0 = (processed() + 4) / per * per + per;
        wait(&|| processed() > k0);
        let disturbed = if processed() > k0 + per / 2 {
            Some(format!("block {k0} computed late"))
        } else {
            assert!(
                e.withhold(0, k0 + per, Cause::FenceDeadline)
                    .unwrap()
                    .is_some()
            );
            let page = k0 * 16 / PAGE;
            let t = Instant::now();
            let record = loop {
                assert!(
                    t.elapsed() < Duration::from_secs(5),
                    "page {page} not settled"
                );
                match hashes.recv_timeout(Duration::from_millis(100)) {
                    Ok(h) if h.page >= page => break h,
                    _ => {}
                }
            };
            let skipped = c.overrun_skipped_samples.load(Relaxed) - base.0;
            let overflows = c.hold_overflows.load(Relaxed) - base.1;
            if skipped > 0 || overflows > 0 {
                Some(format!("{skipped} samples skipped, {overflows} overflows"))
            } else {
                assert_eq!(record.page, page, "{record:?}");
                assert!(record.committed, "{record:?}");
                None
            }
        };
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        disturbed
    };
    let mut seen = Vec::new();
    for _ in 0..4 {
        match run() {
            None => return,
            Some(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

/// An `InputDown`, `InputUp` or `Route` entry that reaches the RT
/// thread after its index, while it still computes the page holding
/// that index, is reported late, not applied, and left out of the
/// page's fingerprint.
#[test]
fn a_late_input_entry_is_left_out_of_the_fingerprint() {
    use crate::blocks::{HEALTHY, PAGE};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A run an overrun skip or a slow pickup disturbed is run again.
    let run = || -> Option<String> {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let domain = std::env::temp_dir().join(format!("samplez-latefp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let (src, next, out) = (
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d51",
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d52",
            "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d53",
        );
        let v = serde_json::json!({
            "libmxl": libmxl,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d", "flowId": src, "channels": 1 }],
            "outputs": [{ "name": out, "domain": "d", "flowId": out, "label": "latefp",
                          "group": "latefp", "role": "Audio", "channels": 1 }],
            "matrix": [[1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, _reports) = mpsc::sync_channel(64);
        let (hash_tx, hashes) = mpsc::sync_channel(1 << 16);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: None,
            hashes: Some(hash_tx),
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        let processed = || e.counters().processed.load(Acquire);
        wait(&|| e.status().outputs[0].realised);
        // Hashed from the writer's first block on: the page it falls
        // in is not hashed whole.
        let begun = processed() * 48 / PAGE;
        // A few blocks into a later page, with room left in it.
        let t = Instant::now();
        let p0 = loop {
            let p = processed();
            let off = p * 48 % PAGE;
            if p * 48 / PAGE > begun && off >= 4 * 48 && off + 10 * 48 < PAGE {
                break p;
            }
            assert!(t.elapsed() < Duration::from_secs(5), "no page entered");
            std::thread::sleep(Duration::from_micros(200));
        };
        let page = p0 * 48 / PAGE;
        let at = (page * PAGE).next_multiple_of(48);
        assert!(at < p0 * 48);
        e.append(1, at, Entry::InputDown { slot: 0 }).unwrap();
        e.append(2, at, Entry::InputUp { slot: 0 }).unwrap();
        let route = Entry::Route {
            slot: 0,
            flow_id: next.to_owned(),
        };
        e.append(3, at, route).unwrap();
        e.commit(3);
        wait(&|| e.counters().entries_consumed.load(Acquire) >= 3);
        let p1 = processed();
        let c = e.counters();
        let disturbed = if p1 * 48 / PAGE != page {
            Some(format!("entries taken in block {p1}, past page {page}"))
        } else {
            assert_eq!(c.command_late.load(Relaxed), 3);
            let t = Instant::now();
            let record = loop {
                assert!(
                    t.elapsed() < Duration::from_secs(5),
                    "page {page} not settled"
                );
                match hashes.recv_timeout(Duration::from_millis(100)) {
                    Ok(h) if h.page >= page => break h,
                    _ => {}
                }
            };
            if record.page != page {
                let skipped = c.overrun_skipped_samples.load(Relaxed);
                let dropped = c.page_hashes_dropped.load(Relaxed);
                assert!(
                    skipped > 0,
                    "no record for page {page} ({dropped} dropped, taken by block \
                         {p1}, from {p0}): {record:?}"
                );
                Some(format!("{skipped} samples skipped"))
            } else {
                assert_eq!(record.fingerprint, HEALTHY, "{record:?}");
                None
            }
        };
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        disturbed
    };
    let mut seen = Vec::new();
    for _ in 0..4 {
        match run() {
            None => return,
            Some(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

/// Page hashes through an overrun skip and clock steps either way:
/// no page missing a skipped block gets a record, every page gets at
/// most one, and pages before and after the skips get theirs. The
/// RealtimeSanitizer gate runs it as well.
#[test]
fn page_hashes_leave_out_the_pages_a_skip_cut() {
    use crate::blocks::PAGE;
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _unshift = Unshift;
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-hashes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let id = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d41";
    let v = serde_json::json!({
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": domain }],
        "outputs": [{ "name": id, "domain": "d", "flowId": id, "label": "hashes",
                      "group": "hashes", "role": "Audio", "channels": 1 }],
        "matrix": [],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let process = rt::setup_process(&config.rt);
    let (report_tx, _reports) = mpsc::sync_channel(1);
    let (status_tx, changes) = mpsc::sync_channel(1 << 20);
    let (hash_tx, hashes) = mpsc::sync_channel(1 << 20);
    let replication = Replication {
        voters: 2,
        reports: report_tx,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let e = Engine::start_replicated(config, process, None, replication).unwrap();
    let processed = || e.counters().processed.load(Acquire);
    wait(&|| e.status().outputs[0].realised);
    committing(&e, 1);
    let first = processed();
    // A page hashed whole from the commit start on, before the stall.
    let mut pages: Vec<u64> = Vec::new();
    let t = Instant::now();
    while pages.last().is_none_or(|&p| p * PAGE < first * 48) {
        assert!(t.elapsed() < Duration::from_secs(10), "no page hashed");
        pages.extend(
            hashes
                .recv_timeout(Duration::from_millis(100))
                .map(|h| h.page),
        );
    }
    let stalled = processed();

    crate::rt_loop::STALL_MS.store(40, Relaxed);
    let mut seen = Vec::new();
    let t = Instant::now();
    while !seen.iter().any(
        |c| matches!(c, StatusChange::Skipped { from, to } if to - from >= 20 && *from >= stalled),
    ) {
        assert!(t.elapsed() < Duration::from_secs(5), "no skip");
        seen.extend(changes.recv_timeout(Duration::from_millis(100)));
    }
    let steps = || e.counters().clock_steps.load(Relaxed);
    let before = steps();
    crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
    wait(&|| steps() > before);
    crate::rt_loop::CLOCK_STEP_NS.store(-60_000_000, Relaxed);
    wait(&|| steps() > before + 1);
    // A page hashed whole after the steps; an overrun skip under load
    // only delays it.
    let last = processed() * 48 / PAGE;
    let t = Instant::now();
    while pages.last().is_none_or(|&p| p <= last) {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "no page after the steps"
        );
        pages.extend(
            hashes
                .recv_timeout(Duration::from_millis(100))
                .map(|h| h.page),
        );
    }
    // Every skip before that page's record was sent first.
    seen.extend(changes.try_iter());

    let skips: Vec<(u64, u64)> = seen
        .iter()
        .filter_map(|c| match *c {
            StatusChange::Skipped { from, to } if to > from => Some((from * 48, to * 48)),
            _ => None,
        })
        .collect();
    assert!(skips.len() >= 2, "{skips:?}");
    for &p in &pages {
        let (start, end) = (p * PAGE, (p + 1) * PAGE);
        assert!(
            !skips.iter().any(|&(f, t)| f < end && start < t),
            "page {p} hashed over a skip: {skips:?}"
        );
    }
    assert!(pages.windows(2).all(|w| w[0] < w[1]), "{pages:?}");
    // A page hashed whole between the commit start and the stall.
    let stall = skips
        .iter()
        .find(|s| s.1 - s.0 >= 20 * 48 && s.0 >= stalled * 48)
        .unwrap();
    assert!(
        pages
            .iter()
            .any(|&p| p * PAGE >= first * 48 && (p + 1) * PAGE <= stall.0),
        "{pages:?}"
    );
    assert_eq!(e.counters().page_hashes_dropped.load(Relaxed), 0);
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// A clean self-report clears only the steps detected before its
/// reading began.
#[test]
fn a_step_during_the_reading_is_not_cleared_by_it() {
    let steps = AtomicU64::new(3);
    let (u, clear) = self_report(&steps, || {
        steps.fetch_add(1, Relaxed);
        Some(false)
    });
    assert_eq!((u, clear), (Some(false), Some(3)));
    assert_eq!(self_report(&steps, || Some(true)), (Some(true), None));
    assert_eq!(self_report(&steps, || None), (None, None));
}

/// One voter has no peer to clear a detected clock step: its next
/// clean self-report lifts the step's fence, and it commits again.
#[test]
fn a_lone_voter_lifts_its_step_fence_on_a_clean_self_report() {
    use crate::blocks::{Cause, Kind};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _unshift = Unshift;
    let (e, _changes, _verdicts, domain) = outputs_of("lone", 1, 1);
    let processed = || e.counters().processed.load(Acquire);
    wait(&|| e.status().clock.unsynchronised.is_some());
    assert_eq!(
        e.status().clock.unsynchronised,
        Some(false),
        "the host's kernel clock is unsynchronised"
    );
    wait(&|| processed() > 0);
    let before = processed();
    let fenced = || e.counters().step_fenced.load(Relaxed);
    crate::rt_loop::CLOCK_STEP_NS.store(60_000_000, Relaxed);
    wait(&|| fenced());
    let t = Instant::now();
    wait(&|| !fenced());
    // adjtimex is read once a second.
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    let k = processed() + 2;
    wait(&|| processed() > k);
    assert_eq!(e.block_status(0, k).map(|s| s.kind), Some(Kind::Committed));
    // One voter withholds the blocks the step fences.
    assert!(
        (before..k).any(|x| {
            e.block_status(0, x)
                .is_some_and(|s| (s.kind, s.cause) == (Kind::Withheld, Cause::ClockStep))
        }),
        "no block was fenced"
    );
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// A backward step of libmxl's clock withholds every block not yet
/// final in the hold ring, cause skip, and computes no block twice:
/// the loop wakes without computing until the clock is due at the
/// block after the last one again, and no output is written below
/// its last commit.
#[test]
fn a_backward_clock_step_withholds_the_ring_and_computes_nothing_twice() {
    use crate::blocks::{Cause, Kind};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _unshift = Unshift;
    let (e, _changes, verdicts, domain) = outputs_only("backward", 1);
    let c = e.counters();
    let processed = || c.processed.load(Acquire);
    committing(&e, 1);
    // F frozen: blocks wait fenced in the ring.
    let f = processed() + 2;
    e.set_fence(f * 48).unwrap();
    wait(&|| processed() > f + 10);
    let open = processed() - 1;
    assert_eq!(e.block_status(0, open).unwrap().kind, Kind::Fenced);

    crate::rt_loop::CLOCK_STEP_NS.store(-60_000_000, Relaxed);
    wait(&|| c.clock_behind_wakes.load(Relaxed) > 20);
    let st = e.block_status(0, open).unwrap();
    assert_eq!((st.kind, st.cause), (Kind::Withheld, Cause::Skip));
    // The loop wakes but computes nothing while behind.
    let (p0, h0) = (processed(), c.heartbeat_blocks.load(Relaxed));
    std::thread::sleep(Duration::from_millis(10));
    if c.clock_behind_wakes.load(Relaxed) < 50 {
        assert_eq!(processed(), p0);
    }
    assert!(c.heartbeat_blocks.load(Relaxed) > h0);
    wait(&|| processed() > p0 + 50);
    let ks: Vec<u64> = verdicts.try_iter().map(|v| v.k).collect();
    assert!(
        ks.windows(2).all(|w| w[1] > w[0]),
        "a block computed twice or out of order"
    );
    assert_eq!(c.output_held_blocks.load(Relaxed), 0);
    wait(&|| e.status().clock.step_fenced);
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// Pauses the loop of `e` once in the step of the first block from
/// `k` on: right after output 0 moved its ring with `in_ring`, else
/// before the outputs decide the block
/// ([`crate::rt_loop::HOLD_PASS_PAUSE`]). Armed before F is frozen,
/// the loop pauses itself, so no ring fills while the test thread
/// waits to be scheduled.
fn pause_at(e: &Engine, k: u64, in_ring: bool) {
    use crate::rt_loop::{HOLD_PASS_PAUSE, HOLD_PASS_PAUSE_IN_RING, HOLD_PASS_PAUSE_K};
    HOLD_PASS_PAUSE_K.store(k, Relaxed);
    HOLD_PASS_PAUSE_IN_RING.store(in_ring, Relaxed);
    HOLD_PASS_PAUSE.store(std::ptr::from_ref(e.counters()) as usize, Release);
}

/// Waits for the loop [`pause_at`] armed to pause.
fn paused() {
    let t = Instant::now();
    while !crate::rt_loop::HOLD_PASS_PAUSED.load(Acquire) {
        assert!(t.elapsed() < Duration::from_secs(10), "timed out");
        std::thread::yield_now();
    }
}

/// Resumes the loop [`pause_at`] paused.
fn resume() {
    crate::rt_loop::HOLD_PASS_PAUSED.store(false, Release);
}

/// Disarms and resumes the loop [`pause_at`] armed, also when a test
/// fails.
struct Unpause;

impl Drop for Unpause {
    fn drop(&mut self) {
        crate::rt_loop::HOLD_PASS_PAUSE.store(0, Relaxed);
        resume();
    }
}

/// A block withheld on one output withdraws the replica on every
/// output before the next one advances: output 1 does not commit
/// block k while output 0 withholds it, also when the supervisor
/// withholds k and moves F past it in the middle of a pass over the
/// hold rings, after output 0 moved its ring.
#[test]
fn a_block_withheld_on_one_output_withdraws_the_next_before_it_advances() {
    use crate::blocks::Kind;
    withhold_in_a_pass("withdraw-every", 0, true, |e, k| {
        let st = e.block_status(1, k).unwrap();
        assert_ne!(st.kind, Kind::Committed, "{st:?}");
    });
}

/// Both outputs hold block k fenced, F frozen, with the loop paused in
/// a step after it: right after output 0 moved its ring with
/// `in_ring`, else after the pass over the rings. The supervisor
/// withholds k on output `on` alone and moves F past it; `check` runs
/// on the status once the loop went on. An overrun skip or a full ring
/// under load withdraws the replica or leaves k without a fenced
/// status: then the outputs are named again on a new engine, since a
/// withdrawn output's ring of fills overflows before its commit
/// start.
fn withhold_in_a_pass(name: &str, on: usize, in_ring: bool, check: impl Fn(&Engine, u64)) {
    use crate::blocks::{Cause, Kind};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for _ in 0..10 {
        let (e, _changes, _verdicts, domain) = outputs_only(name, 2);
        let _unpause = Unpause;
        let processed = || e.counters().processed.load(Acquire);
        let fenced =
            |k: u64| (0..2).all(|o| e.block_status(o, k).is_some_and(|s| s.kind == Kind::Fenced));
        let committing_all =
            || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
        committing(&e, 2);
        let f = processed() + 2;
        pause_at(&e, f + 5, in_ring);
        e.set_fence(f * 48).unwrap();
        paused();
        let first = (f..=processed()).find(|&j| fenced(j));
        if !committing_all() || first != Some(f) {
            resume();
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            continue;
        }
        let st = e.withhold(on, f, Cause::Silence).unwrap().unwrap();
        assert_eq!(st.kind, Kind::Withheld);
        e.set_fence(u64::MAX).unwrap();
        resume();
        wait(&|| processed() > f + 15);
        check(&e, f);
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        return;
    }
    panic!("no pass without an overrun in 10 attempts");
}

/// A block withheld on a later output alone withdraws the replica on
/// every output before any ring moves at the F that passes it: output
/// 0 does not commit block k while output 1 withholds it, though
/// output 0's ring moves first in a pass.
#[test]
fn a_block_withheld_on_a_later_output_withdraws_the_first_before_it_advances() {
    use crate::blocks::Kind;
    withhold_in_a_pass("withdraw-later", 1, false, |e, k| {
        let st = e.block_status(0, k).unwrap();
        assert_ne!(st.kind, Kind::Committed, "{st:?}");
    });
}

/// A held block committed through on one output only up to P inside
/// it withholds the rest of it for a cause, which withdraws the
/// replica on every output before any moves past it: output 0, its
/// ring moved first in a pass, does not commit the block after it
/// alone. Block k is degraded by an unresolved entry, the blocks after
/// it healthy and held behind it; the supervisor marks k on output 0
/// whole and on output 1 up to the middle of k, between two passes.
#[test]
fn a_commit_through_up_to_p_on_a_later_output_withdraws_the_first() {
    use crate::blocks::{HEALTHY, Kind};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for _ in 0..10 {
        let (e, _changes, _verdicts, domain) = outputs_only("through-up-to", 2);
        let _unpause = Unpause;
        let processed = || e.counters().processed.load(Acquire);
        let held = |o: usize, k: u64, healthy: bool| {
            e.block_status(o, k)
                .is_some_and(|s| s.kind == Kind::Held && (s.digest == Some(HEALTHY)) == healthy)
        };
        let committing_all =
            || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
        committing(&e, 2);
        // Block k alone carries the entry: it is resolved while the
        // loop is paused in block k's pass, after k's digest.
        let k = processed() + 10;
        e.unresolved(1, k * 48).unwrap();
        pause_at(&e, k, true);
        paused();
        e.resolved(1).unwrap();
        pause_at(&e, k + 3, false);
        resume();
        paused();
        let ready = committing_all()
            && (0..2).all(|o| held(o, k, false) && held(o, k + 1, true) && held(o, k + 2, true));
        if !ready {
            resume();
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            continue;
        }
        assert!(e.commit_through(0, k, 0, None).unwrap());
        assert!(e.commit_through(1, k, 0, Some(k * 48 + 24)).unwrap());
        resume();
        wait(&|| processed() > k + 10);
        let st = e.block_status(0, k).unwrap();
        assert_eq!(st.kind, Kind::Committed, "{st:?}");
        let st = e.block_status(0, k + 1).unwrap();
        assert_ne!(st.kind, Kind::Committed, "{st:?}");
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        return;
    }
    panic!("no pass without an overrun in 10 attempts");
}

/// A full hold ring moves at the F its block's pass read: with output
/// 0's ring shorter than output 1's, output 1 does not commit block k
/// while output 0 withholds it, also when the supervisor withholds k
/// and moves F past it after the pass over the hold rings, before the
/// outputs decide the block at which output 1's ring is full.
#[test]
fn a_full_ring_moves_at_the_fence_of_its_pass() {
    use crate::blocks::{Cause, Kind, PAGE};
    // The hold ring at the default holdMs and contactWindowMs; the
    // overflow of output 1's block f below proves it.
    const RING: u64 = 20;
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // An overrun skip under load leaves a block of the rings without a
    // fenced status: then the outputs are named again on a new engine.
    for _ in 0..10 {
        let (e, _changes, _verdicts, domain) = outputs_only("full-ring", 2);
        let _unpause = Unpause;
        let processed = || e.counters().processed.load(Acquire);
        let fenced =
            |o: usize, k: u64| e.block_status(o, k).is_some_and(|s| s.kind == Kind::Fenced);
        let committing_all =
            || (0..2).all(|o| matches!(e.standing(o), Some(Standing::Committing { .. })));
        // Output 1 commits from a page boundary before block f, and
        // holds every block from f on, F frozen there. Output 0 commits
        // from the page boundary in block kp, five blocks later: its
        // ring is full five blocks after output 1's, which is full at
        // block f + RING.
        let now = processed();
        let kp = ((now + 40) * 48).next_multiple_of(PAGE) / 48;
        let (f, k) = (kp - 5, kp + 1);
        e.set_fence(u64::MAX).unwrap();
        e.commit_from(1, now + 2).unwrap();
        e.set_fence(f * 48).unwrap();
        e.commit_from(0, kp).unwrap();
        pause_at(&e, f + RING, false);
        paused();
        if !committing_all() || !(f..f + RING).all(|j| fenced(1, j)) || !fenced(0, k) {
            resume();
            e.stop();
            std::fs::remove_dir_all(&domain).unwrap();
            continue;
        }
        let st = e.withhold(0, k, Cause::HoldCap).unwrap().unwrap();
        assert_eq!(st.kind, Kind::Withheld);
        e.set_fence(u64::MAX).unwrap();
        resume();
        wait(&|| processed() > f + RING + 5);
        // Output 1's ring was full at the decision: its front block
        // overflowed, at the F the pass read.
        let st = e.block_status(1, f).unwrap();
        assert_eq!(
            (st.kind, st.cause),
            (Kind::Withheld, Cause::Overflow),
            "{st:?}"
        );
        let st = e.block_status(1, k).unwrap();
        assert_ne!(st.kind, Kind::Committed, "{st:?}");
        e.stop();
        std::fs::remove_dir_all(&domain).unwrap();
        return;
    }
    panic!("no pass without an overrun in 10 attempts");
}

/// The engine's own log stamps an `InputDown` or `InputUp` that
/// reached the RT thread late again, from the state the RT thread has:
/// the input goes down once its flow is gone, and comes up once it is
/// there again. Engine `p` writes the flow, engine `e` reads it.
#[test]
fn a_late_input_entry_of_the_own_log_is_stamped_again() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let shm = std::path::Path::new("/dev/shm");
    let base = if shm.is_dir() {
        shm.to_owned()
    } else {
        std::env::temp_dir()
    };
    let domain = base.join(format!("samplez-restamp-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let (src, out) = (
        "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d01",
        "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d02",
    );
    let output = |id: &str| {
        serde_json::json!({ "name": id, "domain": "d", "flowId": id, "label": "restamp",
                                "group": "restamp", "role": "Audio", "channels": 1 })
    };
    // The reader's margin covers the producer's, which its head lags
    // the clock by.
    let start = |inputs: serde_json::Value, out: &str, margin: u32, matrix: serde_json::Value| {
        let v = serde_json::json!({
            "libmxl": libmxl,
            "readerMarginSamples": margin,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": inputs,
            "outputs": [output(out)],
            "matrix": matrix,
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        Engine::start(config, process, None).unwrap()
    };
    let producer = || {
        let p = start(serde_json::json!([]), src, 576, serde_json::json!([]));
        wait(&|| p.status().outputs[0].realised);
        p
    };
    let input = serde_json::json!([{ "name": "in", "domain": "d", "flowId": src,
                                         "channels": 1 }]);
    let p = producer();
    let e = start(input, out, 3 * 576, serde_json::json!([[1.0]]));
    let attached = || {
        let s = e.status();
        s.inputs[0].up && s.inputs[0].state == InputState::Attached
    };
    // Readable while attached, past the polls after the attach too.
    let readable = || {
        std::thread::sleep(3 * POLL_PERIOD);
        e.status().inputs[0].readable
    };
    wait(&attached);
    assert_eq!(readable(), Some(true));
    let late = || e.counters().command_late.load(Relaxed);

    STAMP_LATE.store(true, Relaxed);
    p.stop();
    wait(&|| !e.status().inputs[0].up);
    assert_eq!(readable(), Some(false), "pruned");
    assert_eq!(late(), 1);
    assert!(e.status().diverged.is_some());

    STAMP_LATE.store(true, Relaxed);
    let p = producer();
    wait(&attached);
    assert_eq!(readable(), Some(true));
    assert_eq!(late(), 2);
    e.stop();
    p.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// The RT loop's failure paths run, so that CI's RealtimeSanitizer
/// run over the lib tests checks them: a stall's catch-up and skip, a
/// forward and a backward clock step with the wakes behind the last
/// block after it, and a late input whose flow is deleted, waited for
/// on its head and pruned without a probe, with a strip advanced over
/// the skip. Engine `p` writes the flow,
/// engine `e` reads it.
#[test]
fn the_failure_paths_run_on_the_rt_thread() {
    use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS, STALL_MS};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    /// Takes the clock shift off again, also when the test fails.
    struct Unshift;
    impl Drop for Unshift {
        fn drop(&mut self) {
            CLOCK_STEP_NS.store(0, Relaxed);
            CLOCK_SHIFT_NS.store(0, Relaxed);
            CLOCK_STEP_LOOP.store(0, Relaxed);
        }
    }
    let _unshift = Unshift;
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-failures-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let (src, out) = (
        "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d05",
        "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d06",
    );
    let start = |inputs: serde_json::Value, out: &str, margin: u32, matrix| {
        let v = serde_json::json!({
            "libmxl": libmxl,
            "readerMarginSamples": margin,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": inputs,
            "outputs": [{ "name": out, "domain": "d", "flowId": out, "label": "failures",
                          "group": "failures", "role": "Audio", "channels": 1 }],
            "matrix": matrix,
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        Engine::start(config, process, None).unwrap()
    };
    let p = start(serde_json::json!([]), src, 576, serde_json::json!([]));
    wait(&|| p.status().outputs[0].realised);
    let input = serde_json::json!([{ "name": "in", "domain": "d", "flowId": src,
                                         "channels": 1 }]);
    let e = start(input, out, 3 * 576, serde_json::json!([[1.0]]));
    wait(&|| e.status().inputs[0].state == InputState::Attached);
    // A strip with filters on: Strip::advance runs over the skip.
    let produced = e.counters.produced.load(Acquire);
    let at = (produced / 48 + 40) * 48;
    let mut params = vec![
        StripParam::Trim { db: -6.0 },
        StripParam::Hpf {
            enabled: true,
            freq: 100.0,
        },
        StripParam::Eq { enabled: true },
        StripParam::DynamicsBeforeEq { enabled: true },
    ];
    params.extend(
        samplez_dsp::strip::DEFAULT_BANDS
            .iter()
            .enumerate()
            .map(|(band, b)| StripParam::EqBand {
                band,
                value: samplez_dsp::EqBand { gain_db: 6.0, ..*b },
            }),
    );
    for param in params {
        e.set_strip(0, param, at).unwrap();
    }
    let c = e.counters();
    let n = |a: &AtomicU64| a.load(Relaxed);
    let until = |what: &str, f: &dyn Fn() -> bool| {
        let t = Instant::now();
        while !f() {
            assert!(t.elapsed() < Duration::from_secs(10), "no {what}: {c:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    };

    // A stall: the overdue blocks are caught up, the rest skipped.
    // Both engines' loops take the stall hook, so it is set again
    // until this engine's loop took one.
    until("skip", &|| {
        STALL_MS.store(30, Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        n(&c.overrun_skipped_samples) > 0
    });
    STALL_MS.store(0, Relaxed);
    // A forward and a backward step of libmxl's clock: after the
    // backward one the output holds the blocks below its last commit.
    // Engine `e`'s loop takes each step at its wake, between two of
    // its blocks; both engines read the stepped clock.
    CLOCK_STEP_LOOP.store(std::ptr::from_ref(c) as usize, Relaxed);
    let steps = n(&c.clock_steps);
    CLOCK_STEP_NS.store(50_000_000, Relaxed);
    until("forward step", &|| n(&c.clock_steps) > steps);
    CLOCK_STEP_NS.store(-50_000_000, Relaxed);
    until("backward step", &|| n(&c.clock_steps) > steps + 1);
    // Behind the last block computed, the loop wakes without computing.
    until("wake behind", &|| n(&c.clock_behind_wakes) > 0);
    // The producer stops and its flow is deleted: the input is late,
    // frozen and pruned, and the RT thread never checks the flow.
    p.stop();
    until("late block", &|| n(&c.input_late_blocks) > 0);
    until("prune", &|| {
        e.status().inputs[0].state == InputState::Pruned
    });
    assert_eq!(e.status().rt_panic, None);
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// A strip entry the slot's strip refuses on the RT thread, as on a
/// replica whose slot has another width than the one the entry was
/// checked against, is not applied: it is counted and marks the
/// engine diverged.
#[test]
fn a_strip_entry_refused_on_the_rt_thread_marks_the_engine_diverged() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-refused-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let v = serde_json::json!({
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d07", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d08", "label": "refused",
                      "group": "refused", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let process = rt::setup_process(&config.rt);
    let e = Engine::start(config, process, None).unwrap();
    let c = &e.counters;
    let until = |what: &str, f: &dyn Fn() -> bool| {
        let t = Instant::now();
        while !f() {
            assert!(t.elapsed() < Duration::from_secs(10), "no {what}: {c:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    until("block", &|| c.heartbeat_blocks.load(Relaxed) > 0);
    // Past the checks of set_strip: leg 1 of a mono strip.
    let refused = StripParam::LegTrim { leg: 1, db: 0.0 };
    let mut log = e.log.lock().unwrap();
    let entry = RtEntry {
        id: log.last + 1,
        // 40 blocks past the earliest index: set_strip below checks
        // the same index against a clock that has moved on, and a
        // scheduling delay of a block or more in between would refuse
        // it as too near.
        at_index: (earliest(&e.api, &e.counters, e.margin, e.lead) + 40 * e.block)
            .next_multiple_of(e.block),
        change: Change::Strip {
            slot: 0,
            param: refused,
        },
    };
    log.append(
        entry,
        None,
        Vec::new(),
        e.counters.entries_consumed.load(Acquire),
    )
    .unwrap();
    log.commit(entry.id);
    drop(log);
    e.set_strip(0, StripParam::Trim { db: -3.0 }, entry.at_index)
        .unwrap();
    until("divergence", &|| e.status().diverged.is_some());
    let diverged = e.status().diverged.unwrap();
    assert!(
        diverged.contains("refused by the strip, limiter"),
        "{diverged}"
    );
    // The entry set_strip took is applied; the supervisor's own
    // InputDown for the missing input may come in between.
    until("both entries", &|| c.entries_consumed.load(Relaxed) >= 2);
    assert_eq!(c.strip_refused.load(Relaxed), 1);
    assert_eq!(c.command_late.load(Relaxed), 0);
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// A replayed core for a block the RT thread passed goes back
/// refused, its state untouched: installed there, it would replace the
/// state after a later block with an older one.
#[test]
fn a_handover_the_rt_thread_passed_is_refused() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("handover-passed", 1);
    let copy = loop {
        match e.copy_core(Duration::from_secs(2)) {
            Err(CopyError::Passed { .. }) => continue,
            c => break c.unwrap(),
        }
    };
    wait(&|| e.counters.processed.load(Acquire) > copy.k0 + 2);
    let mut t = e.transfer.as_ref().unwrap().lock().unwrap();
    let h = Handover {
        seq: 1,
        j: copy.k0,
        last_id: copy.last_id,
        core: copy.core,
        replayed: true,
        strips: Vec::new(),
        limiters: Vec::new(),
        delays: Vec::new(),
        out_strips: Vec::new(),
        out_delays: Vec::new(),
        tones: Vec::new(),
        answer: Answer::Waiting,
    };
    assert!(t.handovers.push(h).is_ok());
    let h = loop {
        if let Ok(h) = t.handed.pop() {
            break h;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(h.answer, Answer::Passed);
    assert_eq!(e.counters.handovers_passed.load(Relaxed), 1);
    assert_eq!(e.counters.handovers_installed.load(Relaxed), 0);
    drop(t);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A copy buffer left with the RT thread by a call that timed out, and
/// filled there, `slack` widened so the test thread may stall.
fn copy_left_behind(e: &Engine) {
    let transfer = e.transfer.as_ref().unwrap();
    transfer.lock().unwrap().slack_blocks = 1000;
    // No control waits: the buffer is sent, and no copy is back yet.
    assert_eq!(e.copy_core(Duration::ZERO).err(), Some(CopyError::Timeout));
    assert!(transfer.lock().unwrap().in_flight, "no buffer sent");
    wait(&|| transfer.lock().unwrap().copies.slots() == 1);
}

/// The copy a timed-out call left behind was filled before a control
/// sent after that call: the next call has a buffer filled again, so
/// its copy holds every control sent before it.
#[test]
fn a_copy_left_behind_holds_the_controls_sent_after_it() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("copy-behind", 1);
    copy_left_behind(&e);
    let far = (e.counters.processed.load(Acquire) + 1_000_000) * e.block;
    e.unresolved(1, far).unwrap();
    let c = e.copy_core(Duration::from_secs(2)).unwrap();
    assert_eq!(c.core.unresolved(), &[(1, far)]);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A core fit for a hand-over after block `j`, of an engine with no
/// inputs and no log entries taken.
fn bare_core(e: &Engine, j: u64, core: &mut Core) -> (u64, LogId) {
    core.pos = Some((j + 1) * e.block);
    (j, 0)
}

/// The copy a timed-out call left behind was filled before a core
/// handed over since: the next call has a buffer filled again, after
/// the install.
#[test]
fn a_copy_left_behind_is_filled_again_after_a_hand_over() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("copy-handover", 1);
    copy_left_behind(&e);
    let j = e
        .offer(
            Duration::from_secs(5),
            0,
            false,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            |j, core| bare_core(&e, j, core),
        )
        .unwrap();
    let c = e.copy_core(Duration::from_secs(2)).unwrap();
    assert!(
        c.k0 > j,
        "copy after block {} of a core installed after {j}",
        c.k0
    );
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A product install that finds the control ring full while a core is
/// held retries the drop until its timeout and fails as full, sending
/// nothing; once the ring has room it drops the core before the
/// install.
#[test]
fn a_product_install_retries_the_drop_while_the_ring_is_full() {
    use crate::rt_loop::STALL_MS;
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("drop-full", 1);
    let c = &e.counters;
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: Vec::new(),
        up: Vec::new(),
        flows: Vec::new(),
        outputs: Vec::new(),
        pre_strip_meters: Vec::new(),
        loudness_running: Vec::new(),
        strips: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 0,
        ..Default::default()
    };
    let processed = || c.processed.load(Acquire);
    // The RT thread stalls with the ring full. A stall another test's
    // RT thread takes is set again.
    let t = Instant::now();
    loop {
        assert!(t.elapsed() < Duration::from_secs(10), "no stall");
        STALL_MS.store(500, Relaxed);
        wait(&|| STALL_MS.load(Relaxed) == 0);
        let p = processed();
        std::thread::sleep(Duration::from_millis(20));
        if processed() == p {
            break;
        }
    }
    while e.withdraw().is_ok() {}
    assert_eq!(
        e.install_product(&state, 0, Duration::from_millis(50)),
        Err(ReplayError::Full)
    );
    assert_eq!(c.blocks_without_core.load(Relaxed), 0, "dropped while full");
    let j = e
        .install_product(&state, 0, Duration::from_secs(5))
        .unwrap();
    assert!(
        c.blocks_without_core.load(Relaxed) > 0,
        "installed over a held core"
    );
    assert!(processed() > j);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A product install's drop, hand-over and install share one timeout:
/// a drop that waited out most of it leaves the install the rest. The
/// RT thread stalls with the control ring full, then computes nothing
/// for 3 s, its clock stepped back.
#[test]
fn a_product_install_keeps_to_one_timeout() {
    use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS, STALL_MS};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    struct Unshift;
    impl Drop for Unshift {
        fn drop(&mut self) {
            STALL_MS.store(0, Relaxed);
            CLOCK_STEP_NS.store(0, Relaxed);
            CLOCK_SHIFT_NS.store(0, Relaxed);
            CLOCK_STEP_LOOP.store(0, Relaxed);
        }
    }
    let _unshift = Unshift;
    let (e, _statuses, _verdicts, domain) = outputs_only("drop-deadline", 1);
    let c = &e.counters;
    CLOCK_STEP_LOOP.store(std::ptr::from_ref(&**c) as usize, Relaxed);
    let state = ProductState {
        limiters: Vec::new(),
        delays: Vec::new(),
        tones: Vec::new(),
        tone_ramps: Vec::new(),
        gains: Vec::new(),
        up: Vec::new(),
        flows: Vec::new(),
        outputs: Vec::new(),
        pre_strip_meters: Vec::new(),
        loudness_running: Vec::new(),
        strips: Vec::new(),
        ducks: Vec::new(),
        masks: Vec::new(),
        in_flight: Default::default(),
        pending: Vec::new(),
        fading: Vec::new(),
        last_id: 0,
        ..Default::default()
    };
    let processed = || c.processed.load(Acquire);
    let t = Instant::now();
    loop {
        assert!(t.elapsed() < Duration::from_secs(10), "no stall");
        STALL_MS.store(600, Relaxed);
        wait(&|| STALL_MS.load(Relaxed) == 0);
        let p = processed();
        std::thread::sleep(Duration::from_millis(20));
        if processed() == p {
            break;
        }
    }
    // Taken at the end of the stall, in the same wake.
    CLOCK_STEP_NS.store(-3_000_000_000, Relaxed);
    while e.withdraw().is_ok() {}
    let t = Instant::now();
    let r = e.install_product(&state, 0, Duration::from_secs(1));
    let took = t.elapsed();
    assert_eq!(r, Err(ReplayError::Timeout { passed: 0 }));
    assert!(took < Duration::from_millis(1300), "took {took:?}");
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A hand-over a call gave up on can still be installed. The drop
/// before a product install waits for its answer and follows it, so
/// no core is held after it returns: the clock steps back as the
/// hand-over is sent, and the RT thread computes nothing, answering
/// nothing, until the clock is due at its block again.
#[test]
fn the_drop_follows_a_hand_over_left_unanswered() {
    use crate::rt_loop::{CLOCK_SHIFT_NS, CLOCK_STEP_LOOP, CLOCK_STEP_NS};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    struct Unshift;
    impl Drop for Unshift {
        fn drop(&mut self) {
            CLOCK_STEP_NS.store(0, Relaxed);
            CLOCK_SHIFT_NS.store(0, Relaxed);
            CLOCK_STEP_LOOP.store(0, Relaxed);
        }
    }
    let _unshift = Unshift;
    let (e, _statuses, _verdicts, domain) = outputs_only("drop-unanswered", 1);
    let c = &e.counters;
    CLOCK_STEP_LOOP.store(std::ptr::from_ref(&**c) as usize, Relaxed);
    let without = || c.blocks_without_core.load(Relaxed);
    let installed = || c.handovers_installed.load(Relaxed);
    // A hand-over taken a block late is refused, not installed: tried
    // again.
    for attempt in 0.. {
        assert!(attempt < 5, "no hand-over installed after its call");
        e.drop_core().unwrap();
        let w = without();
        wait(&|| without() > w + 5);
        let before = installed();
        let r = e.offer(
            Duration::from_secs(5),
            0,
            false,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            |j, core| {
                CLOCK_STEP_NS.store(-1_500_000_000, Relaxed);
                bare_core(&e, j, core)
            },
        );
        assert_eq!(r, Err(ReplayError::Timeout { passed: 0 }));
        assert!(e.transfer.as_ref().unwrap().lock().unwrap().handing);
        e.drop_core_by(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let w = without();
        let p = c.processed.load(Acquire);
        wait(&|| c.processed.load(Acquire) > p + 20);
        if installed() == before {
            continue;
        }
        assert!(without() > w, "a core held after the drop");
        break;
    }
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Blocks a late wake skips while no core is held are not computed
/// for want of a core, as every other block then: they are reported
/// as such in one gapless run of ranges, not as a skip.
#[test]
fn a_skip_without_a_core_is_reported_as_no_core() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, statuses, _verdicts, domain) = outputs_only("no-core-skip", 1);
    let c = &e.counters;
    e.drop_core().unwrap();
    wait(&|| c.blocks_without_core.load(Relaxed) > 5);
    // Changes from before the drop, a skip under load among them.
    statuses.try_iter().for_each(drop);
    let skipped = c.overrun_skipped_samples.load(Relaxed);
    // A stall of 40 blocks, past maxCatchupBlocks. An RT thread of a
    // test holding no engine lock may take the stall: it is set again
    // until one is seen.
    let mut ranges = Vec::new();
    let t = Instant::now();
    while !ranges.iter().any(|&(from, to)| to - from >= 20) {
        assert!(t.elapsed() < Duration::from_secs(10), "no stall");
        if crate::rt_loop::STALL_MS.load(Relaxed) == 0 {
            crate::rt_loop::STALL_MS.store(40, Relaxed);
        }
        match statuses.recv_timeout(Duration::from_millis(200)) {
            Ok(StatusChange::NoCore { from, to }) => ranges.push((from, to)),
            Ok(StatusChange::Skipped { from, to }) => panic!("skipped {from}..{to}"),
            _ => {}
        }
    }
    crate::rt_loop::STALL_MS.store(0, Relaxed);
    assert!(ranges.windows(2).all(|w| w[0].1 == w[1].0), "{ranges:?}");
    assert_eq!(c.overrun_skipped_samples.load(Relaxed), skipped);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// The earliest index a refusal as too near names is a block start, so
/// a caller can retry with it; while the RT thread is behind, the
/// clock less the reader margin sets it, at any sample.
#[test]
fn too_near_names_a_block_start() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let domain = std::env::temp_dir().join(format!("samplez-too-near-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let v = serde_json::json!({
        "libmxl": libmxl,
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0b", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0c", "label": "near",
                      "group": "near", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let process = rt::setup_process(&config.rt);
    let e = Engine::start(config, process, None).unwrap();
    wait(&|| e.counters.processed.load(Acquire) > 10);
    crate::rt_loop::STALL_MS.store(300, Relaxed);
    std::thread::sleep(Duration::from_millis(50));
    let mut named = Vec::new();
    for _ in 0..8 {
        match e.set_gain(0, 0, 0.5, 0) {
            Err(GainError::TooNear { earliest, .. }) => named.push(earliest),
            other => panic!("{other:?}"),
        }
        match e.reset_loudness(0, 0) {
            Err(GainError::TooNear { earliest, .. }) => named.push(earliest),
            other => panic!("{other:?}"),
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    crate::rt_loop::STALL_MS.store(0, Relaxed);
    assert!(named.iter().all(|x| x % e.block == 0), "{named:?}");
    e.stop();
    std::fs::remove_dir_all(&domain).unwrap();
}

/// An entry applied before its generation arrived keeps waiting for it
/// across an install, its slot held: a product core holds no
/// generation and an empty awaiting list, so cleared here the
/// generation would be returned unused when it came, and the slot
/// would stay on the old flow that every other replica left.
#[test]
fn an_install_keeps_an_entry_waiting_for_its_generation() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("install-awaiting", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0d", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0e", "label": "await",
                      "group": "await", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    let route = RtEntry {
        id: 5,
        at_index: 96,
        change: Change::Route { slot: 0 },
    };
    rt.awaiting_gen.push(route);
    rt.slots[0].held = true;
    rt.install_core(&mut core, 5, true);
    assert_eq!(rt.awaiting_gen, [route]);
    assert!(rt.slots[0].held);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A config of a stereo tone generator on a stereo output.
fn tone_config() -> Config {
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": [{ "name": "tone", "tone": {}, "channels": 2 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d20", "label": "tone",
                      "group": "tone", "role": "Audio", "channels": 2 }],
        "matrix": [[1.0, 0.0], [0.0, 1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

/// A tone generator's slot through the RT loop, from a block near
/// index 2^40: settled, each leg is the formula, bit for bit, in no
/// input domain, never silenced, failed or late. A core copied at
/// block P and installed into a second loop continues it: both
/// compute the same bits through a level, frequency and ident change
/// pending at P, as a rebuilt replica computes a running one's. Fails
/// with `d.tone = s.tone;` dropped from `install_core`, or with the
/// generator bypassed in `process`.
#[test]
fn a_tone_slot_is_its_formula_and_a_copied_core_continues_it() {
    let config = tone_config();
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (slots, in_domains) = new_slots(&config);
    assert!(in_domains.is_empty());
    assert_eq!(slots[0].domain, NO_DOMAIN);
    let rt = || {
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        replay_rt(&config, &api, Vec::new(), hashes, control)
    };
    let b = u64::from(config.block_size);
    let first = (1u64 << 40) / b;
    let tone = |id, k: u64, frequency_hz, level_db, ident| RtEntry {
        id,
        at_index: k * b,
        change: Change::Tone {
            slot: 0,
            params: ToneParams {
                frequency_hz,
                level_db,
                ident,
            },
        },
    };
    let mut a = rt();
    assert!(a.mix.matrix.set(0, 0, 1.0, 0) && a.mix.matrix.set(1, 1, 1.0, 0));
    for e in [
        tone(1, first + 2, 997, -6.0, false),
        tone(2, first + 60, 1000, -12.0, true),
        tone(3, first + 120, 440, 0.0, false),
    ] {
        assert!(a.mix.push(e));
    }
    let leg = |rt: &Rt, c: usize| rt.outbuf.channel(c)[..b as usize].to_vec();
    let p = first + 110;
    let settled = first + 2 + u64::from(config.ramp_samples).div_ceil(b);
    let level = samplez_dsp::tone::level_gain(-6.0);
    for k in first..=p {
        a.process(k, u64::MAX);
        if (settled..first + 60).contains(&k) {
            for c in 0..2 {
                for (j, &y) in leg(&a, c).iter().enumerate() {
                    let n = k * b + j as u64;
                    let want = level * samplez_dsp::tone::sine(n, 997, 48_000);
                    assert_eq!(y.to_bits(), want.to_bits(), "leg {c} at {n}");
                }
            }
        }
    }
    let counters = &a.counters;
    assert_eq!(counters.input_silenced_blocks.load(Relaxed), 0);
    assert_eq!(counters.input_late_blocks.load(Relaxed), 0);
    assert_eq!(counters.strip_refused.load(Relaxed), 0);
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    a.capture(&mut core);
    assert!(core.tone(0).is_some());
    let mut c = rt();
    c.install_core(&mut core, 3, true);
    for k in p + 1..p + 400 {
        a.process(k, u64::MAX);
        c.process(k, u64::MAX);
        for ch in 0..2 {
            let (x, y) = (leg(&a, ch), leg(&c, ch));
            let same = x.iter().zip(&y).all(|(x, y)| x.to_bits() == y.to_bits());
            assert!(same, "block {k} leg {ch}");
        }
    }
}

/// Two loops metering a 9.6 kHz tone input with `truePeak`, the
/// second skipping blocks as after an overrun up to a window
/// boundary, read the same true peak, bit for bit, in every 100 ms
/// window both meter whole, the one after the skip included, and the
/// same largest true peak: after the skip the interpolator restarts
/// and leaves out the outputs that reach back over it. The tone's
/// samples fall 18 degrees off its crest, so every window reads the
/// true peak `TruePeak` computes from the formula, above the sample
/// peak. Fails with `self.tp_skip = TP_HISTORY` dropped from
/// `OutMeter::add`, the window after the skip then reading the
/// interpolator's rise from silence, and with that reset dropped
/// whole, the window then reading history from before the skip.
#[test]
fn an_input_true_peak_is_the_same_bits_across_an_overrun_skip() {
    let mut config = tone_config();
    config.inputs[0].true_peak = true;
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let b = u64::from(config.block_size);
    let (f, level) = (9600, samplez_dsp::tone::level_gain(-6.0));
    // Ending on a window boundary, so the window after it is metered
    // whole, and not a multiple of the tone's 5-sample period long, so
    // history kept across it is out of phase.
    let skip = 2003..2100;
    assert!((skip.end * b).is_multiple_of(4800));
    assert!(!((skip.end - skip.start) * b).is_multiple_of(5));
    let run = |skip: &std::ops::Range<u64>| {
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &api, Vec::new(), hashes, control);
        // As the engine's loop, its input meters included.
        rt.replay = false;
        (rt.buses, rt.taps, rt.pre_taps) = new_meters(&config);
        let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
        rt.meters = meters;
        let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
        rt.duck_meters = ducks;
        let (ret, _back) = rtrb::RingBuffer::new(64);
        rt.ret = ret;
        let params = ToneParams {
            frequency_hz: f,
            level_db: -6.0,
            ident: false,
        };
        let change = Change::Tone { slot: 0, params };
        assert!(rt.mix.push(RtEntry {
            id: 1,
            at_index: 0,
            change,
        }));
        let tap = config.outputs.len() as u32;
        let mut l = OutputLoudness::new(config.inputs[0].loudness_weights().unwrap());
        let mut windows = std::collections::BTreeMap::new();
        let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        for k in (0..2400).filter(|k| !skip.contains(k)) {
            rt.process(k, u64::MAX);
            while let Ok(r) = rx.pop() {
                if r.meter == tap {
                    l.record(&r);
                }
            }
            if let Some(w) = l.meters.window {
                windows.insert(w, (bits(&l.meters.true_peak), bits(&l.meters.peak)));
            }
        }
        (windows, bits(&l.meters.max_true_peak))
    };
    let (whole, max) = run(&(0..0));
    let (skipped, skipped_max) = run(&skip);
    let after = skip.end * b / 4800;
    assert!(!skipped.contains_key(&(after - 1)) && skipped.contains_key(&after));
    let x: Vec<f32> = (0..71)
        .map(|n| level * samplez_dsp::tone::sine(n, f, 48_000))
        .collect();
    let mut tp = samplez_dsp::TruePeak::new(1, 64);
    tp.process(0, &x[..samplez_dsp::TruePeak::HISTORY]);
    let want = tp.process(0, &x[samplez_dsp::TruePeak::HISTORY..]);
    let peak = x.iter().fold(0.0f32, |m, v| m.max(v.abs()));
    assert!(want > peak * 1.04, "{want} {peak}");
    for (w, (true_peak, peak)) in &whole {
        if *w > 0 {
            assert_eq!(*true_peak, [want.to_bits(); 2], "window {w}");
            assert!(peak.iter().all(|&p| f32::from_bits(p) < want));
        }
        if let Some(s) = skipped.get(w) {
            assert_eq!(s.0, *true_peak, "window {w}");
        }
    }
    assert_eq!(skipped_max, max);
}

/// A stereo input feeding a stereo output that correlates its legs
/// with `correlating`, a second stereo output that does not, and a
/// 6-channel configured meter that does with `correlating`, legs 0
/// and 1 of each fed by the input's L and R.
fn correlation_config(correlating: bool) -> Config {
    let out = |o: usize, correlation: bool| {
        serde_json::json!({ "name": format!("out{o}"), "domain": "d",
                "flowId": format!("6fbec3b1-1b0f-417d-9059-00000000000{o}"), "label": "o",
                "group": "g", "role": "Audio", "channels": 2, "correlation": correlation })
    };
    let mut matrix = vec![vec![0.0; 10]; 2];
    for c in [0, 2, 4] {
        (matrix[0][c], matrix[1][c + 1]) = (1.0, 1.0);
    }
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "5fbec3b1-1b0f-417d-9059-000000000000", "channels": 2 }],
        "outputs": [out(0, correlating), out(1, false)],
        "meters": [{ "name": "wide", "channels": 6, "correlation": correlating }],
        "matrix": matrix,
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

/// The meter records of [`correlation_config`] fed `source` over
/// blocks `0..blocks` but `skip`, as an overrun skips them, and per
/// 100 ms window read, the correlation bits of each output and meter
/// read at its end.
#[allow(clippy::type_complexity)]
fn correlate(
    config: &Config,
    source: crate::rt_loop::TestInput,
    blocks: u64,
    skip: std::ops::Range<u64>,
) -> (
    Vec<MeterRecord>,
    std::collections::BTreeMap<(usize, u64), Option<u64>>,
    Vec<OutputMeters>,
) {
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(config, &api, vec![None], hashes, control);
    // As the engine's loop: configured and input meters, inputs read.
    rt.replay = false;
    (rt.buses, rt.taps, rt.pre_taps) = new_meters(config);
    rt.test_input = Some(source);
    for (row, gains) in config.matrix.iter().enumerate() {
        for (column, &g) in gains.iter().enumerate() {
            assert!(rt.mix.matrix.set(row, column, g, 0));
        }
    }
    let (meters, mut rx) = rtrb::RingBuffer::new(1 << 12);
    rt.meters = meters;
    let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
    rt.duck_meters = ducks;
    let (ret, _back) = rtrb::RingBuffer::new(64);
    rt.ret = ret;
    let elements = (config.outputs.iter().map(|o| (o.channels, o.correlation)))
        .chain(config.meters.iter().map(|m| (m.channels, m.correlation)));
    let mut l: Vec<OutputLoudness> = elements
        .map(|(n, on)| OutputLoudness::new(vec![1.0; n as usize]).correlating(on))
        .collect();
    let (mut records, mut windows) = (Vec::new(), std::collections::BTreeMap::new());
    for k in (0..blocks).filter(|k| !skip.contains(k)) {
        rt.process(k, u64::MAX);
        while let Ok(r) = rx.pop() {
            if let Some(l) = l.get_mut(r.meter as usize) {
                l.record(&r);
                records.push(r);
            }
        }
        for (m, l) in l.iter().enumerate() {
            if let Some(w) = l.meters.window {
                windows.insert((m, w), l.meters.correlation.map(f64::to_bits));
            }
        }
    }
    assert_eq!(rt.counters.meter_dropped.load(Relaxed), 0);
    (records, windows, l.into_iter().map(|l| l.meters).collect())
}

/// A 1 kHz sine at half scale at sample `n`, phase-shifted by
/// `quarters` quarter periods.
fn sine_1k(n: u64, quarters: u64) -> f32 {
    let phase = ((n * 1000 + quarters * 12_000) % 48_000) as f64 / 48_000.0;
    (0.5 * (std::f64::consts::TAU * phase).sin()) as f32
}

/// An output and a meter that correlate their legs read +1 for L = R,
/// -1 for L = -R, 0 within 1e-3 for sines in quadrature, 0 with one
/// leg silent and none with both, over each 400 ms; the 6-channel
/// meter reads its legs 0 and 1 as the stereo output does. The output
/// that does not correlate reads none, and its records, and those of
/// the others but their cross terms, are the same bits as without
/// the configuration. Fails with the cross term accumulating L * L
/// in `OutMeter::add`, which reads L = -R as +1.
#[test]
fn correlation_reads_the_relation_of_the_legs() {
    let config = correlation_config(true);
    let cases: [(crate::rt_loop::TestInput, Option<(f64, f64)>); 5] = [
        (|_, _, _, n| sine_1k(n, 0), Some((1.0, 0.0))),
        (
            |_, _, c, n| sine_1k(n, 0) * if c == 0 { 1.0 } else { -1.0 },
            Some((-1.0, 0.0)),
        ),
        (|_, _, c, n| sine_1k(n, c as u64), Some((0.0, 1e-3))),
        (
            |_, _, c, n| if c == 0 { sine_1k(n, 0) } else { 0.0 },
            Some((0.0, 0.0)),
        ),
        (|_, _, _, _| 0.0, None),
    ];
    for (i, (source, want)) in cases.into_iter().enumerate() {
        let (_, windows, meters) = correlate(&config, source, 500, 0..0);
        assert_eq!(meters[0].window, Some(4), "case {i}");
        for m in [0, 2] {
            // From the fourth window on, 400 ms are metered.
            for w in 3..5 {
                let got = windows[&(m, w)].map(f64::from_bits);
                match (got, want) {
                    (Some(got), Some((want, tolerance))) => {
                        assert!((got - want).abs() <= tolerance, "case {i} meter {m}: {got}");
                    }
                    (got, want) => assert_eq!(got, want.map(|w| w.0), "case {i} meter {m}"),
                }
            }
        }
        assert_eq!(meters[1].correlation, None, "case {i}");
    }
    // The configuration adds the cross terms and changes nothing else.
    let source: crate::rt_loop::TestInput = |_, _, c, n| sine_1k(n, c as u64);
    let (on, ..) = correlate(&config, source, 500, 0..0);
    let (off, ..) = correlate(&correlation_config(false), source, 500, 0..0);
    let fields = |r: &MeterRecord| {
        let bits = [r.peak.to_bits(), r.true_peak.to_bits()];
        let sums = [r.sum_sq.to_bits(), r.plain_sum_sq.to_bits()];
        (
            r.meter, r.channel, r.slice, r.frames, bits, sums, r.reset_at, r.flags,
        )
    };
    let crosses = on.iter().filter(|r| r.cross()).count();
    assert_eq!(crosses, 2 * 50);
    let on: Vec<_> = on.iter().filter(|r| !r.cross()).map(fields).collect();
    assert_eq!(on, off.iter().map(fields).collect::<Vec<_>>());
}

/// Two loops metering the same input, the second skipping blocks as
/// after an overrun up to a window boundary, read the same
/// correlation, bit for bit, in every 100 ms window whose 400 ms both
/// metered whole: the reading is a function of the slices' indices,
/// not of which records arrived last. The legs beat at 1 Hz, so the
/// reading moves from window to window, and every window's reading
/// is the correlation computed from the samples of its 400 ms,
/// `[(w - 3) * 4800, (w + 1) * 4800)`. Fails with `Correlation::add`
/// placing a slice in the window by the count of slices it added
/// rather than by the slice's index, which the skip shifts, and with
/// `Correlation::read` summing three windows, five over a ring of
/// five, or the window before.
#[test]
fn correlation_is_the_same_bits_across_an_overrun_skip() {
    let config = correlation_config(true);
    let source: crate::rt_loop::TestInput = |_, _, c, n| {
        let hz = if c == 0 { 440 } else { 441 };
        let phase = ((n * hz) % 48_000) as f64 / 48_000.0;
        (0.5 * (std::f64::consts::TAU * phase).sin()) as f32
    };
    // 48-sample blocks; the skip ends on a window boundary.
    let skip = 1003u64..1100;
    assert!((skip.end * 48).is_multiple_of(4800));
    let (_, whole, _) = correlate(&config, source, 2400, 0..0);
    // From the fifth window on, clear of the input's fade-in.
    for (&(m, w), bits) in whole.iter().filter(|((m, w), _)| *m != 1 && *w >= 4) {
        let mut sums = [0.0f64; 3];
        for n in (w - 3) * 4800..(w + 1) * 4800 {
            let (l, r) = (f64::from(source(0, 0, 0, n)), f64::from(source(0, 0, 1, n)));
            for (s, v) in sums.iter_mut().zip([l * l, r * r, l * r]) {
                *s += v;
            }
        }
        let want = sums[2] / (sums[0] * sums[1]).sqrt();
        let got = f64::from_bits(bits.expect("a correlation"));
        assert!(
            (got - want).abs() < 1e-9,
            "meter {m} window {w}: {got} {want}"
        );
    }
    let (_, skipped, _) = correlate(&config, source, 2400, skip.clone());
    let (cut, after) = (skip.start * 48 / 4800, skip.end * 48 / 4800);
    let mut compared = std::collections::BTreeSet::new();
    for (&(m, w), bits) in &skipped {
        if w < cut || w >= after + 3 {
            assert_eq!(*bits, whole[&(m, w)], "meter {m} window {w}");
            compared.insert(bits.map(f64::from_bits).map(|c| (c * 10.0).round() as i64));
        }
    }
    assert!(skipped.contains_key(&(0, after + 3)) && skipped.contains_key(&(2, 23)));
    assert!(compared.len() > 5, "{compared:?}");
}

/// A tone input with `preStripRows` feeds its pre-strip rows, after
/// the input's rows, untrimmed: an output whose leg 0 takes the
/// input's row 0 reads a -20 dB trim, while its leg 1, on the
/// pre-strip row of the input's second channel, stays the formula,
/// bit for bit. A core copied at block P, with a gain on that
/// pre-strip row pending at it, continues both legs in a second loop
/// with the same bits, the gain applied on the same row. Fails with
/// the copy into the pre-strip rows moved after `run_strip` in
/// `Slot::finish`, leg 1 then reading the trim.
#[test]
fn pre_strip_rows_carry_the_input_before_its_trim_into_a_copied_core() {
    let mut config = tone_config();
    config.inputs[0].pre_strip_rows = true;
    config.matrix = vec![vec![0.0; 2]; 4];
    assert_eq!(config.rows(), 4);
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let rt = || {
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        replay_rt(&config, &api, Vec::new(), hashes, control)
    };
    let b = u64::from(config.block_size);
    let first = (1u64 << 40) / b;
    let mut a = rt();
    assert!(a.mix.matrix.set(0, 0, 1.0, 0) && a.mix.matrix.set(3, 1, 1.0, 0));
    let params = ToneParams {
        frequency_hz: 997,
        level_db: -6.0,
        ident: false,
    };
    let trim = StripParam::Trim { db: -20.0 };
    let (input, output, gain, fade) = (3, 1, 0.5, None);
    let entries = [
        (first + 2, Change::Tone { slot: 0, params }),
        (
            first + 20,
            Change::Strip {
                slot: 0,
                param: trim,
            },
        ),
        (
            first + 150,
            Change::Gain {
                input,
                output,
                gain,
                fade,
            },
        ),
    ];
    for (id, (k, change)) in entries.into_iter().enumerate() {
        let at_index = k * b;
        assert!(a.mix.push(RtEntry {
            id: id as u64 + 1,
            at_index,
            change
        }));
    }
    let leg = |rt: &Rt, c: usize| rt.outbuf.channel(c)[..b as usize].to_vec();
    let ramp = u64::from(config.ramp_samples).div_ceil(b);
    let level = samplez_dsp::tone::level_gain(-6.0);
    let want = |k: u64, j: usize| level * samplez_dsp::tone::sine(k * b + j as u64, 997, 48_000);
    let p = first + 140;
    for k in first..=p {
        a.process(k, u64::MAX);
        if k > first + 20 + ramp {
            for (j, (&x, &y)) in leg(&a, 0).iter().zip(&leg(&a, 1)).enumerate() {
                assert_eq!(y.to_bits(), want(k, j).to_bits(), "leg 1 at block {k}");
                assert!((x - 0.1 * want(k, j)).abs() < 1e-4, "leg 0 at block {k}");
            }
        }
    }
    assert_eq!(a.counters.strip_refused.load(Relaxed), 0);
    let (rows, outputs) = (config.rows(), config.output_channels());
    let mut core = Box::new(Core::new(rows, outputs, 1, 64, 1));
    a.capture(&mut core);
    let mut c = rt();
    c.install_core(&mut core, 3, true);
    for k in p + 1..p + 400 {
        a.process(k, u64::MAX);
        c.process(k, u64::MAX);
        for ch in 0..2 {
            let (x, y) = (leg(&a, ch), leg(&c, ch));
            let same = x.iter().zip(&y).all(|(x, y)| x.to_bits() == y.to_bits());
            assert!(same, "block {k} leg {ch}");
        }
        if k > first + 150 + ramp {
            for (j, &y) in leg(&c, 1).iter().enumerate() {
                assert!((y - 0.5 * want(k, j)).abs() < 1e-6, "leg 1 at block {k}");
            }
        }
    }
}

/// Two loops on one log metering a 1 kHz tone input with
/// `preStripMeter`, trimmed by 20 dB in its strip, the second
/// skipping blocks across the trim as after an overrun, read the same
/// pre-strip peaks, bit for bit, in every 100 ms window both meter
/// whole. The pre-strip meter reads the tone's peak before the trim
/// and after it alike, while the input's meter after the strip reads
/// it 20 dB down. Fails with the copy into `prebuf` moved after
/// `run_strip` in `Slot::finish`, the pre-strip meter then reading the
/// trim.
#[test]
fn a_pre_strip_meter_reads_the_input_before_its_trim_across_a_skip() {
    let mut config = tone_config();
    config.inputs[0].pre_strip_meter = true;
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let b = u64::from(config.block_size);
    assert!(4800 % b == 0);
    let per_window = 4800 / b;
    // The trim at the start of window 10; the skip from window 9 to
    // window 12, so the loop skipping it meters window 12 on whole.
    let trim_at = 10 * 4800;
    let skip = 9 * per_window..12 * per_window;
    let run = |skip: &std::ops::Range<u64>| {
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(&config, &api, Vec::new(), hashes, control);
        rt.replay = false;
        (rt.buses, rt.taps, rt.pre_taps) = new_meters(&config);
        let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
        rt.meters = meters;
        let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
        rt.duck_meters = ducks;
        let (ret, _back) = rtrb::RingBuffer::new(64);
        rt.ret = ret;
        let params = ToneParams {
            frequency_hz: 1000,
            level_db: -6.0,
            ident: false,
        };
        let on = Change::PreStripMeter {
            slot: 0,
            enabled: true,
        };
        let entries = [
            (0, Change::Tone { slot: 0, params }),
            (0, on),
            (
                trim_at,
                Change::Strip {
                    slot: 0,
                    param: StripParam::Trim { db: -20.0 },
                },
            ),
        ];
        for (id, (at_index, change)) in entries.into_iter().enumerate() {
            assert!(rt.mix.push(RtEntry {
                id: id as u64 + 1,
                at_index,
                change,
            }));
        }
        let (tap, pre) = (1, 2);
        let weights = || config.inputs[0].loudness_weights().unwrap();
        let (mut post_l, mut pre_l) = (
            OutputLoudness::new(weights()),
            OutputLoudness::new(weights()),
        );
        let mut windows = std::collections::BTreeMap::new();
        let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        for k in (0..30 * per_window).filter(|k| !skip.contains(k)) {
            rt.process(k, u64::MAX);
            while let Ok(r) = rx.pop() {
                match r.meter {
                    m if m == tap => post_l.record(&r),
                    m if m == pre => pre_l.record(&r),
                    m => assert_eq!(m, 0),
                }
            }
            if let (Some(w), Some(v)) = (pre_l.meters.window, post_l.meters.window) {
                assert_eq!(w, v);
                windows.insert(w, (bits(&pre_l.meters.peak), bits(&post_l.meters.peak)));
            }
        }
        windows
    };
    let whole = run(&(0..0));
    let skipped = run(&skip);
    assert!(!skipped.contains_key(&11) && skipped.contains_key(&12));
    let level = whole[&5].0.clone();
    let db = |v: f32| 20.0 * f64::from(v).log10();
    for (w, (pre, post)) in &whole {
        if *w >= 1 {
            assert_eq!(*pre, level, "window {w}");
        }
        // Window 10 holds the trim's ramp.
        if *w >= 11 {
            for (p, q) in pre.iter().zip(post) {
                let (p, q) = (f32::from_bits(*p), f32::from_bits(*q));
                assert!((db(p) - db(q) - 20.0).abs() < 0.01, "window {w}");
            }
        } else if (1..10).contains(w) {
            assert_eq!(pre, post, "window {w}");
        }
        if let Some(s) = skipped.get(w) {
            assert_eq!(s.0, *pre, "window {w}");
        }
    }
}

/// [`tone_config`] at a ramp of 0, its input with a delay line of up
/// to 9600 samples.
fn delayed_tone_config() -> Config {
    let mut config = tone_config();
    config.ramp_samples = 0;
    config.inputs[0].max_delay_samples = 9600;
    config
}

/// The default tone's sample at index `n`, silence below 0.
fn default_tone(n: i64) -> f32 {
    let p = ToneParams::DEFAULT;
    u64::try_from(n).map_or(0.0, |n| {
        samplez_dsp::tone::level_gain(p.level_db)
            * samplez_dsp::tone::sine(n, p.frequency_hz, 48_000)
    })
}

/// A loop on `config` metering as the engine's does, the matrix
/// coefficients `ones` set to 1, run with `changes` over blocks
/// `0..blocks`: the loop, each block's output legs, and the peaks of
/// the input's pre-strip meter per 100 ms window, none without
/// `preStripMeter`.
#[allow(clippy::type_complexity)]
fn delayed_tone(
    config: &Config,
    ones: &[(usize, usize)],
    changes: Vec<(u64, Change)>,
    blocks: u64,
) -> (
    Rt,
    Vec<[Vec<f32>; 2]>,
    std::collections::BTreeMap<u64, Vec<f32>>,
) {
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(config, &api, Vec::new(), hashes, control);
    rt.replay = false;
    (rt.buses, rt.taps, rt.pre_taps) = new_meters(config);
    let (meters, mut rx) = rtrb::RingBuffer::new(1 << 10);
    rt.meters = meters;
    let (ducks, _ducks) = rtrb::RingBuffer::new(1 << 10);
    rt.duck_meters = ducks;
    let (ret, _back) = rtrb::RingBuffer::new(64);
    rt.ret = ret;
    for &(row, column) in ones {
        assert!(rt.mix.matrix.set(row, column, 1.0, 0));
    }
    for (id, (at_index, change)) in changes.into_iter().enumerate() {
        assert!(rt.mix.push(RtEntry {
            id: id as u64 + 1,
            at_index,
            change,
        }));
    }
    let b = config.block_size as usize;
    // The outputs' meters, the input's, then its pre-strip meter.
    let pre = config.outputs.len() as u32 + 1;
    let mut l = OutputLoudness::new(config.inputs[0].loudness_weights().unwrap());
    let (mut legs, mut windows) = (Vec::new(), std::collections::BTreeMap::new());
    for k in 0..blocks {
        rt.process(k, u64::MAX);
        legs.push([0, 1].map(|c| rt.outbuf.channel(c)[..b].to_vec()));
        while let Ok(r) = rx.pop() {
            if r.meter == pre {
                l.record_restarting(&r);
            }
        }
        if let Some(w) = l.meters.window {
            windows.insert(w, l.meters.peak.clone());
        }
    }
    assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
    (rt, legs, windows)
}

/// An input with `preStripRows`, trimmed by 20 dB in its strip, delayed
/// by 1000 samples from index D: an output leg fed only by the
/// pre-strip row of its second channel is the tone untrimmed and
/// delayed, bit for bit: the tone before D, silent for the 999 samples
/// the delay reaches back over, then the tone 1000 samples late. Fails
/// with the delay run after the copy into the pre-strip rows in
/// `Slot::finish`, the row then undelayed.
#[test]
fn a_pre_strip_row_carries_the_input_as_late_as_its_delay() {
    let mut config = delayed_tone_config();
    config.inputs[0].pre_strip_rows = true;
    config.matrix = vec![vec![0.0; 2]; 4];
    let b = u64::from(config.block_size);
    let (n, d) = (1000, 100 * b);
    let trim = StripParam::Trim { db: -20.0 };
    let changes = vec![
        (
            0,
            Change::Strip {
                slot: 0,
                param: trim,
            },
        ),
        (
            d,
            Change::Delay {
                slot: 0,
                samples: n as u32,
            },
        ),
    ];
    let blocks = (d + n) / b + 100;
    let (_, legs, _) = delayed_tone(&config, &[(0, 0), (3, 1)], changes, blocks);
    for (k, [_, leg]) in legs.iter().enumerate() {
        for (j, &y) in leg.iter().enumerate() {
            let i = k as u64 * b + j as u64;
            let want = match i {
                _ if i < d => default_tone(i as i64),
                _ if i < d + n - 1 => 0.0,
                _ => default_tone(i as i64 - n as i64),
            };
            assert_eq!(y.to_bits(), want.to_bits(), "at {i}");
        }
    }
}

/// An input metered before its strip with `preStripMeter`, delayed by
/// 7200 samples from index 0, its tone stepping from -40 to -6 dB at
/// the start of window 10: the pre-strip meter reads the step 7200
/// samples later, in window 11, and window 10 at -40 dB. Fails with
/// the delay run after the copy into `prebuf` in `Slot::finish`, the
/// meter then reading the step in window 10.
#[test]
fn a_pre_strip_meter_reads_the_input_as_late_as_its_delay() {
    let mut config = delayed_tone_config();
    config.inputs[0].pre_strip_meter = true;
    let (n, step): (u32, u64) = (7200, 10 * 4800);
    let level = |level_db| ToneParams {
        level_db,
        ..ToneParams::DEFAULT
    };
    let tone = |params| Change::Tone { slot: 0, params };
    let on = Change::PreStripMeter {
        slot: 0,
        enabled: true,
    };
    let changes = vec![
        (0, on),
        (
            0,
            Change::Delay {
                slot: 0,
                samples: n,
            },
        ),
        (0, tone(level(-40.0))),
        (step, tone(level(-6.0))),
    ];
    let blocks = 15 * 4800 / u64::from(config.block_size);
    let (_, _, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
    // The largest magnitude of the tone delayed by `n` over window `w`.
    let peak = |w: u64| {
        let p = ToneParams::DEFAULT;
        (w * 4800 - u64::from(n)..(w + 1) * 4800 - u64::from(n))
            .map(|i| {
                let db = if i < step { -40.0 } else { -6.0 };
                let x = samplez_dsp::tone::sine(i, p.frequency_hz, 48_000);
                (samplez_dsp::tone::level_gain(db) * x).abs()
            })
            .fold(0.0f32, f32::max)
    };
    assert!(peak(11) > 10.0 * peak(10));
    for w in 2..14 {
        let (peaks, want) = (&windows[&w], peak(w));
        assert!(
            peaks.iter().all(|p| (p - want).abs() < 1e-6),
            "window {w}: {peaks:?}, {want}"
        );
    }
}

/// An input delayed by 1000 samples from index 0 and trimmed by 20 dB
/// in its strip from index X: the trim applies at X to the delayed
/// tone there, the input's sample at X - 1000, as every tap of the
/// input is as late as its delay. Before X each leg is the tone 1000
/// samples late, bit for bit; from X on, 20 dB down. Fails with the
/// delay run after the strip in `Slot::finish`, the legs then stepping
/// at X + 1000.
#[test]
fn a_strip_change_applies_to_the_delayed_input_at_its_index() {
    let config = delayed_tone_config();
    let b = u64::from(config.block_size);
    let (n, x) = (1000, 200 * b);
    let trim = StripParam::Trim { db: -20.0 };
    let changes = vec![
        (
            0,
            Change::Delay {
                slot: 0,
                samples: n as u32,
            },
        ),
        (
            x,
            Change::Strip {
                slot: 0,
                param: trim,
            },
        ),
    ];
    let blocks = (x + n) / b + 100;
    let (_, legs, _) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
    for (k, block) in legs.iter().enumerate() {
        for (c, leg) in block.iter().enumerate() {
            for (j, &y) in leg.iter().enumerate() {
                let i = k as u64 * b + j as u64;
                let want = default_tone(i as i64 - n as i64);
                if i < x {
                    assert_eq!(y.to_bits(), want.to_bits(), "leg {c} at {i}");
                } else {
                    assert!((y - 0.1 * want).abs() < 1e-6, "leg {c} at {i}: {y}");
                }
            }
        }
    }
}

/// An input with a delay line and neither `preStripMeter` nor
/// `preStripRows` has no pre-strip meter, rows or copy: the matrix has
/// the input's rows alone, and its slot copies nothing into `prebuf`,
/// which stays silent through a delay and a trim. Fails with the copy
/// into `prebuf` made whatever [`Slot::pre_strip_on`] says.
#[test]
fn without_the_opt_ins_an_input_has_no_pre_strip_copy() {
    let config = delayed_tone_config();
    assert_eq!(config.rows(), config.input_channels());
    let changes = vec![
        (
            0,
            Change::Delay {
                slot: 0,
                samples: 1000,
            },
        ),
        (
            4800,
            Change::Strip {
                slot: 0,
                param: StripParam::Trim { db: -20.0 },
            },
        ),
    ];
    let (rt, legs, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, 300);
    assert!(!rt.slots[0].pre_strip && rt.slots[0].pre_row.is_none());
    assert!(rt.pre_taps.is_empty() && windows.is_empty());
    assert!(legs.last().unwrap()[0].iter().any(|&y| y != 0.0));
    for c in 0..rt.prebuf.channels() {
        assert!(
            rt.prebuf.channel(c).iter().all(|&y| y == 0.0),
            "channel {c}"
        );
    }
}

/// An input configured with `preStripMeter` and no `PreStripMeter`
/// entry has its pre-strip meter off: its slot copies nothing into
/// `prebuf` and the meter sends no record. Fails with the slot's
/// meter on from the start in `new_slots`, with the copy into
/// `prebuf` made for every slot with the capability, and with the
/// pre-strip meters run whether on or not.
#[test]
fn a_pre_strip_meter_is_off_until_switched_on() {
    let mut config = delayed_tone_config();
    config.inputs[0].pre_strip_meter = true;
    let (rt, legs, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], Vec::new(), 300);
    assert!(rt.slots[0].pre_strip && !rt.slots[0].pre_strip_on);
    assert_eq!(rt.pre_taps.len(), 1);
    assert!(windows.is_empty(), "{windows:?}");
    assert!(legs.last().unwrap()[0].iter().any(|&y| y != 0.0));
    for c in 0..rt.prebuf.channels() {
        assert!(
            rt.prebuf.channel(c).iter().all(|&y| y == 0.0),
            "channel {c}"
        );
    }
}

/// A pre-strip meter switched on at window 2, off inside window 5 and
/// on again at window 8 reads windows 2 to 4 and from 8 on, none of
/// 5 to 7, each at the tone's level then, bit for bit as metered
/// whole: the tone at -6 dB before the stop, at -30 dB from window 6
/// on. Switched on again, the meter starts afresh, so window 8 reads
/// -30 dB alone and not the part of window 5 metered before the stop.
/// Fails with the `restart` dropped from `Rt::set_pre_strip_meter`,
/// window 8 then reading -6 dB, and with the meter left running when
/// switched off.
#[test]
fn a_pre_strip_meter_meters_exactly_where_it_is_on() {
    let mut config = delayed_tone_config();
    config.inputs[0].pre_strip_meter = true;
    let w = 4800u64;
    let set = |enabled| Change::PreStripMeter { slot: 0, enabled };
    let level = |level_db| Change::Tone {
        slot: 0,
        params: ToneParams {
            level_db,
            ..ToneParams::DEFAULT
        },
    };
    let changes = vec![
        (0, level(-6.0)),
        (2 * w, set(true)),
        (5 * w + 960, set(false)),
        (6 * w, level(-30.0)),
        (8 * w, set(true)),
    ];
    let blocks = 12 * w / u64::from(config.block_size);
    let (_, _, windows) = delayed_tone(&config, &[(0, 0), (1, 1)], changes, blocks);
    let metered: Vec<u64> = windows.keys().copied().collect();
    assert_eq!(metered, [2, 3, 4, 8, 9, 10, 11]);
    let peak = |w: u64, db: f32| {
        let p = ToneParams::DEFAULT;
        (w * 4800..(w + 1) * 4800)
            .map(|i| {
                let x = samplez_dsp::tone::sine(i, p.frequency_hz, 48_000);
                (samplez_dsp::tone::level_gain(db) * x).abs()
            })
            .fold(0.0f32, f32::max)
    };
    for (&w, peaks) in &windows {
        let want = peak(w, if w < 6 { -6.0 } else { -30.0 });
        assert!(
            peaks.iter().all(|&p| p == want),
            "window {w}: {peaks:?} {want}"
        );
    }
}

/// Tone parameters are checked against the configuration: a slot
/// that reads flows, no slot, and a frequency or level out of range
/// are refused, and a tone generator's slot reads no flow. Fails with
/// `params.check()` replaced by `Ok(())` in `check_tone`.
#[test]
fn tone_parameters_are_checked_against_the_configuration() {
    let mut config = tone_config();
    let flow: crate::config::Input = serde_json::from_value(serde_json::json!({
        "name": "in", "domain": "d",
        "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d21", "channels": 1
    }))
    .unwrap();
    config.inputs.push(flow);
    let ok = ToneParams::DEFAULT;
    assert_eq!(check_tone(&config, 0, &ok), Ok(()));
    let refused = |slot, p: ToneParams| check_tone(&config, slot, &p).unwrap_err();
    assert!(refused(1, ok).contains("no tone generator"));
    assert!(refused(2, ok).contains("no input slot 2"));
    for p in [
        ToneParams {
            frequency_hz: 19,
            ..ok
        },
        ToneParams {
            frequency_hz: 20_001,
            ..ok
        },
        ToneParams {
            level_db: 0.5,
            ..ok
        },
        ToneParams {
            level_db: -60.5,
            ..ok
        },
        ToneParams {
            level_db: f32::NAN,
            ..ok
        },
    ] {
        assert!(refused(0, p).contains("outside"), "{p:?}");
    }
    let e = config.check_input_flow(0, "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d22");
    assert!(e.unwrap_err().0.contains("tone generator"));
    assert!(config.check_flow_slot(1).is_ok());
}

/// Without a core a strip entry whose index passed still reaches its
/// slot's strip, which the slot keeps across the drop: a core built
/// from product state holds no strip, so the entry would be lost.
#[test]
fn a_strip_entry_due_without_a_core_reaches_the_strip() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("no-core-strip", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d09", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d0a", "label": "strip",
                      "group": "strip", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let param = StripParam::Trim { db: -6.0 };
    let entry = |id, at_index, change| RtEntry {
        id,
        at_index,
        change,
    };
    assert!(rt.mix.push(entry(1, 48, Change::Strip { slot: 0, param })));
    assert!(rt.mix.push(entry(2, 96, Change::InputDown { slot: 0 })));
    assert!(rt.mix.push(entry(3, 960, Change::InputDown { slot: 0 })));
    rt.pass_entries(2);
    let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
    assert_eq!(ids, [3]);
    assert_eq!(rt.mix.last_taken, 2);
    assert!(rt.slots[0].up, "an entry other than a strip entry applied");
    let mut reference = samplez_dsp::Strip::new(1, config.ramp_samples);
    assert!(reference.set(param));
    assert_eq!(rt.slots[0].strip.as_ref(), Some(&reference));
    assert_eq!(rt.slots[0].strip_pos, Some(48));
    assert_eq!(rt.counters.entries_without_core.load(Relaxed), 2);

    // A strip entry the core knew, pending here, whose index the core
    // passed: a product core holds no strip, so it applies at the
    // install.
    let gain = StripParam::Trim { db: -3.0 };
    assert!(rt.mix.push(entry(
        4,
        192,
        Change::Strip {
            slot: 0,
            param: gain
        }
    )));
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    rt.install_core(&mut core, 4, false);
    assert!(rt.mix.pending.is_empty());
    assert_eq!(rt.slots[0].strip_pos, Some(192));
    let mut set = samplez_dsp::Strip::new(1, config.ramp_samples);
    assert!(set.set(param));
    set.advance(192 - 48);
    assert!(set.set(gain));
    assert_eq!(rt.slots[0].strip.as_ref(), Some(&set));
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Without a core an output delay entry whose index passed still
/// reaches the output's line, which the output keeps across the drop,
/// as a slot's delay entry reaches the slot's; one the core knew whose
/// index it passed applies at the install of a core built from product
/// state, which holds no line, and waits for the crossfade the first
/// started.
#[test]
fn an_output_delay_entry_due_without_a_core_reaches_the_line() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("no-core-output-delay", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d15", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d16", "label": "delay",
                      "group": "delay", "role": "Audio", "channels": 1,
                      "maxDelaySamples": 4800 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let entry = |id, at_index, change| RtEntry {
        id,
        at_index,
        change,
    };
    let out = |samples| Change::OutputDelay { output: 0, samples };
    assert!(rt.mix.push(entry(1, 48, out(960))));
    assert!(rt.mix.push(entry(2, 960, Change::InputDown { slot: 0 })));
    rt.pass_entries(2);
    let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
    assert_eq!(ids, [2]);
    let taps = |rt: &Rt| rt.outs[0].delay.as_ref().map(Delay::taps);
    let fading = samplez_dsp::Taps {
        from: 0,
        to: 960,
        start: 48,
        next: None,
    };
    assert_eq!(taps(&rt), Some(fading));

    assert!(rt.mix.push(entry(3, 192, out(480))));
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    rt.install_core(&mut core, 3, false);
    assert!(rt.mix.pending.is_empty());
    let waiting = samplez_dsp::Taps {
        next: Some(480),
        ..fading
    };
    assert_eq!(taps(&rt), Some(waiting));
    assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Without a core an output strip entry whose index passed still
/// reaches the output's strip, which the output keeps across the drop,
/// as a slot's strip entry reaches the slot's; one the core knew whose
/// index it passed applies at the install of a core built from product
/// state, which holds no strip.
#[test]
fn an_output_strip_entry_due_without_a_core_reaches_the_strip() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("no-core-output-strip", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d13", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d14", "label": "strip",
                      "group": "strip", "role": "Audio", "channels": 1, "strip": true }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let param = StripParam::Trim { db: -6.0 };
    let entry = |id, at_index, change| RtEntry {
        id,
        at_index,
        change,
    };
    let out = |param| Change::OutputStrip { output: 0, param };
    assert!(rt.mix.push(entry(1, 48, out(param))));
    assert!(rt.mix.push(entry(2, 960, Change::InputDown { slot: 0 })));
    rt.pass_entries(2);
    let ids: Vec<LogId> = rt.mix.pending.iter().map(|e| e.id).collect();
    assert_eq!(ids, [2]);
    let mut reference = Strip::new(1, config.ramp_samples);
    assert!(reference.set(param));
    assert_eq!(rt.outs[0].strip.as_ref(), Some(&reference));
    assert_eq!(rt.outs[0].strip_pos, Some(48));

    let gain = StripParam::Trim { db: -3.0 };
    assert!(rt.mix.push(entry(3, 192, out(gain))));
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    rt.install_core(&mut core, 3, false);
    assert!(rt.mix.pending.is_empty());
    let mut set = Strip::new(1, config.ramp_samples);
    assert!(set.set(param));
    set.advance(192 - 48);
    assert!(set.set(gain));
    assert_eq!(rt.outs[0].strip.as_ref(), Some(&set));
    assert_eq!(rt.outs[0].strip_pos, Some(192));
    assert_eq!(rt.counters.strip_refused.load(Relaxed), 0);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A product core that knew less than an earlier replayed one: a
/// strip entry the replayed core's strips hold, delivered by the ring
/// only after both, is not applied again, which would restart its
/// ramp; a gain entry the product core did not know is taken as new,
/// not skipped as known, which would lose it.
#[test]
fn a_product_core_knowing_less_than_a_replayed_one_loses_and_repeats_nothing() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("product-below-replay", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d11", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d12", "label": "below",
                      "group": "below", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let (mut ring, entries) = rtrb::RingBuffer::new(4);
    rt.entries = entries;
    let hpf = StripParam::Hpf {
        enabled: true,
        freq: 200.0,
    };
    let strip = RtEntry {
        id: 3,
        at_index: 48,
        change: Change::Strip {
            slot: 0,
            param: hpf,
        },
    };
    let gain = RtEntry {
        id: 2,
        at_index: 1440,
        change: Change::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: None,
        },
    };
    // The replayed core knew entries up to 3 and applied the strip
    // entry at 48; its strip, 432 samples into the ramp, is installed.
    let mut replayed = samplez_dsp::Strip::new(1, config.ramp_samples);
    assert!(replayed.set(hpf));
    replayed.advance(480 - 48);
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    core.slots[0].strip = Some(replayed.clone());
    core.slots[0].strip_pos = Some(480);
    rt.install_core(&mut core, 3, true);
    assert_eq!(rt.slots[0].strip.as_ref(), Some(&replayed));
    // A product core that knew entries up to 1 only.
    rt.capture(&mut core);
    core.pos = Some(960);
    core.pending.clear();
    rt.install_core(&mut core, 1, false);
    // The ring delivers both now.
    let one = |entry| RingEntry { entry, batch: None };
    assert!(ring.push(one(strip)).is_ok() && ring.push(one(gain)).is_ok());
    rt.take_entries();
    assert_eq!(rt.slots[0].strip.as_ref(), Some(&replayed), "applied again");
    assert!(
        rt.mix.holds(2),
        "the gain entry the product core missed is lost"
    );
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// An entry consumed through a replayed core counts as taken, whether
/// the RT thread held it pending at the install, which drops it as
/// known, or the ring delivers it after, which skips it as known: a
/// product core that knew less, installed next, would lose it from
/// the matrix for good, so the hand-over check
/// (`last_taken > last_id`) refuses that core as Missed.
#[test]
fn an_entry_taken_through_a_replayed_core_refuses_a_core_knowing_less() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("taken-through-core", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d13", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d14", "label": "taken",
                      "group": "taken", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let (mut ring, entries) = rtrb::RingBuffer::new(4);
    rt.entries = entries;
    let gain = |id, at_index| RtEntry {
        id,
        at_index,
        change: Change::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: None,
        },
    };
    // Entry 2 pending here, entry 3 still in the ring: the replayed
    // core knew both and applied them.
    assert!(rt.mix.push(gain(2, 48)));
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    rt.install_core(&mut core, 3, true);
    assert!(rt.mix.pending.is_empty());
    assert_eq!(rt.mix.last_taken, 2, "dropped at the install");
    // Delivered between the replayed install and the next one.
    assert!(
        ring.push(RingEntry {
            entry: gain(3, 96),
            batch: None
        })
        .is_ok()
    );
    rt.take_entries();
    assert!(rt.mix.pending.is_empty());
    assert_eq!(rt.mix.last_taken, 3, "skipped as known");
    // A product core that knew entries up to 1 is refused.
    assert!(rt.mix.last_taken > 1);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A strip entry the replayed strips hold, delivered by the ring after
/// the install, counts as taken: another replayed core that knew
/// less would replace the strips with ones lacking it, so the
/// hand-over check (`last_taken > last_id`) refuses that core.
#[test]
fn a_strip_entry_held_by_the_strips_refuses_a_replayed_core_knowing_less() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("strip-taken", 1);
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d15", "channels": 1 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1d16", "label": "held",
                      "group": "held", "role": "Audio", "channels": 1 }],
        "matrix": [[1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let (mut ring, entries) = rtrb::RingBuffer::new(4);
    rt.entries = entries;
    let (inputs, outputs) = (config.input_channels(), config.output_channels());
    let mut core = Box::new(Core::new(inputs, outputs, 1, 64, 1));
    rt.capture(&mut core);
    core.pos = Some(480);
    core.pending.clear();
    rt.install_core(&mut core, 3, true);
    // Strip entry 3, applied by the replay, delivered after the install.
    let strip = RtEntry {
        id: 3,
        at_index: 48,
        change: Change::Strip {
            slot: 0,
            param: StripParam::Trim { db: -6.0 },
        },
    };
    assert!(
        ring.push(RingEntry {
            entry: strip,
            batch: None
        })
        .is_ok()
    );
    rt.take_entries();
    assert!(rt.mix.pending.is_empty());
    assert_eq!(rt.mix.last_taken, 3, "skipped as held by the strips");
    // A replayed core that knew entries up to 2 is refused.
    assert!(rt.mix.last_taken > 2);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A replay RT thread of one input of two legs and one mono output,
/// on flows named by `tag`, taking entries from the returned ring.
fn rt_of(e: &Engine, domain: &std::path::Path, tag: &str) -> (Rt, rtrb::Producer<RingEntry>) {
    let v = serde_json::json!({
        "libmxl": std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned()),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "in", "domain": "d",
                     "flowId": format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c{tag}"),
                     "channels": 2 }],
        "outputs": [{ "name": "out", "domain": "d",
                      "flowId": format!("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6d{tag}"),
                      "label": tag, "group": tag, "role": "Audio", "channels": 1 }],
        "matrix": [[1.0], [1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &e.api, vec![None], hashes, control);
    let (ring, entries) = rtrb::RingBuffer::new(PENDING_CHANGES);
    rt.entries = entries;
    (rt, ring)
}

/// A loop for the skip oracle ([`slice_flags`]): its slots and their
/// channels, those starting detached, its outputs' and configured
/// meters' channels, the coefficients set at the start as (row,
/// column, gain), and its entries as (block, items), one item an
/// entry of its own and more a folded batch, with the block a late
/// one is pushed at, if any.
struct Scene {
    slots: Vec<u32>,
    detached: Vec<usize>,
    outputs: Vec<u32>,
    meters: Vec<u32>,
    gains: Vec<(usize, usize, f32)>,
    entries: Vec<(u64, Vec<Item>)>,
    late: Option<(u64, u64, Change)>,
    blocks: u64,
}

/// The flags of each slice of each meter, by (meter, slice), as `s`'s
/// loop sends them at a block of 64 samples, which does not divide a
/// slice, with the blocks of `skipped` not computed, and held
/// diverged as it computes the block after them when `diverged`.
fn slice_flags(
    s: &Scene,
    skipped: std::ops::Range<u64>,
    diverged: bool,
) -> std::collections::BTreeMap<(u32, u64), u8> {
    (slice_records(s, skipped, diverged).into_iter())
        .map(|(k, (flags, _))| (k, flags))
        .collect()
}

/// [`slice_flags`] with the reset each slice carries.
#[allow(clippy::type_complexity)]
fn slice_records(
    s: &Scene,
    skipped: std::ops::Range<u64>,
    diverged: bool,
) -> std::collections::BTreeMap<(u32, u64), (u8, Option<u64>)> {
    use crate::rt_loop::{Metered, SPAN};
    const B: u64 = 64;
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let rows: u32 = s.slots.iter().sum();
    let columns: u32 = s.outputs.iter().chain(&s.meters).sum();
    let v = serde_json::json!({
        "libmxl": libmxl,
        "blockSize": B,
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": s.slots.iter().enumerate().map(|(i, &ch)| {
            let mut v = serde_json::json!({ "name": format!("in{i}"), "domain": "d",
                "channels": ch });
            if !s.detached.contains(&i) {
                v["flowId"] = format!("5fbec3b1-1b0f-417d-9059-00000000000{i}").into();
            }
            v
        }).collect::<Vec<_>>(),
        "outputs": s.outputs.iter().enumerate().map(|(o, &ch)| serde_json::json!({
            "name": format!("out{o}"), "domain": "d",
            "flowId": format!("6fbec3b1-1b0f-417d-9059-00000000000{o}"), "label": "o",
            "group": "g", "role": "Audio", "channels": ch }))
            .collect::<Vec<_>>(),
        "meters": s.meters.iter().enumerate().map(|(m, &ch)| serde_json::json!({
            "name": format!("m{m}"), "channels": ch }))
            .collect::<Vec<_>>(),
        "matrix": vec![vec![0.0; columns as usize]; rows as usize],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let api = samplez_mxl::load(std::path::Path::new(&libmxl)).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt = replay_rt(&config, &api, vec![None], hashes, control);
    // A replay loop meters its outputs only: the configured meters
    // are added as an engine's loop holds them.
    let mut first: usize = s.outputs.iter().map(|&c| c as usize).sum();
    for &ch in &s.meters {
        rt.buses.push(Metered {
            first_channel: first,
            meter: crate::rt_loop::OutMeter::new(ch as usize),
        });
        first += ch as usize;
    }
    for &(row, column, gain) in &s.gains {
        assert!(rt.mix.matrix.set(row, column, gain, 0));
    }
    let (tx, mut rx) = rtrb::RingBuffer::new(1 << 14);
    rt.meters = tx;
    let coefficients = (rows * columns) as usize;
    for (n, (k, items)) in s.entries.iter().enumerate() {
        let entry = |change| RtEntry {
            id: n as u64 + 1,
            at_index: k * B,
            change,
        };
        match items[..] {
            [Item::Change(change)] => assert!(rt.mix.push(entry(change))),
            _ => {
                let (change, batch) = batch::fold(items, columns as usize, coefficients)
                    .unwrap()
                    .unwrap();
                assert!(rt.mix.push_with(entry(change), batch).is_ok());
            }
        }
    }
    let shared = Arc::clone(&rt.replica.as_ref().unwrap().shared);
    for k in 0..s.blocks {
        if let Some((at, block, change)) = s.late.filter(|l| l.0 == k) {
            let id = s.entries.len() as u64 + 1;
            assert!(rt.mix.push(RtEntry {
                id,
                at_index: block * B,
                change,
            }));
            let _ = at;
        }
        if skipped.contains(&k) {
            continue;
        }
        let held = diverged && k == skipped.end;
        shared.diverged.store(held, Relaxed);
        rt.process(k, u64::MAX);
        shared.diverged.store(false, Relaxed);
    }
    let mut records = std::collections::BTreeMap::new();
    while let Ok(r) = rx.pop() {
        if r.flags & SPAN != 0 {
            for slice in r.slice..r.slice + u64::from(r.frames) {
                // A span's reset is its first slice's.
                let reset = r.reset().filter(|_| slice == r.slice);
                records.insert((r.meter, slice), (r.flags & !SPAN, reset));
            }
        } else if r.channel == 0 {
            records.insert((r.meter, r.slice), (r.flags, r.reset()));
        }
    }
    records
}

/// The gap flags of `flags`.
fn gaps_of(
    flags: &std::collections::BTreeMap<(u32, u64), u8>,
) -> std::collections::BTreeMap<(u32, u64), bool> {
    use crate::rt_loop::GAP;
    flags.iter().map(|(&k, &f)| (k, f & GAP != 0)).collect()
}

/// Three mono slots, slots 0 and 1 into output 0 and slot 2 into
/// output 1, over 80 blocks. Entries: slot 0 down at block 12, its
/// gain to output 0 to 0 at 25, a batch taking slot 2 down with a
/// gain at 35, slot 2 detached at 50; and, as block 70 is due, an
/// `InputDown` of slot 1 at block 4, an index already produced, which
/// is late and applies nowhere.
fn base_scene() -> Scene {
    let gain = |input, output, gain| {
        Item::Change(Change::Gain {
            input,
            output,
            gain,
            fade: None,
        })
    };
    let down = |slot| Item::Change(Change::InputDown { slot });
    Scene {
        slots: vec![1, 1, 1],
        detached: Vec::new(),
        outputs: vec![1, 1],
        meters: Vec::new(),
        gains: vec![(0, 0, 1.0), (1, 0, 1.0), (2, 1, 1.0)],
        entries: vec![
            (12, vec![down(0)]),
            (25, vec![gain(0, 0, 0.0)]),
            (35, vec![down(2), gain(2, 1, 0.5)]),
            (50, vec![Item::Change(Change::Detach { slot: 2 })]),
        ],
        late: Some((70, 4, Change::InputDown { slot: 1 })),
        blocks: 80,
    }
}

/// A stereo slot 0 and mono slots 1 to 3, slot 3 starting detached,
/// into a stereo output 0 (columns 0 and 1), a mono output 1 (column
/// 2) and a mono configured meter (column 3), over 90 blocks:
/// - block 3: slots 0 and 1 down, two entries at one index; slot 0
///   feeds output 0, so a skip from here or later starts in a gap;
/// - 5: a gain of slot 0 into output 0, at the start of a skip there;
/// - 10: slot 1's gain to the meter from 0 to 0.5, while it is down;
/// - 20: slot 0 up;
/// - 30: a batch raising slot 1 with a gain;
/// - 40: slot 2 down, feeding output 1;
/// - 45: slot 2's gain to 0 by a fade delayed 300 samples;
/// - 52: slot 3 down, detached; 55: slot 3 re-attached by a `Route`;
/// - 58: slot 2's gain back to 0.7 by an absolute fade from 0;
/// - 60: slot 3 up and slot 0 down again, at one index;
/// - 70: slot 1 down, at the end of a skip there.
///
/// Output 0 and output 1 are in a gap as a skip up to block 70 ends.
fn moving_scene() -> Scene {
    let b = 64;
    let gain = |input, output, gain, fade| {
        Item::Change(Change::Gain {
            input,
            output,
            gain,
            fade,
        })
    };
    let down = |slot| Item::Change(Change::InputDown { slot });
    let up = |slot| Item::Change(Change::InputUp { slot });
    Scene {
        slots: vec![2, 1, 1, 1],
        detached: vec![3],
        outputs: vec![2, 1],
        meters: vec![1],
        gains: vec![(1, 1, 1.0), (3, 2, 1.0), (4, 3, 1.0)],
        entries: vec![
            (3, vec![down(0)]),
            (3, vec![down(1)]),
            (5, vec![gain(0, 0, 0.5, None)]),
            (10, vec![gain(2, 3, 0.5, None)]),
            (20, vec![up(0)]),
            (30, vec![up(1), gain(2, 3, 0.25, None)]),
            (40, vec![down(2)]),
            (
                45,
                vec![gain(3, 2, 0.0, Some(Fade::after(1.0, 45 * b, 300, 200)))],
            ),
            (52, vec![down(3)]),
            (55, vec![Item::Change(Change::Route { slot: 3 })]),
            (
                58,
                vec![gain(3, 2, 0.7, Some(Fade::after(0.0, 58 * b, 0, 400)))],
            ),
            (60, vec![up(3)]),
            (60, vec![down(0)]),
            (70, vec![down(1)]),
        ],
        late: None,
        blocks: 90,
    }
}

/// Slices an overrun skipped carry the gap flags the same slices carry
/// on a loop that computed them: over an `InputDown`, a gain set to 0,
/// a batch taking a slot down and a `Detach` inside the skip, a late
/// entry that applies on neither, and a block size that does not
/// divide a slice.
#[test]
fn skipped_slices_carry_the_gaps_computed_ones_do() {
    let s = base_scene();
    let computed = gaps_of(&slice_flags(&s, 0..0, false));
    let skipped = gaps_of(&slice_flags(&s, 5..70, false));
    // The run up to block 80 covers slices 0 to 10.
    assert!(computed.len() >= 20, "{computed:?}");
    assert_eq!(skipped, computed);
    let at = |m: u32, k: u64| computed[&(m, k * 64 / SLICE)];
    assert!(
        at(0, 15) && !at(0, 30),
        "output 0: slot 0 down, then its gain 0"
    );
    assert!(
        !at(1, 25) && at(1, 40) && !at(1, 60),
        "output 1: slot 2 down, detached"
    );
}

/// The same over gains from 0 to other than 0 on a slot down, an
/// `InputUp` and a batch raising a slot, a `Route` re-attaching a
/// detached slot, absolute fades, delayed and not, a configured meter
/// and a stereo slot and output, skips starting and ending in a gap,
/// entries at a skip's first and end index and two at one index.
#[test]
fn skipped_slices_carry_the_gaps_computed_ones_do_as_targets_move() {
    let s = moving_scene();
    let computed = gaps_of(&slice_flags(&s, 0..0, false));
    let at = |m: u32, k: u64| computed[&(m, k * 64 / SLICE)];
    assert!(at(0, 8) && !at(0, 25) && at(0, 65), "output 0");
    assert!(
        !at(1, 30) && at(1, 42) && !at(1, 47) && at(1, 65),
        "output 1"
    );
    assert!(
        !at(2, 5) && at(2, 15) && !at(2, 35) && at(2, 57) && !at(2, 65),
        "meter"
    );
    for skip in [5..70, 3..70, 11..62, 1..89] {
        assert_eq!(
            gaps_of(&slice_flags(&s, skip.clone(), false)),
            computed,
            "{skip:?}"
        );
    }
}

/// A loop held diverged as it computes the block after a skip sends
/// the skipped slices diverged, spans included, with the gap flags of
/// the slices computed: diverged wins over gap in the meters.
#[test]
fn skipped_slices_carry_the_divergence_of_the_block_after() {
    use crate::rt_loop::DIVERGED;
    let s = moving_scene();
    let computed = gaps_of(&slice_flags(&s, 0..0, false));
    let flags = slice_flags(&s, 5..70, true);
    assert_eq!(gaps_of(&flags), computed);
    for (&(m, slice), &f) in &flags {
        let within = (slice + 1) * SLICE > 5 * 64 && slice * SLICE < 71 * 64;
        assert_eq!(f & DIVERGED != 0, within, "meter {m} slice {slice}");
    }
}

/// A mono slot into a mono output (meter 0) and a mono slot into a
/// mono configured meter (meter 1), over 240 blocks of 64 samples, a
/// slice 7.5 blocks and a window 75:
/// - block 4: meter 0 paused, inside slice 0;
/// - 15: meter 0 run at the start of slice 2, and meter 1 reset there,
///   at window 1 (block 75);
/// - 26: a batch pausing both meters and resetting meter 0 at window
///   1, inside slice 3;
/// - 30: meter 0 run at the start of slice 4; 45: meter 1 run at the
///   start of slice 6;
/// - 76: meter 0 paused inside slice 10, and 80: reset, at window 2
///   (block 150); 90: run at the start of slice 12;
/// - 120: meter 1 reset at window 2; 155: a batch resetting meter 1 at
///   window 3 (block 225) and pausing it, inside slice 20.
fn loudness_scene() -> Scene {
    let run = |meter, running| Item::Change(Change::LoudnessRun { meter, running });
    let reset = |meter| Item::Change(Change::ResetLoudness { meter });
    Scene {
        slots: vec![1, 1],
        detached: Vec::new(),
        outputs: vec![1],
        meters: vec![1],
        gains: vec![(0, 0, 1.0), (1, 1, 1.0)],
        entries: vec![
            (4, vec![run(0, false)]),
            (15, vec![run(0, true)]),
            (15, vec![reset(1)]),
            (26, vec![run(1, false), reset(0), run(0, false)]),
            (30, vec![run(0, true)]),
            (45, vec![run(1, true)]),
            (76, vec![run(0, false)]),
            (80, vec![reset(0)]),
            (90, vec![run(0, true)]),
            (120, vec![reset(1)]),
            (155, vec![reset(1), run(1, false)]),
        ],
        late: None,
        blocks: 240,
    }
}

/// A slice carries the paused flag when a sample of it was metered
/// with its meter paused by the log, whether the `LoudnessRun` lies
/// inside it or at its start, and the reset of a `ResetLoudness` at the
/// first window at or after its index, alone or in a batch. Skipped
/// slices carry the same flags and resets as computed ones, over
/// skips holding such entries, starting or ending at them, and ending
/// inside a slice or before a reset's window.
#[test]
fn skipped_slices_carry_the_pauses_and_resets_computed_ones_do() {
    use crate::rt_loop::PAUSED;
    let s = loudness_scene();
    let computed = slice_records(&s, 0..0, false);
    let paused = |m: u32, slices: std::ops::Range<u64>| -> Vec<bool> {
        slices.map(|i| computed[&(m, i)].0 & PAUSED != 0).collect()
    };
    assert_eq!(paused(0, 0..6), [true, true, false, true, false, false]);
    assert_eq!(paused(0, 9..13), [false, true, true, false]);
    assert_eq!(paused(1, 2..7), [false, true, true, true, false], "meter 1");
    // Paused from slice 20 to the end.
    let mut tail = vec![true; 13];
    tail[0] = false;
    assert_eq!(paused(1, 19..32), tail);
    let resets: Vec<(u32, u64, u64)> = (computed.iter())
        .filter_map(|(&(m, i), &(_, r))| Some((m, i, r?)))
        .collect();
    assert_eq!(
        resets,
        [
            (0, 10, 4800),
            (0, 20, 9600),
            (1, 10, 4800),
            (1, 20, 9600),
            (1, 30, 14_400)
        ]
    );
    for skip in [
        5..70,
        3..80,
        15..90,
        4..15,
        26..30,
        76..90,
        60..156,
        152..226,
        1..239,
    ] {
        assert_eq!(slice_records(&s, skip.clone(), false), computed, "{skip:?}");
    }
}

/// A batch of two gains, a trim and an `InputDown` of slot 0, and
/// with `up` an `InputUp` of slot 0 after them.
fn batch_of(up: bool) -> (Change, Option<Arc<Batch>>) {
    let mut changes = vec![
        Change::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: None,
        },
        Change::Gain {
            input: 1,
            output: 0,
            gain: 0.25,
            fade: None,
        },
        Change::InputDown { slot: 0 },
        Change::Strip {
            slot: 0,
            param: StripParam::Trim { db: -6.0 },
        },
    ];
    if up {
        changes.push(Change::InputUp { slot: 0 });
    }
    let items: Vec<Item> = changes.into_iter().map(Item::Change).collect();
    batch::fold(&items, 1, 2).unwrap().unwrap()
}

/// An entry the pending list holds already is taken off the ring
/// while the list is full: it needs no room, and the entries behind it
/// would otherwise wait until the list drains, and turn late.
#[test]
fn a_held_entry_is_taken_off_a_full_ring() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("held-first", 1);
    let (mut rt, mut ring) = rt_of(&e, &domain, "1d41");
    let entry = |id| RtEntry {
        id,
        at_index: 1 << 30,
        change: Change::InputDown { slot: 0 },
    };
    for id in 1..=PENDING_CHANGES as u64 {
        assert!(rt.mix.push(entry(id)));
    }
    let (change, batch) = batch_of(false);
    let held = RingEntry {
        entry: RtEntry { change, ..entry(3) },
        batch,
    };
    let new = RingEntry {
        entry: entry(65),
        batch: None,
    };
    assert!(ring.push(held).is_ok() && ring.push(new).is_ok());
    rt.take_entries();
    assert_eq!((rt.taken, rt.last_entry), (1, 3), "the held entry waited");
    assert_eq!(ring.slots(), PENDING_CHANGES - 1, "the new one was taken");
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A copy shares the batches pending, and an install takes the core's
/// batch and hands the batch of an entry it drops back in the core: no
/// batch is allocated or freed on the RT thread.
#[test]
fn an_install_moves_batches_without_allocating() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("batch-install", 1);
    let (mut rt, _ring) = rt_of(&e, &domain, "1d43");
    let (change, mine) = batch_of(false);
    let entry = |id| RtEntry {
        id,
        at_index: 1440,
        change,
    };
    let mine = mine.unwrap();
    let theirs = Arc::new((*mine).clone());
    let (mine_at, theirs_at) = (Arc::as_ptr(&mine), Arc::as_ptr(&theirs));
    assert!(rt.mix.push_with(entry(3), Some(mine)).is_ok());
    let mut core = Box::new(Core::new(2, 1, 1, 64, 1));
    rt.capture(&mut core);
    assert!(
        std::ptr::eq(Arc::as_ptr(&core.batches[0].1), mine_at),
        "copied"
    );
    core.pos = Some(480);
    core.prepare();
    core.pending.clear();
    core.pending.push(entry(5));
    core.batches.push((5, theirs));
    rt.install_core(&mut core, 5, true);
    assert_eq!(rt.mix.pending, [entry(5)]);
    assert_eq!(rt.mix.batches.len(), 1);
    assert!(std::ptr::eq(Arc::as_ptr(&rt.mix.batches[0].1), theirs_at));
    assert!(core.batches.is_empty());
    assert!(
        core.handed
            .0
            .iter()
            .any(|b| std::ptr::eq(Arc::as_ptr(b), mine_at)),
        "the dropped batch did not go back with the core"
    );
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Without a core a batch whose index passed sets its strip
/// parameters on the strips the slots keep and passes the rest; one
/// holding an `InputUp` stays pending whole, as that entry does
/// alone, so its strip parameters wait with it.
#[test]
fn a_batch_due_without_a_core_passes_whole_or_waits_whole() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("batch-no-core", 1);
    let (mut rt, _ring) = rt_of(&e, &domain, "1d45");
    let (plain, plain_batch) = batch_of(false);
    let (up, up_batch) = batch_of(true);
    let at = |id, change| RtEntry {
        id,
        at_index: 48,
        change,
    };
    assert!(rt.mix.push_with(at(1, up), up_batch).is_ok());
    rt.pass_entries(2);
    assert_eq!(rt.mix.pending, [at(1, up)]);
    assert_eq!(
        rt.slots[0].strip_pos, None,
        "a strip parameter of it applied"
    );
    assert!(rt.mix.push_with(at(2, plain), plain_batch).is_ok());
    rt.pass_entries(2);
    assert_eq!(rt.mix.pending, [at(1, up)]);
    assert_eq!(rt.mix.batches.len(), 1);
    assert_eq!(rt.slots[0].strip_pos, Some(48));
    assert!(rt.slots[0].up, "an InputDown applied without a core");
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Without a core, a batch whose strip parameters the replayed strips
/// hold already, marked so when taken, passes without setting them
/// again, which would restart their ramps.
#[test]
fn a_batch_passed_without_a_core_keeps_strips_the_core_brought() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("batch-held-strips", 1);
    let (mut rt, _ring) = rt_of(&e, &domain, "1d49");
    let (change, batch) = batch_of(false);
    let Change::Batch { up, routed, .. } = change else {
        panic!("{change:?}")
    };
    let held = RtEntry {
        id: 1,
        at_index: 48,
        change: Change::Batch {
            strips: false,
            up,
            routed,
            detached: 0,
            resets: false,
        },
    };
    assert!(rt.mix.push_with(held, batch).is_ok());
    rt.pass_entries(2);
    assert!(rt.mix.pending.is_empty());
    assert_eq!(rt.slots[0].strip_pos, None, "a strip parameter set again");
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// Spent batches leave the return ring's reserve free, so a
/// generation swapped in hands the old one back, not leak it: past the
/// reserve they wait on the spent list, beyond it they are leaked and
/// counted, and they leave through the ring once it has room.
#[test]
fn spent_batches_leave_room_for_a_generation_handed_back() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("batch-spent", 1);
    let (mut rt, _ring) = rt_of(&e, &domain, "1d47");
    rt.replay = false;
    let (ret, mut back) = rtrb::RingBuffer::new(8);
    rt.ret = ret;
    rt.spent = Vec::with_capacity(2);
    let generation = || {
        Box::new(Gen {
            mxl: Generation::new(&e.api, &domain, 1).unwrap(),
            prune_after: vec![u64::MAX],
            prune_late: vec![u64::MAX],
            rerouted: Default::default(),
        })
    };
    rt.install(0, generation());
    for _ in 0..10 {
        rt.retire(batch_of(false).1);
    }
    // A reserve of one domain, one output and one more.
    assert_eq!((rt.ret.slots(), rt.spent.len()), (3, 2));
    assert_eq!(rt.counters.batches_leaked.load(Relaxed), 3);
    rt.install(0, generation());
    let returned: Vec<Returned> = std::iter::from_fn(|| back.pop().ok()).collect();
    let generations = returned
        .iter()
        .filter(|r| matches!(r, Returned::Generation(_)))
        .count();
    assert_eq!((returned.len(), generations), (6, 1));
    rt.take_entries();
    assert_eq!(rt.spent.len(), 0);
    assert!(matches!(back.pop(), Ok(Returned::Batch(_))));
    drop(returned);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// No engine entries append nothing, wherever the index: the log id
/// stays free, and an empty translation is refused by no replica.
#[test]
fn a_log_entry_of_no_engine_entries_appends_nothing() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("batch-empty", 1);
    assert_eq!(e.append_batch(5, 7, &[]), Ok(()));
    assert_eq!(e.log.lock().unwrap().last, 0);
    assert_eq!(
        e.append_batch(5, 7, &[Entry::InputDown { slot: 0 }]),
        Err(EntryError::Invalid(
            "at_index 7 is not a multiple of the block size 48".to_owned()
        ))
    );
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A core whose block number leaves the index range at this block
/// size is refused, not overflowed.
#[test]
fn a_core_after_a_block_beyond_any_index_is_not_replayed() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("replay-k0", 1);
    let copy = || loop {
        match e.copy_core(Duration::from_secs(2)) {
            Err(CopyError::Passed { .. }) => continue,
            c => break c.unwrap(),
        }
    };
    for k0 in [u64::MAX, crate::transfer::INDEX_MAX] {
        let mut c = copy();
        c.k0 = k0;
        assert!(
            matches!(e.replay(c), Err(ReplayError::Invalid(m)) if m.contains("after block")),
            "a core after block {k0}"
        );
    }
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// The answer to a hand-over whose caller gave up goes to the next
/// call, which does not take it for its own: that call installs its
/// own core.
#[test]
fn a_late_answer_is_not_taken_for_the_next_handover() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("handover-late", 1);
    let copy = || loop {
        match e.copy_core(Duration::from_secs(2)) {
            Err(CopyError::Passed { .. }) => continue,
            c => break c.unwrap(),
        }
    };
    let n = |c: &AtomicU64| c.load(Relaxed);
    let processed = || e.counters.processed.load(Acquire);
    // An earlier call's hand-over, installed once that call gave up.
    let mut stale = 0;
    while n(&e.counters.handovers_installed) == 0 {
        let mut replay = e.replay(copy()).unwrap();
        let mut t = e.transfer.as_ref().unwrap().lock().unwrap();
        let (inputs, outputs, slots, page, outs) = t.shape;
        let mut core = Box::new(Core::new(inputs, outputs, slots, page, outs));
        let p = processed();
        while processed() == p {
            std::thread::sleep(Duration::from_micros(50));
        }
        replay.run(processed() - 1);
        replay.rt.capture(&mut core);
        t.seq += 1;
        stale = replay.next - 1;
        let h = Handover {
            seq: t.seq,
            j: stale,
            last_id: replay.last_id,
            core,
            replayed: true,
            strips: Vec::new(),
            limiters: Vec::new(),
            delays: Vec::new(),
            out_strips: Vec::new(),
            out_delays: Vec::new(),
            tones: Vec::new(),
            answer: Answer::Waiting,
        };
        assert!(t.handovers.push(h).is_ok());
        t.handing = true;
        let answered = n(&e.counters.handovers_passed);
        wait(&|| {
            n(&e.counters.handovers_installed) > 0 || n(&e.counters.handovers_passed) > answered
        });
        if n(&e.counters.handovers_installed) == 0 {
            while t.handed.pop().is_err() {
                std::thread::sleep(Duration::from_millis(1));
            }
            t.handing = false;
        }
    }
    let mut replay = e.replay(copy()).unwrap();
    let j = e.hand_over(&mut replay, Duration::from_secs(2)).unwrap();
    assert_ne!(j, stale, "the late answer's block returned");
    assert_eq!(n(&e.counters.handovers_installed), 2);
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A core is filled only for a block the RT thread has computed: the
/// clock runs on while the RT thread stalls, so a core for the newest
/// block due would follow a block the RT thread has not computed, and
/// install a state ahead of the one it computes from. `processed` is
/// read inside `make`, so the RT thread's steps after the stall can
/// only make it larger.
#[test]
fn a_core_is_filled_only_for_a_block_the_rt_thread_computed() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("handover-stall", 1);
    let processed = || e.counters.processed.load(Acquire);
    wait(&|| processed() > 10);
    crate::rt_loop::STALL_MS.store(300, Relaxed);
    // The RT thread is in the stall once it stops computing.
    let still = |p: u64| {
        std::thread::sleep(Duration::from_millis(20));
        processed() == p
    };
    let t = Instant::now();
    while !still(processed()) {
        assert!(t.elapsed() < Duration::from_secs(5), "no stall");
    }
    let mut calls = Vec::new();
    let _ = e.offer(
        Duration::from_secs(1),
        0,
        true,
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        |j, _| {
            calls.push((j, processed()));
            (j, 0)
        },
    );
    assert!(!calls.is_empty());
    for &(j, p) in &calls {
        assert!(j < p, "a core after block {j} with {p} computed");
    }
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A hand-over waiting for a stalled RT thread to step sleeps between
/// looks after one block period of yielding: a spin for the whole
/// timeout would spend the CPU quota the RT thread shares.
#[test]
fn a_handover_waiting_on_a_stalled_rt_thread_spends_little_cpu() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("handover-cpu", 1);
    let processed = || e.counters.processed.load(Acquire);
    wait(&|| processed() > 10);
    crate::rt_loop::STALL_MS.store(600, Relaxed);
    let t = Instant::now();
    loop {
        let p = processed();
        std::thread::sleep(Duration::from_millis(20));
        if processed() == p {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(5), "no stall");
    }
    let cpu = || {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: a valid clock id and a timespec to write.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
            0
        );
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    };
    let before = cpu();
    let err = e.offer(
        Duration::from_millis(300),
        0,
        true,
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        |j, _| (j, 0),
    );
    let spent = cpu() - before;
    assert!(matches!(err, Err(ReplayError::Timeout { .. })), "{err:?}");
    assert!(spent < Duration::from_millis(100), "{spent:?} of CPU");
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// A call that times out names how many of its hand-overs came back
/// refused for a block the RT thread passed, so a resync budget can be
/// judged against it.
#[test]
fn a_timed_out_handover_names_its_passed_refusals() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (e, _statuses, _verdicts, domain) = outputs_only("handover-timeout", 1);
    let before = e.counters.handovers_passed.load(Relaxed);
    // Each core follows a block two behind the last one computed:
    // every attempt is refused.
    let err = e.offer(
        Duration::from_millis(300),
        0,
        true,
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
        |j, _| (j.saturating_sub(2), 0),
    );
    let refused = e.counters.handovers_passed.load(Relaxed) - before;
    let Err(ReplayError::Timeout { passed }) = err else {
        panic!("{err:?}");
    };
    assert!(passed > 0);
    // The last attempt's answer may come after the call gave up.
    assert!(
        refused == passed || refused == passed + 1,
        "{refused} {passed}"
    );
    e.stop();
    let _ = std::fs::remove_dir_all(domain);
}

/// What [`lost_run`] does to replica a during the measurement.
#[derive(Clone, Copy, PartialEq)]
enum Upset {
    /// A stall that skips one block, two seconds in.
    Skip,
    /// A stall that skips blocks around the index the log takes the
    /// input down at, a second in, for half a second.
    SkipOverDown,
    /// Marked diverged for about half a second, a second in.
    Diverged,
}

/// What one [`lost_run`] measured on replica a and b.
struct Lost {
    a: OutputMeters,
    b: OutputMeters,
    /// The blocks a skipped, one block a slice.
    skipped: std::ops::Range<u64>,
    /// The blocks a computed while marked diverged, at the least and
    /// at the most.
    diverged: (u64, u64),
    /// The input down by the log over these blocks, on both.
    down: std::ops::Range<u64>,
    /// The measurement's reset and the end of its programme.
    reset: u64,
    end: u64,
}

/// Runs `lost_run` until a run is not disturbed, at most four times.
fn lost(upset: Upset) -> Lost {
    use crate::rt_loop::{STALL_LOOP, STALL_MS};
    struct Unstall;
    impl Drop for Unstall {
        fn drop(&mut self) {
            STALL_MS.store(0, Relaxed);
            STALL_LOOP.store(0, Relaxed);
        }
    }
    let _unstall = Unstall;
    let mut seen = Vec::new();
    for run in 0..4 {
        match lost_run(run, upset) {
            Ok(l) => return l,
            Err(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen:?}");
}

/// Two sines on flow `id`, 1 kHz on channel 0 and 1.5 kHz on channel
/// 1 at half scale, written a block of `block` samples at a time as
/// libmxl's clock passes it, until `stop` is set. Both hold whole
/// periods in every 10 ms slice.
fn sines(
    api: &Arc<samplez_mxl::MxlApi>,
    domain: &std::path::Path,
    id: &str,
    block: u64,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    use samplez_mxl::output::{self, Creation, OutputConfig};
    use samplez_mxl::time;
    let instance = samplez_mxl::Instance::new(api, domain).unwrap();
    let cfg = OutputConfig {
        flow_id: id,
        label: "sines",
        group: "sines",
        role: "Audio",
        channels: 2,
        block,
        batch: block,
    };
    let Creation::Created { mut writer, .. } = output::create(&instance, &cfg).unwrap() else {
        panic!("sine flow {id} not created");
    };
    let api = Arc::clone(api);
    std::thread::spawn(move || {
        let n = block as usize;
        let mut data = vec![0.0f32; 2 * n];
        let mut next: Option<u64> = None;
        while !stop.load(Relaxed) {
            let index = time::timestamp_to_index(&api, time::now(&api));
            if let Some(due) = (index / block).checked_sub(1) {
                for k in next.unwrap_or(due)..=due {
                    for (ch, out) in data.chunks_exact_mut(n).enumerate() {
                        for (i, v) in out.iter_mut().enumerate() {
                            *v = sine(ch, k * block + i as u64);
                        }
                    }
                    writer.write_block(k, &data);
                }
                next = Some(due + 1);
            }
            let k = next.unwrap_or(0);
            time::sleep_until(&api, time::index_to_timestamp(&api, (k + 1) * block));
        }
        drop(writer);
        drop(instance);
    })
}

/// Sample `index` of channel `ch` of [`sines`].
fn sine(ch: usize, index: u64) -> f32 {
    let f = 1000 + 500 * ch as u64;
    let phase = (f * (index % 48_000)) % 48_000;
    0.5 * (std::f64::consts::TAU * phase as f64 / 48_000.0).sin() as f32
}

/// Two replicas of one stereo programme measured from one reset to
/// the end of the programme, its gains ramping to 0, and on past it,
/// replica a upset by `upset` on the way. A run in which a skip or a
/// silenced input read other than the one injected moved a counter
/// is disturbed: an error.
fn lost_run(run: u32, upset: Upset) -> Result<Lost, String> {
    use crate::rt_loop::{STALL_LOOP, STALL_MS};
    use samplez_dsp::WINDOW;
    use samplez_mxl::time;
    // One block a slice, so a skipped block is one slice.
    const B: u64 = 480;
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let shm = std::path::Path::new("/dev/shm");
    let base = if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    };
    let domain = base.join(format!("samplez-lost-{}-{run}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let api = samplez_mxl::load(std::path::Path::new(&libmxl)).unwrap();
    let src = "0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e01";
    let stop = Arc::new(AtomicBool::new(false));
    let producer = sines(&api, &domain, src, B, Arc::clone(&stop));
    let start = |out: &str| {
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": B,
            "readerMarginSamples": 3 * B,
            // A stall of two and a half block periods, after a block,
            // leaves the loop one block past the next one: it skips
            // that one.
            "maxCatchupBlocks": 1,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": [{ "name": "in", "domain": "d", "flowId": src, "channels": 2 }],
            "outputs": [{ "name": "out", "domain": "d", "flowId": out, "label": "lost",
                          "group": "lost", "role": "Audio", "channels": 2 }],
            "matrix": [[1.0, 0.0], [0.0, 1.0]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).unwrap();
        let process = rt::setup_process(&config.rt);
        let (report_tx, reports) = mpsc::sync_channel(64);
        let (status_tx, changes) = mpsc::sync_channel(1 << 20);
        let replication = Replication {
            voters: 2,
            reports: report_tx,
            statuses: Some(status_tx),
            hashes: None,
        };
        let e = Engine::start_replicated(config, process, None, replication).unwrap();
        wait(&|| {
            let s = e.status();
            s.outputs[0].realised && s.inputs[0].state == InputState::Attached
        });
        (e, changes, reports)
    };
    let (a, a_changes, _ra) = start("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e02");
    let (b, b_changes, _rb) = start("0b1f6c1e-3a4e-4d0c-9d7e-5f2b8a6c1e03");
    let now = || time::timestamp_to_index(&api, time::now(&api));
    let reset = (now() + 24_000).next_multiple_of(WINDOW);
    // The programme ends at `end`, its gains ramping to 0 over one
    // slice; after it no block nor short-term value is above the
    // gates.
    let end = reset + 480_000;
    let (down, up) = if upset == Upset::SkipOverDown {
        (reset + 48_000, reset + 72_000)
    } else {
        (end, end)
    };
    for e in [&a, &b] {
        e.append(1, reset, Entry::ResetLoudness { meter: 0 })
            .unwrap();
        let mut id = 1;
        if down < up {
            e.append(2, down, Entry::InputDown { slot: 0 }).unwrap();
            e.append(3, up, Entry::InputUp { slot: 0 }).unwrap();
            id = 3;
        }
        for c in 0..2 {
            let gain = Entry::Gain {
                input: c,
                output: c,
                gain: 0.0,
                fade: None,
            };
            id += 1;
            e.append(id, end, gain).unwrap();
        }
        e.commit(id);
    }
    let silenced = |e: &Engine| e.counters().input_silenced_blocks.load(Relaxed);
    let skipped = |e: &Engine| e.counters().overrun_skipped_samples.load(Relaxed);
    let (sa, sb) = (silenced(&a), silenced(&b));
    let processed = || a.counters().processed.load(Acquire);
    let at = |index: u64| {
        while now() < index {
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    let stall = |ms: u64| {
        STALL_LOOP.store(std::ptr::from_ref(&*a.counters) as usize, Relaxed);
        STALL_MS.store(ms, Relaxed);
        wait(&|| STALL_MS.load(Relaxed) == 0);
        STALL_LOOP.store(0, Relaxed);
    };
    let mut diverged = (0, 0);
    match upset {
        Upset::Skip => {
            at(reset + 96_000);
            stall(B * 5 / 2 / 48);
        }
        Upset::SkipOverDown => {
            // About four blocks. The loop computes a block about four
            // block periods after its index (its read margin of three
            // and the block itself), so a stall from a block after the
            // input goes down skips blocks on both sides of it.
            at(down + B);
            stall(B * 11 / 2 / 48);
        }
        Upset::Diverged => {
            at(reset + 48_000);
            let first = processed();
            a.set_diverged(true).unwrap();
            let from = processed();
            at(reset + 72_000);
            let last = processed();
            a.set_diverged(false).unwrap();
            let to = processed();
            // The loop reads the flag while it meters a block and
            // counts the block processed after it, so the block in
            // flight at either store may have read the flag before
            // it. At the least the blocks processed between the two
            // stores but the first; at the most those up to the block
            // after.
            diverged = (last.saturating_sub(from + 1), to + 1 - first);
        }
    }
    let frozen = end + 3 * 48_000 + 4 * WINDOW;
    let t = Instant::now();
    while [&a, &b].iter().any(|e| {
        let w = e.status().outputs[0].meters.window;
        w.is_none_or(|w| w * WINDOW <= frozen)
    }) {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "measured past the end"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let skips = |changes: &mpsc::Receiver<StatusChange>| -> Vec<(u64, u64)> {
        changes
            .try_iter()
            .filter_map(|c| match c {
                StatusChange::Skipped { from, to } if to * B > reset => Some((from, to)),
                _ => None,
            })
            .collect()
    };
    let (ka, kb) = (skips(&a_changes), skips(&b_changes));
    let (ma, mb) = (
        a.status().outputs[0].meters.clone(),
        b.status().outputs[0].meters.clone(),
    );
    let downed = down / B..up / B;
    let expected_silenced = downed.end - downed.start;
    let injected = if upset == Upset::Diverged { 0 } else { 1 };
    // A silenced block outside the input's down span, a skip on b, or
    // other than the injected skip on a, is a measured disturbance.
    let silenced_a = silenced(&a) - sa;
    let silenced_b = silenced(&b) - sb;
    let disturbed = (silenced_b != expected_silenced || silenced_a > expected_silenced)
        .then(|| format!("input reads silenced: {silenced_a} and {silenced_b}"))
        .or_else(|| (!kb.is_empty()).then(|| format!("b skipped {kb:?}")))
        .or_else(|| (ka.len() != injected).then(|| format!("a skipped {ka:?}")))
        .or_else(|| {
            // The skip straddles the index the input goes down at, so
            // the entry applies inside it and splits its runs.
            let straddles = ka
                .first()
                .is_some_and(|k| k.0 < downed.start && downed.start < k.1);
            (upset == Upset::SkipOverDown && !straddles)
                .then(|| format!("a skipped {ka:?}, not across {}", downed.start))
        })
        .or_else(|| {
            (upset == Upset::Skip && skipped(&a) / B != ka.iter().map(|k| k.1 - k.0).sum::<u64>())
                .then(|| "a skipped more than the stall".to_owned())
        });
    stop.store(true, Relaxed);
    a.stop();
    b.stop();
    producer.join().unwrap();
    let _ = std::fs::remove_dir_all(&domain);
    if let Some(why) = disturbed {
        return Err(why);
    }
    let skipped = ka.first().map_or(0..0, |k| k.0..k.1);
    Ok(Lost {
        a: ma,
        b: mb,
        skipped,
        diverged,
        down: downed,
        reset,
        end,
    })
}

/// Per 10 ms slice, the K-weighted power of [`sine`] relative to that
/// of slice `steady`, as a meter measures it from slice `from` on:
/// with the slice at `skip` left out, its filters running on over the
/// jump, and the gains ramping to 0 over one slice from slice `end`.
fn transient(from: u64, steady: u64, skip: Option<u64>, end: u64, slices: u64) -> Vec<f64> {
    use samplez_dsp::{KWeighting, SLICE};
    let mut kw = KWeighting::new(2);
    let mut x = [vec![0.0f32; SLICE as usize], vec![0.0f32; SLICE as usize]];
    let mut powers = Vec::new();
    let mut jump = 0;
    for slice in from..from + slices {
        if Some(slice) == skip {
            jump = 1;
        }
        let mut p = 0.0;
        for (ch, x) in x.iter_mut().enumerate() {
            for (n, v) in x.iter_mut().enumerate() {
                let s = slice + jump;
                let gain = match s.cmp(&end) {
                    std::cmp::Ordering::Less => 1.0,
                    std::cmp::Ordering::Equal => 1.0 - n as f32 / SLICE as f32,
                    std::cmp::Ordering::Greater => 0.0,
                };
                *v = gain * sine(ch, s * SLICE + n as u64);
            }
            p += kw.process(ch, x) / SLICE as f64;
        }
        powers.push(p);
    }
    let steady = powers[(steady - from) as usize];
    powers.iter().map(|p| p / steady).collect()
}

/// Two replicas of one stereo programme, one of them skipping a block
/// after an injected stall: the skipped slice is lost on that replica
/// alone, both measure on, they count the same gaps (none), and their
/// integrated loudness differs by no more than the bound below.
#[test]
fn a_skip_is_lost_on_its_replica_alone_and_both_measure_on() {
    use samplez_dsp::{SLICE, WINDOW};
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let l = lost(Upset::Skip);
    let (ma, mb) = (&l.a, &l.b);
    let lost = l.skipped.end - l.skipped.start;
    assert!(lost >= 1);
    assert_eq!((ma.lost, mb.lost), (lost, 0), "{ma:?} {mb:?}");
    assert_eq!((ma.gaps, mb.gaps), (0, 0));
    assert!(ma.range.is_some() && mb.range.is_some(), "{ma:?} {mb:?}");
    let (Some(ia), Some(ib)) = (ma.integrated, mb.integrated) else {
        panic!("the integrated loudness did not carry on: {ma:?} {mb:?}");
    };
    // The bound. The programme holds whole periods in every slice, so
    // every slice it fills has one power P. A gating block of 40 kept
    // slices on the 100 ms grid has power P unless it holds a slice
    // of another: on replica a, where the K-weighting runs on across
    // the skipped slice, and on both at the end of the programme. The
    // filters are run here once over each, as the meters run them:
    // after the skip, S slices differ from P, by at most a factor h;
    // at the end, E slices lie between P and 0. Each is asserted
    // below the margin assumed for it.
    let (skip, end) = (l.skipped.start, l.end / SLICE);
    // Steady from 50 slices on, as the filters settle from silence.
    let after_skip = transient(skip - 100, skip - 1, Some(skip), end, 150);
    let tol = 1e-6;
    let after_skip = &after_skip[100..];
    let s = (after_skip.iter())
        .filter(|p| (*p - 1.0).abs() > tol)
        .count() as u64;
    let h_skip = after_skip.iter().fold(1.0f64, |h, &p| h.max(p));
    let at_end = transient(end - 100, end - 1, None, end, 150);
    let e = at_end.iter().filter(|&&p| p > tol && p < 1.0 - tol).count() as u64;
    let h_end = at_end.iter().fold(1.0f64, |h, &p| h.max(p));
    eprintln!("transient: S {s} slices up to {h_skip} P after the skip, E {e} up to {h_end} P");
    // The margin: twice the measured length and twice the measured
    // peak, so a transient of either kind lasts at most S' = 2S and
    // E' = 2E slices, each at most h = 2 max(h_skip, h_end) P.
    let (s2, e2) = (2 * s, 2 * e);
    let h = 2.0 * h_skip.max(h_end);
    // A run of r slices on a 10-slice grid of 40-slice blocks lies in
    // at most (r + 39) / 10 + 1 blocks, and the end, past which the
    // slices are 0, in 4 more. So at most D of the N blocks of either
    // replica differ from P, each in [0, h P], and its integrated
    // loudness, the mean power of the blocks above the gates, lies in
    //   [10 log10((N - D) P / N), 10 log10((N - D + D h) P / N)]:
    // the two lie within the width of that interval. N is at least
    // the slices kept before the end over 10, less the 3 windows
    // before the first block.
    let runs = |r: u64| if r == 0 { 0 } else { (r + 39) / 10 + 1 };
    let d = (runs(s2) + runs(e2) + 4) as f64;
    let kept = (l.end - l.reset) / SLICE - lost;
    let n = (kept / (WINDOW / SLICE) - 3) as f64;
    let tolerance = 10.0 * ((n - d + d * h) / (n - d)).log10();
    assert!(
        (ia - ib).abs() <= tolerance,
        "{ia} {ib}, tolerance {tolerance} dB over {n} blocks"
    );
    eprintln!(
        "lost {lost}: integrated {ia} and {ib}, {} dB apart, tolerance {tolerance} dB",
        (ia - ib).abs()
    );
}

/// A replica that skips blocks over a stretch the log takes an input
/// down for counts the same gaps there as its peer: the slices are
/// decided from the log, not left out as lost. Only those it skipped
/// outside that stretch are lost.
#[test]
fn a_skip_over_an_input_down_counts_the_peers_gaps() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let l = lost(Upset::SkipOverDown);
    let (ma, mb) = (&l.a, &l.b);
    let gaps = l.down.end - l.down.start;
    let outside = (l.skipped.clone()).filter(|k| !l.down.contains(k)).count() as u64;
    eprintln!("skipped {:?}, down {:?}", l.skipped, l.down);
    assert_eq!((ma.gaps, mb.gaps), (gaps, gaps), "{ma:?} {mb:?}");
    assert_eq!((ma.lost, mb.lost), (outside, 0), "{ma:?} {mb:?}");
    assert!(ma.integrated.is_some() && mb.integrated.is_some());
}

/// A replica marked diverged loses the slices it computes meanwhile,
/// and its peer none; both count the same gaps (none) and measure on.
#[test]
fn a_diverged_replica_loses_its_slices_and_its_peer_none() {
    let _engines = ENGINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let l = lost(Upset::Diverged);
    let (ma, mb) = (&l.a, &l.b);
    let (least, most) = l.diverged;
    assert!(least > 0, "{least}");
    assert!(
        (least..=most).contains(&ma.lost),
        "{} not in {least}..={most}",
        ma.lost
    );
    assert_eq!((mb.lost, ma.gaps, mb.gaps), (0, 0, 0), "{ma:?} {mb:?}");
    assert!(ma.integrated.is_some() && mb.integrated.is_some());
}

fn wait(f: &dyn Fn() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(10), "timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
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

/// An entry that arrives while a fade runs takes the value the fade
/// has at its index as the start of its own ramp, or replaces the
/// fade with its own, joined from that value where its own started
/// before the index, or ends it when it changes nothing.
#[test]
fn a_passed_entry_folds_into_a_running_fade() {
    let running = Fade {
        from: 1.0,
        start: 1000,
        len: 400,
    };
    let at = 1200;
    let held = running.value_at(0.25, at);
    assert!(held < 1.0 && held > 0.25);
    let fold = |to: f32, fade: Option<Fade>| {
        let (mut gains, mut fadings) = ([0.25f32], [Some(running)]);
        fold_passed(&mut gains, &mut fadings, 0, (to, fade), at, 480);
        (gains[0], fadings[0])
    };
    // A plain entry ramps over the ramp length from the held value.
    assert_eq!(
        fold(0.5, None),
        (
            0.5,
            Some(Fade {
                from: held,
                start: at,
                len: 480
            })
        )
    );
    // A fade replaces it as it is.
    let own = Fade {
        from: 0.9,
        start: at + 10,
        len: 100,
    };
    assert_eq!(fold(0.5, Some(own)), (0.5, Some(own)));
    // A fade started before the entry is joined from the held value.
    let late = Fade {
        from: 0.0,
        start: 900,
        len: 1000,
    };
    let joined = late.joined(0.5, held, at, 480);
    assert_eq!(
        joined,
        Some(Fade {
            from: held,
            start: at,
            len: 700
        })
    );
    assert_eq!(fold(0.5, Some(late)), (0.5, joined));
    // A target equal to the held value leaves no ramp.
    assert_eq!(fold(held, None), (held, None));
}

/// An entry's fade starts at most the fade bound past its index; a
/// start near the end of the index range is refused, not wrapped.
#[test]
fn a_fade_must_start_within_the_bound_of_its_entry() {
    let max = u64::from(samplez_dsp::MAX_EXACT_RAMP_LEN);
    let at = 1000;
    let f = |start| Fade {
        from: 1.0,
        start,
        len: 10,
    };
    assert!(f(at + max).within(at) && f(0).within(at));
    assert!(!f(at + max + 1).within(at) && !f(u64::MAX).within(at));
    assert!(f(u64::MAX).within(u64::MAX));
    let entry = |fade| Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.5,
        fade,
    };
    assert_eq!(
        fade_beyond(&[entry(None), entry(Some(f(at + max)))], at),
        None
    );
    assert_eq!(
        fade_beyond(&[entry(None), entry(Some(f(at + max + 1)))], at),
        Some(f(at + max + 1))
    );
}

/// A plain gain change renders as the absolute fade a replica tracks
/// for it, `from` the value held at its index and the ramp length as
/// the length, bit for bit, mid-ramp and chained.
#[test]
fn a_plain_set_renders_as_its_absolute_fade() {
    use samplez_dsp::Matrix;
    let ramp = 480u32;
    let mut plain = Matrix::new(1, 1);
    let mut fades = Matrix::new(1, 1);
    assert!(plain.set(0, 0, 1.0, 0) && fades.set(0, 0, 1.0, 0));
    // (index, target): the second change lands mid-ramp, the third
    // after the second has ended.
    let changes = [(100u64, 0.25f32), (300, 0.75), (1500, 0.0)];
    let mut held = 1.0f32;
    let mut last: Option<Fade> = None;
    let mut next = changes.iter().peekable();
    for n in 0..2200u64 {
        if let Some(&&(at, to)) = next.peek().filter(|c| c.0 == n) {
            assert!(plain.set(0, 0, to, ramp));
            let from = last.map_or(held, |f| f.value_at(held, at));
            let f = Fade {
                from,
                start: at,
                len: ramp,
            };
            assert!(fades.set_resumed(0, 0, to, f.ramp_at(to, at)));
            (held, last) = (to, Some(f));
            next.next();
        }
        assert_eq!(
            plain.gain(0, 0).to_bits(),
            fades.gain(0, 0).to_bits(),
            "sample {n}"
        );
        plain.advance(1);
        fades.advance(1);
    }
}
