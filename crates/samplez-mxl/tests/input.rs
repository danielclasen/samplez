//! Input generations, the synchronization group and the RT read against
//! flows written by an independent libmxl writer.

mod common;

use std::time::{Duration, Instant};

use common::{B, Domain, RawWriter, TIMING, api, expected, input, new_id, sample};
use mxl_sys::{
    MXL_ERR_FLOW_INVALID, MXL_ERR_FLOW_NOT_FOUND, MXL_ERR_INVALID_ARG,
    MXL_ERR_OUT_OF_RANGE_TOO_EARLY,
};
use samplez_mxl::input::{
    FROZEN_PROBE_BLOCKS, Generation, GroupWait, ON_TIME_NS, Outcome, Poll, PolledFlow, Recovery,
    SlotState, Timing,
};
use samplez_mxl::{Error, Instance, time};

const K0: u64 = 1_000_000;

/// A domain with one 2-channel flow written over blocks `K0..K0 + blocks`
/// and a generation with that flow attached to slot 0.
struct Fixture {
    domain: Domain,
    id: String,
    writer: RawWriter,
    generation: Generation,
}

fn fixture(blocks: u64) -> Fixture {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let mut writer = RawWriter::create(&api, &domain.0, &id, 2);
    writer.blocks(K0..K0 + blocks);
    let mut generation = Generation::new(&api, &domain.0, 2).unwrap();
    assert_eq!(
        generation.attach(0, &input(&id, 2, 0), &TIMING).unwrap(),
        None
    );
    Fixture {
        domain,
        id,
        writer,
        generation,
    }
}

/// The timestamp the group waits for before block `k`.
fn group_ts(g: &Generation, k: u64) -> u64 {
    time::group_timestamp(g.api(), k, B, g.max_offset())
}

#[test]
fn group_wait_returns_at_once_when_the_data_is_there() {
    let f = fixture(10);
    let g = &f.generation;
    let t = Instant::now();
    assert_eq!(g.wait(group_ts(g, K0 + 9), 2_000_000_000), GroupWait::Ok);
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
}

#[test]
fn group_wait_returns_early_when_the_producer_commits() {
    let mut f = fixture(10);
    let ts = group_ts(&f.generation, K0 + 10);
    let generation = f.generation;
    let t0 = Instant::now();
    let waiter = std::thread::spawn(move || {
        let s = generation.wait(ts, 5_000_000_000);
        (s, Instant::now(), generation)
    });
    std::thread::sleep(Duration::from_millis(100));
    // Ten blocks are one sync batch of libmxl's default 10 ms hint, so one
    // of the commits wakes the waiter.
    let committed = Instant::now();
    f.writer.blocks(K0 + 10..K0 + 20);
    let (status, returned, _generation) = waiter.join().unwrap();
    assert_eq!(status, GroupWait::Ok);
    assert!(returned >= committed, "{:?}", committed - returned);
    assert!(
        returned < t0 + Duration::from_secs(2),
        "{:?}",
        returned - t0
    );
}

#[test]
fn group_wait_times_out_when_the_producer_is_behind() {
    let f = fixture(10);
    let g = &f.generation;
    let t = Instant::now();
    assert_eq!(
        g.wait(group_ts(g, K0 + 10), 30_000_000),
        GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY)
    );
    assert!(
        t.elapsed() >= Duration::from_millis(30),
        "{:?}",
        t.elapsed()
    );
}

#[test]
fn group_wait_reports_a_replaced_flow() {
    let mut f = fixture(10);
    // Release deletes the flow; the same id is created anew.
    drop(f.writer);
    f.writer = RawWriter::create(&api(), &f.domain.0, &f.id, 2);
    f.writer.blocks(K0..K0 + 20);
    let g = &f.generation;
    assert_eq!(
        g.wait(group_ts(g, K0 + 10), 10_000_000),
        GroupWait::Failed(MXL_ERR_FLOW_INVALID)
    );
}

#[test]
fn group_timestamp_includes_the_largest_offset() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let mut w = RawWriter::create(&api, &domain.0, &id, 1);
    let mut g = Generation::new(&api, &domain.0, 2).unwrap();
    g.attach(0, &input(&id, 1, -100), &TIMING).unwrap();
    assert_eq!(g.max_offset(), -100);
    assert_eq!(
        group_ts(&g, K0),
        time::index_to_timestamp(&api, (K0 + 1) * B)
    );
    g.attach(1, &input(&id, 1, 24), &TIMING).unwrap();
    assert_eq!(g.max_offset(), 24);
    assert_eq!(
        group_ts(&g, K0),
        time::index_to_timestamp(&api, (K0 + 1) * B + 24)
    );

    w.write((K0 + 1) * B + 23, B as usize, sample);
    assert_eq!(
        g.wait(group_ts(&g, K0), 0),
        GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY)
    );
    w.write((K0 + 1) * B + 24, 1, sample);
    assert_eq!(g.wait(group_ts(&g, K0), 0), GroupWait::Ok);
}

#[test]
fn reads_return_the_samples_at_the_indices_written() {
    // 400 blocks pass the ring's wrap at a multiple of the buffer length;
    // the last 150 are within the readable half and cross it.
    let mut f = fixture(400);
    let g = &mut f.generation;
    g.attach(1, &input(&f.id, 2, 7), &TIMING).unwrap();
    let info = g.input(0).unwrap().reader().config().unwrap();
    // SAFETY: the flow is audio, a continuous format.
    let buffer_length = u64::from(unsafe { info.__bindgen_anon_1.continuous.bufferLength });
    let wrap = (K0 + 400) * B / buffer_length * buffer_length;
    assert!(wrap > (K0 + 250) * B && !wrap.is_multiple_of(B));

    let n = 2 * B as usize;
    for (slot, offset) in [(0, 0), (1, 7)] {
        let input = g.input(slot).unwrap();
        let mut state = SlotState::default();
        let mut dst = vec![f32::NAN; n];
        for k in K0 + 250..K0 + 399 {
            assert_eq!(
                input.read(&mut state, k, &GroupWait::Ok, &mut dst),
                Outcome::Copy
            );
            assert_eq!(dst, expected(2, k, offset), "slot {slot} block {k}");
        }
        assert_eq!(state.counters.copied, 149);
        assert_eq!(state.last_end, Some((K0 + 399) * B + offset as u64));
    }
}

#[test]
fn each_outcome_is_produced() {
    let f = fixture(300);
    let n = 2 * B as usize;
    let mut dst = vec![f32::NAN; n];
    let timeout = GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY);
    let head = (K0 + 300) * B;
    {
        let r = f.generation.input(0).unwrap();
        let mut s = SlotState::default();

        assert_eq!(r.read(&mut s, K0 + 290, &timeout, &mut dst), Outcome::Copy);
        assert_eq!(s.head, head);
        assert_eq!(dst, expected(2, K0 + 290, 0));

        // The newest block, ending at the head, after a timed-out wait.
        let mut at_head = SlotState::default();
        assert_eq!(
            r.read(&mut at_head, K0 + 299, &timeout, &mut dst),
            Outcome::Copy
        );
        assert_eq!(dst, expected(2, K0 + 299, 0));

        // A destination of the wrong length is refused and zeroed.
        let mut short = vec![f32::NAN; n - 1];
        let mut fresh = SlotState::default();
        assert_eq!(
            r.read(&mut fresh, K0 + 290, &GroupWait::Ok, &mut short),
            Outcome::Other(MXL_ERR_INVALID_ARG)
        );
        assert!(short.iter().all(|&v| v == 0.0));
        assert_eq!(fresh.last_end, None);

        // The same block again and an older one: below the last read.
        for k in [K0 + 290, K0 + 289] {
            dst.fill(f32::NAN);
            assert_eq!(
                r.read(&mut s, k, &GroupWait::Ok, &mut dst),
                Outcome::BelowLastRead
            );
            assert!(dst.iter().all(|&v| v == 0.0));
        }

        // Beyond the head after a timed-out wait: late, no read.
        assert_eq!(r.read(&mut s, K0 + 300, &timeout, &mut dst), Outcome::Late);

        // A block without a wait (catch-up) checks the head as a timed-out
        // wait does: late beyond it, copied up to it.
        let mut skipped = SlotState::default();
        assert_eq!(
            r.read(&mut skipped, K0 + 300, &GroupWait::Skipped, &mut dst),
            Outcome::Late
        );
        assert_eq!(
            r.read(&mut skipped, K0 + 299, &GroupWait::Skipped, &mut dst),
            Outcome::Copy
        );
        assert_eq!(dst, expected(2, K0 + 299, 0));

        // Beyond the head after a group OK, which a head moving backwards
        // between the wait and the read produces: libmxl's TOO_EARLY.
        assert_eq!(
            r.read(&mut s, K0 + 301, &GroupWait::Ok, &mut dst),
            Outcome::TooEarly
        );

        // Older than the readable half of the ring.
        let mut old = SlotState::default();
        assert_eq!(
            r.read(&mut old, K0 + 10, &timeout, &mut dst),
            Outcome::TooLate
        );
        assert_eq!(old.counters.too_late, 1);

        assert_eq!(
            s.counters,
            samplez_mxl::input::Counters {
                copied: 1,
                below_last_read: 2,
                late: 1,
                too_early: 1,
                ..Default::default()
            }
        );
        assert!(!s.invalid);
    }

    // The flow replaced under the same id: the reader still maps the old
    // one.
    drop(f.writer);
    let mut w = RawWriter::create(&api(), &f.domain.0, &f.id, 2);
    w.blocks(K0 + 300..K0 + 302);
    let r = f.generation.input(0).unwrap();
    let mut s = SlotState::default();
    // After a timed-out wait the head check finds the range beyond the old
    // head and makes no read call, so libmxl's validity check does not
    // run.
    assert_eq!(r.read(&mut s, K0 + 300, &timeout, &mut dst), Outcome::Late);
    assert_eq!(s.counters.late, 1);
    assert_eq!(s.counters.invalid + s.counters.too_early, 0);
    assert!(!s.invalid);
    // After a group OK the read call runs into libmxl's validity check.
    assert_eq!(
        r.read(&mut s, K0 + 300, &GroupWait::Ok, &mut dst),
        Outcome::Invalid
    );
    assert!(s.invalid);
    assert_eq!(s.counters.invalid, 1);
}

#[test]
fn identify_probes_and_marks_a_replaced_flow() {
    let f = fixture(20);
    let n = 2 * B as usize;
    let mut dst = vec![0.0; n];
    let timeout = GroupWait::Failed(MXL_ERR_OUT_OF_RANGE_TOO_EARLY);
    let mut s = SlotState::default();
    {
        let r = f.generation.input(0).unwrap();
        // A stalled, valid flow: one probe at the tenth frozen late block.
        for i in 1..=FROZEN_PROBE_BLOCKS + 2 {
            let o = r.read(&mut s, K0 + 20 + u64::from(i), &timeout, &mut dst);
            assert_eq!(o, Outcome::Late);
            assert_eq!(s.frozen_late_blocks, i);
            assert!(!r.identify(&mut s, &timeout, o));
            assert_eq!(s.counters.probes, u64::from(i >= FROZEN_PROBE_BLOCKS));
        }
        // A non-late block is never probed, whatever the group said.
        assert!(!r.identify(
            &mut s,
            &GroupWait::Failed(MXL_ERR_FLOW_INVALID),
            Outcome::TooLate
        ));
        assert_eq!(s.counters.probes, 1);
    }

    drop(f.writer);
    let _w = RawWriter::create(&api(), &f.domain.0, &f.id, 2);
    let r = f.generation.input(0).unwrap();
    let o = r.read(&mut s, K0 + 40, &timeout, &mut dst);
    assert_eq!(o, Outcome::Late);
    // Past the tenth frozen block a timeout alone does not probe.
    assert!(!r.identify(&mut s, &timeout, o));
    assert_eq!(s.counters.probes, 1);
    // A group FLOW_INVALID does, and marks the slot.
    assert!(r.identify(&mut s, &GroupWait::Failed(MXL_ERR_FLOW_INVALID), o));
    assert!(s.invalid);
    assert_eq!(s.counters.probes, 2);
    // Marked: not probed again.
    assert!(!r.identify(&mut s, &GroupWait::Failed(MXL_ERR_FLOW_INVALID), o));
    assert_eq!(s.counters.probes, 2);
}

#[test]
fn a_new_generation_reads_the_same_samples_and_outlives_the_old_one() {
    let api = api();
    let domain = Domain::new();
    let (a, b) = (new_id(), new_id());
    let mut wa = RawWriter::create(&api, &domain.0, &a, 2);
    let mut wb = RawWriter::create(&api, &domain.0, &b, 1);
    wa.blocks(K0..K0 + 20);
    wb.blocks(K0..K0 + 20);
    let build = || {
        let mut g = Generation::new(&api, &domain.0, 2).unwrap();
        g.attach(0, &input(&a, 2, 0), &TIMING).unwrap();
        g.attach(1, &input(&b, 1, 3), &TIMING).unwrap();
        g
    };
    let channels = [2usize, 1];
    let offsets = [0i64, 3];
    let mut states = [SlotState::default(), SlotState::default()];

    let old = build();
    for k in K0 + 10..K0 + 15 {
        for slot in 0..2 {
            let mut dst = vec![0.0; channels[slot] * B as usize];
            let r = old.input(slot).unwrap();
            assert_eq!(
                r.read(&mut states[slot], k, &GroupWait::Ok, &mut dst),
                Outcome::Copy
            );
        }
    }

    // Built while the old generation is in use.
    let new = build();
    assert!(!old.same_instance(&new));
    assert!(old.same_instance(&old));
    for k in K0 + 15..K0 + 19 {
        for slot in 0..2 {
            let mut from_old = vec![0.0f32; channels[slot] * B as usize];
            let mut from_new = vec![1.0f32; channels[slot] * B as usize];
            let r_old = old.input(slot).unwrap();
            let r_new = new.input(slot).unwrap();
            let mut s_old = SlotState::default();
            let mut s_new = SlotState::default();
            assert_eq!(
                r_old.read(&mut s_old, k, &GroupWait::Ok, &mut from_old),
                Outcome::Copy
            );
            assert_eq!(
                r_new.read(&mut s_new, k, &GroupWait::Ok, &mut from_new),
                Outcome::Copy
            );
            let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(bits(&from_old), bits(&from_new));
            assert_eq!(from_new, expected(channels[slot], k, offsets[slot]));
        }
    }

    // The slot state carries over: the last block read through the old
    // generation is not read again through the new one.
    for slot in 0..2 {
        let mut dst = vec![0.0; channels[slot] * B as usize];
        let r = new.input(slot).unwrap();
        assert_eq!(
            r.read(&mut states[slot], K0 + 14, &GroupWait::Ok, &mut dst),
            Outcome::BelowLastRead
        );
        assert_eq!(states[slot].counters.copied, 5);
    }

    drop(old);
    wa.blocks(K0 + 20..K0 + 30);
    wb.blocks(K0 + 20..K0 + 30);
    let ts = time::group_timestamp(&api, K0 + 28, B, new.max_offset());
    assert_eq!(new.wait(ts, 0), GroupWait::Ok);
    for k in K0 + 15..K0 + 29 {
        for slot in 0..2 {
            let mut dst = vec![0.0; channels[slot] * B as usize];
            let r = new.input(slot).unwrap();
            assert_eq!(
                r.read(&mut states[slot], k, &GroupWait::Ok, &mut dst),
                Outcome::Copy
            );
            assert_eq!(dst, expected(channels[slot], k, offsets[slot]));
        }
    }
}

#[test]
fn attach_refuses_flows_that_do_not_fit() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let _w = RawWriter::create(&api, &domain.0, &id, 2);
    let mut g = Generation::new(&api, &domain.0, 1).unwrap();

    assert!(matches!(
        g.attach(0, &input(&id, 3, 0), &TIMING),
        Err(Error::Format(_))
    ));
    assert!(matches!(
        g.attach(0, &input(&new_id(), 2, 0), &TIMING),
        Err(Error::Mxl {
            status: MXL_ERR_FLOW_NOT_FOUND,
            ..
        })
    ));
    assert!(matches!(
        g.attach(1, &input(&id, 2, 0), &TIMING),
        Err(Error::Format(_))
    ));
    // The readable half of the default ring is 9728 samples.
    let wide = Timing {
        margin: 9728 - 4 * B - 100,
        ..TIMING
    };
    assert!(matches!(
        g.attach(0, &input(&id, 2, -101), &wide),
        Err(Error::Window {
            required: 9729,
            buffer_length: 19456
        })
    ));
    assert!(!g.has_inputs());
    assert!(g.attach(0, &input(&id, 2, 100), &wide).is_ok());

    // libmxl's default commit batch hint is 10 ms, 480 samples.
    let narrow = Timing {
        margin: 527,
        ..TIMING
    };
    assert_eq!(g.attach(0, &input(&id, 2, 0), &narrow).unwrap(), Some(528));
    assert_eq!(g.attach(0, &input(&id, 2, -1), &narrow).unwrap(), None);
    assert!(g.has_inputs());
}

#[test]
fn a_poll_reader_follows_a_replaced_flow_for_every_holder() {
    let api = api();
    let domain = Domain::new();
    let id = new_id();
    let poll = Instance::new(&api, &domain.0).unwrap();
    let mut p = PolledFlow::new(&poll, &id).unwrap();
    p.hold();
    assert!(matches!(p.poll(), Poll::Absent(_)));

    let mut w = RawWriter::create(&api, &domain.0, &id, 1);
    w.blocks(K0..K0 + 5);
    assert!(matches!(p.poll(), Poll::Head(h) if h == (K0 + 5) * B));

    // Both holders' references are released, else libmxl would hand the
    // stale reader out again and every poll would report it invalid.
    drop(w);
    let mut w = RawWriter::create(&api, &domain.0, &id, 1);
    w.blocks(K0 + 7..K0 + 8);
    assert!(matches!(p.poll(), Poll::Invalid));
    assert!(matches!(p.poll(), Poll::Head(h) if h == (K0 + 8) * B));
    w.blocks(K0 + 8..K0 + 9);
    assert!(matches!(p.poll(), Poll::Head(h) if h == (K0 + 9) * B));

    assert_eq!(p.release(), 1);
    assert!(matches!(p.poll(), Poll::Head(_)));
}

#[test]
fn recovery_needs_a_head_on_time_for_one_second() {
    let api = api();
    let now0 = time::index_to_timestamp(&api, K0 * B);
    let on_time = K0 * B - TIMING.margin + B + 10;
    let mut r = Recovery::default();
    let at = |ns: u64| now0 + ns;
    // At the threshold with offset 10.
    assert!(!r.update(&api, Some(on_time), at(0), &TIMING, 10));
    assert!(!r.update(
        &api,
        Some(on_time + 48_000),
        at(ON_TIME_NS - 1),
        &TIMING,
        10
    ));
    assert!(r.update(&api, Some(on_time + 48_000), at(ON_TIME_NS), &TIMING, 10));
    // One sample short restarts the period, as does no head.
    let idx = |ns: u64| time::timestamp_to_index(&api, at(ns));
    let threshold = |ns: u64| idx(ns) - TIMING.margin + B + 10;
    let t = 2 * ON_TIME_NS;
    assert!(!r.update(&api, Some(threshold(t) - 1), at(t), &TIMING, 10));
    assert!(!r.update(&api, Some(threshold(t + 1)), at(t + 1), &TIMING, 10));
    assert!(!r.update(&api, None, at(t + 2), &TIMING, 10));
    assert!(!r.update(&api, Some(threshold(t + 3)), at(t + 3), &TIMING, 10));
    assert!(r.update(
        &api,
        Some(threshold(t + 3 + ON_TIME_NS)),
        at(t + 3 + ON_TIME_NS),
        &TIMING,
        10
    ));
}
