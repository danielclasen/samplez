use super::*;
use crate::transfer::SlotCore;

/// Skipped samples go out as the slices they reach: a slice in
/// progress takes the gap flag and is sent at its end, incomplete;
/// whole slices go out as one span record per run; a slice a skip
/// ends inside is filled by the samples computed next and keeps the
/// flag; a pending reset splits a span at its window.
#[test]
fn skipped_samples_go_out_as_slices_and_spans() {
    let (mut tx, mut rx) = rtrb::RingBuffer::new(64);
    let counters = Counters::default();
    let mut seq = 0;
    let mut m = OutMeter::new(1);
    let buf = PlanarBuffer::new(1, 400);
    let mut sink = MeterSink {
        meter: 0,
        ring: &mut tx,
        seq: &mut seq,
        counters: &counters,
    };
    m.add(&mut sink, 0, &buf, 0, 240);
    m.skip(&mut sink, 240, 1200, GAP);
    m.skip(&mut sink, 1200, 2000, 0);
    m.add(&mut sink, 0, &buf, 2000, 400);
    m.reset_at = Some(WINDOW);
    m.skip(&mut sink, 2400, WINDOW + 3 * SLICE, 0);
    m.skip(
        &mut sink,
        WINDOW + 3 * SLICE,
        WINDOW + 5 * SLICE,
        GAP | DIVERGED,
    );
    let mut got = Vec::new();
    while let Ok(r) = rx.pop() {
        got.push((r.slice, r.frames, r.flags, r.reset()));
    }
    assert_eq!(
        got,
        [
            (0, 240, GAP, None),
            (1, 1, SPAN | GAP, None),
            (2, 0, GAP, None),
            (3, 1, SPAN, None),
            (4, 400, 0, None),
            (5, 5, SPAN, None),
            (10, 3, SPAN, Some(WINDOW)),
            (13, 2, SPAN | GAP | DIVERGED, None),
        ]
    );
    assert!(got.len() as u64 == seq);
}

/// The meter records of one output channel pair from block `first`
/// on, with a loudness reset at index `reset`, of a signal that is a
/// function of the sample index.
fn meter_records(first: u64, reset: u64, blocks: u64) -> Vec<MeterRecord> {
    let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 14);
    let counters = Counters::default();
    let mut seq = 0;
    let mut m = OutMeter::new(2);
    let b = B as usize;
    let mut buf = PlanarBuffer::new(2, b);
    let mut records = Vec::new();
    for k in first..blocks {
        if k * B == reset - 4 * B {
            // The command arrives a few blocks ahead, mid-window.
            m.reset_at = Some(reset.div_ceil(WINDOW) * WINDOW);
        }
        for c in 0..2 {
            for (n, s) in buf.channel_mut(c).iter_mut().enumerate() {
                let i = k * B + n as u64;
                *s = ((i * 7919 + c as u64 * 31) % 1013) as f32 / 1013.0 - 0.5;
            }
        }
        let mut sink = MeterSink {
            meter: 0,
            ring: &mut tx,
            seq: &mut seq,
            counters: &counters,
        };
        m.add(&mut sink, 0, &buf, k * B, b);
        while let Ok(r) = rx.pop() {
            records.push(r);
        }
    }
    records
}

/// From a reset on, the records depend only on the output since 11
/// samples before its index: two engines that started at different
/// blocks before that, with different K-weighting histories, send the
/// same bits. Records cover
/// whole slices, and the reset lands on the window boundary after its
/// index.
#[test]
fn meters_after_a_reset_do_not_depend_on_the_start() {
    let reset = 2 * WINDOW;
    let key = |r: &MeterRecord| {
        (
            r.channel,
            r.slice,
            r.frames,
            r.peak.to_bits(),
            r.true_peak.to_bits(),
            r.sum_sq.to_bits(),
            r.reset(),
        )
    };
    let after = |v: Vec<MeterRecord>| -> Vec<_> {
        v.iter()
            .filter(|r| r.slice * SLICE >= reset)
            .map(key)
            .collect()
    };
    let a = meter_records(0, reset, 600);
    let b = meter_records(reset / B - 20, reset, 600);
    assert!(a.windows(2).all(|w| w[1].seq == w[0].seq + 1));
    let (a, b) = (after(a), after(b));
    assert_eq!(a.len(), 2 * (600 * B - reset).div_ceil(SLICE) as usize);
    assert!(
        a[0].6 == Some(reset) && a[1].6 == Some(reset) && a[2].6.is_none(),
        "the reset flags the first slice"
    );
    assert!(a.iter().all(|r| r.2 == SLICE as u32));
    assert_eq!(a, b);
}

/// A reset leaves the true peak of a steady signal alone: the
/// interpolator keeps the samples before the reset index, so the
/// first interpolated outputs after it are not partial sums of the
/// filter, which overshoot by up to 1 dB.
#[test]
fn a_reset_does_not_move_the_true_peak() {
    let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 12);
    let counters = Counters::default();
    let mut seq = 0;
    let mut m = OutMeter::new(1);
    let mut buf = PlanarBuffer::new(1, B as usize);
    let mut peaks = Vec::new();
    for k in 0..30 * SLICE / B {
        if k == 10 * SLICE / B {
            m.reset_at = Some(2 * WINDOW);
        }
        // A 1 kHz cosine at -1.9 dBFS, one period per block.
        for (n, s) in buf.channel_mut(0).iter_mut().enumerate() {
            *s = 0.8 * (std::f32::consts::TAU * n as f32 / B as f32).cos();
        }
        let mut sink = MeterSink {
            meter: 0,
            ring: &mut tx,
            seq: &mut seq,
            counters: &counters,
        };
        m.add(&mut sink, 0, &buf, k * B, B as usize);
        while let Ok(r) = rx.pop() {
            peaks.push((r.reset().is_some(), r.true_peak.to_bits()));
        }
    }
    let reset = peaks.iter().position(|p| p.0).unwrap();
    assert_eq!(reset as u64, 2 * WINDOW / SLICE);
    assert!(
        peaks[1..].iter().all(|p| p.1 == peaks[1].1),
        "{:?}",
        peaks
            .iter()
            .map(|p| f32::from_bits(p.1))
            .collect::<Vec<_>>()
    );
}

const B: u64 = 48;

/// After an overrun skip or a clock step either way, the true peak of
/// the first slice is that of the samples after the discontinuity
/// alone: a quiet cosine after a loud one reads the same bits as its
/// later slices.
#[test]
fn a_discontinuity_restarts_the_true_peak() {
    for resume in [27, 15] {
        let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 12);
        let counters = Counters::default();
        let mut seq = 0;
        let mut m = OutMeter::new(1);
        let mut buf = PlanarBuffer::new(1, B as usize);
        let mut peaks = Vec::new();
        for (i, k) in (0..20).chain(resume..resume + 40).enumerate() {
            let a = if i < 20 { 0.8 } else { 0.1 };
            for (n, s) in buf.channel_mut(0).iter_mut().enumerate() {
                *s = a * (std::f32::consts::TAU * n as f32 / B as f32).cos();
            }
            let mut sink = MeterSink {
                meter: 0,
                ring: &mut tx,
                seq: &mut seq,
                counters: &counters,
            };
            m.add(&mut sink, 0, &buf, k * B, B as usize);
            while let Ok(r) = rx.pop() {
                peaks.push(r.true_peak);
            }
        }
        let after = &peaks[2..];
        assert!(
            after.iter().all(|p| p.to_bits() == after[0].to_bits()) && after[0] < 0.11,
            "resumed at block {resume}: {peaks:?}"
        );
    }
}

/// An input's meter without `truePeak` measures what an output's does
/// but the true peak: the same sample peaks and powers, bit for bit,
/// and a true peak of 0.
#[test]
fn an_input_meter_is_an_output_meter_without_true_peak() {
    let (mut tx, mut rx) = rtrb::RingBuffer::new(1 << 12);
    let counters = Counters::default();
    let mut seq = 0;
    let (mut out, mut input) = (OutMeter::new(2), OutMeter::without_true_peak(2));
    let mut buf = PlanarBuffer::new(2, B as usize);
    let mut records = [Vec::new(), Vec::new()];
    for k in 0..40 {
        for c in 0..2 {
            for (n, s) in buf.channel_mut(c).iter_mut().enumerate() {
                let i = k * B + n as u64;
                *s = ((i * 7919 + c as u64 * 31) % 1013) as f32 / 1013.0 - 0.5;
            }
        }
        for (m, meter) in [&mut out, &mut input].into_iter().enumerate() {
            let mut sink = MeterSink {
                meter: m as u32,
                ring: &mut tx,
                seq: &mut seq,
                counters: &counters,
            };
            meter.add(&mut sink, 0, &buf, k * B, B as usize);
            while let Ok(r) = rx.pop() {
                records[m].push(r);
            }
        }
    }
    let [o, i] = &records;
    assert_eq!(o.len(), 2 * 40 * B as usize / SLICE as usize);
    assert_eq!(o.len(), i.len());
    for (o, i) in o.iter().zip(i) {
        assert_eq!(
            (o.slice, o.channel, o.frames),
            (i.slice, i.channel, i.frames)
        );
        assert_eq!(o.peak.to_bits(), i.peak.to_bits());
        assert_eq!(o.sum_sq.to_bits(), i.sum_sq.to_bits());
        assert_eq!(o.plain_sum_sq.to_bits(), i.plain_sum_sq.to_bits());
        assert!(o.true_peak >= o.peak && i.true_peak == 0.0);
    }
}

fn entry(change: Change, at_index: u64) -> RtEntry {
    static ID: AtomicU64 = AtomicU64::new(1);
    RtEntry {
        id: ID.fetch_add(1, Relaxed),
        at_index,
        change,
    }
}

fn change(gain: f32, at_index: u64) -> RtEntry {
    let change = Change::Gain {
        input: 0,
        output: 0,
        gain,
        fade: None,
    };
    entry(change, at_index)
}

fn mix() -> Mix {
    let mut m = Matrix::new(1, 1);
    assert!(m.set(0, 0, 1.0, 0));
    Mix::new(m, B, 480)
}

/// Runs `to(k)` and returns the late changes it reported.
fn to(m: &mut Mix, k: u64) -> Vec<(u64, u64)> {
    let mut late = Vec::new();
    m.to(k, |c, pos| late.push((c.at_index, pos)));
    late
}

#[test]
fn plan_follows_the_clock() {
    let p = |next, newest| plan(next, newest, 2, B);
    let none = |k| Plan {
        catchup: k..k,
        skipped: 0,
        k,
        idle: false,
    };
    assert_eq!(p(None, None), none(0));
    assert_eq!(p(None, Some(9)), none(10));
    // On time: the newest due block is the last one processed.
    assert_eq!(p(Some(10), Some(9)), none(10));
    // Two blocks behind: both caught up in order.
    assert_eq!(
        p(Some(10), Some(11)),
        Plan {
            catchup: 10..12,
            skipped: 0,
            k: 12,
            idle: false,
        }
    );
    // Forward clock step: only the newest block, the rest skipped.
    assert_eq!(
        p(Some(10), Some(100)),
        Plan {
            catchup: 100..101,
            skipped: 90 * B,
            k: 101,
            idle: false,
        }
    );
    // Backward clock step: nothing is processed until the clock is due
    // at the block after the last one processed again.
    let behind = Plan {
        catchup: 100..100,
        skipped: 0,
        k: 100,
        idle: true,
    };
    assert_eq!(p(Some(100), Some(49)), behind);
    assert_eq!(p(Some(100), Some(97)), behind);
    assert_eq!(p(Some(100), None), behind);
    // A wake a block early waits for the next block as usual.
    assert_eq!(p(Some(100), Some(98)), none(100));
}

#[test]
fn a_forward_step_applies_changes_at_their_index() {
    let mut m = mix();
    assert!(to(&mut m, 10).is_empty());
    assert!(m.push(change(0.5, 50 * B)));
    assert!(to(&mut m, 100).is_empty());
    assert_eq!((m.applied, m.pending.len()), (1, 0));
    // The ramp started at 50B and ended at 50B + 480, before 100B.
    assert_eq!(m.matrix.gain(0, 0), 0.5);
    assert_eq!(m.ramp_end, 50 * B + 480);
    assert!(!m.ramping(100));
    assert_eq!(m.pos, Some(101 * B));
}

/// A gain change with a fade, at `at_index`.
fn faded(gain: f32, at_index: u64, delay: u32, len: u32) -> RtEntry {
    let fade = Some(crate::engine::Fade::after(1.0, at_index, delay, len));
    entry(
        Change::Gain {
            input: 0,
            output: 0,
            gain,
            fade,
        },
        at_index,
    )
}

/// The coefficient at the start of each of `blocks` blocks of `b`
/// samples, the matrix moved on over each block as the mix does.
fn walk(m: &mut Mix, blocks: u64) -> Vec<f32> {
    let mut gains = Vec::new();
    for k in 0..blocks {
        to(m, k);
        gains.push(m.matrix.gain(0, 0));
        m.matrix.advance(m.block as usize);
    }
    gains
}

/// `1.0` to `target`: held below `start`, linear over `len` from it,
/// the target after.
fn expected(n: u64, start: u64, len: u64, target: f32) -> f32 {
    if n < start {
        1.0
    } else if n < start + len {
        1.0 + (n - start) as f32 * ((target - 1.0) / len as f32)
    } else {
        target
    }
}

#[test]
fn a_delayed_fade_holds_then_ramps_exactly() {
    let mut m = mix();
    assert!(m.push(faded(0.25, 2 * B, 96, 192)));
    let gains = walk(&mut m, 12);
    for (k, g) in gains.iter().enumerate() {
        let want = expected(k as u64 * B, 2 * B + 96, 192, 0.25);
        assert_eq!(g.to_bits(), want.to_bits(), "block {k}");
    }
    assert_eq!((gains[4], gains[11]), (1.0, 0.25));
    assert!(gains[5] < 1.0 && gains[5] > 0.25);
    // Flagged as ramping through the delay and the ramp.
    assert_eq!(m.ramp_end, 2 * B + 96 + 192);
    assert!(m.ramping(2) && m.ramping(7) && !m.ramping(8));
}

#[test]
fn a_later_change_replaces_a_fade_still_waiting() {
    let mut m = mix();
    assert!(m.push(faded(0.25, 2 * B, 480, 96)));
    assert!(m.push(change(0.5, 4 * B)));
    let gains = walk(&mut m, 20);
    // The coefficient never goes toward 0.25: it holds 1.0, then
    // ramps to 0.5 over the ramp length from the later entry.
    for (k, g) in gains.iter().enumerate() {
        let want = expected(k as u64 * B, 4 * B, 480, 0.5);
        assert_eq!(g.to_bits(), want.to_bits(), "block {k}");
    }
    assert_eq!(gains[19], 0.5);
}

#[test]
fn a_fade_does_not_depend_on_the_block_size() {
    let walk_b = |b: u64| {
        let mut m = Matrix::new(1, 1);
        assert!(m.set(0, 0, 1.0, 0));
        let mut m = Mix::new(m, b, 480);
        assert!(m.push(faded(0.25, 96, 100, 250)));
        let blocks = 720 / b;
        let all = walk(&mut m, blocks);
        // Every 48th sample, the index both block sizes start a block at.
        all.chunks((48 / b) as usize)
            .map(|c| c[0])
            .collect::<Vec<_>>()
    };
    let reference = walk_b(48);
    assert!(reference.iter().any(|&g| g != 1.0 && g != 0.25));
    for b in [16, 24, 48] {
        let got: Vec<u32> = walk_b(b).iter().map(|g| g.to_bits()).collect();
        let want: Vec<u32> = reference.iter().map(|g| g.to_bits()).collect();
        assert_eq!(got, want, "block size {b}");
    }
}

/// The entry a diff derives from an absolute fade renders the
/// absolute function bit for bit from its index on where the index
/// lies at or before the fade's start, whatever value the coefficient
/// held; past it, the fade it joins from that value
/// ([`crate::engine::Fade::joined`]).
#[test]
fn a_diff_of_an_absolute_fade_renders_the_absolute_function() {
    use crate::targets::Targets;
    let f = crate::engine::Fade {
        from: 1.0,
        start: 6 * B + 10,
        len: 200,
    };
    let next = Targets {
        gains: vec![vec![0.25]],
        fading: vec![vec![Some(f)]],
        ..Targets::default()
    };
    let was = Targets {
        gains: vec![vec![0.7]],
        ..Targets::default()
    };
    for at in [2 * B, 6 * B, 7 * B, 8 * B, 11 * B] {
        let crate::engine::Entry::Gain { fade, .. } = was.diff(&next)[0].clone() else {
            panic!("no gain entry");
        };
        let mut matrix = Matrix::new(1, 1);
        assert!(matrix.set(0, 0, 0.7, 0));
        let mut m = Mix::new(matrix, B, 480);
        let e = entry(
            Change::Gain {
                input: 0,
                output: 0,
                gain: 0.25,
                fade,
            },
            at,
        );
        assert!(m.push(e));
        let gains = walk(&mut m, 14);
        let rendered = f.joined(0.25, 0.7, at, 480);
        assert_eq!(rendered == Some(f), at <= f.start, "at {at}");
        for (k, g) in gains.iter().enumerate() {
            let n = k as u64 * B;
            if n >= at {
                let want = rendered.map_or(0.25, |f| f.value_at(0.25, n));
                assert_eq!(g.to_bits(), want.to_bits(), "at {at}, sample {n}");
            }
        }
    }
}

/// The coefficient at every sample of `blocks` blocks of `m`, the
/// matrix moved on sample by sample as the mix moves it over a block.
fn walk_samples(m: &mut Mix, blocks: u64) -> Vec<f32> {
    let mut gains = Vec::new();
    for k in 0..blocks {
        to(m, k);
        for _ in 0..m.block {
            gains.push(m.matrix.gain(0, 0));
            m.matrix.advance(1);
        }
    }
    gains
}

/// A fade from 0 to 1 over 4800 samples from index 0, applied late at
/// index 960 to a coefficient holding 0.8, while it is at 0.2: the
/// coefficient joins it from 0.8 with no step, in a straight line to
/// its end at 4800, never steeper than a plain change over the ramp
/// length, and the same bits whatever the block size, at ramp lengths
/// of 10 and 20 ms. Fails on an engine that renders the fade's own
/// value from the entry's index, which steps by 0.6.
#[test]
fn a_late_fade_joins_from_the_value_held() {
    let f = crate::engine::Fade {
        from: 0.0,
        start: 0,
        len: 4800,
    };
    let at = 960;
    for ramp in [480u32, 960] {
        let run = |b: u64| {
            let mut matrix = Matrix::new(1, 1);
            assert!(matrix.set(0, 0, 0.8, 0));
            let mut m = Mix::new(matrix, b, ramp);
            let e = entry(
                Change::Gain {
                    input: 0,
                    output: 0,
                    gain: 1.0,
                    fade: Some(f),
                },
                at,
            );
            assert!(m.push(e));
            walk_samples(&mut m, 6000 / b)
        };
        let reference = run(48);
        // The largest step a change over the ramp length takes, and
        // a rounding's worth over it.
        let slope = (1.0f32 - 0.8) / ramp as f32;
        for (n, w) in reference.windows(2).enumerate() {
            let step = (w[1] - w[0]).abs();
            assert!(step <= slope * 1.001, "ramp {ramp}, sample {n}: {step}");
        }
        assert_eq!(reference[at as usize], 0.8, "ramp {ramp}");
        assert!(reference[at as usize + 1] > 0.8, "ramp {ramp}");
        assert!(
            reference[4799] < 1.0 && reference[4800] == 1.0,
            "ramp {ramp}"
        );
        for b in [16, 24, 40] {
            let got: Vec<u32> = run(b).iter().map(|g| g.to_bits()).collect();
            let want: Vec<u32> = reference[..got.len()].iter().map(|g| g.to_bits()).collect();
            assert_eq!(got, want, "ramp {ramp}, block size {b}");
        }
    }
}

/// A fade applied after its end ramps the coefficient from the value
/// held to the fade's target over the ramp length, bit for bit as a
/// change with no fade does. Fails on an engine that jumps to the
/// target.
#[test]
fn a_fade_past_its_end_ramps_as_a_plain_change() {
    let f = crate::engine::Fade {
        from: 0.0,
        start: 0,
        len: 96,
    };
    let run = |fade| {
        let mut m = mix();
        assert!(m.matrix.set(0, 0, 0.25, 0));
        let e = entry(
            Change::Gain {
                input: 0,
                output: 0,
                gain: 1.0,
                fade,
            },
            10 * B,
        );
        assert!(m.push(e));
        walk_samples(&mut m, 30)
    };
    let faded: Vec<u32> = run(Some(f)).iter().map(|g| g.to_bits()).collect();
    let plain: Vec<u32> = run(None).iter().map(|g| g.to_bits()).collect();
    assert_eq!(faded, plain);
    assert_ne!(f32::from_bits(faded[10 * B as usize + 1]), 1.0);
}

/// A fade asserted again mid-ramp leaves the coefficient on the
/// bits of the one that ran without it.
#[test]
fn an_assert_mid_ramp_changes_no_bit() {
    let f = crate::engine::Fade::after(1.0, 2 * B, 48, 300);
    let run = |again: bool| {
        let mut m = mix();
        let e = |at| {
            entry(
                Change::Gain {
                    input: 0,
                    output: 0,
                    gain: 0.25,
                    fade: Some(f),
                },
                at,
            )
        };
        assert!(m.push(e(2 * B)));
        if again {
            assert!(m.push(e(5 * B)));
        }
        walk(&mut m, 14)
            .iter()
            .map(|g| g.to_bits())
            .collect::<Vec<_>>()
    };
    assert_eq!(run(true), run(false));
}

/// A fade starting at the end of the index range neither panics nor
/// wraps the ramp end.
#[test]
fn a_fade_near_the_index_limit_does_not_overflow() {
    let mut m = mix();
    let f = crate::engine::Fade {
        from: 1.0,
        start: u64::MAX - 5,
        len: 100,
    };
    assert!(m.push(entry(
        Change::Gain {
            input: 0,
            output: 0,
            gain: 0.5,
            fade: Some(f)
        },
        2 * B,
    )));
    to(&mut m, 2);
    assert_eq!(m.ramp_end, u64::MAX);
    assert!(!f.within(2 * B));
}

#[test]
fn a_batch_applies_each_gain_with_its_own_fade() {
    use crate::batch::{Item, fold};
    let g = |output, gain, fade| {
        Item::Change(Change::Gain {
            input: 0,
            output,
            gain,
            fade,
        })
    };
    let fade = Some(crate::engine::Fade::after(1.0, 2 * B, 96, 192));
    let items = [g(0, 0.25, fade), g(1, 0.5, None)];
    let Some((change, Some(batch))) = fold(&items, 2, 2).unwrap() else {
        panic!("not a batch");
    };
    let mut matrix = Matrix::new(1, 2);
    assert!(matrix.set(0, 0, 1.0, 0) && matrix.set(0, 1, 1.0, 0));
    let mut m = Mix::new(matrix, B, 480);
    assert!(m.push_with(entry(change, 2 * B), Some(batch)).is_ok());
    let mut seen = Vec::new();
    for k in 0..12 {
        to(&mut m, k);
        seen.push((m.matrix.gain(0, 0), m.matrix.gain(0, 1)));
        m.matrix.advance(B as usize);
    }
    for (k, (a, b)) in seen.iter().enumerate() {
        let n = k as u64 * B;
        assert_eq!(a.to_bits(), expected(n, 2 * B + 96, 192, 0.25).to_bits());
        assert_eq!(b.to_bits(), expected(n, 2 * B, 480, 0.5).to_bits());
    }
}

#[test]
fn a_backward_step_re_anchors_and_applies_changes_at_its_start() {
    let mut m = mix();
    to(&mut m, 99);
    assert!(m.push(change(0.5, 50 * B)));
    // The clock stepped back to block 50: the change at the new start
    // applies, it is not late.
    assert!(to(&mut m, 50).is_empty());
    assert_eq!((m.applied, m.pending.len()), (1, 0));
    assert_eq!(m.pos, Some(51 * B));
    assert!(m.ramping(50) && m.matrix.is_ramping(0, 0));
    // The next block follows the new position.
    assert!(m.push(change(0.25, 51 * B)));
    assert!(to(&mut m, 51).is_empty());
    assert_eq!(m.applied, 2);
}

#[test]
fn a_backward_step_keeps_the_remaining_ramp() {
    let mut m = mix();
    to(&mut m, 9);
    assert!(m.push(change(0.5, 10 * B)));
    to(&mut m, 10);
    assert_eq!(m.ramp_end, 10 * B + 480);
    // Back by 5 blocks at block 12: 480 - 2B of the ramp remain.
    to(&mut m, 11);
    to(&mut m, 7);
    assert_eq!(m.ramp_end, 7 * B + 480 - 2 * B);
}

#[test]
fn a_change_below_the_position_is_late_and_not_applied() {
    let mut m = mix();
    to(&mut m, 10);
    assert!(m.push(change(0.5, 5 * B)));
    assert_eq!(to(&mut m, 11), vec![(5 * B, 11 * B)]);
    assert_eq!((m.applied, m.pending.len()), (0, 0));
    assert_eq!(m.matrix.gain(0, 0), 1.0);
}

#[test]
fn a_no_op_change_does_not_extend_the_ramp() {
    let mut m = mix();
    to(&mut m, 9);
    assert!(m.push(change(1.0, 10 * B)));
    to(&mut m, 10);
    assert_eq!(m.applied, 1);
    assert_eq!(m.ramp_end, 0);
    assert!(!m.ramping(10));
}

#[test]
fn pending_is_bounded_and_ordered() {
    let mut m = mix();
    for i in 0..PENDING_CHANGES as u64 {
        assert!(m.push(change(0.5, (100 - i) * B)));
    }
    assert!(!m.push(change(0.5, 200 * B)));
    assert!(m.pending.windows(2).all(|w| w[0].at_index <= w[1].at_index));
}

/// A page's fingerprint covers the entries applied at an index in
/// it, whatever order they arrived in: not one reported late, nor one
/// of the next page.
#[test]
fn a_page_fingerprints_the_entries_applied_in_it() {
    let mut m = mix();
    m.page_entries = Vec::with_capacity(PENDING_CHANGES * PAGE.div_ceil(B) as usize);
    // Page 3 is blocks 64 to 85; block 86 starts in page 4.
    let (a, b, c) = (
        change(0.5, 66 * B),
        change(0.25, 70 * B),
        change(0.75, 86 * B),
    );
    assert!(m.push(c) && m.push(b) && m.push(a));
    for k in 64..=70 {
        assert!(to(&mut m, k).is_empty());
    }
    let late = change(1.0, 65 * B);
    assert!(m.push(late));
    assert_eq!(to(&mut m, 71), [(65 * B, 71 * B)]);
    for k in 72..=85 {
        to(&mut m, k);
    }
    let want = crate::blocks::fingerprint(&mut [(a.id, a.at_index), (b.id, b.at_index)]);
    assert_eq!(m.fingerprint(), Some(want));
    for k in 86..=106 {
        to(&mut m, k);
    }
    let want = crate::blocks::fingerprint(&mut [(c.id, c.at_index)]);
    assert_eq!(m.fingerprint(), Some(want));
}

/// A page with more entries than its list holds has no fingerprint.
#[test]
fn a_page_with_entries_lost_has_no_fingerprint() {
    let mut m = mix();
    m.page_entries = Vec::with_capacity(1);
    assert!(m.push(change(0.5, 66 * B)) && m.push(change(0.25, 67 * B)));
    for k in 64..=85 {
        to(&mut m, k);
    }
    assert_eq!(m.fingerprint(), None);
    to(&mut m, 86);
    assert_eq!(m.fingerprint(), Some(crate::blocks::HEALTHY));
}

/// Entries at one index apply in log order, whatever order they
/// arrived in.
#[test]
fn entries_at_one_index_are_in_log_order() {
    let mut m = mix();
    let (a, b) = (change(0.5, 10 * B), change(0.25, 10 * B));
    assert!(m.push(b) && m.push(a));
    let ids: Vec<_> = m.pending.iter().map(|e| e.id).collect();
    assert_eq!(ids, [a.id, b.id]);
    assert!(to(&mut m, 10).is_empty());
    // Past the ramp: the later entry's target holds.
    to(&mut m, 30);
    assert_eq!(m.matrix.gain(0, 0), 0.25);
}

fn replica() -> Replica {
    let (changes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, cores) = rtrb::RingBuffer::new(1);
    let (copies, _) = rtrb::RingBuffer::new(1);
    let (_, handovers) = rtrb::RingBuffer::new(1);
    let (handed, _) = rtrb::RingBuffer::new(1);
    Replica {
        shared: Arc::new(Shared {
            fence: AtomicU64::new(0),
            ranks: AtomicU64::new(0),
            self_report: AtomicBool::new(false),
            diverged: AtomicBool::new(false),
            steps_cleared: AtomicU64::new(0),
            statuses: Vec::new(),
            started: Started::new(),
        }),
        changes,
        control,
        unresolved: Vec::with_capacity(UNRESOLVED),
        ids: Vec::with_capacity(UNRESOLVED + PENDING_CHANGES),
        late_routes: Vec::with_capacity(PENDING_CHANGES),
        late_lost_until: 0,
        hashes,
        cores,
        copies,
        handovers,
        handed,
    }
}

fn slot(up: bool, held: bool, fade: Option<u64>, silenced: bool, xfade: Option<u64>) -> Slot {
    Slot {
        flow: 0,
        domain: 0,
        first_channel: 0,
        channels: 1,
        offset: 0,
        state: SlotState::default(),
        fade_start: fade,
        silenced_last: silenced,
        up,
        held,
        xfade,
        prev: SlotState::default(),
        detached: false,
        prune_requested: false,
        stats: EMPTY_STATS,
        strip: None,
        strip_pos: None,
        delay: None,
        tone: None,
        pre_strip: false,
        pre_strip_on: false,
        pre_row: None,
    }
}

/// Every field of the core is copied: each set to a value other than
/// its default, the copy equals a core built from those values.
#[test]
fn the_core_copy_takes_every_field() {
    let mut m = mix();
    m.page_entries = Vec::with_capacity(4);
    assert!(m.matrix.set(0, 0, 0.5, 480));
    m.matrix.advance(100);
    m.pos = Some(20 * B);
    m.ramp_end = 19 * B + 380;
    assert!(m.push(change(0.25, 40 * B)));
    assert!(m.push(entry(Change::InputDown { slot: 1 }, 41 * B)));
    m.page_entries.extend([(3, 2 * B), (4, 3 * B)]);
    m.entries_page = Some(1);
    m.entries_lost = true;
    // Meters on both words of the mask paused.
    m.paused = (1 << 2) | (1 << 100);
    let mut slots = [
        slot(true, true, Some(5 * B), false, Some(18 * B)),
        slot(false, false, None, true, None),
    ];
    let mut strip = Strip::new(1, 480);
    assert!(strip.set(StripParam::Trim { db: -6.0 }));
    strip.process(&mut [0.5; 48], 48);
    slots[0].strip = Some(strip.clone());
    slots[0].strip_pos = Some(20 * B);
    slots[1].detached = true;
    slots[1].pre_strip_on = true;
    // A tone generator mid-crossfade, its level ramping.
    let mut tone = Tone::new(1, 48_000, 480);
    let lineup = ToneParams {
        frequency_hz: 997,
        ..ToneParams::DEFAULT
    };
    assert!(tone.set(19 * B, lineup));
    slots[1].tone = Some(tone);
    let awaiting = [entry(Change::InputUp { slot: 0 }, 19 * B)];
    // Detector 5 keyed by slot 1, mid-envelope after a burst.
    let keys = crate::ducks::KeySet::of(&[(1, 2.0)]).unwrap();
    let keyed = crate::ducks::DuckChange::Keys(keys);
    assert!(m.ducks.core.set(19 * B, 5, keyed, |_| 1));
    let mut burst = PlanarBuffer::new(1, B as usize);
    burst.channel_mut(0).fill(0.5);
    let mut envelopes = PlanarBuffer::new(MAX_DUCKS, B as usize);
    m.ducks
        .process(19 * B, B as usize, |_| 0, &burst, &mut envelopes, None);
    assert!(m.ducks.core.detectors[5].is_engaged());
    let ducks = m.ducks.core.clone();
    assert_ne!(ducks, crate::ducks::DuckCore::new(480));
    let mut r = replica();
    r.unresolved.extend([(8, 21 * B), (9, 30 * B)]);
    r.late_routes.push((10, 25 * B));
    r.late_lost_until = 26 * B;

    let out = |enabled| Out {
        first_channel: 0,
        channels: 1,
        writer: None,
        enabled,
        first_k: 0,
        meter: OutMeter::new(1),
        hold: crate::blocks::Hold::new(1, B, 0, true),
        pages: None,
        strip: None,
        strip_pos: None,
        delay: None,
        limiter: None,
    };
    let mut outs = [out(true), out(false), out(true)];
    // A limiter mid-reduction: its rings and released gain are core.
    let mut limiter = Limiter::new(1, 480);
    assert!(limiter.set(LimiterParams {
        enabled: true,
        ceiling_db: -6.0,
        release_ms: 50.0,
    }));
    limiter.process(0, &mut [0.9; 96], 96);
    outs[1].limiter = Some(limiter.clone());
    // An output strip mid-ramp: its ramps and position are core.
    let mut out_strip = Strip::new(1, 480);
    assert!(out_strip.set(StripParam::Trim { db: 6.0 }));
    out_strip.process(&mut [0.5; 48], 48);
    outs[2].strip = Some(out_strip.clone());
    outs[2].strip_pos = Some(20 * B);
    // An output delay mid-crossfade, another waiting: its taps,
    // position, floor and ring set are core, its history copied off
    // the RT thread.
    let mut line = Delay::new(samplez_dsp::DelayRings::new(1, 100, 1, B as usize), 480);
    assert!(line.set(0, 30) && line.set(B, 60));
    line.process(0, &mut [0.5; 96], 96);
    let line_core = DelayCore {
        taps: line.taps(),
        pos: Some(96),
        floor: line.floor(),
        set: 0,
        span: Vec::new(),
    };
    assert_eq!(line_core.taps.next, Some(60));
    assert!(line_core.floor > 0);
    outs[0].delay = Some(line);

    let mut c = Core::new(1, 1, 2, 4, 3);
    capture_core(&mut c, &m, &slots, &outs, &awaiting, Some(11), &r);
    let want = Core {
        matrix: m.matrix.clone(),
        ducks,
        pos: Some(20 * B),
        ramp_end: 19 * B + 380,
        pending: m.pending.clone(),
        page_entries: vec![(3, 2 * B), (4, 3 * B)],
        entries_page: Some(1),
        entries_lost: true,
        outputs_disabled: 0b010,
        loudness_paused: (1 << 2) | (1 << 100),
        pre_strip_meters: 0b10,
        slots: vec![
            SlotCore {
                up: true,
                held: true,
                fade_start: Some(5 * B),
                silenced_last: false,
                xfade: Some(18 * B),
                detached: false,
                strip: Some(strip),
                strip_pos: Some(20 * B),
                delay: None,
                tone: None,
            },
            SlotCore {
                up: false,
                held: false,
                fade_start: None,
                silenced_last: true,
                xfade: None,
                detached: true,
                strip: None,
                strip_pos: None,
                delay: None,
                tone: Some(tone),
            },
        ],
        limiters: vec![None, Some(limiter), None],
        outs: vec![
            (
                0,
                OutCore {
                    strip: None,
                    strip_pos: None,
                    delay: Some(line_core),
                },
            ),
            (
                2,
                OutCore {
                    strip: Some(out_strip),
                    strip_pos: Some(20 * B),
                    delay: None,
                },
            ),
        ],
        awaiting_gen: awaiting.to_vec(),
        pending_gen: Some(11),
        unresolved: vec![(8, 21 * B), (9, 30 * B)],
        late_routes: vec![(10, 25 * B)],
        late_lost_until: 26 * B,
        batches: Vec::new(),
        handed: Default::default(),
    };
    assert_eq!(c, want);
    assert_eq!(c.pending.len(), 2);
}

/// The digest covers the inputs whose read failed, by crossfade leg,
/// the entries applied without their generation, a late re-route until
/// its crossfade's end and the unresolved entries at or below the
/// block's start, sorted; a healthy block has the empty digest, a
/// clock self-report none.
#[test]
fn the_digest_names_every_self_known_degradation() {
    use crate::blocks::{HEALTHY, degraded_digest};
    let mut r = replica();
    assert_eq!(digest(&mut r, &[], 10 * B, 0, 0), Some(HEALTHY));
    assert_eq!(
        digest(&mut r, &[], 10 * B, 0b101, 0),
        Some(degraded_digest(&[], 0b101, 0))
    );
    assert_eq!(
        digest(&mut r, &[], 10 * B, 0, 0b1),
        Some(degraded_digest(&[], 0, 0b1))
    );
    r.late_routes.push((5, 12 * B));
    assert_eq!(
        digest(&mut r, &[], 11 * B, 0, 0),
        Some(degraded_digest(&[5], 0, 0))
    );
    assert_eq!(digest(&mut r, &[], 12 * B, 0, 0), Some(HEALTHY));
    assert!(r.late_routes.is_empty());
    r.unresolved = vec![(3, 10 * B), (9, 11 * B)];
    let awaiting = [entry(Change::InputUp { slot: 0 }, 5 * B)];
    let want = {
        let mut ids = vec![3, awaiting[0].id];
        ids.sort_unstable();
        degraded_digest(&ids, 0, 0)
    };
    // Entry 9 is not due in the block at 10B.
    assert_eq!(digest(&mut r, &awaiting, 10 * B, 0, 0), Some(want));
    assert_ne!(digest(&mut r, &awaiting, 11 * B, 0, 0), Some(want));
    r.shared.self_report.store(true, Relaxed);
    assert_eq!(digest(&mut r, &awaiting, 10 * B, 0, 0), None);
    r.shared.self_report.store(false, Relaxed);
    r.unresolved.clear();
    // A late re-route the full list has no room for still degrades
    // the blocks of its window.
    for id in 0..PENDING_CHANGES as u64 {
        assert!(note_late_route(&mut r, id, 13 * B));
    }
    assert!(!note_late_route(&mut r, 99, 20 * B));
    assert_ne!(digest(&mut r, &[], 15 * B, 0, 0), Some(HEALTHY));
    assert_eq!(digest(&mut r, &[], 20 * B, 0, 0), Some(HEALTHY));
}

/// A batch's gains apply at its index, all of them, and the batch is
/// due once for the rest; one the matrix passed sets no gain and is
/// due late, once.
#[test]
fn a_batch_sets_its_gains_at_its_index() {
    let mut m = Mix::new(Matrix::new(2, 2), B, 480);
    let changes = [
        Change::Gain {
            input: 0,
            output: 1,
            gain: 0.5,
            fade: None,
        },
        Change::Gain {
            input: 1,
            output: 0,
            gain: 0.25,
            fade: None,
        },
    ];
    let items = changes.map(crate::batch::Item::Change);
    let (change, batch) = crate::batch::fold(&items, 2, 4).unwrap().unwrap();
    let e = RtEntry {
        id: 7,
        at_index: 10 * B,
        change,
    };
    assert!(m.push_with(e, batch.clone()).is_ok());
    to(&mut m, 9);
    assert_eq!((m.matrix.gain(0, 1), m.matrix.gain(1, 0)), (0.0, 0.0));
    to(&mut m, 10);
    assert_eq!(m.due.as_slice(), [(e, false)]);
    to(&mut m, 30);
    assert_eq!((m.matrix.gain(0, 1), m.matrix.gain(1, 0)), (0.5, 0.25));
    assert!(m.take_batch(7).is_some());
    // Past its index: nothing set, due late.
    let late = RtEntry { id: 8, ..e };
    let mut m2 = Mix::new(Matrix::new(2, 2), B, 480);
    to(&mut m2, 11);
    assert!(m2.push_with(late, batch).is_ok());
    to(&mut m2, 12);
    assert_eq!(m2.due.as_slice(), [(late, true)]);
    to(&mut m2, 40);
    assert_eq!((m2.matrix.gain(0, 1), m2.matrix.gain(1, 0)), (0.0, 0.0));
}

/// Input entries leave the pending list at the block of their index,
/// in order, flagged late when the matrix produced their index already.
#[test]
fn input_entries_are_due_at_their_block() {
    let mut m = mix();
    let down = entry(Change::InputDown { slot: 1 }, 10 * B);
    let route = entry(Change::Route { slot: 0 }, 12 * B);
    assert!(m.push(route) && m.push(down));
    to(&mut m, 9);
    assert!(m.due.is_empty());
    to(&mut m, 10);
    assert_eq!(m.due.len(), 1);
    assert_eq!((m.due[0].0.id, m.due[0].1), (down.id, false));
    // A forward step past its index applies it on the next block.
    to(&mut m, 14);
    assert_eq!((m.due[0].0.id, m.due[0].1), (route.id, false));
    // One arriving after the matrix produced its index is late.
    let up = entry(Change::InputUp { slot: 1 }, 14 * B);
    assert!(m.push(up));
    to(&mut m, 15);
    assert_eq!((m.due[0].0.id, m.due[0].1), (up.id, true));
    assert!(m.pending.is_empty());
}

/// A mono slot whose strip ramps over 480 samples.
fn strip_slot() -> Slot {
    Slot::new(0, 0, 1, 0, 480)
}

/// A strip runs in the loop as alone, block by block, with entries set
/// at their index: a slot that skipped blocks, an entry's index among
/// them, computes the block after the skip as a strip advanced to the
/// index, set, and advanced on. Ramps, the dynamics order's crossfade
/// and the compressor's makeup are running across the skip, and over
/// silence they land where processing the skipped samples puts them.
/// Below its position a strip re-anchors and rewinds nothing.
#[test]
fn a_strip_advances_over_samples_not_processed() {
    let comp = |makeup_db| {
        StripParam::Comp(samplez_dsp::CompParams {
            enabled: true,
            threshold_db: -30.0,
            ratio: 4.0,
            attack_ms: 1.0,
            release_ms: 40.0,
            knee_db: 6.0,
            makeup_db,
            mix: 1.0,
        })
    };
    let first = [
        StripParam::Eq { enabled: true },
        StripParam::EqBand {
            band: 1,
            value: samplez_dsp::EqBand {
                kind: samplez_dsp::BandType::Peak,
                freq: 1000.0,
                gain_db: 18.0,
                q: 2.0,
            },
        },
        comp(6.0),
    ];
    // Inside the skip of blocks 2..8.
    let second = [
        StripParam::Trim { db: -6.0 },
        StripParam::DynamicsBeforeEq { enabled: true },
        comp(18.0),
    ];
    let block = |k: usize| -> Vec<f32> {
        (k * 48..(k + 1) * 48)
            .map(|n| 0.8 * (n as f32 * 0.21).sin())
            .collect()
    };
    let mut slot = strip_slot();
    let mut reference = Strip::new(1, 480);
    let mut got = Vec::new();
    for p in first {
        assert!(slot.set_strip(0, p) && reference.set(p));
    }
    for k in [0, 1] {
        let (mut a, mut b) = (block(k), block(k));
        slot.run_strip(k as u64 * 48, &mut a, 48);
        reference.process(&mut b, 48);
        assert_eq!(a, b, "block {k}");
        got.push(a);
    }
    for p in second {
        assert!(slot.set_strip(192, p));
    }
    reference.advance(192 - 96);
    for p in second {
        assert!(reference.set(p));
    }
    reference.advance(384 - 192);
    let (mut a, mut b) = (block(8), block(8));
    slot.run_strip(384, &mut a, 48);
    reference.process(&mut b, 48);
    assert_eq!(a, b);
    assert_eq!(slot.strip.as_ref(), Some(&reference));
    // The changes ramp still: the reference is not a settled strip.
    let mut settled = reference.clone();
    settled.advance(1 << 20);
    let mut c = block(9);
    let mut d = block(9);
    reference.process(&mut c, 48);
    settled.process(&mut d, 48);
    assert_ne!(c, d);

    let held = slot.strip.clone();
    strip_to(&mut slot.strip, &mut slot.strip_pos, 96);
    assert_eq!((slot.strip, slot.strip_pos), (held, Some(96)));

    // Over silence, filter and envelope states stay at zero, so a
    // strip that processed the skipped samples is where one advanced
    // over them is: ramps, the order's crossfade and the makeup gain,
    // all ending inside the skip of blocks 2..18.
    let mut slot = strip_slot();
    let mut processed = Strip::new(1, 480);
    for p in &first {
        assert!(slot.set_strip(0, *p) && processed.set(*p));
    }
    let mut silence = vec![0.0f32; 192];
    for k in [0, 1] {
        slot.run_strip(k * 48, &mut silence[..48], 48);
    }
    processed.process(&mut silence, 192);
    for p in second {
        assert!(slot.set_strip(192, p) && processed.set(p));
    }
    let mut silence = vec![0.0f32; 864 - 192];
    processed.process(&mut silence, 864 - 192);
    let (mut a, mut b) = (block(18), block(18));
    slot.run_strip(864, &mut a, 48);
    processed.process(&mut b, 48);
    assert_eq!(a, b);
    assert_eq!(slot.strip.as_ref(), Some(&processed));
}

/// A mono output of strip and limiter as configured, its strip ramping
/// over 480 samples.
fn strip_out(limiter: bool) -> Out {
    Out {
        first_channel: 0,
        channels: 1,
        writer: None,
        enabled: true,
        first_k: 0,
        meter: OutMeter::new(1),
        hold: crate::blocks::Hold::new(1, B, 0, true),
        pages: None,
        strip: Some(Strip::new(1, 480)),
        strip_pos: None,
        delay: None,
        limiter: limiter.then(|| Limiter::new(1, 480)),
    }
}

/// An output's strip runs in the loop as a slot's: an output that
/// skipped blocks, an entry's index among them, computes the block
/// after the skip as a strip advanced to the index, set, and advanced
/// on, its ramps running across the skip.
#[test]
fn an_output_strip_advances_over_samples_not_processed() {
    let block = |k: u64| -> Vec<f32> {
        (k * B..(k + 1) * B)
            .map(|n| 0.8 * (n as f32 * 0.21).sin())
            .collect()
    };
    let mut out = strip_out(false);
    let mut reference = Strip::new(1, 480);
    let mut buf = PlanarBuffer::new(1, B as usize);
    let mut scratch = vec![0.0f32; B as usize];
    // Each block's input computed once and given to both, so the
    // comparison does not depend on how often `sin` is evaluated.
    let mut run = |out: &mut Out, k: u64, input: &[f32]| {
        buf.channel_mut(0)[..B as usize].copy_from_slice(input);
        out.process(k * B, &mut buf, &mut scratch, B as usize);
        buf.channel(0)[..B as usize].to_vec()
    };
    let first = StripParam::Trim { db: -12.0 };
    assert!(out.set_strip(0, first) && reference.set(first));
    for k in [0, 1] {
        let input = block(k);
        let mut want = input.clone();
        reference.process(&mut want, B as usize);
        assert_eq!(run(&mut out, k, &input), want, "block {k}");
    }
    // Inside the skip of blocks 2..8.
    let second = StripParam::Trim { db: 6.0 };
    assert!(out.set_strip(4 * B, second));
    reference.advance(2 * B as usize);
    assert!(reference.set(second));
    reference.advance(4 * B as usize);
    let input = block(8);
    let mut want = input.clone();
    reference.process(&mut want, B as usize);
    assert_eq!(run(&mut out, 8, &input), want);
    assert_eq!(out.strip.as_ref(), Some(&reference));
    assert_eq!(out.strip_pos, Some(9 * B));
    // The change ramps still: the reference is not a settled strip.
    let mut settled = reference.clone();
    settled.advance(1 << 20);
    let (mut c, mut d) = (block(9), block(9));
    reference.process(&mut c, B as usize);
    settled.process(&mut d, B as usize);
    assert_ne!(c, d);
}

/// An output's strip runs before its limiter: a strip raising the
/// output 18 dB above the limiter's ceiling leaves no sample above it
/// once the limiter is on, and is heard, the output at the ceiling
/// rather than at the matrix's level below it.
#[test]
fn an_output_strip_runs_before_the_limiter() {
    let mut out = strip_out(true);
    assert!(out.set_strip(0, StripParam::Trim { db: 18.0 }));
    let ceiling_db = -6.0;
    let params = LimiterParams {
        enabled: true,
        ceiling_db,
        release_ms: 50.0,
    };
    assert!(out.limiter.as_mut().unwrap().set(params));
    let ceiling = 10f32.powf(ceiling_db / 20.0);
    let mut buf = PlanarBuffer::new(1, B as usize);
    let mut scratch = vec![0.0f32; B as usize];
    let mut loudest = 0.0f32;
    for k in 0..200 {
        let at = |i: usize| (k * B + i as u64) as f32;
        for (i, x) in buf.channel_mut(0)[..B as usize].iter_mut().enumerate() {
            *x = 0.25 * (at(i) * 0.05).sin();
        }
        out.process(k * B, &mut buf, &mut scratch, B as usize);
        // Past the ramps of the trim and of switching the limiter on.
        if k >= 20 {
            for &x in &buf.channel(0)[..B as usize] {
                let within = ceiling * (1.0 + 1e-6);
                assert!(x.abs() <= within, "{x} above {ceiling} in block {k}");
                loudest = loudest.max(x.abs());
            }
        }
    }
    assert!(loudest > 0.4, "the strip unheard: {loudest}");
}

/// An output's delay line runs after its strip and before its limiter:
/// the output computes, bit for bit, a strip, a delay and a limiter run
/// alone in that order. The strip's trim and the limiter's switching
/// on both ramp from index 0 while the line crossfades to a delay of
/// 1000 samples, so the samples coming out of the line after it differ
/// in either other order: delayed before the strip, they meet the
/// trim settled; delayed after the limiter, they come out limited as
/// it was while it ramped on.
#[test]
fn an_output_delay_runs_between_the_strip_and_the_limiter() {
    let line = || Delay::new(samplez_dsp::DelayRings::new(1, 2400, 1, B as usize), 480);
    let trim = StripParam::Trim { db: 6.0 };
    let params = LimiterParams {
        enabled: true,
        ceiling_db: -6.0,
        release_ms: 50.0,
    };
    let mut out = strip_out(true);
    assert!(out.set_strip(0, trim));
    let mut delay = line();
    assert!(delay.set(0, 1000));
    out.delay = Some(delay);
    assert!(out.limiter.as_mut().is_some_and(|l| l.set_at(0, params)));
    let (mut strip, mut delay, mut limiter) = (Strip::new(1, 480), line(), Limiter::new(1, 480));
    assert!(strip.set(trim) && delay.set(0, 1000) && limiter.set_at(0, params));
    let blocks = 60;
    // The input computed once and given to both.
    let input: Vec<f32> = (0..blocks * B)
        .map(|n| 0.8 * (n as f32 * 0.05).sin())
        .collect();
    let mut buf = PlanarBuffer::new(1, B as usize);
    let mut scratch = vec![0.0f32; B as usize];
    for (k, block) in input.chunks_exact(B as usize).enumerate() {
        let start = k as u64 * B;
        buf.channel_mut(0)[..B as usize].copy_from_slice(block);
        out.process(start, &mut buf, &mut scratch, B as usize);
        let mut want = block.to_vec();
        strip.process(&mut want, B as usize);
        delay.process(start, &mut want, B as usize);
        limiter.process(start, &mut want, B as usize);
        let bits = |x: &[f32]| x.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
        assert_eq!(
            bits(&buf.channel(0)[..B as usize]),
            bits(&want),
            "block {k}"
        );
    }
    // Past the delay the line plays what it took, limited.
    let last = &buf.channel(0)[..B as usize];
    assert!(last.iter().any(|v| v.abs() > 0.3), "{last:?}");
}

/// A strip's gate and compressor compute their gains on the grid of
/// the sample index wherever the slot's strip started: a slot whose
/// strip starts at a block off that grid computes the bits of one
/// that ran from index 0.
#[test]
fn strip_dynamics_follow_the_sample_index() {
    let comp = StripParam::Comp(samplez_dsp::CompParams {
        enabled: true,
        threshold_db: -30.0,
        ratio: 4.0,
        attack_ms: 1.0,
        release_ms: 40.0,
        knee_db: 6.0,
        makeup_db: 0.0,
        mix: 1.0,
    });
    let b = 50;
    let block = |k: usize| -> Vec<f32> {
        (k * b..(k + 1) * b)
            .map(|n| 0.8 * (n as f32 * 0.21).sin())
            .collect()
    };
    let (mut early, mut late) = (Slot::new(0, 0, 1, 0, 0), Slot::new(0, 0, 1, 0, 0));
    assert!(early.set_strip(0, comp));
    for k in 0..3 {
        early.run_strip((k * b) as u64, &mut [0.0; 50], b);
    }
    assert!(late.set_strip(3 * b as u64, comp));
    for k in 3..40 {
        let (mut x, mut y) = (block(k), block(k));
        early.run_strip((k * b) as u64, &mut x, b);
        late.run_strip((k * b) as u64, &mut y, b);
        assert_eq!(x, y, "block {k}");
    }
}

/// A strip refuses what it does not take, and a slot wider than a
/// strip has none.
#[test]
fn a_slot_refuses_what_its_strip_does_not_take() {
    let mut slot = strip_slot();
    assert!(!slot.set_strip(0, StripParam::LegTrim { leg: 1, db: 0.0 }));
    assert_eq!(slot.strip, Some(Strip::new(1, 480)));
    let mut wide = Slot::new(0, 0, MAX_LEGS + 1, 0, 480);
    assert!(wide.strip.is_none());
    assert!(!wide.set_strip(0, StripParam::Trim { db: 0.0 }));
}

/// The duck rows of the desk below: a mono key on slot 0 and a mono
/// target on slot 1.
const DESK_ROWS: [(usize, usize); 2] = [(0, 1), (1, 1)];

/// A desk computed off the RT thread as the RT loop computes it: the
/// entries due at each block's start applied, the inputs read from a
/// function of the sample index, then the detectors and the matrix.
/// Two inputs, two outputs: output 0 carries the target ducked by
/// group 0, output 1 the target and the key undocked.
struct Desk {
    mix: Mix,
    inbuf: PlanarBuffer,
    outbuf: PlanarBuffer,
    b: u64,
}

impl Desk {
    fn new(b: u64) -> Desk {
        Desk {
            mix: Mix::new(Matrix::new(2, 2), b, 480),
            inbuf: PlanarBuffer::new(2, b as usize),
            outbuf: PlanarBuffer::new(2, b as usize),
            b,
        }
    }

    /// Queues log entry `id`, its entries folded, at `at`.
    fn entry(&mut self, id: LogId, at: u64, entries: &[crate::engine::Entry]) {
        let mut items = Vec::new();
        for e in entries {
            crate::transfer::items_of(e, &DESK_ROWS, &mut items).unwrap();
        }
        let (change, batch) = crate::batch::fold(&items, 2, 4).unwrap().unwrap();
        let e = RtEntry {
            id,
            at_index: at,
            change,
        };
        assert!(self.mix.push_with(e, batch).is_ok());
    }

    /// Computes blocks `ks`, input channel `c` at index `n` being
    /// `input(c, n)`; the output channels' samples.
    fn run(&mut self, ks: Range<u64>, input: impl Fn(usize, u64) -> f32) -> [Vec<f32>; 2] {
        let b = self.b as usize;
        let mut out = [Vec::new(), Vec::new()];
        for k in ks {
            let start = k * self.b;
            self.mix.to(k, |e, _| panic!("late {e:?}"));
            for n in 0..self.mix.due.len() {
                let (e, late) = self.mix.due[n];
                assert!(!late);
                if let Some(batch) = self.mix.take_batch(e.id) {
                    assert_eq!(self.mix.apply_ducks(e.at_index, &batch, |_| 1), 0);
                }
            }
            self.mix.due.clear();
            for c in 0..2 {
                for (i, x) in self.inbuf.channel_mut(c)[..b].iter_mut().enumerate() {
                    *x = input(c, start + i as u64);
                }
            }
            let rows = |s: usize| DESK_ROWS[s].0;
            self.mix
                .block(start, b, rows, &self.inbuf, &mut self.outbuf, None);
            for (c, o) in out.iter_mut().enumerate() {
                o.extend_from_slice(&self.outbuf.channel(c)[..b]);
            }
        }
        out
    }

    /// The core of the desk after its last block, encoded and decoded
    /// as a joiner takes it from a donor.
    fn copy(&self, k0: u64) -> Core {
        let slots = [
            slot(true, false, None, false, None),
            slot(true, false, None, false, None),
        ];
        let mut c = Core::new(2, 2, 2, 0, 1);
        capture_core(&mut c, &self.mix, &slots, &[], &[], None, &replica());
        let copy = CoreCopy {
            k0,
            last_id: 0,
            core: Box::new(c),
        };
        *CoreCopy::decode(&copy.encode()).unwrap().core
    }

    /// Takes core `c` as the RT thread installs one: its matrix,
    /// detectors, position and pending entries.
    fn install(&mut self, c: &mut Core) {
        self.mix.matrix.copy_from(&c.matrix);
        self.mix.ducks.core.copy_from(&c.ducks);
        self.mix.pos = c.pos;
        self.mix.ramp_end = c.ramp_end;
        self.mix.pending.clear();
        for i in 0..c.pending.len() {
            let e = c.pending[i];
            let b = c.take_batch(e.id);
            assert!(self.mix.push_with(e, b).is_ok());
        }
    }
}

/// A key like speech on input 0, levels in 30 ms segments around the
/// default threshold, and a tone on input 1; exact per index.
fn desk_input(c: usize, n: u64) -> f32 {
    if c == 1 {
        return 0.25 * (n as f32 * 0.07).sin();
    }
    let mut x = (n / 1440).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03;
    x ^= x >> 29;
    let db = -42.0 + (x % 24) as f32;
    let level = 10f32.powf(db / 20.0);
    let mut y = n.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    y ^= y >> 31;
    level * ((y % 2001) as f32 / 1000.0 - 1.0)
}

/// The entries of a ducked desk at index 0: the gains, group 0 keyed
/// by slot 0 with short times, slot 1 ducked 20 dB on output 0.
fn duck_entries() -> Vec<crate::engine::Entry> {
    use crate::ducks::DuckSetting;
    use crate::engine::Entry;
    let gain = |input, output, gain| Entry::Gain {
        input,
        output,
        gain,
        fade: None,
    };
    let duck = |setting| Entry::Duck { group: 0, setting };
    vec![
        gain(1, 0, 1.0),
        gain(1, 1, 1.0),
        gain(0, 1, 0.5),
        duck(DuckSetting::Detector(Box::new(
            samplez_dsp::DetectorParams {
                attack_ms: 10.0,
                hold_ms: 50.0,
                release_ms: 200.0,
                ..samplez_dsp::DetectorParams::DEFAULT
            },
        ))),
        duck(DuckSetting::Keys(vec![(0, 1.0)])),
        duck(DuckSetting::Depth { slot: 1, db: 20.0 }),
        Entry::DuckMask {
            input: 1,
            output: 0,
            mask: 1,
        },
    ]
}

fn bits(x: &[f32]) -> Vec<u32> {
    x.iter().map(|v| v.to_bits()).collect()
}

/// Indices both block sizes below start blocks at: 48 x 55.
const BOTH: u64 = 2640;

/// Two desks of block sizes 48 and 55 given the same input and
/// entries compute the same bits while ducking, by the key and by the
/// manual controls; the column the mask leaves out carries what a
/// desk without ducks carries.
#[test]
fn ducking_does_not_depend_on_the_block_size() {
    use crate::ducks::DuckSetting;
    use crate::engine::Entry;
    use samplez_dsp::DuckMode;
    let duck = |setting| Entry::Duck { group: 0, setting };
    let scripted = |b: u64| {
        let mut d = Desk::new(b);
        d.entry(1, 0, &duck_entries());
        d.entry(2, 10 * BOTH, &[duck(DuckSetting::Mode(DuckMode::On))]);
        d.entry(3, 20 * BOTH, &[duck(DuckSetting::Mode(DuckMode::Off))]);
        d.entry(4, 25 * BOTH, &[duck(DuckSetting::Hold { held: true })]);
        d.entry(5, 27 * BOTH, &[duck(DuckSetting::Hold { held: false })]);
        d.entry(6, 28 * BOTH, &[duck(DuckSetting::Mode(DuckMode::Auto))]);
        d.run(0..40 * BOTH / b, desk_input)
    };
    let (a, b) = (scripted(48), scripted(55));
    assert!(bits(&a[0]) == bits(&b[0]), "the ducked output");
    assert!(bits(&a[1]) == bits(&b[1]), "the undocked output");
    // It ducked, by the key and by hand, and recovered.
    let ratio = |n: usize| a[0][n] / desk_input(1, n as u64);
    let full = samplez_dsp::duck_gain(20.0, 1.0);
    let at = |i: u64| (i * BOTH) as usize;
    assert!(
        (0..at(10)).any(|n| ratio(n).abs() < 0.5),
        "no automatic duck"
    );
    assert!(((at(10) + 9600)..at(20)).all(|n| (ratio(n) - full).abs() < 1e-3));
    // Released: 200 ms and two intervals after mode Off.
    assert!(((at(20) + 9616)..at(25)).all(|n| (ratio(n) - 1.0).abs() < 1e-6));
    assert!(((at(25) + 2400)..at(27)).all(|n| (ratio(n) - full).abs() < 1e-3));
    let mut plain = Desk::new(48);
    plain.entry(1, 0, &duck_entries()[..3]);
    let plain = plain.run(0..40 * BOTH / 48, desk_input);
    assert!(bits(&a[1]) == bits(&plain[1]), "the column not ducked");
    assert!(bits(&a[0]) != bits(&plain[0]));
}

/// A joiner taking a donor's core mid-duck, the envelope between 0
/// and 1 and the key filters ringing, computes the donor's bits from
/// there on; one keeping its own detectors does not.
#[test]
fn a_core_copied_mid_duck_continues_alike() {
    use crate::ducks::DuckSetting;
    use crate::engine::Entry;
    use samplez_dsp::DuckMode;
    let mut donor = Desk::new(55);
    donor.entry(1, 0, &duck_entries());
    let slow = samplez_dsp::DetectorParams {
        attack_ms: 500.0,
        release_ms: 2000.0,
        ..samplez_dsp::DetectorParams::DEFAULT
    };
    let group1 = |setting| Entry::Duck { group: 1, setting };
    donor.entry(
        2,
        BOTH,
        &[
            group1(DuckSetting::Detector(Box::new(slow))),
            group1(DuckSetting::Mode(DuckMode::On)),
            group1(DuckSetting::Depth { slot: 1, db: 30.0 }),
            Entry::DuckMask {
                input: 1,
                output: 0,
                mask: 0b11,
            },
        ],
    );
    let mut joiner = Desk::new(55);
    joiner.entry(1, 0, &duck_entries());
    let k0 = 3 * BOTH / 55;
    donor.run(0..k0 + 1, desk_input);
    joiner.run(0..k0 + 1, |c, n| desk_input(c, n) * 0.5);
    let e = donor.mix.ducks.core.detectors[1].envelope();
    assert!(0.0 < e && e < 1.0, "envelope {e}");
    let mut core = donor.copy(k0);
    let mut stale = Desk::new(55);
    stale.entry(1, 0, &duck_entries());
    stale.run(0..k0 + 1, |c, n| desk_input(c, n) * 0.5);
    joiner.install(&mut core);
    // The stale joiner takes the matrix but keeps its detectors.
    let keep = stale.mix.ducks.core.clone();
    stale.install(&mut donor.copy(k0));
    stale.mix.ducks.core = keep;
    let ks = k0 + 1..k0 + 1 + 4 * BOTH / 55;
    let (a, b, c) = (
        donor.run(ks.clone(), desk_input),
        joiner.run(ks.clone(), desk_input),
        stale.run(ks, desk_input),
    );
    assert!(bits(&a[0]) == bits(&b[0]) && bits(&a[1]) == bits(&b[1]));
    assert!(
        bits(&a[0]) != bits(&c[0]),
        "the detectors made no difference"
    );
}

/// A core copied with no duck set carries no position in the interval:
/// the joiner starts from 0 where the donor is at the index's. Given
/// a mask, depth and manual duck on both sides at a block start off
/// the interval grid, both compute the same bits: the matrix is
/// aligned to the sample index before it mixes.
#[test]
fn a_core_copied_without_ducks_ducks_alike_once_given_a_mask() {
    use crate::ducks::DuckSetting;
    use crate::engine::Entry;
    use samplez_dsp::DuckMode;
    let gains = &duck_entries()[..3];
    let mut donor = Desk::new(55);
    donor.entry(1, 0, gains);
    let k0 = 20;
    donor.run(0..k0 + 1, desk_input);
    let core = donor.copy(k0);
    assert!(!core.matrix.has_ducks());
    let mut joiner = Desk::new(55);
    joiner.install(&mut donor.copy(k0));
    // Index 55 x 30 lies 2 off the interval grid.
    let at = 30 * 55;
    assert_ne!(at % u64::from(samplez_dsp::strip::GAIN_INTERVAL), 0);
    let duck = |setting| Entry::Duck { group: 3, setting };
    let set = [
        duck(DuckSetting::Mode(DuckMode::On)),
        duck(DuckSetting::Depth { slot: 1, db: 12.0 }),
        Entry::DuckMask {
            input: 1,
            output: 0,
            mask: 1 << 3,
        },
    ];
    donor.entry(2, at, &set);
    joiner.entry(2, at, &set);
    let ks = k0 + 1..k0 + 200;
    let (a, b) = (
        donor.run(ks.clone(), desk_input),
        joiner.run(ks, desk_input),
    );
    assert!(bits(&a[0]) == bits(&b[0]));
    assert!(bits(&a[0]) != bits(&a[1]), "nothing ducked");
}

/// Cores built from one product state at one index compute the same
/// bits whatever the buffer held before, and a group its manual
/// controls trigger ducks at full depth from the install on, its
/// coefficients fading in ducked.
#[test]
fn a_rebuilt_core_does_not_depend_on_the_buffer_and_ducks_forced_on_at_once() {
    use crate::ducks::DuckTargets;
    use samplez_dsp::DuckMode;
    let targets = [DuckTargets {
        keys: vec![(0, 1.0)],
        mode: DuckMode::On,
        depths: vec![0.0, 20.0],
        ..DuckTargets::default()
    }];
    let gains = [0.0, 0.5, 1.0, 1.0];
    let masks = [0, 0, 1, 0];
    let mut depths = [0.0; 2 * MAX_DUCKS];
    depths[MAX_DUCKS] = 20.0;
    let at = 101 * 48;
    let build = |d: &mut Desk| {
        let m = &mut d.mix;
        m.ducks.core.cold(480, &targets, |_| 1);
        m.ducks.core.settle(480, at);
        m.matrix.fade_in_ducked(&gains, &masks, &depths, 480);
        m.matrix.align(at);
        m.pos = Some(at);
        m.pending.clear();
        m.batches.clear();
    };
    // One desk ran ducks of its own before, another is new.
    let mut used = Desk::new(48);
    used.entry(1, 0, &duck_entries());
    used.run(0..100, desk_input);
    let mut fresh = Desk::new(48);
    build(&mut used);
    build(&mut fresh);
    assert_eq!(used.mix.ducks.core, fresh.mix.ducks.core);
    assert_eq!(fresh.mix.ducks.core.detectors[0].envelope(), 1.0);
    let (a, b) = (
        used.run(101..200, desk_input),
        fresh.run(101..200, desk_input),
    );
    assert!(bits(&a[0]) == bits(&b[0]) && bits(&a[1]) == bits(&b[1]));
    // Output 0 is the target fading in, ducked 20 dB from the end of
    // the first two intervals, against a desk built without masks.
    let mut plain = Desk::new(48);
    plain.mix.ducks.core.cold(480, &targets, |_| 1);
    plain.mix.ducks.core.settle(480, at);
    plain
        .mix
        .matrix
        .fade_in_ducked(&gains, &[0; 4], &depths, 480);
    plain.mix.matrix.align(at);
    plain.mix.pos = Some(at);
    let p = plain.run(101..200, desk_input);
    let full = samplez_dsp::duck_gain(20.0, 1.0);
    let mut compared = 0;
    for n in 16..a[0].len() {
        if p[0][n].abs() > 1e-4 {
            let r = a[0][n] / p[0][n];
            assert!((r - full).abs() < 1e-4, "sample {n}: {r}");
            compared += 1;
        }
    }
    assert!(compared > 4000, "{compared}");
    assert!(bits(&a[1]) == bits(&p[1]), "the column not ducked");
}

/// A desk rebuilt from its product state while a live one ducks
/// converges on it: at the rebuild the live desk has an Auto group
/// mid-duck, a group whose force ended two blocks before and that
/// releases, and a group keyed through its low-pass with its filters
/// ringing; the rebuilt one starts them cold and its gains from
/// silence. Their outputs differ from the rebuild on and are equal
/// bit for bit from the bound on, for three seconds: the gains' ramp,
/// 50 ms for a key filter's state to reach the live one's bits, and
/// the longest attack, hold and release of a group, in which each
/// envelope reaches 0 or full depth in both.
#[test]
fn a_desk_rebuilt_mid_duck_converges_on_the_live_one() {
    use crate::ducks::{DuckSetting, DuckTargets};
    use crate::engine::Entry;
    use samplez_dsp::{DetectorParams, DuckMode, KeyFilter};
    let p0 = DetectorParams {
        attack_ms: 10.0,
        hold_ms: 50.0,
        release_ms: 200.0,
        ..DetectorParams::DEFAULT
    };
    let p1 = DetectorParams {
        attack_ms: 5.0,
        hold_ms: 100.0,
        release_ms: 300.0,
        ..DetectorParams::DEFAULT
    };
    let p2 = DetectorParams {
        lpf: KeyFilter {
            enabled: true,
            freq: 4000.0,
        },
        attack_ms: 5.0,
        hold_ms: 20.0,
        release_ms: 100.0,
        ..DetectorParams::DEFAULT
    };
    let duck = |group, setting| Entry::Duck { group, setting };
    let mut live = Desk::new(48);
    let mut entries = duck_entries();
    entries.extend([
        duck(1, DuckSetting::Detector(Box::new(p1))),
        duck(1, DuckSetting::Mode(DuckMode::On)),
        duck(1, DuckSetting::Depth { slot: 1, db: 12.0 }),
        duck(2, DuckSetting::Detector(Box::new(p2))),
        duck(2, DuckSetting::Keys(vec![(0, 2.0)])),
        duck(2, DuckSetting::Depth { slot: 1, db: 6.0 }),
        Entry::DuckMask {
            input: 1,
            output: 1,
            mask: 0b110,
        },
    ]);
    live.entry(1, 0, &entries);
    let r = 200 * 48;
    live.entry(2, r - 96, &[duck(1, DuckSetting::Mode(DuckMode::Off))]);
    live.run(0..r / 48, desk_input);
    let d = &live.mix.ducks.core.detectors;
    let e1 = d[1].envelope();
    assert!(0.0 < e1 && e1 < 1.0, "group 1 releasing: {e1}");
    let targets = [
        DuckTargets {
            params: p0,
            keys: vec![(0, 1.0)],
            depths: vec![0.0, 20.0],
            ..DuckTargets::default()
        },
        DuckTargets {
            params: p1,
            mode: DuckMode::Off,
            depths: vec![0.0, 12.0],
            ..DuckTargets::default()
        },
        DuckTargets {
            params: p2,
            keys: vec![(0, 2.0)],
            depths: vec![0.0, 6.0],
            ..DuckTargets::default()
        },
    ];
    let mut depths = [0.0; 2 * MAX_DUCKS];
    depths[MAX_DUCKS..MAX_DUCKS + 3].copy_from_slice(&[20.0, 12.0, 6.0]);
    let mut rebuilt = Desk::new(48);
    let m = &mut rebuilt.mix;
    m.ducks.core.cold(480, &targets, |_| 1);
    m.ducks.core.settle(480, r);
    m.matrix
        .fade_in_ducked(&[0.0, 0.5, 1.0, 1.0], &[0, 0, 1, 0b110], &depths, 480);
    m.matrix.align(r);
    m.pos = Some(r);
    let blocks = r / 48..r / 48 + 3000;
    let (a, b) = (
        live.run(blocks.clone(), desk_input),
        rebuilt.run(blocks, desk_input),
    );
    let ms = |t: f32| (t * 48.0) as usize;
    let longest = [p0, p1, p2]
        .iter()
        .map(|p| ms(p.attack_ms + p.hold_ms + p.release_ms))
        .max()
        .unwrap();
    let bound = 480 + ms(50.0) + longest;
    let last = (0..a[0].len())
        .rev()
        .find(|&n| (0..2).any(|c| a[c][n].to_bits() != b[c][n].to_bits()));
    assert!(a[0][..48] != b[0][..48], "the rebuilt desk never differed");
    let last = last.unwrap();
    assert!(last < bound, "differed until sample {last}, bound {bound}");
}
