//! Input and output delays on real libmxl flows: an input configured
//! with a delay line plays its pilot the set number of samples late, bit
//! for bit, before its strip and the matrix, and an output its mix;
//! a delay beyond the line, or on an input or output without one, is
//! refused.

mod common;

use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, Entry, EntryError, GainError, InputState};

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

/// Input 0 delayed by 1000 samples from an index, with no crossfade:
/// each output channel is its pilot channel before the index, silent for
/// the 999 samples the delay reaches back over, which it read already,
/// and the pilot 1000 samples late from there, bit for bit; input 1,
/// configured without a delay line, is not delayed.
#[test]
fn a_delayed_input_plays_its_samples_late() {
    let domain = Domain::new();
    let (src, other, dst) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _other = Pilot::start(&domain.0, &other, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2), (&other, 1)],
        &[(&dst, 3)],
        diagonal(3, 1.0),
        MARGIN,
    );
    cfg.ramp_samples = 0;
    cfg.inputs[0].max_delay_samples = 4800;
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    let at = (now(&rx) + 100) * B;
    e.set_delay(0, 1000, at).unwrap();
    let ks = at / B - 20..at / B + 60;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let got = read_blocks(&domain.0, &dst, 3, ks.clone());
    // A block, and the blocks a delayed sample of it comes from, read
    // clean.
    let clean = |k: u64| {
        let from = (k * B).saturating_sub(1100) / B;
        (from..=k).all(|j| {
            seen.iter()
                .find(|v| v.k == j)
                .is_some_and(|v| v.silenced == 0 && v.fading == 0)
        })
    };
    let mut compared = 0;
    for (i, k) in ks.enumerate() {
        if !clean(k) {
            continue;
        }
        for c in 0..3 {
            let block = &got[(i * 3 + c) * B as usize..(i * 3 + c + 1) * B as usize];
            for (n, &y) in block.iter().enumerate() {
                let idx = k * B + n as u64;
                // Channels 0 and 1 are input 0's, channel 2 input 1's.
                let want = match c {
                    2 => common::pilot(0, idx),
                    _ if idx >= at + 999 => common::pilot(c, idx - 1000),
                    _ if idx >= at => 0.0,
                    _ => common::pilot(c, idx),
                };
                assert_eq!(
                    y.to_bits(),
                    want.to_bits(),
                    "block {k} channel {c} at {idx}"
                );
            }
        }
        compared += 1;
    }
    assert!(compared > 40, "{compared} blocks compared");
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// A delay above an input's `maxDelaySamples`, on an input without a
/// delay line or on no input is refused; 0 any input takes.
#[test]
fn delay_entries_are_checked() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 1), (&src, 1)],
        &[(&dst, 1)],
        vec![vec![1.0], vec![1.0]],
        MARGIN,
    );
    cfg.inputs[0].max_delay_samples = 480;
    let (e, rx) = common::start(cfg);
    let at = (now(&rx) + 100) * B;
    let invalid = |r: Result<(), GainError>, what: &str| match r {
        Err(GainError::Invalid(m)) => assert!(m.contains(what), "{m}"),
        r => panic!("{r:?}"),
    };
    invalid(e.set_delay(0, 481, at), "maxDelaySamples 480");
    invalid(e.set_delay(1, 1, at), "maxDelaySamples 0");
    invalid(e.set_delay(2, 0, at), "no input slot 2");
    e.set_delay(0, 480, at).unwrap();
    e.set_delay(1, 0, at).unwrap();
    common::stop(e);
}

/// Output 0 delayed by 1000 samples from an index, with no crossfade:
/// each of its channels is its pilot channel before the index, silent for
/// the 999 samples the delay reaches back over, and the pilot 1000
/// samples late from there, bit for bit; output 1, fed the same input and
/// configured without a delay line, is not delayed.
#[test]
fn a_delayed_output_plays_its_mix_late() {
    let domain = Domain::new();
    let (src, dst, plain) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2), (&plain, 1)],
        vec![vec![1.0, 0.0, 1.0], vec![0.0, 1.0, 0.0]],
        MARGIN,
    );
    cfg.ramp_samples = 0;
    cfg.outputs[0].max_delay_samples = 4800;
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised)
            && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    let at = (now(&rx) + 100) * B;
    e.set_output_delay(0, 1000, at).unwrap();
    let ks = at / B - 20..at / B + 60;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let delayed = read_blocks(&domain.0, &dst, 2, ks.clone());
    let undelayed = read_blocks(&domain.0, &plain, 1, ks.clone());
    // A block, and the blocks a delayed sample of it comes from, read
    // clean.
    let clean = |k: u64| {
        let from = (k * B).saturating_sub(1100) / B;
        (from..=k).all(|j| {
            seen.iter()
                .find(|v| v.k == j)
                .is_some_and(|v| v.silenced == 0 && v.fading == 0)
        })
    };
    let b = B as usize;
    let mut compared = 0;
    for (i, k) in ks.enumerate() {
        if !clean(k) {
            continue;
        }
        for c in 0..3 {
            let block = match c {
                2 => &undelayed[i * b..(i + 1) * b],
                _ => &delayed[(i * 2 + c) * b..(i * 2 + c + 1) * b],
            };
            for (n, &y) in block.iter().enumerate() {
                let idx = k * B + n as u64;
                // Output 0's channels are the pilot's, output 1's channel 0.
                let want = match c {
                    2 => common::pilot(0, idx),
                    _ if idx >= at + 999 => common::pilot(c, idx - 1000),
                    _ if idx >= at => 0.0,
                    _ => common::pilot(c, idx),
                };
                assert_eq!(y.to_bits(), want.to_bits(), "block {k} column {c} at {idx}");
            }
        }
        compared += 1;
    }
    assert!(compared > 40, "{compared} blocks compared");
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// A delay above an output's `maxDelaySamples`, on an output without a
/// delay line or on no output is refused, when set and when appended; 0
/// any output takes.
#[test]
fn output_delay_entries_are_checked() {
    let domain = Domain::new();
    let (src, dst, plain) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&dst, 1), (&plain, 1)],
        vec![vec![1.0, 1.0]],
        MARGIN,
    );
    cfg.outputs[0].max_delay_samples = 480;
    let (e, rx) = common::start(cfg.clone());
    let at = (now(&rx) + 100) * B;
    let invalid = |r: Result<(), GainError>, what: &str| match r {
        Err(GainError::Invalid(m)) => assert!(m.contains(what), "{m}"),
        r => panic!("{r:?}"),
    };
    invalid(e.set_output_delay(0, 481, at), "maxDelaySamples 480");
    invalid(e.set_output_delay(1, 1, at), "maxDelaySamples 0");
    invalid(e.set_output_delay(2, 0, at), "no output 2");
    e.set_output_delay(0, 480, at).unwrap();
    e.set_output_delay(1, 0, at).unwrap();
    common::stop(e);

    let (r, _rx, _reports) = common::start_replicated(cfg);
    let far = (1u64 << 50).next_multiple_of(B);
    for (output, samples) in [(0, 481), (1, 1), (2, 0)] {
        let got = r.append(1, far, Entry::OutputDelay { output, samples });
        assert!(
            matches!(got, Err(EntryError::Invalid(_))),
            "{output} {samples}: {got:?}"
        );
    }
    r.append(
        1,
        far,
        Entry::OutputDelay {
            output: 0,
            samples: 480,
        },
    )
    .unwrap();
    r.stop();
}
