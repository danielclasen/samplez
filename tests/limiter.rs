//! Output limiters on real libmxl flows: an output configured with one
//! is late by the limiter's delay, off or on; on, it holds its output at
//! the ceiling, sample peaks and the true peak its meter reads; and its
//! entries are refused for an output without one or out of range.

mod common;

use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, RAMP, config, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, GainError, InputState};
use samplez_dsp::{LIMITER_DELAY, LimiterParams};

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

fn on(ceiling_db: f32) -> LimiterParams {
    LimiterParams {
        enabled: true,
        ceiling_db,
        release_ms: 50.0,
    }
}

fn db(v: f32) -> f64 {
    20.0 * f64::from(v).log10()
}

/// One input of the 2-channel pilot, its full-scale sines at +12 dB, to
/// two stereo outputs: output 0 with a limiter, output 1 without. Before
/// the limiter is switched on output 0 is output 1 late by the limiter's
/// delay, bit for bit; once on, no sample of output 0 exceeds the
/// ceiling, its meter's true peak stays within 0.05 dB of it, and output
/// 1 still carries +12 dB.
#[test]
fn a_limiter_holds_its_output_at_the_ceiling() {
    let domain = Domain::new();
    let (src, limited, plain) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let g = 4.0;
    let matrix = vec![vec![g, 0.0, g, 0.0], vec![0.0, g, 0.0, g]];
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&limited, 2), (&plain, 2)],
        matrix,
        MARGIN,
    );
    cfg.outputs[0].limiter = true;
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised) && s.inputs[0].state == InputState::Attached
    });
    let at = (now(&rx) + 100) * B;
    e.set_limiter(0, on(-1.0), at).unwrap();
    let before = at / B - 40..at / B - 2;
    let after = (at + u64::from(RAMP)) / B + 2..(at + u64::from(RAMP)) / B + 200;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > after.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let clean = |k: u64| {
        let near = |v: &&BlockVerdict| v.k + 2 >= k && v.k <= k;
        seen.iter().filter(near).count() == 3
            && seen
                .iter()
                .filter(near)
                .all(|v| v.silenced == 0 && v.fading == 0)
    };
    let sample = |got: &[f32], i: usize, c: usize, n: usize| got[(i * 2 + c) * B as usize + n];

    // Off: output 0 is output 1, LIMITER_DELAY samples late.
    let (lim, pl) = (
        read_blocks(&domain.0, &limited, 2, before.clone()),
        read_blocks(&domain.0, &plain, 2, before.clone()),
    );
    let mut compared = 0;
    for (i, k) in before.clone().enumerate().skip(2) {
        if !clean(k) {
            continue;
        }
        for c in 0..2 {
            for n in 0..B as usize {
                let late = i * B as usize + n - LIMITER_DELAY;
                let want = sample(&pl, late / B as usize, c, late % B as usize);
                assert_eq!(sample(&lim, i, c, n).to_bits(), want.to_bits(), "block {k}");
            }
        }
        compared += 1;
    }
    assert!(compared > 20, "{compared} blocks compared");

    // On: at the ceiling.
    let ceiling = 10f32.powf(-1.0 / 20.0);
    let (lim, pl) = (
        read_blocks(&domain.0, &limited, 2, after.clone()),
        read_blocks(&domain.0, &plain, 2, after.clone()),
    );
    let peak = |x: &[f32]| x.iter().fold(0.0f32, |m, v| m.max(v.abs()));
    assert!(peak(&lim) <= ceiling * (1.0 + 1e-6), "peak {}", peak(&lim));
    assert!(peak(&lim) > 0.8 * ceiling, "peak {}", peak(&lim));
    assert!(peak(&pl) > 3.9, "peak {}", peak(&pl));
    wait_for(Duration::from_secs(5), "a metered window on", || {
        e.status().outputs[0]
            .meters
            .window
            .is_some_and(|w| w * 4800 > at + u64::from(RAMP) + 4800)
    });
    let s = e.status();
    for c in 0..2 {
        let tp = s.outputs[0].meters.true_peak[c];
        assert!(db(tp) <= -1.0 + 0.05, "channel {c}: {} dBTP", db(tp));
        assert!(db(s.outputs[1].meters.true_peak[c]) > 11.0);
    }
    assert_eq!(s.diverged, None);
    common::stop(e);
}

/// A limiter entry is refused for an output without a limiter, one that
/// does not exist and parameters out of range.
#[test]
fn limiter_entries_are_checked() {
    let domain = Domain::new();
    let (src, a, b) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&a, 1), (&b, 1)],
        vec![vec![1.0, 1.0]],
        MARGIN,
    );
    cfg.outputs[0].limiter = true;
    let (e, rx) = common::start(cfg);
    let at = (now(&rx) + 100) * B;
    let invalid = |r: Result<(), GainError>, what: &str| match r {
        Err(GainError::Invalid(m)) => assert!(m.contains(what), "{m}"),
        r => panic!("{r:?}"),
    };
    invalid(e.set_limiter(1, on(-1.0), at), "output 1 has no limiter");
    invalid(e.set_limiter(2, on(-1.0), at), "no output 2");
    invalid(e.set_limiter(0, on(1.0), at), "limiter ceiling");
    let slow = LimiterParams {
        release_ms: 2000.0,
        ..on(-1.0)
    };
    invalid(e.set_limiter(0, slow, at), "limiter release");
    e.set_limiter(0, on(-1.0), at).unwrap();
    common::stop(e);
}
