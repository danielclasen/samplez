//! Timed gain transitions on real libmxl flows: a gain change with a
//! fade holds its coefficient for the delay and then ramps linearly over
//! the length, the output samples exactly those of the formula.

mod common;

use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, Fade, GainError, InputState};

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

/// The coefficient at sample index `n`: 1.0 below `start`, then linear to
/// `target` over `len`, as the engine's ramp computes it.
fn gain_at(n: u64, start: u64, len: u64, target: f32) -> f32 {
    if n < start {
        1.0
    } else if n < start + len {
        1.0 + (n - start) as f32 * ((target - 1.0) / len as f32)
    } else {
        target
    }
}

/// Output 0 carries the pilot through two coefficients that get a fade,
/// output 1 through unity coefficients. From the index of the entry the
/// coefficients hold for the delay, ramp over the length and hold the
/// target: each sample of output 0 is the sample of output 1 times the
/// coefficient's value at its index, bit for bit, across block
/// boundaries the delay and the ramp do not align to.
#[test]
fn a_fade_holds_then_ramps_exactly() {
    let domain = Domain::new();
    let (src, faded, plain) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let matrix = vec![vec![1.0, 0.0, 1.0, 0.0], vec![0.0, 1.0, 0.0, 1.0]];
    let cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&faded, 2), (&plain, 2)],
        matrix,
        MARGIN,
    );
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs.iter().all(|o| o.realised) && s.inputs[0].state == InputState::Attached
    });
    let (delay, len) = (3 * B as u32 + 10, 5 * B as u32 + 7);
    let fade = Some(Fade { delay, len });
    let at = (now(&rx) + 100) * B;
    e.set_gain_fade(0, 0, 0.25, fade, at).unwrap();
    e.set_gain_fade(1, 1, 0.25, fade, at).unwrap();
    let end = at + u64::from(delay) + u64::from(len);
    let (first, last) = (at / B - 2, end / B + 4);
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > last + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let (out, reference) = (
        read_blocks(&domain.0, &faded, 2, first..last),
        read_blocks(&domain.0, &plain, 2, first..last),
    );
    let mut compared = 0;
    let (mut held, mut ramped) = (0, 0);
    for (i, k) in (first..last).enumerate() {
        let near = |v: &&BlockVerdict| v.k + 2 >= k && v.k <= k;
        let clean = seen.iter().filter(near).count() == 3
            && (seen.iter().filter(near)).all(|v| v.silenced == 0 && v.fading == 0);
        if !clean {
            continue;
        }
        for c in 0..2 {
            for s in 0..B as usize {
                let n = k * B + s as u64;
                let at_i = (i * 2 + c) * B as usize + s;
                let want = gain_at(n, at + u64::from(delay), u64::from(len), 0.25);
                let got = out[at_i];
                assert_eq!(
                    got.to_bits(),
                    (want * reference[at_i]).to_bits(),
                    "sample {n} (coefficient {want})"
                );
                if n >= at && n < at + u64::from(delay) {
                    held += 1;
                } else if n >= at + u64::from(delay) && n < end {
                    ramped += 1;
                }
            }
        }
        compared += 1;
    }
    assert!(compared > 10, "{compared} blocks compared");
    assert!(held > 100 && ramped > 400, "held {held}, ramped {ramped}");
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// A fade longer than the exact ramp length, in either field, is refused.
#[test]
fn a_fade_beyond_the_bound_is_refused() {
    let domain = Domain::new();
    let (src, out) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let cfg = config(
        &domain.0,
        &[(&src, 1)],
        &[(&out, 1)],
        vec![vec![1.0]],
        MARGIN,
    );
    let (e, rx) = common::start(cfg);
    let at = (now(&rx) + 100) * B;
    let max = samplez_dsp::MAX_EXACT_RAMP_LEN;
    for fade in [
        Fade {
            delay: 0,
            len: max + 1,
        },
        Fade {
            delay: max + 1,
            len: 10,
        },
    ] {
        match e.set_gain_fade(0, 0, 0.5, Some(fade), at) {
            Err(GainError::Invalid(m)) => assert!(m.contains("fade"), "{m}"),
            r => panic!("{r:?}"),
        }
    }
    e.set_gain_fade(
        0,
        0,
        0.5,
        Some(Fade {
            delay: max,
            len: max,
        }),
        at,
    )
    .unwrap();
    common::stop(e);
}
