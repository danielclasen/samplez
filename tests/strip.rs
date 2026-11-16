//! Strip entries on real libmxl flows: checked when appended or set,
//! applied at their index to the legs of their slot.

mod common;

use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, Entry, EntryError, GainError, InputState};
use samplez_dsp::StripParam;

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

/// A polarity entry inverts its leg of its slot from its index on, and
/// nothing else: with no ramp, each output channel is its pilot channel
/// before the entry's index and negated from it, bit for bit.
#[test]
fn a_strip_entry_applies_at_its_index_to_its_leg() {
    let domain = Domain::new();
    let (src, mono, dst) = (new_id(), new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let _mono = Pilot::start(&domain.0, &mono, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2), (&mono, 1)],
        &[(&dst, 3)],
        diagonal(3, 1.0),
        MARGIN,
    );
    cfg.ramp_samples = 0;
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    let at = (now(&rx) + 100) * B;
    let at2 = at + 10 * B;
    let invert = |leg| StripParam::Polarity {
        leg,
        inverted: true,
    };
    e.set_strip(0, invert(1), at).unwrap();
    e.set_strip(1, invert(0), at2).unwrap();
    // Output channel c is input channel c: its pilot channel and the
    // index it is inverted from.
    let legs = [(0, u64::MAX), (1, at), (0, at2)];
    let ks = at / B - 20..at2 / B + 20;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let got = read_blocks(&domain.0, &dst, 3, ks.clone());
    let mut compared = 0;
    for (i, k) in ks.enumerate() {
        let Some(v) = seen.iter().find(|v| v.k == k) else {
            continue;
        };
        if v.silenced != 0 || v.fading != 0 {
            continue;
        }
        for (c, &(leg, from)) in legs.iter().enumerate() {
            let block = &got[(i * 3 + c) * B as usize..(i * 3 + c + 1) * B as usize];
            for (n, &y) in block.iter().enumerate() {
                let idx = k * B + n as u64;
                let x = common::pilot(leg, idx);
                let want = if idx >= from { -x } else { x };
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

/// A strip entry is refused for a slot that does not exist, a slot
/// wider than a strip, a leg the slot does not have, a value outside its
/// range and an index off the block grid; and by the log mode that does
/// not take it.
#[test]
fn strip_entries_are_checked() {
    let domain = Domain::new();
    let (src, wide, dst) = (new_id(), new_id(), new_id());
    let cfg = config(
        &domain.0,
        &[(&src, 2), (&wide, 9)],
        &[(&dst, 1)],
        vec![vec![0.0]; 11],
        MARGIN,
    );
    let (e, _rx) = common::start(cfg.clone());
    let trim = StripParam::Trim { db: 0.0 };
    let far = (1u64 << 50).next_multiple_of(B);
    let invalid = |r: Result<(), GainError>, what: &str| {
        assert!(
            matches!(&r, Err(GainError::Invalid(m)) if m.contains(what)),
            "{what}: {r:?}"
        );
    };
    invalid(e.set_strip(2, trim, far), "no input slot 2");
    invalid(e.set_strip(1, trim, far), "9 channels");
    invalid(
        e.set_strip(0, StripParam::LegTrim { leg: 2, db: 0.0 }, far),
        "no leg 2",
    );
    invalid(e.set_strip(0, StripParam::Trim { db: 20.5 }, far), "trim");
    invalid(e.set_strip(0, trim, far + 1), "multiple of the block size");
    assert!(matches!(
        e.set_strip(0, trim, 0),
        Err(GainError::TooNear { .. })
    ));
    e.set_strip(0, trim, far).unwrap();
    let entry = Entry::Strip {
        slot: 0,
        param: trim,
    };
    assert!(matches!(
        e.append(10, far, entry.clone()),
        Err(EntryError::Invalid(m)) if m.contains("own log")
    ));
    e.stop();

    let (r, _rx, _reports) = common::start_replicated(cfg);
    invalid(r.set_strip(0, trim, far), "replicated log");
    let refused = [
        (2, trim),
        (1, trim),
        (
            0,
            StripParam::Polarity {
                leg: 2,
                inverted: true,
            },
        ),
        (0, StripParam::Trim { db: f32::NAN }),
    ];
    for (slot, param) in refused {
        let got = r.append(1, far, Entry::Strip { slot, param });
        assert!(
            matches!(got, Err(EntryError::Invalid(_))),
            "{slot} {param:?}: {got:?}"
        );
    }
    r.append(1, far, entry).unwrap();
    r.stop();
}
