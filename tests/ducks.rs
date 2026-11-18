//! Duck entries on real libmxl flows: checked when appended or set, and
//! applied at their index, the detectors keyed by the inputs after their
//! strips and their readings in the status.

mod common;

use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, new_id, read_blocks, wait_for};
use samplez::{DuckError, DuckSetting, Entry, EntryError, InputState};
use samplez_dsp::{DetectorParams, DuckMode, duck_gain};

/// Every duck entry an engine cannot hold is refused before anything is
/// appended, whole with the log entry it is in, and alike by
/// `append_batch`: a group beyond the detectors, a ninth key, a slot keyed
/// twice, one beyond the inputs or wider than a strip, a gain, parameter
/// or depth out of range, and a mask on a column `duckColumns` does not
/// name; a mask of 0 there is taken.
#[test]
fn duck_entries_are_checked() {
    let domain = Domain::new();
    let mut inputs: Vec<(String, u32)> = (0..9).map(|_| (new_id(), 1)).collect();
    inputs.push((new_id(), 9));
    let inputs: Vec<(&str, u32)> = inputs.iter().map(|(id, n)| (id.as_str(), *n)).collect();
    let out = new_id();
    let mut cfg = config(
        &domain.0,
        &inputs,
        &[(&out, 2)],
        vec![vec![0.0; 2]; 18],
        MARGIN,
    );
    cfg.duck_columns = vec![0];
    let (e, _rx, _reports) = common::start_replicated(cfg);
    let duck = |group, setting| Entry::Duck { group, setting };
    let keys = |slots: &[usize]| DuckSetting::Keys(slots.iter().map(|&s| (s, 1.0)).collect());
    let mask = |input, output, mask| Entry::DuckMask {
        input,
        output,
        mask,
    };
    let refused = |entries: &[Entry], what: &str| {
        let r = e.admits(entries);
        assert!(
            matches!(&r, Err(EntryError::Invalid(m)) if m.contains(what)),
            "{entries:?}: {r:?}"
        );
        assert_eq!(e.append_batch(1, 48_000 * B, entries), r);
    };
    let eight: Vec<usize> = (0..8).collect();
    assert_eq!(e.admits(&[duck(7, keys(&eight))]), Ok(()));
    refused(&[duck(0, keys(&(0..9).collect::<Vec<_>>()))], "at most 8");
    // Whole: the gain beside the over-keyed group is not appended either.
    let gain = Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.5,
    };
    refused(&[gain.clone(), duck(0, keys(&[0; 9]))], "at most 8");
    refused(&[duck(0, keys(&[3, 3]))], "keyed twice");
    refused(&[duck(0, keys(&[10]))], "no input slot 10");
    refused(&[duck(0, keys(&[9]))], "legs");
    refused(
        &[duck(0, DuckSetting::Keys(vec![(0, f32::NAN)]))],
        "key gain",
    );
    refused(
        &[duck(8, DuckSetting::Hold { held: true })],
        "no duck group 8",
    );
    let loud = DetectorParams {
        threshold_db: 6.0,
        ..DetectorParams::DEFAULT
    };
    refused(
        &[duck(0, DuckSetting::Detector(Box::new(loud)))],
        "threshold",
    );
    refused(
        &[duck(0, DuckSetting::Depth { slot: 0, db: 41.0 })],
        "depth",
    );
    refused(
        &[duck(0, DuckSetting::Depth { slot: 10, db: 6.0 })],
        "slot 10",
    );
    refused(&[mask(0, 1, 1)], "duckColumns");
    refused(&[mask(18, 0, 1)], "no matrix coefficient");
    assert_eq!(e.admits(&[mask(0, 1, 0), mask(17, 0, 0xff)]), Ok(()));
    assert_eq!(
        e.admits(&[
            gain,
            duck(0, keys(&[0, 8])),
            duck(0, DuckSetting::Depth { slot: 9, db: 40.0 }),
            duck(0, DuckSetting::Mode(DuckMode::Off)),
        ]),
        Ok(())
    );
    e.stop();
}

/// A duck group keyed by one input ducks another input's coefficient
/// its mask names by the depth set, from the entry's index on, and
/// leaves the coefficient the mask does not name alone: with no ramp,
/// the ducked channel is the target's bit for bit before the index and
/// the target times the full-depth gain once the attack has run. The
/// status reads the group engaged at full depth with the key's level.
/// `set_duck` refuses what `admits` refuses.
#[test]
fn a_duck_group_ducks_its_masked_coefficients_from_its_index() {
    let domain = Domain::new();
    let (key, target, dst) = (new_id(), new_id(), new_id());
    let _key = Pilot::start(&domain.0, &key, 1, B, 0);
    let _target = Pilot::start(&domain.0, &target, 1, B, 0);
    // Output channel 0 carries the target ducked, channel 1 undocked.
    let mut cfg = config(
        &domain.0,
        &[(&key, 1), (&target, 1)],
        &[(&dst, 2)],
        vec![vec![0.0, 0.0], vec![1.0, 1.0]],
        MARGIN,
    );
    cfg.ramp_samples = 0;
    cfg.duck_columns = vec![0];
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs.iter().all(|i| i.state == InputState::Attached)
    });
    assert!(matches!(
        e.set_duck(0, DuckSetting::Keys(vec![(0, 1.0); 9]), 0),
        Err(DuckError::Invalid(_))
    ));
    assert!(matches!(
        e.set_duck_mask(1, 1, 1, 0),
        Err(DuckError::Invalid(_))
    ));
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap().k;
    let at = (rx.try_iter().last().map_or(first, |v| v.k) + 100) * B;
    let fast = DetectorParams {
        attack_ms: 1.0,
        hold_ms: 100.0,
        ..DetectorParams::DEFAULT
    };
    e.set_duck(0, DuckSetting::Detector(Box::new(fast)), at)
        .unwrap();
    e.set_duck(0, DuckSetting::Keys(vec![(0, 1.0)]), at)
        .unwrap();
    e.set_duck(0, DuckSetting::Depth { slot: 1, db: 20.0 }, at)
        .unwrap();
    e.set_duck_mask(1, 0, 1, at).unwrap();
    let ks = at / B - 20..at / B + 40;
    wait_for(Duration::from_secs(10), "blocks written", || {
        rx.try_iter().any(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let got = read_blocks(&domain.0, &dst, 2, ks.clone());
    let full = duck_gain(20.0, 1.0);
    // The attack, 48 samples, and two intervals for the grid.
    let settled = at + 48 + 16;
    let b = B as usize;
    for (i, k) in ks.enumerate() {
        let (ducked, plain) = (
            &got[2 * i * b..(2 * i + 1) * b],
            &got[(2 * i + 1) * b..(2 * i + 2) * b],
        );
        for n in 0..b {
            let idx = k * B + n as u64;
            assert_eq!(plain[n].to_bits(), common::pilot(0, idx).to_bits());
            if idx < at {
                assert_eq!(ducked[n].to_bits(), plain[n].to_bits(), "at {idx}");
            } else if idx >= settled {
                assert!((ducked[n] - plain[n] * full).abs() < 1e-6, "at {idx}");
            }
        }
    }
    wait_for(Duration::from_secs(5), "duck readings", || {
        e.status().ducks[0].slice.is_some()
    });
    let d = e.status().ducks[0];
    assert!(d.engaged && d.envelope == 1.0, "{d:?}");
    // The pilot's full-scale sine through the key high-pass at 120 Hz.
    assert!((0.95..=1.05).contains(&d.key), "{d:?}");
    assert_eq!(e.status().ducks[1].slice, None, "a group never in use");
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}

/// Mode Off releases a duck, mode On ducks whatever the key: an engine
/// keeping its own log takes manual settings at their index.
#[test]
fn manual_settings_apply_at_their_index() {
    let domain = Domain::new();
    let (target, dst) = (new_id(), new_id());
    let _target = Pilot::start(&domain.0, &target, 1, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&target, 1)],
        &[(&dst, 1)],
        vec![vec![1.0]],
        MARGIN,
    );
    cfg.duck_columns = vec![0];
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        e.status().inputs[0].state == InputState::Attached
    });
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap().k;
    let at = (rx.try_iter().last().map_or(first, |v| v.k) + 100) * B;
    e.set_duck(3, DuckSetting::Depth { slot: 0, db: 40.0 }, at)
        .unwrap();
    e.set_duck_mask(0, 0, 1 << 3, at).unwrap();
    e.set_duck(3, DuckSetting::Mode(DuckMode::On), at).unwrap();
    wait_for(Duration::from_secs(5), "ducked", || {
        let d = e.status().ducks[3];
        d.envelope == 1.0 && d.engaged
    });
    let first = rx.try_iter().last().unwrap().k;
    e.set_duck(3, DuckSetting::Mode(DuckMode::Off), (first + 20) * B)
        .unwrap();
    wait_for(Duration::from_secs(5), "released", || {
        let d = e.status().ducks[3];
        d.envelope == 0.0 && !d.engaged
    });
    assert_eq!(e.status().diverged, None);
    common::stop(e);
}
