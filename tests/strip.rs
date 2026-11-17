//! Strip entries on real libmxl flows: checked when appended or set,
//! applied at their index to the legs of their slot, ramping as a strip
//! run alone does; and every input metered after its strip.

mod common;

use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, Pilot, config, diagonal, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, Entry, EntryError, GainError, InputState};
use samplez_dsp::{BandType, EqBand, Strip, StripParam};

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

/// Every input is metered after its strip and every meter after the
/// matrix, whether an output carries them or not: an input that feeds
/// only a meter, trimmed by 6 dB in its strip, reads the pilot's
/// full-scale sines 6 dB down, the meter reads them at its gain on top,
/// and the output the input does not feed reads silence.
#[test]
fn inputs_and_meters_no_output_carries_are_metered() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let silent = vec![vec![0.0], vec![0.0]];
    let mut cfg = config(&domain.0, &[(&src, 2)], &[(&dst, 1)], silent, MARGIN);
    common::add_meter(&mut cfg, "bus", 2, 0.5);
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "input attached", || {
        e.status().inputs[0].state == InputState::Attached
    });
    let at = (now(&rx) + 100) * B;
    e.set_strip(0, StripParam::Trim { db: -6.0 }, at).unwrap();
    // A window that starts after the trim's ramp ended, the momentary
    // loudness's 400 ms past it.
    let after = |w: Option<u64>| w.is_some_and(|w| w * 4800 >= at + 5 * 4800);
    wait_for(Duration::from_secs(10), "meters past the trim", || {
        let s = e.status();
        after(s.inputs[0].meters.window) && after(s.meters[0].meters.window)
    });
    let s = e.status();
    let (input, meter) = (&s.inputs[0].meters, &s.meters[0].meters);
    let db = |v: f32| 20.0 * f64::from(v).log10();
    let near = |v: f32, want: f64| (db(v) - want).abs() < 0.05;
    for c in 0..2 {
        assert!(near(input.peak[c], -6.0), "input {input:?}");
        // Referenced to a sine: a sine's RMS reads its peak.
        assert!(near(input.rms[c], -6.0), "input {input:?}");
        assert!(near(meter.peak[c], -12.02), "meter {meter:?}");
        assert!(
            near(meter.true_peak[c].max(meter.peak[c]), -12.02),
            "meter {meter:?}"
        );
    }
    assert!(input.momentary.is_some() && meter.momentary.is_some());
    assert_eq!(s.outputs[0].meters.peak, [0.0]);
    common::stop(e);
}

/// With a ramp, the engine's output is that of a strip run alone over
/// the pilot, block by block, its entries set at the start of their
/// block: trims, polarity and an equaliser band ramp at the rate of one
/// sample per sample. A block silenced, faded in or skipped in the
/// window makes the run start again, at most four times.
#[test]
fn ramps_run_as_in_a_strip_alone() {
    const RAMP: u32 = 480;
    let mut seen_why = Vec::new();
    for _ in 0..4 {
        match ramp_run(RAMP) {
            Ok(()) => return,
            Err(why) => {
                eprintln!("run disturbed ({why}): running it again");
                seen_why.push(why);
            }
        }
    }
    panic!("every run was disturbed: {seen_why:?}");
}

fn ramp_run(ramp: u32) -> Result<(), String> {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = Pilot::start(&domain.0, &src, 2, B, 0);
    let mut cfg = config(
        &domain.0,
        &[(&src, 2)],
        &[(&dst, 2)],
        diagonal(2, 1.0),
        MARGIN,
    );
    cfg.ramp_samples = ramp;
    let (e, rx) = common::start(cfg);
    wait_for(Duration::from_secs(5), "running", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    let at = (now(&rx) + 100) * B;
    // Inside the first entries' ramp.
    let at2 = at + 4 * B;
    let entries = [
        (at, StripParam::Trim { db: -12.0 }),
        (at, StripParam::Eq { enabled: true }),
        (
            at,
            StripParam::EqBand {
                band: 1,
                value: EqBand {
                    kind: BandType::Peak,
                    freq: 1000.0,
                    gain_db: 9.0,
                    q: 2.0,
                },
            },
        ),
        (at2, StripParam::LegTrim { leg: 1, db: 6.0 }),
        (
            at2,
            StripParam::Polarity {
                leg: 0,
                inverted: true,
            },
        ),
    ];
    for &(i, p) in &entries {
        e.set_strip(0, p, i).unwrap();
    }
    let ks = at / B - 10..at2 / B + 2 * u64::from(ramp) / B;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    for k in ks.clone() {
        match seen.iter().find(|v| v.k == k) {
            None => return Err(format!("block {k} skipped")),
            Some(v) if v.silenced != 0 || v.fading != 0 => {
                return Err(format!("block {k} silenced or faded in"));
            }
            Some(_) => {}
        }
    }
    let got = read_blocks(&domain.0, &dst, 2, ks.clone());
    let mut strip = Strip::new(2, ramp);
    let b = B as usize;
    for (i, k) in ks.enumerate() {
        for &(idx, p) in &entries {
            if idx == k * B {
                assert!(strip.set(p));
            }
        }
        let mut want: Vec<f32> = (0..2)
            .flat_map(|c| (0..B).map(move |n| common::pilot(c, k * B + n)))
            .collect();
        strip.process(&mut want, b);
        let block = &got[i * 2 * b..(i + 1) * 2 * b];
        let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        assert_eq!(bits(block), bits(&want), "block {k}");
    }
    assert_eq!(e.status().diverged, None);
    common::stop(e);
    Ok(())
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
