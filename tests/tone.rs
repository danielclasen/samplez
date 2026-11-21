//! Tone generators on a running engine: an input configured with `tone`
//! reads no flow and is its formula, bit for bit, on its legs; it is
//! metered, stripped and keyed as an input is, never late, pruned or
//! down; and the entries of a flow are refused for it.

mod common;

use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use common::{B, Domain, MARGIN, RAMP, new_id, read_blocks, wait_for};
use samplez::{BlockVerdict, Config, DuckSetting, Entry, EntryError, GainError, InputState};
use samplez_dsp::tone::{level_gain, sine};
use samplez_dsp::{StripParam, ToneParams};

/// The newest block an engine processed.
fn now(rx: &Receiver<BlockVerdict>) -> u64 {
    let first = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    rx.try_iter().last().unwrap_or(first).k
}

/// A stereo tone generator on output `dst`, gain 1 per leg.
fn tone_config(domain: &Path, dst: &str) -> Config {
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": MARGIN,
        "rampSamples": RAMP,
        "libmxl": common::libmxl(),
        "domains": [{ "name": "d", "path": domain }],
        "inputs": [{ "name": "tone", "tone": {}, "channels": 2 }],
        "outputs": [{ "name": "out", "domain": "d", "flowId": dst, "label": "tone",
                      "group": "samplez-test", "role": "Audio", "channels": 2 }],
        "matrix": [[1.0, 0.0], [0.0, 1.0]],
        "rt": { "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

/// Set to 1 kHz at -6 dBFS, each output leg is the formula from the end
/// of the ramp, bit for bit; the input reads as a tone generator, up,
/// never late or silenced, and its meter reads the level as the sample
/// peak. A trim in its strip lowers what its meter reads, and a duck
/// group takes it as a key. Fails with the generator bypassed in
/// `Rt::process` (`if let Some(tone) = &mut slot.tone` never taken) or
/// with its status reported as another state.
#[test]
fn a_tone_input_is_its_formula_metered_and_never_late() {
    let domain = Domain::new();
    let dst = new_id();
    let (e, rx) = common::start(tone_config(&domain.0, &dst));
    wait_for(Duration::from_secs(5), "output created", || {
        e.status().outputs[0].realised
    });
    let at = (now(&rx) + 100) * B;
    let params = ToneParams {
        frequency_hz: 1000,
        level_db: -6.0,
        ident: false,
    };
    e.set_tone(0, params, at).unwrap();
    e.set_duck(0, DuckSetting::Keys(vec![(0, 1.0)]), at)
        .unwrap();
    let settled = (at + u64::from(RAMP)).div_ceil(B);
    let ks = settled..settled + 100;
    let mut seen = Vec::new();
    wait_for(Duration::from_secs(10), "blocks written", || {
        seen.extend(rx.try_iter());
        seen.last().is_some_and(|v| v.k > ks.end + 2)
    });
    std::thread::sleep(Duration::from_millis(50));
    let got = read_blocks(&domain.0, &dst, 2, ks.clone());
    let level = level_gain(-6.0);
    let mut compared = 0;
    for (i, k) in ks.enumerate() {
        // A block skipped by an overrun was not written.
        if !seen.iter().any(|v| v.k == k) {
            continue;
        }
        for c in 0..2 {
            let block = &got[(i * 2 + c) * B as usize..(i * 2 + c + 1) * B as usize];
            for (j, &y) in block.iter().enumerate() {
                let n = k * B + j as u64;
                let want = level * sine(n, 1000, 48_000);
                assert_eq!(y.to_bits(), want.to_bits(), "leg {c} at {n}");
            }
        }
        compared += 1;
    }
    assert!(compared > 50, "{compared} blocks compared");
    assert!(seen.iter().all(|v| v.silenced == 0), "a block silenced");
    // A window past the ramp: 1 kHz peaks on a sample every 48, at the
    // level exactly.
    let after = |w: Option<u64>| w.is_some_and(|w| w * 4800 >= at + 4800);
    wait_for(Duration::from_secs(10), "meters past the level", || {
        after(e.status().inputs[0].meters.window)
    });
    let s = e.status();
    let input = &s.inputs[0];
    assert_eq!(input.state, InputState::Tone);
    assert!(input.up);
    assert_eq!(input.margin_warning, None);
    assert_eq!(input.meters.peak, [level, level], "{:?}", input.meters);
    assert_eq!(e.counters().input_late_blocks.load(Relaxed), 0);
    assert_eq!(e.counters().input_silenced_blocks.load(Relaxed), 0);
    // The strip runs on its legs: 6 dB down on its meter.
    let trim = (now(&rx) + 100) * B;
    e.set_strip(0, StripParam::Trim { db: -6.0 }, trim).unwrap();
    let after = |w: Option<u64>| w.is_some_and(|w| w * 4800 >= trim + 4800);
    wait_for(Duration::from_secs(10), "meters past the trim", || {
        after(e.status().inputs[0].meters.window)
    });
    let s = e.status();
    let db = |v: f32| 20.0 * f64::from(v).log10();
    for c in 0..2 {
        let peak = s.inputs[0].meters.peak[c];
        assert!((db(peak) + 12.0).abs() < 0.05, "{:?}", s.inputs[0].meters);
    }
    assert!(s.inputs[0].up && s.inputs[0].state == InputState::Tone);
    common::stop(e);
}

/// An input meters its true peak where its config sets `truePeak`: a
/// 9.6 kHz tone at -6 dBFS, whose samples fall 18 degrees off its crest,
/// reads the true peak `TruePeak` computes from its formula, 0.36 dB
/// above its sample peak and within 0.1 dB of its crest, the level. The
/// same tone on an input without it reads no true peak. Fails with
/// `OutMeter::without_true_peak` kept for every input in `new_meters`,
/// whose input meter then reads the sample peak as its true peak.
#[test]
fn an_input_meters_its_true_peak_where_its_config_asks() {
    let domain = Domain::new();
    let dst = new_id();
    let tone = |name: &str, true_peak: bool| serde_json::json!({ "name": name, "tone": {}, "channels": 2, "truePeak": true_peak });
    let v = serde_json::json!({
        "blockSize": B,
        "readerMarginSamples": MARGIN,
        "rampSamples": RAMP,
        "libmxl": common::libmxl(),
        "domains": [{ "name": "d", "path": domain.0 }],
        "inputs": [tone("tp", true), tone("plain", false)],
        "outputs": [{ "name": "out", "domain": "d", "flowId": dst, "label": "tone",
                      "group": "samplez-test", "role": "Audio", "channels": 2 }],
        "matrix": [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0], [0.0, 0.0]],
        "rt": { "rtCpu": "none" },
    });
    let (e, rx) = common::start(Config::parse(&v.to_string()).unwrap());
    wait_for(Duration::from_secs(5), "output created", || {
        e.status().outputs[0].realised
    });
    let (f, level) = (9600, level_gain(-6.0));
    let at = (now(&rx) + 100) * B;
    for slot in 0..2 {
        let params = ToneParams {
            frequency_hz: f,
            level_db: -6.0,
            ident: false,
        };
        e.set_tone(slot, params, at).unwrap();
    }
    let after = |w: Option<u64>| w.is_some_and(|w| w * 4800 >= at + u64::from(RAMP) + 4800);
    wait_for(Duration::from_secs(10), "meters past the level", || {
        let s = e.status();
        after(s.inputs[0].meters.window) && after(s.inputs[1].meters.window)
    });
    let x: Vec<f32> = (0..71).map(|n| level * sine(n, f, 48_000)).collect();
    let mut tp = samplez_dsp::TruePeak::new(1, 64);
    tp.process(0, &x[..samplez_dsp::TruePeak::HISTORY]);
    let want = tp.process(0, &x[samplez_dsp::TruePeak::HISTORY..]);
    let db = |v: f32| 20.0 * f64::from(v).log10();
    let s = e.status();
    let (m, plain) = (&s.inputs[0].meters, &s.inputs[1].meters);
    for c in 0..2 {
        assert_eq!(m.true_peak[c].to_bits(), want.to_bits(), "{m:?}");
        assert!(m.max_true_peak[c] >= want, "{m:?}");
        assert!((db(want) - db(m.peak[c]) - 0.36).abs() < 0.01, "{m:?}");
        assert!((db(want) - db(level)).abs() < 0.1, "{m:?}");
    }
    assert!(plain.true_peak.is_empty() && plain.max_true_peak.is_empty());
    assert_eq!(plain.peak, m.peak, "{plain:?}");
    common::stop(e);
}

/// Tone entries out of range, on a slot that reads flows or on no slot
/// are refused when appended or set, and the entries of a flow are
/// refused for a tone generator: `InputDown`, `InputUp`, `Route` and
/// `Detach`. Fails with the tone generator's slot let through in
/// `Engine::change` (its `if self.config.inputs[slot].tone.is_some()`
/// guard dropped).
#[test]
fn tone_entries_are_checked() {
    let domain = Domain::new();
    let (src, dst) = (new_id(), new_id());
    let _pilot = common::Pilot::start(&domain.0, &src, 1, B, 0);
    let mut cfg = tone_config(&domain.0, &dst);
    let flow: samplez::config::Input = serde_json::from_value(serde_json::json!({
        "name": "in", "domain": "d", "flowId": src, "channels": 1
    }))
    .unwrap();
    cfg.inputs.push(flow);
    cfg.matrix.push(vec![1.0, 1.0]);
    let (e, rx) = common::start(cfg);
    let at = (now(&rx) + 100) * B;
    let ok = ToneParams::DEFAULT;
    let tone = |slot, p: ToneParams| Entry::Tone {
        slot,
        frequency_hz: p.frequency_hz,
        level_db: p.level_db,
        ident: p.ident,
    };
    let invalid = |r: Result<(), EntryError>, what: &str| match r {
        Err(EntryError::Invalid(m)) => assert!(m.contains(what), "{m}"),
        r => panic!("{what}: {r:?}"),
    };
    let out_of_range = [
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
            level_db: -61.0,
            ..ok
        },
        ToneParams {
            level_db: f32::NAN,
            ..ok
        },
    ];
    for p in out_of_range {
        invalid(e.admits(&[tone(0, p)]), "outside");
        match e.set_tone(0, p, at) {
            Err(GainError::Invalid(m)) => assert!(m.contains("outside"), "{m}"),
            r => panic!("{p:?}: {r:?}"),
        }
    }
    invalid(e.admits(&[tone(1, ok)]), "no tone generator");
    invalid(e.admits(&[tone(2, ok)]), "no input slot 2");
    e.admits(&[tone(0, ok)]).unwrap();
    e.set_tone(0, ok, at).unwrap();
    for entry in [
        Entry::InputDown { slot: 0 },
        Entry::InputUp { slot: 0 },
        Entry::Detach { slot: 0 },
        Entry::Route {
            slot: 0,
            flow_id: new_id(),
        },
    ] {
        invalid(e.admits(&[entry]), "tone generator");
    }
    e.admits(&[Entry::InputDown { slot: 1 }]).unwrap();
    assert!(matches!(e.detach(0, at), Err(EntryError::Invalid(_))));
    assert!(matches!(
        e.route(0, &new_id(), at),
        Err(EntryError::Invalid(_))
    ));
    common::stop(e);
}
