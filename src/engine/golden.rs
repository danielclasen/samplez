//! The bits the engine computes, pinned per [`crate::COMPUTE_VERSION`].
//! `tests/golden.rs` pins the DSP kernels on a fixed signal; this pins
//! the RT loop around them: an offline loop computes a fixed scene from a
//! fixed log, block by block as the RT thread does, its inputs a
//! deterministic source ([`Rt::test_input`]).
//!
//! The log applies, at the start of its entries' blocks, gain ramps and
//! fades (absolute, delayed, and applied after their start from indices
//! inside a block, joined from the value held), duck masks, depths, modes,
//! hold and detector settings, strip, limiter and delay settings, an input
//! crossfade to another flow with its generation on time, an input taken
//! down and raised with its generation on time, one raised with its
//! generation late and held silent until it arrives, and loudness pauses,
//! runs and resets of outputs and configured meters, at slice boundaries
//! and inside slices. Entries also apply while ramps and fades are in
//! flight. A span of blocks is not computed, as after an overrun, with a
//! fade in flight across it and entries at its first block, inside it and
//! at its last block. A burst on the output of the tighter limiter stays
//! below its ceiling in its samples and exceeds it between them, so only
//! the true-peak estimate engages it.
//!
//! Pinned apart: every column's samples; every value the meters send,
//! each field of each meter and duck record, with the readings the
//! supervisor's fold makes of them; the outputs' page hashes; the entry
//! fingerprints of the pages, which replicas compare; and the encoded
//! core copy a replica of the same version decodes, taken through the
//! loop's copy path mid-scene with entries, batches and a late
//! generation's entry pending. The matrix's serde form leaves a term's
//! mask and duck, the masks, the depths and the duck phase out while
//! they are at their defaults, so the copy is checked to carry each of
//! them; the phase is 0 at every block boundary of a block size
//! [`GAIN_INTERVAL`] divides, as 48 is, so a second, small scene at a
//! block of 20 copies its core where the phase is not, pinned apart. The
//! limiter's minima are not encoded by design: a function of its gains
//! and position, rebuilt when decoded. A change to
//! what the loop computes from a log fails here until the version is
//! bumped and its hashes added, so replicas of a release that keeps the
//! version compute what a running set computes. The page hashes and
//! fingerprints are what replicas compare: a change to how they are
//! computed that leaves the samples alone needs a `PROTOCOL` bump in
//! `samplez-replica`, which this crate cannot see, so their pins are
//! replaced with that bump.
//!
//! A third scene, a tone generator through level, frequency and ident
//! changes, pins the generator's samples and its state in a core copy
//! apart, so the main scene's rows stay as they were before it.
//!
//! Left out: the output write and hold path, real MXL reads and clock
//! steps. An offline loop has no flow writer, so blocks never reach a
//! hold ring and nothing is committed or withheld; the hold ring's
//! decisions are tested in `blocks.rs` and `tests/hold.rs`. Reads go
//! through libmxl and a producer's timing, which is not deterministic;
//! the test source enters at the read's result. A clock step comes from
//! libmxl's clock, which an offline loop never reads. The core copy's
//! `last_id` is 0 and its replicated-state lists are empty: the log
//! reaches the loop's pending list directly, not through the entry ring
//! and the replicated state.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use samplez_dsp::strip::{
    CompParams, DEFAULT_BANDS, EqBand, GAIN_INTERVAL, GateParams, StripParam,
};
use samplez_dsp::{DetectorParams, DuckMode, LimiterParams};
use samplez_mxl::MxlApi;
use samplez_mxl::input::Generation;
use xxhash_rust::xxh3::Xxh3Default;

use super::{Fade, new_meters, replay_rt};
use crate::batch::{self, DuckItem, Item};
use crate::blocks::PAGE;
use crate::config::Config;
use crate::ducks::{DuckChange, KeySet};
use crate::meters::{OutputLoudness, OutputMeters};
use crate::rt_loop::{Change, Command, GAP, Gen, PENDING_CHANGES, Rt, RtEntry, SPAN};
use crate::transfer::{Core, CoreCopy};

/// The golden run's hashes for each compute version: (version, samples,
/// meters, page hashes).
const GOLDEN: &[(u32, u64, u64, u64)] = &[
    (
        1,
        0x7355_b2cf_4e99_c835,
        0xede8_c40d_9813_dca1,
        0x2993_dbbc_89ad_9800,
    ),
    (
        2,
        0x8219_0f50_2117_74a0,
        0xb8de_6030_57c2_013e,
        0x791b_ba52_022d_4223,
    ),
];

/// The pages' entry fingerprints: a function of the log's ids and indices
/// alone, whatever the samples. Replacing it goes with a `PROTOCOL` bump
/// in `samplez-replica`, which this crate cannot see: that pairing is a
/// review step, nothing here checks it.
const FINGERPRINTS: u64 = 0xfd09_ad6f_7cbc_3be9;

/// The encoded core copy after block [`CORE_AT`] for each compute
/// version: (version, XXH3-64 of [`CoreCopy::encode`]'s bytes). A copy
/// is taken only at an equal `COMPUTE_VERSION`, so a change to what it
/// holds or how it encodes needs a bump here, or replicas of the new
/// release fail to decode the running set's copies mid-roll.
const CORES: &[(u32, u64)] = &[(1, 0xcd98_6111_aa8d_c6be), (2, 0xd976_b4b6_f4c1_ba1f)];

/// The block after which the core is copied: entries and their batches
/// pending, slot 0's `InputUp` awaiting its late generation, the
/// re-route's crossfade done, slot 3's delay holding history.
const CORE_AT: u64 = 1650;

/// The small scene's encoded core copy for each compute version:
/// (version, XXH3-64 of [`CoreCopy::encode`]'s bytes).
const PHASED_CORES: &[(u32, u64)] = &[(1, 0x9a09_eea1_eddb_5e87), (2, 0xdac9_558b_b958_0ffc)];

/// The tone scene's hashes for each compute version: (version, samples,
/// XXH3-64 of the encoded core copy after [`TONE_CORE_AT`]). A scene of
/// its own, so the main scene's rows stay as they are.
const TONES: &[(u32, u64, u64)] = &[
    (1, 0x7325_ebfb_0d8e_41e4, 0xb8af_9c78_1774_6585),
    (2, 0x7325_ebfb_0d8e_41e4, 0xb083_0c30_70c5_ded3),
];

/// The block after which the tone scene's core is copied: inside the
/// crossfade to 440 Hz, the change to 1 kHz waiting behind it.
const TONE_CORE_AT: u64 = 206;
/// Two seconds: the ident's cycle of three windows twice and more.
const TONE_BLOCKS: u64 = 2000;

/// The small scene's block, which [`GAIN_INTERVAL`] does not divide.
const PHASED_B: u64 = 20;
/// The block after which its core is copied: 11 blocks of 20 end at
/// index 220, 4 into an interval of 8.
const PHASED_AT: u64 = 10;
const _: () = assert!(!((PHASED_AT + 1) * PHASED_B).is_multiple_of(GAIN_INTERVAL as u64));

const B: u64 = 48;
/// Three seconds.
const BLOCKS: u64 = 3000;
const RAMP: u32 = 480;
/// A stereo programme, a mono programme re-routed to another flow, the
/// voice keying duck group 0, a delayed input taken down and raised, and
/// the true-peak burst.
const SLOTS: usize = 5;
const VOICE: usize = 2;
const TP: usize = 4;
/// Input channels: slot 0 has rows 0 and 1, slot n > 0 row n + 1.
const ROWS: usize = 6;
/// Output 0 (stereo, columns 0 and 1), output 1 (column 2), the aux
/// (3) and the mix-minus (4).
const COLUMNS: usize = 5;
/// The burst's samples, a sine at a quarter of the sample rate 45
/// degrees off its zero crossings: a, a, -a, -a with a = 0.11, below
/// output 1's ceiling of -18 dB FS (0.126), its reconstructed peak a *
/// sqrt(2) = 0.156 above it.
const BURST: Range<u64> = 110_400..115_200;
/// The blocks not computed, as an overrun skips them.
const SKIP: Range<u64> = 2200..2260;
/// The meters: the outputs, then the configured meters.
const AUX: usize = 2;
const MIX_MINUS: usize = 3;

/// The deterministic input: plain arithmetic, so no libm function decides
/// its bits. A triangle per slot, flow and channel at its own period plus
/// noise from an LCG step, and on the true-peak slot the burst alone. The
/// voice comes in bursts of 0.3 s every 0.8 s, rising and falling over
/// 0.1 s, with silence between them: its key crosses the detector's
/// threshold where its key gain puts the crossing, and the detector
/// releases between bursts unless it is held.
fn source(slot: usize, flow: u32, channel: usize, n: u64) -> f32 {
    if slot == TP {
        return if !BURST.contains(&n) {
            0.0
        } else if n % 4 < 2 {
            0.11
        } else {
            -0.11
        };
    }
    let key = (slot as u64) * 3 + u64::from(flow) * 2 + channel as u64;
    let period = 89 + 41 * key;
    let phase = (n % period) as f32 / period as f32;
    let triangle = 4.0 * (phase - 0.5).abs() - 1.0;
    let mut x = n ^ (key << 56);
    x = x
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    x ^= x >> 29;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 32;
    let noise = ((x >> 40) as f32 / (1u64 << 24) as f32) - 0.5;
    if slot == VOICE {
        let p = n % 38_400;
        if p >= 14_400 {
            return 0.0;
        }
        let edge = p.min(14_400 - p).min(4800) as f32 / 4800.0;
        return 0.8 * edge * (triangle + 0.05 * noise);
    }
    let level = if flow == 0 { 0.6 } else { 0.4 };
    level * triangle + 0.05 * noise
}

fn gain(input: usize, output: usize, gain: f32, fade: Option<Fade>) -> Item {
    Item::Change(Change::Gain {
        input,
        output,
        gain,
        fade,
    })
}

fn duck(group: u8, change: DuckChange) -> Item {
    Item::Duck(DuckItem::Set { group, change })
}

fn strip(slot: usize, param: StripParam) -> Item {
    Item::Change(Change::Strip { slot, param })
}

fn limiter(output: usize, ceiling_db: f32, release_ms: f32) -> Item {
    Item::Change(Change::Limiter {
        output,
        params: LimiterParams {
            enabled: true,
            ceiling_db,
            release_ms,
        },
    })
}

fn loudness_run(meter: usize, running: bool) -> Item {
    Item::Change(Change::LoudnessRun { meter, running })
}

fn detector(threshold_db: f32) -> DuckChange {
    DuckChange::Detector(DetectorParams {
        threshold_db,
        attack_ms: 5.0,
        hold_ms: 100.0,
        release_ms: 300.0,
        ..DetectorParams::DEFAULT
    })
}

/// Every strip unit on, on slots 0 to 3; the true-peak slot inverted
/// only, which leaves its samples' magnitudes as they are.
fn strips() -> Vec<Item> {
    let mut items = Vec::new();
    for slot in 0..TP {
        let mut params = vec![
            StripParam::Trim { db: 3.0 },
            StripParam::Hpf {
                enabled: true,
                freq: 80.0,
            },
            StripParam::Eq { enabled: true },
            StripParam::Gate(GateParams {
                enabled: true,
                threshold_db: -50.0,
                range_db: 30.0,
                attack_ms: 1.0,
                release_ms: 50.0,
            }),
            StripParam::Comp(CompParams {
                enabled: true,
                threshold_db: -18.0,
                ratio: 4.0,
                attack_ms: 2.0,
                release_ms: 80.0,
                knee_db: 6.0,
                makeup_db: 2.0,
            }),
            StripParam::DynamicsBeforeEq { enabled: slot == 1 },
        ];
        for (band, b) in DEFAULT_BANDS.iter().enumerate() {
            let value = EqBand { gain_db: 4.0, ..*b };
            params.push(StripParam::EqBand { band, value });
        }
        if slot == 0 {
            params.push(StripParam::LegTrim { leg: 1, db: -2.0 });
        }
        items.extend(params.into_iter().map(|p| strip(slot, p)));
    }
    items.push(strip(
        TP,
        StripParam::Polarity {
            leg: 0,
            inverted: true,
        },
    ));
    items
}

/// The log: (index, items), one item an entry of its own and more a
/// folded batch, ids in this order from 1. An entry applies at the start
/// of a block, so its index is one; a fade's start is any index.
fn entries() -> Vec<(u64, Vec<Item>)> {
    let mut setup = strips();
    setup.extend([
        // Group 0 automatic, keyed by the voice; group 1 forced on.
        duck(0, detector(-24.0)),
        duck(0, DuckChange::Keys(KeySet::of(&[(VOICE, 1.0)]).unwrap())),
        duck(0, DuckChange::Mode(DuckMode::Auto)),
        duck(1, detector(-24.0)),
        duck(1, DuckChange::Mode(DuckMode::On)),
        // The programme on output 0 under the voice, slot 1 on output 1
        // under the forced group.
        Item::Duck(DuckItem::Mask {
            input: 0,
            output: 0,
            mask: 0b01,
        }),
        Item::Duck(DuckItem::Mask {
            input: 1,
            output: 1,
            mask: 0b01,
        }),
        Item::Duck(DuckItem::Mask {
            input: 2,
            output: 0,
            mask: 0b01,
        }),
        Item::Duck(DuckItem::Mask {
            input: 2,
            output: 2,
            mask: 0b10,
        }),
        Item::Duck(DuckItem::Depth {
            row: 0,
            group: 0,
            db: 12.0,
        }),
        Item::Duck(DuckItem::Depth {
            row: 1,
            group: 0,
            db: 12.0,
        }),
        Item::Duck(DuckItem::Depth {
            row: 2,
            group: 0,
            db: 9.0,
        }),
        Item::Duck(DuckItem::Depth {
            row: 2,
            group: 1,
            db: 6.0,
        }),
        limiter(0, -6.0, 50.0),
        limiter(1, -18.0, 50.0),
        Item::Change(Change::Delay {
            slot: 3,
            samples: 480,
        }),
    ]);
    let single = |c| vec![Item::Change(c)];
    vec![
        (0, setup),
        (12_000, vec![gain(0, 0, 0.3, None)]),
        // Absolute, delayed and late fades, the late one joined from the
        // value held.
        (
            18_000,
            vec![gain(
                2,
                1,
                0.2,
                Some(Fade {
                    from: 0.5,
                    start: 18_000,
                    len: 4800,
                }),
            )],
        ),
        (
            24_000,
            vec![gain(3, 0, 0.3, Some(Fade::after(0.7, 24_000, 1001, 2400)))],
        ),
        (
            30_000,
            vec![gain(
                4,
                2,
                0.9,
                Some(Fade {
                    from: 0.6,
                    start: 28_777,
                    len: 4800,
                }),
            )],
        ),
        (
            36_000,
            vec![
                Item::Duck(DuckItem::Mask {
                    input: 2,
                    output: 2,
                    mask: 0b11,
                }),
                Item::Duck(DuckItem::Depth {
                    row: 0,
                    group: 0,
                    db: 20.0,
                }),
            ],
        ),
        (42_000, vec![duck(1, DuckChange::Mode(DuckMode::Off))]),
        // Held from the end of the burst [38_400, 52_800), released
        // 0.2 s into the silence after it.
        (52_800, vec![duck(0, DuckChange::Hold(true))]),
        (48_000, single(Change::Route { slot: 1 })),
        (54_000, single(Change::InputDown { slot: 3 })),
        (
            62_400,
            vec![duck(0, DuckChange::Hold(false)), duck(0, detector(-30.0))],
        ),
        (66_000, single(Change::InputUp { slot: 3 })),
        (72_000, single(Change::InputDown { slot: 0 })),
        (78_000, single(Change::InputUp { slot: 0 })),
        (
            84_000,
            single(Change::Delay {
                slot: 3,
                samples: 1500,
            }),
        ),
        (90_000, vec![limiter(0, -9.0, 120.0)]),
        // Output 1 carries the true-peak slot alone over the burst.
        (
            96_000,
            vec![
                gain(2, 2, 0.0, None),
                gain(3, 2, 0.0, None),
                gain(4, 2, 0.0, None),
            ],
        ),
        (
            120_000,
            vec![
                gain(2, 2, 0.5, None),
                gain(3, 2, 0.9, None),
                gain(4, 2, 0.9, None),
                strip(
                    2,
                    StripParam::Comp(CompParams {
                        enabled: true,
                        threshold_db: -24.0,
                        ratio: 6.0,
                        attack_ms: 1.0,
                        release_ms: 60.0,
                        knee_db: 3.0,
                        makeup_db: 4.0,
                    }),
                ),
            ],
        ),
        (126_000, vec![limiter(1, -14.0, 30.0)]),
        (
            132_000,
            vec![strip(
                1,
                StripParam::EqBand {
                    band: 0,
                    value: EqBand {
                        gain_db: -6.0,
                        ..DEFAULT_BANDS[0]
                    },
                },
            )],
        ),
        // Appended later in the log, so the ids above stay.
        // Output 0 paused at a slice boundary and run inside a slice,
        // the aux paused inside a slice and run at a boundary.
        (57_600, vec![loudness_run(0, false)]),
        (62_640, vec![loudness_run(0, true)]),
        (63_120, vec![loudness_run(AUX, false)]),
        (69_120, vec![loudness_run(AUX, true)]),
        (81_120, single(Change::ResetLoudness { meter: MIX_MINUS })),
        (100_800, single(Change::ResetLoudness { meter: 0 })),
        // Entries applied while a ramp is in flight, on a computed block:
        // a batch under the fade from 18_000, a single gain under the
        // delayed one from 25_001.
        (20_160, vec![gain(0, 4, 0.3, None), gain(1, 4, 0.3, None)]),
        (26_400, vec![gain(4, 3, 0.2, None)]),
        // A fade in flight across the skipped span: from 10 blocks
        // before it to 130 blocks after it.
        (
            (SKIP.start - 10) * B,
            vec![gain(
                2,
                1,
                0.6,
                Some(Fade::after(0.2, (SKIP.start - 10) * B, 0, 9600)),
            )],
        ),
        // At the first block of the skipped span, inside it as a batch
        // and as a single gain, and at its last block, so the loop brings
        // the matrix from the last entry to the block after the span.
        (
            SKIP.start * B,
            vec![gain(0, 0, 0.6, None), loudness_run(MIX_MINUS, false)],
        ),
        (
            (SKIP.start + 20) * B,
            vec![
                limiter(0, -7.0, 80.0),
                gain(3, 0, 0.5, None),
                Item::Duck(DuckItem::Depth {
                    row: 1,
                    group: 0,
                    db: 15.0,
                }),
            ],
        ),
        ((SKIP.start + 40) * B, vec![gain(1, 1, 0.6, None)]),
        (
            (SKIP.end - 1) * B,
            vec![
                loudness_run(MIX_MINUS, true),
                Item::Change(Change::ResetLoudness { meter: AUX }),
            ],
        ),
    ]
}

/// The generations, as (block handed over, entry id, re-routed slots):
/// the re-route's and slot 3's ahead of their entries, slot 0's 50 blocks
/// after its entry, which holds the slot silent until then.
const GENERATIONS: &[(u64, u64, u64)] = &[(990, 9, 0b10), (1360, 12, 0), (1675, 14, 0)];

/// Asserts that core copy `encoded` carries the matrix state its serde
/// form leaves out at the default: a term with a mask other than 0, a
/// term with a duck, the masks and the depths, and with `phase` a duck
/// phase other than 0.
fn assert_ducks_encoded(encoded: &str, phase: bool) {
    let v: serde_json::Value = serde_json::from_str(encoded).unwrap();
    let m = &v["copy"]["core"]["matrix"];
    let terms: Vec<&serde_json::Value> = (m["columns"].as_array().unwrap().iter())
        .flat_map(|c| c.as_array().unwrap())
        .collect();
    let mask = |t: &&serde_json::Value| t["mask"].as_u64().is_some_and(|k| k != 0);
    assert!(terms.iter().any(mask), "no term with a mask encoded");
    let duck = |t: &&serde_json::Value| t.get("duck").is_some_and(|d| !d.is_null());
    assert!(terms.iter().any(duck), "no term with a duck encoded");
    for field in ["masks", "depths"] {
        let set = m[field].as_array().is_some_and(|a| !a.is_empty());
        assert!(set, "no {field} encoded");
    }
    if phase {
        let p = m["phase"].as_u64();
        assert!(p.is_some_and(|p| p != 0), "the duck phase encoded as {p:?}");
    }
}

/// The small scene: a programme ducked by a keyed detector into one
/// output at a block of [`PHASED_B`], its core copied after block
/// [`PHASED_AT`] with a gain entry pending. The encoded copy's hash.
fn phased_core() -> u64 {
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let inputs = (0..2).map(|i| {
        serde_json::json!({ "name": format!("in{i}"), "domain": "d",
            "flowId": format!("5fbec3b1-1b0f-417d-9059-00000000000{i}"), "channels": 1 })
    });
    let v = serde_json::json!({
        "libmxl": libmxl,
        "blockSize": PHASED_B,
        "rampSamples": RAMP,
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": inputs.collect::<Vec<_>>(),
        "outputs": [{ "name": "out", "domain": "d",
            "flowId": "6fbec3b1-1b0f-417d-9059-000000000000", "label": "o",
            "group": "g", "role": "Audio", "channels": 1 }],
        "matrix": [[0.8], [0.5]],
        "duckColumns": [0],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt: Rt = replay_rt(&config, &api, vec![None], hashes, control);
    rt.test_input = Some(source);
    let (mut core_tx, core_rx) = rtrb::RingBuffer::new(1);
    let (copy_tx, mut copy_rx) = rtrb::RingBuffer::new(1);
    let r = rt.replica.as_mut().unwrap();
    (r.cores, r.copies) = (core_rx, copy_tx);
    assert!(rt.mix.matrix.set(0, 0, 0.8, 0));
    assert!(rt.mix.matrix.set(1, 0, 0.5, 0));
    let setup = [
        duck(0, detector(-40.0)),
        duck(0, DuckChange::Keys(KeySet::of(&[(1, 1.0)]).unwrap())),
        duck(0, DuckChange::Mode(DuckMode::Auto)),
        Item::Duck(DuckItem::Mask {
            input: 0,
            output: 0,
            mask: 0b01,
        }),
        Item::Duck(DuckItem::Depth {
            row: 0,
            group: 0,
            db: 12.0,
        }),
    ];
    let (change, batch) = batch::fold(&setup, 1, 2).unwrap().unwrap();
    let entry = |id, at_index, change| RtEntry {
        id,
        at_index,
        change,
    };
    assert!(rt.mix.push_with(entry(1, 0, change), batch).is_ok());
    let Item::Change(later) = gain(1, 0, 0.3, None) else {
        unreachable!()
    };
    assert!(rt.mix.push(entry(2, 40 * PHASED_B, later)));
    let saved = samplez_dsp::set_ftz_daz(true);
    for k in 0..=PHASED_AT {
        // The loop copies after the first block it computes with a
        // buffer waiting.
        if k == PHASED_AT {
            let page = PENDING_CHANGES * PAGE.div_ceil(PHASED_B) as usize;
            let mut buffer = Box::new(Core::new(2, 1, 2, page, 1));
            buffer.prepare();
            assert!(core_tx.push(buffer).is_ok());
        }
        rt.process(k, u64::MAX);
    }
    samplez_dsp::restore_mxcsr(saved);
    let copy = copy_rx.pop().expect("a core copied");
    assert_eq!(copy.k0, PHASED_AT);
    let encoded = CoreCopy::encode(&copy);
    assert_ducks_encoded(&encoded, true);
    xxhash_rust::xxh3::xxh3_64(encoded.as_bytes())
}

/// The tone scene: a stereo tone generator on a stereo output, through a
/// level ramp, a frequency crossfade with a change waiting behind it,
/// the ident on and off mid-cut, and a batch setting it to 20 kHz at -60
/// dBFS with a trim in its strip. The hashes of every output sample and
/// of the encoded core copy after block [`TONE_CORE_AT`].
fn tone_scene() -> (u64, u64) {
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let v = serde_json::json!({
        "libmxl": libmxl,
        "blockSize": B,
        "rampSamples": RAMP,
        "domains": [{ "name": "d", "path": "/nonexistent" }],
        "inputs": [{ "name": "tone", "tone": {}, "channels": 2 }],
        "outputs": [{ "name": "out", "domain": "d",
            "flowId": "6fbec3b1-1b0f-417d-9059-000000000000", "label": "o",
            "group": "g", "role": "Audio", "channels": 2 }],
        "matrix": [[1.0, 0.0], [0.0, 1.0]],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    let config = Config::parse(&v.to_string()).unwrap();
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (hashes, _) = rtrb::RingBuffer::new(1);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt: Rt = replay_rt(&config, &api, Vec::new(), hashes, control);
    let (mut core_tx, core_rx) = rtrb::RingBuffer::new(1);
    let (copy_tx, mut copy_rx) = rtrb::RingBuffer::new(1);
    let r = rt.replica.as_mut().unwrap();
    (r.cores, r.copies) = (core_rx, copy_tx);
    assert!(rt.mix.matrix.set(0, 0, 1.0, 0));
    assert!(rt.mix.matrix.set(1, 1, 1.0, 0));
    let tone = |frequency_hz, level_db, ident| Change::Tone {
        slot: 0,
        params: samplez_dsp::ToneParams {
            frequency_hz,
            level_db,
            ident,
        },
    };
    let entry = |id, at_index, change| RtEntry {
        id,
        at_index,
        change,
    };
    let log = [
        (0, tone(997, -12.0, false)),
        (100 * B, tone(997, -6.0, false)),
        (200 * B, tone(440, -6.0, false)),
        // Inside the crossfade to 440 Hz: it waits for its end.
        (205 * B, tone(1000, -6.0, false)),
        (500 * B, tone(1000, -6.0, true)),
        // Inside leg 0's second cut.
        (1210 * B, tone(1000, -6.0, false)),
    ];
    for (id, &(at, change)) in log.iter().enumerate() {
        assert!(rt.mix.push(entry(id as u64 + 1, at, change)));
    }
    let items = [
        Item::Change(tone(20_000, -60.0, true)),
        Item::Change(Change::Strip {
            slot: 0,
            param: StripParam::Trim { db: -3.0 },
        }),
    ];
    let (change, batch) = batch::fold(&items, 2, 4).unwrap().unwrap();
    assert!(rt.mix.push_with(entry(7, 1500 * B, change), batch).is_ok());
    let mut samples = Xxh3Default::new();
    let saved = samplez_dsp::set_ftz_daz(true);
    for k in 0..TONE_BLOCKS {
        if k == TONE_CORE_AT {
            let page = PENDING_CHANGES * PAGE.div_ceil(B) as usize;
            let mut buffer = Box::new(Core::new(2, 2, 1, page, 1));
            buffer.prepare();
            assert!(core_tx.push(buffer).is_ok());
        }
        rt.process(k, u64::MAX);
        for c in 0..2 {
            for v in &rt.outbuf.channel(c)[..B as usize] {
                samples.update(&v.to_bits().to_le_bytes());
            }
        }
    }
    samplez_dsp::restore_mxcsr(saved);
    let c = &rt.counters;
    for counter in [&c.command_late, &c.strip_refused, &c.input_silenced_blocks] {
        assert_eq!(counter.load(Relaxed), 0);
    }
    let copy = copy_rx.pop().expect("a core copied");
    assert_eq!(copy.k0, TONE_CORE_AT);
    let encoded = CoreCopy::encode(&copy);
    assert!(encoded.contains("\"tone\""), "no tone generator encoded");
    (
        samples.digest(),
        xxhash_rust::xxh3::xxh3_64(encoded.as_bytes()),
    )
}

fn generation(api: &Arc<MxlApi>, domain: &Path, rerouted: u64) -> Box<Gen> {
    Box::new(Gen {
        mxl: Generation::new(api, domain, SLOTS).unwrap(),
        prune_after: vec![u64::MAX; SLOTS],
        prune_late: vec![u64::MAX; SLOTS],
        rerouted,
    })
}

fn config(domain: &Path) -> Config {
    let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    let inputs = [2, 1, 1, 1, 1].iter().enumerate().map(|(i, &ch)| {
        let mut v = serde_json::json!({ "name": format!("in{i}"), "domain": "d",
            "flowId": format!("5fbec3b1-1b0f-417d-9059-00000000000{i}"), "channels": ch });
        if i == 3 {
            v["maxDelaySamples"] = 4800.into();
        }
        v
    });
    let outputs = [2, 1].iter().enumerate().map(|(o, &ch)| {
        serde_json::json!({ "name": format!("out{o}"), "domain": "d",
            "flowId": format!("6fbec3b1-1b0f-417d-9059-00000000000{o}"), "label": "o",
            "group": "g", "role": "Audio", "channels": ch, "limiter": true })
    });
    let v = serde_json::json!({
        "libmxl": libmxl,
        "blockSize": B,
        "rampSamples": RAMP,
        "domains": [{ "name": "d", "path": domain }],
        "inputs": inputs.collect::<Vec<_>>(),
        "outputs": outputs.collect::<Vec<_>>(),
        "meters": [{ "name": "aux", "channels": 1 }, { "name": "mixminus", "channels": 1 }],
        "matrix": vec![vec![0.0; COLUMNS]; ROWS],
        "duckColumns": [0, 1, 2],
        "rt": { "enabled": false, "rtCpu": "none" },
    });
    Config::parse(&v.to_string()).unwrap()
}

fn update_readings(h: &mut Xxh3Default, m: &OutputMeters) {
    h.update(&m.window.unwrap_or(u64::MAX).to_le_bytes());
    for v in m
        .peak
        .iter()
        .chain(&m.rms)
        .chain(&m.true_peak)
        .chain(&m.max_true_peak)
    {
        h.update(&v.to_bits().to_le_bytes());
    }
    for v in [
        m.momentary,
        m.short_term,
        m.max_momentary,
        m.max_short_term,
        m.integrated,
        m.range,
    ] {
        h.update(&v.map_or(u64::MAX, f64::to_bits).to_le_bytes());
    }
    for v in [m.gaps, m.lost, m.since.unwrap_or(u64::MAX)] {
        h.update(&v.to_le_bytes());
    }
}

/// The hashes of one pass, XXH3-64 each.
#[derive(Debug, PartialEq)]
struct Pass {
    /// Every column's samples' bits.
    samples: u64,
    /// Every field of every meter and duck record the loop sends, and the
    /// readings the supervisor's fold makes of them after each block.
    meters: u64,
    /// Every field of every page record but the fingerprint.
    pages: u64,
    /// The pages' entry fingerprints, by output and page.
    fingerprints: u64,
    /// The encoded core copy after block [`CORE_AT`].
    core: u64,
}

/// One pass over the scene: as the engine's loop, or with `replay` as a
/// replay clone, which hashes its outputs' pages and sends no duck
/// records. `name` keeps concurrent passes' domains apart.
fn pass(name: &str, replay: bool) -> Pass {
    let domain = std::env::temp_dir().join(format!("samplez-golden-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&domain);
    std::fs::create_dir(&domain).unwrap();
    let config = config(&domain);
    let api = samplez_mxl::load(&config.libmxl).unwrap();
    let (hashes, mut hash_rx) = rtrb::RingBuffer::new(1 << 10);
    let (_, control) = rtrb::RingBuffer::new(1);
    let mut rt: Rt = replay_rt(&config, &api, vec![None], hashes, control);
    // As the engine's loop: configured and input meters, duck records
    // sent, nothing page-hashed, inputs read; or a replay clone with the
    // same meters.
    rt.replay = replay;
    (rt.buses, rt.taps) = new_meters(&config);
    rt.test_input = Some(source);
    let (meters, mut meter_rx) = rtrb::RingBuffer::new(1 << 14);
    rt.meters = meters;
    let (ducks, mut duck_rx) = rtrb::RingBuffer::new(1 << 14);
    rt.duck_meters = ducks;
    let (ret, mut back) = rtrb::RingBuffer::new(64);
    rt.ret = ret;
    let (mut cmd, cmd_rx) = rtrb::RingBuffer::new(4);
    rt.cmd = cmd_rx;
    // The core copy's rings, as the replicated state holds them.
    let (mut core_tx, core_rx) = rtrb::RingBuffer::new(1);
    let (copy_tx, mut copy_rx) = rtrb::RingBuffer::new(1);
    let r = rt.replica.as_mut().unwrap();
    (r.cores, r.copies) = (core_rx, copy_tx);
    let mut core = None;
    // The configured matrix.
    for (row, column, g) in [
        (0, 0, 0.8),
        (1, 1, 0.8),
        (2, 0, 0.5),
        (2, 1, 0.5),
        (2, 2, 0.5),
        (3, 0, 0.7),
        (3, 1, 0.7),
        (3, 2, 0.9),
        (4, 2, 0.6),
        (5, 2, 1.0),
        (0, 3, 0.4),
        (1, 3, 0.4),
        (2, 3, 0.4),
        (3, 3, 0.4),
        (4, 3, 0.4),
        (0, 4, 0.6),
        (1, 4, 0.6),
        (2, 4, 0.6),
        (4, 4, 0.5),
    ] {
        assert!(rt.mix.matrix.set(row, column, g, 0));
    }
    let entries = entries();
    for (n, (at_index, items)) in entries.iter().enumerate() {
        assert!(at_index.is_multiple_of(B));
        let entry = |change| RtEntry {
            id: n as u64 + 1,
            at_index: *at_index,
            change,
        };
        match items[..] {
            [Item::Change(change)] => assert!(rt.mix.push(entry(change))),
            _ => {
                let (change, batch) = batch::fold(items, COLUMNS, ROWS * COLUMNS)
                    .unwrap()
                    .unwrap();
                assert!(rt.mix.push_with(entry(change), batch).is_ok());
            }
        }
    }
    let weights = (config.outputs.iter().map(|o| o.loudness_weights().unwrap()))
        .chain(config.meters.iter().map(|m| m.loudness_weights().unwrap()))
        .chain(config.inputs.iter().map(|i| i.loudness_weights().unwrap()));
    let mut loudness: Vec<OutputLoudness> = weights.map(OutputLoudness::new).collect();

    let (mut samples, mut metered) = (Xxh3Default::new(), Xxh3Default::new());
    let (mut pages, mut fingerprints) = (Xxh3Default::new(), Xxh3Default::new());
    let (mut next_seq, mut gaps, mut spans, mut engaged) = (0, 0u64, 0u64, [false; 2]);
    let mut burst_peak = 0.0f32;
    let mut paused = vec![0u64; loudness.len()];
    let saved = samplez_dsp::set_ftz_daz(true);
    for k in (0..BLOCKS).filter(|k| !SKIP.contains(k)) {
        for &(_, id, rerouted) in GENERATIONS.iter().filter(|g| g.0 == k) {
            let pending = Command::Pending {
                id,
                domain: 0,
                generation: generation(&api, &domain, rerouted),
            };
            assert!(cmd.push(pending).is_ok());
        }
        if k == CORE_AT {
            // A buffer of the engine's shape, as `Engine::copy_core` hands
            // it over: the loop fills it after the block.
            let page = PENDING_CHANGES * PAGE.div_ceil(B) as usize;
            let mut buffer = Box::new(Core::new(ROWS, COLUMNS, SLOTS, page, 2));
            buffer.prepare();
            assert!(core_tx.push(buffer).is_ok());
        }
        rt.drain_commands(k);
        rt.process(k, u64::MAX);
        if let Ok(mut copy) = copy_rx.pop() {
            // The delays' history before the block after the core, as
            // `Engine::copy_core` adds it off the RT thread.
            let end = (copy.k0 + 1) * B;
            for (s, slot) in copy.core.slots.iter_mut().zip(&rt.slots) {
                if let (Some(d), Some(l)) = (&mut s.delay, &slot.delay) {
                    let rings = l.rings();
                    let len = d.span_len(rings.max()) as u32;
                    d.span = rings.copy(usize::from(d.set), end, len);
                }
            }
            assert_eq!(copy.k0, CORE_AT);
            let encoded = CoreCopy::encode(&copy);
            assert_ducks_encoded(&encoded, false);
            core = Some(xxhash_rust::xxh3::xxh3_64(encoded.as_bytes()));
        }
        let b = B as usize;
        for c in 0..COLUMNS {
            for v in &rt.outbuf.channel(c)[..b] {
                samples.update(&v.to_bits().to_le_bytes());
            }
        }
        if (BURST.start / B + 50..BURST.end / B).contains(&k) {
            let out = &rt.outbuf.channel(2)[..b];
            burst_peak = out.iter().fold(burst_peak, |m, v| m.max(v.abs()));
        }
        let mut seen = false;
        while let Ok(r) = meter_rx.pop() {
            seen = true;
            assert_eq!(r.seq, next_seq, "a meter record lost");
            next_seq = r.seq + 1;
            gaps += u64::from(r.flags & GAP != 0);
            spans += u64::from(r.flags & SPAN != 0);
            for v in [r.seq, u64::from(r.meter), u64::from(r.channel), r.slice] {
                metered.update(&v.to_le_bytes());
            }
            metered.update(&r.frames.to_le_bytes());
            metered.update(&r.peak.to_bits().to_le_bytes());
            metered.update(&r.true_peak.to_bits().to_le_bytes());
            metered.update(&r.sum_sq.to_bits().to_le_bytes());
            metered.update(&r.plain_sum_sq.to_bits().to_le_bytes());
            metered.update(&r.reset_at.to_le_bytes());
            metered.update(&[r.flags]);
            loudness[r.meter as usize].record(&r);
        }
        if seen {
            for (l, p) in loudness.iter().zip(&mut paused) {
                update_readings(&mut metered, &l.meters);
                *p = (*p).max(l.meters.paused);
            }
        }
        while let Ok(d) = duck_rx.pop() {
            if let Some(e) = engaged.get_mut(usize::from(d.group)) {
                *e |= d.engaged;
            }
            metered.update(&[d.group, u8::from(d.engaged)]);
            metered.update(&d.slice.to_le_bytes());
            metered.update(&d.key.to_bits().to_le_bytes());
            metered.update(&d.envelope.to_bits().to_le_bytes());
        }
        while let Ok(h) = hash_rx.pop() {
            for v in [u64::from(h.output), h.page, h.hash] {
                pages.update(&v.to_le_bytes());
            }
            pages.update(&[u8::from(h.committed)]);
            for v in [u64::from(h.output), h.page, h.fingerprint] {
                fingerprints.update(&v.to_le_bytes());
            }
        }
        while back.pop().is_ok() {}
    }
    samplez_dsp::restore_mxcsr(saved);

    // The scene does what it is for: the burst limited by its true peak
    // alone, slot 1 on its second flow, slot 0's generation late, both
    // groups engaged, gaps metered, no entry late or refused, no record
    // dropped.
    assert!(burst_peak > 0.0 && burst_peak < 0.105, "{burst_peak}");
    assert_eq!((rt.slots[0].flow, rt.slots[1].flow), (0, 1));
    assert_eq!(rt.counters.generation_late.load(Relaxed), 1);
    let c = &rt.counters;
    for counter in [
        &c.command_late,
        &c.strip_refused,
        &c.duck_refused,
        &c.duck_records_dropped,
        &c.meter_dropped,
    ] {
        assert_eq!(counter.load(Relaxed), 0);
    }
    assert_eq!(c.page_hashes_dropped.load(Relaxed), 0);
    assert_eq!(engaged, [!replay; 2]);
    assert!(gaps > 0 && spans > 0);
    for m in [0, AUX, MIX_MINUS] {
        assert!(paused[m] > 0, "meter {m} never paused");
        assert!(loudness[m].meters.running, "meter {m} left paused");
    }
    drop(rt);
    let _ = std::fs::remove_dir_all(&domain);
    Pass {
        samples: samples.digest(),
        meters: metered.digest(),
        pages: pages.digest(),
        fingerprints: fingerprints.digest(),
        core: core.expect("a core copied"),
    }
}

/// The golden hashes: (samples, meters, page hashes, fingerprints), the
/// samples and meters as the engine's loop computes them, the pages as a
/// replay clone does, which computes the same samples.
fn run(name: &str) -> (u64, u64, u64, u64, u64) {
    let live = pass(&format!("{name}-live"), false);
    let replay = pass(&format!("{name}-replay"), true);
    assert_eq!(
        live.samples, replay.samples,
        "a replay clone computes other samples than the loop"
    );
    assert!(replay.pages != Xxh3Default::new().digest());
    (
        live.samples,
        live.meters,
        replay.pages,
        replay.fingerprints,
        live.core,
    )
}

#[test]
fn the_engine_computes_the_bits_pinned_for_this_compute_version() {
    let (s, m, p, f, c) = run("pin");
    assert_eq!(
        f, FINGERPRINTS,
        "the pages' entry fingerprints hash to {f:#018x}: replicas compare \
         them, so a change to them needs a PROTOCOL bump in samplez-replica \
         and this value replaced"
    );
    let pinned = GOLDEN
        .iter()
        .find(|g| g.0 == crate::COMPUTE_VERSION)
        .map(|g| (g.1, g.2, g.3));
    assert_eq!(
        Some((s, m, p)),
        pinned,
        "the engine computes samples {s:#018x}, meters {m:#018x} and page \
         hashes {p:#018x}: a change to the bits it computes needs a \
         COMPUTE_VERSION bump and its hashes here; page hashes moving with \
         the samples unchanged need a PROTOCOL bump in samplez-replica and \
         this version's page hash replaced"
    );
    let pinned = CORES.iter().find(|g| g.0 == crate::COMPUTE_VERSION);
    assert_eq!(
        Some(c),
        pinned.map(|g| g.1),
        "the core copy encodes to {c:#018x}: a change to what it holds or \
         how it encodes needs a COMPUTE_VERSION bump and its hash here"
    );
    let c = phased_core();
    let pinned = PHASED_CORES.iter().find(|g| g.0 == crate::COMPUTE_VERSION);
    assert_eq!(
        Some(c),
        pinned.map(|g| g.1),
        "the small scene's core copy encodes to {c:#018x}: a change to what \
         it holds or how it encodes needs a COMPUTE_VERSION bump and its \
         hash here"
    );
    let (s, c) = tone_scene();
    let pinned = TONES.iter().find(|g| g.0 == crate::COMPUTE_VERSION);
    assert_eq!(
        Some((s, c)),
        pinned.map(|g| (g.1, g.2)),
        "the tone scene computes samples {s:#018x} and its core copy encodes \
         to {c:#018x}: a change to either needs a COMPUTE_VERSION bump and its \
         hashes here"
    );
}

/// The run is a function of its scene and log alone: two runs agree.
#[test]
fn the_engine_golden_run_is_deterministic() {
    assert_eq!(run("first"), run("second"));
    assert_eq!(phased_core(), phased_core());
    assert_eq!(tone_scene(), tone_scene());
}
