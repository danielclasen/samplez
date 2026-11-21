//! What the RT benchmarks (`benches/rt.rs`) drive of the crate's private
//! RT loop. Not an API.

use std::sync::Arc;

use crate::config::Config;
use crate::rt_loop::{Change, MeterRecord, Rt, RtEntry};

/// An RT loop off any flow of `inputs` mono inputs, the odd ones feeding
/// every channel of `outputs` stereo outputs, with `entries` entries
/// pending inside a run of skipped blocks: every even one takes an even
/// slot down, which feeds no meter, every odd one sets a gain of an odd
/// input.
pub struct Skip {
    rt: Rt,
    k: u64,
    _records: rtrb::Consumer<MeterRecord>,
}

impl Skip {
    pub fn new(libmxl: &str, inputs: usize, outputs: usize, entries: usize) -> Skip {
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": 48,
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": (0..inputs).map(|i| serde_json::json!({
                "name": format!("in{i}"), "domain": "d",
                "flowId": format!("5fbec3b1-1b0f-417d-9059-{i:012}"), "channels": 1 }))
                .collect::<Vec<_>>(),
            "outputs": (0..outputs).map(|o| serde_json::json!({
                "name": format!("out{o}"), "domain": "d",
                "flowId": format!("6fbec3b1-1b0f-417d-9059-{o:012}"), "label": "o",
                "group": "g", "role": "Audio", "channels": 2 }))
                .collect::<Vec<_>>(),
            "matrix": vec![vec![1.0; 2 * outputs]; inputs],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).expect("a valid configuration");
        let api = samplez_mxl::load(std::path::Path::new(libmxl)).expect("libmxl");
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let gens = (0..1).map(|_| None).collect();
        let mut rt = crate::engine::replay_rt(&config, &Arc::clone(&api), gens, hashes, control);
        // The odd inputs feed every column, the even ones none.
        for i in (1..inputs).step_by(2) {
            for o in 0..2 * outputs {
                assert!(rt.mix.matrix.set(i, o, 1.0, 0));
            }
        }
        let (tx, records) = rtrb::RingBuffer::new(1 << 16);
        rt.meters = tx;
        let b = 48;
        let (from, k) = (1000, 1000 + 4 * entries as u64 + 4);
        rt.mix.pos = Some(from * b);
        for (n, e) in (0..entries).enumerate() {
            let change = if n % 2 == 0 {
                Change::InputDown { slot: e % inputs }
            } else {
                Change::Gain {
                    input: e % inputs,
                    output: e % (2 * outputs),
                    gain: 0.5,
                    fade: None,
                }
            };
            let entry = RtEntry {
                id: n as u64 + 1,
                at_index: (from + 4 * n as u64 + 2) * b,
                change,
            };
            assert!(rt.mix.push(entry));
        }
        Skip {
            rt,
            k,
            _records: records,
        }
    }

    /// Meters the skipped blocks before the next one.
    pub fn run(&mut self) {
        self.rt.bring_to_block(self.k);
    }
}

/// An RT loop off any flow of one mono input feeding every channel of
/// `outputs` stereo outputs, each correlating its legs with
/// `correlation`, before the block that ends a meter slice.
pub struct Slice {
    rt: Rt,
    k: u64,
    _records: rtrb::Consumer<MeterRecord>,
}

impl Slice {
    pub fn new(libmxl: &str, outputs: usize, correlation: bool) -> Slice {
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": 48,
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "5fbec3b1-1b0f-417d-9059-000000000000", "channels": 1 }],
            "outputs": (0..outputs).map(|o| serde_json::json!({
                "name": format!("out{o}"), "domain": "d",
                "flowId": format!("6fbec3b1-1b0f-417d-9059-{o:012}"), "label": "o",
                "group": "g", "role": "Audio", "channels": 2, "correlation": correlation }))
                .collect::<Vec<_>>(),
            "matrix": [vec![1.0; 2 * outputs]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).expect("a valid configuration");
        let api = samplez_mxl::load(std::path::Path::new(libmxl)).expect("libmxl");
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = crate::engine::replay_rt(&config, &api, vec![None], hashes, control);
        let (tx, mut records) = rtrb::RingBuffer::new(1 << 16);
        rt.meters = tx;
        // Blocks of 48 samples, ten to a slice: the slice ends with the
        // tenth block, computed in the benchmark.
        for k in 1000..1009 {
            rt.process(k, u64::MAX);
        }
        assert!(records.pop().is_err(), "a slice ended before the benchmark");
        Slice {
            rt,
            k: 1009,
            _records: records,
        }
    }

    /// Computes and meters the block that ends the slice.
    pub fn run(&mut self) {
        self.rt.process(self.k, u64::MAX);
    }
}

/// An RT loop off any flow of one mono input feeding every channel of
/// `outputs` stereo outputs, each with its limiter on and, with
/// `processed`, a strip with every unit on and a delay line delaying
/// by 240 samples, their ramps and the line's crossfade ended, before a
/// block that computes them.
pub struct Outputs {
    rt: Rt,
    k: u64,
    _records: rtrb::Consumer<MeterRecord>,
}

impl Outputs {
    pub fn new(libmxl: &str, outputs: usize, processed: bool) -> Outputs {
        use samplez_dsp::LimiterParams;
        use samplez_dsp::strip::{CompParams, DEFAULT_BANDS, EqBand, GateParams, StripParam};
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": 48,
            "domains": [{ "name": "d", "path": "/nonexistent" }],
            "inputs": [{ "name": "in", "domain": "d",
                         "flowId": "5fbec3b1-1b0f-417d-9059-000000000000", "channels": 1 }],
            "outputs": (0..outputs).map(|o| {
                let mut out = serde_json::json!({
                    "name": format!("out{o}"), "domain": "d",
                    "flowId": format!("6fbec3b1-1b0f-417d-9059-{o:012}"), "label": "o",
                    "group": "g", "role": "Audio", "channels": 2, "limiter": true });
                if processed {
                    out["strip"] = serde_json::json!(true);
                    out["maxDelaySamples"] = serde_json::json!(480);
                }
                out
            })
            .collect::<Vec<_>>(),
            "matrix": [vec![1.0; 2 * outputs]],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        let config = Config::parse(&v.to_string()).expect("a valid configuration");
        let api = samplez_mxl::load(std::path::Path::new(libmxl)).expect("libmxl");
        let (hashes, _) = rtrb::RingBuffer::new(1);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = crate::engine::replay_rt(&config, &api, vec![None], hashes, control);
        let (tx, records) = rtrb::RingBuffer::new(1 << 16);
        rt.meters = tx;
        let limiter = LimiterParams {
            enabled: true,
            ceiling_db: -6.0,
            release_ms: 50.0,
        };
        let mut strip = vec![
            StripParam::Trim { db: 6.0 },
            StripParam::Hpf {
                enabled: true,
                freq: 80.0,
            },
            StripParam::Eq { enabled: true },
            StripParam::Gate(GateParams {
                enabled: true,
                threshold_db: -60.0,
                range_db: 40.0,
                attack_ms: 1.0,
                release_ms: 50.0,
            }),
            StripParam::Comp(CompParams {
                enabled: true,
                threshold_db: -20.0,
                ratio: 4.0,
                attack_ms: 1.0,
                release_ms: 50.0,
                knee_db: 6.0,
                makeup_db: 3.0,
            }),
        ];
        for (band, b) in DEFAULT_BANDS.iter().enumerate() {
            let value = EqBand { gain_db: 6.0, ..*b };
            strip.push(StripParam::EqBand { band, value });
        }
        // One output's entries at the start of each block, under the
        // pending list's capacity.
        let b = 48;
        let mut k = 1000;
        let mut id = 0;
        for output in 0..outputs {
            let mut changes = vec![Change::Limiter {
                output,
                params: limiter,
            }];
            if processed {
                changes.extend(
                    strip
                        .iter()
                        .map(|&param| Change::OutputStrip { output, param }),
                );
                changes.push(Change::OutputDelay {
                    output,
                    samples: 240,
                });
            }
            for change in changes {
                id += 1;
                let entry = RtEntry {
                    id,
                    at_index: k * b,
                    change,
                };
                assert!(rt.mix.push(entry));
            }
            rt.process(k, u64::MAX);
            k += 1;
        }
        // Past the ramps and the line's crossfade, 480 samples each.
        for _ in 0..20 {
            rt.process(k, u64::MAX);
            k += 1;
        }
        Outputs {
            rt,
            k,
            _records: records,
        }
    }

    /// Computes the next block.
    pub fn run(&mut self) {
        self.rt.process(self.k, u64::MAX);
    }
}
