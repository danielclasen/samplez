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
