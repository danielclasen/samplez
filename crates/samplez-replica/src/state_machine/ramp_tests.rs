use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;

use samplez::blocks::PageHash;
use samplez::{BlockVerdict, Config, Engine, InputState, Replication, Targets};
use serde::{Deserialize, Serialize};

use super::tests::command;
use super::*;

use super::ramp_common as common;
use common::{B, Domain, Pilot, config, new_id, wait_for};

/// The engine's `rampSamples`: about two pages, so a rebuild inside a
/// ramp leaves pages that depend on it.
const RAMP: u32 = 2048;

/// One mono input through one gain: `g <gain>`.
#[derive(Serialize, Deserialize)]
struct Gain1 {
    gain: f64,
    flow: String,
}

impl Product for Gain1 {
    const DERIVATION_VERSION: u32 = 1;
    type Change = ();
    const DRIVES_ENGINE: bool = true;

    fn apply(&mut self, _: &Origin, command: &[u8], _: &Apply) -> Effect<()> {
        let text = std::str::from_utf8(command).expect("utf-8");
        match text.split(' ').collect::<Vec<_>>()[..] {
            ["g", gain] => self.gain = gain.parse().unwrap(),
            _ => return Effect::Stale,
        }
        Effect::Changed(())
    }
    fn expire(&mut self, _: &SessionKey) -> Effect<()> {
        Effect::Unchanged
    }
    fn forget(&mut self, _: &SessionKey) {}
    fn delta(&self, _: u64, _: &[Changed<'_, ()>]) -> String {
        String::new()
    }
    fn text(&self) -> String {
        String::new()
    }
    fn save(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("encodes")
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        *self = serde_json::from_slice(saved).map_err(|e| e.to_string())?;
        Ok(())
    }
    fn config_hash(&self) -> u64 {
        0
    }
    fn targets(&self) -> Option<Targets> {
        Some(Targets {
            gains: vec![vec![self.gain as f32]],
            flows: vec![Some(self.flow.clone())],
            strips: vec![Vec::new()],
            ..Targets::default()
        })
    }
}

struct Hashed {
    e: Engine,
    hashes: Receiver<PageHash>,
    verdicts: Receiver<BlockVerdict>,
}

fn engine(domain: &Domain, src: &str) -> Hashed {
    let mut cfg: Config = config(
        &domain.0,
        &[(src, 1)],
        &[(&new_id(), 1)],
        vec![vec![1.0]],
        960,
    );
    cfg.ramp_samples = RAMP;
    let (reports, _r) = sync_channel(64);
    let (hash_tx, hashes) = sync_channel(1 << 16);
    let (verdict_tx, verdicts) = sync_channel(1 << 16);
    let (status_tx, _s) = sync_channel(1 << 16);
    let process = samplez::rt::setup_process(&cfg.rt);
    let replication = Replication {
        voters: 2,
        reports,
        statuses: Some(status_tx),
        hashes: Some(hash_tx),
    };
    let e = Engine::start_replicated(cfg, process, Some(verdict_tx), replication).unwrap();
    wait_for(Duration::from_secs(5), "attached and realised", || {
        let s = e.status();
        s.outputs[0].realised && s.inputs[0].state == InputState::Attached
    });
    Hashed {
        e,
        hashes,
        verdicts,
    }
}

fn processed(h: &Hashed) -> u64 {
    h.e.counters()
        .processed
        .load(std::sync::atomic::Ordering::Relaxed)
}

#[test]
fn a_rebuild_inside_a_plain_ramp_matches_its_peers() {
    const PAGE: u64 = samplez::blocks::PAGE;
    let domain = Domain::new();
    let src = new_id();
    let _pilot = Pilot::start(&domain.0, &src, 1, B, 0);
    let (a, c) = (engine(&domain, &src), engine(&domain, &src));
    let limits = Limits {
        ramp_samples: RAMP,
        ..Limits::defaults(48_000)
    };
    let mut sm = StateMachine::new(
        Gain1 {
            gain: 1.0,
            flow: src.clone(),
        },
        limits,
    );
    let api = common::api();
    let now = samplez_mxl::time::timestamp_to_index(&api, samplez_mxl::time::now(&api)) / B;
    let at = (now + 100) * B;
    let ask = |id: &str, body: &str, stamp, at| {
        let mut cmd = command("s", id, body);
        cmd.at_index = Some(at);
        Request {
            stamp,
            body: Body::Commands(vec![cmd]),
        }
    };
    // Two plain changes, the second 10 blocks into the first one's ramp.
    for (n, (body, at)) in [("g 0.25", at), ("g 0.75", at + 10 * B)]
        .into_iter()
        .enumerate()
    {
        let index = n as u64 + 1;
        sm.apply_at(index, 1, &ask(&index.to_string(), body, at, at));
        let record = sm.records[&index].clone();
        a.e.append_batch(index, record.at_index, &record.entries)
            .unwrap();
        a.e.commit(index);
    }
    c.e.drop_core().unwrap();
    let state = sm.product_state(2);
    // Inside the second ramp, the index named so the install is exact.
    let j =
        c.e.install_product(&state, at + 10 * B + 96, Duration::from_secs(2))
            .unwrap();
    // The install lies inside the second ramp, or the test proves nothing.
    let installed = (j + 1) * B;
    assert!(
        (at + 10 * B..at + 10 * B + u64::from(RAMP) - PAGE).contains(&installed),
        "installed at {installed}, outside the ramp"
    );
    let from = ((j + 1) * B).div_ceil(PAGE);
    let (mut pa, mut pc) = (
        std::collections::BTreeMap::new(),
        std::collections::BTreeMap::new(),
    );
    let mut newest = [0u64; 2];
    let mut silenced = Vec::new();
    let mut agree = Vec::new();
    wait_for(Duration::from_secs(10), "pages compared", || {
        pa.extend(a.hashes.try_iter().map(|p| (p.page, p)));
        pc.extend(c.hashes.try_iter().map(|p| (p.page, p)));
        for (i, h) in [&a, &c].into_iter().enumerate() {
            for v in h.verdicts.try_iter() {
                if v.silenced != 0 {
                    silenced.push(v.k);
                }
                newest[i] = newest[i].max(v.k);
            }
        }
        let confirmed = newest[0].min(newest[1]);
        agree = (from..from + 3)
            .filter(|p| (p + 1) * PAGE / B < confirmed)
            .filter(|p| {
                !silenced.iter().any(|k| {
                    (k * B / PAGE..(k * B + u64::from(RAMP)).div_ceil(PAGE) + 1).contains(p)
                })
            })
            .filter_map(|p| Some((p, pa.get(&p)?.hash == pc.get(&p)?.hash)))
            .collect();
        agree.len() >= 2 || processed(&a).min(processed(&c)) * B / PAGE > from + 6
    });
    assert!(agree.len() >= 2, "only {} comparable pages", agree.len());
    assert!(
        agree.iter().all(|&(_, same)| same),
        "pages differ: {agree:?}"
    );
}
