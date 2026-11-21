//! The core [`super::Engine::install_product`] builds from a product
//! state, filled for the block the RT thread is to take it at.

use std::sync::Arc;

use samplez_dsp::duck::DEPTH_DB;
use samplez_dsp::{MAX_DUCKS, MAX_EXACT_RAMP_LEN, Ramp};

use super::{Fade, fold_passed, output_disabled};
use crate::batch::Batch;
use crate::config::Config;
use crate::ducks::DuckCore;
use crate::rt_loop::{Change, LogId, RtEntry, bit, meter_bit, slots_of};
use crate::transfer::{Core, InFlight, ProductState, SlotCore};

/// Refuses in-flight ramps no log leaves on an engine of `config` whose
/// matrix is `(n, m)`: of more inputs or duck groups than it has, a
/// crossfade from a flow an input cannot read, a depth ramp from outside
/// the depth range or longer than an exact ramp, or mask changes outside
/// the matrix or not ending at the coefficient's mask.
pub(crate) fn check(
    config: &Config,
    (n, m): (usize, usize),
    state: &ProductState,
) -> Result<(), String> {
    let f = &state.in_flight;
    let slots = config.inputs.len();
    if f.fade_ins.len() > slots
        || f.crossfades.len() > slots
        || f.depths.len() > MAX_DUCKS
        || f.depths.iter().any(|d| d.len() > slots)
    {
        return Err(format!(
            "ramps in flight of more than {slots} inputs or {MAX_DUCKS} duck groups"
        ));
    }
    for (slot, x) in f.crossfades.iter().enumerate() {
        if let Some((_, Some(flow))) = x {
            config.check_input_flow(slot, flow).map_err(|e| e.0)?;
        }
    }
    if let Some(d) = (f.depths.iter().flatten().flatten())
        .find(|d| !DEPTH_DB.contains(&d.from) || d.len > MAX_EXACT_RAMP_LEN)
    {
        return Err(format!(
            "a depth ramp from {} dB over {} samples is not within the depth bounds",
            d.from, d.len
        ));
    }
    for (input, output, _, changes) in &f.masks {
        let mask = (state.masks.get(*input).and_then(|r| r.get(*output)))
            .copied()
            .unwrap_or(0);
        if *input >= n || *output >= m || changes.last().is_none_or(|c| c.1 != mask) {
            return Err(format!(
                "mask changes of coefficient ({input}, {output}) that do not end at its mask"
            ));
        }
    }
    Ok(())
}

/// Per input slot of `state` the flow a crossfade still running leaves,
/// with the index the crossfade ends at, `ramp` samples after its start.
pub(crate) fn leaving(state: &ProductState, ramp: u32) -> Vec<Option<(u64, String)>> {
    (state.in_flight.crossfades.iter())
        .map(|x| match x {
            Some((start, Some(flow))) => {
                Some((start.saturating_add(u64::from(ramp)), flow.clone()))
            }
            _ => None,
        })
        .collect()
}

/// What an install fills a core from: the product state's targets and
/// ramps and its pending entries, folded, prepared off the RT thread
/// before the first attempt, with the room each attempt fills, so an
/// attempt between two blocks of the RT thread allocates nothing but a
/// mask change's list the passed entries add.
pub(crate) struct ProductFill {
    /// Input channels and columns.
    shape: (usize, usize),
    ramp: u32,
    block: u64,
    last_id: LogId,
    /// Per input slot its channels, as the detectors key them.
    legs: Vec<usize>,
    pending: Vec<RtEntry>,
    batches: Vec<(LogId, Arc<Batch>)>,
    targets: Vec<f32>,
    gains: Vec<f32>,
    /// The fades still running, per coefficient. With none in the state
    /// only the cells passed entries touched (`dirty`) can hold one, so
    /// an attempt resets and scans those alone.
    state_fading: bool,
    fading_targets: Vec<Option<Fade>>,
    fadings: Vec<Option<Fade>>,
    dirty: Vec<usize>,
    resumed: Vec<(usize, Ramp)>,
    up_targets: Vec<bool>,
    up: Vec<bool>,
    fade_in_targets: Vec<Option<u64>>,
    fade_ins: Vec<Option<u64>>,
    /// Per input slot the index its crossfade started at.
    crossfades: Vec<Option<u64>>,
    detached: Vec<bool>,
    /// A bit per output disabled, and per meter paused.
    disabled_targets: u64,
    paused_targets: u128,
    /// Empty for none, as a fill takes them: a state and pending list
    /// without masks or depths pay nothing for them.
    mask_targets: Vec<u8>,
    masks: Vec<u8>,
    /// The mask changes whose crossfades may still run, in
    /// [`InFlight::masks`] alone.
    chain_targets: InFlight,
    chains: InFlight,
    /// Per input channel and group, as the matrix holds them.
    depth_targets: Vec<f32>,
    depths: Vec<f32>,
    depth_fading_targets: Vec<Option<Fade>>,
    depth_fadings: Vec<Option<Fade>>,
    /// Cold detectors at the state's settings, settled: an attempt copies
    /// them.
    cold: DuckCore,
}

impl ProductFill {
    /// The fill of `state`, checked, for an engine of `config` whose
    /// matrix is `shape`, its pending entries `pending`, sorted, with
    /// their `batches`.
    pub(crate) fn new(
        config: &Config,
        shape: (usize, usize),
        state: &ProductState,
        pending: Vec<RtEntry>,
        batches: Vec<(LogId, Arc<Batch>)>,
    ) -> ProductFill {
        let (n, m) = shape;
        let ramp = config.ramp_samples;
        let targets: Vec<f32> = state.gains.iter().flatten().copied().collect();
        let state_fading = !state.fading.is_empty();
        let fading_targets: Vec<Option<Fade>> = if state_fading {
            state.fading.iter().flatten().copied().collect()
        } else {
            vec![None; n * m]
        };
        // An output past the state's list starts as configured.
        let disabled_targets = (config.outputs.iter().enumerate())
            .filter(|&(o, cfg)| !state.outputs.get(o).copied().unwrap_or(cfg.enabled))
            .fold(0, |d, (o, _)| d | bit(o));
        // A meter past the list runs.
        let paused_targets = (state.loudness_running.iter().enumerate())
            .filter(|&(_, &running)| !running)
            .fold(0u128, |p, (meter, _)| p | meter_bit(meter));
        let flight = &state.in_flight;
        let pending_masks = batches.iter().any(|(_, b)| !b.masks.is_empty());
        let pending_depths = batches.iter().any(|(_, b)| !b.depths.is_empty());
        let mask_targets: Vec<u8> = if !state.masks.is_empty() {
            state.masks.iter().flatten().copied().collect()
        } else if pending_masks {
            vec![0; n * m]
        } else {
            Vec::new()
        };
        let any_depth = (state.ducks.iter()).any(|d| d.depths.iter().any(|&db| db != 0.0));
        let depth_ramps = flight.depths.iter().flatten().any(Option::is_some);
        let mut depth_targets = if any_depth || pending_depths || depth_ramps {
            vec![0.0f32; n * MAX_DUCKS]
        } else {
            Vec::new()
        };
        let mut depth_fading_targets = vec![None; depth_targets.len()];
        let rows = super::slot_rows(config);
        if !depth_targets.is_empty() {
            for (g, d) in state.ducks.iter().enumerate() {
                for (slot, &db) in d.depths.iter().enumerate() {
                    let (first, legs) = rows[slot];
                    for row in first..first + legs {
                        depth_targets[row * MAX_DUCKS + g] = db;
                    }
                }
            }
            for (g, slots) in flight.depths.iter().enumerate() {
                for (slot, f) in slots.iter().enumerate() {
                    let (first, legs) = rows[slot];
                    for row in first..first + legs {
                        depth_fading_targets[row * MAX_DUCKS + g] = *f;
                    }
                }
            }
        }
        let legs: Vec<usize> = config.inputs.iter().map(|i| i.channels as usize).collect();
        let mut cold = DuckCore::new(ramp);
        cold.cold(ramp, &state.ducks, |s| legs.get(s).copied().unwrap_or(0));
        cold.settle(ramp, 0);
        let slots = state.up.len();
        let fade_in_targets: Vec<Option<u64>> = (0..slots)
            .map(|s| flight.fade_ins.get(s).copied().flatten())
            .collect();
        let chain_targets = InFlight {
            masks: flight.masks.clone(),
            ..InFlight::default()
        };
        ProductFill {
            shape,
            ramp,
            block: u64::from(config.block_size),
            last_id: state.last_id,
            legs,
            pending,
            batches,
            gains: targets.clone(),
            targets,
            state_fading,
            fadings: fading_targets.clone(),
            fading_targets,
            dirty: Vec::with_capacity(n * m),
            resumed: Vec::with_capacity(n * m),
            up: state.up.clone(),
            up_targets: state.up.clone(),
            fade_ins: fade_in_targets.clone(),
            fade_in_targets,
            crossfades: (0..slots)
                .map(|s| {
                    flight
                        .crossfades
                        .get(s)
                        .and_then(|x| x.as_ref().map(|x| x.0))
                })
                .collect(),
            detached: state.flows.iter().map(Option::is_none).collect(),
            disabled_targets,
            paused_targets,
            masks: mask_targets.clone(),
            mask_targets,
            chains: chain_targets.clone(),
            chain_targets,
            depths: depth_targets.clone(),
            depth_targets,
            depth_fadings: depth_fading_targets.clone(),
            depth_fading_targets,
            cold,
        }
    }

    /// Fills `core` for the block after block `j`, the first it computes:
    /// the targets and ramps of the state with the pending entries whose
    /// index the install passed folded in, which reported late would
    /// never apply, and the rest pending. The block it follows and the
    /// last log id it holds.
    pub(crate) fn fill(&mut self, j: u64, core: &mut Core) -> (u64, LogId) {
        let m = self.shape.1;
        let ramp = self.ramp;
        let next = (j + 1) * self.block;
        let ProductFill {
            pending,
            batches,
            gains,
            fadings,
            dirty,
            up,
            fade_ins,
            masks,
            chains,
            depths,
            depth_fadings,
            legs,
            ..
        } = self;
        gains.copy_from_slice(&self.targets);
        if self.state_fading {
            fadings.copy_from_slice(&self.fading_targets);
        } else {
            for c in dirty.drain(..) {
                fadings[c] = None;
            }
        }
        up.copy_from_slice(&self.up_targets);
        fade_ins.copy_from_slice(&self.fade_in_targets);
        masks.copy_from_slice(&self.mask_targets);
        chains.masks.clone_from(&self.chain_targets.masks);
        depths.copy_from_slice(&self.depth_targets);
        depth_fadings.copy_from_slice(&self.depth_fading_targets);
        let mut disabled = self.disabled_targets;
        let mut paused = self.paused_targets;
        // Whatever a spare core held.
        core.ducks.copy_from(&self.cold);
        let mut ducks_passed = false;
        let batch = |id: LogId| batches.iter().find(|b| b.0 == id).map(|b| &*b.1);
        // A slot raised from down fades in from the entry's index.
        let raise = |up: &mut [bool], fade_ins: &mut [Option<u64>], slot: usize, at: u64| {
            if !up[slot] {
                fade_ins[slot] = Some(at);
            }
            up[slot] = true;
        };
        let passed = pending.partition_point(|e| e.at_index < next);
        for e in &pending[..passed] {
            paused = e.change.paused_after(paused, batch(e.id));
            match e.change {
                Change::Gain {
                    input,
                    output,
                    gain,
                    fade,
                } => {
                    dirty.push(input * m + output);
                    fold_passed(
                        gains,
                        fadings,
                        input * m + output,
                        (gain, fade),
                        e.at_index,
                        ramp,
                    );
                }
                Change::InputDown { slot } => up[slot] = false,
                Change::InputUp { slot } => raise(up, fade_ins, slot, e.at_index),
                Change::Output { output, enabled } => {
                    disabled = output_disabled(disabled, output, enabled);
                }
                // Folded into `paused` above.
                Change::LoudnessRun { .. } => {}
                // The core holds neither generation nor a strip, limiter,
                // delay or tone generator, nor the meters: the RT thread
                // applies a passed re-route, detach, strip, output strip,
                // limiter, delay, output delay, tone or loudness reset entry
                // itself, taken before the install or after.
                Change::Route { .. }
                | Change::Detach { .. }
                | Change::Strip { .. }
                | Change::OutputStrip { .. }
                | Change::Limiter { .. }
                | Change::Delay { .. }
                | Change::OutputDelay { .. }
                | Change::Tone { .. }
                | Change::ResetLoudness { .. } => {}
                Change::Batch { up: raised, .. } => {
                    if let Some(b) = batch(e.id) {
                        b.gains.each(|c, g| {
                            dirty.push(c);
                            fold_passed(
                                &mut gains[..],
                                &mut fadings[..],
                                c,
                                (g, b.fades.get(c)),
                                e.at_index,
                                ramp,
                            );
                        });
                        for &slot in &b.downs {
                            up[slot as usize] = false;
                        }
                        for &(o, enabled) in &b.outputs {
                            disabled = output_disabled(disabled, o as usize, enabled);
                        }
                        // After the gains, as the RT thread applies them.
                        b.masks.each(|c, k| {
                            let term = gains[c] != 0.0
                                || fadings[c]
                                    .is_some_and(|f| f.ramp_at(gains[c], e.at_index).is_some());
                            chains.mask((c / m, c % m), (masks[c], k), e.at_index, ramp, term);
                            masks[c] = k;
                        });
                        for &(row, g, db) in &b.depths {
                            let r = row as usize * MAX_DUCKS + usize::from(g);
                            fold_passed(depths, depth_fadings, r, (db, None), e.at_index, ramp);
                        }
                        for &(g, change) in &b.ducks {
                            let legs = |s: usize| legs.get(s).copied().unwrap_or(0);
                            let _ = core.ducks.apply(usize::from(g), change, legs);
                            ducks_passed = true;
                        }
                    }
                    for slot in slots_of(raised) {
                        raise(up, fade_ins, slot, e.at_index);
                    }
                }
            }
        }
        if ducks_passed {
            core.ducks.settle(ramp, next);
        } else {
            core.ducks.align(next);
        }
        // At the targets from the first sample, whatever a spare core
        // held, ducked as the masks and depths say. Fades and depth ramps
        // still running continue at their value for `next`.
        let resumed = &mut self.resumed;
        resumed.clear();
        let mut ramp_end = 0;
        let mut resume = |c: usize| {
            if let Some(r) = fadings[c].and_then(|f| f.ramp_at(gains[c], next)) {
                ramp_end = ramp_end.max(next + u64::from(r.end() - r.position()));
                resumed.push((c, r));
            }
        };
        if self.state_fading {
            (0..fadings.len()).for_each(&mut resume);
        } else {
            dirty.sort_unstable();
            dirty.dedup();
            dirty.iter().copied().for_each(&mut resume);
        }
        let _ = core
            .matrix
            .fade_in_ducked_resuming(gains, masks, depths, 0, resumed);
        for (r, f) in depth_fadings.iter().enumerate() {
            if let Some(d) = f.and_then(|f| f.ramp_at(depths[r], next)) {
                ramp_end = ramp_end.max(next + u64::from(d.end() - d.position()));
                let _ = core.matrix.resume_depth(r / MAX_DUCKS, r % MAX_DUCKS, d);
            }
        }
        core.matrix.align(next);
        // The duck of each masked term at the gains the live engines hold
        // for the settled detectors, not 1 moving to them, a mask
        // crossfade still running included.
        let mut envelopes = [0.0f32; MAX_DUCKS];
        for (e, d) in envelopes.iter_mut().zip(&core.ducks.detectors) {
            *e = d.envelope();
        }
        core.matrix.settle_ducks(&envelopes);
        for (input, output, from, changes) in &chains.masks {
            let depth = |g: usize, n: u64| {
                let r = input * MAX_DUCKS + g;
                depths.get(r).map_or(0.0, |&db| {
                    depth_fadings[r].map_or(db, |f| f.value_at(db, n))
                })
            };
            let changed = (*from, changes.as_slice());
            let _ =
                core.matrix
                    .resume_mask((*input, *output), changed, ramp, &envelopes, depth, next);
            if let Some(&(last, _)) = changes.last() {
                ramp_end = ramp_end.max(last.saturating_add(u64::from(ramp)));
            }
        }
        core.pos = Some(next);
        core.ramp_end = ramp_end;
        core.pending.clear();
        core.pending.extend_from_slice(&pending[passed..]);
        // Shared: nothing is allocated in the hand-over's window.
        core.batches.clear();
        for e in &pending[passed..] {
            if let Some(b) = batches.iter().find(|b| b.0 == e.id) {
                core.batches.push((e.id, Arc::clone(&b.1)));
            }
        }
        core.page_entries.clear();
        core.entries_page = None;
        core.entries_lost = false;
        core.outputs_disabled = disabled;
        core.loudness_paused = paused;
        // The RT thread keeps its strips. An input that is up and attached
        // continues as it does on a replica that ran through: its fade-in
        // or crossfade where one runs, none where none does.
        let len = u64::from(ramp);
        for (slot, c) in core.slots.iter_mut().enumerate() {
            let (up, detached) = (up[slot], self.detached[slot]);
            *c = SlotCore {
                up,
                silenced_last: !up || detached,
                detached,
                fade_start: fade_ins[slot],
                xfade: self.crossfades[slot].filter(|&s| s.saturating_add(len) > next),
                ..SlotCore::default()
            };
        }
        core.awaiting_gen.clear();
        // The RT thread keeps the generation it holds pending: the
        // supervisor sends one only once its entry is committed, so the
        // status can name one the RT thread does not hold yet.
        core.pending_gen = None;
        core.unresolved.clear();
        core.late_routes.clear();
        core.late_lost_until = 0;
        (j, self.last_id)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use samplez_dsp::DuckMode;
    use samplez_mxl::MxlApi;
    use samplez_mxl::input::Generation;

    use super::ProductFill;
    use crate::blocks::PAGE;
    use crate::config::Config;
    use crate::ducks::{DuckSetting, DuckTargets};
    use crate::engine::{Entry, Fade, replay_rt};
    use crate::rt_loop::{Change, Command, Gen, PENDING_CHANGES, Rt, RtEntry};
    use crate::transfer::{Core, InFlight, ProductState};

    const B: u64 = 48;
    const RAMP: u32 = 480;
    const SLOTS: usize = 3;
    /// Where each case changes what ramps: 200 ms in, a forced duck
    /// group's envelope long at full depth by then.
    const AT: u64 = 9600;

    /// Each slot a triangle per flow, a different one after a re-route.
    fn source(slot: usize, flow: u32, _channel: usize, n: u64) -> f32 {
        let period = 89 + 41 * (slot as u64 * 3 + u64::from(flow) * 2);
        let phase = (n % period) as f32 / period as f32;
        (0.3 + 0.2 * flow as f32) * (4.0 * (phase - 0.5).abs() - 1.0)
    }

    /// Three mono inputs into one output, which ducks.
    fn config(domain: &Path) -> Config {
        let libmxl = std::env::var("SAMPLEZ_TEST_LIBMXL")
            .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
        let inputs = (0..SLOTS).map(|i| {
            serde_json::json!({ "name": format!("in{i}"), "domain": "d",
                "flowId": format!("5fbec3b1-1b0f-417d-9059-00000000000{i}"), "channels": 1 })
        });
        let v = serde_json::json!({
            "libmxl": libmxl,
            "blockSize": B,
            "rampSamples": RAMP,
            "domains": [{ "name": "d", "path": domain }],
            "inputs": inputs.collect::<Vec<_>>(),
            "outputs": [{ "name": "out", "domain": "d",
                "flowId": "6fbec3b1-1b0f-417d-9059-000000000000", "label": "o",
                "group": "g", "role": "Audio", "channels": 1 }],
            "matrix": vec![vec![0.0]; SLOTS],
            "duckColumns": [0],
            "rt": { "enabled": false, "rtCpu": "none" },
        });
        Config::parse(&v.to_string()).unwrap()
    }

    fn rt(config: &Config, api: &Arc<MxlApi>) -> Rt {
        let (hashes, _) = rtrb::RingBuffer::new(1 << 10);
        let (_, control) = rtrb::RingBuffer::new(1);
        let mut rt = replay_rt(config, api, vec![None], hashes, control);
        rt.test_input = Some(source);
        rt
    }

    /// Queues log entry `id` of `entries` at `at` on `rt`.
    fn push(rt: &mut Rt, id: u64, at: u64, entries: &[Entry]) {
        let rows: Vec<(usize, usize)> = (0..SLOTS).map(|s| (s, 1)).collect();
        let mut items = Vec::new();
        for e in entries {
            crate::transfer::items_of(e, &rows, &mut items).unwrap();
        }
        let (change, batch) = crate::batch::fold(&items, 1, SLOTS).unwrap().unwrap();
        let e = RtEntry {
            id,
            at_index: at,
            change,
        };
        assert!(rt.mix.push_with(e, batch).is_ok());
    }

    fn gain(input: usize, gain: f32, fade: Option<Fade>) -> Entry {
        Entry::Gain {
            input,
            output: 0,
            gain,
            fade,
        }
    }

    fn duck(setting: DuckSetting) -> Entry {
        Entry::Duck { group: 0, setting }
    }

    fn mask(input: usize, mask: u8) -> Entry {
        Entry::DuckMask {
            input,
            output: 0,
            mask,
        }
    }

    /// The state of the scene's set-up: every gain 0.5, group 0 forced
    /// on, slot 1 ducked by it `db` deep where `masked`.
    fn state(db: f32, masked: bool) -> ProductState {
        ProductState {
            gains: vec![vec![0.5]; SLOTS],
            fading: Vec::new(),
            up: vec![true; SLOTS],
            flows: (0..SLOTS)
                .map(|i| Some(format!("5fbec3b1-1b0f-417d-9059-00000000000{i}")))
                .collect(),
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new(); SLOTS],
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            ducks: vec![DuckTargets {
                mode: DuckMode::On,
                depths: vec![0.0, db],
                ..DuckTargets::default()
            }],
            masks: vec![vec![0], vec![u8::from(masked)], vec![0]],
            in_flight: InFlight::default(),
            pending: Vec::new(),
            last_id: 0,
            ..Default::default()
        }
    }

    /// One engine computes the scene from index 0, `entries` at their
    /// indices and the generations of `generations`, each (block handed
    /// over, entry id, re-routed slots), as they come; another starts at
    /// block `install` from a core built from `state`, with its slots'
    /// flows at `flows`, as the supervisor hands them over. Both compute
    /// the same bits from the install on, for 40 blocks.
    fn rebuilt_alike(
        name: &str,
        entries: &[(u64, Vec<Entry>)],
        generations: &[(u64, u64, u64)],
        install: u64,
        state: &ProductState,
        flows: [u32; SLOTS],
    ) {
        let domain =
            std::env::temp_dir().join(format!("samplez-product-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&domain);
        std::fs::create_dir(&domain).unwrap();
        let config = config(&domain);
        let api = samplez_mxl::load(&config.libmxl).unwrap();
        let mut a = rt(&config, &api);
        let (mut cmd, cmd_rx) = rtrb::RingBuffer::new(4);
        a.cmd = cmd_rx;
        let (ret, mut back) = rtrb::RingBuffer::new(64);
        a.ret = ret;
        for (id, (at, e)) in entries.iter().enumerate() {
            match e[..] {
                [Entry::Route { slot, .. }] => assert!(a.mix.push(RtEntry {
                    id: id as u64 + 1,
                    at_index: *at,
                    change: Change::Route { slot },
                })),
                [Entry::InputUp { slot }] => assert!(a.mix.push(RtEntry {
                    id: id as u64 + 1,
                    at_index: *at,
                    change: Change::InputUp { slot },
                })),
                [Entry::InputDown { slot }] => assert!(a.mix.push(RtEntry {
                    id: id as u64 + 1,
                    at_index: *at,
                    change: Change::InputDown { slot },
                })),
                _ => push(&mut a, id as u64 + 1, *at, e),
            }
        }
        let mut b = rt(&config, &api);
        let saved = samplez_dsp::set_ftz_daz(true);
        let mut want = Vec::new();
        for k in 0..install + 40 {
            for &(_, id, rerouted) in generations.iter().filter(|g| g.0 == k) {
                let generation = Box::new(Gen {
                    mxl: Generation::new(&api, &domain, SLOTS).unwrap(),
                    prune_after: vec![u64::MAX; SLOTS],
                    prune_late: vec![u64::MAX; SLOTS],
                    rerouted,
                });
                let pending = Command::Pending {
                    id,
                    domain: 0,
                    generation,
                };
                assert!(cmd.push(pending).is_ok());
            }
            a.drain_commands(k);
            a.process(k, u64::MAX);
            if k >= install {
                want.extend_from_slice(&a.outbuf.channel(0)[..B as usize]);
            }
            while back.pop().is_ok() {}
        }
        assert_eq!(
            a.counters
                .generation_late
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        for (slot, &flow) in flows.iter().enumerate() {
            b.slots[slot].flow = flow;
        }
        let page = PENDING_CHANGES * PAGE.div_ceil(B) as usize;
        let mut core = Box::new(Core::new(SLOTS, 1, SLOTS, page, 1));
        let mut fill = ProductFill::new(&config, (SLOTS, 1), state, Vec::new(), Vec::new());
        let (j, last_id) = fill.fill(install - 1, &mut core);
        assert_eq!(j + 1, install);
        b.install_core(&mut core, last_id, false);
        let mut got = Vec::new();
        for k in install..install + 40 {
            b.process(k, u64::MAX);
            got.extend_from_slice(&b.outbuf.channel(0)[..B as usize]);
        }
        samplez_dsp::restore_mxcsr(saved);
        drop((a, b));
        let _ = std::fs::remove_dir_all(&domain);
        let first = (got.iter().zip(&want)).position(|(x, y)| x.to_bits() != y.to_bits());
        assert_eq!(
            first, None,
            "{name}: the rebuilt engine differs from sample {first:?} of the install on"
        );
    }

    /// The set-up at index 0: every gain 0.5, group 0 forced on, slot 1
    /// `db` deep, its coefficient masked by the group where `masked`.
    fn setup(db: f32, masked: bool) -> (u64, Vec<Entry>) {
        let mut e: Vec<Entry> = (0..SLOTS).map(|s| gain(s, 0.5, None)).collect();
        e.push(duck(DuckSetting::Mode(DuckMode::On)));
        e.push(duck(DuckSetting::Depth { slot: 1, db }));
        e.push(mask(1, u8::from(masked)));
        (0, e)
    }

    /// A fade applied late, which slot 0's coefficient joins from the 0.5
    /// it held, continues on an engine rebuilt inside it from a state
    /// naming the joined fade, as a replica's state names it.
    #[test]
    fn a_rebuilt_engine_continues_a_joined_fade() {
        let f = Fade {
            from: 0.2,
            start: AT / 2,
            len: 9600,
        };
        let joined = f.joined(1.0, 0.5, AT, RAMP).unwrap();
        assert_ne!(joined, f);
        let entries = [setup(20.0, true), (AT, vec![gain(0, 1.0, Some(f))])];
        let mut state = state(20.0, true);
        state.gains[0][0] = 1.0;
        state.fading = vec![vec![None]; SLOTS];
        state.fading[0][0] = Some(joined);
        rebuilt_alike("joined", &entries, &[], (AT + 2400) / B, &state, [0; SLOTS]);
    }

    /// A depth ramp of a ducked coefficient, under a group forced on,
    /// continues on an engine rebuilt inside it, its duck's gains at the
    /// last two ends of an interval those of the depths there.
    #[test]
    fn a_rebuilt_engine_continues_a_depth_ramp() {
        let entries = [
            setup(10.0, true),
            (AT, vec![duck(DuckSetting::Depth { slot: 1, db: 30.0 })]),
        ];
        let mut state = state(30.0, true);
        let ramp = Fade {
            from: 10.0,
            start: AT,
            len: RAMP,
        };
        state.in_flight.depths = vec![vec![None, Some(ramp)]];
        rebuilt_alike("depth", &entries, &[], (AT + 240) / B, &state, [0; SLOTS]);
    }

    /// A mask crossfade under a group forced on, and a change back
    /// inside it, continue on an engine rebuilt inside both.
    #[test]
    fn a_rebuilt_engine_continues_a_mask_crossfade() {
        let back = AT + 4 * B;
        let entries = [
            setup(20.0, false),
            (AT, vec![mask(1, 1)]),
            (back, vec![mask(1, 0)]),
        ];
        let mut state = state(20.0, false);
        state.in_flight.masks = vec![(1, 0, 0, vec![(AT, 1), (back, 0)])];
        rebuilt_alike("mask", &entries, &[], back / B + 1, &state, [0; SLOTS]);
    }

    /// An input's fade-in after an `InputUp` continues on an engine
    /// rebuilt inside it.
    #[test]
    fn a_rebuilt_engine_continues_an_input_fade_in() {
        let entries = [
            setup(0.0, false),
            (AT / 2, vec![Entry::InputDown { slot: 2 }]),
            (AT, vec![Entry::InputUp { slot: 2 }]),
        ];
        let mut state = state(0.0, false);
        state.in_flight.fade_ins = vec![None, None, Some(AT)];
        rebuilt_alike(
            "fade-in",
            &entries,
            &[(AT / B - 5, 3, 0)],
            AT / B + 3,
            &state,
            [0; SLOTS],
        );
    }

    /// The supervisor reads the flow a crossfade still running leaves
    /// until the crossfade ends; one from silence leaves none.
    #[test]
    fn a_crossfade_leaves_its_flow_to_the_supervisor_until_its_end() {
        let mut s = state(0.0, false);
        let from = "5fbec3b1-1b0f-417d-9059-000000000009".to_owned();
        s.in_flight.crossfades = vec![Some((AT, Some(from.clone()))), Some((AT, None))];
        let want = vec![Some((AT + u64::from(RAMP), from)), None];
        assert_eq!(super::leaving(&s, RAMP), want);
    }

    /// A re-route's crossfade continues on an engine rebuilt inside it,
    /// its generation reading the flow the crossfade leaves.
    #[test]
    fn a_rebuilt_engine_continues_a_route_crossfade() {
        let to = "5fbec3b1-1b0f-417d-9059-000000000009".to_owned();
        let entries = [
            setup(0.0, false),
            (
                AT,
                vec![Entry::Route {
                    slot: 0,
                    flow_id: to.clone(),
                }],
            ),
        ];
        let mut state = state(0.0, false);
        let from = state.flows[0].replace(to);
        state.in_flight.crossfades = vec![Some((AT, from))];
        rebuilt_alike(
            "route",
            &entries,
            &[(AT / B - 5, 2, 0b1)],
            AT / B + 3,
            &state,
            [1, 0, 0],
        );
    }
}
