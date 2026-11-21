//! [`Audio`] over a replicated samplez [`Engine`]: F and unresolved
//! entries go to the engine's control ring, the records the applied log
//! entries derived to its entry ring by log id, and reports read the
//! status words, the processed index, the clock flags and the latest page
//! hashes.
//!
//! Reports carry the status words of the latest blocks rather than the
//! changes since the last report, so a change the engine's ring or
//! channel dropped (counted) costs nothing: the word says it, and a block
//! without a word below the processed one was skipped. Only an unwritten
//! range, which no word records, is lost with its change.
//!
//! A sample drains the engine's status-change and page-hash channels into
//! the unwritten ranges and page records every sample reads, so any
//! caller may sample beside contact without taking anything from it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use samplez::blocks::{
    BlockStatus, Cause, HEALTHY, Kind, PAGE, PageHash, Range, Ranks, Standing, StatusChange,
};
use samplez::{
    CopyError, CoreCopy, DuckSetting, Engine, Entry, EntryError, InputState, ProductState,
    ReplayError,
};

use crate::contact::{Audio, Sample, Timing};
use crate::report::{ClockFlags, Own, Page, Unwritten, page_digest, runs};
use crate::resync::{CoreReply, Failed, Resynced, Source};

/// How long a rebuild waits for the RT thread to take the core, past the
/// floor's lead over the engine ([`EngineAudio::rebuild_engine`]).
const REBUILD_WAIT: Duration = Duration::from_millis(100);

/// How long a donor waits for its RT thread to copy the core, and how
/// often it copies again when the history slack passed meanwhile.
const COPY_WAIT: Duration = Duration::from_millis(100);
const COPY_TRIES: usize = 3;

/// How long a joiner's replay may run on before it gives up, and how long
/// each hand-over is tried before the log is read again.
const REPLAY_WAIT: Duration = Duration::from_secs(2);
const HANDOVER_TRY: Duration = Duration::from_millis(20);

/// Unwritten ranges kept for reports, at most.
const RANGES_KEPT: usize = 64;

/// Page hashes per output a report carries: every page whose record
/// arrived within the last four pages.
pub const PAGES_REPORTED: usize = 4;

struct Unwrittens {
    changes: Receiver<StatusChange>,
    kept: VecDeque<Unwritten>,
    hashes: Receiver<PageHash>,
    /// Per output, the latest page records.
    pages: Vec<VecDeque<Page>>,
}

/// A page record as reports carry it: with the degradation the statuses
/// of its blocks show, read as the record arrives, once every one of them
/// is final.
pub fn page_of(h: &PageHash, block: u64, status: impl Fn(u64) -> Option<BlockStatus>) -> Page {
    let (first, last) = (h.page * PAGE / block, ((h.page + 1) * PAGE - 1) / block);
    let digests: Vec<Option<u64>> = (first..=last)
        .map(|k| status(k).and_then(|s| s.digest))
        .collect();
    Page {
        output: h.output,
        page: h.page,
        hash: h.hash,
        fingerprint: h.fingerprint,
        committed: h.committed,
        degraded: digests.iter().any(|d| d.is_some_and(|d| d != HEALTHY)),
        digest: page_digest(digests),
    }
}

/// Block `k`'s status word, read at or below the processed block. A block
/// there without a word is never written: it was skipped (the RT thread
/// gives a computed block its first status before it publishes it
/// processed, and computes no index twice), or its output had no writer or
/// no commit start yet. So it is withheld, though the change saying so may
/// have been lost.
pub fn word_or_skip(word: Option<BlockStatus>, k: u64) -> BlockStatus {
    word.unwrap_or(BlockStatus {
        k,
        kind: Kind::Withheld,
        range: Range::Whole,
        through: false,
        rank: 0,
        cause: Cause::Skip,
        on_silence_of: 0,
        digest: None,
    })
}

/// The entry setting slot `slot`'s tone generator to `p`.
fn tone_entry(slot: usize, p: samplez_dsp::ToneParams) -> Entry {
    Entry::Tone {
        slot,
        frequency_hz: p.frequency_hz,
        level_db: p.level_db,
        ident: p.ident,
    }
}

pub struct EngineAudio {
    engine: Arc<Engine>,
    outputs: usize,
    /// The outputs and the configured meters.
    meters: usize,
    block: u64,
    /// Blocks up to the processed one whose statuses reports carry.
    window: u64,
    unwritten: Mutex<Unwrittens>,
    /// Appended entries the engine refused.
    refused: AtomicU64,
    /// Derived entries replaced or left out before the engine saw them.
    filtered: AtomicU64,
    /// `maxApplyLeadMs`.
    max_apply_lead: Duration,
    /// A failed rebuild was logged; cleared by one that succeeds.
    rebuild_failed: AtomicBool,
}

impl EngineAudio {
    /// Bridges `engine`, started replicated with `changes` as its status
    /// channel and `hashes` as its page-hash channel. Reports carry the
    /// statuses of the blocks a peer can still hold a decision on: those
    /// of the last `D + w + 2p`, and one more; and the latest
    /// [`PAGES_REPORTED`] page records per output.
    pub fn new(
        engine: Arc<Engine>,
        changes: Receiver<StatusChange>,
        hashes: Receiver<PageHash>,
        timing: &Timing,
    ) -> EngineAudio {
        let status = engine.status();
        let (outputs, meters) = (
            status.outputs.len(),
            status.outputs.len() + status.meters.len(),
        );
        let span = timing.hold + timing.contact_window + 2 * timing.report_period;
        let samples = (span.as_nanos() as u64 * crate::contact::RATE).div_ceil(1_000_000_000);
        EngineAudio {
            outputs,
            meters,
            engine,
            block: timing.block,
            window: samples.div_ceil(timing.block) + 1,
            refused: AtomicU64::new(0),
            filtered: AtomicU64::new(0),
            max_apply_lead: timing.max_apply_lead,
            rebuild_failed: AtomicBool::new(false),
            unwritten: Mutex::new(Unwrittens {
                changes,
                kept: VecDeque::new(),
                hashes,
                pages: (0..outputs).map(|_| VecDeque::new()).collect(),
            }),
        }
    }

    /// The entries of entry `index`'s record the engine takes
    /// ([`EngineAudio::kept`]), a strip parameter, duck setting or duck
    /// mask it refuses replaced first ([`EngineAudio::admitted`]). What is
    /// replaced or left out is counted and logged: the product derived
    /// what it must not.
    fn filter(&self, index: u64, entries: &[Entry]) -> Vec<Entry> {
        let mut replaced = 0;
        let admitted: Vec<Entry> = (entries.iter())
            .filter_map(|e| {
                let a = self.admitted(e);
                replaced += usize::from(a.as_ref() != Some(e));
                a
            })
            .collect();
        let kept = self.kept(&admitted);
        let dropped = (replaced + admitted.len() - kept.len()) as u64;
        if dropped > 0 {
            self.filtered.fetch_add(dropped, Ordering::Relaxed);
            tracing::error!(
                index,
                dropped,
                "the product derived engine entries the engine refuses: replaced or left out"
            );
        }
        kept
    }

    /// Of `entries`, those the engine takes: all of them, or, when it
    /// refuses them together, those it takes alone, of the `InputUp`s,
    /// `Route`s and `Detach`es only those it takes with the ones of lower
    /// slots, so one
    /// generation's worth stays. The engine's configuration alone decides
    /// it, the same on every replica of a compatibility key, so their
    /// outputs still agree.
    fn kept(&self, entries: &[Entry]) -> Vec<Entry> {
        if self.engine.admits(entries).is_ok() {
            return entries.to_vec();
        }
        let built = |e: &Entry| {
            matches!(
                e,
                Entry::InputUp { .. } | Entry::Route { .. } | Entry::Detach { .. }
            )
        };
        let alone: Vec<&Entry> = (entries.iter())
            .filter(|e| self.engine.admits(std::slice::from_ref(*e)).is_ok())
            .collect();
        let mut kept: Vec<Entry> = alone
            .iter()
            .filter(|e| !built(e))
            .map(|e| (*e).clone())
            .collect();
        let mut generation: Vec<&Entry> = alone.into_iter().filter(|e| built(e)).collect();
        generation.sort_by_key(|e| match e {
            Entry::InputUp { slot } | Entry::Route { slot, .. } | Entry::Detach { slot } => *slot,
            _ => usize::MAX,
        });
        for e in generation {
            kept.push(e.clone());
            if self.engine.admits(&kept).is_err() {
                kept.pop();
            }
        }
        kept
    }

    /// `entry`, or, for a slot's or an output's strip parameter, limiter or
    /// tone parameters,
    /// duck setting or duck mask the engine refuses alone, the first of
    /// these the engine takes: its values moved to the nearest ones their
    /// ranges hold ([`samplez_dsp::StripParam::clamped`],
    /// [`samplez_dsp::LimiterParams::clamped`],
    /// [`samplez_dsp::ToneParams::clamped`], [`DuckSetting::clamped`]),
    /// for a finite value out of range; its neutral form
    /// ([`samplez_dsp::StripParam::neutral`], [`DuckSetting::neutral`],
    /// mask 0, [`samplez_dsp::LimiterParams::OFF`], no delay,
    /// [`samplez_dsp::ToneParams::DEFAULT`]), value not finite or key the
    /// engine cannot take, such as a slot or key it lacks. None when it takes
    /// neither. Every other entry as given. The live filter
    /// and the rebuild's state both go through it, so a replica that held
    /// a value before the refused one and one rebuilt without it hold
    /// the same; the engine's configuration alone decides it, the same on
    /// every replica of a key.
    fn admitted(&self, entry: &Entry) -> Option<Entry> {
        let neutral = match entry {
            Entry::Strip { slot, param } => Entry::Strip {
                slot: *slot,
                param: param.neutral(),
            },
            Entry::OutputStrip { output, param } => Entry::OutputStrip {
                output: *output,
                param: param.neutral(),
            },
            Entry::Duck { group, setting } => Entry::Duck {
                group: *group,
                setting: setting.neutral(),
            },
            &Entry::DuckMask { input, output, .. } => Entry::DuckMask {
                input,
                output,
                mask: 0,
            },
            &Entry::Limiter { output, .. } => Entry::Limiter {
                output,
                params: samplez_dsp::LimiterParams::OFF,
            },
            &Entry::Delay { slot, .. } => Entry::Delay { slot, samples: 0 },
            &Entry::OutputDelay { output, .. } => Entry::OutputDelay { output, samples: 0 },
            &Entry::Tone { slot, .. } => tone_entry(slot, samplez_dsp::ToneParams::DEFAULT),
            _ => return Some(entry.clone()),
        };
        let clamped = match entry {
            Entry::Strip { slot, param } => param
                .clamped()
                .map(|param| Entry::Strip { slot: *slot, param }),
            Entry::OutputStrip { output, param } => {
                param.clamped().map(|param| Entry::OutputStrip {
                    output: *output,
                    param,
                })
            }
            Entry::Duck { group, setting } => setting.clamped().map(|setting| Entry::Duck {
                group: *group,
                setting,
            }),
            Entry::Limiter { output, params } => params.clamped().map(|params| Entry::Limiter {
                output: *output,
                params,
            }),
            &Entry::Tone {
                slot,
                frequency_hz,
                level_db,
                ident,
            } => samplez_dsp::ToneParams {
                frequency_hz,
                level_db,
                ident,
            }
            .clamped()
            .map(|p| tone_entry(slot, p)),
            _ => None,
        };
        let takes = |e: &Entry| self.engine.admits(std::slice::from_ref(e)).is_ok();
        if takes(entry) {
            return Some(entry.clone());
        }
        [clamped, Some(neutral)].into_iter().flatten().find(takes)
    }

    /// Per input slot, the flow it reads; none for a detached one and a
    /// tone generator.
    fn reading(&self) -> Vec<Option<String>> {
        (self.engine.status().inputs.into_iter())
            .map(|i| {
                let reads = !matches!(i.state, InputState::Detached | InputState::Tone);
                reads.then_some(i.flow_id)
            })
            .collect()
    }

    /// `state` as the records the bridge filters leave an engine: a flow
    /// differing from the one its input reads is a re-route, a detached
    /// input reading none and a slot without a flow detached, kept by the
    /// rules of [`EngineAudio::kept`] and else left at the one read; a
    /// strip parameter, duck setting or duck mask the engine refuses is
    /// replaced as the live filter replaces it ([`EngineAudio::admitted`])
    /// or left out, output strip, limiter, delay and tone parameters too,
    /// those of an output beyond the engine's left out; a gain it refuses is
    /// silenced, as the state machine
    /// derives it for every replica
    /// ([`crate::state_machine::StateMachine::product_state`] holds none).
    /// A replica rebuilt from it holds what a live one that read the same
    /// flows holds, flows, strips, gains and ducks alike.
    fn sanitised(&self, state: &ProductState) -> ProductState {
        self.sanitised_with(state, &self.reading())
    }

    /// [`EngineAudio::sanitised`] against inputs reading `reading`.
    fn sanitised_with(&self, state: &ProductState, reading: &[Option<String>]) -> ProductState {
        let mut state = state.clone();
        let routes: Vec<Entry> = (state.flows.iter().enumerate())
            .filter(|&(slot, flow)| reading.get(slot).is_some_and(|r| r != flow))
            .map(|(slot, flow)| match flow {
                Some(flow_id) => Entry::Route {
                    slot,
                    flow_id: flow_id.clone(),
                },
                None => Entry::Detach { slot },
            })
            .collect();
        let kept = self.kept(&routes);
        let mut dropped = routes.len() - kept.len();
        // The slots left at the flow they read.
        let mut left = Vec::new();
        for route in &routes {
            if let Entry::Route { slot, .. } | Entry::Detach { slot } = *route
                && !kept.contains(route)
            {
                state.flows[slot] = reading[slot].clone();
                left.push(slot);
            }
        }
        for (slot, params) in state.strips.iter_mut().enumerate() {
            let mut admitted = Vec::with_capacity(params.len());
            for &param in params.iter() {
                match self.admitted(&Entry::Strip { slot, param }) {
                    Some(Entry::Strip { param: p, .. }) => {
                        dropped += usize::from(p != param);
                        admitted.push(p);
                    }
                    _ => dropped += 1,
                }
            }
            *params = admitted;
        }
        // Output strip parameters the engine refuses are replaced as the
        // live filter replaces them, or left out, for an output without a
        // strip; those of an output beyond the engine's too.
        dropped += (state.output_strips.iter().skip(self.outputs))
            .map(Vec::len)
            .sum::<usize>();
        state.output_strips.truncate(self.outputs);
        for (output, params) in state.output_strips.iter_mut().enumerate() {
            let mut admitted = Vec::with_capacity(params.len());
            for &param in params.iter() {
                match self.admitted(&Entry::OutputStrip { output, param }) {
                    Some(Entry::OutputStrip { param: p, .. }) => {
                        dropped += usize::from(p != param);
                        admitted.push(p);
                    }
                    _ => dropped += 1,
                }
            }
            *params = admitted;
        }
        // Limiter parameters the engine refuses are replaced as the live
        // filter replaces them, or left out, for an output without a
        // limiter; those of an output beyond the engine's too.
        dropped += state.limiters.len().saturating_sub(self.outputs);
        state.limiters.truncate(self.outputs);
        for (output, params) in state.limiters.iter_mut().enumerate() {
            let Some(p) = *params else { continue };
            *params = match self.admitted(&Entry::Limiter { output, params: p }) {
                Some(Entry::Limiter { params: q, .. }) => {
                    dropped += usize::from(q != p);
                    Some(q)
                }
                _ => {
                    dropped += 1;
                    None
                }
            };
        }
        // The running state of meters beyond the engine's is left out, as
        // the live filter leaves out their entries.
        dropped += state.loudness_running.len().saturating_sub(self.meters);
        state.loudness_running.truncate(self.meters);
        // A delay the engine refuses is replaced as the live filter
        // replaces it: no delay; those of slots beyond the engine's left
        // out.
        let slots = state.flows.len();
        dropped += state.delays.len().saturating_sub(slots);
        state.delays.truncate(slots);
        for (slot, samples) in state.delays.iter_mut().enumerate() {
            match self.admitted(&Entry::Delay {
                slot,
                samples: *samples,
            }) {
                Some(Entry::Delay { samples: s, .. }) if s == *samples => {}
                Some(Entry::Delay { samples: s, .. }) => {
                    *samples = s;
                    dropped += 1;
                }
                _ => {
                    *samples = 0;
                    dropped += 1;
                }
            }
        }
        // An output delay the engine refuses is replaced as the live filter
        // replaces it: no delay; those of outputs beyond the engine's left
        // out.
        dropped += state.output_delays.len().saturating_sub(self.outputs);
        state.output_delays.truncate(self.outputs);
        for (output, samples) in state.output_delays.iter_mut().enumerate() {
            match self.admitted(&Entry::OutputDelay {
                output,
                samples: *samples,
            }) {
                Some(Entry::OutputDelay { samples: s, .. }) if s == *samples => {}
                Some(Entry::OutputDelay { samples: s, .. }) => {
                    *samples = s;
                    dropped += 1;
                }
                _ => {
                    *samples = 0;
                    dropped += 1;
                }
            }
        }
        // Tone parameters the engine refuses are replaced as the live
        // filter replaces them, or left out, for a slot without a
        // generator; those of slots beyond the engine's too.
        dropped += state.tones.len().saturating_sub(slots);
        state.tones.truncate(slots);
        for (slot, params) in state.tones.iter_mut().enumerate() {
            let Some(p) = *params else { continue };
            *params = match self.admitted(&tone_entry(slot, p)) {
                Some(Entry::Tone {
                    frequency_hz,
                    level_db,
                    ident,
                    ..
                }) => {
                    let q = samplez_dsp::ToneParams {
                        frequency_hz,
                        level_db,
                        ident,
                    };
                    dropped += usize::from(!q.same(&p));
                    Some(q)
                }
                _ => {
                    dropped += 1;
                    None
                }
            };
        }
        // Tone ramps of a slot without a generator, beyond the engine's
        // slots or holding a value no generator holds are left out: the
        // generator then holds its target, as one the log never ramped.
        dropped += state.tone_ramps.len().saturating_sub(slots);
        state.tone_ramps.truncate(slots);
        for (slot, ramps) in state.tone_ramps.iter_mut().enumerate() {
            let generator = || {
                let entry = tone_entry(slot, samplez_dsp::ToneParams::DEFAULT);
                self.engine.admits(std::slice::from_ref(&entry)).is_ok()
            };
            if ramps.is_some_and(|r| !r.is_valid() || !generator()) {
                *ramps = None;
                dropped += 1;
            }
        }
        for gain in state.gains.iter_mut().flatten() {
            if !samplez_dsp::valid_gain(*gain) {
                *gain = 0.0;
                dropped += 1;
            }
        }
        // A duck setting or mask the engine refuses is replaced as the
        // live filter replaces it, or left at what no entry set where the
        // engine takes neither; a group beyond the engine's is left out.
        let duck = |group: usize, setting: DuckSetting| match self
            .admitted(&Entry::Duck { group, setting })
        {
            Some(Entry::Duck { setting, .. }) => Some(setting),
            _ => None,
        };
        dropped += state.ducks.len().saturating_sub(samplez_dsp::MAX_DUCKS);
        state.ducks.truncate(samplez_dsp::MAX_DUCKS);
        for (group, d) in state.ducks.iter_mut().enumerate() {
            let default = samplez::DuckTargets::default();
            match duck(group, DuckSetting::Detector(Box::new(d.params))) {
                Some(DuckSetting::Detector(p)) if *p == d.params => {}
                Some(DuckSetting::Detector(p)) => {
                    d.params = *p;
                    dropped += 1;
                }
                _ => {
                    d.params = default.params;
                    dropped += 1;
                }
            }
            match duck(group, DuckSetting::Keys(d.keys.clone())) {
                Some(DuckSetting::Keys(k)) if k == d.keys => {}
                Some(DuckSetting::Keys(k)) => {
                    d.keys = k;
                    dropped += 1;
                }
                _ => {
                    d.keys = default.keys;
                    dropped += 1;
                }
            }
            for (slot, db) in d.depths.iter_mut().enumerate() {
                match duck(group, DuckSetting::Depth { slot, db: *db }) {
                    Some(DuckSetting::Depth { db: depth, .. })
                        if depth.to_bits() == db.to_bits() => {}
                    Some(DuckSetting::Depth { db: depth, .. }) => {
                        *db = depth;
                        dropped += 1;
                    }
                    _ => {
                        *db = 0.0;
                        dropped += 1;
                    }
                }
            }
        }
        for (input, row) in state.masks.iter_mut().enumerate() {
            for (output, mask) in row.iter_mut().enumerate() {
                let entry = Entry::DuckMask {
                    input,
                    output,
                    mask: *mask,
                };
                if *mask != 0 && self.admitted(&entry) != Some(entry) {
                    *mask = 0;
                    dropped += 1;
                }
            }
        }
        let takes = |group: usize, slot: usize, db: f32| {
            matches!(duck(group, DuckSetting::Depth { slot, db }),
                Some(DuckSetting::Depth { db: d, .. }) if d.to_bits() == db.to_bits())
        };
        let mask = |input: usize, output: usize, mask: u8| {
            let entry = Entry::DuckMask {
                input,
                output,
                mask,
            };
            if mask == 0 || self.admitted(&entry) == Some(entry) {
                mask
            } else {
                0
            }
        };
        dropped += filter_in_flight(&mut state, &left, takes, mask);
        if dropped > 0 {
            tracing::error!(
                dropped,
                "the product state holds what the engine refuses: replaced or left out of the rebuild"
            );
        }
        state
    }

    /// The engine's refusal of an appended entry: the engine then lacks
    /// it, so its output no longer follows the log. Counted, and reported
    /// with every sample, which keeps the replica from taking writes.
    fn refused(&self, index: u64, e: &EntryError) {
        self.refused.fetch_add(1, Ordering::Relaxed);
        tracing::error!(index, error = %e, "the engine refused a log entry");
    }
}

impl Audio for EngineAudio {
    fn ramp_config(&self) -> Option<(u32, u64)> {
        Some((self.engine.ramp_samples(), self.block))
    }
    fn sample(&self) -> Sample {
        let Some(progress) = self.engine.progress() else {
            return Sample::default();
        };
        let mut unwritten = self.unwritten.lock().expect("unwritten lock poisoned");
        let unwritten = &mut *unwritten;
        while let Ok(change) = unwritten.changes.try_recv() {
            if let StatusChange::Unwritten { output, from, to } = change {
                if unwritten.kept.len() == RANGES_KEPT {
                    unwritten.kept.pop_front();
                }
                unwritten.kept.push_back(Unwritten { output, from, to });
            }
        }
        while let Ok(h) = unwritten.hashes.try_recv() {
            let o = h.output as usize;
            let page = page_of(&h, self.block, |k| self.engine.block_status(o, k));
            if let Some(kept) = unwritten.pages.get_mut(o) {
                if kept.len() == PAGES_REPORTED {
                    kept.pop_front();
                }
                kept.push_back(page);
            }
        }
        let status = self.engine.status();
        let mut own = Own {
            processed: progress.processed,
            clock: ClockFlags {
                self_report: progress.self_report,
                step_fence: progress.steps > progress.steps_cleared,
                // Clock verification sets it; until then every replica
                // counts as verified.
                unverified: false,
                tai_offset_s: progress.tai_offset_s,
            },
            statuses: Vec::with_capacity(self.outputs),
            standing: (0..self.outputs)
                .map(|o| {
                    self.engine
                        .standing(o)
                        .unwrap_or(Standing::Idle { since: 0 })
                })
                .collect(),
            unwritten: Vec::new(),
            build_lead: Some(progress.build_lead),
            build_time: Some(progress.build_time),
            pages: unwritten.pages.iter().flatten().copied().collect(),
            ..Own::default()
        };
        for (slot, input) in status.inputs.iter().enumerate().take(64) {
            if input.state == InputState::Detached {
                continue;
            }
            match input.readable {
                Some(true) => own.readable |= 1 << slot,
                Some(false) => own.unreadable |= 1 << slot,
                None => {}
            }
        }
        if let Some(newest) = progress.processed {
            let first = (newest + 1).saturating_sub(self.window);
            unwritten.kept.retain(|u| u.to > first * self.block);
            for o in 0..self.outputs {
                own.statuses.push(runs(
                    (first..=newest).map(|k| Some(word_or_skip(self.engine.block_status(o, k), k))),
                ));
            }
            own.unwritten = unwritten.kept.iter().copied().collect();
        }
        Sample {
            own,
            status_changes_lost: progress.status_changes_lost,
            entries_refused: self.refused.load(Ordering::Relaxed),
            entries_filtered: self.filtered.load(Ordering::Relaxed),
            realised: status.outputs.iter().map(|o| o.realised).collect(),
            enabled: status.outputs.iter().map(|o| o.enabled).collect(),
            generation_ready: status.generation.pending.is_none() || status.generation.ready,
            clock_steps: progress.steps,
            log_overflows: progress.log_overflows,
        }
    }

    fn fence(&self, end: u64) {
        // Refused only by an engine that is not replicated.
        let _ = self.engine.set_fence(end);
    }

    fn settled(&self, k: u64) -> bool {
        let processed = self.engine.progress().and_then(|p| p.processed);
        (0..self.outputs).all(|o| match self.engine.block_status(o, k) {
            Some(s) => matches!(s.kind, Kind::Committed | Kind::Withheld),
            // Never computed below the processed block: the engine computes
            // no index twice.
            None => processed.is_some_and(|p| k <= p),
        })
    }

    fn appended(&self, index: u64, at_index: u64, entries: &[Entry]) {
        // A lease expiry from an older leader derives at a block start
        // for every block size dividing `FALLBACK_LEAD`; for another one
        // the next block start keeps the log's order.
        let at_index = at_index.next_multiple_of(self.block);
        // Filtered only when refused: the fold runs once for a record the
        // engine takes.
        let refused = match self.engine.append_batch(index, at_index, entries) {
            Err(EntryError::Invalid(_)) => {
                let kept = self.filter(index, entries);
                self.engine.append_batch(index, at_index, &kept).err()
            }
            appended => appended.err(),
        };
        if let Some(e) = refused {
            self.refused(index, &e);
        }
    }

    fn committed(&self, up_to: u64) {
        self.engine.commit(up_to);
    }

    fn overwhelmed(&self) {
        tracing::error!("an unresolved entry found no room in the engine: withdrawn");
        // Refused only by an engine that is not replicated.
        let _ = self.engine.withdraw();
    }

    fn unresolved(&self, index: u64, at_index: u64) -> bool {
        self.engine.unresolved(index, at_index).is_ok()
    }

    fn status(&self, output: usize, k: u64) -> Option<BlockStatus> {
        self.engine.block_status(output, k)
    }

    fn withhold(&self, output: usize, k: u64, cause: Cause) -> Option<BlockStatus> {
        self.engine.withhold(output, k, cause).ok().flatten()
    }

    fn commit_through(&self, output: usize, k: u64, on_silence_of: u8, up_to: Option<u64>) -> bool {
        self.engine
            .commit_through(output, k, on_silence_of, up_to)
            .unwrap_or(false)
    }

    fn withdraw(&self) {
        if let Err(e) = self.engine.withdraw() {
            tracing::error!(error = ?e, "the engine refused a withdrawal");
        }
    }

    fn ranks(&self, ranks: Ranks) {
        let _ = self.engine.set_ranks(ranks);
    }

    fn commit_from(&self, output: usize, min_k: u64, seen: u64) {
        if let Err(e) = self.engine.commit_from_seen(output, min_k, seen) {
            tracing::warn!(output, error = %e, "the engine refused a commit start");
        }
    }

    fn cancel_commit_start(&self, output: usize) {
        if let Err(e) = self.engine.cancel_commit_start(output) {
            tracing::warn!(output, error = %e, "the engine refused a commit start's cancel");
        }
    }

    fn stop_at(&self, output: usize, at: u64) {
        if let Err(e) = self.engine.stop_at(output, at) {
            tracing::warn!(output, at, error = %e, "the engine refused a stop");
        }
    }

    fn resolved(&self, index: u64) {
        if let Err(e) = self.engine.resolved(index) {
            tracing::warn!(index, error = %e, "the engine refused a resolution");
        }
    }

    fn drop_engine(&self) {
        if let Err(e) = self.engine.drop_core() {
            tracing::error!(error = %e, "the engine refused to drop its core");
        }
    }

    fn diverged(&self, apart: bool) {
        if let Err(e) = self.engine.set_diverged(apart) {
            tracing::error!(error = ?e, "the engine refused the divergence flag");
        }
    }

    /// Waits for the floor, which lies up to the lead and a generation's
    /// build ahead of the engine under continuous writes, the lead at the
    /// cap up to [`crate::schedule::EXTRA_LEAD`] past it, and the RT
    /// thread's take; a failure is logged once until one succeeds.
    fn rebuild_engine(&self, state: &ProductState, at_least: u64) -> bool {
        let build_lead = self.engine.progress().map_or(0, |p| p.build_lead);
        let ahead = Duration::from_nanos(crate::contact::time_of(build_lead));
        let timeout = REBUILD_WAIT + self.max_apply_lead + crate::schedule::EXTRA_LEAD + ahead;
        let state = self.sanitised(state);
        match self.engine.install_product(&state, at_least, timeout) {
            Ok(_) => {
                self.rebuild_failed.store(false, Ordering::Relaxed);
                true
            }
            Err(e) => {
                if !self.rebuild_failed.swap(true, Ordering::Relaxed) {
                    tracing::warn!(error = ?e, "the engine core was not rebuilt: retried");
                }
                false
            }
        }
    }

    fn follows(&self, state: &ProductState) -> bool {
        let reading = self.reading();
        self.sanitised_with(state, &reading).flows == reading
    }

    fn copy_core(&self) -> Option<CoreReply> {
        for _ in 0..COPY_TRIES {
            match self.engine.copy_core(COPY_WAIT) {
                Ok(copy) => {
                    return Some(CoreReply {
                        k0: copy.k0,
                        last_id: copy.last_id,
                        core: copy.encode(),
                    });
                }
                // The history slack passed while it was copied: again.
                Err(CopyError::Passed { .. }) => continue,
                Err(e) => {
                    tracing::warn!(error = %e, "no engine core copied for a resync");
                    return None;
                }
            }
        }
        None
    }

    /// A core that does not decode, or that the engine refuses as
    /// `ReplayError::Invalid` (another engine shape, an entry outside the
    /// engine), is refused for good ([`Failed::Refused`]).
    fn resync(&self, core: &CoreReply, source: &dyn Source) -> Result<Resynced, Failed> {
        let copy = CoreCopy::decode(&core.core).map_err(|e| Failed::Refused(e.to_string()))?;
        let mut unresolved = copy.core.unresolved().to_vec();
        let mut replay = self.engine.replay(copy).map_err(replay_failed)?;
        let deadline = Instant::now() + REPLAY_WAIT;
        let mut after = core.last_id;
        let j = loop {
            // A gap is never replayed: the attempt fails, and the next
            // takes a newer core.
            let records = source.entries(after)?;
            // The lowest index of the records from each one on.
            let mut lowest = vec![u64::MAX; records.len() + 1];
            for (n, (_, r)) in records.iter().enumerate().rev() {
                lowest[n] = lowest[n + 1].min(r.at_index);
            }
            // An entry at or after the replay's next block is applied,
            // though the donor may have taken it late and held it
            // unresolved: the records carry no decision, and one in the
            // log names the entry only once the leader saw it held
            // unresolved, which an entry the baseline applied in time
            // never draws. The pages after it then differ, the hand-over
            // waits for a matched page and the attempt fails; the next
            // copies a newer core, whose next block is past the entry, and
            // holds it unresolved as below. Decisions are stamped with the
            // lead the entries needed, so a decision is not late again.
            for (n, (index, record)) in records.into_iter().enumerate() {
                after = index;
                let at_index = record.at_index.next_multiple_of(self.block);
                let list = self.filter(index, &record.entries);
                let mut appended = replay.append_batch(index, at_index, &list);
                if appended == Err(ReplayError::Full) {
                    // Its pending list holds the entries before this one:
                    // the replay computes up to the first index still to
                    // come, which applies those due by then.
                    replay.run_to((lowest[n] / self.block).saturating_sub(1));
                    appended = replay.append_batch(index, at_index, &list);
                }
                appended.map_err(replay_failed)?;
            }
            if !source.go() {
                return Err("given up before the hand-over".into());
            }
            replay.run_to(u64::MAX);
            match self.engine.hand_over(&mut replay, HANDOVER_TRY) {
                Ok(j) => break j,
                // An entry the replay lacks, or a block passed: the log
                // and the replay are read again.
                Err(ReplayError::Missed { .. } | ReplayError::Timeout { .. })
                    if Instant::now() < deadline => {}
                Err(e) => return Err(replay_failed(e)),
            }
        };
        // Taken late by the donor after its core and held unresolved, as
        // the donor holds them.
        for &(id, at) in replay.late() {
            if !unresolved.iter().any(|u| u.0 == id) {
                unresolved.push((id, at));
            }
        }
        // A page holding a block computed with an input silenced depends
        // on more than the input and the log: not compared.
        let silenced = replay.silenced();
        let block = self.block;
        let touched = |p: &PageHash| {
            silenced
                .iter()
                .any(|&k| k * block < (p.page + 1) * PAGE && (k + 1) * block > p.page * PAGE)
        };
        let pages = replay
            .pages()
            .iter()
            .filter(|p| !touched(p))
            .map(|p| page_of(p, block, |_| None))
            .collect();
        Ok(Resynced {
            j,
            pages,
            unresolved,
        })
    }
}

/// A replay's refusal as a resync failure: `Invalid` is for good, as no
/// core of the same donor fits better; any other may pass.
fn replay_failed(e: ReplayError) -> Failed {
    match e {
        ReplayError::Invalid(why) => Failed::Refused(why),
        e => Failed::Other(e.to_string()),
    }
}

/// Filters the ramps in flight of `state` as the live filter filters
/// entries, for an engine of `state`'s input slots: a crossfade of a slot
/// of `left`, which stays at the flow it reads, is none; fade-ins and
/// crossfades of slots beyond the engine's are left out; a depth ramp
/// from or to a depth `takes` refuses, by group, slot and dB, is left
/// out; each mask of a mask change, the one it starts from included, is
/// the one `mask` lets through, by input channel and column. How many
/// were left out.
fn filter_in_flight(
    state: &mut ProductState,
    left: &[usize],
    takes: impl Fn(usize, usize, f32) -> bool,
    mask: impl Fn(usize, usize, u8) -> u8,
) -> usize {
    let slots = state.flows.len();
    let flight = &mut state.in_flight;
    for &slot in left {
        if let Some(x) = flight.crossfades.get_mut(slot) {
            *x = None;
        }
    }
    let mut dropped = flight.fade_ins.len().saturating_sub(slots);
    flight.fade_ins.truncate(slots);
    dropped += flight.crossfades.len().saturating_sub(slots);
    flight.crossfades.truncate(slots);
    for (group, ramps) in flight.depths.iter_mut().enumerate() {
        for (slot, ramp) in ramps.iter_mut().enumerate() {
            let target = (state.ducks.get(group))
                .and_then(|d| d.depths.get(slot).copied())
                .unwrap_or(0.0);
            if ramp.is_some_and(|f| !takes(group, slot, f.from) || !takes(group, slot, target)) {
                *ramp = None;
                dropped += 1;
            }
        }
    }
    for (input, output, from, changes) in &mut flight.masks {
        *from = mask(*input, *output, *from);
        for (_, k) in changes.iter_mut() {
            *k = mask(*input, *output, *k);
        }
    }
    dropped
}

#[cfg(test)]
mod tests {
    use samplez::blocks::HEALTHY;

    use super::*;

    #[test]
    fn a_block_without_a_word_below_the_processed_one_is_reported_skipped() {
        let committed = BlockStatus {
            k: 5,
            kind: Kind::Committed,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::None,
            on_silence_of: 0,
            digest: Some(HEALTHY),
        };
        assert_eq!(word_or_skip(Some(committed), 5), committed);
        let skipped = word_or_skip(None, 6);
        assert_eq!(
            (skipped.k, skipped.kind, skipped.cause),
            (6, Kind::Withheld, Cause::Skip)
        );
    }

    /// The crossfade of a slot left at the flow it reads is dropped, the
    /// others kept, and a mask change's starting mask is filtered as its
    /// changes are: a refused mask, here 2, is 0 at both ends.
    #[test]
    fn a_rebuild_filters_the_crossfades_and_masks_in_flight() {
        let mut state = ProductState {
            gains: vec![vec![1.0, 1.0]],
            fading: Vec::new(),
            up: vec![true, true],
            flows: vec![Some("a".to_owned()), Some("b".to_owned())],
            outputs: Vec::new(),
            loudness_running: Vec::new(),
            strips: vec![Vec::new(), Vec::new()],
            limiters: Vec::new(),
            delays: Vec::new(),
            tones: Vec::new(),
            tone_ramps: Vec::new(),
            ducks: Vec::new(),
            masks: vec![vec![1, 0]],
            in_flight: samplez::InFlight {
                crossfades: vec![Some((960, Some("c".to_owned()))), Some((960, None))],
                masks: vec![(0, 0, 2, vec![(960, 1)]), (0, 1, 1, vec![(960, 2)])],
                ..samplez::InFlight::default()
            },
            pending: Vec::new(),
            last_id: 0,
            ..Default::default()
        };
        let refuse_2 = |_: usize, _: usize, k: u8| if k == 2 { 0 } else { k };
        let dropped = filter_in_flight(&mut state, &[1], |_, _, _| true, refuse_2);
        assert_eq!(dropped, 0);
        let f = &state.in_flight;
        assert_eq!(f.crossfades, vec![Some((960, Some("c".to_owned()))), None]);
        assert_eq!(
            f.masks,
            vec![(0, 0, 0, vec![(960, 1)]), (0, 1, 1, vec![(960, 0)])]
        );
    }
}
