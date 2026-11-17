//! [`Audio`] over a replicated samplez [`Engine`]: F and unresolved
//! entries go to the engine's control ring, the log's entries to its
//! entry ring by log id, and reports read the status words, the
//! processed index, the clock flags and the latest page hashes.
//!
//! Reports carry the status words of the latest blocks rather than the
//! changes since the last report, so a change the engine's ring or
//! channel dropped (counted) costs nothing: the word says it, and a block
//! without a word below the processed one was skipped. Only an unwritten
//! range, which no word records, is lost with its change.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use samplez::blocks::{
    BlockStatus, Cause, HEALTHY, Kind, PAGE, PageHash, Range, Ranks, Standing, StatusChange,
};
use samplez::{Engine, Entry, EntryError};

use crate::contact::{Audio, Sample, Timing};
use crate::entry::{Body, Request};
use crate::report::{ClockFlags, Own, Page, Unwritten, page_digest, runs};

/// Turns a log entry into the engine entry it applies, with its index;
/// none for an entry the engine does not apply.
pub type Translate = Box<dyn Fn(&Request) -> Option<(u64, Entry)> + Send + Sync>;

/// The product state, as applied, that a dropped engine core is rebuilt
/// from; none while it cannot be read.
pub type Rebuild = Box<dyn Fn() -> Option<samplez::ProductState> + Send + Sync>;

/// How long a rebuild waits for the RT thread to take the core.
const REBUILD_WAIT: std::time::Duration = std::time::Duration::from_millis(100);

/// The engine entry of an `InputDown` or `InputUp` body; a product's
/// [`Translate`] adds the entries only it can read, such as a route.
pub fn input_entry(request: &Request) -> Option<(u64, Entry)> {
    match request.body {
        Body::InputDown { slot, at_index } => Some((
            at_index,
            Entry::InputDown {
                slot: slot as usize,
            },
        )),
        Body::InputUp { slot, at_index } => Some((
            at_index,
            Entry::InputUp {
                slot: slot as usize,
            },
        )),
        _ => None,
    }
}

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

pub struct EngineAudio {
    engine: Arc<Engine>,
    translate: Translate,
    outputs: usize,
    block: u64,
    /// Blocks up to the processed one whose statuses reports carry.
    window: u64,
    unwritten: Mutex<Unwrittens>,
    /// Appended entries the engine refused.
    refused: AtomicU64,
    rebuild: Option<Rebuild>,
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
        translate: Translate,
    ) -> EngineAudio {
        let outputs = engine.status().outputs.len();
        let span = timing.hold + timing.contact_window + 2 * timing.report_period;
        let samples = (span.as_nanos() as u64 * crate::contact::RATE).div_ceil(1_000_000_000);
        EngineAudio {
            outputs,
            engine,
            translate,
            block: timing.block,
            window: samples.div_ceil(timing.block) + 1,
            refused: AtomicU64::new(0),
            rebuild: None,
            unwritten: Mutex::new(Unwrittens {
                changes,
                kept: VecDeque::new(),
                hashes,
                pages: (0..outputs).map(|_| VecDeque::new()).collect(),
            }),
        }
    }

    /// Where a core dropped on a snapshot install or a purge-point
    /// verdict is rebuilt from; without it the replica stays in
    /// `starting`.
    pub fn with_rebuild(mut self, rebuild: Rebuild) -> EngineAudio {
        self.rebuild = Some(rebuild);
        self
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
            pages: unwritten.pages.iter().flatten().copied().collect(),
            ..Own::default()
        };
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
            realised: status.outputs.iter().map(|o| o.realised).collect(),
            generation_ready: status.generation.pending.is_none() || status.generation.ready,
            clock_steps: progress.steps,
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

    fn appended(&self, index: u64, request: &Request) {
        if let Some((at_index, entry)) = (self.translate)(request)
            && let Err(e) = self.engine.append(index, at_index, entry)
        {
            self.refused(index, &e);
        }
    }

    fn truncated(&self, from: u64) {
        self.engine.truncate(from);
    }

    fn committed(&self, up_to: u64) {
        self.engine.commit(up_to);
    }

    fn skip(&self, index: u64) {
        self.engine.skip(index);
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

    fn rebuild_engine(&self) -> bool {
        let Some(state) = self.rebuild.as_ref().and_then(|r| r()) else {
            return false;
        };
        match self.engine.install_product(&state, REBUILD_WAIT) {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(error = ?e, "the engine core was not rebuilt");
                false
            }
        }
    }
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
}
