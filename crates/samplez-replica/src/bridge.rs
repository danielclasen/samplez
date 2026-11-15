//! [`Audio`] over a replicated samplez [`Engine`]: F and unresolved
//! entries go to the engine's control ring, the log's entries to its
//! entry ring by log id, and reports read the status words, the
//! processed index and the clock flags.
//!
//! Reports carry the status words of the latest blocks rather than the
//! changes since the last report, so a change the engine's ring or
//! channel dropped (counted) costs nothing: the word says it, and a block
//! without a word below the processed one was skipped. Only an unwritten
//! range, which no word records, is lost with its change.

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use samplez::blocks::{BlockStatus, Cause, Kind, Range, StatusChange};
use samplez::{Engine, Entry, EntryError};

use crate::contact::{Audio, Sample, Timing};
use crate::entry::{Body, Request};
use crate::report::{ClockFlags, Own, Unwritten, runs};

/// Turns a log entry into the engine entry it applies, with its index;
/// none for an entry the engine does not apply.
pub type Translate = Box<dyn Fn(&Request) -> Option<(u64, Entry)> + Send + Sync>;

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

struct Unwrittens {
    changes: Receiver<StatusChange>,
    kept: VecDeque<Unwritten>,
}

/// Block `k`'s status word, read at or below the processed block. A block
/// there without a word is never written: it was skipped (the RT thread
/// gives a computed block its first status before it publishes it
/// processed, and computes no index twice), or its output had no writer or
/// no commit start yet. So it is withheld, though the change saying so may
/// have been lost.
fn word_or_skip(word: Option<BlockStatus>, k: u64) -> BlockStatus {
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
}

impl EngineAudio {
    /// Bridges `engine`, started replicated with `changes` as its status
    /// channel. Reports carry the statuses of the blocks a peer can still
    /// hold a decision on: those of the last `D + w + 2p`, and one more.
    pub fn new(
        engine: Arc<Engine>,
        changes: Receiver<StatusChange>,
        timing: &Timing,
        translate: Translate,
    ) -> EngineAudio {
        let span = timing.hold + timing.contact_window + 2 * timing.report_period;
        let samples = (span.as_nanos() as u64 * crate::contact::RATE).div_ceil(1_000_000_000);
        EngineAudio {
            outputs: engine.status().outputs.len(),
            engine,
            translate,
            block: timing.block,
            window: samples.div_ceil(timing.block) + 1,
            unwritten: Mutex::new(Unwrittens {
                changes,
                kept: VecDeque::new(),
            }),
        }
    }

    /// The engine's refusal of an appended entry, logged: the engine then
    /// lacks it, and its commit will find nothing to apply.
    fn refused(&self, index: u64, e: &EntryError) {
        tracing::warn!(index, error = %e, "the engine refused a log entry");
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
            unwritten: Vec::new(),
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

    fn unresolved(&self, index: u64, at_index: u64) -> bool {
        self.engine.unresolved(index, at_index).is_ok()
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
