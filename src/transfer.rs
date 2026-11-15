//! Engine state transfer: the core the RT thread copies after a block, so
//! that another replica can replay from it.
//!
//! The core is the engine state output depends on apart from delay lines:
//! the matrix with its ramps, the entries pending and applied in the page
//! in progress, each input slot's log state and fades, the entries waiting
//! for their generation and the pending generation's entry id, and the
//! unresolved entries with the late re-routes. Meters, hold rings, block
//! statuses, page hashes in progress and the readers stay with the
//! replica. The RT thread copies it into a buffer allocated off the RT
//! thread, of the same shape, so the copy allocates nothing.

use samplez_dsp::Matrix;

use crate::rt_loop::{LogId, PENDING_CHANGES, RtEntry, UNRESOLVED};

/// An input slot's part of the core.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct SlotCore {
    pub up: bool,
    pub held: bool,
    pub fade_start: Option<u64>,
    pub silenced_last: bool,
    pub xfade: Option<u64>,
}

/// The core of an engine after a block.
pub struct Core {
    pub(crate) matrix: Matrix,
    pub(crate) pos: Option<u64>,
    pub(crate) ramp_end: u64,
    pub(crate) pending: Vec<RtEntry>,
    pub(crate) page_entries: Vec<(LogId, u64)>,
    pub(crate) entries_page: Option<u64>,
    pub(crate) entries_lost: bool,
    pub(crate) slots: Vec<SlotCore>,
    pub(crate) awaiting_gen: Vec<RtEntry>,
    pub(crate) pending_gen: Option<LogId>,
    pub(crate) unresolved: Vec<(LogId, u64)>,
    pub(crate) late_routes: Vec<(LogId, u64)>,
    pub(crate) late_lost_until: u64,
}

impl Core {
    /// A buffer for the core of an engine with `inputs` and `outputs`
    /// channels, `slots` input slots and room for `page_entries` entries
    /// of one page's fingerprint.
    pub(crate) fn new(inputs: usize, outputs: usize, slots: usize, page_entries: usize) -> Core {
        Core {
            matrix: Matrix::new(inputs, outputs),
            pos: None,
            ramp_end: 0,
            pending: Vec::with_capacity(PENDING_CHANGES),
            page_entries: Vec::with_capacity(page_entries),
            entries_page: None,
            entries_lost: false,
            slots: vec![SlotCore::default(); slots],
            awaiting_gen: Vec::with_capacity(PENDING_CHANGES),
            pending_gen: None,
            unresolved: Vec::with_capacity(UNRESOLVED),
            late_routes: Vec::with_capacity(PENDING_CHANGES),
            late_lost_until: 0,
        }
    }

    /// The index of the next sample the core computes.
    pub fn next_index(&self) -> Option<u64> {
        self.pos
    }

    /// The coefficient from input channel `input` to output channel
    /// `output` on the next sample.
    pub fn gain(&self, input: usize, output: usize) -> f32 {
        self.matrix.gain(input, output)
    }

    /// The `(log id, at_index)` of the entries pending before their index.
    pub fn pending(&self) -> impl Iterator<Item = (LogId, u64)> + '_ {
        self.pending.iter().map(|e| (e.id, e.at_index))
    }

    /// Whether input slot `slot` is up in the log.
    pub fn input_up(&self, slot: usize) -> bool {
        self.slots[slot].up
    }

    /// The `(log id, at_index)` of the unresolved entries.
    pub fn unresolved(&self) -> &[(LogId, u64)] {
        &self.unresolved
    }
}

/// The core after block `k0`, and the highest log id the RT thread had
/// taken: entries after it reach a replica from its own log.
pub struct CoreCopy {
    pub k0: u64,
    pub last_id: LogId,
    pub core: Box<Core>,
}

/// Why [`Engine::copy_core`](crate::Engine::copy_core) returned no copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyError {
    /// The engine is not replicated with more than one voter.
    Invalid(String),
    /// The RT thread computed no block within the timeout.
    Timeout,
    /// The RT thread had started block `started`, the guard block of the
    /// history after `k0` or later, when the copy was taken: copy again.
    Passed { k0: u64, started: u64 },
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyError::Invalid(m) => write!(f, "{m}"),
            CopyError::Timeout => write!(f, "no block computed within the timeout"),
            CopyError::Passed { k0, started } => write!(
                f,
                "the RT thread started block {started}, past the history slack after block {k0}"
            ),
        }
    }
}

impl std::error::Error for CopyError {}
