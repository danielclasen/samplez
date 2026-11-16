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

use std::sync::atomic::Ordering::Relaxed;

use samplez_dsp::Matrix;
use samplez_mxl::time;

use crate::blocks::PageHash;
use crate::engine::Entry;
use crate::rt_loop::{Change, Control, LogId, PENDING_CHANGES, Rt, RtEntry, UNRESOLVED};

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
#[derive(Debug, PartialEq)]
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

/// A replayed core after block `j`, which knew the log entries up to
/// `last_id`, for the RT thread to install before block `j + 1`.
pub(crate) struct Handover {
    /// Numbers the hand-overs, so that the answer to one a caller gave up
    /// on is told apart from the answer to the next.
    pub seq: u64,
    pub j: u64,
    pub last_id: LogId,
    pub core: Box<Core>,
    pub answer: Answer,
}

/// What the RT thread did with a hand-over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    /// Not answered yet.
    Waiting,
    Installed,
    /// The RT thread had started a block after `j + 1`.
    Passed,
    /// The RT thread held a pending generation the core did not name.
    Generation,
    /// The RT thread had taken entry `id`, above the core's `last_id`,
    /// off its pending list: applied, or reported late. Installed, the
    /// core would lose it.
    Missed(LogId),
}

/// The core after block `k0`, and the highest log id the RT thread had
/// taken: entries after it reach a replica from its own log.
pub struct CoreCopy {
    pub k0: u64,
    pub last_id: LogId,
    pub core: Box<Core>,
}

/// A replay clone ([`Engine::replay`](crate::Engine::replay)): the
/// engine's computation from a donor's core, off the RT thread, on past
/// input. It takes the log entries and unresolved-entry decisions after
/// the core's last log id from the caller, as the engine does, and hashes
/// every page it computes whole.
pub struct Replay {
    pub(crate) rt: Box<Rt>,
    pub(crate) hashes: rtrb::Consumer<PageHash>,
    pub(crate) control: rtrb::Producer<Control>,
    pub(crate) pages: Vec<PageHash>,
    /// The blocks computed with an input silenced, not yet taken.
    pub(crate) silenced: Vec<u64>,
    /// The next block it computes.
    pub(crate) next: u64,
    /// The highest log id it knows.
    pub(crate) last_id: LogId,
}

impl Replay {
    /// The next block the replay computes.
    pub fn next_block(&self) -> u64 {
        self.next
    }

    /// Takes the committed log entry `id` at `at_index`; one the core
    /// knew already changes nothing. Only entries that need no generation
    /// replay; one whose index the replay computed is late.
    pub fn append(&mut self, id: LogId, at_index: u64, entry: &Entry) -> Result<(), ReplayError> {
        if id <= self.last_id {
            return Ok(());
        }
        let change = change_of(entry);
        if change.needs_generation() {
            return Err(ReplayError::Generation);
        }
        let b = self.rt.block;
        if !at_index.is_multiple_of(b) || at_index < self.next * b {
            return Err(ReplayError::Late { id, at_index });
        }
        let e = RtEntry {
            id,
            at_index,
            change,
        };
        if !self.rt.mix.push(e) {
            return Err(ReplayError::Full);
        }
        self.last_id = id;
        Ok(())
    }

    /// Entry `id` at `at_index` is unresolved, from the next block on.
    pub fn unresolved(&mut self, id: LogId, at_index: u64) -> Result<(), ReplayError> {
        let c = Control::Unresolved { id, at_index };
        self.control.push(c).map_err(|_| ReplayError::Full)
    }

    /// Entry `id` is decided, from the next block on.
    pub fn resolved(&mut self, id: LogId) -> Result<(), ReplayError> {
        let c = Control::Resolved { id };
        self.control.push(c).map_err(|_| ReplayError::Full)
    }

    /// Computes the blocks up to `k`, or up to the newest block due if
    /// that is lower: the input of a later block may not be written yet.
    pub fn run_to(&mut self, k: u64) {
        let rt = &self.rt;
        let now = time::now(&rt.api);
        if let Some(due) = time::newest_due_block(&rt.api, now, rt.block, rt.margin) {
            self.run(k.min(due));
        }
    }

    /// Computes the blocks up to `k`, whose input must be written.
    pub(crate) fn run(&mut self, k: u64) {
        let silenced = |rt: &Rt| rt.counters.input_silenced_blocks.load(Relaxed);
        while self.next <= k {
            let before = silenced(&self.rt);
            self.rt.control();
            self.rt.process(self.next, u64::MAX);
            if silenced(&self.rt) != before {
                self.silenced.push(self.next);
            }
            self.pages
                .extend(std::iter::from_fn(|| self.hashes.pop().ok()));
            self.next += 1;
        }
    }

    /// The page hashes computed since the last call, `committed` false.
    pub fn pages(&mut self) -> Vec<PageHash> {
        std::mem::take(&mut self.pages)
    }

    /// The blocks computed since the last call with an input silenced:
    /// the input was not readable, a late write or one the history no
    /// longer held, or the slot was down. Their pages and those of the
    /// fade-in after them depend on more than the input and the log.
    pub fn silenced(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.silenced)
    }
}

/// The change log entry `entry` makes on the RT thread.
pub(crate) fn change_of(entry: &Entry) -> Change {
    match *entry {
        Entry::Gain {
            input,
            output,
            gain,
        } => Change::Gain {
            input,
            output,
            gain,
        },
        Entry::InputDown { slot } => Change::InputDown { slot },
        Entry::InputUp { slot } => Change::InputUp { slot },
        Entry::Route { slot, .. } => Change::Route { slot },
        Entry::Strip { slot, param } => Change::Strip { slot, param },
    }
}

/// The product state an engine core is built from
/// ([`Engine::install_product`](crate::Engine::install_product)).
#[derive(Clone, Debug, PartialEq)]
pub struct ProductState {
    /// The gain targets: one row per input channel, one column per output
    /// channel.
    pub gains: Vec<Vec<f32>>,
    /// Per input slot whether it is up in the log.
    pub up: Vec<bool>,
    /// Committed entries not applied yet: log id, `at_index` and entry.
    pub pending: Vec<(LogId, u64, Entry)>,
    /// The highest log id the state folds in.
    pub last_id: LogId,
}

/// Why a replay was not built, took no entry, or was not handed over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayError {
    /// The engine is not replicated with more than one voter.
    Invalid(String),
    /// A generation-needing entry is in flight in the core, or was
    /// appended: the replay builds no generation.
    Generation,
    /// An input's reader could not be attached.
    Attach(String),
    /// The entry's index is not a block start or was computed already.
    Late { id: LogId, at_index: u64 },
    /// The pending list or the control ring is full.
    Full,
    /// No hand-over was installed within the timeout.
    Timeout,
    /// The RT thread had taken entry `id`, which the core did not know,
    /// off its pending list: the core must know it to be installed.
    Missed { id: LogId },
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::Invalid(m) | ReplayError::Attach(m) => write!(f, "{m}"),
            ReplayError::Generation => write!(f, "a generation-needing entry is in flight"),
            ReplayError::Late { id, at_index } => {
                write!(f, "entry {id} at {at_index} is late for the replay")
            }
            ReplayError::Full => write!(f, "the replay's pending list or control ring is full"),
            ReplayError::Timeout => write!(f, "no hand-over installed within the timeout"),
            ReplayError::Missed { id } => {
                write!(
                    f,
                    "the RT thread took entry {id}, which the core does not know"
                )
            }
        }
    }
}

impl std::error::Error for ReplayError {}

/// Why [`Engine::copy_core`](crate::Engine::copy_core) returned no copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CopyError {
    /// The engine is not replicated with more than one voter.
    NotReplicated,
    /// The RT thread computed no block within the timeout.
    Timeout,
    /// The RT thread had started block `started`, the guard block of the
    /// history after `k0` or later, when the copy was taken: copy again.
    Passed { k0: u64, started: u64 },
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyError::NotReplicated => {
                write!(f, "the engine is not replicated with more than one voter")
            }
            CopyError::Timeout => write!(f, "no block computed within the timeout"),
            CopyError::Passed { k0, started } => write!(
                f,
                "the RT thread started block {started}, past the history slack after block {k0}"
            ),
        }
    }
}

impl std::error::Error for CopyError {}
