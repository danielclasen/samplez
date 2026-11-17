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
use serde::{Deserialize, Serialize};

use crate::blocks::PageHash;
use crate::engine::Entry;
use crate::rt_loop::{Change, Control, LogId, PENDING_CHANGES, Rt, RtEntry, UNRESOLVED};

/// The engine version a core's encoding carries: a core is replayed only
/// by the engine that copied it.
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The highest sample index a decoded core may hold: far beyond any
/// running engine's, and low enough that adding a ramp or a block to it
/// cannot overflow.
pub(crate) const INDEX_MAX: u64 = 1 << 62;

/// An input slot's part of the core.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SlotCore {
    pub up: bool,
    pub held: bool,
    pub fade_start: Option<u64>,
    pub silenced_last: bool,
    pub xfade: Option<u64>,
}

/// The core of an engine after a block.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

    /// Whether everything this core holds is a value an engine holds:
    /// every index at most [`INDEX_MAX`], every gain one a ramp takes,
    /// every strip parameter within its range (legs aside), the lists
    /// within their capacities. A core from another replica is checked
    /// when decoded.
    fn sane(&self) -> bool {
        let index = |i: u64| i <= INDEX_MAX;
        let entry = |e: &RtEntry| {
            index(e.at_index)
                && match e.change {
                    Change::Gain { gain, .. } => samplez_dsp::valid_gain(gain),
                    Change::Strip { param, .. } => param.check(usize::MAX).is_ok(),
                    _ => true,
                }
        };
        let slot = |s: &SlotCore| s.fade_start.is_none_or(index) && s.xfade.is_none_or(index);
        let ats = |list: &[(LogId, u64)]| list.iter().all(|&(_, at)| index(at));
        self.pos.is_none_or(index)
            && index(self.ramp_end)
            && self.entries_page.is_none_or(index)
            && index(self.late_lost_until)
            && self.slots.iter().all(slot)
            && ats(&self.page_entries)
            && ats(&self.unresolved)
            && ats(&self.late_routes)
            && self.pending.len() <= PENDING_CHANGES
            && self.awaiting_gen.len() <= PENDING_CHANGES
            && self.unresolved.len() <= UNRESOLVED
            && self.late_routes.len() <= PENDING_CHANGES
            && self.pending.iter().chain(&self.awaiting_gen).all(entry)
    }

    /// Whether this sane core is of an engine with `inputs` and `outputs`
    /// channels, input slots of `legs` legs each and room for
    /// `page_entries` entries of one page's fingerprint, every entry
    /// within them: checked before a replay installs it.
    pub(crate) fn fits(
        &self,
        inputs: usize,
        outputs: usize,
        legs: &[usize],
        page_entries: usize,
    ) -> bool {
        let slots = legs.len();
        let entry = |e: &RtEntry| match e.change {
            Change::Gain { input, output, .. } => input < inputs && output < outputs,
            Change::InputDown { slot } | Change::InputUp { slot } | Change::Route { slot } => {
                slot < slots
            }
            Change::Strip { slot, param } => slot < slots && param.check(legs[slot]).is_ok(),
        };
        let shape = (
            self.matrix.inputs(),
            self.matrix.outputs(),
            self.slots.len(),
        );
        self.sane()
            && shape == (inputs, outputs, slots)
            && self.page_entries.len() <= page_entries
            && self.pending.iter().chain(&self.awaiting_gen).all(entry)
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
    /// The core names the pending generation it was computed with, which
    /// the RT thread must hold. A core built from product state was
    /// computed with none and keeps the RT thread's.
    pub names_gen: bool,
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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CoreCopy {
    pub k0: u64,
    pub last_id: LogId,
    pub core: Box<Core>,
}

#[derive(Serialize)]
struct WireOut<'a> {
    engine: &'a str,
    copy: &'a CoreCopy,
}

#[derive(Deserialize)]
struct WireIn {
    engine: String,
    copy: serde_json::Value,
}

impl CoreCopy {
    /// The copy as another replica takes it, with [`ENGINE_VERSION`].
    pub fn encode(&self) -> String {
        let wire = WireOut {
            engine: ENGINE_VERSION,
            copy: self,
        };
        serde_json::to_string(&wire).expect("a core encodes")
    }

    /// A copy [`CoreCopy::encode`] made, by an engine of this version.
    pub fn decode(encoded: &str) -> Result<CoreCopy, DecodeError> {
        let wire: WireIn =
            serde_json::from_str(encoded).map_err(|e| DecodeError::Malformed(e.to_string()))?;
        if wire.engine != ENGINE_VERSION {
            return Err(DecodeError::Version(wire.engine));
        }
        let copy: CoreCopy =
            serde_json::from_value(wire.copy).map_err(|e| DecodeError::Malformed(e.to_string()))?;
        // A block number: its first index stays within INDEX_MAX at the
        // largest block size.
        if !copy.core.sane() || copy.k0 > INDEX_MAX / 1024 {
            return Err(DecodeError::Malformed(
                "a core holding a value no engine holds".to_owned(),
            ));
        }
        Ok(copy)
    }
}

/// Why [`CoreCopy::decode`] returned no copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// Copied by an engine of this other version.
    Version(String),
    Malformed(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Version(v) => {
                write!(f, "a core of engine {v}, this engine is {ENGINE_VERSION}")
            }
            DecodeError::Malformed(m) => write!(f, "a malformed core: {m}"),
        }
    }
}

impl std::error::Error for DecodeError {}

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
    /// No hand-over was installed within the timeout; `passed` of the
    /// call's were refused for a block the RT thread had passed.
    Timeout { passed: u64 },
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
            ReplayError::Timeout { passed } => write!(
                f,
                "no hand-over installed within the timeout, {passed} refused for a passed block"
            ),
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

#[cfg(test)]
mod tests {
    use samplez_dsp::StripParam;

    use super::*;

    fn copy() -> CoreCopy {
        let mut core = Core::new(2, 2, 2, 8);
        assert!(core.matrix.set(1, 0, 0.5, 48));
        assert!(core.matrix.set(0, 1, 0.25, 0));
        core.pos = Some(4800);
        core.ramp_end = 4848;
        core.pending.push(RtEntry {
            id: 7,
            at_index: 9600,
            change: Change::Strip {
                slot: 1,
                param: StripParam::Trim { db: -3.0 },
            },
        });
        core.page_entries.push((6, 4752));
        core.slots[1] = SlotCore {
            up: true,
            held: false,
            fade_start: Some(480),
            silenced_last: true,
            xfade: None,
        };
        core.unresolved.push((9, 1 << 40));
        CoreCopy {
            k0: 99,
            last_id: 7,
            core: Box::new(core),
        }
    }

    /// The copy another replica decodes is the copy encoded: the matrix
    /// with its ramps, the entries and the slot state.
    #[test]
    fn a_core_decodes_to_the_copy_encoded() {
        let copy = copy();
        let decoded = CoreCopy::decode(&copy.encode()).unwrap();
        assert_eq!(decoded, copy);
        assert_eq!(decoded.core.gain(1, 0), copy.core.gain(1, 0));
    }

    /// A core copied by another engine version is refused: its state may
    /// mean something else here.
    #[test]
    fn a_core_of_another_engine_version_is_refused() {
        let encoded = copy().encode().replacen(ENGINE_VERSION, "0.0.0-other", 1);
        assert_eq!(
            CoreCopy::decode(&encoded),
            Err(DecodeError::Version("0.0.0-other".to_owned()))
        );
    }

    /// A core whose matrix names an input out of range is refused before
    /// anything indexes by it.
    #[test]
    fn a_matrix_out_of_range_is_refused() {
        let encoded = copy().encode().replace("\"input\":1", "\"input\":5");
        assert!(matches!(
            CoreCopy::decode(&encoded),
            Err(DecodeError::Malformed(_))
        ));
    }

    /// Each class of value a peer can send that no engine holds is
    /// refused when decoded, before a replay or the RT thread indexes,
    /// adds or applies it.
    #[test]
    fn a_core_holding_a_value_no_engine_holds_is_refused() {
        let refused = |what: &str, f: &dyn Fn(&mut Core)| {
            let mut copy = copy();
            f(&mut copy.core);
            assert!(
                matches!(
                    CoreCopy::decode(&copy.encode()),
                    Err(DecodeError::Malformed(_))
                ),
                "{what}"
            );
        };
        refused("next index", &|c| c.pos = Some(u64::MAX));
        refused("ramp end", &|c| c.ramp_end = u64::MAX);
        refused("entries page", &|c| c.entries_page = Some(u64::MAX));
        refused("late-route bound", &|c| c.late_lost_until = u64::MAX);
        refused("crossfade start", &|c| c.slots[0].xfade = Some(u64::MAX));
        refused("fade start", &|c| c.slots[0].fade_start = Some(u64::MAX));
        refused("page entry", &|c| c.page_entries.push((8, u64::MAX)));
        refused("unresolved", &|c| c.unresolved.push((8, u64::MAX)));
        refused("late route", &|c| c.late_routes.push((8, u64::MAX)));
        refused("entry index", &|c| c.pending[0].at_index = u64::MAX);
        refused("gain", &|c| {
            c.pending.push(RtEntry {
                id: 8,
                at_index: 9600,
                change: Change::Gain {
                    input: 0,
                    output: 0,
                    gain: 1.0e9,
                },
            })
        });
        refused("strip parameter", &|c| {
            c.pending[0].change = Change::Strip {
                slot: 1,
                param: StripParam::Trim { db: 1000.0 },
            }
        });
        refused("pending list", &|c| {
            let e = c.pending[0];
            c.pending.resize(PENDING_CHANGES + 1, e)
        });
        // The first ramp is the one of length 48 at position 0.
        let encoded = copy().encode().replacen("\"pos\":0", "\"pos\":49", 1);
        assert!(
            matches!(CoreCopy::decode(&encoded), Err(DecodeError::Malformed(_))),
            "a ramp position past its length"
        );
    }

    /// A core after a block whose index would overflow at the largest
    /// block size is refused.
    #[test]
    fn a_core_after_a_block_beyond_any_index_is_refused() {
        let mut copy = copy();
        copy.k0 = INDEX_MAX / 1024 + 1;
        assert!(matches!(
            CoreCopy::decode(&copy.encode()),
            Err(DecodeError::Malformed(_))
        ));
        copy.k0 = INDEX_MAX / 1024;
        assert!(CoreCopy::decode(&copy.encode()).is_ok());
    }

    /// A core of another shape, or with an entry outside the engine,
    /// does not fit it.
    #[test]
    fn a_core_fits_only_its_own_shape() {
        let mut copy = copy();
        assert!(copy.core.fits(2, 2, &[1, 1], 8));
        assert!(!copy.core.fits(2, 3, &[1, 1], 8));
        assert!(!copy.core.fits(2, 2, &[1, 1], 0));
        copy.core.pending.push(RtEntry {
            id: 8,
            at_index: 9648,
            change: Change::InputDown { slot: 2 },
        });
        assert!(!copy.core.fits(2, 2, &[1, 1], 8));
    }
}
