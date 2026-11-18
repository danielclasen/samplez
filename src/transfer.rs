//! Engine state transfer: the core the RT thread copies after a block, so
//! that another replica can replay from it.
//!
//! The core is the engine state output depends on apart from delay lines:
//! the matrix with its ramps, duck masks, depths and mask crossfades, the
//! duck detectors with their keys, filter, hold and envelope state, the
//! entries pending and applied in the page in progress, each input slot's
//! log state, fades and strip with its position, the entries waiting for
//! their generation and the pending generation's entry id, and the
//! unresolved entries with the late re-routes. Meters, hold rings, block statuses, page hashes in progress
//! and the readers stay with the replica. The RT thread copies it into a
//! buffer allocated off the RT thread, of the same shape, so the copy
//! allocates nothing.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use samplez_dsp::strip::MAX_LEGS;
use samplez_dsp::{Matrix, Strip, StripParam};
use samplez_mxl::time;
use serde::{Deserialize, Serialize};

use crate::batch::{self, Batch, DuckItem, Item};
use crate::blocks::PageHash;
use crate::ducks::{DuckChange, DuckCore, DuckSetting, DuckTargets, KeySet};
use crate::engine::Entry;
use crate::rt_loop::{Change, Control, LogId, PENDING_CHANGES, Rt, RtEntry, UNRESOLVED, slots_of};

/// The engine version a core's encoding carries: a core is replayed only
/// by the engine that copied it.
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The highest sample index a decoded core may hold: far beyond any
/// running engine's, and low enough that adding a ramp or a block to it
/// cannot overflow.
pub(crate) const INDEX_MAX: u64 = 1 << 62;

/// An input slot's part of the core.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SlotCore {
    pub up: bool,
    pub held: bool,
    pub fade_start: Option<u64>,
    pub silenced_last: bool,
    pub xfade: Option<u64>,
    /// The slot's strip, its filter and dynamics state included, and the
    /// index of the next sample it processes. A core built from product
    /// state holds none and leaves the RT thread's.
    pub strip: Option<Strip>,
    pub strip_pos: Option<u64>,
}

/// The core of an engine after a block.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Core {
    pub(crate) matrix: Matrix,
    pub(crate) ducks: DuckCore,
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
    /// The batch of each [`Change::Batch`] pending, by log id.
    pub(crate) batches: Vec<(LogId, Arc<Batch>)>,
    /// The batches an install dropped, to leave the RT thread with the
    /// buffer. Capacity [`PENDING_CHANGES`].
    #[serde(skip)]
    pub(crate) handed: Handed,
}

/// Batches a core buffer carries off the RT thread; never part of what a
/// core holds.
#[derive(Debug, Default)]
pub(crate) struct Handed(pub Vec<Arc<Batch>>);

impl PartialEq for Handed {
    fn eq(&self, _: &Handed) -> bool {
        true
    }
}

impl Core {
    /// A buffer for the core of an engine with `inputs` and `outputs`
    /// channels, `slots` input slots and room for `page_entries` entries
    /// of one page's fingerprint.
    pub(crate) fn new(inputs: usize, outputs: usize, slots: usize, page_entries: usize) -> Core {
        Core {
            matrix: Matrix::new(inputs, outputs),
            ducks: DuckCore::new(0),
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
            batches: Vec::with_capacity(PENDING_CHANGES),
            handed: Handed(Vec::with_capacity(PENDING_CHANGES)),
        }
    }

    /// Off the RT thread, before a buffer goes to it: drops the batches
    /// it carries, so the RT thread drops none, and makes room for as
    /// many as it holds pending.
    pub(crate) fn prepare(&mut self) {
        self.batches.clear();
        self.batches.reserve_exact(PENDING_CHANGES);
        self.handed.0.clear();
        self.handed.0.reserve_exact(PENDING_CHANGES);
    }

    /// Makes `batches` the core's, sharing them: nothing is allocated or
    /// freed, the batches the buffer held kept with the handed ones.
    pub(crate) fn put_batches(&mut self, batches: &[(LogId, Arc<Batch>)]) {
        while let Some((_, b)) = self.batches.pop() {
            self.keep_or_forget(b);
        }
        for (id, b) in batches {
            if self.batches.len() == self.batches.capacity() {
                debug_assert!(false, "a core without room for its batches");
                return;
            }
            self.batches.push((*id, Arc::clone(b)));
        }
    }

    /// Takes the batch of entry `id` out of the core.
    pub(crate) fn take_batch(&mut self, id: LogId) -> Option<Arc<Batch>> {
        let n = self.batches.iter().position(|b| b.0 == id)?;
        Some(self.batches.swap_remove(n).1)
    }

    /// Keeps `batch` with the handed ones, to leave the RT thread with
    /// the buffer; back when they are at capacity.
    pub(crate) fn keep(&mut self, batch: Arc<Batch>) -> Result<(), Arc<Batch>> {
        let handed = &mut self.handed.0;
        if handed.len() < handed.capacity() {
            handed.push(batch);
            Ok(())
        } else {
            Err(batch)
        }
    }

    fn keep_or_forget(&mut self, batch: Arc<Batch>) {
        if let Err(b) = self.keep(batch) {
            // A prepared buffer holds none: unreachable.
            std::mem::forget(b);
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

    /// Duck group `group`'s detector; none beyond [`samplez_dsp::MAX_DUCKS`].
    pub fn detector(&self, group: usize) -> Option<&samplez_dsp::Detector> {
        self.ducks.detectors.get(group)
    }

    /// The duck mask of the coefficient from input channel `input` to
    /// matrix column `output`.
    pub fn duck_mask(&self, input: usize, output: usize) -> u8 {
        self.matrix.mask(input, output)
    }

    /// The depth of input channel `input` under duck group `group` on the
    /// next sample, dB.
    pub fn duck_depth(&self, input: usize, group: usize) -> f32 {
        self.matrix.depth(input, group)
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
    /// every strip parameter and duck setting within its range (legs
    /// aside), a slot named for each duck key keyed and none for another,
    /// the lists within their capacities. A core from another replica is
    /// checked when decoded; a strip, a detector and the matrix in it are
    /// checked as they deserialize ([`Strip::is_valid`],
    /// [`samplez_dsp::Detector::is_valid`]).
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
        let slot = |s: &SlotCore| {
            s.fade_start.is_none_or(index)
                && s.xfade.is_none_or(index)
                && s.strip_pos.is_none_or(index)
        };
        let ats = |list: &[(LogId, u64)]| list.iter().all(|&(_, at)| index(at));
        self.pos.is_none_or(index)
            && self.ducks.sane()
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
            && self.batches_match()
    }

    /// Whether each pending [`Change::Batch`] has its batch, sane and as
    /// the change says, and no other batch is held. A batch awaiting its
    /// generation was applied but for that and holds none.
    fn batches_match(&self) -> bool {
        let mut held = 0;
        for e in &self.pending {
            let Change::Batch { strips, .. } = e.change else {
                continue;
            };
            held += 1;
            let mut of = self.batches.iter().filter(|b| b.0 == e.id);
            let (Some((_, b)), None) = (of.next(), of.next()) else {
                return false;
            };
            if !b.sane() || (strips && b.strips.is_empty()) {
                return false;
            }
        }
        held == self.batches.len()
    }

    /// Whether this sane core is of an engine with `inputs` and `outputs`
    /// channels, input slots of `legs` legs each, strips and duck key
    /// filters ramping over `ramp` samples and room for `page_entries`
    /// entries of one page's fingerprint, every entry and every duck key
    /// within them: checked before a replay installs it. A slot has a
    /// strip of its legs exactly when it is at most [`MAX_LEGS`] wide.
    pub(crate) fn fits(
        &self,
        inputs: usize,
        outputs: usize,
        legs: &[usize],
        ramp: u32,
        page_entries: usize,
    ) -> bool {
        let slots = legs.len();
        let strip = |(s, &legs): (&SlotCore, &usize)| match &s.strip {
            Some(strip) => strip.legs() == legs && strip.ramp() == ramp,
            None => legs > MAX_LEGS,
        };
        let entry = |e: &RtEntry| fits(&e.change, inputs, outputs, legs);
        let batch = |(_, b): &(LogId, Arc<Batch>)| b.fits(inputs, outputs, legs);
        let shape = (
            self.matrix.inputs(),
            self.matrix.outputs(),
            self.slots.len(),
        );
        self.sane()
            && shape == (inputs, outputs, slots)
            && self.page_entries.len() <= page_entries
            && self.pending.iter().chain(&self.awaiting_gen).all(entry)
            && self.batches.iter().all(batch)
            && self.slots.iter().zip(legs).all(strip)
            && self.ducks.fits(legs, ramp)
    }
}

/// Whether change `c` is within an engine of `inputs` and `outputs`
/// channels and input slots of `legs` legs: a coefficient and a gain it
/// has, a slot, and a strip parameter that slot's strip takes. A batch's
/// own values are checked by [`Batch::fits`].
pub(crate) fn fits(c: &Change, inputs: usize, outputs: usize, legs: &[usize]) -> bool {
    let slots = legs.len();
    match *c {
        Change::Gain {
            input,
            output,
            gain,
        } => input < inputs && output < outputs && samplez_dsp::valid_gain(gain),
        Change::InputDown { slot } | Change::InputUp { slot } | Change::Route { slot } => {
            slot < slots
        }
        Change::Strip { slot, param } => {
            slot < slots && legs[slot] <= MAX_LEGS && param.check(legs[slot]).is_ok()
        }
        Change::Batch { up, routed, .. } => slots_of(up | routed).all(|s| s < slots),
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
    /// The core is a replay's: it names the pending generation it was
    /// computed with, which the RT thread must hold, and holds the
    /// strips. A core built from product state was computed with neither
    /// and keeps the RT thread's.
    pub replayed: bool,
    /// Strip parameters set at the install index, by slot: the product
    /// state's. Built off the RT thread and handed back with the core.
    pub strips: Vec<(usize, StripParam)>,
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

    /// Takes the committed log entry `id` at `at_index` of one engine
    /// entry: [`Replay::append_batch`] of one.
    pub fn append(&mut self, id: LogId, at_index: u64, entry: &Entry) -> Result<(), ReplayError> {
        self.append_batch(id, at_index, std::slice::from_ref(entry))
    }

    /// Takes the committed log entry `id` at `at_index`, its engine
    /// entries `entries` folded as [`crate::Engine::append_batch`] folds
    /// them; one the core knew already, or of no entries, changes
    /// nothing. Only entries that need no generation replay; one whose
    /// index the replay computed is late.
    pub fn append_batch(
        &mut self,
        id: LogId,
        at_index: u64,
        entries: &[Entry],
    ) -> Result<(), ReplayError> {
        if id <= self.last_id || entries.is_empty() {
            return Ok(());
        }
        let matrix = &self.rt.mix.matrix;
        let (inputs, columns) = (matrix.inputs(), matrix.outputs());
        let legs: Vec<usize> = self.rt.slots.iter().map(|s| s.channels).collect();
        let rows: Vec<(usize, usize)> = (self.rt.slots.iter())
            .map(|s| (s.first_channel, s.channels))
            .collect();
        let outside =
            || ReplayError::Invalid(format!("log entry {id} has an entry outside the engine"));
        let mut items = Vec::with_capacity(entries.len());
        for e in entries {
            items_of(e, &rows, &mut items).map_err(|_| outside())?;
        }
        // The engine's checks: a donor took no entry outside them.
        let fit = |i: &Item| match i {
            Item::Change(c) => fits(c, inputs, columns, &legs),
            Item::Duck(DuckItem::Mask { input, output, .. }) => {
                *input < inputs && *output < columns
            }
            Item::Duck(_) => true,
        };
        if !items.iter().all(fit) {
            return Err(outside());
        }
        // Folded first, as the engine folds it: an `InputUp` followed by
        // an `InputDown` of the slot needs no generation.
        let folded =
            batch::fold(&items, columns, inputs * columns).map_err(ReplayError::Invalid)?;
        let Some((change, batch)) = folded else {
            return Ok(());
        };
        if batch
            .as_ref()
            .is_some_and(|b| !(b.sane() && b.fits(inputs, columns, &legs)))
        {
            return Err(outside());
        }
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
        if self.rt.mix.push_with(e, batch).is_err() {
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
            // The batches it spent, freed here, outside the computation.
            self.rt.spent.clear();
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

/// What log entry `entry` sets on the RT thread, appended to `out`: a
/// change, or duck items, a depth one per row of its slot, input slot `s`
/// being the matrix rows from `rows[s].0`, `rows[s].1` of them. Refused for
/// a duck setting no batch holds: a group beyond a byte, more keys than a
/// detector takes, a slot that does not exist; the values are checked by
/// the caller, or as a batch holding them is.
pub(crate) fn items_of(
    entry: &Entry,
    rows: &[(usize, usize)],
    out: &mut Vec<Item>,
) -> Result<(), String> {
    let (group, setting) = match entry {
        Entry::Duck { group, setting } => (group, setting),
        &Entry::DuckMask {
            input,
            output,
            mask,
        } => {
            out.push(Item::Duck(DuckItem::Mask {
                input,
                output,
                mask,
            }));
            return Ok(());
        }
        other => {
            out.push(Item::Change(change_of(other)));
            return Ok(());
        }
    };
    let group = u8::try_from(*group).map_err(|_| format!("no duck group {group}"))?;
    let change = match *setting {
        DuckSetting::Depth { slot, db } => {
            let &(first, n) = rows
                .get(slot)
                .ok_or_else(|| format!("no input slot {slot}"))?;
            for row in first..first + n {
                let row = row as u32;
                out.push(Item::Duck(DuckItem::Depth { row, group, db }));
            }
            return Ok(());
        }
        DuckSetting::Detector(ref p) => DuckChange::Detector(**p),
        DuckSetting::Keys(ref keys) => {
            if let Some(&(slot, _)) = keys.iter().find(|k| k.0 >= rows.len()) {
                return Err(format!("no input slot {slot}"));
            }
            DuckChange::Keys(KeySet::of(keys).ok_or_else(|| {
                format!(
                    "{} keys, a detector takes at most {}",
                    keys.len(),
                    samplez_dsp::duck::MAX_KEYS
                )
            })?)
        }
        DuckSetting::Mode(m) => DuckChange::Mode(m),
        DuckSetting::Hold { held } => DuckChange::Hold(held),
    };
    out.push(Item::Duck(DuckItem::Set { group, change }));
    Ok(())
}

/// The change log entry `entry`, other than a duck entry, makes on the RT
/// thread.
fn change_of(entry: &Entry) -> Change {
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
        Entry::Duck { .. } | Entry::DuckMask { .. } => {
            unreachable!("duck entries are items of their own")
        }
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
    /// Per input slot the flow it reads in the log, its last `Route`'s
    /// or its configured one.
    pub flows: Vec<String>,
    /// Per input slot the strip parameters the log set, as values: each
    /// one set again at the install, the strip's own state kept.
    pub strips: Vec<Vec<StripParam>>,
    /// Per duck group, at most [`samplez_dsp::MAX_DUCKS`], its targets: a
    /// group past the list holds [`DuckTargets::default`]. The detectors
    /// start cold at the install: key filters at rest, no hold, the
    /// envelope at 0, or at full depth for a group its manual controls
    /// trigger, depths at their targets.
    pub ducks: Vec<DuckTargets>,
    /// The duck mask of every coefficient, shaped as `gains`, or empty for
    /// every mask 0.
    pub masks: Vec<Vec<u8>>,
    /// Committed entries not applied yet: log id, `at_index` and entry;
    /// the entries of one log entry adjacent, in order, folded as
    /// [`crate::Engine::append_batch`] folds them.
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
    use samplez_dsp::{BandType, CompParams, EqBand, GateParams, StripParam};
    use serde_json::{Value, json};

    use super::*;

    /// A strip mid-envelope: high-pass, a bell, gate and a compressor of
    /// the longest release on, after a burst over the threshold.
    fn strip() -> Strip {
        let mut s = Strip::new(1, 48);
        let bell = EqBand {
            kind: BandType::Peak,
            freq: 1000.0,
            gain_db: 6.0,
            q: 1.0,
        };
        for p in [
            StripParam::Hpf {
                enabled: true,
                freq: 80.0,
            },
            StripParam::Eq { enabled: true },
            StripParam::EqBand {
                band: 1,
                value: bell,
            },
            StripParam::Gate(GateParams {
                enabled: true,
                threshold_db: -50.0,
                range_db: 40.0,
                attack_ms: 1.0,
                release_ms: 200.0,
            }),
            StripParam::Comp(CompParams {
                enabled: true,
                threshold_db: -30.0,
                ratio: 4.0,
                attack_ms: 5.0,
                release_ms: 5000.0,
                knee_db: 6.0,
                makeup_db: 3.0,
            }),
        ] {
            assert!(s.set(p));
        }
        let mut burst: Vec<f32> = (0..480).map(|n| 0.8 * (n as f32 * 0.13).sin()).collect();
        s.process(&mut burst, 480);
        s
    }

    fn copy() -> CoreCopy {
        let mut core = Core::new(2, 2, 2, 8);
        core.ducks = DuckCore::new(48);
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
        core.slots[0] = SlotCore {
            strip: Some(Strip::new(1, 48)),
            strip_pos: Some(4800),
            ..SlotCore::default()
        };
        core.slots[1] = SlotCore {
            up: true,
            held: false,
            fade_start: Some(480),
            silenced_last: true,
            xfade: None,
            strip: Some(strip()),
            strip_pos: Some(4800),
        };
        core.unresolved.push((9, 1 << 40));
        CoreCopy {
            k0: 99,
            last_id: 7,
            core: Box::new(core),
        }
    }

    /// The copy another replica decodes is the copy encoded: the matrix
    /// with its ramps, the entries and the slot state, the strips' filter
    /// and dynamics state bit for bit.
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

    /// A strip state a peer sends that no strip holds is refused when
    /// decoded, before the RT thread runs it: a value beyond f32 decodes
    /// as an infinity and would turn every sample after it into one, an
    /// envelope, pole or ramp out of its range runs the dynamics outside
    /// anything their parameters give, a filter target other than its
    /// parameters' may be unstable, and a state array longer than a
    /// strip's is refused before anything is allocated for it.
    #[test]
    fn a_strip_state_no_strip_holds_is_refused() {
        let wire: Value = serde_json::from_str(&copy().encode()).unwrap();
        let refused = |what: &str, f: &dyn Fn(&mut Value)| {
            let mut wire = wire.clone();
            f(&mut wire["copy"]["core"]["slots"][1]);
            assert!(
                matches!(
                    CoreCopy::decode(&wire.to_string()),
                    Err(DecodeError::Malformed(_))
                ),
                "{what}"
            );
        };
        // NaN has no JSON form: it encodes as null, which no f32 takes.
        refused("NaN filter state", &|s| {
            s["strip"]["hpf"]["state"][0][0] = Value::Null
        });
        refused("infinite equaliser state", &|s| {
            s["strip"]["eq"][1]["state"][0][1] = json!(1e39)
        });
        refused("infinite crossfade state", &|s| {
            s["strip"]["alt_eq"][0][0][0] = json!(-1e39)
        });
        refused("gate gain below its range", &|s| {
            s["strip"]["gate"]["y"] = json!(-200.0)
        });
        refused("compressor gain above 0 dB", &|s| {
            s["strip"]["comp"]["y"] = json!(3.0)
        });
        refused("smoother pole of no decay", &|s| {
            s["strip"]["comp"]["release"]["sample"] = json!(1.5)
        });
        refused("gate hold beyond its length", &|s| {
            s["strip"]["gate"]["hold"] = json!(1 << 30)
        });
        refused("gain interval phase", &|s| {
            s["strip"]["comp"]["interval"]["phase"] = json!(samplez_dsp::strip::GAIN_INTERVAL)
        });
        refused("makeup beyond its range", &|s| {
            s["strip"]["comp"]["makeup"]["target"] = json!(100.0)
        });
        refused("ramp past its length", &|s| {
            s["strip"]["fade"]["len"] = json!(10);
            s["strip"]["fade"]["pos"] = json!(11);
        });
        refused("filter target of other parameters", &|s| {
            s["strip"]["hpf_freq"] = json!(100.0)
        });
        refused("filter ramp through an unstable value", &|s| {
            s["strip"]["hpf"]["len"] = json!(100);
            s["strip"]["hpf"]["step"][0] = json!(-1.0);
        });
        // Finite and stable, but beyond any coefficient a section ramps
        // to: 3e38 x + 3e38 v1 + 3e38 v2 overflows on an ordinary sample
        // for the 2^24 samples of the ramp.
        refused("filter mix starting beyond any target", &|s| {
            let hpf = &mut s["strip"]["hpf"];
            let len = f64::from(samplez_dsp::MAX_RAMP_LEN);
            let target: Vec<f64> = (0..5).map(|i| hpf["target"][i].as_f64().unwrap()).collect();
            let c0 = [target[0], target[1], 3e38, 3e38, 3e38];
            for i in 0..5 {
                hpf["c0"][i] = json!(c0[i]);
                hpf["step"][i] = json!((target[i] - c0[i]) / len);
            }
            hpf["len"] = json!(samplez_dsp::MAX_RAMP_LEN);
            hpf["pos"] = json!(0);
        });
        refused("filter mix ending beyond any target", &|s| {
            s["strip"]["eq"][1]["len"] = json!(100);
            s["strip"]["eq"][1]["step"][3] = json!(10.0);
        });
        // Finite, but 2 v1 - s0 overflows within a sample.
        refused("filter state beyond any a block leaves", &|s| {
            s["strip"]["hpf"]["state"][0][0] = json!(3e38)
        });
        refused("crossfade state beyond any a block leaves", &|s| {
            s["strip"]["alt_eq"][0][0][1] = json!(1e31)
        });
        refused("leg gain beyond any trim", &|s| {
            s["strip"]["gain"][0]["c0"] = json!(1.0e5)
        });
        refused("compressor gain beyond any reduction", &|s| {
            s["strip"]["comp"]["y"] = json!(-1.0e38)
        });
        refused("release slower than the longest", &|s| {
            s["strip"]["comp"]["release"]["interval"] = json!(0.999_999_94)
        });
        refused("gain step beyond one interval", &|s| {
            s["strip"]["gate"]["interval"]["step"] = json!(0.5)
        });
        refused("leg gain of other trims", &|s| {
            s["strip"]["trim_db"] = json!(3.0)
        });
        refused("trim out of range", &|s| {
            s["strip"]["trim_db"] = json!(60.0)
        });
        refused("legs beyond a strip", &|s| {
            s["strip"]["legs"] = json!(1u64 << 40)
        });
        refused("no legs", &|s| s["strip"]["legs"] = json!(0));
        refused("state array beyond a strip's legs", &|s| {
            s["strip"]["hpf"]["state"]
                .as_array_mut()
                .unwrap()
                .push(json!([0.0, 0.0]))
        });
        refused("strip position", &|s| s["strip_pos"] = json!(u64::MAX));
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

    /// A matrix the size of which a peer names beyond the coefficient
    /// limit is refused before anything is allocated for it: 2^40 inputs
    /// would abort the process on the allocation.
    #[test]
    fn a_matrix_beyond_the_coefficient_limit_is_refused() {
        let copy = copy();
        let wire = serde_json::to_string(&copy.core.matrix).unwrap();
        let huge = wire.replacen("\"inputs\":2", &format!("\"inputs\":{}", 1u64 << 40), 1);
        assert_ne!(wire, huge);
        let encoded = copy.encode().replacen(&wire, &huge, 1);
        assert!(matches!(
            CoreCopy::decode(&encoded),
            Err(DecodeError::Malformed(_))
        ));
    }

    /// A core of another shape, or with an entry outside the engine,
    /// does not fit it.
    #[test]
    fn a_core_fits_only_its_own_shape() {
        let mut copy = copy();
        assert!(copy.core.fits(2, 2, &[1, 1], 48, 8));
        assert!(!copy.core.fits(2, 3, &[1, 1], 48, 8));
        assert!(!copy.core.fits(2, 2, &[1, 1], 48, 0));
        // Strips of other legs, another ramp, or none on a narrow slot;
        // a slot wider than a strip has none.
        assert!(!copy.core.fits(2, 2, &[1, 2], 48, 8));
        assert!(!copy.core.fits(2, 2, &[1, 1], 480, 8));
        copy.core.slots[0].strip = None;
        assert!(!copy.core.fits(2, 2, &[1, 1], 48, 8));
        assert!(copy.core.fits(2, 2, &[MAX_LEGS + 1, 1], 48, 8));
        copy.core.slots[0].strip = Some(Strip::new(1, 48));
        copy.core.pending.push(RtEntry {
            id: 8,
            at_index: 9648,
            change: Change::InputDown { slot: 2 },
        });
        assert!(!copy.core.fits(2, 2, &[1, 1], 48, 8));
    }

    /// A copy whose detector 2 is keyed by slot 1, forced on and
    /// mid-transit, with a key filter state, and whose matrix ducks
    /// coefficient 1 -> 0 by it.
    fn ducked() -> CoreCopy {
        use samplez_dsp::{DuckMode, MAX_DUCKS, PlanarBuffer};
        let mut c = copy();
        let keys = KeySet::of(&[(1, 0.5)]).unwrap();
        let ducks = &mut c.core.ducks;
        assert!(ducks.set(4800, 2, DuckChange::Keys(keys), |_| 1));
        assert!(ducks.set(4800, 2, DuckChange::Mode(DuckMode::On), |_| 1));
        let mut d = crate::ducks::Ducks::new(48);
        d.core.copy_from(ducks);
        let mut inbuf = PlanarBuffer::new(2, 480);
        inbuf.channel_mut(1).fill(0.3);
        let mut env = PlanarBuffer::new(MAX_DUCKS, 480);
        d.process(4800, 480, |_| 0, &inbuf, &mut env, None);
        ducks.copy_from(&d.core);
        let e = ducks.detectors[2].envelope();
        assert!(0.0 < e && e < 1.0, "{e}");
        assert!(c.core.matrix.set_mask(1, 0, 0b100, 48));
        assert!(c.core.matrix.set_depth(1, 2, 12.0, 48));
        c
    }

    /// A core with ducks decodes to the copy encoded: the detectors with
    /// their keys, filter, hold and envelope state, the masks, depths and
    /// mask crossfades bit for bit.
    #[test]
    fn a_core_with_ducks_decodes_to_the_copy_encoded() {
        let copy = ducked();
        let decoded = CoreCopy::decode(&copy.encode()).unwrap();
        assert_eq!(decoded, copy);
        let (a, b) = (
            decoded.core.detector(2).unwrap(),
            copy.core.detector(2).unwrap(),
        );
        assert_eq!(a.envelope().to_bits(), b.envelope().to_bits());
        assert_eq!(decoded.core.duck_mask(1, 0), 0b100);
        assert!(decoded.core.fits(2, 2, &[1, 1], 48, 8));
        assert!(
            !decoded.core.fits(2, 2, &[1, 2], 48, 8),
            "key 0 of another width"
        );
        assert!(
            !decoded.core.fits(2, 2, &[1, 1], 480, 8),
            "another key filter ramp"
        );
    }

    /// Each duck value a peer can send that no engine holds is refused
    /// when decoded: a detector state none reaches, keys and slots that
    /// disagree, a position beyond any index, masks, depths and mask
    /// crossfades out of range, and a pending batch's duck setting.
    #[test]
    fn a_duck_state_no_engine_holds_is_refused() {
        let wire: Value = serde_json::from_str(&ducked().encode()).unwrap();
        assert!(CoreCopy::decode(&wire.to_string()).is_ok());
        let refused = |what: &str, f: &dyn Fn(&mut Value)| {
            let mut wire = wire.clone();
            f(&mut wire["copy"]["core"]);
            assert!(
                matches!(
                    CoreCopy::decode(&wire.to_string()),
                    Err(DecodeError::Malformed(_))
                ),
                "{what}"
            );
        };
        let det = |c: &mut Value, f: &dyn Fn(&mut Value)| f(&mut c["ducks"]["detectors"][2]);
        refused("envelope above 1", &|c| det(c, &|d| d["to"] = json!(1.5)));
        refused("hold beyond its time", &|c| {
            det(c, &|d| d["hold"] = json!(1 << 30))
        });
        refused("threshold out of range", &|c| {
            det(c, &|d| d["params"]["threshold_db"] = json!(6.0))
        });
        refused("key filter state beyond any a block leaves", &|c| {
            det(c, &|d| d["keys"][0]["hpf"]["state"][0][0] = json!(3e38))
        });
        refused("key gain out of range", &|c| {
            det(c, &|d| d["keys"][0]["gain"] = json!(-1.0))
        });
        refused("a key without a slot", &|c| {
            c["ducks"]["keys"][2][0] = Value::Null
        });
        refused("a slot without a key", &|c| {
            c["ducks"]["keys"][3][0] = json!(1)
        });
        refused("a detector missing", &|c| {
            c["ducks"]["detectors"].as_array_mut().unwrap().pop();
        });
        refused("position beyond any index", &|c| {
            c["ducks"]["pos"] = json!(u64::MAX)
        });
        refused("mask beyond the matrix", &|c| {
            c["matrix"]["masks"][0][0] = json!(4)
        });
        refused("depth beyond its range", &|c| {
            c["matrix"]["depths"][0][1]["target"] = json!(41.0)
        });
        refused("mask crossfade past its length", &|c| {
            let duck = &mut c["matrix"]["columns"][0][0]["duck"];
            assert!(duck.is_object(), "{c}");
            duck["pos"] = json!(100);
        });
        let mut copy = ducked();
        let p = samplez_dsp::DetectorParams {
            release_ms: 1.0,
            ..samplez_dsp::DetectorParams::DEFAULT
        };
        let item = Item::Duck(DuckItem::Set {
            group: 0,
            change: DuckChange::Detector(p),
        });
        let (change, b) = batch::fold(&[item], 2, 4).unwrap().unwrap();
        copy.core.pending.push(RtEntry {
            id: 8,
            at_index: 9600,
            change,
        });
        copy.core.batches.push((8, b.unwrap()));
        assert!(matches!(
            CoreCopy::decode(&copy.encode()),
            Err(DecodeError::Malformed(_))
        ));
    }
}
