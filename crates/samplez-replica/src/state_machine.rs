//! Applying entries: dedup, the product's state, and the replicated state
//! the replica keeps beside it.

use std::collections::{BTreeMap, VecDeque};

use crate::dedup::{Dedup, Limits};
use crate::entry::{
    Baseline, Body, Command, Disruption, NodeId, Origin, Outcome, Request, Resolution, Response,
    SessionKey,
};
use crate::snapshot::{InFlightBits, Replicated};

/// The product's state, applied in log order on every replica. A
/// snapshot install loads into an empty one its [`Factory`] builds, with
/// the replica's configuration, before it replaces the state.
///
/// The store calls these methods under its own locks, as it applies an
/// entry, builds a snapshot or reads the state for the replica's contact
/// loop, which holds the contact lock meanwhile. None of them may call
/// back into the [`crate::Replica`] or its [`crate::log_store::Store`]:
/// the call panics rather than deadlock. Code the product runs inside
/// [`crate::log_store::Store::with_state`] may. `Sync`, because the
/// contact loop reads the state beside such a closure.
pub trait Product: Send + Sync + 'static {
    /// What one command or expiry changed, as [`Product::delta`] gets it
    /// back.
    type Change;

    /// Applies one command. A command whose second application could undo
    /// a later one's effect carries the ordering its apply needs and is
    /// refused as [`Effect::Stale`] when older, with no effect: a copy can
    /// apply again once its dedup record retired.
    ///
    /// `ctx` says how the writer ordered it. A product that drives an
    /// engine refuses, with no effect, a command that would re-route an
    /// input and was ordered off the generation path
    /// (`ctx.generation == Some(false)`), and one that would re-route
    /// inputs of more than one domain: every replica decides that alike
    /// from its state.
    fn apply(&mut self, origin: &Origin, command: &[u8], ctx: &Apply) -> Effect<Self::Change>;

    /// Whether the product drives an engine through [`Product::targets`]:
    /// its engine entries are derived from its state as each entry
    /// applies, it owns every input's flow, and a client command asking
    /// for an index beyond the earliest is refused.
    const DRIVES_ENGINE: bool = false;

    /// The engine targets of the current state: absolute, a function of
    /// the state and the configuration the product was built with alone,
    /// the same bits on every replica. None for a product that drives no
    /// engine. Only a change [`Product::apply`] or [`Product::expire`]
    /// reports may change them: the state machine derives nothing for
    /// anything else, [`Product::forget`] included.
    fn targets(&self) -> Option<samplez::Targets> {
        None
    }

    /// The input slots `command` from `origin` would re-route applied to
    /// the current state: the writer orders such a command alone on the
    /// generation path. Read before the command is ordered, so the state
    /// it applies to may differ; [`Product::apply`] decides.
    fn reroutes(&self, _origin: &Origin, _command: &[u8]) -> Vec<u32> {
        Vec::new()
    }

    /// Ends what `session` held under its lease, such as touch ownership:
    /// [`Effect::Changed`] when that changed the state. The session's
    /// lifetime ends here; a later command or renewal of it starts a new
    /// one.
    fn expire(&mut self, session: &SessionKey) -> Effect<Self::Change>;

    /// Drops what the product keeps for `session` past its lifetime, such
    /// as the last sequence number applied per target: called once the
    /// stamping index of the entries applied reaches the one of the expiry
    /// that ended the lifetime plus the dedup retention plus the failover
    /// bound ([`Limits`]), the longest a copy of one of its commands can
    /// apply after the original, unless the session came back meanwhile.
    /// In apply order, so at the same point on every replica. Never
    /// changes [`Product::targets`]: nothing derives from it, so a replica
    /// that loads a snapshot after it would derive other entries than one
    /// that applied the whole log.
    fn forget(&mut self, session: &SessionKey);

    /// The one delta clients get for one entry's changes, in the order
    /// applied: a command's with its origin, id and at_index, a server-side
    /// one, such as a lease expiry, without; `stamp` is the entry's
    /// stamping index. Where two change the same value the later one wins.
    /// Called with the state after the entry, and only when something
    /// changed.
    fn delta(&self, stamp: u64, changes: &[Changed<'_, Self::Change>]) -> String;

    /// The whole state as the product's clients read it.
    fn text(&self) -> String;

    /// [`Product::text`] as a closure the replica calls once it no
    /// longer holds the state, for a client snapshot: a product that
    /// keeps its state behind an `Arc` it replaces or copies on apply
    /// returns one holding a clone of that `Arc`, so the snapshot holds
    /// the state, and holds off applies, only for the clone. None, the
    /// default, builds the text under the state.
    ///
    /// The closure must read only state fixed at this call. Entries may
    /// be applied before it runs: one that reads anything an apply
    /// changes reports the text of a later revision than the snapshot
    /// names.
    fn text_later(&self) -> Option<Box<dyn FnOnce() -> String + Send>> {
        None
    }

    /// The state as snapshots carry it.
    fn save(&self) -> Vec<u8>;

    /// Replaces the state with a saved one.
    fn load(&mut self, saved: &[u8]) -> Result<(), String>;

    /// A hash of the configuration the product was built with that
    /// changes how it applies a command or loads a saved state, 0 for a
    /// product without one. Every member of a set must hold equal
    /// configuration: product state is shared by every replica, so a
    /// member that applied a command under another configuration would
    /// hold another state. A snapshot from a replica with another hash is
    /// not installed, and the leader orders no client command while a
    /// member reports another one.
    fn config_hash(&self) -> u64;

    /// The version of how the product derives its state and engine
    /// entries from the log: bumped when the same log would apply to
    /// another state, derive other entries or save another state. It is
    /// part of the compatibility key ([`crate::rollout::Versions`]), so
    /// replicas of a release that keeps it roll in as peers and others
    /// hand the baseline over.
    const DERIVATION_VERSION: u32;

    /// The input slot a `Route` body re-routes, so the state machine keeps
    /// only the latest passed route per slot; none keeps every distinct
    /// passed route.
    fn route_slot(&self, _route: &[u8]) -> Option<u32> {
        None
    }
}

/// Builds an empty product with the replica's configuration: the state a
/// store opens into and every snapshot install loads into.
pub type Factory<P> = std::sync::Arc<dyn Fn() -> P + Send + Sync>;

/// How the writer ordered a command, as [`Product::apply`] gets it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Apply {
    /// [`Command::generation`].
    pub generation: Option<bool>,
    /// The sample index the command's entry takes effect at, at the
    /// engine's rate in the epoch of the flows' indexes: its own
    /// at_index, raised to [`StateMachine::floor`] ([`floored`]), the
    /// index of the record of its engine entries. Every replica applies the
    /// entry at the same one. None for an entry that has none.
    ///
    /// The engine applies the record from the next block boundary,
    /// `at_index.next_multiple_of(block)`. A later Restamp of the entry
    /// moves its engine entries to another index, and a Void drops them,
    /// while the product applied the command with this one.
    pub at_index: Option<u64>,
}

/// An entry's at_index further than this past its stamping index, one
/// second at 48 kHz, does not move [`StateMachine::floor`] past the stamp:
/// an entry from a writer that kept a far client index would hold every
/// later entry back until it.
pub const AHEAD_MAX: u64 = 48_000;

/// How long a derived record is kept past its index: a resync replays the
/// records after a donor's core, copied after the block in progress.
/// Records are bounded by time, not bytes: each holds the entries of one
/// diff, up to every coefficient of the shape for one entry that changes
/// them all, and those of every entry within this window and ahead of it
/// stay held.
pub const RECORDS_KEPT: u64 = 48_000;

/// How far past its stamp a lease expiry from an older leader, which
/// stamps no index, takes effect: 64 ms at 48 kHz, at least the 50 ms
/// default `maxApplyLead`. A constant, never a replica's own timing, so
/// every replica derives at the same index; far enough ahead that the
/// record still reaches the engines before its apply deadline. The stamp
/// is a block start, so the index is one too for every block size that
/// divides 3072: all powers of two within the allowed 16..=1024, and 48,
/// 96, 192, 384 and 768. Contact lists, awaits and floors at this
/// index; the bridge rounds it up to a block for any other block size.
pub const FALLBACK_LEAD: u64 = 3_072;

/// The index the entry `request` takes effect at after entries that
/// left the floor at `floor`, and the floor after it: its own at_index,
/// or its stamp, a block start, for one further than [`AHEAD_MAX`] past
/// it, or [`FALLBACK_LEAD`] past its stamp for a lease expiry from an
/// older leader, which stamps none. A void from an older leader takes
/// effect nowhere, as that leader's binary applies it: the floor stays
/// alike on both. A function of the log prefix alone, so contact
/// computes at append the index the apply derives at.
pub fn floored(floor: u64, request: &Request) -> Option<u64> {
    let own = match (request.at_index(), &request.body) {
        (Some(at), _) if at <= request.stamp.saturating_add(AHEAD_MAX) => at,
        (Some(_), _) => request.stamp,
        (None, Body::LeaseExpiry { .. }) => request.stamp.saturating_add(FALLBACK_LEAD),
        (None, _) => return None,
    };
    Some(own.max(floor))
}

/// The engine entries one log entry derived, and where they apply.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The entry's index raised to [`StateMachine::floor`].
    pub at_index: u64,
    pub entries: Vec<samplez::Entry>,
    /// Whether it raises or re-routes an input: built a generation.
    pub generation: bool,
}

/// How the product applied a command or an expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect<C> {
    /// Changed, with what changed, which [`Product::delta`] gets back.
    Changed(C),
    /// Applied, changing nothing: the state already held it.
    Unchanged,
    Stale,
}

/// One change of an entry, as [`Product::delta`] gets it.
#[derive(Debug, PartialEq, Eq)]
pub struct Changed<'a, C> {
    /// The command that made it, `None` for a server-side change.
    pub command: Option<&'a Command>,
    /// What [`Effect::Changed`] carried.
    pub change: &'a C,
}

/// A product delta: one entry's changes at the product revision it makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub revision: u64,
    pub prev: u64,
    /// The ids of the commands whose changes it carries, in order.
    pub commands: Vec<String>,
    pub text: String,
}

/// How far below the highest stamping index applied an engine entry's
/// at_index lies before [`StateMachine::prune`] keeps only the latest per
/// target: past the longest a restamp can lie behind (three restamps of
/// at most a second each, the decision path's bound) at 48 kHz, and past
/// the retention a resent copy can still be stamped in.
pub const PASSED: u64 = 10 * 48_000;

/// What an engine entry sets: one input slot's state, its route, or the
/// master enable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Target {
    Slot(u32),
    Route(u32),
    /// A route of a slot the product does not name, by its encoding.
    Encoded(u64),
    Master,
}

/// The target of an engine body: a route by the slot the product names,
/// else by its encoding.
fn target<P: Product>(product: &P, body: &Body) -> Option<Target> {
    match body {
        Body::InputDown { slot, .. } | Body::InputUp { slot, .. } => Some(Target::Slot(*slot)),
        Body::Route { body, .. } => Some(match product.route_slot(body) {
            Some(slot) => Target::Route(slot),
            None => Target::Encoded(xxhash_rust::xxh3::xxh3_64(body)),
        }),
        Body::MasterEnable { .. } => Some(Target::Master),
        _ => None,
    }
}

/// Deltas held for clients that resume, the most recent first dropped
/// last.
const DELTAS_HELD: usize = 4096;

/// The `InputUp` that started a slot's last fade-in, as a
/// [`StateMachine`] tracks it: its log index, whether the slot was down
/// before it, and the fade-in recorded before it.
pub type Raise = (u64, bool, Option<u64>);

pub struct StateMachine<P> {
    pub product: P,
    pub dedup: Dedup,
    /// Entries the engine applies at their at_index, by log index: the
    /// latest per target ([`StateMachine::prune`]) and every one ahead.
    pub pending: BTreeMap<u64, Body>,
    /// The term and at_index of each entry with an at_index that can
    /// still be decided, by log index: a decision naming another term at
    /// that index names an entry this log does not hold.
    pub terms: BTreeMap<u64, (u64, u64)>,
    /// The restamps behind a restamped entry, by the log index it is
    /// known under now, with its at_index: a fourth is never appended.
    pub restamps: BTreeMap<u64, (u32, u64)>,
    /// The first decision applied on each (log index, term), by its own
    /// log index, with the at_index it is kept until: a later decision on
    /// the same id is a copy and changes nothing.
    pub decided: BTreeMap<(u64, u64), (u64, u64)>,
    pub baseline: Option<Baseline>,
    pub verdicts: BTreeMap<NodeId, bool>,
    /// The replica the latest `ClockVerdict` judging one named.
    pub latest_judged: Option<NodeId>,
    pub slot: Option<Disruption>,
    /// Each session's last renewal, as a stamping index.
    pub leases: BTreeMap<SessionKey, u64>,
    /// The sessions whose lifetime ended and that did not come back, with
    /// the stamping index of the expiry that ended it, until
    /// [`Product::forget`].
    pub ended: BTreeMap<SessionKey, u64>,
    /// The highest stamping index applied, which [`StateMachine::ended`]
    /// is counted against.
    pub highest_stamp: u64,
    limits: Limits,
    /// The product revision: the entries that changed the product, each
    /// one delta, so deltas number 1, 2, 3 without a gap. The revision
    /// [`samplez_service::ReplicatedState`] names; entries that do not
    /// change the product, such as lease renewals, membership changes, a
    /// new leader's blank entry and commands that change nothing, do not
    /// move it. Every replica applies the same log in order, so it is the
    /// same on each.
    pub revision: u64,
    /// Not replicated: the deltas since the last snapshot install or
    /// start, for [`samplez_service::ReplicatedState::deltas_since`].
    pub deltas: VecDeque<Change>,
    /// `deltas` holds every delta after this product revision.
    pub deltas_after: u64,
    /// The highest index any entry applied so far takes effect at: an
    /// entry's own index is raised to it, so the engine applies the
    /// entries in log order whatever their indexes, and each diff applies
    /// to the targets it was computed against.
    pub floor: u64,
    /// The decidable entries that derived engine entries, by the log index
    /// they are known under, with the input slots they re-routed: a
    /// restamp of one re-asserts the targets, of one that derived nothing
    /// nothing.
    pub derived: BTreeMap<u64, Vec<u32>>,
    /// Of `derived`, the entries whose records reset a meter's loudness
    /// measurement, with those meters: a restamp of one resets them again
    /// at its own index, which re-asserting the targets does not, as the
    /// entry's own record never applied; a void drops them.
    pub resets: BTreeMap<u64, Vec<u32>>,
    /// The highest [`Body::Protocol`] applied: the leader flags commands
    /// only once its own state holds [`crate::entry::PROTOCOL`].
    pub protocol: u32,
    /// Not replicated: the targets as last derived, a product that drives
    /// an engine only.
    image: Option<samplez::Targets>,
    /// Per coefficient the ramp the derived gain entries leave it on, as
    /// the engine renders it: an entry's own fade, or for a plain one a
    /// ramp over `ramp_samples` from the value held at its index, block
    /// aligned as the bridge applies it. A function of the log, carried
    /// by snapshots; finished ramps are pruned below the floor.
    ramps: Vec<Vec<Option<samplez::Fade>>>,
    /// Per input slot the ramps the derived tone entries leave its
    /// generator on, as the engine renders them: each entry's parameters
    /// as the bridge admits them, set at the block-aligned index the
    /// bridge applies it at, over `ramp_samples`. A function of the log,
    /// carried by snapshots; none for a slot no tone entry was derived
    /// for.
    tone_ramps: Vec<Option<samplez_dsp::ToneRamps>>,
    /// The input fade-ins and crossfades and the duck depth ramps and mask
    /// crossfades the derived entries leave running, as the engine renders
    /// them at the block-aligned index the bridge applies each entry at,
    /// over `ramp_samples`. A function of the log, carried by snapshots;
    /// what ended is pruned below the floor. Besides the snapshots, only
    /// [`StateMachine::product_state`] reads it, for an engine built from
    /// product state, which continues these ramps; the samples of an
    /// engine not built from it never depend on it.
    in_flight: samplez::InFlight,
    /// The input slots the derived entries hold down, a bit per slot: an
    /// `InputUp` fades in only a slot it raises from down.
    down: u64,
    /// Per input slot the `InputUp` that started its last fade-in: the
    /// log index it is known under, a restamp's once one moved it, and
    /// whether the slot was down and the fade-in recorded before it. A
    /// restamp moves the fade-in only of the entry that started it, and a
    /// void of that entry, which the engine never applies, puts both back.
    raised: Vec<Option<Raise>>,
    /// The duck masks of the state the replica opened or installed a
    /// snapshot at, which its engine is rebuilt from, and every one
    /// derived since, ORed together: the coefficients an engine may hold
    /// a mask other than 0 for, which a re-assertion clears. None for a
    /// product without targets, every coefficient then counting. Per
    /// replica, re-seeded from the image at open and at snapshot load:
    /// the restamp records of two replicas may differ by mask 0 entries
    /// one derives and the other does not. Mask 0 is always admitted and
    /// leaves a coefficient at 0 as it is, so their outputs agree; their
    /// records need not.
    masked: Option<Vec<Vec<u8>>>,
    /// How many target gains the last derived diff silenced
    /// ([`StateMachine::targets`]), for the log alone.
    silenced: usize,
    /// Not replicated: the records derived since start or load, by log
    /// index, kept [`RECORDS_KEPT`] past their index; shared, so a reader
    /// under the store lock copies none of a record's entries.
    pub records: BTreeMap<u64, std::sync::Arc<Record>>,
    /// Not replicated: `records` holds the record of every entry after
    /// this log index that derived one: the index a snapshot was loaded
    /// at, or the last whose record retention dropped. The floor makes
    /// the records' indexes rise in log order, so retention drops a
    /// prefix.
    pub records_from: u64,
    /// Not replicated: a log index with no entry after it up to
    /// `records_from` that derived a record, none where that is unknown,
    /// so a replay from it on misses no record
    /// ([`StateMachine::records_whole`]). A snapshot carries the newest
    /// record's index and a load takes it. A state saved past the index
    /// its snapshot is stored under can carry a floor above the
    /// `records_from` a load sets, which then adds nothing: a replay from
    /// at or above `records_from` is whole already.
    pub record_floor: Option<u64>,
}

impl<P> StateMachine<P> {
    /// Whether `records` holds the record of every entry after log index
    /// `after` that derived one: `after` is at or above `records_from`,
    /// or at or above `record_floor`, past which no entry derived one up
    /// to `records_from`.
    pub fn records_whole(&self, after: u64) -> bool {
        after >= self.records_from || self.record_floor.is_some_and(|f| after >= f)
    }
}

impl<P: Product> StateMachine<P> {
    pub fn new(product: P, limits: Limits) -> StateMachine<P> {
        StateMachine {
            product,
            dedup: Dedup::new(limits),
            pending: BTreeMap::new(),
            terms: BTreeMap::new(),
            restamps: BTreeMap::new(),
            decided: BTreeMap::new(),
            baseline: None,
            verdicts: BTreeMap::new(),
            latest_judged: None,
            slot: None,
            leases: BTreeMap::new(),
            ended: BTreeMap::new(),
            highest_stamp: 0,
            limits,
            revision: 0,
            deltas: VecDeque::new(),
            deltas_after: 0,
            floor: 0,
            derived: BTreeMap::new(),
            resets: BTreeMap::new(),
            protocol: 0,
            image: None,
            ramps: Vec::new(),
            tone_ramps: Vec::new(),
            in_flight: samplez::InFlight::default(),
            down: 0,
            raised: Vec::new(),
            masked: None,
            silenced: 0,
            records: BTreeMap::new(),
            records_from: 0,
            record_floor: Some(0),
        }
        .imaged()
    }

    /// Follows the ramps the entries of `entries`, applied at `at`, put
    /// the engine on, against `held`, the targets before them: each gain
    /// and its fade, joined ([`samplez::Fade::joined`]), then, as an
    /// engine applies them after a log entry's gains, each duck mask
    /// change and depth, and each crossfade of a re-route or detach. The
    /// value a coefficient has at `at` is the engine's own function of the
    /// ramp it was on, so a plain entry's ramp starts from the bits the
    /// engine's coefficient holds there; a depth likewise.
    fn follow(&mut self, entries: &[samplez::Entry], held: Option<&samplez::Targets>, at: u64) {
        // The index the bridge applies the entries at.
        let at = at.next_multiple_of(self.limits.block);
        let len = self.limits.ramp_samples;
        for e in entries {
            if let samplez::Entry::Tone {
                slot,
                frequency_hz,
                level_db,
                ident,
            } = *e
            {
                self.follow_tone(slot, frequency_hz, level_db, ident, at);
                continue;
            }
            let samplez::Entry::Gain {
                input,
                output,
                gain,
                fade,
            } = e
            else {
                continue;
            };
            if self.ramps.len() <= *input {
                self.ramps.resize(*input + 1, Vec::new());
            }
            let row = &mut self.ramps[*input];
            if row.len() <= *output {
                row.resize(*output + 1, None);
            }
            let old = held
                .and_then(|t| t.gains.get(*input))
                .and_then(|r| r.get(*output))
                .copied()
                .unwrap_or(0.0);
            let slot = &mut row[*output];
            let from = slot.map_or(old, |f| f.value_at(old, at));
            *slot = match fade {
                Some(f) => f.joined(*gain, from, at, len),
                None => (from != *gain && len != 0).then_some(samplez::Fade {
                    from,
                    start: at,
                    len,
                }),
            };
        }
        for e in entries {
            match e {
                &samplez::Entry::DuckMask {
                    input,
                    output,
                    mask,
                } => {
                    let cell = |t: &samplez::Targets| t.masks.get(input)?.get(output).copied();
                    let was = held.and_then(cell).unwrap_or(0);
                    // The coefficient's gain after the entries, and whether
                    // the engine holds a term for it to duck.
                    let gain = (entries.iter().rev())
                        .find_map(|e| match *e {
                            samplez::Entry::Gain {
                                input: i,
                                output: o,
                                gain,
                                ..
                            } if (i, o) == (input, output) => Some(gain),
                            _ => None,
                        })
                        .or_else(|| held?.gains.get(input)?.get(output).copied())
                        .unwrap_or(0.0);
                    let ramp = self.ramps.get(input).and_then(|r| r.get(output)).copied();
                    let term = gain != 0.0
                        || ramp
                            .flatten()
                            .is_some_and(|f| f.ramp_at(gain, at).is_some());
                    self.in_flight
                        .mask((input, output), (was, mask), at, len, term);
                }
                samplez::Entry::Duck {
                    group,
                    setting: setting @ samplez::DuckSetting::Depth { slot, .. },
                } => {
                    // As the bridge admits it: clamped, else 0 dB.
                    let db = match setting.clamped() {
                        Some(samplez::DuckSetting::Depth { db, .. }) => db,
                        _ => 0.0,
                    };
                    let (group, slot) = (*group, *slot);
                    let old = held
                        .and_then(|t| t.ducks.get(group)?.depths.get(slot).copied())
                        .unwrap_or(0.0);
                    let depths = &mut self.in_flight.depths;
                    if depths.len() <= group {
                        depths.resize(group + 1, Vec::new());
                    }
                    if depths[group].len() <= slot {
                        depths[group].resize(slot + 1, None);
                    }
                    let cell = &mut depths[group][slot];
                    let from = cell.map_or(old, |f| f.value_at(old, at));
                    *cell = (from != db && len != 0).then_some(samplez::Fade {
                        from,
                        start: at,
                        len,
                    });
                }
                samplez::Entry::Route { slot, .. } | samplez::Entry::Detach { slot } => {
                    // The flow the slot read; unknown without targets.
                    let Some(Some(was)) = held.map(|t| t.flows.get(*slot).cloned()) else {
                        continue;
                    };
                    let to = match e {
                        samplez::Entry::Route { flow_id, .. } => Some(flow_id),
                        _ => None,
                    };
                    if was.as_ref() != to {
                        let x = &mut self.in_flight.crossfades;
                        if x.len() <= *slot {
                            x.resize(*slot + 1, None);
                        }
                        x[*slot] = Some((at, was));
                    }
                }
                _ => {}
            }
        }
    }

    /// Follows the input states `entries` of the entry at log index
    /// `revision`, applied at `at`, set: an `InputUp` that raises a slot
    /// from down starts its fade-in there; one a restamp moved from log
    /// index `moved`, which applied nowhere before, also where the moved
    /// entry's own record started the slot's last fade-in.
    fn follow_inputs(
        &mut self,
        entries: &[samplez::Entry],
        at: u64,
        revision: u64,
        moved: Option<u64>,
    ) {
        let at = at.next_multiple_of(self.limits.block);
        for e in entries {
            match *e {
                samplez::Entry::InputDown { slot } => {
                    self.down |= 1u64.checked_shl(slot as u32).unwrap_or(0);
                }
                samplez::Entry::InputUp { slot } => {
                    let bit = 1u64.checked_shl(slot as u32).unwrap_or(0);
                    let fade_ins = &mut self.in_flight.fade_ins;
                    if fade_ins.len() <= slot {
                        fade_ins.resize(slot + 1, None);
                    }
                    if self.raised.len() <= slot {
                        self.raised.resize(slot + 1, None);
                    }
                    // Moved, the entry keeps what came before it.
                    let restarted = self.raised[slot].filter(|r| moved == Some(r.0));
                    if self.down & bit != 0 || restarted.is_some() {
                        let before = restarted
                            .map_or((self.down & bit != 0, fade_ins[slot]), |r| (r.1, r.2));
                        fade_ins[slot] = Some(at);
                        self.raised[slot] = Some((revision, before.0, before.1));
                    }
                    self.down &= !bit;
                }
                _ => {}
            }
        }
    }

    /// Follows a void of the `InputUp` for `slot` known under log index
    /// `of`: where it started the slot's last fade-in, the slot is down
    /// and its fade-in as they were before it, as on an engine, which
    /// never applies it.
    fn unraise(&mut self, slot: usize, of: u64) {
        let Some(Some((id, was_down, before))) = self.raised.get(slot).copied() else {
            return;
        };
        if id != of {
            return;
        }
        if was_down {
            self.down |= 1u64.checked_shl(slot as u32).unwrap_or(0);
        }
        if let Some(f) = self.in_flight.fade_ins.get_mut(slot) {
            *f = before;
        }
        self.raised[slot] = None;
    }

    /// Follows the ramps a tone entry for `slot`, applied at `at`, puts
    /// its generator on. The parameters are taken as the bridge admits
    /// them for a generator ([`samplez_dsp::ToneParams::admitted`]):
    /// clamped into their ranges, the default where a value is not
    /// finite.
    fn follow_tone(&mut self, slot: usize, frequency_hz: u32, level_db: f32, ident: bool, at: u64) {
        let p = samplez_dsp::ToneParams {
            frequency_hz,
            level_db,
            ident,
        }
        .admitted();
        if self.tone_ramps.len() <= slot {
            self.tone_ramps.resize(slot + 1, None);
        }
        let ramps = self.tone_ramps[slot].get_or_insert_with(Default::default);
        ramps.set(at, p, self.limits.ramp_samples);
    }

    /// The tone ramps a product state carries: those the derived entries
    /// left, empty where there are none.
    fn tone_ramps_of(&self) -> Vec<Option<samplez_dsp::ToneRamps>> {
        if self.tone_ramps.iter().all(Option::is_none) {
            return Vec::new();
        }
        self.tone_ramps.clone()
    }

    /// ORs the duck masks of `targets` into [`StateMachine::masked`].
    fn mask(&mut self, targets: &samplez::Targets) {
        let Some(masked) = self.masked.as_mut() else {
            return;
        };
        if masked.len() < targets.masks.len() {
            masked.resize(targets.masks.len(), Vec::new());
        }
        for (held, row) in masked.iter_mut().zip(&targets.masks) {
            if held.len() < row.len() {
                held.resize(row.len(), 0);
            }
            for (h, &k) in held.iter_mut().zip(row) {
                *h |= k;
            }
        }
    }

    /// With [`StateMachine::image`] taken from the product.
    fn imaged(mut self) -> StateMachine<P> {
        self.image = if P::DRIVES_ENGINE {
            self.targets().map(|(targets, silenced)| {
                self.silenced(None, silenced);
                targets
            })
        } else {
            None
        };
        // An engine is rebuilt from this state: its masks are these.
        self.masked = self.image.as_ref().map(|t| t.masks.clone());
        self
    }

    /// The product's targets as every replica derives from them, with how
    /// many gains [`samplez_dsp::valid_gain`] refuses it holds: each of
    /// those is 0.0. An engine refuses such a gain, so a live replica
    /// handed it would hold the cell's previous value while one rebuilt
    /// from the product state, which holds no previous value, silences it;
    /// silence is a function of the state alone, so the records, the
    /// image and [`StateMachine::product_state`] agree on every replica,
    /// rebuilt or not.
    fn targets(&self) -> Option<(samplez::Targets, usize)> {
        let mut targets = self.product.targets()?;
        let mut silenced = 0;
        for gain in targets.gains.iter_mut().flatten() {
            if !samplez_dsp::valid_gain(*gain) {
                *gain = 0.0;
                silenced += 1;
            }
        }
        Some((targets, silenced))
    }

    /// Logs `silenced`, the count of target gains [`StateMachine::targets`]
    /// silences, once per change, not per entry while the product keeps
    /// such a gain: as the entry at `revision` derives, or as the state is
    /// opened or loaded from a snapshot, with none.
    fn silenced(&mut self, revision: Option<u64>, silenced: usize) {
        if silenced == self.silenced {
            return;
        }
        self.silenced = silenced;
        if silenced > 0 {
            tracing::warn!(
                ?revision,
                silenced,
                "the product's targets hold gains an engine refuses: derived as silence"
            );
        } else {
            tracing::info!(
                ?revision,
                "the product's targets hold no gain an engine refuses any more"
            );
        }
    }

    /// Derives the engine entries of the entry at log index `revision`,
    /// applied at `at`: the diff of the targets when it `changed` the
    /// product, every target and the routes of the entry it names for a
    /// restamp or void of one that derived some, an input's state for an
    /// `InputDown` or `InputUp` and for a restamp of one.
    /// The input states derive for every product, since the engine's
    /// input health writes them whatever drives the rest; the others only
    /// for one that [`Product::DRIVES_ENGINE`].
    fn derive(
        &mut self,
        revision: u64,
        request: &Request,
        at: u64,
        changed: bool,
        restamped: Option<Vec<u32>>,
    ) {
        let (entries, routed) = match (&request.body, restamped) {
            (Body::Restamp { .. } | Body::Void { .. }, Some(slots)) => {
                let Some((targets, _)) = self.targets() else {
                    return;
                };
                let slots: Vec<usize> = slots.iter().map(|&s| s as usize).collect();
                let mut entries = targets.assert(&slots, self.masked.as_deref());
                // A restamp carries the resets of the entry it moved; a
                // void has dropped them.
                for &meter in self.resets.get(&revision).into_iter().flatten() {
                    let meter = meter as usize;
                    entries.push(samplez::Entry::ResetLoudness { meter });
                }
                let held = self.image.take();
                self.follow(&entries, held.as_ref(), at);
                self.mask(&targets);
                self.image = Some(targets);
                (entries, Vec::new())
            }
            // A restamped input state: the body moved to the restamp's
            // index.
            (Body::Restamp { .. }, None) => match self.pending.get(&revision) {
                Some(body @ (Body::InputDown { .. } | Body::InputUp { .. })) => {
                    (input_entries(body), Vec::new())
                }
                _ => return,
            },
            (Body::InputDown { slot, .. }, _) => (
                vec![samplez::Entry::InputDown {
                    slot: *slot as usize,
                }],
                Vec::new(),
            ),
            (Body::InputUp { slot, .. }, _) => (
                vec![samplez::Entry::InputUp {
                    slot: *slot as usize,
                }],
                Vec::new(),
            ),
            (Body::Commands(_) | Body::LeaseExpiry { .. }, _) if changed && P::DRIVES_ENGINE => {
                let Some((next, silenced)) = self.targets() else {
                    return;
                };
                self.silenced(Some(revision), silenced);
                let entries = self.image.as_ref().map_or_else(
                    || samplez::Targets::default().diff(&next),
                    |image| image.diff(&next),
                );
                let held = self.image.take();
                self.follow(&entries, held.as_ref(), at);
                self.mask(&next);
                self.image = Some(next);
                let routed = entries
                    .iter()
                    .filter_map(|e| match e {
                        samplez::Entry::Route { slot, .. } | samplez::Entry::Detach { slot } => {
                            Some(*slot as u32)
                        }
                        _ => None,
                    })
                    .collect();
                (entries, routed)
            }
            _ => return,
        };
        if entries.is_empty() {
            return;
        }
        let moved = match &request.body {
            Body::Restamp { revision: of, .. } => Some(*of),
            _ => None,
        };
        self.follow_inputs(&entries, at, revision, moved);
        let generation = entries.iter().any(|e| {
            matches!(
                e,
                samplez::Entry::InputUp { .. }
                    | samplez::Entry::Route { .. }
                    | samplez::Entry::Detach { .. }
            )
        });
        // A lease expiry is decided like a command once stamped.
        if matches!(request.body, Body::Commands(_) | Body::LeaseExpiry { .. })
            && self.terms.contains_key(&revision)
        {
            self.derived.insert(revision, routed);
            let resets: Vec<u32> = (entries.iter())
                .filter_map(|e| match *e {
                    samplez::Entry::ResetLoudness { meter } => Some(meter as u32),
                    _ => None,
                })
                .collect();
            if !resets.is_empty() {
                self.resets.insert(revision, resets);
            }
        }
        let record = Record {
            at_index: at,
            entries,
            generation,
        };
        self.records.insert(revision, std::sync::Arc::new(record));
    }

    /// Applies the request of the entry at log index `revision`, of term
    /// 0.
    pub fn apply(&mut self, revision: u64, request: &Request) -> Response {
        self.apply_at(revision, 0, request)
    }

    /// Applies the request of the entry at log index `revision` of term
    /// `term`.
    pub fn apply_at(&mut self, revision: u64, term: u64, request: &Request) -> Response {
        let stamp = request.stamp;
        self.forget_ended(stamp);
        let at = floored(self.floor, request);
        if let Some(at) = at {
            self.floor = at;
        }
        // A restamp or void of an entry held that derived engine entries:
        // the slots it re-routed, read before the decision moves it.
        let restamped = match request.body.decides() {
            Some((of, of_term)) if self.terms.get(&of).is_some_and(|&(t, _)| t == of_term) => {
                self.derived.get(&of).cloned()
            }
            _ => None,
        };
        let revision_before = self.revision;
        if let Some(at) = request.at_index()
            && request.body.decides().is_none()
        {
            self.terms.insert(revision, (term, at));
        }
        if let Some(named) = request.body.decides() {
            let until = self
                .terms
                .get(&named.0)
                .map_or(self.highest_stamp, |&(_, at)| at)
                .max(request.at_index().unwrap_or(0));
            self.decided.entry(named).or_insert((revision, until));
        }
        let response = self.apply_body(revision, term, request, at);
        if let Some(at) = at {
            let changed = self.revision != revision_before;
            self.derive(revision, request, at, changed, restamped);
        }
        self.prune();
        response
    }

    fn apply_body(
        &mut self,
        revision: u64,
        term: u64,
        request: &Request,
        at: Option<u64>,
    ) -> Response {
        let stamp = request.stamp;
        match &request.body {
            Body::Commands(commands) => self.commands(stamp, at, commands),
            Body::Baseline(baseline) => {
                self.baseline = Some(baseline.clone());
                Response::default()
            }
            Body::Restamp {
                revision: of,
                term: of_term,
                at_index,
                ..
            } => {
                // The entry is known under the restamp's id from here on.
                if self.terms.get(of).is_some_and(|&(t, _)| t == *of_term) {
                    self.terms.remove(of);
                    if let Some(mut body) = self.pending.remove(of) {
                        restamp(&mut body, *at_index);
                        self.pending.insert(revision, body);
                    }
                    let behind = self.restamps.remove(of).map_or(0, |(n, _)| n);
                    self.restamps.insert(revision, (behind + 1, *at_index));
                    self.terms.insert(revision, (term, *at_index));
                    if let Some(routed) = self.derived.remove(of) {
                        self.derived.insert(revision, routed);
                    }
                    if let Some(resets) = self.resets.remove(of) {
                        self.resets.insert(revision, resets);
                    }
                }
                Response::default()
            }
            Body::Void {
                revision: of,
                term: of_term,
                at_index,
                ..
            } => {
                if self.terms.get(of).is_some_and(|&(t, _)| t == *of_term) {
                    if let Some(Body::InputUp { slot, .. }) = self.pending.remove(of) {
                        self.unraise(slot as usize, *of);
                    }
                    let behind = self.restamps.remove(of);
                    self.terms.remove(of);
                    let derived = self.derived.remove(of);
                    // Its resets never apply: the state re-asserted holds
                    // no event.
                    self.resets.remove(of);
                    // A void the leader stamped re-asserts the entry's
                    // effect at its own index: one that goes unresolved is
                    // decided like the entry, known under the void's id,
                    // the restamps behind the entry counting.
                    // A void of an engine body drops it: nothing to
                    // re-assert, so final. So is one with the most
                    // restamps behind it already.
                    let late = behind.map_or(0, |(n, _)| n);
                    if let Some(at) = *at_index
                        && derived.is_some()
                        && late < crate::decide::RESTAMPS_MAX
                    {
                        self.terms.insert(revision, (term, at));
                        if let Some((behind, _)) = behind {
                            self.restamps.insert(revision, (behind, at));
                        }
                        if let Some(derived) = derived {
                            self.derived.insert(revision, derived);
                        }
                    }
                }
                Response::default()
            }
            Body::Protocol(protocol) => {
                self.protocol = self.protocol.max(*protocol);
                Response::default()
            }
            // Acted on by the replica it names.
            Body::PurgePoint { .. } => Response::default(),
            Body::ClockVerdict { replica, judged } => {
                self.verdicts.insert(*replica, *judged);
                if *judged {
                    self.latest_judged = Some(*replica);
                }
                Response::default()
            }
            // Acted on by the replica it names, as its commit becomes
            // known; nothing in the state follows from it.
            Body::Diverged { .. } => Response::default(),
            Body::Disrupt(slot) => {
                self.slot = slot.clone();
                Response::default()
            }
            Body::LeaseRenew(sessions) => {
                for session in sessions {
                    self.renew(session, stamp);
                }
                Response::default()
            }
            Body::LeaseExpiry {
                session, renewed, ..
            } => {
                // A renewal after the one the expiry was decided on keeps
                // the lease: the expiry is a no-op.
                if self.leases.get(session).is_some_and(|at| at <= renewed) {
                    self.leases.remove(session);
                    self.ended.insert(session.clone(), stamp);
                    if let Effect::Changed(change) = self.product.expire(session) {
                        let changed = Changed {
                            command: None,
                            change: &change,
                        };
                        let text = self.product.delta(stamp, &[changed]);
                        self.push(Vec::new(), text);
                    }
                }
                Response::default()
            }
            body @ (Body::InputDown { .. }
            | Body::InputUp { .. }
            | Body::Route { .. }
            | Body::MasterEnable { .. }) => {
                self.pending.insert(revision, body.clone());
                Response::default()
            }
        }
    }

    /// Bounds [`StateMachine::pending`] and [`StateMachine::restamps`]:
    /// an entry whose index lies more than [`PASSED`] below the highest
    /// stamping index applied took effect on every engine or was decided,
    /// so of those only the latest per target stays, which a rebuilt
    /// engine starts from. Log entries carry absolute targets, so an
    /// older one on the same target adds nothing. A restamp count goes
    /// with its index passing too: the entry can be late no more.
    fn prune(&mut self) {
        let passed = self.highest_stamp.saturating_sub(PASSED);
        let mut latest: BTreeMap<Target, u64> = BTreeMap::new();
        let mut gone = Vec::new();
        for (&index, body) in &self.pending {
            let (Some(at), Some(target)) = (body.at_index(), target(&self.product, body)) else {
                continue;
            };
            if at >= passed {
                continue;
            }
            if let Some(older) = latest.insert(target, index) {
                gone.push(older);
            }
        }
        for index in gone {
            self.pending.remove(&index);
            self.terms.remove(&index);
        }
        let done: Vec<u64> = self
            .restamps
            .iter()
            .filter(|&(_, &(_, at))| at < passed)
            .map(|(&index, _)| index)
            .collect();
        for index in done {
            self.restamps.remove(&index);
        }
        let pending = &self.pending;
        self.terms
            .retain(|index, &mut (_, at)| at >= passed || pending.contains_key(index));
        self.decided.retain(|_, &mut (_, until)| until >= passed);
        let terms = &self.terms;
        self.derived.retain(|index, _| terms.contains_key(index));
        self.resets.retain(|index, _| terms.contains_key(index));
        let floor = self.floor;
        self.in_flight.prune(floor, self.limits.ramp_samples);
        for f in self.ramps.iter_mut().flatten() {
            if f.is_some_and(|f| f.start.saturating_add(u64::from(f.len)) <= floor) {
                *f = None;
            }
        }
        let kept = self.highest_stamp.saturating_sub(RECORDS_KEPT);
        while let Some(entry) = self.records.first_entry() {
            if entry.get().at_index >= kept {
                break;
            }
            // Every record held lies after `records_from`: the one dropped
            // is the newest at or below it now.
            self.records_from = self.records_from.max(*entry.key());
            self.record_floor = Some(self.records_from);
            entry.remove();
        }
    }

    /// Applies every command of an entry, then makes its changes one delta
    /// at the next product revision. Each command resolves at the revision
    /// after the entry: the entry's when it changed the product, the one
    /// before when it did not.
    fn commands(&mut self, stamp: u64, at: Option<u64>, commands: &[Command]) -> Response {
        // Recorded with this placeholder until the entry's revision is
        // known; a copy later in the same entry reads it from the table.
        const ENTRY: u64 = u64::MAX;
        let mut outcomes = Vec::with_capacity(commands.len());
        let mut recorded = Vec::new();
        let mut changes = Vec::new();
        for (n, command) in commands.iter().enumerate() {
            let session = command.origin.key();
            // A command renews its session's lease.
            self.renew(&session, stamp);
            if self
                .dedup
                .record(&session, &command.id, ENTRY, stamp)
                .is_some()
            {
                let (revision, result) = self.dedup.answer(&session, &command.id).expect("held");
                outcomes.push(Outcome { revision, result });
                continue;
            }
            let ctx = Apply {
                generation: command.generation,
                at_index: at,
            };
            let result = match self.product.apply(&command.origin, &command.body, &ctx) {
                Effect::Changed(change) => {
                    changes.push((n, change));
                    Resolution::Applied
                }
                Effect::Unchanged => Resolution::Unchanged,
                Effect::Stale => Resolution::Stale,
            };
            self.dedup.resolve(&session, &command.id, result, ENTRY);
            recorded.push((session, n, result));
            outcomes.push(Outcome {
                revision: ENTRY,
                result,
            });
        }
        if !changes.is_empty() {
            let changed: Vec<Changed<'_, P::Change>> = changes
                .iter()
                .map(|(n, change)| Changed {
                    command: Some(&commands[*n]),
                    change,
                })
                .collect();
            let text = self.product.delta(stamp, &changed);
            let ids = changes
                .iter()
                .map(|(n, _)| commands[*n].id.clone())
                .collect();
            self.push(ids, text);
        }
        for (session, n, result) in recorded {
            self.dedup
                .resolve(&session, &commands[n].id, result, self.revision);
        }
        for outcome in &mut outcomes {
            if outcome.revision == ENTRY {
                outcome.revision = self.revision;
            }
        }
        Response(outcomes)
    }

    /// Calls [`Product::forget`] for every session whose lifetime ended at
    /// least the dedup retention plus the failover bound before `stamp`,
    /// the highest stamping index applied counting this entry's.
    fn forget_ended(&mut self, stamp: u64) {
        self.highest_stamp = self.highest_stamp.max(stamp);
        let keep = self.limits.retention + self.limits.failover;
        let gone: Vec<SessionKey> = self
            .ended
            .iter()
            .filter(|&(_, &ended)| ended.saturating_add(keep) <= self.highest_stamp)
            .map(|(session, _)| session.clone())
            .collect();
        for session in gone {
            self.ended.remove(&session);
            self.product.forget(&session);
        }
    }

    /// Makes `text` the delta at the next product revision.
    fn push(&mut self, commands: Vec<String>, text: String) {
        if self.deltas.len() == DELTAS_HELD
            && let Some(dropped) = self.deltas.pop_front()
        {
            self.deltas_after = dropped.revision;
        }
        self.revision += 1;
        self.deltas.push_back(Change {
            revision: self.revision,
            prev: self.revision - 1,
            commands,
            text,
        });
    }

    /// Renews `session`'s lease at `stamp`, keeping a later renewal: a
    /// leader after a failover can stamp lower than the one before, and
    /// an expiry decided on the higher renewal must not end a live lease.
    fn renew(&mut self, session: &SessionKey, stamp: u64) {
        // Back: a new lifetime, whose ordering state the product keeps.
        self.ended.remove(session);
        let renewed = self.leases.entry(session.clone()).or_insert(stamp);
        *renewed = (*renewed).max(stamp);
    }

    /// The highest at_index of an applied entry that built a generation
    /// on every replica: a decidable one that re-routed an input, or an
    /// `InputUp` held. The one source of the pending generation of the
    /// applied log, so a command the product refused waits for nothing,
    /// live or after a restart.
    pub fn pending_generation(&self) -> Option<u64> {
        let rerouted = (self.derived.iter())
            .filter(|(_, slots)| !slots.is_empty())
            .filter_map(|(index, _)| self.terms.get(index).map(|&(_, at)| at));
        let raised = self.pending.values().filter_map(|body| match body {
            Body::InputUp { at_index, .. } => Some(*at_index),
            _ => None,
        });
        rerouted.chain(raised).max()
    }

    /// The ramps a product state carries: those the derived entries left
    /// running, over the product's own fades. Shaped as `gains` whenever
    /// any is set, as an install requires, and empty otherwise.
    fn fading_of(&self, targets: &samplez::Targets) -> Vec<Vec<Option<samplez::Fade>>> {
        let mut fading = targets.fading.clone();
        let running =
            |i: usize, o: usize| self.ramps.get(i).and_then(|r| r.get(o)).copied().flatten();
        let any = fading.iter().flatten().any(Option::is_some)
            || (targets.gains.iter().enumerate())
                .any(|(i, row)| (0..row.len()).any(|o| running(i, o).is_some()));
        if !any {
            return Vec::new();
        }
        fading.resize(targets.gains.len(), Vec::new());
        for (i, row) in targets.gains.iter().enumerate() {
            fading[i].resize(row.len(), None);
            for (o, cell) in fading[i].iter_mut().enumerate() {
                if let Some(f) = running(i, o) {
                    *cell = Some(f);
                }
            }
        }
        fading
    }

    /// The engine state of the applied log, the entry at log index `n`
    /// last, that an engine core is built from: the targets, and each
    /// slot up as its latest `InputDown` or `InputUp` held says, up where
    /// none is held. Every record up to `n` takes effect at or before
    /// [`StateMachine::floor`], every later one at or after it, so a core
    /// installed with it there and given the records after `n` holds what
    /// one given every record holds. Empty targets for a product without.
    pub fn product_state(&self, n: u64) -> samplez::ProductState {
        let targets = self.targets().map_or_else(Default::default, |(t, _)| t);
        let mut up = vec![true; targets.flows.len()];
        for body in self.pending.values() {
            if let Body::InputDown { slot, .. } | Body::InputUp { slot, .. } = body
                && let Some(u) = up.get_mut(*slot as usize)
            {
                *u = matches!(body, Body::InputUp { .. });
            }
        }
        let fading = self.fading_of(&targets);
        let tone_ramps = self.tone_ramps_of();
        let in_flight = self.in_flight.clone();
        samplez::ProductState {
            limiters: targets.limiters,
            fading,
            gains: targets.gains,
            up,
            flows: targets.flows,
            outputs: targets.outputs,
            loudness_running: targets.loudness_running,
            strips: targets.strips,
            delays: targets.delays,
            tones: targets.tones,
            tone_ramps,
            ducks: targets.ducks,
            masks: targets.masks,
            in_flight,
            pending: Vec::new(),
            last_id: n,
        }
    }

    pub fn save(&self) -> Replicated {
        Replicated {
            product: self.product.save(),
            pending: self.pending.clone(),
            terms: self.terms.clone(),
            restamps: self.restamps.clone(),
            decided: self.decided.iter().map(|(k, v)| (*k, *v)).collect(),
            baseline: self.baseline.clone(),
            verdicts: self.verdicts.clone(),
            latest_judged: self.latest_judged,
            slot: self.slot.clone(),
            dedup: self.dedup.save(),
            leases: self.leases.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            ended: self.ended.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            highest_stamp: self.highest_stamp,
            revision: self.revision,
            floor: Some(self.floor),
            record_floor: self
                .records
                .keys()
                .next_back()
                .copied()
                .or(self.record_floor),
            derived: self.derived.clone(),
            resets: self.resets.clone(),
            protocol: self.protocol,
            ramps: (self.ramps.iter())
                .map(|row| {
                    (row.iter())
                        .map(|f| f.map(|f| (f.from.to_bits(), f.start, f.len)))
                        .collect()
                })
                .collect(),
            tone_ramps: (self.tone_ramps.iter())
                .map(|r| r.map(Into::into))
                .collect(),
            in_flight: {
                let f = &self.in_flight;
                InFlightBits {
                    down: self.down,
                    raised: self.raised.clone(),
                    fade_ins: f.fade_ins.clone(),
                    crossfades: f.crossfades.clone(),
                    depths: (f.depths.iter())
                        .map(|g| {
                            (g.iter())
                                .map(|d| d.map(|d| (d.from.to_bits(), d.start, d.len)))
                                .collect()
                        })
                        .collect(),
                    masks: f.masks.clone(),
                }
            },
        }
    }

    /// Replaces the state with a snapshot's.
    pub fn load(&mut self, saved: Replicated, limits: Limits) -> Result<(), String> {
        self.product.load(&saved.product)?;
        self.dedup = Dedup::load(limits, saved.dedup);
        self.pending = saved.pending;
        self.terms = saved.terms;
        self.restamps = saved.restamps;
        self.decided = saved.decided.into_iter().collect();
        self.baseline = saved.baseline;
        self.verdicts = saved.verdicts;
        self.latest_judged = saved.latest_judged;
        self.slot = saved.slot;
        self.leases = saved.leases.into_iter().collect();
        self.ended = saved.ended.into_iter().collect();
        self.highest_stamp = saved.highest_stamp;
        self.limits = limits;
        self.revision = saved.revision;
        self.deltas.clear();
        self.deltas_after = saved.revision;
        // A snapshot written before the floor was kept: the highest index
        // an entry still decidable takes effect at, the same on every
        // replica that loads it. One further than [`AHEAD_MAX`] past the
        // highest stamp is a far client index an older writer kept, which
        // a replica applying that entry counts as its stamp. One within
        // that bound but further than [`AHEAD_MAX`] past its own entry's
        // stamp is kept here and counted as its stamp there, so the two
        // floors can differ by up to [`AHEAD_MAX`].
        let highest_stamp = self.highest_stamp;
        self.floor = saved.floor.unwrap_or_else(|| {
            self.terms
                .values()
                .map(|&(_, at)| at)
                .filter(|&at| at <= highest_stamp.saturating_add(AHEAD_MAX))
                .max()
                .unwrap_or(0)
        });
        self.derived = saved.derived;
        self.resets = saved.resets;
        self.protocol = saved.protocol;
        // Ramps no generator holds are left out, as an install would
        // refuse them.
        self.tone_ramps = (saved.tone_ramps.iter())
            .map(|r| r.map(samplez_dsp::ToneRamps::from).filter(|r| r.is_valid()))
            .collect();
        let f = &saved.in_flight;
        self.down = f.down;
        self.raised = f.raised.clone();
        self.in_flight = samplez::InFlight {
            fade_ins: f.fade_ins.clone(),
            crossfades: f.crossfades.clone(),
            depths: (f.depths.iter())
                .map(|g| {
                    (g.iter())
                        .map(|d| {
                            d.map(|(from, start, len)| samplez::Fade {
                                from: f32::from_bits(from),
                                start,
                                len,
                            })
                        })
                        .collect()
                })
                .collect(),
            masks: f.masks.clone(),
        };
        self.ramps = (saved.ramps.iter())
            .map(|row| {
                (row.iter())
                    .map(|f| {
                        f.map(|(from, start, len)| samplez::Fade {
                            from: f32::from_bits(from),
                            start,
                            len,
                        })
                    })
                    .collect()
            })
            .collect();
        self.records.clear();
        // The newest record when the snapshot was taken, at or below the
        // index it was taken at, which the store sets `records_from` to.
        self.record_floor = saved.record_floor;
        self.image = if P::DRIVES_ENGINE {
            self.targets().map(|(targets, silenced)| {
                self.silenced(None, silenced);
                targets
            })
        } else {
            None
        };
        // An engine is rebuilt from this state: its masks are these.
        self.masked = self.image.as_ref().map(|t| t.masks.clone());
        Ok(())
    }
}

/// The engine entry of an `InputDown` or `InputUp`.
fn input_entries(body: &Body) -> Vec<samplez::Entry> {
    match *body {
        Body::InputDown { slot, .. } => vec![samplez::Entry::InputDown {
            slot: slot as usize,
        }],
        Body::InputUp { slot, .. } => vec![samplez::Entry::InputUp {
            slot: slot as usize,
        }],
        _ => Vec::new(),
    }
}

pub(crate) fn restamp(body: &mut Body, to: u64) {
    match body {
        Body::InputDown { at_index, .. }
        | Body::InputUp { at_index, .. }
        | Body::Route { at_index, .. }
        | Body::MasterEnable { at_index, .. }
        | Body::Restamp { at_index, .. } => *at_index = to,
        Body::Void {
            at_index: Some(at_index),
            ..
        }
        | Body::LeaseExpiry {
            at_index: Some(at_index),
            ..
        } => *at_index = to,
        Body::Commands(commands) => {
            for c in commands {
                c.at_index = Some(to);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::entry::Batch;
    use samplez_service::Role;
    use std::collections::BTreeSet;

    /// One value per path, set by `path=value@seq`; an older seq on a path
    /// is stale.
    /// One value per path, set by `path=value@seq`: a seq not above the
    /// last applied on its path is stale, the value it holds already is
    /// unchanged. A session that changed something holds it until its
    /// lease expires, as touch ownership.
    #[derive(Default)]
    pub struct Paths {
        pub values: BTreeMap<String, (String, u64)>,
        pub applied: u64,
        pub holding: BTreeSet<SessionKey>,
        pub expired: Vec<SessionKey>,
        pub forgotten: Vec<SessionKey>,
        /// Each [`Product::delta`] call's stamp and its changes' at_index.
        pub delta_calls: std::sync::Mutex<Vec<(u64, Vec<Option<u64>>)>>,
        /// The configuration it was built with; not saved.
        pub config: u64,
        /// Each command's [`Apply::at_index`], in apply order.
        pub at: Vec<Option<u64>>,
    }

    pub fn parse(command: &[u8]) -> (String, String, u64) {
        let text = std::str::from_utf8(command).expect("utf-8");
        let (path, rest) = text.split_once('=').expect("path=value@seq");
        let (value, seq) = rest.split_once('@').expect("value@seq");
        (path.into(), value.into(), seq.parse().expect("seq"))
    }

    impl Product for Paths {
        const DERIVATION_VERSION: u32 = 1;
        type Change = (String, String);

        fn apply(
            &mut self,
            origin: &Origin,
            command: &[u8],
            ctx: &Apply,
        ) -> Effect<(String, String)> {
            self.at.push(ctx.at_index);
            let (path, value, seq) = parse(command);
            let held = self.values.get(&path).cloned();
            if held.as_ref().is_some_and(|(_, last)| *last >= seq) {
                return Effect::Stale;
            }
            self.applied += 1;
            self.values.insert(path.clone(), (value.clone(), seq));
            if held.is_some_and(|(was, _)| was == value) {
                return Effect::Unchanged;
            }
            self.holding.insert(origin.key());
            Effect::Changed((path, value))
        }
        fn expire(&mut self, session: &SessionKey) -> Effect<(String, String)> {
            if !self.holding.remove(session) {
                return Effect::Unchanged;
            }
            self.expired.push(session.clone());
            Effect::Changed(("owner".into(), self.expired.len().to_string()))
        }
        fn forget(&mut self, session: &SessionKey) {
            self.forgotten.push(session.clone());
        }
        fn delta(&self, stamp: u64, changes: &[Changed<'_, (String, String)>]) -> String {
            let at = changes
                .iter()
                .map(|c| c.command.and_then(|c| c.at_index))
                .collect();
            self.delta_calls.lock().unwrap().push((stamp, at));
            // Later wins per path; every command that changed something is an
            // origin, a server-side change none.
            let mut values = BTreeMap::new();
            let mut origins = Vec::new();
            for changed in changes {
                let (path, value) = changed.change;
                values.insert(path, value);
                if let Some(command) = changed.command {
                    origins.push(format!("{}/{}", command.origin.session, command.id));
                }
            }
            let values: Vec<String> = values.iter().map(|(p, v)| format!("{p}={v}")).collect();
            format!("{} from {}", values.join(","), origins.join(","))
        }
        fn text(&self) -> String {
            serde_json::to_string(&self.values).expect("encodes")
        }
        fn config_hash(&self) -> u64 {
            self.config
        }
        fn save(&self) -> Vec<u8> {
            serde_json::to_vec(&(&self.values, self.applied)).expect("encodes")
        }
        fn load(&mut self, saved: &[u8]) -> Result<(), String> {
            (self.values, self.applied) =
                serde_json::from_slice(saved).map_err(|e| e.to_string())?;
            Ok(())
        }
    }

    pub fn origin(session: &str) -> Origin {
        Origin {
            principal: "anonymous".into(),
            session: session.into(),
            role: Role::Operator,
            connection: "r0:n:1".into(),
        }
    }

    pub fn command(session: &str, id: &str, body: &str) -> Command {
        Command {
            generation: None,
            origin: origin(session),
            id: id.into(),
            at_index: None,
            body: body.as_bytes().to_vec(),
        }
    }

    fn machine() -> StateMachine<Paths> {
        StateMachine::new(Paths::default(), Limits::defaults(48_000))
    }

    /// An input state derives its engine entry for a product that drives
    /// no engine too: the engine's input health writes it whatever
    /// drives the rest.
    #[test]
    fn an_input_state_derives_for_a_product_without_targets() {
        let mut sm = machine();
        let down = Request {
            stamp: 0,
            body: Body::InputDown {
                slot: 0,
                at_index: 960,
            },
        };
        sm.apply(1, &down);
        assert_eq!(
            *sm.records[&1],
            Record {
                at_index: 960,
                entries: vec![samplez::Entry::InputDown { slot: 0 }],
                generation: false,
            }
        );
    }

    fn commands(stamp: u64, commands: Vec<Command>) -> Request {
        Request {
            stamp,
            body: Body::Commands(commands),
        }
    }

    /// The deltas the machine holds, as (revision, prev, commands, text).
    fn deltas(sm: &StateMachine<Paths>) -> Vec<(u64, u64, Vec<String>, String)> {
        sm.deltas
            .iter()
            .map(|c| (c.revision, c.prev, c.commands.clone(), c.text.clone()))
            .collect()
    }

    /// A command sees the index its entry takes effect at: the earliest
    /// of its commands' at_index, raised to the floor an earlier entry
    /// left, none for an entry with none. Two machines applying one log
    /// see the same.
    #[test]
    fn a_command_sees_the_index_its_entry_takes_effect_at() {
        let at = |cmd: Command, at_index: Option<u64>| Command { at_index, ..cmd };
        let log = [
            commands(
                960,
                vec![
                    at(command("a", "1", "/x=1@1"), Some(2_000)),
                    at(command("a", "2", "/y=1@1"), Some(1_500)),
                ],
            ),
            commands(1_920, vec![at(command("a", "3", "/x=2@2"), Some(1_000))]),
            commands(2_880, vec![command("a", "4", "/y=2@2")]),
        ];
        let seen = |_: ()| {
            let mut sm = machine();
            for (n, request) in log.iter().enumerate() {
                sm.apply(n as u64 + 1, request);
            }
            sm.product.at.clone()
        };
        let one = seen(());
        assert_eq!(one, [Some(1_500), Some(1_500), Some(1_500), None]);
        assert_eq!(seen(()), one, "another machine on the same log");
    }

    /// A machine loaded from a save taken mid-log sees, for every later
    /// command, the index a machine that never saved sees: the floor the
    /// entries before the save left goes with it.
    #[test]
    fn a_loaded_machine_sees_the_indexes_one_that_never_saved_sees() {
        let at = |cmd: Command, at_index: Option<u64>| Command { at_index, ..cmd };
        let log = [
            commands(960, vec![at(command("a", "1", "/x=1@1"), Some(3_000))]),
            commands(1_920, vec![at(command("a", "2", "/y=1@1"), Some(1_000))]),
            commands(2_880, vec![at(command("a", "3", "/x=2@2"), Some(4_000))]),
            commands(3_840, vec![command("a", "4", "/y=2@2")]),
        ];
        let mut whole = machine();
        for (n, request) in log.iter().enumerate() {
            whole.apply(n as u64 + 1, request);
        }
        let mut before = machine();
        before.apply(1, &log[0]);
        let mut loaded = machine();
        loaded
            .load(before.save(), Limits::defaults(48_000))
            .expect("loads");
        for (n, request) in log.iter().enumerate().skip(1) {
            loaded.apply(n as u64 + 1, request);
        }
        assert_eq!(loaded.product.at, whole.product.at[1..]);
        assert_eq!(loaded.product.at, [Some(3_000), Some(4_000), None]);
    }

    /// One entry is one delta at one revision: every command applied in
    /// order, two writes of one value listed with the later only, and
    /// every command that changed something named; each resolves at the
    /// entry's revision.
    #[test]
    fn an_entry_of_three_changes_is_one_delta_at_the_next_revision() {
        let mut sm = machine();
        let batch = vec![
            command("a", "1", "gain=1@1"),
            command("b", "2", "pan=1@1"),
            command("c", "3", "gain=2@2"),
        ];
        let response = sm.apply(7, &commands(48, batch));
        assert_eq!(sm.revision, 1);
        assert_eq!(
            deltas(&sm),
            [(
                1,
                0,
                vec!["1".into(), "2".into(), "3".into()],
                "gain=2,pan=1 from a/1,b/2,c/3".into()
            )]
        );
        assert_eq!(sm.product.applied, 3, "no command coalesced away");
        let outcomes: Vec<_> = response.0.iter().map(|o| (o.revision, o.result)).collect();
        assert_eq!(outcomes, [(1, Resolution::Applied); 3]);
        assert_eq!(sm.dedup.revision(&origin("c").key(), "3"), Some(1));
    }

    /// The product gets the entry's stamping index and each command's
    /// at_index with its changes, for the one atIndex its delta carries.
    #[test]
    fn delta_gets_the_stamp_and_each_commands_at_index() {
        let mut sm = machine();
        let mut scheduled = command("a", "1", "gain=1@1");
        scheduled.at_index = Some(4800);
        let batch = vec![scheduled, command("b", "2", "pan=1@1")];
        sm.apply(1, &commands(96, batch));
        let expiry = expire_at(192, &origin("a").key(), 96);
        sm.apply(2, &expiry);
        assert_eq!(
            *sm.product.delta_calls.lock().unwrap(),
            [(96, vec![Some(4800), None]), (192, vec![None])]
        );
    }

    /// An entry whose commands change nothing takes no revision and makes
    /// no delta; its commands resolve at the revision before it.
    #[test]
    fn an_entry_that_changes_nothing_takes_no_revision() {
        let mut sm = machine();
        sm.apply(1, &commands(48, vec![command("a", "1", "gain=5@5")]));
        let batch = vec![command("a", "2", "gain=4@4"), command("b", "3", "gain=3@3")];
        let response = sm.apply(2, &commands(96, batch));
        assert_eq!(sm.revision, 1);
        assert_eq!(deltas(&sm).len(), 1);
        let outcomes: Vec<_> = response.0.iter().map(|o| (o.revision, o.result)).collect();
        assert_eq!(outcomes, [(1, Resolution::Stale); 2]);
        sm.apply(3, &commands(144, vec![command("a", "4", "gain=6@6")]));
        let next = &deltas(&sm)[1];
        assert_eq!((next.0, next.1), (2, 1), "no gap");
    }

    /// A lease expiry that releases something is a server-side change: its
    /// own revision, one delta naming no command. One a renewal made a
    /// no-op changes nothing.
    #[test]
    fn a_lease_expiry_that_releases_something_takes_a_revision() {
        let mut sm = machine();
        let key = origin("s").key();
        sm.apply(1, &commands(48, vec![command("s", "1", "gain=1@1")]));
        let expiry = |renewed| Request {
            stamp: 96,
            body: Body::LeaseExpiry {
                session: key.clone(),
                renewed,
                at_index: None,
            },
        };
        sm.apply(2, &expiry(0));
        assert_eq!(sm.revision, 1, "renewed at 48, after 0");
        sm.apply(3, &expiry(48));
        assert_eq!(sm.revision, 2);
        assert_eq!(
            deltas(&sm)[1],
            (2, 1, Vec::new(), "owner=1 from ".to_owned())
        );
        // A session that only renewed holds nothing: its expiry ends the
        // lifetime and changes nothing.
        let renewed_only = SessionKey::from_parts("anonymous", "t");
        sm.apply(4, &lease(144, vec![renewed_only.clone()]));
        sm.apply(5, &expire_at(192, &renewed_only, 144));
        assert_eq!(sm.revision, 2, "nothing released");
        assert_eq!(deltas(&sm).len(), 2);
        assert_eq!(
            sm.ended.get(&renewed_only),
            Some(&192),
            "its lifetime ended"
        );
    }

    fn lease(stamp: u64, sessions: Vec<SessionKey>) -> Request {
        Request {
            stamp,
            body: Body::LeaseRenew(sessions),
        }
    }

    fn expire_at(stamp: u64, session: &SessionKey, renewed: u64) -> Request {
        Request {
            stamp,
            body: Body::LeaseExpiry {
                session: session.clone(),
                renewed,
                at_index: None,
            },
        }
    }

    /// A session's ordering state goes the dedup retention plus the failover
    /// bound after the expiry that ended its lifetime, counted in stamping
    /// index on the entries applied, so alike on every replica and after a
    /// snapshot load; a session that came back before then keeps it.
    #[test]
    fn a_session_is_forgotten_retention_plus_failover_after_its_lifetime_ended() {
        let limits = Limits {
            retention: 100,
            max_session_commands: 1024,
            max_dedup_entries: 65536,
            failover: 30,
            ramp_samples: 480,
            block: 48,
        };
        let mut sm = StateMachine::new(Paths::default(), limits);
        let (gone, back) = (origin("s").key(), origin("u").key());
        sm.apply(
            1,
            &commands(
                10,
                vec![command("s", "1", "gain=1@1"), command("u", "1", "pan=1@1")],
            ),
        );
        sm.apply(2, &expire_at(20, &gone, 10));
        sm.apply(3, &expire_at(20, &back, 10));
        sm.apply(4, &commands(40, vec![command("u", "2", "pan=2@2")]));
        let other = SessionKey::from_parts("anonymous", "t");
        sm.apply(5, &lease(149, vec![other.clone()]));
        assert!(sm.product.forgotten.is_empty(), "one sample short of 130");
        let mut loaded = StateMachine::new(Paths::default(), limits);
        loaded.load(sm.save(), limits).expect("loads");
        for machine in [&mut sm, &mut loaded] {
            machine.apply(6, &lease(150, vec![other.clone()]));
            assert_eq!(machine.product.forgotten, std::slice::from_ref(&gone));
            machine.apply(7, &lease(10_000, vec![other.clone()]));
            assert_eq!(
                machine.product.forgotten,
                std::slice::from_ref(&gone),
                "u came back"
            );
        }
    }

    /// A command that changes nothing resolves unchanged at the revision
    /// current after its entry and is listed in no delta; a copy of it is a
    /// duplicate.
    #[test]
    fn a_command_that_changes_nothing_is_unchanged_and_in_no_delta() {
        let mut sm = machine();
        sm.apply(1, &commands(48, vec![command("a", "1", "gain=1@1")]));
        let batch = vec![command("b", "2", "gain=1@2"), command("c", "3", "pan=1@1")];
        let response = sm.apply(2, &commands(96, batch));
        let outcomes: Vec<_> = response.0.iter().map(|o| (o.revision, o.result)).collect();
        assert_eq!(
            outcomes,
            [(2, Resolution::Unchanged), (2, Resolution::Applied)]
        );
        assert_eq!(
            deltas(&sm)[1],
            (2, 1, vec!["3".into()], "pan=1 from c/3".to_owned())
        );
        let copy = sm.apply(3, &commands(144, vec![command("b", "2", "gain=1@2")]));
        assert_eq!(
            (copy.0[0].revision, copy.0[0].result),
            (2, Resolution::Duplicate)
        );
    }

    /// A copy resolves as its first copy did, from the table a snapshot
    /// carries as well: stale stays stale at its revision and an applied
    /// command is a duplicate.
    #[test]
    fn a_copy_resolves_as_its_first_copy_also_after_a_snapshot_load() {
        let mut sm = machine();
        sm.apply(1, &commands(48, vec![command("s", "8", "gain=8@8")]));
        let first = sm.apply(2, &commands(96, vec![command("s", "7", "gain=7@7")]));
        assert_eq!(first.0[0].result, Resolution::Stale);
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        for machine in [&mut sm, &mut loaded] {
            let copies = machine.apply(
                3,
                &commands(
                    144,
                    vec![command("s", "7", "gain=7@7"), command("s", "8", "gain=8@8")],
                ),
            );
            let results: Vec<_> = copies.0.iter().map(|o| (o.revision, o.result)).collect();
            assert_eq!(
                results,
                [(1, Resolution::Stale), (1, Resolution::Duplicate)],
                "both at the one change"
            );
        }
    }

    fn batch(commands: &[(&str, &str, &str)]) -> (Vec<usize>, Vec<Command>) {
        let mut batch = Batch::default();
        let at = commands
            .iter()
            .map(|(session, id, body)| batch.push(command(session, id, body)))
            .collect();
        (at, batch.into_commands())
    }

    /// A copy submitted while its first copy waits in the same batch is
    /// folded into it: it is applied once, not dropped with it.
    #[test]
    fn a_copy_in_the_same_batch_is_applied_once() {
        let (at, folded) = batch(&[("s", "x", "gain=5@5"), ("s", "x", "gain=5@5")]);
        assert_eq!(at, [0, 0]);
        let mut sm = machine();
        let response = sm.apply(1, &commands(48, folded));
        assert_eq!(
            response.0,
            [Outcome {
                revision: 1,
                result: Resolution::Applied
            }]
        );
        assert_eq!(sm.product.values["gain"], ("5".into(), 5));
        // The same id in another session is another command.
        let (at, _) = batch(&[("s", "x", "gain=5@5"), ("t", "x", "gain=6@6")]);
        assert_eq!(at, [0, 1]);
    }

    /// Commands of one batch apply in order with their bodies, so the
    /// product's ordering decides: an older seq submitted after a newer
    /// one is stale, as it would be in an entry of its own.
    #[test]
    fn a_batch_applies_like_its_commands_one_by_one() {
        let sent = [("s", "a", "gain=5@5"), ("s", "b", "gain=3@3")];
        let (_, batched) = batch(&sent);
        let mut together = machine();
        let response = together.apply(1, &commands(48, batched));
        let results: Vec<_> = response.0.iter().map(|o| o.result).collect();
        assert_eq!(results, [Resolution::Applied, Resolution::Stale]);
        let mut apart = machine();
        for (n, (session, id, body)) in sent.into_iter().enumerate() {
            apart.apply(
                n as u64 + 1,
                &commands(48, vec![command(session, id, body)]),
            );
        }
        assert_eq!(together.product.values, apart.product.values);
        assert_eq!(together.product.values["gain"], ("5".into(), 5));
    }

    #[test]
    fn a_copy_after_retention_applies_again_and_the_product_refuses_it() {
        let mut sm = StateMachine::new(
            Paths::default(),
            Limits {
                retention: 100,
                max_session_commands: 1024,
                max_dedup_entries: 65536,
                failover: 0,
                ramp_samples: 480,
                block: 48,
            },
        );
        sm.apply(1, &commands(0, vec![command("s", "7", "gain=1@7")]));
        sm.apply(2, &commands(50, vec![command("s", "8", "gain=2@8")]));
        let within = sm.apply(3, &commands(100, vec![command("s", "7", "gain=1@7")]));
        assert_eq!(
            within.0[0],
            Outcome {
                revision: 1,
                result: Resolution::Duplicate
            }
        );
        sm.apply(4, &commands(150, vec![command("s", "9", "pan=0@9")]));
        let after = sm.apply(5, &commands(150, vec![command("s", "7", "gain=1@7")]));
        assert_eq!(
            after.0[0],
            Outcome {
                revision: 3,
                result: Resolution::Stale
            },
            "at the product revision of the three changes before"
        );
        assert_eq!(sm.product.values["gain"].0, "2");
    }

    #[test]
    fn a_far_future_at_index_moves_no_retention() {
        let mut sm = StateMachine::new(
            Paths::default(),
            Limits {
                retention: 100,
                max_session_commands: 1024,
                max_dedup_entries: 65536,
                failover: 0,
                ramp_samples: 480,
                block: 48,
            },
        );
        sm.apply(1, &commands(0, vec![command("s", "1", "a=1@1")]));
        let mut scheduled = command("s", "2", "b=1@2");
        scheduled.at_index = Some(1 << 40);
        sm.apply(2, &commands(48, vec![scheduled]));
        assert_eq!(
            sm.dedup
                .revision(&SessionKey::from_parts("anonymous", "s"), "1"),
            Some(1)
        );
    }

    #[test]
    fn a_lease_expiry_after_a_command_of_its_session_is_a_no_op() {
        let mut sm = machine();
        let key = SessionKey::from_parts("anonymous", "s");
        sm.apply(
            1,
            &Request {
                stamp: 48,
                body: Body::LeaseRenew(vec![key.clone()]),
            },
        );
        sm.apply(2, &commands(96, vec![command("s", "1", "a=1@1")]));
        sm.apply(
            3,
            &Request {
                stamp: 144,
                body: Body::LeaseExpiry {
                    session: key.clone(),
                    renewed: 48,
                    at_index: None,
                },
            },
        );
        assert_eq!(sm.leases.get(&key), Some(&96));
        assert!(sm.product.expired.is_empty());
        sm.apply(
            4,
            &Request {
                stamp: 192,
                body: Body::LeaseExpiry {
                    session: key.clone(),
                    renewed: 96,
                    at_index: None,
                },
            },
        );
        assert_eq!(sm.leases.get(&key), None);
        assert_eq!(sm.product.expired, [key]);
    }

    #[test]
    fn a_lower_stamp_does_not_move_a_lease_back() {
        let mut sm = machine();
        let key = SessionKey::from_parts("anonymous", "s");
        let renew = |stamp| Request {
            stamp,
            body: Body::LeaseRenew(vec![SessionKey::from_parts("anonymous", "s")]),
        };
        sm.apply(1, &renew(192));
        // A new leader whose estimate is behind the old one's.
        sm.apply(2, &renew(96));
        sm.apply(3, &commands(144, vec![command("s", "1", "a=1@1")]));
        assert_eq!(sm.leases.get(&key), Some(&192));
        sm.apply(
            4,
            &Request {
                stamp: 240,
                body: Body::LeaseExpiry {
                    session: key.clone(),
                    renewed: 144,
                    at_index: None,
                },
            },
        );
        assert_eq!(sm.leases.get(&key), Some(&192));
        assert!(sm.product.expired.is_empty());
    }

    #[test]
    fn engine_entries_wait_in_pending_until_voided_or_restamped() {
        let mut sm = machine();
        sm.apply(
            1,
            &Request {
                stamp: 0,
                body: Body::InputDown {
                    slot: 2,
                    at_index: 960,
                },
            },
        );
        sm.apply(
            2,
            &Request {
                stamp: 0,
                body: Body::MasterEnable {
                    enabled: false,
                    at_index: 960,
                },
            },
        );
        sm.apply(
            3,
            &Request {
                stamp: 0,
                body: Body::Restamp {
                    revision: 1,
                    term: 0,
                    at_index: 1920,
                    paths: vec![],
                    generation: None,
                },
            },
        );
        sm.apply(
            4,
            &Request {
                stamp: 0,
                body: Body::Void {
                    revision: 2,
                    term: 0,
                    at_index: None,
                    generation: None,
                },
            },
        );
        assert_eq!(sm.pending.len(), 1);
        assert_eq!(
            sm.pending[&3].at_index(),
            Some(1920),
            "known under the restamp's id"
        );
        assert_eq!(sm.restamps[&3], (1, 1920));
    }

    fn at(stamp: u64, body: Body) -> Request {
        Request { stamp, body }
    }

    /// Each restamp names the one before it and the count follows the
    /// entry; a decision naming another term at that index is one on an
    /// id this log does not hold and leaves its entry alone.
    #[test]
    fn restamps_follow_the_entry_and_a_decision_on_another_term_changes_nothing() {
        let mut sm = machine();
        let down = Body::InputDown {
            slot: 1,
            at_index: 960,
        };
        sm.apply_at(5, 2, &at(0, down));
        sm.apply_at(
            6,
            3,
            &at(
                0,
                Body::Void {
                    revision: 5,
                    term: 1,
                    at_index: None,
                    generation: None,
                },
            ),
        );
        sm.apply_at(
            7,
            3,
            &at(
                0,
                Body::Restamp {
                    revision: 5,
                    term: 1,
                    at_index: 1920,
                    paths: vec![],
                    generation: None,
                },
            ),
        );
        assert_eq!(sm.pending[&5].at_index(), Some(960), "term 1 is not held");
        let mut of = (5, 2);
        for (n, index) in [8u64, 9, 10].into_iter().enumerate() {
            let restamp = Body::Restamp {
                revision: of.0,
                term: of.1,
                at_index: 960 * (index - 6),
                paths: vec![],
                generation: None,
            };
            sm.apply_at(index, 3, &at(0, restamp));
            assert_eq!(sm.restamps[&index].0, n as u32 + 1);
            assert_eq!(sm.pending[&index].at_index(), Some(960 * (index - 6)));
            of = (index, 3);
        }
        assert_eq!(sm.pending.len(), 1);
        sm.apply_at(
            11,
            3,
            &at(
                0,
                Body::Void {
                    revision: 10,
                    term: 3,
                    at_index: None,
                    generation: None,
                },
            ),
        );
        assert!(sm.pending.is_empty() && sm.restamps.is_empty() && sm.terms.is_empty());
    }

    /// The Void the engine is handed in place of a repeat decision on an
    /// id changes nothing: not once the entry was applied (pruned past,
    /// so no longer pending), nor once it was restamped (known under the
    /// restamp's id, which the repeat does not name).
    #[test]
    fn the_void_standing_in_for_a_repeat_decision_is_a_no_op() {
        let mut sm = machine();
        let down = Body::InputDown {
            slot: 1,
            at_index: 960,
        };
        sm.apply_at(5, 2, &at(0, down.clone()));
        let restamp = Body::Restamp {
            revision: 5,
            term: 2,
            at_index: 1920,
            paths: vec![],
            generation: None,
        };
        sm.apply_at(6, 3, &at(0, restamp));
        let (pending, restamps) = (sm.pending.clone(), sm.restamps.clone());
        sm.apply_at(
            7,
            3,
            &at(
                0,
                Body::Void {
                    revision: 5,
                    term: 2,
                    at_index: None,
                    generation: None,
                },
            ),
        );
        assert_eq!(
            (&sm.pending, &sm.restamps),
            (&pending, &restamps),
            "restamped"
        );
        assert_eq!(sm.decided[&(5, 2)].0, 6, "the first decision stays the one");
        // Applied: its index passed long ago and a later entry on the same
        // slot leaves only that one pending.
        let mut sm = machine();
        sm.apply_at(5, 2, &at(0, down));
        let up = Body::InputUp {
            slot: 1,
            at_index: 2 * PASSED,
        };
        sm.apply_at(6, 2, &at(4 * PASSED, up));
        let pending = sm.pending.clone();
        assert!(!pending.contains_key(&5));
        sm.apply_at(
            7,
            3,
            &at(
                4 * PASSED,
                Body::Void {
                    revision: 5,
                    term: 2,
                    at_index: None,
                    generation: None,
                },
            ),
        );
        assert_eq!(sm.pending, pending, "applied");
    }

    /// Pending engine entries stay bounded: once an entry's index lies
    /// [`PASSED`] below the highest stamp applied only the latest per
    /// target is kept, entries ahead all are, and a snapshot carries the
    /// terms and restamp counts.
    #[test]
    fn pending_keeps_the_latest_passed_entry_per_target() {
        let mut sm = machine();
        for n in 0..1000u64 {
            let body = if n % 2 == 0 {
                Body::InputDown {
                    slot: (n % 4) as u32,
                    at_index: n * 48,
                }
            } else {
                Body::MasterEnable {
                    enabled: n % 3 == 0,
                    at_index: n * 48,
                }
            };
            sm.apply_at(n + 1, 1, &at(n * 48, body));
        }
        let ahead = 1000 * 48;
        sm.apply_at(
            1001,
            1,
            &at(
                ahead + PASSED,
                Body::InputUp {
                    slot: 0,
                    at_index: ahead + 2 * PASSED,
                },
            ),
        );
        // Slots 0 and 2 and the master once each, and the one ahead.
        assert_eq!(sm.pending.len(), 4, "{:?}", sm.pending.keys());
        assert_eq!(sm.pending.len(), sm.terms.len());
        assert!(sm.pending.contains_key(&1001));
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        assert_eq!(loaded.terms, sm.terms);
        assert_eq!(loaded.pending, sm.pending);
    }
}

/// Engine entries derived from product state: a desk whose coefficients
/// depend on more than one command.
#[cfg(test)]
mod derive_tests {
    use samplez::{Entry, Targets};
    use samplez_dsp::StripParam;
    use serde::{Deserialize, Serialize};

    use super::tests::{command, origin};
    use super::*;

    /// Two input slots of one channel into one output: each coefficient is
    /// the slot's fader times the VCA, slot 1 silent while a session holds
    /// solo on slot 0. Commands: `fader <slot> <gain>`, `vca <gain>`,
    /// `trim <slot> <db>`, `route <slot> <flow>`, `pair <flow> <flow>`
    /// (both slots), `pause <meter> <0|1>`, `reset <meter>`, `solo`,
    /// `noop`, `stale`.
    /// The input domain of each slot, as configured.
    const DOMAINS: [u8; 2] = [0, 1];

    #[derive(Serialize, Deserialize)]
    struct Desk {
        faders: [f64; 2],
        vca: f64,
        trims: [f32; 2],
        flows: [String; 2],
        solo: Option<SessionKey>,
        /// Slot 1 ducked by group 0 on its coefficient.
        #[serde(default)]
        ducked: bool,
        /// Per meter, its loudness measurement paused.
        #[serde(default)]
        paused: [bool; 2],
        /// Per meter, its loudness reset epoch.
        #[serde(default)]
        resets: [u32; 2],
        /// Slot 0's tone parameters, once a command set them.
        #[serde(default)]
        tone: Option<(u32, f32, bool)>,
    }

    impl Desk {
        fn new() -> Desk {
            Desk {
                faders: [1.0, 1.0],
                vca: 1.0,
                trims: [0.0, 0.0],
                flows: ["a".into(), "b".into()],
                solo: None,
                ducked: false,
                paused: [false; 2],
                resets: [0; 2],
                tone: None,
            }
        }
    }

    impl Product for Desk {
        const DERIVATION_VERSION: u32 = 1;
        type Change = ();
        const DRIVES_ENGINE: bool = true;

        fn apply(&mut self, origin: &Origin, command: &[u8], ctx: &Apply) -> Effect<()> {
            let text = std::str::from_utf8(command).expect("utf-8");
            let words: Vec<&str> = text.split(' ').collect();
            match words[..] {
                ["fader", slot, gain] => {
                    self.faders[slot.parse::<usize>().unwrap()] = gain.parse().unwrap()
                }
                ["vca", gain] => self.vca = gain.parse().unwrap(),
                ["duck", on] => self.ducked = on == "1",
                ["pause", meter, on] => self.paused[meter.parse::<usize>().unwrap()] = on == "1",
                ["reset", meter] => {
                    let epoch = &mut self.resets[meter.parse::<usize>().unwrap()];
                    *epoch = epoch.wrapping_add(1);
                }
                ["tone", hz, db, ident] => {
                    self.tone = Some((hz.parse().unwrap(), db.parse().unwrap(), ident == "1"))
                }
                ["trim", slot, db] => {
                    self.trims[slot.parse::<usize>().unwrap()] = db.parse().unwrap()
                }
                // Ordered off the generation path, or across two input
                // domains: refused.
                ["route", ..] | ["pair", ..] if ctx.generation == Some(false) => {
                    return Effect::Unchanged;
                }
                ["pair", ..] if DOMAINS[0] != DOMAINS[1] => return Effect::Unchanged,
                ["pair", a, b] => self.flows = [a.into(), b.into()],
                ["route", slot, flow] => self.flows[slot.parse::<usize>().unwrap()] = flow.into(),
                ["solo"] => self.solo = Some(origin.key()),
                ["noop"] => return Effect::Unchanged,
                _ => return Effect::Stale,
            }
            Effect::Changed(())
        }
        fn expire(&mut self, session: &SessionKey) -> Effect<()> {
            if self.solo.as_ref() != Some(session) {
                return Effect::Unchanged;
            }
            self.solo = None;
            Effect::Changed(())
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
            let gain = |slot: usize| {
                let muted = slot == 1 && self.solo.is_some();
                if muted {
                    0.0
                } else {
                    (self.faders[slot] * self.vca) as f32
                }
            };
            Some(Targets {
                gains: vec![vec![gain(0)], vec![gain(1)]],
                flows: self.flows.iter().cloned().map(Some).collect(),
                strips: self
                    .trims
                    .iter()
                    .map(|&db| vec![StripParam::Trim { db }])
                    .collect(),
                masks: vec![vec![0], vec![u8::from(self.ducked)]],
                // Listed only while a meter is paused.
                loudness_running: if self.paused.contains(&true) {
                    self.paused.iter().map(|&p| !p).collect()
                } else {
                    Vec::new()
                },
                loudness_resets: self.resets.to_vec(),
                // Listed only once set.
                tones: (self.tone.iter())
                    .map(|&(frequency_hz, level_db, ident)| {
                        Some(samplez_dsp::ToneParams {
                            frequency_hz,
                            level_db,
                            ident,
                        })
                    })
                    .collect(),
                ..Targets::default()
            })
        }
        fn reroutes(&self, _: &Origin, command: &[u8]) -> Vec<u32> {
            let text = std::str::from_utf8(command).expect("utf-8");
            match text.split(' ').collect::<Vec<_>>()[..] {
                ["route", slot, _] => vec![slot.parse().unwrap()],
                ["pair", ..] => vec![0, 1],
                _ => Vec::new(),
            }
        }
    }

    fn machine() -> StateMachine<Desk> {
        StateMachine::new(Desk::new(), Limits::defaults(48_000))
    }

    /// Targets of one coefficient at 0.5 on flow `a`, unmasked, slot 0
    /// ducked 10 dB by group 0.
    fn held() -> Targets {
        Targets {
            gains: vec![vec![0.5]],
            flows: vec![Some("a".to_owned()), Some("b".to_owned())],
            ducks: vec![samplez::DuckTargets {
                depths: vec![10.0],
                ..samplez::DuckTargets::default()
            }],
            masks: vec![vec![0]],
            ..Targets::default()
        }
    }

    /// A fade started before the index its entry applies at is tracked
    /// as the engine joins it, from the value the coefficient holds
    /// there; one starting at its index as it is.
    #[test]
    fn the_tracker_joins_a_late_fade() {
        let mut sm = machine();
        let f = samplez::Fade {
            from: 0.2,
            start: 4800,
            len: 9600,
        };
        let gain = |fade| Entry::Gain {
            input: 0,
            output: 0,
            gain: 1.0,
            fade: Some(fade),
        };
        sm.follow(&[gain(f)], Some(&held()), 9600);
        let joined = f.joined(1.0, 0.5, 9600, 480);
        assert_ne!(joined, Some(f));
        assert_eq!(sm.ramps[0][0], joined);
        let on_time = samplez::Fade { start: 19_200, ..f };
        sm.follow(&[gain(on_time)], Some(&held()), 19_200);
        assert_eq!(sm.ramps[0][0], Some(on_time));
    }

    /// A depth setting ramps the slot's depth from the value it has at
    /// the setting's index over the ramp length, a ramp still running
    /// included, the value as the bridge admits it.
    #[test]
    fn the_tracker_follows_the_depth_ramps() {
        let mut sm = machine();
        let depth = |db| Entry::Duck {
            group: 0,
            setting: samplez::DuckSetting::Depth { slot: 0, db },
        };
        sm.follow(&[depth(30.0)], Some(&held()), 9600);
        let first = samplez::Fade {
            from: 10.0,
            start: 9600,
            len: 480,
        };
        assert_eq!(sm.in_flight.depths, vec![vec![Some(first)]]);
        let mut now = held();
        now.ducks[0].depths[0] = 30.0;
        // Clamped to 40 dB, from where the first ramp is.
        sm.follow(&[depth(99.0)], Some(&now), 9696);
        let second = samplez::Fade {
            from: first.value_at(30.0, 9696),
            start: 9696,
            len: 480,
        };
        assert_eq!(sm.in_flight.depths, vec![vec![Some(second)]]);
    }

    /// A mask change joins the coefficient's changes while the last of
    /// them is within the ramp and two gain intervals, starts new ones
    /// after that, and is no crossfade on a coefficient at 0.
    #[test]
    fn the_tracker_follows_the_mask_crossfades() {
        let mut sm = machine();
        let mask = |mask| Entry::DuckMask {
            input: 0,
            output: 0,
            mask,
        };
        let mut now = held();
        sm.follow(&[mask(1)], Some(&now), 9600);
        now.masks[0][0] = 1;
        sm.follow(&[mask(0)], Some(&now), 9696);
        now.masks[0][0] = 0;
        assert_eq!(
            sm.in_flight.masks,
            vec![(0, 0, 0, vec![(9600, 1), (9696, 0)])]
        );
        sm.follow(&[mask(1)], Some(&now), 20_016);
        now.masks[0][0] = 1;
        assert_eq!(sm.in_flight.masks, vec![(0, 0, 0, vec![(20_016, 1)])]);
        now.gains[0][0] = 0.0;
        sm.follow(&[mask(0)], Some(&now), 40_032);
        assert!(sm.in_flight.masks.is_empty());
    }

    /// An `InputUp` that raises a slot from down starts its fade-in, one
    /// for a slot up starts none; a snapshot carries which slots are down.
    #[test]
    fn the_tracker_follows_the_input_fade_ins() {
        let mut sm = machine();
        let (down, up) = (
            |slot| Entry::InputDown { slot },
            |slot| Entry::InputUp { slot },
        );
        sm.follow_inputs(&[up(0)], 4800, 1, None);
        assert_eq!(sm.in_flight.fade_ins, vec![None]);
        sm.follow_inputs(&[down(1)], 4800, 2, None);
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        for m in [&mut sm, &mut loaded] {
            m.follow_inputs(&[up(1)], 9600, 3, None);
            assert_eq!(m.in_flight.fade_ins, vec![None, Some(9600)]);
        }
    }

    /// A restamp moves a fade-in only where the entry it moves started
    /// it: a moved `InputUp` that raised nothing, its slot up already,
    /// leaves the fade-in an earlier one started where it is, and one that
    /// raised the slot moves its own fade-in to the restamp's index, also
    /// restamped again and after a snapshot load.
    #[test]
    fn a_restamp_moves_only_the_fade_in_its_entry_started() {
        let mut sm = machine();
        let (down, up) = (
            |slot| Entry::InputDown { slot },
            |slot| Entry::InputUp { slot },
        );
        sm.follow_inputs(&[down(0)], 4800, 1, None);
        sm.follow_inputs(&[up(0)], 9600, 2, None);
        // Up already: raises nothing.
        sm.follow_inputs(&[up(0)], 9648, 3, None);
        assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
        // Entry 3 restamped as 4.
        sm.follow_inputs(&[up(0)], 9696, 4, Some(3));
        assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
        // Entry 2 restamped as 5, then 5 as 6 after a load.
        sm.follow_inputs(&[up(0)], 9744, 5, Some(2));
        assert_eq!(sm.in_flight.fade_ins, vec![Some(9744)]);
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        loaded.follow_inputs(&[up(0)], 9792, 6, Some(5));
        assert_eq!(loaded.in_flight.fade_ins, vec![Some(9792)]);
    }

    /// A void of an `InputUp` that raised a slot, which no engine
    /// applies, leaves the slot down and its fade-in as before it: no
    /// fade-in recorded, and the next `InputUp` raises the slot.
    #[test]
    fn a_voided_input_up_leaves_the_slot_as_before_it() {
        let mut sm = machine();
        let request = |body| Request { stamp: 0, body };
        let up = |at_index| Body::InputUp { slot: 0, at_index };
        sm.apply_at(
            1,
            1,
            &request(Body::InputDown {
                slot: 0,
                at_index: 4800,
            }),
        );
        sm.apply_at(2, 1, &request(up(9600)));
        assert_eq!(sm.in_flight.fade_ins, vec![Some(9600)]);
        let void = Body::Void {
            revision: 2,
            term: 1,
            at_index: None,
            generation: None,
        };
        sm.apply_at(3, 1, &request(void));
        assert_eq!(sm.in_flight.fade_ins, vec![None]);
        sm.apply_at(4, 1, &request(up(19_200)));
        assert_eq!(sm.in_flight.fade_ins, vec![Some(19_200)]);
    }

    /// A re-route or detach that changes the flow a slot reads starts a
    /// crossfade from that flow; one to the flow read starts none.
    #[test]
    fn the_tracker_follows_the_route_crossfades() {
        let mut sm = machine();
        let route = |slot, flow: &str| Entry::Route {
            slot,
            flow_id: flow.to_owned(),
        };
        sm.follow(&[route(0, "a"), route(1, "c")], Some(&held()), 9600);
        let from_b = Some((9600, Some("b".to_owned())));
        assert_eq!(sm.in_flight.crossfades, vec![None, from_b]);
        sm.follow(&[Entry::Detach { slot: 0 }], Some(&held()), 19_200);
        let from_a = Some((19_200, Some("a".to_owned())));
        assert_eq!(sm.in_flight.crossfades[0], from_a);
    }

    /// A `Commands` entry of one command `body` of session `s`, stamped
    /// `stamp`, asking for `at`, flagged `generation`.
    fn one(id: &str, body: &str, stamp: u64, at: u64, generation: Option<bool>) -> Request {
        let mut c = command("s", id, body);
        c.at_index = Some(at);
        c.generation = generation;
        Request {
            stamp,
            body: Body::Commands(vec![c]),
        }
    }

    fn gains(r: &Record) -> Vec<(usize, usize, u32)> {
        r.entries
            .iter()
            .filter_map(|e| match *e {
                Entry::Gain {
                    input,
                    output,
                    gain,
                    ..
                } => Some((input, output, gain.to_bits())),
                _ => None,
            })
            .collect()
    }

    /// A command pausing a meter derives its `LoudnessRun`, and one
    /// running it again another, at the command's index; the product state
    /// carries the meter paused, so a rebuilt engine pauses it too.
    #[test]
    fn a_meter_paused_by_a_command_derives_its_entry_and_state() {
        let mut sm = machine();
        sm.apply_at(1, 1, &one("1", "pause 1 1", 0, 960, None));
        let run = |running| Entry::LoudnessRun { meter: 1, running };
        assert_eq!(sm.records[&1].entries, [run(false)]);
        assert_eq!(sm.records[&1].at_index, 960);
        assert_eq!(sm.product_state(1).loudness_running, [true, false]);
        sm.apply_at(2, 1, &one("2", "pause 1 0", 48, 1008, None));
        assert_eq!(sm.records[&2].entries, [run(true)]);
        assert!(sm.product_state(2).loudness_running.is_empty());
    }

    /// A restamp of entry `of` to `at_index`, stamped 96.
    fn restamp_of(of: u64, at_index: u64) -> Request {
        Request {
            stamp: 96,
            body: Body::Restamp {
                revision: of,
                term: 1,
                at_index,
                paths: Vec::new(),
                generation: None,
            },
        }
    }

    /// The loudness resets of `sm`'s records, as (log index, record
    /// index, meter), but for the records of the entries `decided` names:
    /// the log restamped or voided those, so their records never apply.
    fn resets_applied(sm: &StateMachine<Desk>, decided: &[u64]) -> Vec<(u64, u64, usize)> {
        (sm.records.iter())
            .filter(|(i, _)| !decided.contains(i))
            .flat_map(|(&i, r)| {
                (r.entries.iter()).filter_map(move |e| match *e {
                    Entry::ResetLoudness { meter } => Some((i, r.at_index, meter)),
                    _ => None,
                })
            })
            .collect()
    }

    /// A command moving a meter's reset epoch derives one reset at its
    /// index. Restamped, and the restamp restamped again, the reset
    /// moves with it: exactly one applies, at the last restamp's index,
    /// though the restamps re-assert the targets, which hold no reset. A
    /// later command derives none.
    #[test]
    fn a_restamped_reset_applies_once_at_the_restamp() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "reset 1", 0, 3000, None));
        assert_eq!(resets_applied(&sm, &[]), [(10, 3000, 1)]);
        sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
        sm.apply_at(12, 1, &restamp_of(10, 4000));
        assert_eq!(resets_applied(&sm, &[10]), [(12, 4000, 1)]);
        sm.apply_at(13, 1, &restamp_of(12, 5000));
        assert_eq!(resets_applied(&sm, &[10, 12]), [(13, 5000, 1)]);
        sm.apply_at(14, 1, &one("3", "fader 1 0.5", 96, 6000, None));
        assert_eq!(resets_applied(&sm, &[10, 12]), [(13, 5000, 1)]);
    }

    /// A command moving a meter's reset epoch that the log voids resets
    /// nothing: the void re-asserts the state, which holds no reset, and
    /// so does a restamp of the stamped void.
    #[test]
    fn a_voided_reset_applies_nowhere() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "reset 0", 0, 3000, None));
        sm.apply_at(11, 1, &void(10, 96, Some(4800)));
        assert!(
            sm.records.contains_key(&11),
            "the void re-asserts the state"
        );
        assert!(resets_applied(&sm, &[10]).is_empty());
        sm.apply_at(12, 1, &restamp_of(11, 6000));
        assert!(sm.records.contains_key(&12), "the restamp re-asserts it");
        assert!(resets_applied(&sm, &[10, 11]).is_empty());
    }

    /// Two replicas applying one log derive the same resets at the same
    /// indexes, one of them loaded from a snapshot taken after the reset
    /// commands and before the log decided them: a restamped reset
    /// applies at the restamp on both, a voided one on neither.
    #[test]
    fn replicas_derive_the_same_resets_from_one_log() {
        let log = [
            one("1", "reset 1", 0, 3000, None),
            one("2", "reset 0", 48, 3000, None),
            one("3", "fader 0 0.2", 96, 3000, None),
            restamp_of(1, 4000),
            void(2, 144, Some(4800)),
            one("4", "reset 1", 192, 5000, None),
        ];
        let mut whole = machine();
        for (n, r) in log.iter().enumerate().take(3) {
            whole.apply_at(n as u64 + 1, 1, r);
        }
        let mut loaded = machine();
        loaded.load(whole.save(), Limits::defaults(48_000)).unwrap();
        for (n, r) in log.iter().enumerate().skip(3) {
            whole.apply_at(n as u64 + 1, 1, r);
            loaded.apply_at(n as u64 + 1, 1, r);
        }
        let applied = |sm: &StateMachine<Desk>| -> Vec<(u64, u64, usize)> {
            (resets_applied(sm, &[1, 2]).into_iter())
                .filter(|&(i, ..)| i > 3)
                .collect()
        };
        assert_eq!(applied(&whole), [(4, 4000, 1), (6, 5000, 1)]);
        assert_eq!(applied(&loaded), applied(&whole));
    }

    /// The product state carries the ramp each plain gain entry left the
    /// coefficient on, from the value the engine holds at the entry's
    /// index, a ramp still running then included, so a rebuilt engine
    /// continues them.
    #[test]
    fn the_product_state_carries_the_ramps_of_plain_gain_entries() {
        let mut sm = machine();
        let g0 = sm.product_state(0).gains[0][0];
        let ramp = Limits::defaults(48_000).ramp_samples;
        sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
        let first = samplez::Fade {
            from: g0,
            start: 960,
            len: ramp,
        };
        assert_eq!(sm.product_state(1).fading[0][0], Some(first));
        // The second entry lands inside the first one's ramp.
        sm.apply_at(2, 1, &one("2", "fader 0 0.75", 48, 1008, None));
        let second = samplez::Fade {
            from: first.value_at(0.5, 1008),
            start: 1008,
            len: ramp,
        };
        assert_ne!(second.from.to_bits(), 0.5f32.to_bits());
        assert_eq!(sm.product_state(2).fading[0][0], Some(second));
    }

    /// The product state carries the ramps the derived tone entries leave
    /// slot 0's generator on, as a generator that took the same entries at
    /// the block-aligned indices the bridge applies them at holds them: a
    /// change inside the frequency crossfade waiting, and parameters out
    /// of range taken as the bridge admits them, a level of +6 dBFS and a
    /// frequency of 5 Hz clamped to 0 dBFS and 20 Hz, a NaN level the
    /// default. A snapshot carries them, and one written before they were
    /// kept loads with none. Fails with `.admitted()` dropped from
    /// `follow_tone`, or with `tone_ramps: Vec::new()` in `save`.
    #[test]
    fn the_product_state_carries_the_tone_ramps() {
        let mut sm = machine();
        assert!(sm.product_state(0).tone_ramps.is_empty());
        let limits = Limits::defaults(48_000);
        let p = |frequency_hz, level_db, ident| samplez_dsp::ToneParams {
            frequency_hz,
            level_db,
            ident,
        };
        // (command, index, what the bridge applies)
        let log = [
            ("tone 440 -6 0", 970, p(440, -6.0, false)),
            ("tone 1000 -12 1", 1010, p(1000, -12.0, true)),
            ("tone 5 6 1", 1700, p(20, 0.0, true)),
            ("tone 440 NaN 0", 2500, samplez_dsp::ToneParams::DEFAULT),
            // Finite again, so the product saves and loads.
            ("tone 997 -3 1", 4000, p(997, -3.0, true)),
        ];
        let mut want = samplez_dsp::ToneRamps::default();
        for (n, &(body, at, applied)) in log.iter().enumerate() {
            let id = (n + 1).to_string();
            sm.apply_at(n as u64 + 1, 1, &one(&id, body, 48 * n as u64, at, None));
            let at = u64::next_multiple_of(at, limits.block);
            assert!(want.set(at, applied, limits.ramp_samples));
            if n == 2 {
                // At 1728 the crossfade to 1 kHz runs from 1488, the end
                // of the one to 440 Hz it waited for, and 20 Hz waits
                // behind it.
                let f = want.frequency;
                assert_eq!((f.from, f.to, f.start, f.next), (440, 1000, 1488, Some(20)));
            }
            let state = sm.product_state(n as u64 + 1);
            assert_eq!(state.tone_ramps, vec![Some(want)], "after {body}");
        }
        let state = sm.product_state(5);
        assert_eq!(state.tones, vec![Some(p(997, -3.0, true))]);
        let mut loaded = machine();
        loaded.load(sm.save(), limits).expect("loads");
        assert_eq!(loaded.product_state(5).tone_ramps, vec![Some(want)]);
        let legacy = Replicated {
            tone_ramps: Vec::new(),
            ..sm.save()
        };
        loaded.load(legacy, limits).expect("loads");
        assert!(loaded.product_state(5).tone_ramps.is_empty());
    }

    /// With any ramp running, `fading` has the shape of `gains`, which an
    /// install requires: a change of one cell of a matrix of several rows
    /// must not leave the other rows out. With none it is empty.
    #[test]
    fn the_ramps_of_a_product_state_are_shaped_as_its_gains() {
        let mut sm = machine();
        assert!(sm.product_state(0).fading.is_empty());
        // Only the second row changes.
        sm.apply_at(1, 1, &one("1", "fader 1 0.5", 960, 960, None));
        let state = sm.product_state(1);
        assert!(state.fading[1][0].is_some() && state.fading[0][0].is_none());
        assert_eq!(state.fading.len(), state.gains.len());
        assert!(
            state
                .fading
                .iter()
                .zip(&state.gains)
                .all(|(f, g)| f.len() == g.len())
        );
        // A ramp that has finished below the floor is pruned.
        sm.apply_at(2, 1, &one("2", "noop", 960 + 100_000, 960 + 100_000, None));
        assert!(sm.product_state(2).fading.is_empty());
    }

    /// A gain an engine refuses is derived as silence, and the product
    /// state a rebuild reads holds the same: a live replica handed the
    /// records and one rebuilt past them compute alike. A later valid gain
    /// is derived over it.
    #[test]
    fn a_gain_an_engine_refuses_is_derived_as_silence_everywhere() {
        let mut sm = machine();
        sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
        sm.apply_at(2, 1, &one("2", "fader 0 inf", 48, 1008, None));
        assert_eq!(gains(&sm.records[&2]), [(0, 0, 0.0f32.to_bits())]);
        assert_eq!(sm.product_state(2).gains[0][0].to_bits(), 0.0f32.to_bits());
        // Silence already: nothing to derive.
        sm.apply_at(3, 1, &one("3", "fader 0 NaN", 96, 1056, None));
        assert!(!sm.records.contains_key(&3));
        assert_eq!(sm.product_state(3).gains[0][0].to_bits(), 0.0f32.to_bits());
        sm.apply_at(4, 1, &one("4", "fader 0 0.25", 144, 1104, None));
        assert_eq!(gains(&sm.records[&4]), [(0, 0, 0.25f32.to_bits())]);
        // Past MAX_GAIN, finite.
        sm.apply_at(5, 1, &one("5", "fader 0 2e6", 192, 1152, None));
        assert_eq!(gains(&sm.records[&5]), [(0, 0, 0.0f32.to_bits())]);
        assert_eq!(sm.product_state(5).gains[0][0].to_bits(), 0.0f32.to_bits());
    }

    /// A VCA move derives every member coefficient as fader times VCA,
    /// which no translation of the VCA command alone could give, and two
    /// replicas derive the same bits.
    #[test]
    fn a_vca_move_derives_every_member_coefficient() {
        let log = [
            one("1", "fader 0 0.5", 0, 960, None),
            one("2", "fader 1 0.25", 0, 960, None),
            one("3", "vca 0.5", 48, 1008, None),
        ];
        let (mut a, mut b) = (machine(), machine());
        for (n, r) in log.iter().enumerate() {
            a.apply(n as u64 + 1, r);
            b.apply(n as u64 + 1, r);
        }
        let vca = &a.records[&3];
        assert_eq!(
            gains(vca),
            [(0, 0, 0.25f32.to_bits()), (1, 0, 0.125f32.to_bits())]
        );
        assert_eq!(vca.at_index, 1008);
        assert_eq!(a.records, b.records);
    }

    /// A strip parameter is derived whole from the state.
    #[test]
    fn a_strip_parameter_is_derived_from_the_state() {
        let mut sm = machine();
        sm.apply(1, &one("1", "trim 1 -6", 0, 960, None));
        assert_eq!(
            sm.records[&1].entries,
            [Entry::Strip {
                slot: 1,
                param: StripParam::Trim { db: -6.0 }
            }]
        );
    }

    /// A stale, an unchanged and a duplicate command derive nothing, and
    /// the floor still advances to their index.
    #[test]
    fn a_command_that_changes_nothing_derives_nothing() {
        let mut sm = machine();
        sm.apply(1, &one("1", "stale", 0, 960, None));
        sm.apply(2, &one("2", "noop", 0, 1440, None));
        sm.apply(3, &one("3", "fader 0 0.5", 0, 1920, None));
        sm.apply(4, &one("3", "fader 0 0.5", 0, 2400, None));
        assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [3]);
        assert_eq!(sm.floor, 2400);
    }

    /// The index an unstamped lease expiry derives at is a block start for
    /// the block sizes the bridge hands records at unrounded, so the index
    /// contact lists, awaits and floors at is the one the engine applies.
    #[test]
    fn the_fallback_index_is_a_block_start_for_the_usual_block_sizes() {
        let blocks = (4..=10).map(|n| 1u64 << n).chain([48, 96, 192, 384, 768]);
        for block in blocks {
            let request = Request {
                stamp: 7 * block,
                body: Body::LeaseExpiry {
                    session: origin("s").key(),
                    renewed: 0,
                    at_index: None,
                },
            };
            let at = floored(0, &request).expect("an expiry takes effect");
            assert_eq!(at % block, 0, "block {block}: index {at}");
        }
    }

    /// A lease expiry that releases what its session held derives the
    /// coefficients it changes, [`FALLBACK_LEAD`] past its stamp when the
    /// leader stamped no index, raised to the floor.
    #[test]
    fn a_lease_expiry_derives_through_the_same_hook() {
        let mut sm = machine();
        sm.apply(1, &one("1", "solo", 0, 960, None));
        assert_eq!(gains(&sm.records[&1]), [(1, 0, 0.0f32.to_bits())]);
        let expiry = |renewed| Request {
            stamp: 4800,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed,
                at_index: None,
            },
        };
        sm.apply(2, &expiry(0));
        assert_eq!(gains(&sm.records[&2]), [(1, 0, 1.0f32.to_bits())]);
        assert_eq!(sm.records[&2].at_index, 4800 + FALLBACK_LEAD);
        // One the session outlived changes nothing.
        sm.apply(3, &one("3", "solo", 4848, 5808, None));
        sm.apply(4, &expiry(0));
        assert!(!sm.records.contains_key(&4));
    }

    /// An entry stamped earlier than the one before it in the log takes
    /// effect at that one's index: the engine applies them in log order,
    /// and the fader ends at the later entry's value.
    #[test]
    fn an_entry_never_takes_effect_before_the_one_before_it() {
        let mut sm = machine();
        sm.apply(1, &one("1", "route 0 c", 0, 3000, Some(true)));
        sm.apply(2, &one("2", "fader 0 0.5", 0, 1000, None));
        assert_eq!(sm.records[&1].at_index, 3000);
        assert!(sm.records[&1].generation);
        assert_eq!(sm.records[&2].at_index, 3000);
        assert_eq!(gains(&sm.records[&2]), [(0, 0, 0.5f32.to_bits())]);
    }

    /// A restamp re-asserts every target of the state at its own place in
    /// the log, with the routes of the entry it names: a value set after
    /// that entry is not undone, and an entry after the restamp applies at
    /// or after it. A second decision on the entry derives nothing.
    #[test]
    fn a_restamp_reasserts_the_state_at_its_place() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
        sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
        let restamp = |at_index| Request {
            stamp: 96,
            body: Body::Restamp {
                revision: 10,
                term: 1,
                at_index,
                paths: Vec::new(),
                generation: None,
            },
        };
        sm.apply_at(12, 1, &restamp(4000));
        let r = &sm.records[&12];
        assert_eq!(r.at_index, 4000);
        assert!(r.generation);
        assert!(r.entries.contains(&Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.2,
            fade: None
        }));
        assert!(r.entries.contains(&Entry::Route {
            slot: 0,
            flow_id: "c".into()
        }));
        assert!(
            !r.entries
                .iter()
                .any(|e| matches!(e, Entry::Route { slot: 1, .. }))
        );
        sm.apply_at(13, 1, &one("3", "fader 1 0.5", 96, 1000, None));
        assert_eq!(sm.records[&13].at_index, 4000);
        sm.apply_at(14, 1, &restamp(5000));
        assert!(!sm.records.contains_key(&14));
    }

    /// A restamp clears a duck mask only where one was derived: the
    /// coefficient a command masked and another cleared gets its 0
    /// again, one never masked gets none.
    #[test]
    fn a_restamp_clears_only_the_masks_derived() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "fader 0 0.2", 0, 3000, None));
        sm.apply_at(11, 1, &one("2", "duck 1", 0, 3000, None));
        sm.apply_at(12, 1, &one("3", "duck 0", 0, 3000, None));
        let restamp = Request {
            stamp: 96,
            body: Body::Restamp {
                revision: 10,
                term: 1,
                at_index: 4000,
                paths: Vec::new(),
                generation: None,
            },
        };
        sm.apply_at(13, 1, &restamp);
        let masks: Vec<&Entry> = (sm.records[&13].entries.iter())
            .filter(|e| matches!(e, Entry::DuckMask { .. }))
            .collect();
        assert_eq!(
            masks,
            [&Entry::DuckMask {
                input: 1,
                output: 0,
                mask: 0
            }]
        );
    }

    /// A restamp of an entry that derived nothing derives nothing.
    #[test]
    fn a_restamp_of_an_entry_that_derived_nothing_derives_nothing() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "noop", 0, 3000, None));
        sm.apply_at(
            11,
            1,
            &Request {
                stamp: 96,
                body: Body::Restamp {
                    revision: 10,
                    term: 1,
                    at_index: 4000,
                    paths: Vec::new(),
                    generation: None,
                },
            },
        );
        assert!(!sm.records.contains_key(&11));
    }

    /// An entry asking for an index further than `AHEAD_MAX` past its
    /// stamp, as a writer that kept a far client index orders it, moves
    /// the floor to its stamp only.
    #[test]
    fn a_far_index_does_not_move_the_floor_past_its_stamp() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 960, 960 + 60 * 48_000, None));
        assert_eq!(sm.floor, 960);
        assert_eq!(sm.records[&1].at_index, 960);
    }

    /// A snapshot written before the floor was kept loads with the highest
    /// index an entry still decidable takes effect at; one with the floor
    /// loads it.
    #[test]
    fn a_snapshot_without_a_floor_loads_the_highest_decidable_index() {
        let mut sm = machine();
        sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 3000, None));
        sm.apply_at(2, 1, &one("2", "fader 0 0.25", 0, 2000, None));
        let mut saved = sm.save();
        assert_eq!(saved.floor, Some(3000));
        let mut loaded = machine();
        loaded
            .load(saved.clone(), Limits::defaults(48_000))
            .unwrap();
        assert_eq!(loaded.floor, 3000);
        saved.floor = None;
        let mut legacy = machine();
        legacy.load(saved, Limits::defaults(48_000)).unwrap();
        assert_eq!(legacy.floor, 3000);
        // The next entry derives the same on both.
        let next = one("3", "vca 0.5", 96, 1000, None);
        loaded.apply_at(3, 1, &next);
        legacy.apply_at(3, 1, &next);
        assert_eq!(loaded.records, legacy.records);
    }

    /// A command that would re-route, ordered off the generation path, is
    /// refused by the product, which reads the flag it was ordered with;
    /// one from a writer that did not classify applies.
    #[test]
    fn the_product_reads_how_a_command_was_ordered() {
        let mut sm = machine();
        sm.apply(1, &one("1", "route 0 c", 0, 960, Some(false)));
        assert!(!sm.records.contains_key(&1));
        assert_eq!(sm.product.flows[0], "a");
        sm.apply(2, &one("2", "route 0 c", 0, 960, None));
        assert!(sm.records[&2].generation);
        assert_eq!(sm.product.flows[0], "c");
    }

    /// Floats survive a snapshot bit for bit: serde_json parses each one
    /// correctly rounded.
    #[test]
    fn product_floats_survive_a_snapshot_bit_for_bit() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let values: Vec<f64> = (0..20_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                f64::from_bits(x).abs() % 10.0
            })
            .filter(|v| v.is_finite())
            .collect();
        let text = serde_json::to_string(&values).unwrap();
        let back: Vec<f64> = serde_json::from_str(&text).unwrap();
        let differ = values
            .iter()
            .zip(&back)
            .filter(|(a, b)| a.to_bits() != b.to_bits())
            .count();
        assert_eq!(differ, 0, "{differ} of {} values moved", values.len());
    }

    /// A legacy snapshot whose decidable entries include a far client
    /// index an older writer kept loads the floor a replica applying the
    /// whole log holds, which counted that entry at its stamp.
    #[test]
    fn a_legacy_floor_skips_a_far_client_index() {
        let mut sm = machine();
        sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 3000, None));
        sm.apply_at(2, 1, &one("2", "fader 0 0.25", 0, 60 * 48_000, None));
        assert_eq!(sm.floor, 3000);
        let mut saved = sm.save();
        saved.floor = None;
        let mut legacy = machine();
        legacy.load(saved, Limits::defaults(48_000)).unwrap();
        assert_eq!(legacy.floor, sm.floor);
    }

    /// An `InputDown` or `InputUp` derives the input's state at its index,
    /// raised to the floor; only the `InputUp` builds a generation.
    #[test]
    fn input_down_and_up_derive_the_input_state() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 0, 3000, None));
        let input = |body| Request { stamp: 0, body };
        sm.apply(
            2,
            &input(Body::InputDown {
                slot: 1,
                at_index: 960,
            }),
        );
        let down = &sm.records[&2];
        assert_eq!(down.entries, [Entry::InputDown { slot: 1 }]);
        assert_eq!(down.at_index, 3000);
        assert!(!down.generation);
        sm.apply(
            3,
            &input(Body::InputUp {
                slot: 1,
                at_index: 4800,
            }),
        );
        let up = &sm.records[&3];
        assert_eq!(up.entries, [Entry::InputUp { slot: 1 }]);
        assert_eq!(up.at_index, 4800);
        assert!(up.generation);
    }

    /// A void or lease expiry from an older leader, which stamps none:
    /// the void applies as that leader's binary applies it, dropping the
    /// entry and its `derived` bit, deriving nothing and leaving the floor
    /// where it was; the expiry derives [`FALLBACK_LEAD`] past its stamp,
    /// so its record can still reach the engines in time. One the leader
    /// stamped derives at its own index.
    #[test]
    fn an_unstamped_void_or_expiry_applies_as_an_older_binary_does() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
        assert_eq!(sm.derived.get(&10), Some(&vec![0]));
        sm.apply_at(11, 1, &void(10, 4800, None));
        assert!(!sm.derived.contains_key(&10));
        assert!(!sm.terms.contains_key(&10));
        assert!(!sm.terms.contains_key(&11), "an unstamped void is final");
        assert!(!sm.records.contains_key(&11));
        assert_eq!(sm.floor, 3000);

        sm.apply_at(12, 1, &one("2", "solo", 0, 3000, None));
        let expiry = |stamp, at_index| Request {
            stamp,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 0,
                at_index,
            },
        };
        sm.apply_at(13, 1, &expiry(4800, None));
        assert_eq!(sm.records[&13].at_index, 4800 + FALLBACK_LEAD);
        assert_eq!(sm.floor, 4800 + FALLBACK_LEAD);

        sm.apply_at(14, 1, &one("3", "route 1 d", 0, 7200, Some(true)));
        sm.apply_at(15, 1, &void(14, 7200, Some(9600)));
        assert_eq!(sm.records[&15].at_index, 9600);
        assert_eq!(sm.floor, 9600);
    }

    fn void(of: u64, stamp: u64, at_index: Option<u64>) -> Request {
        Request {
            stamp,
            body: Body::Void {
                revision: of,
                term: 1,
                at_index,
                generation: Some(true),
            },
        }
    }

    /// A void of a product entry re-asserts the state at the void, on the
    /// generation path when the entry re-routed: the engine plays the
    /// voided command's effect, which the state holds. One of an id not
    /// held derives nothing.
    #[test]
    fn a_void_reasserts_the_state_at_its_index() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
        sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 3000, None));
        sm.apply_at(12, 1, &void(10, 96, Some(4800)));
        let r = &sm.records[&12];
        assert_eq!(r.at_index, 4800);
        assert!(r.generation);
        assert!(r.entries.contains(&Entry::Route {
            slot: 0,
            flow_id: "c".into()
        }));
        assert!(r.entries.contains(&Entry::Gain {
            input: 0,
            output: 0,
            gain: 0.2,
            fade: None
        }));
        assert!(void(10, 0, None).body.needs_generation());

        sm.apply_at(13, 1, &void(99, 96, Some(6000)));
        assert!(!sm.records.contains_key(&13));
    }

    /// A stamped void can go unresolved itself: a restamp of it re-asserts
    /// again, and the restamps behind the voided entry count against it.
    #[test]
    fn a_stamped_void_is_decided_like_the_entry() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
        sm.restamps.insert(10, (2, 3000));
        sm.apply_at(11, 1, &void(10, 96, Some(4800)));
        assert_eq!(sm.terms.get(&11), Some(&(1, 4800)));
        assert_eq!(sm.restamps.get(&11), Some(&(2, 4800)));
        let restamp = Request {
            stamp: 96,
            body: Body::Restamp {
                revision: 11,
                term: 1,
                at_index: 6000,
                paths: Vec::new(),
                generation: Some(true),
            },
        };
        sm.apply_at(12, 1, &restamp);
        assert_eq!(sm.records[&12].at_index, 6000);
        assert!(sm.records[&12].entries.contains(&Entry::Route {
            slot: 0,
            flow_id: "c".into()
        }));
        assert_eq!(sm.restamps.get(&12).map(|r| r.0), Some(3));
    }

    /// A void of a pending engine body drops it: it derives nothing and
    /// is not decidable. A void with the most restamps behind its entry
    /// re-asserts and is final.
    #[test]
    fn a_void_of_an_engine_body_or_a_late_entry_is_final() {
        let mut sm = machine();
        let up = Body::InputUp {
            slot: 1,
            at_index: 960,
        };
        sm.apply_at(10, 1, &Request { stamp: 0, body: up });
        sm.apply_at(11, 1, &void(10, 96, Some(4800)));
        assert!(!sm.terms.contains_key(&11));
        assert!(!sm.records.contains_key(&11));
        sm.apply_at(12, 1, &one("1", "route 0 c", 0, 4800, Some(true)));
        sm.restamps.insert(12, (crate::decide::RESTAMPS_MAX, 4800));
        sm.apply_at(13, 1, &void(12, 96, Some(6000)));
        assert_eq!(sm.records[&13].at_index, 6000);
        assert!(!sm.terms.contains_key(&13));
        assert!(!sm.restamps.contains_key(&13));
    }

    /// A restamp of an `InputUp` or `InputDown` derives it at the
    /// restamp's index, so the engine raises the input there.
    #[test]
    fn a_restamped_input_state_derives_it_at_the_restamp() {
        let mut sm = machine();
        let restamp = |revision, at_index| Request {
            stamp: 96,
            body: Body::Restamp {
                revision,
                term: 1,
                at_index,
                paths: Vec::new(),
                generation: None,
            },
        };
        for (n, body) in [
            Body::InputUp {
                slot: 1,
                at_index: 960,
            },
            Body::InputDown {
                slot: 0,
                at_index: 960,
            },
        ]
        .into_iter()
        .enumerate()
        {
            let at = 10 + 2 * n as u64;
            sm.apply_at(at, 1, &Request { stamp: 0, body });
            sm.apply_at(at + 1, 1, &restamp(at, 2000));
            let r = &sm.records[&(at + 1)];
            assert_eq!(r.at_index, 2000);
            let want = match n {
                0 => Entry::InputUp { slot: 1 },
                _ => Entry::InputDown { slot: 0 },
            };
            assert_eq!(r.entries, [want]);
            assert_eq!(r.generation, n == 0);
        }
    }

    /// A lease expiry the leader stamped derives at its own index.
    #[test]
    fn a_stamped_lease_expiry_derives_at_its_index() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "solo", 0, 960, None));
        let expiry = Request {
            stamp: 96,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 0,
                at_index: Some(2400),
            },
        };
        sm.apply_at(11, 1, &expiry);
        assert_eq!(sm.records[&11].at_index, 2400);
        assert_eq!(sm.floor, 2400);
    }

    /// Two input domains: a command re-routing slots of both is refused
    /// with any flag, and one re-routing off the generation path too; every
    /// machine refuses alike, derives nothing and still moves the floor.
    #[test]
    fn a_rerouting_command_off_its_path_or_across_domains_is_refused_everywhere() {
        let log = [
            one("1", "pair c d", 0, 960, Some(true)),
            one("2", "route 0 c", 0, 1920, Some(false)),
        ];
        for mut sm in [machine(), machine()] {
            for (n, request) in log.iter().enumerate() {
                let response = sm.apply_at(10 + n as u64, 1, request);
                assert_eq!(response.0[0].result, Resolution::Unchanged);
            }
            assert!(sm.records.is_empty());
            assert_eq!(sm.floor, 1920);
            assert_eq!(sm.product.flows, ["a".to_owned(), "b".to_owned()]);
        }
    }

    /// The pending generation is the index of the latest decidable entry
    /// that re-routed an input, under the restamp that moved it; one that
    /// re-routed nothing needs none.
    #[test]
    fn the_pending_generation_is_the_latest_rerouting_entry() {
        let mut sm = machine();
        assert_eq!(sm.pending_generation(), None);
        sm.apply_at(10, 1, &one("1", "route 0 c", 0, 3000, Some(true)));
        sm.apply_at(11, 1, &one("2", "fader 0 0.2", 0, 5000, None));
        assert!(sm.derived.contains_key(&11));
        assert_eq!(sm.pending_generation(), Some(3000));
        let restamp = Request {
            stamp: 96,
            body: Body::Restamp {
                revision: 10,
                term: 1,
                at_index: 6000,
                paths: Vec::new(),
                generation: Some(true),
            },
        };
        sm.apply_at(12, 1, &restamp);
        assert_eq!(sm.pending_generation(), Some(6000));
    }

    /// `Body::Protocol` raises the replicated protocol, never lowers it,
    /// and a snapshot carries it; one written before it was kept loads at
    /// 0.
    #[test]
    fn the_protocol_marker_is_replicated() {
        let mut sm = machine();
        let marker = |p| Request {
            stamp: 0,
            body: Body::Protocol(p),
        };
        sm.apply_at(1, 1, &marker(2));
        sm.apply_at(2, 1, &marker(1));
        assert_eq!(sm.protocol, 2);
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        assert_eq!(loaded.protocol, 2);
        let mut legacy = serde_json::to_value(sm.save()).unwrap();
        legacy.as_object_mut().unwrap().remove("protocol");
        let legacy: Replicated = serde_json::from_value(legacy).unwrap();
        loaded
            .load(legacy, Limits::defaults(48_000))
            .expect("loads");
        assert_eq!(loaded.protocol, 0);
    }

    /// The ramps the derived gain entries left running are replicated state
    /// of the log: a replica that loads a snapshot carries the same
    /// `fading` in its product state, and one written before they were
    /// kept loads with none.
    #[test]
    fn a_snapshot_carries_the_ramps_of_plain_gain_entries() {
        let mut sm = machine();
        sm.apply_at(1, 1, &one("1", "fader 0 0.5", 0, 960, None));
        sm.apply_at(2, 1, &one("2", "fader 0 0.75", 48, 1008, None));
        let want = sm.product_state(2).fading;
        assert!(want[0][0].is_some());
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000))
            .expect("loads");
        assert_eq!(loaded.product_state(2).fading, want);
        // Absent from a snapshot written before: the serde default.
        let legacy = Replicated {
            ramps: Vec::new(),
            ..sm.save()
        };
        loaded
            .load(legacy, Limits::defaults(48_000))
            .expect("loads");
        assert_ne!(loaded.product_state(2).fading, want);
    }

    /// A record is kept until its index lies [`RECORDS_KEPT`] below the
    /// highest stamp applied, and one still ahead stays.
    #[test]
    fn records_are_kept_for_records_kept_past_their_index() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
        sm.apply(2, &one("2", "fader 1 0.5", 40_000, 88_000, None));
        let renew = |stamp| Request {
            stamp,
            body: Body::LeaseRenew(Vec::new()),
        };
        sm.apply(3, &renew(960 + RECORDS_KEPT));
        assert!(sm.records.contains_key(&1));
        sm.apply(4, &renew(960 + RECORDS_KEPT + 48));
        assert!(!sm.records.contains_key(&1));
        assert!(sm.records.contains_key(&2));
    }

    /// A lease expiry stamped below the floor derives at the floor, after
    /// the entry before it.
    #[test]
    fn a_lease_expiry_below_the_floor_derives_at_the_floor() {
        let mut sm = machine();
        sm.apply(1, &one("1", "solo", 0, 9600, None));
        sm.apply(
            2,
            &Request {
                stamp: 4800,
                body: Body::LeaseExpiry {
                    session: origin("s").key(),
                    renewed: 0,
                    at_index: None,
                },
            },
        );
        assert_eq!(sm.records[&2].at_index, 9600);
        assert_eq!(sm.floor, 9600);
    }

    /// A machine that loads a snapshot taken mid-log holds the targets of
    /// the one that applied the whole log at that point, and derives the
    /// same records from every later entry.
    #[test]
    fn a_machine_loaded_mid_log_derives_what_the_whole_log_derives() {
        let log = [
            one("1", "fader 0 0.5", 0, 960, None),
            one("2", "route 1 c", 48, 1008, None),
            one("3", "vca 0.5", 96, 1056, None),
            one("4", "solo", 144, 1104, None),
            one("5", "trim 0 -3.5", 192, 1152, None),
            one("6", "route 0 d", 240, 1200, Some(true)),
            Request {
                stamp: 288,
                body: Body::LeaseExpiry {
                    session: origin("s").key(),
                    renewed: 240,
                    at_index: None,
                },
            },
        ];
        let mut whole = machine();
        for (n, r) in log.iter().enumerate().take(3) {
            whole.apply_at(n as u64 + 1, 1, r);
        }
        let mut loaded = machine();
        loaded.load(whole.save(), Limits::defaults(48_000)).unwrap();
        assert_eq!(loaded.product.targets(), whole.product.targets());
        for (n, r) in log.iter().enumerate().skip(3) {
            whole.apply_at(n as u64 + 1, 1, r);
            loaded.apply_at(n as u64 + 1, 1, r);
        }
        let later = |sm: &StateMachine<Desk>| {
            sm.records
                .range(4..)
                .map(|(&i, r)| (i, r.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(later(&loaded).len(), 4);
        assert_eq!(later(&loaded), later(&whole));
        assert_eq!(loaded.floor, whole.floor);
    }

    /// A re-routing command from a writer that did not classify it
    /// applies and derives its route like one ordered on the generation
    /// path: two machines, one fed each, agree on the product state.
    #[test]
    fn a_rerouting_command_flagged_none_applies_like_one_flagged_true() {
        let (mut a, mut b) = (machine(), machine());
        a.apply(1, &one("1", "route 0 c", 0, 960, None));
        b.apply(1, &one("1", "route 0 c", 0, 960, Some(true)));
        assert_eq!(a.product.save(), b.product.save());
        assert_eq!(a.records, b.records);
        assert!(a.records[&1].entries.contains(&Entry::Route {
            slot: 0,
            flow_id: "c".into()
        }));
    }

    /// A stamped lease expiry that derived engine entries is decided like
    /// a command: a restamp of it re-asserts the state at the restamp's
    /// index, so a replica that held it unresolved gets its effect.
    #[test]
    fn a_restamp_of_a_lease_expiry_reasserts_its_effect() {
        let mut sm = machine();
        sm.apply_at(10, 1, &one("1", "solo", 0, 960, None));
        let expiry = Request {
            stamp: 96,
            body: Body::LeaseExpiry {
                session: origin("s").key(),
                renewed: 0,
                at_index: Some(2400),
            },
        };
        sm.apply_at(11, 1, &expiry);
        assert!(sm.records.contains_key(&11));
        let restamp = Request {
            stamp: 192,
            body: Body::Restamp {
                revision: 11,
                term: 1,
                at_index: 4800,
                paths: Vec::new(),
                generation: None,
            },
        };
        sm.apply_at(12, 1, &restamp);
        let r = &sm.records[&12];
        assert_eq!(r.at_index, 4800);
        let reasserted = gains(r);
        assert!(
            gains(&sm.records[&11])
                .iter()
                .all(|g| reasserted.contains(g)),
            "the gains after it"
        );
    }

    /// The pending generation of the applied log names an `InputUp` the
    /// state holds, and not a command flagged for its generation that
    /// the product refused, which built none.
    #[test]
    fn the_pending_generation_names_what_built_one() {
        let mut sm = machine();
        sm.apply(1, &one("1", "pair c d", 0, 960, Some(true)));
        assert!(!sm.records.contains_key(&1), "refused");
        assert_eq!(sm.pending_generation(), None);
        let up = Request {
            stamp: 0,
            body: Body::InputUp {
                slot: 1,
                at_index: 4800,
            },
        };
        sm.apply(2, &up);
        assert_eq!(sm.pending_generation(), Some(4800));
    }

    /// The state a core is rebuilt from: the targets, each slot up as its
    /// latest input state held says and up where none is, and the last
    /// log id it folds in.
    #[test]
    fn the_product_state_holds_the_targets_and_the_input_states() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
        let input = |body| Request { stamp: 0, body };
        sm.apply(
            2,
            &input(Body::InputUp {
                slot: 1,
                at_index: 960,
            }),
        );
        sm.apply(
            3,
            &input(Body::InputDown {
                slot: 1,
                at_index: 1920,
            }),
        );
        let state = sm.product_state(3);
        let targets = sm.product.targets().unwrap();
        assert_eq!(state.gains, targets.gains);
        assert_eq!(state.flows, targets.flows);
        assert_eq!(state.strips, targets.strips);
        assert_eq!(state.up, [true, false]);
        assert_eq!(state.last_id, 3);
        assert!(state.pending.is_empty());
    }

    /// Records go once retention passes their index, oldest first, and
    /// `records_from` names the last gone: a resync from before it cannot
    /// be replayed.
    #[test]
    fn records_from_names_the_last_record_retention_dropped() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
        sm.apply(2, &one("2", "fader 0 0.25", 0, 1920, None));
        assert_eq!(sm.records_from, 0);
        sm.apply(
            3,
            &one(
                "3",
                "fader 0 0.75",
                1920 + RECORDS_KEPT,
                1920 + RECORDS_KEPT,
                None,
            ),
        );
        assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [2, 3]);
        assert_eq!(sm.records_from, 1);
        // The record dropped is the newest at or below `records_from`: a
        // replay from before it misses it.
        assert_eq!(sm.record_floor, Some(1));
        assert!(!sm.records_whole(0));
        assert!(sm.records_whole(1));
    }

    /// A machine loaded from a snapshot taken after entries that derived
    /// no record holds every record after the newest that did, though
    /// `records_from` is the snapshot's index: a replay from that record
    /// on misses nothing, one from before it does. A snapshot written
    /// before the floor was kept leaves it unknown, and only
    /// `records_from` counts.
    #[test]
    fn a_snapshot_carries_the_newest_record_as_the_floor() {
        let mut sm = machine();
        sm.apply(1, &one("1", "fader 0 0.5", 0, 960, None));
        for index in 2..=5 {
            sm.apply(
                index,
                &Request {
                    stamp: 960 + 48 * index,
                    body: Body::LeaseRenew(Vec::new()),
                },
            );
        }
        assert_eq!(sm.records.keys().copied().collect::<Vec<_>>(), [1]);
        let saved = sm.save();
        assert_eq!(saved.record_floor, Some(1));
        let load = |saved: Replicated| {
            let mut loaded = machine();
            loaded.load(saved, Limits::defaults(48_000)).expect("loads");
            // As the store sets it.
            loaded.records_from = 5;
            loaded
        };
        let loaded = load(saved.clone());
        assert_eq!(loaded.record_floor, Some(1));
        assert!(!loaded.records_whole(0), "the record at 1 is not held");
        for after in 1..=5 {
            assert!(loaded.records_whole(after), "after {after}");
        }
        // As bytes: a JSON value holds no u128 command id.
        let bytes = String::from_utf8(serde_json::to_vec(&saved).expect("encodes")).unwrap();
        let field = "\"record_floor\":1,";
        assert!(bytes.contains(field), "{bytes}");
        let legacy: Replicated = serde_json::from_str(&bytes.replace(field, "")).expect("decodes");
        assert_eq!(legacy.record_floor, None);
        let loaded = load(legacy);
        assert!(!loaded.records_whole(4), "an unknown floor");
        assert!(loaded.records_whole(5));
    }
}

/// The engine tests' helpers (flows, a pilot, configurations).
#[cfg(test)]
#[allow(unused)]
#[path = "../../../tests/common/mod.rs"]
mod ramp_common;

/// The state machine and a real engine together: a plain gain change, twice
/// in a chain, derived by the state machine and applied live by one
/// engine, and a second rebuilt from the state machine's product state
/// inside the ramp, compute the same pages.
#[cfg(test)]
mod ramp_tests {
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
}
