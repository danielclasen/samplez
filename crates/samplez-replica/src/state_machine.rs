//! Applying entries: dedup, the product's state, and the replicated state
//! the replica keeps beside it.

use std::collections::{BTreeMap, VecDeque};

use crate::dedup::{Dedup, Limits};
use crate::entry::{
    Baseline, Body, Command, Disruption, NodeId, Origin, Outcome, Request, Resolution, Response,
    SessionKey,
};
use crate::snapshot::Replicated;

/// The product's state, applied in log order on every replica. A
/// snapshot install loads into an empty one its [`Factory`] builds, with
/// the replica's configuration, before it replaces the state.
pub trait Product: Send + 'static {
    /// What one command or expiry changed, as [`Product::delta`] gets it
    /// back.
    type Change;

    /// Applies one command. A command whose second application could undo
    /// a later one's effect carries the ordering its apply needs and is
    /// refused as [`Effect::Stale`] when older, with no effect: a copy can
    /// apply again once its dedup record retired.
    fn apply(&mut self, origin: &Origin, command: &[u8]) -> Effect<Self::Change>;

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
    /// In apply order, so at the same point on every replica.
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

    /// The state as snapshots carry it.
    fn save(&self) -> Vec<u8>;

    /// Replaces the state with a saved one.
    fn load(&mut self, saved: &[u8]) -> Result<(), String>;

    /// The product paths `command` sets. The leader restamps an
    /// unresolved entry only for the paths no later committed entry sets,
    /// and voids it when none is left, so a restamp never undoes a later
    /// value.
    fn paths(&self, command: &[u8]) -> Vec<String>;

    /// A hash of the configuration the product was built with that
    /// changes how it applies a command or loads a saved state, 0 for a
    /// product without one. Every member of a set must hold equal
    /// configuration: product state is shared by every replica, so a
    /// member that applied a command under another configuration would
    /// hold another state. A snapshot from a replica with another hash is
    /// not installed, and the leader orders no client command while a
    /// member reports another one.
    fn config_hash(&self) -> u64;

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
        }
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
        let response = self.apply_body(revision, term, request);
        self.prune();
        response
    }

    fn apply_body(&mut self, revision: u64, term: u64, request: &Request) -> Response {
        let stamp = request.stamp;
        match &request.body {
            Body::Commands(commands) => self.commands(stamp, commands),
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
                }
                Response::default()
            }
            Body::Void {
                revision: of,
                term: of_term,
            } => {
                if self.terms.get(of).is_some_and(|&(t, _)| t == *of_term) {
                    self.pending.remove(of);
                    self.restamps.remove(of);
                    self.terms.remove(of);
                }
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
            Body::LeaseExpiry { session, renewed } => {
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
    }

    /// Applies every command of an entry, then makes its changes one delta
    /// at the next product revision. Each command resolves at the revision
    /// after the entry: the entry's when it changed the product, the one
    /// before when it did not.
    fn commands(&mut self, stamp: u64, commands: &[Command]) -> Response {
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
            let result = match self.product.apply(&command.origin, &command.body) {
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
        Ok(())
    }
}

pub(crate) fn restamp(body: &mut Body, to: u64) {
    match body {
        Body::InputDown { at_index, .. }
        | Body::InputUp { at_index, .. }
        | Body::Route { at_index, .. }
        | Body::MasterEnable { at_index, .. }
        | Body::Restamp { at_index, .. } => *at_index = to,
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
    }

    pub fn parse(command: &[u8]) -> (String, String, u64) {
        let text = std::str::from_utf8(command).expect("utf-8");
        let (path, rest) = text.split_once('=').expect("path=value@seq");
        let (value, seq) = rest.split_once('@').expect("value@seq");
        (path.into(), value.into(), seq.parse().expect("seq"))
    }

    impl Product for Paths {
        type Change = (String, String);

        fn apply(&mut self, origin: &Origin, command: &[u8]) -> Effect<(String, String)> {
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
        fn paths(&self, command: &[u8]) -> Vec<String> {
            vec![parse(command).0]
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
            origin: origin(session),
            id: id.into(),
            at_index: None,
            body: body.as_bytes().to_vec(),
        }
    }

    fn machine() -> StateMachine<Paths> {
        StateMachine::new(Paths::default(), Limits::defaults(48_000))
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
