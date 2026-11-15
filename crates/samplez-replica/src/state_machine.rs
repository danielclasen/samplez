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
/// snapshot install loads into a [`Default`] one before it replaces the
/// state.
pub trait Product: Default + Send + 'static {
    /// Applies one command. A command whose second application could undo
    /// a later one's effect carries the ordering its apply needs and is
    /// refused as [`Effect::Stale`] when older, with no effect: a copy can
    /// apply again once its dedup record retired.
    fn apply(&mut self, origin: &Origin, command: &[u8]) -> Effect;

    /// Ends what `session` held under its lease, such as touch ownership:
    /// [`Effect::Changed`] when that changed the state.
    fn expire(&mut self, session: &SessionKey) -> Effect;

    /// The one delta clients get for one entry's changes, in the order
    /// applied: a command's with its origin and id, a server-side one, such
    /// as a lease expiry, without. Where two change the same value the later
    /// one wins. Called with the state after the entry, and only when
    /// something changed.
    fn delta(&self, changes: &[Changed<'_>]) -> String;

    /// The whole state as the product's clients read it.
    fn text(&self) -> String;

    /// The state as snapshots carry it.
    fn save(&self) -> Vec<u8>;

    /// Replaces the state with a saved one.
    fn load(&mut self, saved: &[u8]) -> Result<(), String>;
}

/// How the product applied a command or an expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Changed, with the product's description of the change, which
    /// [`Product::delta`] gets back.
    Changed(String),
    Unchanged,
    Stale,
}

/// One change of an entry, as [`Product::delta`] gets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Changed<'a> {
    /// The command that made it, `None` for a server-side change.
    pub command: Option<(&'a Origin, &'a str)>,
    /// What [`Effect::Changed`] carried.
    pub change: &'a str,
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

/// Deltas held for clients that resume, the most recent first dropped
/// last.
const DELTAS_HELD: usize = 4096;

pub struct StateMachine<P> {
    pub product: P,
    pub dedup: Dedup,
    /// Entries the engine applies at their at_index, by revision.
    pub pending: BTreeMap<u64, Body>,
    pub baseline: Option<Baseline>,
    pub verdicts: BTreeMap<NodeId, bool>,
    pub slot: Option<Disruption>,
    /// Each session's last renewal, as a stamping index.
    pub leases: BTreeMap<SessionKey, u64>,
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
            baseline: None,
            verdicts: BTreeMap::new(),
            slot: None,
            leases: BTreeMap::new(),
            revision: 0,
            deltas: VecDeque::new(),
            deltas_after: 0,
        }
    }

    /// Applies the request of the entry at log index `revision`.
    pub fn apply(&mut self, revision: u64, request: &Request) -> Response {
        let stamp = request.stamp;
        match &request.body {
            Body::Commands(commands) => self.commands(stamp, commands),
            Body::Baseline(baseline) => {
                self.baseline = Some(baseline.clone());
                Response::default()
            }
            Body::Restamp {
                revision: of,
                at_index,
                ..
            } => {
                if let Some(body) = self.pending.get_mut(of) {
                    restamp(body, *at_index);
                }
                Response::default()
            }
            Body::Void { revision: of } => {
                self.pending.remove(of);
                Response::default()
            }
            Body::ClockVerdict { replica, judged } => {
                self.verdicts.insert(*replica, *judged);
                Response::default()
            }
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
                    if let Effect::Changed(change) = self.product.expire(session) {
                        let changed = Changed {
                            command: None,
                            change: &change,
                        };
                        let text = self.product.delta(&[changed]);
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
                Effect::Unchanged => Resolution::Applied,
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
            let changed: Vec<Changed<'_>> = changes
                .iter()
                .map(|(n, change)| Changed {
                    command: Some((&commands[*n].origin, commands[*n].id.as_str())),
                    change,
                })
                .collect();
            let text = self.product.delta(&changed);
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
        let renewed = self.leases.entry(session.clone()).or_insert(stamp);
        *renewed = (*renewed).max(stamp);
    }

    pub fn save(&self) -> Replicated {
        Replicated {
            product: self.product.save(),
            pending: self.pending.clone(),
            baseline: self.baseline.clone(),
            verdicts: self.verdicts.clone(),
            slot: self.slot.clone(),
            dedup: self.dedup.save(),
            leases: self.leases.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            revision: self.revision,
        }
    }

    /// Replaces the state with a snapshot's.
    pub fn load(&mut self, saved: Replicated, limits: Limits) -> Result<(), String> {
        self.product.load(&saved.product)?;
        self.dedup = Dedup::load(limits, saved.dedup);
        self.pending = saved.pending;
        self.baseline = saved.baseline;
        self.verdicts = saved.verdicts;
        self.slot = saved.slot;
        self.leases = saved.leases.into_iter().collect();
        self.revision = saved.revision;
        self.deltas.clear();
        self.deltas_after = saved.revision;
        Ok(())
    }
}

fn restamp(body: &mut Body, to: u64) {
    match body {
        Body::InputDown { at_index, .. }
        | Body::InputUp { at_index, .. }
        | Body::Route { at_index, .. }
        | Body::MasterEnable { at_index, .. } => *at_index = to,
        _ => {}
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::entry::Batch;
    use samplez_service::Role;

    /// One value per path, set by `path=value@seq`; an older seq on a path
    /// is stale.
    #[derive(Default)]
    pub struct Paths {
        pub values: BTreeMap<String, (String, u64)>,
        pub applied: u64,
        pub expired: Vec<SessionKey>,
    }

    pub fn parse(command: &[u8]) -> (String, String, u64) {
        let text = std::str::from_utf8(command).expect("utf-8");
        let (path, rest) = text.split_once('=').expect("path=value@seq");
        let (value, seq) = rest.split_once('@').expect("value@seq");
        (path.into(), value.into(), seq.parse().expect("seq"))
    }

    impl Product for Paths {
        fn apply(&mut self, _origin: &Origin, command: &[u8]) -> Effect {
            let (path, value, seq) = parse(command);
            if self.values.get(&path).is_some_and(|(_, last)| *last >= seq) {
                return Effect::Stale;
            }
            self.applied += 1;
            self.values.insert(path.clone(), (value.clone(), seq));
            Effect::Changed(format!("{path}={value}"))
        }
        /// Releases what the session held, as a touch ownership.
        fn expire(&mut self, session: &SessionKey) -> Effect {
            self.expired.push(session.clone());
            Effect::Changed(format!("owner={}", self.expired.len()))
        }
        fn delta(&self, changes: &[Changed<'_>]) -> String {
            // Later wins per path; every command that changed something is an
            // origin, a server-side change none.
            let mut values = BTreeMap::new();
            let mut origins = Vec::new();
            for changed in changes {
                let (path, value) = changed.change.split_once('=').expect("path=value");
                values.insert(path, value);
                if let Some((origin, id)) = changed.command {
                    origins.push(format!("{}/{id}", origin.session));
                }
            }
            let values: Vec<String> = values.iter().map(|(p, v)| format!("{p}={v}")).collect();
            format!("{} from {}", values.join(","), origins.join(","))
        }
        fn text(&self) -> String {
            serde_json::to_string(&self.values).expect("encodes")
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
                    at_index: 1920,
                    paths: vec![],
                },
            },
        );
        sm.apply(
            4,
            &Request {
                stamp: 0,
                body: Body::Void { revision: 2 },
            },
        );
        assert_eq!(sm.pending.len(), 1);
        assert_eq!(sm.pending[&1].at_index(), Some(1920));
    }
}
