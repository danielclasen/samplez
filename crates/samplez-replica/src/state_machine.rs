//! Applying entries: dedup, the product's state, and the replicated state
//! the replica keeps beside it.

use std::collections::{BTreeMap, VecDeque};

use crate::dedup::{Dedup, Limits};
use crate::entry::{
    Baseline, Body, Command, Disruption, NodeId, Origin, Outcome, Request, Resolution, Response,
    SessionKey,
};
use crate::snapshot::Replicated;

/// The product's state, applied in log order on every replica.
pub trait Product: Send + 'static {
    /// Applies one command. A command whose second application could undo
    /// a later one's effect carries the ordering its apply needs and is
    /// refused as [`Effect::Stale`] when older, with no effect: a copy can
    /// apply again once its dedup record retired.
    fn apply(&mut self, origin: &Origin, command: &[u8]) -> Effect;

    /// Ends what `session` held under its lease, such as touch ownership.
    fn expire(&mut self, session: &SessionKey);

    /// The whole state as the product's clients read it.
    fn text(&self) -> String;

    /// The state as snapshots carry it.
    fn save(&self) -> Vec<u8>;

    /// Replaces the state with a saved one.
    fn load(&mut self, saved: &[u8]) -> Result<(), String>;

    /// The path a command sets a continuous value on, so a later command
    /// on the same path in one entry supersedes it; `None` for commands
    /// that must each apply.
    fn coalesce_path(_command: &[u8]) -> Option<String> {
        None
    }
}

/// How the product applied a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Changed, with the delta its clients get.
    Changed(String),
    Unchanged,
    Stale,
}

/// A product delta at the revision that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub revision: u64,
    pub prev: u64,
    pub command: String,
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
    /// The revision of the last product delta.
    pub last_change: u64,
    /// Not replicated: the deltas since the last snapshot install or
    /// start, for [`samplez_service::ReplicatedState::deltas_since`].
    pub deltas: VecDeque<Change>,
    /// `deltas` holds every delta after this revision.
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
            last_change: 0,
            deltas: VecDeque::new(),
            deltas_after: 0,
        }
    }

    /// Applies the request of the entry at `revision`.
    pub fn apply(&mut self, revision: u64, request: &Request) -> Response {
        let stamp = request.stamp;
        match &request.body {
            Body::Commands(commands) => Response(
                commands
                    .iter()
                    .map(|c| self.command(revision, stamp, c))
                    .collect(),
            ),
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
                    self.leases.insert(session.clone(), stamp);
                }
                Response::default()
            }
            Body::LeaseExpiry { session, renewed } => {
                // A renewal after the one the expiry was decided on keeps
                // the lease: the expiry is a no-op.
                if self.leases.get(session).is_some_and(|at| at <= renewed) {
                    self.leases.remove(session);
                    self.product.expire(session);
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

    fn command(&mut self, revision: u64, stamp: u64, command: &Command) -> Outcome {
        let session = command.origin.key();
        // A command renews its session's lease.
        self.leases.insert(session.clone(), stamp);
        if self
            .dedup
            .record(&session, &command.id, revision, stamp)
            .is_some()
        {
            let (revision, result) = self.dedup.answer(&session, &command.id).expect("held");
            return Outcome { revision, result };
        }
        let Some(body) = &command.body else {
            self.dedup
                .resolve(&session, &command.id, Resolution::Superseded);
            return Outcome {
                revision,
                result: Resolution::Superseded,
            };
        };
        let result = match self.product.apply(&command.origin, body) {
            Effect::Changed(text) => {
                if self.deltas.len() == DELTAS_HELD
                    && let Some(dropped) = self.deltas.pop_front()
                {
                    self.deltas_after = dropped.revision;
                }
                self.deltas.push_back(Change {
                    revision,
                    prev: self.last_change,
                    command: command.id.clone(),
                    text,
                });
                self.last_change = revision;
                Resolution::Applied
            }
            Effect::Unchanged => Resolution::Applied,
            Effect::Stale => Resolution::Stale,
        };
        self.dedup.resolve(&session, &command.id, result);
        Outcome { revision, result }
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
            last_change: self.last_change,
        }
    }

    /// Replaces the state with a snapshot's, taken at `revision`.
    pub fn load(&mut self, saved: Replicated, limits: Limits, revision: u64) -> Result<(), String> {
        self.product.load(&saved.product)?;
        self.dedup = Dedup::load(limits, saved.dedup);
        self.pending = saved.pending;
        self.baseline = saved.baseline;
        self.verdicts = saved.verdicts;
        self.slot = saved.slot;
        self.leases = saved.leases.into_iter().collect();
        self.last_change = saved.last_change;
        self.deltas.clear();
        self.deltas_after = revision;
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
        fn expire(&mut self, session: &SessionKey) {
            self.expired.push(session.clone());
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
        fn coalesce_path(command: &[u8]) -> Option<String> {
            Some(parse(command).0)
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
            body: Some(body.as_bytes().to_vec()),
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

    /// A copy resolves as its first copy did, from the table a snapshot
    /// carries as well: stale stays stale at its revision, superseded stays
    /// superseded and an applied command is a duplicate.
    #[test]
    fn a_copy_resolves_as_its_first_copy_also_after_a_snapshot_load() {
        let mut sm = machine();
        sm.apply(1, &commands(48, vec![command("s", "8", "gain=8@8")]));
        let mut batch = Batch::default();
        for (id, body) in [("6", "pan=6@6"), ("9", "pan=9@9")] {
            let c = command("s", id, body);
            let path = Paths::coalesce_path(c.body.as_deref().expect("body"));
            batch.push(c, path);
        }
        sm.apply(2, &commands(96, batch.into_commands()));
        let first = sm.apply(3, &commands(144, vec![command("s", "7", "gain=7@7")]));
        assert_eq!(first.0[0].result, Resolution::Stale);
        let mut loaded = machine();
        loaded
            .load(sm.save(), Limits::defaults(48_000), 3)
            .expect("loads");
        for machine in [&mut sm, &mut loaded] {
            let copies = machine.apply(
                4,
                &commands(
                    192,
                    vec![
                        command("s", "7", "gain=7@7"),
                        command("s", "8", "gain=8@8"),
                        command("s", "6", "pan=6@6"),
                    ],
                ),
            );
            let results: Vec<_> = copies.0.iter().map(|o| (o.revision, o.result)).collect();
            assert_eq!(
                results,
                [
                    (3, Resolution::Stale),
                    (1, Resolution::Duplicate),
                    (2, Resolution::Superseded)
                ]
            );
        }
    }

    #[test]
    fn a_coalesced_entry_acknowledges_every_merged_id() {
        let mut batch = Batch::default();
        for (session, id, body) in [
            ("s", "1", "gain=1@1"),
            ("t", "1", "gain=2@2"),
            ("s", "2", "pan=0@3"),
        ] {
            let c = command(session, id, body);
            let path = Paths::coalesce_path(c.body.as_deref().expect("body"));
            batch.push(c, path);
        }
        let mut sm = machine();
        let response = sm.apply(5, &commands(48, batch.into_commands()));
        let results: Vec<_> = response.0.iter().map(|o| (o.revision, o.result)).collect();
        assert_eq!(
            results,
            [
                (5, Resolution::Superseded),
                (5, Resolution::Applied),
                (5, Resolution::Applied)
            ]
        );
        assert_eq!(sm.product.applied, 2);
        assert_eq!(sm.product.values["gain"].0, "2");
        // The superseded id is recorded: a resend of it changes nothing and
        // is told it was superseded.
        let resend = sm.apply(6, &commands(96, vec![command("s", "1", "gain=1@1")]));
        assert_eq!(resend.0[0].result, Resolution::Superseded);
        assert_eq!(resend.0[0].revision, 5);
        assert_eq!(sm.product.values["gain"].0, "2");
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
                revision: 5,
                result: Resolution::Stale
            }
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
