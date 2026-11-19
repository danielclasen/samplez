//! A product that drives an engine on an in-process set with contact
//! running: how the writer classifies, orders and refuses its commands.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Set, owner};
use samplez::Targets;
use samplez_replica::contact::{Timing, index_at};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::{Body, Command, NodeId, Origin, Request, SessionKey};
use samplez_replica::membership::{Slot, node_id};
use samplez_replica::peer::{Forwarded, PeerRequest, PeerResponse, Refused};
use samplez_replica::state::ReplicaState;
use samplez_replica::state_machine::{Apply, Changed, Effect, Product, StateMachine};
use samplez_service::ReplicatedState;
use tokio::time::{Instant, sleep};

/// Two input slots into one output: `fader <slot> <gain>` sets a slot's
/// gain, `route <slot> <flow>` the flow it reads, and re-routes it.
struct Desk {
    gains: [f32; 2],
    flows: [String; 2],
}

impl Desk {
    fn new() -> Desk {
        Desk {
            gains: [1.0, 1.0],
            flows: ["a".into(), "b".into()],
        }
    }
}

fn words(command: &[u8]) -> Vec<&str> {
    std::str::from_utf8(command)
        .expect("utf-8")
        .split(' ')
        .collect()
}

impl Product for Desk {
    type Change = ();
    const DRIVES_ENGINE: bool = true;

    fn apply(&mut self, _: &Origin, command: &[u8], _: &Apply) -> Effect<()> {
        match words(command)[..] {
            ["fader", slot, gain] => {
                self.gains[slot.parse::<usize>().unwrap()] = gain.parse().unwrap()
            }
            ["route", slot, flow] => self.flows[slot.parse::<usize>().unwrap()] = flow.into(),
            _ => return Effect::Stale,
        }
        Effect::Changed(())
    }
    fn expire(&mut self, _: &SessionKey) -> Effect<()> {
        Effect::Unchanged
    }
    fn forget(&mut self, _: &SessionKey) {}
    fn delta(&self, _: u64, _: &[Changed<'_, ()>]) -> String {
        self.text()
    }
    fn text(&self) -> String {
        format!(
            "{} {} {} {}",
            self.gains[0], self.gains[1], self.flows[0], self.flows[1]
        )
    }
    fn save(&self) -> Vec<u8> {
        self.text().into_bytes()
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        let w = words(saved);
        let gain = |s: &str| s.parse::<f32>().map_err(|e| e.to_string());
        self.gains = [gain(w[0])?, gain(w[1])?];
        self.flows = [w[2].into(), w[3].into()];
        Ok(())
    }
    fn config_hash(&self) -> u64 {
        0
    }
    fn targets(&self) -> Option<Targets> {
        Some(Targets {
            gains: vec![vec![self.gains[0]], vec![self.gains[1]]],
            flows: self.flows.iter().cloned().map(Some).collect(),
            strips: vec![Vec::new(), Vec::new()],
            ..Targets::default()
        })
    }
    fn reroutes(&self, _: &Origin, command: &[u8]) -> Vec<u32> {
        match words(command)[..] {
            ["route", slot, _] => vec![slot.parse().unwrap()],
            _ => Vec::new(),
        }
    }
}

async fn start() -> Set<Desk> {
    let set = Set::start_with(
        Options {
            contact: Some(Timing::defaults(BLOCK)),
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    sleep(Duration::from_millis(50)).await;
    set
}

/// The `Commands` entries `id`'s log holds, each as its commands.
fn commands(set: &Set<Desk>, id: NodeId) -> Vec<(Request, Vec<Command>)> {
    let mut out = Vec::new();
    set.replica(id).store().entries_after(0, |_, r| {
        if let Body::Commands(commands) = &r.body {
            out.push((r.clone(), commands.clone()));
        }
    });
    out
}

fn bodies(set: &Set<Desk>, id: NodeId) -> Vec<Vec<String>> {
    commands(set, id)
        .into_iter()
        .map(|(_, commands)| {
            commands
                .iter()
                .map(|c| String::from_utf8(c.body.clone()).unwrap())
                .collect()
        })
        .collect()
}

/// One session's commands, a re-routing one between two fader moves,
/// commit in the order submitted: the re-routing one in an entry of its
/// own, not batched with the others nor overtaken by them, at the leader
/// and through a follower alike. Every member speaks protocol 2, so the
/// leader appends the protocol marker once, before the first flag, and
/// flags the re-routing command for its generation and the others not.
#[tokio::test(start_paused = true)]
async fn a_rerouting_command_is_ordered_alone_in_submission_order() {
    let set = start().await;
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    for (id, session) in [(leader, "s"), (follower, "t")] {
        let r = set.replica(id);
        let o = owner(session);
        let (a, b, c) = tokio::join!(
            r.submit(&o, "1", Bytes::from("fader 0 0.5")),
            r.submit(&o, "2", Bytes::from("route 0 c")),
            r.submit(&o, "3", Bytes::from("fader 1 0.25")),
        );
        for result in [a, b, c] {
            result.expect("committed");
        }
    }
    let one = |s: &str| vec![s.to_owned()];
    let expected = [one("fader 0 0.5"), one("route 0 c"), one("fader 1 0.25")];
    assert_eq!(
        bodies(&set, leader),
        [&expected[..], &expected[..]].concat()
    );
    let flags: Vec<Option<bool>> = commands(&set, leader)
        .iter()
        .flat_map(|(_, c)| c)
        .map(|c| c.generation)
        .collect();
    let flagged = [Some(false), Some(true), Some(false)];
    assert_eq!(flags, [flagged, flagged].concat());
    let log = entries(&set, leader);
    let markers: Vec<usize> = (log.iter().enumerate())
        .filter(|(_, b)| matches!(b, Body::Protocol(2)))
        .map(|(n, _)| n)
        .collect();
    let first = log
        .iter()
        .position(|b| matches!(b, Body::Commands(_)))
        .unwrap();
    assert_eq!(markers.len(), 1, "{log:?}");
    assert!(markers[0] < first, "{log:?}");
    assert!(
        set.replica(leader)
            .contact_metrics()
            .unwrap()
            .protocol_gate_open
    );
}

/// Every body `id`'s log holds, in order.
fn entries(set: &Set<Desk>, id: NodeId) -> Vec<Body> {
    let mut out = Vec::new();
    set.replica(id)
        .store()
        .entries_after(0, |_, r| out.push(r.body.clone()));
    out
}

/// A forwarded command carries the flag the leader classified it with,
/// never the forwarding replica's: with the gate open its own, with the
/// gate shut, here with no contact running, none, and no marker.
#[tokio::test(start_paused = true)]
async fn the_leader_flags_a_forwarded_command_itself() {
    let forged = |e| PeerRequest::Forward {
        commands: vec![Forwarded {
            command: Command {
                generation: Some(true),
                origin: Origin::from(&owner("s")),
                id: "1".into(),
                at_index: None,
                body: b"fader 0 0.5".to_vec(),
            },
            received: e,
            within_ms: 1000,
        }],
    };
    let open = start().await;
    let shut = Set::start_with(Options::default(), Arc::new(|_| Desk::new())).await;
    for (set, want) in [(&open, Some(false)), (&shut, None)] {
        let leader = set.leader().await;
        let e = set
            .replica(leader)
            .contact_metrics()
            .and_then(|m| m.estimate)
            .unwrap_or(0);
        let Ok(PeerResponse::Forwarded(results)) =
            set.replica(leader).handle(Some(leader), forged(e)).await
        else {
            panic!("not forwarded");
        };
        assert!(results[0].is_ok(), "{:?}", results[0]);
        let flags: Vec<Option<bool>> = commands(set, leader)
            .iter()
            .flat_map(|(_, c)| c)
            .map(|c| c.generation)
            .collect();
        assert_eq!(flags, [want]);
        let marked = entries(set, leader)
            .iter()
            .any(|b| matches!(b, Body::Protocol(_)));
        assert_eq!(marked, want.is_some());
    }
}

/// Two re-routing commands hold one generation in flight: the second is
/// stamped once the first's index passed. That index lies the lead and
/// the build time past its stamp, since the build starts once the entry
/// applies; the build lead, which holds the engine's own lead again, is
/// not counted.
#[tokio::test(start_paused = true)]
async fn rerouting_commands_hold_one_generation_in_flight() {
    let set = start().await;
    let leader = set.leader().await;
    let build_time = 100 * BLOCK;
    for id in set.ids() {
        let mut fake = set.audio(id).lock();
        fake.build_time = Some(build_time);
        fake.build_lead = Some(1000 * BLOCK);
    }
    sleep(Duration::from_millis(20)).await;
    let r = set.replica(leader);
    let o = owner("s");
    let (a, b) = tokio::join!(
        r.submit(&o, "1", Bytes::from("route 0 c")),
        r.submit(&o, "2", Bytes::from("route 1 d")),
    );
    a.expect("committed");
    b.expect("committed");
    let routes: Vec<Request> = commands(&set, leader).into_iter().map(|(r, _)| r).collect();
    assert_eq!(routes.len(), 2, "{routes:?}");
    let first = routes[0].at_index().unwrap();
    let lead = set.replica(leader).contact_metrics().unwrap().lead;
    assert_eq!(first, routes[0].stamp + lead + build_time);
    assert!(
        routes[1].stamp >= first,
        "the second stamped at {} before the first's index {first} passed",
        routes[1].stamp
    );
}

/// A re-routing command waiting for the pending generation proposes
/// nothing meanwhile: a quiesce does not wait for it, and the leader that
/// quiesced does not write it.
#[tokio::test(start_paused = true)]
async fn a_rerouting_command_waiting_for_a_generation_holds_no_quiesce() {
    let set = start().await;
    let leader = set.leader().await;
    for id in set.ids() {
        set.audio(id).lock().build_lead = Some(1500 * BLOCK);
    }
    sleep(Duration::from_millis(20)).await;
    set.replica(leader)
        .append(Body::InputUp {
            slot: 0,
            at_index: 0,
        })
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    let r = set.replica(leader).clone();
    let route =
        tokio::spawn(async move { r.submit(&owner("s"), "1", Bytes::from("route 0 c")).await });
    sleep(Duration::from_millis(20)).await;
    let t = Instant::now();
    set.replica(leader).quiesce(Duration::from_secs(5)).await;
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "the quiesce waited {:?} for a command waiting for a generation",
        t.elapsed()
    );
    assert!(route.await.unwrap().is_err());
    assert!(
        bodies(&set, leader).is_empty(),
        "{:?}",
        bodies(&set, leader)
    );
}

/// A command asking for an index beyond the earliest is refused before it
/// is ordered; one asking for none beside it is ordered.
#[tokio::test(start_paused = true)]
async fn a_scheduled_command_is_refused() {
    let set = start().await;
    let leader = set.leader().await;
    let e = set
        .replica(leader)
        .contact_metrics()
        .unwrap()
        .estimate
        .expect("E counts a replica");
    let command = |id: &str, at_index| Forwarded {
        command: Command {
            generation: None,
            origin: Origin::from(&owner("s")),
            id: id.into(),
            at_index,
            body: b"fader 0 0.5".to_vec(),
        },
        received: e,
        within_ms: 1000,
    };
    let request = PeerRequest::Forward {
        commands: vec![command("far", Some(e + 10 * 48_000)), command("now", None)],
    };
    let Ok(PeerResponse::Forwarded(results)) =
        set.replica(leader).handle(Some(leader), request).await
    else {
        panic!("not forwarded");
    };
    assert!(
        matches!(&results[0], Err(Refused::Invalid(why)) if why.contains("scheduled")),
        "{:?}",
        results[0]
    );
    assert!(results[1].is_ok(), "{:?}", results[1]);
    assert_eq!(bodies(&set, leader), [vec!["fader 0 0.5".to_owned()]]);
}

/// The product owns every input's flow: an entry that routes an input or
/// enables the master is refused, one that raises an input is ordered.
#[tokio::test(start_paused = true)]
async fn a_route_or_master_enable_is_refused() {
    let set = start().await;
    let leader = set.leader().await;
    let r = set.replica(leader);
    assert!(
        r.append(Body::Route {
            at_index: 0,
            body: Vec::new(),
        })
        .is_err()
    );
    assert!(
        r.append(Body::MasterEnable {
            enabled: true,
            at_index: 0,
        })
        .is_err()
    );
    assert!(
        r.append(Body::InputUp {
            slot: 0,
            at_index: 0,
        })
        .is_ok()
    );
}

/// The leader stamps a void and a lease expiry like a command, and flags
/// a void of an entry that re-routed an input for its generation itself,
/// whatever the body asked: every replica re-asserts the route at the
/// void's index.
#[tokio::test(start_paused = true)]
async fn the_leader_stamps_and_classifies_a_void_and_an_expiry() {
    let set = start().await;
    let leader = set.leader().await;
    let r = set.replica(leader);
    r.submit(&owner("s"), "1", Bytes::from("route 0 c"))
        .await
        .expect("committed");
    let mut route = None;
    r.store().entries_after(0, |id, req| {
        if matches!(req.body, Body::Commands(_)) {
            route = Some((id.index, id.term));
        }
    });
    let (revision, term) = route.unwrap();
    r.append(Body::Void {
        revision,
        term,
        at_index: Some(0),
        generation: None,
    })
    .unwrap();
    r.append(Body::LeaseExpiry {
        session: SessionKey::from_parts("p", "gone"),
        renewed: 0,
        at_index: Some(0),
    })
    .unwrap();
    sleep(Duration::from_millis(500)).await;
    let mut stamped = Vec::new();
    r.store().entries_after(revision, |id, req| {
        stamped.push((id.index, req.clone()));
    });
    let void = stamped
        .iter()
        .find(|(_, req)| matches!(req.body, Body::Void { .. }))
        .expect("void written");
    let Body::Void {
        at_index: Some(at),
        generation,
        ..
    } = void.1.body
    else {
        panic!("{void:?}");
    };
    assert!(at >= void.1.stamp && at > 0, "{void:?}");
    assert_eq!(generation, Some(true));
    let expiry = stamped
        .iter()
        .find(|(_, req)| matches!(req.body, Body::LeaseExpiry { .. }))
        .expect("expiry written");
    assert!(
        matches!(expiry.1.body, Body::LeaseExpiry { at_index: Some(at), .. } if at >= expiry.1.stamp && at > 0),
        "{expiry:?}"
    );
    for id in set.ids() {
        let records = set
            .replica(id)
            .store()
            .with_state(|sm| sm.records.get(&void.0).cloned());
        let record = records.unwrap_or_else(|| panic!("replica {id} derived nothing"));
        assert_eq!(record.at_index, at);
        assert!(record.generation);
    }
    // A void of a pending InputUp drops it: no generation.
    r.append(Body::InputUp {
        slot: 1,
        at_index: 0,
    })
    .unwrap();
    sleep(Duration::from_millis(500)).await;
    let mut up = None;
    r.store().entries_after(0, |id, req| {
        if matches!(req.body, Body::InputUp { .. }) {
            up = Some((id.index, id.term));
        }
    });
    let (revision, term) = up.expect("InputUp written");
    r.append(Body::Void {
        revision,
        term,
        at_index: Some(0),
        generation: Some(true),
    })
    .unwrap();
    sleep(Duration::from_millis(500)).await;
    let mut voided = None;
    r.store().entries_after(revision, |_, req| {
        if let Body::Void {
            revision: of,
            generation,
            ..
        } = req.body
            && of == revision
        {
            voided = Some(generation);
        }
    });
    assert_eq!(voided, Some(None), "a void of an InputUp is not flagged");
}

/// Every generation flag `id`'s log holds, in order.
fn flags(set: &Set<Desk>, id: NodeId) -> Vec<Option<bool>> {
    commands(set, id)
        .iter()
        .flat_map(|(_, c)| c)
        .map(|c| c.generation)
        .collect()
}

/// The log positions of `id`'s protocol markers.
fn markers(set: &Set<Desk>, id: NodeId) -> Vec<usize> {
    (entries(set, id).iter().enumerate())
        .filter(|(_, b)| matches!(b, Body::Protocol(_)))
        .map(|(n, _)| n)
        .collect()
}

/// One member whose binary speaks protocol 1 shuts the gate: the leader
/// writes a re-routing command unflagged and appends no marker, and the
/// metric names that member. Once it speaks protocol 2 the leader appends
/// the marker, then flags the next one.
#[tokio::test(start_paused = true)]
async fn a_member_at_protocol_1_keeps_flags_off_the_log() {
    let set = start().await;
    let leader = set.leader().await;
    let old = set.ids().find(|&id| id != leader).unwrap();
    set.net.report_protocol(old, Some(1));
    sleep(Duration::from_millis(50)).await;
    let r = set.replica(leader);
    let m = r.contact_metrics().unwrap();
    assert!(!m.protocol_gate_open);
    assert_eq!(m.protocol_blocker, Some(old));
    r.submit(&owner("s"), "1", Bytes::from("route 0 c"))
        .await
        .expect("committed");
    assert_eq!(flags(&set, leader), [None]);
    assert!(markers(&set, leader).is_empty());

    set.net.report_protocol(old, None);
    sleep(Duration::from_millis(50)).await;
    assert!(r.contact_metrics().unwrap().protocol_gate_open);
    r.submit(&owner("s"), "2", Bytes::from("route 1 d"))
        .await
        .expect("committed");
    assert_eq!(flags(&set, leader), [None, Some(true)]);
    let log = entries(&set, leader);
    let flagged = log
        .iter()
        .rposition(|b| matches!(b, Body::Commands(_)))
        .unwrap();
    assert_eq!(markers(&set, leader).len(), 1, "{log:?}");
    assert!(markers(&set, leader)[0] < flagged, "{log:?}");
}

/// The command writer and the generation writer, each flagging the first
/// entry it writes while the marker is not applied yet, append one marker
/// between them: the followers are stalled, so the first marker is still
/// being committed when the second writer flags.
#[tokio::test(start_paused = true)]
async fn concurrent_writers_append_the_marker_once() {
    let set = start().await;
    let leader = set.leader().await;
    let r = set.replica(leader);
    // Written unflagged: no marker yet.
    r.append(Body::InputUp {
        slot: 0,
        at_index: 0,
    })
    .unwrap();
    sleep(Duration::from_millis(20)).await;
    let mut up = None;
    r.store().entries_after(0, |id, req| {
        if matches!(req.body, Body::InputUp { .. }) {
            up = Some((id.index, id.term));
        }
    });
    let (revision, term) = up.expect("InputUp written");
    assert!(
        r.store()
            .with_state(|sm| sm.pending.contains_key(&revision))
    );
    assert!(markers(&set, leader).is_empty());

    for id in set.ids().filter(|&id| id != leader) {
        // Below the heartbeat interval, openraft's timeout for an append.
        set.net.stall(id, Duration::from_millis(30));
    }
    // A restamp of the InputUp, which needs a generation, goes to the
    // generation writer, the fader move to the command writer.
    r.append(Body::Restamp {
        revision,
        term,
        at_index: 0,
        paths: Vec::new(),
        generation: Some(true),
    })
    .unwrap();
    let fader = r.submit(&owner("s"), "1", Bytes::from("fader 0 0.5"));
    fader.await.expect("committed");
    for id in set.ids() {
        set.net.unstall(id);
    }
    sleep(Duration::from_millis(500)).await;
    let log = entries(&set, leader);
    assert!(
        log.iter().any(|b| matches!(b, Body::Restamp { .. })),
        "{log:?}"
    );
    assert_eq!(markers(&set, leader).len(), 1, "{log:?}");
}

/// What each replica's engine was handed for log index `index`.
fn handed(set: &Set<Desk>, id: NodeId, index: u64) -> Option<(u64, Vec<samplez::Entry>)> {
    (set.audio(id).lock().appended.iter())
        .find(|a| a.0 == index)
        .map(|a| (a.1, a.2.clone()))
}

/// Fader moves `from..to`, each written through the leader of the
/// moment.
async fn faders(set: &Set<Desk>, from: u64, to: u64) {
    for n in from..to {
        let leader = set.leader().await;
        set.replica(leader)
            .submit(
                &owner("s"),
                &n.to_string(),
                Bytes::from(format!("fader {} 0.{}", n % 2, n % 10)),
            )
            .await
            .expect("committed");
    }
}

/// A set of `options` with contact, whose follower can be made to
/// install a snapshot ([`Set::force_install`]).
async fn start_installable(options: Options) -> Set<Desk> {
    let mut options = Options {
        contact: Some(Timing::defaults(BLOCK)),
        ..options
    };
    options.raft.snapshot_policy = openraft::SnapshotPolicy::Never;
    options.raft.max_in_snapshot_log_to_keep = 0;
    options.raft.purge_batch_size = 1;
    let set = Set::start_with(options, Arc::new(|_| Desk::new())).await;
    sleep(Duration::from_millis(50)).await;
    set
}

/// The leader cut off until another one leads, then healed; first every
/// replica applies what it applied, so no entry is cut off from a
/// follower between its commit and its apply, which would leave it
/// unresolved there and handed elsewhere.
async fn change_leader(set: &Set<Desk>) {
    let old = set.leader().await;
    let applied = set.replica(old).store().applied_index();
    let deadline = Instant::now() + Duration::from_secs(5);
    while set
        .ids()
        .any(|id| set.replica(id).store().applied_index() < applied)
    {
        assert!(Instant::now() < deadline, "applied everywhere within 5 s");
        sleep(Duration::from_millis(5)).await;
    }
    set.net.isolate(old, set.ids());
    while !set.ids().any(|id| {
        id != old
            && set.replica(id).raft().metrics().borrow().state == openraft::ServerState::Leader
    }) {
        assert!(Instant::now() < deadline, "a new leader within 5 s");
        sleep(Duration::from_millis(10)).await;
    }
    set.net.heal();
}

/// Every audio replica's engine is handed the same records, by log
/// index, at the same index: what each entry derived as it applied,
/// across a leader change and a snapshot install on one replica, which
/// is handed every record after the snapshot's index and none another
/// replica was not.
#[tokio::test(start_paused = true)]
async fn every_replica_hands_its_engine_the_same_records() {
    let set = start_installable(Options::default()).await;
    faders(&set, 0, 4).await;
    change_leader(&set).await;
    faders(&set, 4, 8).await;
    let leader = set.leader().await;
    let installed = set.ids().find(|&id| id != leader).unwrap();
    set.force_install(installed).await;
    let snapshot = set.replica(installed).store().applied_index();
    faders(&set, 8, 12).await;
    sleep(Duration::from_millis(200)).await;
    let records = |id| set.audio(id).lock().appended.clone();
    let others: Vec<NodeId> = set.ids().filter(|&id| id != installed).collect();
    let full = records(others[0]);
    assert!(full.len() >= 13, "{full:?}");
    assert_eq!(records(others[1]), full, "the other replicas");
    let after: Vec<_> = (full.iter()).filter(|r| r.0 > snapshot).cloned().collect();
    assert!(after.len() >= 4, "{after:?}");
    let mine = records(installed);
    assert!(
        mine.ends_with(&after),
        "after the install at {snapshot}: {mine:?} against {after:?}"
    );
    assert!(mine.iter().all(|r| full.contains(r)), "{mine:?}");
}

/// The witness's state, which derives records as an audio replica's
/// does though it hands none, saved as a snapshot carries it and loaded
/// into a product the factory builds (design test 20): the floor and the
/// re-routed slots of the audio replicas, and the same record for every
/// entry after it.
#[tokio::test(start_paused = true)]
async fn a_witness_snapshot_loads_to_the_audio_replicas_floor_and_records() {
    let witness = node_id(Slot::Witness, 7);
    let audio = |o: u16| node_id(Slot::Audio(o), 1 + u64::from(o));
    let set = start_installable(Options {
        ids: Some(vec![audio(0), audio(1), witness]),
        ..Options::default()
    })
    .await;
    let leader = set.leader().await;
    set.replica(leader)
        .submit(&owner("s"), "r", Bytes::from("route 0 c"))
        .await
        .expect("committed");
    faders(&set, 0, 4).await;
    sleep(Duration::from_millis(100)).await;
    let saved = set.replica(witness).store().with_state(|sm| sm.save());
    let n = set.replica(witness).store().applied_index();
    let mut loaded = StateMachine::new(Desk::new(), Limits::defaults(48_000));
    loaded.load(saved, Limits::defaults(48_000)).expect("loads");
    let a = set.replica(audio(0)).store();
    assert_eq!(a.applied_index(), n, "settled");
    a.with_state(|sm| {
        assert_eq!(loaded.floor, sm.floor);
        assert_eq!(loaded.derived, sm.derived);
    });
    faders(&set, 4, 8).await;
    sleep(Duration::from_millis(100)).await;
    let mut next = Vec::new();
    a.entries_after(n, |id, request| next.push((id, request.clone())));
    assert!(next.len() >= 4, "{next:?}");
    for (id, request) in &next {
        loaded.apply_at(id.index, id.term, request);
    }
    a.with_state(|sm| {
        for (id, _) in &next {
            assert_eq!(
                loaded.records.get(&id.index),
                sm.records.get(&id.index),
                "entry {}",
                id.index
            );
        }
        assert_eq!(loaded.floor, sm.floor);
    });
}

/// One replica's part in an [`fsync_load`], counted over the measured
/// writes.
#[derive(Debug)]
struct Handed {
    id: NodeId,
    /// Records handed to its engine.
    records: usize,
    /// Entries it held unresolved.
    unresolved: u64,
}

/// Writes every 5 ms, one follower's every fsync `delay` slow (design
/// test 21): for 1 s while the lead settles, then for `run` measured.
/// That follower applies each entry later than the others, and holds
/// unresolved those it applies past their deadline. No engine refuses a
/// record or is handed one late, and no decision contradicts a replica.
/// Returns the writes measured, per replica its part in them, the slow
/// follower first, and the leader's contact metrics.
async fn fsync_load(
    run: Duration,
    delay: Duration,
    timing: Timing,
) -> (u64, Vec<Handed>, samplez_replica::contact::Metrics) {
    let slow = 2;
    let set = Set::start_with(
        Options {
            contact: Some(timing),
            sync_delays: vec![(slow, delay)],
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    sleep(Duration::from_millis(50)).await;
    let ids: Vec<NodeId> = std::iter::once(slow)
        .chain(set.ids().filter(|&id| id != slow))
        .collect();
    let counts = || -> Vec<(usize, u64)> {
        (ids.iter())
            .map(|&id| {
                let unresolved = set.replica(id).contact_metrics().unwrap().unresolved;
                (set.audio(id).lock().appended.len(), unresolved)
            })
            .collect()
    };
    let n = std::cell::Cell::new(0u64);
    let write = async |until: Instant| {
        while Instant::now() < until {
            let leader = set.leader().await;
            assert_ne!(leader, slow, "the slow disk is a follower's");
            set.replica(leader)
                .submit(
                    &owner("s"),
                    &n.get().to_string(),
                    Bytes::from(format!("fader {} 0.{}", n.get() % 2, n.get() % 10)),
                )
                .await
                .expect("committed");
            n.set(n.get() + 1);
            sleep(Duration::from_millis(5)).await;
        }
    };
    write(Instant::now() + Duration::from_secs(1)).await;
    let (before, n0) = (counts(), n.get());
    write(Instant::now() + run).await;
    sleep(Duration::from_millis(500)).await;
    let mut out = Vec::new();
    for (&id, (after, before)) in ids.iter().zip(counts().into_iter().zip(before)) {
        let m = set.replica(id).contact_metrics().unwrap();
        assert_eq!(m.entries_refused, 0, "{id}");
        assert_eq!(set.audio(id).lock().late, 0, "{id} handed a record late");
        assert_eq!(m.contradicted, 0, "{id} contradicted");
        out.push(Handed {
            id,
            records: after.0 - before.0,
            unresolved: after.1 - before.1,
        });
    }
    let leader = set.replica(set.leader().await).contact_metrics().unwrap();
    let n = n.get() - n0;
    eprintln!(
        "{n} writes measured in {run:?}, one follower's fsync {delay:?} slow; per replica, \
         the slow one first: {out:?}; the leader's lead {} samples, capped: {}, raised by {:?}",
        leader.lead, leader.apply_lead_capped, leader.apply_lead_raised_by
    );
    (n, out, leader)
}

/// The timing of a set on network block storage: `maxApplyLead` 80 ms
/// (design D3).
fn network_storage_timing() -> Timing {
    Timing {
        max_apply_lead: Duration::from_millis(80),
        ..Timing::defaults(BLOCK)
    }
}

/// A follower whose fsync takes 20 ms takes every entry written during
/// one fsync with the next, so it applies each one to two fsyncs after
/// its append on the leader, 20 to 40 ms; the lead it raises covers that
/// at the 80 ms cap. It hands its engine every record but under 1 %, the others
/// all of them. Returns the leader's contact metrics.
async fn a_slow_follower_disk_is_covered_by_the_lead(
    run: Duration,
) -> samplez_replica::contact::Metrics {
    let (n, out, leader) =
        fsync_load(run, Duration::from_millis(20), network_storage_timing()).await;
    let slow = &out[0];
    assert!(
        slow.unresolved * 100 < n,
        "{} of {n} unresolved on the slow follower",
        slow.unresolved
    );
    for other in &out[1..] {
        assert_eq!(other.unresolved, 0, "{other:?}");
    }
    assert_eq!(leader.apply_lead_raised_by, Some(slow.id));
    leader
}

/// Over 3 s the lead stays below the 80 ms cap: the margin E lags TAI by
/// here and applyGuard, 32 ms, plus 40 ms, the two blocks of slack the
/// lead keeps and the jitter of E, at most 78 ms.
#[tokio::test(start_paused = true)]
async fn a_slow_follower_disk_refuses_and_contradicts_nothing() {
    let leader = a_slow_follower_disk_is_covered_by_the_lead(Duration::from_secs(3)).await;
    assert!(!leader.apply_lead_capped);
    assert!(
        leader.lead <= index_at(78_000_000),
        "lead {} samples above 78 ms",
        leader.lead
    );
}

/// For the 30 s the design names: run with `--ignored`. Over 30 s the
/// lead can reach the cap of 80 ms, which leaves the slow follower holding
/// an entry unresolved now and then; under 1 % all the same.
#[tokio::test(start_paused = true)]
#[ignore = "30 s of writes; the 3 s variant runs in CI"]
async fn a_slow_follower_disk_refuses_and_contradicts_nothing_for_30_s() {
    a_slow_follower_disk_is_covered_by_the_lead(Duration::from_secs(30)).await;
}

/// A follower whose fsync takes 100 ms needs a lead past the default cap
/// of 50 ms: the lead stops there, the leader names that follower as what
/// holds it there, and it alone holds entries unresolved; the others
/// hand every record.
#[tokio::test(start_paused = true)]
async fn a_follower_too_slow_for_the_cap_is_named_and_alone_degraded() {
    let (n, out, leader) = fsync_load(
        Duration::from_secs(3),
        Duration::from_millis(100),
        Timing::defaults(BLOCK),
    )
    .await;
    let slow = &out[0];
    assert!(leader.apply_lead_capped);
    assert_eq!(leader.lead, index_at(50_000_000));
    assert_eq!(leader.apply_lead_raised_by, Some(slow.id));
    assert!(slow.unresolved * 2 > n, "{slow:?} of {n}");
    for other in &out[1..] {
        assert_eq!(other.unresolved, 0, "{other:?}");
        assert!(other.records as u64 * 100 >= n * 99, "{other:?} of {n}");
    }
}

/// A command applied on no replica by its apply deadline is unresolved
/// everywhere. The leader restamps it, never voids it for a later entry,
/// since the restamp re-asserts the state at its own place; what the
/// restamp derives reaches every engine at the restamp's index, and the
/// command's own record never does.
#[tokio::test(start_paused = true)]
async fn a_command_late_everywhere_is_restamped_and_reasserted() {
    let set = start().await;
    let leader = set.leader().await;
    let followers: Vec<NodeId> = set.ids().filter(|&id| id != leader).collect();
    for &id in &followers {
        set.net.stall(id, Duration::from_millis(60));
    }
    let r = set.replica(leader).clone();
    let submitted = tokio::spawn(async move {
        r.submit(&owner("s"), "late", Bytes::from("fader 0 0.25"))
            .await
    });
    sleep(Duration::from_millis(150)).await;
    for &id in &followers {
        set.net.unstall(id);
    }
    submitted.await.unwrap().expect("committed");
    let mut late = None;
    let mut restamp = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while restamp.is_none() {
        assert!(Instant::now() < deadline, "decided within 5 s");
        set.replica(leader)
            .store()
            .entries_after(0, |id, req| match &req.body {
                Body::Commands(c) if c.iter().any(|c| c.id == "late") => late = Some(id.index),
                Body::Restamp { revision, .. } if Some(*revision) == late => {
                    restamp = Some(id.index)
                }
                Body::Void { revision, .. } if Some(*revision) == late => {
                    panic!("a product entry voided")
                }
                _ => {}
            });
        sleep(Duration::from_millis(5)).await;
    }
    let (late, restamp) = (late.unwrap(), restamp.unwrap());
    sleep(Duration::from_millis(200)).await;
    let first = handed(&set, leader, restamp).expect("the restamp handed");
    assert!(first.1.contains(&samplez::Entry::Gain {
        input: 0,
        output: 0,
        gain: 0.25
    }));
    for id in set.ids() {
        assert_eq!(handed(&set, id, late), None, "{id} handed the late entry");
        assert_eq!(handed(&set, id, restamp).as_ref(), Some(&first), "on {id}");
    }
}

fn committing(set: &Set<Desk>, id: NodeId) -> bool {
    set.replica(id)
        .contact_metrics()
        .and_then(|m| m.replica)
        .and_then(|r| r.state)
        == Some(ReplicaState::Committing)
}

/// One fader move through the leader of the moment, a command each
/// 5 ms; one refused in an election is dropped.
async fn move_fader(set: &Set<Desk>, n: u64) {
    let leader = set.leader().await;
    let replica = set.replica(leader).clone();
    tokio::spawn(async move {
        let command = Bytes::from(format!("fader {} 0.{}", n % 2, n % 10));
        let _ = replica.submit(&owner("s"), &n.to_string(), command).await;
    });
    sleep(Duration::from_millis(5)).await;
}

/// A follower restarted while commands keep coming applies what was
/// written while it was down past each entry's apply deadline as it
/// catches up. Holding those entries unresolved, which the log never
/// decides where they stand, would degrade its blocks for good: it
/// commits again, holding no entry unresolved, within a bound.
#[tokio::test(start_paused = true)]
async fn a_follower_restarted_under_continuous_commands_commits_again() {
    let mut set = Set::start_with(
        Options {
            contact: Some(Timing::defaults(BLOCK)),
            outputs: 1,
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    let mut n = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !set.ids().all(|id| committing(&set, id)) {
        assert!(Instant::now() < deadline, "every replica committing");
        sleep(Duration::from_millis(10)).await;
    }
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    set.crash(follower).await;
    let down = Instant::now() + Duration::from_millis(200);
    while Instant::now() < down {
        n += 1;
        move_fader(&set, n).await;
    }
    set.restart(follower).await;
    set.audio(follower).lock().digest_unresolved = true;
    let held = |set: &Set<Desk>| set.audio(follower).lock().core_unresolved.len();
    let start = Instant::now();
    let mut since: Option<Instant> = None;
    loop {
        n += 1;
        move_fader(&set, n).await;
        if committing(&set, follower) && held(&set) == 0 {
            let from = *since.get_or_insert_with(Instant::now);
            if from.elapsed() >= Duration::from_millis(500) {
                break;
            }
        } else {
            since = None;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the follower committing for 500 ms, holding nothing unresolved, within 5 s; \
             {} unresolved, {:?}",
            held(&set),
            set.replica(follower)
                .contact_metrics()
                .and_then(|m| m.replica)
                .and_then(|r| r.state)
        );
    }
}

/// A follower restarted after a re-route committed while it was down,
/// contact running from the start: one that routes an input to the flow
/// it reads already, ordered on the generation path, derives no record,
/// and one that moves it derives a `Route`. The follower takes both past
/// their apply deadlines and holds them unresolved. The log decides
/// neither, both stand, so the follower resolves the first as it applies
/// it, deriving nothing, and its inputs read the flows of the second. It
/// commits again, reading what the leader reads, within a bound.
#[tokio::test(start_paused = true)]
async fn a_follower_restarted_past_a_re_route_commits_on_its_flows() {
    let mut set = Set::start_with(
        Options {
            contact: Some(Timing::defaults(BLOCK)),
            outputs: 1,
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    for id in set.ids().collect::<Vec<_>>() {
        let mut fake = set.audio(id).lock();
        fake.digest_unresolved = true;
        fake.route_digests = true;
        fake.resyncs = true;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !set.ids().all(|id| committing(&set, id)) {
        assert!(Instant::now() < deadline, "every replica committing");
        sleep(Duration::from_millis(10)).await;
    }
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    set.crash(follower).await;
    for (n, route) in ["route 0 a", "route 1 c"].into_iter().enumerate() {
        set.replica(leader)
            .submit(&owner("s"), &n.to_string(), Bytes::from(route))
            .await
            .expect("committed");
    }
    sleep(Duration::from_millis(200)).await;
    set.restart(follower).await;
    {
        let mut fake = set.audio(follower).lock();
        fake.digest_unresolved = true;
        fake.route_digests = true;
        fake.resyncs = true;
    }
    let reads = |set: &Set<Desk>, id: NodeId| {
        let fake = set.audio(id).lock();
        fake.reads(fake.computed.unwrap_or(0))
    };
    let start = Instant::now();
    let mut since: Option<Instant> = None;
    loop {
        if committing(&set, follower) && reads(&set, follower) == reads(&set, leader) {
            let from = *since.get_or_insert_with(Instant::now);
            if from.elapsed() >= Duration::from_millis(500) {
                break;
            }
        } else {
            since = None;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the follower committing on the leader's flows for 500 ms within 5 s: \
             {:?} reads {:?}, the leader {:?}, {} unresolved",
            set.replica(follower)
                .contact_metrics()
                .and_then(|m| m.replica)
                .and_then(|r| r.state),
            reads(&set, follower),
            reads(&set, leader),
            set.audio(follower).lock().core_unresolved.len(),
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        reads(&set, leader),
        [Some("a".to_owned()), Some("c".to_owned())]
    );
}

/// A diverged replica whose input reads another flow than the applied
/// state names, as one does after holding a `Route` unresolved that
/// stands, cannot be resynced: a replay reads the inputs the engine
/// reads, so every replayed page differs. It is rebuilt from product
/// state instead, which moves the input, and commits again.
#[tokio::test(start_paused = true)]
async fn a_diverged_replica_on_another_flow_is_rebuilt_rather_than_resynced() {
    let m = rebuilt_off_flows(false).await;
    assert!(m.rebuilt_for_flows >= 1, "{m:?}");
}

/// A replay on another flow can hand over, its replayed pages not yet
/// reported by the donor; the first page after it differs and the replica
/// diverges again. The hand-over moved no input, so the mismatch counts on
/// across it: the replica is rebuilt rather than resynced for good.
#[tokio::test(start_paused = true)]
async fn a_replica_handed_over_on_another_flow_is_rebuilt() {
    let m = rebuilt_off_flows(true).await;
    assert!(m.handovers >= 1, "handed over first: {m:?}");
    assert!(m.rebuilt_for_flows >= 1, "{m:?}");
}

/// A follower's input 0 moves to a flow the log never named and a glitch
/// diverges it, every replay handing over on other flows when
/// `hands_over`. Returns its resync metrics once it committed on the log's
/// flows for 500 ms.
async fn rebuilt_off_flows(hands_over: bool) -> samplez_replica::resync::Metrics {
    let set = Set::start_with(
        Options {
            contact: Some(Timing::defaults(BLOCK)),
            outputs: 1,
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    for id in set.ids().collect::<Vec<_>>() {
        let mut fake = set.audio(id).lock();
        fake.route_digests = true;
        fake.resyncs = true;
        fake.hands_over_off_flows = hands_over;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !set.ids().all(|id| committing(&set, id)) {
        assert!(Instant::now() < deadline, "every replica committing");
        sleep(Duration::from_millis(10)).await;
    }
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    // The follower's input 0 moves to a flow the log never named, and a
    // glitch diverges it.
    {
        let mut fake = set.audio(follower).lock();
        let k = fake.computed.unwrap() + 20;
        fake.routes.push((k, 0, Some("z".to_owned())));
        fake.glitched.push((k, harness::FOR_GOOD, 0.5));
    }
    let diverged = Instant::now() + Duration::from_secs(5);
    while set
        .replica(follower)
        .contact_metrics()
        .and_then(|m| m.replica)
        .is_none_or(|r| r.divergences == 0)
    {
        assert!(Instant::now() < diverged, "the follower diverged");
        sleep(Duration::from_millis(10)).await;
    }
    let start = Instant::now();
    let mut since: Option<Instant> = None;
    loop {
        let reads = {
            let fake = set.audio(follower).lock();
            fake.reads(fake.computed.unwrap_or(0))
        };
        if committing(&set, follower) && reads == [Some("a".to_owned()), Some("b".to_owned())] {
            let from = *since.get_or_insert_with(Instant::now);
            if from.elapsed() >= Duration::from_millis(500) {
                break;
            }
        } else {
            since = None;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the follower committing on the log's flows within 10 s: {:?} reads {reads:?}",
            set.replica(follower)
                .contact_metrics()
                .and_then(|m| m.replica)
                .and_then(|r| r.state),
        );
        sleep(Duration::from_millis(10)).await;
    }
    set.replica(follower).contact_metrics().unwrap().resync
}

/// One command through the leader of the moment: every seventh moves an
/// input to a flow, the others are fader moves.
async fn command(set: &Set<Desk>, n: u64) {
    if !n.is_multiple_of(7) {
        return move_fader(set, n).await;
    }
    let replica = set.replica(set.leader().await).clone();
    let flow = if n.is_multiple_of(14) { "a" } else { "c" };
    tokio::spawn(async move {
        let command = Bytes::from(format!("route {} {flow}", n % 2));
        let _ = replica.submit(&owner("s"), &n.to_string(), command).await;
    });
    sleep(Duration::from_millis(5)).await;
}

/// A follower restarted briefly, while commands keep coming, reports
/// itself committing before its log holds the entries written while it
/// was down. It applies them past their apply deadlines in that state,
/// where they are held unresolved, and the log never decides them: they
/// stand on the replicas that applied them in time. The follower then
/// withdraws, its digest naming entries its peers' do not, and stays
/// withdrawn. Once it commits nothing again it is rebuilt past them: it
/// commits again, holding nothing unresolved, within a bound.
#[tokio::test(start_paused = true)]
async fn a_follower_that_applied_late_entries_while_committing_is_rebuilt_past_them() {
    let mut set = Set::start_with(
        Options {
            contact: Some(Timing::defaults(BLOCK)),
            outputs: 1,
            ..Options::default()
        },
        Arc::new(|_| Desk::new()),
    )
    .await;
    for id in set.ids().collect::<Vec<_>>() {
        let mut fake = set.audio(id).lock();
        fake.digest_unresolved = true;
        fake.route_digests = true;
        fake.resyncs = true;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !set.ids().all(|id| committing(&set, id)) {
        assert!(Instant::now() < deadline, "every replica committing");
        sleep(Duration::from_millis(10)).await;
    }
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    set.crash(follower).await;
    let mut n = 0;
    let down = Instant::now() + Duration::from_millis(300);
    while Instant::now() < down {
        n += 1;
        command(&set, n).await;
    }
    set.restart(follower).await;
    {
        let mut fake = set.audio(follower).lock();
        fake.digest_unresolved = true;
        fake.route_digests = true;
        fake.resyncs = true;
    }
    let held = |set: &Set<Desk>| set.audio(follower).lock().core_unresolved.len();
    let start = Instant::now();
    let mut since: Option<Instant> = None;
    loop {
        n += 1;
        command(&set, n).await;
        if committing(&set, follower) && held(&set) == 0 {
            let from = *since.get_or_insert_with(Instant::now);
            if from.elapsed() >= Duration::from_millis(500) {
                break;
            }
        } else {
            since = None;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the follower committing for 500 ms, holding nothing unresolved, within 5 s; \
             {} unresolved, {:?}",
            held(&set),
            set.replica(follower)
                .contact_metrics()
                .and_then(|m| m.replica)
                .and_then(|r| r.state)
        );
    }
    // The scenario's premise: entries taken late went unresolved and the
    // engine was rebuilt past them, rather than holding them.
    let rebuilds = (set.replica(follower).contact_metrics()).map_or(0, |m| m.late_rebuilds);
    assert!(rebuilds >= 1, "no rebuild past late entries: {rebuilds}");
}
