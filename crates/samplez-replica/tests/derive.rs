//! A product that drives an engine on an in-process set with contact
//! running: how the writer classifies, orders and refuses its commands.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Set, owner};
use samplez::Targets;
use samplez_replica::contact::Timing;
use samplez_replica::entry::{Body, Command, NodeId, Origin, Request, SessionKey};
use samplez_replica::peer::{Forwarded, PeerRequest, PeerResponse, Refused};
use samplez_replica::state_machine::{Apply, Changed, Effect, Product};
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
    fn paths(&self, _: &[u8]) -> Vec<String> {
        Vec::new()
    }
    fn config_hash(&self) -> u64 {
        0
    }
    fn targets(&self) -> Option<Targets> {
        Some(Targets {
            gains: vec![vec![self.gains[0]], vec![self.gains[1]]],
            flows: self.flows.to_vec(),
            strips: vec![Vec::new(), Vec::new()],
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
/// stamped once the first's index passed, which lies the build lead past
/// its stamp.
#[tokio::test(start_paused = true)]
async fn rerouting_commands_hold_one_generation_in_flight() {
    let set = start().await;
    let leader = set.leader().await;
    let build_lead = 100 * BLOCK;
    for id in set.ids() {
        set.audio(id).lock().build_lead = Some(build_lead);
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
    assert_eq!(first, routes[0].stamp + build_lead);
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
