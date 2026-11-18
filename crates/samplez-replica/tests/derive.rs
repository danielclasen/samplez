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
/// and through a follower alike. The writer writes no flag until every
/// member applies it.
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
    assert!(
        commands(&set, leader)
            .iter()
            .flat_map(|(_, c)| c)
            .all(|c| c.generation.is_none())
    );
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
