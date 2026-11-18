//! Command scheduling on an in-process set with contact running: E, the
//! stamps and at_indices the leader gives entries, the future list, the
//! serialised generation-needing entries and the command age bound.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Set, owner};
use samplez_replica::contact::{FUTURE_LIST, START_LEAD, Timing, index_at, time_of};
use samplez_replica::entry::{Body, Command, NodeId, Origin, Request};
use samplez_replica::peer::{Forwarded, PeerRequest, PeerResponse, Refused};
use samplez_service::ReplicatedState;
use samplez_service::replica::Reason;
use tokio::time::sleep;

fn timing() -> Timing {
    Timing::defaults(BLOCK)
}

async fn start(ids: Option<Vec<NodeId>>) -> Set {
    let set = Set::start(Options {
        contact: Some(timing()),
        ids,
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    set
}

fn lead() -> u64 {
    index_at(START_LEAD.as_nanos() as u64)
}

/// The application entries `id`'s log holds, in order.
fn entries(set: &Set, id: NodeId) -> Vec<Request> {
    let mut out = Vec::new();
    set.replica(id)
        .store()
        .entries_after(0, |_, r| out.push(r.clone()));
    out
}

fn e(set: &Set, id: NodeId) -> u64 {
    set.replica(id)
        .contact_metrics()
        .unwrap()
        .estimate
        .expect("E counts a replica")
}

#[tokio::test(start_paused = true)]
async fn every_voter_estimates_e_alike_and_counts_through_a_relay() {
    let set = start(None).await;
    let leader = set.leader().await;
    let mut others = set.ids().filter(|&id| id != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    // The link between the followers down: each counts the other through
    // the leader's relay.
    set.net.cut(a, b);
    sleep(Duration::from_millis(30)).await;
    let es: Vec<u64> = set.ids().map(|id| e(&set, id)).collect();
    let spread = es.iter().max().unwrap() - es.iter().min().unwrap();
    assert!(spread <= 4 * BLOCK, "E estimates {es:?} differ by {spread}");
    for id in set.ids() {
        let counted = set.replica(id).contact_metrics().unwrap().counted;
        assert_eq!(counted.len(), 3, "{id} counts {counted:?}");
    }
    // A replica a ClockVerdict judges is counted by nobody: its clock
    // moved three guards off the others, the leader judges it.
    set.clock(b)
        .set_tai_offset(3 * time_of(timing().clock_guard) as i64);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !set.ids().all(|id| {
        set.replica(id)
            .store()
            .with_state(|sm| sm.verdicts.get(&b) == Some(&true))
    }) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{b} judged within 5 s"
        );
        sleep(Duration::from_millis(1)).await;
    }
    sleep(Duration::from_millis(30)).await;
    for id in set.ids() {
        let counted = set.replica(id).contact_metrics().unwrap().counted;
        assert!(!counted.contains(&b), "{id} counts the judged {b}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_replica_with_a_flag_is_counted_only_when_every_one_has_one() {
    let set = start(None).await;
    set.audio(0).lock().flags.step_fence = true;
    set.audio(1).lock().flags.self_report = true;
    sleep(Duration::from_millis(30)).await;
    let counted = set.replica(2).contact_metrics().unwrap().counted;
    assert_eq!(counted, [2], "the free replica alone");
    set.audio(2).lock().flags.self_report = true;
    sleep(Duration::from_millis(30)).await;
    let counted = set.replica(2).contact_metrics().unwrap().counted;
    assert_eq!(counted, [1, 2], "those whose only flag is a self-report");
    set.audio(1).lock().flags.step_fence = true;
    set.audio(2).lock().flags.step_fence = true;
    sleep(Duration::from_millis(30)).await;
    let leader = set.leader().await;
    let refused = set
        .replica(leader)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect_err("no replica takes client writes while E counts nobody");
    assert_eq!(refused.reason, Reason::NotCommitable);
}

#[tokio::test(start_paused = true)]
async fn commands_are_stamped_a_lead_past_e_and_later_indices_kept() {
    let set = start(None).await;
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    let before = e(&set, follower);
    set.replica(follower)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("committed");
    let request = entries(&set, leader).pop().unwrap();
    let Body::Commands(commands) = &request.body else {
        panic!("{request:?}")
    };
    let at = commands[0].at_index.expect("stamped");
    assert_eq!(request.stamp % BLOCK, 0);
    assert!(
        request.stamp >= before,
        "stamp {} below E {before}",
        request.stamp
    );
    assert_eq!(at, request.stamp + lead());
    // An engine entry asked for far ahead keeps its index.
    let far = request.stamp + 480_000;
    set.replica(leader)
        .append(Body::InputDown {
            slot: 0,
            at_index: far,
        })
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    let request = entries(&set, leader).pop().unwrap();
    assert_eq!(request.at_index(), Some(far));
}

#[tokio::test(start_paused = true)]
async fn the_future_list_holds_64_entries() {
    let set = start(None).await;
    let leader = set.leader().await;
    let far = e(&set, leader) + 4_800_000;
    for n in 0..FUTURE_LIST as u64 + 6 {
        set.replica(leader)
            .append(Body::InputDown {
                slot: 0,
                at_index: far + n * BLOCK,
            })
            .unwrap();
    }
    sleep(Duration::from_millis(200)).await;
    let pending = entries(&set, leader)
        .iter()
        .filter(|r| r.at_index().is_some_and(|at| at >= far))
        .count();
    assert_eq!(pending, FUTURE_LIST);
}

#[tokio::test(start_paused = true)]
async fn generation_needing_entries_wait_for_the_last_one_and_buildlead() {
    let set = start(None).await;
    let leader = set.leader().await;
    let build_lead = 100 * BLOCK;
    for id in set.ids() {
        set.audio(id).lock().build_lead = Some(build_lead);
    }
    sleep(Duration::from_millis(20)).await;
    set.replica(leader)
        .append(Body::InputUp {
            slot: 0,
            at_index: 0,
        })
        .unwrap();
    set.replica(leader)
        .append(Body::InputUp {
            slot: 1,
            at_index: 0,
        })
        .unwrap();
    sleep(Duration::from_millis(300)).await;
    let ups: Vec<Request> = entries(&set, leader)
        .into_iter()
        .filter(|r| matches!(r.body, Body::InputUp { .. }))
        .collect();
    assert_eq!(ups.len(), 2, "{ups:?}");
    let first = ups[0].at_index().unwrap();
    assert_eq!(first, ups[0].stamp + build_lead, "buildLead past the lead");
    // The second was stamped once the first's index had passed.
    assert!(
        ups[1].stamp >= first,
        "the second stamped at {} before the first's index {first} passed",
        ups[1].stamp
    );
}

fn forwarded(received: u64) -> PeerRequest {
    let o = owner("s");
    PeerRequest::Forward {
        commands: vec![Forwarded {
            command: Command {
                generation: None,
                origin: Origin::from(&o),
                id: "late".into(),
                at_index: None,
                body: b"gain=2@2".to_vec(),
            },
            received,
            within_ms: 1000,
        }],
    }
}

#[tokio::test(start_paused = true)]
async fn a_forwarded_copy_older_than_the_retention_is_refused_expired() {
    use samplez_replica::membership::{Slot, node_id};
    let witness = node_id(Slot::Witness, 9);
    let mut set = start(Some(vec![0, 1, witness])).await;
    // Also with the witness leading, which has no engine and no E of its
    // own.
    for _ in 0..20 {
        if set.leader().await == witness {
            break;
        }
        let leader = set.leader().await;
        set.crash(leader).await;
        set.leader().await;
        set.restart(leader).await;
        sleep(Duration::from_millis(100)).await;
    }
    let leader = set.leader().await;
    assert_eq!(leader, witness, "the witness never led in 20 elections");
    let e = e(&set, leader);
    let retention = 10 * harness::RATE;
    let old = e - retention;
    let reply = set
        .replica(leader)
        .handle(Some(leader), forwarded(old))
        .await;
    assert_eq!(
        reply,
        Ok(PeerResponse::Forwarded(vec![Err(Refused::Expired)])),
        "leader {leader}"
    );
    let reply = set
        .replica(leader)
        .handle(Some(leader), forwarded(e - retention + 2_000 * BLOCK / 48))
        .await;
    assert!(
        matches!(&reply, Ok(PeerResponse::Forwarded(r)) if r[0].is_ok()),
        "a copy inside the bound commits: {reply:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_client_resend_is_refused_once_its_first_receipt_is_too_old() {
    let set = start(None).await;
    let leader = set.leader().await;
    let mut others = set.ids().filter(|&id| id != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    // a cannot reach the leader: the command is not ordered.
    set.net.cut_peer(a, leader);
    let first = set
        .replica(a)
        .submit(&owner("s"), "7", Bytes::from("gain=7@7"))
        .await;
    assert!(first.is_err());
    set.net.heal();
    sleep(Duration::from_secs(10)).await;
    let resend = set
        .replica(a)
        .submit(&owner("s"), "7", Bytes::from("gain=7@7"))
        .await
        .expect_err("first received 10 s ago");
    assert_eq!(resend.reason, Reason::Expired);
    // Another replica holds no stamp for it: stamped on receipt.
    set.replica(b)
        .submit(&owner("s"), "7", Bytes::from("gain=7@7"))
        .await
        .expect("committed");
}

#[tokio::test(start_paused = true)]
async fn a_late_commit_raises_the_lead_up_to_its_cap() {
    let set = start(None).await;
    let leader = set.leader().await;
    // Every follower takes 25 ms per RPC: commits arrive 50 ms after the
    // stamp, past the apply deadline of a 30 ms lead.
    for id in set.ids().filter(|&id| id != leader) {
        set.net.stall(id, Duration::from_millis(25));
    }
    for n in 0..3 {
        let _ = set
            .replica(leader)
            .submit(&owner("s"), &n.to_string(), Bytes::from(format!("g=1@{n}")))
            .await;
    }
    sleep(Duration::from_millis(100)).await;
    let m = set.replica(leader).contact_metrics().unwrap();
    assert_eq!(m.lead, index_at(timing().max_apply_lead.as_nanos() as u64));
    assert!(m.apply_lead_capped);
}

/// The 64-entry limit counts entries the engines still hold pending, not
/// only those some voter has not reported committed yet.
#[tokio::test(start_paused = true)]
async fn review_the_future_list_counts_committed_entries() {
    let set = start(None).await;
    let leader = set.leader().await;
    let far = e(&set, leader) + 4_800_000;
    for n in 0..FUTURE_LIST as u64 + 6 {
        set.replica(leader)
            .append(Body::InputDown {
                slot: 0,
                at_index: far + n * BLOCK,
            })
            .unwrap();
        // Each committed and reported committed by every voter before the
        // next one.
        sleep(Duration::from_millis(10)).await;
    }
    sleep(Duration::from_millis(200)).await;
    let pending = entries(&set, leader)
        .iter()
        .filter(|r| r.at_index().is_some_and(|at| at >= far))
        .count();
    assert_eq!(pending, FUTURE_LIST, "entries pending at the engines");
}

/// A command queued behind a generation-needing entry that waits for the
/// previous one's index does not wait with it.
#[tokio::test(start_paused = true)]
async fn review_a_pending_generation_does_not_hold_commands() {
    let set = start(None).await;
    let leader = set.leader().await;
    for id in set.ids() {
        set.audio(id).lock().build_lead = Some(400 * BLOCK);
    }
    sleep(Duration::from_millis(20)).await;
    set.replica(leader)
        .append(Body::InputUp {
            slot: 0,
            at_index: 0,
        })
        .unwrap();
    set.replica(leader)
        .append(Body::InputUp {
            slot: 1,
            at_index: 0,
        })
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    let t = tokio::time::Instant::now();
    set.replica(leader)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect("committed");
    assert!(
        t.elapsed() < Duration::from_millis(50),
        "a control write waited {:?} behind a pending generation",
        t.elapsed()
    );
    // And the second generation still follows the first's index.
    sleep(Duration::from_millis(600)).await;
    let ups: Vec<Request> = entries(&set, leader)
        .into_iter()
        .filter(|r| matches!(r.body, Body::InputUp { .. }))
        .collect();
    assert_eq!(ups.len(), 2);
    assert!(ups[1].stamp >= ups[0].at_index().unwrap());
}

#[tokio::test(start_paused = true)]
async fn a_replica_whose_engine_refused_an_entry_takes_no_writes() {
    let set = start(None).await;
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    set.audio(follower).lock().entries_refused = 1;
    sleep(Duration::from_millis(10)).await;
    assert!(!set.replica(follower).replica().commitable);
    let refused = set
        .replica(follower)
        .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
        .await
        .expect_err("not commitable");
    assert_eq!(refused.reason, Reason::NotCommitable);
    assert!(set.replica(leader).replica().commitable);
}

/// A command the engine would refuse once committed is refused to its
/// client before it is ordered, and commands the engine takes alone but
/// not in one entry are ordered in entries of their own: no committed
/// entry is refused, which would stop every replica taking writes.
#[tokio::test(start_paused = true)]
async fn a_command_the_engine_refuses_is_refused_before_it_is_ordered() {
    let set = start(None).await;
    let leader = set.leader().await;
    let follower = set.ids().find(|&id| id != leader).unwrap();
    let before = entries(&set, leader).len();
    for at in [leader, follower] {
        let refused = set
            .replica(at)
            .submit(&owner("s"), &format!("bad-{at}"), Bytes::from("bad=1@1"))
            .await
            .expect_err("refused before it is ordered");
        assert_eq!(refused.reason, Reason::Invalid);
    }
    assert_eq!(entries(&set, leader).len(), before, "ordered");
    let replica = set.replica(leader);
    let (a, b) = tokio::join!(
        replica.submit(&owner("s"), "p1", Bytes::from("pair=1@1")),
        replica.submit(&owner("s"), "p2", Bytes::from("pair=2@2")),
    );
    a.expect("committed");
    b.expect("committed");
    let pairs: Vec<usize> = entries(&set, leader)
        .iter()
        .filter_map(|r| match &r.body {
            Body::Commands(c) => Some(c.iter().filter(|c| c.body.starts_with(b"pair")).count()),
            _ => None,
        })
        .filter(|&n| n > 0)
        .collect();
    assert_eq!(pairs, [1, 1]);
    for id in set.ids() {
        assert_eq!(set.audio(id).lock().entries_refused, 0);
    }
}

/// A command asking for an index between block starts is ordered at the
/// next block start, the only index the engine applies an entry at: at
/// its own index it would be refused after its commit.
#[tokio::test(start_paused = true)]
async fn a_command_asking_for_an_index_off_a_block_start_is_ordered_at_the_next() {
    let set = start(None).await;
    let leader = set.leader().await;
    let asked = e(&set, leader) + 480_000 + 1;
    let o = owner("s");
    let request = PeerRequest::Forward {
        commands: vec![Forwarded {
            command: Command {
                generation: None,
                origin: Origin::from(&o),
                id: "off".into(),
                at_index: Some(asked),
                body: b"gain=3@3".to_vec(),
            },
            received: e(&set, leader),
            within_ms: 1000,
        }],
    };
    let reply = set.replica(leader).handle(Some(leader), request).await;
    assert!(
        matches!(&reply, Ok(PeerResponse::Forwarded(r)) if r[0].is_ok()),
        "{reply:?}"
    );
    let request = entries(&set, leader).pop().unwrap();
    let Body::Commands(commands) = &request.body else {
        panic!("{request:?}")
    };
    assert_eq!(commands[0].at_index, Some(asked.next_multiple_of(BLOCK)));
}

/// Starts a set whose replica `odd` holds product configuration 5 and
/// every other replica 0, and submits one command at each replica: all
/// are refused unavailable, naming `odd`, and none is ordered. Product
/// state is shared by every replica, so a member of another configuration
/// would apply the command into another state.
async fn refused_while_one_configuration_differs(ids: Option<Vec<NodeId>>, odd: NodeId) {
    let set = Set::start(Options {
        contact: Some(timing()),
        ids,
        configs: vec![(odd, 5)],
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    let leader = set.leader().await;
    let before = entries(&set, leader).len();
    use samplez_replica::membership::{Slot, slot_of};
    let audio: Vec<NodeId> = set
        .ids()
        .filter(|&id| slot_of(id) != Slot::Witness)
        .collect();
    for at in audio {
        let refused = set
            .replica(at)
            .submit(&owner("s"), &format!("c-{at}"), Bytes::from("gain=1@1"))
            .await
            .expect_err("refused");
        assert_eq!(refused.reason, Reason::Unavailable);
        assert!(
            refused.detail.contains("config: replica"),
            "{}",
            refused.detail
        );
    }
    assert_eq!(entries(&set, leader).len(), before, "ordered");
}

#[tokio::test(start_paused = true)]
async fn a_voter_of_another_configuration_stops_client_writes() {
    refused_while_one_configuration_differs(None, 2).await;
}

/// The witness applies the log into its own product state like every
/// voter, so it needs the same configuration.
#[tokio::test(start_paused = true)]
async fn a_witness_of_another_configuration_stops_client_writes() {
    use samplez_replica::membership::{Slot, node_id};
    let witness = node_id(Slot::Witness, 7);
    refused_while_one_configuration_differs(Some(vec![0, 1, witness]), witness).await;
}
