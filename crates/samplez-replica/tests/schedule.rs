//! Command scheduling on an in-process set with contact running: E, the
//! stamps and at_indices the leader gives entries, the future list, the
//! serialised generation-needing entries and the command age bound.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Set, owner};
use samplez_replica::contact::{FUTURE_LIST, START_LEAD, Timing, index_at};
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
    // A replica a ClockVerdict judges is counted by nobody.
    set.replica(leader).append(Body::ClockVerdict {
        replica: b,
        judged: true,
    });
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
        .expect_err("nothing is stamped while E counts nobody");
    assert_eq!(refused.reason, Reason::Unavailable);
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
    set.replica(leader).append(Body::InputDown {
        slot: 0,
        at_index: far,
    });
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
        set.replica(leader).append(Body::InputDown {
            slot: 0,
            at_index: far + n * BLOCK,
        });
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
    set.replica(leader).append(Body::InputUp {
        slot: 0,
        at_index: 0,
    });
    set.replica(leader).append(Body::InputUp {
        slot: 1,
        at_index: 0,
    });
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
