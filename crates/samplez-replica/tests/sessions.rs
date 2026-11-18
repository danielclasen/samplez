//! Session leases on an in-process set: the leader ends a lease 2 s after
//! the last applied entry carrying its session, which releases its touch
//! ownership; and a replica E does not count refuses client writes.

mod harness;

use std::time::Duration;

use axum::body::Bytes;
use harness::{BLOCK, Options, Paths, Set, owner};
use samplez_replica::Replica;
use samplez_replica::contact::{Timing, time_of};
use samplez_replica::entry::{Body, NodeId, SessionKey};
use samplez_service::ReplicatedState;
use samplez_service::replica::{Ack, NotApplied, Reason, Resolution};
use tokio::time::{Instant, sleep, sleep_until};

/// A lease ends [`samplez_replica::session::LEASE`] after its last entry;
/// the leader looks each 100 ms and the expiry then commits.
const HELD: Duration = Duration::from_millis(1900);
const ENDED: Duration = Duration::from_millis(2600);

async fn send(replica: &Replica<Paths>, session: &str, id: &str, command: &str) -> Ack {
    try_send(replica, session, id, command)
        .await
        .expect("committed")
}

async fn try_send(
    replica: &Replica<Paths>,
    session: &str,
    id: &str,
    command: &str,
) -> Result<Ack, NotApplied> {
    replica
        .submit(&owner(session), id, Bytes::from(command.to_owned()))
        .await
}

fn key(session: &str) -> SessionKey {
    SessionKey::from_parts("anonymous", session)
}

fn owned(set: &Set, id: NodeId, session: &str) -> Vec<String> {
    set.replica(id)
        .store()
        .with_state(|sm| sm.product.owned_by(session))
}

fn lease(set: &Set, id: NodeId, session: &str) -> Option<u64> {
    set.replica(id)
        .store()
        .with_state(|sm| sm.leases.get(&key(session)).copied())
}

fn ended(set: &Set, id: NodeId, session: &str) -> bool {
    set.replica(id)
        .store()
        .with_state(|sm| sm.ended.contains_key(&key(session)))
}

fn others(set: &Set, of: NodeId) -> Vec<NodeId> {
    set.running().filter(|&id| id != of).collect()
}

/// Every running replica still holds `session`'s lease and `paths`.
fn holds(set: &Set, session: &str, paths: &[&str]) {
    for id in set.running() {
        assert!(
            lease(set, id, session).is_some(),
            "{id}: lease of {session}"
        );
        assert!(!ended(set, id, session), "{id}: {session} ended");
        assert_eq!(owned(set, id, session), paths, "{id}: owned by {session}");
    }
}

/// Waits until every running replica ended `session`'s lifetime and it
/// owns nothing, at the latest `by`.
async fn ends(set: &Set, session: &str, by: Instant) {
    loop {
        if set
            .running()
            .all(|id| ended(set, id, session) && owned(set, id, session).is_empty())
        {
            return;
        }
        assert!(Instant::now() < by, "{session}'s lease still held");
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_lease_outlives_its_leader_and_the_next_leader_ends_it() {
    let mut set = Set::start(Options::default()).await;
    let leader = set.leader().await;
    let follower = others(&set, leader)[0];
    let sent = Instant::now();
    let touch = send(set.replica(follower), "s", "1", "touch fader@1").await;
    set.applied(touch.revision).await;
    set.crash(leader).await;
    let next = set.leader().await;
    assert_ne!(next, leader);
    holds(&set, "s", &["fader"]);
    sleep_until(sent + HELD).await;
    holds(&set, "s", &["fader"]);
    ends(&set, "s", sent + ENDED).await;

    // The killed replica applies the same expiry from the log.
    set.restart(leader).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(ended(&set, leader, "s") && owned(&set, leader, "s").is_empty()) {
        assert!(
            Instant::now() < deadline,
            "{leader} never applied the expiry"
        );
        sleep(Duration::from_millis(10)).await;
    }
    // Its dedup table outlived the kill and the lease.
    set.commitable(leader).await;
    let again = send(set.replica(leader), "s", "1", "touch fader@1").await;
    assert_eq!(again.resolution, Resolution::Duplicate);
    assert_eq!(again.revision, touch.revision);
}

#[tokio::test(start_paused = true)]
async fn a_sessions_first_command_on_a_new_connection_starts_its_lease() {
    let set = Set::start(Options::default()).await;
    let leader = set.leader().await;
    let follower = others(&set, leader)[0];
    // No renewal has named the session: its first command is taken and
    // starts the lease.
    for id in set.running() {
        assert_eq!(lease(&set, id, "new"), None);
    }
    let touch = send(set.replica(follower), "new", "1", "touch fader@1").await;
    assert_eq!(touch.resolution, Resolution::Applied);
    set.applied(touch.revision).await;
    holds(&set, "new", &["fader"]);
    // The connection renews it once a second, as the service does.
    let mut renewed = Instant::now();
    for _ in 0..4 {
        sleep(Duration::from_secs(1)).await;
        set.replica(follower).renew_sessions(&[owner("new")]);
        renewed = Instant::now();
    }
    sleep(Duration::from_millis(200)).await;
    holds(&set, "new", &["fader"]);
    // The connection gone: no renewal, and the lease ends.
    sleep_until(renewed + HELD).await;
    holds(&set, "new", &["fader"]);
    ends(&set, "new", renewed + ENDED).await;
}

#[tokio::test(start_paused = true)]
async fn a_touch_then_disconnect_before_any_renewal_releases_the_touch() {
    let set = Set::start(Options::default()).await;
    let leader = set.leader().await;
    let sent = Instant::now();
    let touch = send(set.replica(leader), "s", "1", "touch fader@1").await;
    set.applied(touch.revision).await;
    sleep_until(sent + HELD).await;
    holds(&set, "s", &["fader"]);
    ends(&set, "s", sent + ENDED).await;
    // The release is one delta of its own, naming no command.
    set.applied(touch.revision + 1).await;
    for id in set.running() {
        assert_eq!(set.replica(id).revision(), touch.revision + 1, "{id}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_expiry_applied_after_a_command_of_its_session_is_a_no_op() {
    let set = Set::start(Options::default()).await;
    let leader = set.leader().await;
    let touch = send(set.replica(leader), "s", "1", "touch fader@1").await;
    set.applied(touch.revision).await;
    let first = lease(&set, leader, "s").expect("leased");
    sleep(Duration::from_millis(1500)).await;
    let sent = Instant::now();
    let gain = send(set.replica(leader), "s", "2", "gain=1@1").await;
    set.applied(gain.revision).await;
    let second = lease(&set, leader, "s").expect("leased");
    assert!(second > first, "the command renewed the lease");
    // An expiry decided on the first renewal, applied after the command.
    set.replica(leader)
        .append(Body::LeaseExpiry {
            session: key("s"),
            renewed: first,
            at_index: None,
        })
        .unwrap();
    sleep(Duration::from_millis(200)).await;
    holds(&set, "s", &["fader"]);
    for id in set.running() {
        assert_eq!(lease(&set, id, "s"), Some(second), "{id}");
    }
    // The leader's own expiry follows the command's entry.
    sleep_until(sent + HELD).await;
    holds(&set, "s", &["fader"]);
    ends(&set, "s", sent + ENDED).await;
}

/// Without a leader for longer than the write timeout, a command resolves
/// not applied and is never ordered.
fn short_writes() -> Options {
    Options {
        write_timeout: Duration::from_millis(300),
        ..Options::default()
    }
}

#[tokio::test(start_paused = true)]
async fn a_command_not_applied_across_a_leader_change_resent_after_a_later_one_is_stale() {
    let mut set = Set::start(short_writes()).await;
    let leader = set.leader().await;
    let [a, b] = others(&set, leader)[..] else {
        panic!("three voters")
    };
    send(set.replica(a), "s", "6", "gain=6@6").await;
    set.crash(leader).await;
    let refused = try_send(set.replica(a), "s", "7", "gain=7@7")
        .await
        .expect_err("no leader within the write timeout");
    assert_eq!(refused.reason, Reason::Unavailable);
    set.leader().await;
    let eight = send(set.replica(b), "s", "8", "gain=8@8").await;
    assert_eq!(eight.resolution, Resolution::Applied);
    let sent = Instant::now();
    let seven = send(set.replica(a), "s", "7", "gain=7@7").await;
    assert_eq!(seven.resolution, Resolution::Stale);
    assert!(seven.revision >= eight.revision, "7 applied after 8");
    set.applied(seven.revision).await;
    for id in set.running() {
        let value = set
            .replica(id)
            .store()
            .with_state(|sm| sm.product.values["gain"].clone());
        assert_eq!(value, ("8".to_owned(), 8), "{id}");
    }
    // The resend renewed the session's lease; it ends 2 s after it.
    sleep_until(sent + HELD).await;
    assert!(set.running().all(|id| !ended(&set, id, "s")));
    ends(&set, "s", sent + ENDED).await;
}

#[tokio::test(start_paused = true)]
async fn a_touch_not_applied_resent_after_its_release_is_stale_and_owns_nothing() {
    let mut set = Set::start(short_writes()).await;
    let leader = set.leader().await;
    let [a, b] = others(&set, leader)[..] else {
        panic!("three voters")
    };
    set.crash(leader).await;
    let refused = try_send(set.replica(a), "s", "t", "touch fader@1")
        .await
        .expect_err("no leader within the write timeout");
    assert_eq!(refused.reason, Reason::Unavailable);
    set.leader().await;
    let release = send(set.replica(b), "s", "r", "release fader@2").await;
    assert_eq!(release.resolution, Resolution::Unchanged);
    let touch = send(set.replica(a), "s", "t", "touch fader@1").await;
    assert_eq!(touch.resolution, Resolution::Stale);
    // Another session takes the fader without a conflict.
    let other = send(set.replica(b), "o", "1", "touch fader@1").await;
    assert_eq!(other.resolution, Resolution::Applied);
    set.applied(other.revision).await;
    for id in set.running() {
        assert!(owned(&set, id, "s").is_empty(), "{id}: s owns nothing");
        assert_eq!(owned(&set, id, "o"), ["fader"], "{id}");
        let conflicts = set
            .replica(id)
            .store()
            .with_state(|sm| sm.product.conflicts);
        assert_eq!(conflicts, 0, "{id}");
    }
    // s's lease ends first and releases nothing of o's, which o's
    // connection renews.
    let ending = Instant::now() + ENDED;
    sleep(Duration::from_secs(1)).await;
    set.replica(b).renew_sessions(&[owner("o")]);
    let renewed = Instant::now();
    ends(&set, "s", ending).await;
    for id in set.running() {
        assert_eq!(owned(&set, id, "o"), ["fader"], "{id}");
    }
    ends(&set, "o", renewed + ENDED).await;
}

async fn refusal(set: &Set, id: NodeId, command: &str) -> NotApplied {
    try_send(
        set.replica(id),
        "s",
        command,
        &format!("gain={command}@{command}"),
    )
    .await
    .expect_err("refused")
}

async fn not_commitable(set: &Set, id: NodeId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while set.replica(id).replica().commitable {
        assert!(Instant::now() < deadline, "{id} still commitable");
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_replica_e_does_not_count_refuses_client_writes() {
    let set = Set::start(Options {
        contact: Some(Timing::defaults(BLOCK)),
        ..Options::default()
    })
    .await;
    sleep(Duration::from_millis(50)).await;
    let leader = set.leader().await;
    let [a, b] = others(&set, leader)[..] else {
        panic!("three voters")
    };

    // Unverified: out of E, not commitable, refused; the others serve.
    // An adjtimex step of its kernel TAI offset unverifies it.
    set.clock(a).step_kernel_offset(0);
    not_commitable(&set, a).await;
    let refused = refusal(&set, a, "1").await;
    assert_eq!(refused.reason, Reason::NotCommitable);
    assert_eq!(refused.detail, "clock_unverified");
    send(set.replica(b), "s", "2", "gain=2@2").await;
    send(set.replica(leader), "s", "3", "gain=3@3").await;
    set.clock(a).step_kernel_offset(37);
    set.commitable(a).await;
    send(set.replica(a), "s", "4", "gain=4@4").await;

    // Judged by the leader's ClockVerdict: its TAI clock moved beyond
    // clockGuard off the others'.
    let guard = time_of(Timing::defaults(BLOCK).clock_guard) as i64;
    set.clock(b).set_tai_offset(3 * guard);
    not_commitable(&set, b).await;
    assert!(
        set.replica(leader)
            .store()
            .with_state(|sm| sm.verdicts.get(&b) == Some(&true)),
        "{b} judged"
    );
    assert_eq!(refusal(&set, b, "5").await.reason, Reason::NotCommitable);
    send(set.replica(a), "s", "6", "gain=6@6").await;
    set.clock(b).set_tai_offset(0);
    set.commitable(b).await;

    // A self-report while E counts a free replica; the free one serves.
    set.audio(a).lock().flags.self_report = true;
    not_commitable(&set, a).await;
    assert_eq!(refusal(&set, a, "7").await.reason, Reason::NotCommitable);
    send(set.replica(b), "s", "8", "gain=8@8").await;

    // E counts nobody: every replica refuses.
    for id in set.ids() {
        set.audio(id).lock().flags.step_fence = true;
    }
    for id in set.ids() {
        not_commitable(&set, id).await;
        assert_eq!(
            refusal(&set, id, &format!("9{id}")).await.reason,
            Reason::NotCommitable
        );
    }
}
