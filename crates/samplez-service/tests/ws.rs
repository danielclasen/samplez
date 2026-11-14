mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use common::*;
use futures_util::StreamExt;
use samplez_service::Ops;
use samplez_service::replica::{Ack, Begin};
use samplez_service::ws::{ClientQueue, Delta, Frame, Hub, Popped, Pushed, pump};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const RETENTION: Duration = Duration::from_secs(60);

fn meter(text: &str) -> Frame {
    Frame::Meter(text.into())
}

fn delta(revision: u64) -> Frame {
    Frame::Delta(Delta {
        revision,
        command: None,
        text: format!("delta@{revision}").into(),
    })
}

async fn popped(queue: &ClientQueue) -> Popped {
    tokio::time::timeout(Duration::from_secs(1), queue.pop())
        .await
        .expect("a frame is queued")
}

#[tokio::test]
async fn full_queue_drops_meters_first_and_never_a_single_delta() {
    let q = ClientQueue::new(3);
    assert_eq!(q.push(meter("m1")), Pushed::Queued);
    assert_eq!(q.push(delta(1)), Pushed::Queued);
    assert_eq!(q.push(delta(2)), Pushed::Queued);
    assert_eq!(q.push(delta(3)), Pushed::MeterDropped, "m1 makes room");
    assert_eq!(
        q.push(meter("m2")),
        Pushed::MeterDropped,
        "only deltas queued"
    );
    assert_eq!(q.len(), 3);

    assert_eq!(q.push(delta(4)), Pushed::Resync, "full of deltas");
    assert!(q.is_empty());
    assert_eq!(q.push(delta(5)), Pushed::Resync, "covered by the catch-up");
    assert_eq!(q.push(meter("m3")), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Resync);
    assert_eq!(popped(&q).await, Popped::Frame(meter("m3")));
    assert_eq!(q.push(delta(6)), Pushed::Queued);
    assert_eq!(popped(&q).await, Popped::Frame(delta(6)));
}

#[tokio::test]
async fn the_oldest_meter_goes_first() {
    let q = ClientQueue::new(2);
    q.push(meter("m1"));
    q.push(meter("m2"));
    assert_eq!(q.push(meter("m3")), Pushed::MeterDropped);
    assert_eq!(popped(&q).await, Popped::Frame(meter("m2")));
    assert_eq!(popped(&q).await, Popped::Frame(meter("m3")));
}

struct Client {
    rx: mpsc::UnboundedReceiver<Message>,
    task: tokio::task::JoinHandle<()>,
}

impl Client {
    /// Runs `pump` into a channel, as a connect with `since` would.
    fn connect(hub: &Hub, state: &Arc<FakeState>, since: Option<u64>) -> Client {
        let queue = hub.subscribe();
        let state = state.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let sink = futures_util::sink::unfold(tx, |tx, msg: Message| async move {
                tx.send(msg).map(|()| tx).map_err(|_| ())
            });
            let mut sink = Box::pin(sink);
            pump(&mut sink, &queue, &*state, since).await;
        });
        Client { rx, task }
    }

    async fn next(&mut self) -> String {
        let msg = tokio::time::timeout(Duration::from_secs(1), self.rx.recv())
            .await
            .expect("a message within 1 s")
            .expect("pump running");
        match msg {
            Message::Text(t) => t.to_string(),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn silent(&mut self) {
        let got = tokio::time::timeout(Duration::from_millis(50), self.rx.recv()).await;
        assert!(got.is_err(), "unexpected {got:?}");
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Applies a delta to the state and publishes it, as the product does.
fn apply(state: &FakeState, hub: &Hub, command: Option<&str>) -> Delta {
    let d = state.apply(command);
    hub.publish(Frame::Delta(d.clone()));
    d
}

#[tokio::test]
async fn connect_catches_up_from_the_last_seen_revision() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(8, RETENTION);
    for _ in 0..3 {
        apply(&state, &hub, None);
    }

    let mut fresh = Client::connect(&hub, &state, None);
    assert_eq!(fresh.next().await, "snapshot@3");

    let mut resumed = Client::connect(&hub, &state, Some(1));
    assert_eq!(resumed.next().await, "delta@2");
    assert_eq!(resumed.next().await, "delta@3");

    let mut current = Client::connect(&hub, &state, Some(3));
    current.silent().await;

    state.forget_through(2);
    let mut stale = Client::connect(&hub, &state, Some(1));
    assert_eq!(stale.next().await, "snapshot@3", "deltas no longer held");

    apply(&state, &hub, None);
    hub.publish(meter("m"));
    for client in [&mut fresh, &mut resumed, &mut current, &mut stale] {
        assert_eq!(client.next().await, "delta@4");
        assert_eq!(client.next().await, "m");
    }
}

#[tokio::test]
async fn a_lagging_replica_skips_what_the_client_saw_on_another() {
    let follower = FakeState::new("r2", "follower");
    let hub = Hub::new(8, RETENTION);
    apply(&follower, &hub, None);

    let mut client = Client::connect(&hub, &follower, Some(3));
    client.silent().await;
    for _ in 0..3 {
        apply(&follower, &hub, None);
    }
    assert_eq!(client.next().await, "delta@4", "2 and 3 skipped");
}

/// The bursts below run without an await, so the pump cannot drain the
/// two-frame queue in between.
#[tokio::test]
async fn overflow_catches_up_from_the_last_sent_revision() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(2, RETENTION);
    for _ in 0..3 {
        apply(&state, &hub, None);
    }
    let mut client = Client::connect(&hub, &state, Some(3));
    client.silent().await;

    for _ in 0..6 {
        apply(&state, &hub, None);
    }
    let mut got = Vec::new();
    for _ in 0..6 {
        got.push(client.next().await);
    }
    let expected: Vec<String> = (4..=9).map(|r| format!("delta@{r}")).collect();
    assert_eq!(got, expected, "in order, none lost or repeated");
    client.silent().await;
}

#[tokio::test]
async fn overflow_falls_back_to_a_snapshot() {
    let state = FakeState::new("r1", "leader");
    let hub = Hub::new(2, RETENTION);
    let mut client = Client::connect(&hub, &state, None);
    assert_eq!(client.next().await, "snapshot@0");

    for _ in 0..3 {
        apply(&state, &hub, None);
    }
    state.forget_through(3);
    assert_eq!(client.next().await, "snapshot@3");
    client.silent().await;
}

#[tokio::test]
async fn commands_apply_once_per_id() {
    let hub = Hub::new(8, RETENTION);
    let commands = hub.commands();

    let Begin::New(pending) = commands.begin("c1") else {
        panic!("c1 is new");
    };
    assert!(matches!(commands.begin("c1"), Begin::InFlight));
    let ack = pending.applied(7);
    assert_eq!(
        ack,
        Ack {
            command: "c1".into(),
            revision: 7
        }
    );
    match commands.begin("c1") {
        Begin::Duplicate(again) => assert_eq!(again, ack),
        _ => panic!("c1 is applied"),
    }

    let Begin::New(rejected) = commands.begin("c2") else {
        panic!("c2 is new");
    };
    drop(rejected);
    assert!(
        matches!(commands.begin("c2"), Begin::New(_)),
        "a rejected command may be resent"
    );
}

#[tokio::test(start_paused = true)]
async fn command_ids_expire_after_the_retention_window() {
    let hub = Hub::new(8, Duration::from_secs(30));
    let Begin::New(pending) = hub.commands().begin("c1") else {
        panic!("c1 is new");
    };
    pending.applied(1);
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert!(matches!(hub.commands().begin("c1"), Begin::Duplicate(_)));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(matches!(hub.commands().begin("c1"), Begin::New(_)));
}

#[tokio::test]
async fn a_command_resent_after_failover_is_not_applied_twice() {
    let (a, b) = (
        FakeState::new("a", "leader"),
        FakeState::new("b", "follower"),
    );
    let (hub_a, hub_b) = (Hub::new(8, RETENTION), Hub::new(8, RETENTION));

    let Begin::New(pending) = hub_a.commands().begin("cmd-1") else {
        panic!("cmd-1 is new");
    };
    let applied = apply(&a, &hub_a, Some("cmd-1"));
    assert_eq!(
        applied.command.as_deref(),
        Some("cmd-1"),
        "the delta echoes the id"
    );
    b.receive(applied.clone());
    hub_b.publish(Frame::Delta(applied.clone()));
    drop(pending);

    match hub_b.commands().begin("cmd-1") {
        Begin::Duplicate(ack) => assert_eq!(ack.revision, applied.revision),
        _ => panic!("replica b applied cmd-1 through replication"),
    }
    match hub_a.commands().begin("cmd-1") {
        Begin::Duplicate(ack) => assert_eq!(ack.revision, applied.revision),
        _ => panic!("the published delta recorded cmd-1 on replica a"),
    }

    let mut client = Client::connect(&hub_b, &b, Some(0));
    assert_eq!(client.next().await, "delta@1");
}

#[tokio::test]
async fn loopback_upgrade_checks_origin_and_resumes_from_since() {
    let state = FakeState::new("r1", "leader");
    let hub = Arc::new(Hub::new(8, RETENTION));
    for _ in 0..3 {
        apply(&state, &hub, None);
    }
    let mut config = config("none", "loopback");
    let loopback = serde_json::json!({ "bind": "127.0.0.1", "port": 0 });
    config["listeners"] = serde_json::json!({ "api": loopback, "ops": loopback, "nmos": loopback });
    let service = service_from(config, Ops::default(), state.clone()).ws({
        let (hub, state) = (hub.clone(), state.clone());
        move |mut socket, connect| {
            let (hub, state) = (hub.clone(), state.clone());
            async move {
                let queue = hub.subscribe();
                pump(&mut socket, &queue, &*state, connect.since).await;
            }
        }
    });
    let bound = service.bind().await.unwrap();
    let addr = bound.api_addr().unwrap();
    let server = tokio::spawn(bound.serve());

    let url = format!("ws://{addr}/api/v1/ws?since=2");
    let mut foreign = url.as_str().into_client_request().unwrap();
    foreign
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    match tokio_tungstenite::connect_async(foreign).await {
        Err(tungstenite::Error::Http(res)) => assert_eq!(res.status(), 403),
        other => panic!("foreign Origin accepted: {other:?}"),
    }

    let (mut ws, res) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .unwrap();
    assert_eq!(res.status(), 101);
    let mut next = async || match ws.next().await {
        Some(Ok(tungstenite::Message::Text(t))) => t.to_string(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(next().await, "delta@3");
    apply(&state, &hub, None);
    assert_eq!(next().await, "delta@4");
    server.abort();
}
