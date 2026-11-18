//! The registration client against in-process mock Registration APIs that
//! answer as IS-04 v1.3 describes and record every request.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use samplez_nmos::uuid::{Uuid, uuid};
use samplez_nmos::*;
use serde_json::{Value, json};
use tokio::time::Instant;
use tower::ServiceExt;

const HEARTBEAT: Duration = Duration::from_millis(300);

#[derive(Debug, Clone)]
struct Seen {
    at: Instant,
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

impl Seen {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
    fn is_heartbeat(&self) -> bool {
        self.path.contains("/health/nodes/")
    }
    fn is_post(&self) -> bool {
        self.method == Method::POST && self.path.ends_with("/resource")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Up,
    /// Every request 503.
    Down,
    /// Every resource POST 200, as for a node still registered.
    FirstRegistration,
    /// No request is ever answered.
    Hang,
}

#[derive(Default)]
struct Mock {
    seen: Vec<Seen>,
    ids: BTreeSet<String>,
    mode: Option<Mode>,
    /// Answers for the next heartbeats, before the mode applies.
    next_heartbeats: Vec<(StatusCode, Option<String>)>,
    /// Answers for the next resource POSTs, before the mode applies.
    next_posts: Vec<(StatusCode, Option<String>)>,
    /// Resources of this type are refused with 400.
    refuse: Option<&'static str>,
}

#[derive(Clone)]
struct Registry {
    url: String,
    state: Arc<Mutex<Mock>>,
}

impl Registry {
    async fn start(mode: Mode) -> Registry {
        let state = Arc::new(Mutex::new(Mock {
            mode: Some(mode),
            ..Mock::default()
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/{*rest}", any(answer))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Registry { url, state }
    }

    fn set(&self, mode: Mode) {
        self.state.lock().unwrap().mode = Some(mode);
    }

    fn forget(&self) {
        self.state.lock().unwrap().ids.clear();
    }

    fn next_heartbeat(&self, status: StatusCode, location: Option<String>) {
        self.state
            .lock()
            .unwrap()
            .next_heartbeats
            .push((status, location));
    }

    fn next_post(&self, status: StatusCode, location: Option<String>) {
        self.state
            .lock()
            .unwrap()
            .next_posts
            .push((status, location));
    }

    fn seen(&self) -> Vec<Seen> {
        self.state.lock().unwrap().seen.clone()
    }

    fn clear(&self) {
        self.state.lock().unwrap().seen.clear();
    }
}

async fn answer(
    State(state): State<Arc<Mutex<Mock>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_owned();
    let hang = {
        let mut m = state.lock().unwrap();
        m.seen.push(Seen {
            at: Instant::now(),
            method: method.clone(),
            path: path.clone(),
            headers,
            body: body.clone(),
        });
        m.mode == Some(Mode::Hang)
    };
    if hang {
        std::future::pending::<()>().await;
    }
    let mut m = state.lock().unwrap();
    let reply = |status: StatusCode, location: Option<String>| {
        let mut r = status.into_response();
        if let Some(l) = location {
            r.headers_mut().insert(header::LOCATION, l.parse().unwrap());
        }
        r
    };
    if m.mode == Some(Mode::Down) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if path.contains("/health/nodes/") {
        if !m.next_heartbeats.is_empty() {
            let (status, location) = m.next_heartbeats.remove(0);
            return reply(status, location);
        }
        let id = path.rsplit('/').next().unwrap();
        return if m.ids.contains(id) {
            (StatusCode::OK, json!({ "health": "1" }).to_string()).into_response()
        } else {
            StatusCode::NOT_FOUND.into_response()
        };
    }
    if method == Method::DELETE {
        m.ids.clear();
        return StatusCode::NO_CONTENT.into_response();
    }
    if !m.next_posts.is_empty() {
        let (status, location) = m.next_posts.remove(0);
        return reply(status, location);
    }
    let v: Value = serde_json::from_slice(&body).unwrap();
    if m.refuse == v["type"].as_str() {
        return reply(StatusCode::BAD_REQUEST, None);
    }
    let id = v["data"]["id"].as_str().unwrap().to_owned();
    let known = !m.ids.insert(id) || m.mode == Some(Mode::FirstRegistration);
    let status = if known {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        v["data"].to_string(),
    )
        .into_response()
}

/// Applies after `delay`.
struct Product {
    delay: Duration,
}

impl Connections for Product {
    fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
        Box::pin(async { Ok(FlowSeen::Absent) })
    }
    fn lead(&self) -> Duration {
        Duration::ZERO
    }
    fn apply(&self, _: Port, _: Active, _: Option<Tai>, _: u64) -> BoxFuture<'_, Result<(), Fail>> {
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(())
        })
    }
}

fn node(registries: &[&Registry], apply: Duration) -> Nmos {
    let domain = uuid!("11111111-2222-4333-8444-555555555555");
    let model = NodeModel {
        label: "mixer".into(),
        description: String::new(),
        senders: vec![SenderDesc {
            key: "pgm".into(),
            label: "Programme".into(),
            channels: vec![ChannelDesc {
                label: "M".into(),
                symbol: None,
            }],
            group: "pgm".into(),
            domain,
            flow: uuid!("aaaaaaaa-0000-4000-8000-000000000001"),
        }],
        receivers: ["a", "b"]
            .into_iter()
            .map(|k| ReceiverDesc {
                key: k.into(),
                label: k.into(),
                channels: ChannelCount::Exactly(1),
                group: k.into(),
                domain,
                flow: None,
            })
            .collect(),
    };
    let cfg = NmosConfig {
        seed: "registration".into(),
        advertise_host: "192.0.2.10".into(),
        advertise_port: 8081,
        asset: Asset {
            manufacturer: "danielclasen".into(),
            product: "samplez".into(),
            instance_id: "registration".into(),
            functions: vec!["Mixer".into()],
        },
        heartbeat: HEARTBEAT,
        registries: registries.iter().map(|r| r.url.clone()).collect(),
        dns_sd_domain: None,
    };
    Nmos::new(
        model,
        cfg,
        Arc::new(Product { delay: apply }),
        &NoStore,
        SystemTai,
    )
    .unwrap()
}

fn types(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .filter(|s| s.is_post())
        .map(|s| s.json()["type"].as_str().unwrap().to_owned())
        .collect()
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registers_parents_first_then_heartbeats_on_cadence() {
    let r = Registry::start(Mode::Up).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("five heartbeats", || {
        r.seen().iter().filter(|s| s.is_heartbeat()).count() >= 5
    })
    .await;
    task.abort();
    let seen = r.seen();
    assert_eq!(
        types(&seen),
        [
            "node", "device", "source", "flow", "sender", "receiver", "receiver"
        ]
    );
    let id = n.node_id().to_string();
    for s in seen.iter().filter(|s| s.is_post()) {
        assert_eq!(s.path, "/x-nmos/registration/v1.3/resource");
        assert_eq!(s.headers[header::CONTENT_TYPE], "application/json");
        assert_eq!(s.headers[header::ACCEPT], "application/json");
        assert_eq!(
            s.headers[header::CONTENT_LENGTH],
            s.body.len().to_string().as_str()
        );
    }
    let beats: Vec<&Seen> = seen.iter().filter(|s| s.is_heartbeat()).collect();
    for b in &beats {
        assert_eq!(b.method, Method::POST);
        assert_eq!(
            b.path,
            format!("/x-nmos/registration/v1.3/health/nodes/{id}")
        );
        assert!(b.body.is_empty());
        assert_eq!(b.headers[header::CONTENT_LENGTH], "0");
        assert!(b.headers.get(header::CONTENT_TYPE).is_none());
    }
    // IS-04-01 allows 0.5 s either way at 5 s; this holds the same
    // proportion at 300 ms.
    let node_post = seen.iter().find(|s| s.is_post()).unwrap().at;
    let mut last = node_post;
    for b in &beats {
        let gap = b.at - last;
        assert!(gap.abs_diff(HEARTBEAT) < HEARTBEAT / 10, "{gap:?}");
        last = b.at;
    }
    assert_eq!(
        n.status().registry,
        Some(format!("{}/x-nmos/registration/v1.3", r.url))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_200_on_first_registration_deletes_the_node_and_registers_again() {
    let r = Registry::start(Mode::FirstRegistration).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    task.abort();
    let seen = r.seen();
    let calls: Vec<(String, String)> = seen
        .iter()
        .take(3)
        .map(|s| (s.method.to_string(), s.path.clone()))
        .collect();
    let id = n.node_id();
    assert_eq!(
        calls,
        [
            ("POST".into(), "/x-nmos/registration/v1.3/resource".into()),
            (
                "DELETE".into(),
                format!("/x-nmos/registration/v1.3/resource/nodes/{id}")
            ),
            ("POST".into(), "/x-nmos/registration/v1.3/resource".into()),
        ]
    );
    // Only the Node is deleted, once.
    assert_eq!(
        seen.iter().filter(|s| s.method == Method::DELETE).count(),
        1
    );
    assert_eq!(types(&seen)[..2], ["node".to_owned(), "node".to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_404_heartbeat_registers_every_resource_again() {
    let r = Registry::start(Mode::Up).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    r.forget();
    r.clear();
    until("registered again", || types(&r.seen()).len() == 7).await;
    until("a heartbeat", || {
        r.seen()
            .iter()
            .any(|s| s.is_heartbeat() && s.at > r.seen()[0].at)
    })
    .await;
    task.abort();
    assert_eq!(
        types(&r.seen()),
        [
            "node", "device", "source", "flow", "sender", "receiver", "receiver"
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_409_unregisters_at_the_other_version_then_registers() {
    let r = Registry::start(Mode::Up).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    let id = n.node_id();
    r.next_heartbeat(
        StatusCode::CONFLICT,
        Some(format!("/x-nmos/registration/v1.2/health/nodes/{id}")),
    );
    r.clear();
    until("registered again", || types(&r.seen()).len() == 7).await;
    task.abort();
    let seen = r.seen();
    let delete = seen
        .iter()
        .position(|s| s.method == Method::DELETE)
        .expect("no DELETE");
    assert_eq!(
        seen[delete].path,
        format!("/x-nmos/registration/v1.2/resource/nodes/{id}")
    );
    assert!(delete < seen.iter().position(Seen::is_post).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_registry_hands_over_to_the_next_with_a_heartbeat_first() {
    let (a, b) = (
        Registry::start(Mode::Up).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    assert!(b.seen().is_empty());
    // The registries share their store, as a registry cluster does.
    b.state.lock().unwrap().ids = a.state.lock().unwrap().ids.clone();
    a.set(Mode::Down);
    let failed = Instant::now();
    until("a heartbeat on b", || {
        b.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    assert!(
        failed.elapsed() < HEARTBEAT * 2 + HEARTBEAT / 2,
        "{:?}",
        failed.elapsed()
    );
    tokio::time::sleep(HEARTBEAT * 2).await;
    task.abort();
    let seen = b.seen();
    // b held the node and nothing changed, so nothing is posted to it
    // (IS-04-01 test_16).
    assert!(seen[0].is_heartbeat());
    assert!(types(&seen).is_empty());
    assert_eq!(
        n.status().registry,
        Some(format!("{}/x-nmos/registration/v1.3", b.url))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_that_failed_a_registry_reaches_the_next() {
    let (a, b) = (
        Registry::start(Mode::Up).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    b.state.lock().unwrap().ids = a.state.lock().unwrap().ids.clone();
    a.set(Mode::Down);
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(uuid!("11111111-2222-4333-8444-555555555555")),
        flow: Some(uuid!("cccccccc-0000-4000-8000-000000000001")),
    };
    // The POST of this change is what finds a down.
    n.external_change(Port::Receiver(0), switched);
    until("the receiver on b", || !types(&b.seen()).is_empty()).await;
    tokio::time::sleep(HEARTBEAT).await;
    task.abort();
    // Right after the heartbeat that showed b holds the node, and only the
    // resource that changed.
    let seen = b.seen();
    assert!(seen[0].is_heartbeat(), "{seen:?}");
    assert!(seen[1].is_post(), "{seen:?}");
    assert_eq!(types(&seen), ["receiver"]);
    assert_eq!(seen[1].json()["data"]["subscription"]["active"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registry_the_node_returns_to_gets_what_changed_while_away() {
    // Two registries that do not share their store.
    let (a, b) = (
        Registry::start(Mode::Up).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    a.set(Mode::Down);
    until("registered on b", || types(&b.seen()).len() == 7).await;
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(uuid!("11111111-2222-4333-8444-555555555555")),
        flow: Some(uuid!("cccccccc-0000-4000-8000-000000000001")),
    };
    n.external_change(Port::Receiver(0), switched);
    until("the change on b", || types(&b.seen()).len() == 8).await;
    // Back to a, which still holds the node at the versions before.
    a.clear();
    a.set(Mode::Up);
    b.set(Mode::Down);
    until("the change on a", || !types(&a.seen()).is_empty()).await;
    tokio::time::sleep(HEARTBEAT).await;
    task.abort();
    let seen = a.seen();
    assert!(seen[0].is_heartbeat(), "{seen:?}");
    assert_eq!(types(&seen), ["receiver"]);
    let post = seen.iter().find(|s| s.is_post()).unwrap();
    assert_eq!(post.json()["data"]["subscription"]["active"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registry_that_failed_the_first_registration_gets_a_heartbeat_only_later() {
    let (a, b) = (
        Registry::start(Mode::Down).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on b", || {
        b.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    // a refused the first registration; it now holds the node, as a
    // cluster with b would.
    assert!(!a.seen().is_empty());
    a.state.lock().unwrap().ids = b.state.lock().unwrap().ids.clone();
    a.clear();
    a.set(Mode::Up);
    b.set(Mode::Down);
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    tokio::time::sleep(HEARTBEAT * 2).await;
    task.abort();
    // IS-04-01 test_16: no POST of the node, nothing posted at all.
    let seen = a.seen();
    assert!(types(&seen).is_empty(), "{:?}", types(&seen));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_registry_just_failed_is_tried_last() {
    let (a, b) = (
        Registry::start(Mode::Up).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    b.state.lock().unwrap().ids = a.state.lock().unwrap().ids.clone();
    a.clear();
    a.set(Mode::Down);
    until("a heartbeat on b", || {
        b.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    tokio::time::sleep(HEARTBEAT).await;
    task.abort();
    // The heartbeat that failed, and nothing after it: the next round went
    // to b first.
    let seen = a.seen();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].is_heartbeat());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_unknown_to_the_next_registry_registers_there() {
    let (a, b) = (
        Registry::start(Mode::Up).await,
        Registry::start(Mode::Up).await,
    );
    let n = node(&[&a, &b], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat on a", || {
        a.seen().iter().any(Seen::is_heartbeat)
    })
    .await;
    a.set(Mode::Down);
    until("registered on b", || types(&b.seen()).len() == 7).await;
    task.abort();
    assert!(b.seen()[0].is_heartbeat());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_registry_failing_backs_off_exponentially() {
    let r = Registry::start(Mode::Down).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("three attempts", || r.seen().len() >= 3).await;
    task.abort();
    let at: Vec<Instant> = r.seen().iter().map(|s| s.at).collect();
    let (first, second) = (at[1] - at[0], at[2] - at[1]);
    assert!(
        first >= Duration::from_millis(950) && first < Duration::from_millis(1500),
        "{first:?}"
    );
    assert!(
        second >= Duration::from_millis(1950) && second < Duration::from_millis(2500),
        "{second:?}"
    );
    assert_eq!(n.status().registry, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeats_continue_while_an_apply_blocks_and_changes_are_posted() {
    let r = Registry::start(Mode::Up).await;
    let apply = Duration::from_secs(3);
    let n = node(&[&r], apply);
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    let receiver = n.status().ports[1].port;
    assert_eq!(receiver, Port::Receiver(0));
    let id = {
        let req = Request::get("/x-nmos/node/v1.3/receivers")
            .body(Body::empty())
            .unwrap();
        let body = to_bytes(
            n.router().oneshot(req).await.unwrap().into_body(),
            usize::MAX,
        )
        .await
        .unwrap();
        serde_json::from_slice::<Value>(&body).unwrap()[0]["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    r.clear();
    let patch = json!({
        "master_enable": true,
        "transport_params": [{ "mxl_flow_id": "cccccccc-0000-4000-8000-000000000001" }],
        "activation": { "mode": "activate_immediate" },
    });
    let req = Request::patch(format!(
        "/x-nmos/connection/v1.2/single/receivers/{id}/staged"
    ))
    .header(header::CONTENT_TYPE, "application/json")
    .body(Body::from(patch.to_string()))
    .unwrap();
    let started = Instant::now();
    assert_eq!(
        n.router().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "the PATCH waited for apply"
    );
    tokio::time::sleep(apply).await;
    task.abort();
    let seen = r.seen();
    let beats = seen.iter().filter(|s| s.is_heartbeat()).count();
    assert!(beats >= 9, "{beats} heartbeats in {apply:?}");
    // The receiver's new subscription reached the registry at once, not
    // after apply returned.
    let post = seen
        .iter()
        .find(|s| s.is_post())
        .expect("the receiver was not posted");
    assert_eq!(post.json()["data"]["id"], id);
    assert_eq!(post.json()["data"]["subscription"]["active"], true);
    assert!(post.at - started < Duration::from_millis(500));
    assert_eq!(types(&seen), ["receiver"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregister_deletes_the_node() {
    let r = Registry::start(Mode::Up).await;
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    task.abort();
    n.unregister().await;
    let seen = r.seen();
    let last = seen.last().unwrap();
    assert_eq!(
        (last.method.clone(), last.path.clone()),
        (
            Method::DELETE,
            format!("/x-nmos/registration/v1.3/resource/nodes/{}", n.node_id())
        )
    );
    assert_eq!(n.status().registry, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_409_on_a_post_unregisters_at_the_other_version_and_posts_again() {
    let r = Registry::start(Mode::Up).await;
    let n = node(&[&r], Duration::ZERO);
    let id = n.node_id();
    r.next_post(
        StatusCode::CONFLICT,
        Some(format!("/x-nmos/registration/v1.2/resource/nodes/{id}")),
    );
    let task = tokio::spawn(n.registration());
    until("a heartbeat", || r.seen().iter().any(Seen::is_heartbeat)).await;
    task.abort();
    let seen = r.seen();
    let calls: Vec<(String, String)> = seen
        .iter()
        .take(3)
        .map(|s| (s.method.to_string(), s.path.clone()))
        .collect();
    assert_eq!(
        calls,
        [
            ("POST".into(), "/x-nmos/registration/v1.3/resource".into()),
            (
                "DELETE".into(),
                format!("/x-nmos/registration/v1.2/resource/nodes/{id}")
            ),
            ("POST".into(), "/x-nmos/registration/v1.3/resource".into()),
        ]
    );
    assert_eq!(
        types(&seen),
        [
            "node", "node", "device", "source", "flow", "sender", "receiver", "receiver"
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_resource_is_not_sent_again_unchanged() {
    let r = Registry::start(Mode::Up).await;
    r.state.lock().unwrap().refuse = Some("device");
    let n = node(&[&r], Duration::ZERO);
    let task = tokio::spawn(n.registration());
    until("two heartbeats", || {
        r.seen().iter().filter(|s| s.is_heartbeat()).count() >= 2
    })
    .await;
    // The registration went on past the refusal and stayed on r.
    assert_eq!(
        types(&r.seen()),
        [
            "node", "device", "source", "flow", "sender", "receiver", "receiver"
        ]
    );
    r.clear();
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(uuid!("11111111-2222-4333-8444-555555555555")),
        flow: Some(uuid!("cccccccc-0000-4000-8000-000000000001")),
    };
    n.external_change(Port::Receiver(0), switched);
    until("the receiver", || !types(&r.seen()).is_empty()).await;
    tokio::time::sleep(HEARTBEAT).await;
    task.abort();
    assert_eq!(types(&r.seen()), ["receiver"]);
    assert_eq!(
        n.status().registry,
        Some(format!("{}/x-nmos/registration/v1.3", r.url))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_registry_fails_over_after_the_timeout_a_503_at_once() {
    for (mode, waited) in [(Mode::Hang, true), (Mode::Down, false)] {
        let (a, b) = (Registry::start(mode).await, Registry::start(Mode::Up).await);
        let n = node(&[&a, &b], Duration::ZERO);
        let task = tokio::spawn(n.registration());
        until("registered on b", || types(&b.seen()).len() == 7).await;
        task.abort();
        let asked = a.seen()[0].at;
        let gap = b.seen()[0].at - asked;
        // The request timeout is the heartbeat period; it starts as the
        // request leaves, a little before the registry sees it.
        if waited {
            assert!(
                gap >= HEARTBEAT * 3 / 4 && gap < HEARTBEAT * 2,
                "{mode:?}: {gap:?}"
            );
        } else {
            assert!(gap < HEARTBEAT / 2, "{mode:?}: {gap:?}");
        }
        assert_eq!(a.seen().len(), 1, "{mode:?}");
    }
}
