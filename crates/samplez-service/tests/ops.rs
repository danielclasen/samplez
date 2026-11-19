mod common;

use std::time::Duration;

use axum::http::{StatusCode, header};
use common::*;
use samplez_service::{MetricKind, Metrics, Ops, ReplicaMetric};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(start_paused = true)]
async fn healthz_follows_the_heartbeat_within_one_second() {
    let ops = Ops::default();
    let router = ops.router(FakeState::new("r1", "leader"));
    let heartbeat = ops.heartbeat();
    tokio::spawn(ops.clone().watch_heartbeat());
    let healthz = || status(&router, get_req("/healthz"));

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        healthz().await,
        StatusCode::SERVICE_UNAVAILABLE,
        "never beat"
    );

    heartbeat.beat();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(healthz().await, StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        healthz().await,
        StatusCode::OK,
        "0.95 s since the last advance"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        healthz().await,
        StatusCode::SERVICE_UNAVAILABLE,
        "stopped for over 1 s"
    );

    for _ in 0..30 {
        heartbeat.beat();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(healthz().await, StatusCode::OK, "advancing for 3 s");
}

/// A liveness probe that waits on the replicated state's locks fails
/// whenever the replica machinery holds them, though the process lives.
#[allow(clippy::await_holding_lock, reason = "the held lock is the test")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_takes_no_lock_of_the_replicated_state() {
    let ops = Ops::default();
    let state = FakeState::new("r1", "leader");
    let router = ops.router(state.clone());
    ops.heartbeat().beat();
    tokio::spawn(ops.clone().watch_heartbeat());
    tokio::time::sleep(Duration::from_millis(250)).await;

    let held = state.replica.lock().unwrap();
    let healthz = tokio::spawn(async move { send(&router, get_req("/healthz")).await });
    let answered = tokio::time::timeout(Duration::from_secs(2), healthz).await;
    drop(held);

    let (code, _, body) = answered
        .expect("/healthz waited on the replicated state")
        .unwrap();
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body, r#"{"ok":true}"#);
}

/// The ops listener answers while the task that serves the others is held
/// by a blocking call, as a product's replica or engine future holding its
/// worker would hold it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_answers_while_the_task_serving_the_listeners_is_blocked() {
    let ops = Ops::default();
    let state = FakeState::new("r1", "leader");
    let mut config = config("none", "blocked");
    let loopback = serde_json::json!({ "bind": "127.0.0.1", "port": 0 });
    config["listeners"] = serde_json::json!({ "api": loopback, "ops": loopback, "nmos": loopback });
    let bound = service_from(config, ops.clone(), state)
        .bind()
        .await
        .unwrap();
    let addr = bound.ops_addr().unwrap();
    let heartbeat = ops.heartbeat();
    let beating = tokio::spawn(async move {
        loop {
            heartbeat.beat();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let serving = tokio::spawn(async move {
        tokio::select! {
            biased;
            r = bound.serve() => r,
            () = async { std::thread::sleep(Duration::from_secs(2)) } => Ok(()),
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let get = async {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: ops\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        reply
    };
    let reply = tokio::time::timeout(Duration::from_secs(1), get)
        .await
        .expect("/healthz was not answered while the serving task was blocked");
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    assert!(reply.ends_with(r#"{"ok":true}"#), "{reply}");

    serving.abort();
    beating.abort();
}

#[tokio::test]
async fn readyz_is_api_serving_engine_started_and_the_replicated_term_whatever_the_role() {
    let ops = Ops::default();
    let state = FakeState::new("r2", "follower");
    let mut config = config("none", "readyz");
    let loopback = serde_json::json!({ "bind": "127.0.0.1", "port": 0 });
    config["listeners"] = serde_json::json!({ "api": loopback, "ops": loopback, "nmos": loopback });
    let service = service_from(config, ops.clone(), state.clone());
    let router = service.routers().ops;
    let readyz = || send(&router, get_req("/readyz"));

    let bound = service.bind().await.unwrap();
    ops.set_engine_started(true);
    assert_eq!(
        readyz().await.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "API not serving yet"
    );

    let serving = tokio::spawn(bound.serve());
    tokio::task::yield_now().await;
    let (code, _, body) = readyz().await;
    assert_eq!(code, StatusCode::OK, "a follower is ready");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        body["replica"],
        serde_json::json!({
            "id": "r2", "role": "follower", "leader": "r1", "inSync": true, "lag": 0,
            "ready": true, "commitable": true,
        })
    );

    state.replica.lock().unwrap().commitable = false;
    let (code, _, body) = readyz().await;
    assert_eq!(code, StatusCode::OK, "taking no writes does not enter it");
    assert!(body.contains("\"commitable\":false"), "{body}");
    state.replica.lock().unwrap().ready = false;
    let (code, _, body) = readyz().await;
    assert_eq!(
        code,
        StatusCode::SERVICE_UNAVAILABLE,
        "the replicated term is false"
    );
    assert!(body.contains("\"ready\":false"), "{body}");
    state.replica.lock().unwrap().ready = true;

    ops.set_engine_started(false);
    assert_eq!(
        readyz().await.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "engine stopped"
    );
    ops.set_engine_started(true);
    state.replica.lock().unwrap().role = "leader".into();
    let (code, _, body) = readyz().await;
    assert_eq!(code, StatusCode::OK);
    assert!(body.contains("\"leader\""));

    serving.abort();
    let _ = serving.await;
    assert_eq!(
        readyz().await.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "API stopped"
    );
}

#[tokio::test]
async fn metrics_render_as_prometheus_text() {
    let ops = Ops::default();
    let m = ops.metrics();
    let late = m.counter(
        "samplez_late_blocks_total",
        "Blocks past their deadline.",
        &[],
    );
    let gaps = m.counter(
        "samplez_input_gaps_total",
        "Input gaps.",
        &[("input", "a\"b\\c\nd")],
    );
    let other = m.counter("samplez_input_gaps_total", "Input gaps.", &[("input", "x")]);
    let wake = m.histogram(
        "samplez_wake_seconds",
        "Wake-up latency.",
        &[("thread", "rt")],
        &[0.25, 1.0],
    );
    late.add(3);
    gaps.add(1);
    other.add(2);
    for v in [0.125, 0.25, 0.5, 2.0] {
        wake.observe(v);
    }

    let (code, headers, body) = send(
        &ops.router(FakeState::new("r\"1", "leader")),
        get_req("/metrics"),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let expected = "\
# HELP samplez_replica_info This replica's id and role.
# TYPE samplez_replica_info gauge
samplez_replica_info{replica=\"r\\\"1\",role=\"leader\"} 1
# HELP samplez_late_blocks_total Blocks past their deadline.
# TYPE samplez_late_blocks_total counter
samplez_late_blocks_total 3
# HELP samplez_input_gaps_total Input gaps.
# TYPE samplez_input_gaps_total counter
samplez_input_gaps_total{input=\"a\\\"b\\\\c\\nd\"} 1
samplez_input_gaps_total{input=\"x\"} 2
# HELP samplez_wake_seconds Wake-up latency.
# TYPE samplez_wake_seconds histogram
samplez_wake_seconds_bucket{thread=\"rt\",le=\"0.25\"} 2
samplez_wake_seconds_bucket{thread=\"rt\",le=\"1\"} 3
samplez_wake_seconds_bucket{thread=\"rt\",le=\"+Inf\"} 4
samplez_wake_seconds_sum{thread=\"rt\"} 2.875
samplez_wake_seconds_count{thread=\"rt\"} 4
";
    assert_eq!(body, expected);
}

/// The replication's metrics reach `/metrics` labelled with the replica,
/// a name's help and type once before its samples.
#[tokio::test]
async fn metrics_carry_the_replications_own_with_the_replica_label() {
    let state = FakeState::new("r1", "follower");
    let sample = |name, kind, labels: Vec<(&'static str, String)>, value| ReplicaMetric {
        name,
        help: "Help.",
        kind,
        labels,
        value,
    };
    *state.metrics.lock().unwrap() = vec![
        sample("samplez_replica_serving", MetricKind::Gauge, vec![], 0.0),
        sample(
            "samplez_replica_state",
            MetricKind::Gauge,
            vec![("state", "committing".into())],
            0.0,
        ),
        sample(
            "samplez_replica_state",
            MetricKind::Gauge,
            vec![("state", "diverged".into())],
            1.0,
        ),
        sample(
            "samplez_replica_resync_failures_total",
            MetricKind::Counter,
            vec![],
            3.0,
        ),
    ];
    let (code, _, body) = send(&Ops::default().router(state), get_req("/metrics")).await;
    assert_eq!(code, StatusCode::OK);
    let expected = "\
# HELP samplez_replica_info This replica's id and role.
# TYPE samplez_replica_info gauge
samplez_replica_info{replica=\"r1\",role=\"follower\"} 1
# HELP samplez_replica_serving Help.
# TYPE samplez_replica_serving gauge
samplez_replica_serving{replica=\"r1\"} 0
# HELP samplez_replica_state Help.
# TYPE samplez_replica_state gauge
samplez_replica_state{replica=\"r1\",state=\"committing\"} 0
samplez_replica_state{replica=\"r1\",state=\"diverged\"} 1
# HELP samplez_replica_resync_failures_total Help.
# TYPE samplez_replica_resync_failures_total counter
samplez_replica_resync_failures_total{replica=\"r1\"} 3
";
    assert_eq!(body, expected);
}

#[test]
#[should_panic(expected = "registered as another type")]
fn a_name_keeps_its_type() {
    let m = Metrics::default();
    m.counter("samplez_x", "x", &[("a", "1")]);
    m.histogram("samplez_x", "x", &[("a", "2")], &[1.0]);
}

#[test]
fn registering_a_series_again_returns_it_and_remove_drops_it() {
    let m = Metrics::default();
    let first = m.counter("samplez_x_total", "x", &[("a", "1")]);
    let again = m.counter("samplez_x_total", "x", &[("a", "1")]);
    again.add(2);
    assert_eq!(first.get(), 2, "one series");
    let h = m.histogram("samplez_h", "h", &[], &[1.0]);
    m.histogram("samplez_h", "h", &[], &[1.0]).observe(0.5);
    h.observe(0.5);
    assert!(m.render().contains("samplez_h_count 2"));

    m.remove("samplez_x_total", &[("a", "1")]);
    m.remove("samplez_h", &[]);
    assert_eq!(m.render(), "");
    let fresh = m.counter("samplez_x_total", "x", &[("a", "1")]);
    assert_eq!(fresh.get(), 0, "registered anew after removal");
}

/// A gauge renders its last value, negative and non-finite ones included.
#[test]
fn gauges_render_the_last_value() {
    let m = Metrics::default();
    let g = m.gauge("samplez_loudness_lufs", "Loudness.", &[("output", "a")]);
    assert_eq!(g.get(), 0.0);
    g.set(1.5);
    g.set(-23.25);
    m.gauge("samplez_loudness_lufs", "Loudness.", &[("output", "b")])
        .set(f64::NEG_INFINITY);
    assert_eq!(
        m.render(),
        "# HELP samplez_loudness_lufs Loudness.\n\
         # TYPE samplez_loudness_lufs gauge\n\
         samplez_loudness_lufs{output=\"a\"} -23.25\n\
         samplez_loudness_lufs{output=\"b\"} -Inf\n"
    );
    let again = m.gauge("samplez_loudness_lufs", "Loudness.", &[("output", "a")]);
    assert_eq!(again.get(), -23.25, "one series");
}

#[test]
fn histograms_skip_nan_and_render_infinities() {
    let m = Metrics::default();
    let h = m.histogram("samplez_h", "h", &[], &[1.0]);
    h.observe(f64::NAN);
    assert!(m.render().contains("samplez_h_count 0"), "NaN not counted");
    h.observe(f64::INFINITY);
    assert!(
        m.render().contains("samplez_h_sum +Inf\n"),
        "{}",
        m.render()
    );
    let n = m.histogram("samplez_n", "n", &[], &[1.0]);
    n.observe(f64::NEG_INFINITY);
    assert!(m.render().contains("samplez_n_sum -Inf\n"));
}

#[test]
#[should_panic(expected = "not finite and ascending")]
fn histogram_bounds_are_finite() {
    Metrics::default().histogram("samplez_h", "h", &[], &[1.0, f64::INFINITY]);
}

#[test]
#[should_panic(expected = "registered with other bounds")]
fn a_histogram_keeps_its_bounds() {
    let m = Metrics::default();
    m.histogram("samplez_h", "h", &[], &[1.0]);
    m.histogram("samplez_h", "h", &[], &[2.0]);
}
