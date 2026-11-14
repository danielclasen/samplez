mod common;

use std::time::Duration;

use axum::http::{StatusCode, header};
use common::*;
use samplez_service::{Metrics, Ops};

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

#[tokio::test]
async fn readyz_is_api_serving_and_engine_started_whatever_the_role() {
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
        serde_json::json!({ "id": "r2", "role": "follower" })
    );

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

#[test]
#[should_panic(expected = "registered as another type")]
fn a_name_keeps_its_type() {
    let m = Metrics::default();
    m.counter("samplez_x", "x", &[("a", "1")]);
    m.histogram("samplez_x", "x", &[("a", "2")], &[1.0]);
}

#[test]
#[should_panic(expected = "with these labels is registered")]
fn a_series_registers_once() {
    let m = Metrics::default();
    m.counter("samplez_x", "x", &[("a", "1")]);
    m.counter("samplez_x", "x", &[("a", "1")]);
}
