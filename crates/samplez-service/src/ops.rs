//! The ops listener: `/healthz`, `/readyz` and `/metrics`, unauthenticated.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use tokio::time::Instant;

use crate::metrics::{Metrics, label_set};
use crate::replica::ReplicatedState;

/// `/healthz` fails once the heartbeat has not advanced for this long.
pub const HEARTBEAT_WINDOW: Duration = Duration::from_secs(1);
const SAMPLE_PERIOD: Duration = Duration::from_millis(100);

/// Health, readiness and metrics state shared by the engine and the ops
/// listener.
#[derive(Clone, Default)]
pub struct Ops(Arc<Inner>);

#[derive(Default)]
struct Inner {
    heartbeat: Arc<AtomicU64>,
    engine_started: AtomicBool,
    api_serving: AtomicBool,
    /// When [`Ops::watch_heartbeat`] last saw the heartbeat advance.
    advanced: Mutex<Option<Instant>>,
    metrics: Metrics,
}

/// The RT thread's liveness counter.
#[derive(Clone)]
pub struct Heartbeat(Arc<AtomicU64>);

impl Heartbeat {
    /// One relaxed atomic add: no allocation, no lock, no syscall.
    pub fn beat(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Ops {
    pub fn heartbeat(&self) -> Heartbeat {
        Heartbeat(self.0.heartbeat.clone())
    }

    pub fn metrics(&self) -> &Metrics {
        &self.0.metrics
    }

    pub fn set_engine_started(&self, started: bool) {
        self.0.engine_started.store(started, Ordering::Relaxed);
    }

    pub(crate) fn set_api_serving(&self, serving: bool) {
        self.0.api_serving.store(serving, Ordering::Relaxed);
    }

    /// Samples the heartbeat every 100 ms; never returns. `Bound::serve`
    /// runs it.
    pub async fn watch_heartbeat(self) {
        let mut seen = 0;
        let mut ticks = tokio::time::interval(SAMPLE_PERIOD);
        loop {
            ticks.tick().await;
            let value = self.0.heartbeat.load(Ordering::Relaxed);
            if value != seen {
                seen = value;
                *self.0.advanced.lock().expect("ops lock poisoned") = Some(Instant::now());
            }
        }
    }

    fn healthy(&self) -> bool {
        self.0
            .advanced
            .lock()
            .expect("ops lock poisoned")
            .is_some_and(|t| t.elapsed() <= HEARTBEAT_WINDOW)
    }

    fn ready(&self) -> bool {
        self.0.api_serving.load(Ordering::Relaxed) && self.0.engine_started.load(Ordering::Relaxed)
    }

    /// `/healthz` and `/readyz` answer with the replica's id and role;
    /// `/metrics` adds them as `samplez_replica_info`. The role does not
    /// enter readiness.
    pub fn router(&self, state: Arc<dyn ReplicatedState>) -> Router {
        let health = self.clone();
        let ready = self.clone();
        let metrics = self.0.metrics.clone();
        let (s1, s2, s3) = (state.clone(), state.clone(), state);
        Router::new()
            .route(
                "/healthz",
                get(move || async move { status(health.healthy(), &*s1) }),
            )
            .route(
                "/readyz",
                get(move || async move { status(ready.ready(), &*s2) }),
            )
            .route(
                "/metrics",
                get(move || async move {
                    let replica = s3.replica();
                    let labels = [("replica", replica.id.as_str()), ("role", &replica.role)];
                    let text = format!(
                        "# HELP samplez_replica_info This replica's id and role.\n\
                         # TYPE samplez_replica_info gauge\n\
                         samplez_replica_info{} 1\n{}",
                        label_set(labels),
                        metrics.render()
                    );
                    (
                        [(
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        text,
                    )
                }),
            )
    }
}

fn status(ok: bool, state: &dyn ReplicatedState) -> impl IntoResponse + use<> {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = serde_json::json!({ "ok": ok, "replica": state.replica() });
    (code, Json(body))
}
