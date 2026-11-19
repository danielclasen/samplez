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
use crate::replica::{MetricKind, ReplicaInfo, ReplicaMetric, ReplicatedState};

/// `/healthz` fails once the heartbeat has not advanced for this long.
pub const HEARTBEAT_WINDOW: Duration = Duration::from_secs(1);
/// `/readyz` and `/metrics` answer 503 when the replicated state's reads
/// have not returned within this long.
pub const REPLICA_READ_TIMEOUT: Duration = Duration::from_secs(1);
/// Pool threads the ops listener lends to replicated-state reads; further
/// reads wait for one and time out.
pub(crate) const MAX_BLOCKING_READS: usize = 4;
pub(crate) const SAMPLE_PERIOD: Duration = Duration::from_millis(100);

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

    /// Samples the heartbeat every 100 ms; never returns. [`Bound::serve`]
    /// samples on the ops listener's thread instead, so that a busy runtime
    /// does not stall the sampling.
    ///
    /// [`Bound::serve`]: crate::Bound::serve
    pub async fn watch_heartbeat(self) {
        let mut seen = 0;
        let mut ticks = tokio::time::interval(SAMPLE_PERIOD);
        loop {
            ticks.tick().await;
            self.sample_heartbeat(&mut seen);
        }
    }

    /// Notes when the heartbeat last differed from `seen`.
    pub(crate) fn sample_heartbeat(&self, seen: &mut u64) {
        let value = self.0.heartbeat.load(Ordering::Relaxed);
        if value != *seen {
            *seen = value;
            // A poisoned lock still holds an instant: the sampling goes on.
            let mut advanced = self.0.advanced.lock().unwrap_or_else(|e| {
                tracing::error!("the heartbeat sampling took a poisoned lock");
                e.into_inner()
            });
            *advanced = Some(Instant::now());
        }
    }

    fn healthy(&self) -> bool {
        self.0
            .advanced
            .lock()
            .expect("ops lock poisoned")
            .is_some_and(|t| t.elapsed() <= HEARTBEAT_WINDOW)
    }

    /// API serving, engine started and the replicated state's term,
    /// [`ReplicaInfo::ready`].
    fn ready(&self, replica: &ReplicaInfo) -> bool {
        self.0.api_serving.load(Ordering::Relaxed)
            && self.0.engine_started.load(Ordering::Relaxed)
            && replica.ready
    }

    /// `/readyz` answers with the replica's [`ReplicaInfo`]; `/metrics` adds
    /// its id and role as `samplez_replica_info`. The role does not enter
    /// readiness. `/healthz` reads the heartbeat alone and answers `{"ok":
    /// bool}`: it takes no lock of the replicated state, so a replica busy
    /// under its own locks cannot fail a liveness probe.
    ///
    /// The replicated state is read on the blocking pool, so a read stuck on
    /// a lock holds neither the listener's thread nor `/healthz`: after
    /// [`REPLICA_READ_TIMEOUT`] `/readyz` and `/metrics` answer 503.
    pub fn router(&self, state: Arc<dyn ReplicatedState>) -> Router {
        let health = self.clone();
        let ready = self.clone();
        let metrics = self.0.metrics.clone();
        let (s2, s3) = (state.clone(), state);
        Router::new()
            .route(
                "/healthz",
                get(move || async move { liveness(health.healthy()) }),
            )
            .route(
                "/readyz",
                get(move || {
                    let (ready, state) = (ready.clone(), s2.clone());
                    async move {
                        match read(move || state.replica()).await {
                            Ok(replica) => status(ready.ready(&replica), replica).into_response(),
                            Err(timeout) => timed_out(timeout, true),
                        }
                    }
                }),
            )
            .route(
                "/metrics",
                get(move || {
                    let (metrics, state) = (metrics.clone(), s3.clone());
                    async move {
                        let read = read(move || (state.replica(), state.replica_metrics())).await;
                        let (replica, samples) = match read {
                            Ok(r) => r,
                            Err(timeout) => return timed_out(timeout, false),
                        };
                        let labels = [("replica", replica.id.as_str()), ("role", &replica.role)];
                        let text = format!(
                            "# HELP samplez_replica_info This replica's id and role.\n\
                             # TYPE samplez_replica_info gauge\n\
                             samplez_replica_info{} 1\n{}{}",
                            label_set(labels),
                            render_replica(&replica.id, &samples),
                            metrics.render()
                        );
                        (
                            [(
                                header::CONTENT_TYPE,
                                "text/plain; version=0.0.4; charset=utf-8",
                            )],
                            text,
                        )
                            .into_response()
                    }
                }),
            )
    }
}

/// Why a read of the replicated state gave no answer.
enum ReadFailed {
    Timeout,
    Panicked,
}

/// Runs `f` on the blocking pool for at most [`REPLICA_READ_TIMEOUT`]. A
/// read that times out keeps its blocking thread until the lock it waits
/// on frees. At most [`MAX_BLOCKING_READS`] are stranded so; later
/// requests time out after 1 s and run once the lock frees.
async fn read<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, ReadFailed> {
    match tokio::time::timeout(REPLICA_READ_TIMEOUT, tokio::task::spawn_blocking(f)).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_)) => Err(ReadFailed::Panicked),
        Err(_) => Err(ReadFailed::Timeout),
    }
}

fn timed_out(why: ReadFailed, json: bool) -> axum::response::Response {
    let msg = match why {
        ReadFailed::Timeout => format!(
            "timeout: the replicated state did not answer within {} ms",
            REPLICA_READ_TIMEOUT.as_millis()
        ),
        ReadFailed::Panicked => "the replicated state read panicked".to_owned(),
    };
    if json {
        let body = serde_json::json!({ "ok": false, "error": msg });
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, msg).into_response()
    }
}

/// The replication's metrics in the text exposition format, each sample
/// labelled with replica `id`, a name's help and type before its first
/// sample.
fn render_replica(id: &str, samples: &[ReplicaMetric]) -> String {
    let mut out = String::new();
    let mut last = None;
    for m in samples {
        if last != Some(m.name) {
            let kind = match m.kind {
                MetricKind::Counter => "counter",
                MetricKind::Gauge => "gauge",
            };
            out.push_str(&format!(
                "# HELP {} {}\n# TYPE {} {kind}\n",
                m.name, m.help, m.name
            ));
            last = Some(m.name);
        }
        let labels =
            std::iter::once(("replica", id)).chain(m.labels.iter().map(|(k, v)| (*k, v.as_str())));
        out.push_str(&format!("{}{} {}\n", m.name, label_set(labels), m.value));
    }
    out
}

fn liveness(ok: bool) -> impl IntoResponse + use<> {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(serde_json::json!({ "ok": ok })))
}

fn status(ok: bool, replica: ReplicaInfo) -> impl IntoResponse + use<> {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = serde_json::json!({ "ok": ok, "replica": replica });
    (code, Json(body))
}
