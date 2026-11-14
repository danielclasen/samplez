#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::http::{HeaderMap, Request, StatusCode, header, request};
use axum::routing::get;
use axum::{Extension, Router};
use futures_util::future::BoxFuture;
use samplez_service::replica::{Ack, NotApplied};
use samplez_service::ws::{Delta, Snapshot};
use samplez_service::{
    Identity, Ops, Owner, Position, ReplicaInfo, ReplicatedState, Service, ServiceConfig,
};
use serde_json::json;
use tokio::sync::oneshot;
use tower::ServiceExt;

pub const HOST: &str = "localhost:8080";
pub const API_TOKEN: &str = "api-token";
pub const MACHINE_TOKEN: &str = "machine-token";

/// A replicated state whose deltas are applied by hand; `receive` stands in
/// for replication from another replica.
pub struct FakeState {
    pub replica: Mutex<ReplicaInfo>,
    pub epoch: Mutex<u64>,
    log: Mutex<Vec<Delta>>,
    /// Deltas at or below this revision are no longer held.
    forgotten: Mutex<u64>,
    /// The replicated table of applied command ids.
    commands: Mutex<HashMap<String, u64>>,
    /// Every `renew_sessions` call.
    pub renewals: Mutex<Vec<Vec<Owner>>>,
    /// How long `snapshot` takes.
    pub snapshot_delay: Mutex<Duration>,
    /// Submitted commands that `commit` applies.
    submitted: Mutex<Vec<(String, oneshot::Sender<Ack>)>>,
}

impl FakeState {
    pub fn new(id: &str, role: &str) -> Arc<FakeState> {
        Arc::new(FakeState {
            replica: Mutex::new(ReplicaInfo {
                id: id.into(),
                role: role.into(),
                leader: Some("r1".into()),
                in_sync: true,
                lag: Some(0),
            }),
            epoch: Mutex::new(7),
            log: Mutex::default(),
            forgotten: Mutex::new(0),
            commands: Mutex::default(),
            renewals: Mutex::default(),
            snapshot_delay: Mutex::default(),
            submitted: Mutex::default(),
        })
    }

    /// Applies the next revision `r` as the delta `delta@r`.
    pub fn apply(&self, command: Option<&str>) -> Delta {
        let prev = self.revision();
        let revision = prev + 1;
        let delta = Delta {
            epoch: self.epoch(),
            revision,
            prev,
            command: command.map(str::to_owned),
            text: format!("delta@{revision}").into(),
        };
        self.receive(delta.clone());
        delta
    }

    pub fn receive(&self, delta: Delta) {
        if let Some(id) = &delta.command {
            self.commands
                .lock()
                .unwrap()
                .insert(id.clone(), delta.revision);
        }
        self.log.lock().unwrap().push(delta);
    }

    /// Applies the submitted commands in order, each id once, as a state
    /// machine does when the entries carrying them are committed.
    pub fn commit(&self) {
        let submitted = std::mem::take(&mut *self.submitted.lock().unwrap());
        for (id, done) in submitted {
            let revision = match self.command_revision(&id) {
                Some(revision) => revision,
                None => self.apply(Some(&id)).revision,
            };
            let _ = done.send(Ack {
                command: id,
                revision,
            });
        }
    }

    pub fn forget_through(&self, revision: u64) {
        *self.forgotten.lock().unwrap() = revision;
    }

    /// Starts a new history, as a state that lost its storage does.
    pub fn reset(&self, epoch: u64) {
        *self.epoch.lock().unwrap() = epoch;
        self.log.lock().unwrap().clear();
        self.commands.lock().unwrap().clear();
        *self.forgotten.lock().unwrap() = 0;
    }
}

impl ReplicatedState for FakeState {
    fn replica(&self) -> ReplicaInfo {
        self.replica.lock().unwrap().clone()
    }

    fn epoch(&self) -> u64 {
        *self.epoch.lock().unwrap()
    }

    fn revision(&self) -> u64 {
        self.log.lock().unwrap().last().map_or(0, |d| d.revision)
    }

    fn snapshot(&self) -> BoxFuture<'_, Snapshot> {
        let (epoch, revision) = (self.epoch(), self.revision());
        let delay = *self.snapshot_delay.lock().unwrap();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Snapshot {
                epoch,
                revision,
                text: format!("snapshot@{epoch}:{revision}").into(),
            }
        })
    }

    fn deltas_since(&self, since: Position) -> BoxFuture<'_, Option<Vec<Delta>>> {
        let held = since.epoch == self.epoch() && since.revision >= *self.forgotten.lock().unwrap();
        let deltas: Vec<Delta> = self
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.revision > since.revision)
            .cloned()
            .collect();
        Box::pin(async move { held.then_some(deltas) })
    }

    fn command_revision(&self, id: &str) -> Option<u64> {
        self.commands.lock().unwrap().get(id).copied()
    }

    fn submit(&self, id: &str, _: Bytes) -> BoxFuture<'_, Result<Ack, NotApplied>> {
        let (done, ack) = oneshot::channel();
        self.submitted.lock().unwrap().push((id.to_owned(), done));
        Box::pin(async move { ack.await.map_err(|_| NotApplied("not committed".into())) })
    }

    fn renew_sessions(&self, live: &[Owner]) {
        self.renewals.lock().unwrap().push(live.to_vec());
    }
}

pub fn secret(name: &str, value: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("samplez-service");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, value).unwrap();
    path
}

/// `test` names the token files, so parallel tests do not share them.
pub fn config(mode: &str, test: &str) -> serde_json::Value {
    let mut config = json!({
        "auth": {
            "mode": mode,
            "proxy": { "groupRoles": { "ops": "operator", "watchers": "viewer" } },
        },
        "api": {
            "allowedHosts": ["samplez.example"],
            "allowedOrigins": ["https://samplez.example"],
        },
    });
    match mode {
        "token" => {
            config["auth"]["token"] =
                json!({ "file": secret(&format!("{test}-token"), &format!("{API_TOKEN}\n")) });
        }
        "proxy" => {
            config["auth"]["machineToken"] =
                json!({ "file": secret(&format!("{test}-machine"), MACHINE_TOKEN) });
            config["listeners"] = json!({ "machine": { "bind": "127.0.0.1", "port": 8082 } });
        }
        _ => {}
    }
    config
}

pub fn product() -> Router {
    Router::new().route(
        "/whoami",
        get(|Extension(id): Extension<Identity>| async move {
            format!("{:?} {:?}", id.role, id.user)
        })
        .post(|| async { "changed" }),
    )
}

pub fn service_from(config: serde_json::Value, ops: Ops, state: Arc<FakeState>) -> Service {
    let config: ServiceConfig = serde_json::from_value(config).unwrap();
    Service::new(config, ops, state)
        .unwrap()
        .api(product())
        .ws(|_, _| async {})
        .nmos(Router::new().route("/x-nmos/", get(|| async { "nmos" })))
}

pub fn service(mode: &str, test: &str) -> Service {
    service_from(
        config(mode, test),
        Ops::default(),
        FakeState::new("r1", "leader"),
    )
}

pub async fn send(router: &Router, req: request::Builder) -> (StatusCode, HeaderMap, String) {
    let res = router
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

pub async fn status(router: &Router, req: request::Builder) -> StatusCode {
    send(router, req).await.0
}

pub fn get_req(path: &str) -> request::Builder {
    Request::get(path).header(header::HOST, HOST)
}

/// A state-changing request with the browser-protection headers.
pub fn write_req(path: &str) -> request::Builder {
    Request::post(path)
        .header(header::HOST, HOST)
        .header("x-samplez-request", "1")
        .header(header::CONTENT_TYPE, "application/json")
}

/// Through `oneshot` there is no connection to upgrade, so a request that
/// passes auth ends in 426 from the upgrade extractor.
pub fn upgrade_req() -> request::Builder {
    get_req("/api/v1/ws")
        .header(header::CONNECTION, "upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
}

/// Replaces the `Host` header the request builders set.
pub fn host(mut req: request::Builder, host: &str) -> request::Builder {
    let headers = req.headers_mut().unwrap();
    headers.insert(header::HOST, host.parse().unwrap());
    req
}

pub fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}
