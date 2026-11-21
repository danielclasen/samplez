//! The IS-04 v1.3 registration client: registers the Node and its
//! resources parents first, heartbeats, re-posts what changed, and handles
//! the registry's answers as IS-04 "Behaviour - Registration" lays out.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, sleep_until};
use uuid::Uuid;

use crate::discovery::{Discovery, Registry};
use crate::{Inner, Tai, resources};

/// While no registry is found, discovery runs again after this long.
const RE_BROWSE: Duration = Duration::from_secs(5);
/// The backoff while every registry fails. Its cap bounds how long a
/// registry that comes back waits for the node: AMWA nmos-testing gives a
/// node 30 s to register once it enables its mock registry.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// How a request ended.
enum Answer {
    /// The status, the `Location` header, and the `error` of a 400's
    /// IS-04 error body.
    Status(StatusCode, Option<String>, Option<String>),
    /// No answer: refused, unreachable or timed out.
    None(String),
}

/// Why the node stops using a registry.
enum Leave {
    /// 5xx, no answer or a timeout: try another registry.
    Failover,
}

struct Registrar {
    http: Client,
    node: Uuid,
}

impl Registrar {
    async fn send(&self, req: reqwest::RequestBuilder) -> Answer {
        match req.header(ACCEPT, "application/json").send().await {
            Ok(r) => {
                let location = r
                    .headers()
                    .get(LOCATION)
                    .and_then(|l| l.to_str().ok())
                    .map(str::to_owned);
                let status = r.status();
                let error = if status == StatusCode::BAD_REQUEST {
                    let body = r.text().await.ok();
                    let body = body.and_then(|b| serde_json::from_str::<Value>(&b).ok());
                    body.and_then(|b| b["error"].as_str().map(str::to_owned))
                } else {
                    None
                };
                Answer::Status(status, location, error)
            }
            Err(e) => Answer::None(e.to_string()),
        }
    }

    async fn post(&self, base: &str, kind: &str, data: &Value) -> Answer {
        let body = json!({ "type": kind, "data": data }).to_string();
        let req = self
            .http
            .post(format!("{base}/resource"))
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        self.send(req).await
    }

    async fn delete(&self, url: &str) -> Answer {
        self.send(self.http.delete(url)).await
    }

    /// An empty body with `Content-Length: 0` and no `Content-Type`.
    async fn heartbeat(&self, base: &str) -> Answer {
        // An empty body alone sends no Content-Length; IS-04 recommends
        // it, and nmos-testing warns without it.
        let req = self
            .http
            .post(format!("{base}/health/nodes/{}", self.node))
            .header(CONTENT_LENGTH, "0");
        self.send(req).await
    }
}

/// The URL a 409's `Location` names, against the registry's origin.
fn absolute(base: &str, location: &str) -> String {
    if location.starts_with("http://") || location.starts_with("https://") {
        return location.to_owned();
    }
    let origin_end = base.find("://").map_or(0, |i| i + 3);
    let origin = match base[origin_end..].find('/') {
        Some(i) => &base[..origin_end + i],
        None => base,
    };
    format!("{origin}{location}")
}

pub(crate) async fn run(inner: Arc<Inner>) {
    let http = match Client::builder().timeout(inner.cfg.heartbeat).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("nmos: no HTTP client, the node does not register: {e}");
            return;
        }
    };
    let client = Registrar {
        http,
        node: inner.ids.node,
    };
    let mut discovery = Discovery::new(
        inner.cfg.registries.clone(),
        inner.cfg.dns_sd_domain.clone(),
    );
    // Per registry the node completed joining, the version of each
    // resource it took. Any other registry found holding the node is taken
    // to share the store of the one the node came from, as a registry
    // cluster does.
    let mut records: BTreeMap<String, BTreeMap<Uuid, String>> = BTreeMap::new();
    let mut last: Option<String> = None;
    let mut registered_once = false;
    let mut backoff = BACKOFF_MIN;
    // The registry the node just failed over from, tried last in the next
    // round.
    let mut failed: Option<String> = None;
    loop {
        let mut found = discovery.find().await;
        if let Some(f) = failed.take() {
            let (rest, last): (Vec<_>, Vec<_>) = found.into_iter().partition(|r| r.base != f);
            found = rest.into_iter().chain(last).collect();
        }
        if found.is_empty() {
            tracing::debug!("nmos: no registry found");
            sleep(RE_BROWSE).await;
            continue;
        }
        let mut served = false;
        for registry in &found {
            // A node that was registered asks a new registry whether it
            // still holds it before registering again, and so does one
            // that continues a held registration from its first.
            let joined = if registered_once || inner.cfg.continue_held {
                match client.heartbeat(&registry.base).await {
                    Answer::Status(StatusCode::OK, ..) => Ok(true),
                    Answer::Status(StatusCode::NOT_FOUND, ..) => Ok(false),
                    Answer::Status(StatusCode::CONFLICT, loc, _) => {
                        unregister_old(&client, &registry.base, loc).await;
                        Ok(false)
                    }
                    a => Err(a),
                }
            } else {
                Ok(false)
            };
            let still_held = match joined {
                Ok(h) => h,
                Err(a) => {
                    log_failover(registry, &a);
                    continue;
                }
            };
            let mut joining = match records.remove(&registry.base) {
                Some(own) => own,
                None => last
                    .as_ref()
                    .and_then(|l| records.get(l))
                    .cloned()
                    .unwrap_or_default(),
            };
            let posted = &mut joining;
            // A registry that still holds the node gets a heartbeat only,
            // as IS-04 asks, then every resource whose version it has not
            // taken: the change whose POST failed the last registry, any
            // made since, and on a registry the node left earlier, any made
            // while it was away.
            // A registration continued from another process carries its
            // versions: every resource gets a newer one first.
            let continued = still_held && !registered_once;
            if continued {
                inner.restamp();
            }
            let joined = if still_held {
                post_changed(&inner, &client, &registry.base, posted, false, continued).await
            } else {
                register_all(&inner, &client, &registry.base, posted).await
            };
            if joined.is_err() {
                continue;
            }
            let posted = records.entry(registry.base.clone()).or_insert(joining);
            registered_once = true;
            served = true;
            backoff = BACKOFF_MIN;
            *inner.registry.lock().unwrap_or_else(|e| e.into_inner()) = Some(registry.base.clone());
            tracing::info!(registry = %registry.base, "nmos: registered");
            last = Some(registry.base.clone());
            let Err(Leave::Failover) = maintain(&inner, &client, &registry.base, posted).await;
            *inner.registry.lock().unwrap_or_else(|e| e.into_inner()) = None;
            tracing::warn!(registry = %registry.base, "nmos: the registry failed; trying another");
            failed = Some(registry.base.clone());
            break;
        }
        if !served {
            tracing::warn!("nmos: no registry took the node; retrying in {backoff:?}");
            sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }
}

fn log_failover(registry: &Registry, a: &Answer) {
    match a {
        Answer::Status(s, ..) => {
            tracing::warn!(registry = %registry.base, status = %s, "nmos: registry unusable")
        }
        Answer::None(e) => {
            tracing::warn!(registry = %registry.base, error = %e, "nmos: registry unreachable")
        }
    }
}

/// Deletes the Node where a 409 says it is registered at another API
/// version, as IS-04 requires before registering at this one.
async fn unregister_old(client: &Registrar, base: &str, location: Option<String>) {
    match location {
        Some(l) => {
            let url = absolute(base, &l);
            let url = match url.find("/health/nodes/") {
                Some(i) => format!("{}/resource/nodes/{}", &url[..i], client.node),
                None => url,
            };
            client.delete(&url).await;
        }
        None => tracing::warn!("nmos: a 409 without a Location; registering anyway"),
    }
}

/// Every resource, parents first, with its type.
fn documents(inner: &Inner) -> Vec<(&'static str, Value)> {
    let st = inner.lock();
    let mut docs = vec![
        ("node", resources::node(inner, &st)),
        ("device", resources::device(inner, &st)),
    ];
    for (kind, all) in [
        ("source", resources::sources(inner, &st)),
        ("flow", resources::flows(inner, &st)),
        ("sender", resources::senders(inner, &st)),
        ("receiver", resources::receivers(inner, &st)),
    ] {
        docs.extend(all.into_iter().map(|d| (kind, d)));
    }
    docs
}

fn id_version(doc: &Value) -> (Uuid, String) {
    let id = doc["id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_default();
    (id, doc["version"].as_str().unwrap_or_default().to_owned())
}

/// Posts every resource whose version the registry has not seen, parents
/// first. A 200 on a first Node registration means the registry still held
/// an earlier registration, and a 400 that it holds one at a later version,
/// as of a replica set rebuilt under the same Node id: the node deletes it
/// and registers anew.
async fn register_all(
    inner: &Inner,
    client: &Registrar,
    base: &str,
    posted: &mut BTreeMap<Uuid, String>,
) -> Result<(), Leave> {
    posted.clear();
    post_changed(inner, client, base, posted, true, false).await
}

async fn post_changed(
    inner: &Inner,
    client: &Registrar,
    base: &str,
    posted: &mut BTreeMap<Uuid, String>,
    mut fresh: bool,
    mut continued: bool,
) -> Result<(), Leave> {
    let failover = |a: &Answer| {
        log_failover(
            &Registry {
                base: base.into(),
                pri: 0,
            },
            a,
        );
        Err(Leave::Failover)
    };
    'pass: loop {
        for (kind, doc) in documents(inner) {
            let (id, version) = id_version(&doc);
            if posted.get(&id) == Some(&version) {
                continue;
            }
            let mut answer = client.post(base, kind, &doc).await;
            let held = match &answer {
                Answer::Status(StatusCode::OK, ..) => true,
                Answer::Status(StatusCode::BAD_REQUEST, _, error) => {
                    held_later(error.as_deref(), &version)
                }
                _ => false,
            };
            if fresh && kind == "node" && held {
                client.delete(&format!("{base}/resource/nodes/{id}")).await;
                answer = client.post(base, kind, &doc).await;
            }
            // In a continued registration, a resource refused, as one
            // whose new version is still not newer than the held one after
            // a clock skew between hosts: the Node is deleted and every
            // resource registered anew, so none of the predecessor's stays.
            if continued && matches!(answer, Answer::Status(StatusCode::BAD_REQUEST, ..)) {
                tracing::warn!(%kind, %id, "nmos: the registry refused a continued resource; registering anew");
                let node = format!("{base}/resource/nodes/{}", client.node);
                match client.delete(&node).await {
                    Answer::Status(s, ..) if s.is_success() || s == StatusCode::NOT_FOUND => {}
                    a => return failover(&a),
                }
                posted.clear();
                (fresh, continued) = (true, false);
                continue 'pass;
            }
            if let Answer::Status(StatusCode::CONFLICT, loc, _) = answer {
                unregister_old(client, base, loc).await;
                answer = client.post(base, kind, &doc).await;
            }
            match answer {
                Answer::Status(s, ..) if s.is_success() => {
                    posted.insert(id, version);
                }
                Answer::Status(s, ..) if s.is_client_error() => {
                    // IS-04: a 400 is not retried as it was sent.
                    tracing::warn!(%kind, %id, status = %s, "nmos: the registry refused a resource");
                    posted.insert(id, version);
                }
                a => return failover(&a),
            }
        }
        return Ok(());
    }
}

/// Whether a 400 to a resource POST at `posted` says the registry holds
/// the resource at a later version. IS-04 gives no structured reason, so
/// the `error` is read: one naming a TAI time later than `posted`, as
/// nmos-cpp's "request for registration with version ... conflicts with
/// the existing registration with version ..." does, or naming a version
/// without a time. No `error` at all counts too: a repost is the one thing
/// that can cure such a refusal.
fn held_later(error: Option<&str>, posted: &str) -> bool {
    let Some(error) = error else {
        return true;
    };
    let Ok(posted) = posted.parse::<Tai>() else {
        return false;
    };
    let times: Vec<Tai> = error
        .split(|c: char| !(c.is_ascii_digit() || c == ':'))
        .filter_map(|w| w.trim_matches(':').parse().ok())
        .collect();
    if times.is_empty() {
        return error.to_ascii_lowercase().contains("version");
    }
    times.iter().any(|t| *t > posted)
}

/// Heartbeats every interval and posts changed resources, until the
/// registry fails.
async fn maintain(
    inner: &Inner,
    client: &Registrar,
    base: &str,
    posted: &mut BTreeMap<Uuid, String>,
) -> Result<Infallible, Leave> {
    let interval = inner.cfg.heartbeat;
    let mut next = Instant::now() + interval;
    loop {
        tokio::select! {
            () = sleep_until(next) => {}
            () = inner.changed.notified() => {
                post_changed(inner, client, base, posted, false, false).await?;
                continue;
            }
        }
        next += interval;
        if next < Instant::now() {
            next = Instant::now() + interval;
        }
        match client.heartbeat(base).await {
            Answer::Status(StatusCode::OK, ..) => {}
            Answer::Status(StatusCode::NOT_FOUND, ..) => {
                tracing::info!("nmos: the registry lost the node; registering again");
                register_all(inner, client, base, posted).await?;
            }
            Answer::Status(StatusCode::CONFLICT, loc, _) => {
                unregister_old(client, base, loc).await;
                register_all(inner, client, base, posted).await?;
            }
            Answer::Status(s, ..) if s.is_client_error() => {
                tracing::warn!(status = %s, "nmos: the registry refused a heartbeat");
            }
            a => {
                log_failover(
                    &Registry {
                        base: base.into(),
                        pri: 0,
                    },
                    &a,
                );
                return Err(Leave::Failover);
            }
        }
    }
}

/// Deletes the Node from the registry it is registered with, if any.
pub(crate) async fn unregister(inner: &Inner) {
    let base = inner
        .registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    let Some(base) = base else {
        return;
    };
    let Ok(http) = Client::builder().timeout(inner.cfg.heartbeat).build() else {
        return;
    };
    let client = Registrar {
        http,
        node: inner.ids.node,
    };
    client
        .delete(&format!("{base}/resource/nodes/{}", inner.ids.node))
        .await;
}

#[cfg(test)]
mod tests {
    use super::{absolute, held_later};

    #[test]
    fn a_400_counts_as_a_later_held_version_only_where_it_says_so() {
        let cpp = "Bad Request; request for registration with version 10:0 \
                   conflicts with the existing registration with version 1000:0";
        assert!(held_later(Some(cpp), "10:0"));
        assert!(!held_later(Some(cpp), "1000:0"));
        assert!(held_later(Some("the version is not newer"), "10:0"));
        assert!(!held_later(
            Some("Bad Request; schema validation failed"),
            "10:0"
        ));
        assert!(held_later(None, "10:0"));
    }

    #[test]
    fn a_location_resolves_against_the_registry_origin() {
        let base = "http://192.0.2.1:8010/x-nmos/registration/v1.3";
        assert_eq!(
            absolute(base, "/x-nmos/registration/v1.2/resource/nodes/x"),
            "http://192.0.2.1:8010/x-nmos/registration/v1.2/resource/nodes/x"
        );
        assert_eq!(absolute(base, "http://192.0.2.9/y"), "http://192.0.2.9/y");
    }
}
