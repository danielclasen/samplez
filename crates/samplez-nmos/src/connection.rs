//! The IS-05 v1.2 Connection API for MXL senders and receivers per
//! BCP-007-03: single and bulk staging, immediate and scheduled
//! activation.
//!
//! `/active` is written when an activation fires; the product's
//! [`Connections::apply`] runs after that, and a failure turns the port's
//! `master_enable` false in `/active` with a new IS-04 version. A pending
//! scheduled activation locks `/staged` (423) until it fires or a PATCH
//! with `mode: null` cancels it; it lives in memory only.

use std::sync::Arc;
use std::time::Duration;

use axum::http::{Method, StatusCode};
use axum::response::Response;
use serde_json::{Map, Value, json};
use tokio::task::AbortHandle;
use uuid::Uuid;

use crate::http::{error, is_get, json as respond, not_allowed, not_found, ok};
use crate::model::{BIT_DEPTH, ChannelCount, SAMPLE_RATE};
use crate::product::{Active, Applied, FlowSeen, Port};
use crate::resources::CONNECTION_VERSION;
use crate::tai::Tai;
use crate::{Inner, State};

const DOMAIN: &str = "mxl_domain_id";
const FLOW: &str = "mxl_flow_id";
const IMMEDIATE: &str = "activate_immediate";
const ABSOLUTE: &str = "activate_scheduled_absolute";
const RELATIVE: &str = "activate_scheduled_relative";

/// A staged transport parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Param {
    Null,
    Auto,
    Id(Uuid),
}

impl Param {
    fn of(id: Option<Uuid>) -> Param {
        id.map_or(Param::Null, Param::Id)
    }

    fn to_json(self) -> Value {
        match self {
            Param::Null => Value::Null,
            Param::Auto => json!("auto"),
            Param::Id(u) => json!(u),
        }
    }

    /// `auto` is the port's own value: BCP-007-03 lets a port resolve it
    /// without a controller, and each port has exactly one domain and a
    /// sender exactly one flow.
    fn resolve(self, own: Option<Uuid>) -> Option<Uuid> {
        match self {
            Param::Null => None,
            Param::Auto => own,
            Param::Id(u) => Some(u),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Activation {
    mode: Option<&'static str>,
    requested: Option<String>,
    time: Option<Tai>,
}

impl Activation {
    fn to_json(&self) -> Value {
        json!({
            "mode": self.mode,
            "requested_time": self.requested,
            "activation_time": self.time.map(|t| t.to_string()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Staged {
    master_enable: bool,
    peer: Option<Uuid>,
    domain: Param,
    flow: Param,
    activation: Activation,
}

pub(crate) struct Pending {
    id: u64,
    at: Tai,
    task: AbortHandle,
}

/// A sender's or receiver's IS-04 version and IS-05 state.
pub(crate) struct PortState {
    pub version: Tai,
    pub staged: Staged,
    pub active: Active,
    active_activation: Activation,
    pending: Option<Pending>,
    /// Counts the writes of `/active`; an apply result is for the write
    /// it was started for only.
    generation: u64,
    /// Counts scheduled activations; a firing one checks it is still the
    /// pending one.
    schedules: u64,
    pub applied: Applied,
    pub failures: u64,
}

impl PortState {
    pub fn new(version: Tai, active: Active) -> PortState {
        PortState {
            version,
            staged: Staged {
                master_enable: active.master_enable,
                peer: active.peer,
                domain: Param::of(active.domain),
                flow: Param::of(active.flow),
                activation: Activation::default(),
            },
            active,
            active_activation: Activation::default(),
            pending: None,
            generation: 0,
            schedules: 0,
            applied: Applied::Initial,
            failures: 0,
        }
    }

    pub fn pending_at(&self) -> Option<Tai> {
        self.pending.as_ref().map(|p| p.at)
    }
}

impl Inner {
    fn port_ids(&self, kind: &str) -> Option<Vec<Uuid>> {
        match kind {
            "senders" => Some(self.ids.senders.iter().map(|s| s.sender).collect()),
            "receivers" => Some(self.ids.receivers.clone()),
            _ => None,
        }
    }

    fn find(&self, kind: &str, id: &str) -> Option<Port> {
        let ids = self.port_ids(kind)?;
        let i = ids.iter().position(|u| u.to_string() == id)?;
        Some(if kind == "senders" {
            Port::Sender(i)
        } else {
            Port::Receiver(i)
        })
    }

    /// The port's own domain and, for a sender, its one flow.
    fn own(&self, port: Port) -> (Uuid, Option<Uuid>) {
        match port {
            Port::Sender(i) => (
                self.model.senders[i].domain,
                Some(self.model.senders[i].flow),
            ),
            Port::Receiver(i) => (self.model.receivers[i].domain, None),
        }
    }
}

pub(crate) async fn handle(
    inner: &Arc<Inner>,
    method: &Method,
    path: &[&str],
    body: &[u8],
) -> Response {
    let get = is_get(method);
    let list = |items: &[&str]| ok(&json!(items));
    match path {
        [] if get => list(&[&format!("{CONNECTION_VERSION}/")]),
        [v] if *v == CONNECTION_VERSION && get => list(&["bulk/", "single/"]),
        [] => not_allowed("GET, HEAD"),
        [v, rest @ ..] if *v == CONNECTION_VERSION => match rest {
            [] => not_allowed("GET, HEAD"),
            ["single" | "bulk"] if get => list(&["senders/", "receivers/"]),
            ["single" | "bulk"] => not_allowed("GET, HEAD"),
            ["bulk", kind @ ("senders" | "receivers")] => {
                if method == Method::POST {
                    bulk(inner, kind, body).await
                } else {
                    not_allowed("POST")
                }
            }
            ["single", kind @ ("senders" | "receivers")] if get => {
                let ids = inner.port_ids(kind).unwrap_or_default();
                ok(&json!(
                    ids.iter().map(|u| format!("{u}/")).collect::<Vec<_>>()
                ))
            }
            ["single", "senders" | "receivers"] => not_allowed("GET, HEAD"),
            ["single", kind @ ("senders" | "receivers"), id, rest @ ..] => {
                match inner.find(kind, id) {
                    Some(port) => single(inner, port, method, rest, body).await,
                    None => not_found(),
                }
            }
            _ => not_found(),
        },
        _ => not_found(),
    }
}

async fn single(
    inner: &Arc<Inner>,
    port: Port,
    method: &Method,
    path: &[&str],
    body: &[u8],
) -> Response {
    let get = is_get(method);
    let sender = matches!(port, Port::Sender(_));
    match path {
        [] if get => ok(&if sender {
            json!([
                "constraints/",
                "staged/",
                "active/",
                "transportfile/",
                "transporttype/"
            ])
        } else {
            json!(["constraints/", "staged/", "active/", "transporttype/"])
        }),
        ["constraints"] if get => ok(&constraints(inner, port)),
        ["staged"] if get => ok(&staged_json(port, &inner.lock().port(port).staged)),
        ["staged"] if method == Method::PATCH => match serde_json::from_slice::<Value>(body) {
            Ok(v) => match patch(inner, port, v).await {
                Ok((status, v)) => respond(status, &v),
                Err((status, m)) => error(status, &m),
            },
            Err(e) => error(
                StatusCode::BAD_REQUEST,
                &format!("the body is no JSON: {e}"),
            ),
        },
        ["active"] if get => {
            let st = inner.lock();
            let p = st.port(port);
            ok(&active_json(port, &p.active, &p.active_activation))
        }
        ["transporttype"] if get => ok(&json!(crate::resources::MXL)),
        // An MXL sender has no transport file (BCP-007-03).
        ["transportfile"] if sender && get => {
            error(StatusCode::NOT_FOUND, "an MXL sender has no transport file")
        }
        [] | ["constraints" | "active" | "transporttype"] => not_allowed("GET, HEAD"),
        ["transportfile"] if sender => not_allowed("GET, HEAD"),
        ["staged"] => not_allowed("GET, HEAD, PATCH"),
        _ => not_found(),
    }
}

fn constraints(inner: &Inner, port: Port) -> Value {
    let (domain, flow) = inner.own(port);
    let flow = match flow {
        Some(f) => json!({ "enum": [f] }),
        None => json!({}),
    };
    json!([{ DOMAIN: { "enum": [domain] }, FLOW: flow }])
}

fn leg(domain: Value, flow: Value) -> Value {
    json!([{ DOMAIN: domain, FLOW: flow }])
}

fn peer_key(port: Port) -> &'static str {
    match port {
        Port::Sender(_) => "receiver_id",
        Port::Receiver(_) => "sender_id",
    }
}

fn with_transport_file(port: Port, mut v: Value) -> Value {
    if let Port::Receiver(_) = port {
        v["transport_file"] = json!({ "data": null, "type": null });
    }
    v
}

fn staged_json(port: Port, s: &Staged) -> Value {
    with_transport_file(
        port,
        json!({
            peer_key(port): s.peer,
            "master_enable": s.master_enable,
            "activation": s.activation.to_json(),
            "transport_params": leg(s.domain.to_json(), s.flow.to_json()),
        }),
    )
}

fn active_json(port: Port, a: &Active, activation: &Activation) -> Value {
    with_transport_file(
        port,
        json!({
            peer_key(port): a.peer,
            "master_enable": a.master_enable,
            "activation": activation.to_json(),
            "transport_params": leg(json!(a.domain), json!(a.flow)),
        }),
    )
}

type Patched = Result<(StatusCode, Value), (StatusCode, String)>;

fn bad<T>(m: impl Into<String>) -> Result<T, (StatusCode, String)> {
    Err((StatusCode::BAD_REQUEST, m.into()))
}

/// A lowercase UUID as the IS-04 and IS-05 schemas write it.
fn uuid_of(v: &Value, versions: &str) -> Option<Uuid> {
    let s = v.as_str()?;
    let u = Uuid::parse_str(s).ok()?;
    let ok = u.hyphenated().to_string() == s
        && versions.contains(s.as_bytes()[14] as char)
        && matches!(s.as_bytes()[19], b'8' | b'9' | b'a' | b'b');
    ok.then_some(u)
}

/// A staged MXL id: `null`, `auto` where `auto_ok`, or an MXL UUID
/// (BCP-007-03 mxl_uuid.json) within `allowed` when that is given.
fn param(
    name: &str,
    v: &Value,
    auto_ok: bool,
    allowed: Option<Uuid>,
) -> Result<Param, (StatusCode, String)> {
    match v {
        Value::Null => Ok(Param::Null),
        Value::String(s) if s == "auto" && auto_ok => Ok(Param::Auto),
        Value::String(s) if s == "auto" => bad(format!("{name} does not take auto")),
        _ => match uuid_of(v, "123456789abcdef") {
            Some(u) if allowed.is_none_or(|a| a == u) => Ok(Param::Id(u)),
            Some(u) => bad(format!("{name} {u} is not in the constraints")),
            None => bad(format!("{name} must be null, auto or an MXL UUID")),
        },
    }
}

/// A requested activation: its mode and requested time.
type Mode = (&'static str, Option<String>);

/// The parsed body of a PATCH to `/staged`.
struct Request {
    master_enable: Option<bool>,
    peer: Option<Option<Uuid>>,
    domain: Option<Param>,
    flow: Option<Param>,
    /// `Some(None)` cancels a pending activation.
    activation: Option<Option<Mode>>,
}

fn parse(inner: &Inner, port: Port, body: &Value) -> Result<Request, (StatusCode, String)> {
    let Some(body) = body.as_object() else {
        return bad("the body must be an object");
    };
    let (domain, flow) = inner.own(port);
    let receiver = matches!(port, Port::Receiver(_));
    let mut r = Request {
        master_enable: None,
        peer: None,
        domain: None,
        flow: None,
        activation: None,
    };
    for (k, v) in body {
        match k.as_str() {
            "master_enable" => {
                r.master_enable = Some(v.as_bool().ok_or((
                    StatusCode::BAD_REQUEST,
                    "master_enable must be a boolean".into(),
                ))?)
            }
            k if k == peer_key(port) => {
                r.peer = Some(match v {
                    Value::Null => None,
                    _ => Some(uuid_of(v, "12345").ok_or((
                        StatusCode::BAD_REQUEST,
                        format!("{k} must be null or a UUID"),
                    ))?),
                })
            }
            "transport_params" => {
                let Some([leg]) = v.as_array().map(Vec::as_slice) else {
                    return bad("transport_params must hold exactly one leg");
                };
                let Some(leg) = leg.as_object() else {
                    return bad("a transport_params leg must be an object");
                };
                for (name, v) in leg {
                    match name.as_str() {
                        DOMAIN => r.domain = Some(param(DOMAIN, v, true, Some(domain))?),
                        FLOW => r.flow = Some(param(FLOW, v, !receiver, flow)?),
                        _ => return bad(format!("{name} is no transport parameter of this port")),
                    }
                }
            }
            "transport_file" if receiver => {
                let ok = v.as_object().is_some_and(|f| {
                    f.iter()
                        .all(|(k, v)| matches!(k.as_str(), "data" | "type") && v.is_null())
                });
                if !ok {
                    return bad("an MXL receiver takes no transport file");
                }
            }
            "activation" => r.activation = Some(activation(v)?),
            _ => return bad(format!("{k} is not a staged parameter")),
        }
    }
    Ok(r)
}

fn activation(v: &Value) -> Result<Option<Mode>, (StatusCode, String)> {
    let Some(a) = v.as_object() else {
        return bad("activation must be an object");
    };
    if let Some(k) = a
        .keys()
        .find(|k| !matches!(k.as_str(), "mode" | "requested_time"))
    {
        return bad(format!("{k} is not an activation parameter"));
    }
    let requested = match a.get("requested_time") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.parse::<Tai>().is_ok() => Some(s.clone()),
        Some(_) => return bad("requested_time must be null or <seconds>:<nanoseconds>"),
    };
    let mode = match a.get("mode") {
        None => return bad("activation needs a mode"),
        Some(Value::Null) => return Ok(None),
        Some(Value::String(m)) => match m.as_str() {
            IMMEDIATE => IMMEDIATE,
            ABSOLUTE => ABSOLUTE,
            RELATIVE => RELATIVE,
            _ => return bad(format!("{m} is no activation mode")),
        },
        Some(_) => return bad("mode must be a string or null"),
    };
    if mode != IMMEDIATE && requested.is_none() {
        return bad("a scheduled activation needs a requested_time");
    }
    Ok(Some((mode, requested)))
}

/// Checks a flow the receiver would read against its caps. An absent flow
/// passes: the receiver reads silence until it appears.
async fn check_flow(inner: &Inner, port: Port, flow: Uuid) -> Result<(), String> {
    let Port::Receiver(i) = port else {
        return Ok(());
    };
    match inner.conns.resolve(port, flow).await.map_err(|r| r.0)? {
        FlowSeen::Absent => Ok(()),
        FlowSeen::Present {
            channels,
            rate,
            float32,
            ..
        } => {
            let caps = inner.model.receivers[i].channels;
            let fits = match caps {
                ChannelCount::Exactly(n) => channels == n,
                ChannelCount::AtLeast(n) => (n..=crate::MAX_CHANNELS).contains(&channels),
            };
            if rate != SAMPLE_RATE || !float32 {
                Err(format!(
                    "flow {flow} is not {BIT_DEPTH}-bit float at {SAMPLE_RATE} Hz"
                ))
            } else if !fits {
                Err(format!(
                    "flow {flow} has {channels} channels, the receiver takes {caps:?}"
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// What activating `staged` makes active, every `auto` resolved, or why it
/// cannot be applied.
async fn target(inner: &Inner, port: Port, staged: &Staged) -> Result<Active, String> {
    let (domain, flow) = inner.own(port);
    let active = Active {
        master_enable: staged.master_enable,
        peer: staged.peer,
        domain: staged.domain.resolve(Some(domain)),
        flow: staged.flow.resolve(flow),
    };
    if active.master_enable {
        let (Some(_), Some(f)) = (active.domain, active.flow) else {
            return Err("master_enable needs an mxl_domain_id and an mxl_flow_id".into());
        };
        check_flow(inner, port, f).await?;
    }
    Ok(active)
}

pub(crate) async fn patch(inner: &Arc<Inner>, port: Port, body: Value) -> Patched {
    let req = parse(inner, port, &body)?;
    // One PATCH at a time, so the checks below see the state they change.
    let _patching = inner.patching.lock().await;
    let mut staged = {
        let st = inner.lock();
        let p = st.port(port);
        if p.pending.is_some() && req.activation != Some(None) {
            return Err((
                StatusCode::LOCKED,
                "a scheduled activation is pending".into(),
            ));
        }
        p.staged.clone()
    };
    if let Some(m) = req.master_enable {
        staged.master_enable = m;
    }
    if let Some(p) = req.peer {
        staged.peer = p;
    }
    if let Some(d) = req.domain {
        staged.domain = d;
    }
    if let Some(f) = req.flow {
        staged.flow = f;
        if let Param::Id(id) = f {
            check_flow(inner, port, id).await.or_else(bad)?;
        }
    }
    let now = inner.clock.now();
    match req.activation {
        None => {
            inner.lock().port_mut(port).staged = staged.clone();
            Ok((StatusCode::OK, staged_json(port, &staged)))
        }
        Some(None) => {
            staged.activation = Activation::default();
            let mut st = inner.lock();
            let p = st.port_mut(port);
            if let Some(pending) = p.pending.take() {
                pending.task.abort();
            }
            p.staged = staged.clone();
            Ok((StatusCode::OK, staged_json(port, &staged)))
        }
        Some(Some((IMMEDIATE, _))) => {
            let active = target(inner, port, &staged).await.or_else(bad)?;
            let activation = Activation {
                mode: Some(IMMEDIATE),
                requested: None,
                time: Some(now),
            };
            let response = Staged {
                activation: activation.clone(),
                ..staged.clone()
            };
            let generation = {
                let mut st = inner.lock();
                st.port_mut(port).staged = staged;
                write_active(&mut st, port, active.clone(), activation, now)
            };
            spawn_apply(inner, port, active, None, generation);
            Ok((StatusCode::OK, staged_json(port, &response)))
        }
        Some(Some((mode, requested))) => {
            target(inner, port, &staged).await.or_else(bad)?;
            let Some(requested) = requested else {
                return bad("a scheduled activation needs a requested_time");
            };
            let t: Tai = requested
                .parse()
                .map_err(|_| (StatusCode::BAD_REQUEST, "requested_time".to_string()))?;
            let at = if mode == RELATIVE {
                now.after(Duration::from_nanos(t.as_nanos() as u64))
            } else {
                t
            };
            staged.activation = Activation {
                mode: Some(mode),
                requested: Some(requested),
                time: Some(at),
            };
            let mut st = inner.lock();
            let p = st.port_mut(port);
            p.schedules += 1;
            let id = p.schedules;
            let task = tokio::spawn(scheduled(inner.clone(), port, id, at)).abort_handle();
            p.pending = Some(Pending { id, at, task });
            p.staged = staged.clone();
            Ok((StatusCode::ACCEPTED, staged_json(port, &staged)))
        }
    }
}

/// Writes `/active`, bumps the port's IS-04 version and returns the
/// generation an apply result has to match.
fn write_active(
    st: &mut State,
    port: Port,
    active: Active,
    activation: Activation,
    now: Tai,
) -> u64 {
    let version = st.bump(now);
    let p = st.port_mut(port);
    p.active = active;
    p.active_activation = activation;
    p.version = version;
    p.generation += 1;
    p.applied = Applied::Applying;
    p.generation
}

fn spawn_apply(inner: &Arc<Inner>, port: Port, active: Active, at: Option<Tai>, generation: u64) {
    let inner = inner.clone();
    tokio::spawn(async move {
        let result = inner.conns.apply(port, active, at).await;
        settle(&inner, port, generation, result.map_err(|f| f.0));
    });
}

/// Records an apply result; a failure turns `master_enable` false in
/// `/active` unless a later activation replaced what failed.
fn settle(inner: &Inner, port: Port, generation: u64, result: Result<(), String>) {
    let now = inner.clock.now();
    let mut st = inner.lock();
    if st.port(port).generation != generation {
        return;
    }
    match result {
        Ok(()) => st.port_mut(port).applied = Applied::Applied,
        Err(reason) => {
            tracing::warn!(port = ?port, %reason, "nmos: the activation was not applied; master_enable is false");
            let version = st.bump(now);
            let p = st.port_mut(port);
            p.active.master_enable = false;
            p.version = version;
            p.applied = Applied::Failed(reason);
            p.failures += 1;
        }
    }
}

/// Runs a scheduled activation: `apply` [`Connections::lead`] before `at`,
/// `/active` at `at`.
async fn scheduled(inner: Arc<Inner>, port: Port, id: u64, at: Tai) {
    let lead = inner.conns.lead();
    sleep_until(&inner, at, lead).await;
    let staged = inner.lock().port(port).staged.clone();
    let target = target(&inner, port, &staged).await;
    let apply = target.clone().ok().map(|active| {
        let inner = inner.clone();
        tokio::spawn(async move { inner.conns.apply(port, active, Some(at)).await })
    });
    sleep_until(&inner, at, Duration::ZERO).await;
    let generation = {
        let mut st = inner.lock();
        if st.port(port).pending.as_ref().is_none_or(|p| p.id != id) {
            return;
        }
        let p = st.port_mut(port);
        p.pending = None;
        let activation = Activation {
            mode: p.staged.activation.mode,
            requested: p.staged.activation.requested.take(),
            time: Some(at),
        };
        p.staged.activation = Activation::default();
        // A target that no longer checks out is written as it would be and
        // then failed below, so `/active` shows what was asked for.
        let active = target.clone().unwrap_or_else(|_| Active {
            master_enable: staged.master_enable,
            peer: staged.peer,
            domain: staged.domain.resolve(Some(inner.own(port).0)),
            flow: staged.flow.resolve(inner.own(port).1),
        });
        write_active(&mut st, port, active, activation, at)
    };
    let result = match (target, apply) {
        (Err(reason), _) => Err(reason),
        (Ok(_), Some(task)) => match task.await {
            Ok(r) => r.map_err(|f| f.0),
            Err(e) => Err(format!("apply did not complete: {e}")),
        },
        (Ok(_), None) => Ok(()),
    };
    settle(&inner, port, generation, result);
}

/// Sleeps until `lead` before `at` on the node's TAI clock.
async fn sleep_until(inner: &Inner, at: Tai, lead: Duration) {
    let wait = at.since(inner.clock.now()).saturating_sub(lead);
    if !wait.is_zero() {
        tokio::time::sleep(wait).await;
    }
}

async fn bulk(inner: &Arc<Inner>, kind: &str, body: &[u8]) -> Response {
    let items = match serde_json::from_slice::<Value>(body) {
        Ok(Value::Array(items)) => items,
        _ => return error(StatusCode::BAD_REQUEST, "the body must be an array"),
    };
    let well_formed = |i: &Value| {
        i.as_object().is_some_and(|o| {
            o.len() == 2
                && uuid_of(&o["id"], "12345").is_some()
                && o.get("params").is_some_and(Value::is_object)
        })
    };
    if !items.iter().all(well_formed) {
        return error(
            StatusCode::BAD_REQUEST,
            "each item needs exactly an id and params",
        );
    }
    let mut out = Vec::new();
    for item in items {
        let id = item["id"].as_str().unwrap_or_default().to_owned();
        let mut r = Map::new();
        r.insert("id".into(), json!(id));
        let result = match inner.find(kind, &id) {
            Some(port) => patch(inner, port, item["params"].clone()).await,
            None => Err((StatusCode::NOT_FOUND, "no such port".into())),
        };
        match result {
            Ok((status, _)) => {
                r.insert("code".into(), json!(status.as_u16()));
            }
            Err((status, m)) => {
                r.insert("code".into(), json!(status.as_u16()));
                r.insert("error".into(), json!(m));
                r.insert("debug".into(), Value::Null);
            }
        }
        out.push(Value::Object(r));
    }
    ok(&Value::Array(out))
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
