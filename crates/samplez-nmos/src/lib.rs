//! NMOS for samplez products: the IS-04 v1.3 Node API and the IS-05 v1.2
//! Connection API for MXL senders and receivers per AMWA BCP-007-03, with
//! BCP-004-01 receiver caps and BCP-002-01/02 tags.
//!
//! The crate knows NMOS ids, MXL ids and TAI time; it links no libmxl and
//! runs no real-time code. A product describes its writers and readers in
//! a [`NodeModel`] and serves [`Nmos::router`] on its NMOS listener.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State as Extract;
use axum::http::{Method, Uri};
use axum::response::Response;
use axum::routing::any;
use serde_json::json;
use uuid::Uuid;

mod connection;
mod http;
mod ids;
mod model;
mod node_api;
mod product;
mod resources;
mod tai;

pub use model::{
    Asset, BIT_DEPTH, ChannelCount, ChannelDesc, MAX_CHANNELS, MEDIA_TYPE, NmosConfig, NodeModel,
    ReceiverDesc, SAMPLE_RATE, SenderDesc,
};
pub use product::{
    Active, Applied, BoxFuture, Connections, Fail, FlowSeen, NmosStatus, NoStore, Port, PortKey,
    PortStatus, Reject, Store,
};
pub use tai::{SystemTai, Tai, TaiClock};
pub use uuid;

use connection::PortState;
use ids::Ids;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The model or the configuration cannot be registered as given.
    Model(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Model(m) => write!(f, "nmos: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// A product's NMOS node. Cheap to clone; every clone is the same node.
#[derive(Clone)]
pub struct Nmos {
    inner: Arc<Inner>,
}

pub(crate) struct Inner {
    cfg: NmosConfig,
    model: NodeModel,
    ids: Ids,
    conns: Arc<dyn Connections>,
    clock: Arc<dyn TaiClock>,
    /// The receivers' caps never change, so their version is the start.
    caps_version: Tai,
    state: Mutex<State>,
    /// Held across a PATCH to `/staged`, checks included.
    patching: tokio::sync::Mutex<()>,
}

pub(crate) struct State {
    /// The last version handed out; versions increase across all resources.
    last_version: Tai,
    node_version: Tai,
    device_version: Tai,
    senders: Vec<SenderState>,
    receivers: Vec<PortState>,
}

pub(crate) struct SenderState {
    source_version: Tai,
    flow_version: Tai,
    port: PortState,
}

impl State {
    /// A version later than every version handed out before, and not
    /// before `now`.
    fn bump(&mut self, now: Tai) -> Tai {
        let v = now.max(self.last_version.next());
        self.last_version = v;
        v
    }

    fn port(&self, port: Port) -> &PortState {
        match port {
            Port::Sender(i) => &self.senders[i].port,
            Port::Receiver(i) => &self.receivers[i],
        }
    }

    fn port_mut(&mut self, port: Port) -> &mut PortState {
        match port {
            Port::Sender(i) => &mut self.senders[i].port,
            Port::Receiver(i) => &mut self.receivers[i],
        }
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn key(&self, port: Port) -> PortKey {
        match port {
            Port::Sender(i) => PortKey::Sender(self.model.senders[i].key.clone()),
            Port::Receiver(i) => PortKey::Receiver(self.model.receivers[i].key.clone()),
        }
    }

    fn ports(&self) -> impl Iterator<Item = Port> + use<> {
        let senders = (0..self.model.senders.len()).map(Port::Sender);
        senders.chain((0..self.model.receivers.len()).map(Port::Receiver))
    }
}

impl Nmos {
    /// Checks the model, derives the ids from `cfg.seed` and sets every
    /// port's `/staged` and `/active`: from `store` where it holds the
    /// port, else senders enabled on their flow and receivers enabled on
    /// their flow when they have one.
    pub fn new(
        model: NodeModel,
        cfg: NmosConfig,
        conns: Arc<dyn Connections>,
        store: &dyn Store,
        clock: impl TaiClock,
    ) -> Result<Nmos, Error> {
        check(&model, &cfg)?;
        if let Some(w) = clock.utc_offset().and_then(tai::offset_warning) {
            tracing::warn!("{w}");
        }
        let ids = Ids::derive(
            &cfg.seed,
            model.senders.iter().map(|s| s.key.as_str()),
            model.receivers.iter().map(|r| r.key.as_str()),
        );
        let stored = store.load();
        let now = clock.now();
        let mut st = State {
            last_version: Tai::default(),
            node_version: Tai::default(),
            device_version: Tai::default(),
            senders: Vec::new(),
            receivers: Vec::new(),
        };
        st.node_version = st.bump(now);
        st.device_version = st.bump(now);
        let caps_version = st.bump(now);
        for s in &model.senders {
            let initial = Active {
                master_enable: true,
                peer: None,
                domain: Some(s.domain),
                flow: Some(s.flow),
            };
            let active = stored
                .get(&PortKey::Sender(s.key.clone()))
                .cloned()
                .unwrap_or(initial);
            let state = SenderState {
                source_version: st.bump(now),
                flow_version: st.bump(now),
                port: PortState::new(st.bump(now), active),
            };
            st.senders.push(state);
        }
        for r in &model.receivers {
            let initial = Active {
                master_enable: r.flow.is_some(),
                peer: None,
                domain: Some(r.domain),
                flow: r.flow,
            };
            let active = stored
                .get(&PortKey::Receiver(r.key.clone()))
                .cloned()
                .unwrap_or(initial);
            let version = st.bump(now);
            st.receivers.push(PortState::new(version, active));
        }
        Ok(Nmos {
            inner: Arc::new(Inner {
                cfg,
                model,
                ids,
                conns,
                clock: Arc::new(clock),
                caps_version,
                state: Mutex::new(st),
                patching: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// `/x-nmos/node/v1.3/` and `/x-nmos/connection/v1.2/`, for the
    /// product's NMOS listener. Every path answers with and without a
    /// trailing slash, every response carries CORS headers and every
    /// `OPTIONS` is a pre-flight. Activations spawn tasks on the runtime
    /// that serves it.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/", any(handle))
            .route("/x-nmos", any(handle))
            .route("/x-nmos/", any(handle))
            .route("/x-nmos/{*rest}", any(handle))
            .with_state(self.inner.clone())
    }

    pub fn node_id(&self) -> Uuid {
        self.inner.ids.node
    }

    /// A change of a port's connection the product made itself, such as a
    /// switch to another leg: `/active` shows it from now on with a new
    /// IS-04 version. `/staged` stays as it was.
    pub fn external_change(&self, port: Port, active: Active) {
        let now = self.inner.clock.now();
        let mut st = self.inner.lock();
        let version = st.bump(now);
        let p = st.port_mut(port);
        p.active = active;
        p.version = version;
    }

    pub fn status(&self) -> NmosStatus {
        let st = self.inner.lock();
        NmosStatus {
            ports: self
                .inner
                .ports()
                .map(|port| {
                    let p = st.port(port);
                    PortStatus {
                        port,
                        key: self.inner.key(port),
                        active: p.active.clone(),
                        pending: p.pending_at(),
                        applied: p.applied.clone(),
                        failures: p.failures,
                    }
                })
                .collect(),
        }
    }
}

async fn handle(
    Extract(inner): Extract<Arc<Inner>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    if method == Method::OPTIONS {
        return http::preflight();
    }
    let path = http::segments(uri.path());
    match path.as_slice() {
        [] | ["x-nmos"] if !http::is_get(&method) => http::not_allowed("GET, HEAD"),
        [] => http::ok(&json!(["x-nmos/"])),
        ["x-nmos"] => http::ok(&json!(["node/", "connection/"])),
        ["x-nmos", "node", rest @ ..] => node_api::handle(&inner, &method, rest),
        ["x-nmos", "connection", rest @ ..] => {
            connection::handle(&inner, &method, rest, &body).await
        }
        _ => http::not_found(),
    }
}

fn check(model: &NodeModel, cfg: &NmosConfig) -> Result<(), Error> {
    let fail = |m: String| Err(Error::Model(m));
    if cfg.advertise_host.is_empty() || cfg.advertise_port == 0 {
        return fail("the advertised host and port must be set".into());
    }
    if cfg.seed.is_empty() {
        return fail("the seed must be set".into());
    }
    let a = &cfg.asset;
    if [&a.manufacturer, &a.product, &a.instance_id]
        .iter()
        .any(|v| v.is_empty())
        || a.functions.is_empty()
        || a.functions.iter().any(|f| f.is_empty())
    {
        return fail("every asset tag needs a value and the device a function".into());
    }
    // Every sender has the role `Audio out` and every receiver `Audio in`
    // in its group, and BCP-002-01 allows one resource per role in a
    // group; the colon separates the group from the role.
    let group_ok = |g: &str| !g.is_empty() && !g.contains(':');
    let mut keys = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for s in &model.senders {
        if !keys.insert(s.key.as_str()) {
            return fail(format!("sender key {:?} is not unique", s.key));
        }
        if !group_ok(&s.group) || !groups.insert(s.group.as_str()) {
            return fail(format!(
                "sender {:?}: group {:?} is empty, holds a colon or has another sender",
                s.key, s.group
            ));
        }
        if !ids::is_mxl_uuid(s.domain) || !ids::is_mxl_uuid(s.flow) {
            return fail(format!(
                "sender {:?}: domain {} or flow {} is no MXL UUID",
                s.key, s.domain, s.flow
            ));
        }
        if s.channels.is_empty() {
            return fail(format!("sender {:?} has no channels", s.key));
        }
        for c in &s.channels {
            if let Some(sym) = &c.symbol
                && !symbol_valid(sym)
            {
                return fail(format!("sender {:?}: {sym:?} is no channel symbol", s.key));
            }
        }
    }
    keys.clear();
    groups.clear();
    for r in &model.receivers {
        if !keys.insert(r.key.as_str()) {
            return fail(format!("receiver key {:?} is not unique", r.key));
        }
        if !group_ok(&r.group) || !groups.insert(r.group.as_str()) {
            return fail(format!(
                "receiver {:?}: group {:?} is empty, holds a colon or has another receiver",
                r.key, r.group
            ));
        }
        if !ids::is_mxl_uuid(r.domain) || r.flow.is_some_and(|f| !ids::is_mxl_uuid(f)) {
            return fail(format!(
                "receiver {:?}: domain {} or flow {:?} is no MXL UUID",
                r.key, r.domain, r.flow
            ));
        }
        let (ChannelCount::Exactly(n) | ChannelCount::AtLeast(n)) = r.channels;
        if n == 0 || n > MAX_CHANNELS {
            return fail(format!("receiver {:?}: {n} channels", r.key));
        }
    }
    Ok(())
}

/// A VSF TR-03 Appendix A symbol, as IS-04's `source_audio` admits it.
fn symbol_valid(s: &str) -> bool {
    const NAMED: [&str; 22] = [
        "L", "R", "C", "LFE", "Ls", "Rs", "Lss", "Rss", "Lrs", "Rrs", "Lc", "Rc", "Cs", "HI",
        "VIN", "M1", "M2", "Lt", "Rt", "Lst", "Rst", "S",
    ];
    let numbered = |prefix: &str, digits: usize, lo: u32, hi: u32| {
        s.strip_prefix(prefix).is_some_and(|n| {
            n.len() == digits
                && n.bytes().all(|b| b.is_ascii_digit())
                && n.parse::<u32>().is_ok_and(|v| (lo..=hi).contains(&v))
        })
    };
    NAMED.contains(&s) || numbered("NSC", 3, 0, 128) || numbered("U", 2, 1, 64)
}

#[cfg(test)]
mod tests;
