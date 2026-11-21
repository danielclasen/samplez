//! NMOS for samplez products: the IS-04 v1.3 Node API and registration and
//! the IS-05 v1.2 Connection API for MXL senders and receivers per AMWA
//! BCP-007-03, with BCP-004-01 receiver caps and BCP-002-01/02 tags.
//!
//! The crate knows NMOS ids, MXL ids and TAI time; it links no libmxl and
//! runs no real-time code. A product describes its writers and readers in
//! a [`NodeModel`] and serves [`Nmos::router`] on its NMOS listener.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State as Extract;
use axum::http::{HeaderMap, Method, Uri, header};
use axum::response::Response;
use axum::routing::any;
use serde_json::json;
use uuid::Uuid;

mod connection;
mod discovery;
mod http;
mod ids;
mod model;
mod node_api;
mod product;
mod registration;
mod resources;
mod tai;

pub use connection::RESOLVE_TIMEOUT;
pub use model::{
    Asset, BIT_DEPTH, ChannelCount, ChannelDesc, MAX_CHANNELS, MEDIA_TYPE, NmosConfig, NodeModel,
    ReceiverDesc, SAMPLE_RATE, SenderDesc,
};
pub use product::DEFAULT_APPLY_TIMEOUT;
pub use product::{
    Activation, ActivationMode, Active, Applied, BoxFuture, Connections, Fail, FlowSeen,
    NmosStatus, NoStore, Port, PortKey, PortStatus, Reject, Replicated, Stamped, Store,
};
pub use tai::{SystemTai, Tai, TaiClock};
pub use uuid;

use connection::PortState;
use ids::Ids;

/// Bucket bounds of `samplez_nmos_activation_late_seconds`.
const LATE_BUCKETS: [f64; 8] = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 1.0];

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
    /// Versions come only from the product, [`Nmos::with_versions`].
    shared: bool,
    /// The receivers' caps never change, so their version is the start;
    /// with shared versions each receiver's own.
    caps_version: Tai,
    state: Mutex<State>,
    /// One per port, senders first; held across a PATCH to the port's
    /// `/staged`, checks included.
    patching: Vec<tokio::sync::Mutex<()>>,
    /// Per port as [`Inner::index`] orders them, once registered.
    late: std::sync::OnceLock<Vec<samplez_service::Histogram>>,
    /// Signalled on every new resource version, for the registration.
    changed: Arc<tokio::sync::Notify>,
    /// The Registration API the node is registered with.
    registry: Mutex<Option<String>>,
}

pub(crate) struct State {
    /// The last version handed out; versions increase across all resources.
    last_version: Tai,
    node_version: Tai,
    device_version: Tai,
    senders: Vec<SenderState>,
    receivers: Vec<PortState>,
    changed: Arc<tokio::sync::Notify>,
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
        self.changed.notify_one();
        v
    }

    /// With shared versions, after a port's version moved: a sender's
    /// Source and Flow take the sender's version, the Node and the Device
    /// the latest of all, and a receiver's caps its receiver's
    /// ([`resources`]). This is deliberate: their content never changes,
    /// but their version has to be the same on every instance and must not
    /// go back across a takeover or a restore, and the crate keeps no
    /// replicated state of its own that could hold one. A version derived
    /// from the ports' replicated versions is both; the cost is a re-post
    /// of the Node, the Device and a sender's Source and Flow with each
    /// change of a port.
    fn derive(&mut self) {
        let mut latest = Tai::default();
        for s in &mut self.senders {
            s.source_version = s.port.version;
            s.flow_version = s.port.version;
            latest = latest.max(s.port.version);
        }
        for r in &self.receivers {
            latest = latest.max(r.version);
        }
        self.node_version = latest;
        self.device_version = latest;
        self.changed.notify_one();
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
    /// Gives every resource a new version, later than every one handed out
    /// and not before the clock: a registration continued from another
    /// process holds that process's versions, which a registry takes a
    /// change over only when newer. Nothing with shared versions: the
    /// predecessor registered the same ones.
    pub(crate) fn restamp(&self) {
        if self.shared {
            return;
        }
        let now = self.clock.now();
        let mut st = self.lock();
        st.node_version = st.bump(now);
        st.device_version = st.bump(now);
        for i in 0..st.senders.len() {
            st.senders[i].source_version = st.bump(now);
            st.senders[i].flow_version = st.bump(now);
            st.senders[i].port.version = st.bump(now);
        }
        for i in 0..st.receivers.len() {
            st.receivers[i].version = st.bump(now);
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn key(&self, port: Port) -> PortKey {
        match port {
            Port::Sender(i) => PortKey::Sender(self.model.senders[i].key.clone()),
            Port::Receiver(i) => PortKey::Receiver(self.model.receivers[i].key.clone()),
        }
    }

    fn patching(&self, port: Port) -> &tokio::sync::Mutex<()> {
        &self.patching[self.index(port)]
    }

    /// The port's place among all ports, senders first.
    fn index(&self, port: Port) -> usize {
        match port {
            Port::Sender(i) => i,
            Port::Receiver(i) => self.model.senders.len() + i,
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
        Nmos::build(model, cfg, conns, store, clock, None)
    }

    /// [`Nmos::new`] for a product whose instances serve one Node, such as
    /// a replica set: every version comes from the product, so instances
    /// fed the same changes serve the same versions. A port's version is
    /// the TAI time of the change that last changed it, from `versions`
    /// (`0:0` for a port it lacks), and moves only with
    /// [`Nmos::replicated_change`] and [`Nmos::set_versions`]. A sender's
    /// Source and Flow carry the sender's version, a receiver's caps the
    /// receiver's, and the Node and the Device the latest of all.
    ///
    /// An activation the node serves writes `/active` and leaves the
    /// IS-04 resource and its version as they are: in its
    /// [`Connections::apply`] the product takes the change from
    /// [`Nmos::activation`] and replicates it, and every instance, the
    /// serving one included, passes it to [`Nmos::replicated_change`], so
    /// `/active` and the resources are the replicated ones everywhere. The
    /// apply of a scheduled activation runs [`Connections::lead`] before
    /// its time, so instances can show its `activation_time` before it
    /// comes. A scheduled activation that no longer checks out at its time
    /// goes to the product with `master_enable` false. A failed apply
    /// shows the replicated connection in `/active` again. `/staged` and
    /// pending scheduled activations are each instance's own, so a
    /// controller stages and activates a port on one instance. A product
    /// that keeps the replicated activations in its [`Store`]
    /// ([`Store::activations`]) starts an instance on its peers'
    /// `/active`. A continued registration ([`NmosConfig::continue_held`])
    /// keeps the versions as they are. Every instance advertises the same
    /// host and port, as the Node carries them.
    pub fn with_versions(
        model: NodeModel,
        cfg: NmosConfig,
        conns: Arc<dyn Connections>,
        store: &dyn Store,
        clock: impl TaiClock,
        versions: &BTreeMap<PortKey, Tai>,
    ) -> Result<Nmos, Error> {
        Nmos::build(model, cfg, conns, store, clock, Some(versions))
    }

    fn build(
        model: NodeModel,
        cfg: NmosConfig,
        conns: Arc<dyn Connections>,
        store: &dyn Store,
        clock: impl TaiClock,
        versions: Option<&BTreeMap<PortKey, Tai>>,
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
        let stored_activations = store.activations();
        // The activation stored with a connection the port starts from.
        let activation = |key: &PortKey, from_store: bool| {
            let a = stored_activations.get(key).filter(|_| from_store);
            a.map(connection::Activation::of).unwrap_or_default()
        };
        let now = clock.now();
        let changed = Arc::new(tokio::sync::Notify::new());
        let mut st = State {
            last_version: Tai::default(),
            node_version: Tai::default(),
            device_version: Tai::default(),
            senders: Vec::new(),
            receivers: Vec::new(),
            changed: changed.clone(),
        };
        let mut caps_version = Tai::default();
        if versions.is_none() {
            st.node_version = st.bump(now);
            st.device_version = st.bump(now);
            caps_version = st.bump(now);
        }
        for s in &model.senders {
            let initial = Active {
                master_enable: true,
                peer: None,
                domain: Some(s.domain),
                flow: Some(s.flow),
            };
            let key = PortKey::Sender(s.key.clone());
            let stored = from_store(&stored, &key, s.domain, Some(s.flow));
            let activation = activation(&key, stored.is_some());
            let active = stored.unwrap_or(initial);
            let state = match versions {
                Some(v) => SenderState {
                    source_version: Tai::default(),
                    flow_version: Tai::default(),
                    port: PortState::new(
                        v.get(&key).copied().unwrap_or_default(),
                        active,
                        activation,
                    ),
                },
                None => SenderState {
                    source_version: st.bump(now),
                    flow_version: st.bump(now),
                    port: PortState::new(st.bump(now), active, activation),
                },
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
            let key = PortKey::Receiver(r.key.clone());
            let stored = from_store(&stored, &key, r.domain, None);
            let activation = activation(&key, stored.is_some());
            let active = stored.unwrap_or(initial);
            let version = match versions {
                Some(v) => v.get(&key).copied().unwrap_or_default(),
                None => st.bump(now),
            };
            st.receivers
                .push(PortState::new(version, active, activation));
        }
        if versions.is_some() {
            st.derive();
        }
        let ports = model.senders.len() + model.receivers.len();
        Ok(Nmos {
            inner: Arc::new(Inner {
                cfg,
                model,
                ids,
                conns,
                clock: Arc::new(clock),
                shared: versions.is_some(),
                caps_version,
                state: Mutex::new(st),
                patching: (0..ports).map(|_| tokio::sync::Mutex::new(())).collect(),
                late: std::sync::OnceLock::new(),
                changed,
                registry: Mutex::new(None),
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

    /// Finds a registry and keeps the node registered with it: posts the
    /// resources, heartbeats every [`NmosConfig::heartbeat`], posts each
    /// resource whose version changed, and fails over to another registry
    /// when one stops answering. Runs until dropped; it never waits on the
    /// product. No registry found means no registration: the node does
    /// not fall back to peer-to-peer advertisement.
    pub fn registration(&self) -> impl Future<Output = ()> + Send + 'static {
        registration::run(self.inner.clone())
    }

    /// Deletes the Node from its registry, for a graceful shutdown after
    /// [`Nmos::registration`] is dropped.
    pub async fn unregister(&self) {
        registration::unregister(&self.inner).await
    }

    pub fn node_id(&self) -> Uuid {
        self.inner.ids.node
    }

    /// A change of a port's connection the product made itself, such as a
    /// switch to another leg: `/active` shows it from now on with a new
    /// IS-04 version, `/staged` stays as it was, and the activations still
    /// queued for the port are dropped (a scheduled one still pending
    /// stays). Returns the change's generation: an apply already running
    /// carries a lower one, and the product ignores it when it resolves
    /// after the change; its result no longer changes `/active`.
    ///
    /// Panics on a node built with [`Nmos::with_versions`], whose changes
    /// carry their version: [`Nmos::replicated_change`].
    pub fn external_change(&self, port: Port, active: Active) -> u64 {
        assert!(
            !self.inner.shared,
            "nmos: a node with shared versions takes changes with replicated_change"
        );
        let now = self.inner.clock.now();
        let mut st = self.inner.lock();
        let version = st.bump(now);
        let p = st.port_mut(port);
        p.published = active.clone();
        p.active = active;
        p.version = version;
        p.applied = Applied::Applied;
        p.changed_by_product()
    }

    /// A change of a port in the product's replicated state, on a node
    /// built with [`Nmos::with_versions`]; `version` is the change's TAI
    /// time there. The port's version becomes `version`, or the next after
    /// its own where that is not earlier, so it increases also for two
    /// changes at one time; the IS-04 resource shows `change.active` from
    /// now on. Returns the version set, which the product keeps, and the
    /// generation.
    ///
    /// `/active` shows the change, in the order the product replicates
    /// changes, with `change.activation` or, when it carries none, the
    /// activation last replicated; an activation served here and not
    /// replicated yet shows once it is. `served` is the generation of the
    /// [`Connections::apply`] call on this node the change replicates, if
    /// any: the activations queued behind it still reach the product.
    /// Otherwise the change is one as [`Nmos::external_change`] makes it,
    /// from another instance or by the product itself.
    ///
    /// Panics on a node built with [`Nmos::new`].
    pub fn replicated_change(
        &self,
        port: Port,
        change: &Replicated,
        version: Tai,
        served: Option<u64>,
    ) -> Stamped {
        assert!(
            self.inner.shared,
            "nmos: replicated_change needs a node built with with_versions"
        );
        let mut st = self.inner.lock();
        let p = st.port_mut(port);
        p.version = version.max(p.version.next());
        p.published = change.active.clone();
        if let Some(a) = &change.activation {
            p.published_activation = connection::Activation::of(a);
        }
        p.show_published();
        let generation = match served {
            Some(_) => p.generation(),
            None => {
                p.applied = Applied::Applied;
                p.changed_by_product()
            }
        };
        let version = p.version;
        st.derive();
        Stamped {
            version,
            generation,
        }
    }

    /// The change the [`Connections::apply`] call of `generation` makes,
    /// the connection and the activation `/active` shows for it, while
    /// that call is with the product; for the product to replicate.
    pub fn activation(&self, port: Port, generation: u64) -> Option<Replicated> {
        self.inner.lock().port(port).in_flight(generation)
    }

    /// Moves each port's version up to the one `versions` holds for it,
    /// from the product's replicated state, as at a leader takeover; a
    /// version is never moved back, and a port it lacks keeps its own.
    /// Panics on a node built with [`Nmos::new`].
    pub fn set_versions(&self, versions: &BTreeMap<PortKey, Tai>) {
        assert!(
            self.inner.shared,
            "nmos: set_versions needs a node built with with_versions"
        );
        let mut st = self.inner.lock();
        for port in self.inner.ports() {
            if let Some(v) = versions.get(&self.inner.key(port)) {
                let p = st.port_mut(port);
                p.version = p.version.max(*v);
            }
        }
        st.derive();
    }

    /// Registers `samplez_nmos_activation_late_seconds{port}`, how long
    /// after its activation time each scheduled activation was written to
    /// `/active`, in `metrics`. Once; a second call does nothing.
    pub fn register_metrics(&self, metrics: &samplez_service::Metrics) {
        let _ = self.inner.late.set(
            self.inner
                .ports()
                .map(|port| {
                    let label = match self.inner.key(port) {
                        PortKey::Sender(k) => format!("sender/{k}"),
                        PortKey::Receiver(k) => format!("receiver/{k}"),
                    };
                    metrics.histogram(
                        "samplez_nmos_activation_late_seconds",
                        "How long after its activation time a scheduled IS-05 activation was written to /active.",
                        &[("port", &label)],
                        &LATE_BUCKETS,
                    )
                })
                .collect(),
        );
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
                        late: p.late,
                    }
                })
                .collect(),
            registry: self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

async fn handle(
    Extract(inner): Extract<Arc<Inner>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method == Method::OPTIONS {
        return http::preflight(headers.get(header::ACCESS_CONTROL_REQUEST_HEADERS));
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

/// The stored `/active` of a port when it fits the model: the port's own
/// domain, a sender's own flow or a receiver's MXL flow, and both where it
/// is enabled. Anything else is left behind with a warning.
fn from_store(
    stored: &BTreeMap<PortKey, Active>,
    key: &PortKey,
    domain: Uuid,
    own_flow: Option<Uuid>,
) -> Option<Active> {
    let a = stored.get(key)?;
    let fits = a.domain.is_none_or(|d| d == domain)
        && a.flow.is_none_or(|f| match own_flow {
            Some(own) => f == own,
            None => ids::is_mxl_uuid(f),
        })
        && (!a.master_enable || (a.domain.is_some() && a.flow.is_some()));
    if !fits {
        tracing::warn!(?key, stored = ?a, "nmos: the stored connection does not fit the model; the port starts from the model");
    }
    fits.then(|| a.clone())
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
    // The registration client speaks plain HTTP only, as discovery uses
    // only registries advertising api_proto=http.
    if let Some(r) = cfg.registries.iter().find(|r| !r.starts_with("http://")) {
        return fail(format!(
            "registry {r:?}: only http:// is supported, the registration client speaks plain HTTP only"
        ));
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
