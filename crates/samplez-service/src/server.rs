//! The listeners and the routers on them.

use std::future::{Future, IntoFuture};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Router, middleware};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::auth::{self, Auth, Identity, Listener, Role, SessionEnd};
use crate::config::{AuthMode, ServiceConfig};
use crate::oidc;
use crate::ops::{self, Ops};
use crate::replica::{Owner, ReplicatedState};
use crate::tls::{ListenerTls, TlsListener, TlsStatus};
use crate::ws::Sessions;
use crate::{Error, config};

/// How often [`Bound::serve`] passes the live sessions to
/// [`ReplicatedState::renew_sessions`].
pub const SESSION_RENEW_PERIOD: Duration = Duration::from_secs(1);
/// The shortest client session id accepted in `?session=`: 16 bytes in
/// base64url without padding.
pub const MIN_SESSION_ID: usize = 22;
/// The longest client session id accepted in `?session=`.
pub const MAX_SESSION_ID: usize = 128;

/// How often [`Bound::serve`] re-reads the token files and the TLS
/// certificate, key and client CA files.
pub const SECRET_RELOAD_PERIOD: Duration = Duration::from_secs(10);

type WsHandler =
    Arc<dyn Fn(WebSocket, Connect) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What the WebSocket handler learns about a client at the upgrade.
#[derive(Debug, Clone)]
pub struct Connect {
    pub identity: Identity,
    /// The principal is the proxy's user or `anonymous`; the session is
    /// `?session=`, chosen by the client and not authenticated, or the
    /// connection id when absent (see [`Owner::session`]).
    pub owner: Owner,
    /// Where [`crate::ws::serve_client`] keeps the connection while it
    /// answers pings.
    pub sessions: Arc<Sessions>,
    /// When the credentials of the upgrade end; [`crate::ws::serve_client`]
    /// closes the connection then.
    pub ends: SessionEnd,
}

/// Returned by [`Connect::require_operator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forbidden;

impl Connect {
    /// Call before applying a command received over the WebSocket: the
    /// role check of the auth middleware covers HTTP requests only.
    pub fn require_operator(&self) -> Result<(), Forbidden> {
        match self.identity.role {
            Role::Operator => Ok(()),
            Role::Viewer => Err(Forbidden),
        }
    }
}

#[derive(Deserialize)]
struct WsQuery {
    session: Option<String>,
}

/// `?session=`: [`MIN_SESSION_ID`] to [`MAX_SESSION_ID`] characters of the
/// base64url alphabet (RFC 4648 section 5), without padding.
fn session_id_valid(s: &str) -> bool {
    (MIN_SESSION_ID..=MAX_SESSION_ID).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The service's own state for a product's status report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    pub secrets: Vec<SecretStatus>,
    /// One per listener that runs TLS.
    pub tls: Vec<TlsStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretStatus {
    /// `token`, `machineToken`, `oidcClientSecret` or `oidcSessionKey`.
    pub name: &'static str,
    /// Seconds since the Unix epoch of the last successful read.
    pub loaded_at: u64,
}

/// Reads the current [`ServiceStatus`]; cloned into the product's routes.
#[derive(Clone)]
pub struct StatusSource(Arc<Auth>, Arc<[ListenerTls]>);

impl StatusSource {
    pub fn get(&self) -> ServiceStatus {
        let mut status = ServiceStatus::default();
        self.0.status(&mut status);
        status.tls = self.1.iter().map(ListenerTls::status).collect();
        status
    }
}

pub struct Service {
    config: ServiceConfig,
    auth: Arc<Auth>,
    tls: Arc<[ListenerTls]>,
    ops: Ops,
    state: Arc<dyn ReplicatedState>,
    api: Router,
    ui: Router,
    ws: Option<WsHandler>,
    nmos: Router,
    sessions: Arc<Sessions>,
    refusal: Option<auth::Refusal>,
}

/// The routers of each listener, for serving or for driving in tests.
pub struct Routers {
    pub api: Router,
    pub machine: Option<Router>,
    pub ops: Router,
    pub nmos: Router,
}

impl Service {
    /// Checks the auth and TLS settings against the listeners, reads the
    /// token and TLS files, generates the self-signed certificates and
    /// draws the random part of the connection ids ([`Sessions::new`]).
    pub fn new(
        config: ServiceConfig,
        ops: Ops,
        state: Arc<dyn ReplicatedState>,
    ) -> Result<Service, Error> {
        let listeners = &config.listeners;
        let mut tls = Vec::new();
        for (name, listener) in [
            ("api", Some(&listeners.api)),
            ("machine", listeners.machine.as_ref()),
            ("ops", Some(&listeners.ops)),
            ("nmos", Some(&listeners.nmos)),
        ] {
            if let Some(l) = listener
                && let Some(t) = ListenerTls::new(name, &l.tls, ops.metrics())?
            {
                tls.push(t);
            }
        }
        Ok(Service {
            auth: Arc::new(Auth::new(&config, ops.metrics())?),
            tls: tls.into(),
            sessions: Arc::new(Sessions::new(&state.replica().id)?),
            config,
            ops,
            state,
            api: Router::new(),
            ui: Router::new(),
            ws: None,
            nmos: Router::new(),
            refusal: None,
        })
    }

    /// Routes served under `/api/v1` on the API listener and, in `proxy`
    /// mode, the machine listener. The service adds `/session` on the API
    /// listener, `/login` and `/logout` in `token` and `oidc` mode, and
    /// `/login/callback` in `oidc` mode.
    pub fn api(mut self, router: Router) -> Service {
        self.api = router;
        self
    }

    /// Renders the response to a request under `/api/v1` that the auth
    /// and browser checks refuse, from its status, 401 or 403, and a short
    /// reason in English, so a product answers it in the body its API
    /// defines. A 401 gets `WWW-Authenticate: Bearer` on top. Without it,
    /// the status alone.
    pub fn refusal<F>(mut self, render: F) -> Service
    where
        F: Fn(StatusCode, &'static str) -> Response + Send + Sync + 'static,
    {
        self.refusal = Some(Arc::new(render));
        self
    }

    /// Served at `/` on the API listener with the `Host` check only, no
    /// authentication: the UI's static files and login page. Paths under
    /// `/api/v1` never reach it.
    pub fn ui(mut self, router: Router) -> Service {
        self.ui = router;
        self
    }

    /// The source of [`ServiceStatus`] for the product's routes.
    pub fn status(&self) -> StatusSource {
        StatusSource(self.auth.clone(), self.tls.clone())
    }

    /// Serves WebSocket upgrades at `/api/v1/ws` once auth and the `Origin`
    /// check pass. The client's position is not part of the upgrade: the
    /// product's protocol carries it ([`crate::ws::Reply::CatchUp`]).
    ///
    /// `?session=` is the client's session id, kept across its
    /// reconnects: at least 16 bytes from a cryptographic random source,
    /// drawn once per client instance and sent in base64url without
    /// padding ([`MIN_SESSION_ID`] to [`MAX_SESSION_ID`] characters of
    /// `A-Z a-z 0-9 - _`). Other values get 400. The service checks the
    /// encoding and length only, not the randomness. It is taken as the
    /// client sends it and identifies nobody: a client can send another
    /// client's session id. Without it the session is the connection id,
    /// new for every connection. The service logs no request URI, and the
    /// `Debug` output of [`Connect`] shows the session's [`Owner::handle`]
    /// in place of the session.
    pub fn ws<F, Fut>(mut self, handler: F) -> Service
    where
        F: Fn(WebSocket, Connect) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.ws = Some(Arc::new(move |socket, connect| {
            Box::pin(handler(socket, connect))
        }));
        self
    }

    /// The NMOS listener's router, served as is: no auth, no `Host` check,
    /// `Authorization` is not read. IS-05 on it is unauthenticated, so the
    /// bind address and a NetworkPolicy are what protect it. Controllers
    /// address the node by its advertised href under any name, which is why
    /// `Host` is not checked.
    pub fn nmos(mut self, router: Router) -> Service {
        self.nmos = router;
        self
    }

    /// Re-reads the token files and the TLS certificate, key and client CA
    /// files now, each on its own. Sessions issued under a replaced API
    /// token end; a replaced certificate or client CA applies from the next
    /// handshake on.
    /// Returns the first error.
    pub fn reload_secrets(&self) -> Result<(), Error> {
        let mut first = self.auth.reload();
        for tls in self.tls.iter() {
            if let Err(e) = tls.reload() {
                first = first.and(Err(e));
            }
        }
        first
    }

    pub fn routers(&self) -> Routers {
        let api = self.authenticated(Listener::Api);
        let machine = self
            .config
            .listeners
            .machine
            .as_ref()
            .map(|_| self.authenticated(Listener::Machine));
        Routers {
            api,
            machine,
            ops: self.ops.router(self.state.clone()),
            nmos: self.nmos.clone(),
        }
    }

    fn authenticated(&self, listener: Listener) -> Router {
        let mut inner = self.api.clone();
        if let Some(handler) = &self.ws {
            let handler = handler.clone();
            let sessions = self.sessions.clone();
            inner = inner.route(
                "/ws",
                get(
                    move |upgrade: WebSocketUpgrade,
                          Extension(identity): Extension<Identity>,
                          ends: Option<Extension<SessionEnd>>,
                          Query(query): Query<WsQuery>| {
                        let handler = handler.clone();
                        let sessions = sessions.clone();
                        async move {
                            let connection = sessions.connection_id();
                            let session = match query.session {
                                Some(s) if !session_id_valid(&s) => {
                                    return StatusCode::BAD_REQUEST.into_response();
                                }
                                Some(s) => s,
                                None => connection.clone(),
                            };
                            let owner = Owner {
                                principal: identity
                                    .user
                                    .clone()
                                    .unwrap_or_else(|| "anonymous".into()),
                                session,
                                role: identity.role,
                                connection,
                            };
                            let connect = Connect {
                                identity,
                                owner,
                                sessions,
                                ends: ends.map(|Extension(e)| e).unwrap_or_default(),
                            };
                            upgrade.on_upgrade(move |socket| handler(socket, connect))
                        }
                    },
                ),
            );
        }
        if listener == Listener::Api {
            let auth = || self.auth.clone();
            inner = match self.auth.mode() {
                AuthMode::Token => inner.route("/login", post(auth::login).with_state(auth())),
                AuthMode::Oidc => inner
                    .route("/login", get(oidc::login).with_state(auth()))
                    .route("/login/callback", get(oidc::callback).with_state(auth())),
                AuthMode::None | AuthMode::Proxy => inner,
            };
            if matches!(self.auth.mode(), AuthMode::Token | AuthMode::Oidc) {
                inner = inner.route("/logout", post(auth::logout).with_state(auth()));
            }
            inner = inner.route("/session", get(auth::session).with_state(auth()));
        }
        let api = Router::new()
            .nest(
                "/api/v1",
                inner.fallback(|| async { StatusCode::NOT_FOUND }),
            )
            .layer(middleware::from_fn_with_state(
                (self.auth.clone(), listener, self.refusal.clone()),
                auth::middleware,
            ));
        match listener {
            Listener::Api => api.merge(
                self.ui
                    .clone()
                    .layer(middleware::from_fn_with_state(self.auth.clone(), host_only)),
            ),
            Listener::Machine => api,
        }
    }

    /// Binds every listener and starts the TLS handshakes on those that
    /// run TLS.
    pub async fn bind(self) -> io::Result<Bound> {
        let listeners = &self.config.listeners;
        let bind = async |name: &str, l: &config::Listener| -> io::Result<Socket> {
            let tcp = TcpListener::bind(SocketAddr::new(l.bind, l.port)).await?;
            match self.tls.iter().find(|t| t.listener() == name) {
                Some(tls) => Ok(Socket::Tls(tls.bind(tcp)?)),
                None => Ok(Socket::Plain(tcp)),
            }
        };
        let api = bind("api", &listeners.api).await?;
        let machine = match &listeners.machine {
            Some(l) => Some(bind("machine", l).await?),
            None => None,
        };
        // The ops listener is registered with a runtime of its own when it
        // serves ([`Bound::serve`]): bound here without one.
        let ops = {
            let std = std::net::TcpListener::bind(SocketAddr::new(
                listeners.ops.bind,
                listeners.ops.port,
            ))?;
            std.set_nonblocking(true)?;
            OpsSocket {
                std,
                tls: self.tls.iter().position(|t| t.listener() == "ops"),
            }
        };
        let nmos = bind("nmos", &listeners.nmos).await?;
        Ok(Bound {
            routers: self.routers(),
            api,
            machine,
            ops,
            nmos,
            service: self,
        })
    }
}

enum Socket {
    Plain(TcpListener),
    Tls(TlsListener),
}

impl Socket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Socket::Plain(l) => l.local_addr(),
            Socket::Tls(l) => axum::serve::Listener::local_addr(l),
        }
    }

    fn serve(self, router: Router) -> BoxFuture<'static, io::Result<()>> {
        match self {
            Socket::Plain(l) => Box::pin(axum::serve(l, router).into_future()),
            Socket::Tls(l) => Box::pin(axum::serve(l, router).into_future()),
        }
    }
}

/// The ops listener before it serves: a bound socket on no runtime, and
/// the TLS of the service's list it runs, if any.
struct OpsSocket {
    std: std::net::TcpListener,
    tls: Option<usize>,
}

pub struct Bound {
    service: Service,
    routers: Routers,
    api: Socket,
    machine: Option<Socket>,
    ops: OpsSocket,
    nmos: Socket,
}

impl Bound {
    pub fn api_addr(&self) -> io::Result<SocketAddr> {
        self.api.local_addr()
    }

    pub fn machine_addr(&self) -> Option<io::Result<SocketAddr>> {
        self.machine.as_ref().map(|l| l.local_addr())
    }

    pub fn ops_addr(&self) -> io::Result<SocketAddr> {
        self.ops.std.local_addr()
    }

    pub fn nmos_addr(&self) -> io::Result<SocketAddr> {
        self.nmos.local_addr()
    }

    /// Serves every listener, watches the heartbeat and reloads the token
    /// and TLS files until a listener fails. A failed reload keeps the
    /// previous token or certificate. `/readyz` reports the API as serving while this runs.
    ///
    /// The ops listener and the heartbeat sampling run on a thread of their
    /// own with a runtime of its own, the listener's socket and TLS
    /// handshakes registered there: whatever blocks the calling runtime's
    /// workers, such as a product's replica and engine futures, cannot hold
    /// back `/healthz`. The thread is stopped and joined when this returns
    /// or is dropped.
    pub async fn serve(self) -> io::Result<()> {
        let Bound {
            service,
            routers,
            api,
            machine,
            ops,
            nmos,
        } = self;
        let machine = async move {
            match (machine, routers.machine) {
                (Some(l), Some(r)) => l.serve(r).await,
                _ => std::future::pending().await,
            }
        };
        service.ops.set_api_serving(true);
        let _serving = Serving(service.ops.clone());
        let (done, ops_done) = tokio::sync::oneshot::channel::<io::Result<()>>();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (tls, router, sampled) = (service.tls.clone(), routers.ops, service.ops.clone());
        let thread = std::thread::Builder::new()
            .name("samplez-ops".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let served = runtime.and_then(|runtime| {
                    runtime.block_on(async move {
                        let tcp = TcpListener::from_std(ops.std)?;
                        let socket = match ops.tls {
                            Some(i) => Socket::Tls(tls[i].bind(tcp)?),
                            None => Socket::Plain(tcp),
                        };
                        let sampling = async {
                            let mut seen = 0;
                            let mut ticks = tokio::time::interval(ops::SAMPLE_PERIOD);
                            loop {
                                ticks.tick().await;
                                sampled.sample_heartbeat(&mut seen);
                            }
                        };
                        tokio::select! {
                            r = socket.serve(router) => r,
                            () = sampling => unreachable!("the sampling never returns"),
                            _ = stopped => Ok(()),
                        }
                    })
                });
                let _ = done.send(served);
            })?;
        let _ops = OpsThread {
            stop: Some(stop),
            thread: Some(thread),
        };
        let servers = async {
            tokio::try_join!(
                api.serve(routers.api),
                machine,
                async {
                    ops_done.await.unwrap_or_else(|_| {
                        Err(io::Error::other("the ops listener's thread ended"))
                    })
                },
                nmos.serve(routers.nmos),
            )
        };
        let reload = async {
            let mut ticks = tokio::time::interval(SECRET_RELOAD_PERIOD);
            loop {
                ticks.tick().await;
                let _ = service.reload_secrets();
            }
        };
        let renew = renew_sessions(&service.sessions, &*service.state);
        tokio::select! {
            r = servers => r.map(|_| ()),
            () = reload => unreachable!("the reload loop never returns"),
            () = renew => unreachable!("the renew loop never returns"),
        }
    }
}

/// The thread of the ops listener and the heartbeat sampling, on a
/// runtime of its own: stopped and joined when dropped.
struct OpsThread {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for OpsThread {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Passes the live sessions to the state every [`SESSION_RENEW_PERIOD`];
/// never returns. [`Bound::serve`] runs it.
pub async fn renew_sessions(sessions: &Sessions, state: &dyn ReplicatedState) {
    let mut ticks = tokio::time::interval(SESSION_RENEW_PERIOD);
    loop {
        ticks.tick().await;
        let live = sessions.live();
        if !live.is_empty() {
            state.renew_sessions(&live);
        }
    }
}

async fn host_only(State(auth): State<Arc<Auth>>, req: Request, next: Next) -> Response {
    if auth.host_allowed(&req) {
        next.run(req).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

/// Clears the API serving flag when `serve` returns or is dropped.
struct Serving(Ops);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.set_api_serving(false);
    }
}
