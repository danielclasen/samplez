//! The listeners and the routers on them.

use std::future::{Future, IntoFuture};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Query;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::routing::{get, post};
use axum::{Extension, Router, middleware};
use serde::Deserialize;
use tokio::net::TcpListener;

use crate::auth::{self, Auth, Identity, Listener};
use crate::config::{AuthMode, ServiceConfig};
use crate::ops::Ops;
use crate::replica::ReplicatedState;
use crate::{Error, config};

/// How often [`Bound::serve`] re-reads the token files.
pub const SECRET_RELOAD_PERIOD: Duration = Duration::from_secs(10);

type WsHandler =
    Arc<dyn Fn(WebSocket, Connect) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What the WebSocket handler learns about a client at the upgrade.
#[derive(Debug, Clone)]
pub struct Connect {
    pub identity: Identity,
    /// The last global revision the client saw, from `?since=`; the
    /// argument of the same name to [`crate::ws::pump`].
    pub since: Option<u64>,
}

#[derive(Deserialize)]
struct WsQuery {
    since: Option<u64>,
}

pub struct Service {
    config: ServiceConfig,
    auth: Arc<Auth>,
    ops: Ops,
    state: Arc<dyn ReplicatedState>,
    api: Router,
    ws: Option<WsHandler>,
    nmos: Router,
}

/// The routers of each listener, for serving or for driving in tests.
pub struct Routers {
    pub api: Router,
    pub machine: Option<Router>,
    pub ops: Router,
    pub nmos: Router,
}

impl Service {
    /// Checks the auth settings against the listeners and reads the token
    /// files.
    pub fn new(
        config: ServiceConfig,
        ops: Ops,
        state: Arc<dyn ReplicatedState>,
    ) -> Result<Service, Error> {
        Ok(Service {
            auth: Arc::new(Auth::new(&config)?),
            config,
            ops,
            state,
            api: Router::new(),
            ws: None,
            nmos: Router::new(),
        })
    }

    /// Routes served under `/api/v1` on the API listener and, in `proxy`
    /// mode, the machine listener.
    pub fn api(mut self, router: Router) -> Service {
        self.api = router;
        self
    }

    /// Serves WebSocket upgrades at `/api/v1/ws` once auth and the `Origin`
    /// check pass.
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

    /// The NMOS listener's router, served as is: no auth, `Authorization`
    /// is not read.
    pub fn nmos(mut self, router: Router) -> Service {
        self.nmos = router;
        self
    }

    /// Re-reads the token files now. Sessions issued under a replaced API
    /// token end.
    pub fn reload_secrets(&self) -> Result<(), Error> {
        self.auth.reload()
    }

    pub fn routers(&self) -> Routers {
        let api = self.authenticated(Listener::Api);
        let machine = self
            .config
            .listeners
            .machine
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
            inner = inner.route(
                "/ws",
                get(
                    move |upgrade: WebSocketUpgrade,
                          Extension(identity): Extension<Identity>,
                          Query(query): Query<WsQuery>| {
                        let handler = handler.clone();
                        let connect = Connect {
                            identity,
                            since: query.since,
                        };
                        async move { upgrade.on_upgrade(move |socket| handler(socket, connect)) }
                    },
                ),
            );
        }
        if listener == Listener::Api && self.auth.mode() == AuthMode::Token {
            inner = inner.route("/login", post(auth::login).with_state(self.auth.clone()));
        }
        Router::new()
            .nest("/api/v1", inner)
            .layer(middleware::from_fn_with_state(
                (self.auth.clone(), listener),
                auth::middleware,
            ))
    }

    /// Binds every listener.
    pub async fn bind(self) -> io::Result<Bound> {
        let listeners = &self.config.listeners;
        let tcp = |l: config::Listener| TcpListener::bind(SocketAddr::new(l.bind, l.port));
        let api = tcp(listeners.api).await?;
        let machine = match listeners.machine {
            Some(l) => Some(tcp(l).await?),
            None => None,
        };
        let ops = tcp(listeners.ops).await?;
        let nmos = tcp(listeners.nmos).await?;
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

pub struct Bound {
    service: Service,
    routers: Routers,
    api: TcpListener,
    machine: Option<TcpListener>,
    ops: TcpListener,
    nmos: TcpListener,
}

impl Bound {
    pub fn api_addr(&self) -> io::Result<SocketAddr> {
        self.api.local_addr()
    }

    pub fn machine_addr(&self) -> Option<io::Result<SocketAddr>> {
        self.machine.as_ref().map(|l| l.local_addr())
    }

    pub fn ops_addr(&self) -> io::Result<SocketAddr> {
        self.ops.local_addr()
    }

    pub fn nmos_addr(&self) -> io::Result<SocketAddr> {
        self.nmos.local_addr()
    }

    /// Serves every listener, watches the heartbeat and reloads the token
    /// files until a listener fails. A failed reload keeps the previous
    /// token. `/readyz` reports the API as serving while this runs.
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
                (Some(l), Some(r)) => axum::serve(l, r).await,
                _ => std::future::pending().await,
            }
        };
        let servers = async {
            tokio::try_join!(
                axum::serve(api, routers.api).into_future(),
                machine,
                axum::serve(ops, routers.ops).into_future(),
                axum::serve(nmos, routers.nmos).into_future(),
            )
        };
        let reload = async {
            let mut ticks = tokio::time::interval(SECRET_RELOAD_PERIOD);
            loop {
                ticks.tick().await;
                let _ = service.auth.reload();
            }
        };
        service.ops.set_api_serving(true);
        let _serving = Serving(service.ops.clone());
        tokio::select! {
            r = servers => r.map(|_| ()),
            () = service.ops.clone().watch_heartbeat() => unreachable!("the watch never returns"),
            () = reload => unreachable!("the reload loop never returns"),
        }
    }
}

/// Clears the API serving flag when `serve` returns or is dropped.
struct Serving(Ops);

impl Drop for Serving {
    fn drop(&mut self) {
        self.0.set_api_serving(false);
    }
}
