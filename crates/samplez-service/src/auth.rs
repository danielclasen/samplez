//! Authentication and the browser protections on the API and machine
//! listeners.
//!
//! On the API listener in `token` and `oidc` mode and on the machine
//! listener, a request carrying `Authorization` is authenticated by bearer
//! only: an invalid one gets 401 and neither cookies nor identity headers
//! are read. `none` mode and the `proxy` mode API listener ignore
//! `Authorization`.
//! Requests without a valid bearer must name an allowed `Host` and, when
//! they change state, carry `X-Samplez-Request: 1` with a JSON content
//! type. Every WebSocket upgrade with an `Origin` must name an allowed one;
//! one without is accepted only with a valid bearer or in `none` mode. The
//! same `Origin` rule holds for every request to the WebSocket path and for
//! every `CONNECT`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::watch;
use utoipa::ToSchema;

use crate::config::{AuthMode, ProxyConfig, ServiceConfig, TlsMode};
use crate::metrics::{Counter, Metrics};
use crate::oidc::{self, Failure, Oidc, Pending};
use crate::{Error, ServiceStatus};

pub const SESSION_COOKIE: &str = "samplez_session";
pub const CSRF_HEADER: &str = "x-samplez-request";
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const LOGIN_PATH: &str = oidc::LOGIN_PATH;
const LOGOUT_PATH: &str = "/api/v1/logout";
const SESSION_PATH: &str = "/api/v1/session";
pub(crate) const WS_PATH: &str = "/api/v1/ws";

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
}

/// Inserted as a request extension on every authenticated request. A
/// viewer's state-changing HTTP requests are refused before they reach a
/// route; WebSocket messages are not HTTP requests, so a WebSocket handler
/// calls [`crate::Connect::require_operator`] before it applies a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The user named by the proxy or by the OpenID provider's token;
    /// `None` in the other modes and for a static bearer token.
    pub user: Option<String>,
    pub role: Role,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listener {
    Api,
    Machine,
}

struct Secret {
    name: &'static str,
    path: PathBuf,
    /// The fewest bytes a value may have.
    min: usize,
    value: RwLock<Vec<u8>>,
    /// Seconds since the Unix epoch of the last successful read.
    loaded_at: AtomicU64,
    failures: Counter,
}

impl Secret {
    fn load(name: &'static str, path: &Path, metrics: &Metrics) -> Result<Secret, Error> {
        Secret::load_min(name, path, 1, metrics)
    }

    fn load_min(
        name: &'static str,
        path: &Path,
        min: usize,
        metrics: &Metrics,
    ) -> Result<Secret, Error> {
        Ok(Secret {
            name,
            path: path.to_owned(),
            min,
            value: RwLock::new(read_secret(path, min)?),
            loaded_at: AtomicU64::new(now()),
            failures: metrics.counter(
                "samplez_secret_reload_failures_total",
                "Failed re-reads of a secret file; the previous value stays in use.",
                &[("secret", name)],
            ),
        })
    }

    fn reload(&self) -> Result<(), Error> {
        let value = match read_secret(&self.path, self.min) {
            Ok(v) => v,
            Err(e) => {
                self.failures.add(1);
                tracing::warn!(secret = self.name, error = %e, "keeping the previous secret");
                return Err(e);
            }
        };
        let mut current = self.value.write().expect("secret lock poisoned");
        if *current != value {
            tracing::info!(secret = self.name, "secret changed");
            *current = value;
        }
        self.loaded_at.store(now(), Ordering::Relaxed);
        Ok(())
    }

    fn get(&self) -> Vec<u8> {
        self.value.read().expect("secret lock poisoned").clone()
    }

    /// Constant-time in the content of both; compares SHA-256 digests so
    /// the length of the secret does not show either.
    fn matches(&self, given: &[u8]) -> bool {
        let given = Sha256::digest(given);
        let own = Sha256::digest(self.get());
        given.ct_eq(&own).into()
    }
}

fn read_secret(path: &Path, min: usize) -> Result<Vec<u8>, Error> {
    let mut value = std::fs::read(path).map_err(|e| Error::Secret(path.to_owned(), e))?;
    value.truncate(value.trim_ascii_end().len());
    if value.is_empty() {
        return Err(Error::Config(format!("{} is empty", path.display())));
    }
    if value.len() < min {
        return Err(Error::Config(format!(
            "{} holds {} bytes, fewer than {min}",
            path.display(),
            value.len()
        )));
    }
    Ok(value)
}

/// The most signed-out sessions remembered; past it the one ending first
/// is forgotten.
pub const MAX_REVOKED: usize = 65_536;
/// The fewest bytes of `auth.oidc.sessionKey`.
pub const MIN_SESSION_KEY: usize = 32;

/// When the credentials a WebSocket was opened with end: a session
/// cookie's expiry or its sign-out on this process, or an access token's
/// `exp`. [`crate::ws::serve_client`] closes the connection then. A static
/// bearer token and `none` and `proxy` mode never end.
#[derive(Clone, Default)]
pub struct SessionEnd {
    /// Seconds since the Unix epoch.
    at: Option<u64>,
    revoked: Option<Revocable>,
}

#[derive(Clone)]
struct Revocable {
    auth: Arc<Auth>,
    id: String,
    changes: watch::Receiver<u64>,
}

impl std::fmt::Debug for SessionEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionEnd")
            .field("at", &self.at)
            .field("revocable", &self.revoked.is_some())
            .finish()
    }
}

impl SessionEnd {
    /// Never ends.
    pub fn never() -> SessionEnd {
        SessionEnd::default()
    }

    /// Ends at `at`, seconds since the Unix epoch.
    pub fn at(at: u64) -> SessionEnd {
        SessionEnd {
            at: Some(at),
            revoked: None,
        }
    }

    /// Seconds since the Unix epoch it ends at, unless signed out sooner.
    pub fn expiry(&self) -> Option<u64> {
        self.at
    }

    /// Completes when the session ends.
    pub async fn ended(&self) {
        let expire = async {
            match self.at {
                Some(at) => tokio::time::sleep(Duration::from_secs(at.saturating_sub(now()))).await,
                None => std::future::pending().await,
            }
        };
        let revoke = async {
            let Some(r) = &self.revoked else {
                return std::future::pending().await;
            };
            let mut changes = r.changes.clone();
            loop {
                if r.auth.is_revoked(&r.id) {
                    return;
                }
                if changes.changed().await.is_err() {
                    return std::future::pending().await;
                }
            }
        };
        tokio::select! {
            () = expire => {}
            () = revoke => {}
        }
    }
}

pub(crate) struct Auth {
    mode: AuthMode,
    token: Option<Secret>,
    machine_token: Option<Secret>,
    proxy: ProxyConfig,
    oidc: Option<Oidc>,
    client_secret: Option<Secret>,
    session_key: Option<Secret>,
    /// The session key of `oidc` mode without a configured one.
    drawn_key: Vec<u8>,
    /// Signed-out sessions by id, with their expiry: this process only,
    /// and lost when it restarts.
    revoked: std::sync::Mutex<HashMap<String, u64>>,
    /// Counts every sign-out, so open WebSockets look again.
    revocations: watch::Sender<u64>,
    hosts: Vec<String>,
    origins: Vec<String>,
    /// The API listener runs TLS, so session cookies are `Secure`.
    tls: bool,
}

/// The outcome of a request's `Authorization` header.
enum Bearer {
    /// None, or one the listener and mode ignore.
    Absent,
    Valid(Identity, SessionEnd),
}

impl Auth {
    pub(crate) fn new(config: &ServiceConfig, metrics: &Metrics) -> Result<Auth, Error> {
        let auth = &config.auth;
        let listeners = &config.listeners;
        if auth.mode == AuthMode::Proxy && listeners.api.bind != std::net::Ipv4Addr::LOCALHOST {
            return Err(Error::Config(
                "proxy mode binds the API listener to 127.0.0.1 only".into(),
            ));
        }
        if listeners.machine.is_some() {
            if auth.mode != AuthMode::Proxy {
                return Err(Error::Config(
                    "the machine listener exists in proxy mode only".into(),
                ));
            }
            if auth.machine_token.is_none() {
                return Err(Error::Config(
                    "the machine listener needs auth.machineToken".into(),
                ));
            }
        }
        if auth.mode == AuthMode::Proxy && auth.proxy.group_roles.is_empty() {
            return Err(Error::Config(
                "proxy mode needs auth.proxy.groupRoles; without it nobody gets a role".into(),
            ));
        }
        let token = match (auth.mode, &auth.token) {
            (AuthMode::Token | AuthMode::Oidc, Some(t)) => {
                Some(Secret::load("token", &t.file, metrics)?)
            }
            (AuthMode::Token, None) => {
                return Err(Error::Config("token mode needs auth.token".into()));
            }
            _ => None,
        };
        let oidc = match (auth.mode, &auth.oidc) {
            (AuthMode::Oidc, Some(o)) => Some(Oidc::new(o)?),
            (AuthMode::Oidc, None) => {
                return Err(Error::Config("oidc mode needs auth.oidc".into()));
            }
            _ => None,
        };
        let oidc_config = oidc.as_ref().map(|o| &o.config);
        let client_secret = match oidc_config.and_then(|o| o.client_secret.as_ref()) {
            Some(f) => Some(Secret::load("oidcClientSecret", &f.file, metrics)?),
            None => None,
        };
        let session_key = match oidc_config.and_then(|o| o.session_key.as_ref()) {
            Some(f) => Some(Secret::load_min(
                "oidcSessionKey",
                &f.file,
                MIN_SESSION_KEY,
                metrics,
            )?),
            None => None,
        };
        let drawn_key = match &oidc {
            Some(_) => oidc::random(32)?.into_bytes(),
            None => Vec::new(),
        };
        let machine_token = match (&listeners.machine, &auth.machine_token) {
            (Some(_), Some(t)) => Some(Secret::load("machineToken", &t.file, metrics)?),
            _ => None,
        };
        let port = listeners.api.port;
        let tls = listeners.api.tls.mode != TlsMode::None;
        let scheme = if tls { "https" } else { "http" };
        let mut hosts = vec!["localhost".to_owned(), "127.0.0.1".to_owned()];
        hosts.extend(
            config
                .api
                .allowed_hosts
                .iter()
                .map(|h| h.to_ascii_lowercase()),
        );
        let mut origins = vec![
            format!("{scheme}://localhost:{port}"),
            format!("{scheme}://127.0.0.1:{port}"),
        ];
        origins.extend(config.api.allowed_origins.iter().cloned());
        if let Some(o) = &oidc {
            hosts.push(o.public_host.clone());
            origins.push(o.public_origin.clone());
        }
        let origins = origins.iter().map(|o| normalize_origin(o)).collect();
        Ok(Auth {
            mode: auth.mode,
            token,
            machine_token,
            proxy: auth.proxy.clone(),
            oidc,
            client_secret,
            session_key,
            drawn_key,
            revoked: std::sync::Mutex::default(),
            revocations: watch::Sender::new(0),
            hosts,
            origins,
            tls,
        })
    }

    fn secrets(&self) -> impl Iterator<Item = &Secret> {
        [
            &self.token,
            &self.machine_token,
            &self.client_secret,
            &self.session_key,
        ]
        .into_iter()
        .flatten()
    }

    pub(crate) fn oidc(&self) -> Option<&Oidc> {
        self.oidc.as_ref()
    }

    pub(crate) fn client_secret(&self) -> Option<Vec<u8>> {
        self.client_secret.as_ref().map(Secret::get)
    }

    /// The key `oidc` mode signs its cookies with.
    fn oidc_key(&self) -> Vec<u8> {
        match &self.session_key {
            Some(k) => k.get(),
            None => self.drawn_key.clone(),
        }
    }

    /// Whether the browser reached the API over https: the listener runs
    /// TLS, the public URL is https, or the proxy in front says so in
    /// `X-Forwarded-Proto`. A chain of proxies appends its schemes; the
    /// first is the client's.
    pub(crate) fn secure(&self, headers: &HeaderMap) -> bool {
        self.tls
            || self.oidc.as_ref().is_some_and(|o| o.public_https)
            || headers.get("x-forwarded-proto").is_some_and(|p| {
                let first = p
                    .as_bytes()
                    .split(|&b| b == b',')
                    .next()
                    .unwrap_or_default();
                first.trim_ascii().eq_ignore_ascii_case(b"https")
            })
    }

    pub(crate) fn login_cookie(&self, pending: &Pending, secure: bool) -> String {
        let expiry = now() + oidc::LOGIN_TTL.as_secs();
        let payload = oidc::encode_pending(pending);
        let tag = hex(&signed(
            &self.oidc_key(),
            b"samplez-oidc-login:",
            &[&expiry.to_string(), &payload],
        ));
        // Lax: the provider sends the browser back with a cross-site
        // navigation, which carries no Strict cookie.
        format!(
            "{}={expiry}.{payload}.{tag}; Path={LOGIN_PATH}; Max-Age={}; HttpOnly; SameSite=Lax{}",
            oidc::LOGIN_COOKIE,
            oidc::LOGIN_TTL.as_secs(),
            if secure { "; Secure" } else { "" },
        )
    }

    pub(crate) fn clear_login_cookie(&self, secure: bool) -> String {
        format!(
            "{}=; Path={LOGIN_PATH}; Max-Age=0; HttpOnly; SameSite=Lax{}",
            oidc::LOGIN_COOKIE,
            if secure { "; Secure" } else { "" },
        )
    }

    /// The sign-in in progress the login cookie holds, when it is ours and
    /// current.
    pub(crate) fn pending(&self, headers: &HeaderMap) -> Option<Pending> {
        let key = self.oidc_key();
        let now = now();
        cookies(headers, oidc::LOGIN_COOKIE).find_map(|value| {
            let mut parts = value.split('.');
            let (expiry, payload, tag) = (parts.next()?, parts.next()?, parts.next()?);
            let expiry = digits(expiry)?;
            if parts.next().is_some()
                || expiry <= now
                || expiry > now + oidc::LOGIN_TTL.as_secs()
                || !tag_valid(
                    &key,
                    b"samplez-oidc-login:",
                    &[&expiry.to_string(), payload],
                    tag,
                )
            {
                return None;
            }
            oidc::decode_pending(payload)
        })
    }

    /// The session cookie of an `oidc` mode sign-in, valid until `expiry`.
    pub(crate) fn oidc_session_cookie(
        &self,
        identity: &Identity,
        expiry: u64,
        now: u64,
        secure: bool,
    ) -> Result<String, Error> {
        let id = oidc::random(16)?;
        Ok(session_cookie(
            &oidc_session_value(&self.oidc_key(), expiry, &id, identity),
            expiry.saturating_sub(now),
            secure,
        ))
    }

    /// The longest a session of this mode lasts, in seconds.
    fn session_ttl(&self) -> u64 {
        match &self.oidc {
            Some(o) => o.session_ttl,
            None => SESSION_TTL.as_secs(),
        }
    }

    fn is_revoked(&self, id: &str) -> bool {
        self.revoked
            .lock()
            .expect("revocations lock poisoned")
            .contains_key(id)
    }

    /// Remembers that session `id`, valid until `expiry`, was signed out,
    /// and wakes the WebSockets opened with it.
    fn revoke(&self, id: &str, expiry: u64) {
        let now = now();
        let mut revoked = self.revoked.lock().expect("revocations lock poisoned");
        if revoked.len() >= MAX_REVOKED {
            revoked.retain(|_, until| *until > now);
        }
        if revoked.len() >= MAX_REVOKED
            && let Some(first) = revoked
                .iter()
                .min_by_key(|(_, until)| **until)
                .map(|(id, _)| id.clone())
        {
            revoked.remove(&first);
        }
        revoked.insert(id.to_owned(), expiry);
        drop(revoked);
        self.revocations.send_modify(|n| *n += 1);
    }

    /// The identity, id and expiry of the current session cookie the
    /// request carries, if any.
    fn cookie_session(&self, headers: &HeaderMap) -> Option<(Identity, String, u64)> {
        let now = now();
        let ttl = self.session_ttl();
        match self.mode {
            AuthMode::Token => {
                let key = self.token.as_ref()?.get();
                cookies(headers, SESSION_COOKIE).find_map(|c| {
                    let (expiry, id) = token_session(&key, c, now)?;
                    let operator = Identity {
                        user: None,
                        role: Role::Operator,
                    };
                    (!self.is_revoked(&id)).then_some((operator, id, expiry))
                })
            }
            AuthMode::Oidc => {
                let key = self.oidc_key();
                cookies(headers, SESSION_COOKIE).find_map(|c| {
                    let s = oidc_session(&key, c, now, ttl)?;
                    (!self.is_revoked(&s.1)).then_some(s)
                })
            }
            AuthMode::None | AuthMode::Proxy => None,
        }
    }

    fn session_end(self: &Arc<Self>, id: String, expiry: u64) -> SessionEnd {
        SessionEnd {
            at: Some(expiry),
            revoked: Some(Revocable {
                auth: self.clone(),
                id,
                changes: self.revocations.subscribe(),
            }),
        }
    }

    /// Re-reads every token file, each on its own: a failed read keeps that
    /// secret's previous value and does not stop the others. Returns the
    /// first error.
    pub(crate) fn reload(&self) -> Result<(), Error> {
        let mut first = Ok(());
        for secret in self.secrets() {
            if let Err(e) = secret.reload() {
                first = first.and(Err(e));
            }
        }
        first
    }

    pub(crate) fn status(&self, status: &mut ServiceStatus) {
        for secret in self.secrets() {
            status.secrets.push(crate::SecretStatus {
                name: secret.name,
                loaded_at: secret.loaded_at.load(Ordering::Relaxed),
            });
        }
    }

    pub(crate) fn mode(&self) -> AuthMode {
        self.mode
    }

    /// Authenticates `Authorization`: the machine token on the machine
    /// listener, the API token in `token` mode, and in `oidc` mode the API
    /// token when one is configured or else an access token of the
    /// provider.
    async fn bearer(&self, listener: Listener, headers: &HeaderMap) -> Result<Bearer, Refused> {
        let reads = match listener {
            Listener::Machine => true,
            Listener::Api => matches!(self.mode, AuthMode::Token | AuthMode::Oidc),
        };
        if !reads {
            return Ok(Bearer::Absent);
        }
        let Some(value) = single(headers, header::AUTHORIZATION.as_str())? else {
            return match listener {
                Listener::Machine => Err(Refused::UNAUTHORIZED),
                Listener::Api => Ok(Bearer::Absent),
            };
        };
        let token = bearer_token(value).ok_or(Refused::UNAUTHORIZED)?;
        let operator = Identity {
            user: None,
            role: Role::Operator,
        };
        let secret = match listener {
            Listener::Machine => self.machine_token.as_ref(),
            Listener::Api => self.token.as_ref(),
        };
        if secret.is_some_and(|s| s.matches(token)) {
            return Ok(Bearer::Valid(operator, SessionEnd::never()));
        }
        let oidc = match (listener, &self.oidc) {
            (Listener::Api, Some(o)) => o,
            _ => return Err(Refused::UNAUTHORIZED),
        };
        let token = std::str::from_utf8(token).map_err(|_| Refused::UNAUTHORIZED)?;
        match oidc.bearer(token).await {
            Ok((identity, exp)) => Ok(Bearer::Valid(identity, SessionEnd::at(exp))),
            Err(Failure::NoRole) => Err(Refused(StatusCode::FORBIDDEN, "no role")),
            Err(Failure::Token(why)) => {
                tracing::debug!(?why, "bearer token refused");
                Err(Refused::UNAUTHORIZED)
            }
            Err(Failure::Provider(e)) => {
                tracing::warn!(error = %e, "bearer token not checked");
                Err(Refused(
                    StatusCode::UNAUTHORIZED,
                    "the identity provider cannot be reached",
                ))
            }
        }
    }

    /// `Ok(None)` lets the request through without an identity: the login
    /// and logout routes, and the session route of a client that is not
    /// signed in.
    fn check(
        self: &Arc<Self>,
        req: &Request,
        bearer: Bearer,
    ) -> Result<Option<(Identity, SessionEnd)>, Refused> {
        let headers = req.headers();
        let (bearer, bearer_identity) = match bearer {
            Bearer::Valid(identity, end) => (true, Some((identity, end))),
            Bearer::Absent => (false, None),
        };

        if !bearer && !self.host_allowed(req) {
            return Err(Refused(StatusCode::FORBIDDEN, "host not allowed"));
        }
        if is_upgrade(headers) || req.method() == Method::CONNECT || req.uri().path() == WS_PATH {
            match headers.get(header::ORIGIN) {
                Some(o) if !self.origin_allowed(o.as_bytes()) => {
                    return Err(Refused(StatusCode::FORBIDDEN, "origin not allowed"));
                }
                None if !bearer && self.mode != AuthMode::None => {
                    return Err(Refused(StatusCode::FORBIDDEN, "origin required"));
                }
                _ => {}
            }
        }
        let writes = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
        if !bearer && writes && !csrf_ok(headers) {
            return Err(Refused(
                StatusCode::FORBIDDEN,
                "X-Samplez-Request: 1 and Content-Type: application/json required",
            ));
        }

        let operator = Identity {
            user: None,
            role: Role::Operator,
        };
        let path = req.uri().path();
        let open = match self.mode {
            AuthMode::Token => [LOGIN_PATH, LOGOUT_PATH].contains(&path),
            AuthMode::Oidc => [LOGIN_PATH, oidc::CALLBACK_PATH, LOGOUT_PATH].contains(&path),
            AuthMode::None | AuthMode::Proxy => false,
        };
        let identity = match bearer_identity {
            Some(identity) => Ok(identity),
            None if open => return Ok(None),
            None => match self.mode {
                AuthMode::None => Ok((operator, SessionEnd::never())),
                AuthMode::Token | AuthMode::Oidc => match self.cookie_session(headers) {
                    Some((identity, id, expiry)) => Ok((identity, self.session_end(id, expiry))),
                    None => Err(Refused::UNAUTHORIZED),
                },
                AuthMode::Proxy => self
                    .proxy_identity(headers)
                    .map(|i| (i, SessionEnd::never())),
            },
        };
        let identity = match identity {
            Ok(identity) => identity,
            Err(_) if path == SESSION_PATH => return Ok(None),
            Err(refused) => return Err(refused),
        };
        if writes && identity.0.role == Role::Viewer {
            return Err(Refused(StatusCode::FORBIDDEN, "viewers may not write"));
        }
        Ok(Some(identity))
    }

    fn origin_allowed(&self, origin: &[u8]) -> bool {
        std::str::from_utf8(origin)
            .is_ok_and(|o| self.origins.iter().any(|a| *a == normalize_origin(o)))
    }

    pub(crate) fn host_allowed(&self, req: &Request) -> bool {
        if req.headers().get_all(header::HOST).iter().nth(1).is_some() {
            return false;
        }
        let host = match req.headers().get(header::HOST) {
            Some(h) => h.to_str().ok(),
            None => req.uri().authority().map(|a| a.as_str()),
        };
        let Some(host) = host else { return false };
        let name = match host.strip_prefix('[') {
            Some(rest) => rest.split(']').next().unwrap_or(""),
            None => host.rsplit_once(':').map_or(host, |(name, _)| name),
        };
        self.hosts.iter().any(|h| h.eq_ignore_ascii_case(name))
    }

    fn proxy_identity(&self, headers: &HeaderMap) -> Result<Identity, Refused> {
        let user = single(headers, &self.proxy.user_header)?
            .and_then(|v| std::str::from_utf8(v).ok())
            .filter(|u| !u.is_empty())
            .ok_or(Refused::UNAUTHORIZED)?;
        let role = single(headers, &self.proxy.groups_header)?
            .and_then(|v| std::str::from_utf8(v).ok())
            .into_iter()
            .flat_map(|v| v.split(','))
            .filter_map(|g| self.proxy.group_roles.get(g.trim()))
            .max()
            .copied()
            .ok_or(Refused(StatusCode::FORBIDDEN, "no group with a role"))?;
        Ok(Identity {
            user: Some(user.to_owned()),
            role,
        })
    }
}

/// The value of a header that may appear at most once; a repeated one is
/// refused, since the parts in front of the service may disagree on which
/// copy counts.
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a [u8]>, Refused> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(Refused(StatusCode::UNAUTHORIZED, "repeated header"));
    }
    Ok(first.map(|v| v.as_bytes()))
}

/// Lowercase, without a trailing `/` and without the scheme's default
/// port, so equal origins compare equal whatever way they are written.
pub(crate) fn normalize_origin(origin: &str) -> String {
    let lower = origin.trim().to_ascii_lowercase();
    let lower = lower.strip_suffix('/').unwrap_or(&lower);
    for (scheme, port) in [("http://", ":80"), ("https://", ":443")] {
        if lower.starts_with(scheme)
            && let Some(rest) = lower.strip_suffix(port)
        {
            return rest.to_owned();
        }
    }
    lower.to_owned()
}

fn bearer_token(value: &[u8]) -> Option<&[u8]> {
    let (scheme, token) = value.split_at_checked(7)?;
    (scheme.eq_ignore_ascii_case(b"bearer ") && !token.is_empty()).then_some(token)
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .is_some_and(|u| u.as_bytes().eq_ignore_ascii_case(b"websocket"))
}

fn csrf_ok(headers: &HeaderMap) -> bool {
    let marked = headers.get(CSRF_HEADER).is_some_and(|v| v == "1");
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"));
    marked && json
}

/// Every value of the cookie `name`: a browser sends one per matching path
/// and domain, and only one of them may be current.
fn cookies<'a>(headers: &'a HeaderMap, name: &'a str) -> impl Iterator<Item = &'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(move |c| c.trim().strip_prefix(name)?.strip_prefix('='))
}

/// Seconds since the Unix epoch.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs()
}

/// Keyed by the token, so a session ends when the token changes.
fn session_mac(token: &[u8], expiry: u64) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(token).expect("HMAC takes any key length");
    mac.update(b"samplez-session:");
    mac.update(expiry.to_string().as_bytes());
    mac
}

fn session_value(token: &[u8], expiry: u64) -> String {
    let tag = session_mac(token, expiry).finalize().into_bytes();
    let hex: String = tag.iter().map(|b| format!("{b:02x}")).collect();
    format!("{expiry}.{hex}")
}

#[cfg(test)]
fn session_valid(token: &[u8], value: &str, now: u64) -> bool {
    token_session(token, value, now).is_some()
}

/// The expiry and id of a current `token` mode session cookie; its id is
/// its tag, written out from the bytes the cookie decodes to.
fn token_session(token: &[u8], value: &str, now: u64) -> Option<(u64, String)> {
    let (expiry, text) = value.split_once('.')?;
    let expiry = digits(expiry)?;
    if expiry <= now || expiry > now + SESSION_TTL.as_secs() {
        return None;
    }
    let tag = unhex(text)?;
    session_mac(token, expiry).verify_slice(&tag).ok()?;
    Some((expiry, hex(&tag)))
}

/// A decimal number of ASCII digits only, no sign.
fn digits(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 64 lower-case hex digits, a SHA-256 HMAC tag, as [`hex`] writes it and
/// in no other spelling: a tag has one text, so a cookie has one text.
fn unhex(hex: &str) -> Option<Vec<u8>> {
    let digit = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    };
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    bytes
        .chunks_exact(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// HMAC-SHA-256 under `key` of `domain` and `parts` joined by `.`.
fn signer(key: &[u8], domain: &[u8], parts: &[&str]) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(domain);
    mac.update(parts.join(".").as_bytes());
    mac
}

fn signed(key: &[u8], domain: &[u8], parts: &[&str]) -> Vec<u8> {
    signer(key, domain, parts).finalize().into_bytes().to_vec()
}

fn tag_valid(key: &[u8], domain: &[u8], parts: &[&str], tag: &str) -> bool {
    unhex(tag).is_some_and(|t| signer(key, domain, parts).verify_slice(&t).is_ok())
}

const OIDC_SESSION: &[u8] = b"samplez-oidc-session:";

/// `<expiry>.<role>.<user, base64url>.<id>.<tag>`; `id` is base64url and
/// names the session for its sign-out.
fn oidc_session_value(key: &[u8], expiry: u64, id: &str, identity: &Identity) -> String {
    let role = match identity.role {
        Role::Operator => "operator",
        Role::Viewer => "viewer",
    };
    let user = oidc::encode_user(identity.user.as_deref().unwrap_or_default());
    let expiry = expiry.to_string();
    let tag = hex(&signed(key, OIDC_SESSION, &[&expiry, role, &user, id]));
    format!("{expiry}.{role}.{user}.{id}.{tag}")
}

/// The identity, id and expiry of a current `oidc` mode session cookie
/// no longer than `ttl` seconds from `now`.
fn oidc_session(key: &[u8], value: &str, now: u64, ttl: u64) -> Option<(Identity, String, u64)> {
    let mut parts = value.split('.');
    let (expiry, role, user, id, tag) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    let at = digits(expiry)?;
    if parts.next().is_some()
        || at <= now
        || at > now + ttl
        || id.is_empty()
        || !tag_valid(key, OIDC_SESSION, &[expiry, role, user, id], tag)
    {
        return None;
    }
    let role = match role {
        "operator" => Role::Operator,
        "viewer" => Role::Viewer,
        _ => return None,
    };
    let identity = Identity {
        user: Some(oidc::decode_user(user)?),
        role,
    };
    Some((identity, id.to_owned(), at))
}

fn session_cookie(value: &str, max_age: u64, secure: bool) -> String {
    format!(
        "{SESSION_COOKIE}={value}; Path=/api/v1; Max-Age={max_age}; HttpOnly; SameSite=Strict{}",
        if secure { "; Secure" } else { "" },
    )
}

/// A refusal of [`Auth::check`]: the status and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refused(StatusCode, &'static str);

impl Refused {
    const UNAUTHORIZED: Refused = Refused(StatusCode::UNAUTHORIZED, "not authenticated");
}

/// Renders the response to a request the checks refuse, from its status
/// and a short reason ([`crate::Service::refusal`]).
pub(crate) type Refusal = Arc<dyn Fn(StatusCode, &'static str) -> Response + Send + Sync>;

pub(crate) async fn middleware(
    State((auth, listener, refusal)): State<(Arc<Auth>, Listener, Option<Refusal>)>,
    mut req: Request,
    next: Next,
) -> Response {
    let checked = match auth.bearer(listener, req.headers()).await {
        Ok(bearer) => auth.check(&req, bearer),
        Err(refused) => Err(refused),
    };
    match checked {
        Ok(identity) => {
            if let Some((identity, end)) = identity {
                req.extensions_mut().insert(identity);
                req.extensions_mut().insert(end);
            }
            next.run(req).await
        }
        Err(Refused(status, reason)) => {
            let mut response = match &refusal {
                Some(render) => render(status, reason),
                None => status.into_response(),
            };
            if status == StatusCode::UNAUTHORIZED {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    header::HeaderValue::from_static("Bearer"),
                );
            }
            response
        }
    }
}

#[derive(Deserialize, ToSchema)]
pub struct Login {
    pub token: String,
}

/// Exchanges the API token for a session cookie; `token` mode only.
#[utoipa::path(
    post,
    path = "/login",
    tag = "auth",
    request_body = Login,
    security(()),
    params(("X-Samplez-Request" = String, Header, description = "Must be 1")),
    responses(
        (status = 204, description = "Session cookie set for 12 h"),
        (status = 401, description = "Wrong token"),
        (status = 403, description = "Host not allowed or request header missing"),
    )
)]
pub(crate) async fn login(
    State(auth): State<Arc<Auth>>,
    headers: HeaderMap,
    Json(body): Json<Login>,
) -> Response {
    let token = auth.token.as_ref().expect("login is routed in token mode");
    if !token.matches(body.token.as_bytes()) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
        )
            .into_response();
    }
    let cookie = session_cookie(
        &session_value(&token.get(), now() + SESSION_TTL.as_secs()),
        SESSION_TTL.as_secs(),
        auth.secure(&headers),
    );
    (
        StatusCode::NO_CONTENT,
        [
            (header::SET_COOKIE, cookie),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response()
}

#[derive(Serialize, ToSchema)]
pub struct Logout {
    /// Where the browser goes next to end its session at the identity
    /// provider too; `oidc` mode with a provider that has an end-session
    /// endpoint only.
    pub redirect: Option<String>,
}

/// Clears the session cookie and ends the session on this process until
/// it restarts, for
/// any copy of the cookie and the WebSockets opened with it; `token` and
/// `oidc` mode.
#[utoipa::path(
    post,
    path = "/logout",
    tag = "auth",
    security(()),
    params(("X-Samplez-Request" = String, Header, description = "Must be 1")),
    responses(
        (status = 200, description = "Session cookie cleared", body = Logout),
        (status = 403, description = "Host not allowed or request header missing"),
    )
)]
pub(crate) async fn logout(State(auth): State<Arc<Auth>>, headers: HeaderMap) -> Response {
    // The cookie is cleared in the browser; a copy of it, and the
    // WebSockets opened with it, end here too.
    if let Some((_, id, expiry)) = auth.cookie_session(&headers) {
        auth.revoke(&id, expiry);
    }
    let redirect = match &auth.oidc {
        Some(o) => o.end_session().await,
        None => None,
    };
    (
        [
            (
                header::SET_COOKIE,
                session_cookie("", 0, auth.secure(&headers)),
            ),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
        Json(Logout { redirect }),
    )
        .into_response()
}

#[derive(Serialize, ToSchema)]
pub struct Session {
    pub mode: AuthMode,
    /// `None` when not signed in, in `none` mode and for a static token.
    pub user: Option<String>,
    /// `None` when not signed in.
    pub role: Option<Role>,
}

/// Who the request is authenticated as. Answered without credentials as
/// well, with no role, so a client learns how to sign in: in `oidc` mode
/// at `GET /login`, in `token` mode with `POST /login`.
#[utoipa::path(
    get,
    path = "/session",
    tag = "auth",
    security(()),
    responses(
        (status = 200, description = "The mode and the identity, if any", body = Session),
        (status = 403, description = "Host not allowed"),
    )
)]
pub(crate) async fn session(
    State(auth): State<Arc<Auth>>,
    identity: Option<axum::Extension<Identity>>,
) -> Json<Session> {
    let identity = identity.map(|axum::Extension(i)| i);
    Json(Session {
        mode: auth.mode,
        role: identity.as_ref().map(|i| i.role),
        user: identity.and_then(|i| i.user),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_expires_and_follows_the_token() {
        let t = 1_000_000;
        let v = session_value(b"secret", t + 60);
        assert!(session_valid(b"secret", &v, t));
        assert!(!session_valid(b"secret", &v, t + 60));
        assert!(!session_valid(b"other", &v, t));
        let far = session_value(b"secret", t + SESSION_TTL.as_secs() + 1);
        assert!(!session_valid(b"secret", &far, t));
        assert!(!session_valid(b"secret", "", t));
        assert!(!session_valid(b"secret", &v.replace('.', "x"), t));
        assert!(!session_valid(b"secret", &format!("+{v}"), t), "sign");
    }

    #[test]
    fn an_oidc_session_carries_user_and_role_and_follows_the_key() {
        let t = 1_000_000;
        let ttl = 3600;
        let alice = Identity {
            user: Some("alice.o'neil@example".into()),
            role: Role::Viewer,
        };
        let v = oidc_session_value(b"key", t + 60, "sid1", &alice);
        let got = |v: &str, key: &[u8], at: u64| oidc_session(key, v, at, ttl).map(|s| s.0);
        assert_eq!(
            oidc_session(b"key", &v, t, ttl),
            Some((alice.clone(), "sid1".to_owned(), t + 60))
        );
        assert_eq!(got(&v, b"key", t + 60), None, "expired");
        assert_eq!(got(&v, b"other", t), None, "another key");
        let promoted = v.replacen(".viewer.", ".operator.", 1);
        assert_eq!(got(&promoted, b"key", t), None, "the role is signed");
        let other_id = v.replacen(".sid1.", ".sid2.", 1);
        assert_eq!(got(&other_id, b"key", t), None, "the id is signed");
        let far = oidc_session_value(b"key", t + ttl + 1, "sid1", &alice);
        assert_eq!(
            got(&far, b"key", t),
            None,
            "longer than the session lifetime"
        );
        assert_eq!(got(&format!("{v}.x"), b"key", t), None);
        assert_eq!(got("", b"key", t), None);
    }

    fn oidc_auth() -> Arc<Auth> {
        let config: ServiceConfig = serde_json::from_value(serde_json::json!({
            "auth": { "mode": "oidc", "oidc": {
                "issuerUrl": "https://idp.example/realms/r", "clientId": "c",
                "publicUrl": "https://faderz.example", "operatorRole": "op",
            } }
        }))
        .unwrap();
        Arc::new(Auth::new(&config, &Metrics::default()).unwrap())
    }

    /// A sign-out ends the session for every copy of its cookie and wakes
    /// the WebSockets opened with it; another session goes on.
    #[tokio::test]
    async fn a_sign_out_revokes_the_session_and_ends_its_websockets() {
        let auth = oidc_auth();
        let identity = Identity {
            user: Some("alice".into()),
            role: Role::Operator,
        };
        let now = now();
        let cookie = |c: String| {
            let mut h = HeaderMap::new();
            h.insert(
                header::COOKIE,
                c.split(';').next().unwrap().parse().unwrap(),
            );
            h
        };
        let mine = cookie(
            auth.oidc_session_cookie(&identity, now + 600, now, true)
                .unwrap(),
        );
        let other = cookie(
            auth.oidc_session_cookie(&identity, now + 600, now, true)
                .unwrap(),
        );
        let (_, id, expiry) = auth.cookie_session(&mine).unwrap();
        assert_eq!(expiry, now + 600);
        let end = auth.session_end(id.clone(), expiry);
        let (_, other_id, _) = auth.cookie_session(&other).unwrap();
        let other_end = auth.session_end(other_id, expiry);
        let waiting = tokio::spawn({
            let end = end.clone();
            async move { end.ended().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        auth.revoke(&id, expiry);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the WebSocket's session ended")
            .unwrap();
        assert!(
            auth.cookie_session(&mine).is_none(),
            "a copy of the cookie is refused"
        );
        assert!(auth.cookie_session(&other).is_some());
        let other_waits = tokio::time::timeout(Duration::from_millis(50), other_end.ended()).await;
        assert!(other_waits.is_err(), "another session goes on");
    }

    /// In token mode as well, a signed-out session stays out under every
    /// spelling of its cookie, and its WebSockets end.
    #[tokio::test]
    async fn a_token_session_signed_out_stays_out_in_every_spelling() {
        let file = std::env::temp_dir().join(format!("samplez-revoke-{}", std::process::id()));
        std::fs::write(&file, "token-of-thirty-two-bytes-or-so!").unwrap();
        let config: ServiceConfig = serde_json::from_value(serde_json::json!({
            "auth": { "mode": "token", "token": { "file": file } }
        }))
        .unwrap();
        let auth = Arc::new(Auth::new(&config, &Metrics::default()).unwrap());
        let _ = std::fs::remove_file(&file);
        let expiry = now() + 600;
        let value = session_value(b"token-of-thirty-two-bytes-or-so!", expiry);
        let cookie = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(
                header::COOKIE,
                format!("{SESSION_COOKIE}={v}").parse().unwrap(),
            );
            h
        };
        let (_, id, at) = auth.cookie_session(&cookie(&value)).unwrap();
        let end = auth.session_end(id.clone(), at);
        auth.revoke(&id, at);
        tokio::time::timeout(Duration::from_secs(1), end.ended())
            .await
            .expect("the WebSocket's session ended");
        let (expiry_text, tag) = value.split_once('.').unwrap();
        for copy in [
            value.clone(),
            format!("{expiry_text}.{}", tag.to_ascii_uppercase()),
            format!(
                "{expiry_text}.{}{}",
                tag[..1].to_ascii_uppercase(),
                &tag[1..]
            ),
        ] {
            assert!(auth.cookie_session(&cookie(&copy)).is_none(), "{copy}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_session_ends_at_its_expiry() {
        let end = SessionEnd::at(now() + 30);
        let started = tokio::time::Instant::now();
        end.ended().await;
        assert!(started.elapsed() >= Duration::from_secs(29));
        let never = SessionEnd::never();
        let waited = tokio::time::timeout(Duration::from_secs(86_400), never.ended()).await;
        assert!(waited.is_err());
    }

    #[test]
    fn revocations_are_bounded() {
        let auth = oidc_auth();
        let far = now() + 3600;
        for i in 0..MAX_REVOKED {
            auth.revoke(&format!("s{i}"), far + i as u64);
        }
        auth.revoke("last", far + MAX_REVOKED as u64);
        let revoked = auth.revoked.lock().unwrap();
        assert_eq!(revoked.len(), MAX_REVOKED);
        assert!(!revoked.contains_key("s0"), "the one ending first goes");
        assert!(revoked.contains_key("last") && revoked.contains_key("s1"));
    }

    /// A tag has one spelling: an upper-case or signed copy of a cookie
    /// would otherwise verify under another id than the one signed out.
    #[test]
    fn tags_are_read_in_lower_case_hex_only() {
        let tag = [0x0bu8; 32];
        let text = hex(&tag);
        assert_eq!(unhex(&text), Some(tag.to_vec()));
        assert_eq!(unhex(&text.to_ascii_uppercase()), None);
        assert_eq!(unhex(&format!("+b{}", &text[2..])), None, "a sign");
        assert_eq!(unhex(&format!("0B{}", &text[2..])), None, "mixed case");
        assert_eq!(unhex(&text[2..]), None);
        let t = 1_000_000;
        let v = session_value(b"secret", t + 60);
        assert!(session_valid(b"secret", &v, t));
        assert!(!session_valid(b"secret", &v.to_ascii_uppercase(), t));
    }

    #[test]
    fn origins_compare_normalized() {
        assert_eq!(
            normalize_origin("HTTPS://Faderz.Example:443/"),
            "https://faderz.example"
        );
        assert_eq!(normalize_origin("http://a.example:80"), "http://a.example");
        assert_eq!(
            normalize_origin("http://a.example:443"),
            "http://a.example:443"
        );
        assert_eq!(
            normalize_origin("https://a.example:8443"),
            "https://a.example:8443"
        );
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_and_needs_a_token() {
        assert_eq!(bearer_token(b"Bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_token(b"bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_token(b"Bearer "), None);
        assert_eq!(bearer_token(b"Basic abcdef"), None);
    }
}
