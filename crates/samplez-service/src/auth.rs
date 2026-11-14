//! Authentication and the browser protections on the API and machine
//! listeners.
//!
//! On the API listener in `token` mode and on the machine listener, a
//! request carrying `Authorization` is authenticated by bearer only: an
//! invalid one gets 401 and neither cookies nor identity headers are read.
//! `none` mode and the `proxy` mode API listener ignore `Authorization`.
//! Requests without a valid bearer must name an allowed `Host` and, when
//! they change state, carry `X-Samplez-Request: 1` with a JSON content
//! type. Every WebSocket upgrade with an `Origin` must name an allowed one;
//! one without is accepted only with a valid bearer or in `none` mode. The
//! same `Origin` rule holds for every request to the WebSocket path and for
//! every `CONNECT`.

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
use utoipa::ToSchema;

use crate::config::{AuthMode, ProxyConfig, ServiceConfig, TlsMode};
use crate::metrics::{Counter, Metrics};
use crate::{Error, ServiceStatus};

pub const SESSION_COOKIE: &str = "samplez_session";
pub const CSRF_HEADER: &str = "x-samplez-request";
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const LOGIN_PATH: &str = "/api/v1/login";
pub(crate) const WS_PATH: &str = "/api/v1/ws";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    /// The user named by the proxy; `None` in the other modes.
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
    value: RwLock<Vec<u8>>,
    /// Seconds since the Unix epoch of the last successful read.
    loaded_at: AtomicU64,
    failures: Counter,
}

impl Secret {
    fn load(name: &'static str, path: &Path, metrics: &Metrics) -> Result<Secret, Error> {
        Ok(Secret {
            name,
            path: path.to_owned(),
            value: RwLock::new(read_secret(path)?),
            loaded_at: AtomicU64::new(now()),
            failures: metrics.counter(
                "samplez_secret_reload_failures_total",
                "Failed re-reads of a secret file; the previous value stays in use.",
                &[("secret", name)],
            ),
        })
    }

    fn reload(&self) -> Result<(), Error> {
        let value = match read_secret(&self.path) {
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

fn read_secret(path: &Path) -> Result<Vec<u8>, Error> {
    let mut value = std::fs::read(path).map_err(|e| Error::Secret(path.to_owned(), e))?;
    value.truncate(value.trim_ascii_end().len());
    if value.is_empty() {
        return Err(Error::Config(format!("{} is empty", path.display())));
    }
    Ok(value)
}

pub(crate) struct Auth {
    mode: AuthMode,
    token: Option<Secret>,
    machine_token: Option<Secret>,
    proxy: ProxyConfig,
    hosts: Vec<String>,
    origins: Vec<String>,
    /// The API listener runs TLS, so session cookies are `Secure`.
    tls: bool,
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
            (AuthMode::Token, Some(t)) => Some(Secret::load("token", &t.file, metrics)?),
            (AuthMode::Token, None) => {
                return Err(Error::Config("token mode needs auth.token".into()));
            }
            _ => None,
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
        let origins = origins.iter().map(|o| normalize_origin(o)).collect();
        Ok(Auth {
            mode: auth.mode,
            token,
            machine_token,
            proxy: auth.proxy.clone(),
            hosts,
            origins,
            tls,
        })
    }

    fn secrets(&self) -> impl Iterator<Item = &Secret> {
        [&self.token, &self.machine_token].into_iter().flatten()
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

    /// `Ok(None)` lets the request through without an identity: the login
    /// route, which checks the token itself.
    fn check(&self, listener: Listener, req: &Request) -> Result<Option<Identity>, StatusCode> {
        let headers = req.headers();
        let bearer_secret = match (listener, self.mode) {
            (Listener::Machine, _) => self.machine_token.as_ref(),
            (Listener::Api, AuthMode::Token) => self.token.as_ref(),
            (Listener::Api, _) => None,
        };
        let bearer = match bearer_secret {
            Some(secret) => match single(headers, header::AUTHORIZATION.as_str())? {
                Some(value) => match bearer_token(value) {
                    Some(t) if secret.matches(t) => true,
                    _ => return Err(StatusCode::UNAUTHORIZED),
                },
                None if listener == Listener::Machine => return Err(StatusCode::UNAUTHORIZED),
                None => false,
            },
            None => false,
        };

        if !bearer && !self.host_allowed(req) {
            return Err(StatusCode::FORBIDDEN);
        }
        if is_upgrade(headers) || req.method() == Method::CONNECT || req.uri().path() == WS_PATH {
            match headers.get(header::ORIGIN) {
                Some(o) if !self.origin_allowed(o.as_bytes()) => {
                    return Err(StatusCode::FORBIDDEN);
                }
                None if !bearer && self.mode != AuthMode::None => {
                    return Err(StatusCode::FORBIDDEN);
                }
                _ => {}
            }
        }
        let writes = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
        if !bearer && writes && !csrf_ok(headers) {
            return Err(StatusCode::FORBIDDEN);
        }

        let operator = Identity {
            user: None,
            role: Role::Operator,
        };
        let identity = if bearer {
            operator
        } else {
            match self.mode {
                AuthMode::None => operator,
                AuthMode::Token if req.uri().path() == LOGIN_PATH => return Ok(None),
                AuthMode::Token => {
                    let token = self.token.as_ref().expect("token mode has a token");
                    let key = token.get();
                    if session_cookies(headers).any(|c| session_valid(&key, c, now())) {
                        operator
                    } else {
                        return Err(StatusCode::UNAUTHORIZED);
                    }
                }
                AuthMode::Proxy => self.proxy_identity(headers)?,
            }
        };
        if writes && identity.role == Role::Viewer {
            return Err(StatusCode::FORBIDDEN);
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

    fn proxy_identity(&self, headers: &HeaderMap) -> Result<Identity, StatusCode> {
        let user = single(headers, &self.proxy.user_header)?
            .and_then(|v| std::str::from_utf8(v).ok())
            .filter(|u| !u.is_empty())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let role = single(headers, &self.proxy.groups_header)?
            .and_then(|v| std::str::from_utf8(v).ok())
            .into_iter()
            .flat_map(|v| v.split(','))
            .filter_map(|g| self.proxy.group_roles.get(g.trim()))
            .max()
            .copied()
            .ok_or(StatusCode::FORBIDDEN)?;
        Ok(Identity {
            user: Some(user.to_owned()),
            role,
        })
    }
}

/// The value of a header that may appear at most once; a repeated one is
/// refused, since the parts in front of the service may disagree on which
/// copy counts.
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a [u8]>, StatusCode> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(StatusCode::UNAUTHORIZED);
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

/// Every `samplez_session` value: a browser sends one per matching path
/// and domain, and only one of them may be current.
fn session_cookies(headers: &HeaderMap) -> impl Iterator<Item = &str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|c| c.trim().strip_prefix(SESSION_COOKIE)?.strip_prefix('='))
}

pub(crate) fn now() -> u64 {
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

fn session_valid(token: &[u8], value: &str, now: u64) -> bool {
    let Some((expiry, hex)) = value.split_once('.') else {
        return false;
    };
    if expiry.is_empty() || !expiry.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Ok(expiry) = expiry.parse::<u64>() else {
        return false;
    };
    if expiry <= now || expiry > now + SESSION_TTL.as_secs() || hex.len() != 64 {
        return false;
    }
    let tag: Option<Vec<u8>> = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect();
    tag.is_some_and(|t| session_mac(token, expiry).verify_slice(&t).is_ok())
}

pub(crate) async fn middleware(
    State((auth, listener)): State<(Arc<Auth>, Listener)>,
    mut req: Request,
    next: Next,
) -> Response {
    match auth.check(listener, &req) {
        Ok(identity) => {
            if let Some(identity) = identity {
                req.extensions_mut().insert(identity);
            }
            next.run(req).await
        }
        Err(StatusCode::UNAUTHORIZED) => (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
        )
            .into_response(),
        Err(status) => status.into_response(),
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
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // A chain of proxies appends its schemes; the first is the client's.
    let secure = auth.tls
        || headers.get("x-forwarded-proto").is_some_and(|p| {
            let first = p
                .as_bytes()
                .split(|&b| b == b',')
                .next()
                .unwrap_or_default();
            first.trim_ascii().eq_ignore_ascii_case(b"https")
        });
    let cookie = format!(
        "{SESSION_COOKIE}={}; Path=/api/v1; Max-Age={}; HttpOnly; SameSite=Strict{}",
        session_value(&token.get(), now() + SESSION_TTL.as_secs()),
        SESSION_TTL.as_secs(),
        if secure { "; Secure" } else { "" },
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
