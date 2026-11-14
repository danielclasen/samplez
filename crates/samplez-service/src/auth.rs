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
//! one without is accepted only with a valid bearer or in `none` mode.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use utoipa::ToSchema;

use crate::Error;
use crate::config::{AuthMode, ProxyConfig, ServiceConfig};

pub const SESSION_COOKIE: &str = "samplez_session";
pub const CSRF_HEADER: &str = "x-samplez-request";
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const LOGIN_PATH: &str = "/api/v1/login";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
}

/// Inserted as a request extension on every authenticated request; a
/// viewer's state-changing requests are refused before they reach a route.
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
    path: PathBuf,
    value: RwLock<Vec<u8>>,
}

impl Secret {
    fn load(path: &Path) -> Result<Secret, Error> {
        Ok(Secret {
            path: path.to_owned(),
            value: RwLock::new(read_secret(path)?),
        })
    }

    fn reload(&self) -> Result<(), Error> {
        let value = read_secret(&self.path)?;
        *self.value.write().expect("secret lock poisoned") = value;
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
}

impl Auth {
    pub(crate) fn new(config: &ServiceConfig) -> Result<Auth, Error> {
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
        let token = match (auth.mode, &auth.token) {
            (AuthMode::Token, Some(t)) => Some(Secret::load(&t.file)?),
            (AuthMode::Token, None) => {
                return Err(Error::Config("token mode needs auth.token".into()));
            }
            _ => None,
        };
        let machine_token = match (&listeners.machine, &auth.machine_token) {
            (Some(_), Some(t)) => Some(Secret::load(&t.file)?),
            _ => None,
        };
        let port = listeners.api.port;
        let mut hosts = vec!["localhost".to_owned(), "127.0.0.1".to_owned()];
        hosts.extend(
            config
                .api
                .allowed_hosts
                .iter()
                .map(|h| h.to_ascii_lowercase()),
        );
        let mut origins = vec![
            format!("http://localhost:{port}"),
            format!("http://127.0.0.1:{port}"),
        ];
        origins.extend(config.api.allowed_origins.iter().cloned());
        Ok(Auth {
            mode: auth.mode,
            token,
            machine_token,
            proxy: auth.proxy.clone(),
            hosts,
            origins,
        })
    }

    /// Re-reads the token files. A failed read keeps the previous value.
    pub(crate) fn reload(&self) -> Result<(), Error> {
        for secret in [&self.token, &self.machine_token].into_iter().flatten() {
            secret.reload()?;
        }
        Ok(())
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
            Some(secret) => match headers.get(header::AUTHORIZATION) {
                Some(value) => match bearer_token(value.as_bytes()) {
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
        if is_upgrade(headers) {
            match headers.get(header::ORIGIN) {
                Some(o) if !self.origins.iter().any(|a| a.as_bytes() == o.as_bytes()) => {
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
                    match session_cookie(headers) {
                        Some(c) if session_valid(&token.get(), c, now()) => operator,
                        _ => return Err(StatusCode::UNAUTHORIZED),
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

    fn host_allowed(&self, req: &Request) -> bool {
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
        let user = headers
            .get(self.proxy.user_header.as_str())
            .and_then(|v| v.to_str().ok())
            .filter(|u| !u.is_empty())
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let role = headers
            .get_all(self.proxy.groups_header.as_str())
            .iter()
            .filter_map(|v| v.to_str().ok())
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

fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|c| c.trim().strip_prefix(SESSION_COOKIE)?.strip_prefix('='))
}

fn now() -> u64 {
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
    let secure = headers
        .get("x-forwarded-proto")
        .is_some_and(|p| p.as_bytes().eq_ignore_ascii_case(b"https"));
    let cookie = format!(
        "{SESSION_COOKIE}={}; Path=/api/v1; Max-Age={}; HttpOnly; SameSite=Strict{}",
        session_value(&token.get(), now() + SESSION_TTL.as_secs()),
        SESSION_TTL.as_secs(),
        if secure { "; Secure" } else { "" },
    );
    (StatusCode::NO_CONTENT, [(header::SET_COOKIE, cookie)]).into_response()
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
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_and_needs_a_token() {
        assert_eq!(bearer_token(b"Bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_token(b"bearer abc"), Some(&b"abc"[..]));
        assert_eq!(bearer_token(b"Bearer "), None);
        assert_eq!(bearer_token(b"Basic abcdef"), None);
    }
}
