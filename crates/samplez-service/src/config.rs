//! The `listeners`, `auth` and `api` sections of a product's config file.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::Role;

/// The sections of the config file this crate reads. A product embeds it
/// with `#[serde(flatten)]` or takes the three fields into its own struct.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub listeners: Listeners,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listeners {
    /// `/api/v1`, `/api/v1/ws` and the UI. Default 127.0.0.1:8080.
    #[serde(default = "default_api")]
    pub api: Listener,
    /// The bearer-only listener for machine clients; `proxy` mode only.
    #[serde(default)]
    pub machine: Option<Listener>,
    /// `/healthz`, `/readyz`, `/metrics`. Default 0.0.0.0:9090.
    #[serde(default = "default_ops")]
    pub ops: Listener,
    /// `/x-nmos/`, served from the router the product passes. Default
    /// 127.0.0.1:8081.
    #[serde(default = "default_nmos")]
    pub nmos: Listener,
}

impl Default for Listeners {
    fn default() -> Self {
        Listeners {
            api: default_api(),
            machine: None,
            ops: default_ops(),
            nmos: default_nmos(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub bind: IpAddr,
    pub port: u16,
    #[serde(default)]
    pub tls: Tls,
}

fn default_api() -> Listener {
    Listener {
        bind: Ipv4Addr::LOCALHOST.into(),
        port: 8080,
        tls: Tls::default(),
    }
}

fn default_ops() -> Listener {
    Listener {
        bind: Ipv4Addr::UNSPECIFIED.into(),
        port: 9090,
        tls: Tls::default(),
    }
}

fn default_nmos() -> Listener {
    Listener {
        bind: Ipv4Addr::LOCALHOST.into(),
        port: 8081,
        tls: Tls::default(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TlsMode {
    /// Plain HTTP, for a listener behind something that terminates TLS.
    #[default]
    None,
    /// `certFile` and `keyFile`, re-read every
    /// [`crate::server::SECRET_RELOAD_PERIOD`] and swapped in when their
    /// content changes.
    Files,
    /// A certificate for `sans` generated at start and lost at exit, valid
    /// for [`crate::tls::SELF_SIGNED_VALIDITY`] (a year) from an hour before
    /// it is generated. A new one replaces it at the first reload within
    /// [`crate::tls::SELF_SIGNED_RENEWAL`] (30 days) of its expiry. Its
    /// SHA-256 fingerprint and expiry are logged and in
    /// [`crate::ServiceStatus`].
    SelfSigned,
}

/// TLS on one listener. See [`crate::tls`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Tls {
    #[serde(default)]
    pub mode: TlsMode,
    /// PEM, the server certificate first, then its chain; `files` mode.
    pub cert_file: Option<PathBuf>,
    /// PEM, PKCS#8, PKCS#1 or SEC1; `files` mode.
    pub key_file: Option<PathBuf>,
    /// PEM CA certificates; when set, a client must present a certificate
    /// issued by one of them. Re-read with the certificate and key files; a
    /// change applies from the next handshake on.
    pub client_ca_file: Option<PathBuf>,
    /// DNS names and IP addresses of the generated certificate;
    /// `selfSigned` mode.
    #[serde(default)]
    pub sans: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    #[default]
    None,
    Token,
    Proxy,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub mode: AuthMode,
    /// The API bearer token; `token` mode.
    pub token: Option<SecretFile>,
    /// The machine listener's bearer token; `proxy` mode.
    pub machine_token: Option<SecretFile>,
    #[serde(default)]
    pub proxy: ProxyConfig,
}

/// A secret mounted as a file, re-read every
/// [`crate::server::SECRET_RELOAD_PERIOD`]. Trailing whitespace is not part
/// of it. A token is 32 random bytes or more, e.g. `openssl rand -hex 32`.
/// Kubernetes never updates a Secret mounted with `subPath`; mount the
/// Secret as a directory so a rotation reaches the file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretFile {
    pub file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProxyConfig {
    #[serde(default = "default_user_header")]
    pub user_header: String,
    /// Comma-separated group names.
    #[serde(default = "default_groups_header")]
    pub groups_header: String,
    /// The role each group grants; a user gets the highest of its groups.
    #[serde(default)]
    pub group_roles: BTreeMap<String, Role>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            user_header: default_user_header(),
            groups_header: default_groups_header(),
            group_roles: BTreeMap::new(),
        }
    }
}

// oauth2-proxy v7.15.x as a reverse proxy sends `X-Forwarded-User` and
// `X-Forwarded-Groups` upstream (`--pass-user-headers`, default true) and
// strips client-supplied copies (`--skip-auth-strip-headers`, default
// true); `X-Auth-Request-*` are response headers for nginx `auth_request`
// (`--set-xauthrequest`). Source:
// https://oauth2-proxy.github.io/oauth2-proxy/configuration/overview
fn default_user_header() -> String {
    "X-Forwarded-User".into()
}

fn default_groups_header() -> String {
    "X-Forwarded-Groups".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiConfig {
    /// Host names accepted in `Host` on requests without a valid bearer,
    /// in addition to `localhost` and `127.0.0.1`.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// `Origin` values accepted on WebSocket upgrades, in addition to
    /// `<scheme>://localhost:<api port>` and `<scheme>://127.0.0.1:<api
    /// port>`, the scheme being `https` when the API listener runs TLS.
    /// Compared without case, a trailing `/` or the scheme's default port.
    ///
    /// Behind a proxy, such as the sidecar in `proxy` mode or anything that
    /// terminates TLS in front of a plain listener, the browser's origin is
    /// the external one, e.g. `https://samplez.example`, which the defaults
    /// never match: WebSocket upgrades get 403 until it is listed here.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}
