//! The `listeners`, `auth` and `api` sections of a product's config file.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use serde::Deserialize;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub bind: IpAddr,
    pub port: u16,
}

fn default_api() -> Listener {
    Listener {
        bind: Ipv4Addr::LOCALHOST.into(),
        port: 8080,
    }
}

fn default_ops() -> Listener {
    Listener {
        bind: Ipv4Addr::UNSPECIFIED.into(),
        port: 9090,
    }
}

fn default_nmos() -> Listener {
    Listener {
        bind: Ipv4Addr::LOCALHOST.into(),
        port: 8081,
    }
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

/// A secret mounted as a file. Trailing whitespace is not part of it.
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

fn default_user_header() -> String {
    "X-Auth-Request-User".into()
}

fn default_groups_header() -> String {
    "X-Auth-Request-Groups".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiConfig {
    /// Host names accepted in `Host` on requests without a valid bearer,
    /// in addition to `localhost` and `127.0.0.1`.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    /// `Origin` values accepted on WebSocket upgrades, in addition to
    /// `http://localhost:<api port>` and `http://127.0.0.1:<api port>`.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}
