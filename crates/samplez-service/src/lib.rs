//! The non-RT service layer of samplez products: the API, machine, ops and
//! NMOS listeners, the auth modes and browser protections on the API, the
//! health, readiness and metrics endpoints, the OpenAPI document and the
//! WebSocket send queues, and TLS on each listener.
//!
//! A product passes its replicated state, its `/api/v1` router, its UI
//! router, its WebSocket handler and its NMOS router to [`Service`], then
//! binds and serves it.

use std::fmt;
use std::io;
use std::path::PathBuf;

pub mod auth;
pub mod config;
pub mod metrics;
pub mod openapi;
pub mod ops;
pub mod replica;
pub mod server;
pub mod tls;
pub mod ws;

pub use auth::{Identity, Role};
pub use config::ServiceConfig;
pub use metrics::{Counter, Histogram, Metrics};
pub use ops::{Heartbeat, Ops};
pub use replica::{Owner, Position, ReplicaInfo, ReplicatedState};
pub use server::{
    Bound, Connect, Forbidden, Routers, SecretStatus, Service, ServiceStatus, StatusSource,
};
pub use tls::TlsStatus;

#[derive(Debug)]
pub enum Error {
    /// The settings contradict each other, a secret is empty or invalid, or
    /// generating a certificate or drawing random bits failed.
    Config(String),
    /// A secret file could not be read.
    Secret(PathBuf, io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(m) => write!(f, "{m}"),
            Error::Secret(path, e) => write!(f, "reading {}: {e}", path.display()),
        }
    }
}

impl std::error::Error for Error {}
