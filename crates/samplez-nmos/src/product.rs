//! What the product implements and what it reads back: the hooks the
//! Connection API calls to check and apply activations, the store of the
//! last `/active` per port, and the status of each port.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use uuid::Uuid;

use crate::tai::Tai;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A sender or receiver, by its index in [`crate::NodeModel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Port {
    Sender(usize),
    Receiver(usize),
}

/// A sender or receiver by its key, stable across restarts and model
/// changes, for [`Store`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PortKey {
    Sender(String),
    Receiver(String),
}

/// A port's active connection, as `/active` shows it with every `auto`
/// resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Active {
    pub master_enable: bool,
    /// `receiver_id` on a sender, `sender_id` on a receiver.
    pub peer: Option<Uuid>,
    pub domain: Option<Uuid>,
    pub flow: Option<Uuid>,
}

/// What a port's domain holds under a flow id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSeen {
    /// No such flow; a receiver reads silence until it appears.
    Absent,
    Present {
        channels: u32,
        /// Samples per second.
        rate: u32,
        /// 32-bit float samples.
        float32: bool,
    },
}

/// Why a flow cannot be checked or connected; answered with 400.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject(pub String);

/// Why the product could not take an activation on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fail(pub String);

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Fail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The product side of IS-05. Called on the runtime that serves the
/// router, never on a real-time thread.
pub trait Connections: Send + Sync + 'static {
    /// What the port's domain holds for `flow`, for the check of a
    /// receiver's staged flow against its caps. A PATCH that waits longer
    /// than [`crate::RESOLVE_TIMEOUT`] for it is refused with 500.
    fn resolve(&self, port: Port, flow: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>>;

    /// How long before a scheduled activation time `apply` has to be
    /// called for the change to land on that time.
    fn lead(&self) -> Duration;

    /// Applies an activation. The calls for one port come one at a time,
    /// each once the one before resolved, and each supersedes every earlier
    /// one, also one for the same `at`; `generation` grows with every call
    /// on the port.
    ///
    /// `at` is the activation time of a scheduled activation, `None` for
    /// an immediate one. An immediate activation shows in `/active` before
    /// the call; a scheduled one is applied [`Connections::lead`] before
    /// `at` and shows in `/active` at `at`. A cancel that arrives after
    /// that call is a second call with the connection `/active` still
    /// shows and the same `at`.
    ///
    /// Resolves once the product has taken the activation on or failed; a
    /// failure or a panic turns the port's `master_enable` false in
    /// `/active`.
    fn apply(
        &self,
        port: Port,
        active: Active,
        at: Option<Tai>,
        generation: u64,
    ) -> BoxFuture<'_, Result<(), Fail>>;
}

/// The last `/active` of each port as the product persisted it, read once
/// at start; a port missing from it starts from the model.
pub trait Store: Send + Sync + 'static {
    fn load(&self) -> BTreeMap<PortKey, Active>;
}

/// Keeps nothing: every start begins from the model.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoStore;

impl Store for NoStore {
    fn load(&self) -> BTreeMap<PortKey, Active> {
        BTreeMap::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// Nothing activated since start.
    Initial,
    /// `apply` has not returned yet.
    Applying,
    Applied,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortStatus {
    pub port: Port,
    pub key: PortKey,
    pub active: Active,
    /// The time a scheduled activation fires at.
    pub pending: Option<Tai>,
    pub applied: Applied,
    /// Activations the product failed to apply since start.
    pub failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NmosStatus {
    pub ports: Vec<PortStatus>,
}
