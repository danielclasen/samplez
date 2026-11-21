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

/// [`Connections::apply_timeout`] unless the product says otherwise.
pub const DEFAULT_APPLY_TIMEOUT: Duration = Duration::from_secs(10);

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

/// How an IS-05 activation was asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationMode {
    Immediate,
    ScheduledAbsolute,
    ScheduledRelative,
}

/// An IS-05 activation as `/active` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activation {
    pub mode: ActivationMode,
    /// `requested_time` as the controller sent it, `None` for an
    /// immediate activation.
    pub requested: Option<String>,
    /// `activation_time`.
    pub time: Tai,
}

/// A port's change as a product replicates it to every instance, for
/// [`crate::Nmos::replicated_change`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replicated {
    pub active: Active,
    /// The activation behind it, from [`crate::Nmos::activation`]; `None`
    /// for a change the product made itself, which leaves the activation
    /// `/active` shows as it is.
    pub activation: Option<Activation>,
}

/// What [`crate::Nmos::replicated_change`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamped {
    /// The port's version now: what the product keeps for
    /// [`crate::Nmos::with_versions`] and [`crate::Nmos::set_versions`],
    /// so an instance restored from it serves what its peers serve.
    pub version: Tai,
    /// The change's generation, as [`crate::Nmos::external_change`]
    /// returns it; for a served activation the port's last one.
    pub generation: u64,
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

    /// How long an `apply` may take before it counts as failed: the port
    /// records the failure and hands the product its next activation.
    fn apply_timeout(&self) -> Duration {
        DEFAULT_APPLY_TIMEOUT
    }

    /// Applies an activation. The calls for one port come one at a time,
    /// each once the one before resolved, and each supersedes every earlier
    /// one, also one for the same `at`; `generation` grows with every call
    /// on the port and with every [`crate::Nmos::external_change`], and a
    /// call whose generation is below the last one the product saw is
    /// stale.
    ///
    /// `at` is the activation time of a scheduled activation, `None` for
    /// an immediate one. An immediate activation shows in `/active` before
    /// the call; a scheduled one is applied [`Connections::lead`] before
    /// `at` and shows in `/active` at `at`. A cancel that arrives after
    /// that call is a second call with the connection `/active` still
    /// shows and the same `at`; one that arrives before it means no call.
    ///
    /// Resolves once the product has taken the activation on or failed; a
    /// failure, a panic or no answer within [`Connections::apply_timeout`]
    /// turns the port's `master_enable` false in `/active`.
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
    /// How long after its activation time the last scheduled activation
    /// was written to `/active`; a time already past when it was asked for
    /// counts.
    pub late: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NmosStatus {
    pub ports: Vec<PortStatus>,
    /// The Registration API the node is registered with.
    pub registry: Option<String>,
}
