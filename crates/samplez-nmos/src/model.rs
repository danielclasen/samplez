//! What a product registers: one Node with one Device, a Source, Flow and
//! Sender per MXL writer and a Receiver per MXL reader.

use std::time::Duration;

use uuid::Uuid;

/// Every Flow and Receiver carries 32-bit float audio at this rate.
pub const SAMPLE_RATE: u32 = 48_000;
pub const MEDIA_TYPE: &str = "audio/float32";
pub const BIT_DEPTH: u32 = 32;
/// The most channels a receiver's caps admit.
pub const MAX_CHANNELS: u32 = 64;

#[derive(Debug, Clone)]
pub struct NodeModel {
    /// Label and description of the Node and the Device.
    pub label: String,
    pub description: String,
    pub senders: Vec<SenderDesc>,
    pub receivers: Vec<ReceiverDesc>,
}

/// One MXL writer: a Source, a Flow and a Sender.
#[derive(Debug, Clone)]
pub struct SenderDesc {
    /// Unique among the senders; part of the derived ids.
    pub key: String,
    pub label: String,
    pub channels: Vec<ChannelDesc>,
    /// The BCP-002-01 group; the role is `Audio out`.
    pub group: String,
    /// The MXL domain the writer writes to.
    pub domain: Uuid,
    /// The MXL flow the writer writes.
    pub flow: Uuid,
}

#[derive(Debug, Clone)]
pub struct ChannelDesc {
    pub label: String,
    /// A VSF TR-03 symbol (`L`, `R`, `C`, ..., `NSC001`, `U01`); `None` is
    /// `U01` for the first channel, `U02` for the second and so on, and no
    /// symbol past the 64th.
    pub symbol: Option<String>,
}

/// One MXL reader.
#[derive(Debug, Clone)]
pub struct ReceiverDesc {
    /// Unique among the receivers; part of the derived ids.
    pub key: String,
    pub label: String,
    /// The channel counts the reader takes, the BCP-004-01
    /// `channel_count` constraint.
    pub channels: ChannelCount,
    /// The BCP-002-01 group; the role is `Audio in`.
    pub group: String,
    /// The MXL domain the reader reads from, the only one it accepts.
    pub domain: Uuid,
    /// The flow read at start, `None` for an unconnected reader.
    pub flow: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelCount {
    Exactly(u32),
    /// From `min` to [`MAX_CHANNELS`].
    AtLeast(u32),
}

/// BCP-002-02 asset tags of the Node and the Device.
#[derive(Debug, Clone)]
pub struct Asset {
    pub manufacturer: String,
    pub product: String,
    pub instance_id: String,
    /// The Device's functions, at least one.
    pub functions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NmosConfig {
    /// Every resource id derives from it; the same seed gives the same ids
    /// across restarts.
    pub seed: String,
    /// The host and port controllers reach the Node and Connection APIs
    /// at, as written into the Node and Device.
    pub advertise_host: String,
    pub advertise_port: u16,
    pub asset: Asset,
    /// How often the Node heartbeats its registry; also the timeout of
    /// each request to it.
    pub heartbeat: Duration,
    /// Registration APIs to use, `http://host:port`, tried in this order;
    /// none browses DNS-SD. The registration client speaks plain HTTP
    /// only, so `https://` is refused.
    pub registries: Vec<String>,
    /// The unicast DNS-SD domain to browse; `None` browses each search
    /// domain of the system resolver.
    pub dns_sd_domain: Option<String>,
    /// Whether this process continues a registration that a registry
    /// still holds for its Node when it first registers. On, it heartbeats
    /// first; when the registry holds the Node, every resource gets a new
    /// version and is posted, and the Node is not deleted. Off, a 200 on
    /// the first Node POST deletes the Node and registers it anew, as IS-04
    /// asks of a restarted Node. On is for processes that take turns under
    /// one Node id, such as a replica set's leaders, so a hand-over keeps
    /// the Node registered. It assumes one process registers the Node at a
    /// time, which the application ensures, and that each presents the
    /// same resource ids: one the predecessor registered and this process
    /// lacks stays in the registry, which offers no listing to find it.
    /// When the registry refuses any resource of a continued registration,
    /// as one whose new version is still older than the held one after a
    /// clock skew between hosts, the Node is deleted and every resource
    /// registered anew.
    pub continue_held: bool,
}
