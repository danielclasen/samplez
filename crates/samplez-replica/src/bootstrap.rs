//! How a replica starts: from its volume when that holds a log, else by
//! the marker ConfigMap `<release>-replica-cluster`, which holds the
//! cluster id and ordinal 0's volume id.
//!
//! Ordinal 0 initialises a set only after creating the marker itself, or,
//! after a crash between the two, when the marker names its own volume,
//! and in both cases only while no other replica answers as a member of a
//! set and a majority of the wanted voters answered that they are not, so
//! a cold start needs that majority running at once; never because its
//! volume is empty. Every other empty volume joins
//! as a learner of the set the marker names, ordinal 0's after a volume
//! loss included. The cluster id is the epoch: drawn with each new marker,
//! so a state initialised empty starts a new history, and kept on the
//! volume, so a restart, a snapshot install and a resync keep it.
//!
//! Created by a [`Kube`] given the audio StatefulSet ([`Kube::owned_by`]),
//! the marker carries an owner reference to that StatefulSet, and
//! Kubernetes' garbage collector deletes it after the StatefulSet. That
//! happens in the background: a StatefulSet created again at once can
//! read the old marker first, its empty volumes joining the old cluster id
//! until the marker is gone and ordinal 0 creates a new one. A deletion
//! with `--cascade=orphan` removes the owner reference and keeps the
//! marker.
//!
//! The leader of a running set recreates a marker it finds missing, under
//! the set's cluster id and volume 0 (`membership`), so a StatefulSet
//! created again on the volumes it kept goes on as the same set. An owned
//! marker is created only for the StatefulSet the process first read, and
//! not while it is being deleted: a leader of a deleted set does not write
//! its cluster id into the marker of one created again under its name. With no set running, an
//! empty volume other than ordinal 0's waits for a marker; one created by
//! hand with `data.cluster` from a member volume's `cluster` file and
//! `data.volume` 0 lets it join.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use hyper::{Method, StatusCode};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};

use crate::entry::NodeId;
use crate::membership::{Slot, node_id};
use crate::net;

/// How long a replica waits between attempts while the API server cannot
/// be reached or ordinal 0 has not created the marker yet.
pub const RETRY: Duration = Duration::from_secs(1);

/// What the marker holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marker {
    pub cluster: u64,
    /// Ordinal 0's volume id.
    pub volume: u64,
}

/// The marker's store: the Kubernetes API ([`Kube`]) or a test's.
pub trait Markers: Send + Sync {
    /// The marker's name, for the status.
    fn name(&self) -> &str;
    fn get(&self) -> BoxFuture<'_, Result<Option<Marker>, String>>;
    /// `false` when a marker exists already; it is left as it is.
    fn create(&self, marker: Marker) -> BoxFuture<'_, Result<bool, String>>;
}

/// Draws a non-zero id from the system's random source.
pub fn random_id() -> io::Result<u64> {
    loop {
        let id = u64::from_be_bytes(random()?);
        if id != 0 {
            return Ok(id);
        }
    }
}

fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut bytes)
        .map_err(|_| io::Error::other("no random bits"))?;
    Ok(bytes)
}

/// A volume id: the seconds since the Unix epoch it was created at in the
/// high 32 of its 48 bits, 16 random bits below, so a newer volume of a
/// slot has the higher id ([`crate::membership::plan`]) while clocks are
/// within a second of each other.
pub fn volume_id(created: u64, random: u16) -> u64 {
    (created & 0xffff_ffff) << 16 | u64::from(random)
}

/// The ids a volume holds beside the log: its own, drawn when it is first
/// used, and the cluster id of the set it belongs to.
pub struct Volume {
    dir: PathBuf,
    pub id: u64,
}

impl Volume {
    pub fn open(dir: &Path) -> io::Result<Volume> {
        fs::create_dir_all(dir)?;
        let id = match read_id(&dir.join("volume"))? {
            Some(id) => id,
            None => {
                let created = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_secs();
                let id = volume_id(created, u16::from_be_bytes(random()?));
                write_id(dir, "volume", id)?;
                id
            }
        };
        Ok(Volume {
            dir: dir.to_owned(),
            id,
        })
    }

    pub fn cluster(&self) -> io::Result<Option<u64>> {
        read_id(&self.dir.join("cluster"))
    }

    pub fn set_cluster(&self, cluster: u64) -> io::Result<()> {
        write_id(&self.dir, "cluster", cluster)
    }
}

fn read_id(path: &Path) -> io::Result<Option<u64>> {
    match fs::read_to_string(path) {
        Ok(text) => text.trim().parse().map(Some).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {e}", path.display()),
            )
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Written to a temporary file, fsynced and renamed into place.
fn write_id(dir: &Path, name: &str, id: u64) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut file = fs::File::create(&tmp)?;
    writeln!(file, "{id}")?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(name))?;
    fs::File::open(dir)?.sync_all()
}

/// What a replica does once started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// Its volume holds a log: go on from it.
    Resume,
    /// Its volume is empty: start or join the set the marker names
    /// ([`step`]).
    Bootstrap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub id: NodeId,
    /// The cluster id the volume holds; 0 for an empty volume that has not
    /// read a marker yet.
    pub epoch: u64,
    pub plan: Plan,
}

/// How the replica in `slot` on `volume` starts. `pristine` is whether its
/// store holds nothing (no vote, log or snapshot). Reads nothing but the
/// volume, so a replica with a log starts without the API server.
pub fn prepare(volume: &Volume, slot: Slot, pristine: bool) -> io::Result<Prepared> {
    let id = node_id(slot, volume.id);
    let cluster = volume.cluster()?;
    if pristine {
        return Ok(Prepared {
            id,
            epoch: cluster.unwrap_or(0),
            plan: Plan::Bootstrap,
        });
    }
    let epoch = cluster.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the volume holds a log but no cluster id",
        )
    })?;
    Ok(Prepared {
        id,
        epoch,
        plan: Plan::Resume,
    })
}

/// What an empty volume does next, from the marker it read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Not ordinal 0 and no marker yet.
    Wait,
    /// Ordinal 0 and no marker: create one naming its volume, then
    /// initialise, unless another replica answers as a member.
    Create,
    /// Ordinal 0 under a marker naming its own volume (a crash between the
    /// marker and the initialisation): initialise, unless another replica
    /// answers as a member.
    Initialize { cluster: u64 },
    /// Join the set `cluster` as a learner.
    Join { cluster: u64 },
}

pub fn step(slot: Slot, volume: u64, marker: Option<Marker>) -> Step {
    match marker {
        None if slot == Slot::Audio(0) => Step::Create,
        None => Step::Wait,
        Some(marker) if slot == Slot::Audio(0) && marker.volume == volume => Step::Initialize {
            cluster: marker.cluster,
        },
        Some(marker) => Step::Join {
            cluster: marker.cluster,
        },
    }
}

/// What a replica with an empty volume needs to start or join a set.
pub struct Bootstrap {
    pub markers: Arc<dyn Markers>,
    pub volume: Volume,
    pub slot: Slot,
}

/// The marker in the Kubernetes API: create and get on one ConfigMap.
pub struct Kube {
    /// `host:port` of the API server.
    addr: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    token: Option<PathBuf>,
    namespace: String,
    name: String,
    /// The StatefulSet the marker is created owned by.
    owner: Option<String>,
    /// That StatefulSet's uid as this process first read it.
    owner_uid: OnceLock<String>,
}

const SERVICE_ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl Kube {
    /// The API server at `addr` (`host:port`), over TLS trusting `ca` (PEM)
    /// when given, authenticated by the bearer token in `token`, re-read
    /// for every request since a projected token is rotated. A token
    /// needs `ca`: it is never sent in plain text.
    pub fn new(
        addr: String,
        ca: Option<&[u8]>,
        token: Option<PathBuf>,
        namespace: String,
        name: String,
    ) -> io::Result<Kube> {
        if token.is_some() && ca.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a bearer token is sent over TLS only",
            ));
        }
        let tls = match ca {
            Some(pem) => {
                let mut roots = RootCertStore::empty();
                for cert in CertificateDer::pem_slice_iter(pem) {
                    roots
                        .add(cert.map_err(io::Error::other)?)
                        .map_err(io::Error::other)?;
                }
                let provider = Arc::new(rustls::crypto::ring::default_provider());
                let config = rustls::ClientConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .map_err(io::Error::other)?
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                Some(Arc::new(config))
            }
            None => None,
        };
        Ok(Kube {
            addr,
            tls,
            token,
            namespace,
            name,
            owner: None,
            owner_uid: OnceLock::new(),
        })
    }

    /// Creates the marker owned by the StatefulSet `statefulset` in its
    /// namespace, the one [`crate::rollout::follow`] reads, so Kubernetes'
    /// garbage collector deletes the marker after it. Its uid is read with
    /// a GET on the StatefulSet at the create; the reference is no
    /// controller and does not block the StatefulSet's deletion. The uid of
    /// this process's first read of it, by a create or
    /// [`Kube::update_revision`], is kept: a create refuses a StatefulSet
    /// created again under the name since, or one being deleted, so a
    /// replica of a deleted set never writes its cluster id into the
    /// marker of the set that replaces it.
    pub fn owned_by(mut self, statefulset: String) -> Kube {
        self.owner = Some(statefulset);
        self
    }

    /// The pod's own API server, service account and namespace, for the
    /// marker `name`.
    pub fn in_cluster(name: String) -> io::Result<Kube> {
        let env =
            |key: &str| std::env::var(key).map_err(|e| io::Error::other(format!("{key}: {e}")));
        let host = env("KUBERNETES_SERVICE_HOST")?;
        let port = env("KUBERNETES_SERVICE_PORT")?;
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host
        };
        let account = Path::new(SERVICE_ACCOUNT);
        let ca = fs::read(account.join("ca.crt"))?;
        let namespace = fs::read_to_string(account.join("namespace"))?;
        Kube::new(
            format!("{host}:{port}"),
            Some(&ca),
            Some(account.join("token")),
            namespace.trim().to_owned(),
            name,
        )
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<(StatusCode, Vec<u8>), String> {
        let exchange = async {
            let mut sender = net::connect(&self.addr, self.tls.clone()).await?;
            let mut request = net::request(method, &self.addr, path, body);
            if let Some(token) = &self.token {
                let token = fs::read_to_string(token)?;
                let value = format!("Bearer {}", token.trim())
                    .parse()
                    .map_err(io::Error::other)?;
                request
                    .headers_mut()
                    .insert(hyper::header::AUTHORIZATION, value);
            }
            net::send(&mut sender, request).await
        };
        match tokio::time::timeout(RETRY * 5, exchange).await {
            Ok(Ok((status, body))) => Ok((status, body.to_vec())),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("timed out".into()),
        }
    }

    fn path(&self) -> String {
        format!("/api/v1/namespaces/{}/configmaps", self.namespace)
    }

    /// The StatefulSet `name` in the marker's namespace.
    async fn statefulset(&self, name: &str) -> Result<StatefulSet, String> {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/statefulsets/{name}",
            self.namespace
        );
        let (status, body) = self.call(Method::GET, &path, Vec::new()).await?;
        if status != StatusCode::OK {
            return Err(format!("GET {path}: {status}"));
        }
        let set: StatefulSet =
            serde_json::from_slice(&body).map_err(|e| format!("StatefulSet {name}: {e}"))?;
        if self.owner.as_deref() == Some(name) {
            let _ = self.owner_uid.set(set.metadata.uid.clone());
        }
        Ok(set)
    }

    /// `status.updateRevision` of the StatefulSet `name` in the marker's
    /// namespace: the revision a rollout moves its pods to. None while
    /// the StatefulSet reports none.
    pub async fn update_revision(&self, name: &str) -> Result<Option<String>, String> {
        let set = self.statefulset(name).await?;
        Ok(set.status.and_then(|s| s.update_revision))
    }
}

#[derive(Deserialize)]
struct StatefulSet {
    metadata: SetMetadata,
    status: Option<SetStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetMetadata {
    uid: String,
    deletion_timestamp: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetStatus {
    update_revision: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Data {
    cluster: String,
    volume: String,
}

#[derive(Deserialize)]
struct ConfigMap {
    data: Option<Data>,
}

impl Markers for Kube {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> BoxFuture<'_, Result<Option<Marker>, String>> {
        Box::pin(async move {
            let path = format!("{}/{}", self.path(), self.name);
            let (status, body) = self.call(Method::GET, &path, Vec::new()).await?;
            match status {
                StatusCode::OK => {}
                StatusCode::NOT_FOUND => return Ok(None),
                status => return Err(format!("GET {path}: {status}")),
            }
            let invalid = |e: &dyn std::fmt::Display| format!("ConfigMap {}: {e}", self.name);
            let config_map: ConfigMap = serde_json::from_slice(&body).map_err(|e| invalid(&e))?;
            let data = config_map.data.ok_or_else(|| invalid(&"no data"))?;
            Ok(Some(Marker {
                cluster: data.cluster.parse().map_err(|e| invalid(&e))?,
                volume: data.volume.parse().map_err(|e| invalid(&e))?,
            }))
        })
    }

    fn create(&self, marker: Marker) -> BoxFuture<'_, Result<bool, String>> {
        Box::pin(async move {
            let mut metadata = serde_json::json!({ "name": self.name });
            if let Some(owner) = &self.owner {
                let set = self.statefulset(owner).await?;
                if set.metadata.deletion_timestamp.is_some() {
                    return Err(format!("StatefulSet {owner} is being deleted"));
                }
                let first = self.owner_uid.get().expect("set by the read");
                if *first != set.metadata.uid {
                    return Err(format!(
                        "StatefulSet {owner} was replaced: uid {}, first read as {first}",
                        set.metadata.uid
                    ));
                }
                metadata["ownerReferences"] = serde_json::json!([{
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "name": owner,
                    "uid": set.metadata.uid,
                    "controller": false,
                }]);
            }
            let body = serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": metadata,
                "data": Data {
                    cluster: marker.cluster.to_string(),
                    volume: marker.volume.to_string(),
                },
            });
            let body = serde_json::to_vec(&body).expect("encodes");
            let (status, _) = self.call(Method::POST, &self.path(), body).await?;
            match status {
                StatusCode::CREATED | StatusCode::OK => Ok(true),
                // AlreadyExists is the only conflict a create returns.
                StatusCode::CONFLICT => Ok(false),
                status => Err(format!("POST {}: {status}", self.path())),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_newer_volume_has_the_higher_id() {
        assert!(volume_id(1_700_000_001, 0) > volume_id(1_700_000_000, 0xffff));
        let id = volume_id(1_700_000_000, 7);
        assert_eq!(id >> 48, 0, "48 bits");
        assert_ne!(id, 0);
    }

    #[test]
    fn the_marker_decides_what_an_empty_volume_does() {
        let marker = |volume| Some(Marker { cluster: 9, volume });
        assert_eq!(step(Slot::Audio(0), 1, None), Step::Create);
        assert_eq!(step(Slot::Audio(1), 1, None), Step::Wait);
        assert_eq!(step(Slot::Witness, 1, None), Step::Wait);
        assert_eq!(
            step(Slot::Audio(0), 1, marker(1)),
            Step::Initialize { cluster: 9 }
        );
        assert_eq!(
            step(Slot::Audio(0), 1, marker(2)),
            Step::Join { cluster: 9 }
        );
        assert_eq!(
            step(Slot::Audio(1), 1, marker(1)),
            Step::Join { cluster: 9 }
        );
    }

    #[test]
    fn a_token_is_not_sent_in_plain_text() {
        let kube = |token: Option<PathBuf>| {
            Kube::new("127.0.0.1:1".into(), None, token, "ns".into(), "m".into())
        };
        assert!(kube(Some("/token".into())).is_err());
        assert!(kube(None).is_ok());
    }
}
