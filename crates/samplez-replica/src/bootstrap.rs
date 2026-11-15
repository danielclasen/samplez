//! How a replica starts: from its volume when that holds a log, else by
//! the marker ConfigMap `<release>-replica-cluster`, which holds the
//! cluster id and ordinal 0's volume id.
//!
//! Ordinal 0 initialises a set only after creating the marker itself, or,
//! after a crash between the two, when the marker names its own volume;
//! never because its volume is empty. Every other empty volume joins as a
//! learner of the set the marker names, ordinal 0's after a volume loss
//! included. The cluster id is the epoch: drawn with each new marker, so a
//! state initialised empty starts a new history, and kept on the volume, so
//! a restart, a snapshot install and a resync keep it.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    let provider = rustls::crypto::ring::default_provider();
    loop {
        let mut bytes = [0; 8];
        provider
            .secure_random
            .fill(&mut bytes)
            .map_err(|_| io::Error::other("no random bits"))?;
        let id = u64::from_be_bytes(bytes);
        if id != 0 {
            return Ok(id);
        }
    }
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
                // 48 bits go into the node id; keep them non-zero.
                let id = loop {
                    let id = random_id()?;
                    if id & 0xffff_ffff_ffff != 0 {
                        break id;
                    }
                };
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
    /// Ordinal 0 of a new set: make itself the first voter.
    Initialize,
    /// Ask the set to take it in as a learner.
    Join,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub id: NodeId,
    /// The cluster id.
    pub epoch: u64,
    pub plan: Plan,
}

/// Decides how the replica in `slot` on `volume` starts. `pristine` is
/// whether its store holds nothing (no vote, log or snapshot). Needs the
/// API server only for a pristine volume, and waits for it, and for
/// ordinal 0's marker, as long as it takes.
pub async fn prepare(
    volume: &Volume,
    slot: Slot,
    pristine: bool,
    markers: &dyn Markers,
) -> io::Result<Prepared> {
    let id = node_id(slot, volume.id);
    if !pristine {
        let epoch = volume.cluster()?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "the volume holds a log but no cluster id",
            )
        })?;
        return Ok(Prepared {
            id,
            epoch,
            plan: Plan::Resume,
        });
    }
    let marker = loop {
        let attempt = async {
            match markers.get().await? {
                Some(marker) => Ok::<_, String>(Some(marker)),
                None if slot == Slot::Audio(0) => {
                    let marker = Marker {
                        cluster: random_id().map_err(|e| e.to_string())?,
                        volume: volume.id,
                    };
                    // Lost to another creator: read theirs next round.
                    Ok(markers.create(marker).await?.then_some(marker))
                }
                None => Ok(None),
            }
        };
        match attempt.await {
            Ok(Some(marker)) => break marker,
            Ok(None) => tracing::info!(marker = markers.name(), "waiting for the marker"),
            Err(e) => tracing::warn!(marker = markers.name(), error = %e, "reading the marker"),
        }
        tokio::time::sleep(RETRY).await;
    };
    volume.set_cluster(marker.cluster)?;
    let plan = if slot == Slot::Audio(0) && marker.volume == volume.id {
        Plan::Initialize
    } else {
        Plan::Join
    };
    Ok(Prepared {
        id,
        epoch: marker.cluster,
        plan,
    })
}

/// The marker in the Kubernetes API: create and get on one ConfigMap.
pub struct Kube {
    /// `host:port` of the API server.
    addr: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    token: Option<PathBuf>,
    namespace: String,
    name: String,
}

const SERVICE_ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl Kube {
    /// The API server at `addr` (`host:port`), over TLS trusting `ca` (PEM)
    /// when given, authenticated by the bearer token in `token`, re-read
    /// for every request since a projected token is rotated.
    pub fn new(
        addr: String,
        ca: Option<&[u8]>,
        token: Option<PathBuf>,
        namespace: String,
        name: String,
    ) -> io::Result<Kube> {
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
        })
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
            let body = serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": self.name },
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
