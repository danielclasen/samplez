//! Replica sets started from the marker in a fake Kubernetes API server,
//! over the peer listener on loopback with mutual TLS. Real time: sockets
//! do not follow a paused clock.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use harness::{Paths, RATE, owner};
use openraft::ServerState;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use samplez_replica::bootstrap::{self, Kube, Markers, Plan, Prepared, Volume};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::NodeId;
use samplez_replica::log_store::Store;
use samplez_replica::membership::{Slot, Voters, slot_of};
use samplez_replica::net::{Network, Pool};
use samplez_replica::peer::{self, PeerClient, PeerRequest, Refused};
use samplez_replica::{Replica, Settings, raft_config};
use samplez_service::config::{Tls, TlsMode};
use samplez_service::replica::{Reason, Resolution};
use samplez_service::{Metrics, PeerTls, ReplicatedState};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const TOKEN: &str = "t0k3n";
const NAMESPACE: &str = "replicas";
const MARKER: &str = "set-replica-cluster";

/// ConfigMaps by name in one namespace: create (409 when it exists), get
/// (404 when absent), delete; every request needs the bearer token.
#[derive(Clone, Default)]
struct Api(Arc<Mutex<BTreeMap<String, Value>>>);

impl Api {
    async fn serve(&self) -> SocketAddr {
        let authorized = |headers: &HeaderMap| {
            headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some(&format!("Bearer {TOKEN}"))
        };
        let create = move |State(api): State<Api>,
                           Path(namespace): Path<String>,
                           headers: HeaderMap,
                           body: Bytes| async move {
            if !authorized(&headers) {
                return StatusCode::UNAUTHORIZED;
            }
            assert_eq!(namespace, NAMESPACE);
            let map: Value = serde_json::from_slice(&body).expect("a ConfigMap");
            let name = map["metadata"]["name"].as_str().expect("a name").to_owned();
            let mut maps = api.0.lock().unwrap();
            if maps.contains_key(&name) {
                return StatusCode::CONFLICT;
            }
            maps.insert(name, map);
            StatusCode::CREATED
        };
        let read = move |State(api): State<Api>,
                         Path((namespace, name)): Path<(String, String)>,
                         headers: HeaderMap| async move {
            if !authorized(&headers) {
                return (StatusCode::UNAUTHORIZED, Vec::new());
            }
            assert_eq!(namespace, NAMESPACE);
            match api.0.lock().unwrap().get(&name) {
                Some(map) => (StatusCode::OK, serde_json::to_vec(map).unwrap()),
                None => (StatusCode::NOT_FOUND, Vec::new()),
            }
        };
        let router = Router::new()
            .route("/api/v1/namespaces/{namespace}/configmaps", post(create))
            .route(
                "/api/v1/namespaces/{namespace}/configmaps/{name}",
                get(read),
            )
            .with_state(self.clone());
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        tokio::spawn(axum::serve(tcp, router).into_future());
        addr
    }

    fn marker(&self) -> Option<(u64, u64)> {
        let maps = self.0.lock().unwrap();
        let data = &maps.get(MARKER)?["data"];
        let field = |f: &str| data[f].as_str().unwrap().parse().unwrap();
        Some((field("cluster"), field("volume")))
    }

    fn delete(&self) {
        self.0.lock().unwrap().remove(MARKER);
    }
}

static SETS: AtomicU64 = AtomicU64::new(0);

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "peer-set-{}-{}",
        std::process::id(),
        SETS.fetch_add(1, Ordering::Relaxed)
    ))
}

fn kube(api: SocketAddr, root: &std::path::Path) -> Kube {
    let token = root.join("token");
    std::fs::write(&token, format!("{TOKEN}\n")).unwrap();
    Kube::new(
        api.to_string(),
        None,
        Some(token),
        NAMESPACE.into(),
        MARKER.into(),
    )
    .unwrap()
}

struct Node {
    replica: Arc<Replica<Paths>>,
    pool: Arc<Pool>,
    prepared: Prepared,
    stop: oneshot::Sender<()>,
    server: JoinHandle<std::io::Result<()>>,
}

/// One replica set: a volume, a certificate and a peer port per slot.
struct Set {
    root: PathBuf,
    api: Api,
    kube: Arc<Kube>,
    voters: Voters,
    addrs: BTreeMap<Slot, String>,
    tls: BTreeMap<Slot, Tls>,
    nodes: BTreeMap<Slot, Node>,
}

fn name(slot: Slot) -> String {
    match slot {
        Slot::Audio(ordinal) => format!("audio-{ordinal}"),
        Slot::Witness => "witness".into(),
    }
}

impl Set {
    async fn new(voters: Voters) -> Set {
        let root = root();
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let api = Api::default();
        let kube = Arc::new(kube(api.serve().await, &root));
        let mut slots: Vec<Slot> = (0..voters.replicas).map(Slot::Audio).collect();
        if voters.witness {
            slots.push(Slot::Witness);
        }
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        let ca_file = root.join("ca.crt");
        std::fs::write(&ca_file, ca.pem()).unwrap();
        let (mut addrs, mut tls) = (BTreeMap::new(), BTreeMap::new());
        for slot in slots {
            let key = KeyPair::generate().unwrap();
            let cert = CertificateParams::new(vec!["127.0.0.1".into()])
                .unwrap()
                .signed_by(&key, &ca)
                .unwrap();
            let (cert_file, key_file) = (
                root.join(format!("{}.crt", name(slot))),
                root.join(format!("{}.key", name(slot))),
            );
            std::fs::write(&cert_file, cert.pem()).unwrap();
            std::fs::write(&key_file, key.serialize_pem()).unwrap();
            tls.insert(
                slot,
                Tls {
                    mode: TlsMode::Files,
                    cert_file: Some(cert_file),
                    key_file: Some(key_file),
                    client_ca_file: Some(ca_file.clone()),
                    sans: Vec::new(),
                },
            );
            // A port nobody else takes while the set runs.
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addrs.insert(slot, tcp.local_addr().unwrap().to_string());
        }
        Set {
            root,
            api,
            kube,
            voters,
            addrs,
            tls,
            nodes: BTreeMap::new(),
        }
    }

    fn dir(&self, slot: Slot) -> PathBuf {
        self.root.join(name(slot))
    }

    /// Starts the replica in `slot` from its volume and the marker.
    async fn start(&self, slot: Slot) -> Node {
        let dir = self.dir(slot);
        let volume = Volume::open(&dir).unwrap();
        let limits = Limits::defaults(RATE);
        let store = Store::open(&dir, Paths::default(), limits).unwrap();
        let prepared = bootstrap::prepare(&volume, slot, store.pristine(), &*self.kube)
            .await
            .unwrap();
        let tls = Arc::new(PeerTls::new(&self.tls[&slot], &Metrics::default()).unwrap());
        let pool = Pool::new(tls.clone());
        let settings = Settings {
            id: prepared.id,
            epoch: prepared.epoch,
            limits,
            block: harness::BLOCK,
            write_timeout: Duration::from_secs(3),
            addr: self.addrs[&slot].clone(),
            peers: self.addrs.values().cloned().collect(),
            voters: Some(self.voters),
        };
        let mut raft = raft_config("test");
        // The snapshot an install needs is taken by hand.
        raft.snapshot_policy = openraft::SnapshotPolicy::Never;
        raft.max_in_snapshot_log_to_keep = 0;
        raft.purge_batch_size = 1;
        let replica = Replica::with_store(
            settings,
            raft,
            store,
            Network { pool: pool.clone() },
            pool.clone(),
            Arc::new(|| 0),
        )
        .await
        .unwrap();
        let replica = Arc::new(replica);
        let tcp = TcpListener::bind(&self.addrs[&slot]).await.unwrap();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(peer::serve(tls, tcp, replica.clone(), async {
            let _ = stopped.await;
        }));
        replica.begin(prepared.plan, MARKER).await.unwrap();
        Node {
            replica,
            pool,
            prepared,
            stop,
            server,
        }
    }

    /// Starts every slot at once, as `podManagementPolicy: Parallel` does.
    async fn start_all(&mut self) {
        let slots: Vec<Slot> = self.addrs.keys().copied().collect();
        let nodes =
            futures_util::future::join_all(slots.iter().map(|&slot| self.start(slot))).await;
        self.nodes.extend(slots.into_iter().zip(nodes));
    }

    async fn crash(&mut self, slot: Slot) {
        let node = self.nodes.remove(&slot).expect("running");
        let _ = node.stop.send(());
        node.replica.shutdown().await;
        let _ = node.server.await;
    }

    fn replica(&self, slot: Slot) -> &Arc<Replica<Paths>> {
        &self.nodes[&slot].replica
    }

    /// Waits until a leader has made the running replicas the voters.
    async fn settled(&self) -> Slot {
        let want: BTreeSet<NodeId> = self.nodes.values().map(|n| n.prepared.id).collect();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for (&slot, node) in &self.nodes {
                let metrics = node.replica.raft().metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
                if metrics.state == ServerState::Leader
                    && voters == want
                    && membership.nodes().count() == want.len()
                    && self
                        .nodes
                        .iter()
                        .all(|(s, n)| *s == Slot::Witness || n.replica.replica().commitable)
                    && self.nodes.values().all(|n| {
                        n.replica.raft().metrics().borrow().current_leader == Some(node.prepared.id)
                    })
                {
                    return slot;
                }
            }
            assert!(
                Instant::now() < deadline,
                "the voters did not settle in 20 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn applied(&self, index: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self
            .nodes
            .values()
            .any(|n| n.replica.store().applied_index() < index)
        {
            assert!(Instant::now() < deadline, "{index} not applied in 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for Set {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn write(replica: &Replica<Paths>, id: &str, command: &str) -> u64 {
    let ack = replica
        .submit(&owner("s"), id, Bytes::from(command.to_owned()))
        .await
        .expect("committed");
    assert_eq!(ack.resolution, Resolution::Applied);
    ack.revision
}

/// Ordinal 0 creates the marker and initialises the set; the other voters
/// join as learners and are promoted; the epoch is the marker's cluster
/// id on every replica. Writes reach the leader through any audio replica,
/// also when the witness leads; the witness serves no clients.
#[tokio::test]
async fn a_set_starts_from_the_marker_and_takes_writes_through_any_audio_replica() {
    let mut set = Set::new(Voters::new(2, true).unwrap()).await;
    set.start_all().await;
    set.settled().await;
    let (cluster, volume) = set.api.marker().expect("ordinal 0 created the marker");
    let first = Volume::open(&set.dir(Slot::Audio(0))).unwrap();
    assert_eq!(volume, first.id, "naming ordinal 0's volume");
    for (slot, node) in &set.nodes {
        assert_eq!(node.replica.epoch(), cluster, "{slot:?}");
        assert_eq!(slot_of(node.prepared.id), *slot);
        let plan = if *slot == Slot::Audio(0) {
            Plan::Initialize
        } else {
            Plan::Join
        };
        assert_eq!(node.prepared.plan, plan, "{slot:?}");
    }

    let mut last = 0;
    for (n, slot) in [Slot::Audio(0), Slot::Audio(1)].into_iter().enumerate() {
        last = write(set.replica(slot), &format!("{n}"), &format!("gain={n}@{n}")).await;
    }
    set.applied(last).await;
    let witness = set.replica(Slot::Witness);
    assert_eq!(witness.store().with_state(|sm| sm.product.applied), 2);
    assert!(!witness.replica().commitable);
    let refused = witness
        .submit(&owner("s"), "w", Bytes::from("gain=9@9"))
        .await
        .expect_err("no clients on the witness");
    assert_eq!(refused.reason, Reason::NotCommitable);

    // The witness can lead: writes are forwarded to it.
    let deadline = Instant::now() + Duration::from_secs(10);
    while set.settled().await != Slot::Witness {
        assert!(
            Instant::now() < deadline,
            "the witness did not take the lead"
        );
        let _ = witness.raft().trigger().elect().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let revision = write(set.replica(Slot::Audio(1)), "led", "pan=1@1").await;
    set.applied(revision).await;
    assert_eq!(witness.store().with_state(|sm| sm.product.applied), 3);
}

/// Ordinal 0 whose volume was lost never initialises a second set: it
/// joins as a learner from the leader's snapshot, takes its old node's
/// place among the voters and keeps the epoch. Its old node id is no
/// longer answered at its address.
#[tokio::test]
async fn ordinal_0_rejoins_as_a_learner_after_losing_its_volume() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_all().await;
    let leader = set.settled().await;
    let epoch = set.replica(leader).epoch();
    let mut last = 0;
    for n in 1..=3 {
        last = write(
            set.replica(Slot::Audio(1)),
            &n.to_string(),
            &format!("gain={n}@{n}"),
        )
        .await;
    }
    set.applied(last).await;
    // The log up to here goes into a snapshot, so the new volume can only
    // catch up by installing it.
    for node in set.nodes.values() {
        let raft = node.replica.raft();
        raft.trigger().snapshot().await.unwrap();
        while raft
            .metrics()
            .borrow()
            .snapshot
            .is_none_or(|id| id.index < last)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        raft.trigger().purge_log(last).await.unwrap();
    }

    let old = set.nodes[&Slot::Audio(0)].prepared.id;
    set.crash(Slot::Audio(0)).await;
    std::fs::remove_dir_all(set.dir(Slot::Audio(0))).unwrap();
    let node = set.start(Slot::Audio(0)).await;
    assert_eq!(node.prepared.plan, Plan::Join, "never initialised again");
    assert_ne!(node.prepared.id, old);
    assert_eq!(node.prepared.epoch, epoch);
    set.nodes.insert(Slot::Audio(0), node);
    set.settled().await;

    let rejoined = set.replica(Slot::Audio(0));
    assert_eq!(rejoined.epoch(), epoch, "kept across the snapshot install");
    assert!(
        rejoined.raft().metrics().borrow().snapshot.is_some(),
        "installed"
    );
    assert_eq!(rejoined.store().with_state(|sm| sm.product.applied), 3);
    assert!(rejoined.replica().commitable);
    let revision = write(rejoined, "4", "gain=4@4").await;
    set.applied(revision).await;

    let pool = &set.nodes[&Slot::Audio(1)].pool;
    let addr = &set.addrs[&Slot::Audio(0)];
    let asked = pool
        .call(
            Some(old),
            addr,
            PeerRequest::ReadIndex,
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(asked, Ok(Err(Refused::Misaddressed)));
}

/// A restart keeps the epoch; a new set, started empty under a new
/// marker, draws a new one. Ordinal 0 on an empty volume while the marker
/// names another and no voter holds a log initialises nothing, and its
/// status names the marker to delete.
#[tokio::test]
async fn the_epoch_is_kept_on_a_restart_and_drawn_anew_for_a_new_set() {
    let mut set = Set::new(Voters::new(1, false).unwrap()).await;
    let slot = Slot::Audio(0);
    set.start_all().await;
    set.settled().await;
    let epoch = set.replica(slot).epoch();
    let revision = write(set.replica(slot), "1", "gain=1@1").await;

    set.crash(slot).await;
    let node = set.start(slot).await;
    assert_eq!(node.prepared.plan, Plan::Resume);
    assert_eq!(node.replica.epoch(), epoch);
    assert_eq!(node.replica.revision(), revision);
    set.nodes.insert(slot, node);
    set.settled().await;

    set.crash(slot).await;
    std::fs::remove_dir_all(set.dir(slot)).unwrap();
    let node = set.start(slot).await;
    assert_eq!(node.prepared.plan, Plan::Join);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!node.replica.raft().is_initialized().await.unwrap());
    let status = node.replica.status().expect("a status");
    assert!(status.contains(MARKER), "{status}");
    set.nodes.insert(slot, node);

    // Deleting the marker starts a new set on the next empty start.
    set.crash(slot).await;
    std::fs::remove_dir_all(set.dir(slot)).unwrap();
    set.api.delete();
    let node = set.start(slot).await;
    assert_eq!(node.prepared.plan, Plan::Initialize);
    assert_ne!(node.replica.epoch(), epoch, "a new history");
    assert_eq!(set.api.marker().unwrap().0, node.replica.epoch());
    set.nodes.insert(slot, node);
    set.settled().await;
    assert_eq!(
        set.replica(slot)
            .store()
            .with_state(|sm| sm.product.applied),
        0
    );
}

/// The marker decides how an empty volume starts: ordinal 0 initialises
/// only under a marker it created or one naming its own volume (a crash
/// between the two), anybody else joins, and a replica other than
/// ordinal 0 waits for the marker.
#[tokio::test]
async fn ordinal_0_initialises_only_under_a_marker_naming_its_volume() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    let api = Api::default();
    let api_addr = api.serve().await;
    let markers = kube(api_addr, &root);

    let waiting = tokio::spawn({
        let volume = Volume::open(&root.join("one")).unwrap();
        let markers = kube(api_addr, &root);
        async move { bootstrap::prepare(&volume, Slot::Audio(1), true, &markers).await }
    });
    tokio::time::sleep(bootstrap::RETRY * 2).await;
    assert!(!waiting.is_finished(), "no marker yet");
    assert_eq!(api.marker(), None);

    let zero = Volume::open(&root.join("zero")).unwrap();
    let first = bootstrap::prepare(&zero, Slot::Audio(0), true, &markers)
        .await
        .unwrap();
    assert_eq!(first.plan, Plan::Initialize);
    assert_eq!(api.marker(), Some((first.epoch, zero.id)));
    // Restarted before initialising: the marker names its volume.
    let again = bootstrap::prepare(&zero, Slot::Audio(0), true, &markers)
        .await
        .unwrap();
    assert_eq!(again, first);

    let joined = waiting.await.unwrap().unwrap();
    assert_eq!((joined.plan, joined.epoch), (Plan::Join, first.epoch));
    let other = Volume::open(&root.join("zero-again")).unwrap();
    let lost = bootstrap::prepare(&other, Slot::Audio(0), true, &markers)
        .await
        .unwrap();
    assert_eq!((lost.plan, lost.epoch), (Plan::Join, first.epoch));
    assert!(
        !markers
            .create(bootstrap::Marker {
                cluster: 1,
                volume: 1
            })
            .await
            .unwrap()
    );
    assert_eq!(api.marker(), Some((first.epoch, zero.id)), "left as it was");

    // A volume holding a log goes on without the API server.
    let gone = Kube::new(
        "127.0.0.1:1".into(),
        None,
        None,
        NAMESPACE.into(),
        MARKER.into(),
    )
    .unwrap();
    let resumed = bootstrap::prepare(&zero, Slot::Audio(0), false, &gone)
        .await
        .unwrap();
    assert_eq!((resumed.plan, resumed.epoch), (Plan::Resume, first.epoch));
    let _ = std::fs::remove_dir_all(&root);
}
