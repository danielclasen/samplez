//! Replica sets started from the marker in a fake Kubernetes API server
//! over TLS, over the peer listener on loopback with mutual TLS. Real
//! time: sockets do not follow a paused clock.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use harness::{PanicGuard, Paths, RATE, owner};
use openraft::ServerState;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use samplez_replica::bootstrap::{self, Bootstrap, Kube, Marker, Markers, Plan, Volume};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::NodeId;
use samplez_replica::log_store::Store;
use samplez_replica::membership::{NO_MEMBER_ROUNDS, PERIOD, Slot, UNCOUNTED, Voters, slot_of};
use samplez_replica::net::{Network, Pool};
use samplez_replica::peer::{self, PeerClient, PeerRequest, Refused};
use samplez_replica::{Replica, Settings, raft_config};
use samplez_service::config::{Tls, TlsMode};
use samplez_service::replica::{Reason, Resolution};
use samplez_service::{Metrics, PeerTls, ReplicatedState};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

const TOKEN: &str = "t0k3n";
const NAMESPACE: &str = "replicas";
const MARKER: &str = "set-replica-cluster";
/// The one StatefulSet the fake API server serves, and its uid.
const STATEFULSET: &str = "set-audio";
const STATEFULSET_UID: &str = "0d6a3c1e-5b2f-4f7a-9c11-2e8b7d4a6f90";

/// A CA, as PEM, and a certificate for 127.0.0.1 it issued.
fn authority_for_loopback() -> (
    CertifiedIssuer<'static, KeyPair>,
    CertificateDer<'static>,
    PrivateKeyDer<'static>,
) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(vec!["127.0.0.1".into()])
        .unwrap()
        .signed_by(&key, &ca)
        .unwrap();
    let der = cert.der().clone();
    (ca, der, PrivateKeyDer::Pkcs8(key.serialize_der().into()))
}

/// Accepts TLS connections for axum, dropping failed handshakes.
struct Tlsd {
    tcp: TcpListener,
    tls: TlsAcceptor,
}

impl axum::serve::Listener for Tlsd {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let Ok((tcp, addr)) = self.tcp.accept().await else {
                continue;
            };
            if let Ok(tls) = self.tls.accept(tcp).await {
                return (tls, addr);
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }
}

/// ConfigMaps by name in one namespace, over TLS: create (409 when it
/// exists), get (404 when absent); and get on the StatefulSet
/// [`STATEFULSET`], its metadata [`STATEFULSET_UID`] and what the second
/// field sets over it. Every request needs the bearer token.
#[derive(Clone, Default)]
struct Api(Arc<Mutex<BTreeMap<String, Value>>>, Arc<Mutex<Value>>);

impl Api {
    /// Serves the API; returns its address and the PEM of the CA its
    /// certificate is from.
    async fn serve(&self) -> (SocketAddr, String) {
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
        let statefulset = move |State(api): State<Api>,
                                Path((namespace, name)): Path<(String, String)>,
                                headers: HeaderMap| async move {
            if !authorized(&headers) {
                return (StatusCode::UNAUTHORIZED, Vec::new());
            }
            assert_eq!(namespace, NAMESPACE);
            if name != STATEFULSET {
                return (StatusCode::NOT_FOUND, Vec::new());
            }
            let mut metadata =
                serde_json::json!({ "name": name, "namespace": namespace, "uid": STATEFULSET_UID });
            if let Value::Object(set) = &*api.1.lock().unwrap() {
                for (k, v) in set {
                    metadata[k] = v.clone();
                }
            }
            let set = serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": metadata,
                "status": { "updateRevision": "set-audio-1" },
            });
            (StatusCode::OK, serde_json::to_vec(&set).unwrap())
        };
        let router = Router::new()
            .route(
                "/apis/apps/v1/namespaces/{namespace}/statefulsets/{name}",
                get(statefulset),
            )
            .route("/api/v1/namespaces/{namespace}/configmaps", post(create))
            .route(
                "/api/v1/namespaces/{namespace}/configmaps/{name}",
                get(read),
            )
            .with_state(self.clone());
        let (ca, cert, key) = authority_for_loopback();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let listener = Tlsd {
            tcp,
            tls: TlsAcceptor::from(Arc::new(config)),
        };
        tokio::spawn(axum::serve(listener, router).into_future());
        (addr, ca.pem())
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

async fn kube(api: &Api, root: &std::path::Path) -> Kube {
    let (addr, ca) = api.serve().await;
    let token = root.join("token");
    std::fs::write(&token, format!("{TOKEN}\n")).unwrap();
    Kube::new(
        addr.to_string(),
        Some(ca.as_bytes()),
        Some(token),
        NAMESPACE.into(),
        MARKER.into(),
    )
    .unwrap()
}

struct Node {
    replica: Arc<Replica<Paths>>,
    pool: Arc<Pool>,
    id: NodeId,
    plan: Plan,
    stop: oneshot::Sender<()>,
    server: JoinHandle<io::Result<()>>,
}

/// One replica set: a volume, a certificate and a peer port per slot.
struct Set {
    root: PathBuf,
    api: Api,
    kube: Arc<Kube>,
    voters: Voters,
    addrs: BTreeMap<Slot, String>,
    /// Bound for the set's lifetime, so a stopped replica's port is not
    /// taken by another socket before it starts again.
    listeners: BTreeMap<Slot, std::net::TcpListener>,
    /// Per slot not running, a task that takes each connection to its port
    /// and drops it, as a stopped pod's address refuses one: a request
    /// queued on the bound port would reach the replica started next,
    /// however old.
    refusers: Mutex<BTreeMap<Slot, JoinHandle<()>>>,
    tls: BTreeMap<Slot, Tls>,
    nodes: BTreeMap<Slot, Node>,
    _panics: PanicGuard,
}

/// Takes each connection to `tcp` and drops it, until aborted.
fn refuse(tcp: &std::net::TcpListener) -> JoinHandle<()> {
    let tcp = TcpListener::from_std(tcp.try_clone().unwrap()).unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((connection, _)) = tcp.accept().await {
                drop(connection);
            }
        }
    })
}

fn name(slot: Slot) -> String {
    match slot {
        Slot::Audio(ordinal) => format!("audio-{ordinal}"),
        Slot::Witness => "witness".into(),
    }
}

impl Set {
    async fn new(voters: Voters) -> Set {
        let root = harness::root("bootstrap-set");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let api = Api::default();
        let kube = Arc::new(kube(&api, &root).await);
        let mut slots: Vec<Slot> = (0..voters.replicas).map(Slot::Audio).collect();
        if voters.witness {
            slots.push(Slot::Witness);
        }
        let (ca, _, _) = authority_for_loopback();
        let ca_file = root.join("ca.crt");
        std::fs::write(&ca_file, ca.pem()).unwrap();
        let (mut addrs, mut tls, mut listeners) =
            (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
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
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            tcp.set_nonblocking(true).unwrap();
            addrs.insert(slot, tcp.local_addr().unwrap().to_string());
            listeners.insert(slot, tcp);
        }
        let refusers = (listeners.iter())
            .map(|(&slot, tcp)| (slot, refuse(tcp)))
            .collect();
        Set {
            root,
            api,
            kube,
            voters,
            addrs,
            listeners,
            refusers: Mutex::new(refusers),
            tls,
            nodes: BTreeMap::new(),
            _panics: PanicGuard::new(),
        }
    }

    fn dir(&self, slot: Slot) -> PathBuf {
        self.root.join(name(slot))
    }

    /// Starts the replica in `slot` from its volume, and the marker when
    /// the volume is empty.
    async fn start(&self, slot: Slot) -> Node {
        let dir = self.dir(slot);
        let volume = Volume::open(&dir).unwrap();
        let limits = Limits::defaults(RATE);
        let store = Store::open(&dir, Arc::new(Paths::default), limits).unwrap();
        let prepared = bootstrap::prepare(&volume, slot, store.pristine()).unwrap();
        let tls = Arc::new(PeerTls::new(&self.tls[&slot], &Metrics::default()).unwrap());
        let pool = Pool::new(tls.clone(), samplez_replica::peer::MAX_RESPONSE);
        let settings = Settings {
            id: prepared.id,
            epoch: prepared.epoch,
            limits,
            block: harness::BLOCK,
            // Long, for a loaded test host.
            write_timeout: Duration::from_secs(10),
            addr: self.addrs[&slot].clone(),
            peers: self.addrs.values().cloned().collect(),
            voters: Some(self.voters),
        };
        let mut raft = raft_config("test");
        // The snapshot an install needs is taken by hand.
        raft.snapshot_policy = openraft::SnapshotPolicy::Never;
        raft.max_in_snapshot_log_to_keep = 0;
        raft.purge_batch_size = 1;
        // Elections only past a long silence: under a loaded host a short
        // timeout moves the leadership in the middle of a test that
        // purged the leader's log.
        raft.election_timeout_min = 1000;
        raft.election_timeout_max = 2000;
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
        let refuser = self.refusers.lock().unwrap().remove(&slot);
        if let Some(refuser) = refuser {
            refuser.abort();
            let _ = refuser.await;
        }
        let tcp = TcpListener::from_std(self.listeners[&slot].try_clone().unwrap()).unwrap();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(peer::serve(tls, tcp, replica.clone(), async {
            let _ = stopped.await;
        }));
        replica.begin(Bootstrap {
            markers: self.kube.clone(),
            volume,
            slot,
        });
        Node {
            replica,
            pool,
            id: prepared.id,
            plan: prepared.plan,
            stop,
            server,
        }
    }

    async fn start_slots(&mut self, slots: &[Slot]) {
        let nodes =
            futures_util::future::join_all(slots.iter().map(|&slot| self.start(slot))).await;
        self.nodes.extend(slots.iter().copied().zip(nodes));
    }

    /// Starts every slot at once, as `podManagementPolicy: Parallel` does.
    async fn start_all(&mut self) {
        let slots: Vec<Slot> = self.addrs.keys().copied().collect();
        self.start_slots(&slots).await;
    }

    async fn crash(&mut self, slot: Slot) {
        let node = self.nodes.remove(&slot).expect("running");
        let _ = node.stop.send(());
        node.replica.shutdown().await;
        let _ = node.server.await;
        let refuser = refuse(&self.listeners[&slot]);
        self.refusers.lock().unwrap().insert(slot, refuser);
    }

    fn replica(&self, slot: Slot) -> &Arc<Replica<Paths>> {
        &self.nodes[&slot].replica
    }

    /// Waits until a leader has made the running replicas the voters and
    /// every audio replica is commitable.
    async fn settled(&self) -> Slot {
        let want: BTreeSet<NodeId> = self.nodes.values().map(|n| n.id).collect();
        match self.voters_become(&want, Duration::from_secs(30)).await {
            Some(leader) => leader,
            None => panic!("the voters did not settle in 30 s:\n{}", self.describe()),
        }
    }

    /// Makes `slot` lead, the voters settled. Followers refuse a vote while
    /// they heard their leader within the 1-2 s election timeout, so an
    /// election `slot` calls takes that long; it calls the next only once
    /// the last one had the time to end.
    async fn lead(&self, slot: Slot) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.settled().await == slot {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{slot:?} did not take the lead in 60 s:\n{}",
                self.describe()
            );
            let raft = self.replica(slot).raft().clone();
            let _ = raft.trigger().elect().await;
            until(Duration::from_secs(5), || {
                raft.metrics().borrow().state == ServerState::Leader
            })
            .await;
        }
    }

    /// Each replica's view, for a failure message.
    fn describe(&self) -> String {
        let mut lines = Vec::new();
        for (slot, node) in &self.nodes {
            let metrics = node.replica.raft().metrics().borrow().clone();
            lines.push(format!(
                "{slot:?} {} {:?} leader {:?} applied {:?} commitable {} status {:?} membership {:?}",
                node.id,
                metrics.state,
                metrics.current_leader,
                metrics.last_applied.map(|id| id.index),
                node.replica.replica().commitable,
                node.replica.status(),
                metrics.membership_config.membership(),
            ));
        }
        lines.join("\n")
    }

    /// The leader's slot once its voters are `want`, out of any joint
    /// configuration, the membership holds nothing else and every running
    /// audio replica is commitable.
    async fn voters_become(&self, want: &BTreeSet<NodeId>, within: Duration) -> Option<Slot> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            for (&slot, node) in &self.nodes {
                let metrics = node.replica.raft().metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
                if metrics.state == ServerState::Leader
                    && membership.get_joint_config().len() == 1
                    && voters == *want
                    && membership.nodes().count() == want.len()
                    && self
                        .nodes
                        .iter()
                        .all(|(s, n)| *s == Slot::Witness || n.replica.replica().commitable)
                {
                    return Some(slot);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    async fn applied(&self, index: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.nodes.values().any(|n| n.replica.revision() < index) {
            assert!(Instant::now() < deadline, "{index} not applied in 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Writes through `slot`; a failure names every replica's view.
    async fn write(&self, slot: Slot, id: &str, command: &str) -> u64 {
        let written = self
            .replica(slot)
            .submit(&owner("s"), id, Bytes::from(command.to_owned()))
            .await;
        match written {
            Ok(ack) => {
                assert_eq!(ack.resolution, Resolution::Applied);
                ack.revision
            }
            Err(e) => panic!("{e} through {slot:?}:\n{}", self.describe()),
        }
    }

    fn epochs(&self) -> BTreeSet<u64> {
        self.nodes.values().map(|n| n.replica.epoch()).collect()
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

/// Waits until `check` holds, at most `within`.
async fn until(within: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    check()
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
    assert_eq!(set.epochs(), BTreeSet::from([cluster]));
    for (slot, node) in &set.nodes {
        assert_eq!(slot_of(node.id), *slot);
        assert_eq!(node.plan, Plan::Bootstrap, "{slot:?}");
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
    set.lead(Slot::Witness).await;
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
    // Only ordinal 1, the leader, purges: openraft holds a leader's purge
    // back while replication still reads the range, and does not on a
    // follower, so a purge on one that is elected meanwhile can race its
    // replication.
    set.lead(Slot::Audio(1)).await;
    let leader = Slot::Audio(1);
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
    // catch up by installing it. `last` is a product revision; snapshots
    // and purges count log ids.
    let raft = set.replica(leader).raft().clone();
    let upto = raft
        .metrics()
        .borrow()
        .last_applied
        .map_or(0, |id| id.index);
    raft.trigger().snapshot().await.unwrap();
    while raft
        .metrics()
        .borrow()
        .snapshot
        .is_none_or(|id| id.index < upto)
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    raft.trigger().purge_log(upto).await.unwrap();
    while raft
        .metrics()
        .borrow()
        .purged
        .is_none_or(|id| id.index < upto)
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let old = set.nodes[&Slot::Audio(0)].id;
    set.crash(Slot::Audio(0)).await;
    std::fs::remove_dir_all(set.dir(Slot::Audio(0))).unwrap();
    let node = set.start(Slot::Audio(0)).await;
    assert_eq!(node.plan, Plan::Bootstrap);
    assert_ne!(node.id, old);
    set.nodes.insert(Slot::Audio(0), node);
    set.settled().await;
    assert!(
        set.api
            .marker()
            .is_some_and(|(cluster, _)| cluster == epoch),
        "the marker is left as it was"
    );

    let rejoined = set.replica(Slot::Audio(0));
    assert_eq!(rejoined.epoch(), epoch, "kept across the snapshot install");
    assert!(
        until(Duration::from_secs(5), || rejoined
            .raft()
            .metrics()
            .borrow()
            .snapshot
            .is_some())
        .await,
        "installed"
    );
    assert_eq!(rejoined.store().with_state(|sm| sm.product.applied), 3);
    assert!(rejoined.replica().commitable);
    let revision = write(rejoined, "4", "gain=4@4").await;
    set.applied(revision).await;

    let pool = &set.nodes[&Slot::Audio(1)].pool;
    let addr = &set.addrs[&Slot::Audio(0)];
    let asked = pool
        .call(Some(old), addr, PeerRequest::Member, Duration::from_secs(1))
        .await;
    assert_eq!(asked, Ok(Err(Refused::Misaddressed)));
}

/// A restart keeps the epoch. An empty ordinal 0 under a marker naming
/// another volume joins; it names the marker to delete only once several
/// rounds found no member to join, and the deletion takes effect without
/// a restart: a new set under a new epoch.
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
    assert_eq!(node.plan, Plan::Resume);
    assert_eq!(node.replica.epoch(), epoch);
    assert_eq!(node.replica.revision(), revision);
    set.nodes.insert(slot, node);
    set.settled().await;

    set.crash(slot).await;
    std::fs::remove_dir_all(set.dir(slot)).unwrap();
    let node = set.start(slot).await;
    assert_eq!(node.plan, Plan::Bootstrap);
    tokio::time::sleep(PERIOD * 2).await;
    let status = node.replica.status().expect("a status");
    assert!(status.contains("joining"), "{status}");
    assert!(
        !status.contains("delete"),
        "only after several rounds: {status}"
    );
    let replica = node.replica.clone();
    set.nodes.insert(slot, node);
    assert!(
        until(PERIOD * (NO_MEMBER_ROUNDS + 3), || replica
            .status()
            .is_some_and(|status| status.contains("delete the ConfigMap")))
        .await,
        "{:?}",
        replica.status()
    );
    assert!(!replica.raft().is_initialized().await.unwrap());
    assert!(replica.status().unwrap().contains(MARKER));

    // Deleting the marker starts a new set, without a restart.
    set.api.delete();
    set.settled().await;
    assert_ne!(replica.epoch(), epoch, "a new history");
    assert_eq!(set.api.marker().unwrap().0, replica.epoch());
    assert_eq!(replica.store().with_state(|sm| sm.product.applied), 0);
    assert_eq!(replica.status(), None);
}

/// Ordinal 0 initialises under a marker naming its own volume, as after a
/// crash between creating it and initialising; any other slot waits for
/// the marker.
#[tokio::test]
async fn ordinal_0_initialises_under_a_marker_naming_its_volume() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_slots(&[Slot::Audio(1), Slot::Audio(2)]).await;
    tokio::time::sleep(PERIOD * 2).await;
    for slot in [Slot::Audio(1), Slot::Audio(2)] {
        let status = set.replica(slot).status().expect("a status");
        assert!(status.contains("waiting for ordinal 0"), "{status}");
    }
    assert_eq!(set.api.marker(), None);

    let zero = Volume::open(&set.dir(Slot::Audio(0))).unwrap();
    let marker = Marker {
        cluster: 77,
        volume: zero.id,
    };
    assert!(set.kube.create(marker).await.unwrap());
    assert!(!set.kube.create(marker).await.unwrap(), "exists");
    set.start_slots(&[Slot::Audio(0)]).await;
    set.settled().await;
    assert_eq!(set.epochs(), BTreeSet::from([77]));
}

/// Replicas still joining under a marker that is deleted and created anew
/// take on the new cluster id instead of joining the new set under the
/// old one, and a join under another cluster id is refused.
#[tokio::test]
async fn joiners_follow_a_replaced_marker_and_a_stale_cluster_is_refused() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    // A marker left from a set none of whose volumes remain.
    let stale = Marker {
        cluster: 11,
        volume: 12345,
    };
    assert!(set.kube.create(stale).await.unwrap());
    set.start_slots(&[Slot::Audio(1), Slot::Audio(2)]).await;
    // Each reads the marker on its next watch period, which a loaded host
    // delays: waited for, not slept for.
    assert!(
        until(PERIOD * 10, || set.epochs() == BTreeSet::from([11])).await,
        "{:?}",
        set.epochs()
    );

    set.api.delete();
    set.start_slots(&[Slot::Audio(0)]).await;
    set.settled().await;
    let (cluster, _) = set.api.marker().unwrap();
    assert_ne!(cluster, 11);
    assert_eq!(set.epochs(), BTreeSet::from([cluster]), "one history");
    for slot in [Slot::Audio(1), Slot::Audio(2)] {
        let volume = Volume::open(&set.dir(slot)).unwrap();
        assert_eq!(volume.cluster().unwrap(), Some(cluster), "{slot:?}");
    }

    let leader = set.settled().await;
    let join = PeerRequest::Join {
        id: 9,
        addr: "127.0.0.1:1".into(),
        cluster: 11,
    };
    let leader_id = set.nodes[&leader].id;
    assert_eq!(
        set.replica(leader).handle(Some(leader_id), join).await,
        Err(Refused::OtherCluster)
    );
}

/// With the marker gone while the set runs, ordinal 0 on an empty volume
/// does not start a second set: the set's leader recreates the marker
/// under the set's cluster id, and ordinal 0 joins the running set.
#[tokio::test]
async fn ordinal_0_joins_the_running_set_after_its_marker_is_gone() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_all().await;
    set.settled().await;
    let epoch = set.replica(Slot::Audio(1)).epoch();
    set.api.delete();
    set.crash(Slot::Audio(0)).await;
    std::fs::remove_dir_all(set.dir(Slot::Audio(0))).unwrap();
    let node = set.start(Slot::Audio(0)).await;
    set.nodes.insert(Slot::Audio(0), node);
    set.settled().await;
    assert_eq!(set.epochs(), BTreeSet::from([epoch]), "one history");
    assert_eq!(set.api.marker().map(|m| m.0), Some(epoch));
    let revision = set.write(Slot::Audio(0), "1", "gain=1@1").await;
    assert!(revision > 0, "the set commits");
    // The stopped replica asks nobody to take its old id back in: past
    // the time it would have found itself uncounted and asked again, the
    // membership still holds the running replicas alone.
    tokio::time::sleep(UNCOUNTED + 2 * PERIOD).await;
    let want: BTreeSet<NodeId> = set.nodes.values().map(|n| n.id).collect();
    assert!(
        set.voters_become(&want, PERIOD).await.is_some(),
        "the stopped replica's id is back in the membership:\n{}",
        set.describe()
    );
}

/// A StatefulSet deleted with its marker and created again on the volumes
/// it kept resumes the set from them, and its leader recreates the marker
/// under the set's cluster id: a replica that then loses its volume joins
/// the set again rather than waiting for a marker nothing creates.
#[tokio::test]
async fn a_set_resumed_from_kept_volumes_recreates_its_marker() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_all().await;
    set.settled().await;
    let epoch = set.replica(Slot::Audio(1)).epoch();
    let slots: Vec<Slot> = set.nodes.keys().copied().collect();
    for &slot in &slots {
        set.crash(slot).await;
    }
    set.api.delete();
    set.start_all().await;
    set.settled().await;
    assert!(
        until(PERIOD * 10, || set.api.marker().is_some()).await,
        "no marker"
    );
    // Volume 0, which no volume has: no empty volume initialises from it.
    assert_eq!(set.api.marker(), Some((epoch, 0)));
    set.crash(Slot::Audio(2)).await;
    std::fs::remove_dir_all(set.dir(Slot::Audio(2))).unwrap();
    let node = set.start(Slot::Audio(2)).await;
    set.nodes.insert(Slot::Audio(2), node);
    set.settled().await;
    assert_eq!(set.epochs(), BTreeSet::from([epoch]), "one history");
}

/// A set scaled from three audio replicas to one and back, with the
/// StatefulSet's default PVC retention (the volumes of ordinals 1 and 2
/// kept), reconciles back to three voters. Ordinal 2 is down while the
/// leader removes it, so its volume still names it a voter; no leader
/// counts the returning replicas while ordinal 0 is down, and they are
/// not commitable meanwhile.
#[tokio::test]
async fn a_scale_down_and_up_with_retained_volumes_gets_its_voters_back() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_all().await;
    set.settled().await;
    let revision = write(set.replica(Slot::Audio(1)), "1", "gain=1@1").await;
    set.applied(revision).await;
    let ids: BTreeMap<Slot, NodeId> = set.nodes.iter().map(|(s, n)| (*s, n.id)).collect();

    // Ordinal 2 leads when it goes, so its volume holds a vote for itself
    // that a restart restores as leadership.
    set.lead(Slot::Audio(2)).await;

    // Scale to one: ordinal 2 goes first, the release restarts the other
    // pods with one wanted voter and the leader drops ordinals 1 and 2.
    set.crash(Slot::Audio(2)).await;
    set.voters = Voters::new(1, false).unwrap();
    for slot in [Slot::Audio(1), Slot::Audio(0)] {
        set.crash(slot).await;
        let node = set.start(slot).await;
        assert_eq!(node.plan, Plan::Resume);
        set.nodes.insert(slot, node);
        let leads = || {
            set.nodes
                .values()
                .any(|n| n.replica.raft().metrics().borrow().state == ServerState::Leader)
        };
        assert!(
            until(Duration::from_secs(10), leads).await,
            "no leader after restarting {slot:?}"
        );
    }
    let only_zero = BTreeSet::from([ids[&Slot::Audio(0)]]);
    let removed = until(Duration::from_secs(20), || {
        set.nodes.values().any(|n| {
            let metrics = n.replica.raft().metrics().borrow().clone();
            let voters: BTreeSet<NodeId> =
                metrics.membership_config.membership().voter_ids().collect();
            metrics.state == ServerState::Leader && voters == only_zero
        })
    })
    .await;
    assert!(removed, "the leader removed the unwanted voters");
    set.crash(Slot::Audio(1)).await;

    // Scale back to three: ordinals 1 and 2 come back on their kept
    // volumes before ordinal 0, which restarts with three wanted voters.
    // Without ordinal 0 no leader can count them: the removal's joint
    // configuration needed ordinal 1, so its log is ahead of ordinal 2's
    // and it refuses ordinal 2 its vote, and every configuration ordinal
    // 1 may hold needs ordinal 0 for a majority.
    set.voters = Voters::new(3, false).unwrap();
    set.crash(Slot::Audio(0)).await;
    set.start_slots(&[Slot::Audio(1), Slot::Audio(2)]).await;
    let restarted = Instant::now();
    // openraft publishes the membership it loaded once its core runs.
    let named = |slot: Slot| {
        let metrics = set.replica(slot).raft().metrics().borrow().clone();
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == ids[&slot])
    };
    assert!(
        until(Duration::from_secs(5), || named(Slot::Audio(2))).await,
        "ordinal 2's volume still names it a voter"
    );
    // Ten counting checks and more: a slow host makes fewer, never a
    // leader that counts them.
    let commitable = || {
        [Slot::Audio(1), Slot::Audio(2)]
            .into_iter()
            .any(|slot| set.replica(slot).replica().commitable)
    };
    assert!(
        !until(Duration::from_secs(1), commitable).await,
        "commitable though no leader counts it, {:?} after the start:\n{}",
        restarted.elapsed(),
        set.describe()
    );
    set.start_slots(&[Slot::Audio(0)]).await;
    let all: BTreeSet<NodeId> = ids.values().copied().collect();
    let back = set.voters_become(&all, Duration::from_secs(30)).await;
    assert!(
        back.is_some(),
        "ordinals 1 and 2 did not rejoin:\n{}",
        set.describe()
    );
    let revision = write(set.replica(Slot::Audio(2)), "2", "gain=2@2").await;
    set.applied(revision).await;
}

/// With the marker gone and the other voters, which hold the set's log,
/// unreachable rather than up, ordinal 0 on an empty volume does not start
/// a second set: they come back holding a quorum of the old one.
#[tokio::test]
async fn ordinal_0_does_not_start_a_set_while_its_peers_are_unreachable() {
    let mut set = Set::new(Voters::new(3, false).unwrap()).await;
    set.start_all().await;
    set.settled().await;
    set.crash(Slot::Audio(1)).await;
    set.crash(Slot::Audio(2)).await;
    set.crash(Slot::Audio(0)).await;
    // Deleted once no leader is left: a leader with a quorum recreates a
    // missing marker each second, which on a loaded host the crashes
    // above outlast.
    set.api.delete();
    std::fs::remove_dir_all(set.dir(Slot::Audio(0))).unwrap();
    let node = set.start(Slot::Audio(0)).await;
    let replica = node.replica.clone();
    set.nodes.insert(Slot::Audio(0), node);
    tokio::time::sleep(PERIOD * 4).await;
    assert_eq!(
        set.api.marker(),
        None,
        "a marker was created beside the old set"
    );
    assert!(
        !replica.raft().is_initialized().await.unwrap(),
        "a second set was initialised"
    );
}

/// A marker created for the StatefulSet names it as its owner, with the
/// uid the StatefulSet has, so Kubernetes deletes the marker with it and
/// a StatefulSet created again under the same name starts a new set. A
/// StatefulSet that cannot be read creates no marker; without one named,
/// the marker has no owner.
#[tokio::test]
async fn the_marker_is_owned_by_the_statefulset_named() {
    let root = harness::root("bootstrap-owner");
    std::fs::create_dir_all(&root).unwrap();
    let marker = Marker {
        cluster: 7,
        volume: 9,
    };
    let metadata = |api: &Api| api.0.lock().unwrap()[MARKER]["metadata"].clone();

    let api = Api::default();
    let owned = kube(&api, &root).await.owned_by(STATEFULSET.into());
    assert!(owned.create(marker).await.unwrap());
    assert_eq!(
        metadata(&api)["ownerReferences"],
        serde_json::json!([{
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "name": STATEFULSET,
            "uid": STATEFULSET_UID,
            "controller": false,
        }])
    );
    assert_eq!(api.marker(), Some((7, 9)));
    assert_eq!(
        owned.update_revision(STATEFULSET).await.unwrap().as_deref(),
        Some("set-audio-1")
    );

    let api = Api::default();
    let unknown = kube(&api, &root).await.owned_by("other".into());
    assert!(unknown.create(marker).await.is_err());
    assert_eq!(api.marker(), None);

    let api = Api::default();
    assert!(kube(&api, &root).await.create(marker).await.unwrap());
    assert_eq!(metadata(&api).get("ownerReferences"), None);
    let _ = std::fs::remove_dir_all(&root);
}

/// The owned marker is created only for the StatefulSet this process first
/// read: one created again under the name since, or one being deleted, is
/// refused, so a replica of a deleted set that still leads never writes
/// its cluster id into the marker of the set that replaces it.
#[tokio::test]
async fn the_owned_marker_is_refused_for_a_replaced_or_deleting_statefulset() {
    let root = harness::root("bootstrap-replaced");
    std::fs::create_dir_all(&root).unwrap();
    let marker = Marker {
        cluster: 7,
        volume: 0,
    };
    let api = Api::default();
    let owned = kube(&api, &root).await.owned_by(STATEFULSET.into());
    // The rollout's first read takes the uid.
    owned.update_revision(STATEFULSET).await.unwrap();
    *api.1.lock().unwrap() = serde_json::json!({ "uid": "5c0e9a7d-1f22-4b36-8e51-a3d0c4b7e912" });
    let refused = owned.create(marker).await.unwrap_err();
    assert!(refused.contains("was replaced"), "{refused}");
    assert_eq!(api.marker(), None);

    *api.1.lock().unwrap() = serde_json::json!({ "deletionTimestamp": "2026-01-01T00:00:00Z" });
    let refused = owned.create(marker).await.unwrap_err();
    assert!(refused.contains("being deleted"), "{refused}");
    assert_eq!(api.marker(), None);

    *api.1.lock().unwrap() = Value::Null;
    assert!(owned.create(marker).await.unwrap());
    assert_eq!(api.marker(), Some((7, 0)));
    let _ = std::fs::remove_dir_all(&root);
}
