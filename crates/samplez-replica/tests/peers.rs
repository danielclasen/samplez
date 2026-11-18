//! Replica sets over the peer listener on loopback with mutual TLS. Real
//! time: sockets do not follow a paused clock.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use harness::{Paths, RATE, owner};
use openraft::ServerState;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use samplez_replica::bootstrap::{Bootstrap, Volume};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::NodeId;
use samplez_replica::membership::slot_of;
use samplez_replica::net::MAX_IDLE;
use samplez_replica::net::{Network, Pool};
use samplez_replica::peer::{self, PeerClient, PeerRequest, Refused};
use samplez_replica::{MAX_COMMAND_BYTES, Replica, Settings, raft_config};
use samplez_service::config::{Tls, TlsMode};
use samplez_service::replica::{Reason, Resolution};
use samplez_service::{Metrics, PeerTls, ReplicatedState};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// A CA and a certificate for 127.0.0.1 it issued, as `<name>.crt`,
/// `<name>.key` and `<name>-ca.crt` in `root`.
fn identity(root: &std::path::Path, name: &str, ca: &CertifiedIssuer<'_, KeyPair>) -> Tls {
    let key = KeyPair::generate().unwrap();
    let cert = CertificateParams::new(vec!["127.0.0.1".into()])
        .unwrap()
        .signed_by(&key, ca)
        .unwrap();
    let (cert_file, key_file, ca_file) = (
        root.join(format!("{name}.crt")),
        root.join(format!("{name}.key")),
        root.join(format!("{name}-ca.crt")),
    );
    std::fs::write(&cert_file, cert.pem()).unwrap();
    std::fs::write(&key_file, key.serialize_pem()).unwrap();
    std::fs::write(&ca_file, ca.pem()).unwrap();
    Tls {
        mode: TlsMode::Files,
        cert_file: Some(cert_file),
        key_file: Some(key_file),
        client_ca_file: Some(ca_file),
        sans: Vec::new(),
    }
}

fn authority() -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
}

struct Node {
    replica: Arc<Replica<Paths>>,
    pool: Arc<Pool>,
    stop: oneshot::Sender<()>,
    server: JoinHandle<std::io::Result<()>>,
}

/// Three voters, node ids 1 to 3, each with its own certificate from the
/// set's CA and a peer port on loopback.
struct Set {
    root: PathBuf,
    addrs: BTreeMap<NodeId, String>,
    /// Bound for the set's lifetime, so a crashed replica's port is not
    /// taken by another socket before it restarts.
    listeners: BTreeMap<NodeId, std::net::TcpListener>,
    tls: BTreeMap<NodeId, Tls>,
    nodes: BTreeMap<NodeId, Node>,
    /// Replicas that never call an election once (re)started.
    quiet: BTreeSet<NodeId>,
    _panics: harness::PanicGuard,
}

impl Set {
    async fn start() -> Set {
        let root = harness::root("peer-set");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let ca = authority();
        let (mut addrs, mut tls, mut listeners) =
            (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
        for id in 1..=3 {
            tls.insert(id, identity(&root, &format!("node-{id}"), &ca));
            let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            tcp.set_nonblocking(true).unwrap();
            addrs.insert(id, tcp.local_addr().unwrap().to_string());
            listeners.insert(id, tcp);
        }
        let mut set = Set {
            quiet: BTreeSet::new(),
            _panics: harness::PanicGuard::new(),
            root,
            listeners,
            addrs,
            tls,
            nodes: BTreeMap::new(),
        };
        for id in 1..=3 {
            set.restart(id).await;
        }
        let members = set.addrs.clone();
        set.nodes[&1].replica.initialize(members).await.unwrap();
        set.leader().await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !set.nodes.values().all(|n| n.replica.replica().commitable) {
            assert!(Instant::now() < deadline, "not every voter ready in 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        set
    }

    /// Starts `id` from what its volume holds.
    async fn restart(&mut self, id: NodeId) {
        let tls = Arc::new(PeerTls::new(&self.tls[&id], &Metrics::default()).unwrap());
        let pool = Pool::new(tls.clone());
        let settings = Settings {
            id,
            epoch: harness::CLUSTER,
            limits: Limits::defaults(RATE),
            block: harness::BLOCK,
            // Long, for a loaded test host.
            write_timeout: Duration::from_secs(10),
            addr: self.addrs[&id].clone(),
            peers: Vec::new(),
            voters: None,
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
        if self.quiet.contains(&id) {
            raft.election_timeout_min = 60_000;
            raft.election_timeout_max = 61_000;
        }
        // Small chunks, so a snapshot of a few commands takes several.
        raft.snapshot_max_chunk_size = 64;
        let replica = Replica::start(
            settings,
            raft,
            &self.root.join(id.to_string()),
            Arc::new(Paths::default),
            Network { pool: pool.clone() },
            pool.clone(),
            Arc::new(|| 0),
        )
        .await
        .unwrap();
        let replica = Arc::new(replica);
        let tcp = TcpListener::from_std(self.listeners[&id].try_clone().unwrap()).unwrap();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(peer::serve(tls, tcp, replica.clone(), async {
            let _ = stopped.await;
        }));
        replica.begin(Bootstrap {
            markers: Arc::new(harness::FakeMarkers),
            volume: Volume::open(&self.root.join(id.to_string())).unwrap(),
            slot: slot_of(id),
        });
        self.nodes.insert(
            id,
            Node {
                replica,
                pool,
                stop,
                server,
            },
        );
    }

    async fn crash(&mut self, id: NodeId) {
        let node = self.nodes.remove(&id).expect("running");
        let _ = node.stop.send(());
        node.replica.shutdown().await;
        let _ = node.server.await;
    }

    fn replica(&self, id: NodeId) -> &Arc<Replica<Paths>> {
        &self.nodes[&id].replica
    }

    /// The leader every running replica agrees on.
    async fn leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for (&id, node) in &self.nodes {
                if node.replica.raft().metrics().borrow().state == ServerState::Leader
                    && self
                        .nodes
                        .values()
                        .all(|n| n.replica.raft().metrics().borrow().current_leader == Some(id))
                {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "no leader agreed in 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn applied(&self, index: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.nodes.values().any(|n| n.replica.revision() < index) {
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

/// Every replica takes writes over the network: a follower's reach the
/// leader with their owner, and a resend through another replica finds
/// the stored result.
#[tokio::test]
async fn voters_replicate_and_forward_writes_over_mutual_tls() {
    let set = Set::start().await;
    let mut last = 0;
    for id in 1..=3 {
        let ack = set
            .replica(id)
            .submit(
                &owner("s"),
                &id.to_string(),
                Bytes::from(format!("gain={id}@{id}")),
            )
            .await
            .expect("committed");
        assert_eq!(ack.resolution, Resolution::Applied, "through {id}");
        last = ack.revision;
    }
    set.applied(last).await;
    for id in 1..=3 {
        let replica = set.replica(id);
        assert_eq!(replica.store().with_state(|sm| sm.product.applied), 3);
        let resent = replica
            .submit(&owner("s"), "1", Bytes::from("gain=1@1"))
            .await
            .expect("answered");
        assert_eq!(resent.resolution, Resolution::Duplicate, "through {id}");
    }
}

/// A replica answers only requests for its own node id, and only replicas
/// with a certificate from the set's CA reach it.
#[tokio::test]
async fn only_the_set_reaches_a_replica_and_only_under_its_own_id() {
    let set = Set::start().await;
    let (pool, addr) = (&set.nodes[&2].pool, &set.addrs[&1]);
    let within = Duration::from_secs(1);
    let asked = pool
        .call(Some(9), addr, PeerRequest::ReadIndex, within)
        .await;
    assert_eq!(asked, Ok(Err(Refused::Misaddressed)));
    let asked = pool
        .call(Some(1), addr, PeerRequest::ReadIndex, within)
        .await;
    assert!(
        matches!(asked, Ok(Ok(_)) | Ok(Err(Refused::NotLeader))),
        "{asked:?}"
    );

    let stranger = identity(&set.root, "stranger", &authority());
    let stranger = Pool::new(Arc::new(
        PeerTls::new(&stranger, &Metrics::default()).unwrap(),
    ));
    let refused = stranger
        .call(Some(1), addr, PeerRequest::ReadIndex, within)
        .await;
    assert!(refused.is_err(), "{refused:?}");
}

/// A follower that was down while the leader purged the entries it lacks
/// catches up by a snapshot sent in several chunks over the peer
/// listener.
#[tokio::test]
async fn a_snapshot_is_installed_over_the_peer_listener() {
    let mut set = Set::start().await;
    let leader = set.leader().await;
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    set.crash(follower).await;
    let mut last = 0;
    for n in 1..=3 {
        last = set
            .replica(leader)
            .submit(
                &owner("s"),
                &n.to_string(),
                Bytes::from(format!("gain={n}@{n}")),
            )
            .await
            .expect("committed")
            .revision;
    }
    set.applied(last).await;
    // `last` is a product revision; snapshots and purges count log ids.
    // Only the leader purges: openraft holds a leader's purge back while
    // replication still reads the range, and does not on a follower, so a
    // purge on one that is elected meanwhile can race its replication.
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

    // A follower whose timer fires while it installs the snapshot would
    // depose the leader with its term, openraft having no pre-vote; the
    // next leader may hold the log the purge removed.
    set.quiet.insert(follower);
    set.restart(follower).await;
    set.applied(last).await;
    let installed = set.replica(follower);
    // The store applies the snapshot before openraft's metrics show it.
    let deadline = Instant::now() + Duration::from_secs(5);
    while installed.raft().metrics().borrow().snapshot.is_none() {
        assert!(Instant::now() < deadline, "no snapshot installed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(installed.store().with_state(|sm| sm.product.applied), 3);
}

/// Many requests to one peer at once leave no more than a few idle
/// connections open to it.
#[tokio::test]
async fn idle_connections_to_a_peer_are_bounded() {
    let set = Set::start().await;
    let (pool, addr) = (&set.nodes[&2].pool, &set.addrs[&1]);
    let calls = (0..32).map(|_| {
        pool.call(
            Some(1),
            addr,
            PeerRequest::ReadIndex,
            Duration::from_secs(2),
        )
    });
    for answer in futures_util::future::join_all(calls).await {
        assert!(answer.is_ok(), "{answer:?}");
    }
    assert!(pool.idle(addr) <= MAX_IDLE, "{} idle", pool.idle(addr));
    assert!(pool.idle(addr) > 0);
}

/// A command too large to replicate in time is refused, and a follower
/// that missed many of the largest commands catches up: no AppendEntries
/// outlasts the heartbeat interval it must complete in.
#[tokio::test]
async fn a_large_command_neither_wedges_nor_slows_replication() {
    let mut set = Set::start().await;
    let leader = set.leader().await;
    let mut big = b"big=".to_vec();
    big.extend(std::iter::repeat_n(b'x', 1 << 20));
    big.extend_from_slice(b"@1");
    let refused = set
        .replica(leader)
        .submit(&owner("s"), "big", Bytes::from(big))
        .await
        .expect_err("too large");
    assert_eq!(refused.reason, Reason::TooLarge);

    let behind = (1..=3).find(|&id| id != leader).unwrap();
    set.crash(behind).await;
    let largest = |n: usize| format!("p{n}={}@1", "x".repeat(MAX_COMMAND_BYTES - 16));
    let mut last = 0;
    for n in 0..32 {
        last = set
            .replica(leader)
            .submit(&owner("s"), &n.to_string(), Bytes::from(largest(n)))
            .await
            .expect("committed")
            .revision;
    }
    set.restart(behind).await;
    set.applied(last).await;
    // Takes writes once a check found a leader counting it.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !set.replica(behind).replica().commitable {
        assert!(Instant::now() < deadline, "not commitable within 10 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let small = set
        .replica(behind)
        .submit(&owner("s"), "small", Bytes::from("gain=1@1"))
        .await
        .expect("committed");
    assert_eq!(small.resolution, Resolution::Applied);
}
