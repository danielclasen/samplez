//! Replica sets over the peer listener on loopback with mutual TLS. Real
//! time: sockets do not follow a paused clock.

mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use axum::body::Bytes;
use axum::serve::ListenerExt;
use harness::{Paths, RATE, owner, raft_config};
use openraft::ServerState;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use samplez_replica::bootstrap::{Bootstrap, Volume};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::NodeId;
use samplez_replica::membership::slot_of;
use samplez_replica::net::{DIAL_WITHIN, MAX_IDLE};
use samplez_replica::net::{Network, Pool};
use samplez_replica::peer::{self, PeerClient, PeerRequest, PeerResponse, Refused};
use samplez_replica::report::{Own, Report};
use samplez_replica::{MAX_COMMAND_BYTES, Replica, Settings};
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
        let pool = Pool::new(tls.clone(), samplez_replica::peer::MAX_RESPONSE);
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
    let stranger = Pool::new(
        Arc::new(PeerTls::new(&stranger, &Metrics::default()).unwrap()),
        samplez_replica::peer::MAX_RESPONSE,
    );
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

/// A report's time in the tests below, the contact window: 8 ms by
/// default, longer here for a loaded test host.
const REPORT_WITHIN: Duration = Duration::from_millis(50);

/// Every other network step of the tests below finishes within this.
const LIMIT: Duration = Duration::from_secs(5);

fn report() -> PeerRequest {
    PeerRequest::Report(Box::new(Report {
        from: 2,
        seq: 0,
        sent_at: 0,
        echo: None,
        committed: None,
        own: Own::default(),
        heard: Vec::new(),
        listed: Vec::new(),
        unlisted: None,
        estimate: None,
        slack: Vec::new(),
    }))
}

/// A peer, node 1, on loopback with mutual TLS, answering every request
/// `Reported`, request `slow` (counted from 1) only after `delay`, and
/// counting the connections it accepted. It serves from `after` past the
/// bind on: a dial before that connects and waits in the TLS handshake.
struct FakePeer {
    root: PathBuf,
    addr: String,
    pool: Arc<Pool>,
    accepted: Arc<AtomicUsize>,
}

impl FakePeer {
    async fn start(slow: usize, delay: Duration, after: Duration) -> FakePeer {
        let root = harness::root("fake-peer");
        std::fs::create_dir_all(&root).unwrap();
        let ca = authority();
        let tls = |name| {
            Arc::new(PeerTls::new(&identity(&root, name, &ca), &Metrics::default()).unwrap())
        };
        let (server, client) = (tls("server"), tls("client"));
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap().to_string();
        let requests = Arc::new(AtomicUsize::new(0));
        let reply = move |_: Bytes| {
            let n = requests.fetch_add(1, SeqCst) + 1;
            async move {
                if n == slow {
                    tokio::time::sleep(delay).await;
                }
                serde_json::to_vec(&Ok::<_, Refused>(PeerResponse::Reported)).unwrap()
            }
        };
        let router = axum::Router::new().route("/peer/{to}", axum::routing::post(reply));
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let listener = server.bind(tcp).unwrap().tap_io(move |_| {
                counted.fetch_add(1, SeqCst);
            });
            let _ = axum::serve(listener, router).await;
        });
        FakePeer {
            root,
            addr,
            pool: Pool::new(client, peer::MAX_RESPONSE),
            accepted,
        }
    }
}

impl Drop for FakePeer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A report answered after its time counts as missed and leaves its
/// connection to the reports after it: they take it once the late answer
/// is in, and nothing dials again.
#[tokio::test]
async fn a_late_answer_keeps_its_connection() {
    let peer = FakePeer::start(2, 2 * REPORT_WITHIN, Duration::ZERO).await;
    let (pool, addr) = (&peer.pool, peer.addr.as_str());
    let reported = Ok(Ok(PeerResponse::Reported));
    // The first report dials, with time for that on a loaded host.
    let first = pool.call(Some(1), addr, report(), LIMIT).await;
    assert_eq!(first, reported);
    let late = pool.call(Some(1), addr, report(), REPORT_WITHIN).await;
    assert!(late.is_err(), "{late:?}");
    for _ in 0..3 {
        let next = pool.call(Some(1), addr, report(), LIMIT).await;
        assert_eq!(next, reported);
    }
    assert_eq!(peer.accepted.load(SeqCst), 1, "connections accepted");
    let [c] = pool.link_counts()[..] else {
        panic!("one peer counted")
    };
    assert_eq!((c.node, c.report_timeouts, c.dials), (1, 1, 1));
    // The late answer reached the pool, not the report's sender.
    assert_eq!(c.reports_sent, 5);
    assert_eq!(c.reports_answered.iter().sum::<u64>(), 4);
}

/// Per peer, the pool counts every report handed to it with its encoded
/// bytes and the time encoding it took, and every answered one by its
/// round trip: a report answered after 3 ms is beyond the 2 ms bucket.
#[tokio::test]
async fn reports_are_counted_with_their_bytes_and_round_trips() {
    let delay = Duration::from_millis(3);
    let peer = FakePeer::start(2, delay, Duration::ZERO).await;
    let (pool, addr) = (&peer.pool, peer.addr.as_str());
    for _ in 0..3 {
        let answered = pool.call(Some(1), addr, report(), LIMIT).await;
        assert_eq!(answered, Ok(Ok(PeerResponse::Reported)));
    }
    let [c] = pool.link_counts()[..] else {
        panic!("one peer counted")
    };
    let bytes = serde_json::to_vec(&report()).unwrap().len() as u64;
    assert_eq!((c.reports_sent, c.report_bytes), (3, 3 * bytes));
    assert_eq!(c.reports_answered.iter().sum::<u64>(), 3);
    assert_eq!(peer::ROUND_TRIP_BOUNDS[2], 0.002);
    let within_2ms: u64 = c.reports_answered[..3].iter().sum();
    assert!(within_2ms <= 2, "the delayed report within 2 ms: {c:?}");
    assert!(c.report_round_trip >= delay, "{c:?}");
    assert!(c.report_encode > Duration::ZERO, "{c:?}");
}

/// A peer slower to connect than a report's time, though within
/// [`DIAL_WITHIN`], is dialed once: the reports missed meanwhile do not
/// dial again, and the connection serves the first report after it.
#[tokio::test]
async fn a_slow_dial_is_dialed_once_and_serves_the_next_report() {
    let after = 6 * REPORT_WITHIN;
    assert!(after < DIAL_WITHIN);
    let peer = FakePeer::start(0, Duration::ZERO, after).await;
    let (pool, addr) = (&peer.pool, peer.addr.as_str());
    let deadline = Instant::now() + LIMIT;
    let mut missed = 0;
    loop {
        match pool.call(Some(1), addr, report(), REPORT_WITHIN).await {
            Ok(Ok(PeerResponse::Reported)) => break,
            answered => assert!(answered.is_err(), "{answered:?}"),
        }
        missed += 1;
        assert!(Instant::now() < deadline, "no report answered in 5 s");
    }
    assert!(missed > 0, "the dial outlasted a report");
    assert_eq!(peer.accepted.load(SeqCst), 1, "connections accepted");
    let [c] = pool.link_counts()[..] else {
        panic!("one peer counted")
    };
    assert_eq!((c.node, c.report_timeouts, c.dials), (1, missed, 1));
}

/// Every replica exposes, per peer labelled `node`, the reports that
/// got no answer in time, the connections it dialed, the reports sent
/// with their bytes and encoding time, and those answered by round trip,
/// no report counted both answered and timed out; and the contact loop's
/// periods and CPU time.
#[tokio::test]
async fn the_exposition_carries_report_timeouts_and_dials_per_peer() {
    use samplez_replica::contact::{SystemClocks, Timing};
    let set = Set::start().await;
    for id in 1..=3 {
        let timing = Timing::defaults(harness::BLOCK);
        set.replica(id)
            .contact(timing, Arc::new(SystemClocks), None)
            .unwrap();
    }
    let leader = set.leader().await;
    // Reports flow every 2 ms once contact runs.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let samples = set.replica(leader).replica_metrics();
    let find = |name: &str, labels: &[(&str, String)]| {
        samples
            .iter()
            .find(|m| m.name == name && m.labels == labels)
            .unwrap_or_else(|| panic!("no {name} sample labelled {labels:?}"))
            .value
    };
    for peer in (1..=3).filter(|&id| id != leader) {
        let of = |name: &str| find(name, &[("node", peer.to_string())]);
        assert!(of("samplez_replica_peer_dials_total") >= 1.0);
        let sent = of("samplez_replica_peer_reports_sent_total");
        assert!(sent >= 1.0, "{sent}");
        assert!(of("samplez_replica_peer_report_bytes_total") > sent);
        assert!(of("samplez_replica_peer_report_encode_cpu_seconds_total") > 0.0);
        assert!(of("samplez_replica_peer_report_round_trip_seconds_total") > 0.0);
        let answered = |le: &str| {
            find(
                "samplez_replica_peer_reports_answered_total",
                &[("node", peer.to_string()), ("le", le.to_owned())],
            )
        };
        // A report is answered, times out at contactWindowMs, which a
        // loaded host can make it do, or is still in flight.
        let all = answered("+Inf");
        let timeouts = of("samplez_replica_peer_report_timeouts_total");
        assert!(all >= 1.0, "{all} of {sent} answered");
        assert!(
            all + timeouts <= sent,
            "{all} answered, {timeouts} timed out of {sent}"
        );
        assert!(answered("0.0005") <= answered("0.008"));
        assert!(answered("0.008") <= all);
    }
    assert!(find("samplez_replica_contact_ticks_total", &[]) >= 1.0);
    let cpu = |part: &str| {
        find(
            "samplez_replica_contact_cpu_seconds_total",
            &[("part", part.to_owned())],
        )
    };
    assert!(cpu("tick") > 0.0);
    assert!(cpu("decisions") > 0.0, "the leader decides");
    assert!(cpu("decode") > 0.0);
    assert!(cpu("receive") > 0.0);
}

/// Prototype measurement, not a test: three replicas over mutual TLS with
/// contact on, 2 s settling, then 10 s measured. Prints per-part CPU, the
/// process CPU, report counts and sizes, and delivery and round-trip
/// latency quantiles. The variant comes from the SAMPLEZ_PROTO_*
/// environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proto_measure() {
    use samplez_replica::contact::{SystemClocks, Timing};
    use samplez_replica::proto::{STATS, codec, pad, transport};
    use std::sync::atomic::Ordering::Relaxed;
    fn rusage() -> (f64, f64) {
        // SAFETY: an all-zero rusage is valid; getrusage writes it.
        let mut r: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
        let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
        (t(r.ru_utime), t(r.ru_stime))
    }
    let set = Set::start().await;
    for id in 1..=3 {
        set.replica(id)
            .contact(
                Timing::defaults(harness::BLOCK),
                Arc::new(SystemClocks),
                None,
            )
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let total = |set: &Set| {
        let mut m = BTreeMap::<String, f64>::new();
        for id in 1..=3 {
            for s in set.replica(id).replica_metrics() {
                if s.name.starts_with("samplez_replica_peer_")
                    || s.name.starts_with("samplez_replica_contact")
                {
                    let labels: Vec<_> = s.labels.iter().filter(|l| l.0 != "node").collect();
                    *m.entry(format!("{}{labels:?}", s.name)).or_default() += s.value;
                }
            }
        }
        m
    };
    let counters = || {
        [
            STATS.sent.load(Relaxed),
            STATS.bytes.load(Relaxed),
            STATS.encode_ns.load(Relaxed),
            STATS.skipped.load(Relaxed),
            STATS.dials.load(Relaxed),
        ]
    };
    for s in &samplez_replica::proto::SEGMENTS {
        s.store(0, Relaxed);
    }
    STATS.latency.lock().unwrap().clear();
    STATS.round_trip.lock().unwrap().clear();
    let (a, c0, k0, t0) = (total(&set), rusage(), counters(), Instant::now());
    tokio::time::sleep(Duration::from_secs(10)).await;
    let (b, c1, k1, secs) = (
        total(&set),
        rusage(),
        counters(),
        t0.elapsed().as_secs_f64(),
    );
    let mut latency = std::mem::take(&mut *STATS.latency.lock().unwrap());
    let mut round_trip = std::mem::take(&mut *STATS.round_trip.lock().unwrap());
    println!(
        "PROTO variant transport={:?} codec={:?} pad={} window_s={secs:.3}",
        transport(),
        codec(),
        pad()
    );
    for (k, v) in &b {
        let d = v - a.get(k).copied().unwrap_or(0.0);
        if d != 0.0 {
            println!("PROTO metric {k} {d:.6}");
        }
    }
    let names = ["sent", "bytes", "encode_ns", "skipped", "dials"];
    for (i, n) in names.iter().enumerate() {
        println!("PROTO stats {n} {}", k1[i] - k0[i]);
    }
    println!(
        "PROTO process_cpu_s user {:.4} system {:.4}",
        c1.0 - c0.0,
        c1.1 - c0.1
    );
    let q = |v: &mut Vec<u64>, name: &str| {
        v.sort_unstable();
        if v.is_empty() {
            println!("PROTO {name} n 0");
            return;
        }
        let at = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize] as f64 / 1e3;
        println!(
            "PROTO {name}_us n {} p50 {:.1} p99 {:.1} p999 {:.1} max {:.1}",
            v.len(),
            at(0.5),
            at(0.99),
            at(0.999),
            at(1.0)
        );
    };
    for (i, name) in ["state", "listed", "heard", "offered", "carry", "assemble"]
        .iter()
        .enumerate()
    {
        let ns = samplez_replica::proto::SEGMENTS[i].load(Relaxed);
        println!("PROTO segment {name} total_ns {ns}");
    }
    q(&mut latency, "delivery");
    q(&mut round_trip, "round_trip");
}
