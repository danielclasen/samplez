//! An in-process replica set: every replica's storage in its own
//! directory, openraft's RPCs routed through one [`Net`] that cuts links
//! and stalls replicas, and a fake clock per replica. Run under a paused
//! tokio clock (`#[tokio::test(start_paused = true)]`) so openraft's
//! timers and the replicas' clocks move only with the runtime's virtual
//! time.
//!
//! openraft draws its election timeouts from an unseeded random source,
//! so which replica leads differs between runs; tests assert what holds
//! whichever one does. A "crash" ([`Set::crash`]) is a shutdown inside
//! this process: what was written but not fsynced stays in the page
//! cache, so nothing here models a power loss or a lost fsync.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, ServerState};
use samplez_replica::bootstrap::{Bootstrap, Marker, Markers, Volume};
use samplez_replica::dedup::Limits;
use samplez_replica::entry::{NodeId, Origin, SessionKey};
use samplez_replica::membership::slot_of;
use samplez_replica::peer::{PeerClient, PeerReply, PeerRequest};
use samplez_replica::state_machine::{Changed, Effect, Product};
use samplez_replica::{Replica, Settings, TypeConfig, raft_config};
use samplez_service::{Owner, ReplicatedState, Role};
use tokio::time::Instant;

pub const RATE: u64 = 48_000;
pub const BLOCK: u64 = 48;

/// One value per path, set by `path=value@seq`: a seq not above the last
/// applied on its path is refused stale, as faderz orders its commands, and
/// the value a path holds already is unchanged.
#[derive(Default)]
pub struct Paths {
    pub values: BTreeMap<String, (String, u64)>,
    pub applied: u64,
}

fn parse(command: &[u8]) -> (String, String, u64) {
    let text = std::str::from_utf8(command).expect("utf-8");
    let (path, rest) = text.split_once('=').expect("path=value@seq");
    let (value, seq) = rest.split_once('@').expect("value@seq");
    (path.into(), value.into(), seq.parse().expect("seq"))
}

impl Product for Paths {
    type Change = (String, String);

    fn apply(&mut self, _origin: &Origin, command: &[u8]) -> Effect<(String, String)> {
        let (path, value, seq) = parse(command);
        if self.values.get(&path).is_some_and(|(_, last)| *last >= seq) {
            return Effect::Stale;
        }
        self.applied += 1;
        let was = self.values.insert(path.clone(), (value.clone(), seq));
        if was.is_some_and(|(was, _)| was == value) {
            return Effect::Unchanged;
        }
        Effect::Changed((path, value))
    }
    fn expire(&mut self, _session: &SessionKey) -> Effect<(String, String)> {
        Effect::Unchanged
    }
    fn forget(&mut self, _session: &SessionKey) {}
    fn delta(&self, _stamp: u64, changes: &[Changed<'_, (String, String)>]) -> String {
        // Later wins per path; every command that changed something is an
        // origin.
        let mut values = BTreeMap::new();
        let mut origins = Vec::new();
        for changed in changes {
            let (path, value) = changed.change;
            values.insert(path, value);
            if let Some(command) = changed.command {
                origins.push(format!("{}/{}", command.origin.session, command.id));
            }
        }
        let values: Vec<String> = values.iter().map(|(p, v)| format!("{p}={v}")).collect();
        format!("{} from {}", values.join(","), origins.join(","))
    }
    fn text(&self) -> String {
        serde_json::to_string(&self.values).expect("encodes")
    }
    fn save(&self) -> Vec<u8> {
        serde_json::to_vec(&(&self.values, self.applied)).expect("encodes")
    }
    fn load(&mut self, saved: &[u8]) -> Result<(), String> {
        (self.values, self.applied) = serde_json::from_slice(saved).map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub fn owner(session: &str) -> Owner {
    Owner {
        principal: "anonymous".into(),
        session: session.into(),
        role: Role::Operator,
        connection: format!("{session}:nonce:1"),
    }
}

/// A replica's CLOCK_MONOTONIC in samples: the runtime's virtual time
/// since the set started, plus an offset a test sets.
pub struct Clock {
    base: Instant,
    offset: AtomicI64,
    /// Told on the next read: a test's hook into the moment a replica
    /// stamps an entry, between its leadership check and the proposal.
    on_read: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl Clock {
    pub fn samples(&self) -> u64 {
        if let Some(read) = self.on_read.lock().expect("clock lock poisoned").take() {
            let _ = read.send(());
        }
        let elapsed = self.base.elapsed().as_nanos() as u64 * RATE / 1_000_000_000;
        elapsed.saturating_add_signed(self.offset.load(Ordering::Relaxed))
    }

    /// Resolves at the next read.
    pub fn next_read(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (read, next) = tokio::sync::oneshot::channel();
        *self.on_read.lock().expect("clock lock poisoned") = Some(read);
        next
    }

    pub fn set_offset(&self, samples: i64) {
        self.offset.store(samples, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct Links {
    rafts: BTreeMap<NodeId, Raft<TypeConfig>>,
    replicas: BTreeMap<NodeId, Arc<Replica<Paths>>>,
    /// (from, to): RPCs from `from` to `to` do not arrive.
    cut: BTreeSet<(NodeId, NodeId)>,
    /// (from, to): RPCs from `from` to `to` get no answer, as through a
    /// network that drops packets, until the caller's timeout.
    hung: BTreeSet<(NodeId, NodeId)>,
    /// RPCs to a stalled replica wait this long before it handles them.
    stalled: BTreeMap<NodeId, Duration>,
    delivered: u64,
    /// Appends from this node that carry the second step of a membership
    /// change, a configuration out of joint, do not arrive.
    hold_uniform_from: Option<NodeId>,
}

/// The links between the replicas of one set.
#[derive(Clone, Default)]
pub struct Net(Arc<Mutex<Links>>);

impl Net {
    fn links(&self) -> std::sync::MutexGuard<'_, Links> {
        self.0.lock().expect("net lock poisoned")
    }

    /// Cuts both directions between `a` and `b`.
    pub fn cut(&self, a: NodeId, b: NodeId) {
        let mut links = self.links();
        links.cut.insert((a, b));
        links.cut.insert((b, a));
    }

    pub fn cut_one_way(&self, from: NodeId, to: NodeId) {
        self.links().cut.insert((from, to));
    }

    /// Cuts `node` off from every other replica.
    pub fn isolate(&self, node: NodeId, of: impl IntoIterator<Item = NodeId>) {
        for other in of {
            if other != node {
                self.cut(node, other);
            }
        }
    }

    /// Makes every RPC between `node` and the others wait for an answer
    /// that never comes.
    pub fn hang(&self, node: NodeId, of: impl IntoIterator<Item = NodeId>) {
        let mut links = self.links();
        for other in of {
            if other != node {
                links.hung.insert((node, other));
                links.hung.insert((other, node));
            }
        }
    }

    pub fn heal(&self) {
        let mut links = self.links();
        links.cut.clear();
        links.hung.clear();
    }

    pub fn stall(&self, node: NodeId, by: Duration) {
        self.links().stalled.insert(node, by);
    }

    pub fn unstall(&self, node: NodeId) {
        self.links().stalled.remove(&node);
    }

    /// Drops `from`'s appends that carry the second step of a membership
    /// change, as a leader that goes down between the two steps leaves it.
    pub fn hold_uniform_from(&self, from: NodeId) {
        self.links().hold_uniform_from = Some(from);
    }

    /// RPCs delivered so far.
    pub fn delivered(&self) -> u64 {
        self.links().delivered
    }

    async fn route<E: std::error::Error>(
        &self,
        from: NodeId,
        to: NodeId,
    ) -> Result<Raft<TypeConfig>, RPCError<NodeId, BasicNode, E>> {
        let hung = self.links().hung.contains(&(from, to));
        if hung {
            std::future::pending::<()>().await;
        }
        let (raft, stall) = {
            let links = self.links();
            let raft = links
                .rafts
                .get(&to)
                .filter(|_| !links.cut.contains(&(from, to)))
                .cloned();
            (raft, links.stalled.get(&to).copied())
        };
        let unreachable = || {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                "{from} -> {to} down"
            ))))
        };
        let raft = raft.ok_or_else(unreachable)?;
        if let Some(stall) = stall {
            tokio::time::sleep(stall).await;
        }
        let mut links = self.links();
        if links.cut.contains(&(from, to)) || !links.rafts.contains_key(&to) {
            return Err(unreachable());
        }
        links.delivered += 1;
        Ok(raft)
    }
}

/// The peer requests of one replica, over the same links as its RPCs; a
/// harness address is the node id.
pub struct Peers {
    from: NodeId,
    net: Net,
}

impl PeerClient for Peers {
    fn call<'a>(
        &'a self,
        to: Option<NodeId>,
        addr: &'a str,
        request: PeerRequest,
        within: Duration,
    ) -> BoxFuture<'a, Result<PeerReply, String>> {
        Box::pin(async move {
            let target = addr.parse().expect("a harness address is a node id");
            let call = async {
                self.net
                    .route::<io::Error>(self.from, target)
                    .await
                    .map_err(|e| e.to_string())?;
                let replica = self.net.links().replicas.get(&target).cloned();
                let replica = replica.ok_or_else(|| format!("{target} down"))?;
                Ok(replica.handle(to, request).await)
            };
            tokio::time::timeout(within, call)
                .await
                .unwrap_or_else(|_| Err("timed out".into()))
        })
    }
}

pub struct Factory {
    from: NodeId,
    net: Net,
}

pub struct Link {
    from: NodeId,
    to: NodeId,
    net: Net,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Link;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Link {
        Link {
            from: self.from,
            to: target,
            net: self.net.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Link {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let held = self.net.links().hold_uniform_from == Some(self.from)
            && rpc.entries.iter().any(|e| {
                matches!(&e.payload,
                openraft::EntryPayload::Membership(m) if m.get_joint_config().len() == 1)
            });
        if held {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "held",
            ))));
        }
        let raft = self.net.route(self.from, self.to).await?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.net.route(self.from, self.to).await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.net.route(self.from, self.to).await?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }
}

static SETS: AtomicU64 = AtomicU64::new(0);

/// How a set is built; the defaults are the chart's.
#[derive(Clone)]
pub struct Options {
    pub voters: u64,
    pub limits: Limits,
    pub raft: openraft::Config,
    /// The voters the leader reconciles the membership with; `None` leaves
    /// it as it is.
    pub wanted: Option<samplez_replica::membership::Voters>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            voters: 3,
            limits: Limits::defaults(RATE),
            raft: raft_config("test"),
            wanted: None,
        }
    }
}

pub struct Set {
    options: Options,
    root: PathBuf,
    pub net: Net,
    clocks: BTreeMap<NodeId, Arc<Clock>>,
    base: Instant,
    markers: Arc<FakeMarkers>,
    _panics: PanicGuard,
    replicas: BTreeMap<NodeId, Arc<Replica<Paths>>>,
}

impl Set {
    /// Starts `options.voters` replicas and initializes them as one set.
    pub async fn start(options: Options) -> Set {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "replica-set-{}-{}",
            std::process::id(),
            SETS.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        let base = Instant::now();
        let clocks = (0..options.voters)
            .map(|id| {
                let clock = Clock {
                    base,
                    offset: AtomicI64::new(0),
                    on_read: Mutex::new(None),
                };
                (id, Arc::new(clock))
            })
            .collect();
        let mut set = Set {
            options,
            root,
            net: Net::default(),
            clocks,
            base,
            markers: Arc::new(FakeMarkers),
            _panics: PanicGuard::new(),
            replicas: BTreeMap::new(),
        };
        for id in 0..set.options.voters {
            set.restart(id).await;
        }
        set.replica(0)
            .initialize((0..set.options.voters).map(|id| (id, id.to_string())))
            .await
            .expect("initializes");
        set.leader().await;
        let deadline = Instant::now() + Duration::from_secs(30);
        while !set.replicas.values().all(|r| r.replica().commitable) {
            assert!(
                Instant::now() < deadline,
                "not every replica commitable in 30 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        set
    }

    pub fn ids(&self) -> impl Iterator<Item = NodeId> + use<> {
        0..self.options.voters
    }

    pub fn clock(&self, id: NodeId) -> &Clock {
        &self.clocks[&id]
    }

    pub fn replica(&self, id: NodeId) -> &Arc<Replica<Paths>> {
        self.replicas.get(&id).expect("replica running")
    }

    pub fn running(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas.keys().copied()
    }

    /// Starts `id` from what its volume holds.
    pub async fn restart(&mut self, id: NodeId) {
        let base = self.base;
        let clock = self
            .clocks
            .entry(id)
            .or_insert_with(|| {
                Arc::new(Clock {
                    base,
                    offset: AtomicI64::new(0),
                    on_read: Mutex::new(None),
                })
            })
            .clone();
        let settings = Settings {
            id,
            epoch: CLUSTER,
            limits: self.options.limits,
            block: BLOCK,
            write_timeout: Duration::from_secs(2),
            addr: id.to_string(),
            peers: self.ids().map(|id| id.to_string()).collect(),
            voters: self.options.wanted,
        };
        let replica = Replica::start(
            settings,
            self.options.raft.clone(),
            &self.root.join(id.to_string()),
            Paths::default(),
            Factory {
                from: id,
                net: self.net.clone(),
            },
            Arc::new(Peers {
                from: id,
                net: self.net.clone(),
            }),
            Arc::new(move || clock.samples()),
        )
        .await
        .expect("replica starts");
        let replica = Arc::new(replica);
        let dir = self.root.join(id.to_string());
        replica.begin(Bootstrap {
            markers: self.markers.clone(),
            volume: Volume::open(&dir).expect("volume opens"),
            slot: slot_of(id),
        });
        let mut links = self.net.links();
        links.rafts.insert(id, replica.raft().clone());
        links.replicas.insert(id, replica.clone());
        drop(links);
        self.replicas.insert(id, replica);
    }

    /// Stops `id` as a process crash would: its peers stop reaching it at
    /// once and it keeps only what its volume holds. The shutdown is
    /// graceful and in-process, so the volume keeps unsynced writes too.
    pub async fn crash(&mut self, id: NodeId) {
        self.net.links().rafts.remove(&id);
        self.net.links().replicas.remove(&id);
        if let Some(replica) = self.replicas.remove(&id) {
            replica.shutdown().await;
        }
    }

    /// The leader every running replica agrees on, waiting for one.
    pub async fn leader(&self) -> NodeId {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leaders: BTreeSet<Option<NodeId>> = self
                .replicas
                .values()
                .map(|r| r.raft().metrics().borrow().current_leader)
                .collect();
            if let [Some(leader)] = leaders.into_iter().collect::<Vec<_>>()[..]
                && self
                    .replicas
                    .get(&leader)
                    .is_some_and(|r| r.raft().metrics().borrow().state == ServerState::Leader)
            {
                return leader;
            }
            assert!(Instant::now() < deadline, "no leader agreed within 30 s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until `id` takes client writes again: within a check of a
    /// leader counting it.
    pub async fn commitable(&self, id: NodeId) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.replica(id).replica().commitable {
            assert!(Instant::now() < deadline, "{id} not commitable within 5 s");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Removes what `id`'s volume holds; for a stopped replica.
    pub fn wipe(&self, id: NodeId) {
        std::fs::remove_dir_all(self.root.join(id.to_string())).expect("wipes");
    }

    /// Waits until every running replica applied product revision
    /// `revision`.
    pub async fn applied(&self, revision: u64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self
            .replicas
            .values()
            .any(|r| ReplicatedState::revision(&**r) < revision)
        {
            assert!(
                Instant::now() < deadline,
                "revision {revision} not applied within 30 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for Set {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// The cluster id of every harness set, as its marker names it.
pub const CLUSTER: u64 = 1;

/// A marker naming a set that already runs: no harness replica is the
/// volume it names, so an empty volume joins as a learner.
pub struct FakeMarkers;

impl Markers for FakeMarkers {
    fn name(&self) -> &str {
        "harness-replica-cluster"
    }

    fn get(&self) -> BoxFuture<'_, Result<Option<Marker>, String>> {
        Box::pin(async {
            Ok(Some(Marker {
                cluster: CLUSTER,
                volume: u64::MAX,
            }))
        })
    }

    fn create(&self, _: Marker) -> BoxFuture<'_, Result<bool, String>> {
        Box::pin(async { Ok(false) })
    }
}

thread_local! {
    static PANICKED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Fails the test, when it is dropped, if a task on the test's thread
/// panicked: the current-thread runtime of `#[tokio::test]` runs every
/// spawned task there, openraft's core among them, and a panicking task
/// does not fail the test by itself.
pub struct PanicGuard(());

impl PanicGuard {
    pub fn new() -> PanicGuard {
        static HOOK: std::sync::Once = std::sync::Once::new();
        HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                PANICKED.with(|panicked| panicked.set(true));
                previous(info);
            }));
        });
        PANICKED.with(|panicked| panicked.set(false));
        PanicGuard(())
    }
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() && PANICKED.with(|panicked| panicked.get()) {
            panic!("a task panicked during the test");
        }
    }
}
