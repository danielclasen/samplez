//! openraft's RPCs and the peer requests over HTTP/1.1 on the peer
//! listener's mutual TLS, and the one-shot HTTP requests the bootstrap
//! makes to the Kubernetes API.
//!
//! Every request names the node id it is meant for, and a replica answers
//! only those meant for itself: after a volume loss a new node id answers
//! at the old one's address, and must not vote or append as the old one.
//! openraft's types carry serde only with its `serde` feature, which the
//! pinned default feature set leaves out, so they travel as the mirrors
//! the volume stores.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use futures_util::future::BoxFuture;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::http1::{self, SendRequest};
use hyper::{Method, Request, StatusCode, header};
use hyper_util::rt::TokioIo;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, SnapshotMismatch,
    Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, SnapshotMeta, SnapshotSegmentId, Vote};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use samplez_service::PeerTls;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

use crate::TypeConfig;
use crate::entry::NodeId;
use crate::peer::{LinkCounts, PeerClient, PeerReply, PeerRequest};
use crate::snapshot::{StoredEntry, StoredLogId, StoredMembershipConfig, StoredVote};

type Sender = SendRequest<Full<Bytes>>;

/// The host part of `host:port`, without the brackets of an IPv6 address.
fn host(addr: &str) -> &str {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.trim_start_matches('[').trim_end_matches(']')
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "timed out")
}

/// Opens an HTTP/1.1 connection to `addr`, over TLS when `tls` is given.
pub(crate) async fn connect(addr: &str, tls: Option<Arc<ClientConfig>>) -> io::Result<Sender> {
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(addr).await?.collect();
    connect_to(&resolved, host(addr), tls).await
}

/// Opens an HTTP/1.1 connection to the first of `resolved` that takes
/// it, over TLS to `name` when `tls` is given.
async fn connect_to(
    resolved: &[SocketAddr],
    name: &str,
    tls: Option<Arc<ClientConfig>>,
) -> io::Result<Sender> {
    let tcp = TcpStream::connect(resolved).await?;
    tcp.set_nodelay(true)?;
    match tls {
        Some(config) => {
            let name = ServerName::try_from(name.to_owned())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            handshake(TlsConnector::from(config).connect(name, tcp).await?).await
        }
        None => handshake(tcp).await,
    }
}

async fn handshake(io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) -> io::Result<Sender> {
    let (sender, connection) = http1::handshake(TokioIo::new(io))
        .await
        .map_err(io::Error::other)?;
    tokio::spawn(connection);
    Ok(sender)
}

/// One request on `sender`: the status and the whole body, refused
/// beyond `limit` bytes.
pub(crate) async fn send(
    sender: &mut Sender,
    request: Request<Full<Bytes>>,
    limit: usize,
) -> io::Result<(StatusCode, Bytes)> {
    sender.ready().await.map_err(io::Error::other)?;
    let response = sender
        .send_request(request)
        .await
        .map_err(io::Error::other)?;
    let status = response.status();
    let body = Limited::new(response.into_body(), limit)
        .collect()
        .await
        .map_err(io::Error::other)?;
    Ok((status, body.to_bytes()))
}

pub(crate) fn request(
    method: Method,
    host: &str,
    path: &str,
    body: Vec<u8>,
) -> Request<Full<Bytes>> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::HOST, host)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("a valid request")
}

/// Idle connections kept per peer; one more is closed once its request
/// is answered.
pub const MAX_IDLE: usize = 4;

/// How long a dial (name resolution, TCP connect, TLS and HTTP
/// handshakes) may take, whatever the time left to the request waiting
/// for it; and how much longer than its caller waits an exchange may
/// take to give its connection back, before the connection is closed
/// and costs a dial.
pub const DIAL_WITHIN: Duration = Duration::from_secs(1);

/// The connections to one peer address.
#[derive(Default)]
struct Peer {
    idle: Vec<Sender>,
    /// What the address resolved to, kept until a dial to it fails: a
    /// peer restarted under the same name may answer at another.
    resolved: Option<Vec<SocketAddr>>,
    dialing: bool,
    /// Dials ended, and the error of the last one when it failed.
    dials_ended: u64,
    dial_error: Option<(io::ErrorKind, String)>,
    /// Exchanges whose caller stopped waiting, each giving its connection
    /// back once answered.
    orphaned: usize,
}

impl Peer {
    fn take_idle(&mut self) -> Option<Sender> {
        std::iter::from_fn(|| self.idle.pop()).find(|sender| !sender.is_closed())
    }

    /// `sender` back in the pool, or closed beyond [`MAX_IDLE`].
    fn give_back(&mut self, sender: Sender) {
        if self.idle.len() < MAX_IDLE {
            self.idle.push(sender);
        }
    }
}

/// The pool's connections, which dials and exchanges running on after
/// their caller stopped waiting change too.
#[derive(Default)]
struct Conns {
    peers: Mutex<HashMap<String, Peer>>,
    /// Notified when a connection is given back or a dial ends.
    changed: Notify,
}

impl Conns {
    fn with<T>(&self, addr: &str, f: impl FnOnce(&mut Peer) -> T) -> T {
        let mut peers = self.peers.lock().expect("pool lock poisoned");
        f(peers.entry(addr.to_owned()).or_default())
    }
}

/// An exchange's state, changed only under the pool lock.
const RUNNING: u8 = 0;
const ABANDONED: u8 = 1;
const DONE: u8 = 2;

/// Counts an exchange whose caller stops waiting before it is done as
/// orphaned, so that the next request waits for its connection rather
/// than dial another.
struct Waiting {
    conns: Arc<Conns>,
    addr: String,
    state: Arc<AtomicU8>,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.conns.with(&self.addr, |peer| {
            if self.state.load(Ordering::Relaxed) == RUNNING {
                self.state.store(ABANDONED, Ordering::Relaxed);
                peer.orphaned += 1;
            }
        });
    }
}

/// Ends an exchange when dropped, however it ends: gives its connection
/// back when it was answered, and counts it out of the orphaned ones.
struct Ended {
    conns: Arc<Conns>,
    addr: String,
    state: Arc<AtomicU8>,
    answered: Option<Sender>,
}

impl Drop for Ended {
    fn drop(&mut self) {
        self.conns.with(&self.addr, |peer| {
            if self.state.swap(DONE, Ordering::Relaxed) == ABANDONED {
                peer.orphaned -= 1;
            }
            if let Some(sender) = self.answered.take() {
                peer.give_back(sender);
            }
        });
        self.conns.changed.notify_waiters();
    }
}

/// Connections to the other replicas, kept open between requests, at
/// most [`MAX_IDLE`] idle ones per peer.
///
/// A request that runs out of time does not cost its connection: it
/// fails at its deadline while the exchange runs on and gives the
/// connection back once answered. A request finding no idle connection
/// waits for one given back or dialed and never dials within its own
/// time: a dial runs on its own within [`DIAL_WITHIN`], one at a time
/// per peer, none while an orphaned exchange is still to give one back,
/// and its connection joins the pool whether or not anyone still waits.
/// An address is resolved on its first dial and again after a dial to
/// what it resolved to fails.
pub struct Pool {
    tls: Arc<PeerTls>,
    conns: Arc<Conns>,
    counts: Mutex<BTreeMap<NodeId, LinkCounts>>,
    max_response: usize,
}

impl Pool {
    /// A pool reading answers of up to `max_response` bytes: at least
    /// [`crate::peer::max_response`] of the engine's `delayBudgetSamples`,
    /// so that a donor's core, its delay lines' history included, fits.
    pub fn new(tls: Arc<PeerTls>, max_response: usize) -> Arc<Pool> {
        Arc::new(Pool {
            tls,
            conns: Arc::default(),
            counts: Mutex::default(),
            max_response,
        })
    }

    fn count(&self, to: NodeId, f: impl FnOnce(&mut LinkCounts)) {
        let mut counts = self.counts.lock().expect("counts lock poisoned");
        f(counts.entry(to).or_insert(LinkCounts {
            node: to,
            ..LinkCounts::default()
        }));
    }

    /// POSTs `body` to `path` on `addr`, node `to` when given, within
    /// `within`; a status other than 200 is an error.
    async fn post(
        &self,
        to: Option<NodeId>,
        addr: &str,
        path: &str,
        body: Vec<u8>,
        within: Duration,
    ) -> io::Result<Bytes> {
        let deadline = Instant::now() + within;
        let mut sender = tokio::time::timeout_at(deadline, self.ready(to, addr))
            .await
            .map_err(|_| timed_out())??;
        let request = request(Method::POST, host(addr), path, body);
        let state = Arc::new(AtomicU8::new(RUNNING));
        let exchange = tokio::spawn({
            let mut ended = Ended {
                conns: self.conns.clone(),
                addr: addr.to_owned(),
                state: state.clone(),
                answered: None,
            };
            let limit = self.max_response;
            async move {
                let sent = tokio::time::timeout_at(
                    deadline + DIAL_WITHIN,
                    send(&mut sender, request, limit),
                )
                .await
                .unwrap_or_else(|_| Err(timed_out()));
                if sent.is_ok() {
                    ended.answered = Some(sender);
                }
                // Dropping `ended` hands an answered connection back.
                drop(ended);
                sent
            }
        });
        let _waiting = Waiting {
            conns: self.conns.clone(),
            addr: addr.to_owned(),
            state,
        };
        let (status, body) = tokio::time::timeout_at(deadline, exchange)
            .await
            .map_err(|_| timed_out())?
            .map_err(io::Error::other)??;
        if status != StatusCode::OK {
            return Err(io::Error::other(format!("{addr}{path}: {status}")));
        }
        Ok(body)
    }

    /// A connection to `addr`: an idle one, else the next one given back
    /// or dialed. The error of a dial that ends while it waits, when no
    /// connection is idle.
    async fn ready(&self, to: Option<NodeId>, addr: &str) -> io::Result<Sender> {
        let mut seen = None;
        loop {
            let changed = self.conns.changed.notified();
            let mut changed = std::pin::pin!(changed);
            changed.as_mut().enable();
            let mut start = false;
            let ready = self.conns.with(addr, |peer| {
                if let Some(sender) = peer.take_idle() {
                    return Some(Ok(sender));
                }
                let seen = *seen.get_or_insert(peer.dials_ended);
                if peer.dials_ended != seen
                    && let Some((kind, error)) = &peer.dial_error
                {
                    return Some(Err(io::Error::new(*kind, error.clone())));
                }
                if !peer.dialing && peer.orphaned == 0 {
                    peer.dialing = true;
                    start = true;
                }
                None
            });
            if let Some(ready) = ready {
                return ready;
            }
            if start {
                if let Some(to) = to {
                    self.count(to, |c| c.dials += 1);
                }
                tokio::spawn(dial(self.conns.clone(), self.tls.clone(), addr.to_owned()));
            }
            changed.await;
        }
    }
}

/// Dials `addr` within [`DIAL_WITHIN`], to what it resolved to before
/// when that is known, and adds the connection to the pool.
async fn dial(conns: Arc<Conns>, tls: Arc<PeerTls>, addr: String) {
    let resolved = conns.with(&addr, |peer| peer.resolved.clone());
    let dialed = tokio::time::timeout(DIAL_WITHIN, async {
        let resolved = match resolved {
            Some(resolved) => resolved,
            None => tokio::net::lookup_host(addr.as_str()).await?.collect(),
        };
        let tls = tls.client().map_err(io::Error::other)?;
        let sender = connect_to(&resolved, host(&addr), Some(tls)).await?;
        Ok::<_, io::Error>((resolved, sender))
    })
    .await
    .unwrap_or_else(|_| Err(timed_out()));
    conns.with(&addr, |peer| {
        peer.dialing = false;
        peer.dials_ended += 1;
        match dialed {
            Ok((resolved, sender)) => {
                peer.resolved = Some(resolved);
                peer.dial_error = None;
                peer.give_back(sender);
            }
            Err(e) => {
                peer.resolved = None;
                peer.dial_error = Some((e.kind(), e.to_string()));
            }
        }
    });
    conns.changed.notify_waiters();
}

impl Pool {
    /// The idle connections kept to `addr`.
    pub fn idle(&self, addr: &str) -> usize {
        self.conns
            .peers
            .lock()
            .expect("pool lock poisoned")
            .get(addr)
            .map_or(0, |peer| peer.idle.len())
    }
}

impl PeerClient for Pool {
    fn call<'a>(
        &'a self,
        to: Option<NodeId>,
        addr: &'a str,
        request: PeerRequest,
        within: Duration,
    ) -> BoxFuture<'a, Result<PeerReply, String>> {
        Box::pin(async move {
            let report = matches!(request, PeerRequest::Report(_));
            let path = format!(
                "/peer/{}",
                to.map_or_else(|| "any".to_owned(), |id| id.to_string())
            );
            let body = serde_json::to_vec(&request).expect("encodes");
            let reply = self.post(to, addr, &path, body, within).await;
            if report
                && let (Some(to), Err(e)) = (to, &reply)
                && e.kind() == io::ErrorKind::TimedOut
            {
                self.count(to, |c| c.report_timeouts += 1);
            }
            let reply = reply.map_err(|e| e.to_string())?;
            serde_json::from_slice(&reply).map_err(|e| e.to_string())
        })
    }

    fn link_counts(&self) -> Vec<LinkCounts> {
        let counts = self.counts.lock().expect("counts lock poisoned");
        counts.values().copied().collect()
    }

    fn retain_links(&self, keep: &dyn Fn(NodeId) -> bool) {
        let mut counts = self.counts.lock().expect("counts lock poisoned");
        counts.retain(|&node, _| keep(node));
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct AppendWire {
    pub vote: StoredVote,
    pub prev: Option<StoredLogId>,
    pub entries: Vec<StoredEntry>,
    pub commit: Option<StoredLogId>,
}

impl From<&AppendEntriesRequest<TypeConfig>> for AppendWire {
    fn from(rpc: &AppendEntriesRequest<TypeConfig>) -> AppendWire {
        AppendWire {
            vote: StoredVote::from(&rpc.vote),
            prev: rpc.prev_log_id.as_ref().map(StoredLogId::from),
            entries: rpc.entries.iter().map(StoredEntry::from).collect(),
            commit: rpc.leader_commit.as_ref().map(StoredLogId::from),
        }
    }
}

impl From<AppendWire> for AppendEntriesRequest<TypeConfig> {
    fn from(wire: AppendWire) -> AppendEntriesRequest<TypeConfig> {
        AppendEntriesRequest {
            vote: wire.vote.into(),
            prev_log_id: wire.prev.map(Into::into),
            entries: wire.entries.iter().map(Into::into).collect(),
            leader_commit: wire.commit.map(Into::into),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum AppendReplyWire {
    Success,
    PartialSuccess(Option<StoredLogId>),
    Conflict,
    HigherVote(StoredVote),
}

impl From<&AppendEntriesResponse<NodeId>> for AppendReplyWire {
    fn from(reply: &AppendEntriesResponse<NodeId>) -> AppendReplyWire {
        match reply {
            AppendEntriesResponse::Success => AppendReplyWire::Success,
            AppendEntriesResponse::PartialSuccess(id) => {
                AppendReplyWire::PartialSuccess(id.as_ref().map(StoredLogId::from))
            }
            AppendEntriesResponse::Conflict => AppendReplyWire::Conflict,
            AppendEntriesResponse::HigherVote(vote) => {
                AppendReplyWire::HigherVote(StoredVote::from(vote))
            }
        }
    }
}

impl From<AppendReplyWire> for AppendEntriesResponse<NodeId> {
    fn from(wire: AppendReplyWire) -> AppendEntriesResponse<NodeId> {
        match wire {
            AppendReplyWire::Success => AppendEntriesResponse::Success,
            AppendReplyWire::PartialSuccess(id) => {
                AppendEntriesResponse::PartialSuccess(id.map(Into::into))
            }
            AppendReplyWire::Conflict => AppendEntriesResponse::Conflict,
            AppendReplyWire::HigherVote(vote) => AppendEntriesResponse::HigherVote(vote.into()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct VoteWire {
    pub vote: StoredVote,
    pub last: Option<StoredLogId>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct VoteReplyWire {
    pub vote: StoredVote,
    pub granted: bool,
    pub last: Option<StoredLogId>,
}

/// One snapshot chunk without its data, which follows it raw in the body.
#[derive(Serialize, Deserialize)]
pub(crate) struct SnapshotWire {
    pub vote: StoredVote,
    pub last_log_id: Option<StoredLogId>,
    pub membership: StoredMembershipConfig,
    pub id: String,
    pub offset: u64,
    pub done: bool,
}

/// A refusal from openraft on the other side.
#[derive(Serialize, Deserialize)]
pub(crate) enum ErrorWire {
    /// The chunk does not continue the snapshot being received; the
    /// sender starts over.
    Mismatch {
        expect: (String, u64),
        got: (String, u64),
    },
    Other(String),
}

impl ErrorWire {
    pub(crate) fn of<E: std::error::Error>(e: &RaftError<NodeId, E>) -> ErrorWire {
        ErrorWire::Other(e.to_string())
    }
}

/// The snapshot RPC's error, the mismatch kept.
pub(crate) fn snapshot_error(e: &RaftError<NodeId, InstallSnapshotError>) -> ErrorWire {
    match e {
        RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m)) => ErrorWire::Mismatch {
            expect: (m.expect.id.clone(), m.expect.offset),
            got: (m.got.id.clone(), m.got.offset),
        },
        e => ErrorWire::of(e),
    }
}

/// `u32` length of the header, the header, then the chunk's data.
pub(crate) fn frame(header: &SnapshotWire, data: &[u8]) -> Vec<u8> {
    let header = serde_json::to_vec(header).expect("encodes");
    let mut body = Vec::with_capacity(4 + header.len() + data.len());
    body.extend_from_slice(&(header.len() as u32).to_be_bytes());
    body.extend_from_slice(&header);
    body.extend_from_slice(data);
    body
}

pub(crate) fn unframe(body: &[u8]) -> Option<(SnapshotWire, &[u8])> {
    let (len, rest) = body.split_first_chunk::<4>()?;
    let (header, data) = rest.split_at_checked(u32::from_be_bytes(*len) as usize)?;
    Some((serde_json::from_slice(header).ok()?, data))
}

/// openraft's network over [`Pool`].
pub struct Network {
    pub pool: Arc<Pool>,
}

pub struct Link {
    pool: Arc<Pool>,
    to: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Link;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Link {
        Link {
            pool: self.pool.clone(),
            to: target,
            addr: node.addr.clone(),
        }
    }
}

impl Link {
    /// One RPC: unreachable when it got no answer, else openraft's answer
    /// or refusal on the other side.
    async fn call<T: for<'de> Deserialize<'de>, E: std::error::Error>(
        &self,
        rpc: &str,
        body: Vec<u8>,
        option: &RPCOption,
    ) -> Result<Result<T, ErrorWire>, RPCError<NodeId, BasicNode, E>> {
        let path = format!("/raft/{}/{rpc}", self.to);
        let reply = self
            .pool
            .post(Some(self.to), &self.addr, &path, body, option.hard_ttl())
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        serde_json::from_slice(&reply).map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

/// A refusal openraft retries like a lost answer.
fn refused<E: std::error::Error>(e: ErrorWire) -> RPCError<NodeId, BasicNode, E> {
    let detail = match e {
        ErrorWire::Other(detail) => detail,
        ErrorWire::Mismatch { .. } => "snapshot mismatch".into(),
    };
    RPCError::Network(NetworkError::new(&io::Error::other(detail)))
}

impl RaftNetwork<TypeConfig> for Link {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // Up to MAX_APPEND_BYTES of commands: encoded off the async worker.
        let encoded =
            tokio::task::spawn_blocking(move || serde_json::to_vec(&AppendWire::from(&rpc)));
        let body = encoded
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .expect("encodes");
        let reply: AppendReplyWire = self.call("append", body, &option).await?.map_err(refused)?;
        Ok(reply.into())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let header = SnapshotWire {
            vote: StoredVote::from(&rpc.vote),
            last_log_id: rpc.meta.last_log_id.as_ref().map(StoredLogId::from),
            membership: StoredMembershipConfig::of(&rpc.meta.last_membership),
            id: rpc.meta.snapshot_id.clone(),
            offset: rpc.offset,
            done: rpc.done,
        };
        let reply = self
            .call("snapshot", frame(&header, &rpc.data), &option)
            .await?;
        match reply {
            Ok(vote) => Ok(InstallSnapshotResponse {
                vote: StoredVote::into(vote),
            }),
            // The sender starts the snapshot over.
            Err(ErrorWire::Mismatch { expect, got }) => {
                let segment = |(id, offset)| SnapshotSegmentId { id, offset };
                let mismatch = SnapshotMismatch {
                    expect: segment(expect),
                    got: segment(got),
                };
                Err(RPCError::RemoteError(RemoteError::new(
                    self.to,
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(mismatch)),
                )))
            }
            Err(e) => Err(refused(e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let wire = VoteWire {
            vote: StoredVote::from(&rpc.vote),
            last: rpc.last_log_id.as_ref().map(StoredLogId::from),
        };
        let body = serde_json::to_vec(&wire).expect("encodes");
        let reply: VoteReplyWire = self.call("vote", body, &option).await?.map_err(refused)?;
        Ok(VoteResponse {
            vote: reply.vote.into(),
            vote_granted: reply.granted,
            last_log_id: reply.last.map(Into::into),
        })
    }
}

impl SnapshotWire {
    pub(crate) fn into_parts(self) -> (Vote<NodeId>, SnapshotMeta<NodeId, BasicNode>, u64, bool) {
        let meta = SnapshotMeta {
            last_log_id: self.last_log_id.map(Into::into),
            last_membership: self.membership.stored(),
            snapshot_id: self.id,
        };
        (self.vote.into(), meta, self.offset, self.done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An answer one byte beyond the cap is refused, read up to it only;
    /// one at the cap is taken.
    #[tokio::test(flavor = "current_thread")]
    async fn an_answer_beyond_the_cap_is_refused() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            for len in [crate::peer::MAX_RESPONSE, crate::peer::MAX_RESPONSE + 1] {
                let (mut tcp, _) = listener.accept().unwrap();
                let mut buf = [0u8; 4096];
                let _ = tcp.read(&mut buf);
                let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {len}\r\n\r\n");
                let _ = tcp.write_all(head.as_bytes());
                let _ = tcp.write_all(&vec![b'x'; len]);
            }
        });
        for (len, taken) in [
            (crate::peer::MAX_RESPONSE, true),
            (crate::peer::MAX_RESPONSE + 1, false),
        ] {
            let mut sender = connect(&addr, None).await.unwrap();
            let sent = send(
                &mut sender,
                request(Method::POST, "x", "/peer/any", Vec::new()),
                crate::peer::MAX_RESPONSE,
            )
            .await;
            assert_eq!(sent.is_ok(), taken, "an answer of {len} bytes");
        }
    }
}
