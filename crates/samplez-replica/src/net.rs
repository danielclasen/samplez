//! openraft's RPCs and the peer requests over HTTP/1.1 on the peer
//! listener's mutual TLS.
//!
//! Every request names the node id it is meant for, and a replica answers
//! only those meant for itself: after a volume loss a new node id answers
//! at the old one's address, and must not vote or append as the old one.
//! openraft's types carry serde only with its `serde` feature, which the
//! pinned default feature set leaves out, so they travel as the mirrors
//! the volume stores.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use futures_util::future::BoxFuture;
use http_body_util::{BodyExt, Full};
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
use tokio_rustls::TlsConnector;

use crate::TypeConfig;
use crate::entry::NodeId;
use crate::peer::{PeerClient, PeerReply, PeerRequest};
use crate::snapshot::{StoredEntry, StoredLogId, StoredMembershipConfig, StoredVote};

type Sender = SendRequest<Full<Bytes>>;

/// The host part of `host:port`, without the brackets of an IPv6 address.
fn host(addr: &str) -> &str {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.trim_start_matches('[').trim_end_matches(']')
}

/// Opens an HTTP/1.1 connection to `addr`, over TLS when `tls` is given.
pub(crate) async fn connect(addr: &str, tls: Option<Arc<ClientConfig>>) -> io::Result<Sender> {
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    match tls {
        Some(config) => {
            let name = ServerName::try_from(host(addr).to_owned())
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

/// One request on `sender`: the status and the whole body.
pub(crate) async fn send(
    sender: &mut Sender,
    request: Request<Full<Bytes>>,
) -> io::Result<(StatusCode, Bytes)> {
    sender.ready().await.map_err(io::Error::other)?;
    let response = sender
        .send_request(request)
        .await
        .map_err(io::Error::other)?;
    let status = response.status();
    let body = response
        .into_body()
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

/// Connections to the other replicas, kept open between requests.
pub struct Pool {
    tls: Arc<PeerTls>,
    idle: Mutex<HashMap<String, Vec<Sender>>>,
}

impl Pool {
    pub fn new(tls: Arc<PeerTls>) -> Arc<Pool> {
        Arc::new(Pool {
            tls,
            idle: Mutex::default(),
        })
    }

    /// POSTs `body` to `path` on `addr` within `within`; a status other
    /// than 200 is an error.
    async fn post(
        &self,
        addr: &str,
        path: &str,
        body: Vec<u8>,
        within: Duration,
    ) -> io::Result<Bytes> {
        let exchange = async {
            let idle = self
                .idle
                .lock()
                .expect("pool lock poisoned")
                .get_mut(addr)
                .and_then(|idle| {
                    std::iter::from_fn(|| idle.pop()).find(|sender| !sender.is_closed())
                });
            let mut sender = match idle {
                Some(sender) => sender,
                None => {
                    let tls = self.tls.client().map_err(io::Error::other)?;
                    connect(addr, Some(tls)).await?
                }
            };
            let (status, body) =
                send(&mut sender, request(Method::POST, host(addr), path, body)).await?;
            self.idle
                .lock()
                .expect("pool lock poisoned")
                .entry(addr.to_owned())
                .or_default()
                .push(sender);
            if status != StatusCode::OK {
                return Err(io::Error::other(format!("{addr}{path}: {status}")));
            }
            Ok(body)
        };
        tokio::time::timeout(within, exchange)
            .await
            .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "timed out")))
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
            let to = to.map_or_else(|| "any".to_owned(), |id| id.to_string());
            let body = serde_json::to_vec(&request).expect("encodes");
            let reply = self
                .post(addr, &format!("/peer/{to}"), body, within)
                .await
                .map_err(|e| e.to_string())?;
            serde_json::from_slice(&reply).map_err(|e| e.to_string())
        })
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
            .post(&self.addr, &path, body, option.hard_ttl())
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
        let body = serde_json::to_vec(&AppendWire::from(&rpc)).expect("encodes");
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
