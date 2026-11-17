//! What replicas ask each other beside openraft's RPCs, and the peer
//! listener serving both, on mutual TLS ([`PeerTls`]) so only the set's
//! replicas reach it: a forwarded command is applied under the owner it
//! carries.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures_util::future::BoxFuture;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest};
use samplez_service::PeerTls;
use samplez_service::server::SECRET_RELOAD_PERIOD;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::entry::{Body, Command, NodeId, Outcome};
use crate::net::{AppendReplyWire, AppendWire, ErrorWire, VoteReplyWire, VoteWire, unframe};
use crate::report::Report;
use crate::snapshot::{StoredLogId, StoredVote};
use crate::state_machine::Product;
use crate::{Replica, net};

/// The largest request the peer listener takes: above openraft's 3 MiB
/// snapshot chunks and an AppendEntries of [`crate::MAX_PAYLOAD_ENTRIES`]
/// entries of [`crate::MAX_ENTRY_BYTES`].
pub const MAX_BODY: usize = 8 << 20;

/// The largest answer a replica reads: [`MAX_BODY`] beside the largest
/// legal engine core, whose matrix of at most
/// [`samplez::MAX_COEFFICIENTS`] terms encodes each in under 128
/// bytes of JSON.
pub const MAX_RESPONSE: usize = MAX_BODY + samplez::MAX_COEFFICIENTS * 128;

/// A client command as the replica that first received it forwards it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Forwarded {
    pub command: Command,
    /// The receiving replica's E estimate at first receipt, in samples.
    pub received: u64,
    /// What is left of the command's write timeout: the leader does not
    /// order it later.
    pub within_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerRequest {
    /// Client commands for the leader to order, in the order the sender
    /// received them.
    Forward { commands: Vec<Forwarded> },
    /// An entry the sender orders on its own account: its lease renewals,
    /// the only entry a peer may have the leader append.
    Append { body: Body, within_ms: u64 },
    /// A replica of the set `cluster` asks the leader to take it in as a
    /// learner at `addr`: an empty volume, or one no leader counts as a
    /// voter any more.
    Join {
        id: NodeId,
        addr: String,
        cluster: u64,
    },
    /// Whether the replica asked is a member of a set.
    Member,
    /// The index a read must have applied to see every committed write,
    /// once the leader confirmed with a quorum that it still leads.
    ReadIndex,
    /// A voter's side report ([`crate::contact`]).
    Report(Box<Report>),
    /// A committing audio replica's engine core, for a resync.
    Core,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerResponse {
    /// One result per forwarded command, in order.
    Forwarded(Vec<Result<Outcome, Refused>>),
    Appended,
    Joined,
    Member(bool),
    ReadIndex(u64),
    Reported,
    Core(crate::resync::CoreReply),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refused {
    /// Not the leader; the sender resends to the leader it learns of.
    NotLeader,
    /// The leader is quiescing before it exits and orders nothing more;
    /// the sender resends to the next leader, and dedup applies a command
    /// it resent once.
    Quiescing,
    /// Not known to be ordered within the time given, with why.
    Unavailable(String),
    /// Another node id answers at this address, such as a replica that
    /// lost its volume and has not joined as the new node yet.
    Misaddressed,
    /// An entry a peer may not have the leader append.
    NotForwardable,
    /// The replica asked is a member of no set.
    NotMember,
    /// A join under another cluster id than the set's, such as one read
    /// from a marker deleted and created anew since.
    OtherCluster,
    /// A join for a slot the set does not want.
    NotWanted,
    /// First received longer ago than the set deduplicates
    /// (`dedupRetentionS - ageGuard` of E): never ordered.
    Expired,
    /// A command the engine would refuse once committed, with why: never
    /// ordered.
    Invalid(String),
    /// A leader asked for a read index that no majority acknowledged
    /// within the lease: the asker reads its own state, marked stale.
    CutOff,
}

pub type PeerReply = Result<PeerResponse, Refused>;

/// Sends [`PeerRequest`]s: [`net::Pool`] over the peer listener, or a test
/// harness in process.
pub trait PeerClient: Send + Sync + 'static {
    /// Asks the replica at `addr`, which must be node `to` when given;
    /// `Err` when no answer came within `within`.
    fn call<'a>(
        &'a self,
        to: Option<NodeId>,
        addr: &'a str,
        request: PeerRequest,
        within: Duration,
    ) -> BoxFuture<'a, Result<PeerReply, String>>;
}

/// The peer listener's routes: `POST /raft/<node id>/{append,vote,snapshot}`
/// and `POST /peer/<node id or any>`. A request for another node id gets
/// 404, which the sender takes as unreachable.
pub fn router<P: Product + Sync>(replica: Arc<Replica<P>>) -> Router {
    Router::new()
        .route("/raft/{to}/{rpc}", post(raft::<P>))
        .route("/peer/{to}", post(peer::<P>))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(replica)
}

/// Serves [`router`] on `tcp` with mutual TLS, re-reading the certificate,
/// key and CA every [`SECRET_RELOAD_PERIOD`], until `shutdown` resolves;
/// then closes every connection once its request in progress is answered.
pub async fn serve<P: Product + Sync>(
    tls: Arc<PeerTls>,
    tcp: TcpListener,
    replica: Arc<Replica<P>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let listener = tls.bind(tcp)?;
    let reload = tokio::spawn(async move {
        let mut ticks = tokio::time::interval(SECRET_RELOAD_PERIOD);
        ticks.tick().await;
        loop {
            ticks.tick().await;
            // A failure is logged and counted; the previous files stay.
            let _ = tls.reload();
        }
    });
    let served = axum::serve(listener, router(replica))
        .with_graceful_shutdown(shutdown)
        .await;
    reload.abort();
    served
}

fn json<T: Serialize>(value: &T) -> Response {
    (StatusCode::OK, serde_json::to_vec(value).expect("encodes")).into_response()
}

async fn raft<P: Product + Sync>(
    State(replica): State<Arc<Replica<P>>>,
    Path((to, rpc)): Path<(NodeId, String)>,
    body: Bytes,
) -> Response {
    if to != replica.id() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let raft = replica.raft();
    let bad = || StatusCode::BAD_REQUEST.into_response();
    match rpc.as_str() {
        "append" => {
            // Up to MAX_BODY of JSON: decoded off the async worker.
            let decoded = tokio::task::spawn_blocking(move || {
                serde_json::from_slice::<AppendWire>(&body).map(AppendEntriesRequest::from)
            });
            let Ok(Ok(rpc)) = decoded.await else {
                return bad();
            };
            let reply = raft.append_entries(rpc).await;
            json(
                &reply
                    .as_ref()
                    .map(AppendReplyWire::from)
                    .map_err(ErrorWire::of),
            )
        }
        "vote" => {
            let Ok(wire) = serde_json::from_slice::<VoteWire>(&body) else {
                return bad();
            };
            let rpc = openraft::raft::VoteRequest {
                vote: wire.vote.into(),
                last_log_id: wire.last.map(Into::into),
            };
            let reply = raft.vote(rpc).await.map(|reply| VoteReplyWire {
                vote: StoredVote::from(&reply.vote),
                granted: reply.vote_granted,
                last: reply.last_log_id.as_ref().map(StoredLogId::from),
            });
            json(&reply.map_err(|e| ErrorWire::of(&e)))
        }
        "snapshot" => {
            let Some((header, data)) = unframe(&body) else {
                return bad();
            };
            let (vote, meta, offset, done) = header.into_parts();
            let rpc = InstallSnapshotRequest {
                vote,
                meta,
                offset,
                data: data.to_vec(),
                done,
            };
            let reply = raft.install_snapshot(rpc).await;
            json(
                &reply
                    .map(|reply| StoredVote::from(&reply.vote))
                    .map_err(|e| net::snapshot_error(&e)),
            )
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn peer<P: Product + Sync>(
    State(replica): State<Arc<Replica<P>>>,
    Path(to): Path<String>,
    body: Bytes,
) -> Response {
    let to = match to.as_str() {
        "any" => None,
        id => match id.parse() {
            Ok(id) => Some(id),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
    };
    let Ok(request) = serde_json::from_slice::<PeerRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    json(&replica.handle(to, request).await)
}
