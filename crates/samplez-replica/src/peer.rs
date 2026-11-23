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
/// snapshot chunks and an AppendEntries of [`crate::MAX_APPEND_BYTES`]
/// of commands beside one entry of [`crate::MAX_ENTRY_BYTES`].
pub const MAX_BODY: usize = 8 << 20;

/// The largest answer a replica reads: [`MAX_BODY`] beside the largest
/// legal engine core, whose matrix of at most
/// [`samplez::MAX_COEFFICIENTS`] terms encodes each in under 128
/// bytes of JSON.
pub const MAX_RESPONSE: usize = MAX_BODY + samplez::MAX_COEFFICIENTS * 128;

/// Bytes a core spends on one sample of a delay line's history: base64
/// of its four bytes, rounded up.
const DELAY_BYTES_PER_SAMPLE: usize = 6;

/// The largest answer a replica reads from a donor whose engine holds
/// delay lines within `delay_budget_samples`
/// ([`samplez::config::Config`]'s `delayBudgetSamples`): [`MAX_RESPONSE`]
/// beside every sample of history the budget allows.
pub fn max_response(delay_budget_samples: u64) -> usize {
    let history = usize::try_from(delay_budget_samples).unwrap_or(usize::MAX);
    MAX_RESPONSE.saturating_add(history.saturating_mul(DELAY_BYTES_PER_SAMPLE))
}

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
    /// A command the writer does not order, with why, such as one asking
    /// for an index beyond the earliest of a product that drives an
    /// engine.
    Invalid(String),
    /// A leader asked for a read index that no majority acknowledged
    /// within the lease: the asker reads its own state, marked stale.
    CutOff,
}

pub type PeerReply = Result<PeerResponse, Refused>;

/// The upper bounds of the round-trip buckets [`LinkCounts::reports_answered`]
/// counts answered reports in, seconds; one more bucket takes the rest.
pub const ROUND_TRIP_BOUNDS: [f64; 5] = [0.0005, 0.001, 0.002, 0.004, 0.008];

/// What a [`PeerClient`] counted of its exchanges with one peer while it
/// is a member.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinkCounts {
    pub node: NodeId,
    /// Reports that got no answer within their time, the contact window.
    pub report_timeouts: u64,
    /// Connections dialed to it.
    pub dials: u64,
    /// Reports handed to the connection pool.
    pub reports_sent: u64,
    /// Their encoded bytes.
    pub report_bytes: u64,
    /// Reports answered, per bucket of [`ROUND_TRIP_BOUNDS`] by round trip
    /// (from the encoded report handed to the pool to its answer), the
    /// last beyond them; not cumulative.
    pub reports_answered: [u64; ROUND_TRIP_BOUNDS.len() + 1],
    /// The round trips of the answered reports, summed.
    pub report_round_trip: Duration,
    /// The CPU time of the thread encoding the reports, summed.
    pub report_encode: Duration,
}

impl LinkCounts {
    /// Counts an answered report by its round trip.
    pub fn answered(&mut self, round_trip: Duration) {
        let s = round_trip.as_secs_f64();
        let bucket = ROUND_TRIP_BOUNDS.iter().take_while(|&&b| s > b).count();
        self.reports_answered[bucket] += 1;
        self.report_round_trip += round_trip;
    }
}

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

    /// Its counts per peer, by node id; none by default.
    fn link_counts(&self) -> Vec<LinkCounts> {
        Vec::new()
    }

    /// Drops the counts of the peers `keep` refuses.
    fn retain_links(&self, _keep: &dyn Fn(NodeId) -> bool) {}

    /// The peer TLS, for the prototype's report streams.
    fn tls(&self) -> Option<Arc<PeerTls>> {
        None
    }
}

/// The peer listener's routes: `POST /raft/<node id>/{append,vote,snapshot}`
/// and `POST /peer/<node id or any>`. A request for another node id gets
/// 404, which the sender takes as unreachable.
pub fn router<P: Product>(replica: Arc<Replica<P>>) -> Router {
    Router::new()
        .route("/raft/{to}/{rpc}", post(raft::<P>))
        .route("/peer/{to}", post(peer::<P>))
        .route("/peer/{to}/stream", post(stream::<P>))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(replica)
}

/// Serves [`router`] on `tcp` with mutual TLS, re-reading the certificate,
/// key and CA every [`SECRET_RELOAD_PERIOD`], until `shutdown` resolves;
/// then closes every connection once its request in progress is answered.
pub async fn serve<P: Product>(
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
    let served = axum::serve(
        listener,
        router(replica).into_make_service_with_connect_info::<crate::proto::PeerCert>(),
    )
    .with_graceful_shutdown(shutdown)
    .await;
    reload.abort();
    served
}

fn json<T: Serialize>(value: &T) -> Response {
    (StatusCode::OK, serde_json::to_vec(value).expect("encodes")).into_response()
}

async fn raft<P: Product>(
    State(replica): State<Arc<Replica<P>>>,
    Path((to, rpc)): Path<(NodeId, String)>,
    body: Bytes,
) -> Response {
    if to != replica.id() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let raft = replica.rpcs();
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

/// The prototype's report stream: answered 101, then read until it ends.
async fn stream<P: Product>(
    State(replica): State<Arc<Replica<P>>>,
    Path(to): Path<NodeId>,
    axum::extract::ConnectInfo(cert): axum::extract::ConnectInfo<crate::proto::PeerCert>,
    mut request: axum::extract::Request,
) -> Response {
    if to != replica.id() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(contact) = replica.shared.contact.get().cloned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        if let Ok(io) = upgrade.await {
            let _ = crate::proto::receive(io, cert, contact).await;
        }
    });
    (
        StatusCode::SWITCHING_PROTOCOLS,
        [
            (axum::http::header::CONNECTION, "upgrade"),
            (axum::http::header::UPGRADE, "samplez-reports"),
        ],
    )
        .into_response()
}

async fn peer<P: Product>(
    State(replica): State<Arc<Replica<P>>>,
    Path(to): Path<String>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let to = match to.as_str() {
        "any" => None,
        id => match id.parse() {
            Ok(id) => Some(id),
            Err(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
    };
    let cpu = crate::contact::thread_cpu();
    let postcard = headers
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v == "application/postcard");
    let Some(request) = crate::proto::decode_request(&body, postcard) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if let Some(sent) = headers
        .get("x-proto-sent")
        .and_then(|v| v.to_str().ok()?.parse::<u64>().ok())
    {
        let now = crate::proto::mono_ns();
        crate::proto::sample(&crate::proto::STATS.latency, now.saturating_sub(sent));
    }
    if let (PeerRequest::Report(_), Some(contact)) = (&request, replica.shared.contact.get()) {
        contact.decoded(cpu);
    }
    json(&replica.handle(to, request).await)
}

#[cfg(test)]
mod link_tests {
    use std::time::Duration;

    use super::LinkCounts;

    /// An answer counts in the first bucket whose bound it does not
    /// exceed, a bound itself included, and beyond the last in the last.
    #[test]
    fn an_answer_counts_in_the_bucket_of_its_round_trip() {
        let mut c = LinkCounts::default();
        for us in [500, 501, 2_000, 8_001, 1_000_000] {
            c.answered(Duration::from_micros(us));
        }
        assert_eq!(c.reports_answered, [1, 1, 1, 0, 0, 2]);
        assert_eq!(c.report_round_trip, Duration::from_micros(1_011_002));
    }
}

#[cfg(test)]
mod budget_tests {
    /// A larger delay budget raises the answer limit by every sample's
    /// encoded bytes; none leaves it at the core's.
    #[test]
    fn the_answer_limit_grows_with_the_delay_budget() {
        assert_eq!(super::max_response(0), super::MAX_RESPONSE);
        assert_eq!(
            super::max_response(1 << 22),
            super::MAX_RESPONSE + (6 << 22)
        );
        assert_eq!(super::max_response(u64::MAX), usize::MAX);
    }
}
