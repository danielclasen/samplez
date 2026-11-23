//! Prototype, not for merging: report transport and encoding variants,
//! chosen by environment, and what they cost.
//!
//! `SAMPLEZ_PROTO_TRANSPORT=stream` sends reports on one long-lived
//! upgraded connection per peer, length-prefixed frames one way, written
//! from the contact loop; otherwise each report is an HTTP POST.
//! `SAMPLEZ_PROTO_CODEC=postcard` encodes reports with postcard, else JSON.
//! `SAMPLEZ_PROTO_PAD=1` makes a replica without an engine report five
//! outputs' statuses and page records, as one with an engine does.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use futures_util::FutureExt;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use samplez::blocks::{BlockStatus, Cause, HEALTHY, Kind, Range, Standing};
use samplez_service::PeerTls;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::entry::NodeId;
use crate::peer::PeerRequest;
use crate::report::{Own, Page, Report, Run};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Http,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Json,
    Postcard,
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

pub fn transport() -> Transport {
    static T: OnceLock<Transport> = OnceLock::new();
    *T.get_or_init(|| match env("SAMPLEZ_PROTO_TRANSPORT").as_str() {
        "stream" => Transport::Stream,
        _ => Transport::Http,
    })
}

pub fn codec() -> Codec {
    static C: OnceLock<Codec> = OnceLock::new();
    *C.get_or_init(|| match env("SAMPLEZ_PROTO_CODEC").as_str() {
        "postcard" => Codec::Postcard,
        _ => Codec::Json,
    })
}

/// `SAMPLEZ_PROTO_BASE=1`: reports and copies as before page records were
/// carried until confirmed.
pub fn base() -> bool {
    static B: OnceLock<bool> = OnceLock::new();
    *B.get_or_init(|| env("SAMPLEZ_PROTO_BASE") == "1")
}

/// Thread CPU per part of a contact tick, ns: state, listed, heard,
/// offered, carry, assemble.
pub static SEGMENTS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];

/// Adds the thread CPU since `*at` to segment `i` and moves `*at` on.
pub fn lap(i: usize, at: &mut std::time::Duration) {
    let now = crate::contact::thread_cpu();
    SEGMENTS[i].fetch_add(now.saturating_sub(*at).as_nanos() as u64, Ordering::Relaxed);
    *at = now;
}

pub fn pad() -> bool {
    static P: OnceLock<bool> = OnceLock::new();
    *P.get_or_init(|| env("SAMPLEZ_PROTO_PAD") == "1")
}

/// Nanoseconds on a monotonic clock shared by every replica of the
/// process.
pub fn mono_ns() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    BASE.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// What the reports of every replica in the process cost, both transports.
pub struct Stats {
    pub sent: AtomicU64,
    pub bytes: AtomicU64,
    pub encode_ns: AtomicU64,
    /// Reports not written: the connection busy, absent or dialing.
    pub skipped: AtomicU64,
    pub dials: AtomicU64,
    /// Sender write to receiver decode, ns.
    pub latency: Mutex<Vec<u64>>,
    /// HTTP only: handed to the pool to answered, ns.
    pub round_trip: Mutex<Vec<u64>>,
}

pub static STATS: Stats = Stats {
    sent: AtomicU64::new(0),
    bytes: AtomicU64::new(0),
    encode_ns: AtomicU64::new(0),
    skipped: AtomicU64::new(0),
    dials: AtomicU64::new(0),
    latency: Mutex::new(Vec::new()),
    round_trip: Mutex::new(Vec::new()),
};

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

pub fn sample(to: &Mutex<Vec<u64>>, ns: u64) {
    to.lock().unwrap().push(ns);
}

/// A report request body in the configured codec, and its content type.
pub fn encode_request(request: &PeerRequest) -> (Vec<u8>, &'static str) {
    match codec() {
        Codec::Json => (
            serde_json::to_vec(request).expect("encodes"),
            "application/json",
        ),
        Codec::Postcard => (
            postcard::to_allocvec(request).expect("encodes"),
            "application/postcard",
        ),
    }
}

pub fn decode_request(body: &[u8], postcard: bool) -> Option<PeerRequest> {
    if postcard {
        postcard::from_bytes(body).ok()
    } else {
        serde_json::from_slice(body).ok()
    }
}

fn encode_report(report: &Report) -> Vec<u8> {
    match codec() {
        Codec::Json => serde_json::to_vec(report).expect("encodes"),
        Codec::Postcard => postcard::to_allocvec(report).expect("encodes"),
    }
}

fn decode_report(body: &[u8]) -> Option<Report> {
    match codec() {
        Codec::Json => serde_json::from_slice(body).ok(),
        Codec::Postcard => postcard::from_bytes(body).ok(),
    }
}

/// Five outputs' statuses and four page records each, as an engine
/// reports them.
pub fn pad_own(own: &mut Own, k: u64, seed: u64) {
    let st = |k: u64| BlockStatus {
        k,
        kind: Kind::Committed,
        range: Range::Whole,
        through: false,
        rank: 0,
        cause: Cause::None,
        on_silence_of: 0,
        digest: Some(HEALTHY),
    };
    let r = |x: u64| x.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ seed;
    own.statuses = (0..5)
        .map(|_| {
            vec![
                Run {
                    status: st(k - 22),
                    count: 22,
                },
                Run {
                    status: BlockStatus {
                        kind: Kind::Held,
                        ..st(k)
                    },
                    count: 1,
                },
            ]
        })
        .collect();
    own.standing = vec![Standing::Committing { from: k * 48 }; 5];
    let page = k * 48 / 1024;
    own.pages = (0..5u32)
        .flat_map(|o| {
            (0..4u64).map(move |p| Page {
                output: o,
                page: page - 4 + p,
                hash: r(u64::from(o) * 8 + p + page),
                fingerprint: r(1000 + u64::from(o) * 8 + p + page),
                committed: true,
                degraded: false,
                digest: Some(r(2000 + u64::from(o) * 8 + p + page)),
            })
        })
        .collect();
}

type Conn = TokioIo<Upgraded>;

/// One peer's report stream.
#[derive(Default)]
struct Link {
    addr: String,
    conn: Option<Conn>,
    dial: Option<oneshot::Receiver<io::Result<Conn>>>,
    /// The frame being written and how much of it is.
    pending: Vec<u8>,
    off: usize,
}

/// The contact loop's report streams, one per peer.
pub struct Streams {
    tls: Arc<PeerTls>,
    links: BTreeMap<NodeId, Link>,
}

impl Streams {
    pub fn new(tls: Arc<PeerTls>) -> Streams {
        Streams {
            tls,
            links: BTreeMap::new(),
        }
    }

    pub fn retain(&mut self, keep: impl Fn(NodeId) -> bool) {
        self.links.retain(|&id, _| keep(id));
    }

    /// Writes `report` to `to` if its stream can take it now, else drops
    /// it: the next period's report replaces it. Dials in the background
    /// when the stream is down.
    pub fn send(&mut self, to: NodeId, addr: &str, report: &Report) {
        let link = self.links.entry(to).or_default();
        if link.addr != addr {
            *link = Link {
                addr: addr.to_owned(),
                ..Link::default()
            };
        }
        if let Some(rx) = &mut link.dial {
            match rx.try_recv() {
                Ok(Ok(conn)) => {
                    link.conn = Some(conn);
                    link.dial = None;
                }
                Ok(Err(_)) | Err(oneshot::error::TryRecvError::Closed) => link.dial = None,
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }
        if link.conn.is_none() {
            if link.dial.is_none() {
                let (tx, rx) = oneshot::channel();
                let (tls, addr) = (self.tls.clone(), addr.to_owned());
                add(&STATS.dials, 1);
                tokio::spawn(async move {
                    let dialed = tokio::time::timeout(
                        crate::net::DIAL_WITHIN,
                        crate::net::upgrade(&addr, tls, &format!("/peer/{to}/stream")),
                    )
                    .await
                    .unwrap_or_else(|_| Err(io::Error::from(io::ErrorKind::TimedOut)));
                    let _ = tx.send(dialed.map(TokioIo::new));
                });
                link.dial = Some(rx);
            }
            add(&STATS.skipped, 1);
            return;
        }
        if link.off < link.pending.len() && !link.write() {
            add(&STATS.skipped, 1);
            return;
        }
        let cpu = crate::contact::thread_cpu();
        let body = encode_report(report);
        add(
            &STATS.encode_ns,
            crate::contact::thread_cpu().saturating_sub(cpu).as_nanos() as u64,
        );
        add(&STATS.sent, 1);
        add(&STATS.bytes, body.len() as u64);
        let mut frame = Vec::with_capacity(12 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&mono_ns().to_be_bytes());
        frame.extend_from_slice(&body);
        link.pending = frame;
        link.off = 0;
        link.write();
    }
}

impl Link {
    /// Writes what the socket takes now, without waiting; false while
    /// some of the frame is left. A failed write drops the connection.
    fn write(&mut self) -> bool {
        let Some(conn) = &mut self.conn else {
            return false;
        };
        let mut failed = false;
        while self.off < self.pending.len() {
            match conn.write(&self.pending[self.off..]).now_or_never() {
                Some(Ok(0)) | Some(Err(_)) => {
                    failed = true;
                    break;
                }
                Some(Ok(n)) => self.off += n,
                None => break,
            }
        }
        if !failed && let Some(Err(_)) = conn.flush().now_or_never() {
            failed = true;
        }
        if failed {
            self.conn = None;
            self.pending.clear();
            self.off = 0;
            return false;
        }
        self.off == self.pending.len()
    }
}

/// The client certificate of a peer connection.
#[derive(Clone)]
pub struct PeerCert(pub Option<rustls::pki_types::CertificateDer<'static>>);

impl
    axum::extract::connect_info::Connected<
        axum::serve::IncomingStream<'_, samplez_service::tls::TlsListener>,
    > for PeerCert
{
    fn connect_info(
        stream: axum::serve::IncomingStream<'_, samplez_service::tls::TlsListener>,
    ) -> PeerCert {
        let certs = stream.io().get_ref().1.peer_certificates();
        PeerCert(
            certs
                .and_then(|c| c.first())
                .map(|c| c.clone().into_owned()),
        )
    }
}

/// Whether `cert` is valid for the host of `addr`.
fn names(cert: &rustls::pki_types::CertificateDer<'_>, addr: &str) -> bool {
    let Ok(ee) = webpki::EndEntityCert::try_from(cert) else {
        return false;
    };
    let Ok(name) = rustls::pki_types::ServerName::try_from(crate::net::host(addr)) else {
        return false;
    };
    ee.verify_is_valid_for_subject_name(&name).is_ok()
}

/// Reads frames from a peer's stream until it ends or misbehaves: each
/// report goes to contact as a POSTed one does. The first frame binds the
/// stream to its sender, whose address the client certificate must name;
/// a frame from another sender ends the stream.
pub async fn receive(
    io: Upgraded,
    cert: PeerCert,
    contact: Arc<crate::contact::Contact>,
) -> io::Result<()> {
    let mut io = TokioIo::new(io);
    let mut bound: Option<NodeId> = None;
    let mut body = Vec::new();
    loop {
        let mut head = [0u8; 12];
        io.read_exact(&mut head).await?;
        let len = u32::from_be_bytes(head[..4].try_into().unwrap()) as usize;
        let stamp = u64::from_be_bytes(head[4..].try_into().unwrap());
        if len > crate::peer::MAX_BODY {
            return Err(io::Error::other("frame too large"));
        }
        body.resize(len, 0);
        io.read_exact(&mut body).await?;
        let cpu = crate::contact::thread_cpu();
        let report = decode_report(&body).ok_or_else(|| io::Error::other("undecodable"))?;
        contact.decoded(cpu);
        sample(&STATS.latency, mono_ns().saturating_sub(stamp));
        match bound {
            Some(from) if from != report.from => {
                return Err(io::Error::other("another sender on the stream"));
            }
            Some(_) => {}
            None => {
                let named = contact
                    .addr_of(report.from)
                    .zip(cert.0.as_ref())
                    .is_some_and(|(addr, cert)| names(cert, &addr));
                if !named {
                    return Err(io::Error::other("the certificate does not name the sender"));
                }
                bound = Some(report.from);
            }
        }
        contact.receive(report);
    }
}
