//! TLS on a listener, with the ring provider of rustls.
//!
//! A task accepts TCP connections and runs each handshake in a task of its
//! own, bounded by [`HANDSHAKE_TIMEOUT`], so a client that stalls in the
//! handshake holds up nobody else. At most [`MAX_HANDSHAKES`] run at once;
//! further connections wait in the listen backlog. In `files` mode the certificate and key
//! are re-read with the token files and swapped in for new handshakes when
//! their content changes. A pair that does not parse or does not match is
//! read once more at once, since a rotation can replace the two files
//! between the reads; when it still fails, the previous pair stays and the
//! failure counts in
//! `samplez_secret_reload_failures_total{secret="<listener>Tls"}`. A client
//! CA file is re-read with them, in any mode, and a changed one applies
//! from the next handshake on; one that does not parse keeps the previous
//! CA and counts the same way. In `selfSigned` mode a new certificate
//! replaces the one in use at the first reload within
//! [`SELF_SIGNED_RENEWAL`] of its expiry.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::Error;
use crate::config::{Tls, TlsMode};
use crate::metrics::{Counter, Metrics};

/// The longest a client may take from connecting to finishing the handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The handshakes one listener runs at once, counting finished ones whose
/// connection the server has not taken yet.
pub const MAX_HANDSHAKES: usize = 256;

/// How long a `selfSigned` certificate is valid, starting an hour before
/// it is generated so a client whose clock is behind accepts it. macOS and
/// iOS refuse TLS server certificates valid for more than 825 days.
pub const SELF_SIGNED_VALIDITY: Duration = Duration::from_secs(365 * 24 * 3600);

/// A `selfSigned` certificate is replaced by a new one at the first reload
/// within this of its expiry.
pub const SELF_SIGNED_RENEWAL: Duration = Duration::from_secs(30 * 24 * 3600);

/// Handshaken connections waiting for the server to take them.
const ACCEPTED_QUEUE: usize = 64;

/// The TLS state of one listener, for [`crate::ServiceStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsStatus {
    /// `api`, `machine`, `ops` or `nmos`.
    pub listener: &'static str,
    pub mode: TlsMode,
    /// SHA-256 of the certificate in use, as colon-separated hex pairs.
    pub fingerprint: String,
    /// Seconds since the Unix epoch of the last successful read or
    /// generation.
    pub loaded_at: u64,
    /// Seconds since the Unix epoch at which the certificate in use
    /// expires.
    pub not_after: u64,
}

/// A certificate with its key, and when it expires.
#[derive(Debug, Clone)]
struct Presented {
    key: Arc<CertifiedKey>,
    /// Seconds since the Unix epoch.
    not_after: u64,
}

/// The certificate presented to new handshakes.
#[derive(Debug)]
struct Resolver(RwLock<Presented>);

impl Resolver {
    fn get(&self) -> Presented {
        self.0.read().expect("certificate lock poisoned").clone()
    }

    fn set(&self, presented: Presented) {
        *self.0.write().expect("certificate lock poisoned") = presented;
    }
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.get().key)
    }
}

struct Files {
    cert: PathBuf,
    key: PathBuf,
    /// The content the certificate in use was built from.
    loaded: Mutex<(Vec<u8>, Vec<u8>)>,
}

struct ClientCa {
    path: PathBuf,
    /// The content the verifier in use was built from.
    loaded: Mutex<Vec<u8>>,
}

/// The configuration of new handshakes; replaced when the client CA changes.
type SharedConfig = Arc<RwLock<Arc<ServerConfig>>>;

pub(crate) struct ListenerTls {
    listener: &'static str,
    mode: TlsMode,
    /// The names a `selfSigned` certificate is generated for.
    sans: Vec<String>,
    files: Option<Files>,
    client_ca: Option<ClientCa>,
    provider: Arc<CryptoProvider>,
    resolver: Arc<Resolver>,
    config: SharedConfig,
    loaded_at: AtomicU64,
    failures: Counter,
}

impl ListenerTls {
    /// `None` when `tls.mode` is `none`.
    pub(crate) fn new(
        listener: &'static str,
        tls: &Tls,
        metrics: &Metrics,
    ) -> Result<Option<ListenerTls>, Error> {
        let field = |f: &str| format!("listeners.{listener}.tls.{f}");
        let refuse =
            |f: &str, mode: &str| Err(Error::Config(format!("{} is for {mode} mode", field(f))));
        match tls.mode {
            TlsMode::None => {
                if *tls != Tls::default() {
                    return Err(Error::Config(format!(
                        "{} is none, so its other settings would be ignored",
                        field("mode")
                    )));
                }
                return Ok(None);
            }
            TlsMode::Files if !tls.sans.is_empty() => return refuse("sans", "selfSigned"),
            TlsMode::SelfSigned if tls.cert_file.is_some() => return refuse("certFile", "files"),
            TlsMode::SelfSigned if tls.key_file.is_some() => return refuse("keyFile", "files"),
            TlsMode::SelfSigned if tls.sans.is_empty() => {
                return Err(Error::Config(format!(
                    "selfSigned mode needs {}",
                    field("sans")
                )));
            }
            _ => {}
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let (files, key) = match tls.mode {
            TlsMode::Files => {
                let (Some(cert), Some(key)) = (&tls.cert_file, &tls.key_file) else {
                    return Err(Error::Config(format!(
                        "files mode needs {} and {}",
                        field("certFile"),
                        field("keyFile")
                    )));
                };
                let content = (read(cert)?, read(key)?);
                let certified = certified_key(&content, cert, &provider)?;
                let files = Files {
                    cert: cert.clone(),
                    key: key.clone(),
                    loaded: Mutex::new(content),
                };
                (Some(files), certified)
            }
            _ => (None, self_signed(&tls.sans, &provider)?),
        };
        let resolver = Arc::new(Resolver(RwLock::new(key)));
        let client_ca = match &tls.client_ca_file {
            Some(path) => Some((path.clone(), read(path)?)),
            None => None,
        };
        let pem = client_ca
            .as_ref()
            .map(|(path, pem)| (path.as_path(), pem.as_slice()));
        let config = server_config(&provider, pem, &resolver)?;
        let client_ca = client_ca.map(|(path, pem)| ClientCa {
            path,
            loaded: Mutex::new(pem),
        });
        let tls = ListenerTls {
            listener,
            mode: tls.mode,
            sans: tls.sans.clone(),
            files,
            client_ca,
            provider,
            resolver,
            config: Arc::new(RwLock::new(Arc::new(config))),
            loaded_at: AtomicU64::new(crate::auth::now()),
            failures: metrics.counter(
                "samplez_secret_reload_failures_total",
                "Failed re-reads of a secret file; the previous value stays in use.",
                &[("secret", &format!("{listener}Tls"))],
            ),
        };
        tracing::info!(
            listener,
            mode = ?tls.mode,
            fingerprint = %tls.fingerprint(),
            not_after = tls.resolver.get().not_after,
            "certificate in use"
        );
        Ok(Some(tls))
    }

    /// Re-reads the certificate and key files in `files` mode and the client
    /// CA file, and swaps in what they hold when it changed. In `selfSigned`
    /// mode, generates a new certificate within [`SELF_SIGNED_RENEWAL`] of
    /// the expiry of the one in use.
    pub(crate) fn reload(&self) -> Result<(), Error> {
        self.reload_with(crate::auth::now(), read)
    }

    /// `now` in seconds since the Unix epoch.
    fn reload_with(
        &self,
        now: u64,
        mut read: impl FnMut(&Path) -> Result<Vec<u8>, Error>,
    ) -> Result<(), Error> {
        let renewed = self.renew_self_signed(now);
        let pair = self.reload_pair(&mut read);
        let client_ca = self.reload_client_ca(&mut read);
        let result = renewed.and(pair).and(client_ca);
        match &result {
            Ok(()) if self.files.is_some() || self.client_ca.is_some() => {
                self.loaded_at.store(now, Ordering::Relaxed);
            }
            Ok(()) => {}
            Err(e) => {
                self.failures.add(1);
                tracing::warn!(listener = self.listener, error = %e, "keeping what was in use");
            }
        }
        result
    }

    fn reload_pair(
        &self,
        read: &mut impl FnMut(&Path) -> Result<Vec<u8>, Error>,
    ) -> Result<(), Error> {
        let Some(files) = &self.files else {
            return Ok(());
        };
        let mut loaded = files.loaded.lock().expect("certificate lock poisoned");
        let mut retried = false;
        loop {
            let content = (read(&files.cert)?, read(&files.key)?);
            if *loaded == content {
                return Ok(());
            }
            match certified_key(&content, &files.cert, &self.provider) {
                Ok(key) => {
                    let not_after = key.not_after;
                    self.resolver.set(key);
                    *loaded = content;
                    tracing::info!(
                        listener = self.listener,
                        fingerprint = %self.fingerprint(),
                        not_after,
                        "certificate changed"
                    );
                    return Ok(());
                }
                Err(_) if !retried => retried = true,
                Err(e) => return Err(e),
            }
        }
    }

    fn renew_self_signed(&self, now: u64) -> Result<(), Error> {
        if self.mode != TlsMode::SelfSigned
            || self.resolver.get().not_after > now + SELF_SIGNED_RENEWAL.as_secs()
        {
            return Ok(());
        }
        let key = self_signed(&self.sans, &self.provider)?;
        let not_after = key.not_after;
        self.resolver.set(key);
        self.loaded_at.store(now, Ordering::Relaxed);
        tracing::info!(
            listener = self.listener,
            fingerprint = %self.fingerprint(),
            not_after,
            "generated a new self-signed certificate"
        );
        Ok(())
    }

    fn reload_client_ca(
        &self,
        read: &mut impl FnMut(&Path) -> Result<Vec<u8>, Error>,
    ) -> Result<(), Error> {
        let Some(ca) = &self.client_ca else {
            return Ok(());
        };
        // Read under the lock, so a concurrent reload cannot install an
        // older read after a newer one.
        let mut loaded = ca.loaded.lock().expect("client CA lock poisoned");
        let content = read(&ca.path)?;
        if *loaded != content {
            let config = server_config(&self.provider, Some((&ca.path, &content)), &self.resolver)?;
            *self.config.write().expect("config lock poisoned") = Arc::new(config);
            *loaded = content;
            tracing::info!(listener = self.listener, "client CA changed");
        }
        Ok(())
    }

    fn fingerprint(&self) -> String {
        let key = self.resolver.get().key;
        let digest = Sha256::digest(key.cert[0].as_ref());
        let hex: Vec<String> = digest.iter().map(|b| format!("{b:02X}")).collect();
        hex.join(":")
    }

    pub(crate) fn status(&self) -> TlsStatus {
        TlsStatus {
            listener: self.listener,
            mode: self.mode,
            fingerprint: self.fingerprint(),
            loaded_at: self.loaded_at.load(Ordering::Relaxed),
            not_after: self.resolver.get().not_after,
        }
    }

    pub(crate) fn listener(&self) -> &'static str {
        self.listener
    }

    /// Starts accepting on `tcp`.
    pub(crate) fn bind(&self, tcp: TcpListener) -> io::Result<TlsListener> {
        self.listen(tcp, MAX_HANDSHAKES, ACCEPTED_QUEUE)
    }

    fn listen(&self, tcp: TcpListener, handshakes: usize, queue: usize) -> io::Result<TlsListener> {
        let local = tcp.local_addr()?;
        let (tx, accepted) = mpsc::channel(queue);
        let handshakes = Arc::new(Semaphore::new(handshakes));
        let task = tokio::spawn(accept_loop(tcp, self.config.clone(), tx, handshakes));
        Ok(TlsListener {
            local,
            accepted,
            task,
        })
    }
}

fn read(path: &Path) -> Result<Vec<u8>, Error> {
    std::fs::read(path).map_err(|e| Error::Secret(path.to_owned(), e))
}

/// Fails when the client CA file holds no usable certificate.
fn server_config(
    provider: &Arc<CryptoProvider>,
    client_ca: Option<(&Path, &[u8])>,
    resolver: &Arc<Resolver>,
) -> Result<ServerConfig, Error> {
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Config(format!("TLS: {e}")))?;
    let builder = match client_ca {
        Some((path, pem)) => {
            let mut roots = RootCertStore::empty();
            for cert in certs(pem, path)? {
                roots
                    .add(cert)
                    .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
            }
            let verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .build()
                    .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_cert_resolver(resolver.clone());
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

fn certs(pem: &[u8], path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(Error::Config(format!(
            "{} holds no certificate",
            path.display()
        )));
    }
    Ok(certs)
}

/// Fails when the key does not belong to the first certificate.
fn certified_key(
    (cert, key): &(Vec<u8>, Vec<u8>),
    cert_path: &Path,
    provider: &CryptoProvider,
) -> Result<Presented, Error> {
    let chain = certs(cert, cert_path)?;
    let config = |e: &dyn std::fmt::Display| Error::Config(format!("{}: {e}", cert_path.display()));
    let not_after = not_after(&chain[0]).ok_or_else(|| config(&"no readable validity"))?;
    let key =
        PrivateKeyDer::from_pem_slice(key).map_err(|e| Error::Config(format!("TLS key: {e}")))?;
    let key = CertifiedKey::from_der(chain, key, provider).map_err(|e| config(&e))?;
    Ok(Presented {
        key: Arc::new(key),
        not_after,
    })
}

/// The notAfter of a DER X.509 certificate in seconds since the Unix epoch
/// (RFC 5280 4.1, 4.1.2.5).
fn not_after(cert: &[u8]) -> Option<u64> {
    let (_, certificate, _) = der(cert)?;
    let (_, tbs, _) = der(certificate)?;
    let (tag, _, after_version) = der(tbs)?;
    // version [0], then serialNumber, signature and issuer before validity
    let mut rest = if tag == 0xa0 { after_version } else { tbs };
    for _ in 0..3 {
        rest = der(rest)?.2;
    }
    let (_, validity, _) = der(rest)?;
    let (_, _, after_not_before) = der(validity)?;
    let (tag, time, _) = der(after_not_before)?;
    let digits = std::str::from_utf8(time).ok()?.strip_suffix('Z')?;
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number = |range: std::ops::Range<usize>| digits.get(range)?.parse::<i64>().ok();
    let (year, rest) = match (tag, digits.len()) {
        // UTCTime YYMMDDHHMMSSZ, the years 1950 to 2049
        (0x17, 12) => match number(0..2)? {
            yy @ 50.. => (1900 + yy, 2),
            yy => (2000 + yy, 2),
        },
        // GeneralizedTime YYYYMMDDHHMMSSZ
        (0x18, 14) => (number(0..4)?, 4),
        _ => return None,
    };
    let field = |n: usize| number(rest + 2 * n..rest + 2 * n + 2);
    let (month, day) = (field(0)?, field(1)?);
    let seconds = field(2)? * 3600 + field(3)? * 60 + field(4)?;
    // Days since 1970-01-01 in the proleptic Gregorian calendar, after
    // http://howardhinnant.github.io/date_algorithms.html#days_from_civil
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    u64::try_from(days * 86_400 + seconds).ok()
}

/// The tag, content and remainder of the DER value at the start of `input`.
fn der(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, input) = input.split_first()?;
    let (&first, input) = input.split_first()?;
    let (len, input) = if first < 0x80 {
        (usize::from(first), input)
    } else {
        let n = usize::from(first & 0x7f);
        let (bytes, input) = input.split_at_checked(n)?;
        let len = bytes.iter().try_fold(0usize, |len, &b| {
            len.checked_mul(256)?.checked_add(usize::from(b))
        })?;
        (len, input)
    };
    let (content, rest) = input.split_at_checked(len)?;
    Some((tag, content, rest))
}

fn self_signed(sans: &[String], provider: &CryptoProvider) -> Result<Presented, Error> {
    let failed =
        |e: &dyn std::fmt::Display| Error::Config(format!("generating a certificate: {e}"));
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| failed(&e))?;
    let mut params = rcgen::CertificateParams::new(sans.to_vec()).map_err(|e| failed(&e))?;
    params.not_before = rcgen::date_time_ymd(1970, 1, 1) + since_epoch - Duration::from_secs(3600);
    params.not_after = params.not_before + SELF_SIGNED_VALIDITY;
    // Apple platforms refuse a TLS server certificate without it.
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let signing_key = rcgen::KeyPair::generate().map_err(|e| failed(&e))?;
    let not_after = u64::try_from(params.not_after.unix_timestamp()).map_err(|e| failed(&e))?;
    let cert = params.self_signed(&signing_key).map_err(|e| failed(&e))?;
    let key = PrivateKeyDer::Pkcs8(signing_key.serialize_der().into());
    let key =
        CertifiedKey::from_der(vec![cert.der().clone()], key, provider).map_err(|e| failed(&e))?;
    Ok(Presented {
        key: Arc::new(key),
        not_after,
    })
}

async fn accept_loop(
    mut tcp: TcpListener,
    config: SharedConfig,
    tx: mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>,
    handshakes: Arc<Semaphore>,
) {
    loop {
        let permit = handshakes
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        let (stream, addr) = axum::serve::Listener::accept(&mut tcp).await;
        let acceptor = TlsAcceptor::from(config.read().expect("config lock poisoned").clone());
        let tx = tx.clone();
        tokio::spawn(async move {
            let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;
            match handshake {
                Ok(Ok(tls)) => {
                    let _ = tx.send((tls, addr)).await;
                }
                Ok(Err(e)) => tracing::debug!(%addr, error = %e, "TLS handshake failed"),
                Err(_) => tracing::debug!(%addr, "TLS handshake timed out"),
            }
            // Only now: a connection waiting to be taken holds its TLS
            // state, so it counts against the limit as well.
            drop(permit);
        });
    }
}

/// A listener yielding connections that finished the TLS handshake.
pub struct TlsListener {
    local: SocketAddr,
    accepted: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    task: JoinHandle<()>,
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.accepted
            .recv()
            .await
            .expect("the accept task runs while the listener exists")
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::future::IntoFuture;

    use axum::Router;
    use axum::routing;
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
    use rustls::ClientConfig;
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsConnector;

    use super::*;

    /// Every network step of a test finishes within this.
    const LIMIT: Duration = Duration::from_secs(5);

    fn dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("samplez-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A self-signed certificate for `localhost` written as `<name>.crt`
    /// and `<name>.key`.
    fn write_pair(name: &str) -> (PathBuf, PathBuf, CertificateDer<'static>) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let (cert, key) = (
            dir().join(format!("{name}.crt")),
            dir().join(format!("{name}.key")),
        );
        std::fs::write(&cert, generated.cert.pem()).unwrap();
        std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
        (cert, key, generated.cert.der().clone())
    }

    fn files(cert: &Path, key: &Path) -> Tls {
        Tls {
            mode: TlsMode::Files,
            cert_file: Some(cert.to_owned()),
            key_file: Some(key.to_owned()),
            ..Tls::default()
        }
    }

    fn listener_tls(tls: &Tls, metrics: &Metrics) -> ListenerTls {
        ListenerTls::new("api", tls, metrics).unwrap().unwrap()
    }

    async fn serve(tls: &ListenerTls) -> SocketAddr {
        serve_with(tls, MAX_HANDSHAKES).await
    }

    async fn serve_with(tls: &ListenerTls, handshakes: usize) -> SocketAddr {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = tls.listen(tcp, handshakes, ACCEPTED_QUEUE).unwrap();
        let addr = axum::serve::Listener::local_addr(&listener).unwrap();
        let router = Router::new().route("/", routing::get(|| async { "ok" }));
        tokio::spawn(axum::serve(listener, router).into_future());
        addr
    }

    fn connector(
        roots: &[&CertificateDer<'static>],
        identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
    ) -> TlsConnector {
        let mut store = RootCertStore::empty();
        for root in roots {
            store.add((*root).clone()).unwrap();
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(store);
        let config = match identity {
            Some((cert, key)) => builder.with_client_auth_cert(vec![cert], key).unwrap(),
            None => builder.with_no_client_auth(),
        };
        TlsConnector::from(Arc::new(config))
    }

    /// `GET /` over TLS to `name`: the response and the certificate the
    /// server presented. An error with no response read is returned.
    async fn get(
        connector: &TlsConnector,
        addr: SocketAddr,
        name: &str,
    ) -> io::Result<(String, CertificateDer<'static>)> {
        let exchange = async {
            let tcp = TcpStream::connect(addr).await?;
            let name = ServerName::try_from(name.to_owned()).unwrap();
            let mut stream = connector.connect(name, tcp).await?;
            let cert = stream.get_ref().1.peer_certificates().unwrap()[0].clone();
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            if let Err(e) = stream.read_to_end(&mut response).await
                && response.is_empty()
            {
                return Err(e);
            }
            Ok((String::from_utf8(response).unwrap(), cert))
        };
        tokio::time::timeout(LIMIT, exchange)
            .await
            .expect("the exchange finished within 5 s")
    }

    fn fingerprint_of(cert: &CertificateDer<'_>) -> String {
        let digest = Sha256::digest(cert.as_ref());
        let hex: Vec<String> = digest.iter().map(|b| format!("{b:02X}")).collect();
        hex.join(":")
    }

    #[tokio::test]
    async fn a_loopback_client_completes_the_handshake_and_gets_a_response() {
        let (cert, key, der) = write_pair("handshake");
        let tls = listener_tls(&files(&cert, &key), &Metrics::default());
        let addr = serve(&tls).await;
        let (response, presented) = get(&connector(&[&der], None), addr, "localhost")
            .await
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with("ok"), "{response}");
        assert_eq!(presented, der);
        let status = tls.status();
        assert_eq!(status.fingerprint, fingerprint_of(&der));
        assert_eq!(
            status.not_after, 67_090_118_400,
            "4096-01-01, rcgen's default"
        );
    }

    #[tokio::test]
    async fn a_reload_presents_a_changed_certificate_and_keeps_it_on_a_bad_pair() {
        let (cert, key, first) = write_pair("reload");
        let metrics = Metrics::default();
        let tls = listener_tls(&files(&cert, &key), &metrics);
        let addr = serve(&tls).await;
        let (old_cert, old_key, _) = write_pair("reload-old");
        std::fs::copy(&cert, &old_cert).unwrap();
        std::fs::copy(&key, &old_key).unwrap();

        let (new_cert, new_key, second) = write_pair("reload-new");
        std::fs::copy(&new_cert, &cert).unwrap();
        std::fs::copy(&new_key, &key).unwrap();
        tls.reload().unwrap();
        let both = connector(&[&first, &second], None);
        let (_, presented) = get(&both, addr, "localhost").await.unwrap();
        assert_eq!(presented, second);
        assert_eq!(tls.status().fingerprint, fingerprint_of(&second));

        std::fs::copy(&old_cert, &cert).unwrap();
        assert!(
            tls.reload().is_err(),
            "the key belongs to the other certificate"
        );
        let (_, presented) = get(&both, addr, "localhost").await.unwrap();
        assert_eq!(presented, second);
        assert!(
            metrics
                .render()
                .contains("samplez_secret_reload_failures_total{secret=\"apiTls\"} 1")
        );
    }

    #[test]
    fn a_pair_read_mid_rotation_is_read_again_before_it_counts_as_a_failure() {
        let (cert, key, _) = write_pair("rotation");
        let metrics = Metrics::default();
        let tls = listener_tls(&files(&cert, &key), &metrics);
        let (new_cert, new_key, second) = write_pair("rotation-new");
        let file = |path: &Path| std::fs::read(path).unwrap();
        // The first reads see the new certificate beside the old key.
        let mut reads = [&new_cert, &key, &new_cert, &new_key]
            .map(|p| file(p))
            .into_iter();
        tls.reload_with(crate::auth::now(), |_| {
            Ok(reads.next().expect("two reads of each file at most"))
        })
        .unwrap();
        assert_eq!(tls.status().fingerprint, fingerprint_of(&second));
        let failures = "samplez_secret_reload_failures_total{secret=\"apiTls\"} 0";
        assert!(metrics.render().contains(failures));
    }

    #[tokio::test]
    async fn a_self_signed_certificate_names_the_configured_sans() {
        let config = Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["samplez.example".into(), "127.0.0.1".into()],
            ..Tls::default()
        };
        let tls = listener_tls(&config, &Metrics::default());
        let der = tls.resolver.get().key.cert[0].clone();
        assert_eq!(tls.status().fingerprint, fingerprint_of(&der));
        let addr = serve(&tls).await;
        let trusting = connector(&[&der], None);
        for name in ["samplez.example", "127.0.0.1"] {
            let (response, _) = get(&trusting, addr, name).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200"), "{name}: {response}");
        }
        assert!(get(&trusting, addr, "other.example").await.is_err());
    }

    /// A CA's PEM and a client identity it issued.
    fn client_ca() -> (String, CertificateDer<'static>, PrivateKeyDer<'static>) {
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
        let client_key = KeyPair::generate().unwrap();
        let client_cert = CertificateParams::new(vec!["client".into()])
            .unwrap()
            .signed_by(&client_key, &ca)
            .unwrap();
        let key = PrivateKeyDer::Pkcs8(client_key.serialize_der().into());
        (ca.pem(), client_cert.der().clone(), key)
    }

    #[test]
    fn a_self_signed_certificate_is_valid_from_an_hour_before_the_start_for_a_year() {
        use rustls::client::WebPkiServerVerifier;
        use rustls::client::danger::ServerCertVerifier;
        use rustls::pki_types::UnixTime;

        let config = Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["localhost".into()],
            ..Tls::default()
        };
        let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let tls = listener_tls(&config, &Metrics::default());
        let der = tls.resolver.get().key.cert[0].clone();
        let mut roots = RootCertStore::empty();
        roots.add(der.clone()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .unwrap();
        let name = ServerName::try_from("localhost").unwrap();
        let valid_at = |t: Duration| {
            let now = UnixTime::since_unix_epoch(t);
            verifier
                .verify_server_cert(&der, &[], &name, &[], now)
                .is_ok()
        };
        let (minute, hour) = (Duration::from_secs(60), Duration::from_secs(3600));
        assert!(valid_at(start - hour + minute), "a clock an hour behind");
        assert!(!valid_at(start - hour - minute));
        assert!(valid_at(start + SELF_SIGNED_VALIDITY - hour - minute));
        assert!(!valid_at(start + SELF_SIGNED_VALIDITY), "expired");
        let expiry = (start + SELF_SIGNED_VALIDITY - hour).as_secs();
        assert!(tls.status().not_after.abs_diff(expiry) <= 1);
    }

    #[test]
    fn a_self_signed_certificate_is_replaced_within_thirty_days_of_its_expiry() {
        let config = Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["localhost".into()],
            ..Tls::default()
        };
        let tls = listener_tls(&config, &Metrics::default());
        let first = tls.status();
        let renewal = first.not_after - SELF_SIGNED_RENEWAL.as_secs();
        let nothing = |_: &Path| -> Result<Vec<u8>, Error> { panic!("no file to read") };
        tls.reload_with(renewal - 1, nothing).unwrap();
        assert_eq!(tls.status(), first, "a second before");
        tls.reload_with(renewal, nothing).unwrap();
        let renewed = tls.status();
        assert_ne!(renewed.fingerprint, first.fingerprint);
        assert!(renewed.not_after >= first.not_after);
        assert_eq!(renewed.loaded_at, renewal);
    }

    #[test]
    fn the_expiry_is_read_from_utc_and_generalized_time() {
        let midnight = |y, m, d| rcgen::date_time_ymd(y, m, d);
        let second = Duration::from_secs(1);
        // UTCTime before and after 2000, then GeneralizedTime from 2050 on.
        for expiry in [
            midnight(2000, 1, 1) - second,
            midnight(2024, 2, 29) + Duration::from_secs(12 * 3600 + 34 * 60 + 56),
            midnight(2050, 1, 1) - second,
            midnight(2050, 1, 1),
            midnight(4096, 1, 1),
        ] {
            let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
            params.not_before = midnight(1975, 1, 1);
            params.not_after = expiry;
            let cert = params.self_signed(&KeyPair::generate().unwrap()).unwrap();
            let expected = u64::try_from(expiry.unix_timestamp()).unwrap();
            assert_eq!(not_after(cert.der()), Some(expected), "{expiry}");
        }
        assert_eq!(not_after(&[0x30, 0x03, 0x30, 0x01]), None, "truncated");
    }

    #[test]
    fn a_self_signed_certificate_is_for_server_authentication() {
        let config = Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["localhost".into()],
            ..Tls::default()
        };
        let der = listener_tls(&config, &Metrics::default())
            .resolver
            .get()
            .key
            .cert[0]
            .clone();
        // extKeyUsage (2.5.29.37) holding only id-kp-serverAuth
        // (1.3.6.1.5.5.7.3.1), RFC 5280 4.2.1.12.
        let extension = [
            0x06, 0x03, 0x55, 0x1d, 0x25, 0x04, 0x0c, 0x30, 0x0a, 0x06, 0x08, 0x2b, 0x06, 0x01,
            0x05, 0x05, 0x07, 0x03, 0x01,
        ];
        assert!(der.windows(extension.len()).any(|w| w == extension));
    }

    #[tokio::test]
    async fn a_client_ca_refuses_clients_without_a_certificate_it_issued() {
        let (ca, client_cert, client_key) = client_ca();
        let ca_file = dir().join("client-ca.crt");
        std::fs::write(&ca_file, ca).unwrap();
        let identity = || Some((client_cert.clone(), client_key.clone_key()));

        let (cert, key, der) = write_pair("mtls");
        let config = Tls {
            client_ca_file: Some(ca_file),
            ..files(&cert, &key)
        };
        let tls = listener_tls(&config, &Metrics::default());
        let addr = serve(&tls).await;
        assert!(
            get(&connector(&[&der], None), addr, "localhost")
                .await
                .is_err()
        );
        let (response, _) = get(&connector(&[&der], identity()), addr, "localhost")
            .await
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    #[tokio::test]
    async fn a_stalled_handshake_does_not_hold_up_other_clients() {
        let (cert, key, der) = write_pair("stalled");
        let tls = listener_tls(&files(&cert, &key), &Metrics::default());
        let addr = serve(&tls).await;
        let _stalled = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        let (response, _) = get(&connector(&[&der], None), addr, "localhost")
            .await
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(started.elapsed() < HANDSHAKE_TIMEOUT / 2);
    }

    #[tokio::test]
    async fn a_reload_applies_a_changed_client_ca() {
        let (first, _, _) = client_ca();
        let (second, client_cert, client_key) = client_ca();
        let ca_file = dir().join("rotated-client-ca.crt");
        std::fs::write(&ca_file, first).unwrap();
        let (cert, key, der) = write_pair("rotated-ca");
        let config = Tls {
            client_ca_file: Some(ca_file.clone()),
            ..files(&cert, &key)
        };
        let tls = listener_tls(&config, &Metrics::default());
        let addr = serve(&tls).await;
        let issued_by_second =
            || connector(&[&der], Some((client_cert.clone(), client_key.clone_key())));
        assert!(get(&issued_by_second(), addr, "localhost").await.is_err());

        std::fs::write(&ca_file, second).unwrap();
        tls.reload().unwrap();
        let (response, _) = get(&issued_by_second(), addr, "localhost").await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    /// Two reloads at once, the first reading the CA file before a rotation
    /// and finishing after the second.
    #[test]
    fn of_two_concurrent_reloads_the_later_read_of_the_client_ca_stays() {
        let pem = || client_ca().0.into_bytes();
        let (before, rotated) = (pem(), pem());
        let ca_file = dir().join("concurrent-client-ca.crt");
        std::fs::write(&ca_file, pem()).unwrap();
        let config = Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["localhost".into()],
            client_ca_file: Some(ca_file),
            ..Tls::default()
        };
        let tls = listener_tls(&config, &Metrics::default());
        let (reading, read) = std::sync::mpsc::channel();
        let (go_on, wait) = std::sync::mpsc::channel::<()>();
        let (tls, before) = (&tls, &before);
        std::thread::scope(|scope| {
            let first = scope.spawn(move || {
                tls.reload_with(crate::auth::now(), move |_| {
                    reading.send(()).unwrap();
                    wait.recv().unwrap();
                    Ok(before.clone())
                })
            });
            read.recv().unwrap();
            let second =
                scope.spawn(|| tls.reload_with(crate::auth::now(), |_| Ok(rotated.clone())));
            // Time for the second to finish, were it not held up.
            std::thread::sleep(Duration::from_millis(200));
            go_on.send(()).unwrap();
            first.join().unwrap().unwrap();
            second.join().unwrap().unwrap();
        });
        let loaded = tls.client_ca.as_ref().unwrap().loaded.lock().unwrap();
        assert!(*loaded == rotated, "the rotated CA is in use");
    }

    #[tokio::test]
    async fn handshakes_beyond_the_limit_wait_for_a_running_one_to_end() {
        let (cert, key, der) = write_pair("limit");
        let tls = listener_tls(&files(&cert, &key), &Metrics::default());
        let addr = serve_with(&tls, 2).await;
        let stalled = [
            TcpStream::connect(addr).await.unwrap(),
            TcpStream::connect(addr).await.unwrap(),
        ];
        tokio::time::sleep(Duration::from_millis(50)).await;
        let client = connector(&[&der], None);
        let third =
            tokio::time::timeout(Duration::from_millis(500), get(&client, addr, "localhost")).await;
        assert!(third.is_err(), "not started while two stall: {third:?}");
        drop(stalled);
        let (response, _) = get(&client, addr, "localhost").await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    #[tokio::test]
    async fn connections_waiting_to_be_taken_count_against_the_handshake_limit() {
        let (cert, key, der) = write_pair("untaken");
        let tls = listener_tls(&files(&cert, &key), &Metrics::default());
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Nothing takes the connections: one fits the queue, one handshake
        // runs.
        let listener = tls.listen(tcp, 1, 1).unwrap();
        let addr = axum::serve::Listener::local_addr(&listener).unwrap();
        let client = connector(&[&der], None);
        let handshake = || async {
            let tcp = TcpStream::connect(addr).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            client.connect(name, tcp).await
        };
        let mut open = Vec::new();
        for _ in 0..2 {
            let connected = tokio::time::timeout(LIMIT, handshake()).await;
            open.push(connected.expect("within 5 s").expect("handshaken"));
        }
        let third = tokio::time::timeout(Duration::from_millis(500), handshake()).await;
        assert!(third.is_err(), "not started: {third:?}");
        drop(listener);
    }

    #[test]
    fn settings_that_would_be_ignored_or_are_missing_are_refused() {
        let metrics = Metrics::default();
        let refused = |tls: Tls| ListenerTls::new("api", &tls, &metrics).is_err();
        let path = Some(PathBuf::from("/nonexistent"));
        assert!(refused(Tls {
            sans: vec!["a".into()],
            ..Tls::default()
        }));
        assert!(refused(Tls {
            mode: TlsMode::SelfSigned,
            ..Tls::default()
        }));
        assert!(refused(Tls {
            mode: TlsMode::Files,
            cert_file: path.clone(),
            ..Tls::default()
        }));
        assert!(refused(Tls {
            mode: TlsMode::SelfSigned,
            sans: vec!["a".into()],
            key_file: path,
            ..Tls::default()
        }));
        assert!(
            ListenerTls::new("api", &Tls::default(), &metrics)
                .unwrap()
                .is_none()
        );
    }
}
