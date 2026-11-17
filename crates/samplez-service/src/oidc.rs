//! `oidc` mode: the provider's metadata and signing keys, ID and access
//! token validation, the authorization code flow with PKCE and the role a
//! token's claims grant.
//!
//! The metadata is read on first use and kept; the signing keys are read
//! with it and again when a token names a key they do not hold, at most
//! once per [`JWKS_MIN_REFRESH`]. One request reads at a time, and a failed
//! read is not repeated for [`PROVIDER_RETRY_AFTER`], so a provider that
//! cannot be reached fails the sign-ins and bearer requests of that moment
//! only, at once, and tokens of keys already held keep verifying.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::Error;
use crate::auth::{self, Auth, Identity, Role};
use crate::config::OidcConfig;

/// Allowed clock difference to the provider on `exp` and `nbf`.
pub const LEEWAY: u64 = 60;
/// The shortest time between two reads of the signing keys.
pub const JWKS_MIN_REFRESH: Duration = Duration::from_secs(10);
/// Every request to the provider is abandoned after this long. A read is
/// at most two requests, the metadata and the keys, and a request waits
/// for another one's read as long as that can take, plus [`WAIT_MARGIN`].
pub const PROVIDER_TIMEOUT: Duration = Duration::from_secs(5);
/// What a request waits for another one's read beyond its two requests.
pub const WAIT_MARGIN: Duration = Duration::from_millis(500);
/// After a failed read of the provider, no read is made for this long; the
/// requests that need one fail at once.
pub const PROVIDER_RETRY_AFTER: Duration = Duration::from_secs(15);
/// The longest answer read from the provider.
pub const MAX_BODY: usize = 1 << 20;
/// How long a session lasts without `sessionTtl`.
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(8 * 3600);
/// The shortest `sessionTtl`, in seconds.
pub const MIN_SESSION_TTL: u64 = 60;
/// The longest `sessionTtl`, in seconds.
pub const MAX_SESSION_TTL: u64 = 7 * 86_400;
/// How long a sign-in may take from the redirect to the provider to the
/// callback.
pub const LOGIN_TTL: Duration = Duration::from_secs(600);
/// Holds the state, nonce and PKCE verifier of a sign-in in progress.
pub const LOGIN_COOKIE: &str = "samplez_login";
pub(crate) const LOGIN_PATH: &str = "/api/v1/login";
pub(crate) const CALLBACK_PATH: &str = "/api/v1/login/callback";

/// Why a token was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalid {
    Malformed,
    /// `none`, an HMAC algorithm or one not supported, or a `crit` header.
    Algorithm,
    /// No signing key of the provider matches `kid` and `alg`.
    UnknownKey,
    Signature,
    Issuer,
    Audience,
    /// `azp` missing where required, or naming a client not allowed.
    Party,
    /// An ID token, or another token that is not an access token, sent
    /// as a bearer.
    NotAccessToken,
    Expired,
    NotYetValid,
    Nonce,
}

/// A signing key of the provider (RFC 7517), kept as published.
#[derive(Debug, Clone, Deserialize)]
pub struct Jwk {
    pub kty: String,
    #[serde(default)]
    pub kid: Option<String>,
    #[serde(default)]
    pub alg: Option<String>,
    #[serde(default, rename = "use")]
    pub usage: Option<String>,
    #[serde(default)]
    pub crv: Option<String>,
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Option<String>,
}

/// What a token's claims have to hold besides a valid signature and
/// lifetime.
pub struct Expect<'a> {
    /// `iss`, compared as given.
    pub issuer: &'a str,
    /// One of `aud`; `None` checks no audience.
    pub audience: Option<&'a str>,
    /// The clients `azp` may name. `azp` is checked when present, and
    /// required when `require_party` is set or `aud` names more than one
    /// audience.
    pub parties: &'a [String],
    pub require_party: bool,
    /// `nonce`; ID tokens only.
    pub nonce: Option<&'a str>,
    /// An access token is wanted: a token with a `nonce`, a `typ` claim of
    /// `ID` or a header `typ` other than `JWT` or `at+jwt` is refused.
    pub access: bool,
}

/// The header fields of a JWT that decide which key verifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub alg: String,
    pub kid: Option<String>,
    pub typ: Option<String>,
}

/// The JWS algorithms accepted, with the key type and curve each needs.
fn algorithm(alg: &str) -> Option<(&'static str, Option<&'static str>)> {
    Some(match alg {
        "RS256" | "RS384" | "RS512" | "PS256" | "PS384" | "PS512" => ("RSA", None),
        "ES256" => ("EC", Some("P-256")),
        "ES384" => ("EC", Some("P-384")),
        "EdDSA" => ("OKP", Some("Ed25519")),
        _ => return None,
    })
}

fn b64(s: &str) -> Result<Vec<u8>, Invalid> {
    B64.decode(s).map_err(|_| Invalid::Malformed)
}

/// Parses a JWT's header and refuses what no key can verify: anything but
/// three parts, an algorithm not accepted, or a `crit` header. Reads
/// nothing from the provider.
pub fn header(token: &str) -> Result<Header, Invalid> {
    let mut parts = token.split('.');
    let (Some(h), Some(_), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Invalid::Malformed);
    };
    let header: Value = serde_json::from_slice(&b64(h)?).map_err(|_| Invalid::Malformed)?;
    if header.get("crit").is_some() {
        return Err(Invalid::Algorithm);
    }
    let alg = header["alg"].as_str().ok_or(Invalid::Malformed)?;
    algorithm(alg).ok_or(Invalid::Algorithm)?;
    let text = |k: &str| header.get(k).and_then(Value::as_str).map(str::to_owned);
    Ok(Header {
        alg: alg.to_owned(),
        kid: text("kid"),
        typ: text("typ"),
    })
}

fn check_signature(alg: &str, key: &Jwk, message: &[u8], sig: &[u8]) -> Result<(), Invalid> {
    let part = |v: &Option<String>| v.as_deref().ok_or(Invalid::UnknownKey).and_then(b64);
    let rsa = |params: &'static signature::RsaParameters| -> Result<(), Invalid> {
        let (n, e) = (part(&key.n)?, part(&key.e)?);
        RsaPublicKeyComponents { n: &n, e: &e }
            .verify(params, message, sig)
            .map_err(|_| Invalid::Signature)
    };
    let ec = |params: &'static signature::EcdsaVerificationAlgorithm, len: usize| {
        let (x, y) = (part(&key.x)?, part(&key.y)?);
        if x.len() != len || y.len() != len {
            return Err(Invalid::UnknownKey);
        }
        let point = [&[4u8][..], &x, &y].concat();
        UnparsedPublicKey::new(params, point)
            .verify(message, sig)
            .map_err(|_| Invalid::Signature)
    };
    match alg {
        "RS256" => rsa(&signature::RSA_PKCS1_2048_8192_SHA256),
        "RS384" => rsa(&signature::RSA_PKCS1_2048_8192_SHA384),
        "RS512" => rsa(&signature::RSA_PKCS1_2048_8192_SHA512),
        "PS256" => rsa(&signature::RSA_PSS_2048_8192_SHA256),
        "PS384" => rsa(&signature::RSA_PSS_2048_8192_SHA384),
        "PS512" => rsa(&signature::RSA_PSS_2048_8192_SHA512),
        "ES256" => ec(&signature::ECDSA_P256_SHA256_FIXED, 32),
        "ES384" => ec(&signature::ECDSA_P384_SHA384_FIXED, 48),
        "EdDSA" => UnparsedPublicKey::new(&signature::ED25519, part(&key.x)?)
            .verify(message, sig)
            .map_err(|_| Invalid::Signature),
        _ => Err(Invalid::Algorithm),
    }
}

/// Checks a signed JWT (RFC 7519) against `keys` and `expect` at `now`,
/// seconds since the Unix epoch, and returns its claims.
pub fn verify(token: &str, keys: &[Jwk], expect: &Expect, now: u64) -> Result<Value, Invalid> {
    let head = header(token)?;
    let mut parts = token.split('.');
    let (h, p, s) = match (parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(p), Some(s)) => (h, p, s),
        _ => return Err(Invalid::Malformed),
    };
    let alg = head.alg.as_str();
    let (kty, crv) = algorithm(alg).ok_or(Invalid::Algorithm)?;
    let kid = head.kid.as_deref();
    let candidates: Vec<&Jwk> = keys
        .iter()
        .filter(|k| k.kty == kty && (crv.is_none() || k.crv.as_deref() == crv))
        .filter(|k| k.alg.as_deref().is_none_or(|a| a == alg))
        .filter(|k| k.usage.as_deref().is_none_or(|u| u == "sig"))
        .filter(|k| kid.is_none() || k.kid.as_deref() == kid)
        .collect();
    let message = format!("{h}.{p}");
    let sig = b64(s)?;
    match candidates.as_slice() {
        [] => return Err(Invalid::UnknownKey),
        // Without a kid, or with keys sharing one, the signature decides.
        many => {
            many.iter()
                .find(|k| check_signature(alg, k, message.as_bytes(), &sig).is_ok())
                .ok_or(Invalid::Signature)?;
        }
    }

    let claims: Value = serde_json::from_slice(&b64(p)?).map_err(|_| Invalid::Malformed)?;
    if !claims.is_object() {
        return Err(Invalid::Malformed);
    }
    if claims["iss"].as_str() != Some(expect.issuer) {
        return Err(Invalid::Issuer);
    }
    let audiences: Vec<&str> = match &claims["aud"] {
        Value::String(a) => vec![a.as_str()],
        Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    if let Some(want) = expect.audience
        && !audiences.contains(&want)
    {
        return Err(Invalid::Audience);
    }
    match claims.get("azp").map(Value::as_str) {
        Some(Some(azp)) if expect.parties.iter().any(|p| p == azp) => {}
        None if !expect.require_party && audiences.len() <= 1 => {}
        _ => return Err(Invalid::Party),
    }
    if expect.access {
        let typ = |v: Option<&str>| v.map(str::to_ascii_lowercase);
        let id_claim = typ(claims["typ"].as_str()).is_some_and(|t| t == "id");
        let header_ok = typ(head.typ.as_deref())
            .is_none_or(|t| ["jwt", "at+jwt", "application/at+jwt"].contains(&t.as_str()));
        if claims.get("nonce").is_some() || id_claim || !header_ok {
            return Err(Invalid::NotAccessToken);
        }
    }
    let exp = claims["exp"].as_u64().ok_or(Invalid::Malformed)?;
    if exp.saturating_add(LEEWAY) <= now {
        return Err(Invalid::Expired);
    }
    if let Some(nbf) = claims.get("nbf") {
        let nbf = nbf.as_u64().ok_or(Invalid::Malformed)?;
        if nbf > now.saturating_add(LEEWAY) {
            return Err(Invalid::NotYetValid);
        }
    }
    if let Some(nonce) = expect.nonce {
        let given = claims["nonce"].as_str().unwrap_or_default();
        if !bool::from(given.as_bytes().ct_eq(nonce.as_bytes())) || given.is_empty() {
            return Err(Invalid::Nonce);
        }
    }
    Ok(claims)
}

/// The strings of the claim `name`: a list of strings or one string. A
/// name that is not a top-level claim and holds `.` is followed as a path.
pub fn claim_values<'a>(claims: &'a Value, name: &str) -> Vec<&'a str> {
    let value = match claims.get(name) {
        Some(v) => Some(v),
        None => name.split('.').try_fold(claims, |v, k| v.get(k)),
    };
    match value {
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// The role `values` grant: operator with `operator`; else viewer with
/// `viewer`, or with any values when no `viewer` is configured; else none.
/// A value matches with or without a leading `/`.
pub fn role(values: &[&str], operator: &str, viewer: Option<&str>) -> Option<Role> {
    let has = |want: &str| {
        values
            .iter()
            .any(|v| *v == want || v.strip_prefix('/') == Some(want))
    };
    if has(operator) {
        Some(Role::Operator)
    } else {
        match viewer {
            Some(v) if !has(v) => None,
            _ => Some(Role::Viewer),
        }
    }
}

/// The user a token names: `preferred_username`, else `email`, else
/// `sub`.
pub fn user(claims: &Value) -> Option<String> {
    ["preferred_username", "email", "sub"]
        .iter()
        .filter_map(|c| claims[*c].as_str())
        .find(|u| !u.is_empty())
        .map(str::to_owned)
}

/// The user's display name a token gives: `preferred_username`, else
/// `name`.
pub fn name(claims: &Value) -> Option<String> {
    ["preferred_username", "name"]
        .iter()
        .filter_map(|c| claims[*c].as_str())
        .find(|n| !n.is_empty())
        .map(str::to_owned)
}

/// `n` random bytes in base64url without padding.
pub(crate) fn random(n: usize) -> Result<String, Error> {
    let mut buf = vec![0u8; n];
    SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| Error::Config("the random source failed".into()))?;
    Ok(B64.encode(buf))
}

/// The PKCE S256 challenge of `verifier` (RFC 7636 section 4.2).
pub fn pkce_challenge(verifier: &str) -> String {
    B64.encode(Sha256::digest(verifier.as_bytes()))
}

#[derive(Debug, Clone, Deserialize)]
struct Metadata {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
    #[serde(default)]
    end_session_endpoint: Option<String>,
}

/// What has been read from the provider. Never held across a read.
#[derive(Default)]
struct Cache {
    metadata: Option<Arc<Metadata>>,
    keys: Option<Arc<Vec<Jwk>>>,
    keys_at: Option<Instant>,
    /// The last failed read and why; no read is made for
    /// [`Tuning::retry_after`] after it.
    failed: Option<(Instant, String)>,
}

/// The metadata, and the keys when asked for.
type Held = (Arc<Metadata>, Option<Arc<Vec<Jwk>>>);

/// What a request needs read from the provider.
enum Need {
    Metadata,
    Keys,
    /// Keys other than these, as a token names a key they do not hold.
    NewKeys(Arc<Vec<Jwk>>),
}

/// The timing of the reads from the provider.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tuning {
    /// Every request to the provider is abandoned after this long.
    pub(crate) timeout: Duration,
    /// The shortest time between two reads of the keys.
    pub(crate) min_refresh: Duration,
    /// How long a failed read answers for the provider.
    pub(crate) retry_after: Duration,
}

impl Tuning {
    /// How long a request waits for a read another one is making: the
    /// longest that read can take, metadata and keys.
    pub(crate) fn wait(&self) -> Duration {
        self.timeout * 2 + WAIT_MARGIN
    }
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            timeout: PROVIDER_TIMEOUT,
            min_refresh: JWKS_MIN_REFRESH,
            retry_after: PROVIDER_RETRY_AFTER,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Failure {
    /// The provider could not be reached or answered something unusable.
    Provider(String),
    Token(Invalid),
    /// A valid token whose claims grant no role.
    NoRole,
}

/// A sign-in in progress, carried in [`LOGIN_COOKIE`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
pub(crate) struct Pending {
    pub(crate) state: String,
    pub(crate) nonce: String,
    pub(crate) verifier: String,
    /// The path the browser returns to.
    pub(crate) to: String,
}

pub(crate) struct Oidc {
    pub(crate) config: OidcConfig,
    pub(crate) redirect_uri: String,
    /// The public URL's origin, normalized.
    pub(crate) public_origin: String,
    pub(crate) public_host: String,
    pub(crate) public_https: bool,
    /// The clients a bearer's `azp` may name.
    parties: Vec<String>,
    /// How long a session lasts, in seconds.
    pub(crate) session_ttl: u64,
    http: reqwest::Client,
    cache: std::sync::Mutex<Cache>,
    /// Held by the one request reading from the provider.
    reading: tokio::sync::Mutex<()>,
    tuning: Tuning,
}

fn loopback(url: &reqwest::Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|a| a.is_loopback())
}

/// `https`, or `http` to a loopback address.
fn secure_url(url: &str) -> Result<reqwest::Url, String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| e.to_string())?;
    match parsed.scheme() {
        "https" => Ok(parsed),
        "http" if loopback(&parsed) => Ok(parsed),
        _ => Err(format!("{url}: https, or http to a loopback address")),
    }
}

/// A session lifetime: seconds, or a number with `s`, `m`, `h` or `d`.
pub fn parse_ttl(ttl: &crate::config::Ttl) -> Result<u64, String> {
    let secs = match ttl {
        crate::config::Ttl::Seconds(s) => *s,
        crate::config::Ttl::Text(t) => {
            let t = t.trim();
            let (n, unit) = match t.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
                Some((i, _)) => t.split_at(i),
                None => (t, "s"),
            };
            let n: u64 = n.parse().map_err(|_| format!("{t:?}: not a duration"))?;
            let scale = match unit {
                "s" => 1,
                "m" => 60,
                "h" => 3600,
                "d" => 86_400,
                _ => return Err(format!("{t:?}: the unit is s, m, h or d")),
            };
            n.saturating_mul(scale)
        }
    };
    if !(MIN_SESSION_TTL..=MAX_SESSION_TTL).contains(&secs) {
        return Err(format!(
            "{secs} s: between {MIN_SESSION_TTL} s and {MAX_SESSION_TTL} s"
        ));
    }
    Ok(secs)
}

/// Reads a response body of at most [`MAX_BODY`] bytes.
async fn body(mut res: reqwest::Response) -> Result<Vec<u8>, String> {
    if res.content_length().is_some_and(|l| l > MAX_BODY as u64) {
        return Err(format!("the answer is longer than {MAX_BODY} bytes"));
    }
    let mut out = Vec::new();
    while let Some(chunk) = res.chunk().await.map_err(|e| e.to_string())? {
        if out.len() + chunk.len() > MAX_BODY {
            return Err(format!("the answer is longer than {MAX_BODY} bytes"));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

impl Oidc {
    pub(crate) fn new(config: &OidcConfig) -> Result<Oidc, Error> {
        Oidc::with_tuning(config, Tuning::default())
    }

    pub(crate) fn with_tuning(config: &OidcConfig, tuning: Tuning) -> Result<Oidc, Error> {
        let bad = |what: &str, why: String| Error::Config(format!("auth.oidc.{what}: {why}"));
        secure_url(&config.issuer_url).map_err(|e| bad("issuerUrl", e))?;
        let public =
            reqwest::Url::parse(&config.public_url).map_err(|e| bad("publicUrl", e.to_string()))?;
        if !matches!(public.scheme(), "http" | "https")
            || public.path() != "/"
            || public.query().is_some()
            || public.fragment().is_some()
            || !public.username().is_empty()
        {
            return Err(bad(
                "publicUrl",
                "a scheme, host and optional port only, e.g. https://faderz.example".into(),
            ));
        }
        if public.scheme() == "http" && !loopback(&public) {
            return Err(bad(
                "publicUrl",
                "https, or http to a loopback address: the session cookie would cross the network in the clear".into(),
            ));
        }
        let public_host = public
            .host_str()
            .ok_or_else(|| bad("publicUrl", "no host".into()))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let public_origin = auth::normalize_origin(&public.origin().ascii_serialization());
        if config.client_id.is_empty() {
            return Err(bad("clientId", "empty".into()));
        }
        if config.operator_role.is_empty() {
            return Err(bad("operatorRole", "empty".into()));
        }
        let session_ttl = match &config.session_ttl {
            Some(t) => parse_ttl(t).map_err(|e| bad("sessionTtl", e))?,
            None => DEFAULT_SESSION_TTL.as_secs(),
        };
        let parties = match config.allowed_clients.as_slice() {
            [] => vec![config.client_id.clone()],
            given => given.to_vec(),
        };
        if config.viewer_role.is_none() {
            tracing::warn!(
                "auth.oidc.viewerRole is not set: every user the provider signs in to this client is a viewer"
            );
        }
        // No redirect is followed: a token request sent on would carry the
        // code, the verifier and the client secret wherever it pointed.
        let http = reqwest::Client::builder()
            .timeout(tuning.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| Error::Config(format!("the HTTP client for the provider: {e}")))?;
        Ok(Oidc {
            redirect_uri: format!("{public_origin}{CALLBACK_PATH}"),
            public_https: public.scheme() == "https",
            public_origin,
            public_host,
            parties,
            session_ttl,
            config: config.clone(),
            http,
            cache: std::sync::Mutex::default(),
            reading: tokio::sync::Mutex::new(()),
            tuning,
        })
    }

    fn issuer(&self) -> &str {
        &self.config.issuer_url
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache.lock().expect("oidc cache lock poisoned")
    }

    async fn get(&self, url: &str) -> Result<Value, String> {
        let fail = |e: String| format!("GET {url}: {e}");
        let res = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| fail(e.to_string()))?;
        if !res.status().is_success() {
            return Err(fail(res.status().to_string()));
        }
        let bytes = body(res).await.map_err(fail)?;
        serde_json::from_slice(&bytes).map_err(|e| fail(e.to_string()))
    }

    /// What the cache holds for `need`, if it is enough.
    fn held(&self, need: &Need) -> Option<Held> {
        let cache = self.cache();
        let metadata = cache.metadata.clone()?;
        match need {
            Need::Metadata => Some((metadata, None)),
            Need::Keys => Some((metadata, Some(cache.keys.clone()?))),
            Need::NewKeys(seen) => {
                let keys = cache.keys.clone()?;
                let recent = cache
                    .keys_at
                    .is_some_and(|at| at.elapsed() < self.tuning.min_refresh);
                (!Arc::ptr_eq(seen, &keys) || recent).then_some((metadata, Some(keys)))
            }
        }
    }

    /// The failure a read within [`Tuning::retry_after`] ended in.
    fn failed(&self) -> Option<Failure> {
        let cache = self.cache();
        let (at, why) = cache.failed.as_ref()?;
        (at.elapsed() < self.tuning.retry_after).then(|| {
            Failure::Provider(format!(
                "not read again for {} s after: {why}",
                self.tuning.retry_after.as_secs()
            ))
        })
    }

    /// What `need` takes from the cache, reading the provider when the
    /// cache does not hold it. One request reads at a time, outside the
    /// cache lock; others wait for it as long as it can take
    /// ([`Tuning::wait`]), then find
    /// its outcome in the cache. A failed read answers for
    /// [`Tuning::retry_after`] without another.
    async fn load(&self, need: Need) -> Result<Held, Failure> {
        if let Some(held) = self.held(&need) {
            return Ok(held);
        }
        if let Some(f) = self.failed() {
            return Err(f);
        }
        let _reading = match self.reading.try_lock() {
            Ok(g) => g,
            Err(_) => tokio::time::timeout(self.tuning.wait(), self.reading.lock())
                .await
                .map_err(|_| Failure::Provider("a read of the provider is still running".into()))?,
        };
        if let Some(held) = self.held(&need) {
            return Ok(held);
        }
        if let Some(f) = self.failed() {
            return Err(f);
        }
        match self.read(&need).await {
            Ok(()) => self
                .held(&need)
                .ok_or_else(|| Failure::Provider("the read left nothing".into())),
            Err(why) => {
                self.cache().failed = Some((Instant::now(), why.clone()));
                Err(Failure::Provider(why))
            }
        }
    }

    /// Reads what `need` takes into the cache.
    async fn read(&self, need: &Need) -> Result<(), String> {
        let metadata = self.cache().metadata.clone();
        let metadata = match metadata {
            Some(m) => m,
            None => {
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    self.issuer().trim_end_matches('/')
                );
                let metadata: Metadata = serde_json::from_value(self.get(&url).await?)
                    .map_err(|e| format!("{url}: {e}"))?;
                if metadata.issuer != self.issuer() {
                    return Err(format!(
                        "{url} names the issuer {:?}, not {:?}",
                        metadata.issuer,
                        self.issuer()
                    ));
                }
                let endpoints = [
                    Some(&metadata.authorization_endpoint),
                    Some(&metadata.token_endpoint),
                    Some(&metadata.jwks_uri),
                    metadata.end_session_endpoint.as_ref(),
                ];
                for e in endpoints.into_iter().flatten() {
                    secure_url(e).map_err(|why| format!("{url}: {why}"))?;
                }
                let metadata = Arc::new(metadata);
                self.cache().metadata = Some(metadata.clone());
                metadata
            }
        };
        if matches!(need, Need::Metadata) {
            return Ok(());
        }
        let set = self.get(&metadata.jwks_uri).await?;
        let keys = set["keys"]
            .as_array()
            .ok_or_else(|| format!("{}: no keys", metadata.jwks_uri))?
            .iter()
            .filter_map(|k| serde_json::from_value::<Jwk>(k.clone()).ok())
            .collect();
        let mut cache = self.cache();
        cache.keys = Some(Arc::new(keys));
        cache.keys_at = Some(Instant::now());
        cache.failed = None;
        Ok(())
    }

    async fn metadata(&self) -> Result<Arc<Metadata>, Failure> {
        Ok(self.load(Need::Metadata).await?.0)
    }

    /// Verifies a token of the provider. Its header is checked before
    /// anything is read; the keys are read again once when it names one
    /// they do not hold.
    async fn verify(&self, token: &str, expect: &Expect<'_>) -> Result<Value, Failure> {
        header(token).map_err(Failure::Token)?;
        let keys = self.load(Need::Keys).await?.1.expect("keys were asked for");
        match verify(token, &keys, expect, auth::now()) {
            Err(Invalid::UnknownKey) => {
                let keys = self
                    .load(Need::NewKeys(keys))
                    .await?
                    .1
                    .expect("keys were asked for");
                verify(token, &keys, expect, auth::now()).map_err(Failure::Token)
            }
            r => r.map_err(Failure::Token),
        }
    }

    /// The identity of a token's claims, or `None` when they grant no
    /// role.
    fn identity(&self, claims: &Value) -> Option<Identity> {
        let c = &self.config;
        let values = claim_values(claims, &c.role_claim);
        Some(Identity {
            role: role(&values, &c.operator_role, c.viewer_role.as_deref())?,
            user: Some(user(claims)?),
            name: name(claims),
        })
    }

    /// The identity of an access token sent as a bearer, and its `exp`.
    /// It has to be an access token issued to one of the allowed clients
    /// (`azp`), and to name the configured audience when there is one.
    pub(crate) async fn bearer(&self, token: &str) -> Result<(Identity, u64), Failure> {
        let expect = Expect {
            issuer: self.issuer(),
            audience: self.config.audience.as_deref(),
            parties: &self.parties,
            require_party: true,
            nonce: None,
            access: true,
        };
        let claims = self.verify(token, &expect).await?;
        let exp = claims["exp"].as_u64().unwrap_or_default();
        Ok((self.identity(&claims).ok_or(Failure::NoRole)?, exp))
    }

    fn authorize_url(&self, metadata: &Metadata, pending: &Pending) -> Result<String, String> {
        let mut url = reqwest::Url::parse(&metadata.authorization_endpoint)
            .map_err(|e| format!("authorization_endpoint: {e}"))?;
        let mut scope = vec!["openid"];
        scope.extend(
            self.config
                .scopes
                .iter()
                .map(String::as_str)
                .filter(|s| *s != "openid"),
        );
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.config.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", &scope.join(" "))
            .append_pair("state", &pending.state)
            .append_pair("nonce", &pending.nonce)
            .append_pair("code_challenge", &pkce_challenge(&pending.verifier))
            .append_pair("code_challenge_method", "S256");
        Ok(url.into())
    }

    /// Redeems `code` at the token endpoint and returns the ID token's
    /// claims.
    async fn redeem(
        &self,
        code: &str,
        pending: &Pending,
        secret: Option<Vec<u8>>,
    ) -> Result<Value, Failure> {
        let metadata = self.metadata().await?;
        let mut form = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("redirect_uri", self.redirect_uri.clone()),
            ("code_verifier", pending.verifier.clone()),
            ("client_id", self.config.client_id.clone()),
        ];
        if let Some(secret) = secret {
            form.push((
                "client_secret",
                String::from_utf8_lossy(&secret).into_owned(),
            ));
        }
        let url = &metadata.token_endpoint;
        let fail = |e: String| Failure::Provider(format!("POST {url}: {e}"));
        let res = self
            .http
            .post(url)
            .form(&form)
            .send()
            .await
            .map_err(|e| fail(e.to_string()))?;
        let status = res.status();
        let bytes = body(res).await.map_err(fail)?;
        if !status.is_success() {
            let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
            return Err(fail(format!(
                "{status} {}",
                body["error"].as_str().unwrap_or("")
            )));
        }
        let body: Value = serde_json::from_slice(&bytes).map_err(|e| fail(e.to_string()))?;
        let id_token = body["id_token"]
            .as_str()
            .ok_or_else(|| fail("no id_token".into()))?;
        let expect = Expect {
            issuer: self.issuer(),
            audience: Some(&self.config.client_id),
            parties: std::slice::from_ref(&self.config.client_id),
            require_party: false,
            nonce: Some(&pending.nonce),
            access: false,
        };
        self.verify(id_token, &expect).await
    }

    /// Where the browser goes after its session cookie is cleared: the
    /// provider's end-session endpoint, which returns it to the public URL.
    pub(crate) async fn end_session(&self) -> Option<String> {
        let metadata = self.metadata().await.ok()?;
        let mut url = reqwest::Url::parse(metadata.end_session_endpoint.as_deref()?).ok()?;
        url.query_pairs_mut()
            .append_pair("client_id", &self.config.client_id)
            .append_pair(
                "post_logout_redirect_uri",
                &format!("{}/", self.public_origin),
            );
        Some(url.into())
    }
}

/// A path on this origin to return to after the sign-in; `/` for anything
/// else.
pub(crate) fn return_path(to: Option<&str>) -> String {
    match to {
        Some(p)
            if p.starts_with('/')
                && !p.starts_with("//")
                && !p.starts_with("/\\")
                && !p.bytes().any(|b| b.is_ascii_control()) =>
        {
            p.to_owned()
        }
        _ => "/".to_owned(),
    }
}

fn text(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        message.into(),
    )
        .into_response()
}

#[derive(Deserialize)]
pub(crate) struct LoginQuery {
    #[serde(rename = "return")]
    to: Option<String>,
}

/// Starts a sign-in at the provider; `oidc` mode only.
#[utoipa::path(
    get,
    path = "/login",
    tag = "auth",
    security(()),
    params(("return" = Option<String>, Query, description = "The path on this origin to return to; `/` when absent")),
    responses(
        (status = 303, description = "To the provider's authorization endpoint, with the sign-in's state in a cookie for 10 min"),
        (status = 403, description = "Host not allowed"),
        (status = 502, description = "The provider's metadata could not be read"),
    )
)]
pub(crate) async fn login(
    State(auth): State<Arc<Auth>>,
    Query(query): Query<LoginQuery>,
    headers: HeaderMap,
) -> Response {
    let oidc = auth.oidc().expect("routed in oidc mode");
    let draw = || -> Result<Pending, Error> {
        let verifier = random(32)?;
        Ok(Pending {
            state: random(16)?,
            nonce: random(16)?,
            verifier,
            to: return_path(query.to.as_deref()),
        })
    };
    let pending = match draw() {
        Ok(p) => p,
        Err(e) => return text(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let metadata = match oidc.metadata().await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = ?e, "oidc sign-in");
            return text(
                StatusCode::BAD_GATEWAY,
                "the identity provider cannot be reached",
            );
        }
    };
    let location = match oidc.authorize_url(&metadata, &pending) {
        Ok(u) => u,
        Err(e) => return text(StatusCode::BAD_GATEWAY, e),
    };
    let cookie = auth.login_cookie(&pending, auth.secure(&headers));
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, location),
            (header::SET_COOKIE, cookie),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response()
}

#[derive(Deserialize)]
pub(crate) struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

/// The redirect URI: redeems the code, checks the ID token and sets the
/// session cookie; `oidc` mode only.
#[utoipa::path(
    get,
    path = "/login/callback",
    tag = "auth",
    security(()),
    params(
        ("code" = Option<String>, Query, description = "The authorization code"),
        ("state" = Option<String>, Query, description = "The state the sign-in started with"),
        ("error" = Option<String>, Query, description = "The provider's error code"),
    ),
    responses(
        (status = 303, description = "Session cookie set for `sessionTtl`, 8 h by default; to the path the sign-in started from"),
        (status = 400, description = "No sign-in in progress, or another one's state"),
        (status = 401, description = "The provider refused the sign-in, or the ID token is invalid"),
        (status = 403, description = "Host not allowed, or the user has no role"),
        (status = 502, description = "The provider could not be reached or did not redeem the code"),
    )
)]
pub(crate) async fn callback(
    State(auth): State<Arc<Auth>>,
    Query(query): Query<CallbackQuery>,
    headers: HeaderMap,
) -> Response {
    let oidc = auth.oidc().expect("routed in oidc mode");
    let secure = auth.secure(&headers);
    let clear = auth.clear_login_cookie(secure);
    let refuse = |status, message: &str| {
        let mut r = text(status, format!("{message}\n"));
        r.headers_mut()
            .append(header::SET_COOKIE, clear.parse().expect("a valid header"));
        r
    };
    if let Some(error) = &query.error {
        let error: String = error
            .chars()
            .filter(|c| c.is_ascii_graphic())
            .take(64)
            .collect();
        return refuse(
            StatusCode::UNAUTHORIZED,
            &format!("sign-in refused by the identity provider: {error}"),
        );
    }
    let Some(pending) = auth.pending(&headers) else {
        return refuse(
            StatusCode::BAD_REQUEST,
            "no sign-in in progress; start again",
        );
    };
    let state = query.state.as_deref().unwrap_or_default();
    if !bool::from(state.as_bytes().ct_eq(pending.state.as_bytes())) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "the state belongs to another sign-in; start again",
        );
    }
    let Some(code) = query.code.as_deref().filter(|c| !c.is_empty()) else {
        return refuse(StatusCode::BAD_REQUEST, "no code");
    };
    let claims = match oidc.redeem(code, &pending, auth.client_secret()).await {
        Ok(c) => c,
        Err(Failure::Token(why)) => {
            tracing::warn!(?why, "oidc ID token refused");
            return refuse(StatusCode::UNAUTHORIZED, "the ID token is invalid");
        }
        Err(e) => {
            tracing::warn!(error = ?e, "oidc code redemption");
            return refuse(
                StatusCode::BAD_GATEWAY,
                "the identity provider did not redeem the code",
            );
        }
    };
    let Some(identity) = oidc.identity(&claims) else {
        return refuse(StatusCode::FORBIDDEN, "signed in, but without a role here");
    };
    // The ID token was valid just now; the session lasts sessionTtl from
    // here, whatever the ID token's own lifetime.
    let now = auth::now();
    let expiry = now + oidc.session_ttl;
    let session = match auth.oidc_session_cookie(&identity, expiry, now, secure) {
        Ok(c) => c,
        Err(e) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let mut response = (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, pending.to.clone()),
            (header::CACHE_CONTROL, "no-store".to_owned()),
        ],
    )
        .into_response();
    let h = response.headers_mut();
    h.append(header::SET_COOKIE, clear.parse().expect("a valid header"));
    h.append(header::SET_COOKIE, session.parse().expect("a valid header"));
    response
}

/// The signed payload of [`LOGIN_COOKIE`].
pub(crate) fn encode_pending(pending: &Pending) -> String {
    B64.encode(json!(pending).to_string())
}

pub(crate) fn decode_pending(s: &str) -> Option<Pending> {
    serde_json::from_slice(&B64.decode(s).ok()?).ok()
}

pub(crate) fn encode_user(user: &str) -> String {
    B64.encode(user)
}

pub(crate) fn decode_user(s: &str) -> Option<String> {
    String::from_utf8(B64.decode(s).ok()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};

    pub(crate) struct Signer {
        pair: EcdsaKeyPair,
        pub(crate) kid: String,
    }

    impl Signer {
        pub(crate) fn new(kid: &str) -> Signer {
            let rng = SystemRandom::new();
            let pkcs8 =
                EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
            let pair =
                EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
                    .unwrap();
            Signer {
                pair,
                kid: kid.into(),
            }
        }

        pub(crate) fn jwk(&self) -> Value {
            let point = self.pair.public_key().as_ref();
            json!({
                "kty": "EC", "crv": "P-256", "kid": self.kid, "use": "sig", "alg": "ES256",
                "x": B64.encode(&point[1..33]), "y": B64.encode(&point[33..65]),
            })
        }

        pub(crate) fn sign(&self, claims: &Value) -> String {
            let header = json!({ "alg": "ES256", "typ": "JWT", "kid": self.kid });
            self.sign_with(&header, claims)
        }

        pub(crate) fn sign_with(&self, header: &Value, claims: &Value) -> String {
            let input = format!(
                "{}.{}",
                B64.encode(header.to_string()),
                B64.encode(claims.to_string())
            );
            let sig = self
                .pair
                .sign(&SystemRandom::new(), input.as_bytes())
                .unwrap();
            format!("{input}.{}", B64.encode(sig.as_ref()))
        }
    }

    const NOW: u64 = 1_800_000_000;
    const ISS: &str = "https://idp.example/realms/r";

    fn claims() -> Value {
        json!({
            "iss": ISS, "aud": "faderz", "azp": "faderz", "sub": "u1",
            "preferred_username": "alice", "exp": NOW + 300, "iat": NOW, "nonce": "n1",
            "groups": ["/faderz-operator"],
        })
    }

    fn faderz() -> &'static [String] {
        static P: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        P.get_or_init(|| vec!["faderz".to_owned()])
    }

    /// What the callback asks of an ID token.
    fn expect() -> Expect<'static> {
        Expect {
            issuer: ISS,
            audience: Some("faderz"),
            parties: faderz(),
            require_party: false,
            nonce: Some("n1"),
            access: false,
        }
    }

    /// What a bearer is asked by default: an access token, `azp` among the
    /// allowed clients, no audience.
    fn bearer_expect() -> Expect<'static> {
        Expect {
            issuer: ISS,
            audience: None,
            parties: faderz(),
            require_party: true,
            nonce: None,
            access: true,
        }
    }

    fn access_claims() -> Value {
        let mut c = claims();
        c.as_object_mut().unwrap().remove("nonce");
        c["aud"] = json!("account");
        c["typ"] = json!("Bearer");
        c
    }

    fn keys(signers: &[&Signer]) -> Vec<Jwk> {
        signers
            .iter()
            .map(|s| serde_json::from_value(s.jwk()).unwrap())
            .collect()
    }

    #[test]
    fn a_valid_id_token_passes() {
        let s = Signer::new("k1");
        let c = verify(&s.sign(&claims()), &keys(&[&s]), &expect(), NOW).unwrap();
        assert_eq!(c["preferred_username"], "alice");
    }

    #[test]
    fn a_token_signed_by_another_key_is_refused() {
        let (s, other) = (Signer::new("k1"), Signer::new("k1"));
        assert_eq!(
            verify(&other.sign(&claims()), &keys(&[&s]), &expect(), NOW),
            Err(Invalid::Signature)
        );
        let token = s.sign(&claims());
        let (head, _) = token.rsplit_once('.').unwrap();
        let mut tampered = claims();
        tampered["groups"] = json!(["admins"]);
        let forged = format!(
            "{}.{}.{}",
            head.split('.').next().unwrap(),
            B64.encode(tampered.to_string()),
            token.rsplit('.').next().unwrap()
        );
        assert_eq!(
            verify(&forged, &keys(&[&s]), &expect(), NOW),
            Err(Invalid::Signature)
        );
    }

    #[test]
    fn issuer_audience_party_and_nonce_are_checked() {
        let s = Signer::new("k1");
        let k = keys(&[&s]);
        let with = |key: &str, v: Value| {
            let mut c = claims();
            c[key] = v;
            verify(&s.sign(&c), &k, &expect(), NOW)
        };
        assert_eq!(
            with("iss", json!("https://evil.example")),
            Err(Invalid::Issuer)
        );
        assert_eq!(with("aud", json!("other")), Err(Invalid::Audience));
        assert!(with("aud", json!(["other", "faderz"])).is_ok());
        assert_eq!(with("azp", json!("other")), Err(Invalid::Party));
        assert_eq!(with("nonce", json!("n2")), Err(Invalid::Nonce));
        assert_eq!(with("nonce", json!("")), Err(Invalid::Nonce));
        let mut c = claims();
        c.as_object_mut().unwrap().remove("nonce");
        assert_eq!(verify(&s.sign(&c), &k, &expect(), NOW), Err(Invalid::Nonce));
    }

    /// OIDC Core 3.1.3.7: with more than one audience, `azp` has to be
    /// present and name this client.
    #[test]
    fn an_id_token_for_several_audiences_needs_this_client_as_azp() {
        let s = Signer::new("k1");
        let k = keys(&[&s]);
        let mut c = claims();
        c["aud"] = json!(["faderz", "other"]);
        c.as_object_mut().unwrap().remove("azp");
        assert_eq!(verify(&s.sign(&c), &k, &expect(), NOW), Err(Invalid::Party));
        c["azp"] = json!("other");
        assert_eq!(verify(&s.sign(&c), &k, &expect(), NOW), Err(Invalid::Party));
        c["azp"] = json!("faderz");
        assert!(verify(&s.sign(&c), &k, &expect(), NOW).is_ok());
        c["aud"] = json!("faderz");
        c.as_object_mut().unwrap().remove("azp");
        assert!(
            verify(&s.sign(&c), &k, &expect(), NOW).is_ok(),
            "one audience, no azp"
        );
    }

    /// A bearer has to be an access token issued to an allowed client: an
    /// ID token of the same client is refused, and so is a token of
    /// another client of the provider, whatever its audience.
    #[test]
    fn a_bearer_is_an_access_token_of_an_allowed_client() {
        let s = Signer::new("k1");
        let k = keys(&[&s]);
        let check = |c: &Value, e: &Expect| verify(&s.sign(c), &k, e, NOW);
        assert!(check(&access_claims(), &bearer_expect()).is_ok());

        assert_eq!(
            check(&claims(), &bearer_expect()),
            Err(Invalid::NotAccessToken),
            "an ID token"
        );
        let mut id_typ = access_claims();
        id_typ["typ"] = json!("ID");
        assert_eq!(
            check(&id_typ, &bearer_expect()),
            Err(Invalid::NotAccessToken)
        );
        let head = |typ: &str| {
            let h = json!({ "alg": "ES256", "kid": "k1", "typ": typ });
            s.sign_with(&h, &access_claims())
        };
        assert!(verify(&head("at+jwt"), &k, &bearer_expect(), NOW).is_ok());
        assert_eq!(
            verify(&head("id+jwt"), &k, &bearer_expect(), NOW),
            Err(Invalid::NotAccessToken)
        );

        let mut other = access_claims();
        other["azp"] = json!("another-client");
        assert_eq!(check(&other, &bearer_expect()), Err(Invalid::Party));
        let mut none = access_claims();
        none.as_object_mut().unwrap().remove("azp");
        assert_eq!(
            check(&none, &bearer_expect()),
            Err(Invalid::Party),
            "azp is required"
        );
        let both = ["faderz".to_owned(), "another-client".to_owned()];
        let allowed = Expect {
            parties: &both,
            ..bearer_expect()
        };
        assert!(check(&other, &allowed).is_ok());

        let with_audience = Expect {
            audience: Some("mixer-api"),
            ..bearer_expect()
        };
        assert_eq!(
            check(&access_claims(), &with_audience),
            Err(Invalid::Audience)
        );
        let mut aud = access_claims();
        aud["aud"] = json!(["account", "mixer-api"]);
        assert!(check(&aud, &with_audience).is_ok());
    }

    #[test]
    fn lifetime_is_checked_with_leeway() {
        let s = Signer::new("k1");
        let k = keys(&[&s]);
        let token = s.sign(&claims());
        assert!(verify(&token, &k, &expect(), NOW + 300 + LEEWAY - 1).is_ok());
        assert_eq!(
            verify(&token, &k, &expect(), NOW + 300 + LEEWAY),
            Err(Invalid::Expired)
        );
        let mut c = claims();
        c["nbf"] = json!(NOW + LEEWAY + 1);
        assert_eq!(
            verify(&s.sign(&c), &k, &expect(), NOW),
            Err(Invalid::NotYetValid)
        );
        c["nbf"] = json!(NOW + LEEWAY);
        assert!(verify(&s.sign(&c), &k, &expect(), NOW).is_ok());
        c.as_object_mut().unwrap().remove("exp");
        assert_eq!(
            verify(&s.sign(&c), &k, &expect(), NOW),
            Err(Invalid::Malformed)
        );
    }

    #[test]
    fn unsigned_hmac_and_unknown_keys_are_refused() {
        let s = Signer::new("k1");
        let k = keys(&[&s]);
        let body = B64.encode(claims().to_string());
        for alg in ["none", "HS256"] {
            let head = B64.encode(json!({ "alg": alg, "kid": "k1" }).to_string());
            let t = format!("{head}.{body}.{}", B64.encode("x"));
            assert_eq!(
                verify(&t, &k, &expect(), NOW),
                Err(Invalid::Algorithm),
                "{alg}"
            );
        }
        let crit = B64.encode(json!({ "alg": "ES256", "kid": "k1", "crit": ["x"] }).to_string());
        let t = format!("{crit}.{body}.{}", B64.encode("x"));
        assert_eq!(verify(&t, &k, &expect(), NOW), Err(Invalid::Algorithm));
        let unknown = Signer::new("k2");
        assert_eq!(
            verify(&unknown.sign(&claims()), &k, &expect(), NOW),
            Err(Invalid::UnknownKey)
        );
        assert_eq!(verify("a.b", &k, &expect(), NOW), Err(Invalid::Malformed));
    }

    /// Keycloak signs with RS256 by default.
    #[test]
    fn rs256_tokens_verify_against_the_published_modulus() {
        let der = include_bytes!("../tests/fixtures/rsa-2048.der");
        let pair = signature::RsaKeyPair::from_der(der).unwrap();
        let public: RsaPublicKeyComponents<Vec<u8>> = pair.public().into();
        let (n, e) = (public.n, public.e);
        let jwk = json!({ "kty": "RSA", "kid": "r1", "n": B64.encode(n), "e": B64.encode(e) });
        let k = vec![serde_json::from_value::<Jwk>(jwk).unwrap()];
        let head = B64.encode(json!({ "alg": "RS256", "kid": "r1" }).to_string());
        let input = format!("{head}.{}", B64.encode(claims().to_string()));
        let mut sig = vec![0; pair.public().modulus_len()];
        pair.sign(
            &signature::RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            input.as_bytes(),
            &mut sig,
        )
        .unwrap();
        let token = format!("{input}.{}", B64.encode(&sig));
        assert!(verify(&token, &k, &expect(), NOW).is_ok());
        sig[0] ^= 1;
        let bad = format!("{input}.{}", B64.encode(&sig));
        assert_eq!(verify(&bad, &k, &expect(), NOW), Err(Invalid::Signature));
    }

    #[test]
    fn roles_map_from_lists_strings_paths_and_group_paths() {
        let c = json!({
            "groups": ["/faderz-operator", "x"],
            "roles": "faderz-viewer",
            "realm_access": { "roles": ["faderz-operator"] },
            "a.b": ["faderz-operator"],
        });
        assert_eq!(claim_values(&c, "groups"), ["/faderz-operator", "x"]);
        assert_eq!(claim_values(&c, "roles"), ["faderz-viewer"]);
        assert_eq!(claim_values(&c, "realm_access.roles"), ["faderz-operator"]);
        assert_eq!(
            claim_values(&c, "a.b"),
            ["faderz-operator"],
            "a top-level name first"
        );
        assert!(claim_values(&c, "missing").is_empty());

        let op = "faderz-operator";
        assert_eq!(role(&["/faderz-operator"], op, None), Some(Role::Operator));
        assert_eq!(
            role(&["faderz-operator"], op, Some("v")),
            Some(Role::Operator)
        );
        assert_eq!(role(&["other"], op, None), Some(Role::Viewer));
        assert_eq!(role(&[], op, None), Some(Role::Viewer));
        assert_eq!(role(&["v"], op, Some("v")), Some(Role::Viewer));
        assert_eq!(role(&["other"], op, Some("v")), None);
        assert_eq!(role(&["faderz-operator-x"], op, Some("v")), None);
    }

    /// The display name is `preferred_username`, else `name`, and never
    /// the email or subject id the user falls back to.
    #[test]
    fn the_name_is_never_an_email_or_a_subject() {
        let both = json!({ "preferred_username": "alice", "name": "Alice A", "email": "a@x", "sub": "s1" });
        assert_eq!(name(&both).as_deref(), Some("alice"));
        let named =
            json!({ "preferred_username": "", "name": "Alice A", "email": "a@x", "sub": "s1" });
        assert_eq!(name(&named).as_deref(), Some("Alice A"));
        let bare = json!({ "email": "a@x", "sub": "s1" });
        assert_eq!(user(&bare).as_deref(), Some("a@x"));
        assert_eq!(name(&bare), None);
    }

    #[test]
    fn pkce_challenge_is_rfc_7636_s256() {
        // RFC 7636 appendix B.
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        let (a, b) = (random(32).unwrap(), random(32).unwrap());
        assert_eq!(a.len(), 43);
        assert_ne!(a, b);
    }

    #[test]
    fn return_paths_stay_on_this_origin() {
        assert_eq!(return_path(Some("/mixer?x=1")), "/mixer?x=1");
        for bad in [
            "//evil.example",
            "/\\evil.example",
            "https://evil.example",
            "mixer",
            "/a\nb",
        ] {
            assert_eq!(return_path(Some(bad)), "/", "{bad}");
        }
        assert_eq!(return_path(None), "/");
    }

    #[test]
    fn pending_round_trips() {
        let p = Pending {
            state: "s".into(),
            nonce: "n".into(),
            verifier: "v".into(),
            to: "/".into(),
        };
        assert_eq!(decode_pending(&encode_pending(&p)), Some(p));
        assert_eq!(decode_pending("!"), None);
    }

    /// What the fake provider answers.
    #[derive(Default)]
    struct Behaviour {
        keys: Value,
        /// Every read answers 500.
        down: bool,
        /// Every read waits this long first.
        delay: Duration,
        /// Overrides the published `jwks_uri`.
        jwks_uri: Option<String>,
        /// The key set is padded past [`MAX_BODY`].
        huge: bool,
    }

    #[derive(Default)]
    struct Hits {
        metadata: std::sync::atomic::AtomicUsize,
        jwks: std::sync::atomic::AtomicUsize,
        /// Requests that followed the token endpoint's redirect.
        redirected: std::sync::atomic::AtomicUsize,
    }

    struct Fake {
        /// The issuer: the base URL with a trailing `/`.
        issuer: String,
        behaviour: Arc<std::sync::Mutex<Behaviour>>,
        hits: Arc<Hits>,
    }

    impl Fake {
        fn hits(&self) -> (usize, usize) {
            use std::sync::atomic::Ordering::SeqCst;
            (self.hits.metadata.load(SeqCst), self.hits.jwks.load(SeqCst))
        }

        fn set(&self, f: impl FnOnce(&mut Behaviour)) {
            f(&mut self.behaviour.lock().unwrap());
        }
    }

    /// A provider whose issuer ends in `/`, as some providers' do.
    async fn fake(keys: Value) -> Fake {
        use axum::routing::{get, post};
        use std::sync::atomic::Ordering::SeqCst;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let issuer = format!("{base}/");
        let behaviour = Arc::new(std::sync::Mutex::new(Behaviour {
            keys,
            ..Behaviour::default()
        }));
        let hits = Arc::new(Hits::default());
        let (b1, b2, h1, h2, h3) = (
            behaviour.clone(),
            behaviour.clone(),
            hits.clone(),
            hits.clone(),
            hits.clone(),
        );
        let (iss, base1) = (issuer.clone(), base.clone());
        let router = axum::Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || {
                    h1.metadata.fetch_add(1, SeqCst);
                    let (down, delay, jwks) = {
                        let b = b1.lock().unwrap();
                        (b.down, b.delay, b.jwks_uri.clone())
                    };
                    let (iss, base) = (iss.clone(), base1.clone());
                    async move {
                        tokio::time::sleep(delay).await;
                        if down {
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                        axum::Json(json!({
                            "issuer": iss, "authorization_endpoint": format!("{base}/auth"),
                            "token_endpoint": format!("{base}/token"),
                            "jwks_uri": jwks.unwrap_or(format!("{base}/jwks")),
                        }))
                        .into_response()
                    }
                }),
            )
            .route(
                "/jwks",
                get(move || {
                    h2.jwks.fetch_add(1, SeqCst);
                    let (down, delay, mut keys, huge) = {
                        let b = b2.lock().unwrap();
                        (b.down, b.delay, b.keys.clone(), b.huge)
                    };
                    async move {
                        tokio::time::sleep(delay).await;
                        if down {
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                        if huge {
                            keys["padding"] = json!("x".repeat(MAX_BODY));
                        }
                        axum::Json(keys).into_response()
                    }
                }),
            )
            .route(
                "/token",
                post(|| async {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(header::LOCATION, "/stolen")],
                    )
                }),
            )
            .route(
                "/stolen",
                post(move || {
                    h3.redirected.fetch_add(1, SeqCst);
                    async { "{}" }
                }),
            );
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Fake {
            issuer,
            behaviour,
            hits,
        }
    }

    fn client(issuer: &str) -> OidcConfig {
        serde_json::from_value(json!({
            "issuerUrl": issuer, "clientId": "faderz", "publicUrl": "https://faderz.example",
            "operatorRole": "faderz-operator",
        }))
        .unwrap()
    }

    fn tuned(issuer: &str, f: impl FnOnce(&mut Tuning)) -> Oidc {
        let mut t = Tuning::default();
        f(&mut t);
        Oidc::with_tuning(&client(issuer), t).unwrap()
    }

    /// An access token of `issuer` signed by `s`.
    fn access(s: &Signer, issuer: &str) -> String {
        let mut c = access_claims();
        c["iss"] = json!(issuer);
        c["exp"] = json!(auth::now() + 300);
        s.sign(&c)
    }

    #[tokio::test]
    async fn an_unknown_kid_reads_the_keys_again_once_per_interval() {
        let (old, new) = (Signer::new("old"), Signer::new("new"));
        let p = fake(json!({ "keys": [old.jwk()] })).await;
        let oidc = tuned(&p.issuer, |t| t.min_refresh = Duration::ZERO);
        let (id, exp) = oidc.bearer(&access(&old, &p.issuer)).await.unwrap();
        assert_eq!(id.role, Role::Operator);
        assert!(exp > auth::now());
        assert_eq!(p.hits(), (1, 1));
        // The provider rotates its key.
        p.set(|b| b.keys = json!({ "keys": [new.jwk()] }));
        let (id, _) = oidc.bearer(&access(&new, &p.issuer)).await.unwrap();
        assert_eq!(id.user.as_deref(), Some("alice"));
        assert_eq!(id.name.as_deref(), Some("alice"));
        assert_eq!(p.hits(), (1, 2), "read again for the new kid");
        oidc.bearer(&access(&new, &p.issuer)).await.unwrap();
        assert_eq!(p.hits(), (1, 2), "the known kid needs no read");

        let held = Oidc::new(&client(&p.issuer)).unwrap();
        held.bearer(&access(&new, &p.issuer)).await.unwrap();
        let before = p.hits();
        let stranger = Signer::new("stranger");
        for _ in 0..3 {
            assert!(matches!(
                held.bearer(&access(&stranger, &p.issuer)).await,
                Err(Failure::Token(Invalid::UnknownKey))
            ));
        }
        assert_eq!(p.hits(), before, "unknown kids cannot force reads");
    }

    /// A bearer that no key could verify is refused without a read, so a
    /// client cannot make the service call the provider with junk.
    #[tokio::test]
    async fn a_malformed_bearer_reads_nothing() {
        let p = fake(json!({ "keys": [] })).await;
        let oidc = Oidc::new(&client(&p.issuer)).unwrap();
        let hs256 = format!(
            "{}.{}.{}",
            B64.encode(json!({ "alg": "HS256" }).to_string()),
            B64.encode("{}"),
            B64.encode("x")
        );
        for junk in ["x", "a.b", "a.b.c", &hs256] {
            assert!(
                matches!(oidc.bearer(junk).await, Err(Failure::Token(_))),
                "{junk}"
            );
        }
        assert_eq!(p.hits(), (0, 0));
    }

    /// While the provider is down, one read fails and the requests after
    /// it fail at once, without reading, until the retry interval passes.
    #[tokio::test]
    async fn a_failed_read_is_not_repeated_within_the_retry_interval() {
        let s = Signer::new("k1");
        let p = fake(json!({ "keys": [s.jwk()] })).await;
        p.set(|b| b.down = true);
        let oidc = tuned(&p.issuer, |t| t.retry_after = Duration::from_millis(300));
        let token = access(&s, &p.issuer);
        for _ in 0..5 {
            assert!(matches!(
                oidc.bearer(&token).await,
                Err(Failure::Provider(_))
            ));
            assert!(matches!(oidc.metadata().await, Err(Failure::Provider(_))));
        }
        assert_eq!(p.hits(), (1, 0), "one read for all of them");
        p.set(|b| b.down = false);
        tokio::time::sleep(Duration::from_millis(350)).await;
        oidc.bearer(&token).await.unwrap();
        assert_eq!(p.hits(), (2, 1));
    }

    /// A slow read of new keys holds up neither tokens of the keys already
    /// held nor, past the read's own timeout, other requests that need a
    /// read: they find its failure.
    #[tokio::test]
    async fn a_slow_read_holds_up_no_token_of_a_known_key() {
        let s = Signer::new("k1");
        let p = fake(json!({ "keys": [s.jwk()] })).await;
        let oidc = Arc::new(tuned(&p.issuer, |t| {
            t.min_refresh = Duration::ZERO;
            t.timeout = Duration::from_millis(400);
        }));
        oidc.bearer(&access(&s, &p.issuer)).await.unwrap();
        p.set(|b| b.delay = Duration::from_secs(2));
        let stranger = access(&Signer::new("stranger"), &p.issuer);
        let slow = tokio::spawn({
            let (oidc, t) = (oidc.clone(), stranger.clone());
            async move { oidc.bearer(&t).await.map(|_| ()) }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        oidc.bearer(&access(&s, &p.issuer)).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "{:?}",
            started.elapsed()
        );
        let started = Instant::now();
        assert!(matches!(
            oidc.bearer(&stranger).await,
            Err(Failure::Provider(_))
        ));
        let waited = started.elapsed();
        assert!(waited < Duration::from_millis(800), "{waited:?}");
        assert!(matches!(slow.await.unwrap(), Err(Failure::Provider(_))));
    }

    /// The issuer is compared as configured, trailing `/` included.
    #[tokio::test]
    async fn the_issuer_is_compared_exactly() {
        let s = Signer::new("k1");
        let p = fake(json!({ "keys": [s.jwk()] })).await;
        let oidc = Oidc::new(&client(&p.issuer)).unwrap();
        oidc.bearer(&access(&s, &p.issuer)).await.unwrap();
        let trimmed = p.issuer.trim_end_matches('/');
        assert!(matches!(
            oidc.bearer(&access(&s, trimmed)).await,
            Err(Failure::Token(Invalid::Issuer))
        ));
        let other = Oidc::new(&client(trimmed)).unwrap();
        match other.metadata().await {
            Err(Failure::Provider(why)) => assert!(why.contains("names the issuer"), "{why}"),
            r => panic!("{r:?}"),
        }
    }

    #[tokio::test]
    async fn provider_endpoints_need_https_and_answers_are_bounded() {
        let s = Signer::new("k1");
        let p = fake(json!({ "keys": [s.jwk()] })).await;
        p.set(|b| b.jwks_uri = Some("http://idp.example/jwks".into()));
        let oidc = Oidc::new(&client(&p.issuer)).unwrap();
        match oidc.bearer(&access(&s, &p.issuer)).await {
            Err(Failure::Provider(why)) => assert!(why.contains("https"), "{why}"),
            r => panic!("{r:?}"),
        }
        p.set(|b| {
            b.jwks_uri = None;
            b.huge = true;
        });
        let oidc = Oidc::new(&client(&p.issuer)).unwrap();
        match oidc.bearer(&access(&s, &p.issuer)).await {
            Err(Failure::Provider(why)) => assert!(why.contains("longer than"), "{why}"),
            r => panic!("{r:?}"),
        }
    }

    /// A redirect from the token endpoint is not followed: the request
    /// carries the code, the verifier and the client secret.
    #[tokio::test]
    async fn the_token_request_follows_no_redirect() {
        let p = fake(json!({ "keys": [] })).await;
        let oidc = Oidc::new(&client(&p.issuer)).unwrap();
        let pending = Pending {
            state: "s".into(),
            nonce: "n".into(),
            verifier: "v".into(),
            to: "/".into(),
        };
        let r = oidc
            .redeem("code", &pending, Some(b"secret".to_vec()))
            .await;
        assert!(matches!(r, Err(Failure::Provider(_))), "{r:?}");
        assert_eq!(
            p.hits.redirected.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    /// The first requests after start all wait for one cold read of the
    /// metadata and the keys, which takes two of the provider's answers;
    /// none of them gives up while that read may still succeed.
    #[tokio::test]
    async fn requests_waiting_for_a_cold_read_wait_for_both_answers() {
        let s = Signer::new("k1");
        let p = fake(json!({ "keys": [s.jwk()] })).await;
        // Each answer takes most of the timeout; both together exceed it.
        p.set(|b| b.delay = Duration::from_millis(700));
        let oidc = Arc::new(tuned(&p.issuer, |t| t.timeout = Duration::from_secs(1)));
        let token = access(&s, &p.issuer);
        let first = tokio::spawn({
            let (oidc, t) = (oidc.clone(), token.clone());
            async move { oidc.bearer(&t).await.map(|_| ()) }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let waiting: Vec<_> = (0..3)
            .map(|_| {
                let (oidc, t) = (oidc.clone(), token.clone());
                tokio::spawn(async move { oidc.bearer(&t).await.map(|_| ()) })
            })
            .collect();
        first.await.unwrap().unwrap();
        for w in waiting {
            w.await.unwrap().unwrap();
        }
        assert_eq!(p.hits(), (1, 1), "one read for all of them");
    }

    #[test]
    fn session_lifetimes_parse_and_are_bounded() {
        use crate::config::Ttl;
        assert_eq!(parse_ttl(&Ttl::Seconds(3600)), Ok(3600));
        assert_eq!(parse_ttl(&Ttl::Text("8h".into())), Ok(8 * 3600));
        assert_eq!(parse_ttl(&Ttl::Text("90m".into())), Ok(5400));
        assert_eq!(parse_ttl(&Ttl::Text("2d".into())), Ok(2 * 86_400));
        assert_eq!(parse_ttl(&Ttl::Text("600".into())), Ok(600));
        for bad in ["59", "8w", "h", "-1h", "8 h", "8d"] {
            assert!(parse_ttl(&Ttl::Text(bad.into())).is_err(), "{bad}");
        }
        assert!(parse_ttl(&Ttl::Seconds(MAX_SESSION_TTL + 1)).is_err());
        let o = Oidc::new(&client("https://idp.example/")).unwrap();
        assert_eq!(o.session_ttl, DEFAULT_SESSION_TTL.as_secs());
    }

    #[test]
    fn the_client_settings_are_checked() {
        let ok = client("https://idp.example/realms/r");
        assert!(Oidc::new(&ok).is_ok());
        let o = Oidc::new(&ok).unwrap();
        assert_eq!(
            o.redirect_uri,
            "https://faderz.example/api/v1/login/callback"
        );
        let mut c = ok.clone();
        c.issuer_url = "http://idp.example".into();
        assert!(Oidc::new(&c).is_err(), "plain http off loopback");
        c.issuer_url = "http://127.0.0.1:8080/realms/r".into();
        assert!(Oidc::new(&c).is_ok());
        for bad in [
            "https://faderz.example/mixer",
            "ftp://faderz.example",
            "faderz.example",
            "http://faderz.example",
        ] {
            let mut c = ok.clone();
            c.public_url = bad.into();
            assert!(Oidc::new(&c).is_err(), "{bad}");
        }
        let mut c = ok.clone();
        c.public_url = "http://127.0.0.1:8080".into();
        assert!(Oidc::new(&c).is_ok(), "plain http on loopback");
        let mut c = ok.clone();
        c.public_url = "https://Faderz.Example:443/".into();
        assert_eq!(
            Oidc::new(&c).unwrap().redirect_uri,
            "https://faderz.example/api/v1/login/callback"
        );
    }
}
