//! `oidc` mode: the provider's metadata and signing keys, ID and access
//! token validation, the authorization code flow with PKCE and the role a
//! token's claims grant.
//!
//! The metadata is read on first use and kept; the signing keys are read
//! with it and again when a token names a key they do not hold, at most
//! once per [`JWKS_MIN_REFRESH`]. A provider that cannot be reached fails
//! the sign-ins and bearer requests of that moment only.

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
/// Every request to the provider is abandoned after this long.
pub const PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
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
    pub issuer: &'a str,
    /// One of `aud`.
    pub audience: &'a str,
    /// `azp`, when the token has one.
    pub authorized_party: Option<&'a str>,
    /// `nonce`; ID tokens only.
    pub nonce: Option<&'a str>,
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
    let mut parts = token.split('.');
    let (Some(h), Some(p), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Invalid::Malformed);
    };
    let header: Value = serde_json::from_slice(&b64(h)?).map_err(|_| Invalid::Malformed)?;
    if header.get("crit").is_some() {
        return Err(Invalid::Algorithm);
    }
    let alg = header["alg"].as_str().ok_or(Invalid::Malformed)?;
    let (kty, crv) = algorithm(alg).ok_or(Invalid::Algorithm)?;
    let kid = header.get("kid").and_then(Value::as_str);
    let candidates: Vec<&Jwk> = keys
        .iter()
        .filter(|k| k.kty == kty && (crv.is_none() || k.crv.as_deref() == crv))
        .filter(|k| k.alg.as_deref().is_none_or(|a| a == alg))
        .filter(|k| k.usage.as_deref().is_none_or(|u| u == "sig"))
        .filter(|k| kid.is_none() || k.kid.as_deref() == kid)
        .collect();
    let key = match candidates.as_slice() {
        [key] => *key,
        [] => return Err(Invalid::UnknownKey),
        // Without a kid, or with keys sharing one, the signature decides.
        many => {
            let message = format!("{h}.{p}");
            let sig = b64(s)?;
            *many
                .iter()
                .find(|k| check_signature(alg, k, message.as_bytes(), &sig).is_ok())
                .ok_or(Invalid::Signature)?
        }
    };
    check_signature(alg, key, format!("{h}.{p}").as_bytes(), &b64(s)?)?;

    let claims: Value = serde_json::from_slice(&b64(p)?).map_err(|_| Invalid::Malformed)?;
    if !claims.is_object() {
        return Err(Invalid::Malformed);
    }
    if claims["iss"].as_str() != Some(expect.issuer) {
        return Err(Invalid::Issuer);
    }
    let audience_ok = match &claims["aud"] {
        Value::String(a) => a == expect.audience,
        Value::Array(a) => a.iter().any(|v| v.as_str() == Some(expect.audience)),
        _ => false,
    };
    if !audience_ok {
        return Err(Invalid::Audience);
    }
    if let (Some(azp), Some(want)) = (claims.get("azp"), expect.authorized_party)
        && azp.as_str() != Some(want)
    {
        return Err(Invalid::Audience);
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

#[derive(Default)]
struct Cache {
    metadata: Option<Arc<Metadata>>,
    keys: Arc<Vec<Jwk>>,
    fetched: Option<Instant>,
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
    issuer: String,
    pub(crate) redirect_uri: String,
    /// The public URL's origin, normalized.
    pub(crate) public_origin: String,
    pub(crate) public_host: String,
    pub(crate) public_https: bool,
    audience: String,
    http: reqwest::Client,
    cache: tokio::sync::Mutex<Cache>,
    min_refresh: Duration,
}

fn loopback(url: &reqwest::Url) -> bool {
    let host = url.host_str().unwrap_or_default();
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|a| a.is_loopback())
}

impl Oidc {
    pub(crate) fn new(config: &OidcConfig) -> Result<Oidc, Error> {
        let bad = |what: &str, why: String| Error::Config(format!("auth.oidc.{what}: {why}"));
        let issuer = config.issuer_url.trim_end_matches('/').to_owned();
        let issuer_url =
            reqwest::Url::parse(&issuer).map_err(|e| bad("issuerUrl", e.to_string()))?;
        if issuer_url.scheme() != "https"
            && !(issuer_url.scheme() == "http" && loopback(&issuer_url))
        {
            return Err(bad(
                "issuerUrl",
                "https, or http to a loopback address".into(),
            ));
        }
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
        let http = reqwest::Client::builder()
            .timeout(PROVIDER_TIMEOUT)
            .build()
            .map_err(|e| Error::Config(format!("the HTTP client for the provider: {e}")))?;
        Ok(Oidc {
            issuer,
            redirect_uri: format!("{public_origin}{CALLBACK_PATH}"),
            public_https: public.scheme() == "https",
            public_origin,
            public_host,
            audience: config.audience.clone().unwrap_or(config.client_id.clone()),
            config: config.clone(),
            http,
            cache: tokio::sync::Mutex::default(),
            min_refresh: JWKS_MIN_REFRESH,
        })
    }

    async fn get(&self, url: &str) -> Result<Value, Failure> {
        let fail = |e: String| Failure::Provider(format!("GET {url}: {e}"));
        let res = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| fail(e.to_string()))?;
        if !res.status().is_success() {
            return Err(fail(res.status().to_string()));
        }
        let bytes = res.bytes().await.map_err(|e| fail(e.to_string()))?;
        serde_json::from_slice(&bytes).map_err(|e| fail(e.to_string()))
    }

    async fn metadata_locked(&self, cache: &mut Cache) -> Result<Arc<Metadata>, Failure> {
        if let Some(m) = &cache.metadata {
            return Ok(m.clone());
        }
        let url = format!("{}/.well-known/openid-configuration", self.issuer);
        let metadata: Metadata = serde_json::from_value(self.get(&url).await?)
            .map_err(|e| Failure::Provider(format!("{url}: {e}")))?;
        if metadata.issuer.trim_end_matches('/') != self.issuer {
            return Err(Failure::Provider(format!(
                "{url} names the issuer {:?}, not {:?}",
                metadata.issuer, self.issuer
            )));
        }
        let metadata = Arc::new(metadata);
        cache.metadata = Some(metadata.clone());
        Ok(metadata)
    }

    async fn metadata(&self) -> Result<Arc<Metadata>, Failure> {
        self.metadata_locked(&mut *self.cache.lock().await).await
    }

    /// Reads the keys again unless they were read within the refresh
    /// interval or have changed since `seen` was taken.
    async fn keys(&self, seen: Option<&Arc<Vec<Jwk>>>) -> Result<Arc<Vec<Jwk>>, Failure> {
        let mut cache = self.cache.lock().await;
        let metadata = self.metadata_locked(&mut cache).await?;
        let stale = match (seen, cache.fetched) {
            (_, None) => true,
            (Some(seen), Some(at)) => {
                Arc::ptr_eq(seen, &cache.keys) && at.elapsed() >= self.min_refresh
            }
            (None, Some(_)) => false,
        };
        if stale {
            let set = self.get(&metadata.jwks_uri).await?;
            let keys = set["keys"]
                .as_array()
                .ok_or_else(|| Failure::Provider(format!("{}: no keys", metadata.jwks_uri)))?
                .iter()
                .filter_map(|k| serde_json::from_value::<Jwk>(k.clone()).ok())
                .collect();
            cache.keys = Arc::new(keys);
            cache.fetched = Some(Instant::now());
        }
        Ok(cache.keys.clone())
    }

    /// Verifies a token of the provider, reading the keys again once when
    /// it names one they do not hold.
    async fn verify(
        &self,
        token: &str,
        audience: &str,
        nonce: Option<&str>,
    ) -> Result<Value, Failure> {
        let expect = Expect {
            issuer: &self.issuer,
            audience,
            authorized_party: nonce.map(|_| self.config.client_id.as_str()),
            nonce,
        };
        let keys = self.keys(None).await?;
        match verify(token, &keys, &expect, auth::now()) {
            Err(Invalid::UnknownKey) => {
                let keys = self.keys(Some(&keys)).await?;
                verify(token, &keys, &expect, auth::now()).map_err(Failure::Token)
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
        })
    }

    /// The identity of an access token sent as a bearer.
    pub(crate) async fn bearer(&self, token: &str) -> Result<Identity, Failure> {
        let claims = self.verify(token, &self.audience, None).await?;
        self.identity(&claims).ok_or(Failure::NoRole)
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
        let bytes = res.bytes().await.map_err(|e| fail(e.to_string()))?;
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
        self.verify(id_token, &self.config.client_id, Some(&pending.nonce))
            .await
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

    #[cfg(test)]
    fn with_min_refresh(mut self, d: Duration) -> Oidc {
        self.min_refresh = d;
        self
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
        (status = 303, description = "Session cookie set until the ID token's expiry, 12 h at most; to the path the sign-in started from"),
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
    let exp = claims["exp"].as_u64().unwrap_or_default();
    let now = auth::now();
    let expiry = exp.min(now + auth::SESSION_TTL.as_secs());
    if expiry <= now {
        return refuse(StatusCode::UNAUTHORIZED, "the ID token has expired");
    }
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
    let session = auth.oidc_session_cookie(&identity, expiry, now, secure);
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

    fn expect() -> Expect<'static> {
        Expect {
            issuer: ISS,
            audience: "faderz",
            authorized_party: Some("faderz"),
            nonce: Some("n1"),
        }
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
        assert_eq!(with("azp", json!("other")), Err(Invalid::Audience));
        assert_eq!(with("nonce", json!("n2")), Err(Invalid::Nonce));
        assert_eq!(with("nonce", json!("")), Err(Invalid::Nonce));
        let mut c = claims();
        c.as_object_mut().unwrap().remove("nonce");
        assert_eq!(verify(&s.sign(&c), &k, &expect(), NOW), Err(Invalid::Nonce));
        let access = Expect {
            nonce: None,
            authorized_party: None,
            ..expect()
        };
        assert!(
            verify(&s.sign(&c), &k, &access, NOW).is_ok(),
            "no nonce on an access token"
        );
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

    /// A provider serving its metadata and a key set that can be swapped.
    async fn provider(
        keys: Arc<std::sync::Mutex<Value>>,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    ) -> String {
        use axum::routing::get;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let issuer = base.clone();
        let router = axum::Router::new()
            .route(
                "/.well-known/openid-configuration",
                get(move || {
                    let b = issuer.clone();
                    async move {
                        axum::Json(json!({
                            "issuer": b, "authorization_endpoint": format!("{b}/auth"),
                            "token_endpoint": format!("{b}/token"), "jwks_uri": format!("{b}/jwks"),
                        }))
                    }
                }),
            )
            .route(
                "/jwks",
                get(move || {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let k = keys.lock().unwrap().clone();
                    async move { axum::Json(k) }
                }),
            );
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        base
    }

    fn client(issuer: &str) -> OidcConfig {
        serde_json::from_value(json!({
            "issuerUrl": issuer, "clientId": "faderz", "publicUrl": "https://faderz.example",
            "operatorRole": "faderz-operator",
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn an_unknown_kid_reads_the_keys_again_once_per_interval() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (old, new) = (Signer::new("old"), Signer::new("new"));
        let keys = Arc::new(std::sync::Mutex::new(json!({ "keys": [old.jwk()] })));
        let hits = Arc::new(AtomicUsize::new(0));
        let issuer = provider(keys.clone(), hits.clone()).await;
        let token = |s: &Signer| {
            let mut c = claims();
            c["iss"] = json!(issuer);
            c["exp"] = json!(auth::now() + 300);
            c.as_object_mut().unwrap().remove("nonce");
            s.sign(&c)
        };

        let oidc = Oidc::new(&client(&issuer))
            .unwrap()
            .with_min_refresh(Duration::ZERO);
        assert_eq!(
            oidc.bearer(&token(&old)).await.unwrap().role,
            Role::Operator
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // The provider rotates its key.
        *keys.lock().unwrap() = json!({ "keys": [new.jwk()] });
        let id = oidc.bearer(&token(&new)).await.unwrap();
        assert_eq!(id.user.as_deref(), Some("alice"));
        assert_eq!(hits.load(Ordering::SeqCst), 2, "read again for the new kid");
        oidc.bearer(&token(&new)).await.unwrap();
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "the known kid needs no read"
        );

        let held = Oidc::new(&client(&issuer)).unwrap();
        held.bearer(&token(&new)).await.unwrap();
        let before = hits.load(Ordering::SeqCst);
        let stranger = Signer::new("stranger");
        for _ in 0..3 {
            assert!(matches!(
                held.bearer(&token(&stranger)).await,
                Err(Failure::Token(Invalid::UnknownKey))
            ));
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            before,
            "unknown kids cannot force reads"
        );
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
        ] {
            let mut c = ok.clone();
            c.public_url = bad.into();
            assert!(Oidc::new(&c).is_err(), "{bad}");
        }
        let mut c = ok.clone();
        c.public_url = "https://Faderz.Example:443/".into();
        assert_eq!(
            Oidc::new(&c).unwrap().redirect_uri,
            "https://faderz.example/api/v1/login/callback"
        );
    }
}
