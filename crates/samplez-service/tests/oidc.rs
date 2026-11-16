//! `oidc` mode against an OpenID provider served in-process.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use common::*;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
use samplez_service::config::AuthMode;
use samplez_service::{Ops, Service, ServiceConfig};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const WHOAMI: &str = "/api/v1/whoami";
const PUBLIC: &str = "faderz.example";
const PASSED_UPGRADE: StatusCode = StatusCode::UPGRADE_REQUIRED;

struct Key {
    pair: EcdsaKeyPair,
    kid: &'static str,
}

impl Key {
    fn new(kid: &'static str) -> Key {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        Key { pair, kid }
    }

    fn jwk(&self) -> Value {
        let p = self.pair.public_key().as_ref();
        json!({ "kty": "EC", "crv": "P-256", "kid": self.kid, "alg": "ES256", "use": "sig",
                "x": B64.encode(&p[1..33]), "y": B64.encode(&p[33..65]) })
    }

    fn sign(&self, claims: &Value) -> String {
        let head = json!({ "alg": "ES256", "typ": "JWT", "kid": self.kid });
        let input = format!(
            "{}.{}",
            B64.encode(head.to_string()),
            B64.encode(claims.to_string())
        );
        let sig = self
            .pair
            .sign(&SystemRandom::new(), input.as_bytes())
            .unwrap();
        format!("{input}.{}", B64.encode(sig.as_ref()))
    }
}

/// What the provider issues for a code it handed out.
#[derive(Clone)]
struct Grant {
    challenge: String,
    redirect_uri: String,
    claims: Value,
}

struct Provider {
    issuer: String,
    key: Key,
    grants: Mutex<HashMap<String, Grant>>,
    /// Every token request's form.
    redemptions: Mutex<Vec<HashMap<String, String>>>,
}

impl Provider {
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn claims(&self, groups: &[&str]) -> Value {
        json!({
            "iss": self.issuer, "aud": "faderz", "azp": "faderz", "sub": "u-1",
            "preferred_username": "alice", "iat": Self::now(), "exp": Self::now() + 300,
            "groups": groups,
        })
    }

    /// What the browser's visit to the authorization endpoint ends in: a
    /// code for `claims`, bound to the request's challenge and nonce.
    fn authorize(&self, location: &str, mut claims: Value) -> (String, HashMap<String, String>) {
        let url = reqwest::Url::parse(location).unwrap();
        assert!(
            location.starts_with(&format!("{}/auth?", self.issuer)),
            "{location}"
        );
        let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
        if claims.get("nonce").is_none() {
            claims["nonce"] = json!(q["nonce"]);
        }
        let code = format!("code-{}", self.grants.lock().unwrap().len());
        self.grants.lock().unwrap().insert(
            code.clone(),
            Grant {
                challenge: q["code_challenge"].clone(),
                redirect_uri: q["redirect_uri"].clone(),
                claims,
            },
        );
        (code, q)
    }
}

async fn token(
    State(p): State<Arc<Provider>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    p.redemptions.lock().unwrap().push(form.clone());
    let grant = p.grants.lock().unwrap().remove(&form["code"]);
    let Some(grant) = grant else {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "invalid_grant" })),
        )
            .into_response();
    };
    let challenge = B64.encode(Sha256::digest(form["code_verifier"].as_bytes()));
    if challenge != grant.challenge
        || form["redirect_uri"] != grant.redirect_uri
        || form["grant_type"] != "authorization_code"
        || form["client_id"] != "faderz"
    {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "invalid_grant" })),
        )
            .into_response();
    }
    axum::Json(json!({
        "access_token": "opaque", "token_type": "Bearer",
        "id_token": p.key.sign(&grant.claims),
    }))
    .into_response()
}

use axum::response::{IntoResponse, Response};

async fn provider(end_session: bool) -> Arc<Provider> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}/realms/r", listener.local_addr().unwrap());
    let p = Arc::new(Provider {
        issuer: issuer.clone(),
        key: Key::new("k1"),
        grants: Mutex::default(),
        redemptions: Mutex::default(),
    });
    let mut metadata = json!({
        "issuer": issuer, "authorization_endpoint": format!("{issuer}/auth"),
        "token_endpoint": format!("{issuer}/token"), "jwks_uri": format!("{issuer}/certs"),
    });
    if end_session {
        metadata["end_session_endpoint"] = json!(format!("{issuer}/logout"));
    }
    let jwks = json!({ "keys": [p.key.jwk()] });
    let router = Router::new()
        .route(
            "/realms/r/.well-known/openid-configuration",
            get(move || async move { axum::Json(metadata) }),
        )
        .route(
            "/realms/r/certs",
            get(move || async move { axum::Json(jwks) }),
        )
        .route("/realms/r/token", post(token))
        .with_state(p.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    p
}

fn oidc_config(p: &Provider, extra: Value) -> Value {
    let mut oidc = json!({
        "issuerUrl": p.issuer, "clientId": "faderz",
        "publicUrl": format!("https://{PUBLIC}"), "operatorRole": "faderz-operator",
    });
    for (k, v) in extra.as_object().unwrap() {
        oidc[k] = v.clone();
    }
    json!({ "auth": { "mode": "oidc", "oidc": oidc } })
}

fn oidc_service(config: Value) -> Service {
    service_from(config, Ops::default(), FakeState::new("r1", "leader"))
}

fn public(req: axum::http::request::Builder) -> axum::http::request::Builder {
    host(req, PUBLIC)
}

/// The `name=value` of the Set-Cookie named `name`.
fn set_cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap())
        .find(|v| v.starts_with(&format!("{name}=")))
}

fn pair(set_cookie: &str) -> String {
    set_cookie.split(';').next().unwrap().to_owned()
}

/// Starts a sign-in returning to `to`, has the provider issue a code for
/// `claims` and comes back with it: the callback's answer.
async fn sign_in(api: &Router, p: &Provider, to: &str, claims: Value) -> (StatusCode, HeaderMap) {
    let (st, headers, _) = send(api, public(get_req(&format!("/api/v1/login?return={to}")))).await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    let location = headers[header::LOCATION].to_str().unwrap();
    let login = set_cookie(&headers, "samplez_login").unwrap();
    let (code, q) = p.authorize(location, claims);
    let callback = format!("/api/v1/login/callback?code={code}&state={}", q["state"]);
    let (st, headers, _) = send(
        api,
        public(get_req(&callback)).header(header::COOKIE, pair(login)),
    )
    .await;
    (st, headers)
}

#[tokio::test]
async fn a_browser_signs_in_with_the_code_flow_and_pkce() {
    let p = provider(false).await;
    let service = oidc_service(oidc_config(&p, json!({ "scopes": ["profile", "openid"] })));
    let api = service.routers().api;

    let (st, headers, _) = send(&api, public(get_req("/api/v1/login?return=/mixer"))).await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    let location = headers[header::LOCATION].to_str().unwrap().to_owned();
    let login = set_cookie(&headers, "samplez_login").unwrap().to_owned();
    for attr in [
        "HttpOnly",
        "SameSite=Lax",
        "Path=/api/v1/login",
        "Max-Age=600",
        "Secure",
    ] {
        assert!(login.contains(attr), "{login} lacks {attr}");
    }
    let (code, q) = p.authorize(&location, p.claims(&["/faderz-operator"]));
    assert_eq!(q["response_type"], "code");
    assert_eq!(q["client_id"], "faderz");
    assert_eq!(
        q["redirect_uri"],
        "https://faderz.example/api/v1/login/callback"
    );
    assert_eq!(q["scope"], "openid profile");
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(q["code_challenge"].len(), 43);
    assert!(q["state"].len() >= 22 && q["nonce"].len() >= 22);

    let callback = format!("/api/v1/login/callback?code={code}&state={}", q["state"]);
    let (st, headers, _) = send(
        &api,
        public(get_req(&callback)).header(header::COOKIE, pair(&login)),
    )
    .await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    assert_eq!(headers[header::LOCATION], "/mixer");
    let redeemed = p.redemptions.lock().unwrap()[0].clone();
    assert_eq!(
        redeemed["code_verifier"].len(),
        43,
        "the verifier went to the token endpoint"
    );
    assert!(!redeemed.contains_key("client_secret"), "a public client");
    let cleared = set_cookie(&headers, "samplez_login").unwrap();
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
    let session = set_cookie(&headers, "samplez_session").unwrap().to_owned();
    for attr in ["HttpOnly", "SameSite=Strict", "Path=/api/v1", "Secure"] {
        assert!(session.contains(attr), "{session} lacks {attr}");
    }
    let max_age: u64 = session
        .split("Max-Age=")
        .nth(1)
        .and_then(|s| s.split(';').next())
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (295..=300).contains(&max_age),
        "not past the ID token's expiry: {max_age}"
    );

    let cookie = pair(&session);
    let with =
        |req: axum::http::request::Builder| public(req).header(header::COOKIE, cookie.clone());
    assert_eq!(
        send(&api, with(get_req(WHOAMI))).await.2,
        "Operator Some(\"alice\")"
    );
    assert_eq!(status(&api, with(write_req(WHOAMI))).await, StatusCode::OK);
    let (_, _, body) = send(&api, with(get_req("/api/v1/session"))).await;
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "mode": "oidc", "user": "alice", "role": "operator" })
    );
    let ws = with(upgrade_req()).header(header::ORIGIN, "https://faderz.example");
    assert_eq!(
        status(&api, ws).await,
        PASSED_UPGRADE,
        "the public origin is allowed"
    );
    let foreign = with(upgrade_req()).header(header::ORIGIN, "https://evil.example");
    assert_eq!(status(&api, foreign).await, StatusCode::FORBIDDEN);

    // The same code twice, and the cookie of a finished sign-in, get nothing.
    let again = send(
        &api,
        public(get_req(&callback)).header(header::COOKIE, pair(&login)),
    )
    .await;
    assert_ne!(again.0, StatusCode::SEE_OTHER);

    let (st, headers, body) = send(&api, with(write_req("/api/v1/logout"))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "redirect": null })
    );
    let gone = set_cookie(&headers, "samplez_session").unwrap();
    assert!(
        gone.starts_with("samplez_session=;") && gone.contains("Max-Age=0"),
        "{gone}"
    );
}

#[tokio::test]
async fn the_callback_wants_its_own_sign_in() {
    let p = provider(false).await;
    let api = oidc_service(oidc_config(&p, json!({}))).routers().api;
    let start = || public(get_req("/api/v1/login"));

    let (_, headers, _) = send(&api, start()).await;
    let login = pair(set_cookie(&headers, "samplez_login").unwrap());
    let (code, q) = p.authorize(headers[header::LOCATION].to_str().unwrap(), p.claims(&[]));
    let callback = |state: &str| format!("/api/v1/login/callback?code={code}&state={state}");

    let no_cookie = send(&api, public(get_req(&callback(&q["state"])))).await;
    assert_eq!(no_cookie.0, StatusCode::BAD_REQUEST);
    let other_state = public(get_req(&callback("other"))).header(header::COOKIE, login.clone());
    assert_eq!(status(&api, other_state).await, StatusCode::BAD_REQUEST);
    let forged = login.replacen("samplez_login=", "samplez_login=1", 1);
    let forged = public(get_req(&callback(&q["state"]))).header(header::COOKIE, forged);
    assert_eq!(status(&api, forged).await, StatusCode::BAD_REQUEST);
    let refused = public(get_req("/api/v1/login/callback?error=access_denied"))
        .header(header::COOKIE, login.clone());
    let (code_, _, body) = send(&api, refused).await;
    assert_eq!(code_, StatusCode::UNAUTHORIZED);
    assert!(body.contains("access_denied"), "{body}");
    assert!(
        p.redemptions.lock().unwrap().is_empty(),
        "nothing redeemed so far"
    );

    // Another sign-in's state with this one's cookie: the code is bound
    // to the other's verifier, which the provider checks.
    let (_, h2, _) = send(&api, start()).await;
    let (code2, q2) = p.authorize(h2[header::LOCATION].to_str().unwrap(), p.claims(&[]));
    let mixed = public(get_req(&format!(
        "/api/v1/login/callback?code={code2}&state={}",
        q["state"]
    )))
    .header(header::COOKIE, login.clone());
    assert_eq!(status(&api, mixed).await, StatusCode::BAD_GATEWAY);
    assert_ne!(q2["nonce"], q["nonce"]);

    let off_host = host(start(), "evil.example");
    assert_eq!(status(&api, off_host).await, StatusCode::FORBIDDEN);
    let (_, h, _) = send(&api, public(get_req("/api/v1/login?return=//evil.example"))).await;
    let (code3, q3) = p.authorize(h[header::LOCATION].to_str().unwrap(), p.claims(&[]));
    let back = public(get_req(&format!(
        "/api/v1/login/callback?code={code3}&state={}",
        q3["state"]
    )))
    .header(
        header::COOKIE,
        pair(set_cookie(&h, "samplez_login").unwrap()),
    );
    let (code_, headers, _) = send(&api, back).await;
    assert_eq!(code_, StatusCode::SEE_OTHER);
    assert_eq!(headers[header::LOCATION], "/", "never off this origin");
}

#[tokio::test]
async fn an_id_token_for_another_nonce_audience_or_issuer_is_refused() {
    let p = provider(false).await;
    let api = oidc_service(oidc_config(&p, json!({}))).routers().api;
    let mut cases = Vec::new();
    let mut c = p.claims(&["faderz-operator"]);
    c["nonce"] = json!("replayed");
    cases.push(("nonce", c));
    let mut c = p.claims(&["faderz-operator"]);
    c["aud"] = json!("other-client");
    cases.push(("aud", c));
    let mut c = p.claims(&["faderz-operator"]);
    c["iss"] = json!("https://evil.example");
    cases.push(("iss", c));
    let mut c = p.claims(&["faderz-operator"]);
    c["exp"] = json!(Provider::now() - 120);
    cases.push(("exp", c));
    for (what, claims) in cases {
        let (st, headers) = sign_in(&api, &p, "/", claims).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "{what}");
        assert!(set_cookie(&headers, "samplez_session").is_none(), "{what}");
    }
}

#[tokio::test]
async fn roles_come_from_the_role_claim() {
    let p = provider(true).await;
    let api = oidc_service(oidc_config(
        &p,
        json!({ "roleClaim": "realm_access.roles" }),
    ))
    .routers()
    .api;
    let mut claims = p.claims(&["faderz-operator"]);
    claims["realm_access"] = json!({ "roles": ["offline_access"] });
    let (st, headers) = sign_in(&api, &p, "/", claims).await;
    assert_eq!(st, StatusCode::SEE_OTHER, "no operator role: a viewer");
    let cookie = pair(set_cookie(&headers, "samplez_session").unwrap());
    let with =
        |req: axum::http::request::Builder| public(req).header(header::COOKIE, cookie.clone());
    assert_eq!(
        send(&api, with(get_req(WHOAMI))).await.2,
        "Viewer Some(\"alice\")"
    );
    assert_eq!(
        status(&api, with(write_req(WHOAMI))).await,
        StatusCode::FORBIDDEN
    );
    let (_, _, body) = send(&api, with(write_req("/api/v1/logout"))).await;
    let redirect = serde_json::from_str::<Value>(&body).unwrap()["redirect"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        redirect.starts_with(&format!("{}/logout?", p.issuer)),
        "{redirect}"
    );
    assert!(
        redirect.contains("post_logout_redirect_uri=https%3A%2F%2Ffaderz.example%2F"),
        "{redirect}"
    );

    let viewers = json!({ "viewerRole": "faderz-viewer", "roleClaim": "roles" });
    let api = oidc_service(oidc_config(&p, viewers)).routers().api;
    let mut claims = p.claims(&[]);
    claims["roles"] = json!(["faderz-viewer"]);
    assert_eq!(
        sign_in(&api, &p, "/", claims).await.0,
        StatusCode::SEE_OTHER
    );
    let mut claims = p.claims(&[]);
    claims["roles"] = json!("faderz-operator");
    assert_eq!(
        sign_in(&api, &p, "/", claims).await.0,
        StatusCode::SEE_OTHER
    );
    let mut claims = p.claims(&[]);
    claims["roles"] = json!(["somebody-else"]);
    let (st, headers) = sign_in(&api, &p, "/", claims).await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "neither role, with a viewer role configured"
    );
    assert!(set_cookie(&headers, "samplez_session").is_none());
}

#[tokio::test]
async fn api_clients_send_an_access_token_or_the_static_token() {
    let p = provider(false).await;
    let token_file = secret("oidc-static-token", "static-token\n");
    let mut config = oidc_config(&p, json!({ "viewerRole": "faderz-viewer" }));
    config["auth"]["token"] = json!({ "file": token_file });
    let api = oidc_service(config).routers().api;
    let bearer_req = |t: &str| get_req(WHOAMI).header(header::AUTHORIZATION, bearer(t));

    let access = p.key.sign(&p.claims(&["faderz-operator"]));
    assert_eq!(
        send(&api, bearer_req(&access)).await.2,
        "Operator Some(\"alice\")"
    );
    let write = write_req(WHOAMI).header(header::AUTHORIZATION, bearer(&access));
    assert_eq!(status(&api, write).await, StatusCode::OK);
    let no_csrf = get_req(WHOAMI)
        .method("POST")
        .header(header::AUTHORIZATION, bearer(&access));
    assert_eq!(
        status(&api, no_csrf).await,
        StatusCode::OK,
        "a bearer needs no request header"
    );
    assert_eq!(
        send(&api, bearer_req("static-token")).await.2,
        "Operator None"
    );

    let mut c = p.claims(&["faderz-operator"]);
    c["aud"] = json!("account");
    assert_eq!(
        status(&api, bearer_req(&p.key.sign(&c))).await,
        StatusCode::UNAUTHORIZED
    );
    let stranger = Key::new("k1");
    let forged = stranger.sign(&p.claims(&["faderz-operator"]));
    assert_eq!(
        status(&api, bearer_req(&forged)).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&api, bearer_req("junk")).await,
        StatusCode::UNAUTHORIZED
    );
    let nobody = p.key.sign(&p.claims(&["other"]));
    assert_eq!(
        status(&api, bearer_req(&nobody)).await,
        StatusCode::FORBIDDEN
    );
    let viewer = p.key.sign(&p.claims(&["faderz-viewer"]));
    let write = write_req(WHOAMI).header(header::AUTHORIZATION, bearer(&viewer));
    assert_eq!(status(&api, write).await, StatusCode::FORBIDDEN);

    // A bearer is the only credential once sent: no cookie fallback.
    let (status_, headers) = sign_in(&api, &p, "/", p.claims(&["faderz-operator"])).await;
    assert_eq!(status_, StatusCode::SEE_OTHER);
    let cookie = pair(set_cookie(&headers, "samplez_session").unwrap());
    let both = bearer_req("junk").header(header::COOKIE, cookie);
    assert_eq!(status(&api, both).await, StatusCode::UNAUTHORIZED);

    let audience = json!({ "audience": "mixer-api" });
    let api = oidc_service(oidc_config(&p, audience)).routers().api;
    let mut c = p.claims(&["faderz-operator"]);
    c["aud"] = json!(["mixer-api", "account"]);
    assert_eq!(
        status(&api, bearer_req(&p.key.sign(&c))).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn without_credentials_the_session_route_tells_how_to_sign_in() {
    let p = provider(false).await;
    let api = oidc_service(oidc_config(&p, json!({}))).routers().api;
    let (status_, _, body) = send(&api, public(get_req("/api/v1/session"))).await;
    assert_eq!(status_, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({ "mode": "oidc", "user": null, "role": null })
    );
    assert_eq!(
        status(&api, public(get_req(WHOAMI))).await,
        StatusCode::UNAUTHORIZED
    );
    let stale = public(get_req(WHOAMI)).header(header::COOKIE, "samplez_session=1.operator.YQ.00");
    assert_eq!(status(&api, stale).await, StatusCode::UNAUTHORIZED);
    let ws = public(upgrade_req()).header(header::ORIGIN, "https://faderz.example");
    assert_eq!(status(&api, ws).await, StatusCode::UNAUTHORIZED);

    // A session key shared by two replicas carries a session across them.
    let key = secret("oidc-session-key", "0123456789abcdef0123456789abcdef");
    let shared = json!({ "sessionKey": { "file": key } });
    let a = oidc_service(oidc_config(&p, shared.clone())).routers().api;
    let b = oidc_service(oidc_config(&p, shared)).routers().api;
    let c = oidc_service(oidc_config(&p, json!({}))).routers().api;
    let (_, headers) = sign_in(&a, &p, "/", p.claims(&["faderz-operator"])).await;
    let cookie = pair(set_cookie(&headers, "samplez_session").unwrap());
    let on = |api: &Router| {
        let api = api.clone();
        let req = public(get_req(WHOAMI)).header(header::COOKIE, cookie.clone());
        async move { status(&api, req).await }
    };
    assert_eq!(on(&b).await, StatusCode::OK);
    assert_eq!(on(&c).await, StatusCode::UNAUTHORIZED, "its own drawn key");
}

#[test]
fn oidc_config_rules() {
    let rules = |config: Value| {
        let config: ServiceConfig = serde_json::from_value(config).unwrap();
        Service::new(config, Ops::default(), FakeState::new("r1", "leader"))
    };
    let base = || {
        json!({ "auth": { "mode": "oidc", "oidc": {
            "issuerUrl": "https://idp.example/realms/r", "clientId": "faderz",
            "publicUrl": "https://faderz.example", "operatorRole": "faderz-operator",
        } } })
    };
    assert!(rules(base()).is_ok());
    let parsed: ServiceConfig = serde_json::from_value(base()).unwrap();
    assert_eq!(parsed.auth.mode, AuthMode::Oidc);
    let o = parsed.auth.oidc.unwrap();
    assert_eq!(o.role_claim, "groups");
    assert!(o.viewer_role.is_none() && o.client_secret.is_none() && o.scopes.is_empty());

    assert!(
        rules(json!({ "auth": { "mode": "oidc" } })).is_err(),
        "no oidc section"
    );
    let mut no_public = base();
    no_public["auth"]["oidc"]
        .as_object_mut()
        .unwrap()
        .remove("publicUrl");
    assert!(serde_json::from_value::<ServiceConfig>(no_public).is_err());
    let mut path = base();
    path["auth"]["oidc"]["publicUrl"] = json!("https://faderz.example/console");
    assert!(rules(path).is_err());
    let mut plain = base();
    plain["auth"]["oidc"]["issuerUrl"] = json!("http://idp.example");
    assert!(rules(plain).is_err());
    let mut missing = base();
    missing["auth"]["oidc"]["clientSecret"] = json!({ "file": "/nonexistent/secret" });
    assert!(matches!(
        rules(missing),
        Err(samplez_service::Error::Secret(..))
    ));
    let mut unknown = base();
    unknown["auth"]["oidc"]["clientSecretValue"] = json!("inline");
    assert!(serde_json::from_value::<ServiceConfig>(unknown).is_err());
}

#[tokio::test]
async fn a_confidential_client_sends_its_secret() {
    let p = provider(false).await;
    let file = secret("oidc-client-secret", "s3cret\n");
    let api = oidc_service(oidc_config(&p, json!({ "clientSecret": { "file": file } })))
        .routers()
        .api;
    let (st, _) = sign_in(&api, &p, "/", p.claims(&[])).await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    assert_eq!(p.redemptions.lock().unwrap()[0]["client_secret"], "s3cret");
}
