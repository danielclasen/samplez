mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, header};
use common::*;
use samplez_service::config::AuthMode;
use samplez_service::{Ops, Service, ServiceConfig};
use serde_json::json;
use tower::ServiceExt;

const WHOAMI: &str = "/api/v1/whoami";
const PASSED_UPGRADE: StatusCode = StatusCode::UPGRADE_REQUIRED;

async fn login(
    router: &Router,
    req: axum::http::request::Builder,
    token: &str,
) -> (StatusCode, String) {
    let body = Body::from(json!({ "token": token }).to_string());
    let res = router
        .clone()
        .oneshot(req.body(body).unwrap())
        .await
        .unwrap();
    let cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .map(|c| c.to_str().unwrap().to_owned())
        .unwrap_or_default();
    (res.status(), cookie)
}

fn cookie_pair(set_cookie: &str) -> String {
    set_cookie.split(';').next().unwrap().to_owned()
}

#[tokio::test]
async fn none_mode_api_listener() {
    let api = service("none", "none-api").routers().api;
    assert_eq!(send(&api, get_req(WHOAMI)).await.2, "Operator None");
    let junk = get_req(WHOAMI).header(header::AUTHORIZATION, "Bearer junk");
    assert_eq!(
        status(&api, junk).await,
        StatusCode::OK,
        "Authorization is ignored"
    );

    assert_eq!(status(&api, write_req(WHOAMI)).await, StatusCode::OK);
    let no_marker = get_req(WHOAMI)
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(status(&api, no_marker).await, StatusCode::FORBIDDEN);
    let not_json = get_req(WHOAMI)
        .method("POST")
        .header("x-samplez-request", "1")
        .header(header::CONTENT_TYPE, "text/plain");
    assert_eq!(status(&api, not_json).await, StatusCode::FORBIDDEN);

    let foreign = host(get_req(WHOAMI), "evil.example:8080");
    assert_eq!(status(&api, foreign).await, StatusCode::FORBIDDEN);
    let listed = host(get_req(WHOAMI), "samplez.example");
    assert_eq!(status(&api, listed).await, StatusCode::OK);

    assert_eq!(
        status(&api, upgrade_req()).await,
        PASSED_UPGRADE,
        "no Origin"
    );
    let own = upgrade_req().header(header::ORIGIN, "http://localhost:8080");
    assert_eq!(status(&api, own).await, PASSED_UPGRADE);
    let evil = upgrade_req().header(header::ORIGIN, "https://evil.example");
    assert_eq!(status(&api, evil).await, StatusCode::FORBIDDEN);

    assert_eq!(
        status(&api, write_req("/api/v1/login")).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn token_mode_api_listener_bearer() {
    let api = service("token", "token-bearer").routers().api;
    let auth = |v: &str| get_req(WHOAMI).header(header::AUTHORIZATION, v.to_owned());

    let (code, headers, _) = send(&api, get_req(WHOAMI)).await;
    assert_eq!(code, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
    assert_eq!(
        send(&api, auth(&bearer(API_TOKEN))).await.2,
        "Operator None"
    );
    assert_eq!(
        status(&api, auth("Bearer wrong")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&api, auth("Basic YXBpLXRva2Vu")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&api, auth(MACHINE_TOKEN)).await,
        StatusCode::UNAUTHORIZED
    );

    let forged = auth("Bearer junk")
        .header("X-Forwarded-User", "admin")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&api, forged).await, StatusCode::UNAUTHORIZED);
    let headers_only = get_req(WHOAMI)
        .header("X-Forwarded-User", "admin")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&api, headers_only).await, StatusCode::UNAUTHORIZED);
    let viewer_headers = auth(&bearer(API_TOKEN))
        .header("X-Forwarded-User", "eve")
        .header("X-Forwarded-Groups", "watchers");
    assert_eq!(
        send(&api, viewer_headers).await.2,
        "Operator None",
        "identity headers ignored"
    );

    let any_host = host(auth(&bearer(API_TOKEN)), "samplez-faderz.media.svc");
    assert_eq!(status(&api, any_host).await, StatusCode::OK);
    let foreign = host(get_req(WHOAMI), "evil.example");
    assert_eq!(status(&api, foreign).await, StatusCode::FORBIDDEN);

    let plain_write = auth(&bearer(API_TOKEN))
        .method("POST")
        .header(header::CONTENT_TYPE, "text/plain");
    assert_eq!(
        status(&api, plain_write).await,
        StatusCode::OK,
        "no CSRF marker with bearer"
    );

    let ws_bearer = upgrade_req().header(header::AUTHORIZATION, bearer(API_TOKEN));
    assert_eq!(
        status(&api, ws_bearer).await,
        PASSED_UPGRADE,
        "no Origin with bearer"
    );
    let ws_foreign = upgrade_req()
        .header(header::AUTHORIZATION, bearer(API_TOKEN))
        .header(header::ORIGIN, "https://evil.example");
    assert_eq!(status(&api, ws_foreign).await, StatusCode::FORBIDDEN);
    let ws_junk = upgrade_req().header(header::AUTHORIZATION, "Bearer junk");
    assert_eq!(status(&api, ws_junk).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn token_mode_cookie_login_and_invalidation() {
    let config = config("token", "token-cookie");
    let token_file = config["auth"]["token"]["file"].as_str().unwrap().to_owned();
    let service = service_from(config, Ops::default(), FakeState::new("r1", "leader"));
    let api = service.routers().api;
    let login_req = || write_req("/api/v1/login");

    let unmarked = get_req("/api/v1/login")
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(
        login(&api, unmarked, API_TOKEN).await.0,
        StatusCode::FORBIDDEN
    );
    let foreign = host(login_req(), "evil.example");
    assert_eq!(
        login(&api, foreign, API_TOKEN).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        login(&api, login_req(), "wrong").await.0,
        StatusCode::UNAUTHORIZED
    );

    let (code, set_cookie) = login(&api, login_req(), API_TOKEN).await;
    assert_eq!(code, StatusCode::NO_CONTENT);
    for attr in [
        "HttpOnly",
        "SameSite=Strict",
        "Max-Age=43200",
        "Path=/api/v1",
    ] {
        assert!(set_cookie.contains(attr), "{set_cookie} lacks {attr}");
    }
    assert!(!set_cookie.contains("Secure"));
    let res = api
        .clone()
        .oneshot(
            login_req()
                .body(Body::from(json!({ "token": API_TOKEN }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.headers()[header::CACHE_CONTROL], "no-store");
    for proto in ["https", "HTTPS", " https , http", "https,http"] {
        let tls = login_req().header("x-forwarded-proto", proto);
        let set_cookie = login(&api, tls, API_TOKEN).await.1;
        assert!(set_cookie.contains("; Secure"), "{proto:?}");
    }
    for proto in ["http", "http, https", "httpsx"] {
        let plain = login_req().header("x-forwarded-proto", proto);
        let set_cookie = login(&api, plain, API_TOKEN).await.1;
        assert!(!set_cookie.contains("Secure"), "{proto:?}");
    }

    let cookie = cookie_pair(&set_cookie);
    let with_cookie =
        |req: axum::http::request::Builder| req.header(header::COOKIE, cookie.clone());
    assert_eq!(
        send(&api, with_cookie(get_req(WHOAMI))).await.2,
        "Operator None"
    );
    assert_eq!(
        status(&api, with_cookie(write_req(WHOAMI))).await,
        StatusCode::OK
    );
    let unmarked = with_cookie(get_req(WHOAMI).method("POST"))
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(status(&api, unmarked).await, StatusCode::FORBIDDEN);
    let junk_bearer = with_cookie(get_req(WHOAMI)).header(header::AUTHORIZATION, "Bearer junk");
    assert_eq!(
        status(&api, junk_bearer).await,
        StatusCode::UNAUTHORIZED,
        "cookie not consulted"
    );
    let ws_no_origin = with_cookie(upgrade_req());
    assert_eq!(status(&api, ws_no_origin).await, StatusCode::FORBIDDEN);
    let ws_own = with_cookie(upgrade_req()).header(header::ORIGIN, "https://samplez.example");
    assert_eq!(status(&api, ws_own).await, PASSED_UPGRADE);
    let stale_first = get_req(WHOAMI).header(
        header::COOKIE,
        format!("samplez_session=1.{}; {cookie}", "0".repeat(64)),
    );
    assert_eq!(
        status(&api, stale_first).await,
        StatusCode::OK,
        "any valid one of several cookies"
    );
    let two_auth = get_req(WHOAMI)
        .header(header::AUTHORIZATION, bearer(API_TOKEN))
        .header(header::AUTHORIZATION, bearer(API_TOKEN));
    assert_eq!(status(&api, two_auth).await, StatusCode::UNAUTHORIZED);

    std::fs::write(&token_file, "rotated-token\n").unwrap();
    service.reload_secrets().unwrap();
    assert_eq!(
        status(&api, with_cookie(get_req(WHOAMI))).await,
        StatusCode::UNAUTHORIZED
    );
    let old = get_req(WHOAMI).header(header::AUTHORIZATION, bearer(API_TOKEN));
    assert_eq!(status(&api, old).await, StatusCode::UNAUTHORIZED);
    let new = get_req(WHOAMI).header(header::AUTHORIZATION, bearer("rotated-token"));
    assert_eq!(status(&api, new).await, StatusCode::OK);
    let (code, renewed) = login(&api, login_req(), "rotated-token").await;
    assert_eq!(code, StatusCode::NO_CONTENT);
    let renewed = get_req(WHOAMI).header(header::COOKIE, cookie_pair(&renewed));
    assert_eq!(status(&api, renewed).await, StatusCode::OK);
}

#[tokio::test]
async fn proxy_mode_api_listener() {
    let api = service("proxy", "proxy-api").routers().api;
    let as_user = |groups: &str| {
        get_req(WHOAMI)
            .header("X-Forwarded-User", "alice")
            .header("X-Forwarded-Groups", groups.to_owned())
    };

    assert_eq!(
        status(&api, get_req(WHOAMI)).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&api, as_user("ops")).await.2,
        "Operator Some(\"alice\")"
    );
    assert_eq!(
        send(&api, as_user("watchers, ops")).await.2,
        "Operator Some(\"alice\")"
    );
    assert_eq!(
        send(&api, as_user("watchers")).await.2,
        "Viewer Some(\"alice\")"
    );
    assert_eq!(
        status(&api, as_user("strangers")).await,
        StatusCode::FORBIDDEN
    );

    let viewer_write = as_user("watchers")
        .method("POST")
        .header("x-samplez-request", "1")
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(status(&api, viewer_write).await, StatusCode::FORBIDDEN);
    let operator_write = as_user("ops")
        .method("POST")
        .header("x-samplez-request", "1")
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(status(&api, operator_write).await, StatusCode::OK);
    let unmarked = as_user("ops")
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json");
    assert_eq!(status(&api, unmarked).await, StatusCode::FORBIDDEN);

    let junk = as_user("ops").header(header::AUTHORIZATION, "Bearer junk");
    assert_eq!(
        status(&api, junk).await,
        StatusCode::OK,
        "Authorization is ignored"
    );
    let machine = get_req(WHOAMI).header(header::AUTHORIZATION, bearer(MACHINE_TOKEN));
    assert_eq!(
        status(&api, machine).await,
        StatusCode::UNAUTHORIZED,
        "bearer is not identity"
    );
    let foreign = host(as_user("ops"), "evil.example");
    assert_eq!(status(&api, foreign).await, StatusCode::FORBIDDEN);

    let ws_no_origin = upgrade_req()
        .header("X-Forwarded-User", "alice")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&api, ws_no_origin).await, StatusCode::FORBIDDEN);
    let ws_own = upgrade_req()
        .header("X-Forwarded-User", "alice")
        .header("X-Forwarded-Groups", "ops")
        .header(header::ORIGIN, "https://samplez.example");
    assert_eq!(status(&api, ws_own).await, PASSED_UPGRADE);

    let login = write_req("/api/v1/login")
        .header("X-Forwarded-User", "alice")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&api, login).await, StatusCode::NOT_FOUND);

    let two_users = as_user("ops").header("X-Forwarded-User", "mallory");
    assert_eq!(status(&api, two_users).await, StatusCode::UNAUTHORIZED);
    let two_groups = as_user("watchers").header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&api, two_groups).await, StatusCode::UNAUTHORIZED);
    let utf8 = get_req(WHOAMI)
        .header(
            "X-Forwarded-User",
            axum::http::HeaderValue::from_bytes("j\u{f6}rg".as_bytes()).unwrap(),
        )
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(send(&api, utf8).await.2, "Operator Some(\"j\u{f6}rg\")");
}

#[tokio::test]
async fn host_and_origin_rules() {
    let api = service("none", "host-origin").routers().api;
    let dup_host = get_req(WHOAMI).header(header::HOST, "localhost:8080");
    assert_eq!(
        status(&api, dup_host).await,
        StatusCode::FORBIDDEN,
        "two Host headers"
    );

    for origin in ["HTTPS://Samplez.Example:443/", "http://LOCALHOST:8080"] {
        let ok = upgrade_req().header(header::ORIGIN, origin);
        assert_eq!(status(&api, ok).await, PASSED_UPGRADE, "{origin}");
    }
    let other_port = upgrade_req().header(header::ORIGIN, "https://samplez.example:8443");
    assert_eq!(status(&api, other_port).await, StatusCode::FORBIDDEN);

    let plain_get = get_req("/api/v1/ws").header(header::ORIGIN, "https://evil.example");
    assert_eq!(
        status(&api, plain_get).await,
        StatusCode::FORBIDDEN,
        "ws path without upgrade"
    );
    let connect = get_req(WHOAMI)
        .method("CONNECT")
        .header(header::ORIGIN, "https://evil.example");
    assert_eq!(
        status(&api, connect).await,
        StatusCode::FORBIDDEN,
        "CONNECT"
    );
}

#[tokio::test]
async fn the_ui_needs_an_allowed_host_only() {
    let ui = Router::new().fallback(|| async { "ui" });
    let api = service("token", "ui").ui(ui).routers().api;
    assert_eq!(send(&api, get_req("/")).await.2, "ui", "no session");
    assert_eq!(send(&api, get_req("/assets/app.js")).await.2, "ui");
    let foreign = host(get_req("/"), "evil.example");
    assert_eq!(status(&api, foreign).await, StatusCode::FORBIDDEN);
    assert_eq!(
        status(&api, get_req(WHOAMI)).await,
        StatusCode::UNAUTHORIZED
    );
    let (code, _, body) = send(&api, get_req("/api/v1/nope")).await;
    assert_eq!(
        code,
        StatusCode::UNAUTHORIZED,
        "API paths never reach the UI: {body}"
    );
    let authed = get_req("/api/v1/nope").header(header::AUTHORIZATION, bearer(API_TOKEN));
    assert_eq!(status(&api, authed).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_failed_secret_reload_counts_and_keeps_the_last_load_time() {
    let config = config("token", "reload-fail");
    let token_file = config["auth"]["token"]["file"].as_str().unwrap().to_owned();
    let ops = Ops::default();
    let service = service_from(config, ops.clone(), FakeState::new("r1", "leader"));
    let source = service.status();
    let loaded = source.get().secrets[0].clone();
    assert_eq!(loaded.name, "token");
    assert!(loaded.loaded_at > 1_700_000_000);

    std::fs::remove_file(&token_file).unwrap();
    assert!(service.reload_secrets().is_err());
    assert!(
        ops.metrics()
            .render()
            .contains("samplez_secret_reload_failures_total{secret=\"token\"} 1")
    );
    assert_eq!(source.get().secrets[0], loaded);
    let api = service.routers().api;
    let still = get_req(WHOAMI).header(header::AUTHORIZATION, bearer(API_TOKEN));
    assert_eq!(
        status(&api, still).await,
        StatusCode::OK,
        "previous token kept"
    );
}

#[tokio::test]
async fn proxy_mode_machine_listener() {
    let machine = service("proxy", "proxy-machine").routers().machine.unwrap();
    let auth = |v: &str| get_req(WHOAMI).header(header::AUTHORIZATION, v.to_owned());

    assert_eq!(
        status(&machine, get_req(WHOAMI)).await,
        StatusCode::UNAUTHORIZED
    );
    let forged = auth("Bearer junk")
        .header("X-Forwarded-User", "admin")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(status(&machine, forged).await, StatusCode::UNAUTHORIZED);
    let headers_only = get_req(WHOAMI)
        .header("X-Forwarded-User", "admin")
        .header("X-Forwarded-Groups", "ops");
    assert_eq!(
        status(&machine, headers_only).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&machine, auth(&bearer(MACHINE_TOKEN))).await.2,
        "Operator None"
    );
    let viewer_headers = auth(&bearer(MACHINE_TOKEN))
        .header("X-Forwarded-User", "eve")
        .header("X-Forwarded-Groups", "watchers");
    assert_eq!(send(&machine, viewer_headers).await.2, "Operator None");

    let any_host = host(auth(&bearer(MACHINE_TOKEN)), "faderz-machine.example");
    assert_eq!(status(&machine, any_host).await, StatusCode::OK);
    let plain_write = auth(&bearer(MACHINE_TOKEN)).method("POST");
    assert_eq!(status(&machine, plain_write).await, StatusCode::OK);

    let ws = upgrade_req().header(header::AUTHORIZATION, bearer(MACHINE_TOKEN));
    assert_eq!(status(&machine, ws).await, PASSED_UPGRADE);
    let ws_foreign = upgrade_req()
        .header(header::AUTHORIZATION, bearer(MACHINE_TOKEN))
        .header(header::ORIGIN, "https://evil.example");
    assert_eq!(status(&machine, ws_foreign).await, StatusCode::FORBIDDEN);
    let ws_none = upgrade_req().header(header::ORIGIN, "https://samplez.example");
    assert_eq!(status(&machine, ws_none).await, StatusCode::UNAUTHORIZED);

    let login = write_req("/api/v1/login").header(header::AUTHORIZATION, bearer(MACHINE_TOKEN));
    assert_eq!(status(&machine, login).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ops_and_nmos_listeners_ignore_authorization() {
    for mode in ["none", "token", "proxy"] {
        let routers = service(mode, &format!("ignore-{mode}")).routers();
        assert_eq!(routers.machine.is_some(), mode == "proxy");
        for auth in [None, Some("Bearer junk")] {
            let with = |req: axum::http::request::Builder| match auth {
                Some(v) => req.header(header::AUTHORIZATION, v),
                None => req,
            };
            let nmos = with(host(get_req("/x-nmos/"), "evil.example"));
            assert_eq!(send(&routers.nmos, nmos).await.2, "nmos", "{mode} {auth:?}");
            assert_eq!(
                status(&routers.ops, with(get_req("/metrics"))).await,
                StatusCode::OK
            );
            let ready = status(&routers.ops, with(get_req("/readyz"))).await;
            assert_eq!(ready, StatusCode::SERVICE_UNAVAILABLE, "{mode} {auth:?}");
            let health = status(&routers.ops, with(get_req("/healthz"))).await;
            assert_eq!(health, StatusCode::SERVICE_UNAVAILABLE, "{mode} {auth:?}");
        }
    }
}

fn try_service(config: serde_json::Value) -> Result<Service, samplez_service::Error> {
    let config: ServiceConfig = serde_json::from_value(config).unwrap();
    Service::new(config, Ops::default(), FakeState::new("r1", "leader"))
}

#[test]
fn config_rules() {
    let mut proxy_public = config("proxy", "rules-proxy");
    proxy_public["listeners"]["api"] = json!({ "bind": "0.0.0.0", "port": 8080 });
    assert!(
        try_service(proxy_public).is_err(),
        "proxy mode API off loopback"
    );

    let mut machine_in_token = config("token", "rules-token");
    machine_in_token["listeners"] = json!({ "machine": { "bind": "0.0.0.0", "port": 8082 } });
    assert!(
        try_service(machine_in_token).is_err(),
        "machine listener outside proxy mode"
    );

    let mut machine_without_token = config("proxy", "rules-proxy2");
    machine_without_token["auth"]["machineToken"] = serde_json::Value::Null;
    assert!(try_service(machine_without_token).is_err());

    let mut token_without_file = config("token", "rules-token2");
    token_without_file["auth"]["token"] = serde_json::Value::Null;
    assert!(try_service(token_without_file).is_err());

    let mut empty = config("token", "rules-token3");
    empty["auth"]["token"]["file"] = json!(secret("rules-empty", " \n"));
    assert!(matches!(
        try_service(empty),
        Err(samplez_service::Error::Config(_))
    ));

    let mut missing = config("token", "rules-token4");
    missing["auth"]["token"]["file"] = json!("/nonexistent/token");
    assert!(matches!(
        try_service(missing),
        Err(samplez_service::Error::Secret(..))
    ));

    let mut no_roles = config("proxy", "rules-proxy3");
    no_roles["auth"]["proxy"]["groupRoles"] = json!({});
    assert!(matches!(
        try_service(no_roles),
        Err(samplez_service::Error::Config(_))
    ));

    assert!(try_service(config("proxy", "rules-ok")).is_ok());
}

#[test]
fn config_shape_and_defaults() {
    let defaults: ServiceConfig = serde_json::from_value(json!({})).unwrap();
    let l = &defaults.listeners;
    assert_eq!(
        (l.api.bind.to_string(), l.api.port),
        ("127.0.0.1".into(), 8080)
    );
    assert_eq!(
        (l.nmos.bind.to_string(), l.nmos.port),
        ("127.0.0.1".into(), 8081)
    );
    assert_eq!(
        (l.ops.bind.to_string(), l.ops.port),
        ("0.0.0.0".into(), 9090)
    );
    assert!(l.machine.is_none());
    assert_eq!(defaults.auth.mode, AuthMode::None);
    assert_eq!(
        defaults.auth.proxy.user_header, "X-Forwarded-User",
        "oauth2-proxy upstream"
    );
    assert_eq!(defaults.auth.proxy.groups_header, "X-Forwarded-Groups");

    let rendered = json!({
        "listeners": {
            "api": { "bind": "0.0.0.0", "port": 8080 },
            "machine": { "bind": "0.0.0.0", "port": 8082 },
            "ops": { "bind": "0.0.0.0", "port": 9090 },
            "nmos": { "bind": "0.0.0.0", "port": 8081 },
        },
        "auth": {
            "mode": "token",
            "token": { "file": "/run/secrets/token/token" },
            "machineToken": { "file": "/run/secrets/machine/token" },
            "proxy": { "userHeader": "X-User", "groupsHeader": "X-Groups",
                       "groupRoles": { "mixers": "operator" } },
        },
        "api": { "allowedHosts": ["faderz.example"], "allowedOrigins": ["https://faderz.example"] },
    });
    let parsed: ServiceConfig = serde_json::from_value(rendered).unwrap();
    assert_eq!(parsed.auth.mode, AuthMode::Token);
    assert_eq!(parsed.listeners.machine.unwrap().port, 8082);

    let secret_value = json!({ "auth": { "mode": "token", "token": { "value": "inline" } } });
    assert!(serde_json::from_value::<ServiceConfig>(secret_value).is_err());
}

#[tokio::test]
async fn an_api_listener_with_tls_sets_secure_cookies_and_allows_its_https_origin() {
    let mut config = config("token", "tls-cookie");
    config["listeners"] = json!({ "api": {
        "bind": "127.0.0.1",
        "port": 8080,
        "tls": { "mode": "selfSigned", "sans": ["localhost"] },
    } });
    let service = service_from(config, Ops::default(), FakeState::new("r1", "leader"));
    let api = service.routers().api;
    let (code, cookie) = login(&api, write_req("/api/v1/login"), API_TOKEN).await;
    assert_eq!(code, StatusCode::NO_CONTENT);
    assert!(cookie.contains("; Secure"), "{cookie}");

    let auth = header::AUTHORIZATION;
    let https = upgrade_req()
        .header(header::ORIGIN, "https://localhost:8080")
        .header(auth.clone(), bearer(API_TOKEN));
    assert_eq!(status(&api, https).await, PASSED_UPGRADE);
    let http = upgrade_req()
        .header(header::ORIGIN, "http://localhost:8080")
        .header(auth, bearer(API_TOKEN));
    assert_eq!(status(&api, http).await, StatusCode::FORBIDDEN);
}
