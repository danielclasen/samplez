use std::path::{Path, PathBuf};

use samplez_service::config::AuthMode;
use samplez_service::openapi::{check_committed, document};
use utoipa::OpenApi;

#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
struct Status {
    revision: u64,
}

/// Only the OpenAPI path generated from it is used.
#[utoipa::path(get, path = "/status", tag = "example", responses((status = 200, body = Status)))]
#[allow(dead_code)]
async fn status() {}

#[derive(OpenApi)]
#[openapi(info(title = "example", version = "0.0.0"), paths(status))]
struct Product;

fn committed() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/openapi.json")
}

/// `SAMPLEZ_UPDATE_COMMITTED=1 cargo test --test openapi committed` rewrites
/// the committed file.
#[test]
fn committed_document_matches() {
    let doc = serde_json::to_value(document(Product::openapi(), AuthMode::Token)).unwrap();
    assert_eq!(doc["openapi"], "3.1.0");
    assert_eq!(doc["info"]["title"], "example");
    assert!(doc["paths"]["/api/v1/status"]["get"].is_object());
    assert!(doc["paths"]["/api/v1/login"]["post"].is_object());
    let schemes = &doc["components"]["securitySchemes"];
    assert_eq!(schemes["bearer"]["scheme"], "bearer");
    assert_eq!(schemes["session"]["in"], "cookie");
    assert_eq!(schemes["session"]["name"], "samplez_session");
    check_committed(&committed(), &doc).unwrap();
}

#[test]
fn drift_and_a_missing_file_are_reported() {
    let copy = Path::new(env!("CARGO_TARGET_TMPDIR")).join("openapi-drift.json");
    std::fs::copy(committed(), &copy).unwrap();
    let mut doc = serde_json::to_value(document(Product::openapi(), AuthMode::Token)).unwrap();
    check_committed(&copy, &doc).unwrap();

    doc["info"]["version"] = "0.0.1".into();
    let err = check_committed(&copy, &doc).unwrap_err();
    assert!(err.contains("differs"), "{err}");

    let err = check_committed(&copy.with_extension("missing"), &doc).unwrap_err();
    assert!(err.contains("SAMPLEZ_UPDATE_COMMITTED=1"), "{err}");
}

#[test]
fn security_follows_the_mode() {
    let none = serde_json::to_value(document(Product::openapi(), AuthMode::None)).unwrap();
    assert!(none.get("security").is_none());
    assert!(none["components"].get("securitySchemes").is_none());
    assert!(none["paths"].get("/api/v1/login").is_none());
    assert!(none["paths"]["/api/v1/status"]["get"].is_object());

    let proxy = serde_json::to_value(document(Product::openapi(), AuthMode::Proxy)).unwrap();
    assert!(proxy["paths"].get("/api/v1/login").is_none());
    assert_eq!(proxy["security"], serde_json::json!([{ "bearer": [] }]));
    assert!(
        proxy["components"]["securitySchemes"]
            .get("session")
            .is_none()
    );
}
