//! The OpenAPI 3.1 document of `/api/v1` and the drift check of committed
//! generated documents.

use std::path::Path;

use utoipa::OpenApi as _;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::openapi::{Components, OpenApi, OpenApiVersion, Paths, SecurityRequirement};

use crate::auth::SESSION_COOKIE;

/// Set to write the generated document instead of comparing it.
pub const UPDATE_ENV: &str = "SAMPLEZ_UPDATE_COMMITTED";

#[derive(utoipa::OpenApi)]
#[openapi(paths(crate::auth::login))]
struct ServiceApi;

/// The product's document with its paths and the login route under
/// `/api/v1`, the `bearer` and `session` security schemes, and a top-level
/// requirement of either. In `none` and `proxy` mode the API listener asks
/// for neither.
pub fn document(product: OpenApi) -> OpenApi {
    let info = product.info.clone();
    let mut api = ServiceApi::openapi();
    api.merge(product);
    let mut doc = OpenApi::new(info, Paths::new()).nest("/api/v1", api);
    doc.openapi = OpenApiVersion::Version31;
    let components = doc.components.get_or_insert_with(Components::default);
    components.add_security_scheme(
        "bearer",
        SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
    );
    components.add_security_scheme(
        "session",
        SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::new(SESSION_COOKIE))),
    );
    doc.security = Some(vec![
        SecurityRequirement::new("bearer", Vec::<String>::new()),
        SecurityRequirement::new("session", Vec::<String>::new()),
    ]);
    doc
}

/// Compares `generated`, pretty-printed with a trailing newline, with the
/// file at `path`; with [`UPDATE_ENV`] set, writes it there instead.
pub fn check_committed(path: &Path, generated: &serde_json::Value) -> Result<(), String> {
    let text = serde_json::to_string_pretty(generated).expect("a JSON value serializes") + "\n";
    let hint = format!("run with {UPDATE_ENV}=1 and commit {}", path.display());
    if std::env::var_os(UPDATE_ENV).is_some() {
        return std::fs::write(path, text).map_err(|e| format!("writing {}: {e}", path.display()));
    }
    match std::fs::read_to_string(path) {
        Ok(committed) if committed == text => Ok(()),
        Ok(_) => Err(format!(
            "{} differs from the generated document; {hint}",
            path.display()
        )),
        Err(e) => Err(format!("reading {}: {e}; {hint}", path.display())),
    }
}
