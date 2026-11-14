//! The OpenAPI 3.1 document of `/api/v1` and the drift check of committed
//! generated documents.

use std::path::Path;

use utoipa::OpenApi as _;
use utoipa::openapi::security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::openapi::{Components, OpenApi, OpenApiVersion, Paths, SecurityRequirement};

use crate::auth::SESSION_COOKIE;
use crate::config::AuthMode;

/// Set to write the generated document instead of comparing it.
pub const UPDATE_ENV: &str = "SAMPLEZ_UPDATE_COMMITTED";

#[derive(utoipa::OpenApi)]
#[openapi(paths(crate::auth::login))]
struct LoginApi;

/// The product's document with its paths under `/api/v1` and what `mode`
/// adds: in `token` mode the login route, the `bearer` and `session`
/// security schemes and a top-level requirement of either; in `proxy` mode
/// the `bearer` scheme of the machine listener, the API listener taking
/// its identity from the proxy in front; in `none` mode no security.
pub fn document(product: OpenApi, mode: AuthMode) -> OpenApi {
    let info = product.info.clone();
    let mut api = match mode {
        AuthMode::Token => LoginApi::openapi(),
        AuthMode::None | AuthMode::Proxy => OpenApi::new(info.clone(), Paths::new()),
    };
    api.merge(product);
    let mut doc = OpenApi::new(info, Paths::new()).nest("/api/v1", api);
    doc.openapi = OpenApiVersion::Version31;
    let bearer = || SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build());
    let mut schemes = Vec::new();
    match mode {
        AuthMode::None => {}
        AuthMode::Token => {
            schemes.push(("bearer", bearer()));
            schemes.push((
                "session",
                SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::new(SESSION_COOKIE))),
            ));
        }
        AuthMode::Proxy => schemes.push(("bearer", bearer())),
    }
    if !schemes.is_empty() {
        doc.security = Some(
            schemes
                .iter()
                .map(|(name, _)| SecurityRequirement::new(*name, Vec::<String>::new()))
                .collect(),
        );
        let components = doc.components.get_or_insert_with(Components::default);
        for (name, scheme) in schemes {
            components.add_security_scheme(name, scheme);
        }
    }
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
