//! The response conventions the NMOS APIs share: JSON bodies, CORS on
//! every response and pre-flight on every path, and the error body
//! `{code, error, debug}`.

use axum::body::Body;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use serde_json::{Value, json};

const ALLOW_METHODS: &str = "GET, PUT, POST, PATCH, HEAD, OPTIONS, DELETE";
const ALLOW_HEADERS: &str = "Content-Type, Accept, Authorization";

pub(crate) fn json(status: StatusCode, body: &Value) -> Response {
    let mut r = Response::new(Body::from(body.to_string()));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    cors(r)
}

pub(crate) fn ok(body: &Value) -> Response {
    json(StatusCode::OK, body)
}

pub(crate) fn error(status: StatusCode, message: &str) -> Response {
    json(
        status,
        &json!({ "code": status.as_u16(), "error": message, "debug": null }),
    )
}

pub(crate) fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "not found")
}

/// 405 naming the methods the path takes.
pub(crate) fn not_allowed(allowed: &str) -> Response {
    let mut r = error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    if let Ok(v) = HeaderValue::from_str(allowed) {
        r.headers_mut().insert(header::ALLOW, v);
    }
    r
}

/// The CORS pre-flight answer, the same on every path. It allows the
/// headers the browser asks for, or the usual ones when it names none.
pub(crate) fn preflight(requested: Option<&HeaderValue>) -> Response {
    let mut r = Response::new(Body::empty());
    let h = r.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(ALLOW_METHODS),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        requested
            .cloned()
            .unwrap_or(HeaderValue::from_static(ALLOW_HEADERS)),
    );
    h.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("3600"),
    );
    cors(r)
}

fn cors(mut r: Response) -> Response {
    let h = r.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("Content-Type, Location"),
    );
    r
}

pub(crate) fn is_get(method: &Method) -> bool {
    method == Method::GET || method == Method::HEAD
}

/// The path's non-empty segments: `/a//b/` and `a/b` are the same path, so
/// every resource answers with and without a trailing slash.
pub(crate) fn segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|s| !s.is_empty()).collect()
}
