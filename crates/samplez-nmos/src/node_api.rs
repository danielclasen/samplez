//! The IS-04 v1.3 Node API, read-only apart from the deprecated Receiver
//! `target`, which answers 501.

use axum::http::{Method, StatusCode};
use axum::response::Response;
use serde_json::{Value, json};

use crate::Inner;
use crate::http::{error, is_get, not_allowed, not_found, ok};
use crate::resources::{self, NODE_VERSION};

const RESOURCES: [&str; 6] = [
    "self",
    "devices",
    "sources",
    "flows",
    "senders",
    "receivers",
];

pub(crate) fn handle(inner: &Inner, method: &Method, path: &[&str]) -> Response {
    let get = is_get(method);
    match path {
        [] if get => ok(&json!([format!("{NODE_VERSION}/")])),
        [v] if *v == NODE_VERSION && get => ok(&json!(
            RESOURCES
                .iter()
                .map(|r| format!("{r}/"))
                .collect::<Vec<_>>()
        )),
        [] => not_allowed("GET, HEAD"),
        [v] if *v == NODE_VERSION => not_allowed("GET, HEAD"),
        [v, rest @ ..] if *v == NODE_VERSION => resource(inner, method, rest),
        _ => not_found(),
    }
}

fn resource(inner: &Inner, method: &Method, path: &[&str]) -> Response {
    let st = inner.lock();
    let list = |kind: &str| -> Option<Vec<Value>> {
        Some(match kind {
            "devices" => vec![resources::device(inner, &st)],
            "sources" => resources::sources(inner, &st),
            "flows" => resources::flows(inner, &st),
            "senders" => resources::senders(inner, &st),
            "receivers" => resources::receivers(inner, &st),
            _ => return None,
        })
    };
    match path {
        ["self"] if is_get(method) => ok(&resources::node(inner, &st)),
        ["self"] => not_allowed("GET, HEAD"),
        [kind] => match list(kind) {
            Some(all) if is_get(method) => ok(&Value::Array(all)),
            Some(_) => not_allowed("GET, HEAD"),
            None => not_found(),
        },
        [kind, id] => match list(kind).and_then(|all| all.into_iter().find(|r| r["id"] == *id)) {
            Some(r) if is_get(method) => ok(&r),
            Some(_) => not_allowed("GET, HEAD"),
            None => not_found(),
        },
        ["receivers", id, "target"] => {
            if !inner.ids.receivers.iter().any(|r| r.to_string() == *id) {
                not_found()
            } else if method == Method::PUT {
                error(
                    StatusCode::NOT_IMPLEMENTED,
                    "basic connection management is not supported; use IS-05",
                )
            } else {
                not_allowed("PUT")
            }
        }
        _ => not_found(),
    }
}
