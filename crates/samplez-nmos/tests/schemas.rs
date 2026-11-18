//! Every document the node serves, validated against the AMWA schemas at
//! the commits pinned in `specs.txt`. `fetch-specs.sh` checks them out;
//! `SAMPLEZ_NMOS_SPECS` names the directory, default `target/nmos-specs`
//! in the workspace. `cargo test --workspace` leaves this test out
//! (`test = false`); CI fetches the specifications and runs it with
//! `--test schemas`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use jsonschema::{Retrieve, Uri, Validator};
use samplez_nmos::uuid::{Uuid, uuid};
use samplez_nmos::*;
use serde_json::{Value, json};
use tower::ServiceExt;

fn specs() -> PathBuf {
    let dir = std::env::var_os("SAMPLEZ_NMOS_SPECS")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/nmos-specs"));
    assert!(
        dir.join("is-04").is_dir(),
        "no specifications at {}: run crates/samplez-nmos/tests/fetch-specs.sh",
        dir.display()
    );
    dir
}

/// Resolves `file://` references, the relative `$ref`s of the AMWA
/// schemas against the file that holds them; a file missing there is
/// looked up in `fallback`.
struct Files {
    fallback: Option<PathBuf>,
}

impl Retrieve for Files {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut path = PathBuf::from(uri.path().as_str());
        if let (false, Some(dir), Some(name)) = (path.exists(), &self.fallback, path.file_name()) {
            path = dir.join(name);
        }
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

fn validator(path: PathBuf) -> Validator {
    validator_with(path, None)
}

fn validator_with(path: PathBuf, fallback: Option<PathBuf>) -> Validator {
    let path = path
        .canonicalize()
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let schema: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft4)
        .with_base_uri(format!("file://{}", path.display()))
        .with_retriever(Files { fallback })
        .should_validate_formats(true)
        .build(&schema)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[track_caller]
fn valid(v: &Validator, doc: &Value, what: &str) {
    let errors: Vec<String> = v
        .iter_errors(doc)
        .map(|e| format!("{e} at {}", e.instance_path()))
        .collect();
    assert!(errors.is_empty(), "{what}: {errors:#?}\n{doc:#}");
}

/// Every flow is absent and every activation applies.
struct Absent;

impl Connections for Absent {
    fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
        Box::pin(async { Ok(FlowSeen::Absent) })
    }
    fn lead(&self) -> Duration {
        Duration::ZERO
    }
    fn apply(&self, _: Port, _: Active, _: Option<Tai>, _: u64) -> BoxFuture<'_, Result<(), Fail>> {
        Box::pin(async { Ok(()) })
    }
}

fn node() -> Nmos {
    let domain = uuid!("11111111-2222-4333-8444-555555555555");
    let model = NodeModel {
        label: "mixer".into(),
        description: String::new(),
        senders: vec![SenderDesc {
            key: "pgm".into(),
            label: "Programme".into(),
            channels: vec![
                ChannelDesc {
                    label: "Left".into(),
                    symbol: Some("L".into()),
                },
                ChannelDesc {
                    label: "Right".into(),
                    symbol: None,
                },
            ],
            group: "pgm".into(),
            domain,
            flow: uuid!("aaaaaaaa-0000-4000-8000-000000000001"),
        }],
        receivers: vec![
            ReceiverDesc {
                key: "a".into(),
                label: "A".into(),
                channels: ChannelCount::Exactly(2),
                group: "a".into(),
                domain,
                flow: Some(uuid!("aaaaaaaa-0000-4000-8000-000000000002")),
            },
            ReceiverDesc {
                key: "b".into(),
                label: "B".into(),
                channels: ChannelCount::AtLeast(1),
                group: "b".into(),
                domain,
                flow: None,
            },
        ],
    };
    let cfg = NmosConfig {
        seed: "schemas".into(),
        advertise_host: "192.0.2.10".into(),
        advertise_port: 8081,
        asset: Asset {
            manufacturer: "danielclasen".into(),
            product: "samplez".into(),
            instance_id: "schemas".into(),
            functions: vec!["Mixer".into()],
        },
    };
    Nmos::new(model, cfg, std::sync::Arc::new(Absent), &NoStore, SystemTai).unwrap()
}

async fn get(nmos: &Nmos, path: &str) -> (StatusCode, Value) {
    send(nmos, "GET", path, None).await
}

async fn send(nmos: &Nmos, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
    let req = Request::builder()
        .method(method)
        .uri(path)
        .body(body)
        .unwrap();
    let resp = nmos.router().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn the_node_api_serves_what_is_04_v1_3_and_bcp_004_01_admit() {
    let specs = specs();
    let is04 = |f: &str| validator(specs.join("is-04/APIs/schemas").join(f));
    let nmos = node();
    const N: &str = "/x-nmos/node/v1.3";

    valid(
        &is04("node.json"),
        &get(&nmos, &format!("{N}/self")).await.1,
        "self",
    );
    for (kind, list, item) in [
        ("devices", "devices.json", "device.json"),
        ("sources", "sources.json", "source_audio.json"),
        ("flows", "flows.json", "flow_audio_raw.json"),
        ("senders", "senders.json", "sender.json"),
        ("receivers", "receivers.json", "receiver_audio.json"),
    ] {
        let (_, all) = get(&nmos, &format!("{N}/{kind}")).await;
        assert!(!all.as_array().unwrap().is_empty(), "{kind}");
        valid(&is04(list), &all, kind);
        let item = is04(item);
        for r in all.as_array().unwrap() {
            valid(&item, r, kind);
            let (status, one) =
                get(&nmos, &format!("{N}/{kind}/{}", r["id"].as_str().unwrap())).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(&one, r);
        }
    }
    valid(&is04("nodeapi-base.json"), &get(&nmos, N).await.1, "base");

    let (_, receivers) = get(&nmos, &format!("{N}/receivers")).await;
    let caps = validator(specs.join("bcp-004-01/APIs/schemas/receiver_constraint_sets.json"));
    // The register's schema names BCP-004-01's parameter constraint
    // schemas by relative path, which the testing tool resolves there.
    let register = validator_with(
        specs.join("nmos-parameter-registers/capabilities/constraint_set.json"),
        Some(specs.join("bcp-004-01/APIs/schemas")),
    );
    for r in receivers.as_array().unwrap() {
        valid(&caps, r, "receiver caps");
        for set in r["caps"]["constraint_sets"].as_array().unwrap() {
            valid(&register, set, "constraint set");
        }
    }

    let (status, error) = get(&nmos, &format!("{N}/flows/nothing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    valid(&is04("error.json"), &error, "error");
}

#[tokio::test]
async fn the_connection_api_serves_what_is_05_v1_2_and_bcp_007_03_admit() {
    let specs = specs();
    let is05 = |f: &str| validator(specs.join("is-05/APIs/schemas").join(f));
    let bcp = |f: &str| validator(specs.join("bcp-007-03/APIs/schemas").join(f));
    let nmos = node();
    const C: &str = "/x-nmos/connection/v1.2";

    valid(
        &is05("connectionapi-base.json"),
        &get(&nmos, C).await.1,
        "base",
    );
    valid(
        &is05("connectionapi-single.json"),
        &get(&nmos, &format!("{C}/single")).await.1,
        "single",
    );
    valid(
        &is05("connectionapi-bulk.json"),
        &get(&nmos, &format!("{C}/bulk")).await.1,
        "bulk",
    );
    let constraints = is05("constraints-schema.json");
    let mxl_constraints = bcp("constraints-schema-mxl.json");
    let transport_type = is05("transporttype-response-schema.json");
    let error = is05("error.json");
    for (kind, port, response, leg) in [
        (
            "senders",
            "connectionapi-sender.json",
            "sender-response-schema.json",
            "sender_transport_params_mxl.json",
        ),
        (
            "receivers",
            "connectionapi-receiver.json",
            "receiver-response-schema.json",
            "receiver_transport_params_mxl.json",
        ),
    ] {
        let (port, response, leg) = (is05(port), is05(response), bcp(leg));
        let (_, ids) = get(&nmos, &format!("{C}/single/{kind}")).await;
        valid(&is05("sender-receiver-base.json"), &ids, kind);
        for id in ids.as_array().unwrap() {
            let base = format!("{C}/single/{kind}/{}", id.as_str().unwrap());
            valid(&port, &get(&nmos, &base).await.1, &base);
            let (_, c) = get(&nmos, &format!("{base}/constraints")).await;
            valid(&constraints, &c, "constraints");
            valid(&mxl_constraints, &c[0], "constraints leg");
            valid(
                &transport_type,
                &get(&nmos, &format!("{base}/transporttype")).await.1,
                "transport type",
            );
            let mut documents = vec![
                get(&nmos, &format!("{base}/staged")).await.1,
                get(&nmos, &format!("{base}/active")).await.1,
            ];
            // Staged auto, null and a peer, then an immediate and a
            // scheduled activation, each answered with the staged document.
            for body in [
                json!({ "transport_params": [{ "mxl_domain_id": "auto" }] }),
                json!({ "master_enable": false, "transport_params": [{ "mxl_flow_id": null }] }),
                json!({ "activation": { "mode": "activate_immediate" } }),
                json!({ "activation": { "mode": "activate_scheduled_relative", "requested_time": "30:0" } }),
            ] {
                let (status, v) = send(
                    &nmos,
                    "PATCH",
                    &format!("{base}/staged"),
                    Some(body.clone()),
                )
                .await;
                assert!(status.is_success(), "{body}: {v}");
                documents.push(v);
            }
            documents.push(get(&nmos, &format!("{base}/staged")).await.1);
            documents.push(get(&nmos, &format!("{base}/active")).await.1);
            for d in &documents {
                valid(&response, d, &base);
                for l in d["transport_params"].as_array().unwrap() {
                    valid(&leg, l, "transport_params leg");
                }
            }
            let (status, locked) =
                send(&nmos, "PATCH", &format!("{base}/staged"), Some(json!({}))).await;
            assert_eq!(status, StatusCode::LOCKED);
            valid(&error, &locked, "423");
            let (status, bad) = send(
                &nmos,
                "PATCH",
                &format!("{base}/staged"),
                Some(json!({ "bad": "data" })),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            valid(&error, &bad, "400");
        }
        let item = json!([{ "id": ids[0].as_str().unwrap().trim_end_matches('/'), "params": { "activation": { "mode": null } } }]);
        let (status, bulk) = send(&nmos, "POST", &format!("{C}/bulk/{kind}"), Some(item)).await;
        assert_eq!(status, StatusCode::OK);
        valid(&is05("bulk-response-schema.json"), &bulk, "bulk");
    }
    // The sender's /transportfile is 404 with an error body (BCP-007-03).
    let (_, ids) = get(&nmos, &format!("{C}/single/senders")).await;
    let (status, e) = get(
        &nmos,
        &format!(
            "{C}/single/senders/{}transportfile",
            ids[0].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    valid(&error, &e, "transportfile");
}
