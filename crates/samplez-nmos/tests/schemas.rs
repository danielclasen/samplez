//! Every document the node serves, validated against the AMWA schemas at
//! the commits pinned in `specs.txt`. `fetch-specs.sh` checks them out;
//! `SAMPLEZ_NMOS_SPECS` names the directory, default `target/nmos-specs`
//! in the workspace. `cargo test --workspace` leaves this test out
//! (`test = false`); CI fetches the specifications and runs it with
//! `--test schemas`.

use std::path::{Path, PathBuf};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use jsonschema::{Retrieve, Uri, Validator};
use samplez_nmos::uuid::uuid;
use samplez_nmos::*;
use serde_json::Value;
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
    Nmos::new(model, cfg, SystemTai).unwrap()
}

async fn get(nmos: &Nmos, path: &str) -> (StatusCode, Value) {
    let req = Request::builder().uri(path).body(Body::empty()).unwrap();
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
