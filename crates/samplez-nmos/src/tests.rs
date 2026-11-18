use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::Value;
use tower::ServiceExt;

use super::*;

pub(crate) const DOMAIN: Uuid = uuid::uuid!("11111111-2222-4333-8444-555555555555");
pub(crate) const OUT_FLOW: Uuid = uuid::uuid!("aaaaaaaa-0000-4000-8000-000000000001");
pub(crate) const IN_FLOW: Uuid = uuid::uuid!("aaaaaaaa-0000-4000-8000-000000000002");

/// A clock tests step by hand.
#[derive(Clone)]
pub(crate) struct Clock(pub Arc<Mutex<Tai>>);

impl Clock {
    pub fn at(sec: u64) -> Clock {
        Clock(Arc::new(Mutex::new(Tai::new(sec, 0))))
    }
}

impl TaiClock for Clock {
    fn now(&self) -> Tai {
        *self.0.lock().unwrap()
    }
}

pub(crate) fn model() -> NodeModel {
    NodeModel {
        label: "mixer".into(),
        description: "a test mixer".into(),
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
            domain: DOMAIN,
            flow: OUT_FLOW,
        }],
        receivers: vec![
            ReceiverDesc {
                key: "pgm".into(),
                label: "Mic".into(),
                channels: ChannelCount::Exactly(2),
                group: "pgm".into(),
                domain: DOMAIN,
                flow: Some(IN_FLOW),
            },
            ReceiverDesc {
                key: "spare".into(),
                label: "Spare".into(),
                channels: ChannelCount::AtLeast(3),
                group: "spare".into(),
                domain: DOMAIN,
                flow: None,
            },
        ],
    }
}

pub(crate) fn config() -> NmosConfig {
    NmosConfig {
        seed: "test/node".into(),
        advertise_host: "192.0.2.10".into(),
        advertise_port: 8081,
        asset: Asset {
            manufacturer: "danielclasen".into(),
            product: "samplez".into(),
            instance_id: "test/node".into(),
            functions: vec!["Mixer".into()],
        },
    }
}

pub(crate) async fn call(
    nmos: &Nmos,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let req = Request::builder().method(method).uri(path);
    let req = match body {
        Some(b) => req
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b.to_string())),
        None => req.body(Body::empty()),
    }
    .unwrap();
    let resp = nmos.router().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, headers, json)
}

pub(crate) async fn get(nmos: &Nmos, path: &str) -> Value {
    let (status, _, body) = call(nmos, "GET", path, None).await;
    assert_eq!(status, StatusCode::OK, "{path}: {body}");
    body
}

fn node() -> Nmos {
    Nmos::new(model(), config(), Clock::at(1_000)).unwrap()
}

const N: &str = "/x-nmos/node/v1.3";

#[tokio::test]
async fn the_api_roots_list_their_children() {
    let n = node();
    assert_eq!(get(&n, "/").await, json!(["x-nmos/"]));
    assert_eq!(get(&n, "/x-nmos/").await[0], "node/");
    assert_eq!(get(&n, "/x-nmos/node/").await, json!(["v1.3/"]));
    let mut base: Vec<String> = serde_json::from_value(get(&n, N).await).unwrap();
    base.sort();
    assert_eq!(
        base,
        [
            "devices/",
            "flows/",
            "receivers/",
            "self/",
            "senders/",
            "sources/"
        ]
    );
}

#[tokio::test]
async fn every_path_answers_with_and_without_a_trailing_slash() {
    let n = node();
    let receivers = get(&n, &format!("{N}/receivers")).await;
    assert_eq!(get(&n, &format!("{N}/receivers/")).await, receivers);
    let id = receivers[0]["id"].as_str().unwrap();
    assert_eq!(get(&n, &format!("{N}/receivers/{id}")).await, receivers[0]);
    assert_eq!(get(&n, &format!("{N}/receivers/{id}/")).await, receivers[0]);
    assert_eq!(
        get(&n, "/x-nmos/node").await,
        get(&n, "/x-nmos/node/").await
    );
}

#[tokio::test]
async fn every_response_carries_cors_and_options_is_a_preflight() {
    let n = node();
    for (method, path) in [
        ("GET", format!("{N}/self")),
        ("GET", format!("{N}/nothing")),
        ("DELETE", format!("{N}/self")),
    ] {
        let (_, h, _) = call(&n, method, &path, None).await;
        assert_eq!(
            h[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "*",
            "{method} {path}"
        );
    }
    let (status, h, _) = call(&n, "OPTIONS", &format!("{N}/receivers/x/target"), None).await;
    assert_eq!(status, StatusCode::OK);
    let methods = h[header::ACCESS_CONTROL_ALLOW_METHODS].to_str().unwrap();
    for m in ["GET", "PUT", "PATCH", "POST"] {
        assert!(methods.contains(m), "{methods}");
    }
    assert!(
        h[header::ACCESS_CONTROL_ALLOW_HEADERS]
            .to_str()
            .unwrap()
            .contains("Content-Type")
    );
}

#[tokio::test]
async fn errors_carry_the_nmos_error_body() {
    let n = node();
    let (status, _, body) = call(&n, "GET", &format!("{N}/senders/{}", Uuid::nil()), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({ "code": 404, "error": "not found", "debug": null })
    );
    let (status, h, body) = call(&n, "POST", &format!("{N}/self"), None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["code"], 405);
    assert_eq!(h[header::ALLOW], "GET, HEAD");
}

#[tokio::test]
async fn the_receiver_target_answers_501() {
    let n = node();
    let id = get(&n, &format!("{N}/receivers")).await[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (status, _, body) = call(
        &n,
        "PUT",
        &format!("{N}/receivers/{id}/target"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], 501);
    let (status, _, _) = call(
        &n,
        "PUT",
        &format!("{N}/receivers/{}/target", Uuid::nil()),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_node_and_device_advertise_the_apis_and_the_asset_tags() {
    let n = node();
    let node = get(&n, &format!("{N}/self")).await;
    assert_eq!(node["href"], "http://192.0.2.10:8081/");
    assert_eq!(
        node["api"],
        json!({ "versions": ["v1.3"], "endpoints": [{ "host": "192.0.2.10", "port": 8081, "protocol": "http", "authorization": false }] })
    );
    assert_eq!(
        node["clocks"],
        json!([{ "name": "clk0", "ref_type": "internal" }])
    );
    assert_eq!(node["interfaces"], json!([]));
    assert_eq!(
        node["tags"]["urn:x-nmos:tag:asset:manufacturer/v1.0"],
        json!(["danielclasen"])
    );
    assert!(
        node["tags"]
            .get("urn:x-nmos:tag:asset:function/v1.0")
            .is_none()
    );

    let devices = get(&n, &format!("{N}/devices")).await;
    let device = &devices[0];
    assert_eq!(devices.as_array().unwrap().len(), 1);
    assert_eq!(device["node_id"], node["id"]);
    assert_eq!(
        device["tags"]["urn:x-nmos:tag:asset:function/v1.0"],
        json!(["Mixer"])
    );
    assert_eq!(
        device["tags"]["urn:x-nmos:tag:asset:instance-id/v1.0"],
        json!(["test/node"])
    );
    assert_eq!(
        device["controls"],
        json!([{ "type": "urn:x-nmos:control:sr-ctrl/v1.2", "href": "http://192.0.2.10:8081/x-nmos/connection/v1.2/", "authorization": false }])
    );
    let senders = get(&n, &format!("{N}/senders")).await;
    let receivers = get(&n, &format!("{N}/receivers")).await;
    assert_eq!(device["senders"], json!([senders[0]["id"]]));
    assert_eq!(
        device["receivers"],
        json!([receivers[0]["id"], receivers[1]["id"]])
    );
}

#[tokio::test]
async fn an_ipv6_host_is_bracketed_in_hrefs() {
    let mut cfg = config();
    cfg.advertise_host = "2001:db8::1".into();
    let n = Nmos::new(model(), cfg, Clock::at(1)).unwrap();
    assert_eq!(
        get(&n, &format!("{N}/self")).await["href"],
        "http://[2001:db8::1]:8081/"
    );
}

#[tokio::test]
async fn senders_are_mxl_with_a_source_and_a_flow_and_no_grain_rate() {
    let n = node();
    let sender = &get(&n, &format!("{N}/senders")).await[0];
    let flow = &get(&n, &format!("{N}/flows")).await[0];
    let source = &get(&n, &format!("{N}/sources")).await[0];
    assert_eq!(sender["transport"], "urn:x-nmos:transport:mxl");
    assert_eq!(sender["interface_bindings"], json!([]));
    assert_eq!(sender["manifest_href"], Value::Null);
    assert_eq!(sender["flow_id"], flow["id"]);
    assert_eq!(
        sender["subscription"],
        json!({ "receiver_id": null, "active": true })
    );
    assert_eq!(
        sender["tags"],
        json!({ "urn:x-nmos:tag:grouphint/v1.0": ["pgm:Audio out"] })
    );
    // The IS-04 flow id is derived; the MXL flow id travels in IS-05.
    assert_ne!(flow["id"], json!(OUT_FLOW));
    assert_eq!(flow["source_id"], source["id"]);
    assert_eq!(flow["media_type"], "audio/float32");
    assert_eq!(flow["bit_depth"], 32);
    assert_eq!(
        flow["sample_rate"],
        json!({ "numerator": 48000, "denominator": 1 })
    );
    assert!(flow.get("grain_rate").is_none() && source.get("grain_rate").is_none());
    assert_eq!(source["clock_name"], "clk0");
    assert_eq!(
        source["channels"],
        json!([{ "label": "Left", "symbol": "L" }, { "label": "Right", "symbol": "U02" }])
    );
}

#[tokio::test]
async fn receivers_carry_bcp_004_01_caps_and_their_own_grouphint_role() {
    let n = node();
    let receivers = get(&n, &format!("{N}/receivers")).await;
    let r = &receivers[0];
    assert_eq!(r["transport"], "urn:x-nmos:transport:mxl");
    assert_eq!(r["format"], "urn:x-nmos:format:audio");
    assert_eq!(r["interface_bindings"], json!([]));
    // A receiver and a sender in one group take different roles.
    assert_eq!(
        r["tags"],
        json!({ "urn:x-nmos:tag:grouphint/v1.0": ["pgm:Audio in"] })
    );
    assert_eq!(
        r["subscription"],
        json!({ "sender_id": null, "active": true })
    );
    assert_eq!(receivers[1]["subscription"]["active"], false);
    assert_eq!(r["caps"]["media_types"], json!(["audio/float32"]));
    let set = &r["caps"]["constraint_sets"][0];
    assert_eq!(
        set["urn:x-nmos:cap:format:channel_count"],
        json!({ "enum": [2] })
    );
    assert_eq!(
        set["urn:x-nmos:cap:format:sample_depth"],
        json!({ "enum": [32] })
    );
    assert_eq!(set["urn:x-nmos:cap:meta:enabled"], true);
    assert_eq!(
        receivers[1]["caps"]["constraint_sets"][0]["urn:x-nmos:cap:format:channel_count"],
        json!({ "minimum": 3, "maximum": 64 })
    );
    assert!(
        r["caps"]["version"]
            .as_str()
            .unwrap()
            .parse::<Tai>()
            .is_ok()
    );
}

#[tokio::test]
async fn versions_are_tai_strings_unique_across_resources() {
    let n = node();
    let mut versions = Vec::new();
    versions.push(get(&n, &format!("{N}/self")).await["version"].clone());
    for kind in ["devices", "sources", "flows", "senders", "receivers"] {
        for r in get(&n, &format!("{N}/{kind}")).await.as_array().unwrap() {
            versions.push(r["version"].clone());
        }
    }
    let mut parsed: Vec<Tai> = versions
        .iter()
        .map(|v| v.as_str().unwrap().parse().unwrap())
        .collect();
    assert!(parsed.iter().all(|t| *t >= Tai::new(1_000, 0)));
    let n = parsed.len();
    parsed.sort();
    parsed.dedup();
    assert_eq!(parsed.len(), n);
}

#[test]
fn bump_never_repeats_or_goes_back() {
    let mut st = State {
        last_version: Tai::default(),
        node_version: Tai::default(),
        device_version: Tai::default(),
        senders: Vec::new(),
        receivers: Vec::new(),
    };
    let a = st.bump(Tai::new(5, 0));
    let b = st.bump(Tai::new(5, 0));
    let c = st.bump(Tai::new(1, 0));
    let d = st.bump(Tai::new(9, 0));
    assert!(a < b && b < c && c < d);
    assert_eq!(d, Tai::new(9, 0));
}

#[test]
fn the_model_is_checked() {
    let refuse = |f: &dyn Fn(&mut NodeModel, &mut NmosConfig)| {
        let (mut m, mut c) = (model(), config());
        f(&mut m, &mut c);
        assert!(Nmos::new(m, c, Clock::at(1)).is_err());
    };
    refuse(&|m, _| m.senders.push(m.senders[0].clone()));
    refuse(&|m, _| m.receivers[1].key = "pgm".into());
    refuse(&|m, _| m.senders[0].channels.clear());
    refuse(&|m, _| m.senders[0].channels[0].symbol = Some("X".into()));
    refuse(&|m, _| m.senders[0].channels[0].symbol = Some("U65".into()));
    refuse(&|m, _| m.receivers[0].channels = ChannelCount::Exactly(0));
    refuse(&|m, _| m.receivers[0].channels = ChannelCount::AtLeast(65));
    refuse(&|_, c| c.advertise_port = 0);
    refuse(&|_, c| c.seed.clear());
    refuse(&|_, c| c.asset.functions.clear());
    refuse(&|_, c| c.asset.manufacturer.clear());
    // One resource per role in a BCP-002-01 group, and no colon in it.
    refuse(&|m, _| {
        let mut s = m.senders[0].clone();
        s.key = "aux".into();
        m.senders.push(s);
    });
    refuse(&|m, _| m.receivers[1].group = "pgm".into());
    refuse(&|m, _| m.senders[0].group = "pgm:main".into());
    refuse(&|m, _| m.receivers[0].group = "a:b".into());
    refuse(&|m, _| m.receivers[0].group.clear());
    // Ids served as BCP-007-03 MXL UUIDs must be ones.
    refuse(&|m, _| m.senders[0].domain = Uuid::nil());
    refuse(&|m, _| m.senders[0].flow = uuid::uuid!("aaaaaaaa-0000-0000-8000-000000000001"));
    refuse(&|m, _| m.receivers[0].domain = uuid::uuid!("11111111-2222-4333-c444-555555555555"));
    refuse(&|m, _| m.receivers[0].flow = Some(Uuid::nil()));
    // A sender and a receiver may share a group: their roles differ.
    let mut m = model();
    m.receivers[1].group = "other".into();
    assert_eq!(m.senders[0].group, m.receivers[0].group);
    assert!(Nmos::new(m, config(), Clock::at(1)).is_ok());
    for ok in ["NSC128", "U64", "LFE", "NSC000"] {
        assert!(symbol_valid(ok), "{ok}");
    }
    for bad in ["NSC129", "U00", "U1", "lfe", ""] {
        assert!(!symbol_valid(bad), "{bad}");
    }
}

/// Records the warnings logged while it is the default subscriber.
struct Warnings(Mutex<Vec<String>>);

impl tracing::Subscriber for Warnings {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, e: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                }
            }
        }
        if *e.metadata().level() == tracing::Level::WARN {
            let mut m = Message(String::new());
            e.record(&mut m);
            self.0.lock().unwrap().push(m.0);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// A clock whose distance from UTC is given.
struct Offset(Option<i64>);

impl TaiClock for Offset {
    fn now(&self) -> Tai {
        Tai::new(1_000, 0)
    }
    fn utc_offset(&self) -> Option<i64> {
        self.0
    }
}

#[test]
fn a_tai_clock_without_the_tai_offset_is_warned_about_at_start() {
    let warnings = |offset| {
        let sub = Arc::new(Warnings(Mutex::new(Vec::new())));
        tracing::subscriber::with_default(sub.clone(), || {
            Nmos::new(model(), config(), Offset(offset)).unwrap();
        });
        sub.0.lock().unwrap().clone()
    };
    let w = warnings(Some(0));
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(w[0].contains("37 s off TAI"), "{w:?}");
    assert!(warnings(Some(37)).is_empty());
    assert!(warnings(None).is_empty());
}

#[tokio::test]
async fn ids_survive_a_restart_with_the_same_seed() {
    let a = node();
    let b = Nmos::new(model(), config(), Clock::at(2_000)).unwrap();
    for kind in ["devices", "sources", "flows", "senders", "receivers"] {
        let ids = |v: Value| {
            v.as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(get(&a, &format!("{N}/{kind}")).await),
            ids(get(&b, &format!("{N}/{kind}")).await)
        );
    }
    assert_eq!(a.node_id(), b.node_id());
}
