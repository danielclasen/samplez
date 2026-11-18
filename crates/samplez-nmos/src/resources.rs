//! The IS-04 v1.3 resources rendered from the model and the ports' state.

use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::model::{BIT_DEPTH, ChannelCount, MAX_CHANNELS, MEDIA_TYPE, SAMPLE_RATE};
use crate::tai::Tai;
use crate::{Inner, State};

pub(crate) const MXL: &str = "urn:x-nmos:transport:mxl";
const AUDIO: &str = "urn:x-nmos:format:audio";
const CLOCK: &str = "clk0";
const GROUPHINT: &str = "urn:x-nmos:tag:grouphint/v1.0";
pub(crate) const NODE_VERSION: &str = "v1.3";
pub(crate) const CONNECTION_VERSION: &str = "v1.2";

/// `http://host:port`, with an IPv6 address in brackets.
pub(crate) fn base_url(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

fn core(id: Uuid, version: Tai, label: &str, description: &str, tags: Value) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("id".into(), json!(id));
    m.insert("version".into(), json!(version.to_string()));
    m.insert("label".into(), json!(label));
    m.insert("description".into(), json!(description));
    m.insert("tags".into(), tags);
    m
}

fn with(mut m: Map<String, Value>, rest: Value) -> Value {
    if let Value::Object(r) = rest {
        m.extend(r);
    }
    Value::Object(m)
}

fn asset_tags(inner: &Inner, device: bool) -> Value {
    let a = &inner.cfg.asset;
    let mut t = json!({
        "urn:x-nmos:tag:asset:manufacturer/v1.0": [a.manufacturer],
        "urn:x-nmos:tag:asset:product/v1.0": [a.product],
        "urn:x-nmos:tag:asset:instance-id/v1.0": [a.instance_id],
    });
    if device {
        t["urn:x-nmos:tag:asset:function/v1.0"] = json!(a.functions);
    }
    t
}

pub(crate) fn node(inner: &Inner, st: &State) -> Value {
    let cfg = &inner.cfg;
    let m = &inner.model;
    with(
        core(
            inner.ids.node,
            st.node_version,
            &m.label,
            &m.description,
            asset_tags(inner, false),
        ),
        json!({
            "href": format!("{}/", base_url(&cfg.advertise_host, cfg.advertise_port)),
            "api": {
                "versions": [NODE_VERSION],
                "endpoints": [{
                    "host": cfg.advertise_host,
                    "port": cfg.advertise_port,
                    "protocol": "http",
                    "authorization": false,
                }],
            },
            "caps": {},
            "services": [],
            "clocks": [{ "name": CLOCK, "ref_type": "internal" }],
            "interfaces": [],
        }),
    )
}

pub(crate) fn device(inner: &Inner, st: &State) -> Value {
    let cfg = &inner.cfg;
    let m = &inner.model;
    with(
        core(
            inner.ids.device,
            st.device_version,
            &m.label,
            &m.description,
            asset_tags(inner, true),
        ),
        json!({
            "type": "urn:x-nmos:device:generic",
            "node_id": inner.ids.node,
            "senders": inner.ids.senders.iter().map(|s| s.sender).collect::<Vec<_>>(),
            "receivers": inner.ids.receivers,
            "controls": [{
                "type": "urn:x-nmos:control:sr-ctrl/v1.2",
                "href": format!(
                    "{}/x-nmos/connection/{CONNECTION_VERSION}/",
                    base_url(&cfg.advertise_host, cfg.advertise_port)
                ),
                "authorization": false,
            }],
        }),
    )
}

fn grouphint(group: &str, role: &str) -> Value {
    json!({ GROUPHINT: [format!("{group}:{role}")] })
}

pub(crate) fn sources(inner: &Inner, st: &State) -> Vec<Value> {
    inner
        .model
        .senders
        .iter()
        .zip(&inner.ids.senders)
        .zip(&st.senders)
        .map(|((d, ids), s)| {
            let channels: Vec<Value> = d
                .channels
                .iter()
                .enumerate()
                .map(|(i, c)| match (&c.symbol, i < MAX_CHANNELS as usize) {
                    (Some(sym), _) => json!({ "label": c.label, "symbol": sym }),
                    (None, true) => json!({ "label": c.label, "symbol": format!("U{:02}", i + 1) }),
                    (None, false) => json!({ "label": c.label }),
                })
                .collect();
            with(
                core(ids.source, s.source_version, &d.label, "", json!({})),
                json!({
                    "caps": {},
                    "device_id": inner.ids.device,
                    "parents": [],
                    "clock_name": CLOCK,
                    "format": AUDIO,
                    "channels": channels,
                }),
            )
        })
        .collect()
}

pub(crate) fn flows(inner: &Inner, st: &State) -> Vec<Value> {
    inner
        .model
        .senders
        .iter()
        .zip(&inner.ids.senders)
        .zip(&st.senders)
        .map(|((d, ids), s)| {
            with(
                core(ids.flow, s.flow_version, &d.label, "", json!({})),
                json!({
                    "source_id": ids.source,
                    "device_id": inner.ids.device,
                    "parents": [],
                    "format": AUDIO,
                    "sample_rate": { "numerator": SAMPLE_RATE, "denominator": 1 },
                    "media_type": MEDIA_TYPE,
                    "bit_depth": BIT_DEPTH,
                }),
            )
        })
        .collect()
}

pub(crate) fn senders(inner: &Inner, st: &State) -> Vec<Value> {
    inner
        .model
        .senders
        .iter()
        .zip(&inner.ids.senders)
        .zip(&st.senders)
        .map(|((d, ids), s)| {
            with(
                core(
                    ids.sender,
                    s.port.version,
                    &d.label,
                    "",
                    grouphint(&d.group, "Audio out"),
                ),
                json!({
                    "flow_id": ids.flow,
                    "transport": MXL,
                    "device_id": inner.ids.device,
                    "manifest_href": null,
                    "interface_bindings": [],
                    "subscription": {
                        "receiver_id": s.port.active.peer,
                        "active": s.port.active.master_enable,
                    },
                }),
            )
        })
        .collect()
}

pub(crate) fn receivers(inner: &Inner, st: &State) -> Vec<Value> {
    inner
        .model
        .receivers
        .iter()
        .zip(&inner.ids.receivers)
        .zip(&st.receivers)
        .map(|((d, id), r)| {
            let channel_count = match d.channels {
                ChannelCount::Exactly(n) => json!({ "enum": [n] }),
                ChannelCount::AtLeast(n) => json!({ "minimum": n, "maximum": MAX_CHANNELS }),
            };
            with(
                core(
                    *id,
                    r.version,
                    &d.label,
                    "",
                    grouphint(&d.group, "Audio in"),
                ),
                json!({
                    "device_id": inner.ids.device,
                    "transport": MXL,
                    "interface_bindings": [],
                    "subscription": {
                        "sender_id": r.active.peer,
                        "active": r.active.master_enable,
                    },
                    "format": AUDIO,
                    "caps": {
                        "media_types": [MEDIA_TYPE],
                        "constraint_sets": [{
                            "urn:x-nmos:cap:meta:label": "MXL audio",
                            "urn:x-nmos:cap:meta:preference": 0,
                            "urn:x-nmos:cap:meta:enabled": true,
                            "urn:x-nmos:cap:format:media_type": { "enum": [MEDIA_TYPE] },
                            "urn:x-nmos:cap:format:sample_rate": {
                                "enum": [{ "numerator": SAMPLE_RATE, "denominator": 1 }]
                            },
                            "urn:x-nmos:cap:format:sample_depth": { "enum": [BIT_DEPTH] },
                            "urn:x-nmos:cap:format:channel_count": channel_count,
                        }],
                        "version": inner.caps_version.to_string(),
                    },
                }),
            )
        })
        .collect()
}
