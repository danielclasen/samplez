use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::{StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::tests::{Clock, DOMAIN, Fake, IN_FLOW, OUT_FLOW, call, config, get, model};
use crate::*;

const C: &str = "/x-nmos/connection/v1.2";
const OTHER: Uuid = uuid::uuid!("bbbbbbbb-0000-4000-8000-000000000003");

fn node_with(fake: Arc<Fake>, store: &dyn Store, clock: impl TaiClock) -> Nmos {
    Nmos::new(model(), config(), fake, store, clock).unwrap()
}

fn node(fake: &Arc<Fake>) -> Nmos {
    node_with(fake.clone(), &NoStore, SystemTai)
}

fn sender(n: &Nmos) -> String {
    n.inner.ids.senders[0].sender.to_string()
}

fn receiver(n: &Nmos, i: usize) -> String {
    n.inner.ids.receivers[i].to_string()
}

async fn patch(n: &Nmos, kind: &str, id: &str, body: Value) -> (StatusCode, Value) {
    let (status, _, v) = call(
        n,
        "PATCH",
        &format!("{C}/single/{kind}/{id}/staged"),
        Some(body),
    )
    .await;
    (status, v)
}

async fn active(n: &Nmos, kind: &str, id: &str) -> Value {
    get(n, &format!("{C}/single/{kind}/{id}/active")).await
}

async fn staged(n: &Nmos, kind: &str, id: &str) -> Value {
    get(n, &format!("{C}/single/{kind}/{id}/staged")).await
}

async fn is04(n: &Nmos, kind: &str, id: &str) -> Value {
    get(n, &format!("/x-nmos/node/v1.3/{kind}/{id}")).await
}

fn leg(v: &Value) -> &Value {
    &v["transport_params"][0]
}

/// Waits for the spawned apply tasks to settle.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn the_connection_api_lists_its_endpoints() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    assert_eq!(get(&n, "/x-nmos/connection/").await, json!(["v1.2/"]));
    assert_eq!(get(&n, C).await, json!(["bulk/", "single/"]));
    assert_eq!(
        get(&n, &format!("{C}/single")).await,
        json!(["senders/", "receivers/"])
    );
    assert_eq!(
        get(&n, &format!("{C}/bulk/")).await,
        json!(["senders/", "receivers/"])
    );
    assert_eq!(
        get(&n, &format!("{C}/single/senders")).await,
        json!([format!("{}/", sender(&n))])
    );
    assert_eq!(
        get(&n, &format!("{C}/single/receivers/")).await,
        json!([
            format!("{}/", receiver(&n, 0)),
            format!("{}/", receiver(&n, 1))
        ])
    );
    assert_eq!(
        get(&n, &format!("{C}/single/senders/{}", sender(&n))).await,
        json!([
            "constraints/",
            "staged/",
            "active/",
            "transportfile/",
            "transporttype/"
        ])
    );
    assert_eq!(
        get(&n, &format!("{C}/single/receivers/{}/", receiver(&n, 0))).await,
        json!(["constraints/", "staged/", "active/", "transporttype/"])
    );
    for (kind, id) in [("senders", sender(&n)), ("receivers", receiver(&n, 0))] {
        let t = get(&n, &format!("{C}/single/{kind}/{id}/transporttype")).await;
        assert_eq!(t, "urn:x-nmos:transport:mxl");
    }
    let (status, h, body) = call(
        &n,
        "GET",
        &format!("{C}/single/senders/{}/transportfile", sender(&n)),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], 404);
    assert_eq!(h[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    let (status, _, _) = call(
        &n,
        "GET",
        &format!("{C}/single/receivers/{}/transportfile", receiver(&n, 0)),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = call(
        &n,
        "GET",
        &format!("{C}/single/senders/{}/active", Uuid::nil()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn constraints_name_the_own_domain_and_never_auto() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    assert_eq!(
        get(
            &n,
            &format!("{C}/single/senders/{}/constraints", sender(&n))
        )
        .await,
        json!([{ "mxl_domain_id": { "enum": [DOMAIN] }, "mxl_flow_id": { "enum": [OUT_FLOW] } }])
    );
    assert_eq!(
        get(
            &n,
            &format!("{C}/single/receivers/{}/constraints", receiver(&n, 0))
        )
        .await,
        json!([{ "mxl_domain_id": { "enum": [DOMAIN] }, "mxl_flow_id": {} }])
    );
}

#[tokio::test]
async fn staged_and_active_start_from_the_model() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let nulls = json!({ "mode": null, "requested_time": null, "activation_time": null });
    let s = sender(&n);
    for v in [
        staged(&n, "senders", &s).await,
        active(&n, "senders", &s).await,
    ] {
        assert_eq!(
            v,
            json!({
                "receiver_id": null, "master_enable": true, "activation": nulls,
                "transport_params": [{ "mxl_domain_id": DOMAIN, "mxl_flow_id": OUT_FLOW }],
            })
        );
    }
    let connected = json!({
        "sender_id": null, "master_enable": true, "activation": nulls,
        "transport_file": { "data": null, "type": null },
        "transport_params": [{ "mxl_domain_id": DOMAIN, "mxl_flow_id": IN_FLOW }],
    });
    let r = receiver(&n, 0);
    assert_eq!(staged(&n, "receivers", &r).await, connected);
    assert_eq!(active(&n, "receivers", &r).await, connected);
    // A receiver without a flow is disabled but keeps its domain, so a
    // controller can stage just a flow.
    let r = receiver(&n, 1);
    for v in [
        staged(&n, "receivers", &r).await,
        active(&n, "receivers", &r).await,
    ] {
        assert_eq!(v["master_enable"], false);
        assert_eq!(
            leg(&v),
            &json!({ "mxl_domain_id": DOMAIN, "mxl_flow_id": null })
        );
    }
}

struct Stored(BTreeMap<PortKey, Active>);

impl Store for Stored {
    fn load(&self) -> BTreeMap<PortKey, Active> {
        self.0.clone()
    }
}

#[tokio::test]
async fn a_stored_active_is_where_staged_and_active_start() {
    let stored = Active {
        master_enable: false,
        peer: Some(OTHER),
        domain: Some(DOMAIN),
        flow: Some(OTHER),
    };
    let store = Stored(BTreeMap::from([(PortKey::Receiver("pgm".into()), stored)]));
    let n = node_with(Arc::new(Fake::default()), &store, SystemTai);
    let r = receiver(&n, 0);
    for v in [
        staged(&n, "receivers", &r).await,
        active(&n, "receivers", &r).await,
    ] {
        assert_eq!(v["master_enable"], false);
        assert_eq!(v["sender_id"], json!(OTHER));
        assert_eq!(leg(&v)["mxl_flow_id"], json!(OTHER));
    }
    assert_eq!(
        is04(&n, "receivers", &r).await["subscription"],
        json!({ "sender_id": OTHER, "active": false })
    );
    // The other receiver is not in the store.
    assert_eq!(
        active(&n, "receivers", &receiver(&n, 1)).await["master_enable"],
        false
    );
    assert_eq!(
        leg(&active(&n, "senders", &sender(&n)).await)["mxl_flow_id"],
        json!(OUT_FLOW)
    );
}

#[tokio::test]
async fn staging_refuses_what_the_schemas_and_constraints_refuse() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let (s, r) = (sender(&n), receiver(&n, 0));
    let refused = [
        ("senders", json!({ "bad": "data" })),
        ("senders", json!([])),
        ("senders", json!({ "master_enable": "yes" })),
        ("senders", json!({ "sender_id": null })),
        ("senders", json!({ "transport_params": [{}, {}] })),
        ("senders", json!({ "transport_params": [] })),
        (
            "senders",
            json!({ "transport_params": [{ "destination_port": 5004 }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": OTHER }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_domain_id": OTHER }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": OUT_FLOW.to_string().to_uppercase() }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": "00000000-0000-0000-0000-000000000000" }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": 7 }] }),
        ),
        (
            "senders",
            json!({ "transport_file": { "data": null, "type": null } }),
        ),
        (
            "senders",
            json!({ "activation": { "mode": "activate_now" } }),
        ),
        (
            "senders",
            json!({ "activation": { "mode": "activate_scheduled_relative" } }),
        ),
        (
            "senders",
            json!({ "activation": { "mode": "activate_scheduled_relative", "requested_time": "1.5" } }),
        ),
        (
            "senders",
            json!({ "activation": { "requested_time": null } }),
        ),
        (
            "senders",
            json!({ "activation": { "mode": null, "when": 1 } }),
        ),
        ("receivers", json!({ "receiver_id": null })),
        ("receivers", json!({ "sender_id": "not-a-uuid" })),
        (
            "receivers",
            json!({ "transport_params": [{ "mxl_flow_id": "auto" }] }),
        ),
        (
            "receivers",
            json!({ "transport_params": [{ "mxl_domain_id": OTHER }] }),
        ),
        (
            "receivers",
            json!({ "transport_file": { "data": "v=0", "type": "application/sdp" } }),
        ),
    ];
    for (kind, body) in refused {
        let id = if kind == "senders" { &s } else { &r };
        let (status, v) = patch(&n, kind, id, body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{kind} {body}: {v}");
        assert_eq!(v["code"], 400);
    }
    // Nothing of it was staged.
    assert_eq!(
        leg(&staged(&n, "senders", &s).await)["mxl_flow_id"],
        json!(OUT_FLOW)
    );
    let (status, _, _) = call(&n, "PATCH", &format!("{C}/single/senders/{s}/staged"), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn staging_accepts_null_auto_and_peers_and_merges_into_staged() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let (s, r) = (sender(&n), receiver(&n, 0));
    let accepted = [
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": null }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_domain_id": "auto" }] }),
        ),
        (
            "senders",
            json!({ "transport_params": [{ "mxl_flow_id": "auto" }] }),
        ),
        ("senders", json!({ "transport_params": [{}] })),
        ("senders", json!({ "receiver_id": OTHER })),
        (
            "receivers",
            json!({ "transport_params": [{ "mxl_flow_id": null, "mxl_domain_id": null }] }),
        ),
        (
            "receivers",
            json!({ "transport_params": [{ "mxl_domain_id": "auto" }] }),
        ),
        (
            "receivers",
            json!({ "transport_file": { "data": null, "type": null } }),
        ),
        ("receivers", json!({ "sender_id": OTHER })),
        ("receivers", json!({ "activation": { "mode": null } })),
        ("receivers", json!({})),
    ];
    for (kind, body) in accepted {
        let id = if kind == "senders" { &s } else { &r };
        let (status, v) = patch(&n, kind, id, body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{kind} {body}: {v}");
    }
    let v = staged(&n, "senders", &s).await;
    assert_eq!(
        leg(&v),
        &json!({ "mxl_domain_id": "auto", "mxl_flow_id": "auto" })
    );
    assert_eq!(v["receiver_id"], json!(OTHER));
    let v = staged(&n, "receivers", &r).await;
    assert_eq!(
        leg(&v),
        &json!({ "mxl_domain_id": "auto", "mxl_flow_id": null })
    );
    assert_eq!(v["sender_id"], json!(OTHER));
    // Staging alone changes nothing active.
    assert_eq!(
        leg(&active(&n, "senders", &s).await)["mxl_flow_id"],
        json!(OUT_FLOW)
    );
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_immediate_activation_writes_active_resolved_then_applies() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let s = sender(&n);
    let version = is04(&n, "senders", &s).await["version"].clone();
    let body = json!({
        "receiver_id": OTHER,
        "transport_params": [{ "mxl_domain_id": "auto", "mxl_flow_id": "auto" }],
        "activation": { "mode": "activate_immediate" },
    });
    let (status, v) = patch(&n, "senders", &s, body).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["activation"]["mode"], "activate_immediate");
    assert_eq!(v["activation"]["requested_time"], Value::Null);
    let time: Tai = v["activation"]["activation_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let a = active(&n, "senders", &s).await;
    assert_eq!(
        leg(&a),
        &json!({ "mxl_domain_id": DOMAIN, "mxl_flow_id": OUT_FLOW })
    );
    assert_eq!(a["activation"]["activation_time"], json!(time.to_string()));
    assert_eq!(a["activation"]["mode"], "activate_immediate");
    // `/staged` returns to its defaults, `auto` kept.
    let st = staged(&n, "senders", &s).await;
    assert_eq!(
        st["activation"],
        json!({ "mode": null, "requested_time": null, "activation_time": null })
    );
    assert_eq!(leg(&st)["mxl_flow_id"], "auto");
    let sender = is04(&n, "senders", &s).await;
    assert_ne!(sender["version"], version);
    assert_eq!(
        sender["subscription"],
        json!({ "receiver_id": OTHER, "active": true })
    );
    settle().await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let want = Active {
        master_enable: true,
        peer: Some(OTHER),
        domain: Some(DOMAIN),
        flow: Some(OUT_FLOW),
    };
    assert_eq!(
        (calls[0].0, &calls[0].1, calls[0].2),
        (Port::Sender(0), &want, None)
    );
    assert_eq!(n.status().ports[0].applied, Applied::Applied);
}

#[tokio::test]
async fn activations_re_apply_unchanged_settings() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    for _ in 0..2 {
        let (status, _) = patch(
            &n,
            "senders",
            &sender(&n),
            json!({ "activation": { "mode": "activate_immediate" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    settle().await;
    assert_eq!(fake.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn an_activation_that_cannot_be_applied_is_refused_before_active_changes() {
    let fake = Arc::new(Fake::default());
    fake.flows.lock().unwrap().insert(
        OTHER,
        FlowSeen::Present {
            channels: 3,
            rate: 48_000,
            float32: true,
            active: true,
        },
    );
    let n = node(&fake);
    let r = receiver(&n, 0);
    let before = active(&n, "receivers", &r).await;
    for body in [
        // Enabled without a flow.
        json!({ "transport_params": [{ "mxl_flow_id": null }], "activation": { "mode": "activate_immediate" } }),
        json!({ "transport_params": [{ "mxl_domain_id": null }], "activation": { "mode": "activate_scheduled_relative", "requested_time": "1:0" } }),
        // A present flow the caps do not take: 3 channels into 2.
        json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } }),
    ] {
        let (status, v) = patch(&n, "receivers", &r, body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {v}");
        // The refused activation leaves /staged unchanged too.
        assert_eq!(
            staged(&n, "receivers", &r).await["activation"]["mode"],
            Value::Null
        );
    }
    assert_eq!(active(&n, "receivers", &r).await, before);
    // Disabled, a null flow activates.
    let body = json!({ "master_enable": false, "transport_params": [{ "mxl_flow_id": null }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_staged_flow_is_checked_against_the_caps() {
    let fake = Arc::new(Fake::default());
    let seen = |channels, rate, float32| FlowSeen::Present {
        channels,
        rate,
        float32,
        active: false,
    };
    let flows = [
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000001"),
            seen(2, 48_000, true),
            true,
            true,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000002"),
            seen(1, 48_000, true),
            false,
            false,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000003"),
            seen(2, 44_100, true),
            false,
            false,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000004"),
            seen(2, 48_000, false),
            false,
            false,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000005"),
            seen(64, 48_000, true),
            false,
            true,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000006"),
            seen(65, 48_000, true),
            false,
            false,
        ),
        (
            uuid::uuid!("cccccccc-0000-4000-8000-000000000007"),
            FlowSeen::Absent,
            true,
            true,
        ),
    ];
    for (id, seen, ..) in flows {
        fake.flows.lock().unwrap().insert(id, seen);
    }
    let n = node(&fake);
    // Receiver 0 takes exactly 2 channels, receiver 1 from 3 to 64.
    for (id, _, exactly_2, at_least_3) in flows {
        for (i, want) in [(0, exactly_2), (1, at_least_3 && id != flows[0].0)] {
            let body = json!({ "transport_params": [{ "mxl_flow_id": id }] });
            let (status, v) = patch(&n, "receivers", &receiver(&n, i), body).await;
            assert_eq!(
                status == StatusCode::OK,
                want,
                "receiver {i} flow {id}: {v}"
            );
        }
    }
}

#[tokio::test]
async fn a_failed_apply_turns_master_enable_false_with_a_new_version() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::ZERO, Err(Fail("refused".into()))));
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    let version = is04(&n, "receivers", &r).await["version"].clone();
    settle().await;
    let a = active(&n, "receivers", &r).await;
    assert_eq!(a["master_enable"], false);
    // The transport parameters stay what was activated.
    assert_eq!(leg(&a)["mxl_flow_id"], json!(OTHER));
    let receiver = is04(&n, "receivers", &r).await;
    assert_ne!(receiver["version"], version);
    assert_eq!(receiver["subscription"]["active"], false);
    let status = &n.status().ports[1];
    assert_eq!(status.key, PortKey::Receiver("pgm".into()));
    assert_eq!(status.applied, Applied::Failed("refused".into()));
    assert_eq!(status.failures, 1);
}

#[tokio::test]
async fn a_late_failure_of_a_replaced_activation_changes_nothing() {
    let fake = Arc::new(Fake::default());
    {
        let mut results = fake.results.lock().unwrap();
        results.push_back((Duration::from_millis(50), Err(Fail("late".into()))));
        results.push_back((Duration::ZERO, Ok(())));
    }
    let n = node(&fake);
    let r = receiver(&n, 0);
    for flow in [OTHER, IN_FLOW] {
        let body = json!({ "transport_params": [{ "mxl_flow_id": flow }], "activation": { "mode": "activate_immediate" } });
        assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let a = active(&n, "receivers", &r).await;
    assert_eq!(
        (a["master_enable"].clone(), leg(&a)["mxl_flow_id"].clone()),
        (json!(true), json!(IN_FLOW))
    );
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
    assert_eq!(n.status().ports[1].failures, 0);
}

#[tokio::test]
async fn a_pending_activation_locks_staged_until_cancelled() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "60:0" },
    });
    let before = SystemTai.now();
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["activation"]["mode"], "activate_scheduled_relative");
    assert_eq!(v["activation"]["requested_time"], "60:0");
    let at: Tai = v["activation"]["activation_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        at >= before.after(Duration::from_secs(60))
            && at <= SystemTai.now().after(Duration::from_secs(60))
    );
    assert_eq!(
        staged(&n, "receivers", &r).await["activation"],
        v["activation"]
    );
    assert_eq!(n.status().ports[1].pending, Some(at));
    for body in [
        json!({ "master_enable": false }),
        json!({ "activation": { "mode": "activate_immediate" } }),
    ] {
        let (status, v) = patch(&n, "receivers", &r, body).await;
        assert_eq!(status, StatusCode::LOCKED, "{v}");
    }
    // A cancel may carry other changes.
    let (status, v) = patch(
        &n,
        "receivers",
        &r,
        json!({ "master_enable": false, "activation": { "mode": null } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["activation"],
        json!({ "mode": null, "requested_time": null, "activation_time": null })
    );
    assert_eq!(v["master_enable"], false);
    assert_eq!(n.status().ports[1].pending, None);
    assert_eq!(
        leg(&active(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(IN_FLOW)
    );
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scheduled_activation_applies_lead_early_and_writes_active_on_time() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(40),
        ..Fake::default()
    });
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:150000000" },
    });
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let at: Tai = v["activation"]["activation_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // Polls /active and notes when the flow shows.
    let shown = loop {
        let now = SystemTai.now();
        if leg(&active(&n, "receivers", &r).await)["mxl_flow_id"] == json!(OTHER) {
            break now;
        }
        assert!(now < at.after(Duration::from_secs(1)), "never activated");
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let ms = |a: Tai, b: Tai| (a.as_nanos() as i128 - b.as_nanos() as i128) as f64 / 1e6;
    assert!(
        (-2.0..10.0).contains(&ms(shown, at)),
        "/active {:.3} ms after the time",
        ms(shown, at)
    );
    let a = active(&n, "receivers", &r).await;
    assert_eq!(a["activation"]["activation_time"], json!(at.to_string()));
    assert_eq!(a["activation"]["requested_time"], "0:150000000");
    assert_eq!(
        staged(&n, "receivers", &r).await["activation"]["mode"],
        Value::Null
    );
    settle().await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].2, Some(at));
    let early = ms(at, calls[0].3);
    assert!(
        (30.0..50.0).contains(&early),
        "apply {early:.3} ms before the time"
    );
    assert_eq!(n.status().ports[1].pending, None);
}

#[tokio::test]
async fn an_absolute_time_in_the_past_fires_at_once() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let s = sender(&n);
    let body = json!({ "master_enable": false, "activation": { "mode": "activate_scheduled_absolute", "requested_time": "1:0" } });
    let (status, v) = patch(&n, "senders", &s, body).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["activation"]["activation_time"], "1:0");
    settle().await;
    let a = active(&n, "senders", &s).await;
    assert_eq!(a["master_enable"], false);
    assert_eq!(a["activation"]["activation_time"], "1:0");
    assert_eq!(fake.calls.lock().unwrap()[0].2, Some(Tai::new(1, 0)));
}

#[tokio::test]
async fn a_scheduled_activation_rechecked_at_its_time_can_fail() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:50000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    // The flow appears with a layout the receiver does not take.
    fake.flows.lock().unwrap().insert(
        OTHER,
        FlowSeen::Present {
            channels: 6,
            rate: 48_000,
            float32: true,
            active: true,
        },
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let a = active(&n, "receivers", &r).await;
    assert_eq!(
        (a["master_enable"].clone(), leg(&a)["mxl_flow_id"].clone()),
        (json!(false), json!(OTHER))
    );
    assert!(matches!(n.status().ports[1].applied, Applied::Failed(_)));
    assert!(fake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bulk_stages_each_item_and_reports_each_code() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let (r0, r1) = (receiver(&n, 0), receiver(&n, 1));
    let body = json!([
        { "id": r0, "params": { "transport_params": [{ "mxl_flow_id": OTHER }] } },
        { "id": r1, "params": { "transport_params": [{ "mxl_flow_id": "auto" }] } },
        { "id": OTHER, "params": {} },
    ]);
    let (status, _, v) = call(&n, "POST", &format!("{C}/bulk/receivers"), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v[0], json!({ "id": r0, "code": 200 }));
    assert_eq!(
        (v[1]["id"].clone(), v[1]["code"].clone()),
        (json!(r1), json!(400))
    );
    assert!(v[1]["error"].is_string());
    assert_eq!(v[2]["code"], 404);
    assert_eq!(
        leg(&staged(&n, "receivers", &r0).await)["mxl_flow_id"],
        json!(OTHER)
    );
    for body in [
        json!({}),
        json!([{ "id": r0 }]),
        json!([{ "id": r0, "params": {}, "x": 1 }]),
    ] {
        let (status, _, _) = call(
            &n,
            "POST",
            &format!("{C}/bulk/receivers"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    for kind in ["senders", "receivers"] {
        let (status, h, v) = call(&n, "GET", &format!("{C}/bulk/{kind}"), None).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(v["code"], 405);
        assert_eq!(h[header::ALLOW], "POST");
    }
}

#[tokio::test]
async fn an_external_change_shows_in_active_with_a_new_version() {
    let fake = Arc::new(Fake::default());
    let n = node_with(fake, &NoStore, Clock::at(5_000));
    let r = receiver(&n, 0);
    let version = is04(&n, "receivers", &r).await["version"].clone();
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(DOMAIN),
        flow: Some(OTHER),
    };
    n.external_change(Port::Receiver(0), switched.clone());
    assert_eq!(
        leg(&active(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(OTHER)
    );
    assert_eq!(
        leg(&staged(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(IN_FLOW)
    );
    assert_ne!(is04(&n, "receivers", &r).await["version"], version);
    assert_eq!(n.status().ports[1].active, switched);
}

#[tokio::test]
async fn a_slow_check_holds_other_patches_back() {
    struct Slow;
    impl Connections for Slow {
        fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(FlowSeen::Absent)
            })
        }
        fn lead(&self) -> Duration {
            Duration::ZERO
        }
        fn apply(&self, _: Port, _: Active, _: Option<Tai>) -> BoxFuture<'_, Result<(), Fail>> {
            Box::pin(async { Ok(()) })
        }
    }
    let n = Nmos::new(model(), config(), Arc::new(Slow), &NoStore, SystemTai).unwrap();
    let r = receiver(&n, 0);
    let start = Instant::now();
    let slow = patch(
        &n,
        "receivers",
        &r,
        json!({ "transport_params": [{ "mxl_flow_id": OTHER }] }),
    );
    let quick = patch(&n, "receivers", &r, json!({ "master_enable": false }));
    let ((a, _), (b, _)) = tokio::join!(slow, quick);
    assert_eq!((a, b), (StatusCode::OK, StatusCode::OK));
    assert!(start.elapsed() >= Duration::from_millis(50));
    let v = staged(&n, "receivers", &r).await;
    // Both changes landed; neither overwrote the other with a stale copy.
    assert_eq!(
        (v["master_enable"].clone(), leg(&v)["mxl_flow_id"].clone()),
        (json!(false), json!(OTHER))
    );
}
