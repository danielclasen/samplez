use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use axum::http::{StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::tests::{Clock, DOMAIN, Fake, IN_FLOW, OUT_FLOW, Paused, call, config, get, model};
use crate::*;

const C: &str = "/x-nmos/connection/v1.2";
const OTHER: Uuid = uuid::uuid!("bbbbbbbb-0000-4000-8000-000000000003");
const THIRD: Uuid = uuid::uuid!("bbbbbbbb-0000-4000-8000-000000000004");

fn node_with(fake: Arc<Fake>, store: &dyn Store, clock: impl TaiClock) -> Nmos {
    Nmos::new(model(), config(), fake, store, clock).unwrap()
}

fn node(fake: &Arc<Fake>) -> Nmos {
    node_with(fake.clone(), &NoStore, fake.clock.clone())
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

/// Lets the spawned apply tasks run; time is paused, so nothing waits.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn a_stored_active_is_where_staged_and_active_start() {
    let stored = Active {
        master_enable: false,
        peer: Some(OTHER),
        domain: Some(DOMAIN),
        flow: Some(OTHER),
    };
    let store = Stored(BTreeMap::from([(PortKey::Receiver("pgm".into()), stored)]));
    let n = node_with(Arc::new(Fake::default()), &store, Paused::default());
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn an_activation_that_cannot_be_applied_is_refused_before_active_changes() {
    let fake = Arc::new(Fake::default());
    fake.flows.lock().unwrap().insert(
        OTHER,
        FlowSeen::Present {
            channels: 3,
            rate: 48_000,
            float32: true,
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

#[tokio::test(start_paused = true)]
async fn a_staged_flow_is_checked_against_the_caps() {
    let fake = Arc::new(Fake::default());
    let seen = |channels, rate, float32| FlowSeen::Present {
        channels,
        rate,
        float32,
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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
    // The product got the second activation only once the first resolved,
    // and ends on it.
    let done: Vec<_> = fake
        .done
        .lock()
        .unwrap()
        .iter()
        .map(|(a, ok)| (a.flow, *ok))
        .collect();
    assert_eq!(done, [(Some(OTHER), false), (Some(IN_FLOW), true)]);
    assert_eq!(fake.ends_on().and_then(|a| a.flow), Some(IN_FLOW));
}

#[tokio::test(start_paused = true)]
async fn applies_reach_the_product_in_activation_order() {
    let fake = Arc::new(Fake::default());
    {
        let mut results = fake.results.lock().unwrap();
        results.push_back((Duration::from_millis(50), Ok(())));
        results.push_back((Duration::from_millis(10), Ok(())));
        results.push_back((Duration::ZERO, Ok(())));
    }
    let n = node(&fake);
    let r = receiver(&n, 0);
    for flow in [OTHER, IN_FLOW, OTHER] {
        let body = json!({ "transport_params": [{ "mxl_flow_id": flow }], "activation": { "mode": "activate_immediate" } });
        assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    }
    // Only the first is with the product while it takes its time.
    settle().await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls = fake.calls.lock().unwrap().clone();
    let flows: Vec<_> = calls.iter().map(|c| c.1.flow).collect();
    assert_eq!(flows, [Some(OTHER), Some(IN_FLOW), Some(OTHER)]);
    assert!(calls.windows(2).all(|w| w[0].4 < w[1].4), "{calls:?}");
    let done: Vec<_> = fake
        .done
        .lock()
        .unwrap()
        .iter()
        .map(|(a, _)| a.flow)
        .collect();
    assert_eq!(done, flows);
    assert_eq!(
        Some(leg(&active(&n, "receivers", &r).await)["mxl_flow_id"].clone()),
        fake.ends_on().map(|a| json!(a.flow))
    );
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
}

#[tokio::test(start_paused = true)]
async fn a_panicking_apply_is_a_failed_one() {
    struct Panics;
    impl Connections for Panics {
        fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
            Box::pin(async { Ok(FlowSeen::Absent) })
        }
        fn lead(&self) -> Duration {
            Duration::ZERO
        }
        fn apply(
            &self,
            _: Port,
            _: Active,
            _: Option<Tai>,
            _: u64,
        ) -> BoxFuture<'_, Result<(), Fail>> {
            Box::pin(async { panic!("the product broke") })
        }
    }
    let n = Nmos::new(
        model(),
        config(),
        Arc::new(Panics),
        &NoStore,
        Paused::default(),
    )
    .unwrap();
    let r = receiver(&n, 0);
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(
        patch(&n, "receivers", &r, body.clone()).await.0,
        StatusCode::OK
    );
    settle().await;
    assert_eq!(active(&n, "receivers", &r).await["master_enable"], false);
    let status = &n.status().ports[1];
    assert!(
        matches!(&status.applied, Applied::Failed(m) if m.contains("panic")),
        "{:?}",
        status.applied
    );
    assert_eq!(status.failures, 1);
    // The port's queue still works after the panic.
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    assert_eq!(n.status().ports[1].failures, 2);
}

#[tokio::test(start_paused = true)]
async fn a_pending_activation_locks_staged_until_cancelled() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "60:0" },
    });
    let before = fake.clock.now();
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["activation"]["mode"], "activate_scheduled_relative");
    assert_eq!(v["activation"]["requested_time"], "60:0");
    let at: Tai = v["activation"]["activation_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(at, before.after(Duration::from_mins(1)));
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

#[tokio::test(start_paused = true)]
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
    assert_eq!(at, fake.clock.now().after(Duration::from_millis(150)));
    let shows = |a: &Value| leg(a)["mxl_flow_id"] == json!(OTHER);
    // Just before the lead: nothing yet.
    tokio::time::sleep(Duration::from_millis(109)).await;
    assert!(fake.calls.lock().unwrap().is_empty());
    // At the lead the product has it, /active not yet.
    tokio::time::sleep(Duration::from_millis(2)).await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].2, Some(at));
    assert_eq!(at.since(calls[0].3), Duration::from_millis(40));
    assert!(!shows(&active(&n, "receivers", &r).await));
    // At the time /active shows it.
    tokio::time::sleep(Duration::from_millis(38)).await;
    assert!(!shows(&active(&n, "receivers", &r).await));
    tokio::time::sleep(Duration::from_millis(2)).await;
    let a = active(&n, "receivers", &r).await;
    assert!(shows(&a));
    assert_eq!(a["activation"]["activation_time"], json!(at.to_string()));
    assert_eq!(a["activation"]["requested_time"], "0:150000000");
    assert_eq!(
        staged(&n, "receivers", &r).await["activation"]["mode"],
        Value::Null
    );
    settle().await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    assert_eq!(n.status().ports[1].pending, None);
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
}

#[tokio::test(start_paused = true)]
async fn a_read_at_the_activation_time_sees_the_activation() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let s = sender(&n);
    let body = json!({
        "master_enable": false,
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:200000000" },
    });
    assert_eq!(patch(&n, "senders", &s, body).await.0, StatusCode::ACCEPTED);
    // The activation time has come on the TAI clock, but the task's timer
    // has not fired yet.
    tokio::time::sleep(Duration::from_micros(199_500)).await;
    fake.clock.skip(Duration::from_micros(500));
    let a = active(&n, "senders", &s).await;
    assert_eq!(a["master_enable"], false, "{a}");
    assert_eq!(a["activation"]["mode"], "activate_scheduled_relative");
    assert_eq!(
        staged(&n, "senders", &s).await["activation"]["mode"],
        Value::Null
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancel_after_the_lead_hands_the_product_the_active_connection_again() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(40),
        ..Fake::default()
    });
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:100000000" },
    });
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let at: Tai = v["activation"]["activation_time"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // Inside the lead: the product has the activation already.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    let (status, _) = patch(
        &n,
        "receivers",
        &r,
        json!({ "activation": { "mode": null } }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // It got the still-active connection for the same time after it, and
    // ends where /active is.
    let calls = fake.calls.lock().unwrap().clone();
    let got: Vec<_> = calls.iter().map(|c| (c.1.flow, c.2)).collect();
    assert_eq!(got, [(Some(OTHER), Some(at)), (Some(IN_FLOW), Some(at))]);
    assert!(calls[0].4 < calls[1].4);
    let a = active(&n, "receivers", &r).await;
    assert_eq!(leg(&a)["mxl_flow_id"], json!(IN_FLOW));
    assert_eq!(a["master_enable"], true);
    assert_eq!(fake.ends_on().and_then(|a| a.flow), Some(IN_FLOW));
    assert_eq!(n.status().ports[1].pending, None);
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
}

#[tokio::test(start_paused = true)]
async fn a_relative_time_past_the_clock_range_is_refused() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "activation": { "mode": "activate_scheduled_relative", "requested_time": format!("{}:0", u64::MAX) },
    });
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["code"], 400);
    assert_eq!(n.status().ports[1].pending, None);
}

#[tokio::test(start_paused = true)]
async fn an_immediate_activation_time_is_read_after_the_check() {
    struct Slow;
    impl Connections for Slow {
        fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(FlowSeen::Absent)
            })
        }
        fn lead(&self) -> Duration {
            Duration::ZERO
        }
        fn apply(
            &self,
            _: Port,
            _: Active,
            _: Option<Tai>,
            _: u64,
        ) -> BoxFuture<'_, Result<(), Fail>> {
            Box::pin(async { Ok(()) })
        }
    }
    let clock = Paused::default();
    let n = Nmos::new(model(), config(), Arc::new(Slow), &NoStore, clock.clone()).unwrap();
    let r = receiver(&n, 0);
    let start = clock.now();
    // Staged first, so the activation's own check is the slow one.
    let body = json!({ "activation": { "mode": "activate_immediate" } });
    let (status, v) = patch(&n, "receivers", &r, body).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["activation"]["activation_time"],
        json!(start.after(Duration::from_secs(1)).to_string())
    );
}

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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

/// With shared versions an activation the node serves changes `/active`
/// but not the version; the product's report of it moves the version and
/// keeps the activation queued behind it on its way to the product, and
/// `/active` follows the replicated changes in their order.
#[tokio::test(start_paused = true)]
async fn a_served_activation_takes_its_version_from_the_replicated_change() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(50), Ok(())));
    let versions = BTreeMap::from([(PortKey::Receiver("pgm".into()), Tai::new(7, 0))]);
    let n = Nmos::with_versions(
        model(),
        config(),
        fake.clone(),
        &NoStore,
        fake.clock.clone(),
        &versions,
    )
    .unwrap();
    let r = receiver(&n, 0);
    for flow in [OTHER, THIRD] {
        let body = json!({ "transport_params": [{ "mxl_flow_id": flow }], "activation": { "mode": "activate_immediate" } });
        assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    }
    assert_eq!(is04(&n, "receivers", &r).await["version"], "7:0");
    settle().await;
    let generation = {
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        calls[0].4
    };
    let change = n.activation(Port::Receiver(0), generation).unwrap();
    assert_eq!(change.active.flow, Some(OTHER));
    n.replicated_change(Port::Receiver(0), &change, Tai::new(8, 0), Some(generation));
    assert_eq!(is04(&n, "receivers", &r).await["version"], "8:0");
    assert_eq!(
        leg(&active(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(OTHER)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(calls[1].1.flow, Some(THIRD));
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
    let change = n.activation(Port::Receiver(0), calls[1].4).unwrap();
    n.replicated_change(Port::Receiver(0), &change, Tai::new(8, 0), Some(calls[1].4));
    assert_eq!(is04(&n, "receivers", &r).await["version"], "8:1");
    assert_eq!(
        leg(&active(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(THIRD)
    );
}

/// With shared versions a failed apply shows the replicated connection in
/// `/active` again, and the resource and its version stay: no instance
/// shows a connection the product did not take on.
#[tokio::test(start_paused = true)]
async fn a_failed_apply_with_shared_versions_shows_the_replicated_connection() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::ZERO, Err(Fail("refused".into()))));
    let n = Nmos::with_versions(
        model(),
        config(),
        fake.clone(),
        &NoStore,
        fake.clock.clone(),
        &BTreeMap::new(),
    )
    .unwrap();
    let r = receiver(&n, 0);
    let before = (
        active(&n, "receivers", &r).await,
        is04(&n, "receivers", &r).await,
    );
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    let after = (
        active(&n, "receivers", &r).await,
        is04(&n, "receivers", &r).await,
    );
    assert_eq!(after, before);
    let status = &n.status().ports[1];
    assert_eq!(status.applied, Applied::Failed("refused".into()));
    assert_eq!(status.failures, 1);
}

fn shared_node(fake: &Arc<Fake>) -> Nmos {
    let versions = BTreeMap::new();
    Nmos::with_versions(
        model(),
        config(),
        fake.clone(),
        &NoStore,
        fake.clock.clone(),
        &versions,
    )
    .unwrap()
}

/// With shared versions a scheduled activation that no longer checks out
/// at its time goes to the product disabled, so the failure is
/// replicated, and `/active` never shows the request enabled.
#[tokio::test(start_paused = true)]
async fn a_scheduled_activation_failing_its_check_reaches_the_product_disabled() {
    let fake = Arc::new(Fake::default());
    let n = shared_node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:50000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    fake.flows.lock().unwrap().insert(
        OTHER,
        FlowSeen::Present {
            channels: 6,
            rate: 48_000,
            float32: true,
        },
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "{calls:?}");
    let (port, asked, at, _, generation) = calls[0].clone();
    assert_eq!(port, Port::Receiver(0));
    assert!(!asked.master_enable);
    assert_eq!(asked.flow, Some(OTHER));
    assert!(at.is_some());
    let a = active(&n, "receivers", &r).await;
    assert!(
        a["master_enable"] == false || leg(&a)["mxl_flow_id"] != json!(OTHER),
        "{a}"
    );
    assert!(matches!(n.status().ports[1].applied, Applied::Failed(_)));
    // The product replicates what it was handed.
    let change = n.activation(Port::Receiver(0), generation).unwrap();
    n.replicated_change(Port::Receiver(0), &change, Tai::new(9, 0), Some(generation));
    let a = active(&n, "receivers", &r).await;
    assert_eq!(
        (a["master_enable"].clone(), leg(&a)["mxl_flow_id"].clone()),
        (json!(false), json!(OTHER))
    );
}

/// With shared versions the instance that served an activation writes
/// `/active` from the replicated change while nothing came after the
/// apply: a scheduled activation, applied `lead` early, shows on it as on
/// its peers once replicated, before its time.
#[tokio::test(start_paused = true)]
async fn a_served_scheduled_activation_shows_once_replicated() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(100),
        ..Fake::default()
    });
    let n = shared_node(&fake);
    let peer = shared_node(&Arc::new(Fake::default()));
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:500000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    let generation = fake.calls.lock().unwrap()[0].4;
    let change = n.activation(Port::Receiver(0), generation).unwrap();
    let at = Tai::new(9, 0);
    n.replicated_change(Port::Receiver(0), &change, at, Some(generation));
    peer.replicated_change(Port::Receiver(0), &change, at, None);
    let view = |n: &Nmos| {
        let p = n.inner.lock();
        let p = p.port(Port::Receiver(0));
        (p.active.clone(), p.active_activation.clone())
    };
    assert_eq!(view(&n), view(&peer));
    assert_eq!(view(&n).0.flow, Some(OTHER));
}

/// With shared versions a cancel after the scheduled activation went to
/// the product, and was replicated, hands the product the connection
/// active before it, not the cancelled one `/active` shows.
#[tokio::test(start_paused = true)]
async fn a_shared_cancel_in_the_lead_window_hands_back_the_connection_before() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(100),
        ..Fake::default()
    });
    let n = shared_node(&fake);
    let r = receiver(&n, 0);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:500000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    tokio::time::sleep(Duration::from_millis(450)).await;
    let generation = fake.calls.lock().unwrap()[0].4;
    let change = n.activation(Port::Receiver(0), generation).unwrap();
    n.replicated_change(Port::Receiver(0), &change, Tai::new(9, 0), Some(generation));
    assert_eq!(
        leg(&active(&n, "receivers", &r).await)["mxl_flow_id"],
        json!(OTHER)
    );
    let cancel = json!({ "activation": { "mode": null } });
    assert_eq!(patch(&n, "receivers", &r, cancel).await.0, StatusCode::OK);
    settle().await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(calls[1].1.flow, Some(IN_FLOW));
    assert_eq!(calls[1].2, calls[0].2);
}

/// Changes replicated in log order decide `/active`: a change made
/// elsewhere that reaches the serving instance between its activation and
/// that activation's replicated change does not leave `/active` on it.
#[tokio::test(start_paused = true)]
async fn an_interleaved_change_leaves_every_instance_on_the_last_replicated() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(50), Ok(())));
    let n = shared_node(&fake);
    let peer = shared_node(&Arc::new(Fake::default()));
    let r = receiver(&n, 0);
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    let gb = fake.calls.lock().unwrap()[0].4;
    let b = n.activation(Port::Receiver(0), gb).unwrap();
    let a = Replicated {
        active: Active {
            master_enable: true,
            peer: None,
            domain: Some(DOMAIN),
            flow: Some(THIRD),
        },
        activation: None,
    };
    for x in [&n, &peer] {
        x.replicated_change(Port::Receiver(0), &a, Tai::new(8, 0), None);
    }
    n.replicated_change(Port::Receiver(0), &b, Tai::new(9, 0), Some(gb));
    peer.replicated_change(Port::Receiver(0), &b, Tai::new(9, 0), None);
    for x in [&n, &peer] {
        let a = active(x, "receivers", &r).await;
        assert_eq!(leg(&a)["mxl_flow_id"], json!(OTHER), "{a}");
    }
    assert_eq!(
        active(&n, "receivers", &r).await,
        active(&peer, "receivers", &r).await
    );
    assert_eq!(
        is04(&n, "receivers", &r).await,
        is04(&peer, "receivers", &r).await
    );
}

/// A replicated change without an activation shows the one last
/// replicated, not one the instance wrote itself.
#[tokio::test(start_paused = true)]
async fn a_change_without_an_activation_shows_the_replicated_one() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(50), Ok(())));
    let n = shared_node(&fake);
    let r = receiver(&n, 0);
    let before = active(&n, "receivers", &r).await["activation"].clone();
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    assert_ne!(active(&n, "receivers", &r).await["activation"], before);
    let change = Replicated {
        active: n.status().ports[1].active.clone(),
        activation: None,
    };
    n.replicated_change(Port::Receiver(0), &change, Tai::new(9, 0), None);
    assert_eq!(active(&n, "receivers", &r).await["activation"], before);
}

#[tokio::test(start_paused = true)]
async fn an_external_change_drops_the_activations_queued_before_it() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(50), Ok(())));
    let n = node(&fake);
    let r = receiver(&n, 0);
    for flow in [OTHER, IN_FLOW] {
        let body = json!({ "transport_params": [{ "mxl_flow_id": flow }], "activation": { "mode": "activate_immediate" } });
        assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    }
    settle().await;
    // The first is with the product, the second still queued.
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(DOMAIN),
        flow: Some(THIRD),
    };
    let generation = n.external_change(Port::Receiver(0), switched.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The queued one never reached the product; the one in flight carries
    // a lower generation, so the product knows it is stale.
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert!(calls[0].4 < generation);
    assert_eq!(n.status().ports[1].active, switched);
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
    // The next activation carries a higher generation still.
    let body = json!({ "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    assert!(fake.calls.lock().unwrap()[1].4 > generation);
}

#[tokio::test(start_paused = true)]
async fn a_scheduled_activation_kept_past_an_external_change_is_newer_than_it() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(40),
        ..Fake::default()
    });
    // An earlier activation keeps the product busy past the lead.
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(100), Ok(())));
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    let body = json!({
        "transport_params": [{ "mxl_flow_id": IN_FLOW }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:60000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    // Past the lead the scheduled one waits in the queue; then the product
    // switches by itself.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(DOMAIN),
        flow: Some(THIRD),
    };
    let change = n.external_change(Port::Receiver(0), switched);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert!(calls[0].4 < change);
    assert_eq!(calls[1].1.flow, Some(IN_FLOW));
    assert!(calls[1].4 > change, "{} <= {change}", calls[1].4);
    let a = active(&n, "receivers", &r).await;
    assert_eq!(leg(&a)["mxl_flow_id"], json!(IN_FLOW));
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
}

#[tokio::test(start_paused = true)]
async fn an_apply_that_does_not_resolve_fails_and_the_queue_moves_on() {
    let fake = Arc::new(Fake {
        apply_timeout: Some(Duration::from_secs(3)),
        ..Fake::default()
    });
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_hours(1), Ok(())));
    let n = node(&fake);
    let r = receiver(&n, 0);
    let version = is04(&n, "receivers", &r).await["version"].clone();
    for flow in [OTHER, IN_FLOW] {
        let body = json!({ "transport_params": [{ "mxl_flow_id": flow }], "activation": { "mode": "activate_immediate" } });
        assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    }
    tokio::time::sleep(Duration::from_millis(2_999)).await;
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(2)).await;
    // The stuck one timed out and the next went to the product; it
    // resolves at once and is the newest, so the port ends applied.
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].1.flow, Some(IN_FLOW));
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
    // A stuck newest activation fails the port.
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_hours(1), Ok(())));
    let body = json!({ "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    let before = is04(&n, "receivers", &r).await["version"].clone();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let a = active(&n, "receivers", &r).await;
    assert_eq!(a["master_enable"], false);
    assert_ne!(is04(&n, "receivers", &r).await["version"], before);
    assert_ne!(before, version);
    assert!(
        matches!(&n.status().ports[1].applied, Applied::Failed(m) if m.contains("did not resolve")),
        "{:?}",
        n.status().ports[1].applied
    );
    assert_eq!(n.status().ports[1].failures, 1);
    assert_eq!(DEFAULT_APPLY_TIMEOUT, Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn a_cancel_before_the_product_got_the_activation_hands_it_nothing() {
    let fake = Arc::new(Fake {
        lead: Duration::from_millis(40),
        ..Fake::default()
    });
    // An earlier activation keeps the product busy past the lead.
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(100), Ok(())));
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({ "master_enable": false, "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    let body = json!({
        "master_enable": true,
        "transport_params": [{ "mxl_flow_id": OTHER }],
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:60000000" },
    });
    assert_eq!(
        patch(&n, "receivers", &r, body).await.0,
        StatusCode::ACCEPTED
    );
    // Past the lead: the scheduled one is queued behind the busy one.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancel = json!({ "activation": { "mode": null } });
    assert_eq!(patch(&n, "receivers", &r, cancel).await.0, StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let calls = fake.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(fake.ends_on().map(|a| a.master_enable), Some(false));
    assert_eq!(active(&n, "receivers", &r).await["master_enable"], false);
}

#[tokio::test(start_paused = true)]
async fn how_late_a_scheduled_activation_fired_is_recorded() {
    let fake = Arc::new(Fake::default());
    let n = node(&fake);
    let metrics = samplez_service::Metrics::default();
    n.register_metrics(&metrics);
    let s = sender(&n);
    assert_eq!(n.status().ports[0].late, None);
    let body = json!({
        "master_enable": false,
        "activation": { "mode": "activate_scheduled_relative", "requested_time": "0:200000000" },
    });
    assert_eq!(patch(&n, "senders", &s, body).await.0, StatusCode::ACCEPTED);
    // Once the task sleeps, the TAI clock steps 3 ms ahead of the timer
    // that wakes it.
    settle().await;
    fake.clock.skip(Duration::from_millis(3));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(n.status().ports[0].late, Some(Duration::from_millis(3)));
    let text = metrics.render();
    assert!(
        text.contains(
            "samplez_nmos_activation_late_seconds_bucket{port=\"sender/pgm\",le=\"0.005\"} 1"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "samplez_nmos_activation_late_seconds_bucket{port=\"sender/pgm\",le=\"0.002\"} 0"
        ),
        "{text}"
    );
    assert!(text.contains("samplez_nmos_activation_late_seconds_count{port=\"receiver/spare\"} 0"));
}

#[tokio::test(start_paused = true)]
async fn an_external_change_outlives_the_failure_of_an_apply_in_flight() {
    let fake = Arc::new(Fake::default());
    fake.results
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(50), Err(Fail("late".into()))));
    let n = node(&fake);
    let r = receiver(&n, 0);
    let body = json!({ "transport_params": [{ "mxl_flow_id": OTHER }], "activation": { "mode": "activate_immediate" } });
    assert_eq!(patch(&n, "receivers", &r, body).await.0, StatusCode::OK);
    settle().await;
    let switched = Active {
        master_enable: true,
        peer: None,
        domain: Some(DOMAIN),
        flow: Some(IN_FLOW),
    };
    n.external_change(Port::Receiver(0), switched.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let a = active(&n, "receivers", &r).await;
    assert_eq!(
        (a["master_enable"].clone(), leg(&a)["mxl_flow_id"].clone()),
        (json!(true), json!(IN_FLOW))
    );
    assert_eq!(n.status().ports[1].active, switched);
    assert_eq!(n.status().ports[1].applied, Applied::Applied);
    assert_eq!(n.status().ports[1].failures, 0);
}

#[tokio::test(start_paused = true)]
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
        fn apply(
            &self,
            _: Port,
            _: Active,
            _: Option<Tai>,
            _: u64,
        ) -> BoxFuture<'_, Result<(), Fail>> {
            Box::pin(async { Ok(()) })
        }
    }
    let n = Nmos::new(
        model(),
        config(),
        Arc::new(Slow),
        &NoStore,
        Paused::default(),
    )
    .unwrap();
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
    assert_eq!(start.elapsed(), Duration::from_millis(50));
    let v = staged(&n, "receivers", &r).await;
    // Both changes landed; neither overwrote the other with a stale copy.
    assert_eq!(
        (v["master_enable"].clone(), leg(&v)["mxl_flow_id"].clone()),
        (json!(false), json!(OTHER))
    );
}

#[tokio::test(start_paused = true)]
async fn a_stuck_check_times_out_and_holds_back_its_port_only() {
    struct Stuck;
    impl Connections for Stuck {
        fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
            Box::pin(std::future::pending())
        }
        fn lead(&self) -> Duration {
            Duration::ZERO
        }
        fn apply(
            &self,
            _: Port,
            _: Active,
            _: Option<Tai>,
            _: u64,
        ) -> BoxFuture<'_, Result<(), Fail>> {
            Box::pin(async { Ok(()) })
        }
    }
    let n = Nmos::new(
        model(),
        config(),
        Arc::new(Stuck),
        &NoStore,
        Paused::default(),
    )
    .unwrap();
    let (r0, r1) = (receiver(&n, 0), receiver(&n, 1));
    let start = Instant::now();
    let stuck = patch(
        &n,
        "receivers",
        &r0,
        json!({ "transport_params": [{ "mxl_flow_id": OTHER }] }),
    );
    let other = async {
        let r = patch(&n, "receivers", &r1, json!({ "master_enable": false })).await;
        (r, start.elapsed())
    };
    let both = tokio::time::timeout(Duration::from_mins(1), async { tokio::join!(stuck, other) });
    let ((status, v), ((other, _), quick)) = both.await.expect("a PATCH never answered");
    assert_eq!(other, StatusCode::OK);
    assert_eq!(quick, Duration::ZERO);
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    assert_eq!(v["code"], 500);
    assert!(
        v["error"].as_str().unwrap().contains("did not resolve"),
        "{v}"
    );
    assert_eq!(start.elapsed(), RESOLVE_TIMEOUT);
    assert_eq!(
        leg(&staged(&n, "receivers", &r0).await)["mxl_flow_id"],
        json!(IN_FLOW)
    );
}

#[tokio::test(start_paused = true)]
async fn a_stored_active_that_does_not_fit_the_model_is_left_behind() {
    let foreign = uuid::uuid!("cccccccc-0000-4000-8000-000000000004");
    let store = Stored(BTreeMap::from([
        (
            PortKey::Sender("pgm".into()),
            Active {
                master_enable: true,
                peer: None,
                domain: Some(foreign),
                flow: Some(OUT_FLOW),
            },
        ),
        (
            PortKey::Receiver("pgm".into()),
            Active {
                master_enable: true,
                peer: None,
                domain: Some(DOMAIN),
                flow: None,
            },
        ),
        (
            PortKey::Receiver("spare".into()),
            Active {
                master_enable: false,
                peer: None,
                domain: Some(DOMAIN),
                flow: Some(Uuid::nil()),
            },
        ),
    ]));
    let n = node_with(Arc::new(Fake::default()), &store, Paused::default());
    let s = sender(&n);
    assert_eq!(
        leg(&staged(&n, "senders", &s).await),
        &json!({ "mxl_domain_id": DOMAIN, "mxl_flow_id": OUT_FLOW })
    );
    let r = active(&n, "receivers", &receiver(&n, 0)).await;
    assert_eq!(
        (r["master_enable"].clone(), leg(&r)["mxl_flow_id"].clone()),
        (json!(true), json!(IN_FLOW))
    );
    let r = active(&n, "receivers", &receiver(&n, 1)).await;
    assert_eq!(leg(&r)["mxl_flow_id"], Value::Null);
    // One that fits is taken as stored.
    let fits = Stored(BTreeMap::from([(
        PortKey::Sender("pgm".into()),
        Active {
            master_enable: false,
            peer: None,
            domain: None,
            flow: None,
        },
    )]));
    let n = node_with(Arc::new(Fake::default()), &fits, Paused::default());
    let s = sender(&n);
    assert_eq!(active(&n, "senders", &s).await["master_enable"], false);
}
