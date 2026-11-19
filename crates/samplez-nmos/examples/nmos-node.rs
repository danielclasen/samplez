//! An NMOS node over `samplez-nmos` with an in-memory product: two MXL
//! senders and two MXL receivers in one domain, every flow absent and
//! every activation applied. CI runs the AMWA nmos-testing suites against
//! it.
//!
//! ```sh
//! nmos-node --advertise-host 198.51.100.3 [--bind 0.0.0.0:8081] [--seed example]
//!           [--domain <uuid>] [--registry http://host:port]... [--dns-sd-domain example.com]
//! ```

use std::net::SocketAddr;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;

use samplez_nmos::uuid::{Uuid, uuid};
use samplez_nmos::*;
use tokio::signal::unix::{SignalKind, signal};

struct InMemory;

impl Connections for InMemory {
    fn resolve(&self, _: Port, _: Uuid) -> BoxFuture<'_, Result<FlowSeen, Reject>> {
        Box::pin(async { Ok(FlowSeen::Absent) })
    }

    fn lead(&self) -> Duration {
        Duration::ZERO
    }

    fn apply(
        &self,
        port: Port,
        active: Active,
        at: Option<Tai>,
        generation: u64,
    ) -> BoxFuture<'_, Result<(), Fail>> {
        tracing::info!(?port, ?active, ?at, generation, "apply");
        Box::pin(async { Ok(()) })
    }
}

struct Args {
    bind: SocketAddr,
    advertise_host: String,
    seed: String,
    domain: Uuid,
    registries: Vec<String>,
    dns_sd_domain: Option<String>,
}

fn args() -> Args {
    let mut a = Args {
        bind: "0.0.0.0:8081".parse().unwrap(),
        advertise_host: String::new(),
        seed: "example".into(),
        domain: uuid!("6f1f1d1e-56f6-4c4c-8d32-9e7f4a3b2c10"),
        registries: Vec::new(),
        dns_sd_domain: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let Some(value) = it.next() else {
            usage(&format!("{flag} needs a value"));
        };
        match flag.as_str() {
            "--bind" => {
                a.bind = value
                    .parse()
                    .unwrap_or_else(|e| usage(&format!("--bind: {e}")))
            }
            "--advertise-host" => a.advertise_host = value,
            "--seed" => a.seed = value,
            "--domain" => {
                a.domain = value
                    .parse()
                    .unwrap_or_else(|e| usage(&format!("--domain: {e}")))
            }
            "--registry" => a.registries.push(value),
            "--dns-sd-domain" => a.dns_sd_domain = Some(value),
            _ => usage(&format!("unknown flag {flag}")),
        }
    }
    if a.advertise_host.is_empty() {
        usage("--advertise-host is required");
    }
    a
}

fn usage(why: &str) -> ! {
    eprintln!("nmos-node: {why}");
    exit(2)
}

fn model(domain: Uuid) -> NodeModel {
    let channel = |label: &str, symbol: Option<&str>| ChannelDesc {
        label: label.into(),
        symbol: symbol.map(Into::into),
    };
    NodeModel {
        label: "samplez-nmos example".into(),
        description: "An in-memory node".into(),
        senders: vec![
            SenderDesc {
                key: "main".into(),
                label: "Main".into(),
                channels: vec![channel("Left", Some("L")), channel("Right", Some("R"))],
                group: "main".into(),
                domain,
                flow: uuid!("0d7f6b8e-4a2c-4e8f-9b1a-2c3d4e5f6a01"),
            },
            SenderDesc {
                key: "aux".into(),
                label: "Aux".into(),
                channels: vec![channel("Aux", None)],
                group: "aux".into(),
                domain,
                flow: uuid!("0d7f6b8e-4a2c-4e8f-9b1a-2c3d4e5f6a02"),
            },
        ],
        receivers: vec![
            ReceiverDesc {
                key: "main".into(),
                label: "Main in".into(),
                channels: ChannelCount::Exactly(2),
                group: "main".into(),
                domain,
                flow: Some(uuid!("0d7f6b8e-4a2c-4e8f-9b1a-2c3d4e5f6a03")),
            },
            ReceiverDesc {
                key: "spare".into(),
                label: "Spare in".into(),
                channels: ChannelCount::AtLeast(1),
                group: "spare".into(),
                domain,
                flow: None,
            },
        ],
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let a = args();
    let cfg = NmosConfig {
        seed: a.seed.clone(),
        advertise_host: a.advertise_host,
        advertise_port: a.bind.port(),
        asset: Asset {
            manufacturer: "danielclasen".into(),
            product: "samplez-nmos example".into(),
            instance_id: a.seed,
            functions: vec!["Mixer".into()],
        },
        heartbeat: Duration::from_secs(5),
        registries: a.registries,
        dns_sd_domain: a.dns_sd_domain,
        continue_held: false,
    };
    let nmos = Nmos::new(
        model(a.domain),
        cfg,
        Arc::new(InMemory),
        &NoStore,
        SystemTai,
    )
    .unwrap_or_else(|e| usage(&e.to_string()));
    let listener = tokio::net::TcpListener::bind(a.bind)
        .await
        .unwrap_or_else(|e| usage(&format!("binding {}: {e}", a.bind)));
    tracing::info!(node = %nmos.node_id(), bind = %a.bind, "serving");
    let registration = tokio::spawn(nmos.registration());
    let serve = tokio::spawn(axum::serve(listener, nmos.router()).into_future());
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let failed = tokio::select! {
        _ = term.recv() => false,
        _ = tokio::signal::ctrl_c() => false,
        r = serve => {
            tracing::error!("the listener stopped: {r:?}");
            true
        }
    };
    // A POST still in flight would land after the DELETE.
    registration.abort();
    let _ = registration.await;
    nmos.unregister().await;
    if failed {
        exit(1);
    }
}
