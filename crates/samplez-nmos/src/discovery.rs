//! Finding IS-04 v1.3 Registration APIs: a configured URL, else unicast
//! DNS-SD under the configured domain or the resolver's search domains,
//! else multicast DNS-SD.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::{RData, RecordType};

use crate::resources::NODE_VERSION;

const SERVICE: &str = "_nmos-register._tcp";
const MDNS_SERVICE: &str = "_nmos-register._tcp.local.";

/// A Registration API: its base URL up to and including the version, and
/// its advertised priority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Registry {
    pub base: String,
    pub pri: u32,
}

/// The priority a registry's TXT record gives, or `None` when the
/// registry is not one this node uses: one serving IS-04 v1.3 over plain
/// HTTP without authorization.
pub(crate) fn suitable<'a>(txt: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<u32> {
    let txt: BTreeMap<&str, &str> = txt.into_iter().collect();
    let versions = txt.get("api_ver")?;
    let ok = versions.split(',').any(|v| v.trim() == NODE_VERSION)
        && txt.get("api_proto") == Some(&"http")
        && txt.get("api_auth") == Some(&"false");
    ok.then(|| txt.get("pri")?.parse().ok()).flatten()
}

/// Highest priority (lowest `pri`) first, registries of equal priority in
/// random order.
pub(crate) fn order(mut found: Vec<Registry>) -> Vec<Registry> {
    let random = RandomState::new();
    found.sort_by_key(|r| (r.pri, random.hash_one(&r.base)));
    found.dedup_by(|a, b| a.base == b.base);
    found
}

pub(crate) fn base_url(host: &IpAddr, port: u16) -> String {
    format!(
        "{}/x-nmos/registration/{NODE_VERSION}",
        crate::resources::base_url(&host.to_string(), port)
    )
}

pub(crate) struct Discovery {
    urls: Vec<String>,
    domain: Option<String>,
    mdns: Option<Mdns>,
}

impl Discovery {
    pub fn new(urls: Vec<String>, domain: Option<String>) -> Discovery {
        Discovery {
            urls,
            domain,
            mdns: None,
        }
    }

    /// The registries to try, in order. Multicast DNS-SD starts only when
    /// unicast DNS-SD finds nothing and keeps browsing from then on.
    pub async fn find(&mut self) -> Vec<Registry> {
        if !self.urls.is_empty() {
            let base = |u: &String| {
                format!(
                    "{}/x-nmos/registration/{NODE_VERSION}",
                    u.trim_end_matches('/')
                )
            };
            return self
                .urls
                .iter()
                .zip(0..)
                .map(|(u, pri)| Registry { base: base(u), pri })
                .collect();
        }
        let unicast = self.unicast().await;
        if !unicast.is_empty() {
            return order(unicast);
        }
        if self.mdns.is_none() {
            match Mdns::start() {
                Ok(m) => self.mdns = Some(m),
                Err(e) => tracing::warn!("nmos: multicast DNS-SD did not start: {e}"),
            }
        }
        order(self.mdns.as_ref().map(Mdns::found).unwrap_or_default())
    }

    /// A resolver built for each round, so no failed answer cached from
    /// before a DNS server came up outlives the round, and a changed
    /// resolver configuration applies.
    async fn unicast(&self) -> Vec<Registry> {
        let resolver = match TokioResolver::builder_tokio().and_then(|b| b.build()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("nmos: no DNS resolver: {e}");
                return Vec::new();
            }
        };
        let resolver = &resolver;
        let domains: Vec<String> = match &self.domain {
            Some(d) => vec![d.clone()],
            None => hickory_resolver::system_conf::read_system_conf()
                .map(|(c, _)| c.search().iter().map(|n| n.to_string()).collect())
                .unwrap_or_default(),
        };
        let mut found = Vec::new();
        for domain in domains {
            let name = format!("{SERVICE}.{}.", domain.trim_end_matches('.'));
            let Ok(ptrs) = resolver.lookup(name.as_str(), RecordType::PTR).await else {
                continue;
            };
            for ptr in ptrs.answers() {
                let RData::PTR(instance) = &ptr.data else {
                    continue;
                };
                if let Some(r) = instance_of(resolver, &instance.0.to_string()).await {
                    found.push(r);
                }
            }
        }
        found
    }
}

/// An instance's SRV and TXT records, its host resolved.
async fn instance_of(resolver: &TokioResolver, instance: &str) -> Option<Registry> {
    let txt = resolver.lookup(instance, RecordType::TXT).await.ok()?;
    let strings: Vec<String> = txt
        .answers()
        .iter()
        .filter_map(|r| match &r.data {
            RData::TXT(t) => Some(
                t.txt_data
                    .iter()
                    .map(|s| String::from_utf8_lossy(s).into_owned()),
            ),
            _ => None,
        })
        .flatten()
        .collect();
    let pri = suitable(strings.iter().filter_map(|s| s.split_once('=')))?;
    let srv = resolver.lookup(instance, RecordType::SRV).await.ok()?;
    let (target, port) = srv.answers().iter().find_map(|r| match &r.data {
        RData::SRV(s) => Some((s.target.to_string(), s.port)),
        _ => None,
    })?;
    let ip = resolver
        .lookup_ip(target.as_str())
        .await
        .ok()?
        .iter()
        .next()?;
    Some(Registry {
        base: base_url(&ip, port),
        pri,
    })
}

/// A multicast DNS-SD browse that runs for as long as it is held, keeping
/// the registries currently advertised.
struct Mdns {
    found: Arc<Mutex<BTreeMap<String, Registry>>>,
    _daemon: mdns_sd::ServiceDaemon,
}

impl Mdns {
    fn start() -> Result<Mdns, mdns_sd::Error> {
        let daemon = mdns_sd::ServiceDaemon::new()?;
        let events = daemon.browse(MDNS_SERVICE)?;
        let found = Arc::new(Mutex::new(BTreeMap::new()));
        let shared = found.clone();
        std::thread::Builder::new()
            .name("nmos-mdns".into())
            .spawn(move || {
                while let Ok(event) = events.recv() {
                    let mut found = shared.lock().unwrap_or_else(|e| e.into_inner());
                    match event {
                        mdns_sd::ServiceEvent::ServiceResolved(s) => {
                            let txt = s.txt_properties.iter().map(|p| (p.key(), p.val_str()));
                            let ip = s
                                .addresses
                                .iter()
                                .map(|a| a.to_ip_addr())
                                .min_by_key(IpAddr::is_ipv6);
                            match (suitable(txt), ip) {
                                (Some(pri), Some(ip)) => {
                                    found.insert(
                                        s.fullname.clone(),
                                        Registry {
                                            base: base_url(&ip, s.port),
                                            pri,
                                        },
                                    );
                                }
                                _ => {
                                    found.remove(&s.fullname);
                                }
                            }
                        }
                        mdns_sd::ServiceEvent::ServiceRemoved(_, name) => {
                            found.remove(&name);
                        }
                        _ => {}
                    }
                }
            })
            .map_err(|e| mdns_sd::Error::Msg(e.to_string()))?;
        Ok(Mdns {
            found,
            _daemon: daemon,
        })
    }

    fn found(&self) -> Vec<Registry> {
        self.found
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txt<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Iterator<Item = (&'a str, &'a str)> {
        pairs.iter().copied()
    }

    #[test]
    fn only_v1_3_over_http_without_auth_is_suitable() {
        let ok = [
            ("api_ver", "v1.2,v1.3"),
            ("api_proto", "http"),
            ("api_auth", "false"),
            ("pri", "20"),
        ];
        assert_eq!(suitable(txt(&ok)), Some(20));
        assert_eq!(
            suitable(txt(&[
                ("api_ver", "v1.3"),
                ("api_proto", "http"),
                ("api_auth", "false"),
                ("pri", "100")
            ])),
            Some(100)
        );
        for bad in [
            [
                ("api_ver", "v9.0"),
                ("api_proto", "http"),
                ("api_auth", "false"),
                ("pri", "10"),
            ],
            [
                ("api_ver", "v1.3"),
                ("api_proto", "invalid"),
                ("api_auth", "false"),
                ("pri", "10"),
            ],
            [
                ("api_ver", "v1.3"),
                ("api_proto", "https"),
                ("api_auth", "false"),
                ("pri", "10"),
            ],
            [
                ("api_ver", "v1.3"),
                ("api_proto", "http"),
                ("api_auth", "true"),
                ("pri", "10"),
            ],
            [
                ("api_ver", "v1.30"),
                ("api_proto", "http"),
                ("api_auth", "false"),
                ("pri", "10"),
            ],
            [
                ("api_ver", "v1.3"),
                ("api_proto", "http"),
                ("api_auth", "false"),
                ("pri", "x"),
            ],
        ] {
            assert_eq!(suitable(txt(&bad)), None, "{bad:?}");
        }
        assert_eq!(suitable(txt(&ok[..3])), None);
    }

    #[test]
    fn registries_are_ordered_by_priority_and_shuffled_among_equals() {
        let r = |base: &str, pri| Registry {
            base: base.into(),
            pri,
        };
        let found = vec![
            r("c", 30),
            r("a", 10),
            r("b1", 20),
            r("b2", 20),
            r("b3", 20),
            r("a", 10),
        ];
        let mut firsts = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let o = order(found.clone());
            let pris: Vec<u32> = o.iter().map(|r| r.pri).collect();
            assert_eq!(pris, [10, 20, 20, 20, 30]);
            firsts.insert(o[1].base.clone());
        }
        // Each of the equal registries comes first some of the time.
        assert_eq!(firsts.len(), 3, "{firsts:?}");
    }

    #[test]
    fn configured_urls_are_the_only_registries_in_their_order() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let urls = vec!["http://192.0.2.5:8010/".into(), "http://192.0.2.4".into()];
        let mut d = Discovery::new(urls, Some("example.com".into()));
        let found = rt.block_on(d.find());
        let r = |base: &str, pri| Registry {
            base: base.into(),
            pri,
        };
        assert_eq!(
            found,
            [
                r("http://192.0.2.5:8010/x-nmos/registration/v1.3", 0),
                r("http://192.0.2.4/x-nmos/registration/v1.3", 1)
            ]
        );
        assert!(d.mdns.is_none());
    }

    #[test]
    fn hosts_are_written_into_urls() {
        assert_eq!(
            base_url(&"192.0.2.1".parse().unwrap(), 80),
            "http://192.0.2.1:80/x-nmos/registration/v1.3"
        );
        assert_eq!(
            base_url(&"2001:db8::2".parse().unwrap(), 81),
            "http://[2001:db8::2]:81/x-nmos/registration/v1.3"
        );
    }
}
