//! Finding IS-04 v1.3 Registration APIs: a configured URL, else unicast
//! DNS-SD under the configured domain or the resolver's search domains,
//! else multicast DNS-SD.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{ConnectionConfig, NameServerConfig, ResolverConfig};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
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

/// An advertised Registration API: its priority and the base URLs it is
/// reached at, in the order to try them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Instance {
    pub name: String,
    pub pri: u32,
    pub bases: Vec<String>,
}

/// Highest priority (lowest `pri`) first, instances of equal priority in
/// random order, each instance's URLs in its own order; a URL once.
pub(crate) fn order(mut found: Vec<Instance>) -> Vec<Registry> {
    let random = RandomState::new();
    found.sort_by_key(|i| (i.pri, random.hash_one(&i.name)));
    let mut out: Vec<Registry> = Vec::new();
    for i in found {
        for base in i.bases {
            if !out.iter().any(|r| r.base == base) {
                out.push(Registry { base, pri: i.pri });
            }
        }
    }
    out
}

/// SRV targets in the order RFC 2782 has a client try them: lowest
/// priority first, and within one priority a weighted random order.
/// `pick(n)` is a random number in `0..n`.
pub(crate) fn srv_order<T>(
    mut records: Vec<(u16, u16, T)>,
    mut pick: impl FnMut(u64) -> u64,
) -> Vec<T> {
    // Zero weights first within a priority, as the RFC's selection needs.
    records.sort_by_key(|r| (r.0, r.1 != 0));
    let mut out = Vec::with_capacity(records.len());
    while !records.is_empty() {
        let n = records.iter().take_while(|r| r.0 == records[0].0).count();
        let mut group: Vec<_> = records.drain(..n).collect();
        while !group.is_empty() {
            let total: u64 = group.iter().map(|r| u64::from(r.1)).sum();
            let r = pick(total + 1);
            let mut sum = 0;
            let i = group
                .iter()
                .position(|g| {
                    sum += u64::from(g.1);
                    sum >= r
                })
                .unwrap_or(0);
            out.push(group.remove(i).2);
        }
    }
    out
}

fn random(n: u64) -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static DRAWS: AtomicU64 = AtomicU64::new(0);
    RandomState::new().hash_one(DRAWS.fetch_add(1, Ordering::Relaxed)) % n
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
    /// The DNS server to ask instead of the system's.
    nameserver: Option<SocketAddr>,
    mdns: Option<Mdns>,
}

impl Discovery {
    pub fn new(urls: Vec<String>, domain: Option<String>) -> Discovery {
        Discovery {
            urls,
            domain,
            nameserver: None,
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

    fn resolver(&self) -> Result<TokioResolver, String> {
        let builder = match self.nameserver {
            None => TokioResolver::builder_tokio().map_err(|e| e.to_string())?,
            Some(addr) => {
                let mut udp = ConnectionConfig::udp();
                udp.port = addr.port();
                let server = NameServerConfig::new(addr.ip(), true, vec![udp]);
                TokioResolver::builder_with_config(
                    ResolverConfig::from_name_servers(vec![server]),
                    TokioRuntimeProvider::default(),
                )
            }
        };
        builder.build().map_err(|e| e.to_string())
    }

    /// A resolver built for each round, so no failed answer cached from
    /// before a DNS server came up outlives the round, and a changed
    /// resolver configuration applies.
    async fn unicast(&self) -> Vec<Instance> {
        let resolver = match self.resolver() {
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
                .map(|(c, _)| {
                    c.search()
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect()
                })
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

/// An instance's TXT and SRV records, every SRV target and every address
/// of each in the order to try them.
async fn instance_of(resolver: &TokioResolver, instance: &str) -> Option<Instance> {
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
    let targets = srv
        .answers()
        .iter()
        .filter_map(|r| match &r.data {
            RData::SRV(s) => Some((s.priority, s.weight, (s.target.to_string(), s.port))),
            _ => None,
        })
        .collect();
    let mut bases = Vec::new();
    for (target, port) in srv_order(targets, random) {
        if let Ok(ips) = resolver.lookup_ip(target.as_str()).await {
            bases.extend(ips.iter().map(|ip| base_url(&ip, port)));
        }
    }
    (!bases.is_empty()).then(|| Instance {
        name: instance.to_owned(),
        pri,
        bases,
    })
}

/// A multicast DNS-SD browse that runs for as long as it is held, keeping
/// the registries currently advertised.
struct Mdns {
    found: Arc<Mutex<BTreeMap<String, Instance>>>,
    daemon: mdns_sd::ServiceDaemon,
}

/// The daemon's thread runs until told to exit, with its sockets; it
/// closes the browse, which ends the thread reading it.
impl Drop for Mdns {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.shutdown() {
            tracing::warn!("nmos: multicast DNS-SD did not shut down: {e}");
        }
    }
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
                    let mut found = shared
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match event {
                        mdns_sd::ServiceEvent::ServiceResolved(s) => {
                            let txt = s.txt_properties.iter().map(|p| (p.key(), p.val_str()));
                            let mut ips: Vec<IpAddr> = s
                                .addresses
                                .iter()
                                .map(mdns_sd::ScopedIp::to_ip_addr)
                                .collect();
                            ips.sort_by_key(IpAddr::is_ipv6);
                            match suitable(txt) {
                                Some(pri) if !ips.is_empty() => {
                                    found.insert(
                                        s.fullname.clone(),
                                        Instance {
                                            name: s.fullname.clone(),
                                            pri,
                                            bases: ips
                                                .iter()
                                                .map(|ip| base_url(ip, s.port))
                                                .collect(),
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
        Ok(Mdns { found, daemon })
    }

    fn found(&self) -> Vec<Instance> {
        self.found
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        let i = |name: &str, pri, bases: &[&str]| Instance {
            name: name.into(),
            pri,
            bases: bases.iter().map(std::string::ToString::to_string).collect(),
        };
        let found = vec![
            i("c", 30, &["c"]),
            i("a", 10, &["a1", "a2"]),
            i("b1", 20, &["b1"]),
            i("b2", 20, &["b2"]),
            i("b3", 20, &["b3", "a1"]),
        ];
        let mut firsts = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let o = order(found.clone());
            let pris: Vec<u32> = o.iter().map(|r| r.pri).collect();
            assert_eq!(pris, [10, 10, 20, 20, 20, 30]);
            // An instance's URLs stay in its order; a URL comes once.
            assert_eq!((o[0].base.as_str(), o[1].base.as_str()), ("a1", "a2"));
            firsts.insert(o[2].base.clone());
        }
        // Each of the equal registries comes first some of the time.
        assert_eq!(firsts.len(), 3, "{firsts:?}");
    }

    #[test]
    fn srv_targets_go_by_priority_then_weight() {
        let records = vec![
            (20, 0, "late"),
            (10, 0, "light"),
            (10, 60, "heavy"),
            (5, 1, "first"),
        ];
        let mut heavy_first = 0;
        for _ in 0..200 {
            let o = srv_order(records.clone(), random);
            assert_eq!(o.len(), 4);
            assert_eq!((o[0], o[3]), ("first", "late"));
            heavy_first += usize::from(o[1] == "heavy");
        }
        // Weight 60 against 0: the light one comes first about 1 in 61.
        assert!(heavy_first > 180, "{heavy_first}");
        // The draw decides within a priority: 0 picks the zero weight.
        assert_eq!(srv_order(vec![(1, 5, "w"), (1, 0, "z")], |_| 0), ["z", "w"]);
        assert_eq!(
            srv_order(vec![(1, 5, "w"), (1, 0, "z")], |n| n - 1),
            ["w", "z"]
        );
    }

    /// Answers from a fixed zone over UDP.
    async fn dns_server(zone: Vec<hickory_resolver::proto::rr::Record>) -> SocketAddr {
        use hickory_resolver::proto::op::Message;
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                let (n, from) = socket.recv_from(&mut buf).await.unwrap();
                let query = Message::from_vec(&buf[..n]).unwrap();
                let mut reply = Message::response(query.metadata.id, query.metadata.op_code);
                for q in &query.queries {
                    reply.add_query(q.clone());
                    for r in &zone {
                        if r.name == *q.name() && r.record_type() == q.query_type() {
                            reply.add_answer(r.clone());
                        }
                    }
                }
                socket
                    .send_to(&reply.to_vec().unwrap(), from)
                    .await
                    .unwrap();
            }
        });
        addr
    }

    #[tokio::test]
    async fn unicast_dns_sd_tries_every_target_and_address_by_priority() {
        use hickory_resolver::proto::rr::rdata::{A, PTR, SRV, TXT};
        use hickory_resolver::proto::rr::{Name, Record};
        let name = |s: &str| Name::from_ascii(s).unwrap();
        let rec = |n: &str, d: RData| Record::from_rdata(name(n), 60, d);
        let service = "_nmos-register._tcp.test.example.";
        let txt = |pri: &str| {
            RData::TXT(TXT::new(vec![
                "api_ver=v1.2,v1.3".into(),
                "api_proto=http".into(),
                "api_auth=false".into(),
                format!("pri={pri}"),
            ]))
        };
        let a = |ip: [u8; 4]| RData::A(A::from(std::net::Ipv4Addr::from(ip)));
        let zone = vec![
            rec(
                service,
                RData::PTR(PTR(name("one._nmos-register._tcp.test.example."))),
            ),
            rec(
                service,
                RData::PTR(PTR(name("two._nmos-register._tcp.test.example."))),
            ),
            rec(
                service,
                RData::PTR(PTR(name("old._nmos-register._tcp.test.example."))),
            ),
            rec("one._nmos-register._tcp.test.example.", txt("10")),
            rec("two._nmos-register._tcp.test.example.", txt("5")),
            rec(
                "old._nmos-register._tcp.test.example.",
                RData::TXT(TXT::new(vec![
                    "api_ver=v1.2".into(),
                    "api_proto=http".into(),
                    "api_auth=false".into(),
                    "pri=0".into(),
                ])),
            ),
            rec(
                "one._nmos-register._tcp.test.example.",
                RData::SRV(SRV::new(20, 0, 2000, name("h2.test.example."))),
            ),
            rec(
                "one._nmos-register._tcp.test.example.",
                RData::SRV(SRV::new(10, 0, 1000, name("h1.test.example."))),
            ),
            rec(
                "two._nmos-register._tcp.test.example.",
                RData::SRV(SRV::new(0, 0, 3000, name("h3.test.example."))),
            ),
            rec(
                "old._nmos-register._tcp.test.example.",
                RData::SRV(SRV::new(0, 0, 9, name("h3.test.example."))),
            ),
            rec("h1.test.example.", a([127, 0, 0, 2])),
            rec("h1.test.example.", a([127, 0, 0, 3])),
            rec("h2.test.example.", a([127, 0, 0, 4])),
            rec("h3.test.example.", a([127, 0, 0, 5])),
        ];
        let mut d = Discovery::new(Vec::new(), Some("test.example".into()));
        d.nameserver = Some(dns_server(zone).await);
        let found: Vec<(String, u32)> = d
            .find()
            .await
            .into_iter()
            .map(|r| (r.base, r.pri))
            .collect();
        let url = |hp: &str| format!("http://{hp}/x-nmos/registration/v1.3");
        assert_eq!(found.len(), 4, "{found:?}");
        assert_eq!(found[0], (url("127.0.0.5:3000"), 5));
        let mut h1: Vec<_> = found[1..3].to_vec();
        h1.sort();
        assert_eq!(
            h1,
            [(url("127.0.0.2:1000"), 10), (url("127.0.0.3:1000"), 10)]
        );
        assert_eq!(found[3], (url("127.0.0.4:2000"), 10));
        assert!(d.mdns.is_none());
    }

    /// The names of this process's threads.
    fn threads() -> Vec<String> {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
            .map(|c| c.trim_end().to_owned())
            .collect()
    }

    #[test]
    fn dropping_the_multicast_browse_ends_its_threads() {
        let count = |name: &str| threads().iter().filter(|t| *t == name).count();
        let wait = |what: &str, done: &dyn Fn() -> bool| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !done() {
                assert!(std::time::Instant::now() < deadline, "{what}");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        };
        let before = (count("mDNS_daemon"), count("nmos-mdns"));
        let mdns = Mdns::start().unwrap();
        wait("the threads start", &|| {
            (count("mDNS_daemon"), count("nmos-mdns")) == (before.0 + 1, before.1 + 1)
        });
        drop(mdns);
        wait("the threads end", &|| {
            (count("mDNS_daemon"), count("nmos-mdns")) == before
        });
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
