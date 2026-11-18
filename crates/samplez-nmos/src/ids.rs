//! Resource ids: UUIDv5 under a fixed namespace, from the seed and a path
//! naming the resource, so a restart with the same seed keeps them.

use uuid::{Uuid, uuid};

const NAMESPACE: Uuid = uuid!("3bd5c2df-88d0-4122-8009-349861a360c8");

#[derive(Debug, Clone)]
pub(crate) struct Ids {
    pub node: Uuid,
    pub device: Uuid,
    pub senders: Vec<SenderIds>,
    pub receivers: Vec<Uuid>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SenderIds {
    pub source: Uuid,
    pub flow: Uuid,
    pub sender: Uuid,
}

impl Ids {
    pub fn derive<'a>(
        seed: &str,
        senders: impl Iterator<Item = &'a str>,
        receivers: impl Iterator<Item = &'a str>,
    ) -> Ids {
        let base = Uuid::new_v5(&NAMESPACE, seed.as_bytes());
        let id = |path: &str| Uuid::new_v5(&base, path.as_bytes());
        Ids {
            node: id("node"),
            device: id("device"),
            senders: senders
                .map(|k| SenderIds {
                    source: id(&format!("source/{k}")),
                    flow: id(&format!("flow/{k}")),
                    sender: id(&format!("sender/{k}")),
                })
                .collect(),
            receivers: receivers.map(|k| id(&format!("receiver/{k}"))).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_depend_on_the_seed_and_the_key_only() {
        let a = Ids::derive("ns/rel", ["out"].into_iter(), ["in"].into_iter());
        // The same seed and keys in another model give the same ids.
        let b = Ids::derive("ns/rel", ["x", "out"].into_iter(), ["in"].into_iter());
        assert_eq!(a.node, b.node);
        assert_eq!(a.senders[0].sender, b.senders[1].sender);
        assert_eq!(a.receivers, b.receivers);
        let c = Ids::derive("ns/other", ["out"].into_iter(), ["in"].into_iter());
        assert_ne!(a.node, c.node);
        assert_ne!(a.senders[0].flow, c.senders[0].flow);
        // Pinned: a change of the namespace or the paths would re-key
        // every deployed node in its registry.
        assert_eq!(a.node.to_string(), "af5cb3fb-e9a8-529d-a0f5-1c54af68f609");
    }

    #[test]
    fn ids_are_distinct_and_version_5() {
        let a = Ids::derive("s", ["a", "b"].into_iter(), ["a", "b"].into_iter());
        let mut all = vec![a.node, a.device];
        for s in &a.senders {
            all.extend([s.source, s.flow, s.sender]);
        }
        all.extend(&a.receivers);
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n);
        assert!(all.iter().all(|u| u.get_version_num() == 5));
    }
}
