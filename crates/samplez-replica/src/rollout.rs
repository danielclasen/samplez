//! The compatibility key and the target revision (plan 2.11, Baseline):
//! what a rollout that changes the key hands the baseline over by. Page
//! hashes are compared and state is transferred only between replicas
//! with equal keys; the target revision only picks which key the
//! baseline moves to.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use samplez::blocks::PAGE;

use crate::bootstrap::Kube;

/// The versions a replica computes, logs and derives by. Replicas whose
/// versions and output config are equal compute equal pages from one
/// log, so a release that bumps none of them rolls into a running set as
/// a peer; any other change to the key hands the baseline over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versions {
    /// [`samplez::COMPUTE_VERSION`].
    pub compute: u32,
    /// [`crate::PROTOCOL`].
    pub protocol: u32,
    /// [`crate::Product::DERIVATION_VERSION`].
    pub derivation: u32,
}

impl Versions {
    /// The compatibility key of these versions with the config that
    /// affects output, encoded as `output`.
    pub fn key(&self, output: &[u8]) -> u64 {
        key_of(*self, output)
    }

    /// The versions of this build running product `P`.
    pub fn of<P: crate::Product>() -> Versions {
        Versions {
            compute: samplez::COMPUTE_VERSION,
            protocol: crate::PROTOCOL,
            derivation: P::DERIVATION_VERSION,
        }
    }
}

/// The compatibility key of this build running product `P` with a shape
/// and the config that affects output, encoded as `output`. It holds the
/// [`Versions`] and `output` only, so a release that bumps no version
/// keeps it.
pub fn key<P: crate::Product>(output: &[u8]) -> u64 {
    Versions::of::<P>().key(output)
}

/// XXH3-64 over the three versions, little-endian, and `output` prefixed
/// with its length.
fn key_of(v: Versions, output: &[u8]) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3Default::new();
    for part in [v.compute, v.protocol, v.derivation] {
        h.update(&part.to_le_bytes());
    }
    h.update(&(output.len() as u64).to_le_bytes());
    h.update(output);
    h.digest()
}

/// The at_index of a `Baseline` that hands the baseline over to another
/// key, at or after `at`: a multiple of both the page and the block size
/// `block`, so P, the first page boundary at or after it, is a block
/// boundary. The old-key replicas then stop at a block boundary right
/// after a block they committed and fill on from P, with no block split
/// at P that none of them could fill.
pub fn hand_over_at(at: u64, block: u64) -> u64 {
    let mut a = PAGE;
    let mut b = block;
    while b != 0 {
        (a, b) = (b, a % b);
    }
    at.next_multiple_of(PAGE / a * block)
}

/// What a replica knows of a rollout. The contact loop calls it under
/// its lock: a call back into the [`crate::Replica`] panics there.
pub trait Rollout: Send + Sync + 'static {
    /// This replica's compatibility key.
    fn key(&self) -> u64;
    /// This pod's revision, its `controller-revision-hash` label.
    fn revision(&self) -> Option<String>;
    /// The audio StatefulSet's `status.updateRevision`, as last read; the
    /// leader reads it.
    fn target(&self) -> Option<String>;
}

/// A key and a revision fixed at start, and a target revision set from
/// outside ([`follow`]).
#[derive(Debug)]
pub struct Fixed {
    key: u64,
    revision: Option<String>,
    target: Mutex<Option<String>>,
}

impl Fixed {
    pub fn new(key: u64, revision: Option<String>) -> Arc<Fixed> {
        Arc::new(Fixed {
            key,
            revision,
            target: Mutex::new(None),
        })
    }

    pub fn set_target(&self, target: Option<String>) {
        *self.target.lock().expect("target lock poisoned") = target;
    }
}

impl Rollout for Fixed {
    fn key(&self) -> u64 {
        self.key
    }

    fn revision(&self) -> Option<String> {
        self.revision.clone()
    }

    fn target(&self) -> Option<String> {
        self.target.lock().expect("target lock poisoned").clone()
    }
}

/// Reads the StatefulSet `statefulset`'s target revision into `into`
/// every `every`; a failed read keeps the last one.
pub async fn follow(kube: Arc<Kube>, statefulset: String, into: Arc<Fixed>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    loop {
        tick.tick().await;
        if let Ok(target) = kube.update_revision(&statefulset).await {
            into.set_target(target);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: Versions = Versions {
        compute: 1,
        protocol: 2,
        derivation: 3,
    };

    /// The key changes with each version and with the output config.
    #[test]
    fn the_key_follows_every_version_and_the_output_config() {
        let k = key_of(V, b"shape");
        assert_eq!(k, key_of(V, b"shape"));
        for other in [
            Versions { compute: 2, ..V },
            Versions { protocol: 3, ..V },
            Versions { derivation: 4, ..V },
        ] {
            assert_ne!(k, key_of(other, b"shape"), "{other:?}");
        }
        assert_ne!(k, key_of(V, b"shape2"));
    }

    /// The key is a function of the versions and the output config alone:
    /// its value for fixed ones is pinned, so a release that bumps the
    /// crate version and no constant keeps every replica's key, and a
    /// change to how the key is built fails here first.
    #[test]
    fn the_key_holds_no_crate_version() {
        assert_eq!(key_of(V, b"shape"), 0xac8b_212e_184b_8c77);
    }

    /// A hand-over's P is a block boundary: at B = 48 every third page
    /// boundary.
    #[test]
    fn a_hand_over_index_is_a_page_and_block_boundary() {
        assert_eq!(hand_over_at(1, 48), 3 * PAGE);
        assert_eq!(hand_over_at(3 * PAGE, 48), 3 * PAGE);
        assert_eq!(hand_over_at(3 * PAGE + 1, 48), 6 * PAGE);
        assert_eq!(hand_over_at(5, 64), PAGE);
    }
}
