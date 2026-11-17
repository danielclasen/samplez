//! The compatibility key and the target revision (plan 2.11, Baseline):
//! what a rollout that changes the key hands the baseline over by. Page
//! hashes are compared and state is transferred only between replicas
//! with equal keys; the target revision only picks which key the
//! baseline moves to.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use samplez::blocks::PAGE;

use crate::bootstrap::Kube;

/// The compatibility key of an engine of version `engine` running a
/// shape and the config that affects output, encoded as `output`:
/// XXH3-64 over both, each prefixed with its length.
pub fn key(engine: &str, output: &[u8]) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3Default::new();
    for part in [engine.as_bytes(), output] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
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

/// What a replica knows of a rollout.
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

    /// The key changes with the engine version and with the output
    /// config, and the two parts do not run into each other.
    #[test]
    fn the_key_follows_engine_version_and_output_config() {
        let k = key("0.4.0", b"shape");
        assert_eq!(k, key("0.4.0", b"shape"));
        assert_ne!(k, key("0.5.0", b"shape"));
        assert_ne!(k, key("0.4.0", b"shape2"));
        assert_ne!(key("0.4.0s", b"hape"), k);
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
