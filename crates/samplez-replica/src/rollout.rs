//! The compatibility key and the target revision (plan 2.11, Baseline):
//! what a rollout that changes the key hands the baseline over by. Page
//! hashes are compared and state is transferred only between replicas
//! with equal keys; the target revision only picks which key the
//! baseline moves to.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use samplez::blocks::PAGE;
use serde::{Deserialize, Serialize};

use crate::bootstrap::Kube;
use crate::entry::Baseline;
use crate::report::Own;

/// The versions a replica computes, logs and derives by. Replicas whose
/// versions and output config are equal compute equal pages from one
/// log, so a release that bumps none of them rolls into a running set as
/// a peer; any other change to the key hands the baseline over.
///
/// They are ordered by compute, then protocol, then derivation: a
/// hand-over moves the baseline only to higher versions
/// ([`may_take_over`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// Whether a replica reporting versions `mine`, under another key than
/// the latest `Baseline`'s, may take the baseline over from that
/// `Baseline`'s versions `base`, `live` the versions every live audio
/// voter reports. Higher versions may, and equal ones, whose keys differ
/// by the output config alone. Lower ones may only once `base` is
/// `gone`, no voter carrying it for the departure window
/// ([`DEPARTED`]), and then only the highest live. No versions, an older
/// peer's, rank below any.
pub fn may_take_over(
    mine: Option<Versions>,
    base: Option<Versions>,
    gone: bool,
    live: &[Option<Versions>],
) -> bool {
    mine >= base || (gone && live.iter().all(|l| *l <= mine))
}

/// How long no live audio voter carries a `Baseline`'s versions before
/// they count as gone and lower ones may take the baseline over: as long
/// as no leader counting a replica takes before it counts as departed
/// ([`crate::membership::UNCOUNTED`]). A voter unheard for a contact
/// window is no departure.
pub const DEPARTED: std::time::Duration = crate::membership::UNCOUNTED;

/// The key and versions of the latest `Baseline` `b`, `named` the latest
/// report of the replica it names. A `Baseline` named before that replica
/// reported them carries none, and its report tells them while it
/// commits: what it commits is the baseline's, and a replica restarted
/// under another key since commits nothing until named again. The leader
/// and every replica resolve them by this alone, so a replica's own rules
/// see the key the leader hands over from.
pub(crate) fn baseline_of(b: &Baseline, named: Option<&Own>) -> (Option<u64>, Option<Versions>) {
    let named = named.filter(|o| o.state == Some(crate::state::ReplicaState::Committing));
    (
        b.key.or_else(|| named.and_then(|o| o.key)),
        b.versions.or_else(|| named.and_then(|o| o.versions)),
    )
}

/// Whether versions `base` are known and no live voter, its versions in
/// `live`, carries them now.
pub fn base_gone(base: Option<Versions>, live: &[Option<Versions>]) -> bool {
    base.is_some() && !live.contains(&base)
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

/// The `at_index` of a `Baseline` that hands the baseline over to another
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
    /// The versions the key holds, reported beside it; none where they
    /// are not known.
    fn versions(&self) -> Option<Versions> {
        None
    }
    /// This pod's revision, its `controller-revision-hash` label.
    fn revision(&self) -> Option<String>;
    /// The audio StatefulSet's `status.updateRevision`, as last read; the
    /// leader reads it.
    fn target(&self) -> Option<String>;
}

/// A key, its versions and a revision fixed at start, and a target
/// revision set from outside ([`follow`]).
#[derive(Debug)]
pub struct Fixed {
    key: u64,
    versions: Option<Versions>,
    revision: Option<String>,
    target: Mutex<Option<String>>,
}

impl Fixed {
    pub fn new(key: u64, versions: Option<Versions>, revision: Option<String>) -> Arc<Fixed> {
        Arc::new(Fixed {
            key,
            versions,
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

    fn versions(&self) -> Option<Versions> {
        self.versions
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

    /// Compute ranks first, then protocol, then derivation: a higher
    /// compute version outranks any protocol or derivation version.
    #[test]
    fn versions_order_by_compute_then_protocol_then_derivation() {
        let higher = [
            Versions { derivation: 4, ..V },
            Versions {
                protocol: 3,
                derivation: 0,
                ..V
            },
            Versions {
                compute: 2,
                protocol: 0,
                derivation: 0,
            },
        ];
        let mut prev = V;
        for v in higher {
            assert!(v > prev, "{v:?} above {prev:?}");
            prev = v;
        }
        let mut sorted = vec![higher[2], higher[0], V, higher[1]];
        sorted.sort();
        assert_eq!(sorted, vec![V, higher[0], higher[1], higher[2]]);
    }

    /// Higher and equal versions take over; lower ones only once no live
    /// voter carries the baseline's, and then only the highest live;
    /// none never outranks some, and a set without any hands over.
    #[test]
    fn lower_versions_take_over_only_once_the_baselines_are_gone() {
        let up = Some(Versions { compute: 2, ..V });
        let base = Some(V);
        let low = Some(Versions { compute: 0, ..V });
        assert!(may_take_over(up, base, false, &[base, up]));
        assert!(
            !may_take_over(low, base, false, &[base, low]),
            "a downgrade"
        );
        assert!(
            may_take_over(base, base, false, &[base]),
            "equal versions: another output config"
        );
        assert!(
            !may_take_over(low, base, false, &[low, low]),
            "unheard, not yet gone"
        );
        assert!(may_take_over(low, base, true, &[low, low]), "gone");
        assert!(
            !may_take_over(low, up, true, &[low, base]),
            "a higher live voter than it"
        );
        assert!(
            may_take_over(base, up, true, &[low, base]),
            "the highest live"
        );
        assert!(
            !may_take_over(None, base, false, &[base, None]),
            "none below some"
        );
        assert!(
            may_take_over(base, None, false, &[None, base]),
            "some above none"
        );
        assert!(may_take_over(None, None, false, &[None]), "none known");
        assert!(base_gone(base, &[low, None]));
        assert!(!base_gone(base, &[low, base]));
        assert!(!base_gone(None, &[low]), "unknown versions are never gone");
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
