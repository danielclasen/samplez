//! Session leases: every replica renews
//! the sessions its clients hold live once a second
//! ([`samplez_service::ReplicatedState::renew_sessions`]), every applied
//! command renews its own session, and the leader ends a lease [`LEASE`]
//! after the last applied entry carrying the session with a
//! [`Body::LeaseExpiry`], which releases what the session held, such as
//! touch ownership. An expiry applied after a later renewal of its session
//! is a no-op.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Instant, MissedTickBehavior};

use crate::contact::index_at;
use crate::entry::{Body, SessionKey};
use crate::log_store::Store;
use crate::state_machine::Product;
use crate::{Estimate, Queued, Shared};

/// How long a lease lasts past the stamping index of the last applied
/// entry carrying its session.
pub const LEASE: Duration = Duration::from_secs(2);

/// How often the leader looks for leases to end.
pub const CHECK: Duration = Duration::from_millis(100);

/// [`LEASE`] in samples.
pub fn lease_samples() -> u64 {
    index_at(LEASE.as_nanos() as u64)
}

/// The sessions whose lease ended by `now`, a stamping index, with the
/// renewal each expiry is decided on.
pub fn expired(leases: &BTreeMap<SessionKey, u64>, now: u64) -> Vec<(SessionKey, u64)> {
    let lease = lease_samples();
    leases
        .iter()
        .filter(|&(_, &renewed)| renewed.saturating_add(lease) <= now)
        .map(|(session, &renewed)| (session.clone(), renewed))
        .collect()
}

/// The leader's side: each [`CHECK`], appends an expiry for every lease
/// past [`LEASE`] by E, once per renewal until the write timeout passes
/// without it applied. Pauses while this replica does not lead and while
/// E counts nobody, when nothing is stamped; ends with openraft.
pub(crate) async fn expire<P: Product>(
    shared: Arc<Shared>,
    store: Store<P>,
    queue: UnboundedSender<Queued>,
    estimate: Estimate,
) {
    let metrics = shared.raft.metrics();
    let mut tick = tokio::time::interval(CHECK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Expiries appended and not yet applied: the renewal each was decided
    // on and when it was appended.
    let mut sent: BTreeMap<SessionKey, (u64, Instant)> = BTreeMap::new();
    loop {
        tick.tick().await;
        if metrics.has_changed().is_err() {
            return;
        }
        if !shared.leads() {
            sent.clear();
            continue;
        }
        let Some(e) = shared.e(&estimate) else {
            continue;
        };
        let now = crate::entry::stamping_index(e, shared.settings.block);
        let due = store.read_state(|_, sm| expired(&sm.leases, now));
        let at = Instant::now();
        let timeout = shared.settings.write_timeout;
        sent.retain(|session, (renewed, appended)| {
            due.iter().any(|(s, r)| s == session && r == renewed)
                && at.duration_since(*appended) < timeout
        });
        for (session, renewed) in due {
            if sent.contains_key(&session) {
                continue;
            }
            sent.insert(session.clone(), (renewed, at));
            // The leader stamps it on append.
            let body = Body::LeaseExpiry {
                session,
                renewed,
                at_index: Some(0),
            };
            let _ = queue.send(Queued::Entry {
                body,
                deadline: at + timeout,
                reply: None,
                forwarded: false,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_ends_two_seconds_of_samples_after_its_renewal() {
        let a = SessionKey::from_parts("anonymous", "a");
        let b = SessionKey::from_parts("anonymous", "b");
        let leases = BTreeMap::from([(a.clone(), 1_000), (b, 50_000)]);
        let lease = lease_samples();
        assert_eq!(lease, 96_000, "2 s at 48 kHz");
        assert!(expired(&leases, 1_000 + lease - 1).is_empty());
        assert_eq!(expired(&leases, 1_000 + lease), vec![(a, 1_000)]);
        assert_eq!(expired(&leases, 50_000 + lease).len(), 2);
    }
}
