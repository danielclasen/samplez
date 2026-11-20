//! Input liveness: whether an input is up or down is the audio voters'
//! majority decision, not one engine's. Each audio voter reports the
//! slots its engine reads and those it does not ([`Own::readable`],
//! [`Own::unreadable`]); the leader takes a slot down once more than half
//! of the audio voters of every configuration report it unreadable, up
//! once more than half report it readable, and else leaves it as the log
//! has it. A silent voter, and one that says neither of a slot, counts
//! for neither side. A decided change that has held for
//! `pruneAfterBlocks` is handed to the product
//! ([`crate::Replica::input_liveness`]), which appends it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use samplez::Report;
use tokio::time::Instant;

use crate::baseline::Voter;
use crate::entry::NodeId;
use crate::membership::{Slot, slot_of};
use crate::report::Own;

/// How long a change handed on is not handed on again while the log
/// does not show it.
const INFLIGHT: Duration = Duration::from_secs(1);

/// The slots a majority of the audio voters reads, and those a majority
/// does not read, by bit: in every configuration of `configs`, more than
/// half of its voters but the witness. `audio` are the audio voters; a
/// silent one counts for neither side.
pub(crate) fn majority(configs: &[BTreeSet<NodeId>], audio: &[Voter]) -> (u64, u64) {
    if configs.is_empty() {
        return (0, 0);
    }
    let (mut up, mut down) = (u64::MAX, u64::MAX);
    for config in configs {
        let n = (config.iter())
            .filter(|&&id| slot_of(id) != Slot::Witness)
            .count();
        let heard: Vec<&Own> = (audio.iter())
            .filter(|v| !v.silent && config.contains(&v.id))
            .filter_map(|v| v.own)
            .collect();
        let (mut u, mut d) = (0, 0);
        for slot in 0..64 {
            let bit = 1u64 << slot;
            let reads = heard.iter().filter(|o| o.readable & bit != 0).count();
            let lost = (heard.iter())
                .filter(|o| o.unreadable & !o.readable & bit != 0)
                .count();
            if reads > n / 2 {
                u |= bit;
            }
            if lost > n / 2 {
                d |= bit;
            }
        }
        up &= u;
        down &= d;
    }
    (up, down)
}

/// The leader's decisions on the inputs' liveness.
#[derive(Debug, Default)]
pub(crate) struct Liveness {
    /// Per slot, the state a majority decided against the log's, up or
    /// down, and since when it has.
    changing: BTreeMap<u32, (bool, Instant)>,
    /// The changes handed on, with when.
    handed: BTreeMap<u32, (bool, Instant)>,
}

impl Liveness {
    /// Leadership or contact is lost: decided anew.
    pub fn step_down(&mut self) {
        self.changing.clear();
        self.handed.clear();
    }

    /// One period of the leader in contact: `up` and `down` as
    /// [`majority`] decides them, `log_up` the slots up in the log.
    pub fn period(&mut self, up: u64, down: u64, log_up: u64, now: Instant) {
        for slot in 0..64 {
            let bit = 1u64 << slot;
            let logged = log_up & bit != 0;
            let decided = (up & bit != 0) || (down & bit == 0 && logged);
            if decided == logged {
                self.changing.remove(&slot);
            } else if self.changing.get(&slot).is_none_or(|c| c.0 != decided) {
                self.changing.insert(slot, (decided, now));
            }
            if self.handed.get(&slot).is_some_and(|h| h.0 == logged) {
                self.handed.remove(&slot);
            }
        }
    }

    /// The changes that held for `debounce`, as an engine reports them:
    /// `InputPruned` to take an input down, `InputReadable` to bring it
    /// up. Each is handed on once, and again after [`INFLIGHT`] while the
    /// log does not show it.
    pub fn due(&mut self, debounce: Duration, now: Instant) -> Vec<Report> {
        self.handed
            .retain(|_, h| now.saturating_duration_since(h.1) < INFLIGHT);
        let mut out = Vec::new();
        for (&slot, &(up, since)) in &self.changing {
            if now.saturating_duration_since(since) < debounce
                || self.handed.get(&slot).is_some_and(|h| h.0 == up)
            {
                continue;
            }
            self.handed.insert(slot, (up, now));
            let slot = slot as usize;
            out.push(if up {
                Report::InputReadable { slot }
            } else {
                Report::InputPruned { slot }
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::node_id;

    fn own(readable: u64, unreadable: u64) -> Own {
        Own {
            readable,
            unreadable,
            ..Own::default()
        }
    }

    fn voters<'a>(owns: &[(NodeId, &'a Own)]) -> Vec<Voter<'a>> {
        (owns.iter())
            .map(|&(id, own)| Voter {
                id,
                silent: false,
                own: Some(own),
            })
            .collect()
    }

    /// A change is handed on once it held for the debounce, not again
    /// while handed and the log does not show it yet, again once
    /// [`INFLIGHT`] passed with the log still lacking it, and never once
    /// the log shows it.
    #[tokio::test(start_paused = true)]
    async fn a_change_is_handed_once_and_again_only_while_the_log_lacks_it() {
        const D: Duration = Duration::from_millis(50);
        let down = [Report::InputPruned { slot: 0 }];
        // Slot 0 lost by a majority.
        let period = |l: &mut Liveness, log_up: u64| l.period(0, 1, log_up, Instant::now());
        let mut l = Liveness::default();
        period(&mut l, u64::MAX);
        assert_eq!(l.due(D, Instant::now()), [], "within the debounce");
        tokio::time::advance(D).await;
        period(&mut l, u64::MAX);
        assert_eq!(l.due(D, Instant::now()), down, "held for the debounce");
        tokio::time::advance(INFLIGHT / 2).await;
        period(&mut l, u64::MAX);
        assert_eq!(l.due(D, Instant::now()), [], "handed, not in the log");
        tokio::time::advance(INFLIGHT / 2).await;
        period(&mut l, u64::MAX);
        assert_eq!(l.due(D, Instant::now()), down, "offered again");
        period(&mut l, !1);
        tokio::time::advance(2 * INFLIGHT).await;
        period(&mut l, !1);
        assert_eq!(l.due(D, Instant::now()), [], "the log shows it");
    }

    /// In a joint configuration each half needs its own majority, and the
    /// witness counts in no half: with the witness, two audio voters of
    /// three must agree.
    #[test]
    fn every_configuration_needs_a_majority_of_its_audio_voters() {
        let a = |o| node_id(Slot::Audio(o), 1 + u64::from(o));
        let w = node_id(Slot::Witness, 9);
        let (lost, reads) = (own(0, 1), own(1, 0));
        let old: BTreeSet<NodeId> = [a(0), a(1), w].into();
        let new: BTreeSet<NodeId> = [a(0), a(2), a(3)].into();
        let one_lost = voters(&[(a(0), &lost), (a(1), &reads), (a(2), &lost), (a(3), &lost)]);
        assert_eq!(majority(std::slice::from_ref(&new), &one_lost), (0, 1));
        assert_eq!(
            majority(&[old.clone(), new.clone()], &one_lost),
            (0, 0),
            "one of the old configuration's two audio voters"
        );
        let both = voters(&[(a(0), &lost), (a(1), &lost), (a(2), &lost)]);
        assert_eq!(majority(&[old, new], &both), (0, 1));
    }
}
