//! Withholding (plan 2.11, Withholding and divergence): the supervisor's
//! moves on its replica's blocks. Each other voter's statuses, direct or
//! relayed, are folded per block; a block fenced or not computed by its
//! fence deadline `T_k + D`, or held at its hold cap `T_k + D + w`, is
//! withheld; a held block commits through on evidence from every other
//! audio voter, per range of the block, and is withheld on a `committed`
//! report with another digest or on one naming this replica in
//! `onSilenceOf`. No leader is involved.

use std::collections::BTreeMap;

use samplez::blocks::{BlockStatus, Cause, Kind, Range, Standing};

use crate::contact::{Audio, Members, Timing};
use crate::entry::NodeId;
use crate::membership::{Slot, slot_of};
use crate::rank::silence_bit;
use crate::report::Own;

/// What one voter's reports said of its blocks.
#[derive(Debug, Default)]
struct Folded {
    /// By (output, block): the latest status, a final one kept.
    statuses: BTreeMap<(usize, u64), BlockStatus>,
    processed: Option<u64>,
    standing: Vec<Standing>,
}

/// Moves the supervisor made, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Blocks fenced, or not computed, at their fence deadline.
    pub fence_deadline: u64,
    /// Blocks held at their hold cap.
    pub hold_cap: u64,
    /// Blocks withheld on a `committed` report with another digest.
    pub other_digest: u64,
    /// Blocks withheld on a status naming this replica in `onSilenceOf`.
    pub silence: u64,
    /// Blocks committed through, and those whose evidence used a silence.
    pub through: u64,
    pub through_on_silence: u64,
    /// Withdrawals on a status naming this replica in `onSilenceOf`.
    pub withdrawals: u64,
}

/// The supervisor's view of the other voters' blocks.
#[derive(Debug, Default)]
pub(crate) struct Withhold {
    peers: BTreeMap<NodeId, Folded>,
    /// (holder, held): the latest TAI time, by the holder's clock, of a
    /// report in which the holder held `held` silent.
    silent: BTreeMap<(NodeId, NodeId), u64>,
    /// The first block whose hold cap has not passed.
    next: Option<u64>,
    /// Blocks of this replica named in `onSilenceOf`, and whether the
    /// naming was acted on.
    named: BTreeMap<(usize, u64), bool>,
    pub counts: Counts,
}

/// What one voter's statuses say of one range of a held block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Evidence: it commits none of the range other than this replica.
    Evidence,
    /// It holds the range open and is silent about the block.
    Silent,
    /// It may still commit the range.
    Wait,
    /// It committed the range with another digest.
    Withhold,
}

/// Equal digests: both present and equal. A status without digest equals
/// no digest, the empty one included.
fn same_digest(a: Option<u64>, b: Option<u64>) -> bool {
    a.is_some() && a == b
}

/// The samples `[start, end)` of block `k` a status covers, and the range
/// below or above a split, which is withheld.
fn pieces(s: &BlockStatus, block: u64) -> [(u64, u64, bool); 2] {
    let (start, end) = (s.k * block, (s.k + 1) * block);
    match s.range {
        Range::Whole => [(start, end, true), (end, end, false)],
        Range::From(p) => [(start, p, false), (p, end, true)],
        Range::UpTo(p) => [(start, p, true), (p, end, false)],
    }
}

/// The samples of block `k` a status of this replica covers.
fn range_of(s: &BlockStatus, block: u64) -> (u64, u64) {
    let [a, b] = pieces(s, block);
    if a.2 { (a.0, a.1) } else { (b.0, b.1) }
}

impl Withhold {
    /// Takes the newest copy of `node`'s report of itself; `me` is this
    /// replica.
    pub fn fold(&mut self, me: NodeId, node: NodeId, own: &Own) {
        let my_bit = silence_bit(me).unwrap_or(0);
        let f = self.peers.entry(node).or_default();
        f.processed = own.processed;
        f.standing.clone_from(&own.standing);
        for (o, runs) in own.statuses.iter().enumerate() {
            for run in runs {
                for k in run.status.k..run.status.k + u64::from(run.count) {
                    let s = BlockStatus { k, ..run.status };
                    if s.kind == Kind::Committed && s.on_silence_of & my_bit != 0 {
                        self.named.entry((o, k)).or_insert(false);
                    }
                    let slot = f.statuses.entry((o, k)).or_insert(s);
                    let final_ = matches!(slot.kind, Kind::Committed | Kind::Withheld);
                    if !final_ {
                        *slot = s;
                    }
                }
            }
        }
    }

    /// A report `holder` sent at its TAI time `sent_at` held `held`
    /// silent.
    pub fn held_silent(&mut self, holder: NodeId, held: NodeId, sent_at: u64) {
        let t = self.silent.entry((holder, held)).or_default();
        *t = (*t).max(sent_at);
    }

    /// Forgets the voters no longer in `members`.
    pub fn retain(&mut self, members: &Members) {
        let voters = members.voters();
        self.peers.retain(|id, _| voters.contains(id));
        self.silent
            .retain(|(a, b), _| voters.contains(a) && voters.contains(b));
    }

    /// Whether `x` is silent about block `k` for this replica `me`: a
    /// majority of voters, `me` included, held it silent in reports
    /// generated at or after `T_k` by the sender's clock.
    fn silent(
        &self,
        x: NodeId,
        k: u64,
        me: NodeId,
        mine: &dyn Fn(NodeId) -> Option<u64>,
        members: &Members,
        timing: &Timing,
    ) -> bool {
        let tk = timing.block_deadline(k);
        members.majority(|v| {
            let at = if v == me {
                mine(x)
            } else {
                self.silent.get(&(v, x)).copied()
            };
            v != x && at.is_some_and(|t| t >= tk)
        })
    }

    /// What voter `x` says of `[a, b)` of this replica's held block `mine`
    /// of output `o`.
    fn verdict(
        &self,
        x: NodeId,
        o: usize,
        mine: &BlockStatus,
        (a, b): (u64, u64),
        silent: bool,
        block: u64,
    ) -> Verdict {
        let k = mine.k;
        let Some(f) = self.peers.get(&x) else {
            return if silent {
                Verdict::Silent
            } else {
                Verdict::Wait
            };
        };
        let Some(s) = f.statuses.get(&(o, k)) else {
            // Standing `withdrawn` stands for `withheld` up to its
            // processed index, and is no evidence beyond.
            let standing = match f.standing.get(o) {
                Some(Standing::Idle { since } | Standing::Withdrawn { since }) => Some(*since),
                _ => None,
            };
            if standing.is_some_and(|since| since <= k) && f.processed.is_some_and(|p| k <= p) {
                return Verdict::Evidence;
            }
            return if silent {
                Verdict::Silent
            } else {
                Verdict::Wait
            };
        };
        let mut v = Verdict::Evidence;
        for (start, end, own) in pieces(s, block) {
            if start.max(a) >= end.min(b) {
                continue;
            }
            // Withheld, held at a lower rank, or with this replica's
            // digest.
            let evidence = !own
                || s.kind == Kind::Withheld
                || (s.kind == Kind::Held && s.rank > mine.rank)
                || (matches!(s.kind, Kind::Held | Kind::Committed)
                    && same_digest(s.digest, mine.digest));
            let here = if evidence {
                Verdict::Evidence
            } else if s.kind == Kind::Committed {
                Verdict::Withhold
            } else if silent {
                Verdict::Silent
            } else {
                Verdict::Wait
            };
            v = match (v, here) {
                (Verdict::Withhold, _) | (_, Verdict::Withhold) => Verdict::Withhold,
                (Verdict::Wait, _) | (_, Verdict::Wait) => Verdict::Wait,
                (Verdict::Silent, _) | (_, Verdict::Silent) => Verdict::Silent,
                _ => Verdict::Evidence,
            };
        }
        v
    }

    /// One pass over this replica's blocks whose hold cap has not passed,
    /// as of TAI time `tai`.
    #[allow(clippy::too_many_arguments)]
    pub fn decide(
        &mut self,
        me: NodeId,
        own: &Own,
        mine: &dyn Fn(NodeId) -> Option<u64>,
        members: &Members,
        timing: &Timing,
        audio: &dyn Audio,
        tai: u64,
    ) {
        let t = timing;
        let b = t.block;
        let d = t.hold.as_nanos() as u64;
        let w = t.contact_window.as_nanos() as u64;
        let ring = crate::contact::index_at(d + w).div_ceil(b) + 2;
        let due = crate::contact::index_at(tai).saturating_sub(t.margin) / b;
        // Blocks further back are past their hold cap, their status slots
        // reused.
        let first = self.next.unwrap_or(0).max(due.saturating_sub(2 * ring + 4));
        // The last block whose fence deadline passed, or the last one
        // computed: past either, nothing is open.
        let mut last = first;
        while t.block_deadline(last + 1) + d <= tai {
            last += 1;
        }
        let last = last.max(own.processed.unwrap_or(0));
        let audio_voters: Vec<NodeId> = members
            .voters()
            .into_iter()
            .filter(|&v| v != me && slot_of(v) != Slot::Witness)
            .collect();

        // Statuses naming this replica in `onSilenceOf`.
        let named: Vec<(usize, u64)> = self
            .named
            .iter_mut()
            .filter(|(_, acted)| !**acted)
            .map(|(&ok, acted)| {
                *acted = true;
                ok
            })
            .collect();
        for (o, k) in named {
            // An older one, relayed late, is ignored.
            if let Some(Standing::Committing { from }) = own.standing.get(o)
                && (k + 1) * b > *from
            {
                audio.withdraw();
                self.counts.withdrawals += 1;
            }
            if audio
                .status(o, k)
                .is_some_and(|s| matches!(s.kind, Kind::Fenced | Kind::Held))
                && audio
                    .withhold(o, k, Cause::Silence)
                    .is_some_and(|s| s.kind == Kind::Withheld)
            {
                self.counts.silence += 1;
            }
        }

        for k in first..=last {
            let fence_passed = t.block_deadline(k) + d <= tai;
            let cap_passed = t.block_deadline(k) + d + w <= tai;
            for (o, standing) in own.standing.iter().enumerate() {
                let Some(s) = audio.status(o, k) else {
                    // Not computed by its fence deadline.
                    if fence_passed
                        && !matches!(standing, Standing::Idle { .. })
                        && audio
                            .withhold(o, k, Cause::FenceDeadline)
                            .is_some_and(|s| s.kind == Kind::Withheld)
                    {
                        self.counts.fence_deadline += 1;
                    }
                    continue;
                };
                match s.kind {
                    Kind::Fenced if fence_passed => {
                        if audio
                            .withhold(o, k, Cause::FenceDeadline)
                            .is_some_and(|s| s.kind == Kind::Withheld)
                        {
                            self.counts.fence_deadline += 1;
                        }
                    }
                    Kind::Held if cap_passed => {
                        if audio
                            .withhold(o, k, Cause::HoldCap)
                            .is_some_and(|s| s.kind == Kind::Withheld)
                        {
                            self.counts.hold_cap += 1;
                        }
                    }
                    Kind::Held if !s.through => {
                        self.evidence(me, o, &s, &audio_voters, mine, members, timing, audio);
                    }
                    _ => {}
                }
            }
        }
        let mut next = first;
        while next <= last && t.block_deadline(next) + d + w <= tai {
            next += 1;
        }
        self.next = Some(next);
        let floor = next.saturating_sub(2 * ring);
        for f in self.peers.values_mut() {
            f.statuses.retain(|&(_, k), _| k >= floor);
        }
        self.named.retain(|&(_, k), _| k >= floor);
    }

    /// Commits the held block `s` of output `o` through when every other
    /// audio voter gives evidence for it, per range; withholds it on a
    /// `committed` report with another digest.
    #[allow(clippy::too_many_arguments)]
    fn evidence(
        &mut self,
        me: NodeId,
        o: usize,
        s: &BlockStatus,
        audio_voters: &[NodeId],
        mine: &dyn Fn(NodeId) -> Option<u64>,
        members: &Members,
        timing: &Timing,
        audio: &dyn Audio,
    ) {
        let b = timing.block;
        let (a, z) = range_of(s, b);
        // Ranges as other voters' splits cut it: a block holds at most one
        // page boundary.
        let mut cut = None;
        for x in audio_voters {
            if let Some(xs) = self.peers.get(x).and_then(|f| f.statuses.get(&(o, s.k)))
                && let Range::From(p) | Range::UpTo(p) = xs.range
                && a < p
                && p < z
            {
                cut = Some(p);
            }
        }
        let ranges: Vec<(u64, u64)> = match cut {
            Some(p) => vec![(a, p), (p, z)],
            None => vec![(a, z)],
        };
        let silent: Vec<bool> = audio_voters
            .iter()
            .map(|&x| self.silent(x, s.k, me, mine, members, timing))
            .collect();
        // Per range: the verdict over every other audio voter, and the
        // voters whose silence it uses.
        let mut verdicts = Vec::with_capacity(ranges.len());
        for &r in &ranges {
            let mut v = Verdict::Evidence;
            let mut bits = 0u8;
            for (&x, &silent) in audio_voters.iter().zip(&silent) {
                let here = self.verdict(x, o, s, r, silent, b);
                if here == Verdict::Silent {
                    bits |= silence_bit(x).unwrap_or(0);
                }
                v = match (v, here) {
                    (Verdict::Withhold, _) | (_, Verdict::Withhold) => Verdict::Withhold,
                    (Verdict::Wait, _) | (_, Verdict::Wait) => Verdict::Wait,
                    _ => Verdict::Evidence,
                };
            }
            verdicts.push((v, bits));
        }
        let (through, up_to) = match verdicts[..] {
            [(Verdict::Withhold, _), ..] => {
                if audio
                    .withhold(o, s.k, Cause::OtherDigest)
                    .is_some_and(|s| s.kind == Kind::Withheld)
                {
                    self.counts.other_digest += 1;
                }
                return;
            }
            [(Verdict::Evidence, b0)] => (b0, None),
            [(Verdict::Evidence, b0), (Verdict::Evidence, b1)] => (b0 | b1, None),
            // Committed through below the other's split, withheld above.
            [(Verdict::Evidence, b0), (Verdict::Withhold, _)] => (b0, Some(ranges[1].0)),
            _ => return,
        };
        if !audio.commit_through(o, s.k, through, up_to) {
            return;
        }
        self.counts.through += 1;
        self.counts.through_on_silence += u64::from(through != 0);
        // The engine withdraws the replica as it commits the block up to P.
        if up_to.is_some() {
            self.counts.other_digest += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use samplez::blocks::{HEALTHY, Ranks};

    use super::*;
    use crate::contact::Sample;
    use crate::entry::Request;
    use crate::membership::node_id;
    use crate::report::runs;

    const B: u64 = 48;

    /// Records the supervisor's calls; holds no block.
    #[derive(Default)]
    struct Calls(Mutex<Vec<String>>);

    impl Audio for Calls {
        fn sample(&self) -> Sample {
            Sample::default()
        }
        fn fence(&self, _: u64) {}
        fn settled(&self, _: u64) -> bool {
            false
        }
        fn appended(&self, _: u64, _: &Request) {}
        fn truncated(&self, _: u64) {}
        fn committed(&self, _: u64) {}
        fn unresolved(&self, _: u64, _: u64) -> bool {
            true
        }
        fn skip(&self, _: u64) {}
        fn overwhelmed(&self) {}
        fn status(&self, _: usize, _: u64) -> Option<BlockStatus> {
            None
        }
        fn withhold(&self, _: usize, _: u64, _: Cause) -> Option<BlockStatus> {
            None
        }
        fn commit_through(&self, _: usize, _: u64, _: u8, _: Option<u64>) -> bool {
            false
        }
        fn withdraw(&self) {
            self.0.lock().unwrap().push("withdraw".into());
        }
        fn ranks(&self, _: Ranks) {}
    }

    fn a(o: u16) -> NodeId {
        node_id(Slot::Audio(o), 10 + u64::from(o))
    }

    fn members() -> Members {
        Members {
            configs: vec![[a(0), a(1), node_id(Slot::Witness, 3)].into()],
            addrs: BTreeMap::new(),
        }
    }

    fn committed(k: u64, on_silence_of: u8) -> BlockStatus {
        BlockStatus {
            k,
            kind: Kind::Committed,
            range: Range::Whole,
            through: true,
            rank: 0,
            cause: Cause::None,
            on_silence_of,
            digest: Some(HEALTHY),
        }
    }

    /// A status naming this replica in `onSilenceOf` withdraws it for a
    /// block at or after its last commit start; an older one, relayed
    /// late after it resumed, is ignored.
    #[test]
    fn only_a_silence_naming_after_the_commit_start_withdraws() {
        let t = Timing::defaults(B);
        let k0 = 1_000_000;
        let me = a(1);
        let p = (k0 + 5) * B + 16;
        let own = Own {
            processed: Some(k0 + 30),
            standing: vec![Standing::Committing { from: p }],
            ..Own::default()
        };
        let tai = t.block_deadline(k0 + 10);
        let peer = |k| Own {
            processed: Some(k0 + 30),
            statuses: vec![runs([Some(committed(k, 0b10))])],
            ..Own::default()
        };
        let none = |_: NodeId| None;
        let calls = Calls::default();
        let mut w = Withhold::default();
        w.fold(me, a(0), &peer(k0 + 4));
        w.decide(me, &own, &none, &members(), &t, &calls, tai);
        assert!(
            calls.0.lock().unwrap().is_empty(),
            "an older naming withdrew"
        );
        w.fold(me, a(0), &peer(k0 + 5));
        w.decide(me, &own, &none, &members(), &t, &calls, tai);
        assert_eq!(*calls.0.lock().unwrap(), ["withdraw"]);
        // Acted on once.
        w.decide(me, &own, &none, &members(), &t, &calls, tai);
        assert_eq!(calls.0.lock().unwrap().len(), 1);
    }

    /// Per range: `committed from P` is `withheld` below P; a held status
    /// at a lower rank, or one with this replica's digest, is evidence; a
    /// fill (no digest) equals no digest; a fenced one or none is not,
    /// unless its voter is silent.
    #[test]
    fn a_status_is_evidence_only_for_the_samples_it_covers() {
        let mut w = Withhold::default();
        let k = 1000;
        let p = k * B + 16;
        let mine = BlockStatus {
            kind: Kind::Held,
            through: false,
            digest: Some(7),
            rank: 1,
            ..committed(k, 0)
        };
        let fold = |w: &mut Withhold, s: BlockStatus| {
            w.peers.clear();
            w.fold(
                a(1),
                a(0),
                &Own {
                    statuses: vec![runs([Some(s)])],
                    ..Own::default()
                },
            );
        };
        let whole = (k * B, (k + 1) * B);
        let v = |w: &Withhold, r, silent| w.verdict(a(0), 0, &mine, r, silent, B);
        fold(
            &mut w,
            BlockStatus {
                range: Range::From(p),
                ..committed(k, 0)
            },
        );
        assert_eq!(v(&w, (k * B, p), false), Verdict::Evidence);
        assert_eq!(v(&w, (p, (k + 1) * B), false), Verdict::Withhold);
        let held = |rank, digest| BlockStatus {
            kind: Kind::Held,
            rank,
            digest,
            ..committed(k, 0)
        };
        fold(&mut w, held(2, Some(9)));
        assert_eq!(v(&w, whole, false), Verdict::Evidence, "lower rank");
        fold(&mut w, held(0, Some(7)));
        assert_eq!(v(&w, whole, false), Verdict::Evidence, "equal digest");
        fold(&mut w, held(0, Some(9)));
        assert_eq!(v(&w, whole, false), Verdict::Wait);
        assert_eq!(v(&w, whole, true), Verdict::Silent);
        let fill = BlockStatus {
            digest: None,
            ..mine
        };
        let w2 = {
            let mut w = Withhold::default();
            fold(&mut w, held(0, None));
            w
        };
        assert_eq!(
            w2.verdict(a(0), 0, &fill, whole, false, B),
            Verdict::Wait,
            "fills never match"
        );
        fold(
            &mut w,
            BlockStatus {
                kind: Kind::Fenced,
                ..held(2, Some(7))
            },
        );
        assert_eq!(v(&w, whole, false), Verdict::Wait, "fenced");
        // No status: standing withdrawn counts up to its processed index.
        w.peers.clear();
        w.fold(
            a(1),
            a(0),
            &Own {
                processed: Some(k),
                standing: vec![Standing::Withdrawn { since: k - 5 }],
                ..Own::default()
            },
        );
        assert_eq!(v(&w, whole, false), Verdict::Evidence);
        w.peers.get_mut(&a(0)).unwrap().processed = Some(k - 1);
        assert_eq!(
            v(&w, whole, false),
            Verdict::Wait,
            "beyond its processed index"
        );
    }

    /// Silence for k: a majority, this replica included, held the voter
    /// silent in reports generated at or after `T_k`.
    #[test]
    fn silence_counts_only_from_reports_at_or_after_the_block_deadline() {
        let t = Timing::defaults(B);
        let k = 1000;
        let tk = t.block_deadline(k);
        let m = Members {
            configs: vec![[a(0), a(1), a(2)].into()],
            addrs: BTreeMap::new(),
        };
        let mut w = Withhold::default();
        let mine_late = |_: NodeId| Some(tk);
        let mine_early = |_: NodeId| Some(tk - 1);
        assert!(!w.silent(a(0), k, a(1), &mine_late, &m, &t), "one of three");
        w.held_silent(a(2), a(0), tk - 1);
        assert!(
            !w.silent(a(0), k, a(1), &mine_late, &m, &t),
            "a2 before T_k"
        );
        w.held_silent(a(2), a(0), tk + 5);
        assert!(w.silent(a(0), k, a(1), &mine_late, &m, &t));
        assert!(!w.silent(a(0), k, a(1), &mine_early, &m, &t));
    }
}
