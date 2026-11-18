//! A replica's state (plan 2.11, Replica states) and the rules that move
//! it without a leader: leaving `starting` once ready, the Named rule,
//! Resume on page hashes per output, the per-output rule, and the commit
//! start each of them names. The leader's part is in [`crate::baseline`].
//!
//! Every commit start is named to the engine again each report period
//! until the output takes it, since a withdrawal may drop it, with the
//! processed block the decision saw: the engine ignores one decided
//! before a block it withheld for a cause since. One a later decision
//! takes back is cancelled before anything else is named.

use std::collections::BTreeMap;

use samplez::blocks::{Kind, PAGE, Range, Standing};
use serde::{Deserialize, Serialize};

use crate::contact::{Audio, Timing};
use crate::entry::{Baseline, NodeId};
use crate::report::{Own, Page};

/// A replica's state; the witness has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Not ready yet: it commits nothing.
    Starting,
    Committing,
    /// Through only: it fills a stop, from a page boundary after a block
    /// it committed.
    Withdrawn,
    /// Its output no longer follows the log or its peers: as withdrawn,
    /// never resuming on a hash match.
    Diverged,
    /// Its compatibility key differs from the baseline's and it is no
    /// old-key filler: it computes and commits nothing.
    VersionMismatch,
    /// Diverged, replaying a donor's core to hand it over; after the
    /// hand-over it resumes on a hash match.
    Resync,
}

impl ReplicaState {
    /// Demoted (plan 2.11, Baseline): ranks below every replica that is
    /// not.
    pub fn demoted(self) -> bool {
        matches!(
            self,
            ReplicaState::Withdrawn | ReplicaState::Diverged | ReplicaState::Resync
        )
    }

    /// Diverged or resyncing: ranked as diverged, no Resume before a
    /// hand-over, and each fill is a succession discontinuity.
    pub fn diverged(self) -> bool {
        matches!(self, ReplicaState::Diverged | ReplicaState::Resync)
    }
}

/// Pages kept per voter and output, newest first kept.
pub(crate) const PAGES_KEPT: usize = 64;

/// The page hashes every voter reported, by voter, output and page.
#[derive(Debug, Default)]
pub(crate) struct PageBook {
    pages: BTreeMap<(NodeId, u32), BTreeMap<u64, Page>>,
}

impl PageBook {
    /// Takes `node`'s page records.
    pub fn fold(&mut self, node: NodeId, pages: &[Page]) {
        for p in pages {
            let held = self.pages.entry((node, p.output)).or_default();
            held.insert(p.page, *p);
            while held.len() > PAGES_KEPT {
                held.pop_first();
            }
        }
    }

    pub fn of(&self, node: NodeId, output: u32) -> Option<&BTreeMap<u64, Page>> {
        self.pages.get(&(node, output))
    }

    pub fn get(&self, node: NodeId, output: u32, page: u64) -> Option<&Page> {
        self.of(node, output)?.get(&page)
    }

    /// The newest page `node` reported of `output`.
    pub fn newest(&self, node: NodeId, output: u32) -> Option<u64> {
        self.of(node, output)?.last_key_value().map(|(&q, _)| q)
    }

    /// Forgets the voters `keep` does not hold.
    pub fn retain(&mut self, keep: impl Fn(NodeId) -> bool) {
        self.pages.retain(|&(node, _), _| keep(node));
    }

    /// The pages of `output` some voter reported, in order.
    pub fn pages(&self, output: u32) -> std::collections::BTreeSet<u64> {
        self.pages
            .iter()
            .filter(|((_, o), _)| *o == output)
            .flat_map(|(_, held)| held.keys().copied())
            .collect()
    }

    pub fn outputs(&self) -> std::collections::BTreeSet<u32> {
        self.pages.keys().map(|&(_, o)| o).collect()
    }
}

/// The latest block of output `o` `own` reported withheld for a cause
/// ([`samplez::blocks::BlockStatus::for_a_cause`]), Resume's reference;
/// an idle output gives no status, so its blocks say nothing.
pub fn last_withheld(own: &Own, o: usize) -> Option<u64> {
    if matches!(own.standing.get(o), None | Some(Standing::Idle { .. })) {
        return None;
    }
    own.statuses.get(o)?.iter().rev().find_map(|run| {
        run.status
            .for_a_cause()
            .then(|| run.status.k + u64::from(run.count) - 1)
    })
}

/// Whether `own` reported a block at or after `k` withheld for a cause.
pub fn withheld_from(own: &Own, k: u64) -> bool {
    (0..own.standing.len()).any(|o| last_withheld(own, o).is_some_and(|w| w >= k))
}

/// Whether `own` reported a fill committed: a withdrawn replica filling
/// a stop.
pub fn filling(own: &Own) -> bool {
    own.state.is_some_and(ReplicaState::demoted)
        && own
            .statuses
            .iter()
            .flatten()
            .any(|run| run.status.kind == Kind::Committed && run.status.digest.is_none())
}

/// Whether two compatibility keys are known and differ.
pub fn differs(a: Option<u64>, b: Option<u64>) -> bool {
    matches!((a, b), (Some(a), Some(b)) if a != b)
}

/// A peer as this replica's rules see it: its newest copy, direct or
/// relayed.
pub(crate) struct Seen<'a> {
    pub id: NodeId,
    pub silent: bool,
    pub own: &'a Own,
}

/// What a replica's rules read in one report period.
pub(crate) struct View<'a> {
    pub me: NodeId,
    pub own: &'a Own,
    /// Per output, realised.
    pub realised: &'a [bool],
    /// Ready (plan 2.11, Replica states).
    pub ready: bool,
    /// The only voter: it commits once ready.
    pub alone: bool,
    /// The latest `Baseline` applied.
    pub baseline: Option<&'a Baseline>,
    /// This replica's compatibility key ([`crate::rollout`]).
    pub key: Option<u64>,
    /// Retargets the engine's core from product state, with the entries
    /// still unresolved given to it again; false to retry. The engine
    /// keeps the generations built ([`samplez::Engine::install_product`]).
    pub retarget: &'a dyn Fn() -> bool,
    /// Excluded by a `ClockVerdict`.
    pub excluded: bool,
    /// The other audio voters.
    pub peers: Vec<Seen<'a>>,
    /// The other audio voters ranked above this replica now.
    pub above: Vec<NodeId>,
    pub pages: &'a PageBook,
    pub timing: &'a Timing,
    pub tai: u64,
}

/// Why a commit start was named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Why {
    /// The Named rule under the `Baseline` whose P this is.
    Named(u64),
    Resume,
    PerOutput,
    /// The only voter.
    Alone,
}

#[derive(Debug, Clone, Copy)]
struct Start {
    min_k: u64,
    why: Why,
    /// The end of the last commit of the output when it was named.
    end_before: Option<u64>,
    /// The replica filled a stop when it was named.
    filling: bool,
    /// Resume's reference for the output when it was named.
    reference: u64,
}

/// The replica's moves, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Metrics {
    pub state: Option<ReplicaState>,
    /// Commits on the replica's own state where another's were followed:
    /// a named replica that was not filling taking its commit start, and
    /// each fill of a diverged replica.
    pub succession_discontinuity_total: u64,
    /// Samples a local member skipped between its last commit and a
    /// commit start after it, which libmxl zero-filled.
    pub resume_gap_samples: u64,
    /// Time spent withdrawn or diverged.
    pub withdrawn_seconds_total: f64,
    /// Outputs resumed on a page-hash match, and mismatches.
    pub resumes: u64,
    pub divergences: u64,
    /// Content discontinuities of a key hand-over: a commit start under
    /// a `Baseline` whose key replaced another, and each fill of an
    /// old-key filler.
    pub version_switch_total: u64,
}

/// One audio replica's state and the commit starts it named.
pub(crate) struct Life {
    state: ReplicaState,
    /// The block processed when it became ready.
    ready_at: Option<u64>,
    /// Per output: the last block withheld for a cause.
    withheld: Vec<Option<u64>>,
    /// Per output: the commit start named and not yet taken.
    starts: Vec<Option<Start>>,
    /// Per output: the end of its last commit.
    committed_end: Vec<Option<u64>>,
    /// Per output: its latest commit start.
    last_start: Vec<Option<u64>>,
    /// The Named rule's plan under the latest `Baseline` naming this
    /// replica: its P, and the block to commit from once decided.
    named: Option<(u64, Option<u64>)>,
    /// The P under which the Named rule no longer applies: the replica
    /// withheld a block for a cause at or after it.
    spent: Option<u64>,
    /// The highest fill block counted, per output.
    fills_counted: Vec<u64>,
    /// The key of the latest `Baseline` seen, once one was.
    base_key: Option<Option<u64>>,
    /// An old-key filler from this P on: it fills through only, ranks
    /// below every replica with the baseline's key and never resumes.
    filler: Option<u64>,
    /// The commit start the Named rule takes next follows a key
    /// hand-over to this replica's key.
    switching: bool,
    /// Blocks up to here were withheld while the replica computed
    /// nothing (`version_mismatch`): they spend no naming.
    computed_from: Option<u64>,
    /// The highest old-key fill block counted, per output.
    switch_counted: Vec<u64>,
    /// The block after which a resync's state was handed over, while it
    /// waits for its first matched page.
    handed: Option<u64>,
    /// A Resume commit start took after the hand-over: one matched page.
    matched: bool,
    /// When the last period was taken, TAI ns.
    last_tai: Option<u64>,
    withdrawn_ns: u64,
    pub metrics: Metrics,
}

impl Life {
    pub fn new() -> Life {
        Life {
            state: ReplicaState::Starting,
            ready_at: None,
            withheld: Vec::new(),
            starts: Vec::new(),
            committed_end: Vec::new(),
            last_start: Vec::new(),
            named: None,
            spent: None,
            fills_counted: Vec::new(),
            base_key: None,
            filler: None,
            switching: false,
            computed_from: None,
            switch_counted: Vec::new(),
            handed: None,
            matched: false,
            last_tai: None,
            withdrawn_ns: 0,
            metrics: Metrics::default(),
        }
    }

    pub fn state(&self) -> ReplicaState {
        self.state
    }

    /// An old-key filler in a key hand-over (plan 2.11, Baseline).
    pub fn filler(&self) -> bool {
        self.filler.is_some()
    }

    /// One report period.
    pub fn step(&mut self, v: &View, audio: &dyn Audio) {
        let outputs = v.own.standing.len();
        self.withheld.resize(outputs, None);
        self.starts.resize(outputs, None);
        self.committed_end.resize(outputs, None);
        self.fills_counted.resize(outputs, 0);
        self.switch_counted.resize(outputs, 0);
        self.note_starts(v.own);
        let b = v.timing.block;
        if let Some(last) = self.last_tai
            && self.state.demoted()
        {
            self.withdrawn_ns += v.tai.saturating_sub(last);
        }
        self.last_tai = Some(v.tai);
        self.metrics.withdrawn_seconds_total = self.withdrawn_ns as f64 / 1e9;
        for o in 0..outputs {
            if let Some(w) = last_withheld(v.own, o)
                && self.computed_from.is_none_or(|f| w > f)
            {
                self.withheld[o] = self.withheld[o].max(Some(w));
            }
            for run in v.own.statuses.get(o).into_iter().flatten() {
                let s = run.status;
                if s.kind != Kind::Committed {
                    continue;
                }
                let last = s.k + u64::from(run.count) - 1;
                let end = match s.range {
                    Range::UpTo(p) if run.count == 1 => p,
                    _ => (last + 1) * b,
                };
                self.committed_end[o] = self.committed_end[o].max(Some(end));
                // Each fill of a diverged replica.
                if self.state.diverged() && s.digest.is_none() && last > self.fills_counted[o] {
                    let from = self.fills_counted[o].max(s.k.saturating_sub(1));
                    self.metrics.succession_discontinuity_total += last - from;
                    self.fills_counted[o] = last;
                }
                // Each fill of an old-key filler.
                if self.filler.is_some_and(|p| (last + 1) * b > p)
                    && s.digest.is_none()
                    && last > self.switch_counted[o]
                {
                    let from = self.switch_counted[o].max(s.k.saturating_sub(1));
                    self.metrics.version_switch_total += last - from;
                    self.switch_counted[o] = last;
                }
            }
        }
        // A replica held in `starting` stays there; it diverges once it
        // leaves. Excluded, a replica resyncing is diverged again: no
        // hand-over.
        if v.excluded && !matches!(self.state, ReplicaState::Diverged | ReplicaState::Starting) {
            self.diverge(audio);
        }
        self.taken(v);
        self.keys(v, audio);
        self.note_naming(v, audio);
        self.leave_starting(v);
        self.follow_standing(v);
        self.lapse(audio);
        self.named_rule(v, audio);
        if v.alone {
            self.alone(v);
        } else {
            for o in 0..outputs {
                self.resume(o, v, audio);
                self.per_output(o, v);
            }
        }
        // Named again until taken: a withdrawal may have dropped it.
        let seen = v.own.processed.unwrap_or(0);
        for (o, start) in self.starts.iter().enumerate() {
            if let Some(s) = start {
                audio.commit_from(o, s.min_k, seen);
            }
        }
        self.metrics.state = Some(self.state);
    }

    /// Each output's latest commit start.
    fn note_starts(&mut self, own: &Own) {
        self.last_start
            .resize(own.standing.len().max(self.last_start.len()), None);
        for (o, s) in own.standing.iter().enumerate() {
            if let Standing::Committing { from } = *s {
                self.last_start[o] = self.last_start[o].max(Some(from));
            }
        }
    }

    /// Cancels the Resume and per-output commit starts not taken that
    /// were decided on what no longer holds: a block withheld for a cause
    /// after Resume's reference, or the replica no longer committing.
    fn lapse(&mut self, audio: &dyn Audio) {
        for o in 0..self.starts.len() {
            let Some(s) = self.starts[o] else { continue };
            let lapsed = match s.why {
                Why::Resume => self.reference(o) != s.reference,
                Why::PerOutput => self.state != ReplicaState::Committing,
                Why::Named(_) | Why::Alone => false,
            };
            if lapsed {
                self.starts[o] = None;
                audio.cancel_commit_start(o);
            }
        }
    }

    /// The commit starts the outputs took.
    fn taken(&mut self, v: &View) {
        for o in 0..self.starts.len() {
            let Some(s) = self.starts[o] else { continue };
            let Some(Standing::Committing { from }) = v.own.standing.get(o).copied() else {
                continue;
            };
            if from < s.min_k * v.timing.block {
                continue;
            }
            self.starts[o] = None;
            if let Some(end) = s.end_before {
                self.metrics.resume_gap_samples += from.saturating_sub(end);
            }
            if matches!(s.why, Why::Named(_)) && self.switching && !s.filling {
                self.switching = false;
                self.metrics.version_switch_total += 1;
            }
            match s.why {
                Why::Named(_) if !s.filling => self.metrics.succession_discontinuity_total += 1,
                Why::Resume => {
                    self.metrics.resumes += 1;
                    self.matched |= self.handed.is_some();
                }
                _ => {}
            }
        }
    }

    fn leave_starting(&mut self, v: &View) {
        if self.state != ReplicaState::Starting || !v.ready {
            return;
        }
        let Some(p) = v.own.processed else { return };
        self.ready_at = Some(p);
        self.state = ReplicaState::Withdrawn;
        let named = v.baseline.is_some_and(|b| b.replica == v.me);
        if !named && differs(v.key, v.baseline.and_then(|b| b.key)) {
            self.state = ReplicaState::VersionMismatch;
        }
    }

    /// Committing while an output commits and none is withdrawn; withdrawn
    /// on the engine's withdrawal.
    fn follow_standing(&mut self, v: &View) {
        if matches!(
            self.state,
            ReplicaState::Starting | ReplicaState::VersionMismatch
        ) {
            return;
        }
        let realised = |o: usize| v.realised.get(o).copied().unwrap_or(true);
        let standing = || {
            v.own
                .standing
                .iter()
                .enumerate()
                .filter(|&(o, _)| realised(o))
        };
        let committing = standing().any(|(_, s)| matches!(s, Standing::Committing { .. }));
        let withdrawn = standing().any(|(_, s)| matches!(s, Standing::Withdrawn { .. }));
        match self.state {
            ReplicaState::Committing if withdrawn || !committing => {
                self.state = ReplicaState::Withdrawn
            }
            // Handed over: committing once a commit start Resume named
            // on a matched page took.
            ReplicaState::Resync if self.matched && committing && !withdrawn => {
                self.state = ReplicaState::Committing;
                self.handed = None;
                self.matched = false;
            }
            // A diverged replica commits again only by the Named rule,
            // under a naming it has not spent.
            ReplicaState::Diverged | ReplicaState::Resync
                if committing
                    && !withdrawn
                    && self.named.is_some_and(|(p, _)| {
                        self.spent != Some(p)
                            && v.own
                                .standing
                                .iter()
                                .any(|s| matches!(s, Standing::Committing { from } if *from >= p))
                    }) =>
            {
                self.state = ReplicaState::Committing;
                self.handed = None;
                self.matched = false;
            }
            ReplicaState::Withdrawn if committing && !withdrawn => {
                self.state = ReplicaState::Committing
            }
            _ => {}
        }
    }

    /// The Named rule: the latest applied `Baseline` names this replica,
    /// which is not committing and not excluded. It commits from P, the
    /// first page boundary at or after the entry's at_index, when it knew
    /// the entry before it processed the block holding P; else, or when
    /// named while starting, from its commit start once ready. A replica
    /// that withheld a block for a cause at or after P does not commit by
    /// this rule until a later `Baseline` names it.
    fn named_rule(&mut self, v: &View, audio: &dyn Audio) {
        let Some((p, _)) = self.named else { return };
        let kp = p / v.timing.block;
        if self.spent == Some(p) {
            return;
        }
        if self.withheld.iter().any(|&w| w.is_some_and(|w| w >= kp)) {
            self.spent = Some(p);
            self.cancel(audio, |why| why == Why::Named(p));
            return;
        }
        // Resyncing before its hand-over: a commit start taken between the
        // replay's go-ahead and the hand-over would commit the state the
        // hand-over then replaces.
        let replaying = self.state == ReplicaState::Resync && self.handed.is_none();
        if replaying
            || matches!(
                self.state,
                ReplicaState::Starting | ReplicaState::Committing
            )
        {
            return;
        }
        let Some(processed) = v.own.processed else {
            return;
        };
        // Named, a replica that computes nothing retargets its core from
        // product state first, and commits once it took it.
        if self.state == ReplicaState::VersionMismatch {
            // Refused, and retried, while a generation the core names is
            // not built: it never hands over without it.
            if !(v.retarget)() {
                return;
            }
            self.computed_from = Some(processed);
            self.state = ReplicaState::Withdrawn;
        }
        let min_k = match self.named {
            Some((_, Some(k))) => k,
            _ => {
                let k = processed + 1;
                self.named = Some((p, Some(k)));
                k
            }
        };
        let filling = filling(v.own);
        for o in 0..self.starts.len() {
            let committing = matches!(v.own.standing[o], Standing::Committing { .. });
            if committing || !v.realised.get(o).copied().unwrap_or(true) {
                continue;
            }
            if self.starts[o].is_some_and(|s| s.why == Why::Named(p)) {
                continue;
            }
            if self.starts[o].is_some() {
                audio.cancel_commit_start(o);
            }
            self.starts[o] = Some(Start {
                min_k,
                why: Why::Named(p),
                end_before: self.committed_end[o],
                filling,
                reference: self.reference(o),
            });
        }
    }

    /// The key hand-over (plan 2.11, Baseline). Once the latest `Baseline`
    /// carries another key than this replica's, a replica that had the
    /// previous baseline's key is an old-key filler from P: committing,
    /// it commits up to P (`committed up to P` on the block holding P);
    /// demoted and not excluded, it fills as before. It turns
    /// `version_mismatch` once a second replica with the baseline's key
    /// commits; any other replica is `version_mismatch` at once. Once the
    /// baseline's key is its own again it is no filler, and one in
    /// `version_mismatch` retargets its core and is withdrawn, its resume
    /// reference that moment.
    fn keys(&mut self, v: &View, audio: &dyn Audio) {
        let Some(b) = v.baseline else { return };
        let now = b.key;
        let before = self.base_key.replace(now);
        let mine = v.key;
        if !differs(mine, now) {
            // Back to its key before its stop at P took: the stop lapses
            // and the output goes on committing.
            if self.filler.take().is_some() {
                for (o, s) in v.own.standing.iter().enumerate() {
                    if matches!(s, Standing::Committing { .. }) {
                        audio.stop_at(o, u64::MAX);
                    }
                }
            }
            if before != Some(now) && b.replica == v.me && before.is_some_and(|k| differs(mine, k))
            {
                self.switching = true;
            }
            if self.state == ReplicaState::VersionMismatch && (v.retarget)() {
                self.computed_from = v.own.processed;
                self.ready_at = v.own.processed;
                self.state = ReplicaState::Withdrawn;
            }
            return;
        }
        if self.state == ReplicaState::Starting {
            return;
        }
        let handed_over = before.is_some_and(|k| k != now && !differs(mine, k));
        if handed_over && self.state != ReplicaState::VersionMismatch && !v.excluded {
            let p = b.at_index.next_multiple_of(PAGE);
            self.filler = Some(p);
            self.cancel(audio, |_| true);
            for (o, s) in v.own.standing.iter().enumerate() {
                if matches!(s, Standing::Committing { .. }) {
                    audio.stop_at(o, p);
                }
            }
            self.state = ReplicaState::Withdrawn;
            return;
        }
        // The baseline moved on from another key than its own: no filler.
        if before.is_some_and(|k| k != now && differs(mine, k)) {
            self.filler = None;
        }
        let second = v
            .peers
            .iter()
            .filter(|x| {
                !x.silent
                    && x.own.key.is_some()
                    && x.own.key == now
                    && x.own.state == Some(ReplicaState::Committing)
            })
            .count()
            >= 2;
        if self.state != ReplicaState::VersionMismatch && (self.filler.is_none() || second) {
            self.filler = None;
            self.state = ReplicaState::VersionMismatch;
            self.cancel(audio, |_| true);
            audio.withdraw();
            audio.drop_engine();
        }
    }

    /// The `Baseline` naming this replica, as first seen: one taken back
    /// cancels the commit starts it named.
    fn note_naming(&mut self, v: &View, audio: &dyn Audio) {
        let naming = v
            .baseline
            .filter(|bl| bl.replica == v.me && !v.excluded)
            .map(|bl| bl.at_index.next_multiple_of(PAGE));
        if self.named.is_some_and(|(p, _)| Some(p) != naming) {
            self.named = None;
            self.cancel(audio, |why| matches!(why, Why::Named(_)));
        }
        let Some(p) = naming else { return };
        // A naming spent by a divergence is not taken up again.
        if self.named.is_none() && self.spent != Some(p) {
            // Known before the block holding P was processed: from P.
            let kp = p / v.timing.block;
            let in_time =
                self.state != ReplicaState::Starting && v.own.processed.is_none_or(|k| k < kp);
            self.named = Some((p, in_time.then_some(kp)));
        }
    }

    /// Resume's reference for output `o`: its last block withheld for a
    /// cause, or the block processed when it became ready.
    fn reference(&self, o: usize) -> u64 {
        self.withheld[o]
            .max(self.ready_at)
            .max(self.handed)
            .unwrap_or(0)
    }

    /// One voter commits once ready.
    fn alone(&mut self, v: &View) {
        let Some(processed) = v.own.processed else {
            return;
        };
        if self.state == ReplicaState::Starting {
            return;
        }
        for o in 0..self.starts.len() {
            if self.starts[o].is_none()
                && !matches!(v.own.standing[o], Standing::Committing { .. })
                && v.realised.get(o).copied().unwrap_or(true)
            {
                self.starts[o] = Some(Start {
                    min_k: processed + 1,
                    why: Why::Alone,
                    end_before: None,
                    filling: false,
                    reference: self.reference(o),
                });
            }
        }
    }

    /// Resume, per output: the first page this replica computed wholly
    /// after its reference, against every committing replica that
    /// committed it (the next page when none did).
    fn resume(&mut self, o: usize, v: &View, audio: &dyn Audio) {
        let handed = self.state == ReplicaState::Resync && self.handed.is_some();
        if !(handed
            || matches!(
                self.state,
                ReplicaState::Withdrawn | ReplicaState::Committing
            ))
            || self.filler.is_some()
            || self.starts[o].is_some()
            || matches!(v.own.standing[o], Standing::Committing { .. })
            || !v.realised.get(o).copied().unwrap_or(true)
        {
            return;
        }
        let Some(processed) = v.own.processed else {
            return;
        };
        match self.compare(o, v) {
            Some(true) => {
                self.starts[o] = Some(Start {
                    min_k: processed + 1,
                    why: Why::Resume,
                    end_before: self.committed_end[o],
                    filling: false,
                    reference: self.reference(o),
                });
            }
            Some(false) => self.diverge(audio),
            None => {}
        }
    }

    /// Whether this replica's page matches the committing replicas',
    /// none until it can tell.
    fn compare(&self, o: usize, v: &View) -> Option<bool> {
        let b = v.timing.block;
        let first = ((self.reference(o) + 1) * b).div_ceil(PAGE);
        let committing: Vec<&Seen> = v
            .peers
            .iter()
            .filter(|p| !p.silent && p.own.state == Some(ReplicaState::Committing))
            .collect();
        if committing.is_empty() {
            return None;
        }
        let output = o as u32;
        for (&q, mine) in v.pages.of(v.me, output)?.range(first..) {
            let mut compared = false;
            for x in &committing {
                match v.pages.get(x.id, output, q) {
                    Some(r) if r.committed => {
                        if (r.hash, r.fingerprint) != (mine.hash, mine.fingerprint) {
                            return Some(false);
                        }
                        compared = true;
                    }
                    Some(_) => {}
                    // Not reported yet: wait, unless it reported later ones.
                    None if v.pages.newest(x.id, output).is_some_and(|n| n > q) => {}
                    None => return None,
                }
            }
            if compared {
                return Some(true);
            }
        }
        None
    }

    /// The per-output rule: no replica commits output `o`, which this
    /// committing replica has realised, and every replica ranked above it
    /// is silent or reports `o` standing withdrawn: it commits `o` from
    /// its commit start. No leader is involved.
    fn per_output(&mut self, o: usize, v: &View) {
        if self.state != ReplicaState::Committing
            || self.starts[o].is_some()
            || matches!(v.own.standing[o], Standing::Committing { .. })
            || !v.realised.get(o).copied().unwrap_or(true)
        {
            return;
        }
        let commits = |own: &Own| matches!(own.standing.get(o), Some(Standing::Committing { .. }));
        if v.peers.iter().any(|p| !p.silent && commits(p.own)) {
            return;
        }
        let ahead = v.above.iter().any(|&id| {
            v.peers.iter().any(|p| {
                p.id == id
                    && !p.silent
                    && !matches!(
                        p.own.standing.get(o),
                        None | Some(Standing::Withdrawn { .. } | Standing::Idle { .. })
                    )
            })
        });
        if ahead {
            return;
        }
        let Some(processed) = v.own.processed else {
            return;
        };
        self.starts[o] = Some(Start {
            min_k: processed + 1,
            why: Why::PerOutput,
            end_before: self.committed_end[o],
            filling: false,
            reference: self.reference(o),
        });
    }

    /// The leader's verdict on `page`, once known committed: diverged,
    /// unless the latest commit start of every realised output that
    /// committed lies after that page, committing now or not, or the
    /// replica is diverged already.
    pub fn verdict(&mut self, page: u64, own: &Own, realised: &[bool], audio: &dyn Audio) {
        if self.state.diverged()
            || matches!(
                self.state,
                ReplicaState::Starting | ReplicaState::VersionMismatch
            )
        {
            return;
        }
        self.note_starts(own);
        let end = (page + 1) * PAGE;
        let mut starts = self
            .last_start
            .iter()
            .enumerate()
            .filter(|&(o, _)| realised.get(o).copied().unwrap_or(true))
            .filter_map(|(_, s)| *s)
            .peekable();
        let stale = starts.peek().is_some() && starts.all(|from| from >= end);
        if stale {
            return;
        }
        self.diverge(audio);
    }

    /// Diverged at once: the replica applied an entry the log restamped
    /// or voided, so its state contradicts the log's decision.
    pub fn contradicted(&mut self, audio: &dyn Audio) {
        if matches!(
            self.state,
            ReplicaState::Diverged | ReplicaState::Starting | ReplicaState::VersionMismatch
        ) {
            return;
        }
        self.diverge(audio);
    }

    /// A donor's core arrived for a diverged replica: `resync`. False
    /// when the replica is no longer diverged.
    pub fn resyncing(&mut self) -> bool {
        if self.state != ReplicaState::Diverged {
            return false;
        }
        self.state = ReplicaState::Resync;
        self.handed = None;
        self.matched = false;
        true
    }

    /// The RT thread took the replayed state after block `j`: the replica
    /// resumes on the first page it computes wholly after `j` that
    /// matches the committing replicas'.
    pub fn handed_over(&mut self, j: u64) {
        if self.state == ReplicaState::Resync {
            self.handed = Some(j);
        }
    }

    /// The resync failed: diverged again, to retry with a newer core.
    pub fn resync_failed(&mut self) {
        if self.state == ReplicaState::Resync {
            self.state = ReplicaState::Diverged;
            self.handed = None;
            self.matched = false;
        }
    }

    /// Back to `starting` after a snapshot install or a purge-point
    /// verdict: whatever the running engine computed no longer follows
    /// the log, so nothing more of it is committed. The replica leaves
    /// `starting` once ready, with an engine rebuilt from product state.
    pub fn restart(&mut self, audio: &dyn Audio) {
        self.cancel(audio, |_| true);
        self.handed = None;
        self.matched = false;
        if let Some((p, _)) = self.named.take() {
            self.spent = Some(p);
        }
        self.ready_at = None;
        self.filler = None;
        // A replica still starting has committed nothing to withdraw.
        if std::mem::replace(&mut self.state, ReplicaState::Starting) != ReplicaState::Starting {
            audio.withdraw();
        }
    }

    fn diverge(&mut self, audio: &dyn Audio) {
        self.state = ReplicaState::Diverged;
        self.handed = None;
        self.matched = false;
        self.metrics.divergences += 1;
        self.cancel(audio, |_| true);
        // Committing again by the Named rule needs a later `Baseline`.
        if let Some((p, _)) = self.named.take() {
            self.spent = Some(p);
        }
        audio.withdraw();
    }

    fn cancel(&mut self, audio: &dyn Audio, which: impl Fn(Why) -> bool) {
        for (o, start) in self.starts.iter_mut().enumerate() {
            if start.is_some_and(|s| which(s.why)) {
                *start = None;
                audio.cancel_commit_start(o);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use samplez::blocks::{BlockStatus, Cause, Ranks};

    use super::*;
    use crate::contact::Sample;
    use crate::membership::{Slot, node_id};
    use crate::report::runs;

    const B: u64 = 48;

    /// Records the commit starts, cancels and withdrawals named.
    #[derive(Default)]
    struct Calls(Mutex<Vec<String>>);

    impl Calls {
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut self.0.lock().unwrap())
        }
    }

    impl Audio for Calls {
        fn sample(&self) -> Sample {
            Sample::default()
        }
        fn fence(&self, _: u64) {}
        fn settled(&self, _: u64) -> bool {
            false
        }
        fn appended(&self, _: u64, _: u64, _: &[samplez::Entry]) {}
        fn committed(&self, _: u64) {}
        fn unresolved(&self, _: u64, _: u64) -> bool {
            true
        }
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
        fn commit_from(&self, output: usize, min_k: u64, _: u64) {
            self.0
                .lock()
                .unwrap()
                .push(format!("from {output} {min_k}"));
        }
        fn cancel_commit_start(&self, output: usize) {
            self.0.lock().unwrap().push(format!("cancel {output}"));
        }
        fn stop_at(&self, output: usize, at: u64) {
            self.0.lock().unwrap().push(format!("stop {output} {at}"));
        }
        fn drop_engine(&self) {
            self.0.lock().unwrap().push("drop".into());
        }
        fn rebuild_engine(&self, _: &samplez::ProductState, _: u64) -> bool {
            self.0.lock().unwrap().push("rebuild".into());
            true
        }
    }

    fn me() -> NodeId {
        node_id(Slot::Audio(0), 1)
    }

    fn own(processed: u64, standing: Standing) -> Own {
        Own {
            processed: Some(processed),
            standing: vec![standing],
            statuses: vec![Vec::new()],
            ..Own::default()
        }
    }

    fn step(life: &mut Life, own: &Own, baseline: Option<&Baseline>, calls: &Calls) {
        let pages = PageBook::default();
        step_with(life, own, baseline, Vec::new(), &pages, calls);
    }

    fn step_with(
        life: &mut Life,
        own: &Own,
        baseline: Option<&Baseline>,
        peers: Vec<Seen>,
        pages: &PageBook,
        calls: &Calls,
    ) {
        let timing = Timing::defaults(B);
        let realised = vec![true; own.standing.len()];
        let v = View {
            me: me(),
            own,
            realised: &realised,
            ready: true,
            alone: false,
            baseline,
            key: None,
            retarget: &|| {
                let state = samplez::ProductState {
                    gains: Vec::new(),
                    up: Vec::new(),
                    flows: Vec::new(),
                    strips: Vec::new(),
                    ducks: Vec::new(),
                    masks: Vec::new(),
                    pending: Vec::new(),
                    last_id: 0,
                };
                calls.rebuild_engine(&state, 0)
            },
            excluded: false,
            above: peers.iter().map(|p| p.id).collect(),
            peers,
            pages,
            timing: &timing,
            tai: 0,
        };
        life.step(&v, calls);
    }

    fn peer() -> NodeId {
        node_id(Slot::Audio(1), 2)
    }

    fn page(q: u64, committed: bool) -> Page {
        Page {
            output: 0,
            page: q,
            hash: 1,
            fingerprint: 1,
            committed,
            degraded: false,
            digest: None,
        }
    }

    fn skipped(k: u64) -> BlockStatus {
        BlockStatus {
            k,
            kind: Kind::Withheld,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::Skip,
            on_silence_of: 0,
            digest: None,
        }
    }

    fn naming(at_index: u64) -> Baseline {
        Baseline {
            replica: me(),
            order: Vec::new(),
            at_index,
            key: None,
        }
    }

    const K: u64 = 1_000_000;

    /// Named before it processed the block holding P, a replica commits
    /// from P; named after it, from its commit start; the commit start is
    /// named again every period until the output takes it.
    #[test]
    fn the_named_rule_names_p_or_the_commit_start_until_it_takes() {
        let idle = Standing::Idle { since: 0 };
        let b = naming((K + 10) * B);
        let p = b.at_index.next_multiple_of(PAGE);
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &own(K, idle), None, &calls);
        assert_eq!(life.state(), ReplicaState::Withdrawn);
        step(&mut life, &own(K + 1, idle), Some(&b), &calls);
        step(&mut life, &own(K + 2, idle), Some(&b), &calls);
        let want = format!("from 0 {}", p / B);
        assert_eq!(calls.take(), [want.clone(), want]);
        step(
            &mut life,
            &own(p / B, Standing::Committing { from: p }),
            Some(&b),
            &calls,
        );
        assert!(calls.take().is_empty(), "taken: named no more");
        assert_eq!(life.state(), ReplicaState::Committing);
        assert_eq!(life.metrics.succession_discontinuity_total, 1);

        // Known late: from the block after the one it processed.
        let mut life = Life::new();
        step(&mut life, &own(K, idle), None, &calls);
        calls.take();
        step(&mut life, &own(p / B + 3, idle), Some(&b), &calls);
        assert_eq!(calls.take(), [format!("from 0 {}", p / B + 4)]);
    }

    /// A naming taken back by a later `Baseline` naming another replica
    /// cancels the commit start it named before anything else.
    #[test]
    fn a_naming_taken_back_cancels_its_commit_start() {
        let idle = Standing::Idle { since: 0 };
        let b = naming((K + 10) * B);
        let other = Baseline {
            replica: node_id(Slot::Audio(1), 2),
            ..b.clone()
        };
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &own(K, idle), None, &calls);
        step(&mut life, &own(K + 1, idle), Some(&b), &calls);
        calls.take();
        step(&mut life, &own(K + 2, idle), Some(&other), &calls);
        assert_eq!(calls.take(), ["cancel 0"]);
        step(&mut life, &own(K + 3, idle), Some(&other), &calls);
        assert!(calls.take().is_empty());
    }

    /// A replica that withheld a block for a cause at or after P commits
    /// by the Named rule no more under that `Baseline`, and a diverged one
    /// only under a later one; a fill withheld on another's commit is no
    /// cause.
    #[test]
    fn a_block_withheld_at_or_after_p_spends_the_naming() {
        let b = naming((K + 10) * B);
        let p = b.at_index.next_multiple_of(PAGE);
        let calls = Calls::default();
        let mut life = Life::new();
        let mut o = own(p / B + 5, Standing::Withdrawn { since: K });
        step(&mut life, &o, None, &calls);
        o.statuses[0] = runs([Some(BlockStatus {
            k: p / B + 4,
            kind: Kind::Withheld,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::OtherDigest,
            on_silence_of: 0,
            digest: None,
        })]);
        step(&mut life, &o, Some(&b), &calls);
        assert_eq!(calls.take().len(), 1, "a fill withheld is no cause");
        o.statuses[0] = runs([Some(BlockStatus {
            k: p / B + 4,
            kind: Kind::Withheld,
            range: Range::Whole,
            through: false,
            rank: 0,
            cause: Cause::Skip,
            on_silence_of: 0,
            digest: None,
        })]);
        step(&mut life, &o, Some(&b), &calls);
        assert_eq!(calls.take(), ["cancel 0"]);
        step(&mut life, &o, Some(&b), &calls);
        assert!(calls.take().is_empty());
    }

    /// A verdict on a page before the commit start is ignored; one on a
    /// later page diverges the replica once, withdrawing it.
    #[test]
    fn a_verdict_before_the_commit_start_is_ignored() {
        let p = 977 * PAGE;
        let committing = own(p / B + 50, Standing::Committing { from: p });
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &committing, None, &calls);
        assert_eq!(life.state(), ReplicaState::Committing);
        life.verdict(p / PAGE - 1, &committing, &[true], &calls);
        assert_eq!(life.state(), ReplicaState::Committing);
        life.verdict(p / PAGE, &committing, &[true], &calls);
        life.verdict(p / PAGE + 1, &committing, &[true], &calls);
        assert_eq!(life.state(), ReplicaState::Diverged);
        assert_eq!(calls.take(), ["withdraw"]);
        assert_eq!(life.metrics.divergences, 1);
    }

    /// A replica resyncing takes no commit start under a `Baseline`
    /// naming it until its replayed state is handed over: one taken before
    /// would commit the state the hand-over replaces.
    #[test]
    fn a_resyncing_replica_takes_no_naming_before_its_hand_over() {
        let idle = Standing::Idle { since: 0 };
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &own(K, idle), None, &calls);
        life.contradicted(&calls);
        assert!(life.resyncing());
        calls.take();
        let b = naming((K + 10) * B);
        step(&mut life, &own(K + 1, idle), Some(&b), &calls);
        step(&mut life, &own(K + 2, idle), Some(&b), &calls);
        assert!(
            !calls.take().iter().any(|c| c.starts_with("from")),
            "named while replaying"
        );
        life.handed_over(K + 2);
        step(&mut life, &own(K + 3, idle), Some(&b), &calls);
        assert!(
            calls.take().iter().any(|c| c.starts_with("from")),
            "named once handed over"
        );
        assert_eq!(life.state(), ReplicaState::Resync);
    }

    /// A named replica diverged by a verdict stays diverged under the
    /// `Baseline` that named it, though its output still shows the
    /// commit start it took under it until the engine withdraws it.
    #[test]
    fn a_diverged_named_baseline_stays_diverged() {
        let idle = Standing::Idle { since: 0 };
        let b = naming((K + 10) * B);
        let p = b.at_index.next_multiple_of(PAGE);
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &own(K, idle), None, &calls);
        step(&mut life, &own(K + 1, idle), Some(&b), &calls);
        let committing = own(p / B + 50, Standing::Committing { from: p });
        step(&mut life, &committing, Some(&b), &calls);
        assert_eq!(life.state(), ReplicaState::Committing);
        life.verdict(p / PAGE, &committing, &[true], &calls);
        assert_eq!(life.state(), ReplicaState::Diverged);
        calls.take();
        step(&mut life, &committing, Some(&b), &calls);
        assert_eq!(life.state(), ReplicaState::Diverged);
        let withdrawn = own(p / B + 51, Standing::Withdrawn { since: p / B + 51 });
        step(&mut life, &withdrawn, Some(&b), &calls);
        assert_eq!(life.state(), ReplicaState::Diverged);
        assert!(calls.take().is_empty(), "no commit start under it again");
    }

    /// A per-output commit start not taken yet is cancelled once the
    /// replica no longer commits: a withdrawn replica commits no output
    /// it named while committing.
    #[test]
    fn a_per_output_start_does_not_outlive_a_withdrawal() {
        let idle = Standing::Idle { since: 0 };
        let two = |processed, first| Own {
            processed: Some(processed),
            standing: vec![first, idle],
            statuses: vec![Vec::new(), Vec::new()],
            ..Own::default()
        };
        let calls = Calls::default();
        let mut life = Life::new();
        step(
            &mut life,
            &two(K, Standing::Committing { from: 0 }),
            None,
            &calls,
        );
        assert_eq!(life.state(), ReplicaState::Committing);
        assert_eq!(calls.take(), [format!("from 1 {}", K + 1)]);
        step(
            &mut life,
            &two(K + 1, Standing::Withdrawn { since: K + 1 }),
            None,
            &calls,
        );
        assert_eq!(life.state(), ReplicaState::Withdrawn);
        assert_eq!(calls.take(), ["cancel 1"]);
    }

    /// A Resume commit start not taken yet is cancelled once a block is
    /// withheld for a cause after the reference it was decided on.
    #[test]
    fn a_resume_start_lapses_on_a_block_withheld_after_its_reference() {
        let calls = Calls::default();
        let mut life = Life::new();
        let mut withdrawn = own(K, Standing::Withdrawn { since: 0 });
        step(&mut life, &withdrawn, None, &calls);
        assert_eq!(life.state(), ReplicaState::Withdrawn);
        let q = ((K + 1) * B).div_ceil(PAGE);
        let mut pages = PageBook::default();
        pages.fold(me(), &[page(q, false)]);
        pages.fold(peer(), &[page(q, true)]);
        let other = Own {
            state: Some(ReplicaState::Committing),
            ..own(q * PAGE / B + 30, Standing::Committing { from: 0 })
        };
        let seen = || {
            vec![Seen {
                id: peer(),
                silent: false,
                own: &other,
            }]
        };
        withdrawn.processed = Some(q * PAGE / B + 30);
        step_with(&mut life, &withdrawn, None, seen(), &pages, &calls);
        assert_eq!(calls.take(), [format!("from 0 {}", q * PAGE / B + 31)]);
        withdrawn.statuses[0] = runs([Some(skipped(q * PAGE / B + 31))]);
        withdrawn.processed = Some(q * PAGE / B + 31);
        step_with(&mut life, &withdrawn, None, seen(), &pages, &calls);
        assert_eq!(calls.take(), ["cancel 0"]);
    }

    /// A verdict is ignored only when every output that committed last
    /// started committing after the page: one output committing since
    /// before it is enough, and a replica withdrawn since its last commit
    /// start after the page ignores it.
    #[test]
    fn a_verdict_applies_while_another_output_committed_the_page() {
        let p = 977 * PAGE;
        let mixed = Own {
            processed: Some(p / B + 50),
            standing: vec![
                Standing::Committing { from: p - PAGE },
                Standing::Committing { from: p + PAGE },
            ],
            statuses: vec![Vec::new(), Vec::new()],
            ..Own::default()
        };
        let calls = Calls::default();
        let mut life = Life::new();
        step(&mut life, &mixed, None, &calls);
        assert_eq!(life.state(), ReplicaState::Committing);
        life.verdict(p / PAGE, &mixed, &[true, true], &calls);
        assert_eq!(life.state(), ReplicaState::Diverged);

        let mut life = Life::new();
        step(
            &mut life,
            &own(p / B + 50, Standing::Committing { from: p }),
            None,
            &calls,
        );
        let withdrawn = own(p / B + 60, Standing::Withdrawn { since: p / B + 60 });
        step(&mut life, &withdrawn, None, &calls);
        assert_eq!(life.state(), ReplicaState::Withdrawn);
        life.verdict(p / PAGE - 1, &withdrawn, &[true], &calls);
        assert_eq!(life.state(), ReplicaState::Withdrawn, "stale");
    }
}
