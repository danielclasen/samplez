//! The input delay: each leg of an input played a number of samples
//! late, the samples stored by sample index in [`History`] rings, so the
//! output at index `n` is the input at `n - d` whatever the block size.
//!
//! A change of the delay crossfades from the old tap to the new one over
//! the ramp length from its index, linearly in gain as a re-route does;
//! a change during a crossfade waits until it ends, the latest one
//! counting. Outside a crossfade the output is the input `d` samples
//! late, bit for bit.
//!
//! Each leg has two rings, one written and read by the RT thread, so a
//! thread installing another engine's history writes the other one,
//! which the RT thread takes only at an install.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

use crate::{History, Ramp};

/// The longest delay: 5 s at 48 kHz.
pub const MAX_DELAY_SAMPLES: u32 = 240_000;

/// Where a delay stands: the tap it leaves, the tap it crossfades to from
/// index `start` over the ramp length, and a tap waiting for that
/// crossfade to end. Taps are delays in samples.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Taps {
    pub from: u32,
    pub to: u32,
    pub start: u64,
    pub next: Option<u32>,
}

impl Taps {
    /// The longest tap the delay reads from.
    pub fn longest(&self) -> u32 {
        self.from.max(self.to).max(self.next.unwrap_or(0))
    }

    /// Whether every tap is at most `max` and a waiting one waits for a
    /// crossfade.
    pub fn is_within(&self, max: u32) -> bool {
        self.longest() <= max && (self.next.is_none() || self.from != self.to)
    }

    /// The waiting tap starts its crossfade once the one before ended at
    /// or before index `n`.
    fn promote(&mut self, n: u64, ramp: u32) {
        let end = self.start.saturating_add(u64::from(ramp));
        if let Some(next) = self.next
            && n >= end
        {
            self.from = self.to;
            self.to = next;
            self.start = end;
            self.next = None;
        }
    }

    /// Whether a crossfade runs at index `n`.
    fn fading(&self, n: u64, ramp: u32) -> bool {
        self.from != self.to && n < self.start.saturating_add(u64::from(ramp))
    }

    /// The longest tap read at index `n` or waiting to be: the old tap
    /// only while the crossfade from it runs.
    fn reach(&self, n: u64, ramp: u32) -> u32 {
        let old = if n < self.start || self.fading(n, ramp) {
            self.from
        } else {
            0
        };
        old.max(self.to).max(self.next.unwrap_or(0))
    }
}

/// The rings of one input's delay, shared by the RT thread and the
/// threads that copy or install its history: two sets of one ring per
/// leg, the RT thread on the active one.
pub struct DelayRings {
    sets: [Vec<History>; 2],
    active: AtomicU8,
    max: u32,
}

impl DelayRings {
    /// Silent rings for `legs` legs and delays up to `max` samples, with
    /// `slack_blocks` blocks of `block` samples of slack.
    pub fn new(legs: usize, max: u32, slack_blocks: usize, block: usize) -> Arc<DelayRings> {
        let set = || {
            (0..legs)
                .map(|_| History::new(max as usize, slack_blocks, block))
                .collect()
        };
        Arc::new(DelayRings {
            sets: [set(), set()],
            active: AtomicU8::new(0),
            max,
        })
    }

    pub fn legs(&self) -> usize {
        self.sets[0].len()
    }

    /// The longest delay the rings hold.
    pub fn max(&self) -> u32 {
        self.max
    }

    /// The set the RT thread writes and reads.
    pub fn active(&self) -> usize {
        usize::from(self.active.load(Ordering::Acquire))
    }

    /// The other set.
    pub fn inactive(&self) -> usize {
        1 - self.active()
    }

    /// Off the RT thread: the `len` samples of each leg of set `set`
    /// before index `end`, at most [`DelayRings::max`]; indices below 0
    /// are silence. Intact only if [`crate::Started::confirms`] says so
    /// afterwards, for the RT thread's set.
    pub fn copy(&self, set: usize, end: u64, len: u32) -> Vec<Vec<f32>> {
        (self.sets[set].iter())
            .map(|ring| {
                let mut span = vec![0.0; len.min(self.max) as usize];
                ring.copy(end, &mut span);
                span
            })
            .collect()
    }

    /// Stores `span`, a leg each, in set `set` at the indices before
    /// `end`: what [`DelayRings::copy`] took, written where another
    /// engine reads it. A delay installed with taps no longer than the
    /// span reads nothing older.
    pub fn fill(&self, set: usize, end: u64, span: &[Vec<f32>]) {
        for (ring, leg) in self.sets[set].iter().zip(span) {
            let from = end.saturating_sub(leg.len() as u64);
            ring.write(from, &leg[leg.len() - (end - from) as usize..]);
        }
    }
}

/// Off the RT thread: copies into `dst`'s inactive set the `len` samples
/// of each leg before index `end` that `src`'s active set holds, where
/// `done` names a set and an index up to which an earlier call filled
/// it: of the same set, only the samples from there on are copied,
/// which [`DelayRings::fill`] places as a whole copy would. Returns the
/// set filled and `end`, for the next call.
pub fn copy_since(
    src: &DelayRings,
    dst: &DelayRings,
    end: u64,
    len: u32,
    done: Option<(usize, u64)>,
) -> (usize, u64) {
    let set = dst.inactive();
    let mut from = end.saturating_sub(u64::from(len));
    if let Some((s, d)) = done
        && s == set
    {
        from = from.max(d.min(end));
    }
    let span = src.copy(src.active(), end, (end - from) as u32);
    dst.fill(set, end, &span);
    (set, end)
}

/// One input's delay on the RT thread. Before any is set it passes its
/// input on unchanged, still storing it.
pub struct Delay {
    rings: Arc<DelayRings>,
    taps: Taps,
    ramp: u32,
    /// The index of the next sample processed.
    pos: Option<u64>,
    /// Samples before this index read as silence: the oldest one the
    /// taps have reached without a break. A delay raised reads silence
    /// for the samples it reaches back over, rather than playing them
    /// again, so the history a delay needs is only what its taps reach.
    floor: u64,
}

impl Delay {
    /// A delay on `rings` whose changes crossfade over `ramp` samples.
    pub fn new(rings: Arc<DelayRings>, ramp: u32) -> Delay {
        Delay {
            rings,
            taps: Taps::default(),
            ramp,
            pos: None,
            floor: 0,
        }
    }

    pub fn rings(&self) -> &Arc<DelayRings> {
        &self.rings
    }

    pub fn taps(&self) -> Taps {
        self.taps
    }

    pub fn pos(&self) -> Option<u64> {
        self.pos
    }

    /// The oldest index the delay may still read; samples before it read
    /// as silence.
    pub fn floor(&self) -> u64 {
        self.floor
    }

    /// The samples of history before index `end`, the next one processed,
    /// that the delay may still read: from its floor, at most the rings'
    /// longest delay. A copy of them is all another delay needs.
    pub fn span(&self, end: u64) -> u32 {
        end.saturating_sub(self.floor)
            .min(u64::from(self.rings.max)) as u32
    }

    /// The state another delay on rings of the same shape held: its taps,
    /// position, floor and the active set. The rings themselves are
    /// written by [`DelayRings::fill`]; what lies below the floor is never
    /// read.
    pub fn install(&mut self, taps: Taps, pos: Option<u64>, floor: u64, set: usize) {
        self.taps = taps;
        self.pos = pos;
        self.floor = floor;
        self.rings.active.store(set as u8, Ordering::Release);
    }

    /// Sets the delay to `samples` from index `at_index`: a crossfade
    /// from there, or after the one running. False, and nothing set,
    /// above the rings' longest delay.
    #[must_use]
    pub fn set(&mut self, at_index: u64, samples: u32) -> bool {
        if samples > self.rings.max {
            return false;
        }
        let t = &mut self.taps;
        t.promote(at_index, self.ramp);
        if t.fading(at_index, self.ramp) {
            t.next = (samples != t.to).then_some(samples);
        } else if samples != t.to {
            *t = Taps {
                from: t.to,
                to: samples,
                start: at_index,
                next: None,
            };
        }
        true
    }

    /// Stores `frames` samples of each leg from index `start` and
    /// replaces them by the delayed ones: `buf` holds the legs one after
    /// the other. Indices skipped since the last call are stored as
    /// silence.
    ///
    /// # Panics
    ///
    /// If `buf` is shorter than the legs times `frames`.
    pub fn process(&mut self, start: u64, buf: &mut [f32], frames: usize) {
        let rings = &self.rings.sets[self.rings.active()];
        if let Some(pos) = self.pos
            && start > pos
        {
            for ring in rings {
                ring.clear(pos, start - pos);
            }
        }
        self.pos = Some(start + frames as u64);
        for (ring, leg) in rings.iter().zip(buf.chunks_exact(frames)) {
            ring.write(start, leg);
        }
        let end = start + frames as u64;
        self.taps.promote(start, self.ramp);
        let to = u64::from(self.taps.to);
        // Outside a crossfade, at or past the delay and the floor: one
        // read a leg, the floor moved to the last sample's reach.
        if !self.taps.fading(start, self.ramp)
            && self.taps.next.is_none()
            && start >= self.taps.start
            && start >= to
            && start - to >= self.floor
        {
            for (ring, leg) in rings.iter().zip(buf.chunks_exact_mut(frames)) {
                ring.read(start - to, leg);
            }
            self.floor = self.floor.max(end - 1 - to);
            return;
        }
        let mut up = Ramp::new(0.0);
        let _ = up.set_target(1.0, self.ramp);
        let mut down = Ramp::new(1.0);
        let _ = down.set_target(0.0, self.ramp);
        for n in start..end {
            self.taps.promote(n, self.ramp);
            let t = self.taps;
            let reach = u64::from(t.reach(n, self.ramp));
            self.floor = self.floor.max(n.saturating_sub(reach));
            let floor = self.floor;
            let at = |ring: &History, n: u64, d: u32| {
                let mut v = [0.0];
                if n >= u64::from(d) && n - u64::from(d) >= floor {
                    ring.read(n - u64::from(d), &mut v);
                }
                v[0]
            };
            let i = (n - start) as usize;
            for (ring, leg) in rings.iter().zip(buf.chunks_exact_mut(frames)) {
                leg[i] = if n < t.start {
                    at(ring, n, t.from)
                } else if t.fading(n, self.ramp) {
                    let pos = (n - t.start) as u32;
                    let (new, old) = (at(ring, n, t.to), at(ring, n, t.from));
                    new * up.value_at(pos) + old * down.value_at(pos)
                } else {
                    at(ring, n, t.to)
                };
            }
        }
        self.taps.promote(end, self.ramp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAMP: u32 = 48;

    fn signal(legs: usize, n: u64) -> Vec<f32> {
        (0..legs)
            .map(|leg| ((n * 7919 + leg as u64 * 31) % 1013) as f32 / 1013.0 - 0.5)
            .collect()
    }

    /// `len` samples from index `first` through `d` in blocks of `b`,
    /// with `set` applied at its index: (index, samples).
    fn run(d: &mut Delay, first: u64, len: u64, b: u64, set: &[(u64, u32)]) -> Vec<Vec<f32>> {
        let legs = d.rings().legs();
        let mut out = vec![Vec::new(); legs];
        let mut n = first;
        while n < first + len {
            for &(at, s) in set.iter().filter(|(at, _)| (n..n + b).contains(at)) {
                assert!(d.set(at, s));
            }
            let mut buf: Vec<f32> = (0..legs)
                .flat_map(|leg| (n..n + b).map(move |i| signal(legs, i)[leg]))
                .collect();
            d.process(n, &mut buf, b as usize);
            for (leg, o) in out.iter_mut().enumerate() {
                o.extend_from_slice(&buf[leg * b as usize..(leg + 1) * b as usize]);
            }
            n += b;
        }
        out
    }

    /// Rings with slack for blocks up to 256 samples, the largest run.
    fn delay(legs: usize, max: u32) -> Delay {
        Delay::new(DelayRings::new(legs, max, 2, 256), RAMP)
    }

    /// Set before the first sample with no crossfade, the output is the
    /// input `d` samples late bit for bit, silence before it.
    #[test]
    fn the_output_is_the_input_late() {
        let mut d = Delay::new(DelayRings::new(2, 1000, 2, 64), 0);
        let out = run(&mut d, 0, 2000, 64, &[(0, 700)]);
        for (leg, o) in out.iter().enumerate() {
            for (n, &y) in o.iter().enumerate() {
                let want = if n < 700 {
                    0.0
                } else {
                    signal(2, n as u64 - 700)[leg]
                };
                assert_eq!(y.to_bits(), want.to_bits(), "leg {leg} at {n}");
            }
        }
    }

    /// A change crossfades from the old tap to the new over the ramp
    /// length from its index; one during the crossfade waits for its end.
    /// A raised delay reads silence for the samples it reaches back over,
    /// those older than the taps reached before: here everything before
    /// index 539, the oldest the 100-sample tap had read.
    #[test]
    fn a_change_crossfades_and_one_during_it_waits() {
        let mut d = delay(1, 1000);
        let out = run(&mut d, 0, 2000, 64, &[(0, 100), (640, 300), (660, 500)]);
        let x = |n: u64, t: u64| {
            if n < t + 539 && n >= 640 || n < t {
                0.0
            } else {
                signal(1, n - t)[0]
            }
        };
        let fade = |n: u64, s: u64, from: u64, to: u64| {
            let mut up = Ramp::new(0.0);
            let _ = up.set_target(1.0, RAMP);
            let mut down = Ramp::new(1.0);
            let _ = down.set_target(0.0, RAMP);
            let p = (n - s) as u32;
            x(n, to) * up.value_at(p) + x(n, from) * down.value_at(p)
        };
        for n in 0..2000u64 {
            let want = match n {
                ..48 => fade(n, 0, 0, 100),
                48..640 => x(n, 100),
                640..688 => fade(n, 640, 100, 300),
                688..736 => fade(n, 688, 300, 500),
                _ => x(n, 500),
            };
            assert_eq!(out[0][n as usize].to_bits(), want.to_bits(), "at {n}");
        }
        // The last block processed ends at 2048.
        assert_eq!(d.floor(), 2048 - 1 - 500);
    }

    /// The output depends on neither the block size nor the split, a
    /// change during a crossfade and a raise included.
    #[test]
    fn the_output_does_not_depend_on_the_blocks() {
        // Indices on every block size's grid; a 300-sample crossfade
        // from 512, the change at 768 waiting for its end.
        let set = [(0, 100), (512, 7), (768, 900), (2048, 0), (2560, 600)];
        let line = || Delay::new(DelayRings::new(2, 1000, 2, 256), 300);
        let reference = run(&mut line(), 0, 4096, 16, &set);
        for b in [32, 64, 128, 256] {
            let got = run(&mut line(), 0, 4096, b, &set);
            let differ =
                |n: usize| (0..2).any(|l| got[l][n].to_bits() != reference[l][n].to_bits());
            assert_eq!((0..4096).find(|&n| differ(n)), None, "block {b}");
        }
    }

    /// Samples skipped are stored as silence, not left from a ring's
    /// length before.
    #[test]
    fn skipped_samples_are_silence() {
        let mut d = delay(1, 200);
        assert!(d.set(0, 100));
        let _ = run(&mut d, 0, 640, 64, &[]);
        // 128 samples skipped, then the delayed ones read silence there.
        let out = run(&mut d, 768, 128, 64, &[]);
        let want: Vec<f32> = (768..896u64)
            .map(|n| {
                if (640..768).contains(&(n - 100)) {
                    0.0
                } else {
                    signal(1, n - 100)[0]
                }
            })
            .collect();
        assert_eq!(out[0], want);
    }

    /// Copied in steps, the history in the target set is that of one
    /// whole copy bit for bit: a first call copies all of it, a later one
    /// of the same set only the samples since, a raised floor between
    /// them included; a call for another set copies all of it again.
    #[test]
    fn a_copy_in_steps_equals_one_whole_copy() {
        let mut src = delay(2, 1000);
        let _ = run(&mut src, 0, 1280, 64, &[(0, 300)]);
        let dst = DelayRings::new(2, 1000, 2, 256);
        let whole = |s: &Delay, end: u64| {
            let rings = DelayRings::new(2, 1000, 2, 256);
            let _ = copy_since(s.rings(), &rings, end, s.span(end), None);
            rings.copy(rings.inactive(), end, s.span(end))
        };
        let done = copy_since(src.rings(), &dst, 1280, src.span(1280), None);
        // On, then the delay raised: the floor moves, the span grows.
        let _ = run(&mut src, 1280, 640, 64, &[(1600, 900)]);
        let end = 1920;
        let done = copy_since(src.rings(), &dst, end, src.span(end), Some(done));
        assert_eq!(done, (dst.inactive(), end));
        assert_eq!(
            dst.copy(dst.inactive(), end, src.span(end)),
            whole(&src, end)
        );
        // A fill of another set does not count: the target set, holding
        // stale samples, is copied whole.
        let _ = run(&mut src, 1920, 128, 64, &[]);
        let stale = DelayRings::new(2, 1000, 2, 256);
        stale.fill(stale.inactive(), 2048, &vec![vec![9.0f32; 1000]; 2]);
        let other = Some((1 - stale.inactive(), 2040));
        let _ = copy_since(src.rings(), &stale, 2048, src.span(2048), other);
        assert_eq!(
            stale.copy(stale.inactive(), 2048, src.span(2048)),
            whole(&src, 2048)
        );
    }

    /// A delay above the rings' longest is refused and changes nothing.
    #[test]
    fn a_delay_beyond_the_rings_is_refused() {
        let mut d = delay(1, 200);
        assert!(!d.set(0, 201));
        assert_eq!(d.taps(), Taps::default());
        assert!(d.set(0, 200));
    }

    /// A span copied from one delay's rings and filled into another's,
    /// installed with its taps and floor, continues bit for bit, through a
    /// raise to the longest delay; what lay below the span is not read.
    #[test]
    fn a_copied_span_continues_alike() {
        let mut a = delay(2, 1000);
        let _ = run(&mut a, 0, 1280, 64, &[(0, 300), (1216, 900)]);
        let span = a.rings().copy(a.rings().active(), 1280, a.span(1280));
        let mut b = delay(2, 1000);
        let set = b.rings().inactive();
        // Stale history below the span, which the floor keeps unread.
        let junk = vec![vec![9.0f32; 1000]; 2];
        b.rings().fill(set, 1280 - span[0].len() as u64, &junk);
        b.rings().fill(set, 1280, &span);
        b.install(a.taps(), a.pos(), a.floor(), set);
        let ya = run(&mut a, 1280, 1280, 64, &[(1920, 1000)]);
        let yb = run(&mut b, 1280, 1280, 64, &[(1920, 1000)]);
        assert_eq!(ya, yb);
    }
}
