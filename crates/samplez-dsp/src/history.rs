//! History rings and the protocol that copies them off the RT thread.
//!
//! A history ring holds the last `maxDelay + slack` samples of one
//! channel as `f32` bit patterns in relaxed atomics: the RT thread stores
//! a block's samples with scalar stores, and another thread copies a span
//! without a lock while the RT thread runs on. The ring's length is the
//! delay plus `slack` whole blocks, so the span ending at the start of
//! block `e` is first overwritten by block `e + slack`.
//!
//! The RT thread calls [`Started::begin`] before it stores any sample of a
//! block. The copying thread reads the span, then asks
//! [`Started::confirms`]: if a value it read came from a block's stores,
//! the release fence in `begin` and the acquire fence in `confirms` make
//! that block's number visible to it, so a copy confirmed while the RT
//! thread had started no block at or past `e + slack - 1` (the block in
//! progress plus one block of guard) holds no sample written after the
//! span.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed, fence};

/// One channel's history ring.
pub struct History {
    ring: Box<[AtomicU32]>,
    max_delay: usize,
}

impl History {
    /// A silent ring of `max_delay + slack_blocks * block` samples.
    pub fn new(max_delay: usize, slack_blocks: usize, block: usize) -> History {
        History {
            ring: (0..max_delay + slack_blocks * block)
                .map(|_| AtomicU32::new(0))
                .collect(),
            max_delay,
        }
    }

    /// RT side: stores `samples` at the indices from `start` on. Call
    /// [`Started::begin`] for the block first.
    pub fn write(&self, start: u64, samples: &[f32]) {
        let len = self.ring.len();
        let mut pos = (start % len as u64) as usize;
        for s in samples {
            self.ring[pos].store(s.to_bits(), Relaxed);
            pos += 1;
            if pos == len {
                pos = 0;
            }
        }
    }

    /// Copy side: fills `dst`, at most `max_delay` samples, with the
    /// samples at the indices before `end`; an index below 0 reads
    /// silence. The copy is intact only if [`Started::confirms`] says so
    /// afterwards.
    pub fn copy(&self, end: u64, dst: &mut [f32]) {
        assert!(dst.len() <= self.max_delay, "a span longer than the delay");
        let len = self.ring.len() as u64;
        let missing = (dst.len() as u64).saturating_sub(end) as usize;
        dst[..missing].fill(0.0);
        let first = end + missing as u64 - dst.len() as u64;
        for (n, d) in dst[missing..].iter_mut().enumerate() {
            let i = ((first + n as u64) % len) as usize;
            *d = f32::from_bits(self.ring[i].load(Relaxed));
        }
    }
}

/// The block the RT thread computes, for copies of its history rings.
#[derive(Debug, Default)]
pub struct Started(AtomicU64);

impl Started {
    pub const fn new() -> Started {
        Started(AtomicU64::new(0))
    }

    /// RT side: block `k` starts; called before any of its history stores.
    pub fn begin(&self, k: u64) {
        self.0.store(k, Relaxed);
        fence(std::sync::atomic::Ordering::Release);
    }

    /// Copy side, after reading a span that ends at the start of block
    /// `end_block` from rings of `slack_blocks` blocks of slack: the
    /// started block when it is at or past `end_block + slack_blocks - 1`,
    /// so the span may hold samples written after it, else none.
    pub fn confirms(&self, end_block: u64, slack_blocks: u64) -> Result<(), u64> {
        fence(std::sync::atomic::Ordering::Acquire);
        let started = self.0.load(Relaxed);
        if started + 1 < end_block + slack_blocks {
            Ok(())
        } else {
            Err(started)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    const B: usize = 4;

    /// The sample block `k` stores at index `i`: its own number, so a copy
    /// shows which block wrote each value.
    fn sample(k: u64, i: u64) -> f32 {
        ((k % 4096) * 1000 + i % 1000) as f32
    }

    fn run(h: &History, s: &Started, blocks: std::ops::Range<u64>) {
        for k in blocks {
            s.begin(k);
            let data: Vec<f32> = (0..B as u64).map(|n| sample(k, k * B as u64 + n)).collect();
            h.write(k * B as u64, &data);
        }
    }

    #[test]
    fn a_copy_reads_the_span_before_its_end() {
        let (h, s) = (History::new(6, 3, B), Started::new());
        run(&h, &s, 0..5);
        let mut dst = [0.0; 6];
        h.copy(5 * B as u64, &mut dst);
        let want: Vec<f32> = (14..20).map(|i| sample(i / B as u64, i)).collect();
        assert_eq!(dst.to_vec(), want);
        // Indices below 0 are silence.
        let (h, s) = (History::new(6, 3, B), Started::new());
        run(&h, &s, 0..1);
        h.copy(2, &mut dst);
        assert_eq!(dst, [0.0, 0.0, 0.0, 0.0, sample(0, 0), sample(0, 1)]);
    }

    /// The guard row: with 3 blocks of slack the span ending at block 10
    /// is first overwritten by block 13; a copy is kept while the RT
    /// thread started at most block 11 and rejected once it starts block
    /// 12, the guard block.
    #[test]
    fn a_copy_is_rejected_once_the_guard_block_started() {
        let (h, s) = (History::new(8, 3, B), Started::new());
        run(&h, &s, 0..10);
        let mut dst = [0.0; 8];
        h.copy(10 * B as u64, &mut dst);
        assert_eq!(s.confirms(10, 3), Ok(()));
        run(&h, &s, 10..12);
        assert_eq!(s.confirms(10, 3), Ok(()), "block 11 overwrites nothing");
        run(&h, &s, 12..13);
        assert_eq!(s.confirms(10, 3), Err(12));
        // Block 12 still left the span alone; block 13 overwrites it.
        let mut again = [0.0; 8];
        h.copy(10 * B as u64, &mut again);
        assert_eq!(again, dst);
        run(&h, &s, 13..14);
        h.copy(10 * B as u64, &mut again);
        assert_ne!(again, dst);
    }

    /// A copy that races the writer is either confirmed and intact, or
    /// rejected: never confirmed with a sample of a later block. This
    /// checks the guard arithmetic under a real race; the two fences are
    /// needed under the Rust memory model all the same, and x86's store
    /// order keeps this test green without them.
    #[test]
    fn a_confirmed_copy_is_never_torn() {
        let (delay, slack) = (16, 8);
        let h = Arc::new(History::new(delay, slack, B));
        let s = Arc::new(Started::new());
        let stop = Arc::new(AtomicBool::new(false));
        // Detached: it ends once `stop` is set.
        {
            let (h, s, stop) = (Arc::clone(&h), Arc::clone(&s), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut k = 0;
                while !stop.load(Relaxed) {
                    run(&h, &s, k..k + 1);
                    k += 1;
                    // Slower than the copies, so some are kept; fast
                    // enough to race them.
                    for _ in 0..64 {
                        std::hint::spin_loop();
                    }
                }
            });
        }
        // The writer thread may start late: every attempt below copies.
        while s.0.load(std::sync::atomic::Ordering::Acquire) < 9 {
            std::hint::spin_loop();
        }
        let mut kept = 0;
        let mut dst = vec![0.0; delay];
        for _ in 0..20_000 {
            // A span that has been written: reading started block L with
            // acquire makes the stores of every block before L - 1
            // visible.
            let end_block =
                s.0.load(std::sync::atomic::Ordering::Acquire)
                    .saturating_sub(1);
            if end_block < 8 {
                continue;
            }
            let end = end_block * B as u64;
            h.copy(end, &mut dst);
            if s.confirms(end_block, slack as u64).is_err() {
                continue;
            }
            kept += 1;
            for (n, d) in dst.iter().enumerate() {
                let i = end - delay as u64 + n as u64;
                assert_eq!(*d, sample(i / B as u64, i), "index {i} of a kept copy");
            }
        }
        stop.store(true, Relaxed);
        assert!(kept > 0, "no copy kept");
    }
}
