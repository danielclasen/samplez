//! Supervisor side. Lookups of flow directories, each on a thread of its
//! own.
//!
//! On a domain whose file system waits on the lookup of a missing name for
//! the name to appear, libmxl's own open of a missing flow waits as long. A
//! lookup made here first waits on its own thread instead, so the caller
//! is never held up by it, and the file system sees the walk all the same:
//! one that has the flow brought to the node when it is wanted is asked
//! for it. libmxl is called for a flow only once a lookup found it.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError, sync_channel};
use std::time::{Duration, Instant};

/// How a directory is looked up: true when it is there.
pub type Look = fn(&Path) -> bool;

/// Stack of a lookup thread: one `lstat`.
const STACK: usize = 64 * 1024;

/// The lookup [`Look`] of a real domain: whether `dir` exists.
pub fn exists(dir: &Path) -> bool {
    dir.symlink_metadata().is_ok()
}

/// The directory libmxl keeps flow `flow_id` in on `domain`.
pub fn flow_dir(domain: &Path, flow_id: &str) -> PathBuf {
    domain.join(format!("{flow_id}.mxl-flow"))
}

/// One directory, looked up at most once at a time.
pub struct Lookup {
    dir: PathBuf,
    look: Look,
    running: Option<Receiver<bool>>,
}

impl Lookup {
    pub fn new(dir: PathBuf, look: Look) -> Lookup {
        Lookup {
            dir,
            look,
            running: None,
        }
    }

    /// A lookup was started and has not been answered.
    pub fn running(&self) -> bool {
        self.running.is_some()
    }

    /// Starts a lookup unless one runs. False when no thread could be
    /// started.
    pub fn start(&mut self) -> bool {
        if self.running.is_some() {
            return true;
        }
        let (tx, rx) = sync_channel(1);
        let (dir, look) = (self.dir.clone(), self.look);
        let spawned = std::thread::Builder::new()
            .name("samplez-lookup".into())
            .stack_size(STACK)
            .spawn(move || {
                let _ = tx.send(look(&dir));
            });
        if spawned.is_ok() {
            self.running = Some(rx);
        }
        spawned.is_ok()
    }

    /// The answer of the running lookup once it has one, without waiting.
    /// A lookup thread that ended without one answers false.
    pub fn answer(&mut self) -> Option<bool> {
        let answer = match self.running.as_ref()?.try_recv() {
            Ok(found) => found,
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Disconnected) => false,
        };
        self.running = None;
        Some(answer)
    }

    /// [`Lookup::answer`], waiting for it until `until`.
    pub fn answer_by(&mut self, until: Instant) -> Option<bool> {
        let wait = until.saturating_duration_since(Instant::now());
        #[allow(clippy::disallowed_methods, reason = "supervisor side")]
        let answer = self.running.as_ref()?.recv_timeout(wait);
        let answer = match answer {
            Ok(found) => found,
            Err(RecvTimeoutError::Timeout) => return None,
            Err(RecvTimeoutError::Disconnected) => false,
        };
        self.running = None;
        Some(answer)
    }
}

/// The directories one caller is about to open, looked up together: what
/// the caller finds absent, or not yet answered, it leaves out, and a
/// lookup not answered keeps running for its next call.
pub struct Gate {
    look: Look,
    lookups: HashMap<PathBuf, Lookup>,
}

impl Gate {
    pub fn new(look: Look) -> Gate {
        Gate {
            look,
            lookups: HashMap::new(),
        }
    }

    /// Whether each of `dirs` was found. Starts a lookup for each that has
    /// none running, and waits for the answers until `wait` has passed. A
    /// lookup that answered since an earlier call is started afresh, so an
    /// answer is never older than the call.
    pub fn found(&mut self, dirs: &[PathBuf], wait: Duration) -> Vec<bool> {
        let until = Instant::now() + wait;
        // Answered lookups of directories not asked for now are dropped.
        self.lookups.retain(|_, l| {
            let _ = l.answer();
            l.running()
        });
        for dir in dirs {
            let look = self.look;
            let l = (self.lookups)
                .entry(dir.clone())
                .or_insert_with(|| Lookup::new(dir.clone(), look));
            l.start();
        }
        dirs.iter()
            .map(|dir| {
                let l = self.lookups.get_mut(dir).expect("inserted above");
                l.answer_by(until) == Some(true)
            })
            .collect()
    }
}

/// When a missing flow is looked up again.
///
/// For [`GRACE`] after the flow was wanted anew (first wanted, re-routed to,
/// raised), a lookup is kept running all the time, so the flow is found as
/// soon as it appears; on a file system that answers at once, that is one
/// lookup per poll. A flow missing for longer is taken for disconnected
/// and looked up after a gap that starts at [`FIRST_GAP`] and doubles up to
/// [`MAX_GAP`].
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    since: Instant,
    gap: Duration,
    next: Instant,
}

/// How long after a flow was wanted anew it is looked up without a gap.
pub const GRACE: Duration = Duration::from_secs(10);
/// The first gap between lookups of a flow missing for longer.
pub const FIRST_GAP: Duration = Duration::from_millis(100);
/// The longest gap between lookups of a missing flow.
pub const MAX_GAP: Duration = Duration::from_secs(5);

impl Backoff {
    /// Wanted anew at `now`.
    pub fn new(now: Instant) -> Backoff {
        Backoff {
            since: now,
            gap: Duration::ZERO,
            next: now,
        }
    }

    /// A lookup answered at `now` that the flow is missing.
    pub fn missing(&mut self, now: Instant) {
        if now.saturating_duration_since(self.since) < GRACE {
            self.next = now;
            return;
        }
        self.gap = if self.gap.is_zero() {
            FIRST_GAP
        } else {
            (2 * self.gap).min(MAX_GAP)
        };
        self.next = now + self.gap;
    }

    /// Whether a lookup is due at `now`.
    pub fn due(&self, now: Instant) -> bool {
        now >= self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Gaps after the grace: 100 ms doubling to 5 s, and wanted anew, none.
    #[test]
    fn a_missing_flow_backs_off_after_the_grace() {
        let t0 = Instant::now();
        let mut b = Backoff::new(t0);
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        b.missing(at(9_999));
        assert!(b.due(at(9_999)));
        let mut t = 10_000;
        for gap in [100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000] {
            b.missing(at(t));
            assert!(!b.due(at(t + gap - 1)), "gap {gap} at {t}");
            assert!(b.due(at(t + gap)), "gap {gap} at {t}");
            t += gap;
        }
        let mut b = Backoff::new(at(t));
        b.missing(at(t + 1));
        assert!(b.due(at(t + 1)));
    }

    /// An answer the gate waited for counts; one not given within the wait
    /// does not, and its lookup is the one the next call reads.
    #[test]
    #[allow(clippy::disallowed_methods, reason = "a test, not the RT thread")]
    fn the_gate_waits_its_time_and_keeps_a_lookup_running() {
        fn slow(dir: &Path) -> bool {
            std::thread::sleep(Duration::from_millis(300));
            exists(dir)
        }
        let dir = std::env::temp_dir();
        let mut gate = Gate::new(slow);
        let started = Instant::now();
        assert_eq!(
            gate.found(std::slice::from_ref(&dir), Duration::from_millis(20)),
            [false]
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        assert_eq!(
            gate.found(std::slice::from_ref(&dir), Duration::from_secs(5)),
            [true]
        );
        assert!(started.elapsed() < Duration::from_millis(600), "one lookup");
        let mut fast = Gate::new(exists);
        let missing = dir.join("samplez-no-such-directory");
        assert_eq!(
            fast.found(&[dir, missing], Duration::from_secs(5)),
            [true, false]
        );
    }
}
