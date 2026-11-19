//! Locks that record their holders, and a watchdog for locks held too long.
//!
//! [`WatchedMutex`] wraps `std::sync::Mutex` and [`WatchedRwLock`]
//! `std::sync::RwLock`; each notes, while it is held, which threads hold
//! it, since when, and the call site of each acquisition.
//! A thread that has to wait for it is noted with its call site as well.
//! [`start_watchdog`] runs a thread that, once a lock has been held past the
//! limit, writes each such lock and its holder and waiters, each with its call site, to stderr and
//! aborts the process: a lock-order deadlock then ends loudly instead of
//! waiting for a liveness probe to kill the process.
//!
//! Only non-RT locks use this; the RT thread's code paths never touch it.

use std::fmt::Write as _;
use std::ops::{Deref, DerefMut};
use std::panic::Location;
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, Weak,
};
use std::thread;
use std::time::{Duration, Instant};

/// A lock held for longer than this ends the process.
pub const HELD_LIMIT: Duration = Duration::from_secs(10);

struct Held {
    id: u64,
    thread: String,
    since: Instant,
    taken: &'static Location<'static>,
}

struct Waiter {
    id: u64,
    thread: String,
    since: Instant,
    at: &'static Location<'static>,
}

/// What the watchdog reads of a lock.
pub(crate) struct Record {
    name: &'static str,
    /// Held for a few instructions only, never across a wait.
    state: Mutex<Notes>,
}

#[derive(Default)]
struct Notes {
    held: Vec<Held>,
    waiting: Vec<Waiter>,
}

fn registry() -> &'static Mutex<Vec<Weak<Record>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<Record>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

fn current() -> String {
    let t = thread::current();
    match t.name() {
        Some(n) => format!("{n} ({:?})", t.id()),
        None => format!("{:?}", t.id()),
    }
}

/// A mutex the watchdog can name and whose holder it can report.
pub struct WatchedMutex<T> {
    record: Arc<Record>,
    inner: Mutex<T>,
}

impl<T> WatchedMutex<T> {
    pub fn new(name: &'static str, value: T) -> Self {
        Self {
            record: register(name),
            inner: Mutex::new(value),
        }
    }

    #[cfg(test)]
    pub(crate) fn record(&self) -> Arc<Record> {
        self.record.clone()
    }

    /// Locks, panicking on a poisoned lock. The caller's location is noted
    /// as the holder's site, or as a waiter's while the lock is taken.
    #[track_caller]
    pub fn lock(&self) -> WatchedGuard<'_, T> {
        let at = Location::caller();
        let name = self.record.name;
        let guard = match self.inner.try_lock() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(_)) => panic!("{name} lock poisoned"),
            Err(TryLockError::WouldBlock) => {
                let _waiting = Waiting::note(&self.record, at);
                self.inner
                    .lock()
                    .unwrap_or_else(|_| panic!("{name} lock poisoned"))
            }
        };
        WatchedGuard {
            guard,
            record: &self.record,
            id: self.record.hold(at),
        }
    }
}

/// A waiter's note, removed on every path out of the wait.
struct Waiting<'a> {
    record: &'a Record,
    id: u64,
}

impl<'a> Waiting<'a> {
    fn note(record: &'a Record, at: &'static Location<'static>) -> Self {
        let id = next_id();
        record.notes().waiting.push(Waiter {
            id,
            thread: current(),
            since: Instant::now(),
            at,
        });
        Self { record, id }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.record.notes().waiting.retain(|w| w.id != self.id);
    }
}

/// A new lock's record, registered with the watchdog.
fn register(name: &'static str) -> Arc<Record> {
    let record = Arc::new(Record {
        name,
        state: Mutex::default(),
    });
    let mut all = registry().lock().expect("lock registry poisoned");
    all.retain(|w| w.strong_count() > 0);
    all.push(Arc::downgrade(&record));
    record
}

fn next_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

impl Record {
    /// How long the lock has been held at `now`, by its longest holder;
    /// none while it is free.
    #[cfg(test)]
    pub(crate) fn held_for(&self, now: Instant) -> Option<Duration> {
        (self.notes().held.iter())
            .map(|h| now.saturating_duration_since(h.since))
            .max()
    }

    /// The threads waiting for the lock.
    #[cfg(test)]
    pub(crate) fn waiters(&self) -> usize {
        self.notes().waiting.len()
    }

    /// Notes the current thread as a holder, taken at `at`.
    fn hold(&self, at: &'static Location<'static>) -> u64 {
        let id = next_id();
        self.notes().held.push(Held {
            id,
            thread: current(),
            since: Instant::now(),
            taken: at,
        });
        id
    }

    fn release(&self, id: u64) {
        self.notes().held.retain(|h| h.id != id);
    }

    fn notes(&self) -> MutexGuard<'_, Notes> {
        // The notes hold plain data: a poisoned lock still reads.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub struct WatchedGuard<'a, T> {
    guard: MutexGuard<'a, T>,
    record: &'a Record,
    id: u64,
}

impl<T> Deref for WatchedGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for WatchedGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for WatchedGuard<'_, T> {
    /// Runs before `guard` unlocks, so a new holder's note is never
    /// cleared by the previous one.
    fn drop(&mut self) {
        self.record.release(self.id);
    }
}

/// A read-write lock the watchdog can name and whose holders it can
/// report, readers and writer alike.
pub struct WatchedRwLock<T> {
    record: Arc<Record>,
    inner: RwLock<T>,
}

impl<T> WatchedRwLock<T> {
    pub fn new(name: &'static str, value: T) -> Self {
        Self {
            record: register(name),
            inner: RwLock::new(value),
        }
    }

    #[cfg(test)]
    pub(crate) fn record(&self) -> Arc<Record> {
        self.record.clone()
    }

    /// Takes the lock for reading, panicking on a poisoned lock.
    #[track_caller]
    pub fn read(&self) -> WatchedReadGuard<'_, T> {
        let at = Location::caller();
        let name = self.record.name;
        let guard = match self.inner.try_read() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(_)) => panic!("{name} lock poisoned"),
            Err(TryLockError::WouldBlock) => {
                let _waiting = Waiting::note(&self.record, at);
                self.inner
                    .read()
                    .unwrap_or_else(|_| panic!("{name} lock poisoned"))
            }
        };
        WatchedReadGuard {
            guard,
            record: &self.record,
            id: self.record.hold(at),
        }
    }

    /// Takes the lock for writing, panicking on a poisoned lock.
    #[track_caller]
    pub fn write(&self) -> WatchedWriteGuard<'_, T> {
        let at = Location::caller();
        let name = self.record.name;
        let guard = match self.inner.try_write() {
            Ok(g) => g,
            Err(TryLockError::Poisoned(_)) => panic!("{name} lock poisoned"),
            Err(TryLockError::WouldBlock) => {
                let _waiting = Waiting::note(&self.record, at);
                self.inner
                    .write()
                    .unwrap_or_else(|_| panic!("{name} lock poisoned"))
            }
        };
        WatchedWriteGuard {
            guard,
            record: &self.record,
            id: self.record.hold(at),
        }
    }
}

pub struct WatchedReadGuard<'a, T> {
    guard: RwLockReadGuard<'a, T>,
    record: &'a Record,
    id: u64,
}

impl<T> Deref for WatchedReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> Drop for WatchedReadGuard<'_, T> {
    fn drop(&mut self) {
        self.record.release(self.id);
    }
}

pub struct WatchedWriteGuard<'a, T> {
    guard: RwLockWriteGuard<'a, T>,
    record: &'a Record,
    id: u64,
}

impl<T> Deref for WatchedWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for WatchedWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for WatchedWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.record.release(self.id);
    }
}

/// Every lock among `records` held at `now` for longer than `limit`: the
/// lock, its holder and the call site of the acquisition, and the threads
/// waiting for it. `None` when no lock is.
pub(crate) fn overdue(records: &[Arc<Record>], limit: Duration, now: Instant) -> Option<String> {
    let mut out = String::new();
    for r in records {
        // Copied out, so that no note lock is held while formatting.
        let (held, waiting) = {
            let notes = r.notes();
            let held: Vec<_> = (notes.held.iter())
                .map(|h| (h.thread.clone(), h.since, h.taken))
                .filter(|&(_, since, _)| now.saturating_duration_since(since) > limit)
                .collect();
            let waiting: Vec<_> = notes
                .waiting
                .iter()
                .map(|w| (w.thread.clone(), w.since, w.at))
                .collect();
            (held, waiting)
        };
        if held.is_empty() {
            continue;
        }
        for (thread, since, taken) in held {
            let _ = writeln!(
                out,
                "lock {:?} held for {:.1} s (limit {:.1} s) by thread {thread} at {taken}",
                r.name,
                now.saturating_duration_since(since).as_secs_f64(),
                limit.as_secs_f64(),
            );
        }
        for (thread, since, at) in waiting {
            let _ = writeln!(
                out,
                "  thread {thread} has waited for it {:.1} s at {at}",
                now.saturating_duration_since(since).as_secs_f64(),
            );
        }
    }
    (!out.is_empty()).then_some(out)
}

pub(crate) fn live() -> Vec<Arc<Record>> {
    registry()
        .lock()
        .expect("lock registry poisoned")
        .iter()
        .filter_map(Weak::upgrade)
        .collect()
}

/// Starts the watchdog thread once per process: it checks the watched
/// locks every `limit / 10` (at most every second) and, on the first one
/// held past `limit`, writes the report to stderr and aborts the process.
/// A later call changes nothing and panics when its `limit` differs from
/// the first call's.
///
/// Only [`WatchedMutex`] and [`WatchedRwLock`] locks are watched: the
/// replica's `Contact` lock and the log `Store`'s locks. Any process that starts a replica,
/// a test included, aborts when one of them is held past the limit.
pub fn start_watchdog(limit: Duration) {
    static STARTED: OnceLock<Duration> = OnceLock::new();
    let first = *STARTED.get_or_init(|| {
        let period = (limit / 10).clamp(Duration::from_millis(10), Duration::from_secs(1));
        let spawned = thread::Builder::new()
            .name("samplez-lock-watchdog".into())
            .spawn(move || {
                loop {
                    thread::sleep(period);
                    if let Some(report) = overdue(&live(), limit, Instant::now()) {
                        eprintln!(
                            "samplez: a replica lock was held past its limit, aborting\n{report}"
                        );
                        std::process::abort();
                    }
                }
            });
        if let Err(e) = spawned {
            tracing::error!("the lock watchdog did not start: {e}");
        }
        limit
    });
    assert_eq!(
        first, limit,
        "the lock watchdog runs with the limit it started with"
    );
}

/// Marks the current thread as running product code under a replica
/// lock ([`crate::state_machine::Product`] calls, and the closures
/// [`crate::log_store::Store`] runs under its locks) while it lives.
/// Taking the store or contact lock then panics
/// ([`assert_no_product_code`]): a product that calls back into the
/// replica from there would deadlock against the contact loop, which
/// holds the contact lock and waits for the store.
pub(crate) struct ProductCode(std::marker::PhantomData<*const ()>);

thread_local! {
    static PRODUCT_CODE: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

impl ProductCode {
    pub(crate) fn enter() -> ProductCode {
        PRODUCT_CODE.with(|n| n.set(n.get() + 1));
        ProductCode(std::marker::PhantomData)
    }
}

impl Drop for ProductCode {
    fn drop(&mut self) {
        PRODUCT_CODE.with(|n| n.set(n.get() - 1));
    }
}

/// Panics when the current thread runs product code under a replica lock
/// ([`ProductCode`]): taking the `name` lock from there could deadlock.
#[track_caller]
pub(crate) fn assert_no_product_code(name: &str) {
    if PRODUCT_CODE.with(std::cell::Cell::get) > 0 {
        panic!(
            "the {name} lock taken from product code the replica runs under its own \
             lock: a Product method, or a closure given to a Store read other than \
             with_state or view, must not call back into the Replica or its Store"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records<T>(m: &WatchedMutex<T>) -> Vec<Arc<Record>> {
        vec![m.record.clone()]
    }

    /// A lock held past the limit is named with its holder's thread and
    /// the call site where it was taken; a free lock and one held for
    /// less are not reported.
    #[test]
    fn a_lock_held_past_the_limit_is_reported_with_its_holder() {
        let lock = Arc::new(WatchedMutex::new("test-lock", 0u32));
        let limit = Duration::from_secs(60);
        assert_eq!(overdue(&records(&lock), limit, Instant::now()), None);

        let (taken, release) = (
            std::sync::mpsc::channel::<()>(),
            std::sync::mpsc::channel::<()>(),
        );
        let holder = {
            let lock = lock.clone();
            thread::Builder::new()
                .name("test-holder".into())
                .spawn(move || {
                    let _g = lock.lock();
                    taken.0.send(()).unwrap();
                    release.1.recv().unwrap();
                })
                .unwrap()
        };
        taken.1.recv().unwrap();
        assert_eq!(
            overdue(&records(&lock), limit, Instant::now()),
            None,
            "held for less than the limit"
        );

        let report = overdue(
            &records(&lock),
            limit,
            Instant::now() + Duration::from_secs(120),
        )
        .expect("held past the limit");
        assert!(report.contains("\"test-lock\""), "{report}");
        assert!(report.contains("test-holder"), "{report}");
        assert!(report.contains("held for"), "{report}");

        release.0.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(
            overdue(
                &records(&lock),
                limit,
                Instant::now() + Duration::from_secs(120)
            ),
            None,
            "released"
        );
    }

    /// A thread blocked on the lock is named in the report, so the other
    /// half of a lock cycle shows.
    #[test]
    fn a_waiter_is_named_in_the_report() {
        let lock = Arc::new(WatchedMutex::new("waited-on", ()));
        let guard = lock.lock();
        let waiter = {
            let lock = lock.clone();
            thread::Builder::new()
                .name("test-waiter".into())
                .spawn(move || drop(lock.lock()))
                .unwrap()
        };
        let report = loop {
            let r = overdue(
                &records(&lock),
                Duration::ZERO,
                Instant::now() + Duration::from_secs(120),
            );
            if r.as_deref().is_some_and(|r| r.contains("test-waiter")) {
                break r.unwrap();
            }
            thread::sleep(Duration::from_millis(5));
        };
        assert!(report.contains("has waited for it"), "{report}");
        drop(guard);
        waiter.join().unwrap();
    }

    /// The report gives the call site of the holder and of a waiter as
    /// file and line, the two being the lines that called `lock`.
    #[test]
    fn the_report_names_the_holder_and_waiter_call_sites() {
        /// A wrapper as `Contact::lock` is one: the site must be its caller's.
        struct Wrapped(Arc<WatchedMutex<()>>);
        impl Wrapped {
            #[track_caller]
            fn lock(&self) -> WatchedGuard<'_, ()> {
                self.0.lock()
            }
        }
        let inner = Arc::new(WatchedMutex::new("sited", ()));
        let lock = Arc::new(Wrapped(inner.clone()));
        let lock_records = records(&inner);
        let (held_guard, held_line) = (lock.lock(), line!());
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = {
            let lock = lock.clone();
            thread::spawn(move || {
                let (g, line) = (lock.lock(), line!());
                tx.send(line).unwrap();
                drop(g);
            })
        };
        // The waiter's line is known once it has the lock; until then it
        // waits at its own call site.
        let report = loop {
            let r = overdue(
                &lock_records,
                Duration::ZERO,
                Instant::now() + Duration::from_secs(120),
            );
            if r.as_deref().is_some_and(|r| r.contains("has waited")) {
                break r.unwrap();
            }
            thread::sleep(Duration::from_millis(5));
        };
        let file = file!();
        assert!(
            report.contains(&format!("{file}:{held_line}:")),
            "holder site {file}:{held_line} missing: {report}"
        );
        drop(held_guard);
        let waiter_line = rx.recv().unwrap();
        waiter.join().unwrap();
        assert!(
            report.contains(&format!("{file}:{waiter_line}:")),
            "waiter site {file}:{waiter_line} missing: {report}"
        );
        assert_ne!(held_line, waiter_line);
    }
}
