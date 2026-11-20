//! The log, vote, committed log id and snapshot on the replica's volume,
//! with the state machine beside them: openraft 0.9's [`RaftStorage`],
//! which [`openraft::storage::Adaptor`] splits into its log store and state
//! machine (the split traits are sealed without the `storage-v2` feature).
//!
//! `log` is an append-only file of frames, one per write, each a length,
//! a CRC-32C of its body and the body, the records of that write. A tail
//! that holds no whole frame, as a write cut short by a crash leaves, is
//! cut on open; a damaged frame with a whole one after it is corruption,
//! and the store does not open. Damage in the last frame, fsynced or not,
//! looks the same as a torn write, so bit rot there is cut too and drops
//! the acknowledged entries or vote it held. `snapshot` is replaced by
//! rename.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    AnyError, BasicNode, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::dedup::Limits;
use crate::entry::{Body, NodeId, Request, Response};
use crate::lock_watch::{
    ProductCode, WatchedGuard, WatchedMutex, WatchedReadGuard, WatchedRwLock, WatchedWriteGuard,
    assert_no_product_code,
};
use crate::snapshot::{
    StoredEntry, StoredLogId, StoredMembershipConfig, StoredPayload, StoredSnapshot, StoredVote,
};
use crate::state_machine::{Factory, Product, StateMachine};
use crate::{MAX_APPEND_BYTES, TypeConfig};

type Error = StorageError<NodeId>;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Record {
    Vote(StoredVote),
    Committed(Option<StoredLogId>),
    Entry(StoredEntry),
    /// Drops the entries from this index on.
    Truncate(u64),
    /// Drops the entries up to and including this id.
    Purge(StoredLogId),
}

struct Inner {
    dir: PathBuf,
    file: Arc<File>,
    vote: Option<StoredVote>,
    committed: Option<StoredLogId>,
    log: BTreeMap<u64, StoredEntry>,
    last_purged: Option<StoredLogId>,
    limits: Limits,
    snapshot: Option<StoredSnapshot>,
    snapshots_built: u64,
    /// Snapshots installed from a leader since the store was opened.
    installs: u64,
}

/// The state machine and what was applied to it, changed together.
struct Applied<P> {
    sm: StateMachine<P>,
    last_applied: Option<StoredLogId>,
    membership: StoredMembershipConfig,
}

thread_local! {
    /// The gates ([`Store::with_state`]) this thread holds, by address.
    static GATES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// A store's gate held by this thread, for reading or writing; released
/// on drop.
struct Gate<'a> {
    _held: (
        Option<WatchedReadGuard<'a, ()>>,
        Option<WatchedWriteGuard<'a, ()>>,
    ),
    key: usize,
}

impl Drop for Gate<'_> {
    fn drop(&mut self) {
        GATES.with(|g| {
            let mut g = g.borrow_mut();
            if let Some(i) = g.iter().rposition(|&k| k == self.key) {
                g.remove(i);
            }
        });
    }
}

/// One replica's storage. Clones share it.
///
/// File writes and fsyncs run off the async worker, without the lock that
/// readers of the state take; the in-memory state follows once a write
/// is done. openraft's adaptor makes one log or state machine call at a
/// time; a snapshot build runs beside them and `snapshots` orders it
/// with an install.
///
/// The applied state has a lock of its own, `applied`, which the replica
/// reads under its contact lock. [`Store::with_state`] and
/// [`Store::view`] take the `gate` for reading first, and every change to
/// the applied state takes it for writing: while a closure given to them
/// runs, no change waits for `applied`, so the replica's own reads of it
/// go on and the closure may call into the [`crate::Replica`]. A change
/// waiting for the gate holds off closures that come after it, so it
/// waits only for those already running. Locks are taken in the order
/// gate, `inner`, `applied`.
pub struct Store<P> {
    inner: Arc<WatchedMutex<Inner>>,
    applied: Arc<WatchedRwLock<Applied<P>>>,
    gate: Arc<WatchedRwLock<()>>,
    /// Builds the empty product a snapshot install loads into.
    factory: Factory<P>,
    /// The product's configuration hash, fixed when the store opened.
    config: u64,
    /// Called with this replica's hash and a refused snapshot's before
    /// the install fails: by default ends the process
    /// ([`Store::on_config_mismatch`]).
    on_mismatch: Arc<dyn Fn(u64, u64) + Send + Sync>,
    snapshots: Arc<tokio::sync::Mutex<()>>,
    /// Added to every fsynced write, for tests of a slow volume.
    sync_delay: std::time::Duration,
}

impl<P> Clone for Store<P> {
    fn clone(&self) -> Self {
        Store {
            inner: self.inner.clone(),
            applied: self.applied.clone(),
            gate: self.gate.clone(),
            factory: self.factory.clone(),
            config: self.config,
            on_mismatch: self.on_mismatch.clone(),
            snapshots: self.snapshots.clone(),
            sync_delay: self.sync_delay,
        }
    }
}

/// Exit status of a replica that refused a snapshot of another product
/// configuration: `EX_CONFIG`.
pub const CONFIG_MISMATCH_EXIT: i32 = 78;

/// Ends the process on a snapshot refused for its configuration, so the
/// refusal shows as a restarting replica rather than a raft core that
/// stopped behind a process still running. Its storage is untouched: a
/// restart under the set's configuration installs the snapshot.
fn exit_on_mismatch(ours: u64, theirs: u64) {
    eprintln!(
        "samplez-replica: a snapshot from a replica of product configuration {theirs} \
         is not this replica's, {ours}: exiting"
    );
    std::process::exit(CONFIG_MISMATCH_EXIT);
}

/// What a store opening under configuration `ours` warns of when its
/// snapshot was built under another one, `built_under`.
fn open_warning(ours: u64, built_under: Option<u64>) -> Option<String> {
    let theirs = built_under.filter(|&t| t != ours)?;
    Some(format!(
        "the stored snapshot was built under product configuration {theirs}, this replica \
         runs {ours}: expected once after a configuration change made with every replica \
         stopped; otherwise this replica ran under another configuration than the set's, \
         holds another state and is rebuilt from an empty volume"
    ))
}

/// Runs file work on tokio's blocking pool.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(io::Error::other)?
}

fn io_error<E: std::error::Error + 'static>(
    make: impl FnOnce(AnyError) -> StorageIOError<NodeId>,
    e: &E,
) -> Error {
    StorageError::IO {
        source: make(AnyError::new(e)),
    }
}

/// The CRC-32C (Castagnoli) table, reflected.
const CRC32C: [u32; 256] = {
    let mut table = [0; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
};

fn crc32c(bytes: &[u8]) -> u32 {
    !bytes.iter().fold(!0, |crc, &b| {
        CRC32C[((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8)
    })
}

/// A frame's length and checksum, before its body.
const HEAD: usize = 8;

/// Frames `records` as one, so a crash in the write that appends them
/// leaves one damaged frame at the tail, whichever of its pages reached
/// the disk.
fn encode(records: &[Record], out: &mut Vec<u8>) {
    let body = serde_json::to_vec(records).expect("records encode");
    let len = u32::try_from(body.len()).expect("frame under 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32c(&body).to_le_bytes());
    out.extend_from_slice(&body);
}

/// The body of the frame at `at` when a whole one is there: it fits and
/// its checksum matches. No frame is empty, so zeroes are none.
fn frame_at(bytes: &[u8], at: usize) -> Option<&[u8]> {
    let head = bytes.get(at..at + HEAD)?;
    let len = u32::from_le_bytes(head[..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().expect("4 bytes"));
    let body = bytes.get(at + HEAD..at + HEAD + len)?;
    (len > 0 && crc32c(body) == crc).then_some(body)
}

/// The records of `bytes` and the length their frames take. Only a crash
/// in an unsynced write leaves a damaged frame, and then nothing whole
/// after it: a damaged frame with a whole one after it is corruption that
/// cutting would turn into lost fsynced entries and votes.
fn decode(bytes: &[u8]) -> io::Result<(Vec<Record>, usize)> {
    let mut records = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let Some(body) = frame_at(bytes, at) else {
            if let Some(next) = (at + 1..bytes.len()).find(|&n| frame_at(bytes, n).is_some()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "the log frame at offset {at} is damaged, and of the {} bytes \
                         from there a whole frame starts at offset {next}",
                        bytes.len() - at
                    ),
                ));
            }
            break;
        };
        let frame: Vec<Record> = serde_json::from_slice(body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the log frame at offset {at}, {} bytes, does not decode: {e}",
                    body.len()
                ),
            )
        })?;
        records.extend(frame);
        at += HEAD + body.len();
    }
    Ok((records, at))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Writes `bytes` to `dir/name` by rename, so a crash leaves the old file
/// or the new one.
fn replace(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

impl<P: Product> Store<P> {
    /// The product's configuration hash
    /// ([`Product::config_hash`]).
    pub fn config_hash(&self) -> u64 {
        self.config
    }

    /// The configuration hash the stored snapshot was built under, as
    /// last built, installed or read at open; none without one or from a
    /// replica that recorded none.
    pub fn snapshot_config(&self) -> Option<u64> {
        self.lock().snapshot.as_ref().and_then(|s| s.config)
    }

    /// This store calling `f` with its own hash and the snapshot's when it
    /// refuses a snapshot of another product configuration, in place of
    /// ending the process with [`CONFIG_MISMATCH_EXIT`]; the install fails
    /// after it either way.
    pub fn on_config_mismatch(mut self, f: impl Fn(u64, u64) + Send + Sync + 'static) -> Store<P> {
        self.on_mismatch = Arc::new(f);
        self
    }

    /// Opens the storage in `dir`, creating it when absent, with an empty
    /// product `factory` builds as the state the snapshot (if any) is
    /// loaded into. Every snapshot installed later loads into one it
    /// builds as well, so the product keeps the replica's configuration.
    ///
    /// A snapshot sent by a replica of another product configuration
    /// ([`Product::config_hash`]) is refused, and the process exits with
    /// [`CONFIG_MISMATCH_EXIT`] unless [`Store::on_config_mismatch`] says
    /// otherwise: a learner catching up, or any member lagging, may receive
    /// one.
    pub fn open(dir: &Path, factory: Factory<P>, limits: Limits) -> io::Result<Store<P>> {
        fs::create_dir_all(dir)?;
        let mut sm = StateMachine::new(factory(), limits);
        let config = sm.product.config_hash();
        let mut last_applied = None;
        let mut membership = StoredMembershipConfig::default();
        let snapshot = match fs::read(dir.join("snapshot")) {
            Ok(bytes) => {
                let snapshot: StoredSnapshot =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                sm.load(snapshot.state.clone(), limits)
                    .map_err(io::Error::other)?;
                last_applied = snapshot.last_applied;
                sm.records_from = last_applied.map_or(0, |id| id.index);
                membership = snapshot.membership.clone();
                Some(snapshot)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let path = dir.join("log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let (records, whole) = decode(&bytes)?;
        if whole < bytes.len() {
            tracing::warn!(
                log = %path.display(),
                offset = whole,
                bytes = bytes.len() - whole,
                "cutting a torn tail off the log"
            );
            file.set_len(whole as u64)?;
            file.sync_all()?;
        }
        let mut inner = Inner {
            dir: dir.to_owned(),
            file: Arc::new(file),
            vote: None,
            committed: None,
            log: BTreeMap::new(),
            last_purged: None,
            limits,
            snapshot,
            snapshots_built: 0,
            installs: 0,
        };
        for record in records {
            inner.replay(record);
        }
        sync_dir(dir)?;
        if let Some(warning) = open_warning(config, inner.snapshot.as_ref().and_then(|s| s.config))
        {
            tracing::warn!("{warning}");
        }
        Ok(Store {
            inner: Arc::new(WatchedMutex::new("store", inner)),
            applied: Arc::new(WatchedRwLock::new(
                "applied",
                Applied {
                    sm,
                    last_applied,
                    membership,
                },
            )),
            gate: Arc::new(WatchedRwLock::new("state gate", ())),
            factory,
            config,
            on_mismatch: Arc::new(exit_on_mismatch),
            snapshots: Arc::default(),
            sync_delay: std::time::Duration::ZERO,
        })
    }

    /// This store with every fsynced write taking `delay` longer: a slow
    /// disk, for tests.
    pub fn with_sync_delay(mut self, delay: std::time::Duration) -> Store<P> {
        self.sync_delay = delay;
        self
    }

    /// Appends `records` to `log` as one frame, fsynced with `sync`.
    async fn write(&self, records: &[Record], sync: bool) -> io::Result<()> {
        if sync && !self.sync_delay.is_zero() {
            tokio::time::sleep(self.sync_delay).await;
        }
        let mut out = Vec::new();
        encode(records, &mut out);
        let file = self.lock().file.clone();
        blocking(move || {
            (&*file).write_all(&out)?;
            if sync { file.sync_data() } else { Ok(()) }
        })
        .await
    }

    /// Reads the state machine as applied up to an entry, never part way
    /// through one. `f` runs with no lock the replica's contact loop
    /// waits for, so it may call [`crate::Replica`] methods and this
    /// store's reads, `with_state` included. Closures run side by side;
    /// an entry is applied once those running when it arrived return.
    ///
    /// `f` must therefore not wait for an entry to be applied, as
    /// blocking on a `ReplicatedState::applied` or `read_index` future
    /// does, nor for another thread that reads this store with
    /// `with_state` or `view`: an apply arriving meanwhile waits for `f`
    /// and holds the other thread off, and neither ever returns. Not from
    /// a [`Product`] method: one the store calls panics here rather than
    /// deadlock. An apply or snapshot install on a thread inside `f`
    /// panics as well.
    pub fn with_state<T>(&self, f: impl FnOnce(&StateMachine<P>) -> T) -> T {
        self.view(|_, sm| f(sm))
    }

    /// [`Store::with_state`] with the index of the last entry applied.
    pub fn view<T>(&self, f: impl FnOnce(u64, &StateMachine<P>) -> T) -> T {
        let _gate = self.gate(false);
        let applied = self.read();
        f(applied.last_applied.map_or(0, |id| id.index), &applied.sm)
    }

    /// The product revision and the product's whole text there, for a
    /// client snapshot: built after the gate is released when the
    /// product gives [`Product::text_later`].
    pub fn text(&self) -> (u64, String) {
        let (revision, text) = self.with_state(|sm| {
            let text = sm.product.text_later().ok_or_else(|| sm.product.text());
            (sm.revision, text)
        });
        (revision, text.map_or_else(|text| text, |later| later()))
    }

    /// Reads the applied state without the gate, as the replica does
    /// under its contact lock: `f` runs as product code
    /// ([`ProductCode`]), so it must not call back into the replica.
    pub(crate) fn read_state<T>(&self, f: impl FnOnce(u64, &StateMachine<P>) -> T) -> T {
        let applied = self.read();
        let _product = ProductCode::enter();
        f(applied.last_applied.map_or(0, |id| id.index), &applied.sm)
    }

    /// The index of the last entry applied.
    pub fn applied_index(&self) -> u64 {
        self.read().last_applied.map_or(0, |id| id.index)
    }

    /// The applied index, the floor there and the engine state of the
    /// applied log ([`StateMachine::product_state`]), read at once.
    pub fn product_state(&self) -> (u64, u64, samplez::ProductState) {
        self.read_state(|n, sm| (n, sm.floor, sm.product_state(n)))
    }

    /// Whether the volume holds nothing of a set: no vote, entry or
    /// snapshot, as openraft finds a node it may initialise.
    pub fn pristine(&self) -> bool {
        let inner = self.lock();
        inner.vote.is_none()
            && inner.log.is_empty()
            && inner.last_purged.is_none()
            && inner.snapshot.is_none()
    }

    /// Whether the volume has joined a set, a committed id saved on it, and
    /// every entry up to that id, capped at the last entry held, is
    /// applied. A vote alone is not joining: a wiped volume saves one on
    /// first contact, before it holds any entry.
    pub fn caught_up(&self) -> bool {
        let inner = self.lock();
        if inner.committed.is_none() {
            return false;
        }
        let committed = inner.capped_committed().map_or(0, |id| id.index);
        self.applied_index() >= committed
    }
}

impl<P> Store<P> {
    /// The committed id saved, capped at the last entry held.
    pub fn committed(&self) -> Option<StoredLogId> {
        self.lock().capped_committed()
    }

    /// The id of the entry held at `index`; none for one purged or not
    /// held.
    pub fn log_id_at(&self, index: u64) -> Option<StoredLogId> {
        self.lock().log.get(&index).map(|e| e.log_id)
    }

    /// Calls `f` with the id and request of every application entry held
    /// above `index`, in log order.
    pub fn entries_after(&self, index: u64, mut f: impl FnMut(StoredLogId, &Request)) {
        let inner = self.lock();
        let _product = ProductCode::enter();
        for e in inner.log.range(index.saturating_add(1)..).map(|(_, e)| e) {
            if let StoredPayload::Normal(request) = &e.payload {
                f(e.log_id, request);
            }
        }
    }

    /// [`Store::entries_after`] for the first `max` application entries
    /// only, the rest of the log not read: true when more follow.
    pub fn entries_after_max(
        &self,
        index: u64,
        max: usize,
        mut f: impl FnMut(StoredLogId, &Request),
    ) -> bool {
        let inner = self.lock();
        let mut requests =
            (inner.log.range(index.saturating_add(1)..)).filter_map(|(_, e)| match &e.payload {
                StoredPayload::Normal(request) => Some((e.log_id, request)),
                _ => None,
            });
        let _product = ProductCode::enter();
        for (id, request) in requests.by_ref().take(max) {
            f(id, request);
        }
        requests.next().is_some()
    }

    /// The first decision applied on (`index`, `term`), by its log
    /// index.
    pub fn decided(&self, index: u64, term: u64) -> Option<u64> {
        self.read().sm.decided.get(&(index, term)).map(|&(d, _)| d)
    }

    /// The term of the entry at `index`: from the log, else from the
    /// state machine while the entry can still be decided.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        let inner = self.lock();
        inner
            .log
            .get(&index)
            .map(|e| e.log_id.term)
            .or_else(|| self.read().sm.terms.get(&index).map(|&(t, _)| t))
    }

    /// Snapshots installed from a leader since the store was opened.
    pub fn installs(&self) -> u64 {
        self.lock().installs
    }

    /// The index of the last purged log id, 0 for none.
    pub fn purged_index(&self) -> u64 {
        self.lock().last_purged.map_or(0, |id| id.index)
    }

    /// The restamps behind the entry at `index`, as applied.
    pub fn restamps(&self, index: u64) -> u32 {
        self.read().sm.restamps.get(&index).map_or(0, |&(n, _)| n)
    }

    /// Calls `f` with the log index and record of every entry after
    /// `after` up to `up_to` that derived engine entries and is held, in
    /// log order; false when one may no longer be held
    /// ([`StateMachine::records_whole`]).
    pub fn records(
        &self,
        after: u64,
        up_to: u64,
        mut f: impl FnMut(u64, &Arc<crate::state_machine::Record>),
    ) -> bool {
        let applied = self.read();
        let _product = ProductCode::enter();
        if after < up_to {
            for (index, record) in applied.sm.records.range(after + 1..=up_to) {
                f(*index, record);
            }
        }
        applied.sm.records_whole(after)
    }

    /// The log index after which the state machine holds every record
    /// ([`StateMachine::records_from`]).
    pub fn records_from(&self) -> u64 {
        self.read().sm.records_from
    }

    /// The newest entry at or below `records_from` that derived a record
    /// ([`StateMachine::record_floor`]).
    pub fn record_floor(&self) -> Option<u64> {
        self.read().sm.record_floor
    }

    /// The floor after every entry below `index`: the applied state's,
    /// raised over the entries held after it as the apply will raise it.
    /// The applied state's for an `index` at or below the applied one.
    pub fn floor_before(&self, index: u64) -> u64 {
        let inner = self.lock();
        let state = self.read();
        let applied = state.last_applied.map_or(0, |id| id.index);
        let mut floor = state.sm.floor;
        let end = index.max(applied + 1);
        for e in inner.log.range(applied + 1..end).map(|(_, e)| e) {
            if let StoredPayload::Normal(request) = &e.payload
                && let Some(at) = crate::state_machine::floored(floor, request)
            {
                floor = at;
            }
        }
        floor
    }

    /// Engine entries pending above `floor`, read in one hold: the records
    /// held that take effect above it, and the entries held unapplied
    /// whose index, as their apply will raise it, lies above it.
    pub fn pending_above(&self, floor: u64) -> usize {
        let inner = self.lock();
        let state = self.read();
        let applied = state.last_applied.map_or(0, |id| id.index);
        let records = (state.sm.records.values())
            .filter(|r| r.at_index > floor)
            .count();
        let mut raised = state.sm.floor;
        let mut unapplied = 0;
        for e in inner.log.range(applied + 1..).map(|(_, e)| e) {
            if let StoredPayload::Normal(request) = &e.payload
                && let Some(at) = crate::state_machine::floored(raised, request)
            {
                raised = at;
                unapplied += usize::from(at > floor);
            }
        }
        records + unapplied
    }

    /// Calls `f` with the log index and body of every entry the engine
    /// applies at a future index, as the state machine holds them, purged
    /// ones included.
    pub fn pending(&self, mut f: impl FnMut(u64, &Body)) {
        let applied = self.read();
        let _product = ProductCode::enter();
        for (index, body) in &applied.sm.pending {
            f(*index, body);
        }
    }

    #[track_caller]
    fn lock(&self) -> WatchedGuard<'_, Inner> {
        assert_no_product_code("store");
        self.inner.lock()
    }

    #[track_caller]
    fn read(&self) -> WatchedReadGuard<'_, Applied<P>> {
        assert_no_product_code("store");
        self.applied.read()
    }

    /// Changes to the applied state take the gate for writing first.
    #[track_caller]
    fn change(&self) -> WatchedWriteGuard<'_, Applied<P>> {
        assert_no_product_code("store");
        self.applied.write()
    }

    /// Takes the gate, for writing when `write`, or nothing when this
    /// thread holds it already: a `with_state` closure reading the store
    /// again, which a writer waiting for the gate would otherwise hold
    /// off. A writer on such a thread, an apply or install inside
    /// `with_state`, panics: it would wait for the state its own closure
    /// holds.
    #[track_caller]
    fn gate(&self, write: bool) -> Option<Gate<'_>> {
        assert_no_product_code("store");
        let key = Arc::as_ptr(&self.gate) as usize;
        if GATES.with(|g| g.borrow().contains(&key)) {
            assert!(
                !write,
                "an apply or install on a thread inside with_state would wait for itself"
            );
            return None;
        }
        let held = if write {
            (None, Some(self.gate.write()))
        } else {
            (Some(self.gate.read()), None)
        };
        GATES.with(|g| g.borrow_mut().push(key));
        Some(Gate { _held: held, key })
    }
}

impl Inner {
    fn capped_committed(&self) -> Option<StoredLogId> {
        match (self.committed, self.last_log_id()) {
            (Some(committed), Some(last)) if committed.index > last.index => Some(last),
            (Some(_), None) => None,
            (committed, _) => committed,
        }
    }

    fn replay(&mut self, record: Record) {
        match record {
            Record::Vote(vote) => self.vote = Some(vote),
            Record::Committed(committed) => self.committed = committed,
            Record::Entry(entry) => {
                self.log.insert(entry.log_id.index, entry);
            }
            Record::Truncate(since) => {
                self.log.split_off(&since);
            }
            Record::Purge(upto) => self.purge(upto),
        }
    }

    fn purge(&mut self, upto: StoredLogId) {
        self.log = self.log.split_off(&(upto.index + 1));
        self.last_purged = Some(upto);
    }

    /// `log` as it holds once the entries up to `upto` are purged.
    fn compacted(&self, upto: StoredLogId) -> Vec<u8> {
        let mut records = Vec::new();
        records.extend(self.vote.map(Record::Vote));
        records.push(Record::Committed(self.committed));
        records.push(Record::Purge(upto));
        records.extend(
            self.log
                .range(upto.index + 1..)
                .map(|(_, e)| Record::Entry(e.clone())),
        );
        let mut out = Vec::new();
        encode(&records, &mut out);
        out
    }

    fn last_log_id(&self) -> Option<StoredLogId> {
        self.log
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(self.last_purged)
    }
}

impl<P: Product> RaftLogReader<TypeConfig> for Store<P> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, Error> {
        Ok(self
            .lock()
            .log
            .range(range)
            .map(|(_, e)| e.into())
            .collect())
    }

    /// The entries of `[start, end)` up to [`MAX_APPEND_BYTES`] of
    /// commands beside the first, which is always taken: what one
    /// AppendEntries carries.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, Error> {
        let inner = self.lock();
        let mut bytes = 0;
        Ok(inner
            .log
            .range(start..end)
            .enumerate()
            .take_while(|(i, (_, e))| {
                bytes += command_bytes(e);
                *i == 0 || bytes <= MAX_APPEND_BYTES
            })
            .map(|(_, (_, e))| e.into())
            .collect())
    }
}

/// The command bytes ([`crate::entry::Command::size`]) an entry carries;
/// 0 for an entry without commands.
fn command_bytes(entry: &StoredEntry) -> usize {
    match &entry.payload {
        StoredPayload::Normal(Request {
            body: Body::Commands(commands),
            ..
        }) => commands.iter().map(|c| c.size()).sum(),
        _ => 0,
    }
}

impl<P: Product> RaftSnapshotBuilder<TypeConfig> for Store<P> {
    /// Takes the state under the lock and encodes and writes it after.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, Error> {
        let _building = self.snapshots.lock().await;
        let (dir, snapshot) = {
            let mut inner = self.lock();
            let applied = self.read();
            let _product = ProductCode::enter();
            inner.snapshots_built += 1;
            let last = applied.last_applied;
            // The build time keeps ids apart across restarts, where the
            // count starts again.
            let started = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos());
            let snapshot = StoredSnapshot {
                id: format!(
                    "{}-{started}-{}",
                    last.map_or(0, |id| id.index),
                    inner.snapshots_built
                ),
                last_applied: last,
                membership: applied.membership.clone(),
                state: applied.sm.save(),
                config: Some(self.config),
            };
            (inner.dir.clone(), snapshot)
        };
        let bytes = serde_json::to_vec(&snapshot).expect("snapshots encode");
        let bytes = blocking(move || replace(&dir, "snapshot", &bytes).map(|()| bytes))
            .await
            .map_err(|e| io_error(|a| StorageIOError::write_snapshot(None, a), &e))?;
        let meta = meta(&snapshot);
        self.lock().snapshot = Some(snapshot);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

fn meta(snapshot: &StoredSnapshot) -> SnapshotMeta<NodeId, BasicNode> {
    SnapshotMeta {
        last_log_id: snapshot.last_applied.map(LogId::from),
        last_membership: snapshot.membership.stored(),
        snapshot_id: snapshot.id.clone(),
    }
}

impl<P: Product> RaftStorage<TypeConfig> for Store<P> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), Error> {
        let vote = StoredVote::from(vote);
        self.write(&[Record::Vote(vote)], true)
            .await
            .map_err(|e| io_error(StorageIOError::write_vote, &e))?;
        self.lock().vote = Some(vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, Error> {
        Ok(self.lock().vote.map(Vote::from))
    }

    /// Written with no fsync of its own: the next log write's fsync covers
    /// it, and a process that dies before keeps it in the page cache.
    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), Error> {
        let committed = committed.as_ref().map(StoredLogId::from);
        self.write(&[Record::Committed(committed)], false)
            .await
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        self.lock().committed = committed;
        Ok(())
    }

    /// The saved id, capped at the last entry the store holds, so it never
    /// names entries a crash lost.
    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, Error> {
        Ok(self.lock().capped_committed().map(LogId::from))
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, Error> {
        let inner = self.lock();
        Ok(LogState {
            last_purged_log_id: inner.last_purged.map(LogId::from),
            last_log_id: inner.last_log_id().map(LogId::from),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
    {
        let entries: Vec<StoredEntry> =
            entries.into_iter().map(|e| StoredEntry::from(&e)).collect();
        let records: Vec<Record> = entries.iter().cloned().map(Record::Entry).collect();
        self.write(&records, true)
            .await
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        let mut inner = self.lock();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), Error> {
        self.write(&[Record::Truncate(log_id.index)], true)
            .await
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        self.lock().log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), Error> {
        let upto = StoredLogId::from(&log_id);
        let (dir, out) = {
            let inner = self.lock();
            (inner.dir.clone(), inner.compacted(upto))
        };
        let file = blocking(move || {
            replace(&dir, "log", &out)?;
            OpenOptions::new().append(true).open(dir.join("log"))
        })
        .await
        .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        let mut inner = self.lock();
        inner.purge(upto);
        inner.file = Arc::new(file);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), Error> {
        let applied = self.read();
        Ok((
            applied.last_applied.map(LogId::from),
            applied.membership.stored(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<TypeConfig>],
    ) -> Result<Vec<Response>, Error> {
        let _gate = self.gate(true);
        let mut applied = self.change();
        let _product = ProductCode::enter();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            let id = StoredLogId::from(&entry.log_id);
            applied.last_applied = Some(id);
            responses.push(match &entry.payload {
                EntryPayload::Blank => Response::default(),
                EntryPayload::Normal(request) => applied.sm.apply_at(id.index, id.term, request),
                EntryPayload::Membership(membership) => {
                    applied.membership = StoredMembershipConfig {
                        log_id: Some(id),
                        ..StoredMembershipConfig::from(membership)
                    };
                    Response::default()
                }
            });
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, Error> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), Error> {
        let bytes = snapshot.into_inner();
        let read_error = |e: &dyn std::fmt::Display| StorageError::IO {
            source: StorageIOError::read_snapshot(
                Some(meta.signature()),
                AnyError::error(e.to_string()),
            ),
        };
        let stored: StoredSnapshot = serde_json::from_slice(&bytes).map_err(|e| read_error(&e))?;
        // A state applied under another configuration is another state:
        // this replica stops rather than load it.
        if let Some(theirs) = stored.config
            && theirs != self.config
        {
            tracing::error!(
                ours = self.config,
                theirs,
                "a snapshot from a replica of another product configuration"
            );
            (self.on_mismatch)(self.config, theirs);
            return Err(read_error(&format!(
                "the snapshot's product configuration hash {theirs} is not this \
                 replica's, {}",
                self.config
            )));
        }
        let _installing = self.snapshots.lock().await;
        // Loaded beside the state, then written, then swapped in: a
        // snapshot that does not load or a failed write leaves the state
        // and the file as they were.
        let (dir, limits) = {
            let inner = self.lock();
            (inner.dir.clone(), inner.limits)
        };
        let mut sm = StateMachine::new((self.factory)(), limits);
        sm.load(stored.state.clone(), limits)
            .map_err(|e| read_error(&e))?;
        sm.records_from = stored.last_applied.map_or(0, |id| id.index);
        blocking(move || replace(&dir, "snapshot", &bytes))
            .await
            .map_err(|e| {
                io_error(
                    |a| StorageIOError::write_snapshot(Some(meta.signature()), a),
                    &e,
                )
            })?;
        let gate = self.gate(true);
        let mut inner = self.lock();
        let mut applied = self.change();
        let old = std::mem::replace(&mut applied.sm, sm);
        applied.last_applied = stored.last_applied;
        applied.membership = stored.membership.clone();
        inner.snapshot = Some(stored);
        inner.installs += 1;
        drop((applied, inner, gate));
        // The product replaced is dropped with no lock held.
        drop(old);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, Error> {
        // Sent under this replica's configuration: one changed with every
        // replica stopped applies to the saved state as its load does.
        let snapshot = self.lock().snapshot.clone().map(|s| StoredSnapshot {
            config: Some(self.config),
            ..s
        });
        Ok(snapshot.map(|snapshot| Snapshot {
            meta: meta(&snapshot),
            snapshot: Box::new(Cursor::new(
                serde_json::to_vec(&snapshot).expect("snapshots encode"),
            )),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::entry::{Body, Request};
    use crate::state_machine::tests::{Paths, command};
    use openraft::CommittedLeaderId;

    fn dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("samplez-replica-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &Path) -> Store<Paths> {
        Store::open(dir, Arc::new(Paths::default), Limits::defaults(48_000)).expect("opens")
    }

    /// A `with_state` closure reads the store again, `with_state`
    /// included, while another thread applies: the apply waits for the
    /// closure, and the closure's reads do not wait for the apply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_with_state_closure_reads_the_store_while_an_apply_waits() {
        let dir = dir("with-state-reads");
        let mut store = open(&dir);
        let (entered, inside) = std::sync::mpsc::channel();
        let reader = store.clone();
        let gate = store.gate.record();
        let read = std::thread::spawn(move || {
            reader.with_state(|_| {
                entered.send(()).unwrap();
                // Read only once the apply waits for the gate.
                let deadline = Instant::now() + Duration::from_secs(5);
                while gate.waiters() == 0 {
                    assert!(Instant::now() < deadline, "the apply never waited");
                    std::thread::sleep(Duration::from_millis(1));
                }
                (
                    reader.applied_index(),
                    reader.committed(),
                    reader.with_state(|sm| sm.floor),
                    reader.product_state().0,
                )
            })
        });
        inside.recv().unwrap();
        store.append_to_log(vec![entry(1)]).await.unwrap();
        store.apply_to_state_machine(&[entry(1)]).await.unwrap();
        let (applied, _, _, at) = read.join().unwrap();
        assert_eq!(
            (applied, at),
            (0, 0),
            "the closure saw the state before the apply"
        );
        assert_eq!(store.applied_index(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A product the size of a large mixer: its text is its whole state,
    /// built under the state or, `later`, from a clone of its `Arc`.
    struct Mixer {
        state: Arc<serde_json::Value>,
        later: bool,
    }

    impl Mixer {
        fn new(later: bool) -> Mixer {
            let input = |i: usize| {
                serde_json::json!({
                    "name": format!("input {i}"), "gain": -3.5, "trim": 0.0, "pan": 0.0,
                    "mute": false, "solo": false, "phase": false, "delay": 0.0,
                    "eq": (0..4).map(|b| serde_json::json!({"f": 100.0 * f64::from(b + 1),
                        "g": 0.0, "q": 0.7, "on": true})).collect::<Vec<_>>(),
                    "dyn": {"threshold": -20.0, "ratio": 4.0, "attack": 5.0, "release": 80.0},
                    "sends": (0..32).map(|_| serde_json::json!({"level": -90.0, "on": false}))
                        .collect::<Vec<_>>(),
                })
            };
            let output = |o: usize| {
                serde_json::json!({
                    "name": format!("output {o}"), "level": 0.0, "mute": false,
                    "delay": 0.0, "limiter": {"on": true, "ceiling": -1.0},
                    "source": format!("bus {o}"),
                })
            };
            let state = serde_json::json!({
                "inputs": (0..384).map(input).collect::<Vec<_>>(),
                "outputs": (0..256).map(output).collect::<Vec<_>>(),
            });
            Mixer {
                state: Arc::new(state),
                later,
            }
        }
    }

    impl Product for Mixer {
        type Change = ();
        fn apply(
            &mut self,
            _: &crate::entry::Origin,
            _: &[u8],
            _: &crate::state_machine::Apply,
        ) -> crate::state_machine::Effect<()> {
            crate::state_machine::Effect::Unchanged
        }
        fn expire(&mut self, _: &crate::entry::SessionKey) -> crate::state_machine::Effect<()> {
            crate::state_machine::Effect::Unchanged
        }
        fn forget(&mut self, _: &crate::entry::SessionKey) {}
        fn delta(&self, _: u64, _: &[crate::state_machine::Changed<'_, ()>]) -> String {
            String::new()
        }
        fn text(&self) -> String {
            serde_json::json!({ "state": *self.state }).to_string()
        }
        fn text_later(&self) -> Option<Box<dyn FnOnce() -> String + Send>> {
            let state = self.state.clone();
            self.later.then(|| {
                Box::new(move || {
                    assert!(
                        GATES.with(|g| g.borrow().is_empty()),
                        "the text is built under the gate"
                    );
                    serde_json::json!({ "state": *state }).to_string()
                }) as Box<dyn FnOnce() -> String + Send>
            })
        }
        fn save(&self) -> Vec<u8> {
            Vec::new()
        }
        fn load(&mut self, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn config_hash(&self) -> u64 {
            0
        }
    }

    fn mixer(name: &str, later: bool) -> (PathBuf, Store<Mixer>) {
        let dir = dir(name);
        let factory = Arc::new(move || Mixer::new(later));
        let store = Store::open(&dir, factory, Limits::defaults(48_000)).unwrap();
        (dir, store)
    }

    /// A client snapshot of a product that gives `text_later` builds the
    /// text once the gate is released, and the same text as `text`.
    #[test]
    fn a_snapshot_builds_a_later_text_after_the_gate() {
        let (dir, store) = mixer("text-later", true);
        let (_, text) = store.text();
        assert_eq!(text, store.with_state(|sm| sm.product.text()));
        let _ = fs::remove_dir_all(&dir);
    }

    /// How long a client snapshot holds the gate on a large state, with
    /// the text built under it and with `text_later`.
    #[test]
    #[ignore = "a measurement: prints how long a snapshot holds the gate"]
    fn snapshot_text_gate_hold() {
        let quantiles = |mut d: Vec<Duration>| {
            d.sort();
            format!(
                "min {:?} median {:?} max {:?}",
                d[0],
                d[d.len() / 2],
                d[d.len() - 1]
            )
        };
        let (under_dir, under) = mixer("gate-hold-under", false);
        let (later_dir, later) = mixer("gate-hold-later", true);
        let (mut held, mut gate, mut whole) = (Vec::new(), Vec::new(), Vec::new());
        let mut bytes = 0;
        for _ in 0..50 {
            let started = Instant::now();
            bytes = under.text().1.len();
            held.push(started.elapsed());
            let started = Instant::now();
            let text = later.with_state(|sm| sm.product.text_later());
            gate.push(started.elapsed());
            drop(text);
            let started = Instant::now();
            later.text();
            whole.push(started.elapsed());
        }
        eprintln!("text {bytes} bytes");
        eprintln!("text under the gate: gate held {}", quantiles(held));
        eprintln!("text_later: gate held {}", quantiles(gate));
        eprintln!("text_later: whole snapshot {}", quantiles(whole));
        let _ = fs::remove_dir_all(&under_dir);
        let _ = fs::remove_dir_all(&later_dir);
    }

    /// Two `with_state` closures share the gate: the second starts while
    /// the first runs. This holds only while no apply is pending, which
    /// would hold the second off; a closure waiting for another thread's
    /// read, as here, is the pattern `with_state` forbids, and the test
    /// shows no more than that readers share the gate.
    #[test]
    fn with_state_closures_run_side_by_side() {
        let dir = dir("with-state-side-by-side");
        let store = open(&dir);
        let (started, second) = std::sync::mpsc::channel();
        let other = store.clone();
        store.with_state(|_| {
            std::thread::spawn(move || other.with_state(|_| started.send(()).unwrap()));
            second
                .recv_timeout(Duration::from_secs(5))
                .expect("the second closure waited for the first");
        });
        let _ = fs::remove_dir_all(&dir);
    }

    /// An apply on a thread inside `with_state` panics rather than wait
    /// for the state its own closure holds.
    #[test]
    #[should_panic(expected = "an apply or install on a thread inside with_state")]
    fn an_apply_inside_with_state_panics() {
        let dir = dir("apply-inside-with-state");
        let store = open(&dir);
        let worker = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            store.with_state(|_| {
                let mut store = store.clone();
                runtime
                    .block_on(store.apply_to_state_machine(&[entry(1)]))
                    .is_ok()
            })
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !worker.is_finished() {
            assert!(
                Instant::now() < deadline,
                "the apply deadlocked on its own thread"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        if let Err(panic) = worker.join() {
            std::panic::resume_unwind(panic);
        }
    }

    /// A closure the store runs under its lock, as it runs product code,
    /// that calls back into the store panics rather than deadlock.
    #[test]
    #[should_panic(expected = "lock taken from product code")]
    fn a_read_from_product_code_under_the_lock_panics() {
        let dir = dir("product-code-reads");
        let store = open(&dir);
        store.read_state(|_, _| store.committed());
    }

    /// `with_state` from product code under the lock panics as well: an
    /// apply holds the gate and the state, and would wait for itself.
    #[test]
    #[should_panic(expected = "lock taken from product code")]
    fn with_state_from_product_code_under_the_lock_panics() {
        let dir = dir("product-code-with-state");
        let store = open(&dir);
        store.read_state(|_, _| store.with_state(|sm| sm.floor));
    }

    fn entry(index: u64) -> openraft::Entry<TypeConfig> {
        openraft::Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(Request {
                stamp: index * 48,
                body: Body::Commands(vec![command(
                    "s",
                    &index.to_string(),
                    &format!("p=v@{index}"),
                )]),
            }),
        }
    }

    fn id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(1, 0), index)
    }

    /// Replaying a long log with no snapshot holds no watched lock: the
    /// state is built and replayed before the store's lock exists, so the
    /// watchdog cannot fire on a restart however long the log is. A lock
    /// held for as long as the open took does exceed a limit set under it.
    #[tokio::test]
    async fn opening_a_long_log_holds_no_watched_lock() {
        let dir = dir("long-log");
        let mut store = open(&dir);
        let entries: Vec<_> = (1..=6000).map(entry).collect();
        store.append_to_log(entries).await.expect("append");
        drop(store);

        let before: Vec<_> = crate::lock_watch::live();
        let known = |r: &Arc<crate::lock_watch::Record>| before.iter().any(|b| Arc::ptr_eq(b, r));
        // Other tests hold their own locks meanwhile: only this thread's count.
        let opener = std::thread::current().name().expect("named").to_owned();
        let opened = std::sync::atomic::AtomicBool::new(false);
        let sampled_held = std::sync::atomic::AtomicBool::new(false);
        let took = std::thread::scope(|scope| {
            // Any watched lock held by the opening thread.
            scope.spawn(|| {
                while !opened.load(std::sync::atomic::Ordering::Relaxed) {
                    let new: Vec<_> = crate::lock_watch::live()
                        .into_iter()
                        .filter(|r| !known(r))
                        .collect();
                    if crate::lock_watch::overdue(
                        &new,
                        Duration::ZERO,
                        Instant::now() + Duration::from_secs(1),
                    )
                    .is_some_and(|r| r.contains(&opener))
                    {
                        sampled_held.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
            let started = Instant::now();
            let store = open(&dir);
            let took = started.elapsed();
            opened.store(true, std::sync::atomic::Ordering::Relaxed);
            drop(store);
            took
        });
        assert!(!sampled_held.load(std::sync::atomic::Ordering::Relaxed));

        // The control: the same duration under a watched lock trips a
        // limit set below it.
        let control = crate::lock_watch::WatchedMutex::new("control", ());
        let _held = control.lock();
        let report =
            crate::lock_watch::overdue(&[control.record()], took / 2, Instant::now() + took);
        assert!(report.is_some_and(|r| r.contains("control")));
        fs::remove_dir_all(&dir).ok();
    }

    /// Contact's floor at append is the one the apply reaches: the
    /// applied state's, raised over the entries held after it as they
    /// will apply; and the records the apply derives are read back at
    /// it.
    #[tokio::test]
    async fn the_floor_before_an_entry_is_the_one_its_apply_reaches() {
        let dir = dir("floor");
        let mut store = open(&dir);
        let down = |index, at_index| openraft::Entry {
            log_id: id(index),
            payload: EntryPayload::Normal(Request {
                stamp: 0,
                body: Body::InputDown { slot: 0, at_index },
            }),
        };
        let entries = vec![down(1, 4800), down(2, 960), down(3, 9600)];
        store.append_to_log(entries.clone()).await.expect("append");
        store
            .apply_to_state_machine(&entries[..1])
            .await
            .expect("apply");
        assert_eq!(
            [
                store.floor_before(2),
                store.floor_before(3),
                store.floor_before(4)
            ],
            [4800, 4800, 9600]
        );
        // Entry 2 asks for 960 and applies at 4800: pending above 2400,
        // as are the record of entry 1 and entry 3.
        assert_eq!(store.pending_above(2400), 3);
        store
            .apply_to_state_machine(&entries[1..])
            .await
            .expect("apply");
        assert_eq!(store.with_state(|sm| sm.floor), 9600);
        let mut records = Vec::new();
        assert!(store.records(0, 3, |i, r| records.push((i, r.at_index))));
        assert_eq!(records, [(1, 4800), (2, 4800), (3, 9600)]);
        // Read as held, not copied.
        let mut shared = Vec::new();
        store.records(0, 3, |i, r| shared.push((i, Arc::clone(r))));
        store.with_state(|sm| {
            for (i, r) in &shared {
                assert!(Arc::ptr_eq(r, &sm.records[i]));
            }
        });
    }

    /// One AppendEntries takes every entry written since the last up to
    /// [`MAX_APPEND_BYTES`] of commands, so a follower whose fsync takes
    /// longer than the gap between small entries catches up in one; and
    /// at least the first entry, whatever its size.
    #[tokio::test]
    async fn an_append_carries_entries_up_to_its_byte_bound() {
        let dir = dir("limited");
        let mut store = open(&dir);
        // An entry of one command of `size` bytes.
        let sized = |index: u64, size: usize| {
            let mut c = command("s", &index.to_string(), "");
            c.body = vec![b'x'; size - c.size()];
            assert_eq!(c.size(), size);
            openraft::Entry {
                log_id: id(index),
                payload: EntryPayload::Normal(Request {
                    stamp: 0,
                    body: Body::Commands(vec![c]),
                }),
            }
        };
        let quarter = MAX_APPEND_BYTES / 4;
        let entries = (1..=64)
            .map(|i| sized(i, 100))
            .chain((65..=70).map(|i| sized(i, quarter)))
            .chain([sized(71, MAX_APPEND_BYTES + 1), sized(72, 100)]);
        store.append_to_log(entries).await.expect("append");
        let taken = async |store: &mut Store<Paths>, start, end| {
            let entries = store.limited_get_log_entries(start, end).await;
            (entries.expect("entries").iter())
                .map(|e| e.log_id.index)
                .collect::<Vec<u64>>()
        };
        assert_eq!(taken(&mut store, 1, 65).await, (1..=64).collect::<Vec<_>>());
        assert_eq!(taken(&mut store, 65, 73).await, [65, 66, 67, 68]);
        assert_eq!(taken(&mut store, 64, 73).await, [64, 65, 66, 67]);
        assert_eq!(taken(&mut store, 71, 73).await, [71]);
        assert_eq!(taken(&mut store, 72, 73).await, [72]);
    }

    #[tokio::test]
    async fn log_vote_and_committed_survive_a_reopen() {
        let dir = dir("reopen");
        let mut store = open(&dir);
        store
            .save_vote(&Vote::new_committed(1, 0))
            .await
            .expect("vote");
        store
            .append_to_log((1..=5).map(entry))
            .await
            .expect("append");
        store.save_committed(Some(id(4))).await.expect("committed");
        store
            .delete_conflict_logs_since(id(5))
            .await
            .expect("truncate");
        drop(store);
        let mut store = open(&dir);
        assert_eq!(
            store.read_vote().await.expect("vote"),
            Some(Vote::new_committed(1, 0))
        );
        assert_eq!(
            store.read_committed().await.expect("committed"),
            Some(id(4))
        );
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_log_id, Some(id(4)));
        let entries = store.try_get_log_entries(1..).await.expect("entries");
        assert_eq!(entries.len(), 4);
    }

    #[tokio::test]
    async fn read_committed_is_capped_at_the_last_entry_held() {
        let dir = dir("cap");
        let mut store = open(&dir);
        store
            .append_to_log((1..=3).map(entry))
            .await
            .expect("append");
        // A committed id past the entries the store holds, as when the
        // entries after a commit were lost and the id was not.
        store.save_committed(Some(id(5))).await.expect("committed");
        assert_eq!(
            store.read_committed().await.expect("committed"),
            Some(id(3))
        );
    }

    #[tokio::test]
    async fn a_torn_tail_is_dropped_and_appends_continue_after_it() {
        let dir = dir("torn");
        let mut store = open(&dir);
        store
            .append_to_log((1..=3).map(entry))
            .await
            .expect("append");
        drop(store);
        let mut log = OpenOptions::new()
            .append(true)
            .open(dir.join("log"))
            .expect("log");
        log.write_all(&[200, 0, 0, 0, b'{']).expect("torn write");
        drop(log);
        let mut store = open(&dir);
        store.append_to_log([entry(4)]).await.expect("append");
        drop(store);
        let mut store = open(&dir);
        let entries = store.try_get_log_entries(1..).await.expect("entries");
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
    }

    #[test]
    fn crc32c_matches_the_castagnoli_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[tokio::test]
    async fn a_zero_filled_tail_is_cut() {
        let dir = dir("zeroes");
        let mut store = open(&dir);
        store
            .append_to_log((1..=2).map(entry))
            .await
            .expect("append");
        drop(store);
        let len = fs::metadata(dir.join("log")).expect("log").len();
        let log = OpenOptions::new()
            .append(true)
            .open(dir.join("log"))
            .expect("log");
        log.set_len(len + 4096).expect("extend");
        drop(log);
        let mut store = open(&dir);
        assert_eq!(fs::metadata(dir.join("log")).expect("log").len(), len);
        assert_eq!(
            store.try_get_log_entries(1..).await.expect("entries").len(),
            2
        );
    }

    /// A damaged frame with whole ones after it is not a torn tail:
    /// cutting it would drop fsynced entries, so the store does not open
    /// and leaves the file as it is.
    #[tokio::test]
    async fn a_damaged_frame_before_whole_ones_refuses_to_open() {
        let dir = dir("damaged");
        let mut store = open(&dir);
        for index in 1..=3 {
            store.append_to_log([entry(index)]).await.expect("append");
        }
        drop(store);
        let mut bytes = fs::read(dir.join("log")).expect("log");
        let len = bytes.len();
        bytes[HEAD + 2] ^= 0x20;
        fs::write(dir.join("log"), &bytes).expect("damage");
        let Err(e) = Store::open(&dir, Arc::new(Paths::default), Limits::defaults(48_000)) else {
            panic!("opened a damaged log");
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("offset 0"), "{e}");
        assert_eq!(
            fs::metadata(dir.join("log")).expect("log").len() as usize,
            len
        );
    }

    /// A crash in a write of several records can leave a later page of
    /// it on disk and not an earlier one. The write is one frame, so that
    /// is a torn tail and cut, not a damaged frame before whole ones.
    #[tokio::test]
    async fn a_torn_write_of_several_records_is_cut() {
        let dir = dir("torn-batch");
        let mut store = open(&dir);
        store
            .append_to_log((1..=2).map(entry))
            .await
            .expect("append");
        let synced = fs::metadata(dir.join("log")).expect("log").len() as usize;
        store
            .append_to_log((3..=6).map(entry))
            .await
            .expect("append");
        drop(store);
        let mut bytes = fs::read(dir.join("log")).expect("log");
        bytes[synced + HEAD..synced + HEAD + 32].fill(0);
        fs::write(dir.join("log"), &bytes).expect("tear");
        let mut store = open(&dir);
        assert_eq!(
            fs::metadata(dir.join("log")).expect("log").len() as usize,
            synced
        );
        let entries = store.try_get_log_entries(1..).await.expect("entries");
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            [1, 2]
        );
    }

    /// A flipped bit that leaves the body valid JSON is caught by the
    /// checksum, not read back as another entry.
    #[tokio::test]
    async fn a_frame_that_still_parses_is_caught_by_its_checksum() {
        let dir = dir("bitflip");
        let mut store = open(&dir);
        for index in 1..=2 {
            store.append_to_log([entry(index)]).await.expect("append");
        }
        drop(store);
        let mut bytes = fs::read(dir.join("log")).expect("log");
        let at = bytes
            .windows(10)
            .position(|w| w == b"\"stamp\":48")
            .expect("first entry's stamp")
            + 8;
        bytes[at] = b'5';
        fs::write(dir.join("log"), &bytes).expect("damage");
        let Err(e) = Store::open(&dir, Arc::new(Paths::default), Limits::defaults(48_000)) else {
            panic!("read a damaged frame back");
        };
        assert!(e.to_string().contains("offset 0"), "{e}");
    }

    #[tokio::test]
    async fn a_purge_compacts_the_file_and_survives_a_reopen() {
        let dir = dir("purge");
        let mut store = open(&dir);
        store
            .append_to_log((1..=6).map(entry))
            .await
            .expect("append");
        let before = fs::metadata(dir.join("log")).expect("log").len();
        store.purge_logs_upto(id(4)).await.expect("purge");
        assert!(fs::metadata(dir.join("log")).expect("log").len() < before);
        drop(store);
        let mut store = open(&dir);
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, Some(id(4)));
        assert_eq!(state.last_log_id, Some(id(6)));
        assert_eq!(
            store.try_get_log_entries(1..).await.expect("entries").len(),
            2
        );
    }

    /// A purge whose file cannot be written leaves the entries readable,
    /// as the log on disk still holds them.
    #[tokio::test]
    async fn a_failed_purge_leaves_the_log_alone() {
        use std::os::unix::fs::PermissionsExt;
        let dir = dir("purge-readonly");
        let mut store = open(&dir);
        store
            .append_to_log((1..=6).map(entry))
            .await
            .expect("append");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("read-only");
        let purged = store.purge_logs_upto(id(4)).await;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("writable");
        assert!(purged.is_err());
        let state = store.get_log_state().await.expect("state");
        assert_eq!(state.last_purged_log_id, None);
        assert_eq!(
            store.try_get_log_entries(1..).await.expect("entries").len(),
            6
        );
    }

    #[tokio::test]
    async fn caught_up_once_applied_up_to_the_capped_committed_id() {
        let dir = dir("caught-up");
        let mut store = open(&dir);
        assert!(!store.caught_up(), "an empty volume has joined nothing");
        // A wiped volume saves the leader's vote on first contact, before
        // any committed id reaches it.
        store
            .save_vote(&Vote::new_committed(1, 0))
            .await
            .expect("vote");
        assert!(!store.caught_up(), "a vote alone is not joining");
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.save_committed(Some(id(2))).await.expect("committed");
        store
            .apply_to_state_machine(&entries[..1])
            .await
            .expect("apply");
        assert!(!store.caught_up(), "entry 2 is committed, not applied");
        store
            .apply_to_state_machine(&entries[1..2])
            .await
            .expect("apply");
        assert!(store.caught_up(), "entry 3 is not committed");
        // An id past the entries held counts up to the last one.
        store.save_committed(Some(id(9))).await.expect("committed");
        assert!(!store.caught_up());
        store
            .apply_to_state_machine(&entries[2..])
            .await
            .expect("apply");
        assert!(store.caught_up());
    }

    #[tokio::test]
    async fn a_built_snapshot_restores_the_state_on_open_and_on_install() {
        let dir = dir("snap");
        let mut store = open(&dir);
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let snapshot = store.build_snapshot().await.expect("build");
        let saved = store.with_state(|sm| sm.save());
        drop(store);
        let store = open(&dir);
        assert_eq!(store.applied_index(), 3);
        assert_eq!(store.with_state(|sm| sm.save()), saved);

        let mut other = open(&super::tests::dir("snap-install"));
        other
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");
        assert_eq!(other.with_state(|sm| sm.save()), saved);
        assert_eq!(other.applied_index(), 3);
    }

    #[tokio::test]
    async fn snapshot_ids_do_not_repeat_after_a_restart() {
        let dir = dir("snap-ids");
        let mut store = open(&dir);
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let first = store.build_snapshot().await.expect("build").meta;
        drop(store);
        let mut store = open(&dir);
        let second = store.build_snapshot().await.expect("build").meta;
        assert_eq!(first.last_log_id, second.last_log_id);
        assert_ne!(first.snapshot_id, second.snapshot_id);
    }

    fn open_as(dir: &Path, config: u64) -> Store<Paths> {
        let factory: Factory<Paths> = Arc::new(move || Paths {
            config,
            ..Paths::default()
        });
        Store::open(dir, factory, Limits::defaults(48_000)).expect("opens")
    }

    /// An installed snapshot loads into a product the store's factory
    /// built, so the replica keeps its configuration: a product built
    /// empty by its `Default` would hold none.
    #[tokio::test]
    async fn an_installed_snapshot_keeps_the_replicas_configuration() {
        let mut store = open_as(&dir("cfg-from"), 7);
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let snapshot = store.build_snapshot().await.expect("build");
        let mut other = open_as(&dir("cfg-to"), 7);
        other
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");
        assert_eq!(other.applied_index(), 3);
        assert_eq!(other.with_state(|sm| sm.product.config), 7);
        assert_eq!(other.config_hash(), 7);
    }

    /// A snapshot from a replica of another product configuration is not
    /// installed: the process would end, the state and the volume stay as
    /// they were, and the replica restarted under the set's configuration
    /// installs it.
    #[tokio::test]
    async fn a_snapshot_of_another_configuration_is_refused() {
        let mut store = open_as(&dir("cfg-other-from"), 8);
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let snapshot = store.build_snapshot().await.expect("build");
        let to = dir("cfg-other-to");
        let ended = Arc::new(Mutex::new(Vec::new()));
        let seen = ended.clone();
        let mut other = open_as(&to, 7).on_config_mismatch(move |ours, theirs| {
            seen.lock().unwrap().push((ours, theirs));
        });
        let before = other.with_state(|sm| sm.save());
        let bytes = snapshot.snapshot.into_inner();
        let installed = other
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
            .await;
        assert!(installed.is_err());
        assert_eq!(*ended.lock().unwrap(), [(7, 8)], "the process ends");
        assert_eq!(other.applied_index(), 0);
        assert_eq!(other.with_state(|sm| sm.save()), before);
        assert_eq!(other.installs(), 0);
        drop(other);
        let mut restarted = open_as(&to, 8);
        restarted
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes)))
            .await
            .expect("installs under the set's configuration");
        assert_eq!(restarted.applied_index(), 3);
    }

    /// The open warning names both hashes and both cases: expected after
    /// a change made with every replica stopped, else a rebuild.
    #[test]
    fn a_snapshot_built_under_another_configuration_warns_at_open() {
        assert_eq!(open_warning(7, None), None);
        assert_eq!(open_warning(7, Some(7)), None);
        let w = open_warning(9, Some(7)).expect("a warning");
        assert!(w.contains("configuration 7, this replica runs 9"), "{w}");
        assert!(w.contains("every replica stopped"), "{w}");
        assert!(w.contains("empty volume"), "{w}");
    }

    /// A snapshot or a report from a replica that records no
    /// configuration, its JSON without the field, decodes as none.
    #[test]
    fn a_snapshot_and_a_report_without_a_configuration_decode() {
        let snapshot = StoredSnapshot {
            id: "s".into(),
            last_applied: None,
            membership: StoredMembershipConfig::default(),
            state: StateMachine::new(Paths::default(), Limits::defaults(48_000)).save(),
            config: Some(1),
        };
        let mut json = serde_json::to_value(&snapshot).expect("encodes");
        json.as_object_mut().expect("an object").remove("config");
        let decoded: StoredSnapshot = serde_json::from_value(json).expect("decodes");
        assert_eq!(decoded.config, None);
        let own = crate::report::Own {
            config: Some(1),
            ..Default::default()
        };
        let mut json = serde_json::to_value(&own).expect("encodes");
        json.as_object_mut().expect("an object").remove("config");
        let decoded: crate::report::Own = serde_json::from_value(json).expect("decodes");
        assert_eq!(decoded.config, None);
    }

    /// A snapshot is sent under the sender's configuration now: one saved
    /// before the configuration changed with every replica stopped, and
    /// one from a replica that recorded none, still install.
    #[tokio::test]
    async fn a_snapshot_is_sent_under_the_senders_configuration() {
        let dir_from = dir("cfg-sent-from");
        let mut store = open_as(&dir_from, 7);
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        store.build_snapshot().await.expect("build");
        drop(store);
        let mut store = open_as(&dir_from, 9);
        assert_eq!(
            store.snapshot_config(),
            Some(7),
            "built under 7, read at open"
        );
        let current = store
            .get_current_snapshot()
            .await
            .expect("read")
            .expect("a snapshot");
        let sent: StoredSnapshot =
            serde_json::from_slice(current.snapshot.get_ref()).expect("decodes");
        assert_eq!(sent.config, Some(9));
        let mut other = open_as(&dir("cfg-sent-to"), 9);
        other
            .install_snapshot(&current.meta, current.snapshot)
            .await
            .expect("install");
        let unrecorded = StoredSnapshot {
            config: None,
            ..sent
        };
        let mut third = open_as(&dir("cfg-sent-none"), 9);
        third
            .install_snapshot(
                &current.meta,
                Box::new(Cursor::new(
                    serde_json::to_vec(&unrecorded).expect("encodes"),
                )),
            )
            .await
            .expect("install");
    }

    /// A snapshot that decodes but does not load leaves the state and the
    /// snapshot on disk as they were.
    #[tokio::test]
    async fn a_snapshot_that_does_not_load_leaves_the_file_alone() {
        let mut store = open(&dir("snap-bad-from"));
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let snapshot = store.build_snapshot().await.expect("build");
        let mut stored: StoredSnapshot =
            serde_json::from_slice(snapshot.snapshot.get_ref()).expect("decodes");
        stored.state.product = b"not the product's state".to_vec();
        let to = dir("snap-bad-to");
        let mut other = open(&to);
        other
            .append_to_log(entries[..1].to_vec())
            .await
            .expect("append");
        other
            .apply_to_state_machine(&entries[..1])
            .await
            .expect("apply");
        other.build_snapshot().await.expect("build");
        let file = fs::read(to.join("snapshot")).expect("snapshot");
        let before = other.with_state(|sm| sm.save());
        let installed = other
            .install_snapshot(
                &snapshot.meta,
                Box::new(Cursor::new(serde_json::to_vec(&stored).expect("encodes"))),
            )
            .await;
        assert!(installed.is_err());
        assert_eq!(fs::read(to.join("snapshot")).expect("snapshot"), file);
        assert_eq!(other.applied_index(), 1);
        assert_eq!(other.with_state(|sm| sm.save()), before);
    }

    /// An install whose file cannot be written leaves the state as it was,
    /// in step with the snapshot on disk.
    #[tokio::test]
    async fn a_failed_snapshot_write_leaves_the_state_alone() {
        use std::os::unix::fs::PermissionsExt;
        let mut store = open(&dir("snap-from"));
        let entries: Vec<_> = (1..=3).map(entry).collect();
        store.append_to_log(entries.clone()).await.expect("append");
        store.apply_to_state_machine(&entries).await.expect("apply");
        let snapshot = store.build_snapshot().await.expect("build");
        let to = dir("snap-readonly");
        let mut other = open(&to);
        let before = other.with_state(|sm| sm.save());
        fs::set_permissions(&to, fs::Permissions::from_mode(0o555)).expect("read-only");
        let installed = other
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await;
        fs::set_permissions(&to, fs::Permissions::from_mode(0o755)).expect("writable");
        assert!(installed.is_err());
        assert_eq!(other.applied_index(), 0);
        assert_eq!(other.with_state(|sm| sm.save()), before);
    }
}
