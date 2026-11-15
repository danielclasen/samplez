//! The log, vote, committed log id and snapshot on the replica's volume,
//! with the state machine beside them: openraft 0.9's [`RaftStorage`],
//! which [`openraft::storage::Adaptor`] splits into its log store and state
//! machine (the split traits are sealed without the `storage-v2` feature).
//!
//! `log` is an append-only file of records, each a length, a CRC-32C of
//! its body and the body. A tail that holds no whole record, as a write
//! cut short by a crash leaves, is cut on open; a damaged record with a
//! whole one after it is corruption, and the store does not open.
//! `snapshot` is replaced by rename.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    AnyError, BasicNode, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::TypeConfig;
use crate::dedup::Limits;
use crate::entry::{NodeId, Response};
use crate::snapshot::{
    StoredEntry, StoredLogId, StoredMembershipConfig, StoredSnapshot, StoredVote,
};
use crate::state_machine::{Product, StateMachine};

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

struct Inner<P> {
    dir: PathBuf,
    file: Arc<File>,
    vote: Option<StoredVote>,
    committed: Option<StoredLogId>,
    log: BTreeMap<u64, StoredEntry>,
    last_purged: Option<StoredLogId>,
    sm: StateMachine<P>,
    limits: Limits,
    last_applied: Option<StoredLogId>,
    membership: StoredMembershipConfig,
    snapshot: Option<StoredSnapshot>,
    snapshots_built: u64,
}

/// One replica's storage. Clones share it.
///
/// File writes and fsyncs run off the async worker, without the lock that
/// readers of the state take; the in-memory state follows once a write
/// is done. openraft's adaptor makes one log or state machine call at a
/// time; a snapshot build runs beside them and `snapshots` orders it
/// with an install.
pub struct Store<P> {
    inner: Arc<Mutex<Inner<P>>>,
    snapshots: Arc<tokio::sync::Mutex<()>>,
}

impl<P> Clone for Store<P> {
    fn clone(&self) -> Self {
        Store {
            inner: self.inner.clone(),
            snapshots: self.snapshots.clone(),
        }
    }
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

/// A record's length and checksum, before its body.
const HEAD: usize = 8;

fn encode(record: &Record, out: &mut Vec<u8>) {
    let body = serde_json::to_vec(record).expect("records encode");
    let len = u32::try_from(body.len()).expect("record under 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32c(&body).to_le_bytes());
    out.extend_from_slice(&body);
}

/// The body of the record at `at` when a whole one is there: it fits and
/// its checksum matches. No record is empty, so zeroes are none.
fn record_at(bytes: &[u8], at: usize) -> Option<&[u8]> {
    let head = bytes.get(at..at + HEAD)?;
    let len = u32::from_le_bytes(head[..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().expect("4 bytes"));
    let body = bytes.get(at + HEAD..at + HEAD + len)?;
    (len > 0 && crc32c(body) == crc).then_some(body)
}

/// The records of `bytes` and the length they take. Only a crash in an
/// unsynced write leaves a damaged record, and then nothing whole after
/// it: a damaged record with a whole one after it is corruption that
/// cutting would turn into lost fsynced entries and votes.
fn decode(bytes: &[u8]) -> io::Result<(Vec<Record>, usize)> {
    let mut records = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let Some(body) = record_at(bytes, at) else {
            if let Some(next) = (at + 1..bytes.len()).find(|&n| record_at(bytes, n).is_some()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "the log record at offset {at} is damaged, and of the {} bytes \
                         from there a whole record starts at offset {next}",
                        bytes.len() - at
                    ),
                ));
            }
            break;
        };
        let record = serde_json::from_slice(body).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the log record at offset {at}, {} bytes, does not decode: {e}",
                    body.len()
                ),
            )
        })?;
        records.push(record);
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
    /// Opens the storage in `dir`, creating it when absent, with `product`
    /// as the empty state the snapshot (if any) is loaded into.
    pub fn open(dir: &Path, product: P, limits: Limits) -> io::Result<Store<P>> {
        fs::create_dir_all(dir)?;
        let mut sm = StateMachine::new(product, limits);
        let mut last_applied = None;
        let mut membership = StoredMembershipConfig::default();
        let snapshot = match fs::read(dir.join("snapshot")) {
            Ok(bytes) => {
                let snapshot: StoredSnapshot =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                let at = snapshot.last_applied.map_or(0, |id| id.index);
                sm.load(snapshot.state.clone(), limits, at)
                    .map_err(io::Error::other)?;
                last_applied = snapshot.last_applied;
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
            sm,
            limits,
            last_applied,
            membership,
            snapshot,
            snapshots_built: 0,
        };
        for record in records {
            inner.replay(record);
        }
        sync_dir(dir)?;
        Ok(Store {
            inner: Arc::new(Mutex::new(inner)),
            snapshots: Arc::default(),
        })
    }

    /// Appends `records` to `log`, fsynced with `sync`.
    async fn write(&self, records: &[Record], sync: bool) -> io::Result<()> {
        let mut out = Vec::new();
        for record in records {
            encode(record, &mut out);
        }
        let file = self.lock().file.clone();
        blocking(move || {
            (&*file).write_all(&out)?;
            if sync { file.sync_data() } else { Ok(()) }
        })
        .await
    }

    fn lock(&self) -> MutexGuard<'_, Inner<P>> {
        self.inner.lock().expect("store lock poisoned")
    }

    /// Reads the state machine.
    pub fn with_state<T>(&self, f: impl FnOnce(&StateMachine<P>) -> T) -> T {
        f(&self.lock().sm)
    }

    /// Reads the state machine with the index of the last entry applied.
    pub fn view<T>(&self, f: impl FnOnce(u64, &StateMachine<P>) -> T) -> T {
        let inner = self.lock();
        f(inner.last_applied.map_or(0, |id| id.index), &inner.sm)
    }

    /// The index of the last entry applied.
    pub fn applied_index(&self) -> u64 {
        self.view(|applied, _| applied)
    }

    /// Whether the volume has joined a set, a vote or a committed id saved
    /// on it, and every entry up to the saved committed id, capped at the
    /// last entry held, is applied. An empty volume has learnt nothing yet,
    /// so it is not.
    pub fn caught_up(&self) -> bool {
        let inner = self.lock();
        if inner.vote.is_none() && inner.committed.is_none() {
            return false;
        }
        let committed = inner.capped_committed().map_or(0, |id| id.index);
        inner.last_applied.map_or(0, |id| id.index) >= committed
    }
}

impl<P> Inner<P> {
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

    /// `log` as it holds now, without purged entries.
    fn compacted(&self) -> Vec<u8> {
        let mut records = Vec::new();
        records.extend(self.vote.map(Record::Vote));
        records.push(Record::Committed(self.committed));
        records.extend(self.last_purged.map(Record::Purge));
        records.extend(self.log.values().cloned().map(Record::Entry));
        let mut out = Vec::new();
        for record in &records {
            encode(record, &mut out);
        }
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
}

impl<P: Product> RaftSnapshotBuilder<TypeConfig> for Store<P> {
    /// Takes the state under the lock and encodes and writes it after.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, Error> {
        let _building = self.snapshots.lock().await;
        let (dir, snapshot) = {
            let mut inner = self.lock();
            inner.snapshots_built += 1;
            let last = inner.last_applied;
            let snapshot = StoredSnapshot {
                id: format!(
                    "{}-{}",
                    last.map_or(0, |id| id.index),
                    inner.snapshots_built
                ),
                last_applied: last,
                membership: inner.membership.clone(),
                state: inner.sm.save(),
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
        let (dir, out) = {
            let mut inner = self.lock();
            inner.purge(StoredLogId::from(&log_id));
            (inner.dir.clone(), inner.compacted())
        };
        let file = blocking(move || {
            replace(&dir, "log", &out)?;
            OpenOptions::new().append(true).open(dir.join("log"))
        })
        .await
        .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        self.lock().file = Arc::new(file);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), Error> {
        let inner = self.lock();
        Ok((
            inner.last_applied.map(LogId::from),
            inner.membership.stored(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<TypeConfig>],
    ) -> Result<Vec<Response>, Error> {
        let mut inner = self.lock();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            let id = StoredLogId::from(&entry.log_id);
            inner.last_applied = Some(id);
            responses.push(match &entry.payload {
                EntryPayload::Blank => Response::default(),
                EntryPayload::Normal(request) => inner.sm.apply(id.index, request),
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembershipConfig {
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
        let _installing = self.snapshots.lock().await;
        // The file first, so a failed write leaves the state as the file
        // on disk holds it.
        let dir = self.lock().dir.clone();
        blocking(move || replace(&dir, "snapshot", &bytes))
            .await
            .map_err(|e| {
                io_error(
                    |a| StorageIOError::write_snapshot(Some(meta.signature()), a),
                    &e,
                )
            })?;
        let mut inner = self.lock();
        let limits = inner.limits;
        let at = stored.last_applied.map_or(0, |id| id.index);
        inner
            .sm
            .load(stored.state.clone(), limits, at)
            .map_err(|e| read_error(&e))?;
        inner.last_applied = stored.last_applied;
        inner.membership = stored.membership.clone();
        inner.snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, Error> {
        let snapshot = self.lock().snapshot.clone();
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
        Store::open(dir, Paths::default(), Limits::defaults(48_000)).expect("opens")
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

    /// A damaged record with whole ones after it is not a torn tail:
    /// cutting it would drop fsynced entries, so the store does not open
    /// and leaves the file as it is.
    #[tokio::test]
    async fn a_damaged_record_before_whole_ones_refuses_to_open() {
        let dir = dir("damaged");
        let mut store = open(&dir);
        store
            .append_to_log((1..=3).map(entry))
            .await
            .expect("append");
        drop(store);
        let mut bytes = fs::read(dir.join("log")).expect("log");
        let len = bytes.len();
        bytes[HEAD + 2] ^= 0x20;
        fs::write(dir.join("log"), &bytes).expect("damage");
        let Err(e) = Store::open(&dir, Paths::default(), Limits::defaults(48_000)) else {
            panic!("opened a damaged log");
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(e.to_string().contains("offset 0"), "{e}");
        assert_eq!(
            fs::metadata(dir.join("log")).expect("log").len() as usize,
            len
        );
    }

    /// A flipped bit that leaves the body valid JSON is caught by the
    /// checksum, not read back as another entry.
    #[tokio::test]
    async fn a_record_that_still_parses_is_caught_by_its_checksum() {
        let dir = dir("bitflip");
        let mut store = open(&dir);
        store
            .append_to_log((1..=2).map(entry))
            .await
            .expect("append");
        drop(store);
        let mut bytes = fs::read(dir.join("log")).expect("log");
        let at = bytes
            .windows(10)
            .position(|w| w == b"\"stamp\":48")
            .expect("first entry's stamp")
            + 8;
        bytes[at] = b'5';
        fs::write(dir.join("log"), &bytes).expect("damage");
        let Err(e) = Store::open(&dir, Paths::default(), Limits::defaults(48_000)) else {
            panic!("read a damaged record back");
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

    #[tokio::test]
    async fn caught_up_once_applied_up_to_the_capped_committed_id() {
        let dir = dir("caught-up");
        let mut store = open(&dir);
        assert!(!store.caught_up(), "an empty volume has joined nothing");
        store
            .save_vote(&Vote::new_committed(1, 0))
            .await
            .expect("vote");
        assert!(store.caught_up(), "nothing committed, nothing to apply");
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
