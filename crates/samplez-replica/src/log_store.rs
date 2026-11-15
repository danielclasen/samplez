//! The log, vote, committed log id and snapshot on the replica's volume,
//! with the state machine beside them: openraft 0.9's [`RaftStorage`],
//! which [`openraft::storage::Adaptor`] splits into its log store and state
//! machine (the split traits are sealed without the `storage-v2` feature).
//!
//! `log` is an append-only file of length-prefixed records, read back to
//! the last whole one; `snapshot` is replaced by rename.

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
    file: File,
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
pub struct Store<P> {
    inner: Arc<Mutex<Inner<P>>>,
}

impl<P> Clone for Store<P> {
    fn clone(&self) -> Self {
        Store {
            inner: self.inner.clone(),
        }
    }
}

fn io_error<E: std::error::Error + 'static>(
    make: impl FnOnce(AnyError) -> StorageIOError<NodeId>,
    e: &E,
) -> Error {
    StorageError::IO {
        source: make(AnyError::new(e)),
    }
}

fn encode(record: &Record, out: &mut Vec<u8>) {
    let body = serde_json::to_vec(record).expect("records encode");
    let len = u32::try_from(body.len()).expect("record under 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&body);
}

/// The whole records at the start of `bytes`, and the length they take: a
/// write cut short by a crash leaves a tail that is not one.
fn decode(bytes: &[u8]) -> (Vec<Record>, usize) {
    let mut records = Vec::new();
    let mut at = 0;
    while let Some(head) = bytes.get(at..at + 4) {
        let len = u32::from_le_bytes(head.try_into().expect("4 bytes")) as usize;
        let Some(body) = bytes.get(at + 4..at + 4 + len) else {
            break;
        };
        let Ok(record) = serde_json::from_slice(body) else {
            break;
        };
        records.push(record);
        at += 4 + len;
    }
    (records, at)
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
        let (records, whole) = decode(&bytes);
        if whole < bytes.len() {
            file.set_len(whole as u64)?;
            file.sync_all()?;
        }
        let mut inner = Inner {
            dir: dir.to_owned(),
            file,
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
        })
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

    /// Whether every entry up to the saved committed id, capped at the
    /// last entry held, is applied.
    pub fn caught_up(&self) -> bool {
        let inner = self.lock();
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

    fn write(&mut self, records: &[Record], sync: bool) -> io::Result<()> {
        let mut out = Vec::new();
        for record in records {
            encode(record, &mut out);
        }
        self.file.write_all(&out)?;
        if sync { self.file.sync_data() } else { Ok(()) }
    }

    /// Rewrites `log` with what it holds now, dropping purged entries.
    fn compact(&mut self) -> io::Result<()> {
        let mut records = Vec::new();
        records.extend(self.vote.map(Record::Vote));
        records.push(Record::Committed(self.committed));
        records.extend(self.last_purged.map(Record::Purge));
        records.extend(self.log.values().cloned().map(Record::Entry));
        let mut out = Vec::new();
        for record in &records {
            encode(record, &mut out);
        }
        replace(&self.dir, "log", &out)?;
        self.file = OpenOptions::new().append(true).open(self.dir.join("log"))?;
        Ok(())
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
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, Error> {
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
        let bytes = serde_json::to_vec(&snapshot).expect("snapshots encode");
        replace(&inner.dir, "snapshot", &bytes)
            .map_err(|e| io_error(|a| StorageIOError::write_snapshot(None, a), &e))?;
        let meta = meta(&snapshot);
        inner.snapshot = Some(snapshot);
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
        let mut inner = self.lock();
        let vote = StoredVote::from(vote);
        inner
            .write(&[Record::Vote(vote)], true)
            .map_err(|e| io_error(StorageIOError::write_vote, &e))?;
        inner.vote = Some(vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, Error> {
        Ok(self.lock().vote.map(Vote::from))
    }

    /// Written with no fsync of its own: the next log write's fsync covers
    /// it, and a process that dies before keeps it in the page cache.
    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> Result<(), Error> {
        let mut inner = self.lock();
        let committed = committed.as_ref().map(StoredLogId::from);
        inner
            .write(&[Record::Committed(committed)], false)
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        inner.committed = committed;
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
        let mut inner = self.lock();
        let entries: Vec<StoredEntry> =
            entries.into_iter().map(|e| StoredEntry::from(&e)).collect();
        let records: Vec<Record> = entries.iter().cloned().map(Record::Entry).collect();
        inner
            .write(&records, true)
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), Error> {
        let mut inner = self.lock();
        inner
            .write(&[Record::Truncate(log_id.index)], true)
            .map_err(|e| io_error(StorageIOError::write_logs, &e))?;
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), Error> {
        let mut inner = self.lock();
        inner.purge(StoredLogId::from(&log_id));
        inner
            .compact()
            .map_err(|e| io_error(StorageIOError::write_logs, &e))
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
        let mut inner = self.lock();
        let limits = inner.limits;
        let at = stored.last_applied.map_or(0, |id| id.index);
        inner
            .sm
            .load(stored.state.clone(), limits, at)
            .map_err(|e| read_error(&e))?;
        inner.last_applied = stored.last_applied;
        inner.membership = stored.membership.clone();
        replace(&inner.dir, "snapshot", &bytes).map_err(|e| {
            io_error(
                |a| StorageIOError::write_snapshot(Some(meta.signature()), a),
                &e,
            )
        })?;
        inner.snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, Error> {
        let inner = self.lock();
        Ok(inner.snapshot.as_ref().map(|snapshot| Snapshot {
            meta: meta(snapshot),
            snapshot: Box::new(Cursor::new(
                serde_json::to_vec(snapshot).expect("snapshots encode"),
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
}
