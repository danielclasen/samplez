//! The replicated tables of applied command ids.
//!
//! Keyed by (principal, session, XXH3-128 of the client's id), each record
//! holds the revision of the entry that carried the command, how the
//! command resolved and that entry's stamping index. Every change happens in state-machine apply, in
//! apply order, and every container is ordered, so every replica and every
//! snapshot install holds the same table.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::entry::{Resolution, SessionKey};

/// Bounds on the table, set alike on every replica. They are applied in
/// state-machine apply and on a snapshot load but not carried in the log,
/// so replicas with different limits hold different tables and answer
/// copies differently: they are part of the set's compatibility key,
/// and changing one is a change of that key, not a rolling update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `dedupRetentionS` in samples: a record retires once its stamping
    /// index lies more than this below the highest one applied.
    pub retention: u64,
    /// `maxSessionCommands`: records per session, oldest retired first.
    pub max_session_commands: usize,
    /// `maxDedupEntries`: records in the set, oldest retired first.
    pub max_dedup_entries: usize,
}

impl Limits {
    /// The defaults at `rate` samples per second: 10 s, 1024 and 65536.
    pub fn defaults(rate: u64) -> Limits {
        Limits {
            retention: 10 * rate,
            max_session_commands: 1024,
            max_dedup_entries: 65536,
        }
    }
}

/// One applied command id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub session: SessionKey,
    pub id: u128,
    pub revision: u64,
    /// How the first copy resolved: applied or stale.
    pub resolution: Resolution,
    pub stamp: u64,
}

/// The table as snapshots carry it: records in apply order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Saved {
    pub highest_stamp: u64,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
pub struct Dedup {
    limits: Limits,
    highest_stamp: u64,
    next_seq: u64,
    /// Apply order.
    records: BTreeMap<u64, Record>,
    sessions: BTreeMap<SessionKey, BTreeMap<u128, u64>>,
    by_stamp: BTreeSet<(u64, u64)>,
    early_retired: u64,
}

/// The id as the table keys it.
pub fn hash_id(id: &str) -> u128 {
    xxhash_rust::xxh3::xxh3_128(id.as_bytes())
}

impl Dedup {
    pub fn new(limits: Limits) -> Dedup {
        Dedup {
            limits,
            highest_stamp: 0,
            next_seq: 0,
            records: BTreeMap::new(),
            sessions: BTreeMap::new(),
            by_stamp: BTreeSet::new(),
            early_retired: 0,
        }
    }

    /// The revision `id` was applied at in `session`, while its record is
    /// held.
    pub fn revision(&self, session: &SessionKey, id: &str) -> Option<u64> {
        let seq = self.sessions.get(session)?.get(&hash_id(id))?;
        Some(self.records[seq].revision)
    }

    /// The answer to a copy of `id` in `session` while its record is held:
    /// its revision, and how its first copy resolved, an applied one as a
    /// duplicate.
    pub fn answer(&self, session: &SessionKey, id: &str) -> Option<(u64, Resolution)> {
        let seq = self.sessions.get(session)?.get(&hash_id(id))?;
        let record = &self.records[seq];
        let resolution = match record.resolution {
            Resolution::Applied => Resolution::Duplicate,
            other => other,
        };
        Some((record.revision, resolution))
    }

    /// Sets how the command just recorded resolved, which its record holds
    /// as applied until then.
    pub fn resolve(&mut self, session: &SessionKey, id: &str, resolution: Resolution) {
        if let Some(seq) = self
            .sessions
            .get(session)
            .and_then(|ids| ids.get(&hash_id(id)))
        {
            self.records
                .get_mut(seq)
                .expect("indexed record")
                .resolution = resolution;
        }
    }

    /// Records `id` for `session` at `revision`, carried by an entry
    /// stamped `stamp`. Retention runs first, at the entry's stamp, so a
    /// copy applied more than the retention after its original applies
    /// again; the caps run after. Returns the stored revision when the id
    /// is still held, and records nothing then.
    pub fn record(
        &mut self,
        session: &SessionKey,
        id: &str,
        revision: u64,
        stamp: u64,
    ) -> Option<u64> {
        self.highest_stamp = self.highest_stamp.max(stamp);
        self.expire();
        if let Some(stored) = self.revision(session, id) {
            return Some(stored);
        }
        let hash = hash_id(id);
        let seq = self.next_seq;
        self.next_seq += 1;
        self.sessions
            .entry(session.clone())
            .or_default()
            .insert(hash, seq);
        self.by_stamp.insert((stamp, seq));
        self.records.insert(
            seq,
            Record {
                session: session.clone(),
                id: hash,
                revision,
                resolution: Resolution::Applied,
                stamp,
            },
        );
        self.expire();
        self.cap(session);
        None
    }

    fn expire(&mut self) {
        let floor = self.highest_stamp.saturating_sub(self.limits.retention);
        while let Some(&(stamp, seq)) = self.by_stamp.first() {
            if stamp >= floor {
                break;
            }
            self.remove(seq);
        }
    }

    fn cap(&mut self, session: &SessionKey) {
        while let Some(&oldest) = self
            .sessions
            .get(session)
            .filter(|ids| ids.len() > self.limits.max_session_commands)
            .and_then(|ids| ids.values().min())
        {
            self.remove(oldest);
            self.early_retired += 1;
        }
        while self.records.len() > self.limits.max_dedup_entries {
            let (&oldest, _) = self
                .records
                .first_key_value()
                .expect("records over the cap");
            self.remove(oldest);
            self.early_retired += 1;
        }
    }

    fn remove(&mut self, seq: u64) {
        let record = self.records.remove(&seq).expect("indexed record");
        self.by_stamp.remove(&(record.stamp, seq));
        let ids = self
            .sessions
            .get_mut(&record.session)
            .expect("indexed session");
        ids.remove(&record.id);
        if ids.is_empty() {
            self.sessions.remove(&record.session);
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Sessions holding at least one record.
    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    /// Records retired by a cap before their retention ran out, on this
    /// replica since it started: `dedup_early_retired_total`.
    pub fn early_retired(&self) -> u64 {
        self.early_retired
    }

    pub fn save(&self) -> Saved {
        Saved {
            highest_stamp: self.highest_stamp,
            records: self.records.values().cloned().collect(),
        }
    }

    pub fn load(limits: Limits, saved: Saved) -> Dedup {
        let mut dedup = Dedup::new(limits);
        dedup.highest_stamp = saved.highest_stamp;
        for record in saved.records {
            let seq = dedup.next_seq;
            dedup.next_seq += 1;
            dedup
                .sessions
                .entry(record.session.clone())
                .or_default()
                .insert(record.id, seq);
            dedup.by_stamp.insert((record.stamp, seq));
            dedup.records.insert(seq, record);
        }
        dedup
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(session: &str) -> SessionKey {
        SessionKey {
            principal: "anonymous".into(),
            session: session.into(),
        }
    }

    fn limits() -> Limits {
        Limits {
            retention: 1000,
            max_session_commands: 1024,
            max_dedup_entries: 65536,
        }
    }

    #[test]
    fn a_resend_resolves_to_the_stored_revision() {
        let mut dedup = Dedup::new(limits());
        assert_eq!(dedup.record(&key("s"), "7", 3, 48), None);
        assert_eq!(dedup.record(&key("s"), "7", 9, 96), Some(3));
        assert_eq!(dedup.revision(&key("s"), "7"), Some(3));
    }

    #[test]
    fn ids_are_keyed_per_principal_and_session() {
        let mut dedup = Dedup::new(limits());
        dedup.record(&key("s"), "7", 3, 48);
        assert_eq!(dedup.revision(&key("t"), "7"), None);
        let other = SessionKey {
            principal: "alice".into(),
            session: "s".into(),
        };
        assert_eq!(dedup.revision(&other, "7"), None);
    }

    #[test]
    fn retention_counts_in_stamping_index_not_in_entries() {
        let mut dedup = Dedup::new(limits());
        dedup.record(&key("s"), "old", 1, 0);
        dedup.record(&key("s"), "edge", 2, 500);
        // Many entries at one stamp retire nothing.
        for n in 0..100 {
            dedup.record(&key("s"), &format!("n{n}"), 3 + n, 1000);
        }
        assert_eq!(dedup.revision(&key("s"), "old"), Some(1));
        // At 1001 a copy of "old" is past the retention: it is recorded
        // anew rather than resolved to its first revision.
        assert_eq!(dedup.record(&key("s"), "old", 200, 1001), None);
        assert_eq!(dedup.revision(&key("s"), "old"), Some(200));
        assert_eq!(dedup.revision(&key("s"), "edge"), Some(2));
        assert_eq!(dedup.early_retired(), 0);
    }

    #[test]
    fn a_session_table_goes_with_its_last_record() {
        let mut dedup = Dedup::new(limits());
        dedup.record(&key("gone"), "1", 1, 0);
        dedup.record(&key("kept"), "1", 2, 2000);
        assert_eq!(dedup.sessions(), 1);
        assert_eq!(dedup.revision(&key("kept"), "1"), Some(2));
    }

    #[test]
    fn caps_retire_the_oldest_first_and_count_them() {
        let mut dedup = Dedup::new(Limits {
            retention: u64::MAX,
            max_session_commands: 4,
            max_dedup_entries: 6,
        });
        for n in 0..6 {
            dedup.record(&key("a"), &n.to_string(), n, n);
        }
        // Session cap: 0 and 1 retired, 2..6 held.
        assert_eq!(dedup.revision(&key("a"), "1"), None);
        assert_eq!(dedup.revision(&key("a"), "2"), Some(2));
        assert_eq!(dedup.early_retired(), 2);
        for n in 0..3 {
            dedup.record(&key("b"), &n.to_string(), 10 + n, 10 + n);
        }
        // Set cap: 7 records over 6 retires the oldest of the set, a's 2.
        assert_eq!(dedup.len(), 6);
        assert_eq!(dedup.revision(&key("a"), "2"), None);
        assert_eq!(dedup.revision(&key("a"), "3"), Some(3));
        assert_eq!(dedup.early_retired(), 3);
    }

    #[test]
    fn a_saved_table_loads_identical() {
        let mut dedup = Dedup::new(Limits {
            retention: 100,
            max_session_commands: 3,
            max_dedup_entries: 5,
        });
        for n in 0..20u64 {
            dedup.record(&key(&(n % 3).to_string()), &n.to_string(), n, n * 10);
        }
        let mut loaded = Dedup::load(dedup.limits, dedup.save());
        assert_eq!(loaded.save(), dedup.save());
        // And it keeps retiring alike.
        dedup.record(&key("x"), "y", 99, 400);
        loaded.record(&key("x"), "y", 99, 400);
        assert_eq!(loaded.save(), dedup.save());
    }
}
