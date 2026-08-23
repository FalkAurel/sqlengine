use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::transaction::{Active, Change, CommitError, Transaction};

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct StableVersion(u64);

impl StableVersion {
    pub const fn new(version: u64) -> Self {
        Self(version)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

pub struct MVCC {
    /// Highest fully published commit version.
    stable_version: AtomicU64,

    /// Serializes the commit/publication phase.
    commit_lock: Mutex<()>,
}

impl MVCC {
    pub fn new() -> Self {
        Self {
            stable_version: AtomicU64::new(0),
            commit_lock: Mutex::new(()),
        }
    }

    pub fn begin_transaction(&self) -> Transaction<'_, Active> {
        let snapshot_version: StableVersion =
            StableVersion::new(self.stable_version.load(Ordering::Acquire));

        Transaction::new(self, snapshot_version)
    }

    pub(crate) fn commit(&self, changes: &mut Vec<Change>) -> Result<StableVersion, CommitError> {
        let _commit_guard = self.commit_lock.lock().unwrap();

        // The next committed version.
        let commit_version = StableVersion::new(self.stable_version.load(Ordering::Relaxed) + 1);

        // Everything below happens before stable_version is advanced.
        self.publish(changes, commit_version)?;

        // Publication is complete before this release-store.
        self.stable_version
            .store(commit_version.get(), Ordering::Release);

        Ok(commit_version)
    }

    fn publish(
        &self,
        changes: &[Change],
        commit_version: StableVersion,
    ) -> Result<(), CommitError> {
        for change in changes {
            match change {
                Change::Insert(insert) => {
                    self.publish_insert(insert, commit_version)?;
                }

                Change::Delete(delete) => {
                    self.publish_delete(delete, commit_version)?;
                }

                Change::Update(update) => {
                    self.publish_update(update, commit_version)?;
                }
            }
        }

        Ok(())
    }

    fn publish_insert(
        &self,
        insert: &Insert,
        commit_version: StableVersion,
    ) -> Result<(), CommitError> {
        // Publish immutable data / metadata.
        todo!()
    }

    fn publish_delete(
        &self,
        delete: &Delete,
        commit_version: StableVersion,
    ) -> Result<(), CommitError> {
        // Publish deletion metadata.
        todo!()
    }

    fn publish_update(
        &self,
        update: &Update,
        commit_version: StableVersion,
    ) -> Result<(), CommitError> {
        // Publish delete + insert.
        todo!()
    }
}
