use std::marker::PhantomData;

use crate::mvcc::{MVCC, StableVersion};

pub struct Active;
pub struct Committed;
pub struct RolledBack;

pub struct Transaction<'mvcc, Kind> {
    mvcc: &'mvcc MVCC,

    /// The committed version this transaction reads from.
    snapshot_version: StableVersion,

    /// Changes prepared by this transaction.
    ///
    /// These do not have a commit version yet.
    changes: Vec<Change>,

    /// Set only after the transaction successfully commits.
    commit_version: Option<StableVersion>,
    kind: PhantomData<Kind>,
}

impl<'mvcc> Transaction<'mvcc, Active> {
    pub(crate) fn new(mvcc: &'mvcc MVCC, snapshot_version: StableVersion) -> Self {
        Self {
            mvcc,
            snapshot_version,
            changes: Vec::new(),
            commit_version: None,
            kind: PhantomData,
        }
    }

    pub fn add(&mut self, change: Change) {
        self.changes.push(change);
    }

    pub fn commit(mut self) -> Result<Transaction<'mvcc, Committed>, CommitError> {
        let stable_version: StableVersion = self.mvcc.commit(&mut self.changes)?;

        Ok(Transaction {
            mvcc: self.mvcc,
            snapshot_version: self.snapshot_version,
            changes: self.changes,
            commit_version: Some(stable_version),
            kind: PhantomData,
        })
    }
}


pub enum Action {
    Delete(DeleteAction)
}

pub enum Change {
    Insert(Insert),
    Delete(Delete),
    Update(Update),
}

pub(crate) struct Delete {
    record_id: RecordId,
}

pub struct CommitError;
