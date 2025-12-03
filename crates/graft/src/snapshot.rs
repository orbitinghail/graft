use std::ops::RangeInclusive;

use crate::core::{
    LogId,
    logref::LogRef,
    lsn::{LSN, LSNRangeExt},
};
use smallvec::SmallVec;

/// A `Snapshot` represents a logical view of a Volume, possibly made
/// up of LSN ranges from multiple Logs.
#[derive(Clone, Hash)]
pub struct Snapshot {
    path: SmallVec<[LogRangeRef; 1]>,
}

/// A reference to a Log and a range of LSNs within that Log.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LogRangeRef {
    pub log: LogId,
    pub lsns: RangeInclusive<LSN>,
}

impl std::fmt::Debug for LogRangeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}[{}]", self.log, self.lsns.to_string())
    }
}

impl LogRangeRef {
    pub fn start_ref(&self) -> LogRef {
        LogRef::new(self.log.clone(), *self.lsns.start())
    }

    pub fn end_ref(&self) -> LogRef {
        LogRef::new(self.log.clone(), *self.lsns.end())
    }
}

impl Snapshot {
    pub const EMPTY: Self = Self { path: SmallVec::new_const() };

    pub fn new(log: LogId, lsns: RangeInclusive<LSN>) -> Self {
        assert!(!lsns.is_empty());
        Self {
            path: SmallVec::from_const([LogRangeRef { log, lsns }]),
        }
    }

    pub fn head(&self) -> Option<(&LogId, LSN)> {
        self.path
            .first()
            .map(|entry| (&entry.log, *entry.lsns.end()))
    }

    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn append(&mut self, log: LogId, lsns: RangeInclusive<LSN>) {
        assert!(!lsns.is_empty());
        self.path.push(LogRangeRef { log, lsns });
    }

    /// iterate through all of the log range references in the snapshot
    pub fn iter(&self) -> std::slice::Iter<'_, LogRangeRef> {
        self.path.iter()
    }
}

impl IntoIterator for Snapshot {
    type Item = LogRangeRef;
    type IntoIter = smallvec::IntoIter<[LogRangeRef; 1]>;
    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Snapshot").field(&self.path).finish()
    }
}
