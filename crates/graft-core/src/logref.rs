use std::fmt::Display;

use bilrost::Message;

use crate::{LogId, lsn::LSN};

/// A reference to a Log at a particular LSN.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct LogRef {
    /// The referenced Log ID
    #[bilrost(1)]
    pub log: LogId,

    /// The referenced LSN.
    #[bilrost(2)]
    pub lsn: LSN,
}

impl LogRef {
    pub fn new(log: LogId, lsn: LSN) -> Self {
        Self { log, lsn }
    }

    pub fn log(&self) -> &LogId {
        &self.log
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }
}

impl Display for LogRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.log.short(), self.lsn)
    }
}
