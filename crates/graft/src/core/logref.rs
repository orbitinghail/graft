use std::{fmt::Display, str::FromStr};

use bilrost::Message;
use thiserror::Error;

use crate::core::{
    LogId,
    gid::GidParseErr,
    lsn::{LSN, ParseLSNErr},
};

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

#[derive(Debug, Error)]
pub enum ParseLogRefErr {
    #[error("argument must be in the form: `log:lsn`")]
    InvalidFormat,

    #[error("invalid log: {0}")]
    InvalidLog(#[from] GidParseErr),

    #[error("invalid lsn: {0}")]
    InvalidLsn(#[from] ParseLSNErr),
}

impl FromStr for LogRef {
    type Err = ParseLogRefErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (log, lsn) = s.split_once(':').ok_or(ParseLogRefErr::InvalidFormat)?;
        Ok(Self::new(log.parse()?, lsn.parse()?))
    }
}
