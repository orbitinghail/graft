use std::fmt::Debug;

use graft_core::page_index::ConvertToPageIdxErr;
use graft_proto::common::v1::{GraftErr, GraftErrCode};
use thiserror::Error;

use crate::runtime::storage;

#[derive(Error, Debug)]
pub enum ClientErr {
    #[error("graft error: {0}")]
    GraftErr(#[from] GraftErr),

    #[error("http request failed: {0}")]
    HttpErr(#[from] ureq::Error),

    #[error("failed to decode protobuf message")]
    ProtobufDecodeErr,

    #[error("failed to parse splinter: {0}")]
    SplinterParseErr(#[from] splinter::DecodeErr),

    #[error("local storage error: {0}")]
    StorageErr(#[from] storage::StorageErr),

    #[error("io error: {0}")]
    IoErr(std::io::ErrorKind),

    #[error("invalid page index")]
    ConvertToPageIdxErr(#[from] ConvertToPageIdxErr),
}

impl From<http::Error> for ClientErr {
    fn from(err: http::Error) -> Self {
        Self::HttpErr(err.into())
    }
}

impl From<std::io::Error> for ClientErr {
    fn from(err: std::io::Error) -> Self {
        // attempt to convert the io error to a ureq error
        match ureq::Error::from(err) {
            // if we get an io error back then we normalize it
            ureq::Error::Io(ioerr) => Self::IoErr(ioerr.kind()),
            // if we get a decompression error, unpack the wrapped io error
            ureq::Error::Decompress(_, ioerr) => ioerr.into(),
            // otherwise we use the ureq Error
            other => Self::HttpErr(other),
        }
    }
}

impl From<prost::DecodeError> for ClientErr {
    fn from(_: prost::DecodeError) -> Self {
        ClientErr::ProtobufDecodeErr
    }
}

impl ClientErr {
    pub(crate) fn is_snapshot_missing(&self) -> bool {
        match self {
            Self::GraftErr(err) => err.code() == GraftErrCode::SnapshotMissing,
            _ => false,
        }
    }

    pub(crate) fn is_commit_rejected(&self) -> bool {
        match self {
            Self::GraftErr(err) => err.code() == GraftErrCode::CommitRejected,
            _ => false,
        }
    }
}
