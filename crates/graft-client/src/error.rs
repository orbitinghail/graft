use std::fmt::Debug;

use graft_proto::common::v1::{GraftErr, GraftErrCode};
use thiserror::Error;

use crate::runtime::storage;

#[derive(Error, Debug)]
pub enum ClientErr {
    #[error("graft error: {0}")]
    GraftErr(#[from] GraftErr),

    #[error("request failed: {0}")]
    RequestErr(#[from] reqwest::Error),

    #[error("failed to decode protobuf message")]
    ProtobufDecodeErr,

    #[error("failed to parse splinter: {0}")]
    SplinterParseErr(#[from] splinter::DecodeErr),

    #[error("local storage error: {0}")]
    StorageErr(#[from] storage::StorageErr),
}

impl From<prost::DecodeError> for ClientErr {
    fn from(_: prost::DecodeError) -> Self {
        ClientErr::ProtobufDecodeErr
    }
}

impl ClientErr {
    pub(crate) fn is_snapshot_missing(&self) -> bool {
        match self {
            ClientErr::GraftErr(err) => err.code() == GraftErrCode::SnapshotMissing,
            _ => false,
        }
    }
}
