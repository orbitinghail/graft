use std::fmt::Debug;

use graft_proto::common::v1::{GraftErr, GraftErrCode};
use thiserror::Error;
use trackerr::{format_location_stack, summarize_location_stack, CallerLocation, LocationStack};

use crate::runtime::storage;

#[derive(Error)]
pub enum ClientErr {
    #[error("graft error: {0}")]
    GraftErr(#[from] GraftErr, #[implicit] CallerLocation),

    #[error("request failed: {0}")]
    RequestErr(#[from] reqwest::Error, #[implicit] CallerLocation),

    #[error("failed to parse response: {0}")]
    ResponseParseErr(#[from] prost::DecodeError, #[implicit] CallerLocation),

    #[error("failed to parse splinter: {0}")]
    SplinterParseErr(#[from] splinter::DecodeErr, #[implicit] CallerLocation),

    #[error("local storage error: {0}")]
    StorageErr(#[from] storage::StorageErr, #[implicit] CallerLocation),
}

impl ClientErr {
    pub(crate) fn is_snapshot_missing(&self) -> bool {
        match self {
            ClientErr::GraftErr(err, _) => err.code() == GraftErrCode::SnapshotMissing,
            _ => false,
        }
    }
}

impl LocationStack for ClientErr {
    fn location(&self) -> &CallerLocation {
        use ClientErr::*;
        match self {
            GraftErr(_, loc)
            | RequestErr(_, loc)
            | ResponseParseErr(_, loc)
            | SplinterParseErr(_, loc)
            | StorageErr(_, loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        use ClientErr::*;
        match self {
            SplinterParseErr(err, _) => Some(err),
            StorageErr(err, _) => Some(err),
            _ => None,
        }
    }
}

impl Debug for ClientErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            summarize_location_stack(f, self)
        } else {
            format_location_stack(f, self)
        }
    }
}
