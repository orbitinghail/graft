use std::fmt::Debug;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{
    gid::{GidParseErr, VolumeId},
    lsn::LSN,
    page::PageSizeErr,
    page_offset::PageOffset,
};
use graft_proto::common::v1::{GraftErr, GraftErrCode};
use splinter::DecodeErr;
use thiserror::Error;
use trackerr::{format_location_stack, summarize_location_stack, CallerLocation, LocationStack};

use crate::{
    api::response::ProtoResponse,
    segment::closed::SegmentValidationErr,
    volume::{catalog::VolumeCatalogErr, store::VolumeStoreErr, updater::UpdateErr},
};

#[derive(Error)]
pub enum ApiErr {
    #[error("invalid request body: {0}")]
    InvalidRequestBody(String, CallerLocation),

    #[error("failed to parse id")]
    GidParseErr(#[from] GidParseErr, #[implicit] CallerLocation),

    #[error("invalid page")]
    PageSizeErr(#[from] PageSizeErr, #[implicit] CallerLocation),

    #[error("duplicate page offset detected: {0}")]
    DuplicatePageOffset(PageOffset, CallerLocation),

    #[error("failed to parse offsets")]
    OffsetsDecodeErr(#[from] DecodeErr, #[implicit] CallerLocation),

    #[error("catalog error")]
    CatalogErr(#[from] VolumeCatalogErr, #[implicit] CallerLocation),

    #[error("volume store error")]
    VolumeStoreErr(#[from] VolumeStoreErr, #[implicit] CallerLocation),

    #[error("failed to load snapshot for volume {0} at lsn {1:?}")]
    SnapshotMissing(VolumeId, Option<LSN>, CallerLocation),

    #[error("io error")]
    IoErr(#[from] std::io::Error, #[implicit] CallerLocation),

    #[error("failed to load segment")]
    SegmentValidationErr(#[from] SegmentValidationErr, #[implicit] CallerLocation),

    #[error(
        "commit rejected for volume {vid}: snapshot lsn {snapshot:?} is out of sync with latest lsn {latest:?}"
    )]
    CommitSnapshotOutOfDate {
        vid: VolumeId,
        snapshot: Option<LSN>,
        latest: Option<LSN>,
        location: CallerLocation,
    },

    #[error("graft client request failed")]
    ClientErr(#[from] graft_client::ClientErr, #[implicit] CallerLocation),
}

impl From<UpdateErr> for ApiErr {
    fn from(value: UpdateErr) -> Self {
        match value {
            UpdateErr::CatalogErr(err, loc) => ApiErr::CatalogErr(err, loc),
            UpdateErr::StoreErr(err, loc) => ApiErr::VolumeStoreErr(err, loc),
            UpdateErr::ClientErr(err, loc) => ApiErr::ClientErr(err, loc),
        }
    }
}

impl LocationStack for ApiErr {
    fn location(&self) -> &CallerLocation {
        use ApiErr::*;
        match self {
            InvalidRequestBody(_, loc)
            | GidParseErr(_, loc)
            | PageSizeErr(_, loc)
            | DuplicatePageOffset(_, loc)
            | OffsetsDecodeErr(_, loc)
            | CatalogErr(_, loc)
            | VolumeStoreErr(_, loc)
            | SnapshotMissing(_, _, loc)
            | IoErr(_, loc)
            | SegmentValidationErr(_, loc)
            | CommitSnapshotOutOfDate { location: loc, .. }
            | ClientErr(_, loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        use ApiErr::*;
        match self {
            GidParseErr(err, _) => Some(err),
            PageSizeErr(err, _) => Some(err),
            OffsetsDecodeErr(err, _) => Some(err),
            CatalogErr(err, _) => Some(err),
            VolumeStoreErr(err, _) => Some(err),
            SegmentValidationErr(err, _) => Some(err),
            ClientErr(err, _) => Some(err),
            _ => None,
        }
    }
}

impl Debug for ApiErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            summarize_location_stack(f, self)
        } else {
            format_location_stack(f, self)
        }
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErr::*;

        tracing::error!(error = ?self, "api error");

        let (status, code) = match self {
            InvalidRequestBody(_, _) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            GidParseErr(_, _) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            DuplicatePageOffset(_, _) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            OffsetsDecodeErr(_, _) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            SnapshotMissing(_, _, _) => (StatusCode::NOT_FOUND, GraftErrCode::SnapshotMissing),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, GraftErrCode::Client),
        };
        let message = self.to_string();

        (
            status,
            ProtoResponse::new(GraftErr { code: code as i32, message }),
        )
            .into_response()
    }
}
