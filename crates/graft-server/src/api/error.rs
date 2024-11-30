use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{
    gid::{GidParseErr, VolumeId},
    lsn::LSN,
    offset::Offset,
    page::PageSizeErr,
};
use graft_proto::common::v1::{GraftErr, GraftErrCode};
use splinter::DecodeErr;
use thiserror::Error;

use crate::{
    api::response::ProtoResponse,
    segment::closed::SegmentValidationErr,
    volume::{catalog::VolumeCatalogErr, store::VolumeStoreErr, updater::UpdateErr},
};

#[derive(Debug, Error)]
pub enum ApiErr {
    #[error("invalid request body: {0}")]
    InvalidRequestBody(String),

    #[error("failed to parse id: {0}")]
    GidParseErr(#[from] GidParseErr),

    #[error(transparent)]
    PageSizeErr(#[from] PageSizeErr),

    #[error("duplicate page offset detected: {0}")]
    DuplicatePageOffset(Offset),

    #[error("failed to parse offsets: {0}")]
    OffsetsDecodeErr(#[from] DecodeErr),

    #[error(transparent)]
    CatalogErr(#[from] VolumeCatalogErr),

    #[error(transparent)]
    VolumeStoreErr(#[from] VolumeStoreErr),

    #[error("failed to load snapshot for volume {0} at lsn {1:?}")]
    SnapshotMissing(VolumeId, Option<LSN>),

    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error("failed to load segment: {0}")]
    SegmentValidationErr(#[from] SegmentValidationErr),

    #[error(
        "commit rejected for volume {vid}: snapshot lsn {snapshot:?} is out of sync with latest lsn {latest:?}"
    )]
    CommitSnapshotOutOfDate {
        vid: VolumeId,
        snapshot: Option<LSN>,
        latest: Option<LSN>,
    },

    #[error("graft client request failed")]
    ClientErr(#[from] graft_client::ClientErr),
}

impl From<UpdateErr> for ApiErr {
    fn from(value: UpdateErr) -> Self {
        match value {
            UpdateErr::CatalogErr(err) => ApiErr::CatalogErr(err),
            UpdateErr::StoreErr(err) => ApiErr::VolumeStoreErr(err),
            UpdateErr::ClientErr(err) => ApiErr::ClientErr(err),
        }
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErr::*;

        tracing::error!(error = ?self, "api error");

        let (status, code) = match self {
            InvalidRequestBody(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            GidParseErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            DuplicatePageOffset(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            OffsetsDecodeErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Server),
            SnapshotMissing(_, _) => (StatusCode::NOT_FOUND, GraftErrCode::SnapshotMissing),
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
