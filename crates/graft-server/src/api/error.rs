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
use splinter::DecodeErr;
use thiserror::Error;

use crate::{
    segment::closed::SegmentValidationErr,
    volume::{catalog::VolumeCatalogErr, store::VolumeStoreErr, updater::UpdateErr},
};

#[derive(Debug, Error)]
pub enum ApiErr {
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
}

impl From<UpdateErr> for ApiErr {
    fn from(value: UpdateErr) -> Self {
        match value {
            UpdateErr::CatalogErr(err) => ApiErr::CatalogErr(err),
            UpdateErr::StoreErr(err) => ApiErr::VolumeStoreErr(err),
        }
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErr::*;
        let status = match self {
            GidParseErr(_) => StatusCode::BAD_REQUEST,
            DuplicatePageOffset(_) => StatusCode::BAD_REQUEST,
            OffsetsDecodeErr(_) => StatusCode::BAD_REQUEST,
            SnapshotMissing(_, _) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
