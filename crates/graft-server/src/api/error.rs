use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{
    gid::{GidParseErr, VolumeId},
    offset::Offset,
    page::PageSizeErr,
};
use splinter::DecodeErr;
use thiserror::Error;

use crate::{segment::closed::SegmentValidationErr, volume::catalog::VolumeCatalogErr};

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

    #[error("failed to load latest snapshot for volume: {0}")]
    SnapshotMissing(VolumeId),

    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error("failed to load segment: {0}")]
    SegmentValidationErr(#[from] SegmentValidationErr),
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErr::*;
        let status = match self {
            GidParseErr(_) => StatusCode::BAD_REQUEST,
            DuplicatePageOffset(_) => StatusCode::BAD_REQUEST,
            OffsetsDecodeErr(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
