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
    GidParseError(#[from] GidParseErr),

    #[error(transparent)]
    PageSizeError(#[from] PageSizeErr),

    #[error("duplicate page offset detected: {0}")]
    DuplicatePageOffset(Offset),

    #[error("failed to parse offsets: {0}")]
    OffsetsDecodeError(#[from] DecodeErr),

    #[error(transparent)]
    CatalogError(#[from] VolumeCatalogErr),

    #[error("failed to load latest snapshot for volume: {0}")]
    SnapshotMissing(VolumeId),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("failed to load segment: {0}")]
    SegmentValidationError(#[from] SegmentValidationErr),
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErr::*;
        let status = match self {
            GidParseError(_) => StatusCode::BAD_REQUEST,
            DuplicatePageOffset(_) => StatusCode::BAD_REQUEST,
            OffsetsDecodeError(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}
