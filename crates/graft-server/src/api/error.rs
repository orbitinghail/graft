use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{
    guid::{GuidParseError, VolumeId},
    offset::Offset,
    page::PageSizeError,
};
use splinter::DecodeErr;
use thiserror::Error;

use crate::{segment::closed::SegmentValidationErr, volume::catalog::VolumeCatalogError};

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("failed to parse GUID: {0}")]
    GuidParseError(#[from] GuidParseError),

    #[error(transparent)]
    PageSizeError(#[from] PageSizeError),

    #[error("duplicate page offset detected: {0}")]
    DuplicatePageOffset(Offset),

    #[error("failed to parse offsets: {0}")]
    OffsetsDecodeError(#[from] DecodeErr),

    #[error(transparent)]
    CatalogError(#[from] VolumeCatalogError),

    #[error("failed to load snapshot for volume: {0}")]
    SnapshotMissing(VolumeId),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("failed to load segment: {0}")]
    SegmentValidationError(#[from] SegmentValidationErr),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}
