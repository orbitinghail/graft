use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{guid::GuidParseError, offset::Offset, page::PageSizeError};
use splinter::DecodeErr;
use thiserror::Error;

use crate::volume::catalog::VolumeCatalogError;

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
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}
