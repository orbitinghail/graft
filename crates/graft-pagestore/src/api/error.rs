use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{guid::GuidParseError, page::PageSizeError};
use splinter::DecodeErr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("failed to parse GUID: {0}")]
    GuidParseError(#[from] GuidParseError),

    #[error(transparent)]
    PageSizeError(#[from] PageSizeError),

    #[error("failed to parse offsets: {0}")]
    OffsetsDecodeError(#[from] DecodeErr),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.to_string()).into_response()
    }
}
