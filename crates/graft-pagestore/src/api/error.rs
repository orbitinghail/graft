use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::{guid::GuidParseError, page::PageSizeError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("failed to parse GUID: {0}")]
    GuidParseError(#[from] GuidParseError),

    #[error(transparent)]
    PageSizeError(#[from] PageSizeError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use ApiError::*;

        let status = match self {
            GuidParseError(..) => StatusCode::BAD_REQUEST,
            PageSizeError(..) => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}
