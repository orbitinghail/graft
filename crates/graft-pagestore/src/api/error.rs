use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use graft_core::guid::GuidParseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("failed to parse GUID: {0}")]
    GuidParseError(#[from] GuidParseError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::GuidParseError(..) => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}
