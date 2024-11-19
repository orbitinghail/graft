use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use super::MetastoreApiState;

pub async fn handler(
    State(_state): State<Arc<MetastoreApiState>>,
) -> Result<&'static str, StatusCode> {
    Ok("OK\n")
}
