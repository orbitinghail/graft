use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use crate::segment::cache::Cache;

use super::PagestoreApiState;

pub async fn handler<C: Cache>(
    State(_state): State<Arc<PagestoreApiState<C>>>,
) -> Result<&'static str, StatusCode> {
    Ok("OK\n")
}
