use std::sync::Arc;

use axum::{extract::State, http::StatusCode};
use object_store::ObjectStore;

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(_state): State<Arc<MetastoreApiState<O>>>,
) -> Result<&'static str, StatusCode> {
    Ok("OK\n")
}
