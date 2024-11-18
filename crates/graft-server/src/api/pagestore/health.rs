use std::sync::Arc;

use axum::{extract::State, http::StatusCode};
use object_store::ObjectStore;

use crate::segment::cache::Cache;

use super::PagestoreApiState;

pub async fn handler<O: ObjectStore, C: Cache>(
    State(_state): State<Arc<PagestoreApiState<O, C>>>,
) -> Result<&'static str, StatusCode> {
    Ok("OK\n")
}
