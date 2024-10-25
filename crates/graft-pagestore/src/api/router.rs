use std::sync::Arc;

use axum::{routing::post, Router};
use object_store::ObjectStore;

use crate::storage::cache::Cache;

use super::{read_pages, state::ApiState, write_pages};

pub fn router<O, C>() -> Router<Arc<ApiState<O, C>>>
where
    O: ObjectStore + Sync + Send + 'static,
    C: Cache + Sync + Send + 'static,
{
    Router::new()
        .route("/api/v1/pages/read", post(read_pages::handler))
        .route("/api/v1/pages/write", post(write_pages::handler))
}
