use std::sync::Arc;

use axum::Router;

pub struct MetastoreApiState {}

pub fn metastore_router() -> Router<Arc<MetastoreApiState>> {
    Router::new()
    // .route("/api/v1/snapshot", post(snapshot::handler))
    // .route("/api/v1/pull_offsets", post(pull_offsets::handler))
    // .route("/api/v1/pull_segments", post(pull_segments::handler))
    // .route("/api/v1/commit", post(commit::handler))
    // .route("/api/v1/checkpoint", post(checkpoint::handler))
}
