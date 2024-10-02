use std::sync::Arc;

use axum::{routing::post, Router};

use super::{read_pages, state::ApiState, write_pages};

pub fn router() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/api/v1/pages/read", post(read_pages::handler))
        .route("/api/v1/pages/write", post(write_pages::handler))
}
