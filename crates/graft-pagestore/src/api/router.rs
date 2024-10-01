use axum::{routing::post, Router};

use super::{read_pages, write_pages};

// IMPORTANT: Return Router without type params due to Axum semantics
// Reason: https://docs.rs/axum/latest/axum/struct.Router.html#what-s-in-routers-means
pub fn router() -> Router {
    let state = Default::default();
    Router::new()
        .route("/api/v1/pages/read", post(read_pages::handler))
        .route("/api/v1/pages/write", post(write_pages::handler))
        .with_state(state)
}
