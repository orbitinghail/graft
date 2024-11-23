use std::sync::Arc;

use axum::{
    routing::{get, MethodRouter},
    Router,
};

use crate::metrics::registry::Registry;

use super::{
    health,
    metrics::{self},
};

pub type Routes<S> = Vec<(&'static str, MethodRouter<S>)>;

pub fn build_router<S: Send + Sync + Clone + 'static>(
    registry: Registry,
    state: S,
    routes: Vec<(&'static str, MethodRouter<S>)>,
) -> Router {
    let router = routes
        .into_iter()
        .fold(Router::new(), |router, (path, handler)| {
            router.route(path, handler)
        })
        .with_state(state);

    router
        .route("/health", get(health::handler))
        .route("/metrics", get(metrics::handler))
        .with_state(Arc::new(registry))
}
