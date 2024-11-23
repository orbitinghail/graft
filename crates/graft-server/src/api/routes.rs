use std::sync::Arc;

use axum::{
    middleware,
    routing::{get, MethodRouter},
    Router,
};
use lasso::Rodeo;

use crate::metrics::registry::Registry;

use super::{
    health,
    metrics::{self, metrics_layer, HttpMetrics},
};

pub type Routes<S> = Vec<(&'static str, MethodRouter<S>)>;

pub fn build_router<S: Send + Sync + Clone + 'static>(
    mut registry: Registry,
    state: S,
    routes: Vec<(&'static str, MethodRouter<S>)>,
) -> Router {
    let mut paths = Rodeo::default();
    paths.get_or_intern_static("/health");
    paths.get_or_intern_static("/metrics");

    let router = routes
        .into_iter()
        .fold(Router::new(), |router, (path, handler)| {
            paths.get_or_intern_static(path);
            router.route(path, handler)
        })
        .with_state(state);

    let paths = Arc::new(paths.into_reader());
    let metrics = Arc::new(HttpMetrics::new(paths));
    registry.register_http(metrics.clone());

    router
        .route("/health", get(health::handler))
        .route("/metrics", get(metrics::metrics_handler))
        .with_state(Arc::new(registry))
        .layer(middleware::from_fn_with_state(metrics, metrics_layer))
}
