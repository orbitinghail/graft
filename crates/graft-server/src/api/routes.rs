use std::sync::Arc;

use axum::{
    Router,
    middleware::from_fn_with_state,
    routing::{MethodRouter, get},
};
use tower_http::{catch_panic::CatchPanicLayer, compression::CompressionLayer};

use crate::metrics::registry::Registry;

use super::{
    auth::{AuthState, auth_layer},
    health,
    metrics::{self},
};

pub type Routes<S> = Vec<(&'static str, MethodRouter<S>)>;

pub fn build_router<S: Send + Sync + Clone + 'static>(
    registry: Registry,
    auth: Option<AuthState>,
    state: S,
    routes: Vec<(&'static str, MethodRouter<S>)>,
) -> Router {
    let router = routes
        .into_iter()
        .fold(Router::new(), |router, (path, handler)| {
            router.route(path, handler)
        })
        .with_state(state);

    let compression_layer = CompressionLayer::new()
        .gzip(true)
        .deflate(true)
        .br(true)
        .zstd(true);

    let panic_layer = CatchPanicLayer::custom(crate::api::error::handle_panic);

    let router = router.layer(compression_layer);

    let router = if let Some(auth) = auth {
        router.layer(from_fn_with_state(auth, auth_layer))
    } else {
        router
    };

    let router = router
        .route("/metrics", get(metrics::handler))
        .with_state(Arc::new(registry))
        .route("/health", get(health::handler));

    router.layer(panic_layer)
}
