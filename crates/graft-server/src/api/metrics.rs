use std::{ops::Deref, sync::LazyLock, time::Instant};

use axum::{
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use measured::{
    metric::histogram::Thresholds, Counter, FixedCardinalityLabel, Histogram, LabelGroup,
    MetricGroup,
};

use crate::metrics::split_gauge::{SplitGauge, SplitGaugeExt};

const REQUESTS_DURATION_SECONDS_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub static HTTP_METRICS: LazyLock<HttpMetrics> = LazyLock::new(HttpMetrics::new);

#[derive(FixedCardinalityLabel, Clone, Copy)]
enum StatusClass {
    Success = 200,
    Redirect = 300,
    ClientError = 400,
    ServerError = 500,
}

#[derive(LabelGroup)]
#[label(set = HttpLabelSet)]
struct HttpLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo)]
    path: &'a str,

    status_class: StatusClass,
}

#[derive(MetricGroup)]
#[metric(new())]
pub struct HttpMetrics {
    /// total number of http requests received
    requests_total: Counter,

    /// number of http requests pending
    requests_pending: SplitGauge,

    /// the request duration for all http requests handled
    #[metric(init = Histogram::with_metadata(Thresholds::with_buckets(
        REQUESTS_DURATION_SECONDS_BUCKETS
    )))]
    requests_duration_seconds: Histogram<{ REQUESTS_DURATION_SECONDS_BUCKETS.len() }>,
}

pub async fn metrics_layer(
    State(metrics): State<&'static HttpMetrics>,
    req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    metrics.requests_total.inc();
    let _g = metrics.requests_pending.guard();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    metrics.requests_duration_seconds.observe(latency);

    response
}

pub async fn metrics_layer_static(req: Request, next: Next) -> Response {
    let metrics = HTTP_METRICS.deref();

    let start = Instant::now();
    metrics.requests_total.inc();
    let _g = metrics.requests_pending.guard();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    metrics.requests_duration_seconds.observe(latency);

    // TODO: add labels
    // TODO: debug why from_fn is not working

    response
}

pub fn foo() -> Router<()> {
    Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .layer(middleware::from_fn(metrics_layer_static))
}
