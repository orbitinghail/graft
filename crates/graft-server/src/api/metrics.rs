use std::{sync::Arc, time::Instant};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use lasso::RodeoReader;
use measured::{
    metric::histogram::Thresholds, text::BufferedTextEncoder, CounterVec, FixedCardinalityLabel,
    HistogramVec, LabelGroup, MetricGroup,
};

use crate::metrics::{
    registry::Registry,
    split_gauge::{SplitGaugeVec, SplitGaugeVecExt},
};

const REQUESTS_DURATION_SECONDS_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(FixedCardinalityLabel, Clone, Copy)]
enum StatusClass {
    Success = 200,
    Redirect = 300,
    ClientError = 400,
    ServerError = 500,
}

impl From<StatusCode> for StatusClass {
    fn from(status: StatusCode) -> Self {
        match status.as_u16() {
            200..=299 => StatusClass::Success,
            300..=399 => StatusClass::Redirect,
            400..=499 => StatusClass::ClientError,
            500..=599 => StatusClass::ServerError,
            s => unreachable!("unexpected status code: {s}"),
        }
    }
}

#[derive(LabelGroup, Clone, Copy)]
#[label(set = ResponseLabelSet)]
struct ResponseLabelGroup<'a> {
    #[label(fixed_with = Arc<RodeoReader>)]
    path: &'a str,

    status_class: StatusClass,
}

#[derive(LabelGroup, Clone, Copy)]
#[label(set = InFlightLabelSet)]
struct InFlightLabelGroup<'a> {
    #[label(fixed_with = Arc<RodeoReader>)]
    path: &'a str,
}

#[derive(MetricGroup)]
#[metric(new(paths: Arc<RodeoReader>))]
pub struct HttpMetrics {
    /// number of in flight http requests
    #[metric(label_set = InFlightLabelSet::new(paths.clone()))]
    requests_pending: SplitGaugeVec<InFlightLabelSet>,

    /// total number of completed http requests
    #[metric(label_set = ResponseLabelSet::new(paths.clone()))]
    requests_total: CounterVec<ResponseLabelSet>,

    /// duration histogram of completed http requests
    #[metric(
        init = HistogramVec::with_label_set_and_metadata(
            ResponseLabelSet::new(paths.clone()),
            Thresholds::with_buckets(REQUESTS_DURATION_SECONDS_BUCKETS
        )
    ))]
    requests_duration_seconds:
        HistogramVec<ResponseLabelSet, { REQUESTS_DURATION_SECONDS_BUCKETS.len() }>,
}

pub async fn metrics_layer(
    State(metrics): State<Arc<HttpMetrics>>,
    req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();

    // TODO: intern this
    let path = req.uri().path().to_string();

    metrics
        .requests_pending
        .inc(InFlightLabelGroup { path: &path });

    let response = next.run(req).await;

    let status_class = response.status().into();
    let pend_label = InFlightLabelGroup { path: &path };
    let resp_label = ResponseLabelGroup { path: &path, status_class };

    metrics.requests_pending.dec(pend_label);
    metrics.requests_total.inc(resp_label);
    metrics
        .requests_duration_seconds
        .observe(resp_label, start.elapsed().as_secs_f64());

    response
}

pub async fn metrics_handler(
    State(registry): State<Arc<Registry>>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut encoder = BufferedTextEncoder::new();
    registry
        .collect_group_into(&mut encoder)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(encoder.finish())
}
