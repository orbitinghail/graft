use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse};
use measured::{MetricGroup, metric::name::WithNamespace, text::BufferedTextEncoder};

use crate::metrics::registry::Registry;

pub async fn handler(
    State(registry): State<Arc<Registry>>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut encoder = BufferedTextEncoder::new();
    WithNamespace::new("graft", registry)
        .collect_group_into(&mut encoder)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(encoder.finish())
}
