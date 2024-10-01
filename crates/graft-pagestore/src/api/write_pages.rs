use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};

use super::state::ApiState;

pub async fn handler(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    "Coming soon"
}
