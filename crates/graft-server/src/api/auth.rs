use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use rusty_paseto::prelude::*;
use serde::{Deserialize, Serialize};

use super::error::{ApiErr, ApiErrCtx};

#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AuthState {
    #[serde(skip_serializing, deserialize_with = "deserialize_key")]
    key: Key<32>,
}

fn deserialize_key<'de, D>(deserializer: D) -> Result<Key<32>, D::Error>
where
    D: serde::Deserializer<'de>,
    D::Error: serde::de::Error,
{
    let key = String::deserialize(deserializer)?;
    key.as_str()
        .try_into()
        .map_err(serde::de::Error::custom)
}

impl std::fmt::Debug for AuthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("key", &"[redacted]")
            .finish()
    }
}

pub async fn auth_layer(
    State(state): State<AuthState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiErr> {
    // extract bearer token from request.headers()
    let token = request
        .headers()
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ApiErr::from(ApiErrCtx::Unauthorized))?;

    // validate the token
    let key = state.key.into();
    PasetoParser::<V4, Local>::default()
        .parse(token, &key)
        .map_err(|err| {
            tracing::error!("paseto validation failure: {err:?}");
            ApiErr::from(ApiErrCtx::Unauthorized)
        })?;

    Ok(next.run(request).await)
}
