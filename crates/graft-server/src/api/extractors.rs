use axum::{
    async_trait,
    extract::{FromRequest, Request},
    http::{header, HeaderValue},
};
use bytes::Bytes;

use super::error::ApiErr;

pub const CONTENT_TYPE_PROTOBUF: HeaderValue = HeaderValue::from_static("application/x-protobuf");

pub struct Protobuf<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for Protobuf<T>
where
    S: Send + Sync,
    T: prost::Message + Default,
{
    type Rejection = ApiErr;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let is_protobuf = req
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|v| v == CONTENT_TYPE_PROTOBUF);
        if !is_protobuf {
            return Err(ApiErr::InvalidRequestBody(
                "invalid content type".to_string(),
            ));
        }

        let body = Bytes::from_request(req, state)
            .await
            .map_err(|err| ApiErr::InvalidRequestBody(err.to_string()))?;

        if body.is_empty() {
            return Err(ApiErr::InvalidRequestBody("empty body".to_string()));
        }

        let value = T::decode(body).map_err(|err| ApiErr::InvalidRequestBody(err.to_string()))?;
        Ok(Protobuf(value))
    }
}
