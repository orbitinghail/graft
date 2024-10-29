use axum::{
    async_trait,
    extract::{FromRequest, Request},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

pub const CONTENT_TYPE_PROTOBUF: HeaderValue = HeaderValue::from_static("application/x-protobuf");

pub struct Protobuf<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for Protobuf<T>
where
    S: Send + Sync,
    T: prost::Message + Default,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let is_protobuf = req
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|v| v == CONTENT_TYPE_PROTOBUF);
        if !is_protobuf {
            return Err(
                (StatusCode::BAD_REQUEST, "invalid content type".to_string()).into_response(),
            );
        }

        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;

        if body.is_empty() {
            return Err((StatusCode::BAD_REQUEST, "empty body".to_string()).into_response());
        }

        let value = T::decode(body)
            .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())?;
        Ok(Protobuf(value))
    }
}
