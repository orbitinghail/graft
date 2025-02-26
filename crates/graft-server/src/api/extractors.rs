use axum::{
    extract::{FromRequest, Request},
    http::{header, HeaderValue},
};
use bytes::Bytes;
use culprit::Culprit;

use super::error::{ApiErr, ApiErrCtx};

pub const CONTENT_TYPE_PROTOBUF: HeaderValue = HeaderValue::from_static("application/x-protobuf");

pub struct Protobuf<T>(pub T);

impl<S, T> FromRequest<S> for Protobuf<T>
where
    S: Send + Sync,
    T: prost::Message + Default,
{
    type Rejection = ApiErr;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_type = req.headers().get(header::CONTENT_TYPE);
        let is_protobuf = content_type == Some(&CONTENT_TYPE_PROTOBUF);
        if !is_protobuf {
            return Err(Culprit::new_with_note(
                ApiErrCtx::InvalidRequestBody,
                format!(
                    "expected content type '{CONTENT_TYPE_PROTOBUF:?}' but received '{content_type:?}'"
                ),
            )
            .into());
        }

        let body = Bytes::from_request(req, state).await.map_err(|err| {
            Culprit::new_with_note(ApiErrCtx::InvalidRequestBody, err.to_string())
        })?;

        if body.is_empty() {
            return Err(Culprit::new_with_note(ApiErrCtx::InvalidRequestBody, "empty body").into());
        }

        let value = T::decode(body).map_err(|err| {
            Culprit::new_with_note(ApiErrCtx::InvalidRequestBody, err.to_string())
        })?;
        Ok(Protobuf(value))
    }
}
