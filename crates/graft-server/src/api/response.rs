use axum::{
    body::Body,
    http::{Response, header},
    response::IntoResponse,
};
use bytes::BytesMut;
use prost::Message;

use super::extractors::CONTENT_TYPE_PROTOBUF;

pub struct ProtoResponse<M> {
    msg: M,
}

impl<M> ProtoResponse<M> {
    pub fn new(msg: M) -> Self {
        Self { msg }
    }
}

impl<M: Message> IntoResponse for ProtoResponse<M> {
    fn into_response(self) -> Response<Body> {
        let mut buf = BytesMut::with_capacity(self.msg.encoded_len());

        self.msg
            .encode(&mut buf)
            .expect("insufficient buffer capacity");

        (
            [(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)],
            buf.freeze(),
        )
            .into_response()
    }
}
