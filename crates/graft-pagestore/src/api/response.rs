use axum::{body::Body, http::Response, response::IntoResponse};
use bytes::BytesMut;
use prost::Message;

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

        buf.freeze().into_response()
    }
}
