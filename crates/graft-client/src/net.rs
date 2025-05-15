use bytes::{BufMut, BytesMut};
use culprit::{Culprit, ResultExt};
use graft_core::byte_unit::ByteUnit;
use graft_proto::common::v1::GraftErr;
use http::{
    HeaderName, HeaderValue, Uri,
    header::AUTHORIZATION,
    uri::{Builder, PathAndQuery},
};
use std::{any::type_name, sync::Arc, time::Duration};
use tracing::field;
use url::Url;

use ureq::{Agent, config::AutoHeaderValue};

use crate::{USER_AGENT, error::ClientErr};

use prost::Message;

const CONTENT_TYPE: HeaderName = HeaderName::from_static("content-type");
const APPLICATION_PROTOBUF: HeaderValue = HeaderValue::from_static("application/x-protobuf");
const MAX_READ_SIZE: ByteUnit = ByteUnit::from_mb(8);

#[derive(Debug, Clone)]
pub(crate) struct EndpointBuilder {
    endpoint: Uri,
}

impl From<Url> for EndpointBuilder {
    fn from(endpoint: Url) -> Self {
        let endpoint: Uri = endpoint.as_str().parse().expect("url is valid uri");
        assert!(
            endpoint.path_and_query().is_none_or(|p| p.path() == "/"),
            "endpoint can not include a path {endpoint}"
        );
        Self { endpoint }
    }
}

impl EndpointBuilder {
    pub(crate) fn build(&self, path: &'static str) -> Result<Uri, http::Error> {
        assert!(path.starts_with("/"), "path must begin with /");
        let path = PathAndQuery::from_static(path);
        let uri = Builder::from(self.endpoint.clone())
            .path_and_query(path)
            .build()?;
        Ok(uri)
    }
}

#[derive(Debug, Clone)]
pub struct NetClient {
    api_token: Option<String>,
    agent: Agent,
}

impl NetClient {
    pub fn new(api_token: Option<String>) -> Self {
        Self {
            api_token,
            agent: Agent::config_builder()
                .user_agent(AutoHeaderValue::Provided(Arc::new(USER_AGENT.to_string())))
                .http_status_as_error(false)
                .max_idle_age(Duration::from_secs(300))
                .timeout_connect(Some(Duration::from_secs(60)))
                .timeout_recv_response(Some(Duration::from_secs(60)))
                .timeout_global(Some(Duration::from_secs(300)))
                .build()
                .new_agent(),
        }
    }

    pub(crate) fn send<Msg: Message, Resp: Message + Default>(
        &self,
        uri: Uri,
        msg: Msg,
    ) -> Result<Resp, Culprit<ClientErr>> {
        let span = tracing::trace_span!(
            "NetClient::send",
            path = uri.path(),
            status = field::Empty,
            err = field::Empty
        )
        .entered();

        let req = self
            .agent
            .post(uri)
            .header(CONTENT_TYPE, APPLICATION_PROTOBUF);

        let req = if let Some(token) = &self.api_token {
            req.header(AUTHORIZATION, format!("Bearer {token}"))
        } else {
            req
        };

        let resp = match req.send(&msg.encode_to_vec()) {
            Ok(resp) => resp,
            Err(err) => {
                span.record("err", err.to_string());
                return Err(err.into());
            }
        };

        let status = resp.status();
        span.record("status", status.as_u16());

        let content_type = resp.headers().get(CONTENT_TYPE);
        if content_type != Some(&APPLICATION_PROTOBUF) {
            return Err(
                Culprit::new(ClientErr::ProtobufDecodeErr).with_note(format!(
                    "expected content type '{}' but received {:?}",
                    APPLICATION_PROTOBUF.to_str().unwrap(),
                    content_type
                )),
            );
        }

        let success = (200..300).contains(&status.as_u16());

        // read the response into a Bytes object
        let reader = resp
            .into_body()
            .into_with_config()
            .limit(MAX_READ_SIZE.as_u64());
        let mut writer = BytesMut::new().writer();
        std::io::copy(&mut reader.reader(), &mut writer).or_into_ctx()?;
        let body = writer.into_inner().freeze();
        let body_size = ByteUnit::new(body.len() as u64);

        if success {
            Ok(Resp::decode(body).map_err(|err| {
                let note = format!(
                    "failed to decode response body into {} from buffer of size {}",
                    type_name::<Resp>(),
                    body_size
                );
                Culprit::from_err(err).with_note(note)
            })?)
        } else {
            let err = GraftErr::decode(body).map_err(|err| {
                let note = format!(
                    "failed to decode response body into GraftErr from buffer of size {body_size}"
                );
                Culprit::from_err(err).with_note(note)
            })?;

            // 5xx errors are not expected from client requests unless the graft
            // error signals that the service is temporarily unavailable
            precept::expect_always_or_unreachable!(
                !(500..600).contains(&status.as_u16()) || err.code() == graft_proto::GraftErrCode::ServiceUnavailable,
                "client requests should not return 5xx errors",
                {
                    "status": status.as_u16(),
                    "code": err.code().as_str_name(),
                    "message": err.message
                }
            );
            Err(err.into())
        }
    }
}
