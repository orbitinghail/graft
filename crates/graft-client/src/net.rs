use bytes::{BufMut, BytesMut};
use culprit::{Culprit, ResultExt};
use graft_core::byte_unit::ByteUnit;
use graft_proto::common::v1::GraftErr;
use std::{any::type_name, io::Read};

use ureq::{Agent, OrAnyStatus};
use url::Url;

use crate::error::ClientErr;

use prost::Message;

const CONTENT_TYPE: &str = "Content-Type";
const APPLICATION_PROTOBUF: &str = "application/x-protobuf";
const MAX_READ_SIZE: ByteUnit = ByteUnit::from_mb(8);

pub fn prost_request<Req: Message, Resp: Message + Default>(
    agent: &Agent,
    url: Url,
    req: Req,
) -> Result<Resp, Culprit<ClientErr>> {
    log::trace!("sending request to {}", url);

    let resp = agent
        .request_url("POST", &url)
        .set(CONTENT_TYPE, APPLICATION_PROTOBUF)
        .send_bytes(&req.encode_to_vec())
        .or_any_status()?;

    if resp.content_type() != APPLICATION_PROTOBUF {
        return Err(
            Culprit::new(ClientErr::ProtobufDecodeErr).with_note(format!(
                "expected content type '{}' but received '{}'",
                APPLICATION_PROTOBUF,
                resp.content_type()
            )),
        );
    }

    let status = resp.status();
    log::trace!("received response with status {}", resp.status());

    let success = (200..300).contains(&status);

    // read the response into a Bytes object
    let mut reader = resp.into_reader().take(MAX_READ_SIZE.as_u64());
    let mut writer = BytesMut::new().writer();
    std::io::copy(&mut reader, &mut writer).or_into_ctx()?;
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
                "failed to decode response body into GraftErr from buffer of size {}",
                body_size
            );
            Culprit::from_err(err).with_note(note)
        })?;
        precept::expect_always_or_unreachable!(
            !(500..600).contains(&status),
            "client requests should not return 5xx errors",
            { "status": status, "code": err.code().as_str_name(), "message": err.message }
        );
        Err(err.into())
    }
}
