use culprit::Culprit;
use graft_proto::common::v1::GraftErr;

use reqwest::header::CONTENT_TYPE;

use crate::error::ClientErr;

use reqwest::Url;

use prost::Message;

pub async fn prost_request<Req: Message, Resp: Message + Default>(
    http: &reqwest::Client,
    url: Url,
    req: Req,
) -> Result<Resp, Culprit<ClientErr>> {
    let req = http
        .post(url)
        .body(req.encode_to_vec())
        .header(CONTENT_TYPE, "application/x-protobuf");
    log::trace!("sending request: {:?}", req);
    let resp = req.send().await?;
    log::trace!("received response: {:?}", resp);
    let success = resp.status().is_success();
    let body = resp.bytes().await?;
    if success {
        Ok(Resp::decode(body)?)
    } else {
        let err = GraftErr::decode(body)?;
        Err(err.into())
    }
}
