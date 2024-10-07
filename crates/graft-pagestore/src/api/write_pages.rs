use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use graft_core::{guid::VolumeId, offset::Offset, page::Page};
use graft_proto::pagestore::v1::WritePagesRequest;

use crate::segment::bus::WritePageReq;

use super::{error::ApiError, extractors::Protobuf, state::ApiState};

pub async fn handler(
    State(state): State<Arc<ApiState>>,
    Protobuf(req): Protobuf<WritePagesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let vid: VolumeId = req.vid.try_into()?;

    for page in req.pages {
        let offset: Offset = page.offset;
        let page: Page = page.data.try_into()?;

        state
            .write_page(WritePageReq::new(vid.clone(), offset, page))
            .await;
    }

    Ok("Write pages request")
}
