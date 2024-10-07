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

    // subscribe to the broadcast channel
    // let commit_rx = state.subscribe_commits();

    for page in req.pages {
        let offset: Offset = page.offset;
        let page: Page = page.data.try_into()?;

        state
            .write_page(WritePageReq::new(vid.clone(), offset, page))
            .await;
    }

    // TODO listen for commit messages, buffering up our response to the client
    // TODO switch to a streaming model

    Ok("Write pages request")
}
