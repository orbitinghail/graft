use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use graft_core::{guid::VolumeId, lsn::LSN, offset::Offset};
use graft_proto::pagestore::v1::ReadPagesRequest;

use super::{error::ApiError, extractors::Protobuf, state::ApiState};

pub async fn handler(
    State(state): State<Arc<ApiState>>,
    Protobuf(req): Protobuf<ReadPagesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let vid: VolumeId = req.vid.try_into()?;

    Ok(format!(
        "Read pages request: volume_id={}, lsn={}, offsets={:?}",
        vid, req.lsn, req.offsets
    ))
}
