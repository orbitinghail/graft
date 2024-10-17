use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use graft_core::{guid::VolumeId, lsn::LSN};
use graft_proto::pagestore::v1::ReadPagesRequest;
use splinter::SplinterRef;

use super::{error::ApiError, extractors::Protobuf, state::ApiState};

pub async fn handler(
    State(state): State<Arc<ApiState>>,
    Protobuf(req): Protobuf<ReadPagesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: LSN = req.lsn;
    let offsets = SplinterRef::from_bytes(req.offsets)?;

    let snapshot = state.catalog().snapshot(&vid)?;
    let needs_update = snapshot.is_none() || snapshot.is_some_and(|s| s.lsn() < lsn);

    if needs_update {
        // TODO: update the segment index
    }

    // 2. query the segment index for relevant segments
    //    -> this query is perfect as the index contains offset maps for every segment
    // 3. prefetch missing segments
    // 4. iterate through each segment, extracting relevant pages

    Ok(format!(
        "Read pages request: volume_id={}, lsn={}, offsets={:?}",
        vid, lsn, offsets
    ))
}
