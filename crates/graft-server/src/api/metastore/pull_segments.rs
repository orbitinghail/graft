use std::sync::Arc;

use axum::extract::State;
use graft_core::VolumeId;
use graft_proto::{
    common::v1::LsnRange,
    metastore::v1::{PullSegmentsRequest, PullSegmentsResponse},
};
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::MetastoreApiState;

/// Returns a list of segments added between the provided LSN (exclusive) and the
/// latest LSN (inclusive). This method will also return the latest Snapshot of
/// the Volume. If the provided LSN is missing or before the last checkpoint,
/// only segments starting at the last checkpoint will be returned.
pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<PullSegmentsRequest>,
) -> Result<ProtoResponse<PullSegmentsResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsns: LsnRange = req.range.unwrap_or_default();

    // load the snapshot at the end of the lsn range
    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, lsns.end())
        .await?;

    let Some(snapshot) = snapshot else {
        return Err(ApiErr::SnapshotMissing(vid, lsns.end()));
    };

    // resolve the start of the range; skipping up to the last checkpoint if needed
    let checkpoint = snapshot.checkpoint();
    let start_lsn = lsns.start().unwrap_or(checkpoint).max(checkpoint);

    // calculate the resolved lsn range
    let lsns = start_lsn..(snapshot.lsn() + 1);

    // ensure the catalog contains the requested LSNs
    state
        .updater
        .update_catalog_from_store_in_range(&state.store, &state.catalog, &vid, lsns)
        .await?;

    // TODO: retrieve the segments from the catalog
    // TODO: return a PullSegmentsResponse

    todo!()
}

#[cfg(test)]
mod tests {
    use axum::handler::Handler;
    use axum_test::TestServer;
    use object_store::memory::InMemory;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{catalog::VolumeCatalog, store::VolumeStore},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_pull_segments_sanity() {
        let store = Arc::new(InMemory::default());
        let store = Arc::new(VolumeStore::new(store));
        let catalog = VolumeCatalog::open_temporary().unwrap();

        let state = Arc::new(MetastoreApiState::new(store.clone(), catalog.clone(), 8));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        let vid = VolumeId::random();

        todo!("implement sanity test")
    }
}
