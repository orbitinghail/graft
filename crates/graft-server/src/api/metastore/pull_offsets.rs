use std::sync::Arc;

use axum::extract::State;
use graft_core::VolumeId;
use graft_proto::{
    common::v1::LsnRange,
    metastore::v1::{PullOffsetsRequest, PullOffsetsResponse},
};
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<PullOffsetsRequest>,
) -> Result<ProtoResponse<PullOffsetsResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsns: LsnRange = req.range.unwrap_or_default();

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
    async fn test_pull_offsets_sanity() {
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
