use std::sync::Arc;

use axum::extract::State;
use graft_core::{lsn::LSN, offset::Offset, VolumeId};
use graft_proto::metastore::v1::{CommitRequest, CommitResponse};
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<CommitRequest>,
) -> Result<ProtoResponse<CommitResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let snapshot_lsn: LSN = req.snapshot_lsn;
    let last_offset: Offset = req.last_offset;

    // reminder: need to determine if this commit is a checkpoint

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
    async fn test_commit_sanity() {
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
