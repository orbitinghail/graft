use std::sync::Arc;

use axum::extract::State;
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::metastore::v1::{SnapshotRequest, SnapshotResponse};
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<SnapshotRequest>,
) -> Result<ProtoResponse<SnapshotResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: Option<LSN> = req.lsn;

    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, lsn)
        .await?;

    if let Some(snapshot) = snapshot {
        Ok(ProtoResponse::new(SnapshotResponse {
            snapshot: Some(snapshot.into_snapshot(&vid)),
        }))
    } else {
        Err(ApiErr::SnapshotMissing(vid, lsn))
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use axum::{handler::Handler, http::StatusCode};
    use axum_test::TestServer;
    use graft_core::SegmentId;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::Splinter;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{
            catalog::VolumeCatalog,
            commit::{CommitBuilder, CommitMeta},
            store::VolumeStore,
            updater::VolumeCatalogUpdater,
        },
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_snapshot_sanity() {
        let store = Arc::new(InMemory::default());
        let store = Arc::new(VolumeStore::new(store));
        let catalog = VolumeCatalog::open_temporary().unwrap();

        let state = Arc::new(MetastoreApiState::new(
            store.clone(),
            catalog.clone(),
            VolumeCatalogUpdater::new(8),
        ));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        let vid = VolumeId::random();

        // case 1: catalog and store are empty

        // request latest
        let req = SnapshotRequest { vid: vid.copy_to_bytes(), lsn: None };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // request specific
        let req = SnapshotRequest { vid: vid.copy_to_bytes(), lsn: Some(10) };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // case 2: catalog is empty, store has a commit
        let meta = CommitMeta::new(0, 0, 0, SystemTime::now());
        let mut commit = CommitBuilder::default();
        commit.write_offsets(
            SegmentId::random(),
            &[0u32]
                .into_iter()
                .collect::<Splinter>()
                .serialize_to_bytes(),
        );
        store.commit(commit.build(vid.clone(), meta)).await.unwrap();

        // request latest
        let req = SnapshotRequest { vid: vid.copy_to_bytes(), lsn: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = SnapshotResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.vid().unwrap(), &vid);
        assert_eq!(snapshot.lsn(), 0);
        assert_eq!(snapshot.last_offset(), 0);
        assert!(snapshot.timestamp.is_some());
    }
}
