use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::{
    common::v1::Snapshot,
    metastore::v1::{SnapshotRequest, SnapshotResponse},
};
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<SnapshotRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: Option<LSN> = req.lsn;

    // if a specific lsn is requested and we have a snapshot for it, return it
    if let Some(lsn) = lsn {
        if let Some(snapshot) = state.catalog().snapshot(vid.clone(), lsn)? {
            return Ok(ProtoResponse::new(SnapshotResponse {
                snapshot: Some(Snapshot::new(
                    &vid,
                    snapshot.lsn(),
                    snapshot.last_offset(),
                    snapshot.system_time(),
                )),
            }));
        }
    }

    // otherwise we need to update the catalog
    state
        .updater
        .update_catalog_from_store(state.store(), state.catalog(), &vid, lsn)
        .await?;

    // return the requested snapshot or latest
    let snapshot = if let Some(lsn) = lsn {
        state.catalog().snapshot(vid.clone(), lsn)?
    } else {
        state.catalog().latest_snapshot(&vid)?
    };

    if let Some(snapshot) = snapshot {
        Ok(ProtoResponse::new(SnapshotResponse {
            snapshot: Some(Snapshot::new(
                &vid,
                snapshot.lsn(),
                snapshot.last_offset(),
                snapshot.system_time(),
            )),
        }))
    } else {
        Err(ApiErr::SnapshotMissing(vid, lsn))
    }
}

#[cfg(test)]
mod tests {
    use axum::{handler::Handler, http::StatusCode};
    use axum_test::TestServer;
    use graft_core::SegmentId;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::Splinter;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{catalog::VolumeCatalog, store::VolumeStore},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_snapshot_sanity() {
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

        // case 1: catalog and store are empty

        // request latest
        let req = SnapshotRequest { vid: vid.clone().into(), lsn: None };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // request specific
        let req = SnapshotRequest { vid: vid.clone().into(), lsn: Some(10) };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // case 2: catalog is empty, store has a commit
        let mut commit = store.prepare(vid.clone(), 0, 0);
        commit.write_offsets(
            SegmentId::random(),
            &[0u32]
                .into_iter()
                .collect::<Splinter>()
                .serialize_to_bytes(),
        );
        store.commit(commit).await.unwrap();

        // request latest
        let req = SnapshotRequest { vid: vid.clone().into(), lsn: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = SnapshotResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.vid().unwrap(), &vid);
        assert_eq!(snapshot.lsn(), 0);
        assert_eq!(snapshot.last_offset(), 0);
        assert!(snapshot.timestamp.is_some());
    }
}
