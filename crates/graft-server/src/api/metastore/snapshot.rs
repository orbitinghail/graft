use std::sync::Arc;

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::metastore::v1::{SnapshotRequest, SnapshotResponse};

use crate::api::{
    error::{ApiErr, ApiErrCtx},
    extractors::Protobuf,
    response::ProtoResponse,
};

use super::MetastoreApiState;

#[tracing::instrument(name = "metastore/v1/snapshot", skip(state, req))]
pub async fn handler(
    State(state): State<Arc<MetastoreApiState>>,
    Protobuf(req): Protobuf<SnapshotRequest>,
) -> Result<ProtoResponse<SnapshotResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: Option<LSN> = req.lsn.map(LSN::try_from).transpose().or_into_ctx()?;

    tracing::info!(?vid, ?lsn);

    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, lsn)
        .await
        .or_into_ctx()?;

    if let Some(snapshot) = snapshot {
        Ok(ProtoResponse::new(SnapshotResponse {
            snapshot: Some(snapshot.into_snapshot()),
        }))
    } else {
        return Err(Culprit::new_with_note(
            ApiErrCtx::SnapshotMissing,
            format!("volume {vid} is missing snapshot at {lsn:?}"),
        )
        .into());
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use axum::{handler::Handler, http::StatusCode};
    use axum_test::TestServer;
    use graft_core::{gid::ClientId, page_count::PageCount, SegmentId};
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::Splinter;

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

    #[graft_test::test]
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
        let cid = ClientId::random();

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
        let meta = CommitMeta::new(
            vid.clone(),
            cid,
            LSN::FIRST,
            LSN::FIRST,
            PageCount::new(1),
            SystemTime::now(),
        );
        let mut commit = CommitBuilder::new_with_capacity(meta, 1);
        commit.write_graft(
            SegmentId::random(),
            Splinter::from_slice(&[0]).serialize_to_bytes(),
        );
        store.commit(commit.build()).await.unwrap();

        // request latest
        let req = SnapshotRequest { vid: vid.copy_to_bytes(), lsn: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = SnapshotResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.vid().unwrap(), &vid);
        assert_eq!(snapshot.lsn().unwrap(), 1);
        assert_eq!(snapshot.pages(), 1);
        assert!(snapshot.timestamp.is_some());
    }
}
