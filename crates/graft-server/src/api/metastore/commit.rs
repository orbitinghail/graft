use std::{sync::Arc, time::SystemTime};

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::{lsn::LSN, page_count::PageCount, VolumeId};
use graft_proto::metastore::v1::{CommitRequest, CommitResponse};

use crate::{
    api::{
        error::{ApiErr, ApiErrCtx},
        extractors::Protobuf,
        response::ProtoResponse,
    },
    volume::commit::{CommitBuilder, CommitMeta},
};

use super::MetastoreApiState;

#[tracing::instrument(name = "metastore/v1/commit", skip(state, req))]
pub async fn handler(
    State(state): State<Arc<MetastoreApiState>>,
    Protobuf(req): Protobuf<CommitRequest>,
) -> Result<ProtoResponse<CommitResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let snapshot_lsn: Option<LSN> = req
        .snapshot_lsn
        .map(LSN::try_from)
        .transpose()
        .or_into_ctx()?;
    let page_count: PageCount = req.page_count.into();

    tracing::info!(
        ?vid,
        ?snapshot_lsn,
        ?page_count,
        num_segments = req.segments.len(),
    );

    // load the Volume's latest snapshot
    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, None)
        .await
        .or_into_ctx()?;

    let commit_lsn = match (snapshot_lsn, snapshot.as_ref().map(|s| s.lsn())) {
        (None, None) => LSN::FIRST,
        (Some(snapshot), Some(latest)) if snapshot == latest => latest.saturating_next(),

        // in every other case, the commit is out of sync
        // TODO: implement page based MVCC
        (snapshot, latest) => {
            return Err(Culprit::new_with_note(ApiErrCtx::RejectedCommit, format!("commit rejected for volume {vid}: snapshot lsn {snapshot:?} is out of sync with latest lsn {latest:?}")).into());
        }
    };

    let mut commit = CommitBuilder::default();
    for segment in req.segments {
        let sid = segment.sid().or_into_ctx()?;
        let offsets = segment.offsets().or_into_ctx()?;
        commit.write_offsets(sid.clone(), offsets.inner());
    }

    // checkpoint doesn't change
    let checkpoint = snapshot.map(|s| s.checkpoint()).unwrap_or(LSN::FIRST);

    let meta = CommitMeta::new(commit_lsn, checkpoint, page_count, SystemTime::now());
    let commit = commit.build(vid.clone(), meta.clone());

    // commit the new snapshot to the store
    state.store.commit(commit.clone()).await.or_into_ctx()?;

    // update the catalog
    let mut batch = state.catalog.batch_insert();
    batch.insert_commit(commit).or_into_ctx()?;
    batch.commit().or_into_ctx()?;

    // the commit was successful, return the new snapshot
    Ok(ProtoResponse::new(CommitResponse {
        snapshot: Some(meta.into_snapshot(&vid)),
    }))
}

#[cfg(test)]
mod tests {
    use axum::handler::Handler;
    use axum_test::TestServer;
    use graft_core::SegmentId;
    use graft_proto::common::v1::SegmentInfo;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::Splinter;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_commit_sanity() {
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
        let offsets = Splinter::from_iter([0u32]).serialize_to_bytes();

        // let's commit and validate the store 10 times
        for i in 1..10 {
            tracing::info!(i);

            let snapshot_lsn = (i != 1).then(|| i - 1);
            let lsn = LSN::new(i);

            let commit = CommitRequest {
                vid: vid.copy_to_bytes(),
                snapshot_lsn,
                page_count: 1,
                segments: vec![SegmentInfo::new(&SegmentId::random(), offsets.clone())],
            };

            // run the commit against the api
            let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
            let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
            let snapshot = resp.snapshot.unwrap();
            assert_eq!(snapshot.vid().unwrap(), &vid);
            assert_eq!(snapshot.lsn().expect("invalid LSN"), i);
            assert_eq!(snapshot.pages(), 1);
            assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());

            // check the commit in the store and the catalog
            let commit = store.get_commit(vid.clone(), lsn).await.unwrap();
            assert_eq!(commit.meta().lsn(), lsn);

            let snapshot = catalog.latest_snapshot(&vid).unwrap().unwrap();
            assert_eq!(snapshot.lsn(), lsn);
        }
    }
}
