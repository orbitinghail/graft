use std::{sync::Arc, time::SystemTime};

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::{gid::ClientId, lsn::LSN, page_count::PageCount, VolumeId};
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
    let vid = VolumeId::try_from(req.vid).or_into_culprit("failed to parse VolumeId")?;
    let cid = ClientId::try_from(req.cid).or_into_culprit("failed to parse ClientId")?;
    let snapshot_lsn: Option<LSN> = req
        .snapshot_lsn
        .map(LSN::try_from)
        .transpose()
        .or_into_ctx()?;
    let page_count: PageCount = req.page_count.into();

    tracing::info!(
        ?vid,
        ?cid,
        ?snapshot_lsn,
        ?page_count,
        num_segments = req.segments.len(),
    );

    // calculate the commit_lsn
    let commit_lsn = snapshot_lsn.map_or(LSN::FIRST, |lsn| lsn.saturating_next());

    // load the Volume's latest snapshot
    let latest_snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, None)
        .await
        .or_into_ctx()?;

    let latest_lsn = latest_snapshot.as_ref().map(|s| s.lsn());

    // if the client's snapshot is out of sync with the latest snapshot we can't
    // proceed with the commit
    if latest_lsn != snapshot_lsn {
        // to enable idempotent commits, we check to see if the clients commit
        // already landed.
        if Some(commit_lsn) <= latest_lsn {
            // load the commit snapshot
            let commit_snapshot = if latest_lsn == Some(commit_lsn) {
                latest_snapshot
            } else {
                state
                    .updater
                    .snapshot(&state.store, &state.catalog, &vid, Some(commit_lsn))
                    .await
                    .or_into_ctx()?
            };
            // if the commit snapshot's cid matches the cid, return a successful response
            if commit_snapshot.as_ref().map(|s| s.cid()) == Some(&cid) {
                return Ok(ProtoResponse::new(CommitResponse {
                    snapshot: commit_snapshot.map(|s| s.into_snapshot()),
                }));
            }
        }
        // otherwise reject this commit
        return Err(Culprit::new_with_note(
            ApiErrCtx::RejectedCommit,
            format!("commit rejected for volume {vid}: client snapshot lsn {snapshot_lsn:?} is out of sync with latest lsn {latest_lsn:?}")
        ).into());
    }

    // checkpoint doesn't change
    let checkpoint = latest_snapshot
        .map(|s| s.checkpoint())
        .unwrap_or(LSN::FIRST);

    let mut commit = CommitBuilder::new_with_capacity(
        CommitMeta::new(
            vid.clone(),
            cid,
            commit_lsn,
            checkpoint,
            page_count,
            SystemTime::now(),
        ),
        req.segments.len(),
    );
    for segment in req.segments {
        let sid = segment.sid().or_into_ctx()?;
        let graft = segment.graft().or_into_ctx()?;
        commit.write_graft(sid.clone(), graft.into_inner());
    }

    let commit = commit.build();

    // commit the new snapshot to the store
    state.store.commit(commit.clone()).await.or_into_ctx()?;

    // update the catalog
    let mut batch = state.catalog.batch_insert();
    batch.insert_commit(&commit).or_into_ctx()?;
    batch.commit().or_into_ctx()?;

    // the commit was successful, return the new snapshot
    Ok(ProtoResponse::new(CommitResponse {
        snapshot: Some(commit.into_snapshot()),
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

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater},
    };

    use super::*;

    #[graft_test::test]
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
        let cid = ClientId::random();
        let graft = Splinter::from_iter([0u32]).serialize_to_bytes();

        // let's commit and validate the store 10 times
        for i in 1..10 {
            tracing::info!(i);

            let snapshot_lsn = (i != 1).then(|| i - 1);
            let lsn = LSN::new(i);

            let commit = CommitRequest {
                vid: vid.copy_to_bytes(),
                cid: cid.copy_to_bytes(),
                snapshot_lsn,
                page_count: 1,
                segments: vec![SegmentInfo::new(&SegmentId::random(), graft.clone())],
            };

            // commit
            let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
            let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
            let snapshot = resp.snapshot.unwrap();
            assert_eq!(snapshot.vid().unwrap(), &vid);
            assert_eq!(snapshot.lsn().expect("invalid LSN"), i);
            assert_eq!(snapshot.pages(), 1);
            assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());

            // commit 2 more times to ensure idempotency
            for _ in 0..2 {
                let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
                let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
                assert_eq!(resp.snapshot.unwrap(), snapshot);
            }

            // check the commit in the store and the catalog
            let commit = store.get_commit(vid.clone(), lsn).await.unwrap();
            assert_eq!(commit.meta().lsn(), lsn);

            let snapshot = catalog.latest_snapshot(&vid).unwrap().unwrap();
            assert_eq!(snapshot.lsn(), lsn);
        }

        // ensure that an older commit is still idempotent
        let commit = CommitRequest {
            vid: vid.copy_to_bytes(),
            cid: cid.copy_to_bytes(),
            snapshot_lsn: Some(5),
            // Currently idempotency does not care about segments or pages
            page_count: 1,
            segments: vec![],
        };
        let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
        let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.lsn().expect("invalid LSN"), 6);

        // ensure that an out of sync client is rejected
        let commit = CommitRequest {
            vid: vid.copy_to_bytes(),
            cid: ClientId::random().copy_to_bytes(),
            snapshot_lsn: Some(5),
            page_count: 1,
            segments: vec![SegmentInfo::new(&SegmentId::random(), graft.clone())],
        };
        server
            .post("/")
            .expect_failure()
            .bytes(commit.encode_to_vec().into())
            .await;
    }
}
