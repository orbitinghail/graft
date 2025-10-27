use std::sync::Arc;

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, lsn::LSNRangeExt};
use graft_proto::{
    common::v1::{Commit, SegmentInfo},
    metastore::v1::{PullCommitsRequest, PullCommitsResponse},
};
use tryiter::TryIteratorExt;

use crate::api::{
    error::{ApiErr, ApiErrCtx},
    extractors::Protobuf,
    response::ProtoResponse,
};

use super::MetastoreApiState;

/// Returns a list of segments added in the lsn range. This method will also
/// return the latest Snapshot of the Volume. If no lsn range is specified, only
/// commits starting at the last checkpoint will be returned.
#[tracing::instrument(name = "metastore/v1/pull_commits", skip(state, req))]
pub async fn handler(
    State(state): State<Arc<MetastoreApiState>>,
    Protobuf(req): Protobuf<PullCommitsRequest>,
) -> Result<ProtoResponse<PullCommitsResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsns = req.range;
    let end_lsn = match lsns {
        Some(l) => l.end().or_into_ctx()?,
        None => None,
    };

    tracing::info!(?vid, ?lsns);

    // load the snapshot at the end of the lsn range
    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, end_lsn)
        .await
        .or_into_ctx()?;

    let Some(snapshot) = snapshot else {
        return Err(Culprit::new_with_note(
            ApiErrCtx::SnapshotMissing,
            format!("volume {vid} is missing snapshot at {end_lsn:?}"),
        )
        .into());
    };

    // resolve the start of the range, defaulting to the last checkpoint
    let checkpoint = snapshot.checkpoint();
    let start_lsn = match lsns {
        Some(l) => l.start().or_into_ctx()?,
        None => checkpoint,
    };

    // calculate the resolved lsn range
    let lsns = start_lsn..=snapshot.lsn();

    // ensure the catalog contains the requested LSNs
    state
        .updater
        .update_catalog_from_store_in_range(&state.store, &state.catalog, &vid, &lsns)
        .await
        .or_into_ctx()?;

    let mut result = PullCommitsResponse {
        commits: Vec::with_capacity(lsns.len() as usize),
    };

    let mut scan = state.catalog.scan_volume(&vid, &lsns);
    while let Some((meta, mut segments)) = scan.try_next().or_into_ctx()? {
        let mut segment_infos = Vec::default();
        while let Some((key, splinter)) = segments.try_next().or_into_ctx()? {
            segment_infos.push(SegmentInfo {
                sid: key.sid().copy_to_bytes(),
                graft: splinter.into_inner(),
            });
        }

        result.commits.push(Commit {
            snapshot: Some(meta.into_snapshot()),
            segments: segment_infos,
        });
    }

    Ok(ProtoResponse::new(result))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use axum::{handler::Handler, http::StatusCode};
    use axum_test::TestServer;
    use graft_core::{SegmentId, gid::ClientId, lsn::LSN, page_count::PageCount};
    use graft_proto::common::v1::LsnRange;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter_rs::{Encodable, Splinter};

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
    async fn test_pull_commits_sanity() {
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
        let req = PullCommitsRequest { vid: vid.copy_to_bytes(), range: None };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // case 2: catalog is empty, store has 10 commits
        let graft = Splinter::from_iter([0]).encode_to_bytes();
        for lsn in 1u64..11 {
            let meta = CommitMeta::new(
                vid.clone(),
                cid.clone(),
                LSN::new(lsn),
                LSN::FIRST,
                PageCount::new(1),
                SystemTime::now(),
            );
            let mut commit = CommitBuilder::new_with_capacity(meta, 1);
            commit.write_graft(SegmentId::random(), graft.clone());
            let commit = commit.build();
            store.commit(commit).await.unwrap();
        }

        // request the last 5 commits
        let lsns = LSN::new(5)..LSN::new(10);
        let req = PullCommitsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(lsns)),
        };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullCommitsResponse::decode(resp.into_bytes()).unwrap();
        assert_eq!(resp.commits.len(), 5);
        let last_commit = resp.commits.last().unwrap();
        let snapshot = last_commit.snapshot.as_ref().unwrap();
        assert_eq!(snapshot.lsn().unwrap(), 9);
        assert_eq!(snapshot.pages(), 1);
        assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());
        for segment in &last_commit.segments {
            assert_eq!(segment.graft, graft);
        }

        // request all the commits
        let req = PullCommitsRequest { vid: vid.copy_to_bytes(), range: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullCommitsResponse::decode(resp.into_bytes()).unwrap();
        assert_eq!(resp.commits.len(), 10);
    }
}
