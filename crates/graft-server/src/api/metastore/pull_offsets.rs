use std::sync::Arc;

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::VolumeId;
use graft_proto::{
    common::v1::LsnRange,
    metastore::v1::{PullOffsetsRequest, PullOffsetsResponse},
};
use splinter::{ops::Merge, Splinter};
use tryiter::TryIteratorExt;

use crate::api::{
    error::{ApiErr, ApiErrCtx},
    extractors::Protobuf,
    response::ProtoResponse,
};

use super::MetastoreApiState;

/// Returns a set of changed offsets in the lsn range. This method will also
/// return the latest Snapshot of the Volume. If no lsn range is specified, it
/// will return offsets changed between the last checkpoint and the latest
/// snapshot.
#[tracing::instrument(name = "metastore/v1/pull_offsets", skip(state, req))]
pub async fn handler(
    State(state): State<Arc<MetastoreApiState>>,
    Protobuf(req): Protobuf<PullOffsetsRequest>,
) -> Result<ProtoResponse<PullOffsetsResponse>, ApiErr> {
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

    // if the snapshot happens before the start_lsn, return a missing snapshot error
    if snapshot.lsn() < start_lsn {
        return Err(Culprit::new_with_note(
            ApiErrCtx::SnapshotMissing,
            format!(
                "volume {vid} snapshot {:?} happens before {start_lsn:?}",
                snapshot.lsn()
            ),
        )
        .into());
    }

    // calculate the resolved lsn range
    let lsns = start_lsn..=snapshot.lsn();

    tracing::trace!(?lsns, "resolved LSN range");

    // ensure the catalog contains the requested LSNs
    state
        .updater
        .update_catalog_from_store_in_range(&state.store, &state.catalog, &vid, &lsns)
        .await
        .or_into_ctx()?;

    // read the segments, and merge into a single splinter
    let mut iter = state.catalog.scan_segments(&vid, &lsns);
    let mut splinter = Splinter::default();
    while let Some((_, offsets)) = iter.try_next().or_into_ctx()? {
        splinter.merge(&offsets);
    }

    Ok(ProtoResponse::new(PullOffsetsResponse {
        snapshot: Some(snapshot.into_snapshot(&vid)),
        range: Some(LsnRange::from_range(lsns)),
        offsets: splinter.serialize_to_bytes(),
    }))
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use axum::{handler::Handler, http::StatusCode};
    use axum_test::TestServer;
    use graft_core::{gid::ClientId, lsn::LSN, page_count::PageCount, SegmentId};
    use object_store::memory::InMemory;
    use prost::Message;
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
    async fn test_pull_offsets_sanity() {
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
        let req = PullOffsetsRequest { vid: vid.copy_to_bytes(), range: None };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // case 2: catalog is empty, store has 10 commits
        let offsets = Splinter::from_iter([0u32]).serialize_to_bytes();
        for lsn in 1u64..=9 {
            let meta = CommitMeta::new(
                cid.clone(),
                LSN::new(lsn),
                LSN::FIRST,
                PageCount::new(1),
                SystemTime::now(),
            );
            let mut commit = CommitBuilder::default();
            commit.write_offsets(SegmentId::random(), &offsets);
            let commit = commit.build(vid.clone(), meta);
            store.commit(commit).await.unwrap();
        }

        // request the last 5 segments
        let lsns = LSN::new(5)..=LSN::new(9);
        let req = PullOffsetsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(lsns.clone())),
        };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullOffsetsResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.lsn().unwrap(), 9);
        assert_eq!(snapshot.pages(), 1);
        assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());
        assert_eq!(
            resp.range
                .map(|r| r.start().unwrap()..=r.end().unwrap().unwrap()),
            Some(lsns)
        );
        let splinter = Splinter::from_bytes(resp.offsets).unwrap();
        assert_eq!(splinter.cardinality(), 1);
        assert_eq!(splinter.iter().collect::<Vec<_>>(), vec![0]);

        // request all the segments
        let req = PullOffsetsRequest { vid: vid.copy_to_bytes(), range: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullOffsetsResponse::decode(resp.into_bytes()).unwrap();
        let splinter = Splinter::from_bytes(resp.offsets).unwrap();
        assert_eq!(splinter.cardinality(), 1);
        assert_eq!(splinter.iter().collect::<Vec<_>>(), vec![0]);
    }
}
