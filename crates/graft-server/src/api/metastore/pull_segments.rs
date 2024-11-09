use std::sync::Arc;

use axum::extract::State;
use bytes::Bytes;
use graft_core::VolumeId;
use graft_proto::{
    common::v1::{LsnRange, SegmentInfo, Snapshot},
    metastore::v1::{PullSegmentsRequest, PullSegmentsResponse},
};
use itertools::Itertools;
use object_store::ObjectStore;
use tryiter::TryIteratorExt;

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
    let lsns = start_lsn..=snapshot.lsn();

    // ensure the catalog contains the requested LSNs
    state
        .updater
        .update_catalog_from_store_in_range(&state.store, &state.catalog, &vid, &lsns)
        .await?;

    let mut result = PullSegmentsResponse {
        snapshot: Some(Snapshot::new(
            &vid,
            snapshot.lsn(),
            snapshot.last_offset(),
            snapshot.system_time(),
        )),
        range: Some(LsnRange::from_bounds(&lsns)),
        segments: Vec::with_capacity(lsns.try_len().unwrap_or_default()),
    };

    let mut iter = state.catalog.query_segments(&vid, &lsns);
    while let Some((sid, splinter)) = iter.try_next()? {
        let offsets = Bytes::copy_from_slice(splinter.into_inner().as_ref());
        result
            .segments
            .push(SegmentInfo { sid: sid.into(), offsets });
    }

    Ok(ProtoResponse::new(result))
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
        },
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

        // case 1: catalog and store are empty
        let req = PullSegmentsRequest { vid: vid.clone().into(), range: None };
        let resp = server
            .post("/")
            .bytes(req.encode_to_vec().into())
            .expect_failure()
            .await;
        assert_eq!(resp.status_code(), StatusCode::NOT_FOUND);

        // case 2: catalog is empty, store has 10 commits
        let offsets = &[0u32]
            .into_iter()
            .collect::<Splinter>()
            .serialize_to_bytes();
        for lsn in 0u64..10 {
            let meta = CommitMeta::new(lsn, 0, 0, SystemTime::now());
            let mut commit = CommitBuilder::default();
            commit.write_offsets(SegmentId::random(), offsets);
            store.commit(vid.clone(), meta, commit).await.unwrap();
        }

        // request the last 5 segments
        let lsns = 5..10;
        let req = PullSegmentsRequest {
            vid: vid.clone().into(),
            range: Some(LsnRange::from_bounds(&lsns)),
        };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullSegmentsResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.lsn, 9);
        assert_eq!(snapshot.last_offset, 0);
        assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());
        assert_eq!(resp.range.map(|r| r.canonical()), Some(lsns));
        assert_eq!(resp.segments.len(), 5);
        for segment in resp.segments {
            assert_eq!(segment.offsets, offsets);
        }

        // request all the segments
        let req = PullSegmentsRequest { vid: vid.clone().into(), range: None };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        let resp = PullSegmentsResponse::decode(resp.into_bytes()).unwrap();
        assert_eq!(resp.segments.len(), 10);
    }
}
