use std::{sync::Arc, time::SystemTime};

use axum::extract::State;
use graft_core::{lsn::LSN, offset::Offset, SegmentId, VolumeId};
use graft_proto::{
    common::v1::Snapshot,
    metastore::v1::{CommitRequest, CommitResponse},
};
use itertools::Itertools;
use object_store::ObjectStore;
use splinter::{ops::Merge, Splinter, SplinterRef};

use crate::{
    api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse},
    volume::commit::{CommitBuilder, CommitMeta},
};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<CommitRequest>,
) -> Result<ProtoResponse<CommitResponse>, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let snapshot_lsn: Option<LSN> = req.snapshot_lsn;
    let last_offset: Offset = req.last_offset;

    // load the Volume's latest snapshot
    let snapshot = state
        .updater
        .snapshot(&state.store, &state.catalog, &vid, None)
        .await?;

    let commit_lsn = match (snapshot_lsn, snapshot.as_ref().map(|s| s.lsn())) {
        (None, None) => 0,
        (Some(snapshot), Some(latest)) if snapshot == latest => latest + 1,

        // in every other case, the commit is out of sync
        // TODO: implement page based MVCC
        (snapshot, latest) => {
            return Err(ApiErr::CommitSnapshotOutOfDate { vid, snapshot, latest })
        }
    };

    let mut commit = CommitBuilder::default();
    let mut all_offsets = Splinter::default();
    for segment in req.segments {
        let sid: SegmentId = segment.sid.try_into()?;
        let offsets = SplinterRef::from_bytes(segment.offsets)?;
        all_offsets.merge(&offsets);
        commit.write_offsets(sid, offsets.inner());
    }

    // this commit is a checkpoint if the splinter is contiguous, and it's last offset == last_offset
    let is_contiguous = all_offsets.iter().tuple_windows().all(|(a, b)| a + 1 == b);
    let has_last_offset = all_offsets.last() == Some(last_offset);
    let checkpoint = if is_contiguous && has_last_offset {
        commit_lsn
    } else {
        snapshot.map(|s| s.checkpoint()).unwrap_or_default()
    };

    let meta = CommitMeta::new(commit_lsn, checkpoint, last_offset, SystemTime::now());
    let commit = commit.build(vid.clone(), meta.clone());

    // commit the new snapshot to the store
    state.store.commit(commit.clone()).await?;

    // update the catalog
    let mut batch = state.catalog.batch_insert();
    batch.insert_commit(commit)?;
    batch.commit()?;

    // the commit was successful, return the new snapshot
    Ok(ProtoResponse::new(CommitResponse {
        snapshot: Some(Snapshot::new(
            &vid,
            meta.lsn(),
            meta.last_offset(),
            meta.system_time(),
        )),
    }))
}

#[cfg(test)]
mod tests {
    use axum::handler::Handler;
    use axum_test::TestServer;
    use graft_proto::common::v1::SegmentInfo;
    use object_store::memory::InMemory;
    use prost::Message;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        volume::{catalog::VolumeCatalog, commit, store::VolumeStore},
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
        let offsets = Splinter::from_iter([0u32]).serialize_to_bytes();

        // let's commit and validate the store 10 times
        for i in 0..10 {
            let commit = CommitRequest {
                vid: vid.clone().into(),
                snapshot_lsn: (i != 0).then(|| i - 1),
                last_offset: 0,
                segments: vec![SegmentInfo::new(&SegmentId::random(), offsets.clone())],
            };

            // run the commit against the api
            let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
            let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
            let snapshot = resp.snapshot.unwrap();
            assert_eq!(snapshot.vid().unwrap(), &vid);
            assert_eq!(snapshot.lsn(), i);
            assert_eq!(snapshot.last_offset(), 0);
            assert!(snapshot.system_time().unwrap().unwrap() < SystemTime::now());

            // check the commit in the store and the catalog
            let commit = store.get_commit(vid.clone(), i).await.unwrap();
            assert_eq!(commit.meta().lsn(), i);

            let snapshot = catalog.latest_snapshot(&vid).unwrap().unwrap();
            assert_eq!(snapshot.lsn(), i);
        }
    }
}
