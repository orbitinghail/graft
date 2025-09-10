use std::{sync::Arc, time::SystemTime};

use axum::extract::State;
use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, gid::ClientId, lsn::LSN, page_count::PageCount};
use graft_proto::metastore::v1::{CommitRequest, CommitResponse};
use splinter_rs::{Cut, PartitionRead, Splinter};

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

            // if we have a commit snapshot and the cid matches, return a successful response
            if let Some(commit_snapshot) = commit_snapshot
                && commit_snapshot.cid() == &cid
            {
                if commit_snapshot.page_count() != page_count {
                    return Err(Culprit::new_with_note(
                        ApiErrCtx::InvalidIdempotentCommit,
                        "page count mismatch",
                    )
                    .into());
                }

                // check that the segments being committed contain the same
                // set of pages as the segments in the catalog
                let mut committed_pages = state
                    .catalog()
                    .scan_segments(&vid, &(commit_lsn..=commit_lsn))
                    .try_fold(Splinter::default(), |mut acc, kv| {
                        kv.map(|(_, g)| {
                            acc |= g;
                            acc
                        })
                    })
                    .or_into_ctx()?;

                for segment in req.segments {
                    let graft = segment.graft().or_into_ctx()?;
                    let cut = committed_pages.cut(&graft);
                    if cut != graft {
                        return Err(Culprit::new_with_note(
                            ApiErrCtx::InvalidIdempotentCommit,
                            "extra page idxs",
                        )
                        .into());
                    }
                }

                if !committed_pages.is_empty() {
                    return Err(Culprit::new_with_note(
                        ApiErrCtx::InvalidIdempotentCommit,
                        "missing page idxs",
                    )
                    .into());
                }

                precept::expect_reachable!(
                    "detected idempotent commit request and reused previous response",
                    {
                        "vid": vid,
                        "cid": cid,
                        "commit_lsn": commit_lsn,
                    }
                );
                tracing::debug!(
                    "detected idempotent commit request for volume {vid}: reusing commit at lsn {commit_lsn:?}"
                );

                return Ok(ProtoResponse::new(CommitResponse {
                    snapshot: Some(commit_snapshot.into_snapshot()),
                }));
            }
        }
        // otherwise reject this commit
        let note = format!(
            "commit rejected for volume {vid}: client snapshot lsn {snapshot_lsn:?} is out of sync with latest lsn {latest_lsn:?}"
        );
        tracing::debug!(%note);
        return Err(Culprit::new_with_note(ApiErrCtx::RejectedCommit, note).into());
    }

    // checkpoint doesn't change
    let checkpoint = latest_snapshot.map_or(LSN::FIRST, |s| s.checkpoint());

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
        if graft.contains(0) {
            return Err(ApiErrCtx::ZeroPageIdx.into());
        }
        commit.write_graft(sid.clone(), graft.into_inner());
    }

    let commit = commit.build();

    // commit the new snapshot to the store
    state.store.commit(commit.clone()).await.or_into_ctx()?;

    // update the catalog
    let mut batch = state.catalog.batch_insert();
    batch.insert_commit(&commit).or_into_ctx()?;
    batch.commit().or_into_ctx()?;

    tracing::info!(
        "successful commit to volume {vid}: new snapshot lsn {}",
        commit.meta().lsn()
    );

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
    use splinter_rs::Encodable;

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
        let graft = Splinter::from_iter([1]).encode_to_bytes();

        // store commit requests to test idempotency later
        let mut commits = vec![];

        // let's commit and validate the store 10 times
        for i in 1..10 {
            println!("iteration: {i}");

            let snapshot_lsn = (i != 1).then(|| i - 1);
            let lsn = LSN::new(i);

            let commit = CommitRequest {
                vid: vid.copy_to_bytes(),
                cid: cid.copy_to_bytes(),
                snapshot_lsn,
                page_count: 1,
                segments: vec![SegmentInfo::new(&SegmentId::random(), graft.clone())],
            };
            commits.push(commit.clone());

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
            let commit = store.get_commit(&vid, lsn).await.unwrap().unwrap();
            assert_eq!(commit.meta().lsn(), lsn);

            let snapshot = catalog.latest_snapshot(&vid).unwrap().unwrap();
            assert_eq!(snapshot.lsn(), lsn);
        }

        // ensure that an older commit is still idempotent
        let mut commit = commits[5].clone();
        // the segment id can be different
        commit.segments[0].sid = SegmentId::random().copy_to_bytes();
        let resp = server.post("/").bytes(commit.encode_to_vec().into()).await;
        let resp = CommitResponse::decode(resp.into_bytes()).unwrap();
        let snapshot = resp.snapshot.unwrap();
        assert_eq!(snapshot.lsn().unwrap(), 6);

        let bad_commits = [
            ("different page count", {
                let mut c = commits[5].clone();
                c.page_count = 2;
                c
            }),
            ("missing segments", {
                let mut c = commits[5].clone();
                c.segments.clear();
                c
            }),
            ("extra offset", {
                let mut c = commits[5].clone();
                c.segments[0].graft = Splinter::from_iter([1u32, 2]).encode_to_bytes();
                c
            }),
            ("missing offset", {
                let mut c = commits[5].clone();
                c.segments[0].graft = Splinter::from_iter([2u32]).encode_to_bytes();
                c
            }),
            ("extra segment", {
                let mut c = commits[5].clone();
                c.segments
                    .push(SegmentInfo::new(&SegmentId::random(), graft.clone()));
                c
            }),
        ];

        for (name, bc) in bad_commits {
            println!("testing bad commit: {name}");

            server
                .post("/")
                .expect_failure()
                .bytes(bc.encode_to_vec().into())
                .await;
        }

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
