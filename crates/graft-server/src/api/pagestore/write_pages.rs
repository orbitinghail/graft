use std::{sync::Arc, time::Duration, vec};

use axum::{extract::State, response::IntoResponse};
use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, page::Page};
use graft_proto::{
    common::v1::SegmentInfo,
    pagestore::v1::{WritePagesRequest, WritePagesResponse},
};
use hashbrown::HashSet;
use tokio::{
    sync::broadcast::error::RecvError,
    time::{Instant, timeout_at},
};

use crate::{
    api::{error::ApiErrCtx, response::ProtoResponse},
    segment::writer::WritePageMsg,
};

use crate::api::{error::ApiErr, extractors::Protobuf};

use super::PagestoreApiState;

const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[tracing::instrument(name = "pagestore/v1/write_pages", skip(state, req))]
pub async fn handler<C>(
    State(state): State<Arc<PagestoreApiState<C>>>,
    Protobuf(req): Protobuf<WritePagesRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let expected_pages = req.pages.len();

    // acquire a permit to write to the volume.
    // This permit is critical as it ensures that no other write_pages handler
    // can concurrently write pages into this volume.
    let _permit = state.volume_write_limiter().acquire(&vid).await;

    // subscribe to the broadcast channel
    let mut segment_rx = state.subscribe_to_uploaded_segments();

    tracing::info!(?vid, expected_pages);

    let mut seen = HashSet::with_capacity(req.pages.len());
    for page in req.pages {
        let pageidx = page.pageidx().or_into_ctx()?;
        let page: Page = Page::try_from(page.data).or_into_ctx()?;

        if !seen.insert(pageidx) {
            return Err(Culprit::new_with_note(
                ApiErrCtx::DuplicatePageIdx,
                format!("page index: {pageidx}"),
            )
            .into());
        }

        state
            .write_page(WritePageMsg::new(vid.clone(), pageidx, page))
            .await;
    }

    let mut segments: Vec<SegmentInfo> = vec![];

    // fail if we don't receive all segments by this deadline
    let deadline = Instant::now() + WRITE_TIMEOUT;

    let mut count = 0;
    while count < expected_pages {
        let segment = match timeout_at(deadline, segment_rx.recv()).await {
            Ok(result) => match result {
                Ok(segment) => segment,
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!("segment channel lagged by {n} messages");
                    continue;
                }
                Err(RecvError::Closed) => panic!("segment channel unexpectedly closed"),
            },
            Err(_) => {
                return Err(Culprit::new_with_note(
                    ApiErrCtx::Timeout,
                    "timeout while waiting for segments to upload",
                )
                .into());
            }
        };

        // check to see if this segment contains any offsets for our vid
        if let Some(graft) = segment.graft(&vid) {
            let sid = segment.sid().or_into_ctx()?;
            tracing::trace!("write_pages handler received segment {sid} for volume {vid}",);

            count += graft.cardinality();

            // store the segment
            segments.push(SegmentInfo {
                sid: sid.copy_to_bytes(),
                graft: graft.inner().clone(),
            });
        }
    }

    assert_eq!(
        count, expected_pages,
        "expected {expected_pages} pages, but got {count}"
    );

    Ok(ProtoResponse::new(WritePagesResponse { segments }))
}

#[cfg(test)]
mod tests {
    use std::{future::IntoFuture, time::Duration};

    use axum::handler::Handler;
    use axum_test::TestServer;
    use bytes::Bytes;
    use graft_client::{MetastoreClient, NetClient};
    use graft_proto::pagestore::v1::PageAtIdx;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::SplinterRef;
    use tokio::sync::mpsc;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        segment::{
            bus::Bus, cache::mem::MemCache, loader::SegmentLoader, uploader::SegmentUploaderTask,
            writer::SegmentWriterTask,
        },
        supervisor::SupervisedTask,
        volume::{catalog::VolumeCatalog, updater::VolumeCatalogUpdater},
    };

    use super::*;

    #[graft_test::test]
    async fn test_write_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());
        let catalog = VolumeCatalog::open_temporary().unwrap();
        let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);

        let (page_tx, page_rx) = mpsc::channel(128);
        let (store_tx, store_rx) = mpsc::channel(8);
        let commit_bus = Bus::new(128);

        SegmentWriterTask::new(
            Default::default(),
            page_rx,
            store_tx,
            Duration::from_secs(1),
        )
        .testonly_spawn();

        SegmentUploaderTask::new(
            Default::default(),
            store_rx,
            commit_bus.clone(),
            store.clone(),
            cache.clone(),
        )
        .testonly_spawn();

        let client = NetClient::new();
        let metastore_uri = "http://localhost:3000".parse().unwrap();

        let state = Arc::new(PagestoreApiState::new(
            page_tx,
            commit_bus,
            catalog,
            loader,
            MetastoreClient::new(metastore_uri, client),
            VolumeCatalogUpdater::new(10),
            10,
        ));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        // issue two concurrent writes to different volumes
        let page: Bytes = Page::test_filled(1).into();

        let req1 = WritePagesRequest {
            vid: VolumeId::random().copy_to_bytes(),
            pages: vec![PageAtIdx { pageidx: 1, data: page.clone() }],
        };

        let req2 = WritePagesRequest {
            vid: VolumeId::random().copy_to_bytes(),
            pages: vec![
                PageAtIdx { pageidx: 1, data: page.clone() },
                PageAtIdx { pageidx: 2, data: page.clone() },
            ],
        };

        let local = tokio::task::LocalSet::new();
        let req1 = local.spawn_local(
            server
                .post("/")
                .bytes(req1.encode_to_vec().into())
                .into_future(),
        );
        let req2 = local.spawn_local(
            server
                .post("/")
                .bytes(req2.encode_to_vec().into())
                .into_future(),
        );

        // wait for both requests to complete
        local.await;
        let (resp1, resp2) = (req1.await.unwrap(), req2.await.unwrap());

        let resp1 = WritePagesResponse::decode(resp1.into_bytes()).unwrap();
        assert_eq!(resp1.segments.len(), 1, "expected 1 segment");
        let graft = SplinterRef::from_bytes(resp1.segments[0].graft.clone()).unwrap();
        assert_eq!(graft.cardinality(), 1);
        assert!(graft.contains(1));

        let resp2 = WritePagesResponse::decode(resp2.into_bytes()).unwrap();
        assert_eq!(resp2.segments.len(), 1, "expected 1 segment");
        let graft = SplinterRef::from_bytes(resp2.segments[0].graft.clone()).unwrap();
        assert_eq!(graft.cardinality(), 2);
        assert!(graft.contains(1));
        assert!(graft.contains(2));
    }
}
