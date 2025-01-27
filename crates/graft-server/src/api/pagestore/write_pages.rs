use std::{sync::Arc, vec};

use axum::{extract::State, response::IntoResponse};
use culprit::{Culprit, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_proto::{
    common::v1::SegmentInfo,
    pagestore::v1::{WritePagesRequest, WritePagesResponse},
};
use hashbrown::HashSet;
use tokio::sync::broadcast::error::RecvError;

use crate::{
    api::{error::ApiErrCtx, response::ProtoResponse},
    segment::bus::WritePageReq,
};

use crate::api::{error::ApiErr, extractors::Protobuf};

use super::PagestoreApiState;

#[tracing::instrument(name = "pagestore/v1/write_pages", skip(state, req))]
pub async fn handler<C>(
    State(state): State<Arc<PagestoreApiState<C>>>,
    Protobuf(req): Protobuf<WritePagesRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let expected_pages = req.pages.len();

    // acquire a permit to write to the volume
    let _permit = state.volume_write_limiter().acquire(&vid).await;

    // subscribe to the broadcast channel
    let mut commit_rx = state.subscribe_commits();

    tracing::info!(?vid, expected_pages);

    let mut seen = HashSet::with_capacity(req.pages.len());
    for page in req.pages {
        let offset: PageOffset = page.offset.into();
        let page: Page = Page::try_from(page.data).or_into_ctx()?;

        if !seen.insert(offset) {
            return Err(Culprit::new_with_note(
                ApiErrCtx::DuplicatePageOffset,
                format!("duplicate page offset: {offset}"),
            )
            .into());
        }

        state
            .write_page(WritePageReq::new(vid.clone(), offset, page))
            .await;
    }

    let mut segments: Vec<SegmentInfo> = vec![];

    let mut count = 0;
    while count < expected_pages {
        let commit = match commit_rx.recv().await {
            Ok(commit) => commit,
            Err(RecvError::Lagged(n)) => panic!("commit channel lagged by {}", n),
            Err(RecvError::Closed) => panic!("commit channel unexpectedly closed"),
        };

        if let Some(offsets) = commit.offsets.get(&vid) {
            tracing::debug!("write_pages handler received commit: {commit:?} for volume {vid}");

            count += offsets.cardinality();

            // store the segment
            segments.push(SegmentInfo {
                sid: commit.sid.copy_to_bytes(),
                offsets: offsets.inner().clone(),
            });
        }
    }

    assert_eq!(
        count, expected_pages,
        "expected {} pages, but got {}",
        expected_pages, count
    );

    Ok(ProtoResponse::new(WritePagesResponse { segments }))
}

#[cfg(test)]
mod tests {
    use std::{future::IntoFuture, time::Duration};

    use axum::handler::Handler;
    use axum_test::TestServer;
    use bytes::Bytes;
    use graft_client::ClientBuilder;
    use graft_proto::pagestore::v1::PageAtOffset;
    use object_store::memory::InMemory;
    use prost::Message;
    use splinter::SplinterRef;
    use tokio::{sync::mpsc, time::sleep};
    use tracing_test::traced_test;

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

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_write_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());
        let catalog = VolumeCatalog::open_temporary().unwrap();
        let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);

        let (page_tx, page_rx) = mpsc::channel(128);
        let (store_tx, store_rx) = mpsc::channel(8);
        let commit_bus = Bus::new(128);

        let writer_controller = SegmentWriterTask::new(
            Default::default(),
            page_rx,
            store_tx,
            Duration::from_secs(100),
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

        let state = Arc::new(PagestoreApiState::new(
            page_tx,
            commit_bus,
            catalog,
            loader,
            ClientBuilder::new("http://localhost:3000".try_into().unwrap())
                .build()
                .unwrap(),
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
            pages: vec![PageAtOffset { offset: 0, data: page.clone() }],
        };

        let req2 = WritePagesRequest {
            vid: VolumeId::random().copy_to_bytes(),
            pages: vec![
                PageAtOffset { offset: 0, data: page.clone() },
                PageAtOffset { offset: 1, data: page.clone() },
            ],
        };

        // we pause the writer here to ensure that both requests are placed in
        // the same segment
        writer_controller.pause();

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

        // this sleep allows tokio to advance past the writer timer as well as
        // advance each request until they are waiting for the next commit
        local.run_until(sleep(Duration::from_secs(5))).await;

        // resume the writer, causing the segment to flush
        writer_controller.resume();

        // wait for both requests to complete
        local.await;
        let (resp1, resp2) = (req1.await.unwrap(), req2.await.unwrap());

        let resp1 = WritePagesResponse::decode(resp1.into_bytes()).unwrap();
        assert_eq!(resp1.segments.len(), 1, "expected 1 segment");
        let offsets = SplinterRef::from_bytes(resp1.segments[0].offsets.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 1);
        assert!(offsets.contains(0));

        let resp2 = WritePagesResponse::decode(resp2.into_bytes()).unwrap();
        assert_eq!(resp2.segments.len(), 1, "expected 1 segment");
        assert_eq!(
            resp2.segments[0].sid().unwrap(),
            resp1.segments[0].sid().unwrap(),
            "expected same segment"
        );
        let offsets = SplinterRef::from_bytes(resp2.segments[0].offsets.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 2);
        assert!(offsets.contains(0));
        assert!(offsets.contains(1));
    }
}
