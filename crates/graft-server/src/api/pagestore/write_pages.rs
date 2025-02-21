use std::{sync::Arc, vec};

use axum::{extract::State, response::IntoResponse};
use culprit::{Culprit, ResultExt};
use graft_core::{page::Page, VolumeId};
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

    // acquire a permit to write to the volume.
    // This permit is critical as it ensures that no other write_pages handler
    // can concurrently write pages into this volume.
    let _permit = state.volume_write_limiter().acquire(&vid).await;

    // subscribe to the broadcast channel
    let mut commit_rx = state.subscribe_commits();

    tracing::info!(?vid, expected_pages);

    let mut seen = HashSet::with_capacity(req.pages.len());
    for page in req.pages {
        let offset = page.pageidx().or_into_ctx()?;
        let page: Page = Page::try_from(page.data).or_into_ctx()?;

        if !seen.insert(offset) {
            return Err(Culprit::new_with_note(
                ApiErrCtx::DuplicatePageIdx,
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
                graft: offsets.inner().clone(),
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
            pages: vec![PageAtIdx { idx: 1, data: page.clone() }],
        };

        let req2 = WritePagesRequest {
            vid: VolumeId::random().copy_to_bytes(),
            pages: vec![
                PageAtIdx { idx: 1, data: page.clone() },
                PageAtIdx { idx: 2, data: page.clone() },
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
        let offsets = SplinterRef::from_bytes(resp1.segments[0].graft.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 1);
        assert!(offsets.contains(1));

        let resp2 = WritePagesResponse::decode(resp2.into_bytes()).unwrap();
        assert_eq!(resp2.segments.len(), 1, "expected 1 segment");
        let offsets = SplinterRef::from_bytes(resp2.segments[0].graft.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 2);
        assert!(offsets.contains(1));
        assert!(offsets.contains(2));
    }
}
