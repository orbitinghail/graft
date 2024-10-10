use std::{sync::Arc, vec};

use axum::{extract::State, response::IntoResponse};
use bytes::{Bytes, BytesMut};
use graft_core::{guid::VolumeId, offset::Offset, page::Page};
use graft_proto::{
    common::v1::SegmentInfo,
    pagestore::v1::{WritePagesRequest, WritePagesResponse},
};
use hashbrown::HashSet;
use prost::Message;
use tokio::sync::broadcast::error::RecvError;

use crate::segment::bus::WritePageReq;

use super::{error::ApiError, extractors::Protobuf, state::ApiState};

pub async fn handler(
    State(state): State<Arc<ApiState>>,
    Protobuf(req): Protobuf<WritePagesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let vid: VolumeId = req.vid.try_into()?;

    // subscribe to the broadcast channel
    let mut commit_rx = state.subscribe_commits();

    let expected_pages = req.pages.len();
    let mut seen = HashSet::with_capacity(req.pages.len());
    for page in req.pages {
        let offset: Offset = page.offset;
        let page: Page = page.data.try_into()?;

        if seen.contains(&offset) {
            return Err(ApiError::DuplicatePageOffset(offset));
        }
        seen.insert(offset);

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
                sid: Bytes::copy_from_slice(commit.sid.as_ref()),
                offsets: offsets.inner().clone(),
            });
        }
    }

    assert_eq!(
        count, expected_pages,
        "expected {} pages, but got {}",
        expected_pages, count
    );

    let response = WritePagesResponse { segments };
    let mut buf = BytesMut::with_capacity(response.encoded_len());
    response
        .encode(&mut buf)
        .expect("insufficient buffer capacity");

    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::handler::Handler;
    use axum_test::TestServer;
    use graft_proto::pagestore::v1::PageAtOffset;
    use object_store::memory::InMemory;
    use splinter::Splinter;
    use tokio::sync::mpsc;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        segment::{bus::Bus, uploader::SegmentUploaderTask, writer::SegmentWriterTask},
        storage::mem::MemCache,
        supervisor::SupervisedTask,
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_write_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let (page_tx, page_rx) = mpsc::channel(128);
        let (store_tx, store_rx) = mpsc::channel(8);
        let commit_bus = Bus::new(128);

        SegmentWriterTask::new(page_rx, store_tx, Duration::from_secs(1)).testonly_spawn();

        SegmentUploaderTask::new(store_rx, commit_bus.clone(), store.clone(), cache.clone())
            .testonly_spawn();

        let state = Arc::new(ApiState::new(page_tx, commit_bus));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        // issue two concurrent writes to different volumes
        let page: Bytes = Page::test_filled(1).into();

        let req1 = WritePagesRequest {
            vid: Bytes::copy_from_slice(VolumeId::random().as_ref()),
            pages: vec![PageAtOffset { offset: 0, data: page.clone() }],
        };
        let req1 = server.post("/").bytes(req1.encode_to_vec().into());

        let req2 = WritePagesRequest {
            vid: Bytes::copy_from_slice(VolumeId::random().as_ref()),
            pages: vec![
                PageAtOffset { offset: 0, data: page.clone() },
                PageAtOffset { offset: 1, data: page.clone() },
            ],
        };
        let req2 = server.post("/").bytes(req2.encode_to_vec().into());

        // wait for both requests to complete
        let (resp1, resp2) = tokio::join!(req1, req2);

        let resp1 = WritePagesResponse::decode(resp1.into_bytes()).unwrap();
        assert_eq!(resp1.segments.len(), 1, "expected 1 segment");
        let offsets = Splinter::from_bytes(resp1.segments[0].offsets.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 1);
        assert!(offsets.contains(0));

        let resp2 = WritePagesResponse::decode(resp2.into_bytes()).unwrap();
        assert_eq!(resp2.segments.len(), 1, "expected 1 segment");
        assert_eq!(
            resp2.segments[0].sid, resp1.segments[0].sid,
            "expected same segment"
        );
        let offsets = Splinter::from_bytes(resp2.segments[0].offsets.clone()).unwrap();
        assert_eq!(offsets.cardinality(), 2);
        assert!(offsets.contains(0));
        assert!(offsets.contains(1));
    }
}
