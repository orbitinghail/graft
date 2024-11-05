use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use futures::{stream::FuturesUnordered, FutureExt, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::pagestore::v1::{PageAtOffset, ReadPagesRequest, ReadPagesResponse};
use object_store::ObjectStore;
use splinter::{ops::Cut, Splinter};

use crate::segment::cache::Cache;
use crate::segment::closed::ClosedSegment;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::PagestoreApiState;

pub async fn handler<O: ObjectStore, C: Cache>(
    State(state): State<Arc<PagestoreApiState<O, C>>>,
    Protobuf(req): Protobuf<ReadPagesRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: LSN = req.lsn;
    let mut offsets = Splinter::from_bytes(req.offsets)?;
    let num_offsets = offsets.cardinality();

    let snapshot = state.catalog().latest_snapshot(&vid)?;
    let needs_update = snapshot.is_none() || snapshot.as_ref().is_some_and(|s| s.lsn() < lsn);

    if needs_update {
        // TODO: update the segment index
    }

    let mut loading = FuturesUnordered::new();

    // TODO: If we know the last_offset in the requested LSN, we can skip
    // returning any offsets that are greater than that.

    let segments = state.catalog().query_segments(vid.clone(), lsn);
    for result in segments {
        let (sid, splinter) = result?;

        let cut = offsets.cut(&splinter);
        if !cut.is_empty() {
            loading.push(
                state
                    .loader()
                    .load_segment(sid)
                    .map(|result| result.map(|segment| (segment, cut)))
                    .boxed(),
            );
        }

        if offsets.is_empty() {
            // all offsets have been found
            break;
        }
    }

    let mut result = ReadPagesResponse { pages: Vec::with_capacity(num_offsets) };
    while let Some((segment, cut)) = loading.try_next().await? {
        let segment = ClosedSegment::from_bytes(&segment)?;

        for offset in cut.iter() {
            let page = segment
                .find_page(vid.clone(), offset)
                .expect("failed to find expected offset in segment; index out of sync");
            result
                .pages
                .push(PageAtOffset { offset, data: page.into() });
        }
    }

    Ok(ProtoResponse::new(result))
}

#[cfg(test)]
mod tests {
    use axum::handler::Handler;
    use axum_test::TestServer;
    use bytes::Bytes;
    use graft_core::{gid::SegmentId, offset::Offset, page::Page};
    use graft_proto::common::v1::SegmentInfo;
    use object_store::{memory::InMemory, path::Path};
    use prost::Message;
    use tokio::sync::mpsc;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        segment::{
            bus::Bus, cache::mem::MemCache, loader::SegmentLoader, offsets_map::OffsetsMap,
            open::OpenSegment,
        },
        volume::{catalog::VolumeCatalog, commit::CommitMeta},
    };

    use super::*;

    fn mksegment(pages: Vec<(VolumeId, Offset, Page)>) -> (Bytes, OffsetsMap) {
        let mut segment = OpenSegment::default();
        for (vid, off, page) in pages {
            segment.insert(vid, off, page).unwrap();
        }
        segment.serialize()
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_read_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());
        let catalog = VolumeCatalog::open_temporary().unwrap();
        let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);

        let (page_tx, _) = mpsc::channel(128);
        let commit_bus = Bus::new(128);

        let state = Arc::new(PagestoreApiState::new(
            page_tx,
            commit_bus,
            catalog.clone(),
            loader,
        ));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            // .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        // setup test data
        let lsn: LSN = 2;
        let vid = VolumeId::random();

        // segment 1 is in the store
        let sid1 = SegmentId::random();
        let (segment, offsets1) = mksegment(vec![
            (vid.clone(), 0, Page::test_filled(0)),
            (vid.clone(), 1, Page::test_filled(1)),
            (vid.clone(), 2, Page::test_filled(2)),
        ]);
        store
            .put(&Path::from(sid1.pretty()), segment.into())
            .await
            .unwrap();

        // segment 2 is already in the cache
        let sid2 = SegmentId::random();
        let (segment, offsets2) = mksegment(vec![
            (vid.clone(), 3, Page::test_filled(3)),
            (vid.clone(), 4, Page::test_filled(4)),
        ]);
        cache.put(&sid2, segment).await.unwrap();

        // notify the catalog about the segments
        let mut batch = catalog.batch_insert();
        batch
            .insert_snapshot(
                vid.clone(),
                CommitMeta::new(lsn, 0, 4, 0),
                vec![
                    SegmentInfo {
                        sid: sid1.into(),
                        offsets: offsets1.get(&vid).unwrap().clone().into_inner(),
                    },
                    SegmentInfo {
                        sid: sid2.into(),
                        offsets: offsets2.get(&vid).unwrap().clone().into_inner(),
                    },
                ],
            )
            .unwrap();
        batch.commit().unwrap();

        // we are finally able to test read_pages :)
        let req = ReadPagesRequest {
            vid: vid.into(),
            lsn,
            offsets: (0u32..=4).collect::<Splinter>().serialize_to_bytes(),
        };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        if resp.status_code() != 200 {
            let data = resp.as_bytes();
            println!("response: {:?}", data);
            panic!("unexpected response status: {}", resp.status_code());
        }
        let mut resp = ReadPagesResponse::decode(resp.into_bytes()).unwrap();

        // we expect to see all 5 pages here
        assert_eq!(resp.pages.len(), 5);
        // sort by offset to make the test deterministic
        resp.pages.sort_by_key(|p| p.offset);
        for (PageAtOffset { offset, data }, expected) in resp.pages.into_iter().zip(0..) {
            assert_eq!(offset, expected);
            assert_eq!(
                data,
                Bytes::from(Page::test_filled(expected as u8)),
                "page data mismatch for offset: {offset}",
            );
        }
    }
}
