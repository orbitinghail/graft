use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use culprit::ResultExt;
use futures::{stream::FuturesUnordered, FutureExt, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::pagestore::v1::{PageAtOffset, ReadPagesRequest, ReadPagesResponse};
use splinter::{ops::Cut, Splinter};

use crate::api::error::ApiErrCtx;
use crate::segment::cache::Cache;
use crate::segment::closed::ClosedSegment;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::PagestoreApiState;

/// The maximum amount of pages that can be returned in a single request.
/// This results in the maximum response size being roughly 4MB
pub const MAX_OFFSETS: usize = 1024;

#[tracing::instrument(name = "pagestore/v1/read_pages", skip(state, req))]
pub async fn handler<C: Cache>(
    State(state): State<Arc<PagestoreApiState<C>>>,
    Protobuf(req): Protobuf<ReadPagesRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn: LSN = req.lsn.into();
    let mut offsets = Splinter::from_bytes(req.offsets).or_into_ctx()?;
    let num_offsets = offsets.cardinality();

    tracing::info!(?vid, ?lsn, num_offsets);

    if num_offsets > MAX_OFFSETS {
        return Err(ApiErrCtx::TooManyOffsets.into());
    }

    // ensure we've replayed the catalog up to the requested LSN
    state
        .updater()
        .update_catalog_from_client(state.metastore_client(), state.catalog(), &vid, Some(lsn))
        .await
        .or_into_ctx()?;

    // load the snapshot, this should never be missing since we just updated
    let snapshot = state
        .catalog()
        .snapshot(vid.clone(), lsn)
        .or_into_ctx()?
        .expect("missing snapshot after update");
    let checkpoint = snapshot.checkpoint();

    let mut loading = FuturesUnordered::new();

    let segments = state.catalog().scan_segments(&vid, &(checkpoint..=lsn));
    for result in segments {
        let (key, splinter) = result.or_into_ctx()?;

        let cut = offsets.cut(&splinter);
        if !cut.is_empty() {
            let sid = key.sid().clone();
            loading.push(
                state
                    .loader()
                    .load_segment(sid)
                    .map(move |result| result.map(|segment| (segment, cut)))
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
        let segment = ClosedSegment::from_bytes(&segment).or_into_ctx()?;

        for offset in cut.iter() {
            let page = segment
                .find_page(vid.clone(), offset.into())
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
    use std::time::SystemTime;

    use axum::handler::Handler;
    use axum_test::TestServer;
    use bytes::Bytes;
    use graft_client::ClientBuilder;
    use graft_core::{gid::SegmentId, page::Page, page_count::PageCount, page_offset::PageOffset};
    use graft_proto::common::v1::SegmentInfo;
    use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};
    use prost::Message;
    use tokio::sync::mpsc;
    use tracing_test::traced_test;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        bytes_vec::BytesVec,
        segment::{
            bus::Bus, cache::mem::MemCache, loader::SegmentLoader, offsets_map::OffsetsMap,
            open::OpenSegment,
        },
        volume::{catalog::VolumeCatalog, commit::CommitMeta, updater::VolumeCatalogUpdater},
    };

    use super::*;

    fn mksegment(
        sid: &SegmentId,
        pages: Vec<(VolumeId, PageOffset, Page)>,
    ) -> (BytesVec, OffsetsMap) {
        let mut segment = OpenSegment::default();
        for (vid, off, page) in pages {
            segment.insert(vid, off, page).unwrap();
        }
        segment.serialize(sid.clone())
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
            ClientBuilder::new("http://localhost:3000".try_into().unwrap())
                .build()
                .unwrap(),
            VolumeCatalogUpdater::new(10),
            10,
        ));

        let server = TestServer::builder()
            .default_content_type(CONTENT_TYPE_PROTOBUF.to_str().unwrap())
            // .expect_success_by_default()
            .build(handler.with_state(state).into_make_service())
            .unwrap();

        // setup test data
        let lsn: LSN = 2.into();
        let vid = VolumeId::random();

        // segment 1 is in the store
        let sid1 = SegmentId::random();
        let (segment, offsets1) = mksegment(
            &sid1,
            vec![
                (vid.clone(), PageOffset::new(0), Page::test_filled(0)),
                (vid.clone(), PageOffset::new(1), Page::test_filled(1)),
                (vid.clone(), PageOffset::new(2), Page::test_filled(2)),
            ],
        );
        store
            .put(
                &Path::from(sid1.pretty()),
                PutPayload::from_iter(segment.iter().cloned()),
            )
            .await
            .unwrap();

        // segment 2 is already in the cache
        let sid2 = SegmentId::random();
        let (segment, offsets2) = mksegment(
            &sid2,
            vec![
                (vid.clone(), PageOffset::new(3), Page::test_filled(3)),
                (vid.clone(), PageOffset::new(4), Page::test_filled(4)),
            ],
        );
        cache.put(&sid2, segment).await.unwrap();

        // notify the catalog about the segments
        let mut batch = catalog.batch_insert();
        batch
            .insert_snapshot(
                vid.clone(),
                CommitMeta::new(lsn, LSN::ZERO, PageCount::new(4), SystemTime::now()),
                vec![
                    SegmentInfo {
                        sid: sid1.copy_to_bytes(),
                        offsets: offsets1.get(&vid).unwrap().clone().into_inner(),
                    },
                    SegmentInfo {
                        sid: sid2.copy_to_bytes(),
                        offsets: offsets2.get(&vid).unwrap().clone().into_inner(),
                    },
                ],
            )
            .unwrap();
        batch.commit().unwrap();

        // we are finally able to test read_pages :)
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
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
