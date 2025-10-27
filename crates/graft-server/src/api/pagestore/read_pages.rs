use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use culprit::ResultExt;
use futures::{FutureExt, TryStreamExt, stream::FuturesUnordered};
use graft_core::{VolumeId, lsn::LSN};
use graft_proto::pagestore::v1::{PageAtIdx, ReadPagesRequest, ReadPagesResponse};
use splinter_rs::{CowSplinter, Cut, PartitionRead};

use crate::api::error::ApiErrCtx;
use crate::segment::cache::Cache;
use crate::segment::closed::ClosedSegment;

use crate::api::{error::ApiErr, extractors::Protobuf, response::ProtoResponse};

use super::PagestoreApiState;

/// The maximum amount of pages that can be returned in a single request.
/// This results in the maximum response size being roughly 4MB
pub const MAX_PAGES: usize = 1024;

#[tracing::instrument(name = "pagestore/v1/read_pages", skip(state, req))]
pub async fn handler<C: Cache>(
    State(state): State<Arc<PagestoreApiState<C>>>,
    Protobuf(req): Protobuf<ReadPagesRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    let vid: VolumeId = req.vid.try_into()?;
    let lsn = LSN::try_from(req.lsn).or_into_ctx()?;
    let mut graft = CowSplinter::from_bytes(req.graft).or_into_ctx()?;
    let num_pages = graft.cardinality();

    tracing::info!(?vid, ?lsn, num_pages);

    if num_pages > MAX_PAGES {
        return Err(ApiErrCtx::GraftTooLarge.into());
    }
    if graft.contains(0) {
        return Err(ApiErrCtx::ZeroPageIdx.into());
    }

    // ensure we've replayed the catalog up to the requested LSN
    state
        .updater()
        .update_catalog_from_metastore(state.metastore_client(), state.catalog(), &vid, lsn)
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

    let lsns = checkpoint..=lsn;
    let segments = state.catalog().scan_segments(&vid, &lsns);
    for result in segments {
        let (key, splinter) = result.or_into_ctx()?;

        let cut = graft.to_mut().cut(&splinter);
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

        if graft.is_empty() {
            // all pages have been found
            break;
        }
    }

    let mut result = ReadPagesResponse { pages: Vec::with_capacity(num_pages) };
    while let Some((segment, cut)) = loading.try_next().await.or_into_ctx()? {
        let segment = ClosedSegment::from_bytes(&segment).or_into_ctx()?;

        for pageidx in cut.iter() {
            let page = segment
                .find_page(&vid, pageidx.try_into()?)
                .expect("bug: failed to find expected pageidx in segment; index out of sync");
            result.pages.push(PageAtIdx { pageidx, data: page.into() });
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
    use graft_client::{MetastoreClient, NetClient};
    use graft_core::{
        PageIdx,
        gid::{ClientId, SegmentId},
        lsn,
        page::Page,
        page_count::PageCount,
        pageidx,
    };
    use graft_proto::common::v1::SegmentInfo;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use prost::Message;
    use splinter_rs::{Encodable, PartitionWrite, Splinter};
    use tokio::sync::mpsc;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        bytes_vec::BytesVec,
        segment::{cache::mem::MemCache, loader::SegmentLoader, open::OpenSegment},
        volume::{catalog::VolumeCatalog, commit::CommitMeta, updater::VolumeCatalogUpdater},
    };

    use super::*;

    fn mksegment(vid: &VolumeId, pages: Vec<(PageIdx, Page)>) -> (SegmentId, BytesVec, Splinter) {
        let mut segment = OpenSegment::default();
        let mut graft = Splinter::default();
        for (off, page) in pages {
            segment.insert(vid.clone(), off, page).unwrap();
            graft.insert(off.to_u32());
        }
        let (sid, buf) = segment.serialize();
        (sid, buf, graft)
    }

    #[graft_test::test]
    #[tokio::test]
    async fn test_read_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());
        let catalog = VolumeCatalog::open_temporary().unwrap();
        let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);

        let (page_tx, _) = mpsc::channel(128);

        let client = NetClient::new_with_proxy(None, None);
        let metastore_uri = "http://127.0.0.1:3000".parse().unwrap();

        let state = Arc::new(PagestoreApiState::new(
            page_tx,
            catalog.clone(),
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

        // setup test data
        let lsn: LSN = lsn!(2);
        let vid = VolumeId::random();
        let cid = ClientId::random();

        // segment 1 is in the store
        let (sid1, segment, graft1) = mksegment(
            &vid,
            vec![
                (pageidx!(1), Page::test_filled(1)),
                (pageidx!(2), Page::test_filled(2)),
                (pageidx!(3), Page::test_filled(3)),
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
        let (sid2, segment, graft2) = mksegment(
            &vid,
            vec![
                (pageidx!(4), Page::test_filled(4)),
                (pageidx!(5), Page::test_filled(5)),
            ],
        );
        cache.put(&sid2, segment).await.unwrap();

        // notify the catalog about the segments
        let mut batch = catalog.batch_insert();
        batch
            .insert_snapshot(
                vid.clone(),
                CommitMeta::new(
                    vid.clone(),
                    cid,
                    lsn,
                    LSN::FIRST,
                    PageCount::new(5),
                    SystemTime::now(),
                ),
                vec![
                    SegmentInfo {
                        sid: sid1.copy_to_bytes(),
                        graft: graft1.encode_to_bytes(),
                    },
                    SegmentInfo {
                        sid: sid2.copy_to_bytes(),
                        graft: graft2.encode_to_bytes(),
                    },
                ],
            )
            .unwrap();
        batch.commit().unwrap();

        // we are finally able to test read_pages :)
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            graft: (1u32..=5).collect::<Splinter>().encode_to_bytes(),
        };
        let resp = server.post("/").bytes(req.encode_to_vec().into()).await;
        if resp.status_code() != 200 {
            let data = resp.as_bytes();
            println!("response: {data:?}");
            panic!("unexpected response status: {}", resp.status_code());
        }
        let mut resp = ReadPagesResponse::decode(resp.into_bytes()).unwrap();

        // we expect to see all 5 pages here
        assert_eq!(resp.pages.len(), 5);
        // sort by pageidx to make the test deterministic
        resp.pages.sort_by_key(|p| p.pageidx);
        for (PageAtIdx { pageidx, data }, expected) in resp.pages.into_iter().zip(1..) {
            assert_eq!(pageidx, expected);
            assert_eq!(
                data,
                Bytes::from(Page::test_filled(expected as u8)),
                "page data mismatch for pageidx: {pageidx}",
            );
        }
    }
}
