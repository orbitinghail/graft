use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use culprit::ResultExt;
use futures::{stream::FuturesUnordered, FutureExt, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::pagestore::v1::{PageAtIdx, ReadPagesRequest, ReadPagesResponse};
use splinter::{ops::Cut, Splinter};

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
    let mut graft = Splinter::from_bytes(req.graft).or_into_ctx()?;
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
        .update_catalog_from_client(state.metastore_client(), state.catalog(), &vid, lsn)
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

        let cut = graft.cut(&splinter);
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
    while let Some((segment, cut)) = loading.try_next().await? {
        let segment = ClosedSegment::from_bytes(&segment).or_into_ctx()?;

        for pageidx in cut.iter() {
            let page = segment
                .find_page(vid.clone(), pageidx.try_into()?)
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
        gid::{ClientId, SegmentId},
        page::Page,
        page_count::PageCount,
        pageidx, PageIdx,
    };
    use graft_proto::common::v1::SegmentInfo;
    use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};
    use prost::Message;
    use tokio::sync::mpsc;

    use crate::{
        api::extractors::CONTENT_TYPE_PROTOBUF,
        bytes_vec::BytesVec,
        segment::{
            bus::Bus, cache::mem::MemCache, loader::SegmentLoader, multigraft::MultiGraft,
            open::OpenSegment,
        },
        volume::{catalog::VolumeCatalog, commit::CommitMeta, updater::VolumeCatalogUpdater},
    };

    use super::*;

    fn mksegment(sid: &SegmentId, pages: Vec<(VolumeId, PageIdx, Page)>) -> (BytesVec, MultiGraft) {
        let mut segment = OpenSegment::default();
        for (vid, off, page) in pages {
            segment.insert(vid, off, page).unwrap();
        }
        segment.serialize(sid.clone())
    }

    #[graft_test::test]
    async fn test_read_pages_sanity() {
        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());
        let catalog = VolumeCatalog::open_temporary().unwrap();
        let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);

        let (page_tx, _) = mpsc::channel(128);
        let commit_bus = Bus::new(128);

        let client = NetClient::new();
        let metastore_uri = "http://localhost:3000".parse().unwrap();

        let state = Arc::new(PagestoreApiState::new(
            page_tx,
            commit_bus,
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
        let lsn: LSN = LSN::new(2);
        let vid = VolumeId::random();
        let cid = ClientId::random();

        // segment 1 is in the store
        let sid1 = SegmentId::random();
        let (segment, graft1) = mksegment(
            &sid1,
            vec![
                (vid.clone(), pageidx!(1), Page::test_filled(1)),
                (vid.clone(), pageidx!(2), Page::test_filled(2)),
                (vid.clone(), pageidx!(3), Page::test_filled(3)),
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
        let (segment, graft2) = mksegment(
            &sid2,
            vec![
                (vid.clone(), pageidx!(4), Page::test_filled(4)),
                (vid.clone(), pageidx!(5), Page::test_filled(5)),
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
                        graft: graft1.get(&vid).unwrap().clone().into_inner(),
                    },
                    SegmentInfo {
                        sid: sid2.copy_to_bytes(),
                        graft: graft2.get(&vid).unwrap().clone().into_inner(),
                    },
                ],
            )
            .unwrap();
        batch.commit().unwrap();

        // we are finally able to test read_pages :)
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            graft: (1u32..=5).collect::<Splinter>().serialize_to_bytes(),
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
