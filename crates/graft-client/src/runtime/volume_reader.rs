use std::{collections::HashMap, iter::once, sync::Arc};

use culprit::{Result, ResultExt};

use graft_core::{
    lsn::LSN,
    page::{Page, EMPTY_PAGE},
    PageIdx, VolumeId,
};
use splinter::Splinter;
use tryiter::TryIteratorExt;

use crate::{oracle::Oracle, ClientErr, ClientPair};

use super::{
    storage::{page::PageValue, snapshot::Snapshot, Storage},
    volume_writer::VolumeWriter,
};

pub trait VolumeRead {
    fn vid(&self) -> &VolumeId;

    /// Retrieve the Volume snapshot backing this reader
    fn snapshot(&self) -> Option<&Snapshot>;

    /// Read a page from the snapshot
    fn read<O: Oracle>(&self, oracle: &mut O, pageidx: PageIdx) -> Result<Page, ClientErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    vid: VolumeId,
    snapshot: Option<Snapshot>,
    clients: Arc<ClientPair>,
    storage: Arc<Storage>,
}

impl VolumeReader {
    pub(crate) fn new(
        vid: VolumeId,
        snapshot: Option<Snapshot>,
        clients: Arc<ClientPair>,
        storage: Arc<Storage>,
    ) -> Self {
        Self { vid, snapshot, clients, storage }
    }

    /// Upgrade this reader into a writer
    pub fn upgrade(self) -> VolumeWriter {
        self.into()
    }

    /// decompose this reader into snapshot and storage
    pub(crate) fn into_parts(self) -> (VolumeId, Option<Snapshot>, Arc<ClientPair>, Arc<Storage>) {
        (self.vid, self.snapshot, self.clients, self.storage)
    }
}

impl VolumeRead for VolumeReader {
    #[inline]
    fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    fn read<O: Oracle>(&self, oracle: &mut O, pageidx: PageIdx) -> Result<Page, ClientErr> {
        if let Some(snapshot) = self.snapshot() {
            match self
                .storage
                .read(self.vid(), snapshot.local(), pageidx)
                .or_into_ctx()?
            {
                (_, PageValue::Available(page)) => {
                    oracle.observe_cache_hit(pageidx);
                    Ok(page)
                }
                (_, PageValue::Empty) => {
                    oracle.observe_cache_hit(pageidx);
                    Ok(EMPTY_PAGE)
                }
                (local_lsn, PageValue::Pending) => {
                    if let Some(remote_lsn) = snapshot.remote() {
                        fetch_page(
                            &self.clients,
                            &self.storage,
                            oracle,
                            self.vid(),
                            remote_lsn,
                            local_lsn,
                            pageidx,
                        )
                        .or_into_ctx()
                    } else {
                        Ok(EMPTY_PAGE)
                    }
                }
            }
        } else {
            Ok(EMPTY_PAGE)
        }
    }
}

fn fetch_page<O: Oracle>(
    clients: &ClientPair,
    storage: &Storage,
    oracle: &mut O,
    vid: &VolumeId,
    remote_lsn: LSN,
    local_lsn: LSN,
    pageidx: PageIdx,
) -> Result<Page, ClientErr> {
    let _span = tracing::trace_span!(
        "fetching page from pagestore",
        ?vid,
        ?remote_lsn,
        ?local_lsn,
        ?pageidx,
    )
    .entered();

    // predict future page fetches using the oracle, then eliminate pages we
    // have already fetched.
    let mut graft = Splinter::default();
    let pageidxs = once(pageidx).chain(oracle.predict_next(pageidx)).map(Ok);
    let mut existing_iter = storage.query_pages(vid, local_lsn, pageidxs);
    while let Some((pageidx, page)) = existing_iter.try_next().or_into_ctx()? {
        if let None | Some(PageValue::Pending) = page {
            graft.insert(pageidx.to_u32())
        }
    }

    // process client results and reshape into a hashmap
    let pages: HashMap<PageIdx, Page> = clients
        .pagestore()
        .read_pages(vid, remote_lsn, graft.serialize_to_bytes())?
        .into_iter()
        .map(|p| Ok((p.pageidx().or_into_ctx()?, p.page().or_into_ctx()?)))
        .collect::<Result<_, ClientErr>>()?;

    storage
        .receive_pages(vid, local_lsn, graft, &pages)
        .or_into_ctx()?;

    Ok(pages.get(&pageidx).cloned().unwrap_or(EMPTY_PAGE))
}
