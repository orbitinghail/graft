use std::{collections::HashMap, iter::once, sync::Arc};

use culprit::{Result, ResultExt};

use graft_core::{
    PageIdx, VolumeId,
    lsn::LSN,
    page::{EMPTY_PAGE, Page},
};
use splinter::Splinter;

use crate::{ClientErr, ClientPair, oracle::Oracle};

use super::{
    storage::{Storage, page::PageValue, snapshot::Snapshot},
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
                (_, PageValue::Pending) => {
                    if let Some((remote_lsn, local_lsn)) = snapshot.remote_mapping().splat() {
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
        %remote_lsn,
        %local_lsn,
        %pageidx,
    )
    .entered();

    // predict future page fetches using the oracle, then eliminate pages we
    // have already fetched while building our update hashmap.
    let mut graft = Splinter::default();
    let mut pages = HashMap::new();
    for pageidx in once(pageidx).chain(oracle.predict_next(pageidx)) {
        let (lsn, page) = storage.read(vid, local_lsn, pageidx).or_into_ctx()?;
        if matches!(page, PageValue::Pending) {
            graft.insert(pageidx.to_u32());
            pages.insert(pageidx, (lsn, PageValue::Empty));
        }
    }

    // process client results and update the hashmap
    let response = clients
        .pagestore()
        .read_pages(vid, remote_lsn, graft.serialize_to_bytes())?;
    for page in response {
        if let Some(entry) = pages.get_mut(&page.pageidx().or_into_ctx()?) {
            entry.1 = page.page().or_into_ctx()?.into();
        }
    }

    // update local storage with fetched pages
    storage.receive_pages(vid, &pages).or_into_ctx()?;

    // return the requested page
    Ok(pages
        .remove(&pageidx)
        .map_or(EMPTY_PAGE, |(_, p)| p.expect("bug: page not in update set")))
}
