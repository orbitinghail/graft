use std::{borrow::Cow, collections::HashMap, iter::once, sync::Arc};

use culprit::{Result, ResultExt};

use graft_core::{PageIdx, VolumeId, lsn::LSN, page::Page};
use splinter_rs::{Encodable, PartitionWrite, Splinter};
use tracing::field;

use crate::{ClientErr, ClientPair, oracle::Oracle};

use super::{
    storage::{
        Storage,
        page::{PageStatus, PageValue},
        snapshot::Snapshot,
    },
    volume_writer::VolumeWriter,
};

pub trait VolumeRead {
    fn vid(&self) -> &VolumeId;

    /// Retrieve the Volume snapshot backing this reader
    fn snapshot(&self) -> Option<&Snapshot>;

    /// Read a page from the snapshot
    fn read<O: Oracle>(&self, oracle: &mut O, pageidx: PageIdx) -> Result<Page, ClientErr>;

    /// Retrieve a page's status
    fn status(&self, pageidx: PageIdx) -> Result<PageStatus, ClientErr>;
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
                    Ok(Page::EMPTY)
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
                        Ok(Page::EMPTY)
                    }
                }
            }
        } else {
            Ok(Page::EMPTY)
        }
    }

    fn status(&self, pageidx: PageIdx) -> Result<PageStatus, ClientErr> {
        if let Some(snapshot) = self.snapshot() {
            match self
                .storage
                .read(self.vid(), snapshot.local(), pageidx)
                .or_into_ctx()?
            {
                (lsn, PageValue::Available(_)) => Ok(PageStatus::Available(lsn)),
                (lsn, PageValue::Empty) => Ok(PageStatus::Empty(Some(lsn))),
                (_, PageValue::Pending) => Ok(PageStatus::Pending),
            }
        } else {
            Ok(PageStatus::Empty(None))
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
    let span = tracing::trace_span!(
        "fetching page from pagestore",
        ?vid,
        %remote_lsn,
        %local_lsn,
        %pageidx,
        num_pages=field::Empty,
    )
    .entered();

    // predict future page fetches using the oracle, then eliminate pages we
    // have already fetched while building our update hashmap.
    let mut graft = Splinter::default();
    let mut pages = HashMap::new();
    for idx in once(pageidx).chain(oracle.predict_next(pageidx)) {
        let (lsn, page) = storage.read(vid, local_lsn, idx).or_into_ctx()?;
        if matches!(page, PageValue::Pending) {
            graft.insert(idx.to_u32());
            pages.insert(idx, (lsn, PageValue::Empty));
        }
    }

    span.record("num_pages", pages.len());

    // process client results and update the hashmap
    let response = clients
        .pagestore()
        .read_pages(vid, remote_lsn, graft.encode_to_bytes())?;
    for page in response {
        if let Some(entry) = pages.get_mut(&page.pageidx().or_into_ctx()?) {
            entry.1 = page.page().or_into_ctx()?.into();
        } else {
            tracing::warn!(?vid, %remote_lsn, pageidx=page.pageidx, "unexpected page");
            precept::expect_unreachable!(
                "received unexpected page from pagestore",
                {
                    "vid": vid,
                    "remote_lsn": remote_lsn,
                    "pageidx": page.pageidx,
                }
            );
        }
    }

    let requested_page = pages
        .get(&pageidx)
        .cloned()
        .and_then(|(_, p)| p.try_into_page())
        .expect("requested page not found");

    // update local storage with fetched pages
    storage.receive_pages(vid, pages).or_into_ctx()?;

    // return the requested page
    Ok(requested_page)
}

pub enum VolumeReadRef<'a> {
    Reader(Cow<'a, VolumeReader>),
    Writer(&'a VolumeWriter),
}

impl VolumeRead for VolumeReadRef<'_> {
    fn vid(&self) -> &VolumeId {
        match self {
            VolumeReadRef::Reader(reader) => reader.vid(),
            VolumeReadRef::Writer(writer) => writer.vid(),
        }
    }

    fn snapshot(&self) -> Option<&Snapshot> {
        match self {
            VolumeReadRef::Reader(reader) => reader.snapshot(),
            VolumeReadRef::Writer(writer) => writer.snapshot(),
        }
    }

    fn read<O: Oracle>(&self, oracle: &mut O, pageidx: PageIdx) -> Result<Page, ClientErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.read(oracle, pageidx),
            VolumeReadRef::Writer(writer) => writer.read(oracle, pageidx),
        }
    }

    fn status(&self, pageidx: PageIdx) -> Result<PageStatus, ClientErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.status(pageidx),
            VolumeReadRef::Writer(writer) => writer.status(pageidx),
        }
    }
}
