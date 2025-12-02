use std::borrow::Cow;

use culprit::Culprit;
use graft_core::{PageCount, PageIdx, VolumeId, page::Page};

use crate::{KernelErr, rt::runtime::Runtime, snapshot::Snapshot, volume_writer::VolumeWriter};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> culprit::Result<PageCount, KernelErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    runtime: Runtime,
    vid: VolumeId,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(runtime: Runtime, vid: VolumeId, snapshot: Snapshot) -> Self {
        Self { runtime, vid, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<KernelErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.runtime,
            reader.vid,
            reader.snapshot,
            page_count,
        ))
    }
}

impl VolumeRead for VolumeReader {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> culprit::Result<PageCount, KernelErr> {
        self.runtime.snapshot_pages(&self.snapshot)
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }
}

pub enum VolumeReadRef<'a> {
    Reader(Cow<'a, VolumeReader>),
    Writer(&'a VolumeWriter),
}

impl VolumeRead for VolumeReadRef<'_> {
    fn snapshot(&self) -> &Snapshot {
        match self {
            VolumeReadRef::Reader(r) => r.snapshot(),
            VolumeReadRef::Writer(w) => w.snapshot(),
        }
    }

    fn page_count(&self) -> culprit::Result<PageCount, KernelErr> {
        match self {
            VolumeReadRef::Reader(r) => r.page_count(),
            VolumeReadRef::Writer(w) => w.page_count(),
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        match self {
            VolumeReadRef::Reader(r) => r.read_page(pageidx),
            VolumeReadRef::Writer(w) => w.read_page(pageidx),
        }
    }
}
