use std::borrow::Cow;

use culprit::{Culprit, ResultExt};
use graft_core::{PageCount, PageIdx, VolumeId, page::Page, pageset::PageSet};

use crate::{
    KernelErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot, volume_writer::VolumeWriter,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> culprit::Result<PageCount, KernelErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr>;
    fn missing_pages(&self) -> culprit::Result<PageSet, KernelErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    runtime: RuntimeHandle,
    graft: VolumeId,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(runtime: RuntimeHandle, graft: VolumeId, snapshot: Snapshot) -> Self {
        Self { runtime, graft, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<KernelErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.runtime,
            reader.graft,
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
        if let Some((vid, lsn)) = self.snapshot.head() {
            Ok(self
                .runtime
                .storage()
                .read()
                .page_count(vid, lsn)
                .or_into_ctx()?
                .expect("BUG: missing page count for snapshot"))
        } else {
            Ok(PageCount::ZERO)
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }

    fn missing_pages(&self) -> culprit::Result<PageSet, KernelErr> {
        self.runtime.missing_pages(&self.snapshot)
    }
}

impl From<VolumeReader> for VolumeReadRef<'_> {
    fn from(reader: VolumeReader) -> Self {
        VolumeReadRef::Reader(Cow::Owned(reader))
    }
}

impl<'a> From<&'a VolumeReader> for VolumeReadRef<'a> {
    fn from(reader: &'a VolumeReader) -> Self {
        VolumeReadRef::Reader(Cow::Borrowed(reader))
    }
}

pub enum VolumeReadRef<'a> {
    Reader(Cow<'a, VolumeReader>),
    Writer(&'a VolumeWriter),
}

impl VolumeRead for VolumeReadRef<'_> {
    fn snapshot(&self) -> &Snapshot {
        match self {
            VolumeReadRef::Reader(reader) => reader.snapshot(),
            VolumeReadRef::Writer(writer) => writer.snapshot(),
        }
    }

    fn page_count(&self) -> culprit::Result<PageCount, KernelErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.page_count(),
            VolumeReadRef::Writer(writer) => writer.page_count(),
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.read_page(pageidx),
            VolumeReadRef::Writer(writer) => writer.read_page(pageidx),
        }
    }

    fn missing_pages(&self) -> culprit::Result<PageSet, KernelErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.missing_pages(),
            VolumeReadRef::Writer(writer) => writer.missing_pages(),
        }
    }
}
