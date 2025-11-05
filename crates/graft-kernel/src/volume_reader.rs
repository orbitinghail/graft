use std::borrow::Cow;

use culprit::{Culprit, ResultExt};
use graft_core::{PageCount, PageIdx, page::Page};

use crate::{
    GraftErr, page_status::PageStatus, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot,
    volume_name::VolumeName, volume_writer::VolumeWriter,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> culprit::Result<PageCount, GraftErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, GraftErr>;
    fn page_status(&self, pageidx: PageIdx) -> culprit::Result<PageStatus, GraftErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    name: VolumeName,
    runtime: RuntimeHandle,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(name: VolumeName, runtime: RuntimeHandle, snapshot: Snapshot) -> Self {
        Self { name, runtime, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<GraftErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.name,
            reader.runtime,
            reader.snapshot,
            page_count,
        ))
    }
}

impl VolumeRead for VolumeReader {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> culprit::Result<PageCount, GraftErr> {
        self.runtime
            .storage()
            .read()
            .page_count(&self.snapshot)
            .or_into_ctx()
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, GraftErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }

    fn page_status(&self, pageidx: PageIdx) -> culprit::Result<PageStatus, GraftErr> {
        self.runtime.page_status(&self.snapshot, pageidx)
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

    fn page_count(&self) -> culprit::Result<PageCount, GraftErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.page_count(),
            VolumeReadRef::Writer(writer) => writer.page_count(),
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, GraftErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.read_page(pageidx),
            VolumeReadRef::Writer(writer) => writer.read_page(pageidx),
        }
    }

    fn page_status(&self, pageidx: PageIdx) -> culprit::Result<PageStatus, GraftErr> {
        match self {
            VolumeReadRef::Reader(reader) => reader.page_status(pageidx),
            VolumeReadRef::Writer(writer) => writer.page_status(pageidx),
        }
    }
}
