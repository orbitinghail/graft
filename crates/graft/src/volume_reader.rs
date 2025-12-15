use std::borrow::Cow;

use crate::core::{PageCount, PageIdx, VolumeId, page::Page};

use crate::{GraftErr, rt::runtime::Runtime, snapshot::Snapshot, volume_writer::VolumeWriter};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> PageCount;
    fn read_page(&self, pageidx: PageIdx) -> Result<Page, GraftErr>;
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

impl From<VolumeReader> for VolumeWriter {
    fn from(reader: VolumeReader) -> Self {
        Self::new(reader.runtime, reader.vid, reader.snapshot)
    }
}

impl VolumeRead for VolumeReader {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> PageCount {
        self.snapshot.page_count
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, GraftErr> {
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

    fn page_count(&self) -> PageCount {
        match self {
            VolumeReadRef::Reader(r) => r.page_count(),
            VolumeReadRef::Writer(w) => w.page_count(),
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, GraftErr> {
        match self {
            VolumeReadRef::Reader(r) => r.read_page(pageidx),
            VolumeReadRef::Writer(w) => w.read_page(pageidx),
        }
    }
}
