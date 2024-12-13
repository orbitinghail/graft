use std::fmt::Debug;

use std::fmt::Display;

use bytes::Bytes;
use fjall::Slice;
use graft_core::lsn::LSN;

use graft_core::page::Page;
use graft_core::page::PageSizeErr;
use graft_core::page_offset::PageOffset;

use graft_core::VolumeId;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;
use zerocopy::TryFromBytes;
use zerocopy::Unaligned;
use zerocopy::U64;

use zerocopy::BE;

use zerocopy::U32;

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned)]
#[repr(C)]
pub struct PageKey {
    vid: VolumeId,
    offset: U32<BE>,
    lsn: U64<BE>,
}

impl PageKey {
    #[inline]
    pub fn new(vid: VolumeId, offset: PageOffset, lsn: LSN) -> Self {
        Self {
            vid,
            offset: offset.into(),
            lsn: lsn.into(),
        }
    }

    #[inline]
    pub fn set_offset(&mut self, offset: PageOffset) {
        self.offset = offset.into();
    }
}

impl AsRef<[u8]> for PageKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.vid.short(), self.offset, self.lsn)
    }
}

impl Debug for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Clone for PageKey {
    fn clone(&self) -> Self {
        Self {
            vid: self.vid.clone(),
            offset: self.offset,
            lsn: self.lsn,
        }
    }
}

/// PageValue is used to read and write pages to storage.
/// It uses the length of the page to determine if the page is available or pending.
pub enum PageValue {
    Pending,
    Available(Page),
}

impl TryFrom<Slice> for PageValue {
    type Error = PageSizeErr;

    fn try_from(value: Slice) -> Result<Self, Self::Error> {
        let bytes: Bytes = value.into();
        bytes.try_into()
    }
}

impl TryFrom<Bytes> for PageValue {
    type Error = PageSizeErr;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Ok(PageValue::Pending);
        }
        Ok(PageValue::Available(value.try_into()?))
    }
}

impl From<PageValue> for Bytes {
    fn from(val: PageValue) -> Self {
        match val {
            PageValue::Pending => Bytes::new(),
            PageValue::Available(page) => page.into(),
        }
    }
}

impl From<PageValue> for Slice {
    fn from(value: PageValue) -> Self {
        let bytes: Bytes = value.into();
        bytes.into()
    }
}
