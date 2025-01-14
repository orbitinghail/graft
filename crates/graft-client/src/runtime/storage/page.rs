use std::fmt::Debug;

use std::fmt::Display;

use bytes::Bytes;
use culprit::Culprit;
use culprit::ResultExt;
use fjall::Slice;
use graft_core::lsn::LSN;

use graft_core::page::Page;
use graft_core::page::PageSizeErr;
use graft_core::page::EMPTY_PAGE;
use graft_core::page_offset::PageOffset;

use graft_core::VolumeId;
use thiserror::Error;
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

    pub fn lsn(&self) -> LSN {
        self.lsn.into()
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

#[derive(Debug, Error)]
pub enum PageValueConversionErr {
    #[error("invalid page size")]
    PageSize(#[from] PageSizeErr),

    #[error("invalid page value mark")]
    InvalidMark,
}

const PAGE_VALUE_PENDING: &'static [u8] = b"PENDING_";
const PAGE_VALUE_EMPTY: &'static [u8] = b"EMPTY___";
const PAGE_VALUE_MARK_LEN: usize = 8;

static_assertions::const_assert_eq!(PAGE_VALUE_PENDING.len(), PAGE_VALUE_MARK_LEN);
static_assertions::const_assert_eq!(PAGE_VALUE_EMPTY.len(), PAGE_VALUE_MARK_LEN);

/// PageValue is used to read and write pages to storage.
/// It uses the length of the page to determine if the page is available or
/// pending.
pub enum PageValue {
    Pending,
    Empty,
    Available(Page),
}

impl PageValue {
    /// resolves the PageValue to a Page, panicing if the page is Pending
    pub fn expect(self, msg: &str) -> Page {
        match self {
            PageValue::Pending => panic!("{}", msg),
            PageValue::Empty => EMPTY_PAGE,
            PageValue::Available(page) => page,
        }
    }
}

impl TryFrom<Slice> for PageValue {
    type Error = Culprit<PageValueConversionErr>;

    #[inline]
    fn try_from(value: Slice) -> Result<Self, Self::Error> {
        Bytes::from(value).try_into()
    }
}

impl TryFrom<Bytes> for PageValue {
    type Error = Culprit<PageValueConversionErr>;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() == PAGE_VALUE_MARK_LEN {
            match value.as_ref() {
                PAGE_VALUE_PENDING => Ok(PageValue::Pending),
                PAGE_VALUE_EMPTY => Ok(PageValue::Empty),
                _ => Err(Culprit::new_with_note(
                    PageValueConversionErr::InvalidMark,
                    format!("invalid page value mark: {:?}", value),
                )),
            }
        } else {
            Ok(PageValue::Available(Page::try_from(value).or_into_ctx()?))
        }
    }
}

impl From<PageValue> for Bytes {
    fn from(val: PageValue) -> Self {
        match val {
            PageValue::Pending => Bytes::from_static(PAGE_VALUE_PENDING),
            PageValue::Empty => Bytes::from_static(PAGE_VALUE_EMPTY),
            PageValue::Available(page) => page.into(),
        }
    }
}

impl From<PageValue> for Slice {
    fn from(value: PageValue) -> Self {
        Bytes::from(value).into()
    }
}
