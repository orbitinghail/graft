use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{
    lsn::LSN,
    page::{Page, PageSizeErr, EMPTY_PAGE},
    zerocopy_ext::TryFromBytesExt,
    PageIdx, VolumeId,
};
use std::fmt::{Debug, Display};
use thiserror::Error;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned, BE, U32, U64};

use super::StorageErr;

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned)]
#[repr(C)]
pub struct PageKey {
    vid: VolumeId,
    index: U32<BE>,
    lsn: U64<BE>,
}

impl PageKey {
    #[inline]
    pub fn new(vid: VolumeId, index: PageIdx, lsn: LSN) -> Self {
        Self {
            vid,
            index: index.into(),
            lsn: lsn.into(),
        }
    }

    #[track_caller]
    pub(crate) fn try_ref_from_bytes(bytes: &[u8]) -> Result<&Self, Culprit<StorageErr>> {
        TryFromBytesExt::try_ref_from_unaligned_bytes(bytes).or_ctx(StorageErr::CorruptKey)
    }

    #[inline]
    pub fn with_index(self, index: PageIdx) -> Self {
        Self { index: index.into(), ..self }
    }

    pub fn lsn(&self) -> LSN {
        self.lsn.try_into().expect("invalid LSN")
    }
}

impl AsRef<[u8]> for PageKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.vid.short(), self.index, self.lsn)
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
            index: self.index,
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

const PAGE_VALUE_PENDING: &[u8] = b"PENDING_";
const PAGE_VALUE_EMPTY: &[u8] = b"EMPTY___";
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

impl From<Page> for PageValue {
    fn from(page: Page) -> Self {
        if page.is_empty() {
            PageValue::Empty
        } else {
            PageValue::Available(page)
        }
    }
}
