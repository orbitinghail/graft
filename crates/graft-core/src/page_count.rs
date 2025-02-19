use std::fmt::Display;

use splinter::SPLINTER_MAX_VALUE;
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::{
    byte_unit::ByteUnit,
    page::PAGESIZE,
    page_offset::{PageOffset, PageOffsetOverflow},
    page_range::PageRange,
};

#[derive(Error, Debug)]
#[error("page count out of bounds")]
pub struct PageCountOverflow;

impl From<PageOffsetOverflow> for PageCountOverflow {
    #[inline]
    fn from(_: PageOffsetOverflow) -> Self {
        Self
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    KnownLayout,
    IntoBytes,
    FromBytes,
    Unaligned,
    Immutable,
)]
#[repr(transparent)]
/// The number of pages in a volume.
pub struct PageCount(PageOffset);

impl PageCount {
    pub const ZERO: Self = Self(PageOffset::ZERO);
    pub const ONE: Self = Self(1);
    pub const MAX: Self = Self(SPLINTER_MAX_VALUE + 1);

    #[inline]
    pub const fn new(count: u32) -> Self {
        assert!(count <= Self::MAX.0, "page count out of bounds");
        Self(count)
    }

    #[inline]
    pub const fn try_from_u32(offset: u32) -> Result<Self, PageCountOverflow> {
        if offset <= Self::MAX.0 {
            Ok(Self(offset))
        } else {
            Err(PageCountOverflow)
        }
    }

    #[inline]
    pub const fn offsets(self) -> PageRange {
        // PageRange's end is exclusive, so this is correct for all values of Self
        let end = PageOffset::new(self.0);
        PageRange::new(PageOffset::ZERO, end)
    }

    #[inline]
    pub const fn last_offset(self) -> Option<PageOffset> {
        let prev = self.0.checked_sub(1);
        match prev {
            Some(offset) => Some(PageOffset::new(offset)),
            None => None,
        }
    }

    #[inline]
    pub const fn saturating_incr(self) -> Self {
        let next = self.0 + 1;
        if next > Self::MAX.0 {
            Self::MAX
        } else {
            Self(next)
        }
    }

    #[inline]
    pub const fn saturating_decr(self) -> Self {
        Self(self.0.saturating_sub(1))
    }

    #[inline]
    pub const fn to_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn to_u32(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn contains(self, offset: PageOffset) -> bool {
        offset.to_u32() < self.0
    }

    #[inline]
    pub fn size(self) -> ByteUnit {
        PAGESIZE * self.0
    }
}

impl Display for PageCount {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<u32> for PageCount {
    type Error = PageCountOverflow;

    #[inline]
    fn try_from(count: u32) -> Result<Self, Self::Error> {
        Self::try_from_u32(count)
    }
}

impl TryFrom<usize> for PageCount {
    type Error = PageCountOverflow;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let n: u32 = value.try_into().map_err(|_| PageCountOverflow)?;
        Self::try_from_u32(n)
    }
}

impl From<PageCount> for u32 {
    #[inline]
    fn from(count: PageCount) -> u32 {
        count.to_u32()
    }
}

impl PartialEq<PageCount> for u32 {
    #[inline]
    fn eq(&self, other: &PageCount) -> bool {
        *self == other.0
    }
}

impl PartialEq<u32> for PageCount {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<PageCount> for u32 {
    #[inline]
    fn partial_cmp(&self, other: &PageCount) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl PartialOrd<u32> for PageCount {
    #[inline]
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<PageCount> for zerocopy::U32<E> {
    #[inline]
    fn from(value: PageCount) -> Self {
        zerocopy::U32::new(value.0)
    }
}

impl<E: zerocopy::ByteOrder> TryFrom<zerocopy::U32<E>> for PageCount {
    type Error = PageCountOverflow;

    #[inline]
    fn try_from(value: zerocopy::U32<E>) -> Result<Self, Self::Error> {
        Self::try_from_u32(value.get())
    }
}
