use std::{
    fmt::{Debug, Display},
    num::ParseIntError,
    str::FromStr,
};

use thiserror::Error;
use zerocopy::{ByteHash, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::page_count::PageCount;

#[derive(Error, Debug)]
#[error("page offset out of bounds")]
pub struct PageOffsetOverflow;

#[derive(
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ByteHash,
    KnownLayout,
    IntoBytes,
    FromBytes,
    Unaligned,
    Immutable,
)]
#[repr(transparent)]
/// The position of a page within a volume, measured in terms of page numbers
/// rather than bytes. The offset represents the index of the page, with the
/// first page in the volume having an offset of 0.
pub struct PageOffset(NonZero<u32>);

/// Create a page offset at compile time
#[macro_export]
macro_rules! page_offset {
    ($offset:literal) => {
        assert!($offset <= 0xFF_FF_FF, "page offset out of bounds");
        graft_core::page_offset::PageOffset::saturating_from_u32($offset)
    };
}

impl PageOffset {
    pub const ZERO: Self = Self([0; 3]);
    pub const MAX: Self = Self([0xFF; 3]);
    const MAX_U32: u32 = 0xFF_FF_FF;

    #[inline]
    pub const fn try_from_u32(n: u32) -> Result<Self, PageOffsetOverflow> {
        if n <= Self::MAX_U32 {
            Ok(Self::saturating_from_u32(n))
        } else {
            Err(PageOffsetOverflow)
        }
    }

    #[inline]
    pub const fn saturating_from_u32(n: u32) -> Self {
        let [_, b @ ..] = n.to_be_bytes();
        Self(b)
    }

    #[inline]
    pub const fn to_u32(self) -> u32 {
        let Self([a, b, c]) = self;
        u32::from_be_bytes([0, a, b, c])
    }

    #[inline]
    pub const fn saturating_next(self) -> Self {
        Self::saturating_from_u32(self.to_u32() + 1)
    }

    #[inline]
    pub const fn saturating_prev(self) -> Self {
        Self::saturating_from_u32(self.to_u32() - 1)
    }

    #[inline]
    pub const fn pages(self) -> PageCount {
        // PageCount::new(self.0 + 1)
        todo!()
    }
}

#[derive(Error, Debug)]
pub enum ParsePageOffsetErr {
    #[error(transparent)]
    Overflow(#[from] PageOffsetOverflow),

    #[error("invalid page offset: {0}")]
    ParseIntError(#[from] ParseIntError),
}

impl FromStr for PageOffset {
    type Err = ParsePageOffsetErr;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let n = s.parse::<u32>()?;
        Ok(Self::try_from_u32(n)?)
    }
}

impl Display for PageOffset {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_u32())
    }
}

impl Debug for PageOffset {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_u32())
    }
}

impl TryFrom<usize> for PageOffset {
    type Error = PageOffsetOverflow;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let n: u32 = value.try_into().map_err(|_| PageOffsetOverflow)?;
        Self::try_from_u32(n)
    }
}

impl TryFrom<u32> for PageOffset {
    type Error = PageOffsetOverflow;

    #[inline]
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::try_from_u32(n)
    }
}

impl From<PageOffset> for u32 {
    #[inline]
    fn from(offset: PageOffset) -> u32 {
        offset.to_u32()
    }
}

impl PartialEq<PageOffset> for u32 {
    #[inline]
    fn eq(&self, other: &PageOffset) -> bool {
        *self == other.to_u32()
    }
}

impl PartialEq<u32> for PageOffset {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.to_u32() == *other
    }
}

impl PartialOrd<PageOffset> for u32 {
    #[inline]
    fn partial_cmp(&self, other: &PageOffset) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.to_u32())
    }
}

impl PartialOrd<u32> for PageOffset {
    #[inline]
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        self.to_u32().partial_cmp(other)
    }
}
