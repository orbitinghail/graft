use std::{
    fmt::{Debug, Display},
    num::{NonZero, ParseIntError, TryFromIntError},
    str::FromStr,
    u32,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::page_count::PageCount;

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ByteHash,
    KnownLayout,
    IntoBytes,
    TryFromBytes,
    Immutable,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
/// The index of a page within a volume. The first page of a volume has a page
/// index of 1.
pub struct PageIdx(NonZero<u32>);

#[macro_export]
/// Create a PageIndex from a literal at compile time.
macro_rules! pageidx {
    ($idx:literal) => {
        $crate::page_index::PageIdx::try_new($idx).expect("page index out of range")
    };
}

impl PageIdx {
    pub const FIRST: Self = pageidx!(1);
    pub const LAST: Self = pageidx!(0xFFFF_FFFF);

    /// Create a new PageIndex from a u32. Returns None if the PageIndex is 0.
    #[inline]
    pub const fn try_new(n: u32) -> Option<Self> {
        match NonZero::new(n) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }

    #[inline]
    pub const unsafe fn new_unchecked(n: u32) -> Self {
        Self(NonZero::new_unchecked(n))
    }

    #[inline]
    pub const fn to_u32(self) -> u32 {
        self.0.get()
    }

    #[inline]
    pub const fn saturating_next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    #[inline]
    pub const fn saturating_prev(self) -> Self {
        let prev = self.0.get().saturating_sub(1);
        match NonZero::new(prev) {
            Some(n) => Self(n),
            None => Self::FIRST,
        }
    }

    #[inline]
    pub const fn pages(self) -> PageCount {
        PageCount::new(self.to_u32())
    }

    #[inline]
    pub const fn is_first_page(self) -> bool {
        self.0.get() == Self::FIRST.0.get()
    }
}

impl Default for PageIdx {
    #[inline]
    fn default() -> Self {
        Self::FIRST
    }
}

#[derive(Error, Debug)]
pub enum ConvertToPageIdxErr {
    #[error("page index must be greater than zero")]
    Zero,

    #[error("invalid page index: {0}")]
    ParseIntErr(#[from] ParseIntError),

    #[error("invalid page index: {0}")]
    TryFromIntErr(#[from] TryFromIntError),
}

impl FromStr for PageIdx {
    type Err = ConvertToPageIdxErr;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let n = s.parse::<u32>()?;
        n.try_into()
    }
}

impl Display for PageIdx {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_u32())
    }
}

impl Debug for PageIdx {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_u32())
    }
}

impl TryFrom<usize> for PageIdx {
    type Error = ConvertToPageIdxErr;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let n: u32 = value.try_into()?;
        n.try_into()
    }
}

impl TryFrom<u32> for PageIdx {
    type Error = ConvertToPageIdxErr;

    #[inline]
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        match Self::try_new(n) {
            Some(idx) => Ok(idx),
            None => Err(ConvertToPageIdxErr::Zero),
        }
    }
}

impl From<PageIdx> for u32 {
    #[inline]
    fn from(idx: PageIdx) -> u32 {
        idx.to_u32()
    }
}

impl PartialEq<PageIdx> for u32 {
    #[inline]
    fn eq(&self, other: &PageIdx) -> bool {
        *self == other.to_u32()
    }
}

impl PartialEq<u32> for PageIdx {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.to_u32() == *other
    }
}

impl PartialOrd<PageIdx> for u32 {
    #[inline]
    fn partial_cmp(&self, other: &PageIdx) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.to_u32())
    }
}

impl PartialOrd<u32> for PageIdx {
    #[inline]
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        self.to_u32().partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<PageIdx> for zerocopy::U32<E> {
    #[inline]
    fn from(value: PageIdx) -> Self {
        zerocopy::U32::new(value.to_u32())
    }
}

impl<E: zerocopy::ByteOrder> TryFrom<zerocopy::U32<E>> for PageIdx {
    type Error = ConvertToPageIdxErr;

    fn try_from(value: zerocopy::U32<E>) -> Result<Self, Self::Error> {
        let n = value.get();
        n.try_into()
    }
}
