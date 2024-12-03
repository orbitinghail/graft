use std::{
    fmt::Display,
    ops::{Add, Sub},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::page_count::PageCount;

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    FromBytes,
    KnownLayout,
    IntoBytes,
    Immutable,
)]
#[repr(transparent)]
/// The position of a page within a volume, measured in terms of page numbers
/// rather than bytes. The offset represents the index of the page, with the
/// first page in the volume having an offset of 0.
pub struct PageOffset(u32);

impl PageOffset {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u32::MAX - 1);

    #[inline]
    pub fn new(offset: u32) -> Self {
        debug_assert!(Self::MAX >= offset, "page offset out of bounds");
        Self(offset)
    }

    pub fn next(&self) -> Self {
        Self::new(self.0 + 1)
    }

    pub fn pages(&self) -> PageCount {
        PageCount::new(self.0 + 1)
    }
}

impl FromStr for PageOffset {
    type Err = std::num::ParseIntError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u32>().map(Self::new)
    }
}

impl Display for PageOffset {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u32> for PageOffset {
    #[inline]
    fn from(offset: u32) -> Self {
        Self(offset)
    }
}

impl From<PageOffset> for u32 {
    #[inline]
    fn from(offset: PageOffset) -> u32 {
        offset.0
    }
}

impl From<&PageOffset> for u32 {
    #[inline]
    fn from(offset: &PageOffset) -> u32 {
        offset.0
    }
}

impl<'a> From<&'a u32> for &'a PageOffset {
    #[inline]
    fn from(value: &'a u32) -> Self {
        zerocopy::transmute_ref!(value)
    }
}

impl PartialEq<u32> for PageOffset {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u32> for PageOffset {
    #[inline]
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<PageOffset> for zerocopy::U32<E> {
    #[inline]
    fn from(value: PageOffset) -> Self {
        zerocopy::U32::new(value.0)
    }
}

impl<E: zerocopy::ByteOrder> From<zerocopy::U32<E>> for PageOffset {
    #[inline]
    fn from(value: zerocopy::U32<E>) -> Self {
        Self(value.get())
    }
}

impl Add<PageCount> for PageOffset {
    type Output = Self;

    #[inline]
    fn add(self, rhs: PageCount) -> Self {
        Self::new(self.0 + u32::from(rhs))
    }
}

impl Sub<PageCount> for PageOffset {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: PageCount) -> Self {
        Self::new(self.0 - u32::from(rhs))
    }
}
