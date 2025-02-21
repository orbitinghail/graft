use std::{fmt::Display, num::TryFromIntError};

use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{byte_unit::ByteUnit, page::PAGESIZE, page_idx::PageIdx};

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
    Immutable,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
/// The number of pages in a volume.
pub struct PageCount(u32);

impl PageCount {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1);
    pub const MAX: Self = Self(u32::MAX);

    #[inline]
    pub const fn new(count: u32) -> Self {
        Self(count)
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub const fn iter(self) -> PageCountIter {
        PageCountIter { idx: 0, limit: self.0 }
    }

    #[inline]
    pub const fn last_index(self) -> Option<PageIdx> {
        if self.is_empty() {
            None
        } else {
            // SAFETY: self is not empty
            Some(unsafe { PageIdx::new_unchecked(self.0) })
        }
    }

    #[inline]
    pub const fn saturating_incr(self) -> Self {
        Self(self.0.saturating_add(1))
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
    pub const fn contains(self, idx: PageIdx) -> bool {
        idx.to_u32() <= self.0
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

impl From<u32> for PageCount {
    #[inline]
    fn from(count: u32) -> Self {
        Self::new(count)
    }
}

impl From<PageCount> for u32 {
    #[inline]
    fn from(count: PageCount) -> u32 {
        count.to_u32()
    }
}

impl TryFrom<usize> for PageCount {
    type Error = TryFromIntError;

    #[inline]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let n: u32 = value.try_into()?;
        Ok(Self::new(n))
    }
}

impl PartialEq<PageCount> for u32 {
    #[inline]
    fn eq(&self, other: &PageCount) -> bool {
        *self == other.to_u32()
    }
}

impl PartialEq<u32> for PageCount {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.to_u32() == *other
    }
}

impl PartialOrd<PageCount> for u32 {
    #[inline]
    fn partial_cmp(&self, other: &PageCount) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.to_u32())
    }
}

impl PartialOrd<u32> for PageCount {
    #[inline]
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        self.to_u32().partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<PageCount> for zerocopy::U32<E> {
    #[inline]
    fn from(value: PageCount) -> Self {
        zerocopy::U32::new(value.to_u32())
    }
}

impl<E: zerocopy::ByteOrder> From<zerocopy::U32<E>> for PageCount {
    #[inline]
    fn from(value: zerocopy::U32<E>) -> Self {
        Self::new(value.get())
    }
}

pub struct PageCountIter {
    idx: u32,
    limit: u32,
}

impl Iterator for PageCountIter {
    type Item = PageIdx;

    fn next(&mut self) -> Option<Self::Item> {
        self.idx = self.idx.saturating_add(1);
        if self.idx <= self.limit {
            // SAFETY: self.idx was just incremented and saturates at numeric bounds
            Some(unsafe { PageIdx::new_unchecked(self.idx) })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pageidx;

    #[test]
    fn test_page_count_iter() {
        let count = super::PageCount::new(3);
        let mut iter = count.iter();
        assert_eq!(iter.next(), Some(pageidx!(1)));
        assert_eq!(iter.next(), Some(pageidx!(2)));
        assert_eq!(iter.next(), Some(pageidx!(3)));
        assert_eq!(iter.next(), None);

        let count = super::PageCount::default();
        let mut iter = count.iter();
        assert_eq!(iter.next(), None);

        let count = super::PageCount::new(1);
        let mut iter = count.iter();
        assert_eq!(iter.next(), Some(pageidx!(1)));
        assert_eq!(iter.next(), None);
    }
}
