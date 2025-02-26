use std::{
    fmt::{Debug, Display},
    iter::FusedIterator,
    num::{NonZero, ParseIntError, TryFromIntError},
    str::FromStr,
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
/// Create a `PageIndex` from a literal at compile time.
macro_rules! pageidx {
    ($idx:literal) => {
        $crate::PageIdx::try_new($idx).expect("page index out of range")
    };
}

impl PageIdx {
    pub const FIRST: Self = pageidx!(1);
    pub const LAST: Self = pageidx!(0xFFFF_FFFF);

    /// Create a new `PageIndex` from a u32. Returns None if the `PageIndex` is 0.
    #[inline]
    pub const fn try_new(n: u32) -> Option<Self> {
        match NonZero::new(n) {
            Some(n) => Some(Self(n)),
            None => None,
        }
    }

    /// Create a new `PageIndex` from a u32 without checking if it is 0.
    ///
    /// # Safety
    /// The provided u32 must not be 0.
    #[inline]
    pub const unsafe fn new_unchecked(n: u32) -> Self {
        Self(NonZero::new_unchecked(n))
    }

    #[inline]
    pub const fn to_u32(self) -> u32 {
        self.0.get()
    }

    #[inline]
    pub const fn saturating_add(self, n: u32) -> Self {
        Self(self.0.saturating_add(n))
    }

    #[inline]
    pub const fn saturating_next(self) -> Self {
        self.saturating_add(1)
    }

    #[inline]
    pub const fn saturating_sub(self, n: u32) -> Self {
        let prev = self.0.get().saturating_sub(n);
        match NonZero::new(prev) {
            Some(n) => Self(n),
            None => Self::FIRST,
        }
    }

    #[inline]
    pub const fn saturating_prev(self) -> Self {
        self.saturating_sub(1)
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

pub struct PageIdxIter {
    /// The inclusive start `pageidx`
    start: PageIdx,
    /// The exclusive end `pageidx`
    end: PageIdx,
}

impl PageIdxIter {
    pub const fn new(start: PageIdx, end: PageIdx) -> Self {
        Self { start, end }
    }
}

impl Iterator for PageIdxIter {
    type Item = PageIdx;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }

        let next = self.start;
        self.start = self.start.saturating_next();
        Some(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = if self.start < self.end {
            self.end.to_u32() - self.start.to_u32()
        } else {
            0
        };
        (len as usize, Some(len as usize))
    }
}

impl ExactSizeIterator for PageIdxIter {}
impl FusedIterator for PageIdxIter {}

impl DoubleEndedIterator for PageIdxIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }

        let prev = self.end.saturating_prev();
        self.end = prev;
        Some(prev)
    }
}

pub trait PageIdxRangeExt {
    fn iter(self) -> PageIdxIter;
}

impl PageIdxRangeExt for std::ops::Range<PageIdx> {
    fn iter(self) -> PageIdxIter {
        PageIdxIter::new(self.start, self.end)
    }
}

impl PageIdxRangeExt for std::ops::RangeInclusive<PageIdx> {
    fn iter(self) -> PageIdxIter {
        PageIdxIter::new(*self.start(), self.end().saturating_next())
    }
}

#[cfg(test)]
mod tests {
    use crate::{page_idx::PageIdxRangeExt, pageidx, PageCount, PageIdx};

    #[test]
    fn test_page_idx_iter() {
        let count = PageCount::new(3);
        let mut iter = count.iter();
        assert_eq!(iter.next(), Some(pageidx!(1)));
        assert_eq!(iter.next(), Some(pageidx!(2)));
        assert_eq!(iter.next(), Some(pageidx!(3)));
        assert_eq!(iter.next(), None);

        let count = PageCount::ZERO;
        let mut iter = count.iter();
        assert_eq!(iter.next(), None);

        let count = PageCount::new(1);
        let mut iter = count.iter();
        assert_eq!(iter.next(), Some(pageidx!(1)));
        assert_eq!(iter.next(), None);

        let custom = pageidx!(5)..pageidx!(10);
        for (i, idx) in custom.iter().enumerate() {
            assert_eq!(idx, PageIdx::new(i as u32 + 5));
        }

        let custom = pageidx!(5)..=pageidx!(10);
        for (i, idx) in custom.iter().enumerate() {
            assert_eq!(idx, PageIdx::new(i as u32 + 5));
        }
    }
}
