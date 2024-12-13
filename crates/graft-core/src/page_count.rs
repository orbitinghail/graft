use std::{
    fmt::Display,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{page_offset::PageOffset, page_range::PageRange};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    FromBytes,
    KnownLayout,
    IntoBytes,
    Immutable,
)]
#[repr(transparent)]
/// The number of pages in a volume.
pub struct PageCount(u32);

impl PageCount {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1);
    pub const MAX: Self = Self(u32::MAX);

    #[inline]
    pub const fn new(page_count: u32) -> Self {
        Self(page_count)
    }

    #[inline]
    pub fn offsets(&self) -> PageRange {
        PageRange::new(0, self.0)
    }

    #[inline]
    pub fn last_offset(&self) -> Option<PageOffset> {
        self.0.checked_sub(1).map(PageOffset::new)
    }

    #[inline]
    pub fn incr(&mut self) {
        self.0 = self.0.checked_add(1).expect("page count overflow");
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
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
        Self(count)
    }
}

impl From<PageCount> for u32 {
    #[inline]
    fn from(count: PageCount) -> u32 {
        count.0
    }
}

impl From<&PageCount> for u32 {
    #[inline]
    fn from(count: &PageCount) -> u32 {
        count.0
    }
}

impl<'a> From<&'a u32> for &'a PageCount {
    #[inline]
    fn from(value: &'a u32) -> Self {
        zerocopy::transmute_ref!(value)
    }
}

impl PartialEq<u32> for PageCount {
    #[inline]
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
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

impl<E: zerocopy::ByteOrder> From<zerocopy::U32<E>> for PageCount {
    #[inline]
    fn from(value: zerocopy::U32<E>) -> Self {
        Self(value.get())
    }
}

impl Add for PageCount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.checked_add(rhs.0).expect("page count overflow"))
    }
}

impl AddAssign for PageCount {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.checked_add(rhs.0).expect("page count overflow");
    }
}

impl Sub for PageCount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl SubAssign for PageCount {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_sub(rhs.0);
    }
}
