use std::{
    fmt::Display,
    num::ParseIntError,
    ops::{Bound, RangeBounds, RangeInclusive},
};

use serde::{Deserialize, Serialize};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
pub struct LSN(u64);

impl LSN {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);
    pub const ALL: RangeInclusive<Self> = Self::ZERO..=Self::MAX;

    #[inline]
    pub fn new(lsn: u64) -> Self {
        Self(lsn)
    }

    #[inline]
    pub fn next(&self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    #[inline]
    pub fn saturating_next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    #[inline]
    pub fn prev(&self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    #[inline]
    pub fn saturating_prev(&self) -> Self {
        Self(self.0.saturating_sub(1))
    }

    /// Returns the difference between this LSN and another LSN.
    /// If the other LSN is greater than this LSN, None is returned.
    #[inline]
    pub fn since(&self, other: &Self) -> Option<u64> {
        self.0.checked_sub(other.0)
    }

    /// Formats the LSN as a fixed-width hexadecimal string.
    /// The string will be 16 characters long, with leading zeros.
    pub fn format_fixed_hex(&self) -> String {
        format!("{:0>16x}", self.0)
    }

    /// Parses an LSN from a hexadecimal string.
    pub fn from_hex(s: &str) -> Result<Self, ParseIntError> {
        let v = u64::from_str_radix(s, 16)?;
        Ok(Self(v))
    }
}

impl Display for LSN {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> From<&'a u64> for &'a LSN {
    #[inline]
    fn from(value: &'a u64) -> Self {
        zerocopy::transmute_ref!(value)
    }
}

impl From<&LSN> for u64 {
    #[inline]
    fn from(value: &LSN) -> Self {
        value.0
    }
}

impl From<u64> for LSN {
    #[inline]
    fn from(lsn: u64) -> Self {
        Self(lsn)
    }
}

impl From<LSN> for u64 {
    #[inline]
    fn from(lsn: LSN) -> Self {
        lsn.0
    }
}

impl PartialEq<u64> for LSN {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for LSN {
    #[inline]
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<LSN> for zerocopy::U64<E> {
    #[inline]
    fn from(val: LSN) -> Self {
        zerocopy::U64::new(val.0)
    }
}

impl<E: zerocopy::ByteOrder> From<zerocopy::U64<E>> for LSN {
    #[inline]
    fn from(val: zerocopy::U64<E>) -> Self {
        Self(val.get())
    }
}

pub trait LSNRangeExt {
    fn try_len(&self) -> Option<usize>;
    fn try_start(&self) -> Option<LSN>;
    fn try_start_exclusive(&self) -> Option<LSN>;
    fn try_end(&self) -> Option<LSN>;
    fn try_end_exclusive(&self) -> Option<LSN>;
    fn iter(&self) -> LSNRangeIter;
}

impl<T: RangeBounds<LSN>> LSNRangeExt for T {
    fn try_len(&self) -> Option<usize> {
        let start = self.try_start()?;
        let end = self.try_end_exclusive()?;
        end.since(&start).map(|len| len as usize)
    }

    fn try_start(&self) -> Option<LSN> {
        match self.start_bound() {
            Bound::Included(lsn) => Some(*lsn),
            Bound::Excluded(lsn) => Some(lsn.saturating_next()),
            Bound::Unbounded => None,
        }
    }

    fn try_start_exclusive(&self) -> Option<LSN> {
        match self.start_bound() {
            Bound::Included(lsn) => lsn.prev(),
            Bound::Excluded(lsn) => Some(*lsn),
            Bound::Unbounded => None,
        }
    }

    fn try_end(&self) -> Option<LSN> {
        match self.end_bound() {
            Bound::Included(lsn) => Some(*lsn),
            Bound::Excluded(lsn) => Some(lsn.saturating_prev()),
            Bound::Unbounded => None,
        }
    }

    fn try_end_exclusive(&self) -> Option<LSN> {
        match self.end_bound() {
            Bound::Included(lsn) => lsn.next(),
            Bound::Excluded(lsn) => Some(*lsn),
            Bound::Unbounded => None,
        }
    }

    fn iter(&self) -> LSNRangeIter {
        let start = self.try_start().unwrap_or(LSN::ZERO).into();
        let end = self.try_end().unwrap_or(LSN::MAX).into();
        LSNRangeIter { range: start..=end }
    }
}

#[derive(Debug, Clone)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct LSNRangeIter {
    range: RangeInclusive<u64>,
}

impl Iterator for LSNRangeIter {
    type Item = LSN;

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(LSN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_next() {
        let lsn = LSN::new(0);
        assert_eq!(lsn.saturating_next(), 1);
    }
}
