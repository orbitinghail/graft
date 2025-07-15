use std::{
    fmt::Display,
    num::{NonZero, ParseIntError},
    ops::{Bound, RangeBounds, RangeInclusive},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, ValidityError};

use crate::{cbe::CBE64, derive_newtype_proxy};

#[derive(Debug, Error)]
#[error("LSN must be non-zero")]
pub struct InvalidLSN;

impl<S, D: TryFromBytes> From<ValidityError<S, D>> for InvalidLSN {
    fn from(_: ValidityError<S, D>) -> Self {
        InvalidLSN
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    TryFromBytes,
    KnownLayout,
    IntoBytes,
    Immutable,
    ByteHash,
)]
#[repr(transparent)]
pub struct LSN(NonZero<u64>);

// We use NonZero to enable Option optimizations
static_assertions::assert_eq_size!(Option<LSN>, LSN);

impl LSN {
    pub const FIRST: Self = unsafe { Self::new_unchecked(1) };
    pub const LAST: Self = unsafe { Self::new_unchecked(u64::MAX) };
    pub const ALL: RangeInclusive<Self> = Self::FIRST..=Self::LAST;

    /// Creates a new LSN from a u64 value.
    /// Panics if value is 0.
    #[inline]
    pub fn new(lsn: u64) -> Self {
        Self(NonZero::new(lsn).expect("LSN must be non-zero"))
    }

    /// Creates a new LSN from a non-zero u64 value.
    ///
    /// SAFETY:
    /// Undefined behavior if value is zero.
    #[inline]
    const unsafe fn new_unchecked(lsn: u64) -> Self {
        unsafe { Self(NonZero::new_unchecked(lsn)) }
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
        let n = self.0.get().saturating_sub(1);
        if n == 0 {
            None
        } else {
            // SAFETY: n is non-zero
            unsafe { Some(Self(NonZero::new_unchecked(n))) }
        }
    }

    #[inline]
    pub fn saturating_prev(&self) -> Self {
        let n = self.0.get().saturating_sub(1);
        if n == 0 {
            Self::FIRST
        } else {
            // SAFETY: n is non-zero
            unsafe { Self(NonZero::new_unchecked(n)) }
        }
    }

    /// Returns the difference between this LSN and another LSN.
    /// If the other LSN is greater than this LSN, None is returned.
    #[inline]
    pub fn since(&self, other: &Self) -> Option<u64> {
        let me = self.0.get();
        let other = other.0.get();
        if me >= other { Some(me - other) } else { None }
    }

    /// Formats the LSN as a fixed-width hexadecimal string.
    /// The string will be 16 characters long, with leading zeros.
    pub fn format_fixed_hex(&self) -> String {
        format!("{:0>16x}", self.0)
    }

    /// Parses an LSN from a hexadecimal string.
    pub fn from_hex(s: &str) -> Result<Self, ParseIntError> {
        Ok(Self::new(u64::from_str_radix(s, 16)?))
    }

    #[inline]
    pub const fn to_u64(self) -> u64 {
        self.0.get()
    }
}

impl Display for LSN {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Default for LSN {
    #[inline]
    fn default() -> Self {
        Self::FIRST
    }
}

impl<'a> TryFrom<&'a u64> for &'a LSN {
    type Error = InvalidLSN;

    #[inline]
    fn try_from(value: &'a u64) -> Result<Self, Self::Error> {
        Ok(zerocopy::try_transmute_ref!(value)?)
    }
}

impl From<&LSN> for u64 {
    #[inline]
    fn from(value: &LSN) -> Self {
        value.0.get()
    }
}

impl From<LSN> for u64 {
    #[inline]
    fn from(lsn: LSN) -> Self {
        lsn.0.get()
    }
}

impl TryFrom<u64> for LSN {
    type Error = InvalidLSN;

    #[inline]
    fn try_from(lsn: u64) -> Result<Self, Self::Error> {
        match NonZero::new(lsn) {
            Some(lsn) => Ok(Self(lsn)),
            None => Err(InvalidLSN),
        }
    }
}

impl PartialEq<u64> for LSN {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0.get() == *other
    }
}

impl PartialOrd<u64> for LSN {
    #[inline]
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.get().partial_cmp(other)
    }
}

impl<E: zerocopy::ByteOrder> From<LSN> for zerocopy::U64<E> {
    #[inline]
    fn from(val: LSN) -> Self {
        zerocopy::U64::new(val.0.get())
    }
}

impl<E: zerocopy::ByteOrder> TryFrom<zerocopy::U64<E>> for LSN {
    type Error = InvalidLSN;

    #[inline]
    fn try_from(value: zerocopy::U64<E>) -> Result<Self, Self::Error> {
        match NonZero::new(value.get()) {
            Some(lsn) => Ok(Self(lsn)),
            None => Err(InvalidLSN),
        }
    }
}

impl From<LSN> for CBE64 {
    #[inline]
    fn from(lsn: LSN) -> Self {
        CBE64::new(lsn.0.get())
    }
}

impl TryFrom<CBE64> for LSN {
    type Error = InvalidLSN;

    #[inline]
    fn try_from(cbe: CBE64) -> Result<Self, Self::Error> {
        cbe.get().try_into()
    }
}

derive_newtype_proxy!(
    newtype (LSN)
    with empty value (LSN::FIRST)
    with proxy type (u64) and encoding (::bilrost::encoding::Varint)
    with sample value (LSN::new(12345))
    into_proxy (&self) {
        self.0.get()
    }
    from_proxy (&mut self, proxy) {
        *self = Self::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
        Ok(())
    }
);

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
        let start = self.try_start().unwrap_or(LSN::FIRST).into();
        let end = self.try_end().unwrap_or(LSN::LAST).into();
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
        self.range.next().map(|n| {
            // SAFETY: we know n is non-zero because next is monotonically
            // increasing
            unsafe { LSN::new_unchecked(n) }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[graft_test::test]
    fn test_lsn_next() {
        let lsn = LSN::FIRST;
        assert_eq!(lsn.saturating_next(), 2);
    }
}
