use core::ops::RangeInclusive;
use std::{
    fmt::Display,
    num::{NonZero, ParseIntError},
    ops::{Bound, RangeBounds},
};

use range_set_blaze::{CheckSortedDisjoint, RangeSetBlaze};
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

/// Creates a `LSN` value from a literal or expression. Expressions are
/// evaluated at compile time.
///
/// This macro provides a convenient way to construct `LSN` values with compile-time
/// validation. For literal values, it ensures they don't exceed `u64::MAX`.
#[macro_export]
macro_rules! lsn {
    ($v:expr) => {{
        // force $v to be u64
        const V: u64 = $v;
        static_assertions::const_assert!(V > 0 && V <= u64::MAX);
        // SAFETY: V is checked at compile time to be > 0
        unsafe { $crate::lsn::LSN::new_unchecked(V) }
    }};
}

/// Creates a `LSN` run at compile time from a literal RangeInclusive
///
/// Example:
///
/// ```rust
/// lsn_run!(5..=10)
/// ```
///
#[macro_export]
macro_rules! lsn_run {
    ($left:literal ..= $right:literal) => {{ lsn!($left)..=lsn!($right) }};
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
    /// The smallest LSN
    /// SAFETY: provably safe
    pub const FIRST: Self = unsafe { Self::new_unchecked(1) };

    /// The largest LSN
    /// SAFETY: provably safe
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
    pub const unsafe fn new_unchecked(lsn: u64) -> Self {
        // SAFETY: Undefined behavior if value is zero.
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

    /// Wrapping addition for LSN values.
    /// Since LSN values are in range [1, u64::MAX], wrapping occurs at u64::MAX.
    /// For example: LSN(u64::MAX).wrapping_add(1) == LSN(1)
    #[inline]
    fn wrapping_add(self, rhs: u64) -> Self {
        // Use u128 for intermediate calculation to handle overflow correctly
        // We need to compute: ((self + rhs - 1) % u64::MAX) + 1
        let sum = (self.0.get() as u128) + (rhs as u128);
        let result = (((sum - 1) % (u64::MAX as u128)) + 1) as u64;
        // SAFETY: result is in range [1, u64::MAX], so it's always non-zero
        unsafe { Self::new_unchecked(result) }
    }

    /// Wrapping subtraction for LSN values.
    /// Since LSN values are in range [1, u64::MAX], wrapping occurs at the boundaries.
    /// For example: LSN(1).wrapping_sub(1) == LSN(u64::MAX)
    #[inline]
    fn wrapping_sub(self, rhs: u64) -> Self {
        // Use i128 for intermediate calculation to handle underflow correctly
        // We need to compute: ((self - rhs - 1) rem_euclid u64::MAX) + 1
        let diff = (self.0.get() as i128) - (rhs as i128);
        let modulus = u64::MAX as i128;
        let result = ((diff - 1).rem_euclid(modulus) + 1) as u64;
        // SAFETY: result is in range [1, u64::MAX], so it's always non-zero
        unsafe { Self::new_unchecked(result) }
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

impl range_set_blaze::Integer for LSN {
    type SafeLen = u64;

    fn checked_add_one(self) -> Option<Self> {
        self.next()
    }

    fn add_one(self) -> Self {
        LSN(self.0.checked_add(1).unwrap())
    }

    fn sub_one(self) -> Self {
        let n = self.0.get().saturating_sub(1);
        if n == 0 {
            panic!("LSN underflow")
        } else {
            // SAFETY: n is non-zero
            unsafe { Self(NonZero::new_unchecked(n)) }
        }
    }

    fn assign_sub_one(&mut self) {
        *self = self.sub_one();
    }

    fn range_next(range: &mut RangeInclusive<Self>) -> Option<Self> {
        use core::cmp::Ordering;
        let (start, end) = (*range.start(), *range.end());
        match start.cmp(&end) {
            Ordering::Less => {
                *range = start.saturating_next()..=end;
                Some(start)
            }
            Ordering::Equal => {
                *range = LSN::LAST..=LSN::FIRST;
                Some(start)
            }
            Ordering::Greater => None,
        }
    }

    fn range_next_back(range: &mut RangeInclusive<Self>) -> Option<Self> {
        use core::cmp::Ordering;
        let (end, start) = (*range.end(), *range.start());
        match end.cmp(&start) {
            Ordering::Greater => {
                *range = start..=end.saturating_prev();
                Some(end)
            }
            Ordering::Equal => {
                *range = LSN::LAST..=LSN::FIRST;
                Some(end)
            }
            Ordering::Less => None,
        }
    }

    fn min_value() -> Self {
        LSN::FIRST
    }

    fn max_value() -> Self {
        LSN::LAST
    }

    fn safe_len(range: &RangeInclusive<Self>) -> Self::SafeLen {
        range.end().since(range.start()).unwrap_or(0) + 1
    }

    fn f64_to_safe_len_lossy(f: f64) -> Self::SafeLen {
        f as Self::SafeLen
    }

    fn safe_len_to_f64_lossy(len: Self::SafeLen) -> f64 {
        len as f64
    }

    fn inclusive_end_from_start(self, b: Self::SafeLen) -> Self {
        debug_assert!(b > 0 && b <= u64::MAX - 1, "b must be in range 1..=max_len");
        // If b is in range, two’s-complement wrap-around yields the correct inclusive end even if the add overflows
        self.wrapping_add(b - 1)
    }

    fn start_from_inclusive_end(self, b: Self::SafeLen) -> Self {
        debug_assert!(b > 0 && b <= u64::MAX - 1, "b must be in range 1..=max_len");
        // If b is in range, two’s-complement wrap-around yields the correct inclusive end even if the add overflows
        self.wrapping_sub(b - 1)
    }
}

derive_newtype_proxy!(
    newtype (LSN)
    with empty value (LSN::FIRST)
    with proxy type (u64) and encoding (::bilrost::encoding::Varint)
    with sample value (lsn!(12345))
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
    /// Returns the length of this LSN range.
    /// Returns None if start > end
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

/// A set of LSNs, optimized to store LSNs in runs.
pub type LSNSet = RangeSetBlaze<LSN>;

pub trait LSNSetExt {
    fn from_range(lsns: RangeInclusive<LSN>) -> RangeSetBlaze<LSN> {
        // TODO: replace this with RangeSetBlaze::from once this lands
        // https://github.com/CarlKCarlK/range-set-blaze/pull/21
        RangeSetBlaze::from_sorted_disjoint(CheckSortedDisjoint::new([lsns]))
    }
}
impl LSNSetExt for LSNSet {}

#[cfg(test)]
mod tests {
    use crate::lsn;

    use super::*;

    #[test]
    fn test_lsn_next() {
        let lsn = LSN::FIRST;
        assert_eq!(lsn.saturating_next(), 2);
    }

    #[test]
    fn test_lsn_wrapping_add() {
        // Test wrapping at the boundary
        assert_eq!(LSN::LAST.wrapping_add(1), LSN::FIRST);
        assert_eq!(LSN::LAST.wrapping_add(2), lsn!(2));

        // Test normal addition
        assert_eq!(lsn!(5).wrapping_add(3), lsn!(8));

        // Test large addition that wraps: (u64::MAX - 5) + 10 = u64::MAX + 5 wraps to 5
        assert_eq!(lsn!(u64::MAX - 5).wrapping_add(10), lsn!(5));
    }

    #[test]
    fn test_lsn_wrapping_sub() {
        // Test wrapping at the boundary
        assert_eq!(LSN::FIRST.wrapping_sub(1), LSN::LAST);
        assert_eq!(lsn!(2).wrapping_sub(2), LSN::LAST);

        // Test normal subtraction
        assert_eq!(lsn!(8).wrapping_sub(3), lsn!(5));

        // Test large subtraction that wraps: 5 - 10 wraps to u64::MAX - 5
        assert_eq!(lsn!(5).wrapping_sub(10), lsn!(u64::MAX - 5));
    }

    #[test]
    fn test_lsn_set() {
        let mut set = LSNSet::new();
        for i in 1..=10 {
            assert!(set.insert(LSN::new(i)));
        }
        for i in 1024..=2048 {
            assert!(set.insert(LSN::new(i)));
        }
        assert!(set.ranges_insert(lsn!(128)..=lsn!(256)));

        assert_eq!(
            set.ranges().collect::<Vec<_>>(),
            vec![
                lsn!(1)..=lsn!(10),
                lsn!(128)..=lsn!(256),
                lsn!(1024)..=lsn!(2048)
            ]
        );
    }
}
