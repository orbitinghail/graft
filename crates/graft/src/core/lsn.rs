use core::ops::RangeInclusive;
use std::{
    fmt::Display,
    iter::FusedIterator,
    num::{NonZero, ParseIntError},
    ops::{Bound, RangeBounds},
};

use crate::core::cbe::CBE64;
use crate::derive_newtype_proxy;
use range_set_blaze::RangeSetBlaze;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, ValidityError};

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
        unsafe { $crate::core::lsn::LSN::new_unchecked(V) }
    }};
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
    /// # Safety
    /// Undefined behavior if value is zero.
    #[inline]
    pub const unsafe fn new_unchecked(lsn: u64) -> Self {
        // SAFETY: Undefined behavior if value is zero.
        unsafe { Self(NonZero::new_unchecked(lsn)) }
    }

    /// Returns the next LSN
    ///
    /// # Panics
    /// Panics if the LSN would overflow
    #[inline]
    pub fn next(&self) -> Self {
        Self(self.0.checked_add(1).expect("LSN overflow"))
    }

    #[inline]
    pub fn saturating_next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    #[inline]
    pub fn checked_add(&self, n: u64) -> Option<Self> {
        Some(Self(self.0.checked_add(n)?))
    }

    #[inline]
    pub fn checked_next(&self) -> Option<Self> {
        Some(Self(self.0.checked_add(1)?))
    }

    #[inline]
    pub fn checked_prev(&self) -> Option<Self> {
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
    pub fn since(&self, other: Self) -> Option<u64> {
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
    /// Since LSN values are in range [1, `u64::MAX`], wrapping occurs at `u64::MAX`.
    /// For example: `LSN(u64::MAX).wrapping_add(1)` == LSN(1)
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
    /// Since LSN values are in range [1, `u64::MAX`], wrapping occurs at the boundaries.
    /// For example: `LSN(1).wrapping_sub(1)` == `LSN(u64::MAX)`
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

    #[inline]
    fn checked_add_one(self) -> Option<Self> {
        self.checked_next()
    }

    #[inline]
    fn add_one(self) -> Self {
        self.next()
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
        match range.end().since(*range.start()) {
            None => 0,
            Some(delta) => delta + 1,
        }
    }

    fn f64_to_safe_len_lossy(f: f64) -> Self::SafeLen {
        f as Self::SafeLen
    }

    fn safe_len_to_f64_lossy(len: Self::SafeLen) -> f64 {
        len as f64
    }

    fn inclusive_end_from_start(self, b: Self::SafeLen) -> Self {
        debug_assert!(b > 0 && b < u64::MAX, "b must be in range 1..=max_len");
        // If b is in range, two’s-complement wrap-around yields the correct inclusive end even if the add overflows
        self.wrapping_add(b - 1)
    }

    fn start_from_inclusive_end(self, b: Self::SafeLen) -> Self {
        debug_assert!(b > 0 && b < u64::MAX, "b must be in range 1..=max_len");
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
    fn is_empty(&self) -> bool;
    fn len(&self) -> u64;
    fn as_inclusive(&self) -> RangeInclusive<LSN>;
    fn iter(&self) -> LSNRangeIter;
    fn to_string(&self) -> String;
}

fn as_inclusive_raw<T: RangeBounds<LSN>>(range: &T) -> (LSN, LSN) {
    const EMPTY_RANGE: (LSN, LSN) = (LSN::LAST, LSN::FIRST);
    let start = match range.start_bound() {
        Bound::Included(&start) => start,
        Bound::Excluded(&prev) => match prev.checked_next() {
            Some(start) => start,
            None => return EMPTY_RANGE,
        },
        Bound::Unbounded => LSN::FIRST,
    };
    let end = match range.end_bound() {
        Bound::Included(&end) => end,
        Bound::Excluded(&next) => match next.checked_prev() {
            Some(end) => end,
            None => return EMPTY_RANGE,
        },
        Bound::Unbounded => LSN::LAST,
    };
    (start, end)
}

impl<T: RangeBounds<LSN>> LSNRangeExt for T {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length of this LSN range.
    fn len(&self) -> u64 {
        let (start, end) = as_inclusive_raw(self);
        match end.since(start) {
            Some(diff) => diff + 1,
            None => 0,
        }
    }

    fn as_inclusive(&self) -> RangeInclusive<LSN> {
        let (start, end) = as_inclusive_raw(self);
        start..=end
    }

    fn iter(&self) -> LSNRangeIter {
        let (start, end) = as_inclusive_raw(self);
        LSNRangeIter { range: start.into()..=end.into() }
    }

    fn to_string(&self) -> String {
        let (start, end) = as_inclusive_raw(self);
        if end == LSN::LAST {
            format!("{start}..")
        } else if start == LSN::FIRST {
            format!("..={end}")
        } else {
            format!("{start}..={end}")
        }
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
            // SAFETY: LSNRangeExt::iter ensures that start is never == 0
            unsafe { LSN::new_unchecked(n) }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl DoubleEndedIterator for LSNRangeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map(|n| {
            // SAFETY: LSNRangeExt::iter ensures that start is never == 0
            unsafe { LSN::new_unchecked(n) }
        })
    }
}

impl ExactSizeIterator for LSNRangeIter {}
impl FusedIterator for LSNRangeIter {}

/// A set of LSNs, optimized to store LSNs in runs.
pub type LSNSet = RangeSetBlaze<LSN>;

#[cfg(test)]
mod tests {
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

        // inserting an empty range should not actually insert anything
        assert!(!set.ranges_insert(lsn!(700)..=lsn!(699)));

        assert_eq!(
            set.ranges().collect::<Vec<_>>(),
            vec![
                lsn!(1)..=lsn!(10),
                lsn!(128)..=lsn!(256),
                lsn!(1024)..=lsn!(2048)
            ]
        );
    }

    #[test]
    fn test_lsn_range_ext() {
        use Bound::*;

        #[derive(Debug)]
        struct Case {
            range: (Bound<LSN>, Bound<LSN>),
            len: u64,
            as_inclusive: RangeInclusive<LSN>,
        }

        let cases = [
            Case {
                range: (Unbounded, Unbounded),
                len: u64::MAX,
                as_inclusive: LSN::FIRST..=LSN::LAST,
            },
            Case {
                range: (Unbounded, Included(LSN::FIRST)),
                len: 1,
                as_inclusive: LSN::FIRST..=LSN::FIRST,
            },
            Case {
                range: (Unbounded, Excluded(LSN::FIRST)),
                len: 0,
                as_inclusive: LSN::LAST..=LSN::FIRST,
            },
            Case {
                range: (Included(LSN::LAST), Unbounded),
                len: 1,
                as_inclusive: LSN::LAST..=LSN::LAST,
            },
            Case {
                range: (Excluded(LSN::LAST), Unbounded),
                len: 0,
                as_inclusive: LSN::LAST..=LSN::FIRST,
            },
            Case {
                range: (Excluded(LSN::FIRST), Excluded(LSN::FIRST)),
                len: 0,
                as_inclusive: LSN::LAST..=LSN::FIRST,
            },
            Case {
                range: (Excluded(LSN::FIRST), Excluded(lsn!(2))),
                len: 0,
                as_inclusive: lsn!(2)..=LSN::FIRST,
            },
            Case {
                range: (Excluded(LSN::FIRST), Included(lsn!(2))),
                len: 1,
                as_inclusive: lsn!(2)..=lsn!(2),
            },
            Case {
                range: (Excluded(LSN::FIRST), Included(lsn!(3))),
                len: 2,
                as_inclusive: lsn!(2)..=lsn!(3),
            },
            Case {
                range: (Included(LSN::FIRST), Excluded(lsn!(2))),
                len: 1,
                as_inclusive: lsn!(1)..=lsn!(1),
            },
        ];

        for (i, case) in cases.into_iter().enumerate() {
            println!("Case {}: {:?}", i, case);
            assert_eq!(case.range.len(), case.len, "len");
            let is_empty = LSNRangeExt::is_empty(&case.range);
            assert_eq!(is_empty, case.len == 0, "is_empty");
            assert_eq!(case.range.as_inclusive(), case.as_inclusive, "as_inclusive");
            let mut iter = case.range.iter().peekable();
            if is_empty {
                assert_eq!(iter.next(), None, "iter is empty")
            } else {
                assert_eq!(iter.peek(), Some(case.as_inclusive.start()), "iter start");
                assert_eq!(iter.next_back(), Some(*case.as_inclusive.end()), "iter end");
            }
        }
    }
}
