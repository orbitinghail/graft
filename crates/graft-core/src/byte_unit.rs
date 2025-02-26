use std::{
    fmt::{self, Debug, Display},
    ops::{Add, Div, Mul, Range, Rem, Shl, Shr, Sub},
    str::FromStr,
};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

struct NamedByteUnit {
    value: ByteUnit,
    suffix: &'static str,
}

const KB: NamedByteUnit = NamedByteUnit { value: ByteUnit::KB, suffix: "KB" };
const MB: NamedByteUnit = NamedByteUnit { value: ByteUnit::MB, suffix: "MB" };
const GB: NamedByteUnit = NamedByteUnit { value: ByteUnit::GB, suffix: "GB" };
const TB: NamedByteUnit = NamedByteUnit { value: ByteUnit::TB, suffix: "TB" };
const PB: NamedByteUnit = NamedByteUnit { value: ByteUnit::PB, suffix: "PB" };
const EB: NamedByteUnit = NamedByteUnit { value: ByteUnit::EB, suffix: "EB" };

#[derive(Clone, Copy, Eq, Ord)]
pub struct ByteUnit(u64);

impl ByteUnit {
    pub const ZERO: ByteUnit = ByteUnit(0);
    pub const MAX: ByteUnit = ByteUnit(u64::MAX);

    pub const KB: ByteUnit = ByteUnit(1 << 10);
    pub const MB: ByteUnit = ByteUnit(1 << 20);
    pub const GB: ByteUnit = ByteUnit(1 << 30);
    pub const TB: ByteUnit = ByteUnit(1 << 40);
    pub const PB: ByteUnit = ByteUnit(1 << 50);
    pub const EB: ByteUnit = ByteUnit(1 << 60);

    pub const fn new(bytes: u64) -> ByteUnit {
        ByteUnit(bytes)
    }

    pub const fn size_of<T>() -> ByteUnit {
        ByteUnit(std::mem::size_of::<T>() as u64)
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    pub const fn as_u32(&self) -> u32 {
        self.0 as u32
    }

    pub const fn as_u16(&self) -> u16 {
        self.0 as u16
    }

    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }

    const fn as_f64(&self) -> f64 {
        self.0 as f64
    }

    pub const fn is_power_of_two(&self) -> bool {
        self.0.is_power_of_two()
    }

    pub const fn from_kb(kb: u64) -> ByteUnit {
        ByteUnit(kb.saturating_mul(KB.value.0))
    }

    pub const fn from_mb(mb: u64) -> ByteUnit {
        ByteUnit(mb.saturating_mul(MB.value.0))
    }

    pub const fn from_gb(gb: u64) -> ByteUnit {
        ByteUnit(gb.saturating_mul(GB.value.0))
    }

    pub const fn from_tb(tb: u64) -> ByteUnit {
        ByteUnit(tb.saturating_mul(TB.value.0))
    }

    pub const fn from_pb(pb: u64) -> ByteUnit {
        ByteUnit(pb.saturating_mul(PB.value.0))
    }

    pub const fn from_eb(eb: u64) -> ByteUnit {
        ByteUnit(eb.saturating_mul(EB.value.0))
    }

    /// Returns the absolute difference between two `ByteUnit` values.
    pub const fn diff(self, other: ByteUnit) -> ByteUnit {
        if self.0 > other.0 {
            ByteUnit(self.0 - other.0)
        } else {
            ByteUnit(other.0 - self.0)
        }
    }

    /// Returns a range representing the byte range from `self` to `end`.
    pub const fn range(self, end: ByteUnit) -> Range<usize> {
        (self.0 as usize)..(end.0 as usize)
    }
}

impl Display for ByteUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = *self;
        for unit in &[EB, PB, TB, GB, MB, KB] {
            if value >= unit.value {
                let whole = value / unit.value;
                let rem = value % unit.value;
                let frac = rem.as_f64() / unit.value.as_f64();
                if frac < 0.005 {
                    write!(f, "{} {}", whole.0, unit.suffix)?;
                } else if frac >= 0.95 {
                    write!(f, "{} {}", whole.0 + 1, unit.suffix)?;
                } else {
                    write!(f, "{}.{:02.0} {}", whole.0, frac * 100.0, unit.suffix)?;
                }
                return Ok(());
            }
        }

        // If we reach this point, the value is smaller than 1KB
        write!(f, "{} B", value.0)
    }
}

impl Debug for ByteUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
pub enum ByteUnitParseError {
    #[error("Invalid format: got {0}, expected <number> [<unit>]")]
    InvalidFormat(String),

    #[error("Invalid number: {0}")]
    InvalidNumber(String),

    #[error("Unknown unit: {0}")]
    UnknownUnit(String),
}

impl FromStr for ByteUnit {
    type Err = ByteUnitParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let mut parts = s.split_ascii_whitespace();
        let number = parts
            .next()
            .ok_or(ByteUnitParseError::InvalidFormat(s.to_string()))?;

        let unit = parts.next().unwrap_or("B").to_uppercase();

        let value = number
            .parse::<u64>()
            .map_err(|_| ByteUnitParseError::InvalidNumber(number.into()))?;

        Ok(match unit.as_str() {
            "B" => ByteUnit(value),
            "KB" => ByteUnit::from_kb(value),
            "MB" => ByteUnit::from_mb(value),
            "GB" => ByteUnit::from_gb(value),
            "TB" => ByteUnit::from_tb(value),
            "PB" => ByteUnit::from_pb(value),
            "EB" => ByteUnit::from_eb(value),
            _ => return Err(ByteUnitParseError::UnknownUnit(unit)),
        })
    }
}

impl<'de> Deserialize<'de> for ByteUnit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            FromStr::from_str(&s).map_err(serde::de::Error::custom)
        } else {
            let bytes = u64::deserialize(deserializer)?;
            Ok(ByteUnit(bytes))
        }
    }
}

impl Serialize for ByteUnit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.to_string().serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<T: Into<ByteUnit> + Copy> PartialEq<T> for ByteUnit {
    fn eq(&self, other: &T) -> bool {
        self.0 == (*other).into().0
    }
}

impl<T: Into<ByteUnit> + Copy> PartialOrd<T> for ByteUnit {
    fn partial_cmp(&self, other: &T) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&(*other).into().0)
    }
}

impl<T: Into<ByteUnit>> Mul<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn mul(self, rhs: T) -> Self::Output {
        ByteUnit(self.0.saturating_mul(rhs.into().0))
    }
}

impl<T: Into<ByteUnit>> Add<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn add(self, rhs: T) -> Self::Output {
        ByteUnit(self.0.saturating_add(rhs.into().0))
    }
}
impl<T: Into<ByteUnit>> Sub<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn sub(self, rhs: T) -> Self::Output {
        ByteUnit(self.0.saturating_sub(rhs.into().0))
    }
}

impl<T: Into<ByteUnit>> Div<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn div(self, rhs: T) -> Self::Output {
        ByteUnit(self.0.saturating_div(rhs.into().0))
    }
}

impl<T: Into<ByteUnit>> Rem<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn rem(self, rhs: T) -> Self::Output {
        let value = rhs.into().0;
        match value {
            0 => ByteUnit(0),
            _ => ByteUnit(self.0 % value),
        }
    }
}

impl<T: Into<ByteUnit>> Shl<T> for ByteUnit {
    type Output = Self;

    #[inline(always)]
    fn shl(self, rhs: T) -> Self::Output {
        ByteUnit(self.0 << rhs.into().0)
    }
}

impl<T: Into<ByteUnit>> Shr<T> for ByteUnit {
    type Output = ByteUnit;

    #[inline(always)]
    fn shr(self, rhs: T) -> Self::Output {
        ByteUnit(self.0 >> rhs.into().0)
    }
}

macro_rules! impl_arith_op {
    ($T:ident, $Trait:ident, $func:ident, $op:tt) => (
        impl $Trait<ByteUnit> for $T {
            type Output = ByteUnit;

            #[inline(always)]
            fn $func(self, rhs: ByteUnit) -> Self::Output {
                ByteUnit::from(self) $op rhs
            }
        }
    )
}

macro_rules! impl_primitive {
    ($T:ident) => {
        impl From<$T> for ByteUnit {
            fn from(bytes: $T) -> ByteUnit {
                ByteUnit(bytes as u64)
            }
        }

        impl PartialEq<ByteUnit> for $T {
            fn eq(&self, other: &ByteUnit) -> bool {
                *self as u64 == other.0
            }
        }

        impl PartialOrd<ByteUnit> for $T {
            fn partial_cmp(&self, other: &ByteUnit) -> Option<std::cmp::Ordering> {
                (*self as u64).partial_cmp(&other.0)
            }
        }

        impl_arith_op!($T, Mul, mul, *);
        impl_arith_op!($T, Div, div, /);
        impl_arith_op!($T, Rem, rem, %);
        impl_arith_op!($T, Add, add, +);
        impl_arith_op!($T, Sub, sub, -);
        impl_arith_op!($T, Shl, shl, <<);
        impl_arith_op!($T, Shr, shr, >>);
    };
}

impl_primitive!(u8);
impl_primitive!(u16);
impl_primitive!(u32);
impl_primitive!(u64);
impl_primitive!(usize);

impl_primitive!(i8);
impl_primitive!(i16);
impl_primitive!(i32);
impl_primitive!(i64);
impl_primitive!(isize);

#[cfg(test)]
mod tests {
    use super::*;

    #[graft_test::test]
    fn test_sanity() {
        assert_eq!(0, ByteUnit::ZERO);
        assert_eq!(ByteUnit::ZERO, 0);
        assert_eq!(ByteUnit::ZERO, ByteUnit::ZERO);
        assert!(0 < ByteUnit::KB);
        assert!(ByteUnit::KB > 0);
    }

    #[graft_test::test]
    fn test_display() {
        assert_eq!(format!("{}", ByteUnit::ZERO), "0 B");
        assert_eq!(format!("{}", ByteUnit::MAX), "16 EB");

        for unit in &[KB, MB, GB, TB, PB, EB] {
            assert_eq!(format!("{}", unit.value), format!("1 {}", unit.suffix));
            assert_eq!(format!("{}", 7 * unit.value), format!("7 {}", unit.suffix));
        }

        assert_eq!(
            format!("{}", (7 * ByteUnit::MB) + (132 * ByteUnit::KB)),
            "7.13 MB"
        );
        assert_eq!(
            format!("{}", (7 * ByteUnit::MB) + (512 * ByteUnit::KB)),
            "7.50 MB"
        );
        assert_eq!(
            format!("{}", (7 * ByteUnit::MB) + ((1024 - 1) * ByteUnit::KB)),
            "8 MB"
        );
    }

    #[graft_test::test]
    fn test_const() {
        const X: ByteUnit = ByteUnit::from_kb(4);
        let arr: [u8; X.as_usize()] = [0; X.as_usize()];
        assert_eq!(arr.len(), 4 * 1024);
    }

    #[graft_test::test]
    fn test_parse() {
        let cases = [
            ByteUnit::new(0),
            ByteUnit::from_kb(10),
            ByteUnit::from_mb(10),
            ByteUnit::from_gb(10),
            ByteUnit::from_tb(10),
            ByteUnit::from_pb(10),
            ByteUnit::from_eb(10),
        ];

        // for each case, check that it roundtrips through display then parse
        for &unit in &cases {
            let s = format!("{unit}");
            let parsed = s.parse::<ByteUnit>().unwrap();
            println!("parsed `{s}` into {parsed}");
            assert_eq!(unit, parsed);
        }

        // some valid cases that exercise the parser's flexibility
        let nonstandard_cases = [
            ("0", "no unit", ByteUnit::new(0)),
            ("    0", "no unit prefix whitespace", ByteUnit::new(0)),
            ("0 ", "no unit trailing whitespace", ByteUnit::new(0)),
            (" 0 ", "no unit both whitespace", ByteUnit::new(0)),
            ("0  kb", "lowercase", ByteUnit::new(0)),
            ("5  kb", "lowercase kb", ByteUnit::from_kb(5)),
            (" 5 \t kb  ", "lots of whitespace", ByteUnit::from_kb(5)),
        ];

        // check that each case parses
        for &(s, desc, expected) in &nonstandard_cases {
            let parsed = s.parse::<ByteUnit>().unwrap();
            println!("parsed `{s}` into {parsed}");
            assert_eq!(parsed, expected, "{desc}");
        }

        let invalid_cases = [
            ("", "empty string", "Invalid format"),
            ("    ", "only whitespace", "Invalid format"),
            ("12.2", "decimal number", "Invalid number"),
            ("12.2 kb", "decimal number", "Invalid number"),
            ("123 xb", "unknown unit", "Unknown unit"),
        ];

        // check that each case fails to parse, and the error contains the expected message
        for &(s, desc, expected) in &invalid_cases {
            let parsed = s.parse::<ByteUnit>();
            println!("parsed `{s}` into {parsed:?}");
            assert!(parsed.is_err(), "{}", desc);
            assert!(
                parsed.unwrap_err().to_string().contains(expected),
                "{}",
                desc
            );
        }
    }
}
