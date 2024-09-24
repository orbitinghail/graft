use std::{
    fmt::{self, Debug, Display},
    ops::{Add, Div, Mul, Rem, Shl, Shr, Sub},
};

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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    const fn as_f64(&self) -> f64 {
        self.0 as f64
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

    #[test]
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
}
