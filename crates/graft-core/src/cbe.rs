use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

macro_rules! define_cbe {
    ($name:ident, $native:ident, $bytes:expr) => {
        /// A ones-complement big-endian encoded unsigned integer, stored in a
        /// fixed-size byte array. This value is unaligned and suitable for use within
        /// zerocopy datastructures. It's purpose is to encode unsigned numbers in a
        /// descending numeric order when compared alphanumerically.
        #[derive(
            IntoBytes,
            FromBytes,
            Immutable,
            KnownLayout,
            Unaligned,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Clone,
            Copy,
        )]
        #[repr(transparent)]
        pub struct $name([u8; $bytes]);

        impl $name {
            pub const ZERO: Self = Self([0xff; $bytes]);
            pub const MAX_VALUE: Self = Self([0x00; $bytes]);

            #[inline(always)]
            pub const fn new(value: $native) -> Self {
                Self((!value).to_be_bytes())
            }

            #[inline(always)]
            pub const fn get(&self) -> $native {
                !$native::from_be_bytes(self.0)
            }

            #[inline(always)]
            pub fn set(&mut self, n: $native) {
                *self = Self::new(n);
            }

            /// Extracts the encoded bytes of `self`.
            #[inline(always)]
            pub const fn into_bytes(self) -> [u8; $bytes] {
                self.0
            }
        }

        impl From<$native> for $name {
            #[inline(always)]
            fn from(value: $native) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for $native {
            #[inline(always)]
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl Default for $name {
            #[inline(always)]
            fn default() -> Self {
                Self::ZERO
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.get()).finish()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.get().fmt(f)
            }
        }
    };
}

define_cbe!(CBE32, u32, 4);

define_cbe!(CBE64, u64, 8);

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_cbe64() {
        let tests = [
            (0, CBE64::ZERO),
            (u64::MAX, CBE64::MAX_VALUE),
            (1, CBE64::new(1)),
            (1256, CBE64::new(1256)),
        ];

        for (value, expected) in tests {
            let cbe = CBE64::new(value);
            assert_eq!(cbe, expected);
            assert_eq!(cbe.get(), value);
            assert_eq!(CBE64::from(value), expected);
        }
    }

    #[test]
    fn test_cbe64_order() {
        // verifies that cbe64 values naturally sort in descending order
        let expected = vec![
            CBE64::MAX_VALUE,
            CBE64::new(1 << 48),
            CBE64::new(1 << 32),
            CBE64::new(1 << 22),
            CBE64::new(1 << 19),
            CBE64::new(1 << 17),
            CBE64::new(1 << 16),
            CBE64::new(1 << 1),
            CBE64::ZERO,
        ];

        let mut actual = expected.clone();
        actual.reverse();
        actual.sort(); // should result in descending order

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_cbe32() {
        let tests = [
            (0, CBE32::ZERO),
            (u32::MAX, CBE32::MAX_VALUE),
            (1, CBE32::new(1)),
            (1256, CBE32::new(1256)),
        ];

        for (value, expected) in tests {
            let cbe = CBE32::new(value);
            assert_eq!(cbe, expected);
            assert_eq!(cbe.get(), value);
            assert_eq!(CBE32::from(value), expected);
        }
    }

    #[test]
    fn test_cbe32_order() {
        // verifies that cbe32 values naturally sort in descending order
        let expected = vec![
            CBE32::MAX_VALUE,
            CBE32::new(1 << 22),
            CBE32::new(1 << 19),
            CBE32::new(1 << 17),
            CBE32::new(1 << 16),
            CBE32::new(1 << 1),
            CBE32::ZERO,
        ];

        let mut actual = expected.clone();
        actual.reverse();
        actual.sort(); // should result in descending order

        assert_eq!(actual, expected);
    }
}
