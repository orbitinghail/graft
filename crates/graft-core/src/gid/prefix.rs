use std::hash::Hash;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoBytes,
    Unaligned,
    TryFromBytes,
    Immutable,
    KnownLayout,
)]
#[repr(u8)]
pub enum Prefix {
    Volume = 0b1000_0000,
    Segment = 0b0100_0000,
}

impl Prefix {
    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            0b1000_0000 => Self::Volume,
            0b0100_0000 => Self::Segment,
            _ => panic!("Invalid prefix: {}", value),
        }
    }
}
