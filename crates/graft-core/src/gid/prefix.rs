//! Prefixes are used to distinguish between different types of Graft IDs.
//! The most-significant bit is always set to ensure a Prefix byte is never zero.
//! The next 2 bits determine the variant.
//! The rest of the Prefix is reserved for future use.

// We group bytes by field in this file to make it easier to see the bit layout.
#![allow(clippy::unusual_byte_groupings)]

use static_assertions::const_assert_ne;
use std::{fmt::Debug, hash::Hash};
use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    ByteHash,
    PartialOrd,
    Ord,
    IntoBytes,
    Unaligned,
    TryFromBytes,
    Immutable,
    KnownLayout,
    Default,
)]
#[repr(u8)]
pub enum Log {
    #[default]
    Value = 0b1_01_00000,
}

impl Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Log")
    }
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    ByteHash,
    PartialOrd,
    Ord,
    IntoBytes,
    Unaligned,
    TryFromBytes,
    Immutable,
    KnownLayout,
    Default,
)]
#[repr(u8)]
pub enum Segment {
    #[default]
    Value = 0b1_10_00000,
}

impl Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment")
    }
}

// Ensure none of the prefixes equal one-another
const_assert_ne!(Log::Value as u8, Segment::Value as u8);

// Ensure none of the prefixes are zero.
const_assert_ne!(Log::Value as u8, 0);
const_assert_ne!(Segment::Value as u8, 0);

pub trait ConstDefault {
    const DEFAULT: Self;
}

impl ConstDefault for Log {
    const DEFAULT: Self = Log::Value;
}

impl ConstDefault for Segment {
    const DEFAULT: Self = Segment::Value;
}

pub trait Prefix:
    Clone
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Hash
    + IntoBytes
    + TryFromBytes
    + Immutable
    + KnownLayout
    + Default
    + Unaligned
    + ConstDefault
{
}

impl Prefix for Log {}
impl Prefix for Segment {}
