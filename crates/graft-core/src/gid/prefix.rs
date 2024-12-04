//! Prefixes are used to distinguish between different types of Graft IDs.
//! The first bit is always set. This ensures that serialized graft IDs serialize to 22 bytes.
//! The next 2 bits currently determine the variant.
//! The rest of the Prefix is reserved for future use.

// We group bytes by field in this file to make it easier to see the bit layout.
#![allow(clippy::unusual_byte_groupings)]

use static_assertions::const_assert_ne;
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
    Default,
)]
#[repr(u8)]
pub enum Volume {
    #[default]
    Value = 0b1_00_00000,
}

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
    Default,
)]
#[repr(u8)]
pub enum Segment {
    #[default]
    Value = 0b1_01_00000,
}

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
    Default,
)]
#[repr(u8)]
pub enum Client {
    #[default]
    Value = 0b1_10_00000,
}

const_assert_ne!(Volume::Value as u8, Segment::Value as u8);
const_assert_ne!(Volume::Value as u8, Client::Value as u8);
const_assert_ne!(Segment::Value as u8, Client::Value as u8);

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
{
}

impl Prefix for Volume {}
impl Prefix for Segment {}
impl Prefix for Client {}
