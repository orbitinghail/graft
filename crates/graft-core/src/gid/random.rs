use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::gid::prefix::Prefix;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ByteHash,
    IntoBytes,
    TryFromBytes,
    Immutable,
    KnownLayout,
    Unaligned,
)]
#[repr(C)]
pub struct GidRandom<P: Prefix> {
    prefix: P,
    data: [u8; 8],
}

impl<P: Prefix> GidRandom<P> {
    pub const ZERO: Self = Self { prefix: P::DEFAULT, data: [0; 8] };

    pub fn random() -> Self {
        Self { prefix: P::DEFAULT, data: rand::random() }
    }
}
