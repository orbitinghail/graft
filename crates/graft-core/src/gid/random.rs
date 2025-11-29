use zerocopy::{ByteHash, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::gid::prefix::ConstDefault;

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
pub struct GidRandom {
    data: [u8; 9],
}

impl ConstDefault for GidRandom {
    const DEFAULT: Self = Self { data: [0x80, 0, 0, 0, 0, 0, 0, 0, 0] };
}

impl GidRandom {
    pub fn random() -> Self {
        let mut data: [u8; 9] = rand::random();
        // set the first bit of the first byte to 1
        // this ensures that the bs58 representation of Random is always 13 bytes
        data[0] |= 0x80;
        Self { data }
    }
}
