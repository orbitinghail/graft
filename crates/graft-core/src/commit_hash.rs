use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::{byte_unit::ByteUnit, derive_zerocopy_encoding};

const HASH_SIZE: ByteUnit = ByteUnit::new(32);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
    Unaligned,
)]
#[repr(transparent)]
pub struct CommitHash {
    hash: [u8; HASH_SIZE.as_usize()],
}

impl CommitHash {
    const ZERO: Self = Self { hash: [0; HASH_SIZE.as_usize()] };
}

static_assertions::assert_eq_size!(CommitHash, [u8; HASH_SIZE.as_usize()]);

derive_zerocopy_encoding!(
    encode borrowed type (CommitHash)
    with size (HASH_SIZE.as_usize())
    with for overwrite (CommitHash::ZERO)
);
