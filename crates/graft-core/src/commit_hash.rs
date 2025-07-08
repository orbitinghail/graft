use bilrost::encoding::PlainBytes;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::{byte_unit::ByteUnit, derive_newtype_proxy, derive_zerocopy_encoding};

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

    #[cfg(test)]
    fn random() -> Self {
        Self { hash: rand::random() }
    }
}

impl From<[u8; HASH_SIZE.as_usize()]> for CommitHash {
    fn from(value: [u8; HASH_SIZE.as_usize()]) -> Self {
        Self { hash: value }
    }
}

impl From<CommitHash> for [u8; HASH_SIZE.as_usize()] {
    fn from(value: CommitHash) -> Self {
        value.hash
    }
}

static_assertions::assert_eq_size!(CommitHash, [u8; HASH_SIZE.as_usize()]);

derive_zerocopy_encoding!(
    encode type (CommitHash)
    with size (HASH_SIZE.as_usize())
    with empty (CommitHash::ZERO)
);

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use bilrost::{BorrowedMessage, Message, OwnedMessage};

    #[graft_test::test]
    fn test_bilrost() {
        #[derive(Message, Debug, PartialEq, Eq)]
        struct TestMsg {
            hash: Option<CommitHash>,
        }

        let msg = TestMsg { hash: Some(CommitHash::random()) };
        let b = msg.encode_to_bytes();
        let decoded: TestMsg = TestMsg::decode(b).unwrap();
        assert_eq!(decoded, msg, "Decoded message does not match original");
    }
}
