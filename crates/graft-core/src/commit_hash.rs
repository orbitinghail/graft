use bytes::Bytes;
use prost::DecodeError;

use crate::{derive_newtype_message_bytes, protoutil::NewtypeMessageBytes};

const HASH_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct CommitHash {
    hash: [u8; HASH_SIZE],
}

static_assertions::assert_eq_size!(CommitHash, [u8; HASH_SIZE]);

impl NewtypeMessageBytes for CommitHash {
    fn encode(&self) -> impl bytes::Buf {
        self.hash.as_slice()
    }

    fn decode(&mut self, buf: Bytes) -> Result<(), DecodeError> {
        if buf.len() != HASH_SIZE {
            return Err(DecodeError::new("CommitHash must be 32 bytes long"));
        }
        self.hash.copy_from_slice(&buf);
        Ok(())
    }

    fn serialized_size(&self) -> usize {
        HASH_SIZE
    }
}

derive_newtype_message_bytes!(CommitHash);
