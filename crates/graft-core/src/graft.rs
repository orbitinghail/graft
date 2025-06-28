use bytes::Bytes;
use prost::DecodeError;
use splinter_rs::cow::CowSplinter;

use crate::{derive_newtype_message_bytes, protoutil::NewtypeMessageBytes};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Graft {
    splinter: CowSplinter<Bytes>,
}

impl NewtypeMessageBytes for Graft {
    fn encode(&self) -> impl bytes::Buf {
        self.splinter.serialize_to_bytes()
    }

    fn decode(&mut self, buf: Bytes) -> Result<(), DecodeError> {
        self.splinter =
            CowSplinter::from_bytes(buf).map_err(|err| DecodeError::new(err.to_string()))?;
        Ok(())
    }

    fn serialized_size(&self) -> usize {
        self.splinter.serialized_size()
    }
}

derive_newtype_message_bytes!(Graft);
