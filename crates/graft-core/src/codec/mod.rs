use bilrost::OwnedMessage;
use bytes::{Buf, BufMut, Bytes};
use culprit::ResultExt;

pub(crate) mod v1 {
    pub mod local;
    pub mod remote;
}

pub(crate) mod newtype_proxy;
pub(crate) mod zerocopy_encoding;

#[derive(Debug, Clone, thiserror::Error)]
#[error("Buffer has insufficient capacity")]
pub struct InsufficientCapacityErr;

#[derive(Debug, Clone, thiserror::Error)]
pub enum DecodeErr {
    #[error("Bilrost decoding error: {0}")]
    Bilrost(#[from] bilrost::DecodeError),
}

pub trait Codec {
    /// Encodes the object into the provided buffer.
    fn encode<B: BufMut>(&self, buf: &mut B) -> culprit::Result<(), InsufficientCapacityErr>;

    /// Decodes the object from the provided buffer.
    fn decode<B: Buf>(buf: B) -> culprit::Result<Self, DecodeErr>
    where
        Self: Sized;

    /// Returns the length of the encoded object.
    fn encoded_len(&self) -> usize;

    /// Encodes the object into a `Bytes` buffer.
    fn encode_to_bytes(&self) -> Bytes;
}

impl<M: OwnedMessage> Codec for M {
    #[inline]
    fn encode<B: BufMut>(&self, buf: &mut B) -> culprit::Result<(), InsufficientCapacityErr> {
        self.encode(buf)
            .map_err(|_| InsufficientCapacityErr)
            .or_into_ctx()
    }

    #[inline]
    fn decode<B: Buf>(buf: B) -> culprit::Result<Self, DecodeErr>
    where
        Self: Sized,
    {
        Ok(Self::decode(buf)?)
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        self.encoded_len()
    }

    #[inline]
    fn encode_to_bytes(&self) -> Bytes {
        self.encode_to_bytes()
    }
}
