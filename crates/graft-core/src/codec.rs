use bilrost::OwnedMessage;
use bytes::{Buf, BufMut, Bytes};
use culprit::ResultExt;

use crate::page::{Page, PageSizeErr};

pub(crate) mod newtype_proxy;
pub(crate) mod zerocopy_encoding;

#[derive(Debug, thiserror::Error)]
pub enum EncodeErr {
    #[error("Buffer has insufficient capacity")]
    InsufficientCapacity,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeErr {
    #[error("Bilrost decoding error: {0}")]
    Bilrost(#[from] bilrost::DecodeError),

    #[error(transparent)]
    PageSizeErr(#[from] PageSizeErr),
}

pub trait Codec {
    type Message;

    /// Encodes the message into the provided buffer.
    fn encode<B: BufMut>(msg: Self::Message, buf: &mut B) -> culprit::Result<(), EncodeErr>;

    /// Encodes the message into a `Bytes` buffer.
    fn encode_to_bytes(msg: Self::Message) -> Bytes;

    /// Decodes the message from the provided buffer.
    fn decode<B: Buf>(buf: B) -> culprit::Result<Self::Message, DecodeErr>;
}

pub struct BilrostCodec<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> Codec for BilrostCodec<T>
where
    T: OwnedMessage,
{
    type Message = T;

    fn encode<B: BufMut>(msg: Self::Message, buf: &mut B) -> culprit::Result<(), EncodeErr> {
        Ok(msg
            .encode(buf)
            .map_err(|_| EncodeErr::InsufficientCapacity)?)
    }

    fn encode_to_bytes(msg: Self::Message) -> Bytes {
        msg.encode_to_bytes()
    }

    fn decode<B: Buf>(buf: B) -> culprit::Result<Self::Message, DecodeErr> {
        T::decode(buf).or_into_ctx()
    }
}

pub struct PageCodec;

impl Codec for PageCodec {
    type Message = Page;

    fn encode<B: BufMut>(msg: Self::Message, buf: &mut B) -> culprit::Result<(), EncodeErr> {
        buf.put(Bytes::from(msg));
        Ok(())
    }

    fn encode_to_bytes(msg: Self::Message) -> Bytes {
        msg.into()
    }

    fn decode<B: Buf>(buf: B) -> culprit::Result<Self::Message, DecodeErr> {
        Page::from_buf(buf).or_into_ctx()
    }
}
