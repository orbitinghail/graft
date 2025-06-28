use bytes::{Buf, Bytes};
use prost::DecodeError;

// HandleId { string id = 1; } -> HandleId(String)
//      -> LengthDelimited len bytes
//      -> buf.put_slice
// LSN { uint64 lsn = 1; } -> LSN
//      -> Varint varint
// PageIdx { uint32 pageidx = 1; } -> PageIdx
//      -> Varint varint
// PageCount { uint32 page_count = 1; } -> PageCount
//      -> Varint varint

pub(crate) trait NewtypeMessageBytes: Default {
    /// serialize this type into a buffer.
    fn encode(&self) -> impl Buf;

    /// deserialize a buffer into self.
    fn decode(&mut self, buf: Bytes) -> Result<(), DecodeError>;

    /// return the serialized size of this type
    /// SAFETY: must match the size of `self.encode`
    fn serialized_size(&self) -> usize;

    /// reset this type to its default value
    #[inline]
    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[macro_export]
macro_rules! derive_newtype_message_bytes {
    ($ty:ty) => {
        ::static_assertions::assert_impl_all!(
            $ty: $crate::protoutil::NewtypeMessageBytes
        );

        impl ::prost::Message for $ty {
            fn encode_raw(&self, buf: &mut impl ::bytes::BufMut)
            where
                Self: Sized,
            {
                ::prost::encoding::encode_key(1, ::prost::encoding::WireType::LengthDelimited, buf);
                let encoded = $crate::protoutil::NewtypeMessageBytes::encode(self);
                ::prost::encoding::encode_varint(::bytes::Buf::remaining(&encoded) as u64, buf);
                buf.put(encoded);
            }

            fn merge_field(
                &mut self,
                tag: u32,
                wire_type: ::prost::encoding::WireType,
                buf: &mut impl ::bytes::Buf,
                ctx: ::prost::encoding::DecodeContext,
            ) -> Result<(), ::prost::DecodeError>
            where
                Self: Sized,
            {
                if tag == 1 {
                    ::prost::encoding::check_wire_type(
                        ::prost::encoding::WireType::LengthDelimited,
                        wire_type
                    )?;
                    let len = ::prost::encoding::decode_varint(buf)? as usize;
                    if len > buf.remaining() {
                        return Err(::prost::DecodeError::new("buffer underflow"));
                    }
                    $crate::protoutil::NewtypeMessageBytes::decode(self, buf.copy_to_bytes(len))
                } else {
                    ::prost::encoding::skip_field(wire_type, tag, buf, ctx)
                }
            }

            fn encoded_len(&self) -> usize {
                $crate::protoutil::NewtypeMessageBytes::serialized_size(self)
            }

            fn clear(&mut self) {
                $crate::protoutil::NewtypeMessageBytes::clear(self)
            }
        }
    };
}
