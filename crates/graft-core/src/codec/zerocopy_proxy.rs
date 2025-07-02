use std::any::type_name;

use bilrost::{
    DecodeError,
    encoding::{
        PlainBytes, ValueBorrowDecoder, ValueDecoder, ValueEncoder, WireType, Wiretyped,
        encoded_len_varint,
    },
};
use bytes::BufMut;

pub struct ZerocopyEncoding<const SIZE: usize>;

bilrost::encoding_implemented_via_value_encoding!(
    ZerocopyEncoding<SIZE>,
    with generics(const SIZE: usize)
);

bilrost::implement_core_empty_state_rules!(
    ZerocopyEncoding<SIZE>,
    with generics(const SIZE: usize)
);

pub trait ZerocopyProxiable<const SIZE: usize>:
    zerocopy::IntoBytes + zerocopy::TryFromBytes + zerocopy::Immutable + zerocopy::KnownLayout
{
    const WIRE_SIZE: usize = SIZE + encoded_len_varint(SIZE as u64);
}

impl<const SIZE: usize, T: ZerocopyProxiable<SIZE>> Wiretyped<ZerocopyEncoding<SIZE>, T> for () {
    const WIRE_TYPE: WireType = WireType::LengthDelimited;
}

impl<const SIZE: usize, T> ValueEncoder<ZerocopyEncoding<SIZE>, T> for ()
where
    T: ZerocopyProxiable<SIZE>,
{
    #[inline]
    fn encode_value<B: bytes::BufMut + ?Sized>(value: &T, buf: &mut B) {
        <() as ValueEncoder<PlainBytes, _>>::encode_value(&value.as_bytes(), buf)
    }

    #[inline]
    fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &T, buf: &mut B) {
        <() as ValueEncoder<PlainBytes, _>>::prepend_value(&value.as_bytes(), buf)
    }

    #[inline]
    fn value_encoded_len(value: &T) -> usize {
        debug_assert_eq!(
            <() as ValueEncoder<PlainBytes, _>>::value_encoded_len(&value.as_bytes()),
            T::WIRE_SIZE,
            "Invalid ZerocopyProxiable::<SIZE> for {}",
            type_name::<T>()
        );
        T::WIRE_SIZE
    }

    #[inline]
    fn many_values_encoded_len<I>(values: I) -> usize
    where
        I: ExactSizeIterator,
        I::Item: std::ops::Deref<Target = T>,
    {
        let len = values.len();
        let many_size = T::WIRE_SIZE
            .checked_mul(len)
            .expect("Overflow in many_values_encoded_len for ZerocopyProxiable type");
        debug_assert_eq!(
            values
                .map(|val| Self::value_encoded_len(&val.as_bytes()))
                .sum::<usize>(),
            many_size,
            "Invalid ZerocopyProxiable::<SIZE> for {}",
            type_name::<T>()
        );
        many_size
    }
}

impl<const SIZE: usize, T> ValueDecoder<ZerocopyEncoding<SIZE>, T> for ()
where
    T: ZerocopyProxiable<SIZE>,
{
    fn decode_value<B: bytes::Buf + ?Sized>(
        value: &mut T,
        mut buf: bilrost::encoding::Capped<B>,
        _ctx: bilrost::encoding::DecodeContext,
    ) -> Result<(), bilrost::DecodeError> {
        let buf = buf.take_length_delimited()?;
        let mut bytes = [0u8; SIZE];
        bytes.as_mut_slice().put(buf.take_all());
        *value = T::try_read_from_bytes(&bytes).map_err(map_zerocopy_err::<T, _, _, _>)?;
        Ok(())
    }
}

impl<'a, const SIZE: usize, T> ValueBorrowDecoder<'a, ZerocopyEncoding<SIZE>, &'a T> for ()
where
    &'a T: ZerocopyProxiable<SIZE> + zerocopy::Unaligned,
    T: ZerocopyProxiable<SIZE>,
{
    fn borrow_decode_value(
        value: &mut &'a T,
        mut buf: bilrost::encoding::Capped<&'a [u8]>,
        _ctx: bilrost::encoding::DecodeContext,
    ) -> Result<(), DecodeError> {
        let bytes = buf.take_borrowed_length_delimited()?;
        *value = T::try_ref_from_bytes(&bytes).map_err(map_zerocopy_err::<T, _, _, _>)?;
        todo!()
    }
}

fn map_zerocopy_err<T: zerocopy::TryFromBytes, A, S, V>(
    err: zerocopy::ConvertError<A, S, V>,
) -> bilrost::DecodeError {
    let mut e = DecodeError::new(bilrost::DecodeErrorKind::InvalidValue);
    e.push(
        match err {
            zerocopy::ConvertError::Alignment(_) => {
                "source alignment does not match destination alignment"
            }
            zerocopy::ConvertError::Size(_) => "source size does not match destination size",
            zerocopy::ConvertError::Validity(_) => {
                "source bytes are not a valid value of the destination type"
            }
        },
        type_name::<T>(),
    );
    e
}

#[macro_export]
macro_rules! derive_zerocopy_encoding {
    (
        encode type ($ty:ty)
        with size ($size:expr)
        with for overwrite ($for_overwrite:expr)
        $(with generics ($($impl_generics:tt)*))?
        $(with empty state ($empty:expr))?
    ) => {
        impl$(<$($impl_generics)*>)? $crate::codec::zerocopy_proxy::ZerocopyProxiable<{ $size }> for $ty {}

        impl$(<$($impl_generics)*>)? ::bilrost::encoding::ForOverwrite<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, $ty> for () {
            #[inline]
            fn for_overwrite() -> $ty {
                $for_overwrite
            }
        }
        impl$(<$($impl_generics)*>)? ::bilrost::encoding::ForOverwrite<(), $ty> for () {
            #[inline]
            fn for_overwrite() -> $ty {
                $for_overwrite
            }
        }

        derive_zerocopy_encoding!(
            @internal
            encode type ($ty)
            with size ($size)
            with for overwrite ($for_overwrite)
            $(with generics ($($impl_generics)*))?
            with empty state $(($empty))?
        );

        impl$(<$($impl_generics)*>)? ::bilrost::encoding::Wiretyped<::bilrost::encoding::General, $ty> for () {
            const WIRE_TYPE: ::bilrost::encoding::WireType =
                ::bilrost::encoding::WireType::LengthDelimited;
        }

        impl$(<$($impl_generics)*>)? ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General, $ty> for () {
            #[inline]
            fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &$ty, buf: &mut B) {
                <() as ::bilrost::encoding::ValueEncoder<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, _>>
                    ::encode_value(value, buf);
            }

            #[inline]
            fn prepend_value<B: ::bilrost::buf::ReverseBuf + ?Sized>(value: &$ty, buf: &mut B) {
                <() as ::bilrost::encoding::ValueEncoder<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, _>>
                    ::prepend_value(value, buf);
            }

            #[inline]
            fn value_encoded_len(value: &$ty) -> usize {
                <() as ::bilrost::encoding::ValueEncoder<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, _>>
                    ::value_encoded_len(value)
            }

            #[inline]
            fn many_values_encoded_len<I>(values: I) -> usize
            where
                I: ExactSizeIterator,
                I::Item: std::ops::Deref<Target = $ty>,
            {
                <() as ::bilrost::encoding::ValueEncoder<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, _>>
                    ::many_values_encoded_len(values)
            }
        }

        impl$(<$($impl_generics)*>)? ::bilrost::encoding::ValueDecoder<::bilrost::encoding::General, $ty> for () {
            fn decode_value<B: ::bytes::Buf + ?Sized>(
                value: &mut $ty,
                buf: ::bilrost::encoding::Capped<B>,
                ctx: ::bilrost::encoding::DecodeContext,
            ) -> Result<(), ::bilrost::DecodeError> {
                <() as ::bilrost::encoding::ValueDecoder<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, _>>::decode_value(value, buf, ctx)
            }
        }
    };

    (
        @internal
        encode type ($ty:ty)
        with size ($size:expr)
        with for overwrite ($for_overwrite:expr)
        $(with generics ($($impl_generics:tt)*))?
        with empty state ($empty:expr)
    ) => {
        impl$(<$($impl_generics)*>)? ::bilrost::encoding::EmptyState<$crate::codec::zerocopy_proxy::ZerocopyEncoding<{ $size }>, $ty> for () {
            #[inline]
            fn empty() -> $ty
            where
                $ty: Sized,
            {
                $empty
            }

            #[inline]
            fn is_empty(val: &$ty) -> bool {
                *val == $empty
            }

            #[inline]
            fn clear(val: &mut $ty) {
                *val = $empty;
            }
        }
    };

    (
        @internal
        encode type ($ty:ty)
        with size ($size:expr)
        with for overwrite ($for_overwrite:expr)
        $(with generics ($($impl_generics:tt)*))?
        with empty state
    ) => {
    }
}
