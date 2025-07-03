use std::any::type_name;

use bilrost::DecodeError;

use crate::zerocopy_ext::ZerocopyErr;

pub struct CowEncoding<E = bilrost::encoding::General>(E);
bilrost::encoding_implemented_via_value_encoding!(CowEncoding<E>, with generics(E));
bilrost::implement_core_empty_state_rules!(CowEncoding<E>, with generics(E));

pub(crate) fn map_zerocopy_err<T>(err: ZerocopyErr) -> bilrost::DecodeError {
    let mut e = DecodeError::new(bilrost::DecodeErrorKind::InvalidValue);
    e.push(
        match err {
            ZerocopyErr::InvalidAlignment => {
                "source alignment does not match destination alignment"
            }
            ZerocopyErr::InvalidSize => "source size does not match destination size",
            ZerocopyErr::InvalidData => {
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
        with empty ($empty:expr)
        $(with generics ($($impl_generics:tt)*))?
    ) => {
        const _:() = {
            use $crate::codec::zerocopy_encoding::map_zerocopy_err;
            use ::bilrost::encoding::{
                Wiretyped, WireType, ForOverwrite, GeneralGeneric, ValueEncoder,
                PlainBytes, encoded_len_varint, ValueDecoder, Capped, DecodeContext,
                EmptyState,
            };
            use ::bilrost::DecodeError;
            use ::bilrost::buf::ReverseBuf;
            use ::bytes::{BufMut, Buf};
            use ::zerocopy::{TryFromBytes, IntoBytes, Immutable, KnownLayout};

            #[doc(hidden)]
            trait AssertIsZerocopy: IntoBytes + TryFromBytes + Immutable + KnownLayout {}
            #[doc(hidden)]
            impl$(<$($impl_generics)*>)? AssertIsZerocopy for $ty {}

            const WIRE_SIZE: usize = $size + encoded_len_varint($size as u64);

            impl$(<$($impl_generics)*>)? ForOverwrite<(), $ty> for () {
                #[inline]
                fn for_overwrite() -> $ty {
                    $empty
                }
            }

            impl$(<$($impl_generics)*>)? EmptyState<(), $ty> for () {
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

            impl<const __G:u8 $(,$($impl_generics)*)?> Wiretyped<GeneralGeneric<__G>, $ty> for () {
                const WIRE_TYPE: WireType = WireType::LengthDelimited;
            }

            impl<const __G:u8 $(,$($impl_generics)*)?> ValueEncoder<GeneralGeneric<__G>, $ty> for () {
                #[inline]
                fn encode_value<B: BufMut + ?Sized>(value: &$ty, buf: &mut B) {
                    <() as ValueEncoder<PlainBytes, _>>::encode_value(&value.as_bytes(), buf)
                }

                #[inline]
                fn prepend_value<B: ReverseBuf + ?Sized>(value: &$ty, buf: &mut B) {
                    <() as ValueEncoder<PlainBytes, _>>::prepend_value(&value.as_bytes(), buf)
                }

                #[inline]
                fn value_encoded_len(value: &$ty) -> usize {
                    debug_assert_eq!(
                        <() as ValueEncoder<PlainBytes, _>>::value_encoded_len(&value.as_bytes()),
                        WIRE_SIZE,
                        concat!("Invalid size in derive_zerocopy_encoding for ", stringify!($ty))
                    );
                    WIRE_SIZE
                }

                #[inline]
                fn many_values_encoded_len<I>(values: I) -> usize
                where
                    I: ExactSizeIterator,
                    I::Item: std::ops::Deref<Target = $ty>,
                {
                    let many_size = WIRE_SIZE
                        .checked_mul(values.len())
                        .expect(concat!("Overflow in many_values_encoded_len for ", stringify!($ty)));
                    debug_assert_eq!(
                        values
                            .map(|val| Self::value_encoded_len(&val.as_bytes()))
                            .sum::<usize>(),
                        many_size,
                        concat!("Invalid size in derive_zerocopy_encoding for ", stringify!($ty))
                    );
                    many_size
                }
            }

            impl<const __G:u8 $(,$($impl_generics)*)?> ValueDecoder<GeneralGeneric<__G>, $ty> for () {
                fn decode_value<B: Buf + ?Sized>(
                    value: &mut $ty,
                    mut buf: Capped<B>,
                    _ctx: DecodeContext,
                ) -> Result<(), DecodeError> {
                    let buf = buf.take_length_delimited()?;
                    let mut bytes = [0u8; $size];
                    bytes.as_mut_slice().put(buf.take_all());
                    *value = <$ty>::try_read_from_bytes(&bytes)
                        .map_err(|e| map_zerocopy_err::<$ty>(e.into()))?;
                    Ok(())
                }
            }
        };
    };

    (
        encode borrowed type ($ty:ty)
        with size ($size:expr)
        with empty ($empty:expr)
        $(with generics ($($impl_generics:tt)*))?
    ) => {
        derive_zerocopy_encoding!(
            encode type ($ty)
            with size ($size)
            with empty ($empty)
            $(with generics ($($impl_generics)*))?
        );
        const _:() = {
            use $crate::codec::zerocopy_encoding::{CowEncoding, map_zerocopy_err};
            use $crate::zerocopy_ext::TryFromBytesExt;
            use ::bilrost::encoding::{
                Wiretyped, WireType, GeneralGeneric, ValueEncoder,
                ValueDecoder, Capped, DecodeContext, ValueBorrowDecoder,
                ForOverwrite, encoded_len_varint, EmptyState
            };
            use ::bilrost::DecodeError;
            use ::bilrost::buf::ReverseBuf;
            use ::bytes::{BufMut, Buf};
            use ::zerocopy::Unaligned;
            use ::std::borrow::Cow;

            const WIRE_SIZE: usize = $size + encoded_len_varint($size as u64);

            #[doc(hidden)]
            trait AssertIsZerocopy: Unaligned {}
            #[doc(hidden)]
            impl$(<$($impl_generics)*>)? AssertIsZerocopy for $ty {}

            type Enc<const G: u8> = CowEncoding<GeneralGeneric<G>>;

            impl<'a, const __G:u8 $(,$($impl_generics)*)?> ForOverwrite<Enc<__G>, Cow<'a, $ty>> for () {
                #[inline]
                fn for_overwrite() -> Cow<'a, $ty> {
                    Cow::Owned($empty)
                }
            }

            impl<'a, const __G:u8 $(,$($impl_generics)*)?> EmptyState<Enc<__G>, Cow<'a, $ty>> for () {
                #[inline]
                fn empty() -> Cow<'a, $ty>
                where
                    Cow<'a, $ty>: Sized,
                {
                    Cow::Owned($empty)
                }

                #[inline]
                fn is_empty(val: &Cow<'a, $ty>) -> bool {
                    **val == $empty
                }

                #[inline]
                fn clear(val: &mut Cow<'a, $ty>) {
                    *val = Cow::Owned($empty);
                }
            }

            impl<const __G:u8 $(,$($impl_generics)*)?> Wiretyped<Enc<__G>, Cow<'_, $ty>> for ()
            {
                const WIRE_TYPE: WireType = WireType::LengthDelimited;
            }

            impl<'a, const __G:u8 $(,$($impl_generics)*)?> ValueEncoder<Enc<__G>, Cow<'a, $ty>> for ()
            {
                #[inline]
                fn encode_value<B: BufMut + ?Sized>(value: &Cow<$ty>, buf: &mut B) {
                    <() as ValueEncoder<GeneralGeneric<__G>, _>>::encode_value(&**value, buf)
                }

                #[inline]
                fn prepend_value<B: ReverseBuf + ?Sized>(value: &Cow<$ty>, buf: &mut B) {
                    <() as ValueEncoder<GeneralGeneric<__G>, _>>::prepend_value(&**value, buf)
                }

                #[inline]
                fn value_encoded_len(value: &Cow<$ty>) -> usize {
                    <() as ValueEncoder<GeneralGeneric<__G>, _>>::value_encoded_len(&**value)
                }

                #[inline]
                fn many_values_encoded_len<I>(values: I) -> usize
                where
                    I: ExactSizeIterator,
                    I::Item: std::ops::Deref<Target = Cow<'a, $ty>>,
                {
                    let many_size = WIRE_SIZE
                        .checked_mul(values.len())
                        .expect(concat!("Overflow in many_values_encoded_len for ", stringify!($ty)));
                    debug_assert_eq!(
                        values
                            .map(|val| Self::value_encoded_len(&val.as_bytes()))
                            .sum::<usize>(),
                        many_size,
                        concat!("Invalid size in derive_zerocopy_encoding for ", stringify!($ty))
                    );
                    many_size
                }
            }

            impl<'a, const __G:u8 $(,$($impl_generics)*)?> ValueDecoder<Enc<__G>, Cow<'a, $ty>> for ()
            {
                #[inline]
                fn decode_value<B: Buf + ?Sized>(
                    value: &mut Cow<$ty>,
                    buf: Capped<B>,
                    ctx: DecodeContext,
                ) -> Result<(), DecodeError> {
                    <() as ValueDecoder<GeneralGeneric<__G>, _>>::decode_value(value.to_mut(), buf, ctx)
                }
            }

            impl<'a, const __G:u8 $(,$($impl_generics)*)?>
            ValueBorrowDecoder<'a, Enc<__G>, Cow<'a, $ty>> for ()
            {
                fn borrow_decode_value(
                    value: &mut Cow<'a, $ty>,
                    mut buf: Capped<&'a [u8]>,
                    _ctx: DecodeContext,
                ) -> Result<(), DecodeError> {
                    let bytes = buf.take_borrowed_length_delimited()?;
                    *value = Cow::Borrowed(<$ty>::try_ref_from_unaligned_bytes(&bytes)
                        .map_err(|e| map_zerocopy_err::<$ty>(e.into()))?);
                    Ok(())
                }
            }
        };
    };
}
