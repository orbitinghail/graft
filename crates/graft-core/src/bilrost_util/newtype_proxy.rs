pub struct NewTypeProxyTag;

#[macro_export]
macro_rules! derive_newtype_proxy {
    (
        newtype ($ty:ty)
        with empty value ($empty:expr)
        with proxy type ($proxy_type:ty) and encoding ($proxy_encoding:ty)
        with sample value ($sample:expr)
        into_proxy(&$self1:ident) $into_proxy:block
        from_proxy(&mut $self2:ident, $proxy:ident) $from_proxy:block
    ) => {
        const _: () = {
            use $crate::bilrost_util::newtype_proxy::NewTypeProxyTag;
            use ::bilrost::encoding::{ Proxiable, ForOverwrite, EmptyState };
            use ::bilrost::DecodeErrorKind;

            impl Proxiable<NewTypeProxyTag> for $ty {
                type Proxy = $proxy_type;

                #[inline]
                fn encode_proxy(&$self1) -> Self::Proxy {
                    $into_proxy
                }

                #[inline]
                fn decode_proxy(&mut $self2, $proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
                    $from_proxy
                }
            }

            impl ForOverwrite<(), $ty> for () {
                #[inline(always)]
                fn for_overwrite() -> $ty where $ty: Sized { $empty }
            }

            impl EmptyState<(), $ty> for () {
                #[inline(always)]
                fn empty() -> $ty where $ty: Sized { $empty }

                #[inline(always)]
                fn is_empty(val: &$ty) -> bool { *val == $empty }

                #[inline(always)]
                fn clear(val: &mut $ty) { *val = $empty; }
            }

            ::bilrost::delegate_proxied_encoding!(
                use encoding ($proxy_encoding)
                to encode proxied type ($ty) using proxy tag (NewTypeProxyTag)
                with general encodings
            );
        };

        #[cfg(test)]
        mod newtype_encoding_tests {
            use super::*;

            #[graft_test::test]
            fn test_newtype_encoding() {
                #[derive(::bilrost::Message, Debug, PartialEq, Eq)]
                struct TestMsg {
                    value: $ty,
                    values: Vec<$ty>,
                    optional_value: Option<$ty>,
                }
                let msg = TestMsg {
                    value: $sample,
                    values: vec![$sample, $sample],
                    optional_value: Some($sample),
                };
                let b = ::bilrost::Message::encode_to_bytes(&msg);
                let decoded_msg: TestMsg = ::bilrost::OwnedMessage::decode(b).unwrap();
                assert_eq!(decoded_msg, msg);
            }
        }
    };
}
