use std::str::Utf8Error;

use crate::core::{
    gid::{self, Gid},
    lsn::InvalidLSN,
    page::PageSizeErr,
    pageidx::ConvertToPageIdxErr,
    zerocopy_ext::ZerocopyErr,
};
use bytes::Bytes;
use bytestring::ByteString;
use fjall::Slice;
use zerocopy::IntoBytes;

use crate::core::gid::GidParseErr;

#[derive(Debug, thiserror::Error)]
pub enum DecodeErr {
    #[error("Invalid LSN: {0}")]
    InvalidLSN(#[from] InvalidLSN),

    #[error("Zerocopy error: {0}")]
    Zerocopy(#[from] ZerocopyErr),

    #[error("Invalid page index: {0}")]
    InvalidPageIdx(#[from] ConvertToPageIdxErr),

    #[error(transparent)]
    InvalidUtf8(#[from] Utf8Error),

    #[error("Invalid ID: {0}")]
    GidParseErr(#[from] GidParseErr),

    #[error("Bilrost error: {0}")]
    BilrostErr(#[from] bilrost::DecodeError),

    #[error(transparent)]
    PageSizeErr(#[from] PageSizeErr),

    #[error("Expected empty value; got value of size {0}")]
    NonemptyValue(usize),
}

pub trait FjallReprRef {
    /// Converts Self into a type that can be cheaply converted into a byte
    /// slice. For `ZeroCopy` types, this may simply be the raw bytes of Self.
    fn as_slice(&self) -> impl AsRef<[u8]>;

    /// Converts Self into a Fjall Slice
    #[inline]
    fn into_slice(self) -> Slice
    where
        Self: Sized,
    {
        Slice::new(self.as_slice().as_ref())
    }
}

pub trait FjallRepr: FjallReprRef + Clone {
    /// Converts a Fjall Slice into Self.
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr>;
}

impl FjallReprRef for str {
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }
}

impl<P: gid::prefix::Prefix> FjallReprRef for Gid<P> {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }
}

impl<P: gid::prefix::Prefix> FjallRepr for Gid<P> {
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        Ok(Self::try_from(Bytes::from(slice))?)
    }
}

impl FjallReprRef for ByteString {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }
}

impl FjallRepr for ByteString {
    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        let bytes: Bytes = slice.into();
        Ok(ByteString::try_from(bytes)?)
    }
}

#[macro_export]
macro_rules! proxy_to_fjall_repr {
    (
        encode ($ty:ty) using proxy ($proxy:ty)
        into_proxy($me:ident) $into_proxy:block
        from_proxy($iproxy:ident) $from_proxy:block
    ) => {
        impl FjallReprRef for $ty {
            #[inline]
            fn as_slice(&self) -> impl AsRef<[u8]> {
                let $me = self.clone();
                $into_proxy
            }

            #[inline]
            fn into_slice(self) -> Slice
            where
                Self: Sized,
            {
                let $me = self;
                Slice::new($into_proxy.as_ref())
            }
        }

        impl FjallRepr for $ty {
            #[inline]
            fn try_from_slice(
                slice: Slice,
            ) -> Result<Self, $crate::local::fjall_storage::fjall_repr::DecodeErr> {
                let $iproxy: &$proxy = <$proxy>::try_ref_from_unaligned_bytes(&slice)?;
                $from_proxy
            }
        }
    };
}

#[cfg(test)]
pub mod testutil {
    use super::*;
    use std::fmt::Debug;

    /// Tests that a `FjallRepr` value can be encoded to a slice and then decoded back to the original value.
    #[track_caller]
    pub fn test_roundtrip<T>(value: T)
    where
        T: FjallRepr + PartialEq + Clone + Debug,
    {
        let slice = value.clone().into_slice();
        let decoded = T::try_from_slice(slice).expect("Failed to decode");
        assert_eq!(value, decoded, "Roundtrip failed");
    }

    /// Tests that a `FjallRepr` value correctly fails to decode invalid data.
    #[track_caller]
    pub fn test_invalid<T: FjallRepr>(slice: &[u8]) {
        assert!(
            T::try_from_slice(Slice::from(slice)).is_err(),
            "Expected error for invalid slice"
        );
    }

    /// Tests that a `FjallRepr` value decodes empty data into it's default repr.
    #[track_caller]
    pub fn test_empty_default<T: FjallRepr + Default + Debug + PartialEq>() {
        assert_eq!(
            T::try_from_slice(Slice::new(b"")).expect("failed to decode"),
            T::default(),
            "Expected empty slice to decode to default value"
        );
    }

    /// Tests that a `FjallRepr` type serializes to the expected ordering.
    #[track_caller]
    pub fn test_serialized_order<T>(values: &[T])
    where
        T: FjallRepr + PartialEq + Clone + Debug,
    {
        // serialize every element of T into a list of Slice
        let mut slices: Vec<Slice> = values.iter().cloned().map(|v| v.into_slice()).collect();

        // then sort the list of slices by their natural bytewise order
        slices.sort();

        // then verify that the resulting list of slices is in the same order as
        // the values array
        for (i, slice) in slices.into_iter().enumerate() {
            let decoded = T::try_from_slice(slice).expect("Failed to decode");
            assert_eq!(decoded, values[i], "Order mismatch at index {i}");
        }
    }
}
