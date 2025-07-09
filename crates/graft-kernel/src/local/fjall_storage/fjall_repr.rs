use culprit::Result;
use fjall::Slice;
use graft_core::{
    handle_id::HandleIdErr, lsn::InvalidLSN, page::PageSizeErr, page_idx::ConvertToPageIdxErr,
    zerocopy_ext::ZerocopyErr,
};

#[derive(Debug, thiserror::Error)]
pub enum DecodeErr {
    #[error("Invalid LSN: {0}")]
    InvalidLSN(#[from] InvalidLSN),

    #[error("Zerocopy error: {0}")]
    Zerocopy(#[from] ZerocopyErr),

    #[error("Invalid page index: {0}")]
    InvalidPageIdx(#[from] ConvertToPageIdxErr),

    #[error("Invalid handle ID: {0}")]
    InvalidHandleId(#[from] HandleIdErr),

    #[error("Invalid VolumeID: {0}")]
    GidParseErr(#[from] graft_core::gid::GidParseErr),

    #[error("Bilrost error: {0}")]
    BilrostErr(#[from] bilrost::DecodeError),

    #[error(transparent)]
    PageSizeErr(#[from] PageSizeErr),
}

pub trait FjallRepr: Sized {
    /// Converts Self into a type that can be cheaply converted into a byte
    /// slice. For ZeroCopy types, this may simply be the raw bytes of Self.
    fn as_slice(&self) -> impl AsRef<[u8]>;

    /// Converts a Fjall Slice into Self.
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr>;

    /// Converts Self into a Fjall Slice
    #[inline]
    fn into_slice(self) -> Slice {
        Slice::new(self.as_slice().as_ref())
    }
}

#[macro_export]
macro_rules! proxy_to_fjall_repr {
    (
        encode ($ty:ty) using proxy ($proxy:ty)
        into_proxy(&$self:ident) $into_proxy:block
        from_proxy($iproxy:ident) $from_proxy:block
    ) => {
        impl FjallRepr for $ty {
            #[inline]
            fn as_slice(&$self) -> impl AsRef<[u8]> {
                $into_proxy
            }

            #[inline]
            fn try_from_slice(slice: Slice) -> Result<Self, $crate::local::fjall_storage::fjall_repr::DecodeErr> {
                let $iproxy: &$proxy =
                    <$proxy>::try_ref_from_unaligned_bytes(&slice).or_into_ctx()?;
                $from_proxy
            }
        }
    };
}

#[cfg(test)]
pub mod testutil {
    use super::*;
    use std::{fmt::Debug, ops::Deref};

    /// Tests that a FjallRepr value can be encoded to a slice and then decoded back to the original value.
    #[track_caller]
    pub fn test_roundtrip<T>(value: T)
    where
        T: FjallRepr + PartialEq + Clone + Debug,
    {
        let slice = value.clone().into_slice();
        let decoded = T::try_from_slice(slice).expect("Failed to decode");
        assert_eq!(value, decoded, "Roundtrip failed");
    }

    /// Tests that a FjallRepr value correctly fails to decode invalid data.
    #[track_caller]
    pub fn test_invalid<T: FjallRepr>(slice: &[u8]) {
        assert!(
            T::try_from_slice(Slice::from(slice)).is_err(),
            "Expected error for invalid slice"
        );
    }

    /// Tests that a FjallRepr value decodes empty data into it's default repr.
    #[track_caller]
    pub fn test_empty_default<T: FjallRepr + Default + Debug + PartialEq>() {
        assert_eq!(
            T::try_from_slice(Slice::new(b"")).expect("failed to decode"),
            T::default(),
            "Expected empty slice to decode to default value"
        );
    }

    /// Tests that a FjallRepr type serializes to the expected ordering.
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
            assert_eq!(decoded, values[i], "Order mismatch at index {}", i);
        }
    }
}
