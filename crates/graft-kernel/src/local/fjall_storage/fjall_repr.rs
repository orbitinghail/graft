use std::fmt::Debug;

use culprit::Result;
use fjall::Slice;
use graft_core::{
    handle_id::HandleIdErr, lsn::InvalidLSN, page_idx::ConvertToPageIdxErr,
    zerocopy_ext::ZerocopyErr,
};

#[derive(Debug, thiserror::Error)]
pub enum DecodeErr {
    #[error("Bilrost error: {0}")]
    Bilrost(#[from] bilrost::DecodeError),

    #[error(transparent)]
    PageSizeErr(#[from] graft_core::page::PageSizeErr),

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
}

pub trait FjallRepr: Sized + Debug {
    fn as_slice(&self) -> Slice;
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr>;
}

#[cfg(test)]
pub mod testutil {
    use super::FjallRepr;
    use fjall::Slice;

    /// Tests that a FjallRepr value can be encoded to a slice and then decoded back to the original value.
    #[track_caller]
    pub fn test_roundtrip<T: FjallRepr + PartialEq>(value: T) {
        let slice = value.as_slice();
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

    /// Tests that a FjallRepr type serializes to the expected ordering.
    #[track_caller]
    pub fn test_serialized_order<T: FjallRepr + PartialEq>(values: &[T]) {
        // serialize every element of T into a list of Slice
        let mut slices: Vec<Slice> = values.iter().map(|v| v.as_slice()).collect();

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
