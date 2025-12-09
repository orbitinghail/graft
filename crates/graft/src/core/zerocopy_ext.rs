use thiserror::Error;
use zerocopy::{
    ConvertError, Immutable, KnownLayout, SizeError, TryFromBytes, Unaligned, ValidityError,
};

pub trait TryFromBytesExt: TryFromBytes {
    #[must_use = "has no side effects"]
    #[inline]
    fn try_ref_from_unaligned_bytes(source: &[u8]) -> Result<&Self, ZerocopyErr>
    where
        Self: Unaligned + Immutable + KnownLayout,
    {
        #[allow(clippy::disallowed_methods)]
        Self::try_ref_from_bytes(source).map_err(Into::into)
    }
}

impl<T: TryFromBytes + ?Sized> TryFromBytesExt for T {}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ZerocopyErr {
    #[error("Invalid Alignment")]
    InvalidAlignment,

    #[error("Invalid Size")]
    InvalidSize,

    #[error("Invalid Data")]
    InvalidData,
}

impl<A, S, V> From<ConvertError<A, S, V>> for ZerocopyErr {
    #[inline]
    fn from(value: ConvertError<A, S, V>) -> Self {
        match value {
            ConvertError::Alignment(_) => Self::InvalidAlignment,
            ConvertError::Size(_) => Self::InvalidSize,
            ConvertError::Validity(_) => Self::InvalidData,
        }
    }
}

impl<A, B: ?Sized> From<SizeError<A, B>> for ZerocopyErr {
    #[inline]
    fn from(_: SizeError<A, B>) -> Self {
        Self::InvalidSize
    }
}

impl<A, B: ?Sized + TryFromBytes> From<ValidityError<A, B>> for ZerocopyErr {
    #[inline]
    fn from(_: ValidityError<A, B>) -> Self {
        Self::InvalidData
    }
}
