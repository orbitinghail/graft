use thiserror::Error;
use zerocopy::{ConvertError, SizeError, TryFromBytes, ValidityError};

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
    #[track_caller]
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
    #[track_caller]
    fn from(_: SizeError<A, B>) -> Self {
        Self::InvalidSize
    }
}

impl<A, B: ?Sized + TryFromBytes> From<ValidityError<A, B>> for ZerocopyErr {
    #[inline]
    #[track_caller]
    fn from(_: ValidityError<A, B>) -> Self {
        Self::InvalidData
    }
}
