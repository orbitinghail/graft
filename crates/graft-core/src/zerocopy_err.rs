use thiserror::Error;
use zerocopy::{ConvertError, SizeError};

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
    fn from(value: ConvertError<A, S, V>) -> Self {
        match value {
            ConvertError::Alignment(_) => Self::InvalidAlignment,
            ConvertError::Size(_) => Self::InvalidSize,
            ConvertError::Validity(_) => Self::InvalidData,
        }
    }
}

impl<A, B> From<SizeError<A, B>> for ZerocopyErr {
    fn from(_: SizeError<A, B>) -> Self {
        Self::InvalidSize
    }
}
