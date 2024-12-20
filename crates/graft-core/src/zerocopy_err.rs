use thiserror::Error;
use trackerr::{CallerLocation, LocationStack};
use zerocopy::{ConvertError, SizeError};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ZerocopyErr {
    #[error("Invalid Alignment")]
    InvalidAlignment(CallerLocation),

    #[error("Invalid Size")]
    InvalidSize(CallerLocation),

    #[error("Invalid Data")]
    InvalidData(CallerLocation),
}

impl<A, S, V> From<ConvertError<A, S, V>> for ZerocopyErr {
    #[inline]
    #[track_caller]
    fn from(value: ConvertError<A, S, V>) -> Self {
        match value {
            ConvertError::Alignment(_) => Self::InvalidAlignment(Default::default()),
            ConvertError::Size(_) => Self::InvalidSize(Default::default()),
            ConvertError::Validity(_) => Self::InvalidData(Default::default()),
        }
    }
}

impl<A, B> From<SizeError<A, B>> for ZerocopyErr {
    #[inline]
    #[track_caller]
    fn from(_: SizeError<A, B>) -> Self {
        Self::InvalidSize(Default::default())
    }
}

impl LocationStack for ZerocopyErr {
    fn location(&self) -> &CallerLocation {
        match self {
            ZerocopyErr::InvalidAlignment(loc)
            | ZerocopyErr::InvalidSize(loc)
            | ZerocopyErr::InvalidData(loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        None
    }
}
