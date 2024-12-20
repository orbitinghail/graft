use thiserror::Error;

mod bitmap;
mod block;
mod index;
pub mod ops;
mod partition;
mod relational;
mod splinter;
mod util;

#[cfg(test)]
mod testutil;

pub use splinter::{Splinter, SplinterRef};
use trackerr::{CallerLocation, LocationStack};

type Segment = u8;

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {ty}; needs {size} bytes")]
    InvalidLength {
        ty: &'static str,
        size: usize,
        loc: CallerLocation,
    },

    #[error("Invalid magic number")]
    InvalidMagic(CallerLocation),
}

impl LocationStack for DecodeErr {
    fn location(&self) -> &CallerLocation {
        match self {
            DecodeErr::InvalidLength { loc, .. } => loc,
            DecodeErr::InvalidMagic(loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        None
    }
}
