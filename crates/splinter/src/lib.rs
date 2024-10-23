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

type Segment = u8;

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {ty}; needs {size} bytes")]
    InvalidLength { ty: &'static str, size: usize },

    #[error("Invalid magic number")]
    InvalidMagic,
}
