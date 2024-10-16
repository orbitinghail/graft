use thiserror::Error;

mod bitmap;
mod block;
mod index;
mod partition;
mod splinter;

type Segment = u8;

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {ty}; needs {size} bytes")]
    InvalidLength { ty: &'static str, size: usize },

    #[error("Invalid magic number")]
    InvalidMagic,
}
