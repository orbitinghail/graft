use graft_core::codec;

#[derive(Debug, thiserror::Error)]
pub enum StorageErr {
    #[error("Fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("Codec error: {0}")]
    CodecDecodeErr(#[from] codec::DecodeErr),
}

pub trait Storage {}
