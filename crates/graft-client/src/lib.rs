mod builder;
mod error;
mod metastore;
mod pagestore;
mod request;

mod runtime {
    mod handle;
    mod storage;
}

pub use builder::{ClientBuildErr, ClientBuilder};
pub use error::ClientErr;
pub use metastore::MetastoreClient;
pub use pagestore::PagestoreClient;
