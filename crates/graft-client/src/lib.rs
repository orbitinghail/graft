mod builder;
mod error;
mod metastore;
mod pagestore;
mod pair;
mod request;

mod runtime {
    pub mod handle;
    pub mod storage;
    pub mod sync;
    pub mod txn;
}

pub use builder::{ClientBuildErr, ClientBuilder};
pub use error::ClientErr;
pub use metastore::MetastoreClient;
pub use pagestore::PagestoreClient;
pub use pair::ClientPair;
pub use runtime::handle::RuntimeHandle;
