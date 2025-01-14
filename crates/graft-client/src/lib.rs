mod builder;
mod error;
mod metastore;
mod net;
mod pagestore;
mod pair;

pub mod runtime {
    pub mod fetcher;
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
