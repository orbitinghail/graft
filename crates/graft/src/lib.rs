pub mod local {
    pub mod fjall_storage;
    pub mod grove;
}

pub mod rt {
    pub mod runtime;

    mod action;
    mod task;
}

pub mod core;
pub mod err;
pub mod oracle;
pub mod remote;
pub mod setup;
pub mod snapshot;
pub mod volume;
pub mod volume_reader;
pub mod volume_writer;

#[cfg(any(test, feature = "testutil"))]
pub mod testutil;

#[cfg(feature = "precept")]
pub mod fault;

pub use err::{GraftErr, LogicalErr};

// re-export static_assertions for macros
#[doc(hidden)]
pub use static_assertions;
