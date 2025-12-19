use std::{fmt::Debug, sync::Arc};

use crate::{GraftErr, local::fjall_storage::FjallStorage, remote::Remote};

macro_rules! action {
    ($mod:tt, $action:ident) => {
        mod $mod;
        pub use $mod::$action;
    };
}

action!(fetch_segment, FetchSegment);
action!(fetch_log, FetchLog);
action!(hydrate_snapshot, HydrateSnapshot);
action!(remote_commit, RemoteCommit);

pub type Result<T> = std::result::Result<T, GraftErr>;

/// A one-off async action.
pub trait Action: Debug {
    /// Run the action.
    async fn run(self, storage: Arc<FjallStorage>, remote: Arc<Remote>) -> Result<()>;
}
