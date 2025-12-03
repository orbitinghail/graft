use std::fmt::Debug;

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

pub type Result<T> = culprit::Result<T, GraftErr>;

/// A one-off async action.
pub trait Action: Debug {
    /// Run the action.
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> Result<()>;
}
