use std::fmt::Debug;

use culprit::ResultExt;

use crate::{GraftErr, local::fjall_storage::FjallStorage, volume_name::VolumeName};

/// Fast-forwards the local volume to include any remote commits. Fails if
/// the local volume has unpushed commits.
pub struct Opts {
    /// Name of the volume to sync.
    pub name: VolumeName,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRemoteToLocal")
            .field("name", &self.name.to_string())
            .finish()
    }
}

pub async fn run(storage: &FjallStorage, opts: Opts) -> culprit::Result<(), GraftErr> {
    storage.sync_remote_to_local(opts.name).or_into_ctx()
}
