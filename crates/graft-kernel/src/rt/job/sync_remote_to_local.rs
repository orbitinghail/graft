use std::fmt::Debug;

use culprit::ResultExt;
use graft_core::VolumeId;

use crate::{KernelErr, local::fjall_storage::FjallStorage};

/// Fast-forwards the local volume to include any remote commits. Fails if
/// the local volume has unpushed commits.
pub struct Opts {
    /// Name of the volume to sync.
    pub graft: VolumeId,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRemoteToLocal")
            .field("graft", &self.graft)
            .finish()
    }
}

pub async fn run(storage: &FjallStorage, opts: Opts) -> culprit::Result<(), KernelErr> {
    storage.sync_remote_to_local(opts.graft).or_into_ctx()
}
