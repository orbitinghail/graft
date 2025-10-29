use std::fmt::Debug;

use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr,
    volume_name::VolumeName,
};

/// Resumes from an interrupted `Job::RemoteCommit`. This job should be
/// triggered when a `NamedVolume` has a `pending_commit` and no `RemoteCommit`
/// operation is in progress.
pub struct Opts {
    pub name: VolumeName,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoverPendingCommit")
            .field("name", &self.name.to_string())
            .finish()
    }
}

pub async fn run(
    _storage: &FjallStorage,
    _remote: &Remote,
    _opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    todo!()
}
