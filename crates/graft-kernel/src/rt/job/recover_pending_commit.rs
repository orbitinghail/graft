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

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    todo!()
}
