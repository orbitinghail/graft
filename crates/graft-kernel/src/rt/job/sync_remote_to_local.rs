use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr,
    volume_name::VolumeName,
};

/// Fast-forwards the local volume to include any remote commits. Fails if
/// the local volume has unpushed commits, unless `force` is specified.
pub struct Opts {
    /// Name of the volume to sync.
    pub name: VolumeName,

    /// If true, discards any unpushed local commits before syncing.
    pub force: bool,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    todo!()
}
