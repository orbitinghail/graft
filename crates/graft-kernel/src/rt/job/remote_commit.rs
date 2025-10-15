use crate::{
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{err::RuntimeErr, job::Job},
    volume_name::VolumeName,
};

/// Commits a Named Volume's local changes into its remote.
pub struct Opts {
    pub name: VolumeName,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<Option<Job>, RuntimeErr> {
    todo!()
}
