use graft_core::{VolumeId, lsn::LSN};

use crate::{
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{err::RuntimeErr, job::Job},
};

/// Pulls commits and metadata from a remote.
pub struct Opts {
    /// The Volume to fetch.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<Option<Job>, RuntimeErr> {
    todo!()
}
