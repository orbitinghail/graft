use std::ops::RangeInclusive;

use graft_core::lsn::LSN;

use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr, snapshot::Snapshot,
    volume_name::VolumeName,
};

/// Commits a Named Volume's local changes into its remote.
///
/// This process involves the following stages:
///
/// 1. prepare commit
/// 2. push segment
/// 3. commit
/// 4. update named volume (on success or failure)
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

struct CommitState {
    local: Snapshot,
    local_lsns: RangeInclusive<LSN>,
    remote: Snapshot,
    commit_lsn: LSN,
}

async fn prepare_commit(
    storage: &FjallStorage,
    name: VolumeName,
) -> culprit::Result<CommitState, RuntimeErr> {
    // grab snapshots
    // verify pre-commit invariants
    // calculate sync lsns
    todo!()
}
