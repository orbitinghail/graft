use std::fmt::Debug;

use culprit::Result;
use graft_core::{VolumeId, lsn::LSN};

use crate::{GraftErr, local::fjall_storage::FjallStorage, remote::Remote};

/// Downloads all missing pages for a Volume up to an optional maximum LSN.
/// If max_lsn is not specified, will hydrate the Volume up to its latest snapshot.
pub struct Opts {
    /// The Volume to hydrate.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("HydrateVolume");
        out.field("vid", &self.vid);
        if let Some(max_lsn) = self.max_lsn {
            out.field("max_lsn", &max_lsn.to_string());
            out.finish()
        } else {
            out.finish_non_exhaustive()
        }
    }
}

pub async fn run(storage: &FjallStorage, remote: &Remote, opts: Opts) -> Result<(), GraftErr> {
    todo!()
}
