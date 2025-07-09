use graft_core::snapshot::Snapshot;

use crate::search_path::SearchPath;

pub struct TrackedSnapshot {
    snapshot: Snapshot,
    search: SearchPath,
}
