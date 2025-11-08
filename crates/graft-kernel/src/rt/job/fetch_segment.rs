use std::fmt::Debug;

use culprit::{Result, ResultExt};
use graft_core::{SegmentId, commit::SegmentRangeRef};

use crate::{
    KernelErr,
    local::fjall_storage::FjallStorage,
    remote::{Remote, segment::segment_frame_iter},
};

/// Fetches one or more Segment frames and loads the pages into Storage.
pub struct Opts {
    /// The Segment we are fetching
    pub sid: SegmentId,

    /// The subset of the Segment to retrieve.
    pub frame: SegmentRangeRef,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchSegment")
            .field("sid", &self.sid)
            .field("frame", &self.frame)
            .finish()
    }
}

pub async fn run(storage: &FjallStorage, remote: &Remote, opts: Opts) -> Result<(), KernelErr> {
    let bytes = remote
        .get_segment_range(&opts.sid, &opts.frame.bytes)
        .await
        .or_into_ctx()?;
    let pages = segment_frame_iter(opts.frame.pageset.iter(), &bytes);
    let mut batch = storage.batch();
    for (pageidx, page) in pages {
        batch.write_page(opts.sid.clone(), pageidx, page);
    }
    batch.commit().or_into_ctx()?;
    Ok(())
}
