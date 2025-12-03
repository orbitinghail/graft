use std::fmt::Debug;

use crate::core::commit::SegmentRangeRef;
use culprit::ResultExt;

use crate::{
    local::fjall_storage::FjallStorage,
    remote::{Remote, segment::segment_frame_iter},
    rt::action::{Action, Result},
};

/// Fetches one or more Segment frames and loads the pages into Storage.
#[derive(Debug)]
pub struct FetchSegment {
    pub range: SegmentRangeRef,
}

impl Action for FetchSegment {
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> Result<()> {
        let bytes = remote
            .get_segment_range(&self.range.sid, &self.range.bytes)
            .await
            .or_into_ctx()?;
        let pages = segment_frame_iter(self.range.pageset.iter(), &bytes);
        let mut batch = storage.batch();
        for (pageidx, page) in pages {
            batch.write_page(self.range.sid.clone(), pageidx, page);
        }
        batch.commit().or_into_ctx()?;
        Ok(())
    }
}
