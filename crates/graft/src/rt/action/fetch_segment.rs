use std::fmt::Debug;
use std::sync::Arc;

use crate::core::commit::SegmentRangeRef;

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
    async fn run(self, storage: Arc<FjallStorage>, remote: Arc<Remote>) -> Result<()> {
        let bytes = remote
            .get_segment_range(&self.range.sid, self.range.bytes)
            .await?;
        let pageidxs = self.range.pageset.iter();
        let pages = segment_frame_iter(&bytes);
        let mut batch = storage.batch();
        for (pageidx, page) in pageidxs.zip(pages) {
            batch.write_page(self.range.sid.clone(), pageidx, page);
        }
        batch.commit()?;
        Ok(())
    }
}
