//! A Segment writer is a task which builds segments and passes them onto the
//! Segment uploader.

use super::open::OpenSegment;

pub async fn segment_writer() {
    let open = OpenSegment::default();
}
