/*
 * Segments are sequences of compressed frames. All pages in a Segment is stored in order by `PageIdx`.
 * Consider using zstd's trailing checksums.
 * Building a Segment needs to also emit a SegmentIdx
 *
 * https://github.com/carlsverre/rust-compression-playground
 *
 * Ok so my plan is to go with compressing frames at a time using zstd directly.
 * I need to make sure this integrates well into the object storage upload api.
 * I think an iterator of frames should work well for that.
 */

/// The maximum number of pages per Frame
const FRAME_MAX_PAGES: usize = 64;

pub struct SegmentBuilder {}

struct FrameBuilder {}
