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
*
*
fn run_zstd(frame_data: &[u8]) {
    let mut compressed = File::create("data.zstd").unwrap();
    let mut compressor = Compressor::new(3).unwrap();

    compressor.include_checksum(true).unwrap();
    compressor.include_contentsize(true).unwrap();

    let mut buf = Vec::with_capacity(compress_bound(frame_data.len()));

    for _ in 0..FRAMES {
        buf.clear();
        compressor
            .compress_to_buffer(&frame_data, &mut buf)
            .unwrap();
        compressed.write_all(&buf).unwrap();
    }

    compressed.flush().unwrap();
}
*/

/// The maximum number of pages per Frame
const FRAME_MAX_PAGES: usize = 64;

pub struct SegmentBuilder {}

struct FrameBuilder {}
