/*
* Segments are sequences of compressed ZStd frames. All pages in a Segment is stored
* in order by `PageIdx`.
*/

use bytes::{Bytes, BytesMut};
use graft_core::{
    PageCount, PageIdx,
    commit::SegmentFrameIdx,
    page::{PAGESIZE, Page},
};
use smallvec::SmallVec;
use zstd::zstd_safe::{CCtx, CParameter, DCtx, InBuffer, OutBuffer, zstd_sys::ZSTD_EndDirective};

/// The maximum number of pages per Frame.
/// At 4k per page this is 256k
const FRAME_MAX_PAGES: PageCount = PageCount::new(64);

/// The ZSTD compression level
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

pub struct SegmentBuilder {
    /// index of compressed frames
    frames: SmallVec<[SegmentFrameIdx; 1]>,

    /// chunks of the resulting segment. each chunk represents a portion of the
    /// compressed stream of frames
    chunks: SmallVec<[Bytes; 1]>,

    /// the compression context
    cctx: CCtx<'static>,

    /// the last pageidx; used to ensure pages are pushed in order and to build
    /// the frame index
    last_pageidx: Option<PageIdx>,

    /// the number of pages written to the current frame
    current_frame_pages: PageCount,

    /// the compressed size of current frame
    current_frame_bytes: usize,

    /// the active chunk
    chunk: Vec<u8>,
}

impl Default for SegmentBuilder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentBuilder {
    pub fn new() -> Self {
        let mut cctx = CCtx::create();
        cctx.set_parameter(CParameter::ContentSizeFlag(false))
            .expect("BUG: failed to set content size flag");
        cctx.set_parameter(CParameter::ChecksumFlag(true))
            .expect("BUG: failed to set checksum flag");
        cctx.set_parameter(CParameter::CompressionLevel(ZSTD_COMPRESSION_LEVEL))
            .expect("BUG: failed to set compression level");
        Self {
            frames: Default::default(),
            chunks: Default::default(),
            cctx,
            last_pageidx: None,
            current_frame_pages: PageCount::ZERO,
            current_frame_bytes: 0,
            chunk: Vec::with_capacity(CCtx::out_size()),
        }
    }

    fn flush_chunk(&mut self) {
        let chunk = std::mem::replace(&mut self.chunk, Vec::with_capacity(CCtx::out_size()));
        self.chunks.push(chunk.into());
    }

    pub fn write(&mut self, pageidx: PageIdx, page: Page) {
        if let Some(last_pageidx) = self.last_pageidx.replace(pageidx) {
            assert!(pageidx > last_pageidx, "Pages must be pushed in order")
        }

        let mut in_buf = InBuffer::around(page.as_ref());

        while in_buf.pos() < PAGESIZE {
            let start_pos = self.chunk.len();
            let mut out_buf = OutBuffer::around_pos(&mut self.chunk, start_pos);

            let pending_flush = self
                .cctx
                .compress_stream2(
                    &mut out_buf,
                    &mut in_buf,
                    ZSTD_EndDirective::ZSTD_e_continue,
                )
                .expect("BUG: failed to compress frame");

            self.current_frame_bytes += out_buf.pos() - start_pos;

            if pending_flush > 0 && out_buf.pos() == out_buf.capacity() {
                // output buffer is full, swap chunks
                self.flush_chunk();
            }
        }

        self.current_frame_pages = self.current_frame_pages.saturating_incr();

        if self.current_frame_pages >= FRAME_MAX_PAGES {
            self.end_frame();
        }
    }

    fn end_frame(&mut self) {
        let mut in_buf = InBuffer::around(&[]);
        loop {
            let start_pos = self.chunk.len();
            let mut out_buf = OutBuffer::around_pos(&mut self.chunk, start_pos);

            let pending_flush = self
                .cctx
                .compress_stream2(&mut out_buf, &mut in_buf, ZSTD_EndDirective::ZSTD_e_end)
                .expect("BUG: failed to compress frame");

            self.current_frame_bytes += out_buf.pos() - start_pos;

            if pending_flush > 0 && out_buf.pos() == out_buf.capacity() {
                // output buffer is full, swap chunks
                self.flush_chunk();
            } else if pending_flush == 0 {
                break;
            }
        }

        // record the frame
        self.frames.push(SegmentFrameIdx::new(
            self.current_frame_bytes,
            self.last_pageidx.expect("BUG: flushing empty frame"),
        ));

        // reset current frame vars
        self.current_frame_bytes = 0;
        self.current_frame_pages = PageCount::ZERO;
        self.cctx
            .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
            .expect("BUG: failed to reset context");
    }

    pub fn finish(mut self) -> (SmallVec<[SegmentFrameIdx; 1]>, SmallVec<[Bytes; 1]>) {
        // flush the last frame if needed
        if self.current_frame_pages > 0 {
            self.end_frame();
        }

        let Self { mut chunks, chunk, frames, .. } = self;

        // flush the last chunk if it's non-empty
        if !chunk.is_empty() {
            chunks.push(chunk.into());
        }

        (frames, chunks)
    }
}

#[must_use]
pub struct SegmentFrameIter<'a> {
    dctx: DCtx<'static>,
    in_buf: InBuffer<'a>,
}

impl<'a> SegmentFrameIter<'a> {
    pub fn from_bytes(frame: &'a [u8]) -> Self {
        let dctx = DCtx::create();
        let in_buf = InBuffer::around(frame);
        SegmentFrameIter { dctx, in_buf }
    }
}

impl Iterator for SegmentFrameIter<'_> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        let mut page = BytesMut::with_capacity(PAGESIZE.as_usize());
        let mut out_buf = OutBuffer::around(page.as_mut());

        while out_buf.pos() < out_buf.capacity() {
            let n = self
                .dctx
                .decompress_stream(&mut out_buf, &mut self.in_buf)
                .expect("BUG: failed to decompress segment frame");
            assert!(n > 0, "BUG: reached end of frame before filling page");
        }

        // SAFETY: we just allocated page to be PAGESIZE bytes
        Some(unsafe { Page::from_bytes_unchecked(page.freeze()) })
    }
}

#[cfg(test)]
mod test {
    use graft_core::pageidx;

    use super::*;

    #[test]
    fn test_empty_segment() {
        let segment = SegmentBuilder::new();
        let (frames, chunks) = segment.finish();
        assert_eq!(frames.len(), 0);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn test_segment() {
        let mut segment = SegmentBuilder::new();

        // Push 1.5 frames worth of pages
        for i in 1..=96 {
            segment.write(PageIdx::must_new(i), Page::test_filled(i as u8));
        }

        // Finish the segment
        let (frames, chunks) = segment.finish();

        // Check the frames and chunks
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].last_pageidx(), pageidx!(64));
        assert_eq!(frames[1].last_pageidx(), pageidx!(96));
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].len(),
            frames[0].frame_size() + frames[1].frame_size()
        );
    }
}
