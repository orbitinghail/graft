//! The cache manages on disk and mem-mapped segments.
//! The cache needs to respect the following limits:
//!   - Disk space
//!   - Maximum open file descriptors (maximum mmap'ed segments)

use std::{future::Future, io, ops::Deref};

use bytes::Buf;
use graft_core::SegmentId;

pub mod atomic_file;
pub mod disk;
pub mod mem;

pub trait Cache: Send + Sync {
    type Item<'a>: Deref<Target = [u8]>
    where
        Self: 'a;

    fn put<T: Buf + Send + 'static>(
        &self,
        sid: &SegmentId,
        data: T,
    ) -> impl Future<Output = io::Result<()>> + Send;

    fn get(
        &self,
        sid: &SegmentId,
    ) -> impl Future<Output = io::Result<Option<Self::Item<'_>>>> + Send;
}
