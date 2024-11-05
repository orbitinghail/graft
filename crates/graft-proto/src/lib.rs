include!("mod.rs");

use std::time::SystemTime;

use bytes::Bytes;
use common::v1::Snapshot;
use graft_core::{gid::GidParseErr, lsn::LSN, VolumeId};
use prost_types::TimestampError;
use zerocopy::IntoBytes;

pub use graft::*;

impl Snapshot {
    pub fn new(vid: &VolumeId, lsn: LSN, last_offset: u32, timestamp: SystemTime) -> Self {
        Self {
            vid: Bytes::copy_from_slice(vid.as_bytes()),
            lsn,
            last_offset,
            timestamp: Some(timestamp.into()),
        }
    }

    pub fn vid(&self) -> Result<&VolumeId, GidParseErr> {
        self.vid.as_ref().try_into()
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn last_offset(&self) -> u32 {
        self.last_offset
    }

    pub fn system_time(&self) -> Option<Result<SystemTime, TimestampError>> {
        self.timestamp.map(|ts| ts.try_into())
    }
}
