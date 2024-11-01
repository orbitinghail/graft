use std::{sync::Arc, time::SystemTime};

use bytes::Bytes;
use graft_core::{
    lsn::{self, LSN},
    VolumeId,
};
use object_store::{path::Path, ObjectStore};

use super::commit::CommitBuilder;

pub struct VolumeStore<O> {
    store: Arc<O>,
}

impl<O: ObjectStore> VolumeStore<O> {
    pub fn new(store: Arc<O>) -> Self {
        Self { store }
    }

    pub fn prepare(&self, vid: VolumeId, lsn: LSN, last_offset: u32) -> CommitBuilder {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        CommitBuilder::new(vid, lsn, last_offset, timestamp)
    }

    pub async fn commit(&self, commit: CommitBuilder) -> anyhow::Result<()> {
        let (vid, lsn, commit) = commit.freeze();
        let path = Path::from(format!("volumes/{}/{}", vid.pretty(), encode_lsn(lsn)));
        self.store.put(&path, commit.into()).await?;
        Ok(())
    }

    pub fn replay(&self, vid: VolumeId) {
        todo!("returns a stream of commits")
    }
}

fn encode_lsn(lsn: LSN) -> String {
    format!("{:0>18x}", lsn)
}
