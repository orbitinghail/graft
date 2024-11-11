use std::{fmt::Debug, ops::RangeBounds, path::Path};

use fjall::{
    Batch, Config, Keyspace, KvSeparationOptions, Partition, PartitionCreateOptions, Slice,
};
use graft_core::{
    gid::{SegmentId, VolumeId},
    lsn::LSN,
};
use graft_proto::common::v1::SegmentInfo;
use splinter::SplinterRef;
use zerocopy::{FromBytes, TryFromBytes};

use super::{
    commit::{Commit, CommitMeta, OffsetsValidationErr},
    kv::{CommitKey, SegmentKey},
};

#[derive(Debug, thiserror::Error)]
pub enum VolumeCatalogErr {
    #[error(transparent)]
    GidParseErr(#[from] graft_core::gid::GidParseErr),

    #[error(transparent)]
    FjallErr(#[from] fjall::Error),

    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error("Failed to decode entry into type {target}")]
    DecodeErr { target: &'static str },

    #[error(transparent)]
    SplinterErr(#[from] splinter::DecodeErr),

    #[error(transparent)]
    OffsetsValidationErr(#[from] OffsetsValidationErr),
}

type Result<T> = std::result::Result<T, VolumeCatalogErr>;

#[derive(Clone)]
pub struct VolumeCatalog {
    keyspace: Keyspace,

    /// maps kv::CommitKey { vid, lsn } to CommitMeta { lsn, last_offset, timestamp }
    volumes: Partition,

    /// maps kv::SegmentKey { CommitKey { vid, lsn }, sid} to OffsetSet
    segments: Partition,
}

impl VolumeCatalog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_config(Config::new(path))
    }

    pub fn open_temporary() -> Result<Self> {
        Self::open_config(Config::new(tempfile::tempdir()?).temporary(true))
    }

    pub fn open_config(config: Config) -> Result<Self> {
        let keyspace = config.open()?;

        let volumes = keyspace.open_partition("volumes", PartitionCreateOptions::default())?;

        let segments = keyspace.open_partition(
            "segments",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;

        Ok(Self { keyspace, volumes, segments })
    }

    pub fn batch_insert(&self) -> VolumeCatalogBatch {
        VolumeCatalogBatch {
            batch: self.keyspace.batch(),
            volumes: self.volumes.clone(),
            segments: self.segments.clone(),
        }
    }

    pub fn contains_snapshot(&self, vid: VolumeId, lsn: LSN) -> Result<bool> {
        Ok(self.volumes.contains_key(CommitKey::new(vid, lsn))?)
    }

    pub fn contains_range<R: RangeBounds<LSN>>(&self, vid: &VolumeId, lsns: &R) -> Result<bool> {
        let range = CommitKey::range(vid, lsns);

        // verify that lsns in the range are contiguous
        let mut cursor = range.start.lsn();
        let mut empty = true;

        for kv in self.volumes.range(range) {
            let (key, _) = kv?;
            let key = CommitKey::try_ref_from_bytes(&key)
                .map_err(|_| VolumeCatalogErr::DecodeErr { target: "CommitKey" })?;
            if key.lsn() != cursor {
                return Ok(false);
            }
            cursor += 1;
            empty = false;
        }
        Ok(!empty)
    }

    /// Return the snapshot for the specified Volume at the provided LSN.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn snapshot(&self, vid: VolumeId, lsn: LSN) -> Result<Option<CommitMeta>> {
        self.volumes
            .get(CommitKey::new(vid, lsn))?
            .map(|bytes| {
                CommitMeta::read_from_bytes(&bytes)
                    .map_err(|_| VolumeCatalogErr::DecodeErr { target: "CommitMeta" })
            })
            .transpose()
    }

    /// Return the latest snapshot for the specified Volume.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn latest_snapshot(&self, vid: &VolumeId) -> Result<Option<CommitMeta>> {
        self.volumes
            .prefix(vid)
            .next_back()
            .transpose()?
            .map(|(_, bytes)| {
                CommitMeta::read_from_bytes(&bytes)
                    .map_err(|_| VolumeCatalogErr::DecodeErr { target: "CommitMeta" })
            })
            .transpose()
    }

    /// Query the catalog for segments in the specified Volume. Segments are
    /// scanned in reverse order by LSN.
    pub fn query_segments<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> impl Iterator<Item = Result<(SegmentId, SplinterRef<Slice>)>> {
        let range = CommitKey::range(vid, lsns);
        let scan = self.segments.range(range).rev();
        SegmentsQueryIter { scan }
    }
}

pub struct VolumeCatalogBatch {
    batch: Batch,
    volumes: Partition,
    segments: Partition,
}

impl VolumeCatalogBatch {
    pub fn insert_commit(&mut self, commit: Commit) -> Result<()> {
        let commit_key = CommitKey::new(commit.vid().clone(), commit.meta().lsn());

        self.batch.insert(&self.volumes, &commit_key, commit.meta());

        let mut iter = commit.iter_offsets();
        while let Some((sid, offsets)) = iter.next().transpose()? {
            let key = SegmentKey::new(commit_key.clone(), sid.clone());
            self.batch.insert(&self.segments, key, offsets.inner());
        }

        Ok(())
    }

    pub fn insert_snapshot(
        &mut self,
        vid: VolumeId,
        snapshot: CommitMeta,
        segments: Vec<SegmentInfo>,
    ) -> Result<()> {
        let commit_key = CommitKey::new(vid, snapshot.lsn());

        self.batch.insert(&self.volumes, &commit_key, &snapshot);
        for segment in segments {
            let key = SegmentKey::new(commit_key.clone(), segment.sid.try_into()?);
            self.batch.insert(&self.segments, key, segment.offsets);
        }
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.batch.commit()?;
        Ok(())
    }
}

pub struct SegmentsQueryIter<I> {
    scan: I,
}

impl<I: Iterator<Item = fjall::Result<(Slice, Slice)>>> SegmentsQueryIter<I> {
    fn next_inner(
        &mut self,
        entry: fjall::Result<(Slice, Slice)>,
    ) -> Result<(SegmentId, SplinterRef<Slice>)> {
        let (key, value) = entry?;
        let key = SegmentKey::try_read_from_bytes(&key)
            .map_err(|_| VolumeCatalogErr::DecodeErr { target: "SegmentKey" })?;
        let val = SplinterRef::from_bytes(value)?;
        Ok((key.sid().clone(), val))
    }
}

impl<I: Iterator<Item = fjall::Result<(Slice, Slice)>>> Iterator for SegmentsQueryIter<I> {
    type Item = Result<(SegmentId, SplinterRef<Slice>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next().map(|entry| self.next_inner(entry))
    }
}
