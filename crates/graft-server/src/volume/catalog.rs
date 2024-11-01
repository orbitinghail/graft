use std::{fmt::Debug, path::Path};

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
    commit::{Commit, OffsetsValidationErr},
    kv::{SegmentKey, SegmentKeyPrefix, Snapshot},
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

    /// maps VolumeId to kv::Snapshot { lsn, last_offset }
    volumes: Partition,

    /// maps kv::SegmentKey { vid, lsn, sid} to OffsetSet
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

    /// Return the latest snapshot for the specified Volume.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn snapshot(&self, vid: &VolumeId) -> Result<Option<Snapshot>> {
        self.volumes
            .get(vid)?
            .map(|bytes| {
                Snapshot::read_from_bytes(&bytes)
                    .map_err(|_| VolumeCatalogErr::DecodeErr { target: "Snapshot" })
            })
            .transpose()
    }

    /// Query the catalog for segments in the specified Volume. Segments are
    /// scanned in reverse order starting from the specified LSN.
    pub fn query_segments(
        &self,
        vid: VolumeId,
        lsn: LSN,
    ) -> impl Iterator<Item = Result<(SegmentId, SplinterRef<Slice>)>> {
        let scan = self.segments.range(SegmentKeyPrefix::range(vid, lsn)).rev();
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
        self.batch.insert(
            &self.volumes,
            commit.vid(),
            Snapshot::new(commit.lsn(), commit.last_offset()),
        );

        let mut iter = commit.iter_offsets();
        while let Some((sid, offsets)) = iter.next().transpose()? {
            let key = SegmentKey::new(commit.vid().clone(), commit.lsn(), sid.clone());
            self.batch.insert(&self.segments, key, offsets.inner());
        }

        Ok(())
    }

    pub fn insert_snapshot(
        &mut self,
        vid: VolumeId,
        snapshot: Snapshot,
        segments: Vec<SegmentInfo>,
    ) -> Result<()> {
        self.batch.insert(&self.volumes, &vid, &snapshot);
        for segment in segments {
            let key = SegmentKey::new(vid.clone(), snapshot.lsn(), segment.sid.try_into()?);
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
