use std::{fmt::Debug, path::Path};

use fjall::{Config, Keyspace, KvSeparationOptions, Partition, PartitionCreateOptions, Slice};
use graft_core::{
    guid::{SegmentId, VolumeId},
    lsn::LSN,
};
use graft_proto::common::v1::SegmentInfo;
use splinter::SplinterRef;
use zerocopy::FromBytes;

use super::kv::{SegmentKey, SegmentKeyPrefix, Snapshot};

#[derive(Debug, thiserror::Error)]
pub enum VolumeCatalogError {
    #[error(transparent)]
    GuidParseError(#[from] graft_core::guid::GuidParseError),

    #[error(transparent)]
    FjallError(#[from] fjall::Error),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Failed to decode entry into type {target}")]
    DecodeError { target: &'static str },

    #[error(transparent)]
    SplinterError(#[from] splinter::DecodeErr),
}

type Result<T> = std::result::Result<T, VolumeCatalogError>;

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

    pub fn open_config(config: Config) -> Result<Self> {
        let keyspace = config.open()?;

        let volumes = keyspace.open_partition("volumes", PartitionCreateOptions::default())?;

        let segments = keyspace.open_partition(
            "segments",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;

        Ok(Self { keyspace, volumes, segments })
    }

    pub fn open_temporary() -> Result<Self> {
        Self::open_config(Config::new(tempfile::tempdir()?).temporary(true))
    }

    pub fn update_volume(
        &self,
        vid: VolumeId,
        snapshot: Snapshot,
        segments: Vec<SegmentInfo>,
    ) -> Result<()> {
        let mut batch = self.keyspace.batch();

        batch.insert(&self.volumes, &vid, &snapshot);

        for segment in segments {
            let key = SegmentKey::new(vid.clone(), snapshot.lsn(), segment.sid.try_into()?);
            batch.insert(&self.segments, key, segment.offsets);
        }

        batch.commit()?;

        Ok(())
    }

    /// Return the latest snapshot for the specified Volume.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn snapshot(&self, vid: &VolumeId) -> Result<Option<Snapshot>> {
        self.volumes
            .get(vid)?
            .map(|bytes| {
                Snapshot::read_from_bytes(&bytes)
                    .map_err(|_| VolumeCatalogError::DecodeError { target: "Snapshot" })
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

pub struct SegmentsQueryIter<I> {
    scan: I,
}

impl<I: Iterator<Item = fjall::Result<(Slice, Slice)>>> SegmentsQueryIter<I> {
    fn next_inner(
        &mut self,
        entry: fjall::Result<(Slice, Slice)>,
    ) -> Result<(SegmentId, SplinterRef<Slice>)> {
        let (key, value) = entry?;
        let key = SegmentKey::read_from_bytes(&key)
            .map_err(|_| VolumeCatalogError::DecodeError { target: "SegmentKey" })?;
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
