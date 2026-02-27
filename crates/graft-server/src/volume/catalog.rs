use std::{
    fmt::Debug,
    io,
    ops::RangeBounds,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use fjall::{
    Batch, Config, Keyspace, KvSeparationOptions, Partition, PartitionCreateOptions, Slice,
};
use graft_core::{gid::VolumeId, lsn::LSN, zerocopy_err::ZerocopyErr};
use graft_proto::common::v1::SegmentInfo;
use serde::{Deserialize, Serialize};
use splinter::SplinterRef;
use tryiter::TryIteratorExt;
use zerocopy::{ConvertError, IntoBytes, SizeError, TryFromBytes};

use super::{
    commit::{Commit, CommitMeta, OffsetsValidationErr},
    kv::{CommitKey, SegmentKey},
};

#[derive(Debug, thiserror::Error)]
pub enum VolumeCatalogErr {
    #[error("failed to parse Gid")]
    GidParseErr(#[from] graft_core::gid::GidParseErr),

    #[error("fjall error")]
    FjallErr,

    #[error("io error")]
    IoErr(std::io::ErrorKind),

    #[error("Failed to decode entry")]
    DecodeErr(#[from] ZerocopyErr),

    #[error("splinter error")]
    SplinterErr(#[from] splinter::DecodeErr),

    #[error("offsets validation error")]
    OffsetsValidationErr(#[from] OffsetsValidationErr),
}

impl<A, S, V> From<ConvertError<A, S, V>> for VolumeCatalogErr {
    fn from(err: ConvertError<A, S, V>) -> Self {
        VolumeCatalogErr::DecodeErr(err.into())
    }
}

impl<A, B> From<SizeError<A, B>> for VolumeCatalogErr {
    fn from(err: SizeError<A, B>) -> Self {
        VolumeCatalogErr::DecodeErr(err.into())
    }
}

impl From<fjall::Error> for VolumeCatalogErr {
    fn from(_: fjall::Error) -> Self {
        VolumeCatalogErr::FjallErr
    }
}

impl From<lsm_tree::Error> for VolumeCatalogErr {
    fn from(_: lsm_tree::Error) -> Self {
        VolumeCatalogErr::FjallErr
    }
}

impl From<io::Error> for VolumeCatalogErr {
    fn from(err: io::Error) -> Self {
        VolumeCatalogErr::IoErr(err.kind())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct VolumeCatalogConfig {
    /// path to the directory where the catalog will be stored
    /// if not provided, a temporary directory will be created
    pub path: Option<PathBuf>,
}

impl TryFrom<VolumeCatalogConfig> for Config {
    type Error = Culprit<VolumeCatalogErr>;

    fn try_from(value: VolumeCatalogConfig) -> std::result::Result<Self, Self::Error> {
        let (path, temporary) = if let Some(path) = value.path {
            (path, false)
        } else {
            (tempfile::tempdir()?.into_path(), true)
        };
        Ok(Config::new(path).temporary(temporary))
    }
}

#[derive(Clone)]
pub struct VolumeCatalog {
    keyspace: Keyspace,

    /// maps kv::CommitKey { vid, lsn } to CommitMeta { lsn, page_count, timestamp }
    volumes: Partition,

    /// maps kv::SegmentKey { CommitKey { vid, lsn }, sid} to OffsetSet
    segments: Partition,
}

impl VolumeCatalog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Culprit<VolumeCatalogErr>> {
        Self::open_config(VolumeCatalogConfig { path: Some(path.as_ref().to_path_buf()) })
    }

    pub fn open_temporary() -> Result<Self, Culprit<VolumeCatalogErr>> {
        Self::open_config(VolumeCatalogConfig { path: None })
    }

    pub fn open_config(config: VolumeCatalogConfig) -> Result<Self, Culprit<VolumeCatalogErr>> {
        let config: Config = config.try_into()?;
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

    pub fn contains_snapshot(
        &self,
        vid: VolumeId,
        lsn: LSN,
    ) -> Result<bool, Culprit<VolumeCatalogErr>> {
        Ok(self.volumes.contains_key(CommitKey::new(vid, lsn))?)
    }

    pub fn contains_range<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> Result<bool, Culprit<VolumeCatalogErr>> {
        let range = CommitKey::range(vid, lsns);

        // verify that lsns in the range are contiguous
        let mut cursor = range.start.lsn();
        let mut empty = true;

        for kv in self.volumes.snapshot().range(range) {
            let (key, _) = kv?;
            let key = CommitKey::try_ref_from_bytes(&key)
                .or_into_culprit("failed to decode CommitKey")?;
            if key.lsn() != cursor {
                return Ok(false);
            }
            cursor = cursor.saturating_next();
            empty = false;
        }
        Ok(!empty)
    }

    /// Return the snapshot for the specified Volume at the provided LSN.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn snapshot(
        &self,
        vid: VolumeId,
        lsn: LSN,
    ) -> Result<Option<CommitMeta>, Culprit<VolumeCatalogErr>> {
        if let Some(bytes) = self.volumes.get(CommitKey::new(vid, lsn))? {
            Ok(Some(
                CommitMeta::try_read_from_bytes(&bytes)
                    .or_into_culprit("failed to decode CommitMeta")?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Return the latest snapshot for the specified Volume.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn latest_snapshot(
        &self,
        vid: &VolumeId,
    ) -> Result<Option<CommitMeta>, Culprit<VolumeCatalogErr>> {
        self.volumes
            .snapshot()
            .prefix(vid)
            .rev()
            .err_into()
            .map_ok(|(_, bytes)| {
                CommitMeta::try_read_from_bytes(&bytes)
                    .or_into_culprit("failed to decode CommitMeta")
            })
            .try_next()
    }

    /// scan the catalog for segments in the specified Volume. Segments are
    /// scanned in reverse order by LSN.
    pub fn scan_segments<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> impl Iterator<Item = Result<(SegmentKey, SplinterRef<Bytes>), Culprit<VolumeCatalogErr>>>
    {
        let range = CommitKey::range(vid, lsns);
        let scan = self.segments.snapshot().range(range).rev();
        SegmentsQueryIter { scan }
    }

    /// scan the catalog for commits in the specified Volume in order by lsn
    #[allow(clippy::type_complexity)]
    pub fn scan_volume<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> impl Iterator<
        Item = Result<
            (
                CommitMeta,
                impl Iterator<
                    Item = Result<(SegmentKey, SplinterRef<Bytes>), Culprit<VolumeCatalogErr>>,
                >,
            ),
            Culprit<VolumeCatalogErr>,
        >,
    > + '_ {
        let seqno = self.keyspace.instant();
        let range = CommitKey::range(vid, lsns);
        self.volumes
            .snapshot_at(seqno)
            .range(range)
            .err_into()
            .map_ok(move |(key, meta)| {
                let key = CommitKey::try_read_from_bytes(&key)
                    .or_into_culprit("failed to decode CommitKey")?;
                let meta = CommitMeta::try_read_from_bytes(&meta)
                    .or_into_culprit("failed to decode CommitMeta")?;

                // scan segments for this commit
                let segments = self.segments.snapshot_at(seqno).prefix(key);
                let segments = SegmentsQueryIter { scan: segments };

                Ok((meta, segments))
            })
    }
}

pub struct VolumeCatalogBatch {
    batch: Batch,
    volumes: Partition,
    segments: Partition,
}

impl VolumeCatalogBatch {
    pub fn insert_commit(&mut self, commit: Commit) -> Result<(), Culprit<VolumeCatalogErr>> {
        let commit_key = CommitKey::new(commit.vid().clone(), commit.meta().lsn());

        self.batch.insert(
            &self.volumes,
            commit_key.as_bytes(),
            commit.meta().as_bytes(),
        );

        let mut iter = commit.iter_offsets();
        while let Some((sid, offsets)) = iter.try_next().or_into_ctx()? {
            let key = SegmentKey::new(commit_key.clone(), sid);
            self.batch.insert(&self.segments, key, offsets.into_inner());
        }

        Ok(())
    }

    pub fn insert_snapshot(
        &mut self,
        vid: VolumeId,
        snapshot: CommitMeta,
        segments: Vec<SegmentInfo>,
    ) -> Result<(), Culprit<VolumeCatalogErr>> {
        let commit_key = CommitKey::new(vid, snapshot.lsn());

        self.batch
            .insert(&self.volumes, commit_key.as_bytes(), snapshot);
        for segment in segments {
            let key = SegmentKey::new(commit_key.clone(), segment.sid.try_into()?);
            self.batch.insert(&self.segments, key, segment.offsets);
        }
        Ok(())
    }

    pub fn commit(self) -> Result<(), Culprit<VolumeCatalogErr>> {
        self.batch.commit()?;
        Ok(())
    }
}

pub struct SegmentsQueryIter<I> {
    scan: I,
}

impl<I: Iterator<Item = Result<(Slice, Slice), lsm_tree::Error>>> SegmentsQueryIter<I> {
    fn next_inner(
        &mut self,
        entry: Result<(Slice, Slice), lsm_tree::Error>,
    ) -> Result<(SegmentKey, SplinterRef<Bytes>), Culprit<VolumeCatalogErr>> {
        let (key, value) = entry?;
        let key =
            SegmentKey::try_read_from_bytes(&key).or_into_culprit("failed to decode SegmentKey")?;
        let val = SplinterRef::from_bytes(Bytes::from(value)).or_into_ctx()?;
        Ok((key, val))
    }
}

impl<I: Iterator<Item = Result<(Slice, Slice), lsm_tree::Error>>> Iterator
    for SegmentsQueryIter<I>
{
    type Item = Result<(SegmentKey, SplinterRef<Bytes>), Culprit<VolumeCatalogErr>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next().map(|entry| self.next_inner(entry))
    }
}
