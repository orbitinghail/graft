use std::{
    fmt::Debug,
    io,
    ops::RangeBounds,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use fjall::{
    Batch, Config, Keyspace, KvSeparationOptions, Partition, PartitionCreateOptions, Slice,
};
use graft_core::{gid::VolumeId, lsn::LSN, zerocopy_err::ZerocopyErr};
use graft_proto::common::v1::SegmentInfo;
use serde::{Deserialize, Serialize};
use splinter::SplinterRef;
use trackerr::{CallerLocation, LocationStack};
use tryiter::TryIteratorExt;
use zerocopy::{FromBytes, TryFromBytes};

use super::{
    commit::{Commit, CommitMeta, OffsetsValidationErr},
    kv::{CommitKey, SegmentKey},
};

#[derive(Debug, thiserror::Error)]
pub enum VolumeCatalogErr {
    #[error("failed to parse Gid")]
    GidParseErr(
        #[from] graft_core::gid::GidParseErr,
        #[implicit] CallerLocation,
    ),

    #[error("fjall error")]
    FjallErr(#[from] fjall::Error, #[implicit] CallerLocation),

    #[error("io error")]
    IoErr(#[from] std::io::Error, #[implicit] CallerLocation),

    #[error("Failed to decode entry into type {target}")]
    DecodeErr {
        target: &'static str,
        source: ZerocopyErr,
        loc: CallerLocation,
    },

    #[error("splinter error")]
    SplinterErr(#[from] splinter::DecodeErr, #[implicit] CallerLocation),

    #[error("offsets validation error")]
    OffsetsValidationErr(#[from] OffsetsValidationErr, #[implicit] CallerLocation),
}

impl From<lsm_tree::Error> for VolumeCatalogErr {
    fn from(err: lsm_tree::Error) -> Self {
        VolumeCatalogErr::FjallErr(err.into(), Default::default())
    }
}

impl LocationStack for VolumeCatalogErr {
    fn location(&self) -> &CallerLocation {
        use VolumeCatalogErr::*;
        match self {
            GidParseErr(_, loc)
            | FjallErr(_, loc)
            | IoErr(_, loc)
            | DecodeErr { loc, .. }
            | SplinterErr(_, loc)
            | OffsetsValidationErr(_, loc) => loc,
        }
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        use VolumeCatalogErr::*;
        match self {
            GidParseErr(err, _) => Some(err),
            DecodeErr { source, .. } => Some(source),
            SplinterErr(err, _) => Some(err),
            OffsetsValidationErr(err, _) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct VolumeCatalogConfig {
    /// path to the directory where the catalog will be stored
    /// if not provided, a temporary directory will be created
    pub path: Option<PathBuf>,
}

impl TryFrom<VolumeCatalogConfig> for Config {
    type Error = io::Error;

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
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, VolumeCatalogErr> {
        Self::open_config(VolumeCatalogConfig { path: Some(path.as_ref().to_path_buf()) })
    }

    pub fn open_temporary() -> Result<Self, VolumeCatalogErr> {
        Self::open_config(VolumeCatalogConfig { path: None })
    }

    pub fn open_config(config: VolumeCatalogConfig) -> Result<Self, VolumeCatalogErr> {
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

    pub fn contains_snapshot(&self, vid: VolumeId, lsn: LSN) -> Result<bool, VolumeCatalogErr> {
        Ok(self.volumes.contains_key(CommitKey::new(vid, lsn))?)
    }

    pub fn contains_range<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> Result<bool, VolumeCatalogErr> {
        let range = CommitKey::range(vid, lsns);

        // verify that lsns in the range are contiguous
        let mut cursor = range.start.lsn();
        let mut empty = true;

        for kv in self.volumes.snapshot().range(range) {
            let (key, _) = kv?;
            let key =
                CommitKey::try_ref_from_bytes(&key).map_err(|err| VolumeCatalogErr::DecodeErr {
                    target: "CommitKey",
                    source: err.into(),
                    loc: Default::default(),
                })?;
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
    ) -> Result<Option<CommitMeta>, VolumeCatalogErr> {
        self.volumes
            .get(CommitKey::new(vid, lsn))?
            .map(|bytes| {
                CommitMeta::read_from_bytes(&bytes).map_err(|err| VolumeCatalogErr::DecodeErr {
                    target: "CommitMeta",
                    source: err.into(),
                    loc: Default::default(),
                })
            })
            .transpose()
    }

    /// Return the latest snapshot for the specified Volume.
    /// Returns None if no snapshot is found, or the snapshot is corrupt.
    pub fn latest_snapshot(&self, vid: &VolumeId) -> Result<Option<CommitMeta>, VolumeCatalogErr> {
        self.volumes
            .snapshot()
            .prefix(vid)
            .rev()
            .err_into()
            .map_ok(|(_, bytes)| {
                CommitMeta::read_from_bytes(&bytes).map_err(|err| VolumeCatalogErr::DecodeErr {
                    target: "CommitMeta",
                    source: err.into(),
                    loc: Default::default(),
                })
            })
            .try_next()
    }

    /// scan the catalog for segments in the specified Volume. Segments are
    /// scanned in reverse order by LSN.
    pub fn scan_segments<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        lsns: &R,
    ) -> impl Iterator<Item = Result<(SegmentKey, SplinterRef<Bytes>), VolumeCatalogErr>> {
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
                impl Iterator<Item = Result<(SegmentKey, SplinterRef<Bytes>), VolumeCatalogErr>>,
            ),
            VolumeCatalogErr,
        >,
    > + '_ {
        let seqno = self.keyspace.instant();
        let range = CommitKey::range(vid, lsns);
        self.volumes
            .snapshot_at(seqno)
            .range(range)
            .err_into::<VolumeCatalogErr>()
            .map_ok(move |(key, meta)| {
                let key = CommitKey::try_read_from_bytes(&key).map_err(|err| {
                    VolumeCatalogErr::DecodeErr {
                        target: "CommitKey",
                        source: err.into(),
                        loc: Default::default(),
                    }
                })?;
                let meta = CommitMeta::read_from_bytes(&meta).map_err(|err| {
                    VolumeCatalogErr::DecodeErr {
                        target: "CommitMeta",
                        source: err.into(),
                        loc: Default::default(),
                    }
                })?;

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
    pub fn insert_commit(&mut self, commit: Commit) -> Result<(), VolumeCatalogErr> {
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
    ) -> Result<(), VolumeCatalogErr> {
        let commit_key = CommitKey::new(vid, snapshot.lsn());

        self.batch.insert(&self.volumes, &commit_key, &snapshot);
        for segment in segments {
            let key = SegmentKey::new(commit_key.clone(), segment.sid.try_into()?);
            self.batch.insert(&self.segments, key, segment.offsets);
        }
        Ok(())
    }

    pub fn commit(self) -> Result<(), VolumeCatalogErr> {
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
    ) -> Result<(SegmentKey, SplinterRef<Bytes>), VolumeCatalogErr> {
        let (key, value) = entry?;
        let key =
            SegmentKey::try_read_from_bytes(&key).map_err(|err| VolumeCatalogErr::DecodeErr {
                target: "SegmentKey",
                source: err.into(),
                loc: Default::default(),
            })?;
        let val = SplinterRef::from_bytes(Bytes::from(value))?;
        Ok((key, val))
    }
}

impl<I: Iterator<Item = Result<(Slice, Slice), lsm_tree::Error>>> Iterator
    for SegmentsQueryIter<I>
{
    type Item = Result<(SegmentKey, SplinterRef<Bytes>), VolumeCatalogErr>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next().map(|entry| self.next_inner(entry))
    }
}
