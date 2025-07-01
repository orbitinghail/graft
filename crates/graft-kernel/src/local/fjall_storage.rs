mod keys;

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: fjall::Partition,

    /// This partition stores Volume properties
    /// {vid} / control -> Control
    /// {vid} / checkpoints -> `LocalCheckpointSet`
    /// Keyed by `keys::VolumeKey`
    volumes: fjall::Partition,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    /// Keyed by `keys::CommitKey`
    log: fjall::Partition,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    /// Keyed by `keys::PageKey`
    pages: fjall::Partition,
}
