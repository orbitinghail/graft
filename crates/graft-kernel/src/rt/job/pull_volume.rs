use culprit::{Result, ResultExt};
use graft_core::{
    VolumeId,
    checkpoint_set::CheckpointSet,
    lsn::{LSN, LSNSet, LSNSetExt},
    volume_ref::VolumeRef,
};

use crate::{
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{err::RuntimeErr, job::Job},
    search_path::{PathEntry, SearchPath},
};

/// Pulls commits and metadata from a remote.
pub struct Opts {
    /// The Volume to fetch.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> Result<Option<Job>, RuntimeErr> {
    let snapshot = storage
        .read()
        .snapshot_at(&opts.vid, opts.max_lsn)
        .or_into_ctx()?;

    // calculate the maximum commit LSN to retrieve
    let max_lsn = snapshot.lsn().or(opts.max_lsn).unwrap_or(LSN::LAST);

    // fetch the search path
    let search = fetch_search_path(storage, remote, opts.vid.clone(), max_lsn).await?;

    for PathEntry { vid, lsns } in search {
        let all_lsns = storage.read().lsns(&vid).or_into_ctx()?;
        let lsns = LSNSet::from_range(lsns) - all_lsns;
        let commits = remote.fetch_commits(&vid, lsns).await;

        // TODO: process new commits
    }

    todo!()
}

/// Load missing control files and changed checkpoint files while iterating the
/// path through a Volume and its parents. The `lsn` field ensures that we
/// retrieve the visible path to that specific LSN, rather than the latest path.
///
/// Returns the computed search path.
async fn fetch_search_path(
    storage: &FjallStorage,
    remote: &Remote,
    vid: VolumeId,
    lsn: LSN,
) -> Result<SearchPath, RuntimeErr> {
    let mut cursor = Some(VolumeRef::new(vid, lsn));
    let mut path = SearchPath::EMPTY;

    while let Some(vref) = cursor.take() {
        let meta = if let Some(meta) = storage.read().volume_meta(vref.vid()).or_into_ctx()? {
            // we know about this volume, refresh its checkpoints
            let (etag, prev_checkpoints) = match meta.checkpoints() {
                Some((a, b)) => (Some(a), Some(b)),
                None => (None, None),
            };
            let checkpoints = remote
                .fetch_checkpoints(vref.vid(), etag)
                .await
                .or_into_ctx()?;
            if let Some(checkpoints) = checkpoints {
                refresh_checkpoint_commits(
                    storage,
                    remote,
                    prev_checkpoints,
                    checkpoints.1.clone(),
                )
                .await
                .or_into_ctx()?;
                storage
                    .update_checkpoints(vref.vid().clone(), checkpoints)
                    .or_into_ctx()?
            } else {
                meta
            }
        } else {
            // we don't know about this volume, pull it
            let control = remote.fetch_control(vref.vid()).await.or_into_ctx()?;
            let checkpoints = remote
                .fetch_checkpoints(vref.vid(), None)
                .await
                .or_into_ctx()?;
            if let Some((_, checkpoints)) = checkpoints.as_ref() {
                refresh_checkpoint_commits(storage, remote, None, checkpoints.clone())
                    .await
                    .or_into_ctx()?;
            }
            storage
                .register_volume(control, checkpoints)
                .or_into_ctx()?
        };

        debug_assert_eq!(meta.vid(), vref.vid());

        // if this volume has a checkpoint for the LSN we are searching for,
        // then we can terminate the search path at that checkpoint.
        if let Some(checkpoint) = meta.checkpoint_for(vref.lsn()) {
            path.append(vref.vid().clone(), checkpoint..=vref.lsn());
            break;
        }

        // otherwise, there are no checkpoints, so we need to scan to the
        // beginning of this Volume and recurse to its parent if it has one
        path.append(vref.vid().clone(), LSN::FIRST..=vref.lsn());
        cursor = meta.parent().cloned();
    }

    Ok(path)
}

async fn refresh_checkpoint_commits(
    storage: &FjallStorage,
    remote: &Remote,
    prev_checkpoints: Option<&CheckpointSet>,
    checkpoints: CheckpointSet,
) -> Result<(), RuntimeErr> {
    // checkpoints are never modified, so figure out which checkpoints were
    // added and re-fetch them from the remote
    todo!()
}
