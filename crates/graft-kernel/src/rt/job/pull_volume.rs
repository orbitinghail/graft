use culprit::{Result, ResultExt};
use graft_core::{
    VolumeId,
    checkpoints::{CachedCheckpoints, Checkpoints},
    lsn::{LSN, LSNRangeExt},
    volume_ref::VolumeRef,
};
use itertools::{EitherOrBoth, Itertools};
use range_set_blaze::RangeOnce;
use tokio_stream::StreamExt;

use crate::{
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::err::RuntimeErr,
    search_path::{PathEntry, SearchPath},
};

/// Pulls commits and metadata from a remote.
pub struct Opts {
    /// The Volume to fetch.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

pub async fn run(storage: &FjallStorage, remote: &Remote, opts: Opts) -> Result<(), RuntimeErr> {
    let snapshot = storage
        .read()
        .snapshot_at(&opts.vid, opts.max_lsn)
        .or_into_ctx()?;

    // calculate the maximum commit LSN to retrieve
    let max_lsn = snapshot.lsn().or(opts.max_lsn).unwrap_or(LSN::LAST);

    // fetch the search path
    let search = fetch_search_path(storage, remote, opts.vid.clone(), max_lsn).await?;

    // fetch any missing commits
    let mut batch = storage.batch();
    for PathEntry { vid, lsns } in search {
        let all_lsns = storage.read().lsns(&vid).or_into_ctx()?;
        let lsns = RangeOnce::new(lsns) - all_lsns.ranges();
        let mut commits = remote.stream_sorted_commits(&vid, lsns.flat_map(|r| r.iter()));
        while let Some(commit) = commits.try_next().await.or_into_ctx()? {
            batch.write_commit(commit);
        }
    }

    batch.commit().or_into_ctx()?;
    Ok(())
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
        let known_meta = storage.read().volume_meta(vref.vid()).or_into_ctx()?;
        let (known_etag, known_checkpoints) = match &known_meta {
            Some(meta) => (
                meta.checkpoints_etag().map(|e| e.to_string()),
                meta.checkpoints(),
            ),
            None => (None, &Checkpoints::EMPTY),
        };

        let remote_checkpoints = match remote.get_checkpoints(vref.vid(), known_etag).await {
            Ok(cached) => {
                refresh_checkpoint_commits(
                    storage,
                    remote,
                    vref.vid(),
                    known_checkpoints,
                    cached.checkpoints(),
                )
                .await
                .or_into_ctx()?;
                cached
            }

            Err(err) if err.ctx().is_not_modified() => known_meta
                .as_ref()
                .map_or(CachedCheckpoints::EMPTY, |meta| {
                    meta.cached_checkpoints().clone()
                }),
            Err(err) if err.ctx().is_not_found() => CachedCheckpoints::EMPTY,
            Err(err) => Err(err).or_into_ctx()?,
        };

        let meta = if known_meta.is_some() {
            // we know about this volume, just update its checkpoints
            storage
                .update_checkpoints(vref.vid().clone(), remote_checkpoints)
                .or_into_ctx()?
        } else {
            // we don't know about this volume, pull it
            let control = remote.get_control(vref.vid()).await.or_into_ctx()?;
            storage
                .register_volume(control, remote_checkpoints)
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
    vid: &VolumeId,
    prev_checkpoints: &Checkpoints,
    checkpoints: &Checkpoints,
) -> Result<(), RuntimeErr> {
    // Checkpoints are sorted, thus we can merge join the two lists of LSNs to
    // figure out which ones were added.
    let added: Vec<LSN> = prev_checkpoints
        .iter()
        .merge_join_by(checkpoints.iter(), Ord::cmp)
        .filter_map(|join| match join {
            EitherOrBoth::Right(v) => Some(*v),
            _ => None,
        })
        .collect();

    let mut commits = remote.stream_sorted_commits(vid, added);
    let mut batch = storage.batch();
    while let Some(commit) = commits.try_next().await.or_into_ctx()? {
        batch.write_commit(commit);
    }
    batch.commit().or_into_ctx()?;
    Ok(())
}
