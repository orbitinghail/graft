use std::{collections::BTreeMap, ops::RangeInclusive};

use crate::core::{
    CommitHashBuilder, LogId, PageCount, PageIdx, SegmentId, VolumeId,
    commit::{Commit, SegmentIdx},
    commit_hash::CommitHash,
    logref::LogRef,
    lsn::LSN,
};
use bytes::Bytes;
use splinter_rs::{Optimizable, PartitionRead, Splinter};
use thin_vec::thin_vec;
use tryiter::TryIteratorExt;

use crate::{
    GraftErr, LogicalErr,
    local::fjall_storage::FjallStorage,
    remote::{Remote, segment::SegmentBuilder},
    rt::action::{Action, FetchLog},
    snapshot::Snapshot,
    volume::PendingCommit,
};

/// Commits a Volume's local changes into its remote.
#[derive(Debug)]
pub struct RemoteCommit {
    pub vid: VolumeId,
}

impl Action for RemoteCommit {
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> Result<(), GraftErr> {
        // first, check if we need to recover from a pending commit
        // we do this *before* plan since this may modify storage
        attempt_recovery(storage, remote, &self.vid).await?;

        let Some(plan) = plan_commit(storage, &self.vid)? else {
            // nothing to commit
            return Ok(());
        };

        // build & upload segment
        let (commit_hash, segment_idx, segment_chunks) = build_segment(storage, &plan)?;
        remote
            .put_segment(segment_idx.sid(), segment_chunks)
            .await?;

        #[cfg(feature = "precept")]
        precept::sometimes_fault!(
            "RemoteCommit: before prepare",
            crate::fault::fault_crash(),
            { "vid": self.vid }
        );

        // make final preparations before pushing to the remote.
        // these preparations include checking preconditions and setting
        // pending_commit on the Volume
        storage.read_write().remote_commit_prepare(
            &self.vid,
            PendingCommit {
                local: *plan.lsns.end(),
                commit: plan.commit_ref.lsn,
                commit_hash: commit_hash.clone(),
            },
        )?;

        // detect implicit checkpoint: if we're uploading all pages, this is a checkpoint
        let maybe_checkpoint = if segment_idx.page_count() == plan.page_count {
            thin_vec![plan.commit_ref.lsn]
        } else {
            thin_vec![]
        };

        let commit = Commit::new(
            plan.commit_ref.log().clone(),
            plan.commit_ref.lsn(),
            plan.page_count,
        )
        .with_commit_hash(Some(commit_hash.clone()))
        .with_segment_idx(Some(segment_idx))
        .with_checkpoints(maybe_checkpoint);

        #[cfg(feature = "precept")]
        precept::sometimes_fault!(
            "RemoteCommit: before commit",
            crate::fault::fault_crash(),
            { "vid": self.vid }
        );

        // issue the remote commit!
        let result = remote.put_commit(&commit).await;

        #[cfg(feature = "precept")]
        precept::sometimes_fault!(
            "RemoteCommit: after commit",
            crate::fault::fault_crash(),
            { "vid": self.vid }
        );

        match result {
            Ok(()) => {
                storage
                    .read_write()
                    .remote_commit_success(&self.vid, commit)?;
                Ok(())
            }
            Err(err) if err.is_already_exists() => {
                // The commit already exists on the remote. This could be because:
                // 1. Someone (including us) pushed the same commit (idempotency)
                // 2. Someone (including us) pushed a DIFFERENT commit (divergence)
                attempt_recovery(storage, remote, &self.vid).await
            }
            Err(err) => {
                // if any other error occurs, we leave the pending_commit in place and fail the job.
                // this allows the `recover_pending_commit` job to run at a later
                // point which will attempt to figure out if the commit was
                // successful on the remote or not
                Err(err.into())
            }
        }
    }
}

struct CommitPlan {
    local: LogId,
    lsns: RangeInclusive<LSN>,
    commit_ref: LogRef,
    page_count: PageCount,
}

fn plan_commit(storage: &FjallStorage, vid: &VolumeId) -> Result<Option<CommitPlan>, GraftErr> {
    let reader = storage.read();
    let volume = reader.volume(vid)?;

    if volume.pending_commit().is_some() {
        // this should have been handled earlier
        return Err(LogicalErr::VolumeNeedsRecovery(volume.vid).into());
    }

    let Some(latest_local) = reader.latest_lsn(&volume.local)? else {
        // nothing to push
        return Ok(None);
    };
    let latest_remote = reader.latest_lsn(&volume.remote)?;

    let page_count = reader
        .page_count(&volume.local, latest_local)?
        .expect("BUG: no page count for commit");

    let Some(sync) = volume.sync() else {
        // this is the first time we are pushing this volume to the remote
        assert_eq!(latest_remote, None, "BUG: remote should be empty");
        return Ok(Some(CommitPlan {
            local: volume.local.clone(),
            lsns: LSN::FIRST..=latest_local,
            commit_ref: LogRef::new(volume.remote, LSN::FIRST),
            page_count,
        }));
    };

    // check for divergence
    if volume.remote_changes(latest_remote).is_some() {
        // the remote and local logs have diverged
        let status = volume.status(Some(latest_local), latest_remote);
        tracing::debug!("volume {} has diverged; status=`{status}`", volume.local);
        return Err(LogicalErr::VolumeDiverged(volume.vid).into());
    }

    // calculate which LSNs we need to sync
    let Some(local_lsns) = volume.local_changes(Some(latest_local)) else {
        // nothing to push
        return Ok(None);
    };

    // calculate the commit lsn
    let commit_lsn = sync.remote.next();

    Ok(Some(CommitPlan {
        local: volume.local.clone(),
        lsns: local_lsns,
        commit_ref: LogRef::new(volume.remote.clone(), commit_lsn),
        page_count,
    }))
}

fn build_segment(
    storage: &FjallStorage,
    plan: &CommitPlan,
) -> Result<(CommitHash, SegmentIdx, Vec<Bytes>), GraftErr> {
    let reader = storage.read();

    // built a snapshot which only matches the LSNs we want to
    // include in the segment
    let snapshot = Snapshot::new(plan.local.clone(), plan.lsns.clone(), plan.page_count);

    // collect all of the segment pages, only keeping the newest (first) page
    // for each unique pageidx
    let page_count = plan.page_count;
    let mut pages = BTreeMap::new();
    let mut pageset = Splinter::default();
    let mut commits = reader.commits(&snapshot);
    while let Some(commit) = commits.try_next()? {
        if let Some(idx) = commit.segment_idx {
            let mut commit_pages = idx.pageset;

            // truncate any pages in this commit that extend beyond the page count
            if commit_pages.last().map(|idx| idx.pages()) > Some(page_count) {
                commit_pages.truncate(page_count);
            }

            // figure out which pages we haven't seen
            let outstanding = Splinter::from(commit_pages) - &pageset;
            // load all of the outstanding pages
            for pageidx in outstanding.iter() {
                // SAFETY: outstanding is built from a set of valid PageIdxs
                let pageidx = unsafe { PageIdx::new_unchecked(pageidx) };
                debug_assert!(plan.page_count.contains(pageidx));
                let page = reader.read_page(idx.sid.clone(), pageidx)?;
                pages.insert(pageidx, page.expect("BUG: missing page"));
            }
            // update the pageset accordingly
            pageset |= outstanding;
        }
    }

    // optimize the pageset
    pageset.optimize();

    let mut segment_builder = SegmentBuilder::new();
    let mut commithash_builder = CommitHashBuilder::new(
        plan.commit_ref.log().clone(),
        plan.commit_ref.lsn(),
        plan.page_count,
    );

    let sid = SegmentId::random();

    let mut batch = storage.batch();
    for (pageidx, page) in pages {
        commithash_builder.write_page(pageidx, &page);
        segment_builder.write(pageidx, &page);

        #[cfg(feature = "precept")]
        precept::sometimes_fault!(
            "RemoteCommit: skipping segment cache",
            continue,
            { "sid": sid }
        );

        // we immediately cache the new segment's pages into storage, as new
        // Snapshots will read from the new commits rather than the local
        // commits.
        batch.write_page(sid.clone(), pageidx, page);
    }

    let commit_hash = commithash_builder.build();
    let (frames, chunks) = segment_builder.finish();
    let idx = SegmentIdx::new(sid, pageset.into()).with_frames(frames);

    batch.commit()?;

    Ok((commit_hash, idx, chunks))
}

/// Attempts to recover from a remote commit conflict by checking the remote
/// for the commit we tried to push.
async fn attempt_recovery(
    storage: &FjallStorage,
    remote: &Remote,
    vid: &VolumeId,
) -> Result<(), GraftErr> {
    let reader = storage.read();
    let volume = reader.volume(vid)?;

    #[cfg(feature = "precept")]
    precept::sometimes_fault!(
        "RemoteCommit: attempting recovery",
        crate::fault::fault_crash(),
        { "vid": vid }
    );

    if let Some(pending) = volume.pending_commit {
        tracing::debug!(?pending, "attempting to recover pending commit");

        // If we have a pending commit, we need to fetch the remote log to see
        // if our pending commit already landed.
        FetchLog {
            log: volume.remote.clone(),
            max_lsn: Some(pending.commit),
        }
        .run(storage, remote)
        .await?;

        storage.read_write().recover_pending_commit(vid)?;
        Ok(())
    } else {
        // recovery not needed
        Ok(())
    }
}
