use std::{collections::BTreeMap, ops::RangeInclusive};

use bytes::Bytes;
use culprit::ResultExt;
use graft_core::{
    CommitHashBuilder, PageCount, PageIdx, SegmentId, VolumeId,
    commit::{Commit, SegmentIdx},
    commit_hash::CommitHash,
    lsn::LSN,
    volume_ref::VolumeRef,
};
use smallvec::SmallVec;
use splinter_rs::{Optimizable, PartitionRead, Splinter};
use tryiter::TryIteratorExt;

use crate::{
    KernelErr, LogicalErr,
    graft::PendingCommit,
    local::fjall_storage::FjallStorage,
    remote::{Remote, segment::SegmentBuilder},
    rt::action::{Action, FetchVolume},
    snapshot::Snapshot,
};

/// Commits a Graft's local changes into its remote.
#[derive(Debug)]
pub struct RemoteCommit {
    pub graft: VolumeId,
}

impl Action for RemoteCommit {
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> culprit::Result<(), KernelErr> {
        // first, check if we need to recover from a pending commit
        // we do this *before* plan since this may modify storage
        attempt_recovery(storage, &self.graft).or_into_culprit("attempting recovery")?;

        let Some(plan) = plan_commit(storage, &self.graft)? else {
            // nothing to commit
            return Ok(());
        };

        // build & upload segment
        let (commit_hash, segment_idx, segment_chunks) = build_segment(storage, &plan)?;
        remote
            .put_segment(segment_idx.sid(), segment_chunks)
            .await
            .or_into_ctx()?;

        // make final preparations before pushing to the remote.
        // these preparations include checking preconditions and setting
        // pending_commit on the Graft
        storage
            .read_write()
            .remote_commit_prepare(
                &self.graft,
                PendingCommit {
                    local: *plan.lsns.end(),
                    commit: plan.commit_ref.lsn,
                    commit_hash: commit_hash.clone(),
                },
            )
            .or_into_ctx()?;

        let commit = Commit::new(
            plan.commit_ref.vid().clone(),
            plan.commit_ref.lsn(),
            plan.page_count,
        )
        .with_commit_hash(Some(commit_hash.clone()))
        .with_segment_idx(Some(segment_idx));

        // issue the remote commit!
        let result = remote.put_commit(&commit).await;

        match result {
            Ok(()) => {
                storage
                    .read_write()
                    .remote_commit_success(&self.graft, commit)
                    .or_into_ctx()?;
                Ok(())
            }
            Err(err) if err.ctx().is_already_exists() => {
                // The commit already exists on the remote. This could be because:
                // 1. Someone (including us) pushed the same commit (idempotency)
                // 2. Someone (including us) pushed a DIFFERENT commit (divergence)
                // To resolve this, refetch the remote and attempt recovery.
                FetchVolume {
                    vid: commit.vid.clone(),
                    max_lsn: Some(commit.lsn),
                }
                .run(storage, remote)
                .await?;
                attempt_recovery(storage, &self.graft)
                    .or_into_culprit("recovering from existing remote commit")
            }
            Err(err) => {
                // if any other error occurs, we leave the pending_commit in place and fail the job.
                // this allows the `recover_pending_commit` job to run at a later
                // point which will attempt to figure out if the commit was
                // successful on the remote or not
                Err(err).or_into_ctx()
            }
        }
    }
}

struct CommitPlan {
    local_vid: VolumeId,
    lsns: RangeInclusive<LSN>,
    commit_ref: VolumeRef,
    page_count: PageCount,
}

fn plan_commit(
    storage: &FjallStorage,
    graft: &VolumeId,
) -> culprit::Result<Option<CommitPlan>, KernelErr> {
    let reader = storage.read();
    let graft = reader.graft(graft).or_into_ctx()?;

    if graft.pending_commit().is_some() {
        // this should have been handled earlier
        return Err(LogicalErr::GraftNeedsRecovery(graft.local).into());
    }

    let Some(latest_local) = reader.latest_lsn(&graft.local).or_into_ctx()? else {
        // nothing to push
        return Ok(None);
    };
    let latest_remote = reader.latest_lsn(&graft.remote).or_into_ctx()?;

    let page_count = reader
        .page_count(&graft.local, latest_local)
        .or_into_ctx()?
        .expect("BUG: no page count for local volume");

    let Some(sync) = graft.sync() else {
        // this is the first time we are pushing this graft to the remote
        assert_eq!(latest_remote, None, "BUG: remote should be empty");
        return Ok(Some(CommitPlan {
            local_vid: graft.local.clone(),
            lsns: LSN::FIRST..=latest_local,
            commit_ref: VolumeRef::new(graft.remote, LSN::FIRST),
            page_count,
        }));
    };

    // check for divergence
    if graft.remote_changes(latest_remote).is_some() {
        // the remote and local volumes have diverged
        let status = graft.status(Some(latest_local), latest_remote);
        tracing::debug!("graft {} has diverged; status=`{status}`", graft.local);
        return Err(LogicalErr::GraftDiverged(graft.local).into());
    }

    // calculate which LSNs we need to sync
    let Some(local_lsns) = graft.local_changes(Some(latest_local)) else {
        // nothing to push
        return Ok(None);
    };

    // calculate the commit lsn
    let commit_lsn = sync.remote.next();

    Ok(Some(CommitPlan {
        local_vid: graft.local.clone(),
        lsns: local_lsns,
        commit_ref: VolumeRef::new(graft.remote.clone(), commit_lsn),
        page_count,
    }))
}

fn build_segment(
    storage: &FjallStorage,
    plan: &CommitPlan,
) -> culprit::Result<(CommitHash, SegmentIdx, SmallVec<[Bytes; 1]>), KernelErr> {
    let reader = storage.read();

    // built a snapshot which only matches the LSNs we want to
    // include in the segment
    let segment_path = Snapshot::new(plan.local_vid.clone(), plan.lsns.clone());

    // collect all of the segment pages, only keeping the newest (first) page
    // for each unique pageidx
    let mut page_count = plan.page_count;
    let mut pages = BTreeMap::new();
    let mut pageset = Splinter::default();
    let mut commits = reader.commits(&segment_path);
    while let Some(commit) = commits.try_next().or_into_ctx()? {
        // if we encounter a smaller commit on our travels, we need to shrink
        // the page_count to ensure that truncation is respected
        page_count = page_count.min(commit.page_count);

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
                // SAFETY: outstanding is built from a Graft of already valid PageIdxs
                let pageidx = unsafe { PageIdx::new_unchecked(pageidx) };
                debug_assert!(plan.page_count.contains(pageidx));
                let page = reader.read_page(idx.sid.clone(), pageidx).or_into_ctx()?;
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
        plan.commit_ref.vid().clone(),
        plan.commit_ref.lsn(),
        plan.page_count,
    );

    let sid = SegmentId::random();

    let mut batch = storage.batch();
    for (pageidx, page) in pages {
        commithash_builder.write_page(pageidx, &page);
        segment_builder.write(pageidx, &page);

        // we immediately cache the new segment's pages into storage, as new
        // Snapshots will read from the new commits rather than the local
        // commits.
        batch.write_page(sid.clone(), pageidx, page);
    }

    let commit_hash = commithash_builder.build();
    let (frames, chunks) = segment_builder.finish();
    let idx = SegmentIdx::new(sid, pageset.into()).with_frames(frames);

    batch.commit().or_into_ctx()?;

    Ok((commit_hash, idx, chunks))
}

/// Attempts to recover from a remote commit conflict by checking the remote
/// for the commit we tried to push.
fn attempt_recovery(storage: &FjallStorage, graft: &VolumeId) -> culprit::Result<(), KernelErr> {
    let reader = storage.read();
    let graft = reader.graft(graft).or_into_ctx()?;

    if let Some(pending) = graft.pending_commit {
        tracing::debug!(?pending, "got pending commit");
        match storage
            .read()
            .get_commit(&graft.remote, pending.commit)
            .or_into_ctx()?
        {
            Some(commit) if commit.commit_hash() == Some(&pending.commit_hash) => {
                // It's the same commit. Recovery success!
                storage
                    .read_write()
                    .remote_commit_success(&graft.local, commit)
                    .or_into_ctx()?;
                Ok(())
            }
            Some(commit) => {
                // Case 2: Divergence detected.
                storage
                    .read_write()
                    .drop_pending_commit(&graft.local)
                    .or_into_ctx()?;
                tracing::warn!(
                    "remote commit rejected for graft {}, commit {}/{} already exists with different hash: {:?}",
                    graft.local,
                    graft.remote,
                    pending.commit,
                    commit.commit_hash
                );
                Err(LogicalErr::GraftDiverged(graft.local).into())
            }
            None => {
                // No commit found. Recovery unknown.
                // We don't drop the pending commit, as we may need to wait for the
                // commit to show up in the remote.
                Err(LogicalErr::GraftNeedsRecovery(graft.local).into())
            }
        }
    } else {
        // recovery not needed
        Ok(())
    }
}
