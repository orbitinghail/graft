use std::{collections::BTreeMap, ops::RangeInclusive, time::SystemTime};

use bytes::Bytes;
use culprit::ResultExt;
use graft_core::{
    CommitHashBuilder, PageCount, PageIdx, SegmentId, VolumeId,
    commit::{Commit, SegmentIdx},
    commit_hash::CommitHash,
    lsn::LSN,
    volume_control::VolumeControl,
    volume_ref::VolumeRef,
};
use smallvec::SmallVec;
use splinter_rs::{PartitionRead, Splinter};
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::FjallStorage,
    named_volume::NamedVolumeState,
    remote::{Remote, segment::SegmentBuilder},
    rt::err::RuntimeErr,
    search_path::SearchPath,
    snapshot::Snapshot,
    volume_name::VolumeName,
};

/// Commits a Named Volume's local changes into its remote.
///
/// This process involves the following stages:
///
/// 1. prepare commit
/// 2. push segment
/// 3. commit
/// 4. update named volume (on success or failure)
pub struct Opts {
    pub name: VolumeName,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    let mut plan = plan_commit(storage, &opts.name)?;
    let (commit_hash, segment_idx, segment_chunks) = build_segment(storage, &plan)?;

    // upload segment
    remote
        .put_segment(plan.commit_ref.vid(), segment_idx.sid(), segment_chunks)
        .await
        .or_into_ctx()?;

    // if this is the first commit to the remote volume, create the control
    if plan.remote.is_none() {
        remote
            .put_control(
                plan.commit_ref.vid(),
                VolumeControl::new(plan.commit_ref.vid().clone(), None, SystemTime::now()),
            )
            .await
            .or_into_ctx()?;
    }

    // make final preparations before pushing to the remote
    plan.handle = storage
        .remote_commit_prepare(
            &plan.handle,
            plan.remote.as_ref(),
            plan.commit_ref.lsn(),
            &commit_hash,
        )
        .or_into_ctx()?;

    let commit = Commit::new(
        plan.commit_ref.vid().clone(),
        plan.commit_ref.lsn(),
        plan.page_count,
    )
    .with_commit_hash(Some(commit_hash))
    .with_segment_idx(Some(segment_idx));

    // issue the remote commit!
    let result = remote
        .put_commit(plan.commit_ref.vid(), commit.clone())
        .await;

    match result {
        Ok(()) => {
            storage
                .remote_commit_success(&plan.handle, plan.local_ref, commit)
                .or_into_ctx()?;
        }
        Err(err) if err.ctx().is_already_exists() => {
            storage.remote_commit_rejected(&plan.handle).or_into_ctx()?;
        }
        Err(err) => {
            // if any other error occurs, we leave the pending_commit in place and fail the job.
            // this allows the `recover_pending_commit` job to run at a later
            // point which will attempt to figure out if the commit was
            // successful on the remote or not
            return Err(err).or_into_ctx();
        }
    }
    Ok(())
}

struct CommitPlan {
    /// the state of the handle at the beginning of the remote commit process
    handle: NamedVolumeState,

    /// a reference to the local commit we are syncing to the remote
    local_ref: VolumeRef,

    /// the local lsns to commit to the remote
    local_lsns: RangeInclusive<LSN>,

    /// the page count of the Volume at the local snapshot
    page_count: PageCount,

    /// the latest remote snapshot
    remote: Option<Snapshot>,

    /// the `VolumeRef` of the resulting commit should this process be successful
    commit_ref: VolumeRef,
}

fn plan_commit(
    storage: &FjallStorage,
    name: &VolumeName,
) -> culprit::Result<CommitPlan, RuntimeErr> {
    let reader = storage.read();
    let Some(handle) = reader.named_volume(name).or_into_ctx()? else {
        return Err(RuntimeErr::NamedVolumeNotFound(name.clone()).into());
    };
    if handle.pending_commit().is_some() {
        return Err(RuntimeErr::NamedVolumeNeedsRecovery(name.clone()).into());
    }

    let latest_local = reader.snapshot(handle.local()).or_into_ctx()?;
    let page_count = reader.page_count(&latest_local).or_into_ctx()?;

    let Some(sync) = handle.sync() else {
        // this is the first time we are pushing this named volume to the remote
        let Some(latest_local_lsn) = latest_local.lsn() else {
            // nothing to push
            let status = handle.sync_status(&latest_local, None);
            return Err(RuntimeErr::NamedVolumeNoChanges(name.clone(), status).into());
        };
        return Ok(CommitPlan {
            handle,
            local_ref: VolumeRef::new(latest_local.vid().clone(), latest_local_lsn),
            local_lsns: LSN::FIRST..=latest_local_lsn,
            page_count,
            remote: None,
            commit_ref: VolumeRef::new(VolumeId::random(), LSN::FIRST),
        });
    };

    // load the latest remote snapshot
    let latest_remote = reader.snapshot(sync.remote().vid()).or_into_ctx()?;

    // calculate which LSNs we need to sync
    let Some(local_lsns) = latest_local
        .lsn()
        .and_then(|latest| (sync.local().lsn() < latest).then_some(sync.local().lsn()..=latest))
    else {
        // nothing to push
        let status = handle.sync_status(&latest_local, Some(&latest_remote));
        return Err(RuntimeErr::NamedVolumeNoChanges(name.clone(), status).into());
    };

    // make sure the remote isn't ahead of the sync point
    if latest_remote.lsn() != Some(sync.remote().lsn()) {
        // the remote and local volumes have diverged
        let status = handle.sync_status(&latest_local, Some(&latest_remote));
        return Err(RuntimeErr::NamedVolumeDiverged(name.clone(), status).into());
    }

    // calculate the commit result
    let commit_lsn = sync.remote().lsn().next().expect("maximum LSN exceeded");
    let commit_ref = VolumeRef::new(sync.remote().vid().clone(), commit_lsn);

    let local_ref = VolumeRef::new(handle.local().clone(), *local_lsns.end());
    let page_count = reader.page_count(&latest_local).or_into_ctx()?;

    Ok(CommitPlan {
        handle,
        local_ref,
        local_lsns,
        page_count,
        remote: Some(latest_remote),
        commit_ref,
    })
}

fn build_segment(
    storage: &FjallStorage,
    plan: &CommitPlan,
) -> culprit::Result<(CommitHash, SegmentIdx, SmallVec<[Bytes; 1]>), RuntimeErr> {
    let reader = storage.read();

    // built a search path which only matches the LSNs we want to
    // include in the segment
    let segment_path = SearchPath::new(plan.local_ref.vid().clone(), plan.local_lsns.clone());

    // collect all of the segment pages, only keeping the newest (first) page
    // for each unique pageidx
    let mut pages = BTreeMap::new();
    let mut graft = Splinter::default();
    let mut commits = reader.commits(&segment_path);
    while let Some(commit) = commits.try_next().or_into_ctx()? {
        if let Some(idx) = commit.segment_idx() {
            // calculate which pages we need from this commit
            let outstanding = idx.graft().splinter() - &graft;
            // load all of the outstanding pages
            for pageidx in outstanding.iter() {
                // SAFETY: outstanding is built from a Graft of already valid PageIdxs
                let pageidx = unsafe { PageIdx::new_unchecked(pageidx) };
                // ignore truncated pages
                if plan.page_count.contains(pageidx) {
                    let page = reader.read_page(idx.sid().clone(), pageidx).or_into_ctx()?;
                    pages.insert(pageidx, page.expect("BUG: missing page"));
                }
            }
            // update the graft accordingly
            graft |= outstanding;
        }
    }

    let mut segment_builder = SegmentBuilder::new();
    let mut commithash_builder = CommitHashBuilder::new(
        plan.commit_ref.vid().clone(),
        plan.commit_ref.lsn(),
        plan.page_count,
    );

    // TODO: we may want to writeback the segment into storage if we expect to
    // be resetting the local volume to the remote anytime soon (or otherwise
    // querying the remote volume)

    for (pageidx, page) in pages {
        commithash_builder.write_page(pageidx, &page);
        segment_builder.write(pageidx, page);
    }

    let commit_hash = commithash_builder.build();
    let (frames, chunks) = segment_builder.finish();
    let idx = SegmentIdx::new(SegmentId::random(), graft.into()).with_frames(frames);

    Ok((commit_hash, idx, chunks))
}
