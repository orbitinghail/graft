use std::{collections::BTreeMap, fmt::Debug, time::SystemTime};

use bytes::Bytes;
use culprit::ResultExt;
use graft_core::{
    CommitHashBuilder, PageIdx, SegmentId, VolumeId,
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
    named_volume::PendingCommit,
    remote::{Remote, segment::SegmentBuilder},
    rt::err::RuntimeErr,
    search_path::SearchPath,
    volume_err::VolumeErr,
    volume_name::VolumeName,
};

/// Commits a Named Volume's local changes into its remote.
pub struct Opts {
    pub name: VolumeName,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCommit")
            .field("name", &self.name.to_string())
            .finish()
    }
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    let Some(mut plan) = plan_commit(storage, &opts.name)? else {
        // nothing to commit
        return Ok(());
    };

    // build & upload segment
    let (commit_hash, segment_idx, segment_chunks) = build_segment(storage, &plan)?;
    remote
        .put_segment(plan.commit_ref.vid(), segment_idx.sid(), segment_chunks)
        .await
        .or_into_ctx()?;

    // update the commit plan with the computed commit hash
    plan.commit_hash = commit_hash.clone();

    // if this is the first commit to the remote volume, create the control
    if plan.commit_ref.lsn() == LSN::FIRST {
        remote
            .put_control(VolumeControl::new(
                plan.commit_ref.vid().clone(),
                None,
                SystemTime::now(),
            ))
            .await
            .or_into_ctx()?;
    }

    // make final preparations before pushing to the remote.
    // these preparations include checking preconditions and setting
    // pending_commit on the NamedVolume
    storage
        .remote_commit_prepare(&opts.name, &plan)
        .or_into_ctx()?;

    let commit = Commit::new(
        plan.commit_ref.vid().clone(),
        plan.commit_ref.lsn(),
        plan.page_count,
    )
    .with_commit_hash(Some(commit_hash))
    .with_segment_idx(Some(segment_idx));

    // issue the remote commit!
    let result = remote.put_commit(commit.clone()).await;

    match result {
        Ok(()) => {
            storage
                .remote_commit_success(&opts.name, commit)
                .or_into_ctx()?;
            Ok(())
        }
        Err(err) if err.ctx().is_already_exists() => {
            storage.drop_pending_commit(&opts.name).or_into_ctx()?;
            // TODO: mark rejected status somewhere or put it in a log
            tracing::warn!(
                "remote commit rejected for named volume {}, commit {} already exists",
                opts.name,
                plan.commit_ref
            );
            Ok(())
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

fn plan_commit(
    storage: &FjallStorage,
    name: &VolumeName,
) -> culprit::Result<Option<PendingCommit>, RuntimeErr> {
    let reader = storage.read();
    let Some(handle) = reader.named_volume(name).or_into_ctx()? else {
        return Err(VolumeErr::NamedVolumeNotFound(name.clone()).into());
    };
    if handle.pending_commit().is_some() {
        return Err(VolumeErr::NamedVolumeNeedsRecovery(name.clone()).into());
    }

    let latest_local = reader.snapshot(handle.local()).or_into_ctx()?;
    let page_count = reader.page_count(&latest_local).or_into_ctx()?;

    let Some(sync) = handle.sync() else {
        // this is the first time we are pushing this named volume to the remote
        let Some(latest_local_lsn) = latest_local.lsn() else {
            return Ok(None);
        };
        return Ok(Some(PendingCommit {
            local_vid: latest_local.vid().clone(),
            local_lsns: LSN::FIRST..=latest_local_lsn,
            page_count,
            commit_ref: VolumeRef::new(VolumeId::random(), LSN::FIRST),
            commit_hash: CommitHash::ZERO,
        }));
    };

    // load the latest remote snapshot
    let latest_remote = reader.snapshot(sync.remote().vid()).or_into_ctx()?;

    // calculate which LSNs we need to sync
    let Some(local_lsns) = sync.local_changes(&latest_local) else {
        // nothing to push
        return Ok(None);
    };

    // make sure the remote isn't ahead of the sync point
    if latest_remote.lsn() != Some(sync.remote().lsn()) {
        // the remote and local volumes have diverged
        let status = handle.sync_status(&latest_local, Some(&latest_remote));
        return Err(VolumeErr::NamedVolumeDiverged(name.clone(), status).into());
    }

    // calculate the commit result
    let commit_lsn = sync.remote().lsn().next().expect("maximum LSN exceeded");
    let commit_ref = VolumeRef::new(sync.remote().vid().clone(), commit_lsn);

    Ok(Some(PendingCommit {
        local_vid: latest_local.vid().clone(),
        local_lsns,
        commit_ref,
        page_count,
        commit_hash: CommitHash::ZERO,
    }))
}

fn build_segment(
    storage: &FjallStorage,
    plan: &PendingCommit,
) -> culprit::Result<(CommitHash, SegmentIdx, SmallVec<[Bytes; 1]>), RuntimeErr> {
    let reader = storage.read();

    // built a search path which only matches the LSNs we want to
    // include in the segment
    let segment_path = SearchPath::new(plan.local_vid.clone(), plan.local_lsns.clone());

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
