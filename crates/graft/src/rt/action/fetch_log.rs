use crate::core::{
    LogId,
    checkpoints::Checkpoints,
    lsn::{LSN, LSNRangeExt},
};
use culprit::ResultExt;
use itertools::{EitherOrBoth, Itertools};
use range_set_blaze::RangeOnce;
use tokio_stream::StreamExt;

use crate::{
    local::fjall_storage::{FjallStorage, ReadGuard, WriteBatch},
    remote::Remote,
    rt::action::{Action, Result},
};

/// Fetches new commits and metadata from a remote.
#[derive(Debug)]
pub struct FetchLog {
    pub log: LogId,
    pub max_lsn: Option<LSN>,
}

impl Action for FetchLog {
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> Result<()> {
        let reader = storage.read();
        let mut batch = storage.batch();

        // refresh checkpoint commits if needed
        refresh_checkpoint_commits(&reader, &mut batch, remote, &self.log).await?;

        // calculate the lsn range to retrieve
        let start = reader
            .latest_lsn(&self.log)
            .or_into_ctx()?
            .map_or(LSN::FIRST, |lsn| lsn.next());
        let end = self.max_lsn.unwrap_or(LSN::LAST);
        let lsns = start..=end;

        tracing::debug!(log = ?self.log, lsns = %lsns.to_string(), "fetching log");

        // figure out which lsns we are missing
        let existing_lsns = storage.read().lsns(&self.log, &lsns).or_into_ctx()?;
        let missing_lsns =
            (RangeOnce::new(lsns) - existing_lsns.into_ranges()).flat_map(|r| r.iter());

        // fetch missing lsns
        let mut commits = remote.stream_commits_ordered(&self.log, missing_lsns);
        while let Some(commit) = commits.try_next().await.or_into_ctx()? {
            batch.write_commit(commit);
        }

        batch.commit().or_into_ctx()
    }
}

async fn refresh_checkpoint_commits(
    reader: &ReadGuard<'_>,
    batch: &mut WriteBatch<'_>,
    remote: &Remote,
    log: &LogId,
) -> Result<()> {
    let cached_checkpoints = reader.checkpoints(log).or_into_ctx()?;
    let (old_etag, old_checkpoints) = match &cached_checkpoints {
        Some(c) => (c.etag().map(|e| e.to_string()), c.checkpoints()),
        None => (None, &Checkpoints::EMPTY),
    };

    let new_checkpoints = match remote.get_checkpoints(log, old_etag).await {
        Ok(c) => c,
        Err(err) if err.ctx().is_not_modified() || err.ctx().is_not_found() => return Ok(()),
        Err(err) => Err(err).or_into_ctx()?,
    };

    // Checkpoints are sorted, thus we can merge join the two lists of LSNs to
    // figure out which ones were added.
    let added: Vec<LSN> = old_checkpoints
        .iter()
        .merge_join_by(new_checkpoints.checkpoints().iter(), Ord::cmp)
        .filter_map(|join| match join {
            EitherOrBoth::Right(v) => Some(*v),
            _ => None,
        })
        .collect();

    let mut commits = remote.stream_commits_ordered(log, added);
    while let Some(commit) = commits.try_next().await.or_into_ctx()? {
        batch.write_commit(commit);
    }
    Ok(())
}
