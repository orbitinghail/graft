use crate::{PageHash, Ticker, workload::fjall_storage_bench::FjallStorageBench};

use super::{PageTracker, PageTrackerErr};
use config::ConfigError;
use culprit::{Culprit, ResultExt};
use enum_dispatch::enum_dispatch;
use graft_client::{
    ClientErr,
    oracle::Oracle,
    runtime::{
        runtime::Runtime,
        storage::{
            StorageErr,
            volume_state::{SyncDirection, VolumeStatus},
        },
        sync::{ShutdownErr, StartupErr},
        volume_handle::VolumeHandle,
        volume_reader::{VolumeRead, VolumeReader},
    },
};
use graft_core::{gid::ClientId, page::PageSizeErr, zerocopy_ext::ZerocopyErr};
use graft_proto::GraftErrCode;
use graft_server::supervisor;
use precept::expect_always_or_unreachable;
use rand::Rng;
use serde::{Deserialize, Serialize};
use simple_reader::SimpleReader;
use simple_writer::SimpleWriter;
use sqlite_sanity::SqliteSanity;
use thiserror::Error;
use tracing::field;
use zerocopy::{CastError, FromBytes, SizeError};

pub mod fjall_storage_bench;
pub mod simple_reader;
pub mod simple_writer;
pub mod sqlite_sanity;

#[derive(Debug, Error)]
pub enum WorkloadErr {
    #[error("invalid workload configuration")]
    InvalidConfig,

    #[error("client error: {0}")]
    ClientErr(#[from] ClientErr),

    #[error("sync task startup error: {0}")]
    SyncTaskStartupErr(#[from] StartupErr),

    #[error("sync task shutdown error: {0}")]
    SyncTaskShutdownErr(#[from] ShutdownErr),

    #[error("page tracker error: {0}")]
    PageTrackerErr(#[from] PageTrackerErr),

    #[error("supervisor shutdown error: {0}")]
    SupervisorShutdownErr(#[from] supervisor::ShutdownErr),

    #[error("uniform rng error")]
    RngErr(#[from] rand::distr::uniform::Error),

    #[error(transparent)]
    ZerocopyErr(#[from] ZerocopyErr),

    #[error(transparent)]
    PageSizeErr(#[from] PageSizeErr),

    #[error(transparent)]
    RusqliteErr(#[from] rusqlite::Error),

    #[error("I/O error: {0}")]
    IoErr(#[from] std::io::Error),

    #[error("Executed command {cmd} failed. Stderr:\n{stderr}")]
    Exec { cmd: &'static str, stderr: String },
}

impl From<StorageErr> for WorkloadErr {
    fn from(err: StorageErr) -> Self {
        WorkloadErr::ClientErr(ClientErr::StorageErr(err))
    }
}

impl From<ConfigError> for WorkloadErr {
    fn from(_: ConfigError) -> Self {
        WorkloadErr::InvalidConfig
    }
}

impl<A, B> From<CastError<A, B>> for WorkloadErr {
    fn from(err: CastError<A, B>) -> Self {
        WorkloadErr::ZerocopyErr(err.into())
    }
}

impl<A, B> From<SizeError<A, B>> for WorkloadErr {
    fn from(err: SizeError<A, B>) -> Self {
        WorkloadErr::ZerocopyErr(err.into())
    }
}

impl WorkloadErr {
    fn should_retry(&self) -> bool {
        fn should_retry_io(err: std::io::ErrorKind) -> bool {
            matches!(
                err,
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::NetworkDown
                    | std::io::ErrorKind::NetworkUnreachable
            )
        }

        match self {
            WorkloadErr::ClientErr(ClientErr::GraftErr(err)) => matches!(
                err.code(),
                GraftErrCode::CommitRejected
                    | GraftErrCode::SnapshotMissing
                    | GraftErrCode::ServiceUnavailable
            ),
            WorkloadErr::ClientErr(ClientErr::HttpErr(err)) => match err {
                ureq::Error::ConnectionFailed
                | ureq::Error::HostNotFound
                | ureq::Error::Timeout(_) => true,
                ureq::Error::Decompress(_, ioerr) => should_retry_io(ioerr.kind()),
                ureq::Error::Io(ioerr) => should_retry_io(ioerr.kind()),
                _ => false,
            },
            WorkloadErr::ClientErr(ClientErr::IoErr(err)) => should_retry_io(*err),
            WorkloadErr::ClientErr(ClientErr::StorageErr(
                StorageErr::ConcurrentWrite | StorageErr::RemoteConflict,
            )) => true,
            WorkloadErr::RusqliteErr(rusqlite::Error::SqliteFailure(err, _)) => matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::SystemIoFailure
            ),
            WorkloadErr::IoErr(ioerr) => should_retry_io(ioerr.kind()),
            _ => false,
        }
    }
}

pub struct WorkloadEnv<R: Rng> {
    cid: ClientId,
    runtime: Runtime,
    rng: R,
    ticker: Ticker,
}

#[enum_dispatch]
#[allow(unused_variables)]
pub trait Workload {
    fn setup<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>> {
        Ok(())
    }

    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>>;
}

#[enum_dispatch(Workload)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum WorkloadConfig {
    SimpleWriter,
    SimpleReader,
    SqliteSanity,
    FjallStorageBench,
}

impl WorkloadConfig {
    pub fn execute<R: Rng>(
        mut self,
        cid: ClientId,
        runtime: Runtime,
        rng: R,
        ticker: Ticker,
    ) -> Result<(), Culprit<WorkloadErr>> {
        let mut env = WorkloadEnv { cid, runtime, rng, ticker };

        self.setup(&mut env)?;

        while env.ticker.tick() {
            if let Err(err) = self.run(&mut env) {
                if err.ctx().should_retry() {
                    tracing::warn!("retrying workload after error: {:?}", err);
                    precept::expect_reachable!("retryable error occurred");
                    continue;
                } else {
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

pub fn recover_and_sync_volume(
    cid: &ClientId,
    handle: &VolumeHandle,
) -> Result<(), Culprit<WorkloadErr>> {
    let vid = handle.vid();
    let status = handle.status().or_into_ctx()?;
    let span = tracing::info_span!(
        "recover_and_sync_volume",
        ?status,
        ?vid,
        ?cid,
        result = field::Empty
    )
    .entered();

    match status {
        VolumeStatus::Ok | VolumeStatus::InterruptedPush => {
            precept::expect_sometimes!(
                status == VolumeStatus::InterruptedPush,
                "volume has an interrupted push",
                { "vid": handle.vid(), "cid": cid, "status": status }
            );

            // attempt to sync with the remote, resetting the volume on conflict
            if let Err(err) = handle.sync_with_remote(SyncDirection::Both) {
                match err.ctx() {
                    ClientErr::GraftErr(err) if err.code() == GraftErrCode::CommitRejected => {
                        handle.reset_to_remote().or_into_ctx()?;
                    }
                    ClientErr::StorageErr(
                        StorageErr::VolumeIsSyncing | StorageErr::RemoteConflict,
                    ) => {
                        handle.reset_to_remote().or_into_ctx()?;
                    }
                    _ => return Err(err.map_ctx(WorkloadErr::from)),
                }
            }
        }
        VolumeStatus::RejectedCommit | VolumeStatus::Conflict => {
            precept::expect_reachable!("volume needs reset", {
                "vid": handle.vid(), "cid": cid, "status": status
            });
            // reset the volume to the latest remote snapshot
            handle.reset_to_remote().or_into_ctx()?;
        }
    }

    span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));

    Ok(())
}

pub fn load_tracker(
    oracle: &mut impl Oracle,
    reader: &VolumeReader,
    cid: &ClientId,
) -> Result<PageTracker, Culprit<WorkloadErr>> {
    let span = tracing::info_span!("load_tracker", snapshot=?reader.snapshot(), hash=field::Empty)
        .entered();

    // load the page tracker from the volume
    let first_page = reader.read(oracle, PageTracker::PAGEIDX).or_into_ctx()?;

    // record the hash of the page tracker for debugging
    span.record("hash", PageHash::new(&first_page).to_string());

    let page_tracker = PageTracker::read_from_bytes(&first_page[..])?;

    // ensure the page tracker is only empty when we expect it to be
    expect_always_or_unreachable!(
        page_tracker.is_empty() ^ reader.snapshot().is_some(),
        "page tracker should only be empty when the snapshot is missing",
        {
            "snapshot": reader.snapshot(),
            "cid": cid
        }
    );

    Ok(page_tracker)
}
