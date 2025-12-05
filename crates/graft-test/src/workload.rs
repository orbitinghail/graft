<<<<<<< HEAD
use crate::{PageHash, Ticker};

use super::{PageTracker, PageTrackerErr};
use config::ConfigError;
use enum_dispatch::enum_dispatch;
use graft::core::{gid::ClientId, page::PageSizeErr, zerocopy_ext::ZerocopyErr};
use precept::expect_always_or_unreachable;
use rand::Rng;
use serde::{Deserialize, Serialize};
use simple_reader::SimpleReader;
use simple_writer::SimpleWriter;
use sqlite_sanity::SqliteSanity;
use thiserror::Error;
use tracing::field;
use zerocopy::{CastError, FromBytes, SizeError};

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
    fn setup<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        Ok(())
    }

    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr>;
}

#[enum_dispatch(Workload)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum WorkloadConfig {
    SimpleWriter,
    SimpleReader,
    SqliteSanity,
}

impl WorkloadConfig {
    pub fn execute<R: Rng>(
        mut self,
        cid: ClientId,
        runtime: Runtime,
        rng: R,
        ticker: Ticker,
    ) -> Result<(), WorkloadErr> {
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

pub fn recover_and_sync_volume(cid: &ClientId, handle: &VolumeHandle) -> Result<(), WorkloadErr> {
    let vid = handle.vid();
    let status = handle.status()?;
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
                        handle.reset_to_remote()?;
                    }
                    ClientErr::StorageErr(
                        StorageErr::VolumeIsSyncing | StorageErr::RemoteConflict,
                    ) => {
                        handle.reset_to_remote()?;
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
            handle.reset_to_remote()?;
        }
    }

    span.record("result", format!("{:?}", handle.snapshot()?));

    Ok(())
}

pub fn load_tracker(
    oracle: &mut impl Oracle,
    reader: &VolumeReader,
    cid: &ClientId,
) -> Result<PageTracker, WorkloadErr> {
    let span = tracing::info_span!("load_tracker", snapshot=?reader.snapshot(), hash=field::Empty)
        .entered();

    // load the page tracker from the volume
    let first_page = reader.read(oracle, PageTracker::PAGEIDX)?;

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
||||||| parent of 073ea35 (make workloads reusable)
=======
use culprit::{Culprit, ResultExt};
use graft::{
    GraftErr, LogicalErr,
    core::{LogId, VolumeId},
    rt::runtime::Runtime,
};
use rand::Rng;
use rusqlite::Connection;

#[derive(Debug, thiserror::Error)]
pub enum WorkloadErr {
    #[error(transparent)]
    GraftErr(#[from] GraftErr),

    #[error(transparent)]
    RusqliteErr(#[from] rusqlite::Error),
}

pub struct Env<R> {
    pub rng: R,
    pub runtime: Runtime,
    pub vid: VolumeId,
    pub log: LogId,
    pub sqlite: Connection,
}

const NUM_ACCOUNTS: usize = 100_000;
const INITIAL_BALANCE: u64 = 1_000;
const TOTAL_BALANCE: u64 = NUM_ACCOUNTS as u64 * INITIAL_BALANCE;

pub fn bank_setup<R: Rng>(env: Env<R>) -> Result<(), Culprit<WorkloadErr>> {
    let Env { mut sqlite, runtime, vid, .. } = env;

    tracing::info!("setting up bank workload with {} accounts", NUM_ACCOUNTS);

    // start a sql tx
    let tx = sqlite.transaction().or_into_ctx()?;

    tx.execute("DROP TABLE if exists accounts", [])
        .or_into_ctx()?;

    // create an accounts table with an integer primary key and a balance
    tx.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)",
        [],
    )
    .or_into_ctx()?;

    // initialize the accounts table with NUM_ACCOUNTS each starting with INITIAL_BALANCE
    let mut stmt = tx
        .prepare("INSERT OR IGNORE INTO accounts (id, balance) VALUES (?, ?)")
        .or_into_ctx()?;
    for id in 0..NUM_ACCOUNTS {
        stmt.execute([id as i64, INITIAL_BALANCE as i64])
            .or_into_ctx()?;
    }
    drop(stmt);

    tx.commit().or_into_ctx()?;

    // run runtime.volume_push
    runtime.volume_push(vid).or_into_ctx()?;

    Ok(())
}

fn run_bank_transactions(
    rng: &mut impl Rng,
    sqlite: &mut Connection,
    runtime: &Runtime,
    vid: &VolumeId,
) -> Result<(), Culprit<WorkloadErr>> {
    // randomly choose a number of transactions to make
    let num_transactions = rng.random_range(1..=100);

    tracing::info!("performing {} bank transactions", num_transactions);

    for _ in 0..num_transactions {
        // randomly pick two account ids (they are between 0 and NUM_ACCOUNTS)
        let id_a = rng.random_range(0..NUM_ACCOUNTS) as i64;
        let id_b = rng.random_range(0..NUM_ACCOUNTS) as i64;
        if id_a == id_b {
            continue;
        }

        // start a sql tx
        let tx = sqlite.transaction().or_into_ctx()?;

        // check both account balances
        let balance_a: i64 = tx
            .query_row("SELECT balance FROM accounts WHERE id = ?", [id_a], |row| {
                row.get(0)
            })
            .or_into_ctx()?;
        let balance_b: i64 = tx
            .query_row("SELECT balance FROM accounts WHERE id = ?", [id_b], |row| {
                row.get(0)
            })
            .or_into_ctx()?;

        // send half of the balance of the larger account to the smaller account
        let (from_id, to_id, transfer_amount) = if balance_a > balance_b {
            (id_a, id_b, balance_a / 2)
        } else {
            (id_b, id_a, balance_b / 2)
        };

        if transfer_amount > 0 {
            tx.execute(
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                [transfer_amount, from_id],
            )
            .or_into_ctx()?;
            tx.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                [transfer_amount, to_id],
            )
            .or_into_ctx()?;
        }

        // commit the tx
        tx.commit().or_into_ctx()?;
    }

    // attempt to push
    runtime.volume_push(vid.clone()).or_into_ctx()?;

    Ok(())
}

pub fn bank_tx<R: Rng>(env: Env<R>) -> Result<(), Culprit<WorkloadErr>> {
    let Env {
        mut rng,
        mut sqlite,
        runtime,
        mut vid,
        log,
    } = env;

    loop {
        match run_bank_transactions(&mut rng, &mut sqlite, &runtime, &vid) {
            Ok(()) => return Ok(()),
            Err(err) => match err.ctx() {
                WorkloadErr::GraftErr(GraftErr::Logical(LogicalErr::VolumeDiverged(_))) => {
                    tracing::warn!("volume diverged, performing recovery and retrying");
                    // close the sqlite connection to release the volume
                    drop(sqlite);

                    // reopen the remote and update the tag
                    let volume = runtime
                        .volume_open(None, None, Some(log.clone()))
                        .or_into_ctx()?;
                    runtime
                        .tag_replace("main", volume.vid.clone())
                        .or_into_ctx()?;
                    vid = volume.vid;

                    // make sure we are up to date with the remote
                    runtime.volume_pull(vid.clone()).or_into_ctx()?;

                    // reopen sqlite connection with new volume
                    sqlite = Connection::open("main").or_into_ctx()?;
                }
                _ => return Err(err),
            },
        }
    }
}

pub fn bank_validate<R: Rng>(env: Env<R>) -> Result<(), Culprit<WorkloadErr>> {
    let Env { sqlite, runtime, vid, .. } = env;

    tracing::info!("validating bank workload");

    // pull the database
    runtime.volume_pull(vid).or_into_ctx()?;

    // verify that the total balance (sum(balance)) is equal to TOTAL_BALANCE
    let total: i64 = sqlite
        .query_row("SELECT SUM(balance) FROM accounts", [], |row| row.get(0))
        .or_into_ctx()?;

    assert_eq!(
        total as u64, TOTAL_BALANCE,
        "total balance mismatch: expected {}, got {}",
        TOTAL_BALANCE, total
    );

    Ok(())
>>>>>>> 073ea35 (make workloads reusable)
}
