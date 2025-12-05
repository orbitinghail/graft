use std::{
    env::temp_dir,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, ValueEnum};
use culprit::{Culprit, ResultExt};
use file_lock::{FileLock, FileOptions};
use graft::{
    GraftErr, LogicalErr,
    core::{LogId, VolumeId},
    remote::{RemoteConfig, RemoteErr},
    rt::runtime::Runtime,
    setup::{GraftConfig, InitErr, setup_graft},
};
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{SubscriberInitExt, TracingConsumer, setup_tracing};
use precept::dispatch::{antithesis::AntithesisDispatch, noop::NoopDispatch};
use rand::{
    Rng,
    distr::{Alphabetic, SampleString},
    seq::SliceRandom,
};
use rusqlite::Connection;
use sqlite_plugin::vfs::RegisterOpts;

#[derive(Clone, ValueEnum)]
enum RemoteType {
    Fs,
    S3Compatible,
}

#[derive(Parser)]
struct Args {
    #[clap(long)]
    rootdir: Option<PathBuf>,

    #[clap(long, default_value = "fs")]
    remote: RemoteType,

    #[clap(long, default_value = "74ggciv9wN-3y7Sx8h6qCJmt")]
    log: LogId,

    #[command(subcommand)]
    workload: Workload,
}

#[derive(Debug, thiserror::Error)]
enum TestErr {
    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error(transparent)]
    GraftErr(#[from] GraftErr),

    #[error(transparent)]
    RemoteErr(#[from] RemoteErr),

    #[error(transparent)]
    InitErr(#[from] InitErr),

    #[error(transparent)]
    RusqliteErr(#[from] rusqlite::Error),
}

fn get_or_init_data_dir(rng: &mut impl Rng, rootdir: &Path) -> (PathBuf, FileLock) {
    let rootdir = rootdir.join("clients");
    std::fs::create_dir_all(&rootdir).expect("failed to create clients directory");
    let mut entries = std::fs::read_dir(&rootdir)
        .expect("failed to read clients directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to read clients directory");

    // shuffle entries so we have an even chance of picking any client
    entries.shuffle(rng);

    for entry in entries {
        let path = entry.path();
        assert!(path.is_dir(), "locks dir should only contain directories");
        let lock_path = path.join("test_lock");
        let opts = FileOptions::new().create(true).read(true).write(true);
        if let Ok(lock) = FileLock::lock(lock_path, /*is_blocking*/ false, opts) {
            return (path, lock);
        }
    }

    // we were unable to reuse an existing datadir, create a new one
    let name = Alphabetic.sample_string(rng, 16);
    let path = rootdir.join(name);
    let lock_path = path.join("test_lock");
    std::fs::create_dir_all(&path).expect("failed to create client directory");
    let opts = FileOptions::new().create(true).read(true).write(true);
    let lock = FileLock::lock(lock_path, /*is_blocking*/ false, opts)
        .expect("failed to create new worker lock");
    (path, lock)
}

#[derive(Subcommand)]
enum Workload {
    BankSetup,
    BankTx,
    BankValidate,
}

fn main() -> Result<(), Culprit<TestErr>> {
    let dispatcher =
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init_boxed(dispatcher).expect("failed to setup precept");
    setup_tracing(TracingConsumer::Test).init();

    let args = Args::parse();
    let rootdir = args
        .rootdir
        .unwrap_or_else(|| temp_dir().join("graft_test_root"));
    let remote = match args.remote {
        RemoteType::Fs => {
            let remoteroot = rootdir.join("remote");
            std::fs::create_dir_all(&remoteroot).or_into_ctx()?;
            RemoteConfig::Fs { root: remoteroot }
        }
        RemoteType::S3Compatible => RemoteConfig::S3Compatible {
            bucket: "primary".to_string(),
            prefix: None,
        },
    };

    let mut rng = precept::random::rng();
    let (data_dir, _lock) = get_or_init_data_dir(&mut rng, &rootdir);

    // create the Graft runtime
    let runtime = setup_graft(GraftConfig { remote, data_dir, autosync: None }).or_into_ctx()?;

    // initialize the main tag if needed
    let vid = if let Some(vid) = runtime.tag_get("main").or_into_ctx()? {
        vid
    } else {
        let volume = runtime
            .volume_open(None, None, Some(args.log.clone()))
            .or_into_ctx()?;
        runtime
            .tag_replace("main", volume.vid.clone())
            .or_into_ctx()?;
        volume.vid
    };

    // register the Graft VFS with SQLite
    let vfs = GraftVfs::new(runtime.clone());
    sqlite_plugin::vfs::register_static(c"graft".into(), vfs, RegisterOpts { make_default: true })
        .expect("failed to register vfs with SQLite");

    // open a sqlite connection
    let sqlite = Connection::open("main").or_into_ctx()?;

    let env = Env { rng, runtime, vid, log: args.log, sqlite };
    match args.workload {
        Workload::BankSetup => bank_setup(env),
        Workload::BankTx => bank_tx(env),
        Workload::BankValidate => bank_validate(env),
    }
}

struct Env<R> {
    rng: R,
    runtime: Runtime,
    vid: VolumeId,
    log: LogId,
    sqlite: Connection,
}

const NUM_ACCOUNTS: usize = 100_000;
const INITIAL_BALANCE: u64 = 1_000;
const TOTAL_BALANCE: u64 = NUM_ACCOUNTS as u64 * INITIAL_BALANCE;

fn bank_setup<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
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
) -> Result<(), Culprit<TestErr>> {
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

fn bank_tx<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
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
                TestErr::GraftErr(GraftErr::Logical(LogicalErr::VolumeDiverged(_))) => {
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

fn bank_validate<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
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
}
