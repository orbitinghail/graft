use std::{
    env::temp_dir,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, ValueEnum};
use culprit::{Culprit, ResultExt};
use file_lock::{FileLock, FileOptions};
use graft::{
    GraftErr,
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
    let rootdir = rootdir.join("graft_test_clients");
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
        let opts = FileOptions::new().read(true).write(true);
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

    // ensure the main tag points at the specified remote log
    let volume = runtime
        .volume_open(None, None, Some(args.log))
        .or_into_ctx()?;
    runtime
        .tag_replace("main", volume.vid.clone())
        .or_into_ctx()?;

    // register the Graft VFS with SQLite
    let vfs = GraftVfs::new(runtime.clone());
    sqlite_plugin::vfs::register_static(c"graft".into(), vfs, RegisterOpts { make_default: true })
        .expect("failed to register vfs with SQLite");

    // open a sqlite connection
    let sqlite = Connection::open("main").or_into_ctx()?;

    let env = Env {
        rng,
        runtime: runtime,
        vid: volume.vid,
        sqlite,
    };

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
    sqlite: Connection,
}

const NUM_ACCOUNTS: usize = 10_000;
const INITIAL_BALANCE: u64 = 1_000;
const TOTAL_BALANCE: u64 = NUM_ACCOUNTS as u64 * INITIAL_BALANCE;

fn bank_setup<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
    // create an accounts table with an integer primary key and a balance
    // initialize the accounts table with NUM_ACCOUNTS each starting with INITIAL_BALANCE
    // run runtime.volume_push
    Ok(())
}

fn bank_tx<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
    // randomly choose a number of transactions to make between 1 and 100
    // for each transaction randomly pick two account ids (they are between 0 and NUM_ACCOUNTS)
    //   start a sql tx
    //   check both account balances and send half of the balance of the larger account to the smaller account
    //   commit the tx
    //
    // run runtime.volume_push at the end
    Ok(())
}

fn bank_validate<R: Rng>(env: Env<R>) -> Result<(), Culprit<TestErr>> {
    // verify that the total balance (sum(balance)) is equal to TOTAL_BALANCE
    Ok(())
}
