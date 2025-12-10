use std::{
    env::temp_dir,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand, ValueEnum};
use file_lock::{FileLock, FileOptions};
use graft::{
    GraftErr, LogicalErr,
    core::LogId,
    remote::{RemoteConfig, RemoteErr},
    setup::{GraftConfig, InitErr, setup_graft},
};
use graft_sqlite::vfs::GraftVfs;
use graft_test::{
    require,
    workload::{Env, WorkloadErr, bank_setup, bank_tx, bank_validate, pull_if_empty},
};
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
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Graft(#[from] GraftErr),

    #[error(transparent)]
    Workload(#[from] WorkloadErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error(transparent)]
    Init(#[from] InitErr),

    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
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
#[allow(
    clippy::enum_variant_names,
    reason = "designed to support other workloads"
)]
enum Workload {
    BankSetup,
    BankTx,
    BankValidate,
}

fn main() -> Result<(), TestErr> {
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
            std::fs::create_dir_all(&remoteroot)?;
            RemoteConfig::Fs { root: remoteroot }
        }
        RemoteType::S3Compatible => RemoteConfig::S3Compatible {
            bucket: "primary".to_string(),
            prefix: None,
        },
    };

    let mut rng = precept::random::rng();
    let (data_dir, _lock) = get_or_init_data_dir(&mut rng, &rootdir);

    // 10% of the time disable all precept faults
    if rng.random_ratio(1, 10) {
        precept::disable_faults();
        tracing::warn!("Precept Faults disabled");
    }

    // create the Graft runtime
    let runtime = setup_graft(GraftConfig { remote, data_dir, autosync: None })?;

    // initialize the main tag if needed
    let vid = if let Some(vid) = runtime.tag_get("main")? {
        vid
    } else {
        let volume = runtime.volume_open(None, None, Some(args.log.clone()))?;
        runtime.tag_replace("main", volume.vid.clone())?;
        volume.vid
    };

    // register the Graft VFS with SQLite
    let vfs = GraftVfs::new(runtime.clone());
    sqlite_plugin::vfs::register_static(c"graft".into(), vfs, RegisterOpts { make_default: true })
        .expect("failed to register vfs with SQLite");

    // open a sqlite connection
    let sqlite = Connection::open("main")?;

    // build the test environment
    let mut env = Env { rng, runtime, vid, log: args.log, sqlite };

    // pull the volume if it's empty
    pull_if_empty(&env)?;

    // run the workload until it completes without running into a retryable or
    // recoverable error
    loop {
        let result = match args.workload {
            Workload::BankSetup => bank_setup(&mut env),
            Workload::BankTx => bank_tx(&mut env),
            Workload::BankValidate => bank_validate(&mut env),
        };

        match result {
            Ok(()) => return Ok(()),
            Err(WorkloadErr::GraftErr(GraftErr::Logical(LogicalErr::VolumeDiverged(_)))) => {
                tracing::warn!("volume diverged, performing recovery and retrying");

                precept::expect_reachable!("volume diverged");

                // reopen the remote and update the tag
                let volume = env.runtime.volume_open(None, None, Some(env.log.clone()))?;
                env.runtime.tag_replace("main", volume.vid.clone())?;
                env.vid = volume.vid;

                // make sure we are up to date with the remote
                env.runtime.volume_pull(env.vid.clone())?;

                // verify no divergence in status
                let status = env.runtime.volume_status(&env.vid)?;
                require!(
                    !status.has_diverged(),
                    "volume is not diverged post recovery"
                );

                // reopen sqlite connection with new volume
                env.sqlite = Connection::open("main")?;
            }
            Err(err) if err.should_retry() => (),
            Err(err) => return Err(err.into()),
        }
    }
}
