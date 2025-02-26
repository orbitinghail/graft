use std::{env::temp_dir, time::Duration};

use clap::Parser;
use config::Config;
use culprit::{Culprit, ResultExt};
use file_lock::{FileLock, FileOptions};
use graft_client::{
    runtime::{runtime::Runtime, storage::Storage},
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
};
use graft_core::gid::ClientId;
use graft_test::{
    workload::{Workload, WorkloadErr},
    Ticker,
};
use graft_tracing::{init_tracing, running_in_antithesis, TracingConsumer};
use precept::dispatch::{antithesis::AntithesisDispatch, noop::NoopDispatch};
use rand::Rng;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// The workload file to run
    workload: String,
}

fn get_or_init_cid() -> (ClientId, FileLock) {
    let locks = temp_dir().join("worker_locks");
    std::fs::create_dir_all(&locks)
        .unwrap_or_else(|_| panic!("failed to create workers directory: {locks:?}"));

    for entry in std::fs::read_dir(&locks)
        .unwrap_or_else(|_| panic!("failed to read workers directory: {locks:?}"))
    {
        let entry = entry.expect("failed to read entry");
        let path = entry.path();
        assert!(path.is_file(), "locks dir should only contain files");

        let opts = FileOptions::new().read(true).write(true);
        if let Ok(lock) = FileLock::lock(&path, /*is_blocking*/ false, opts) {
            let file_name = path.file_name().unwrap();
            assert!(file_name.is_ascii(), "worker id is not ascii");
            let file_name = file_name.to_string_lossy();
            let cid: ClientId = file_name
                .parse()
                .unwrap_or_else(|_| panic!("failed to parse ClientId from {file_name}"));
            return (cid, lock);
        }
    }

    // we were unable to reuse an existing worker, create a new one
    let cid = ClientId::random();
    let lock = locks.join(cid.to_string());
    let opts = FileOptions::new().create(true).read(true).write(true);
    let lock = FileLock::lock(lock, /*is_blocking*/ false, opts)
        .expect("failed to create new worker lock");
    (cid, lock)
}

fn main() {
    if let Err(err) = main_inner() {
        tracing::error!(?err, "workload failed");
        std::process::exit(1);
    }
}

fn main_inner() -> Result<(), Culprit<WorkloadErr>> {
    let dispatcher =
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init(Box::leak(dispatcher)).expect("failed to setup precept");

    let mut rng = precept::random::rng();
    let (cid, _worker_lock) = get_or_init_cid();
    init_tracing(TracingConsumer::Test, Some(cid.short()));
    let args = Args::parse();

    let workload: Workload = Config::builder()
        .add_source(config::Environment::with_prefix("WORKLOAD").separator("_"))
        .add_source(config::File::with_name(&args.workload))
        .build()?
        .try_deserialize()?;

    tracing::info!(
        workload_file = args.workload,
        ?workload,
        ?cid,
        "STARTING TEST WORKLOAD"
    );

    let client = NetClient::new();
    let metastore_client =
        MetastoreClient::new("http://metastore:3001".parse().unwrap(), client.clone());
    let pagestore_client =
        PagestoreClient::new("http://pagestore:3000".parse().unwrap(), client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = temp_dir().join("storage").join(cid.pretty());
    let storage = Storage::open(storage_path).or_into_ctx()?;
    let runtime = Runtime::new(cid.clone(), clients, storage);
    runtime
        .start_sync_task(Duration::from_secs(1), 8, true)
        .or_into_ctx()?;

    precept::setup_complete!({ "workload": workload });

    let (ticker, shutdown_timeout) = if running_in_antithesis() {
        (
            Ticker::new(rng.random_range(100..5000)),
            Duration::from_secs(3600),
        )
    } else {
        (Ticker::new(100), Duration::from_secs(60))
    };

    tracing::info!(?ticker, "running test workload");
    workload
        .run(cid.clone(), runtime.clone(), rng, ticker)
        .or_into_ctx()?;

    tracing::info!("workload finished");
    tracing::info!("waiting for sync task to shutdown");
    runtime.shutdown_sync_task(shutdown_timeout).or_into_ctx()?;

    precept::expect_reachable!("test workload finishes", { "worker": cid });

    tracing::info!("shutdown complete");

    Ok(())
}
