use std::{env::temp_dir, time::Duration};

use clap::Parser;
use config::Config;
use culprit::{Culprit, ResultExt};
use file_lock::{FileLock, FileOptions};
use graft_client::{
    runtime::{fetcher::NetFetcher, runtime::Runtime, storage::Storage},
    ClientBuilder, ClientPair, MetastoreClient, PagestoreClient,
};
use graft_test::{
    workload::{Workload, WorkloadErr},
    Ticker,
};
use graft_tracing::{running_in_antithesis, tracing_init, TracingConsumer};
use rand::Rng;
use url::Url;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// The workload file to run
    workload: String,
}

fn get_or_init_worker() -> (String, FileLock) {
    let locks = temp_dir().join("worker_locks");
    std::fs::create_dir_all(&locks)
        .expect(&format!("failed to create workers directory: {locks:?}"));

    for entry in
        std::fs::read_dir(&locks).expect(&format!("failed to read workers directory: {locks:?}"))
    {
        let entry = entry.expect("failed to read entry");
        let path = entry.path();
        assert!(path.is_file(), "locks dir should only contain files");

        let opts = FileOptions::new().read(true).write(true);
        if let Ok(lock) = FileLock::lock(&path, /*is_blocking*/ false, opts) {
            let worker_id = path.file_name().unwrap();
            assert!(worker_id.is_ascii(), "worker id is not ascii");
            let worker_id = worker_id.to_string_lossy().to_string();
            return (worker_id, lock);
        }
    }

    // we were unable to reuse an existing worker, create a new one
    let worker_id = bs58::encode(rand::random::<u64>().to_le_bytes()).into_string();
    let lock = locks.join(&worker_id);
    let opts = FileOptions::new().create(true).read(true).write(true);
    let lock = FileLock::lock(lock, /*is_blocking*/ false, opts)
        .expect("failed to create new worker lock");
    (worker_id, lock)
}

fn main() {
    if let Err(err) = main_inner() {
        tracing::error!(?err, "workload failed");
        std::process::exit(1);
    }
}

fn main_inner() -> Result<(), Culprit<WorkloadErr>> {
    precept::init();
    let mut rng = precept::random::rng();
    let (worker_id, _worker_lock) = get_or_init_worker();
    tracing_init(TracingConsumer::Test, Some(worker_id.clone()));
    let args = Args::parse();

    let workload: Workload = Config::builder()
        .add_source(config::Environment::with_prefix("WORKLOAD").separator("_"))
        .add_source(config::File::with_name(&args.workload))
        .build()?
        .try_deserialize()?;

    tracing::info!(
        workload_file = args.workload,
        ?workload,
        "STARTING TEST WORKLOAD"
    );

    let metastore_client: MetastoreClient =
        ClientBuilder::new(Url::parse("http://metastore:3001").unwrap())
            .build()
            .or_into_ctx()?;
    let pagestore_client: PagestoreClient =
        ClientBuilder::new(Url::parse("http://pagestore:3000").unwrap())
            .build()
            .or_into_ctx()?;
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = temp_dir().join("storage").join(&worker_id);
    let storage = Storage::open(storage_path).or_into_ctx()?;
    let runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    runtime
        .start_sync_task(clients, Duration::from_secs(1), 8)
        .or_into_ctx()?;

    precept::setup_complete!({ "workload": workload });

    let (ticker, shutdown_timeout) = if running_in_antithesis() {
        (
            Ticker::new(rng.gen_range(100..5000)),
            Duration::from_secs(3600),
        )
    } else {
        (Ticker::new(100), Duration::from_secs(60))
    };

    tracing::info!(?ticker, "running test workload");
    workload
        .run(&worker_id, runtime.clone(), rng, ticker)
        .or_into_ctx()?;

    tracing::info!("workload finished");
    tracing::info!("waiting for sync task to shutdown");
    runtime.shutdown_sync_task(shutdown_timeout).or_into_ctx()?;

    precept::expect_reachable!("test workload finishes", { "worker": worker_id });

    tracing::info!("shutdown complete");

    Ok(())
}
