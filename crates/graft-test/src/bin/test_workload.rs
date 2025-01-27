use std::time::Duration;

use clap::Parser;
use config::Config;
use culprit::{Culprit, ResultExt};
use graft_client::{
    runtime::{fetcher::NetFetcher, runtime::Runtime, storage::Storage},
    ClientBuilder, ClientPair, MetastoreClient, PagestoreClient,
};
use graft_test::{
    running_in_antithesis,
    test_tracing::tracing_init,
    worker_id,
    workload::{Workload, WorkloadErr},
};
use rand::Rng;
use url::Url;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// The workload file to run
    workload: String,
}

fn main() -> Result<(), Culprit<WorkloadErr>> {
    precept::init();
    let mut rng = precept::random::rng();
    let worker_id = worker_id(&mut rng);
    tracing_init(Some(worker_id.clone()));
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

    let storage = Storage::open_temporary().unwrap();
    let runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    runtime
        .start_sync_task(clients, Duration::from_secs(1), 8)
        .or_into_ctx()?;

    precept::setup_complete!({ "workload": workload });

    let (ticks, shutdown_timeout) = if running_in_antithesis() {
        (rng.gen_range(100..5000), Duration::from_secs(3600))
    } else {
        (100, Duration::from_secs(60))
    };

    tracing::info!(?ticks, "running test workload");
    workload
        .run(&worker_id, runtime.clone(), rng, ticks)
        .or_into_ctx()?;

    tracing::info!("workload finished");
    tracing::info!("waiting for sync task to shutdown");
    runtime.shutdown_sync_task(shutdown_timeout).or_into_ctx()?;

    precept::expect_reachable!("test workload finishes", { "worker": worker_id });

    tracing::info!("shutdown complete");

    Ok(())
}
