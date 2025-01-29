use std::time::Duration;

use clap::Parser;
use config::Config;
use culprit::{Culprit, ResultExt};
use graft_client::{
    runtime::{fetcher::NetFetcher, runtime::Runtime, storage::Storage},
    ClientBuilder, ClientPair, MetastoreClient, PagestoreClient,
};
use graft_test::{
    workload::{Workload, WorkloadErr},
    Ticker,
};
use graft_tracing::{running_in_antithesis, tracing_init, TracingConsumer, PROCESS_ID};
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
    let worker_id = PROCESS_ID.as_str();
    precept::init();
    let mut rng = precept::random::rng();
    tracing_init(TracingConsumer::Test);
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
