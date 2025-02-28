use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use config::File;
use culprit::{Culprit, ResultExt};
use graft_client::{
    ClientPair,
    runtime::{runtime::Runtime, storage::Storage},
};
use graft_core::gid::ClientId;
use graft_test::{
    Ticker, start_graft_backend,
    workload::{WorkloadConfig, WorkloadErr},
};

const WRITER_CONFIG: &str = r#"
type = "SimpleWriter"
vids = [ "GonuUEmt4XLJuHpcg7X35N" ]
interval_ms = 10
"#;

const READER_CONFIG: &str = r#"
type = "SimpleReader"
vids = [ "GonuUEmt4XLJuHpcg7X35N" ]
recv_timeout_ms = 10
"#;

fn sqlite_sanity_config(vfs_name: &str) -> String {
    format!(
        r#"
type = "SqliteSanity"
vids = [ "GonuUrdaXZTJVDLgbSg3cs" ]
interval_ms = 10
initial_accounts = 1000
vfs_name = "{vfs_name}"
    "#
    )
}

struct WorkloadRunner {
    runtime: Runtime,
    workload: JoinHandle<()>,
}

impl WorkloadRunner {
    fn run(
        name: &str,
        clients: ClientPair,
        ticker: Ticker,
        workload_conf: &str,
    ) -> Result<Self, Culprit<WorkloadErr>> {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().or_into_ctx()?;
        let runtime = Runtime::new(cid.clone(), clients, storage);
        runtime
            .start_sync_task(Duration::from_millis(100), 8, true, &format!("{name}-sync"))
            .or_into_ctx()?;
        let workload: WorkloadConfig = config::Config::builder()
            .add_source(File::from_str(workload_conf, config::FileFormat::Toml))
            .build()?
            .try_deserialize()?;
        let r2 = runtime.clone();
        let workload = thread::Builder::new()
            .name(name.into())
            .spawn(move || workload.execute(cid, r2, rand::rng(), ticker).unwrap())
            .unwrap();
        Ok(WorkloadRunner { runtime, workload })
    }
}

fn test_runners(runners: Vec<WorkloadRunner>) -> Result<(), Culprit<WorkloadErr>> {
    // run all workloads to completion or timeout
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut finished = false;
    while !finished && Instant::now() < deadline {
        finished = runners.iter().all(|r| r.workload.is_finished());
        thread::sleep(Duration::from_millis(100));
    }

    if !finished {
        panic!("workloads did not finish within timeout");
    }

    // shutdown runners
    for runner in runners {
        runner.workload.join().expect("workload failed");
        runner
            .runtime
            .shutdown_sync_task(Duration::from_secs(5))
            .or_into_ctx()?;
    }

    Ok(())
}

#[graft_test::test]
fn test_workloads_sanity() -> Result<(), Culprit<WorkloadErr>> {
    let (backend, clients) = start_graft_backend();

    let ticker = Ticker::new(50);

    let runners = vec![
        WorkloadRunner::run("writer-1", clients.clone(), ticker, WRITER_CONFIG)?,
        WorkloadRunner::run("writer-2", clients.clone(), ticker, WRITER_CONFIG)?,
        WorkloadRunner::run("reader", clients.clone(), ticker, READER_CONFIG)?,
    ];

    test_runners(runners)?;

    // shutdown backend
    backend.shutdown(Duration::from_secs(5)).or_into_ctx()?;

    Ok(())
}

#[graft_test::test]
fn test_sqlite_sanity() -> Result<(), Culprit<WorkloadErr>> {
    let (backend, clients) = start_graft_backend();

    let ticker = Ticker::new(50);

    let runners = vec![
        WorkloadRunner::run(
            "node-1",
            clients.clone(),
            ticker,
            &sqlite_sanity_config("node-1-vfs"),
        )?,
        WorkloadRunner::run(
            "node-2",
            clients.clone(),
            ticker,
            &sqlite_sanity_config("node-2-vfs"),
        )?,
    ];

    test_runners(runners)?;

    // shutdown backend
    backend.shutdown(Duration::from_secs(5)).or_into_ctx()?;

    Ok(())
}
