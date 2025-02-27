use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use config::File;
use culprit::{Culprit, ResultExt};
use graft_client::runtime::{runtime::Runtime, storage::Storage};
use graft_core::gid::ClientId;
use graft_test::{
    Ticker, start_graft_backend,
    workload::{WorkloadConfig, WorkloadErr},
};

struct WorkloadRunner {
    runtime: Runtime,
    workload: JoinHandle<Result<(), Culprit<WorkloadErr>>>,
}

const WRITER_CONFIG: &str = r#"
type = "SimpleWriter"
vids = [ "GonuUEmt4XLJuHpcg7X35N", "GonuUEmtx9iM4EdCWtg2eK" ]
interval_ms = 10
"#;

const READER_CONFIG: &str = r#"
type = "SimpleReader"
vids = [ "GonuUEmt4XLJuHpcg7X35N", "GonuUEmtx9iM4EdCWtg2eK" ]
recv_timeout_ms = 10
"#;

#[graft_test::test]
fn test_workloads_sanity() -> Result<(), Culprit<WorkloadErr>> {
    let (backend, clients) = start_graft_backend();

    let ticker = Ticker::new(50);

    let writer = {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().or_into_ctx()?;
        let runtime = Runtime::new(cid.clone(), clients.clone(), storage);
        runtime
            .start_sync_task(Duration::from_millis(10), 8, true)
            .or_into_ctx()?;
        let workload: WorkloadConfig = config::Config::builder()
            .add_source(File::from_str(WRITER_CONFIG, config::FileFormat::Toml))
            .build()?
            .try_deserialize()?;
        let r2 = runtime.clone();
        let workload = thread::Builder::new()
            .name("writer".into())
            .spawn(move || workload.run(cid, r2, rand::rng(), ticker))
            .unwrap();
        WorkloadRunner { runtime, workload }
    };

    let reader = {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().or_into_ctx()?;
        let runtime = Runtime::new(cid.clone(), clients, storage);
        runtime
            .start_sync_task(Duration::from_millis(10), 8, true)
            .or_into_ctx()?;
        let workload: WorkloadConfig = config::Config::builder()
            .add_source(File::from_str(READER_CONFIG, config::FileFormat::Toml))
            .build()?
            .try_deserialize()?;
        let r2 = runtime.clone();
        let workload = thread::Builder::new()
            .name("reader".into())
            .spawn(move || workload.run(cid, r2, rand::rng(), ticker))
            .unwrap();
        WorkloadRunner { runtime, workload }
    };

    // run both tasks to completion or timeout
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut finished = false;
    while !finished && Instant::now() < deadline {
        finished = writer.workload.is_finished() && reader.workload.is_finished();
        thread::sleep(Duration::from_millis(100));
    }

    // join and raise if either workload finished
    if writer.workload.is_finished() {
        writer.workload.join().unwrap().or_into_ctx()?
    }
    if reader.workload.is_finished() {
        reader.workload.join().unwrap().or_into_ctx()?
    }

    if !finished {
        panic!("workloads did not finish within timeout");
    }

    // shutdown everything
    writer
        .runtime
        .shutdown_sync_task(Duration::from_secs(5))
        .or_into_ctx()?;
    reader
        .runtime
        .shutdown_sync_task(Duration::from_secs(5))
        .or_into_ctx()?;
    backend.shutdown(Duration::from_secs(5)).or_into_ctx()?;

    Ok(())
}
