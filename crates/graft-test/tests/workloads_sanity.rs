use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use culprit::{Culprit, ResultExt};
use graft_client::runtime::{fetcher::NetFetcher, runtime::Runtime, storage::Storage};
use graft_core::{gid::ClientId, VolumeId};
use graft_test::{
    start_graft_backend,
    workload::{Workload, WorkloadErr},
    Ticker,
};

struct WorkloadRunner {
    runtime: Runtime,
    workload: JoinHandle<Result<(), Culprit<WorkloadErr>>>,
}

#[graft_test::test]
fn test_workloads_sanity() -> Result<(), Culprit<WorkloadErr>> {
    let (backend, clients) = start_graft_backend();

    let vid = VolumeId::random();
    let ticker = Ticker::new(10);

    let writer = {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().or_into_ctx()?;
        let runtime = Runtime::new(cid.clone(), NetFetcher::new(clients.clone()), storage);
        runtime
            .start_sync_task(clients.clone(), Duration::from_millis(10), 8, true)
            .or_into_ctx()?;
        let workload = Workload::Writer { vid: vid.clone(), interval_ms: 10 };
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
        let runtime = Runtime::new(cid.clone(), NetFetcher::new(clients.clone()), storage);
        runtime
            .start_sync_task(clients.clone(), Duration::from_millis(10), 8, true)
            .or_into_ctx()?;
        let workload = Workload::Reader { vid: vid.clone(), interval_ms: 10 };
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
