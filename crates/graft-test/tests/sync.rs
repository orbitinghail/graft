use std::time::Duration;

use graft_client::runtime::{
    handle::RuntimeHandle,
    storage::{
        volume::{SyncDirection, VolumeConfig},
        Storage,
    },
};
use graft_core::{page::Page, VolumeId};
use graft_test::{run_graft_services, setup_logger};

#[tokio::test(start_paused = true)]
async fn test_client_sync_sanity() {
    setup_logger();

    let (mut supervisor, clients) = run_graft_services().await;

    let storage = Storage::open_temporary().unwrap();
    let runtime = RuntimeHandle::new(storage);
    let sync = runtime.spawn_sync_task(clients, Duration::from_secs(1));

    // create a local volume
    let vid = VolumeId::random();
    runtime
        .add_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .unwrap();

    // write a page to the volume
    let mut txn = runtime.write_txn(&vid).unwrap();
    txn.write(0.into(), Page::test_filled(0x42));
    txn.commit().unwrap();

    // this sleep will ensure that the sync task has a chance to run.
    // tokio time makes this deterministic.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // shutdown everything
    sync.shutdown(Duration::from_secs(1)).await;
    supervisor.shutdown(Duration::from_secs(1)).await;
}
