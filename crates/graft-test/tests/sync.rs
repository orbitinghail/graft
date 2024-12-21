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
use tokio::time::timeout;

#[tokio::test(start_paused = true)]
async fn test_client_sync_sanity() {
    setup_logger();

    let (mut supervisor, clients) = run_graft_services().await;

    let storage = Storage::open_temporary().unwrap();
    let runtime = RuntimeHandle::new(storage);
    let sync = runtime.spawn_sync_task(clients.clone(), Duration::from_secs(1));

    // create a second client to sync to
    let storage2 = Storage::open_temporary().unwrap();
    let mut commits_rx = storage2.subscribe_to_remote_commits();
    let runtime2 = RuntimeHandle::new(storage2);
    let sync2 = runtime2.spawn_sync_task(clients, Duration::from_secs(1));

    // register the volume with both clients, pushing from client 1 to client 2
    let vid = VolumeId::random();
    runtime
        .add_volume(&vid, VolumeConfig::new(SyncDirection::Push))
        .unwrap();
    runtime2
        .add_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .unwrap();

    // write a page to the volume in client 1
    let mut txn = runtime.write_txn(&vid).unwrap();
    txn.write(0.into(), Page::test_filled(0x42));
    txn.commit().unwrap();

    // wait for client 2 to receive the write
    timeout(Duration::from_secs(5), commits_rx.recv())
        .await
        .unwrap()
        .unwrap();

    // shutdown everything
    sync.shutdown(Duration::from_secs(1)).await.unwrap();
    sync2.shutdown(Duration::from_secs(1)).await.unwrap();
    supervisor.shutdown(Duration::from_secs(1)).await.unwrap();
}
