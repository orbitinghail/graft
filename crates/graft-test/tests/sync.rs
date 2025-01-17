use std::time::Duration;

use graft_client::runtime::{
    fetcher::NetFetcher,
    runtime::Runtime,
    storage::{
        volume_config::{SyncDirection, VolumeConfig},
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
    let runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    let sync = runtime.start_sync_task(clients.clone(), Duration::from_secs(1));

    // create a second client to sync to
    let storage2 = Storage::open_temporary().unwrap();
    let runtime2 = Runtime::new(NetFetcher::new(clients.clone()), storage2);
    let sync2 = runtime2.start_sync_task(clients, Duration::from_secs(1));

    // register the volume with both clients, pushing from client 1 to client 2
    let vid = VolumeId::random();
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Push))
        .unwrap();
    let handle2 = runtime2
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .unwrap();

    let subscription = handle2.subscribe_to_remote_changes();

    let page = Page::test_filled(0x42);

    // write and wait for replication multiple times
    for i in 0..5 {
        // write multiple times to the volume
        let mut txn = handle.write_txn().unwrap();
        txn.write(0.into(), page.clone());
        txn.commit().unwrap();

        let mut txn = handle.write_txn().unwrap();
        txn.write(0.into(), page.clone());
        txn.commit().unwrap();

        // wait for client 2 to receive the write
        subscription
            .recv_timeout(Duration::from_secs(5))
            .expect("subscription failed");

        let snapshot = handle2.snapshot().unwrap();
        log::info!("received remote snapshot: {snapshot:?}");
        assert_eq!(snapshot.lsn(), i);
        assert_eq!(snapshot.page_count(), 1);

        // TODO: implement downloading pages from the remote to make this assertion pass
        // let txn = runtime2.read_txn(&vid).unwrap();
        // let page2 = txn.read(0.into()).unwrap();
        // assert_eq!(page, page2, "page read from client 2 does not match");
    }

    // shutdown everything
    sync.shutdown_timeout(Duration::from_secs(5)).unwrap();
    sync2.shutdown_timeout(Duration::from_secs(5)).unwrap();
    supervisor.shutdown(Duration::from_secs(5)).await.unwrap();
}
