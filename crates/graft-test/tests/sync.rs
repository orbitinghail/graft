use std::time::Duration;

use graft_client::runtime::{
    fetcher::NetFetcher,
    runtime::Runtime,
    storage::{
        volume_config::{SyncDirection, VolumeConfig},
        Storage,
    },
};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_test::{setup_logger, start_graft_backend};

#[test]
fn test_client_sync_sanity() {
    setup_logger();

    let (backend, clients) = start_graft_backend();

    let storage = Storage::open_temporary().unwrap();
    let runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    let sync = runtime.start_sync_task(clients.clone(), Duration::from_secs(1));

    // create a second client to sync to
    let storage2 = Storage::open_temporary().unwrap();
    let runtime2 = Runtime::new(NetFetcher::new(clients.clone()), storage2);
    let sync2 = runtime2.start_sync_task(clients, Duration::from_millis(10));

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
    let offset = PageOffset::new(0);

    // write and wait for replication multiple times
    for i in 0..5 {
        // write multiple times to the volume
        let mut writer = handle.writer().unwrap();
        writer.write(offset, page.clone());
        writer.commit().unwrap();

        let mut writer = handle.writer().unwrap();
        writer.write(offset, page.clone());
        writer.commit().unwrap();

        // wait for client 2 to receive the write
        subscription
            .recv_timeout(Duration::from_secs(5))
            .expect("subscription failed");

        let snapshot = handle2.snapshot().unwrap();
        log::info!("received remote snapshot: {snapshot:?}");
        assert_eq!(snapshot.local().lsn(), i + 1);
        assert_eq!(snapshot.local().page_count(), 1);

        let reader = handle2.reader_at(snapshot);
        let received = reader.read(offset).unwrap();
        assert_eq!(received, page, "received page does not match written page");
    }

    // shutdown everything
    sync.shutdown_timeout(Duration::from_secs(5)).unwrap();
    sync2.shutdown_timeout(Duration::from_secs(5)).unwrap();
    backend.shutdown(Duration::from_secs(5)).unwrap();
}
