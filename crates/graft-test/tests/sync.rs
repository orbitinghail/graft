use std::time::Duration;

use graft_client::{
    oracle::NoopOracle,
    runtime::{
        runtime::Runtime,
        storage::{
            volume_state::{SyncDirection, VolumeConfig},
            Storage,
        },
        volume_reader::VolumeRead,
        volume_writer::VolumeWrite,
    },
};
use graft_core::{gid::ClientId, page::Page, PageIdx, VolumeId};
use graft_test::start_graft_backend;

#[graft_test::test]
fn test_client_sync_sanity() {
    let (backend, clients) = start_graft_backend();

    let storage = Storage::open_temporary().unwrap();
    let runtime = Runtime::new(ClientId::random(), clients.clone(), storage);
    runtime
        .start_sync_task(Duration::from_secs(1), 8, true)
        .unwrap();

    // create a second client to sync to
    let storage2 = Storage::open_temporary().unwrap();
    let runtime2 = Runtime::new(ClientId::random(), clients, storage2);
    runtime2
        .start_sync_task(Duration::from_millis(100), 8, true)
        .unwrap();

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
    let pageidx = PageIdx::FIRST;

    // write and wait for replication multiple times
    for i in 1..10 {
        // write multiple times to the volume
        let mut writer = handle.writer().unwrap();
        writer.write(pageidx, page.clone());
        writer.commit().unwrap();

        let mut writer = handle.writer().unwrap();
        writer.write(pageidx, page.clone());
        writer.commit().unwrap();

        // wait for client 2 to receive the write
        // this timeout has to be large enough to allow both sync tasks to run
        // as well as the segment flush interval in the backend
        subscription
            .recv_timeout(Duration::from_secs(5))
            .expect("subscription failed");

        let snapshot = handle2.snapshot().unwrap().unwrap();
        tracing::info!("received remote snapshot: {snapshot:?}");
        assert_eq!(snapshot.local(), i);
        assert_eq!(snapshot.pages(), 1);

        let reader = handle2.reader_at(Some(snapshot));
        let received = reader.read(&mut NoopOracle, pageidx).unwrap();
        assert_eq!(received, page, "received page does not match written page");
    }

    // shutdown everything
    runtime.shutdown_sync_task(Duration::from_secs(5)).unwrap();
    runtime2.shutdown_sync_task(Duration::from_secs(5)).unwrap();
    backend.shutdown(Duration::from_secs(5)).unwrap();
}
