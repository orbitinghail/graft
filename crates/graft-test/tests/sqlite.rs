use std::{
    thread::sleep,
    time::{Duration, Instant},
};

use graft_client::runtime::{
    runtime::Runtime,
    storage::{
        Storage,
        volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
    },
};
use graft_core::{ClientId, VolumeId};
use graft_sqlite::vfs::GraftVfs;
use graft_test::start_graft_backend;
use rusqlite::{Connection, OpenFlags};
use sqlite_plugin::vfs::{RegisterOpts, register_static};

#[graft_test::test]
fn test_sync_and_reset() {
    let (backend, clients) = start_graft_backend();

    // create the first node
    let storage1 = Storage::open_temporary().unwrap();
    let runtime1 = Runtime::new(ClientId::random(), clients.clone(), storage1);
    runtime1
        .start_sync_task(Duration::from_secs(1), 8, true, "sync-1")
        .unwrap();
    register_static(
        c"graft-1".to_owned(),
        GraftVfs::new(runtime1.clone()),
        RegisterOpts { make_default: false },
    )
    .expect("failed to register vfs");

    // create the second node
    let storage2 = Storage::open_temporary().unwrap();
    let runtime2 = Runtime::new(ClientId::random(), clients, storage2);
    runtime2
        .start_sync_task(Duration::from_millis(100), 8, true, "sync-2")
        .unwrap();
    register_static(
        c"graft-2".to_owned(),
        GraftVfs::new(runtime2.clone()),
        RegisterOpts { make_default: false },
    )
    .expect("failed to register vfs");

    let vid = VolumeId::random();

    // open a sqlite connection and handle to the same volume on both nodes
    let sqlite1 = Connection::open_with_flags_and_vfs(
        vid.pretty(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        c"graft-1",
    )
    .unwrap();
    let handle1 = runtime1
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .unwrap();

    let sqlite2 = Connection::open_with_flags_and_vfs(
        vid.pretty(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        c"graft-2",
    )
    .unwrap();
    let handle2 = runtime2
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .unwrap();

    // subscribe to remote changes to sequence the test correctly
    let subscription2 = handle2.subscribe_to_remote_changes();

    // create two counter tables
    sqlite1
        .execute_batch(
            r#"
            CREATE TABLE t1 (counter INTEGER);
            INSERT INTO t1 VALUES (0);
            CREATE TABLE t2 (counter INTEGER);
            INSERT INTO t2 VALUES (0);
            "#,
        )
        .unwrap();
    // wait for the changes to be replicated to node 2
    subscription2.recv_timeout(Duration::from_secs(5)).unwrap();

    // disable sync
    runtime1.set_autosync(false);
    runtime2.set_autosync(false);

    // write to both nodes
    sqlite1.execute("update t1 set counter = 1", []).unwrap();
    sqlite2.execute("update t2 set counter = 1", []).unwrap();

    // enable sync on node1 and wait for it to push the changes
    let snapshot1 = handle1.snapshot().unwrap();
    runtime1.set_autosync(true);
    let snapshot1 = wait_for_change(Duration::from_secs(5), snapshot1, || {
        handle1.snapshot().unwrap()
    });

    // enable sync on node2 and wait for it to detect the conflict
    runtime2.clients().pagestore().reset_pages_read();
    let status = handle2.status().unwrap();
    runtime2.set_autosync(true);
    wait_for_change(Duration::from_secs(5), status, || handle2.status().unwrap());

    // reset to remote on node2
    handle2.reset_to_remote().unwrap();
    assert_eq!(handle2.status().unwrap(), VolumeStatus::Ok);
    let snapshot2 = handle2.snapshot().unwrap();

    assert_eq!(
        snapshot1.as_ref().and_then(|s| s.remote()),
        snapshot2.as_ref().and_then(|s| s.remote())
    );

    // verify that node2 sees that the t1 counter is 1 and the t2 counter is 0
    let t1_counter: u64 = sqlite2
        .query_row("select counter from t1", [], |row| row.get(0))
        .unwrap();
    let t2_counter: u64 = sqlite2
        .query_row("select counter from t2", [], |row| row.get(0))
        .unwrap();
    assert_eq!(t1_counter, 1);
    assert_eq!(t2_counter, 0);

    // We resolved the conflict after only fetching a single page, out of a total of 3
    assert_eq!(snapshot1.unwrap().pages(), 3);
    assert_eq!(runtime2.clients().pagestore().pages_read(), 1);

    // shutdown everything
    runtime1.shutdown_sync_task(Duration::from_secs(5)).unwrap();
    runtime2.shutdown_sync_task(Duration::from_secs(5)).unwrap();
    backend.shutdown(Duration::from_secs(5)).unwrap();
}

#[graft_test::test]
fn test_sqlite_query_only_fetches_needed_pages() {
    let (backend, clients) = start_graft_backend();
    let vid = VolumeId::random();

    // create the first node (writer)
    let writer_runtime = Runtime::new(
        ClientId::random(),
        clients.clone(),
        Storage::open_temporary().unwrap(),
    );
    writer_runtime
        .start_sync_task(Duration::from_secs(1), 8, true, "sync-1")
        .unwrap();
    register_static(
        c"graft-writer".to_owned(),
        GraftVfs::new(writer_runtime.clone()),
        RegisterOpts { make_default: false },
    )
    .expect("failed to register vfs");

    // open a sqlite connection and handle to the same volume on both nodes
    let sqlite_writer = Connection::open_with_flags_and_vfs(
        vid.pretty(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        c"graft-writer",
    )
    .unwrap();
    let writer_handle = writer_runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .unwrap();

    // create a table with 100 rows and enough data per row to pad out a few blocks
    sqlite_writer
        .execute_batch(
            r#"
            CREATE TABLE test_data (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL
            );
            WITH RECURSIVE generate_rows(x) AS (
                SELECT 0
                UNION ALL
                SELECT x + 1 FROM generate_rows WHERE x + 1 <= 100
            )
            INSERT INTO test_data (id, value)
            SELECT x, printf('%.*c', 100, 'x') FROM generate_rows;
            "#,
        )
        .unwrap();
    assert_eq!(writer_handle.snapshot().unwrap().unwrap().pages(), 5);

    // create the second node (reader)
    let reader_runtime = Runtime::new(
        ClientId::random(),
        clients,
        Storage::open_temporary().unwrap(),
    );
    reader_runtime
        .start_sync_task(Duration::from_millis(100), 8, true, "sync-2")
        .unwrap();
    register_static(
        c"graft-reader".to_owned(),
        GraftVfs::new(reader_runtime.clone()),
        RegisterOpts { make_default: false },
    )
    .expect("failed to register vfs");
    let sqlite_reader = Connection::open_with_flags_and_vfs(
        vid.pretty(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        c"graft-reader",
    )
    .unwrap();

    // subscribe to remote changes and wait for the change metadata to be replicated
    reader_runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .unwrap()
        .subscribe_to_remote_changes()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    // this doesn't do any page reads yet
    assert_eq!(reader_runtime.clients().pagestore().pages_read(), 0);

    // perform a single row lookup by ID
    let value: i32 = sqlite_reader
        .query_row("SELECT id FROM test_data WHERE id = 42", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(value, 42);
    // only a small number of pages are read
    assert_eq!(reader_runtime.clients().pagestore().pages_read(), 3);

    // perform a query that reads all rows
    let value: i32 = sqlite_reader
        .query_row("SELECT sum(id) FROM test_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 5050);
    // this pulls in the rest of the pages
    assert_eq!(
        reader_runtime.clients().pagestore().pages_read(),
        writer_handle.snapshot().unwrap().unwrap().pages()
    );

    // shutdown everything
    writer_runtime
        .shutdown_sync_task(Duration::from_secs(5))
        .unwrap();
    reader_runtime
        .shutdown_sync_task(Duration::from_secs(5))
        .unwrap();
    backend.shutdown(Duration::from_secs(5)).unwrap();
}

fn wait_for_change<T: Eq>(timeout: Duration, baseline: T, mut cb: impl FnMut() -> T) -> T {
    let start = Instant::now();
    loop {
        let next = cb();
        if next != baseline {
            return next;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for change");
        }
        sleep(Duration::from_millis(100));
    }
}
