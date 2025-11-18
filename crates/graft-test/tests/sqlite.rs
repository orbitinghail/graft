use graft_core::VolumeId;
use graft_test::GraftTestRuntime;

#[graft_test::test]
fn test_sync_and_reset() {
    // create two nodes connected to the same remote
    let remote = VolumeId::random();
    let mut runtime1 = GraftTestRuntime::with_memory_remote();
    let sqlite1 = runtime1.open_sqlite("main", Some(remote.clone()));
    let mut runtime2 = runtime1.spawn_peer();
    let sqlite2 = runtime2.open_sqlite("main", Some(remote.clone()));

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

    // sync the changes from node 1 to node 2
    sqlite1.graft_pragma("push");
    sqlite2.graft_pragma("pull");

    // write to both nodes, creating a conflict
    sqlite1.execute("update t1 set counter = 1", []).unwrap();
    sqlite2.execute("update t2 set counter = 1", []).unwrap();

    // sync the changes from node 1
    sqlite1.graft_pragma("push");

    // attempt to push from node 2, which should detect the conflict
    let result = sqlite2.pragma_query(None, "graft_push", |_| Ok(()));
    assert!(result.is_err(), "push should fail due to divergence");

    // force reset node 2 to the latest remote
    sqlite2.graft_pragma("fetch");
    sqlite2.graft_pragma("clone");

    // verify both nodes are now pointing at the same remote LSN
    // and they have no outstanding local changes
    let status1 = runtime1
        .get_or_create_tag("main")
        .unwrap()
        .status()
        .unwrap();
    let status2 = runtime2
        .get_or_create_tag("main")
        .unwrap()
        .status()
        .unwrap();
    assert_eq!(status1.remote, status2.remote);
    assert_eq!(status1.remote_status.base, status2.remote_status.base);
    assert_eq!(status1.local_status.changes(), None);
    assert_eq!(status2.local_status.changes(), None);

    // verify that node2 sees that the t1 counter is 1 and the t2 counter is 0
    let t1_counter: u64 = sqlite2
        .query_row("select counter from t1", [], |row| row.get(0))
        .unwrap();
    let t2_counter: u64 = sqlite2
        .query_row("select counter from t2", [], |row| row.get(0))
        .unwrap();
    assert_eq!(t1_counter, 1);
    assert_eq!(t2_counter, 0);

    // shutdown everything
    runtime1.shutdown().unwrap();
    runtime2.shutdown().unwrap();
}

// #[graft_test::test]
// fn test_sqlite_query_only_fetches_needed_pages() {
//     let (backend, clients) = start_graft_backend();
//     let vid = VolumeId::random();

//     // create the first node (writer)
//     let writer_runtime = Runtime::new(
//         ClientId::random(),
//         clients.clone(),
//         Storage::open_temporary().unwrap(),
//     );
//     writer_runtime
//         .start_sync_task(Duration::from_secs(1), 8, true, "sync-1")
//         .unwrap();
//     register_static(
//         c"graft-writer".to_owned(),
//         GraftVfs::new(writer_runtime.clone()),
//         RegisterOpts { make_default: false },
//     )
//     .expect("failed to register vfs");

//     // open a sqlite connection and handle to the same volume on both nodes
//     let sqlite_writer = Connection::open_with_flags_and_vfs(
//         vid.pretty(),
//         OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
//         c"graft-writer",
//     )
//     .unwrap();
//     let writer_handle = writer_runtime
//         .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
//         .unwrap();

//     // create a table with 100 rows and enough data per row to pad out a few blocks
//     sqlite_writer
//         .execute_batch(
//             r#"
//             CREATE TABLE test_data (
//                 id INTEGER PRIMARY KEY,
//                 value TEXT NOT NULL
//             );
//             WITH RECURSIVE generate_rows(x) AS (
//                 SELECT 0
//                 UNION ALL
//                 SELECT x + 1 FROM generate_rows WHERE x + 1 <= 100
//             )
//             INSERT INTO test_data (id, value)
//             SELECT x, printf('%.*c', 100, 'x') FROM generate_rows;
//             "#,
//         )
//         .unwrap();
//     assert_eq!(writer_handle.snapshot().unwrap().unwrap().pages(), 5);

//     // create the second node (reader)
//     let reader_runtime = Runtime::new(
//         ClientId::random(),
//         clients,
//         Storage::open_temporary().unwrap(),
//     );
//     reader_runtime
//         .start_sync_task(Duration::from_millis(100), 8, true, "sync-2")
//         .unwrap();
//     register_static(
//         c"graft-reader".to_owned(),
//         GraftVfs::new(reader_runtime.clone()),
//         RegisterOpts { make_default: false },
//     )
//     .expect("failed to register vfs");
//     let sqlite_reader = Connection::open_with_flags_and_vfs(
//         vid.pretty(),
//         OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
//         c"graft-reader",
//     )
//     .unwrap();

//     // subscribe to remote changes and wait for the change metadata to be replicated
//     reader_runtime
//         .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
//         .unwrap()
//         .subscribe_to_remote_changes()
//         .recv_timeout(Duration::from_secs(5))
//         .unwrap();
//     // this doesn't do any page reads yet
//     assert_eq!(reader_runtime.clients().pagestore().pages_read(), 0);

//     // perform a single row lookup by ID
//     let value: i32 = sqlite_reader
//         .query_row("SELECT id FROM test_data WHERE id = 42", [], |row| {
//             row.get(0)
//         })
//         .unwrap();
//     assert_eq!(value, 42);
//     // only a small number of pages are read
//     assert_eq!(reader_runtime.clients().pagestore().pages_read(), 3);

//     // perform a query that reads all rows
//     let value: i32 = sqlite_reader
//         .query_row("SELECT sum(id) FROM test_data", [], |row| row.get(0))
//         .unwrap();
//     assert_eq!(value, 5050);
//     // this pulls in the rest of the pages
//     assert_eq!(
//         reader_runtime.clients().pagestore().pages_read(),
//         writer_handle.snapshot().unwrap().unwrap().pages()
//     );

//     // shutdown everything
//     writer_runtime
//         .shutdown_sync_task(Duration::from_secs(5))
//         .unwrap();
//     reader_runtime
//         .shutdown_sync_task(Duration::from_secs(5))
//         .unwrap();
//     backend.shutdown(Duration::from_secs(5)).unwrap();
// }
