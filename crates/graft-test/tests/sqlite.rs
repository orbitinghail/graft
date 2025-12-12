use graft::core::{LogId, PageCount};
use graft_test::GraftTestRuntime;
use rusqlite::Connection;

#[test]
fn test_sync_and_reset() {
    graft_test::setup_precept_and_disable_faults();

    // create two nodes connected to the same remote
    let remote = LogId::random();
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
    sqlite1.graft_pragma("push").unwrap();
    sqlite2.graft_pragma("pull").unwrap();

    // write to both nodes, creating a conflict
    sqlite1.execute("update t1 set counter = 1", []).unwrap();
    sqlite2.execute("update t2 set counter = 1", []).unwrap();

    // sync the changes from node 1
    sqlite1.graft_pragma("push").unwrap();

    // attempt to push from node 2, which should detect the conflict
    let result = sqlite2.pragma_query(None, "graft_push", |_| Ok(()));
    assert!(result.is_err(), "push should fail due to divergence");

    // force reset node 2 to the latest remote
    sqlite2.graft_pragma("fetch").unwrap();
    sqlite2.graft_pragma("clone").unwrap();

    // verify both nodes are now pointing at the same remote LSN
    // and they have no outstanding local changes
    let graft1 = runtime1.tag_get("main").unwrap().unwrap();
    let status1 = runtime1.volume_status(&graft1).unwrap();
    let graft2 = runtime2.tag_get("main").unwrap().unwrap();
    let status2 = runtime2.volume_status(&graft2).unwrap();
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

#[test]
fn test_import_export() {
    graft_test::setup_precept_and_disable_faults();

    let mut runtime = GraftTestRuntime::with_memory_remote();
    let sqlite = runtime.open_sqlite("main", None);

    // Create a table with some data
    sqlite
        .execute_batch(
            r#"
            CREATE TABLE test_data (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER NOT NULL
            );
            INSERT INTO test_data (id, name, value) VALUES
                (1, 'Alice', 100),
                (2, 'Bob', 200),
                (3, 'Charlie', 300);
            "#,
        )
        .unwrap();

    // Verify the data
    let count: i64 = sqlite
        .query_row("SELECT COUNT(*) FROM test_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);

    // Create a temporary directory for export
    let temp_dir = tempfile::tempdir().unwrap();
    let export_path = temp_dir.path().join("exported.db");
    let export_path_str = export_path.to_str().unwrap();

    // Export the database
    sqlite.graft_pragma_arg("export", export_path_str).unwrap();

    // Verify the exported file exists
    assert!(export_path.exists());

    // Open the exported SQLite file directly to verify it's valid
    let exported_conn = Connection::open(&export_path).unwrap();

    // Verify we can query the exported database
    let count: i64 = exported_conn
        .query_row("SELECT COUNT(*) FROM test_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);

    // Verify the data is correct
    let name: String = exported_conn
        .query_row("SELECT name FROM test_data WHERE id = 2", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(name, "Bob");

    drop(exported_conn);

    // Create a new volume and import the exported database
    let sqlite2 = runtime.open_sqlite("imported", None);
    sqlite2.graft_pragma_arg("import", export_path_str).unwrap();

    // Verify the imported data
    let count: i64 = sqlite2
        .query_row("SELECT COUNT(*) FROM test_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 3);

    let name: String = sqlite2
        .query_row("SELECT name FROM test_data WHERE id = 2", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(name, "Bob");

    // Verify we can query all the data
    let sum: i64 = sqlite2
        .query_row("SELECT SUM(value) FROM test_data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(sum, 600);

    // Cleanup
    runtime.shutdown().unwrap();
}

#[test]
fn test_sqlite_query_only_fetches_needed_pages() {
    graft_test::setup_precept_and_disable_faults();

    let log = LogId::random();

    // create a writer
    let mut writer = GraftTestRuntime::with_memory_remote();
    let writer_sql = writer.open_sqlite("main", Some(log.clone()));
    let writer_vid = writer.tag_get("main").unwrap().unwrap();

    // create a reader
    let mut reader = writer.spawn_peer();
    let reader_sql = reader.open_sqlite("main", Some(log.clone()));
    let reader_vid = reader.tag_get("main").unwrap().unwrap();

    // create a table and then insert 10 rows, which each consume just over a page. then push each segment to the remote
    // note: we use separate txns for each row to ensure they end up in separate segments
    writer_sql.execute("CREATE TABLE t (d)", []).unwrap();
    for _ in 0..10 {
        writer_sql
            .execute("insert into t values (printf('%0*d', 4096, 0))", [])
            .unwrap();
        writer_sql.graft_pragma("push").unwrap();
    }

    let snapshot = writer.volume_snapshot(&writer_vid).unwrap();
    assert_eq!(
        writer.snapshot_pages(&snapshot).unwrap(),
        PageCount::new(14)
    );

    // pull changes into the reader
    reader_sql.graft_pragma("pull").unwrap();

    // all pages missing
    let snapshot = reader.volume_snapshot(&reader_vid).unwrap();
    assert_eq!(
        reader
            .snapshot_missing_pages(&snapshot)
            .unwrap()
            .cardinality()
            .to_usize(),
        14
    );

    // perform a single row lookup by ID
    let value: i32 = reader_sql
        .query_row("SELECT length(d) FROM t LIMIT 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 4096);

    // only 5 pages retrieved
    assert_eq!(
        reader
            .snapshot_missing_pages(&snapshot)
            .unwrap()
            .cardinality()
            .to_usize(),
        9
    );

    // perform a query that reads all rows
    let value: i32 = reader_sql
        .query_row("SELECT sum(length(d)) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 40960);

    // no pages missing
    assert_eq!(
        reader
            .snapshot_missing_pages(&snapshot)
            .unwrap()
            .cardinality()
            .to_usize(),
        0
    );
}
