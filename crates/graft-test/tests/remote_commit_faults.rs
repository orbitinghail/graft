use std::panic::{AssertUnwindSafe, catch_unwind};

use graft::{
    core::{LogId, PageIdx, page::Page},
    volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};
use graft_test::{
    GraftTestRuntime,
    workload::{Env, bank_setup, bank_tx, bank_validate},
};

#[test]
fn test_skip_segment_cache() {
    graft_test::ensure_test_env();

    let runtime = GraftTestRuntime::with_memory_remote();

    // setup RemoteCommit faults
    let fault = precept::fault::get_fault_by_name("RemoteCommit: skipping segment cache").unwrap();
    fault.set_pending(1);
    let fault = precept::fault::get_fault_by_name("RemoteCommit: before commit").unwrap();
    fault.set_pending(1);

    // write to a volume and push
    let vid = runtime.volume_open(None, None, None).unwrap().vid;
    let mut writer = runtime.volume_writer(vid.clone()).unwrap();
    writer
        .write_page(PageIdx::FIRST, Page::test_filled(123))
        .unwrap();
    writer.commit().unwrap();

    // push should panic right before commit
    let err = catch_unwind(AssertUnwindSafe(|| runtime.volume_push(vid.clone())))
        .expect_err("expected volume_push to panic");
    tracing::info!("caught panic as expected: {:?}", err);

    // read the volume to make sure our page is still there
    let reader = runtime.volume_reader(vid.clone()).unwrap();
    let page = reader.read_page(PageIdx::FIRST).unwrap();
    assert_eq!(page, Page::test_filled(123));

    // a subsequent push should succeed
    runtime.volume_push(vid.clone()).unwrap();
    let remote = runtime.volume_get(&vid).unwrap().remote;

    // make sure we can pull the page to a peer
    let peer = runtime.spawn_peer();
    let vid2 = peer.volume_open(None, None, Some(remote)).unwrap().vid;
    peer.volume_pull(vid2.clone()).unwrap();

    let reader = peer.volume_reader(vid2.clone()).unwrap();
    let page = reader.read_page(PageIdx::FIRST).unwrap();
    assert_eq!(page, Page::test_filled(123));
}

#[test]
fn test_bank_balance_skip_seg_cache() {
    graft_test::ensure_test_env();
    let mut runtime = GraftTestRuntime::with_memory_remote();

    // setup the main tag
    let log = LogId::random();
    let volume = runtime.volume_open(None, None, Some(log.clone())).unwrap();
    runtime.tag_replace("main", volume.vid.clone()).unwrap();

    // create test env
    let sqlite = runtime.open_sqlite("main", None).into();
    let rng = rand::rng();
    let mut env = Env {
        cid: "client".to_string(),
        rng,
        runtime: runtime.clone(),
        vid: volume.vid,
        log,
        sqlite,
    };

    // Run bank_setup -> bank_tx -> bank_validate
    bank_setup(&mut env).unwrap();
    bank_tx(&mut env).unwrap();
    bank_validate(&mut env).unwrap();

    // cause segment cache to be skipped
    let fault = precept::fault::get_fault_by_name("RemoteCommit: skipping segment cache").unwrap();
    fault.set_pending(1);

    bank_tx(&mut env).unwrap();
    bank_validate(&mut env).unwrap();
}

#[test]
fn test_crash_after_commit_recovery() {
    graft_test::ensure_test_env();

    let mut runtime = GraftTestRuntime::with_memory_remote();

    // setup the main tag
    let log = LogId::random();
    let volume = runtime.volume_open(None, None, Some(log.clone())).unwrap();
    runtime.tag_replace("main", volume.vid.clone()).unwrap();

    // create test env
    let sqlite = runtime.open_sqlite("main", None).into();
    let rng = rand::rng();
    let mut env = Env {
        cid: "client".to_string(),
        rng,
        runtime: runtime.clone(),
        vid: volume.vid.clone(),
        log: log.clone(),
        sqlite,
    };

    // Run bank_setup (faults disabled)
    bank_setup(&mut env).unwrap();
    bank_validate(&mut env).unwrap();

    // Do a first round of bank_tx (faults disabled) to establish some baseline transactions
    bank_tx(&mut env).unwrap();
    bank_validate(&mut env).unwrap();

    // cause a crash right after we commit to the remote but before we handle the commit
    let after_commit_fault =
        precept::fault::get_fault_by_name("RemoteCommit: after commit").unwrap();
    after_commit_fault.set_pending(1);

    // This bank_tx should panic during its push due to the fault
    let err = catch_unwind(AssertUnwindSafe(|| bank_tx(&mut env)))
        .expect_err("expected bank_tx to panic during push");
    tracing::info!("caught panic as expected: {:?}", err);

    // The push failed. but the local state should be fine
    bank_validate(&mut env).unwrap();

    // Now retry the push which should recover from the aborted push and re-use the remote commit
    runtime.volume_push(env.vid.clone()).unwrap();

    // final validate should be ok
    bank_validate(&mut env).unwrap();
}
