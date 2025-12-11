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
    graft_test::setup_precept_and_disable_faults();
    graft::fault::set_crash_mode(true);

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
    graft_test::setup_precept_and_disable_faults();
    let mut runtime = GraftTestRuntime::with_memory_remote();

    // setup the main tag
    let log = LogId::random();
    let volume = runtime.volume_open(None, None, Some(log.clone())).unwrap();
    runtime.tag_replace("main", volume.vid.clone()).unwrap();

    // create test env
    let sqlite = runtime.open_sqlite("main", None).into();
    let rng = rand::rng();
    let mut env = Env {
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
