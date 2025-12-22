use std::panic::{AssertUnwindSafe, catch_unwind};

use anyhow::Ok;
use graft::{
    core::{LogId, page::Page},
    pageidx,
    volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};
use graft_test::GraftTestRuntime;

#[test]
fn test_snapshot_correct_after_pull() -> anyhow::Result<()> {
    graft_test::ensure_test_env();

    let remote = LogId::random();
    let runtime = GraftTestRuntime::with_memory_remote();

    // open the same remote in two different volumes
    let vid1 = runtime.volume_open(None, None, Some(remote.clone()))?.vid;
    let vid2 = runtime.volume_open(None, None, Some(remote.clone()))?.vid;

    // write a page to vid1
    let mut writer = runtime.volume_writer(vid1.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(1))?;
    writer.commit()?;

    // push it to the remote
    runtime.volume_push(vid1.clone())?;

    // pull and update the page in vid2
    runtime.volume_pull(vid2.clone())?;
    let mut writer = runtime.volume_writer(vid2.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(2))?;
    writer.commit()?;

    // push it to the remote
    runtime.volume_push(vid2.clone())?;

    // pull and verify that we see the page in vid1
    runtime.volume_pull(vid1.clone())?;
    tracing::info!(snapshot=?runtime.volume_snapshot(&vid1)?);
    let reader = runtime.volume_reader(vid1.clone())?;
    let page = reader.read_page(pageidx!(1))?;
    assert!(page == Page::test_filled(2), "page is correct");

    // write to a different page, and then validate the resulting snapshot
    let mut writer = runtime.volume_writer(vid1.clone())?;
    writer.write_page(pageidx!(2), Page::test_filled(2))?;
    let reader = writer.commit()?;
    tracing::info!(snapshot=?reader.snapshot());
    let page = reader.read_page(pageidx!(1))?;
    assert!(page == Page::test_filled(2), "page is correct");
    let page = reader.read_page(pageidx!(2))?;
    assert!(page == Page::test_filled(2), "page is correct");

    // shutdown the runtime
    runtime.shutdown().unwrap();

    Ok(())
}

#[test]
fn test_latest_snapshot_correct_after_pull() -> anyhow::Result<()> {
    graft_test::ensure_test_env();

    let remote = LogId::random();
    let runtime = GraftTestRuntime::with_memory_remote();

    // open the same remote in two different volumes
    let vid1 = runtime.volume_open(None, None, Some(remote.clone()))?.vid;
    let vid2 = runtime.volume_open(None, None, Some(remote.clone()))?.vid;

    // write a page to vid1
    let mut writer = runtime.volume_writer(vid1.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(1))?;
    writer.commit()?;

    let fault = precept::fault::get_fault_by_name("RemoteCommit: after commit").unwrap();
    fault.set_pending(1);

    // push it to the remote but crash before we update our state
    let err = catch_unwind(AssertUnwindSafe(|| runtime.volume_push(vid1.clone())))
        .expect_err("expected volume_push to panic");
    tracing::info!("caught panic as expected: {:?}", err);
    tracing::info!(snapshot=?runtime.volume_snapshot(&vid1)?);

    // pull and update the page in vid2
    runtime.volume_pull(vid2.clone())?;
    let mut writer = runtime.volume_writer(vid2.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(2))?;
    tracing::info!(snapshot=?writer.snapshot());
    writer.commit()?;
    runtime.volume_push(vid2.clone())?;

    // write a commit to vid1, but dont commit yet
    let mut writer = runtime.volume_writer(vid1.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(3))?;

    // pull the changes from vid2 which also triggers recovery
    runtime.volume_pull(vid1.clone())?;
    tracing::info!(snapshot=?runtime.volume_snapshot(&vid1)?);

    // this should fail
    tracing::info!(snapshot=?writer.snapshot());
    writer
        .commit()
        .expect_err("expected commit to fail with a concurrency error");

    Ok(())
}
