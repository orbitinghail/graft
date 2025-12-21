use graft::{
    core::{LogId, page::Page},
    pageidx,
    volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};
use graft_test::GraftTestRuntime;

#[test]
fn test_snapshot_correct_after_push() -> anyhow::Result<()> {
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
    let reader = runtime.volume_reader(vid1.clone())?;
    let page = reader.read_page(pageidx!(1))?;
    assert!(page == Page::test_filled(2), "page is correct");

    // shutdown the runtime
    runtime.shutdown().unwrap();

    Ok(())
}
