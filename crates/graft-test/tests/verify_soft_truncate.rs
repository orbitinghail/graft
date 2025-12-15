use graft::{
    core::{LogId, PageCount, page::Page},
    pageidx,
    volume_reader::VolumeRead,
    volume_writer::VolumeWrite,
};
use graft_test::GraftTestRuntime;

/// This test verifies the behavior of soft truncation in Graft.
/// Notably, truncation doesn't actually erase pages. Thus if you resize smaller
/// and then larger, pages from before the first resize may become visible.
#[test]
fn test_soft_truncate() -> anyhow::Result<()> {
    graft_test::ensure_test_env();

    // create two nodes connected to the same remote
    let remote = LogId::random();
    let runtime1 = GraftTestRuntime::with_memory_remote();
    let runtime2 = runtime1.spawn_peer();

    let vid1 = runtime1.volume_open(None, None, Some(remote.clone()))?.vid;
    let vid2 = runtime2.volume_open(None, None, Some(remote.clone()))?.vid;

    let mut writer = runtime1.volume_writer(vid1.clone())?;
    writer.write_page(pageidx!(1), Page::test_filled(1))?;
    writer.write_page(pageidx!(2), Page::test_filled(2))?;
    writer.write_page(pageidx!(3), Page::test_filled(3))?;
    writer.commit()?;

    runtime1.volume_push(vid1.clone())?;
    let mut writer = runtime1.volume_writer(vid1.clone())?;
    writer.soft_truncate(PageCount::new(0))?;
    writer.commit()?;
    let mut writer = runtime1.volume_writer(vid1.clone())?;
    writer.soft_truncate(PageCount::new(3))?;
    writer.commit()?;
    runtime1.volume_push(vid1.clone())?;

    // Verify that we can still read page 3 on the original volume
    let reader = runtime1.volume_reader(vid1.clone())?;
    let page = reader.read_page(pageidx!(3))?;
    assert!(page == Page::test_filled(3));

    // Verify that soft truncation is being respected on the replica. We should
    // see page 3 even though we just truncated the volume through PageCount == 0
    runtime2.volume_pull(vid2.clone())?;
    let reader = runtime2.volume_reader(vid2.clone())?;
    let page = reader.read_page(pageidx!(3))?;
    assert!(page == Page::test_filled(3));

    // shutdown everything
    runtime1.shutdown().unwrap();
    runtime2.shutdown().unwrap();

    Ok(())
}
