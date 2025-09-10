use std::time::Duration;

pub mod test_object_store;

pub async fn assert_would_timeout<F, O>(fut: F)
where
    F: Future<Output = O>,
{
    // we wait for the future to complete with an effectively unbounded timeout
    // this function expects that the tokio runtime is paused which will ensure
    // that tokio auto-advances once it has no work to do, hence exiting this sleep
    tokio::select! {
        _ = tokio::time::sleep(Duration::MAX) => {}
        _ = fut => panic!("expected timeout"),
    }
}
