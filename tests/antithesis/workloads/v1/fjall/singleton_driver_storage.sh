#!/busybox/sh
export RUST_BACKTRACE=1
/test_workload /workloads/fjall_storage.toml
