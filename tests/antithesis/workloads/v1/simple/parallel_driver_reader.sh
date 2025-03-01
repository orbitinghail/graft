#!/busybox/sh
export RUST_BACKTRACE=1
/test_workload /workloads/simple_reader.toml
