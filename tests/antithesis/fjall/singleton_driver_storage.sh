#!/usr/bin/bash
export RUST_BACKTRACE=1
exec /app/target/debug/rust-storage-bench run \
  --compression none \
  --backend fjall-nightly \
  --data-dir .data \
  --cache-size 536870912 \
  --seconds 900 \
  --out random_fjall-nightly.jsonl \
  read-write --write-random --value-size 0 --item-count 1000
