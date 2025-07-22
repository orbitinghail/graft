#!/busybox/sh
export RUST_BACKTRACE=1

export ANTITHESIS_OUTPUT_DIR=${ANTITHESIS_OUTPUT_DIR:-/antithesis_output}
mkdir -p ${ANTITHESIS_OUTPUT_DIR}

echo '{"antithesis_setup": { "status": "complete", "details": null }}' >> ${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl

echo "Wrote setup complete event to ${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl"

exec /rust-storage-bench run \
  --compression none \
  --backend fjall-nightly \
  --data-dir .data \
  --cache-size 536870912 \
  --seconds 900 \
  --out fjall-nightly-output.jsonl \
  read-write --write-random --value-size 0 --item-count 1000
