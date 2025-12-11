#!/bin/bash
ARGS=(
  --remote s3-compatible
)

if [[ -f /faults-disabled ]]; then
  ARGS+=(
    --disable-faults
  )
fi

/test_client ${ARGS[@]} bank-tx
