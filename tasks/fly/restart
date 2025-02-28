#!/bin/bash
set -euo pipefail

IDS=$(fly --config deploy/metastore/fly.toml machine list -q)
fly --config deploy/metastore/fly.toml machine stop -w 30s $IDS
fly --config deploy/metastore/fly.toml machine start $IDS

IDS=$(fly --config deploy/pagestore/fly.toml machine list -q)
fly --config deploy/pagestore/fly.toml machine stop -w 30s $IDS
fly --config deploy/pagestore/fly.toml machine start $IDS
