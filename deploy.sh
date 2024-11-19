#!/bin/bash
set -euo pipefail

# change directory to the git root
cd "$(git rev-parse --show-toplevel)"

fly deploy --config deploy/metastore/fly.toml
fly deploy --config deploy/pagestore/fly.toml
