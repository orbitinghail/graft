#!/bin/bash
set -euo pipefail

# usage fly.sh all|metastore|pagestore <command>
APP="${1}"
shift

if [ -z "${APP}" ]; then
    echo "usage: $0 <all|metastore|pagestore> <command>"
    exit 1
fi

if [ "${APP}" == "all" ]; then
    fly --config deploy/metastore/fly.toml "$@"
    fly --config deploy/pagestore/fly.toml "$@"
else
    exec fly --config deploy/${APP}/fly.toml "$@"
fi
