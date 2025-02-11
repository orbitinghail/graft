#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_PATH="${SCRIPT_DIR}/target/debug"

VID="GontkHa6QVLMYnkyk16wUP"

GDB=${GDB:-0}
LOG=${LOG:-error}

# make sure sqlite can find the vfs
export LD_LIBRARY_PATH=${LIB_PATH}:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=${LIB_PATH}:$DYLD_LIBRARY_PATH

cargo build

# parse flags
while [[ $# -gt 0 ]]; do
    case $1 in
        -v*)
            VID="${1:2}"
            shift
            ;;
        --trace)
            LOG="trace"
            shift
            ;;
        --gdb)
            GDB=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-vVID] [--trace] [--gdb]"
            exit 1
            ;;
    esac
done

ARGS=(
    -header
    -table
    -cmd '.log stderr'
    -cmd '.load libgraft'
    -cmd ".open 'file:${VID}?vfs=graft'"
    -cmd ".pragma graft_log=${LOG}"
)

if [ "${GDB}" == 1 ]; then
    GDB_ARGS=(
        --eval-command="set breakpoint pending on"
        --eval-command="break rust_panic"
        -ex run
        --args sqlite3
        "${ARGS[@]}"
    )
    exec rust-gdb "${GDB_ARGS[@]}"
else
    exec sqlite3 "${ARGS[@]}"
fi
