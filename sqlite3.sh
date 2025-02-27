#!/usr/bin/env bash
set -uo pipefail

GIT_ROOT="$(git rev-parse --show-toplevel)"
EXAMPLES_LIB_PATH="${GIT_ROOT}/target/debug/examples"
LIB_PATH="${GIT_ROOT}/target/debug"

# detect platform
UNAME_KERNEL="$(uname -s)"
case "${UNAME_KERNEL}" in
    Linux*)     PLATFORM=linux;;
    Darwin*)    PLATFORM=osx;;
    *)
        echo "Unsupported platform: ${UNAME_KERNEL}"
        exit 1
        ;;
esac

SQLITE_YEAR=2023
SQLITE_VERSION=3440000
SQLITE_URL="https://www.sqlite.org/${SQLITE_YEAR}/sqlite-tools-${PLATFORM}-x64-${SQLITE_VERSION}.zip"
SQLITE_DIR="${GIT_ROOT}/target/sqlite-${SQLITE_VERSION}"
SQLITE_BIN="${SQLITE_DIR}/sqlite3"

# install sqlite3 into ${GIT_ROOT}/target/sqlite3/
# if it's not already installed
if [ ! -f "${SQLITE_BIN}" ]; then
    echo "Installing sqlite3 from ${SQLITE_URL} into ${SQLITE_DIR}"
    mkdir -p "${SQLITE_DIR}"
    SQLITE_ZIP="$(mktemp -d)/sqlite3.zip"
    curl -L -o "${SQLITE_ZIP}" "${SQLITE_URL}"
    unzip -d "${SQLITE_DIR}" "${SQLITE_ZIP}"
    rm "${SQLITE_ZIP}"
fi

# make sure sqlite can find the vfs
export LD_LIBRARY_PATH=${LIB_PATH}:${EXAMPLES_LIB_PATH}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export DYLD_LIBRARY_PATH=${LIB_PATH}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}

# exec sqlite3 with all arguments
exec "${SQLITE_BIN}" "$@"
