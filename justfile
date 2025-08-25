set unstable

GIT_SHA := `git describe --abbrev=40 --always --dirty --match=nevermatch 2>/dev/null`
GIT_SUMMARY := `git show --no-patch 2>/dev/null`

# set this argument via: just instrumented=1 ...
instrumented := ""
BUILD_ARGS := instrumented && "--build-arg INSTRUMENTED=1" || ""

ANTITHESIS_REGISTRY := "us-central1-docker.pkg.dev/molten-verve-216720/orbitinghail-repository"
DOCKER_PLATFORM := "linux/amd64"

METASTORE_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "metastore:" + GIT_SHA
PAGESTORE_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "pagestore:" + GIT_SHA
CONFIG_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "config:" + GIT_SHA
TEST_WORKLOAD_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "test_workload:" + GIT_SHA
MINIO_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "minio:" + GIT_SHA

default:
    @just --list

[positional-arguments]
[no-exit-message]
run *args:
    #!/usr/bin/env bash
    if [ "$#" -eq 0 ]; then
        echo "Usage: run <task> [arguments...]" >&2
        exit 1
    fi

    # Store all command-line arguments in an array.
    args=("$@")
    num_args=$#

    found=""
    found_index=0

    # Try the longest possible prefix down to a single argument.
    for (( i = num_args; i > 0; i-- )); do
        candidate="./tasks"
        for (( j = 0; j < i; j++ )); do
             candidate="${candidate}/${args[j]}"
        done
        if [ -f "$candidate" ] && [ -x "$candidate" ]; then
             found="$candidate"
             found_index=$i
             break
        fi
    done

    if [ -z "$found" ]; then
        echo "Error: No valid executable found matching the given arguments in ./tasks." >&2
        exit 1
    fi

    # Execute the found file with any remaining arguments.
    exec "$found" "${args[@]:$found_index}"

test:
    cargo nextest run
    just run sqlite test
    just run fuse test

metastore-tag:
    @echo {{METASTORE_ANTITHESIS_TAG}}

metastore-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        --target metastore \
        -t metastore \
        -t {{METASTORE_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} .

pagestore-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        --target pagestore \
        -t pagestore \
        -t {{PAGESTORE_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} .

test-workload-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        --target test_workload \
        -t test_workload \
        -t {{TEST_WORKLOAD_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} .

antithesis-config-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        -t antithesis-config \
        -t {{CONFIG_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} --build-arg TAG={{GIT_SHA}} \
        tests/antithesis

minio-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        -t minio \
        -t {{MINIO_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} tests/antithesis/minio

build-images: metastore-image pagestore-image

antithesis-prep: antithesis-config-image
    just instrumented=1 build-images test-workload-image minio-image
    docker push {{METASTORE_ANTITHESIS_TAG}}
    docker push {{PAGESTORE_ANTITHESIS_TAG}}
    docker push {{CONFIG_ANTITHESIS_TAG}}
    docker push {{TEST_WORKLOAD_ANTITHESIS_TAG}}
    docker push {{MINIO_ANTITHESIS_TAG}}

antithesis-run duration='120': antithesis-prep
    antithesis run \
        --name='graft test workload' \
        --description='{{GIT_SUMMARY}}' \
        --tenant="${ANTITHESIS_TENANT}" \
        --username="${ANTITHESIS_USERNAME}" \
        --password="${ANTITHESIS_PASSWORD}" \
        --config='{{CONFIG_ANTITHESIS_TAG}}' \
        --image='{{METASTORE_ANTITHESIS_TAG}}' \
        --image='{{PAGESTORE_ANTITHESIS_TAG}}' \
        --image='{{TEST_WORKLOAD_ANTITHESIS_TAG}}' \
        --image='{{MINIO_ANTITHESIS_TAG}}' \
        --duration={{duration}} \
        --email='antithesis-results@orbitinghail.dev'
