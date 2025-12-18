set unstable := true

GIT_SHA := `git describe --abbrev=40 --always --dirty --match=nevermatch 2>/dev/null`
GIT_SUMMARY := `git show --no-patch 2>/dev/null`
DOCKER_PLATFORM := "linux/amd64"
ANTITHESIS_REGISTRY := "us-central1-docker.pkg.dev/molten-verve-216720/orbitinghail-repository"
CONFIG_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "config:" + GIT_SHA
TEST_CLIENT_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "test_client:" + GIT_SHA

default:
    @just --list

[no-exit-message]
[positional-arguments]
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
    cargo test --doc
    just run sqlite test

build-all:
    cargo build
    cargo build --no-default-features --features register-static --package graft-sqlite
    cargo build --no-default-features --features static --package graft-ext
    cargo build --no-default-features --features dynamic --package graft-ext

test-workload-image:
    docker build \
      --platform {{ DOCKER_PLATFORM }} \
      --target test_client \
      -t test_client \
      -t {{ TEST_CLIENT_ANTITHESIS_TAG }} \
      -f antithesis.Dockerfile \
      .

antithesis-config-image:
    docker build \
      --platform {{ DOCKER_PLATFORM }} \
      -t antithesis-config \
      -t {{ CONFIG_ANTITHESIS_TAG }} \
      --build-arg TAG={{ GIT_SHA }} \
      tests/antithesis

antithesis-build: antithesis-config-image test-workload-image

antithesis-push: antithesis-build
    docker push {{ CONFIG_ANTITHESIS_TAG }}
    docker push {{ TEST_CLIENT_ANTITHESIS_TAG }}

antithesis-run duration='120': antithesis-push
    snouty run \
      --webhook basic_test \
      --antithesis.test_name 'graft test workload' \
      --antithesis.description '{{ GIT_SUMMARY }}' \
      --antithesis.config_image '{{ CONFIG_ANTITHESIS_TAG }}' \
      --antithesis.images '{{ TEST_CLIENT_ANTITHESIS_TAG }}' \
      --antithesis.duration {{ duration }} \
      --antithesis.report.recipients 'antithesis-results@orbitinghail.dev'
