#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--duration seconds] [--registry <registry>] [--desc <description>]" >&2
}

DURATION="60"
REGISTRY=""
USER_DESCRIPTION=""
DOCKER_PLATFORM="linux/amd64"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --duration" >&2
        usage
        exit 2
      fi
      DURATION="$2"
      shift 2
      ;;
    --registry)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --registry" >&2
        usage
        exit 2
      fi
      REGISTRY="$2"
      shift 2
      ;;
    --desc)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --desc" >&2
        usage
        exit 2
      fi
      USER_DESCRIPTION="$2"
      shift 2
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
    *)
      echo "Unexpected argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

# Build images
docker build --platform "$DOCKER_PLATFORM" --build-arg INSTRUMENTED=1 \
  --target metastore -t metastore .

docker build --platform "$DOCKER_PLATFORM" --build-arg INSTRUMENTED=1 \
  --target pagestore -t pagestore .

docker build --platform "$DOCKER_PLATFORM" --build-arg INSTRUMENTED=1 \
  --target test_workload -t test_workload .

docker build --platform "$DOCKER_PLATFORM" --build-arg INSTRUMENTED=1 \
  --target minio -t minio .

docker build --platform "$DOCKER_PLATFORM" --build-arg INSTRUMENTED=1 \
  -t config antithesis

if [[ -z "$REGISTRY" ]]; then
  echo "Build complete. No --registry provided, skipping push and test run."
  exit 0
fi

METASTORE_IMAGE="${REGISTRY}/metastore:latest"
PAGESTORE_IMAGE="${REGISTRY}/pagestore:latest"
CONFIG_IMAGE="${REGISTRY}/config:latest"
TEST_WORKLOAD_IMAGE="${REGISTRY}/test_workload:latest"
MINIO_IMAGE="${REGISTRY}/minio:latest"

# Tag and push images
docker tag metastore "$METASTORE_IMAGE"
docker tag pagestore "$PAGESTORE_IMAGE"
docker tag config "$CONFIG_IMAGE"
docker tag test_workload "$TEST_WORKLOAD_IMAGE"
docker tag minio "$MINIO_IMAGE"

docker push "$METASTORE_IMAGE"
docker push "$PAGESTORE_IMAGE"
docker push "$CONFIG_IMAGE"
docker push "$TEST_WORKLOAD_IMAGE"
docker push "$MINIO_IMAGE"

GIT_REV="$(git rev-parse HEAD)"
RUN_DESCRIPTION="graft bughunt (rev ${GIT_REV})"
if [[ -n "$USER_DESCRIPTION" ]]; then
  RUN_DESCRIPTION="${RUN_DESCRIPTION} - ${USER_DESCRIPTION}"
fi

# Submit test run
snouty run \
  --webhook basic_test \
  --antithesis.test_name 'graft-bug-hunt' \
  --antithesis.description "$RUN_DESCRIPTION" \
  --antithesis.config_image "$CONFIG_IMAGE" \
  --antithesis.images "${METASTORE_IMAGE};${PAGESTORE_IMAGE};${TEST_WORKLOAD_IMAGE};${MINIO_IMAGE}" \
  --antithesis.duration "$DURATION" \
  --antithesis.report.recipients 'antithesis-results@orbitinghail.dev'
