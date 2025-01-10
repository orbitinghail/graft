set unstable

# set this argument via: just instrumented=1 ...
instrumented := ""
BUILD_ARGS := instrumented && "--build-arg INSTRUMENTED=1" || ""

ANTITHESIS_REGISTRY := "us-central1-docker.pkg.dev/molten-verve-216720/ant-pdogfood-repository"
DOCKER_PLATFORM := "linux/amd64"

METASTORE_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "metastore:latest"
PAGESTORE_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "pagestore:latest"
CONFIG_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "config:latest"
TEST_WORKLOAD_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "test_workload:latest"
MINIO_ANTITHESIS_TAG := ANTITHESIS_REGISTRY / "minio:latest"

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

antithesis-config-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        -t {{CONFIG_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} antithesis

test-workload-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        --target test_workload \
        -t test_workload \
        -t {{TEST_WORKLOAD_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} .

minio-image:
    docker build \
        --platform {{DOCKER_PLATFORM}} \
        --target minio \
        -t minio \
        -t {{MINIO_ANTITHESIS_TAG}} \
        {{BUILD_ARGS}} .

build-images: metastore-image pagestore-image test-workload-image minio-image

antithesis-prep: antithesis-config-image
    just instrumented=1 build-images
    docker push {{METASTORE_ANTITHESIS_TAG}}
    docker push {{PAGESTORE_ANTITHESIS_TAG}}
    docker push {{CONFIG_ANTITHESIS_TAG}}
    docker push {{TEST_WORKLOAD_ANTITHESIS_TAG}}
    docker push {{MINIO_ANTITHESIS_TAG}}

antithesis-run: antithesis-prep
    antithesis-cli run \
        --name='graft' \
        --description='my first antithesis test' \
        --tenant="${ANTITHESIS_TENANT}" \
        --username="${ANTITHESIS_USERNAME}" \
        --password="${ANTITHESIS_PASSWORD}" \
        --config='{{CONFIG_ANTITHESIS_TAG}}' \
        --image='{{METASTORE_ANTITHESIS_TAG}}' \
        --image='{{PAGESTORE_ANTITHESIS_TAG}}' \
        --image='{{TEST_WORKLOAD_ANTITHESIS_TAG}}' \
        --image='{{MINIO_ANTITHESIS_TAG}}' \
        --duration=15 \
        --email='carl@f0a.org'
