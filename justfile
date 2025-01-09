set unstable

# set this argument via: just instrumented=1 ...
instrumented := ""
BUILD_ARGS := instrumented && "--build-arg INSTRUMENTED=1" || ""

metastore-image:
    docker build --target metastore -t metastore {{BUILD_ARGS}} .

pagestore-image:
    docker build --target pagestore -t pagestore {{BUILD_ARGS}} .

antithesis-config-image:
    docker build -t antithesis-config antithesis {{BUILD_ARGS}}

build-images: metastore-image pagestore-image antithesis-config-image
