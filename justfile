metastore-image:
    docker build --target metastore -t metastore .

pagestore-image:
    docker build --target pagestore -t pagestore .

antithesis-config-image:
    docker build -t antithesis-config antithesis

build-images: metastore-image pagestore-image antithesis-config-image
