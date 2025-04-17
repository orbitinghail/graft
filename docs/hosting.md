# Self-hosting Graft

The Graft backend is composed of two services: the `PageStore` and the `MetaStore`. In order to run Graft yourself, you will need to run both services somewhere, and ensure that they are connected to a compatible object store (i.e. S3, R2, Tigris).

## Building

### Docker images

The easiest way to build and run the Graft backend is via the Docker images. Make sure you have [just] installed and then run:

```bash
just metastore-image pagestore-image
```

The resulting Docker images will be tagged `metastore:latest` and `pagestore:latest`

### From source

You can build the MetaStore and PageStore from source using:

```bash
cargo build --bin metastore --release --features precept/disabled
cargo build --bin pagestore --release --features precept/disabled
```

The resulting binaries will be available at `./target/release/{metastore,pagestore}`.

[just]: https://github.com/casey/just

## Configuration

Graft can be configured by environment variables or configuration files. The default production config files are located at `./deploy/metastore/metastore.toml` and `./deploy/pagestore/pagestore.toml`. Both binaries search for their configuration file in their current working directory.

### PageStore

All configuration variables can be specified via environment variables prefixed with `PAGESTORE_`. Nested segments should be separated by `__` (double underscore). For example: `PAGESTORE_CACHE__SPACE_LIMIT="1 MB"`.

```toml
# the port to listen on
port = 3000
# metastore url
metastore = "http://localhost:3001"
# PASETO token to authenticate with the metastore; requires the metastore to have authentication enabled.
token = "v4.local.GSdE..."

# concurrent catalog updates across multiple volumes
catalog_update_concurrency = 16
# concurrent segment downloads
download_concurrency = 16
# concurrent writes to different volumes
write_concurrency = 16

[catalog]
# path to the Volume Catalog directory; defaults to a temp dir
path = "..."

[cache]
# path to the page cache directory; defaults to a temp dir
path = "..."
# cache size limit
space_limit = "1 GB"
# maximum open files; defaults to ulimit(NOFILE) / 2
open_limit = 1024

[objectstore]
type = "s3_compatible"
# the bucket name
bucket = "graft-primary"
# add this prefix to all keys
prefix = "pagestore"

# Optionally enable PASETO authentication by specifying an [auth] block.
# See the PASETO docs for more details.
[auth]
# A 32-byte symmetric key encoded into HEX
key = ""
```

### MetaStore

All configuration variables can be specified via environment variables prefixed with `METASTORE_`. Nested segments should be separated by `__` (double underscore). For example: `METASTORE_CATALOG__PATH="..."`.

```toml
# the port to listen on
port = 3000

# concurrent catalog updates across multiple volumes
catalog_update_concurrency = 16

[catalog]
# path to the Volume Catalog directory; defaults to a temp dir
path = "..."

[objectstore]
type = "s3_compatible"
# the bucket name
bucket = "graft-primary"
# add this prefix to all keys
prefix = "pagestore"

# Optionally enable PASETO authentication by specifying an [auth] block.
# See the PASETO docs for more details.
[auth]
# A 32-byte symmetric key encoded into HEX
key = ""
```

## PASETO Authentication

Currently the Graft backend is authenticated using PASETO.

Once configured,all requests must include a PASETO token generated using the same key. You can use `just run tool {secret-key, token, validate-token}` to generate compatible keys and tokens.
