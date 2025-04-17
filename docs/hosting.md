# Self-hosting Graft

The Graft backend is composed of two services: the `PageStore` and the `MetaStore`. In order to run Graft yourself, you will need to run both services somewhere, and ensure that they are connected to a compatible object store (i.e. S3, R2, Tigris).

## Deployment Architecture

The Graft PageStore and MetaStore are ephemeral services that synchronously read and write to object storage. They take advantage of local disk to cache requests but are otherwise stateless - allowing them to scale out horizontally or be distributed across availability zones and regions.

The official Graft managed service runs Graft on Fly.io and uses Tigris as it's object storage provider. This allows Graft to seamlessly be available in regions all around the world.

Graft's networking protocol is Protobuf messages over HTTP. When deployed to the internet Graft should be placed behind a hardened proxy that can terminate SSL, load balance, and pipeline requests.

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

Graft can be configured by environment variables or configuration files. The default production config files are located at [`deploy/metastore/metastore.toml`](/deploy/metastore/metastore.toml) and [`deploy/pagestore/pagestore.toml`](/deploy/pagestore/pagestore.toml). Both binaries search for their configuration file in their current working directory.

### Object Storage

The PageStore and MetaStore pick up standard AWS environment variables for their connection to object storage. These variables include:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT`

Review the documentation of your chosen object store to determine how to set these variables correctly.

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

Currently the Graft backend is authenticated using PASETO. A simple token based authentication system.

> [!IMPORTANT]
> In this guide, keys are shown with portions masked. This is to _ensure_ that you do not use the keys or tokens in this document. Authentication secrets should be generated in a secure environment and stored correctly.

To configure PASETO, you first need to generate a 32-byte hex-encoded secret key:

```bash
$ just run tool secret-key
86d94c08c767...d74c5a8282f81367886255
```

This key must be specified in the PageStore and MetaStore config:

```
[auth]
key = "86d94c08c767...d74c5a8282f81367886255"
```

You can also specify the key with the environment variables `PAGESTORE_AUTH__KEY` and `METASTORE_AUTH__KEY`.

Once configured, all requests must include a PASETO token generated using the same key. You can generate a token for a particular subject like so:

```
$ SK=86d94c08c767...d74c5a8282f81367886255
$ just run tool token --sk $SK subject-name
v4.local.PxdyJ4TwdDIWRUp0C...kgr8Ha11PHD_j9OAUeupqJ_bWa0UJ56nVMk7U
```

Subjects are just metadata attached to the token to differentiate between who is accessing Graft. Graft will eventually associate permissions with subjects in order to enable fine-grained Authorization.

The first token you generate should be for the PageStore to communicate with the MetaStore. You can use any subject, but I like to use `graft-pagestore`. The token can be provided to the PageStore via the config (`token = "..."`) or environment variable: `PAGESTORE_TOKEN=...`.

Once PASETO authentication is configured, all Graft clients must use a valid token. See the [Graft SQLite documentation](./sqlite.md) for more information on configuring the token to use.
