---
title: Config
description: Configuring the Graft MetaStore and PageStore
---

Graft can be configured by environment variables or configuration files. The default production config files are located at [`deploy/metastore/metastore.toml`] and [`deploy/pagestore/pagestore.toml`]. Both binaries search for their configuration file in their current working directory.

[`deploy/metastore/metastore.toml`]: https://github.com/orbitinghail/graft/blob/v1/deploy/metastore/metastore.toml
[`deploy/pagestore/pagestore.toml`]: https://github.com/orbitinghail/graft/blob/v1/deploy/pagestore/pagestore.toml

## Object Storage

The PageStore and MetaStore pick up standard AWS environment variables for their connection to object storage. These variables include:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT`

Review the documentation of your chosen object store to determine how to set these variables correctly.

## PageStore

All configuration variables can be specified via environment variables prefixed with `PAGESTORE_`. Nested segments should be separated by `__` (double underscore). For example: `PAGESTORE_CACHE__SPACE_LIMIT="1 MB"`.

```toml
# the port to listen on
port = 3000
# metastore url
metastore = "http://127.0.0.1:3001"
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

## MetaStore

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
