---
title: Config
description: Configuring the Graft SQLite extension
---

The `libgraft` SQLite extension can be configured using either a configuration file (`graft.toml`) or environment variables.

`libgraft` searches for the configuration file in the current directory or in the following standard locations:

| Platform      | Configuration Path                  | Example                                           |
| ------------- | ----------------------------------- | ------------------------------------------------- |
| Linux & macOS | `$XDG_CONFIG_HOME/graft/graft.toml` | `/home/alice/.config/graft/graft.toml`            |
| Windows       | `%APPDATA%\graft\graft.toml`        | `C:\Users\Alice\AppData\Roaming\graft\graft.toml` |

If the `GRAFT_CONFIG` environment variable is set, `libgraft` will use the provided path instead.

## Configuration Options

### `data_dir`

- **Environment variable:** `GRAFT_DIR`
- **Description:** Path to the directory where Graft stores its local data (Fjall LSM storage).
- **Default:**
  - Linux & macOS: `$XDG_DATA_HOME/graft` or `~/.local/share/graft`
  - Windows: `%LOCALAPPDATA%\graft` or `C:\Users\%USERNAME%\AppData\Local\graft`

### `remote`

Configuration for remote object storage. This is where Graft stores the source of truth for your data.

#### `remote.type = "memory"`

In-memory object storage. Useful for testing and development.

```toml
[remote]
type = "memory"
```

#### `remote.type = "fs"`

Local filesystem storage. Good for development and single-machine deployments.

```toml
[remote]
type = "fs"
root = "/path/to/storage"
```

- **`root`**: Path to the directory where remote data is stored.

#### `remote.type = "s3_compatible"`

S3-compatible object storage (AWS S3, MinIO, R2, etc.). Recommended for production.

```toml
[remote]
type = "s3_compatible"
bucket = "my-graft-bucket"
prefix = "optional/prefix"  # optional
```

- **`bucket`**: S3 bucket name.
- **`prefix`**: Optional path prefix within the bucket.

**Credentials:** S3 credentials and configuration are loaded from standard AWS environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_ENDPOINT` (for S3-compatible services like MinIO, R2, etc.)

### `autosync`

- **Environment variable:** `GRAFT_AUTOSYNC`
- **Description:** Background synchronization interval in seconds. When set, Graft will automatically sync volumes with the remote at this interval.
- **Default:** Not set (no automatic synchronization)
- **Example:** `autosync = 60` (sync every 60 seconds)

### `log_file`

- **Environment variable:** `GRAFT_LOG_FILE`
- **Description:** Write a verbose log of all Graft operations to the specified log file. Verbosity can be controlled using the `RUST_LOG` environment variable.
- **Valid verbosity levels:** `error`, `warn`, `info`, `debug`, `trace`

### `make_default`

- **Environment variable:** `GRAFT_MAKE_DEFAULT`
- **Description:** Cause the Graft VFS to become the default VFS for all new database connections.

## Example Configurations

### Production (S3)

```toml
data_dir = "/var/lib/graft"
autosync = 60

[remote]
type = "s3_compatible"
bucket = "my-app-graft"
prefix = "prod"
```

Set environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

### Development (Filesystem)

```toml
data_dir = "./data"

[remote]
type = "fs"
root = "./remote-storage"
```

### Testing (In-Memory)

```toml
[remote]
type = "memory"
```
