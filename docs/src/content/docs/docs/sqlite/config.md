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

### Configuration Options

The configuration file supports the following options:

#### `data_dir`

- **Environment variable:** `GRAFT_DIR`
- **Description:** Path to the directory where Graft stores its data. Relative paths are resolved from the current working directory.
- **Default:**
  - Linux & macOS: `$XDG_DATA_HOME/graft` or `~/.local/share/graft`
  - Windows: `%LOCALAPPDATA%\graft` or `C:\Users\%USERNAME%\AppData\Local\graft`

#### `metastore`

- **Environment variable:** `GRAFT_METASTORE`
- **Description:** URL for the Graft MetaStore.
- **Default:** `http://127.0.0.1:3001`

#### `pagestore`

- **Environment variable:** `GRAFT_PAGESTORE`
- **Description:** URL for the Graft PageStore.
- **Default:** `http://127.0.0.1:3000`

#### `token`

- **Environment variable:** `GRAFT_TOKEN`
- **Description:** Provide an API token to use when connecting to the Graft MetaStore and PageStore.

#### `autosync`

- **Environment variable:** `GRAFT_AUTOSYNC`
- **Description:** Enables or disables background synchronization.
- **Default:** `true`
- **Values:** `true`, `false`
- **Note:** Even if set to `false`, background sync can be enabled explicitly using `pragma graft_sync = true`.

#### `client_id`

- **Environment variable:** `GRAFT_CLIENT_ID`
- **Description:** Specify a unique Client ID to use. If not set, a new Client ID is randomly generated. It is strongly recommended to set this explicitly in production environments.

#### `log_file`

- **Environment variable:** `GRAFT_LOG_FILE`
- **Description:** Write a verbose log of all Graft operations to the specified log file. The verbosity can be controlled using the environment variable `RUST_LOG`. Valid verbosity levels are: `error, warn, info, debug, trace`

#### `make_default`

- **Environment variable:** `GRAFT_MAKE_DEFAULT`
- **Description:** When `make_default` is true, Graft will register itself as the _default_ SQLite VFS which will cause _all_ new connections to use Graft. This is mainly useful for integrating Graft into SQLite libraries which don't support specifying which VFS to use.

### Example Configuration File (`graft.toml`)

```toml
data_dir = "/var/lib/graft"
metastore = "http://metastore.example.com:3001"
pagestore = "http://pagestore.example.com:3000"
autosync = false
client_id = "QiAaSzeTbNnMQFxK6jm125"
```
