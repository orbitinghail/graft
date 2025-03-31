# The Graft SQLite Extension (libgraft)

`libgraft` is a native SQLite extension that works anywhere SQLite does. It uses Graft to replicate just the parts of the database that a client actually uses, making it possible to run SQLite in resource constrained environments.

`libgraft` implements a SQLite virtual file system (VFS) allowing it to intercept all reads and writes to the database. It provides the same transactional semantics as SQLite does when running in WAL mode. Using `libgraft` provides your application with the following benefits:

- asynchronous replication to object storage
- stateless lazy partial replicas on the edge and in devices
- serializable snapshot isolation
- point in time restore

## Compatibility

The Graft SQLite extension should work with any version of [SQLite] after 3.44.0 (released Nov 2023). It probably works with earlier versions, but no guarantees.

[SQLite]: https://www.sqlite.org/index.html

## Downloading `libgraft`

### Manual download

`libgraft`, is released using [GitHub Releases] for most platforms. You can access the latest release using the links below:

| Platform | Architecture | Download Link                   |
| -------- | ------------ | ------------------------------- |
| Linux    | x86_64       | [libgraft-linux-x86_64.tar.gz]  |
| Linux    | aarch64      | [libgraft-linux-aarch64.tar.gz] |
| Windows  | x86_64       | [libgraft-windows-x86_64.zip]   |
| Windows  | aarch64      | [libgraft-windows-aarch64.zip]  |
| macOS    | x86_64       | [libgraft-macos-x86_64.tar.gz]  |
| macOS    | aarch64      | [libgraft-macos-aarch64.tar.gz] |

After downloading the file for your system's platform and architecture, decompress the file to access the extension, which is named `libgraft.[dll,dylib,so]`.

[libgraft-linux-x86_64.tar.gz]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-linux-x86_64.tar.gz
[libgraft-linux-aarch64.tar.gz]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-linux-aarch64.tar.gz
[libgraft-windows-x86_64.zip]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-windows-x86_64.zip
[libgraft-windows-aarch64.zip]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-windows-aarch64.zip
[libgraft-macos-x86_64.tar.gz]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-macos-x86_64.tar.gz
[libgraft-macos-aarch64.tar.gz]: https://github.com/orbitinghail/graft/releases/latest/download/libgraft-macos-aarch64.tar.gz
[install-sqlite-ext]: https://antonz.org/install-sqlite-extension/
[GitHub Releases]: https://github.com/orbitinghail/graft/releases/latest

### Download using a package manager

Rather than having to download and manage the extension manually, `libgraft` is availble through the [sqlpkg] SQLite extension manager! This means you can download the extension like so:

**Linux/macOS**:

```bash
sqlpkg install orbitinghail/graft
```

**Windows**:

```pwsh
sqlpkg.exe install orbitinghail/graft
```

Once installed, you can find the path to `libgraft` using the `which` subcommand:

**Linux/macOS**:

```bash
sqlpkg which orbitinghail/graft
```

**Windows**:

```pwsh
sqlpkg.exe which orbitinghail/graft
```

The author of `sqlpkg`, [Anton Zhiyanov][anton], published a comprehensive guide to SQLite extensions on their blog [which is available here][sqlpkg-guide]. I highly recommend reading that post for more ways to install and use SQLite extensions.

[anton]: https://www.linkedin.com/in/nalgeon/
[sqlpkg]: https://github.com/nalgeon/sqlpkg-cli
[sqlpkg-guide]: https://antonz.org/install-sqlite-extension/

## Using `libgraft`

### From the SQLite command-line interface

When installed using your system package manager or via another binary distribution, SQLite ships with a command-line interface (CLI) usually called `sqlite3` (`sqlite3.exe` on Windows).

After starting the SQLite shell you can load the Graft extension with the `.load` command:

```sqlite
.load PATH_TO_LIBGRAFT
```

Here is an example of loading `libgraft` on linux, opening a Volume, and checking `pragma graft_status` to make sure it all works:

```
➜ sqlite3
SQLite version 3.49.1 2025-02-18 13:38:58
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.

sqlite> # load the Graft extension
sqlite> .load ./libgraft.so

sqlite> # open a Graft Volume as a database
sqlite> .open 'file:random?vfs=graft

sqlite> # verify Graft is working using pragma
sqlite> pragma graft_status;
Graft Status
Client ID: Qi81Dp4C52izQ3LwX2YfZJ
Volume ID: GonugMKom6Q92W5YddpVTd
Current snapshot: None
Autosync: true
Volume status: Ok
```

### From your favorite programming language:

SQLite is available as a library in most programming languages, and as long the language/runtime supports loading SQLite extensions, `libgraft` should work!

Here is an example of loading Graft using Python's built in SQLite support:

```python
import sqlite3

# the path to libgraft
# change the extension to .dylib on macOS and .dll on Windows
libgraft_path = "./libgraft.so"

# load graft using a temporary (empty) in-memory SQLite database
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension(libgraft_path)

# open a Graft volume as a database
conn = sqlite3.connect(f"file:random?vfs=graft", autocommit=True, uri=True)

# use pragma to verify graft is working
result = conn.execute("pragma graft_status")
print(result.fetchall()[0][0])
```

## Volume IDs

When connecting to a Graft SQLite database, you can specify a particular Volume ID directly:

```sql
.open 'file:GonugMKom6Q92W5YddpVTd?vfs=graft'
```

Alternatively, you can use `random` to automatically generate a new Volume:

```sql
.open 'file:random?vfs=graft'
```

To open additional connections to a randomly generated Volume, you'll first need the generated Volume ID. You can retrieve it using either of the following methods:

- **Using the SQLite CLI:**

  ```sql
  .databases
  ```

  The Volume ID will appear in the second column for each attached database which uses Graft.

- **Programmatically via SQLite interfaces such as Python:**

  ```python
  import sqlite3

  conn = sqlite3.connect('file:random?vfs=graft', autocommit=True, uri=True)
  cursor = conn.execute('PRAGMA database_list')
  db_list = cursor.fetchall()

  for db in db_list:
      db_alias = db[1]    # Database alias (e.g., 'main', 'attached_db')
      volume_id = db[2]   # Filename, i.e., the Volume ID
      print(f"{db_alias}: {volume_id}")
  ```

These retrieved Volume IDs can then be used to open the same Volumes across multiple connections and from multiple nodes.

## Configuration

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

### Example Configuration File (`graft.toml`)

```toml
data_dir = "/var/lib/graft"
metastore = "http://metastore.example.com:3001"
pagestore = "http://pagestore.example.com:3000"
autosync = false
client_id = "QiAaSzeTbNnMQFxK6jm125"
```

## Supported pragmas

The application can interact with Graft using the following pragma statements:

`pragma graft_status`:
Report the status of the current Volume and the current connection's Snapshot. Note that different SQLite connections to the same Graft Volume can concurrently access different snapshots via read transactions.

`pragma graft_snapshot`:
Returns a compressed description of the current connections Snapshot.

`pragma graft_pages`:
Reports the version and cache status of every page accessible by the current connection's Snapshot.

`pragma graft_sync = true|false`:
Turn background sync on or off.

`pragma graft_sync_errors`:
Reveal the most recent 16 errors and warnings encountered during background sync. This pragma pops errors out of a ring buffer, so calling it repeatedly will only report errors and warnings since the last call.

`pragma graft_reset`:
This pragma drops all local changes and forceably resets to the latest server Snapshot. Make sure you are ok with loosing local changes permanently when running this pragma.
