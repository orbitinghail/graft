# The Graft SQLite Extension (libgraft)

`libgraft` is a native SQLite extension that works anywhere SQLite does. It uses Graft to replicate just the parts of the database that a client actually uses, making it possible to run SQLite in resource constrained environments.

`libgraft` implements a SQLite virtual file system (VFS) allowing it to intercept all reads and writes to the database. It provides the same transactional semantics as SQLite does when running in WAL mode. Using `libgraft` provides your application with the following benefits:

- asynchronous replication to object storage
- stateless lazy partial replicas on the edge and in devices
- optimistic concurrency
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

## Using `libgraft` in the SQLite command-line interface

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
sqlite> # (you can generate a unique vid by running `just run tool vid` in the Graft Git repo)
sqlite> .open 'file:GonugMKom6Q92W5YddpVTd?vfs=graft

sqlite> # verify Graft is working using pragma
sqlite> pragma graft_status;
Graft Status
Client ID: Qi81Dp4C52izQ3LwX2YfZJ
Volume ID: GonugMKom6Q92W5YddpVTd
Current snapshot: None
Autosync: true
Volume status: Ok
```

## Using `libgraft` from your favorite programming language:

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
# (you can generate a unique vid by running `just run tool vid` in the Graft Git repo)
volume_id = "GonugMKom6Q92W5YddpVTd"
conn = sqlite3.connect(f"file:{volume_id}?vfs=graft", autocommit=True, uri=True)

# use pragma to verify graft is working
result = conn.execute("pragma graft_status")
print(result.fetchall()[0][0])
```

## Configuration

Currently the `libgraft` extension is configured via environment variables:

`GRAFT_DIR`:
This variable must be a valid path to an existing directory in the filesystem. Graft will store all of it's data in this directory.

`GRAFT_PROFILE`:
This variable will be used to derive the Graft ClientID and should be unique among all clients of the Volume.

`GRAFT_METASTORE`:
A URL pointing at the Graft MetaStore. Defaults to localhost:3001

`GRAFT_PAGESTORE`:
A URL pointing at the Graft PageStore. Defaults to localhost:3000

## Supported pragmas

The application can communicate and control Graft via the following pragma statements:

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
