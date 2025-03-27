# The Graft SQLite Extension (libgraft)

`libgraft` is a native SQLite extension that works anywhere SQLite does. It uses Graft to replicate just the parts of the database that a client actually uses, making it possible to run SQLite in resource constrained environments.

`libgraft` implements a SQLite virtual file system (VFS) allowing it to intercept all reads and writes to the database. It provides the same transactional semantics as SQLite does when running in WAL mode. Using `libgraft` provides your application with the following benefits:

- asynchronous replication to object storage
- stateless lazy partial replicas on the edge and in devices
- optimistic concurrency
- point in time restore

## Using `libgraft` with the SQLite shell

The fastest way to play with `libgraft` is by loading the extension into SQLite's CLI. Assuming you have `libgraft` downloaded to the current directory and `SQLite` installed, the following steps should work:

```
# First make sure SQLite can find the extension
export LD_LIBRARY_PATH=${PWD}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# OR, if you are on a mac:
export DYLD_LIBRARY_PATH=${PWD}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}

# Start the SQLite shell:
➜ sqlite3
SQLite version 3.49.1 2025-02-18 13:38:58
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.

sqlite> # load the Graft extension
sqlite> .load libgraft

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

## Using `libgraft` with a SQLite library

SQLite is available as a library in most programming languages, and as long the language/runtime supports loading SQLite extensions, `libgraft` should work!

Here is an example of loading Graft using Python's built in SQLite support:

```python
import sqlite3

# the path to libgraft
# change the extension to .dylib if using a mac
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

Currently `libgraft` is configured via environment variables:

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
