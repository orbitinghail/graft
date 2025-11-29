---
title: Pragmas
description: Interact with the Graft SQLite extension using pragmas
---

The application can interact with the Graft SQLite extension using the following [pragma statements].

[pragma statements]: https://www.sqlite.org/pragma.html

## Graft Management

### `pragma graft_list`

Lists all grafts and their status.

```sql
pragma graft_list;
```

Shows each graft's local Volume ID, remote Volume ID, and sync status. The current graft is marked with "(current)".

### `pragma graft_tags`

Lists all tags and their associated grafts.

```sql
pragma graft_tags;
```

Displays tag names, the graft they point to, remote Volume IDs, and sync status.

### `pragma graft_new`

Creates a new graft with a random Volume ID.

```sql
pragma graft_new;
```

This creates a fresh, empty database with no remote tracking.

### `pragma graft_switch = "vid[:remote_vid]"`

Switches the current connection to a different graft.

```sql
-- Switch to a specific graft by Volume ID
pragma graft_switch = "GonugMKom6Q92W5YddpVTd";

-- Switch to a graft and set its remote
pragma graft_switch = "GonugMKom6Q92W5YddpVTd:GpABCDEFGHIJKLMNOPQRST";
```

If the graft doesn't exist, it will be created. Optionally specify a remote Volume ID to track.

### `pragma graft_clone = "remote_vid"`

Clones a graft from a remote volume.

```sql
-- Clone from a specific remote volume
pragma graft_clone = "GonugMKom6Q92W5YddpVTd";

-- Clone from the current graft's remote
pragma graft_clone;
```

Creates a new local graft that tracks the specified remote volume. Like `git clone`.

### `pragma graft_fork`

Forks the current snapshot into a new independent graft.

```sql
pragma graft_fork;
```

Creates a divergent copy of your current database state. The volume must be fully hydrated (all pages downloaded) before forking. Like `git fork` - creates an independent copy.

## Introspection

### `pragma graft_info`

Shows detailed information about the current graft.

```sql
pragma graft_info;
```

Displays:
- Graft ID (local Volume ID)
- Remote Volume ID
- Last sync status
- Current snapshot
- Snapshot page count
- Snapshot size

### `pragma graft_status`

Shows the synchronization status of the current graft.

```sql
pragma graft_status;
```

Indicates whether the local volume is ahead, behind, or up-to-date with the remote. Suggests actions like `pragma graft_pull` or `pragma graft_push` when appropriate.

### `pragma graft_snapshot`

Returns a compressed description of the current connection's snapshot.

```sql
pragma graft_snapshot;
```

Shows the snapshot structure, which may span multiple volumes with LSN ranges.

### `pragma graft_audit`

Shows page coverage statistics for the current snapshot.

```sql
pragma graft_audit;
```

Reports how many pages are cached locally versus the total number of pages. If fully hydrated, shows a checksum. Otherwise, suggests using `pragma graft_hydrate`.

### `pragma graft_version`

Displays Graft's version and commit hash.

```sql
pragma graft_version;
```

Useful for debugging and support.

## Synchronization

### `pragma graft_fetch`

Fetches remote metadata without applying changes.

```sql
pragma graft_fetch;
```

Updates the local cache of remote checkpoints. Like `git fetch` - downloads metadata but doesn't merge.

### `pragma graft_pull`

Fetches and merges changes from the remote volume.

```sql
pragma graft_pull;
```

Combines fetch and merge into one operation. Like `git pull` - downloads and applies remote changes.

### `pragma graft_push`

Pushes local changes to the remote volume.

```sql
pragma graft_push;
```

Uploads local commits to remote storage and updates remote checkpoints. Like `git push`.

### `pragma graft_hydrate`

Downloads all missing pages for the current snapshot.

```sql
pragma graft_hydrate;
```

Ensures all pages are available locally. Required before `pragma graft_fork`.

## Data Import

### `pragma graft_import = "PATH"`

Imports an existing SQLite database file into the current graft.

```sql
pragma graft_import = "/path/to/database.db";
```

Reads a SQLite database file and writes its pages into the current graft. The graft must be empty before importing.
