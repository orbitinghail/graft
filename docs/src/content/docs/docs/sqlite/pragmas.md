---
title: Pragmas
description: Interact with the Graft SQLite extension using pragmas
---

The application can interact with the Graft SQLite extension using the following [pragma statements].

[pragma statements]: https://www.sqlite.org/pragma.html

## Graft Management

### `pragma graft_volumes`

Lists all Volumes and their status.

```sql
pragma graft_volumes;
```

Shows each Volume's Volume ID, local Log ID, remote Log ID, and sync status. The current Volume is marked with "(current)".

### `pragma graft_tags`

Lists all tags and their associated Volumes.

```sql
pragma graft_tags;
```

Displays tag names, the Volume they point to, and sync status.

### `pragma graft_new`

Creates a new Volume with a random Volume ID.

```sql
pragma graft_new;
```

This is a convenience shortcut for `pragma graft_switch` with a randomly generated VolumeId. It creates a fresh, empty database with no remote tracking.

### `pragma graft_switch = "local_vid[:local[:remote]]"`

Switches the current connection to a different Volume.

```sql
-- Switch to a specific Volume by Volume ID
pragma graft_switch = "GonugMKom6Q92W5YddpVTd";

-- Switch to a Volume and specify its local and remote Log IDs
pragma graft_switch = "GonugMKom6Q92W5YddpVTd:GpABCDEFGHIJKLMNOPQRST:GqXYZABCDEFGHIJKLMNOPQ";
```

If the Volume doesn't exist, it will be created. Optionally specify a local LogId and remote LogId to track.

### `pragma graft_clone = "remote_log_id"`

Clones a Volume from a remote Log.

```sql
-- Clone from a specific remote Log
pragma graft_clone = "GonugMKom6Q92W5YddpVTd";

-- Clone from the current Volume's remote Log
pragma graft_clone;
```

Creates a new local Volume that tracks the specified remote Log. Like `git clone`.

### `pragma graft_fork`

Forks the current snapshot into a new independent Volume.

```sql
pragma graft_fork;
```

Creates a divergent copy of your current database state. The Volume must be fully hydrated (all pages downloaded) before forking. Like `git fork` - creates an independent copy.

## Introspection

### `pragma graft_info`

Shows detailed information about the current Volume.

```sql
pragma graft_info;
```

Displays:
- Volume ID
- Local Log ID
- Remote Log ID
- Last sync status
- Current snapshot
- Snapshot page count
- Snapshot size

### `pragma graft_status`

Shows the synchronization status of the current Volume.

```sql
pragma graft_status;
```

Indicates whether the local Log is ahead, behind, or up-to-date with the remote Log. Suggests actions like `pragma graft_pull` or `pragma graft_push` when appropriate.

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

Fetches and merges changes from the remote Log.

```sql
pragma graft_pull;
```

Combines fetch and merge into one operation. Like `git pull` - downloads and applies remote changes.

### `pragma graft_push`

Pushes local changes to the remote Log.

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

Imports an existing SQLite database file into the current Volume.

```sql
pragma graft_import = "/path/to/database.db";
```

Reads a SQLite database file and writes its pages into the current Volume. The Volume must be empty before importing.
