---
title: Architecture
description: An overview of how Graft works
sidebar:
  order: 0
---

Graft is a transactional storage engine designed for **lazy, partial replication** to the edge. It provides strong consistency with object storage durability, making it ideal for syncing data across distributed environments.

## High-Level Architecture

```
┌─────────────────────────────────────────┐
│    SQLite Extension (libgraft)          │
│    VFS + Pragma Interface               │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│        Runtime (graft-kernel)           │
│  • Tag Management                       │
│  • Volume Management                    │
│  • VolumeReader/Writer                  │
│  • Sync Operations (pull/push/fetch)    │
└────────────────┬────────────────────────┘
                 │
      ┌──────────┴───────────┐
      │                      │
┌─────▼──────────┐   ┌───────▼───────────┐
│ FjallStorage   │   │ Remote Storage    │
│   (Local)      │   │ (S3/FS/Memory)    │
│                │   │                   │
│ LSM Partitions │   │ - checkpoints     │
│ - tags         │   │ - commits         │
│ - volumes      │   │ - segments        │
│ - log          │   │                   │
│ - pages        │   │                   │
└────────────────┘   └───────────────────┘
```

## Core Transaction Model

Graft's transactional system enables safe concurrent access through snapshot isolation combined with strict commit serialization.

### Lock-Free Concurrent Reads

All reads operate against immutable [Snapshots](/docs/internals/glossary/#transaction--sync-concepts). A Snapshot is a logical view of a [Volume](/docs/concepts/volumes/) at a specific point in time, consisting of LSN ranges from one or more logs. Because snapshots are immutable, multiple [VolumeReaders](/docs/internals/glossary/#replication-concepts) can safely read in parallel without coordination or locking.

### Read-Your-Write Semantics

[VolumeWriters](/docs/internals/glossary/#replication-concepts) provide transactional write isolation on top of a snapshot. Each writer maintains:

- An immutable base snapshot from transaction start.
- A staged segment tracking all pages modified in the transaction.
- Read-your-write semantics: reads within the transaction see uncommitted writes from the current transaction.

### Strictly Serialized Commits

While reads are lock-free, commits are strictly serialized using optimistic concurrency control:

1. **Validation Phase**: Before committing, verify the base snapshot is still the latest version
2. **Serialization**: Acquire a global write lock ensuring commits execute one at a time
3. **Write**: Append the new commit to the log with a monotonically increasing [LSN](/docs/internals/glossary/#transaction--sync-concepts)
4. **Conflict Detection**: If validation fails, return an error requiring the transaction to abort and retry

## Replication Model

Graft's replication system coordinates changes between local and remote logs using the [SyncPoint](/docs/internals/glossary/#replication-concepts) to track synchronization state.

### Push: Local to Remote with Rollup

When pushing local changes to remote storage:

1. **Plan Commit**: Determine which LSN range to push by comparing the local log head against the SyncPoint's `local_watermark`
2. **Build Segment**: Create a snapshot of the LSN range and collect all referenced pages, deduplicating to include only the latest version of each page. This effectively rolls up multiple local commits into a single remote commit
3. **Compress**: Use Zstd to compress pages into frames (up to 64 pages per frame) for efficient storage and transfer
4. **Upload**: Write the segment to object storage at `/segments/{SegmentId}`
5. **Atomic Commit**: Write the commit metadata to `/logs/{LogId}/commits/{LSN}` using a conditional write to detect conflicts
6. **Update SyncPoint**: On success, update the Volume's SyncPoint with the new LSNs in the local and remote logs, marking these changes as synced

### Pull: Remote to Local with Snapshots

When pulling remote changes:

1. **Fetch Commits**: Stream missing commits from the remote log at `/logs/{LogId}/commits/{LSN}`, downloading only LSNs not yet present locally
2. **Detect Divergence**: Check if both local and remote have uncommitted changes (divergence requires manual intervention)
3. **Update SyncPoint**: If no divergence, update the Volume's SyncPoint with `remote` pointing to the latest remote LSN, marking those commits as pulled

The SyncPoint serves as the coordination mechanism, tracking both what's been pulled from remote (`remote` field) and what's been pushed to remote (`local_watermark` field). New snapshots are always built from the current SyncPoint, ensuring consistent views across the replication boundary.

Pages are loaded lazily on-demand: when a reader requests a page, Graft first searches the snapshot to find which segment contains it, then fetches that segment's frame from remote storage if not already cached locally.
