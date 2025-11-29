---
title: Glossary
description: Glossary of Graft terms.
---

## Core Identifiers

- **GID**
  A 128-bit Graft Identifier. See [GIDs](/docs/internals/gid/) for details.
- **Volume ID**
  A 16-byte GID used to uniquely identify a Volume.
- **Segment ID**
  A 16-byte GID used to uniquely identify a Segment.
- **Log ID**
  A 16-byte GID used to uniquely identify a Log.

## Storage Concepts

- **Volume**
  A sparse data object consisting of Pages located at PageIdxs starting from 1. Volumes are referred to by their Volume ID. Each Volume tracks a local Log and a remote Log for replication. See [Volumes](/docs/concepts/volumes) for details.

- **Page**
  A fixed-length block of storage. The default size is 4 KiB (4096 bytes).

- **PageIdx**
  The index of a page within a volume. The first page of a volume has a page index of 1.

- **PageCount**
  The number of logical pages in a Volume. This does not account for sparseness. Writing to `PageIdx(1000)` in an empty Volume immediately sets the page count to 1000.

- **Segment**
  A file composed of one or more ZStd compressed frames containing pages. Segments are tracked by Commits and uploaded to Remote storage.

## Transaction & Sync Concepts

- **LSN** (Log Sequence Number)
  A sequentially increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume. LSNs never have gaps and are monotonic.

- **Snapshot**
  An immutable logical view of a Volume at a particular point in time.

- **Commit**
  A transaction that advances a Volume's LSN and records changed pages.

## Replication Concepts

- **Tag**
  A human-readable name that references a Volume. Stored as a name → VolumeId mapping.

- **SyncPoint**
  Tracks synchronization state between local and remote Logs. Contains the remote LSN (attachment point) and optional local watermark (last pushed LSN). Determines ahead/behind status.

- **VolumeReader**
  Read-only interface to a Volume at a specific snapshot. Immutable - always sees a consistent snapshot. Multiple VolumeReaders can operate in parallel without locking.

- **VolumeWriter**
  Write interface for writing transactionally to a Volume. Provides read-your-writes semantics on top of an immutable Snapshot.

## Storage Implementation

- **FjallStorage**
  Local LSM-tree based storage layer. Stores Tags, Volumes, Logs, and Pages. Provides fast local cache and transaction staging.

- **Remote Storage**
  Object storage backend (S3, filesystem, or memory). Stores commits and segments. The shared source of truth for replication.
