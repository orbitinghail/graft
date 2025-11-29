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

## Storage Concepts

- **Volume**
  A sparse data object consisting of Pages located at PageIdxs starting from 1. Volumes are referred to by their Volume ID. See [Volumes](/docs/concepts/volumes) for details.

- **Page**
  A fixed-length block of storage. The default size is 4 KiB (4096 bytes).

- **PageIdx**
  The index of a page within a volume. The first page of a volume has a page index of 1.

- **PageCount**
  The number of logical pages in a Volume. This does not account for sparseness. Writing to PageIdx(1000) in an empty Volume immediately sets the page count to 1000.

- **Segment**
  A file composed of one or more ZStd compressed frames containing pages. Segments are tracked by Commits.

## Transaction & Sync Concepts

- **LSN** (Log Sequence Number)
  A sequentially increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume. LSNs never have gaps and are monotonic.

- **Snapshot**
  An immutable logical view of a graft at a particular point in time.

- **Commit**
  A transaction that advances a Volume's LSN and records changed pages.

## Replication Concepts

- **Graft**
  A replication unit that pairs a local Volume with a remote Volume. Contains synchronization metadata (SyncPoint) and enables bidirectional sync. Similar to a git branch tracking a remote.

- **Tag**
  A human-readable name that references a Graft. Enables easy database switching via `pragma graft_switch`. Stored as a name → VolumeId mapping.

- **SyncPoint**
  Tracks synchronization state between local and remote volumes. Contains the remote LSN (attachment point) and optional local watermark (last pushed LSN). Determines ahead/behind status.

- **GraftReader**
  Read-only interface to a volume at a specific snapshot. Provides page reads and page count queries. Immutable - always sees a consistent snapshot.

- **GraftWriter**
  Write interface for creating new transactions. Accumulates changes in a staging area. Commit creates a new snapshot and returns a GraftReader.

## Storage Implementation

- **FjallStorage**
  Local LSM-tree based storage layer. Stores tags, grafts, commit log, and pages. Provides fast local cache and transaction staging.

- **Remote Storage**
  Object storage backend (S3, filesystem, or memory). Stores commits and segments. The shared source of truth for replication.
