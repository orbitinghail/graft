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
│  • Graft Management                     │
│  • GraftReader/Writer                   │
│  • Sync Operations (pull/push/fetch)    │
└────────────────┬────────────────────────┘
                 │
      ┌──────────┴──────────┐
      │                     │
┌─────▼─────────┐   ┌──────▼────────────┐
│ FjallStorage  │   │ Remote Storage    │
│   (Local)     │   │ (S3/FS/Memory)    │
│               │   │                   │
│ LSM Partitions│   │ Object Paths:     │
│ - tags        │   │ - commits         │
│ - grafts      │   │ - segments        │
│ - log         │   │                   │
│ - pages       │   │                   │
└───────────────┘   └───────────────────┘
```

## Core Concepts

### Volume

A **Volume** is a sparse, append-only collection of 4 KiB pages. Each page is addressed by a PageIdx starting at 1. Volumes are identified by a unique VolumeId ([GID](/docs/internals/gid/)).

Volumes can be **local** (your working copy, cached on device) or **remote** (the source of truth in object storage).

See [Volumes](/docs/concepts/volumes/) for more details.

### Graft

A **Graft** is the replication unit in Graft. It pairs a local volume with a remote volume and tracks their synchronization state.

Think of a graft like a git branch tracking a remote:
- **Local Volume**: Your working copy
- **Remote Volume**: The source of truth
- **SyncPoint**: Tracks which LSNs have been synced

Grafts enable bidirectional synchronization - you can pull changes from remote and push changes to remote.

### Tag

A **Tag** is a human-readable name that points to a graft. Instead of remembering Volume IDs like `GonugMKom6Q92W5YddpVTd`, you can use tags like `main` or `production`.

```sql
pragma graft_tags;           -- List all tags
pragma graft_switch = "main"; -- Switch to the "main" tag
```

### Snapshot

A **Snapshot** is an immutable view of one or more volumes at specific LSN ranges. All reads in Graft happen against a snapshot, ensuring consistent reads even while writes are happening.

Snapshots can span multiple volumes, enabling powerful forking capabilities.

### LSN (Log Sequence Number)

An **LSN** is a monotonically increasing number that tracks changes to a volume. Each transaction generates a new LSN. LSNs are unique per volume and never have gaps.

### Segment

A **Segment** is a compressed container for pages stored in remote object storage. Multiple pages are grouped into a single segment with a frame index for random access. Segments are identified by a SegmentId ([GID](/docs/internals/gid/)).

## Data Flow

### Write Transaction

1. Application writes to SQLite → intercepted by Graft VFS
2. GraftWriter accumulates changed pages in a staging area
3. On commit:
   - Generate new LSN
   - Write commit record to local storage
   - Write pages to local storage
   - Return GraftReader with updated snapshot
4. Background autosync (if enabled) pushes changes to remote

### Pull Operation

1. `pragma graft_pull` → Runtime fetches remote metadata
2. Compare remote LSN with local sync point
3. If remote is ahead:
   - Fetch missing commits from remote
   - Update local storage
   - Update graft's sync point
4. Next read sees new data in updated snapshot

### Push Operation

1. `pragma graft_push` → Runtime checks for local changes
2. If local has commits ahead of sync point:
   - Upload commits to remote storage
   - Upload new segments to remote storage
   - Update graft's sync point

## Storage Layer

### FjallStorage (Local)

Local LSM-tree based storage (using the Fjall crate). Stores:
- **tags**: Tag name → VolumeId
- **grafts**: VolumeId → Graft metadata
- **log**: (VolumeId, LSN) → Commit record
- **pages**: (SegmentId, PageIdx) → Page data

Fast local cache and staging area for transactions.

### Remote Storage (Object Store)

Object storage backend with three variants:
- **Memory**: In-memory (testing)
- **Filesystem**: Local disk (development)
- **S3-compatible**: AWS S3, MinIO, R2, etc. (production)

Storage paths:
- **Commits**: `/volumes/{vid}/log/{LSN}`
- **Segments**: `/segments/{sid}`

All metadata and data live in object storage - no separate services needed.

## Architectural Benefits

### No Servers Required

Unlike Graft v1, there are no MetaStore or PageStore servers. Each client accesses object storage directly. This makes deployment simple and costs predictable.

### Lazy Replication

Clients only download the pages they actually access. A 100 GB database might only require 10 MB locally if you're only querying a small subset.

### Instant Read Replicas

New replicas can start serving reads immediately by using remote metadata. No need to download the entire database or replay a log.

### Edge-Optimized

The stateless, object-storage-based design works anywhere - cloud, edge, mobile, embedded devices. Perfect for offline-first applications.

### Strong Consistency

Despite being distributed, Graft provides [Serializable Snapshot Isolation](/docs/concepts/consistency/) globally, ensuring correct and consistent data views.
