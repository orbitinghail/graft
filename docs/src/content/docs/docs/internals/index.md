---
title: Architecture
description: An overview of how Graft works
sidebar:
  order: 0
---

Graft is a transactional storage engine designed for **lazy, partial replication** to the edge. It provides a strongly consistent data model with object storage durability, making it ideal for syncing structured or semi-structured data across unreliable or distributed environments.

At its core, Graft splits storage responsibilities across three major layers:

---

### 1. **[Volumes]** (Logical Data Model)

A **[Volume]** is the core unit of data in Graft. Each Volume:

- Is identified by a 128-bit Volume ID ([GID])
- Stores fixed-size **Pages** (4KiB each), indexed by `PageIdx` starting at 1
- Tracks a monotonically increasing **LSN** (Log Sequence Number) for transactional updates
- Grows sparsely—writing to `PageIdx(1000)` will increase the volume's `PageCount` to 1000, even if no pages before that were written

Volumes are designed to provide a **mutable, versioned interface** over immutable backend storage.

---

### 2. **Segments** (Physical Storage Format)

Graft writes immutable **Segments** to durable object storage. Each Segment:

- Contains a subset of pages from a specific Volume
- Includes an internal index mapping `(VolumeID, PageIdx)` to page offsets
- Is identified by a unique Segment ID (also a [GID])
- Is created via writes to the [PageStore]

Segments serve as the **durable, append-only log** of page updates over time. They allow efficient reads by indexing content and enable snapshot reconstruction via minimal state tracking.

---

### 3. **Snapshots and Grafts** (Version Control)

- A **Snapshot** represents the state of a Volume at a specific `LSN`. It is defined as `(VolumeID, LSN, PageCount)` and allows consistent reads across distributed clients.
- A **Graft** is a sparse set of `PageIdx`s—essentially a patch or delta—which indicates which pages were changed in a given Segment or between Snapshots.

These structures are used to replicate only the necessary data to edge nodes or replicas. Instead of shipping entire Segments, Graft-aware clients can subscribe to or request just the Pages they care about, using Grafts to stay up to date.

---

### 4. **Backend Services**

Graft’s architecture is service-oriented, with distinct components for coordination and data access:

- **[Metastore]**: Stores metadata for each Volume, including the log of Segments, Snapshot history, and current LSN. Also responsible for coordination tasks like garbage collection, authentication, and graft tracking.
- **[Pagestore]**: Stores the raw Pages stored in Segments. Supports efficient versioned reads of Pages at a particular LSN.

Both services can be deployed independently and are designed to scale horizontally.

---

### 5. **Client Models**

Graft supports multiple client types to serve different replication and access patterns:

- **[Replica Client]**: Subscribes to volume updates and lazily replicates only the Pages it needs, based on Graft data.
- **[Lite Client]**: Stateless client optimized for single reads or writes, typically used for fire-and-forget workloads or one-shot syncs.

[PageStore]: /docs/internals/pagestore/
[MetaStore]: /docs/internals/metastore/
[GID]: /docs/internals/gid/
[Volume]: /docs/concepts/volumes/
[Volumes]: /docs/concepts/volumes/
[Replica Client]: /docs/internals/client/
[Lite Client]: /docs/internals/client/#lite-client
