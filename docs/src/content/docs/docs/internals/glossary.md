---
title: Glossary
description: Glossary of Graft terms.
---

- **GID**
  A 128 bit Graft Identifier. See [GIDs](/docs/internals/gid/) for details.

- **Volume**
  A sparse data object consisting of Pages located at PageIdxs starting from 1. Volumes are referred to primarily by a Volume ID.

- **Volume ID**
  A 16 byte GID used to uniquely identify a Volume.

- **Page**
  A fixed-length block of storage. The default size is 4KiB (4096 bytes).

- **PageIdx**
  The index of a page within a volume. The first page of a volume has a page index of 1.

- **PageCount**
  The number of logical pages in a Volume. This does not take into account sparseness. This means that if a page is written to PageIdx(1000) in an empty Volume, the Volume's size will immediately jump to 1000 pages.

- **LSN** (Log Sequence Number)
  A sequentially increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume. The commit process ensures that the sequence of LSNs never has gaps and is monotonic.

- **Snapshot**
  A tuple (volume id, lsn, PageCount) that defines a fixed point in time for the state of a volume.

- **Segment**
  An object stored in blob storage containing Pages and an index mapping from (Volume ID, PageIdx) to each Page.

- **Segment ID**
  A 16 byte GID used to uniquely identify a Segment.
