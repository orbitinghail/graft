---
title: PageStore
description: The PageStore stores Volume pages.
---

The Pagestore is responsible for storing and looking up Pages in Segments stored in Blob Storage Services like S3 or Tigris.

## Storage Layout

```
/segments
  /[Segment ID] -> Segment
```

## Segment Layout

A Segment is a binary file composed of three sections: Pages, Index, Footer.

**Footer**
The footer is stored at the end of the Segment.

```
Footer (32 bytes)
  sid: SegmentId
  volumes: u16
  index_size: u16
  _padding: 8 bytes
  magic: u32
```

**Pages**
List of Pages stored back to back starting at the beginning of the segment.

**Index**
A SegmentIndex which has two sections: a Volume Index and a list of PageIdxs.

The Volume Index is a list of (VolumeId, Start, Pages) tuples.

- VolumeId: The VolumeId for this set of pages
- Start: The position of the first page and page index for this Volume
- Pages: The number of pages stored in this Segment for this Volume

The VolumeId Table is sorted by VolumeId.

The list of PageIdxs is stored in the same order as pages are stored in the segment, and the index requires that each set of PageIdxs corresponding to a Volume is sorted.

## Segment Cache

The Pagestore must cache recently read Segments in order to minimize round trips to Object Storage and improve performance. The disk cache should have a configurable target max size, and remove the least recently accessed Segment to reclaim space.

In addition, we should have a memory based cache. One option is to read all of the Segment indexes into memory, and leave page caching up to the kernel. Research needs to be done on if this approach is feasible given the planned compute sizes.

## API

[PageStore API docs](/docs/backend/api#pagestore)

## Pagestore internal dataflow

https://link.excalidraw.com/readonly/TAmndg0ba36Ex63w2F5M
