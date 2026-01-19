A Grove (Graft Repository) contains a tree of content-addressed Commits.

A Commit represents a sparse change to a Volume.

```rust
struct Commit {
    /// The Commit ID: A 32 byte blake3 hash of the Commit metadata + data
    cid: CommitId,

    /// The Commit's parent CommitId
    parent: CommitId,

    /// Version vector referencing every log containing an ancestor
    logs: Map<LogId, LSN>,

    /// The timestamp of the Commit
    timestamp: Timestamp,

    /// True when this Commit contains all of the Pages in the Volume.
    /// Note: Volumes are Sparse, so this property does not imply that the size
    /// of the Segment is equal to the PageCount.
    is_image: bool,

    /// The total number of pages in the Volume as of this Commit
    page_count: PageCount,

    /// If this commit contains any pages, `segment_idx` tracks them
    segment_idx: Option<SegmentIdx>,
}

struct SegmentIdx {
    /// The Segment ID: A 32 byte blake3 hash of the Segment
    sid: SegmentId,

    /// A sorted set of `PageIdxs`. Stored as a Splinter.
    pageset: PageSet,

    /// An index of this Segment's frames.
    frames: Vec<SegmentFrameIdx>,
}

pub struct SegmentFrameIdx {
    /// The length of the compressed frame in bytes.
    frame_size: u64,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    last_pageidx: PageIdx,
}
```

A Volume is a sparse ordered set of Pages indexed by PageIdx.

A PageCount represents the number of pages in a Volume.

A PageIdx is the index of a Page in a Volume. PageIdx is a 32-bit unsigned integer which starts at the value one. The value zero is not a valid PageIdx and can be used as a sentinel value.

A Page is a fixed size array of bytes. All Pages in a Volume have the same size.

An Image Commit contains all pages that have been written to the Volume. Because Volumes may be sparse, an Image Commit’s Segment may contain fewer pages than the Volume’s logical page count. Any page not present in the Segment must be treated as empty by the reader.

Commits form a tree which may contain Branches, but not merges (not a DAG). Thus, any commits which wish to be merged into a branch must be first rebased on top of the latest commit in that branch.

**Valid Graft Trees:**

```
o---o---o

o---o---o
     \
      o---o---o

o---o---o
    |    \
     \     o---o---o
      o---o
```

A Branch is a named, mutable reference to a Commit.

```
o---o---o  ← main
      \
       o---o---o  ← dev
```

```rust
struct Branch {
    /// The name of the Branch
    name: String,
    tip: CommitHash,
    logs: Map<LogId, LSN>,
}
```
