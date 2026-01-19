# Definitions

A Grove (Graft Repository) is a tree of Logs and content-addressed Segments.

A Log is a monotonic sequence of Commits identified by Log Sequence Number (LSN). A Log either starts from empty or branches from another Log at a particular LSN.

A LSN is a strictly sequential monotonic sequence unsigned 64-bit integer starting at 1. The value 0 is a reserved sentinel.

A Commit represents a sparse change to a Volume.

A Volume is a sparse set of Pages ordered by PageIdx.

A PageCount represents the number of pages in a Volume.

A PageIdx is the index of a Page in a Volume. PageIdx is a 32-bit unsigned integer starting at 1. The value 0 is a reserved sentinel.

A Page is a fixed size array of bytes. All Pages in a Volume have the same size.

An Image Commit contains all pages that have been written to the Volume. Because Volumes may be sparse, an Image Commit’s Segment may contain fewer pages than the Volume’s logical page count. Any page not present in the Segment must be treated as empty by the reader.

All of the Logs in a Grove form a tree. A Log may branch off of another log at any point. The Branch point is represented by a `LogRef { LogId, LSN }`.

**Valid Graft Trees:**

```
o---o---o ← log a

o---o---o ← log a
     \
      o---o---o ← log b

o---o---o ← log a   o---o  ← log d
    |    \         /
     \    o---o---o---o ← log b
      o---o ← log c
```

A Mark is a named, mutable reference to a LogRef. A Branch is one kind of Mark which tracks the tip of a log.

```
o---o---o  ← main
     \
      o---o---o  ← dev
```

```rust
struct Branch {
    /// The name of the Branch
    name: String,

    /// The tip of the Branch
    tip: LogRef,
}
```

A Remote is a named reference to remote storage, identified by a URI like `s3://bucket/path/in/bucket?region=us-east-1`.

A Snapshot is a consistent view of a Volume at a fixed point in time represented by a LogRef. When reading from a snapshot, Provenance may be used to canonicalize the read path.

The Provenance field on a Commit tracks the origin of the Commit. This is used to track equivalence relations between Commits in different logs. If Commit A1's Provenance is Commit B3, this means that the logical state of the Volume at B3 is identical to the state of the volume at A1. This allows Logs to branch off one another and then periodically merge back their changes without creating a DAG.

```
Three logs: { a, b, c }. b and c branch from a2.
b1.provenance = c3
a3.provenance = b1
and so on.

a1---a2----------------a3--------a4---a5---a6
      \               /         /
       \-------------b1--------b2
        \           /         /
         c1---c2---c3---c4---c5

Thus a Snapshot at c4 can form the canonical read path by following provenance relations.

a1---a2---a3---c4
```

Provenance can also be used to track Commit Images. In this example, the origin/main/images log (d) contains three checkpoints { d1, d2, d3 } which map to { a3, a5, a2 } respectively.

```
       d3                d1             d2 ← origin/main/images
      /                 /              /
a1---a2----------------a3--------a4---a5---a6 ← origin/main
      \               /         /
       \-------------b1--------b2 ← main/staged
        \           /         /
         c1---c2---c3---c4---c5 ← main
```

A Filter is an index of all of the PageIdxs modified by a sequential range of Commits in a Log. Filters may return false-positives, but never return false-negatives.

# Remote Storage Layout

```
/root
  /logs/{LogId}/{LSN} -> Commit
  /marks/{name} -> Mark
  /segments/{hash prefix}/{hash suffix} -> Serialized Segment
```

# Local Storage Layout

```
/root
  /logs/{LogId}/{LSN} -> Commit
  /marks/{name} -> Mark
  /segments/{hash prefix}/{hash suffix} -> Serialized Segment
  /filters/{LogId}/{start LSN}-{end LSN} -> Filter
```

# Type System

```rust

struct LSN(NonZero<u64>);
struct PageIdx(NonZero<u32>);
struct PageCount(u32);

struct Commit {
    /// The ID of the Log
    log: LogId,

    /// The Commit's LSN in the Log
    lsn: LSN,

    /// The timestamp of the Commit
    timestamp: Timestamp,

    /// If this Commit is branched from another Log, this field contains the Branch point. Only valid on the first Commit to a Log (when LSN == 1).
    branched_from: Option<LogRef>,

    /// The origin of this Commit if this Commit was constructed from another Log. Provenance guarantees that the state of the Volume represented by this commit is exactly the same as the state represented by the provenance commit.
    provenance: Option<LogRef>,

    /// True when this Commit contains all of the Pages in the Volume.
    /// Note: Volumes are sparse, so this property does not imply that the size
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

struct SegmentFrameIdx {
    /// The length of the compressed frame in bytes.
    frame_size: u64,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    last_pageidx: PageIdx,
}

enum MarkKind {
    /// A Branch mark is automatically updated to the tip of a Log as it receives commits.
    Branch,
    /// A Tag mark is a stable reference to a LogRef. It can only be moved explicitly via a force push.
    Tag,
}

struct Mark {
    /// The name of this mark
    name: String,
    /// The mark kind
    kind: MarkKind,
    /// The LogRef this mark points at to
    target: LogRef,
}
```

# Commit filtering

Periodically (by default, every 32 commits) we will write a Filter to the `filters` folder. A Filter is an index of all of the PageIdxs modified by a sequential range of Commits in a Log. Filters may return false-positives, but never return false-negatives.

We will support two kinds of filters:

1. a Splinter containing the union of all offsets in the chunk
2. a Binary Fuse Filter (`xorf::BinaryFuse8`) over all offsets in the chunk

We need two kinds of filters due to `BinaryFuse8`'s construction being fallible and the observation that in some cases a Splinter may be more optimal. We may add other filter formats in the future.

The filters will allow us to quickly skip over 32 commits at a time. Future optimizations may include dynamically varying the number of commits based on statistics or configuration.

# TODO

## How do we track branch sets?

Our write model involves 3 branches. There is the remote branch, the staging branch, and the local branch (names tbd).

Local branches are created as needed if one doesn't already exist. A pull that modifies the remote branch invalidates any active local branches.

Staging branches are created as needed when merging down the local branch in preparation for sync.

The remote branches are created when pushing a log to the remote.

The snapshot code needs to efficiently take a snapshot of a Volume, which must take into account any active local or staging branches. And then it needs to load any provenance pointers between those three branches.

It seems like the mark system may need to track this relationship. The relationship we are creating is "upwards" I think... in the sense that provenance points downwards, which requires landing in A before B while B points back at A.

So when user checks out branch main, we would ideally learn about any active staging or local branches for main when we read the main mark. Also same thing with image branches. This implies that mark relationships should also be pushable.

## How do we efficiently track provenance?

- an in-memory index would probably be ok?
- provenance is an optimization, not required for correctness
- probably need to load provenance from mark relationships and incrementally update them as the various logs change
