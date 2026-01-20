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

A Snapshot is a consistent view of a Volume at a fixed point in time represented by a LogRef. When reading from a snapshot, Provenance may be used to canonicalize the read path by following Provenance links in reverse (i.e. Commit A.provence = B; a read may read from A instead of B, then proceed back through A's log).

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

Provenance can also be used to track Commit Images. In this example, the origin/main/images log (d) contains three checkpoints { d1, d2, d3 } which map to { a3, a5, a2 } respectively. A Log composed of Commit Images may be out of order, as an Image always terminates a read operation.

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

struct Commit {
    /// The ID of the Log
    log: LogId,

    /// The Commit's LSN in the Log
    lsn: LSN,

    /// The timestamp of the Commit
    timestamp: Timestamp,

    /// If this Commit is branched from another Log, this field contains the Branch point { LogId, LSN }. Only valid on the first Commit to a Log (when LSN == 1).
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

We will support two kinds of filters to start:

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

Some more ideas. What if each Mark has a role field. And we enforce uniqueness across { name, role }. Role could be: { Local, Staged, Published, Image }. One thing I dislike about this is it makes Branches less flexible. For example, it would be possible for an Image branch to contain images for many other Logs.

Another way to think about this is by scanning all of the Commits and Branches, building up a full graph. This feels fuzzy, but perhaps flexible in a good way.

For now we could just add roles to the name, similar to what Git does by organizing refs into categories (origin, tags, heads, etc). Notably Git does not enforce what the category names actually are. They are just strings.

While I'm here. What are some better names for the main 4 categories of Marks in Graft? Let's talk about each one

One mark tracks the latest position in a Log that is owned by a remote. Essentially this means: it is always updated when the remote changes and we learn about the change. If we support multiple Remotes, this implies that a Log is owned by exactly one remote. Good names for this category of Marks is: Remote, Published, Upstream.

Then there is a pair of marks which represent the current write target for a branch and the staging area for squashing commits. I've been calling these Marks: Local and Staging, respectively. I'm partial to Staging, but not super happy with Local. Perhaps names that evoke a sense of "raw" -> "refined" before eventually flowing to Published would be better.

Finally there is a Mark that tracks Images. Notably, anyone who writes to such a mark would also need at least a Staging mark to prepare Images before uploading them to the remote.

We may also have Tag Marks, representing fixed (notable) points in time. Similar to Git tags.

Something to consider is how multiple processes might update Marks. We've talked about having a local double buffered WAL to accelerate Writes. Thus the WAL would receive the first level of writes. And then one of those processes would checkpoint the WAL into the current Local branch. Thus we can use shared memory to handle knowing which branch is local and generally coordinating the checkpoint/fetch/push/pull processes.

I have a vague idea that Marks should be able to point at other Marks, forming a layered relationship between them. And perhaps they simply need to be marked as mutable or immutable.

Hence:

```rust
struct Mark {
    /// The name of this mark
    name: String,
    /// The mark kind
    kind: MarkKind,
    /// The LogRef this mark points at to
    target: LogRef,
    /// The Parent of this Mark
    parent: LogId,
}
```

This feels a bit awkward because of:

1. Logs also have a branched_from pointer on their initial commit. The branched_from pointer might differe from the Parent pointer.
2. It's not really a parent. It's more of a remote target. Hmmm perhaps that's the model?

It also only makes sense for branches, not tags. So what if we do something like this:

```rust
enum MarkKind {
    Branch { upstream: LogId },
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

## Should Marks track LogRefs or just LogIds?

The main downside of Marks tracking LogIds rather than LogRefs is that a force push (that changes the log a mark points at) can clobber a concurrent write to the log.

Let's say we have two clients A and B, mark M pointing at log L.

A pushes a new log L' to the remote.
B pushes a new commit to L.
A force pushes L' to M

The question is whether or not this behavior is reasonable. In terms of strict serializability it's not, as the write { M: L' } did not observe the commit pushed to L.

However, in terms of reasonable intention, is there a situation where B would have made a different decision, knowing about the commit in L?

The datastructures are still entirely correct, and no data is lost. If a user wants to revert the situation they can simply force push the mark again.

Now let's talk about read and write path overhead.

In the read path, whether or not we store the LogRef we need to check to see if the Mark has changed. If it has changed we need to pull the new log accordingly. An advantage of the LogRef approach is that we could use that as the explicit "pull this ref". Hence the read path simply pulls the mark, and then pulls the LogRef - being able to always fast foward in parallel. With the other approach, we need to read a small batch of LogRefs in parallel, trading off get request cost for fetch latency.

In the write path, we save an additional put request, unless we are force pushing the Mark. The write path only needs to do a get request to determine if the Mark has changed after updating the Log. If we instead use LogRefs, the write path has to make at least three put requests. The first hitting the segment, the second the log, and the last the mark.

## How do we efficiently track provenance?

- an in-memory index would probably be ok?
- provenance is an optimization, not required for correctness
- probably need to load provenance from mark relationships and incrementally update them as the various logs change

## How do we validate correctness?

The goal is to be able to validate that the state of the Volume is correct. One idea is building an incremental Checksum that is stored in each Commit. Doing so involves reading all of the previous page versions for each page in the commit, to remove their contribution to the Checksum.

The issue with an incremental Checksum is truncation and sparsity. Since Volumes can be sparse, we don't know which pages actually exist in any range. Furthermore, a truncate operation can eliminate millions of pages in a single moment, without guaranteeing that the client doing so even has read any of the pages it may or may not be eliminating. Thus, when Truncating a volume to a smaller size, we would need to fetch every page above the new max page size of the Volume.

Perhaps this is acceptable. But it feels like a performance pitfall that may be problematic.

Another issue with incremental Checksums is the propagation of corruption. It's difficult to know which commit caused corruption in the Checksum without scanning every Commit.

So, if we don't use full Volume checksums, perhaps it's sufficient to detect corruption in a Commit in isolation.

Currently SegmentIds are just GID's. If we upgrade them to hashes we could use them to detect corruption. However we'd ideally like to detect corruption when downloading individual frames. So we also plan on having checksums at the frame level.

The current zstd encoding includes a Checksum at the end of each frame. It feels a bit dangerous to depend on this and not put it in our explicit encoding.

The simplest solution is to store a Checksum of each Segment Frame in the SegmentIdx stored in the Commit. The downside of this is that Segments require an associated SegmentIdx to decode.

It would be nice to be able to inspect a Segment directly and pull out useful information. At the very least, know which frames exist and whether or not they are valid.

I'm loosly thinking the Segment encoding looks like:

```rust
struct Segment {
    frames: [SegmentFrame],
    presence: [u8; 4],
    magic: u32,
}

enum SegmentEncoding {
    ZStd,
}

struct SegmentFrame {
    data: [u8],
    encoding: ZStd,
    checksum: u128,
    length: u32,
}
```

This encoding can be streamed, as each Frame is suffixed with a fixed size trailer (encoding, length).

A presense bitmap is appended to the file along with some magic bytes for file identification. The presense bitmap stores the number of frames which are present in the Segment.

A checksum is stored after each frame for content verification.

When a Segment Frame is downloaded, we check if a segment file already exists. If it does we mmap it, then write out the new Frame directly into the file. While writing the frame we build a checksum which is validated against the checksum in the frame trailer. Once this is done we issue a memory fence, and then update the presence bitmap using a single atomic write.
