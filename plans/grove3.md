# Definitions

A Grove (Graft Repository) is a tree of Logs and content-addressed Segments.

A Log is a monotonic sequence of Commits identified by Log Sequence Number (LSN). A Log either starts from empty or branches from another Log at a particular LSN.

A LSN is a strictly sequential monotonic sequence unsigned 64-bit integer starting at 1. The value 0 is a reserved sentinel.

A Commit represents a sparse change to a Volume.

A Volume is a sparse set of Pages ordered by PageIdx. A Volume has a VolumeName.

A VolumeName is the immutable identity of a Volume. It's a simple alphanumeric string made up of lowercase ASCII a-z, digits 0-9, separators `-` and `_` with a maximum length of 128 bytes. In addition VolumeNames must start with an alphanumeric character and not start with the reserved prefix "graft".

A PageCount represents the number of pages in a Volume.

A PageIdx is the index of a Page in a Volume. PageIdx is a 32-bit unsigned integer starting at 1. The value 0 is a reserved sentinel.

A Page is a fixed size array of bytes. All Pages in a Volume have the same size.

An Image Commit contains all pages that have been written to the Volume. Because Volumes may be sparse, an Image Commit’s Segment may contain fewer pages than the Volume’s logical page count. Any page not present in the Segment must be treated as empty by the reader.

All of the Logs in a Grove form a tree. A Log may branch off of another log at any point. The Branch point is represented by a `LogRef { LogId, LSN }` and stored in the first commit in a Log.

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

A Remote is a named reference to remote storage, identified by a URI like:
`s3://bucket/path/in/bucket?region=us-east-1`.

A Snapshot is a consistent view of a Volume at a fixed point in time represented by a LogRef. When reading from a snapshot, Provenance may be used to canonicalize the read path.

The Provenance field on a Commit tracks the origin of the Commit. This is used to track equivalence relations between Commits in different logs. If Commit A1's Provenance is Commit B3, this means that the logical state of the Volume at B3 is identical to the state of the volume at A1. This allows Logs to branch off one another and then periodically merge back their changes.

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

Thus a Snapshot at c4 can form the canonical read path by following provenance relations backwards.

a1---a2---a3---c4
```

Provenance can also be used to track Commit Images. In this example, the `origin/main/images` log (d) contains three checkpoints `{ d1, d2, d3 }` which map to `{ a3, a5, a2 }` respectively. When canonicalizing a read path, Snapshots will terminate at the closest reachable Commit Image (via provenance).

```
       d3                d1             d2 ← origin/main/images
      /                 /              /
a1---a2----------------a3--------a4---a5---a6 ← origin/main
      \               /         /
       \-------------b1--------b2 ← main/staged
        \           /         /
         c1---c2---c3---c4---c5 ← main
```

A snapshot at c4 will have the canonical read path: `d1--c4`

A Filter is an index of all of the PageIdxs modified by a sequential range of Commits in a Log. Filters may return false-positives, but never return false-negatives. Filters are purely a performance optimization, and are used to quickly determine if a page was modified by a subset of commits.

# Storage Layout

## Remote Storage Layout

```
/root
  /logs/{LogId}/{LSN Bucket}/{LSN} -> Commit
  /volumes/{name} -> VolumeManifest
  /segments/{hash prefix}/{hash suffix} -> Serialized Segment
```

## Local Storage Layout

```
/root
  /logs/{LogId}/{LSN Bucket}/{LSN} -> Commit
  /volumes/{name} -> VolumeManifest
  /segments/{hash prefix}/{hash suffix} -> Serialized Segment
  /filters/{LogId}/{start LSN}-{end LSN} -> Filter
```

## LSN Bucketing and Serialization

To ensure that we don't run into filesystem/remote storage listing limitations, we shard LSN entries into buckets. For now, buckets have a fixed size of 4096 commits per bucket. A bucket is serialized to a path as a BigEndian u64 encoded to hex (16 digits). A LSN's bucket is calculated as `(lsn - 1) / 4096`.

LSNs are serialized to a path as a BigEndian u64 encoded to hex (16 digits).

As an example, here are the commit paths for LSNs 1, 4096, 4097, and MAX in Log `74ggm5u4pH-49kJcJnhj2ZEu`:

/logs/74ggm5u4pH-49kJcJnhj2ZEu/0000000000000000/0000000000000001 // 1
/logs/74ggm5u4pH-49kJcJnhj2ZEu/0000000000000000/0000000000001000 // 4096
/logs/74ggm5u4pH-49kJcJnhj2ZEu/0000000000000001/0000000000001001 // 4097
/logs/74ggm5u4pH-49kJcJnhj2ZEu/000fffffffffffff/ffffffffffffffff // LSN(MAX)

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

struct LocalLogs {
    /// The staging Log contains commits which are ready to be pushed to the
    /// main Log. Each commit has Provenance to the write Log.
    staged: LogId,

    /// The write Log receives new commits for a Volume.
    write: LogId,
}

struct VolumeManifest {
    /// The name of this Volume
    name: String,

    /// The main Log backing this Volume. This is the Volume's source of truth.
    main: LogId,

    /// A set of zero or more Image Logs. These Logs contain commits with Provenance to main.
    images: Set<LogId>,

    /// Only used on writers nodes and never pushed to the remote manifest. This
    /// field tracks the Logs used to write to the main Log and stage commits
    /// for pushing to the main Log. Both logs are branched from main.
    local: Option<LocalLogs>,
}
```

# Commit filtering

Periodically (by default, every 32 commits) we will write a Filter to the `filters` folder. A Filter is an index of all of the PageIdxs modified by a sequential range of Commits in a Log. Filters may return false-positives, but never return false-negatives.

We will support two kinds of filters:

1. a Splinter containing the union of all offsets in the chunk
2. a Binary Fuse Filter (`xorf::BinaryFuse8`) over all offsets in the chunk

We need two kinds of filters due to `BinaryFuse8`'s construction being fallible and the observation that in some cases a Splinter may be more optimal. We may add other filter formats in the future.

The filters will allow us to quickly skip over 32 commits at a time. Future optimizations may include dynamically varying the number of commits based on statistics or configuration.

# Safety Invariants

## Conditional Manifest Writes

Every write to `/volumes/{name}` must be conditional on the version that was read (for example ETag, generation token, or filesystem lock token). Unconditional overwrites are invalid.

## Deterministic Manifest Merge

When concurrent updates are merged, `images` is merged by set union (`remote ∪ local`) and serialized in deterministic order (sorted by `LogId`) so retries are stable and idempotent.

## Canonical Volume Name Encoding

Volume names are canonicalized to lowercase before persistence and must start and end with an alphanumeric character (`[a-z0-9]`). This prevents ambiguous keys across filesystems/object stores.

## Atomic Log Append

All commits to a Log must be written via atomic exclusive create at `LSN(tip + 1)`. A write to any existing LSN or any non-tip LSN is invalid and must fail with conflict.

## Shared Main Log Is Allowed

Multiple Volume manifests may reference the same main `LogId` by design. Correctness relies on atomic append semantics on the underlying log store, not exclusive ownership of main.

# TODO

## How do we efficiently track provenance?

- an in-memory index would probably be ok?
- provenance is an optimization, not required for correctness
- for every named volume, we will need to load all of the relevant logs and index provenance relationships between commits. Each provenance relationship is an edge in a DAG.

### DAG index sketch (per Volume)

Build and maintain an in-memory DAG index per opened Volume. The index is append-friendly and can be incrementally updated as manifests/log tips change.

```rust
struct CommitKey {
    log: LogId,
    lsn: LSN,
}

struct DagNode {
    key: CommitKey,
    // Commit's parent in the same log (lsn - 1), if present.
    parent: Option<CommitKey>,
    // Optional equivalence edge to another log.
    provenance: Option<CommitKey>,
    is_image: bool,
}

struct VolumeDagIndex {
    // All indexed commits by identity.
    nodes: HashMap<CommitKey, DagNode>,

    // Reverse edges for traversal and invalidation.
    children_by_parent: HashMap<CommitKey, SmallVec<[CommitKey; 2]>>,
    children_by_provenance: HashMap<CommitKey, SmallVec<[CommitKey; 2]>>,

    // Fast tip tracking for active logs in this Volume.
    tip_by_log: HashMap<LogId, LSN>,

    // Image candidates reachable by canonical walk.
    image_commits: HashSet<CommitKey>,

    // Optional memoization of canonical predecessor:
    // "from this commit, next hop on canonical path is X".
    canonical_next: HashMap<CommitKey, CommitKey>,
}
```

### Incremental update strategy

1. Read `VolumeManifest`.
2. For each referenced log (`main`, `images`, and local logs if present), pull unseen commits `(last_indexed_lsn+1..=tip)`.
3. Insert new `DagNode`s and update reverse indexes.
4. If a commit has `provenance`, add edge `commit -> provenance`.
5. Invalidate `canonical_next` entries for affected descendants (via reverse maps), then lazily recompute on demand.

### Core queries

1. **Canonical read path from snapshot `S`**
   - Walk backwards from `S` choosing:
     - provenance hop when present (equivalence shortcut), otherwise
     - in-log parent hop.
   - Stop at nearest reachable `is_image` commit (or log start).
   - Memoize hops in `canonical_next`.

2. **Closest reachable image commit from `S`**
   - Same traversal as above, first node with `is_image == true` wins.
   - Tie-breaker when multiple candidates are discovered at same depth:
     lexicographically smallest `(log, lsn)` for deterministic behavior.

3. **Is commit `A` equivalent to commit `B`**
   - Canonicalize both to their reduced path heads (or full canonical chains hash).
   - Equal canonical head/hash implies equivalent logical state.

### Persistence and rebuild

- Index is a cache: correctness must not depend on it.
- On restart, rebuild from manifest + logs; optionally persist checkpoints
  (`tip_by_log`, compacted `canonical_next`) to reduce warmup time.
