# Grove: A forest of logs

A `Graft Volume` is represented by a `Log`. A Log is identified by a `LogId`. A log is composed of a series of monotonic, immutable commits identified by `LSN`. A `LogRef` is the tuple `(LogId, LSN)` and always represents a consistent & fully durable Volume state.

Clients modify Volumes by atomically pushing a new Commit to a Log. If the atomic append operation fails due to racing with another Client, the Client must resolve the delta and retry the commit process.

Clients must be able to buffer up many changes locally before committing, to ensure that their write path is not coupled to remote commit latency & throughput. They do this in a per-log WAL.

A Log may be branched from a parent `LogRef`. This means that until the Log is checkpointed, all reads will need to flow through to the parent.

## Remote Storage

Remote storage mostly lines up with what we already have. Each Log is stored as a series of separate commit objects at `/logs/{LogId}/commits/{LSN}`. Pages are stored separately in `Segments` which are internally chunked to allow Clients to read only the subset they need. Segments are stored at `/segments/{SegmentId}`.

### Remote Refs

Currently Volumes are referenced by local-only mutable tags. For Graft to be more git-like, the remote needs to store named references.

To do this we will push tags to the remote. They will be stored at `/tags/{name}` and contain only a `LogId`. Clients may force push tags to the remote if desired.

Tags do not participate in consistency or durability. They may be used by remote GC to eliminate un-referenced Logs.

### Remote Checkpoints

Currently checkpoints involve rewriting a Commit to include all of the pages as of it's LSN, and then appending a new commit recording the existence of the checkpoint to the log. This has two downsides:

1. increases write contention on the Log
2. makes it difficult for logs to branch from one another, as branched logs have no way of easily finding recent checkpoints of their parents.

To solve this, we need to record checkpoints in a singleton manifest per log. This manifest will keep track of all checkpoints for the log as well as the parent LogRef if one exists. The manifest will be stored at `/logs/{LogId}/manifest` and is atomically updated by CAS operations.

In addition, rather than rewriting a commit, checkpoint commits will simply be output to a separate path: `/logs/{LogId}/checkpoints/{LSN}`.

GC will be a separate operation from checkpointing, but will be dependent on checkpointing to provide valid minimum GC points in the tree of Logs. GC will scan logs, looking for checkpoints that satisfy GC criteria, truncating the prefix of any matching log (up to the satisfying checkpoint).

GC requires a lot of future work, but this architecture allows for a very flexible GC to be built, which is able to complete its work as a series of gated phases. The mark phase can add watermarks to the manifests, allowing for a sweep phase to happen later.

The per-log manifest will need to be periodically and recursively pulled by Clients until they find the next valid checkpoint.

The per-log manifest will be optimistically updated by Clients when they write out checkpoints. If the optimistic write fails (i.e. the client crashes between writing a checkpoint commit and updating the manifest), a background checkpoint process will eventually discover the discrepancy and update the manifest. As the manifest gates GC, this is safe to do in an async fashion.

## Local Storage

Each Log is stored as a directory of commits on the filesystem. At the top of the Log directory tree is the cached manifest and a control file which is mapped into shared memory by all processes which have opened the same Log.

Commits are only written to the Log after they have been stored remotely, hence the Log can always be deleted and refetched from the remote. The Commit log may be partial, allowing the Fetch and Pull operations to only pull down to the latest checkpoint. As commits are immutable, multiple writers can race to update them, as long as all writes are atomic.

### Commit filtering

Periodically (by default, every 32 commits) we will write a filter to the `commit-filter` folder. A filter is an index of all of page offsets touched by any commit in the chunk range (start-end LSN). Filters may return false-positives, but never return false-negatives.

We will support two kinds of filters:

1. a Splinter containing the union of all offsets in the chunk
2. a Binary Fuse Filter (`xorf::BinaryFuse8`) over all offsets in the chunk

We need two kinds of filters due to `BinaryFuse8`'s construction being fallible and the observation that in some cases a Splinter may be more optimal. We may add other filter formats in the future.

The filters will allow us to quickly skip over 32 commits at a time. Future optimizations may include dynamically varying the number of commits based on statistics or configuration.

### Segments

Segments are stored as sparse files on compatible filesystems. On non-compatible filesystems they are regular files. In both cases they are pre-truncated to the size of the Segment.

Segments are encoded as a header followed by a series of chunks. The header stores a presence byte per chunk. Segments are mmapped into shared memory. Each chunk is a zstd frame containing it's own checksum. If the chunk fails to decompress then the client simply refetches the chunk.

Optionally, we may use flush or async flush (mmap2 terms) to ensure (or hasten) durability after writing out chunks. This can be easily tuned by the user based on their workload and does not affect correctness (as long as they still have network access).

### Layout

```
/{LogId}
  /manifest -> cached remote manifest
  /control -> shared memory coordination
  /commits
    /{LSN} -> serialized commit
  /commit-filter
    /{start}-{end} -> offsets filter for commits in LSN range start..=end
  /checkpoints
    /{LSN} -> serialized checkpoint commit
  /segments
    /{SegmentId} -> serialized segment
  /wal
    /{epoch}.wal
```

## The WAL

> This design is heavily based on the SQLite WAL.
> reference 1: https://sqlite.org/walformat.html
> reference 2: https://sqlite.org/fileformat2.html#the_write_ahead_log

Local writes are buffered in a chunked `WAL` stored at `/{LogId}/wal/{epoch}.wal`. Each WAL file contains a WAL header followed by an append only sequence of `Entries`, each composed of a header and Page.

<<<< WIP POINT >>>>
Notes:

- need to look at how sqlite's wal2 design works, I assume it uses shared memory to coordinate which wal is the right one
- with epochs rather than checkpoints, I think my design can be simpler... I just need to know the latest epoch and I should be good. read slots will need to track both the epoch and frame index

<<<< WIP POINT >>>>

The WAL header stores:

- Magic number
- File format version
-

The Entry header stores:

- The Page Index
- For commit entries the size of the Volume in pages, for all other entries, zero.
- The current WAL salts
- A cumulative checksum containing all non-checksum data in the WAL up to and including this Entry

1. `{LogId}.wal`: A series of WAL `Entries`, each containing a single `Page`. The Page is prepended with a Entry header which stores checksums, salts, and the length of the volume (in pages) for commit Entries. The Entry header may also support extensions like page compression.

2. `{LogId}.shm`: Only used as a shared memory point, never fsynced. Based on the `wal-index` from SQLite, this file contains a header followed by an index enabling fast lookups of the latest page to use for a given upper bound WAL position. The header contains read slots for readers, and write slots for the writer and checkpointer process.

3. `{LogId}.map`: A sparse map of pages that are in the buffer. The writers periodically rollup the WAL into this map to keep the WAL from growing too large. On filesystems that support sparse files, the map is fallocated to the Volume size

### Volume Snapshot

In order to read from a Volume, a Client must first take a consistent snapshot on both the Log and the WAL (if one exists). This involves the following steps:

1. Retrieve the

### Write process

Writers and Readers all start the same

## Algorithms

### Pushing

To Push a Commit to a Log, the Client takes the following steps:

1. take the remote-operation lock for the log
2. capture the push watermark for the WAL. this is the last entry in the WAL that has already been successfully pushed to the remote.
3. open a Volume snapshot
4. verify that there are commits in the WAL between the push watermark and the snapshot
5. build and upload a segment
6. atomically write a new commit to the remote
   -> how do we track the partial commit state to do recovery later
   -> idea is to store client provenance (client id, new watermark) in the commit
   -> in this case, we may need to construct the watermark out of a wal epoch + entry index. or just make the entry index a monotonic u64 if we are worried about 4 billion entries being too little.

On success:

1. write the commit to the log cache
2. optimistically update the manifest if this commit is a checkpoint
3. release the remote-operation lock

On conflict:

1. drop the pending commit
2. attempt recovery from the remote
   -> the conflict may be with ourself in certain edge cases
   -> if recovery succeeds, we can attempt the success branch
   -> otherwise report the volume diverged error

On other failure:

1. drop the pending commit
2. raise error

### Pulling

1. take the remote-operation lock for the Log
2. fetch the Log manifest if missing
3. if the log has a parent recursively pull the parent up to the branch LogRef of the Parent.
4.

# Comments

The following comments are from reviewing this design against the current Graft codebase. They highlight areas that may need clarification, potential issues, and integration considerations.

## Epoch Semantics Need Definition

The WAL section mentions "epochs rather than checkpoints" but doesn't define:

- What triggers an epoch transition? Is it tied to remote commits? Client sessions? Time?
- How does a reader discover the current epoch?
- Can multiple epochs be live simultaneously (e.g., during a push)?
- What's the relationship between epoch and the push watermark?

In SQLite, the WAL checkpoint moves pages back to the main database file. In Grove, if epochs are tied to remote commits, then an epoch transition would mean: "all entries in the old epoch have been durably committed to remote, so the old epoch can be discarded." But this needs explicit specification.

## Volume Snapshot Algorithm Incomplete

The section "In order to read from a Volume, a Client must first take a consistent snapshot on both the Log and the WAL" cuts off at "1. Retrieve the". This is a critical algorithm for correctness.

Key questions that need answering:

- How do you atomically capture both log position and WAL position?
- What ordering guarantees exist between log and WAL reads?
- How do readers coordinate with the writer (who may be mid-commit)?
- What's in the "read slots" mentioned in the `.shm` file?

The current architecture uses Fjall's snapshot mechanism for point-in-time consistency. Grove needs an equivalent.

## Push Watermark Storage Location

The pushing algorithm mentions "capture the push watermark for the WAL" but doesn't specify where this watermark is stored. The current architecture stores this in `Volume.sync.local_watermark` persisted in Fjall.

Options for Grove:

1. In the control/shm file (volatile, needs recovery protocol)
2. In the manifest (remote, requires fetch on startup)
3. In a separate local file (needs crash-safety)
4. Derived from comparing WAL entries against remote log (expensive on startup)

The choice affects both normal operation and crash recovery.

## Two-Log Model vs Single-Log Model

The current architecture maintains two LogIds per Volume: `local` and `remote`. The local log accumulates commits that are periodically pushed to create commits in the remote log. This allows local and remote commits to have independent LSN sequences.

Grove appears to collapse this into a single Log with WAL buffering. This is a significant architectural change that affects:

- How `Snapshot` works (currently can span multiple logs for branch scenarios)
- The `Volume` struct (currently stores both local and remote LogId)
- The sync point tracking (currently tracks alignment between two logs)

Need to clarify: Is the WAL a replacement for the local log concept? If so, how do WAL entries map to remote LSNs?

## Recovery Mechanism for Partial Commits

The current architecture has explicit `PendingCommit` state that tracks in-flight remote commits, enabling recovery after crashes. Grove mentions "store client provenance (client id, new watermark) in the commit" as an idea.

Considerations:

- If provenance is stored in the remote commit, how does the client discover its own commits after restart?
- The current architecture checks `commit_hash` to verify identity. Will Grove continue using CommitHash?
- What happens if a client crashes between segment upload and commit write? The segment is orphaned.
- What if the client crashes between commit write and local watermark update?

The current `recover_pending_commit` logic in `fjall_storage.rs:518-545` shows the complexity here.

## Shared Memory Control File Needs Specification

The control file is mapped into shared memory but needs more detail:

- What's the exact format? (Header + read slots + write slots + ?)
- What locking protocol? (POSIX advisory locks? Futex? Custom spinlocks?)
- How are stale readers detected and evicted?
- What happens if a process crashes while holding a lock?
- Is this portable across platforms? (Windows has different shared memory semantics)

SQLite's wal-index uses a specific format with hash tables and frame pointers. Grove's requirements may be simpler but need specification.

## Segment Sparse File Portability

The document mentions "sparse files on compatible filesystems" with regular files as fallback. Considerations:

- How do you detect sparse file support at runtime? (Create test file and check?)
- Windows NTFS supports sparse files but with different APIs
- Some network filesystems (NFS, SMB) have varying sparse file support
- Docker overlay filesystems may not support sparse files efficiently
- How much space is wasted if sparse files aren't supported? (Full segment size)

The mmap + sparse file approach is elegant but needs careful fallback handling.

## Pull Recursion for Branched Logs

"if the log has a parent recursively pull the parent up to the branch LogRef of the Parent."

For deeply nested branches, this could be expensive:

- Is there a recursion depth limit to prevent stack overflow?
- Is there caching to avoid re-pulling the same parent commits?
- Can you do lazy pulling (only pull parent chunks when needed for a page read)?
- What if the parent log is huge but you only need a small branch?

Consider an iterative approach or explicit branch-chain caching.

## Tag Race Conditions

"Clients may force push tags to the remote if desired."

Tags are mutable references which introduces race conditions:

- What if two clients race to update the same tag?
- Is there any optimistic concurrency (CAS on tag version)?
- Can you get a history of tag values? (For debugging/auditing)
- What happens to clients observing the old tag value? (Stale LogId)

The current architecture only has local tags, so this is new complexity.

## GC Safety with Active Readers

GC "truncates the prefix of any matching log (up to the satisfying checkpoint)" but:

- How do you prevent GC from racing with active readers?
- What if a client has an open snapshot referencing soon-to-be-GC'd commits?
- For branched logs, can you GC a parent while children are reading?
- Is there a "GC lease" or "read lease" mechanism?

The manifest watermarks are mentioned for gated phases, but the read-side coordination needs specification.

## Offline Operation

"Commits are only written to the Log after stored remotely" implies the local commit cache is just that—a cache. But:

- Can you continue working with just the WAL during network partition?
- How large can the WAL grow before it becomes problematic?
- Is there a "offline mode" that allows local-only commits?
- What happens if you're offline for days and accumulate thousands of WAL entries?

The current architecture allows the local log to grow independently of the remote. This flexibility may be important for some use cases.

## Manifest Format and Concurrent Updates

The manifest is atomically updated via CAS, but needs specification:

- What's the serialization format? (Protobuf like other structures?)
- What fields exactly? (checkpoints list, parent LogRef, GC watermarks, ?)
- Maximum size constraints?
- What if CAS fails repeatedly due to contention?

The "optimistic update after checkpoint write" pattern means transient inconsistency is expected. The background reconciliation process needs more detail.

## Integration with Existing Core Types

Good news: Many core types can be reused:

- `LSN`, `LogId`, `VolumeId`, `SegmentId`, `PageIdx` - unchanged
- `Commit`, `SegmentIdx`, `SegmentFrameIdx` - unchanged
- `CommitHash` - unchanged
- `Page`, `PageSet` - unchanged
- `Snapshot` - may need changes to understand WAL

Types that need changes:

- `Volume` - loses the local/remote log split?
- `SyncPoint` - replaced by push watermark in control file?
- `PendingCommit` - replaced by provenance in commits?

The `FjallStorage` trait boundary is clean enough that Grove can implement a compatible interface, but the `Volume` struct changes may propagate widely.

## Migration Path from Fjall

How do existing Fjall-based deployments migrate to Grove?

- Is there a data migration tool?
- Can you run both side-by-side during transition?
- Is there version detection in storage format?
- Can you rollback if Grove has issues?

This is future work but worth considering during design.

## Commit Filter Chunk Boundaries

Filters are written "every 32 commits" but:

- What if there are fewer than 32 commits in the log?
- Are filters aligned to fixed LSN ranges, or sliding windows?
- When scanning, how do you know which filter files exist?
- Can filters become stale if commits are GC'd?

Consider: filters at `/{start}-{end}` with end being exclusive might be cleaner for boundary handling.

## WAL Rollup to Map

"The writers periodically rollup the WAL into this map to keep the WAL from growing too large."

Questions:

- What triggers rollup? (Size threshold? Entry count? Time?)
- Can rollup happen during active reads?
- Is the map file durable or reconstructible from WAL?
- What's the atomicity guarantee during rollup?
- How does this interact with epochs?

This is effectively a compaction process and needs careful specification.

## Entry Header Compression Extensions

"The Entry header may also support extensions like page compression."

If pages can be compressed in the WAL:

- Is there a per-entry flag indicating compression?
- What compression algorithm? (zstd like segments?)
- How does this affect the cumulative checksum calculation?
- Can compressed and uncompressed entries coexist?

## Salt Purpose and Rotation

"The current WAL salts" are stored in entry headers but:

- What is the purpose of salts? (Detect file corruption? Prevent replay?)
- How are salts generated and rotated?
- What happens if salts don't match during read?
- How do salts interact with epochs?

SQLite uses salts for WAL file identification (to detect when a WAL belongs to a different database). Clarify the purpose here.

## Remote Operation Lock Scope

Both push and pull "take the remote-operation lock for the log" but:

- Is this a process-local lock or cross-process (via control file)?
- Can reads proceed while the lock is held?
- What's the lock timeout/deadlock prevention?
- Can push and pull be concurrent on different logs?

The current architecture uses a mutex in `FjallStorage` for read-write transactions. Grove's scope may be different.

## Segment Checksum Verification Timing

"Each chunk is a zstd frame containing its own checksum. If the chunk fails to decompress then the client simply refetches the chunk."

But:

- Decompression is expensive—can you verify checksum first?
- What if the remote also has corruption?
- Is there an outer checksum on the segment header?
- How do you prevent infinite refetch loops on persistent corruption?

## Branch Creation and Merge Semantics

Branching is mentioned ("A Log may be branched from a parent LogRef") but needs more detail:

- How do you create a branch? (API surface)
- Can you merge branches? (Or is it one-way only?)
- What happens if parent and child both advance? (Divergence handling)
- Can a log have multiple children? (Presumably yes)
- Can a branch be re-parented?

## Summary

Grove is an ambitious redesign that addresses real limitations in the current architecture:

- Better GC support through manifests
- Explicit WAL for write buffering
- Log branching for multi-client scenarios
- More filesystem-native local storage

The core ideas are sound. The main gaps are in the detailed algorithms for:

1. Snapshot consistency across log and WAL
2. Epoch lifecycle and transitions
3. Recovery from partial commit states
4. Cross-process coordination via control file

I'd suggest completing the WAL section first, as it's the foundation for everything else. The SQLite references are a good starting point, but Grove's epoch-based model (vs checkpoint-based) needs its own specification.
