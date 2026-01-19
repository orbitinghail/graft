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
    /wal0.wal
    /wal1.wal
```

### Commit filtering

Periodically (by default, every 32 commits) we will write a filter to the `commit-filter` folder. A filter is an index of all of page offsets touched by any commit in the chunk range (start-end LSN). Filters may return false-positives, but never return false-negatives.

We will support two kinds of filters:

1. a Splinter containing the union of all offsets in the chunk
2. a Binary Fuse Filter (`xorf::BinaryFuse8`) over all offsets in the chunk

We need two kinds of filters due to `BinaryFuse8`'s construction being fallible and the observation that in some cases a Splinter may be more optimal. We may add other filter formats in the future.

The filters will allow us to quickly skip over 32 commits at a time. Future optimizations may include dynamically varying the number of commits based on statistics or configuration.

### Segments

Segments are stored as sparse files on compatible filesystems. On non-compatible filesystems they are regular files. In both cases they are pre-truncated to the size of the Segment.

Segments are encoded as a header followed by a series of frames. The header stores a presence byte per chunk. Segments are mmapped into shared memory. Each frame is a zstd frame containing it's own checksum. If the frame fails to decompress then the client simply refetches it.

Optionally, we may use flush or async flush (mmap2 terms) to ensure (or hasten) durability after writing out frames. This can be easily tuned by the user based on their workload and does not affect correctness (as long as they still have network access).

## The WAL

The goal is to buffer local commits. This needs to maintain local serializability across local writers, readers, and multiple processes. The WAL is ultimately rolled up into remote commits.

The WAL has a similar design to the [WAL2 prototype in SQLite][wal2]. There are two WAL files and a sparse checkpoint. Pages are appended to the active WAL file, and a checkpoint process periodically writes the idle WAL into the sparse checkpoint file.

[wal2]: https://sqlite.org/src/doc/wal2/doc/wal2.md

The two WAL files are managed by seven locks in shared memory:

WRITE
CHECKPOINT
RECOVERY
NEED_WAL0_PART
NEED_WAL0_PART_WAL1_FULL
NEED_WAL1_PART
NEED_WAL1_PART_WAL0_FULL

The shared memory region also contains the following wal-fields in an atomic region called the WALState. This region is double buffered

- walIdx (u8): which wal (0 or 1) is active
- checkpointed (u8): whether or not the inactive wal has been checkpointed
- lastEntry0: the last valid commit's entry number in wal0
- lastEntry1: the last valid commit's entry number in wal1
- salt: the current salt, set to the last entry's checksum when the active wal is switched.
- checksum: checksum of above fields

Each WAL file begins with a header:

- Magic number
- File format version
- page size
- checkpoint sequence number (even=wal0, odd=wal1)
- salt (previous wal's final checksum)
- header checksum

Each WAL file is composed of entries made up of a header followed by the page data.

- Page Index
- Page Count: For commit entries the size of the Volume in pages, for all other entries, zero.
- The current salt
- A cumulative checksum containing all non-checksum data in the WAL up to and including this Entry

## Algorithms

### Volume Snapshot

In order to read from a Volume, a Client must first take a consistent snapshot on both the Log and the WAL (if one exists). This involves the following steps:

1. Load the Logs manifest to discover it's checkpoints and potential parent Log.
2. Recursively load parent Logs until a checkpoint is discovered, this checkpoint serves as the low-watermark for the Snapshot.
3. Build a snapshot containing the LSN ranges of each discovered Log.
4. Take a read snapshot of the WAL
   a. acquire shared RECOVERY lock
   b. read WAL state from control (double buffered + checksum)
   c. acquire shared lock: PART on active WAL, full on inactive WAL unless checkpointed
   d. validate snapshot consistency: memory barrier + validate that the WALState has not changed since acquiring the shared lock

### Writing

Writers start by acquiring a snapshot.

When they want to start writing they take the writer lock.

**Page Buffering Strategy**: Buffer all dirty pages in memory until commit time. This approach is chosen because:

1. It simplifies recovery - entries only become visible after their commit entry is durable
2. It avoids partial transaction state in the WAL
3. It aligns naturally with the entry format where commit entries are distinguished by non-zero PageCount
4. Writers already hold the WRITE lock for the duration of their transaction, so we're not blocking others by buffering

At commit time, all buffered pages are written to the WAL as a contiguous batch followed by a commit entry. The commit entry serves as a "trailer" that atomically commits all preceding entries in this batch.

**Write Process**:

1. Acquire the WRITE lock (blocks other writers)
2. Buffer dirty pages in memory as the transaction proceeds
3. Track the entry index where this transaction will begin writing

**Commit Process**:

1. Append all buffered pages to the active WAL as entries (PageCount=0 for each):
   - Write entry header: Page Index, PageCount=0, Salt, Checksum
   - Write page data
   - Update the page index in SHM with two atomic writes:
     - entry index -> page index
     - page hash -> entry index

2. Append a commit entry (PageCount = volume size in pages):
   - Write entry header: Page Index of last written page (or 0 if no pages), PageCount=volume_size, Salt, Checksum
   - No page data follows the commit entry

3. fsync the WAL file

4. Update WALState in SHM:
   - Compute new lastEntry for the active WAL (lastEntry0 or lastEntry1 based on walIdx)
   - Update the appropriate lastEntry field atomically
   - Update the WALState checksum
   - Write to the inactive buffer slot, then flip the active buffer indicator

5. Consider switching to the inactive WAL:
   - If active WAL exceeds size threshold AND inactive WAL is checkpointed:
     a. Set the new salt to the final checksum of the current active WAL
     b. Flip walIdx to the other WAL
     c. Reset the new active WAL's lastEntry to 0
     d. Clear the checkpointed flag
     e. Update WALState checksum and flip the buffer
     f. Truncate and reinitialize the new active WAL file with fresh header

6. Release the WRITE lock

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
