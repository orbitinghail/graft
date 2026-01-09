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

Periodically (by default, every 32 commits) we will write a filter to the `commit-filter` folder. A filter is an index of all of page offsets touched by any commit in the chunk range (start-end LSN). Filters never return false-positives, but may return false-negatives.

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
