# Graft

Transactional blob storage engine supporting lazy partial replication to the edge. Optimized for scale and cost over latency. Leverages object storage for durability.

# Glossary

- **GID**
  A 128 bit Graft Identifier. See [GIDs](#gids) for details.

- **Volume**
  A sparse data object consisting of Pages located at PageIdxs starting from 1. Volumes are referred to primarily by a Volume ID.

- **Volume ID**
  A 16 byte GID used to uniquely identify a Volume.

- **Page**
  A fixed-length block of storage. The default size is 4KiB (4096 bytes).

- **PageIdx**
  The index of a page within a volume. The first page of a volume has a page index of 1.

- **Graft**
  A set of PageIdxs corresponding to a single Volume. Used to track which PageIdxs are contained in a Segment or which PageIdxs have changed between two Snapshots.

- **PageCount**
  The number of logical pages in a Volume. This does not take into account sparseness. This means that if a page is written to PageIdx(1000) in an empty Volume, the Volume's size will immediately jump to 1000 pages.

- **LSN** (Log Sequence Number)
  A sequentially increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume. The commit process ensures that the sequence of LSNs never has gaps and is monotonic.

- **Snapshot**
  A tuple (volume id, lsn, PageCount) that defines a fixed point in time for the state of a volume.

- **Metastore**
  A service which stores Volume metadata including the log of segments per Volume. This service is also responsible for coordinating GC, authn, authz, and background tasks.

- **Pagestore**
  A service which stores pages keyed by `[volume id]/[pageidx]/[lsn]`. It can efficiently retrieve the latest LSN for a given PageIdx that is less than or equal to a specified LSN, allowing the Pagestore to read the state of a Volume at any Snapshot.

- **Replica Client**
  A node that keeps up with changes to a Volume over time. May subscribe the Metastore to receive Grafts, or periodically poll for updates. Notably, Graft Replicas lazily retrieve Pages they want rather than downloading all changes.

- **Lite Client**
  An embedded client optimized for reading or writing to a volume without any state. Generally has a very small (or non-existant) cache and does not subscribe to updates. Used in "fire and forget" workloads.

- **Segment**
  An object stored in blob storage containing Pages and an index mapping from (Volume ID, PageIdx) to each Page.

- **Segment ID**
  A 16 byte GID used to uniquely identify a Segment.

# GIDs

Graft uses a 16 byte identifier called a Graft Identifier (GID) to identify Segments and Volumes. GIDs are based on ULIDs with a prefix byte.

The primary goals of GIDs are:

- 128 bits in size
- they are alphanumerically sortable by time in both their serialized and binary representations
- they are "typed" such that equality takes the type into account
- collisions have close to zero probability assuming that less than 10k GIDs are created per second

GIDs have the following layout:

```
 0               1               2               3
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     prefix    |                   timestamp                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   timestamp                   |     random    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                             random                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                             random                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Every GID has a 1 byte prefix which encodes it's type. There are currently three known GID types: Volume, Segment, and Client. The prefix may contain other types or namespace metadata in the future. The highest bit of the prefix is always set to ensure that GIDs bs58 serialize to exactly 22 bytes.

Following the prefix is a 48 bit timestamp encoding milliseconds since the unix epoch and stored in network byte order (MSB first).

Finally there are 72 bits of random noise allowing up to `2^72` GIDs to be generated per millisecond.

GIDs are canonically serialized into 22 bytes using the bs58 algorithm with the Bitcoin alphabet:

```
123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz
```

# Consistency Model

_(This model only applies when using the official Graft Client with Graft Server. Third-party client implementations may violate this model.)_

## Global Consistency

Graft provides **[Serializable Snapshot Isolation](https://distributed-computing-musings.com/2022/02/transactions-serializable-snapshot-isolation/)** globally.

All read operations are executed on an isolated snapshot of a Volume.

A write transaction must be based on the latest snapshot to commit. Assuming a compliant Graft client, this enforces [Strict Serializable](https://jepsen.io/consistency/models/strong-serializable).

## Local Consistency

By default, Graft clients commit locally and then asynchronously attempt to commit remotely. Because Graft enforces **Strict Serializability** globally, when two clients concurrently commit based on the same snapshot, one commit will succeed and the other will fail.

Upon rejection, the client must choose one of:

1. **Fork the volume permanently**: This results in a new volume and retains **Strict Serializability**.
2. **Reset and replay**: Reset to the latest snapshot from the server, replay local transactions, and attempt again.
   - The global consistency remains **Strict Serializable**.
   - Locally, the client experiences **Optimistic Snapshot Isolation**, meaning:
     - Reads always observe internally consistent snapshots.
     - However, these snapshots may later be discarded if the commit is rejected.
3. **Merge**: Attempt to merge the remote snapshot with local commits. _(Not yet implemented by Graft; this degrades global consistency to [snapshot isolation](https://jepsen.io/consistency/models/snapshot-isolation))_

**Optimistic Snapshot Isolation:**

Under optimistic snapshot isolation, a client may observe a snapshot which never exists in the global timeline. Here is an example of this in action:

1. Initial State: `accounts = { id: { bal: 10 } }`

2. client A commits locally:
   `update accounts set bal = bal - 10 where id = 1`

   - SNAPSHOT 1: `accounts = { id: { bal: 0 } }`

3. client B commits locally:
   `update accounts set bal = bal - 5 where id = 1`

   - SNAPSHOT 2: `accounts = { id: { bal: 5 } }`

4. client B allows a read transaction based on SNAPSHOT 2:

   - Reads an optimistic snapshot that's not yet committed to the server.

5. client A successfully commits globally.

6. client B’s global commit is rejected:

   - Client B resets to SNAPSHOT 1: `accounts = { id: { bal: 0 } }`

7. client B replays transaction:
   `update accounts set bal = bal - 5 where id = 1`
   - Commit rejected locally: invariant violated (balance cannot be negative).

At this stage, client B should ideally replay or invalidate the read transaction from step (4). If external state changes were based on that read, the client must perform reconciliation to ensure correctness.

# Metastore

A service which stores Volume metadata including the log of segments per Volume. This service is also responsible for coordinating GC, authn, authz, and background tasks.

## Metastore Storage

The Metastore will store it's data in a key value store. For now we will use object storage directly. Each commit to a volume will be a separate file, stored in a way that makes it easy for downstream consumers to quickly get up to date.

## Storage Layout

```
/volumes/[VolumeId]/[LSN]
  CommitHeader
  list of Segment

CommitHeader
  vid: VolumeId
  meta: CommitMeta

CommitMeta:
  cid: ClientId
  lsn: LSN
  checkpoint_lsn: LSN
  page_count: u32
  timestamp: u64

Segment
  sid: SegmentId
  size: u32
  graft: Splinter (size bytes)
```

To ensure that each volume log sorts correctly, LSNs will need to be fixed length and encoded in a sortable way. The easiest solution is to use 0 padded decimal numbers. However the key size can be compressed if more characters are used. It appears that base58 should sort correctly as long as the resulting string is padded to a consistent length.

## API

**`snapshot(VolumeId, LSN)`**
Returns Snapshot metadata for a particular LSN (or the latest if null). Does not include Segments.

**`pull_graft(VolumeId, LSN Range)`**
Retrieve the snapshot at the end of the given LSN range along with a Graft containing all changed indexes in the range. If the start of the range is Unbounded, it will be set to the last checkpoint.

**`pull_commits(VolumeId, LSN Range)`**
Retrieve all of the commits to the Volume in the provided LSN Range. If the start of the range is Unbounded, it will be set to the last checkpoint. Returns: graft.metastore.v1.PullSegmentsResponse

**`commit(VolumeId, ClientId, Snapshot LSN, page_count, segments)`**
Commit changes to a Volume if it is safe to do so. The provided Snapshot LSN is the snapshot the commit was based on. Returns the newly committed Snapshot on success.

The Commit handler is idempotent if the same ClientId tries to issue a duplicate commit. Currently the Metastore only compares the ClientId to detect duplicates. It's up to the Client to ensure that it doesn't submit two different commits at the same LSN. This may be improved via a checksum in the future.

## Checkpointing

A Volume checkpoint represents the oldest LSN for which commit history is stored. Requesting commits or pages for LSNs earlier than the checkpoint may result in an error.

Soon after a Volume checkpoint changes, background jobs on the client and server will begin removing orphaned data:

- Remove any commits in Metastore storage older than the checkpoint LSN
- For each removed commit, reduce the refcount on the commit's segments
  -> Garbage Collection will delete segments with refcount=0 later
- Remove all but the most recent page as of the Checkpoint LSN on clients

## Garbage Collection

Once a segment is no longer referenced by any commit it can be deleted. A grace period will be used to provide safety while we gain confidence in the correctness of the system. To do this we can mark a segment for deletion with a timestamp, and then only delete it once the grace period has elapsed.

## API Keys

For now we will proceed without authentication. Eventually, the Metastore will manage API keys, and associate them with Organizations. Authentication across the distributed system will be handled via Signed Tokens to ensure that the Pagestore and Metastore can validate tokens without centralized communication.

# Pagestore

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

**`read_pages(Volume ID, LSN, graft)`**
First, updates our segment index if we haven't seen this Volume ID/LSN before by querying the Metastore for new Segments.

Then selects a list of Segment candidates by querying the Segment index for all Segments that contain LSN's up to the target LSN for the specific Volume ID and overlaps with the requested graft.

Finally, queries the index of each matching Segment, which may require downloading and caching the Segment from Object Storage. As the node finds the most recent matching LSN for each PageIdx, some of the Segment candidates may be skippable (if they no longer overlap with outstanding PageIdxs). Pages are sent back to the client as they are found in a stream. Each page is prefixed with a header containing it's PageIdx.

If the Pagestore encounters missing Segments, it must update the Segment index. It's possible that the client is querying a LSN which is older than the oldest checkpoint in which case we will fail the request.

> Important: Segments with overlapping grafts and version ranges must be iterated in an order determined by the metastore. This is to handle the case that a single transaction wrote the same PageIdx multiple times at the same LSN.

**`write_pages(Volume ID, [(pageidx, page)]`**
Writes a set of Pages for a Volume. Returns a list of new Segments: `[(segment ID, graft)]` once they have been flushed to durable storage. Implementations should support streaming writes to the server to improve pipeline performance.

The writePages request will fail if the client submits the same PageIdx multiple times. This ensures that every segment generated by a request does not intersect.

Newly written segments may be cached on disk, but not added to the Segment index. This is because the pagestore doesn't yet know if the Segments have been accepted by the Metastore, and additionally doesn't know their assigned LSN.

## Pagestore internal dataflow

https://app.excalidraw.com/s/65i7nRDHAIV/1GVOEpCLvJ0

# Volume Router

In order to scale the Metastore around the world, we need a globally available routing system to determine where each Volume lives. This allows the Metastore to be entirely region local which is simpler, faster, and cheaper than backing it with some kind of globally available database.

The only data we will need to make globally available is where each Volume lives. We can solve this in a number of ways:

1. Add region namespacing to Volume Ids. This permanently pins each Volume to a region (or at least a namespace) allowing clients to send traffic to the right location without any additional communication. The downside is a lack of flexibility.
2. A globally available volume registry service. Cloudflare might be the ideal place for this. They provide multiple storage and caching services that would fairly efficiently keep this routing data highly available globally.

I'm still undecided, but leaning towards using CF as a volume registry to increase flexiblity in volume placement (and more importantly the ability to move volumes).

# Client

Graft Clients support reading and writing to Volumes.

## Local Storage

The current Graft client runtime stores data in three Fjall partitions.

```
volumes:
  {vid}/config          -> VolumeConfig
  {vid}/status          -> VolumeStatus
  {vid}/snapshot        -> Snapshot
  {vid}/watermarks      -> Watermarks

pages:
  {vid}/{pageidx}/{LSN} -> PageValue

commits:
  {vid}/{LSN}           -> Graft

VolumeConfig:
  sync: Disabled | Push | Pull | Both

VolumeStatus: Ok | RejectedCommit | Conflict

Snapshot:
  local: LSN
  remote: RemoteMapping
  pages: PageCount

RemoteMapping:
  Unmapped
  Mapped {
    remote: LSN,
    local: LSN
  }

Watermarks:
  pending_sync: Option<LSN>
  checkpoint: Option<LSN>

PageValue:
  Pending
  Empty
  Available(Page)

Graft:
  Splinter of all PageIdxs changed in the commit
```

## Reading

To issue a local read against a Volume snapshot:

1. Lookup the latest page in storage such that `page.LSN <= snapshot.local`

   - If this page is either Available or Empty return the page

2. If `snapshot.remote` is empty, return an empty page

3. Request the page from the Pagestore

   - This may be batched along with prefetches

4. Save the requested page into storage at `page.LSN`

## Writing

Writes commit locally and then are asynchronously committed remotely. This section only deals with the local commit.

Writes go through a `VolumeWriter` which buffers newly written pages in a memtable. Reads check the memtable to enable RYOW before falling back to the regular Read algorithm. Each `VolumeWriter` is pinned to a Snapshot.

The commit process happens atomically via a Fjall batch.

1. Set `commit_lsn = snapshot.local.next()`
2. Persist the memtable at `commit_lsn`
3. Write out a Graft to the commits partition at `commit_lsn`
4. Take the local commit lock
5. Set `latest` to the latest volume Snapshot
6. Fail if `latest.local != snapshot.local`
7. Write out the new snapshot (without changing the remote mapping)
8. Commit the Fjall batch
9. release the commit lock

## Sync

The Graft Client runtime supports asynchronously pushing and pulling from the server. Since this process happens out of band, two writers committing to the same Volume will frequently conflict and will need to rebase or reset to continue.

Future work:

- synchronous commit+push to make conflicts easier to detect
- MVCC automatic conflict resolution
- Rebase conflict resolution

### Sync: Pull

The Graft runtime polls /metastore/v1/pull_graft for changes. When a change is detected, the runtime attempts to "accept" the change.

The pull process happens atomically via a Fjall batch.

1. Take the local commit lock
2. Read the latest Volume Snapshot and Watermarks
3. If remote_mapping.local < pending_sync: FAIL with VolumeNeedsRecovery
4. If remote_mapping.local < snapshot.local: FAIL with RemoteConflict

   - set Volume status to VolumeStatus::Conflict

5. Set `commit_lsn = snapshot.local.next()`
6. Update the snapshot

   - `local=commit_lsn, remote=(remote_lsn, commit_lsn), pages=remote_pages`

7. Update the watermarks

   - `pending_sync=commit_lsn`

8. For each changed pageidx in the remote commit, write out PageValue::Pending into the pages partition using `commit_lsn`. This ensures that future reads know to fetch the page from the Pagestore.
9. Commit the Fjall batch
10. release the commit lock

FAIL states:
VolumeNeedsRecovery
This means that we had previously crashed in the middle of pushing the Volume to the server. The client needs to recover or reset the volume before continuing.

Conflict
This means that we have made local commits since the last successful sync. The client needs to reconcile with the server before continuing.

### Sync: Push

When the Graft runtime detects a local commit has occurred, it tries to push the commit to the server.

1. Take the local commit lock
2. Read the latest Volume Snapshot and Watermarks
3. If remote_mapping.local < pending_sync: FAIL with VolumeNeedsRecovery
4. update `watermarks.pending_sync` to `snapshot.local`
5. calculate the LSN range to push:

   - `start_lsn = remote_mapping.local.next()`
   - `end_lsn = snapshot.local`

6. release the local commit lock
7. iterate through the local commit splinters

   - send the most recent page for each pageidx to the pagestore
   - collect new segments

8. commit the segments to the metastore
9. take the local commit lock

**On commit success:**

1. Open a Fjall batch
2. Read the latest Volume Snapshot and Watermarks
3. Assert that the new remote LSN is larger than the last remote LSN
4. Assert that `watermarks.pending_sync == snapshot.local`
5. Update the snapshot's remote mapping to (remote_lsn, snapshot.local)
6. Remove all successfully synced commit grafts
7. Commit the batch
8. Release the local commit lock

**On commit failure:**

1. Update `watermarks.pending_sync = snapshot.remote_mapping.local`
2. Set Volume status to VolumeStatus::RejectedCommit

## Crash recovery

The Graft client runtime must be able to crash at any point and recover. Fjall already has it's own recovery mechanisms built in, so we just need to handle failed Pushes. Failed pushes can be detected when `pending_sync` is larger than `remote_mapping.local` and no concurrent Push job is running.

When a volume is in this failed push state, it needs to determine if the commit was successfully accepted by the Metastore or not. It does so by retrying the commit process with the same idempotency token.

## Lite Client

In some cases, a Client may want to boot without any state and quickly read (+ possibly write) to a particular Volume snapshot. In the most minimal case, if the client already knows the LSN of the snapshot they want to access, they can read from the Page Server immediately. If they want to issue a write, they will need to read the latest snapshot to get the page count and current remote LSN.

Supporting Lite Clients is desirable to help enable edge serverless workloads which want to optimize for latency and have no cached state.

# Implementation Details

The Metastore and Pagestore will be written in Rust using Tokio.

The Client will be a Rust library, optimized to use a minimum amount of resources and be embedded into other libraries. The primary targets will be:

- shared object to be used with SQLite
- python library
- rust library (eventually supporting async and wasm)

Networking stack:

- transport: TCP
- application: Protobuf over HTTPs

## Endianness

Graft serializes and deserializes a lot of data to disk and object_storage. It currently only runs on Little Endian systems which are the vast majority. If you happen to want to use Graft on a Big Endian system please file an issue and we can talk about it. All Graft network messages are Protobuf which is agnostic to the Endianness of the system, so building a Graft Client that works on Big Endian systems should be fine.
