# Graft <!-- omit from toc -->

Transactional lazy replication to the edge. Optimized for scale and cost over latency. Leverages object storage for durability.

# Outline <!-- omit from toc -->

- [High Level Architecture](#high-level-architecture)
- [Glossary](#glossary)
- [GIDs](#gids)
- [Metastore](#metastore)
  - [Metastore Storage](#metastore-storage)
  - [Storage Layout](#storage-layout)
  - [API](#api)
  - [Checkpointing](#checkpointing)
  - [Garbage Collection](#garbage-collection)
  - [API Keys](#api-keys)
- [Pagestore](#pagestore)
  - [Storage Layout](#storage-layout-1)
  - [Segment Layout](#segment-layout)
  - [Segment Cache](#segment-cache)
  - [API](#api-1)
  - [Pagestore internal dataflow](#pagestore-internal-dataflow)
- [Volume Router](#volume-router)
- [Client](#client)
  - [Initialization](#initialization)
  - [Local Storage](#local-storage)
  - [Reading](#reading)
  - [Writing](#writing)
  - [Lite Client](#lite-client)
- [Implementation Details](#implementation-details)

# High Level Architecture

https://link.excalidraw.com/readonly/CJ51JUnshsBnsrxqLB1M

# Glossary

- **GID**
  A 128 bit Graft Identifier. See [GIDs](#gids) for details.

- **Volume**  
  A sparse data object consisting of Pages located at Offsets starting from 0. Volumes are referred to primarily by a Volume ID.

- **Volume ID**  
  A 16 byte GID used to uniquely identify a Volume.

- **Page**  
  A fixed-length block of storage. The default size is 4KiB (4096 bytes).

- **Offset**  
  The position of a page within a volume, measured in terms of page numbers rather than bytes. The offset represents the index of the page, with the first page in the volume having an offset of 0.

- **LSN**  
  A monotonically increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume.

- **Snapshot**  
  A tuple (volume id, lsn, max offset) that defines a fixed point in time for the state of a volume. Max offset can be used to determine Volume length and calculate the Volume's maximum size (actual size must take sparseness into account).

- **Graft**  
  The set of Offsets which have changed between any two Snapshots.

- **Metastore**  
  A service which stores Volume metadata including the log of segments per Volume. This service is also responsible for coordinating GC, authn, authz, and background tasks.

- **Pagestore**  
  A service which stores pages keyed by `[volume id]/[offset]/[lsn]`. It can efficiently retrieve the latest LSN for a given Offset that is less than or equal to a specified LSN, allowing the Pagestore to read the state of a Volume at any Snapshot.

- **Replica Client**  
  A node that keeps up with changes to a Volume over time. May subscribe the Metastore to receive Grafts, or periodically poll for updates. Notably, Graft Replicas lazily retrieve Pages they want rather than downloading all changes.

- **Lite Client**  
  An embedded client optimized for reading or writing to a volume without any state. Generally has a very small (or non-existant) cache and does not subscribe to updates. Used in "fire and forget" workloads.

- **Segment**
  An object stored in blob storage containing Pages and an index mapping from (Volume ID, Offset) to each Page.

- **Segment ID**
  A 16 byte GID used to uniquely identify a Segment.

# GIDs

Graft uses a 16 byte identifier called a Graft Identifier (GID) to identify Segments and Volumes. GIDs are based on ULIDs with a prefix byte.

The primary goals of GIDs are:
- 128 bits in size
- they are alphanumerically sortable by time
- they are "typed" such that equality takes the type into account
- collisions have close to zero probability assuming that less than 10k GIDs are created per second

GIDs have the following layout:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
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

Every GID has a 1 byte prefix which encodes it's type. There are currently two known GID types: Volume and Segment. The prefix may contain other types or namespace metadata in the future.

Following the prefix is a 48 bit timestamp encoding milliseconds since the unix epoch and stored in network byte order (MSB first).

Finally there are 72 bits of random noise allowing up to `2^72` Gids to be generated per millisecond.

# Metastore
A service which stores Volume metadata including the log of segments per Volume. This service is also responsible for coordinating GC, authn, authz, and background tasks.

## Metastore Storage

The Metastore will store it's data in a key value store. For now we will use object storage directly. Each commit to a volume will be a separate file, stored in a way that makes it easy for downstream consumers to quickly get up to date.

## Storage Layout

```
/volumes/[VolumeId]/[LSN]
  CommitHeader
  list of Segment

CommitHeader (36 bytes)
  vid: VolumeId (16 bytes)
  lsn: LSN (8 bytes)
  ts: Unix timestamp in milliseconds (8 bytes)
  num_segments: u32 (4 bytes)

Segment
  sid: SegmentId (16 bytes)
  offsets_size: u32 (4 bytes)
  offsets: Splinter (offsets_size bytes)
```

To ensure that each volume log sorts correctly, LSNs will need to be fixed length and encoded in a sortable way. The easiest solution is to use 0 padded decimal numbers. However the key size can be compressed if more characters are used. It appears that base58 should sort correctly as long as the resulting string is padded to a consistent length.

## API

**`snapshot(VolumeId, LSN)`**  
Returns Snapshot metadata for a particular LSN (or the latest if null). Does not include Segments.

**`pull_offsets(VolumeId, LSN Range)`**  
Retrieve the snapshot at the end of the given LSN range along with a Splinter containing all changed offsets. If the start of the range is Unbounded, it will be set to the last checkpoint.

**`pull_commits(VolumeId, LSN Range)`**  
Retrieve all of the commits to the Volume in the provided LSN Range. If the start of the range is Unbounded, it will be set to the last checkpoint.  Returns: graft.metastore.v1.PullSegmentsResponse

**`commit(VolumeId, Snapshot LSN, last_offset, segments)`**  
Commit changes to a Volume if it is safe to do so. The provided Snapshot LSN is the snapshot the commit was based on. Returns the newly committed Snapshot metadata on success.

A checkpoint may be created by issuing a commit that covers the entire offset range of the volume.

## Checkpointing
Checkpointing a Volume involves picking a LSN to checkpoint at and producing a complete image of the Volume at that LSN. It replaces any single-LSN segments at the specified LSN for the Volume.

Checkpoints are created whenever a configurable amount of data has changed since the last checkpoint and when the checkpoint would be of an acceptable size. The Metastore issues and monitors checkpoint jobs.

Checkpoint Job Algorithm:
```
1. retrieve metadata of last checkpoint (may be multiple segments)
2. retrieve metadata of all changes since checkpoint
3. for any unchanged segments from previous checkpoint, simply re-emit those segments at the checkpoint LSN
4. rewrite any changed segments by merging in new offsets
5. emit changed segments at checkpoint LSN
6. commit checkpoint to Metastore
```

We should have a special Segment type which is optimized for storing checkpoints. A checkpoint segment contains a single contiguous range of offsets for a single volume. It may be desirable to separate them from regular segments in storage to make them more efficient to find.

We will keep around multiple checkpoints for a Volume in order to support some configurable amount of history. The Metastore is responsible for truncating history based on each volume's configuration.

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

A Segment is a binary file composed of three sections: Header, Data, Index.
The Index is only included if it's larger than what can fit in the Header.

```
Header (Page Size)
  magic
  version
  index_offset (if 0, then inline)
  [Index inlined]
```

**Data**  
List of Pages stored back to back immediately after the Header

**Index**  
A serialized ODHT (https://docs.rs/odht). Built as a regular in-memory HT while collecting Pages, and then compressed into an on-disk ODHT via from_iterator (max_load_factor=100%).

The Index is a map from `(Volume ID, Offset)` to the Page offset in the file. 

## Segment Cache
The Pagestore must cache recently read Segments in order to minimize round trips to Object Storage and improve performance. The disk cache should have a configurable target max size, and remove the least recently accessed Segment to reclaim space.

In addition, we should have a memory based cache. One option is to read all of the Segment indexes into memory, and leave page caching up to the kernel. Research needs to be done on if this approach is feasible given the planned compute sizes.

## API

**`read_pages(Volume ID, LSN, offsets)`**  
First, updates our segment index if we haven't seen this Volume ID/LSN before by querying the Metastore for new Segments.

Then selects a list of Segment candidates by querying the Segment index for all Segments that contain LSN's up to the target LSN for the specific Volume ID and overlaps with the requested offsets.

Finally, queries the index of each matching Segment, which may require downloading and caching the Segment from Object Storage. As the node finds the most recent matching LSN for each offset, some of the Segment candidates may be skippable (if they no longer overlap with outstanding offsets). Pages are sent back to the client as they are found in a stream. Each page is prefixed with a header containing it's offset and length.

If the Pagestore encounters missing Segments, it must update the Segment index. It's possible that the client is querying a LSN which is older than the oldest checkpoint in which case we will fail the request.

> Important: Segments with overlapping offset and version ranges must be iterated in an order determined by the metastore. This is to handle the case that a single transaction wrote the same offset multiple times at the same LSN.

**`write_pages(Volume ID, [(offset,page)]`**  
Writes a set of Pages for a Volume. Returns a list of new Segments: `[(segment ID, offset range)]` once they have been flushed to durable storage. Implementations should support streaming writes to the server to improve pipeline performance.

The writePages request will fail if the client submits the same offset multiple times. This ensures that every segment generated by a request does not intersect.

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

## Initialization
To start, a Client must open a Volume at either a specific or the latest LSN. This entails sending a `getSnapshotMetadata` request to the Metastore. If the Client has cached state for this Volume, it can instead run `pullOffsets(last LSN)` to become aware of changed offsets while retrieving the snapshot.

## Local Storage
Clients will cache Pages keyed by `[volume id]/[offset]/[LSN]`. Pages which are known of (via `pullOffsets`) but not yet retrieved will point at a 'missing token'.

Maintaining and compacting the cache is highly platform dependent. We will likely need to implement a checkpointing mechanism on each platform that can roll up all downloaded offsets to a particular LSN in order to free space. Depending on the KV store used, this may be easier or harder to do. On the plus side, we can always re-download any accidentally deleted pages assuming the client has an internet connection.

## Reading
To issue a local read:
1. Snapshot the volume at the target LSN (or latest)
2. Notify GC not to checkpoint past the snapshot LSN
3. Read through local storage, fetching pages as needed
4. Release the GC lock

> Note: We can increase read performance by detecting scans and performing predictive fetches on offsets we think we may need soon. mvSQLite uses a basic relative offset history cache which is interesting. But to start a simple scan detector may be sufficient. Also explore a heatmap to detect pages that are frequently read soon after an update.

## Writing
To write to a volume we:
1. Snapshot the volume at the latest LSN
2. Notify GC not to checkpoint past the snapshot LSN
3. Read through local storage, fetching pages as needed
4. Write new pages directly to the Pagestore keeping track of new segments
   - Write through local storage, but be prepared to rollback if needed
5. Commit new segments and offsets to the Metastore
6. Record the new Snapshot locally as the latest snapshot (which should implictly commit changes to local storage)

> Note on concurrent writers: If it's possible that other writers may be concurrently accessing the volume, then first we need to query the Metastore for the latest Volume snapshot. This may be out of date by the time we commit, so Volume concurrency must be extremely low for this to work. A future iteration on the Metastore will increase concurrency via inspecting the read/write sets of concurrent txns.

> Note on offline/async writers: It's possible to perform writes offline/async and then commit at a later point. This has some tradeoffs. On the plus side, we can make progress without waiting for the network. We can also coalesce multiple local transactions into a single network transaction. On the down side, this requires some form of local durability, makes local storage a source of truth for some subset of the data, and does not handle concurrency very well. In theory mvcc optimistic concurrency would still work so long as the writers never overlapped their read/write sets. But in general, concurrency would not be recommended.

## Lite Client
In some cases, a Client may want to boot without any state and quickly read (+ possibly write) to a particular Volume snapshot. In the most minimal case, if the client already knows the LSN of the snapshot they want to access, they can read from the Page Server immediately. If they want to issue a write, they may need to read the latest Snapshot metadata (to get the Max Offset) first, but otherwise can pretty much just run the write algorithm.

Supporting Lite Clients is desirable to help enable edge serverless workloads which want to optimize for latency and have no cached state.

# Implementation Details

The Metastore and Pagestore will be written in Rust using Tokio.

The Client will be a Rust library, optimized to use a minimum amount of resources and be embedded into other libraries. The primary targets will be:
- shared object to be used with SQLite
- python library
- rust library (eventually supporting async and wasm)

Networking stack:
- transport: TCP
- application: HTTP(s)