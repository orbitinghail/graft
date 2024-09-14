# Graft <!-- omit from toc -->

Transactional lazy replication to the edge. Optimized for scale and cost over latency. Heavily leverages object storage for durability.

# Outline <!-- omit from toc -->

- [High Level Architecture](#high-level-architecture)
- [Glossary](#glossary)
- [MetaStore](#metastore)
  - [Storage Layout](#storage-layout)
  - [Durable Object Initialization](#durable-object-initialization)
  - [API](#api)
  - [Checkpointing](#checkpointing)
- [PageStore](#pagestore)
  - [Storage Layout](#storage-layout-1)
  - [Segment Layout](#segment-layout)
  - [Segment Cache](#segment-cache)
  - [API](#api-1)
  - [PageStore internal dataflow](#pagestore-internal-dataflow)
- [Control Plane](#control-plane)
  - [Volume Management](#volume-management)
  - [Segment Management](#segment-management)
  - [Segment Compaction](#segment-compaction)
  - [Volume History Truncation](#volume-history-truncation)
  - [Garbage Collection](#garbage-collection)
  - [API Keys](#api-keys)
- [Client](#client)
  - [Initialization](#initialization)
  - [Local Storage](#local-storage)
  - [Reading](#reading)
  - [Writing](#writing)
  - [Lite Client](#lite-client)
- [Implementation Details](#implementation-details)
  - [Performance](#performance)
    - [Request Hedging](#request-hedging)

# High Level Architecture

https://link.excalidraw.com/readonly/CJ51JUnshsBnsrxqLB1M

# Glossary

- **Volume**  
  A sparse data object consisting of Pages located at Offsets starting from 0. Volumes are referred to primarily by a Volume ID.

- **Volume ID**  
  A 16 byte GUID used to uniquely identify a Volume.

- **Page**  
  A fixed-length block of storage. The default size is 4KiB (4096 bytes).

- **Offset**  
  The position of a page within a volume, starting from 0.

- **LSN**  
  A monotonically increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume.

- **rLSN**
  A relative LSN used to represent LSNs in Segments. Each Segment may store the same Offset for a Volume multiple times at various LSNs. However, we do not store the absolute LSNs in the Segment to allow the MetaStore to decide on the final commit order. When reading Segments, we thus have to convert rLSNs to absolute LSNs by adding the base LSN for the Volume in this Segment (which is stored in the MetaStore's Segment index).

- **Snapshot**  
  A tuple (volume id, lsn, max offset) that defines a fixed point in time for the state of a volume.

- **Graft**  
  The set of Page Offsets which have changed between any two Snapshots.

- **MetaStore**  
  A service which stores a log of Snapshots and Grafts for each Volume.

- **PageStore**  
  A service which stores pages keyed by `[volume id]/[offset]/[lsn]`. It can efficiently retrieve the latest LSN for a given Offset that is less than or equal to a specified LSN, allowing the PageStore to read the state of a Volume at any Snapshot.

- **Replica**  
  A node that keeps up with changes to a Volume over time. May subscribe the MetaStore to receive Grafts, or periodically poll for updates.

- **Lite Client**  
  A node which only reads a Volume at a particular Snapshot.

- **Segment**
  An object stored in blob storage containing Pages. Also contains an index mapping from (Volume ID, Offset, LSN) to each Page.

- **Segment ID**
  A 16 byte GUID used to uniquely identify a Segment.


# MetaStore

The MetaStore is implemented as a CloudFlare Durable Object per Volume. Each object maintains the Volume's log using transactional storage.

## Storage Layout

```
/log/[lsn]
  /snapshot -> SnapshotHeader
  /segments
    /added -> SegmentList
    /removed -> SegmentIDList

SnapshotHeader
  LSN
  Max Offset in Volume
  Commit Timestamp
  Offsets 
    compressed bitmap of offsets changed in this lsn

SegmentList
  [
    {
      Segment ID
      Base LSN
      Max LSN
      Offset range
      Num Pages
    }
  ]

SegmentIDList
  [Segment ID]
```

> Note: If Segments or Offsets overflow we can store additional sibling entries. The max size of an entry is 128KiB in CF, so to start we can probably just fail the txn.

## Durable Object Initialization

When initializing the DO, we just need to read the latest Snapshot and prepare the next LSN.

## API

**`getSnapshotMetadata(LSN)`**  
Returns Snapshot metadata for a particular LSN (or the latest if null). Does not include Offsets or Segments.

**`getChangedOffsets(LSN A, [LSN B])`**  
Returns a compressed bitmap of changed offsets between LSN A (exclusive) and LSN B (inclusive). LSN B can be null, in which case it defaults to the latest Snapshot. This method will also return the Snapshot at LSN B.

**`getNewSegments(LSN A, [LSN B])`**  
Returns a list of segments added between LSN A (exclusive) and LSN B (inclusive). LSN B can be null, in which case it defaults to the latest Snapshot. This method will also return the Snapshot at LSN B.

**`commit(Snapshot LSN, new max offset, offsets, segments)`**  
Commit a new Snapshot to the Volume at the next LSN. Returns the committed Snapshot metadata or an error.

**`compact(Snapshot LSN, added segments, removed segments)`**  
Inform the MetaStore that a new checkpoint has been created at a particular LSN. If multiple segments are generated, they will represent non-overlapping offset ranges. Will be stored in the log at the next LSN.

## Checkpointing
Checkpointing a Volume involves picking a LSN to checkpoint at and producing a complete image of the Volume at that LSN. It replaces any single-LSN segments at the specified LSN for the Volume.

Checkpoints are created whenever a configurable amount of data has changed since the last checkpoint. The MetaStore issues and monitors checkpoint jobs. The jobs run in the background on the PageStore.

Checkpoint Job Algorithm:
```
1. retrieve metadata of last checkpoint (may be multiple segments)
2. retrieve metadata of all changes since checkpoint
3. for any unchanged segments from previous checkpoint, simply re-emit those segments at the checkpoint LSN (yay for rLSNs)
4. rewrite any changed segments by merging in offsets
5. emit changed segments at checkpoint LSN
6. commit checkpoint to MetaStore
```

# PageStore

The PageStore is responsible for storing and looking up Pages in Segments stored in Blob Storage Services like S3 or Tigris.

## Storage Layout

```
/segments
  /[Segment ID] -> Segment
```

## Segment Layout

A Segment is a binary file composed of three sections: Header, Data, Index.
The Index is only included if it's larger than what can fit in the Header.

```
Header (4KiB)
  magic
  version
  index_offset (if 0, then inline)
  [Index inlined]
```

**Data**  
List of Pages stored back to back immediately after the Header

**Index**  
A serialized ODHT (https://docs.rs/odht). Built as a regular in-memory HT while collecting Pages, and then compressed into an on-disk ODHT via from_iterator (max_load_factor=100%).

The Index is a map from `(Volume ID, Offset, rLSN)` to the Page offset in the file. In order to lookup an offset at a particular LSN, one must first retrieve the base LSN for the Segment from the MetaStore and add it to each rLSN.

## Segment Cache
The PageStore must cache recently read Segments in order to minimize round trips to Object Storage and improve performance. The disk cache should have a configurable target max size, and remove the least recently accessed Segment to reclaim space.

In addition, we should have a memory based cache. One option is to read all of the Segment indexes into memory, and leave page caching up to the kernel. Research needs to be done on if this approach is feasible given the planned compute sizes.

## API

**`readPages(Volume ID, LSN, offsets)`**  
First, updates our segment index if we haven't seen this Volume ID/LSN before by querying the MetaStore for new Segments.

Then selects a list of Segment candidates by querying the Segment index for all Segments that contain LSN's up to the target LSN for the specific Volume ID and overlaps with the requested offsets.

Finally, queries the index of each matching Segment, which may require downloading and caching the Segment from Object Storage. As the node finds the most recent matching LSN for each offset, some of the Segment candidates may be skippable (if they no longer overlap with outstanding offsets). Pages are sent back to the client as they are found in a stream. Each page is prefixed with a header containing it's offset and length.

If the PageStore encounters missing Segments, it must update the Segment index. It's possible that the client is querying a LSN which is older than the oldest checkpoint in which case we will fail the request.

> Important: Segments with overlapping offset and version ranges must be iterated in an order determined by the metastore. This is to handle the case that a single transaction wrote the same offset multiple times at the same LSN.

**`writePages(Volume ID, [(offset,page)]`**  
Writes a set of Pages for a Volume. Returns a list of new Segments: `[(segment ID, offset range)]` once they have been flushed to durable storage. Implementations should support streaming writes to the server to improve pipeline performance.

It's critical that this method produces Segments without duplicate offsets. Every offset written by this method will have a rLSN of 0.

Clients may write the same offset multiple times in rare cases. When this happens, the PageStore simply needs to ensure that each segment produced contains no duplicate offsets. If the offset already exists in the currently open Segment then overwrite it with the new offset. Otherwise simply write it to the next open Segment.

Segments must be returned in the order they were written. Thus ensuring that more recent Segments shadow offsets in earlier Segments.

Newly written segments may be cached on disk, but not added to the Segment index. This is because the pagestore doesn't yet know if the Segments have been accepted by the MetaStore, and additionally doesn't know their assigned LSN.

## PageStore internal dataflow

https://app.excalidraw.com/s/65i7nRDHAIV/1GVOEpCLvJ0

# Control Plane
The Control Plane manages Volumes, Segments, and API Keys. All data is stored in a centralized (but globally available) D1 database and exposed via a Worker based API.

## Volume Management
Volumes are managed through the Control Plane which recusively communicates with the Volume's Durable Object as needed. The Control Plane stores information about every volume in its database.

## Segment Management
After the MetaStore commits new segments it's responsible for replicating added and removed segments to the Control Plane. This can be done async on a background timer.

## Segment Compaction
Compaction is the act of reorganizing segments over time to improve read performance as well as storage cost.

In L0, each Segment contains offsets at one rLSN per Volume. This is the default layer in which Segments are created.

In L1, each Segment contains offsets at multiple rLSNs per Volume. Whenever possible, L1 Segments contain data for a single Volume.

The decision to merge depends on optimal Segment size. Let's say the optimal Segment size is 8 MB (AnyBlob suggests 8-16 MB, while Neon uses 128 MB). In this case we would want to collect Segments which overlap in Volume until we can produce at least one Optimal Segment which only contains data for a single Volume (or we run out of Segments to merge).

Once we can produce one single-volume optimal Segment. The rest of the data is distributed to other Segments. This packing problem can be solved using the following greedy approach:

1. Set min_bucket=8MB and max_bucket=16MB
2. Collect offsets and rLSNs per Volume from Segments into candidate chunks. Care should be taken to always include all Segments from a Snapshot to handle duplicate offsets. Stop collection once the largest chunk is larger than min_bucket size.
3. Partition any chunks larger than max_bucket by offset and LSN until all chunks are smaller than max_bucket size.
4. Iterate through chunks from largest to smallest, emitting Segments as they reach min_bucket size.
5. Commit added/removed segments to each MetaStore
6. Delete all removed segments

## Volume History Truncation
We only need to keep around a certain amount of history for each Volume. This probably should be configurable, but for now we can default it to one week.

Truncation can be handled by taking a checkpoing at the oldest surviving LSN and then removing any older segments from the Volume. GC will handle removing those segments from Storage eventually.

## Garbage Collection
As the MetaStore informs the Control Plane of removed Segments, once a Segment is not referenced by any Volume it can be deleted. We may want to delay actual deletion via a grace period until we gain confidence in the correctness of the system.

## API Keys
For now we will proceed without authentication. Eventually, the Control Plane will manage API keys, and associate them with Organizations. Authentication across the distributed system will be handled via Signed Tokens to ensure that the PageStore and MetaStore can validate tokens without centralized communication.

# Client
Graft Clients support reading and writing to Volumes.

## Initialization
To start, a Client must open a Volume at either a specific or the latest LSN. This entails sending a `getSnapshotMetadata` request to the MetaStore. If the Client has cached state for this Volume, it can also run `getChangedOffsets(last LSN, new LSN)` to become aware of changed offsets.

## Local Storage
Clients will cache Pages keyed by `[volume id]/[offset]/[LSN]`. Pages which are known of (via `getChangedOffsets`) but not yet retrieved will point at a 'missing token'.

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
4. Write new pages directly to the PageStore keeping track of new segments
   - Write through local storage, but be prepared to rollback if needed
5. Commit new segments and offsets to the MetaStore
6. Record the new Snapshot locally as the latest snapshot (which should implictly commit changes to local storage)

> Note on concurrent writers: If it's possible that other writers may be concurrently accessing the volume, then first we need to query the MetaStore for the latest Volume snapshot. This may be out of date by the time we commit, so Volume concurrency must be extremely low for this to work. A future iteration on the MetaStore will increase concurrency via inspecting the read/write sets of concurrent txns.

> Note on offline/async writers: It's possible to perform writes offline/async and then commit at a later point. This has some tradeoffs. On the plus side, we can make progress without waiting for the network. We can also coalesce multiple local transactions into a single network transaction. On the down side, this requires some form of local durability, makes local storage a source of truth for some subset of the data, and does not handle concurrency very well. In theory mvcc optimistic concurrency would still work so long as the writers never overlapped their read/write sets. But in general, concurrency would not be recommended.

## Lite Client
In some cases, a Client may want to boot without any state and quickly read (+ possibly write) to a particular Volume snapshot. In the most minimal case, if the client already knows the LSN of the snapshot they want to access, they can read from the Page Server immediately. If they want to issue a write, they may need to read the latest Snapshot metadata (to get the Max Offset) first, but otherwise can pretty much just run the write algorithm.

Supporting Lite Clients is desirable to help enable edge serverless workloads which want to optimize for latency and have no cached state.

# Implementation Details

For the MetaStore and Control Plane we will use Typescript for a more native CloudFlare worker experience.

The PageStore will be written in Rust using Tokio. If this proves to be a PITA we can switch to Go.

The Client will be a Rust library, optimized to use a minimum amount of resources and be embedded into other libraries. The primary targets will be:
- shared object to be used with SQLite
- python library
- rust library (eventually supporting async and wasm)

Networking stack:
- transport: TCP
- application: HTTP

## Performance

### Request Hedging
According to the [AnyBlob paper], hedging requests to blob storage can help dramatically reduce tail latency. For S3, the paper suggests hedging if you haven't received the first byte within 200ms. Slightly more aggressive hedging may also be desirable, like hedging if you haven't completly downloaded the file within 600ms. Making this configurable and testing is important.

[AnyBlob paper]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf