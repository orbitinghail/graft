# Graft <!-- omit from toc -->

Transactional lazy replication to the edge. Optimized for scale and cost over latency. Leverages object storage for durability.

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
  - [Checkpointing](#checkpointing-1)
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
  The position of a page within a volume, measured in terms of page numbers rather than bytes. The offset represents the index of the page, with the first page in the volume having an offset of 0.

- **LSN**  
  A monotonically increasing number that tracks changes to a Volume. Each transaction results in a new LSN, which is greater than all previous LSNs for the Volume.

- **Snapshot**  
  A tuple (volume id, lsn, max offset) that defines a fixed point in time for the state of a volume. Max offset can be used to determine Volume length and calculate the Volume's maximum size (actual size must take sparseness into account).

- **Graft**  
  The set of Offsets which have changed between any two Snapshots.

- **MetaStore**  
  A service which stores a log of Snapshots and Grafts for each Volume.

- **PageStore**  
  A service which stores pages keyed by `[volume id]/[offset]/[lsn]`. It can efficiently retrieve the latest LSN for a given Offset that is less than or equal to a specified LSN, allowing the PageStore to read the state of a Volume at any Snapshot.

- **Replica Client**  
  A node that keeps up with changes to a Volume over time. May subscribe the MetaStore to receive Grafts, or periodically poll for updates. Notably, Graft Replicas lazily retrieve Pages they want rather than downloading all changes.

- **Lite Client**  
  An embedded client optimized for reading or writing to a volume without any state. Generally has a very small (or non-existant) cache and does not subscribe to updates. Used in "fire and forget" workloads.

- **Segment**
  An object stored in blob storage containing Pages and an index mapping from (Volume ID, Offset) to each Page.

- **Segment ID**
  A 16 byte GUID used to uniquely identify a Segment.


# MetaStore

The MetaStore is implemented as a CloudFlare Durable Object per Volume. Each object maintains the Volume's log using transactional storage.

## Storage Layout

```
/log/[lsn]
  /header -> CommitHeader
  /overflow_0 -> CommitPart

CommitHeader
  LSN
  Max Offset in Volume
  Commit Timestamp
  segments: SegmentList

CommitPart
  segments: SegmentList

OffsetSet
  compressed bitmap of offsets changed in this lsn

SegmentList
  [
    {
      Segment ID
      OffsetSet
    }
  ]
```

> Note: If Segments or Offsets overflow we can store additional sibling entries. The max size of an entry is 128KiB in CF.

## Durable Object Initialization

When initializing the DO, we just need to read the latest Commit and prepare the next LSN.

## API

**`getSnapshotMetadata(LSN)`**  
Returns Snapshot metadata for a particular LSN (or the latest if null). Does not include Segments.

**`pullOffsets(LSN A)`**  
Returns a compressed bitmap of changed offsets between LSN A (exclusive) and the latest LSN (inclusive). This method will also return the Snapshot at LSN B.

**`pullSegments(LSN A)`**  
Returns a list of segments added between LSN A (exclusive) and the latest LSN (inclusive). This method will also return the Snapshot at LSN B.

**`commit(Snapshot LSN, segments)`**  
Commit a new Snapshot to the Volume at the next LSN. Returns the committed Snapshot metadata or an error.

**`checkpoint(Snapshot LSN, added segments)`**  
Inform the MetaStore that a new checkpoint has been created at a particular LSN. If multiple segments are generated, they will represent non-overlapping offset ranges. Will be stored in the log at the next LSN.

## Checkpointing
Checkpointing a Volume involves picking a LSN to checkpoint at and producing a complete image of the Volume at that LSN. It replaces any single-LSN segments at the specified LSN for the Volume.

Checkpoints are created whenever a configurable amount of data has changed since the last checkpoint. The MetaStore issues and monitors checkpoint jobs. The jobs run in the background on the PageStore.

Checkpoint Job Algorithm:
```
1. retrieve metadata of last checkpoint (may be multiple segments)
2. retrieve metadata of all changes since checkpoint
3. for any unchanged segments from previous checkpoint, simply re-emit those segments at the checkpoint LSN
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

The writePages request will fail if the client submits the same offset multiple times. This ensures that every segment generated by a request does not intersect.

Newly written segments may be cached on disk, but not added to the Segment index. This is because the pagestore doesn't yet know if the Segments have been accepted by the MetaStore, and additionally doesn't know their assigned LSN.

## PageStore internal dataflow

https://app.excalidraw.com/s/65i7nRDHAIV/1GVOEpCLvJ0

# Control Plane
The Control Plane manages Volumes, Segments, and API Keys. All data is stored in a centralized (but globally available) D1 database and exposed via a Worker based API.

## Volume Management
Volumes are managed through the Control Plane which recusively communicates with the Volume's Durable Object as needed. The Control Plane stores information about every volume in its database.

## Segment Management
After the MetaStore commits new segments it's responsible for replicating added segments to the Control Plane. This can be done async on a background timer.

## Checkpointing
We only need to keep around a certain amount of history for each Volume.

Checkpointing should trigger once we can build single volume segments composed of 8-16 MB of data or when we have enough commits. A Volume larger than this amount will be split into multiple Segments.

We can keep around multiple checkpoints for a Volume in order to support some configurable amount of history. The Metastore is responsible for truncating its history based on the volume configuration. It will inform the control plane of removed segments to allow GC to eventually trigger once the segments have no references.

## Garbage Collection
As the MetaStore informs the Control Plane of removed Segments, once a Segment is not referenced by any Volume it can be deleted. We may want to delay actual deletion via a grace period until we gain confidence in the correctness of the system.

## API Keys
For now we will proceed without authentication. Eventually, the Control Plane will manage API keys, and associate them with Organizations. Authentication across the distributed system will be handled via Signed Tokens to ensure that the PageStore and MetaStore can validate tokens without centralized communication.

# Client
Graft Clients support reading and writing to Volumes.

## Initialization
To start, a Client must open a Volume at either a specific or the latest LSN. This entails sending a `getSnapshotMetadata` request to the MetaStore. If the Client has cached state for this Volume, it can instead run `pullOffsets(last LSN)` to become aware of changed offsets while retrieving the snapshot.

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