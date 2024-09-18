# Future Work <!-- omit from toc -->

This file documents future work that has been punted to help accelerate Graft to a MvP.

# Outline <!-- omit from toc -->

- [Time-Travel and Point in Time Restore](#time-travel-and-point-in-time-restore)
  - [Segment Compaction](#segment-compaction)
- [Page deltas](#page-deltas)

# Time-Travel and Point in Time Restore

Currently, it's only possible to go back in time to fixed checkpoints and recently written LSNs. Returning to an arbitrary LSN or timestamp in the last week or so is not possible since we aggressively checkpoint.

The simplest solution is to simply keep the existing history around longer before checkpointing. The main downside of this approach is that it results in a dramatically large amount of files, and thus decreases search performance.

The main optimization that other bottomless databases take is to merge segments such that they store multiple versions of each page. Thus there are fewer files to query and the files compress well (subsequent page versions usually don't change much).

The downside of this approach is that it makes searching for a particular page version more difficult, adds complexity to segments, and makes the merger do more work.

A key aspect of our design is that segments do not store absolute LSNs. This allows them to be reordered safely by the metastore, as well as reused between checkpoints for unchanged portions of the keyspace. In order to support multiple versions of a page in a segment while also maintaining LSN independence, we will need to store LSNs in Segments as "relative" LSNs, resolving them to absolute LSNs via inspecting the metastore's log.

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

# Page deltas

Currently we store every page directly in a Segment. This wastes a ton of space as most page changes are extremely small. When Segments store multiple versions of each page, they will naturally compress well, however this doesn't help out with pages stored in different segments.

One approach is to store XOR deltas rather than full pages. For pages that haven't changed much, a XOR delta will be mostly zeros and thus compress extremely well. The tradeoff is that to reproduce the page we will need to look up the base page as well as the delta.

This also adds complexity to GC, as a base page can't be deleted until all deltas that use it are also unused.

One solution to these issues is to always base XOR deltas off the last checkpoint. Thus a writer only needs to retrieve one segment (the portion of the checkpoint containing the page offset in question) and can quickly decide if storing a XOR delta is worthwhile (i.e. 0s out X% of the bytes). GC thus knows that a checkpoint can't be deleted until no snapshots exist between the checkpoint and the subsequent one.

For XOR delta compression to work we also need to remove the runs of zeros in the resulting segment. We can either leverage a generic compression library when uploading/downloading the segment, or we can employ RLE/Sparse compression on each page to simply strip out all the zeros. Or compress each page with something like LZ to strip out patterns. Notably this will affect read performance as well as potentially affecting our ability to read pages directly via content-range requests.