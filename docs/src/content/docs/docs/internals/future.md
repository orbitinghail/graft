---
title: Future work
description: What's next for Graft development?
---

This page documents the direction of Graft development. As ideas on this page are made more concrete they will be migrated to GitHub issues and scheduled for development. Thus, if you'd like to look at what's being worked on, visit the [Graft Issue Tracker].

[Graft Issue Tracker]: https://github.com/orbitinghail/graft/issues

## WebAssembly support

Supporting WebAssembly (Wasm) would allow Graft to be used in the browser. I’d like to eventually support SQLite’s official Wasm build, wa-sqlite, and sql.js.

## Integrating Graft and SQLSync

Once Graft supports Wasm, integrating it with [SQLSync] will be straightforward. The plan is to split out SQLSync’s mutation, rebase, and query subscription layers so it can lay on top of a database using Graft replication.

[SQLSync]: https://sqlsync.dev

## Conflict handling

Graft should offer built-in conflict resolution strategies and extension points so applications can control how conflicts are handled. The initial built-in strategy will automatically merge non-overlapping transactions. While this relaxes global consistency to optimistic snapshot isolation, it can significantly boost performance in collaborative and multiplayer scenarios.

## Low-latency writes

Currently Graft provides high-latency writes at low cost. For some workloads, it may be desirable to tweak this relationship and pay higher cost for lower latency. To do this we will need a durable storage layer with lower latency than object storage.

This can be addressed in a number of ways:

- Experiment with S3 express zone
- Buffer writes in a low-latency durable consensus group sitting in front of object storage.

## Variable sized pages

I am very curious how much impact variable sized pages would be to Graft adoption. Currently pages are exactly 4KiB which will likely limit workloads. We could implement variable length pages in one of two ways:

1. Each page is variable. This is the most flexible option, allowing Graft to be used to replicate lists of things for example.

2. Each Volume's page size can be configured at creation. This is less flexible as it still restricts use cases, however it is more flexible than the current setup. It would likely allow Graft more optimization flexibility in storage.

## Garbage collection, checkpointing, and compaction

These features are needed to maximize query performance, minimize wasted space, and enable deleting data permanently.

## Page deltas

Currently we store every page directly in a Segment. This wastes a ton of space as most page changes are extremely small. When Segments store multiple versions of each page, they will naturally compress well, however this doesn't help out with pages stored in different segments.

One approach is to store XOR deltas rather than full pages. For pages that haven't changed much, a XOR delta will be mostly zeros and thus compress extremely well. The tradeoff is that to reproduce the page we will need to look up the base page as well as the delta.

This also adds complexity to GC, as a base page can't be deleted until all deltas that use it are also unused.

One solution to these issues is to always base XOR deltas off the last checkpoint. Thus a writer only needs to retrieve one segment (the portion of the checkpoint containing the PageIdx in question) and can quickly decide if storing a XOR delta is worthwhile (i.e. 0s out X% of the bytes). GC thus knows that a checkpoint can't be deleted until no snapshots exist between the checkpoint and the subsequent one.

For XOR delta compression to work we also need to remove the runs of zeros in the resulting segment. We can either leverage a generic compression library when uploading/downloading the segment, or we can employ RLE/Sparse compression on each page to simply strip out all the zeros. Or compress each page with something like LZ to strip out patterns. Notably this will affect read performance as well as potentially affecting our ability to read pages directly via content-range requests.

## Request Hedging

According to the go, hedging requests to blob storage can help dramatically reduce tail latency. For S3, the paper suggests hedging if you haven't received the first byte within 200ms. Slightly more aggressive hedging may also be desirable, like hedging if you haven't completely downloaded the file within 600ms. Making this configurable and testing is important.

[AnyBlob paper]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf

## Performance Optimizations

Once Graft server is sufficiently mature, a series of performance optimization passes should be performed. I'll keep track of relevant blog posts and tools to make this easier here:

- [Compiling with PGO and BOLT]

[Compiling with PGO and BOLT]: https://kobzol.github.io/rust/cargo/2023/07/28/rust-cargo-pgo.html
