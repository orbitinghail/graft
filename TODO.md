Next: SQLite extension

Stack:

- autosync
- migrate sql snapshot tests to rust
- sql sanity tests
- sqlite antithesis workload

Then:

- prefetcher

  - do we need an index tracking which offsets we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching offsets we already have? or more generally, how can we avoid refetching efficiently?

- consider switching pagestore to websockets or http streaming bodies
- garbage collection
- authentication (api keys)

# Client prefetching and overfetching

We want the client to support prefetching whenever it fetches pages from the server. We also want to avoid fetching pages we already have as well as overfetching the same page from multiple concurrent tasks.

For now, we can solve refetching via checking storage for every page we decide to prefetch.

fetch logic:

```
fetcher.fetch(vid, lsn, offset).await
  -> fetcher expands the offset into a offset range using the prefetcher
  -> checks storage to resolve each offset into a specific LSN + state
    -> if an offset is already available, return
    -> otherwise resolve to it's pending LSN
    -> if an offset is completely missing then resolve the offset to the request LSN and potentially add a pending token to storage
  -> then inspects concurrently active tokens for overlap
  -> creates new tokens for non-overlapping ranges
  -> constructs a request that will resolve once all relevant tokens resolve

```

Detecing overlap between tokens is not trivial to do perfectly. The issue stems from two concurrent requests for the same offsets in different LSNs. In this case, if the offsets didn't change between the two LSNs, we will fetch the same page multiple times. Need to think about how likely this will be in my primary use cases.

# Prefetching algorithm

The goal is identify scans and frequently requested pages at runtime in order to fetch larger and larger amounts of the underlying Volume to amortize round trips to the server.

TODO: Investigate mvSQLite's relative offset history cache

# Variable sized pages idea

I am very curious how much impact variable sized pages would be to Graft adoption. Currently pages are exactly 4KiB which will likely limit workloads. We could implement variable length pages in one of two ways:

1. Each page is variable. This is the most flexible option, allowing Graft to be used to replicate lists of things for example.

2. Each Volume's page size can be configured at creation. This is less flexible as it still restricts use cases, however it is more flexible than the current setup. It would likely allow Graft more optimization flexibility in storage.

The primary downside of either approach is complexity. It also starts to beg the question of whether Graft should just offer a non-paged abstraction layer that internally maps onto pages.

Another downside is the current offset size limit of 24 bits. This decision may need to be revisited if pages become much smaller and potentially more numerous.

I'm leaning towards building (1) as it feels like a reasonable lift from the current design.
