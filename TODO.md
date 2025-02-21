Stack:

- rename all instances of "offset" to "pageidx" and "offsets" to "graft" or "PageIdxs"
- prefetcher

  - do we need an index tracking which PageIdxs we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching PageIdxs we already have? or more generally, how can we avoid refetching efficiently?

- sqlite antithesis workload
- currently we inconsistently use magic numbers and sometimes format versions
  - we also don't use checksums
  - consider standardizing magic+version, and adopting checksums
- garbage collection
- consider switching pagestore to websockets or http streaming bodies
- authentication (api keys)

# Client prefetching and overfetching

We want the client to support prefetching whenever it fetches pages from the server. We also want to avoid fetching pages we already have as well as overfetching the same page from multiple concurrent tasks.

For now, we can solve refetching via checking storage for every page we decide to prefetch.

fetch logic:

```
fetcher.fetch(vid, lsn, pageidx).await
  -> fetcher expands the pageidx into a graft using the prefetcher
  -> checks storage to resolve each pageidx into a specific LSN + state
    -> if an pageidx is already available, return
    -> otherwise resolve to it's pending LSN
    -> if an pageidx is completely missing then resolve the pageidx to the request LSN and potentially add a pending token to storage
  -> then inspects concurrently active tokens for overlap
  -> creates new tokens for non-overlapping ranges
  -> constructs a request that will resolve once all relevant tokens resolve

```

Detecing overlap between tokens is not trivial to do perfectly. The issue stems from two concurrent requests for the same pageidxs in different LSNs. In this case, if the pageidxs didn't change between the two LSNs, we will fetch the same page multiple times. Need to think about how likely this will be in my primary use cases.

# Prefetching algorithm

The goal is identify scans and frequently requested pages at runtime in order to fetch larger and larger amounts of the underlying Volume to amortize round trips to the server.

TODO: Investigate mvSQLite's relative pageidx history cache

# Variable sized pages idea

I am very curious how much impact variable sized pages would be to Graft adoption. Currently pages are exactly 4KiB which will likely limit workloads. We could implement variable length pages in one of two ways:

1. Each page is variable. This is the most flexible option, allowing Graft to be used to replicate lists of things for example.

2. Each Volume's page size can be configured at creation. This is less flexible as it still restricts use cases, however it is more flexible than the current setup. It would likely allow Graft more optimization flexibility in storage.

The primary downside of either approach is complexity. It also starts to beg the question of whether Graft should just offer a non-paged abstraction layer that internally maps onto pages.

I'm leaning towards building (1) as it feels like a reasonable lift from the current design.
