Next: SQLite extension

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
