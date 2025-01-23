Todo:
- fix bugs in new sync code

Next: Client Runtime
- sync recovery
- prefetcher
  - do we need an index tracking which offsets we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching offsets we already have? or more generally, how can we avoid refetching efficiently?
- update virtual file module to use the new runtime code

Then:
- SQLite extension
- consider switching pagestore to websockets or http streaming bodies
- garbage collection
- authentication (api keys)

# Storage commit lock and snapshots

Currently we store four snapshots per volume in local storage. Some issues:
1. we don't consistently manage the commit lock. we should be taking it every time we touch snapshots at the very least. And we should be minimizing the lock scope each time.
2. it's still not completely clear what all the snapshot states are... perhaps this needs to be simplified or encoded into the typesystem?

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

# local storage & syncing

Graft clients can choose whether they want to sync or async commit to the remote Volume. A sync commit will block until it's fully flushed and committed to the remote. An async commit will commit locally with full RYOW support for local transactions, and sync to the server at a later point. Multiple local commits may result in a single server commit. Because of this, clients deal in local LSNs which do not correlate 1-1 with remote LSNs.

For now we are using Fjall as our client storage layer. We allocate three Fjall partitions:

```
volumes:
  This maps each VolumeId to a set of snapshots used to track the volume's state:
    local: the latest local snapshot, updated by writes
    sync: the last local snapshot synced to the server
    remote: the latest remote snapshot
    checkpoint: the latest remote checkpoint

pages:
  This maps from (VolumeId, Offset, LSN) to a PageValue.
  PageValue considers empty values to be pending pages.
  The LSN is local to this client.

commits:
  This stores metadata for each local commit that has yet to sync to the server.
  (VolumeId, LSN) -> Splinter of all modified offsets by this commit

read snapshot:
  Take a copy of the latest Volume snapshot from the volumes partition.
  To read a page, the client opens a reverse iterater on the pages partition starting from the snapshot local lsn and returns the first matched page.
  If the client encounters a PendingPage, it fetches the page:
    Query the prefetcher for additional pages to fetch
      this returns a list of page partition keys
    Request page offsets at the snapshot's remote lsn.
    Write pages to the partition key we retrieved earlier - this ensures we are writing to the correct local lsn.

write transaction
  Take a copy of the latest Volume snapshot from the volumes partition.
  take single writer lock on volume id
    allocate a new local lsn, add to our read snapshot
  To write a page we write to a memtable
  To read a page, we read from the memtable and then the read snapshot
  To commit the transaction
    create a fjall batch
    write out our memtable
    update the volume snapshot
    write out a changed offsets splinter to the commits partition
    commit the batch

sync from local to remote
  take a read snapshot
  gather all commits between the last synced LSN and the latest local LSN
  update volume/sync snapshot to the latest local LSN
  start a fjall batch
  for each commit:
    flush the commit's pages to the Graft Pagestore
    remove the commit (in the batch)
  commit segments to the Graft Metastore
  on success
    update volume/remote Snapshot to the volume snapshot (in the batch)
    commit the batch
  on concurrent write failure:
    this means that someone else has written to the volume concurrently
    for now we just crash
  on transient failure:
    retry a few times, before aborting and trying again later
  crash recovery:
    if the graft client crashes during the sync process before we are able to commit locally, we will need to recover.
    we can detect recovery is needed at boot by checking if there are any local commits earlier than the volume/sync snapshot
    for any volume that needs recovery:
      request the latest remote snapshot
      if local.remote_lsn != remote.lsn && local.client_id == remote.client_id:
        the remote snapshot has changed due to our commit
        cleanup local commits up to the sync snapshot
      else if client ids don't match:
        someone else committed, possibly after our commit
        to figure this out we would need to ask the server to determine if our commit landed, this would require another server endpoint, or for pull offsets to return all relevant client ids. for now we will just crash
      else if remote lsn matches local remote lsn:
        in this case, our last sync did not go through
        restart the sync process
```

## Receiving a remote commit

When we pull from the server, we create a new local LSN corresponding to the remote LSN we receive. We want to ensure that we don't round trip this LSN back to the server as a new commit, which would create an infinite loop between client and server. It's also wasteful.

One way to solve this is by nooping the push. We should be able to detect this state since the remote commit will not correspond to any commits in the commits partition.

The other way to solve this is to fast-forward the sync watermarks. This makes sense since the Pull will have to check to see if we have local outstanding commits anyways. If we do, the pull should reject since it has no current way of merging remote and local changes. Once it validates that the local state is clean, it can safely commit along with fast forwarding the sync watermarks.

## Sync watermarks

The runtime will track two LSNs corresponding to the current push sync state:
- last_sync: the last local LSN that was successfully pushed
- pending_sync: the last local LSN that attempted to push

Invariants:
- last_sync <= pending_sync
- pending_sync <= local
- (implies: last_sync <= local)

States:
- last_sync < pending_sync && no active sync job: volume needs recovery, other sync jobs paused
- last_sync < local && last_sync == pending_sync: volume has pending commits to sync

The PushJob updates both watermarks in the following way:
- assert that last_sync == pending_sync
- set pending_sync = local
- sync_range = last_sync..=local
- iterate commits
  - assert that all commits are present in the sync range
  - push changed pages to the page store
- commit to the metastore
  - success: delete synced commits, set last_sync = pending_sync
  - failure: set pending_sync = last_sync
  - crash: will leave volume in needs-recovery state

The Sync task needs to generate recovery jobs instead of other jobs whenever it detects recovery is needed. The recovery job will have to reconcile with the server to determine if the commit did actually go through or not. Using this information the the partial sync can be completed like normal.

Tasks:
- Figure out where we are storing the sync watermarks
- update the push job
- update job gen to check for sync watermarks and detect volume state

## Volume Write/Replicate example

```
create volume
write 10 times:
  offsets: 0, 1
  local lsn = 9
  remote lsn = 0
pull offsets: (someone else wrote)
  offsets: 0, 1
  local lsn = 10 <- we write pending marks at this lsn
  remote lsn = 1
write 10 times:
  offsets: 2, 3
  local lsn = 19
  remote lsn = 2
pull offsets: (someone else wrote)
  offsets: 2, 3
  local lsn = 20 <- we write pending marks at this lsn
  remote lsn = 3
read:
  snapshot: local = 20, remote = 3
  all offsets are pending
  if we read offset = 0
    we get pending mark at local lsn 10
      prefetch
        adds offset 1 @ local lsn 10
        adds offset 2 @ local lsn 20
      we read pages at remote lsn 3, offset = 0, 1, 2
        page @ offset=0, lsn=1
          write at local lsn 10
        page @ offset=1, lsn=1
          write at local lsn 10
        page @ offset=2, lsn=3
          write at local lsn 20
```

# Checkpointing

The current impl of Checkpointing is incomplete and inconsistent. It's also very expensive as it invalidates every offset in a volume to downstream readers.

The idea of checkpointing is to periodically truncate the offset log prefix to allow unused page versions to be deleted.

Another way to think about it is that it's a watermark LSN, such that we only keep the last page version per-offset that existed prior to the watermark.

Knowing this watermark LSN is sufficient for clients to do GC. They can read the page count at the watermark LSN to also truncate volume size.

However, on the server side it's a bit more tricky. We need to effectively know which segments we can delete. This is done via ref-counting. Every volume holds a ref on every segment that contains at least one alive page for the volume. When we checkpoint, we want to remove these refs for pages that are no longer valid.

However, this is also doable using the watermark and page count. We simply need to scan the log and remove refs on any expired segments.

The summary of all this, is that when we checkpoint we just need to update the checkpoint LSN in the metastore for the volume. In addition, this process should probably be automatic and configured per volume rather than part of the API.
