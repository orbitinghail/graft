Now: Client Runtime
- storage
- remote reads/writes
- prefetcher
  - do we need an index tracking which offsets we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching offsets we already have? or more generally, how can we avoid refetching efficiently?
- subscriptions
  - rather than N background tasks, consider one background task and a set of volumes we are subscribed to. For now we can refresh all of them at a set interval. Eventually we might want to use a websocket or long polling to handle this.

Upcoming:
- consider switching pagestore to websockets or http streaming bodies
- end to end testing framework
- garbage collection
- authentication (api keys)

# client id
A basic form of idempotency should be provided via the commit process taking a client id. This can be used to reject duplicate commits.

# local storage & syncing

Graft clients can choose whether they want to sync or async commit to the remote Volume. A sync commit will block until it's fully flushed and committed to the remote. An async commit will commit locally with full RYOW support for local transactions, and sync to the server at a later point. Multiple local commits may result in a single server commit. Because of this, clients deal in local LSNs which do not correlate 1-1 with remote LSNs.

For now we are using Fjall as our client storage layer. We allocate three Fjall partitions:

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
  take the sync lock if needed
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

# Volume Write/Replicate example

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