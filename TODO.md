Now: Client Runtime
- storage
- remote reads/writes
- prefetcher
  - do we need an index tracking which offsets we have for the latest snapshot? if not, how does the prefetcher avoid re-fetching offsets we already have? or more generally, how can we avoid refetching efficiently?
- subscriptions
  - rather than N background tasks, consider one background task and a set of volumes we are subscribed to. For now we can refresh all of them at a set interval. Eventually we might want to use a websocket or long polling to handle this.

Upcoming:
- consider switching last_offset to length for a more ergonomic API
- consider switching pagestore to websockets or http streaming bodies
- end to end testing framework
- garbage collection
- authentication (api keys)

# local storage & syncing

- storage will separate remotely committed pages from locally committed pages.
  - either via a key prefix, or two fjall partitions
- local commits will have a local lsn
- the metastore will support a opaque idempotency token stored on each snapshot
  - this is used to prevent duplicate commits
  - we reject a commit based on idempotency when the idempotency token matches any concurrent snapshot (for now concurrency is fixed to 1)
  - for maximum safety idempotency tokens should be Gid's

read transaction
  take snapshot at the latest remote lsn and the latest local lsn
  each read operation needs to first scan the local commits for the offset and then the remote commits
  when encountering a missing page we can back fill it at its correct lsn

write transaction
  take single writer lock on volume id
  allocate a new local lsn
  create a fjall write txn
  take snapshot of remote lsn
  writes write to the fjall txn
  reads resolve through the fjall txn
    against both the local and remote page store
  after a successful commit we release the lock on the volume id which should also allow the sync process to acquire that local lsn

sync
  determine the range of local lsns to sync
    if recovering from a previously interrupted sync
      -> read the stored sync range and idempotency token
    otherwise store a new sync range and idempotency token
  create a write transaction to move pages
  for each page in the sync range:
    write to the pagestore
    move the page to the expected lsn (removing it from local pages)
  commit segments to the metastore
  on remote commit success:
    write the new volume snapshot
    commit the local txn
  crash safety:
    if we crash between committing remotely and committing locally, when we recover we will need to first pull the latest remote snapshot before starting the sync process. when we pull the latest snapshot:
    -> if it is the expected next lsn and contains our idempotency token then we know that our sync completed, and we can complete the sync by moving pages from the local store.
    -> if it's still the previous lsn, then we can discard all sync state and start from scratch
    -> if it's an unexpected lsn and idempotency token, then someone else has committed. if we have unsynced pages this is a fatal error.
      in the future we may upgrade this to either rollback and notify the user code, or when we build mvcc we will probably need to return all relevant idempotency tokens  