---
title: Client
description: Details on how Graft clients work.
---

Graft Clients support reading and writing to Volumes.

## Local Storage

Graft client uses [Fjall], an embeddable rust key-value store based on LSM trees, for local storage. Graft splits up the data between three [Fjall] partitions with the following key layout and value types:

```
volumes:
  {vid}/config          -> VolumeConfig
  {vid}/status          -> VolumeStatus
  {vid}/snapshot        -> Snapshot
  {vid}/watermarks      -> Watermarks

pages:
  {vid}/{pageidx}/{LSN} -> PageValue

commits:
  {vid}/{LSN}           -> Graft

VolumeConfig:
  sync: Disabled | Push | Pull | Both

VolumeStatus: Ok | RejectedCommit | Conflict

Snapshot:
  local: LSN
  remote: RemoteMapping
  pages: PageCount

RemoteMapping:
  Unmapped
  Mapped {
    remote: LSN,
    local: LSN
  }

Watermarks:
  pending_sync: Option<LSN>
  checkpoint: Option<LSN>

PageValue:
  Pending
  Empty
  Available(Page)

Graft:
  Splinter of all PageIdxs changed in the commit
```

## Reading

To issue a local read against a Volume snapshot:

1. Lookup the latest page in storage such that `page.LSN <= snapshot.local`

   - If this page is either Available or Empty return the page

2. If `snapshot.remote` is empty, return an empty page

3. Request the page from the Pagestore

   - This may be batched along with prefetches

4. Save the requested page into storage at `page.LSN`

## Writing

Writes commit locally and then are asynchronously committed remotely. This section only deals with the local commit.

Writes go through a `VolumeWriter` which buffers newly written pages in a memtable. Reads check the memtable to enable RYOW before falling back to the regular Read algorithm. Each `VolumeWriter` is pinned to a Snapshot.

The commit process happens atomically via a [Fjall] batch.

1. Set `commit_lsn = snapshot.local.next()`
2. Persist the memtable at `commit_lsn`
3. Write out a Graft to the commits partition at `commit_lsn`
4. Take the local commit lock
5. Set `latest` to the latest volume Snapshot
6. Fail if `latest.local != snapshot.local`
7. Write out the new snapshot (without changing the remote mapping)
8. Commit the [Fjall] batch
9. release the commit lock

## Sync

The Graft Client runtime supports asynchronously pushing and pulling from the server. Since this process happens out of band, two writers committing to the same Volume will frequently conflict and will need to rebase or reset to continue.

Future work:

- synchronous commit+push to make conflicts easier to detect
- MVCC automatic conflict resolution
- Rebase conflict resolution

### Sync: Pull

The Graft runtime polls `/metastore/v1/pull_graft` for changes. When a change is detected, the runtime attempts to "accept" the change.

The pull process happens atomically via a [Fjall] batch.

1. Take the local commit lock
2. Read the latest Volume Snapshot and Watermarks
3. If `remote_mapping.local < pending_sync`: FAIL with `VolumeNeedsRecovery`
4. If `remote_mapping.local < snapshot.local`: FAIL with `RemoteConflict`

   - set Volume status to `VolumeStatus::Conflict`

5. Set `commit_lsn = snapshot.local.next()`
6. Update the snapshot

   - `local=commit_lsn, remote=(remote_lsn, commit_lsn), pages=remote_pages`

7. Update the watermarks

   - `pending_sync=commit_lsn`

8. For each changed pageidx in the remote commit, write out `PageValue::Pending` into the pages partition using `commit_lsn`. This ensures that future reads know to fetch the page from the PageStore.
9. Commit the [Fjall] batch
10. release the commit lock

**FAIL states:**

- `VolumeNeedsRecovery`: This means that we had previously crashed in the middle of pushing the Volume to the server. The client needs to recover or reset the volume before continuing.

- `Conflict`: This means that we have made local commits since the last successful sync. The client needs to reconcile with the server before continuing.

### Sync: Push

When the Graft runtime detects a local commit has occurred, it tries to push the commit to the server.

1. Take the local commit lock
2. Read the latest Volume Snapshot and Watermarks
3. If `remote_mapping.local < pending_sync`: FAIL with `VolumeNeedsRecovery`
4. update `watermarks.pending_sync` to `snapshot.local`
5. calculate the LSN range to push:

   - `start_lsn = remote_mapping.local.next()`
   - `end_lsn = snapshot.local`

6. release the local commit lock
7. iterate through the local commit splinters

   - send the most recent page for each pageidx to the pagestore
   - collect new segments

8. commit the segments to the metastore
9. take the local commit lock

**On commit success:**

1. Open a [Fjall] batch
2. Read the latest Volume Snapshot and Watermarks
3. Assert that the new remote LSN is larger than the last remote LSN
4. Assert that `watermarks.pending_sync == snapshot.local`
5. Update the snapshot's remote mapping to `(remote_lsn, snapshot.local)`
6. Remove all successfully synced commit grafts
7. Commit the batch
8. Release the local commit lock

**On commit failure:**

1. Update `watermarks.pending_sync = snapshot.remote_mapping.local`
2. Set Volume status to `VolumeStatus::RejectedCommit`

## Crash recovery

The Graft client runtime must be able to crash at any point and recover. [Fjall] already has its own recovery mechanisms built in, so we just need to handle failed Pushes. Failed pushes can be detected when `pending_sync` is larger than `remote_mapping.local` and no concurrent Push job is running.

When a volume is in this failed push state, it needs to determine if the commit was successfully accepted by the Metastore or not. It does so by retrying the commit process with the same idempotency token.

## Lite Client

In some cases, a Client may want to boot without any state and quickly read (+ possibly write) to a particular Volume snapshot. In the most minimal case, if the client already knows the LSN of the snapshot they want to access, they can read from the Page Server immediately. If they want to issue a write, they will need to read the latest snapshot to get the page count and current remote LSN.

Supporting Lite Clients is desirable to help enable edge serverless workloads which want to optimize for latency and have no cached state.

[Fjall]: https://github.com/fjall-rs/fjall
