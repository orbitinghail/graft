---
title: MetaStore
description: The MetaStore stores Volume metadata.
---

A service which stores Volume metadata including the log of segments per Volume. This service is also responsible for coordinating GC, authn, authz, and background tasks.

## Metastore Storage

The Metastore will store it's data in a key value store. For now we will use object storage directly. Each commit to a volume will be a separate file, stored in a way that makes it easy for downstream consumers to quickly get up to date.

## Storage Layout

```
/volumes/[VolumeId]/[LSN]
  CommitHeader
  list of Segment

CommitHeader
  vid: VolumeId
  meta: CommitMeta

CommitMeta:
  cid: ClientId
  lsn: LSN
  checkpoint_lsn: LSN
  page_count: u32
  timestamp: u64

Segment
  sid: SegmentId
  size: u32
  graft: Splinter (size bytes)
```

To ensure that each volume log sorts correctly, LSNs will need to be fixed length and encoded in a sortable way. The easiest solution is to use 0 padded decimal numbers. However the key size can be compressed if more characters are used. It appears that base58 should sort correctly as long as the resulting string is padded to a consistent length.

## API

[MetaStore API docs](/docs/backend/api#metastore)

## Checkpointing

A Volume checkpoint represents the oldest LSN for which commit history is stored. Requesting commits or pages for LSNs earlier than the checkpoint may result in an error.

Soon after a Volume checkpoint changes, background jobs on the client and server will begin removing orphaned data:

- Remove any commits in Metastore storage older than the checkpoint LSN
- For each removed commit, reduce the refcount on the commit's segments
  -> Garbage Collection will delete segments with refcount=0 later
- Remove all but the most recent page as of the Checkpoint LSN on clients

## Garbage Collection

Once a segment is no longer referenced by any commit it can be deleted. A grace period will be used to provide safety while we gain confidence in the correctness of the system. To do this we can mark a segment for deletion with a timestamp, and then only delete it once the grace period has elapsed.
