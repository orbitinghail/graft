---
title: Volumes
description: An overview of Graft Volumes
---

Graft organizes data into **Volumes**.

## What is a Volume?

A **Volume** in Graft is a sparse, append-only collection of fixed-size 4 KiB pages. Pages are indexed starting at 1, and not every PageIdx needs to be filled—Volumes are inherently sparse. Applications can write to any PageIdx, and the Volume will automatically grow to accommodate the highest written index.

Volumes are highly flexible and can represent:

- Entire databases (e.g., a full SQLite database)
- Subsets of data (e.g., a shard or a logical table)
- Any sparse or partially replicated dataset

Volumes support **lazy**, **partial** replication: clients can track only a subset of Pages without downloading or storing the entire Volume.

## What is a Volume ID?

Each Volume is uniquely identified by a **Volume ID**, which is a 16 byte **[Graft Identifier (GID)]**.

> **Note**: Graft Volume IDs are similar in spirit to [ULID]s — they embed timestamp information — but they use a custom, compact encoding tailored for Graft.

[Graft Identifier (GID)]: /docs/internals/gid/
[ULID]: https://github.com/ulid/spec

Here is an example serialized Volume ID:

```
5rMJkYma1s-2da5Kmp3uLKEs
```

## Local vs. Remote Volumes

In Graft's replication system, volumes have two roles:

- **Remote Volume**: The source of truth, stored in object storage (S3, filesystem, etc.). This is the authoritative version that multiple clients can sync with.
- **Local Volume**: Your working copy, cached and staged locally on your device. This is where you read and write data.

A **Graft** pairs a local volume with a remote volume, enabling:
- **Pulling** changes from remote to local
- **Pushing** changes from local to remote
- **Working offline** with local data
- **Lazy loading** of pages on demand

Think of it like git: the remote volume is like a GitHub repository, and your local volume is like your local clone.

## Volume Tags

Rather than using Volume IDs directly, you can assign human-readable **tags** to grafts. Tags make it easy to manage multiple databases without memorizing long IDs.

```sql
-- List all tags
pragma graft_tags;

-- Switch to a tagged graft
pragma graft_switch = "main";
```

For example, you might have tags like:
- `main` - Your primary production database
- `staging` - Staging environment
- `feature-xyz` - A feature branch

Tags are stored as simple name → VolumeId mappings and make database management much more intuitive.
