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
