---
title: Architecture
description: An overview of how Graft works
sidebar:
  order: 0
---

Graft is a transactional storage engine designed for **lazy, partial replication** to the edge. It provides strong consistency with object storage durability, making it ideal for syncing data across distributed environments.

## High-Level Architecture

```
┌─────────────────────────────────────────┐
│    SQLite Extension (libgraft)          │
│    VFS + Pragma Interface               │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│        Runtime (graft-kernel)           │
│  • Tag Management                       │
│  • Volume Management                    │
│  • VolumeReader/Writer                  │
│  • Sync Operations (pull/push/fetch)    │
└────────────────┬────────────────────────┘
                 │
      ┌──────────┴───────────┐
      │                      │
┌─────▼──────────┐   ┌───────▼───────────┐
│ FjallStorage   │   │ Remote Storage    │
│   (Local)      │   │ (S3/FS/Memory)    │
│                │   │                   │
│ LSM Partitions │   │ - checkpoints     │
│ - tags         │   │ - commits         │
│ - volumes      │   │ - segments        │
│ - log          │   │                   │
│ - pages        │   │                   │
└────────────────┘   └───────────────────┘
```
