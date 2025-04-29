---
title: Introduction
description: An overview of Graft
---

**Graft** is an open-source transactional storage engine designed for efficient data synchronization at the edge. It supports lazy, partial replication with strong consistency, ensuring applications replicate only the data they need.

**Core Benefits:**

- **Lazy Replication**: Clients sync data on demand, saving network and compute.
- **Partial Replication**: Minimize bandwidth by syncing only required data.
- **Edge Optimization**: Lightweight client designed for edge, mobile, and embedded environments.
- **Strong Consistency**: Serializable Snapshot Isolation ensures correct, consistent data views.
- **Transactional Object Storage**: Graft turns object storage into a transactional system—supporting consistent updates to subsets of data at page granularity, without imposing any data format or schema.
- **Instant Read Replicas**: Decoupled metadata and data allow replicas to spin up immediately—no replay, no waiting for full recovery.

**Use Cases:**

- Offline-first and mobile applications
- Cross-platform synchronization
- Stateless replicas for serverless or embedded environments
- Diverse data replication scenarios
- Storage and replication for databases
