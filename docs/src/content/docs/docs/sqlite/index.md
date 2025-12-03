---
title: Overview
description: Overview of the Graft SQLite extension
---

The Graft SQLite extension is a native SQLite extension that works anywhere SQLite does. It uses Graft to replicate just the parts of the database that a client actually uses, making it possible to run SQLite in resource constrained environments.

The extension implements a SQLite virtual file system (VFS) allowing it to intercept all reads and writes to the database. It provides the same transactional semantics as SQLite does when running in WAL mode. Using the extension provides your application with the following benefits:

- asynchronous replication to object storage
- stateless lazy partial replicas on the edge and in devices
- serializable snapshot isolation
- point-in-time restore
