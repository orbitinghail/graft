---
title: FAQ
description: Frequently asked questions
---

## Why the name Graft?

**Graft** is inspired by the idea of combining and growing data structures the same way a botanist grafts branches from one plant onto another. In Graft, data can be replicated or merged at the page level, instead of needing to copy entire volumes. The name reflects how Graft enables partial, efficient, and flexible synchronization of data across the edge.

## Can I use Graft with something other than SQLite?

Yes. Graft is a **general-purpose transactional storage engine**. While one of its first integrations is as a backing store for SQLite via a lightweight extension, Graft is designed to support many use cases: embedded databases, custom formats, distributed systems, and more. You can build directly on Graft's core APIs to store and sync structured or unstructured data beyond SQL.

## Why is Graft built using Rust?

Rust provides **memory safety, strong concurrency support, and predictable performance** — all without needing a garbage collector. These properties make it ideal for building a storage engine that must be safe, scalable, and embeddable across many environments, from cloud servers to edge devices. Rust also makes it easier to enforce correctness in complex transactional and replication logic, which is critical for Graft's design goals.

## Is Graft free?

Yes. Graft is open source, released under a dual license: MIT or Apache 2.0. You are free to use, modify, and integrate it into your own projects, whether personal or commercial. We also plan to offer hosted services and support plans for teams that want managed Graft infrastructure or deeper integration help — but the core engine is and will remain free and open.
