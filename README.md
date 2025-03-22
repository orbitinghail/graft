<h1 align="center">Graft</h1>
<p align="center">
  <a href="https://docs.rs/graft-client"><img alt="docs.rs" src="https://img.shields.io/docsrs/graft-client"></a>
  &nbsp;
  <a href="https://crates.io/crates/graft-client"><img alt="crates.io" src="https://img.shields.io/crates/v/graft-client.svg"></a>
  &nbsp;
  <a href="https://github.com/orbitinghail/graft/actions"><img alt="Build Status" src="https://github.com/orbitinghail/graft/actions/workflows/ci.yml/badge.svg"></a>
</p>

Transactional page storage engine supporting lazy partial replication to the edge. Optimized for scale and cost over latency. Leverages object storage for durability.

# Using Graft

Graft should be considered **Alpha** quality software. Thus, don't use it for production workloads yet.

## SQLite extension

The Graft [SQLite] extension should work with any version of SQLite after 3.44.0. It probably works with earlier versions, but no guarantees.

You can download the latest extension dynamic object from [GitHub Releases]. The name of the file is:
...TODO finish this...

[SQLite]: https://www.sqlite.org/index.html
[install-sqlite-ext]: https://antonz.org/install-sqlite-extension/
[GitHub Releases]: https://github.com/orbitinghail/graft/releases/latest

## Rust Crate

Graft can be embedded in your Rust application directly, although for now that is left as an exercise for the reader. You can find the Rust docs here: https://docs.rs/graft-client

## Other languages?

Please [file an issue] if you'd like to use Graft directly from a language other than Rust!

[file an issue]: https://github.com/orbitinghail/graft/issues/new

# Technical Overview

## Consistency Model

_(This model only applies when using the official Graft Client with Graft Server. Third-party client implementations may violate this model.)_

### Global Consistency

Graft provides **[Serializable Snapshot Isolation](https://distributed-computing-musings.com/2022/02/transactions-serializable-snapshot-isolation/)** globally.

All read operations are executed on an isolated snapshot of a Volume.

A write transaction must be based on the latest snapshot to commit. Assuming a compliant Graft client, this enforces [Strict Serializable](https://jepsen.io/consistency/models/strong-serializable).

### Local Consistency:

By default, Graft clients commit locally and then asynchronously attempt to commit remotely. Because Graft enforces **Strict Serializability** globally, when two clients concurrently commit based on the same snapshot, one commit will succeed and the other will fail.

Upon rejection, the client must choose one of:

1. **Fork the volume permanently**: This results in a new volume and retains **Strict Serializability**.
2. **Reset and replay**: Reset to the latest snapshot from the server, replay local transactions, and attempt again.
   - The global consistency remains **Strict Serializable**.
   - Locally, the client experiences **Optimistic Snapshot Isolation**, meaning:
     - Reads always observe internally consistent snapshots.
     - However, these snapshots may later be discarded if the commit is rejected.
3. **Merge**: Attempt to merge the remote snapshot with local commits. _(Not yet implemented by Graft; this degrades global consistency to [snapshot isolation](https://jepsen.io/consistency/models/snapshot-isolation))_

**Optimistic Snapshot Isolation:**

Under optimistic snapshot isolation, a client may observe a snapshot which never exists in the global timeline. Here is an example of this in action:

1. Initial State: `accounts = { id: { bal: 10 } }`

2. client A commits locally:
   `update accounts set bal = bal - 10 where id = 1`

   - SNAPSHOT 1: `accounts = { id: { bal: 0 } }`

3. client B commits locally:
   `update accounts set bal = bal - 5 where id = 1`

   - SNAPSHOT 2: `accounts = { id: { bal: 5 } }`

4. client B allows a read transaction based on SNAPSHOT 2:

   - Reads an optimistic snapshot that's not yet committed to the server.

5. client A successfully commits globally.

6. client B’s global commit is rejected:

   - Client B resets to SNAPSHOT 1: `accounts = { id: { bal: 0 } }`

7. client B replays transaction:
   `update accounts set bal = bal - 5 where id = 1`
   - Commit rejected locally: invariant violated (balance cannot be negative).

At this stage, client B should ideally replay or invalidate the read transaction from step (4). If external state changes were based on that read, the client must perform reconciliation to ensure correctness.
