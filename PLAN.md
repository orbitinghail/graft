# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- structured logging
- RecoverPendingCommit job
- RuntimeHandle::read_page
- Runtime::create_volume_from_remote
- comprehensive tests
- libgraft SQLite

# Working on SQLite v2

- build delete\* methods for managing tags and grafts
- how to recover from a remote volume disappearing? (or switching remotes)
  - currently you need to hydrate before it goes away, then fork, then push
- build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- consider adding a read oracle (do some perf testing)
- create some hello-world examples of using Graft without SQLite
- port tests
- write first draft of antithesis tests

# The new taxonomy and actions

Tag -> Graft -> Volume

A **Volume** represents a sparse ordered set of pages over time. A volume is identified by a VolumeId and represents time as a LSN which is a particular sequence number in the Volume's commit log. Every unique (VolumeId, LSN) pair represents a consistent snapshot of the Volume.

A **Graft** tracks the sync state between two volumes, one local and one remote. A Graft is identified by its local VolumeId. A Graft allows writes to be written optimistically to the local volume and asynchronously collapsed + synced to the remote volume. A graft also allows a remote volume to be fetched and the changes then pulled into the local volume.

A **Tag** is a mutable string that points at a Graft (by its local VolumeId).

**Decision**: remove TagHandle, move all methods to Runtime, and have apps interact with Tags and Grafts directly.

- [x] implement methods on Runtime
- [x] rename volumereader/writer to graft\*
- [x] remove tag handle
- [ ] sync_remote_to_local should just fast forward the sync point, no need to copy commits
- [x] fixup sqlite extension
