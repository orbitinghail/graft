# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] structured logging
- [x] RecoverPendingCommit job
- [x] RuntimeHandle::read_page
- [x] Runtime::create_volume_from_remote
- [ ] comprehensive tests
- [ ] libgraft SQLite

# Working on SQLite v2

- [ ] tag handle should probably not cache the graft id - it's easy to get out of sync (see test_sync_and_reset)

- [ ] build delete\* methods for managing tags and grafts
- [ ] how to recover from a remote volume disappearing? (or switching remotes)
      -> currently you need to hydrate before it goes away, then fork, then push
- [ ] build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- [ ] consider adding a read oracle (do some perf testing)
- [ ] port tests
- [ ] write first draft of antithesis tests

## done

- [x] more robust sync
- [x] rename RuntimeHandle to Runtime
- [x] BUG: graft push should fail if push fails due to divergence
      graft_push output: Pushed LSNs unknown from local Volume 5rMJii2Nik-2dv7ZBHJUXDov to remote Volume 5rMJii2Ndd-2dodwccLe9PQf @ 1
