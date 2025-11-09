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

- [ ] build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
- [ ] consider adding a read oracle (do some perf testing)
- [ ] port tests
- [ ] write first draft of antithesis tests

# Features needed for SyncConf demo

- [ ] consistency check pragma: blake3 hash of volume
- [x] autosync delay on commit
- [x] fix graft_hydrate
