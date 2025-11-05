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

- PRIORITY: allow specifying a remote vid when opening a volume
- add remote information and full volume id to graft_status
- implement rest of the pragmas
- default `just run sqlite shell` to use a shared remote on the filesystem
- consider adding a read oracle (do some perf testing)
- port tests
- write first draft of antithesis tests
