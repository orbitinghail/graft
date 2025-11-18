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
- [x] BUG: graft push should fail if push fails due to divergence
      graft_push output: Pushed LSNs unknown from local Volume 5rMJii2Nik-2dv7ZBHJUXDov to remote Volume 5rMJii2Ndd-2dodwccLe9PQf @ 1
- [ ] how to recover from a remote volume disappearing? (or switching remotes)
      -> currently you need to hydrate before it goes away, then fork, then push
- [ ] build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- [ ] consider adding a read oracle (do some perf testing)
- [ ] port tests
- [ ] write first draft of antithesis tests

# more robust sync system

Currently we have a collection of jobs which can be used to manually sync or automatically sync.

However, automatic sync has a lot of issues, especially when it encounters errors.

- an error blocks other grafts from syncing
- the sync system doesn't know how to dig itself out of situations like divergence

Proposed solution:

1. decouple fetching the remote from pushing/pulling. This makes divergence detectable rather than being an ephemeral state.
2. run jobs on diff grafts in parallel
3. remove recovery job, and put logic in remote_commit instead
4. make sure that divergence is easy to detect from status
