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

- [ ] add remote information and full volume id to graft_status
- [ ] implement all outstanding pragmas
- [x] default `just run sqlite shell` to use a shared remote on the filesystem
- [ ] consider adding a read oracle (do some perf testing)
- [ ] port tests
- [ ] write first draft of antithesis tests

# New and improved pragmas

[x] `pragma graft_status`
[ ] `pragma graft_fetch`
fetch the remote volume without pulling in changes

[ ] `pragma graft_pull`
fetch + pull changes from the remote vid into the local vid

[ ] `pragma graft_push`
push local changes to the remote

[ ] `pragma graft_autosync = on|off`

enables/disables autosync for this graft

[ ] `pragma graft_checkout [= remote_vid[:LSN]]`
if remote_vid is specified, this pragma creates a new graft starting from the remote vid, possibly at a specified LSN.

if remote_vid is not specified, this pragma creates a new GraftRef pointing at the current remote.

also updates the current ref to point at the new graft

returns the new local graft vid

[ ] `pragma graft_checkout_empty`
same as graft checkout but creates an empty remote
