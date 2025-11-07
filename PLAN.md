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

- add remote information and full volume id to graft_status
- implement all outstanding pragmas
- default `just run sqlite shell` to use a shared remote on the filesystem
- consider adding a read oracle (do some perf testing)
- port tests
- write first draft of antithesis tests

# New and improved pragmas

`pragma graft_status`
show the local and remote volume status in a git like manner.
something like this:

local changes:

```
On ref main

Your local Volume is 1 commit ahead of the remote.
  (use "graft_push" to push changes)
```

remote changes:

```
On ref main

The remote Volume is 1 commit ahead of the local Volume.
  (use "graft_pull" to pull changes)
```

diverged:

```
On ref main

The local Volume and remote Volumes have diverged,
and have 1 and 2 different commits each, respectively.
```

`pragma graft_fetch`
fetch the remote volume without pulling in changes

`pragma graft_pull`
fetch + pull changes from the remote vid into the local vid

`pragma graft_push`
push local changes to the remote

`pragma graft_autosync = on|off`

enables/disables autosync for this graft

`pragma graft_checkout [= remote_vid[:LSN]]`
if remote_vid is specified, this pragma creates a new graft starting from the remote vid, possibly at a specified LSN.

if remote_vid is not specified, this pragma creates a new GraftRef pointing at the current remote.

also updates the current ref to point at the new graft

returns the new local graft vid

`pragma graft_checkout_empty`
same as graft checkout but creates an empty remote
