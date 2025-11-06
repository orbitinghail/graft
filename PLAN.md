# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] structured logging
- [x] RecoverPendingCommit job
- [x] RuntimeHandle::read_page
- [x] Runtime::create_volume_from_remote
- [ ] comprehensive tests
- [ ] libgraft SQLite

# Other changes

- switch Gid pretty to be `(prefix + ts)-(random)` and short to just be random. this will also ensure that it sorts alphanumerically.

# Working on SQLite v2

- PRIORITY: allow specifying a remote vid when opening a volume
- add remote information and full volume id to graft_status
- implement rest of the pragmas
- default `just run sqlite shell` to use a shared remote on the filesystem
- consider adding a read oracle (do some perf testing)
- port tests
- write first draft of antithesis tests

# Explicit volume version control

TLDR;

- GraftRefs are mutable named pointers to Grafts
- Grafts represent a local volume grafted to a remote at a particular remote LSN.
- Remove parent/fork concept, forks will be more expensive (copy commit log), but still pretty cheap, checkpoints become much simpler.
- Snapshots will be constructed explicitly from two LSN ranges (from the local and remote volume). the graft will track this
- local commits are unchanged (advance local volume)
- successful remote commits will advance the base local LSN
  -> we need to writeback the new segment during the commit process
- eventually GC will truncate the prefix of the local volume when no relevant snapshots are open
- when divergence happens, the user will need to create a new graft starting from the most recent remote lsn, leaving the old graft to be gc'ed (no refs pointing at it)

## Terms

A GraftRef is a mutable name that points at a Graft. It can be changed to point at a different Graft.

A Graft is a local staging Volume grafted to a Remote volume.
It allows changes to be pushed and pulled between the local
and remote volume. A Graft's ID is the Local Volume ID. There is a 1-1 relationship between a graft and a local volume.

## graft operations will be explicit, with opt-in autosync

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

enables/disables autosync for this named volume

`pragma graft_checkout [= remote_vid[:LSN]]`
if remote_vid is specified, this pragma creates a new graft starting from the remote vid, possibly at a specified LSN.

if remote_vid is not specified, this pragma creates a new GraftRef pointing at the current remote.

also updates the current ref to point at the new graft

returns the new local graft vid

`pragma graft_checkout_empty`
same as graft checkout but creates an empty remote

## Sync process over time

```
-> Checkout remote R at LSN 5
remote = R:1-5
trunk = R:5
sync = _
local = L:0

-> Make 3 local changes
remote = R:1-5
trunk = R:5
sync = _
local = L:1-3

-> Push
remote = R:1-6
trunk = R:6
sync = L:3
local = L:1-3

-> Make 2 local changes
remote = R:1-6
trunk = R:6
sync = L:3
local = L:1-5

-> GC
remote = R:1-6
trunk = R:6
sync = L:3
local = L:4-5

-> Make 2 local changes
remote = R:1-6
trunk = R:6
sync = L:3
local = L:4-7

-> Remote has 2 changes
remote = R:1-8
trunk = R:6
sync = L:3
local = L:4-7

-> Reset to remote by creating new graft L'
remote = R:1-8
trunk = R:8
sync = _
local = L':0

/> Alternatively, we could push to a new remote R'
remote = R':1
trunk = R':1
sync = L:7
local = L:4-7
```
