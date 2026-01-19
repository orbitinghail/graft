# Graft v3

Can we have Graft operate even more like Git. Let's define the idea:

Git works _around_ a working directory, representing the current checked out state of the Repo.

What if we make Graft operate similarly. Instead of a working directory, there is a file on disk (or in memory) representing the currently checked out state of the file.

Local writes hit a double buffered WAL which are periodically incorporated back into the current snapshot. Each time we checkpoint into the current snapshot, a commit is generated.

Commits are the combination of a changeset Splinter, and a segment (or segments) containing the new page versions.

Thus checkpointing involves:

- writing changes from the inactive WAL into a segment + commit
- writing them to the remote
- fast forwarding the local snapshot to the new remote version
- marking the inactive wal as checkpointed

The downside of this is that checkpointing blocks on writing to the remote.

So, instead we could just write locally and async replicate commits to the remote.

On conflict we'd have local branch at version A and remote branch at version B, such that A and B share a common ancestor.

One issue is how to store this DAG.

Currently Graft uses globally unique LogIds which contain a monotonic sequence of commits. This enables two critical features in the current design:

1. given a LogId, we can trivially find the latest commit by binary searching forward, and we can trivially CAS to write a new commit with multi-writers
2. given a LogId, we can trivially fetch all missing commits in parallel

But there is a major downside: branches must be explicit, and lead to the generation of a new LogId (Logs parent other logs explicitly, implying that the monotonic sequence of the child starts at a point in the parent).

This downside leads to a major complexity which is how to handle pushing to the remote. Currently graft models this as two Logs, one local and one remote. Pushing involves rolling up changes in the local log and landing them as a single change to the remote. The issue here is that then the local log logically fast forwards to be parented by the newly rolled up change in the remote. If some remote changes then come in, the local log becomes physically partitioned, and is no longer a valid continuous series of commits. To handle this more correctly, Graft could probably drop the local log once it lands it in the remote, and create a new local log on the latest remote for new changes. The downside of this is that writers would have to coordinate with the remote commit process to ensure that concurrent writes land in the new log.

Also, in the current design, Graft doesn't support trivial zero-copy branches, it instead copies all the commits from a snapshot into a new Log.

Git has an advantage here in that each commit is part of a graph, and can refer to any number of other commits as parents. Hence, a branch is nothing more than a pointer to a commit, which can coexist with any other unrelated or related commits. This makes representing branches and handling conflicts easy to reason about computationally. But Git's architecture leads to a downside which is that to efficiently pull updates, a complex client and server side computation needs to run to compute which commits are missing. This leads to the generation of visibility indexes, and the design does not seem to work well without an interactive server being able to do the set calculations and figure out the minimum set of commits to send to the client.

Is there another design that Graft could use which would enable more git-like branching, without the complexity of independent log ids for every branch point? Ideally I'd like a more DAG like datastructure like Git, but with a object storage friendly layout that can be efficiently fetched from by clients without needless round trips.

---

Idea:

Each writer has it's own log.

Commits and segments are content addressed same as git, and published.

Branch ref contains tip commit hash + set of logrefs. Effectively it's a version vector? Anyone who fetches the latest ref can immediately determine which client logs they need to fast forward.

Assuming small number of unique writers, the logref set can be constrained or pruned over time, especially as checkpoints show up.

Checkpoints in this model need to be thought about. Perhaps they are equivalence relations at a particular commit hash. I.e. a rollup of the volume at the commit hash, and published to a separate root under the same commit hash.

Then the latest checkpoint will need to be published into the ref potentially. Not sure about this...

Whats the upsides and downsides of this model?

Upsides:

- fast forwarding a ref requires fastforwarding N logs, all of which can be parallelized after fetching the ref
- every client sees a subgraph formed of all the commits in the logs they have fetched. this subgraph is guaranteed to cover at least one ref, and automatically shares storage with other refs
- clients can choose how they want to publish changes, either into one log or multiple

Downsides:

- clients may receive more commits than they need
- minimum write is: 1 segment, 1 log write, 1 ref update

---

Log per ref.

Each client maintains a log per ref that they write to containing all of the commits that client issued to the requisite log.

Lets say that another client comes along and branches off an arbitrary commit. To do this they need to know the version vector at the time of that commit, for its history. To make this fast, perhaps every commit stores a version vector.

```
Log: Vec<Commit>

Commit {
  hash: CommitHash
  parents: Vec<CommitHash>
  logs: Map<LogId, LSN>
  pages: PageCount
  segment: Option<SegmentIdx>
  is_checkpoint: bool
  metadata: Map<String, String>
}

Ref {
  name: String,
  head: CommitHash,
  logs: Map<LogId, LSN>,
  checkpoint: Option<CommitHash>
}
```

This model requires that the head and checkpoint commit hash are visible somewhere in the ref's logs.

Checkpoints are commits with some interesting characteristics:

- they have two parents: the previous checkpoint, and the commit they are checkpointing. in effect they are a merge commit between a checkpoint and a series of changes
- is_checkpoint is true

A new client cloning a ref will start by pulling all of the referenced logs back to front. As they pull, they will keep an eye out for the checkpoint commit, at which point they discover the low-watermark to pull to.

How does a client pull an arbitrary commit? They need to pull all of the available logs until they find the commit. Once they find the commit, they can narrow down their search to only the named logs in the commit. Unfortunately due to how content addressing works, there is no easy way to find the most recent checkpoint other than searching all the logs (unless lucky and finds the checkpoint in the commits own logs).

One way to improve checkpoints is to have clients publish all checkpoint commits under a dedicated per-client checkpoint log. If these logs were tracked somewhere, it would be possible to keep up to date with all the latest checkpoints from every client. In production workloads checkpoints will be generated by a separate client (probably also associated with garbage collection and other background work), thus most checkpoints will exist in a single log.

Ok, so let's talk about read/write performance.

In the general case a client either is writing to a volume, or keeping a volume up to date with remote changes (read-only replica). In rare cases the client is continuously syncing with the remote while also writing, enabling multi-writer. We need to consider all three cases.

**Continuous single-writer**
The writer has a snapshot and a WAL. Writes flow into the WAL and are collapsed into the snapshot. Each time we update the snapshot, we construct a new commit object and segment in the graph. Asynchronously we push those new commit objects out into our commit log for the ref. We also attempt to update the remote

**ABORT: Problems on problems:**

- commits are blocked on checkpointing both WALs in the worst case
- all writes to the snapshot must flow through the WAL to ensure readers are safe -> leads to no current way to backfill missing segments (as they would have to show up at the end of the WAL, which is outside of the snapshot)

Maintaining a snapshot doesn't make sense. We should figure out how to read a page from a logical snapshot at a particular commit.

Our commit index researched discovered that binary fuse filters perform very well to quickly filter for matching LSNs in a log. We derived a binary fuse filter every 32 LSNs in the single log model.

How can we map that to a DAG of commits?

# Aside: Git commit-graph

https://git-scm.com/docs/commit-graph

The Git commit graph is a clever datastructure. It's composed of a OID Fanout+Lookup table followed by a a set of data tables (CDAT, BIDX, GDA2) followed by data chunks.

How to lookup a commit:

> Step 1: Fanout table (OIDF)
>
> The fanout is a 256-entry table indexed by the first byte of the commit SHA:
>
> fanout[0x00] = count of commits where SHA starts with 0x00 or less
> fanout[0x01] = count of commits where SHA starts with 0x01 or less
> ...
> fanout[0xff] = total commit count (1,174 in your repo)
>
> Step 2: Binary search in OIDL
>
> The OIDL chunk contains all commit SHAs sorted lexicographically. The fanout narrows the search range:
>
> Looking for commit 5512367c...
>
> First byte = 0x55
>
> low = fanout[0x54] # commits before 0x55
> high = fanout[0x55] # commits up through 0x55
>
> Binary search in OIDL[low..high] for exact match

At this point we know the offset of the matching commit hash in the OIDL chunk. This offset can then be used to lookup fields in the data tables: CDAT, BIDX, GDA2

CDAT is the main commit data table. Each commit stores 36 bytes containing:

- the commit hash
- the parent 1 and parent 2 positions
- the generation number
- the timestamp

GDA2 stores corrected commit date offsets per commit which handles clock skew and makes generation numbers monotonic.

BIDX stores the bloom index which is 4 bytes per commit. Contains the cumulative end offset into the BDAT chunk stored after the data tables. Each commit has a bloom filter stored at `BIDX[i-1]` with size `BIDX[i] - BIDX[i-1]`.

# aside JJ and ERSC and sapling

Learned a bunch about incrementally building skip indexes on top of the commit graph. The summary is that ancestor style queries benefit from an index that let's the cursor jump backwards through the commit graph in power of two chunks.

This would be useful mainly for ancestor queries, which I don't think graft needs.
