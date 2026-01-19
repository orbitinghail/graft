I'm struggling to balance between the benefits of content addressing and the power of monotonic logs.

But the reality of what I'm building is ultimately a monotonic log. But one that has to be able to represent this locally:

```
o---o---o  ← origin/main
     \
      o---o---o  ← main
```

The thing is though, that once the user receives that state, they probably want to rebase and then push:

```
o---o---o  ← origin/main
         \
          o---o---o  ← main

push main to origin/main

o---o---o---o---o---o  ← origin/main, main
```

Alternatively the user could push their branch to a different remote ref and reset main:

```
o---o---o  ← origin/main, main
     \
      o---o---o  ← origin/foo, foo
```

We want to represent all of these different layouts.

But we don't need to represent merges (multi-parent commits) as there is no way to generically merge physical pages. And even a merge would basically be a rebase.

In addition, we want to represent squash merging from a local branch into main and then pushing. All while handling concurrent writes to the local branch:

```
                origin/main ↴
o---o-----------o-----------o ← main
     \         /           /
      o---o---o---o---o---o---o---o---o  ← local
```

We also want to support multiple writers sequentially hitting the remote:

```
                origin/main ↴
o---o-----------o---o-------o ← main
     \         /     \     /
      o---o---o       o---o---o---o---o  ← local
```

A key question is how we actually want to represent this. Since true merges aren't possible, this diagram is not actually correct:

```
o---o-----------o ← main
     \         /
      o---o---o   ← local
```

Instead, the actual graph is just a tree of independent branches:

```
o---o-----------a ← main
     \
      b---c---d   ← local
```

Such that commit a is the revset `b..=d` squashed together.

Furthermore, since local is only a local branch, we don't even need to track provenance beyond maybe which client submitted the write.

However, we still need to differentiate between remote refs and local ones, even for the main ref. Since we need to handle divergence:

```
      x ← origin/main
     /
o---o-----------a ← main
     \
      b---c---d   ← local
```

In the case of divergence, we want to be able to compare the local and remote version of the branch.

Let's consider what this might look like with sequential logs and named LogRefs.

```
       a3 ← origin/main
      /
a1---a2------------a3 ← main
      \           / ← squash
       b1---b2---b3   ← local
```

So in this example, a local log `b` was branched from `a2` and then changes were made. The branch was squashed into `a3`.

Then when we tried to push `a3` to the remote we discovered that `a3` already existed. This is the point where things get weird.

In the current version of Graft, we combine pushing with the creation of a3, thus preventing a3 from ever existing if it is diverged. This allows log a to always be "authoritative" but leads to the complexity of SyncState.

As an alternative, we could use 3 logs:

```
a1---a2---a3 ← origin/main
      \
       \-------------b1 ← staged
        \           /
         c1---c2---c3   ← local
```

Thus, we can squash outstanding changes from the local log into a staging log and attempt to push the staging log to the remote. This decouples local and remote writes from the push operation.

```
a1---a2----------------a3------------a4------a5   ← origin/main
      \               /               \     /
       \-------------b1                \---e1     ← staged
        \           /                   \ /
         c1---c2---c3                    d1       ← local
```

In the above example, we successfully landed b2 into a3. Later on someone else modified origin/main creating a4 which we pulled, modified, and pushed.

Next, let's consider a long lived single-writer:

```
a1---a2----------------a3--------a4  ← origin/main
      \               /         /
       \-------------b1--------b2  ← staged
        \           /         /
         c1---c2---c3---c4---c5---c6  ← local
```

This writer periodically squashes local into a staging branch, allowing writes to continue concurrently. In the background, the staging branch is being pushed to the remote.

An equivalence relation has to keep track that `c5==b2==a4`. This allows the snapshot to take advantage of the squashed commits, presumably allowing GC to truncate history.

```
a1---a2----------------a3--------a4  ← origin/main
      \               /         /
       \-------------b1--------b2  ← staged
        \           /         /
         c1---c2---c3---c4---c5---c6  ← local

equivalent snapshot:
a1---a2----------------a3  ← origin/main
                        \
                         c4---c5---c6  ← local

a1---a2----------------a3--------a4  ← origin/main
                                  \
                                   c6  ← local

equivalence relations:
(c1..=c3) => b1 => a3
(c4..=c5) => b2 => a4

Enables the following substitutions:

a1---a2----------------a3  ← origin/main
                        \
                         c4---c5---c6  ← local

a1---a2----------------a3  ← origin/main
                        \
                         b2---c6

a1---a2----------------a3  ← origin/main
                        \
                         a4---c6
```

But things get more tricky in this situation:

```
a1---a2----------------a3--------a4  ← origin/main
      \               /         /
       \-------------b1--------b2--------b3 ← staged
        \           /         /         /
         c1---c2---c3---c4---c5---c6---c7---c8  ← local


latest canonical snapshot:
a1---a2----------------a3--------a4  ← origin/main
                                  \
                                   b3 ← staged
                                    \
                                     c8 ← local

equivalence relations:
(c1..=c3) => b1 => a3
(c4..=c5) => b2 => a4
(c6..=c7) => b3

extreme snapshot canonicalization:

a1---a2  ← origin/main
      \
       b1---b2 ← staged
             \
              c6---c7---c8 ← local

replace b1=>a3

a1---a2  ← origin/main
      \
       a3---b2 ← staged
             \
              c6---c7---c8 ← local

replace b2=>a4

a1---a2  ← origin/main
      \
       a3---a4
             \
              c6---c7---c8 ← local

replace c6..=c7 => b3

a1---a2  ← origin/main
      \
       a3---a4
             \
              b3---c8 ← local, staged
```

After canonicalization, two snapshots can be compared to determine if they are equal. This is important to handle local write concurrency.

---

Ok so in this architecture, Grove is a forest of monotonic logs, each identified by a GUID. Logs must be able to branch from one another in a cheap way.

Branches are named, and must exist in the remote. Each branch is a pointer to a LogId.

Branches only need to change their Log when the user wants to re-write history. This is a rare operation, thus we should optimize the happy path around expecting that the remote log does not change.

A force push (repointing the log) involves creating or selecting the log to point to, and atomically updating the branch.

A regular push (appending commits to an existing log) is simply:

- atomically append commits via monotonic lsns
- check the branch ref, update if needed

We need to prove that this is safe.

```

Ops:
- Pull<Log>: attempt to fast-forward to the latest version of the log
- Append<Log>: atomically write commit to log at next LSN
- Branch<From, To>: atomically write commit to new log, branched from previous log
- Check<Branch,Log>: check if the branch is pointing to the Log
- CAS<Branch,From,To>: atomically compare and swap branch to a new Log

Initial state:
- main: a

Each line is: Actor Op Result

1 Append<a> Ok
1 Check<main, a> Ok

1 Append<a> Ok
2 Append<a> Err: conflict
1 Check<main, a> Ok

1 Append<a> Ok
2 Append<a> Err: conflict
2 Branch<a, b> Ok
2 CAS<main, a, b> Ok
1 Check<main, a> Err

1 Append<a> Ok

// 2 and 3 both try to write, and fail
2 Append<a> Err: conflict
3 Append<a> Err: conflict

// 2 and 3 both branch
2 Branch<a, b> Ok
3 Branch<a, c> Ok

// 2 wins the race, 3 fails
2 CAS<main, a, b> Ok
3 CAS<main, a, c> Err(b)

// 1 checks here, sees different log
1 Check<main, a> Err(b)

// now 1 and 3 both have to decide what to do
// what if they both decide to force push?
1 CAS<main, b, a> Ok
3 CAS<main, b, c> Err(a)
```

It seems like this approach should be safe?

How does the read path look? The reader has two pieces of information, the last known log for the given branch, and the branch name.

The reader can optimistically pull both in parallel to update. If the branch has changed, then it fails over to loading the new log.

This can lead to some interesting operation logs though:

```
Ops:
Pull<Log>: pull any unseen changes in a log
Check<Branch, Log>: Check that the branch still points at the log

Each line is: Actor Op Result

1 Pull<a> Ok
1 Check<main, a> Err(b)
2 CAS<main, a, b> Ok
1 Pull<b> Ok
3 CAS<main, b, c> Ok
```

Thus it's very possible for a node to see an outdated version... but I think it's the same result as git force push.

If we add the log LSN to the branch metadata, that gives us two advantages:

1. it allows a branch to point at an arbitrary commit
2. it allows us to make the CAS include the current LSN

If the CAS includes the current LSN, it ensures that a force push only succeeds if the branch's logical state hasn't changed.

---

```
a1---a2----------------a3--------a4---a5---a6  ← origin/main
      \               /         /
       \-------------b1--------b2  ← staged
        \           /         /
         c1---c2---c3---c4---c5  ← local

a1---a2----------------a3--------a4  ← origin/main
      \               /         /
       \-------------b1--------b2---b3---b4  ← staged
        \           /         /    /    /
         c1---c2---c3---c4---c5---c6---c7  ← local
```
