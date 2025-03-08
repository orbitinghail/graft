Stack:

- garbage collection
- consider switching pagestore to websockets or http streaming bodies
- authentication (api keys)

# Idempotency

Currently we run some heuristics to determine idempotency. This has proved to be error prone. The safer option would be to have the client store the fully serialized commit before sending to the metastore, and then reply that on recovery. This may also make some of the other client side replay code simpler.

# SQLite sanity balance bug

Antithesis has discovered that it's possible for the total balance of all accounts to get out of sync with the circulation total. So far I've been unable to figure out the bug. I don't think it's an issue with how the transactions are written, so I'm assuming that it's a worst case scenario issue with either client storage or the pagestore.

So far I've narrowed the bug down to occurring right after a reset. So I've added many checks in the reset process to validate that the result is correct. None of those checks fire, and yet the bug still exists.

Some ideas on where to go from here:
1. switch to a ledger style workload and record which client issues which transactions - in theory an append only ledger will at least make it easier to determine where things get out of sync.
2. add some kind of hashing to the workload, similar to what we do in the other simple kv workload
3. try to reproduce the bug in the simpler kv workload