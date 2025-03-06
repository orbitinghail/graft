Stack:

- garbage collection
- consider switching pagestore to websockets or http streaming bodies
- authentication (api keys)

# Idempotency

Currently we run some heuristics to determine idempotency. This has proved to be error prone. The safer option would be to have the client store the fully serialized commit before sending to the metastore, and then reply that on recovery. This may also make some of the other client side replay code simpler.
