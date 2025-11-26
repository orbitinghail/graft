# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- consider adding a read oracle (do some perf testing)
- create some hello-world examples of using Graft without SQLite
- port tests
- write first draft of antithesis tests
