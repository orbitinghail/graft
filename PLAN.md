- build a simple GC that simply drops orphan segments
  - pay special attention to in-progress VolumeWriters
  - make sure to run fjall gc when deleting pages
- consider adding a read oracle (do some perf testing)
- create some hello-world examples of using Graft without SQLite
- port tests
- write first draft of antithesis tests

# Rename graft-kernel to graft, and merge in graft-core

I've got the `graft` crate on crates.io! Whoopie!
