# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] graft-kernel scaffolding
- [x] protobuf local + remote schemas
- [ ] optimized message types: LSN, GID, PageCount, PageIndex
  - pagecount/pageindex/lsn may be optimized into a single NonZero type
- [ ] local storage
- [ ] remote storage
- [ ] async kernel
  - ideally agnostic to async runtime
- [ ] Splinter iter_range
- [ ] Volume Handle
- [ ] Volume Reader
- [ ] Volume Writer
- [ ] libgraft SQLite
