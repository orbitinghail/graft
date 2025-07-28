# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] graft-kernel scaffolding
- [x] protobuf local + remote schemas
- [x] optimized message type: GID
- [ ] local storage
- [ ] remote storage
- [ ] async kernel
  - ideally agnostic to async runtime
- [x] Splinter iter_range
- [ ] Volume Handle
- [ ] Volume Reader
- [ ] Volume Writer
- [ ] libgraft SQLite

## Notes

- make TypedPartitionSnapshot::prefix type safe
