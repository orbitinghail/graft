# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] graft-kernel scaffolding
- [x] protobuf local + remote schemas
- [x] optimized message type: GID
- [x] local storage
- [ ] remote storage
- [x] async kernel
- [x] Splinter iter_range
- [x] Named Volume
- [x] Volume Reader
- [x] Volume Writer
- [ ] libgraft SQLite

---

Might make sense to take a crack at Remote, before getting back to kernel jobs
(i.e. pull_volume).
