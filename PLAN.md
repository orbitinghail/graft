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

While working on pull_volume I context switched to Splinter in order to make it work better when the Splinter is close to full. This will allow Graft to use Splinters as an efficient commit index + use those indexes to quickly determine which commits we are missing.

Once Splinter can handle this, we can add these in-memory indexes to FjallStorage.
