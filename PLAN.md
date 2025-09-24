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
- [x] Volume Reader
- [x] Volume Writer
- [ ] libgraft SQLite

# Status

NamedVolumeState requires local=VolumeRef, but that doesn't work for an empty
volume. Need to think about how to represent this.

Then finish up using NamedVolume.reader/writer rather than RuntimeHandle.volume_reader/volume_writer.

Then initial sanity tests should work.
