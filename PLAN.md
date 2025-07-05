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

Currently building out concrete types for each of the codec's versioned types.

The idea is to abstract the versioned encodings behind a higher level concrete type to make migration easier in the future.

The Codec system also needs to support:

- version envelopes for object store
- support segment frames
  - effectively a custom codec + compression

In terms of abstraction, we have two choices:

1. wrap the underlying versioned type with a newtype, then impl Codec with a passthrough macro
2. copy the underlying versioned type's fields into a concrete type, then impl Codec via a splat macro
   -> a benefit of this is that we can correctly encode empty states and perform additional validation while splatting into the concrete type
