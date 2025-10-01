# Direct Storage Implementation Plan

A loose plan to implement Graft's new direct storage architecture as documented in [this RFC].

[this RFC]: https://graft.rs/docs/rfcs/0001-direct-storage-architecture/

- [x] graft-kernel scaffolding
- [x] protobuf local + remote schemas
- [x] optimized message type: GID
- [x] local storage
- [ ] remote storage
- [ ] async kernel
- [x] Splinter iter_range
- [x] Named Volume
- [x] Volume Reader
- [x] Volume Writer
- [ ] libgraft SQLite

---

Next steps:

Need to build out the async kernel and sync subsystem. This includes building:

- [x] A tokio friendly version of `ChangeSet`. Needs to integrate into the
      Runtime events stream. Possibly use https://docs.rs/async-notify/latest/async_notify/struct.NotifyStream.html for subscriptions.
- [ ] Need to figure out how to actually run the sync jobs. I think the approach used by graft-client isn't too bad. Although we could spawn the jobs and use some kind of bounded job queue to process them. Might be easier to handle hanging jobs due to network issues for example.
