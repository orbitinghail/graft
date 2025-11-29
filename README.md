<h1 align="center">Graft</h1>
<p align="center">
  <a href="https://github.com/orbitinghail/graft/actions"><img alt="Build Status" src="https://img.shields.io/github/actions/workflow/status/orbitinghail/graft/ci.yml"></a>
  &nbsp;
  <a href="https://docs.rs/graft-kernel"><img alt="docs.rs" src="https://img.shields.io/docsrs/graft-kernel?label=docs.rs"></a>
  &nbsp;
  <a href="https://crates.io/crates/graft-kernel"><img alt="crates.io" src="https://img.shields.io/crates/v/graft-kernel.svg"></a>
  &nbsp;
  <a href="https://graft.rs"><img alt="graft.rs" src="https://img.shields.io/badge/graft.rs-docs-blue"></a>
  &nbsp;
  <a href="https://deepwiki.com/orbitinghail/graft"><img alt="Ask DeepWiki" src="https://deepwiki.com/badge.svg"></a>
</p>

**Graft** is an open-source transactional storage engine designed for efficient data synchronization at the edge. It supports lazy, partial replication with strong consistency, ensuring applications replicate only the data they need.

**Core Benefits:**

- **Lazy Replication**: Clients sync data on demand, saving network and compute.
- **Partial Replication**: Minimize bandwidth by syncing only required data.
- **Edge Optimization**: Lightweight client designed for edge, mobile, and embedded environments.
- **Strong Consistency**: Serializable Snapshot Isolation ensures correct, consistent data views.
- **Transactional Object Storage**: Graft turns object storage into a transactional system—supporting consistent updates to subsets of data at page granularity, without imposing any data format or schema.
- **Instant Read Replicas**: Decoupled metadata and data allow replicas to spin up immediately—no replay, no waiting for full recovery.

**Use Cases:**

- Offline-first and mobile applications
- Cross-platform synchronization
- Stateless replicas for serverless or embedded environments
- Diverse data replication scenarios
- Storage and replication for databases

**Learn more:**

- [Blog Post](https://sqlsync.dev/posts/stop-syncing-everything/)
- [Vancouver Systems Talk](https://www.youtube.com/watch?v=eRsD8uSAi0s)
- [High Performance SQLite Talk](https://www.youtube.com/watch?v=dJurdmhPLH4)
- [Documentation](https://graft.rs)

## Using Graft

Graft should be considered **Alpha** quality software. Thus, please message @carlsverre before using it in production.

### SQLite extension

The easiest way to use Graft is via the Graft SQLite extension which is called `libgraft`. [Please see the documentation][libgraft-docs] for instructions on how to download and use `libgraft`.

[libgraft-docs]: https://graft.rs/docs/sqlite/

### Rust Crate

Graft can be embedded in your Rust application directly, although for now that is left as an exercise for the reader. You can find the Rust docs here: https://docs.rs/graft-kernel

### Other languages?

You can use the Graft SQLite extension from any language that has a native SQLite library. Please [see the documentation for details][libgraft-docs].

If you'd like to access the Graft low-level Volume API from a language other than Rust, please [file an issue]!

[file an issue]: https://github.com/orbitinghail/graft/issues/new

## Technical Overview

For an overview of how Graft works, visit https://graft.rs/docs/internals.

## Contributing

Thank you for your interest in contributing your time and expertise to the project. Please [read our contribution guide] to learn more about the process.

[read our contribution guide]: https://github.com/orbitinghail/graft/blob/main/CONTRIBUTING.md

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: https://github.com/orbitinghail/graft/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/graft/blob/main/LICENSE-MIT
