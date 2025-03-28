<h1 align="center">Graft</h1>
<p align="center">
  <a href="https://docs.rs/graft-client"><img alt="docs.rs" src="https://img.shields.io/docsrs/graft-client"></a>
  &nbsp;
  <a href="https://crates.io/crates/graft-client"><img alt="crates.io" src="https://img.shields.io/crates/v/graft-client.svg"></a>
  &nbsp;
  <a href="https://github.com/orbitinghail/graft/actions"><img alt="Build Status" src="https://github.com/orbitinghail/graft/actions/workflows/ci.yml/badge.svg"></a>
</p>

Transactional page storage engine supporting lazy partial replication to the edge. Optimized for scale and cost over latency. Leverages object storage for durability.

## Using Graft

Graft should be considered **Alpha** quality software. Thus, don't use it for production workloads yet.

### SQLite extension

The easiest way to use Graft is via the Graft SQLite extension which is called `libgraft`. [Please see the documentation][libgraft-docs] for instructions on how to download and use `libgraft`.

[libgraft-docs]: https://github.com/orbitinghail/graft/blob/main/docs/sqlite.md

### Rust Crate

Graft can be embedded in your Rust application directly, although for now that is left as an exercise for the reader. You can find the Rust docs here: https://docs.rs/graft-client

### Other languages?

Please [file an issue] if you'd like to use Graft directly from a language other than Rust!

[file an issue]: https://github.com/orbitinghail/graft/issues/new

## Technical Overview

For a detailed overview of how Graft works, read [design.md].

[design.md]: https://github.com/orbitinghail/graft/blob/main/docs/design.md

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
