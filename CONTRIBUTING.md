# Contributing to Graft

Welcome to the orbitinghail Graft repo! We are so excited you are here. Thank you for your interest in contributing your time and expertise to the project. The following document details contribution guidelines.

Whether you're addressing an open issue (or filing a new one), fixing a typo in our documentation, adding to core capabilities of the project, or introducing a new use case, all kinds of contributions are welcome.

## Gaining consensus

Before working on Graft, it's important to gain consensus on what you want to change or build. This will streamline the PR review process and make sure that your work is aligned with the projects goals. This can be done in a number of ways:

- [File an issue]: best for bug reports and concrete feature requests
- [Start a discussion]: best for ideas and more abstract topics
- [Join the Discord]: best for real-time collaboration

[File an issue]: https://github.com/orbitinghail/graft/issues/new
[Start a discussion]: https://github.com/orbitinghail/graft/discussions/new/choose
[Join the Discord]: https://discord.gg/etFk2N9nzC

Once you're ready to start building, it's time to get Graft running on your computer!

## Running Graft locally

To build and run Graft, ensure you have the following dependencies installed:

| Name         | Version | Where to Get It        |
| ------------ | ------- | ---------------------- |
| rust + cargo | 1.85    | [rustup]               |
| just         | 1.40    | [just]                 |
| clang + llvm | 19      | System package manager |
| mold         | 2.37    | System package manager |
| nextest      | 9       | [nextest]              |

[rustup]: https://rustup.rs/
[just]: https://github.com/casey/just
[nextest]: https://nexte.st/docs/installation/pre-built-binaries/

The easiest way to ensure everything works is to run the tests. This can be done via `just test` for a single command that runs everything, or you can run individual test suites like so:

```bash
# Test the whole workspace or an individual crate
# cargo nextest run [-p <crate>] [-- <filter for a specific test name>]
cargo nextest run
cargo nextest run -p splinter
cargo nextest run client_sync_sanity

# Run SQLite tests
just run sqlite test
```

Next, if you'd like to run Graft locally run the following commands in different terminals:

```bash
# first, start a local Graft Metastore
cargo run --bin metastore

# next, in another terminal, run a local Graft pagestore
cargo run --bin pagestore

# finally, you can open up a SQLite shell connected to localhost by default
just run sqlite shell
```

Further reading:

- For a detailed overview of how Graft works, you might want to read [design.md].
- For an overview of how to use the Graft SQLite extension, see [sqlite.md].

[design.md]: https://github.com/orbitinghail/graft/blob/main/docs/design.md
[sqlite.md]: https://github.com/orbitinghail/graft/blob/main/docs/sqlite.md

## Pull Request (PR) process

To ensure your contribution is reviewed, all pull requests must be made against the `main` branch.

PRs must include a brief summary of what the change is, any issues associated with the change, and any fixes the change addresses. Please include the relevant link(s) for any fixed issues.

Pull requests do not have to pass all automated checks before being opened, but all checks must pass before merging. This can be useful if you need help figuring out why a required check is failing.

Our automated PR checks verify that:

- All unit tests pass, which can be done locally by running `just test`
- The code has been formatted correctly, according to `cargo fmt`.
- There are no linting errors, according to `cargo clippy`.

## Licensing

Graft is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

[LICENSE-APACHE]: https://github.com/orbitinghail/graft/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/graft/blob/main/LICENSE-MIT

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you shall be dual licensed as above, without any additional terms or conditions.

All submissions are bound by the [Developer's Certificate of Origin 1.1](https://developercertificate.org/) and shall be dual licensed as above, without any additional terms or conditions.

```
Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```
