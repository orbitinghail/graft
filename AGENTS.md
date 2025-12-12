# AGENTS.md

This file provides guidance when working with code in this repository.

## Dev environment tips

- Use `just test` to run all tests
- Use `cargo nextest run` to run Rust tests
- Use `just run sqlite test` to run SQL tests (./tests/sql/\*.sql)
- Use `cargo build` to build crates
- Use `pnpm build` to build docs (in ./docs)
- Use `cargo check|fmt|clippy` to lint all Rust code
- Use `just run tool vid|log|sid` to generate a random VolumeId, LogId, or SegmentId for testing

## Searching the codebase

When searching for a particular function, struct, enum, trait, etc. always look for a matching file first before using general pattern search tools.

Use the mgrep tool for semantic search of the codebase.
Use the built in Search tool for searching for exact maches like usages of a particular function, struct, enum, trait, etc.

## Coding & Collaborating Guidelines

- Refer to CONTRIBUTING.md for coding style guidelines
- NEVER commit without being explicitly asked to
