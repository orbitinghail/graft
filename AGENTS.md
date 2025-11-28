# AGENTS.md

This file provides guidance when working with code in this repository.

## Common Commands

**Building and Testing:**

```bash
# Run all tests
just test

# Run Rust tests only
cargo nextest run
cargo nextest run -p <crate>
cargo nextest run <test_filter>

# Run SQLite extension tests
just run sqlite test
just run sqlite test <filter>

# Build individual components
cargo build --package graft-sqlite-extension

# Build docs
cd ./docs && pnpm build
```

**Development Tools:**

```bash
# Code quality
cargo check
cargo fmt
cargo clippy
```

## Coding Guidelines

- Refer to CONTRIBUTING.md for coding style guidelines

## Commit Guidelines

- **Never commit without being explicitly asked to**
- When committing on behalf of the user just sign off with: `Co-Authored-By: Claude <noreply@anthropic.com>`

## Testing Utilities

### Generating VolumeIds or SegmentIds

Run `just run tool [vid|sid]` to randomly generate a new VolumeId or SegmentId if you need one in a test.

You can parse the resulting id like so:

```rust
let vid: VolumeId = "GonvVp514wF3ifTRoo11vY".parse().unwrap()
```
