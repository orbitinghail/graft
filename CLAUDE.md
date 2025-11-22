# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) and other AI agents when working with code in this repository.

## Common Commands

**Building and Testing:**

```bash
# Run all tests (nextest + SQLite tests)
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

## Working in .kosho Worktrees

**⚠️ Important: Worktree Isolation**

When Claude is executing in a `.kosho` worktree (identifiable by `.kosho` in the current working directory path), you MUST:

- **Only read and write files within the current worktree directory**
- **Never access files in the top-level repository root**
- **Use relative paths or paths within the current worktree**

**Example:**

- ✅ Current worktree: `<repo>/.kosho/feat-branch`
- ✅ Access files like: `crates/graft-core/src/page.rs` (relative to worktree)
- ❌ Never access: `<repo>/crates/graft-core/src/page.rs` (repo root)

This ensures that all changes are made within the isolated feature branch worktree and don't accidentally modify the main repository.

## Architecture

For detailed architecture documentation, see `docs/src/content/docs/docs/rfcs/0001-direct-storage-architecture.mdx`.

## Coding Guidelines

- Refer to CONTRIBUTING.md for coding style guidelines

## Commit Guidelines

- **Never commit without being explicitly asked to**
- When committing on behalf of the user just sign off with: `Co-Authored-By: Claude <noreply@anthropic.com>`
- Never add `🤖 Generated with [Claude Code](https://claude.ai/code)` to any commit messages.

## Testing Utilities

### Generating VolumeIds, SegmentIds, or ClientIds

Run `just run tool [vid|sid|cid]` to randomly generate a new VolumeId, SegmentId, or ClientId if you need one in a test.

You can convert the resulting id like so:

```rust
let vid: VolumeId = "GonvVp514wF3ifTRoo11vY".parse().unwrap()
```
