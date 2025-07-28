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

## Working in Graft-Kernel vs Legacy Architecture

**⚠️ Important Context:**

This repository is transitioning from a legacy client/server architecture to a new direct-storage architecture (RFC 0001). The work is happening in two parallel development tracks:

### When Working on `graft-kernel` Crate:

- **Focus**: Code correctness, following existing patterns, implementing the new direct-storage architecture
- **Architecture**: Direct object storage access, eliminates metastore and pagestore
- **Design Doc**: See `docs/src/content/docs/docs/rfcs/0001-direct-storage-architecture.mdx`
- **Key Components**:
  - Local Fjall storage with partitioned keyspaces (handles, volumes, log, pages)
  - Direct object storage interface with Control/CheckpointSet/Commit/Segment files
  - Volume handles managing local-remote synchronization
  - CBE64 encoding for LSN ordering

### When Working on Legacy Components:

- **Components**: `graft-client`, `graft-server`, metastore/pagestore services
- **Architecture**: Traditional client-server with separate metastore and pagestore
- **Status**: Maintained for compatibility but being phased out

## Core Architecture (Legacy)

**Distributed Storage Model:**

- **Metastore**: Manages volume metadata, commits, and sync operations
- **Pagestore**: Stores actual page data in object storage with local caching
- **Client**: Local runtime with volume handles, networking, and sync logic

**Key Abstractions:**

- **Volume**: Logical database container with unique VID identifier
- **Page**: Fundamental 4KB storage unit, immutable once written
- **Commit**: Versioned snapshot of volume state with LSN ordering
- **GID (Global ID)**: Universal identifiers for volumes, segments, clients (graft-core/src/gid.rs)
- **LSN (Log Sequence Number)**: Monotonic version numbers for ordering (graft-core/src/lsn.rs)

**Storage Hierarchy:**

```
SQLite Database (VFS layer)
├── Graft Volume (logical container)
├── Local Storage (Fjall LSM-tree partitions)
├── Network Layer (metastore/pagestore clients)
└── Object Storage Backend (S3, etc.)
```

**Local Storage (Fjall):**

- Uses partitioned LSM-tree storage via `fjall` crate
- Key partitions: pages, commits, volume state, metadata
- Values use custom serialization via `FjallRepr` trait (graft-kernel/src/local/fjall_storage/fjall_repr.rs)

**SQLite Integration:**

- Custom VFS implementation in `graft-sqlite` crate
- Maps SQLite page operations to Graft volume operations
- Extension (`graft-sqlite-extension`) provides loadable SQLite module

This architecture enables distributed, versioned SQLite databases with lazy replication and strong consistency guarantees.

## Coding Guidelines

- Refer to CONTRIBUTING.md for coding style guidelines

## Commit Guidelines

- When committing on behalf of the user just sign off with: `Co-Authored-By: Claude <noreply@anthropic.com>`
- Never add `🤖 Generated with [Claude Code](https://claude.ai/code)` to any commit messages.

## Testing Utilities

### Generating VolumeIds, SegmentIds, or ClientIds

Run `just run tool [vid|sid|cid]` to randomly generate a new VolumeId, SegmentId, or ClientId if you need one in a test.

You can convert the resulting id like so:

```rust
let vid: VolumeId = "GonvVp514wF3ifTRoo11vY".parse().unwrap()
```
