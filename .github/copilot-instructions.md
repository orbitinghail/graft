# GitHub Copilot Instructions for Graft

This file provides context and guidance for GitHub Copilot when working with the Graft codebase. Graft is a distributed, versioned SQLite system built in Rust.

## Project Overview

Graft enables distributed, versioned SQLite databases with lazy replication and strong consistency guarantees. The system is transitioning from a legacy client/server architecture to a new direct-storage architecture.

## Architecture Context

### Current Transition: Legacy vs New Architecture

The codebase has two parallel development tracks:

**New Architecture (graft-kernel crate):**
- Direct object storage access, eliminates metastore and pagestore
- Local Fjall storage with partitioned keyspaces (handles, volumes, log, pages)
- Direct object storage interface with Control/CheckpointSet/Commit/Segment files
- Volume handles managing local-remote synchronization
- CBE64 encoding for LSN ordering

**Legacy Architecture (being phased out):**
- `graft-client`, `graft-server`, metastore/pagestore services
- Traditional client-server with separate metastore and pagestore
- Maintained for compatibility but being phased out

### Key Components

**Core Abstractions:**
- **Volume**: Logical database container with unique VID identifier
- **Page**: Fundamental 4KB storage unit, immutable once written
- **Commit**: Versioned snapshot of volume state with LSN ordering
- **GID (Global ID)**: Universal identifiers for volumes, segments, clients
- **LSN (Log Sequence Number)**: Monotonic version numbers for ordering

**Storage Hierarchy:**
```
SQLite Database (VFS layer)
├── Graft Volume (logical container)
├── Local Storage (Fjall LSM-tree partitions)
├── Network Layer (metastore/pagestore clients)
└── Object Storage Backend (S3, etc.)
```

## Coding Guidelines

Graft is low-level systems software. When suggesting code, prioritize:

### Safety
- Use simple, explicit control structures. Avoid recursion
- Keep functions under 70 lines
- Use fixed-size types (e.g. `u32`, `i64`)
- Prefer stack allocation or startup allocation over dynamic allocation
- Use assertions for invariants and argument checks
- Treat warnings as errors

### Performance
- Design for performance from the start
- Batch I/O or expensive operations
- Prioritize optimizing: network > disk > memory > CPU
- Write predictable, branch-friendly code

### Clarity
- Use clear, descriptive variable names
- Avoid abbreviations and single-letter variables
- Use specific types like `ByteUnit` and `Duration` instead of bare types
- Keep functions simple and group related code
- Declare variables near usage
- Write idiomatic Rust code

### Error Handling
- Use `Result<T, E>` for recoverable errors
- Use assertions for invariants
- Prefer explicit error handling over panics
- Use the `culprit` crate pattern for error context

## Common Patterns

### ID Types and Parsing
```rust
// Use proper ID types instead of strings
let vid: VolumeId = "GonvVp514wF3ifTRoo11vY".parse().unwrap();
let cid = ClientId::random();
let lsn = LSN::new(42);
```

### Error Handling with Culprit
```rust
use culprit::Culprit;

fn example() -> Result<(), ApiErr> {
    let vid = VolumeId::try_from(req.vid)
        .or_into_culprit("failed to parse VolumeId")?;
    // ... rest of function
}
```

### Tracing and Logging
```rust
#[tracing::instrument(skip(state, req))]
pub async fn handler(/* ... */) -> Result</* ... */, ApiErr> {
    tracing::info!(?vid, ?cid, ?snapshot_lsn, "processing request");
    // ... function body
}
```

## Common Commands

### Building and Testing
```bash
# Run all tests
just test

# Run Rust tests only
cargo nextest run
cargo nextest run -p <crate>

# Run SQLite extension tests
just run sqlite test

# Build individual components
cargo build --package graft-sqlite-extension
```

### Code Quality
```bash
cargo check
cargo fmt
cargo clippy
```

### Development Tools
```bash
# Generate test IDs
just run tool vid  # VolumeId
just run tool sid  # SegmentId
just run tool cid  # ClientId
```

## File Organization

```
crates/
├── graft-core/          # Core types and utilities
├── graft-kernel/        # New direct-storage architecture
├── graft-client/        # Legacy client library
├── graft-server/        # Legacy server components
├── graft-sqlite/        # SQLite VFS integration
└── graft-sqlite-extension/ # SQLite loadable extension
```

## Testing Patterns

### Unit Tests
- Use `#[cfg(test)]` modules
- Test both success and error cases
- Use descriptive test names that explain what is being tested

### Integration Tests
- Place in `tests/` directory or crate-specific test files
- Test end-to-end workflows
- Use realistic test data and scenarios

## Worktree Isolation

When working in `.kosho` worktrees:
- Only read/write files within the current worktree
- Use relative paths within the worktree
- Never access files in the top-level repository root

## Performance Considerations

- Graft handles 4KB pages as the fundamental storage unit
- LSN ordering is critical for consistency
- Network operations should be batched when possible
- Local storage uses Fjall LSM-tree for efficient key-value operations
- Object storage operations are expensive and should be minimized

## Dependencies

Key external crates used:
- `fjall`: LSM-tree storage engine
- `tokio`: Async runtime
- `tracing`: Structured logging
- `serde`: Serialization
- `axum`: HTTP server framework
- `culprit`: Error handling and context

When suggesting new dependencies, prefer crates that align with the existing stack and performance requirements.

## Related Documentation

- `CLAUDE.md`: Instructions for Claude Code AI agent
- `CONTRIBUTING.md`: General contribution guidelines and coding style
- `README.md`: Project overview and getting started guide