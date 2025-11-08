# Copilot Instructions

This repository uses GitHub Copilot. For detailed instructions and context, see [.github/copilot-instructions.md](.github/copilot-instructions.md).

## Quick Context

- **Language**: Rust (systems programming)
- **Domain**: Distributed SQLite with versioning
- **Architecture**: Direct-storage with object storage backend
- **Style**: Safety, performance, and clarity prioritized
- **Testing**: `just test` for all tests, `cargo nextest run` for Rust only

When working with this codebase:
1. Follow the coding guidelines in `.github/copilot-instructions.md`
2. Use proper error handling with the `culprit` crate
3. Write performance-conscious code for systems-level operations