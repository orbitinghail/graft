# Copilot Instructions

This repository uses GitHub Copilot. For detailed instructions and context, see [.github/copilot-instructions.md](.github/copilot-instructions.md).

## Quick Context

- **Language**: Rust (systems programming)
- **Domain**: Distributed SQLite with versioning
- **Architecture**: Transitioning from client/server to direct-storage
- **Style**: Safety, performance, and clarity prioritized
- **Testing**: `just test` for all tests, `cargo nextest run` for Rust only

When working with this codebase:
1. Follow the coding guidelines in `.github/copilot-instructions.md`
2. Understand the architecture transition (legacy vs graft-kernel)
3. Use proper error handling with the `culprit` crate
4. Write performance-conscious code for systems-level operations