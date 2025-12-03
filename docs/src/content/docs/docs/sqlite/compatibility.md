---
title: Compatibility
description: SQLite configuration, compatibility, and behaviors when using Graft
---

When using Graft as the storage engine for SQLite via the extension, it’s important to understand how SQLite's configuration interacts with Graft’s underlying behavior. Graft replaces SQLite's built-in filesystem storage with its own crash-safe, transactional engine. This affects how certain SQLite settings should be used and how SQLite behaves.

## SQLite version compatibility

The Graft SQLite extension should work with any version of [SQLite] after 3.44.0 (released Nov 2023). It probably works with earlier versions, but no guarantees.

[SQLite]: https://www.sqlite.org/index.html

## TL;DR: Recommended SQLite settings

```sql
-- only this setting should be changed when using Graft
PRAGMA journal_mode = MEMORY;

-- these settings are unchanged from SQLite defaults:
PRAGMA synchronous = NORMAL;
PRAGMA locking_mode = NORMAL;
PRAGMA cache_size = -2000;
PRAGMA temp_store = DEFAULT;
```

Read on for more details.

## Journaling and durability

SQLite includes several [`journal_mode`]s (`DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF`) to maintain crash safety and atomicity during writes.

**With Graft:**

- These modes become redundant, and `WAL` mode in particular may confuse SQLite.
- Graft provides its own crash-safe durability and rollback mechanisms.
- Enabling SQLite’s journaling adds unnecessary I/O and file operations that Graft does not require for safety.

**Recommendation:**
Set `PRAGMA journal_mode = MEMORY` when initializing the database. This makes redundant journaling work much cheaper while still preserving atomicity and durability through Graft.

**Note on `WAL` mode:**
Write-Ahead Logging (WAL) is **not supported** with Graft. It relies on shared memory and filesystem-level WAL files, which Graft does not provide.

[`journal_mode`]: https://www.sqlite.org/pragma.html#pragma_journal_mode

## Multi-process

Graft **does not currently support accessing the same SQLite database from multiple processes.** If you need this capability, please [file an issue].

However, Graft **does support multiple concurrent connections within a single process.**

[file an issue]: https://github.com/orbitinghail/graft/issues/new

## Other Settings

**[`synchronous`]**
**Suggested:** Default
This setting controls SQLite’s use of `fsync`, which Graft fully handles. Changing it has no effect when using Graft.

**[`locking_mode`]**
**Suggested:** `NORMAL`
Do **not** use `EXCLUSIVE` mode. It causes SQLite to hold the database file open in a way that prevents Graft from syncing intermediate state. This results in all transient writes being buffered in memory until the lock is released, increasing memory usage and risk of data loss on crash.

**[`cache_size`]**
**Suggested:** Default
The default value works well for most workloads. Changing it may improve read or write performance in some cases, but any tuning should be guided by benchmarking.

**[`temp_store`]**
**Suggested:** Default
Graft provides an in-memory temporary filesystem for temporary objects. This setting is ignored.

[`journal_mode`]: https://www.sqlite.org/pragma.html#pragma_journal_mode
[`synchronous`]: https://www.sqlite.org/pragma.html#pragma_synchronous
[`locking_mode`]: https://www.sqlite.org/pragma.html#pragma_locking_mode
[`cache_size`]: https://www.sqlite.org/pragma.html#pragma_cache_size
[`temp_store`]: https://www.sqlite.org/pragma.html#pragma_temp_store

## Summary

- Graft replaces SQLite’s need for journaling by providing its own crash-safe durability layer.
- `WAL` mode is not compatible with Graft.
- Graft supports multiple connections in a single process but not across processes.
- Default SQLite settings are generally sufficient when using Graft.
