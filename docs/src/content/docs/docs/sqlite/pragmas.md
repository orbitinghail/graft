---
title: Pragmas
description: Interact with the Graft SQLite extension using pragmas
---

The application can interact with the Graft SQLite extension using the following [pragma statements].

[pragma statements]: https://www.sqlite.org/pragma.html

#### **`pragma graft_status`**

Report the status of the current Volume and the current connection's Snapshot. Note that different SQLite connections to the same Graft Volume can concurrently access different snapshots via read transactions.

#### **`pragma graft_snapshot`**

Returns a compressed description of the current connections Snapshot.

#### **`pragma graft_pages`**

Reports the status of every page accessible by the current connection's Snapshot.

#### **`pragma graft_pull`**

Pulls every page accessible by the current connection's Snapshot from the server.

#### **`pragma graft_sync = true|false`**

Turn background sync on or off.

#### **`pragma graft_sync_errors`**

Reveal the most recent 16 errors and warnings encountered during background sync. This pragma pops errors out of a ring buffer, so calling it repeatedly will only report errors and warnings since the last call.

#### **`pragma graft_reset`**

This pragma drops all local changes and forcibly resets to the latest server Snapshot. Make sure you are ok with losing local changes permanently when running this pragma.

#### **`pragma graft_version`**

This pragma prints out Graft's version and commit hash which can be useful for debugging and support.
