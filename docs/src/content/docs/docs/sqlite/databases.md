---
title: Databases
description: Understanding how to connect to different databases using Graft
---

When using Graft, each SQLite database maps to a **Volume** - which tracks a local Log (your working copy) with a remote Log (the source of truth in object storage).

You can create or connect to Volumes several ways:

- By Tag
- Create new databases
- Clone from remote
- Fork a database

## Connect by Tag

When you open a database using the Graft VFS, the name of the database is used as the Tag.

```sql
-- open the tag "main" using graft
.open 'file:main?vfs=graft'
```

You can also use `ATTACH`:

```sql
-- attach to another tag
ATTACH 'file:production?vfs=graft' AS prod;
```

## Create a New Database

Use `graft_new` to create a new empty Volume and update your current tag to point at it. If your tag was attached to another Volume, it will remain untouched.

```sql
sqlite> pragma graft_status;
+--------------------------------------------------+
|                   On tag main                    |
+--------------------------------------------------+
| On tag main                                      |
| Local Log 74ggc1nwQK-3SivxKLwdLBFq is grafted to |
| remote Log 74ggc1nwQK-2nuRmHd4ZHivj.             |
|                                                  |
| The Volume is up to date with the remote.        |
+--------------------------------------------------+
sqlite> select * from t;
+-------------+
|    data     |
+-------------+
| hello world |
| hi bob      |
| testing     |
+-------------+
sqlite> pragma graft_new;
+-----------------------------------------------------------------------------------------------------------------------------+
| Switched to Volume 5rMJkfMogd-3bVjH8fwwX44x with local Log 74ggc1o8bK-34EknTZT8mjSx and remote Log 74ggc1o8bK-3LdWJvxfi3sPr |
+-----------------------------------------------------------------------------------------------------------------------------+
| Switched to Volume 5rMJkfMogd-3bVjH8fwwX44x with local Log 74ggc1o8bK-34EknTZT8mjSx and remote Log 74ggc1o8bK-3LdWJvxfi3sPr |
+-----------------------------------------------------------------------------------------------------------------------------+
sqlite> select * from t;
(1) no such table: t in "select * from t;"
Runtime error: no such table: t
sqlite> pragma graft_volumes;
+--------------------------------------------+
|      Volume: 5rMJkfMcVd-3DsuawkC8v1Do      |
+--------------------------------------------+
| Volume: 5rMJkfMcVd-3DsuawkC8v1Do           |
|   Local: 74ggc1nwQK-3SivxKLwdLBFq          |
|   Remote: 74ggc1nwQK-2nuRmHd4ZHivj         |
|   Status: 1 r_                             |
| Volume: 5rMJkfMogd-3bVjH8fwwX44x (current) |
|   Local: 74ggc1o8bK-34EknTZT8mjSx          |
|   Remote: 74ggc1o8bK-3LdWJvxfi3sPr         |
|   Status: _ r_                             |
+--------------------------------------------+
```

## Clone from Remote

You can create a new local Volume based on a remote Log Id:

```sql
pragma graft_clone = "74ggc1X5BE-3A7QEtHWMomvb";
+--------------------------------------------------------------------------------------+
| Created new Volume 5rMJkfNQfi-3i6P4jPw9XWVh from remote Log 74ggc1X5BE-3A7QEtHWMomvb |
+--------------------------------------------------------------------------------------+
| Created new Volume 5rMJkfNQfi-3i6P4jPw9XWVh from remote Log 74ggc1X5BE-3A7QEtHWMomvb |
+--------------------------------------------------------------------------------------+
sqlite> pragma graft_pull;
+-----------------------------------------------------------+
| Pulled LSNs ..=1 into remote Log 74ggc1X5BE-3A7QEtHWMomvb |
+-----------------------------------------------------------+
| Pulled LSNs ..=1 into remote Log 74ggc1X5BE-3A7QEtHWMomvb |
+-----------------------------------------------------------+
sqlite> .tables
wdi_country         wdi_csv             wdi_series
wdi_country_series  wdi_footnote        wdi_series_time
```

## Fork

A database can be forked into a new volume using `graft_fork`:

```sql
-- First ensure all pages are downloaded
pragma graft_hydrate;

-- Then fork
pragma graft_fork;
+---------------------------------------------------------------+
| Forked current snapshot into Volume: 5rMJkfPC21-3AHxh6aeSWzCp |
+---------------------------------------------------------------+
| Forked current snapshot into Volume: 5rMJkfPC21-3AHxh6aeSWzCp |
+---------------------------------------------------------------+
```

This creates a divergent copy that's independent from the original. The Volume must be fully hydrated before forking.

## Import from Existing Database

You can import an existing SQLite database into Graft using SQLite's `VACUUM INTO` command with a URI filename:

```sql
-- Open your existing database
.open /path/to/existing.db

-- Import into a new Graft tag
vacuum into 'file:mytag?vfs=graft';
```

This creates a new Graft Volume containing all the data from your existing database. The original database remains unchanged.
